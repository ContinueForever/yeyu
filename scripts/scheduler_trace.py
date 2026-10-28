"""Strict Linux perf scheduler parsing for offline evidence verification."""

from bisect import bisect_right
from collections import Counter, defaultdict
import gzip
import re

# perf may print -1/-1 for short-lived tasks with missing comm metadata. The
# tracepoint payload still carries the actual switching/woken TIDs.
LINE = re.compile(
    r"(-?\d+)/(-?\d+)\s+\[(\d+)\]\s+(\d+)\.(\d{9}):\s+sched:sched_(switch|wakeup_new|wakeup):\s+(.*)"
)
SWITCH = re.compile(r".*:(\d+)\s+\[\d+\]\s+(\S+)\s+==>\s+.*:(\d+)\s+\[\d+\]")
WAKE = re.compile(r".*:(\d+)\s+\[\d+\]\s+CPU:(\d+)")


def parse(path):
    runnable = defaultdict(list)
    running = defaultdict(list)
    sleeping = defaultdict(list)
    ready = {}
    blocked = {}
    cpus = {}
    on_cpu = {}
    counts = Counter()
    first = last = None
    idle = defaultdict(list)
    names = {}
    states = defaultdict(Counter)
    with gzip.open(path, "rt") as stream:
        for line in stream:
            if "LOST" in line.upper():
                raise ValueError("lost scheduler events: " + line.strip())
            match = LINE.search(line)
            if not match:
                if line.strip():
                    counts["unparsed_lines"] += 1
                continue
            _, _, cpu, seconds, nanos, kind, detail = match.groups()
            cpu = int(cpu)
            now = int(seconds) * 1_000_000_000 + int(nanos)
            if first is None:
                first = now
            if last is not None and now < last:
                counts["nonmonotonic_events"] += 1
            last = now
            counts[kind] += 1
            if kind != "switch":
                w = WAKE.fullmatch(detail)
                assert w, detail
                tid = int(w[1])
                if tid in on_cpu:
                    counts["wakeup_while_running"] += 1
                    continue
                if tid in blocked:
                    start, state = blocked.pop(tid)
                    if now >= start:
                        sleeping[tid].append((start, now, state))
                ready.setdefault(tid, (now, "wakeup"))
                continue
            s = SWITCH.fullmatch(detail)
            assert s, detail
            previous, state, next_tid = int(s[1]), s[2], int(s[3])
            left, right = detail.split(" ==> ")
            names[previous] = left.rsplit(":", 1)[0]
            names[next_tid] = right.rsplit(":", 1)[0]
            states[previous][state] += 1
            current = cpus.get(cpu)
            if current is not None:
                if current[0] != previous:
                    counts["cpu_sequence_mismatch"] += 1
                elif previous:
                    running[previous].append((current[1], now, str(cpu)))
                else:
                    idle[cpu].append((current[1], now, "idle"))
            if previous:
                if on_cpu.get(previous) == cpu:
                    on_cpu.pop(previous, None)
                if state.startswith("R"):
                    ready.setdefault(previous, (now, "preempted"))
                else:
                    ready.pop(previous, None)
                    blocked[previous] = (now, state)
            if next_tid:
                if next_tid in ready:
                    start, reason = ready.pop(next_tid)
                    if now >= start:
                        runnable[next_tid].append((start, now, reason))
                else:
                    counts["switch_in_without_ready"] += 1
                blocked.pop(next_tid, None)
                on_cpu[next_tid] = cpu
            cpus[cpu] = (next_tid, now)
    assert first is not None and last > first
    for tid, (start, reason) in ready.items():
        if last > start:
            runnable[tid].append((start, last, reason + "-capture-end"))
    for cpu, (tid, start) in cpus.items():
        if tid and last > start:
            running[tid].append((start, last, str(cpu)))
        elif not tid and last > start:
            idle[cpu].append((start, last, "idle"))
    for tid, (start, state) in blocked.items():
        if last > start:
            sleeping[tid].append((start, last, state + "-capture-end"))
    assert (
        not counts["cpu_sequence_mismatch"]
        and not counts["nonmonotonic_events"]
        and not counts["unparsed_lines"]
    ), counts
    return dict(
        first=first,
        last=last,
        runnable=runnable,
        running=running,
        sleeping=sleeping,
        counts=dict(counts),
        idle=idle,
        names=names,
        states=states,
    )


class Intervals:
    def __init__(self, rows):
        self.rows = sorted(rows)
        self.ends = [x[1] for x in self.rows]

    def overlap(self, start, end):
        total = 0
        pieces = []
        for i in range(bisect_right(self.ends, start), len(self.rows)):
            a, b, reason = self.rows[i]
            if a >= end:
                break
            lo, hi = max(a, start), min(b, end)
            if hi > lo:
                total += hi - lo
                pieces.append((lo, hi, reason))
        return total / 1e6, pieces


def union_ms(rows):
    rows = sorted((a, b) for a, b, *_ in rows)
    if not rows:
        return 0
    start, end = rows[0]
    total = 0
    for a, b in rows[1:]:
        if a <= end:
            end = max(end, b)
        else:
            total += end - start
            start, end = a, b
    return (total + end - start) / 1e6
