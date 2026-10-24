#!/usr/bin/env python3
"""Observe an existing Linux node without changing its service or network settings."""

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time


def command(*args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=5)
    return dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)


def text(path):
    try:
        return Path(path).read_text()
    except OSError as error:
        return "ERROR: " + str(error)


def paired_counters(path):
    lines = text(path).splitlines()
    counters = {}
    for header, values in zip(lines[::2], lines[1::2]):
        names, numbers = header.split(), values.split()
        if names and numbers and names[0] == numbers[0]:
            counters.update({names[0] + name: int(value)
                             for name, value in zip(names[1:], numbers[1:])})
    return counters


def process(pid):
    directory = Path(f"/proc/{pid}")
    raw = (directory / "stat").read_text()
    fields = raw.rsplit(")", 1)[1].split()
    threads = []
    for entry in sorted((directory / "task").iterdir()):
        try:
            thread_fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            threads.append(dict(tid=int(entry.name), state=thread_fields[0],
                                utime=int(thread_fields[11]), stime=int(thread_fields[12]),
                                schedstat=(entry / "schedstat").read_text().strip(),
                                context_switches=[line for line in (entry / "status").read_text().splitlines()
                                                  if "ctxt_switches" in line]))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return dict(pid=pid, start_ticks=int(fields[19]), state=fields[0],
                utime=int(fields[11]), stime=int(fields[12]),
                rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
                io=text(directory / "io"), schedstat=text(directory / "schedstat"),
                threads=threads)


def sample(pid):
    before = time.monotonic_ns()
    result = dict(realtime_ns=time.time_ns(), monotonic_ns=before,
                  node=process(pid), cpu=[line for line in text("/proc/stat").splitlines()
                                         if line.startswith(("cpu", "ctxt ", "procs_"))],
                  load=text("/proc/loadavg"),
                  memory=[line for line in text("/proc/meminfo").splitlines()
                          if line.startswith(("MemAvailable:", "MemFree:", "Dirty:", "Writeback:", "SwapFree:"))],
                  disks=text("/proc/diskstats"), network=text("/proc/net/dev"),
                  tcp=paired_counters("/proc/net/snmp"),
                  tcp_ext=paired_counters("/proc/net/netstat"),
                  pressure={name: text(f"/proc/pressure/{name}") for name in ("cpu", "io", "memory")})
    result["collection_ns"] = time.monotonic_ns() - before
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--duration", type=float, default=1800)
    parser.add_argument("--interval", type=float, default=.25)
    parser.add_argument("--tcp-interval", type=float, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (args.pid > 0 and 1 <= args.node_id <= 3 and 1 <= args.port <= 65535
            and .1 <= args.interval <= 10 and args.interval <= args.tcp_interval <= 60
            and 1 <= args.duration <= 7200):
        parser.error("invalid PID, node, port or bounded sampling interval/duration")
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    stopping = []
    def stop(signum, _frame):
        stopping.append(signum)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    metadata = dict(version=1, node_id=args.node_id, hostname=socket.gethostname(),
                    started_utc=datetime.now(timezone.utc).isoformat(),
                    clock_ticks=os.sysconf("SC_CLK_TCK"), cpu_count=os.cpu_count(),
                    boot_id=text("/proc/sys/kernel/random/boot_id").strip(),
                    pid=args.pid, interval=args.interval, tcp_interval=args.tcp_interval,
                    executable=os.readlink(f"/proc/{args.pid}/exe"),
                    binary_sha256=hashlib.sha256(Path(f"/proc/{args.pid}/exe").read_bytes()).hexdigest(),
                    tool_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    service=command("systemctl", "show", f"yewukv-node@{args.node_id}.service",
                                    "--property=MainPID,ActiveState,NRestarts,ExecMainStartTimestamp", "--no-pager"),
                    note="System counters are host-wide; process/thread counters are scoped to the existing node. TCP snapshots are diagnostic, not packet captures.")
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    started, next_tcp = time.monotonic(), 0
    initial_start, count = None, 0
    result = dict(status="failed")
    try:
        with gzip.open(args.output / "samples.jsonl.gz", "wt") as target, \
                gzip.open(args.output / "tcp.jsonl.gz", "wt") as tcp:
            while time.monotonic() - started < args.duration and not stopping:
                if (args.output / "STOP").exists():
                    break
                before = time.monotonic()
                data = sample(args.pid)
                identity = data["node"]["start_ticks"]
                if initial_start is None:
                    initial_start = identity
                if identity != initial_start:
                    raise RuntimeError("node PID was reused; stable-run observation rejected")
                target.write(json.dumps(data, separators=(",", ":")) + "\n")
                target.flush()
                count += 1
                if before >= next_tcp:
                    snapshot = dict(realtime_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                                    ss=command("ss", "-tinp", f"( sport = :{args.port} or dport = :{args.port} )"))
                    tcp.write(json.dumps(snapshot, separators=(",", ":")) + "\n")
                    tcp.flush()
                    next_tcp = before + args.tcp_interval
                time.sleep(max(0, args.interval - (time.monotonic() - before)))
        result.update(status="complete", samples=count,
                      stop_reason="controller" if (args.output / "STOP").exists() else
                                  "signal" if stopping else "duration",
                      elapsed_seconds=time.monotonic() - started,
                      collector_cpu_seconds=time.process_time())
        after = command("systemctl", "show", f"yewukv-node@{args.node_id}.service",
                        "--property=MainPID,ActiveState,NRestarts,ExecMainStartTimestamp", "--no-pager")
        if after != metadata["service"]:
            raise RuntimeError("service changed while observing")
    except BaseException as error:
        result.update(status="failed", error=repr(error), samples=count)
        raise
    finally:
        result["finished_utc"] = datetime.now(timezone.utc).isoformat()
        result["sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in sorted(args.output.iterdir()) if p.is_file() and p.name != "summary.json"}
        (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
