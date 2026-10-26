#!/usr/bin/env python3
"""Join one node's numeric persistence/read trace with C++ client samples."""

import argparse
from bisect import bisect_left, bisect_right
from collections import defaultdict
import gzip
import json
from pathlib import Path

from cpp_load import read, require, summarize

STAGES = {1: "encode", 2: "write", 3: "file_sync", 4: "directory_sync", 5: "rename"}


def union_overlap(intervals, begin, end):
    clipped = sorted(
        (max(a, begin), min(b, end)) for a, b in intervals if a < end and b > begin
    )
    total, previous = 0, begin
    for a, b in clipped:
        total += max(0, b - max(a, previous))
        previous = max(previous, b)
    return total


def validate_persist(event):
    require(
        event["kind"] in [1, 2, 3, 4]
        and event["sequence"] > 0
        and event["tid"] > 0
        and 0 < event["started_ns"] <= event["finished_ns"]
        and event["success"] in [0, 1]
        and event["spans_limited"] in [0, 1]
        and 0 <= len(event["spans"]) <= 12,
        "invalid persistence record",
    )
    end, encoding = event["started_ns"], 0
    for stage, a, b in event["spans"]:
        require(
            stage in STAGES and end <= a <= b <= event["finished_ns"],
            "invalid persistence span",
        )
        end = b
        if stage == 1:
            encoding += b - a
    require(0 <= event["crc_ns"] <= encoding, "CRC is not contained in encoding")


def validate_client_ranges(directories):
    ranges = []
    for directory in directories:
        directory = Path(directory)
        config = read(directory / "result.json")
        require(
            config.get("trace_identity") is True, "randomized trace identity required"
        )
        for client in range(config["clients"]):
            record = read(directory / f"client-{client}.json")
            begin = record.get("connection_first_request_id", 0)
            measured = record.get("first_request_id", 0)
            end = measured + len(record["samples"]) - 1
            require(
                (1 << 62) <= begin <= measured and begin <= end < (1 << 64),
                "invalid connection request identity range",
            )
            ranges.append((begin, end))
    ranges.sort()
    require(
        all(a[1] < b[0] for a, b in zip(ranges, ranges[1:])),
        "connection request identity ranges overlap",
    )
    return len(ranges)


def analyze(events, directory, slow_ns=20_000_000):
    require(
        events and events[0]["event"] == "metadata" and events[0]["schema"] == 2,
        "schema 2 node trace required",
    )
    metadata = events[0]
    require(all(e["node"] == metadata["node"] for e in events), "mixed node clocks")
    kinds = {"metadata", "summary", "mode", "read", "peer", "persist"}
    require(all(e["event"] in kinds for e in events), "unknown trace event")
    footer = events[-1] if events[-1]["event"] == "summary" else None
    require(
        sum(e["event"] == "metadata" for e in events) == 1
        and sum(e["event"] == "summary" for e in events) == bool(footer),
        "mixed process lifetimes or misplaced summary",
    )
    recorded = sum(e["event"] in ["read", "peer", "persist"] for e in events)
    clean = bool(
        footer
        and footer["written"] == recorded
        and footer["accepted"] == recorded
        and not any(footer[k] for k in ["dropped", "limited", "io_failed"])
    )
    reads, operations = defaultdict(list), defaultdict(list)
    sequences = set()
    for event in events:
        if event["event"] == "read":
            reads[event["client_port"], event["request_id"]].append(event)
        elif event["event"] == "persist":
            require(
                not event.get("task_sequence", 0),
                "async disk tasks require dependency analysis; same-thread overlap is invalid",
            )
            validate_persist(event)
            require(
                event["sequence"] not in sequences, "duplicate persistence sequence"
            )
            sequences.add(event["sequence"])
            operations[event["tid"]].append(event)
            clean = clean and not event["spans_limited"]
    indexed = {}
    for tid, values in operations.items():
        values.sort(key=lambda p: p["started_ns"])
        require(
            all(
                a["finished_ns"] <= b["started_ns"] for a, b in zip(values, values[1:])
            ),
            "overlapping store operations on one worker",
        )
        indexed[tid] = (
            values,
            [p["started_ns"] for p in values],
            [p["finished_ns"] for p in values],
        )
    directory = Path(directory)
    config = read(directory / "result.json")
    require(config.get("trace_identity") is True, "randomized trace identity required")
    validate_client_ranges([directory])
    measured = summarize(directory)
    require(measured["valid"], "complete successful client samples required")
    rows, missing, ambiguous, gets, slow_gets = [], 0, 0, 0, 0
    for client in range(config["clients"]):
        record = read(directory / f"client-{client}.json")
        require(
            0 < record.get("local_port", 0) <= 65535
            and record.get("first_request_id", 0) > 0,
            "C++ connection identity metadata required",
        )
        for sequence, sample in enumerate(record["samples"]):
            if sample[4]:
                continue
            gets += 1
            slow_gets += sample[0] >= slow_ns
            request_id = record["first_request_id"] + sequence
            candidates = reads[record["local_port"], request_id]
            if not candidates:
                missing += 1
                continue
            if len(candidates) != 1:
                ambiguous += 1
                continue
            event = candidates[0]
            names = [
                "received_ns",
                "handled_ns",
                "context_ns",
                "quorum_ns",
                "ready_ns",
                "response_queued_ns",
                "tcp_loop_ns",
                "tcp_dispatch_done_ns",
            ]
            stamps = [event[k] for k in names]
            require(
                event["code"] == 0
                and event["handled_tid"] > 0
                and stamps[0] > 0
                and stamps == sorted(stamps),
                "successful client joined an invalid read trace",
            )
            begin, handled, end = (
                event["received_ns"],
                event["handled_ns"],
                event["ready_ns"],
            )
            values, starts, ends = indexed.get(event["handled_tid"], ([], [], []))
            selected = values[bisect_right(ends, begin) : bisect_left(starts, end)]
            intervals = [(p["started_ns"], p["finished_ns"]) for p in selected]
            overlap = union_overlap(intervals, begin, end)
            stage_overlap = {
                name
                + "_overlap_ns": union_overlap(
                    [
                        (a, b)
                        for p in selected
                        for stage, a, b in p["spans"]
                        if stage == number
                    ],
                    begin,
                    end,
                )
                for number, name in STAGES.items()
            }
            require(
                sum(stage_overlap.values()) <= overlap <= end - begin,
                "overlap accounting exceeds request interval",
            )
            rows.append(
                dict(
                    client=client,
                    client_port=record["local_port"],
                    request_id=request_id,
                    client_latency_ns=sample[0],
                    received_ns=begin,
                    handled_ns=handled,
                    ready_ns=end,
                    server_receive_to_ready_ns=end - begin,
                    queue_ns=handled - begin,
                    queue_persist_overlap_ns=union_overlap(intervals, begin, handled),
                    persist_overlap_ns=overlap,
                    handled_persist_overlap_ns=union_overlap(intervals, handled, end),
                    **stage_overlap,
                )
            )
    require(gets > 0, "no measured GETs")
    return dict(
        node=metadata["node"],
        pid=metadata["pid"],
        capture_clean=clean,
        measured_gets=gets,
        joined_gets=len(rows),
        missing_gets=missing,
        ambiguous_gets=ambiguous,
        joined_fraction=len(rows) / gets,
        client_slow_threshold_ns=slow_ns,
        client_slow_gets=slow_gets,
        joined_slow_gets=sum(r["client_latency_ns"] >= slow_ns for r in rows),
        joined_gets_overlapping_persist=sum(r["persist_overlap_ns"] > 0 for r in rows),
        recorded_persistence_operations=sum(len(p) for p in operations.values()),
        failed_persistence_operations=sum(
            not p["success"] for ps in operations.values() for p in ps
        ),
        persistence_sample_every=metadata["sample_every"],
        rows=rows,
        note="Same-node steady-clock interval unions on the read's worker TID. Observed overlaps are lower bounds when sampled, toggled, truncated or dropped. Client identity joins use port and sequential request ID; ambiguous port reuse is excluded. No cross-host clock subtraction or attribution of follower I/O, scheduling or network delay.",
    )


def load_trace(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as stream:
        return [json.loads(line) for line in stream if line.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(load_trace(args.trace), args.samples)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({k: v for k, v in result.items() if k != "rows"}))
