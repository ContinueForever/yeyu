#!/usr/bin/env python3
"""Offline 48-run evidence verification. Never executes archived source/tools."""
import argparse
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import tarfile
from scheduler_trace import parse as parse_scheduler

ORIGINAL = "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
EXPECTED = {
    "initial": (20, 1608704),
    "address-fix": (8, 808276),
    "scheduler": (5, 592058),
    "affinity": (15, 2015625),
}
STAGES = [
    "received_ns",
    "handled_ns",
    "context_ns",
    "quorum_ns",
    "ready_ns",
    "response_queued_ns",
    "tcp_loop_ns",
    "tcp_dispatch_done_ns",
]
PEER = [
    "scheduled_ns",
    "worker_ns",
    "call_ns",
    "first_write_ns",
    "write_done_ns",
    "decoded_ns",
    "returned_ns",
    "node_received_ns",
]


def require(value, message):
    if not value:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    files = load(root / "publication-manifest.json")["files"]
    actual = {
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and p.name != "publication-manifest.json"
    }
    require(actual == set(files), "publication inventory differs")
    for name, wanted in files.items():
        path = root / name
        require(
            not path.is_symlink() and path.resolve().is_relative_to(root.resolve()),
            "unsafe evidence path",
        )
        require(
            path.stat().st_size == wanted["bytes"] and digest(path) == wanted["sha256"],
            "file hash differs: " + name,
        )
    return len(files)


def verify_case(folder, identity, original=False):
    result = load(folder / "result.json")
    config = load(folder / "workload.json")
    require(
        result["status"] == "complete"
        and result["config"] == config
        and result["failures"] == result["completed_puts"] == 0,
        "failed/inconsistent run",
    )
    require(
        config["shared_read_keys"] == 256
        and config["value_size"] == 128
        and config["write_percent"] == 0
        and config["clients"] in [8, 32]
        and config["duration_seconds"] == 20,
        "workload differs",
    )
    require([result["term"], result["leader_id"]] == identity, "identity changed")
    clients = result["clients"]
    require(
        len(clients) == config["clients"]
        and {c["client"] for c in clients} == set(range(config["clients"])),
        "client inventory differs",
    )
    values = []
    for client in clients:
        require(
            client == load(folder / f"client-{client['client']}.json")
            and client["status"] == "complete"
            and client["failures"] == 0,
            "client metadata differs",
        )
        require(
            client["samples_file"] == f"client-{client['client']}.json.gz",
            "unsafe samples file",
        )
        with gzip.open(folder / client["samples_file"], "rt") as stream:
            raw = json.load(stream)
        require(
            raw["columns"] == ["latency_ns", "completed_offset_ns", "put", "ok"],
            "sample schema differs",
        )
        previous = 0
        for latency, offset, put, ok in raw["samples"]:
            require(
                type(latency) == type(offset) == int
                and 0 < latency <= offset
                and offset - latency >= previous
                and offset <= client["finished_ns"] - result["start_ns"]
                and put == 0
                and ok == 1,
                "invalid/failed sample",
            )
            previous = offset
            values.append(latency)
        require(
            client["final_versions"] == [0] * 256, "final value verification differs"
        )
    values.sort()
    n = len(values)
    require(n > 0, "empty samples")
    stats = {
        name: round(values[math.ceil(n * f) - 1] / 1e6, 3)
        for name, f in [
            ("p50_ms", 0.5),
            ("p95_ms", 0.95),
            ("p99_ms", 0.99),
            ("p999_ms", 0.999),
        ]
    }
    stats.update(max_ms=round(values[-1] / 1e6, 3), samples=n)
    require(
        result["latency"]["all"] == result["latency"]["get"] == stats
        and result["latency"]["put"] is None,
        "percentile differs",
    )
    elapsed = (max(c["finished_ns"] for c in clients) - result["start_ns"]) / 1e9
    require(
        20 <= elapsed < 26
        and result["operations"] == result["completed_gets"] == n
        and result["elapsed_seconds"] == elapsed
        and result["ops_per_second"] == n / elapsed,
        "request/throughput total differs",
    )
    before, after = load(folder / "before.json"), load(folder / "after.json")
    require(
        len(before) == len(after) == 3 and [s["id"] for s in before] == [1, 2, 3],
        "node inventory differs",
    )
    for a, b in zip(before, after):
        require(
            a["last_index"]
            == a["commit_index"]
            == a["applied_index"]
            == b["last_index"]
            == b["commit_index"]
            == b["applied_index"]
            and a["disk_bytes"] == b["disk_bytes"],
            "read changed log/disk",
        )
        require(
            a["term"] == b["term"] == identity[0]
            and a["leader_id"] == b["leader_id"] == identity[1],
            "leader changed during run",
        )
        if original:
            require(
                config["trace_enabled"] == 0
                and "trace_enabled" not in a
                and "trace_enabled" not in b,
                "original trace fields differ",
            )
        else:
            require(
                a["trace_enabled"] == b["trace_enabled"] == config["trace_enabled"]
                and all(
                    a[k] == b[k] == 0
                    for k in ["trace_dropped", "trace_failed", "trace_limited"]
                ),
                "trace mode/error",
            )
    require(
        after[identity[1] - 1]["read_index_completed"]
        - before[identity[1] - 1]["read_index_completed"]
        >= n,
        "read confirmation accounting differs",
    )
    return n


def trace_records(folder):
    for path in sorted(folder.glob("*.jsonl.gz")):
        count = 0
        last = None
        with gzip.open(path, "rt") as stream:
            for line in stream:
                event = json.loads(line)
                last = event
                require(
                    event["event"] in ["metadata", "mode", "read", "peer", "summary"]
                    and all(
                        type(v) == int and v >= 0
                        for k, v in event.items()
                        if k != "event"
                    ),
                    "trace schema differs",
                )
                if event["event"] in ["read", "peer"]:
                    count += 1
                yield event
        require(
            last
            and last["event"] == "summary"
            and last["accepted"] == last["written"] == count
            and last["dropped"] == last["limited"] == last["io_failed"] == 0,
            "trace footer incomplete",
        )


def correlate(root, phase):
    if not phase["read_correlation_valid"]:
        return 0
    leader = phase["identity"][1]
    term = phase["identity"][0]
    reads = {}
    peers = {}
    for event in trace_records(root / "trace" / f"node-{leader}"):
        if event["event"] == "read" and event["term"] == term:
            reads.setdefault((event["client_port"], event["request_id"]), []).append(
                event
            )
        elif event["event"] == "peer":
            key = (event["term"], event["peer_id"], event["sequence"])
            require(key not in peers, "duplicate peer RPC")
            peers[key] = event
    matched = 0
    for case in phase["cases"]:
        if not case["mode"] or case["name"] == "c32-trace1-packets":
            continue
        folder = root / "cases" / phase["phase"] / case["name"]
        result = load(folder / "result.json")
        prefix = "leader2-boundary-" if phase["phase"] == "leader2" else "boundary-"
        lower = load(root / "control" / (prefix + "before-" + case["name"] + ".json"))[
            leader - 1
        ]["monotonic_ns"]
        upper = load(root / "control" / (prefix + "after-" + case["name"] + ".json"))[
            leader - 1
        ]["monotonic_ns"]
        for client in result["clients"]:
            with gzip.open(folder / client["samples_file"], "rt") as stream:
                samples = json.load(stream)["samples"]
            for offset, sample in enumerate(samples):
                candidates = [
                    t
                    for t in reads.get(
                        (
                            client["local_endpoint"][1],
                            client["measured_first_request_id"] + offset,
                        ),
                        [],
                    )
                    if lower <= t["received_ns"] <= t["tcp_dispatch_done_ns"] <= upper
                ]
                if not candidates:
                    continue
                require(len(candidates) == 1, "ambiguous trace match")
                t = candidates[0]
                require(
                    t["code"] == 0
                    and [t[k] for k in STAGES] == sorted(t[k] for k in STAGES)
                    and t["received_ns"] > 0,
                    "read stage order",
                )
                p = peers.get((t["term"], t["confirmed_by"], t["confirmed_sequence"]))
                require(
                    p
                    and p["context"] == t["context"]
                    and p["success"] == 1
                    and p["scheduled_ns"] >= t["context_ns"]
                    and p["node_received_ns"] <= t["quorum_ns"]
                    and [p[k] for k in PEER] == sorted(p[k] for k in PEER),
                    "confirming RPC differs",
                )
                matched += 1
    return matched


def verify_affinity(root, label, cases):
    folder = root / "affinity" / label
    initial = load(folder / "original-affinity.json")
    restored = load(folder / "restored-affinity.json")
    require(
        initial["pid"] == restored["pid"]
        and all(v == [0, 1] for v in initial["threads"].values())
        and all(v == [0, 1] for v in restored["threads"].values()),
        "affinity not restored",
    )
    for case in cases:
        for when in ["before", "after"]:
            actual = load(folder / f"affinity-{when}-{case['name']}.json")
            require(
                actual["pid"] == initial["pid"]
                and actual["threads"]
                and all(
                    v == [int(n) for n in case["affinity"].split(",")]
                    for v in actual["threads"].values()
                ),
                "wrong affinity mask",
            )
    before = load(folder / "services-before-campaign.json")
    after = load(folder / "services-after-campaign.json")
    require(len(before) == len(after) == 3, "service inventory")
    for a, b in zip(before, after):
        require(
            a["properties"] == b["properties"]
            and a["running_sha256"] == b["running_sha256"]
            and a["boot_id"] == b["boot_id"]
            and a["env"] == b["env"],
            "process/config changed",
        )
        if label == "original":
            require(
                a["running_sha256"] == a["installed_sha256"] == ORIGINAL
                and not a["properties"]["DropInPaths"],
                "original program differs",
            )


def verify(root):
    files = inventory(root)
    manifest = load(root / "manifest.json")
    counts = Counter()
    ops = Counter()
    correlations = {}
    require(
        manifest["runs"] == 48
        and manifest["operations"] == 5024663
        and manifest["failures"] == 0,
        "manifest totals differ",
    )
    for dataset in EXPECTED:
        folder = root / dataset
        inputs = load(folder / "local/inputs.json")
        require(
            digest(folder / "local/source.snapshot.tar.gz") == inputs["source_sha256"],
            "source archive differs",
        )
        with tarfile.open(folder / "local/source.snapshot.tar.gz", "r:gz") as archive:
            for name, wanted in load(folder / "local/source-manifest.json")[
                "files_sha256"
            ].items():
                require(
                    hashlib.sha256(
                        archive.extractfile("source/" + name).read()
                    ).hexdigest()
                    == wanted,
                    "source file differs",
                )
        for kind in ["normal", "asan"]:
            entry = load(folder / "local/test-result.json")[kind]
            path = folder / entry["file"]
            require(
                digest(path) == entry["sha256"]
                and "100% tests passed, 0 tests failed out of 134" in path.read_text(),
                "test evidence differs",
            )
        for node in [1, 2, 3]:
            for _ in trace_records(folder / "trace" / f"node-{node}"):
                pass
        for service in load(folder / "control/restoration-audit.json"):
            require(
                service["running_sha256"] == service["installed_sha256"] == ORIGINAL
                and service["properties"]["ActiveState"] == "active"
                and not service["properties"]["DropInPaths"]
                and not service["temporary_jobs"].strip(),
                "service not restored",
            )
    for phase in manifest["phases"]:
        dataset = phase["dataset"]
        folder = root / dataset
        for case in phase["cases"]:
            ops[dataset] += verify_case(
                folder / "cases" / phase["phase"] / case["name"],
                phase["identity"],
                case["program"] == "original",
            )
            counts[dataset] += 1
        if phase["phase"] == "original":
            verify_affinity(folder, "original", phase["cases"])
        elif dataset == "affinity":
            verify_affinity(folder, "trace", phase["cases"])
        matched = correlate(folder, phase)
        correlations[dataset + "/" + phase["phase"]] = matched
        if phase["read_correlation_valid"]:
            # Scheduler diagnostics are included in its original matching cohort.
            expected = {
                "address-fix": 14357,
                "scheduler": 23406,
                "affinity": 43097,
            }.get(dataset)
            if expected is not None:
                require(matched == expected, "read correlation count differs")
    for dataset, (runs, operations) in EXPECTED.items():
        require(
            (counts[dataset], ops[dataset]) == (runs, operations),
            "dataset total differs: " + dataset,
        )
    sched = load(root / "scheduler/analysis/scheduler-analysis.json")
    events = 0
    for number, case in enumerate(sched["cases"], 1):
        parsed = parse_scheduler(
            root / f"scheduler/scheduler/perf-{number}/events.txt.gz"
        )
        require(
            parsed["counts"] == case["event_counts"]
            and parsed["first"] == case["first_ns"]
            and parsed["last"] == case["last_ns"]
            and case["matched_reads_outside_capture"] == 0,
            "scheduler reconstruction differs",
        )
        events += sum(
            parsed["counts"].get(k, 0) for k in ["switch", "wakeup", "wakeup_new"]
        )
    require(
        events == 1722899
        and sum(c["captured_matched_reads"] for c in sched["cases"]) == 9380,
        "scheduler totals differ",
    )
    final = load(root / "affinity/control/final-data-verification.json")
    require(
        final["verified"]
        and final["checked_experiment_keys"] == 512
        and all(
            s["applied_index"] == s["commit_index"] == s["last_index"] == 711089
            and s["keys"] == 2852
            for s in final["states"]
        ),
        "final data differs",
    )
    return dict(
        verified=True,
        runs=sum(counts.values()),
        operations=sum(ops.values()),
        failures=0,
        files=files,
        read_correlations=correlations,
        scheduler_events=events,
        note="Retained numeric evidence consistency; packet summaries are not independent packet recomputation. No authentication of remote machines or production-capacity claim.",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.root.resolve())))


if __name__ == "__main__":
    main()
