#!/usr/bin/env python3
"""Verify published file hashes, benchmark accounting and bounded fault histories.

This checks the retained records without starting servers. It does not rerun the
original experiments or establish a production performance/safety guarantee.
"""

import argparse
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from linearizability import check


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def verify_files(root):
    inventory = load(root / "publication-manifest.json")["files"]
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
              and p.name != "publication-manifest.json"}
    require(actual == set(inventory), "publication file inventory differs")
    for name, expected in inventory.items():
        path = root / name
        require(not path.is_symlink() and path.resolve().is_relative_to(root), f"unsafe path: {name}")
        content = path.read_bytes()
        require(len(content) == expected["bytes"], f"file size differs: {name}")
        require(hashlib.sha256(content).hexdigest() == expected["sha256"], f"hash differs: {name}")
    for source in root.rglob("source-manifest.json"):
        archive = source.with_name("source.tar.gz")
        require(archive.is_file(), f"missing source archive: {source}")
        expected = load(source)
        with tarfile.open(archive, "r:gz") as reader:
            members = reader.getmembers()
            require(len(members) == len(expected), f"source inventory differs: {archive}")
            require({m.name for m in members} == set(expected), f"source paths differ: {archive}")
            for member in members:
                require(member.isfile(), f"non-file source member: {member.name}")
                with reader.extractfile(member) as content:
                    digest = hashlib.sha256(content.read()).hexdigest()
                require(digest == expected[member.name], f"source hash differs: {member.name}")
    return len(inventory)


def verify_matrix(directory):
    manifest = load(directory / "manifest.json")
    require(manifest["status"] == "complete", f"incomplete matrix: {directory}")
    operations = 0
    for result in manifest["runs"]:
        name = (f"{result['profile']}-batch{result['batch_size']}-"
                f"snap{result['snapshot_entries']}-run{result['repeat']}")
        run = directory / name
        require(load(run / "result.json") == result, f"run differs from manifest: {name}")
        require(result["failures"] == 0, f"failed run: {name}")
        latencies, buckets = [], Counter()
        clients = result["client_results"]
        require(len(clients) == result["clients"], f"client count differs: {name}")
        require(len({c["client_id"] for c in clients}) == len(clients), f"duplicate client: {name}")
        for client in clients:
            with gzip.open(run / client["samples_file"], "rt") as reader:
                samples = json.load(reader)
            latency, offsets = samples["latency_ns"], samples["completed_offset_ns"]
            require(len(latency) == len(offsets) == client["operations"], f"sample count differs: {name}")
            require(client["puts"] + client["gets"] == len(latency), f"client mix differs: {name}")
            require(all(isinstance(v, int) and v > 0 for v in latency), f"invalid latency: {name}")
            require(offsets == sorted(offsets), f"completion order differs: {name}")
            require(all(lag <= offset <= result["elapsed_seconds"] * 1e9 + 1
                        for lag, offset in zip(latency, offsets)), f"invalid timing: {name}")
            latencies.extend(latency)
            buckets.update(offset // 1_000_000_000 for offset in offsets)
        require(len(latencies) == result["operations"], f"operation count differs: {name}")
        require(result["completed_puts"] + result["completed_gets"] == len(latencies), f"mix differs: {name}")
        require(math.isclose(len(latencies) / result["elapsed_seconds"], result["ops_per_second"]),
                f"throughput differs: {name}")
        latencies.sort()
        for field, quantile in (("p50_ms", .50), ("p95_ms", .95), ("p99_ms", .99)):
            measured = round(latencies[math.ceil(len(latencies) * quantile) - 1] / 1e6, 3)
            require(measured == result[field], f"{field} differs: {name}")
        timeline = load(run / "timeline.json")
        require(sum(row["operations"] for row in timeline) == len(latencies), f"timeline sum differs: {name}")
        for row in timeline:
            require(buckets.pop(row["start_seconds"], 0) == row["operations"], f"timeline bucket differs: {name}")
        require(not buckets, f"missing timeline bucket: {name}")
        operations += len(latencies)
    return len(manifest["runs"]), operations


def verify_soak(directory):
    summary = load(directory / "summary.json")
    require(summary["status"] == "complete", "incomplete fault campaign")
    epochs, attempted, checked, unknown, faults = 0, 0, 0, 0, Counter()
    with gzip.open(directory / "epochs.jsonl.gz", "rt") as reader:
        for line in reader:
            record = json.loads(line)
            history, result = record["history"], record["result"]
            require(history["epoch"] == result["epoch"] == epochs, "fault epoch sequence differs")
            # JSON uses null for an unknown write's unbounded completion time.
            model = [{**op, "end": math.inf if op.get("optional") else op["end"]}
                     for op in history["operations"]]
            require(all(op["end"] is None for op in history["operations"] if op.get("optional")),
                    "unknown write must have an unbounded completion time")
            require(result["checked"] and check(model), f"history rejected: epoch {epochs}")
            require(len(history["attempts"]) == result["attempted_operations"], "attempt count differs")
            require(len(history["operations"]) == result["checked_operations"], "checked count differs")
            optional_writes = sum(op.get("optional", False) and op["kind"] != "get"
                                  for op in history["operations"])
            require(optional_writes == result["unknown_writes"], "unknown write count differs")
            require(max(Counter(op["key"] for op in history["operations"]).values(), default=0) <= 9,
                    "campaign exceeds documented per-key bound")
            attempted += len(history["attempts"])
            checked += len(history["operations"])
            unknown += optional_writes
            faults[result["fault"]] += 1
            epochs += 1
    for field, value in (("epochs", epochs), ("attempted_operations", attempted),
                         ("checked_operations", checked), ("unknown_writes", unknown),
                         ("fault_counts", dict(faults))):
        require(summary[field] == value, f"campaign summary differs: {field}")
    return epochs, checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    root = parser.parse_args().directory.resolve()
    files = verify_files(root)
    runs, operations = 0, 0
    for dataset in ("matrix-20261021", "snapshot-ab-20261021"):
        count, samples = verify_matrix(root / dataset)
        runs += count
        operations += samples
    epochs, checked = verify_soak(root / "soak-20261021-final-15min")
    print(json.dumps(dict(files=files, benchmark_runs=runs, timed_operations=operations,
                          fault_epochs=epochs, model_checked_operations=checked, verified=True)))


if __name__ == "__main__":
    main()
