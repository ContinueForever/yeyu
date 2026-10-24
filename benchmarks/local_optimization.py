#!/usr/bin/env python3
"""Interleaved same-binary A/B/C: image+log, journal+log, journal+ReadIndex.

Every mode retains synchronous persistence and majority-committed writes.
Raw per-request latency/completion times, resource samples and phase counters
are retained. This is a same-host closed-loop experiment, not saturation data.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import random
import statistics
import sys

import cluster_matrix as matrix
import rpc_baseline as rpc
import run_baseline as baseline

VARIANTS = {
    "image-log": ["--raft-store", "image", "--read-mode", "log"],
    "journal-log": ["--raft-store", "journal", "--read-mode", "log"],
    "journal-read-index": ["--raft-store", "journal", "--read-mode", "read-index"],
}
matrix.PROFILES["read-only"] = dict(clients=8, write_percent=0, value_size=128, keys_per_client=8)


def run_name(run):
    return (f"{run['variant']}-{run['profile']}-batch{run['batch_size']}-"
            f"snap{run['snapshot_entries']}-run{run['repeat']}")


def inventory(root):
    files = {}
    for path in sorted(root.rglob("*")):
        name = path.relative_to(root)
        if (not path.is_file() or path.name == "publication-manifest.json" or
                any(part in ("node-1", "node-2", "node-3") for part in name.parts)):
            continue
        files[name.as_posix()] = dict(bytes=path.stat().st_size, sha256=baseline.digest(path))
    baseline.save(root / "publication-manifest.json", dict(files=files))


def verify(root):
    def require(condition, message):
        if not condition:
            raise ValueError(message)
    files = json.loads((root / "publication-manifest.json").read_text())["files"]
    for name, expected in files.items():
        relative = PurePosixPath(name)
        require(not relative.is_absolute() and ".." not in relative.parts, "unsafe evidence path")
        path = root / name
        require(not path.is_symlink() and path.resolve().is_relative_to(root.resolve()), "unsafe evidence file")
        require(path.stat().st_size == expected["bytes"] and baseline.digest(path) == expected["sha256"],
                f"file checksum differs: {name}")
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["status"] == "complete", "incomplete experiment")
    count = 0
    for run in manifest["runs"]:
        directory = root / run_name(run)
        require(json.loads((directory / "result.json").read_text()) == run, "result differs from manifest")
        require(run["failures"] == 0, "failed workload")
        samples, buckets = [], Counter()
        for client in run["client_results"]:
            with gzip.open(directory / client["samples_file"], "rt") as source:
                raw = json.load(source)
            values, offsets = raw["latency_ns"], raw["completed_offset_ns"]
            require(len(values) == len(offsets) == client["operations"], "sample count differs")
            require(client["puts"] + client["gets"] == len(values), "client operation mix differs")
            require(offsets == sorted(offsets), "completion order differs")
            require(all(0 < latency <= offset <= run["elapsed_seconds"] * 1e9 + 1
                        for latency, offset in zip(values, offsets)), "invalid sample timing")
            samples.extend(values)
            buckets.update(offset // 1_000_000_000 for offset in offsets)
        require(len(samples) == run["operations"] == run["completed_puts"] + run["completed_gets"],
                "operation count differs")
        require(math.isclose(len(samples) / run["elapsed_seconds"], run["ops_per_second"]), "throughput differs")
        samples.sort()
        for name, quantile in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99)):
            require(rpc.percentile(samples, quantile) == run[name], f"{name} differs")
        timeline = json.loads((directory / "timeline.json").read_text())
        require(sum(row["operations"] for row in timeline) == len(samples), "timeline count differs")
        for row in timeline:
            require(buckets.pop(row["start_seconds"], 0) == row["operations"], "timeline bucket differs")
        require(not buckets, "missing timeline bucket")
        for metric in ("disk_bytes", "disk_saves", "disk_file_syncs", "disk_encode_ns", "disk_sync_ns", "disk_write_ns"):
            require(sum(a[metric] - b[metric] for a, b in zip(run["after"], run["before"])) == run[metric],
                    f"metric delta differs: {metric}")
        if run["profile"] == "read-only" and run["variant"] == "journal-read-index":
            require(run["disk_bytes"] == run["disk_saves"] == 0, "pure reads performed persistent writes")
            require(all(a["last_index"] == b["last_index"] for a, b in zip(run["after"], run["before"])),
                    "pure reads appended log entries")
        count += len(samples)
    expected = manifest["repeats"] * len(manifest["profiles"]) * len(VARIANTS)
    require(len(manifest["runs"]) == expected, "missing runs")
    print(json.dumps(dict(verified=True, files=len(files), runs=expected, timed_operations=count)))


def summarize(args, manifest):
    summary = []
    rows = ["# Local journal / ReadIndex comparison", "",
            "Same binary, same host, synchronous persistence, fixed three voters. Each cell is the median of run values.", "",
            "| Profile | Mode | ops/s | P99 ms | Written B/op (3 nodes) | Encode+CRC us/op | Sync+publish us/op | Server cores |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for profile in args.profiles:
        for variant in VARIANTS:
            runs = [r for r in manifest["runs"] if r["profile"] == profile and r["variant"] == variant]
            row = dict(profile=profile, variant=variant, repeats=len(runs), operations=sum(r["operations"] for r in runs))
            for name in ("ops_per_second", "p50_ms", "p95_ms", "p99_ms", "disk_bytes", "disk_saves",
                         "disk_file_syncs", "disk_directory_syncs", "server_average_cores", "client_average_cores"):
                row[name] = statistics.median(r[name] for r in runs)
            row["bytes_per_operation"] = statistics.median(r["disk_bytes"] / r["operations"] for r in runs)
            for phase in ("encode", "write", "sync", "persist"):
                row[f"{phase}_us_per_operation"] = statistics.median(
                    r[f"disk_{phase}_ns"] / r["operations"] / 1000 for r in runs)
            summary.append(row)
            rows.append(f"| {profile} | {variant} | {row['ops_per_second']:.1f} | {row['p99_ms']:.3f} | "
                        f"{row['bytes_per_operation']:.1f} | {row['encode_us_per_operation']:.1f} | "
                        f"{row['sync_us_per_operation']:.1f} | {row['server_average_cores']:.2f} |")
    rows += ["", "Three 10-second runs are the default; use manifest.json for the actual duration and repetitions.",
             "All phases and written bytes sum the three nodes. Written bytes are user-space successful durable writes, not device-sector traffic.",
             "Encode includes serialization and CRC; sync/publish includes file/directory synchronization and checkpoint publication.",
             "Preload and warmup are excluded. Disk/CPU counters include a short replication drain. P99 is a median of run P99s.",
             "Clients use a closed loop and disjoint keys and verify every GET. No saturation or cross-host availability claim is made."]
    baseline.save(args.output / "summary.json", summary)
    (args.output / "summary.md").write_text("\n".join(rows) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--node", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profiles", default="read-only,balanced,large-value,more-keys")
    parser.add_argument("--duration-seconds", type=float, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.verify:
        verify(args.verify.resolve())
        return
    args.profiles = args.profiles.split(",")
    if (not args.node or not args.output or args.repeats < 1 or args.workers < 1 or
            not math.isfinite(args.duration_seconds) or args.duration_seconds <= 0 or
            any(profile not in matrix.PROFILES for profile in args.profiles)):
        parser.error("provide node/output and valid workloads")
    args.node, args.output = args.node.resolve(), args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    args.snapshot_bytes = 1024 * 1024
    manifest = dict(format_version=1, command=sys.argv, status="running", profiles=args.profiles,
                    repeats=args.repeats, environment=baseline.environment(args.node),
                    source_manifest_sha256=baseline.source_snapshot(args.output),
                    variants=VARIANTS, runs=[])
    baseline.save(args.output / "manifest.json", manifest)
    try:
        for repeat in range(1, args.repeats + 1):
            order = [(profile, variant) for profile in args.profiles for variant in VARIANTS]
            random.Random(20261021 + repeat).shuffle(order)
            for profile, variant in order:
                args.variant, args.node_options = variant, VARIANTS[variant]
                result = matrix.run_one(args, profile, repeat, 32, 128)
                manifest["runs"].append(result)
                baseline.save(args.output / "manifest.json", manifest)
                print(json.dumps(dict(variant=variant, profile=profile, repeat=repeat,
                                      ops_per_second=result["ops_per_second"], p99_ms=result["p99_ms"])), flush=True)
        if baseline.digest(args.node) != manifest["environment"]["server_sha256"]:
            raise RuntimeError("binary changed during measurement")
        manifest["status"] = "complete"
        summarize(args, manifest)
    except BaseException as error:
        manifest.update(status="failed", error=repr(error))
        raise
    finally:
        baseline.save(args.output / "manifest.json", manifest)
        inventory(args.output)
    verify(args.output)


if __name__ == "__main__":
    main()
