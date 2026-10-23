#!/usr/bin/env python3
"""Interleaved frozen-baseline/current ReadIndex and large-live-state experiments.

Every request is checked. Cold snapshots contain 1/8/15 MiB of live values,
while eight clients update small disjoint hot keys. Each large-state run ends
with SIGKILL of all nodes, then a complete cold/hot value check after recovery.
All nodes/load generators share one host; this is a closed-loop comparison.
"""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import random
import socket
import statistics
import subprocess
import sys
import time

import cluster_matrix as matrix
import local_optimization as evidence
import rpc_baseline as rpc
import run_baseline as baseline

sys.path.insert(0, str(baseline.ROOT / "tests"))
import test_rpc_network as wire


def cold_value(index):
    return f"cold-{index}|".encode().ljust(8192, b"c")


def cold_key(index):
    return f"cold-{index:05d}".encode()


def digest_value(digest, key, value):
    digest.update(len(key).to_bytes(4, "big"))
    digest.update(key)
    digest.update(len(value).to_bytes(4, "big"))
    digest.update(value)


def expected_recovery_digest(run):
    digest = hashlib.sha256()
    for i in range(run["cold_mib"] * 128):
        digest_value(digest, cold_key(i), cold_value(i))
    for client in sorted(run["client_results"], key=lambda c: c["client_id"]):
        for i, version in enumerate(client["final_versions"]):
            key = f"client-{client['client_id']}-key-{i}".encode()
            value = matrix.value_for(client["client_id"], i, version, run["value_size"])
            digest_value(digest, key, value)
    return digest.hexdigest()


def prepare(cluster, config):
    count = config.get("cold_mib", 0) * 128
    if not count:
        return
    with socket.create_connection(("127.0.0.1", cluster.ports[cluster.leader]), 20) as connection:
        connection.settimeout(30)
        for first in range(0, count, 32):
            indexes = range(first, min(first + 32, count))
            connection.sendall(b"".join(wire.request(i + 1, "put", cold_key(i), cold_value(i))
                                         for i in indexes))
            for i in indexes:
                response = wire.receive_response(connection)
                if (response["id"], response["code"]) != (i + 1, 0):
                    raise AssertionError(f"cold prefill failed: {response}")


def validate(cluster, config, clients):
    count = config.get("cold_mib", 0) * 128
    if not count:
        return dict(checked=True, cold_keys=0, all_node_sigkill=False)
    before = cluster.states()
    restart_begin = time.monotonic()
    for process in cluster.processes:
        process.kill()
    for process in cluster.processes:
        process.wait(timeout=5)
    commands = json.loads((cluster.output / "commands.json").read_text())
    cluster.processes = [subprocess.Popen(command, stdout=log, stderr=log)
                         for command, log in zip(commands, cluster.logs)]
    deadline = time.monotonic() + 30
    while True:
        cluster.alive()
        try:
            states = cluster.states()
            leaders = [i for i, state in enumerate(states)
                       if state["role"] == "leader" and state["commit_index"] >= before[cluster.leader]["commit_index"]]
            if len(leaders) == 1:
                cluster.leader = leaders[0]
                break
        except (OSError, EOFError):
            pass
        if time.monotonic() > deadline:
            raise AssertionError("large-state restart did not converge")
        time.sleep(.05)
    ready_seconds = time.monotonic() - restart_begin
    checked = 0
    observed_digest = hashlib.sha256()
    with socket.create_connection(("127.0.0.1", cluster.ports[cluster.leader]), 20) as connection:
        connection.settimeout(30)

        def check_value(key, expected):
            nonlocal checked
            checked += 1
            connection.sendall(wire.request(checked, "get", key))
            response = wire.receive_response(connection)
            if (response["id"], response["code"], response["value"]) != (checked, 0, expected):
                raise AssertionError(f"recovered value differs: {key!r}")
            digest_value(observed_digest, key, response["value"])

        for i in range(count):
            check_value(cold_key(i), cold_value(i))
        for client in sorted(clients, key=lambda c: c["client_id"]):
            for i, version in enumerate(client["final_versions"]):
                key = f"client-{client['client_id']}-key-{i}".encode()
                expected = matrix.value_for(client["client_id"], i, version, config["value_size"])
                check_value(key, expected)
    return dict(checked=True, cold_keys=count, hot_keys=checked - count, all_node_sigkill=True,
                states_before_restart=before, states_after_restart=cluster.states(),
                acknowledged_final_values_checked=checked, recovered_values_sha256=observed_digest.hexdigest(),
                restart_to_committed_leader_seconds=ready_seconds,
                restart_to_all_values_checked_seconds=time.monotonic() - restart_begin)


def verify(root):
    # Reuse raw sample/timeline/file validation with this experiment's matrix.
    manifest = json.loads((root / "manifest.json").read_text())
    expected = manifest["repeats"] * len(manifest["profiles"]) * len(manifest["variants"])
    saved = evidence.VARIANTS
    try:
        evidence.VARIANTS = manifest["variants"]
        evidence.verify(root)
    finally:
        evidence.VARIANTS = saved
    if len(manifest["runs"]) != expected:
        raise AssertionError("missing experiment run")
    identities = [(r["profile"], r["variant"], r["repeat"]) for r in manifest["runs"]]
    required = {(p, v, n) for p in manifest["profiles"] for v in manifest["variants"]
                for n in range(1, manifest["repeats"] + 1)}
    if len(identities) != len(required) or set(identities) != required:
        raise AssertionError("missing or duplicate workload")
    for run in manifest["runs"]:
        variant = manifest["variants"][run["variant"]]
        if run["server_sha256"] != variant["server_sha256"]:
            raise AssertionError("run binary identity differs")
        recovery = run["recovery_validation"]
        if not recovery["checked"]:
            raise AssertionError("recovery not checked")
        if run.get("cold_mib"):
            if (not recovery["all_node_sigkill"] or recovery["cold_keys"] != run["cold_mib"] * 128 or
                    recovery["hot_keys"] != run["clients"] * run["keys_per_client"]):
                raise AssertionError("large-state recovery coverage differs")
            if recovery["recovered_values_sha256"] != expected_recovery_digest(run):
                raise AssertionError("recovered value digest differs")
            if not 0 < recovery["restart_to_committed_leader_seconds"] <= recovery["restart_to_all_values_checked_seconds"]:
                raise AssertionError("invalid recovery timing")
        if variant["options"][-1:] == ["read-index"] and run["profile"] == "read-only":
            if run["disk_bytes"] != 0 or run["disk_saves"] != 0:
                raise AssertionError("pure reads persisted data")
            if any(a["last_index"] != b["last_index"] for a, b in zip(run["after"], run["before"])):
                raise AssertionError("pure reads appended log entries")
        directory = root / evidence.run_name(run)
        samples, reads, writes = [], [], []
        for client in run["client_results"]:
            with gzip.open(directory / client["samples_file"], "rt") as source:
                raw = json.load(source)
            samples.extend(raw["latency_ns"])
            flags = raw["write_flags"]
            if (len(flags) != len(raw["latency_ns"]) or any(type(f) is not int or f not in (0, 1) for f in flags) or
                    sum(flags) != client["puts"]):
                raise AssertionError("raw operation mix differs")
            for latency, write in zip(raw["latency_ns"], flags):
                (writes if write else reads).append(latency)
        samples.sort()
        if (rpc.percentile(samples, .999) != run["p999_ms"] or max(samples) / 1e6 != run["max_ms"]):
            raise AssertionError("tail latency statistics differ")
        for name, values in (("read", reads), ("write", writes)):
            expected_p99 = rpc.percentile(sorted(values), .99) if values else None
            if run[f"{name}_p99_ms"] != expected_p99:
                raise AssertionError("per-operation tail latency differs")
    print(json.dumps(dict(followup_verified=True, runs=expected)))


def summarize(root, manifest):
    rows = ["# Local read scheduling / checkpoint follow-up", "",
            "Median of independent runs; same host, synchronous journal, fixed three voters.", "",
            "| Profile | Variant | ops/s | P99 ms | P99.9 ms | Max ms | Written B/op | Checkpoints (3 nodes) | Snapshot pause ms/op |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    summary = []
    for profile in manifest["profiles"]:
        for variant in manifest["variants"]:
            runs = [r for r in manifest["runs"] if r["profile"] == profile and r["variant"] == variant]
            row = dict(profile=profile, variant=variant)
            for name in ("ops_per_second", "p50_ms", "p95_ms", "p99_ms", "p999_ms", "max_ms",
                         "disk_checkpoint_saves", "server_average_cores"):
                row[name] = statistics.median(r[name] for r in runs)
            for name in ("read_p99_ms", "write_p99_ms"):
                row[name] = statistics.median(r[name] for r in runs) if runs[0][name] is not None else None
            row["bytes_per_operation"] = statistics.median(r["disk_bytes"] / r["operations"] for r in runs)
            row["snapshot_pause_ms_per_operation"] = statistics.median(
                r["snapshot_pause_ns"] / r["operations"] / 1e6 for r in runs)
            summary.append(row)
            rows.append(f"| {profile} | {variant} | {row['ops_per_second']:.1f} | {row['p99_ms']:.3f} | "
                        f"{row['p999_ms']:.3f} | {row['max_ms']:.3f} | {row['bytes_per_operation']:.1f} | "
                        f"{row['disk_checkpoint_saves']:.0f} | {row['snapshot_pause_ms_per_operation']:.4f} |")
    rows += ["", "All runs verify GET values. Large-state runs verify every cold/hot final value after all-node SIGKILL/restart.",
             "Raw samples, binary/source identities and exact commands are retained. No saturation or physical power-loss claim.",
             "Snapshot pause is unavailable (reported as zero) in the frozen baseline; it must not be treated as a zero-cost baseline.",
             "P99/P99.9/max are medians of run statistics, not merged-sample statistics. Prefill/recovery are outside the timing window."]
    baseline.save(root / "summary.json", summary)
    (root / "summary.md").write_text("\n".join(rows) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--node", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--suite", choices=("reads", "snapshots"), default="reads")
    parser.add_argument("--profiles")
    parser.add_argument("--duration-seconds", type=float, default=15)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.verify:
        verify(args.verify.resolve())
        return
    if (not args.baseline or not args.node or not args.output or args.repeats < 1 or args.workers < 1 or
            not math.isfinite(args.duration_seconds) or args.duration_seconds <= 0):
        parser.error("provide baseline/node/output and valid run size")
    args.node, args.baseline, args.output = args.node.resolve(), args.baseline.resolve(), args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    current = args.node
    matrix.PROFILES["read-only"] = dict(clients=8, write_percent=0, value_size=128, keys_per_client=8)
    for mib in (1, 8, 15):
        matrix.PROFILES[f"cold-{mib}MiB"] = dict(clients=8, write_percent=50, value_size=128,
                                                keys_per_client=8, cold_mib=mib)
    variants = {
        "baseline-read-index": dict(node=args.baseline, options=["--read-mode", "read-index"]),
        "current-read-index": dict(node=current, options=["--read-mode", "read-index"]),
        "current-log": dict(node=current, options=["--read-mode", "log"]),
    } if args.suite == "reads" else {
        "baseline-fixed": dict(node=args.baseline, options=[]),
        "current-fixed": dict(node=current, options=["--snapshot-policy", "fixed"]),
        "current-adaptive": dict(node=current, options=["--snapshot-policy", "adaptive"]),
    }
    profiles = (args.profiles or ("read-only,balanced,write-heavy,large-value,more-keys"
                                  if args.suite == "reads" else "cold-1MiB,cold-8MiB,cold-15MiB")).split(",")
    if any(profile not in matrix.PROFILES for profile in profiles):
        parser.error("unknown profile")
    for variant in variants.values():
        variant["server_sha256"] = baseline.digest(variant["node"])
        variant["node"] = str(variant["node"])
    args.snapshot_bytes = 1024 * 1024
    args.prepare_cluster, args.validate_cluster = prepare, validate
    manifest = dict(format_version=2, command=sys.argv, status="running", suite=args.suite,
                    profiles=profiles, repeats=args.repeats, duration_seconds=args.duration_seconds,
                    environment=baseline.environment(current), variants=variants, runs=[],
                    source_manifest_sha256=baseline.source_snapshot(args.output))
    baseline.save(args.output / "manifest.json", manifest)
    try:
        for repeat in range(1, args.repeats + 1):
            order = [(profile, variant) for profile in profiles for variant in variants]
            random.Random(20261022 + repeat).shuffle(order)
            for profile, variant in order:
                config = variants[variant]
                args.node, args.node_options, args.variant = Path(config["node"]), config["options"], variant
                result = matrix.run_one(args, profile, repeat, 32, 128)
                directory = args.output / evidence.run_name(result)
                samples, reads, writes = [], [], []
                for client in result["client_results"]:
                    with gzip.open(directory / client["samples_file"], "rt") as stream:
                        raw = json.load(stream)
                    samples.extend(raw["latency_ns"])
                    for latency, write in zip(raw["latency_ns"], raw["write_flags"]):
                        (writes if write else reads).append(latency)
                samples.sort()
                result.update(server_sha256=config["server_sha256"],
                              p999_ms=rpc.percentile(samples, .999), max_ms=max(samples) / 1e6,
                              read_p99_ms=rpc.percentile(sorted(reads), .99) if reads else None,
                              write_p99_ms=rpc.percentile(sorted(writes), .99) if writes else None)
                baseline.save(directory / "result.json", result)
                manifest["runs"].append(result)
                baseline.save(args.output / "manifest.json", manifest)
        for config in variants.values():
            if baseline.digest(Path(config["node"])) != config["server_sha256"]:
                raise AssertionError("binary changed during measurement")
        manifest["status"] = "complete"
        summarize(args.output, manifest)
    except BaseException as error:
        manifest.update(status="failed", error=repr(error))
        raise
    finally:
        baseline.save(args.output / "manifest.json", manifest)
        evidence.inventory(args.output)
    verify(args.output)


if __name__ == "__main__":
    main()
