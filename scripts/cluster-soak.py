#!/usr/bin/env python3
"""Repeat bounded checked histories while injecting real process/network faults.

Each epoch uses fresh keys. This keeps the exhaustive per-key oracle bounded
without dropping dependencies between operations on the same key.
"""
import argparse
import concurrent.futures
import json
import math
from pathlib import Path
import random
import sys
import threading
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "benchmarks"))
import test_cluster as harness
from linearizability import check
import run_baseline as baseline


def save(path, value):
    baseline.save(path, value)


def epoch(cluster, number, seed, output):
    history, attempts, faults = [], [], []
    lock = threading.Lock()
    barrier = threading.Barrier(4)
    leader = cluster.leader()
    fault_kind = ("leader-kill", "follower-partition", "directed-partition", "link-delay", "all-restart")[number % 5]
    started = time.monotonic()

    def client(client_id):
        generator = random.Random(seed + client_id)
        barrier.wait(timeout=5)
        for index in range(18):
            key = f"epoch-{number}-key-{index % 6}".encode()
            kind = generator.choice(("put", "put", "get", "del"))
            value = f"{number}:{client_id}:{index}".encode()
            begin = time.monotonic_ns()
            code, result = 6, None
            try:
                target = cluster.leader(timeout=4)
                response = cluster.call(target, kind, key, value, timeout=3)
                code = response["code"]
                result = response["value"].hex() if code == 0 else None
            except (OSError, EOFError):
                pass
            end = time.monotonic_ns()
            record = dict(client=client_id, kind=kind, key=key.hex(), value=value.hex(),
                          start=begin, end=end, code=code, result=result)
            model = dict(record)
            if kind == "get":
                include = code in (0, 1)
            else:
                include = code in (0, 6)
                if code == 6:
                    model.update(optional=True, end=float("inf"))
            with lock:
                attempts.append(record)
                if include:
                    history.append(model)
            time.sleep(generator.uniform(0.004, 0.012))

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        work = [pool.submit(client, i) for i in range(3)]
        barrier.wait(timeout=5)
        time.sleep(0.04)
        chosen = (leader + 1 + seed % 2) % 3
        faults.append(dict(kind=fault_kind, node=(leader if fault_kind == "leader-kill" else chosen) + 1,
                           started_ns=time.monotonic_ns()))
        if fault_kind == "leader-kill":
            cluster.kill(leader)
            time.sleep(0.35)
            cluster.start(leader)
        elif fault_kind == "follower-partition":
            cluster.blocked.add(chosen)
            time.sleep(0.65)
            cluster.blocked.clear()
        elif fault_kind == "directed-partition":
            cluster.blocked.update((leader, target) for target in range(3) if target != leader)
            time.sleep(0.85)
            cluster.blocked.clear()
        elif fault_kind == "link-delay":
            for source, target in cluster.proxies:
                if chosen in (source, target):
                    cluster.delays[source, target] = 0.04
            time.sleep(0.65)
            cluster.delays.clear()
        else:
            for node in range(3):
                cluster.kill(node)
            time.sleep(0.15)
            for node in range(3):
                cluster.start(node)
        faults[-1]["healed_ns"] = time.monotonic_ns()
        for future in work:
            future.result(timeout=45)
    # Persist before running the oracle, including on an oracle failure.
    trace = dict(epoch=number, seed=seed, fault=faults, attempts=attempts,
                 operations=[{**op, "end": None if op.get("optional") else op["end"]} for op in history])
    save(output / f"epoch-{number:05d}.json", trace)
    if len(history) < 12 or not check(history):
        raise AssertionError(f"history failed: epoch={number} seed={seed}")
    cluster.eventually_value(b"soak-durable", b"acknowledged-before-faults", timeout=15)
    states = [cluster.status(i) for i in range(3)]
    result = dict(epoch=number, seed=seed, fault=fault_kind, checked_operations=len(history),
                  attempted_operations=len(attempts), unknown_writes=sum(op.get("optional", False) for op in history),
                  elapsed_seconds=time.monotonic() - started,
                  observed_first_success_after_heal_seconds=(
                      min([op["end"] for op in attempts if op["code"] in (0, 1) and
                           op["end"] >= faults[-1]["healed_ns"]] or [time.monotonic_ns()]) -
                      faults[-1]["healed_ns"]) / 1e9,
                  epoch_completion_after_heal_seconds=(time.monotonic_ns() - faults[-1]["healed_ns"]) / 1e9,
                  states=states, checked=True)
    save(output / f"epoch-{number:05d}-result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--evidence-root", type=Path)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--duration-seconds", type=float, default=0)
    parser.add_argument("--seeds", default="771,56,20261021")
    args = parser.parse_args()
    seeds = [int(seed) for seed in args.seeds.split(",")]
    if args.epochs < 1 or not math.isfinite(args.duration_seconds) or args.duration_seconds < 0 or not seeds:
        parser.error("invalid run size")
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    if args.output:
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
    else:
        root = args.evidence_root or harness.NODE.parent.parent / "soak-evidence"
        root.mkdir(parents=True, exist_ok=True)
        output = Path(tempfile.mkdtemp(prefix="campaign-", dir=root)).resolve()
    runtime = output / "runtime"
    runtime.mkdir()
    manifest = dict(command=sys.argv, environment=baseline.environment(harness.NODE),
                    source_manifest_sha256=baseline.source_snapshot(output), status="running",
                    note="Three loopback processes on one host. Fresh per-epoch keys; exhaustive per-key oracle <=9 operations/key.")
    save(output / "manifest.json", manifest)
    results = []
    cluster = harness.Cluster(runtime, extra_args=["--snapshot-entries", "16"])
    started = time.monotonic()
    try:
        leader = cluster.leader()
        if cluster.call(leader, "put", b"soak-durable", b"acknowledged-before-faults")["code"] != 0:
            raise AssertionError("initial durable write failed")
        while (time.monotonic() - started < args.duration_seconds if args.duration_seconds else len(results) < args.epochs):
            number = len(results)
            result = epoch(cluster, number, seeds[number % len(seeds)], output)
            results.append(result)
            save(output / "progress.json", dict(epochs=len(results), elapsed_seconds=time.monotonic() - started,
                 checked_operations=sum(r["checked_operations"] for r in results), latest=result))
            if number % 10 == 0:
                print(json.dumps({k: result[k] for k in ("epoch", "fault", "checked_operations", "elapsed_seconds")}), flush=True)
        for node in range(3):
            cluster.kill(node)
        for node in range(3):
            cluster.start(node)
        cluster.eventually_value(b"soak-durable", b"acknowledged-before-faults", timeout=15)
        if baseline.digest(harness.NODE) != manifest["environment"]["server_sha256"]:
            raise AssertionError("binary changed during fault campaign")
        manifest["status"] = "complete"
    except BaseException as error:
        manifest.update(status="failed", error=repr(error))
        raise
    finally:
        cluster.close()
        manifest.update(elapsed_seconds=time.monotonic() - started, epochs=len(results),
                        attempted_operations=sum(r["attempted_operations"] for r in results),
                        checked_operations=sum(r["checked_operations"] for r in results),
                        unknown_writes=sum(r["unknown_writes"] for r in results))
        save(output / "manifest.json", manifest)
        save(output / "summary.json", {**manifest, "fault_counts": {
             name: sum(r["fault"] == name for r in results) for name in sorted({r["fault"] for r in results})}})
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
