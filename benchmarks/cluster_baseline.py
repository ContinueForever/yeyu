#!/usr/bin/env python3
"""Repeatable three-process A/B: proposal batch 1 vs 32, same binary and semantics."""
import argparse
import json
from pathlib import Path
import socket
import statistics
import struct
import subprocess
import sys
import time
from types import SimpleNamespace

import rpc_baseline as rpc
import run_baseline as baseline


def status(port):
    with socket.create_connection(("127.0.0.1", port), timeout=1) as sock:
        sock.settimeout(2)
        request = b"\x08\x01" + rpc.bytes_field(6, b"")
        sock.sendall(struct.pack("!I", len(request)) + request)
        length, = struct.unpack("!I", rpc.read_exact(sock, 4))
        if not 0 < length <= 65536:
            raise ValueError("invalid status frame")
        response = rpc.decode_response(rpc.read_exact(sock, length))
        if response.get(2, 0) != 0:
            raise RuntimeError(response)
        return json.loads(response[3])


def run(args, batch, repeat):
    output = args.output / f"batch{batch}-run{repeat}"
    output.mkdir()
    reservations = [socket.socket() for _ in range(3)]
    for sock in reservations:
        sock.bind(("127.0.0.1", 0))
    ports = [sock.getsockname()[1] for sock in reservations]
    for sock in reservations:
        sock.close()
    members = ",".join(f"{i + 1}@127.0.0.1:{port}" for i, port in enumerate(ports))
    processes, logs, commands = [], [], []
    try:
        for i, port in enumerate(ports):
            command = [str(args.node), "--id", str(i + 1), "--port", str(port), "--members", members,
                       "--data-dir", str(output / f"node-{i + 1}"),
                       "--proposal-batch-size", str(batch), "--request-timeout-ms", "5000"]
            commands.append(command)
            log = (output / f"node-{i + 1}.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen(command, stdout=log, stderr=log))
        baseline.save(output / "commands.json", commands)
        deadline = time.monotonic() + 8
        while True:
            try:
                before = [status(port) for port in ports]
                leaders = [i for i, state in enumerate(before)
                           if state["role"] == "leader" and state["commit_index"] >= 1]
                if len(leaders) == 1:
                    leader = leaders[0]
                    break
            except (OSError, EOFError, ConnectionError):
                pass
            if time.monotonic() > deadline or any(p.poll() is not None for p in processes):
                raise RuntimeError(f"benchmark cluster startup failed: {output}")
            time.sleep(0.02)
        workload = SimpleNamespace(host="127.0.0.1", port=ports[leader], clients=args.clients,
                                   timeout=5, value_size=128, warmup_ops=20,
                                   ops_per_client=args.ops_per_client, write_percent=50)
        latencies, elapsed, writes = rpc.measure(workload)
        time.sleep(0.15)
        after = [status(port) for port in ports]
        if any(state["term"] != before[leader]["term"] for state in after):
            raise RuntimeError("term changed during benchmark; retain logs and reject run")
        sorted_samples = sorted(latencies)
        metrics = {name: sum(a[name] - b[name] for a, b in zip(after, before))
                   for name in ("disk_saves", "disk_bytes", "disk_persist_ns")}
        result = dict(batch=batch, repeat=repeat, measured_operations=len(latencies),
                      writes=writes, clients=args.clients, elapsed_seconds=elapsed,
                      ops_per_second=len(latencies) / elapsed,
                      p50_ms=rpc.percentile(sorted_samples, 0.50),
                      p95_ms=rpc.percentile(sorted_samples, 0.95),
                      p99_ms=rpc.percentile(sorted_samples, 0.99),
                      metrics_include_prepopulate_and_warmup=True,
                      total_operations_including_warmup=len(latencies) + args.clients * 21,
                      before=before, after=after, **metrics)
        baseline.save(output / "samples-ns.json", latencies)
        baseline.save(output / "result.json", result)
        print(json.dumps({k: result[k] for k in ("batch", "repeat", "ops_per_second", "p99_ms", "disk_saves")}))
        return result
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
                raise RuntimeError("benchmark node failed graceful shutdown")
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--ops-per-client", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.clients <= 64 or args.ops_per_client < 1 or args.repeats < 1:
        parser.error("invalid workload size")
    args.node, args.output = args.node.resolve(), args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    source_digest = baseline.source_snapshot(args.output)
    baseline.save(args.output / "environment.json", baseline.environment(args.node))
    baseline.save(args.output / "manifest.json", dict(command=sys.argv, source_manifest_sha256=source_digest,
                  git=rpc.git_state(), note="Three local processes, shared VM/filesystem; all reads use Raft log commitment."))
    results = []
    for repeat in range(1, args.repeats + 1):
        for batch in ([1, 32] if repeat % 2 else [32, 1]):
            results.append(run(args, batch, repeat))
    summary = []
    for batch in (1, 32):
        selected = [r for r in results if r["batch"] == batch]
        summary.append(dict(batch=batch, repeats=len(selected), **{
            key: statistics.median(r[key] for r in selected)
            for key in ("ops_per_second", "p50_ms", "p95_ms", "p99_ms", "disk_saves", "disk_bytes")}))
    baseline.save(args.output / "summary.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
