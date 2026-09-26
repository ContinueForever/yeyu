#!/usr/bin/env python3
"""Run a local RPC matrix and retain source identity, raw samples and logs."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import statistics
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parent.parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def source_snapshot(output):
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
    ).decode().split("\0")
    manifest = {}
    with tarfile.open(output / "source.tar.gz", "w:gz") as archive:
        for name in sorted(set(paths)):
            path = ROOT / name
            # Results and old result snapshots are never inputs to the build.
            if not name or name.startswith("benchmarks/results/") or not path.is_file():
                continue
            if output == path or output in path.parents:
                continue
            manifest[name] = digest(path)
            archive.add(path, arcname=name)
    save(output / "source-manifest.json", manifest)
    return digest(output / "source-manifest.json")


def environment(server):
    cpu_model = "unknown"
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            cpu_model = line.split(":", 1)[1].strip()
            break
    cache = server.parent.parent / "CMakeCache.txt"
    build = {}
    if cache.exists():
        for line in cache.read_text().splitlines():
            if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:",
                                "CMAKE_CXX_FLAGS", "YEWUKV_ENABLE_SANITIZERS:")):
                key, value = line.split("=", 1)
                build[key.split(":", 1)[0]] = value
    compiler = build.get("CMAKE_CXX_COMPILER")
    return {
        "platform": platform.platform(), "cpu_model": cpu_model,
        "logical_cpus": os.cpu_count(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "memory": Path("/proc/meminfo").read_text().splitlines()[0],
        "python": sys.version, "load_average": os.getloadavg(),
        "server_sha256": digest(server), "build": build,
        "compiler_version": subprocess.check_output([compiler, "--version"], text=True)
        if compiler else None,
    }


def run_one(args, output, threads, clients, repeat):
    name = f"io{threads}-clients{clients}-run{repeat}"
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    server_command = [str(args.server), "--port", str(port), "--protocol", "rpc",
                      "--io-threads", str(threads), "--idle-timeout", "60",
                      "--data-dir", str(output / f"{name}-data")]
    client_command = [sys.executable, str(ROOT / "benchmarks/rpc_baseline.py"),
                      "--port", str(port), "--clients", str(clients),
                      "--ops-per-client", str(args.ops_per_client), "--warmup-ops", "100",
                      "--value-size", "128", "--write-percent", "50",
                      "--output", str(output / f"{name}.json"),
                      "--samples-output", str(output / f"{name}-samples.json")]
    # Persist commands even if this run fails. Each run gets an isolated WAL.
    save(output / f"{name}-commands.json", {"server": server_command, "client": client_command})
    with (output / f"{name}-server.txt").open("w") as log:
        process = subprocess.Popen(server_command, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"server exited: see {name}-server.txt")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("server startup timed out")
                    time.sleep(0.02)
            with (output / f"{name}-client.txt").open("w") as client_log:
                subprocess.run(client_command, stdout=client_log, stderr=subprocess.STDOUT,
                               check=True, timeout=120)
            if process.poll() is not None:
                raise RuntimeError("server exited during benchmark")
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.returncode != 0:
            raise RuntimeError(f"server shutdown failed: {process.returncode}")
    result = json.loads((output / f"{name}.json").read_text())
    result.update(io_threads=threads, repeat=repeat, raw_file=f"{name}.json")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, default=ROOT / "build/bin/yewukv-server")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ops-per-client", type=int, default=5000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.ops_per_client < 1 or args.repeats < 1:
        parser.error("ops-per-client and repeats must be positive")
    args.server = args.server.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)  # Never overwrite a prior experiment.
    manifest = {
        "version": 1, "started_utc": datetime.now(timezone.utc).isoformat(),
        "command": sys.argv, "environment": environment(args.server),
        "git_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
        "source_manifest_sha256": source_snapshot(output),
        "status": "running", "runs": [],
    }
    save(output / "manifest.json", manifest)
    try:
        # Interleave configurations between repeats to reduce ordering bias.
        for repeat in range(1, args.repeats + 1):
            for threads, clients in ((0, 1), (4, 1), (0, 8), (4, 8)):
                result = run_one(args, output, threads, clients, repeat)
                manifest["runs"].append(result)
                save(output / "manifest.json", manifest)
                print(f"io={threads} clients={clients} repeat={repeat}: "
                      f"{result['throughput_ops_per_second']} ops/s, "
                      f"P99={result['latency_ms']['p99']} ms", flush=True)
        if digest(args.server) != manifest["environment"]["server_sha256"]:
            raise RuntimeError("server binary changed during the experiment")
        manifest["status"] = "complete"
    except BaseException as error:
        manifest["status"] = "failed"
        manifest["error"] = repr(error)
        raise
    finally:
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save(output / "manifest.json", manifest)
    rows = ["# Local RPC development baseline", "", "| I/O threads | Clients | Median ops/s | Min–max ops/s | Median run P99 (ms) |",
            "| --- | --- | --- | --- | --- |"]
    for threads, clients in ((0, 1), (4, 1), (0, 8), (4, 8)):
        runs = [r for r in manifest["runs"] if r["io_threads"] == threads and r["clients"] == clients]
        rates = [r["throughput_ops_per_second"] for r in runs]
        p99 = statistics.median(r["latency_ms"]["p99"] for r in runs)
        rows.append(f"| {threads} | {clients} | {statistics.median(rates):.1f} | "
                    f"{min(rates):.1f}–{max(rates):.1f} | {p99:.3f} |")
    rows += ["", "Same-host Python threaded closed-loop load; one in-flight RPC and one hot key",
             "per connection. 128-byte values, requested 50% PUT / 50% GET, 100 warmup",
             "operations per connection. Samples exclude preload, warmup and socket teardown.",
             "These results do not establish server saturation, storage or cluster performance.",
             "P99 column is the median of run P99s, not a pooled percentile.",
             "", "See manifest.json for environment, build, source identity and each run.",
             "source.tar.gz captures the dirty working-tree inputs; source-manifest.json",
             "contains their SHA-256 hashes. Each run retains nanosecond latency samples."]
    (output / "summary.md").write_text("\n".join(rows) + "\n")


if __name__ == "__main__":
    main()
