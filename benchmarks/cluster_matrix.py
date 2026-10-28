#!/usr/bin/env python3
"""Timed multiprocess load matrix, with checked values and retained raw evidence.

All three nodes and load generators share one host. This measures the current
implementation under stated workloads, not a production saturation limit.
"""
import argparse
import concurrent.futures
import gzip
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import queue
import random
import socket
import statistics
import subprocess
import sys
import threading
import time

import cluster_baseline as cluster_rpc
import rpc_baseline as rpc
import run_baseline as baseline

PROFILES = {
    "single-client": dict(clients=1, write_percent=50, value_size=128, keys_per_client=8),
    "read-heavy": dict(clients=8, write_percent=10, value_size=128, keys_per_client=8),
    "balanced": dict(clients=8, write_percent=50, value_size=128, keys_per_client=8),
    "write-heavy": dict(clients=8, write_percent=90, value_size=128, keys_per_client=8),
    "large-value": dict(clients=8, write_percent=50, value_size=8192, keys_per_client=8),
    "more-keys": dict(clients=8, write_percent=50, value_size=128, keys_per_client=128),
    "more-clients": dict(clients=16, write_percent=50, value_size=128, keys_per_client=8),
}


class StartClock:
    def __init__(self, clock, event):
        self.clock, self.event = clock, event

    def __call__(self):
        self.clock.value = time.perf_counter_ns()
        self.event.set()


def value_for(client_id, key_id, version, size):
    return f"{client_id}/{key_id}/{version}|".encode().ljust(size, b"v")[:size]


def load_client(client_id, config, port, barrier, clock, ready):
    generator = random.Random(1701 + client_id)
    versions = [0] * config["keys_per_client"]
    samples, offsets, write_flags, puts, gets, request_id = [], [], [], 0, 0, 0
    keys = [f"client-{client_id}-key-{i}".encode() for i in range(len(versions))]
    try:
        with socket.create_connection(("127.0.0.1", port), 20) as connection:
            connection.settimeout(20)
            for i, key in enumerate(keys):
                request_id += 1
                rpc.rpc(connection, request_id, "put", key, value_for(client_id, i, 0, config["value_size"]))
            for _ in range(20):
                i = generator.randrange(len(keys))
                request_id += 1
                rpc.rpc(connection, request_id, "get", keys[i], value_for(client_id, i, 0, config["value_size"]))
            ready.put(("ready", client_id))
            barrier.wait(timeout=120)
            measurement_begin = clock.value
            deadline = measurement_begin + int(config["duration_seconds"] * 1e9)
            while time.perf_counter_ns() < deadline:
                i = generator.randrange(len(keys))
                operation = "put" if generator.randrange(100) < config["write_percent"] else "get"
                if operation == "put":
                    versions[i] += 1
                request_id += 1
                value = value_for(client_id, i, versions[i], config["value_size"])
                begin = time.perf_counter_ns()
                rpc.rpc(connection, request_id, operation, keys[i], value)
                completed = time.perf_counter_ns()
                samples.append(completed - begin)
                offsets.append(completed - measurement_begin)
                write_flags.append(int(operation == "put"))
                puts += operation == "put"
                gets += operation == "get"
            finished = time.perf_counter_ns()
        return dict(client_id=client_id, samples=samples, offsets=offsets,
                    write_flags=write_flags, puts=puts, gets=gets, finished_ns=finished,
                    final_versions=versions)
    except BaseException as error:
        ready.put(("error", repr(error)))
        barrier.abort()
        raise


def load_process(ids, config, port, barrier, clock, started, ready, done, output):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ids)) as pool:
            work = [pool.submit(load_client, i, config, port, barrier, clock, ready) for i in ids]
            if not started.wait(timeout=120):
                raise TimeoutError("load start barrier timed out")
            cpu_begin = time.process_time_ns()
            results = [future.result() for future in work]
            cpu_seconds = (time.process_time_ns() - cpu_begin) / 1e9
        for result in results:
            samples = result.pop("samples")
            offsets = result.pop("offsets")
            write_flags = result.pop("write_flags")
            name = f"client-{result['client_id']}-samples-ns.json.gz"
            with gzip.open(Path(output) / name, "wt") as target:
                json.dump(dict(latency_ns=samples, completed_offset_ns=offsets,
                               write_flags=write_flags), target)
            result.update(samples_file=name, operations=len(samples))
        done.put(dict(results=results, cpu_seconds=cpu_seconds))
    except BaseException as error:
        done.put(dict(error=repr(error)))
        ready.put(("error", repr(error)))


def proc_stats(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return dict(pid=pid, cpu_seconds=(int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                    rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE"))
    except (FileNotFoundError, ProcessLookupError):
        return dict(pid=pid, cpu_seconds=0, rss_bytes=0)


class LocalCluster:
    def __init__(self, node, output, batch, snapshot_entries, snapshot_bytes, extra_args=None):
        self.output = output
        self.processes, self.logs = [], []
        reservations = [socket.socket() for _ in range(3)]
        for connection in reservations:
            connection.bind(("127.0.0.1", 0))
        self.ports = [connection.getsockname()[1] for connection in reservations]
        for connection in reservations:
            connection.close()
        members = ",".join(f"{i + 1}@127.0.0.1:{port}" for i, port in enumerate(self.ports))
        commands = []
        try:
            for i, port in enumerate(self.ports):
                command = [str(node), "--id", str(i + 1), "--port", str(port), "--members", members,
                           "--data-dir", str(output / f"node-{i + 1}"), "--proposal-batch-size", str(batch),
                           "--snapshot-entries", str(snapshot_entries), "--snapshot-bytes", str(snapshot_bytes),
                           "--request-timeout-ms", "10000"]
                command.extend(extra_args or [])
                commands.append(command)
                log = (output / f"node-{i + 1}.log").open("w")
                self.logs.append(log)
                self.processes.append(subprocess.Popen(command, stdout=log, stderr=log))
            baseline.save(output / "commands.json", commands)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                self.alive()
                try:
                    states = self.states()
                    leaders = [i for i, state in enumerate(states) if state["role"] == "leader" and state["commit_index"]]
                    if len(leaders) == 1:
                        self.leader = leaders[0]
                        return
                except (OSError, EOFError):
                    pass
                time.sleep(0.05)
            raise RuntimeError("cluster did not elect a leader")
        except BaseException:
            self.close()
            raise

    def alive(self):
        if any(process.poll() is not None for process in self.processes):
            raise RuntimeError(f"benchmark node exited; inspect {self.output}")

    def states(self):
        return [cluster_rpc.status(port) for port in self.ports]

    def close(self):
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in self.logs:
            log.close()


def run_one(args, profile, repeat, batch, snapshot_entries):
    variant = getattr(args, "variant", "")
    tag = f"{variant}-" if variant else ""
    output = args.output / f"{tag}{profile}-batch{batch}-snap{snapshot_entries}-run{repeat}"
    output.mkdir()
    config = dict(PROFILES[profile], duration_seconds=args.duration_seconds,
                  snapshot_entries=snapshot_entries, snapshot_bytes=args.snapshot_bytes)
    baseline.save(output / "workload.json", config)
    cluster = LocalCluster(args.node, output, batch, snapshot_entries, args.snapshot_bytes,
                           getattr(args, "node_options", None))
    context = mp.get_context("spawn")
    clock, started = context.Value("q", 0), context.Event()
    ready, done = context.Queue(), context.Queue()
    barrier = context.Barrier(config["clients"] + 1, action=StartClock(clock, started))
    workers = min(args.workers, config["clients"])
    processes = [context.Process(target=load_process, args=(list(range(i, config["clients"], workers)),
                 config, cluster.ports[cluster.leader], barrier, clock, started, ready, done, str(output)))
                 for i in range(workers)]
    stop, samples = threading.Event(), []

    def sample():
        while not stop.is_set():
            samples.append(dict(timestamp_ns=time.perf_counter_ns(),
                           nodes=[proc_stats(p.pid) for p in cluster.processes],
                           clients=[proc_stats(p.pid) for p in processes],
                           state_file_bytes=[(output / f"node-{i + 1}" / "raft.state").stat().st_size
                                             for i in range(3)]))
            stop.wait(0.25)

    sampler = None
    try:
        prepare = getattr(args, "prepare_cluster", None)
        if prepare:
            prepare(cluster, config)
        for process in processes:
            process.start()
        for _ in range(config["clients"]):
            kind, value = ready.get(timeout=120)
            if kind != "ready":
                raise RuntimeError(f"load prefill failed: {value}")
        prefill_deadline = time.monotonic() + 15
        while True:
            before = cluster.states()
            if all(state["applied_index"] == before[cluster.leader]["commit_index"] for state in before):
                break
            if time.monotonic() > prefill_deadline:
                raise RuntimeError("prefill did not converge before measurement")
            time.sleep(0.05)
        cpu_before = [proc_stats(p.pid) for p in cluster.processes]
        sampler = threading.Thread(target=sample)
        sampler.start()
        barrier.wait(timeout=30)
        results = [done.get(timeout=args.duration_seconds + 45) for _ in processes]
        if any("error" in result for result in results):
            raise RuntimeError(f"load generator failed: {results}")
        stop.set()
        sampler.join()
        cluster.alive()
        # Include replication drain in disk counters; exclude it from latencies.
        drain_deadline = time.monotonic() + 10
        while True:
            after = cluster.states()
            if all(state["applied_index"] == after[cluster.leader]["commit_index"] for state in after):
                break
            if time.monotonic() > drain_deadline:
                raise RuntimeError("followers did not drain after workload")
            time.sleep(0.05)
        if any(state["term"] != before[cluster.leader]["term"] for state in after):
            raise RuntimeError("term changed during stable-load benchmark")
        cpu_after = [proc_stats(p.pid) for p in cluster.processes]
        clients = [client for result in results for client in result["results"]]
        latencies, windows = [], {}
        for client in clients:
            with gzip.open(output / client["samples_file"], "rt") as source:
                raw = json.load(source)
                latencies.extend(raw["latency_ns"])
                for latency, offset in zip(raw["latency_ns"], raw["completed_offset_ns"]):
                    windows.setdefault(offset // 1000000000, []).append(latency)
        if not latencies or any(client["operations"] == 0 for client in clients):
            raise RuntimeError("empty measured client workload")
        elapsed = (max(client["finished_ns"] for client in clients) - clock.value) / 1e9
        timeline = []
        for second, values in sorted(windows.items()):
            values.sort()
            window_seconds = min(1.0, elapsed - second)
            timeline.append(dict(start_seconds=second, window_seconds=window_seconds,
                            operations=len(values), ops_per_second=len(values) / window_seconds,
                            p50_ms=rpc.percentile(values, .50), p99_ms=rpc.percentile(values, .99)))
        latencies.sort()
        server_cpu = sum(a["cpu_seconds"] - b["cpu_seconds"] for a, b in zip(cpu_after, cpu_before))
        client_cpu = sum(result["cpu_seconds"] for result in results)
        metric_names = ("disk_saves", "disk_bytes", "disk_persist_ns", "snapshots_saved", "snapshots_installed",
                        "disk_encode_ns", "disk_sync_ns", "disk_write_ns", "disk_file_syncs", "disk_directory_syncs",
                        "disk_checkpoint_saves", "disk_journal_saves", "read_index_completed", "read_quorum_rounds")
        metric_names += ("read_overlapped_requests", "append_requests", "read_append_requests",
                         "read_append_with_entries", "snapshot_pause_ns")
        metrics = {name: sum(a.get(name, 0) - b.get(name, 0) for a, b in zip(after, before))
                   for name in metric_names}
        result = dict(profile=profile, repeat=repeat, batch_size=batch, **config,
                      operations=len(latencies), completed_puts=sum(client["puts"] for client in clients),
                      completed_gets=sum(client["gets"] for client in clients), failures=0,
                      elapsed_seconds=elapsed, ops_per_second=len(latencies) / elapsed,
                      p50_ms=rpc.percentile(latencies, .50), p95_ms=rpc.percentile(latencies, .95),
                      p99_ms=rpc.percentile(latencies, .99), server_cpu_seconds=server_cpu,
                      server_average_cores=server_cpu / elapsed, client_cpu_seconds=client_cpu,
                      client_average_cores=client_cpu / elapsed, client_processes=workers,
                      peak_server_rss_bytes=max(sum(p["rss_bytes"] for p in sample["nodes"]) for sample in samples),
                      peak_client_rss_bytes=max(sum(p["rss_bytes"] for p in sample["clients"]) for sample in samples),
                      max_state_bytes=max(max(sample["state_file_bytes"]) for sample in samples),
                      before=before, after=after, disk_metrics_include_post_load_replication_drain=True,
                      metrics_exclude_preload_and_warmup=True, client_results=clients, **metrics)
        baseline.save(output / "resource-samples.json", samples)
        if variant:
            result["variant"] = variant
        validate = getattr(args, "validate_cluster", None)
        if validate:
            result["recovery_validation"] = validate(cluster, config, clients)
        baseline.save(output / "timeline.json", timeline)
        baseline.save(output / "result.json", result)
        print(json.dumps({k: result[k] for k in ("profile", "repeat", "batch_size", "operations", "ops_per_second", "p99_ms")}), flush=True)
        return result
    except BaseException as error:
        baseline.save(output / "failure.json", dict(error=repr(error), resource_samples=samples))
        raise
    finally:
        stop.set()
        if sampler:
            sampler.join(timeout=5)
        barrier.abort()
        for process in processes:
            if process.pid:
                process.join(timeout=5)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
        cluster.close()
        ready.close()
        done.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profiles", default=",".join(PROFILES))
    parser.add_argument("--batch-sizes", default="32")
    parser.add_argument("--duration-seconds", type=float, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--snapshot-entries", default="128", help="comma-separated entry thresholds for A/B")
    parser.add_argument("--snapshot-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()
    profiles, batches = args.profiles.split(","), [int(size) for size in args.batch_sizes.split(",")]
    entry_limits = [int(size) for size in args.snapshot_entries.split(",")]
    if (any(profile not in PROFILES for profile in profiles) or not math.isfinite(args.duration_seconds) or args.duration_seconds <= 0 or
            args.repeats < 1 or args.workers < 1 or any(not 1 <= n <= 1000000000 for n in entry_limits) or
            not 1 <= args.snapshot_bytes <= 64 * 1024 * 1024 or
            any(not 1 <= size <= 32 for size in batches)):
        parser.error("invalid workload configuration")
    args.node, args.output = args.node.resolve(), args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = dict(format_version=2, command=sys.argv, environment=baseline.environment(args.node),
                    source_manifest_sha256=baseline.source_snapshot(args.output), status="running", runs=[],
                    note="Same-host three-process cluster, multiprocess clients, one in-flight request/connection, disjoint per-client keys. Reads commit through Raft log. Failed runs are retained and abort the experiment.")
    baseline.save(args.output / "manifest.json", manifest)
    try:
        for repeat in range(1, args.repeats + 1):
            order = [(profile, batch, entry_limit) for profile in profiles for batch in batches
                     for entry_limit in entry_limits]
            random.Random(2100 + repeat).shuffle(order)
            for profile, batch, entry_limit in order:
                result = run_one(args, profile, repeat, batch, entry_limit)
                manifest["runs"].append(result)
                baseline.save(args.output / "manifest.json", manifest)
        if baseline.digest(args.node) != manifest["environment"]["server_sha256"]:
            raise RuntimeError("binary changed during benchmark")
        manifest["status"] = "complete"
    except BaseException as error:
        manifest.update(status="failed", error=repr(error))
        raise
    finally:
        baseline.save(args.output / "manifest.json", manifest)
    summary = []
    for profile in profiles:
        for batch in batches:
            for entry_limit in entry_limits:
                runs = [run for run in manifest["runs"] if run["profile"] == profile and
                        run["batch_size"] == batch and run["snapshot_entries"] == entry_limit]
                summary.append(dict(profile=profile, batch_size=batch, snapshot_entries=entry_limit,
                               snapshot_bytes=args.snapshot_bytes, repeats=len(runs), **PROFILES[profile],
                               measured_operations=sum(run["operations"] for run in runs), failures=0,
                               min_ops_per_second=min(run["ops_per_second"] for run in runs),
                               max_ops_per_second=max(run["ops_per_second"] for run in runs), **{
                                   key: statistics.median(run[key] for run in runs) for key in
                                   ("ops_per_second", "p50_ms", "p95_ms", "p99_ms", "disk_bytes", "disk_saves",
                                    "server_average_cores", "client_average_cores", "peak_server_rss_bytes",
                                    "snapshots_saved", "max_state_bytes")}))
    baseline.save(args.output / "summary.json", summary)
    rows = ["# Same-host snapshot KV load matrix", "",
            "| Profile | Batch | Snapshot entries | Clients | PUT % | Value B | Keys | Median ops/s | Median run P99 ms | Server cores | Client cores |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in summary:
        rows.append(f"| {row['profile']} | {row['batch_size']} | {row['snapshot_entries']} | {row['clients']} | {row['write_percent']} | "
                    f"{row['value_size']} | {row['clients'] * row['keys_per_client']} | {row['ops_per_second']:.1f} | "
                    f"{row['p99_ms']:.3f} | {row['server_average_cores']:.2f} | {row['client_average_cores']:.2f} |")
    rows += ["", "P99 is the median of run P99s, not a pooled percentile. CPU cores are CPU seconds / measured wall time;",
             "node CPU and disk counters include a short replication drain. RSS is sampled every 250 ms.",
             "Preload and 20 warmup GETs per client are excluded. Every GET verifies the latest value for that client's key.",
             "Client CPU is process CPU across load threads; raw latency_ns/completed_offset_ns are saved as gzip JSON.",
             "timeline.json groups completions by second and retains throughput and latency trends within each run.",
             "Processes share the same host, CPU and filesystem. These closed-loop loads do not establish saturation.",
             "See manifest.json, source snapshot, commands, workload, resource-samples and all per-run results."]
    (args.output / "summary.md").write_text("\n".join(rows) + "\n")


if __name__ == "__main__":
    main()
