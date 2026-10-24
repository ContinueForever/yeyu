#!/usr/bin/env python3
"""Measure an existing three-node cluster; never start, stop or replace a node.

Multiprocess, closed-loop clients use persistent RPC connections to the discovered
leader. Any failed or ambiguous request rejects the run, with no replay. Samples,
failed runs and tool inputs remain on disk for later inspection.
"""

import argparse
import concurrent.futures
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import ipaddress
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import platform
import random
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import uuid

import rpc_baseline as rpc


PROFILES = {"read-only": 0, "balanced": 50, "write-heavy": 90}
PERCENTILES = {"p50_ms": .5, "p95_ms": .95, "p99_ms": .99, "p999_ms": .999}
PROTECTED = ("cloud-check", "failover-check", "rejoin-check", "reboot-check")


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def now():
    return datetime.now(timezone.utc).isoformat()


def query(endpoint, operation, key=b"", timeout=5, allow_missing=False):
    field = {"get": 3, "status": 6}[operation]
    command = rpc.bytes_field(1, key) if operation == "get" else b""
    request = b"\x08\x01" + rpc.bytes_field(field, command)
    with socket.create_connection(endpoint, timeout=timeout) as connection:
        connection.sendall(struct.pack("!I", len(request)) + request)
        length, = struct.unpack("!I", rpc.read_exact(connection, 4))
        if not 0 < length <= 65536:
            raise ValueError("invalid diagnostic frame")
        response = rpc.decode_response(rpc.read_exact(connection, length))
    if response.get(1) == 1 and allow_missing and response.get(2, 0) == 1:
        return None
    if response.get(1) != 1 or response.get(2, 0) != 0:
        raise RuntimeError(f"{endpoint}: {response!r}")
    return json.loads(response[3]) if operation == "status" else response.get(3, b"")


def protected_values(endpoint, timeout):
    values = {}
    for key in PROTECTED:
        value = query(endpoint, "get", key.encode(), timeout, allow_missing=True)
        values[key] = value.hex() if value is not None else None
    return values


def local_environment(client_id):
    def command(*args):
        result = subprocess.run(args, text=True, capture_output=True, timeout=10, check=False)
        return dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
    data = dict(cpuinfo=Path("/proc/cpuinfo").read_text().split("\n\n")[0],
                meminfo=Path("/proc/meminfo").read_text(),
                os_release=Path("/etc/os-release").read_text(),
                boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    repository = str(Path(__file__).resolve().parents[1])
    data["tool_git_head"] = command("git", "-C", repository, "rev-parse", "HEAD")
    data["tool_git_status"] = command("git", "-C", repository, "status", "--short")
    if client_id:
        unit = f"yewukv-node@{client_id}.service"
        data["unit"] = command("systemctl", "show", unit, "--no-pager",
                               "--property=MainPID,ActiveState,ExecStart,User,FragmentPath")
        data["local_server_sha256"] = hashlib.sha256(Path("/usr/local/bin/yewukv-node").read_bytes()).hexdigest()
        data["local_node_env"] = Path(f"/etc/yewukv/node-{client_id}.env").read_text()
    return data


def cluster_state(endpoints, timeout, expected=None):
    states = [query(endpoint, "status", timeout=timeout) for endpoint in endpoints]
    leaders = [i for i, state in enumerate(states) if state["role"] == "leader"]
    if len(leaders) != 1 or len({s["id"] for s in states}) != 3:
        raise RuntimeError("expected three different nodes and exactly one leader")
    leader = leaders[0]
    identity = (states[leader]["term"], states[leader]["id"])
    if expected is not None and identity != tuple(expected):
        raise RuntimeError(f"leader/term changed: {expected} -> {identity}")
    if any((s["term"], s["leader_id"]) != identity for s in states):
        raise RuntimeError("nodes disagree about term or leader")
    return states, leader, identity


def settled(endpoints, timeout, expected=None):
    deadline = time.monotonic() + 15
    while True:
        states, leader, identity = cluster_state(endpoints, timeout, expected)
        index = states[leader]["commit_index"]
        if all(s["last_index"] == s["commit_index"] == s["applied_index"] == index
               and s["pending"] == 0 for s in states):
            return states, leader, identity
        if time.monotonic() >= deadline:
            raise RuntimeError("replicas did not converge within 15 seconds")
        time.sleep(.1)


def value_for(client, key, version, size):
    return f"{client}/{key}/{version}|".encode().ljust(size, b"v")[:size]


def client_worker(client, config, endpoint, barrier, clock, abort, ready, output):
    shared = config.get("shared_read_keys", 0)
    value_client = 0 if shared else client
    samples, versions = [], [0] * (shared or config["keys_per_client"])
    keys = ([f"{config['prefix']}-shared-{i}".encode() for i in range(shared)] if shared else
            [f"{config['prefix']}-{client}-{i}".encode() for i in range(len(versions))])
    generator = random.Random(config["seed"] + client)
    result = dict(client=client, status="failed", phase="connect", failures=0)
    request_id = 0
    try:
        with socket.create_connection(endpoint, timeout=config["timeout_seconds"]) as connection:
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            connection.settimeout(config["timeout_seconds"])
            result["local_endpoint"] = list(connection.getsockname())
            result["phase"] = "preload"
            if not shared:
                for i, key in enumerate(keys):
                    request_id += 1
                    rpc.rpc(connection, request_id, "put", key,
                            value_for(client, i, 0, config["value_size"]))
            result["phase"] = "warmup"
            warmup_deadline = time.perf_counter() + config["warmup_seconds"]
            while time.perf_counter() < warmup_deadline and not abort.is_set():
                i = generator.randrange(len(keys))
                operation = "put" if generator.randrange(100) < config["write_percent"] else "get"
                version = versions[i] + (operation == "put")
                request_id += 1
                rpc.rpc(connection, request_id, operation, keys[i],
                        value_for(value_client, i, version, config["value_size"]))
                versions[i] = version
            ready.put(client)
            result["phase"] = "barrier"
            barrier.wait(timeout=120)
            result["phase"] = "measured"
            result["measured_first_request_id"] = request_id + 1
            deadline = clock.value + int(config["duration_seconds"] * 1e9)
            while time.perf_counter_ns() < deadline and not abort.is_set():
                i = generator.randrange(len(keys))
                operation = "put" if generator.randrange(100) < config["write_percent"] else "get"
                version = versions[i] + (operation == "put")
                request_id += 1
                begin = time.perf_counter_ns()
                try:
                    rpc.rpc(connection, request_id, operation, keys[i],
                            value_for(value_client, i, version, config["value_size"]))
                except Exception as error:
                    completed = time.perf_counter_ns()
                    samples.append([completed - begin, completed - clock.value,
                                    int(operation == "put"), 0])
                    result["failures"] += 1
                    result["failed_request"] = dict(request_id=request_id, operation=operation,
                                                    key=keys[i].decode(), error=repr(error),
                                                    outcome="not retried; PUT may be unknown")
                    raise
                completed = time.perf_counter_ns()
                samples.append([completed - begin, completed - clock.value,
                                int(operation == "put"), 1])
                versions[i] = version
            result["finished_ns"] = time.perf_counter_ns()
            if abort.is_set():
                raise RuntimeError("another client failed")
            result["phase"] = "final-value-check"
            for i, key in enumerate(keys):
                request_id += 1
                rpc.rpc(connection, request_id, "get", key,
                        value_for(value_client, i, versions[i], config["value_size"]))
            result.update(status="complete", final_versions=versions)
    except BaseException as error:
        result["error"] = repr(error)
        abort.set()
        barrier.abort()
        ready.put(dict(error=repr(error), client=client))
    finally:
        name = f"client-{client}.json.gz"
        with gzip.open(Path(output) / name, "wt") as target:
            json.dump(dict(columns=["latency_ns", "completed_offset_ns", "put", "ok"],
                           samples=samples), target)
        result["samples_file"] = name
        save(Path(output) / f"client-{client}.json", result)
    return result


def load_process(ids, config, endpoint, barrier, clock, abort, ready, done, output):
    begin = time.process_time()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ids)) as pool:
            futures = [pool.submit(client_worker, i, config, endpoint, barrier, clock,
                                   abort, ready, output) for i in ids]
            results = [future.result() for future in futures]
        done.put(dict(clients=results, process_cpu_seconds=time.process_time() - begin))
    except BaseException as error:
        abort.set()
        barrier.abort()
        ready.put(dict(error=repr(error)))
        done.put(dict(error=repr(error)))


def summarize_samples(clients, directory, start_ns):
    if not clients or any(client["status"] != "complete" for client in clients):
        raise RuntimeError("empty or failed workload; raw evidence retained")
    by_operation = {"all": [], "get": [], "put": []}
    puts = failures = 0
    for client in clients:
        with gzip.open(directory / client["samples_file"], "rt") as source:
            raw = json.load(source)
        previous = 0
        for latency, offset, put, ok in raw["samples"]:
            if not (isinstance(latency, int) and 0 < latency <= offset and offset - latency >= previous
                    and offset <= client.get("finished_ns", 0) - start_ns
                    and put in (0, 1) and ok in (0, 1)):
                raise ValueError("invalid raw sample")
            previous = offset
            if not ok:
                failures += 1
                continue
            by_operation["all"].append(latency)
            by_operation["put" if put else "get"].append(latency)
            puts += put
    if not by_operation["all"] or failures:
        raise RuntimeError("empty or failed workload; raw evidence retained")
    elapsed = (max(c["finished_ns"] for c in clients) - start_ns) / 1e9
    latency = {}
    for operation, values in by_operation.items():
        values.sort()
        latency[operation] = ({name: rpc.percentile(values, fraction)
                               for name, fraction in PERCENTILES.items()} |
                              {"max_ms": round(max(values) / 1e6, 3), "samples": len(values)}
                              if values else None)
    count = len(by_operation["all"])
    return dict(operations=count, completed_puts=puts, completed_gets=count - puts,
                failures=failures, elapsed_seconds=elapsed, ops_per_second=count / elapsed,
                latency=latency)


def proc_sample(pids):
    processes = []
    for pid in pids:
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            processes.append(dict(pid=pid, cpu_seconds=(int(fields[11]) + int(fields[12])) /
                                  os.sysconf("SC_CLK_TCK"),
                                  rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE")))
        except (FileNotFoundError, ProcessLookupError):
            pass
    return dict(monotonic_ns=time.perf_counter_ns(), load=os.getloadavg(), processes=processes,
                cpu_ticks=Path("/proc/stat").read_text().splitlines()[0],
                meminfo=Path("/proc/meminfo").read_text().splitlines()[:3])


def run_one(args, config, endpoints, identity, directory):
    directory.mkdir()
    save(directory / "workload.json", config)
    states, leader, _ = settled(endpoints, args.timeout, identity)
    save(directory / "initial-states.json", states)
    context = mp.get_context("spawn")
    clock, abort = context.Value("q", 0), context.Event()
    ready, done = context.Queue(), context.Queue()
    barrier = context.Barrier(config["clients"] + 1)
    workers = min(args.workers, config["clients"])
    processes = [context.Process(target=load_process,
                 args=(list(range(i, config["clients"], workers)), config, endpoints[leader],
                       barrier, clock, abort, ready, done, str(directory))) for i in range(workers)]
    stop, monitor = threading.Event(), None
    resources = []
    result = dict(status="failed", config=config)
    try:
        for process in processes:
            process.start()
        for _ in range(config["clients"]):
            message = ready.get(timeout=120)
            if isinstance(message, dict):
                raise RuntimeError(f"preload/warmup failed: {message}")
        before, _, _ = settled(endpoints, args.timeout, identity)
        save(directory / "before.json", before)

        def sample():
            while not stop.is_set():
                resources.append(proc_sample([p.pid for p in processes]))
                stop.wait(1)

        monitor = threading.Thread(target=sample, daemon=True)
        monitor.start()
        clock.value = time.perf_counter_ns()  # Before releasing the first requester.
        barrier.wait(timeout=30)
        messages = [done.get(timeout=args.duration_seconds + args.timeout + 45) for _ in processes]
        if any("error" in message for message in messages):
            raise RuntimeError(f"load process failed: {messages}")
        clients = sorted([client for message in messages for client in message["clients"]],
                         key=lambda client: client["client"])
        result.update(clients=clients, start_ns=clock.value)
        stats = summarize_samples(clients, directory, clock.value)
        after, _, _ = settled(endpoints, args.timeout, identity)
        save(directory / "after.json", after)
        if config["write_percent"] == 0 and any(
                a["last_index"] != b["last_index"] or a["disk_bytes"] != b["disk_bytes"]
                for a, b in zip(after, before)):
            raise RuntimeError("read-only phase changed Raft log or persistent bytes")
        result.update(stats, status="complete", leader_id=identity[1], term=identity[0],
                      client_processes=workers,
                      client_process_cpu_seconds_including_setup=sum(
                          message["process_cpu_seconds"] for message in messages))
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        abort.set()
        barrier.abort()
        for process in processes:
            if process.pid:
                process.join(timeout=args.timeout + 5)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
        stop.set()
        if monitor:
            monitor.join(timeout=3)
        save(directory / "resources-client-host.json", resources)
        save(directory / "result.json", result)
    return result


def summary(root, results):
    rows = ["# Existing-cluster benchmark", "",
            "Each row summarizes repeated runs; percentile columns are medians of run",
            "percentiles, not percentiles of a pooled distribution. See individual run JSON",
            "for GET/PUT breakdown, P99.9, maximum latency, actual mix and raw samples.", "",
            "| Profile | Connections | Runs | Median ops/s | Min–max ops/s | P95 ms | P99 ms | Failures |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for profile, clients in sorted({(r["config"]["profile"], r["config"]["clients"]) for r in results}):
        group = [r for r in results if (r["config"]["profile"], r["config"]["clients"]) == (profile, clients)]
        rates = [r["ops_per_second"] for r in group]
        p95 = statistics.median(r["latency"]["all"]["p95_ms"] for r in group)
        p99 = statistics.median(r["latency"]["all"]["p99_ms"] for r in group)
        rows.append(f"| {profile} | {clients} | {len(group)} | {statistics.median(rates):.1f} | "
                    f"{min(rates):.1f}–{max(rates):.1f} | {p95:.3f} | {p99:.3f} | 0 |")
    rows += ["", "Client-side closed-loop latency includes request encoding, networking,",
             "server work and response validation. One request is outstanding per connection.",
             "Setup, preload, warmup and final value checks are excluded from throughput.",
             "The final outstanding requests drain into the measured elapsed interval.",
             "A client sharing an ECS node competes for resources. This is a workload",
             "observation, not an isolated server capacity or open-loop latency claim.",
             "Do not average the per-run P99s and label it a pooled P99."]
    (root / "summary.md").write_text("\n".join(rows) + "\n")


def verify(root):
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["status"] != "complete" or not manifest["runs"] or manifest["runs"] != manifest["planned_runs"]:
        raise ValueError("experiment is incomplete or failed")
    if manifest["protected_values_hex"] != manifest["protected_values_after_hex"]:
        raise ValueError("acceptance values changed")
    for relative, digest in manifest["files_sha256"].items():
        path = (root / relative).resolve()
        if root.resolve() not in path.parents or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError(f"file digest differs: {relative}")
    results = []
    for name in manifest["runs"]:
        directory = root / name
        result = json.loads((directory / "result.json").read_text())
        calculated = summarize_samples(result["clients"], directory, result["start_ns"])
        if (len(result["clients"]) != result["config"]["clients"]
                or {c["client"] for c in result["clients"]} != set(range(result["config"]["clients"]))
                or result["status"] != "complete"
                or any(result[key] != value for key, value in calculated.items())):
            raise ValueError(f"sample accounting differs: {name}")
        config = result["config"]
        if name != f"{config['profile']}-c{config['clients']}-run{config['repeat']}":
            raise ValueError("configuration differs from the planned case")
        before = json.loads((directory / "before.json").read_text())
        after = json.loads((directory / "after.json").read_text())
        if any([s["term"], s["leader_id"]] != manifest["identity"] for s in before + after):
            raise ValueError("leader/term changed")
        if config["write_percent"] == 0 and (result["completed_puts"] or any(
                a["last_index"] != b["last_index"] or a["disk_bytes"] != b["disk_bytes"]
                for a, b in zip(after, before))):
            raise ValueError("read-only phase changed Raft log or persistent bytes")
        results.append(result)
    print(json.dumps(dict(verified=True, runs=len(results), operations=sum(r["operations"] for r in results))))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--nodes", help="three numeric IPv4:port endpoints, in node ID order")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--client-node-id", type=int, choices=(0, 1, 2, 3), default=3,
                        help="0 for a separate load generator; otherwise it must be a follower")
    parser.add_argument("--concurrency", default="1,4,8,16,32")
    parser.add_argument("--profiles", default=",".join(PROFILES))
    parser.add_argument("--duration-seconds", type=float, default=30)
    parser.add_argument("--warmup-seconds", type=float, default=5)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--shared-read-keys", type=int, default=0,
                        help="fixed shared keyspace across concurrency levels; read-only profiles only")
    parser.add_argument("--keys-per-client", type=int, default=8)
    parser.add_argument("--value-size", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--seed", type=int, default=1701)
    parser.add_argument("--preflight", action="store_true", help="read cluster state without sending PUTs")
    args = parser.parse_args()
    if args.verify:
        return args
    try:
        args.endpoints = [(str(ipaddress.IPv4Address(host)), int(port))
                          for host, port in (part.split(":") for part in args.nodes.split(","))]
        args.levels = [int(n) for n in args.concurrency.split(",")]
        args.profile_names = args.profiles.split(",")
        if len(args.endpoints) != 3 or len(set(args.endpoints)) != 3 or any(
                not 1 <= port <= 65535 for _, port in args.endpoints):
            raise ValueError("exactly three distinct endpoints are required")
        if (not args.output or not args.levels or len(set(args.levels)) != len(args.levels)
                or any(not 1 <= n <= 128 for n in args.levels)
                or any(name not in PROFILES for name in args.profile_names)):
            raise ValueError("invalid output, concurrency (1..128) or profiles")
        if (not 0 < args.duration_seconds <= 300 or not 0 <= args.warmup_seconds <= 60
                or not 0 < args.timeout <= 30 or not 1 <= args.workers <= 16
                or not 1 <= args.repeats <= 10 or not 1 <= args.keys_per_client <= 128
                or not 32 <= args.value_size <= 8192):
            raise ValueError("invalid duration, warmup, timeout, workers, repeats or key/value sizes")
        if not 0 <= args.shared_read_keys <= 4096 or (args.shared_read_keys and
                any(name != "read-only" for name in args.profile_names)):
            raise ValueError("shared keyspace requires 1..4096 keys and read-only profiles")
        dataset_keys = args.shared_read_keys or max(args.levels) * args.keys_per_client
        if dataset_keys * (args.value_size + 128) > 4 * 1024 * 1024:
            raise ValueError("benchmark keyspace must fit within a conservative 4 MiB budget")
        if not all(math.isfinite(n) for n in (args.duration_seconds, args.warmup_seconds, args.timeout)):
            raise ValueError("non-finite timing option")
    except (ValueError, AttributeError, TypeError) as error:
        parser.error(str(error))
    return args


def main():
    args = parse_args()
    if args.verify:
        verify(args.verify)
        return
    # Prevent accidental overlapping generators on this host. Do not remove lock inode.
    with open("/tmp/yewukv-remote-matrix.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        root = args.output.resolve()
        root.mkdir(parents=True, exist_ok=False)
        for source in (Path(__file__), Path(rpc.__file__)):
            (root / source.name).write_bytes(source.read_bytes())
        manifest = dict(version=1, status="running", started_utc=now(), command=sys.argv,
                        client_machine=dict(platform=platform.platform(), python=sys.version,
                                            cpus=os.cpu_count(), affinity=sorted(os.sched_getaffinity(0))),
                        nodes=args.endpoints, client_node_id=args.client_node_id,
                        prefix=f"cloudbench-{uuid.uuid4().hex[:16]}", runs=[],
                        note="Existing data, reused bounded experiment keyspace; no node restarts.")
        save(root / "manifest.json", manifest)
        results = []
        try:
            states, leader, identity = settled(args.endpoints, args.timeout)
            if [state["id"] for state in states] != [1, 2, 3]:
                raise ValueError("--nodes must be in node ID order 1,2,3")
            if args.client_node_id and identity[1] == args.client_node_id:
                raise RuntimeError("client host is the leader; choose a follower or a separate client")
            if args.client_node_id:
                address = args.endpoints[args.client_node_id - 1][0]
                with socket.socket() as local:
                    local.bind((address, 0))  # Verify the stated client node belongs to this machine.
            added_bytes = (args.shared_read_keys or max(args.levels) * args.keys_per_client) * (args.value_size + 128)
            if max(s["live_snapshot_bytes"] for s in states) + added_bytes > 16 * 1024 * 1024:
                raise RuntimeError("existing state leaves insufficient capacity for the bounded benchmark keyspace")
            manifest.update(initial_states=states, identity=identity)
            manifest["local_environment_before"] = local_environment(args.client_node_id)
            # These optional acceptance keys are read-only and never part of load keys.
            protected = protected_values(args.endpoints[leader], args.timeout)
            manifest["protected_values_hex"] = protected
            if args.preflight:
                manifest["status"] = "preflight-complete"
                print(json.dumps(dict(preflight=True, leader_id=identity[1], term=identity[0],
                                      client_node_id=args.client_node_id, states=states)))
                return
            config = dict(prefix=manifest["prefix"], value_size=args.value_size,
                          keys_per_client=args.keys_per_client, shared_read_keys=args.shared_read_keys, seed=args.seed,
                          duration_seconds=args.duration_seconds, warmup_seconds=args.warmup_seconds,
                          timeout_seconds=args.timeout)
            schedule = []
            for repeat in range(1, args.repeats + 1):
                cases = [(profile, clients) for profile in args.profile_names for clients in args.levels]
                random.Random(args.seed + repeat).shuffle(cases)
                schedule.extend((repeat, profile, clients) for profile, clients in cases)
            manifest["planned_runs"] = [f"{profile}-c{clients}-run{repeat}"
                                        for repeat, profile, clients in schedule]
            save(root / "manifest.json", manifest)
            # Fill the maximum keyspace once so dataset size is fixed across configurations.
            with socket.create_connection(args.endpoints[leader], timeout=args.timeout) as connection:
                request_id = 0
                for client in range(1 if args.shared_read_keys else max(args.levels)):
                    for key in range(args.shared_read_keys or args.keys_per_client):
                        request_id += 1
                        encoded_key = (f"{config['prefix']}-shared-{key}" if args.shared_read_keys else
                                       f"{config['prefix']}-{client}-{key}").encode()
                        rpc.rpc(connection, request_id, "put", encoded_key,
                                value_for(client, key, 0, args.value_size))
            for repeat, profile, clients in schedule:
                name = f"{profile}-c{clients}-run{repeat}"
                run_config = dict(config, profile=profile, clients=clients, repeat=repeat,
                                  write_percent=PROFILES[profile])
                result = run_one(args, run_config, args.endpoints, identity, root / name)
                results.append(result)
                manifest["runs"].append(name)
                save(root / "manifest.json", manifest)
                print(f"{name}: {result['ops_per_second']:.1f} ops/s, "
                      f"P95 {result['latency']['all']['p95_ms']:.3f} ms, "
                      f"P99 {result['latency']['all']['p99_ms']:.3f} ms, failures 0", flush=True)
            manifest["protected_values_after_hex"] = protected_values(args.endpoints[leader], args.timeout)
            if manifest["protected_values_after_hex"] != protected:
                raise RuntimeError("existing acceptance keys changed")
            manifest["local_environment_after"] = local_environment(args.client_node_id)
            local_before, local_after = (manifest[f"local_environment_{phase}"] for phase in ("before", "after"))
            if any(local_before.get(key) != local_after.get(key)
                   for key in ("boot_id", "local_server_sha256", "local_node_env", "unit")):
                raise RuntimeError("local node restarted or its executable/configuration changed")
            manifest["status"] = "complete"
            summary(root, results)
        except BaseException as error:
            manifest.update(status="failed", error=repr(error))
            raise
        finally:
            manifest["finished_utc"] = now()
            manifest["files_sha256"] = {
                str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(root.rglob("*")) if path.is_file() and path.name != "manifest.json"}
            save(root / "manifest.json", manifest)


if __name__ == "__main__":
    main()
