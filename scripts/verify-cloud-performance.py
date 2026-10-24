#!/usr/bin/env python3
"""Recompute the retained 45-run ECS benchmark using only the standard library.

Checks retained evidence, not remote host identity or production capacity.
Never imports or executes source files inside an evidence archive.
"""

import argparse
import csv
from datetime import datetime
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import re
import statistics
import tarfile


RUNTIME = "909df769ae2d9b0579a31f9d89745e589da86819"
TOOL = "c1b238cb7430a051ea8f21798c71aa064f8cdc41"
TOOL_HASHES = {
    "remote_matrix.py": "312498749e33c4acfdeac7b5f93a99d7fad7ef045c3235ae84df17fa121a5e49",
    "rpc_baseline.py": "bf5dddb6c99c124007fafdbba25f2a2b0b79410fc77f66416173eac78e154cb2",
}
BINARY_HASHES = {
    "yewukv-node": "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02",
    "yewukv-client": "f4022c9fb1c886f439804638b7c243aed564dcb473848f86f63ed4d53e2c5774",
}
PROFILES = {"read-only": 0, "balanced": 50, "write-heavy": 90}
LEVELS = (1, 4, 8, 16, 32)
QUANTILES = {"p50_ms": .5, "p95_ms": .95, "p99_ms": .99, "p999_ms": .999}
VALUES = {"cloud-check": "ready", "failover-check": "survived",
          "rejoin-check": "caught-up", "reboot-check": "alive"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def quantiles(values):
    if not values:
        return None
    values.sort()
    result = {name: round(values[math.ceil(len(values) * fraction) - 1] / 1e6, 3)
              for name, fraction in QUANTILES.items()}
    return dict(result, max_ms=round(values[-1] / 1e6, 3), samples=len(values))


def check_states(states, index, keys):
    require([s["id"] for s in states] == [1, 2, 3], "node IDs differ")
    for state in states:
        expected = dict(term=96, leader_id=2, last_index=index, commit_index=index,
                        applied_index=index, keys=keys, pending=0, quorum_timeouts=0,
                        busy_rejections=0, storage_mode="journal", read_mode="read-index",
                        snapshot_policy="adaptive", role="leader" if state["id"] == 2 else "follower")
        require(all(state.get(k) == v for k, v in expected.items()), "node state differs")


def sampled_client_resources(directory, result):
    begin, end = result["start_ns"], max(c["finished_ns"] for c in result["clients"])
    frames = [s for s in load(directory / "resources-client-host.json")
              if begin <= s["monotonic_ns"] <= end
              and len(s["processes"]) == result["client_processes"]]
    if len(frames) < 2:
        return None
    first, last = frames[0], frames[-1]
    seconds = (last["monotonic_ns"] - first["monotonic_ns"]) / 1e9
    initial_cpu = {p["pid"]: p["cpu_seconds"] for p in first["processes"]}
    require(set(initial_cpu) == {p["pid"] for p in last["processes"]}, "load processes changed")
    cpu_seconds = sum(p["cpu_seconds"] - initial_cpu[p["pid"]] for p in last["processes"])
    a, b = ([int(n) for n in s["cpu_ticks"].split()[1:9]] for s in (first, last))
    delta = [y - x for x, y in zip(a, b)]
    require(all(n >= 0 for n in delta) and sum(delta) > 0, "CPU clock reset")
    return dict(sampled_seconds=seconds, client_average_cores=cpu_seconds / seconds,
                client_peak_sampled_rss_bytes=max(sum(p["rss_bytes"] for p in s["processes"]) for s in frames),
                host_steal_percent=100 * delta[7] / sum(delta),
                host_iowait_percent=100 * delta[4] / sum(delta))


def analyze(experiment):
    manifest = load(experiment / "manifest.json")
    require(manifest["status"] == "complete" and manifest["identity"] == [96, 2], "incomplete matrix")
    expected_names = {f"{p}-c{c}-run{r}" for p in PROFILES for c in LEVELS for r in (1, 2, 3)}
    require(len(manifest["runs"]) == 45 and set(manifest["runs"]) == expected_names
            and manifest["runs"] == manifest["planned_runs"], "matrix is missing or repeats a case")
    protected = {k: v.encode().hex() for k, v in VALUES.items()}
    require(manifest["protected_values_hex"] == manifest["protected_values_after_hex"] == protected,
            "acceptance values differ")
    check_states(manifest["initial_states"], 2294, 36)
    previous_index, records, warmup_puts = 2294 + 256, [], 0
    for name in manifest["runs"]:
        directory = experiment / name
        result, config = load(directory / "result.json"), load(directory / "workload.json")
        require(result["status"] == "complete" and result["config"] == config, f"failed case: {name}")
        require(config["profile"] in PROFILES and config["clients"] in LEVELS and
                config["repeat"] in (1, 2, 3) and
                name == f"{config['profile']}-c{config['clients']}-run{config['repeat']}", "case name differs")
        expected = dict(value_size=128, keys_per_client=8, duration_seconds=30,
                        warmup_seconds=5, timeout_seconds=5, seed=1701,
                        write_percent=PROFILES[config["profile"]], prefix=manifest["prefix"])
        require(all(config.get(k) == v for k, v in expected.items()), "workload differs")
        require(result["client_processes"] == min(2, config["clients"]), "process count differs")
        clients = result["clients"]
        require([c["client"] for c in clients] == list(range(config["clients"])), "client inventory differs")
        times, puts, gets, run_warmup = [], 0, 0, 0
        by_operation = {"all": [], "put": [], "get": []}
        for client in clients:
            require(client["status"] == "complete" and client["failures"] == 0, "failed client")
            require(client["samples_file"] == f"client-{client['client']}.json.gz", "sample path differs")
            require(load(directory / f"client-{client['client']}.json") == client, "client metadata differs")
            with gzip.open(directory / client["samples_file"], "rt") as source:
                raw = json.load(source)
            require(raw["columns"] == ["latency_ns", "completed_offset_ns", "put", "ok"], "sample schema differs")
            require(raw["samples"], "empty client")
            previous_offset, client_puts = 0, 0
            finish = client["finished_ns"] - result["start_ns"]
            for latency, offset, put, ok in raw["samples"]:
                require(isinstance(latency, int) and isinstance(offset, int)
                        and latency > 0 and offset - latency >= previous_offset
                        and offset <= finish and put in (0, 1) and ok == 1, "invalid or failed sample")
                previous_offset = offset
                by_operation["all"].append(latency)
                by_operation["put" if put else "get"].append(latency)
                client_puts += put
            versions = client["final_versions"]
            require(len(versions) == 8 and all(isinstance(v, int) and v >= 0 for v in versions), "version checks missing")
            require(sum(versions) >= client_puts, "version count precedes acknowledged PUTs")
            run_warmup += sum(versions) - client_puts
            puts += client_puts
            gets += len(raw["samples"]) - client_puts
            times.append(client["finished_ns"])
        elapsed = (max(times) - result["start_ns"]) / 1e9
        require(30 <= elapsed < 35 and result["elapsed_seconds"] == elapsed, "duration differs")
        count = puts + gets
        require((result["operations"], result["completed_puts"], result["completed_gets"], result["failures"])
                == (count, puts, gets, 0), "operation accounting differs")
        require(math.isclose(result["ops_per_second"], count / elapsed, rel_tol=1e-12), "rate differs")
        latency = {op: quantiles(values) for op, values in by_operation.items()}
        require(result["latency"] == latency, "raw sample percentiles differ")
        initial = load(directory / "initial-states.json")
        before, after = load(directory / "before.json"), load(directory / "after.json")
        check_states(initial, previous_index, 292)
        before_index = previous_index + config["clients"] * 8 + run_warmup
        check_states(before, before_index, 292)
        previous_index = before_index + puts
        check_states(after, previous_index, 292)
        if config["write_percent"] == 0:
            require(puts == run_warmup == 0 and all(a["disk_bytes"] == b["disk_bytes"]
                    for a, b in zip(after, before)), "read-only phase persisted data")
        warmup_puts += run_warmup
        records.append(dict(name=name, profile=config["profile"], clients=config["clients"],
                            repeat=config["repeat"], operations=count, puts=puts, gets=gets,
                            elapsed_seconds=elapsed, ops_per_second=count / elapsed, latency=latency,
                            warmup_puts=run_warmup, resources=sampled_client_resources(directory, result)))
    groups = []
    for profile in PROFILES:
        for clients in LEVELS:
            group = [r for r in records if (r["profile"], r["clients"]) == (profile, clients)]
            row = dict(profile=profile, clients=clients, repeats=3,
                       operations=sum(r["operations"] for r in group),
                       puts=sum(r["puts"] for r in group), gets=sum(r["gets"] for r in group), failures=0)
            for metric in ("ops_per_second", *QUANTILES, "max_ms"):
                values = [r[metric] if metric == "ops_per_second" else r["latency"]["all"][metric] for r in group]
                for stat, function in (("median_run", statistics.median), ("min_run", min), ("max_run", max)):
                    row[f"{metric}_{stat}"] = function(values)
            row["per_operation_median_run"] = {
                op: ({k: statistics.median(r["latency"][op][k] for r in group) for k in (*QUANTILES, "max_ms")}
                     if group[0]["latency"][op] is not None else None) for op in ("get", "put")}
            groups.append(row)
    total = sum(r["operations"] for r in records)
    require(total == 1954767 and sum(r["puts"] for r in records) == 599523 and
            warmup_puts == 100611 and previous_index == 707076, "retained experiment totals differ")
    return dict(version=1, runs=45, client_runs=549, operations=total,
                puts=599523, gets=1355244, failures=0, warmup_puts=warmup_puts,
                global_prefill_puts=256, per_run_prefill_puts=4392,
                initial_index=2294, final_index=707076, initial_keys=36, final_keys=292,
                started_utc=manifest["started_utc"], finished_utc=manifest["finished_utc"],
                wall_seconds=(datetime.fromisoformat(manifest["finished_utc"]) -
                              datetime.fromisoformat(manifest["started_utc"])).total_seconds(),
                measured_seconds=sum(r["elapsed_seconds"] for r in records),
                max_latency_ms=max(r["latency"]["all"]["max_ms"] for r in records),
                groups=groups, run_results=records)


def summary_csv(analysis):
    target = io.StringIO(newline="")
    fields = [k for k in analysis["groups"][0] if k != "per_operation_median_run"]
    writer = csv.DictWriter(target, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows({k: row[k] for k in fields} for row in analysis["groups"])
    return target.getvalue()


def verify(root):
    root = root.resolve()
    manifest = load(root / "publication-manifest.json")
    require(manifest["runtime_commit"] == RUNTIME and manifest["benchmark_commit"] == TOOL, "source versions differ")
    require(not any(p.is_symlink() for p in root.rglob("*")), "symlink in evidence")
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*")
              if p.is_file() and p.name != "publication-manifest.json"}
    require(actual == set(manifest["files"]), "publication inventory differs")
    for name, record in manifest["files"].items():
        path = root / name
        require(path.resolve().is_relative_to(root), "unsafe inventory path")
        content = path.read_bytes()
        require(record == dict(bytes=len(content), sha256=hashlib.sha256(content).hexdigest()), f"digest differs: {name}")
    source = load(root / "runtime-source-manifest.json")
    with tarfile.open(root / "runtime-source.tar.gz") as archive:
        require(archive.pax_headers.get("comment") == RUNTIME, "runtime source marker differs")
        members = archive.getmembers()
        require(all((m.isfile() or m.isdir()) and not Path(m.name).is_absolute()
                    and ".." not in Path(m.name).parts for m in members), "unsafe source member")
        files = [m for m in members if m.isfile()]
        require(len(files) == len(source) == 154 and {m.name for m in files} == set(source), "source inventory differs")
        for member in files:
            require(hashlib.sha256(archive.extractfile(member).read()).hexdigest() == source[member.name], "source hash differs")
    experiment = root / "experiment"
    run_manifest = load(experiment / "manifest.json")
    require(run_manifest["client_node_id"] == 3 and
            run_manifest["nodes"] == [[f"10.0.0.{10+n}", 9101] for n in (1, 2, 3)], "deployment differs")
    for filename, expected_hash in TOOL_HASHES.items():
        require(hashlib.sha256((experiment / filename).read_bytes()).hexdigest() == expected_hash, "benchmark source differs")
    expected_members = ",".join(f"{n}@10.0.0.{10+n}:9101" for n in (1, 2, 3))
    for node in (1, 2, 3):
        environment = (root / "nodes" / f"node-{node}-environment.txt").read_text()
        require(datetime.fromisoformat(environment.splitlines()[0]) < datetime.fromisoformat(run_manifest["started_utc"]),
                "environment capture is after load start")
        expected = (f"Linux ecs-node-{node} ", 'VERSION_ID="24.04"', "CPU(s):                                  2",
                    f"YEWUKV_LISTEN_HOST=10.0.0.{10+node}", "YEWUKV_PORT=9101",
                    f"YEWUKV_MEMBERS={expected_members}", f"--id {node} ",
                    "ActiveState=active", "User=yewukv")
        require(all(s in environment for s in expected), f"node {node} configuration differs")
        hashes = {name: digest for digest, name in re.findall(r"^([0-9a-f]{64})\s+/usr/local/bin/(yewukv-(?:node|client))$", environment, re.M)}
        require(hashes == BINARY_HASHES, "installed binary hashes differ")
    for phase in ("before", "after"):
        environment = run_manifest[f"local_environment_{phase}"]
        require(environment["local_server_sha256"] == BINARY_HASHES["yewukv-node"] and
                environment["tool_git_head"]["stdout"].strip() == TOOL and
                environment["tool_git_status"] == dict(returncode=0, stdout="", stderr=""), "local version differs")
    for field in ("boot_id", "unit", "local_server_sha256", "local_node_env"):
        require(run_manifest["local_environment_before"][field] == run_manifest["local_environment_after"][field],
                "client node identity changed")
    for filename, digest in run_manifest["files_sha256"].items():
        path = experiment / filename
        require(path.resolve().is_relative_to(experiment.resolve()), "unsafe experiment path")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "experiment digest differs")
    computed = analyze(experiment)
    require(load(root / "analysis.json") == computed, "derived analysis differs")
    require((root / "summary.csv").read_text() == summary_csv(computed), "CSV differs")
    console = "".join(f"{r['name']}: {r['ops_per_second']:.1f} ops/s, "
                      f"P95 {r['latency']['all']['p95_ms']:.3f} ms, "
                      f"P99 {r['latency']['all']['p99_ms']:.3f} ms, failures 0\n"
                      for r in computed["run_results"])
    require((root / "run.log").read_text() == console, "console results differ")
    return dict(verified=True, files=len(manifest["files"]), runs=45,
                operations=computed["operations"], failures=0, final_index=computed["final_index"],
                final_keys=computed["final_keys"], runtime_commit=RUNTIME, benchmark_commit=TOOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.root)))
