#!/usr/bin/env python3
"""Verify finite cloud histories, load accounting, source and cleanup offline."""
import argparse
from collections import Counter
import gzip, hashlib, json, math, os, statistics, sys, tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
sys.path.insert(0, str(ROOT / "tests"))
from cloud_load import summarize
from linearizability import check


def require(value, message):
    if not value:
        raise ValueError(message)


def load(path):
    if path.suffix == ".gz":
        with gzip.open(path, "rt") as f:
            return json.load(f)
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
    for name, w in files.items():
        p = root / name
        require(
            not p.is_symlink()
            and p.resolve().is_relative_to(root.resolve())
            and p.stat().st_size == w["bytes"]
            and digest(p) == w["sha256"],
            "file differs: " + name,
        )
    for prefix in ["runtime", "tested"]:
        with tarfile.open(root / f"source/{prefix}-source.tar.gz", "r:gz") as archive:
            for name, w in load(root / f"source/{prefix}-source-manifest.json").items():
                require(
                    hashlib.sha256(
                        archive.extractfile("source/" + name).read()
                    ).hexdigest()
                    == w,
                    "source differs: " + name,
                )
    return len(files)


def resources(folder, start, end):
    frames = load(folder / "resources.json.gz")
    selected = [f for f in frames if start <= f["monotonic_ns"] <= end]
    result = dict(
        frames=len(selected),
        servers={},
        client_peak_rss_bytes=0,
        client_average_cores=None,
    )

    def proc(row):
        f = row["stat"].rsplit(")", 1)[1].split()
        return int(f[11]) + int(f[12]), int(f[21]) * 4096

    for frame in selected:
        result["client_peak_rss_bytes"] = max(
            result["client_peak_rss_bytes"], sum(proc(c)[1] for c in frame["clients"])
        )
    complete = [f for f in selected if len(f["clients"]) == 2]
    if len(complete) > 1:
        a, b = complete[0], complete[-1]
        initial = {p["pid"]: proc(p)[0] for p in a["clients"]}
        if set(initial) == {p["pid"] for p in b["clients"]}:
            result["client_average_cores"] = (
                sum(proc(p)[0] - initial[p["pid"]] for p in b["clients"])
                / 100
                / ((b["monotonic_ns"] - a["monotonic_ns"]) / 1e9)
            )
    for node in [1, 2, 3]:
        samples = [
            (f["servers"][node - 1])
            for f in selected
            if len(f["servers"]) == 3 and "stat" in f["servers"][node - 1]
        ]
        if len(samples) < 2:
            continue
        a, b = samples[0], samples[-1]
        require(a["pid"] == b["pid"], "server process changed")
        result["servers"][str(node)] = dict(
            average_cores=(proc(b)[0] - proc(a)[0])
            / 100
            / ((b["monotonic_ns"] - a["monotonic_ns"]) / 1e9),
            peak_rss_bytes=max(proc(s)[1] for s in samples),
            pid=a["pid"],
        )
    return result


def verify_case(folder, leader):
    r = load(folder / "result.json")
    c = load(folder / "workload.json")
    require(
        r["status"] == "complete"
        and r["config"] == c
        and r["identity"][1] == leader
        and len(r["clients"]) == c["clients"]
        and {x["client"] for x in r["clients"]} == set(range(c["clients"])),
        "case identity/config differs",
    )
    for client in r["clients"]:
        require(
            client["samples_file"] == f"client-{client['client']}.json.gz"
            and client == load(folder / f"client-{client['client']}.json"),
            "client metadata differs",
        )
        if c["offered_rps"]:
            period = round(1e9 * c["clients"] / c["offered_rps"])
            offset = round(client["client"] * 1e9 / c["offered_rps"])
            offered = max(
                0, (round(c["duration_seconds"] * 1e9) - 1 - offset) // period + 1
            )
            require(client["offered_slots"] == offered, "offered schedule differs")
            raw = load(folder / client["samples_file"])
            previous = -1
            for row in raw["samples"]:
                require(
                    (row[4] - offset) % period == 0
                    and offset <= row[4] < c["duration_seconds"] * 1e9
                    and row[4] > previous,
                    "paced schedule differs",
                )
                previous = row[4]
    result = summarize(folder, r)
    require(all(r[k] == v for k, v in result.items()), "sample accounting differs")
    require(
        result["codes"].get("98", 0) == 0 and result["failures"] == 0,
        "unexpected failed measured request",
    )
    before, after = load(folder / "before.json"), load(folder / "after.json")
    require(
        len(before) == len(after) == 3 and [s["id"] for s in before] == [1, 2, 3],
        "node inventory differs",
    )
    for a, b in zip(before, after):
        require(
            a["term"] == b["term"] == r["identity"][0]
            and a["leader_id"] == b["leader_id"] == leader
            and b["last_index"] == b["commit_index"] == b["applied_index"],
            "term/leader/convergence differs",
        )
        require(
            a["snapshot_encoder"] == b["snapshot_encoder"] == c["encoder"],
            "snapshot encoder differs",
        )
        if c["write_percent"] == 0:
            require(
                a["last_index"] == b["last_index"]
                and a["disk_bytes"] == b["disk_bytes"],
                "pure read changed persistent data",
            )
    require(
        after[leader - 1]["last_index"] - before[leader - 1]["last_index"]
        == result["completed_puts"],
        "successful PUT log accounting differs",
    )
    if c["cold_keys"]:
        recovery = load(folder / "restart-verification.json")
        require(
            recovery["verified"] and recovery["cold_keys"] == 1024,
            "cold snapshot restart not checked",
        )
        require(
            all(s["snapshot_index"] > 0 for s in recovery["states"]),
            "snapshot absent after restart",
        )
    return dict(
        name=folder.name,
        config=c,
        identity=r["identity"],
        **result,
        resources=resources(
            folder, r["start_ns"], max(x["finished_ns"] for x in r["clients"])
        ),
        leader_before=before[leader - 1],
        leader_after=after[leader - 1],
    )


def fault_histories(folder):
    summary = load(folder / "summary.json")
    require(
        summary["status"] == "complete"
        and len(summary["epochs"]) == 18
        and summary["catchup_snapshot_verified"]
        and summary["full_restart_verified"]
        and summary["ring_values"] == 16,
        "fault summary incomplete",
    )
    totals = Counter()
    kinds = Counter()
    for i, s in enumerate(summary["epochs"]):
        r = load(folder / f"epoch-{i:03d}.json")
        history = r["operations"]
        model = [dict(op, end=math.inf) if op.get("optional") else op for op in history]
        require(
            r["epoch"] == s["epoch"] == i
            and len(r["attempts"]) == s["attempts"] == 120
            and len(history) == s["checked_operations"]
            and s["checked"]
            and check(model),
            "fault history rejected",
        )
        require(
            sum(op.get("optional", False) for op in history) == s["unknown_writes"]
            and all(op["end"] is None for op in history if op.get("optional")),
            "unknown write accounting differs",
        )
        overlap = sum(
            op["start"] < r["fault"]["healed_ns"]
            and op["end"] > r["fault"]["applied_ns"]
            for op in r["attempts"]
        )
        require(
            overlap == s["overlapping_attempts"] and overlap > 0,
            "traffic does not overlap fault",
        )
        probe = r["minority_read_probe"]
        if s["kind"] in ["leader-partition", "directed-partition"]:
            require(probe and probe["code"] not in [0, 1], "minority read succeeded")
        totals.update(attempts=120, checked=len(history), unknown=s["unknown_writes"])
        kinds[s["kind"]] += 1
    require(
        set(kinds.values()) == {3}
        and len(kinds) == 6
        and totals["attempts"] == summary["attempts"] == 2160
        and totals["checked"] == summary["checked_operations"] == 2491,
        "fault matrix differs",
    )
    return dict(
        epochs=18,
        attempts=2160,
        checked_operations=2491,
        unknown_writes=totals["unknown"],
        kinds=dict(kinds),
    )


def verify(root):
    count = inventory(root)
    meta = load(root / "manifest.json")
    require(
        digest(root / "source/runtime-source.tar.gz") == meta["runtime_source_sha256"],
        "runtime source hash",
    )
    environment = load(root / "control/node-environment.json")
    require(
        [n["node"] for n in environment] == [1, 2, 3]
        and all(
            n["cpu_count"] == 2
            and n["clock_ticks"] == 100
            and n["page_size"] == 4096
            and n["experiment_node_sha256"] == meta["runtime_binary_sha256"]
            for n in environment
        ),
        "runtime identity or resource conversion assumptions differ",
    )
    for kind in ["normal", "asan"]:
        require(
            "100% tests passed, 0 tests failed out of 143"
            in (root / f"tests/final-ctest-{kind}.log").read_text(),
            "full regression evidence differs",
        )
    require(
        "[  PASSED  ] 13 tests." in (root / "tests/tsan-state-machine.log").read_text(),
        "component TSan evidence differs",
    )
    rows = []
    for group, g in meta["groups"].items():
        folder = root / "results" / group
        manifest = load(folder / "manifest.json")
        require(
            manifest["status"] == "complete"
            and len(manifest["plan"]) == len(manifest["completed"]) == g["runs"],
            "incomplete load matrix",
        )
        require(
            len({c["name"] for c in manifest["plan"]}) == g["runs"], "duplicate case"
        )
        if group.startswith("snapshot"):
            require(
                {(c["encoder"], c["repeat"]) for c in manifest["plan"]}
                == {(m, r) for m in ["sync", "background"] for r in [1, 2, 3]},
                "snapshot repeat inventory",
            )
        else:
            require(
                {
                    (c["write_percent"], c["clients"], c["repeat"])
                    for c in manifest["plan"]
                    if not c["offered_rps"]
                }
                == {
                    (w, n, r)
                    for w in [0, 50, 90]
                    for n in [8, 32, 128]
                    for r in [1, 2, 3]
                },
                "capacity repeat inventory",
            )
            require(
                {
                    (c["offered_rps"], c["repeat"])
                    for c in manifest["plan"]
                    if c["offered_rps"]
                }
                == {(n, r) for n in [500, 2500, 10000] for r in [1, 2, 3]},
                "paced repeat inventory",
            )
        for case, record in zip(manifest["plan"], manifest["completed"]):
            require(case["name"] == record["name"], "case order differs")
            row = verify_case(folder / case["name"], g["leader"])
            row.update(group=group, exploratory=g["exploratory"])
            rows.append(row)
            require(
                (
                    row["attempts"],
                    row["successes"],
                    row["failures"],
                    row["successful_ops_s"],
                    row["latency_success_ms"],
                    row["missed_slots"],
                )
                == (
                    record["attempts"],
                    record["successes"],
                    record["failures"],
                    record["ops_s"],
                    record["latency"],
                    record["missed_slots"],
                ),
                "manifest result differs",
            )
    faults = fault_histories(root / "results/faults")
    upgrade = load(root / "results/probes/upgrade.json")
    require(
        upgrade["verified"] and upgrade["legacy_values_checked"] == 31,
        "upgrade probe absent",
    )
    events = {e["step"]: e for e in upgrade["events"]}
    require(
        events["read-v1"]["response"]["revision"] == 0
        and events["migration-conflict"]["response"]["code"] == 7
        and sorted(r["code"] for r in events["two-publishers"]["responses"]) == [0, 7]
        and events["delete-recreate-ABA"]["stale"]["code"] == 7,
        "upgrade/CAS semantics differ",
    )
    admission = load(root / "results/probes/admission.json")
    codes = [n for c in admission["clients"] for n in c["codes"]]
    require(
        admission["verified"]
        and admission["recovered"]
        and len(codes) == admission["attempts"] == 1024
        and codes.count(0) == admission["ok"]
        and codes.count(4) == admission["busy"]
        and all(c in [0, 4] for c in codes)
        and admission["busy"] > 0,
        "admission accounting differs",
    )
    original = load(root / "control/original-data-verification.json")
    require(
        original["verified"]
        and original["checked_experiment_keys"] == 512
        and len(original["protected"]) == 4
        and all(
            s["keys"] == 2852
            and s["last_index"] == s["commit_index"] == s["applied_index"] == 711089
            for s in original["states"]
        ),
        "original data not preserved",
    )
    for s in load(root / "control/final-services.json"):
        require(
            s["binary_sha256"]
            == "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
            and s["properties"]["ActiveState"] == "active"
            and s["properties"]["NRestarts"] == "0"
            and not s["properties"]["DropInPaths"]
            and not s["temporary_units"].strip()
            and all(v == [0, 1] for v in s["threads"].values()),
            "original service/cleanup differs",
        )
    for dispatch, subdir in [
        ("controlled-load-dispatch.json", "results/executed-load-tools"),
        ("snapshot-dispatch.json", "pilot-tools"),
    ]:
        for name, wanted in load(root / "control" / dispatch)["tools_sha256"].items():
            require(
                digest(root / subdir / name) == wanted, "executed tool hash differs"
            )
    return dict(
        verified=True,
        files=count,
        load_runs=len(rows),
        primary_runs=sum(not r["exploratory"] for r in rows),
        attempts=sum(r["attempts"] for r in rows),
        primary_attempts=sum(r["attempts"] for r in rows if not r["exploratory"]),
        failed_measured_requests=sum(r["failures"] for r in rows),
        missed_offered_slots=sum(r["missed_slots"] for r in rows),
        faults=faults,
        admission=dict(attempts=1024, ok=admission["ok"], busy=admission["busy"]),
        original_data_verified=True,
        rows=rows,
        note="Finite checked histories and co-located load. Missed offered slots are separate from sent-request failures; no independent server capacity or general linearizability proof.",
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    a = p.parse_args()
    print(json.dumps(verify(a.root.resolve())))


if __name__ == "__main__":
    main()
