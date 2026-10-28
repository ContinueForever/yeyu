"""Current-code checks for the frozen opt-in background stability campaign."""

import importlib.util
from pathlib import Path

from async_disk_evidence import argument
from cloud_soak import phase_options
from cpp_load import read, require
from peer_disk_evidence import NODE_SHA256, PEER_COUNTERS, peer_counters
from soak_evidence import counter_delta, resource_rows, summarize_campaign

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "current_soak_raw_verifier", ROOT / "scripts/verify-cloud-soak.py"
)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


def states(records, encoder):
    require(len(records) == 3, "missing background states")
    for node, state in enumerate(records, 1):
        require(
            state["id"] == node
            and state["disk_executor"] == "background"
            and state["snapshot_encoder"] == encoder
            and state["snapshot_policy"] == "adaptive"
            and state["storage_mode"] == "journal"
            and state["read_mode"] == "read-index"
            and all(
                state[k] == 0
                for k in (
                    "trace_enabled",
                    "trace_written",
                    "trace_dropped",
                    "trace_failed",
                )
            )
            and state["last_index"] == state["commit_index"] == state["applied_index"],
            "background mode, trace or convergence differs",
        )
        peer_counters(state, state)


def verify_raw(root, environment):
    """Includes exact paced-calendar/raw latency validation, never archived code."""
    root = Path(root)
    progress = read(root / "progress.json")
    require(
        progress["disk_executor"] == "background"
        and progress["offered_rps"] == 600
        and progress["original_services_protected"] is True,
        "background campaign scope differs",
    )
    result = base.verify(root)
    smoke = result["smoke"]
    require(
        root.name == ("soak-smoke" if smoke else "soak-main")
        and progress["shutdown_agents_requested"] is (not smoke)
        and progress["finished_unix_ns"] > progress["started_unix_ns"]
        and result["offered_slots"] == (2400 if smoke else 12960000),
        "campaign duration or shutdown contract differs",
    )
    totals = {str(n): {k: 0 for k in PEER_COUNTERS} for n in (1, 2, 3)}
    for phase_index, phase in enumerate(progress["plan"]):
        folder = root / f"phase-{phase_index:02d}"
        setup = read(folder / "setup.json")
        options = phase_options(phase, f"{root.name}-p{phase_index}", "background")
        require(setup["options"] == options, "background phase options differ")
        require(len(setup["launched"]) == 3, "missing actual launch")
        for node, launch in enumerate(setup["launched"], 1):
            argv = launch["argv"]
            require(
                launch["started"] is True
                and launch["binary_sha256"] == NODE_SHA256
                and Path(argv[0]).name == "yewukv-node"
                and argument(argv, "--id") == str(node)
                and argument(argv, "--port") == "19101"
                and argument(argv, "--disk-executor") == "background"
                and argument(argv, "--snapshot-encoder") == phase["encoder"]
                and argument(argv, "--snapshot-policy") == "adaptive"
                and argument(argv, "--snapshot-entries") == "128"
                and argument(argv, "--snapshot-bytes") == "1048576"
                and argument(argv, "--journal-checkpoint-bytes") == "4194304"
                and argument(argv, "--request-timeout-ms") == "1500"
                and "--trace-prefix" not in argv,
                "actual background launch differs",
            )
        states(setup["states"], phase["encoder"])
        previous = None
        identities = {n: set() for n in (1, 2, 3)}
        for window in range(phase["windows"]):
            directory = folder / f"window-{window:03d}"
            observation = read(directory / "observation.json")
            before, after = observation["before"], observation["after"]
            states(before, phase["encoder"])
            states(after, phase["encoder"])
            frames = read(folder / f"resources-{window:03d}.json")
            resources = resource_rows(frames, environment)
            for frame in frames:
                for node, server in enumerate(frame["servers"], 1):
                    active = server["active"]
                    require(
                        active["case"] == options["case"]
                        and active["program"] == "new"
                        and active["binary_sha256"] == NODE_SHA256
                        and active["encoder"] == phase["encoder"]
                        and active["disk_executor"] == "background"
                        and active["trace"] is False
                        and active["proxies"] is False,
                        "sampled background runtime differs",
                    )
            for node, (a, b) in enumerate(zip(before, after), 1):
                delta = peer_counters(a, b)
                counter_delta(a, b)
                if previous is not None:
                    peer_counters(previous[node - 1], a)
                    counter_delta(previous[node - 1], a)
                for k in PEER_COUNTERS:
                    totals[str(node)][k] += delta[k]
                identity = resources[f"node_{node}"]
                identities[node].add((identity["pid"], identity["start_ticks"]))
            previous = after
        require(
            all(len(v) == 1 for v in identities.values()),
            "node changed within background phase",
        )
        recovery = read(folder / "restart-verification.json")
        states(recovery["states"], phase["encoder"])
        require(
            all(s["keys"] == setup["cold_keys"] + 257 for s in recovery["states"])
            and len({s["term"] for s in recovery["states"]}) == 1
            and len({s["applied_index"] for s in recovery["states"]}) == 1
            and all(
                s["applied_index"] >= previous[n]["applied_index"]
                for n, s in enumerate(recovery["states"])
            ),
            "background recovery state differs",
        )
    result.update(
        disk_executor="background",
        default_executor="sync",
        performance_gate=False,
        full_restart_checks=len(progress["plan"]),
        peer_window_counter_deltas=totals,
    )
    summary = None if smoke else summarize_campaign(root, environment)
    return result, summary
