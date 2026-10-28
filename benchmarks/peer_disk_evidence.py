"""Independent raw recomputation for the three-variant disk admission campaign."""

from pathlib import Path

from async_disk_evidence import campaign as disk_campaign
from cloud_peer_disk import phase_options, plan
from cpp_load import read, require

CORE = "834bb430a1263ed1c4e3f76f5696fd14f415ac55"
NODE_SHA256 = "e58f9fe73dfef9e6d21b8f56c607bde0a9ad82c45c40e419cba5cc0f43ff85b7"
BASELINE_SHA256 = "f992830abb5e9471d8c3e66efd6fa8d512eb62110ab891aabac1da9cb38ed569"
PEER_COUNTERS = (
    "peer_deferred",
    "peer_deferred_admitted",
    "peer_deferred_expired",
    "peer_deferred_abandoned",
    "peer_deferred_rejected",
    "peer_deferred_wait_ns",
    "peer_busy_received",
    "peer_rpc_failed",
)


def peer_counters(before, after):
    for state in (before, after):
        require(
            state["peer_deferred_capacity"] == 2
            and 0 <= state["peer_deferred_pending"] <= 2
            and state["peer_deferred_wait_ns"]
            >= state["peer_deferred_wait_max_ns"]
            >= 0,
            "invalid bounded peer queue observation",
        )
    result = {k: after[k] - before[k] for k in PEER_COUNTERS}
    require(
        all(type(v) is int and v >= 0 for v in result.values()),
        "peer counter regressed",
    )
    require(
        result["peer_deferred"]
        == sum(
            result[k]
            for k in (
                "peer_deferred_admitted",
                "peer_deferred_expired",
                "peer_deferred_abandoned",
            )
        )
        + after["peer_deferred_pending"]
        - before["peer_deferred_pending"],
        "peer wait accounting differs",
    )
    return result


def campaign(nodes, environment, name="peer-main"):
    require(name in ("peer-main", "peer-smoke"), "unknown peer campaign")
    smoke = name == "peer-smoke"
    result = disk_campaign(
        nodes,
        environment,
        name,
        phases=plan(smoke),
        window_seconds=2 if smoke else 30,
        phase_builder=phase_options,
        grouping="variant",
        binary_hashes={"new": NODE_SHA256, "baseline": BASELINE_SHA256},
    )
    root = Path(nodes) / "node-3" / name
    for row in result["rows"]:
        if row["variant"] == "before":
            continue
        observation = read(root / row["label"] / "observation.json")
        row["peer_counter_deltas"] = {
            str(n): peer_counters(a, b)
            for n, (a, b) in enumerate(
                zip(observation["before"], observation["after"]), 1
            )
        }
    return result


def mechanism(root):
    """Recompute the controlled local probe of the exact cloud binaries."""
    result = {}
    for variant, wanted in (("before", BASELINE_SHA256), ("after", NODE_SHA256)):
        record = read(Path(root) / "probes" / ("cloud-" + variant) / "result.json")
        leader, target, other = (
            record[k] for k in ("leader", "checkpoint_follower", "isolated_follower")
        )
        require(
            set((leader, target, other)) == {0, 1, 2}
            and record["binary_sha256"] == wanted
            and record["restart_check"]
            and record["reply"]["code"] == 0
            and record["armed_ns"]
            < record["probe_submitted_ns"]
            < record["released_ns"]
            < record["acknowledged_ns"],
            "controlled probe identity or ordering differs",
        )
        gated = record["while_gated"]
        require(
            gated["id"] == target + 1
            and gated["persistence_pending"] == 1
            and gated["last_index"] == gated["applied_index"]
            and gated["snapshot_index"] < gated["applied_index"],
            "follower was not gated on a local checkpoint",
        )
        release_ms = (record["acknowledged_ns"] - record["released_ns"]) / 1e6
        submitted_ms = (record["acknowledged_ns"] - record["probe_submitted_ns"]) / 1e6
        require(
            record["release_to_ack_ms"] == release_ms
            and record["submitted_to_ack_ms"] == submitted_ms,
            "controlled timing summary differs",
        )
        events = [
            e
            for e in record["events"]
            if e["source"] == leader
            and e["target"] == target
            and record["armed_ns"] <= e["mono_ns"] <= record["acknowledged_ns"]
        ]
        require(
            all(a["mono_ns"] <= b["mono_ns"] for a, b in zip(events, events[1:])),
            "proxy event clock regressed",
        )
        busy = [e for e in events if e["response"] and e.get("code") == 4]
        item = dict(
            binary_sha256=wanted,
            release_to_ack_ms=release_ms,
            submitted_to_ack_ms=submitted_ms,
            busy_responses=len(busy),
            restart_check=True,
        )
        if variant == "before":
            require(
                busy == record["busy_while_gated"] and busy,
                "old background probe did not observe the BUSY response",
            )
            response = busy[0]
            sent = [
                e
                for e in events
                if not e["response"]
                and e["request_id"] == response["request_id"]
                and e["mono_ns"] < response["mono_ns"]
            ]
            retry = [
                e
                for e in events
                if not e["response"] and e["mono_ns"] > response["mono_ns"]
            ]
            require(
                sent and retry and retry[0]["sequence"] > sent[-1]["sequence"],
                "missing correlated request and later retry",
            )
            item.update(
                rejected_sequence=sent[-1]["sequence"],
                retry_sequence=retry[0]["sequence"],
                busy_to_retry_ms=(retry[0]["mono_ns"] - response["mono_ns"]) / 1e6,
            )
        else:
            require(
                not busy
                and not record["busy_while_gated"]
                and gated["peer_deferred_pending"] == 1
                and record["after"][target]["peer_deferred_admitted"] > 0,
                "new background probe did not resume the deferred RPC",
            )
        result[variant] = item
    return result
