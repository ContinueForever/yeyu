"""Recompute the short cloud observation from independent node/client evidence."""

import statistics
from pathlib import Path

from cpp_load import final_values, read, require, summarize
from persistence_timeline import (
    analyze,
    load_trace,
    validate_client_ranges,
    validate_persist,
)
from soak_evidence import actual_mix, counter_delta, resource_rows


def capture_inventory(nodes):
    records = []
    for node in [1, 2, 3]:
        paths = sorted((Path(nodes) / f"node-{node}/traces").rglob("*.jsonl"))
        require(len(paths) == 8, "trace lifetime inventory differs")
        for path in paths:
            events = load_trace(path)
            metadata, footer = events[0], events[-1]
            require(
                metadata["event"] == "metadata"
                and metadata["schema"] == 2
                and metadata["sample_every"] == 1
                and metadata["max_events"] == 500000
                and footer["event"] == "summary"
                and all(e["node"] == node for e in events)
                and sum(e["event"] == "metadata" for e in events) == 1
                and sum(e["event"] == "summary" for e in events) == 1
                and all(
                    e["event"]
                    in ["metadata", "summary", "mode", "read", "peer", "persist"]
                    for e in events
                )
                and footer["accepted"]
                == footer["written"]
                == sum(e["event"] in ["read", "peer", "persist"] for e in events)
                and footer["written"] < metadata["max_events"]
                and not any(footer[k] for k in ["dropped", "limited", "io_failed"])
                and path.stat().st_size < 256 * 1024**2,
                "trace footer, options or budget differs",
            )
            for event in events:
                if event["event"] == "persist":
                    validate_persist(event)
                    require(
                        event["success"] and not event["spans_limited"],
                        "failed/truncated persistence",
                    )
            records.append(
                dict(
                    path=str(path.relative_to(Path(nodes) / f"node-{node}")),
                    pid=metadata["pid"],
                    bytes=path.stat().st_size,
                    **footer,
                )
            )
    return records


def campaign(nodes, environment, name):
    nodes = Path(nodes)
    root = nodes / "node-3" / name
    progress = read(root / "progress.json")
    smoke = name == "trace-smoke"
    seconds, cold = (2, 1) if smoke else (30, 15)
    modes = [False, True] if smoke else [False, True, True, False, False, True]
    require(
        progress["status"] == "complete"
        and not progress["cleanup_errors"]
        and progress["seconds"] == seconds
        and progress["cold_mib"] == cold
        and progress["encoders"] == ["sync", "background"]
        and progress["modes"] == modes
        and len(progress["completed"]) == 2 * len(modes)
        and progress["measured_seconds"] == 2 * len(modes) * seconds,
        "incomplete or different trace plan",
    )
    directories = sorted(root.glob("*/window-*"))
    require(len(directories) == 2 * len(modes), "window inventory differs")
    identity_ranges = validate_client_ranges(directories)
    rows, timelines, groups = [], {}, []
    for encoder in progress["encoders"]:
        folder = root / encoder
        setup = read(folder / "setup.json")
        options = setup["options"]
        require(
            options["encoder"] == encoder
            and options["snapshot_policy"] == "adaptive"
            and options["snapshot_entries"] == 128
            and options["snapshot_bytes"] == 1048576
            and options["journal_checkpoint_bytes"] == 4194304
            and options["request_timeout_ms"] == 1500
            and options["trace"] is True
            and options["trace_sample_every"] == 1
            and options["trace_max_events"] == 500000
            and setup["cold_keys"] == cold * 128,
            "runtime configuration differs",
        )
        case = options["case"]
        pid = setup["processes"][1]["pid"]
        paths = list(
            (nodes / "node-2/traces" / (case + "-node-2")).glob(f"trace-{pid}-*.jsonl")
        )
        require(len(paths) == 1, "leader trace identity differs")
        events = load_trace(paths[0])
        require(
            events[0]["pid"] == pid
            and events[0]["sample_every"] == 1
            and events[0]["max_events"] == 500000,
            "trace process/options differ",
        )
        selected_rows = []
        joined_rows = {False: [], True: []}
        for index, enabled in enumerate(modes):
            directory = folder / f"window-{index:02d}"
            config = read(directory / "result.json")
            require(
                config["schema"] == 2
                and config["duration_ms"] == seconds * 1000
                and config["warmup_ms"] == 1000
                and config["clients"] == 32
                and config["offered_rps"] == 600
                and config["write_percent"] == 50
                and config["value_size"] == 128
                and config["max_samples"] == 200000
                and config["trace_identity"] is True,
                "workload configuration differs",
            )
            result = summarize(directory)
            observation = read(directory / "observation.json")
            require(
                result["valid"]
                and result["failures"] == 0
                and observation["summary"] == result
                and observation["enabled"] == enabled,
                "raw window accounting differs",
            )
            before, after = observation["before"], observation["after"]
            require(
                len(before) == len(after) == 3
                and before[1]["role"] == after[1]["role"] == "leader",
                "leader changed",
            )
            for node, (a, b) in enumerate(zip(before, after), 1):
                require(
                    a["id"] == b["id"] == node
                    and a["term"] == b["term"] == before[1]["term"]
                    and a["term"] == setup["states"][1]["term"]
                    and b["last_index"] == b["commit_index"] == b["applied_index"]
                    and a["trace_enabled"] == b["trace_enabled"] == int(enabled)
                    and a["snapshot_encoder"] == b["snapshot_encoder"] == encoder
                    and a["snapshot_policy"] == b["snapshot_policy"] == "adaptive"
                    and a["storage_mode"] == b["storage_mode"] == "journal"
                    and a["read_mode"] == b["read_mode"] == "read-index"
                    and all(
                        not b[k]
                        for k in ["trace_failed", "trace_dropped", "trace_limited"]
                    ),
                    "node state or trace control differs",
                )
                counter_delta(a, b)
            resources = resource_rows(
                read(folder / f"resources-{index:02d}.json"), environment
            )
            require(
                all(
                    resources[f"node_{n}"]["pid"] == setup["processes"][n - 1]["pid"]
                    for n in [1, 2, 3]
                ),
                "node process changed within phase",
            )
            mix = actual_mix(directory)
            timeline = analyze(events, directory)
            require(
                timeline["capture_clean"]
                and timeline["failed_persistence_operations"] == 0
                and timeline["measured_gets"] == mix["gets"]
                and timeline["joined_fraction"] == int(enabled),
                "trace coverage or identity join differs",
            )
            compact = {k: v for k, v in timeline.items() if k != "rows"}
            slow = [
                r
                for r in timeline["rows"]
                if r["client_latency_ns"] >= timeline["client_slow_threshold_ns"]
            ]
            denominator = sum(r["server_receive_to_ready_ns"] for r in slow)
            compact["slow_server_time_weighted_persist_overlap_fraction"] = (
                sum(r["persist_overlap_ns"] for r in slow) / denominator
                if denominator
                else None
            )
            compact["slow_server_time_weighted_file_sync_overlap_fraction"] = (
                sum(r["file_sync_overlap_ns"] for r in slow) / denominator
                if denominator
                else None
            )
            row = dict(
                encoder=encoder,
                window=index,
                enabled=enabled,
                **result,
                **mix,
                resources=resources,
                timeline=compact,
                leader_counter_delta=counter_delta(before[1], after[1]),
            )
            rows.append(row)
            selected_rows.append(row)
            joined_rows[enabled].extend(timeline["rows"])
            if enabled:
                timelines[f"{encoder}-{index:02d}"] = timeline
        recovery = read(folder / "restart-verification.json")
        require(
            recovery["verified"]
            and recovery["cold_keys"] == cold * 128
            and recovery["hot_keys"] == len(final_values(directory)) == 256
            and recovery["anchor_revision"] == setup["anchor_revision"],
            "restart verification differs",
        )
        for enabled in [False, True]:
            selected = [r for r in selected_rows if r["enabled"] == enabled]
            slow = [
                r for r in joined_rows[enabled] if r["client_latency_ns"] >= 20_000_000
            ]
            denominator = sum(r["server_receive_to_ready_ns"] for r in slow)
            overlaps = {
                k: sum(r[k] for r in slow)
                for k in [
                    "persist_overlap_ns",
                    "queue_persist_overlap_ns",
                    "handled_persist_overlap_ns",
                    "encode_overlap_ns",
                    "write_overlap_ns",
                    "file_sync_overlap_ns",
                    "directory_sync_overlap_ns",
                    "rename_overlap_ns",
                ]
            }
            groups.append(
                dict(
                    encoder=encoder,
                    enabled=enabled,
                    windows=len(selected),
                    attempts=sum(r["attempts"] for r in selected),
                    missed_slots=sum(r["missed_slots"] for r in selected),
                    median_ops_s=statistics.median(
                        r["successful_ops_s"] for r in selected
                    ),
                    median_p99_ms=statistics.median(r["p99_ms"] for r in selected),
                    median_p999_ms=statistics.median(r["p999_ms"] for r in selected),
                    maximum_ms=max(r["max_ms"] for r in selected),
                    resources={
                        key: dict(
                            average_cores=sum(
                                r["resources"][key]["cpu_seconds"] for r in selected
                            )
                            / sum(
                                r["resources"][key]["sample_seconds"] for r in selected
                            ),
                            peak_rss_bytes=max(
                                r["resources"][key]["peak_rss_bytes"] for r in selected
                            ),
                        )
                        for key in ["node_1", "node_2", "node_3", "client"]
                    },
                    timeline=dict(
                        measured_gets=sum(r["gets"] for r in selected),
                        joined_gets=len(joined_rows[enabled]),
                        client_slow_gets=sum(
                            r["timeline"]["client_slow_gets"] for r in selected
                        ),
                        joined_slow_gets=len(slow),
                        joined_gets_overlapping_persist=sum(
                            r["persist_overlap_ns"] > 0 for r in joined_rows[enabled]
                        ),
                        joined_slow_gets_overlapping_persist=sum(
                            r["persist_overlap_ns"] > 0 for r in slow
                        ),
                        slow_server_receive_to_ready_ns=denominator,
                        slow_overlap_ns=overlaps,
                        slow_time_weighted_overlap_fractions={
                            k: v / denominator if denominator else None
                            for k, v in overlaps.items()
                        },
                    ),
                )
            )
    total = {
        k: sum(r[k] for r in rows)
        for k in [
            "attempts",
            "successes",
            "failures",
            "offered_slots",
            "missed_slots",
            "puts",
            "gets",
        ]
    }
    summary = dict(
        name=name,
        measured_seconds=progress["measured_seconds"],
        wall_seconds=(progress["finished_unix_ns"] - progress["started_unix_ns"]) / 1e9,
        started_unix_ns=progress["started_unix_ns"],
        finished_unix_ns=progress["finished_unix_ns"],
        windows=len(rows),
        identity_ranges=identity_ranges,
        total=total,
        groups=groups,
        rows=rows,
        note=(
            "One smoke window per group. "
            if smoke
            else "Three correlated off/on windows per group. "
        )
        + "Same-process default-adaptive observation; load shares Follower 3. Percentiles are medians of window percentiles, not pooled percentiles. Leader-thread overlap is request-weighted wall time, not device latency or system utilization; excludes follower I/O and uninstrumented phases.",
    )
    return summary, timelines
