"""Recompute the paired cloud disk comparison from client and node evidence."""

import hashlib
import statistics
from pathlib import Path

from cloud_async_disk import check_states, phase_options, plan
from cpp_load import final_values, read, require, summarize
from soak_evidence import actual_mix, counter_delta, resource_rows


def argument(argv, name):
    require(
        argv.count(name) == 1 and argv.index(name) + 1 < len(argv),
        "missing/duplicate option",
    )
    return argv[argv.index(name) + 1]


def value_matches(record, value):
    require(
        record["code"] == 0
        and record["bytes"] == len(value)
        and record["sha256"] == hashlib.sha256(value).hexdigest()
        and record["revision"] > 0,
        "recovered value differs",
    )


def campaign(
    nodes,
    environment,
    name="async-main",
    *,
    phases=None,
    window_seconds=None,
    phase_builder=phase_options,
    grouping="executor",
    binary_hashes=None,
):
    root = Path(nodes) / "node-3" / name
    progress = read(root / "progress.json")
    smoke = name == "async-smoke"
    seconds = (2 if smoke else 30) if window_seconds is None else window_seconds
    phases = plan(smoke) if phases is None else phases
    require(grouping in ("executor", "variant"), "unknown comparison grouping")
    require(
        progress["status"] == "complete"
        and not progress["cleanup_errors"]
        and progress["plan"] == phases
        and progress["seconds"] == seconds
        and progress["measured_seconds"] == len(phases) * seconds
        and len(progress["completed"]) == len(phases)
        and progress["finished_unix_ns"] > progress["started_unix_ns"],
        "incomplete or different comparison plan",
    )
    expected_labels = {
        r.get("label", f"m{r['cold_mib']}-r{r['repeat']}-{r['executor']}")
        for r in phases
    }
    require(
        {p.name for p in root.iterdir() if p.is_dir()} == expected_labels,
        "phase inventory differs",
    )
    rows, identities = [], {n: set() for n in (1, 2, 3)}
    for position, phase in enumerate(phases):
        label = phase.get(
            "label", f"m{phase['cold_mib']}-r{phase['repeat']}-{phase['executor']}"
        )
        folder = root / label
        setup = read(folder / "setup.json")
        options = setup["options"]
        require(
            options == phase_builder(phase, name + "-" + label),
            "runtime plan options differ",
        )
        cold_keys = phase["cold_mib"] * 128
        require(setup["cold_keys"] == cold_keys, "preload size differs")
        launched = setup["launched"]
        require(len(launched) == 3, "missing launch record")
        hosts = [argument(item["argv"], "--listen-host") for item in launched]
        require(len(set(hosts)) == 3, "member hosts are not distinct")
        members = ",".join(f"{n}@{host}:19101" for n, host in enumerate(hosts, 1))
        for node, item in enumerate(launched, 1):
            argv = item["argv"]
            require(
                item["started"]
                and Path(argv[0]).name
                == (
                    "yewukv-node-baseline"
                    if options.get("program") == "baseline"
                    else "yewukv-node"
                )
                and argument(argv, "--id") == str(node)
                and argument(argv, "--port") == "19101"
                and argument(argv, "--members") == members
                and Path(argument(argv, "--data-dir")).name
                == options["case"] + f"-node-{node}"
                and argument(argv, "--disk-executor") == phase["executor"]
                and argument(argv, "--snapshot-encoder") == "background"
                and argument(argv, "--snapshot-policy") == "adaptive"
                and argument(argv, "--snapshot-entries") == "128"
                and argument(argv, "--snapshot-bytes") == "1048576"
                and argument(argv, "--journal-checkpoint-bytes") == "4194304"
                and argument(argv, "--request-timeout-ms") == "1500"
                and "--trace-prefix" not in argv,
                "actual launch configuration differs",
            )
            if binary_hashes is not None:
                require(
                    item["binary_sha256"]
                    == binary_hashes[options.get("program", "new")],
                    "actual launch binary hash differs",
                )
        directory = folder / "window"
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
            and config["prefix"] == "disk-hot"
            and config["trace_identity"] is False,
            "workload configuration differs",
        )
        command = read(folder / "command.json")
        for option, value in [
            ("--host", hosts[1]),
            ("--port", "19101"),
            ("--clients", "32"),
            ("--duration-ms", str(seconds * 1000)),
            ("--warmup-ms", "1000"),
            ("--write-percent", "50"),
            ("--value-size", "128"),
            ("--offered-rps", "600"),
        ]:
            require(argument(command, option) == value, "client command differs")
        result = summarize(directory)
        observation = read(folder / "observation.json")
        completed = progress["completed"][position]
        require(
            result["valid"]
            and not result["failures"]
            and observation["summary"] == result
            and completed == dict(position=position, **phase, summary=result),
            "window summary differs from raw samples",
        )
        before, after = observation["before"], observation["after"]
        require(before == setup["states"], "preload state differs")
        check_states(before, phase["executor"])
        check_states(after, phase["executor"])
        mix = actual_mix(directory)
        deltas = {}
        for node, (a, b) in enumerate(zip(before, after), 1):
            require(
                a["id"] == b["id"] == node
                and a["term"] == b["term"] == before[1]["term"]
                and a["keys"] == cold_keys + 1
                and b["keys"] == cold_keys + 257
                and a["last_index"] == a["commit_index"] == a["applied_index"]
                and b["last_index"] == b["commit_index"] == b["applied_index"]
                and b["last_index"] >= a["last_index"] + 256 + mix["puts"]
                and not a["trace_limited"]
                and not b["trace_limited"],
                "node identity, application or trace state differs",
            )
            deltas[str(node)] = counter_delta(a, b)
        frames = read(folder / "resources.json")
        resources = resource_rows(frames, environment)
        for frame in frames:
            for node, sample in enumerate(frame["servers"], 1):
                active = sample["active"]
                require(
                    active["case"] == options["case"]
                    and active["disk_executor"] == phase["executor"]
                    and active["encoder"] == "background"
                    and not active["trace"]
                    and not active["proxies"],
                    "sampled runtime configuration differs",
                )
                if binary_hashes is not None:
                    require(
                        active["program"] == options["program"]
                        and active["binary_sha256"]
                        == binary_hashes[options["program"]],
                        "sampled binary variant differs",
                    )
        for node in (1, 2, 3):
            item = resources[f"node_{node}"]
            identity = (item["pid"], item["start_ticks"])
            require(identity not in identities[node], "phase reused a process")
            identities[node].add(identity)
        recovery = read(folder / "restart-verification.json")
        hot = final_values(directory)
        require(
            recovery["verified"]
            and recovery["cold_keys"] == cold_keys
            and recovery["hot_keys"] == len(hot) == 256
            and set(recovery["values"])
            == {f"cold-{i}" for i in range(cold_keys)} | set(hot),
            "restart value inventory differs",
        )
        for key, record in recovery["values"].items():
            value_matches(record, hot[key] if key in hot else b"c" * 8192)
        anchor = bytes.fromhex(setup["anchor_value"])
        value_matches(recovery["anchor"], anchor)
        require(
            anchor == label.encode()
            and recovery["anchor"]["revision"] == setup["anchor"]["revision"],
            "anchor value/revision differs after restart",
        )
        restored = recovery["states"]
        check_states(restored, phase["executor"], fixed_leader=False)
        require(
            len(
                {
                    (
                        s["term"],
                        s["leader_id"],
                        s["last_index"],
                        s["commit_index"],
                        s["applied_index"],
                    )
                    for s in restored
                }
            )
            == 1
            and sum(s["role"] == "leader" for s in restored) == 1
            and restored[0]["term"] > after[0]["term"]
            and restored[0]["applied_index"] >= after[0]["applied_index"]
            and all(s["keys"] == cold_keys + 257 for s in restored),
            "full restart did not converge",
        )
        rows.append(
            dict(
                position=position,
                **phase,
                **result,
                **mix,
                resources=resources,
                counter_deltas=deltas,
                cold_keys=cold_keys,
                hot_keys=256,
                anchor_revision=recovery["anchor"]["revision"],
            )
        )
    groups, pairs = [], []
    for size in sorted({r["cold_mib"] for r in rows}):
        for selected_group in (
            ("sync", "background")
            if grouping == "executor"
            else ("before", "after", "sync")
        ):
            selected = [
                r
                for r in rows
                if r["cold_mib"] == size and r[grouping] == selected_group
            ]
            group = dict(
                cold_mib=size, **{grouping: selected_group}, windows=len(selected)
            )
            for key in ("successful_ops_s", "p99_ms", "p999_ms", "scheduled_p99_ms"):
                values = [r[key] for r in selected]
                group[key] = dict(
                    median=statistics.median(values),
                    minimum=min(values),
                    maximum=max(values),
                )
            group.update(
                successes=sum(r["successes"] for r in selected),
                missed_slots=sum(r["missed_slots"] for r in selected),
                offered_slots=sum(r["offered_slots"] for r in selected),
                max_ms=max(r["max_ms"] for r in selected),
                resources={},
            )
            for key in ("node_1", "node_2", "node_3", "client"):
                cpu = sum(r["resources"][key]["cpu_seconds"] for r in selected)
                elapsed = sum(r["resources"][key]["sample_seconds"] for r in selected)
                group["resources"][key] = dict(
                    average_cores=cpu / elapsed,
                    sampled_max_rss_bytes=max(
                        r["resources"][key]["peak_rss_bytes"] for r in selected
                    ),
                )
            groups.append(group)
        for repeat in sorted({r["repeat"] for r in rows if r["cold_mib"] == size}):
            selected = {
                r[grouping]: r
                for r in rows
                if r["cold_mib"] == size and r["repeat"] == repeat
            }
            for reference in ["sync"] if grouping == "executor" else ["before", "sync"]:
                target = "background" if grouping == "executor" else "after"
                changes = {
                    key: 100 * (selected[target][key] / selected[reference][key] - 1)
                    for key in (
                        "successful_ops_s",
                        "p99_ms",
                        "p999_ms",
                        "scheduled_p99_ms",
                    )
                }
                pair = dict(cold_mib=size, repeat=repeat, percent_change=changes)
                if grouping == "variant":
                    pair["comparison"] = "after_vs_" + reference
                pairs.append(pair)
    total = {
        k: sum(r[k] for r in rows)
        for k in (
            "attempts",
            "successes",
            "failures",
            "offered_slots",
            "missed_slots",
            "puts",
            "gets",
        )
    }
    total["max_ms"] = max(r["max_ms"] for r in rows)
    require(
        total["puts"] + total["gets"] == total["successes"], "operation counts differ"
    )
    return dict(
        windows=len(rows),
        measured_seconds=len(rows) * seconds,
        wall_seconds=(progress["finished_unix_ns"] - progress["started_unix_ns"]) / 1e9,
        total=total,
        rows=rows,
        groups=groups,
        pairs=pairs,
    )
