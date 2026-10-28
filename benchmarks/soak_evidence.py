"""Offline summaries for the six-phase cloud window; never execute archived tools."""

from collections import defaultdict
import statistics

from cpp_load import read, require


COUNTERS = [
    "disk_bytes",
    "disk_checkpoint_saves",
    "disk_journal_saves",
    "disk_encode_ns",
    "disk_crc_ns",
    "disk_write_ns",
    "disk_file_sync_ns",
    "disk_directory_sync_ns",
    "snapshot_pause_ns",
    "snapshots_saved",
]
BUCKETS = ["disk_checkpoint_duration_buckets", "disk_journal_sync_duration_buckets"]
MAXIMA = [
    "disk_checkpoint_max_ns",
    "disk_journal_sync_max_ns",
    "snapshot_pause_max_ns",
    "snapshot_freeze_max_ns",
    "snapshot_publish_max_ns",
]


def proc(stat, page_size):
    fields = stat.rsplit(")", 1)[1].split()
    return dict(
        pid=int(stat.split(" ", 1)[0]),
        start_ticks=int(fields[19]),
        cpu_ticks=int(fields[11]) + int(fields[12]),
        rss_bytes=int(fields[21]) * page_size,
    )


def resource_rows(frames, environment):
    require(len(frames) >= 2, "insufficient resource frames")
    result = {}
    for node, env in enumerate(environment):
        require(env["node"] == node + 1, "environment order differs")
        samples = []
        for frame in frames:
            require(len(frame["servers"]) == 3, "missing server frame")
            server = frame["servers"][node]
            original = dict(
                line.split("=", 1)
                for line in server["original_service"].splitlines()
            )
            properties = dict(
                line.split("=", 1) for line in server["properties"].splitlines()
            )
            require(
                original["ActiveState"] == properties["ActiveState"] == "active"
                and original["NRestarts"] == properties["NRestarts"] == "0"
                and not original["DropInPaths"]
                and server["free_disk_bytes"] >= 2 * 1024**3,
                "resource guard or service state differs",
            )
            p = proc(server["stat"], env["page_size"])
            require(
                p["pid"] == server["pid"] == int(properties["MainPID"])
                and 0 < p["rss_bytes"] <= 480 * 1024**2,
                "node identity or RSS differs",
            )
            samples.append(dict(**p, monotonic_ns=server["monotonic_ns"]))
        result[f"node_{node+1}"] = process_interval(samples, env["clock_ticks"])
        result[f"node_{node+1}"]["minimum_free_disk_bytes"] = min(
            f["servers"][node]["free_disk_bytes"] for f in frames
        )
    client = [
        dict(
            **proc(f["client_stat"], environment[2]["page_size"]),
            monotonic_ns=f["monotonic_ns"],
        )
        for f in frames
        if "client_stat" in f
    ]
    require(len(client) >= 2, "insufficient client samples")
    result["client"] = process_interval(client, environment[2]["clock_ticks"])
    return result


def process_interval(samples, clock_ticks):
    a, b = samples[0], samples[-1]
    require(
        len({(p["pid"], p["start_ticks"]) for p in samples}) == 1
        and all(
            y["monotonic_ns"] > x["monotonic_ns"]
            and y["cpu_ticks"] >= x["cpu_ticks"]
            for x, y in zip(samples, samples[1:])
        ),
        "sampled process changed or clock/counter regressed",
    )
    elapsed = (b["monotonic_ns"] - a["monotonic_ns"]) / 1e9
    cpu_seconds = (b["cpu_ticks"] - a["cpu_ticks"]) / clock_ticks
    return dict(
        pid=a["pid"],
        start_ticks=a["start_ticks"],
        sample_seconds=elapsed,
        cpu_seconds=cpu_seconds,
        average_cores=cpu_seconds / elapsed,
        peak_rss_bytes=max(p["rss_bytes"] for p in samples),
        first_rss_bytes=a["rss_bytes"],
        last_rss_bytes=b["rss_bytes"],
    )


def counter_delta(a, b):
    require(
        a["id"] == b["id"]
        and a["term"] == b["term"]
        and a["disk_latency_bounds_ns"] == b["disk_latency_bounds_ns"],
        "counter identity differs",
    )
    result = {k: b[k] - a[k] for k in COUNTERS}
    for k in BUCKETS:
        require(
            len(a[k]) == len(b[k]) == len(a["disk_latency_bounds_ns"]) + 1,
            "histogram shape differs",
        )
        result[k] = [y - x for x, y in zip(a[k], b[k])]
        require(all(v >= 0 for v in result[k]), "histogram counter regressed")
    require(all(result[k] >= 0 for k in COUNTERS), "counter regressed")
    require(
        result["disk_crc_ns"] <= result["disk_encode_ns"]
        and sum(result[BUCKETS[0]]) == result["disk_checkpoint_saves"]
        and sum(result[BUCKETS[1]]) == result["disk_journal_saves"]
        and all(b[k] >= a[k] for k in MAXIMA),
        "persistence accounting differs",
    )
    return result


def actual_mix(directory):
    config = read(directory / "result.json")
    puts, gets = 0, 0
    for client in range(config["clients"]):
        random = 0x9E3779B9 + client
        for row in read(directory / f"client-{client}.json")["samples"]:
            random ^= (random << 13) & (2**64 - 1)
            random ^= random >> 7
            random ^= (random << 17) & (2**64 - 1)
            require(
                row[4] == int(random % 100 < config["write_percent"]),
                "recorded operation mix differs",
            )
            puts += row[4]
            gets += 1 - row[4]
    return dict(puts=puts, gets=gets)


def summarize_campaign(root, environment):
    progress = read(root / "progress.json")
    rows, phases = [], []
    for index, phase in enumerate(progress["plan"]):
        folder = root / f"phase-{index:02d}"
        phase_rows, node_identities = [], defaultdict(set)
        for window in range(phase["windows"]):
            directory = folder / f"window-{window:03d}"
            config = read(directory / "result.json")
            require(
                config["schema"] == 2
                and config["value_size"] == 128
                and config["warmup_ms"] == 1000
                and config["max_samples"] == 200000,
                "formal workload options differ",
            )
            observation = read(directory / "observation.json")
            resources = resource_rows(
                read(folder / f"resources-{window:03d}.json"), environment
            )
            deltas = {}
            for node, (a, b) in enumerate(
                zip(observation["before"], observation["after"]), 1
            ):
                require(
                    a["id"] == b["id"] == node
                    and b["last_index"] == b["commit_index"] == b["applied_index"]
                    and a["term"] == b["term"] == observation["before"][1]["term"],
                    "window convergence differs",
                )
                deltas[str(node)] = counter_delta(a, b)
                p = resources[f"node_{node}"]
                node_identities[node].add((p["pid"], p["start_ticks"]))
            mix = actual_mix(directory)
            require(
                mix["gets"] + mix["puts"] == observation["summary"]["successes"],
                "operation accounting differs",
            )
            row = dict(
                phase=index,
                window=window,
                cold_mib=phase["cold_mib"],
                encoder=phase["encoder"],
                write_percent=config["write_percent"],
                **observation["summary"],
                **mix,
                resources=resources,
                counter_deltas=deltas,
            )
            rows.append(row)
            phase_rows.append(row)
        require(
            all(len(ids) == 1 for ids in node_identities.values()),
            "node process changed within phase",
        )
        groups = []
        for percent in [0, 50, 90]:
            selected = [r for r in phase_rows if r["write_percent"] == percent]
            groups.append(
                dict(
                    write_percent=percent,
                    windows=len(selected),
                    attempts=sum(r["attempts"] for r in selected),
                    missed_slots=sum(r["missed_slots"] for r in selected),
                    median_ops_s=statistics.median(
                        r["successful_ops_s"] for r in selected
                    ),
                    median_p99_ms=statistics.median(r["p99_ms"] for r in selected),
                    median_p999_ms=statistics.median(r["p999_ms"] for r in selected),
                    median_scheduled_p99_ms=statistics.median(
                        r["scheduled_p99_ms"] for r in selected
                    ),
                    maximum_ms=max(r["max_ms"] for r in selected),
                )
            )
        before = read(folder / "window-000/observation.json")["before"][1]
        after = read(folder / f"window-{phase['windows']-1:03d}/observation.json")[
            "after"
        ][1]
        sampled = {}
        for name in ["node_1", "node_2", "node_3", "client"]:
            values = [r["resources"][name] for r in phase_rows]
            sampled[name] = dict(
                peak_rss_bytes=max(v["peak_rss_bytes"] for v in values),
                first_rss_bytes=values[0]["first_rss_bytes"],
                last_rss_bytes=values[-1]["last_rss_bytes"],
                average_cores=sum(v["cpu_seconds"] for v in values)
                / sum(v["sample_seconds"] for v in values),
            )
        phases.append(
            dict(
                phase=index,
                **phase,
                profiles=groups,
                missed_slots=sum(r["missed_slots"] for r in phase_rows),
                maximum_ms=max(r["max_ms"] for r in phase_rows),
                resources=sampled,
                leader_counter_delta_first_to_last=counter_delta(before, after),
                leader_process_lifetime_maxima_at_last_observation={
                    k: after[k] for k in MAXIMA
                },
                disk_latency_bounds_ns=after["disk_latency_bounds_ns"],
            )
        )
    return dict(
        total={
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
        },
        windows=len(rows),
        measured_seconds=progress["measured_seconds"],
        wall_seconds=(progress["finished_unix_ns"] - progress["started_unix_ns"]) / 1e9,
        started_unix_ns=progress["started_unix_ns"],
        finished_unix_ns=progress["finished_unix_ns"],
        maximum_ms=max(r["max_ms"] for r in rows),
        minimum_sampled_free_disk_bytes=min(
            r["resources"][f"node_{n}"]["minimum_free_disk_bytes"]
            for r in rows
            for n in [1, 2, 3]
        ),
        phases=phases,
        rows=rows,
        scope="Medians of correlated 60-second windows, not pooled percentiles or independent repetitions. Sampled CPU/RSS include setup and warmup. First-to-last leader counter intervals also include probes and gaps. Lifetime maxima cannot be differenced into window maxima.",
    )
