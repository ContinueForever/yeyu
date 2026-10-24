#!/usr/bin/env python3
"""Read long-window evidence without executing its archived tools."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from cpp_load import summarize, read, require, final_values
from cloud_soak import plan


def verify(root):
    progress = read(root / "progress.json")
    require(
        progress["status"] == "complete" and not progress["cleanup_errors"],
        "campaign incomplete",
    )
    smoke = progress["window_seconds"] == 2
    wanted = plan([1] if smoke else [1, 8, 15], 1 if smoke else 60)
    require(
        progress["plan"] == wanted
        and progress["completed_phases"] == list(range(len(wanted))),
        "phase plan differs",
    )
    total = dict(windows=0, attempts=0, failures=0, missed_slots=0, offered_slots=0)
    for index, phase in enumerate(wanted):
        folder = root / f"phase-{index:02d}"
        setup = read(folder / "setup.json")
        require(
            setup["config"] == phase
            and setup["options"]["snapshot_policy"] == "adaptive"
            and setup["options"]["journal_checkpoint_bytes"] == 4194304
            and setup["options"]["request_timeout_ms"] == 1500,
            "default policy differs",
        )
        for window in range(phase["windows"]):
            directory = folder / f"window-{window:03d}"
            row = summarize(directory)
            config = read(directory / "result.json")
            require(
                row["valid"]
                and not row["failures"]
                and config["duration_ms"] == progress["window_seconds"] * 1000
                and config["offered_rps"] == 600
                and config["clients"] == 32
                and config["write_percent"] == [0, 50, 90][window % 3],
                "load configuration differs",
            )
            observation = read(directory / "observation.json")
            require(observation["summary"] == row, "window summary differs")
            a, b = observation["before"], observation["after"]
            require(
                len(a) == len(b) == 3
                and a[1]["role"] == b[1]["role"] == "leader"
                and a[1]["term"] == b[1]["term"],
                "leader changed",
            )
            require(
                all(
                    s["snapshot_policy"] == "adaptive"
                    and s["snapshot_encoder"] == phase["encoder"]
                    and s["storage_mode"] == "journal"
                    for s in a + b
                ),
                "runtime policy differs",
            )
            total["windows"] += 1
            for k in ["attempts", "failures", "missed_slots", "offered_slots"]:
                total[k] += row[k]
        recovery = read(folder / "restart-verification.json")
        require(
            recovery["verified"]
            and recovery["cold_keys"] == phase["cold_mib"] * 128
            and recovery["hot_keys"] == len(final_values(directory)) == 256
            and recovery["anchor_revision"] == setup["anchor_revision"],
            "recovery differs",
        )
    require(
        total["windows"] == progress["completed_windows"]
        and total["windows"] * progress["window_seconds"]
        == progress["measured_seconds"],
        "window count differs",
    )
    return dict(
        verified=True,
        smoke=smoke,
        measured_seconds=progress["measured_seconds"],
        **total,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.root)))
