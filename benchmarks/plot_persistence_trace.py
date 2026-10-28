#!/usr/bin/env python3
"""Render the published window variance and measured slow-GET intersections."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def plot(root):
    data = json.loads((root / "derived/summary.json").read_text())
    figure, axes = plt.subplots(1, 3, figsize=(15, 4.6), constrained_layout=True)
    for axis, encoder in zip(axes[:2], ["sync", "background"]):
        rows = [r for r in data["rows"] if r["encoder"] == encoder]
        axis.plot(
            [r["window"] for r in rows],
            [r["p99_ms"] for r in rows],
            color="#8792a2",
            linewidth=1,
        )
        for enabled, color in [(False, "#3274a1"), (True, "#e1812c")]:
            selected = [r for r in rows if r["enabled"] == enabled]
            axis.scatter(
                [r["window"] for r in selected],
                [r["p99_ms"] for r in selected],
                color=color,
                label="Trace on" if enabled else "Trace off",
                s=55,
                zorder=3,
            )
        axis.set(
            title=encoder + ": all-request P99",
            xlabel="30 s window (fixed order)",
            ylabel="Milliseconds",
            xticks=range(6),
            ylim=(0, 90),
        )
        axis.grid(axis="y", alpha=0.2)
        axis.legend(frameon=False)
    axis = axes[2]
    stages = [
        ("file_sync_overlap_ns", "File sync", "#3274a1"),
        ("encode_overlap_ns", "Encode (includes CRC)", "#e1812c"),
        ("write_overlap_ns", "Write", "#3a923a"),
        ("directory_sync_overlap_ns", "Directory sync", "#c03d3e"),
        ("rename_overlap_ns", "Rename", "#9372b2"),
    ]
    enabled_groups = [g for g in data["groups"] if g["enabled"]]
    for y, group in enumerate(enabled_groups):
        fractions = group["timeline"]["slow_time_weighted_overlap_fractions"]
        left = 0
        for key, label, color in stages:
            value = fractions[key] * 100
            axis.barh(y, value, left=left, color=color, label=label if y == 0 else None)
            left += value
        residual = fractions["persist_overlap_ns"] * 100 - left
        axis.barh(
            y,
            residual,
            left=left,
            color="#8c613c",
            label="Other traced persist" if y == 0 else None,
        )
        left += residual
        axis.barh(
            y,
            100 - left,
            left=left,
            color="#d6d6d6",
            label="Outside traced persist" if y == 0 else None,
        )
    axis.set(
        title="Slow GETs (client >= 20 ms)\nRequest-weighted server intervals",
        xlabel="Share of receive-to-ready wall time (%)",
        yticks=[0, 1],
        yticklabels=[
            f"{g['encoder']} ({g['timeline']['joined_slow_gets']})"
            for g in enabled_groups
        ],
        xlim=(0, 100),
    )
    axis.legend(
        loc="upper center",
        bbox_to_anchor=(0.48, -0.2),
        frameon=False,
        ncol=2,
        fontsize=8,
    )
    figure.suptitle(
        "Same-process observation: correlated windows; interval overlap does not establish device latency or causality",
        fontsize=11,
    )
    out = root / "figures/tracing.png"
    out.parent.mkdir(exist_ok=True)
    figure.savefig(out, dpi=150)
    plt.close(figure)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    plot(parser.parse_args().root)
