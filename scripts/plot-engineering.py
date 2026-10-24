#!/usr/bin/env python3
"""Plot verifier output; requires matplotlib, which is not a service dependency."""
import argparse
import json
import statistics
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("analysis", type=Path)
    p.add_argument(
        "--output", type=Path, required=True, help="Output path without suffix"
    )
    a = p.parse_args()
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    data = json.loads(a.analysis.read_text())
    assert data["verified"]
    rows = [r for r in data["rows"] if not r["exploratory"]]
    colors = ["#526582", "#007f82", "#c26324"]
    plt.rcParams.update(
        {"font.size": 11, "axes.spines.top": False, "axes.spines.right": False}
    )
    fig, axes = plt.subplots(2, 2, figsize=(13.5, 9.0))
    fig.suptitle(
        "yewukv | Three-VM engineering experiments",
        fontsize=20,
        x=0.07,
        ha="left",
        y=0.98,
    )
    fig.text(
        0.07,
        0.935,
        "2 vCPU per VM · Leader on node 2 · two load processes share Follower node 3",
        color="#455166",
    )

    for ax, metric, title, ylabel in [
        (
            axes[0, 0],
            lambda r: r["successful_ops_s"],
            "A  Snapshot throughput",
            "Successful operations / second",
        ),
        (
            axes[0, 1],
            lambda r: r["latency_success_ms"]["p99_ms"],
            "B  Snapshot P99 latency",
            "Client latency (ms)",
        ),
    ]:
        for i, mode in enumerate(["sync", "background"]):
            values = [
                metric(r)
                for r in rows
                if r["group"] == "snapshot-fixed" and r["config"]["encoder"] == mode
            ]
            median = statistics.median(values)
            ax.bar(i, median, color=colors[i], width=0.56, alpha=0.83)
            ax.errorbar(
                i,
                median,
                yerr=[[median - min(values)], [max(values) - median]],
                color="#17293e",
                capsize=7,
            )
            ax.scatter([i - 0.10, i, i + 0.10], values, color="#17293e", s=28, zorder=4)
            ax.text(
                i,
                max(values) + max(values) * 0.07,
                f"{median:,.1f}",
                ha="center",
                fontweight="bold",
            )
        ax.set_xticks([0, 1], ["Sync", "Background"])
        ax.set_ylim(0, ax.get_ylim()[1] * 1.18)
        ax.set_title(title, loc="left", fontweight="bold", pad=12)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.18)
        ax.set_axisbelow(True)
    axes[0, 0].text(
        0.03,
        0.91,
        "+40.5% median",
        transform=axes[0, 0].transAxes,
        color=colors[1],
        fontweight="bold",
    )
    axes[0, 1].text(
        0.97,
        0.91,
        "−34.9% median",
        transform=axes[0, 1].transAxes,
        ha="right",
        color=colors[1],
        fontweight="bold",
    )

    ax = axes[1, 0]
    for color, write in zip(colors, [0, 50, 90]):
        medians, lows, highs = [], [], []
        for clients in [8, 32, 128]:
            values = [
                r["successful_ops_s"]
                for r in rows
                if r["group"] == "capacity-fixed"
                and r["config"]["write_percent"] == write
                and r["config"]["clients"] == clients
                and not r["config"]["offered_rps"]
            ]
            m = statistics.median(values)
            medians.append(m)
            lows.append(m - min(values))
            highs.append(max(values) - m)
        ax.errorbar(
            [0, 1, 2],
            medians,
            yerr=[lows, highs],
            label=f"{write}% writes",
            color=color,
            marker="o",
            capsize=4,
        )
    ax.set_xticks([0, 1, 2], ["8", "32", "128"])
    ax.set_xlabel("Concurrent connections (one request in flight per connection)")
    ax.set_ylabel("Successful operations / second")
    ax.set_title("C  Closed-loop load", loc="left", fontweight="bold", pad=12)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=0.18)
    ax.legend(frameon=False, loc="upper left")

    ax = axes[1, 1]
    for i, rate in enumerate([500, 2500, 10000]):
        selected = [r for r in rows if r["config"]["offered_rps"] == rate]
        values = [r["successful_ops_s"] for r in selected]
        median = statistics.median(values)
        missed = sum(r["missed_slots"] for r in selected)
        offered = sum(r["offered_slots"] for r in selected)
        ax.bar(i, rate, color="#dfe5ec", width=0.66)
        ax.bar(i, median, color=colors[1], width=0.48)
        ax.scatter([i - 0.10, i, i + 0.10], values, color="#17293e", s=26, zorder=4)
        ax.text(i, rate + 350, f"{missed/offered:.2%} missed", ha="center", fontsize=10)
    ax.set_xticks([0, 1, 2], ["500", "2,500", "10,000"])
    ax.set_xlabel("Scheduled arrivals / second (50% writes, 32 connections)")
    ax.set_ylabel("Operations / second")
    ax.set_title(
        "D  Paced arrivals: offered vs completed", loc="left", fontweight="bold", pad=12
    )
    ax.set_ylim(0, 11900)
    ax.grid(axis="y", alpha=0.18)
    ax.text(
        0.03,
        0.93,
        "Gray: offered · Teal: completed median",
        transform=ax.transAxes,
        color="#455166",
        fontsize=10,
    )
    for ax in axes.flat:
        ax.ticklabel_format(axis="y", style="plain")
    fig.subplots_adjust(
        left=0.075, right=0.975, top=0.86, bottom=0.17, hspace=0.53, wspace=0.28
    )
    fig.text(
        0.075,
        0.09,
        "Bars/lines: median of 3 runs; error bars: min–max; dots: individual runs. Percentiles are not pooled.",
        fontsize=10,
    )
    fig.text(
        0.075,
        0.061,
        "Snapshot stress: 8 MiB cold values, fixed 128-entry trigger, 32 connections, 50% writes, 10 s + 2 s warmup.",
        fontsize=10,
    )
    fig.text(
        0.075,
        0.032,
        "Paced clients skip unserved slots; missed arrivals have no measured latency. Co-location limits capacity claims; disk publication still blocks.",
        fontsize=9,
        color="#455166",
    )
    a.output.parent.mkdir(parents=True, exist_ok=True)
    for suffix in [".png", ".svg"]:
        fig.savefig(a.output.with_suffix(suffix), dpi=165, facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    main()
