#!/usr/bin/env python3
"""Counterbalanced before/after disk admission and default-sync comparison."""
import argparse
import json
from pathlib import Path
import signal

from cloud_async_disk import phase_options as disk_options, run
from cloud_campaign import Fleet


def plan(smoke=False):
    variants = ["before", "after", "sync"]
    rows = []
    for size_index, size in enumerate([1] if smoke else [1, 15]):
        for repeat in range(1 if smoke else 3):
            order = variants[repeat:] + variants[:repeat]
            if size_index % 2:
                order.reverse()
            for variant in order:
                rows.append(
                    dict(
                        cold_mib=size,
                        repeat=repeat,
                        variant=variant,
                        executor="sync" if variant == "sync" else "background",
                        label=f"m{size}-r{repeat}-{variant}",
                    )
                )
    return rows


def phase_options(row, case):
    if row["variant"] not in ("before", "after", "sync") or row["executor"] != (
        "sync" if row["variant"] == "sync" else "background"
    ):
        raise ValueError("variant and executor differ")
    options = disk_options(row, case)
    if row["variant"] == "before":
        options.update(case="baseline-" + case, program="baseline")
    else:
        options["program"] = "new"
    return options


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--bench", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--shutdown-agents", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, mode=0o700, exist_ok=False)
    signal.signal(
        signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError("terminated"))
    )
    result = run(
        Fleet(json.loads(args.config.read_text())),
        args.bench,
        args.output,
        smoke=args.smoke,
        shutdown_agents=args.shutdown_agents,
        phases=plan(args.smoke),
        window_seconds=2 if args.smoke else 30,
        phase_builder=phase_options,
        note="before=frozen old background binary; after=new background binary; sync=new default binary; KV encoder background; counterbalanced 3 variants, 3 repeats per size; trace recorder absent; 32 clients/600 offered rps/50% writes/128B; load shares Follower3, not isolated capacity",
    )
    print(json.dumps(dict(status=result["status"], windows=len(result["completed"]))))


if __name__ == "__main__":
    main()
