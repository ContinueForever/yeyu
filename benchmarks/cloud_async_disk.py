#!/usr/bin/env python3
"""Trace-free, paired disk-executor comparison in fresh isolated cloud data."""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import time

from cloud_campaign import Fleet, rpc, save
from cloud_load import Connection
from cloud_soak import compress_samples, guard
from cpp_load import final_values, summarize


def plan(smoke=False):
    rows = []
    for size_index, size in enumerate([1] if smoke else [1, 15]):
        for repeat in range(1 if smoke else 3):
            order = ["sync", "background"]
            if (size_index + repeat) % 2:
                order.reverse()
            for executor in order:
                rows.append(dict(cold_mib=size, repeat=repeat, executor=executor))
    return rows


def phase_options(row, case):
    if row["executor"] not in ("sync", "background") or row["cold_mib"] not in (1, 15):
        raise ValueError("invalid disk comparison phase")
    if not 0 <= row["repeat"] < 3:
        raise ValueError("invalid repeat")
    return dict(
        case=case,
        encoder="background",
        disk_executor=row["executor"],
        proxies=False,
        snapshot_policy="adaptive",
        snapshot_entries=128,
        snapshot_bytes=1048576,
        journal_checkpoint_bytes=4194304,
        request_timeout_ms=1500,
        trace=False,
    )


def check_states(states, executor, fixed_leader=True):
    if len(states) != 3 or not all(states):
        raise ValueError("three node states required")
    for state in states:
        if (
            state["disk_executor"] != executor
            or state["snapshot_encoder"] != "background"
            or state["snapshot_policy"] != "adaptive"
            or state["storage_mode"] != "journal"
            or state["read_mode"] != "read-index"
            or state["trace_enabled"]
            or state["trace_written"]
            or state["trace_dropped"]
            or state["trace_failed"]
        ):
            raise ValueError("comparison configuration differs or tracing is active")
    if fixed_leader and states[1]["role"] != "leader":
        raise ValueError("fixed leader changed")


def record_value(response):
    return dict(
        code=response["code"],
        revision=response.get("revision", 0),
        bytes=len(response["value"]),
        sha256=hashlib.sha256(response["value"]).hexdigest(),
    )


def run(
    fleet,
    bench,
    output,
    smoke=False,
    shutdown_agents=False,
    *,
    phases=None,
    window_seconds=None,
    phase_builder=phase_options,
    note=None,
):
    seconds = (2 if smoke else 30) if window_seconds is None else window_seconds
    phases = plan(smoke) if phases is None else phases
    if type(seconds) is not int or not 1 <= seconds <= 120 or not phases:
        raise ValueError("invalid bounded comparison window")
    progress = dict(
        status="running",
        started_unix_ns=time.time_ns(),
        plan=phases,
        seconds=seconds,
        completed=[],
        measured_seconds=0,
        note=note
        or "same binary; trace recorder absent; background KV encoder fixed; 32 clients, 600 offered rps, 50% writes, 128B; fresh data per phase, three paired repeats per size; load shares Follower 3, not isolated capacity",
    )
    save(output / "progress.json", progress)
    try:
        for position, row in enumerate(phases):
            label = row.get(
                "label", f"m{row['cold_mib']}-r{row['repeat']}-{row['executor']}"
            )
            folder = output / label
            folder.mkdir()
            options = phase_builder(row, output.name + "-" + label)
            progress.update(
                position=position, phase=row, last_update_unix_ns=time.time_ns()
            )
            save(output / "progress.json", progress)
            launched = fleet.all("start", **options)
            fleet.settled()
            fleet.select_leader(2)
            guard(fleet.all("inspect"))
            check_states(fleet.settled(), row["executor"])
            cold_keys = row["cold_mib"] * 128
            connection = Connection(fleet.endpoints[1])
            try:
                for key in range(cold_keys):
                    if (
                        connection.call("put", f"cold-{key}".encode(), b"c" * 8192)[0]
                        != 0
                    ):
                        raise RuntimeError("cold preload failed")
                    if key % 256 == 255:
                        guard(fleet.all("inspect"))
            finally:
                connection.close()
            anchor_value = label.encode()
            anchor = fleet.call("cas", b"disk-anchor", anchor_value, revision=0)
            if anchor["code"] != 0 or not anchor["revision"]:
                raise RuntimeError("anchor create failed")
            before = fleet.settled()
            check_states(before, row["executor"])
            save(
                folder / "setup.json",
                dict(
                    options=options,
                    launched=launched,
                    cold_keys=cold_keys,
                    anchor=record_value(anchor),
                    anchor_value=anchor_value.hex(),
                    states=before,
                ),
            )
            target = folder / "window"
            argv = [
                str(bench),
                "--host",
                fleet.config["hosts"][1],
                "--port",
                "19101",
                "--clients",
                "32",
                "--duration-ms",
                str(seconds * 1000),
                "--warmup-ms",
                "1000",
                "--write-percent",
                "50",
                "--value-size",
                "128",
                "--offered-rps",
                "600",
                "--max-samples",
                "200000",
                "--prefix",
                "disk-hot",
                "--output",
                str(target),
            ]
            save(folder / "command.json", argv)
            frames = []

            def observe():
                servers = fleet.all("inspect")
                guard(servers)
                frame = dict(monotonic_ns=time.monotonic_ns(), servers=servers)
                if process is not None:
                    try:
                        frame["client_stat"] = (
                            Path("/proc") / str(process.pid) / "stat"
                        ).read_text()
                    except FileNotFoundError:
                        pass
                frames.append(frame)
                save(folder / "resources.json", frames)

            process = None
            observe()
            with (folder / "client.log").open("w") as log:
                process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
                try:
                    began = time.monotonic()
                    while process.poll() is None:
                        observe()
                        if time.monotonic() - began > seconds + 60:
                            raise TimeoutError("bounded client stalled")
                        try:
                            process.wait(timeout=2 if smoke else 5)
                        except subprocess.TimeoutExpired:
                            pass
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
            observe()
            summary = summarize(target)
            compress_samples(target)
            after = fleet.settled()
            check_states(after, row["executor"])
            save(
                folder / "observation.json",
                dict(summary=summary, before=before, after=after),
            )
            if process.returncode or not summary["valid"] or summary["failures"]:
                raise RuntimeError(
                    "load window failed; evidence retained without replay"
                )
            if before[1]["term"] != after[1]["term"]:
                raise RuntimeError("leader term changed during window")
            # Stop/reopen only this experiment, then read cold, hot and revision.
            fleet.all("stop")
            fleet.all("start", **options)
            restored = fleet.settled()
            check_states(restored, row["executor"], fixed_leader=False)
            recovered_leader = fleet.leader()
            verified = {}
            for key in range(cold_keys):
                name = f"cold-{key}"
                response = rpc(fleet.endpoints[recovered_leader], "get", name.encode())
                if response["code"] != 0 or response["value"] != b"c" * 8192:
                    raise RuntimeError("cold value changed after full restart")
                verified[name] = record_value(response)
            for key, value in final_values(target).items():
                response = rpc(fleet.endpoints[recovered_leader], "get", key.encode())
                if response["code"] != 0 or response["value"] != value:
                    raise RuntimeError("hot value changed after full restart")
                verified[key] = record_value(response)
            response = fleet.call("get", b"disk-anchor")
            if (
                response["code"] != 0
                or response["revision"] != anchor["revision"]
                or response["value"] != anchor_value
            ):
                raise RuntimeError("anchor value/revision changed after full restart")
            save(
                folder / "restart-verification.json",
                dict(
                    verified=True,
                    cold_keys=cold_keys,
                    hot_keys=256,
                    anchor=record_value(response),
                    values=verified,
                    states=fleet.settled(),
                ),
            )
            guard(fleet.all("inspect"))
            fleet.all("stop")
            if (
                sum(p.stat().st_size for p in output.rglob("*") if p.is_file())
                > 384 * 1024**2
            ):
                raise RuntimeError("result budget exceeded")
            progress["completed"].append(
                dict(position=position, **row, summary=summary)
            )
            progress["measured_seconds"] += seconds
            progress["last_update_unix_ns"] = time.time_ns()
            save(output / "progress.json", progress)
            print(json.dumps(dict(position=position, **row, **summary)), flush=True)
        progress["status"] = "complete"
    except BaseException as error:
        progress.update(status="failed", error=repr(error))
        raise
    finally:
        errors = []
        for action in ["stop", "finish"] if shutdown_agents else ["stop"]:
            try:
                fleet.all(action)
            except Exception as error:
                errors.append(dict(action=action, error=repr(error)))
        progress.update(finished_unix_ns=time.time_ns(), cleanup_errors=errors)
        if errors and progress["status"] == "complete":
            progress["status"] = "cleanup_failed"
        save(output / "progress.json", progress)
    return progress


if __name__ == "__main__":
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
        args.smoke,
        args.shutdown_agents,
    )
    print(
        json.dumps(
            dict(status=result["status"], measured_seconds=result["measured_seconds"])
        )
    )
