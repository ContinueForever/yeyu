#!/usr/bin/env python3
"""Bounded, restart-checked cloud stability windows in isolated experiment data."""
import argparse
import concurrent.futures
import gzip
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

from cloud_campaign import Fleet, rpc, save
from cloud_load import Connection
from cpp_load import summarize, final_values


def plan(cold_mib, windows):
    return [
        dict(cold_mib=size, encoder=encoder, windows=windows)
        for i, size in enumerate(cold_mib)
        for encoder in (
            ["sync", "background"] if i % 2 == 0 else ["background", "sync"]
        )
    ]


def guard(frames):
    if len(frames) != 3:
        raise RuntimeError("missing resource samples")
    for frame in frames:
        props = dict(
            line.split("=", 1) for line in frame["original_service"].splitlines()
        )
        if (
            props.get("ActiveState") != "active"
            or props.get("NRestarts") != "0"
            or props.get("DropInPaths")
        ):
            raise RuntimeError("original service differs; stop only experiment")
        if frame["free_disk_bytes"] < 2 * 1024**3:
            raise RuntimeError("low free disk")
        if "stat" not in frame:
            raise RuntimeError("temporary node stopped")
        stat = frame["stat"].rsplit(")", 1)[1].split()
        if int(stat[21]) * os.sysconf("SC_PAGE_SIZE") > 480 * 1024**2:
            raise RuntimeError("experiment node RSS limit")


def phase_options(phase, case, disk_executor=None):
    options = dict(
        case=case,
        encoder=phase["encoder"],
        proxies=False,
        snapshot_policy="adaptive",
        snapshot_entries=128,
        snapshot_bytes=1048576,
        journal_checkpoint_bytes=4194304,
        request_timeout_ms=1500,
    )
    if disk_executor is not None:
        if disk_executor not in ("sync", "background"):
            raise ValueError("invalid disk executor")
        options.update(disk_executor=disk_executor, trace=False)
    return options


def check_disk_executor(states, disk_executor):
    if disk_executor is None:
        return
    if len(states) != 3 or any(
        state["disk_executor"] != disk_executor
        or state["trace_enabled"]
        or state["trace_written"]
        or state["trace_dropped"]
        or state["trace_failed"]
        for state in states
    ):
        raise RuntimeError("long-run disk mode differs or tracing is active")


def compress_samples(folder):
    for path in folder.glob("client-*.json"):
        target = path.with_suffix(".json.gz")
        with path.open("rb") as source, target.open("wb") as output:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=output, mtime=0, compresslevel=1
            ) as stream:
                shutil.copyfileobj(source, stream)
        path.unlink()


def run(
    fleet,
    bench,
    output,
    phases,
    seconds,
    rate,
    shutdown_agents=False,
    disk_executor=None,
):
    progress = dict(
        status="running",
        started_unix_ns=time.time_ns(),
        plan=phases,
        window_seconds=seconds,
        offered_rps=rate,
        completed_phases=[],
        completed_windows=0,
        measured_seconds=0,
        original_services_protected=True,
        note="Three VMs; C++ load shares Follower 3. Default adaptive policy; one-core/512MiB node limits. Windows exclude setup; phase metrics include preload, warmup and probes.",
    )
    if disk_executor is not None:
        if disk_executor not in ("sync", "background"):
            raise ValueError("invalid disk executor")
        progress["disk_executor"] = disk_executor
    save(output / "progress.json", progress)
    result_bytes = 0
    try:
        for index, phase in enumerate(phases):
            case = f"{output.name}-p{index}"
            folder = output / f"phase-{index:02d}"
            folder.mkdir()
            options = phase_options(phase, case, disk_executor)
            launched = fleet.all("start", **options)
            fleet.settled()
            fleet.select_leader(2)
            cold_keys = phase["cold_mib"] * 128
            connection = Connection(fleet.endpoints[1])
            try:
                for key in range(cold_keys):
                    if (
                        connection.call("put", f"cold-{key}".encode(), b"c" * 8192)[0]
                        != 0
                    ):
                        raise RuntimeError("cold preload failed")
            finally:
                connection.close()
            anchor = fleet.call(
                "cas", b"soak-anchor", f"phase-{index}".encode(), revision=0
            )
            if anchor["code"] != 0 or not anchor["revision"]:
                raise RuntimeError("anchor create failed")
            setup = dict(
                config=phase,
                options=options,
                cold_keys=cold_keys,
                anchor_revision=anchor["revision"],
                states=fleet.settled(),
            )
            check_disk_executor(setup["states"], disk_executor)
            if disk_executor is not None:
                setup["launched"] = launched
            save(folder / "setup.json", setup)
            for window in range(phase["windows"]):
                progress.update(
                    phase=index, window=window, last_update_unix_ns=time.time_ns()
                )
                save(output / "progress.json", progress)
                before = fleet.settled()
                check_disk_executor(before, disk_executor)
                if before[1]["role"] != "leader":
                    raise RuntimeError("leader changed before window")
                target = folder / f"window-{window:03d}"
                command = [
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
                    str([0, 50, 90][window % 3]),
                    "--offered-rps",
                    str(rate),
                    "--max-samples",
                    "200000",
                    "--prefix",
                    "soak-hot",
                    "--output",
                    str(target),
                ]
                save(folder / f"command-{window:03d}.json", command)
                frames = []
                with (folder / f"client-{window:03d}.log").open("w") as log:
                    process = subprocess.Popen(
                        command, stdout=log, stderr=subprocess.STDOUT
                    )
                    try:
                        began = time.monotonic()
                        while process.poll() is None:
                            servers = fleet.all("inspect")
                            frame = dict(
                                monotonic_ns=time.monotonic_ns(), servers=servers
                            )
                            try:
                                frame["client_stat"] = (
                                    Path("/proc") / str(process.pid) / "stat"
                                ).read_text()
                            except FileNotFoundError:
                                pass
                            frames.append(frame)
                            save(folder / f"resources-{window:03d}.json", frames)
                            guard(servers)
                            if time.monotonic() - began > seconds + 120:
                                raise TimeoutError("load window stalled")
                            try:
                                process.wait(timeout=min(10, seconds))
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
                if not (target / "result.json").exists():
                    raise RuntimeError("load result absent")
                summary = summarize(target)
                compress_samples(target)
                after = fleet.settled()
                check_disk_executor(after, disk_executor)
                save(
                    target / "observation.json",
                    dict(summary=summary, before=before, after=after),
                )
                if process.returncode or not summary["valid"] or summary["failures"]:
                    raise RuntimeError("load window failed; retained without replay")
                if (
                    before[1]["term"] != after[1]["term"]
                    or after[1]["role"] != "leader"
                ):
                    raise RuntimeError("leader changed in healthy-load window")
                fleet.assert_value(b"soak-anchor", f"phase-{index}".encode())
                result_bytes = sum(
                    p.stat().st_size for p in output.rglob("*") if p.is_file()
                )
                if result_bytes > 1024**3:
                    raise RuntimeError("result disk budget exceeded")
                progress["completed_windows"] += 1
                progress["measured_seconds"] += seconds
                progress.update(
                    last_update_unix_ns=time.time_ns(),
                    last_window=summary,
                    result_bytes=result_bytes,
                )
                save(output / "progress.json", progress)
                print(
                    json.dumps(dict(phase=index, window=window, **summary)), flush=True
                )
            fleet.all("stop")
            fleet.all("start", **options)
            check_disk_executor(fleet.settled(), disk_executor)
            for key in range(cold_keys):
                fleet.assert_value(f"cold-{key}".encode(), b"c" * 8192)
            restored = fleet.call("get", b"soak-anchor")
            if (
                restored["code"] != 0
                or restored["revision"] != anchor["revision"]
                or restored["value"] != f"phase-{index}".encode()
            ):
                raise RuntimeError("anchor value/revision changed after restart")
            hot = final_values(folder / f"window-{phase['windows']-1:03d}")
            for key, value in hot.items():
                fleet.assert_value(key.encode(), value)
            save(
                folder / "restart-verification.json",
                dict(
                    verified=True,
                    cold_keys=cold_keys,
                    hot_keys=len(hot),
                    anchor_revision=anchor["revision"],
                    states=fleet.settled(),
                ),
            )
            progress["completed_phases"].append(index)
            fleet.all("stop")
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
        progress.update(
            finished_unix_ns=time.time_ns(),
            cleanup_errors=errors,
            shutdown_agents_requested=shutdown_agents,
        )
        if errors and progress["status"] == "complete":
            progress["status"] = "cleanup_failed"
        save(output / "progress.json", progress)
    return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--bench", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--shutdown-agents", action="store_true")
    parser.add_argument("--disk-executor", choices=("sync", "background"))
    a = parser.parse_args()
    a.output.mkdir(parents=True, mode=0o700, exist_ok=False)
    signal.signal(
        signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError("terminated"))
    )
    phases = plan([1] if a.smoke else [1, 8, 15], 1 if a.smoke else 60)
    result = run(
        Fleet(json.loads(a.config.read_text())),
        a.bench,
        a.output,
        phases,
        2 if a.smoke else 60,
        600,
        a.shutdown_agents,
        a.disk_executor,
    )
    print(
        json.dumps(
            dict(status=result["status"], measured_seconds=result["measured_seconds"])
        )
    )


if __name__ == "__main__":
    main()
