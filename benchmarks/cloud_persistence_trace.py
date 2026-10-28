#!/usr/bin/env python3
"""Same-process trace off/on windows on an isolated three-node experiment."""
import argparse
import json
from pathlib import Path
import signal
import subprocess
import time

from cloud_campaign import Fleet, save
from cloud_load import Connection
from cloud_soak import compress_samples, guard
from cpp_load import final_values, summarize

MODES = [False, True, True, False, False, True]


def wait_trace(fleet, enabled):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        states = fleet.settled()
        if all(s["trace_enabled"] == int(enabled) for s in states):
            return states
        time.sleep(0.1)
    raise TimeoutError("trace control did not settle")


def run(fleet, bench, output, smoke=False, shutdown_agents=False):
    seconds, cold_mib = (2, 1) if smoke else (30, 15)
    modes = [False, True] if smoke else MODES
    progress = dict(
        status="running",
        started_unix_ns=time.time_ns(),
        seconds=seconds,
        cold_mib=cold_mib,
        modes=modes,
        encoders=["sync", "background"],
        completed=[],
        measured_seconds=0,
        note="32 connections, 600 offered rps, 50% writes, 128B hot values; randomized request IDs in both groups; three VMs with C++ load on Follower 3. Correlated alternating windows, not independent repetitions or capacity measurements.",
    )
    save(output / "progress.json", progress)
    try:
        for encoder in progress["encoders"]:
            folder = output / encoder
            folder.mkdir()
            options = dict(
                case=output.name + "-" + encoder,
                encoder=encoder,
                proxies=False,
                snapshot_policy="adaptive",
                snapshot_entries=128,
                snapshot_bytes=1048576,
                journal_checkpoint_bytes=4194304,
                request_timeout_ms=1500,
                trace=True,
                trace_initially_enabled=False,
                trace_sample_every=1,
                trace_slow_ms=10,
                trace_max_events=500000,
            )
            fleet.all("start", **options)
            fleet.settled()
            fleet.select_leader(2)
            wait_trace(fleet, False)
            connection = Connection(fleet.endpoints[1])
            try:
                for key in range(cold_mib * 128):
                    if (
                        connection.call("put", f"cold-{key}".encode(), b"c" * 8192)[0]
                        != 0
                    ):
                        raise RuntimeError("cold preload failed")
            finally:
                connection.close()
            anchor = fleet.call("cas", b"trace-anchor", encoder.encode(), revision=0)
            if anchor["code"] != 0 or not anchor["revision"]:
                raise RuntimeError("anchor create failed")
            save(
                folder / "setup.json",
                dict(
                    options=options,
                    cold_keys=cold_mib * 128,
                    anchor_revision=anchor["revision"],
                    states=fleet.settled(),
                    processes=fleet.all("inspect"),
                ),
            )
            for index, enabled in enumerate(modes):
                progress.update(
                    encoder=encoder,
                    window=index,
                    trace_enabled=enabled,
                    last_update_unix_ns=time.time_ns(),
                )
                save(output / "progress.json", progress)
                fleet.all("trace", enabled=enabled)
                before = wait_trace(fleet, enabled)
                if before[1]["role"] != "leader":
                    raise RuntimeError("leader changed")
                target = folder / f"window-{index:02d}"
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
                    "50",
                    "--value-size",
                    "128",
                    "--offered-rps",
                    "600",
                    "--max-samples",
                    "200000",
                    "--trace-identity",
                    "1",
                    "--prefix",
                    "trace-hot",
                    "--output",
                    str(target),
                ]
                save(folder / f"command-{index:02d}.json", command)
                frames = []
                with (folder / f"client-{index:02d}.log").open("w") as log:
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
                            save(folder / f"resources-{index:02d}.json", frames)
                            guard(servers)
                            if time.monotonic() - began > seconds + 120:
                                raise TimeoutError("client stalled")
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
                summary = summarize(target)
                compress_samples(target)
                after = fleet.settled()
                save(
                    target / "observation.json",
                    dict(enabled=enabled, summary=summary, before=before, after=after),
                )
                if process.returncode or not summary["valid"] or summary["failures"]:
                    raise RuntimeError("failed load window, retained without replay")
                if (
                    after[1]["role"] != "leader"
                    or before[1]["term"] != after[1]["term"]
                ):
                    raise RuntimeError("leader changed during window")
                if any(
                    s["trace_failed"] or s["trace_limited"] or s["trace_dropped"]
                    for s in after
                ):
                    raise RuntimeError("incomplete diagnostic capture")
                if (
                    sum(p.stat().st_size for p in output.rglob("*") if p.is_file())
                    > 512 * 1024**2
                ):
                    raise RuntimeError("client result budget exceeded")
                progress["completed"].append(
                    dict(
                        encoder=encoder, window=index, enabled=enabled, summary=summary
                    )
                )
                progress["measured_seconds"] += seconds
                progress["last_update_unix_ns"] = time.time_ns()
                save(output / "progress.json", progress)
                print(
                    json.dumps(
                        dict(encoder=encoder, window=index, enabled=enabled, **summary)
                    ),
                    flush=True,
                )
            fleet.all("stop")
            fleet.all("start", **options)
            fleet.settled()
            for key in range(cold_mib * 128):
                fleet.assert_value(f"cold-{key}".encode(), b"c" * 8192)
            restored = fleet.call("get", b"trace-anchor")
            if (
                restored["code"] != 0
                or restored["revision"] != anchor["revision"]
                or restored["value"] != encoder.encode()
            ):
                raise RuntimeError("anchor value/revision changed on restart")
            hot = final_values(target)
            for key, value in hot.items():
                fleet.assert_value(key.encode(), value)
            save(
                folder / "restart-verification.json",
                dict(
                    verified=True,
                    cold_keys=cold_mib * 128,
                    hot_keys=len(hot),
                    anchor_revision=anchor["revision"],
                    states=fleet.settled(),
                ),
            )
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
