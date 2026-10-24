#!/usr/bin/env python3
"""Whitelisted legacy migration and bounded-admission recovery probes."""
import argparse, concurrent.futures, json, socket, struct, threading
from pathlib import Path
from cloud_campaign import Fleet, rpc, save
import rpc_baseline as wire


def encoded(response):
    return {k: (v.hex() if isinstance(v, bytes) else v) for k, v in response.items()}


def probes(fleet, output):
    legacy = "legacy-upgrade"
    events = []
    try:
        fleet.all(
            "start",
            case=legacy,
            program="legacy",
            encoder="sync",
            proxies=False,
            snapshot_entries=8,
            snapshot_bytes=32768,
        )
        fleet.settled()
        fleet.select_leader(2)
        for i in range(32):
            assert fleet.call("put", f"old-{i}".encode(), b"legacy-value")["code"] == 0
        old_states = fleet.settled()
        assert all(s["snapshot_index"] > 0 for s in old_states)
        fleet.all("stop")
        fleet.all(
            "start",
            case=legacy,
            program="new",
            encoder="background",
            proxies=False,
            snapshot_entries=8,
            snapshot_bytes=32768,
        )
        fleet.settled()
        fleet.select_leader(2)
        old = fleet.call("get", b"old-0")
        assert (
            old["code"] == 0
            and old["value"] == b"legacy-value"
            and old["revision"] == 0
        )
        events.append(dict(step="read-v1", response=encoded(old)))
        migration = fleet.call("cas", b"old-0", b"must-not-overwrite", 0)
        assert (
            migration["code"] == 7
            and migration["value"] == b"legacy-value"
            and migration["revision"] > 0
        )
        events.append(dict(step="migration-conflict", response=encoded(migration)))
        revision = fleet.call("get", b"old-0")["revision"]
        leader = fleet.leader()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda v: rpc(
                        fleet.endpoints[leader], "cas", b"old-0", v, revision
                    ),
                    [b"publisher-a", b"publisher-b"],
                )
            )
        assert sorted(r["code"] for r in results) == [0, 7]
        events.append(
            dict(step="two-publishers", responses=[encoded(r) for r in results])
        )
        winner = fleet.call("get", b"old-0")
        fleet.select_leader(3)
        after = fleet.call("get", b"old-0")
        assert after == winner
        events.append(
            dict(step="leader-change", response=encoded(after), states=fleet.settled())
        )
        fleet.all("stop")
        fleet.all(
            "start",
            case=legacy,
            program="new",
            encoder="background",
            proxies=False,
            snapshot_entries=8,
            snapshot_bytes=32768,
        )
        fleet.settled()
        assert fleet.call("get", b"old-0") == winner
        assert fleet.call("del", b"old-0")["code"] == 0
        recreated = fleet.call("cas", b"old-0", winner["value"], 0)
        assert recreated["code"] == 0 and recreated["revision"] > winner["revision"]
        stale = fleet.call("cas", b"old-0", b"ABA-stale", winner["revision"])
        assert stale["code"] == 7
        events.append(
            dict(
                step="delete-recreate-ABA",
                created=encoded(recreated),
                stale=encoded(stale),
            )
        )
        for i in range(1, 32):
            fleet.assert_value(f"old-{i}".encode(), b"legacy-value")
        save(
            output / "upgrade.json",
            dict(
                verified=True,
                old_states=old_states,
                events=events,
                final_states=fleet.settled(),
                legacy_values_checked=31,
            ),
        )
    finally:
        fleet.all("stop")
    case = "admission-probe"
    fleet.all(
        "start",
        case=case,
        encoder="sync",
        proxies=False,
        pending_limit=4,
        event_queue=8,
        snapshot_entries=128,
    )
    fleet.settled()
    fleet.select_leader(2)
    leader = fleet.leader()
    barrier = threading.Barrier(65)

    def client(number):
        with socket.create_connection(fleet.endpoints[leader], timeout=3) as connection:
            connection.settimeout(5)
            barrier.wait(timeout=10)
            frames = []
            for i in range(16):
                packet = b"\x08" + wire.varint(i + 1) + wire.bytes_field(5, b"")
                frames.append(struct.pack("!I", len(packet)) + packet)
            connection.sendall(b"".join(frames))
            codes = []
            for i in range(16):
                length = struct.unpack("!I", wire.read_exact(connection, 4))[0]
                response = wire.decode_response(wire.read_exact(connection, length))
                assert response[1] == i + 1
                codes.append(response.get(2, 0))
            return dict(client=number, codes=codes)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            jobs = [pool.submit(client, i) for i in range(64)]
            barrier.wait(timeout=10)
            results = [job.result(timeout=15) for job in jobs]
        codes = [code for c in results for code in c["codes"]]
        assert len(codes) == 1024 and all(c in [0, 4] for c in codes) and 4 in codes
        assert fleet.call("put", b"recovered", b"healthy")["code"] == 0
        fleet.assert_value(b"recovered", b"healthy")
        states = fleet.settled()
        assert all(s["pending"] <= 4 and s["read_pending"] <= 4 for s in states)
        save(
            output / "admission.json",
            dict(
                verified=True,
                attempts=len(codes),
                ok=codes.count(0),
                busy=codes.count(4),
                clients=results,
                states=states,
                recovered=True,
                pending_limit=4,
                event_queue=8,
            ),
        )
    finally:
        fleet.all("stop")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    try:
        probes(Fleet(json.loads(a.config.read_text())), a.output)
    except BaseException as error:
        save(a.output / "failure.json", dict(error=repr(error)))
        raise


if __name__ == "__main__":
    main()
