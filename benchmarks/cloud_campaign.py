#!/usr/bin/env python3
"""Run checked faults or bounded load against isolated temporary cloud nodes.

The private config contains a short-lived capability for cloud-node-agent.py.
It is never copied into result artifacts. All operation clocks are on this host.
"""
import argparse, concurrent.futures, gzip, json, math, multiprocessing as mp, os, random, socket, struct, sys, threading, time
from pathlib import Path
from urllib.request import Request, build_opener, ProxyHandler
import rpc_baseline as wire

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from linearizability import check


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def rpc(endpoint, kind, key=b"", value=b"", revision=0, timeout=2):
    operation = {"put": 2, "get": 3, "del": 4, "status": 6, "cas": 9}[kind]
    body = wire.bytes_field(1, key) if kind != "status" else b""
    if kind == "put":
        body += wire.bytes_field(2, value)
    if kind == "cas":
        body += wire.varint(16) + wire.varint(revision) + wire.bytes_field(3, value)
    with socket.create_connection(endpoint, timeout=timeout) as connection:
        connection.settimeout(timeout)
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connection.sendall(
            struct.pack(
                "!I", len(packet := b"\x08\x01" + wire.bytes_field(operation, body))
            )
            + packet
        )
        length = struct.unpack("!I", wire.read_exact(connection, 4))[0]
        if not 0 < length <= 65536:
            raise ValueError("invalid frame size")
        fields = wire.decode_response(wire.read_exact(connection, length))
        if fields.get(1) != 1:
            raise ValueError("response ID differs")
        return dict(
            code=fields.get(2, 0), value=fields.get(3, b""), revision=fields.get(9, 0)
        )


class Fleet:
    def __init__(self, config):
        self.config = config
        self.endpoints = [(host, 19101) for host in config["hosts"]]

    def agent(self, node, action, **values):
        data = json.dumps(dict(action=action, **values)).encode()
        request = Request(
            f'http://{self.config["hosts"][node-1]}:19190/',
            data=data,
            headers={
                "Authorization": "Bearer " + self.config["token"],
                "Content-Type": "application/json",
            },
        )
        with build_opener(ProxyHandler({})).open(request, timeout=30) as response:
            return json.load(response)

    def all(self, action, **values):
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(lambda n: self.agent(n, action, **values), [1, 2, 3]))

    def states(self):
        def query(endpoint):
            try:
                return json.loads(rpc(endpoint, "status", timeout=0.4)["value"])
            except (OSError, ValueError):
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(query, self.endpoints))

    def leader(self, seconds=5):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            states = self.states()
            leaders = [i for i, s in enumerate(states) if s and s["role"] == "leader"]
            if leaders:
                return max(leaders, key=lambda i: states[i]["term"])
            time.sleep(0.02)
        raise TimeoutError("no leader")

    def settled(self, seconds=15):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            states = self.states()
            if (
                all(states)
                and len(
                    {
                        (
                            s["term"],
                            s["leader_id"],
                            s["last_index"],
                            s["commit_index"],
                            s["applied_index"],
                        )
                        for s in states
                    }
                )
                == 1
                and sum(s["role"] == "leader" for s in states) == 1
            ):
                return states
            time.sleep(0.05)
        raise TimeoutError("cluster did not settle")

    def select_leader(self, node=2):
        if self.leader() == node - 1:
            return self.settled()
        paused = []
        try:
            for other in [1, 2, 3]:
                if other != node:
                    self.agent(other, "signal", signal="STOP")
                    paused.append(other)
            until = time.monotonic() + 6
            while time.monotonic() < until:
                state = json.loads(
                    rpc(self.endpoints[node - 1], "status", timeout=0.5)["value"]
                )
                if state["role"] == "candidate":
                    break
                time.sleep(0.05)
            else:
                raise TimeoutError("target did not campaign")
            resumed = paused.pop(0)
            self.agent(resumed, "signal", signal="CONT")
            until = time.monotonic() + 6
            while time.monotonic() < until:
                state = json.loads(
                    rpc(self.endpoints[node - 1], "status", timeout=0.5)["value"]
                )
                if state["role"] == "leader":
                    break
                time.sleep(0.05)
            else:
                raise TimeoutError("target did not become leader")
        finally:
            for other in paused:
                self.agent(other, "signal", signal="CONT")
        states = self.settled()
        assert self.leader() == node - 1
        return states

    def call(self, kind, key, value=b"", revision=0):
        until = time.monotonic() + 8
        while time.monotonic() < until:
            response = rpc(self.endpoints[self.leader()], kind, key, value, revision)
            if response["code"] != 5:
                return response
        raise TimeoutError("leader redirect deadline")

    def assert_value(self, key, value):
        response = self.call("get", key)
        assert response["code"] == 0 and response["value"] == value, (key, response)

    def heal(self):
        self.all("rules")
        for node in [1, 2, 3]:
            try:
                self.agent(node, "signal", signal="CONT")
            except Exception:
                pass


def fault_campaign(fleet, output, repeats):
    case = "fault-campaign"
    fleet.all(
        "start",
        case=case,
        encoder="background",
        proxies=True,
        snapshot_entries=64,
        snapshot_bytes=65536,
    )
    fleet.settled()
    assert fleet.call("put", b"durable-anchor", b"acknowledged")["code"] == 0
    ring = {}
    summaries = []
    kinds = [
        "leader-kill",
        "leader-pause",
        "leader-partition",
        "directed-partition",
        "link-delay",
        "all-restart",
    ]
    try:
        for epoch in range(repeats * len(kinds)):
            kind = kinds[epoch % len(kinds)]
            leader = fleet.leader()
            chosen = (leader + 1) % 3
            attempts = []
            history = []
            barrier = threading.Barrier(4)
            lock = threading.Lock()

            def traffic(client):
                generator = random.Random(1901 + epoch * 100 + client)
                target = leader
                barrier.wait(timeout=10)
                for index in range(40):
                    key = f"fault-{epoch}-key-{index%20}".encode()
                    operation = generator.choice(["put", "put", "get", "del"])
                    value = f"{epoch}:{client}:{index}".encode()
                    start = time.monotonic_ns()
                    code = 6
                    result = None
                    try:
                        response = rpc(
                            fleet.endpoints[target], operation, key, value, timeout=1.5
                        )
                        code = response["code"]
                        if code == 0 and operation == "get":
                            result = response["value"].hex()
                    except OSError:
                        pass
                    end = time.monotonic_ns()
                    record = dict(
                        client=client,
                        kind=operation,
                        key=key.hex(),
                        value=value.hex(),
                        start=start,
                        end=end,
                        code=code,
                        result=result,
                    )
                    with lock:
                        attempts.append(record)
                        if (operation == "get" and code in [0, 1]) or (
                            operation != "get" and code in [0, 6]
                        ):
                            history.append(
                                dict(
                                    record,
                                    **(
                                        dict(optional=True, end=None)
                                        if code == 6
                                        else {}
                                    ),
                                )
                            )
                    if code in [5, 6]:
                        try:
                            target = fleet.leader(0.8)
                        except TimeoutError:
                            pass
                    time.sleep(0.035)

            fault = dict(kind=kind, node=leader + 1, started_ns=None)
            probe = None
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                jobs = [pool.submit(traffic, c) for c in range(3)]
                barrier.wait(timeout=10)
                time.sleep(0.08)
                fault["started_ns"] = time.monotonic_ns()
                try:
                    if kind in ["leader-kill", "leader-pause"]:
                        fleet.agent(
                            leader + 1,
                            "signal",
                            signal="KILL" if kind == "leader-kill" else "STOP",
                        )
                    elif kind == "all-restart":
                        fleet.all("stop")
                    elif kind in ["leader-partition", "directed-partition"]:
                        fleet.agent(
                            leader + 1,
                            "rules",
                            blocked=[n for n in [1, 2, 3] if n != leader + 1],
                        )
                        if kind == "leader-partition":
                            for n in [1, 2, 3]:
                                if n != leader + 1:
                                    fleet.agent(n, "rules", blocked=[leader + 1])
                    else:
                        fault["node"] = chosen + 1
                        fleet.agent(
                            chosen + 1,
                            "rules",
                            delay={str(n): 0.04 for n in [1, 2, 3] if n != chosen + 1},
                        )
                    fault["applied_ns"] = time.monotonic_ns()
                    if kind in ["leader-partition", "directed-partition"]:
                        began = time.monotonic_ns()
                        try:
                            probe = rpc(
                                fleet.endpoints[leader],
                                "get",
                                b"durable-anchor",
                                timeout=1.2,
                            )
                        except OSError:
                            probe = dict(code=6, value=b"")
                        probe = dict(
                            start=began, end=time.monotonic_ns(), code=probe["code"]
                        )
                        assert probe["code"] not in [0, 1], probe
                    time.sleep(
                        max(0, 1.5 - (time.monotonic_ns() - fault["applied_ns"]) / 1e9)
                    )
                    fault["heal_started_ns"] = time.monotonic_ns()
                finally:
                    fleet.heal()
                    if kind == "leader-kill":
                        fleet.agent(
                            leader + 1,
                            "start",
                            case=case,
                            encoder="background",
                            proxies=True,
                            snapshot_entries=64,
                            snapshot_bytes=65536,
                        )
                    elif kind == "all-restart":
                        fleet.all(
                            "start",
                            case=case,
                            encoder="background",
                            proxies=True,
                            snapshot_entries=64,
                            snapshot_bytes=65536,
                        )
                    fault["healed_ns"] = time.monotonic_ns()
                for job in jobs:
                    job.result(timeout=30)
            # Preserve inputs before checking. Timeout writes stay optional, never replayed.
            record = dict(
                epoch=epoch,
                fault=fault,
                attempts=attempts,
                operations=history,
                minority_read_probe=probe,
            )
            path = output / f"epoch-{epoch:03d}.json"
            save(path, record)
            fleet.settled()
            fleet.assert_value(b"durable-anchor", b"acknowledged")
            for key, value in ring.items():
                fleet.assert_value(key, value)
            key = f"ring-{epoch%16}".encode()
            value = f"ack-{epoch}".encode()
            assert fleet.call("put", key, value)["code"] == 0
            ring[key] = value
            for index in range(20):
                key = f"fault-{epoch}-key-{index}".encode()
                start = time.monotonic_ns()
                response = fleet.call("get", key)
                assert response["code"] in [0, 1], response
                history.append(
                    dict(
                        kind="get",
                        key=key.hex(),
                        start=start,
                        end=time.monotonic_ns(),
                        result=(
                            response["value"].hex() if response["code"] == 0 else None
                        ),
                    )
                )
            record["operations"] = history
            record["durable_ring"] = {k.hex(): v.hex() for k, v in ring.items()}
            save(path, record)
            model = [
                dict(op, end=math.inf) if op.get("optional") else op for op in history
            ]
            assert check(model), "history failed: " + str(epoch)
            overlap = sum(
                op["start"] < fault["healed_ns"] and op["end"] > fault["applied_ns"]
                for op in attempts
            )
            assert overlap > 0, "no measured traffic overlapped fault"
            summary = dict(
                epoch=epoch,
                kind=kind,
                attempts=len(attempts),
                checked_operations=len(history),
                unknown_writes=sum(op.get("optional", False) for op in history),
                overlapping_attempts=overlap,
                minority_read_code=probe["code"] if probe else None,
                checked=True,
            )
            summaries.append(summary)
            save(output / "summary.json", dict(status="running", epochs=summaries))
            print(json.dumps(summary), flush=True)
        # Force a lagging follower to install a snapshot while a healthy pair writes.
        leader = fleet.leader()
        lagging = (leader + 1) % 3
        fleet.agent(lagging + 1, "stop")
        before = fleet.states()
        cold = b"c" * 8192
        for index in range(128):
            assert fleet.call("put", f"catchup-{index}".encode(), cold)["code"] == 0
        until = time.monotonic() + 10
        while (
            time.monotonic() < until
            and fleet.states()[leader]["snapshot_index"]
            <= before[leader]["applied_index"]
        ):
            time.sleep(0.05)
        target = fleet.states()[leader]["snapshot_index"]
        assert target > before[leader]["applied_index"]
        fleet.agent(
            lagging + 1,
            "start",
            case=case,
            encoder="background",
            proxies=True,
            snapshot_entries=64,
            snapshot_bytes=65536,
        )
        states = fleet.settled()
        assert states[lagging]["snapshots_installed"] > 0
        for index in range(128):
            fleet.assert_value(f"catchup-{index}".encode(), cold)
        fleet.all("stop")
        fleet.all(
            "start",
            case=case,
            encoder="background",
            proxies=True,
            snapshot_entries=64,
            snapshot_bytes=65536,
        )
        states = fleet.settled()
        fleet.assert_value(b"durable-anchor", b"acknowledged")
        for key, value in ring.items():
            fleet.assert_value(key, value)
        final = dict(
            status="complete",
            epochs=summaries,
            states=states,
            catchup_snapshot_verified=True,
            full_restart_verified=True,
            ring_values=len(ring),
            attempts=sum(s["attempts"] for s in summaries),
            checked_operations=sum(s["checked_operations"] for s in summaries),
        )
        save(output / "summary.json", final)
        return final
    finally:
        fleet.heal()
        fleet.all("stop")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    assert 1 <= args.repeats <= 10
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    fleet = Fleet(json.loads(args.config.read_text()))
    try:
        fault_campaign(fleet, args.output, args.repeats)
    except BaseException as error:
        save(args.output / "failure.json", dict(error=repr(error)))
        raise


if __name__ == "__main__":
    main()
