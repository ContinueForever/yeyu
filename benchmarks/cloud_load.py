#!/usr/bin/env python3
"""Bounded multiprocess load, paced arrivals and snapshot encoding A/B.

Two load processes share a follower machine. Missed paced slots are counted,
not hidden or accumulated into an unbounded client queue. Writes are not replayed.
"""
import argparse, concurrent.futures, gzip, hashlib, json, math, multiprocessing as mp, os, random, socket, struct, threading, time
from pathlib import Path
from cloud_campaign import Fleet, rpc, save
import rpc_baseline as wire


class Connection:
    def __init__(self, endpoint):
        self.socket = socket.create_connection(endpoint, timeout=2)
        self.socket.settimeout(2)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.ident = 0

    def close(self):
        self.socket.close()

    def call(self, kind, key, value=b""):
        self.ident += 1
        body = wire.bytes_field(1, key)
        if kind == "put":
            body += wire.bytes_field(2, value)
        packet = (
            b"\x08"
            + wire.varint(self.ident)
            + wire.bytes_field(2 if kind == "put" else 3, body)
        )
        self.socket.sendall(struct.pack("!I", len(packet)) + packet)
        length = struct.unpack("!I", wire.read_exact(self.socket, 4))[0]
        if not 0 < length <= 65536:
            raise ValueError("invalid response frame")
        response = wire.decode_response(wire.read_exact(self.socket, length))
        if response.get(1) != self.ident:
            raise ValueError("wrong response ID")
        return response.get(2, 0), response.get(3, b"")


def value_for(client, key, version, size=128):
    return f"{client}:{key}:{version}:".encode().ljust(size, b"x")


def load_process(ids, endpoint, config, clock, start, abort, ready, done, folder):
    def client(client):
        connection = None
        samples = []
        candidates = [set() for _ in range(8)]
        versions = [0] * 8
        unknown = 0
        result = dict(client=client, status="failed")
        keys = [f"load-{client}-{i}".encode() for i in range(8)]
        randomizer = random.Random(config["repeat"] * 10000 + client)
        try:
            connection = Connection(endpoint)
            for i, key in enumerate(keys):
                value = value_for(client, i, 0)
                code, _ = connection.call("put", key, value)
                assert code == 0, "preload rejection"
                candidates[i] = {value}
            until = time.monotonic() + config["warmup_seconds"]
            index = 0
            while time.monotonic() < until:
                key = index % 8
                put = randomizer.randrange(100) < config["write_percent"]
                if put:
                    versions[key] += 1
                    value = value_for(client, key, versions[key])
                    code, _ = connection.call("put", keys[key], value)
                    assert code == 0, "warmup rejection"
                    candidates[key] = {value}
                else:
                    code, value = connection.call("get", keys[key])
                    assert (
                        code == 0 and value in candidates[key]
                    ), "warmup read mismatch"
                index += 1
            ready.put(dict(client=client, ready=True))
            assert start.wait(60) and not abort.is_set()
            began = clock.value
            deadline = began + int(config["duration_seconds"] * 1e9)
            rate = config.get("offered_rps", 0)
            period = max(1, round(1e9 * config["clients"] / rate)) if rate else 0
            slot = began + round(client * 1e9 / rate) if rate else began
            offered = 0 if not rate else max(0, (deadline - 1 - slot) // period + 1)
            while not abort.is_set():
                now = time.monotonic_ns()
                if now >= deadline or (rate and slot >= deadline):
                    break
                if rate:
                    if now < slot:
                        time.sleep((slot - now) / 1e9)
                    now = time.monotonic_ns()
                    if now >= deadline:
                        break
                    if now - slot >= period:
                        slot += ((now - slot) // period) * period
                    if slot >= deadline:
                        break
                key = index % 8
                put = randomizer.randrange(100) < config["write_percent"]
                value = b""
                if put:
                    versions[key] += 1
                    value = value_for(client, key, versions[key])
                actual = time.monotonic_ns()
                scheduled = slot if rate else actual
                code = 6
                try:
                    if connection is None:
                        connection = Connection(endpoint)
                    code, received = connection.call(
                        "put" if put else "get", keys[key], value
                    )
                    if code == 0:
                        if put:
                            candidates[key] = {value}
                        elif received not in candidates[key]:
                            code = 98
                    elif put and code == 6:
                        candidates[key].add(value)
                        unknown += 1
                except OSError:
                    if connection is not None:
                        connection.close()
                        connection = None
                    if put:
                        candidates[key].add(value)
                        unknown += 1
                completed = time.monotonic_ns()
                samples.append(
                    [
                        completed - actual,
                        completed - began,
                        int(put),
                        code,
                        scheduled - began,
                        actual - began,
                    ]
                )
                index += 1
                if rate:
                    slot += period
            finished = time.monotonic_ns()
            checked = 0
            if connection is not None:
                connection.close()
                connection = None
            connection = Connection(endpoint)
            for i, key in enumerate(keys):
                code, value = connection.call("get", key)
                assert code == 0 and value in candidates[i], (
                    "final value mismatch",
                    client,
                    i,
                    code,
                )
                checked += 1
            result.update(
                status="complete",
                finished_ns=finished,
                offered_slots=offered if rate else len(samples),
                missed_slots=offered - len(samples) if rate else 0,
                unknown_writes=unknown,
                final_checked_keys=checked,
                samples_file=f"client-{client}.json.gz",
            )
            assert result["missed_slots"] >= 0
            with gzip.open(Path(folder) / result["samples_file"], "wt") as stream:
                json.dump(
                    dict(
                        columns=[
                            "latency_ns",
                            "completed_offset_ns",
                            "put",
                            "code",
                            "scheduled_offset_ns",
                            "started_offset_ns",
                        ],
                        samples=samples,
                    ),
                    stream,
                    separators=(",", ":"),
                )
            save(Path(folder) / f"client-{client}.json", result)
            done.put(result)
        except BaseException as error:
            abort.set()
            result["error"] = repr(error)
            save(Path(folder) / f"client-{client}.json", result)
            ready.put(result)
            done.put(result)
        finally:
            if connection is not None:
                connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ids)) as pool:
        list(pool.map(client, ids))


def quantiles(values):
    if not values:
        return None
    values.sort()
    n = len(values)
    return {
        **{
            name: round(values[math.ceil(n * f) - 1] / 1e6, 3)
            for name, f in [
                ("p50_ms", 0.5),
                ("p95_ms", 0.95),
                ("p99_ms", 0.99),
                ("p999_ms", 0.999),
            ]
        },
        "max_ms": round(values[-1] / 1e6, 3),
        "samples": n,
    }


def summarize(folder, result):
    codes = {}
    service = []
    end_to_end = []
    failed = []
    puts = gets = 0
    attempts = 0
    for client in result["clients"]:
        assert client["status"] == "complete" and client["final_checked_keys"] == 8
        with gzip.open(folder / client["samples_file"], "rt") as stream:
            raw = json.load(stream)
        assert raw["columns"] == [
            "latency_ns",
            "completed_offset_ns",
            "put",
            "code",
            "scheduled_offset_ns",
            "started_offset_ns",
        ]
        previous = 0
        for latency, offset, put, code, scheduled, started in raw["samples"]:
            assert all(
                type(n) == int for n in [latency, offset, put, code, scheduled, started]
            )
            assert (
                0 < latency <= offset <= client["finished_ns"] - result["start_ns"]
                and started + latency == offset
                and 0 <= scheduled <= started
                and started >= previous
                and put in [0, 1]
                and code in list(range(8)) + [98]
            )
            previous = offset
            attempts += 1
            codes[str(code)] = codes.get(str(code), 0) + 1
            if code == 0:
                service.append(latency)
                end_to_end.append(offset - scheduled)
                puts += put
                gets += 1 - put
            else:
                failed.append(latency)
        assert (
            len(raw["samples"]) + client["missed_slots"] == client["offered_slots"]
            and client["missed_slots"] >= 0
        )
    elapsed = (
        max(c["finished_ns"] for c in result["clients"]) - result["start_ns"]
    ) / 1e9
    return dict(
        attempts=attempts,
        successes=puts + gets,
        failures=len(failed),
        completed_puts=puts,
        completed_gets=gets,
        codes=codes,
        elapsed_seconds=elapsed,
        successful_ops_s=(puts + gets) / elapsed,
        attempts_s=attempts / elapsed,
        offered_slots=sum(c["offered_slots"] for c in result["clients"]),
        missed_slots=sum(c["missed_slots"] for c in result["clients"]),
        latency_success_ms=quantiles(service),
        scheduled_latency_success_ms=quantiles(end_to_end),
        latency_failed_ms=quantiles(failed),
    )


def measure(fleet, folder, config):
    folder.mkdir(parents=True, exist_ok=False)
    save(folder / "workload.json", config)
    states = fleet.settled()
    leader = fleet.leader()
    identity = [states[leader]["term"], leader + 1]
    endpoint = fleet.endpoints[leader]
    save(folder / "initial-states.json", states)
    ctx = mp.get_context("fork")
    clock = ctx.Value("Q", 0)
    start = ctx.Event()
    abort = ctx.Event()
    ready = ctx.Queue()
    done = ctx.Queue()
    groups = [list(range(i, config["clients"], 2)) for i in range(2)]
    groups = [g for g in groups if g]
    workers = [
        ctx.Process(
            target=load_process,
            args=(ids, endpoint, config, clock, start, abort, ready, done, str(folder)),
        )
        for ids in groups
    ]
    resources = []
    stop = threading.Event()

    def observe():
        while not stop.is_set():
            frame = dict(
                monotonic_ns=time.monotonic_ns(),
                host_cpu=Path("/proc/stat").read_text().splitlines()[0],
                load=os.getloadavg(),
                clients=[],
                servers=[],
            )
            for process in workers:
                try:
                    frame["clients"].append(
                        dict(
                            pid=process.pid,
                            stat=Path(f"/proc/{process.pid}/stat").read_text(),
                            status=Path(f"/proc/{process.pid}/status").read_text(),
                        )
                    )
                except OSError:
                    pass
            try:
                frame["servers"] = fleet.all("inspect")
            except OSError as error:
                frame["observer_error"] = repr(error)
            resources.append(frame)
            stop.wait(0.25)

    result = dict(status="failed", config=config, identity=identity, clients=[])
    try:
        for process in workers:
            process.start()
        messages = [ready.get(timeout=90) for _ in range(config["clients"])]
        assert all(m.get("ready") for m in messages), messages
        save(folder / "before.json", fleet.states())
        clock.value = time.monotonic_ns()
        result["start_ns"] = clock.value
        observer = threading.Thread(target=observe)
        observer.start()
        start.set()
        result["clients"] = [
            done.get(timeout=config["duration_seconds"] + 30)
            for _ in range(config["clients"])
        ]
        assert all(c["status"] == "complete" for c in result["clients"]), result[
            "clients"
        ]
        for process in workers:
            process.join(timeout=10)
            assert process.exitcode == 0
        stop.set()
        observer.join(timeout=10)
        assert not observer.is_alive()
        save(folder / "after.json", fleet.settled())
        assert [
            fleet.states()[leader]["term"],
            fleet.leader() + 1,
        ] == identity, "leader changed"
        result.update(summarize(folder, result))
        assert "98" not in result["codes"], "read validation failed"
        result["status"] = "complete"
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        abort.set()
        start.set()
        stop.set()
        for process in workers:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        save(folder / "result.json", result)
        save(folder / "resources.json", resources)
    return result


def campaign(fleet, output, group, repeats):
    plan = []
    if group == "snapshot":
        for repeat in range(1, repeats + 1):
            for encoder in (
                ["sync", "background"] if repeat % 2 else ["background", "sync"]
            ):
                plan.append(
                    dict(
                        name=f"snapshot-{encoder}-r{repeat}",
                        clients=32,
                        write_percent=50,
                        repeat=repeat,
                        encoder=encoder,
                        cold_keys=1024,
                        duration_seconds=10,
                        warmup_seconds=2,
                        offered_rps=0,
                    )
                )
    else:
        for repeat in range(1, repeats + 1):
            for write_percent in [0, 50, 90]:
                for clients in [8, 32, 128]:
                    plan.append(
                        dict(
                            name=f"capacity-w{write_percent}-c{clients}-r{repeat}",
                            clients=clients,
                            write_percent=write_percent,
                            repeat=repeat,
                            encoder="sync",
                            cold_keys=0,
                            duration_seconds=6,
                            warmup_seconds=2,
                            offered_rps=0,
                        )
                    )
            for rate in [500, 2500, 10000]:
                plan.append(
                    dict(
                        name=f"paced-{rate}-r{repeat}",
                        clients=32,
                        write_percent=50,
                        repeat=repeat,
                        encoder="sync",
                        cold_keys=0,
                        duration_seconds=6,
                        warmup_seconds=2,
                        offered_rps=rate,
                    )
                )
    progress = dict(status="running", group=group, plan=plan, completed=[])
    save(output / "manifest.json", progress)
    try:
        for config in plan:
            case = output.name + "-" + config["name"]
            config["leader_id"] = 2
            fleet.all(
                "start",
                case=case,
                encoder=config["encoder"],
                proxies=False,
                snapshot_entries=128,
                snapshot_bytes=1048576,
            )
            fleet.settled()
            fleet.select_leader(2)
            if config["cold_keys"]:
                leader = fleet.leader()
                connection = Connection(fleet.endpoints[leader])
                try:
                    for i in range(config["cold_keys"]):
                        assert (
                            connection.call("put", f"cold-{i}".encode(), b"c" * 8192)[0]
                            == 0
                        )
                finally:
                    connection.close()
            r = measure(fleet, output / config["name"], config)
            progress["completed"].append(
                dict(
                    name=config["name"],
                    attempts=r["attempts"],
                    successes=r["successes"],
                    failures=r["failures"],
                    ops_s=r["successful_ops_s"],
                    latency=r["latency_success_ms"],
                    missed_slots=r["missed_slots"],
                )
            )
            save(output / "manifest.json", progress)
            print(json.dumps(progress["completed"][-1]), flush=True)
            if config["cold_keys"]:
                fleet.all("stop")
                fleet.all(
                    "start",
                    case=case,
                    encoder=config["encoder"],
                    proxies=False,
                    snapshot_entries=128,
                    snapshot_bytes=1048576,
                )
                fleet.settled()
                for i in range(config["cold_keys"]):
                    fleet.assert_value(f"cold-{i}".encode(), b"c" * 8192)
                save(
                    output / config["name"] / "restart-verification.json",
                    dict(
                        verified=True,
                        cold_keys=config["cold_keys"],
                        states=fleet.states(),
                    ),
                )
            fleet.all("stop")
        progress["status"] = "complete"
        save(output / "manifest.json", progress)
    finally:
        fleet.all("stop")
    return progress


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--group", choices=["snapshot", "capacity"], required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    assert 1 <= args.repeats <= 10
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    try:
        campaign(
            Fleet(json.loads(args.config.read_text())),
            args.output,
            args.group,
            args.repeats,
        )
    except BaseException as error:
        save(args.output / "failure.json", dict(error=repr(error)))
        raise


if __name__ == "__main__":
    main()
