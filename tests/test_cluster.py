#!/usr/bin/env python3
"""Real TCP/RPC three-node failures, using controllable directed peer proxies."""

import argparse
import concurrent.futures
import json
import os
import random
from pathlib import Path
import select
import shutil
import signal
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import unittest

import test_rpc_network as wire
from linearizability import check as check_history

NODE = CLIENT = None


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Proxy:
    def __init__(self, source, target, port, blocked, stop, delays=None):
        self.source, self.target, self.port = source, target, port
        self.blocked, self.stop = blocked, stop
        self.delays = delays if delays is not None else {}
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.address = self.listener.getsockname()[1]
        self.handlers = []
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            handler = threading.Thread(target=self.relay, args=(connection,), daemon=True)
            if len(self.handlers) >= 64:
                self.handlers = [previous for previous in self.handlers if previous.is_alive()]
            self.handlers.append(handler)
            handler.start()

    def relay(self, connection):
        with connection:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.4) as remote:
                    connection.settimeout(0.4)
                    remote.settimeout(0.4)
                    while not self.stop.is_set():
                        if (self.source in self.blocked or self.target in self.blocked or
                                (self.source, self.target) in self.blocked):
                            return
                        readable, _, _ = select.select([connection, remote], [], [], 0.05)
                        for current in readable:
                            payload = current.recv(65536)
                            if not payload:
                                return
                            if self.stop.wait(self.delays.get((self.source, self.target), 0)):
                                return
                            (remote if current is connection else connection).sendall(payload)
            except OSError:
                return

    def close(self):
        self.listener.close()
        self.thread.join(timeout=2)
        for handler in self.handlers:
            handler.join(timeout=1)


class Cluster:
    def __init__(self, directory, request_timeout=900, extra_args=None, node_args=None,
                 node_env=None):
        self.directory = Path(directory)
        self.request_timeout = request_timeout
        self.extra_args = extra_args or []
        self.node_args = node_args or {}
        self.node_env = node_env or {}
        self.blocked = set()
        self.delays = {}
        self.stop = threading.Event()
        self.proxies = {}
        reservations = []
        try:
            # Keep node ports bound until all proxy listeners have bound. A
            # released ephemeral port could otherwise be selected by a proxy,
            # yielding duplicate member endpoints or an occupied node port.
            for _ in range(3):
                reservation = socket.socket()
                reservations.append(reservation)
                reservation.bind(("127.0.0.1", 0))
            self.ports = [reservation.getsockname()[1] for reservation in reservations]
            for i in range(3):
                for j in range(3):
                    if i != j:
                        self.proxies[i, j] = Proxy(i, j, self.ports[j], self.blocked, self.stop, self.delays)
        except BaseException:
            self.stop.set()
            for proxy in self.proxies.values():
                proxy.close()
            raise
        finally:
            for reservation in reservations:
                reservation.close()
        self.processes = [None] * 3
        for i in range(3):
            self.start(i)

    def start(self, i):
        members = ",".join(f"{j + 1}@127.0.0.1:"
                           f"{self.ports[j] if i == j else self.proxies[i, j].address}"
                           for j in range(3))
        log = open(self.directory / f"node-{i + 1}.log", "ab")
        try:
            self.processes[i] = subprocess.Popen(
                [str(NODE), "--id", str(i + 1), "--port", str(self.ports[i]),
                 "--members", members, "--data-dir", str(self.directory / f"node-{i + 1}"),
                 "--request-timeout-ms", str(self.request_timeout), *self.extra_args,
                 *self.node_args.get(i, [])], stdout=log, stderr=log,
                env={**os.environ, **self.node_env.get(i, {})})
        finally:
            log.close()  # The child owns its duplicate; restarts must not leak parent FDs.

    def kill(self, i):
        process = self.processes[i]
        self.processes[i] = None
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    def call(self, i, operation, key=b"", value=b"", timeout=2):
        with socket.create_connection(("127.0.0.1", self.ports[i]), timeout=timeout) as connection:
            connection.settimeout(timeout)
            if operation == "status":
                packet = wire.frame(b"\x08\x01" + wire.bytes_field(6, b""))
            else:
                packet = wire.request(1, operation, key, value)
            connection.sendall(packet)
            return wire.receive_response(connection)

    def status(self, i):
        response = self.call(i, "status", timeout=0.5)
        if response["code"] != 0:
            raise RuntimeError(response)
        return json.loads(response["value"])

    def leader(self, exclude=(), timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = []
            for i in range(3):
                process = self.processes[i]
                if i in exclude or not process:
                    continue
                if process.poll() is not None:
                    if self.processes[i] is not process:
                        continue  # Concurrent intentional termination.
                    raise AssertionError(self.diagnostics())
                try:
                    status = self.status(i)
                    if status["role"] == "leader":
                        found.append((status["term"], i))
                except (OSError, EOFError):
                    pass
            if found:
                return max(found)[1]
            time.sleep(0.05)
        raise AssertionError("no leader\n" + self.diagnostics())

    def eventually_value(self, key, value, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leader = self.leader(timeout=max(0.1, deadline - time.monotonic()))
            try:
                response = self.call(leader, "get", key)
                if response["code"] == 0 and response["value"] == value:
                    return leader
            except (OSError, EOFError):
                pass
            time.sleep(0.1)
        raise AssertionError("value did not recover\n" + self.diagnostics())

    def diagnostics(self):
        return "\n".join(f"node {i + 1}: " +
                         (self.directory / f"node-{i + 1}.log").read_text(errors="replace")[-4000:]
                         for i in range(3))

    def close(self):
        for process in self.processes:
            if process and process.poll() is None:
                process.send_signal(signal.SIGTERM)
        for process in self.processes:
            if process:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    raise AssertionError("cluster node failed graceful shutdown")
        self.stop.set()
        for proxy in self.proxies.values():
            proxy.close()
        evidence = NODE.parent.parent / "cluster-evidence" / self.directory.name
        evidence.mkdir(parents=True, exist_ok=True)
        for log in self.directory.glob("*.log"):
            shutil.copyfile(log, evidence / log.name)


class ClusterTests(unittest.TestCase):
    def test_concurrent_history_is_linearizable_during_leader_failure(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-cluster-history-") as directory:
            cluster = Cluster(directory)
            history = []
            history_lock = threading.Lock()
            barrier = threading.Barrier(4)
            try:
                leader = cluster.leader()

                def run(client_id):
                    randomizer = random.Random(771 + client_id)
                    barrier.wait(timeout=5)
                    for index in range(18):
                        key = f"history-{index % 6}".encode()
                        kind = randomizer.choice(["put", "put", "get", "del"])
                        value = f"{client_id}:{index}".encode()
                        start = time.monotonic_ns()
                        try:
                            selected = cluster.leader()
                            response = cluster.call(selected, kind, key, value)
                        except (OSError, EOFError):
                            response = {"code": 6}
                        end = time.monotonic_ns()
                        operation = dict(key=key.hex(), kind=kind, value=value.hex(),
                                         start=start, end=end, code=response["code"])
                        if kind == "get" and response["code"] in (0, 1):
                            operation["result"] = response["value"].hex() if response["code"] == 0 else None
                        elif kind != "get" and response["code"] == 6:
                            operation["optional"] = True
                            operation["end"] = float("inf")
                        elif response["code"] != 0:
                            continue  # Definite rejection or unknown read, no state effect.
                        with history_lock:
                            history.append(operation)
                        time.sleep(0.002)

                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                    work = [pool.submit(run, i) for i in range(3)]
                    barrier.wait(timeout=5)
                    time.sleep(0.01)
                    cluster.kill(leader)
                    for future in work:
                        future.result(timeout=20)
                self.assertGreaterEqual(len(history), 30)
                passed = check_history(history)
                trace = dict(seed=771, checked=passed, operations=[
                    {**op, "end": None if op.get("optional") else op["end"]} for op in history])
                (NODE.parent.parent / "cluster-history.json").write_text(json.dumps(trace, indent=2))
                self.assertTrue(passed, "history violated the per-key linearizable register model")
            finally:
                cluster.close()

    def test_cli_redirects_only_rejected_requests_and_never_replays_uncertain_writes(self):
        def fields(payload):
            result, offset = {}, 0
            while offset < len(payload):
                tag, offset = wire.read_varint(payload, offset)
                if tag & 7 == 0:
                    value, offset = wire.read_varint(payload, offset)
                elif tag & 7 == 2:
                    length, offset = wire.read_varint(payload, offset)
                    value = payload[offset:offset + length]
                    offset += length
                else:
                    raise AssertionError("unexpected client wire type")
                result[tag >> 3] = value
            return result

        def integer(field, value):
            return wire.varint(field << 3) + wire.varint(value)

        for redirect in (False, True):
            state = {"phase": 0, "writes": [0, 0, 0]}
            lock = threading.Lock()

            class Handler(socketserver.BaseRequestHandler):
                def handle(self):
                    try:
                        header = wire.receive_exact(self.request, 4)
                        count = int.from_bytes(header, "big")
                        request = fields(wire.receive_exact(self.request, count))
                        index = self.server.node_index
                        response = integer(1, request[1])
                        with lock:
                            if 6 in request:
                                leader = 1 if state["phase"] else 0
                                response += integer(6, int(index == leader)) + integer(8, 1 + state["phase"])
                            else:
                                state["writes"][index] += 1
                                if not redirect:
                                    return  # Connection loss after request: outcome unknown.
                                if index == 0:
                                    state["phase"] = 1
                                    response += integer(2, 5)  # Definitely rejected before append.
                        self.request.sendall(wire.frame(response))
                    except OSError:
                        pass

            class TCPServer(socketserver.ThreadingTCPServer):
                daemon_threads = True

            servers, threads = [], []
            try:
                for index in range(3):
                    server = TCPServer(("127.0.0.1", 0), Handler)
                    server.node_index = index
                    thread = threading.Thread(target=server.serve_forever,
                                              kwargs={"poll_interval": 0.05}, daemon=True)
                    thread.start()
                    servers.append(server)
                    threads.append(thread)
                addresses = ",".join(f"127.0.0.1:{s.server_address[1]}" for s in servers)
                result = subprocess.run([str(CLIENT), "--nodes", addresses, "--timeout-ms", "3000",
                                         "put", "key", "value"], capture_output=True, text=True, timeout=5)
                self.assertEqual(state["writes"], [1, 1, 0] if redirect else [1, 0, 0])
                self.assertEqual(result.returncode == 0, redirect, result.stderr)
            finally:
                for server in servers:
                    server.shutdown()
                    server.server_close()
                for thread in threads:
                    thread.join(timeout=1)

    def test_pending_request_is_not_acknowledged_for_a_replaced_log_index(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-cluster-pending-") as directory:
            cluster = Cluster(directory, request_timeout=4000)
            try:
                old = cluster.leader()
                cluster.blocked.add(old)
                time.sleep(0.2)
                previous_index = cluster.status(old)["last_index"]
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(cluster.call, old, "put", b"old", b"uncommitted")
                    deadline = time.monotonic() + 2
                    while cluster.status(old)["last_index"] == previous_index:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.01)
                    leader = cluster.leader(exclude={old})
                    self.assertEqual(cluster.call(leader, "put", b"new", b"committed")["code"], 0)
                    cluster.blocked.clear()
                    self.assertEqual(pending.result(timeout=5)["code"], 6)
                self.assertEqual(cluster.call(leader, "get", b"old")["code"], 1)
                self.assertEqual(cluster.call(leader, "get", b"new")["value"], b"committed")
            finally:
                cluster.close()

    def test_failover_partitions_recovery_ordering_and_client(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-cluster-") as directory:
            cluster = Cluster(directory)
            try:
                leader = cluster.leader()
                self.assertEqual(cluster.call(leader, "put", b"durable", b"one")["code"], 0)
                self.assertEqual(cluster.call(leader, "get", b"durable")["value"], b"one")
                self.assertEqual(cluster.call(leader, "put", b"oversized", b"x" * 8193)["code"], 2)
                # A malformed application command cannot poison a healthy node,
                # even when wrapped in a configured peer's higher-term envelope.
                integer = lambda field, value: wire.varint(field << 3) + wire.varint(value)
                sender = (leader + 1) % 3 + 1
                entry = integer(2, 1) + integer(3, 1) + wire.bytes_field(4, b"invalid-command")
                append = integer(1, 100000) + integer(2, sender) + wire.bytes_field(5, entry)
                peer = integer(1, sender) + integer(2, leader + 1) + integer(3, 77) + wire.bytes_field(6, append)
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 2) as connection:
                    connection.sendall(wire.frame(integer(1, 1) + wire.bytes_field(7, peer)))
                    self.assertEqual(wire.receive_response(connection)["code"], 2)
                self.assertLess(cluster.status(leader)["term"], 100000)
                follower = (leader + 1) % 3
                self.assertEqual(cluster.call(follower, "put", b"rejected", b"x")["code"], 5)

                # One connection pipelines changes and reads; ordered responses
                # must correspond to application order through the replicated log.
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 2) as connection:
                    connection.sendall(b"".join(wire.request(i + 1, "put", f"k{i}".encode(),
                                                            f"v{i}".encode()) for i in range(40)))
                    for i in range(40):
                        response = wire.receive_response(connection)
                        self.assertEqual((response["id"], response["code"]), (i + 1, 0))

                # An acknowledged write must survive an actual leader SIGKILL.
                cluster.kill(leader)
                new_leader = cluster.eventually_value(b"durable", b"one")
                self.assertNotEqual(new_leader, leader)
                self.assertEqual(cluster.call(new_leader, "put", b"durable", b"two")["code"], 0)
                cluster.start(leader)
                time.sleep(0.5)

                # Keep old leader's client port reachable while cutting EVERY
                # directed peer link. It must not acknowledge a write or stale read.
                old = cluster.leader()
                cluster.blocked.add(old)
                # Wait for proxies to close existing persistent connections;
                # packets already in flight at the cut may legitimately commit.
                time.sleep(0.2)
                old_commit = cluster.status(old)["commit_index"]
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    write = pool.submit(cluster.call, old, "put", b"minority", b"no")
                    read = pool.submit(cluster.call, old, "get", b"durable")
                    new_leader = cluster.leader(exclude={old})
                    self.assertEqual(cluster.call(new_leader, "put", b"durable", b"three")["code"], 0)
                    self.assertEqual(write.result(timeout=4)["code"], 6)
                    self.assertEqual(read.result(timeout=4)["code"], 6)
                self.assertEqual(cluster.status(old)["commit_index"], old_commit)
                cluster.blocked.clear()
                cluster.eventually_value(b"durable", b"three")
                deadline = time.monotonic() + 6
                while time.monotonic() < deadline:
                    if cluster.status(old)["role"] != "leader":
                        break
                    time.sleep(0.05)
                self.assertNotEqual(cluster.status(old)["role"], "leader")
                leader = cluster.leader()
                self.assertEqual(cluster.call(leader, "get", b"minority")["code"], 1)

                # Real CLI discovers the current leader; no blind write retries.
                addresses = ",".join(f"127.0.0.1:{port}" for port in cluster.ports)
                result = subprocess.run([str(CLIENT), "--nodes", addresses, "--timeout-ms", "5000",
                                         "get", "durable"], capture_output=True, text=True, timeout=7)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "three")

                # All processes disappear; no persisted KV map or commit index
                # exists. The new leader's no-op must reestablish the prefix.
                for i in range(3):
                    cluster.kill(i)
                for i in range(3):
                    cluster.start(i)
                leader = cluster.eventually_value(b"durable", b"three")
                for i in range(40):
                    self.assertEqual(cluster.call(leader, "get", f"k{i}".encode())["value"],
                                     f"v{i}".encode())
                self.assertEqual(cluster.call(leader, "del", b"durable")["code"], 0)
                self.assertEqual(cluster.call(leader, "get", b"durable")["code"], 1)
            finally:
                cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True, type=Path)
    parser.add_argument("--client", required=True, type=Path)
    args, remaining = parser.parse_known_args()
    NODE, CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining])
