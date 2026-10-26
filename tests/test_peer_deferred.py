#!/usr/bin/env python3
"""Bounded peer admission behind real disk barriers, using actual RPC sockets."""
import argparse
import concurrent.futures
from pathlib import Path
import socket
import struct
import tempfile
import time
import unittest

import test_async_persistence as asynchronous
import test_cluster as harness
from test_cluster_snapshot import wait_for
import test_rpc_network as wire


def vote(connection, request_id, sender, receiver, term, last_index, last_term):
    def integer(field, value):
        return wire.varint(field << 3) + wire.varint(value)

    body = b"".join(
        integer(field, value)
        for field, value in enumerate((term, sender, last_index, last_term), 1)
    )
    peer = (
        integer(1, sender)
        + integer(2, receiver)
        + integer(3, request_id)
        + wire.bytes_field(4, body)
    )
    connection.sendall(wire.frame(integer(1, request_id) + wire.bytes_field(7, peer)))


class PeerDeferredTests(unittest.TestCase):
    def test_checkpoint_wait_resumes_without_busy_reply_and_never_acks_early(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-peer-checkpoint-") as directory:
            root = Path(directory)
            environment = asynchronous.fault_environment(directory, "gate-sync")
            for row in environment.values():
                row["YEWUKV_TEST_IO_CHECKPOINT_ONLY"] = "1"
            cluster = asynchronous.AsyncCluster(
                directory,
                request_timeout=3000,
                extra_args=[
                    "--snapshot-encoder",
                    "background",
                    "--snapshot-policy",
                    "fixed",
                    "--snapshot-entries",
                    "4",
                    "--snapshot-bytes",
                    "67108864",
                ],
                node_env=environment,
            )
            try:
                leader = cluster.leader()
                follower, other = (leader + 1) % 3, (leader + 2) % 3
                self.assertEqual(
                    cluster.call(leader, "put", b"anchor", b"kept")["code"], 0
                )
                wait_for(
                    cluster,
                    lambda: all(
                        cluster.status(i)["applied_index"]
                        == cluster.status(leader)["last_index"]
                        for i in range(3)
                    ),
                )
                before = cluster.status(leader)
                (root / f"arm-{follower}").touch()
                for i in range(2):
                    self.assertEqual(
                        cluster.call(leader, "put", f"trigger-{i}".encode(), b"v")[
                            "code"
                        ],
                        0,
                    )
                wait_for(cluster, lambda: (root / f"entered-{follower}").exists())
                cluster.blocked.add(other)
                time.sleep(0.06)
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    write = pool.submit(
                        cluster.call, leader, "put", b"probe", b"after", timeout=5
                    )
                    try:
                        wait_for(
                            cluster,
                            lambda: cluster.status(follower)["peer_deferred_pending"]
                            == 1,
                        )
                        self.assertEqual(
                            cluster.status(follower)["persistence_pending"], 1
                        )
                        self.assertFalse(write.done())
                        self.assertEqual(
                            cluster.status(leader)["peer_busy_received"],
                            before["peer_busy_received"],
                        )
                    finally:
                        (root / f"release-{follower}").touch()
                    self.assertEqual(write.result(timeout=5)["code"], 0)
                cluster.blocked.clear()
                wait_for(
                    cluster,
                    lambda: cluster.status(follower)["peer_deferred_pending"] == 0,
                )
                self.assertGreater(
                    cluster.status(follower)["peer_deferred_admitted"], 0
                )
                self.assertEqual(
                    cluster.status(leader)["peer_busy_received"],
                    before["peer_busy_received"],
                )
                for i in range(3):
                    cluster.kill(i)
                    (root / f"arm-{i}").unlink(missing_ok=True)
                for i in range(3):
                    cluster.start(i)
                cluster.eventually_value(b"probe", b"after")
            finally:
                cluster.blocked.clear()
                for i in range(3):
                    (root / f"release-{i}").touch()
                cluster.close()

    def test_full_slots_still_fence_and_persist_the_highest_received_term(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-peer-term-") as directory:
            root = Path(directory)
            cluster = asynchronous.AsyncCluster(
                directory,
                request_timeout=3000,
                node_env=asynchronous.fault_environment(directory, "gate-sync"),
            )
            sockets = []
            try:
                leader = cluster.leader()
                self.assertEqual(
                    cluster.call(leader, "put", b"anchor", b"kept")["code"], 0
                )
                before = cluster.status(leader)
                (root / f"arm-{leader}").touch()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    write = pool.submit(
                        cluster.call, leader, "put", b"uncertain", b"value", timeout=5
                    )
                    try:
                        wait_for(cluster, lambda: (root / f"entered-{leader}").exists())
                        voters = [i + 1 for i in range(3) if i != leader]
                        for ordinal, sender in enumerate(voters):
                            connection = socket.create_connection(
                                ("127.0.0.1", cluster.ports[leader]), 3
                            )
                            sockets.append(connection)
                            vote(
                                connection,
                                701 + ordinal,
                                sender,
                                leader + 1,
                                before["term"] + 2 + ordinal,
                                before["last_index"],
                                before["term"],
                            )
                            if ordinal == 0:
                                wait_for(
                                    cluster,
                                    lambda: cluster.status(leader)[
                                        "peer_deferred_pending"
                                    ]
                                    == 1,
                                )
                                with socket.create_connection(
                                    ("127.0.0.1", cluster.ports[leader]), 3
                                ) as duplicate:
                                    vote(
                                        duplicate,
                                        700,
                                        sender,
                                        leader + 1,
                                        before["term"] + 1,
                                        before["last_index"],
                                        before["term"],
                                    )
                                    self.assertEqual(
                                        wire.receive_response(duplicate)["code"], 4
                                    )
                                self.assertEqual(
                                    cluster.status(leader)["peer_deferred_pending"], 1
                                )
                        wait_for(
                            cluster,
                            lambda: cluster.status(leader)["peer_deferred_pending"]
                            == 2,
                        )
                        overflow = socket.create_connection(
                            ("127.0.0.1", cluster.ports[leader]), 3
                        )
                        sockets.append(overflow)
                        highest = before["term"] + 7
                        vote(
                            overflow,
                            703,
                            voters[0],
                            leader + 1,
                            highest,
                            before["last_index"],
                            before["term"],
                        )
                        self.assertEqual(wire.receive_response(overflow)["code"], 4)
                        state = cluster.status(leader)
                        self.assertEqual(state["peer_deferred_capacity"], 2)
                        self.assertEqual(state["peer_deferred_pending"], 2)
                        self.assertGreater(state["peer_deferred_rejected"], 0)
                        self.assertEqual(
                            (
                                state["role"],
                                state["term"],
                                state["persistence_pending"],
                            ),
                            ("follower", before["term"], 1),
                        )
                        for connection in sockets[:2]:
                            connection.settimeout(0.02)
                            with self.assertRaises(socket.timeout):
                                connection.recv(1, socket.MSG_PEEK)
                    finally:
                        (root / f"release-{leader}").touch()
                    self.assertNotEqual(write.result(timeout=5)["code"], 0)
                wait_for(
                    cluster,
                    lambda: cluster.status(leader)["term"] >= highest
                    and cluster.status(leader)["persistence_pending"] == 0,
                )
                for connection in sockets[:2]:
                    self.assertEqual(wire.receive_response(connection)["code"], 0)
                cluster.kill(leader)
                cluster.start(leader)
                wait_for(cluster, lambda: cluster.status(leader)["term"] >= highest)
                cluster.eventually_value(b"anchor", b"kept")
            finally:
                for connection in sockets:
                    connection.close()
                for i in range(3):
                    (root / f"release-{i}").touch()
                cluster.close()

    def test_expired_and_disconnected_waiters_leave_the_bounded_slots(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-peer-expiry-") as directory:
            root = Path(directory)
            cluster = asynchronous.AsyncCluster(
                directory,
                request_timeout=3000,
                node_env=asynchronous.fault_environment(directory, "gate-sync"),
            )
            sockets = []
            try:
                leader = cluster.leader()
                self.assertEqual(
                    cluster.call(leader, "put", b"anchor", b"kept")["code"], 0
                )
                before = cluster.status(leader)
                sender = (leader + 1) % 3 + 1
                (root / f"arm-{leader}").touch()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    write = pool.submit(
                        cluster.call, leader, "put", b"probe", b"after", timeout=5
                    )
                    try:
                        wait_for(cluster, lambda: (root / f"entered-{leader}").exists())
                        connection = socket.create_connection(
                            ("127.0.0.1", cluster.ports[leader]), 3
                        )
                        sockets.append(connection)
                        vote(
                            connection,
                            801,
                            sender,
                            leader + 1,
                            before["term"],
                            before["last_index"],
                            before["term"],
                        )
                        wait_for(
                            cluster,
                            lambda: cluster.status(leader)["peer_deferred_pending"]
                            == 1,
                        )
                        self.assertEqual(wire.receive_response(connection)["code"], 6)
                        state = cluster.status(leader)
                        self.assertEqual(state["peer_deferred_pending"], 0)
                        self.assertEqual(state["peer_deferred_expired"], 1)
                        self.assertEqual(state["peer_deferred_admitted"], 0)
                        disconnected = socket.create_connection(
                            ("127.0.0.1", cluster.ports[leader]), 3
                        )
                        sockets.append(disconnected)
                        vote(
                            disconnected,
                            802,
                            sender,
                            leader + 1,
                            before["term"],
                            before["last_index"],
                            before["term"],
                        )
                        wait_for(
                            cluster,
                            lambda: cluster.status(leader)["peer_deferred_pending"]
                            == 1,
                        )
                        disconnected.setsockopt(
                            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                        )
                        disconnected.close()
                        wait_for(
                            cluster,
                            lambda: cluster.status(leader)["peer_deferred_abandoned"]
                            == 1,
                        )
                        self.assertEqual(
                            cluster.status(leader)["peer_deferred_pending"], 0
                        )
                        self.assertFalse(write.done())
                    finally:
                        (root / f"release-{leader}").touch()
                    self.assertEqual(write.result(timeout=5)["code"], 0)
                self.assertEqual(
                    cluster.call(leader, "get", b"probe")["value"], b"after"
                )
            finally:
                for connection in sockets:
                    connection.close()
                for i in range(3):
                    (root / f"release-{i}").touch()
                cluster.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--fault-library", type=Path, required=True)
    parser.add_argument("--asan-runtime")
    args = parser.parse_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    asynchronous.FAULT_LIBRARY = args.fault_library.resolve()
    asynchronous.ASAN_RUNTIME = args.asan_runtime
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(PeerDeferredTests)
    )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
