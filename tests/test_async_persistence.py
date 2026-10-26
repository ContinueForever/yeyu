#!/usr/bin/env python3
"""Real RPC acceptance for the opt-in nonblocking Raft disk executor."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import signal
import socket
import tempfile
import time
import unittest

import test_cluster as harness
from test_cluster_snapshot import wait_for
import test_rpc_network as wire
import test_read_index
import test_cas_snapshot

FAULT_LIBRARY = ASAN_RUNTIME = None
OriginalCluster = harness.Cluster


class AsyncCluster(OriginalCluster):
    def __init__(self, *args, extra_args=None, **kwargs):
        super().__init__(
            *args,
            extra_args=["--disk-executor", "background", *(extra_args or [])],
            **kwargs,
        )


def fault_environment(directory, mode):
    preload = str(FAULT_LIBRARY)
    if ASAN_RUNTIME:
        preload = f"{ASAN_RUNTIME}:{preload}"
    return {
        i: {
            "LD_PRELOAD": preload,
            "YEWUKV_TEST_IO_FAULT": str(Path(directory) / f"arm-{i}"),
            "YEWUKV_TEST_IO_MODE": mode,
            "YEWUKV_TEST_IO_ENTERED": str(Path(directory) / f"entered-{i}"),
            "YEWUKV_TEST_IO_RELEASE": str(Path(directory) / f"release-{i}"),
        }
        for i in range(3)
    }


class AsyncRuntimeTests(unittest.TestCase):
    def test_status_and_heartbeats_continue_while_get_waits_for_earlier_durable_put(
        self,
    ):
        for store_mode in ("journal", "image"):
            with self.subTest(store=store_mode), tempfile.TemporaryDirectory(
                prefix="yewukv-async-gate-"
            ) as directory:
                root = Path(directory)
                cluster = AsyncCluster(
                    directory,
                    request_timeout=3000,
                    extra_args=["--raft-store", store_mode],
                    node_env=fault_environment(directory, "gate-sync"),
                )
                try:
                    leader = cluster.leader()
                    self.assertEqual(
                        cluster.call(leader, "put", b"fenced", b"old")["code"], 0
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
                    (root / f"arm-{leader}").touch()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                        write = pool.submit(
                            cluster.call, leader, "put", b"fenced", b"new", timeout=5
                        )
                        try:
                            wait_for(
                                cluster,
                                lambda: (root / f"entered-{leader}").exists(),
                                seconds=5,
                            )
                            status = cluster.status(
                                leader
                            )  # 0.5 s timeout; disk is gated indefinitely.
                            self.assertEqual(status["disk_executor"], "background")
                            self.assertEqual(status["persistence_pending"], 1)
                            self.assertEqual(status["last_index"], before["last_index"])
                            self.assertEqual(
                                status["admitted_last_index"], before["last_index"] + 1
                            )
                            self.assertFalse(write.done())
                            read = pool.submit(
                                cluster.call, leader, "get", b"fenced", timeout=5
                            )
                            wait_for(
                                cluster,
                                lambda: cluster.status(leader)["read_pending"] == 1,
                            )
                            # Stay gated beyond the follower election timeout.
                            # Old durable-prefix heartbeats must still flow.
                            until = time.monotonic() + 1.5
                            while time.monotonic() < until:
                                self.assertEqual(
                                    cluster.status(leader)["term"], before["term"]
                                )
                                self.assertEqual(
                                    cluster.status(leader)["role"], "leader"
                                )
                                self.assertFalse(write.done())
                                self.assertFalse(read.done())
                                time.sleep(0.05)
                        finally:
                            (root / f"release-{leader}").touch()
                        self.assertEqual(write.result(timeout=5)["code"], 0)
                        reply = read.result(timeout=5)
                        self.assertEqual((reply["code"], reply["value"]), (0, b"new"))
                    # Crash all nodes after the acknowledged write, then recover.
                    for i in range(3):
                        cluster.kill(i)
                    for i in range(3):
                        cluster.start(i)
                    cluster.eventually_value(b"fenced", b"new")
                finally:
                    for i in range(3):
                        (root / f"release-{i}").touch()
                    cluster.close()

    def test_sigkill_during_disk_work_never_acks_and_recovers_in_both_formats(self):
        for store_mode in ("journal", "image"):
            with self.subTest(store=store_mode), tempfile.TemporaryDirectory(
                prefix="yewukv-async-crash-"
            ) as directory:
                root = Path(directory)
                cluster = AsyncCluster(
                    directory,
                    extra_args=["--raft-store", store_mode],
                    node_env=fault_environment(directory, "gate-sync"),
                )
                connection = None
                try:
                    leader = cluster.leader()
                    self.assertEqual(
                        cluster.call(leader, "put", b"anchor", b"kept")["code"], 0
                    )
                    (root / f"arm-{leader}").touch()
                    connection = socket.create_connection(
                        ("127.0.0.1", cluster.ports[leader]), 3
                    )
                    connection.sendall(wire.request(5, "put", b"uncertain", b"unknown"))
                    wait_for(cluster, lambda: (root / f"entered-{leader}").exists())
                    self.assertEqual(cluster.status(leader)["persistence_pending"], 1)
                    connection.settimeout(0.1)
                    with self.assertRaises(socket.timeout):
                        connection.recv(1, socket.MSG_PEEK)
                    cluster.kill(leader)
                    (root / f"arm-{leader}").unlink()
                    cluster.start(leader)
                    current = cluster.eventually_value(b"anchor", b"kept")
                    self.assertEqual(
                        cluster.call(current, "put", b"after-crash", b"ok")["code"], 0
                    )
                    cluster.eventually_value(b"after-crash", b"ok")
                finally:
                    if connection:
                        connection.close()
                    for i in range(3):
                        (root / f"release-{i}").touch()
                    cluster.close()

    def test_shutdown_waits_for_accepted_disk_work_and_then_exits(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-async-stop-") as directory:
            root = Path(directory)
            cluster = AsyncCluster(
                directory, node_env=fault_environment(directory, "gate-sync")
            )
            connection = None
            try:
                leader = cluster.leader()
                self.assertEqual(
                    cluster.call(leader, "put", b"anchor", b"kept")["code"], 0
                )
                (root / f"arm-{leader}").touch()
                connection = socket.create_connection(
                    ("127.0.0.1", cluster.ports[leader]), 3
                )
                connection.sendall(
                    wire.request(5, "put", b"unacknowledged", b"unknown")
                )
                wait_for(cluster, lambda: (root / f"entered-{leader}").exists())
                process = cluster.processes[leader]
                process.send_signal(signal.SIGTERM)
                time.sleep(0.15)
                self.assertIsNone(
                    process.poll(), "shutdown discarded an accepted disk task"
                )
                (root / f"release-{leader}").touch()
                self.assertEqual(process.wait(timeout=5), 0)
                cluster.processes[leader] = None
                cluster.start(leader)
                cluster.eventually_value(b"anchor", b"kept")
            finally:
                if connection:
                    connection.close()
                for i in range(3):
                    (root / f"release-{i}").touch()
                cluster.close()

    def test_disk_errors_never_ack_and_the_remaining_quorum_preserves_acknowledged_data(
        self,
    ):
        for mode in ("enospc", "sync", "torn-write"):
            with self.subTest(fault=mode), tempfile.TemporaryDirectory(
                prefix="yewukv-async-failure-"
            ) as directory:
                root = Path(directory)
                cluster = AsyncCluster(
                    directory, node_env=fault_environment(directory, mode)
                )
                try:
                    leader = cluster.leader()
                    self.assertEqual(
                        cluster.call(leader, "put", b"anchor", b"kept")["code"], 0
                    )
                    (root / f"arm-{leader}").touch()
                    try:
                        reply = cluster.call(leader, "put", b"uncertain", b"unknown")
                    except (OSError, EOFError):
                        reply = {"code": 6}
                    self.assertNotEqual(reply["code"], 0)
                    process = cluster.processes[leader]
                    self.assertEqual(process.wait(timeout=5), 1)
                    cluster.processes[leader] = None
                    (root / f"arm-{leader}").unlink()
                    cluster.start(leader)
                    current = cluster.eventually_value(b"anchor", b"kept")
                    self.assertEqual(
                        cluster.call(current, "put", b"after", b"ok")["code"], 0
                    )
                    cluster.eventually_value(b"after", b"ok")
                finally:
                    cluster.close()


def main():
    global FAULT_LIBRARY, ASAN_RUNTIME
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--fault-library", type=Path, required=True)
    parser.add_argument("--asan-runtime", type=Path)
    args = parser.parse_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    FAULT_LIBRARY, ASAN_RUNTIME = args.fault_library.resolve(), args.asan_runtime
    # Reuse existing process failure/history, ReadIndex and CAS/snapshot oracles
    # unchanged, selecting the executor in the harness rather than weakening checks.
    harness.Cluster = AsyncCluster
    suite = unittest.TestSuite()
    loader = unittest.defaultTestLoader
    suite.addTests(loader.loadTestsFromTestCase(AsyncRuntimeTests))
    suite.addTests(loader.loadTestsFromTestCase(test_read_index.ReadIndexTests))
    suite.addTests(loader.loadTestsFromTestCase(harness.ClusterTests))
    suite.addTests(loader.loadTestsFromModule(test_cas_snapshot))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
