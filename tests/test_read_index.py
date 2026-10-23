#!/usr/bin/env python3
"""Real RPC acceptance for log-free quorum reads and pipelined write fences."""
import argparse
import concurrent.futures
from pathlib import Path
import socket
import tempfile
import time
import unittest

import test_cluster as harness
import test_rpc_network as wire
from test_cluster_snapshot import wait_for


class ReadIndexTests(unittest.TestCase):
    def test_pure_reads_change_neither_log_nor_persistent_bytes(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-read-index-") as directory:
            cluster = harness.Cluster(directory)
            try:
                leader = cluster.leader()
                self.assertEqual(cluster.call(leader, "put", b"config", b"v1")["code"], 0)
                wait_for(cluster, lambda: all(cluster.status(i)["applied_index"] ==
                         cluster.status(leader)["last_index"] for i in range(3)))
                before = [cluster.status(i) for i in range(3)]
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 3) as connection:
                    connection.settimeout(5)
                    for i in range(100):
                        connection.sendall(wire.request(i + 1, "get", b"config"))
                        response = wire.receive_response(connection)
                        self.assertEqual((response["id"], response["code"], response["value"]),
                                         (i + 1, 0, b"v1"))
                after = [cluster.status(i) for i in range(3)]
                for old, new in zip(before, after):
                    self.assertEqual(new["last_index"], old["last_index"])
                    self.assertEqual(new["disk_bytes"], old["disk_bytes"])
                    self.assertEqual(new["disk_saves"], old["disk_saves"])
                self.assertEqual(after[leader]["read_index_completed"] - before[leader]["read_index_completed"], 100)
            finally:
                cluster.close()

    def test_pipelined_get_waits_for_preceding_put_to_apply(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-read-fence-") as directory:
            cluster = harness.Cluster(directory)
            try:
                leader = cluster.leader()
                before = cluster.status(leader)
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 3) as connection:
                    connection.settimeout(10)
                    packets = []
                    for i in range(50):
                        key, value = f"key-{i}".encode(), f"value-{i}".encode()
                        packets.extend((wire.request(i * 2 + 1, "put", key, value),
                                        wire.request(i * 2 + 2, "get", key)))
                    connection.sendall(b"".join(packets))
                    for i in range(50):
                        put, get = wire.receive_response(connection), wire.receive_response(connection)
                        self.assertEqual((put["id"], put["code"]), (i * 2 + 1, 0))
                        self.assertEqual((get["id"], get["code"], get["value"]),
                                         (i * 2 + 2, 0, f"value-{i}".encode()))
                after = cluster.status(leader)
                self.assertGreater(after["read_overlapped_requests"], before["read_overlapped_requests"])
                self.assertGreater(after["read_append_with_entries"], before["read_append_with_entries"])
            finally:
                cluster.close()

    def test_read_queue_is_bounded_and_old_leader_never_returns_local_value(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-read-bound-") as directory:
            cluster = harness.Cluster(directory, extra_args=["--pending-limit", "2"])
            try:
                old = cluster.leader()
                self.assertEqual(cluster.call(old, "put", b"k", b"old")["code"], 0)
                cluster.blocked.add(old)
                time.sleep(0.15)  # Close peer channels before starting these reads.
                with socket.create_connection(("127.0.0.1", cluster.ports[old]), 3) as connection:
                    connection.settimeout(5)
                    connection.sendall(b"".join(wire.request(i + 1, "get", b"k") for i in range(8)))
                    replies = [wire.receive_response(connection) for _ in range(8)]
                self.assertTrue(all(reply["code"] in (4, 6) for reply in replies))
                self.assertIn(4, [reply["code"] for reply in replies])
                self.assertLessEqual(cluster.status(old)["read_pending"], 2)
                leader = cluster.leader()
                self.assertNotEqual(leader, old)
                self.assertEqual(cluster.call(leader, "put", b"k", b"new")["code"], 0)
                cluster.blocked.clear()
                cluster.eventually_value(b"k", b"new")
            finally:
                cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
