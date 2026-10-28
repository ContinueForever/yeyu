#!/usr/bin/env python3
"""Trace correlation, runtime control and minority-read safety on real processes."""
import argparse
import json
from pathlib import Path
import socket
import tempfile
import time
import unittest

import test_cluster as harness
import test_rpc_network as wire
from test_cluster_snapshot import wait_for


class ReadTraceTests(unittest.TestCase):
    def test_control_correlation_and_quorum_failure_preserve_read_contract(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-trace-runtime-") as temporary:
            root = Path(temporary)
            prefixes = [root / f"trace-{node}" for node in range(3)]
            cluster = harness.Cluster(temporary, node_args={
                node: ["--trace-prefix", str(prefix), "--trace-sample-every", "1",
                       "--trace-slow-ms", "1"] for node, prefix in enumerate(prefixes)})
            try:
                leader = cluster.leader()
                self.assertEqual(cluster.call(leader, "put", b"private-key", b"private-value")["code"], 0)
                wait_for(cluster, lambda: all(cluster.status(i)["applied_index"] ==
                         cluster.status(leader)["last_index"] for i in range(3)))
                before = [cluster.status(i) for i in range(3)]
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 3) as connection:
                    connection.settimeout(5)
                    client_port = connection.getsockname()[1]
                    def reads(start):
                        for request in range(start, start + 10):
                            connection.sendall(wire.request(request, "get", b"private-key"))
                            reply = wire.receive_response(connection)
                            self.assertEqual((reply["id"], reply["code"], reply["value"]),
                                             (request, 0, b"private-value"))
                    reads(100)
                    # Different connections may reuse the same request ID.
                    with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 3) as other:
                        other.settimeout(5)
                        other_port = other.getsockname()[1]
                        self.assertNotEqual(client_port, other_port)
                        other.sendall(wire.request(100, "get", b"private-key"))
                        self.assertEqual(wire.receive_response(other)["code"], 0)
                    Path(str(prefixes[leader]) + ".enabled").write_text("0\n")
                    wait_for(cluster, lambda: cluster.status(leader)["trace_enabled"] == 0)
                    reads(200)
                    Path(str(prefixes[leader]) + ".enabled").write_text("1\n")
                    wait_for(cluster, lambda: cluster.status(leader)["trace_enabled"] == 1)
                    reads(300)
                    after = [cluster.status(i) for i in range(3)]
                    for old, new in zip(before, after):
                        self.assertEqual(old["last_index"], new["last_index"])
                        self.assertEqual(old["disk_bytes"], new["disk_bytes"])
                    cluster.blocked.add(leader)
                    time.sleep(.2)
                    connection.sendall(wire.request(4000, "get", b"private-key"))
                    reply = wire.receive_response(connection)
                    self.assertEqual(reply["code"], 6)
                    cluster.blocked.clear()
            finally:
                cluster.close()
            paths = list(root.glob(f"trace-{leader}-*.jsonl"))
            self.assertEqual(len(paths), 1)
            content = paths[0].read_text()
            self.assertNotIn("private-key", content)
            self.assertNotIn("private-value", content)
            events = [json.loads(line) for line in content.splitlines()]
            footer = events[-1]
            self.assertEqual(footer["event"], "summary")
            self.assertEqual((footer["dropped"], footer["limited"], footer["io_failed"]), (0, 0, 0))
            reads = {(e["client_port"], e["request_id"]): e for e in events if e["event"] == "read"}
            expected = {(client_port, request) for request in
                        list(range(100, 110)) + list(range(300, 310))} | {(other_port, 100)}
            self.assertTrue(expected <= set(reads))
            self.assertFalse({(client_port, request) for request in range(200, 210)} & set(reads))
            peers = {(e["peer_id"], e["sequence"]): e for e in events if e["event"] == "peer"}
            for port, request in expected:
                read = reads[port, request]
                self.assertEqual(read["client_port"], port)
                self.assertGreater(read["received_tid"], 0)
                self.assertEqual(read["received_tid"], read["tcp_tid"])
                self.assertGreater(read["handled_tid"], 0)
                self.assertNotEqual(read["received_tid"], read["handled_tid"])
                self.assertEqual(read["code"], 0)
                stamps = [read[k] for k in ("received_ns", "handled_ns", "context_ns", "quorum_ns",
                                           "ready_ns", "response_queued_ns", "tcp_loop_ns",
                                           "tcp_dispatch_done_ns")]
                self.assertGreater(stamps[0], 0)
                self.assertEqual(stamps, sorted(stamps))
                peer = peers[read["confirmed_by"], read["confirmed_sequence"]]
                self.assertEqual(peer["context"], read["context"])
                self.assertEqual(peer["success"], 1)
                self.assertEqual(peer["scheduled_tid"], read["handled_tid"])
                self.assertEqual(peer["node_tid"], read["handled_tid"])
                self.assertGreater(peer["worker_tid"], 0)
                self.assertGreater(peer["rpc_tid"], 0)
                self.assertNotEqual(peer["worker_tid"], peer["rpc_tid"])
                stamps = [peer[k] for k in ("scheduled_ns", "worker_ns", "call_ns", "first_write_ns",
                                           "write_done_ns", "decoded_ns", "returned_ns", "node_received_ns")]
                self.assertGreater(stamps[0], 0)
                self.assertEqual(stamps, sorted(stamps))
            self.assertEqual(reads[client_port, 4000]["code"], 6)
            self.assertEqual(reads[client_port, 4000]["quorum_ns"], 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
