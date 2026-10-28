#!/usr/bin/env python3
"""CAS contention, ABA protection and background-snapshot recovery over real TCP."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest

import test_cluster as harness
import test_rpc_network as wire
from test_cluster_snapshot import wait_for


def cas(cluster, node, key, revision, value):
    body = (
        wire.bytes_field(1, key)
        + wire.varint(16)
        + wire.varint(revision)
        + wire.bytes_field(3, value)
    )
    with socket.create_connection(
        ("127.0.0.1", cluster.ports[node]), timeout=5
    ) as connection:
        connection.sendall(wire.frame(b"\x08\x01" + wire.bytes_field(9, body)))
        return wire.receive_response(connection)


class CasSnapshotTests(unittest.TestCase):
    def test_contending_publishers_restart_aba_and_snapshot_catchup(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-cas-snapshot-") as directory:
            cluster = harness.Cluster(
                directory,
                extra_args=[
                    "--snapshot-encoder",
                    "background",
                    "--snapshot-policy",
                    "fixed",
                    "--snapshot-entries",
                    "8",
                    "--snapshot-bytes",
                    "32768",
                ],
            )
            try:
                leader = cluster.leader()
                initial = cas(cluster, leader, b"versioned", 0, b"first")
                self.assertEqual(initial["code"], 0)
                revision = initial["revision"]
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(
                        pool.map(
                            lambda value: cas(
                                cluster, leader, b"versioned", revision, value
                            ),
                            [b"publisher-a", b"publisher-b"],
                        )
                    )
                self.assertEqual(sorted(r["code"] for r in results), [0, 7])
                winner = cluster.call(leader, "get", b"versioned")
                self.assertGreater(winner["revision"], revision)
                metadata = subprocess.run(
                    [
                        str(harness.CLIENT),
                        "--port",
                        str(cluster.ports[leader]),
                        "get-meta",
                        "versioned",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertEqual(
                    json.loads(metadata.stdout)["revision"], winner["revision"]
                )

                lagging = (leader + 1) % 3
                cluster.kill(lagging)
                cold = b"c" * 8192
                for index in range(64):
                    self.assertEqual(
                        cluster.call(leader, "put", f"cold-{index}".encode(), cold)[
                            "code"
                        ],
                        0,
                    )
                wait_for(cluster, lambda: cluster.status(leader)["snapshot_index"] > 32)
                cluster.start(lagging)
                wait_for(
                    cluster, lambda: cluster.status(lagging)["snapshots_installed"] > 0
                )
                wait_for(
                    cluster,
                    lambda: cluster.status(lagging)["applied_index"]
                    == cluster.status(leader)["applied_index"],
                )
                for node in range(3):
                    cluster.kill(node)
                for node in range(3):
                    cluster.start(node)
                leader = cluster.eventually_value(b"versioned", winner["value"])
                recovered = cluster.call(leader, "get", b"versioned")
                self.assertEqual(recovered["revision"], winner["revision"])
                self.assertEqual(
                    cas(cluster, leader, b"versioned", revision, b"stale")["code"], 7
                )
                self.assertEqual(cluster.call(leader, "del", b"versioned")["code"], 0)
                replacement = cas(cluster, leader, b"versioned", 0, winner["value"])
                self.assertEqual(replacement["code"], 0)
                self.assertGreater(replacement["revision"], winner["revision"])
                self.assertEqual(
                    cas(cluster, leader, b"versioned", winner["revision"], b"ABA")[
                        "code"
                    ],
                    7,
                )
                for index in range(64):
                    self.assertEqual(
                        cluster.call(leader, "get", f"cold-{index}".encode())["value"],
                        cold,
                    )
                state = cluster.status(leader)
                self.assertEqual(state["snapshot_encoder"], "background")
            finally:
                cluster.close()

    def test_config_publish_condition_conflicts_without_overwriting(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-config-cas-") as directory:
            cluster = harness.Cluster(directory)
            try:
                cluster.leader()
                config = Path(directory) / "config.json"
                config.write_text(
                    json.dumps(
                        dict(message="first", enabled=True, request_timeout_ms=1000)
                    )
                )
                app = Path(__file__).resolve().parents[1] / "examples/config_store.py"
                import sys

                command = [
                    sys.executable,
                    str(app),
                    "--client",
                    str(harness.CLIENT),
                    "--nodes",
                    ",".join(f"127.0.0.1:{p}" for p in cluster.ports),
                ]
                subprocess.run(
                    [
                        *command,
                        "publish",
                        "--file",
                        str(config),
                        "--expected-revision",
                        "0",
                    ],
                    check=True,
                    capture_output=True,
                )
                meta = json.loads(subprocess.check_output([*command, "inspect"]))
                old = meta["storage_revision"]
                self.assertGreater(old, 0)
                config.write_text(
                    json.dumps(
                        dict(message="winner", enabled=True, request_timeout_ms=1000)
                    )
                )
                subprocess.run(
                    [
                        *command,
                        "publish",
                        "--file",
                        str(config),
                        "--expected-revision",
                        str(old),
                    ],
                    check=True,
                    capture_output=True,
                )
                config.write_text(
                    json.dumps(
                        dict(message="loser", enabled=True, request_timeout_ms=1000)
                    )
                )
                result = subprocess.run(
                    [
                        *command,
                        "publish",
                        "--file",
                        str(config),
                        "--expected-revision",
                        str(old),
                    ],
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("conflict", result.stderr)
                shown = json.loads(subprocess.check_output([*command, "show"]))
                self.assertEqual(shown["config"]["message"], "winner")
                leader = cluster.leader()
                self.assertEqual(
                    cluster.call(leader, "put", b"config/demo", b"[]")["code"], 0
                )
                malformed = subprocess.run(
                    [*command, "inspect"], capture_output=True, text=True
                )
                self.assertNotEqual(malformed.returncode, 0)
                self.assertIn("invalid configuration", malformed.stderr)
                self.assertNotIn("Traceback", malformed.stderr)
            finally:
                cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
