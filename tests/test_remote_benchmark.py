#!/usr/bin/env python3
"""Test external-cluster measurement accounting and a bounded real-cluster smoke."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
import remote_matrix as bench
import test_cluster as harness
from test_cluster_address import DirectCluster


class AccountingTests(unittest.TestCase):
    def test_throughput_uses_shared_start_and_last_response_and_splits_latencies(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            clients = []
            for i, (samples, finish) in enumerate((([[10, 10, 1, 1], [20, 30, 0, 1]], 100),
                                                  ([[30, 40, 0, 1], [40, 80, 1, 1]], 200))):
                name = f"client-{i}.gz"
                with gzip.open(root / name, "wt") as target:
                    json.dump(dict(samples=samples), target)
                clients.append(dict(status="complete", finished_ns=finish + 1000, samples_file=name))
            result = bench.summarize_samples(clients, root, 1000)
            self.assertEqual(result["operations"], 4)
            self.assertEqual(result["completed_puts"], 2)
            self.assertEqual(result["completed_gets"], 2)
            self.assertEqual(result["elapsed_seconds"], 200 / 1e9)
            self.assertEqual(result["ops_per_second"], 20_000_000)
            self.assertEqual(result["latency"]["get"]["samples"], 2)

    def test_nearest_rank_p95_p99_are_recomputed_from_raw_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with gzip.open(root / "samples.gz", "wt") as target:
                json.dump(dict(samples=[[i * 1_000_000, i * (i + 1) // 2 * 1_000_000, 0, 1]
                                        for i in range(1, 101)]), target)
            clients = [dict(status="complete", finished_ns=5_050_000_000, samples_file="samples.gz")]
            result = bench.summarize_samples(clients, root, 0)
            self.assertEqual(result["latency"]["all"]["p95_ms"], 95)
            self.assertEqual(result["latency"]["all"]["p99_ms"], 99)
            self.assertIsNone(result["latency"]["put"])

    def test_failed_request_is_retained_once_and_is_never_replayed(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = dict(keys_per_client=1, prefix="test", seed=1, timeout_seconds=1,
                          value_size=128, warmup_seconds=0, duration_seconds=1, write_percent=100)
            barrier, abort = threading.Barrier(1), threading.Event()
            with patch.object(bench.socket, "create_connection", return_value=MagicMock()), \
                    patch.object(bench.rpc, "rpc", side_effect=[None, TimeoutError("unknown PUT")]) as call:
                result = bench.client_worker(0, config, ("127.0.0.1", 1), barrier,
                                             SimpleNamespace(value=time.perf_counter_ns()),
                                             abort, queue.Queue(), temporary)
            self.assertEqual(call.call_count, 2)  # One prefill, exactly one measured attempt.
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["phase"], "measured")
            self.assertEqual(result["failures"], 1)
            self.assertTrue(abort.is_set())
            with gzip.open(Path(temporary) / result["samples_file"], "rt") as source:
                samples = json.load(source)["samples"]
            self.assertEqual(len(samples), 1)
            self.assertEqual(samples[0][2:], [1, 0])
            with self.assertRaises(RuntimeError):
                bench.summarize_samples([result], Path(temporary), 0)

    def test_different_terms_reject_a_nominal_single_leader(self):
        states = [dict(id=1, role="leader", term=8, leader_id=1),
                  dict(id=2, role="follower", term=7, leader_id=1),
                  dict(id=3, role="follower", term=8, leader_id=1)]
        with patch.object(bench, "query", side_effect=states):
            with self.assertRaisesRegex(RuntimeError, "disagree"):
                bench.cluster_state([("127.0.0.1", p) for p in (1, 2, 3)], 1)


class ExternalClusterSmoke(unittest.TestCase):
    def run_tool(self, *args):
        return subprocess.run([sys.executable, str(ROOT / "benchmarks/remote_matrix.py"), *args],
                              capture_output=True, text=True, timeout=90)

    def test_existing_cluster_and_failure_aware_offline_verification(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-remote-test-") as temporary:
            root = Path(temporary)
            node_dir = root / "nodes"
            node_dir.mkdir()
            cluster = DirectCluster(node_dir, ["127.0.0.2", "127.0.0.3", "127.0.0.4"], shared_port=True)
            try:
                cluster.leader()
                cluster.client("put", "cloud-check", "ready")
                nodes = ",".join(f"{host}:{port}" for host, port in zip(cluster.hosts, cluster.ports))
                output = root / "evidence"
                result = self.run_tool("--nodes", nodes, "--client-node-id", "0", "--output", str(output),
                                       "--duration-seconds", "0.3", "--warmup-seconds", "0.05",
                                       "--repeats", "1", "--concurrency", "1,4,32", "--workers", "2",
                                       "--profiles", "read-only,balanced,write-heavy", "--keys-per-client", "2")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                manifest = json.loads((output / "manifest.json").read_text())
                self.assertEqual(manifest["status"], "complete")
                self.assertEqual(len(manifest["runs"]), 9)
                active_leader = cluster.leader()
                rejected = self.run_tool("--nodes", nodes, "--client-node-id", str(active_leader + 1),
                                         "--preflight", "--output", str(root / "leader-preflight"))
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn("client host is the leader", rejected.stderr)
                self.assertEqual(cluster.client("get", "cloud-check"), "ready")
                self.assertTrue(all(process.poll() is None for process in cluster.processes))
                checked = self.run_tool("--verify", str(output))
                self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)
                # A digest-only checker would accept this after the manifest update.
                name = manifest["runs"][0] + "/result.json"
                path = output / name
                record = json.loads(path.read_text())
                record["ops_per_second"] += 1
                bench.save(path, record)
                manifest["files_sha256"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
                bench.save(output / "manifest.json", manifest)
                checked = self.run_tool("--verify", str(output))
                self.assertNotEqual(checked.returncode, 0)
                self.assertIn("accounting differs", checked.stderr)
                # A strict subset of a successful matrix must not appear complete.
                manifest["runs"].pop()
                bench.save(output / "manifest.json", manifest)
                checked = self.run_tool("--verify", str(output))
                self.assertNotEqual(checked.returncode, 0)
            finally:
                cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path)
    parser.add_argument("--client", type=Path)
    args, remaining = parser.parse_known_args()
    harness.NODE = (args.node or ROOT / "build/bin/yewukv-node").resolve()
    harness.CLIENT = (args.client or ROOT / "build/bin/yewukv-client").resolve()
    unittest.main(argv=[sys.argv[0], *remaining])
