#!/usr/bin/env python3
"""Persistence intervals and measured C++ GETs on real three-node processes."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import test_cluster as harness

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from persistence_timeline import analyze, load_trace

BENCH = None
FAULT_LIBRARY = None
ASAN_RUNTIME = None


class PersistenceTraceTests(unittest.TestCase):
    def test_measured_reads_join_real_persistence_intervals_and_restart(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-persist-trace-") as temporary:
            root = Path(temporary)
            prefixes = [root / f"trace-{node}" for node in range(3)]
            arms = [root / f"slow-sync-{node}" for node in range(3)]
            self.assertTrue(FAULT_LIBRARY and FAULT_LIBRARY.exists())
            preload = str(FAULT_LIBRARY)
            if ASAN_RUNTIME:
                preload = str(ASAN_RUNTIME) + ":" + preload
            cluster = harness.Cluster(
                temporary,
                node_env={
                    node: {
                        "LD_PRELOAD": preload,
                        "YEWUKV_TEST_IO_FAULT": str(arm),
                        "YEWUKV_TEST_IO_MODE": "slow-sync",
                        "YEWUKV_TEST_IO_DELAY_MS": "5",
                    }
                    for node, arm in enumerate(arms)
                },
                node_args={
                    node: [
                        "--trace-prefix",
                        str(prefix),
                        "--trace-sample-every",
                        "1",
                        "--snapshot-policy",
                        "fixed",
                        "--snapshot-entries",
                        "8",
                        "--snapshot-bytes",
                        "65536",
                        "--journal-checkpoint-bytes",
                        "131072",
                    ]
                    for node, prefix in enumerate(prefixes)
                },
            )
            try:
                leader = cluster.leader()
                for i in range(32):
                    self.assertEqual(
                        cluster.call(
                            leader, "put", f"cold-{i}".encode(), b"private-value" * 512
                        )["code"],
                        0,
                    )
                # Deliberately slow only this leader's Raft file syncs. A fast
                # machine can legitimately produce zero overlaps without this
                # condition; this is a correlation test, not a latency benchmark.
                arms[leader].touch()
                result = subprocess.run(
                    [
                        str(BENCH),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(cluster.ports[leader]),
                        "--clients",
                        "8",
                        "--duration-ms",
                        "2000",
                        "--warmup-ms",
                        "200",
                        "--trace-identity",
                        "1",
                        "--write-percent",
                        "50",
                        "--value-size",
                        "128",
                        "--offered-rps",
                        "300",
                        "--prefix",
                        "private-key",
                        "--output",
                        str(root / "samples"),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
                arms[leader].unlink()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    cluster.call(leader, "put", b"anchor", b"preserved")["code"], 0
                )
                for process in cluster.processes:
                    process.terminate()
                for process in cluster.processes:
                    self.assertEqual(process.wait(timeout=5), 0)
                paths = list(root.glob(f"trace-{leader}-*.jsonl"))
                self.assertEqual(len(paths), 1)
                events = load_trace(paths[0])
                report = analyze(events, root / "samples")
                self.assertTrue(report["capture_clean"])
                self.assertGreater(report["measured_gets"], 100)
                self.assertEqual(report["joined_fraction"], 1)
                self.assertGreater(report["joined_gets_overlapping_persist"], 0)
                self.assertTrue(
                    any(r["file_sync_overlap_ns"] > 0 for r in report["rows"])
                )
                self.assertTrue(
                    any(
                        e["event"] == "persist"
                        and any(
                            stage == 3 and end - start >= 5_000_000
                            for stage, start, end in e["spans"]
                        )
                        for e in events
                    )
                )
                self.assertTrue(
                    any(e["event"] == "persist" and e["kind"] == 2 for e in events)
                )
                content = paths[0].read_text()
                self.assertNotIn("private-key", content)
                self.assertNotIn("private-value", content)
                for node in range(3):
                    cluster.start(node)
                leader = cluster.leader()
                self.assertEqual(
                    cluster.call(leader, "get", b"anchor")["value"], b"preserved"
                )
            finally:
                cluster.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--node", type=Path, required=True)
    p.add_argument("--client", type=Path, required=True)
    p.add_argument("--bench", type=Path, required=True)
    p.add_argument("--fault-library", type=Path, required=True)
    p.add_argument("--asan-runtime", type=Path)
    args, remaining = p.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    BENCH = args.bench.resolve()
    FAULT_LIBRARY = args.fault_library.resolve()
    ASAN_RUNTIME = args.asan_runtime.resolve() if args.asan_runtime else None
    unittest.main(argv=[__file__, *remaining], verbosity=2)
