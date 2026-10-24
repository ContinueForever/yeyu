#!/usr/bin/env python3
"""Check benchmark accounting without relying on machine speed or real sockets."""

import argparse
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
import rpc_baseline as bench


def options(**overrides):
    values = dict(host="127.0.0.1", port=9000, clients=2, timeout=1,
                  value_size=128, warmup_ops=3, ops_per_client=101, write_percent=50)
    values.update(overrides)
    return argparse.Namespace(**values)


class BenchmarkTests(unittest.TestCase):
    def test_warmup_excluded_and_actual_mix_counted(self):
        barrier = threading.Barrier(1)
        calls = []
        def record(sock, request_id, operation, key, value):
            calls.append((request_id, operation))
        with patch.object(bench.socket, "create_connection", return_value=MagicMock()), \
                patch.object(bench, "rpc", side_effect=record):
            latencies, _, writes = bench.worker(0, options(), barrier)
        self.assertEqual(len(calls), 1 + 3 + 101)
        self.assertEqual(len(latencies), 101)
        self.assertEqual(writes, 51)  # Partial 100-op blocks are not exactly 50%.
        self.assertEqual([call[0] for call in calls], list(range(1, 106)))

    def test_timer_starts_before_workers_and_ends_at_last_response(self):
        def worker(client_id, args, barrier):
            barrier.wait(timeout=1)
            return [10], 200 + 100 * client_id, 1
        with patch.object(bench.time, "perf_counter_ns", return_value=100), \
                patch.object(bench, "worker", side_effect=worker):
            samples, elapsed, writes = bench.measure(options())
        self.assertEqual(samples, [10, 10])
        self.assertEqual(elapsed, 200 / 1_000_000_000)
        self.assertEqual(writes, 2)

    def test_connect_failure_aborts_waiting_peers(self):
        barrier = threading.Barrier(2)
        with patch.object(bench.socket, "create_connection", side_effect=ConnectionError("refused")):
            with self.assertRaises(ConnectionError):
                bench.worker(0, options(), barrier)
        self.assertTrue(barrier.broken)

    def test_percentile_uses_nearest_rank(self):
        samples = [i * 1_000_000 for i in range(1, 101)]
        self.assertEqual(bench.percentile(samples, 0.5), 50)
        self.assertEqual(bench.percentile(samples, 0.99), 99)

    def test_wrong_get_value_fails_run(self):
        # Response: request_id=1, value='bad', success status omitted.
        payload = b"\x08\x01\x1a\x03bad"
        sock = MagicMock()
        with patch.object(bench, "read_exact", side_effect=[len(payload).to_bytes(4, "big"), payload]):
            with self.assertRaisesRegex(ValueError, "wrong value"):
                bench.rpc(sock, 1, "get", b"key", b"expected")


if __name__ == "__main__":
    unittest.main()
