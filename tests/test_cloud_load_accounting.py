#!/usr/bin/env python3
"""Check failed requests, paced omissions and overlapping-sample rejection."""
import gzip, json, sys, tempfile, unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from cloud_load import summarize


class LoadAccountingTests(unittest.TestCase):
    def data(self, p, samples):
        with gzip.open(p / "client-0.json.gz", "wt") as f:
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
                f,
            )
        return dict(
            start_ns=1000,
            clients=[
                dict(
                    client=0,
                    status="complete",
                    final_checked_keys=8,
                    finished_ns=1040,
                    offered_slots=5,
                    missed_slots=2,
                    samples_file="client-0.json.gz",
                )
            ],
        )

    def test_timeout_and_missed_slots_are_not_counted_as_success(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t)
            r = self.data(
                p, [[5, 10, 0, 0, 3, 5], [10, 25, 1, 6, 12, 15], [5, 40, 1, 0, 30, 35]]
            )
            s = summarize(p, r)
            self.assertEqual(
                (s["attempts"], s["successes"], s["failures"], s["missed_slots"]),
                (3, 2, 1, 2),
            )
            self.assertEqual(s["completed_puts"], 1)
            self.assertEqual(s["elapsed_seconds"], 40 / 1e9)
            self.assertEqual(s["scheduled_latency_success_ms"]["samples"], 2)

    def test_overlapping_connection_samples_are_rejected(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t)
            r = self.data(
                p, [[5, 10, 0, 0, 3, 5], [10, 18, 1, 6, 7, 8], [5, 40, 1, 0, 30, 35]]
            )
            with self.assertRaises(AssertionError):
                summarize(p, r)

    def test_omitted_offered_slot_must_be_accounted_for(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t)
            r = self.data(
                p, [[5, 10, 0, 0, 3, 5], [10, 25, 1, 6, 12, 15], [5, 40, 1, 0, 30, 35]]
            )
            r["clients"][0]["missed_slots"] = 1
            with self.assertRaises(AssertionError):
                summarize(p, r)


if __name__ == "__main__":
    unittest.main(verbosity=2)
