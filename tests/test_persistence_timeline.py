#!/usr/bin/env python3
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from persistence_timeline import analyze, union_overlap, validate_client_ranges

FIRST_ID = (1 << 62) + 100


class TimelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.folder = Path(self.temporary.name)
        config = dict(
            schema=2,
            trace_identity=True,
            valid=True,
            columns=[
                "latency_ns",
                "scheduled_offset_ns",
                "started_offset_ns",
                "completed_offset_ns",
                "put",
                "code",
            ],
            clients=1,
            offered_rps=0,
            duration_ms=100,
            max_samples=128,
            attempts=1,
            successes=1,
            failures=0,
            offered_slots=1,
            missed_slots=0,
            elapsed_ns=100000000,
            setup_failed=False,
            cancelled=False,
            sample_limit_reached=False,
        )
        (self.folder / "result.json").write_text(json.dumps(config))
        (self.folder / "client-0.json").write_text(
            json.dumps(
                dict(
                    client=0,
                    local_port=12345,
                    first_request_id=FIRST_ID,
                    connection_first_request_id=FIRST_ID - 8,
                    offered_slots=1,
                    samples=[[1000, 0, 0, 1000, 0, 0]],
                )
            )
        )
        self.events = [
            dict(event="metadata", node=1, schema=2, pid=11, sample_every=32),
            dict(
                event="persist",
                node=1,
                sequence=1,
                kind=2,
                tid=7,
                started_ns=150,
                finished_ns=450,
                success=1,
                bytes=100,
                crc_ns=0,
                spans_limited=0,
                spans=[[3, 200, 400]],
            ),
            dict(
                event="read",
                node=1,
                client_port=12345,
                request_id=FIRST_ID,
                code=0,
                handled_tid=7,
                received_ns=100,
                handled_ns=300,
                context_ns=400,
                quorum_ns=500,
                ready_ns=600,
                response_queued_ns=700,
                tcp_loop_ns=800,
                tcp_dispatch_done_ns=900,
            ),
            dict(
                event="summary",
                node=1,
                accepted=2,
                written=2,
                dropped=0,
                limited=0,
                io_failed=0,
            ),
        ]

    def tearDown(self):
        self.temporary.cleanup()

    def test_union_never_double_counts_overlapping_intervals(self):
        self.assertEqual(union_overlap([(0, 100), (10, 70), (90, 120)], 20, 110), 90)

    def test_sequential_ids_without_randomized_identity_are_not_joined(self):
        path = self.folder / "result.json"
        config = json.loads(path.read_text())
        config["trace_identity"] = False
        path.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, "randomized trace identity"):
            analyze(self.events, self.folder)

    def test_reused_random_identity_blocks_are_rejected_across_windows(self):
        with self.assertRaisesRegex(ValueError, "identity ranges overlap"):
            validate_client_ranges([self.folder, self.folder])

    def test_same_worker_queue_and_handled_overlap_are_separate(self):
        r = analyze(self.events, self.folder)
        self.assertTrue(r["capture_clean"])
        self.assertEqual(r["joined_gets"], 1)
        row = r["rows"][0]
        self.assertEqual(
            (row["queue_persist_overlap_ns"], row["handled_persist_overlap_ns"]),
            (150, 150),
        )
        self.assertEqual(row["persist_overlap_ns"], 300)
        self.assertEqual(row["file_sync_overlap_ns"], 200)

    def test_other_thread_io_is_not_attributed_to_the_read_worker(self):
        events = copy.deepcopy(self.events)
        events[1]["tid"] = 8
        r = analyze(events, self.folder)
        self.assertEqual(r["rows"][0]["persist_overlap_ns"], 0)

    def test_reused_port_and_request_id_are_ambiguous_not_guessed(self):
        events = copy.deepcopy(self.events)
        events.insert(3, copy.deepcopy(events[2]))
        events[-1].update(written=3, accepted=3)
        r = analyze(events, self.folder)
        self.assertEqual((r["joined_gets"], r["ambiguous_gets"]), (0, 1))

    def test_dropped_and_truncated_capture_is_not_complete(self):
        self.events[-1]["dropped"] = 1
        self.assertFalse(analyze(self.events, self.folder)["capture_clean"])
        self.assertFalse(analyze(self.events[:-1], self.folder)["capture_clean"])

    def test_mixed_node_clock_and_crc_double_accounting_are_rejected(self):
        events = copy.deepcopy(self.events)
        events[1]["node"] = 2
        with self.assertRaisesRegex(ValueError, "mixed node clocks"):
            analyze(events, self.folder)
        events = copy.deepcopy(self.events)
        events[1]["crc_ns"] = 10
        with self.assertRaisesRegex(ValueError, "CRC"):
            analyze(events, self.folder)


if __name__ == "__main__":
    unittest.main()
