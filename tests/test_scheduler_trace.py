import gzip
from pathlib import Path
import tempfile
import unittest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from scheduler_trace import parse, Intervals


class SchedulerParserTests(unittest.TestCase):
    def test_wakeup_preemption_and_sleep_are_different_intervals(self):
        lines = [
            "swapper 0/0 [000] 1.000000000: sched:sched_switch: swapper/0:0 [120] R ==> node:10 [120]",
            "node 10/10 [000] 1.005000000: sched:sched_switch: node:10 [120] S ==> swapper/0:0 [120]",
            "worker 20/20 [001] 1.010000000: sched:sched_wakeup: node:10 [120] CPU:000",
            "swapper 0/0 [000] 1.050000000: sched:sched_switch: swapper/0:0 [120] R ==> node:10 [120]",
            ":-1 -1/-1 [000] 1.052000000: sched:sched_switch: node:10 [120] R+ ==> worker:20 [120]",
            "worker 20/20 [000] 1.062000000: sched:sched_switch: worker:20 [120] S ==> node:10 [120]",
            "node 10/10 [000] 1.063000000: sched:sched_switch: node:10 [120] S ==> swapper/0:0 [120]",
            "worker 20/20 [001] 1.070000000: sched:sched_wakeup: node:10 [120] CPU:000",
            "swapper 0/0 [000] 1.071000000: sched:sched_switch: swapper/0:0 [120] R ==> node:10 [120]",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.txt.gz"
            with gzip.open(path, "wt") as out:
                out.write("\n".join(lines) + "\n")
            data = parse(path)
            self.assertEqual(
                [(b - a) / 1e6 for a, b, _ in data["runnable"][10]], [40, 10, 1]
            )
            self.assertEqual(
                [(b - a) / 1e6 for a, b, _ in data["sleeping"][10]], [5, 7]
            )
            amount, _ = Intervals(data["runnable"][10]).overlap(
                1_020_000_000, 1_060_000_000
            )
            self.assertEqual(amount, 38)

    def test_lost_records_reject_attribution(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.txt.gz"
            with gzip.open(path, "wt") as out:
                out.write("PERF_RECORD_LOST 100\n")
            with self.assertRaisesRegex(ValueError, "lost"):
                parse(path)

    def test_cpu_sequence_gap_rejects_attribution(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.txt.gz"
            with gzip.open(path, "wt") as out:
                out.write(
                    "swapper 0/0 [000] 1.000000000: sched:sched_switch: swapper/0:0 [120] R ==> node:10 [120]\n"
                )
                out.write(
                    "other 99/99 [000] 1.010000000: sched:sched_switch: other:99 [120] S ==> node:10 [120]\n"
                )
            with self.assertRaises(AssertionError):
                parse(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
