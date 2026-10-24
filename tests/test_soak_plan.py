import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from cloud_soak import plan, guard


class SoakPlanTests(unittest.TestCase):
    def test_long_plan_and_original_service_guard(self):
        phases = plan([1, 8, 15], 60)
        self.assertEqual(len(phases), 6)
        self.assertEqual(sum(p["windows"] * 60 for p in phases), 21600)
        self.assertEqual(
            [(p["cold_mib"], p["encoder"]) for p in phases],
            [
                (1, "sync"),
                (1, "background"),
                (8, "background"),
                (8, "sync"),
                (15, "sync"),
                (15, "background"),
            ],
        )
        # A stopped/changed original always aborts the experiment guard.
        with self.assertRaisesRegex(RuntimeError, "original service"):
            guard(
                [
                    dict(
                        original_service="ActiveState=inactive\nNRestarts=0\nDropInPaths=\n"
                    )
                ]
                * 3
            )
        with self.assertRaisesRegex(RuntimeError, "missing resource"):
            guard([])


if __name__ == "__main__":
    unittest.main(verbosity=2)
