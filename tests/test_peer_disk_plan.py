"""Guard the before/after comparison and opt-in long-run disk selection."""

from pathlib import Path
import sys
import unittest
import copy

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
import cloud_peer_disk
import cloud_soak
from test_async_disk_plan import agent


class PeerDiskPlanTests(unittest.TestCase):
    def test_long_mode_guard_rejects_unmatched_or_traced_nodes(self):
        states = [
            dict(
                disk_executor="background",
                trace_enabled=0,
                trace_written=0,
                trace_dropped=0,
                trace_failed=0,
            )
            for _ in range(3)
        ]
        cloud_soak.check_disk_executor(states, "background")
        cloud_soak.check_disk_executor([], None)
        for change in [
            dict(disk_executor="sync"),
            dict(trace_enabled=1),
            dict(trace_written=1),
            dict(trace_dropped=1),
            dict(trace_failed=1),
        ]:
            different = copy.deepcopy(states)
            different[0].update(change)
            with self.assertRaises(RuntimeError):
                cloud_soak.check_disk_executor(different, "background")

    def test_three_variants_repeat_at_each_order_position_in_both_sizes(self):
        rows = cloud_peer_disk.plan()
        self.assertEqual(len(rows), 18)
        self.assertEqual(len({r["label"] for r in rows}), 18)
        for size in (1, 15):
            orders = [
                [
                    r["variant"]
                    for r in rows
                    if r["cold_mib"] == size and r["repeat"] == repeat
                ]
                for repeat in range(3)
            ]
            for position in range(3):
                self.assertEqual(
                    {order[position] for order in orders}, {"before", "after", "sync"}
                )
        self.assertEqual(len(rows) * 30 * 600, 324000)
        self.assertEqual(len(cloud_peer_disk.plan(True)), 3)

    def test_only_binary_and_disk_variant_change_and_baseline_data_is_isolated(self):
        for row in cloud_peer_disk.plan():
            options = cloud_peer_disk.phase_options(row, "peer-main-" + row["label"])
            self.assertEqual(
                options["program"], "baseline" if row["variant"] == "before" else "new"
            )
            self.assertEqual(
                options["case"].startswith("baseline-"), row["variant"] == "before"
            )
            self.assertEqual(options["encoder"], "background")
            self.assertEqual(
                options["disk_executor"],
                "sync" if row["variant"] == "sync" else "background",
            )
            self.assertFalse(options["trace"])
            self.assertEqual(
                agent.disk_executor_args(options, options["program"]),
                ["--disk-executor", options["disk_executor"]],
            )

    def test_invalid_variants_are_rejected(self):
        row = cloud_peer_disk.plan()[0]
        for change in [
            dict(variant="unknown"),
            dict(executor="sync"),
            dict(cold_mib=64),
            dict(repeat=3),
        ]:
            with self.assertRaises(ValueError):
                cloud_peer_disk.phase_options(dict(row, **change), "test")

    def test_long_run_omission_preserves_earlier_binary_compatibility(self):
        phase = cloud_soak.plan([1], 1)[0]
        original = cloud_soak.phase_options(phase, "case")
        self.assertNotIn("disk_executor", original)
        for mode in ("sync", "background"):
            selected = cloud_soak.phase_options(phase, "case", mode)
            self.assertEqual(selected.pop("disk_executor"), mode)
            self.assertFalse(selected.pop("trace"))
            self.assertEqual(selected, original)
        for mode in ("unknown", True, 0, []):
            with self.assertRaises(ValueError):
                cloud_soak.phase_options(phase, "case", mode)
        self.assertEqual(
            sum(p["windows"] * 60 for p in cloud_soak.plan([1, 8, 15], 60)), 21600
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
