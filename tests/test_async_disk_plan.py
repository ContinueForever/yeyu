#!/usr/bin/env python3
"""Validate bounded paired plans and explicit, compatible agent disk selection."""
import copy
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
from cloud_async_disk import check_states, phase_options, plan

spec = importlib.util.spec_from_file_location(
    "agent", ROOT / "scripts/cloud-node-agent.py"
)
agent = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent)


class AsyncDiskPlanTests(unittest.TestCase):
    def test_formal_plan_has_three_pairs_each_size_and_alternates_order(self):
        rows = plan()
        self.assertEqual(len(rows), 12)
        starts = []
        for size in (1, 15):
            for repeat in range(3):
                pair = [
                    r for r in rows if r["cold_mib"] == size and r["repeat"] == repeat
                ]
                self.assertEqual({r["executor"] for r in pair}, {"sync", "background"})
                starts.append(pair[0]["executor"])
        self.assertEqual(starts.count("sync"), 3)
        self.assertEqual(starts.count("background"), 3)
        self.assertEqual(
            len({(r["cold_mib"], r["repeat"], r["executor"]) for r in rows}), 12
        )
        self.assertEqual(len(rows) * 30 * 600, 216000)

    def test_smoke_is_small_and_still_compares_both_executors(self):
        self.assertEqual(
            plan(True),
            [dict(cold_mib=1, repeat=0, executor=m) for m in ("sync", "background")],
        )

    def test_all_other_runtime_choices_are_held_constant_and_trace_is_absent(self):
        for row in plan():
            options = phase_options(row, "test-case")
            self.assertFalse(options["trace"])
            self.assertEqual(options["encoder"], "background")
            self.assertEqual(options["snapshot_policy"], "adaptive")
            self.assertEqual(options["journal_checkpoint_bytes"], 4 * 1024**2)
            self.assertFalse(options["proxies"])
            self.assertEqual(
                agent.disk_executor_args(options, "new"),
                ["--disk-executor", row["executor"]],
            )

    def test_invalid_phase_is_rejected_before_starting_services(self):
        for field, value in [("cold_mib", 64), ("executor", "pool"), ("repeat", 3)]:
            row = dict(plan()[0], **{field: value})
            with self.assertRaises(ValueError):
                phase_options(row, "test-case")

    def test_omitted_mode_preserves_older_binaries(self):
        for program in ("new", "legacy"):
            self.assertEqual(agent.disk_executor_args({}, program), [])

    def test_invalid_or_legacy_explicit_mode_never_reaches_launch(self):
        for mode in (True, 0, [], {}, "pool", ""):
            with self.assertRaises(ValueError):
                agent.disk_executor_args({"disk_executor": mode}, "new")
        for mode in ("sync", "background"):
            with self.assertRaises(ValueError):
                agent.disk_executor_args({"disk_executor": mode}, "legacy")

    def test_state_guard_rejects_tracing_or_an_unmatched_executor(self):
        states = [
            dict(
                disk_executor="background",
                snapshot_encoder="background",
                snapshot_policy="adaptive",
                storage_mode="journal",
                read_mode="read-index",
                trace_enabled=0,
                trace_written=0,
                trace_dropped=0,
                trace_failed=0,
                role="leader" if i == 1 else "follower",
            )
            for i in range(3)
        ]
        check_states(states, "background")
        for field, value in [
            ("trace_enabled", 1),
            ("trace_written", 1),
            ("disk_executor", "sync"),
        ]:
            broken = copy.deepcopy(states)
            broken[0][field] = value
            with self.assertRaises(ValueError):
                check_states(broken, "background")
        states[1]["role"], states[0]["role"] = "follower", "leader"
        with self.assertRaises(ValueError):
            check_states(states, "background")
        check_states(states, "background", fixed_leader=False)


if __name__ == "__main__":
    unittest.main()
