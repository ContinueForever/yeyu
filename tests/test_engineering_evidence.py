#!/usr/bin/env python3
"""Reject changed pacing calendars and impossible reads in retained evidence."""
import gzip, importlib.util, json, shutil, sys, tempfile, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location(
    "engineering_verifier", ROOT / "scripts/verify-engineering.py"
)
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
PUBLIC = ROOT / "benchmarks/published/engineering-20261024"


class EngineeringEvidenceTests(unittest.TestCase):
    def test_changed_pacing_calendar_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = Path(temporary) / "case"
            shutil.copytree(PUBLIC / "results/capacity-fixed/paced-500-r1", p)
            f = p / "client-0.json.gz"
            with gzip.open(f, "rt") as reader:
                raw = json.load(reader)
            raw["samples"][0][4] += 1
            with gzip.open(f, "wt") as writer:
                json.dump(raw, writer)
            with self.assertRaisesRegex(ValueError, "paced schedule"):
                v.verify_case(p, 2)

    def test_impossible_successful_read_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = Path(temporary) / "faults"
            shutil.copytree(PUBLIC / "results/faults", p)
            path = p / "epoch-000.json"
            r = v.load(path)
            read = next(op for op in r["operations"] if op["kind"] == "get")
            read["result"] = "ff" * 40
            path.write_text(json.dumps(r))
            with self.assertRaisesRegex(ValueError, "history rejected"):
                v.fault_histories(p)


if __name__ == "__main__":
    unittest.main(verbosity=2)
