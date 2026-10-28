#!/usr/bin/env python3
"""Semantic mutations must fail even when a publisher could refresh file hashes."""
import gzip, importlib.util, json, shutil, sys, tempfile, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location(
    "diagnosis_verifier", ROOT / "scripts/verify-cloud-diagnosis.py"
)
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
PUBLIC = ROOT / "benchmarks/published/cloud-diagnosis-20261024"


class DiagnosisEvidenceTests(unittest.TestCase):
    def copy_case(self, temporary):
        source = PUBLIC / "affinity/cases/original/c32-trace0-original-cpu0-r1"
        target = Path(temporary) / "case"
        shutil.copytree(source, target)
        return target

    def test_percentile_edit_rejected_from_full_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = self.copy_case(temporary)
            r = v.load(p / "result.json")
            r["latency"]["all"]["p99_ms"] += 1
            r["latency"]["get"]["p99_ms"] += 1
            (p / "result.json").write_text(json.dumps(r))
            with self.assertRaisesRegex(ValueError, "percentile"):
                v.verify_case(p, [114, 2], original=True)

    def test_failed_sample_cannot_be_hidden_by_successful_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = self.copy_case(temporary)
            sample = p / "client-0.json.gz"
            with gzip.open(sample, "rt") as f:
                data = json.load(f)
            data["samples"][0][3] = 0
            with gzip.open(sample, "wt") as f:
                json.dump(data, f)
            with self.assertRaisesRegex(ValueError, "failed sample"):
                v.verify_case(p, [114, 2], original=True)

    def test_read_only_disk_mutation_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = self.copy_case(temporary)
            after = v.load(p / "after.json")
            after[1]["disk_bytes"] += 1
            (p / "after.json").write_text(json.dumps(after))
            with self.assertRaisesRegex(ValueError, "log/disk"):
                v.verify_case(p, [114, 2], original=True)

    def test_dropped_trace_footer_rejects_completeness(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = Path(temporary)
            with gzip.open(p / "trace.jsonl.gz", "wt") as f:
                for row in [
                    dict(event="metadata", schema=1),
                    dict(
                        event="summary",
                        accepted=0,
                        written=0,
                        dropped=1,
                        limited=0,
                        io_failed=0,
                    ),
                ]:
                    f.write(json.dumps(row) + "\n")
            with self.assertRaisesRegex(ValueError, "footer"):
                list(v.trace_records(p))

    def test_wrong_affinity_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            p = Path(temporary)
            shutil.copytree(PUBLIC / "affinity/affinity", p / "affinity")
            folder = p / "affinity/original"
            file = folder / "affinity-before-c32-trace0-original-cpu0-r1.json"
            data = v.load(file)
            data["threads"][next(iter(data["threads"]))] = [0, 1]
            file.write_text(json.dumps(data))
            phase = next(
                x
                for x in v.load(PUBLIC / "manifest.json")["phases"]
                if x["phase"] == "original"
            )
            with self.assertRaisesRegex(ValueError, "affinity mask"):
                v.verify_affinity(p, "original", phase["cases"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
