#!/usr/bin/env python3
"""Semantic negative cases use copies; the published archive stays unchanged."""

import copy
import csv
import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "benchmarks/published/cloud-soak-20261024"
sys.path.insert(0, str(ROOT / "benchmarks"))
from cpp_load import summarize
from soak_evidence import counter_delta, resource_rows

spec = importlib.util.spec_from_file_location(
    "soak_publication_verifier", ROOT / "scripts/verify-cloud-soak-publication.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class EvidenceNegatives(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="yewukv-soak-negative-")
        cls.folder = Path(cls.temporary.name)
        with tarfile.open(PUBLIC / "results/phase-00.tar.gz", "r:gz") as archive:
            for member in archive:
                if member.name.endswith("resources-000.json"):
                    cls.frames = json.load(archive.extractfile(member))
                if "/window-000/" in member.name:
                    data = archive.extractfile(member).read()
                    (cls.folder / Path(member.name).name).write_bytes(data)
        cls.env = json.loads((PUBLIC / "control/node-environment-after.json").read_text())
        observation = json.loads((cls.folder / "observation.json").read_text())
        cls.before, cls.after = observation["before"][1], observation["after"][1]

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_original_service_interruption_is_not_healthy(self):
        healthy = resource_rows(self.frames, self.env)
        self.assertGreater(healthy["node_2"]["peak_rss_bytes"], 0)
        frames = copy.deepcopy(self.frames)
        frames[1]["servers"][0]["original_service"] = (
            "NRestarts=1\nActiveState=active\nDropInPaths=\n"
        )
        with self.assertRaisesRegex(ValueError, "service state"):
            resource_rows(frames, self.env)

    def test_pid_reuse_does_not_hide_process_restart(self):
        frames = copy.deepcopy(self.frames)
        server = frames[1]["servers"][0]
        head, tail = server["stat"].rsplit(")", 1)
        fields = tail.split()
        fields[19] = str(int(fields[19]) + 1)
        server["stat"] = head + ") " + " ".join(fields)
        with self.assertRaisesRegex(ValueError, "process changed"):
            resource_rows(frames, self.env)

    def test_histogram_reset_cannot_be_a_window_distribution(self):
        counter_delta(self.before, self.after)
        after = copy.deepcopy(self.after)
        after["disk_journal_sync_duration_buckets"][0] = -1
        with self.assertRaisesRegex(ValueError, "histogram counter regressed"):
            counter_delta(self.before, after)

    def test_unchanged_counts_do_not_hide_wrong_arrival_calendar(self):
        self.assertTrue(summarize(self.folder)["valid"])
        path = self.folder / "client-0.json.gz"
        original = path.read_bytes()
        try:
            record = json.loads(gzip.decompress(original))
            record["samples"][0][1] += 1
            path.write_bytes(gzip.compress(json.dumps(record).encode(), mtime=0))
            with self.assertRaisesRegex(ValueError, "calendar differs"):
                summarize(self.folder)
        finally:
            path.write_bytes(original)

    def test_result_archives_cannot_escape_the_expected_phase(self):
        archive_path = self.folder / "unexpected.tar.gz"
        with tarfile.open(archive_path, "w:gz") as archive:
            member = tarfile.TarInfo("soak-main/phase-00/../../outside")
            member.size = 1
            archive.addfile(member, io.BytesIO(b"x"))
        with self.assertRaisesRegex(ValueError, "unsafe or unexpected"):
            verifier.unpack(archive_path, self.folder, ["soak-main/phase-00"])

    def test_csv_cannot_silently_understate_tail_latency(self):
        summary = json.loads((PUBLIC / "derived/summary.json").read_text())
        verifier.verify_csv(PUBLIC / "derived", summary)
        with (PUBLIC / "derived/windows.csv").open(newline="") as f:
            rows = list(csv.DictReader(f))
        rows[0]["p99_ms"] = "0"
        with (self.folder / "windows.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        with self.assertRaisesRegex(ValueError, "derived CSV differs"):
            verifier.verify_csv(self.folder, summary)


if __name__ == "__main__":
    unittest.main()
