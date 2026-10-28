#!/usr/bin/env python3
"""Check semantic rejection on the real, separately archived preflight windows."""
import gzip
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
from background_soak_evidence import verify_raw
from cpp_load import read

spec = importlib.util.spec_from_file_location(
    "background_publication", ROOT / "scripts/verify-background-soak.py"
)
publication = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publication)
PUBLICATION = ROOT / "benchmarks/published/background-soak-20261026"


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class BackgroundEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="yewukv-background-fixture-")
        target = Path(cls.fixture.name)
        private = os.environ.get("YEWUKV_BACKGROUND_RAW")
        if private:
            shutil.copytree(Path(private), target / "soak-smoke")
            cls.environment = read(Path(os.environ["YEWUKV_BACKGROUND_ENV"]))
        else:
            manifest = read(PUBLICATION / "manifest.json")
            publication.unpack_parts(
                PUBLICATION, manifest["other_archive"], target, ["nodes"]
            )
            cls.environment = read(PUBLICATION / "control/node-environment.json")
            shutil.copytree(target / "nodes/node-3/soak-smoke", target / "soak-smoke")
        cls.source = target / "soak-smoke"
        # Confirm the unmodified fixture first; no tests depend on an invalid baseline.
        verify_raw(cls.source, cls.environment)

    @classmethod
    def tearDownClass(cls):
        cls.fixture.cleanup()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix="yewukv-background-negative-"
        )
        self.root = Path(self.temporary.name) / "soak-smoke"
        shutil.copytree(self.source, self.root)
        self.phase = self.root / "phase-00"
        self.window = self.phase / "window-000"

    def tearDown(self):
        self.temporary.cleanup()

    def rejected(self, reason):
        # Raw checks are run directly: checksums cannot mask these semantic negatives.
        with self.assertRaisesRegex(ValueError, reason):
            verify_raw(self.root, self.environment)

    def test_actual_launch_mode_is_checked(self):
        path = self.phase / "setup.json"
        setup = read(path)
        argv = setup["launched"][0]["argv"]
        argv[argv.index("--disk-executor") + 1] = "sync"
        write(path, setup)
        self.rejected("actual background launch differs")

    def test_counter_conservation_is_checked(self):
        path = self.window / "observation.json"
        observation = read(path)
        observation["after"][0]["peer_deferred"] += 1
        write(path, observation)
        self.rejected("peer wait accounting differs")

    def test_calendar_cannot_hide_unissued_slots(self):
        path = self.window / "client-0.json.gz"
        record = json.loads(gzip.decompress(path.read_bytes()))
        record["samples"][0][1] += 1
        path.write_bytes(gzip.compress(json.dumps(record).encode(), mtime=0))
        self.rejected("exact paced calendar differs")

    def test_summary_cannot_replace_raw_percentile(self):
        path = self.window / "observation.json"
        observation = read(path)
        observation["summary"]["p999_ms"] += 100
        write(path, observation)
        self.rejected("window summary differs")

    def test_resource_process_counter_regression_is_rejected(self):
        path = self.phase / "resources-000.json"
        frames = read(path)
        prefix, fields = frames[1]["servers"][0]["stat"].rsplit(")", 1)
        fields = fields.split()
        fields[11] = "-1000000"
        frames[1]["servers"][0]["stat"] = prefix + ") " + " ".join(fields)
        write(path, frames)
        self.rejected("clock/counter regressed")

    def test_restart_anchor_revision_is_checked(self):
        path = self.phase / "restart-verification.json"
        recovery = read(path)
        recovery["anchor_revision"] += 1
        write(path, recovery)
        self.rejected("recovery differs")

    def test_runtime_trace_cannot_be_silently_enabled(self):
        path = self.window / "observation.json"
        observation = read(path)
        observation["before"][0]["trace_enabled"] = 1
        write(path, observation)
        self.rejected("background mode, trace or convergence differs")


if __name__ == "__main__":
    unittest.main(verbosity=2)
