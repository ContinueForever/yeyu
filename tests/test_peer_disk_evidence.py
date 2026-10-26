#!/usr/bin/env python3
"""Semantic negatives refresh checksums and still must fail independent checks."""
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
PUBLICATION = Path(
    os.environ.get(
        "YEWUKV_PEER_PUBLICATION", ROOT / "benchmarks/published/peer-disk-20261026"
    )
)
spec = importlib.util.spec_from_file_location(
    "verifier", ROOT / "scripts/verify-peer-disk.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def refresh(root):
    mapping = read(root / "control/export-map.json")
    for item in mapping:
        path = root / f"nodes/node-{item['node']}" / item["path"]
        item["public_sha256"], item["public_bytes"] = digest(path), path.stat().st_size
        if item["unchanged"]:
            # Forge the claimed original hashes too: rejection must be semantic.
            item["original_sha256"] = item["public_sha256"]
            item["original_bytes"] = item["public_bytes"]
    write(root / "control/export-map.json", mapping)
    manifest = read(root / "manifest.json")
    manifest["files"] = {name: digest(root / name) for name in manifest["files"]}
    write(root / "manifest.json", manifest)
    prefix = "benchmarks/published/peer-disk-20261026/"
    (root / "SHA256SUMS").write_text(
        "\n".join(
            digest(p) + "  " + prefix + str(p.relative_to(root))
            for p in sorted(root.rglob("*"))
            if p.is_file() and p.name != "SHA256SUMS"
        )
        + "\n"
    )


class PeerDiskEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix="yewukv-peer-evidence-negative-"
        )
        self.root = Path(self.temporary.name) / "publication"
        shutil.copytree(PUBLICATION, self.root)
        self.folder = self.root / "nodes/node-3/peer-main/m1-r0-after"

    def tearDown(self):
        self.temporary.cleanup()

    def rejected(self, reason):
        refresh(self.root)
        with self.assertRaisesRegex(ValueError, reason):
            verifier.verify(self.root)

    def test_forged_declared_p99_cannot_replace_raw_samples(self):
        observation = read(self.folder / "observation.json")
        observation["summary"]["p99_ms"] += 100
        write(self.folder / "observation.json", observation)
        progress = read(self.folder.parent / "progress.json")
        progress["completed"][0]["summary"] = observation["summary"]
        write(self.folder.parent / "progress.json", progress)
        self.rejected("raw samples")

    def test_actual_disk_mode_must_match_the_comparison(self):
        path = self.root / "nodes/node-3/peer-main/m1-r0-after/setup.json"
        setup = read(path)
        argv = setup["launched"][0]["argv"]
        argv[argv.index("--disk-executor") + 1] = "sync"
        write(path, setup)
        self.rejected("actual launch configuration")

    def test_bounded_wait_counters_must_conserve_requests(self):
        path = self.folder / "observation.json"
        observation = read(path)
        observation["after"][0]["peer_deferred"] += 1
        write(path, observation)
        self.rejected("peer wait accounting differs")

    def test_controlled_ack_cannot_precede_disk_release(self):
        path = self.root / "probes/cloud-after/result.json"
        probe = read(path)
        probe["acknowledged_ns"] = probe["released_ns"] - 1
        write(path, probe)
        self.rejected("controlled probe identity or ordering differs")

    def test_cpu_counter_regression_is_not_accepted_as_low_cpu_cost(self):
        path = self.folder / "resources.json"
        frames = read(path)
        prefix, fields = frames[1]["servers"][0]["stat"].rsplit(")", 1)
        values = fields.split()
        values[11] = "-1000000"
        frames[1]["servers"][0]["stat"] = prefix + ") " + " ".join(values) + "\n"
        write(path, frames)
        self.rejected("clock/counter regressed")

    def test_forged_recovered_cold_value_hash_is_rejected(self):
        path = self.folder / "restart-verification.json"
        recovery = read(path)
        recovery["values"]["cold-0"]["sha256"] = "0" * 64
        write(path, recovery)
        self.rejected("recovered value differs")

    def test_client_calendar_cannot_hide_unissued_slots(self):
        path = self.folder / "window/client-0.json.gz"
        record = json.loads(gzip.decompress(path.read_bytes()))
        record["samples"][0][1] += 1
        path.write_bytes(gzip.compress(json.dumps(record).encode(), mtime=0))
        self.rejected("exact paced calendar differs")


if __name__ == "__main__":
    unittest.main(verbosity=2)
