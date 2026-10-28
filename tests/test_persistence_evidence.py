#!/usr/bin/env python3
"""Reject semantic errors even after all declared file hashes are refreshed."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "benchmarks/published/persistence-trace-20261025"
spec = importlib.util.spec_from_file_location(
    "persistence_publication_verifier", ROOT / "scripts/verify-persistence-trace.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def replace(path, data):
    # Replace whole files so the published inputs are never edited in place.
    temporary = path.with_name(path.name + ".negative-new")
    temporary.write_bytes(data)
    temporary.replace(path)


def rehash(root, path):
    p = root / "publication-manifest.json"
    record = json.loads(p.read_text())
    data = path.read_bytes()
    record["files"][str(path.relative_to(root))] = dict(
        bytes=len(data), sha256=digest(data)
    )
    replace(p, json.dumps(record).encode())
    verifier.archive_helpers.inventory(root)


def repack(root, node, name, mutate):
    path = root / f"nodes/node-{node}.tar.gz"
    temporary = path.with_name(path.name + ".negative-new")
    with tarfile.open(path, "r:gz") as source:
        changed = mutate(source.extractfile(name).read())
        export = json.load(source.extractfile("export.json"))
        export["files"][name] = dict(bytes=len(changed), sha256=digest(changed))
        with tarfile.open(temporary, "w:gz") as output:
            for member in source.getmembers():
                data = (
                    changed
                    if member.name == name
                    else (
                        json.dumps(export).encode()
                        if member.name == "export.json"
                        else source.extractfile(member).read()
                    )
                )
                member.size = len(data)
                output.addfile(member, io.BytesIO(data))
    temporary.replace(path)
    rehash(root, path)


class EvidenceNegatives(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="yewukv-persist-negative-")
        self.root = Path(self.temporary.name) / "publication"
        shutil.copytree(PUBLIC, self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_rehashed_trace_switch_mismatch_is_rejected(self):
        def mutate(data):
            record = json.loads(data)
            record["enabled"] = True  # Planned window 00 has tracing off.
            return json.dumps(record).encode()

        repack(self.root, 3, "trace-main/sync/window-00/observation.json", mutate)
        with self.assertRaisesRegex(ValueError, "raw window accounting differs"):
            verifier.verify(self.root)

    def test_rehashed_outside_persistence_span_is_rejected(self):
        captures = json.loads((PUBLIC / "derived/captures.json").read_text())
        name = next(
            r["path"]
            for r in captures
            if r["node"] == 2
            and r["written"] > 0
            and r["path"].startswith("traces/trace-main-sync-node-2/")
        )

        def mutate(data):
            lines = data.splitlines(keepends=True)
            for i, line in enumerate(lines):
                record = json.loads(line)
                if record["event"] == "persist" and record["spans"]:
                    record["spans"][0][2] = record["finished_ns"] + 1
                    lines[i] = json.dumps(record).encode() + b"\n"
                    return b"".join(lines)
            self.fail("no persistence span in fixture")

        repack(self.root, 2, name, mutate)
        with self.assertRaisesRegex(ValueError, "invalid persistence span"):
            verifier.verify(self.root)

    def test_rehashed_summary_cannot_hide_missed_arrivals(self):
        path = self.root / "derived/summary.json"
        record = json.loads(path.read_text())
        record["total"]["missed_slots"] = 0
        replace(path, json.dumps(record).encode())
        rehash(self.root, path)
        with self.assertRaisesRegex(ValueError, "derived campaign summary differs"):
            verifier.verify(self.root)

    def test_node_archive_path_escape_is_rejected(self):
        path = Path(self.temporary.name) / "bad.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            member = tarfile.TarInfo("traces/../../outside")
            member.size = 1
            archive.addfile(member, io.BytesIO(b"x"))
        with self.assertRaisesRegex(ValueError, "unexpected node export member"):
            verifier.unpack(path, Path(self.temporary.name) / "unpacked")


if __name__ == "__main__":
    unittest.main()
