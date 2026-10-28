#!/usr/bin/env python3
"""Reject semantic mutations even after both digest inventories are refreshed."""
import argparse
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('cloud_perf', REPO/'scripts/verify-cloud-performance.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)
EVIDENCE = REPO/'benchmarks/published/cloud-performance-20261023'


def save(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def refresh(root):
    experiment = root/'experiment'
    inner = verifier.load(experiment/'manifest.json')
    inner['files_sha256'] = {str(p.relative_to(experiment)): hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in experiment.rglob('*') if p.is_file() and p.name != 'manifest.json'}
    save(experiment/'manifest.json', inner)
    outer = verifier.load(root/'publication-manifest.json')
    outer['files'] = {str(p.relative_to(root)): dict(bytes=p.stat().st_size,
                      sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in root.rglob('*')
                      if p.is_file() and p.name != 'publication-manifest.json'}
    save(root/'publication-manifest.json', outer)


class EvidenceMutations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.baseline = verifier.verify(EVIDENCE)
        assert cls.baseline['operations'] == 1954767

    def mutated(self, change, expected):
        with tempfile.TemporaryDirectory(prefix='yewukv-cloud-performance-negative-') as temporary:
            root = Path(temporary)/'evidence'
            shutil.copytree(EVIDENCE, root)
            inner = verifier.load(root/'experiment/manifest.json')
            change(root, inner)
            refresh(root)
            with self.assertRaisesRegex(ValueError, expected):
                verifier.verify(root)

    def test_changed_p99_is_recomputed_from_samples(self):
        def change(root, inner):
            path = root/'experiment'/inner['runs'][0]/'result.json'
            data = verifier.load(path)
            data['latency']['all']['p99_ms'] += 1
            save(path, data)
        self.mutated(change, 'raw sample percentiles differ')

    def test_missing_planned_case_cannot_appear_complete(self):
        def change(root, inner):
            inner['runs'].pop()
            inner['planned_runs'].pop()
            save(root/'experiment/manifest.json', inner)
        self.mutated(change, 'matrix is missing or repeats a case')

    def test_changed_applied_index_is_rejected(self):
        def change(root, inner):
            path = root/'experiment'/inner['runs'][0]/'after.json'
            data = verifier.load(path)
            data[1]['applied_index'] += 1
            save(path, data)
        self.mutated(change, 'node state differs')

    def test_mismatched_node_executable_is_rejected(self):
        def change(root, inner):
            path = root/'nodes/node-2-environment.txt'
            path.write_text(path.read_text().replace(verifier.BINARY_HASHES['yewukv-node'], '0'*64))
        self.mutated(change, 'installed binary hashes differ')

    def test_hidden_failed_request_is_rejected(self):
        def change(root, inner):
            directory = root/'experiment'/inner['runs'][0]
            data = verifier.load(directory/'result.json')
            path = directory/data['clients'][0]['samples_file']
            with gzip.open(path, 'rt') as source:
                raw = json.load(source)
            raw['samples'][0][3] = 0
            with gzip.open(path, 'wt') as target:
                json.dump(raw, target)
        self.mutated(change, 'invalid or failed sample')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=EVIDENCE)
    args, remaining = parser.parse_known_args()
    EVIDENCE = args.root.resolve()
    unittest.main(argv=[sys.argv[0], *remaining])
