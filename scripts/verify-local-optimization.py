#!/usr/bin/env python3
"""Check published optimization hashes, latency samples and bounded fault histories.

This rechecks retained evidence; it does not rerun servers or benchmark hardware.
"""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import sys
import tarfile
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
sys.path.insert(0, str(ROOT / "tests"))
import local_optimization as performance
from linearizability import check


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_files(root):
    files = json.loads((root / "publication-manifest.json").read_text())["files"]
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file() and
              p != root / "publication-manifest.json"}
    require(actual == set(files), "published file inventory differs")
    for name, expected in files.items():
        relative = PurePosixPath(name)
        require(not relative.is_absolute() and ".." not in relative.parts, "unsafe path")
        path = root / name
        require(not path.is_symlink() and path.resolve().is_relative_to(root), "unsafe file")
        content = path.read_bytes()
        require(len(content) == expected["bytes"] and hashlib.sha256(content).hexdigest() == expected["sha256"],
                f"file checksum differs: {name}")
    for source in root.rglob("source-manifest.json"):
        expected = json.loads(source.read_text())
        with tarfile.open(source.with_name("source.tar.gz"), "r:gz") as archive:
            members = archive.getmembers()
            require(len(members) == len(expected) and {m.name for m in members} == set(expected),
                    "source archive inventory differs")
            for member in members:
                require(member.isfile(), "non-file source archive member")
                with archive.extractfile(member) as file:
                    require(hashlib.sha256(file.read()).hexdigest() == expected[member.name], "source archive checksum differs")
    return len(files)


def verify_faults(directory):
    summary = json.loads((directory / "summary.json").read_text())
    require(summary["status"] == "complete", "incomplete fault campaign")
    epochs, checked, attempts, unknown, kinds = 0, 0, 0, 0, Counter()
    with gzip.open(directory / "epochs.jsonl.gz", "rt") as source:
        for line in source:
            record = json.loads(line)
            history, result = record["history"], record["result"]
            require(history["epoch"] == result["epoch"] == epochs, "fault epoch sequence differs")
            reconstructed = []
            for attempt in history["attempts"]:
                if attempt["kind"] == "get":
                    include = attempt["code"] in (0, 1)
                else:
                    include = attempt["code"] in (0, 6)
                if include:
                    operation = dict(attempt)
                    if attempt["kind"] != "get" and attempt["code"] == 6:
                        operation.update(optional=True, end=None)
                    reconstructed.append(operation)
            require(reconstructed == history["operations"], "model input differs from recorded attempts")
            model = [{**op, "end": math.inf if op.get("optional") else op["end"]}
                     for op in history["operations"]]
            require(all(op["end"] is None for op in history["operations"] if op.get("optional")),
                    "unknown writes must be unbounded")
            require(result["checked"] and check(model), f"history rejected at epoch {epochs}")
            require(len(model) == result["checked_operations"], "checked count differs")
            require(len(history["attempts"]) == result["attempted_operations"], "attempt count differs")
            require(max(Counter(op["key"] for op in model).values(), default=0) <= 9, "per-key bound exceeded")
            optional = sum(op.get("optional", False) for op in model)
            require(optional == result["unknown_writes"], "unknown write count differs")
            kinds[result["fault"]] += 1
            epochs += 1
            checked += len(model)
            attempts += len(history["attempts"])
            unknown += optional
    for name, value in (("epochs", epochs), ("checked_operations", checked),
                        ("attempted_operations", attempts), ("unknown_writes", unknown), ("fault_counts", dict(kinds))):
        require(summary[name] == value, f"fault summary differs: {name}")
    return epochs, checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    root = parser.parse_args().directory.resolve()
    count = verify_files(root)
    performance.verify(root / "performance")
    manifest = json.loads((root / "performance/manifest.json").read_text())
    identities = [(r["profile"], r["variant"], r["repeat"]) for r in manifest["runs"]]
    expected = {(p, v, n) for p in manifest["profiles"] for v in performance.VARIANTS
                for n in range(1, manifest["repeats"] + 1)}
    require(len(identities) == len(expected) and set(identities) == expected, "missing or duplicate workload")
    epochs, checked = verify_faults(root / "faults")
    print(json.dumps(dict(verified=True, published_files=count, fault_epochs=epochs,
                          model_checked_operations=checked)))


if __name__ == "__main__":
    main()
