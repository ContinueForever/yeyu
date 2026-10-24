#!/usr/bin/env python3
"""Verify short-tooling regression evidence and retained CAS histories offline."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from revision_history import check


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify(root):
    metadata = json.loads((root / "manifest.json").read_text())
    inventory = json.loads((root / "publication-manifest.json").read_text())["files"]
    actual = {
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and p.name != "publication-manifest.json"
    }
    require(actual == set(inventory), "inventory differs")
    for name, expected in inventory.items():
        path = root / name
        require(
            not path.is_symlink()
            and path.resolve().is_relative_to(root.resolve())
            and path.stat().st_size == expected["bytes"]
            and hashlib.sha256(path.read_bytes()).hexdigest() == expected["sha256"],
            "file differs: " + name,
        )
    for kind in ["normal", "asan"]:
        require(
            "100% tests passed, 0 tests failed out of 148"
            in (root / f"tests/ctest-final-{kind}.log").read_text(),
            "regression differs",
        )
    with tarfile.open(root / "source.tar.gz", "r:gz") as archive:
        for name, expected in json.loads(
            (root / "source-manifest.json").read_text()
        ).items():
            require(
                hashlib.sha256(archive.extractfile("source/" + name).read()).hexdigest()
                == expected,
                "source differs: " + name,
            )
    files = sorted((root / "histories").glob("*/*.json"))
    require(
        len(files) == metadata["history_files"] and len(files) >= 6,
        "history inventory differs",
    )
    operations, unknown = 0, 0
    for path in files:
        data = json.loads(path.read_text())
        history = [
            dict(op, end=math.inf) if op.get("optional") else op
            for op in data["operations"]
        ]
        require(
            data["fault"].get("verified")
            and data["fault"]["kind"] in ["kill", "pause", "partition"]
            and any(a["discarded"] for a in data["attempts"])
            and check(history),
            "CAS history rejected",
        )
        operations += len(history)
        unknown += sum(bool(op.get("optional")) for op in history)
    return dict(
        verified=True,
        files=len(inventory),
        history_files=len(files),
        checked_operations=operations,
        unknown_mutations=unknown,
        ordinary_ctest=148,
        asan_ctest=148,
        note="Repeated local test invocations; not independent cloud trials or a general CAS proof.",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.root.resolve())))
