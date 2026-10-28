#!/usr/bin/env python3
"""Manifest a stopped cluster copy, or verify it before a restore."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath

MANIFEST = "backup-manifest.json"


def check_journals(root):
    for node in (1, 2, 3):
        image = (root / f"node-{node}/raft.state").read_bytes()
        if image[:8] == b"YRFT\x00\x00\x00\x03":
            if len(image) < 68:
                raise ValueError("truncated journal checkpoint")
            generation = int.from_bytes(image[56:64], "big")
            if not generation or not (root / f"node-{node}/raft.wal.{generation}").is_file():
                raise ValueError("backup is missing the checkpoint-bound journal")


def hashes(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("backup must not contain symlinks")
        if path.is_file() and path.name != MANIFEST:
            result[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def manifest(root):
    for node in (1, 2, 3):
        for name in ("raft.state", "cluster.identity"):
            if not (root / f"node-{node}" / name).is_file():
                raise ValueError(f"backup is missing node-{node}/{name}")
    check_journals(root)
    (root / MANIFEST).write_text(json.dumps({"schema": 1, "files": hashes(root)}, indent=2) + "\n")


def verify(root):
    data = json.loads((root / MANIFEST).read_text())
    if data.get("schema") != 1 or not isinstance(data.get("files"), dict):
        raise ValueError("invalid backup manifest")
    for name in data["files"]:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("invalid manifest path")
    if hashes(root) != data["files"]:
        raise ValueError("backup checksums or file list differ")
    for node in (1, 2, 3):
        for name in ("raft.state", "cluster.identity"):
            if f"node-{node}/{name}" not in data["files"]:
                raise ValueError("incomplete cluster backup")
    check_journals(root)


def verify_local(root):
    verify(root)
    members = "".join(f"{i}=127.0.0.1:{9100 + i}\n" for i in (1, 2, 3))
    for node in (1, 2, 3):
        expected = f"yewukv-fixed-cluster-v1\nnode={node}\n" + members
        if (root / f"node-{node}/cluster.identity").read_text() != expected:
            raise ValueError("backup identity does not match the local cluster configuration")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["manifest", "verify", "verify-local"])
    parser.add_argument("directory", type=Path)
    options = parser.parse_args()
    {"manifest": manifest, "verify": verify, "verify-local": verify_local}[options.mode](
        options.directory.resolve())
    print(f"backup {options.mode}: OK")
