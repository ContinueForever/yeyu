#!/usr/bin/env python3
"""Verify a published paired disk comparison without executing archived code."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from async_disk_evidence import campaign
from cpp_load import read, require


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check_archive(path, expected):
    with tarfile.open(fileobj=io.BytesIO(path.read_bytes()), mode="r:gz") as archive:
        seen = set()
        for item in archive:
            require(
                item.isfile()
                and item.name.startswith("source/")
                and ".." not in Path(item.name).parts,
                "unsafe archived source",
            )
            name = item.name[len("source/") :]
            require(name not in seen and name in expected, "source inventory differs")
            seen.add(name)
            require(
                digest(archive.extractfile(item).read()) == expected[name],
                "archived source differs",
            )
        require(seen == set(expected), "missing archived source")


def verify(root):
    root = Path(root)
    manifest = read(root / "manifest.json")
    actual = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}
    require(
        actual == set(manifest["files"]) | {"manifest.json", "SHA256SUMS"},
        "publication inventory differs",
    )
    for name, wanted in manifest["files"].items():
        require(
            not Path(name).is_absolute()
            and ".." not in Path(name).parts
            and digest((root / name).read_bytes()) == wanted,
            "publication checksum differs",
        )
    exported = read(root / "control/export-map.json")
    exported_paths = set()
    for item in exported:
        name = f"nodes/node-{item['node']}/" + item["path"]
        require(
            name not in exported_paths and name in manifest["files"],
            "export inventory differs",
        )
        exported_paths.add(name)
        data = (root / name).read_bytes()
        require(
            digest(data) == item["public_sha256"] and len(data) == item["public_bytes"],
            "exported bytes differ",
        )
        if item["unchanged"]:
            require(
                item["original_sha256"] == item["public_sha256"]
                and item["original_bytes"] == item["public_bytes"],
                "raw byte identity differs",
            )
        if Path(name).name.startswith("client-") and name.endswith(".json.gz"):
            require(item["unchanged"], "numeric client samples were rewritten")
    require(
        exported_paths == {n for n in manifest["files"] if n.startswith("nodes/")},
        "unmapped exported artifact",
    )
    execution = read(root / "control/execution.json")
    require(
        manifest["core_commit"] == execution["core_commit"]
        and manifest["tools_commit"] == execution["commit"],
        "source identity differs",
    )
    require(
        execution["commit"] == "21f56012bf56e9afc9aa25132178e37f4ef63661"
        and execution["core_commit"] == "66bd2a92c25069279d3066b5e34d4e3368bb6e96"
        and execution["trace_recorder"] is False
        and execution["node_cpu_quota"] == execution["driver_cpu_quota"] == "100%"
        and execution["node_memory_max"] == execution["driver_memory_max"] == "512M",
        "executed version or resource limits differ",
    )
    build = read(root / "control/build-result.json")
    require(
        build["status"] == "complete"
        and build["binaries"] == execution["binaries"]
        and "-DCMAKE_BUILD_TYPE=RelWithDebInfo" in build["commands"][0]
        and "-DYEWUKV_BUILD_TESTS=OFF" in build["commands"][0],
        "cloud build identity differs",
    )
    source = root / "source/runtime-source.tar.gz"
    require(
        digest(source.read_bytes()) == execution["source_sha256"],
        "runtime source archive differs",
    )
    check_archive(source, read(root / "source/runtime-source-manifest.json"))
    check_archive(root / "source/executed-tools.tar.gz", execution["components"])
    environment = read(root / "control/node-environment.json")
    require(len(environment) == 3, "missing environment")
    for node, item in enumerate(environment, 1):
        require(
            item["node"] == node
            and item["binaries"]["yewukv-node"] == execution["binaries"]["yewukv-node"]
            and item["clock_ticks"] > 0
            and item["page_size"] > 0,
            "node binary/environment differs",
        )
    require(
        environment[2]["binaries"]["yewukv-bench"]
        == execution["binaries"]["yewukv-bench"],
        "client binary differs",
    )
    main = campaign(root / "nodes", environment)
    smoke = campaign(root / "nodes", environment, "async-smoke")
    require(
        manifest["performance_improved"] is False
        and manifest["default_executor"] == "sync"
        and all(
            p["percent_change"]["p99_ms"] > 0
            and p["percent_change"]["successful_ops_s"] < 0
            for p in main["pairs"]
        ),
        "published regression conclusion differs from paired measurements",
    )
    require(
        main == read(root / "summary.json")
        and smoke == read(root / "smoke-summary.json"),
        "derived summary differs",
    )
    protection = read(root / "control/original-protection.json")
    require(
        protection["verified"]
        and protection["checked_experiment_keys"] == 512
        and protection["protected_values"] == 4
        and protection["before"] == protection["after"],
        "original values changed",
    )
    services = read(root / "control/final-services.json")
    require(len(services) == 3, "missing service cleanup")
    for record in services:
        props = record["original"]
        require(
            props["ActiveState"] == "active"
            and props["NRestarts"] == "0"
            and not props["DropInPaths"]
            and record["original_binary_sha256"]
            == "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
            and not record["temporary_units"].strip(),
            "original service or temporary cleanup differs",
        )
        require(
            int(props["MainPID"]) == protection["before"]["pids"][record["node"] - 1],
            "original process changed",
        )
    return dict(
        verified=True,
        windows=main["windows"],
        measured_seconds=main["measured_seconds"],
        total=main["total"],
        full_restart_checks=main["windows"],
        smoke_restart_checks=smoke["windows"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory), indent=2))
