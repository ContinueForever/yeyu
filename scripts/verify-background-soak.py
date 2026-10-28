#!/usr/bin/env python3
"""Independently verify the optional background path's frozen six-hour evidence."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
from background_soak_evidence import verify_raw
from cpp_load import read, require
from peer_disk_evidence import CORE, NODE_SHA256

spec = importlib.util.spec_from_file_location(
    "peer_publication_verifier", ROOT / "scripts/verify-peer-disk.py"
)
peer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(peer)
EXECUTION_SHA256 = "32a71c49a19fb7ed3f3ea1c22db6d239ff55317437ec1dfd40db2cc91393a6e2"
TOOLS_SHA256 = "9b376be6b66e1a1475b06140621b926b1c645b8117828ef80a16f2d8126dbd2b"
SOURCE_SHA256 = "f6d50aa012f00b0d17bc94b856fa277f9a125b8e95e00d7eb0076a0582f810a5"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    manifest = read(root / "manifest.json")
    actual = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}
    require(
        actual == set(manifest["files"]) | {"manifest.json", "SHA256SUMS"},
        "publication inventory differs",
    )
    for name, wanted in manifest["files"].items():
        p = root / name
        require(
            not p.is_symlink()
            and not Path(name).is_absolute()
            and p.resolve().is_relative_to(root.resolve())
            and p.stat().st_size == wanted["bytes"]
            and digest(p) == wanted["sha256"],
            "publication checksum differs: " + name,
        )
    require(
        manifest["core_commit"] == CORE
        and manifest["windows"] == 360
        and manifest["measured_seconds"] == 21600
        and manifest["disk_executor"] == "background"
        and manifest["default_executor"] == "sync"
        and manifest["performance_gate"] is False
        and len(manifest["phase_archives"]) == 6,
        "published acceptance scope differs",
    )
    return manifest


def metadata(root):
    require(
        digest(root / "control/execution.json") == EXECUTION_SHA256,
        "frozen execution differs",
    )
    execution = read(root / "control/execution.json")
    require(
        execution["core_commit"] == execution["commit"] == CORE,
        "frozen version differs",
    )
    source = root / "source/runtime-source.tar.gz"
    tools = root / "source/executed-tools.tar.gz"
    require(
        digest(source) == SOURCE_SHA256 and digest(tools) == TOOLS_SHA256,
        "frozen source/tool archive differs",
    )
    peer.check_archive(source, read(root / "source/runtime-source-manifest.json"))
    peer.check_archive(tools, execution["components"])
    build = read(root / "control/build-result.json")
    require(
        build["status"] == "complete" and build["binaries"] == execution["binaries"],
        "build identity differs",
    )
    ci = read(root / "control/ci.json")
    require(
        ci["core_commit"] == CORE
        and ci["validation_commit"] == "4193bb012f152e6742e5a4b2396fd9b3f9dc4d10"
        and len(ci["runs"]) == 2
        and {r["event"] for r in ci["runs"]} == {"push", "pull_request"}
        and all(
            r["conclusion"] == "success" and r["head"] == ci["validation_commit"]
            for r in ci["runs"]
        ),
        "prelaunch validation differs",
    )
    scope = read(root / "control/long-acceptance-scope.json")
    require(
        scope["performance_gate"] is False
        and scope["default_executor"] == "sync"
        and scope["disk_executor"] == "background"
        and scope["initial_decision_sha256"]
        == digest(root / "control/initial-long-decision.json"),
        "prelaunch scope amendment differs",
    )
    environment = read(root / "control/node-environment.json")
    require(
        [e["node"] for e in environment] == [1, 2, 3], "environment inventory differs"
    )
    require(
        all(
            e["clock_ticks"] > 0
            and e["page_size"] > 0
            and e["binaries"]["yewukv-node"] == NODE_SHA256
            for e in environment
        )
        and environment[2]["binaries"]["yewukv-bench"]
        == execution["binaries"]["yewukv-bench"],
        "observed runtime binary differs",
    )
    protection = read(root / "control/original-protection.json")
    require(
        protection["verified"] is True
        and protection["checked_experiment_keys"] == 512
        and protection["protected_values"] == 4
        and protection["before"] == protection["after"]
        and protection["before"]["keys"] == [2852] * 3
        and protection["before"]["applied_indexes"] == [711089] * 3,
        "original protected data differs",
    )
    services = read(root / "control/final-services.json")
    require([s["node"] for s in services] == [1, 2, 3], "service inventory differs")
    for item in services:
        original = item["original"]
        require(
            original["ActiveState"] == "active"
            and original["NRestarts"] == "0"
            and not original["DropInPaths"]
            and int(original["MainPID"])
            == protection["before"]["pids"][item["node"] - 1]
            and item["original_binary_sha256"]
            == "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
            and not item["temporary_units"].strip()
            and all(v == [0, 1] for v in item["affinity"].values()),
            "original service or final cleanup differs",
        )
    return environment


def unpack_parts(root, parts, destination, prefixes):
    require(
        parts and len(set(parts)) == len(parts), "missing or duplicate archive part"
    )
    # Parts split one gzip stream into bounded Git blobs; join only data, never run it.
    with tempfile.TemporaryFile() as joined:
        size = 0
        for name in parts:
            p = root / name
            require(
                p.is_file() and p.resolve().is_relative_to(root.resolve()),
                "invalid archive part",
            )
            size += p.stat().st_size
            require(size <= 512 * 1024**2, "compressed archive budget exceeded")
            with p.open("rb") as f:
                shutil.copyfileobj(f, joined)
        joined.seek(0)
        with tarfile.open(fileobj=joined, mode="r:gz") as archive:
            seen, total = set(), 0
            for member in archive:
                name = PurePosixPath(member.name)
                total += member.size
                require(
                    member.isfile()
                    and not name.is_absolute()
                    and ".." not in name.parts
                    and member.name not in seen
                    and any(member.name.startswith(p + "/") for p in prefixes)
                    and 0 <= member.size <= 64 * 1024**2
                    and total <= 1024**3,
                    "unsafe or unexpected result member",
                )
                seen.add(member.name)
                path = destination / member.name
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as f, path.open("xb") as output:
                    shutil.copyfileobj(f, output)


def exported(root, extracted):
    mapping = read(root / "control/export-map.json")
    expected = set()
    for item in mapping:
        name = f"nodes/node-{item['node']}/" + item["path"]
        p = extracted / name
        require(name not in expected and p.is_file(), "export inventory differs")
        expected.add(name)
        require(
            p.stat().st_size == item["public_bytes"]
            and digest(p) == item["public_sha256"],
            "export bytes differ",
        )
        if item["unchanged"]:
            require(
                item["original_bytes"] == item["public_bytes"]
                and item["original_sha256"] == item["public_sha256"],
                "raw identity differs",
            )
        if p.name.startswith("client-") and p.name.endswith(".json.gz"):
            require(item["unchanged"] is True, "raw client gzip was rewritten")
    actual = {
        str(p.relative_to(extracted))
        for p in (extracted / "nodes").rglob("*")
        if p.is_file()
    }
    require(actual == expected, "unmapped export artifact")


def verify(root):
    root = Path(root).resolve()
    manifest = inventory(root)
    environment = metadata(root)
    with tempfile.TemporaryDirectory(prefix="yewukv-background-evidence-") as temporary:
        extracted = Path(temporary)
        unpack_parts(root, manifest["other_archive"], extracted, ["nodes"])
        for index, parts in enumerate(manifest["phase_archives"]):
            unpack_parts(
                root, parts, extracted, [f"nodes/node-3/soak-main/phase-{index:02d}"]
            )
        exported(root, extracted)
        for node in (1, 2, 3):
            require(
                read(extracted / f"nodes/node-{node}/execution.json")
                == read(root / "control/execution.json"),
                "deployed execution differs",
            )
        main, summary = verify_raw(extracted / "nodes/node-3/soak-main", environment)
        smoke, _ = verify_raw(extracted / "nodes/node-3/soak-smoke", environment)
        require(
            main == read(root / "derived/verification.json"),
            "recorded independent verification differs",
        )
        require(
            smoke == read(root / "derived/smoke-verification.json"),
            "recorded preflight verification differs",
        )
        require(
            summary == read(root / "derived/summary.json"), "derived summary differs"
        )
    return dict(
        **main,
        maximum_ms=summary["maximum_ms"],
        wall_seconds=summary["wall_seconds"],
        original_data_preserved=True,
        temporary_services_stopped=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory), indent=2))
