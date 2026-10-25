#!/usr/bin/env python3
"""Verify raw cloud trace joins, alternating cohorts, provenance and cleanup."""
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
from cpp_load import read, require
from persistence_evidence import campaign, capture_inventory
from persistence_timeline import validate_client_ranges

spec = importlib.util.spec_from_file_location(
    "current_soak_publication", ROOT / "scripts/verify-cloud-soak-publication.py"
)
archive_helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive_helpers)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sources(root):
    execution = read(root / "control/execution.json")
    require(
        execution["commit"] == "27baea4d528ee3d372b6b523996f36508c6ca16b",
        "runtime commit differs",
    )
    for name in ["runtime-source", "core-source", "executed-tools"]:
        archive_helpers.source(
            root / f"source/{name}.tar.gz", root / f"source/{name}-manifest.json"
        )
    require(
        digest((root / "source/runtime-source.tar.gz").read_bytes())
        == execution["source_sha256"],
        "runtime source hash differs",
    )
    require(
        read(root / "source/executed-tools-manifest.json") == execution["components"],
        "executed tools differ",
    )
    core = read(root / "source/core-source-manifest.json")
    runtime = read(root / "source/runtime-source-manifest.json")
    require(
        all(core.get(k) == v for k, v in runtime.items()),
        "runtime differs from tested core",
    )
    for kind in ["normal", "asan"]:
        import gzip

        require(
            "100% tests passed, 0 tests failed out of 154"
            in gzip.decompress(
                (root / f"tests/ctest-{kind}.log.gz").read_bytes()
            ).decode(),
            "regression evidence differs",
        )
    environment = read(root / "control/node-environment-after.json")
    require(
        [r["node"] for r in environment] == [1, 2, 3]
        and all(
            r["clock_ticks"] == 100
            and r["page_size"] == 4096
            and r["cpu_count"] == 2
            and r["binaries"]["yewukv-node"] == execution["binaries"]["yewukv-node"]
            for r in environment
        )
        and environment[2]["binaries"]["yewukv-bench"]
        == execution["binaries"]["yewukv-bench"],
        "environment/runtime identity differs",
    )
    return environment


def original_services(root):
    before = read(root / "control/original-data-before.json")
    after = read(root / "control/original-data-after.json")
    for record in [before, after]:
        require(
            record["verified"]
            and record["checked_experiment_keys"] == 512
            and len(record["protected"]) == 4
            and all(
                s["keys"] == 2852
                and s["last_index"] == s["commit_index"] == s["applied_index"] == 711089
                for s in record["states"]
            ),
            "original data differs",
        )
    require(
        before["protected"] == after["protected"], "original protected values changed"
    )
    services = read(root / "control/final-services.json")
    require([r["node"] for r in services] == [1, 2, 3], "service inventory differs")
    for record in services:
        require(
            record["original"]["ActiveState"] == "active"
            and record["original"]["NRestarts"] == "0"
            and not record["original"]["DropInPaths"]
            and not record["temporary_units"].strip()
            and record["original_binary_sha256"]
            == "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
            and all(v == [0, 1] for v in record["affinity"].values()),
            "service cleanup/protection differs",
        )


def unpack(path, destination):
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        require(
            len({m.name for m in members}) == len(members)
            and sum(m.size for m in members) < 1024**3,
            "archive inventory/budget differs",
        )
        for member in members:
            name = PurePosixPath(member.name)
            require(
                member.isfile()
                and not name.is_absolute()
                and ".." not in name.parts
                and member.name != "private-config.json"
                and (
                    name.parts[0] in ["trace-main", "trace-smoke", "traces"]
                    or (
                        len(name.parts) == 1
                        and (
                            member.name.endswith(".log")
                            or member.name
                            in ["execution.json", "export.json", "build-result.json"]
                        )
                    )
                ),
                "unexpected node export member",
            )
            p = destination / member.name
            p.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, p.open("xb") as output:
                shutil.copyfileobj(source, output)
    original = read(destination / "export.json")["files"]
    for name, wanted in original.items():
        if name.startswith(("trace-main/", "trace-smoke/", "traces/")) and not Path(
            name
        ).name.startswith("command-"):
            p = destination / name
            require(
                p.stat().st_size == wanted["bytes"]
                and digest(p.read_bytes()) == wanted["sha256"],
                "numeric evidence changed during publication: " + name,
            )


def verify(root):
    files = archive_helpers.inventory(root)
    environment = sources(root)
    original_services(root)
    with tempfile.TemporaryDirectory(prefix="yewukv-persist-evidence-") as temporary:
        nodes = Path(temporary)
        for node in [1, 2, 3]:
            destination = nodes / f"node-{node}"
            unpack(root / f"nodes/node-{node}.tar.gz", destination)
            require(
                read(destination / "execution.json")
                == read(root / "control/execution.json"),
                "deployed execution differs",
            )
        directories = [
            p.parent for p in (nodes / "node-3").glob("trace-*/*/window-*/result.json")
        ]
        require(
            validate_client_ranges(directories) == 512,
            "activity connection range inventory differs",
        )
        captures = capture_inventory(nodes)
        require(
            captures == read(root / "derived/captures.json"),
            "capture inventory differs",
        )
        main, timelines = campaign(nodes, environment, "trace-main")
        smoke, _ = campaign(nodes, environment, "trace-smoke")
        require(
            main == read(root / "derived/summary.json")
            and smoke == read(root / "derived/smoke-summary.json"),
            "derived campaign summary differs",
        )
        require(
            set(timelines)
            == {
                p.name.removesuffix(".json.gz")
                for p in (root / "derived/timelines").glob("*.json.gz")
            },
            "timeline inventory differs",
        )
        for name, wanted in timelines.items():
            require(
                read(root / f"derived/timelines/{name}.json") == wanted,
                "derived timeline differs: " + name,
            )
    return dict(
        verified=True,
        publication_files=files,
        core_ctest=154,
        asan_ctest=154,
        windows=main["windows"],
        measured_seconds=main["measured_seconds"],
        **main["total"],
        measured_traced_gets=sum(
            r["timeline"]["measured_gets"] for r in main["rows"] if r["enabled"]
        ),
        joined_traced_gets=sum(
            r["timeline"]["joined_gets"] for r in main["rows"] if r["enabled"]
        ),
        smoke_windows=smoke["windows"],
        identity_ranges=512,
        trace_lifetimes=len(captures),
        original_preserved=True,
        temporary_services_stopped=True,
        note=main["note"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.root.resolve())))
