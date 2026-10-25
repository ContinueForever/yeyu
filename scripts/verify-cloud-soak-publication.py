#!/usr/bin/env python3
"""Verify published six-hour samples, summaries, provenance and service protection."""

import argparse
import csv
import gzip
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
from soak_evidence import summarize_campaign

spec = importlib.util.spec_from_file_location(
    "current_cloud_soak_verifier", ROOT / "scripts/verify-cloud-soak.py"
)
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    expected = read(root / "publication-manifest.json")["files"]
    actual = {
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and p.name != "publication-manifest.json"
    }
    require(actual == set(expected), "publication inventory differs")
    for name, wanted in expected.items():
        p = root / name
        require(
            not p.is_symlink()
            and p.resolve().is_relative_to(root.resolve())
            and p.stat().st_size == wanted["bytes"]
            and digest(p) == wanted["sha256"],
            "publication file differs: " + name,
        )
    return len(expected)


def source(archive_path, manifest_path):
    manifest = read(manifest_path)
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        require(
            len(members) == len(manifest)
            and {m.name for m in members} == {"source/" + n for n in manifest},
            "source inventory differs",
        )
        for member in members:
            require(member.isfile(), "non-file in source archive")
            require(
                hashlib.sha256(archive.extractfile(member).read()).hexdigest()
                == manifest[member.name.removeprefix("source/")],
                "source content differs: " + member.name,
            )


def metadata(root):
    meta = read(root / "manifest.json")
    current = read(root / "control/execution.json")
    initial = read(root / "control/execution-initial.json")
    require(
        current["commit"] == "d34a605d0f50727c996fb9c287b40bbe4b8700d7"
        and meta["runtime_commit"] == current["commit"]
        and current["calendar_schema"] == 2,
        "executed version differs",
    )
    for version, execution in [("v2", current), ("initial", initial)]:
        archive = root / f"source/runtime-source-{version}.tar.gz"
        source(archive, root / f"source/runtime-source-{version}-manifest.json")
        require(digest(archive) == execution["source_sha256"], "runtime source hash")
        tools = root / f"source/executed-tools-{version}.tar.gz"
        tools_manifest = root / f"source/executed-tools-{version}-manifest.json"
        require(read(tools_manifest) == execution["components"], "tool inventory")
        source(tools, tools_manifest)
    env = read(root / "control/node-environment-after.json")
    require(
        [n["node"] for n in env] == [1, 2, 3]
        and all(
            n["cpu_count"] == 2
            and n["clock_ticks"] == 100
            and n["page_size"] == 4096
            and n["binaries"]["yewukv-node"] == current["binaries"]["yewukv-node"]
            for n in env
        )
        and env[2]["binaries"]["yewukv-bench"]
        == current["binaries"]["yewukv-bench"],
        "environment/runtime identity differs",
    )
    before = read(root / "control/original-data-before.json")
    after = read(root / "control/original-data-after.json")
    for record in [before, after]:
        require(
            record["verified"]
            and record["checked_experiment_keys"] == 512
            and len(record["protected"]) == 4
            and all(
                n["keys"] == 2852
                and n["last_index"] == n["commit_index"] == n["applied_index"] == 711089
                for n in record["states"]
            ),
            "original data not preserved",
        )
    require(before["protected"] == after["protected"], "protected values changed")
    services = read(root / "control/final-services.json")
    require([s["node"] for s in services] == [1, 2, 3], "service inventory")
    for s in services:
        require(
            s["original"]["ActiveState"] == "active"
            and s["original"]["NRestarts"] == "0"
            and not s["original"]["DropInPaths"]
            and s["original_binary_sha256"]
            == "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
            and not s["temporary_units"].strip()
            and all(v == [0, 1] for v in s["affinity"].values()),
            "service protection or cleanup differs",
        )
    require(
        meta["phase_archives"] == [f"results/phase-{i:02d}.tar.gz" for i in range(6)],
        "phase archive inventory differs",
    )
    return meta, env


def unpack(archive_path, destination, prefixes):
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        require(len({m.name for m in members}) == len(members), "duplicate member")
        require(sum(m.size for m in members) <= 1024**3, "archive budget exceeded")
        for member in members:
            name = PurePosixPath(member.name)
            require(
                member.isfile()
                and not name.is_absolute()
                and ".." not in name.parts
                and any(member.name.startswith(p + "/") for p in prefixes),
                "unsafe or unexpected result member",
            )
            p = destination / member.name
            p.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source_file, p.open("xb") as output:
                shutil.copyfileobj(source_file, output)


def verify_csv(root, summary):
    fields = [
        "phase", "window", "cold_mib", "encoder", "write_percent", "attempts",
        "successes", "failures", "offered_slots", "missed_slots", "puts", "gets",
        "successful_ops_s", "p99_ms", "p999_ms", "max_ms", "scheduled_p99_ms",
    ]
    windows = [
        {k: str(row[k]) for k in fields}
        for row in summary["rows"]
    ]
    profiles = []
    for phase in summary["phases"]:
        for profile in phase["profiles"]:
            record = dict(
                phase=phase["phase"],
                cold_mib=phase["cold_mib"],
                encoder=phase["encoder"],
                **profile,
            )
            profiles.append({k: str(v) for k, v in record.items()})
    for name, expected in [("windows", windows), ("profiles", profiles)]:
        with (root / f"{name}.csv").open(newline="") as f:
            require(list(csv.DictReader(f)) == expected, "derived CSV differs: " + name)


def verify(root):
    files = inventory(root)
    meta, env = metadata(root)
    with tempfile.TemporaryDirectory(prefix="yewukv-soak-evidence-") as temporary:
        extracted = Path(temporary)
        main = extracted / "soak-main"
        main.mkdir()
        shutil.copyfile(root / "results/progress.json", main / "progress.json")
        for i, name in enumerate(meta["phase_archives"]):
            unpack(root / name, extracted, [f"soak-main/phase-{i:02d}"])
        contents = read(root / "results/sample-content-manifest.json")
        require(
            set(contents) == {
                str(p.relative_to(extracted))
                for p in main.glob("phase-*/window-*/client-*.json.gz")
            },
            "sample content inventory differs",
        )
        for name, expected in contents.items():
            require(
                hashlib.sha256(gzip.decompress((extracted / name).read_bytes())).hexdigest()
                == expected,
                "sample JSON content differs: " + name,
            )
        result = campaign.verify(main)
        require(
            result == read(root / "control/long-verification.json")
            == read(root / "control/local-long-verification.json"),
            "recorded verification differs",
        )
        summary = summarize_campaign(main, env)
        require(summary == read(root / "derived/summary.json"), "derived data differs")
        verify_csv(root / "derived", summary)
        require(
            summary["total"]["attempts"] == result["attempts"]
            and summary["total"]["missed_slots"] == result["missed_slots"]
            and summary["total"]["offered_slots"] == 12960000
            and summary["total"]["failures"] == 0,
            "campaign accounting differs",
        )
        unpack(root / "results/smoke.tar.gz", extracted, ["soak-smoke", "soak-smoke-exact"])
        smoke = campaign.verify(extracted / "soak-smoke")
        exact = campaign.verify(extracted / "soak-smoke-exact")
        require(
            smoke["attempts"] == exact["attempts"] == 2400
            and smoke["offered_slots"] == 2402
            and smoke["missed_slots"] == 2
            and exact["offered_slots"] == 2400
            and exact["missed_slots"] == 0,
            "smoke/calendar recovery differs",
        )
        unpack(
            root / "nodes.tar.gz", extracted / "nodes", ["node-1", "node-2", "node-3"]
        )
        for n in [1, 2, 3]:
            for name in ["execution.json", "execution-v2.json"]:
                require(
                    read(extracted / "nodes" / f"node-{n}" / name)
                    == read(root / "control/execution.json"),
                    "deployed execution metadata differs",
                )
    return dict(
        publication_files=files,
        **result,
        smoke_initial=smoke,
        smoke_exact=exact,
        maximum_ms=summary["maximum_ms"],
        original_data_preserved=True,
        temporary_services_stopped=True,
        note=summary["scope"],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    args = p.parse_args()
    print(json.dumps(verify(args.root.resolve())))
