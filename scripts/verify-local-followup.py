#!/usr/bin/env python3
"""Recheck published read/checkpoint samples and cross-epoch durability histories.

Only retained evidence is checked; this does not rerun processes or hardware.
"""
import argparse
import gzip
import importlib.util
import json
import hashlib
import re
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))
import local_followup

spec = importlib.util.spec_from_file_location("prior_evidence", ROOT / "scripts/verify-local-optimization.py")
prior = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prior)


def verify_durability(directory):
    ring, checks, epochs = {}, 0, 0
    with gzip.open(directory / "epochs.jsonl.gz", "rt") as source:
        for line in source:
            record = json.loads(line)
            history, result = record["history"], record["result"]
            expected_fault = ("leader-kill", "follower-partition", "directed-partition", "link-delay",
                              "all-restart", "leader-pause")[epochs % 6]
            prior.require(len(history["fault"]) == 1 and history["fault"][0]["kind"] ==
                          result["fault"] == expected_fault, "fault schedule differs")
            fault = history["fault"][0]
            prior.require(fault["started_ns"] < fault["healed_ns"], "invalid fault interval")
            durability = history["durability"]
            expected = [dict(key=k.hex(), value=v.hex()) for k, v in sorted(ring.items())]
            observed = durability["verified_before_update"]
            prior.require([dict(key=v["key"], value=v["value"]) for v in observed] == expected,
                          "cross-epoch acknowledged values were not all checked")
            prior.require(result["durable_ring_values_checked"] == len(ring), "durability count differs")
            key = f"soak-ring-{epochs % 32}".encode()
            value = f"acknowledged-epoch-{epochs}".encode()
            acknowledged = durability["acknowledged_update"]
            prior.require(dict(key=acknowledged["key"], value=acknowledged["value"]) ==
                          dict(key=key.hex(), value=value.hex()),
                          "acknowledged ring update differs")
            previous = fault["healed_ns"]
            for observation in [*observed, acknowledged]:
                prior.require(observation["code"] == 0 and observation["node"] in (1, 2, 3) and
                              previous <= observation["start"] < observation["end"],
                              "invalid durable observation or order")
                previous = observation["end"]
            successful = [op["end"] for op in history["attempts"] if op["code"] in (0, 1) and
                          op["end"] >= fault["healed_ns"]]
            expected_recovery = (min(successful) - fault["healed_ns"]) / 1e9 if successful else None
            prior.require(result["observed_first_success_after_heal_seconds"] == expected_recovery,
                          "post-heal observation differs")
            checks += len(ring)
            ring[key] = value
            epochs += 1
    summary = json.loads((directory / "summary.json").read_text())
    recovery = summary["final_recovery"]
    prior.require(recovery["all_node_sigkill"] and recovery["anchor_checked"], "final restart was not checked")
    prior.require(recovery["durable_ring_values"] == [dict(key=k.hex(), value=v.hex())
                  for k, v in sorted(ring.items())], "final recovered ring differs")
    prior.require(epochs == summary["epochs"], "durability epoch count differs")
    prior.require(summary["durable_ring_capacity"] == 32, "ring capacity differs")
    prior.require(summary["elapsed_seconds"] >= summary["requested_duration_seconds"], "campaign stopped early")
    if summary["requested_duration_seconds"] == 0:
        prior.require(epochs == summary["requested_epochs"], "epoch campaign stopped early")
    return checks + len(ring)


def verify_build_inputs(root):
    identity = json.loads((root / "acceptance/identity.json").read_text())
    baseline = json.loads((root / "baseline/identity.json").read_text())
    current = json.loads((root / "acceptance/source-manifest.json").read_text())

    def build_inputs(manifest):
        return {name: digest for name, digest in manifest.items()
                if name == "CMakeLists.txt" or
                (name.startswith(("app/", "src/", "proto/")) and
                 (name.endswith((".cc", ".h", ".proto")) or name.endswith("CMakeLists.txt")))}

    for suite in ("reads", "snapshots", "faults"):
        source = json.loads((root / suite / "source-manifest.json").read_text())
        prior.require(build_inputs(source) == build_inputs(current), f"{suite} build inputs differ")
        manifest = json.loads((root / suite / "manifest.json").read_text())
        prior.require(hashlib.sha256((root / suite / "source-manifest.json").read_bytes()).hexdigest() ==
                      manifest["source_manifest_sha256"], "experiment source manifest identity differs")
        prior.require(manifest["environment"]["server_sha256"] == identity["normal_node_sha256"],
                      f"{suite} current binary differs")
        for name, variant in manifest.get("variants", {}).items():
            expected = baseline["server_sha256"] if name.startswith("baseline-") else identity["normal_node_sha256"]
            prior.require(variant["server_sha256"] == expected, "variant binary identity differs")
    compatibility = json.loads((root / "acceptance/compatibility/result.json").read_text())
    prior.require(compatibility["status"] == "complete" and len(compatibility["stages"]) == 5 and
                  compatibility["current_sha256"] == identity["normal_node_sha256"] and
                  compatibility["prior_sha256"] == baseline["server_sha256"], "compatibility check differs")
    prior.require(compatibility["final_values_checked"] == 59 and
                  compatibility["deleted_keys_checked"] == 5, "compatibility recovery coverage differs")
    for number, stage in enumerate(compatibility["stages"]):
        expected_binary = identity["normal_node_sha256"] if number % 2 == 0 else baseline["server_sha256"]
        prior.require(stage["stage"] == number and stage["binary_sha256"] == expected_binary and
                      stage["acknowledged_values"] == 63 - number, "compatibility sequence differs")
        prior.require(stage["independent_crc"]["checked"] and
                      len(stage["independent_crc"]["files"]) == 6, "independent CRC audit missing")
    for name, expected in (("normal", identity["normal_node_sha256"]),
                           ("asan", identity["asan_node_sha256"]),
                           ("disk", identity["normal_node_sha256"])):
        for evidence in (root / "acceptance" / f"{name}-evidence").glob("*.json"):
            record = json.loads(evidence.read_text())
            prior.require(record["checked"] and record["binary_sha256"] == expected,
                          "regression evidence belongs to another binary")
    audit = json.loads((root / "acceptance/sanitizer-log-audit.json").read_text())
    prior.require(audit["checked"] and not audit["diagnostics"], "sanitizer log audit failed")
    pattern = re.compile(r"ERROR: (?:AddressSanitizer|LeakSanitizer)|SUMMARY: "
                         r"(?:AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)|runtime error:")
    for record in audit["files"]:
        path = root / "acceptance" / record["path"]
        content = path.read_bytes()
        prior.require(hashlib.sha256(content).hexdigest() == record["sha256"] and
                      not pattern.search(content.decode(errors="replace")), "sanitizer node diagnostic found")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    root = parser.parse_args().directory.resolve()
    count = prior.verify_files(root)
    verify_build_inputs(root)
    for name in ("reads", "snapshots"):
        local_followup.verify(root / name)
    epochs, checked = prior.verify_faults(root / "faults")
    durable = verify_durability(root / "faults")
    print(json.dumps(dict(verified=True, published_files=count, fault_epochs=epochs,
                          model_checked_operations=checked, cross_epoch_durability_checks=durable)))


if __name__ == "__main__":
    main()
