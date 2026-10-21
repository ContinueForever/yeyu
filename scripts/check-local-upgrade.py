#!/usr/bin/env python3
"""Offline whole-cluster v1->snapshot upgrade acceptance using a saved old binary."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))
import test_cluster as harness


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-node", type=Path, required=True)
    parser.add_argument("--new-node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    harness.NODE, harness.CLIENT = args.old_node.resolve(), args.client.resolve()
    cluster = harness.Cluster(args.output)
    result = dict(old_binary_sha256=hashlib.sha256(args.old_node.read_bytes()).hexdigest(),
                  new_binary_sha256=hashlib.sha256(args.new_node.read_bytes()).hexdigest(),
                  command=sys.argv, status="running")
    try:
        leader = cluster.leader()
        expected = {f"legacy-{i}".encode(): bytes([i]) * 256 for i in range(20)}
        for key, value in expected.items():
            assert cluster.call(leader, "put", key, value)["code"] == 0
        assert cluster.call(leader, "del", b"legacy-3")["code"] == 0
        expected.pop(b"legacy-3")
        # Stop every old node before changing executable/configuration.
        for i in range(3):
            cluster.kill(i)
        for i in range(3):
            header = (args.output / f"node-{i + 1}" / "raft.state").read_bytes()[:8]
            assert header == b"YRFT\0\0\0\1"
        harness.NODE = args.new_node.resolve()
        cluster.extra_args = ["--snapshot-entries", "8"]
        for i in range(3):
            cluster.start(i)
        leader = cluster.eventually_value(b"legacy-0", expected[b"legacy-0"])
        for key, value in expected.items():
            assert cluster.call(leader, "get", key)["value"] == value
        assert cluster.call(leader, "get", b"legacy-3")["code"] == 1
        assert cluster.call(leader, "put", b"new", b"snapshot-version")["code"] == 0
        for _ in range(16):
            assert cluster.call(leader, "get", b"new")["value"] == b"snapshot-version"
        deadline = time.monotonic() + 10
        while not all(cluster.status(i)["snapshot_index"] > 0 for i in range(3)):
            assert time.monotonic() < deadline
            time.sleep(0.05)
        for i in range(3):
            cluster.kill(i)
        for i in range(3):
            assert (args.output / f"node-{i + 1}" / "raft.state").read_bytes()[:8] == b"YRFT\0\0\0\2"
            cluster.start(i)
        leader = cluster.eventually_value(b"new", b"snapshot-version")
        for key, value in expected.items():
            assert cluster.call(leader, "get", key)["value"] == value
        assert cluster.call(leader, "get", b"legacy-3")["code"] == 1
        result.update(status="complete", preserved_legacy_keys=len(expected), deleted_key_stays_deleted=True,
                      v2_restart_checked=True, states=[cluster.status(i) for i in range(3)])
    except BaseException as error:
        result.update(status="failed", error=repr(error))
        raise
    finally:
        cluster.close()
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
