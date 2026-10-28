#!/usr/bin/env python3
"""Check this round's two v3 binaries via whole-cluster switches and zlib CRC.

Requires explicit prior/current binaries. This is not a rolling-upgrade test or
a promise that older v1/v2-only programs can open journal data.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
import test_cluster as harness
from test_cluster_snapshot import wait_for


def audit(directory):
    files, records = [], 0
    for node in sorted(directory.glob("node-*/")):
        state = node / "raft.state"
        data = state.read_bytes()
        if (data[:8] != b"YRFT\0\0\0\3" or
                int.from_bytes(data[-4:], "big") != zlib.crc32(data[:-4])):
            raise AssertionError("checkpoint CRC/version differs from independent zlib")
        files.append(dict(path=str(state.relative_to(directory)), bytes=len(data),
                          sha256=hashlib.sha256(data).hexdigest()))
        journals = list(node.glob("raft.wal.*"))
        if len(journals) != 1:
            raise AssertionError("expected one active generation journal")
        for journal in journals:
            data, offset = journal.read_bytes(), 0
            while offset < len(data):
                header = data[offset:offset + 32]
                if len(header) != 32 or header[:4] != b"YRJL":
                    raise AssertionError("incomplete or invalid journal header")
                size = int.from_bytes(header[12:16], "big")
                record = data[offset:offset + 32 + size + 4]
                if (len(record) != 32 + size + 4 or
                        int.from_bytes(header[28:32], "big") != zlib.crc32(header[:28]) or
                        int.from_bytes(record[-4:], "big") != zlib.crc32(record[:-4])):
                    raise AssertionError("journal CRC differs from independent zlib")
                records += 1
                offset += len(record)
            files.append(dict(path=str(journal.relative_to(directory)), bytes=len(data),
                              sha256=hashlib.sha256(data).hexdigest()))
    if len(files) != 6:
        raise AssertionError("missing node state/journal")
    return dict(files=files, journal_records=records, checked=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prior", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prior, current, output = args.prior.resolve(), args.current.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    runtime = output / "runtime"
    runtime.mkdir()
    harness.NODE = current
    cluster = harness.Cluster(runtime, extra_args=["--snapshot-entries", "8", "--snapshot-bytes", "65536"])
    expected, stages = {}, []
    result = dict(command=sys.argv, status="running", stages=stages,
                  prior_sha256=hashlib.sha256(prior.read_bytes()).hexdigest(),
                  current_sha256=hashlib.sha256(current.read_bytes()).hexdigest())
    try:
        for stage, binary in enumerate((current, prior, current, prior, current)):
            if stage:
                harness.NODE = binary
                for node in range(3):
                    cluster.start(node)
            leader = cluster.leader()
            for key, value in sorted(expected.items()):
                response = cluster.call(leader, "get", key)
                if response["code"] != 0 or response["value"] != value:
                    raise AssertionError(f"switched reader lost {key!r}")
            old_count = len(expected)
            for i in range(64 if stage == 0 else 24):
                key = f"key-{i}".encode()
                value = bytes([stage, i, 0, 255]).ljust(8192 if stage == 0 else 1024, b"v")
                if cluster.call(leader, "put", key, value)["code"] != 0:
                    raise AssertionError("compatibility write was not acknowledged")
                expected[key] = value
            deleted = f"key-{63 - stage}".encode()
            if cluster.call(leader, "del", deleted)["code"] != 0:
                raise AssertionError("delete was not acknowledged")
            expected.pop(deleted, None)
            wait_for(cluster, lambda: all(cluster.status(i)["applied_index"] ==
                     cluster.status(leader)["last_index"] for i in range(3)))
            states = [cluster.status(i) for i in range(3)]
            if not all(state["snapshot_index"] > 0 for state in states):
                raise AssertionError("no persisted KV snapshots")
            for node in range(3):
                cluster.kill(node)
            stages.append(dict(stage=stage, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                               recovered_values_checked=old_count, acknowledged_values=len(expected),
                               states=states, independent_crc=audit(runtime)))
        harness.NODE = current
        for node in range(3):
            cluster.start(node)
        leader = cluster.leader()
        for key, value in expected.items():
            response = cluster.call(leader, "get", key)
            if response["code"] != 0 or response["value"] != value:
                raise AssertionError("final recovery differs")
        for stage in range(5):
            if cluster.call(leader, "get", f"key-{63-stage}".encode())["code"] != 1:
                raise AssertionError("a deleted key reappeared")
        result.update(status="complete", final_values_checked=len(expected), deleted_keys_checked=5)
    except BaseException as error:
        result.update(status="failed", error=repr(error))
        raise
    finally:
        cluster.close()
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
