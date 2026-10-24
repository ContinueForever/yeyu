#!/usr/bin/env python3
"""Check retained three-ECS records without starting servers or rerunning faults.

The initial PONG/PUT/GET sequence is screenshot-only. This verifier checks the
five retained fault/recovery scenarios, CTest records and provenance inventory;
it does not authenticate the remote machines or prove continuous availability.
"""

import argparse
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import sys
import tarfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def verify_inventory(root):
    manifest = load(root / "publication-manifest.json")
    expected = manifest["files"]
    require(not any(p.is_symlink() for p in root.rglob("*")), "symlink in evidence")
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*")
              if p.is_file() and p.name != "publication-manifest.json"}
    require(actual == set(expected), "file inventory differs")
    for name, record in expected.items():
        path = root / name
        require(path.resolve().is_relative_to(root), f"unsafe inventory path: {name}")
        content = path.read_bytes()
        require(len(content) == record["bytes"], f"size differs: {name}")
        require(hashlib.sha256(content).hexdigest() == record["sha256"], f"hash differs: {name}")
    source = load(root / "source-manifest.json")
    with tarfile.open(root / "source.tar.gz", "r:gz") as archive:
        require(archive.pax_headers.get("comment") == manifest["tested_commit"],
                "source archive commit marker differs")
        files = [m for m in archive.getmembers() if m.isfile()]
        require(all(m.isfile() or m.isdir() for m in archive.getmembers()), "unsafe source member")
        require(len(files) == len(source) and {m.name for m in files} == set(source),
                "source inventory differs")
        for member in files:
            path = Path(member.name)
            require(not path.is_absolute() and ".." not in path.parts, "unsafe source path")
            content = archive.extractfile(member).read()
            require(hashlib.sha256(content).hexdigest() == source[member.name],
                    f"source hash differs: {member.name}")
    return manifest


def statuses(text):
    return [json.loads(line[line.index('{"id":'):]) for line in text.splitlines()
            if '{"id":' in line]


def check_status(state, node_id, leader, term, index, keys):
    expected = dict(id=node_id, role="leader" if node_id == leader else "follower",
                    leader_id=leader, term=term, last_index=index,
                    commit_index=index, applied_index=index, keys=keys, pending=0)
    require(all(state.get(k) == v for k, v in expected.items()),
            f"unexpected node {node_id} state: {expected}")


def contains(text, sequence, label):
    require(sequence in text, f"missing {label} output")


def service_times(text, action, node_id):
    pattern = rf"^Oct 23 (\d\d:\d\d:\d\d) .*systemd\[1\]: {action} yewukv-node@{node_id}\.service"
    return [datetime.fromisoformat(f"2026-10-23T{t}+08:00")
            for t in re.findall(pattern, text, re.M)]


def verify(root):
    manifest = verify_inventory(root)
    require(manifest["tested_commit"] == "909df769ae2d9b0579a31f9d89745e589da86819",
            "unexpected tested commit")
    require({x["node_id"] for x in manifest["original_archives"]} == {1, 2, 3},
            "original archive provenance missing")
    test_sets, hashes, members, services = [], [], [], {}
    for node_id in (1, 2, 3):
        node = root / "nodes" / f"node-{node_id}"
        environment = (node / "environment.txt").read_text()
        require(re.findall(r"^[a-f0-9]{40}$", environment, re.M) == [manifest["tested_commit"]],
                f"source commit differs: node {node_id}")
        binaries = dict((name, digest) for digest, name in re.findall(
            r"^([a-f0-9]{64})\s+/usr/local/bin/(yewukv-(?:node|client))$", environment, re.M))
        require(set(binaries) == {"yewukv-node", "yewukv-client"}, "binary hashes missing")
        require(re.search(re.escape(manifest["tested_commit"]) + r"\n[a-f0-9]{64}\s", environment),
                f"unexpected git status output: node {node_id}")
        hashes.append(binaries)
        require('VERSION_ID="24.04"' in environment and "x86_64" in environment,
                f"unexpected OS: node {node_id}")
        for setting in ("User=yewukv", "Restart=on-failure", "WantedBy=multi-user.target"):
            require(setting in environment, f"missing service setting: {setting}")
        config = dict(line.split("=", 1) for line in (node / "node.env").read_text().splitlines())
        require(config["YEWUKV_LISTEN_HOST"] == f"10.0.0.{10 + node_id}" and
                config["YEWUKV_PORT"] == "9101", f"node address binding differs: {node_id}")
        members.append(config["YEWUKV_MEMBERS"])
        tests = gzip.decompress((node / "ctest.log.gz").read_bytes()).decode()
        starts = list(re.finditer(r"^(\d+)/126 Testing: (.+)$", tests, re.M))
        require(len(starts) == 126 and {int(m[1]) for m in starts} == set(range(1, 127)),
                f"CTest inventory differs: node {node_id}")
        test_sets.append({(int(m[1]), m[2]) for m in starts})
        for i, start in enumerate(starts):
            block = tests[start.start():starts[i + 1].start() if i + 1 < len(starts) else len(tests)]
            require(re.findall(r"^Test (Passed|Failed)\.$", block, re.M) == ["Passed"],
                    f"CTest failed or incomplete: node {node_id}, {start[2]}")
        require(len({m[2] for m in starts}) == 126, "duplicate CTest name")
        check_status(load(node / "status.json"), node_id, 2, 96, 11, 4)
        services[node_id] = (node / "service.log").read_text()
    require(test_sets[0] == test_sets[1] == test_sets[2], "CTest sets differ")
    require(hashes[0] == hashes[1] == hashes[2] == manifest["binary_sha256"], "binary hashes differ")
    expected_members = ",".join(f"{n}@10.0.0.{10+n}:9101" for n in (1, 2, 3))
    require(members == [expected_members] * 3, "members differ")

    def record(node_id, filename):
        return (root / "nodes" / f"node-{node_id}" / filename).read_text()

    first = record(1, "yewukv-failover-20261023T130640Z.log")
    contains(first, "\nready\nOK\nsurvived\n", "first failover")
    first_states = statuses(first)
    require(len(first_states) == 3 and {s["id"] for s in first_states} == {1, 2, 3},
            "first failover states missing")
    for state in first_states:
        check_status(state, state["id"], 3, 81, 5, 2)
    second = record(3, "yewukv-second-failover-20261023T130953Z.log")
    contains(second, "\nsurvived\nOK\ncaught-up\n", "second failover")
    second_states = statuses(second)
    require(len(second_states) == 3 and {s["id"] for s in second_states} == {1, 2, 3},
            "second failover states missing")
    for state in second_states:
        check_status(state, state["id"], 1, 82, 7, 3)
    contains(services[3], "event=role role=leader term=81", "first new leader service")
    contains(services[1], "event=role role=leader term=82", "second new leader service")

    reboot = record(1, "yewukv-reboot-20261023T131528Z.log")
    contains(reboot, "第三台暂时不可达", "reboot observation")
    contains(reboot, "\ncaught-up\nOK\nalive\n", "reboot read/write")
    node3 = root / "nodes" / "node-3"
    old_boot = (node3 / "yewukv-before-reboot.boot-id").read_text().strip()
    new_boot = (node3 / "current-boot-id.txt").read_text().strip()
    require(old_boot != new_boot and re.fullmatch(r"[a-f0-9-]{36}", new_boot), "boot ID unchanged")
    contains(services[3], f"-- Boot {new_boot.replace('-', '')} --", "reboot journal marker")
    require("Started yewukv-node@3.service" in services[3].split(f"-- Boot {new_boot.replace('-', '')} --", 1)[1],
            "no service startup in new boot")

    minority = record(1, "yewukv-minority-20261023T132652Z.log")
    before, after = statuses(minority)
    check_status(before, 1, 1, 82, 8, 4)
    require(after["last_index"] == 9 and after["quorum_timeouts"] == 2 and
            before["quorum_timeouts"] == 0, "minority append/timeout record differs")
    for field in ("id", "role", "term", "leader_id", "commit_index", "applied_index", "keys", "pending"):
        require(before[field] == after[field], f"minority advanced {field}")
    contains(minority, "read quorum unavailable or leadership changed", "minority read refusal")
    contains(minority, "leadership lost or quorum timeout; operation outcome is unknown", "unknown write")

    all_stopped = record(1, "yewukv-all-stopped-20261023T133735Z.log")
    observed = datetime(2026, 10, 23, 13, 37, 35, tzinfo=timezone.utc)
    latest_stops, earliest_starts = [], []
    for node_id in (1, 2, 3):
        contains(all_stopped, f"10.0.0.{10+node_id} 已不可达", "all-stopped node")
        stop_files = list((root / "nodes" / f"node-{node_id}").glob(f"yewukv-full-stop-node{node_id}-*.log"))
        require(len(stop_files) == 1 and "\ninactive\n" in stop_files[0].read_text(), "stop record missing")
        stopped = [t for t in service_times(services[node_id], "Stopped", node_id) if t < observed]
        started = [t for t in service_times(services[node_id], "Started", node_id) if t > observed]
        require(stopped and started, "stop/start journal events missing")
        latest_stops.append(max(stopped))
        earliest_starts.append(min(started))
    require(max(latest_stops) < observed < min(earliest_starts), "no common stopped interval")
    require(min(earliest_starts) - max(latest_stops) < timedelta(minutes=5), "unexpected stop interval")
    recovery = record(1, "yewukv-full-recovery-20261023T133845Z.log")
    expected_values = dict(zip(("cloud-check", "failover-check", "rejoin-check", "reboot-check"),
                               ("ready", "survived", "caught-up", "alive")))
    for key, value in expected_values.items():
        contains(recovery, f"检查：{key}\n{value}\n", f"recovered {key}")
    final_states = statuses(recovery)
    require(len(final_states) == 3 and {s["id"] for s in final_states} == {1, 2, 3}, "recovery states missing")
    for state in final_states:
        check_status(state, state["id"], 2, 96, 11, 4)
    return dict(verified=True, files=len(manifest["files"]), nodes=3, ctest_passed_per_node=126,
                retained_fault_scenarios=5, initial_sequence="screenshot-only", final_term=96,
                final_leader=2, final_applied_index=11, confirmed_values=expected_values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    root = parser.parse_args().directory.resolve()
    try:
        print(json.dumps(verify(root), ensure_ascii=False))
    except (ValueError, KeyError, OSError, tarfile.TarError) as error:
        print(f"verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
