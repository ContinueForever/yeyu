#!/usr/bin/env python3
"""Retain bounded CAS histories across real leader failures and snapshot restart."""
import argparse
import concurrent.futures
import json
import math
import signal
import socket
import tempfile
import threading
import time
from pathlib import Path
import unittest

import test_cluster as harness
import test_rpc_network as wire
from revision_history import check
from test_cluster_snapshot import wait_for


class RevisionFaultTests(unittest.TestCase):
    def test_cas_history_across_kill_pause_and_partition(self):
        for kind in ["kill", "pause", "partition"]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory(
                prefix="yewukv-revision-"
            ) as directory:
                cluster = harness.Cluster(
                    directory,
                    extra_args=[
                        "--snapshot-encoder",
                        "background",
                        "--snapshot-policy",
                        "fixed",
                        "--snapshot-entries",
                        "8",
                        "--snapshot-bytes",
                        "32768",
                    ],
                )
                history, attempts = [], []
                lock = threading.Lock()
                key = b"cas-history"
                fault = dict(kind=kind)

                def call(node, operation, value=b"", expected=0, discard=False):
                    start = time.monotonic_ns()
                    body = wire.bytes_field(1, key)
                    field = {"put": 2, "get": 3, "del": 4, "cas": 9}[operation]
                    if operation == "put":
                        body += wire.bytes_field(2, value)
                    elif operation == "cas":
                        body += (
                            wire.varint(16)
                            + wire.varint(expected)
                            + wire.bytes_field(3, value)
                        )
                    try:
                        with socket.create_connection(
                            ("127.0.0.1", cluster.ports[node]), timeout=1.5
                        ) as connection:
                            connection.settimeout(1.5)
                            connection.sendall(
                                wire.frame(b"\x08\x01" + wire.bytes_field(field, body))
                            )
                            if discard:
                                raise ConnectionError(
                                    "intentionally discard response after send"
                                )
                            response = wire.receive_response(connection)
                    except (OSError, EOFError):
                        response = dict(code=6)
                    end = time.monotonic_ns()
                    code = response["code"]
                    op = dict(kind=operation, key=key.hex(), start=start, end=end)
                    if operation in ["cas", "put"]:
                        op["value"] = value.hex()
                    if operation == "cas":
                        op["expected_revision"] = expected
                    if code in [0, 1, 7]:
                        op.update(code=code, revision=response.get("revision", 0))
                        if operation == "get" or code == 7:
                            op["result"] = (
                                response.get("value", b"").hex()
                                if op["revision"]
                                else None
                            )
                    elif code == 6 and operation != "get":
                        op.update(code=6, optional=True, end=math.inf)
                    else:
                        op = None
                    with lock:
                        attempts.append(
                            dict(
                                kind=operation,
                                start=start,
                                end=end,
                                code=code,
                                discarded=discard,
                            )
                        )
                        if op:
                            history.append(op)
                    return response

                try:
                    leader = cluster.leader()
                    call(leader, "cas", b"lost-reply", discard=True)
                    initial = call(leader, "put", b"initial")
                    self.assertEqual(initial["code"], 0)
                    barrier = threading.Barrier(4)

                    def client(client_id):
                        expected = initial["revision"]
                        barrier.wait(timeout=5)
                        for step in range(3):
                            response = call(
                                leader,
                                "get" if step == 1 else "cas",
                                f"client-{client_id}-{step}".encode(),
                                expected,
                            )
                            if response["code"] in [0, 7]:
                                expected = response["revision"]
                            time.sleep(0.04)

                    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                        jobs = [pool.submit(client, n) for n in range(3)]
                        barrier.wait(timeout=5)
                        time.sleep(0.025)
                        if kind == "kill":
                            cluster.kill(leader)
                        elif kind == "pause":
                            cluster.processes[leader].send_signal(signal.SIGSTOP)
                        else:
                            cluster.blocked.add(leader)
                        fault["applied_ns"] = time.monotonic_ns()
                        try:
                            time.sleep(1.1)
                        finally:
                            if kind == "kill":
                                cluster.start(leader)
                            elif kind == "pause":
                                cluster.processes[leader].send_signal(signal.SIGCONT)
                            cluster.blocked.clear()
                            fault["healed_ns"] = time.monotonic_ns()
                        for job in jobs:
                            job.result(timeout=15)
                    leader = cluster.leader()
                    current = call(leader, "get")
                    self.assertEqual(current["code"], 0)
                    self.assertEqual(call(leader, "cas", b"stale", 0)["code"], 7)
                    self.assertEqual(call(leader, "del")["code"], 0)
                    created = call(leader, "cas", b"recreated", 0)
                    self.assertEqual(created["code"], 0)
                    self.assertEqual(
                        call(leader, "cas", b"ABA", current["revision"])["code"], 7
                    )
                    for i in range(32):
                        self.assertEqual(
                            cluster.call(
                                leader, "put", f"cold-{i}".encode(), b"c" * 1024
                            )["code"],
                            0,
                        )
                    wait_for(
                        cluster, lambda: cluster.status(leader)["snapshot_index"] > 8
                    )
                    for n in range(3):
                        cluster.kill(n)
                    for n in range(3):
                        cluster.start(n)
                    leader = cluster.eventually_value(key, b"recreated")
                    restored = call(leader, "get")
                    self.assertEqual(restored["revision"], created["revision"])
                    self.assertTrue(
                        any(
                            a["start"] < fault["healed_ns"]
                            and a["end"] > fault["applied_ns"]
                            for a in attempts
                        )
                    )
                    self.assertTrue(check(history))
                    fault["verified"] = True
                finally:
                    evidence = harness.NODE.parent.parent / "cas-history-evidence"
                    evidence.mkdir(exist_ok=True)
                    (evidence / (Path(directory).name + ".json")).write_text(
                        json.dumps(
                            dict(
                                fault=fault,
                                attempts=attempts,
                                operations=[
                                    dict(op, end=None) if op.get("optional") else op
                                    for op in history
                                ],
                            ),
                            indent=2,
                        )
                        + "\n"
                    )
                    for process in cluster.processes:
                        if process and process.poll() is None:
                            process.send_signal(signal.SIGCONT)
                    cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
