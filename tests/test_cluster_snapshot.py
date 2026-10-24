#!/usr/bin/env python3
"""Snapshot capacity/recovery and process-level resource failure acceptance."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import time
import unittest

import test_cluster as harness
import test_rpc_network as wire

FAULT_LIBRARY = None
ASAN_RUNTIME = None


def wait_for(cluster, condition, seconds=15):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = condition()
            if result:
                return result
        except (OSError, EOFError, RuntimeError):
            pass
        time.sleep(0.05)
    raise AssertionError("condition did not converge\n" + cluster.diagnostics())


def evidence(name, value):
    output = harness.NODE.parent.parent / "cluster-evidence"
    output.mkdir(exist_ok=True)
    value = {**value, "temporary_directory_root": tempfile.gettempdir(),
             "binary_sha256": hashlib.sha256(harness.NODE.read_bytes()).hexdigest()}
    (output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")


class SnapshotTests(unittest.TestCase):
    def test_adaptive_cadence_keeps_byte_trigger_and_recovers_cold_and_hot_values(self):
        for policy in ("fixed", "adaptive"):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory(prefix="yewukv-snapshot-policy-") as directory:
                cluster = harness.Cluster(directory, extra_args=["--snapshot-entries", "8",
                    "--snapshot-bytes", "65536", "--snapshot-policy", policy])
                try:
                    leader = cluster.leader()
                    cold = b"c" * 8192
                    for i in range(32):
                        self.assertEqual(cluster.call(leader, "put", f"cold-{i}".encode(), cold)["code"], 0)
                    wait_for(cluster, lambda: all(cluster.status(i)["applied_index"] ==
                             cluster.status(leader)["last_index"] for i in range(3)))
                    before = cluster.status(leader)
                    for i in range(24):
                        self.assertEqual(cluster.call(leader, "put", b"hot", b"small")["code"], 0)
                    middle = cluster.status(leader)
                    self.assertEqual(middle["snapshot_policy"], policy)
                    if policy == "adaptive":
                        self.assertEqual(middle["snapshots_saved"], before["snapshots_saved"])
                    else:
                        self.assertGreater(middle["snapshots_saved"], before["snapshots_saved"])
                    # The byte trigger still compacts even while entry cadence
                    # is amortized against the larger live image.
                    for i in range(96):
                        value = str(i).encode().ljust(1024, b"h")
                        self.assertEqual(cluster.call(leader, "put", b"hot", value)["code"], 0)
                    after = cluster.status(leader)
                    self.assertGreater(after["snapshot_index"], middle["snapshot_index"])
                    self.assertLess(after["retained_log_bytes"], 65536)
                    for i in range(3):
                        cluster.kill(i)
                    for i in range(3):
                        cluster.start(i)
                    leader = cluster.eventually_value(b"hot", value)
                    for i in range(32):
                        self.assertEqual(cluster.call(leader, "get", f"cold-{i}".encode())["value"], cold)
                    evidence(f"snapshot-policy-{policy}", dict(before=before, middle=middle,
                             after=after, checked_cold_keys=32, all_node_sigkill=True, checked=True))
                finally:
                    cluster.close()

    def test_cumulative_payload_exceeds_old_limit_then_all_nodes_restart(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-snapshot-capacity-") as directory:
            cluster = harness.Cluster(directory)
            try:
                leader = cluster.leader()
                count, value_size, maximum = 9000, 8192, 0
                value = b""
                started = time.monotonic()
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 10) as connection:
                    connection.settimeout(30)
                    for first in range(0, count, 32):
                        value = str(first).encode().ljust(value_size, b"v")
                        last = min(first + 32, count)
                        connection.sendall(b"".join(wire.request(i + 1, "put", b"live", value)
                                                    for i in range(first, last)))
                        for i in range(first, last):
                            response = wire.receive_response(connection)
                            self.assertEqual((response["id"], response["code"]), (i + 1, 0))
                        maximum = max(maximum, cluster.status(leader)["state_bytes"])
                states = [cluster.status(i) for i in range(3)]
                self.assertGreater(count * value_size, 64 * 1024 * 1024)
                self.assertGreater(states[leader]["snapshot_index"], 0)
                self.assertLess(maximum, 3 * 1024 * 1024)
                for i in range(3):
                    cluster.kill(i)
                for i in range(3):
                    cluster.start(i)
                cluster.eventually_value(b"live", value)
                evidence("snapshot-capacity", dict(acknowledged_puts=count,
                         cumulative_value_bytes=count * value_size, max_observed_state_bytes=maximum,
                         elapsed_seconds=time.monotonic() - started, states_before_restart=states,
                         recovered_value_sha256=hashlib.sha256(value).hexdigest(), checked=True))
            finally:
                cluster.close()

    def test_offline_follower_restarts_mid_transfer_then_recovers_deletes_and_updates(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-snapshot-transfer-") as directory:
            cluster = harness.Cluster(directory, extra_args=["--snapshot-entries", "8"])
            try:
                leader = cluster.leader()
                lagging = (leader + 1) % 3
                cluster.kill(lagging)
                for i in range(40):
                    self.assertEqual(cluster.call(leader, "put", f"k{i}".encode(), b"x" * 4096)["code"], 0)
                self.assertEqual(cluster.call(leader, "del", b"k0")["code"], 0)
                self.assertEqual(cluster.call(leader, "put", b"k1", b"updated")["code"], 0)
                for i in range(16):
                    self.assertEqual(cluster.call(leader, "put", b"marker", b"last")["code"], 0)
                covered = cluster.status(leader)["snapshot_index"]
                self.assertGreater(cluster.status(leader)["snapshot_bytes"], 65536)
                for i in range(3):
                    if i != lagging:
                        cluster.delays[i, lagging] = 0.04
                cluster.start(lagging)
                time.sleep(0.25)
                cluster.kill(lagging)
                cluster.start(lagging)
                wait_for(cluster, lambda: cluster.status(lagging)["snapshots_installed"] > 0 and
                         cluster.status(lagging)["snapshot_index"] >= covered)
                cluster.delays.clear()
                cluster.kill(leader)
                new_leader = cluster.eventually_value(b"marker", b"last")
                self.assertEqual(cluster.call(new_leader, "get", b"k0")["code"], 1)
                self.assertEqual(cluster.call(new_leader, "get", b"k1")["value"], b"updated")
                self.assertEqual(cluster.call(new_leader, "get", b"k39")["value"], b"x" * 4096)
                evidence("snapshot-transfer", dict(covered_index=covered,
                         lagging_node=lagging + 1, state=cluster.status(lagging), checked=True))
            finally:
                cluster.close()


class ResourceTests(unittest.TestCase):
    def test_slow_reader_is_disconnected_while_other_clients_keep_working(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-slow-reader-") as directory:
            cluster = harness.Cluster(directory)
            try:
                leader = cluster.leader()
                self.assertEqual(cluster.call(leader, "put", b"large", b"v" * 8192)["code"], 0)
                with socket.socket() as connection:
                    connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                    connection.settimeout(5)
                    connection.connect(("127.0.0.1", cluster.ports[leader]))
                    connection.sendall(b"".join(wire.request(i + 1, "get", b"large") for i in range(3000)))
                    time.sleep(1)
                    received = 0
                    try:
                        while True:
                            part = connection.recv(65536)
                            if not part:
                                break
                            received += len(part)
                    except ConnectionResetError:
                        pass
                self.assertEqual(cluster.call(cluster.leader(), "get", b"large")["value"], b"v" * 8192)
                evidence("resource-slow-reader", dict(bytes_received_before_close=received,
                         other_client_read_succeeded=True, checked=True))
            finally:
                cluster.close()

    def test_enospc_and_sync_error_stop_leader_without_false_success(self):
        self.assertTrue(FAULT_LIBRARY and FAULT_LIBRARY.exists())
        for mode in ("write", "sync", "torn-write"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="yewukv-io-fault-") as directory:
                arm = [Path(directory) / f"arm-{i}" for i in range(3)]
                preload = str(FAULT_LIBRARY)
                if ASAN_RUNTIME:
                    preload = f"{ASAN_RUNTIME}:{preload}"
                environments = {i: {"LD_PRELOAD": preload, "YEWUKV_TEST_IO_FAULT": str(arm[i]),
                                     "YEWUKV_TEST_IO_MODE": mode} for i in range(3)}
                cluster = harness.Cluster(directory, node_env=environments)
                try:
                    leader = cluster.leader()
                    self.assertEqual(cluster.call(leader, "put", b"acknowledged", b"safe")["code"], 0)
                    arm[leader].touch()
                    try:
                        response = cluster.call(leader, "put", b"uncertain", b"x", timeout=5)
                        self.assertNotEqual(response["code"], 0)
                    except (OSError, EOFError):
                        pass
                    wait_for(cluster, lambda: cluster.processes[leader].poll() is not None)
                    cluster.kill(leader)
                    cluster.eventually_value(b"acknowledged", b"safe")
                    arm[leader].unlink()
                    cluster.start(leader)
                    wait_for(cluster, lambda: cluster.status(leader)["applied_index"] >= 2)
                    evidence(f"resource-io-{mode}", dict(failed_leader=leader + 1,
                             injected_errno="ENOSPC" if mode == "write" else "EIO",
                             partial_record_written=(mode == "torn-write"),
                             acknowledged_value_recovered=True, checked=True))
                finally:
                    cluster.close()

    def test_short_writes_and_slow_follower_sync_preserve_acknowledged_data(self):
        self.assertTrue(FAULT_LIBRARY and FAULT_LIBRARY.exists())
        for mode in ("short-write", "slow-sync"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="yewukv-io-delay-") as directory:
                arm = [Path(directory) / f"arm-{i}" for i in range(3)]
                preload = str(FAULT_LIBRARY)
                if ASAN_RUNTIME:
                    preload = f"{ASAN_RUNTIME}:{preload}"
                environments = {i: {"LD_PRELOAD": preload, "YEWUKV_TEST_IO_FAULT": str(arm[i]),
                                     "YEWUKV_TEST_IO_MODE": mode} for i in range(3)}
                cluster = harness.Cluster(directory, node_env=environments)
                acknowledged = []
                try:
                    leader = cluster.leader()
                    target = leader if mode == "short-write" else (leader + 1) % 3
                    self.assertEqual(cluster.call(leader, "put", b"anchor", b"safe")["code"], 0)
                    arm[target].touch()
                    for i in range(24):
                        key, value = f"io-{i}".encode(), bytes([i]) * 1024
                        self.assertEqual(cluster.call(leader, "put", key, value)["code"], 0)
                        acknowledged.append((key, value))
                    arm[target].unlink()
                    wait_for(cluster, lambda: cluster.status(target)["applied_index"] >=
                             cluster.status(leader)["commit_index"])
                    for i in range(3):
                        cluster.kill(i)
                    for i in range(3):
                        cluster.start(i)
                    leader = cluster.eventually_value(b"anchor", b"safe")
                    for key, value in acknowledged:
                        self.assertEqual(cluster.call(leader, "get", key)["value"], value)
                    evidence(f"resource-io-{mode}", dict(acknowledged_puts=len(acknowledged),
                             injected_node=target + 1, acknowledged_values_recovered=True,
                             checked=True))
                finally:
                    cluster.close()

    def test_one_node_capacity_limit_preserves_majority_and_recovers_without_deleting_data(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-node-capacity-") as directory:
            cluster = harness.Cluster(directory, node_args={0: ["--max-state-bytes", "4096",
                                      "--snapshot-entries", "1000000000",
                                      "--snapshot-bytes", "67108864"]})
            acknowledged = []
            try:
                leader = cluster.leader()
                for index in range(100):
                    if cluster.processes[0].poll() is not None:
                        break
                    key, value = f"ack-{index}".encode(), b"v" * 200
                    try:
                        response = cluster.call(leader, "put", key, value)
                        if response["code"] == 0:
                            acknowledged.append((key, value))
                    except (OSError, EOFError):
                        break
                wait_for(cluster, lambda: cluster.processes[0].poll() is not None)
                cluster.kill(0)
                leader = cluster.leader()
                for key, value in acknowledged:
                    self.assertEqual(cluster.call(leader, "get", key)["value"], value)
                self.assertEqual(cluster.call(leader, "put", b"majority", b"available")["code"], 0)
                cluster.node_args[0] = ["--snapshot-entries", "8"]
                cluster.start(0)
                wait_for(cluster, lambda: cluster.status(0)["applied_index"] >=
                         cluster.status(leader)["commit_index"])
                evidence("resource-capacity", dict(acknowledged_puts=len(acknowledged),
                         recovered_state=cluster.status(0), checked=True))
            finally:
                cluster.close()

    def test_pending_and_event_queue_backpressure_and_recovery(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-node-busy-") as directory:
            cluster = harness.Cluster(directory, extra_args=["--pending-limit", "4", "--event-queue", "8"])
            try:
                leader = cluster.leader()
                self.assertEqual(cluster.call(leader, "put", b"seed", b"safe")["code"], 0)
                cluster.blocked.add(leader)
                time.sleep(0.25)
                with socket.create_connection(("127.0.0.1", cluster.ports[leader]), 3) as connection:
                    connection.settimeout(5)
                    connection.sendall(b"".join(wire.request(i + 1, "put", f"q{i}".encode(), b"x")
                                                for i in range(40)))
                    responses = [wire.receive_response(connection) for _ in range(40)]
                self.assertEqual([r["id"] for r in responses], list(range(1, 41)))
                self.assertTrue(any(r["code"] == 4 for r in responses))
                self.assertTrue(all(r["code"] in (4, 6) for r in responses))
                state = cluster.status(leader)
                self.assertLessEqual(state["pending"], 4)
                cluster.blocked.clear()
                cluster.eventually_value(b"seed", b"safe")
                evidence("resource-backpressure", dict(response_codes=[r["code"] for r in responses],
                         state=state, checked=True))
            finally:
                cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--suite", choices=("snapshot", "resource"), required=True)
    parser.add_argument("--fault-library", type=Path)
    parser.add_argument("--asan-runtime")
    args = parser.parse_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    FAULT_LIBRARY, ASAN_RUNTIME = args.fault_library, args.asan_runtime
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        SnapshotTests if args.suite == "snapshot" else ResourceTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
