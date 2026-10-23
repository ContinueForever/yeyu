#!/usr/bin/env python3
"""Exercise explicit IPv4 listeners, peer replication, client discovery and restart."""

import argparse
import fcntl
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import test_cluster as harness
import test_rpc_network as wire


class DirectCluster(harness.Cluster):
    """Use the same advertised member list on all nodes, without fault proxies."""

    def __init__(self, directory, hosts, shared_port=False, explicit=True):
        self.directory = Path(directory)
        self.hosts, self.explicit = hosts, explicit
        self.stop, self.proxies = threading.Event(), {}
        self.processes = [None] * 3
        reservations = []
        try:
            for host in hosts:
                reservation = socket.socket()
                reservations.append(reservation)
                port = reservations[0].getsockname()[1] if shared_port and len(reservations) > 1 else 0
                reservation.bind((host, port))
            self.ports = [reservation.getsockname()[1] for reservation in reservations]
        finally:
            for reservation in reservations:
                reservation.close()
        self.members = ",".join(f"{i + 1}@{host}:{port}"
                                for i, (host, port) in enumerate(zip(hosts, self.ports)))
        try:
            for i in range(3):
                self.start(i)
        except BaseException:
            self.close()
            raise

    def start(self, i):
        with open(self.directory / f"node-{i + 1}.log", "ab") as log:
            self.processes[i] = subprocess.Popen(
                [str(harness.NODE), "--id", str(i + 1), "--port", str(self.ports[i]),
                 "--members", self.members, "--data-dir", str(self.directory / f"node-{i + 1}"),
                 *(["--listen-host", self.hosts[i]] if self.explicit else [])],
                stdout=log, stderr=log)

    def call(self, i, operation, key=b"", value=b"", timeout=2):
        with socket.create_connection((self.hosts[i], self.ports[i]), timeout=timeout) as connection:
            if operation == "status":
                packet = wire.frame(b"\x08\x01" + wire.bytes_field(6, b""))
            else:
                packet = wire.request(1, operation, key, value)
            connection.sendall(packet)
            return wire.receive_response(connection)

    def client(self, *operation):
        nodes = ",".join(f"{host}:{port}" for host, port in zip(self.hosts, self.ports))
        result = subprocess.run([str(harness.CLIENT), "--nodes", nodes, "--timeout-ms", "10000",
                                 *operation], capture_output=True, text=True, timeout=15)
        if result.returncode:
            raise AssertionError(f"{result.stdout}\n{result.stderr}\n{self.diagnostics()}")
        return result.stdout.strip()


def interface_ipv4():
    with socket.socket() as sock:
        for _, name in socket.if_nameindex():
            try:
                result = fcntl.ioctl(sock, 0x8915, struct.pack("256s", name.encode()[:15]))
                host = socket.inet_ntoa(result[20:24])
                if not host.startswith("127.") and host != "0.0.0.0":
                    return host
            except OSError:
                pass
    return None


class ClusterAddressTests(unittest.TestCase):
    def assert_not_listening(self, host, port):
        with self.assertRaises(OSError):
            socket.create_connection((host, port), timeout=0.5).close()

    def test_distinct_addresses_same_port_failover_and_full_restart(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-address-") as directory:
            cluster = DirectCluster(directory, ["127.0.0.2", "127.0.0.3", "127.0.0.4"], shared_port=True)
            try:
                old = cluster.leader()
                self.assert_not_listening("127.0.0.1", cluster.ports[0])
                self.assertEqual(cluster.client("put", "before-failure", "retained"), "OK")
                cluster.kill(old)
                cluster.eventually_value(b"before-failure", b"retained")
                self.assertEqual(cluster.client("put", "after-failure", "also-retained"), "OK")
                cluster.start(old)
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    try:
                        if all(cluster.status(i)["keys"] == 2 for i in range(3)):
                            break
                    except (OSError, EOFError):
                        pass
                    time.sleep(0.05)
                else:
                    self.fail("restarted replica did not catch up\n" + cluster.diagnostics())
                for i in range(3):
                    cluster.kill(i)
                for i in range(3):
                    cluster.start(i)
                cluster.eventually_value(b"after-failure", b"also-retained")
                self.assertEqual(cluster.client("get", "before-failure"), "retained")
                self.assertEqual(cluster.client("get", "after-failure"), "also-retained")
            finally:
                cluster.close()

    def test_default_stays_on_loopback(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-default-address-") as directory:
            cluster = DirectCluster(directory, ["127.0.0.1"] * 3, explicit=False)
            try:
                cluster.leader()
                for port in cluster.ports:
                    self.assert_not_listening("127.0.0.2", port)
                self.assertEqual(cluster.client("ping"), "PONG")
            finally:
                cluster.close()

    def test_non_loopback_interface_replication(self):
        host = interface_ipv4()
        if host is None:
            self.skipTest("no non-loopback IPv4 interface available")
        with tempfile.TemporaryDirectory(prefix="yewukv-interface-") as directory:
            cluster = DirectCluster(directory, [host] * 3)
            try:
                cluster.leader()
                for port in cluster.ports:
                    self.assert_not_listening("127.0.0.1", port)
                self.assertEqual(cluster.client("put", "interface", "ready"), "OK")
                self.assertEqual(cluster.client("get", "interface"), "ready")
                cluster.kill(cluster.leader())
                cluster.eventually_value(b"interface", b"ready")
            finally:
                cluster.close()

    def test_invalid_listener_is_rejected_before_creating_data(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-invalid-address-") as directory:
            data = Path(directory) / "must-not-be-created"
            for host in ("localhost", "0.0.0.0", "224.0.0.1", "255.255.255.255", "127.0.0.1\n"):
                with self.subTest(host=host):
                    result = subprocess.run(
                        [str(harness.NODE), "--id", "1", "--port", "9101", "--data-dir", str(data),
                         "--members", "1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103",
                         "--listen-host", host], capture_output=True, text=True, timeout=5)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("--listen-host must be", result.stderr)
                    self.assertFalse(data.exists())

    def test_cloud_install_check_rejects_unsafe_members(self):
        installer = Path(__file__).resolve().parents[1] / "scripts/install-cloud-node.py"
        command = [sys.executable, str(installer), "--id", "2", "--check",
                   "--binary-dir", str(harness.NODE.parent), "--members"]
        valid = "1@172.22.44.37:9101,2@172.22.44.38:9101,3@172.22.44.39:9101"
        result = subprocess.run([*command, valid], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("YEWUKV_LISTEN_HOST=172.22.44.38\n", result.stdout)
        self.assertIn("YEWUKV_MEMBERS=" + valid, result.stdout)
        for members in (valid + ",", valid.replace("172.22.44.38", "0.0.0.0"),
                        valid.replace("172.22.44.38", "8.8.8.8"),
                        valid.replace("172.22.44.38", "172.22.44.37"),
                        valid.replace("2@", "1@"), valid.replace(":9101", ":0"),
                        valid + "\nExecStart=/bin/false"):
            with self.subTest(members=members):
                result = subprocess.run([*command, members], capture_output=True, text=True, timeout=5)
                self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
