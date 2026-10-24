#!/usr/bin/env python3
import argparse
import json
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
import rpc_baseline as raw
from cpp_load import summarize, final_values

BENCH = None


class CppBenchTests(unittest.TestCase):
    def test_closed_and_paced_samples_and_limits_against_real_cluster(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-cpp-bench-") as directory:
            root = Path(directory)
            cluster = harness.Cluster(directory)
            try:
                leader = cluster.leader()
                for rate in [0, 50]:
                    output = root / f"rate-{rate}"
                    prefix = f"bench-test-{rate}"
                    command = [
                        str(BENCH),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(cluster.ports[leader]),
                        "--clients",
                        "4",
                        "--duration-ms",
                        "400",
                        "--warmup-ms",
                        "50",
                        "--offered-rps",
                        str(rate),
                        "--prefix",
                        prefix,
                        "--output",
                        str(output),
                    ]
                    subprocess.run(command, check=True, capture_output=True, timeout=15)
                    result = summarize(output)
                    self.assertTrue(result["valid"])
                    self.assertEqual(result["failures"], 0)
                    if rate:
                        self.assertEqual(result["offered_slots"], 20)
                    self.assertEqual(
                        result["offered_slots"],
                        result["attempts"] + result["missed_slots"],
                    )
                    reconstructed = final_values(output)
                    for client in range(4):
                        expected = [
                            bytes([ord("a") + (client + key) % 26]) * 128
                            for key in range(8)
                        ]
                        random = 0x9E3779B9 + client
                        samples = json.loads(
                            (output / f"client-{client}.json").read_text()
                        )["samples"]
                        for sequence, row in enumerate(samples, 1):
                            random ^= (random << 13) & (2**64 - 1)
                            random ^= random >> 7
                            random ^= (random << 17) & (2**64 - 1)
                            key = (random // 100) % 8
                            self.assertEqual(row[4], int(random % 100 < 50))
                            if row[4]:
                                value = bytearray([ord("a") + sequence % 26] * 128)
                                value[:8] = sequence.to_bytes(8, "little")
                                value[8] = client
                                expected[key] = bytes(value)
                        for key, value in enumerate(expected):
                            self.assertEqual(
                                reconstructed[f"{prefix}-{client}-{key}"], value
                            )
                            self.assertEqual(
                                cluster.call(
                                    leader, "get", f"{prefix}-{client}-{key}".encode()
                                )["value"],
                                value,
                            )
                    if rate:
                        p = output / "client-0.json"
                        data = json.loads(p.read_text())
                        data["samples"][0][1] += 1
                        p.write_text(json.dumps(data))
                        with self.assertRaises(ValueError):
                            summarize(output)
                    self.assertNotEqual(
                        subprocess.run(command, capture_output=True).returncode, 0
                    )
                limited = root / "limited"
                result = subprocess.run(
                    [
                        str(BENCH),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(cluster.ports[leader]),
                        "--clients",
                        "1",
                        "--duration-ms",
                        "3000",
                        "--warmup-ms",
                        "0",
                        "--write-percent",
                        "0",
                        "--max-samples",
                        "128",
                        "--prefix",
                        "bench-limit",
                        "--output",
                        str(limited),
                    ],
                    capture_output=True,
                    timeout=15,
                )
                self.assertEqual(result.returncode, 2)
                self.assertTrue(
                    json.loads((limited / "result.json").read_text())[
                        "sample_limit_reached"
                    ]
                )
                self.assertEqual(summarize(limited)["attempts"], 128)
                cancelled = root / "cancelled"
                process = subprocess.Popen(
                    [
                        str(BENCH),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(cluster.ports[leader]),
                        "--clients",
                        "4",
                        "--duration-ms",
                        "10000",
                        "--warmup-ms",
                        "0",
                        "--offered-rps",
                        "100",
                        "--prefix",
                        "cancelled-load",
                        "--output",
                        str(cancelled),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                try:
                    time.sleep(0.5)
                    process.terminate()
                    process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 2)
                    self.assertTrue(
                        json.loads((cancelled / "result.json").read_text())["cancelled"]
                    )
                    self.assertFalse(summarize(cancelled)["valid"])
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
            finally:
                cluster.close()

    def test_wrong_read_value_is_retained_and_stops_without_replay(self):
        errors, requests = [], []
        with socket.socket() as listener, tempfile.TemporaryDirectory() as directory:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(5)

            def serve():
                try:
                    connection, _ = listener.accept()
                    with connection:
                        while True:
                            header = connection.recv(4)
                            if not header:
                                return
                            while len(header) < 4:
                                header += raw.read_exact(connection, 4 - len(header))
                            fields = raw.decode_response(
                                raw.read_exact(
                                    connection, struct.unpack("!I", header)[0]
                                )
                            )
                            requests.append(fields)
                            connection.sendall(
                                wire.frame(
                                    b"\x08"
                                    + wire.varint(fields[1])
                                    + wire.bytes_field(3, b"wrong")
                                )
                            )
                except Exception as error:
                    errors.append(repr(error))

            worker = threading.Thread(target=serve)
            worker.start()
            output = Path(directory) / "wrong"
            result = subprocess.run(
                [
                    str(BENCH),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(listener.getsockname()[1]),
                    "--clients",
                    "1",
                    "--duration-ms",
                    "300",
                    "--warmup-ms",
                    "0",
                    "--write-percent",
                    "0",
                    "--prefix",
                    "wrong-value",
                    "--output",
                    str(output),
                ],
                capture_output=True,
                timeout=10,
            )
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(result.returncode, 2)
            summary = summarize(output)
            self.assertEqual(summary["codes"], {"-2": 1})
            self.assertEqual(len(requests), 9)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--bench", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT, BENCH = (
        args.node.resolve(),
        args.client.resolve(),
        args.bench.resolve(),
    )
    unittest.main(argv=[__file__, *remaining], verbosity=2)
