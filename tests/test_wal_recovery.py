#!/usr/bin/env python3
"""Check acknowledged writes across SIGKILL and process-level directory locking."""

import argparse
from pathlib import Path
import tempfile
import unittest

import test_network as net


class WalRecoveryTests(unittest.TestCase):
    def test_sigkill_recovery_and_exclusive_lock(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-recovery-") as directory:
            first = net.ServerProcess(threads=2, data_directory=directory)
            contender = None
            restarted = None
            try:
                first.start()
                with first.connect() as connection:
                    connection.sendall(b"PUT durable acknowledged\n")
                    self.assertEqual(net.receive_exact(connection, 4), b"+OK\n")

                contender = net.ServerProcess(threads=0, data_directory=directory)
                with self.assertRaisesRegex(AssertionError, "server exited during startup"):
                    contender.start()
                self.assertIn("already in use", contender.diagnostics())

                first.process.kill()
                self.assertEqual(first.process.wait(timeout=5), -9)
                with open(Path(directory) / "wal.log", "ab") as wal:
                    wal.write(b"YWA")  # Simulate a torn final header after the durable Put.

                restarted = net.ServerProcess(threads=0, data_directory=directory)
                restarted.start()
                with restarted.connect() as connection:
                    connection.sendall(b"GET durable\nPUT later value\nGET later\n")
                    expected = b"$12\nacknowledged\n+OK\n$5\nvalue\n"
                    self.assertEqual(net.receive_exact(connection, len(expected)), expected)
                self.assertEqual((Path(directory) / "wal.log").stat().st_size,
                                 24 + 7 + 12 + 4 + 24 + 5 + 5 + 4)
            finally:
                if restarted:
                    restarted.cleanup()
                if contender:
                    contender.cleanup()
                first.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True, type=Path)
    args, remaining = parser.parse_known_args()
    net.SERVER = args.server.resolve()
    unittest.main(argv=[__file__, *remaining])
