#!/usr/bin/env python3
"""Idle eviction, activity refresh and disabling across both protocol modes."""

import argparse
import contextlib
from pathlib import Path
import time
import unittest

import test_network as net
import test_rpc_network as rpc


class IdleNetworkTests(unittest.TestCase):
    @contextlib.contextmanager
    def server(self, threads, protocol, timeout):
        server = net.ServerProcess(
            threads, ["--protocol", protocol, "--idle-timeout", str(timeout)]
        )
        try:
            server.start()
            yield server
            self.assertIsNone(server.process.poll())
            # Exercise graceful shutdown with the idle timer installed/disabled.
            server.process.terminate()
            self.assertEqual(server.process.wait(timeout=3), 0)
        except Exception as error:
            raise AssertionError(f"{error}\n{server.diagnostics()}") from error
        finally:
            server.cleanup()

    def ping(self, sock, protocol):
        if protocol == "text":
            sock.sendall(b"PING\n")
            self.assertEqual(net.receive_exact(sock, 6), b"+PONG\n")
        else:
            sock.sendall(rpc.request(1, "ping"))
            response = rpc.receive_response(sock)
            self.assertEqual(response["id"], 1)
            self.assertEqual(response["code"], rpc.OK)

    def test_idle_and_partial_requests_close_while_active_connection_survives(self):
        for protocol in ("text", "rpc"):
            for threads in (0, 4):
                with self.subTest(protocol=protocol, threads=threads):
                    with self.server(threads, protocol, 1) as server:
                        idle = server.connect()
                        partial = server.connect()
                        partial.sendall(b"P" if protocol == "text" else b"\x00")
                        active = server.connect()
                        # Keep one connection active beyond both the timeout and
                        # scan interval; the other two must have been reclaimed.
                        deadline = time.monotonic() + 2.4
                        while time.monotonic() < deadline:
                            self.ping(active, protocol)
                            time.sleep(0.1)
                        self.assertEqual(net.receive_until_closed(idle, timeout=2), b"")
                        self.assertEqual(net.receive_until_closed(partial, timeout=2), b"")
                        self.ping(active, protocol)
                        # Stop activity: this same connection must now expire.
                        started = time.monotonic()
                        self.assertEqual(net.receive_until_closed(active, timeout=3), b"")
                        self.assertGreaterEqual(time.monotonic() - started, 0.8)
                        # Listener and worker loops still serve new connections.
                        self.ping(server.connect(), protocol)

    def test_zero_disables_idle_eviction(self):
        for protocol in ("text", "rpc"):
            for threads in (0, 4):
                with self.subTest(protocol=protocol, threads=threads):
                    with self.server(threads, protocol, 0) as server:
                        sock = server.connect()
                        self.ping(sock, protocol)
                        time.sleep(2.2)
                        self.ping(sock, protocol)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    args, remaining = parser.parse_known_args()
    net.SERVER = args.server.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
