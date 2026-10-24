#!/usr/bin/env python3
"""Process-level TCP regression tests; uses only the Python standard library."""

import argparse
import concurrent.futures
import contextlib
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest


SERVER = None
MAX_LINE = 64 * 1024
SOCKET_TIMEOUT = 4.0


def unused_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def receive_exact(sock, size, timeout=SOCKET_TIMEOUT):
    """TCP may split any response, even a six-byte PONG, across reads."""
    deadline = time.monotonic() + timeout
    result = bytearray()
    while len(result) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"received {len(result)} of {size} expected bytes")
        sock.settimeout(remaining)
        chunk = sock.recv(min(size - len(result), 65536))
        if not chunk:
            raise EOFError(f"connection closed after {len(result)} of {size} bytes")
        result.extend(chunk)
    return bytes(result)


def receive_until_closed(sock, timeout=SOCKET_TIMEOUT, byte_limit=16 * 1024 * 1024):
    deadline = time.monotonic() + timeout
    result = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("peer did not close the connection")
        sock.settimeout(remaining)
        try:
            chunk = sock.recv(65536)
        except ConnectionResetError:
            return bytes(result)
        if not chunk:
            return bytes(result)
        result.extend(chunk)
        if len(result) > byte_limit:
            raise AssertionError("peer exceeded the response limit without closing")


def bulk(value):
    return b"$" + str(len(value)).encode() + b"\n" + value + b"\n"


class ServerProcess:
    def __init__(self, threads, extra_args=(), data_directory=None):
        self.port = unused_port()
        self.threads = threads
        self.extra_args = extra_args
        self.process = None
        self.log = tempfile.TemporaryFile(mode="w+b")
        self.owned_directory = (tempfile.TemporaryDirectory(prefix="yewukv-network-")
                                if data_directory is None else None)
        self.data_directory = (self.owned_directory.name if self.owned_directory
                               else str(data_directory))
        self.sockets = []

    def start(self):
        self.process = subprocess.Popen(
            [str(SERVER), "--port", str(self.port), "--io-threads", str(self.threads),
             "--data-dir", self.data_directory, *self.extra_args],
            stdin=subprocess.DEVNULL,
            stdout=self.log,
            stderr=self.log,
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"server exited during startup: {self.process.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                    return
            except OSError:
                time.sleep(0.01)
        raise TimeoutError("server did not start listening within five seconds")

    def connect(self, receive_buffer=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sockets.append(sock)
        if receive_buffer is not None:
            # Set before connect so the initial advertised receive window is small.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        sock.settimeout(SOCKET_TIMEOUT)
        sock.connect(("127.0.0.1", self.port))
        return sock

    def diagnostics(self):
        self.log.flush()
        self.log.seek(0)
        return self.log.read().decode("utf-8", errors="replace")[-12000:]

    def cleanup(self):
        for sock in self.sockets:
            sock.close()
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=2.5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=2.0)
            else:
                self.process.wait()
        self.log.close()
        if self.owned_directory:
            self.owned_directory.cleanup()


class NetworkTests(unittest.TestCase):
    @contextlib.contextmanager
    def server(self, threads):
        server = ServerProcess(threads)
        try:
            server.start()
            yield server
            self.assertIsNone(server.process.poll(), "server terminated unexpectedly")
        except Exception as error:
            raise AssertionError(
                f"{error}\nserver io-threads={threads}, port={server.port}\n"
                f"--- captured server output ---\n{server.diagnostics()}"
            ) from error
        finally:
            server.cleanup()

    def exchange(self, sock, request, expected):
        sock.sendall(request)
        self.assertEqual(receive_exact(sock, len(expected)), expected)

    def assert_alive(self, server):
        self.assertIsNone(server.process.poll())
        with server.connect() as sock:
            self.exchange(sock, b"PING\n", b"+PONG\n")

    def test_pipeline_and_fragmented_commands(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                with server.connect() as sock:
                    self.exchange(
                        sock,
                        b"PING\nPUT alpha hello world\nGET alpha\nDEL alpha\nGET alpha\n",
                        b"+PONG\n+OK\n$11\nhello world\n:1\n$-1\n",
                    )
                    for part in (b"PU", b"T frag", b"mented split", b" value\r", b"\nGE", b"T fragmented\n"):
                        sock.sendall(part)
                    expected = b"+OK\n$11\nsplit value\n"
                    self.assertEqual(receive_exact(sock, len(expected)), expected)

    def test_disconnects_and_concurrent_clients(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                # Close before reading a reply, both normally and with an RST.
                for index in range(60):
                    sock = server.connect()
                    sock.sendall(b"PING\n" * 8)
                    if index % 2:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    sock.close()

                barrier = threading.Barrier(8, timeout=4.0)

                def client(client_id):
                    with server.connect() as sock:
                        barrier.wait()
                        commands = bytearray()
                        expected = bytearray()
                        for item in range(40):
                            key = f"client-{client_id}-item-{item}".encode()
                            value = f"value-{client_id}-{item}".encode()
                            commands.extend(b"PUT " + key + b" " + value + b"\nGET " + key + b"\n")
                            commands.extend(b"DEL " + key + b"\nGET " + key + b"\n")
                            expected.extend(b"+OK\n" + bulk(value) + b":1\n$-1\n")
                        self.exchange(sock, commands, expected)

                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    futures = [executor.submit(client, client_id) for client_id in range(8)]
                    for future in futures:
                        future.result(timeout=10.0)
                self.assert_alive(server)

    def test_half_close_preserves_all_responses(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                value = b"v" * (48 * 1024)
                with server.connect() as writer:
                    self.exchange(writer, b"PUT half " + value + b"\n", b"+OK\n")
                with server.connect(receive_buffer=4096) as reader:
                    reader.sendall(b"GET half\n" * 12 + b"PING\n")
                    reader.shutdown(socket.SHUT_WR)
                    expected = bulk(value) * 12 + b"+PONG\n"
                    self.assertEqual(receive_exact(reader, len(expected)), expected)
                    self.assertEqual(receive_until_closed(reader), b"")
                self.assert_alive(server)

    def test_line_limit_rejects_only_offending_connection(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                with server.connect() as healthy:
                    prefix = b"PUT boundary "
                    value = b"v" * (MAX_LINE - len(prefix) - 1)
                    self.exchange(healthy, prefix + value + b"\n", b"+OK\n")
                    self.exchange(healthy, b"GET boundary\n", bulk(value))

                    prefix = b"PUT rejected "
                    too_long = prefix + b"v" * (MAX_LINE - len(prefix)) + b"\n"
                    for request in (b"x" * (MAX_LINE + 1), too_long):
                        with server.connect() as offender:
                            try:
                                offender.sendall(request)
                            except (BrokenPipeError, ConnectionResetError):
                                pass
                            response = receive_until_closed(offender, byte_limit=4096)
                            self.assertNotIn(b"+OK\n", response)
                        self.exchange(healthy, b"PING\nGET rejected\n", b"+PONG\n$-1\n")
                self.assert_alive(server)

    def test_slow_reader_is_disconnected_at_output_limit(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                value = b"v" * (60 * 1024)
                with server.connect() as writer:
                    self.exchange(writer, b"PUT large " + value + b"\n", b"+OK\n")
                with server.connect(receive_buffer=4096) as slow:
                    # About 60 MiB of requested responses defeats kernel buffering;
                    # the 1 MiB application queue must close this non-reading peer.
                    slow.sendall(b"GET large\n" * 1024)
                    time.sleep(0.1)
                    self.assert_alive(server)
                    # Drain only after the server had time to detect backpressure.
                    slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
                    receive_until_closed(slow, timeout=8.0)
                self.assert_alive(server)

    def test_signals_stop_idle_active_and_slow_connections(self):
        for threads in (0, 4):
            for stop_signal in (signal.SIGINT, signal.SIGTERM):
                for state in ("idle", "active", "slow"):
                    with self.subTest(threads=threads, signal=stop_signal.name, state=state):
                        # This test expects process exit, unlike the normal fixture.
                        server = ServerProcess(threads)
                        stop_client = threading.Event()
                        client_thread = None
                        errors = []
                        try:
                            server.start()
                            client = server.connect(receive_buffer=4096 if state == "slow" else None)
                            if state == "active":
                                started = threading.Event()

                                def keep_active():
                                    try:
                                        while not stop_client.is_set():
                                            self.exchange(client, b"PING\n" * 16, b"+PONG\n" * 16)
                                            started.set()
                                    except (OSError, EOFError):
                                        pass  # The shutdown may interrupt an in-flight exchange.
                                    except Exception as error:
                                        errors.append(error)

                                client_thread = threading.Thread(target=keep_active, daemon=True)
                                client_thread.start()
                                self.assertTrue(started.wait(timeout=2.0), "active client never received a reply")
                            elif state == "slow":
                                value = b"v" * (60 * 1024)
                                with server.connect() as writer:
                                    self.exchange(writer, b"PUT pending " + value + b"\n", b"+OK\n")
                                # Stay below the output cap so this tests shutdown
                                # with unread responses, not the overflow path.
                                client.sendall(b"GET pending\n" * 12)
                                time.sleep(0.05)
                            else:
                                self.exchange(client, b"PING\n", b"+PONG\n")

                            started_at = time.monotonic()
                            server.process.send_signal(stop_signal)
                            return_code = server.process.wait(timeout=2.5)
                            self.assertLess(time.monotonic() - started_at, 2.5)
                            self.assertEqual(return_code, 0, "signal must trigger a clean exit")
                        except Exception as error:
                            raise AssertionError(
                                f"{error}\nserver output:\n{server.diagnostics()}"
                            ) from error
                        finally:
                            stop_client.set()
                            # Close owned sockets to wake an in-flight client recv.
                            for sock in server.sockets:
                                try:
                                    sock.shutdown(socket.SHUT_RDWR)
                                except OSError:
                                    pass
                            if client_thread is not None:
                                client_thread.join(timeout=1.0)
                            server.cleanup()
                        self.assertFalse(errors, f"active client saw invalid responses: {errors}")
                        if client_thread is not None:
                            self.assertFalse(client_thread.is_alive(), "client thread did not stop")

    def test_sigterm_drains_generated_responses_without_reset(self):
        for threads in (0, 4):
            with self.subTest(threads=threads):
                server = ServerProcess(threads)
                try:
                    server.start()
                    with server.connect(receive_buffer=4096) as client:
                        value = b"d" * (60 * 1024)
                        self.exchange(client, b"PUT drain " + value + b"\n", b"+OK\n")
                        client.sendall(b"GET drain\n" * 8)

                        # The last bulk header proves every requested response
                        # has been generated. Its body is still unread, so no
                        # expectation depends on unprocessed requests at SIGTERM.
                        header = b"$" + str(len(value)).encode() + b"\n"
                        generated_prefix = bulk(value) * 7 + header
                        self.assertEqual(
                            receive_exact(client, len(generated_prefix)), generated_prefix
                        )
                        server.process.send_signal(signal.SIGTERM)

                        # Leave additional input while shutdown drains output.
                        # An incomplete line generates no extra reply even if
                        # it races ahead of signal handling. Closing the socket
                        # with this input unread must not reset the response.
                        client.sendall(b"GET " + b"x" * (48 * 1024))
                        self.assertEqual(receive_exact(client, len(value) + 1), value + b"\n")
                        client.settimeout(2.5)
                        # Deliberately do not use receive_until_closed, which
                        # accepts RST for tests that intentionally reject peers.
                        self.assertEqual(client.recv(1), b"", "expected an orderly server FIN")
                        client.shutdown(socket.SHUT_WR)
                    self.assertEqual(server.process.wait(timeout=2.5), 0)
                except Exception as error:
                    raise AssertionError(
                        f"{error}\nserver io-threads={threads}\n"
                        f"server output:\n{server.diagnostics()}"
                    ) from error
                finally:
                    server.cleanup()

    def assert_startup_rejected(self, arguments):
        with tempfile.TemporaryFile(mode="w+b") as log:
            process = subprocess.Popen(
                [str(SERVER), *arguments],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
            )
            try:
                try:
                    return_code = process.wait(timeout=2.5)
                except subprocess.TimeoutExpired as error:
                    raise AssertionError(f"server accepted invalid startup arguments: {arguments}") from error
                log.seek(0)
                diagnostics = log.read().decode("utf-8", errors="replace")
                self.assertNotEqual(return_code, 0, diagnostics)
                self.assertTrue(diagnostics.strip(), "startup failure needs a diagnostic")
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=2.0)

    def test_occupied_port_is_rejected(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            self.assert_startup_rejected(
                ["--port", str(listener.getsockname()[1]), "--io-threads", "0"]
            )

    def test_invalid_arguments_are_rejected(self):
        invalid_arguments = [
            ["--port", value]
            for value in ("-1", "0", "65536", "abc", "12x", "999999999999999999999")
        ]
        invalid_arguments += [
            ["--io-threads", value]
            for value in ("-1", "abc", "2x", "999999999999999999999")
        ]
        invalid_arguments += [
            ["--idle-timeout", value]
            for value in ("-1", "86401", "abc", "1.5", "999999999999999999999")
        ]
        invalid_arguments += [
            ["--storage-queue", value]
            for value in ("0", "-1", "8193", "abc", "999999999999999999999")
        ]
        invalid_arguments += [["--storage-queue"], ["--idle-timeout"]]
        invalid_arguments += [["--unknown"], ["--port"], ["--io-threads"]]
        for invalid in invalid_arguments:
            with self.subTest(arguments=invalid):
                self.assert_startup_rejected(
                    ["--port", str(unused_port()), "--io-threads", "0", *invalid]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path, help="path to yewukv-server")
    arguments, unittest_arguments = parser.parse_known_args()
    SERVER = arguments.server.resolve()
    if not SERVER.is_file() or not os.access(SERVER, os.X_OK):
        parser.error(f"server executable is missing or not executable: {SERVER}")
    unittest.main(argv=[__file__, *unittest_arguments], verbosity=2)
