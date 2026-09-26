#!/usr/bin/env python3
"""RPC process tests using the protobuf wire format and Python standard library."""

import argparse
import contextlib
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest


SERVER = None
MAX_PAYLOAD = 65536
OK, NOT_FOUND, INVALID_ARGUMENT, INTERNAL = range(4)


def varint(value):
    if not 0 <= value <= (1 << 64) - 1:
        raise ValueError("value must fit uint64")
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def bytes_field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value


def request_payload(request_id, operation, key=b"", value=b"", include_id=True):
    payload = b"\x08" + varint(request_id) if include_id else b""
    if operation is None:
        return payload
    number = {"put": 2, "get": 3, "del": 4, "ping": 5}[operation]
    body = b"" if operation == "ping" else bytes_field(1, key)
    if operation == "put":
        body += bytes_field(2, value)
    return payload + bytes_field(number, body)


def frame(payload):
    return struct.pack("!I", len(payload)) + payload


def request(request_id, operation, key=b"", value=b"", include_id=True):
    return frame(request_payload(request_id, operation, key, value, include_id))


def read_varint(payload, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(payload):
            raise AssertionError("truncated protobuf varint")
        byte = payload[offset]
        offset += 1
        if shift == 63 and byte > 1:
            raise AssertionError("protobuf varint overflows uint64")
        value |= (byte & 127) << shift
        if not byte & 128:
            return value, offset
    raise AssertionError("invalid protobuf varint")


def decode_response(payload):
    result = {"id": 0, "code": OK, "value": b"", "error": ""}
    offset = 0
    while offset < len(payload):
        tag, offset = read_varint(payload, offset)
        number, wire_type = tag >> 3, tag & 7
        if number == 0:
            raise AssertionError("invalid protobuf field number")
        if wire_type == 0:
            value, offset = read_varint(payload, offset)
        elif wire_type == 2:
            length, offset = read_varint(payload, offset)
            if length > len(payload) - offset:
                raise AssertionError("truncated protobuf bytes field")
            value = payload[offset:offset + length]
            offset += length
        elif wire_type in (1, 5):
            width = 8 if wire_type == 1 else 4
            if offset + width > len(payload):
                raise AssertionError("truncated protobuf fixed field")
            value = payload[offset:offset + width]
            offset += width
        else:
            raise AssertionError("unsupported response wire type")
        if number in (1, 2):
            if wire_type != 0:
                raise AssertionError("wrong response integer wire type")
            result["id" if number == 1 else "code"] = value
        elif number in (3, 4):
            if wire_type != 2:
                raise AssertionError("wrong response bytes wire type")
            result["value" if number == 3 else "error"] = (
                value if number == 3 else value.decode("utf-8")
            )
    return result


def receive_exact(sock, count, timeout=4.0):
    deadline = time.monotonic() + timeout
    result = bytearray()
    while len(result) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"received {len(result)} of {count} bytes")
        sock.settimeout(remaining)
        chunk = sock.recv(min(count - len(result), 65536))
        if not chunk:
            raise EOFError(f"connection closed after {len(result)} of {count} bytes")
        result.extend(chunk)
    return bytes(result)


def receive_response(sock):
    length, = struct.unpack("!I", receive_exact(sock, 4))
    if not 1 <= length <= MAX_PAYLOAD:
        raise AssertionError(f"invalid response payload length: {length}")
    return decode_response(receive_exact(sock, length))


class ServerProcess:
    def __init__(self, threads, protocol="rpc"):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        self.threads = threads
        self.protocol = protocol
        self.process = None
        self.log = tempfile.TemporaryFile(mode="w+b")
        self.data_directory = tempfile.TemporaryDirectory(prefix="yewukv-rpc-")
        self.sockets = []

    def start(self):
        command = [str(SERVER), "--port", str(self.port), "--io-threads", str(self.threads),
                   "--data-dir", self.data_directory.name]
        if self.protocol is not None:
            command += ["--protocol", self.protocol]
        self.process = subprocess.Popen(
            command, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log
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
        raise TimeoutError("server did not start listening")

    def connect(self):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=4.0)
        self.sockets.append(sock)
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
        self.data_directory.cleanup()


class RpcNetworkTests(unittest.TestCase):
    @contextlib.contextmanager
    def server(self, threads, protocol="rpc", expect_exit=False):
        server = ServerProcess(threads, protocol)
        try:
            server.start()
            yield server
            if not expect_exit:
                self.assertIsNone(server.process.poll(), "server terminated unexpectedly")
        except Exception as error:
            raise AssertionError(
                f"{error}\nprotocol={protocol}, io-threads={threads}, port={server.port}\n"
                f"--- captured server output ---\n{server.diagnostics()}"
            ) from error
        finally:
            server.cleanup()

    def assert_response(self, sock, request_id, code=OK, value=b""):
        response = receive_response(sock)
        self.assertEqual(response["id"], request_id)
        self.assertEqual(response["code"], code)
        self.assertEqual(response["value"], value)
        if code == OK:
            self.assertEqual(response["error"], "")
        else:
            self.assertTrue(response["error"])
        return response

    def assert_closed_without_response(self, sock):
        sock.settimeout(2.0)
        try:
            received = sock.recv(1)
        except ConnectionResetError:
            return
        self.assertEqual(received, b"", "invalid wire input must close without a response")

    def assert_alive(self, server):
        self.assertIsNone(server.process.poll())
        with server.connect() as sock:
            sock.sendall(request(817, "ping"))
            self.assert_response(sock, 817)

    def test_coalesced_requests_preserve_ids_and_binary_values(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                key = b"binary\x00key\xff"
                value = b"zero\x00newline\n\r\xff"
                with server.connect() as sock:
                    sock.sendall(
                        request(17, "put", key, value)
                        + request(400, "get", key)
                        + request(3, "del", key)
                        + request(99, "get", key)
                        + request((1 << 64) - 1, "ping")
                        + request(20, "put", b"empty", b"")
                        + request(21, "get", b"empty")
                        + request(22, "del", b"absent")
                    )
                    self.assert_response(sock, 17)
                    self.assert_response(sock, 400, value=value)
                    self.assert_response(sock, 3)
                    self.assert_response(sock, 99, NOT_FOUND)
                    self.assert_response(sock, (1 << 64) - 1)
                    self.assert_response(sock, 20)
                    self.assert_response(sock, 21)
                    self.assert_response(sock, 22)

    def test_fragmented_header_and_payload(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                value = bytes(range(256)) * 20
                encoded = request(32769, "put", b"fragmented", value)
                with server.connect() as sock:
                    sock.sendall(encoded[:2])
                    sock.settimeout(0.05)
                    with self.assertRaises(socket.timeout):
                        sock.recv(1)
                    sock.sendall(encoded[2:17])
                    with self.assertRaises(socket.timeout):
                        sock.recv(1)
                    for offset in range(17, len(encoded), 137):
                        sock.sendall(encoded[offset:offset + 137])
                    self.assert_response(sock, 32769)
                    sock.sendall(request(8, "get", b"fragmented") + request(1, "ping"))
                    self.assert_response(sock, 8, value=value)
                    self.assert_response(sock, 1)

    def test_invalid_frame_lengths_and_protobuf_close_only_the_peer(self):
        invalid_inputs = [
            struct.pack("!I", length) for length in (0, MAX_PAYLOAD + 1, 0xFFFFFFFF)
        ]
        invalid_inputs += [frame(payload) for payload in (b"\x80", b"\x00", b"\x12\x05\x0a\x01x")]
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                for encoded in invalid_inputs:
                    with self.subTest(encoded=encoded), server.connect() as offender:
                        offender.sendall(encoded)
                        self.assert_closed_without_response(offender)
                    self.assert_alive(server)

    def test_invalid_request_fields_return_errors_without_closing(self):
        requests = [
            (0, request(0, "ping")),
            (0, request(0, "ping", include_id=False)),
            (43, request(43, None)),
            (44, request(44, "put", b"", b"value")),
            (45, request(45, "get", b"")),
            (46, request(46, "del", b"")),
        ]
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                with server.connect() as sock:
                    sock.sendall(b"".join(encoded for _, encoded in requests) + request(47, "ping"))
                    for request_id, _ in requests:
                        self.assert_response(sock, request_id, INVALID_ARGUMENT)
                    self.assert_response(sock, 47)

    def test_maximum_frame_and_oversized_response_are_bounded(self):
        # A short ID lets PUT carry a value that a GET with a ten-byte ID cannot
        # fit into its response. Both request frames are individually valid.
        value_size = MAX_PAYLOAD - 13
        value = b"v" * value_size
        payload = request_payload(1, "put", b"k", value)
        self.assertEqual(len(payload), MAX_PAYLOAD)
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                with server.connect() as sock:
                    sock.sendall(frame(payload))
                    self.assert_response(sock, 1)
                    sock.sendall(request(2, "get", b"k"))
                    self.assert_response(sock, 2, value=value)
                    sock.sendall(request((1 << 64) - 1, "get", b"k"))
                    self.assert_response(sock, (1 << 64) - 1, INTERNAL)
                    sock.sendall(request(3, "ping"))
                    self.assert_response(sock, 3)
                self.assert_alive(server)

    def test_rpc_and_text_modes_are_separate(self):
        for threads in (0, 4):
            with self.subTest(threads=threads), self.server(threads) as server:
                with server.connect() as sock:
                    sock.sendall(b"PING\n")
                    self.assert_closed_without_response(sock)
                self.assert_alive(server)
            for mode in (None, "text"):
                with self.subTest(threads=threads, mode=mode), self.server(threads, mode) as server:
                    with server.connect() as sock:
                        sock.sendall(b"PING\n")
                        self.assertEqual(receive_exact(sock, 6), b"+PONG\n")
                        sock.sendall(request(1, "ping") + b"\nPING\n")
                        expected = b"-ERR bad command\n+PONG\n"
                        self.assertEqual(receive_exact(sock, len(expected)), expected)

    def test_rpc_signals_stop_cleanly(self):
        for threads in (0, 4):
            for stop_signal in (signal.SIGINT, signal.SIGTERM):
                with self.subTest(threads=threads, signal=stop_signal.name):
                    with self.server(threads, expect_exit=True) as server:
                        with server.connect() as sock:
                            sock.sendall(request(1, "ping"))
                            self.assert_response(sock, 1)
                            server.process.send_signal(stop_signal)
                            self.assertEqual(server.process.wait(timeout=2.5), 0)

    def test_invalid_protocol_option_is_rejected(self):
        for arguments in (["--protocol", "binary"], ["--protocol"]):
            with self.subTest(arguments=arguments):
                completed = subprocess.run(
                    [str(SERVER), *arguments], stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2.5
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn(b"--protocol", completed.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path, help="path to yewukv-server")
    arguments, unittest_arguments = parser.parse_known_args()
    SERVER = arguments.server.resolve()
    if not SERVER.is_file() or not os.access(SERVER, os.X_OK):
        parser.error(f"server executable is missing or not executable: {SERVER}")
    unittest.main(argv=[__file__, *unittest_arguments], verbosity=2)
