#!/usr/bin/env python3
"""Measure single-node KV RPC latency and throughput with persistent connections.

This development baseline is not a substitute for a controlled cluster benchmark.
It uses only the Python standard library and the documented Protobuf wire format.
"""

import argparse
import concurrent.futures
import json
from datetime import datetime, timezone
import math
import os
import platform
import socket
import struct
import subprocess
import threading
import time
from pathlib import Path


def varint(value):
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def bytes_field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value


def read_varint(data, index):
    value = 0
    for shift in range(0, 70, 7):
        if index >= len(data):
            raise ValueError("truncated Protobuf varint")
        byte = data[index]
        index += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, index
    raise ValueError("invalid Protobuf varint")


def decode_response(data):
    fields = {}
    index = 0
    while index < len(data):
        tag, index = read_varint(data, index)
        number, wire_type = tag >> 3, tag & 7
        if wire_type == 0:
            value, index = read_varint(data, index)
        elif wire_type == 2:
            length, index = read_varint(data, index)
            if length > len(data) - index:
                raise ValueError("truncated Protobuf field")
            value = data[index : index + length]
            index += length
        else:
            raise ValueError(f"unsupported Protobuf wire type {wire_type}")
        fields[number] = value
    return fields


def read_exact(sock, count):
    result = bytearray()
    while len(result) < count:
        part = sock.recv(count - len(result))
        if not part:
            raise ConnectionError("RPC peer closed before response completed")
        result.extend(part)
    return bytes(result)


def rpc(sock, request_id, operation, key, value):
    if operation == "put":
        command = bytes_field(1, key) + bytes_field(2, value)
        operation_field = 2
    else:
        command = bytes_field(1, key)
        operation_field = 3
    request = varint(8) + varint(request_id) + bytes_field(operation_field, command)
    sock.sendall(struct.pack("!I", len(request)) + request)
    length = struct.unpack("!I", read_exact(sock, 4))[0]
    if length == 0 or length > 65536:
        raise ValueError(f"invalid response frame length: {length}")
    response = decode_response(read_exact(sock, length))
    if response.get(1) != request_id:
        raise ValueError(f"response ID mismatch for request {request_id}")
    if response.get(2, 0) != 0:
        raise ValueError(f"RPC application error {response.get(2)}: {response.get(4)}")
    if operation == "get" and response.get(3, b"") != value:
        raise ValueError("GET returned the wrong value")


def worker(client_id, args, barrier):
    key = f"bench-{client_id}".encode()
    value = b"v" * args.value_size
    latency_ns = []
    writes = 0
    try:
        with socket.create_connection((args.host, args.port), timeout=args.timeout) as sock:
            sock.settimeout(args.timeout)
            rpc(sock, 1, "put", key, value)
            for index in range(args.warmup_ops):
                operation = "put" if index % 100 < args.write_percent else "get"
                rpc(sock, index + 2, operation, key, value)
            barrier.wait(timeout=30)
            for index in range(args.ops_per_client):
                operation = "put" if index % 100 < args.write_percent else "get"
                before = time.perf_counter_ns()
                rpc(sock, args.warmup_ops + index + 2, operation, key, value)
                latency_ns.append(time.perf_counter_ns() - before)
                writes += operation == "put"
            finished = time.perf_counter_ns()
        return latency_ns, finished, writes
    except BaseException:
        barrier.abort()  # Release peers promptly if connect/prepopulation fails.
        raise


def measure(args):
    # Barrier actions run before any participant is released. End at the last
    # completed request, excluding socket teardown and future collection.
    started = []
    barrier = threading.Barrier(
        args.clients, action=lambda: started.append(time.perf_counter_ns())
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        futures = [pool.submit(worker, i, args, barrier) for i in range(args.clients)]
        results = [future.result() for future in futures]
    latencies = [sample for samples, _, _ in results for sample in samples]
    elapsed_seconds = (max(end for _, end, _ in results) - started[0]) / 1_000_000_000
    return latencies, elapsed_seconds, sum(writes for _, _, writes in results)


def percentile(values, fraction):
    return round(values[max(0, math.ceil(len(values) * fraction) - 1)] / 1_000_000, 3)


def git_state():
    repo = Path(__file__).resolve().parent.parent
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=False
    )
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=repo,
        capture_output=True, text=True, check=False
    )
    return revision.stdout.strip() or "unknown", bool(dirty.stdout) or dirty.returncode != 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--ops-per-client", type=int, default=1000)
    parser.add_argument("--write-percent", type=int, default=50)
    parser.add_argument("--value-size", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--warmup-ops", type=int, default=100)
    parser.add_argument("--samples-output", type=Path, help="save each timed latency in nanoseconds")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.clients < 1 or args.ops_per_client < 1 or not math.isfinite(args.timeout) or args.timeout <= 0):
        parser.error("clients, ops-per-client and timeout must be positive")
    if not 0 <= args.write_percent <= 100 or not 0 <= args.value_size <= 60000:
        parser.error("write-percent must be 0..100 and value-size must be 0..60000")
    if args.warmup_ops < 0 or not 1 <= args.port <= 65535:
        parser.error("warmup-ops must be nonnegative and port must be 1..65535")
    latencies, elapsed_seconds, writes = measure(args)
    if args.samples_output:
        args.samples_output.write_text(json.dumps(latencies) + "\n", encoding="utf-8")
    # Sorting is outside the measured interval.
    latencies.sort()
    revision, dirty = git_state()
    result = {
        "version": 2,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "git_revision": revision,
        "working_tree_dirty": dirty,
        "host": args.host,
        "port": args.port,
        "clients": args.clients,
        "operations": len(latencies),
        "ops_per_client": args.ops_per_client,
        "warmup_ops_per_client": args.warmup_ops,
        "timeout_seconds": args.timeout,
        "completed_puts": writes,
        "completed_gets": len(latencies) - writes,
        "samples_file": str(args.samples_output) if args.samples_output else None,
        "write_percent": args.write_percent,
        "value_size_bytes": args.value_size,
        "elapsed_seconds": elapsed_seconds,
        "throughput_ops_per_second": round(len(latencies) / elapsed_seconds, 1),
        "latency_ms": {"p50": percentile(latencies, 0.50),
                       "p95": percentile(latencies, 0.95),
                       "p99": percentile(latencies, 0.99)},
        "machine": {"platform": platform.platform(), "logical_cpus": os.cpu_count(),
                    "python": platform.python_version(),
                    "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None},
        "note": "Python threaded closed-loop client; one outstanding RPC and one hot key per connection. Not a server saturation benchmark.",
    }
    output = json.dumps(result, ensure_ascii=False, indent=2)
    print(output)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
