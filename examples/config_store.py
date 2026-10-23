#!/usr/bin/env python3
"""Publish JSON configuration and run a small service that polls yewukv.

Uses the shipped C++ client, so leader discovery and uncertain-write handling
are shared with normal clients. No third-party Python packages are required.
"""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def validate_config(config):
    if not isinstance(config, dict) or set(config) != {"message", "enabled", "request_timeout_ms"}:
        raise ValueError("config requires exactly message, enabled and request_timeout_ms")
    if not isinstance(config["message"], str) or not 1 <= len(config["message"]) <= 200:
        raise ValueError("message must contain 1–200 characters")
    if type(config["enabled"]) is not bool:
        raise ValueError("enabled must be a boolean")
    if type(config["request_timeout_ms"]) is not int or not 1 <= config["request_timeout_ms"] <= 60000:
        raise ValueError("request_timeout_ms must be an integer in 1–60000")
    return config


def envelope(config):
    validate_config(config)
    revision = hashlib.sha256(canonical(config).encode()).hexdigest()
    return dict(schema_version=1, revision=revision, config=config)


class ConfigStore:
    def __init__(self, client, nodes, name, timeout_ms):
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", name):
            raise ValueError("name must contain 1–80 letters, numbers, dots, underscores or hyphens")
        self.client, self.nodes, self.key, self.timeout_ms = str(client), nodes, f"config/{name}", timeout_ms

    def call(self, operation, *values):
        result = subprocess.run([self.client, "--nodes", self.nodes, "--timeout-ms", str(self.timeout_ms),
                                 operation, self.key, *values], capture_output=True, text=True,
                                timeout=self.timeout_ms / 1000 + 3)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or f"client exited with {result.returncode}")
        return result.stdout.strip()

    def publish(self, config):
        document = envelope(config)
        payload = canonical(document)
        if len(payload.encode()) > 8192:
            raise ValueError("encoded configuration exceeds the 8192-byte value limit")
        # The client never automatically replays a write with unknown outcome.
        self.call("put", payload)
        return document

    def read(self):
        document = json.loads(self.call("get"))
        if (not isinstance(document, dict) or document.get("schema_version") != 1 or
                "config" not in document or document != envelope(document["config"])):
            raise ValueError("invalid configuration envelope or content revision")
        return document


class Consumer:
    def __init__(self, store, stale_after):
        self.store, self.stale_after = store, stale_after
        self.lock = threading.Lock()
        self.document, self.refreshed, self.last_error = None, 0, None

    def refresh(self):
        try:
            document = self.store.read()
            with self.lock:
                self.document, self.refreshed, self.last_error = document, time.monotonic(), None
            return True
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            with self.lock:
                self.last_error = str(error)
            return False

    def state(self):
        with self.lock:
            age = time.monotonic() - self.refreshed if self.document else None
            return dict(document=self.document, stale=age is None or age > self.stale_after,
                        age_seconds=age, last_error=self.last_error)


def serve(store, port, poll_seconds, stale_after):
    consumer = Consumer(store, stale_after)
    if not consumer.refresh():
        raise RuntimeError(f"initial configuration unavailable: {consumer.state()['last_error']}")
    stopped = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state = consumer.state()
            status = 503 if state["stale"] else 200
            if self.path == "/health":
                body = dict(healthy=not state["stale"], **state)
            elif self.path == "/config":
                body = state
            elif self.path == "/hello":
                config = state["document"]["config"]
                if not config["enabled"]:
                    status = 503
                body = dict(message=config["message"], enabled=config["enabled"], stale=state["stale"],
                            revision=state["document"]["revision"], request_timeout_ms=config["request_timeout_ms"])
            else:
                status, body = 404, dict(error="unknown path")
            payload = canonical(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    def poll():
        while not stopped.wait(poll_seconds):
            consumer.refresh()

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    print(canonical(dict(listen=f"http://127.0.0.1:{server.server_port}", revision=consumer.state()["document"]["revision"])), flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        stopped.set()
        server.server_close()
        thread.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, default=ROOT / "build/bin/yewukv-client")
    parser.add_argument("--nodes", default="127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103")
    parser.add_argument("--name", default="demo")
    parser.add_argument("--timeout-ms", type=int, default=1500)
    sub = parser.add_subparsers(dest="command", required=True)
    publish = sub.add_parser("publish")
    publish.add_argument("--file", type=Path, required=True)
    sub.add_parser("show")
    consumer = sub.add_parser("serve")
    consumer.add_argument("--port", type=int, default=8080)
    consumer.add_argument("--poll-seconds", type=float, default=0.5)
    consumer.add_argument("--stale-after", type=float, default=5)
    args = parser.parse_args()
    if not 100 <= args.timeout_ms <= 60000:
        parser.error("timeout-ms must be in 100–60000")
    store = ConfigStore(args.client, args.nodes, args.name, args.timeout_ms)
    if args.command == "publish":
        print(json.dumps(store.publish(json.loads(args.file.read_text())), indent=2, ensure_ascii=False))
    elif args.command == "show":
        print(json.dumps(store.read(), indent=2, ensure_ascii=False))
    else:
        if (not 0 <= args.port <= 65535 or not math.isfinite(args.poll_seconds) or
                not math.isfinite(args.stale_after) or args.poll_seconds < 0.05 or
                args.stale_after <= args.poll_seconds):
            parser.error("invalid consumer port or refresh/staleness interval")
        serve(store, args.port, args.poll_seconds, args.stale_after)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"config-store: {error}", file=sys.stderr)
        sys.exit(1)
