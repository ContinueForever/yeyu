#!/usr/bin/env python3
"""Bounded loopback peer proxies for isolated cloud fault experiments."""
import argparse, json, select, socket, threading, time
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--node", type=int, required=True)
    p.add_argument("--hosts", required=True)
    p.add_argument("--control", type=Path, required=True)
    a = p.parse_args()
    hosts = a.hosts.split(",")
    assert len(hosts) == 3 and a.node in [1, 2, 3]
    stopping = threading.Event()
    limit = threading.BoundedSemaphore(64)

    def rules():
        try:
            return json.loads(a.control.read_text())
        except (OSError, ValueError):
            return {"blocked": [1, 2, 3]}

    def relay(client, target):
        try:
            with client, socket.create_connection(
                (hosts[target - 1], 19101), timeout=0.5
            ) as remote:
                for s in [client, remote]:
                    s.settimeout(0.5)
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                while not stopping.is_set():
                    policy = rules()
                    if target in policy.get("blocked", []):
                        return
                    readable, _, _ = select.select([client, remote], [], [], 0.02)
                    for current in readable:
                        data = current.recv(65536)
                        if not data:
                            return
                        time.sleep(policy.get("delay", {}).get(str(target), 0))
                        (remote if current is client else client).sendall(data)
        except OSError:
            pass
        finally:
            limit.release()

    def listen(target):
        with socket.socket() as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("127.0.0.1", 19200 + target))
            server.listen(32)
            server.settimeout(0.2)
            while not stopping.is_set():
                try:
                    client, _ = server.accept()
                except socket.timeout:
                    continue
                if not limit.acquire(blocking=False):
                    client.close()
                    continue
                threading.Thread(
                    target=relay, args=(client, target), daemon=True
                ).start()

    threads = [
        threading.Thread(target=listen, args=(n,), daemon=True)
        for n in [1, 2, 3]
        if n != a.node
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
