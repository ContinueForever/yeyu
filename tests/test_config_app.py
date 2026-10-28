#!/usr/bin/env python3
"""Exercise configuration publishing, live reload, failover and stale health."""
import argparse
import json
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import HTTPError
from urllib.request import urlopen

import test_cluster as harness
from test_cluster_snapshot import wait_for

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/config_store.py"


class ConfigApplicationTests(unittest.TestCase):
    def test_publish_reload_failover_unavailability_and_full_restart(self):
        with tempfile.TemporaryDirectory(prefix="yewukv-config-app-") as directory:
            cluster = harness.Cluster(directory)
            app = None
            try:
                cluster.leader()
                nodes = ",".join(f"127.0.0.1:{port}" for port in cluster.ports)
                command = [
                    sys.executable,
                    str(EXAMPLE),
                    "--client",
                    str(harness.CLIENT),
                    "--nodes",
                    nodes,
                ]
                config = Path(directory) / "config.json"

                def publish(message):
                    config.write_text(
                        json.dumps(
                            dict(message=message, enabled=True, request_timeout_ms=1500)
                        )
                    )
                    result = subprocess.run(
                        [*command, "publish", "--file", str(config)],
                        text=True,
                        capture_output=True,
                        timeout=10,
                    )
                    # Publishing uses the application's 1500 ms default. The
                    # 500 ms probe budget below serves stale-health detection,
                    # not a write-latency requirement on sanitizer CI runners.
                    self.assertEqual(
                        result.returncode,
                        0,
                        f"publish stdout={result.stdout!r}, stderr={result.stderr!r}\n{cluster.diagnostics()}",
                    )
                    return json.loads(result.stdout)

                first = publish("version one")
                shown = subprocess.run(
                    [*command, "show"],
                    check=True,
                    text=True,
                    capture_output=True,
                    timeout=10,
                )
                self.assertEqual(json.loads(shown.stdout), first)
                app = subprocess.Popen(
                    [
                        *command,
                        "--timeout-ms",
                        "500",
                        "serve",
                        "--port",
                        "0",
                        "--poll-seconds",
                        "0.1",
                        "--stale-after",
                        "0.4",
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                self.assertTrue(
                    select.select([app.stdout], [], [], 10)[0], "consumer did not start"
                )
                line = app.stdout.readline()
                self.assertTrue(line, f"consumer exited with {app.poll()}")
                url = json.loads(line)["listen"]

                def request(path):
                    try:
                        with urlopen(url + path, timeout=2) as response:
                            return response.status, json.load(response)
                    except HTTPError as error:
                        return error.code, json.load(error)

                self.assertEqual(request("/hello")[1]["message"], "version one")
                second = publish("version two")
                wait_for(
                    cluster,
                    lambda: request("/hello")[1].get("revision") == second["revision"],
                )
                old = cluster.leader()
                cluster.kill(old)
                cluster.eventually_value(
                    b"config/demo",
                    json.dumps(
                        second,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode(),
                )
                wait_for(cluster, lambda: request("/health")[0] == 200)
                cluster.start(old)
                cluster.blocked.update(range(3))
                wait_for(cluster, lambda: request("/health")[0] == 503)
                self.assertTrue(request("/config")[1]["stale"])
                self.assertEqual(request("/hello")[0], 503)
                cluster.blocked.clear()
                wait_for(cluster, lambda: request("/hello")[0] == 200)
                for i in range(3):
                    cluster.kill(i)
                for i in range(3):
                    cluster.start(i)
                cluster.eventually_value(
                    b"config/demo",
                    json.dumps(
                        second,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode(),
                )
                wait_for(cluster, lambda: request("/health")[0] == 200)
                self.assertEqual(request("/hello")[1]["revision"], second["revision"])
                config.write_text('{"message":"bad"}')
                invalid = subprocess.run(
                    [*command, "publish", "--file", str(config)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertNotEqual(invalid.returncode, 0)
                self.assertEqual(request("/hello")[1]["revision"], second["revision"])
            finally:
                if app:
                    app.terminate()
                    try:
                        app.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        app.kill()
                        app.communicate(timeout=5)
                cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    harness.NODE, harness.CLIENT = args.node.resolve(), args.client.resolve()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
