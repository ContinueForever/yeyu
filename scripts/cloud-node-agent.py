#!/usr/bin/env python3
"""Temporary, capability-authenticated control of one isolated experiment node.

No shell/command API. Only the agent's exact temporary units and data prefix
can be controlled. Bind to a private address; config and capability stay private.
"""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac, json, os
import hashlib
from pathlib import Path
import pwd, re, signal, subprocess, threading, time


def command(*args):
    return subprocess.check_output(args, text=True, timeout=20)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text())
    tag = cfg["tag"]
    node = cfg["node"]
    root = Path(cfg["root"])
    assert (
        re.fullmatch(r"yewukv-next-[a-zA-Z0-9-]+", tag)
        and node in [1, 2, 3]
        and root == Path("/root") / tag
    )
    unit = tag + "-node-" + str(node)
    proxyunit = tag + "-proxy-" + str(node)
    binary = Path("/usr/local/libexec") / tag / "yewukv-node"
    proxy = binary.parent / "peer-proxy.py"
    data_root = Path("/var/lib") / tag
    data_root.mkdir(mode=0o700, exist_ok=True)
    user = pwd.getpwnam("yewukv")
    os.chown(data_root, user.pw_uid, user.pw_gid)
    control = root / "proxy-control.json"
    control.write_text("{}\n")
    lock = threading.Lock()
    active = {}

    def stop(name):
        subprocess.run(
            ["systemctl", "stop", name + ".service"], capture_output=True, timeout=20
        )

    def launch(name, args, case):
        return command(
            "systemd-run",
            "--unit",
            name,
            "--collect",
            "--property=Restart=no",
            "--property=UMask=0077",
            "--property=StandardOutput=append:" + str(root / (case + ".log")),
            "--property=StandardError=append:" + str(root / (case + ".log")),
            *args,
        )

    def action(payload):
        selected = payload["action"]
        with lock:
            if selected == "start":
                case = payload["case"]
                mode = payload.get("encoder", "sync")
                proxies = payload.get("proxies", False)
                program = payload.get("program", "new")
                if program not in ["new", "legacy"] or (
                    program == "legacy" and not case.startswith("legacy-")
                ):
                    raise ValueError(
                        "legacy execution requires an isolated legacy case"
                    )
                assert re.fullmatch(r"[a-zA-Z0-9-]{1,64}", case) and mode in [
                    "sync",
                    "background",
                ]
                stop(unit)
                stop(proxyunit)
                directory = data_root / (case + "-node-" + str(node))
                directory.mkdir(mode=0o700, exist_ok=True)
                if directory.is_symlink():
                    raise ValueError("experiment data directory must not be a symlink")
                marker = root / (case + ".program")
                if (
                    program == "legacy"
                    and marker.exists()
                    and marker.read_text() == "new"
                ):
                    raise ValueError("never open upgraded data using a legacy program")
                selected_binary = (
                    binary if program == "new" else Path("/usr/local/bin/yewukv-node")
                )
                if (
                    program == "legacy"
                    and hashlib.sha256(selected_binary.read_bytes()).hexdigest()
                    != "6bb46da21f4a3284cace8536eec9d2c8867e67cc9a3f187ea532ef0308575b02"
                ):
                    raise ValueError("legacy binary hash differs")
                marker.write_text(program)
                os.chown(directory, user.pw_uid, user.pw_gid)
                members = ",".join(
                    f"{n}@{ip if n==node or not proxies else '127.0.0.1'}:{19101 if n==node or not proxies else 19200+n}"
                    for n, ip in enumerate(cfg["hosts"], 1)
                )
                if proxies:
                    control.write_text("{}\n")
                    launch(
                        proxyunit,
                        [
                            "/usr/bin/python3",
                            str(proxy),
                            "--node",
                            str(node),
                            "--hosts",
                            ",".join(cfg["hosts"]),
                            "--control",
                            str(control),
                        ],
                        case + "-proxy",
                    )
                argv = [
                    str(selected_binary),
                    "--id",
                    str(node),
                    "--listen-host",
                    cfg["hosts"][node - 1],
                    "--port",
                    "19101",
                    "--members",
                    members,
                    "--data-dir",
                    str(directory),
                    "--snapshot-policy",
                    "fixed",
                    "--snapshot-entries",
                    str(payload.get("snapshot_entries", 128)),
                    "--snapshot-bytes",
                    str(payload.get("snapshot_bytes", 1048576)),
                    "--journal-checkpoint-bytes",
                    "67108864",
                    "--request-timeout-ms",
                    "900",
                    "--pending-limit",
                    str(payload.get("pending_limit", 1024)),
                    "--event-queue",
                    str(payload.get("event_queue", 4096)),
                ]
                if program == "new":
                    argv += ["--snapshot-encoder", mode]
                # Service credentials match the existing node; original units/data are never used.
                launch(
                    unit,
                    [
                        "/usr/bin/setpriv",
                        "--reuid=" + str(user.pw_uid),
                        "--regid=" + str(user.pw_gid),
                        "--init-groups",
                        *argv,
                    ],
                    case + "-node",
                )
                active.update(case=case, encoder=mode, proxies=proxies)
                return dict(started=True, argv=argv)
            if selected == "stop":
                stop(unit)
                stop(proxyunit)
                active.clear()
                return dict(stopped=True)
            if selected == "signal":
                sig = payload["signal"]
                assert sig in ["KILL", "STOP", "CONT"]
                return dict(
                    output=command(
                        "systemctl",
                        "kill",
                        "--kill-whom=main",
                        "--signal=" + sig,
                        unit + ".service",
                    )
                )
            if selected == "rules":
                blocked = payload.get("blocked", [])
                delay = payload.get("delay", {})
                assert all(
                    type(n) == int and n in [1, 2, 3] and n != node for n in blocked
                )
                assert all(
                    k in ["1", "2", "3"] and 0 <= v <= 0.2 for k, v in delay.items()
                )
                temporary = control.with_suffix(".tmp")
                temporary.write_text(json.dumps(dict(blocked=blocked, delay=delay)))
                temporary.replace(control)
                return dict(updated=True)
            if selected == "inspect":
                props = command(
                    "systemctl",
                    "show",
                    unit + ".service",
                    "--property=MainPID,ActiveState,NRestarts",
                )
                pid = next(
                    (
                        line.split("=", 1)[1]
                        for line in props.splitlines()
                        if line.startswith("MainPID=")
                    ),
                    "0",
                )
                info = dict(
                    properties=props, active=active, monotonic_ns=time.monotonic_ns()
                )
                if pid != "0":
                    p = Path("/proc") / pid
                    info.update(
                        pid=int(pid),
                        stat=(p / "stat").read_text(),
                        status=(p / "status").read_text(),
                        io=(p / "io").read_text(),
                    )
                info["cpu_stat"] = Path("/proc/stat").read_text().splitlines()[0]
                return info
            raise ValueError("unsupported action")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if not hmac.compare_digest(
                self.headers.get("Authorization", ""), "Bearer " + cfg["token"]
            ):
                self.send_error(403)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                assert 0 < length <= 8192
                result = action(json.loads(self.rfile.read(length)))
                data = json.dumps(result).encode()
                self.send_response(200)
            except Exception as error:
                data = json.dumps(dict(error=repr(error))).encode()
                self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer((cfg["hosts"][node - 1], 19190), Handler)

    def shutdown(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        stop(unit)
        stop(proxyunit)
        server.server_close()


if __name__ == "__main__":
    main()
