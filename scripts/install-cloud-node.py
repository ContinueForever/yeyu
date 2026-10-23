#!/usr/bin/env python3
"""Install one fixed-member Raft node as a system service on a private IPv4 network."""

import argparse
import ipaddress
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PRIVATE_NETWORKS = tuple(ipaddress.IPv4Network(network) for network in
                         ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))


def members_from(text):
    members = {}
    for item in text.split(","):
        try:
            identity, endpoint = item.split("@")
            host, port = endpoint.split(":")
            identity, port = int(identity), int(port)
            address = ipaddress.IPv4Address(host)
        except ValueError as error:
            raise ValueError("members must be three ID@private-IPv4:port entries") from error
        if identity not in (1, 2, 3) or identity in members or not 1 <= port <= 65535:
            raise ValueError("use unique member IDs 1, 2, 3 and ports 1..65535")
        if not any(address in network for network in PRIVATE_NETWORKS):
            raise ValueError("cloud members must use RFC 1918 private IPv4 addresses")
        members[identity] = (str(address), port)
    if set(members) != {1, 2, 3} or len({host for host, _ in members.values()}) != 3:
        raise ValueError("provide three different server IPs with member IDs 1, 2, 3")
    return members


def write_atomic(path, content, mode):
    """Stage in the destination directory; replacing a binary never truncates it."""
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            os.fchmod(output.fileno(), mode)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run(command):
    subprocess.run(command, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", type=int, choices=(1, 2, 3), required=True)
    parser.add_argument("--members", required=True)
    parser.add_argument("--binary-dir", type=Path, default=ROOT / "build/bin")
    parser.add_argument("--check", action="store_true",
                        help="validate and display configuration without changing the machine")
    args = parser.parse_args()
    members = members_from(args.members)
    host, port = members[args.id]
    member_text = ",".join(f"{identity}@{address}:{member_port}"
                           for identity, (address, member_port) in sorted(members.items()))
    environment = (f"YEWUKV_LISTEN_HOST={host}\nYEWUKV_PORT={port}\n"
                   f"YEWUKV_MEMBERS={member_text}\n")
    identity_text = (f"yewukv-fixed-cluster-v1\nnode={args.id}\n" +
                     "".join(f"{identity}={address}:{member_port}\n"
                             for identity, (address, member_port) in sorted(members.items())))
    template = ROOT / "deploy/systemd/yewukv-node@.service"
    binaries = [args.binary_dir / name for name in ("yewukv-node", "yewukv-client")]
    for binary in binaries:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError(f"build the executable first: {binary}")
    unit_bytes = template.read_bytes()
    service = f"yewukv-node@{args.id}.service"
    data = Path(f"/var/lib/yewukv/node-{args.id}")
    if args.check:
        print(f"Service: {service}\nData: {data}\nConfiguration:\n{environment}", end="")
        print("Configuration only; installation also checks the local address and existing services.")
        return
    if os.geteuid() != 0:
        raise ValueError("run installation as root, or use --check to inspect configuration")
    if not Path("/run/systemd/system").is_dir():
        raise ValueError("installation requires a running system-level systemd manager")
    # Check before creating users or replacing any configuration or binary.
    current_units = subprocess.run(
        ["systemctl", "list-units", "--all", "--no-legend", "--plain",
         "yewukv-node@*.service"], check=True, text=True, capture_output=True).stdout
    for unit in current_units.splitlines():
        if unit.split()[2] not in ("inactive", "failed"):
            raise ValueError("stop all yewukv-node@ services on this machine before installation")
    with socket.socket() as listener:
        listener.bind((host, 0))  # A copied membership list must describe this machine's own IP.
    if data.exists():
        identity_path = data / "cluster.identity"
        if identity_path.exists():
            if identity_path.read_text() != identity_text:
                raise ValueError(f"existing data belongs to a different configuration: {data}")
        elif any(data.iterdir()):
            raise ValueError(f"refusing a nonempty data directory without cluster.identity: {data}")
    try:
        account = pwd.getpwnam("yewukv")
        if account.pw_uid == 0 or account.pw_gid == 0:
            raise ValueError("the yewukv service account must not use root UID or GID")
    except KeyError:
        run(["useradd", "--system", "--user-group", "--no-create-home",
             "--home-dir", "/nonexistent", "--shell", "/usr/sbin/nologin", "yewukv"])
        account = pwd.getpwnam("yewukv")
    if data.exists() and data.stat().st_uid != account.pw_uid:
        raise ValueError(f"existing data must already belong to the yewukv account: {data}")
    config = Path("/etc/yewukv")
    config.mkdir(mode=0o700, exist_ok=True)
    for binary in binaries:
        write_atomic(Path("/usr/local/bin") / binary.name, binary.read_bytes(), 0o755)
    write_atomic(config / f"node-{args.id}.env", environment.encode(), 0o600)
    write_atomic(Path("/etc/systemd/system/yewukv-node@.service"), unit_bytes, 0o644)
    run(["systemctl", "daemon-reload"])
    run(["systemctl", "enable", "--now", service])
    print(f"Installed {service}; data: {data}")
    print("A majority of the three servers must be running before reads and writes can succeed.")
    print(f"Inspect with: systemctl status {service} --no-pager")
    print(f"Logs: journalctl -u {service} -n 50 --no-pager")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"install-cloud-node: {error}", file=sys.stderr)
        sys.exit(1)
