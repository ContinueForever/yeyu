#!/usr/bin/env bash
# Manage the fixed three-node loopback development cluster with user systemd.
set -euo pipefail
SOURCE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="${YEWUKV_CLUSTER_ROOT:-$(dirname -- "$SOURCE")/cluster-runtime}"
NODES="127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103"
MEMBERS="1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103"
ACTION="${1:-status}"
if [[ $# -gt 0 ]]; then shift; fi
case "$ACTION" in
  install)
    [[ -x "$SOURCE/build/bin/yewukv-node" ]]
    mkdir -p "$RUNTIME/data" "$RUNTIME/units"
    cat > "$RUNTIME/units/yewukv-cluster@.service" <<EOF
[Unit]
Description=yewukv Raft node %i
PartOf=yewukv-cluster.target

[Service]
Type=simple
ExecStart="$SOURCE/build/bin/yewukv-node" --id %i --port 910%i --members $MEMBERS --data-dir "$RUNTIME/data/node-%i"
Restart=on-failure
RestartSec=2
TimeoutStopSec=10
UMask=0077
EOF
    cat > "$RUNTIME/units/yewukv-cluster.target" <<EOF
[Unit]
Description=yewukv fixed-three-node local cluster
Wants=yewukv-cluster@1.service yewukv-cluster@2.service yewukv-cluster@3.service
After=yewukv-cluster@1.service yewukv-cluster@2.service yewukv-cluster@3.service

[Install]
WantedBy=default.target
EOF
    systemctl --user link "$RUNTIME/units/yewukv-cluster@.service" \
      "$RUNTIME/units/yewukv-cluster.target"
    systemctl --user daemon-reload
    systemctl --user enable yewukv-cluster.target
    systemctl --user restart yewukv-cluster.target
    "$SOURCE/build/bin/yewukv-client" --nodes "$NODES" --timeout-ms 5000 ping
    ;;
  start|stop|restart)
    systemctl --user "$ACTION" yewukv-cluster.target
    ;;
  status)
    for port in 9101 9102 9103; do
      "$SOURCE/build/bin/yewukv-client" --port "$port" --timeout-ms 1000 status || true
    done
    ;;
  logs)
    journalctl --user -u 'yewukv-cluster@*' -n 60 --no-pager
    ;;
  backup)
    DEST="${1:?provide an empty backup destination}"
    [[ ! -e "$DEST" ]] || { echo "backup destination already exists" >&2; exit 1; }
    python3 - "$RUNTIME/data" "$DEST" <<'PY'
import sys
from pathlib import Path
data, destination = (Path(p).resolve() for p in sys.argv[1:])
if destination == data or data in destination.parents:
    raise SystemExit("backup destination must be outside the live data directory")
PY
    # Stop all three units before taking a consistent filesystem copy. A trap
    # restores service even if copying fails. All node identities are included.
    WAS_ACTIVE="$(systemctl --user is-active yewukv-cluster.target || true)"
    systemctl --user stop yewukv-cluster.target
    trap 'if [[ "$WAS_ACTIVE" == active ]]; then systemctl --user start yewukv-cluster.target; fi' EXIT
    mkdir -p "$(dirname -- "$DEST")"
    cp -a -- "$RUNTIME/data" "$DEST"
    python3 "$SOURCE/scripts/cluster_backup.py" manifest "$DEST"
    sync -f "$DEST"
    ;;
  restore)
    BACKUP="${1:?provide the backup directory}"
    python3 "$SOURCE/scripts/cluster_backup.py" verify-local "$BACKUP"
    STAGING="$RUNTIME/restore-staging-$$"
    [[ ! -e "$STAGING" ]]
    cp -a -- "$BACKUP" "$STAGING"
    python3 "$SOURCE/scripts/cluster_backup.py" verify "$STAGING"
    sync -f "$STAGING"
    WAS_ACTIVE="$(systemctl --user is-active yewukv-cluster.target || true)"
    systemctl --user stop yewukv-cluster.target
    trap 'if [[ "$WAS_ACTIVE" == active ]]; then systemctl --user start yewukv-cluster.target; fi' EXIT
    ROLLBACK="$RUNTIME/before-restore-$(date +%Y%m%d-%H%M%S)-$$"
    # An empty placeholder also permits full recovery after the data directory
    # is lost; the original configuration is verified against the backup itself.
    mkdir -p "$RUNTIME/data"
    # Linux atomic directory exchange keeps data/ present throughout restore.
    python3 - "$RUNTIME/data" "$STAGING" "$ROLLBACK" <<'PY'
import ctypes
import os
import sys
from pathlib import Path
data, staging, rollback = map(Path, sys.argv[1:])
libc = ctypes.CDLL(None, use_errno=True)
exchange = libc.renameat2
exchange.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
exchange.restype = ctypes.c_int
if exchange(-100, os.fsencode(data), -100, os.fsencode(staging), 2) != 0:
    raise OSError(ctypes.get_errno(), "atomic data directory exchange failed")
fd = os.open(data.parent, os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(fd)
    staging.rename(rollback)
    os.fsync(fd)
finally:
    os.close(fd)
PY
    echo "previous data preserved at: $ROLLBACK"
    ;;
  *)
    exec "$SOURCE/build/bin/yewukv-client" --nodes "$NODES" --timeout-ms 5000 "$ACTION" "$@"
    ;;
esac
