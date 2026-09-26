#!/usr/bin/env bash
# Chaos injection helpers (M3+). Requires NET_ADMIN inside containers/VM.
# Usage:
#   scripts/chaos.sh partition <peer_ip>      # block traffic to a peer
#   scripts/chaos.sh restore   <peer_ip>
#   scripts/chaos.sh delay <ms>               # add latency on eth0
#   scripts/chaos.sh loss  <percent>          # drop percentage of packets
set -euo pipefail
IFACE="${IFACE:-eth0}"
case "${1:-}" in
  partition)
    sudo iptables -A INPUT -s "$2" -j DROP
    sudo iptables -A OUTPUT -d "$2" -j DROP
    echo "partitioned from $2" ;;
  restore)
    sudo iptables -D INPUT -s "$2" -j DROP
    sudo iptables -D OUTPUT -d "$2" -j DROP
    echo "restored traffic with $2" ;;
  delay)
    sudo tc qdisc add dev "$IFACE" root netem delay "${2}ms" 2>/dev/null \
      || sudo tc qdisc change dev "$IFACE" root netem delay "${2}ms"
    echo "added ${2}ms delay on $IFACE" ;;
  loss)
    sudo tc qdisc add dev "$IFACE" root netem loss "$2%" 2>/dev/null \
      || sudo tc qdisc change dev "$IFACE" root netem loss "$2%"
    echo "added $2% loss on $IFACE" ;;
  clear)
    sudo tc qdisc del dev "$IFACE" root || true
    echo "cleared tc rules on $IFACE" ;;
  *)
    grep '^#' "$0" | sed 's/^# \{0,1\}//' ; exit 1 ;;
esac
