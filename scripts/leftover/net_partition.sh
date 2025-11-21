#!/usr/bin/env bash
set -euo pipefail

# Linux tc netem example to inject delay/loss on loopback broker port (adjust device/topic accordingly).
# Usage: DURATION=${DURATION:-30} ./scripts/net_partition.sh

DURATION=${DURATION:-30}
IFACE=${IFACE:-lo}
DELAY=${DELAY:-200ms}
JITTER=${JITTER:-50ms}
LOSS=${LOSS:-10%}

echo "[net_partition] Applying netem on $IFACE: delay=$DELAY jitter=$JITTER loss=$LOSS"
sudo tc qdisc add dev "$IFACE" root netem delay "$DELAY" "$JITTER" loss "$LOSS" || true
echo "[net_partition] Hold for ${DURATION}s ..."
sleep "$DURATION"
echo "[net_partition] Removing netem..."
sudo tc qdisc del dev "$IFACE" root netem || true
echo "[net_partition] Done."


