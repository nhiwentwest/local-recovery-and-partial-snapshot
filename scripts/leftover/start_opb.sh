#!/usr/bin/env bash
set -euo pipefail

# Script để start OpB một cách ổn định, tự động tạo transaction ID mới

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
HTTP_PORT=${HTTP_PORT:-:8089}
WINDOW_SIZE=${WINDOW_SIZE:-1800}
STATE_BACKEND=${STATE_BACKEND:-memory}
LOG_FILE=${LOG_FILE:-/tmp/opb.log}

# Kill OpB cũ nếu có
pkill -f "bin/opb.*-http ${HTTP_PORT}" || true
sleep 1

# Build nếu cần
if [ ! -f ./bin/opb ]; then
  echo "Building OpB..."
  go build -o bin/opb ./cmd/opb
fi

# Tạo transaction ID unique
echo "Starting OpB with:"
echo "  Window size: ${WINDOW_SIZE}s"
echo "  HTTP: ${HTTP_PORT}"
echo "  Log: ${LOG_FILE}"

./bin/opb \
  -kafka-bootstrap "$BOOTSTRAP" \
  -input-source kafka \
  -group-id opb \
  -state-backend "$STATE_BACKEND" \
  -window-size "$WINDOW_SIZE" \
  -http "$HTTP_PORT" \
  > "$LOG_FILE" 2>&1 &

OPB_PID=$!
echo "OpB started with PID: $OPB_PID"

# Đợi OpB ready
echo "Waiting for OpB to be ready..."
for i in {1..30}; do
  if curl -s "http://localhost${HTTP_PORT/:/}/healthz" > /dev/null 2>&1; then
    echo "OpB is ready!"
    curl -s "http://localhost${HTTP_PORT/:/}/healthz"
    echo ""
    echo "Heatmap: http://localhost${HTTP_PORT/:/}/viz/"
    exit 0
  fi
  sleep 1
done

echo "⚠️ OpB may not be ready yet. Check log: tail -f $LOG_FILE"
exit 1

