#!/usr/bin/env bash
#
# Bundle 2 – EOS Idempotent Replay (Pebble + LastSeq)
# ---------------------------------------------------
# Mục tiêu:
#   - Chạy pha restore-only trên cùng một snapshot + changelog.
#   - Trong một lần chạy binary, replay changelog nhiều lần trên cùng state.
#   - Dựa vào log "bundle2: replay pass=X applied=... skipped=..." để chứng minh:
#       + Pass 1: applied > 0, skipped ~ 0  (replay bình thường).
#       + Pass 2,3: applied ~ 0, skipped > 0 (idempotent do LastSeq).
#
# Yêu cầu:
#   - Đã có sẵn snapshot + changelog cho kịch bản Bundle 2:
#       snapshots-bundle2_eos_p4_N50000/
#       changelog-bundle2_eos_p4_N50000/
#   - Binary đã build: bin/opb
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BIN_OPB="${ROOT_DIR}/bin/opb"
if [[ ! -x "${BIN_OPB}" ]]; then
  echo "error: ${BIN_OPB} not found or not executable. Run 'make build' first." >&2
  exit 1
fi

# Dùng bộ snapshot + manifest đã có sẵn từ kịch bản recovery.
SNAP_DIR="${ROOT_DIR}/snapshots-recovery"
CHANGELOG_DIR="${ROOT_DIR}/changelog"
STATE_DIR="${ROOT_DIR}/tmp-bundle2-eos-state"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${STATE_DIR}" "${LOG_DIR}"

LOG_FILE="${LOG_DIR}/bundle2_eos_replay3.log"
rm -f "${LOG_FILE}"

echo "=== Bundle 2 – EOS Idempotent Replay (Pebble + LastSeq) ==="
echo "Snapshot dir : ${SNAP_DIR}"
echo "Changelog dir: ${CHANGELOG_DIR}"
echo "State dir    : ${STATE_DIR}"
echo "Log file     : ${LOG_FILE}"

rm -rf "${STATE_DIR:?}/"*

# Chạy một lần restore-only với 3 passes replay.
# - replay-extra-passes=3: pass=1 là restore chuẩn, pass=2,3 là extra.
"${BIN_OPB}" \
  --group-id "opb-bundle2-eos" \
  --window-size 60 \
  --snapshot-dir "${SNAP_DIR}" \
  --snapshot-interval 0 \
  --snapshot-shards 0 \
  --state-backend pebble \
  --state-dir "${STATE_DIR}" \
  --changelog-source file \
  --changelog-dir "${CHANGELOG_DIR}" \
  --manifest-source file \
  --changelog-sink file \
  --manifest-sink file \
  --input-source sample \
  --restore-on-start \
  --restore-only \
  --replay-extra-passes 3 \
  --http ":8099" \
  --instance-id "bundle2-eos-tech" \
  > "${LOG_FILE}" 2>&1 || true

echo
echo "=== Bundle 2 log (bundle2: replay pass=...) ==="
grep -E 'bundle2: replay pass=' "${LOG_FILE}" || {
  echo "warning: no bundle2 replay lines found in log; check ${LOG_FILE}" >&2
}

echo
echo "=== Restore summary line ==="
grep -E 'restore completed: applied=' "${LOG_FILE}" || true

echo
echo "Done. Use the lines above (pass 1..3) cho phần Bundle 2 trong report/slide."


