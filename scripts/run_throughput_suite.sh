#!/usr/bin/env bash
set -euo pipefail

# Chạy một loạt cấu hình throughput và tổng hợp kết quả (thô) vào logs/throughput_report.txt

ROOT=$(cd "$(dirname "$0")/.." && pwd)
REPORT="$ROOT/logs/throughput_report.txt"
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}

echo "# Throughput report $(date -Is)" > "$REPORT"

run_case(){
  local label=$1; shift
  echo "\n## $label" >> "$REPORT"
  echo "cmd: $*" >> "$REPORT"
  (cd "$ROOT" && eval "$*" | tee -a "$REPORT")
}

# Case 1: PARALLEL=2, SLEEP=0.02
run_case "PARALLEL=2 SLEEP=0.02 N=20000" \
  "TOPIC=p2.orders.enriched MODE=enriched PARALLEL=2 SLEEP=0.02 N=20000 CHUNK=1000 ./scripts/pump_test.sh & PID=\$!; bin/count_changelog -bootstrap $BOOTSTRAP -topic p2.opb-changelog -seconds 70; kill \$PID 2>/dev/null || true"

# Case 2: PARALLEL=4, SLEEP=0.02
run_case "PARALLEL=4 SLEEP=0.02 N=20000" \
  "TOPIC=p2.orders.enriched MODE=enriched PARALLEL=4 SLEEP=0.02 N=20000 CHUNK=1000 ./scripts/pump_test.sh & PID=\$!; bin/count_changelog -bootstrap $BOOTSTRAP -topic p2.opb-changelog -seconds 70; kill \$PID 2>/dev/null || true"

# Case 3: PARALLEL=6, SLEEP=0.01
run_case "PARALLEL=6 SLEEP=0.01 N=20000" \
  "TOPIC=p2.orders.enriched MODE=enriched PARALLEL=6 SLEEP=0.01 N=20000 CHUNK=1000 ./scripts/pump_test.sh & PID=\$!; bin/count_changelog -bootstrap $BOOTSTRAP -topic p2.opb-changelog -seconds 70; kill \$PID 2>/dev/null || true"

echo "\n# Done" >> "$REPORT"
echo "Report written to $REPORT"


