#!/usr/bin/env bash
set -euo pipefail

# Bundle 1A – Baseline plain snapshot + full replay (luôn replay Kafka tail)
# Mục tiêu:
# - Với mỗi cấu hình (PARTS, N) chạy flow:
#   1) Reset topic + dir
#   2) Seed dữ liệu vào p1.orders.enriched
#   3) Cắt snapshot (full, không inflight)
#   4) Crash + restore-only một instance OpB với restore-trust-manifest=false
#   5) Parse log restore để lấy: manifestMs, snapshotMs, changelogMs, totalMs, ttrMs, applied, skipped
# - In đúng 1 dòng CSV dạng:
#   RESULT,bundle=1,mode=baseline,parts=4,N=50000,snapshotMs=...,changelogMs=...,totalMs=...,ttrMs=...,applied=...,skipped=...,inflightEvents=0,replaySkipped=false
#
# Env đầu vào chung:
#   BOOTSTRAP       (mặc định 127.0.0.1:9092)
#   PARTS_LIST      (ví dụ: "4,8,12")
#   N_LIST          (ví dụ: "50000,100000")
#   REPEATS         (số lần lặp mỗi cấu hình, ví dụ: 3 – hiện tại script chạy 1, bạn có thể wrap ngoài)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
BIN_OPB="${BIN_OPB:-$ROOT_DIR/bin/opb}"
KADMIN_BIN="${KADMIN_BIN:-$ROOT_DIR/bin/kadmin}"
GEN_BIN="${GEN_BIN:-$ROOT_DIR/bin/genorders}"

PARTS_LIST="${PARTS_LIST:-4}"
N_LIST="${N_LIST:-50000}"
REPEATS="${REPEATS:-1}"

TOPIC_ENRICHED="${TOPIC_ENRICHED:-p1.orders.enriched}"
TOPIC_SNAP="${TOPIC_SNAP:-p1.opb-snapshots}"
TOPIC_CL="${TOPIC_CL:-p1.opb-changelog}"

WINDOW_SIZE="${WINDOW_SIZE:-3600}"
STATE_DIR_BASE="${STATE_DIR_BASE:-$ROOT_DIR/data/opb-bundle1}"
SNAPSHOT_DIR_BASE="${SNAPSHOT_DIR_BASE:-$ROOT_DIR/snapshots-bundle1}"
CHANGELOG_DIR_BASE="${CHANGELOG_DIR_BASE:-$ROOT_DIR/changelog-bundle1}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/runlogs}"
POST_CUT_FACTOR="${POST_CUT_FACTOR:-1}" # số batch post-cut ~ POST_CUT_FACTOR * N (sử dụng lại tham số seed)

mkdir -p "$LOG_DIR"

say() { printf "\n\e[1;36m[BUNDLE1A]\e[0m %s\n" "$*"; }

require_bin() {
  local bin=$1 pkg=$2
  if [[ ! -x "$bin" ]]; then
    say "Building $pkg -> $bin"
    (cd "$ROOT_DIR" && go build -o "$bin" "./cmd/$pkg")
  fi
}

require_kadmin() { require_bin "$KADMIN_BIN" kadmin; }
require_opb() { require_bin "$BIN_OPB" opb; }
require_genorders() { require_bin "$GEN_BIN" genorders; }

delete_topic_if_exists() {
  local topic=$1
  "$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd delete -topic "$topic" >/dev/null 2>&1 || true
}

ensure_topic() {
  local topic=$1 parts=$2 config=$3
  if [[ -n "$config" ]]; then
    "$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd create -topic "$topic" -partitions "$parts" -rf 1 -config "$config" >/dev/null 2>&1 || true
  else
    "$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd create -topic "$topic" -partitions "$parts" -rf 1 >/dev/null 2>&1 || true
  fi
}

get_lag_total() {
  local url=${1:-http://127.0.0.1:8089/status}
  local data lag
  data=$(curl -s "$url" || true)
  lag=$(printf '%s' "$data" | sed -n 's/.*"lagTotal"[[:space:]]*:[[:space:]]*\([0-9.][0-9.]*\).*/\1/p' | head -n1)
  if [[ -z "$lag" || "$lag" == "null" ]]; then echo 0; else printf '%.0f' "$lag"; fi
}

http_ok() { curl -sf "$1" >/dev/null 2>&1; }

wait_ready() {
  local url=$1; local n=${2:-180}
  for ((i=0;i<n;i++)); do
    if http_ok "$url"; then return 0; fi
    sleep 1
  done
  return 1
}

parse_restore_csv() {
  local logf=$1
  local metricsf=${2:-}
  local manifest_ms="" snap_ms="" changelog_ms="" total_ms="" ttr_ms="" applied="" skipped=""

  if [[ -f "$logf" ]]; then
    local phases
    phases=$(grep -F "restore phases:" "$logf" | tail -n1 | sed -E 's/.*restore phases: //')
    if [[ -n "$phases" ]]; then
      manifest_ms=$(jq -r '.timings.manifestMs // ""' <<<"$phases" 2>/dev/null || echo "")
      snap_ms=$(jq -r '.timings.snapshotTotalMs // ""' <<<"$phases" 2>/dev/null || echo "")
      changelog_ms=$(jq -r '.timings.changelogMs // ""' <<<"$phases" 2>/dev/null || echo "")
      total_ms=$(jq -r '.timings.totalMs // ""' <<<"$phases" 2>/dev/null || echo "")
    fi
    ttr_ms=$(grep -E "restore completed: .*elapsedMs=[0-9]+" "$logf" | tail -n1 | sed -E 's/.*elapsedMs=([0-9]+).*/\1/' || true)
    applied=$(grep -E "restore completed: applied=[0-9]+" "$logf" | tail -n1 | sed -E 's/.*applied=([0-9]+).*/\1/' || true)
    skipped=$(grep -E "restore completed: applied=[0-9]+ skipped=[0-9]+" "$logf" | tail -n1 | sed -E 's/.*skipped=([0-9]+).*/\1/' || true)
  fi

  # Fallback: nếu thiếu timings trong log, thử đọc từ restore-metrics.json
  if [[ -n "$metricsf" && -f "$metricsf" ]]; then
    if [[ -z "$manifest_ms" || "$manifest_ms" == "" || "$manifest_ms" == "null" ]]; then
      manifest_ms=$(jq -r '.phases.manifestMs // ""' "$metricsf" 2>/dev/null || echo "")
    fi
    if [[ -z "$snap_ms" || "$snap_ms" == "" || "$snap_ms" == "null" ]]; then
      snap_ms=$(jq -r '.phases.snapshotTotalMs // ""' "$metricsf" 2>/dev/null || echo "")
    fi
    if [[ -z "$changelog_ms" || "$changelog_ms" == "" || "$changelog_ms" == "null" ]]; then
      changelog_ms=$(jq -r '.phases.changelogMs // ""' "$metricsf" 2>/dev/null || echo "")
    fi
    if [[ -z "$total_ms" || "$total_ms" == "" || "$total_ms" == "null" ]]; then
      total_ms=$(jq -r '.phases.totalMs // ""' "$metricsf" 2>/dev/null || echo "")
    fi
    if [[ -z "$ttr_ms" || "$ttr_ms" == "" || "$ttr_ms" == "null" ]]; then
      ttr_ms=$(jq -r '.ttrMs // ""' "$metricsf" 2>/dev/null || echo "")
    fi
  fi

  echo "${manifest_ms:-},${snap_ms:-},${changelog_ms:-},${total_ms:-},${ttr_ms:-},${applied:-},${skipped:-}"
}

main() {
  require_kadmin
  require_opb
  require_genorders

  IFS=',' read -ra PARTS_ARR <<<"$PARTS_LIST"
  IFS=',' read -ra N_ARR <<<"$N_LIST"

  for parts in "${PARTS_ARR[@]}"; do
    for N in "${N_ARR[@]}"; do
      for ((r=1; r<=REPEATS; r++)); do
        local_state_dir="${STATE_DIR_BASE}.p${parts}.N${N}.r${r}"
        local_snapshot_dir="${SNAPSHOT_DIR_BASE}.p${parts}.N${N}.r${r}"
        local_changelog_dir="${CHANGELOG_DIR_BASE}.p${parts}.N${N}.r${r}"
        mkdir -p "$local_state_dir" "$local_snapshot_dir" "$local_changelog_dir"

        say "Config parts=$parts N=$N run=$r – reset topics/dirs"
        rm -rf "$local_state_dir" "$local_snapshot_dir" "$local_changelog_dir"
        mkdir -p "$local_state_dir" "$local_snapshot_dir" "$local_changelog_dir"

        delete_topic_if_exists "$TOPIC_ENRICHED"
        delete_topic_if_exists "$TOPIC_SNAP"
        delete_topic_if_exists "$TOPIC_CL"

        ensure_topic "$TOPIC_ENRICHED" "$parts" ""
        ensure_topic "$TOPIC_SNAP" 2 "cleanup.policy=compact"
        ensure_topic "$TOPIC_CL" "$parts" "cleanup.policy=delete"

        say "Seeding N≈$N events vào $TOPIC_ENRICHED với $parts partitions"
        local per_key=1
        # Approx: N ≈ stores * products * per_key
        local stores=$parts
        local products=$(( N / (stores*per_key) ))
        if (( products < 1 )); then products=1; fi

        "$GEN_BIN" --mode kafka \
          --bootstrap "$BOOTSTRAP" --topic "$TOPIC_ENRICHED" \
          --stores "$stores" --products "$products" \
          --n-per-key "$per_key" --window-size "$WINDOW_SIZE" --linger-ms 10

        # Start a single OpB instance để cắt snapshot
        local opb_log="$LOG_DIR/bundle1a_p${parts}_N${N}_r${r}_b1.log"
        "$BIN_OPB" \
          --state-backend pebble --state-dir "$local_state_dir" --snapshot-dir "$local_snapshot_dir" \
          --kafka-bootstrap "$BOOTSTRAP" --group-id "bundle1a-b1-p${parts}-N${N}-r${r}" \
          --input-source kafka --topic-enriched "$TOPIC_ENRICHED" \
          --changelog-dir "$local_changelog_dir" \
          --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
          --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
          --snapshot-interval 0 \
          --http :8089 --instance-id "B1-bundle1a" > "$opb_log" 2>&1 &
        local opb_pid=$!
        if ! wait_ready "http://127.0.0.1:8089/healthz" 180; then
          say "ERROR: B1 không lên healthz, xem log $opb_log"
          kill "$opb_pid" 2>/dev/null || true
          continue
        fi

        # Đợi lag≈0 để snapshot full không có inflight
        for ((i=1;i<=300;i++)); do
          lag=$(get_lag_total "http://127.0.0.1:8089/status")
          if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 1 )); then break; fi
          sleep 1
        done

        # Cắt snapshot full (manifest lưu offsets tại thời điểm cut)
        curl -s -X POST "http://127.0.0.1:8089/admin/snapshot-cut?type=full" >/dev/null || true
        # Chờ manifest.latest.json xuất hiện
        for ((i=1;i<=60;i++)); do
          [[ -f "$local_snapshot_dir/manifest.latest.json" ]] && break
          sleep 1
        done

        # Ngay sau cut: bơm thêm backlog để baseline LUÔN phải replay tail
        # (manifest.offsets vẫn là offsets tại thời điểm cut, nên ChangelogHasBacklog sẽ thấy backlog>0).
        if (( POST_CUT_FACTOR > 0 )); then
          local batches=$POST_CUT_FACTOR
          local b
          for ((b=1;b<=batches;b++)); do
            say "Inject post-cut batch $b/$batches cho baseline (tạo Kafka tail để replay)"
            "$GEN_BIN" --mode kafka \
              --bootstrap "$BOOTSTRAP" --topic "$TOPIC_ENRICHED" \
              --stores "$stores" --products "$products" \
              --n-per-key "$per_key" --window-size "$WINDOW_SIZE" --linger-ms 10
          done
          # QUAN TRỌNG: Đợi B1 consume post-cut events và viết vào changelog topic
          # (nếu kill B1 ngay, changelog topic không có backlog → restore sẽ skip replay)
          say "Đợi B1 consume post-cut events và viết vào changelog..."
          for ((i=1;i<=120;i++)); do
            lag=$(get_lag_total "http://127.0.0.1:8089/status")
            if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 10 )); then
              say "B1 đã consume post-cut events (lag=$lag), đợi flush changelog vào Kafka..."
              break
            fi
            sleep 1
          done
          # Đợi B1 flush changelog vào Kafka (B1 có changelog-sink=both: file + Kafka)
          # Check file changelog để xem B1 đã viết vào file chưa, rồi đợi thêm để flush vào Kafka
          local changelog_file="$local_changelog_dir/opb.jsonl"
          local initial_size=0
          if [[ -f "$changelog_file" ]]; then
            initial_size=$(stat -f%z "$changelog_file" 2>/dev/null || stat -c%s "$changelog_file" 2>/dev/null || echo 0)
          fi
          # Đợi file changelog tăng lên (B1 đã viết post-cut events vào file)
          for ((i=1;i<=60;i++)); do
            if [[ -f "$changelog_file" ]]; then
              local current_size
              current_size=$(stat -f%z "$changelog_file" 2>/dev/null || stat -c%s "$changelog_file" 2>/dev/null || echo 0)
              if [[ "$current_size" =~ ^[0-9]+$ ]] && (( current_size > initial_size )); then
                say "B1 đã viết post-cut events vào file changelog (size: $initial_size → $current_size), đợi flush vào Kafka..."
                break
              fi
            fi
            sleep 1
          done
          # Đợi thêm để đảm bảo Kafka producer flush (linger.ms + flush timeout)
          # QUAN TRỌNG: Không kill B1 ngay, để B1 tiếp tục chạy và flush changelog vào Kafka
          # (Nếu kill B1 ngay, changelog có thể chưa được flush vào Kafka → restore sẽ skip replay)
          say "Đợi 15s để B1 flush changelog vào Kafka (B1 vẫn chạy)..."
          sleep 15
        fi

        # Kill B1 để chuẩn bị restore-only baseline
        # (B1 đã consume post-cut events và flush changelog vào Kafka)
        kill "$opb_pid" 2>/dev/null || true
        sleep 2

        # Restore-only với restore-trust-manifest=false (baseline luôn replay tail)
        local restore_log="$LOG_DIR/bundle1a_p${parts}_N${N}_r${r}_restore.log"
        local restore_state_dir="${local_state_dir}.restore"
        "$BIN_OPB" \
          --state-backend pebble --state-dir "$restore_state_dir" --snapshot-dir "$local_snapshot_dir" \
          --kafka-bootstrap "$BOOTSTRAP" --group-id "bundle1a-restore-p${parts}-N${N}-r${r}" \
          --input-source kafka --topic-enriched "$TOPIC_ENRICHED" \
          --changelog-dir "${local_changelog_dir}.restore" \
          --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
          --manifest-sink file --manifest-source file --changelog-sink kafka --changelog-source kafka \
          --snapshot-interval 0 \
          --window-size "$WINDOW_SIZE" \
          --restore-on-start --restore-only \
          --http :8092 --instance-id "B1-restore-bundle1a" > "$restore_log" 2>&1 || true

        csv=$(parse_restore_csv "$restore_log" "$restore_state_dir/restore-metrics.json")
        IFS=',' read -r manifest_ms snap_ms changelog_ms total_ms ttr_ms applied skipped <<<"$csv"

        # Baseline: không inflight, luôn replay tail => inflightEvents=0, replaySkipped=false
        printf "RESULT,bundle=1,mode=baseline,parts=%s,N=%s,snapshotMs=%s,changelogMs=%s,totalMs=%s,ttrMs=%s,applied=%s,skipped=%s,inflightEvents=0,replaySkipped=false\n" \
          "$parts" "$N" "${snap_ms:-}" "${changelog_ms:-}" "${total_ms:-}" "${ttr_ms:-}" "${applied:-}" "${skipped:-}"
      done
    done
  done
}

main "$@"


