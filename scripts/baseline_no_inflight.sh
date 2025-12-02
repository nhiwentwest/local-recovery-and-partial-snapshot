#!/usr/bin/env bash
set -euo pipefail

# Bundle 2A – Baseline no inflight + full tail replay
# Mục tiêu:
# - Không có inflight snapshot → phải replay nhiều events từ Kafka tail
# - So sánh với tech có inflight → replay ít hơn từ tail
# - In CSV: parts,N,snapshotMs,changelogMs,totalMs,ttrMs,applied,skipped,inflightEvents=0,tailReplayEvents=applied

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
STATE_DIR_BASE="${STATE_DIR_BASE:-$ROOT_DIR/data/opb-bundle2}"
SNAPSHOT_DIR_BASE="${SNAPSHOT_DIR_BASE:-$ROOT_DIR/snapshots-bundle2}"
CHANGELOG_DIR_BASE="${CHANGELOG_DIR_BASE:-$ROOT_DIR/changelog-bundle2}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/runlogs}"
BACKLOG_FACTOR="${BACKLOG_FACTOR:-1}" # số batch backlog ~ BACKLOG_FACTOR * N (inject TRƯỚC khi cut)

mkdir -p "$LOG_DIR"

say() { printf "\n\e[1;36m[BUNDLE2A]\e[0m %s\n" "$*"; }

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
        local stores=$parts
        local products=$(( N / (stores*per_key) ))
        if (( products < 1 )); then products=1; fi

        "$GEN_BIN" --mode kafka \
          --bootstrap "$BOOTSTRAP" --topic "$TOPIC_ENRICHED" \
          --stores "$stores" --products "$products" \
          --n-per-key "$per_key" --window-size "$WINDOW_SIZE" --linger-ms 10

        # Start OpB instance với --disable-barrier-cut (pause-the-world, không capture inflight)
        local opb_log="$LOG_DIR/bundle2a_p${parts}_N${N}_r${r}_b1.log"
        "$BIN_OPB" \
          --state-backend pebble --state-dir "$local_state_dir" --snapshot-dir "$local_snapshot_dir" \
          --kafka-bootstrap "$BOOTSTRAP" --group-id "bundle2a-b1-p${parts}-N${N}-r${r}" \
          --input-source kafka --topic-enriched "$TOPIC_ENRICHED" \
          --changelog-dir "$local_changelog_dir" \
          --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
          --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
          --snapshot-interval 0 \
          --disable-barrier-cut \
          --http :8089 --instance-id "B1-bundle2a" > "$opb_log" 2>&1 &
        local opb_pid=$!
        if ! wait_ready "http://127.0.0.1:8089/healthz" 180; then
          say "ERROR: B1 không lên healthz, xem log $opb_log"
          kill "$opb_pid" 2>/dev/null || true
          continue
        fi

        # Đợi lag≈0 để đảm bảo base events đã được consume
        for ((i=1;i<=300;i++)); do
          lag=$(get_lag_total "http://127.0.0.1:8089/status")
          if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 1 )); then break; fi
          sleep 1
        done

        # Cắt snapshot full (pause-the-world, không capture inflight)
        say "Cut snapshot full (pause-the-world, không capture inflight)"
        curl -s -X POST "http://127.0.0.1:8089/admin/snapshot-cut?type=full" >/dev/null || true
        # Chờ manifest.latest.json xuất hiện
        for ((i=1;i<=60;i++)); do
          [[ -f "$local_snapshot_dir/manifest.latest.json" ]] && break
          sleep 1
        done

        # QUAN TRỌNG: Inject backlog events SAU khi cut snapshot
        # (Baseline không có inflight, nên backlog này sẽ phải replay từ Kafka tail)
        if (( BACKLOG_FACTOR > 0 )); then
          local batches=$BACKLOG_FACTOR
          say "Inject backlog events SAU khi cut (tạo Kafka tail để replay sau)"
          for ((b=1;b<=batches;b++)); do
            say "Inject backlog batch $b/$batches"
            "$GEN_BIN" --mode kafka \
              --bootstrap "$BOOTSTRAP" --topic "$TOPIC_ENRICHED" \
              --stores "$stores" --products "$products" \
              --n-per-key "$per_key" --window-size "$WINDOW_SIZE" --linger-ms 10
          done
          # Đợi B1 consume backlog events và viết vào changelog
          say "Đợi B1 consume backlog events và viết vào changelog..."
          for ((i=1;i<=180;i++)); do
            lag=$(get_lag_total "http://127.0.0.1:8089/status")
            if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 10 )); then
              say "B1 đã consume backlog events (lag=$lag), đợi flush changelog vào Kafka..."
              break
            fi
            sleep 1
          done
          # Đợi B1 flush changelog vào Kafka
          say "Đợi 15s để B1 flush changelog vào Kafka..."
          sleep 15
        fi

        # Kill B1 để chuẩn bị restore-only
        kill "$opb_pid" 2>/dev/null || true
        sleep 2

        # Restore-only với --restore-force-replay (baseline luôn replay tail)
        local restore_log="$LOG_DIR/bundle2a_p${parts}_N${N}_r${r}_restore.log"
        local restore_state_dir="${local_state_dir}.restore"
        "$BIN_OPB" \
          --state-backend pebble --state-dir "$restore_state_dir" --snapshot-dir "$local_snapshot_dir" \
          --kafka-bootstrap "$BOOTSTRAP" --group-id "bundle2a-restore-p${parts}-N${N}-r${r}" \
          --input-source kafka --topic-enriched "$TOPIC_ENRICHED" \
          --changelog-dir "${local_changelog_dir}.restore" \
          --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
          --manifest-sink file --manifest-source file --changelog-sink kafka --changelog-source kafka \
          --snapshot-interval 0 \
          --window-size "$WINDOW_SIZE" \
          --restore-force-replay \
          --restore-on-start --restore-only \
          --http :8092 --instance-id "B1-restore-bundle2a" > "$restore_log" 2>&1 || true

        csv=$(parse_restore_csv "$restore_log" "$restore_state_dir/restore-metrics.json")
        IFS=',' read -r manifest_ms snap_ms changelog_ms total_ms ttr_ms applied skipped <<<"$csv"

        # Baseline: không inflight => inflightEvents=0, tailReplayEvents=applied
        local inflight_events=0
        local tail_replay_events="${applied:-0}"
        printf "RESULT,bundle=2,mode=baseline,parts=%s,N=%s,snapshotMs=%s,changelogMs=%s,totalMs=%s,ttrMs=%s,applied=%s,skipped=%s,inflightEvents=%s,tailReplayEvents=%s\n" \
          "$parts" "$N" "${snap_ms:-}" "${changelog_ms:-}" "${total_ms:-}" "${ttr_ms:-}" "${applied:-}" "${skipped:-}" "$inflight_events" "$tail_replay_events"
      done
    done
  done
}

main "$@"

