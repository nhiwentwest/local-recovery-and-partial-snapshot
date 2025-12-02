#!/usr/bin/env bash
set -euo pipefail

# Bundle 2B – Tech causal inflight + tail replay (ít hơn baseline)
# Mục tiêu:
# - Có inflight snapshot → replay ít hơn từ Kafka tail
# - So sánh với baseline không inflight → phải replay nhiều hơn từ tail
# - In CSV: parts,N,snapshotMs,changelogMs,totalMs,ttrMs,applied,skipped,inflightEvents,tailReplayEvents=applied-inflightEvents

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
BACKLOG_FACTOR="${BACKLOG_FACTOR:-1}" # số batch backlog ~ BACKLOG_FACTOR * N (inject TRƯỚC khi barrier cut)
BACKLOG_PER_KEY_DELTA="${BACKLOG_PER_KEY_DELTA:-1}" # tăng n-per-key cho backlog để tránh trùng orderId -> không bị dedup
POSTTAIL_FACTOR="${POSTTAIL_FACTOR:-0}"      # số batch tail events sau khi barrier finalize (tạo Kafka tail thật sự)
POSTTAIL_PER_KEY="${POSTTAIL_PER_KEY:-1}"   # n-per-key cho tail
FAST_RESUME="${FAST_RESUME:-1}"       # 1: resume ngay sau POST delta để backlog đi trước barrier
RESUME_PRIME_SEC="${RESUME_PRIME_SEC:-1}" # ngủ ngắn sau resume để consumer bắt đầu đọc backlog

mkdir -p "$LOG_DIR"

say() { printf "\n\e[1;36m[BUNDLE2B]\e[0m %s\n" "$*"; }

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
  local url=${1:-http://127.0.0.1:${HTTP_PORT:-8089}/status}
  local data lag
  data=$(curl -s "$url" || true)
  lag=$(printf '%s' "$data" | sed -n 's/.*"lagTotal"[[:space:]]*:[[:space:]]*\([0-9.][0-9.]*\).*/\1/p' | head -n1)
  if [[ -z "$lag" || "$lag" == "null" ]]; then echo 0; else printf '%.0f' "$lag"; fi
}

http_ok() { curl -sf "$1" >/dev/null 2>&1; }

fail() { say "ERROR: $*"; exit 1; }

post_snapshot_cut() {
  local type=${1:-full}
  local url="http://127.0.0.1:${HTTP_PORT:-8089}/admin/snapshot-cut?type=${type}"
  local out
  out=$(curl -s -f -X POST "$url" 2>&1) || fail "snapshot-cut POST failed (type=${type}): $out"
  if command -v jq >/dev/null 2>&1; then
    local st
    st=$(jq -r '.status // empty' <<<"$out" 2>/dev/null || echo "")
    [[ "$st" == "accepted" ]] || fail "snapshot-cut not accepted (type=${type}): $out"
  else
    echo "$out" | grep -q 'accepted' || fail "snapshot-cut not accepted (type=${type}): $out"
  fi
  echo "$out"
}

wait_causal_cut() {
  local timeout=${1:-20}
  for ((i=1;i<=timeout*10;i++)); do
    local j
    j=$(curl -s "http://127.0.0.1:${HTTP_PORT:-8089}/status" || echo "{}")
    if command -v jq >/dev/null 2>&1; then
      if [[ "$(jq -r '.causalCutId // ""' <<<"$j")" != "" ]]; then return 0; fi
    else
      if grep -q '"causalCutId"' <<<"$j"; then return 0; fi
    fi
    sleep 0.1
  done
  return 1
}

wait_manifest_offsets() {
  # Đợi manifest.latest.json có .changelog.offsets.
  # Sau khi đã vá FilesystemManifest.PublishLatest để không làm "mỏng" latest nữa,
  # script không cần fallback per-snapshot ở đây – chỉ tin vào latest cho sạch mô hình.
  local file=$1; local timeout=${2:-60}
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$file" ]]; then
      if command -v jq >/dev/null 2>&1; then
        if jq -e '.changelog.offsets|length>0' "$file" >/dev/null 2>&1; then return 0; fi
      else
        # minimal check: file exists; caller có thể kiểm tra sâu hơn nếu cần
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

wait_ready() {
  local url=$1; local n=${2:-180}
  for ((i=0;i<n;i++)); do
    if http_ok "$url"; then return 0; fi
    sleep 1
  done
  return 1
}

wait_manifest_inflight() {
  local dir=$1
  local timeout=${2:-30}
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$dir/manifest.latest.json" ]]; then
      local inflight sid
      inflight=$(jq -r '.inflightFile // ""' "$dir/manifest.latest.json" 2>/dev/null || echo "")
      sid=$(jq -r '.snapshotId // ""' "$dir/manifest.latest.json" 2>/dev/null || echo "")
      if [[ -n "$inflight" && "$inflight" != "null" ]]; then
        echo "$inflight"
        return 0
      fi
      # Nếu manifest đã có snapshotId nhưng không có inflightFile → đã finalized, không có inflight
      if [[ -n "$sid" && "$sid" != "null" ]]; then
        # Đã có snapshotId nhưng không có inflightFile → không có inflight, return ngay
        return 1
      fi
    fi
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
    if [[ -z "$applied" || "$applied" == "" || "$applied" == "null" ]]; then
      applied=$(jq -r '.applied // ""' "$metricsf" 2>/dev/null || echo "")
    fi
    if [[ -z "$skipped" || "$skipped" == "" || "$skipped" == "null" ]]; then
      skipped=$(jq -r '.skipped // ""' "$metricsf" 2>/dev/null || echo "")
    fi
  fi

  echo "${manifest_ms:-},${snap_ms:-},${changelog_ms:-},${total_ms:-},${ttr_ms:-},${applied:-},${skipped:-}"
}

get_inflight_events_count() {
  local inflight_path=$1
  if [[ -f "$inflight_path" ]]; then
    if command -v jq >/dev/null 2>&1; then
      jq '(.events | map(length) | add) // 0' "$inflight_path" 2>/dev/null || echo 0
    else
      grep -c '"key"' "$inflight_path" 2>/dev/null || echo 0
    fi
  else
    echo 0
  fi
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

        # Start OpB instance với barrier cut (mặc định, sẽ capture inflight)
        local opb_log="$LOG_DIR/bundle2b_p${parts}_N${N}_r${r}_b1.log"
        OPB_DEBUG="${OPB_DEBUG:-1}" "$BIN_OPB" \
          --state-backend pebble --state-dir "$local_state_dir" --snapshot-dir "$local_snapshot_dir" \
          --kafka-bootstrap "$BOOTSTRAP" --group-id "bundle2b-b1-p${parts}-N${N}-r${r}" \
          --input-source kafka --topic-enriched "$TOPIC_ENRICHED" \
          --changelog-dir "$local_changelog_dir" \
          --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
          --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
          --snapshot-interval 0 \
          --enable-pebble-phase3 \
          --http :${HTTP_PORT:-8089} --instance-id "${INSTANCE_ID:-B1-bundle2b}" > "$opb_log" 2>&1 &
        local opb_pid=$!
        if ! wait_ready "http://127.0.0.1:${HTTP_PORT:-8089}/healthz" 180; then
          say "ERROR: B1 không lên healthz, xem log $opb_log"
          kill "$opb_pid" 2>/dev/null || true
          continue
        fi

        # Đợi lag≈0 để đảm bảo base events đã được consume
        for ((i=1;i<=300;i++)); do
          lag=$(get_lag_total "http://127.0.0.1:${HTTP_PORT:-8089}/status")
          if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 1 )); then break; fi
          sleep 1
        done

        # Pre-cut: tạo base full snapshot khi ingest đang chạy để đảm bảo delta có prev manifest với offsets
        say "Pre-cut: Creating base full snapshot (ingestion running)"
        # Pre-cut: với việc FilesystemManifest luôn giữ offsets trong latest,
        # ta chỉ cần một lần cut và chờ manifest.latest.json có .changelog.offsets.
          post_snapshot_cut full >/dev/null || true
        if ! wait_manifest_offsets "$local_snapshot_dir/manifest.latest.json" 240; then
          fail "base full snapshot manifest missing offsets in manifest.latest.json"
        fi
        say "✓ Base full snapshot published with offsets in manifest.latest.json"

        # QUAN TRỌNG: Barrier cut với inflight capture
        # Step 1: Pause ingest để tạo backlog
        say "Step 1: Pausing ingestion to build backlog"
        curl -s -X POST "http://127.0.0.1:${HTTP_PORT:-8089}/admin/ingest/pause" >/dev/null || true
        sleep 0.5

        # Step 2: Inject backlog events (sẽ bị capture bởi barrier)
        # QUAN TRỌNG: Inject backlog events TRƯỚC khi trigger barrier cut
        # Để đảm bảo backlog events nằm trong Kafka TRƯỚC barrier
        if (( BACKLOG_FACTOR > 0 )); then
          say "Step 2: Injecting backlog events (while paused, sẽ bị capture bởi barrier)"
          for ((b=1;b<=BACKLOG_FACTOR;b++)); do
            "$GEN_BIN" --mode kafka \
              --bootstrap "$BOOTSTRAP" --topic "$TOPIC_ENRICHED" \
              --stores "$stores" --products "$products" \
              --n-per-key "${BACKLOG_PER_KEY_DELTA}" --window-size "$WINDOW_SIZE" --linger-ms 10 --source delta
          done
          # Đợi events được publish vào Kafka (đảm bảo backlog events có trong Kafka)
          say "  Đợi backlog events được publish vào Kafka..."
          sleep 2
        fi

        # Step 3: Trigger barrier cut (delta snapshot)
        # QUAN TRỌNG: Barrier được inject SAU backlog events trong Kafka
        # Khi resume, consumer sẽ consume backlog events TRƯỚC barrier → record vào inflight
        say "Step 3: Triggering barrier cut (delta snapshot) để capture inflight"
        post_snapshot_cut delta >/dev/null

        # Step 4: Xác nhận currentCut đã được tạo (dựa trên /status.causalCutId), sau đó RESUME (nếu FAST_RESUME=1)
        # Mục tiêu: resume càng sớm càng tốt ngay khi currentCut tồn tại để backlog được ghi vào inflight,
        # nhưng tránh race khi currentCut chưa được tạo.
        say "Step 4: Waiting for causalCutId appear..."
        local have_cut=0
        local WAIT_CUT_SEC=${WAIT_CUT_SEC:-20}
        for ((i=1;i<=WAIT_CUT_SEC*10;i++)); do
          local j
          j=$(curl -s "http://127.0.0.1:${HTTP_PORT:-8089}/status" || echo "{}")
          if command -v jq >/dev/null 2>&1; then
            if [[ "$(jq -r '.causalCutId // ""' <<<"$j")" != "" ]]; then have_cut=1; break; fi
          else
            if grep -q '"causalCutId"' <<<"$j"; then have_cut=1; break; fi
          fi
          sleep 0.1
        done
        if [[ $have_cut -eq 1 ]]; then
          say "✓ causalCutId present"
        else
          say "WARN: causalCutId not observed (proceeding)"
        fi

        if [[ "${FAST_RESUME}" == "1" ]]; then
          say "Step 5: FAST_RESUME=1 → resuming ingestion immediately"
          curl -s -X POST "http://127.0.0.1:${HTTP_PORT:-8089}/admin/ingest/resume" >/dev/null || true
          if [[ "${RESUME_PRIME_SEC}" =~ ^[0-9]+$ && "${RESUME_PRIME_SEC}" -gt 0 ]]; then
            say "  Prime ${RESUME_PRIME_SEC}s to let backlog flow before barriers are seen"
            sleep "${RESUME_PRIME_SEC}"
          fi
        else
          # Legacy: chờ log barrier injected rồi mới resume
          say "Step 5: Legacy resume path (waiting barrier injected log)"
          local barrier_injected=0
          for ((i=1;i<=30;i++)); do
            if grep -q "snapshot-cut: barrier injected" "$opb_log" 2>/dev/null; then
              say "✓ Barrier messages injected (barrier nằm SAU backlog events trong Kafka)"
              barrier_injected=1
              break
            fi
            sleep 0.2
          done
          if [[ $barrier_injected -eq 0 ]]; then
            say "WARN: Barrier injection not confirmed in logs, proceeding anyway"
          fi
          # Đợi thêm một chút để đảm bảo currentCut đã được tạo
          sleep 1
          say "Resuming ingestion now"
          curl -s -X POST "http://127.0.0.1:${HTTP_PORT:-8089}/admin/ingest/resume" >/dev/null || true
        fi

        # Đợi consumer bắt đầu consume backlog events (trước khi barrier đến)
        say "  Đợi consumer consume backlog events (trước khi barrier đến)..."
        sleep 2

        # Đợi barrier cut finalized và manifest có inflightFile
        # Sử dụng status endpoint để theo dõi barrier cut progress (giống demo_recovery.sh)
        say "Step 6: Waiting for barrier cut to finalize..."
        local causal_finalized=0
        for ((i=1;i<=180;i++)); do
          local status_data
          status_data=$(curl -s "http://127.0.0.1:${HTTP_PORT:-8089}/status" || echo "{}")
          local causal_id causal_phase causal_seen causal_total
          if command -v jq >/dev/null 2>&1; then
            causal_id=$(jq -r '.causalCutId // ""' <<<"$status_data" 2>/dev/null || echo "")
            causal_phase=$(jq -r '.causalPhase // ""' <<<"$status_data" 2>/dev/null || echo "")
            causal_seen=$(jq -r '.causalMarkersSeen // 0' <<<"$status_data" 2>/dev/null || echo "0")
            causal_total=$(jq -r '.causalMarkersTotal // 0' <<<"$status_data" 2>/dev/null || echo "0")
          else
            causal_id=""
            causal_phase=""
            causal_seen=0
            causal_total=0
          fi
          # Nếu không còn causalCutId trong status → barrier cut đã finalized
          if [[ -z "$causal_id" || "$causal_id" == "null" || "$causal_id" == "" ]]; then
            say "✓ Barrier cut finalized (no causalCutId in status)"
            causal_finalized=1
            break
          fi
          if (( i % 10 == 0 )); then
            say "  [${i}/180] Waiting... causalCutId=$causal_id phase=$causal_phase markers=$causal_seen/$causal_total"
          fi
          sleep 1
        done
        if [[ $causal_finalized -eq 0 ]]; then
          say "WARN: Barrier cut may not have finalized within timeout"
        fi

        # Đợi manifest có inflightFile (hoặc chấp nhận nếu không có)
        say "Step 7: Checking manifest for inflightFile..."
        local inflight_file=""
        # Đợi ngắn hơn (30s) vì có thể không có inflight
        inflight_file=$(wait_manifest_inflight "$local_snapshot_dir" 30)
        if [[ -z "$inflight_file" ]]; then
          say "INFO: inflightFile not found in manifest after barrier cut"
          say "  This may happen if backlog events were consumed before barriers arrived"
          say "  Continuing with inflightEvents=0 (barrier cut still performed, just no inflight captured)"
        else
          say "✓ Found inflightFile in manifest: $inflight_file"
        fi

        # Verify inflight.json tồn tại và có events
        local snapshot_id
        snapshot_id=$(jq -r '.snapshotId // ""' "$local_snapshot_dir/manifest.latest.json" 2>/dev/null || echo "")
        local inflight_path="$local_snapshot_dir/${snapshot_id:-}/$inflight_file"
        # Wait up to 15s for inflight.json to be materialized on disk
        for ((i=1;i<=15;i++)); do
          [[ -f "$inflight_path" ]] && break
          sleep 1
        done
        local inflight_events=0
        if [[ -f "$inflight_path" ]]; then
          inflight_events=$(get_inflight_events_count "$inflight_path")
          say "✓ Causal snapshot captured: $inflight_events inflight events"
        else
          say "WARN: inflight.json not found at $inflight_path"
          # Try to find alternative path
          local alt_inflight
          alt_inflight=$(find "$local_snapshot_dir" -maxdepth 2 -name "inflight.json" 2>/dev/null | head -n1 || true)
          if [[ -n "$alt_inflight" && -f "$alt_inflight" ]]; then
            inflight_path="$alt_inflight"
            inflight_events=$(get_inflight_events_count "$inflight_path")
            say "✓ Found inflight.json at alternative path: $alt_inflight ($inflight_events events)"
          fi
        fi

        # Step 7b: Inject post-tail (sau barrier finalize) để tạo Kafka tail thật sự
        if (( POSTTAIL_FACTOR > 0 )); then
          say "Step 7b: Injecting POST-TAIL events (after barrier finalize)"
          for ((b=1;b<=POSTTAIL_FACTOR;b++)); do
            "$GEN_BIN" --mode kafka \
              --bootstrap "$BOOTSTRAP" --topic "$TOPIC_ENRICHED" \
              --stores "$stores" --products "$products" \
              --n-per-key "${POSTTAIL_PER_KEY}" --window-size "$WINDOW_SIZE" --linger-ms 10 --source postCut
          done
          say "  Waiting 3s for tail to settle in Kafka high watermarks..."
          sleep 3
        fi

        # Đợi B1 consume backlog events và viết vào changelog (nếu còn)
        say "Đợi B1 consume backlog events và viết vào changelog..."
        for ((i=1;i<=180;i++)); do
          lag=$(get_lag_total "http://127.0.0.1:${HTTP_PORT:-8089}/status")
          if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 10 )); then
            say "B1 đã consume backlog events (lag=$lag), đợi flush changelog vào Kafka..."
            break
          fi
          sleep 1
        done
        say "Đợi 15s để B1 flush changelog vào Kafka..."
        sleep 15

        # Kill B1 để chuẩn bị restore-only
        kill "$opb_pid" 2>/dev/null || true
        sleep 2

        # Restore-only (mặc định, sẽ replay cả inflight và tail)
        local restore_log="$LOG_DIR/bundle2b_p${parts}_N${N}_r${r}_restore.log"
        local restore_state_dir="${local_state_dir}.restore"
        OPB_DEBUG="${OPB_DEBUG:-1}" "$BIN_OPB" \
          --state-backend pebble --state-dir "$restore_state_dir" --snapshot-dir "$local_snapshot_dir" \
          --kafka-bootstrap "$BOOTSTRAP" --group-id "bundle2b-restore-p${parts}-N${N}-r${r}" \
          --input-source kafka --topic-enriched "$TOPIC_ENRICHED" \
          --changelog-dir "${local_changelog_dir}.restore" \
          --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
          --manifest-sink file --manifest-source file --changelog-sink kafka --changelog-source kafka \
          --snapshot-interval 0 \
          --window-size "$WINDOW_SIZE" \
          --restore-on-start --restore-only \
          --http :8092 --instance-id "B1-restore-bundle2b" > "$restore_log" 2>&1 || true

        csv=$(parse_restore_csv "$restore_log" "$restore_state_dir/restore-metrics.json")
        IFS=',' read -r manifest_ms snap_ms changelog_ms total_ms ttr_ms applied skipped <<<"$csv"

        # Tech: có inflight => tailReplayEvents = applied - inflightEvents
        local tail_replay_events=0
        if [[ -n "${applied:-}" ]] && [[ "$applied" =~ ^[0-9]+$ ]]; then
          tail_replay_events=$(( applied - inflight_events ))
          if (( tail_replay_events < 0 )); then tail_replay_events=0; fi
        fi

        printf "RESULT,bundle=2,mode=tech,parts=%s,N=%s,snapshotMs=%s,changelogMs=%s,totalMs=%s,ttrMs=%s,applied=%s,skipped=%s,inflightEvents=%s,tailReplayEvents=%s,postTailFactor=%s\n" \
          "$parts" "$N" "${snap_ms:-}" "${changelog_ms:-}" "${total_ms:-}" "${ttr_ms:-}" "${applied:-}" "${skipped:-}" "$inflight_events" "$tail_replay_events" "${POSTTAIL_FACTOR}"
      done
    done
  done
}

main "$@"

