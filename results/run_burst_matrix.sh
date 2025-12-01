#!/usr/bin/env bash
set -euo pipefail

OUT_FILE=${1:-results/summary_burst.csv}
PARTS=(4 8 12)
TOTALS=(50000 100000 300000 1000000)
# Optional overrides to run a subset: set SAMPLE_PARTS and SAMPLE_TOTALS in env
if [[ -n "${SAMPLE_PARTS:-}" ]]; then PARTS=($SAMPLE_PARTS); fi
if [[ -n "${SAMPLE_TOTALS:-}" ]]; then TOTALS=($SAMPLE_TOTALS); fi

mkdir -p results runlogs results/matrix

echo "partitions,burst_total_events,ttr_s,restore_ms,replay_s,replay_events,inflight,snapshot_kb,sst_files,inc_files" > "$OUT_FILE"

for p in "${PARTS[@]}"; do
  for T in "${TOTALS[@]}"; do
    per=$(( (T + p - 1) / p ))
    logf="runlogs/matrix-p${p}-T${T}.log"
    echo "[MATRIX] Running p=$p total=$T per_part=$per"
    PROM_URL="" SCENARIO=burst PREWARM_ENABLE=0 HOTSPARE_FAST_SWITCH=0 INTERACTIVE=0 AUTO_Y=1 SLEEP_BEFORE_SHUTDOWN=0 ENRICHED_PARTITIONS="$p" BURST_EVENTS_PER_PART="$per" CUT_TYPE_AFTER_BURST=delta MANIFEST_SOURCE=file \
      ./scripts/demo_recovery.sh > "$logf" 2>&1 || true
    # Archive B3 log per run
    if [[ -f ./logs/recovery_b3.out ]]; then cp ./logs/recovery_b3.out "./logs/recovery_b3.p${p}.T${T}.out" || true; fi
    b3log="./logs/recovery_b3.p${p}.T${T}.out"

    # Archive manifest and inflight per run (avoid overwrite by next run)
    man="./snapshots-recovery/manifest.latest.json"
    sid="" inflight_path="" inflight_cnt=0 channels=0 snap_kb="" sst_files=0 inc_files=0
    if [[ -f "$man" ]]; then
      cp -f "$man" "results/matrix/p${p}-T${T}-manifest.json" || true
    fi
    # Prefer reading from archived per-run manifest copy
    man_copy="results/matrix/p${p}-T${T}-manifest.json"
    if [[ -f "$man_copy" ]]; then
      sid=$(jq -r '.snapshotId // ""' "$man_copy" 2>/dev/null || echo "")
    fi
    if [[ -z "$sid" && -f "$man" ]]; then
      sid=$(jq -r '.snapshotId // ""' "$man" 2>/dev/null || echo "")
    fi
    if [[ -n "$sid" && -f "snapshots-recovery/$sid/manifest.json" ]]; then
      m2="snapshots-recovery/$sid/manifest.json"
      inflight=$(jq -r '.inflightFile // ""' "$m2" 2>/dev/null || echo "")
      if [[ -n "$inflight" && "$inflight" != "null" && -f "snapshots-recovery/$sid/$inflight" ]]; then
        inflight_path="results/matrix/p${p}-T${T}-inflight.json"
        cp -f "snapshots-recovery/$sid/$inflight" "$inflight_path" || true
      fi
      sst_files=$(jq -r '.pebbleSstFiles | length' "$m2" 2>/dev/null || echo 0)
      inc_files=$(jq -r '.pebbleIncrementalFiles | length' "$m2" 2>/dev/null || echo 0)
      # size in KB
      if [[ -d "snapshots-recovery/$sid" ]]; then
        snap_kb=$(du -sk "snapshots-recovery/$sid" 2>/dev/null | awk '{print $1}')
      fi
    fi
    if [[ -n "$inflight_path" && -f "$inflight_path" ]]; then
      # Try new format with inflightEvents in manifest copy first
      inflight_cnt=$(jq -r '([.events[] | length] | add) // 0' "$inflight_path" 2>/dev/null || echo 0)
      if [[ "$inflight_cnt" == "0" || -z "$inflight_cnt" ]]; then
        if [[ -f "$man_copy" ]]; then
          inflight_cnt=$(jq -r '.inflightEvents // 0' "$man_copy" 2>/dev/null || echo 0)
        fi
      fi
      channels=$(jq -r '.channels | length' "$inflight_path" 2>/dev/null || echo 0)
    else
      # No inflight file copied; try manifest copy for count
      if [[ -f "$man_copy" ]]; then
        inflight_cnt=$(jq -r '.inflightEvents // 0' "$man_copy" 2>/dev/null || echo 0)
      fi
    fi

    # Parse restore timings from per-run B3 log
    manifest_ms="" snap_ms="" changelog_ms="" total_ms="" elapsed_ms=""
    if [[ -f "$b3log" ]]; then
      phases=$(grep -F "restore phases:" "$b3log" | tail -n1 | sed -E 's/.*restore phases: //')
      if [[ -n "$phases" ]]; then
        manifest_ms=$(jq -r '.timings.manifestMs // ""' <<<"$phases" 2>/dev/null || echo "")
        snap_ms=$(jq -r '.timings.snapshotTotalMs // ""' <<<"$phases" 2>/dev/null || echo "")
        changelog_ms=$(jq -r '.timings.changelogMs // ""' <<<"$phases" 2>/dev/null || echo "")
        total_ms=$(jq -r '.timings.totalMs // ""' <<<"$phases" 2>/dev/null || echo "")
      fi
      # Prefer elapsedMs from 'restore completed' line as ttr
      elapsed_ms=$(grep -E "restore completed: .*elapsedMs=[0-9]+" "$b3log" | tail -n1 | sed -E 's/.*elapsedMs=([0-9]+).*/\1/' || true)
    fi

    # replay_s
    if grep -q "skipped Kafka replay" "$b3log" 2>/dev/null; then
      replay_s=0
    else
      if [[ -n "${changelog_ms}" && "${changelog_ms}" != "" && "${changelog_ms}" != "null" ]]; then
        replay_s=$(awk -v m="$changelog_ms" 'BEGIN{printf "%.3f", m/1000}')
      else
        replay_s=""
      fi
    fi

    # ttr_s prefer elapsed_ms else total_ms
    if [[ -n "${elapsed_ms}" && "${elapsed_ms}" != "" && "${elapsed_ms}" != "null" ]]; then
      ttr_s=$(awk -v m="$elapsed_ms" 'BEGIN{printf "%.3f", m/1000}')
    elif [[ -n "${total_ms}" && "${total_ms}" != "" && "${total_ms}" != "null" ]]; then
      ttr_s=$(awk -v m="$total_ms" 'BEGIN{printf "%.3f", m/1000}')
    else
      ttr_s=""
    fi

    # restore_ms prefer snapshotTotalMs else elapsed_ms else manifestMs
    if [[ -n "${snap_ms}" && "${snap_ms}" != "" && "${snap_ms}" != "null" ]]; then
      restore_ms="$snap_ms"
    elif [[ -n "${elapsed_ms}" && "${elapsed_ms}" != "" && "${elapsed_ms}" != "null" ]]; then
      restore_ms="$elapsed_ms"
    elif [[ -n "${manifest_ms}" && "${manifest_ms}" != "" && "${manifest_ms}" != "null" ]]; then
      restore_ms="$manifest_ms"
    else
      restore_ms=""
    fi

    # replay_events
    if grep -q "Causal inflight replay applied" "$b3log" 2>/dev/null; then
      replay_events=$(grep "Causal inflight replay applied" "$b3log" | tail -n1 | sed -E 's/.*events=([0-9]+).*/\1/')
    else
      replay_events=0
    fi

    echo "$p,$T,${ttr_s:-},${restore_ms:-},${replay_s:-},${replay_events:-0},${inflight_cnt:-0},${snap_kb:-},${sst_files:-0},${inc_files:-0}" >> "$OUT_FILE"
    echo "[MATRIX] Done p=$p total=$T"
  done
done

echo "CSV written: $OUT_FILE"

