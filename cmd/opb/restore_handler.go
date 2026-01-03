package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"hpb/internal/kafkautil"
	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/opb"
	rf "hpb/internal/restorefs"
	rk "hpb/internal/restorekafka"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// restoreDependencies contains all dependencies needed for restore logic.
type restoreDependencies struct {
	cfg                    Config
	st                     state.Store
	fullSnap               snapshot.Snapshotter
	maniReader             rf.Reader
	snapFormat             snapshot.Format
	snapshotShards         int
	appStatus              *opb.StatusManager
	mreg                   *metrics.Registry
	seedStoreGauges        func()
	resolveSnapshotFormat  func(string) snapshot.Format
	resolveSnapshotShards  func(int) int
	readSnapshotManifest   func(string) (manifest.Manifest, error)
	snapshotSizeBytes      func(string, snapshot.Format, int) float64
	deltaSnapshotSizeBytes func(string, snapshot.Format, int) float64
	snapshotIncrementalBytes func(string, []string) float64
	metricsPath            string
	stateImported          *atomic.Bool
}

// performRestore performs the restore operation (snapshot + changelog replay).
func performRestore(deps restoreDependencies) error {
	if !deps.cfg.RestoreOnStart || deps.stateImported.Load() {
		if !deps.cfg.RestoreOnStart {
			log.Printf("restore: skipped at start (restore-on-start=false)")
		}
		return nil
	}

	restoreTsStart := time.Now()
	log.Printf("restore: starting (source=%s, changelogSource=%s, topicSnapshots=%s) at %s",
		deps.cfg.ManifestSource, deps.cfg.ChangelogSource, deps.cfg.TopicSnapshots, restoreTsStart.Format(time.RFC3339Nano))

	// Read latest manifest with internal reader timeout (no long outer loop)
	manifestStart := time.Now()
	var m manifest.Manifest
	m, mErr := deps.maniReader.ReadLatest()
	phaseTimings := restorePhaseTimings{
		ManifestMs: time.Since(manifestStart).Milliseconds(),
	}

	if mErr != nil || m.SnapshotID == "" {
		// Fallback: try filesystem manifest reader if kafka source fails and FS snapshot exists
		if deps.cfg.SnapshotDir != "" {
			manifestStart = time.Now()
			if m2, e2 := rf.NewFilesystemReader(deps.cfg.SnapshotDir).ReadLatest(); e2 == nil && m2.SnapshotID != "" {
				log.Printf("restore: fallback FS manifest loaded snapshotId=%s lastChangelogOffset=%d", m2.SnapshotID, m2.LastChangelogOffset)
				m, mErr = m2, nil
			}
			phaseTimings.ManifestMs += time.Since(manifestStart).Milliseconds()
		}
	}

	if mErr != nil || m.SnapshotID == "" {
		log.Printf("restore: no manifest found after wait; skipping restore (err=%v, snapshotId=%s)", mErr, m.SnapshotID)
		return nil
	}

	log.Printf("restore: manifest loaded snapshotId=%s lastChangelogOffset=%d", m.SnapshotID, m.LastChangelogOffset)
	deps.appStatus.SetRecovering(m.SnapshotID, m.LastChangelogOffset)
	t0 := time.Now()
	restorer := rf.NewRestorerWithOptions(deps.st, deps.fullSnap, deps.maniReader, deps.cfg.SnapshotDir, deps.snapFormat, deps.snapshotShards)
	restoreFormat := deps.resolveSnapshotFormat(m.SnapshotFormat)
	restoreShards := deps.resolveSnapshotShards(m.SnapshotShards)

	// Always restore snapshot before replaying changelog (supports chain)
	snapshotStart := time.Now()
	var restoreErr error
	if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
		restoreErr = restorer.RestoreChainFromLatestWithOptions(m, rf.RestoreOptions{
			Parallelism:       deps.cfg.RestoreParallelism,
			SkipMissingDelta:  deps.cfg.RestoreSkipMissingDelta,
			ValidateChain:     deps.cfg.RestoreValidateChain,
		})
	} else {
		restoreErr = restorer.RestoreFromSnapshotWithFormatParallel(m.SnapshotID, restoreFormat, restoreShards, m.SnapshotKeys, deps.cfg.RestoreParallelism)
	}

	if restoreErr != nil {
		log.Printf("restore snapshot error: %v", restoreErr)
		if deps.cfg.RestoreOnly {
			return fmt.Errorf("restore failed: %w", restoreErr)
		}
		return nil
	}

	phaseTimings.SnapshotTotalMs = time.Since(snapshotStart).Milliseconds()
	if stats := restorer.LastSnapshotStats(); stats.Shards >= 0 {
		phaseTimings.SnapshotReadMs = stats.ReadNs / int64(time.Millisecond)
		phaseTimings.SnapshotDecodeMs = stats.DecodeNs / int64(time.Millisecond)
		phaseTimings.SnapshotLoadMs = stats.LoadNs / int64(time.Millisecond)
	}
	log.Printf("restore: snapshot restored snapshotId=%s type=%s base=%s dseq=%d", m.SnapshotID, m.SnapshotType, m.BaseSnapshotID, m.DeltaSequence)

	var causalReplayEvents int64
	var inflightEventCount int
	var inflightChannelCount int
	if m.InflightFile != "" {
		if snap, ierr := readInflightSnapshot(deps.cfg.SnapshotDir, m.SnapshotID, m.InflightFile); ierr != nil {
			log.Printf("restore: inflight read error: %v", ierr)
		} else {
			var replayTotal int
			for _, evs := range snap.Events {
				replayTotal += len(evs)
			}
			if deps.cfg.SkipInflightReplay {
				log.Printf("restore: skip inflight replay: channels=%d events=%d (flag set)", len(snap.Events), replayTotal)
				// Still record the inflight count for status/metrics even when replay is skipped (stage-2 restart).
				causalReplayEvents = int64(replayTotal)
				inflightEventCount = replayTotal
				inflightChannelCount = len(snap.Events)
				if inflightChannelCount == 0 && snap.Channels != nil {
					inflightChannelCount = len(snap.Channels)
				}
			} else if err := replayInflightEvents(deps.cfg, deps.st, snap); err != nil {
				log.Printf("restore: inflight replay error: %v", err)
			} else if len(snap.Events) > 0 {
				deps.mreg.CausalReplay.Add(float64(replayTotal))
				deps.appStatus.AddCausalReplay(int64(replayTotal))
				causalReplayEvents = int64(replayTotal)
				inflightEventCount = replayTotal
				inflightChannelCount = len(snap.Events)
				if inflightChannelCount == 0 && snap.Channels != nil {
					inflightChannelCount = len(snap.Channels)
				}
				log.Printf("restore: inflight replay applied channels=%d events=%d", len(snap.Events), replayTotal)
			}
		}
	}

	var result rf.RestoreResult
	// replayFn captures the exact replay function (Kafka or file) used for the
	// initial changelog replay so that we can optionally invoke it multiple
	// additional times on the already-restored state store (Bundle 2 EOS demo).
	var replayFn func() rf.RestoreResult
	replayedOnce := false
	var changelogStart time.Time

	if deps.cfg.ChangelogSource == "kafka" && deps.cfg.KafkaBootstrap != "" {
		var skipKafkaReplay bool
		if deps.cfg.RestoreTrustManifest && manifestAllowsReplaySkip(m) {
			skipKafkaReplay = true
			log.Printf("restore: freeze hint => skipping changelog replay (manifest replayRequired=false)")
		} else if m.Changelog != nil && m.Changelog.Topic != "" && len(m.Changelog.Offsets) > 0 {
			if hasBacklog, err := kafkautil.ChangelogHasBacklog(deps.cfg.KafkaBootstrap, m.Changelog.Topic, m.Changelog.Offsets); err != nil {
				log.Printf("restore: changelog backlog check error: %v", err)
			} else if !hasBacklog {
				skipKafkaReplay = true
				log.Printf("restore: skipping changelog replay (no backlog beyond manifest offsets)")
			}
		}
		if !skipKafkaReplay {
			if m.Changelog != nil && m.Changelog.Topic != "" && len(m.Changelog.Offsets) > 0 {
				replayFn = func() rf.RestoreResult {
					return rk.ReplayChangelogKafkaParallel(deps.st, []string{deps.cfg.KafkaBootstrap}, m.Changelog.Topic, m.Changelog.Offsets, 0, deps.cfg.ReplayWorkers)
				}
			} else {
				replayFn = func() rf.RestoreResult {
					return rk.ReplayChangelogKafkaParallel(deps.st, []string{deps.cfg.KafkaBootstrap}, deps.cfg.TopicChangelog, nil, m.LastChangelogOffset, deps.cfg.ReplayWorkers)
				}
			}
			changelogStart = time.Now()
			result = replayFn()
			replayedOnce = true
		}
	} else {
		// file mode
		changelogStart = time.Now()
		replayFn = func() rf.RestoreResult {
			return restorer.ReplayChangelog(fmt.Sprintf("%s/opb.jsonl", deps.cfg.ChangelogDir), m.LastChangelogOffset)
		}
		result = replayFn()
		replayedOnce = true
	}

	if !changelogStart.IsZero() {
		phaseTimings.ChangelogMs = time.Since(changelogStart).Milliseconds()
	}

	if result.Error != nil {
		log.Printf("restore replay error: %v", result.Error)
		if deps.cfg.RestoreOnly {
			return fmt.Errorf("replay failed: %w", result.Error)
		}
		return nil
	}

	// Log the primary replay pass (pass=1). This is the canonical restore.
	if replayedOnce {
		log.Printf("bundle2: replay pass=%d applied=%d skipped=%d", 1, result.Applied, result.Skipped)
	}

	// For EOS/idempotent replay demos (Bundle 2), optionally run the same
	// replay function multiple additional times against the already
	// restored state store. With an idempotent backend (Pebble+LastSeq),
	// subsequent passes should have applied=0 and skipped>0.
	if deps.cfg.RestoreOnly && deps.cfg.ReplayExtraPasses > 1 && replayFn != nil && replayedOnce {
		for pass := 2; pass <= deps.cfg.ReplayExtraPasses; pass++ {
			extra := replayFn()
			if extra.Error != nil {
				log.Printf("bundle2: replay pass=%d error=%v", pass, extra.Error)
				break
			}
			log.Printf("bundle2: replay pass=%d applied=%d skipped=%d", pass, extra.Applied, extra.Skipped)
		}
	}

	elapsed := time.Since(t0)
	deps.appStatus.SetRecovered(elapsed, int64(result.Applied), int64(result.Skipped))
	// After restore, seed per-store gauges for Prometheus
	deps.seedStoreGauges()
	restoreTsDone := time.Now()
	log.Printf("restore completed: applied=%d skipped=%d elapsedMs=%.0f finishedAt=%s", result.Applied, result.Skipped, elapsed.Seconds()*1000, restoreTsDone.Format(time.RFC3339Nano))
	log.Printf("restore ts: start=%s done=%s", restoreTsStart.Format(time.RFC3339Nano), restoreTsDone.Format(time.RFC3339Nano))

	metricsStart := time.Now()
	// Finalize phase timings before persisting them to restore-metrics.json.
	// NOTE: phaseTimings is a value type; if we copy it into restoreMetrics before filling TotalMs/MetricsMs,
	// the JSON file will show an empty "phases" object (all fields omitted).
	phaseTimings.TotalMs = time.Since(restoreTsStart).Milliseconds()
	phaseTimings.MetricsMs = time.Since(metricsStart).Milliseconds()

	newMetrics := restoreMetrics{
		SnapshotID:          m.SnapshotID,
		LastChangelogOffset: m.LastChangelogOffset,
		Applied:             int64(result.Applied),
		Skipped:             int64(result.Skipped),
		TTRMs:               time.Since(t0).Milliseconds(),
		CausalReplayEvents:  causalReplayEvents,
		InflightEvents:      inflightEventCount,
		InflightChannels:    inflightChannelCount,
		UpdatedAt:           time.Now().UTC(),
		Phases:              phaseTimings,
	}

	shouldWrite := true
	if prevMetrics, err := readRestoreMetrics(deps.metricsPath); err == nil {
		if prevMetrics.SnapshotID == newMetrics.SnapshotID &&
			prevMetrics.Applied == newMetrics.Applied &&
			prevMetrics.Skipped == newMetrics.Skipped &&
			prevMetrics.CausalReplayEvents == newMetrics.CausalReplayEvents {
			shouldWrite = false
		}
	}
	if shouldWrite {
		if err := writeRestoreMetrics(deps.metricsPath, newMetrics); err != nil {
			log.Printf("restore history: write error: %v", err)
		}
	}

	// Expose "Last Restore Summary" metrics to Prometheus for viz panels.
	// Labels: instance, snapshot_id, snapshot_type, format.
	formatLabel := string(restoreFormat)
	if formatLabel == "" {
		formatLabel = m.SnapshotFormat
	}
	lbls := []string{deps.cfg.InstanceID, m.SnapshotID, m.SnapshotType, formatLabel}
	if deps.mreg.LastRestoreTTRSeconds != nil {
		deps.mreg.LastRestoreTTRSeconds.WithLabelValues(lbls...).Set(float64(newMetrics.TTRMs) / 1000.0)
	}
	// Lưu các metric phụ thuộc vào phaseTimings sau khi đã tính MetricsMs/TotalMs ở bên dưới.
	if deps.mreg.LastRestoreReplaySeconds != nil {
		deps.mreg.LastRestoreReplaySeconds.WithLabelValues(lbls...).Set(float64(phaseTimings.ChangelogMs) / 1000.0)
	}
	if deps.mreg.LastRestoreReplayEvents != nil {
		deps.mreg.LastRestoreReplayEvents.WithLabelValues(lbls...).Set(float64(result.Applied + result.Skipped))
	}
	if deps.mreg.LastRestoreSnapshotBytes != nil {
		var snapBytes float64
		// Compute size using on-disk snapshot files; fall back to listed SSTs/incremental files.
		if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
			snapBytes = deps.deltaSnapshotSizeBytes(m.SnapshotID, restoreFormat, restoreShards)
			if snapBytes == 0 && len(m.PebbleIncrementalFiles) > 0 {
				snapBytes = deps.snapshotIncrementalBytes(m.SnapshotID, m.PebbleIncrementalFiles)
			}
		} else {
			snapBytes = deps.snapshotSizeBytes(m.SnapshotID, restoreFormat, restoreShards)
			if snapBytes == 0 && len(m.PebbleSSTFiles) > 0 {
				snapBytes = deps.snapshotIncrementalBytes(m.SnapshotID, m.PebbleSSTFiles)
			}
		}
		deps.mreg.LastRestoreSnapshotBytes.WithLabelValues(lbls...).Set(snapBytes)
	}
	if deps.mreg.LastRestoreSSTFilesTotal != nil {
		deps.mreg.LastRestoreSSTFilesTotal.WithLabelValues(lbls...).Set(float64(len(m.PebbleAllFiles)))
	}
	if deps.mreg.LastRestoreIncrementalFiles != nil {
		deps.mreg.LastRestoreIncrementalFiles.WithLabelValues(lbls...).Set(float64(len(m.PebbleIncrementalFiles)))
	}
	if deps.mreg.LastRestoreInflightReplayed != nil {
		deps.mreg.LastRestoreInflightReplayed.WithLabelValues(lbls...).Set(float64(newMetrics.InflightEvents))
	}
	if deps.mreg.LastRestoreEOSOK != nil {
		// Treat a successful restore (no replay error) as EOS OK=1.
		deps.mreg.LastRestoreEOSOK.WithLabelValues(lbls...).Set(1)
	}
	// Use the finalized timings already embedded in newMetrics.
	if deps.mreg.LastRestoreRestoreOnlyMs != nil {
		deps.mreg.LastRestoreRestoreOnlyMs.WithLabelValues(lbls...).Set(float64(newMetrics.Phases.TotalMs))
	}
	if newMetrics.Phases.TotalMs > 0 {
		line := map[string]any{
			"phase":   "restore-phases",
			"timings": newMetrics.Phases,
		}
		if b, err := json.Marshal(line); err == nil {
			log.Printf("restore phases: %s", b)
		}
	}

	if deps.cfg.RestoreOnly {
		log.Printf("restore-only: exiting after successful restore")
		return nil
	}

	return nil
}

