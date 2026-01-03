package main

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"hpb/internal/kafkautil"
	"hpb/internal/manifest"
	"hpb/internal/opb"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// wireOperatorComplete wires op.Complete. This is a direct extraction of the
// previous inline implementation in runMultiInputRuntime (no behaviour change).
func wireOperatorComplete(
	op *opb.DynamicNInputOperator,
	cfg Config,
	st state.Store,
	snap pebbleSnapshotViewAdapter,
	mani manifest.Publisher,
	activeCuts *activeCutsMap,
) {
	op.Complete = func(id string, inflight map[string][]opb.Event) {
		activeCuts.mu.Lock()
		cutCtx, ok := activeCuts.m[id]
		if ok {
			delete(activeCuts.m, id)
		}
		activeCuts.mu.Unlock()
		if !ok {
			log.Printf("mi event=complete status=error id=%s err=no_cut_context", id)
			return
		}

		var totalInflight int
		for _, evs := range inflight {
			totalInflight += len(evs)
		}
		log.Printf("mi event=complete id=%s type=%s channels=%d inflightEvents=%d", id, cutCtx.cutType, len(inflight), totalInflight)

		// Build current changelog offsets (needed for delta dirty-keys scan)
		var offInfo *manifest.OffsetsInfo
		if cfg.KafkaBootstrap != "" && cfg.TopicChangelog != "" {
			if offs, parts, err := kafkautil.CollectChangelogOffsets(cfg.KafkaBootstrap, cfg.TopicChangelog); err == nil {
				offInfo = &manifest.OffsetsInfo{Topic: cfg.TopicChangelog, Partitions: parts, Offsets: offs}
			} else {
				log.Printf("mi event=snapshot stage=collect-offsets status=error id=%s err=%v", id, err)
			}
		}

		// Snapshot from a point-in-time view
		view, err := st.NewSnapshotView()
		if err != nil {
			log.Printf("mi event=snapshot stage=view status=error id=%s err=%v", id, err)
			return
		}
		defer view.Close()

		var meta snapshot.Result
		var serr error
		mtype := manifest.SnapshotTypeFull
		var baseID, parentID string
		var dseq int

		t0 := time.Now()
		if cutCtx.cutType == manifest.SnapshotTypeDelta {
			// Validate prev manifest and offsets
			if cutCtx.prev == nil || cutCtx.prev.Changelog == nil || offInfo == nil || offInfo.Topic == "" || cutCtx.prev.Changelog.Topic == "" {
				log.Printf("mi event=snapshot stage=delta-skip id=%s reason=missing-prev-or-offsets", id)
				meta, serr = snap.WriteSnapshotFromView(id, view)
				mtype = manifest.SnapshotTypeFull
			} else {
				// Determine base/parent and sequence
				parentID = cutCtx.prev.SnapshotID
				if cutCtx.prev.SnapshotType == manifest.SnapshotTypeDelta && cutCtx.prev.BaseSnapshotID != "" {
					baseID = cutCtx.prev.BaseSnapshotID
					dseq = cutCtx.prev.DeltaSequence + 1
				} else {
					baseID = cutCtx.prev.SnapshotID
					dseq = 1
				}
				// Scan dirty keys between prev and current changelog offsets
				keys, kerr := kafkautil.ScanDirtyKeysKafka([]string{cfg.KafkaBootstrap}, cutCtx.prev.Changelog.Topic, cutCtx.prev.Changelog.Offsets, offInfo.Offsets, 0, 1500*time.Millisecond)
				if kerr != nil {
					log.Printf("mi event=snapshot stage=dirty-scan status=error id=%s err=%v", id, kerr)
					meta, serr = snap.WriteSnapshotFromView(id, view)
					mtype = manifest.SnapshotTypeFull
				} else {
					log.Printf("mi event=snapshot stage=delta-start id=%s dirtyKeys=%d", id, len(keys))
					meta, serr = snap.WriteDeltaSnapshotFromView(id, view, keys)
					mtype = manifest.SnapshotTypeDelta
				}
			}
		} else {
			meta, serr = snap.WriteSnapshotFromView(id, view)
		}
		durMs := time.Since(t0).Milliseconds()

		if serr != nil {
			log.Printf("mi event=snapshot stage=write status=error id=%s type=%s err=%v", id, mtype, serr)
			return
		}
		st.MarkSnapshotDone()

		var channels []string
		for ch := range inflight {
			channels = append(channels, ch)
		}
		inflightRecords := make(map[string][]inflightRecord, len(inflight))
		for ch, evs := range inflight {
			for _, ev := range evs {
				rec := inflightRecord{Key: ev.Key}
				if ev.VC != nil {
					rec.VC = ev.VC.Copy()
				}
				inflightRecords[ch] = append(inflightRecords[ch], rec)
			}
		}
		relInflight, inflightCount, inflightErr := writeInflightSnapshot(cfg.SnapshotDir, id, channels, inflightRecords)
		if inflightErr != nil {
			log.Printf("mi event=inflight stage=write status=error id=%s err=%v", id, inflightErr)
		} else if inflightCount > 0 {
			log.Printf("mi event=inflight stage=write status=ok id=%s count=%d", id, inflightCount)
		}

		// Log delta size for tuning (if delta)
		if mtype == manifest.SnapshotTypeDelta {
			var totalBytes int64
			if meta.Shards <= 1 {
				fp := filepath.Join(cfg.SnapshotDir, id, meta.Format.FileNameDelta())
				if fi, err := os.Stat(fp); err == nil {
					totalBytes += fi.Size()
				}
			} else {
				for i := 0; i < meta.Shards; i++ {
					fp := filepath.Join(cfg.SnapshotDir, id, meta.Format.FileNameDeltaForShard(i, meta.Shards))
					if fi, err := os.Stat(fp); err == nil {
						totalBytes += fi.Size()
					}
				}
			}
			log.Printf("mi event=snapshot stage=delta-metrics id=%s keys=%d bytes=%d durMs=%d", id, meta.Keys, totalBytes, durMs)
		}

		m := manifest.Manifest{
			SnapshotID:           id,
			SnapshotFormat:       meta.Format.String(),
			SnapshotShards:       meta.Shards,
			SnapshotKeys:         meta.Keys,
			SnapshotType:         mtype,
			BaseSnapshotID:       baseID,
			ParentSnapshotID:     parentID,
			DeltaSequence:        dseq,
			CreatedAtEpochSecond: time.Now().UTC().Unix(),
			Changelog:            offInfo,
			Channels:             channels,
			InflightFile:         relInflight,
			InflightEvents:       inflightCount,
		}
		// Set Pebble-specific fields if format is pebble
		if meta.Format == snapshot.FormatPebble {
			m.PebbleSSTFiles = meta.PebbleSSTFiles
			m.PebbleFormatVersion = meta.PebbleFormatVersion
			m.PebbleSSTChecksums = meta.PebbleSSTChecksums
			m.PebbleIncrementalFiles = meta.PebbleIncrementalFiles
			m.PebbleAllFiles = meta.PebbleSSTFiles
		}
		// Publish manifest with one retry
		publishOnce := func() error {
			if fp, ok := mani.(manifest.FullPublisher); ok {
				return fp.Publish(m)
			}
			return mani.PublishLatest(id, 0)
		}
		perr := publishOnce()
		if perr != nil {
			time.Sleep(200 * time.Millisecond)
			perr = publishOnce()
		}
		if perr != nil {
			log.Printf("mi event=manifest stage=publish status=error id=%s err=%v", id, perr)
			return
		}
		log.Printf("mi event=manifest stage=publish status=ok id=%s type=%s shards=%d keys=%d", id, mtype, meta.Shards, meta.Keys)
	}
}

