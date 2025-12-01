package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry struct {
	reg                      *prometheus.Registry
	Applied                  prometheus.Counter
	Skipped                  prometheus.Counter
	TTRSec                   prometheus.Gauge
	ReplayBytes              prometheus.Counter
	ReplayRecords            prometheus.Counter
	Lag                      prometheus.Gauge
	LastManifestAgeSec       prometheus.Gauge
	SnapshotTimeMs           prometheus.Histogram
	SnapshotBytes            prometheus.Gauge
	SnapshotIncrementalBytes prometheus.Gauge
	SnapshotIncrementalFiles prometheus.Gauge

	// OpB transactional metrics
	TxProduced         prometheus.Counter
	TxAborted          prometheus.Counter
	TxLatencySec       prometheus.Histogram
	TxBatchDurationSec prometheus.Histogram
	OffsetsBoundLag    prometheus.Gauge
	ChangelogAppended  prometheus.Counter

	// Per-partition lag (labels: topic, partition, group, instance)
	PartitionLag *prometheus.GaugeVec

	// New EOS fast-path counters
	EventsApplied      prometheus.Counter
	EventsSkippedDedup prometheus.Counter
	EventsSkippedSeq   prometheus.Counter
	CausalInflight     prometheus.Gauge
	CausalReplay       prometheus.Counter

	// Per-store live aggregates (for zone viz)
	StoreSumQty    *prometheus.GaugeVec
	StoreSumAmount *prometheus.GaugeVec

	// Last-restore Prometheus gauges (labeled): instance, snapshot_id, snapshot_type, format
	LastRestoreTTRSeconds       *prometheus.GaugeVec
	LastRestoreRestoreOnlyMs    *prometheus.GaugeVec
	LastRestoreReplaySeconds    *prometheus.GaugeVec
	LastRestoreReplayEvents     *prometheus.GaugeVec
	LastRestoreSnapshotBytes    *prometheus.GaugeVec
	LastRestoreSSTFilesTotal    *prometheus.GaugeVec
	LastRestoreIncrementalFiles *prometheus.GaugeVec
	LastRestoreInflightReplayed *prometheus.GaugeVec
	LastRestoreEOSOK            *prometheus.GaugeVec
}

func NewRegistry() *Registry {
	r := prometheus.NewRegistry()
	applied := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_applied_total"})
	skipped := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_skipped_total"})
	ttr := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_recovery_ttr_seconds"})
	replayBytes := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_bytes_total"})
	replayRecords := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_records_total"})
	lag := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_changelog_lag"})
	lastAge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_last_manifest_age_seconds"})
	snapTime := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "opb_snapshot_time_ms", Buckets: prometheus.ExponentialBuckets(10, 2, 8)})
	snapBytes := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_snapshot_bytes"})
	snapIncrBytes := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_snapshot_incremental_bytes"})
	snapIncrFiles := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_snapshot_incremental_files"})

	txProduced := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_tx_produced_total"})
	txAborted := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_tx_aborted_total"})
	txLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opb_tx_latency_seconds",
		Buckets: prometheus.DefBuckets,
	})
	txBatchDur := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opb_tx_batch_duration_seconds",
		Buckets: prometheus.DefBuckets,
	})
	offsetsBoundLag := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_offsets_bound_lag"})
	changelogAppended := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_changelog_appended_total"})

	partLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_partition_lag"}, []string{"topic", "partition", "group", "instance"})

	// New EOS counters
	evApplied := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_events_applied_total"})
	evSkipDedup := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_events_skipped_dedup_total"})
	evSkipSeq := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_events_skipped_seq_total"})

	causalInflight := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_causal_inflight"})
	causalReplay := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_causal_replay_total"})

	// Per-store gauges
	storeSumQty := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_store_sum_qty"}, []string{"storeId"})
	storeSumAmount := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_store_sum_amount"}, []string{"storeId"})

	// Last restore labeled gauges
	labels := []string{"instance", "snapshot_id", "snapshot_type", "format"}
	lastTTR := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_ttr_seconds"}, labels)
	lastRestoreOnly := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_restore_only_ms"}, labels)
	lastReplaySecs := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_replay_seconds"}, labels)
	lastReplayEvents := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_replay_events"}, labels)
	lastSnapBytes := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_snapshot_bytes"}, labels)
	lastSstTotal := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_sst_files_total"}, labels)
	lastIncFiles := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_incremental_files"}, labels)
	lastInflight := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_inflight_replayed"}, labels)
	lastEOS := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_last_restore_eos_ok"}, labels)

	r.MustRegister(applied, skipped, ttr, replayBytes, replayRecords, lag, lastAge, snapTime, snapBytes, snapIncrBytes, snapIncrFiles, txProduced, txAborted, txLatency, txBatchDur, offsetsBoundLag, changelogAppended, partLag, evApplied, evSkipDedup, evSkipSeq, causalInflight, causalReplay, storeSumQty, storeSumAmount,
		lastTTR, lastRestoreOnly, lastReplaySecs, lastReplayEvents, lastSnapBytes, lastSstTotal, lastIncFiles, lastInflight, lastEOS)
	return &Registry{
		reg:                      r,
		Applied:                  applied,
		Skipped:                  skipped,
		TTRSec:                   ttr,
		ReplayBytes:              replayBytes,
		ReplayRecords:            replayRecords,
		Lag:                      lag,
		LastManifestAgeSec:       lastAge,
		SnapshotTimeMs:           snapTime,
		SnapshotBytes:            snapBytes,
		SnapshotIncrementalBytes: snapIncrBytes,
		SnapshotIncrementalFiles: snapIncrFiles,
		TxProduced:               txProduced,
		TxAborted:                txAborted,
		TxLatencySec:             txLatency,
		TxBatchDurationSec:       txBatchDur,
		OffsetsBoundLag:          offsetsBoundLag,
		ChangelogAppended:        changelogAppended,
		PartitionLag:             partLag,
		EventsApplied:            evApplied,
		EventsSkippedDedup:       evSkipDedup,
		EventsSkippedSeq:         evSkipSeq,
		CausalInflight:           causalInflight,
		CausalReplay:             causalReplay,
		StoreSumQty:              storeSumQty,
		StoreSumAmount:           storeSumAmount,

		LastRestoreTTRSeconds:       lastTTR,
		LastRestoreRestoreOnlyMs:    lastRestoreOnly,
		LastRestoreReplaySeconds:    lastReplaySecs,
		LastRestoreReplayEvents:     lastReplayEvents,
		LastRestoreSnapshotBytes:    lastSnapBytes,
		LastRestoreSSTFilesTotal:    lastSstTotal,
		LastRestoreIncrementalFiles: lastIncFiles,
		LastRestoreInflightReplayed: lastInflight,
		LastRestoreEOSOK:            lastEOS,
	}
}

func (r *Registry) Handler() http.Handler { return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{}) }
