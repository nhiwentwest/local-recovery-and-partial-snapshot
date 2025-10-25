package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry struct {
	reg                    *prometheus.Registry
	Applied                prometheus.Counter
	Skipped                prometheus.Counter
	TTRSec                 prometheus.Gauge
	ReplayBytes            prometheus.Counter
	Lag                    prometheus.Gauge
	LastManifestAgeSec     prometheus.Gauge
	ReplayRate             prometheus.Gauge
	SnapshotRestoreTimeSec prometheus.Gauge
	SnapshotSizeBytes      prometheus.Gauge

	// OpB transactional metrics
	TxProduced        prometheus.Counter
	TxAborted         prometheus.Counter
	TxLatencySec      prometheus.Histogram
	ChangelogAppended prometheus.Counter

	// Per-partition lag (labels: topic, partition, group, instance)
	PartitionLag *prometheus.GaugeVec
}

func NewRegistry() *Registry {
	r := prometheus.NewRegistry()

	// Recovery metrics
	applied := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_applied_total"})
	skipped := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_skipped_total"})
	ttr := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_recovery_ttr_seconds"})
	replayBytes := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_replay_bytes_total"})
	lag := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_changelog_lag"})
	lastAge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_last_manifest_age_seconds"})
	replayRate := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_replay_rate_records_per_sec"})
	snapshotRestoreTime := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_snapshot_restore_time_seconds"})
	snapshotSizeBytes := prometheus.NewGauge(prometheus.GaugeOpts{Name: "opb_snapshot_size_bytes"})

	// OpB transactional metrics
	txProduced := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_tx_produced_total"})
	txAborted := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_tx_aborted_total"})
	txLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opb_tx_latency_seconds",
		Buckets: prometheus.DefBuckets,
	})
	changelogAppended := prometheus.NewCounter(prometheus.CounterOpts{Name: "opb_changelog_appended_total"})

	partLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "opb_partition_lag"}, []string{"topic", "partition", "group", "instance"})

	r.MustRegister(
		applied, skipped, ttr, replayBytes, lag, lastAge, replayRate,
		snapshotRestoreTime, snapshotSizeBytes,
		txProduced, txAborted, txLatency, changelogAppended, partLag,
	)

	return &Registry{
		reg:                    r,
		Applied:                applied,
		Skipped:                skipped,
		TTRSec:                 ttr,
		ReplayBytes:            replayBytes,
		Lag:                    lag,
		LastManifestAgeSec:     lastAge,
		ReplayRate:             replayRate,
		SnapshotRestoreTimeSec: snapshotRestoreTime,
		SnapshotSizeBytes:      snapshotSizeBytes,
		TxProduced:             txProduced,
		TxAborted:              txAborted,
		TxLatencySec:           txLatency,
		ChangelogAppended:      changelogAppended,
		PartitionLag:           partLag,
	}
}

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}
