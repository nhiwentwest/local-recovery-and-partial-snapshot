package metrics

import (
	"testing"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegistry_CountersAndHistograms(t *testing.T) {
	r := NewRegistry()

	// Counters
	r.TxProduced.Inc()
	r.TxAborted.Add(2)
	r.ChangelogAppended.Add(3)

	if got, want := promtest.ToFloat64(r.TxProduced), 1.0; got != want {
		t.Fatalf("TxProduced: got=%v want=%v", got, want)
	}
	if got, want := promtest.ToFloat64(r.TxAborted), 2.0; got != want {
		t.Fatalf("TxAborted: got=%v want=%v", got, want)
	}
	if got, want := promtest.ToFloat64(r.ChangelogAppended), 3.0; got != want {
		t.Fatalf("ChangelogAppended: got=%v want=%v", got, want)
	}

	// Histograms: observe a few values and ensure count > 0
	r.TxLatencySec.Observe(0.01)
	r.TxLatencySec.Observe(0.02)
	r.TxBatchDurationSec.Observe(0.05)

	// promtest.CollectAndCount returns number of samples across metrics
	if n := promtest.CollectAndCount(r.TxLatencySec); n == 0 {
		t.Fatalf("TxLatencySec histogram has no samples")
	}
	if n := promtest.CollectAndCount(r.TxBatchDurationSec); n == 0 {
		t.Fatalf("TxBatchDurationSec histogram has no samples")
	}

	// GaugeVec: can set one sample
	r.PartitionLag.WithLabelValues("topic", "0", "group", "inst").Set(5)
	if n := promtest.CollectAndCount(r.PartitionLag); n == 0 {
		t.Fatalf("PartitionLag has no samples")
	}
}
