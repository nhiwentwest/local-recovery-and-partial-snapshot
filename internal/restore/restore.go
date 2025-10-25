package restore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

type Restorer struct {
	stateStore      state.Store
	snapshotter     snapshot.Snapshotter
	manifestReader  manifest.Reader
	snapshotBaseDir string
	metrics         *metrics.Registry
}

type Reader interface {
	ReadLatest() (manifest.Manifest, error)
}

type FilesystemReader struct {
	baseDir string
}

func NewFilesystemReader(baseDir string) *FilesystemReader {
	return &FilesystemReader{baseDir: baseDir}
}

func (r *FilesystemReader) ReadLatest() (manifest.Manifest, error) {
	file := filepath.Join(r.baseDir, "manifest.latest.json")
	data, err := os.ReadFile(file)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m manifest.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest.Manifest{}, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return m, nil
}

// KafkaReader reads latest manifest record from a compacted Kafka topic.
type KafkaReader struct {
	brokers []string
	topic   string
	key     []byte
}

func NewKafkaReader(brokers []string, topic string, key string) *KafkaReader {
	return &KafkaReader{brokers: brokers, topic: topic, key: []byte(key)}
}

func (k *KafkaReader) ReadLatest() (manifest.Manifest, error) {
	// Use a kafka.Reader with StartOffset = Last and scan backwards to find the key
	// Simpler approach: read from beginning and keep last seen for key (ok for small dev topics)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   k.brokers,
		Topic:     k.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var last manifest.Manifest
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return manifest.Manifest{}, fmt.Errorf("read kafka: %w", err)
		}
		if string(m.Key) != string(k.key) {
			continue
		}
		var man manifest.Manifest
		if err := json.Unmarshal(m.Value, &man); err != nil {
			return manifest.Manifest{}, fmt.Errorf("unmarshal kafka manifest: %w", err)
		}
		last = man
	}
	if last.SnapshotID == "" {
		return manifest.Manifest{}, fmt.Errorf("no manifest found for key")
	}
	return last, nil
}

func NewRestorer(st state.Store, snap snapshot.Snapshotter, mr manifest.Reader, snapshotBaseDir string, m *metrics.Registry) *Restorer {
	return &Restorer{
		stateStore:      st,
		snapshotter:     snap,
		manifestReader:  mr,
		snapshotBaseDir: snapshotBaseDir,
		metrics:         m,
	}
}

func (r *Restorer) UpdateRecoveryMetrics(result RestoreResult, startTime time.Time) {
	if r.metrics == nil {
		return
	}

	duration := time.Since(startTime)
	r.metrics.TTRSec.Set(duration.Seconds())
	r.metrics.Applied.Add(float64(result.Applied))
	r.metrics.Skipped.Add(float64(result.Skipped))
	r.metrics.ReplayBytes.Add(float64(result.Bytes))

	// Calculate replay rate (records per second)
	totalRecords := result.Applied + result.Skipped
	if duration.Seconds() > 0 {
		rate := float64(totalRecords) / duration.Seconds()
		r.metrics.ReplayRate.Set(rate)
	}

	// Update lag to 0 after recovery completes
	r.metrics.Lag.Set(0)

	log.Printf("Recovery metrics updated: TTR=%.3fs, Applied=%d, Skipped=%d, Bytes=%d, Rate=%.2f records/s",
		duration.Seconds(), result.Applied, result.Skipped, result.Bytes,
		float64(totalRecords)/duration.Seconds())
}

type RestoreResult struct {
	Applied int
	Skipped int
	// Bytes is the total bytes of replayed deltas (Kafka/file)
	Bytes int64
	// LastAppliedOffset is the Kafka offset of the last applied delta (Kafka only)
	LastAppliedOffset int64
	Error             error
}

func (r *Restorer) RestoreFromSnapshot(snapshotID string) error {
	// Phase 1: simple restore from JSON snapshot
	if snapshotID == "" {
		return nil
	}

	path := filepath.Join(r.snapshotBaseDir, snapshotID, "state.json")

	// Measure snapshot file size
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("restore: snapshot not found at %s, skipping", path)
			return nil
		}
		return fmt.Errorf("read snapshot: %w", err)
	}
	snapshotSize := fileInfo.Size()

	// Measure restore time
	startTime := time.Now()
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	var dump map[string]state.RecordState
	if err := json.Unmarshal(data, &dump); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	r.stateStore.LoadAll(dump)
	restoreDuration := time.Since(startTime)

	// Update snapshot metrics
	if r.metrics != nil {
		r.metrics.SnapshotRestoreTimeSec.Set(restoreDuration.Seconds())
		r.metrics.SnapshotSizeBytes.Set(float64(snapshotSize))
	}

	log.Printf("restore: loaded %d keys from snapshot %s (size: %d bytes, time: %v)",
		len(dump), snapshotID, snapshotSize, restoreDuration)
	return nil
}

func (r *Restorer) ReplayChangelog(changelogPath string, fromOffset int64) RestoreResult {
	file, err := os.Open(changelogPath)
	if err != nil {
		return RestoreResult{Error: fmt.Errorf("open changelog: %w", err)}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	applied, skipped := 0, 0
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		if int64(lineNum) <= fromOffset {
			continue
		}

		var d changelog.Delta
		if err := json.Unmarshal(scanner.Bytes(), &d); err != nil {
			return RestoreResult{Error: fmt.Errorf("unmarshal line %d: %w", lineNum, err)}
		}

		ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
		if err != nil {
			return RestoreResult{Error: fmt.Errorf("apply line %d: %w", lineNum, err)}
		}
		if ok {
			applied++
		} else {
			skipped++
		}
	}

	if err := scanner.Err(); err != nil {
		return RestoreResult{Error: fmt.Errorf("scan changelog: %w", err)}
	}

	return RestoreResult{Applied: applied, Skipped: skipped}
}

// ReplayChangelogKafka consumes deltas from Kafka topic (partition 0) and applies them.
// fromOffset here is interpreted as message index (dev simplification).
func (r *Restorer) ReplayChangelogKafka(brokers []string, topic string, fromOffset int64) RestoreResult {
	// For now, we'll use the existing partition-based approach
	// In Phase 2, we can enhance this to use consumer groups properly
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0, // Start with partition 0 for simplicity
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer rd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applied, skipped := 0, 0
	var bytes int64
	var lastOffset int64 = -1
	idx := int64(0)

	for {
		m, err := rd.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // Context timeout, normal exit
			}
			return RestoreResult{Applied: applied, Skipped: skipped, Error: fmt.Errorf("read kafka: %w", err)}
		}

		idx++
		if idx <= fromOffset {
			continue
		}

		var d changelog.Delta
		if err := json.Unmarshal(m.Value, &d); err != nil {
			return RestoreResult{Applied: applied, Skipped: skipped, Error: fmt.Errorf("unmarshal delta: %w", err)}
		}

		ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
		if err != nil {
			return RestoreResult{Applied: applied, Skipped: skipped, Error: fmt.Errorf("apply: %w", err)}
		}

		if ok {
			applied++
		} else {
			skipped++
		}

		bytes += int64(len(m.Value))
		lastOffset = m.Offset
	}

	return RestoreResult{Applied: applied, Skipped: skipped, Bytes: bytes, LastAppliedOffset: lastOffset}
}

func (r *Restorer) RestoreAndReplay() (RestoreResult, error) {

	startTime := time.Now()
	// Read latest manifest
	m, err := r.manifestReader.ReadLatest()
	if err != nil {
		return RestoreResult{}, fmt.Errorf("read manifest: %w", err)
	}

	// Person 3: Get initial consumer lag (if using Kafka)

	// Restore from snapshot
	if err := r.RestoreFromSnapshot(m.SnapshotID); err != nil {
		return RestoreResult{}, fmt.Errorf("restore snapshot: %w", err)
	}

	// By default use file-based replay (callers can invoke Kafka variant directly if needed)
	result := r.ReplayChangelog("./changelog/opb.jsonl", m.LastChangelogOffset)

	r.UpdateRecoveryMetrics(result, startTime)

	return result, result.Error
}
