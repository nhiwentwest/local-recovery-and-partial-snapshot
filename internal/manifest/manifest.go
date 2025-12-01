package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Manifest struct {
	SnapshotID           string            `json:"snapshotId"`
	SnapshotFormat       string            `json:"snapshotFormat,omitempty"`
	SnapshotShards       int               `json:"snapshotShards,omitempty"`
	SnapshotKeys         int               `json:"snapshotKeys,omitempty"`
	SnapshotBytes        int64             `json:"snapshotBytes,omitempty"`
	SnapshotType         string            `json:"snapshotType,omitempty"`     // full|delta
	BaseSnapshotID       string            `json:"baseSnapshotId,omitempty"`   // nearest full
	ParentSnapshotID     string            `json:"parentSnapshotId,omitempty"` // previous in chain
	DeltaSequence        int               `json:"deltaSequence,omitempty"`    // order in chain since base
	LastChangelogOffset  int64             `json:"lastChangelogOffset"`        // legacy total count; kept for backward compat
	CreatedAtEpochSecond int64             `json:"createdAt"`
	Changelog            *OffsetsInfo      `json:"changelog,omitempty"`
	Channels             []string          `json:"channels,omitempty"`
	InflightFile         string            `json:"inflightFile,omitempty"`
	InflightEvents       int               `json:"inflightEvents,omitempty"`
	SnapshotVectorClock  map[string]uint64 `json:"vectorClock,omitempty"`
	ReplayRequired       *bool             `json:"replayRequired,omitempty"`

	// Pebble-specific fields (experimental SSTable shipping backend).
	// When SnapshotFormat == "pebble", these describe the exported SSTables for the snapshot.
	PebbleSSTFiles      []string          `json:"pebbleSstFiles,omitempty"`
	PebbleFormatVersion string            `json:"pebbleFormatVersion,omitempty"`
	PebbleSSTChecksums  map[string]string `json:"pebbleSstChecksums,omitempty"`
	// Phase 3: Incremental checkpoint metadata.
	PebbleIncrementalFiles []string `json:"pebbleIncrementalFiles,omitempty"` // new files in this snapshot
	PebbleAllFiles         []string `json:"pebbleAllFiles,omitempty"`         // all files (for reference)
}

const (
	SnapshotTypeFull  = "full"
	SnapshotTypeDelta = "delta"
)

// OffsetsInfo holds per-partition offsets for the changelog topic.
// Offsets are exclusive (i.e., start reading from this offset).
type OffsetsInfo struct {
	Topic      string  `json:"topic"`
	Partitions int     `json:"partitions"`
	Offsets    []int64 `json:"offsets"`
}

type Publisher interface {
	PublishLatest(snapshotID string, lastChangelogOffset int64) error
}

// FullPublisher publishes a full manifest with per-partition offsets.
// Implementations should also implement Publisher for backward compatibility during migration.
type FullPublisher interface {
	Publish(m Manifest) error
}

// MultiPublisher writes to multiple publishers sequentially.
type MultiPublisherImpl struct {
	pubs []Publisher
}

func MultiPublisher(pubs ...Publisher) Publisher {
	return &MultiPublisherImpl{pubs: pubs}
}

// PublishLatest satisfies Publisher for MultiPublisherImpl.
func (m *MultiPublisherImpl) PublishLatest(snapshotID string, lastChangelogOffset int64) error {
	for _, p := range m.pubs {
		if err := p.PublishLatest(snapshotID, lastChangelogOffset); err != nil {
			return err
		}
	}
	return nil
}

// Publish satisfies FullPublisher for MultiPublisherImpl when underlying publishers support it.
// If a publisher does not implement FullPublisher, falls back to PublishLatest with minimal fields.
func (m *MultiPublisherImpl) Publish(man Manifest) error {
	for _, p := range m.pubs {
		if fp, ok := p.(FullPublisher); ok {
			if err := fp.Publish(man); err != nil {
				return err
			}
			continue
		}
		// Fallback minimal publish
		if err := p.PublishLatest(man.SnapshotID, man.LastChangelogOffset); err != nil {
			return err
		}
	}
	return nil
}

type Reader interface {
	ReadLatest() (Manifest, error)
}

type FilesystemManifest struct {
	baseDir string
}

func NewFilesystemManifest(baseDir string) *FilesystemManifest {
	return &FilesystemManifest{baseDir: baseDir}
}

func (f *FilesystemManifest) writeFile(m Manifest) error {
	if err := os.MkdirAll(f.baseDir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	file := filepath.Join(f.baseDir, "manifest.latest.json")
	out, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&m); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	// Also archive a copy under the snapshot directory for chain restore if SnapshotID is present
	if m.SnapshotID != "" {
		sdir := filepath.Join(f.baseDir, m.SnapshotID)
		if err := os.MkdirAll(sdir, 0o755); err == nil {
			_ = os.WriteFile(filepath.Join(sdir, "manifest.json"), []byte(""), 0o644) // ensure file exists even if below fails
			b, _ := json.MarshalIndent(m, "", "  ")
			_ = os.WriteFile(filepath.Join(sdir, "manifest.json"), b, 0o644)
		}
	}
	return nil
}

func (f *FilesystemManifest) PublishLatest(snapshotID string, lastChangelogOffset int64) error {
	m := Manifest{
		SnapshotID:           snapshotID,
		LastChangelogOffset:  lastChangelogOffset,
		CreatedAtEpochSecond: time.Now().UTC().Unix(),
	}
	return f.writeFile(m)
}

// Publish publishes full manifest including per-partition offsets when available.
func (f *FilesystemManifest) Publish(m Manifest) error {
	if m.CreatedAtEpochSecond == 0 {
		m.CreatedAtEpochSecond = time.Now().UTC().Unix()
	}
	return f.writeFile(m)
}

func (f *FilesystemManifest) ReadLatest() (Manifest, error) {
	file := filepath.Join(f.baseDir, "manifest.latest.json")
	data, err := os.ReadFile(file)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return m, nil
}

// KafkaManifest publishes manifest.latest as a compacted Kafka record.
type KafkaManifest struct {
	writer kafkaMessageWriter
	key    []byte
}

// kafkaMessageWriter abstracts kafka.Writer for testability.
type kafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// NewKafkaManifest creates a Kafka manifest publisher.
// bootstrap can be comma-separated brokers. key is typically "opb-manifest-latest".
func NewKafkaManifest(bootstrap string, topic string, key string) *KafkaManifest {
	addrs := strings.Split(bootstrap, ",")
	var brokers []string
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a != "" {
			brokers = append(brokers, a)
		}
	}
	return &KafkaManifest{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}, key: []byte(key)}
}

func (k *KafkaManifest) PublishLatest(snapshotID string, lastChangelogOffset int64) error {
	m := Manifest{
		SnapshotID:           snapshotID,
		LastChangelogOffset:  lastChangelogOffset,
		CreatedAtEpochSecond: time.Now().UTC().Unix(),
	}
	return k.Publish(m)
}

// Publish publishes full manifest including per-partition offsets when available.
func (k *KafkaManifest) Publish(m Manifest) error {
	if m.CreatedAtEpochSecond == 0 {
		m.CreatedAtEpochSecond = time.Now().UTC().Unix()
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return k.writer.WriteMessages(context.Background(), kafka.Message{Key: k.key, Value: b})
}

// NewKafkaManifestWith is only for tests to inject a fake writer.
func NewKafkaManifestWith(w kafkaMessageWriter, key string) *KafkaManifest {
	return &KafkaManifest{writer: w, key: []byte(key)}
}
