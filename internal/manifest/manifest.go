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
	SnapshotID           string `json:"snapshotId"`
	LastChangelogOffset  int64  `json:"lastChangelogOffset"`
	CreatedAtEpochSecond int64  `json:"createdAt"`
}

type Publisher interface {
	PublishLatest(snapshotID string, lastChangelogOffset int64) error
}

// MultiPublisher writes to multiple publishers sequentially.
type MultiPublisherImpl struct {
	pubs []Publisher
}

func MultiPublisher(pubs ...Publisher) Publisher {
	return &MultiPublisherImpl{pubs: pubs}
}

func (m *MultiPublisherImpl) PublishLatest(snapshotID string, lastChangelogOffset int64) error {
	for _, p := range m.pubs {
		if err := p.PublishLatest(snapshotID, lastChangelogOffset); err != nil {
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

func (f *FilesystemManifest) PublishLatest(snapshotID string, lastChangelogOffset int64) error {
	if err := os.MkdirAll(f.baseDir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	m := Manifest{
		SnapshotID:           snapshotID,
		LastChangelogOffset:  lastChangelogOffset,
		CreatedAtEpochSecond: time.Now().UTC().Unix(),
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
	return nil
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
