package changelog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/segmentio/kafka-go"
)

type Delta struct {
	Key      string `json:"key"`
	Seq      int64  `json:"seq"`
	Delta    int64  `json:"delta"` // amount delta
	DeltaQty int64  `json:"deltaQty,omitempty"`
	TS       int64  `json:"ts"`
}

type Writer interface {
	Append(d Delta) error
}

// MultiWriter fans out writes to multiple underlying writers.
type MultiWriter struct {
	writers []Writer
}

func NewMultiWriter(ws ...Writer) *MultiWriter {
	return &MultiWriter{writers: ws}
}

func (m *MultiWriter) Append(d Delta) error {
	for _, w := range m.writers {
		if err := w.Append(d); err != nil {
			return err
		}
	}
	return nil
}

type FileWriter struct {
	path string
}

func NewFileWriter(dir string, filename string) (*FileWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	return &FileWriter{path: filepath.Join(dir, filename)}, nil
}

func (w *FileWriter) Append(d Delta) error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err := enc.Encode(&d); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return nil
}

// KafkaWriter publishes deltas to a Kafka topic. Pure-Go client (segmentio/kafka-go).
type KafkaWriter struct {
	writer kafkaMessageWriter
}

// kafkaMessageWriter abstracts kafka.Writer for testability.
type kafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// NewKafkaWriter creates a Kafka writer.
// bootstrap can be a comma-separated list of host:port.
func NewKafkaWriter(bootstrap string, topic string) *KafkaWriter {
	addrs := strings.Split(bootstrap, ",")
	var brokers []string
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a != "" {
			brokers = append(brokers, a)
		}
	}
	return &KafkaWriter{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}}
}

func (k *KafkaWriter) Append(d Delta) error {
	b, err := json.Marshal(&d)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return k.writer.WriteMessages(
		context.Background(),
		kafka.Message{Key: []byte(d.Key), Value: b},
	)
}

// NewKafkaWriterWith is only for tests to inject a fake writer.
func NewKafkaWriterWith(w kafkaMessageWriter) *KafkaWriter {
	return &KafkaWriter{writer: w}
}
