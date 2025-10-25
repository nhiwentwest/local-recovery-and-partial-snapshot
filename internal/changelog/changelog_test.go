package changelog

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestFileWriter_Append(t *testing.T) {
	dir := t.TempDir()
	w, err := NewFileWriter(dir, "opb.jsonl")
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}

	d1 := Delta{Key: "A#p1#100", Seq: 1, Delta: 100, TS: 1}
	d2 := Delta{Key: "A#p2#100", Seq: 2, Delta: 200, TS: 2}
	if err := w.Append(d1); err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w.Append(d2); err != nil {
		t.Fatalf("append2: %v", err)
	}

	fpath := filepath.Join(dir, "opb.jsonl")
	f, err := os.Open(fpath)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	var got []Delta
	for s.Scan() {
		var d Delta
		if err := json.Unmarshal(s.Bytes(), &d); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		got = append(got, d)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 lines, got %d", len(got))
	}
	if got[0] != d1 || got[1] != d2 {
		t.Fatalf("mismatch: %+v vs %+v,%+v", got, d1, d2)
	}
}

// fakeKafkaWriter implements kafkaMessageWriter for tests
type fakeKafkaWriter struct {
	msgs []kafka.Message
	fail bool
}

func (f *fakeKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if f.fail {
		return errors.New("fail")
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func TestKafkaWriter_Append_Success(t *testing.T) {
	fk := &fakeKafkaWriter{}
	kw := NewKafkaWriterWith(fk)
	d := Delta{Key: "K#1", Seq: 1, Delta: 10, DeltaQty: 1, TS: 1}
	if err := kw.Append(d); err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(fk.msgs) != 1 {
		t.Fatalf("want 1 msg, got %d", len(fk.msgs))
	}
	if string(fk.msgs[0].Key) != d.Key {
		t.Fatalf("bad key: %s", string(fk.msgs[0].Key))
	}
}

func TestKafkaWriter_Append_Fail(t *testing.T) {
	fk := &fakeKafkaWriter{fail: true}
	kw := NewKafkaWriterWith(fk)
	d := Delta{Key: "K#1", Seq: 1, Delta: 10, DeltaQty: 1, TS: 1}
	if err := kw.Append(d); err == nil {
		t.Fatalf("expected error")
	}
}
