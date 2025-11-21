package state

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInMemory_LastUpdatedBy(t *testing.T) {
	s := NewInMemoryStore()
	s.SetInstanceID("B1")
	applied, st, err := s.Apply("A#p1#100", 10, 1, 1)
	if err != nil || !applied {
		t.Fatalf("apply err=%v applied=%v", err, applied)
	}
	if st.LastUpdatedBy != "B1" {
		t.Fatalf("want LastUpdatedBy=B1 got %q", st.LastUpdatedBy)
	}
	// Get retains transient field for InMemory (stored in RAM)
	got, ok := s.Get("A#p1#100")
	if !ok || got.LastUpdatedBy != "B1" {
		t.Fatalf("Get want LastUpdatedBy=B1 got %+v ok=%v", got, ok)
	}
}

func TestPebble_LastUpdatedBy_NotPersisted(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPebbleStore(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("pebble: %v", err)
	}
	defer p.Close()
	p.SetInstanceID("X2")
	applied, st, err := p.Apply("A#p1#100", 10, 1, 1)
	if err != nil || !applied {
		t.Fatalf("apply err=%v applied=%v", err, applied)
	}
	if st.LastUpdatedBy != "X2" {
		t.Fatalf("want returned LastUpdatedBy=X2 got %q", st.LastUpdatedBy)
	}
	// When reading from storage, transient field should not persist
	got, ok := p.Get("A#p1#100")
	if !ok {
		t.Fatalf("Get not found")
	}
	if got.LastUpdatedBy != "" {
		t.Fatalf("LastUpdatedBy should not be persisted, got %q", got.LastUpdatedBy)
	}
	_ = os.RemoveAll(dir)
}

