package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"hpb/internal/state"
)

type Snapshotter interface {
	WriteSnapshot(snapshotID string, st state.Store) error
}

type FilesystemSnapshotter struct {
	baseDir string
}

func NewFilesystemSnapshotter(baseDir string) *FilesystemSnapshotter {
	return &FilesystemSnapshotter{baseDir: baseDir}
}

func (f *FilesystemSnapshotter) WriteSnapshot(snapshotID string, st state.Store) error {
	if err := os.MkdirAll(filepath.Join(f.baseDir, snapshotID), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	file := filepath.Join(f.baseDir, snapshotID, "state.json")
	out, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer out.Close()

	dump := make(map[string]state.RecordState)
	if err := st.Range(func(key string, rs state.RecordState) error {
		dump[key] = rs
		return nil
	}); err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(dump); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return nil
}
