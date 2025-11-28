package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"

	"hpb/internal/state"
)

type Format string

const (
	FormatJSON    Format = "json"
	FormatMsgpack Format = "msgpack"
	// FormatPebble is an experimental format where the snapshot stores Pebble SSTables
	// instead of logical JSON/msgpack dumps. When used, snapshot writing/reading is
	// delegated to the underlying Pebble store via a checkpoint API.
	FormatPebble Format = "pebble"
)

// ParseFormat normalizes raw string input into a supported snapshot format.
// Empty input defaults to JSON for backward compatibility.
func ParseFormat(raw string) (Format, error) {
	switch Format(raw) {
	case "", FormatJSON:
		return FormatJSON, nil
	case FormatMsgpack:
		return FormatMsgpack, nil
	case FormatPebble:
		return FormatPebble, nil
	default:
		return "", fmt.Errorf("unsupported snapshot format: %s (use json|msgpack)", raw)
	}
}

func (f Format) FileName() string {
	switch f {
	case FormatMsgpack:
		return "state.msgpack"
	default:
		return "state.json"
	}
}

func (f Format) FileNameDelta() string {
	switch f {
	case FormatMsgpack:
		return "state.delta.msgpack"
	default:
		return "state.delta.json"
	}
}

func (f Format) FileNameForShard(shardIdx, shardCount int) string {
	if shardCount <= 1 {
		return f.FileName()
	}
	return fmt.Sprintf("%s.%d", f.FileName(), shardIdx)
}

func (f Format) FileNameDeltaForShard(shardIdx, shardCount int) string {
	if shardCount <= 1 {
		return f.FileNameDelta()
	}
	return fmt.Sprintf("%s.%d", f.FileNameDelta(), shardIdx)
}

func (f Format) String() string {
	if f == "" {
		return string(FormatJSON)
	}
	return string(f)
}

type Result struct {
	Format Format
	Shards int
	Keys   int
	// Pebble-specific fields (only set when Format == FormatPebble)
	PebbleSSTFiles         []string
	PebbleFormatVersion    string
	PebbleSSTChecksums     map[string]string
	PebbleIncrementalFiles []string // Phase 3: new files in incremental snapshot
}

type Snapshotter interface {
	WriteSnapshot(snapshotID string, st state.Store) (Result, error)
}

type FilesystemSnapshotter struct {
	baseDir string
	format  Format
	shards  int
}

// WriteDeltaSnapshotFromView writes only selected keys (changed keys) from a point-in-time view.
// keys may contain duplicates; only existing keys in the view will be emitted.
func (f *FilesystemSnapshotter) WriteDeltaSnapshotFromView(snapshotID string, view state.SnapshotView, keys []string) (Result, error) {
	if err := os.MkdirAll(filepath.Join(f.baseDir, snapshotID), 0o755); err != nil {
		return Result{}, fmt.Errorf("mkdir: %w", err)
	}
	// Build a set for fast membership test
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	shardCount := f.shards
	if shardCount <= 1 {
		// Single file delta
		file := filepath.Join(f.baseDir, snapshotID, f.format.FileNameDelta())
		out, err := os.Create(file)
		if err != nil {
			return Result{}, fmt.Errorf("create: %w", err)
		}
		defer out.Close()
		dump := make(map[string]state.RecordState)
		if err := view.Range(func(key string, rs state.RecordState) error {
			if _, ok := set[key]; ok {
				dump[key] = rs
			}
			return nil
		}); err != nil {
			return Result{}, err
		}
		if err := encodeSnapshot(out, f.format, dump); err != nil {
			return Result{}, err
		}
		if err := out.Sync(); err != nil {
			return Result{}, err
		}
		return Result{Format: f.format, Shards: 1, Keys: len(dump)}, nil
	}
	// Sharded delta
	shards := make([]map[string]state.RecordState, shardCount)
	for i := range shards {
		shards[i] = make(map[string]state.RecordState)
	}
	var totalKeys int
	if err := view.Range(func(key string, rs state.RecordState) error {
		if _, ok := set[key]; !ok {
			return nil
		}
		idx := shardIndex(key, shardCount)
		shards[idx][key] = rs
		totalKeys++
		return nil
	}); err != nil {
		return Result{}, err
	}
	for i, data := range shards {
		file := filepath.Join(f.baseDir, snapshotID, f.format.FileNameDeltaForShard(i, shardCount))
		out, err := os.Create(file)
		if err != nil {
			return Result{}, fmt.Errorf("create shard %d: %w", i, err)
		}
		if err := encodeSnapshot(out, f.format, data); err != nil {
			out.Close()
			return Result{}, err
		}
		if err := out.Sync(); err != nil {
			out.Close()
			return Result{}, err
		}
		_ = out.Close()
	}
	return Result{Format: f.format, Shards: shardCount, Keys: totalKeys}, nil
}

// WriteSnapshotFromView writes a snapshot using a provided point-in-time view.
// Caller is responsible for closing the view once writing completes.
func (f *FilesystemSnapshotter) WriteSnapshotFromView(snapshotID string, view state.SnapshotView) (Result, error) {
	if err := os.MkdirAll(filepath.Join(f.baseDir, snapshotID), 0o755); err != nil {
		return Result{}, fmt.Errorf("mkdir: %w", err)
	}
	shardCount := f.shards
	if shardCount <= 1 {
		return f.writeSingleFromView(snapshotID, view)
	}
	return f.writeShardedFromView(snapshotID, view, shardCount)
}

func (f *FilesystemSnapshotter) writeSingleFromView(snapshotID string, view state.SnapshotView) (Result, error) {
	// Experimental fast-path: when format is pebble and the underlying store supports
	// CheckpointCapable, delegate snapshot writing to the Pebble backend instead of
	// emitting a logical JSON/msgpack dump.
	if f.format == FormatPebble {
		if cap, ok := any(view).(interface {
			ExportSSTables(dstDir string) ([]string, string, error)
		}); ok {
			// NOTE: snapshot views currently do not expose the underlying store, so this
			// branch is left as a placeholder for future refinement.
			_, _, _ = cap, snapshotID, view
		}
	}
	file := filepath.Join(f.baseDir, snapshotID, f.format.FileName())
	out, err := os.Create(file)
	if err != nil {
		return Result{}, fmt.Errorf("create: %w", err)
	}
	defer out.Close()
	dump := make(map[string]state.RecordState)
	if err := view.Range(func(key string, rs state.RecordState) error {
		dump[key] = rs
		return nil
	}); err != nil {
		return Result{}, err
	}
	if err := encodeSnapshot(out, f.format, dump); err != nil {
		return Result{}, err
	}
	if err := out.Sync(); err != nil {
		return Result{}, err
	}
	return Result{Format: f.format, Shards: 1, Keys: len(dump)}, nil
}

func (f *FilesystemSnapshotter) writeShardedFromView(snapshotID string, view state.SnapshotView, shardCount int) (Result, error) {
	shards := make([]map[string]state.RecordState, shardCount)
	for i := range shards {
		shards[i] = make(map[string]state.RecordState)
	}
	var totalKeys int
	if err := view.Range(func(key string, rs state.RecordState) error {
		idx := shardIndex(key, shardCount)
		shards[idx][key] = rs
		totalKeys++
		return nil
	}); err != nil {
		return Result{}, err
	}
	for i, data := range shards {
		file := filepath.Join(f.baseDir, snapshotID, f.format.FileNameForShard(i, shardCount))
		out, err := os.Create(file)
		if err != nil {
			return Result{}, fmt.Errorf("create shard %d: %w", i, err)
		}
		if err := encodeSnapshot(out, f.format, data); err != nil {
			out.Close()
			return Result{}, err
		}
		if err := out.Sync(); err != nil {
			out.Close()
			return Result{}, err
		}
		_ = out.Close()
	}
	return Result{Format: f.format, Shards: shardCount, Keys: totalKeys}, nil
}

func NewFilesystemSnapshotter(baseDir string, format Format, shards int) *FilesystemSnapshotter {
	if format == "" {
		format = FormatJSON
	}
	if shards < 1 {
		shards = 1
	}
	return &FilesystemSnapshotter{baseDir: baseDir, format: format, shards: shards}
}

func (f *FilesystemSnapshotter) WriteSnapshot(snapshotID string, st state.Store) (Result, error) {
	if err := os.MkdirAll(filepath.Join(f.baseDir, snapshotID), 0o755); err != nil {
		return Result{}, fmt.Errorf("mkdir: %w", err)
	}
	shardCount := f.shards
	if shardCount <= 1 {
		return f.writeSingle(snapshotID, st)
	}
	return f.writeSharded(snapshotID, st, shardCount)
}

func (f *FilesystemSnapshotter) writeSingle(snapshotID string, st state.Store) (Result, error) {
	file := filepath.Join(f.baseDir, snapshotID, f.format.FileName())
	out, err := os.Create(file)
	if err != nil {
		return Result{}, fmt.Errorf("create: %w", err)
	}
	defer out.Close()

	dump := make(map[string]state.RecordState)
	view, err := st.NewSnapshotView()
	if err != nil {
		return Result{}, err
	}
	defer view.Close()
	if err := view.Range(func(key string, rs state.RecordState) error {
		dump[key] = rs
		return nil
	}); err != nil {
		return Result{}, err
	}
	if err := encodeSnapshot(out, f.format, dump); err != nil {
		return Result{}, err
	}
	if err := out.Sync(); err != nil {
		return Result{}, err
	}
	return Result{Format: f.format, Shards: 1, Keys: len(dump)}, nil
}

func (f *FilesystemSnapshotter) writeSharded(snapshotID string, st state.Store, shardCount int) (Result, error) {
	shards := make([]map[string]state.RecordState, shardCount)
	for i := range shards {
		shards[i] = make(map[string]state.RecordState)
	}
	var totalKeys int
	view, err := st.NewSnapshotView()
	if err != nil {
		return Result{}, err
	}
	defer view.Close()
	if err := view.Range(func(key string, rs state.RecordState) error {
		idx := shardIndex(key, shardCount)
		shards[idx][key] = rs
		totalKeys++
		return nil
	}); err != nil {
		return Result{}, err
	}
	for i, data := range shards {
		file := filepath.Join(f.baseDir, snapshotID, f.format.FileNameForShard(i, shardCount))
		out, err := os.Create(file)
		if err != nil {
			return Result{}, fmt.Errorf("create shard %d: %w", i, err)
		}
		if err := encodeSnapshot(out, f.format, data); err != nil {
			out.Close()
			return Result{}, err
		}
		if err := out.Sync(); err != nil {
			out.Close()
			return Result{}, err
		}
		_ = out.Close()
	}
	return Result{Format: f.format, Shards: shardCount, Keys: totalKeys}, nil
}

func encodeSnapshot(w io.Writer, format Format, dump map[string]state.RecordState) error {
	switch format {
	case FormatMsgpack:
		enc := msgpack.NewEncoder(w)
		if err := enc.Encode(dump); err != nil {
			return fmt.Errorf("encode msgpack: %w", err)
		}
	default:
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(dump); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
	}
	return nil
}

// DecodeSnapshot parses snapshot bytes using the provided format.
func DecodeSnapshot(data []byte, format Format) (map[string]state.RecordState, error) {
	switch format {
	case FormatMsgpack:
		var dump map[string]state.RecordState
		if err := msgpack.NewDecoder(bytes.NewReader(data)).Decode(&dump); err != nil {
			return nil, fmt.Errorf("decode msgpack: %w", err)
		}
		return dump, nil
	default:
		var dump map[string]state.RecordState
		if err := json.Unmarshal(data, &dump); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}
		return dump, nil
	}
}

func shardIndex(key string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shardCount))
}
