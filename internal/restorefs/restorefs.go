package restorefs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

type Restorer struct {
	stateStore      state.Store
	snapshotter     snapshot.Snapshotter
	manifestReader  manifest.Reader
	snapshotBaseDir string
}

type Reader interface {
	ReadLatest() (manifest.Manifest, error)
}

type FilesystemReader struct{ baseDir string }

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

func NewRestorer(st state.Store, snap snapshot.Snapshotter, mr manifest.Reader, snapshotBaseDir string) *Restorer {
	return &Restorer{stateStore: st, snapshotter: snap, manifestReader: mr, snapshotBaseDir: snapshotBaseDir}
}

type RestoreResult struct {
	Applied           int
	Skipped           int
	Bytes             int64
	LastAppliedOffset int64
	Error             error
}

func (r *Restorer) RestoreFromSnapshot(snapshotID string) error {
	if snapshotID == "" {
		return nil
	}
	path := filepath.Join(r.snapshotBaseDir, snapshotID, "state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("restore: snapshot not found at %s, skipping", path)
			return nil
		}
		return fmt.Errorf("read snapshot: %w", err)
	}
	var dump map[string]state.RecordState
	if err := json.Unmarshal(data, &dump); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}
	r.stateStore.LoadAll(dump)
	log.Printf("restore: loaded %d keys from snapshot %s", len(dump), snapshotID)
	return nil
}

func (r *Restorer) ReplayChangelog(changelogPath string, fromOffset int64) RestoreResult {
	f, err := os.Open(changelogPath)
	if err != nil {
		return RestoreResult{Error: fmt.Errorf("open changelog: %w", err)}
	}
	defer f.Close()
	return r.replayLines(f, fromOffset)
}

func (r *Restorer) RestoreAndReplay() (RestoreResult, error) {
	m, err := r.manifestReader.ReadLatest()
	if err != nil {
		return RestoreResult{}, fmt.Errorf("read manifest: %w", err)
	}
	if err := r.RestoreFromSnapshot(m.SnapshotID); err != nil {
		return RestoreResult{}, fmt.Errorf("restore snapshot: %w", err)
	}
	res := r.ReplayChangelog("./changelog/opb.jsonl", m.LastChangelogOffset)
	return res, res.Error
}

func (r *Restorer) parseAndApplyLine(line []byte) (bool, bool, error) {
	var d changelog.Delta
	if err := json.Unmarshal(line, &d); err != nil {
		return false, false, fmt.Errorf("unmarshal delta: %w", err)
	}
	ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
	if err != nil {
		return false, false, fmt.Errorf("apply: %w", err)
	}
	if ok {
		return true, false, nil
	}
	return false, true, nil
}

func (r *Restorer) replayLines(reader io.Reader, fromOffset int64) RestoreResult {
	sc := bufio.NewScanner(reader)
	applied, skipped := 0, 0
	line := 0
	var bytes int64
	var last int64 = -1
	for sc.Scan() {
		line++
		if int64(line) <= fromOffset {
			continue
		}
		b := sc.Bytes()
		a, s, err := r.parseAndApplyLine(b)
		if err != nil {
			return RestoreResult{Error: fmt.Errorf("line %d: %w", line, err)}
		}
		if a {
			applied++
		} else if s {
			skipped++
		}
		bytes += int64(len(b))
		last = int64(line)
	}
	if err := sc.Err(); err != nil {
		return RestoreResult{Error: fmt.Errorf("scan changelog: %w", err)}
	}
	return RestoreResult{Applied: applied, Skipped: skipped, Bytes: bytes, LastAppliedOffset: last}
}
