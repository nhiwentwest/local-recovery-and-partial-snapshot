package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"hpb/internal/manifest"
	rf "hpb/internal/restorefs"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

func readManifestFromID(baseDir, id string) (manifest.Manifest, error) {
	var m manifest.Manifest
	b, err := os.ReadFile(filepath.Join(baseDir, id, "manifest.json"))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, nil
}

func loadEffectiveState(baseDir, id string) (map[string]state.RecordState, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot id")
	}
	m, err := readManifestFromID(baseDir, id)
	if err != nil {
		return nil, fmt.Errorf("read manifest for %s: %w", id, err)
	}
	fmt.Printf("load id=%s type=%s base=%s parent=%s dseq=%d\n", id, m.SnapshotType, m.BaseSnapshotID, m.ParentSnapshotID, m.DeltaSequence)
	st := state.NewInMemoryStore()
	r := rf.NewRestorerWithOptions(st, nil, rf.NewFilesystemReader(baseDir), baseDir, snapshot.FormatJSON, 1)
	fmt.Printf("restoring...\n")
	if m.SnapshotType == manifest.SnapshotTypeDelta {
		if err := r.RestoreChainFromLatestWithOptions(m, rf.RestoreOptions{Parallelism: 0, ValidateChain: true, SkipMissingDelta: false}); err != nil {
			return nil, fmt.Errorf("restore chain: %w", err)
		}
	} else {
		fmtFmt, err := snapshot.ParseFormat(m.SnapshotFormat)
		if err != nil {
			return nil, fmt.Errorf("parse format: %w", err)
		}
		shards := m.SnapshotShards
		if shards <= 0 {
			shards = 1
		}
		if err := r.RestoreFromSnapshotWithFormatParallel(m.SnapshotID, fmtFmt, shards, m.SnapshotKeys, 0); err != nil {
			return nil, fmt.Errorf("restore full: %w", err)
		}
	}
	// Dump to map
	result := make(map[string]state.RecordState)
	_ = st.Range(func(k string, rs state.RecordState) error { result[k] = rs; return nil })
	return result, nil
}

func compareStates(a, b map[string]state.RecordState) (equal bool, details string) {
	if len(a) != len(b) {
		equal = false
		details += fmt.Sprintf("key count differs: left=%d right=%d\n", len(a), len(b))
	}
	// produce sorted keys union
	m := map[string]struct{}{}
	for k := range a { m[k] = struct{}{} }
	for k := range b { m[k] = struct{}{} }
	keys := make([]string, 0, len(m))
	for k := range m { keys = append(keys, k) }
	sort.Strings(keys)
	mismatch := 0
	missingLeft := 0
	missingRight := 0
	for _, k := range keys {
		va, oka := a[k]
		vb, okb := b[k]
		if !oka || !okb {
			if !oka { missingLeft++ }
			if !okb { missingRight++ }
			continue
		}
		if va.SumAmount != vb.SumAmount || va.SumQty != vb.SumQty || va.LastSeq != vb.LastSeq {
			if mismatch < 20 {
				details += fmt.Sprintf("diff key=%s left={sum=%d qty=%d seq=%d} right={sum=%d qty=%d seq=%d}\n", k, va.SumAmount, va.SumQty, va.LastSeq, vb.SumAmount, vb.SumQty, vb.LastSeq)
			}
			mismatch++
		}
	}
	if missingLeft > 0 || missingRight > 0 {
		details += fmt.Sprintf("missing: left_missing=%d right_missing=%d\n", missingLeft, missingRight)
	}
	if mismatch > 0 {
		details += fmt.Sprintf("value mismatches: %d (showing up to 20)\n", mismatch)
	}
	return details == "", details
}

func main() {
	var snapshotDir string
	var leftID string
	var rightID string
	flag.StringVar(&snapshotDir, "snapshot-dir", "./snapshots", "snapshot directory")
	flag.StringVar(&leftID, "left-id", "", "left snapshot ID (base or delta)")
	flag.StringVar(&rightID, "right-id", "", "right snapshot ID (base or delta)")
	flag.Parse()

	if leftID == "" || rightID == "" {
		fmt.Fprintln(os.Stderr, "usage: compare_effective_state --snapshot-dir ./snapshots --left-id <ID1> --right-id <ID2>")
		os.Exit(2)
	}

	left, err := loadEffectiveState(snapshotDir, leftID)
	if err != nil { log.Fatalf("left: %v", err) }
	right, err := loadEffectiveState(snapshotDir, rightID)
	if err != nil { log.Fatalf("right: %v", err) }

	equal, details := compareStates(left, right)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	if equal {
		fmt.Fprintln(w, "EQUAL")
		fmt.Fprintf(w, "keys=%d\n", len(left))
		os.Exit(0)
	}
	fmt.Fprintln(w, "NOT EQUAL")
	fmt.Fprint(w, details)
	os.Exit(1)
}

