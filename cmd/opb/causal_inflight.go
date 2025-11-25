package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"hpb/internal/opb"
	"hpb/internal/state"
)

// inflightRecord captures a single pending event for causal recovery.
type inflightRecord struct {
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload,omitempty"`
	VC      opb.VectorClock `json:"vectorClock,omitempty"`
}

type inflightSnapshot struct {
	Channels []string                    `json:"channels,omitempty"`
	Events   map[string][]inflightRecord `json:"events"`
}

func writeInflightSnapshot(baseDir, snapID string, channels []string, inflight map[string][]inflightRecord) (string, int, error) {
	if len(inflight) == 0 {
		return "", 0, nil
	}
	dir := filepath.Join(baseDir, snapID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, fmt.Errorf("mkdir inflight dir: %w", err)
	}
	out := inflightSnapshot{
		Channels: append([]string(nil), channels...),
		Events:   inflight,
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return "", 0, fmt.Errorf("marshal inflight: %w", err)
	}
	relPath := "inflight.json"
	if err := os.WriteFile(filepath.Join(dir, relPath), b, 0o644); err != nil {
		return "", 0, fmt.Errorf("write inflight file: %w", err)
	}
	total := 0
	for _, evs := range inflight {
		total += len(evs)
	}
	return relPath, total, nil
}

func readInflightSnapshot(baseDir, snapID, relPath string) (inflightSnapshot, error) {
	var snap inflightSnapshot
	if relPath == "" {
		return snap, nil
	}
	fp := filepath.Join(baseDir, snapID, relPath)
	b, err := os.ReadFile(fp)
	if err != nil {
		return snap, err
	}
	if err := json.Unmarshal(b, &snap); err != nil {
		return inflightSnapshot{}, fmt.Errorf("unmarshal inflight: %w", err)
	}
	return snap, nil
}

func replayInflightEvents(cfg Config, st state.Store, snap inflightSnapshot) error {
	if len(snap.Events) == 0 {
		return nil
	}
	order := snap.Channels
	if len(order) == 0 {
		for ch := range snap.Events {
			order = append(order, ch)
		}
		sort.Strings(order)
	}
	for _, ch := range order {
		for _, rec := range snap.Events[ch] {
			if len(rec.Payload) == 0 {
				continue
			}
			var ev opb.OrderEnriched
			if err := json.Unmarshal(rec.Payload, &ev); err != nil {
				continue
			}
			if _, _, _, err := opb.AggregateAndBuildOutput(st, cfg.WindowSizeSec, ev); err != nil {
				return fmt.Errorf("replay inflight channel=%s key=%s: %w", ch, rec.Key, err)
			}
		}
	}
	return nil
}
