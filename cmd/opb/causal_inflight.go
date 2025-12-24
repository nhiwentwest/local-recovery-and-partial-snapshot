package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

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

	// --- Parallel Replay Logic ---
	numWorkers := cfg.ReplayWorkers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
		if numWorkers > 8 {
			numWorkers = 8 // Cap at 8 to avoid excessive I/O contention
		}
	}
	if numWorkers > len(order) {
		numWorkers = len(order)
	}

	jobs := make(chan string, len(order))
	results := make(chan error, len(order))
	var wg sync.WaitGroup

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for ch := range jobs {
		for _, rec := range snap.Events[ch] {
			if len(rec.Payload) == 0 {
				continue
			}
			var ev opb.OrderEnriched
			if err := json.Unmarshal(rec.Payload, &ev); err != nil {
				continue
			}
			if _, _, _, err := opb.AggregateAndBuildOutput(st, cfg.WindowSizeSec, ev); err != nil {
						results <- fmt.Errorf("worker %d: replay inflight channel=%s key=%s: %w", workerID, ch, rec.Key, err)
						return
			}
		}
	}
		}(w)
	}

	for _, ch := range order {
		jobs <- ch
	}
	close(jobs)

	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}
