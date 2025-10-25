package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"hpb/internal/metrics"
	"hpb/internal/restore"
	"hpb/internal/state"
)

func main() {
	var (
		snapshotDir      string
		httpAddr         string
		pollIntervalSec  int
		useKafka         bool
		bootstrapServers string
		topicManifest    string
		topicChangelog   string
		consumerGroup    string
	)

	flag.StringVar(&snapshotDir, "snapshot-dir", ".", "directory for snapshots and manifest")
	flag.StringVar(&httpAddr, "http", ":2112", "HTTP listen address for metrics")
	flag.IntVar(&pollIntervalSec, "poll", 30, "poll interval in seconds")
	flag.BoolVar(&useKafka, "use-kafka", false, "use Kafka topics for manifest and changelog")
	flag.StringVar(&bootstrapServers, "bootstrap", "localhost:9092", "Kafka bootstrap servers")
	flag.StringVar(&topicManifest, "topic-manifest", "p2.opb-snapshots", "Kafka topic for manifest")
	flag.StringVar(&topicChangelog, "topic-changelog", "p2.opb-changelog", "Kafka topic for changelog")
	flag.StringVar(&consumerGroup, "consumer-group", "recovery-group", "Kafka consumer group ID")
	flag.Parse()

	// Initialize metrics
	reg := metrics.NewRegistry()

	// Start metrics server
	go func() {
		http.Handle("/metrics", reg.Handler())
		log.Printf("Metrics server listening on %s", httpAddr)
		if err := http.ListenAndServe(httpAddr, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	// Initialize manifest reader based on source
	var mReader restore.Reader
	if useKafka {
		log.Printf("Using Kafka mode: bootstrap=%s, manifest-topic=%s, changelog-topic=%s",
			bootstrapServers, topicManifest, topicChangelog)
		mReader = restore.NewKafkaReader([]string{bootstrapServers}, topicManifest, "opb-manifest-latest")
	} else {
		log.Printf("Using file mode: snapshot-dir=%s", snapshotDir)
		mReader = restore.NewFilesystemReader(snapshotDir)
	}

	ticker := time.NewTicker(time.Duration(pollIntervalSec) * time.Second)
	defer ticker.Stop()

	log.Println("Starting recovery service...")

	for {
		startTime := time.Now()

		// Fresh state store for each recovery cycle
		st := state.NewInMemoryStore()
		r := restore.NewRestorer(st, nil, mReader, snapshotDir, reg)

		// Perform recovery based on source
		var result restore.RestoreResult
		var err error

		if useKafka {
			// For Kafka, we need to manually read manifest and replay
			m, err := mReader.ReadLatest()
			if err != nil {
				log.Printf("Failed to read manifest: %v", err)
				<-ticker.C
				continue
			}

			// Restore from snapshot
			if err := r.RestoreFromSnapshot(m.SnapshotID); err != nil {
				log.Printf("Failed to restore snapshot: %v", err)
				<-ticker.C
				continue
			}

			// Replay from Kafka changelog with consumer group
			result = r.ReplayChangelogKafka([]string{bootstrapServers}, topicChangelog, m.LastChangelogOffset)
		} else {
			// For file mode, use the existing method but with integrated snapshots
			result, err = integratedRestoreAndReplay(snapshotDir, st, mReader, r)
			if err != nil {
				log.Printf("Recovery failed: %v", err)
				<-ticker.C
				continue
			}
		}

		if result.Error != nil {
			log.Printf("Replay failed: %v", result.Error)
		} else {
			log.Printf("Recovery completed: TTR=%.3fs, Applied=%d, Skipped=%d, Bytes=%d",
				time.Since(startTime).Seconds(), result.Applied, result.Skipped, result.Bytes)
		}

		<-ticker.C
	}
}

// integratedRestoreAndReplay handles restoration from both original and P3 snapshots
func integratedRestoreAndReplay(snapshotDir string, st state.Store, mReader restore.Reader, r *restore.Restorer) (restore.RestoreResult, error) {
	// ƯU TIÊN 1: Sử dụng snapshot gốc từ thư mục snapshots/
	originalSnapshotsDir := filepath.Join(snapshotDir, "snapshots")
	if _, err := os.Stat(originalSnapshotsDir); err == nil {
		latestOriginal := findLatestSnapshot(originalSnapshotsDir)
		if latestOriginal != "" {
			log.Printf("Found original snapshots, using: %s", latestOriginal)
			return restoreFromSeparateSnapshots(snapshotDir, st, mReader, r)
		}
	}

	// ƯU TIÊN 2: Snapshot tích hợp (nếu có)
	integratedSnapshotPath := filepath.Join(snapshotDir, "snapshots-integrated", "state.json")
	if _, err := os.Stat(integratedSnapshotPath); err == nil {
		log.Printf("Using integrated snapshot")
		return restoreFromIntegratedSnapshot(integratedSnapshotPath, st, r)
	}

	// ƯU TIÊN 3: Chỉ P3 (fallback)
	log.Printf("No original snapshots found, using P3 only")
	return restoreFromSeparateSnapshots(snapshotDir, st, mReader, r)
}

// restoreFromIntegratedSnapshot loads state from a pre-integrated snapshot file
func restoreFromIntegratedSnapshot(snapshotPath string, st state.Store, r *restore.Restorer) (restore.RestoreResult, error) {
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return restore.RestoreResult{}, err
	}

	var integratedState map[string]state.RecordState
	if err := json.Unmarshal(data, &integratedState); err != nil {
		return restore.RestoreResult{}, err
	}

	// Load all keys from integrated snapshot using the existing restore mechanism
	// Since we don't have direct Put access, we'll use the existing RestoreAndReplay
	// but this requires creating a temporary snapshot that the restorer can read
	tempSnapshotID := "integrated-temp"
	tempSnapshotDir := filepath.Join(filepath.Dir(snapshotPath), tempSnapshotID)
	os.MkdirAll(tempSnapshotDir, 0755)

	// Copy the integrated snapshot to a location the restorer expects
	tempSnapshotPath := filepath.Join(tempSnapshotDir, "state.json")
	if err := os.WriteFile(tempSnapshotPath, data, 0644); err != nil {
		return restore.RestoreResult{}, err
	}

	// Use the restorer to load this snapshot
	if err := r.RestoreFromSnapshot(tempSnapshotID); err != nil {
		return restore.RestoreResult{}, err
	}

	log.Printf("restore: loaded %d keys from integrated snapshot %s", len(integratedState), snapshotPath)

	// Clean up temp snapshot
	os.RemoveAll(tempSnapshotDir)

	// Now run the normal replay process
	return r.RestoreAndReplay()
}

// restoreFromSeparateSnapshots loads state from both original and P3 snapshots
func restoreFromSeparateSnapshots(snapshotDir string, st state.Store, mReader restore.Reader, r *restore.Restorer) (restore.RestoreResult, error) {
	totalApplied := 0
	totalBytes := 0

	// 1. Restore from ORIGINAL snapshots (p1, p2) - Sửa đường dẫn
	originalSnapshotsDir := filepath.Join(snapshotDir, "snapshots")
	if _, err := os.Stat(originalSnapshotsDir); err == nil {
		// Find latest original snapshot
		latestOriginal := findLatestSnapshot(originalSnapshotsDir)
		if latestOriginal != "" {
			originalPath := filepath.Join(originalSnapshotsDir, latestOriginal, "state.json")
			data, err := os.ReadFile(originalPath)
			if err == nil {
				var originalState map[string]state.RecordState
				if err := json.Unmarshal(data, &originalState); err == nil {
					// SỬA: Load original snapshot bằng cách tạo snapshot tạm
					tempSnapshotID := "original-temp"
					tempSnapshotDir := filepath.Join(snapshotDir, tempSnapshotID)
					os.MkdirAll(tempSnapshotDir, 0755)

					tempSnapshotPath := filepath.Join(tempSnapshotDir, "state.json")
					if err := os.WriteFile(tempSnapshotPath, data, 0644); err == nil {
						if err := r.RestoreFromSnapshot(tempSnapshotID); err == nil {
							totalApplied += len(originalState)
							totalBytes += len(data)
							log.Printf("restore: loaded %d keys from ORIGINAL snapshot %s", len(originalState), latestOriginal)
						}
						// Clean up temp snapshot
						os.RemoveAll(tempSnapshotDir)
					}
				}
			}
		}
	}

	// 2. Restore from P3 snapshots (nếu có)
	p3Pattern := filepath.Join(snapshotDir, "p3-snapshot-*")
	matches, err := filepath.Glob(p3Pattern)
	if err == nil && len(matches) > 0 {
		latestP3 := findLatestSnapshotByTime(matches)
		p3Result, err := loadP3Snapshot(latestP3, st, r)
		if err == nil {
			// Combine results
			totalApplied += p3Result.Applied
			totalBytes += int(p3Result.Bytes)
			log.Printf("restore: loaded P3 snapshot %s (%d applied)",
				filepath.Base(latestP3), p3Result.Applied)
		} else {
			log.Printf("Failed to load P3 snapshot: %v", err)
		}
	}

	// 3. Now run the normal replay process for changelog
	result, err := r.RestoreAndReplay()
	if err != nil {
		return result, err
	}

	// Combine results
	result.Applied += totalApplied
	result.Bytes += int64(totalBytes)

	return result, nil
}

// loadP3Snapshot loads a P3 snapshot using the restorer
func loadP3Snapshot(snapshotPath string, st state.Store, r *restore.Restorer) (restore.RestoreResult, error) {
	// Read the P3 snapshot data
	data, err := os.ReadFile(filepath.Join(snapshotPath, "state.json"))
	if err != nil {
		return restore.RestoreResult{}, err
	}

	var p3State map[string]state.RecordState
	if err := json.Unmarshal(data, &p3State); err != nil {
		return restore.RestoreResult{}, err
	}

	// Create a temporary snapshot that the restorer can load
	tempSnapshotID := "p3-temp"
	tempSnapshotDir := filepath.Join(filepath.Dir(snapshotPath), tempSnapshotID)
	os.MkdirAll(tempSnapshotDir, 0755)

	tempSnapshotPath := filepath.Join(tempSnapshotDir, "state.json")
	if err := os.WriteFile(tempSnapshotPath, data, 0644); err != nil {
		return restore.RestoreResult{}, err
	}

	// Use the restorer to load the P3 snapshot
	if err := r.RestoreFromSnapshot(tempSnapshotID); err != nil {
		return restore.RestoreResult{}, err
	}

	// Clean up
	os.RemoveAll(tempSnapshotDir)

	return restore.RestoreResult{
		Applied: len(p3State),
		Skipped: 0,
		Bytes:   int64(len(data)),
	}, nil
}

// findLatestSnapshotByTime finds the latest P3 snapshot based on timestamp in name
func findLatestSnapshotByTime(matches []string) string {
	if len(matches) == 0 {
		return ""
	}

	latest := matches[0]
	for i := 1; i < len(matches); i++ {
		if matches[i] > latest {
			latest = matches[i]
		}
	}
	return latest
}

func findLatestSnapshot(snapshotsDir string) string {
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		return ""
	}

	var latest string
	for _, entry := range entries {
		if entry.IsDir() {
			current := entry.Name()
			if latest == "" || current > latest {
				latest = current
			}
		}
	}
	return latest
}
