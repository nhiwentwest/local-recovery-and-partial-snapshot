package snapshot

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"hpb/internal/manifest"
)

// GarbageCollector is responsible for identifying and removing obsolete snapshots.
// It requires a manifest.Reader to determine the latest snapshot chain.
type GarbageCollector struct {
	baseDir        string
	retentionCount int
	retentionDays  int
	manifestReader manifest.Reader
}

// NewGarbageCollector creates a new GC instance.
func NewGarbageCollector(baseDir string, retentionCount, retentionDays int, mr manifest.Reader) *GarbageCollector {
	return &GarbageCollector{
		baseDir:        baseDir,
		retentionCount: retentionCount,
		retentionDays:  retentionDays,
		manifestReader: mr,
	}
}

// Collect identifies and removes obsolete snapshots based on the retention policy.
// It returns the list of deleted snapshot IDs.
func (gc *GarbageCollector) Collect() ([]string, error) {
	if gc.retentionCount <= 0 && gc.retentionDays <= 0 {
		return nil, nil // GC is disabled
	}

	// 1. Build the protected set from the latest manifest chain.
	protected, err := gc.buildProtectedSet()
	if err != nil {
		return nil, fmt.Errorf("build protected set: %w", err)
	}

	// 2. List all available snapshots.
	allSnapshots, err := gc.listAllSnapshots()
	if err != nil {
		return nil, fmt.Errorf("list all snapshots: %w", err)
	}

	// 3. Identify snapshots to delete.
	toDelete := gc.identifyForDeletion(allSnapshots, protected)

	// 4. Delete the identified snapshots.
	var deletedIDs []string
	for _, m := range toDelete {
		log.Printf("gc: deleting snapshot %s (created at %s)", m.SnapshotID, time.Unix(m.CreatedAtEpochSecond, 0).UTC())
		dir := filepath.Join(gc.baseDir, m.SnapshotID)
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("gc: error deleting snapshot directory %s: %v", dir, err)
			continue // Log error but continue with others
		}
		deletedIDs = append(deletedIDs, m.SnapshotID)
	}

	return deletedIDs, nil
}

// buildProtectedSet walks the chain from the latest manifest to find all snapshots that must be kept.
func (gc *GarbageCollector) buildProtectedSet() (map[string]bool, error) {
	latest, err := gc.manifestReader.ReadLatest()
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]bool), nil // No manifest, no protected snapshots
		}
		return nil, err
	}

	protected := make(map[string]bool)
	currentID := latest.SnapshotID
	for currentID != "" {
		protected[currentID] = true
		path := filepath.Join(gc.baseDir, currentID, "manifest.json")
		b, err := os.ReadFile(path)
		if err != nil {
			log.Printf("gc: warning: could not read manifest for protected snapshot %s: %v", currentID, err)
			break // Stop if chain is broken
		}
		var m manifest.Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			log.Printf("gc: warning: could not parse manifest for protected snapshot %s: %v", currentID, err)
			break // Stop if chain is broken
		}
		if strings.ToLower(m.SnapshotType) != manifest.SnapshotTypeDelta {
			break // Reached the base of the chain
		}
		currentID = m.ParentSnapshotID
	}
	return protected, nil
}

// listAllSnapshots scans the base directory for all snapshot manifests.
func (gc *GarbageCollector) listAllSnapshots() ([]manifest.Manifest, error) {
	entries, err := os.ReadDir(gc.baseDir)
	if err != nil {
		return nil, err
	}

	var all []manifest.Manifest
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		path := filepath.Join(gc.baseDir, id, "manifest.json")
		b, err := os.ReadFile(path)
		if err != nil {
			continue // Skip if manifest is unreadable
		}
		var m manifest.Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			continue // Skip if manifest is corrupt
		}
		all = append(all, m)
	}

	// Sort by creation time, newest first.
	sort.Slice(all, func(i, j int) bool {
		return all[i].CreatedAtEpochSecond > all[j].CreatedAtEpochSecond
	})

	return all, nil
}

// identifyForDeletion applies retention policies to the list of all snapshots.
func (gc *GarbageCollector) identifyForDeletion(all []manifest.Manifest, protected map[string]bool) []manifest.Manifest {
	var toDelete []manifest.Manifest
	now := time.Now().UTC()
	keptCount := 0

	for _, m := range all {
		if protected[m.SnapshotID] {
			keptCount++
			continue
		}

		// Retention by count
		if gc.retentionCount > 0 && keptCount < gc.retentionCount {
			// This snapshot is one of the N most recent non-protected snapshots to keep.
			// Note: this logic assumes 'all' is sorted newest to oldest.
			// We count protected snapshots as part of the total to keep.
			keptCount++
			continue
		}

		// Retention by days
		if gc.retentionDays > 0 {
			createdTime := time.Unix(m.CreatedAtEpochSecond, 0).UTC()
			if now.Sub(createdTime).Hours() < float64(gc.retentionDays*24) {
				continue
			}
		}

		// If we reach here, the snapshot is not protected and fails retention policies.
		toDelete = append(toDelete, m)
	}

	return toDelete
}

