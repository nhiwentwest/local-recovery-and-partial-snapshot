package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/restore"
	"hpb/internal/state"

	"github.com/segmentio/kafka-go"
)

func main() {
	var (
		bootstrap       string
		groupID         string
		instanceID      string
		manifestSource  string
		changelogSource string
		topicSnapshots  string
		topicChangelog  string
		snapshotDir     string
		httpAddr        string
		pollIntervalSec int
		advanceManifest bool
	)
	flag.StringVar(&bootstrap, "bootstrap", "localhost:19092", "kafka bootstrap")
	flag.StringVar(&groupID, "group-id", "recover", "metrics label: group id")
	flag.StringVar(&instanceID, "instance-id", "R", "metrics label: instance id")
	flag.StringVar(&manifestSource, "manifest-source", "kafka", "file|kafka")
	flag.StringVar(&changelogSource, "changelog-source", "kafka", "file|kafka")
	flag.StringVar(&topicSnapshots, "topic-snapshots", "p2.opb-snapshots", "manifest topic")
	flag.StringVar(&topicChangelog, "topic-changelog", "p2.opb-changelog", "changelog topic")
	flag.StringVar(&snapshotDir, "snapshot-dir", "./snapshots", "snapshot dir for file mode")
	flag.StringVar(&httpAddr, "http", ":9090", "http listen for /metrics")
	flag.IntVar(&pollIntervalSec, "poll", 10, "poll interval seconds for manifest")
	flag.BoolVar(&advanceManifest, "advance-manifest", true, "after replay, publish updated manifest with new last offset")
	flag.Parse()

	mreg := metrics.NewRegistry()
	go func() {
		http.Handle("/metrics", mreg.Handler())
		_ = http.ListenAndServe(httpAddr, nil)
	}()

	// Build readers
	var mReader restore.Reader
	if manifestSource == "file" {
		mReader = restore.NewFilesystemReader(snapshotDir)
	} else {
		mReader = restore.NewKafkaReader([]string{bootstrap}, topicSnapshots, "opb-manifest-latest")
	}

	// Build publisher for advancing manifest
	var mPublisher manifest.Publisher
	if advanceManifest {
		if manifestSource == "file" {
			mPublisher = manifest.NewFilesystemManifest(snapshotDir)
		} else {
			mPublisher = manifest.NewKafkaManifest(bootstrap, topicSnapshots, "opb-manifest-latest")
		}
	}

	st := restore.NewFilesystemReader("") // only to satisfy types when constructing Restorer; state passed inside Restorer
	_ = st

	ticker := time.NewTicker(time.Duration(pollIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		t1 := time.Now()
		// Use Restorer with a fresh in-memory state each cycle (demo simplicity)
		r := restore.NewRestorer(state.NewInMemoryStore(), nil, mReader, snapshotDir)
		m, err := mReader.ReadLatest()
		if err != nil {
			log.Printf("read manifest: %v", err)
			<-ticker.C
			continue
		}
		if err := r.RestoreFromSnapshot(m.SnapshotID); err != nil {
			log.Printf("restore snapshot: %v", err)
			<-ticker.C
			continue
		}

		var res restore.RestoreResult
		if changelogSource == "file" {
			res = r.ReplayChangelog("./changelog/opb.jsonl", m.LastChangelogOffset)
		} else {
			res = r.ReplayChangelogKafka([]string{bootstrap}, topicChangelog, m.LastChangelogOffset)
		}
		if res.Error != nil {
			log.Printf("replay: %v", res.Error)
			<-ticker.C
			continue
		}

		// Update metrics
		mreg.Applied.Add(float64(res.Applied))
		mreg.Skipped.Add(float64(res.Skipped))
		mreg.TTRSec.Set(time.Since(t1).Seconds())
		// Compute lag: headOffset - lastAppliedOffset
		if changelogSource == "kafka" {
			// Per-partition head and last applied (we only have a single lastAppliedOffset across partitions in demo).
			// For visualization, export head lag at partition 0, and set partition-labeled gauge where possible.
			// Head for partition 0 (compat):
			head := headOffset(topicChangelog, bootstrap)
			if head >= 0 && res.LastAppliedOffset >= 0 {
				mreg.Lag.Set(float64(head - res.LastAppliedOffset))
			}
			// Export partition 0 lag as labeled metric as a minimal viable demo
			if head >= 0 && res.LastAppliedOffset >= 0 {
				mreg.PartitionLag.WithLabelValues(topicChangelog, "0", groupID, instanceID).Set(float64(head - res.LastAppliedOffset))
			}
		}
		// Manifest age
		mreg.LastManifestAgeSec.Set(time.Since(time.Unix(m.CreatedAtEpochSecond, 0)).Seconds())
		// Optionally advance manifest to new last offset
		if advanceManifest && mPublisher != nil {
			if res.LastAppliedOffset > m.LastChangelogOffset {
				if err := mPublisher.PublishLatest(m.SnapshotID, res.LastAppliedOffset); err != nil {
					log.Printf("advance manifest: %v", err)
				} else {
					log.Printf("advanced manifest: snapshot=%s lastOffset=%d (prev=%d)", m.SnapshotID, res.LastAppliedOffset, m.LastChangelogOffset)
				}
			}
		}
		log.Printf("recovery cycle: applied=%d skipped=%d ttr=%.3fs", res.Applied, res.Skipped, time.Since(t1).Seconds())

		<-ticker.C
	}
}

// headOffset returns the last (high-watermark - 1) offset of partition 0 for a topic
func headOffset(topic string, bootstrap string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(ctx, "tcp", bootstrap, topic, 0)
	if err != nil {
		return -1
	}
	defer conn.Close()
	off, err := conn.ReadLastOffset()
	if err != nil {
		return -1
	}
	return off - 1
}

// (no adapter needed; state.NewInMemoryStore satisfies state.Store)
