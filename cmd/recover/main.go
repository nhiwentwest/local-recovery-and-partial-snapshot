package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"hpb/internal/metrics"
	"hpb/internal/restore"
	"hpb/internal/state"

	"github.com/segmentio/kafka-go"
)

func main() {
	var (
		bootstrap       string
		manifestSource  string
		changelogSource string
		topicSnapshots  string
		topicChangelog  string
		snapshotDir     string
		httpAddr        string
		pollIntervalSec int
	)
	flag.StringVar(&bootstrap, "bootstrap", "localhost:19092", "kafka bootstrap")
	flag.StringVar(&manifestSource, "manifest-source", "kafka", "file|kafka")
	flag.StringVar(&changelogSource, "changelog-source", "kafka", "file|kafka")
	flag.StringVar(&topicSnapshots, "topic-snapshots", "p2.opb-snapshots", "manifest topic")
	flag.StringVar(&topicChangelog, "topic-changelog", "p2.opb-changelog", "changelog topic")
	flag.StringVar(&snapshotDir, "snapshot-dir", "./snapshots", "snapshot dir for file mode")
	flag.StringVar(&httpAddr, "http", ":9090", "http listen for /metrics")
	flag.IntVar(&pollIntervalSec, "poll", 10, "poll interval seconds for manifest")
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
			head := headOffset(topicChangelog, bootstrap)
			if head >= 0 && res.LastAppliedOffset >= 0 {
				mreg.Lag.Set(float64(head - res.LastAppliedOffset))
			}
		}
		// Manifest age
		mreg.LastManifestAgeSec.Set(time.Since(time.Unix(m.CreatedAtEpochSecond, 0)).Seconds())
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
