package main

import (
    "fmt"

    "hpb/internal/changelog"
)

// InitChangelog sets up the changelog Writer based on CLI config.
// Returns (writer, kafkaEnabled, error).
// Logic identical to original block in run().
func InitChangelog(cfg Config) (changelog.Writer, bool, error) {
    var clog changelog.Writer

    if cfg.ChangelogSink == "file" || cfg.ChangelogSink == "both" || cfg.ChangelogSink == "" {
        fw, err := changelog.NewFileWriter(cfg.ChangelogDir, "opb.jsonl")
        if err != nil {
            return nil, false, fmt.Errorf("init changelog file: %w", err)
        }
        clog = fw
    }
    if (cfg.ChangelogSink == "kafka" || cfg.ChangelogSink == "both") && cfg.KafkaBootstrap != "" {
        kw := changelog.NewKafkaWriter(cfg.KafkaBootstrap, cfg.TopicChangelog)
        if clog == nil {
            clog = kw
        } else {
            clog = changelog.NewMultiWriter(clog, kw)
        }
    }
    kafkaEnabled := cfg.ChangelogSink == "kafka" || cfg.ChangelogSink == "both"
    return clog, kafkaEnabled, nil
}

