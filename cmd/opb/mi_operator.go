package main

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"

    ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

    "hpb/internal/opb"
)

// wireOperatorBasics moves the non-snapshot parts (Expected / Propagate / Block / Unblock)
// out of runMultiInputRuntime. Behaviour is unchanged.
//
//  op      – operator instance to configure
//  prod    – transactional producer used to propagate barrier markers
//  cfg     – CLI config (needed for topic names)
//  assign  – pointer to assignment cache (topic -> partitions) used in Expected()
func wireOperatorBasics(op *opb.DynamicNInputOperator, prod *ck.Producer, cfg Config, assign *struct {
    mu    sync.RWMutex
    m     map[string][]int32
}) {
    // Expected provider based on current assignment across all input topics
    op.Expected = func() []string {
        assign.mu.RLock()
        defer assign.mu.RUnlock()
        var keys []string
        for t, parts := range assign.m {
            for _, p := range parts {
                keys = append(keys, fmt.Sprintf("%s#%d", t, p))
            }
        }
        return keys
    }

    // Propagate barrier: on first marker, send to ALL partitions of output topic
    op.Propagate = func(m opb.Marker) {
        var md *ck.Metadata
        var merr error
        for i := 0; i < 3; i++ {
            md, merr = prod.GetMetadata(&cfg.OutputTopic, false, int((3 * time.Second).Milliseconds()))
            if merr == nil {
                break
            }
            time.Sleep(200 * time.Millisecond)
        }
        if merr != nil {
            log.Printf("mi event=propagate stage=metadata status=failed id=%s topic=%s err=%v", m.SnapshotID, cfg.OutputTopic, merr)
            return
        }
        tp, ok := md.Topics[cfg.OutputTopic]
        if !ok {
            log.Printf("mi event=propagate status=failed id=%s topic=%s err=%s", m.SnapshotID, cfg.OutputTopic, "not-found")
            return
        }
        h := opb.BarrierHeaders(m.SnapshotID)
        _ = prod.BeginTransaction()
        for _, part := range tp.Partitions {
            _ = prod.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.OutputTopic, Partition: int32(part.ID)}, Key: []byte("barrier"), Headers: h}, nil)
        }
        var cerr error
        for i := 0; i < 2; i++ {
            cerr = prod.CommitTransaction(context.TODO())
            if cerr == nil {
                break
            }
            time.Sleep(150 * time.Millisecond)
        }
        if cerr != nil {
            log.Printf("mi event=propagate stage=commit status=failed id=%s topic=%s err=%v", m.SnapshotID, cfg.OutputTopic, cerr)
            return
        }
        log.Printf("mi event=propagate status=committed id=%s topic=%s partitions=%d", m.SnapshotID, cfg.OutputTopic, len(tp.Partitions))
    }

    // Logging for block/unblock
    op.OnBlock = func(ch string) { log.Printf("mi event=block channel=%s cutId=%s", ch, op.CurCutID()) }
    op.OnUnblock = func() { log.Printf("mi event=unblock cutId=%s", op.CurCutID()) }
}

