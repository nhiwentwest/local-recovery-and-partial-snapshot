package main

import (
	"fmt"
	"log"
	"sync"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// input mirrors the struct defined in multi_runtime.go (kept here for reuse).
type input struct {
	topic string
	c     *ck.Consumer
}

type assignCache struct {
	mu sync.RWMutex
	m  map[string][]int32
}

type consumersBundle struct {
	inputs     []input
	assign     *assignCache
	pauseAll   func()
	resumeAll  func()
	cleanup    func()
	importOnce *sync.Once
}

// buildInputConsumers moves consumer creation & assignment tracking logic out of
// the monolithic runMultiInputRuntime.  Because the original logic also
// defined pause/resume helpers and an importOnce guard, those are returned as
// part of consumersBundle so the caller can wire them with other runtime
// components (e.g., state import during rebalance).
func buildInputConsumers(cfg Config, topics []string) (*consumersBundle, error) {
	assign := &assignCache{m: make(map[string][]int32)}
	var inputs []input

	// Guards for Pause/Resume across all inputs (filled after consumer list)
	var pauseMu sync.Mutex
	pauseAll := func() {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		for _, in := range inputs {
			ass, _ := in.c.Assignment()
			if len(ass) > 0 {
				_ = in.c.Pause(ass)
			}
		}
	}
	resumeAll := func() {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		for _, in := range inputs {
			ass, _ := in.c.Assignment()
			if len(ass) > 0 {
				_ = in.c.Resume(ass)
			}
		}
	}

	importOnce := &sync.Once{}

	// For cleanup at the end of runtime
	var closers []func()

	for i, topic := range topics {
		c, err := ck.NewConsumer(&ck.ConfigMap{
			"bootstrap.servers":             cfg.KafkaBootstrap,
			"group.id":                      fmt.Sprintf("%s-mi-%d", cfg.GroupID, i),
			"enable.auto.commit":            false,
			"isolation.level":               "read_committed",
			"auto.offset.reset":             "earliest",
			"partition.assignment.strategy": "cooperative-sticky",
			"client.id":                     fmt.Sprintf("%s-mi-%d", cfg.InstanceID, i),
			"session.timeout.ms":            cfg.SessionTimeoutMs,
			"heartbeat.interval.ms":         cfg.HeartbeatIntervalMs,
		})
		if err != nil {
			// on error close already created consumers
			for _, closer := range closers {
				closer()
			}
			return nil, fmt.Errorf("multi-input: consumer %d init: %w", i, err)
		}
		closers = append(closers, func() { _ = c.Close() })

		// rebalance callback captures topic, assign cache, pause/resume and importOnce.
		rebalanceCb := func(c *ck.Consumer, event ck.Event) error {
			switch ev := event.(type) {
			case ck.AssignedPartitions:
				if err := c.IncrementalAssign(ev.Partitions); err != nil {
					log.Printf("mi event=rebalance action=assign err=%v", err)
				}
				parts := make([]int32, 0, len(ev.Partitions))
				for _, tp := range ev.Partitions {
					parts = append(parts, tp.Partition)
				}
				assign.mu.Lock()
				assign.m[topic] = parts
				assign.mu.Unlock()
				log.Printf("mi event=rebalance action=assigned topic=%s parts=%v", topic, parts)

				if cfg.RebalanceImportState && cfg.PeersCSV != "" {
					importOnce.Do(func() {
						pauseAll() // will be unpaused in caller where state import implemented
					})
				}
			case ck.RevokedPartitions:
				if err := c.IncrementalUnassign(ev.Partitions); err != nil {
					log.Printf("mi event=rebalance action=unassign err=%v", err)
				}
				assign.mu.Lock()
				assign.m[topic] = nil
				assign.mu.Unlock()
				log.Printf("mi event=rebalance action=revoked topic=%s count=%d", topic, len(ev.Partitions))
			}
			return nil
		}

		if err := c.SubscribeTopics([]string{topic}, rebalanceCb); err != nil {
			for _, closer := range closers {
				closer()
			}
			return nil, fmt.Errorf("multi-input: subscribe %s: %w", topic, err)
		}
		inputs = append(inputs, input{topic: topic, c: c})
	}

	bundle := &consumersBundle{
		inputs:    inputs,
		assign:    assign,
		pauseAll:  pauseAll,
		resumeAll: resumeAll,
		cleanup: func() {
			for _, closer := range closers {
				closer()
			}
		},
		importOnce: importOnce,
	}
	return bundle, nil
}
