package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func mustAdmin(bootstrap string) *ck.AdminClient {
	ac, err := ck.NewAdminClient(&ck.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		log.Fatalf("admin: %v", err)
	}
	return ac
}

func topicExists(bootstrap, topic string) (bool, int, error) {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           "kadmin-meta",
		"enable.auto.commit": false,
	})
	if err != nil {
		return false, 0, err
	}
	defer c.Close()
	md, err := c.GetMetadata(&topic, false, 5000)
	if err != nil {
		return false, 0, err
	}
	t, ok := md.Topics[topic]
	if !ok || t.Error.Code() == ck.ErrUnknownTopicOrPart {
		return false, 0, nil
	}
	return true, len(t.Partitions), nil
}

func waitTopicGone(bootstrap, topic string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ex, _, _ := topicExists(bootstrap, topic)
		if !ex {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func waitPartitionCount(bootstrap, topic string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ex, parts, _ := topicExists(bootstrap, topic)
		if ex && parts == want {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func doDeleteTopic(ac *ck.AdminClient, topic string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := ac.DeleteTopics(ctx, []string{topic})
	if err != nil {
		return err
	}
	if len(res) > 0 && res[0].Error.Code() != ck.ErrNoError && res[0].Error.Code() != ck.ErrUnknownTopicOrPart {
		return fmt.Errorf("delete: %v", res[0].Error)
	}
	return nil
}

func doCreateTopic(ac *ck.AdminClient, topic string, parts int, rf int, cfg map[string]string, timeout time.Duration) error {
	op := ck.TopicSpecification{Topic: topic, NumPartitions: parts, ReplicationFactor: rf, Config: cfg}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := ac.CreateTopics(ctx, []ck.TopicSpecification{op})
	if err != nil {
		return err
	}
	if len(res) > 0 && res[0].Error.Code() != ck.ErrNoError && res[0].Error.Code() != ck.ErrTopicAlreadyExists {
		return fmt.Errorf("create: %v", res[0].Error)
	}
	return nil
}

func doIncreasePartitions(ac *ck.AdminClient, topic string, newCount int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := ac.CreatePartitions(ctx, []ck.PartitionsSpecification{{Topic: topic, IncreaseTo: newCount}})
	if err != nil {
		return err
	}
	if len(res) > 0 && res[0].Error.Code() != ck.ErrNoError {
		return fmt.Errorf("alter: %v", res[0].Error)
	}
	return nil
}

func doDeleteGroup(ac *ck.AdminClient, group string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := ac.DeleteConsumerGroups(ctx, []string{group})
	if err != nil {
		return err
	}
	for _, cg := range res.ConsumerGroupResults {
		if cg.Error.Code() != ck.ErrNoError && cg.Error.Code() != ck.ErrGroupIDNotFound {
			return fmt.Errorf("delete-group %s: %v", cg.Group, cg.Error)
		}
	}
	return nil
}

func main() {
	var (
		bootstrap = flag.String("bootstrap", "127.0.0.1:9092", "bootstrap servers")
		cmd       = flag.String("cmd", "describe", "command: describe|delete|create|increase|delete-group")
		topic     = flag.String("topic", "", "topic name")
		group     = flag.String("group", "", "consumer group id for delete-group")
		parts     = flag.Int("partitions", 4, "partitions for create/increase")
		rf        = flag.Int("rf", 1, "replication factor for create")
		waitSec   = flag.Int("wait", 30, "wait seconds for convergence")
		configStr = flag.String("config", "", "comma-separated topic configs key=value (create only)")
	)
	flag.Parse()
	ac := mustAdmin(*bootstrap)
	defer ac.Close()
	parseConfigs := func(raw string) map[string]string {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil
		}
		cfg := make(map[string]string)
		for _, kv := range strings.Split(raw, ",") {
			kv = strings.TrimSpace(kv)
			if kv == "" {
				continue
			}
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			if key == "" {
				continue
			}
			cfg[key] = val
		}
		if len(cfg) == 0 {
			return nil
		}
		return cfg
	}
	cfgMap := parseConfigs(*configStr)
	switch *cmd {
	case "describe":
		if *topic == "" {
			log.Fatalf("missing -topic")
		}
		ex, pc, err := topicExists(*bootstrap, *topic)
		if err != nil {
			log.Fatalf("describe: %v", err)
		}
		if !ex {
			fmt.Printf("topic=%s exists=false\n", *topic)
			return
		}
		fmt.Printf("topic=%s exists=true partitions=%d\n", *topic, pc)
	case "delete":
		if *topic == "" {
			log.Fatalf("missing -topic")
		}
		if err := doDeleteTopic(ac, *topic, time.Duration(*waitSec)*time.Second); err != nil {
			log.Fatalf("delete: %v", err)
		}
		if !waitTopicGone(*bootstrap, *topic, time.Duration(*waitSec)*time.Second) {
			log.Fatalf("wait delete timeout")
		}
		fmt.Println("deleted")
	case "create":
		if *topic == "" {
			log.Fatalf("missing -topic")
		}
		if err := doCreateTopic(ac, *topic, *parts, *rf, cfgMap, time.Duration(*waitSec)*time.Second); err != nil {
			log.Fatalf("create: %v", err)
		}
		if !waitPartitionCount(*bootstrap, *topic, *parts, time.Duration(*waitSec)*time.Second) {
			log.Fatalf("wait create timeout")
		}
		fmt.Println("created")
	case "increase":
		if *topic == "" {
			log.Fatalf("missing -topic")
		}
		ex, pc, err := topicExists(*bootstrap, *topic)
		if err != nil {
			log.Fatalf("exists: %v", err)
		}
		if !ex {
			log.Fatalf("increase: topic not found")
		}
		if *parts <= pc {
			log.Fatalf("increase: new partitions %d must be > current %d", *parts, pc)
		}
		if err := doIncreasePartitions(ac, *topic, *parts, time.Duration(*waitSec)*time.Second); err != nil {
			log.Fatalf("increase: %v", err)
		}
		if !waitPartitionCount(*bootstrap, *topic, *parts, time.Duration(*waitSec)*time.Second) {
			log.Fatalf("wait increase timeout")
		}
		fmt.Println("increased")
	case "delete-group":
		if *group == "" {
			log.Fatalf("missing -group")
		}
		if err := doDeleteGroup(ac, *group, time.Duration(*waitSec)*time.Second); err != nil {
			log.Fatalf("delete-group: %v", err)
		}
		fmt.Println("group-deleted")
	default:
		log.Fatalf("unknown cmd: %s", *cmd)
	}
}
