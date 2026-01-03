package main

import (
    "context"
    "fmt"

    ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// initMiProducers creates the transactional data producer (used for barrier
// propagation) and the injector producer (best-effort). It mirrors exactly the
// configuration previously in runMultiInputRuntime.
func initMiProducers(cfg Config) (prod *ck.Producer, inj *ck.Producer, err error) {
    pCfg := &ck.ConfigMap{
        "bootstrap.servers":  cfg.KafkaBootstrap,
        "enable.idempotence": true,
        "acks":               "all",
        "transactional.id":   fmt.Sprintf("opb-mi-%s", cfg.InstanceID),
        "linger.ms":          5,
        "compression.type":   "lz4",
    }
    prod, err = ck.NewProducer(pCfg)
    if err != nil {
        return nil, nil, fmt.Errorf("producer: %w", err)
    }
    if err = prod.InitTransactions(context.TODO()); err != nil {
        prod.Close()
        return nil, nil, fmt.Errorf("init tx: %w", err)
    }

    inj, _ = ck.NewProducer(&ck.ConfigMap{
        "bootstrap.servers": cfg.KafkaBootstrap,
        "linger.ms":         5,
        "compression.type":  "lz4",
    })
    return
}

