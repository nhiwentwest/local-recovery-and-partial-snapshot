package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"hpb/internal/opb"
)

func main() {
	var count int
	var outputFile string
	flag.IntVar(&count, "count", 100, "number of orders to generate")
	flag.StringVar(&outputFile, "output", "p2.orders.enriched.jsonl", "output file")
	flag.Parse()

	if err := generateOrders(count, outputFile); err != nil {
		log.Fatalf("generation failed: %v", err)
	}
}

func generateOrders(count int, outputFile string) error {
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	stores := []string{"A", "B", "C"}
	products := []string{"p1", "p2", "p3", "p4", "p5"}

	baseTime := time.Now().UTC().Unix()
	rand.Seed(time.Now().UnixNano())

	enc := json.NewEncoder(file)
	for i := 0; i < count; i++ {
		order := opb.OrderEnriched{
			OrderID:   fmt.Sprintf("o%d", i+1),
			ProductID: products[rand.Intn(len(products))],
			Price:     int64(1000 + rand.Intn(9000)), // 1000-9999
			Qty:       int64(1 + rand.Intn(5)),       // 1-5
			StoreID:   stores[rand.Intn(len(stores))],
			TS:        baseTime + int64(i*10), // 10s intervals
			Validated: true,
			NormTS:    baseTime + int64(i*10),
		}
		if err := enc.Encode(&order); err != nil {
			return fmt.Errorf("encode order %d: %w", i+1, err)
		}
	}

	log.Printf("generated %d orders to %s", count, outputFile)
	return nil
}
