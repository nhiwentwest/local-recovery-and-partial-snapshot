package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"

    "github.com/cockroachdb/pebble"

    "hpb/internal/state"
)

func main() {
    var storeDir string
    var key string
    flag.StringVar(&storeDir, "store", "", "path to pebble store")
    flag.StringVar(&key, "key", "", "state key to inspect")
    flag.Parse()
    if storeDir == "" || key == "" {
        log.Fatalf("usage: go run tools/dump_key.go -store <dir> -key <stateKey>")
    }
    db, err := pebble.Open(storeDir, &pebble.Options{})
    if err != nil {
        log.Fatalf("open store: %v", err)
    }
    defer db.Close()
    val, closer, err := db.Get([]byte(key))
    if err != nil {
        log.Fatalf("get key: %v", err)
    }
    defer closer.Close()
    var st state.RecordState
    if err := json.Unmarshal(val, &st); err != nil {
        log.Fatalf("decode: %v", err)
    }
    fmt.Printf("key=%s lastSeq=%d sumQty=%d sumAmount=%d\n", key, st.LastSeq, st.SumQty, st.SumAmount)
}
