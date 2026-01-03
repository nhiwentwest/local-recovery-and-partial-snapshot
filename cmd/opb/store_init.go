package main

import (
    "fmt"

    "hpb/internal/state"
)

// InitStateStore creates and configures the state.Store implementation based on
// CLI flags. It returns the store instance and a cleanup callback (which may be
// nil if no cleanup is required).
//
// NOTE: This helper is extracted from the original run() function without any
// behavioural changes in order to lower that function's cyclomatic complexity.
func InitStateStore(cfg Config) (state.Store, func() error, error) {
    var st state.Store
    var cleanup func() error

    switch cfg.StateBackend {
    case "pebble":
        ps, err := state.NewPebbleStore(cfg.StateDir)
        if err != nil {
            return nil, nil, fmt.Errorf("init pebble: %w", err)
        }
        st = ps
        cleanup = ps.Close
    case "memory":
        st = state.NewInMemoryStore()
    default:
        return nil, nil, fmt.Errorf("unknown state-backend: %s (use pebble|memory)", cfg.StateBackend)
    }

    // Apply InstanceID for LastUpdatedBy visibility. This is transient runtime
    // information and therefore safe to keep here.
    switch v := st.(type) {
    case *state.InMemoryStore:
        v.SetInstanceID(cfg.InstanceID)
    case *state.PebbleStore:
        v.SetInstanceID(cfg.InstanceID)
    }

    return st, cleanup, nil
}

