# HPB - OpB (Aggregator, Changelog, Snapshot, Manifest)

This repository scaffolds the OpB service (Người 2) for the local-recovery-and-partial-snapshot project.

## Components

- cmd/opb: Main entry for OpB
- internal/state: Key-value state store abstraction (Phase 1: in-memory placeholder)
- internal/snapshot: Snapshot writer/reader (filesystem-based stub)
- internal/manifest: Manifest publisher/reader (filesystem-based stub)
- scripts/topics.sh: Helper script to create Kafka topics (placeholder)

## Build & Run (local, Phase 1)

```bash
make build
./bin/opb --topic-prefix p2 --snapshot-dir ./snapshots --badger-dir ./data/opb
```

Notes:
- Phase 1 uses in-memory state and filesystem snapshots to validate the control flow.
- Kafka client and BadgerDB integration will be added next.

## Flags

- --topic-prefix: topic prefix (e.g., p2)
- --group-id: consumer group id (default: opb)
- --window-size: aggregation window seconds (default: 300)
- --snapshot-interval: seconds between snapshots (default: 60)
- --changelog: on|off toggle for changelog emission (default: on)
- --snapshot-dir: directory to store snapshots
- --badger-dir: directory for state (reserved for Badger; not used in Phase 1)

## Layout

```
├─ README.md
├─ docker-compose.yml
├─ Makefile
├─ cmd/
│  └─ opb/
│     └─ main.go
├─ internal/
│  ├─ manifest/
│  │  └─ manifest.go
│  ├─ snapshot/
│  │  └─ snapshot.go
│  └─ state/
│     └─ state.go
└─ scripts/
   └─ topics.sh
```
