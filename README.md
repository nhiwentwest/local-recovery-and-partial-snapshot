# HPB - Quickstart (Bounded Run)

This repo includes scripts to run a reproducible, bounded 60s benchmark end-to-end on a local single-node Redpanda container.

## Prerequisites
- Docker + Docker Compose
- Go 1.21+
- rpk (Redpanda CLI, via Homebrew: `brew install redpanda-data/tap/redpanda`)

## One-click 60s Benchmark
```bash
# From repo root
bash scripts/run-bench-60s.sh
```
What it does:
- Starts a single-node Redpanda container
- Builds `opb`
- Creates topics `p2.orders.enriched.60s` (60 partitions) and `orders.output.60s` (24 partitions)
- Launches 6 `opb` replicas with EOS enabled
- Runs a 60s load benchmark (~600k messages total)
- Prints group/topic summary, then shuts everything down cleanly

Expected outcome:
- Group total lag should be ~0 at the end for the configured load

## Stop / Cleanup
```bash
bash scripts/stop-all.sh
```
Stops all `opb` processes and tears down the Redpanda container/network.

## Notes
- If you want to change load, edit variables in `scripts/run-bench-60s.sh` (DURATION, PROCS, RPS_PER_PROC).
- For native (non-Docker) Redpanda, ensure the `redpanda` binary is installed and in PATH, then adapt the script broker address to `127.0.0.1:9092`.
