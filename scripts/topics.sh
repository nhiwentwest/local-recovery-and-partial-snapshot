#!/usr/bin/env bash
set -euo pipefail

# Create topics for p2 prefix using Redpanda rpk (or kafka-topics if using Kafka)
# Requires: redpanda container running from docker-compose.yml

if ! command -v rpk >/dev/null 2>&1; then
  echo "rpk not found. Install Redpanda rpk or exec into container: docker exec -it redpanda rpk ..." >&2
  exit 1
fi

# Standard topics
rpk topic create p2.orders.enriched p2.orders.output || true

# Compacted topics for changelog/manifest
rpk topic create p2.opb-changelog --config cleanup.policy=compact || true
rpk topic create p2.opb-snapshots --config cleanup.policy=compact || true

echo "Topics ensured: p2.orders.enriched, p2.orders.output, p2.opb-changelog(compacted), p2.opb-snapshots(compacted)"
