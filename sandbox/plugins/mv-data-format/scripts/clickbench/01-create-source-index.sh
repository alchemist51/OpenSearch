#!/usr/bin/env bash
set -euo pipefail

OS_URL="${OS_URL:-http://localhost:9200}"
SOURCE_INDEX="${SOURCE_INDEX:-cb100m}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
BASE_MAPPING="$REPO_ROOT/sandbox/plugins/analytics-engine/src/test/resources/clickbench/mappings/clickbench_index_mapping.json"
BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT

# Source only: Parquet primary + Lucene secondary. Never activate an MV ship path here.
jq '
  .settings = {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "index.queries.cache.enabled": false,
    "index.requests.cache.enable": false,
    "index.codec": "best_compression",
    "index.translog.sync_interval": "30s",
    "index.translog.durability": "async",
    "index.refresh_interval": "10s",
    "index.sort.field": ["CounterID", "EventDate", "UserID", "EventTime", "WatchID"],
    "index.sort.order": ["desc", "desc", "desc", "desc", "desc"],
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": ["lucene"]
  }
  | .mappings.dynamic = "false"
  | .mappings.properties.EventTime.format = "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
' "$BASE_MAPPING" > "$BODY"

curl --fail-with-body --silent --show-error \
  -X PUT "$OS_URL/$SOURCE_INDEX" \
  -H 'Content-Type: application/json' \
  --data-binary "@$BODY" | jq .
