#!/usr/bin/env bash
set -euo pipefail

OS_URL="${OS_URL:-http://localhost:9200}"
TARGET_INDEX="${TARGET_INDEX:-cb100m_mv}"

curl --fail-with-body --silent --show-error \
  -X PUT "$OS_URL/$TARGET_INDEX/_settings" \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "index.mv_pull.max_docs_per_round": 2000000,
    "index.mv_pull.max_generations_before_compact": 1000000
  }' | jq .
