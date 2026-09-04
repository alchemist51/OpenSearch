#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

curl --fail-with-body --silent --show-error \
  -X PUT "http://localhost:9200/cb100m" \
  -H "Content-Type: application/json" \
  --data-binary @cb100m-source-index.json | jq .
