#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# The view service creates the dedicated DerivedEngine target and binding metadata.
curl --fail-with-body --silent --show-error \
  -X PUT "http://localhost:9200/_mv/views/cb100m_mv" \
  -H "Content-Type: application/json" \
  --data-binary @cb100m-mv-create.json | jq .
