#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

curl --fail-with-body --silent --show-error \
  -X POST "http://localhost:9200/_mv/_validate" \
  -H "Content-Type: application/json" \
  --data-binary @cb100m-mv-validate.json | jq .
