#!/usr/bin/env bash
set -euo pipefail

OS_URL="${OS_URL:-http://localhost:9200}"
SOURCE_INDEX="${SOURCE_INDEX:-cb100m}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BODY="$(mktemp)"
DESCRIPTOR="$(mktemp)"
trap 'rm -f "$BODY" "$DESCRIPTOR"' EXIT

jq -n -f "$SCRIPT_DIR/cb100m-mv-descriptor.jq" > "$DESCRIPTOR"
jq -n --arg source "$SOURCE_INDEX" --slurpfile descriptor "$DESCRIPTOR" \
  '{source_index: $source, descriptor: $descriptor[0]}' > "$BODY"

curl --fail-with-body --silent --show-error \
  -X POST "$OS_URL/_mv/_validate" \
  -H 'Content-Type: application/json' \
  --data-binary "@$BODY" | jq .
