#!/usr/bin/env bash
set -euo pipefail

OS_URL="${OS_URL:-http://localhost:9200}"
SOURCE_INDEX="${SOURCE_INDEX:-cb100m}"
TARGET_INDEX="${TARGET_INDEX:-cb100m_mv}"
VIEW_NAME="${VIEW_NAME:-cb100m_mv}"
POLL_INTERVAL="${POLL_INTERVAL:-1s}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BODY="$(mktemp)"
DESCRIPTOR="$(mktemp)"
trap 'rm -f "$BODY" "$DESCRIPTOR"' EXIT

jq -n -f "$SCRIPT_DIR/cb100m-mv-descriptor.jq" > "$DESCRIPTOR"
jq -n \
  --arg source "$SOURCE_INDEX" \
  --arg target "$TARGET_INDEX" \
  --arg poll "$POLL_INTERVAL" \
  --slurpfile descriptor "$DESCRIPTOR" \
  '{source_index: $source, target_index: $target, poll_interval: $poll, descriptor: $descriptor[0]}' > "$BODY"

# The view service creates the dedicated DerivedEngine target and binding metadata.
curl --fail-with-body --silent --show-error \
  -X PUT "$OS_URL/_mv/views/$VIEW_NAME" \
  -H 'Content-Type: application/json' \
  --data-binary "@$BODY" | jq .
