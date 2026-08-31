#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
RUNS=5
TIMEOUT_SECONDS=900
RESULTS_ROOT="$ROOT/sandbox/qa/mv-pull-chaos/results"
BASE_SEED="5A17E1C500000000"

usage() {
  cat <<'EOF'
Usage: run.sh [--runs N] [--timeout-seconds N] [--base-seed HEX] [--results DIR]

Runs the separate-index pull-MV lifecycle chaos test once per deterministic
seed. Each run validates A (DataFusion Final over mv_state) against B (the
source-derived expected model) through replica convergence, primary relocation,
full node-process restart, post-restart pulling, compaction, and orphan cleanup.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) RUNS=$2; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS=$2; shift 2 ;;
    --base-seed) BASE_SEED=${2#0x}; shift 2 ;;
    --results) RESULTS_ROOT=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || { echo "--runs must be positive" >&2; exit 2; }
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || { echo "--timeout-seconds must be positive" >&2; exit 2; }
[[ "$BASE_SEED" =~ ^[0-9A-Fa-f]{1,16}$ ]] || { echo "--base-seed must contain at most 16 hex digits" >&2; exit 2; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$ROOT" rev-parse --short HEAD)"
OUT="$RESULTS_ROOT/$RUN_ID"
mkdir -p "$OUT/logs"

{
  echo "run_id=$RUN_ID"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "root=$ROOT"
  echo "git_head=$(git -C "$ROOT" rev-parse HEAD)"
  echo "git_branch=$(git -C "$ROOT" branch --show-current)"
  echo "runs=$RUNS"
  echo "base_seed=$BASE_SEED"
  echo "timeout_seconds=$TIMEOUT_SECONDS"
  echo "cargo_profile_release_lto=false"
  uname -a
  java -version 2>&1
  git -C "$ROOT" status --short
} > "$OUT/environment.txt"

printf '%s\n' \
  'A=DataFusion Final over primary and replica-local mv_state Arrow artifacts' \
  'B=deterministic source-side expected group count/sum model' \
  'faults=primary relocation, full process restart, role reassignment, compaction' \
  'invariants=exactness, atomic files+W, one primary poller, zero replica pollers, zero target translog ops, orphan deletion' \
  > "$OUT/matrix.txt"

failures=0
for ((i=0; i<RUNS; i++)); do
  seed=$(python3 - "$BASE_SEED" "$i" <<'PY'
import sys
print(f"{(int(sys.argv[1], 16) + int(sys.argv[2])) & ((1 << 64) - 1):016X}")
PY
)
  log="$OUT/logs/run-$(printf '%03d' "$i")-$seed.log"
  result="$OUT/run-$(printf '%03d' "$i").json"
  started=$(date +%s)
  command=(
    ./gradlew
    :sandbox:plugins:mv-pull-engine:internalClusterTest
    --tests org.opensearch.mvpull.MVPullDataFusionIT.testDataFusionFoldedPullMatchesSource
    -Dtests.seed="$seed"
    --console=plain
  )
  printf '%q ' "${command[@]}" > "$OUT/run-$(printf '%03d' "$i").command"
  printf '\n' >> "$OUT/run-$(printf '%03d' "$i").command"
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] run=$i seed=$seed" | tee -a "$OUT/commands.log"

  set +e
  CARGO_PROFILE_RELEASE_LTO=false python3 - "$TIMEOUT_SECONDS" "$ROOT" "$log" "${command[@]}" <<'PY'
import os, selectors, subprocess, sys, time
limit, cwd, log, *cmd = sys.argv[1:]
limit = int(limit)
started = time.monotonic()
with open(log, "w", encoding="utf-8") as out:
    process = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, bufsize=1, start_new_session=True,
                               env={**os.environ, "CARGO_PROFILE_RELEASE_LTO": "false"})
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    timed_out = False
    while process.poll() is None:
        for key, _ in selector.select(timeout=1):
            line = key.fileobj.readline()
            if line:
                out.write(line)
                out.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
        if time.monotonic() - started > limit:
            timed_out = True
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
            break
    remainder = process.stdout.read()
    if remainder:
        out.write(remainder)
        sys.stdout.write(remainder)
    rc = 124 if timed_out else process.wait()
sys.exit(rc)
PY
  rc=$?
  set -e
  ended=$(date +%s)
  duration=$((ended-started))
  [[ $rc -eq 0 ]] || failures=$((failures+1))
  python3 - "$result" "$i" "$seed" "$rc" "$duration" "$log" <<'PY'
import json, pathlib, sys
path, run, seed, rc, duration, log = sys.argv[1:]
pathlib.Path(path).write_text(json.dumps({
    "run": int(run), "seed": seed, "exitCode": int(rc),
    "status": "PASS" if int(rc) == 0 else ("TIMEOUT" if int(rc) == 124 else "FAIL"),
    "durationSeconds": int(duration), "log": log
}, indent=2) + "\n")
PY
  echo "run=$i seed=$seed rc=$rc duration_seconds=$duration" | tee -a "$OUT/commands.log"
done

python3 - "$OUT" "$RUN_ID" "$RUNS" "$failures" <<'PY'
import json, pathlib, sys
out, run_id, runs, failures = sys.argv[1:]
root = pathlib.Path(out)
items = [json.loads(p.read_text()) for p in sorted(root.glob("run-*.json"))]
(root / "results.json").write_text(json.dumps({
    "runId": run_id, "status": "PASS" if int(failures) == 0 else "FAIL",
    "runs": int(runs), "passed": int(runs) - int(failures),
    "failed": int(failures), "results": items
}, indent=2) + "\n")
PY

cat "$OUT/results.json"
echo "Artifacts: $OUT"
[[ $failures -eq 0 ]]
