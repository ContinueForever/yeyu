#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
project="yewukv-ci-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose --project-name "$project" --file deploy/docker-compose.yml)
key="ci-smoke-$project"
value=survived-recreate

cleanup() {
  status=$?
  if (( status != 0 )); then
    "${compose[@]}" ps --all || true
    "${compose[@]}" logs --no-color || true
  fi
  "${compose[@]}" down --volumes --remove-orphans || true
  exit "$status"
}
trap cleanup EXIT

client() {
  "${compose[@]}" exec -T node /usr/local/bin/yewukv-client \
    --host 127.0.0.1 --port 9000 "$@"
}

assert_value() {
  actual=$(client get "$key")
  if [[ "$actual" != "$value" ]]; then
    echo "expected '$value', got '$actual'" >&2
    return 1
  fi
}

"${compose[@]}" up --build --wait --wait-timeout 60
[[ "$(client put "$key" "$value")" == "OK" ]]
assert_value

"${compose[@]}" restart node
"${compose[@]}" up --wait --wait-timeout 60
assert_value

"${compose[@]}" down
"${compose[@]}" up --wait --wait-timeout 60
assert_value

echo "Compose durability smoke test passed: write, restart, recreate"
