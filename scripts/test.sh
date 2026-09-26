#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
ctest --test-dir build --output-on-failure "$@"
