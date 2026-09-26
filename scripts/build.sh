#!/usr/bin/env bash
# Configure + build. Usage: scripts/build.sh [extra cmake args...]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_PREFIX_PATH="$YEWU_ENV" "$@"
cmake --build build -j"$(nproc)"
echo "[build] done -> build/bin"
