#!/usr/bin/env bash
# Configure + build. Usage: scripts/build.sh [extra cmake args...]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
cmake_args=()
if [[ -n "$YEWU_ENV" ]]; then
  cmake_args+=("-DCMAKE_PREFIX_PATH=$YEWU_ENV")
fi
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  "${cmake_args[@]}" "$@"
cmake --build build -j"$(nproc)"
echo "[build] done -> build/bin"
