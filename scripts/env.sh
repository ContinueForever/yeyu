#!/usr/bin/env bash
# Source this file before configuring/building:
#   source scripts/env.sh
# Prefer an explicitly selected or existing legacy Conda environment; otherwise
# use the system toolchain documented in README.md.
_YEWU_ENV="${YEWU_TOOLCHAIN:-$HOME/.micromamba/envs/yewu}"
export YEWU_ENV=""
if [[ -z "${YEWU_TOOLCHAIN:-}" && ! -x "$_YEWU_ENV/bin/cmake" ]]; then
  if ! command -v cmake >/dev/null || ! command -v ctest >/dev/null; then
    echo "[env] install the system dependencies listed in README.md" >&2
    return 1 2>/dev/null || exit 1
  fi
  return 0 2>/dev/null || exit 0
fi
for _YEWU_TOOL in cmake ctest x86_64-conda-linux-gnu-gcc x86_64-conda-linux-gnu-g++; do
  if [[ ! -x "$_YEWU_ENV/bin/$_YEWU_TOOL" ]]; then
    echo "[env] toolchain executable missing: $_YEWU_ENV/bin/$_YEWU_TOOL" >&2
    return 1 2>/dev/null || exit 1
  fi
done
export PATH="$_YEWU_ENV/bin:$PATH"
export CC="$_YEWU_ENV/bin/x86_64-conda-linux-gnu-gcc"
export CXX="$_YEWU_ENV/bin/x86_64-conda-linux-gnu-g++"
export CMAKE_PREFIX_PATH="$_YEWU_ENV${CMAKE_PREFIX_PATH:+:$CMAKE_PREFIX_PATH}"
export YEWU_ENV="$_YEWU_ENV"
echo "[env] yewukv toolchain: $($_YEWU_ENV/bin/x86_64-conda-linux-gnu-g++ --version | head -1)"
