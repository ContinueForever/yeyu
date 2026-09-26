#!/usr/bin/env bash
# Source this file before configuring/building:
#   source scripts/env.sh
# Activates the user-space conda toolchain installed under ~/.micromamba.
_YEWU_ENV="${YEWU_TOOLCHAIN:-$HOME/.micromamba/envs/yewu}"
if [[ ! -x "$_YEWU_ENV/bin/cmake" ]]; then
  echo "[env] toolchain not found at $_YEWU_ENV" >&2
  return 1 2>/dev/null || exit 1
fi
export PATH="$_YEWU_ENV/bin:$PATH"
export CC="$_YEWU_ENV/bin/x86_64-conda-linux-gnu-gcc"
export CXX="$_YEWU_ENV/bin/x86_64-conda-linux-gnu-g++"
export CMAKE_PREFIX_PATH="$_YEWU_ENV${CMAKE_PREFIX_PATH:+:$CMAKE_PREFIX_PATH}"
export YEWU_ENV="$_YEWU_ENV"
echo "[env] yewukv toolchain: $($_YEWU_ENV/bin/x86_64-conda-linux-gnu-g++ --version | head -1)"
