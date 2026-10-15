#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build_dir=${1:-"$root/build-verification"}
log_file=${2:-"$build_dir/verification.log"}
jobs=${JOBS:-$(nproc)}

mkdir -p "$build_dir"
cmake -S "$root" -B "$build_dir" -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DTINYLAMB_ENABLE_GOOGLESQL=ON \
  -DBUILD_TESTING=ON
cmake --build "$build_dir" -j"$jobs"

{
  printf 'root=%s\n' "$root"
  printf 'build_dir=%s\n' "$build_dir"
  printf 'head=%s\n' "$(git -C "$root" rev-parse HEAD)"
  printf 'build_type=RelWithDebInfo\n'
  printf 'ctest_jobs=%s\n' "${CTEST_JOBS:-$jobs}"
  ctest --test-dir "$build_dir" --output-on-failure --no-tests=error \
    -j"${CTEST_JOBS:-$jobs}"
  python3 "$root/scripts/check_layering.py" --root "$root"
} 2>&1 | tee "$log_file"
