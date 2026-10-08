#!/usr/bin/env sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/tide-edbt-build.XXXXXX")
trap 'rm -rf "$build_dir"' EXIT HUP INT TERM

cmake -S "$repo_dir/fpdb-tide" -B "$build_dir" -DTIDE_BUILD_TESTS=ON
cmake --build "$build_dir" -j 2
(cd "$build_dir" && ctest --output-on-failure)
