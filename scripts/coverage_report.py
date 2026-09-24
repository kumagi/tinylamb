#!/usr/bin/env python3
# Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
"""Measure line coverage of the production sources.

Prerequisite: a build with -DTINYLAMB_ENABLE_COVERAGE=ON (see build-cov) and
a full ctest run so the .gcda files are fresh:

    cmake --build build-cov -j
    ctest --test-dir build-cov -j

Usage:
    python3 scripts/coverage_report.py [--build build-cov] [--top 40]
                                       [--info /tmp/cov_prod.info]

Prints the overall line coverage over production sources (test binaries,
fuzzers, benchmarks and vendored dependencies are excluded) plus a ranking of
the files with the most uncovered lines.
"""

import argparse
import collections
import os
import subprocess
import sys

# This lcov caps how many error kinds a single --ignore-errors flag
# accepts, so the kinds are split across two flags.
IGNORE_A = "negative,mismatch,unused,empty,corrupt,unsupported,format"
IGNORE_B = "inconsistent,count,category,source,gcov"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", default="build-cov")
    parser.add_argument("--top", type=int, default=40)
    parser.add_argument("--info", default="/tmp/cov_prod.info")
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ignore_flags = [
        "--ignore-errors",
        IGNORE_A,
        "--ignore-errors",
        IGNORE_B,
    ]
    capture = subprocess.run(
        ["lcov", "--capture", f"--directory={root}",
         f"--output-file={args.info}", "--no-external"] + ignore_flags,
        capture_output=True,
        text=True,
    )
    if capture.returncode != 0:
        print(capture.stderr[-2000:], file=sys.stderr)
        return 1

    # Exclude tests, fuzzers, benchmarks, generated deps and CLI mains: the
    # report targets the production engine sources.
    remove = subprocess.run(
        ["lcov", "--remove", args.info, "*_test*", "*fuzzer*",
         "*/benchmark/*", "*main.cpp", "*/build-*/_deps/*", "*/.freebuff/*"]
        + ignore_flags
        + [f"--output-file={args.info}"],
        capture_output=True,
        text=True,
    )
    if remove.returncode != 0:
        print(remove.stderr[-2000:], file=sys.stderr)
        return 1

    files: dict[str, list[int]] = {}
    current = None
    for line in open(args.info, encoding="utf-8"):
        if line.startswith("SF:"):
            current = line[3:].strip()
            files[current] = [0, 0]
        elif line.startswith("DA:"):
            parts = line[3:].split(",")
            hit = int(parts[1])
            files[current][1] += 1
            if hit == 0:
                files[current][0] += 1

    total_unc = sum(v[0] for v in files.values())
    total = sum(v[1] for v in files.values())
    print(f"production files: {len(files)}")
    print(
        f"line coverage: {100 * (total - total_unc) / total:.1f}% "
        f"({total - total_unc}/{total})"
    )

    per_dir: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
    for path, (unc, lines) in files.items():
        rel = os.path.relpath(path, root)
        top = rel.split(os.sep)[0]
        per_dir[top][0] += unc
        per_dir[top][1] += lines
    print("\nper-directory:")
    for top, (unc, lines) in sorted(
        per_dir.items(), key=lambda kv: -kv[1][0]
    ):
        if lines == 0:
            continue
        print(
            f"  {top:14} {100 * (lines - unc) / lines:5.1f}% "
            f"({unc} uncovered of {lines})"
        )

    print(f"\ntop {args.top} uncovered files:")
    for path, (unc, lines) in sorted(
        files.items(), key=lambda kv: -kv[1][0]
    )[: args.top]:
        cov = 100 * (lines - unc) / lines if lines else 100.0
        print(
            f"  {unc:>6}  {cov:5.1f}%  {os.path.relpath(path, root)}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
