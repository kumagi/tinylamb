#!/usr/bin/env python3
"""Sweep throwaway database artifacts out of the repository root.

Tests, benchmarks, servers and fuzzers open databases through relative
paths, so every run strands <name>.db / <name>.log / <name>.last_checkpoint
next to wherever it was launched -- typically the project root. The gtest
workspace-cleanup environment (database/test_workspace_cleanup_env.cpp)
catches the test binaries; this script is the belt-and-braces sweep used
manually and by CI after suites whose child processes (death-test probes,
spawned servers) can outlive the parent's cleanup.

Only files matching a known artifact pattern AND ignored (or unknown) to
git are removed, so a tracked fixture is never touched.

Usage: scripts/clean_root_artifacts.py [--dry-run] [dirs...]
"""

import argparse
import fnmatch
import os
import subprocess
import sys
from pathlib import Path

PATTERNS = (
    "*.db",
    "*.db.tmp",
    "*.log",
    "*.last_checkpoint",
    "*.wal",
)

# Fuzzing byproducts worth deleting only with --fuzz-artifacts: a repro
# .test or crash input is evidence you usually want to read (or commit to
# query/testdata/) before it goes away.
FUZZ_PATTERNS = (
    "crash-*",
    "leak-*",
    "timeout-*",
    "sql_oracle_fuzz-repro-*.test",
    "expr_oracle_fuzz-repro-*.test",
    "griffin_fuzz-repro-*.test",
    "sql_session_fuzz-repro-*.test",
)


def tracked_files(root: Path) -> set:
    try:
        out = subprocess.run(
            ["git", "ls-files"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return set()
    return {line for line in out.splitlines() if line}


def is_artifact(name: str, fuzz_artifacts: bool) -> bool:
    if any(fnmatch.fnmatch(name, pattern) for pattern in PATTERNS):
        return True
    return fuzz_artifacts and any(
        fnmatch.fnmatch(name, pattern) for pattern in FUZZ_PATTERNS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="list what would be removed; delete nothing")
    parser.add_argument("--fuzz-artifacts", action="store_true",
                        help="also remove crash/timeout inputs and "
                             "*-repro-*.test files")
    parser.add_argument("dirs", nargs="*", default=None,
                        help="directories to sweep (default: repo root)")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    roots = [Path(d) for d in args.dirs] if args.dirs else [repo_root]
    tracked = tracked_files(repo_root)

    removed = 0
    for root in roots:
        if not root.is_dir():
            print(f"skip (not a directory): {root}", file=sys.stderr)
            continue
        for entry in sorted(root.iterdir()):
            if not entry.is_file() or not is_artifact(
                    entry.name, args.fuzz_artifacts):
                continue
            try:
                relative = entry.resolve().relative_to(repo_root.resolve())
            except ValueError:
                relative = None
            if relative is not None and str(relative) in tracked:
                continue  # a versioned fixture that merely looks like debris
            verb = "would remove" if args.dry_run else "removed"
            print(f"{verb}: {entry}")
            if not args.dry_run:
                try:
                    os.remove(entry)
                    removed += 1
                except OSError as error:
                    print(f"  failed: {error}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
