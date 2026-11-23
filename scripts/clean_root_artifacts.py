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

Usage: scripts/clean_root_artifacts.py [--dry-run] [--fuzz-artifacts] [dirs...]
"""

import argparse
import fnmatch
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PATTERNS = (
    "*.db",
    "*.db.tmp",
    "*.db.blocks*",
    "*.db.lsm*",
    "*.db.tlcs*",
    "*.log",
    "*.log.meta",
    "*.log.meta.tmp",
    "*.log.[0-9]*",
    "*.last_checkpoint",
    "*.last_checkpoint.tmp",
    "*.wal",
    "tinylamb-spill-*.bin",
)

# Fuzzing byproducts worth deleting only with --fuzz-artifacts: a repro
# .test or crash input is evidence you usually want to read (or commit to
# query/testdata/) before it goes away.
FUZZ_PATTERNS = (
    "crash-*",
    "leak-*",
    "timeout-*",
    "oom-*",
    "*_fuzz-repro-*.test",
    # harness-run finding archives (SUMMARY.md + raw inputs), e.g. the
    # expression_fuzzer artifacts of issue #33; kept by default like the
    # other evidence patterns, removed only with --fuzz-artifacts.
    ".fuzz_findings*",
)


def tracked_files(root: Path) -> set | None:
    try:
        out = subprocess.run(
            ["git", "ls-files"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as error:
        # Without the tracked set we cannot tell versioned fixtures from
        # debris; the caller must not treat "unknown" as "untracked".
        print(f"warning: git ls-files failed ({error}); tracked-file "
              "protection is disabled", file=sys.stderr)
        return None
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
    tracked = tracked_files(repo_root)

    # Precompute the ancestor directories of every tracked file once. The
    # recursive findings-dir check below then is O(1) per detection instead
    # of rescanning all of `git ls-files` for every bucket directory.
    tracked_dirs: set[str] = set()
    if tracked is not None:
        for name in tracked:
            parent = os.path.dirname(name)
            while parent:
                tracked_dirs.add(parent)
                parent = os.path.dirname(parent)

    removed = 0

    def sweep_directory(root: Path, temp_only: bool = False) -> None:
        nonlocal removed
        if not root.is_dir():
            print(f"skip (not a directory): {root}", file=sys.stderr)
            return
        for entry in sorted(root.iterdir()):
            if not entry.is_file():
                # Findings archive dirs (issue #33, e.g. .fuzz_findings_small)
                # are swept recursively, but only with --fuzz-artifacts.
                if (entry.is_dir() and args.fuzz_artifacts
                        and is_artifact(entry.name, True)):
                    # .fuzz_findings* is an add-only evidence store: retiring a
                    # finding must go through `fuzz_collector.py --retire` so
                    # retired.jsonl records the decision. A sweep must never
                    # recursively delete it.
                    if entry.name.startswith(".fuzz_findings"):
                        print(f"kept (add-only findings store): {entry}/")
                        continue
                    try:
                        relative_dir = entry.resolve().relative_to(
                            repo_root.resolve())
                    except ValueError:
                        continue
                    # When git failed, `tracked` is None: the tracked set is
                    # unknown, so nothing may be deleted (same rule as the
                    # regular-file branch below).
                    if tracked is None or str(relative_dir) in tracked_dirs:
                        continue  # unknown or versioned findings dir
                    verb = "would remove" if args.dry_run else "removed"
                    print(f"{verb}: {entry}/")
                    if not args.dry_run:
                        try:
                            shutil.rmtree(entry)
                        except OSError as exc:
                            print(f"could not remove {entry}/: {exc}")
                        else:
                            removed += 1
                continue
            if temp_only:
                if not fnmatch.fnmatch(entry.name, "tinylamb-spill-*.bin"):
                    continue
            elif not is_artifact(entry.name, args.fuzz_artifacts):
                continue
            try:
                relative = entry.resolve().relative_to(repo_root.resolve())
            except ValueError:
                relative = None
            if not temp_only and relative is None:
                # Outside the repository we cannot consult git at all, so
                # we cannot prove this is untracked debris. Never delete it.
                continue
            if relative is not None:
                if tracked is None:
                    continue  # cannot prove the file is untracked debris
                if str(relative) in tracked:
                    continue  # a versioned fixture that merely looks like debris
            verb = "would remove" if args.dry_run else "removed"
            print(f"{verb}: {entry}")
            if not args.dry_run:
                try:
                    os.remove(entry)
                    removed += 1
                except OSError as error:
                    print(f"  failed: {error}", file=sys.stderr)

    if args.dirs:
        for d in args.dirs:
            sweep_directory(Path(d))
    else:
        sweep_directory(repo_root)
        temp_dir = Path(tempfile.gettempdir())
        if temp_dir.resolve() != repo_root.resolve():
            sweep_directory(temp_dir, temp_only=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
