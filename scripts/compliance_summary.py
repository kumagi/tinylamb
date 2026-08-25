#!/usr/bin/env python3
"""Run GoogleSQL compliance tests and produce a granular summary."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="GoogleSQL compliance test runner & summary")
    parser.add_argument("--filter", default="GoogleSqlCompliance/GoogleSqlComplianceFileTest.*", help="gtest filter")
    parser.add_argument("--binary", default="build/googlesql_compliance_test", help="Path to test binary")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    binary = root / args.binary
    if not binary.exists():
        print(f"Error: binary {binary} not found. Please build target googlesql_compliance_test first.", file=sys.stderr)
        sys.exit(1)

    cmd = [str(binary), f"--gtest_filter={args.filter}"]
    print(f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    stdout, _ = proc.communicate()

    passed = []
    failed = []
    current_test = None
    failures_by_test = {}

    for line in stdout.splitlines():
        m_run = re.search(r'\[ RUN      \] GoogleSqlCompliance/GoogleSqlComplianceFileTest\.RunsFile/(.*)', line)
        if m_run:
            current_test = m_run.group(1)
            failures_by_test[current_test] = []
        m_ok = re.search(r'\[       OK \] GoogleSqlCompliance/GoogleSqlComplianceFileTest\.RunsFile/(.*)', line)
        if m_ok:
            passed.append(m_ok.group(1))
        m_fail = re.search(r'\[  FAILED  \] GoogleSqlCompliance/GoogleSqlComplianceFileTest\.RunsFile/(.*)', line)
        if m_fail:
            failed.append(m_fail.group(1))
        if current_test and ("Failure" in line or "failed:" in line or "threw:" in line):
            failures_by_test[current_test].append(line.strip())

    total = len(passed) + len(failed)
    print("\n" + "=" * 60)
    print("GOOGLESQL COMPLIANCE CONFORMANCE SUMMARY")
    print("=" * 60)
    print(f"Total Test Files Ran: {total}")
    print(f"Passed:               {len(passed)} ({len(passed) / total * 100:.2f}%)" if total else "No tests ran")
    print(f"Failed:               {len(failed)} ({len(failed) / total * 100:.2f}%)" if total else "")
    print("=" * 60)

    if passed:
        print("\nPASSED FILES:")
        for p in sorted(passed):
            print(f"  [OK] {p}")

    if failed:
        print(f"\nFAILED FILES ({len(failed)}):")
        for f in sorted(failed):
            print(f"  [FAIL] {f}")


if __name__ == "__main__":
    main()
