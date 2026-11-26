#!/usr/bin/env python3
# Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
"""Render a CTest JUnit report as GitHub Actions job-summary Markdown.

Usage: ctest_summary.py <junit.xml> [--title TITLE] [--slowest N]

Writes to $GITHUB_STEP_SUMMARY when set (stdout otherwise) and emits an
::error annotation per failed test so failures show up on the PR page.
Never fails the job by itself: the ctest step's exit code is the gate.
"""

import argparse
import os
import sys
import xml.etree.ElementTree as ET


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("junit")
    parser.add_argument("--title", default="Test results")
    parser.add_argument("--slowest", type=int, default=10)
    args = parser.parse_args()

    if not os.path.exists(args.junit):
        print(f"{args.junit} not found; nothing to summarize", file=sys.stderr)
        return 0
    try:
        root = ET.parse(args.junit).getroot()
    except ET.ParseError as err:
        print(f"{args.junit}: unreadable JUnit report ({err})", file=sys.stderr)
        return 0
    cases = []
    for case in root.iter("testcase"):
        status = case.get("status", "run")
        if case.find("failure") is not None or case.find("error") is not None:
            status = "failed"
        elif case.find("skipped") is not None or status in ("notrun",
                                                             "disabled"):
            status = "skipped"
        else:
            status = "passed"
        cases.append((case.get("name", "?"), float(case.get("time", 0) or 0),
                      status))

    passed = sum(1 for c in cases if c[2] == "passed")
    failed = [c for c in cases if c[2] == "failed"]
    skipped = sum(1 for c in cases if c[2] == "skipped")
    total_time = sum(c[1] for c in cases)

    icon = "✅" if not failed else "❌"
    out = [f"## {icon} {args.title}", "",
           "| total | passed | failed | skipped | Σ time |",
           "|---:|---:|---:|---:|---:|",
           f"| {len(cases)} | {passed} | {len(failed)} | {skipped} "
           f"| {total_time:.1f}s |", ""]
    if failed:
        out += ["### Failed tests", ""]
        out += [f"- `{name}` ({t:.1f}s)" for name, t, _ in failed]
        out.append("")
        for name, _, _ in failed:
            print(f"::error title=ctest failure::{name}")
    slow = sorted(cases, key=lambda c: -c[1])[: args.slowest]
    if slow:
        out += ["<details><summary>Slowest tests</summary>", "",
                "| test | time |", "|---|---:|"]
        out += [f"| `{name}` | {t:.1f}s |" for name, t, _ in slow]
        out += ["", "</details>", ""]

    text = "\n".join(out) + "\n"
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
