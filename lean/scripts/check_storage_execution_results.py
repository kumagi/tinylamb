#!/usr/bin/env python3
"""Cross-check the 1,000 StorageExecution Goals against parallel proof reports."""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GOAL_DIR = ROOT / "lean/TinylambOpt/ResearchGoals/StorageExecution"
REPORT_DIR = ROOT / "lean/issues/research/storage-execution-results"
PREFIX = "TinylambOpt.ResearchGoals.StorageExecution."
SHARDS = [
    "BagKernelsA", "BagKernelsB",
    "BitLayoutsA", "BitLayoutsB",
    "ClusterOrderA", "ClusterOrderB",
    "DerivedZonesA", "DerivedZonesB",
    "FrameSelectionA", "FrameSelectionB",
    "GatherPlansA", "GatherPlansB",
    "GrammarScansA", "GrammarScansB",
    "OrderedFootersA", "OrderedFootersB",
    "PostingJoinsA", "PostingJoinsB",
    "RankedMasksA", "RankedMasksB",
]
GOAL_RE = re.compile(r"^\s*def\s+([A-Za-z0-9_'./]+\.Goal)\s*:\s*Prop\b")
FULL_GOAL_RE = re.compile(rf"{re.escape(PREFIX)}[A-Za-z0-9_'./]+\.Goal")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def goal_inventory() -> set[str]:
    goals: set[str] = set()
    for path in sorted(GOAL_DIR.glob("*.lean")):
        if path.name == "Boundaries.lean":
            continue
        namespaces: list[str] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line.startswith("namespace "):
                namespaces.append(line.removeprefix("namespace ").strip())
            elif line.startswith("end "):
                ended = line.removeprefix("end ").strip()
                if not namespaces or namespaces[-1] != ended:
                    raise ValueError(f"{path}: unbalanced namespace end {ended!r}")
                namespaces.pop()
            else:
                match = GOAL_RE.match(line)
                if match:
                    suffix = match.group(1)
                    full = ".".join([*namespaces, suffix])
                    if not full.startswith(PREFIX):
                        raise ValueError(f"{path}: Goal outside prefix: {full}")
                    goals.add(full)
    return goals


def read_reports() -> tuple[dict[str, str], list[str]]:
    statuses: dict[str, str] = {}
    errors: list[str] = []
    for shard in SHARDS:
        path = REPORT_DIR / f"{shard}.md"
        if not path.exists():
            errors.append(f"missing report: {display_path(path)}")
            continue
        shard_seen: set[str] = set()
        for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            names = FULL_GOAL_RE.findall(raw)
            if not names:
                continue
            upper = raw.upper()
            if "REFUTED" in upper:
                status = "REFUTED"
            elif "PROVEN" in upper:
                status = "PROVEN"
            else:
                errors.append(f"{display_path(path)}:{lineno}: no PROVEN/REFUTED status")
                continue
            for name in names:
                if name in shard_seen:
                    errors.append(f"{display_path(path)}:{lineno}: duplicate {name}")
                shard_seen.add(name)
                previous = statuses.get(name)
                if previous is not None and previous != status:
                    errors.append(f"conflicting status for {name}: {previous} vs {status}")
                statuses[name] = status
        if len(shard_seen) != 50:
            errors.append(
                f"{display_path(path)}: expected 50 Goals, found {len(shard_seen)}"
            )
    return statuses, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    goals = goal_inventory()
    statuses, errors = read_reports()
    if len(goals) != 1000:
        errors.append(f"expected 1000 Goals, found {len(goals)}")
    missing = sorted(goals - statuses.keys())
    extra = sorted(statuses.keys() - goals)
    errors.extend(f"missing result: {name}" for name in missing)
    errors.extend(f"unknown Goal in report: {name}" for name in extra)

    by_domain: dict[str, Counter[str]] = defaultdict(Counter)
    for name, status in statuses.items():
        if name in goals:
            domain = name.removeprefix(PREFIX).split(".", 1)[0]
            by_domain[domain][status] += 1

    if errors:
        print(f"FAIL: {len(errors)} error(s)", file=sys.stderr)
        for error in errors[:100]:
            print(f"  {error}", file=sys.stderr)
        if len(errors) > 100:
            print(f"  ... and {len(errors) - 100} more", file=sys.stderr)
        return 1

    print(f"PASS: {len(goals)} Goals covered ({len(statuses)} reports)")
    for domain in sorted(by_domain):
        counts = by_domain[domain]
        print(f"  {domain}: {counts['PROVEN']} proven, {counts['REFUTED']} refuted")
    if args.verbose:
        for name in sorted(statuses):
            print(f"{statuses[name]} {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
