#!/usr/bin/env python3
"""Benchmark regression gate (P3-1).

Compares a new measurement file (JSON or TSV) against the newest baseline
section of BenchmarkHistory.md and exits 1 when any shared metric regresses
beyond --threshold-pct percent.

Baseline extraction (BenchmarkHistory.md):
  - Sections start with "## "; the FIRST section is treated as the newest
    (the file is maintained newest-first).
  - Metrics are harvested from two shapes:
      table rows:   | `tps` | 821.1 |
      bold bullets: - **Query sum (Q1-Q22):** **81.30 s**
                    - **Load:** 126.39 s total
  - Keys are normalized: lowercased, every non-alphanumeric run -> "_",
    trimmed. e.g. "Query sum (Q1\u2013Q22)" -> "query_sum_q1_q22".

New results file:
  - *.json: flat object {"query_sum_q1_q22": 85.2, ...}
  - anything else: TSV, one "key<TAB>value" pair per line ("#" comments OK).

Direction: keys containing tps/qps/tpm/rps/ops are higher-is-better;
everything else is treated as time (lower-is-better).

Exit codes: 0 pass, 1 regression / no comparable metric (unless
--allow-empty), 2 usage error.
"""

import argparse
import json
import re
import sys
from pathlib import Path

HIGHER_IS_BETTER_TOKENS = ("tps", "qps", "tpm", "rps", "ops")


def normalize_key(name: str) -> str:
    key = re.sub(r"[^0-9a-z]+", "_", name.lower())
    return key.strip("_")


def parse_number(text: str):
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def extract_baseline(history_path: Path) -> dict:
    sections = []
    current = None
    for line in history_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            current = [line]
            sections.append(current)
        elif current is not None:
            current.append(line)
    if not sections:
        return {}
    body = "\n".join(sections[0])  # newest-first document

    metrics = {}
    # "| `tps` | 821.1 |" style rows (first cell label, second cell number)
    for match in re.finditer(r"^\s*\|\s*`?([^|`]+)`?\s*\|\s*([0-9][0-9,.]*)\s*(?:\||$)",
                             body, re.MULTILINE):
        value = parse_number(match.group(2))
        if value is not None:
            metrics.setdefault(normalize_key(match.group(1)), value)

    # "- **Label:** **81.30 s**" / "- **Label:** 126.39 s total" bullets
    for match in re.finditer(r"\*\*([^*:]+):\*\*\s*(?:\*\*)?~?([0-9][0-9,.]*)\s*(ms|s)\b",
                             body):
        value = parse_number(match.group(2))
        if value is None:
            continue
        if match.group(3) == "ms":
            value /= 1000.0
        metrics.setdefault(normalize_key(match.group(1)), value)
    return metrics


def parse_results(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        data = json.loads(text)
        if not isinstance(data, dict):
            raise SystemExit("error: JSON results must be a flat object")
        parsed = {}
        for key, value in data.items():
            num = value if isinstance(value, (int, float)) else parse_number(str(value))
            if isinstance(num, (int, float)):
                parsed[normalize_key(key)] = float(num)
        return parsed
    parsed = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t") if "\t" in line else line.split()
        if len(fields) < 2:
            continue
        value = parse_number(fields[1])
        if value is not None:
            parsed.setdefault(normalize_key(fields[0]), value)
    return parsed


def is_higher_better(key: str) -> bool:
    return any(token in key for token in HIGHER_IS_BETTER_TOKENS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument("--history", type=Path, default=repo_root / "BenchmarkHistory.md",
                        help="baseline markdown (newest section first)")
    parser.add_argument("--results", type=Path, required=True,
                        help="new measurement (.json or TSV)")
    parser.add_argument("--threshold-pct", type=float, default=10.0,
                        help="allowed regression in percent (default: 10)")
    parser.add_argument("--allow-empty", action="store_true",
                        help="exit 0 when no metric is comparable")
    args = parser.parse_args()
    if args.threshold_pct < 0:
        parser.error("--threshold-pct must be >= 0")

    if not args.history.is_file():
        print(f"error: history not found: {args.history}", file=sys.stderr)
        return 2
    if not args.results.is_file():
        print(f"error: results not found: {args.results}", file=sys.stderr)
        return 2

    baseline = extract_baseline(args.history)
    if not baseline:
        print(f"error: no baseline metrics parsed from {args.history}", file=sys.stderr)
        return 2
    results = parse_results(args.results)

    regressions = []
    comparable = 0
    print(f"gate threshold: {args.threshold_pct:g}% "
          f"(history: {args.history}, results: {args.results})")
    for key in sorted(set(baseline) & set(results)):
        comparable += 1
        base_value = baseline[key]
        new_value = results[key]
        higher_better = is_higher_better(key)
        limit = (base_value * (1 + args.threshold_pct / 100.0)
                 if not higher_better
                 else base_value * (1 - args.threshold_pct / 100.0))
        failed = new_value > limit if not higher_better else new_value < limit
        direction = "higher-is-better" if higher_better else "lower-is-better"
        status = "FAIL" if failed else "ok"
        print(f"  [{status}] {key}: baseline={base_value:.4f} new={new_value:.4f} "
              f"({direction}, limit={limit:.4f})")
        if failed:
            regressions.append(key)

    if comparable == 0 and not args.allow_empty:
        print("error: no comparable metrics between baseline and results",
              file=sys.stderr)
        return 1
    if regressions:
        print(f"REGRESSION: {', '.join(regressions)}", file=sys.stderr)
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
