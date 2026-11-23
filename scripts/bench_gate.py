#!/usr/bin/env python3
"""Benchmark regression gate (P3-1).

Compares a new measurement file (JSON or TSV) against the newest baseline
section of BenchmarkHistory.md and exits 1 when any shared metric regresses
beyond --threshold-pct percent.

Baseline extraction (BenchmarkHistory.md):
  - Sections start with "## "; the file is maintained newest-first.  Each
    metric key is taken from the NEWEST section that contains it, so a
    section documenting a different workload (e.g. TPC-C) does not blind the
    gate to metrics that still have current baselines in older sections.
  - Metrics are harvested from two shapes:
      table rows:   | `tps` | 821.1 |
      bold bullets: - **Query sum (Q1-Q22):** **81.30 s**
                    - **Load:** 126.39 s total
  - A section that explicitly disclaims its numbers (e.g. "2026-10-31
    correction: ... retained as raw observations, not accepted as a correct
    ... score") is untrusted from that annotation onward: those values are
    raw observations, not gate baselines, and are skipped.
  - Keys are normalized: lowercased, every non-alphanumeric run -> "_",
    trimmed. e.g. "Query sum (Q1\u2013Q22)" -> "query_sum_q1_q22".

New results file:
  - *.json: flat object {"query_sum_q1_q22": 85.2, ...}
  - anything else: one "key<TAB>value" pair per line ("#" comments OK);
    whitespace-delimited lines are also accepted, with the value last.

Direction: keys with tps/qps/tpm/rps/ops as a complete token are
higher-is-better, unless the key also contains error/fail/drop; everything
else is treated as time (lower-is-better).

Exit codes: 0 pass, 1 regression / no comparable metric (unless
--allow-empty), 2 usage error or malformed/missing baseline data.
"""

import argparse
import json
import math
import re
import sys
from pathlib import Path

HIGHER_IS_BETTER_TOKENS = ("tps", "qps", "tpm", "rps", "ops")

# A section may retract its own numbers as "raw observations, not accepted as
# a correct ... score" (e.g. the pre-C3 Q16 result).  From such an annotation
# onward the rest of that section must not contribute gate baselines.
SECTION_DISCLAIMER_RE = re.compile(
    r"correction:\*\*|not accepted as a correct|retained as raw observations",
    re.IGNORECASE,
)


def normalize_key(name: str) -> str:
    key = re.sub(r"[^0-9a-z]+", "_", name.lower())
    return key.strip("_")


def parse_number(text: str):
    try:
        value = float(text.replace(",", ""))
    except ValueError:
        return None
    # NaN compares false against everything and Inf silently passes ratio
    # gates; treat both as a missing measurement so the gate fails loudly.
    return value if math.isfinite(value) else None


def extract_baseline(history_path: Path) -> dict:
    sections = []
    current = None
    for line in history_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            current = [line]
            sections.append(current)
        elif current is not None:
            current.append(line)
    metrics = {}
    # newest-first document: iterate oldest -> newest and let newer sections
    # overwrite, so each key resolves to its newest recorded baseline.
    for section in reversed(sections):
        # Drop everything from an explicit retraction onward.  Newest-first
        # documents may keep an old, already-corrected measurement in an
        # earlier section; harvesting it would resurrect a value the history
        # itself declared unfit as a performance baseline (and would also
        # give the gate a misleading ~97% safety margin).
        trusted_lines = []
        for line in section:
            if SECTION_DISCLAIMER_RE.search(line):
                break
            trusted_lines.append(line)
        body = "\n".join(trusted_lines)
        # "| `tps` | 821.1 |" style rows (first cell label, second cell number)
        for match in re.finditer(
                r"^\s*\|\s*`?([^|`]+)`?\s*\|\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*[.,;:]?\s*(?:\||$)",
                body, re.MULTILINE):
            value = parse_number(match.group(2))
            if value is not None:
                metrics[normalize_key(match.group(1))] = value

        # "- **Label:** **81.30 s**" / "- **Label:** 126.39 s total" bullets
        for match in re.finditer(
                r"\*\*([^*:]+):\*\*\s*(?:\*\*)?~?([0-9][0-9,]*(?:\.[0-9]+)?)\s*(?:\*\*)?\s*(ms|s)\b",
                body):
            value = parse_number(match.group(2))
            if value is None:
                continue
            if match.group(3) == "ms":
                value /= 1000.0
            metrics[normalize_key(match.group(1))] = value
    return metrics


def parse_results(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        data = json.loads(text)
        if not isinstance(data, dict):
            raise SystemExit("error: JSON results must be a flat object")
        parsed = {}
        for key, value in data.items():
            if isinstance(value, bool):
                continue
            num = value if isinstance(value, (int, float)) else parse_number(str(value))
            # json.loads accepts NaN/Infinity literals; they compare false
            # against every gate limit, so reject them just like parse_number
            # does for text measurements.
            if isinstance(num, (int, float)) and math.isfinite(num):
                parsed[normalize_key(key)] = float(num)
        return parsed
    parsed = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "\t" in line:
            fields = line.split("\t")
            if len(fields) != 2:
                # The documented format is exactly key<TAB>value; a wider
                # TSV would silently mis-key the metric under fields[0].
                continue
        else:
            fields = line.rsplit(None, 1)
        if len(fields) < 2:
            continue
        value = parse_number(fields[-1])
        if value is not None:
            parsed[normalize_key(fields[0])] = value
    return parsed


def is_higher_better(key: str) -> bool:
    # A token must be a full component at the start or after a delimiter, but
    # a following "_" is a legitimate compound-metric suffix (tps_p95,
    # qps_avg, tpm_total are all higher-is-better). Only error/failure counts
    # such as "ops_errors" invert the direction despite matching a token.
    lowered = key.lower()
    if any(word in lowered for word in ("error", "fail", "drop")):
        return False
    return any(re.search(rf"(?<![a-z0-9]){token}(?![a-z0-9])", key)
               for token in HIGHER_IS_BETTER_TOKENS)


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
    if args.threshold_pct >= 100:
        parser.error("--threshold-pct must be < 100 (100+ disables the higher-is-better gate)")

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
        if base_value == 0:
            # A zero baseline makes the ratio gate meaningless: limit 0
            # fails every positive lower-is-better sample and passes every
            # higher-is-better one. Almost always a parse artifact.
            print(f"warning: {key}: zero baseline, skipping gate",
                  file=sys.stderr)
            continue
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
