# `scripts/` — tooling (outside the layer DAG)

Not C++ layers; ignored by `check_layering.py` (`IGNORED_PREFIXES`).

## Key files

- `check_layering.py` — include-lint. `python3 scripts/check_layering.py`
  must exit 0. Reads quoted `#include`s, ranks `common → … → server`
  (`executor/detail/` before `executor/`), exempts `build*`, `benchmark/`,
  `main.cpp`, `*_test*`/`*_fuzzer*`/`*_benchmark.*`. Accepted debt lives in
  `DEFAULT_ALLOWLIST` (V1/V3'/V4 markers + structural edges) — **pay it down
  by deleting lines, never extend it without rethinking placement**.
  Options: `--root DIR`, `--allowlist FILE`.
- `bench_gate.py` — compares benchmark JSON/TSV against the
  `BenchmarkHistory.md` baseline section; exits 1 past threshold.
- `compliance_summary.py` — runs `build/googlesql_compliance_test` and
  aggregates results.
- `expr_oracle.py` — Python oracle for Lisp S-expressions with GoogleSQL
  semantics (`[PY-DIFF]` marks intentional divergences).
- `fetch_googlesql_compliance.py` — fetches
  `google/googlesql@2026.7.2` testdata (keep in sync with the
  `TINYLAMB_GOOGLESQL_VERSION` CMake variable).
