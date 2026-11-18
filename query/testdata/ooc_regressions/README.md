# `sql_ooc_fuzzer` regressions

Self-contained traces written by the OOC session fuzzer as
`sql_ooc_fuzz-*.test` (by `sql_ooc_fuzzer_libfuzzer`, by a failing
`SqlOocFuzzer.SeededSessionsShard*` case, or by `scripts/ooc_fuzz.py`).  The
header carries the memory profile (`-- profile: pool=... opmem=...`), every
following line is one interpreter op (`create`, `ins_bulk`, `upd`, `check ...`,
`q ... :: <sql>` for the budget-invariance oracle, `sweep mem|io N :: <op>`
for an exhaustive failure-injection sweep, `crash`, `reopen`, ...).
`OocCommittedRegressionTest.ReplayCommittedRegressionFiles` replays each file
in 16 independent CTest shards (each retains the 60-second timeout) (CMake
wires `TINYLAMB_OOC_REGRESSION_DIR` to this directory), so only put a trace
here once the bug it found is fixed, and keep it afterwards.

Working with a trace:

```bash
ninja -C build sql_ooc_fuzz
build/sql_ooc_fuzz replay bad.test --verbose          # exit 1 + FINDING: on failure
scripts/ooc_fuzz.py minimize --bin build/sql_ooc_fuzz bad.test --out min.test
```
