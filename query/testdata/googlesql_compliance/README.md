# GoogleSQL compliance testdata

Vendored from [google/googlesql](https://github.com/google/googlesql)
`googlesql/compliance/testdata/*.test` (Apache-2.0).

Pin: `TINYLAMB_GOOGLESQL_VERSION` in CMakeLists.txt (currently `2026.7.2`).
Currently **274** upstream `*.test` files from that tag, plus locally authored
`optimizer_*.test` expectation suites (currently 12 files / 225 cases).

Re-fetch with:

```
python3 scripts/fetch_googlesql_compliance.py
```

The script pulls `googlesql/compliance/testdata/*.test` from that git tag.
It preserves local `optimizer_*.test` files.

Differential privacy / anonymization files are **not** copied. Every other
upstream `*.test` file is included so tinylamb's SQL engine can grow against
the real golden corpus. Cases tinylamb cannot execute yet are expected to fail
the parameterized `googlesql_compliance_test` binary; they are not rewritten
to match local bugs.

The harness skips a case only when:

- GoogleSQL is not built (`TINYLAMB_ENABLE_GOOGLESQL=OFF`), or
- the SQL/name/features mention differential privacy or anonymization.

## tinylamb optimizer expectations

Local optimizer cases can check result rows and stable plan fragments together
using repeatable `[plan_contains=...]` and `[plan_not_contains=...]` options.
`[mode=explain]` runs `EXPLAIN`; `[mode=explain_analyze]` runs
`EXPLAIN ANALYZE` so runtime/adaptive behavior can also be asserted. Fragment
matching avoids brittle exact goldens for costs and timings.
