# GoogleSQL compliance testdata

Vendored from [google/googlesql](https://github.com/google/googlesql)
`googlesql/compliance/testdata/*.test` (Apache-2.0).

Pin: `TINYLAMB_GOOGLESQL_VERSION` in CMakeLists.txt (currently `2026.7.2`).
Currently **274** `*.test` files from that tag.

Re-fetch with:

```
python3 scripts/fetch_googlesql_compliance.py
```

The script pulls `googlesql/compliance/testdata/*.test` from that git tag.

Differential privacy / anonymization files are **not** copied. Every other
upstream `*.test` file is included so tinylamb's SQL engine can grow against
the real golden corpus. Cases tinylamb cannot execute yet are expected to fail
the parameterized `googlesql_compliance_test` binary; they are not rewritten
to match local bugs.

The harness skips a case only when:

- GoogleSQL is not built (`TINYLAMB_ENABLE_GOOGLESQL=OFF`), or
- the SQL/name/features mention differential privacy or anonymization.
