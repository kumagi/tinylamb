# `scripts/` — tooling (outside the layer DAG)

Not C++ layers; ignored by `check_layering.py` (`IGNORED_PREFIXES`).

## Key files

- `check_layering.py` — include-lint. `python3 scripts/check_layering.py`
  must exit 0. Reads quoted `#include`s, ranks `common → … → server`
  (`executor/detail/` before `executor/`) and rejects the unconditional
  `FORBIDDEN_PATTERNS` edges (currently `expression/* -> database/*`)
  regardless of rank or allowlist. Exempts `build*`, `benchmark/`,
  `main.cpp`, `*_test*`/`*_fuzzer*`/`*_benchmark.*` — except the nine fuzzer
  harnesses in `PRODUCTION_FUZZERS`, which CMake builds into `tinylamb_sql`.
  Accepted debt lives in
  `DEFAULT_ALLOWLIST` (V1/V3'/V4 markers + structural edges) — **pay it down
  by deleting lines, never extend it without rethinking placement**.
  Options: `--root DIR`, `--allowlist FILE`.
- `check_ooc_invariants.py` — OOC static invariant lint. Enforces allowlists on
  forced memory reservations (`ReserveForced`/`AddForced`), full result
  materialization (`Collect`), and eager row reads (`ReadAllRows` /
  `ReadAllPositioned`); `python3 scripts/check_ooc_invariants.py` must exit 0.
- `run_clang_tidy.py` — runs `clang-tidy` across translation units in
  parallel locally (`--jobs`; no remote/distributed mode).
- `verify_all.sh` — runs `ctest`, `check_layering.py`, and
  `check_ooc_invariants.py`.
- `clean_root_artifacts.py` — sweeps leftover `.db`/`.log`/benchmark
  artifacts (`--dry-run` supported); fuzzer crash/timeout inputs and
  `*-repro-*.test` files are removed only with `--fuzz-artifacts`.
- `bench_gate.py` — compares benchmark JSON/TSV against the
  `BenchmarkHistory.md` baseline section; exits 1 past threshold.
- `compliance_summary.py` — runs the `googlesql_compliance_test` binary and
  aggregates results. Default is `build/googlesql_compliance_test`; pass
  `--binary PATH` to use a custom build directory (e.g. `build-fuzz/`).
- `expr_oracle.py` — Python oracle for Lisp S-expressions with GoogleSQL
  semantics (`[PY-DIFF]` marks intentional divergences).
- `fetch_googlesql_compliance.py` — fetches
  `google/googlesql@2026.7.2` testdata (keep in sync with the
  `TINYLAMB_GOOGLESQL_VERSION` CMake variable).
- `oci_fuzz.sh` + `remote_fuzz_supervisor.sh` + `fuzz_collector.py` —
  continuous remote SQL fuzzing & failure collection pipeline on `ssh oci`
  (`deploy`, `status`, `collect`, `logs`, `start`, `stop`, `restart`); keeps
  active DB/WAL/corpus/logs on `tmpfs` (`/tmp/tinylamb-fuzz`); each fuzz
  worker parses through its own zygote socket
  (`TINYLAMB_GOOGLESQL_ZYGOTE_SOCK=/tmp/googlesql_zygote_<worker>.sock`; the
  watchdog maintains `/tmp/googlesql_zygote{,_1.._5}.sock`, and the base
  `/tmp/googlesql_zygote.sock` serves replay/test paths).
  Supervisor worker 5 runs the out-of-core campaign (`ooc_fuzz.py run` over
  `build-fuzz/sql_ooc_fuzz`, `TINYLAMB_FUZZ_OOC_*` knobs); `collect` mirrors
  its signature buckets and replayable traces to `.fuzz_findings/ooc/` —
  see `docs/ooc_fuzzing.md`. `deploy` clears remote findings: `collect` first.
- `build_googlesql_parse.sh` — builds a native `aarch64` / `x86_64`
  parser-only `execute_query` binary from `google/googlesql` (`2026.7.2`) with
  a built-in zero-fork Unix-domain socket daemon (`--zygote_sock` /
  `GOOGLESQL_ZYGOTE_SOCK`), replacing QEMU x86_64 emulation on ARM64.
- `oci_small_fuzz.sh` + `remote_small_fuzz_supervisor.sh` — continuous
  low-memory (`nice -n 19`, `-rss_limit_mb=192`) in-process C++ fuzzing
  controller and supervisor for `ssh oci-small1` and `ssh oci-small2`.
- `build_gantt.py` — distributed clean-build timeline viewer. Runs a clean
  build through `tools/distcomp` (starts `distcomp_coord --trace`), then
  renders ONE self-contained HTML Gantt chart: X = elapsed time, Y = bands for
  CPU core / distcomp slots on both machines plus link, archive, and other
  tasks (hover detail, zoom; idle compile slots are visible).
  Data joined from `<build>/.ninja_log` + the coordinator's JSONL trace
  (`<build>/distcomp_trace.jsonl`). Output: `<build>/gantt.html`.
  `--no-build` re-renders from the last run; `--sync-src` rsyncs the source
  tree to the remote first (the remote compiles files from ITS own disk).
- `ctest_gantt.py` — distributed **ctest** timeline viewer sharing the same
  HTML template (imports `build_gantt`). Schedules every test itself from a
  global queue (`ctest -N` list) executed one-at-a-time via `ctest -I n,n` by
  L local + R remote worker threads (ssh ControlMaster-multiplexed), so
  per-test start/end/host/worker are exact; rsync reuses
  `distributed_ctest.run_rsync`. Output: `<build>/ctest_gantt.html` (bars =
  tests, pass/fail/skip colors, failure tails, workers-per-row, in-flight
  strip). `-R/-E`, `--no-sync`, `--local-only` supported; exits 1 on failures.
