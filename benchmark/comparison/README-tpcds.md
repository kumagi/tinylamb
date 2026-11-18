# TPC-DS comparison

## SF 1・クエリ順の2倍上限確認（2026-11-17）

Q1→Q99の順に比較し、結果不一致・実行失敗・中央値がDuckDBの2倍を
超えた時点で次のクエリを中断する `--max-ratio 2` で改善した。
同じ最終バイナリによる2回の順方向実行で、99件すべて結果一致・2倍以下を確認。
8スレッド、CPU 8–15、各クエリwarmup 1回＋測定7回の中央値。
DuckDB 2.0.0、SF 1、DOUBLEを使用する既存の比較条件であり、コールドI/Oの測定ではない。

| 実行 | 一致／上限内 | 最大 tinylamb / DuckDB | tinylamb中央値合計 | DuckDB中央値合計 |
|---|---:|---:|---:|---:|
| gate07 | 99 / 99 | 1.728（Q54） | 1,024.11 ms | 1,099.22 ms |
| gate08 | 99 / 99 | 1.783（Q54） | 1,015.91 ms | 1,094.03 ms |

列キャッシュを最近の利用順で回収し、使用中のキャッシュの重複再構築を抑制した。
単一要素INの比較への変換、整数条件の重複辞書判定除去、NULLを含む密な
ランタイムフィルタと少数キーの先行選択によりスキャンを短縮した。
Q81で見つかった、列形式の結合集約におけるNULL／大きな整数キーの
フォールバックと文字列辞書の参照寿命も修正した。

[最終結果](../../build/benchmarks/tpcds-sf1-sequential-20261117/gate08/summary.json)・
[クエリ別の時間](../../build/benchmarks/tpcds-sf1-sequential-20261117/gate08/latency.png)・
[再測定結果](../../build/benchmarks/tpcds-sf1-sequential-20261117/gate07/summary.json)。
各実行ディレクトリにバイナリ、ソース差分、入力ハッシュ、全サンプルと回答を保存した。
関連CTest 351件、Python 9件、layering / OOC invariantsのチェックは通過。
10分のOOCファズ（1,311セッション、157,604チェック）では21種類の失敗を検出した。
全種類の代表ケースは作業前のバイナリでも失敗し、一部はハング等の別の失敗形態になった。
この21種類は後続のOOC修正で解消し、元の全21トレースの再実行と、
`query/testdata/ooc_regressions/20261117_*.test` の回帰テストで確認した。
回帰テストは16シャードに分け、各CTestの60秒制限を維持している。
保存済み40トレースと関連948テストが通過。追加ファズ760セッション・96,862チェックで
新規失敗0件（無視した失敗0件）。[OOC修正の検証記録](../../build/quality/ooc-fixes-20261117/verification.json)。
OOC修正後の性能再測定でQ28が2倍を超えたため、範囲条件の選択を分岐なしで
圧縮し、OR内のBETWEENの値・NULL読取りを一回にまとめた。再測定で超過したQ82も、
少数のランタイム結合キーをバッチ比較し、一致行だけを後続の条件評価へ渡すようにした。
副キーのランタイムフィルタも選択率に応じて先に適用する。NULL、INT64両端、NaN、
無限大、フィルタのAND条件、単調列・複数ワーカーを回帰テストで確認した。

最終バイナリでQ1→Q99を2回通して再測定し、いずれも全件一致・2倍以下。
条件は上記と同じ（SF 1、8スレッド、CPU 8–15、warmup 1回＋測定7回）。

| 性能修正後 | Q28 tinylamb / DuckDB | Q82 tinylamb / DuckDB | 全クエリ最大比率 |
|---|---:|---:|---:|
| gate03 | 20.00 / 17.05 ms（1.174倍） | 4.66 / 2.96 ms（1.576倍） | 1.894（Q2） |
| gate04 | 20.33 / 19.96 ms（1.018倍） | 4.78 / 3.26 ms（1.465倍） | 1.716（Q76） |

[性能修正の検証記録](../../build/quality/tpcds-performance-20261117/verification.json)・
[gate03](../../build/quality/tpcds-performance-20261117/gate03/summary.json)・
[gate04](../../build/quality/tpcds-performance-20261117/gate04/summary.json)。
関連CTest 305件とlayering / OOC invariantsのチェックも通過。
最終バイナリの10分OOCファズは1,285セッション・158,616チェック、失敗0件
（無視した検出0件）。
[検証記録](../../build/benchmarks/tpcds-sf1-sequential-20261117/validation/verification.json)。

```sh
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/tpcds.py \
  --dataset build/benchmarks/tpcds-sf1-scaling-fixture-20261111/dataset \
  --fixture build/benchmarks/tpcds-sf1-sequential-20261117/native \
  --binary build-tpcds-release/tinylamb_tpcds_compare \
  --output build/benchmarks/tpcds-sf1-next-check \
  --threads 8 --cpus 8,9,10,11,12,13,14,15 \
  --warmup 1 --repeats 7 --max-ratio 2
```

## データ増量・マルチコア比較（2026-11-11）

SF 0.1（約118万行）とSF 1（約1,956万行）を2・8・16物理コアで比較。各クエリwarmup 1回＋7回、実行順を反転した2実行の範囲。値は99件の中央値合計（ms）。全12実行で3製品とも99/99件一致。

| SF | コア／最大スレッド | Tinylamb ms | ClickHouse ms | DuckDB ms |
|---|---:|---:|---:|---:|
| 0.1 | 2 | 460.26–464.92 | 1,270.70–1,289.10 | 437.39–444.95 |
| 0.1 | 8 | 403.12–408.15 | 1,104.82–1,110.83 | 389.70–391.07 |
| 0.1 | 16 | 514.88–521.74 | 1,237.95–1,240.62 | 456.97–463.88 |
| 1 | 2 | 7,715.61–7,796.16 | 6,875.27–6,886.67 | 1,848.39–1,875.19 |
| 1 | 8 | 6,314.02–6,345.79 | 3,570.43–3,595.93 | 964.30–968.52 |
| 1 | 16 | 7,367.75–7,393.54 | 3,962.89–3,974.03 | 1,062.95–1,069.72 |

SF 1・8コアのTinylambはDuckDBの約6.55倍、ClickHouseの約1.77倍の時間。2→8コアの高速化はTinylamb約1.23倍、他2製品約1.9倍。16コアは全製品で8コアより遅かった。
測定中に発見したQ39の統合未対応集約を並列化するバグを修正し、ClickHouse比較SQLのROUNDの同率丸めも補正した。上表は修正後の同一バイナリで全条件を取り直したもの。
[レポートと再現条件](../../build/benchmarks/tpcds-scaling-final-20261111/REPORT.md)、[比較画像](../../build/benchmarks/tpcds-scaling-final-20261111/comparison.png)。SF 10以上は未測定。


2026-11-10: ユーザーの指示により、DuckDB合計時間の20%短縮作業を中断し、
ClickBenchの全43クエリでDuckDBを上回る改善へ切り替えた。
TPC-DSの20%短縮は未達。採用済み改善のopt151測定は全99件一致、
372.327 ms対427.517 ms（約12.9%短縮）で、7回・逆順の最終測定は未実施。
以降の複合キー追加フィルタ・UNION引数移動・複数列遅延読み込みの実験は不採用として取り除いた。


`comparison/tpcds.py` follows the TPC-H comparison workflow: generate one
shared dataset, load both embedded engines, execute every query, verify answers,
and plot individual timings. It uses DuckDB's `tpcds` extension for `dsdgen`
and the 99 SQL queries. This is an engineering comparison, not a certified
TPC-DS result.

```sh
python3 -m venv /var/tmp/tinylamb-tpcds-venv
/var/tmp/tinylamb-tpcds-venv/bin/pip install -r benchmark/comparison/requirements-tpcds.txt
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j8 --target tinylamb_tpcds_compare
/var/tmp/tinylamb-tpcds-venv/bin/python benchmark/comparison/tpcds.py \
  --output build/benchmarks/tpcds-first --scale .1 --threads 2 \
  --warmup 1 --repeats 7
```

The measured optimized build uses Clang, Release, native CPU tuning and ThinLTO:

```sh
cmake -S . -B build-tpcds-release -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld \
  -DTINYLAMB_ARCH=native -DTINYLAMB_ENABLE_LTO=ON \
  -DTINYLAMB_ALLOCATOR=mimalloc -DMI_BUILD_TESTS=OFF
cmake --build build-tpcds-release -j8 --target tinylamb_tpcds_compare
```

Pass `--binary build-tpcds-release/tinylamb_tpcds_compare` to use it.
Native CPU tuning makes this binary machine-specific; the normal build above
is the portable option. Build options are saved with every run.

The output directory must be new. The default scale is 0.1; other scales are
supported. The generated SQL uses the extension's fixed query parameters
(scale 1), independently of the data scale. Both engines use DOUBLE in place
of DECIMAL storage and casts because tinylamb's numeric path is binary64.
INTEGER/BIGINT columns use signed 64-bit storage on both engines.
`sqlglot` ports the SQL to GoogleSQL, preserving explicit window NULL ordering.
Original SQL, both executed dialects, all input tables and their hashes remain
in `dataset/`. Neither engine gets additional indexes or primary keys.

Both processes inherit the same CPU affinity (the first `--threads` available
logical CPUs, or `--cpus 2,3`) and receive the same thread budget. Engines run
sequentially, with their execution order alternating by query number. Timing
covers SQL execution, planning and complete result collection in each embedded
API; loading, process startup, transaction setup and JSON serialization are
excluded. Python's DuckDB fetch conversion is included. These are warm process
measurements, not cold I/O tests or prepared-statement execution-only timings.
Native SQL parser caching is enabled as in normal use. No result cache is
shared between query executions.

Every warmup and measured answer is checked for stability. Cross-engine checks
compare multisets, including duplicate multiplicity and NULLs. Integers compare
exactly; floating-point values use `rel_tol=1e-9, abs_tol=1e-7`. Output ordering
is not checked, but LIMIT's selected rows are. Error, timeout and mismatch
queries remain failures rather than disappearing from the report. A failed
run exits nonzero. `summary.json` also lists empty answers, which are common
at small scale and should be considered when interpreting query win counts.

To reuse data and the loaded native database:

```sh
/var/tmp/tinylamb-tpcds-venv/bin/python benchmark/comparison/tpcds.py \
  --dataset build/benchmarks/tpcds-first/dataset \
  --fixture build/benchmarks/tpcds-first/tinylamb \
  --output build/benchmarks/tpcds-repeat --threads 2 --warmup 1 --repeats 7
```

Do not open the same native fixture from multiple processes. Reuse requires
its `.dataset.json` provenance marker. `--prepare-only` creates a fixture
without measurements. `--queries 1 3 7` runs a diagnostic subset; this must not
be presented as the full 99-query comparison. `--profile` adds native plan
statistics and EXPLAIN output for diagnosis, and should be omitted for final
timings. `--binary` selects a different build.

`--max-ratio 2` runs the requested queries in order and stops before the next
query if an answer fails validation, execution fails, or the median tinylamb
time exceeds twice the DuckDB median. Equality passes. Warmup is excluded from
the ratio, and `--timeout` remains a separate per-response hang guard. A failed
gate exits nonzero; `summary.json` records performance failures and unattempted
queries separately from answer mismatches. The default query order is Q1–Q99;
use `--queries` to remeasure the first failing query during optimization.

Outputs include `results.jsonl` (every sample and final answer), `summary.json`,
`environment.json` (CPU, versions, flags, source/binary identity), a frozen native
binary, CMake cache, source diff, benchmark sources, logs, and `latency` /
`speedup` plots in PNG and SVG. A speedup greater than 1 means tinylamb is faster.
Bars show per-query medians; latency error bars show min/max, not confidence
intervals. The summary separately counts wins with no overlapping samples and
reports the sum of medians, so a majority of query wins is not confused with a
lower total execution time.

Regenerate plots without rerunning:

```sh
/var/tmp/tinylamb-tpcds-venv/bin/python benchmark/comparison/tpcds.py \
  --report build/benchmarks/tpcds-repeat
/var/tmp/tinylamb-tpcds-venv/bin/python -m unittest discover \
  -s benchmark/comparison -p tpcds_test.py
```

## Engine changes exercised by the workload

The implementation extends existing relational planning and execution paths:

- Preserve hidden `GROUPING(...)` sort keys when expanding grouping sets, and
  keep visible `SELECT *` column order when inner joins are reordered.
- Scope dynamic filters to the statement generation, including folded HAVING
  statements whose stack addresses can be reused by sibling UNION branches.
- Honor the query's parallel worker budget throughout scans, joins and groups.
- Load selective dimensions before fact tables and stop an inner join when an
  input becomes empty, retaining its complete output schema.
- Fold constant IN-list arithmetic, retain prefix predicates when crossing OR
  branches, and derive conservative integer enclosures for pre-decode filters.
  The complete predicate still checks holes and NULL semantics.
- Compile inner-join residual filters; evaluate integer CASE range aggregates
  positionally in fused joins; support up to eight raw aggregate accumulators.
  Overflow and unsupported types return to the canonical evaluator.
- Index materialized CTE correlations with binding-identity checks, extract
  equalities shared by OR branches, and restrict supported string-key builds
  to the current outer domain with generation-aware invalidation.
- Precompute supported CTE scalar aggregate comparisons and immutable string
  membership sets, retaining NULL / NOT IN / collation behavior.

Regression tests cover NULLs, duplicates, reversed comparisons, sparse domains,
CTE rebinding, outer-domain changes, hidden sort columns, overflow and empty
inputs. Full-suite and oracle-fuzzer logs accompany the measured artifacts.

## Current total-time optimization checkpoint

This work is still in progress: the target is a sum of all 99 query medians
at most 80% of DuckDB's sum, with all answers matching. The latest measured
checkpoint below has **not** reached that target.

`build/benchmarks/tpcds-total-opt151/` records SF 0.1, CPUs 0/1, two threads
per engine, one warmup and three measured samples per query. The build uses
Clang 21.1.8, Release/native/ThinLTO, mimalloc 2.1.7 and instrumentation-based
PGO trained on all 99 queries. DuckDB is 2.0.0. No allocator runtime environment
overrides were used for this checkpoint.

| Answer matches | Median wins | Nonempty wins | tinylamb total | DuckDB total |
| ---: | ---: | ---: | ---: | ---: |
| 99/99 | 76/99 | 63/85 | 372.33 ms | 427.52 ms |

This is a 12.9% total-time reduction. The 20% target for this run is 342.01 ms.
The three-sample checkpoint is diagnostic; seven-sample forward/reverse
confirmation remains pending. All samples, answers and PNG/SVG plots are in
that result directory. The historical results below predate these changes.

For the measured compiler profile, the run preserves
`build-inputs/profile-0.profdata`; pass its absolute path through both
`-DCMAKE_C_FLAGS=-fprofile-use=...` and
`-DCMAKE_CXX_FLAGS=-fprofile-use=...` when rebuilding the corresponding source
snapshot. Use a fresh profile after changing engine code. The profile was
trained using twelve full-workload passes, alternating query order; training
runs are not benchmark results. The frozen source diff, additional untracked
sources, CMake cache, binary hash and shared-library hashes record the exact
inputs. `build-inputs/libmimalloc.so.2` is retained as well; when replaying the
frozen binary, put that directory on `LD_LIBRARY_PATH` so its original RPATH
does not select a subsequently rebuilt allocator.

Recent changes include deferred columnar join/aggregate execution, narrow
integer and borrowed string join keys, retained row buffers, direct window
keys and projections, selective dimension-chain loading, NULL/date bitmap
filters, compact grouping-set state, bounded exact integer partial merges,
compiled QUALIFY filtering, and partition-owned parallel window prefixes.
Each execution still reads its
transaction snapshot; these changes do not cache query answers.

Quality checkpoints are recorded separately from timing: the clean build at
iteration 133 had no warnings; the subsequent full CTest run at iteration 146
reported 3,700 tests with no failures and three existing skips. Full Clang-Tidy
at iteration 115 checked 389
translation units without diagnostics; subsequent modified translation units
have focused rechecks. Iteration 124's three ASan/UBSan oracle fuzzers each
completed 601 seconds without findings (600 SQL, 51,382 expression and 3,788
Griffin inputs). Iteration 151 passed 145 relational tests in both normal and
ASan/UBSan builds, and 19 related TSan tests without data-race reports.
Later full-build and fuzz verification is ongoing; these
checkpoints do not certify future edits.

## Historical majority-of-queries snapshot (2026-11-10)

AMD Ryzen 9 9950X3D, CPUs 0/1, two threads per engine; Clang 21.1.8
Release/native/ThinLTO; DuckDB 2.0.0. SF 0.1 contains 1,176,170 rows across
24 tables. Each query has one warmup followed by seven samples.

| Run | Matched answers | tinylamb median wins | Nonempty-query wins |
| --- | ---: | ---: | ---: |
| Q1 → Q99 | 99/99 | 52/99 | 43/85 |
| Q99 → Q1, confirmation | 99/99 | 54/99 | 45/85 |

The confirmation has 45 wins whose measured sample ranges do not overlap.
The sum of query medians is 9,153.84 ms for tinylamb and 433.70 ms for DuckDB;
therefore DuckDB remains faster in total. The geometric mean of
`tinylamb / DuckDB` time is 1.676. Fourteen queries return no rows at this scale.
These results establish the majority-of-queries target for this configuration,
not superiority at every scale or a lower total workload time.

Raw measurements and PNG/SVG plots are in
`build/benchmarks/tpcds-final17/` and
`build/benchmarks/tpcds-final-verified/`. An intervening reverse-order run
(`tpcds-confirm17`) was interrupted after external parallel builds/tests
introduced contention; its raw records and exclusion note are retained.
The confirmation used the same binary SHA-256 as the first complete run and
records system load before and after every query.

Validation: ctest 3,582 registered tests, zero failures, three existing skips;
65 relational and 11 scan-filter regression tests; six Python tests; layer
lint passes with 78 existing allowlisted edges. Final-binary oracle fuzzing:
SQL 861 inputs and expression 178,522 inputs, 601 seconds each, zero failures.
See the result directory's `validation/` logs and `verification.json`.

## Historical warning and static-analysis verification

`scripts/run_clang_tidy.py` runs the existing `.clang-tidy` checks against all
project translation units in the compilation database, including tests and
benchmarks. It keeps distinct compilation configurations, omits fetched
third-party/generated sources, records every exit code, and fails on either
diagnostics or an analyzer crash. `--files` permits focused rechecks without
changing the enabled checks or hiding diagnostics from project headers.

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build --clean-first -j8 --target all \
  tinylamb_tpcc_tcp_benchmark tinylamb_tpcc_three_benchmark \
  tinylamb_tpch_benchmark tinylamb_tpcds_benchmark tinylamb_tpcds_compare \
  tinylamb_expression_jit_benchmark
python3 scripts/run_clang_tidy.py --jobs 8 --output-dir build/quality/tidy
ctest --test-dir build --output-on-failure -j16
python3 scripts/check_layering.py
```

The cleanup retains the existing warning flags and Clang-Tidy configuration.
It replaces fixed C arrays with `std::array`, makes ownership and special
members explicit, initializes temporaries, and clarifies arithmetic and control
flow. PAX's checked scaled sum now tests signed 128-bit bounds before performing
arithmetic; this preserves overflow rejection and avoids a Clang 21 analyzer
crash on the corresponding overflow builtins. Boundary cases are covered by
`PaxChunkCodecsTest.ScaledSumChecksProductAndAdditionBounds`, including unchanged
accumulators on failure. JSON control-character escaping and runtime-ID
self-assignment also have regression coverage.

The cross-session view test retries only the server's documented SQLSTATE
55006 (catalog DDL requires quiescent sessions). Its intermittent race was
reproduced before the change; 1,000 repeated runs passed afterward. Other errors
still fail the test, and the server's concurrency contract is unchanged.

Final cleanup verification (Clang / Clang-Tidy 21.1.8): clean RelWithDebInfo
build of all normal and the six additional benchmark targets, 747 steps,
zero warnings; 388 project translation units / 393 distinct compile
configurations, zero Clang-Tidy diagnostics and zero analyzer failures. The
full analysis was followed by rechecks of every translation unit affected by
subsequent edits; per-source logs and the merged manifest are retained in
`build/quality/tidy-final/summary.json`.

CTest: 3,584 registered, 3,581 passed, three existing skips, zero failures.
The six Python tests and layering lint also pass. The eight PAX codec tests,
including scaled-sum bounds, pass with undefined-behavior/bounds sanitizers.
Fuzzing of the final sources completed without findings: SQL 732 inputs, expression
187,326 inputs and compressed-region corruption 39,358 inputs (601 seconds
each), plus PAX mutation 5,771,341 inputs (301 seconds). Seed: 20261112.
Build, test and fuzz logs, source hashes, and the analysis configuration are
under `build/quality/validation/`; `build/quality/verification.json` ties them
to the measured binary.

The final cleaned binary was measured twice on the same SF 0.1 dataset, CPUs
0/1, two threads per engine, one warmup and seven measured samples per query.
DuckDB version: 2.0.0. All 99 answer multisets matched in both runs.

| Query order | tinylamb wins | Nonempty wins | Nonoverlapping wins | Sum of tinylamb medians | Sum of DuckDB medians |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 through 99 | 53/99 | 44/85 | 44/99 | 10,264.55 ms | 431.12 ms |
| 99 through 1 | 53/99 | 44/85 | 47/99 | 10,296.82 ms | 436.14 ms |

Results and PNG/SVG plots: `build/benchmarks/tpcds-quality-final/` and
`build/benchmarks/tpcds-quality-reverse/`. Majority wins remain; total elapsed
query time still favors DuckDB. The cleaned native binary SHA-256 is
`0e3c1ec7cf60847a07c64502b53aa07018147f0fa3c88fc3e0920f9e9fe8e6a9`.

Q16 was slower than the historical run, so the frozen old binary was checked
again under the current conditions (diagnostic subset, not another full-suite
result). Its median was 4,565.23 ms, versus 4,521.87 / 4,535.87 ms for the cleaned
binary. The historical increase therefore also occurs with the unchanged old
binary; this check did not show a cleanup-specific Q16 regression. That
comparison is retained in `build/benchmarks/tpcds-quality-old-q16-check/`, with
explicit historical-binary provenance.
