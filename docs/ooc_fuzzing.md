# Out-of-core fuzzing (`sql_ooc_fuzzer`)

The OOC session fuzzer shrinks the process memory knobs until a database of a
few MiB is many times larger than the buffer pool and every operator budget,
then holds the engine to exact results.  It exists to break the code behind
[`ooca-v2.md`](../ooca-v2.md): spill operators, page eviction and fault-in,
private spill of uncommitted pages, recovery under a tiny pool.

| Piece | Where |
|---|---|
| Session interpreter, mirror, oracles, generator | `query/sql_ooc_fuzzer.{hpp,cpp}` |
| Random query generator for the budget-invariance oracle | `query/sql_ooc_query_gen.{hpp,cpp}` |
| One-session-per-process driver | `query/sql_ooc_fuzz_main.cpp` (target `sql_ooc_fuzz`, `EXCLUDE_FROM_ALL`) |
| Campaigns, buckets, minimization | `scripts/ooc_fuzz.py` (`scripts/ooc_fuzz_test.py`) |
| Continuous campaign on `ssh oci` | `scripts/oci_fuzz.sh`, worker 5 of `scripts/remote_fuzz_supervisor.sh`, `scripts/fuzz_collector.py` |
| ctest units (60 s each) | `query/sql_ooc_fuzzer_test.cpp` |
| libFuzzer entry | `query/sql_ooc_fuzzer_libfuzzer.cpp` |
| Findings not fixed yet | `query/testdata/ooc_open_findings/` |
| Fixed findings, replayed by ctest | `query/testdata/ooc_regressions/` |

## What a session checks

A session is a list of textual ops run by one interpreter; generation and
replay share it, so a `.test` file reproduces without the generator.

1. **Mirror.** A C++ copy of every table.  `check ...` ops compare fixed
   queries against it (full dumps, ranges, group-by, joins, windows, set
   operations, top-N), after commits, aborts, crashes and reopens too.
2. **Budget invariance (ooca-v2 G2).** `q ... :: <sql>` runs a random query
   with every memory knob relaxed, then again under the session knobs and up
   to two random pressure settings (operator budget 4 KiB..1 MiB, always-spill,
   forced relational route).  All executions must return the same rows.
   `take=N` closes the pressure cursor after N rows (early-close path);
   `ex=1` also runs `EXPLAIN ANALYZE` and compares its row count.
3. **Failure injection (G6).** `inject mem|io N` fails the N-th reservation /
   spill I/O call of the next op.  `sweep mem|io CAP :: <op>` repeats one op
   with the 1st, 2nd, 3rd ... call failing until the op makes fewer calls than
   the countdown.  Every attempt must fail cleanly (roll back to the committed
   image) or succeed exactly.
4. **Leaks.** After every op the query pool must be back to zero and
   descriptors of unlinked temp files must not pile up; after the session
   every pool must be back where it started.
5. **Skew and shape (G4).** Table flavours with one or two distinct group
   keys, a constant `v`, identical wide pads, rows wider than a page, and tiny
   rows by the thousand.

Session flavours (`sql_ooc_fuzz flavors`) weight the same op vocabulary
differently: `mixed`, `query`, `bigtxn` (pool of one or two pages, statements
touching every page, few commits), `skew`, `inject`, `wide`.

A failed statement is only tolerated when the active pressure explains it:
`kMemoryLimitExceeded` always, `kNoSpace` / `kIOError` under an injection or a
temp-disk cap, `kTooBigData` when one value really is large against the
budget.  Anything else is a finding.

## Running it

```bash
cmake --build build --target sql_ooc_fuzz

# 30 minutes, half the cores, shrink one trace per new bucket
scripts/ooc_fuzz.py run --bin build/sql_ooc_fuzz --out /tmp/ooc \
    --minutes 30 --known query/testdata/ooc_open_findings --minimize

scripts/ooc_fuzz.py report /tmp/ooc            # buckets + pressure evidence
```

Useful switches: `--flavor query,skew` (cycle through flavours), `--scale 4`
(more rows per table), `--seconds 20` (generation time per session),
`--op-timeout 120` (one op running longer is a `hang /` finding),
`--work /dev/shm/ooc` (session databases on tmpfs), `--resume`.

For sanitizer coverage point `--bin` at a `sql_ooc_fuzz` built in an
ASan/UBSan tree (`-DTINYLAMB_ENABLE_SANITIZERS=ON`); a sanitizer abort is
bucketed by its `SUMMARY:` line.

`<out>/findings/<bucket>/` holds `SIGNATURE`, `seed-N.test` (as generated),
`seed-N.report.txt`, and after `--minimize` / `triage` a `minimized.test`.

`<out>/summary.json` is rewritten after every session and at most every
2 seconds during a campaign, so a campaign stopped by SIGINT/SIGTERM can lose
up to the last 2 seconds of sessions; `--resume` adds to it and the buckets.
`--max-traces N` caps the `seed-N.test` files kept per bucket (the sessions
are still counted).  When the trace `--minimize` picked does not fail again
on replay, the bucket gets an `UNREPRODUCED` marker (`replay holds` in
`report`) instead of a `minimized.test`: the finding depends on timing, so
work from `seed-N.report.txt`.

`run` exits 1 only when a bucket that is not `known` appeared (0 otherwise,
130 when interrupted); `replay` exits 1 when any trace fails.

### Reading the summary

`report` ends with the evidence that the campaign actually applied pressure.
If these stay at zero the fuzzer is not testing what it claims to:

- `spill_bytes`, `spilled_queries`, `max_partition_depth`, `skew_fallbacks`
- `evicted_pages`, `uncommitted_spill_evictions`
- `sweep_fired` (injected failures that really fired)
- `executor routes: pipeline=N legacy=M` — only `pipeline` statements ran on
  the OOC sinks (ADR 0006: `legacy_source_ms = 0`); `legacy` ones went through
  `LegacySource` and their memory is still proportional to the input.
- `pipeline operators seen` — which sinks and sources were reached.

## Signatures

A signature is the bucket key: the diagnostic with digits, table names and
SQL text abstracted away, so different seeds hitting one bug collide.

| Form | Meaning |
|---|---|
| `check full(t#) mismatch` | mirror check returned wrong rows |
| `budget-invariance mismatch [join+groupby]` | same query, different rows under pressure; the tag lists the query features |
| `query fails under memory pressure but runs without it [...] \| stream: X` | unexpected status under pressure |
| `UPDATE failed unexpectedly \| stream: Conflicts` | DML failed with a status the pressure does not explain |
| `post-crash count(t#) mismatch`, `post-reopen ...`, `post-post-failure abort ...` | committed image wrong after that boundary |
| `crash recovery failed to reopen the database \| status: X` | reopen failed |
| `sweep mem / ...`, `sweep io / ...` | the inner finding, reached by an injected failure |
| `query memory pool still holds # bytes after op`, `memory pool accounting drifted across the session`, `unlinked temp files pile up` | leaks |
| `crash / SIGSEGV / in Foo::Bar` | the process died; named after the faulting engine frame when gdb is installed, after the op kind otherwise |
| `hang / commit`, `timeout / q` | one op exceeded `--op-timeout` / the session exceeded `--session-timeout` |

## From finding to regression test

```bash
scripts/ooc_fuzz.py minimize --bin build/sql_ooc_fuzz bad.test --out min.test
build/sql_ooc_fuzz replay min.test --verbose
```

`minimize` is delta debugging over out-of-process replays, so crashes and
hangs shrink like wrong results.  It replays leniently (ops whose table or
key disappeared with a removed op are skipped), then relaxes every profile
knob the failure does not need.  Crashes or hangs with a different signature
met on the way are written next to the output as `*.side-<bucket>.test`.

1. A new finding goes to `query/testdata/ooc_open_findings/` (minimized, with
   the `-- failure: <signature> :: <report>` header the script writes).  From
   then on `--known` campaigns step over it where they can and label its
   bucket `known`, so only new buckets make a run exit 1.
2. `scripts/ooc_fuzz.py replay --bin build/sql_ooc_fuzz
   query/testdata/ooc_open_findings` prints one verdict per trace:
   `STILL-FAILS`, `CHANGED` (fails differently now) or `FIXED`.
3. Once `FIXED`, move the trace to `query/testdata/ooc_regressions/`;
   `OocCommittedRegressionTest` replays that directory in
   ctest.  Never put a still-failing trace there.

### Stepping over known findings

One shallow bug can end every session before it reaches anything deeper.

| Knob | Effect |
|---|---|
| `--known DIR` / `--ignore 'sig a;sig b'` (`TINYLAMB_OOC_FUZZ_IGNORE`) | findings whose signature contains an entry are counted (`ignored_findings`) and stepped over: read-only checks continue, failed DML is rolled back, accounting drift is not reported |
| `--skip-ops compact,checkpoint` (`TINYLAMB_OOC_FUZZ_SKIP_OPS`) | the generator never emits those op kinds; the only way around a crash or hang, which ends its process whatever the ignore list says |
| `--env TINYLAMB_OOC_FUZZ_ABORT_AFTER_FAILED_READ=1` | roll the transaction back after a tolerated read failure, stepping over the whole "a failed query leaves the transaction unable to write or commit" family |

## Continuous campaign on `ssh oci`

`scripts/oci_fuzz.sh deploy` builds `sql_ooc_fuzz` in `build-fuzz/` next to
the libFuzzer targets and restarts the supervisor
(`scripts/remote_fuzz_supervisor.sh`).  Its worker 5 runs
`scripts/ooc_fuzz.py run --resume --minimize --known
query/testdata/ooc_open_findings` in slices, forever, into
`~/tinylamb/.fuzz_campaign/findings/ooc/` (session databases stay on tmpfs
under `/tmp/tinylamb-fuzz/work/worker-5/`).  Seeds start at epoch
milliseconds, so slices never repeat one.

```bash
scripts/oci_fuzz.sh status                   # worker 5 = sql_ooc; bucket table
scripts/oci_fuzz.sh logs sql_ooc             # the campaign's own output
scripts/oci_fuzz.sh collect                  # mirror findings, keep the remote copy
scripts/oci_fuzz.sh collect --drain          # ... and clear the remote copy

scripts/ooc_fuzz.py report .fuzz_findings/ooc
scripts/ooc_fuzz.py replay --bin build/sql_ooc_fuzz .fuzz_findings/ooc/findings
```

`collect` leaves the campaign directory as `.fuzz_findings/ooc/`
(`summary.json`, `findings/<bucket>/{SIGNATURE,minimized.test,seed-N.test,*.report.txt}`)
and lists the buckets, new ones first, at the end of
`.fuzz_findings/SUMMARY.md`.  The traces are self-contained, so `replay`
needs only a local `sql_ooc_fuzz`; a finding that does not fail locally is
usually architecture- or timing-dependent (the host is aarch64) — keep its
`*.report.txt`.

`deploy` clears the remote findings before it restarts: `collect` first.
Filing a bucket under `query/testdata/ooc_open_findings/` and deploying again
is what turns it from `NEW` to `known` on the host.

Supervisor knobs (environment of `remote_fuzz_supervisor.sh`; the defaults
are at the top of the script):

| Variable | Default | Meaning |
|---|---|---|
| `TINYLAMB_FUZZ_OOC_JOBS` | 2 | parallel sessions; 0 disables the worker |
| `TINYLAMB_FUZZ_OOC_SLICE_SECONDS` | `TINYLAMB_FUZZ_SLICE_SECONDS` | length of one `run` slice |
| `TINYLAMB_FUZZ_OOC_SESSION_SECONDS` | 20 | generation time per session (`--seconds`) |
| `TINYLAMB_FUZZ_OOC_OP_TIMEOUT` | 180 | `--op-timeout`; the host is slow, a lower value reports slow queries as `hang /` |
| `TINYLAMB_FUZZ_OOC_MAX_TRACES` | 5 | `--max-traces` |
| `TINYLAMB_FUZZ_OOC_MINIMIZE_SECONDS` | 300 | minimization budget per new bucket |
| `TINYLAMB_FUZZ_OOC_KNOWN_DIR` | `query/testdata/ooc_open_findings` | `--known` |

## Trace format

```
-- sql_ooc_fuzzer regression
-- seed: 1001
-- profile: pool=65536 columnar=0 opmem=16384 spill=1 temp=0 route=0
-- failure: <signature> :: <report on one line>
create t0 7 50 900              # name groups pad_min pad_span [v_mod vnull_mod pad_mode]
index t0 ix0 uk                 # uk | g | tk | pad | gv
ins_bulk t0 1 400 11            # INSERT ... SELECT over keys lo..hi, salt
upd t0 range 10 200 1 0 pad 1500 3 vadd 5 0
check full t0
q ord=1 heavy=0 take=-1 nv=3 vs=7 ex=1 snap=0 :: SELECT ...
sweep mem 40 :: check joinrows t0 t1
inject io 3
commit | abort | crash | reopen | checkpoint | evict N | compact t0
knob pool|opmem|spill|columnar|temp|route N
snap open ro|rw | snap check t0 | snap close
```

`profile` is applied before the database opens, so it also governs recovery.
`SqlOocFuzzer.HandWrittenTraceHolds` exercises every op kind.

## ctest units

`sql_ooc_fuzzer_test` keeps each case under the 60 s cap: eight seeded shards
of three 7-second sessions, one case per flavour asserting that the flavour
reaches what it is for (spilled pipeline queries, fired sweeps, uncommitted
page spill), and unit tests of the trace format, the signatures and the
in-process minimizer.  A failing seed writes `sql_ooc_fuzz-seed-N.test` into
the working directory; an op running past `TINYLAMB_OOC_FUZZ_OP_TIMEOUT`
(default 45 s) does the same and fails the case instead of ending as a bare
ctest timeout.  `TINYLAMB_OOC_FUZZ_ONLY_SEED=N` reruns one seed.

Sessions are budgeted by wall-clock time, so a seed is not bit-for-bit
repeatable across machines; the trace it writes is.
