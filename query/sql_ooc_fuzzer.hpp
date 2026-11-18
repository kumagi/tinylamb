/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_OOC_FUZZER_HPP
#define TINYLAMB_SQL_OOC_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Larger-than-memory session fuzzing (ooca-v2.md G2/G3/G4/G6 as a fuzzer).
//
// Each session shrinks the process memory knobs (storage pool, columnar pool,
// operator budget, always-spill, temp-disk cap) so a database of a few MiB is
// many times larger than the buffer pool, then drives wide-row tables through
// bulk DML, index builds, TLCS compaction, forced eviction, checkpoints,
// crashes, clean reopens, snapshot readers and injected memory / spill-I/O
// failures.  Three oracles hold the engine to exact results:
//
//  * Mirror: a C++ copy of every table; fixed check queries (dumps, ranges,
//    group-by, many-to-many joins, windows, set operations, wide sorts, index
//    probes) are compared against it.
//  * Budget invariance (G2): random queries from sql_ooc_query_gen (every
//    join type, non-composable aggregates, window frames, set operations,
//    recursive CTEs, correlated subqueries, band / cross joins, index paths)
//    run once with every memory knob relaxed and again under the session's
//    knobs plus random tiny budgets; all executions must agree.  A variant
//    may stop reading its cursor early, and EXPLAIN ANALYZE must report the
//    same row count.
//  * Resource accounting (G6): after every statement the query pool must be
//    empty and no spill file descriptor may stay behind; injected failures
//    are swept over every reservation / spill-I/O call of a statement, and
//    each failed attempt must roll back to the committed image.
//
// The trace is a list of textual ops; generation and replay run the very same
// interpreter, so a `.test` file reproduces without the generator.

// Session-level memory profile (applied before the database opens so it also
// covers recovery; `knob` ops change it mid-session).
struct OocProfile {
  size_t pool_bytes{0};      // storage pool cap (0 = default)
  size_t columnar_bytes{0};  // columnar pool cap (0 = default)
  size_t op_memory_bytes{0};
  bool always_spill{false};
  size_t temp_limit_bytes{0};
  bool force_relational{false};

  bool operator==(const OocProfile&) const = default;
};

struct OocTrace {
  OocProfile profile;
  std::vector<std::string> ops;

  bool operator==(const OocTrace&) const = default;
};

struct OocStats {
  int statements{0};
  int checks{0};
  int rows_inserted{0};
  int commits{0};
  int aborts{0};
  int crashes{0};
  int reopens{0};
  int checkpoints{0};
  int evictions{0};
  int compactions{0};
  int injections{0};
  int tolerated_failures{0};
  int snapshot_checks{0};
  int indexes{0};
  // Budget-invariance oracle.
  int diff_queries{0};
  int diff_variants{0};
  int diff_tolerated{0};
  int diff_skipped{0};
  int partial_cursors{0};
  int explain_checks{0};
  int pure_pipeline_queries{0};
  int spilled_queries{0};
  // Failure-injection sweeps.
  int sweeps{0};
  int sweep_points{0};
  int sweep_fired{0};
  // Pressure evidence observed during the session.
  uint64_t evicted_pages{0};
  uint64_t uncommitted_spill_evictions{0};
  uint64_t spill_bytes{0};
  uint64_t spill_ops{0};
  uint32_t max_partition_depth{0};
  uint64_t skew_fallbacks{0};
  size_t max_db_bytes{0};
  int max_deleted_fds{0};
  // Query-template and pipeline-operator coverage.
  std::map<std::string, int> shapes;
  std::map<std::string, int> operators;
  // "<query features> => pipeline|legacy|other": which executor EXPLAIN
  // ANALYZE says served the statement under the session's knobs
  // (ooca-v2.md D4: only legacy_source_ms = 0 queries are out-of-core).
  std::map<std::string, int> routes;
  // Findings matched by TINYLAMB_OOC_FUZZ_IGNORE on read-only statements.
  int ignored_findings{0};

  void Merge(const OocStats& o);
  // One line of key=value pairs (coverage maps excluded).
  [[nodiscard]] std::string Summary() const;
};

struct OocRunOptions {
  bool verbose{false};
  // Generation time cap (0 = TINYLAMB_OOC_FUZZ_SECONDS or 20 s); the epilogue
  // (commit, crash, reopen, full dumps) always runs.
  double budget_seconds{0};
  // Data-volume multiplier (0 = TINYLAMB_OOC_FUZZ_SCALE or 1).
  int scale{0};
  // Session flavour: -1 draws one from the seed; see OocFlavorNames().
  int flavor{-1};
  // Replay only: ops that no longer apply (their table, key or snapshot is
  // gone because a trace minimizer removed an earlier op) are skipped instead
  // of reported as malformed.
  bool lenient{false};
  // Called once with the profile before the database opens, and with every
  // op right before it executes.  A driver that appends them to a file keeps
  // a reproducible trace even when the process crashes or hangs.
  std::function<void(const OocProfile&)> on_profile;
  std::function<void(const std::string&)> on_op;
};

// Runs one session drawn from `data` (bytes steer the decisions; once they
// run out a PRNG seeded by `seed` takes over).  Returns "" when every check
// held, otherwise a diagnostic.  `trace` always receives the recipe.
std::string RunOocSession(const uint8_t* data, size_t size, uint64_t seed,
                          const OocRunOptions& options,
                          OocStats* stats = nullptr, OocTrace* trace = nullptr);

// Convenience wrapper kept for the gtest / libFuzzer callers.
std::string RunOocIteration(const uint8_t* data, size_t size, uint64_t seed,
                            bool verbose, OocStats* stats = nullptr,
                            OocTrace* trace = nullptr,
                            double budget_seconds = 0);

// Replays a trace on a fresh throwaway database; "" = all checks hold.
std::string ReplayOocTrace(const OocTrace& trace, const OocRunOptions& options,
                           OocStats* stats = nullptr);
std::string ReplayOocTrace(const OocTrace& trace, bool verbose = false,
                           OocStats* stats = nullptr);

// Stable bucket key for a diagnostic: digits, table names and SQL text are
// abstracted away so the same bug found by different seeds collides.
std::string OocFailureSignature(std::string_view report);

// Shrinks a failing trace in-process (delta debugging over the op list with
// lenient replays), keeping candidates whose replay fails with the same
// signature as `report`.  Crashes and hangs need the out-of-process
// minimizer (scripts/ooc_fuzz.py minimize).
OocTrace MinimizeOocTrace(const OocTrace& trace, std::string_view report,
                          double budget_seconds, std::string* final_report);

std::vector<std::string> OocFlavorNames();

// Self-contained text format ("-- key: value" header + one op per line).
std::string SerializeOocTest(uint64_t seed, const OocTrace& trace,
                             const std::string& failure_summary);
bool ParseOocTest(std::string_view text, uint64_t* seed, OocTrace* trace,
                  std::string* failure_summary);
// Header-only prefix of the text format (for incremental trace files).
std::string SerializeOocHeader(uint64_t seed, const OocProfile& profile);

// libFuzzer harness.  On failure writes sql_ooc_fuzz-repro-<seed>.test
// (verified by a replay first) and aborts unless TINYLAMB_FUZZ_CONTINUE=1.
void OocFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_OOC_FUZZER_HPP
