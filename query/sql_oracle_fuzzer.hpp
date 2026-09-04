/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_ORACLE_FUZZER_HPP
#define TINYLAMB_SQL_ORACLE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Metamorphic-oracle fuzzing of the SQL engine (Cascades optimizer and
// executor logic bugs), following arXiv:2311.06728 taxonomy:
//
// - TLP (Query Partitioning, Rigger & Su 2020): for a query
//   `SELECT proj FROM t WHERE p` and an auxiliary predicate c,
//     Q == Q[p AND c] UNION Q[p AND NOT c] UNION Q[p AND c IS NULL]
//   holds because c is exactly TRUE, FALSE or NULL for every row.
//
// - NoREC (Non-Optimizing Reference Engine, Rigger & Su 2020): moving the
//   predicate from WHERE into a CASE projection bypasses filter pushdown and
//   index selection, so
//     COUNT(*) WHERE p == SUM(CASE WHEN p THEN 1 ELSE 0 END)  (NULL SUM = 0)
//
// A violation of either identity on a healthy engine is a logic bug, and so
// are crashes, sanitizer reports and hangs.
//
// Each iteration builds a throwaway single-table database (tiny value domain
// to force duplicates and NULLs), generates a random SELECT, and checks both
// oracles.  Queries the engine rejects (unsupported syntax) are skipped.
//
// On a failure the fuzzer emits a self-contained `.test` file (see
// SerializeOracleTest) that reproduces the mismatch; committed `.test` files
// can be replayed as permanent regression guards via ParseOracleTest +
// ReplayOracleTrace.

// The reproduction recipe for one iteration: setup statements (CREATE +
// INSERTs) and the oracle queries that were actually executed.  Both oracle
// lists are empty when the iteration skipped them.
struct OracleTrace {
  std::vector<std::string> setup;    // CREATE TABLE + INSERTs, in order
  std::string predicate;             // human-readable root predicate
  std::vector<std::string> tlp;      // original, part1, part2, part3
  std::vector<std::string> norec;    // optimized COUNT, reference SUM

  bool operator==(const OracleTrace&) const = default;
};

struct OracleIterationStats {
  bool tlp_ran{false};
  bool norec_ran{false};
};

// Runs one seeded iteration.  Returns "" when every oracle held (or the
// iteration was skipped), otherwise a diagnostic containing the reproduced
// SQL statements and both sides of the mismatch.  If `trace` is non-null it
// always receives the executed statements, failing or not.
std::string RunOracleIteration(std::mt19937& rng, bool verbose,
                               OracleIterationStats* stats = nullptr,
                               OracleTrace* trace = nullptr);

// Replays a trace on a fresh throwaway database and returns "" when all
// recorded oracles hold, or the mismatch diagnostic.
std::string ReplayOracleTrace(const OracleTrace& trace, bool verbose = false);

// Self-contained text format ("-- key: value" lines; one SQL per line).
std::string SerializeOracleTest(uint64_t seed, const OracleTrace& trace,
                                const std::string& failure_summary);
// Returns false on malformed input.
bool ParseOracleTest(std::string_view text, uint64_t* seed,
                     OracleTrace* trace, std::string* failure_summary);

// libFuzzer harness: interprets `data` as a seed for the generator.  On a
// mismatch, writes `sql_oracle_fuzz-repro-<seed>.test` into the working
// directory (after verifying the serialized form replays the mismatch) and
// aborts.
void OracleFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_ORACLE_FUZZER_HPP
