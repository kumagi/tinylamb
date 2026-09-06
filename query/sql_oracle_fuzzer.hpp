/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_ORACLE_FUZZER_HPP
#define TINYLAMB_SQL_ORACLE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace tinylamb {

// Metamorphic- and constraint-oracle fuzzing of the SQL engine (Cascades
// optimizer and executor logic bugs), following arXiv:2311.06728 taxonomy:
//
// - TLP (Query Partitioning): Q == Q[p AND c] U Q[p AND NOT c] U Q[p AND
//   c IS NULL].
// - NoREC (Non-Optimizing Reference Engine): COUNT(*) WHERE p ==
//   SUM(CASE WHEN p THEN 1 ELSE 0 END).
// - Constraint-solving / PQS-flavoured oracle: the harness mirrors every
//   generated row in C++ and evaluates predicates with 3-valued logic, so it
//   knows the ground truth; a pivot row is forced into a query via predicate
//   adjustment (p / NOT p / p IS NULL) and must appear in the result.
// - Constraint-rewriting oracle (index independence): results must not
//   change across CREATE INDEX.
// - Statement-type transformation (DQE): COUNT(*) WHERE p must equal the
//   rows affected by DELETE WHERE p, and the remainder must add up.
// - Transaction-splitting oracle: a transaction executed atomically must
//   leave the same state as its statements executed autocommit.
// - AMOEBA-flavoured performance oracle: equivalent predicates must not
//   differ wildly in execution time (RunAmoebaIteration, opt-in).
// - QPG-flavoured plan feedback: OracleSession rewards generator flavours
//   that surface unseen EXPLAIN plan shapes.
//
// On a failure the fuzzer emits a self-contained `.test` file (see
// SerializeOracleTest); committed `.test` files replay as permanent
// regression guards via ParseOracleTest + ReplayOracleTrace.

// The reproduction recipe for one iteration.  Fields stay empty when the
// corresponding oracle was skipped (e.g. the engine rejected the syntax).
struct OracleTrace {
  std::string table;               // generated table name (unique per run)
  std::vector<std::string> setup;  // CREATE TABLE + INSERTs, in order
  std::string predicate;
  std::vector<std::string> tlp;    // original, part1, part2, part3
  std::vector<std::string> norec;  // optimized COUNT, reference SUM
  // PQS: ground-truth row count, pivot id, and the queries that verify them.
  std::string pqs_count;  // SELECT COUNT(*) WHERE p'
  std::string pqs_u;      // SELECT u WHERE p'
  int64_t pqs_expected{0};
  int64_t pqs_pivot_u{0};
  // Constraint rewriting: probe before/after the DDL must agree.
  std::vector<std::string> index_ddl;
  std::string index_probe;
  // Statement-type transformation: COUNT WHERE p, COUNT all, DELETE WHERE p.
  std::vector<std::string> dqe;  // exactly 3 when active
  // Transaction splitting: statements run inside one txn vs autocommit.
  std::vector<std::string> troc;  // mutating statements
  std::string troc_probe;         // state probe, run in both branches

  bool operator==(const OracleTrace&) const = default;
};

struct OracleIterationStats {
  bool tlp_ran{false};
  bool norec_ran{false};
  bool pqs_ran{false};
  bool idx_ran{false};
  bool dqe_ran{false};
  bool troc_ran{false};
};

// QPG-flavoured plan feedback: remembers EXPLAIN fingerprints and rewards
// generator flavours (search-space biases) that surface unseen plans.
class OracleSession {
 public:
  // Records the fingerprint of one executed query plan; returns true when it
  // was new.  Empty fingerprints (EXPLAIN unavailable) are ignored.
  bool ObservePlan(const std::string& fingerprint);
  // Epsilon-greedy flavour choice in [0, flavour_count); `reward` feeds the
  // discovery signal of the flavour last chosen via SelectFlavour.
  int SelectFlavour(int flavour_count);
  void RewardFlavour(int flavour, bool discovered_new_plan);
  uint64_t PlansSeen() const { return plans_.size(); }

 private:
  std::unordered_set<std::string> plans_;
  std::vector<uint64_t> rewards_;
  std::mt19937 rng_{42};
};

// Runs one seeded iteration.  Returns "" when every oracle held (or the
// iteration was skipped), otherwise a diagnostic with the reproduced SQL and
// both sides of the mismatch.  `trace` always receives the executed
// statements; `session` (optional) gets plan feedback.
std::string RunOracleIteration(std::mt19937& rng, bool verbose,
                               OracleIterationStats* stats = nullptr,
                               OracleTrace* trace = nullptr,
                               OracleSession* session = nullptr);

// Replays a trace on a fresh throwaway database and returns "" when all
// recorded oracles hold, or the mismatch diagnostic.
std::string ReplayOracleTrace(const OracleTrace& trace, bool verbose = false);

// Self-contained text format ("-- key: value" lines; one SQL per line).
std::string SerializeOracleTest(uint64_t seed, const OracleTrace& trace,
                                const std::string& failure_summary);
// Returns false on malformed input.
bool ParseOracleTest(std::string_view text, uint64_t* seed, OracleTrace* trace,
                     std::string* failure_summary);

// AMOEBA-flavoured performance oracle (opt-in; timing is environment
// sensitive).  Builds equivalent predicate pairs, measures both sides, and
// returns a diagnostic when the ratio exceeds conservative thresholds.
std::string RunAmoebaIteration(std::mt19937& rng, bool verbose);

// libFuzzer harness: interprets `data` as a seed for the generator.  On a
// mismatch, writes `sql_oracle_fuzz-repro-<seed>.test` into the working
// directory (after verifying the serialized form replays the mismatch) and
// aborts.
void OracleFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_ORACLE_FUZZER_HPP
