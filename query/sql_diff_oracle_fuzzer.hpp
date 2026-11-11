/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_DIFF_ORACLE_FUZZER_HPP
#define TINYLAMB_SQL_DIFF_ORACLE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Differential SQL oracles. Each kind is generated and checked on a fresh
// database. `kind` selects which relation is checked:
//
// - tqs: join bitmap. The harness knows which source rows were inserted into
//   each side; INNER / LEFT / IN / NOT IN on a unique id must match the
//   bitmap AND, the preserved side, and its complement.
// - subset: predicate shape. A conjunct's rows are a subset of either
//   factor, a disjunction is a superset, a predicate is disjoint from its
//   negation, and COUNT(DISTINCT) does not exceed COUNT(*). Expression
//   rewrites (`=` vs wildcard-free LIKE, `!=` vs NOT =) must match exactly.
// - dqp: plan differential. One query runs on the default plan, the
//   relational fallback, and the Cascades grouped route. Results must agree
//   whenever at least two paths execute.
// - radar: instance differential. Two databases hold the same rows; indexes
//   and ANALYZE exist only on the second. The same SELECT must match.

struct DiffOracleTrace {
  std::string kind;  // tqs | subset | dqp | radar
  std::vector<std::string> setup;
  std::vector<std::string> indexes;
  std::vector<std::string> queries;
  // TQS: one multiset per query, rows separated by '\n' in Row::ToString form.
  std::vector<std::string> expect;
  // Subset pairs, encoded "left || right".
  std::vector<std::string> subset_and;   // right ⊆ left
  std::vector<std::string> subset_or;    // left ⊆ right
  std::vector<std::string> subset_dis;   // intersection empty
  std::vector<std::string> subset_eq;    // equal sets (expression rewrite)
  std::vector<std::string> subset_card;  // left count <= right count

  bool operator==(const DiffOracleTrace&) const = default;
};

struct DiffOracleStats {
  int tqs_queries{0};
  int subset_checks{0};
  int dqp_queries{0};
  int radar_queries{0};
};

std::string RunTqsIteration(std::mt19937& rng, bool verbose,
                            DiffOracleStats* stats = nullptr,
                            DiffOracleTrace* trace = nullptr);
std::string RunSubsetIteration(std::mt19937& rng, bool verbose,
                               DiffOracleStats* stats = nullptr,
                               DiffOracleTrace* trace = nullptr);
std::string RunDqpIteration(std::mt19937& rng, bool verbose,
                            DiffOracleStats* stats = nullptr,
                            DiffOracleTrace* trace = nullptr);
std::string RunRadarIteration(std::mt19937& rng, bool verbose,
                              DiffOracleStats* stats = nullptr,
                              DiffOracleTrace* trace = nullptr);

std::string ReplayDiffOracleTrace(const DiffOracleTrace& trace,
                                  bool verbose = false);

std::string SerializeDiffOracleTest(uint64_t seed, const DiffOracleTrace& trace,
                                    const std::string& failure_summary);
bool ParseDiffOracleTest(std::string_view text, uint64_t* seed,
                         DiffOracleTrace* trace, std::string* failure_summary);

// libFuzzer: the first byte selects the oracle. A mismatch writes
// `sql_diff_oracle_fuzz-repro-<seed>.test` and aborts.
void DiffOracleFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_DIFF_ORACLE_FUZZER_HPP
