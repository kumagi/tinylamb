/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_AGGREGATE_FUZZER_HPP
#define TINYLAMB_SQL_AGGREGATE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Aggregation / GROUP BY / window-function oracle.  Mirrors a single table
// in C++, then runs random GROUP BY queries (COUNT/COUNT(x)/SUM/MIN/MAX over
// nullable columns, optional WHERE) and window queries (ROW_NUMBER / RANK /
// COUNT(*) / running SUM over PARTITION BY with a unique ORDER BY) and
// compares whole result multisets against the mirror's SQL-semantics
// computation (NULL groups, NULL-skipping aggregates, NULLS-FIRST ranking).

struct AggQueryExpect {
  std::string sql;
  std::vector<std::string> expected;

  bool operator==(const AggQueryExpect&) const = default;
};

struct AggTrace {
  std::vector<std::string> setup;
  std::vector<AggQueryExpect> queries;

  bool operator==(const AggTrace&) const = default;
};

struct AggStats {
  int group_queries{0};
  int window_queries{0};
};

// Runs one seeded iteration; "" = all results matched the mirror.
std::string RunAggregateIteration(std::mt19937& rng, bool verbose,
                                  AggStats* stats = nullptr,
                                  AggTrace* trace = nullptr);

// Replays a trace on a fresh throwaway database; "" = all checks hold.
std::string ReplayAggregateTrace(const AggTrace& trace, bool verbose = false);

std::string SerializeAggregateTest(uint64_t seed, const AggTrace& trace,
                                   const std::string& failure_summary);
bool ParseAggregateTest(std::string_view text, uint64_t* seed, AggTrace* trace,
                        std::string* failure_summary);

// libFuzzer harness (data = seed); writes a repro .test and aborts on
// mismatch.
void AggregateFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_AGGREGATE_FUZZER_HPP
