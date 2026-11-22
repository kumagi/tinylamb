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

// Aggregation / GROUP BY / window-function oracle. Mirrors the generated
// input in C++ and checks a separate primary-key fixture. It runs aggregate
// queries — GROUP BY over 1-2 key columns, optional WHERE and HAVING (3VL on
// the aggregate result), global aggregates over possibly-empty input
// (COUNT(*)=0 / SUM=NULL contract), COUNTIF and DISTINCT aggregates
// (COUNT(DISTINCT)/SUM(DISTINCT)) — and window queries (ROW_NUMBER / RANK /
// COUNT(*) / running SUM plus ROWS BETWEEN k PRECEDING AND CURRENT ROW frames
// for SUM/COUNT(b)). Window cases cover both unique ordering and peer groups.
// About 12% of iterations use large inputs loaded by a single-statement INSERT
// ... SELECT ... FROM UNNEST(GENERATE_SERIES(...)), with about 8,300–8,409 or
// 33,000–33,040 rows to cross parallel execution thresholds. The C++ mirror
// checks whole result multisets (NULL groups, NULL-skipping aggregates,
// NULLS-FIRST ranking).

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
  int primary_key_group_queries{0};
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
