/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_JOIN_FUZZER_HPP
#define TINYLAMB_SQL_JOIN_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Multi-table join oracle.  Builds a fact table (u, k, a) and a dimension
// table (k, tag) whose exact contents the harness mirrors in C++, then runs
// random equi-joins (INNER/LEFT, either direction, optional post-join WHERE,
// random projections) and compares the full result multiset against the
// mirror's nested-loop join.  Exercises hash join, nested loop join, merge
// join plan choices and NULL join-key semantics.

struct QueryExpect {
  std::string sql;
  std::vector<std::string> expected;  // sorted, harness-formatted rows

  bool operator==(const QueryExpect&) const = default;
};

struct JoinTrace {
  std::vector<std::string> setup;  // CREATE TABLE + INSERTs
  std::vector<QueryExpect> queries;

  bool operator==(const JoinTrace&) const = default;
};

struct JoinStats {
  int queries{0};
  int left_joins{0};
  int null_key_rows{0};
};

// Runs one seeded iteration; "" = all join results matched the mirror.
std::string RunJoinIteration(std::mt19937& rng, bool verbose,
                             JoinStats* stats = nullptr,
                             JoinTrace* trace = nullptr);

// Replays a trace on a fresh throwaway database; "" = all checks hold.
std::string ReplayJoinTrace(const JoinTrace& trace, bool verbose = false);

// Self-contained text format.
std::string SerializeJoinTest(uint64_t seed, const JoinTrace& trace,
                              const std::string& failure_summary);
bool ParseJoinTest(std::string_view text, uint64_t* seed, JoinTrace* trace,
                   std::string* failure_summary);

// libFuzzer harness (data = seed); writes a repro .test and aborts on
// mismatch.
void JoinFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_JOIN_FUZZER_HPP
