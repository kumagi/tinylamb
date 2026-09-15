/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_PLANCACHE_FUZZER_HPP
#define TINYLAMB_SQL_PLANCACHE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Plan-cache hit/miss differential.  The compiled-plan cache replays
// specialized SELECT plans when the SQL fingerprint, database and schema
// epoch match and the literal parameters are identical; everything else
// recompiles.  This harness runs the same query set four ways and demands
// identical, mirror-correct results:
//
//   1. fresh database, first run        (miss -> fill)
//   2. same database, same text         (hit -> replay)
//   3. after DML drift, same database   (hit with drifted data)
//   4. fresh database, replayed script  (miss, same drifted data)
//
// A divergence means the cached plan bakes stale state: stale constants,
// index ranges, or statistics-dependent results.

struct PlanCacheTrace {
  std::vector<std::string> setup;      // CREATE + initial INSERTs
  std::vector<std::string> queries;    // the differential query set
  std::vector<std::string> mutations;  // DML applied between phase 2 and 3
  std::vector<std::string> expected;   // mirror dump per query after drift

  bool operator==(const PlanCacheTrace&) const = default;
};

struct PlanCacheStats {
  int queries_compared{0};
};

// Runs one seeded iteration; "" = every phase agreed with the mirror.
std::string RunPlanCacheIteration(std::mt19937& rng, bool verbose,
                                  PlanCacheStats* stats = nullptr,
                                  PlanCacheTrace* trace = nullptr);

// Replays the trace (same phase structure) on a fresh database.
std::string ReplayPlanCacheTrace(const PlanCacheTrace& trace,
                                 bool verbose = false);

std::string SerializePlanCacheTest(uint64_t seed, const PlanCacheTrace& trace,
                                   const std::string& failure_summary);
bool ParsePlanCacheTest(std::string_view text, uint64_t* seed,
                        PlanCacheTrace* trace, std::string* failure_summary);

// libFuzzer harness (data = seed); writes a repro .test and aborts.
void PlanCacheFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_PLANCACHE_FUZZER_HPP
