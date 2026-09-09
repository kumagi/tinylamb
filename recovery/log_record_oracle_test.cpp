/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "recovery/log_record_oracle.hpp"

#include <cstdint>
#include <random>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

// Seeded record sweep: every generated record must roundtrip semantically,
// stay byte-stable, agree with Size(), and tolerate every truncation.
// Mirrors the ExprSimplifyOracle seeded-iteration style so a failure
// reproduces from the seed alone.
TEST(LogRecordOracle, SeededIterationsPreserveSerdesEquivalence) {
  constexpr int kIterations = 300;
  int ran = 0;
  int saw_checkpoint = 0;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    const GeneratedLogRecord generated = GenerateLogRecord(rng);
    if (generated.record.type == LogType::kBeginCheckpoint ||
        generated.record.type == LogType::kEndCheckpoint) {
      ++saw_checkpoint;
    }
    EXPECT_TRUE(CheckLogRecordEquivalence(generated).empty())
        << true << (seed != 0u) << true
        << CheckLogRecordEquivalence(generated);
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
  // The generator must reach the checkpoint family, not just row ops.
  EXPECT_GT(saw_checkpoint, 0);
}

// The generator is deterministic: reseeding reproduces the record exactly,
// which is what lets a failure replay from the seed alone.
TEST(LogRecordOracle, SameSeedReproducesSameRecord) {
  for (uint32_t seed : {0U, 1U, 7U, 42U, 12345U}) {
    std::mt19937 first(seed);
    std::mt19937 second(seed);
    const GeneratedLogRecord a = GenerateLogRecord(first);
    const GeneratedLogRecord b = GenerateLogRecord(second);
    EXPECT_TRUE(a.record == b.record) << "seed=" << seed;
    EXPECT_EQ(a.record.Serialize(), b.record.Serialize()) << "seed=" << seed;
  }
}

// The shrinker never hides a mismatch and never grows the record.
TEST(LogRecordOracle, ShrinkNeverHidesMismatchNorGrows) {
  for (uint32_t seed : {0U, 1U, 7U, 42U, 1234U}) {
    std::mt19937 rng(seed);
    const GeneratedLogRecord generated = GenerateLogRecord(rng);
    const GeneratedLogRecord shrunk = ShrinkLogRecord(generated);
    EXPECT_GE(generated.record.Serialize().size(),
              shrunk.record.Serialize().size())
        << "seed=" << seed;
    EXPECT_EQ(CheckLogRecordEquivalence(generated).empty(),
              CheckLogRecordEquivalence(shrunk).empty())
        << "seed=" << seed;
  }
}

}  // namespace
}  // namespace tinylamb
