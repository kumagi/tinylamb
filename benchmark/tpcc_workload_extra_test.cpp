/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra TPC-C workload coverage beyond tpcc_workload_test.cpp: the
// spec-defined last-name syllable table (including out-of-range clamps),
// NURand table determinism from a fixed seed, and the weighted transaction
// mix replaying deterministically per terminal seed.
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "benchmark/tpcc_workload.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"

namespace tinylamb {

TEST(TpccLastNameTest, FollowsTheSpecSyllableTable) {
  // Clause 4.3.2.2 syllables: BAR OUGHT ABLE PRI PRES ESE ANTI CALLY ATION
  // EING; n = 0 -> BARBARBAR, n = 123 -> ABLEOUGHTABLE.
  EXPECT_EQ(TpccLastName(0), "BARBARBAR");
  EXPECT_EQ(TpccLastName(123), "OUGHTABLEPRI");
  EXPECT_EQ(TpccLastName(999), "EINGEINGEING");
  // Out-of-range inputs clamp into [0, 999] instead of invoking UB.
  EXPECT_EQ(TpccLastName(-7), TpccLastName(0));
  EXPECT_EQ(TpccLastName(1500), TpccLastName(999));
  // Every in-range name is exactly three syllables.
  for (int n = 0; n <= 999; n += 37) {
    const std::string name = TpccLastName(n);
    EXPECT_GE(name.size(), 9U) << "n=" << n;
    EXPECT_LE(name.size(), 15U) << "n=" << n;
  }
}

TEST(TpccNurandSeedTest, TableIsDeterministicPerSeed) {
  const TpccNurand first = TpccNurand::FromSeed(1234);
  const TpccNurand same = TpccNurand::FromSeed(1234);
  const TpccNurand other = TpccNurand::FromSeed(1235);
  ASSERT_TRUE(first.valid);
  ASSERT_TRUE(same.valid);
  ASSERT_TRUE(other.valid);
  EXPECT_EQ(first.c_last_load, same.c_last_load);
  EXPECT_EQ(first.c_last_run, same.c_last_run);
  EXPECT_EQ(first.c_id, same.c_id);
  EXPECT_EQ(first.c_ol_i_id, same.c_ol_i_id);
  EXPECT_NE(first.c_last_run, other.c_last_run);
}

namespace {

class TpccWorkloadExtraTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "tpcc_workload_extra-" + RandomString();
    database_ = Database::Create(path_).MoveValue();
  }
  void TearDown() override {
    database_->DeleteAll();
    database_.reset();
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

}  // namespace

TEST_F(TpccWorkloadExtraTest, TransactionMixReplaysPerSeedAndCoversAllTypes) {
  TpccScale scale = TpccScale::Official(1);
  TpccWorkload first(*database_, scale, /*seed=*/99);
  TpccWorkload second(*database_, scale, /*seed=*/99);
  std::vector<TpccTransactionType> first_mix;
  std::vector<TpccTransactionType> second_mix;
  std::set<TpccTransactionType> seen;
  for (int i = 0; i < 500; ++i) {
    const TpccTransactionType type = first.NextTransactionType();
    first_mix.push_back(type);
    seen.insert(type);
    second_mix.push_back(second.NextTransactionType());
  }
  EXPECT_EQ(first_mix, second_mix)
      << "same terminal seed must replay the same mix";
  // Every transaction type must be reachable from the weighted mix.
  EXPECT_EQ(seen.size(), 5U) << "weighted mix must cover all five types";
}

}  // namespace tinylamb
