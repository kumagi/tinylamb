/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Unit tests for the sound runtime ("dynamic") join filters in
// common/runtime_filter.hpp.
//
// The whole feature rests on ONE property: `Reject(k) == true` implies no
// build row carries an equal non-NULL key `k`.  A Bloom false positive is
// allowed (the key then goes through the real join); a false negative is not.
// Every test below therefore checks membership against an independent oracle
// rather than against the filter's own bookkeeping.
#include "common/runtime_filter.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "common/query_parallelism.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr int64_t kMinI64 = std::numeric_limits<int64_t>::min();
constexpr int64_t kMaxI64 = std::numeric_limits<int64_t>::max();

// The build key set a probe key is tested against.
std::set<int64_t> Oracle(const RuntimeFilterGroup& group,
                         const std::vector<int64_t>& probe) {
  std::set<int64_t> out;
  for (const int64_t key : probe) {
    if (!group.Reject(key)) {
      out.insert(key);
    }
  }
  return out;
}

std::shared_ptr<const RuntimeFilterGroup> ExactOf(
    const std::vector<int64_t>& keys) {
  RuntimeFilterBuilder builder;
  for (const int64_t key : keys) {
    builder.Observe(key);
  }
  return builder.Build();
}

TEST(QueryParallelismBudgetTest, AdmissionSharesCpuAndRestoresNestedScopes) {
  EXPECT_EQ(QueryParallelWorkerLimit(8), 8U);
  std::atomic<size_t> active{4};
  {
    ScopedQueryParallelismBudget outer(&active, 8);
    EXPECT_EQ(QueryParallelWorkerLimit(8), 2U);
    active.store(16);
    EXPECT_EQ(QueryParallelWorkerLimit(8), 1U);
    {
      ScopedQueryParallelismBudget nested(nullptr, 1);
      EXPECT_EQ(QueryParallelWorkerLimit(8), 8U);
    }
    EXPECT_EQ(QueryParallelWorkerLimit(8), 1U);
    active.store(1);
    EXPECT_EQ(QueryParallelWorkerLimit(8), 8U);
    std::thread independent([] { EXPECT_EQ(QueryParallelWorkerLimit(8), 8U); });
    independent.join();
  }
  EXPECT_EQ(QueryParallelWorkerLimit(8), 8U);
}

TEST(QueryParallelismBudgetTest,
     MixedWorkUsesRequestParallelismBeforeAnalyticTeams) {
  std::atomic<size_t> pending{4}, reads{1};
  ScopedQueryParallelismBudget budget(&pending, 8, &reads);
  EXPECT_EQ(QueryParallelWorkerLimit(8), 1U);
  pending.store(16);
  EXPECT_EQ(QueryParallelWorkerLimit(8), 1U);
  pending.store(1);
  EXPECT_EQ(QueryParallelWorkerLimit(8), 8U);
  pending.store(2);
  EXPECT_EQ(QueryParallelWorkerLimit(8), 1U);
  reads.store(2);
  EXPECT_EQ(QueryParallelWorkerLimit(8), 4U);
  EXPECT_EQ(QueryParallelWorkerLimit(2), 2U);
}

TEST(RuntimeFilterTest, ExactKeyEnclosureKeepsConjunctiveMembership) {
  std::vector<std::unique_ptr<RuntimeFilter>> parts;
  parts.push_back(std::make_unique<ExactSetRuntimeFilter>(
      std::vector<int64_t>{1, 3, 5, 7}));
  parts.push_back(
      std::make_unique<ExactSetRuntimeFilter>(std::vector<int64_t>{3, 5, 9}));
  parts.push_back(std::make_unique<MinMaxRuntimeFilter>(4, 10, 3));
  const RuntimeFilterGroup group(std::move(parts));
  ASSERT_NE(group.ExactKeys(), nullptr);
  EXPECT_EQ(*group.ExactKeys(), std::vector<int64_t>({3, 5, 9}));
  for (int64_t key = 0; key < 12; ++key) {
    if (!group.Reject(key)) {
      EXPECT_TRUE(std::binary_search(group.ExactKeys()->begin(),
                                     group.ExactKeys()->end(), key));
    }
  }
  EXPECT_EQ(Oracle(group, *group.ExactKeys()), std::set<int64_t>({5}));
  const RuntimeFilterGroup empty;
  EXPECT_EQ(empty.ExactKeys(), nullptr);
}

TEST(RuntimeFilterTest, EmptyBuildPublishesNothingUnlessRejectAllAllowed) {
  RuntimeFilterBuilder builder;
  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(builder.Build(), nullptr);
  const auto reject_all = builder.Build(/*allow_reject_all=*/true);
  ASSERT_NE(reject_all, nullptr);
  EXPECT_TRUE(reject_all->Has(RuntimeFilterKind::kRejectAll));
  EXPECT_TRUE(reject_all->Reject(0));
  EXPECT_TRUE(reject_all->Reject(kMaxI64));
  EXPECT_NE(reject_all->Describe().find("reject_all"), std::string::npos);
}

TEST(RuntimeFilterTest, AllNullBuildIsRejectAllAndNeverMatches) {
  RuntimeFilterBuilder builder;
  builder.ObserveNull();
  builder.ObserveNull();
  EXPECT_EQ(builder.Build(), nullptr);
  const auto reject_all = builder.Build(true);
  ASSERT_NE(reject_all, nullptr);
  EXPECT_TRUE(reject_all->Has(RuntimeFilterKind::kRejectAll));
  EXPECT_TRUE(reject_all->Reject(0));
}

TEST(RuntimeFilterTest, NullObservationsDoNotWeakenTheKeySet) {
  RuntimeFilterBuilder mixed;
  mixed.Observe(7);
  mixed.ObserveNull();
  mixed.Observe(7);  // duplicate
  mixed.Observe(9);
  const auto group = mixed.Build();
  ASSERT_NE(group, nullptr);
  EXPECT_TRUE(group->Has(RuntimeFilterKind::kExactSet));
  EXPECT_FALSE(group->Reject(7));
  EXPECT_FALSE(group->Reject(9));
  EXPECT_TRUE(group->Reject(8));
  EXPECT_EQ(group->DistinctKeys(), 2U);
  EXPECT_EQ(mixed.Observations(), 4U);
  EXPECT_TRUE(mixed.saw_null());
}

TEST(RuntimeFilterTest, SmallDomainUsesTheExactSet) {
  const std::vector<int64_t> keys = {5, 1, 3};
  const auto group = ExactOf(keys);
  ASSERT_NE(group, nullptr);
  EXPECT_TRUE(group->Has(RuntimeFilterKind::kExactSet));
  EXPECT_FALSE(group->Has(RuntimeFilterKind::kBloom));
  EXPECT_FALSE(group->Has(RuntimeFilterKind::kMinMax));
  EXPECT_EQ(group->DistinctKeys(), 3U);
  EXPECT_EQ(group->PartCount(), 1U);
  EXPECT_EQ(Oracle(*group, {0, 1, 2, 3, 4, 5, 6}),
            (std::set<int64_t>{1, 3, 5}));
}

TEST(RuntimeFilterTest, ExactSetHandlesEveryInt64Boundary) {
  const std::vector<int64_t> boundaries = {kMinI64, -1, 0, 1, kMaxI64};
  const auto group = ExactOf(boundaries);
  ASSERT_NE(group, nullptr);
  for (const int64_t key : boundaries) {
    EXPECT_FALSE(group->Reject(key)) << "boundary " << key;
  }
  EXPECT_TRUE(group->Reject(-2));
  EXPECT_TRUE(group->Reject(2));
  // INT64_MAX - 1 and INT64_MIN + 1 are one step inside the domain.
  EXPECT_TRUE(group->Reject(kMaxI64 - 1));
  EXPECT_TRUE(group->Reject(kMinI64 + 1));
}

TEST(RuntimeFilterTest, MinMaxEnclosureRejectsStrictlyOutsideOnly) {
  MinMaxRuntimeFilter range(/*lo=*/10, /*hi=*/20, /*distinct_keys=*/11);
  EXPECT_FALSE(range.Reject(10));
  EXPECT_FALSE(range.Reject(15));
  EXPECT_FALSE(range.Reject(20));
  EXPECT_TRUE(range.Reject(9));
  EXPECT_TRUE(range.Reject(21));
  EXPECT_EQ(range.DistinctKeys(), 11U);
  EXPECT_EQ(range.MemoryBytes(), sizeof(MinMaxRuntimeFilter));
}

TEST(RuntimeFilterTest, MinMaxClampsAnInvertedEnclosureInsteadOfRejectingAll) {
  // Defensive: a caller that mixes up the bounds must not end up with a filter
  // that rejects even the build keys.
  MinMaxRuntimeFilter swapped(/*lo=*/20, /*hi=*/10, /*distinct_keys=*/1);
  EXPECT_FALSE(swapped.Reject(15));
  EXPECT_TRUE(swapped.Reject(21));
  EXPECT_TRUE(swapped.Reject(9));
  EXPECT_EQ(swapped.Lo(), 10);
  EXPECT_EQ(swapped.Hi(), 20);
}

TEST(RuntimeFilterTest, LargeDomainFallsBackToMinMaxAndBloom) {
  std::vector<int64_t> keys;
  keys.reserve(RuntimeFilterBuilder::kMaxExactKeys + 1);
  const auto kMaxExact =
      static_cast<int64_t>(RuntimeFilterBuilder::kMaxExactKeys);
  for (int64_t i = 0; i <= kMaxExact; ++i) {
    keys.push_back(i * 2);
  }
  const auto group = ExactOf(keys);
  ASSERT_NE(group, nullptr);
  EXPECT_FALSE(group->Has(RuntimeFilterKind::kExactSet));
  EXPECT_TRUE(group->Has(RuntimeFilterKind::kMinMax));
  EXPECT_TRUE(group->Has(RuntimeFilterKind::kBloom));
  EXPECT_EQ(group->PartCount(), 2U);
  EXPECT_EQ(group->DistinctKeys(), keys.size());
  const auto describe = group->Describe();
  EXPECT_NE(describe.find("min_max"), std::string::npos);
  EXPECT_NE(describe.find("bloom"), std::string::npos);
  EXPECT_NE(describe.find("keys="), std::string::npos);
  for (const int64_t key : keys) {
    EXPECT_FALSE(group->Reject(key)) << "build key " << key;
  }
  EXPECT_TRUE(group->Reject(-1));
  EXPECT_TRUE(group->Reject(kMaxI64));
}

TEST(RuntimeFilterTest, BloomNeverProducesAFalseNegative) {
  // 200k distinct odd keys: the domain is sparse, so min/max rejects almost
  // nothing and the Bloom part carries the pruning power. Membership is still
  // checked against the oracle: survivors must be a superset of the build keys.
  constexpr int64_t kCount = 200000;
  std::vector<int64_t> build;
  build.reserve(kCount);
  for (int64_t i = 0; i < kCount; ++i) {
    build.push_back((i * 2) + 1);
  }
  const auto group = ExactOf(build);
  ASSERT_NE(group, nullptr);
  ASSERT_TRUE(group->Has(RuntimeFilterKind::kBloom));
  for (const int64_t key : build) {
    ASSERT_FALSE(group->Reject(key)) << "build key " << key;
  }
  size_t survivors = 0;
  for (int64_t i = 0; i < kCount; ++i) {
    if (!group->Reject(i * 2)) {  // the even keys are all absent
      ++survivors;
    }
  }
  // A Bloom filter may pass some absent keys through, but at ~10 bits/key and
  // 2 probes the false-positive rate is a few percent -- never "most of them".
  EXPECT_LT(survivors, static_cast<size_t>(kCount / 10));
  // ~10 bits/key over the odd domain: the false-positive rate on absent keys
  // must stay far below the 50% that would make the filter pointless.
  size_t false_positives = 0;
  for (int64_t i = 0; i < 20000; ++i) {
    const int64_t absent = -((i * 2) + 3);
    if (!group->Reject(absent)) {
      ++false_positives;
    }
  }
  EXPECT_LT(false_positives, 2000U);
}

TEST(RuntimeFilterTest, BloomConformsToAnArbitraryBitCount) {
  // A non power-of-two / undersized bitset would index out of bounds. The
  // repair stops pruning instead of inventing bits: zeroing the bits would
  // reject every build key, which is unsound.
  std::vector<uint64_t> too_small(1, 0);
  BloomRuntimeFilter broken(std::move(too_small), /*bit_count=*/1000,
                            /*distinct_keys=*/1);
  EXPECT_TRUE(broken.disabled());
  EXPECT_EQ(broken.BitCount(), 0U);
  for (int64_t key = -100; key < 100; ++key) {
    EXPECT_FALSE(broken.Reject(key))
        << "disabled filter must not prune " << key;
  }
  std::vector<uint64_t> empty_bits;
  BloomRuntimeFilter zero(std::move(empty_bits), /*bit_count=*/0,
                          /*distinct_keys=*/0);
  EXPECT_TRUE(zero.disabled());
  EXPECT_FALSE(zero.Reject(0));
}

TEST(RuntimeFilterTest, BloomProbesAreIndependentPositions) {
  // Two probes that collapsed onto one position would halve the effective
  // bit count; the build below must set two distinct bits per key.
  for (int64_t key = 0; key < 1000; ++key) {
    const size_t a =
        static_cast<size_t>(BloomRuntimeFilter::Hash(key, 0)) & 511;
    const size_t b =
        static_cast<size_t>(BloomRuntimeFilter::Hash(key, 1)) & 511;
    EXPECT_NE(a, b) << "key " << key;
  }
}

TEST(RuntimeFilterTest, OverflowFallsBackToTheRangeEnclosureOnly) {
  // A truncated key set would prune rows that DO match, so an overflowing
  // builder keeps only the running [lo, hi] enclosure.
  RuntimeFilterBuilder builder;
  const auto count =
      static_cast<int64_t>(RuntimeFilterBuilder::kMaxTrackedKeys);
  for (int64_t i = 0; i <= count; ++i) {
    builder.Observe(i);
  }
  EXPECT_TRUE(builder.overflowed());
  const auto group = builder.Build();
  ASSERT_NE(group, nullptr);
  EXPECT_TRUE(group->Has(RuntimeFilterKind::kMinMax));
  EXPECT_FALSE(group->Has(RuntimeFilterKind::kExactSet));
  EXPECT_FALSE(group->Has(RuntimeFilterKind::kBloom));
  EXPECT_EQ(group->PartCount(), 1U);
  // Sound: every observed key lies inside the surviving enclosure.
  for (int64_t i = 0; i <= count; i += 4093) {
    EXPECT_FALSE(group->Reject(i)) << "observed key " << i;
  }
  EXPECT_TRUE(group->Reject(-1));
  EXPECT_TRUE(group->Reject(count + 1));
}

TEST(RuntimeFilterTest, ObservationsAfterOverflowKeepOnlyTheEnclosure) {
  // Once the cap was hit the key set must stay retired: re-filling it would
  // burn the 16 MiB cap twice for nothing (Build() ignores it anyway) and
  // report a bogus distinct-key count in EXPLAIN.
  RuntimeFilterBuilder builder;
  const auto count =
      static_cast<int64_t>(RuntimeFilterBuilder::kMaxTrackedKeys);
  for (int64_t i = 0; i < count; ++i) {
    builder.Observe(i);
  }
  EXPECT_FALSE(builder.overflowed());
  // Push past the cap, then keep observing: every post-overflow key must
  // still widen the enclosure (soundness) without re-populating the set.
  for (int64_t i = count; i < count + 4096; ++i) {
    builder.Observe(i * 3);
  }
  EXPECT_TRUE(builder.overflowed());
  EXPECT_EQ(builder.DistinctKeys(), 0U);
  const auto group = builder.Build();
  ASSERT_NE(group, nullptr);
  EXPECT_TRUE(group->Has(RuntimeFilterKind::kMinMax));
  EXPECT_EQ(group->PartCount(), 1U);
  EXPECT_EQ(group->DistinctKeys(), 0U);
  for (int64_t i = 0; i < count; i += 8192) {
    EXPECT_FALSE(group->Reject(i)) << "observed key " << i;
  }
  for (int64_t i = count; i < count + 4096; ++i) {
    EXPECT_FALSE(group->Reject(i * 3)) << "observed key " << i * 3;
  }
  EXPECT_TRUE(group->Reject(-1));
  EXPECT_TRUE(group->Reject(((count + 4095) * 3) + 1));
}

TEST(RuntimeFilterTest, ExactSetSortsItsInputDefensively) {
  // A caller that hands over an unsorted set would make binary_search reject
  // every row; the constructor repairs it.
  const ExactSetRuntimeFilter filter({9, 1, 5, 1, 9});
  EXPECT_FALSE(filter.Reject(1));
  EXPECT_FALSE(filter.Reject(5));
  EXPECT_FALSE(filter.Reject(9));
  EXPECT_TRUE(filter.Reject(4));
  EXPECT_EQ(filter.DistinctKeys(), 3U);
  EXPECT_EQ(filter.Min(), 1);
  EXPECT_EQ(filter.Max(), 9);
}

TEST(RuntimeFilterTest, EmptyGroupRejectsNothing) {
  const RuntimeFilterGroup empty;
  EXPECT_TRUE(empty.empty());
  EXPECT_EQ(empty.PartCount(), 0U);
  EXPECT_EQ(empty.DistinctKeys(), 0U);
  EXPECT_EQ(empty.MemoryBytes(), sizeof(RuntimeFilterGroup));
  EXPECT_EQ(empty.Describe(), "none");
  EXPECT_FALSE(empty.Reject(0));
  EXPECT_FALSE(empty.Reject(kMaxI64));
}

TEST(RuntimeFilterTest, GroupIsConjunctive) {
  std::vector<std::unique_ptr<RuntimeFilter>> parts;
  parts.push_back(std::make_unique<MinMaxRuntimeFilter>(0, 100, 1));
  parts.push_back(std::make_unique<ExactSetRuntimeFilter>(
      std::vector<int64_t>{10, 20, 30}));
  const RuntimeFilterGroup group(std::move(parts));
  EXPECT_FALSE(group.Reject(10));
  EXPECT_FALSE(group.Reject(30));
  EXPECT_TRUE(group.Reject(11));   // inside the range, absent from the set
  EXPECT_TRUE(group.Reject(101));  // outside the range
  EXPECT_TRUE(group.Reject(-1));
  EXPECT_EQ(Oracle(group, {9, 10, 11, 30, 31, 100, 101}),
            (std::set<int64_t>{10, 30}));
}

TEST(RuntimeFilterTest, RandomizedMembershipMatchesTheOracle) {
  // A fixed seed keeps this oracle reproducible; that is the point.
  std::mt19937_64 rng(20260926);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
  for (int round = 0; round < 32; ++round) {
    const size_t build_size = 1 + (rng() % 6000);
    std::vector<int64_t> build;
    build.reserve(build_size);
    for (size_t i = 0; i < build_size; ++i) {
      build.push_back(static_cast<int64_t>(rng()));
    }
    const auto group = ExactOf(build);
    ASSERT_NE(group, nullptr);
    std::set<int64_t> build_set(build.begin(), build.end());
    for (int probe = 0; probe < 200; ++probe) {
      const auto key = static_cast<int64_t>(rng());
      if (build_set.contains(key)) {
        ASSERT_FALSE(group->Reject(key)) << "round " << round << " key " << key;
      }
    }
    // NULL is never in the domain, and a NULL key is rejected by the consumer,
    // never by the group.
    EXPECT_FALSE(group->Reject(0) && build_set.contains(0));
  }
}

TEST(RuntimeFilterTest, ScanRuntimeFilterBindingIsTypeChecked) {
  RuntimeFilterBuilder builder;
  builder.Observe(10);
  builder.Observe(20);
  ScanRuntimeFilter int_filter;
  int_filter.group = builder.Build();
  int_filter.column = 3;
  int_filter.type = ValueType::kInt64;
  EXPECT_TRUE(int_filter.enabled());
  EXPECT_FALSE(int_filter.Reject(10));
  EXPECT_TRUE(int_filter.Reject(11));
  // A DATE probe column shares the int64 domain but not the comparison
  // domain: the binding must not prune it.
  EXPECT_FALSE(int_filter.Reject(static_cast<int8_t>(ValueType::kDate),
                                 /*is_null=*/false, 11));
  // NULL keys never match an equi-join, so they are always rejected.
  EXPECT_TRUE(int_filter.Reject(static_cast<int8_t>(ValueType::kInt64),
                                /*is_null=*/true, 10));
  // A disabled binding never rejects.
  ScanRuntimeFilter off;
  EXPECT_FALSE(off.enabled());
  EXPECT_FALSE(off.Reject(11));
  EXPECT_FALSE(off.Reject(static_cast<int8_t>(ValueType::kInt64), true, 0));
}

TEST(RuntimeFilterTest, FromKeysMatchesObserve) {
  const std::vector<int64_t> keys = {4, 4, 1, 9};
  const auto from_keys = RuntimeFilterBuilder::FromKeys(keys);
  RuntimeFilterBuilder builder;
  for (const int64_t key : keys) {
    builder.Observe(key);
  }
  const auto observed = builder.Build();
  ASSERT_NE(from_keys, nullptr);
  ASSERT_NE(observed, nullptr);
  EXPECT_EQ(from_keys->Describe(), observed->Describe());
  EXPECT_EQ(RuntimeFilterBuilder::FromKeys({}), nullptr);
}

TEST(RuntimeFilterTest, KindNamesAreStable) {
  EXPECT_STREQ(RuntimeFilterKindName(RuntimeFilterKind::kExactSet),
               "exact_set");
  EXPECT_STREQ(RuntimeFilterKindName(RuntimeFilterKind::kMinMax), "min_max");
  EXPECT_STREQ(RuntimeFilterKindName(RuntimeFilterKind::kBloom), "bloom");
  EXPECT_STREQ(RuntimeFilterKindName(RuntimeFilterKind::kRejectAll),
               "reject_all");
}

// The soundness contract lets one group be shared by every concurrent scan
// worker. Hammer it from several threads so TSAN flags any accidental mutable
// state, and check the exact-set result stays the no-false-positives answer.
TEST(RuntimeFilterTest, ConcurrentRejectsFromSharedWorkersAreRaceFree) {
  RuntimeFilterBuilder builder;
  for (int64_t i = 0; i < 4000; ++i) {
    builder.Observe(i * 3);
  }
  const auto group = builder.Build();
  ASSERT_NE(group, nullptr);
  ASSERT_TRUE(group->Has(RuntimeFilterKind::kExactSet));

  constexpr int64_t kProbeBegin = -1000;
  constexpr int64_t kProbeEnd = 21000;
  // Workers stride by 4 so their probe sequences partition, not overlap.
  std::atomic<int64_t> accepted{0};
  {
    std::vector<std::jthread> workers;
    workers.reserve(4);
    for (int64_t t = 0; t < 4; ++t) {
      workers.emplace_back([&group, &accepted, t] {
        int64_t local = 0;
        for (int64_t key = kProbeBegin + t; key < kProbeEnd; key += 4) {
          local += group->Reject(key) ? 0 : 1;
        }
        accepted.fetch_add(local, std::memory_order_relaxed);
      });
    }
  }
  // Every accepted key is a genuine build key: no false positives allowed.
  int64_t expected = 0;
  for (int64_t key = kProbeBegin; key < kProbeEnd; ++key) {
    expected += (key >= 0 && key <= int64_t{3999} * 3 && key % 3 == 0) ? 1 : 0;
  }
  EXPECT_EQ(accepted.load(), expected);
}

// A malformed bitset must disable pruning entirely (never reject), because a
// filter that rejects build keys would drop probe rows that do match.
TEST(RuntimeFilterTest, MalformedBitsetDisablesPruning) {
  BloomRuntimeFilter non_power_of_two(std::vector<uint64_t>(3, ~0ULL),
                                      /*bit_count=*/100, /*distinct_keys=*/5);
  EXPECT_TRUE(non_power_of_two.disabled());
  EXPECT_FALSE(non_power_of_two.Reject(12345));
  EXPECT_EQ(non_power_of_two.MemoryBytes(), sizeof(non_power_of_two));

  BloomRuntimeFilter short_words(std::vector<uint64_t>(1, ~0ULL),
                                 /*bit_count=*/512, /*distinct_keys=*/5);
  EXPECT_TRUE(short_words.disabled());
  EXPECT_FALSE(short_words.Reject(1));

  BloomRuntimeFilter zero_bits(std::vector<uint64_t>{}, /*bit_count=*/0,
                               /*distinct_keys=*/0);
  EXPECT_TRUE(zero_bits.disabled());
  EXPECT_FALSE(zero_bits.Reject(-7));
}

}  // namespace
}  // namespace tinylamb
