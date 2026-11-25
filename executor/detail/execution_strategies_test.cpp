/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/execution_strategies.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "executor/selection_vector.hpp"
#include "executor/zone_map.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "gtest/gtest.h"
#include "table/lstable_bridge.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::execution_strategies {
namespace {

// 1. FrameIndexes (Wave7FrameIndexes)
TEST(ExecutionStrategiesTest, FrameIndexesMatchBruteForce) {
  const std::vector<int64_t> xs{5, 2, 8, 1, 9, 3, 7, 4};
  SparseMinimumTable smt;
  smt.Build(xs);
  for (size_t l = 0; l < xs.size(); ++l) {
    for (size_t r = l + 1; r <= xs.size(); ++r) {
      const int64_t ref =
          *std::min_element(xs.begin() + static_cast<std::ptrdiff_t>(l),
                            xs.begin() + static_cast<std::ptrdiff_t>(r));
      EXPECT_EQ(smt.Query(l, r), ref);
    }
  }

  const std::vector<std::optional<int64_t>> nxs{10, std::nullopt, -3,
                                                7,  std::nullopt, 5};
  PrefixNullableSum pns;
  pns.Build(nxs);
  EXPECT_EQ(pns.Query(0, 6), (std::pair<int64_t, size_t>{19, 4}));
  EXPECT_EQ(pns.Query(1, 5), (std::pair<int64_t, size_t>{4, 2}));

  EXPECT_EQ(SlidingMinimum(xs, 3),
            (std::vector<int64_t>{5, 2, 2, 1, 1, 1, 3, 3}));
  EXPECT_EQ(TileArithmetic(10, 3, 0), 0U);
  EXPECT_EQ(TileArithmetic(10, 3, 3), 0U);
  EXPECT_EQ(TileArithmetic(10, 3, 4), 1U);
  EXPECT_EQ(TileArithmetic(10, 3, 7), 2U);
  EXPECT_EQ(SessionBoundaryCount({10, 12, 25, 27, 40}, 5), 3U);
  EXPECT_EQ(PeerRankRuns({{10, 2}, {20, 3}, {30, 1}}),
            (std::vector<std::pair<size_t, size_t>>{{1, 1}, {3, 2}, {6, 3}}));
  EXPECT_EQ(ExcludePeerSelect(xs, 2, 5, 2), 3);
  EXPECT_EQ(DisjointRangeFold({"a", "bc", "d", "ef"}, 1, 3), "bcd");
  EXPECT_EQ(WaveletQuantile({5, 2, 8, 1, 9, 3}, 1, 5, 2), 8);
}

// 2. CorrelatedProbes (Wave7CorrelatedProbes)
TEST(ExecutionStrategiesTest, CorrelatedProbesMatchReference) {
  EXPECT_TRUE(
      RelationalDivision({1, std::nullopt, 2}, {3, 2, std::nullopt, 1}));
  EXPECT_FALSE(RelationalDivision({1, 2, 4}, {1, 2, 3}));

  EXPECT_EQ(OffsetScalar({{1, 10}, {2, 20}, {1, 30}, {1, std::nullopt}}, 1, 1),
            30);
  EXPECT_EQ(OffsetScalar({{1, 10}, {1, std::nullopt}}, 1, 1), std::nullopt);

  EXPECT_EQ(ExcludingMinimum({5, 1, 4, 2}, 1), 2);
  EXPECT_EQ(ExcludingMinimum({5, 1, 4, 2}, 0), 1);

  EXPECT_EQ(CountDistinctStop({1, 2, 1, std::nullopt, 3, 4}, 2), 2U);
  EXPECT_EQ(CountDistinctStop({1, 1, std::nullopt}, 5), 1U);

  NeWitnessSummary w1;
  w1.Build({std::nullopt, 5, 5});
  EXPECT_FALSE(w1.HasNotEqual(5));
  EXPECT_TRUE(w1.HasNotEqual(6));
  EXPECT_FALSE(w1.HasNotEqual(std::nullopt));

  NeWitnessSummary w2;
  w2.Build({5, 7, 5});
  EXPECT_TRUE(w2.HasNotEqual(5));
  EXPECT_TRUE(w2.HasNotEqual(7));

  EXPECT_EQ(OrExistsFilter({{1, 10}, {2, 20}, {3, 30}}, {1, 9}, {30}),
            (std::vector<std::pair<uint64_t, uint64_t>>{{1, 10}, {3, 30}}));
  EXPECT_EQ(PrefixAntichain({"foo", "foobar", "bar", "foo", "baz", "barx"}),
            (std::vector<std::string>{"bar", "baz", "foo"}));
}

// 3. IndexPreimages (Wave7IndexPreimages) + live zone_map.cpp integration
TEST(ExecutionStrategiesTest, IndexPreimagesAndZoneMapIntegration) {
  // TruncBucketPreimage: exact match against C++ integer division (trunc toward
  // 0) for positive, zero, and negative quotients.
  for (int64_t d : {1, 3, 7}) {
    for (int64_t q = -3; q <= 3; ++q) {
      const IntInterval iv = TruncBucketPreimage(d, q);
      for (int64_t x = -30; x <= 30; ++x) {
        EXPECT_EQ(!iv.empty && iv.lo <= x && x <= iv.hi, (x / d == q))
            << "d=" << d << " q=" << q << " x=" << x;
      }
    }
  }
  EXPECT_EQ(RoundedBucketPreimage(4, 2), (IntInterval{6, 9, false}));
  EXPECT_EQ(ModularLookup(7, 5, 2, 4), 3);  // (3*3 + 2) % 7 = 4, inv(3,7)=5
  EXPECT_EQ(DescendingBandPreimage(1, 2, 20, 4), (IntInterval{9, 16, false}));
  EXPECT_EQ(CheckedDistanceBand(10, 3), (IntInterval{7, 13, false}));
  EXPECT_TRUE(CheckedDistanceBand(10, -1).empty);
  const auto [lo_ring, hi_ring] = DistanceRingPreimage(10, 2, 5);
  EXPECT_EQ(lo_ring, (IntInterval{5, 8, false}));
  EXPECT_EQ(hi_ring, (IntInterval{12, 15, false}));
  EXPECT_EQ(CursorRangeFilter({{1, 5}, {1, 6}, {2, 0}}, {1, 5}),
            (std::vector<std::pair<int64_t, int64_t>>{{1, 6}, {2, 0}}));
  const auto [m_lo, m_hi] = MortonSquareRange(5, 6, 1);
  for (uint32_t x = 4; x <= 5; ++x) {
    for (uint32_t y = 6; y <= 7; ++y) {
      const uint64_t code = MortonEncode2D(x, y);
      EXPECT_GE(code, m_lo);
      EXPECT_LE(code, m_hi);
    }
  }
  EXPECT_EQ(PeriodicSeek(10, 20, 4, 1), 13);
  EXPECT_EQ(PeriodicSeek(10, 12, 4, 1), std::nullopt);

  // Live ZoneMap Kleene pruning for ABS(x - 50) <= 5 and DIV(x, 10) = 5.
  Schema schema("t", {Column("x", ValueType::kInt64)});
  LSTablePageBlock blk;
  blk.row_count = 10;
  LSTableColumnBlock col;
  col.type = ValueType::kInt64;
  col.has_min_max = true;
  col.min_int = 100;
  col.max_int = 200;
  blk.columns.resize(1);
  blk.columns[0] = col;

  const Expression abs_pred = BinaryExpressionExp(
      FunctionCallExp(
          "ABS",
          {BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kSubtract,
                               ConstantValueExp(Value(50)))}),
      BinaryOperation::kLessThanEquals, ConstantValueExp(Value(5)));
  EXPECT_EQ(EvaluateBlockKleene(abs_pred, schema, blk), kKleeneF);

  const Expression div_pred = BinaryExpressionExp(
      FunctionCallExp("DIV",
                      {ColumnValueExp("x"), ConstantValueExp(Value(10))}),
      BinaryOperation::kEquals, ConstantValueExp(Value(5)));
  EXPECT_EQ(EvaluateBlockKleene(div_pred, schema, blk), kKleeneF);

  blk.columns[0].min_int = 50;
  blk.columns[0].max_int = 54;
  EXPECT_EQ(EvaluateBlockKleene(abs_pred, schema, blk), kKleeneT);
  EXPECT_EQ(EvaluateBlockKleene(div_pred, schema, blk), kKleeneT);
}

// 4. OrderedStreams (Wave7OrderedStreams)
TEST(ExecutionStrategiesTest, OrderedStreamsMatchReference) {
  const std::vector<int64_t> a{1, 4, 7, 10};
  const std::vector<int64_t> b{2, 3, 7, 12};
  const auto [i, j] = MergeDiagonal(a, b, 5);
  EXPECT_EQ(i + j, 5U);
  EXPECT_EQ(i, 2U);
  EXPECT_EQ(j, 3U);

  EXPECT_EQ(
      MonotoneProbes({1, 3, 7, 15}, {1, 2, 7, 7, 10}),
      (std::vector<std::optional<size_t>>{0, std::nullopt, 2, std::nullopt}));
  EXPECT_EQ(WeightedOffset({{10, 2}, {20, 3}, {30, 1}}, 4), 20);
  EXPECT_EQ(RangeCoalescing({1, 2, 4, 10, 12}, 2),
            (std::vector<std::pair<uint64_t, uint64_t>>{{1, 4}, {10, 12}}));
  EXPECT_EQ(
      RunIntersection({{0, 5}, {10, 20}}, {{3, 12}, {15, 25}}),
      (std::vector<std::pair<int64_t, int64_t>>{{3, 5}, {10, 12}, {15, 20}}));
}

// 5. PairAggregates (Wave7PairAggregates)
TEST(ExecutionStrategiesTest, PairAggregatesMatchBruteForce) {
  const std::vector<int64_t> xs{3, -1, 7, 2};
  const std::vector<int64_t> ys{5, 0, -2};
  int64_t abs_ref = 0;
  for (int64_t a : xs) {
    for (int64_t b : xs) {
      abs_ref += std::abs(a - b);
    }
  }
  EXPECT_EQ(AbsoluteDistanceSum(xs), abs_ref);

  int64_t sq_ref = 0;
  for (int64_t a : xs) {
    for (int64_t b : ys) {
      sq_ref += (a - b) * (a - b);
    }
  }
  EXPECT_EQ(SquaredDistanceSum(xs, ys), sq_ref);

  EXPECT_EQ(ResidualInequalityCount(xs, ys, 3), 6U);
  EXPECT_EQ(SameGroupPairsCount({1, 2, 1, 1, 2}), 4U);
  EXPECT_EQ(ComplementProbeCount(xs, ys, 2), 1U);
  EXPECT_EQ(IncreasingTriplesCount({1, 5}, {2, 4, 6}, {3, 7}), 5U);

  const std::vector<uint8_t> bx{1, 5, 12};
  const std::vector<uint8_t> by{3, 9};
  uint64_t xor_ref = 0;
  for (uint8_t a : bx) {
    for (uint8_t b : by) {
      xor_ref += static_cast<uint64_t>(a ^ b);
    }
  }
  EXPECT_EQ(XorDistanceSum(bx, by), xor_ref);

  const std::vector<uint64_t> la{2, 5, 1};
  const std::vector<uint64_t> lb{4, 3};
  uint64_t lc_ref = 0;
  for (uint64_t a : la) {
    for (uint64_t b : lb) {
      lc_ref += std::min(a, b);
    }
  }
  EXPECT_EQ(LayerCakeMinimumSum(la, lb), lc_ref);
}

// 6. PlaneKernels (Wave7PlaneKernels)
TEST(ExecutionStrategiesTest, PlaneKernelsMatchScalar) {
  std::array<uint8_t, 64> ax{};
  std::array<uint8_t, 64> ay{};
  std::array<uint8_t, 64> az{};
  for (size_t i = 0; i < 64; ++i) {
    ax[i] = static_cast<uint8_t>((i * 37 + 11) & 0xFF);
    ay[i] = static_cast<uint8_t>((i * 53 + 7) & 0xFF);
    az[i] = static_cast<uint8_t>((i * 19 + 3) & 0xFF);
  }
  const BitPlane8x64 px = BitPlane8x64::FromBytes(ax);
  const BitPlane8x64 py = BitPlane8x64::FromBytes(ay);
  const BitPlane8x64 pz = BitPlane8x64::FromBytes(az);
  const uint64_t mask = 0x0F0F0F0F0F0F0F0FULL;

  const uint64_t lt = PlaneParallelLess(px, py, mask);
  const BitPlane8x64 sum = PlaneAdd(px, py);
  const BitPlane8x64 med = PlaneMedianOfThree(px, py, pz);
  uint64_t dot_ref = 0;
  std::vector<uint8_t> masked_vals;
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(sum.At(i), static_cast<uint8_t>((ax[i] + ay[i]) & 0xFF));
    std::array<uint8_t, 3> tri{ax[i], ay[i], az[i]};
    std::ranges::sort(tri);
    EXPECT_EQ(med.At(i), tri[1]);
    if (((mask >> i) & 1ULL) != 0) {
      EXPECT_EQ(((lt >> i) & 1ULL) != 0, ax[i] < ay[i]);
      dot_ref += static_cast<uint64_t>(ax[i]) * ay[i];
      masked_vals.push_back(ax[i]);
    }
  }
  EXPECT_EQ(PlaneDot(px, py, mask), dot_ref);
  std::ranges::sort(masked_vals);
  EXPECT_EQ(PlaneRadixSelect(px, mask, 5), masked_vals[5]);
  const uint64_t argmin = PlaneArgMinMask(px, mask);
  EXPECT_NE(argmin, 0ULL);
  EXPECT_EQ(px.At(static_cast<size_t>(std::countr_zero(argmin))),
            masked_vals[0]);
  EXPECT_EQ(PlaneRunBoundary(px, ax[0], mask) & 1ULL, 1ULL);
}

// 7. EncodedBlocks (Wave7EncodedBlocks)
TEST(ExecutionStrategiesTest, EncodedBlocksMatchReference) {
  EXPECT_EQ(PatchedRangeFilter(100, 102, 105, {0, 2, 5, 10}, {{3, 103}}),
            (std::vector<size_t>{1, 2, 3}));
  EXPECT_EQ(OverlapDot({{2, 3}, {5, 2}}, {{1, 2}, {4, 3}}), (2 * 2) + 8 + 40);
  EXPECT_EQ(ApplyRangeEdits({1, 1, 1, 1, 1}, {{1, 4, 3}, {2, 5, -1}}),
            (std::vector<int64_t>{1, 4, 3, 3, 0}));
  EXPECT_EQ(DistinctSeamsCount({{1, 2, 2}, {2, 3}, {}, {3, 5}}), 4U);
  EXPECT_EQ(SecondDifferencesDecode(10, 12, {1, 2, -1}),
            (std::vector<int64_t>{10, 12, 15, 20, 24}));
}

// 8. SearchBounds (Wave7SearchBounds)
TEST(ExecutionStrategiesTest, SearchBoundsHold) {
  const std::vector<int64_t> a{1, 1, 2, 3};
  const std::vector<int64_t> b{1, 2, 2, 2};
  EXPECT_GE(CollisionJoinBound(a, b), CappedJoinCount(a, b, 100));
  EXPECT_EQ(CappedJoinCount(a, b, 4), 4U);
  EXPECT_EQ(CappedJoinCount(a, b, 100), 5U);
  EXPECT_EQ(TriangleAglBound(4, 9, 4), 12U);
  const auto mg = MisraGriesCandidates({1, 1, 1, 2, 3, 1, 4}, 2);
  EXPECT_TRUE(mg.contains(1));
  EXPECT_TRUE(PredicateExchangePreferFirst(100, 1, 10, 10, 50));
}

// 9. DeltaMaintenance (Wave7DeltaMaintenance)
TEST(ExecutionStrategiesTest, DeltaMaintenanceMatchesFullRecompute) {
  const KeyBag a{{1, 2}, {2, 1}};
  const KeyBag b{{1, 3}, {2, 0}};
  const KeyBag da{{1, 1}};
  const KeyBag db{{1, -1}, {2, 2}};
  const KeyBag dj = DeltaJoinBatch(a, b, da, db);
  // New (a+da)*(b+db): k=1 -> 3*2=6 (old 6 -> d=0), k=2 -> 1*2=2 (old 0 -> d=2)
  EXPECT_EQ(dj, (KeyBag{{2, 2}}));

  const KeyBag dop = DeltaOuterPadding(a, b, db);
  // k=1: old_fan=3, new_fan=2 -> 2*(2-3)=-2; k=2: old_fan=1, new_fan=2 -> 1*1=1
  EXPECT_EQ(dop, (KeyBag{{1, -2}, {2, 1}}));

  EXPECT_EQ(DeltaMinimumRepair(5, 2, {5}, {10}), 5);
  EXPECT_EQ(DeltaMinimumRepair(5, 1, {5}, {3}), 3);
  EXPECT_EQ(DeltaMinimumRepair(5, 1, {5}, {10}), std::nullopt);
}

// 10. RecursiveJoins (Wave7RecursiveJoins)
TEST(ExecutionStrategiesTest, RecursiveJoinsMatchReference) {
  EXPECT_TRUE(
      MeetFrontiersReachable(5, {{0, 1}, {1, 2}, {2, 3}, {3, 4}}, 0, 4));
  EXPECT_FALSE(MeetFrontiersReachable(5, {{0, 1}, {2, 3}}, 0, 3));

  BinaryLiftingTable bl;
  bl.Build({1, 2, 3, 4, 0}, 6);
  EXPECT_EQ(bl.Jump(0, 13), 3U);

  // K_{2,3} bipartite graph has C(2,2)*C(3,2) = 3 butterflies.
  EXPECT_EQ(
      ButterflyCount(2, 3, {{0, 0}, {0, 1}, {0, 2}, {1, 0}, {1, 1}, {1, 2}}),
      3U);
  // K_4 has C(4,3) = 4 triangles.
  EXPECT_EQ(TriangleOrientationCount(
                4, {{0, 1}, {0, 2}, {0, 3}, {1, 2}, {1, 3}, {2, 3}}),
            4U);
}

// 11. VersionedIndexes (Wave7VersionedIndexes)
TEST(ExecutionStrategiesTest, VersionedIndexesPreserveSnapshotVisibility) {
  const std::vector<VersionedEntry> entries{{1, 10, false, 100},
                                            {1, 5, true, 0},
                                            {1, 2, false, 20},
                                            {2, 8, false, 200},
                                            {2, 3, false, 30}};
  EXPECT_EQ(SnapshotAnchorLookup(entries, 1, 10), 100);
  EXPECT_EQ(SnapshotAnchorLookup(entries, 1, 7), std::nullopt);
  EXPECT_EQ(SnapshotAnchorLookup(entries, 1, 3), 20);

  const std::vector<RangeTombstoneMark> rts{{1, 2, 9}};
  EXPECT_EQ(RangeTombstoneLookup(entries, rts, 1, 10), 100);
  EXPECT_EQ(RangeTombstoneLookup(entries, rts, 2, 10), std::nullopt);
  EXPECT_EQ(RangeTombstoneLookup(entries, rts, 2, 8), 200);

  const auto gc = BottomTombstoneGC(entries, 6);
  EXPECT_EQ(gc.size(), 4U);
  EXPECT_EQ(SnapshotAnchorLookup(gc, 1, 10), 100);

  const auto compacted = DominatedRunCompact(entries, 6);
  // Key 1 seq=2 is dominated by seq=5 <= 6; all t >= 6 snapshots preserved.
  EXPECT_EQ(compacted.size(), 4U);
  for (uint64_t t : {uint64_t{6}, uint64_t{7}, uint64_t{10}}) {
    EXPECT_EQ(SnapshotAnchorLookup(compacted, 1, t),
              SnapshotAnchorLookup(entries, 1, t));
    EXPECT_EQ(SnapshotAnchorLookup(compacted, 2, t),
              SnapshotAnchorLookup(entries, 2, t));
  }
}

// 12. Scan/Selection/Block-Skip Proofs (MaskToRanges, AdaptiveSelection,
//     RankScatter, TrigramSkip, BloomBlockIntersection, SnapshotZoneMap)
TEST(ExecutionStrategiesTest, SelectionAndBlockSkipProofsMatchReference) {
  // MaskToRanges + RankScatter + AdaptiveSelection
  ValidityBitmap bm(135, false);
  for (size_t i : {0, 1, 2, 62, 63, 64, 65, 130, 134}) {
    bm.SetBit(i);
  }
  std::vector<ValidityBitmap::RowRange> ranges;
  bm.ToContiguousRanges(&ranges, 100);
  ASSERT_EQ(ranges.size(), 4U);
  EXPECT_EQ(ranges[0].start, 100U);
  EXPECT_EQ(ranges[0].length, 3U);
  EXPECT_EQ(ranges[1].start, 162U);
  EXPECT_EQ(ranges[1].length, 4U);  // fused across 64-bit word boundary!
  EXPECT_EQ(ranges[2].start, 230U);
  EXPECT_EQ(ranges[2].length, 1U);
  EXPECT_EQ(ranges[3].start, 234U);
  EXPECT_EQ(ranges[3].length, 1U);

  SelectionVector sv;
  bm.ToSelectionVector(&sv);
  ASSERT_EQ(sv.Size(), 9U);
  const ValidityBitmap roundtrip = sv.ToValidityBitmap(135);
  EXPECT_EQ(roundtrip.CountValid(), 9U);
  for (size_t i = 0; i < 135; ++i) {
    EXPECT_EQ(roundtrip.Get(i), bm.Get(i));
  }

  std::vector<uint32_t> src(135);
  std::iota(src.begin(), src.end(), 1000U);
  std::vector<uint32_t> dst(135, 0);
  const size_t scattered = bm.RankScatter(src.data(), dst.data());
  ASSERT_EQ(scattered, 9U);
  EXPECT_EQ(dst[0], 1000U);
  EXPECT_EQ(dst[3], 1062U);
  EXPECT_EQ(dst[8], 1134U);

  // TrigramSkip
  TrigramBlockSummary trig;
  trig.AddString("https://example.com/catalog");
  trig.AddString("https://tinylamb.org/docs");
  EXPECT_FALSE(CanSkipLikeBlockByTrigrams("%example%", trig));
  EXPECT_FALSE(CanSkipLikeBlockByTrigrams("%tinylamb%docs%", trig));
  EXPECT_TRUE(CanSkipLikeBlockByTrigrams("%postgres%", trig));
  EXPECT_FALSE(CanSkipLikeBlockByTrigrams("%ab%", trig));  // < 3 chars

  // BloomBlockIntersection (seed match + null_safe_eq)
  BlockBloomSignature build_sig;
  build_sig.Init(4, 0x1234ULL);
  build_sig.AddInt64(10);
  build_sig.AddInt64(20);
  BlockBloomSignature probe_hit;
  probe_hit.Init(4, 0x1234ULL);
  probe_hit.AddInt64(20);
  EXPECT_FALSE(CanSkipBlockByBloomIntersection(build_sig, probe_hit));
  BlockBloomSignature probe_miss;
  probe_miss.Init(4, 0x1234ULL);
  probe_miss.AddInt64(999999);
  // Seed mismatch must NEVER skip (H1).
  BlockBloomSignature probe_bad_seed = probe_miss;
  probe_bad_seed.seed = 0x9999ULL;
  EXPECT_FALSE(CanSkipBlockByBloomIntersection(build_sig, probe_bad_seed));
  // Null-safe equality must not skip when both sides have NULL.
  build_sig.AddNull();
  probe_miss.AddNull();
  EXPECT_FALSE(CanSkipBlockByBloomIntersection(build_sig, probe_miss,
                                               /*null_safe=*/true));

  // SnapshotZoneMap
  EXPECT_TRUE(SnapshotZoneMapValid(10, 12, false));
  EXPECT_FALSE(SnapshotZoneMapValid(15, 12, false));
  EXPECT_FALSE(SnapshotZoneMapValid(10, 12, true));
}

// 13. Frontiers, Claude1125, Slices, Ceb, Retail Proofs
TEST(ExecutionStrategiesTest, FrontiersClaude1125AndAnalyticsProofs) {
  // SpillScheduling: ExcessFirst + LPT
  const auto victims = ExcessFirstSpillVictims({100, 500, 300, 50}, 700);
  EXPECT_EQ(victims, (std::vector<size_t>{1, 2}));
  const auto assign = LptAssignWorkers({10, 8, 6, 4, 2}, 2);
  ASSERT_EQ(assign.size(), 5U);
  std::array<uint64_t, 2> loads{0, 0};
  const std::vector<uint64_t> work{10, 8, 6, 4, 2};
  for (size_t i = 0; i < work.size(); ++i) {
    loads[assign[i]] += work[i];
  }
  EXPECT_LE(std::max(loads[0], loads[1]) - std::min(loads[0], loads[1]), 2ULL);

  // RankedJoins: WeightedSeek + NearPairs
  EXPECT_EQ(RankedJoinTopKSums({10, 7, 3}, {100, 90, 50}, 4),
            (std::vector<int64_t>{110, 107, 103, 100}));
  EXPECT_EQ(NearPairsCount({1, 5, 10}, {2, 6, 15}, 1), 2ULL);

  // Arithmetic: CheckedSumFirstOverflow + RepeatedScore + AverageFence
  const auto ok_sum = CheckedSumFirstOverflow({10, 20, -5});
  EXPECT_EQ(ok_sum.first, 25);
  EXPECT_EQ(ok_sum.second, std::nullopt);
  const auto ov_sum =
      CheckedSumFirstOverflow({10, std::numeric_limits<int64_t>::max(), 1});
  EXPECT_EQ(ov_sum.second, 1U);

  const auto rep_ok = RepeatedScoreCheckedAdd(10, 5, 4);
  EXPECT_EQ(rep_ok, (std::pair<int64_t, uint64_t>{30, 4ULL}));
  const auto rep_ov =
      RepeatedScoreCheckedAdd(std::numeric_limits<int64_t>::max() - 7, 3, 10);
  EXPECT_EQ(rep_ov, (std::pair<int64_t, uint64_t>{
                        std::numeric_limits<int64_t>::max() - 1, 2ULL}));

  EXPECT_TRUE(ExactAverageAtLeast(10, 3, 3, 1));
  EXPECT_FALSE(ExactAverageAtLeast(8, 3, 3, 1));

  // Claude1125: PagedTombstones + GrayTile + RestartedDeltaRank
  EXPECT_TRUE(CanSkipTombstoneEpochPage(20, 15));
  EXPECT_FALSE(CanSkipTombstoneEpochPage(15, 15));
  for (uint64_t i = 0; i < 64; ++i) {
    EXPECT_EQ(GrayTileDecode(GrayTileEncode(i)), i);
  }
  const std::vector<int64_t> sorted{2, 4, 6, 8, 10, 12, 14, 16, 18};
  EXPECT_EQ(RestartedDeltaRankLE(sorted, 3, 1), 0U);
  EXPECT_EQ(RestartedDeltaRankLE(sorted, 3, 10), 5U);
  EXPECT_EQ(RestartedDeltaRankLE(sorted, 3, 100), 9U);

  // Slices & PublicBI: ComplementGroupSum + WitnessedBlockExtrema
  EXPECT_EQ(ComplementGroupSum(1000, 230), 770);
  EXPECT_TRUE(CanSkipBlockForExtremum(50, 80, 40, /*is_min=*/true));
  EXPECT_FALSE(CanSkipBlockForExtremum(30, 80, 40, /*is_min=*/true));
  EXPECT_TRUE(CanSkipBlockForExtremum(10, 35, 40, /*is_min=*/false));
}

}  // namespace
}  // namespace tinylamb::execution_strategies
