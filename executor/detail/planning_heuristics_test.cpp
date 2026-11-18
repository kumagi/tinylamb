/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/detail/planning_heuristics.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor/detail/columnar_table.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::AnalyzePredicates;
using relational_detail::ConcatRows;
using relational_detail::EqualityPairKey;
using relational_detail::EstimateJoinRows;
using relational_detail::EstimateJoinRowsCached;
using relational_detail::HybridHashJoin;
using relational_detail::IsNullRejectingPredicate;
using relational_detail::JoinKeyFrequencyMaps;
using relational_detail::PredicateInfo;
using relational_detail::Relation;
using relational_detail::SameSingleIntegerJoinKey;
using relational_detail::SpillPartitionOf;
using relational_detail::SynthesizeDisjunctEqualityPredicates;

TEST(PlanningHeuristicsTest, EqualityPairKeyIsSymmetric) {
  ColumnName a("t1", "id");
  ColumnName b("t2", "id");
  EXPECT_EQ(EqualityPairKey(a, b), EqualityPairKey(b, a));
  EXPECT_NE(EqualityPairKey(a, b),
            EqualityPairKey(a, ColumnName("t2", "other")));
}

TEST(PlanningHeuristicsTest, ConcatRowsAndSameSingleIntegerJoinKey) {
  Row left({Value(int64_t{1}), Value("a")});
  Row right({Value(int64_t{2})});
  Row joined = ConcatRows(left, right);
  ASSERT_EQ(joined.values_.size(), 3U);
  EXPECT_EQ(joined[0], Value(int64_t{1}));
  EXPECT_EQ(joined[1], Value("a"));
  EXPECT_EQ(joined[2], Value(int64_t{2}));

  Schema s1("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64),
                   Column(ColumnName("t1", "v"), ValueType::kVarChar)});
  Schema s2("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64)});
  EXPECT_TRUE(SameSingleIntegerJoinKey(s1, {0}, s2, {0}));
  EXPECT_FALSE(SameSingleIntegerJoinKey(s1, {1}, s2, {0}));
}

TEST(PlanningHeuristicsTest, IsNullRejectingPredicate) {
  Expression eq = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "a")),
                                      BinaryOperation::kEquals,
                                      ConstantValueExp(Value(int64_t{10})));
  EXPECT_TRUE(IsNullRejectingPredicate(eq));

  Expression is_null = UnaryExpressionExp(ColumnValueExp(ColumnName("t1", "a")),
                                          UnaryOperation::kIsNull);
  EXPECT_FALSE(IsNullRejectingPredicate(is_null));
}

TEST(PlanningHeuristicsTest, SynthesizeDisjunctEqualityPredicates) {
  Relation r1;
  r1.schema = Schema("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64),
                            Column(ColumnName("t1", "x"), ValueType::kInt64)});
  Relation r2;
  r2.schema = Schema("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64),
                            Column(ColumnName("t2", "y"), ValueType::kInt64)});
  std::vector<Relation> relations;
  relations.push_back(std::move(r1));
  relations.push_back(std::move(r2));

  Expression eq_k = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "k")),
                                        BinaryOperation::kEquals,
                                        ColumnValueExp(ColumnName("t2", "k")));
  Expression b1 = BinaryExpressionExp(
      eq_k, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "x")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1}))));
  Expression b2 = BinaryExpressionExp(
      eq_k, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("t2", "y")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{2}))));
  Expression disj = BinaryExpressionExp(b1, BinaryOperation::kOr, b2);

  std::vector<PredicateInfo> analyzed = AnalyzePredicates(disj, relations);
  std::vector<PredicateInfo> synth =
      SynthesizeDisjunctEqualityPredicates(analyzed, relations);
  ASSERT_EQ(synth.size(), 1U);
  EXPECT_EQ(synth[0].sources.size(), 2U);
}

TEST(PlanningHeuristicsTest, EstimateJoinRowsAndCachedAgree) {
  Relation left;
  left.schema =
      Schema("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64)});
  left.rows = {Row({Value(int64_t{1})}), Row({Value(int64_t{2})}),
               Row({Value(int64_t{2})}), Row({Value(int64_t{3})})};

  Relation right;
  right.schema =
      Schema("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64)});
  right.rows = {Row({Value(int64_t{2})}), Row({Value(int64_t{2})}),
                Row({Value(int64_t{4})})};

  Expression eq = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "k")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "k")));
  std::vector<Expression> preds = {eq};

  const size_t est = EstimateJoinRows(left, right, preds);
  EXPECT_EQ(est, 4U);

  std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>> cache;
  EXPECT_EQ(EstimateJoinRowsCached(left, right, preds, &cache), 4U);
  EXPECT_EQ(EstimateJoinRowsCached(left, right, preds, &cache), 4U);
}

TEST(PlanningHeuristicsTest, EstimateJoinRowsOnSpilledRelations) {
  Relation left;
  left.schema =
      Schema("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64)});
  for (int64_t i = 0; i < 4000; ++i) {
    left.rows.push_back(Row({Value(i % 7)}));
  }
  Relation right;
  right.schema =
      Schema("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64)});
  for (int64_t i = 0; i < 3000; ++i) {
    right.rows.push_back(Row({Value(i % 5)}));
  }
  left.EnsureSpill();
  right.EnsureSpill();
  ASSERT_TRUE(left.HasSpill());
  ASSERT_TRUE(right.HasSpill());

  Expression eq = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "k")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "k")));
  std::vector<Expression> preds = {eq};

  // The sampled estimate must be positive, and repeated calls must agree.
  // (Sample-size bounds / avoiding a full re-decode are not asserted here.)
  const size_t est = EstimateJoinRows(left, right, preds);
  EXPECT_GT(est, 0U);

  std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>> cache;
  EXPECT_EQ(EstimateJoinRowsCached(left, right, preds, &cache), est);
  EXPECT_EQ(EstimateJoinRowsCached(left, right, preds, &cache), est);
}

TEST(PlanningHeuristicsTest,
     ColumnarMaterializeFlatIntoMultiHopGatheredAndNulls) {
  const Schema schema("t", {Column(ColumnName("t", "id"), ValueType::kInt64),
                            Column(ColumnName("t", "s"), ValueType::kVarChar)});
  auto base_tbl = std::make_shared<relational_detail::ColumnarTable>();
  {
    relational_detail::ColumnarChunk ch0;
    ch0.InitFromSchema(schema);
    ch0.AppendRow(Row({Value(int64_t{10}), Value("ten")}));
    ch0.AppendRow(Row({Value(int64_t{20}), Value("twenty")}));
    base_tbl->AppendChunk(std::move(ch0));
  }
  {
    relational_detail::ColumnarChunk ch1;
    ch1.InitFromSchema(schema);
    ch1.AppendRow(Row({Value(), Value("null_id")}));
    ch1.AppendRow(Row({Value(int64_t{30}), Value("thirty")}));
    base_tbl->AppendChunk(std::move(ch1));
  }

  auto hop1_idx = std::make_shared<relational_detail::ChunkGatherIndex>();
  hop1_idx->source_table = base_tbl;
  hop1_idx->src_chunks = {1, 0, 1};
  hop1_idx->src_rows = {1, 0, 0};
  hop1_idx->has_null = false;

  auto hop1_tbl = std::make_shared<relational_detail::ColumnarTable>();
  {
    relational_detail::ColumnarChunk h1_ch;
    h1_ch.rows = 3;
    h1_ch.columns.resize(2);
    h1_ch.columns[0].SetGathered(hop1_idx, 0, ValueType::kInt64, false, false,
                                 true, true);
    h1_ch.columns[1].SetGathered(hop1_idx, 1, ValueType::kVarChar, false, false,
                                 true, false);
    hop1_tbl->AppendChunk(std::move(h1_ch));
  }

  auto hop2_idx = std::make_shared<relational_detail::ChunkGatherIndex>();
  hop2_idx->source_table = hop1_tbl;
  hop2_idx->src_chunks = {0, 0, 0, 0};
  hop2_idx->src_rows = {1, 0, 2, 0};
  hop2_idx->src_chunks[1] = 0;
  hop2_idx->src_rows[1] = relational_detail::ChunkGatherIndex::kNullRow;
  hop2_idx->has_null = true;

  relational_detail::ColumnarColumn g2_int;
  g2_int.SetGathered(hop2_idx, 0, ValueType::kInt64, false, false, true, true);
  relational_detail::ColumnarColumn g2_str;
  g2_str.SetGathered(hop2_idx, 1, ValueType::kVarChar, false, false, true,
                     true);

  relational_detail::ColumnarColumn flat_int;
  relational_detail::ColumnarColumn flat_str;
  relational_detail::ChunkStringArena arena;
  g2_int.MaterializeFlatInto(4, &flat_int, &arena);
  g2_str.MaterializeFlatInto(4, &flat_str, &arena);

  EXPECT_EQ(flat_int.encoding,
            relational_detail::ColumnarColumn::Encoding::kFlat);
  EXPECT_TRUE(flat_int.has_null);
  EXPECT_FALSE(flat_int.IsNull(0));
  EXPECT_EQ(flat_int.IntAt(0), 10);
  EXPECT_TRUE(flat_int.IsNull(1));
  EXPECT_TRUE(flat_int.IsNull(2));
  EXPECT_FALSE(flat_int.IsNull(3));
  EXPECT_EQ(flat_int.IntAt(3), 30);

  EXPECT_EQ(flat_str.encoding,
            relational_detail::ColumnarColumn::Encoding::kFlat);
  EXPECT_TRUE(flat_str.has_null);
  EXPECT_FALSE(flat_str.IsNull(0));
  EXPECT_EQ(flat_str.StringAt(0), "ten");
  EXPECT_TRUE(flat_str.IsNull(1));
  EXPECT_FALSE(flat_str.IsNull(2));
  EXPECT_EQ(flat_str.StringAt(2), "null_id");
  EXPECT_FALSE(flat_str.IsNull(3));
  EXPECT_EQ(flat_str.StringAt(3), "thirty");
}

TEST(PlanningHeuristicsTest, HybridHashJoinResidentCapHonorsUnlimitedBudget) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  const size_t prev_limit = budget.Limit();
  budget.ResetForTest(0, /*isolate_from_process=*/true);
  ASSERT_TRUE(budget.Unlimited());

  // Under Unlimited() the hybrid partition count is the fixed kDefault
  // (32), so craft integer keys that all land in the resident partition 0:
  // that routes every build row through the resident_cap gate, letting the
  // probe predicate observe how much the resident charge was allowed to
  // grow. A buggy cap pins the charge at the 8 MiB floor.
  const size_t partitions = HybridPartitionCount(size_t{1} << 30);
  std::vector<int64_t> p0_keys;
  for (int64_t k = 0; p0_keys.size() < 10024 && k < (int64_t{1} << 30); ++k) {
    if (SpillPartitionOf(k, partitions) == 0) {
      p0_keys.push_back(k);
    }
  }
  ASSERT_EQ(p0_keys.size(), 10024U);

  Relation right;
  right.schema =
      Schema("r", {Column(ColumnName("r", "k"), ValueType::kInt64),
                   Column(ColumnName("r", "v"), ValueType::kVarChar)});
  const std::string payload(1024, 'x');
  for (int64_t k : p0_keys) {
    right.rows.push_back(Row({Value(k), Value(std::string(payload))}));
  }
  // ~11 MiB of partition-0 build rows: over the buggy 8 MiB resident cap,
  // under any unlimited-budget reading.

  Relation left;
  left.schema =
      Schema("l", {Column(ColumnName("l", "k"), ValueType::kInt64),
                   Column(ColumnName("l", "v"), ValueType::kVarChar)});
  for (size_t i = 0; i < 4; ++i) {
    left.rows.push_back(Row({Value(p0_keys[i]), Value("probe")}));
  }

  size_t comparisons = 0;
  size_t peak_used = 0;
  StatusOr<Relation> joined = HybridHashJoin(
      std::move(left), std::move(right), {0}, {0},
      [&](const Row&) {
        // Invoked while resident_charge is still held (it is released only
        // after the resident probe completes), so Used() reflects the
        // resident build bytes the cap allowed.
        peak_used = std::max(peak_used, budget.Used());
        return true;
      },
      /*left_join=*/false, &comparisons, /*null_safe=*/false,
      /*right_join=*/false);

  budget.ResetForTest(prev_limit);

  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  EXPECT_EQ(comparisons, 4U);
  EXPECT_EQ(joined.Value().rows.size(), 4U);
  // With the 8 MiB floor applied to Unlimited() this peaks at ~8 MiB; with
  // the cap removed it must pass the whole ~11 MiB resident build.
  EXPECT_GT(peak_used, size_t{9} << 20);
}

}  // namespace
}  // namespace tinylamb
