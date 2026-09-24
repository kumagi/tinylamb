/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for executor paths the regression suites miss:
//   - TopNExecutor: the with-ties sort path (boundary ties, NULLS FIRST/LAST,
//     NaN ordering, descending keys, overflow-capacity guard, key evaluation
//     failures), the limit == 0 fast path, and Dump.
//   - MaterializeExecutor: the full cache/replay lifecycle (Next, NextBatch,
//     Rewind, MaterializePipeline, accounting, Dump/Explain) over empty and
//     non-empty children.
//   - TwoPhaseDistinctAgg: date/array value types in distinct sets and output
//     schemas, non-int64/double finalize branches, NextBatch, and the
//     evaluation-failure paths for group keys and aggregate children.

#include <gtest/gtest.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/materialize.hpp"
#include "executor/topn.hpp"
#include "executor/two_phase_distinct_agg.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

std::vector<Value> ReadAll(ExecutorBase* exec) {
  std::vector<Value> out;
  Row row;
  while (exec->Next(&row, nullptr)) {
    out.push_back(row[0]);
    row = Row();
  }
  return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// TopNExecutor
// ---------------------------------------------------------------------------

TEST(TopNCoverageTest, LimitZeroFastPathReturnsNothing) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
      Row({Value(int64_t{2})}),
  });
  TopNExecutor topn(
      src, schema,
      {TopNExecutor::Key{.expression = ColumnValueExp("x"), .ascending = true}},
      /*limit=*/0, /*offset=*/0);
  EXPECT_TRUE(ReadAll(&topn).empty());
}

TEST(TopNCoverageTest, WithTiesExtendsBoundaryTies) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  // Boundary at LIMIT 3 is x = 30, which appears three times: all ties must
  // be emitted even though the heap would stop at three rows.
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{10})}),
      Row({Value(int64_t{30})}),
      Row({Value(int64_t{20})}),
      Row({Value(int64_t{30})}),
      Row({Value(int64_t{30})}),
      Row({Value(int64_t{40})}),
  });
  TopNExecutor topn(
      src, schema,
      {TopNExecutor::Key{.expression = ColumnValueExp("x"), .ascending = true}},
      /*limit=*/3, /*offset=*/0, /*with_ties=*/true);
  const auto rows = ReadAll(&topn);
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0], Value(int64_t{10}));
  EXPECT_EQ(rows[4], Value(int64_t{30}));
}

TEST(TopNCoverageTest, WithTiesHonorsOffsetAndOverflowCapacity) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
      Row({Value(int64_t{2})}),
      Row({Value(int64_t{2})}),
  });
  TopNExecutor topn(
      src, schema,
      {TopNExecutor::Key{.expression = ColumnValueExp("x"), .ascending = true}},
      /*limit=*/1, /*offset=*/1, /*with_ties=*/true);
  const auto rows = ReadAll(&topn);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Value(int64_t{2}));
  EXPECT_EQ(rows[1], Value(int64_t{2}));

  // offset + limit overflows size_t: capacity saturates and nothing is
  // emitted because every row is filtered by the offset.
  TopNExecutor saturated(
      src, schema,
      {TopNExecutor::Key{.expression = ColumnValueExp("x"), .ascending = true}},
      /*limit=*/1,
      /*offset=*/std::numeric_limits<size_t>::max(),
      /*with_ties=*/false);
  EXPECT_TRUE(ReadAll(&saturated).empty());
}

TEST(TopNCoverageTest, WithTiesNullOrderingBothDirections) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto make_src = []() {
    return std::make_shared<ValuesExecutor>(std::vector<Row>{
        Row({Value()}),
        Row({Value(int64_t{5})}),
        Row({Value(int64_t{5})}),
        Row({Value(int64_t{7})}),
        Row({Value()}),
    });
  };
  // NULLS FIRST: the two NULL rows sort before 5; LIMIT 4 puts the boundary
  // on the first 5 and the WITH TIES extension carries the second one.
  TopNExecutor nulls_first(make_src(), schema,
                           {TopNExecutor::Key{.expression = ColumnValueExp("x"),
                                              .ascending = true,
                                              .nulls_first = true}},
                           /*limit=*/4, /*offset=*/0, /*with_ties=*/true);
  const auto rows = ReadAll(&nulls_first);
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_TRUE(rows[0].IsNull());
  EXPECT_TRUE(rows[1].IsNull());
  EXPECT_EQ(rows[2], Value(int64_t{5}));
  EXPECT_EQ(rows[3], Value(int64_t{5}));

  // NULLS LAST, descending: 7, then the 5-tie pair, NULLs dropped by limit.
  TopNExecutor nulls_last(make_src(), schema,
                          {TopNExecutor::Key{.expression = ColumnValueExp("x"),
                                             .ascending = false,
                                             .nulls_first = false}},
                          /*limit=*/2, /*offset=*/0, /*with_ties=*/true);
  const auto desc = ReadAll(&nulls_last);
  ASSERT_EQ(desc.size(), 3U);
  EXPECT_EQ(desc[0], Value(int64_t{7}));
  EXPECT_EQ(desc[1], Value(int64_t{5}));
  EXPECT_EQ(desc[2], Value(int64_t{5}));
}

TEST(TopNCoverageTest, WithTiesNaNAndDescendingDoubleKeys) {
  const Schema schema("t", {Column("x", ValueType::kDouble)});
  const double nan = std::numeric_limits<double>::quiet_NaN();
  auto make_src = [nan]() {
    return std::make_shared<ValuesExecutor>(std::vector<Row>{
        Row({Value(1.0)}),
        Row({Value(nan)}),
        Row({Value(2.0)}),
        Row({Value(2.0)}),
    });
  };
  // Ascending keeps NaN just above NULL (CompareForOrderBy: NULL < NaN <
  // everything). LIMIT 3 puts the boundary on the first 2.0 and the
  // extension carries the duplicate.
  TopNExecutor asc(
      make_src(), schema,
      {TopNExecutor::Key{.expression = ColumnValueExp("x"), .ascending = true}},
      /*limit=*/3, /*offset=*/0, /*with_ties=*/true);
  const auto rows = ReadAll(&asc);
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_TRUE(std::isnan(rows[0].value.double_value));
  EXPECT_EQ(rows[1], Value(1.0));
  EXPECT_EQ(rows[2], Value(2.0));
  EXPECT_EQ(rows[3], Value(2.0));

  // Descending is the exact reverse: [2.0, 2.0, 1.0, NaN]. LIMIT 1 puts the
  // boundary on the first 2.0 and the extension carries its tie.
  TopNExecutor desc(make_src(), schema,
                    {TopNExecutor::Key{.expression = ColumnValueExp("x"),
                                       .ascending = false}},
                    /*limit=*/1, /*offset=*/0, /*with_ties=*/true);
  const auto desc_rows = ReadAll(&desc);
  ASSERT_EQ(desc_rows.size(), 2U);
  EXPECT_EQ(desc_rows[0], Value(2.0));
  EXPECT_EQ(desc_rows[1], Value(2.0));
}

TEST(TopNCoverageTest, KeyEvaluationFailurePropagates) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
      Row({Value(int64_t{2})}),
  });
  // The key column does not exist in the schema: TryEvaluate fails on the
  // first candidate in both the with-ties and heap paths.
  TopNExecutor ties(src, schema,
                    {TopNExecutor::Key{.expression = ColumnValueExp("nope"),
                                       .ascending = true}},
                    /*limit=*/1, /*offset=*/0, /*with_ties=*/true);
  EXPECT_TRUE(ReadAll(&ties).empty());
  EXPECT_NE(ties.GetStatus(), Status::kSuccess);

  TopNExecutor heap(src, schema,
                    {TopNExecutor::Key{.expression = ColumnValueExp("nope"),
                                       .ascending = true}},
                    /*limit=*/1, /*offset=*/0);
  EXPECT_TRUE(ReadAll(&heap).empty());
  EXPECT_NE(heap.GetStatus(), Status::kSuccess);
}

TEST(TopNCoverageTest, DumpRendersConfiguration) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
  });
  TopNExecutor topn(src, schema,
                    {TopNExecutor::Key{.expression = ColumnValueExp("x"),
                                       .ascending = false}},
                    /*limit=*/4, /*offset=*/2, /*with_ties=*/true);
  std::ostringstream oss;
  topn.Dump(oss, 0);
  const std::string dumped = oss.str();
  EXPECT_NE(dumped.find("TopN"), std::string::npos);
  EXPECT_NE(dumped.find("limit=4"), std::string::npos);
  EXPECT_NE(dumped.find("offset=2"), std::string::npos);
  EXPECT_NE(dumped.find("with ties"), std::string::npos);
}

// ---------------------------------------------------------------------------
// MaterializeExecutor
// ---------------------------------------------------------------------------

TEST(MaterializeExecutorTest, CachesRowsAndSupportsRewind) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
      Row({Value(int64_t{2})}),
      Row({Value(int64_t{3})}),
  });
  MaterializeExecutor materialize(src, schema);
  EXPECT_FALSE(materialize.IsMaterialized());
  EXPECT_EQ(materialize.MaterializedRowCount(), 0U);

  const auto first_pass = ReadAll(&materialize);
  ASSERT_EQ(first_pass.size(), 3U);
  EXPECT_EQ(first_pass[0], Value(int64_t{1}));
  EXPECT_EQ(first_pass[2], Value(int64_t{3}));
  EXPECT_TRUE(materialize.IsMaterialized());
  EXPECT_EQ(materialize.MaterializedRowCount(), 3U);
  EXPECT_GT(materialize.MaterializedBytes(), 0U);

  // The child stays readable: a rewind replays the cached rows.
  materialize.Rewind();
  const auto second_pass = ReadAll(&materialize);
  EXPECT_EQ(second_pass.size(), first_pass.size());
}

TEST(MaterializeExecutorTest, MaterializePipelinePreloadsWithoutDraining) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{9})}),
      Row({Value(int64_t{8})}),
  });
  MaterializeExecutor materialize(src, schema);
  materialize.MaterializePipeline();
  EXPECT_TRUE(materialize.IsMaterialized());
  EXPECT_EQ(materialize.MaterializedRowCount(), 2U);
}

TEST(MaterializeExecutorTest, NextBatchStreamsChunks) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
      Row({Value(int64_t{2})}),
      Row({Value(int64_t{3})}),
  });
  MaterializeExecutor materialize(src, schema);

  DataChunk chunk;
  ASSERT_EQ(materialize.NextBatch(&chunk, /*max_rows=*/2), 2U);
  EXPECT_EQ(chunk.Size(), 2U);
  EXPECT_EQ(chunk.RowAt(0)[0], Value(int64_t{1}));
  EXPECT_EQ(materialize.NextBatch(&chunk, /*max_rows=*/2), 1U);
  EXPECT_EQ(materialize.NextBatch(&chunk, /*max_rows=*/2), 0U);
  EXPECT_EQ(materialize.NextBatch(&chunk, /*max_rows=*/0), 0U);
  EXPECT_EQ(materialize.NextBatch(nullptr, /*max_rows=*/4), 0U);
}

TEST(MaterializeExecutorTest, EmptyChildYieldsNothing) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{});
  MaterializeExecutor materialize(src, schema);
  EXPECT_TRUE(ReadAll(&materialize).empty());
  EXPECT_TRUE(materialize.IsMaterialized());
  EXPECT_EQ(materialize.MaterializedRowCount(), 0U);

  DataChunk chunk;
  EXPECT_EQ(materialize.NextBatch(&chunk, 8), 0U);
}

TEST(MaterializeExecutorTest, DumpAndExplainRenderRowStats) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
  });
  MaterializeExecutor materialize(src, schema);
  std::ostringstream dumped;
  materialize.Dump(dumped, 0);
  EXPECT_NE(dumped.str().find("MaterializeExecutor(rows="), std::string::npos);

  std::ostringstream explained;
  materialize.Explain(explained, 0);
  EXPECT_NE(explained.str().find("materialized=false"), std::string::npos);
}

// ---------------------------------------------------------------------------
// TwoPhaseDistinctAgg
// ---------------------------------------------------------------------------

TEST(TwoPhaseDistinctAggCoverageTest, DateAndArrayDistinctSets) {
  const Schema schema(
      "d", {Column("dt", ValueType::kDate), Column("arr", ValueType::kArray)});
  const Value d1 = Value::Date("2026-01-01");
  const Value d2 = Value::Date("2026-02-01");
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({d1, Value::Array({Value(int64_t{1})}, "INT64")}),
      Row({d1, Value::Array({Value(int64_t{1})}, "INT64")}),
      Row({d2, Value::Array({Value(int64_t{2})}, "INT64")}),
      Row({Value(), Value::Array({Value(int64_t{3})}, "INT64")}),
  });

  auto dcnt_date = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("dt"), true);
  auto dcnt_arr = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("arr"), true);
  auto dmin_date = std::make_shared<AggregateExpression>(
      AggregationType::kMin, ColumnValueExp("dt"), true);
  auto dmax_date = std::make_shared<AggregateExpression>(
      AggregationType::kMax, ColumnValueExp("dt"), true);
  auto davg_date = std::make_shared<AggregateExpression>(
      AggregationType::kAvg, ColumnValueExp("dt"), true);

  TwoPhaseDistinctAggExecutor agg(
      src, schema,
      {NamedExpression("dc_dt", dcnt_date), NamedExpression("dc_arr", dcnt_arr),
       NamedExpression("min_dt", dmin_date),
       NamedExpression("max_dt", dmax_date),
       NamedExpression("avg_dt", davg_date)});

  // The output schema maps DATE keys to kDate and falls back for arrays.
  Row row;
  ASSERT_TRUE(agg.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(int64_t{2}));  // two distinct dates
  EXPECT_EQ(row[1], Value(int64_t{3}));  // three distinct arrays
  EXPECT_EQ(row[2], d1);
  EXPECT_EQ(row[3], d2);
  EXPECT_EQ(row[4], Value(0.0));  // AVG over dates has no numeric element

  DataChunk chunk;
  ASSERT_TRUE(agg.NextBatch(&chunk, 4) == 0);
}

TEST(TwoPhaseDistinctAggCoverageTest, DateGroupKeyTypesTheOutputColumn) {
  const Schema schema(
      "d", {Column("dt", ValueType::kDate), Column("v", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value::Date("2026-03-01"), Value(int64_t{5})}),
      Row({Value::Date("2026-03-01"), Value(int64_t{7})}),
  });
  auto dsum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                    ColumnValueExp("v"), true);
  TwoPhaseDistinctAggExecutor agg(src, schema,
                                  {NamedExpression("dt", ColumnValueExp("dt"))},
                                  {NamedExpression("s", dsum)});
  EXPECT_EQ(agg.OutputSchema().GetColumn(0).Type(), ValueType::kDate);

  Row row;
  ASSERT_TRUE(agg.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value::Date("2026-03-01"));
  EXPECT_EQ(row[1], Value(12.0));
  EXPECT_FALSE(agg.Next(&row, nullptr));
}

TEST(TwoPhaseDistinctAggCoverageTest, NextBatchEmitsGroupedRowsInChunks) {
  const Schema schema(
      "d", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1}), Value(int64_t{1})}),
      Row({Value(int64_t{1}), Value(int64_t{2})}),
      Row({Value(int64_t{2}), Value(int64_t{4})}),
  });
  auto dcnt = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                    ColumnValueExp("v"), true);
  TwoPhaseDistinctAggExecutor agg(src, schema,
                                  {NamedExpression("k", ColumnValueExp("k"))},
                                  {NamedExpression("c", dcnt)});

  DataChunk chunk;
  ASSERT_EQ(agg.NextBatch(&chunk, /*max_rows=*/1), 1U);
  EXPECT_EQ(chunk.Size(), 1U);
  const size_t remaining = agg.NextBatch(&chunk, /*max_rows=*/8);
  ASSERT_EQ(remaining, 1U);
  EXPECT_EQ(agg.NextBatch(&chunk, /*max_rows=*/8), 0U);
  EXPECT_EQ(agg.NextBatch(&chunk, /*max_rows=*/0), 0U);
}

TEST(TwoPhaseDistinctAggCoverageTest, EvaluationFailuresPropagates) {
  const Schema schema("d", {Column("v", ValueType::kInt64)});
  auto make_src = []() {
    return std::make_shared<ValuesExecutor>(
        std::vector<Row>{Row({Value(int64_t{1})})});
  };

  // Group key referencing a missing column fails during phase 1.
  auto dcnt = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                    ColumnValueExp("v"), true);
  TwoPhaseDistinctAggExecutor bad_key(
      make_src(), schema, {NamedExpression("k", ColumnValueExp("missing"))},
      {NamedExpression("c", dcnt)});
  Row row;
  EXPECT_FALSE(bad_key.Next(&row, nullptr));
  EXPECT_NE(bad_key.GetStatus(), Status::kSuccess);

  // A distinct aggregate over a missing column fails while collecting.
  auto bad_distinct = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("missing"), true);
  TwoPhaseDistinctAggExecutor bad_agg(make_src(), schema,
                                      {NamedExpression("s", bad_distinct)});
  EXPECT_FALSE(bad_agg.Next(&row, nullptr));
  EXPECT_NE(bad_agg.GetStatus(), Status::kSuccess);

  // A plain aggregate over a missing column fails in the non-distinct path.
  auto bad_plain = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("missing"));
  TwoPhaseDistinctAggExecutor bad_plain_agg(make_src(), schema,
                                            {NamedExpression("s", bad_plain)});
  EXPECT_FALSE(bad_plain_agg.Next(&row, nullptr));
  EXPECT_NE(bad_plain_agg.GetStatus(), Status::kSuccess);
}

}  // namespace tinylamb
