/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for executor paths the regression suites miss:
//   - TopNExecutor: the with-ties sort path (boundary ties, NULLS FIRST/LAST,
//     NaN ordering, descending keys, overflow-capacity guard, key evaluation
//     failures), the limit == 0 fast path, and Dump.
//   - MaterializeExecutor: the full cache/replay lifecycle (Next, NextBatch,
//     Rewind, MaterializePipeline, accounting, Dump/Explain) over empty and
//     non-empty children.

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

}  // namespace tinylamb
