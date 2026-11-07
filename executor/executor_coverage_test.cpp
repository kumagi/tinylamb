/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for executor paths the regression suites miss:
//   - TopNExecutor: the with-ties sort path (boundary ties, NULLS FIRST/LAST,
//     NaN ordering, descending keys, overflow-capacity guard, key evaluation
//     failures), the limit == 0 fast path, and Dump.
//   - MaterializeExecutor: the full cache/replay lifecycle (Next, NextBatch,
//     Rewind, MaterializePipeline, accounting, Dump/Explain) over empty and
//     non-empty children.

#include <gtest/gtest.h>

#include <algorithm>
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
#include "executor/incremental_sort.hpp"
#include "executor/limit.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
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

TEST(TopNCoverageTest, BatchedHeapKeepsStableNullOrderPayloadAndPositions) {
  class Source final : public ExecutorBase {
   public:
    std::vector<Row> rows;
    size_t offset{0};
    size_t batches{0};
    size_t NextBatch(DataChunk* chunk, size_t limit) override {
      ++batches;
      chunk->Reset();
      while (offset < rows.size() && chunk->Size() < limit) {
        chunk->Append(rows[offset],
                      RowPosition(7, static_cast<slot_t>(offset)));
        ++offset;
      }
      return chunk->Size();
    }
    void Dump(std::ostream&, int) const override {}
  };
  auto source = std::make_shared<Source>();
  for (int i = 0; i < 3000; ++i) {
    source->rows.push_back(Row({i % 13 == 0 ? Value() : Value(i % 7), Value(i),
                                Value(std::string(64, 'x'))}));
  }
  auto expected = source->rows;
  std::stable_sort(expected.begin(), expected.end(),
                   [](const Row& a, const Row& b) {
                     if (a[0].IsNull() != b[0].IsNull()) {
                       return !a[0].IsNull();
                     }
                     return !a[0].IsNull() && a[0] > b[0];
                   });
  Schema schema(
      "t", {Column("x", ValueType::kInt64), Column("id", ValueType::kInt64),
            Column("payload", ValueType::kVarChar)});
  TopNExecutor top(source, schema,
                   {{.expression = ColumnValueExp("x"),
                     .ascending = false,
                     .nulls_first = false}},
                   25, 17);
  Row row;
  RowPosition position;
  size_t count = 0;
  while (top.Next(&row, &position)) {
    ASSERT_LT(count, 25U);
    EXPECT_EQ(row, expected[count + 17]);
    EXPECT_EQ(position,
              RowPosition(7, static_cast<slot_t>(
                                 expected[count + 17][1].value.int_value)));
    ++count;
  }
  EXPECT_EQ(count, 25U);
  EXPECT_GE(source->batches, 3U);
  EXPECT_EQ(top.GetStatus(), Status::kSuccess);
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

  auto src2 = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{1})}),
      Row({Value(int64_t{2})}),
  });
  TopNExecutor heap(src2, schema,
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

}  // namespace tinylamb

namespace tinylamb {
TEST(IncrementalSortCoverageTest, LimitConsumesOnlyFirstPeerGroupAndLookahead) {
  class Source final : public ExecutorBase {
   public:
    size_t consumed{0};
    size_t NextBatch(DataChunk* chunk, size_t max_rows) override {
      chunk->Reset();
      while (consumed < 10000 && chunk->Size() < max_rows) {
        const auto group = static_cast<int64_t>(99 - consumed / 100);
        const auto suffix = static_cast<int64_t>(99 - consumed % 100);
        chunk->Append(Row{Value(group), Value(suffix)},
                      RowPosition(1, static_cast<slot_t>(consumed)));
        ++consumed;
      }
      return chunk->Size();
    }
    void Dump(std::ostream& out, int) const override {
      out << "CountingSource";
    }
  };
  const Schema schema("t", {Column("group", ValueType::kInt64),
                            Column("suffix", ValueType::kInt64)});
  auto source = std::make_shared<Source>();
  auto sort = std::make_shared<IncrementalSortExecutor>(
      source, schema,
      std::vector<SortExecutor::Key>{{ColumnValueExp("group"), false}},
      std::vector<SortExecutor::Key>{{ColumnValueExp("suffix"), true}});
  LimitExecutor limit(sort, 100, 0);
  DataChunk chunk;
  ASSERT_EQ(limit.NextBatch(&chunk, 100), 100);
  for (size_t i = 0; i < 100; ++i) {
    EXPECT_EQ(chunk.RowAt(i), Row({Value(99), Value(static_cast<int64_t>(i))}));
    EXPECT_EQ(chunk.PositionAt(i), RowPosition(1, static_cast<slot_t>(99 - i)));
  }
  EXPECT_EQ(source->consumed, 101);
  EXPECT_EQ(limit.NextBatch(&chunk, 100), 0);
  EXPECT_EQ(source->consumed, 101);
  EXPECT_EQ(limit.GetStatus(), Status::kSuccess);
}
TEST(IncrementalSortCoverageTest, SingletonPeersKeepPositionsAndLookahead) {
  class Source final : public ExecutorBase {
   public:
    size_t consumed{0};
    size_t NextBatch(DataChunk* chunk, size_t max_rows) override {
      chunk->Reset();
      while (consumed < 1000 && chunk->Size() < max_rows) {
        chunk->Append(Row({Value(static_cast<int64_t>(consumed)), Value()}),
                      RowPosition(7, static_cast<slot_t>(consumed)));
        ++consumed;
      }
      return chunk->Size();
    }
    void Dump(std::ostream& out, int) const override { out << "Singletons"; }
  };
  const Schema schema(
      "t", {Column("g", ValueType::kInt64), Column("v", ValueType::kInt64)});
  auto source = std::make_shared<Source>();
  auto sort = std::make_shared<IncrementalSortExecutor>(
      source, schema,
      std::vector<SortExecutor::Key>{{ColumnValueExp("g"), true}},
      std::vector<SortExecutor::Key>{{ColumnValueExp("v"), false}});
  LimitExecutor limit(sort, 10, 0);
  Row row;
  RowPosition position;
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_TRUE(limit.Next(&row, &position));
    EXPECT_EQ(row, Row({Value(i), Value()}));
    EXPECT_EQ(position, RowPosition(7, static_cast<slot_t>(i)));
  }
  EXPECT_FALSE(limit.Next(&row, &position));
  EXPECT_EQ(source->consumed, 11U);
  EXPECT_EQ(limit.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  sort->Dump(dump, 0);
  EXPECT_NE(dump.str().find("singleton_runs=10"), std::string::npos);
}

TEST(IncrementalSortCoverageTest, SingletonDoesNotElideSuffixErrors) {
  const Schema schema("t", {Column("g", ValueType::kInt64)});
  auto source =
      std::make_shared<ValuesExecutor>(std::vector<Row>{Row({Value(1)})});
  IncrementalSortExecutor sort(source, schema, {{ColumnValueExp("g"), true}},
                               {{ColumnValueExp("missing"), true}});
  Row row;
  EXPECT_FALSE(sort.Next(&row, nullptr));
  EXPECT_NE(sort.GetStatus(), Status::kSuccess);
}

TEST(IncrementalSortCoverageTest, SpilledNullPeerAndExplicitMaterialization) {
  struct RestoreBudget {
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(16384);
  const Schema schema(
      "t", {Column("g", ValueType::kInt64), Column("v", ValueType::kInt64)});
  std::vector<Row> input;
  for (int64_t i = 2000; i >= 0; --i) input.push_back(Row({Value(), Value(i)}));
  input.push_back(Row({Value(1), Value(4)}));
  auto source = std::make_shared<ValuesExecutor>(std::move(input));
  IncrementalSortExecutor sort(
      source, schema,
      {SortExecutor::Key{.expression = ColumnValueExp("g"), .ascending = true}},
      {SortExecutor::Key{.expression = ColumnValueExp("v"),
                         .ascending = true}});
  Row row;
  ASSERT_TRUE(sort.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(), Value(0)}));
  sort.MaterializePipeline();
  EXPECT_TRUE(sort.IsMaterialized());
  EXPECT_EQ(sort.MaterializedRowCount(), 2001U);
  for (int64_t i = 1; i <= 2000; ++i) {
    ASSERT_TRUE(sort.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(), Value(i)}));
  }
  ASSERT_TRUE(sort.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1), Value(4)}));
  EXPECT_FALSE(sort.Next(&row, nullptr));
  EXPECT_EQ(sort.GetStatus(), Status::kSuccess);
}

TEST(LimitCoverageTest, BoundedDemandCrossesProjectionAndFilterWithOffset) {
  class CountingSource final : public ExecutorBase {
   public:
    size_t consumed{0};
    size_t NextBatch(DataChunk* chunk, size_t max_rows) override {
      chunk->Reset();
      while (consumed < 10000 && chunk->Size() < max_rows) {
        chunk->Append(Row({Value(static_cast<int64_t>(consumed)),
                           Value(static_cast<int64_t>(consumed % 100))}),
                      RowPosition(1, static_cast<slot_t>(consumed)));
        ++consumed;
      }
      return chunk->Size();
    }
    void Dump(std::ostream& out, int) const override {
      out << "CountingSource";
    }
  };
  auto source = std::make_shared<CountingSource>();
  const Schema schema("t", {Column("id", ValueType::kInt64),
                            Column("bucket", ValueType::kInt64)});
  auto filtered = std::make_shared<Selection>(
      BinaryExpressionExp(ColumnValueExp("bucket"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))),
      schema, source);
  auto projected = std::make_shared<Projection>(
      std::vector<NamedExpression>{NamedExpression("id")}, schema, filtered);
  LimitExecutor limit(projected, 3, 2);
  Row row;
  RowPosition position;
  ASSERT_TRUE(limit.Next(&row, &position));
  EXPECT_EQ(row, Row({Value(207)}));
  EXPECT_EQ(position, RowPosition(1, 207));
  DataChunk batch;
  ASSERT_EQ(limit.NextBatch(&batch, 1), 1U);
  EXPECT_EQ(batch.RowAt(0), Row({Value(307)}));
  ASSERT_TRUE(limit.Next(&row, &position));
  EXPECT_EQ(row, Row({Value(407)}));
  EXPECT_EQ(position, RowPosition(1, 407));
  EXPECT_FALSE(limit.Next(&row, &position));
  EXPECT_EQ(source->consumed, 408U);
  EXPECT_EQ(limit.GetStatus(), Status::kSuccess);
}

}  // namespace tinylamb
