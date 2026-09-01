#include <gtest/gtest.h>

#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "executor/aggregation.hpp"
#include "executor/as_of_join.hpp"
#include "executor/batch_nested_loop_join.hpp"
#include "executor/cardinality_probe.hpp"
#include "executor/chunked_scan.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/grouping_sets.hpp"
#include "executor/incremental_sort.hpp"
#include "executor/interval_join.hpp"
#include "executor/materialize.hpp"
#include "executor/merge.hpp"
#include "executor/minmax_index.hpp"
#include "executor/nested_loop_join.hpp"
#include "executor/numa_arena.hpp"
#include "executor/operator_memory.hpp"
#include "executor/parallel_hash_join.hpp"
#include "executor/parallel_merge_join.hpp"
#include "executor/partial_aggregate.hpp"
#include "executor/partial_sort.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/selection_vector.hpp"
#include "executor/simd_comparison.hpp"
#include "executor/skip_scan_distinct.hpp"
#include "executor/sort.hpp"
#include "executor/two_phase_distinct_agg.hpp"
#include "executor/values.hpp"
#include "executor/vectorized_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(MinMaxIndexExecutorTest, EmptyAndNullPrefixProduceScalarNull) {
  auto source = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value()}), Row({Value(int64_t{42})})});
  MinMaxIndexExecutor executor(std::move(source), 0);

  Row result;
  EXPECT_TRUE(executor.Next(&result, nullptr));
  ASSERT_EQ(result.values_.size(), 1U);
  EXPECT_EQ(result[0], Value(int64_t{42}));
  EXPECT_FALSE(executor.Next(&result, nullptr));

  MinMaxIndexExecutor empty(
      std::make_shared<ValuesExecutor>(std::vector<Row>{}), 0);
  EXPECT_TRUE(empty.Next(&result, nullptr));
  ASSERT_EQ(result.values_.size(), 1U);
  EXPECT_TRUE(result[0].IsNull());
  EXPECT_FALSE(empty.Next(&result, nullptr));
}

TEST(PartialSortTest, TopKWithoutSortingEntireRun) {
  const Schema schema("scores", {Column("player", ValueType::kVarChar),
                                 Column("score", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value("P1"), Value(int64_t{50})}),
      Row({Value("P2"), Value(int64_t{20})}),
      Row({Value("P3"), Value(int64_t{90})}),
      Row({Value("P4"), Value(int64_t{10})}),
      Row({Value("P5"), Value(int64_t{70})}),
      Row({Value("P6"), Value(int64_t{40})}),
  });

  std::vector<SortExecutor::Key> keys = {
      {ColumnValueExp("score"), true, std::nullopt}};

  PartialSortExecutor partial_sort(src, schema, keys, /*top_k=*/3, /*offset=*/0);
  EXPECT_FALSE(partial_sort.IsMaterialized());
  partial_sort.MaterializePipeline();
  EXPECT_TRUE(partial_sort.IsMaterialized());
  EXPECT_EQ(partial_sort.MaterializedRowCount(), 3);

  std::vector<Row> out_rows;
  Row row;
  RowPosition rp;
  while (partial_sort.Next(&row, &rp)) {
    out_rows.push_back(std::move(row));
  }

  ASSERT_EQ(out_rows.size(), 3);
  EXPECT_EQ(out_rows[0][1], Value(int64_t{10}));
  EXPECT_EQ(out_rows[1][1], Value(int64_t{20}));
  EXPECT_EQ(out_rows[2][1], Value(int64_t{40}));
}

TEST(PartialSortTest, NullsFirstDefaultMatchesSortExecutor) {
  // PRODUCTION BUG (fixed): PartialSort/PdqSort defaulted ASC to NULLS LAST
  // while SortExecutor/TopN used NULLS FIRST for ascending keys, so the same
  // ORDER BY produced different row order depending on the plan shape.
  const Schema schema("vals", {Column("v", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{2})}),
      Row({Value()}),
      Row({Value(int64_t{1})}),
  });

  std::vector<SortExecutor::Key> keys = {
      {ColumnValueExp("v"), true, std::nullopt}};

  // SortExecutor (reference).
  SortExecutor full_sort(src, schema, keys);
  full_sort.MaterializePipeline();
  std::vector<Value> reference;
  Row row;
  RowPosition rp;
  while (full_sort.Next(&row, &rp)) {
    reference.push_back(row[0]);
  }
  ASSERT_EQ(reference.size(), 3U);
  EXPECT_TRUE(reference[0].IsNull());

  // PartialSortExecutor with top_k large enough to see every row.
  auto src2 = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{2})}),
      Row({Value()}),
      Row({Value(int64_t{1})}),
  });
  PartialSortExecutor partial_sort(src2, schema, keys, /*top_k=*/3, 0);
  partial_sort.MaterializePipeline();
  std::vector<Value> partial;
  while (partial_sort.Next(&row, &rp)) {
    partial.push_back(row[0]);
  }
  ASSERT_EQ(partial.size(), 3U);
  EXPECT_TRUE(partial[0].IsNull());
  EXPECT_EQ(partial[1], Value(int64_t{1}));
  EXPECT_EQ(partial[2], Value(int64_t{2}));
  EXPECT_TRUE(reference[0].IsNull() == partial[0].IsNull());
  EXPECT_EQ(reference[1], partial[1]);
  EXPECT_EQ(reference[2], partial[2]);
}

TEST(PartialSortTest, PartitionedBlockSorting) {
  const Schema schema("numbers", {Column("val", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{30})}),
      Row({Value(int64_t{10})}),
      Row({Value(int64_t{20})}),
      Row({Value(int64_t{90})}),
      Row({Value(int64_t{80})}),
      Row({Value(int64_t{70})}),
  });

  std::vector<SortExecutor::Key> keys = {
      {ColumnValueExp("val"), true, std::nullopt}};

  PartialSortExecutor partial_sort(src, schema, keys, /*top_k=*/0, /*offset=*/0,
                                  /*block_size=*/3);

  DataChunk destination(schema, 10);
  size_t count = partial_sort.NextBatch(&destination);
  ASSERT_EQ(count, 6);
  EXPECT_EQ(destination.ColumnAt(0).ValueAt(0), Value(int64_t{10}));
  EXPECT_EQ(destination.ColumnAt(0).ValueAt(1), Value(int64_t{20}));
  EXPECT_EQ(destination.ColumnAt(0).ValueAt(2), Value(int64_t{30}));
  EXPECT_EQ(destination.ColumnAt(0).ValueAt(3), Value(int64_t{70}));
  EXPECT_EQ(destination.ColumnAt(0).ValueAt(4), Value(int64_t{80}));
  EXPECT_EQ(destination.ColumnAt(0).ValueAt(5), Value(int64_t{90}));
}

TEST(PercentileContTest, OrderedSetAggregationLinearInterpolation) {
  const Schema schema("scores", {Column("val", ValueType::kDouble)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(10.0)}),
      Row({Value(20.0)}),
      Row({Value(30.0)}),
      Row({Value(40.0)}),
  });

  auto agg_exp = std::make_shared<AggregateExpression>(
      AggregationType::kPercentileCont, ConstantValueExp(Value(0.5)));
  agg_exp->SetInnerOrderBy({WindowOrderTerm(ColumnValueExp("val"), true)});

  std::vector<NamedExpression> aggs = {NamedExpression("median", agg_exp)};
  AggregationExecutor agg_exec(src, schema, aggs);

  Row result;
  RowPosition rp;
  ASSERT_TRUE(agg_exec.Next(&result, &rp));
  EXPECT_DOUBLE_EQ(result[0].value.double_value, 25.0);

  auto agg_25 = std::make_shared<AggregateExpression>(
      AggregationType::kPercentileCont, ConstantValueExp(Value(0.25)));
  agg_25->SetInnerOrderBy({WindowOrderTerm(ColumnValueExp("val"), true)});

  auto src2 = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(10.0)}),
      Row({Value(20.0)}),
      Row({Value(30.0)}),
      Row({Value(40.0)}),
  });
  AggregationExecutor agg_exec2(src2, schema, {NamedExpression("p25", agg_25)});
  ASSERT_TRUE(agg_exec2.Next(&result, &rp));
  EXPECT_DOUBLE_EQ(result[0].value.double_value, 17.5);
}

TEST(VectorizedAggregationExecutorTest, BitwiseAndLogicalAggregation) {
  const Schema schema("data", {Column("a", ValueType::kInt64),
                               Column("b", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{0b1100}), Value(int64_t{1})}),
      Row({Value(int64_t{0b1010}), Value(int64_t{1})}),
      Row({Value(int64_t{0b1001}), Value(int64_t{0})}),
  });

  auto bit_and = std::make_shared<AggregateExpression>(
      AggregationType::kBitAnd, ColumnValueExp("a"));
  auto bit_or = std::make_shared<AggregateExpression>(
      AggregationType::kBitOr, ColumnValueExp("a"));
  auto bit_xor = std::make_shared<AggregateExpression>(
      AggregationType::kBitXor, ColumnValueExp("a"));
  auto log_and = std::make_shared<AggregateExpression>(
      AggregationType::kLogicalAnd, ColumnValueExp("b"));
  auto log_or = std::make_shared<AggregateExpression>(
      AggregationType::kLogicalOr, ColumnValueExp("b"));

  AggregationExecutor agg_exec(
      src, schema,
      {NamedExpression("bit_and", bit_and), NamedExpression("bit_or", bit_or),
       NamedExpression("bit_xor", bit_xor), NamedExpression("log_and", log_and),
       NamedExpression("log_or", log_or)});

  Row result;
  RowPosition rp;
  ASSERT_TRUE(agg_exec.Next(&result, &rp));
  EXPECT_EQ(result[0], Value(int64_t{0b1000}));
  EXPECT_EQ(result[1], Value(int64_t{0b1111}));
  EXPECT_EQ(result[2], Value(int64_t{0b1111}));
  EXPECT_EQ(result[3], Value(int64_t{0}));
  EXPECT_EQ(result[4], Value(int64_t{1}));
}

TEST(TwoPhaseDistinctAggTest, BasicDistinctAggregation) {
  const Schema schema("data", {Column("dept", ValueType::kVarChar),
                               Column("salary", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value("Eng"), Value(int64_t{100})}),
      Row({Value("Eng"), Value(int64_t{100})}),
      Row({Value("Eng"), Value(int64_t{200})}),
      Row({Value("Sales"), Value(int64_t{50})}),
  });

  auto agg_count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("salary"), true);

  TwoPhaseDistinctAggExecutor distinct_agg(
      src, schema, {ColumnValueExp("dept")}, {NamedExpression("cnt", agg_count)});

  std::vector<Row> out_rows;
  Row row;
  RowPosition rp;
  while (distinct_agg.Next(&row, &rp)) {
    out_rows.push_back(std::move(row));
  }

  ASSERT_EQ(out_rows.size(), 2);
  EXPECT_EQ(out_rows[0][0], Value("Eng"));
  EXPECT_EQ(out_rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(out_rows[1][0], Value("Sales"));
  EXPECT_EQ(out_rows[1][1], Value(int64_t{1}));
}

TEST(SharedBuildParallelHashJoinTest, BasicExecution) {
  std::vector<Row> left_rows = {
      Row({Value(1), Value("a"), Value(10.0)}),
      Row({Value(2), Value("b"), Value(20.0)}),
      Row({Value(3), Value("c"), Value(30.0)}),
      Row({Value(4), Value("d"), Value(40.0)}),
  };
  std::vector<Row> right_rows = {
      Row({Value(1), Value("x"), Value(100.0)}),
      Row({Value(2), Value("y"), Value(200.0)}),
      Row({Value(3), Value("z"), Value(300.0)}),
      Row({Value(4), Value("w"), Value(400.0)}),
  };

  Executor left1 = std::make_shared<ValuesExecutor>(left_rows);
  Executor right1 = std::make_shared<ValuesExecutor>(right_rows);
  SharedBuildParallelHashJoin join_inner(left1, {0}, right1, {0}, 4,
                                        JoinKind::kInner);
  size_t inner_count = 0;
  Row row;
  RowPosition rp;
  while (join_inner.Next(&row, &rp)) {
    ++inner_count;
  }
  EXPECT_EQ(inner_count, 4);

  Executor left2 = std::make_shared<ValuesExecutor>(left_rows);
  Executor right2 = std::make_shared<ValuesExecutor>(right_rows);
  SharedBuildParallelHashJoin join_semi(left2, {0}, right2, {0}, 4,
                                        JoinKind::kSemi);
  size_t semi_count = 0;
  while (join_semi.Next(&row, &rp)) {
    ++semi_count;
  }
  EXPECT_EQ(semi_count, 4);

  Executor left3 = std::make_shared<ValuesExecutor>(left_rows);
  Executor right3 = std::make_shared<ValuesExecutor>(right_rows);
  SharedBuildParallelHashJoin join_anti(left3, {0}, right3, {0}, 4,
                                        JoinKind::kAnti);
  size_t anti_count = 0;
  while (join_anti.Next(&row, &rp)) {
    ++anti_count;
  }
  EXPECT_EQ(anti_count, 0);
}

}  // namespace tinylamb
