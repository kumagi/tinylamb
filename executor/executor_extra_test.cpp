#include <gtest/gtest.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "executor/aggregation.hpp"
#include "executor/batch_nested_loop_join.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/exchange.hpp"
#include "executor/grouping_sets.hpp"
#include "executor/minmax_index.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/parallel_hash_join.hpp"
#include "executor/partial_sort.hpp"
#include "executor/sort.hpp"
#include "executor/two_phase_distinct_agg.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(MinMaxIndexExecutorTest, EmptyAndNullPrefixProduceScalarNull) {
  auto source = std::make_shared<ValuesExecutor>(
      std::vector<Row>{Row({Value()}), Row({Value(int64_t{42})})});
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

  PartialSortExecutor partial_sort(src, schema, keys, /*top_k=*/3,
                                   /*offset=*/0);
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
  const Schema schema(
      "data", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  auto src = std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value(int64_t{0b1100}), Value(int64_t{1})}),
      Row({Value(int64_t{0b1010}), Value(int64_t{1})}),
      Row({Value(int64_t{0b1001}), Value(int64_t{0})}),
  });

  auto bit_and = std::make_shared<AggregateExpression>(AggregationType::kBitAnd,
                                                       ColumnValueExp("a"));
  auto bit_or = std::make_shared<AggregateExpression>(AggregationType::kBitOr,
                                                      ColumnValueExp("a"));
  auto bit_xor = std::make_shared<AggregateExpression>(AggregationType::kBitXor,
                                                       ColumnValueExp("a"));
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

  TwoPhaseDistinctAggExecutor distinct_agg(src, schema,
                                           {ColumnValueExp("dept")},
                                           {NamedExpression("cnt", agg_count)});

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

TEST(BatchNestedLoopJoinTest, RightOuterEmitsUnmatchedRightRows) {
  const Schema left_schema("l", {Column("id", ValueType::kInt64)});
  const Schema right_schema("r", {Column("id", ValueType::kInt64)});
  Executor left = std::make_shared<ValuesExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  Executor right = std::make_shared<ValuesExecutor>(
      std::vector<Row>{Row({Value(2)}), Row({Value(3)})});
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("r", "id")));
  BatchNestedLoopJoin join(std::move(left), left_schema, std::move(right),
                           right_schema, predicate, JoinKind::kRightOuter, 1);
  Row row;
  RowPosition rp;
  std::vector<Row> out;
  while (join.Next(&row, &rp)) {
    out.push_back(row);
  }
  // (2,2) matched + (NULL,3) unmatched = 2 rows.
  ASSERT_EQ(out.size(), 2);
  size_t null_left = 0;
  for (const Row& r : out) {
    if (r[0].IsNull()) {
      ++null_left;
      EXPECT_EQ(r[1], Value(3));
    }
  }
  EXPECT_EQ(null_left, 1);
}

TEST(ScanFilterTest, UnsignedComparisonsMatchGroundTruth) {
  // Fixed: the signed int fast path mis-filtered UINT64 boundary values
  // (UINT64_MAX stored as bit pattern -1 matched `id < 0`).
  Column unsigned_col("id", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema("t", {unsigned_col});
  const Value max_uint = Value(int64_t{-1}).WithUnsigned();
  const Row row({max_uint});
  relational_detail::SimpleComparePredicate pred;
  pred.column = 0;
  pred.op = BinaryOperation::kLessThan;
  pred.constant = Value(int64_t{0});
  pred.int_payload = true;
  pred.int_constant = 0;
  EXPECT_FALSE(relational_detail::MatchSimpleCompare(row, pred))
      << "UINT64_MAX is not < 0 under unsigned semantics";
  EXPECT_FALSE(
      EvaluateBinary(BinaryOperation::kLessThan, max_uint, Value(int64_t{0}))
          .Truthy());
  // MatchScanFilter tags rows from the schema before matching, so the
  // residual path agrees too.
  Row tagged = row;
  tagged[0] = tagged[0].WithUnsigned();
  EXPECT_FALSE(relational_detail::MatchSimpleCompare(tagged, pred));
}

TEST(GroupingSetsTest, OutputSchemaMatchesAggregateResultTypes) {
  // Fixed: SUM(int) was declared Double and BIT_AND/MIN(int) VarChar while
  // the executor emits child-typed values.
  const Schema input("t", {Column("x", ValueType::kInt64)});
  const Expression sum = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("x"));
  const NamedExpression sum_named("s", sum);
  Executor child = std::make_shared<ValuesExecutor>(
      std::vector<Row>{Row({Value(int64_t{1})})});
  GroupingSetsExecutor gs(std::move(child), input, {}, {{}}, {sum_named});
  const Schema& out = gs.OutputSchema();
  ASSERT_EQ(out.ColumnCount(), 1U);
  EXPECT_EQ(out.GetColumn(0).Type(), ValueType::kInt64);
}

TEST(ExchangeTest, NextBatchResetsReusedDestination) {
  // Fixed: Append-only NextBatch mixed the previous batch into a reused
  // destination chunk.
  Executor child = std::make_shared<ValuesExecutor>(
      std::vector<Row>{Row({Value(int64_t{1})}), Row({Value(int64_t{2})}),
                       Row({Value(int64_t{3})})});
  auto exchange = std::make_shared<ExchangeExecutor>(std::move(child),
                                                     ExchangeType::kGather, 1);
  exchange->MaterializePipeline();
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  DataChunk chunk(schema, 8);
  Executor part = exchange->GetPartitionExecutor(0);
  size_t first = part->NextBatch(&chunk, 2);
  EXPECT_EQ(first, 2U);
  size_t second = part->NextBatch(&chunk, 2);
  EXPECT_EQ(second, 1U);
  EXPECT_EQ(chunk.Size(), 1U) << "reused chunk must not retain prior rows";
}

TEST(BatchNestedLoopJoinTest, PredicateErrorPropagatesForAntiJoin) {
  const Schema left_schema("l", {Column("a", ValueType::kInt64)});
  const Schema right_schema("r", {Column("b", ValueType::kInt64)});
  Executor left =
      std::make_shared<ValuesExecutor>(std::vector<Row>{Row({Value(1)})});
  Executor right =
      std::make_shared<ValuesExecutor>(std::vector<Row>{Row({Value(0)})});
  // 1 / 0 throws; it must not be swallowed as FALSE (which would emit the
  // anti-join row instead of erroring).
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "a")), BinaryOperation::kDivide,
      ColumnValueExp(ColumnName("r", "b")));
  BatchNestedLoopJoin join(std::move(left), left_schema, std::move(right),
                           right_schema, predicate, JoinKind::kAnti, 16);
  Row row;
  RowPosition rp;
  EXPECT_THROW(
      {
        while (join.Next(&row, &rp)) {
        }
      },
      std::runtime_error);
}

// ===== Statistical aggregates (CORR / COVAR_* / VAR_* / STDDEV_*) =====
//
// The goldens mirror the GoogleSQL compliance corpus (stat_aggregation.test):
// 3-valued NULL semantics, NaN/inf propagation, one-pair → NULL for
// CORR/COVAR_SAMP but a computed value for COVAR_POP, zero-over-zero → NaN.

namespace {

// Builds a two-argument statistical aggregate whose second argument rides as
// a trailing argument, exactly like the SQL frontend does for CORR(y, x).
Expression StatAggregate(AggregationType type, const std::string& first,
                         const std::string& second) {
  auto aggregate =
      std::make_shared<AggregateExpression>(type, ColumnValueExp(first));
  aggregate->SetTrailingArgs({ColumnValueExp(second)});
  return {std::move(aggregate)};
}

std::vector<NamedExpression> MakeCovarianceAggregates() {
  return {
      NamedExpression("corr", StatAggregate(AggregationType::kCorr, "y", "x")),
      NamedExpression("covar_samp",
                      StatAggregate(AggregationType::kCovarSamp, "y", "x")),
      NamedExpression("covar_pop",
                      StatAggregate(AggregationType::kCovarPop, "y", "x"))};
}

// Runs one scalar aggregation over `rows` through both the serial executor
// (ground-truth accumulator delegation) and the parallel executor; the two
// paths must agree on every statistical aggregate.
std::vector<Row> RunStatAggregation(
    const Schema& schema, const std::vector<Row>& rows,
    const std::vector<NamedExpression>& aggregates) {
  std::vector<Row> results;
  AggregationExecutor serial(std::make_shared<ValuesExecutor>(rows), schema,
                             aggregates);
  Row serial_result;
  EXPECT_TRUE(serial.Next(&serial_result, nullptr));
  EXPECT_FALSE(serial.Next(&serial_result, nullptr));
  results.push_back(serial_result);

  ParallelAggregationExecutor parallel(std::make_shared<ValuesExecutor>(rows),
                                       schema, aggregates, 2);
  Row parallel_result;
  EXPECT_TRUE(parallel.Next(&parallel_result, nullptr));
  results.push_back(parallel_result);
  return results;
}

void ExpectStatValue(const Row& row, size_t index, double expected) {
  ASSERT_FALSE(row[index].IsNull());
  EXPECT_EQ(row[index].type, ValueType::kDouble);
  EXPECT_EQ(row[index].value.double_value, expected);
}

void ExpectStatNull(const Row& row, size_t index) {
  EXPECT_TRUE(row[index].IsNull()) << "column " << index;
}

void ExpectStatNaN(const Row& row, size_t index) {
  ASSERT_FALSE(row[index].IsNull());
  EXPECT_TRUE(std::isnan(row[index].value.double_value)) << "column " << index;
}

}  // namespace

TEST(StatAggregateTest, CorrAndCovarNormalValues) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const std::vector<Row> rows = {
      Row({Value(1.0), Value(5.0)}), Row({Value(3.0), Value(9.0)}),
      Row({Value(4.0), Value(7.0)}), Row({Value(5.0), Value(1.0)}),
      Row({Value(7.0), Value(13.0)})};
  // GoogleSQL goldens: CORR=0.4, COVAR_SAMP=4, COVAR_POP=3.2.
  for (const Row& result :
       RunStatAggregation(schema, rows, MakeCovarianceAggregates())) {
    ExpectStatValue(result, 0, 0.4);
    ExpectStatValue(result, 1, 4.0);
    ExpectStatValue(result, 2, 3.2);
  }
}

TEST(StatAggregateTest, CorrAndCovarOnePair) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const std::vector<Row> rows = {Row({Value(1.0), Value(1.0)})};
  // One pair: COVAR_SAMP and CORR are NULL, COVAR_POP is 0.
  for (const Row& result :
       RunStatAggregation(schema, rows, MakeCovarianceAggregates())) {
    ExpectStatNull(result, 0);
    ExpectStatNull(result, 1);
    ExpectStatValue(result, 2, 0.0);
  }
}

TEST(StatAggregateTest, CorrAndCovarEmptyInput) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  for (const Row& result :
       RunStatAggregation(schema, {}, MakeCovarianceAggregates())) {
    ExpectStatNull(result, 0);
    ExpectStatNull(result, 1);
    ExpectStatNull(result, 2);
  }
}

TEST(StatAggregateTest, CorrAndCovarSkipNullPairs) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const Value null_value;
  const std::vector<Row> rows = {
      Row({Value(1.0), Value(1.0)}),    Row({Value(2.0), Value(2.0)}),
      Row({Value(3.0), Value(3.0)}),    Row({Value(4.0), Value(4.0)}),
      Row({Value(5.0), Value(5.0)}),    Row({null_value, Value(1000.0)}),
      Row({Value(2000.0), null_value}), Row({null_value, null_value})};
  // Only the five complete pairs count: COVAR_SAMP=2.5, COVAR_POP=2, CORR=1.
  for (const Row& result :
       RunStatAggregation(schema, rows, MakeCovarianceAggregates())) {
    ExpectStatValue(result, 0, 1.0);
    ExpectStatValue(result, 1, 2.5);
    ExpectStatValue(result, 2, 2.0);
  }
}

TEST(StatAggregateTest, CorrAndCovarPropagateInfAndNaN) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const double inf = std::numeric_limits<double>::infinity();
  const double nan = std::numeric_limits<double>::quiet_NaN();
  // A non-finite value in any complete pair makes every two-input form NaN.
  const std::vector<std::pair<double, double>> non_finite = {
      {inf, 4.0}, {3.0, inf}, {-inf, 4.0}, {3.0, -inf},
      {nan, 4.0}, {3.0, nan}, {inf, inf},  {-inf, -inf}};
  for (const auto& [y, x] : non_finite) {
    for (const Row& result : RunStatAggregation(
             schema, {Row({Value(1.0), Value(2.0)}), Row({Value(y), Value(x)})},
             MakeCovarianceAggregates())) {
      ExpectStatNaN(result, 0);
      ExpectStatNaN(result, 1);
      ExpectStatNaN(result, 2);
    }
  }
}

TEST(StatAggregateTest, CorrAndCovarTooFewArgsAreNullEvenNonFinite) {
  // The compliance corpus pins "one pair containing inf/nan" to NULL for the
  // sample forms (zero degrees of freedom), while COVAR_POP still evaluates
  // its single pair and surfaces inf - inf as NaN.
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const double inf = std::numeric_limits<double>::infinity();
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const std::vector<std::pair<double, double>> single_pair = {
      {1.0, inf}, {1.0, -inf}, {1.0, nan}, {inf, 1.0}, {nan, 1.0}};
  for (const auto& [y, x] : single_pair) {
    for (const Row& result : RunStatAggregation(
             schema, {Row({Value(y), Value(x)})}, MakeCovarianceAggregates())) {
      ExpectStatNull(result, 0);
      ExpectStatNull(result, 1);
      ExpectStatNaN(result, 2);
    }
  }
}

TEST(StatAggregateTest, CorrAndCovarExtremeNegativeSlope) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const double inf = std::numeric_limits<double>::infinity();
  // COVAR overflows to -inf while CORR stays exactly -1.
  const std::vector<Row> covar_rows = {Row({Value(1.0), Value(1.0)}),
                                       Row({Value(2.2e304), Value(-2.2e304)})};
  for (const Row& result : RunStatAggregation(
           schema, covar_rows,
           {NamedExpression(
                "covar_samp",
                StatAggregate(AggregationType::kCovarSamp, "y", "x")),
            NamedExpression(
                "covar_pop",
                StatAggregate(AggregationType::kCovarPop, "y", "x"))})) {
    ASSERT_FALSE(result[0].IsNull());
    EXPECT_EQ(result[0].value.double_value, -inf);
    ASSERT_FALSE(result[1].IsNull());
    EXPECT_EQ(result[1].value.double_value, -inf);
  }
  const std::vector<Row> corr_rows = {Row({Value(1.0), Value(-1.0)}),
                                      Row({Value(2.2e154), Value(-2.2e154)})};
  for (const Row& result : RunStatAggregation(
           schema, corr_rows,
           {NamedExpression(
               "corr", StatAggregate(AggregationType::kCorr, "y", "x"))})) {
    ExpectStatValue(result, 0, -1.0);
  }
}

TEST(StatAggregateTest, CorrZeroOverZeroIsNaN) {
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  // Constant on both sides: 0/0 must surface as NaN, not NULL and not an
  // error.
  const std::vector<Row> rows = {Row({Value(1.0), Value(2.0)}),
                                 Row({Value(1.0), Value(2.0)})};
  for (const Row& result : RunStatAggregation(
           schema, rows,
           {NamedExpression(
               "corr", StatAggregate(AggregationType::kCorr, "y", "x"))})) {
    ExpectStatNaN(result, 0);
  }
}

TEST(StatAggregateTest, VarianceAndStddevAcrossPaths) {
  const Schema schema("t", {Column("x", ValueType::kDouble)});
  auto variance_aggregates = [] {
    return std::vector<NamedExpression>{
        NamedExpression("var_samp",
                        AggregateExpressionExp(AggregationType::kVarSamp,
                                               ColumnValueExp("x"))),
        NamedExpression("var_pop",
                        AggregateExpressionExp(AggregationType::kVarPop,
                                               ColumnValueExp("x"))),
        NamedExpression("stddev_samp",
                        AggregateExpressionExp(AggregationType::kStddevSamp,
                                               ColumnValueExp("x"))),
        NamedExpression("stddev_pop",
                        AggregateExpressionExp(AggregationType::kStddevPop,
                                               ColumnValueExp("x")))};
  };
  // VAR_SAMP([50,60,70])=100, VAR_POP=66.66.., STDDEV_SAMP=10, STDDEV_POP
  // = 8.16..; goldens from stat_aggregation.test (var_samp_same_distance...).
  const std::vector<Row> nice = {Row({Value(50.0)}), Row({Value(60.0)}),
                                 Row({Value(70.0)})};
  for (const Row& result :
       RunStatAggregation(schema, nice, variance_aggregates())) {
    ExpectStatValue(result, 0, 100.0);
    ExpectStatValue(result, 1, 200.0 / 3.0);
    ExpectStatValue(result, 2, 10.0);
    ExpectStatValue(result, 3, 10.0 * std::sqrt(2.0 / 3.0));
  }
  // Same values: zero variance everywhere.
  const std::vector<Row> same = {Row({Value(1.0)}), Row({Value(1.0)})};
  for (const Row& result :
       RunStatAggregation(schema, same, variance_aggregates())) {
    ExpectStatValue(result, 0, 0.0);
    ExpectStatValue(result, 1, 0.0);
    ExpectStatValue(result, 2, 0.0);
    ExpectStatValue(result, 3, 0.0);
  }
  // A single non-NULL value: the sample forms are NULL, the population forms
  // are 0 -- even when that value is non-finite.
  const double inf = std::numeric_limits<double>::infinity();
  for (const Row& result :
       RunStatAggregation(schema, {Row({Value(inf)})}, variance_aggregates())) {
    ExpectStatNull(result, 0);
    ExpectStatNaN(result, 1);
    ExpectStatNull(result, 2);
    ExpectStatNaN(result, 3);
  }
  // Two rows containing inf: every form is NaN.
  for (const Row& result :
       RunStatAggregation(schema, {Row({Value(1.0)}), Row({Value(inf)})},
                          variance_aggregates())) {
    ExpectStatNaN(result, 0);
    ExpectStatNaN(result, 1);
    ExpectStatNaN(result, 2);
    ExpectStatNaN(result, 3);
  }
  // Empty input: NULL everywhere.
  for (const Row& result :
       RunStatAggregation(schema, {}, variance_aggregates())) {
    ExpectStatNull(result, 0);
    ExpectStatNull(result, 1);
    ExpectStatNull(result, 2);
    ExpectStatNull(result, 3);
  }
  // Extreme spread: VAR_* overflow to inf; STDDEV_SAMP survives as 2.2e304
  // and STDDEV_POP as 1.7962924780409973e304 (GoogleSQL goldens).
  const std::vector<Row> extreme = {Row({Value(1.0)}), Row({Value(2.2e304)}),
                                    Row({Value(-2.2e304)})};
  for (const Row& result :
       RunStatAggregation(schema, extreme, variance_aggregates())) {
    EXPECT_EQ(result[0].value.double_value, inf);
    EXPECT_EQ(result[1].value.double_value, inf);
    ExpectStatValue(result, 2, 2.2e304);
    ExpectStatValue(result, 3, 1.7962924780409973e304);
  }
}

TEST(StatAggregateTest, ParallelGenericPathHandlesExpressionArguments) {
  // COVAR over expressions (not bare columns) exercises the parallel
  // executor's per-row statistical path, including paired NULL skipping.
  const Schema schema(
      "t", {Column("y", ValueType::kDouble), Column("x", ValueType::kDouble)});
  const Value null_value;
  std::vector<Row> rows = {
      Row({Value(1.0), Value(5.0)}),  Row({Value(3.0), Value(9.0)}),
      Row({Value(4.0), Value(7.0)}),  Row({Value(5.0), Value(1.0)}),
      Row({Value(7.0), Value(13.0)}), Row({null_value, Value(42.0)}),
      Row({Value(42.0), null_value})};
  auto y_expr = BinaryExpressionExp(ColumnValueExp("y"), BinaryOperation::kAdd,
                                    ConstantValueExp(Value(0.0)));
  auto x_expr = BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kAdd,
                                    ConstantValueExp(Value(0.0)));
  auto covar = std::make_shared<AggregateExpression>(
      AggregationType::kCovarSamp, std::move(y_expr));
  covar->SetTrailingArgs({std::move(x_expr)});
  std::vector<NamedExpression> aggregates = {NamedExpression("covar", covar)};

  AggregationExecutor serial(std::make_shared<ValuesExecutor>(rows), schema,
                             aggregates);
  Row serial_result;
  ASSERT_TRUE(serial.Next(&serial_result, nullptr));
  ExpectStatValue(serial_result, 0, 4.0);

  ParallelAggregationExecutor parallel(std::make_shared<ValuesExecutor>(rows),
                                       schema, aggregates, 2);
  Row parallel_result;
  ASSERT_TRUE(parallel.Next(&parallel_result, nullptr));
  ExpectStatValue(parallel_result, 0, 4.0);
}

TEST(StatAggregateTest, ParallelPathLargeMixedInputMatchesSerial) {
  // Above the parallel threshold shape: enough rows that workers split the
  // input, mixing INT64 child columns with a DOUBLE trailing column and
  // NULL gaps. Serial and parallel results must agree bit for bit.
  const Schema schema(
      "t", {Column("y", ValueType::kInt64), Column("x", ValueType::kDouble)});
  std::vector<Row> rows;
  for (int64_t i = 0; i < 500; ++i) {
    if (i % 17 == 3) {
      rows.push_back(Row({Value(), Value(static_cast<double>(i))}));
    } else if (i % 17 == 9) {
      rows.push_back(Row({Value(i), Value()}));
    } else {
      rows.push_back(Row({Value(i), Value(static_cast<double>(i) * 1.5)}));
    }
  }
  const std::vector<NamedExpression> aggregates = {
      NamedExpression("corr", StatAggregate(AggregationType::kCorr, "y", "x")),
      NamedExpression("covar_samp",
                      StatAggregate(AggregationType::kCovarSamp, "y", "x")),
      NamedExpression("var_pop",
                      AggregateExpressionExp(AggregationType::kVarPop,
                                             ColumnValueExp("y")))};

  AggregationExecutor serial(std::make_shared<ValuesExecutor>(rows), schema,
                             aggregates);
  Row serial_result;
  ASSERT_TRUE(serial.Next(&serial_result, nullptr));

  ParallelAggregationExecutor parallel(std::make_shared<ValuesExecutor>(rows),
                                       schema, aggregates, 4);
  Row parallel_result;
  ASSERT_TRUE(parallel.Next(&parallel_result, nullptr));

  ASSERT_EQ(serial_result.values_.size(), parallel_result.values_.size());
  for (size_t i = 0; i < serial_result.values_.size(); ++i) {
    SCOPED_TRACE(i);
    if (serial_result[i].IsNull()) {
      ExpectStatNull(parallel_result, i);
    } else {
      EXPECT_EQ(serial_result[i].type, ValueType::kDouble);
      EXPECT_EQ(serial_result[i].value.double_value,
                parallel_result[i].value.double_value);
    }
  }
  // Sanity: the correlation of a perfect line is 1.
  ExpectStatValue(serial_result, 0, 1.0);
}

}  // namespace tinylamb
