#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "executor/aggregation.hpp"
#include "executor/batch_nested_loop_join.hpp"
#include "executor/cross_join.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/hash_join.hpp"
#include "executor/minmax_index.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/parallel_hash_join.hpp"
#include "executor/pdqsort.hpp"
#include "executor/projection.hpp"
#include "executor/sort.hpp"
#include "executor/topn.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
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
  EXPECT_FALSE(relational_detail::MatchSimpleCompare(row, pred));
  EXPECT_FALSE(
      EvaluateBinary(BinaryOperation::kLessThan, max_uint, Value(int64_t{0}))
          .Truthy());
  // MatchScanFilter tags rows from the schema before matching, so the
  // residual path agrees too.
  Row tagged = row;
  tagged[0] = tagged[0].WithUnsigned();
  EXPECT_FALSE(relational_detail::MatchSimpleCompare(tagged, pred));
}

TEST(ScanFilterTest, CompileTimeResolvesDeterministicConstantConjuncts) {
  // Constant-residual boundary: a deterministic conjunct with a compile-time
  // value is decided once by CompileScanFilter, not per row.  TRUE
  // discharges the conjunct; FALSE / NULL rejects the whole conjunction
  // (same three-valued rule the row loop applies, so sibling errors that a
  // rejecting conjunct would suppress stay suppressed); volatile conjuncts
  // stay runtime predicates.
  const Schema schema("const_k", {Column("key", ValueType::kInt64)});
  const auto with_constant = [](const Expression& conjunct) {
    return std::vector<Expression>{
        conjunct, BinaryExpressionExp(ColumnValueExp("key"),
                                      BinaryOperation::kGreaterThan,
                                      ConstantValueExp(Value(int64_t{2})))};
  };
  const Row row_pass({Value(int64_t{3})});
  const Row row_reject({Value(int64_t{2})});

  {
    // Constant TRUE (upper('A') = 'A'): discharged into nothing.
    const Expression true_conjunct = BinaryExpressionExp(
        FunctionCallExp("upper", {ConstantValueExp(Value("A"))}),
        BinaryOperation::kEquals, ConstantValueExp(Value("A")));
    const relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter(with_constant(true_conjunct),
                                             schema);
    EXPECT_FALSE(filter.compile_decided_reject);
    EXPECT_TRUE(filter.residual.empty());
    ASSERT_EQ(filter.simple.size(), 1U);
    const relational_detail::WorkerScanFilterPrograms programs =
        relational_detail::CompileWorkerFilter(filter, schema);
    EXPECT_EQ(relational_detail::MatchScanFilterWorker(row_pass, schema, filter,
                                                       programs),
              1);
    EXPECT_EQ(relational_detail::MatchScanFilterWorker(row_reject, schema,
                                                       filter, programs),
              0);
  }
  {
    // Constant FALSE (upper('A') = 'B'): decided once, rejects every row.
    const Expression false_conjunct = BinaryExpressionExp(
        FunctionCallExp("upper", {ConstantValueExp(Value("A"))}),
        BinaryOperation::kEquals, ConstantValueExp(Value("B")));
    const relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter(with_constant(false_conjunct),
                                             schema);
    EXPECT_TRUE(filter.compile_decided_reject);
    EXPECT_TRUE(filter.simple.empty());
    EXPECT_TRUE(filter.residual.empty());
    const relational_detail::WorkerScanFilterPrograms programs =
        relational_detail::CompileWorkerFilter(filter, schema);
    EXPECT_EQ(relational_detail::MatchScanFilterWorker(row_pass, schema, filter,
                                                       programs),
              0);
    EXPECT_EQ(relational_detail::MatchScanFilterWorker(row_reject, schema,
                                                       filter, programs),
              0);
  }
  {
    // Constant NULL: the three-valued rule rejects the conjunction.
    const relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter(
            with_constant(ConstantValueExp(Value())), schema);
    EXPECT_TRUE(filter.compile_decided_reject);
  }
  {
    // A volatile call keeps its per-row evaluation count: folding would
    // change how often it runs, so the conjunct stays residual.
    const Expression volatile_conjunct = BinaryExpressionExp(
        FunctionCallExp("rand", {}), BinaryOperation::kLessThan,
        ConstantValueExp(Value(0.0)));
    const relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter(with_constant(volatile_conjunct),
                                             schema);
    EXPECT_FALSE(filter.compile_decided_reject);
    ASSERT_EQ(filter.residual.size(), 1U);
  }
}

// The constant-residual boundary is for column-free trees only.  The
// empty-schema TryEvaluate it relies on is not a column gate: an unbound
// column does not reliably fail there, and an error-absorbing call
// (ISERROR / NULLIFERROR) turns whatever that evaluation produced into a
// value.  A column-bearing conjunct "decided" that way silently filtered
// whole grouped results (every group of `GROUP BY A HAVING ISERROR(A+1/A)`
// survived, every group of the NULLIFERROR spelling vanished), so the
// boundary must check touched columns explicitly and leave such conjuncts
// to the per-row evaluation -- which agrees with the ground truth of the
// same expression in SELECT-list position.  The full statement semantics
// are pinned at the SQL level (QueryTest.
// GroupedHavingErrorAbsorptionStaysPerRow).
TEST(ScanFilterTest, ConstantResidualBoundaryNeverDecidesColumnConjuncts) {
  const Schema schema("boundary_col", {Column("key", ValueType::kInt64)});
  {
    // ISERROR(key + 1/key) touches the key column: stays per-row.
    const Expression conjunct = FunctionCallExp(
        "iserror", {BinaryExpressionExp(
                       ColumnValueExp("key"), BinaryOperation::kAdd,
                       BinaryExpressionExp(ConstantValueExp(Value(int64_t{1})),
                                           BinaryOperation::kDivide,
                                           ColumnValueExp("key")))});
    const relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter({conjunct}, schema);
    EXPECT_FALSE(filter.compile_decided_reject);
    EXPECT_FALSE(filter.simple.empty() && filter.residual.empty());
  }
  {
    // NULLIFERROR(key + 1/key) >= 0: still column-bearing after the fold.
    const Expression conjunct = BinaryExpressionExp(
        FunctionCallExp(
            "nulliferror",
            {BinaryExpressionExp(
                ColumnValueExp("key"), BinaryOperation::kAdd,
                BinaryExpressionExp(ConstantValueExp(Value(int64_t{1})),
                                    BinaryOperation::kDivide,
                                    ColumnValueExp("key")))}),
        BinaryOperation::kGreaterThanEquals,
        ConstantValueExp(Value(int64_t{0})));
    const relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter({conjunct}, schema);
    EXPECT_FALSE(filter.compile_decided_reject);
    // The comparison stays a live runtime predicate in some component
    // (simple / residual list / residual bytecode / prebuilt compare).
    EXPECT_FALSE(filter.simple.empty() && filter.residual.empty() &&
                 filter.prebuilt_scalar_compare.empty() &&
                 filter.prefix_in.empty() &&
                 filter.prebuilt_membership.empty() &&
                 !filter.residual_bytecode.has_value() &&
                 filter.disjunctive_branches.empty());
  }
}

TEST(SortExecutorTest, UnsignedInt64AscendingAndDescendingOrder) {
  Column unsigned_col("id", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema("t", {unsigned_col});

  const Value u0 = Value(0ULL).WithUnsigned();
  const Value u10 = Value(10ULL).WithUnsigned();
  const Value umax = Value(std::numeric_limits<uint64_t>::max()).WithUnsigned();

  // Test single-column path (SingleKey optimization) and multi-column path
  // (AppendEncoded).
  for (bool single_key : {true, false}) {
    auto make_src = [&]() {
      return std::make_shared<ValuesExecutor>(std::vector<Row>{
          Row({umax}),
          Row({u0}),
          Row({u10}),
      });
    };

    // Ascending
    std::vector<SortExecutor::Key> asc_keys;
    if (single_key) {
      asc_keys.push_back(SortExecutor::Key{.expression = ColumnValueExp("id"),
                                           .ascending = true,
                                           .nulls_first = std::nullopt});
    } else {
      asc_keys.push_back(SortExecutor::Key{.expression = ColumnValueExp("id"),
                                           .ascending = true,
                                           .nulls_first = std::nullopt});
      asc_keys.push_back(SortExecutor::Key{.expression = ColumnValueExp("id"),
                                           .ascending = true,
                                           .nulls_first = std::nullopt});
    }
    SortExecutor sort_asc(make_src(), schema, asc_keys);
    sort_asc.MaterializePipeline();
    std::vector<Value> asc_results;
    Row row;
    RowPosition rp;
    while (sort_asc.Next(&row, &rp)) {
      asc_results.push_back(row[0]);
    }
    ASSERT_EQ(asc_results.size(), 3U);
    EXPECT_EQ(asc_results[0], u0);
    EXPECT_EQ(asc_results[1], u10);
    EXPECT_EQ(asc_results[2], umax);

    // Descending
    std::vector<SortExecutor::Key> desc_keys;
    if (single_key) {
      desc_keys.push_back(SortExecutor::Key{.expression = ColumnValueExp("id"),
                                            .ascending = false,
                                            .nulls_first = std::nullopt});
    } else {
      desc_keys.push_back(SortExecutor::Key{.expression = ColumnValueExp("id"),
                                            .ascending = false,
                                            .nulls_first = std::nullopt});
      desc_keys.push_back(SortExecutor::Key{.expression = ColumnValueExp("id"),
                                            .ascending = false,
                                            .nulls_first = std::nullopt});
    }
    SortExecutor sort_desc(make_src(), schema, desc_keys);
    sort_desc.MaterializePipeline();
    std::vector<Value> desc_results;
    while (sort_desc.Next(&row, &rp)) {
      desc_results.push_back(row[0]);
    }
    ASSERT_EQ(desc_results.size(), 3U);
    EXPECT_EQ(desc_results[0], umax);
    EXPECT_EQ(desc_results[1], u10);
    EXPECT_EQ(desc_results[2], u0);
  }
}

TEST(TopNExecutorTest, UnsignedInt64TopNMatchesSort) {
  Column unsigned_col("id", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema("t", {unsigned_col});

  const Value u0 = Value(0ULL).WithUnsigned();
  const Value u10 = Value(10ULL).WithUnsigned();
  const Value umax = Value(std::numeric_limits<uint64_t>::max()).WithUnsigned();

  auto make_src = [&]() {
    return std::make_shared<ValuesExecutor>(std::vector<Row>{
        Row({umax}),
        Row({u0}),
        Row({u10}),
    });
  };

  std::vector<TopNExecutor::Key> keys = {
      TopNExecutor::Key{.expression = ColumnValueExp("id"),
                        .ascending = true,
                        .nulls_first = std::nullopt}};

  TopNExecutor topn(make_src(), schema, keys, /*limit=*/2, /*offset=*/0);

  std::vector<Value> results;
  Row row;
  RowPosition rp;
  while (topn.Next(&row, &rp)) {
    results.push_back(row[0]);
  }
  ASSERT_EQ(results.size(), 2U);
  EXPECT_EQ(results[0], u0);
  EXPECT_EQ(results[1], u10);
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
  while (join.Next(&row, &rp)) {
  }
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
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
  EXPECT_TRUE(row[index].IsNull()) << index;
}

void ExpectStatNaN(const Row& row, size_t index) {
  ASSERT_FALSE(row[index].IsNull());
  EXPECT_TRUE(std::isnan(row[index].value.double_value)) << index;
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

// ---------------------------------------------------------------------------
// Coverage for previously untested executor building blocks: the
// scalar-subquery guard, the in-memory sort kernel, the
// partitioned bump arena, and the cardinality probe wrapper.
// ---------------------------------------------------------------------------

TEST(PdqSortTest, MatchesOrderingContractAcrossShapes) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});

  // Ascending on the first column, with duplicates preserved.
  std::vector<Row> rows;
  rows.reserve(6);
  for (const int64_t v : {5, 1, 4, 1, 3, 5}) {
    rows.emplace_back(Row({Value(v), Value(v * 10)}));
  }
  PdqSort::Sort(rows, schema,
                {SortExecutor::Key{.expression = ColumnValueExp("a"),
                                   .ascending = true}});
  std::vector<int64_t> keys;
  keys.reserve(rows.size());
  for (const Row& r : rows) {
    keys.push_back(r[0].value.int_value);
  }
  EXPECT_EQ(keys, (std::vector<int64_t>{1, 1, 3, 4, 5, 5}));

  // Descending reverses the order.
  PdqSort::Sort(rows, schema,
                {SortExecutor::Key{.expression = ColumnValueExp("a"),
                                   .ascending = false}});
  keys.clear();
  for (const Row& r : rows) {
    keys.push_back(r[0].value.int_value);
  }
  EXPECT_EQ(keys, (std::vector<int64_t>{5, 5, 4, 3, 1, 1}));
}

TEST(PdqSortTest, TiebreakAndNullPlacement) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  // Ties on `a` fall back to the descending `b` key; a NULL sorts first
  // under the ascending default (nulls_first defaults to ascending).
  std::vector<Row> rows;
  rows.emplace_back(Row({Value(int64_t{2}), Value(int64_t{30})}));
  rows.emplace_back(Row({Value()}));
  rows.emplace_back(Row({Value(int64_t{2}), Value(int64_t{10})}));
  rows.emplace_back(Row({Value(int64_t{1}), Value(int64_t{99})}));
  PdqSort::Sort(
      rows, schema,
      {SortExecutor::Key{.expression = ColumnValueExp("a"), .ascending = true},
       SortExecutor::Key{.expression = ColumnValueExp("b"),
                         .ascending = false}});
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_TRUE(rows[0][0].IsNull());
  EXPECT_EQ(rows[1][0].value.int_value, 1);
  EXPECT_EQ(rows[2][0].value.int_value, 2);
  EXPECT_EQ(rows[2][1].value.int_value, 30);
  EXPECT_EQ(rows[3][0].value.int_value, 2);
  EXPECT_EQ(rows[3][1].value.int_value, 10);

  // Trivial inputs must be no-ops, not crashes.
  std::vector<Row> none;
  PdqSort::Sort(none, schema,
                {SortExecutor::Key{.expression = ColumnValueExp("a"),
                                   .ascending = true}});
  EXPECT_TRUE(none.empty());
}

// A child executor that fails the drain after a bounded number of rows, so
// drain-error propagation can be exercised without a real I/O fault.
class DrainFailingRows final : public ExecutorBase {
 public:
  DrainFailingRows(std::vector<Row> rows, size_t fail_after)
      : rows_(std::move(rows)), fail_after_(fail_after) {}
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset(Schema("t", {Column("v", ValueType::kInt64)}),
                       std::min(max_rows, rows_.size()));
    if (emitted_ >= fail_after_) {
      (void)FailWith(StatusError(StatusCode::kIOError, "source failed"));
      return 0;
    }
    const size_t limit =
        std::min({max_rows, fail_after_ - emitted_, rows_.size() - emitted_});
    for (size_t i = 0; i < limit; ++i) {
      destination->Append(rows_[emitted_],
                          RowPosition(0, static_cast<slot_t>(emitted_)));
      ++emitted_;
    }
    return limit;
  }
  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "DrainFailingRows";
  }

 private:
  std::vector<Row> rows_;
  size_t fail_after_{0};
  size_t emitted_{0};
};

TEST(DrainErrorPropagationTest, AggregationJitSumForwardsChildError) {
  // A JIT'ed SUM over a failing child must fail the whole drain
  // as a sticky error, never as a partial sum emitted with success.
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  AggregationExecutor agg(
      std::make_shared<DrainFailingRows>(
          std::vector<Row>{Row({Value(int64_t{10})}), Row({Value(int64_t{20})}),
                           Row({Value(int64_t{30})})},
          2),
      schema,
      {NamedExpression("total",
                       std::make_shared<AggregateExpression>(
                           AggregationType::kSum, ColumnValueExp("v")))});

  Row row;
  EXPECT_FALSE(agg.Next(&row, nullptr));
  EXPECT_NE(agg.GetStatus(), Status::kSuccess);
}

TEST(DrainErrorPropagationTest, CrossJoinForwardsLeftError) {
  const Schema left_schema("l", {Column("v", ValueType::kInt64)});
  const Schema right_schema("r", {Column("w", ValueType::kInt64)});
  CrossJoin join(
      std::make_shared<DrainFailingRows>(
          std::vector<Row>{Row({Value(int64_t{1})}), Row({Value(int64_t{2})})},
          1),
      std::make_shared<ValuesExecutor>(
          std::vector<Row>{Row({Value(int64_t{5})})}));

  Row row;
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(int64_t{1}));
  // The left side fails after its last row; the join must report that error
  // instead of ending as a clean EOF with a truncated right pairing.
  EXPECT_FALSE(join.Next(&row, nullptr));
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
}

TEST(DrainErrorPropagationTest, AuditHashJoinReportsSpillReadErrorAtEof) {
  struct Restore {
    size_t budget = QueryMemoryBudget::Global().Limit();
    std::optional<std::string> temp;
    std::filesystem::path directory;
    Restore() {
      if (const char* current = std::getenv("TINYLAMB_TEMP")) {
        temp = current;
      }
      directory = std::filesystem::temp_directory_path() /
                  ("tinylamb-audit-spill-" + RandomString());
      platform::SetEnv("TINYLAMB_TEMP", directory.c_str());
      QueryMemoryBudget::Global().ResetForTest(65536);
    }
    Restore(const Restore&) = delete;
    Restore& operator=(const Restore&) = delete;
    Restore(Restore&&) = delete;
    Restore& operator=(Restore&&) = delete;
    ~Restore() {
      QueryMemoryBudget::Global().ResetForTest(budget);
      if (temp) {
        platform::SetEnv("TINYLAMB_TEMP", temp->c_str());
      } else {
        platform::UnsetEnv("TINYLAMB_TEMP");
      }
      std::error_code error;
      std::filesystem::remove_all(directory, error);
    }
  } restore;
  std::vector<Row> probe;
  std::vector<Row> build;
  probe.reserve(10000);
  build.reserve(32);
  for (int64_t i = 0; i < 10000; ++i) {
    probe.emplace_back(Row({Value(i % 32), Value(std::string(128, 'x'))}));
  }
  for (int64_t i = 0; i < 32; ++i) {
    build.emplace_back(Row({Value(i)}));
  }
  HashJoin join(std::make_shared<ValuesExecutor>(std::move(probe)), {0},
                std::make_shared<ValuesExecutor>(std::move(build)), {0},
                HashJoinMode::kInMemory, 1);
  Row row;
  ASSERT_TRUE(join.Next(&row, nullptr));
  size_t files = 0;
  for (const auto& file :
       std::filesystem::directory_iterator(restore.directory)) {
    std::filesystem::remove(file.path());
    ++files;
  }
  ASSERT_GT(files, 0U);
  size_t emitted = 1;
  while (join.Next(&row, nullptr)) {
    ++emitted;
  }
  EXPECT_LT(emitted, 10000U);
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
}

TEST(AggregateAuditTest, TypedOverflowStopsWithoutReplayingTheBatch) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  for (bool floating : {false, true}) {
    const Expression argument =
        floating ? ConstantValueExp(Value(1e308)) : ColumnValueExp("v");
    AggregationExecutor aggregate(
        std::make_shared<ValuesExecutor>(
            std::vector<Row>{Row({Value(std::numeric_limits<int64_t>::max())}),
                             Row({Value(1)})}),
        schema,
        {NamedExpression("n", AggregateExpressionExp(AggregationType::kCount,
                                                     ColumnValueExp("*"))),
         NamedExpression(
             "s", AggregateExpressionExp(AggregationType::kSum, argument))});
    Row row;
    EXPECT_NO_THROW(EXPECT_FALSE(aggregate.Next(&row, nullptr)));
    EXPECT_NE(aggregate.GetStatus(), Status::kSuccess);
  }
}

TEST(AggregateAuditTest, MixedSignedUnsignedSumsKeepBothPartialTotals) {
  const auto sum =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"));
  for (bool reverse : {false, true}) {
    relational_detail::AggregateAccumulator sequential(
        &sum->AsAggregateExpression());
    relational_detail::AggregateAccumulator left(&sum->AsAggregateExpression());
    relational_detail::AggregateAccumulator right(
        &sum->AsAggregateExpression());
    const Value a = reverse ? Value(7).WithUnsigned() : Value(5);
    const Value b = reverse ? Value(5) : Value(7).WithUnsigned();
    ASSERT_EQ(sequential.Add(a), Status::kSuccess);
    ASSERT_EQ(sequential.Add(b), Status::kSuccess);
    ASSERT_EQ(left.Add(a), Status::kSuccess);
    ASSERT_EQ(right.Add(b), Status::kSuccess);
    ASSERT_EQ(left.Merge(right), Status::kSuccess);
    const auto sequential_result = sequential.TryFinish();
    const auto merged_result = left.TryFinish();
    ASSERT_TRUE(sequential_result.HasValue());
    ASSERT_TRUE(merged_result.HasValue());
    EXPECT_EQ(sequential_result.Value(), Value(12).WithUnsigned());
    EXPECT_EQ(merged_result.Value(), Value(12).WithUnsigned());
  }
}

TEST(AggregateAuditTest, ColumnSpellingDoesNotChangeSignedSumDomain) {
  for (const char* name : {"uint64_label", "x"}) {
    for (bool distinct : {false, true}) {
      const Schema schema("t", {Column(name, ValueType::kInt64)});
      auto sum = std::make_shared<AggregateExpression>(
          AggregationType::kSum, ColumnValueExp(name), distinct);
      relational_detail::AggregateAccumulator accumulator(sum.get(), &schema);
      ASSERT_EQ(accumulator.Add(Value(-1)), Status::kSuccess);
      EXPECT_EQ(accumulator.Add(Value(2)), Status::kSuccess);
      const auto result = accumulator.TryFinish();
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value(), Value(1));
      EXPECT_FALSE(result.Value().IsUnsigned());
    }
  }
}

TEST(AggregateAuditTest, ParallelSumOverflowReturnsStatus) {
  const Schema schema("s", {Column("v", ValueType::kInt64)});
  std::vector<Row> rows(8192, Row({Value(int64_t{1} << 50)}));
  ParallelAggregationExecutor aggregate(
      std::make_shared<ValuesExecutor>(rows), schema,
      {NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                     ColumnValueExp("v")))},
      2);
  DataChunk output;
  EXPECT_NO_THROW(EXPECT_EQ(aggregate.NextBatch(&output), 0U));
  EXPECT_EQ(aggregate.GetStatus(), Status::kIsInfinity);
}

TEST(ProjectionAuditTest, AffineFoldingPreservesIntermediateOverflow) {
  const Schema schema("s", {Column("v", ValueType::kInt64)});
  const int64_t max = std::numeric_limits<int64_t>::max();
  for (const int64_t outer : {max, int64_t{0}}) {
    SCOPED_TRACE(outer);
    const Expression expr = BinaryExpressionExp(
        BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kMultiply,
                            ConstantValueExp(Value(max))),
        BinaryOperation::kMultiply, ConstantValueExp(Value(outer)));
    // Promote the kernel on safe input, then cross the intermediate bound.
    std::vector<Row> rows(2048, Row({Value(0)}));
    rows.emplace_back(Row({Value(2)}));
    Projection projection({NamedExpression("p", expr)}, schema,
                          std::make_shared<ValuesExecutor>(rows), 1);
    DataChunk output;
    while (projection.NextBatch(&output) != 0) {
    }
    EXPECT_EQ(projection.GetStatus(),
              expr->TryEvaluate(Row({Value(2)}), schema).GetStatus());
  }
}

TEST(JoinAuditTest, BiasedKeysRequireBothSignedIntegerDomains) {
  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("l", "v")),
                          BinaryOperation::kAdd, ConstantValueExp(Value(5))),
      BinaryOperation::kEquals, ColumnValueExp(ColumnName("r", "v")));
  const Schema right("r", {Column("v", ValueType::kInt64)});
  Column unsigned_column("v", ValueType::kInt64);
  unsigned_column.SetUnsigned(true);
  for (const Column& column : {unsigned_column, Column("v", ValueType::kDouble),
                               Column("v", ValueType::kVarChar)}) {
    const Schema left("l", {column});
    EXPECT_TRUE(
        relational_detail::EqualityKeys(left, right, {predicate}).empty());
  }
  EXPECT_EQ(
      relational_detail::EqualityKeys(
          Schema("l", {Column("v", ValueType::kInt64)}), right, {predicate})
          .size(),
      1U);
}

class AuditPositionedRows final : public ExecutorBase {
 public:
  AuditPositionedRows(size_t count, page_id_t page)
      : count_(count), page_(page) {}
  size_t NextBatch(DataChunk* output,
                   size_t max_rows = kDefaultVectorSize) override {
    output->Reset();
    while (offset_ < count_ && output->Size() < max_rows) {
      output->Append(Row({Value(1)}),
                     RowPosition(page_, static_cast<slot_t>(offset_++)));
    }
    return output->Size();
  }
  void Dump(std::ostream& output, int /*indent*/) const override {
    output << "AuditPositionedRows";
  }

 private:
  size_t count_;
  page_id_t page_;
  size_t offset_ = 0;
};

TEST(JoinAuditTest, NullAwareAntiIncludesNullProbeForEmptyBuild) {
  const std::vector<Row> rows{Row({Value()}), Row({Value(7)})};
  SharedBuildParallelHashJoin join(
      std::make_shared<ValuesExecutor>(rows), {0},
      std::make_shared<ValuesExecutor>(std::vector<Row>{}), {0}, 2,
      JoinKind::kNullAwareAnti);
  std::vector<Row> result;
  Row row;
  while (join.Next(&row, nullptr)) {
    result.push_back(row);
  }
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  EXPECT_EQ(result, rows);
}

TEST(JoinAuditTest, SharedBuildKeepsLeftRowPositionsWhenBuildSideFlips) {
  for (const size_t left_count : {size_t{1}, size_t{100}}) {
    SharedBuildParallelHashJoin join(
        std::make_shared<AuditPositionedRows>(left_count, 11), {0},
        std::make_shared<AuditPositionedRows>(100, 22), {0}, 2,
        JoinKind::kInner);
    Row row;
    RowPosition position;
    size_t count = 0;
    while (join.Next(&row, &position)) {
      EXPECT_EQ(position.page_id, 11U);
      ++count;
    }
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    EXPECT_EQ(count, left_count * 100);
  }
}

class AuditThrowingRows final : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* /*output*/, size_t /*max_rows*/) override {
    throw std::runtime_error("injected worker allocation failure");
  }
  void Dump(std::ostream& output, int /*indent*/) const override {
    output << "AuditThrowingRows";
  }
};

TEST(JoinAuditTest, HashJoinConvertsExceptionsToStickyStatus) {
  HashJoin join(
      std::make_shared<ValuesExecutor>(std::vector<Row>{Row({Value(1)})}), {0},
      std::make_shared<AuditThrowingRows>(), {0});
  DataChunk output;
  EXPECT_NO_THROW(EXPECT_EQ(join.NextBatch(&output), 0U));
  EXPECT_EQ(join.GetStatus(), Status::kRuntimeError);
  EXPECT_NO_THROW(EXPECT_EQ(join.NextBatch(&output), 0U));
  EXPECT_EQ(join.GetStatus(), Status::kRuntimeError);
}

}  // namespace tinylamb
