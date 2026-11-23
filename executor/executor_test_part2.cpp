/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "executor/executor_test.hpp"

namespace tinylamb {
TEST_F(ExecutorTest, DecoratorForwardsInnerFailure) {
  class FailsImmediately final : public ExecutorBase {
   public:
    size_t NextBatch(DataChunk* dst, size_t /*max_rows*/) override {
      dst->Reset();
      FailWith(StatusError(StatusCode::kConflicts, "synthetic inner failure"));
      return 0;
    }
    void Dump(std::ostream& out, int /*indent*/) const override {
      out << "FailsImmediately";
    }
  };
  IndexSkipScanExecutor decorator(std::make_shared<FailsImmediately>());
  Row row;
  EXPECT_FALSE(decorator.Next(&row, nullptr));
  EXPECT_EQ(decorator.GetStatus(), Status::kConflicts);
  EXPECT_FALSE(decorator.ok());
}

TEST_F(ExecutorTest, ParallelAggregationRethrowsOnRetry) {
  // Regression (§6.4): after a worker failure the executor used to answer a
  // later Next() with a well-formed empty aggregate (COUNT=0 / SUM=NULL).
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("value"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(
      std::make_shared<FailingBatchExecutor>(3), schema, aggregates, 2);
  Row result;
  // A worker failure is latched: both the first Next() and the retry must
  // report the sticky error rather than a well-formed empty aggregate.
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
  EXPECT_NE(aggregate.GetStatus(), Status::kSuccess);
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelAggregationInt64FastPathMatchesSequential) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  // Values 1..100 with one interior and one trailing null: the columnar fast
  // path must skip nulls exactly like the generic per-row path.
  std::vector<Row> rows;
  rows.reserve(100);
  for (int64_t value = 1; value <= 100; ++value) {
    rows.push_back(Row({Value(value)}));
  }
  rows[56] = Row({Value()});
  rows.push_back(Row({Value()}));
  auto make_aggregates = [] {
    return std::vector<NamedExpression>{
        NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                      ColumnValueExp("value"))),
        NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                      ColumnValueExp("value"))),
        NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                      ColumnValueExp("value"))),
        NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                      ColumnValueExp("value"))),
        NamedExpression("count",
                        AggregateExpressionExp(AggregationType::kCount,
                                               ColumnValueExp("value"))),
        NamedExpression("count_star",
                        AggregateExpressionExp(AggregationType::kCount,
                                               ColumnValueExp("*"))),
        NamedExpression("sum_expr",
                        AggregateExpressionExp(
                            AggregationType::kSum,
                            BinaryExpressionExp(ColumnValueExp("value"),
                                                BinaryOperation::kAdd,
                                                ConstantValueExp(Value(1)))))};
  };

  AggregationExecutor sequential(std::make_shared<ConstantExecutor>(rows),
                                 schema, make_aggregates());
  Row expected;
  ASSERT_TRUE(sequential.Next(&expected, nullptr));

  ParallelAggregationExecutor parallel(std::make_shared<ConstantExecutor>(rows),
                                       schema, make_aggregates(), 4);
  Row actual;
  ASSERT_TRUE(parallel.Next(&actual, nullptr));

  // sum(1..100) - 57 = 4993 over 99 non-null values, 101 rows in total.
  ASSERT_EQ(expected.values_.size(), 7U);
  EXPECT_EQ(actual[0], Value(4993));
  EXPECT_EQ(actual[1], Value(1));
  EXPECT_EQ(actual[2], Value(100));
  EXPECT_DOUBLE_EQ(actual[3].value.double_value, 4993.0 / 99.0);
  EXPECT_EQ(actual[4], Value(99));
  EXPECT_EQ(actual[5], Value(101));
  EXPECT_EQ(actual[6], Value(4993 + 99));
  for (size_t column = 0; column < expected.values_.size(); ++column) {
    if (column == 3) {
      continue;
    }
    EXPECT_EQ(actual[column], expected[column]) << (column != 0U);
  }
  EXPECT_DOUBLE_EQ(actual[3].value.double_value,
                   expected[3].value.double_value);
}

// ===== Insert / Update =====
TEST_F(ExecutorTest, InsertWithRowPosition) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto insert = std::make_shared<Insert>(
      ctx.txn_, &*tbl,
      std::make_shared<ConstantExecutor>(
          std::vector<Row>{Row({Value(100), Value("new"), Value(1.0)})}));
  Row result;
  RowPosition rp;
  ASSERT_TRUE(insert->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Insert Rows"), Value(1)}));
  ASSERT_FALSE(insert->Next(&result, &rp));
}

TEST_F(ExecutorTest, UpdateWithRowPosition) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  std::vector<NamedExpression> update_rule = {
      NamedExpression("key", ColumnValueExp("key")),
      NamedExpression("name", ConstantValueExp(Value("changed"))),
      NamedExpression("score", ColumnValueExp("score"))};
  auto update = std::make_shared<Update>(
      ctx.txn_, &*tbl,
      std::make_shared<Projection>(update_rule, tbl->GetSchema(),
                                   std::make_shared<FullScan>(ctx.txn_, *tbl)));
  Row result;
  RowPosition rp;
  ASSERT_TRUE(update->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Update Rows"), Value(4)}));
  ASSERT_FALSE(update->Next(&result, &rp));
}

// ===== Sort / HashJoin =====
TEST_F(ExecutorTest, SortDumpAndSmallInput) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(2)}), Row({Value(1)}), Row({Value(2)})});
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("value"), true, std::nullopt}});
  Row row;
  RowPosition pos;
  ASSERT_TRUE(sort.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(sort.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_TRUE(sort.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(sort.Next(&row, nullptr));
  std::stringstream ss;
  sort.Dump(ss, 0);
  // "ParallelSort" only with >1 worker; a 1-worker sort dumps as "Sort (".
  EXPECT_NE(ss.str().find("Sort ("), std::string::npos) << ss.str();
}

TEST_F(ExecutorTest, HashJoinNextBatchAndDump) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("l")}), Row({Value(2), Value("l2")})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("r1")}), Row({Value(1), Value("r2")})});
  HashJoin join(left, {0}, right, {0});
  LOG(INFO) << join;
  DataChunk chunk;
  EXPECT_EQ(join.NextBatch(&chunk, 8), 2U);
  EXPECT_EQ(chunk.Size(), 2U);
  EXPECT_EQ(join.NextBatch(&chunk, 8), 0U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("PartitionedHashJoin"), std::string::npos);
}

TEST_F(ExecutorTest, HashJoinSpilledBuildKeepsNullProbeKeyOutOfAntiOutput) {
  // PRODUCTION BUG (fixed): with the build side spilled, the resident-probe
  // path used a bool match array, so a NULL probe key ("no match") was
  // emitted for anti joins although NULL NOT IN (...) is UNKNOWN.
  const size_t original = QueryMemoryBudget::Global().Limit();
  QueryMemoryBudget::Global().ResetForTest(8192);
  struct Restore {
    size_t value;
    explicit Restore(size_t v) : value(v) {}
    ~Restore() { QueryMemoryBudget::Global().ResetForTest(value); }
    Restore(const Restore&) = delete;
    Restore& operator=(const Restore&) = delete;
    Restore(Restore&&) = delete;
    Restore& operator=(Restore&&) = delete;
  } restore{original};

  // Build side needs enough rows to exceed the tiny budget.
  std::vector<Row> build_rows;
  build_rows.reserve(400);
  for (int64_t i = 0; i < 400; ++i) {
    build_rows.emplace_back(Row({Value(i)}));
  }
  auto build = std::make_shared<ConstantExecutor>(build_rows);
  // Probe: NULL, 399 (matches), 500 (no match). Only 500 may survive anti.
  auto probe = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value()}),
      Row({Value(int64_t{399})}),
      Row({Value(int64_t{500})}),
  });
  HashJoin join(probe, {0}, build, {0}, HashJoinMode::kInMemory,
                JoinKind::kNullAwareAnti);
  join.MaterializePipeline();
  std::vector<Row> out;
  Row row;
  RowPosition rp;
  while (join.Next(&row, &rp)) {
    out.push_back(row);
  }
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0][0], Value(int64_t{500}));
}

TEST_F(ExecutorTest, InMemoryShardedBuildKeepsNullBuildKeyOutOfMatches) {
  // PRODUCTION BUG (fixed): the pipelined in-memory join's sharded build
  // inserted NULL (invalid) keys as KeyRef{} -- int_key 0, hash 0 -- so a
  // probe key of 0 chained onto the NULL-keyed build row (SQL: no match).
  // The build path must respect the same contract as BuildSideIndex, which
  // already skipped invalid keys.
  // The smaller side always becomes the build side, so the NULL-keyed side
  // must be the small one.
  auto build = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(int64_t{0})}),
      Row({Value()}),  // NULL build key.
      Row({Value(int64_t{4999})}),
  });
  std::vector<Row> probe_rows;
  probe_rows.reserve(5000);
  for (int64_t i = 0; i < 5000; ++i) {
    probe_rows.emplace_back(Row({Value(i)}));
  }
  probe_rows.emplace_back(Row({Value(int64_t{12345})}));  // matches nothing.
  auto probe = std::make_shared<ConstantExecutor>(probe_rows);
  HashJoin join(probe, {0}, build, {0}, HashJoinMode::kInMemory,
                JoinKind::kInner, /*worker_count=*/8);
  std::vector<Row> out;
  Row row;
  RowPosition rp;
  while (join.Next(&row, &rp)) {
    out.push_back(row);
  }
  // Probe 0 matches the real 0 row; 4999 matches; 12345 matches nothing and
  // must not match the NULL build key.
  ASSERT_EQ(out.size(), 2U);
  EXPECT_EQ(out[0][0], Value(int64_t{0}));
  EXPECT_EQ(out[1][0], Value(int64_t{4999}));
}

TEST_F(ExecutorTest, HashJoinWithMultipleJoinColumnsDumpsAllKeys) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1), Value(10), Value("l")})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1), Value(10), Value("r")})});
  HashJoin join(left, {0, 1}, right, {0, 1});
  Row got;
  ASSERT_TRUE(join.Next(&got, nullptr));
  EXPECT_EQ(got, Row({Value(1), Value(10), Value("l"), Value(1), Value(10),
                      Value("r")}));
  ASSERT_FALSE(join.Next(&got, nullptr));
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("left: {0, 1}"), std::string::npos);
  EXPECT_NE(ss.str().find("right: {0, 1}"), std::string::npos);
}

// ===== LimitExecutor =====
TEST_F(ExecutorTest, LimitLessThanInputRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  LimitExecutor limit(input, 2, 0);
  Row row;
  RowPosition rp;
  ASSERT_TRUE(limit.Next(&row, &rp));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(limit.Next(&row, &rp));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitEqualToInputRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  LimitExecutor limit(input, 2, 0);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitGreaterThanInputRows) {
  auto input =
      std::make_shared<ConstantExecutor>(std::vector<Row>{Row({Value(1)})});
  LimitExecutor limit(input, 5, 0);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitZeroMeansNoLimit) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  LimitExecutor limit(input, 0, 0);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitOffsetSkipsLeadingRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  LimitExecutor limit(input, 1, 2);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(3)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitOffsetBeyondInputEmitsNothing) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  LimitExecutor limit(input, 5, 10);
  Row row;
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitOffsetWithEmptyInputEmitsNothing) {
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  LimitExecutor limit(input, 3, 1);
  Row row;
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitDump) {
  auto input = std::make_shared<SyntheticBatchExecutor>(5);
  LimitExecutor limit(input, 3, 1);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  std::stringstream ss;
  limit.Dump(ss, 0);
  EXPECT_NE(ss.str().find("Limit: 3 offset 1"), std::string::npos);
  EXPECT_NE(ss.str().find("SyntheticBatchExecutor"), std::string::npos);
}

TEST_F(ExecutorTest, ParallelHashJoinEmittedForLargeInputs) {
  Plan left =
      std::make_shared<ValuesPlan>(JoinLeftSchema(), ShuffledJoinLeft());
  Plan right =
      std::make_shared<ValuesPlan>(JoinRightSchema(), SortedJoinRight());
  ProductPlan join(left, {ColumnName("l", "k")}, right, {ColumnName("r", "k")});
  TransactionContext ctx = rs_->BeginContext();
  Executor executor = join.EmitExecutor(ctx);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("SharedBuildParallelHashJoin"), std::string::npos)
      << dump.str();
  // 80 left rows per key x 2 right rows per key x 100 keys.
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row.values_.size(), 4U);
    EXPECT_EQ(row[0], row[2]);
    ++rows;
  }
  EXPECT_EQ(rows, 16000U);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, StaleRowEstimatesCannotChangeMixedSignednessJoinResults) {
  TransactionContext setup = rs_->BeginContext();
  Column unsigned_key("k", ValueType::kInt64);
  unsigned_key.SetUnsigned(true);
  ASSIGN_OR_ASSERT_FAIL(
      Table, signed_table,
      rs_->CreateTable(
          setup, Schema("signed_keys", {Column("k", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, unsigned_table,
      rs_->CreateTable(setup, Schema("unsigned_keys", {unsigned_key})));
  for (const int64_t value : {int64_t{-1}, int64_t{5}, int64_t{6}}) {
    ASSERT_SUCCESS(
        signed_table.Insert(setup.txn_, Row({Value(value)})).GetStatus());
  }
  for (const int64_t bits : {int64_t{5}, int64_t{-1}}) {
    ASSERT_SUCCESS(
        unsigned_table.Insert(setup.txn_, Row({Value(bits).WithUnsigned()}))
            .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  // Actual cardinality picks the byte-key build first, then the signed-int
  // build after adding unmatched unsigned rows. Estimates independently pick
  // serial vs parallel execution over exactly the same visible rows.
  for (const bool larger_unsigned : {false, true}) {
    if (larger_unsigned) {
      TransactionContext write = rs_->BeginContext();
      for (const int64_t value : {int64_t{100}, int64_t{101}}) {
        ASSERT_SUCCESS(
            unsigned_table
                .Insert(write.txn_, Row({Value(value).WithUnsigned()}))
                .GetStatus());
      }
      ASSERT_SUCCESS(write.PreCommit());
    }
    for (const size_t estimate : {size_t{3}, size_t{8192}}) {
      SCOPED_TRACE(::testing::Message() << larger_unsigned << "," << estimate);
      TransactionContext context = rs_->BeginContext();
      Plan left = std::make_shared<FullScanPlan>(
          signed_table,
          TableStatistics(signed_table.GetSchema()).ScaleToRows(estimate));
      Plan right = std::make_shared<FullScanPlan>(
          unsigned_table,
          TableStatistics(unsigned_table.GetSchema()).ScaleToRows(estimate));
      const ProductPlan join(left, {ColumnName("signed_keys", "k")}, right,
                             {ColumnName("unsigned_keys", "k")});
      const Executor executor = join.EmitExecutor(context);
      std::ostringstream dump;
      executor->Dump(dump, 0);
      EXPECT_EQ(
          dump.str().find("SharedBuildParallelHashJoin") != std::string::npos,
          estimate == 8192);
      std::vector<Row> rows;
      Row row;
      while (executor->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
      EXPECT_EQ(rows,
                (std::vector<Row>{Row({Value(5), Value(5).WithUnsigned()})}));
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
}

TEST_F(ExecutorTest, SequentialHashJoinEmittedForSmallInputs) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 100; ++i) {
    left_rows.emplace_back(Row({Value(i), Value(i)}));
    right_rows.emplace_back(Row({Value(i), Value(i * 2)}));
  }
  Plan left = std::make_shared<ValuesPlan>(JoinLeftSchema(), left_rows);
  Plan right = std::make_shared<ValuesPlan>(JoinRightSchema(), right_rows);
  ProductPlan join(left, {ColumnName("l", "k")}, right, {ColumnName("r", "k")});
  TransactionContext ctx = rs_->BeginContext();
  Executor executor = join.EmitExecutor(ctx);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_EQ(dump.str().find("SharedBuildParallelHashJoin"), std::string::npos)
      << dump.str();
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 100U);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, ParallelMergeJoinEmittedForLargeSortedInputs) {
  Plan left = std::make_shared<ValuesPlan>(JoinLeftSchema(), SortedJoinLeft());
  Plan right =
      std::make_shared<ValuesPlan>(JoinRightSchema(), SortedJoinRight());
  MergeJoinPlan join(left, {ColumnName("l", "k")}, right,
                     {ColumnName("r", "k")});
  TransactionContext ctx = rs_->BeginContext();
  Executor executor = join.EmitExecutor(ctx);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("ParallelMergeJoin"), std::string::npos)
      << dump.str();
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row.values_.size(), 4U);
    EXPECT_EQ(row[0], row[2]);
    ++rows;
  }
  EXPECT_EQ(rows, 16000U);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, SequentialMergeJoinEmittedForSmallInputs) {
  std::vector<Row> left_rows;
  left_rows.reserve(100);
  std::vector<Row> right_rows;
  right_rows.reserve(100);
  for (int64_t i = 0; i < 100; ++i) {
    left_rows.emplace_back(Row({Value(i), Value(i)}));
    right_rows.emplace_back(Row({Value(i), Value(i * 2)}));
  }
  Plan left = std::make_shared<ValuesPlan>(JoinLeftSchema(), left_rows);
  Plan right = std::make_shared<ValuesPlan>(JoinRightSchema(), right_rows);
  MergeJoinPlan join(left, {ColumnName("l", "k")}, right,
                     {ColumnName("r", "k")});
  TransactionContext ctx = rs_->BeginContext();
  Executor executor = join.EmitExecutor(ctx);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_EQ(dump.str().find("ParallelMergeJoin"), std::string::npos)
      << dump.str();
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 100U);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(ExecutorTest, RelationalTruthinessOfBareColumns) {
  // name (varchar), score (double) and key (int64) are tested for truthiness
  // directly inside the relational predicate evaluator (relational.cpp Truthy).
  const auto rows = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE (name AND score AND key) OR "
      "key = 5 ORDER BY key;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
  EXPECT_EQ(rows[2][0], Value(3));
}

TEST_F(ExecutorTest, RelationalNotEqualsAndLikePredicates) {
  // kNotEquals on numeric operands and LIKE with a trailing '%'.
  const auto not_equals = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key != 2 OR key = 5 ORDER BY "
      "key;");
  ASSERT_EQ(not_equals.size(), 3U);
  EXPECT_EQ(not_equals[0][0], Value(0));
  EXPECT_EQ(not_equals[1][0], Value(1));
  EXPECT_EQ(not_equals[2][0], Value(3));

  const auto like = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE name LIKE 'hello%' OR key = 3 "
      "ORDER BY key;");
  ASSERT_EQ(like.size(), 2U);
  EXPECT_EQ(like[0][0], Value(0));
  EXPECT_EQ(like[1][0], Value(3));
}

TEST_F(ExecutorTest, RelationalStringConcatenationWithPlus) {
  // '+' on two VARCHAR operands concatenates in the relational evaluator.
  const auto rows = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key = 1 OR name + name = "
      "'hellohello' ORDER BY key;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(0));
  EXPECT_EQ(rows[1][0], Value(1));
}

TEST_F(ExecutorTest, RelationalUnaryOperators) {
  const auto minus = RelationalRun(
      *rs_, "SELECT -key, -score FROM SampleTable WHERE key = 1 OR key = 2;");
  ASSERT_EQ(minus.size(), 2U);
  EXPECT_EQ(minus[0][0], Value(-1));
  EXPECT_EQ(minus[0][1], Value(-4.9));
  EXPECT_EQ(minus[1][0], Value(-2));
  EXPECT_EQ(minus[1][1], Value(-4.14));

  const auto is_null = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key IS NULL OR key IS NOT "
      "NULL;");
  EXPECT_EQ(is_null.size(), 4U);

  const auto not_expr = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE NOT (key = 1) OR key = 2 "
      "ORDER BY key;");
  ASSERT_EQ(not_expr.size(), 3U);
  EXPECT_EQ(not_expr[0][0], Value(0));
  EXPECT_EQ(not_expr[1][0], Value(2));
  EXPECT_EQ(not_expr[2][0], Value(3));
}

TEST_F(ExecutorTest, RelationalSortMixedSignDescendingKeepsOrder) {
  // Regression: the radix-sort fast path encoded DESC keys by flipping all
  // bits of the raw two's-complement value, which kept negative keys below
  // positive ones and produced a scrambled order for mixed-sign INT64.
  const auto descending = RelationalRun(
      *rs_,
      "SELECT key FROM (SELECT -1 AS key UNION ALL SELECT 13 "
      "UNION ALL SELECT -3 UNION ALL SELECT 2 UNION ALL SELECT 0) "
      "ORDER BY key DESC;");
  ASSERT_EQ(descending.size(), 5U);
  EXPECT_EQ(descending[0][0], Value(13));
  EXPECT_EQ(descending[1][0], Value(2));
  EXPECT_EQ(descending[2][0], Value(0));
  EXPECT_EQ(descending[3][0], Value(-1));
  EXPECT_EQ(descending[4][0], Value(-3));
}

TEST_F(ExecutorTest, RelationalAggregateInUnaryAndCase) {
  const auto negated =
      RelationalRun(*rs_, "SELECT -COUNT(*) FROM SampleTable;");
  ASSERT_EQ(negated.size(), 1U);
  EXPECT_EQ(negated[0][0], Value(-4));

  const auto conditional = RelationalRun(
      *rs_,
      "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM SampleTable;");
  ASSERT_EQ(conditional.size(), 1U);
  EXPECT_EQ(conditional[0][0], Value(1));

  // Aggregate in the CASE else-clause (when-clauses carry no aggregate).
  const auto aggregate_else =
      RelationalRun(*rs_,
                    "SELECT CASE WHEN key > 0 THEN 1 ELSE COUNT(*) END FROM "
                    "SampleTable;");
  ASSERT_EQ(aggregate_else.size(), 1U);
  EXPECT_EQ(aggregate_else[0][0], Value(4));
}

TEST_F(ExecutorTest, RelationalScalarFunctionsInGroupedQuery) {
  const auto coalesce = RelationalRun(
      *rs_, "SELECT COALESCE(NULL, key) AS c FROM SampleTable GROUP BY key;");
  ASSERT_EQ(coalesce.size(), 4U);

  const auto coalesce_all_null = RelationalRun(
      *rs_, "SELECT COALESCE(NULL, NULL) AS c FROM SampleTable GROUP BY key;");
  ASSERT_EQ(coalesce_all_null.size(), 4U);
  EXPECT_TRUE(coalesce_all_null[0][0].IsNull());

  const auto concat = RelationalRun(
      *rs_, "SELECT CONCAT('a', NULL, 'b') FROM SampleTable GROUP BY key;");
  ASSERT_EQ(concat.size(), 4U);
  EXPECT_TRUE(concat[0][0].IsNull());

  const auto now = RelationalRun(
      *rs_, "SELECT CURRENT_TIMESTAMP() FROM SampleTable GROUP BY key;");
  ASSERT_EQ(now.size(), 4U);
  EXPECT_EQ(now[0][0].type, ValueType::kVarChar);
  EXPECT_FALSE(now[0][0].IsNull());
}

TEST_F(ExecutorTest, RelationalDateFunctions) {
  const auto add =
      RelationalRun(*rs_, "SELECT DATE_ADD('2026-01-01', INTERVAL 3 DAY);");
  ASSERT_EQ(add.size(), 1U);
  EXPECT_EQ(add[0][0], Value("2026-01-04"));

  const auto extract = RelationalRun(
      *rs_,
      "SELECT EXTRACT(YEAR FROM '2026-08-21'), EXTRACT(MONTH FROM "
      "'2026-08-21'), EXTRACT(DAY FROM '2026-08-21');");
  ASSERT_EQ(extract.size(), 1U);
  EXPECT_EQ(extract[0][0], Value(2026));
  EXPECT_EQ(extract[0][1], Value(8));
  EXPECT_EQ(extract[0][2], Value(21));
}

TEST_F(ExecutorTest, RelationalCorrelatedExistsBuildsIndex) {
  // The correlated equality `o.key = key` drives the per-subquery correlated
  // index (local key on the right-hand side of the equality).
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.key FROM SampleTable AS o WHERE EXISTS (SELECT 1 FROM "
      "SampleTable WHERE o.key = key AND key > 1) ORDER BY o.key;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[1][0], Value(3));
}

TEST_F(ExecutorTest, RelationalCorrelatedExistsCacheHits) {
  // A cross join repeats every outer key 4 times; only the first evaluation
  // per key builds the correlated index, the rest must hit the result cache.
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.key FROM SampleTable AS o CROSS JOIN SampleTable AS b "
      "WHERE EXISTS (SELECT COUNT(*) FROM SampleTable WHERE o.key = key "
      "HAVING COUNT(*) > 0) ORDER BY o.key;");
  ASSERT_EQ(rows.size(), 16U);
  // Every outer key survives (EXISTS is true for all four), repeated once
  // per cross-join partner: a cache that served a stale key would show up
  // as a wrong value or a missing multiplicity here.
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i][0], Value(static_cast<int64_t>(i / 4)));
  }
}

TEST_F(ExecutorTest, RelationalInnerAndLeftJoins) {
  // Inner join without an equality key uses the nested-loop fallback.
  const auto inner = RelationalRun(
      *rs_,
      "SELECT a.key FROM SampleTable AS a JOIN SampleTable AS b ON "
      "a.key > b.key ORDER BY a.key;");
  ASSERT_EQ(inner.size(), 6U);
  // Left join without an equality key emits unmatched rows with NULLs.
  const auto left = RelationalRun(
      *rs_,
      "SELECT a.key FROM SampleTable AS a LEFT JOIN SampleTable AS b ON "
      "a.key > b.key ORDER BY a.key;");
  ASSERT_EQ(left.size(), 7U);
  int64_t previous = -1;
  for (const Row& row : left) {
    EXPECT_GE(row[0].value.int_value, previous);
    previous = row[0].value.int_value;
  }
}

TEST_F(ExecutorTest, RelationalDistinctDeduplicatesJoinedRows) {
  const auto rows = RelationalRun(
      *rs_,
      "SELECT DISTINCT a.key FROM SampleTable AS a CROSS JOIN SampleTable "
      "AS b WHERE a.key = 1 OR a.key = 2;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
}

TEST_F(ExecutorTest, RelationalSortAndLimitOffset) {
  const auto sorted = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key = 1 OR key = 2 ORDER BY "
      "key = 99;");
  ASSERT_EQ(sorted.size(), 2U);

  const auto limited = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key = 1 OR key = 2 LIMIT 1 "
      "OFFSET 1;");
  ASSERT_EQ(limited.size(), 1U);
  EXPECT_EQ(limited[0][0], Value(2));
}

TEST_F(ExecutorTest, RelationalInSubqueriesAndScalarFallback) {
  const auto uncorrelated =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE key IN (SELECT key FROM "
                    "SampleTable ORDER BY key);");
  EXPECT_EQ(uncorrelated.size(), 4U);

  // A multi-column IN subquery is rejected by both routes with the same
  // pinned message (sql_subquery_shapes_coverage_test pins the default
  // route); the relational interpreter must not silently return 0 rows.
  ExpectMessageContains(
      RelationalThrow(*rs_,
                      "SELECT key FROM SampleTable WHERE key IN "
                      "(SELECT * FROM (SELECT * FROM SampleTable));"),
      "Subquery of type IN must have only one output column");

  // Scalar subquery that is neither correlated-indexable nor cacheable must
  // fall back to a full ExecuteQuery.  LIMIT 1 keeps the scalar subquery
  // within its one-row contract.
  const auto scalar = RelationalRun(
      *rs_,
      "SELECT (SELECT * FROM (SELECT * FROM SampleTable) LIMIT 1) FROM "
      "SampleTable;");
  ASSERT_EQ(scalar.size(), 4U);
  for (const Row& row : scalar) {
    EXPECT_EQ(row[0], Value(0));
  }

  // A subquery whose JOIN condition references an outer-column qualifier is
  // rejected as uncorrelated by StatementUsesOnlyScopes; the unresolvable join
  // predicate is then dropped, leaving a cross join, so IN tests only match
  // the returned constant.
  const auto cross =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE key IN (SELECT 1 FROM "
                    "SampleTable AS s JOIN SampleTable AS s2 ON s2.key = "
                    "SampleTable.key);");
  ASSERT_EQ(cross.size(), 1U);
  EXPECT_EQ(cross[0][0], Value(1));
}

TEST_F(ExecutorTest, RelationalScalarSubqueryReturnsNullWhenEmpty) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT (SELECT key FROM SampleTable WHERE key = 99) FROM "
                    "SampleTable;");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_TRUE(rows[0][0].IsNull());
}

TEST_F(ExecutorTest, RelationalIntervalAndNoFromQueries) {
  const auto interval = RelationalRun(*rs_, "SELECT INTERVAL 3 DAY;");
  ASSERT_EQ(interval.size(), 1U);
  EXPECT_EQ(interval[0][0], Value("0-0 3 0:0:0").WithInterval());

  const auto boolean = RelationalRun(*rs_, "SELECT 1 OR 1;");
  ASSERT_EQ(boolean.size(), 1U);
  EXPECT_EQ(boolean[0][0], Value(1));
}

TEST_F(ExecutorTest, RelationalDdlAndDmlRoundTrip) {
  RelationalRun(*rs_, "CREATE TABLE RoundTrip (id INT64, label STRING);");
  RelationalRun(*rs_, "INSERT INTO RoundTrip VALUES (1, 'one'), (2, 'two');");
  RelationalRun(*rs_, "UPDATE RoundTrip SET label = 'uno' WHERE id = 1;");
  RelationalRun(*rs_, "DELETE FROM RoundTrip WHERE id = 2;");
  const auto rows = RelationalRun(*rs_, "SELECT id, label FROM RoundTrip;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[0][1], Value("uno"));
}

TEST_F(ExecutorTest, RelationalExplainPlans) {
  auto explain = [&](std::string_view sql) {
    std::string text;
    for (const Row& row :
         RelationalRun(*rs_, std::string("EXPLAIN ") + std::string(sql))) {
      text += row[0].AsString();
      text += '\n';
    }
    return text;
  };

  const std::string derived =
      explain("SELECT * FROM (SELECT * FROM SampleTable);");
  EXPECT_NE(derived.find("Relational Physical Plan"), std::string::npos)
      << derived;
  EXPECT_NE(derived.find("SubqueryScan"), std::string::npos) << derived;

  const std::string aliased = explain("SELECT * FROM SampleTable AS t;");
  EXPECT_NE(aliased.find("AS t"), std::string::npos) << aliased;

  const std::string inner = explain(
      "SELECT * FROM SampleTable AS a JOIN SampleTable AS b ON a.key = b.key "
      "WHERE EXISTS (SELECT 1 FROM SampleTable);");
  EXPECT_TRUE(inner.find("SemiHashJoin") != std::string::npos ||
              inner.find("HashJoin") != std::string::npos ||
              inner.find("HybridHashJoin") != std::string::npos)
      << inner;

  const std::string left = explain(
      "SELECT a.key FROM SampleTable AS a LEFT JOIN SampleTable AS b ON "
      "a.key = b.key;");
  // LEFT JOIN with unused right side may be eliminated.
  // If present, it should carry type=left (relational) or a LeftHashJoin /
  // Left Outer Join node (M6 Cascades route).
  if (left.find("type=left") == std::string::npos &&
      left.find("LeftHashJoin") == std::string::npos &&
      left.find("Left Outer Join") == std::string::npos) {
    EXPECT_NE(left.find("SeqScan SampleTable AS a"), std::string::npos) << left;
  }

  const std::string limited = explain(
      "SELECT key FROM SampleTable WHERE key = 1 OR key = 2 LIMIT 3 OFFSET 1;");
  EXPECT_NE(limited.find("Limit count=3"), std::string::npos) << limited;
  EXPECT_NE(limited.find("offset=1"), std::string::npos) << limited;
}

TEST_F(ExecutorTest, RelationalAggregateOnVarcharThrows) {
  ExpectMessageContains(
      RelationalThrow(*rs_,
                      "SELECT name, SUM(name) AS s FROM SampleTable "
                      "GROUP BY name;"),
      "numeric value required");
}

TEST_F(ExecutorTest, RelationalAmbiguousColumnThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT key FROM SampleTable AS a "
                                        "JOIN SampleTable AS b ON "
                                        "a.key = b.key;"),
                        "ambiguous column key");
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT a.key FROM SampleTable AS "
                                        "a JOIN SampleTable AS b ON "
                                        "a.key = b.key WHERE key > 1;"),
                        "ambiguous column key");
}

TEST_F(ExecutorTest, RelationalUnknownColumnThrows) {
  ExpectMessageContains(
      RelationalThrow(*rs_, "SELECT (SELECT nope) FROM SampleTable;"),
      "column nope not found");
}

TEST_F(ExecutorTest, RelationalLikeNonStringOperandThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT key FROM SampleTable WHERE "
                                        "key LIKE '5' OR key = 1;"),
                        "LIKE requires string operands");
}

TEST_F(ExecutorTest, RelationalAggregateInWhereThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT key FROM SampleTable WHERE "
                                        "COUNT(*) > 0;"),
                        "aggregate outside grouping");
}

TEST_F(ExecutorTest, RelationalMissingTableThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT key FROM nonexistent JOIN "
                                        "SampleTable ON TRUE;"),
                        "table nonexistent not found");
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT (SELECT x FROM missing) "
                                        "FROM SampleTable;"),
                        "table missing not found");
}

TEST_F(ExecutorTest, RelationalUnsupportedFunctionThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT FOOBAR(1) FROM SampleTable "
                                        "GROUP BY key;"),
                        "unsupported function foobar");
}

TEST_F(ExecutorTest, RelationalDateAddArityThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT DATE_ADD('2026-01-01', "
                                        "5);"),
                        "DATE_ADD/DATE_SUB arity");
}

TEST_F(ExecutorTest, RelationalDatePlusDateThrows) {
  RelationalRun(*rs_, "CREATE TABLE RelDate (d DATE, v INT64);");
  RelationalRun(*rs_, "INSERT INTO RelDate VALUES (date '2026-01-01', 1);");
  RelationalRun(*rs_, "INSERT INTO RelDate VALUES (date '2026-06-01', 2);");
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT v FROM RelDate WHERE v = 1 "
                                        "OR d + d = 'x';"),
                        "unsupported binary operation");
}

TEST_F(ExecutorTest, RelationalCorrelatedSubqueryOverNullOuterDocumentsBug) {
  // A correlated EXISTS whose outer column is NULL must evaluate to UNKNOWN,
  // not attempt to encode NULL in the correlated-subquery cache key and
  // crash.  The LEFT JOIN below null-extends b for every outer row, so the
  // EXISTS predicate rejects every row.
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.key FROM SampleTable AS o LEFT JOIN SampleTable AS b ON "
      "o.key = b.key AND b.key = 99 WHERE EXISTS (SELECT 1 FROM "
      "SampleTable WHERE b.key = key);");
  EXPECT_TRUE(rows.empty());
}

TEST_F(ExecutorTest,
       RelationalInnerJoinOnRejectsUnresolvableAndAmbiguousColumns) {
  RelationalRun(*rs_, "CREATE TABLE tree (id INT64, parent INT64, w DOUBLE);");
  RelationalRun(*rs_,
                "INSERT INTO tree VALUES (1, NULL, 1.0), (2, 1, 2.5), "
                "(3, 1, 2.5), (4, 2, NULL), (5, 5, 0.0);");

  auto exec_status = [&](std::string_view sql, bool force_relational) {
    TransactionContext ctx = rs_->BeginContext();
    SqlEngine engine(*rs_);
    engine.SetForceRelationalFallback(force_relational);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    if (!prepared.HasValue()) {
      EXPECT_SUCCESS(ctx.Abort());
      return prepared.GetStatus();
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    const Status st = prepared.Value()->GetStatus();
    EXPECT_SUCCESS(ctx.Abort());
    return st;
  };

  for (const char* sql : {
           "SELECT c.id, p.id FROM tree c JOIN tree p ON w.parent = p.id "
           "ORDER BY c.id;",
           "SELECT c.id, p.id FROM (SELECT id, parent, w FROM tree) c JOIN "
           "(SELECT id, parent FROM tree) p ON w.parent = p.id ORDER BY c.id;",
           "SELECT c.id, p.id FROM tree c JOIN tree p ON unknown_alias.parent "
           "= p.id ORDER BY c.id;",
           "SELECT c.id, p.id FROM tree c JOIN tree p ON id = p.id "
           "ORDER BY c.id;",
           "SELECT c.id, p.id FROM (SELECT id, parent, w FROM tree "
           "WHERE FALSE) c JOIN tree p ON w.parent = p.id;",
       }) {
    SCOPED_TRACE(sql);
    EXPECT_NE(exec_status(sql, true), Status::kSuccess);
    EXPECT_NE(exec_status(sql, false), Status::kSuccess);
  }

  // Even on empty base tables (0 rows), unresolved ON qualifiers must fail on
  // both default and relational routes.
  RelationalRun(*rs_,
                "CREATE TABLE empty_tree (id INT64, parent INT64, w DOUBLE);");
  for (const char* sql : {
           "SELECT c.id, p.id FROM empty_tree c JOIN empty_tree p "
           "ON w.parent = p.id;",
           "SELECT c.id, p.id FROM empty_tree c JOIN empty_tree p "
           "ON unknown_alias.parent = p.id;",
           "SELECT c.id, p.id FROM empty_tree c JOIN empty_tree p "
           "ON id = p.id;",
       }) {
    SCOPED_TRACE(sql);
    EXPECT_NE(exec_status(sql, true), Status::kSuccess);
    EXPECT_NE(exec_status(sql, false), Status::kSuccess);
  }
}

TEST_F(ExecutorTest, RelationalInnerJoinOnEvaluatesCorrelatedOuterColumn) {
  RelationalRun(*rs_,
                "CREATE TABLE corr_tree (id INT64, parent INT64, w DOUBLE);");
  RelationalRun(*rs_,
                "INSERT INTO corr_tree VALUES (1, NULL, 1.0), (2, 1, 2.5), "
                "(3, 2, 2.5), (4, 99, 0.0);");
  // An inner-join ON referencing an outer qualifier must evaluate against the
  // outer scope rather than being skipped as unresolvable in inner relations.
  const std::vector<Row> rows = RelationalRun(
      *rs_,
      "SELECT w.id FROM corr_tree w WHERE EXISTS ("
      "SELECT 1 FROM corr_tree c JOIN corr_tree p ON w.parent = p.id "
      "AND c.id = 1) ORDER BY w.id;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[1][0], Value(3));
}

TEST_F(ExecutorTest, AggregationExpressionChildIsEvaluatedPerRow) {
  // Arrange: the aggregate child is an expression (not a plain column), so the
  // executor materializes each row and evaluates it via the expression.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum_plus_one",
      AggregateExpressionExp(
          AggregationType::kSum,
          BinaryExpressionExp(ColumnValueExp("value"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1)))))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: (1+1)+(2+1)+(3+1) = 9
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(9));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationMissingColumnThrows) {
  // Arrange: the aggregate references a column that is not in the input schema.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)})};
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum_missing",
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("nope")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: the missing column surfaces as a sticky executor error.
  Row result;
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
  EXPECT_NE(aggregate.GetStatus(), Status::kSuccess);
}

TEST_F(ExecutorTest, AggregationAverageOverIntColumn) {
  // Arrange: AVG over an INT64 column must convert each value to double.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "avg",
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: (1+2+3)/3 = 2.0
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(2.0));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationDumpListsAggregates) {
  // Arrange: an aggregation over a two-row constant input.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)})};
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "count", AggregateExpressionExp(AggregationType::kCount,
                                      ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act: dump the executor and then consume its single result row.
  std::stringstream ss;
  aggregate.Dump(ss, 0);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));

  // Assert: the dump names the executor and the aggregate; the count is 2.
  EXPECT_NE(ss.str().find("Aggregate strategy=scalar"), std::string::npos);
  EXPECT_NE(ss.str().find("count"), std::string::npos);
  EXPECT_EQ(result[0], Value(2));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, SortWithThrowingKeyExpressionRethrows) {
  // Arrange: the sort key references a column missing from the schema, so the
  // comparator throws inside the sort worker and must be rethrown.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(2)}), Row({Value(1)})});
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("nope"), true, std::nullopt}});

  // Act + Assert: materializing the sort records the worker failure as a
  // sticky executor status.
  Row row;
  RowPosition pos;
  EXPECT_FALSE(sort.Next(&row, &pos));
  EXPECT_NE(sort.GetStatus(), Status::kSuccess);
}

TEST_F(ExecutorTest, AggregationJitEligibleSumWithoutThresholdUsesFallback) {
  // Arrange: a single INT64 SUM aggregate is JIT-eligible, but with a row
  // count below the threshold the JIT is never compiled and every batch (one
  // of which carries a NULL) must fall back to the row-wise accumulator.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value()}),
                        Row({Value(4)})};
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum",
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: the NULL is skipped and the remaining values sum to 7.
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(7));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationCountStarCountsNullRows) {
  // Arrange: COUNT(*) must count every row, including rows whose value column
  // is NULL.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value()}), Row({Value(3)})};
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "count_star",
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: all three rows are counted.
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(3));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationAverageOverEmptyInputIsNull) {
  // Arrange: AVG over an empty input has no values to average.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "avg",
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::vector<Row>{}), schema,
      std::move(aggregates));

  // Act + Assert: the result is NULL rather than zero.
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_TRUE(result[0].IsNull());
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

// ===== RelationalExecutor spill-to-disk coverage =====

TEST_F(ExecutorTest, RelationalIdenticalUncorrelatedSubqueriesShareResult) {
  // The two QueryExpression nodes are allocated independently by the SQL
  // visitor. Structural caching must nevertheless execute the same scalar
  // subquery once and serve the second occurrence from the shared result.
  const std::string plan = RelationalExplain(
      *rs_,
      "SELECT key, (SELECT MAX(key) FROM SampleTable) AS first_value, "
      "(SELECT MAX(key) FROM SampleTable) AS second_value "
      "FROM SampleTable ORDER BY key;",
      /*analyze=*/true);
  EXPECT_NE(plan.find("ScalarSubquery executions=1"), std::string::npos)
      << plan;
  EXPECT_GT(StatsValue(plan, "uncorrelated_cache_hits="), 0) << plan;
}

TEST_F(ExecutorTest, RelationalCorrelatedDerivedSubqueryUsesApplyCache) {
  // A derived source prevents the specialized single-table equality index,
  // but repeated outer parameter tuples are still safe to cache. The cross
  // join evaluates each key four times, so later probes reuse the result.
  const std::string query =
      "SELECT o.key FROM SampleTable AS o CROSS JOIN SampleTable AS b "
      "WHERE (SELECT COUNT(*) FROM (SELECT key FROM SampleTable) AS i "
      "WHERE i.key = o.key) > 0 ORDER BY o.key;";
  const auto rows = RelationalRun(*rs_, query);
  ASSERT_EQ(rows.size(), 16U);
  // Same shape as RelationalCorrelatedExistsCacheHits: each outer key must
  // appear exactly four times (one per cross-join partner), not merely stay
  // inside the key domain.
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i][0], Value(static_cast<int64_t>(i / 4)));
  }
  const std::string plan = RelationalExplain(*rs_, query, true);
  EXPECT_GT(StatsValue(plan, "correlated_result_cache_hits="), 0) << plan;
}

TEST_F(ExecutorTest, RelationalCorrelatedAggregatePreservesTypedKeys) {
  RelationalRun(*rs_, "CREATE TABLE TypedDateKeys(d DATE)");
  RelationalRun(*rs_,
                "INSERT INTO TypedDateKeys VALUES (DATE '2026-01-01'), "
                "(DATE '2026-01-01'), (DATE '2026-01-02')");
  const std::string date_query =
      "SELECT o.d, (SELECT COUNT(*) FROM TypedDateKeys i WHERE i.d = o.d) "
      "FROM TypedDateKeys o ORDER BY o.d";
  const auto date_rows = RelationalRun(*rs_, date_query);
  ASSERT_EQ(date_rows.size(), 3U);
  EXPECT_EQ(date_rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(date_rows[1][1], Value(int64_t{2}));
  EXPECT_EQ(date_rows[2][1], Value(int64_t{1}));
  const std::string date_plan = RelationalExplain(*rs_, date_query, true);
  EXPECT_GT(StatsValue(date_plan, "correlated_result_cache_hits="), 0)
      << date_plan;

  RelationalRun(*rs_, "CREATE TABLE TypedUnsignedKeys(k UINT64)");
  RelationalRun(*rs_, "INSERT INTO TypedUnsignedKeys VALUES (1), (1), (2)");
  const std::string unsigned_query =
      "SELECT o.k, (SELECT COUNT(*) FROM TypedUnsignedKeys i WHERE i.k = o.k) "
      "FROM TypedUnsignedKeys o ORDER BY o.k";
  const auto unsigned_rows = RelationalRun(*rs_, unsigned_query);
  ASSERT_EQ(unsigned_rows.size(), 3U);
  EXPECT_EQ(unsigned_rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(unsigned_rows[1][1], Value(int64_t{2}));
  EXPECT_EQ(unsigned_rows[2][1], Value(int64_t{1}));
  const std::string unsigned_plan =
      RelationalExplain(*rs_, unsigned_query, true);
  EXPECT_GT(StatsValue(unsigned_plan, "correlated_result_cache_hits="), 0)
      << unsigned_plan;
}

// The keyless aggregate preagg yields exactly one row, so LIMIT 0 and
// OFFSET > 0 demand an empty scalar the cached single row cannot express.
// The preagg gate must refuse such statements (same no-LIMIT/no-OFFSET
// shape as the scalar variant), leaving LIMIT/OFFSET to the executor.
TEST_F(ExecutorTest, RelationalCorrelatedAggregateCacheRespectsLimitOffset) {
  RelationalRun(*rs_, "CREATE TABLE LimitAggOuter(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE LimitAggInner(k INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO LimitAggOuter VALUES (1)");
  RelationalRun(*rs_, "INSERT INTO LimitAggInner VALUES (1, 2), (1, 3)");
  EXPECT_TRUE(RelationalRun(*rs_,
                            "SELECT (SELECT SUM(i.v) FROM LimitAggInner i "
                            "WHERE i.k = o.k LIMIT 0) FROM LimitAggOuter "
                            "o")[0][0]
                  .IsNull());
  EXPECT_TRUE(
      RelationalRun(*rs_,
                    "SELECT (SELECT SUM(i.v) FROM LimitAggInner i "
                    "WHERE i.k = o.k LIMIT 1 OFFSET 1) FROM LimitAggOuter "
                    "o")[0][0]
          .IsNull());
  // Without LIMIT/OFFSET the same shape still aggregates.
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT (SELECT SUM(i.v) FROM LimitAggInner i "
                          "WHERE i.k = o.k) FROM LimitAggOuter o")[0][0],
            Value(int64_t{5}));
}

TEST_F(ExecutorTest, RelationalCorrelatedSetOperationFallsBack) {
  RelationalRun(*rs_, "CREATE TABLE CorrelatedSetOuter(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrelatedSetEmpty(k INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrelatedSetFilled(k INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO CorrelatedSetOuter VALUES (1)");
  RelationalRun(*rs_, "INSERT INTO CorrelatedSetFilled VALUES (1, 99)");
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.k, (SELECT d.v FROM CorrelatedSetEmpty d "
      "WHERE d.k = o.k UNION ALL SELECT n.v FROM CorrelatedSetFilled n "
      "WHERE n.k = o.k LIMIT 1) FROM CorrelatedSetOuter o");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][1], Value(int64_t{99}));
}

TEST_F(ExecutorTest, RelationalCorrelatedWindowFallsBack) {
  RelationalRun(*rs_, "CREATE TABLE CorrelatedWindowOuter(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrelatedWindowLocal(k INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO CorrelatedWindowOuter VALUES (1)");
  RelationalRun(*rs_,
                "INSERT INTO CorrelatedWindowLocal VALUES (1, 10), (1, 20)");
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.k, (SELECT d.v FROM CorrelatedWindowLocal d "
      "WHERE d.k = o.k QUALIFY ROW_NUMBER() OVER (ORDER BY d.v) = 3 LIMIT 1) "
      "FROM CorrelatedWindowOuter o");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_TRUE(rows[0][1].IsNull());
}

TEST_F(ExecutorTest, RelationalCorrelatedMixedKeyFallsBack) {
  RelationalRun(*rs_, "CREATE TABLE CorrelatedMixedLocal(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrelatedMixedOuter(k FLOAT64, id INT64)");
  RelationalRun(*rs_, "INSERT INTO CorrelatedMixedLocal VALUES (1)");
  RelationalRun(*rs_, "INSERT INTO CorrelatedMixedOuter VALUES (1.0, 1)");
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT o.id, (SELECT COUNT(*) FROM CorrelatedMixedLocal d "
                    "WHERE d.k = o.k) FROM CorrelatedMixedOuter o");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][1], Value(int64_t{1}));
}

TEST_F(ExecutorTest, RelationalCorrelatedOrderByCacheIncludesOuterDependency) {
  RelationalRun(*rs_, "CREATE TABLE CorrelatedOrderLocal(k INT64, v INT64)");
  RelationalRun(
      *rs_, "CREATE TABLE CorrelatedOrderOuter(k INT64, s INT64, id INT64)");
  RelationalRun(*rs_,
                "INSERT INTO CorrelatedOrderLocal VALUES (1, 10), (1, 20)");
  RelationalRun(
      *rs_, "INSERT INTO CorrelatedOrderOuter VALUES (1, 1, 1), (1, -1, 2)");
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT o.id, (SELECT d.v FROM CorrelatedOrderLocal d "
                    "WHERE d.k = o.k ORDER BY d.v * o.s LIMIT 1) "
                    "FROM CorrelatedOrderOuter o ORDER BY o.id");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][1], Value(int64_t{10}));
  EXPECT_EQ(rows[1][1], Value(int64_t{20}));
}

TEST_F(ExecutorTest, RelationalCorrelatedNestedQueryCacheIncludesDependencies) {
  RelationalRun(*rs_, "CREATE TABLE CorrelatedNestedLocal(k INT64, v INT64)");
  RelationalRun(
      *rs_, "CREATE TABLE CorrelatedNestedOuter(k INT64, s INT64, id INT64)");
  RelationalRun(*rs_,
                "INSERT INTO CorrelatedNestedLocal VALUES (1, 10), (1, 20)");
  RelationalRun(
      *rs_, "INSERT INTO CorrelatedNestedOuter VALUES (1, 1, 1), (1, -1, 2)");
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.id, (SELECT (SELECT d.v FROM CorrelatedNestedLocal d "
      "WHERE d.k = o.k ORDER BY d.v * o.s LIMIT 1)) "
      "FROM CorrelatedNestedOuter o ORDER BY o.id");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][1], Value(int64_t{10}));
  EXPECT_EQ(rows[1][1], Value(int64_t{20}));
}

TEST_F(ExecutorTest, RelationalHybridHashJoinSpillsUnderBudget) {
  CreateWideTable(*rs_, "WideJoinLeft", 200);
  CreateWideTable(*rs_, "WideJoinRight", 200);
  ScopedQueryMemory memory(65536);
  // The EXISTS marker forces the relational engine (Phase 8 routes plain
  // aliased joins to the cost-based optimizer). Read both payloads to keep
  // the wide joins and prevent key-only join elimination.
  constexpr std::string_view sql =
      "SELECT a.key, a.name, b.name FROM WideJoinLeft AS a "
      "JOIN WideJoinRight AS b ON a.key = b.key "
      "WHERE EXISTS (SELECT 1 FROM WideJoinLeft);";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 200U);
  std::unordered_set<int64_t> keys;
  keys.reserve(rows.size());
  for (const Row& row : rows) {
    EXPECT_TRUE(keys.insert(row[0].value.int_value).second);
    ASSERT_EQ(row.Count(), 3U);
    EXPECT_EQ(row[1], row[2]);
  }
  EXPECT_EQ(keys.size(), 200U);
  EXPECT_TRUE(keys.contains(0));
  EXPECT_TRUE(keys.contains(199));
  const std::string plan = RelationalExplain(*rs_, sql, /*analyze=*/true);
  EXPECT_EQ(StatsValue(plan, "hybrid_hash_joins="), 1);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

TEST_F(ExecutorTest, RelationalHybridHashLeftJoinSpillsUnderBudget) {
  CreateWideTable(*rs_, "WideLeft", 200);
  ScopedQueryMemory memory(65536);
  // Arithmetic keeps this predicate out of the non-throwing bare-comparison
  // scan pushdown, so this test still exercises a spilled right build.
  // No right row matches (b.key + 0) = 999, so every left row is null-extended;
  // the hybrid hash join must emit unmatched rows from both the resident bucket
  // and the replayed spilled partitions. The EXISTS marker forces the
  // relational engine (M6 routes plain LEFT joins to the cost-based
  // optimizer); it is semantically neutral.
  const auto rows = RelationalRun(
      *rs_,
      "SELECT a.key FROM WideLeft AS a LEFT JOIN WideLeft AS b ON "
      "a.key = b.key AND (b.key + 0) = 999 WHERE EXISTS (SELECT 1 FROM "
      "WideLeft);");
  ASSERT_EQ(rows.size(), 200U);
  const std::string plan =
      RelationalExplain(*rs_,
                        "SELECT a.key FROM WideLeft AS a "
                        "LEFT JOIN WideLeft AS b ON "
                        "a.key = b.key AND (b.key + 0) = 999 "
                        "WHERE EXISTS (SELECT 1 FROM "
                        "WideLeft);",
                        /*analyze=*/true);
  EXPECT_EQ(StatsValue(plan, "hybrid_hash_joins="), 1);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

// D8 (docs/design.md) acceptance: hybrid hash join with spill, compared
// against the same query with no memory budget.  Covers null-safe equality,
// RIGHT OUTER and FULL OUTER, and mixed resident/spill inputs.  A budget of
// A 64KiB budget puts the 200-row wide tables under spill pressure so the
// partition handling code runs; these tests assert result equivalence, not
// the spill count itself (the sibling test above pins the spill marker).
// (resident build, spilled build, unmatched spill rows) is exercised.

TEST_F(ExecutorTest, RelationalNullSafeInnerJoinSpillMatchesUnbudgeted) {
  CreateWideTableWithNullKeys(*rs_, "NullSafeJoin", 200);
  const std::string sql =
      "SELECT a.key, b.key FROM NullSafeJoin AS a JOIN NullSafeJoin AS b "
      "ON a.key IS NOT DISTINCT FROM b.key "
      "WHERE EXISTS (SELECT 1 FROM NullSafeJoin);";
  std::vector<std::pair<int64_t, int64_t>> reference;
  {
    ScopedQueryMemory unlimited(0);  // no budget: everything resident
    reference = KeyPairCounts(RelationalRun(*rs_, sql));
  }
  EXPECT_FALSE(reference.empty());
  {
    ScopedQueryMemory memory(65536);  // 64KiB: spill pressure, results asserted
    const auto spilled = KeyPairCounts(RelationalRun(*rs_, sql));
    EXPECT_EQ(spilled, reference);
  }
}

TEST_F(ExecutorTest, RelationalNullSafeOuterSpillMatchesUnbudgeted) {
  CreateWideTableWithNullKeys(*rs_, "NullSafeOuterJoin", 200);
  const std::vector<std::string> sqls{
      "SELECT a.key, b.key FROM NullSafeOuterJoin AS a "
      "LEFT JOIN NullSafeOuterJoin AS b "
      "ON a.key IS NOT DISTINCT FROM b.key "
      "WHERE EXISTS (SELECT 1 FROM NullSafeOuterJoin)",
      "SELECT a.key, b.key FROM NullSafeOuterJoin AS a "
      "RIGHT JOIN NullSafeOuterJoin AS b "
      "ON a.key IS NOT DISTINCT FROM b.key "
      "WHERE EXISTS (SELECT 1 FROM NullSafeOuterJoin)",
      "SELECT a.key, b.key FROM NullSafeOuterJoin AS a "
      "FULL JOIN NullSafeOuterJoin AS b "
      "ON a.key IS NOT DISTINCT FROM b.key "
      "WHERE EXISTS (SELECT 1 FROM NullSafeOuterJoin)"};
  for (const std::string& sql : sqls) {
    std::vector<std::pair<int64_t, int64_t>> reference;
    {
      ScopedQueryMemory unlimited(0);
      reference = KeyPairCounts(RelationalRun(*rs_, sql));
    }
    EXPECT_FALSE(reference.empty());
    {
      ScopedQueryMemory memory(65536);
      EXPECT_EQ(KeyPairCounts(RelationalRun(*rs_, sql)), reference);
    }
  }
}

TEST_F(ExecutorTest, RelationalRightOuterJoinSpillMatchesUnbudgeted) {
  CreateWideTableWithNullKeys(*rs_, "RightJoin", 200);
  // The right (build) side spills; every unmatched right row must still be
  // emitted null-extended on the left, from both resident and spilled
  // partitions (D8: no silent loss, no double output).
  const std::string sql =
      "SELECT a.key, b.key FROM (SELECT key FROM RightJoin) AS a "
      "RIGHT JOIN RightJoin AS b ON a.key = b.key "
      "WHERE EXISTS (SELECT 1 FROM RightJoin);";
  std::vector<std::pair<int64_t, int64_t>> reference;
  {
    ScopedQueryMemory unlimited(0);
    reference = KeyPairCounts(RelationalRun(*rs_, sql));
  }
  EXPECT_FALSE(reference.empty());
  {
    ScopedQueryMemory memory(65536);
    const auto spilled = KeyPairCounts(RelationalRun(*rs_, sql));
    EXPECT_EQ(spilled, reference);
  }
}

TEST_F(ExecutorTest, RelationalFullOuterJoinSpillMatchesUnbudgeted) {
  CreateWideTableWithNullKeys(*rs_, "FullJoin", 200);
  const std::string sql =
      "SELECT a.key, b.key FROM FullJoin AS a FULL JOIN FullJoin AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM FullJoin);";
  std::vector<std::pair<int64_t, int64_t>> reference;
  {
    ScopedQueryMemory unlimited(0);
    reference = KeyPairCounts(RelationalRun(*rs_, sql));
  }
  EXPECT_FALSE(reference.empty());
  {
    ScopedQueryMemory memory(65536);
    const auto spilled = KeyPairCounts(RelationalRun(*rs_, sql));
    EXPECT_EQ(spilled, reference);
  }
}

TEST_F(ExecutorTest, RelationalNullableJoinKeysDoNotMatch) {
  // Regression (§6.2): equi-joins over nullable columns crashed with
  // "Cannot encode unknown type." as soon as a NULL key row arrived.
  // NULL keys are non-matching rows, so only the (1,1) pair joins.
  RelationalRun(*rs_, "CREATE TABLE NullJoinA (k INT64, tag STRING);");
  RelationalRun(*rs_, "CREATE TABLE NullJoinB (k INT64, tag STRING);");
  RelationalRun(*rs_, "INSERT INTO NullJoinA VALUES (1, 'a1');");
  RelationalRun(*rs_, "INSERT INTO NullJoinA VALUES (NULL, 'an');");
  RelationalRun(*rs_, "INSERT INTO NullJoinA VALUES (2, 'a2');");
  RelationalRun(*rs_, "INSERT INTO NullJoinB VALUES (NULL, 'bn');");
  RelationalRun(*rs_, "INSERT INTO NullJoinB VALUES (1, 'b1');");
  const auto rows = RelationalRun(
      *rs_,
      "SELECT a.tag, b.tag FROM NullJoinA AS a JOIN NullJoinB AS b ON "
      "a.k = b.k WHERE EXISTS (SELECT 1 FROM NullJoinA);");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value("a1"), Value("b1")}));

  // LEFT JOIN keeps the unmatched NULL-key left rows null-extended.
  const auto left_rows = RelationalRun(
      *rs_,
      "SELECT a.tag, b.tag FROM NullJoinA AS a LEFT JOIN NullJoinB AS b ON "
      "a.k = b.k;");
  ASSERT_EQ(left_rows.size(), 3U);
  std::unordered_set<Row> null_extended;
  null_extended.reserve(left_rows.size());
  for (const Row& row : left_rows) {
    null_extended.insert(row);
  }
  EXPECT_EQ(null_extended,
            (std::unordered_set<Row>{Row({Value("a1"), Value("b1")}),
                                     Row({Value("a2"), Value()}),
                                     Row({Value("an"), Value()})}));
}

TEST_F(ExecutorTest, RelationalPartitionedAggregationSpillsUnderBudget) {
  // 400 rows: the key-only projection (~104B/row) still exceeds the soft budget
  // so the partitioned aggregation path (spill-by-hash-partition) is forced.
  CreateWideTable(*rs_, "WideAgg", 400);
  ScopedQueryMemory memory(65536);
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT key, COUNT(*) FROM WideAgg GROUP BY key "
                    "HAVING COUNT(*) > 0;");
  ASSERT_EQ(rows.size(), 400U);
  for (const Row& row : rows) {
    EXPECT_EQ(row[1], Value(1));  // keys are unique, every group is size 1
  }
  const std::string plan = RelationalExplain(*rs_,
                                             "SELECT key, COUNT(*) FROM "
                                             "WideAgg GROUP BY key "
                                             "HAVING COUNT(*) > 0;",
                                             /*analyze=*/true);
  EXPECT_EQ(StatsValue(plan, "aggregate_groups="), 400);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

TEST_F(ExecutorTest, RelationalKeyOnlyHavingTransposesIntoScanFilter) {
  // HAVING transpose (aggregate <-> filter): within a grouping every row of a
  // group carries the same key values, so a conjunct over only grouping-key
  // columns is decided inside the scan filter; doomed groups never form and
  // the aggregate-referencing remainder stays post-aggregate.
  constexpr std::string_view kHavingTable = "HavingTranspose";
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{
        std::string(kHavingTable),
        {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    ASSERT_SUCCESS(
        table.Insert(writer.txn_, Row({Value(int64_t{0}), Value(int64_t{1})}))
            .GetStatus());
    ASSERT_SUCCESS(
        table.Insert(writer.txn_, Row({Value(int64_t{1}), Value(int64_t{10})}))
            .GetStatus());
    ASSERT_SUCCESS(
        table.Insert(writer.txn_, Row({Value(int64_t{2}), Value(int64_t{30})}))
            .GetStatus());
    ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(), Value(int64_t{5})}))
                       .GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  // Rows are identical with or without the transpose (semantics guard).
  const auto strictly = RelationalRun(
      *rs_,
      "SELECT k, SUM(v) FROM HavingTranspose GROUP BY k HAVING k > 1 ORDER "
      "BY k;");
  ASSERT_EQ(strictly.size(), 1U);
  EXPECT_EQ(strictly[0], Row({Value(2), Value(30)}));
  const auto mixed = RelationalRun(
      *rs_,
      "SELECT k, SUM(v) FROM HavingTranspose GROUP BY k HAVING k > 1 AND "
      "SUM(v) > 25 ORDER BY k;");
  ASSERT_EQ(mixed.size(), 1U);
  EXPECT_EQ(mixed[0], Row({Value(2), Value(30)}));
  const auto null_group = RelationalRun(
      *rs_,
      "SELECT k, SUM(v) FROM HavingTranspose GROUP BY k HAVING k IS NULL "
      "ORDER BY k;");
  ASSERT_EQ(null_group.size(), 1U);
  EXPECT_EQ(null_group[0][0], Value());
  // The transpose is observable in the runtime counters: with the key-only
  // conjuncts evaluated in the scan filter, the surviving group's rows are
  // all that reach the aggregator.
  const std::string plan = RelationalExplain(
      *rs_, "SELECT k, SUM(v) FROM HavingTranspose GROUP BY k HAVING k > 1;",
      /*analyze=*/true);
  EXPECT_EQ(StatsValue(plan, "aggregate_groups="), 1);
  EXPECT_EQ(StatsValue(plan, "aggregate_input_rows="), 1);
}

TEST_F(ExecutorTest, RelationalGroupByOrderBySpillsUnderBudget) {
  CreateWideTable(*rs_, "WideSort", 200);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_, "SELECT key, COUNT(*) FROM WideSort GROUP BY key ORDER BY key;");
  ASSERT_EQ(rows.size(), 200U);
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i][0], Value(static_cast<int64_t>(i)));
  }
}

TEST_F(ExecutorTest, RelationalDistinctCrossJoinSpillsUnderBudget) {
  CreateWideTable(*rs_, "WideDistinct", 200);
  ScopedQueryMemory memory(65536);
  // EXISTS marker: keeps this on the relational engine (see
  // RelationalHybridHashJoinSpillsUnderBudget).
  const auto rows = RelationalRun(
      *rs_,
      "SELECT DISTINCT a.key FROM WideDistinct AS a CROSS JOIN WideDistinct AS "
      "b WHERE EXISTS (SELECT 1 FROM WideDistinct);");
  ASSERT_EQ(rows.size(), 200U);
  const std::string plan = RelationalExplain(
      *rs_,
      "SELECT DISTINCT a.key FROM WideDistinct AS a CROSS JOIN "
      "WideDistinct AS b WHERE EXISTS (SELECT 1 FROM WideDistinct);",
      /*analyze=*/true);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

TEST_F(ExecutorTest, RelationalScalarSubquerySurvivesSpilledRows) {
  // Regression guard: scalar-subquery evaluation must read spilled rows too,
  // not just the in-memory portion of the cached subquery result. Every outer
  // row therefore sees the scalar key value instead of NULL. The subquery is
  // capped with LIMIT 1 because a scalar subquery must return at most one
  // row; the full 200-row relation is still materialized (and spilled) before
  // truncation.
  CreateWideTable(*rs_, "WideScalar", 200);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_,
      "SELECT (SELECT key FROM (SELECT * FROM WideScalar) LIMIT 1) FROM "
      "WideScalar;");
  ASSERT_EQ(rows.size(), 200U);
  for (const Row& row : rows) {
    EXPECT_FALSE(row[0].IsNull());
  }
}

}  // namespace tinylamb
