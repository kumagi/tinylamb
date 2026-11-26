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

#include "common/platform/process.hpp"
#include "common/test_util.hpp"
#include "executor/executor_test.hpp"

namespace tinylamb {

// An empty right side makes every probe miss: the cursor must report a clean
// exhaustion, not a failure, and must not lose the left side's error either.
TEST_F(ExecutorTest, LookupJoinEmptyRightSideExhaustsCleanly) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"EmptyTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64)}}));
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "EmptyTarget",
                IndexSchema("EmptyAB", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto reloaded_status = ctx.GetTable("EmptyTarget");
  ASSERT_EQ(reloaded_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& reloaded = reloaded_status.Value();

  LookupJoin join(
      ctx.txn_,
      RangeProbeInput({Row({Value(1), Value(2)}), Row({Value(3), Value(4)})}),
      {0, 1}, *reloaded, reloaded->GetIndex(0), {0, 1}, 1,
      BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(join).empty());

  // An empty left side never probes at all.
  LookupJoin empty_left(ctx.txn_, RangeProbeInput({}), {0, 1}, *reloaded,
                        reloaded->GetIndex(0), {0, 1}, 1,
                        BinaryOperation::kLessThan);
  EXPECT_TRUE(Drain(empty_left).empty());
}

// The residual predicate carries what the probe cannot decide. Driven here
// through the plan/factory pair: an equality on a NON-key column of a composite
// index leaves the seek on the leading key only, and the extra equality must
// still filter the probe's over-fetch. The old executor sought with the join's
// own key order here, landing between unrelated stored keys and silently
// dropping every match.
TEST_F(ExecutorTest, LookupJoinRechecksEqualityOnANonKeyColumn) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_table,
      rs_->CreateTable(ctx, Schema{"ProbeSide",
                                   {Column("x", ValueType::kInt64),
                                    Column("y", ValueType::kInt64)}}));
  ASSERT_SUCCESS(
      left_table.Insert(ctx.txn_, Row({Value(1), Value(5)})).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"CompositeSide",
                                   {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64),
                                    Column("c", ValueType::kInt64)}}));
  for (const auto& row : {Row({Value(1), Value(10), Value(5)}),
                          Row({Value(1), Value(20), Value(5)}),
                          Row({Value(1), Value(30), Value(6)}),
                          Row({Value(2), Value(40), Value(5)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "CompositeSide",
                IndexSchema("CompositeAB", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto right = ctx.GetTable("CompositeSide");
  const auto right_stats = ctx.GetStats("CompositeSide");
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(right_stats.HasValue());
  // NOTE: FullScanPlan keeps its statistics by reference, so the scan's stats
  // must outlive the plan -- bind them to a named local, not a temporary.
  const TableStatistics left_stats(left_table.GetSchema());
  Plan plan(new ProductPlan(
      std::make_shared<FullScanPlan>(left_table, left_stats),
      {ColumnName("ProbeSide", "x"), ColumnName("ProbeSide", "y")},
      *right.Value(), right.Value()->GetIndex(0),
      {ColumnName("CompositeSide", "a"), ColumnName("CompositeSide", "c")},
      *right_stats.Value()));
  // The probe derives the (a) prefix in the index's own key order; `c` is not
  // an index key column, so `x = c` is not part of the seek. The join emits
  // the probe's over-fetch and the residual predicate above it decides.
  auto& product = dynamic_cast<ProductPlan&>(*plan);
  const std::optional<LookupSeek>& probe = product.Lookup();
  ASSERT_TRUE(probe.has_value());
  ASSERT_EQ(SeekOf(probe).right_columns.size(), 1U);
  EXPECT_EQ(SeekOf(probe).right_columns[0], ColumnName("CompositeSide", "a"));
  EXPECT_EQ(SeekOf(probe).left_columns[0], ColumnName("ProbeSide", "x"));
  ASSERT_EQ(SeekOf(probe).range_position, std::nullopt);
  // Without the residual the probe over-fetches every row under the (a)
  // prefix: 1, 2 and 3 rows of the composite side.
  {
    Executor raw = plan->EmitExecutor(ctx);
    const std::vector<Row> over_fetched = Drain(*raw);
    ASSERT_EQ(over_fetched.size(), 3U);
  }

  const Expression residual = BinaryExpressionExp(
      ColumnValueExp(ColumnName("ProbeSide", "y")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("CompositeSide", "c")));
  Plan filtered = std::make_shared<SelectionPlan>(
      plan, residual, TableStatistics(right.Value()->GetSchema()));
  Executor executor = filtered->EmitExecutor(ctx);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("LookupJoin"), std::string::npos) << dump.str();
  const std::vector<Row> rows = Drain(*executor);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(5), Value(1), Value(10), Value(5)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(5), Value(1), Value(20), Value(5)}));
}

// The range allowlist is INT64/DATE only. A FLOAT64 range column stays a
// residual predicate, so the probe must still return the right rows through the
// wider equality-prefix scan.
TEST_F(ExecutorTest, LookupJoinRejectsNonAllowlistedRangeTypes) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, probe_table,
      rs_->CreateTable(ctx, Schema{"FloatProbe",
                                   {Column("a", ValueType::kInt64),
                                    Column("f", ValueType::kDouble)}}));
  ASSERT_SUCCESS(
      probe_table.Insert(ctx.txn_, Row({Value(1), Value(1.5)})).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      rs_->CreateTable(ctx, Schema{"FloatTarget",
                                   {Column("a", ValueType::kInt64),
                                    Column("f", ValueType::kDouble),
                                    Column("v", ValueType::kInt64)}}));
  for (const auto& row : {Row({Value(1), Value(1.5), Value(10)}),
                          Row({Value(1), Value(9.5), Value(90)}),
                          Row({Value(1), Value(2.5), Value(20)})}) {
    ASSERT_SUCCESS(right_table.Insert(ctx.txn_, row).GetStatus());
  }
  ASSERT_EQ(rs_->CreateIndex(
                ctx, "FloatTarget",
                IndexSchema("FloatAV", {0, 2}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  const auto right = ctx.GetTable("FloatTarget");
  const auto right_stats = ctx.GetStats("FloatTarget");
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(right_stats.HasValue());
  // NOTE: FullScanPlan keeps its statistics by reference, so the scan's stats
  // must outlive the plan -- bind them to a named local, not a temporary.
  const TableStatistics left_stats(probe_table.GetSchema());
  Plan left_plan = std::make_shared<FullScanPlan>(probe_table, left_stats);
  auto product = std::make_shared<ProductPlan>(
      left_plan,
      std::vector<ColumnName>{ColumnName("FloatProbe", "a"),
                              ColumnName("FloatProbe", "f")},
      *right.Value(), right.Value()->GetIndex(0),
      std::vector<ColumnName>{ColumnName("FloatTarget", "a"),
                              ColumnName("FloatTarget", "f")},
      *right_stats.Value());
  EXPECT_FALSE(product->SetLookupRange(1, ColumnName("FloatProbe", "f"),
                                       ColumnName("FloatTarget", "f"),
                                       BinaryOperation::kLessThan));
  ASSERT_TRUE(product->Lookup().has_value());
  EXPECT_EQ(SeekOf(product->Lookup()).range_position, std::nullopt);
  EXPECT_EQ(SeekOf(product->Lookup()).right_columns.size(), 1U);
  // A mismatched operand type is rejected as well: the per-row re-check could
  // not decide a cross-typed comparison.
  EXPECT_FALSE(product->SetLookupRange(1, ColumnName("FloatProbe", "a"),
                                       ColumnName("FloatTarget", "f"),
                                       BinaryOperation::kLessThan));
  // The range column is not the first unpinned index key column, so a bound
  // there cannot narrow the seek at all.
  EXPECT_FALSE(product->SetLookupRange(2, ColumnName("FloatProbe", "f"),
                                       ColumnName("FloatTarget", "f"),
                                       BinaryOperation::kLessThan));
}

TEST_F(ExecutorTest, Insert) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Schema src_schema{
      "SrcTable",
      {Column("key2", ValueType::kInt64), Column("name2", ValueType::kVarChar),
       Column("score2", ValueType::kDouble)}};
  rs_->CreateTable(ctx, src_schema);
  const StatusOr<std::shared_ptr<Table>> right_tbl_status =
      ctx.GetTable("SrcTable");
  ASSERT_EQ(right_tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& right_tbl = right_tbl_status.Value();
  BulkInsert(ctx.txn_, *right_tbl,
             {{Row({Value(9), Value("troop"), Value(1.2)})},
              {Row({Value(7), Value("arise"), Value(3.9)})},
              {Row({Value(1), Value("probe"), Value(4.9)})},
              {Row({Value(3), Value("ought"), Value(12.4)})},
              {Row({Value(3), Value("extra"), Value(99.9)})},
              {Row({Value(232), Value("out"), Value(40.9)})},
              {Row({Value(0), Value("arise"), Value(9.2)})}});
  auto insert = std::make_shared<Insert>(
      ctx.txn_, &*tbl, std::make_shared<FullScan>(ctx.txn_, *right_tbl));
  LOG(INFO) << *insert;
  Row result;

  // Act 1 -- iterate Insert executor to produce rows from SrcTable into
  // SampleTable
  ASSERT_TRUE(insert->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(7));
  ASSERT_FALSE(insert->Next(&result, nullptr));

  // Assert 1 -- Insert executor produced the expected row and then exhausted
  // (implicit above)

  // Arrange 2 -- prepare verification FullScan over the populated SampleTable
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)}),
                           Row({Value(9), Value("troop"), Value(1.2)}),
                           Row({Value(7), Value("arise"), Value(3.9)}),
                           Row({Value(1), Value("probe"), Value(4.9)}),
                           Row({Value(3), Value("ought"), Value(12.4)}),
                           Row({Value(3), Value("extra"), Value(99.9)}),
                           Row({Value(232), Value("out"), Value(40.9)}),
                           Row({Value(0), Value("arise"), Value(9.2)})});

  // Act 2 -- iterate FullScan to verify all 12 rows are present in SampleTable
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }

  // Assert 2 -- FullScan consumed all expected rows and cursor exhausted
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Update) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Schema src_schema{
      "SrcTable",
      {Column("key2", ValueType::kInt64), Column("name2", ValueType::kVarChar),
       Column("score2", ValueType::kDouble)}};
  rs_->CreateTable(ctx, src_schema);
  const StatusOr<std::shared_ptr<Table>> right_tbl_status =
      ctx.GetTable("SrcTable");
  ASSERT_EQ(right_tbl_status.GetStatus(), Status::kSuccess);

  std::vector<NamedExpression> update_rule = {
      NamedExpression("key", ColumnValueExp("key")),
      NamedExpression("name", ConstantValueExp(Value("****"))),
      NamedExpression("score",
                      BinaryExpressionExp(ColumnValueExp("score"),
                                          BinaryOperation::kMultiply,
                                          ConstantValueExp(Value(2.0))))};
  auto update = std::make_shared<Update>(
      ctx.txn_, &*tbl,
      std::make_shared<Projection>(update_rule, tbl->GetSchema(),
                                   std::make_shared<FullScan>(ctx.txn_, *tbl)));
  LOG(INFO) << *update;
  Row result;

  // Act 1 -- iterate Update executor to apply updates to SampleTable rows
  ASSERT_TRUE(update->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(4));
  ASSERT_FALSE(update->Next(&result, nullptr));

  // Assert 1 -- Update executor produced the expected updated row and exhausted
  // (implicit above)

  // Arrange 2 -- prepare verification FullScan over the updated SampleTable
  std::unordered_set<Row> rows({{Row({Value(0), Value("****"), Value(2.4)})},
                                {Row({Value(3), Value("****"), Value(24.4)})},
                                {Row({Value(1), Value("****"), Value(9.8)})},
                                {Row({Value(2), Value("****"), Value(8.28)})}});

  // Act 2 -- iterate FullScan to verify all 4 updated rows are present
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }

  // Assert 2 -- FullScan consumed all expected updated rows and cursor
  // exhausted
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, UpdatePreservesConstraintFailureStatusAndMessage) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  // Deliberately provide an incomplete replacement image at valid positions.
  auto source = std::make_shared<Projection>(
      std::vector<NamedExpression>{
          NamedExpression("key", ColumnValueExp("key"))},
      table->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *table));
  Update update(ctx.txn_, table.get(), std::move(source));
  Row result;
  EXPECT_FALSE(update.Next(&result, nullptr));
  EXPECT_EQ(update.GetStatus(), Status::kInvalidArgument);
  EXPECT_NE(update.GetStatus().GetMessage().find("update failed on table"),
            std::string::npos);
  EXPECT_NE(update.GetStatus().GetMessage().find(
                "row column count does not match table schema"),
            std::string::npos);
  size_t count = 0;
  for (auto scan = table->BeginFullScan(ctx.txn_); scan.IsValid(); ++scan) {
    EXPECT_EQ((*scan).values_.size(), 3);
    ++count;
  }
  EXPECT_EQ(count, 4);
}

TEST_F(ExecutorTest, Aggregation) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("key"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("score"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("score"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("score"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("score")))};
  AggregationExecutor agg(std::move(fs), tbl->GetSchema(),
                          std::move(aggregates));
  Row result;

  // Act -- advance Aggregation cursor to compute aggregates over all rows
  ASSERT_TRUE(agg.Next(&result, nullptr));

  // Assert -- aggregated values match expected statistics
  ASSERT_EQ(result[0], Value(4));
  // Accumulating doubles loses low-order bits, so a strict equality against
  // the decimal literal is not a stable contract; allow rounding slack.
  EXPECT_NEAR(result[1].value.double_value, 22.44, 1e-9);
  EXPECT_NEAR(result[2].value.double_value, 5.61, 1e-9);
  ASSERT_EQ(result[3], Value(1.2));
  ASSERT_EQ(result[4], Value(12.2));
  ASSERT_FALSE(agg.Next(&result, nullptr));
}

TEST_F(ExecutorTest, MixedAggregateKeepsBitAndResult) {
  // PRODUCTION BUG (fixed): when a typed aggregate (BIT_AND) shared the row
  // loop with a generic one (STRING_AGG), the typed aggregate owned no
  // accumulator and the row-wise switch had no BIT_* cases, so it silently
  // produced NULL.
  const Schema schema("bits", {Column("x", ValueType::kInt64),
                               Column("y", ValueType::kVarChar)});
  auto src = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(int64_t{5}), Value("a")}),
      Row({Value(int64_t{1}), Value("b")}),
      Row({Value(int64_t{3}), Value("c")}),
  });
  std::vector<NamedExpression> aggregates = {
      NamedExpression("bit_and",
                      AggregateExpressionExp(AggregationType::kBitAnd,
                                             ColumnValueExp("x"))),
      NamedExpression("str", AggregateExpressionExp(AggregationType::kStringAgg,
                                                    ColumnValueExp("y")))};
  AggregationExecutor agg(src, schema, aggregates);
  Row result;
  ASSERT_TRUE(agg.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(int64_t{1}));  // 5 & 1 & 3
  EXPECT_EQ(result[1], Value("a,b,c"));
}

TEST_F(ExecutorTest, MinMaxWithNaNIsNaNAcrossPaths) {
  // PRODUCTION BUG (fixed): the typed fast paths skipped NaN (IEEE compares
  // false) while the ground truth accumulator returns NaN for any group
  // containing NaN, so the result depended on the execution path.
  const Schema schema("nans", {Column("v", ValueType::kDouble)});
  auto make_src = [] {
    return std::make_shared<ConstantExecutor>(std::vector<Row>{
        Row({Value(std::numeric_limits<double>::quiet_NaN())}),
        Row({Value(3.0)}),
    });
  };
  auto make_aggregates = [] {
    return std::vector<NamedExpression>{
        NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                      ColumnValueExp("v"))),
        NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                      ColumnValueExp("v")))};
  };

  AggregationExecutor serial(make_src(), schema, make_aggregates());
  Row serial_result;
  ASSERT_TRUE(serial.Next(&serial_result, nullptr));
  EXPECT_TRUE(std::isnan(serial_result[0].value.double_value));
  EXPECT_TRUE(std::isnan(serial_result[1].value.double_value));

  ParallelAggregationExecutor parallel(make_src(), schema, make_aggregates(),
                                       2);
  Row parallel_result;
  ASSERT_TRUE(parallel.Next(&parallel_result, nullptr));
  EXPECT_TRUE(std::isnan(parallel_result[0].value.double_value));
  EXPECT_TRUE(std::isnan(parallel_result[1].value.double_value));
}

TEST_F(ExecutorTest, VectorizedScanFilterProjectAggregatePipeline) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      ctx.GetTable(kTableName);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  const Schema input_schema = table->GetSchema();
  auto scan = std::make_shared<FullScan>(ctx.txn_, *table);
  auto filter = std::make_shared<Selection>(
      BinaryExpressionExp(ColumnValueExp("score"),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(4.0))),
      input_schema, scan);
  std::vector<NamedExpression> projections = {NamedExpression(
      "doubled",
      BinaryExpressionExp(ColumnValueExp("score"), BinaryOperation::kMultiply,
                          ConstantValueExp(Value(2.0))))};
  auto project = std::make_shared<Projection>(projections, input_schema,
                                              std::move(filter));
  const Schema projected_schema("projected",
                                {Column("doubled", ValueType::kDouble)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("doubled"))),
      NamedExpression("sum",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("doubled")))};
  AggregationExecutor aggregate(std::move(project), projected_schema,
                                std::move(aggregates));

  DataChunk output;
  ASSERT_EQ(aggregate.NextBatch(&output, 8), 1);
  ASSERT_EQ(output.Size(), 1);
  const Row result = output.RowAt(0);
  EXPECT_EQ(result[0], Value(3));
  // Accumulated double sum: allow rounding slack against the literal.
  EXPECT_NEAR(result[1].value.double_value, 42.48, 1e-9);
  EXPECT_EQ(aggregate.NextBatch(&output, 8), 0);
}

TEST_F(ExecutorTest, MorselDrivenParallelScanReadsEveryPageOnce) {
  constexpr std::string_view kParallelTable = "ParallelScanTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kParallelTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_, Row({Value(key), Value(std::move(payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      reader.GetTable(kParallelTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 4, 1);
  ASSERT_GT(scan.MorselCount(), 4U);
  EXPECT_EQ(scan.WorkerCount(), 4U);

  std::unordered_set<int64_t> keys;
  keys.reserve(kRows);
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 37) != 0) {
    ASSERT_LE(chunk.Size(), 37U);
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(
          keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value).second);
    }
  }
  EXPECT_EQ(keys.size(), static_cast<size_t>(kRows));
  EXPECT_TRUE(keys.contains(0));
  EXPECT_TRUE(keys.contains(kRows - 1));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, FusedTypedScanAggregatesNullableOldSnapshot) {
  constexpr std::string_view name = "FoldSnapshot";
  std::vector<RowPosition> positions;
  positions.reserve(2000);
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer, Schema(name, {Column("value", ValueType::kInt64),
                                  Column("padding", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 2000; ++i) {
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, position,
          table.Insert(writer.txn_, Row({i % 5 == 0 ? Value() : Value(i),
                                         Value(std::string(100, 'x'))})));
      positions.push_back(position);
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, old.GetTable(name));
  auto old_scan = std::make_shared<ParallelScan>(old.txn_, *table, 4, 1,
                                                 std::vector<slot_t>{0});
  ASSERT_GT(old_scan->MorselCount(), 4U);
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(table
                       ->Update(writer.txn_, positions[0],
                                Row({Value(99), Value(std::string(100, 'x'))}))
                       .GetStatus());
    ASSERT_SUCCESS(table->Delete(writer.txn_, positions[1]));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  const Schema schema(name, {Column("value", ValueType::kInt64)});
  const std::vector<NamedExpression> aggregates{
      NamedExpression("rows",
                      AggregateExpressionExp(AggregationType::kCount, nullptr)),
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("value"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("value"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("value"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("value")))};
  AggregationExecutor before(old_scan, schema, aggregates);
  Row row;
  ASSERT_TRUE(before.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2000), Value(1600), Value(1600000), Value(1),
                      Value(1999)}));
  EXPECT_TRUE(old_scan->WasTypedFolded());
  EXPECT_FALSE(before.Next(&row, nullptr));
  DataChunk exhausted;
  EXPECT_EQ(old_scan->NextBatch(&exhausted), 0U);
  ASSERT_SUCCESS(before.GetStatus());
  ASSERT_SUCCESS(old.PreCommit());
  auto current = rs_->BeginReadOnlyContext();
  auto current_scan = std::make_shared<ParallelScan>(current.txn_, *table, 4, 1,
                                                     std::vector<slot_t>{0});
  AggregationExecutor after(current_scan, schema, aggregates);
  ASSERT_TRUE(after.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1999), Value(1600), Value(1600098), Value(2),
                      Value(1999)}));
  EXPECT_TRUE(current_scan->WasTypedFolded());
  ASSERT_SUCCESS(after.GetStatus());
  ASSERT_SUCCESS(current.PreCommit());
}

TEST_F(ExecutorTest, FusedTypedScanPreservesIntermediateSumOverflow) {
  const int64_t hi = std::numeric_limits<int64_t>::max();
  const int64_t lo = std::numeric_limits<int64_t>::min();
  const std::vector<std::vector<int64_t>> cases{
      {hi, 0, 1, -1}, {lo, 0, -1, 1}, {-hi, 0, hi, hi}, {hi, 0, -hi, -hi}};
  for (size_t i = 0; i < cases.size(); ++i) {
    const std::string name = "FoldOverflow" + std::to_string(i);
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(
          Table, table,
          rs_->CreateTable(
              writer, Schema(name, {Column("value", ValueType::kInt64),
                                    Column("padding", ValueType::kVarChar)})));
      for (int64_t value : cases[i]) {
        ASSERT_SUCCESS(
            table
                .Insert(writer.txn_,
                        Row({Value(value), Value(std::string(12000, 'x'))}))
                .GetStatus());
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    const std::vector<NamedExpression> aggregates{NamedExpression(
        "sum", AggregateExpressionExp(AggregationType::kSum,
                                      ColumnValueExp("value")))};
    Row expected;
    std::vector<Row> reference_rows;
    reference_rows.reserve(cases[i].size());
    for (int64_t value : cases[i]) {
      reference_rows.emplace_back(std::vector<Value>{Value(value)});
    }
    AggregationExecutor serial(
        std::make_shared<ConstantExecutor>(std::move(reference_rows)),
        Schema(name, {Column("value", ValueType::kInt64)}), aggregates);
    const bool serial_success = serial.Next(&expected, nullptr);
    EXPECT_EQ(serial_success, i >= 2);
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1,
                                               std::vector<slot_t>{0});
    ASSERT_GE(scan->MorselCount(), 2U);
    AggregationExecutor fused(
        scan, Schema(name, {Column("value", ValueType::kInt64)}), aggregates);
    Row actual;
    EXPECT_EQ(fused.Next(&actual, nullptr), serial_success);
    EXPECT_TRUE(scan->WasTypedFolded());
    if (serial_success) {
      EXPECT_EQ(actual, expected);
      ASSERT_SUCCESS(fused.GetStatus());
    } else {
      EXPECT_EQ(fused.GetStatus(), Status::kIsInfinity);
      EXPECT_EQ(serial.GetStatus(), Status::kIsInfinity);
    }
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(ExecutorTest, FusedTypedScanEmptyAndAllNullAggregates) {
  for (int count : {0, 20}) {
    const std::string name = "FoldNull" + std::to_string(count);
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(
          Table, table,
          rs_->CreateTable(writer,
                           Schema(name, {Column("value", ValueType::kInt64)})));
      for (int i = 0; i < count; ++i) {
        ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value()})).GetStatus());
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1);
    AggregationExecutor aggregate(
        scan, table->GetSchema(),
        {NamedExpression(
             "rows", AggregateExpressionExp(AggregationType::kCount, nullptr)),
         NamedExpression("count",
                         AggregateExpressionExp(AggregationType::kCount,
                                                ColumnValueExp("value"))),
         NamedExpression("sum",
                         AggregateExpressionExp(AggregationType::kSum,
                                                ColumnValueExp("value")))});
    Row row;
    ASSERT_TRUE(aggregate.Next(&row, nullptr));
    EXPECT_TRUE(scan->WasTypedFolded());
    EXPECT_EQ(row, Row({Value(count), Value(0), Value()}));
    ASSERT_SUCCESS(aggregate.GetStatus());
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(ExecutorTest, FusedTypedScanSurfacesConsumerFailure) {
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTableName));
  ParallelScan scan(reader.txn_, *table, 4, 1, std::vector<slot_t>{0});
  StatusOr<bool> result = scan.TryFoldTypedMorsels(
      [](size_t, const TypedCellBatch&) { return Status::kConflicts; });
  EXPECT_FALSE(result.HasValue());
  EXPECT_EQ(result.GetStatus(), Status::kConflicts);
  EXPECT_EQ(scan.GetStatus(), Status::kConflicts);
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk), 0U);
  ASSERT_SUCCESS(reader.PreCommit());
}

// The worker-side row filter must decide rows inside the decode workers and
// hand chunks out exactly as a serial Selection over the same conjuncts
// would: survivors only, once each, no rows lost across morsel boundaries.
TEST_F(ExecutorTest, MorselDrivenParallelScanWorkerFilterDecidesRows) {
  constexpr std::string_view kFilteredTable = "ParallelFilteredTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kFilteredTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_, Row({Value(key), Value(std::move(payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      reader.GetTable(kFilteredTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();

  // One simple compare (worker fast path) plus one residual (bytecode):
  // `key >= 10 AND key % 2 = 0`.
  const std::vector<Expression> conjuncts{
      BinaryExpressionExp(ColumnValueExp("key"),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(int64_t{10}))),
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kModulo,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})))};
  const Schema& schema = table->GetSchema();
  const relational_detail::CompiledScanFilter filter =
      relational_detail::CompileScanFilter(conjuncts, schema);
  relational_detail::WorkerScanFilterPrograms programs =
      relational_detail::CompileWorkerFilter(filter, schema);
  ASSERT_TRUE(programs.complete)
      << "residual bytecode must compile for the worker filter test";

  ParallelScan scan(reader.txn_, *table, 4, 1);
  ASSERT_TRUE(scan.CanTakeRowFilter());
  scan.SetRowFilter(conjuncts, filter, std::move(programs));

  std::unordered_set<int64_t> keys;
  keys.reserve(kRows);
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 53) != 0) {
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(
          keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value).second);
    }
  }
  ASSERT_SUCCESS(scan.GetStatus());
  for (int64_t key = 0; key < kRows; ++key) {
    EXPECT_EQ(keys.contains(key), key >= 10 && key % 2 == 0) << key;
  }
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

// Rows whose worker program raises are parked for the contextual re-check on
// the consumer thread, and the first such error surfaces as the scan's
// sticky status: a raising row must not be silently dropped.
TEST_F(ExecutorTest, MorselDrivenParallelScanSurfacesDeferredRowErrors) {
  constexpr std::string_view kDeferredTable = "ParallelDeferredTable";
  constexpr int64_t kRows = 2000;
  constexpr int64_t kBoomKey = 500;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kDeferredTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_, Row({Value(key), Value(std::move(payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status =
      reader.GetTable(kDeferredTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();

  // `1000 / (500 - key) = 1`: INT64 division promotes to FLOAT64, so within
  // the key domain [0, 2000) no row satisfies it (the exact quotient 1.0
  // would need key == -500) -- every row is a clean rejection except
  // key == 500, where the division by zero raises inside the worker program
  // and parks the row for the consumer-side re-check.
  const std::vector<Expression> conjuncts{BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{1000})),
                          BinaryOperation::kDivide,
                          BinaryExpressionExp(ConstantValueExp(Value(kBoomKey)),
                                              BinaryOperation::kSubtract,
                                              ColumnValueExp("key"))),
      BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{1})))};
  const Schema& schema = table->GetSchema();
  const relational_detail::CompiledScanFilter filter =
      relational_detail::CompileScanFilter(conjuncts, schema);
  relational_detail::WorkerScanFilterPrograms programs =
      relational_detail::CompileWorkerFilter(filter, schema);
  ASSERT_TRUE(programs.complete);

  ParallelScan scan(reader.txn_, *table, 4, 1);
  scan.SetRowFilter(conjuncts, filter, std::move(programs));

  std::unordered_set<int64_t> keys;
  keys.reserve(kRows);
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 53) != 0) {
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(
          keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value).second);
    }
  }
  EXPECT_NE(scan.GetStatus(), Status::kSuccess)
      << "the deferred raising row must surface as a sticky error";
  // The raising row must not be silently dropped into the result as a
  // match or a non-match: the scan ends in an error, never a row.
  EXPECT_TRUE(keys.empty());
  EXPECT_FALSE(keys.contains(kBoomKey));
  EXPECT_EQ(tinylamb::Status::kSuccess, reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ThreadLocalAggregationMergesPartialAndDistinctStates) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(10000);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("value"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("value"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("value"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("value"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("value"))),
      NamedExpression("distinct_count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("value"), true))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        4);

  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(10000));
  EXPECT_EQ(result[1], Value(495000));
  EXPECT_EQ(result[2], Value(49.5));
  EXPECT_EQ(result[3], Value(0));
  EXPECT_EQ(result[4], Value(99));
  EXPECT_EQ(result[5], Value(100));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelSortPreservesOrderAndStableTies) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(10000);
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("value"), true, std::nullopt}}, 4);

  int64_t previous_value = -1;
  slot_t previous_position = 0;
  size_t count = 0;
  Row row;
  RowPosition position;
  while (sort.Next(&row, &position)) {
    const int64_t value = row[0].value.int_value;
    EXPECT_GE(value, previous_value);
    if (value == previous_value) {
      EXPECT_GT(position.slot, previous_position);
    }
    previous_value = value;
    previous_position = position.slot;
    ++count;
  }
  EXPECT_EQ(count, 10000U);
  EXPECT_EQ(previous_value, 99);
}

// ===== DistinctExecutor =====
TEST_F(ExecutorTest, DistinctEmptyInput) {
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::vector<Row>{}));
  Row row;
  RowPosition pos;
  ASSERT_FALSE(distinct.Next(&row, &pos));
  ASSERT_FALSE(distinct.Next(&row, nullptr));
}

TEST_F(ExecutorTest, DistinctAllDuplicates) {
  std::vector<Row> rows;
  rows.reserve(6);
  for (int i = 0; i < 6; ++i) {
    rows.emplace_back(std::vector<Value>{Value(7)});
  }
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  ASSERT_TRUE(distinct.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(7)}));
  ASSERT_FALSE(distinct.Next(&row, nullptr));
}

TEST_F(ExecutorTest, DistinctAlreadySortedInput) {
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(2)}),
                        Row({Value(3)}), Row({Value(4)}), Row({Value(4)})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  int64_t expected = 1;
  while (distinct.Next(&row, &pos)) {
    EXPECT_EQ(row, Row({Value(expected)}));
    ++expected;
  }
  EXPECT_EQ(expected, 5);
}

TEST_F(ExecutorTest, DistinctUnsortedInterleavedDuplicates) {
  std::vector<Row> rows{Row({Value(3)}), Row({Value(1)}), Row({Value(3)}),
                        Row({Value(2)}), Row({Value(1)}), Row({Value(4)})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  std::unordered_set<Value> seen;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_TRUE(seen.insert(row[0]).second);
    ++count;
  }
  EXPECT_EQ(count, 4U);
  EXPECT_EQ(seen, std::unordered_set<Value>(
                      {Value(1), Value(2), Value(3), Value(4)}));
}

TEST_F(ExecutorTest, DistinctMultipleColumns) {
  std::vector<Row> rows{
      Row({Value(1), Value("a")}), Row({Value(1), Value("a")}),
      Row({Value(1), Value("b")}), Row({Value(2), Value("a")}),
      Row({Value(2), Value("a")}), Row({Value(2), Value("c")})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  std::unordered_set<Row> seen;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_TRUE(seen.insert(row).second);
    ++count;
  }
  EXPECT_EQ(count, 4U);
  EXPECT_TRUE(seen.contains(Row({Value(1), Value("b")})));
  EXPECT_TRUE(seen.contains(Row({Value(2), Value("c")})));
}

TEST_F(ExecutorTest, DistinctNullValueTreatedAsValue) {
  // std::hash<Value> maps kNull to a constant (it no longer throws), so
  // DistinctExecutor deduplicates NULL-containing rows like any other value.
  std::vector<Row> rows{Row({Value()}), Row({Value()}), Row({Value(5)})};
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  size_t count = 0;
  bool saw_null = false;
  while (distinct.Next(&row, &pos)) {
    if (row[0].IsNull()) {
      saw_null = true;
    }
    ++count;
  }
  EXPECT_EQ(count, 2U);
  EXPECT_TRUE(saw_null);
}

TEST_F(ExecutorTest, DistinctOverFullScanPreservesRowsAndPositions) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  DistinctExecutor distinct(std::make_shared<FullScan>(ctx.txn_, *tbl));
  LOG(INFO) << distinct;
  std::unordered_set<Row> expected(
      {Row({Value(0), Value("hello"), Value(1.2)}),
       Row({Value(3), Value("piyo"), Value(12.2)}),
       Row({Value(1), Value("world"), Value(4.9)}),
       Row({Value(2), Value("arise"), Value(4.14)})});
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_NE(expected.find(row), expected.end());
    ASSERT_TRUE(pos.IsValid());
    ++count;
  }
  EXPECT_EQ(count, 4U);
  std::stringstream ss;
  distinct.Dump(ss, 0);
  EXPECT_NE(ss.str().find("Distinct"), std::string::npos);
}

// ===== CrossJoin =====
TEST_F(ExecutorTest, CrossJoinProducesCartesianProduct) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value("a")}), Row({Value("b")})});
  CrossJoin join(left, right);
  std::unordered_set<Row> expected(
      {Row({Value(1), Value("a")}), Row({Value(1), Value("b")}),
       Row({Value(2), Value("a")}), Row({Value(2), Value("b")})});
  Row got;
  RowPosition pos;
  size_t count = 0;
  while (join.Next(&got, &pos)) {
    ASSERT_NE(expected.find(got), expected.end());
    ++count;
  }
  EXPECT_EQ(count, 4U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("CrossJoin"), std::string::npos);
}

TEST_F(ExecutorTest, CrossJoinEmptyRightSideYieldsNoRows) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  CrossJoin join(left, right);
  Row got;
  RowPosition pos;
  ASSERT_FALSE(join.Next(&got, &pos));
}

TEST_F(ExecutorTest, CrossJoinEmptyLeftSideYieldsNoRows) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value("a")}), Row({Value("b")})});
  CrossJoin join(left, right);
  Row got;
  ASSERT_FALSE(join.Next(&got, nullptr));
}

TEST_F(ExecutorTest, CrossJoinBothSidesEmptyYieldsNoRows) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  CrossJoin join(left, right);
  Row got;
  DataChunk chunk;
  ASSERT_FALSE(join.Next(&got, nullptr));
  EXPECT_EQ(join.NextBatch(&chunk, 8), 0U);
}

// ===== DeleteExecutor =====
TEST_F(ExecutorTest, DeleteRemovesAllRows) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto deleter = std::make_shared<DeleteExecutor>(
      ctx.txn_, *tbl, std::make_shared<FullScan>(ctx.txn_, *tbl));
  LOG(INFO) << *deleter;
  Row result;
  RowPosition rp;
  ASSERT_TRUE(deleter->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Delete Rows"), Value(4)}));
  ASSERT_FALSE(deleter->Next(&result, &rp));
  Row got;
  FullScan fs(ctx.txn_, *tbl);
  ASSERT_FALSE(fs.Next(&got, nullptr));
  std::stringstream ss;
  deleter->Dump(ss, 0);
  EXPECT_NE(ss.str().find("Delete"), std::string::npos);
}

TEST_F(ExecutorTest, DeleteEmptySourceCountsZero) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  auto deleter = std::make_shared<DeleteExecutor>(
      ctx.txn_, *tbl, std::make_shared<ConstantExecutor>(std::vector<Row>{}));
  Row result;
  ASSERT_TRUE(deleter->Next(&result, nullptr));
  EXPECT_EQ(result, Row({Value("Delete Rows"), Value(0)}));
  ASSERT_FALSE(deleter->Next(&result, nullptr));
}

// ===== ConstantExecutor =====
TEST_F(ExecutorTest, ConstantExecutorNextBatchSplitsRows) {
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  ConstantExecutor ce(std::move(rows));
  DataChunk chunk;
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 2U);
  EXPECT_EQ(chunk.RowAt(0), Row({Value(1)}));
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 1U);
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 0U);
  ConstantExecutor single(Row({Value(9)}));
  Row got;
  ASSERT_TRUE(single.Next(&got, nullptr));
  EXPECT_EQ(got, Row({Value(9)}));
  ASSERT_FALSE(single.Next(&got, nullptr));
  std::stringstream ss;
  ce.Dump(ss, 0);
  EXPECT_NE(ss.str().find("ConstantExecutor"), std::string::npos);
}

// ===== ZoneMap =====
TEST_F(ExecutorTest, ZoneMapTracksStats) {
  ZoneMap map;
  EXPECT_FALSE(map.Initialized());
  EXPECT_EQ(map.NullCount(), 0U);
  EXPECT_EQ(map.ValueCount(), 0U);
  EXPECT_FALSE(map.Minimum().has_value());
  EXPECT_FALSE(map.Maximum().has_value());
  map.Add(Value(10));
  map.Add(Value(3));
  map.Add(Value(7));
  map.Add(Value());
  EXPECT_TRUE(map.Initialized());
  EXPECT_EQ(map.NullCount(), 1U);
  EXPECT_EQ(map.ValueCount(), 3U);
  const std::optional<Value> minimum = map.Minimum();
  const std::optional<Value> maximum = map.Maximum();
  if (!minimum || !maximum) {
    GTEST_FAIL() << "failed";
    return;
  }
  EXPECT_EQ(*minimum, Value(3));
  EXPECT_EQ(*maximum, Value(10));
  map.Reset();
  EXPECT_FALSE(map.Initialized());
  EXPECT_EQ(map.NullCount(), 0U);
  EXPECT_EQ(map.ValueCount(), 0U);
  EXPECT_FALSE(map.Minimum().has_value());
  EXPECT_FALSE(map.Maximum().has_value());
}

TEST_F(ExecutorTest, ZoneMapMayMatchComparisons) {
  ZoneMap map;
  map.Add(Value(5));
  map.Add(Value(10));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kEquals, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kEquals, Value(7)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kEquals, Value(4)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kEquals, Value(11)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kNotEquals, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kNotEquals, Value(4)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kLessThan, Value(11)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kLessThan, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kLessThanEquals, Value(5)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kLessThanEquals, Value(4)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kGreaterThan, Value(4)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kGreaterThan, Value(10)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kGreaterThanEquals, Value(10)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kGreaterThanEquals, Value(11)));
}

TEST_F(ExecutorTest, ZoneMapMayMatchDegenerateCases) {
  ZoneMap single;
  single.Add(Value(7));
  EXPECT_FALSE(single.MayMatch(BinaryOperation::kNotEquals, Value(7)));
  EXPECT_TRUE(single.MayMatch(BinaryOperation::kNotEquals, Value(6)));
  EXPECT_TRUE(single.MayMatch(BinaryOperation::kAdd, Value(1)));
  EXPECT_FALSE(single.MayMatch(BinaryOperation::kEquals, Value()));
  ZoneMap empty;
  // An uninitialized zone map proves nothing: the producer may not have
  // maintained it, so MayMatch must stay conservative (§6.8).
  EXPECT_FALSE(empty.Initialized());
  EXPECT_TRUE(empty.MayMatch(BinaryOperation::kEquals, Value(1)));
  ZoneMap null_only;
  null_only.AddNull();
  // An initialized zone holding only NULLs can never satisfy a comparison.
  EXPECT_TRUE(null_only.Initialized());
  EXPECT_FALSE(null_only.MayMatch(BinaryOperation::kEquals, Value(1)));
  ZoneMap doubles;
  doubles.Add(Value(1.0));
  // Type-mismatched constants must be conservative: the comparison may match
  // after implicit conversion, so pruning is not allowed.
  EXPECT_TRUE(doubles.MayMatch(BinaryOperation::kEquals, Value(1)));
  EXPECT_TRUE(doubles.MayMatch(BinaryOperation::kLessThan, Value(2)));
}

// ===== Selection edge cases =====
TEST_F(ExecutorTest, SelectionConstantOnLeftIsFlipped) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto collect_keys = [&](const Expression& exp) {
    Selection sel(exp, tbl->GetSchema(),
                  std::make_shared<FullScan>(ctx.txn_, *tbl));
    std::vector<int64_t> keys;
    Row got;
    while (sel.Next(&got, nullptr)) {
      keys.push_back(got[0].value.int_value);
    }
    std::ranges::sort(keys);
    return keys;
  };
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(2)),
                                             BinaryOperation::kLessThan,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(2)),
                                             BinaryOperation::kLessThanEquals,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({2, 3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(3)),
                                             BinaryOperation::kGreaterThan,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(3)), BinaryOperation::kGreaterThanEquals,
                ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2, 3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(3)),
                                             BinaryOperation::kEquals,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(ConstantValueExp(Value(3)),
                                             BinaryOperation::kNotEquals,
                                             ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2}));
}

TEST_F(ExecutorTest, SelectionSupportsOrPredicate) {
  TransactionContext ctx = rs_->BeginContext();
  const StatusOr<std::shared_ptr<Table>> tbl_status = ctx.GetTable(kTableName);
  ASSERT_EQ(tbl_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_status.Value();
  Expression key_is_0 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(0)));
  Expression key_is_3 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(3)));
  Selection sel(BinaryExpressionExp(key_is_0, BinaryOperation::kOr, key_is_3),
                tbl->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *tbl));
  Row got;
  std::unordered_set<Value> keys;
  while (sel.Next(&got, nullptr)) {
    keys.insert(got[0]);
  }
  EXPECT_EQ(keys, std::unordered_set<Value>({Value(0), Value(3)}));
}

// ===== ParallelScan =====
TEST_F(ExecutorTest, ParallelScanEmptyTableReturnsNoRows) {
  constexpr std::string_view kTable = "EmptyScanTable";
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    const StatusOr<Table> table_status = rs_->CreateTable(writer, schema);
    ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1);
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk), 0U);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanNextScalarPath) {
  constexpr std::string_view kTable = "ParallelScalarTable";
  constexpr int64_t kRows = 500;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(key)})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1);
  LOG(INFO) << scan;
  std::unordered_set<int64_t> keys;
  keys.reserve(static_cast<size_t>(kRows));
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (scan.Next(&row, &pos)) {
    ASSERT_TRUE(keys.insert(row[0].value.int_value).second);
    ASSERT_TRUE(pos.IsValid());
    ++count;
  }
  EXPECT_EQ(count, static_cast<size_t>(kRows));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanProjectedColumns) {
  constexpr std::string_view kTable = "ParallelProjectionTable";
  constexpr int64_t kRows = 800;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable,
                  {Column("key", ValueType::kInt64),
                   Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({Value(key), Value("n" + std::to_string(key))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{0, 1});
  DataChunk chunk;
  size_t total = 0;
  std::unordered_set<std::string> names;
  names.reserve(static_cast<size_t>(kRows));
  while (scan.NextBatch(&chunk, 64) != 0) {
    ASSERT_EQ(chunk.ColumnCount(), 2U);
    for (size_t row = 0; row < chunk.Size(); ++row) {
      names.insert(chunk.ColumnAt(0).ValueAt(row).AsString());
    }
    total += chunk.Size();
  }
  EXPECT_EQ(total, static_cast<size_t>(kRows));
  EXPECT_EQ(names.size(), static_cast<size_t>(kRows));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanRejectsReorderedProjection) {
  constexpr std::string_view kTable = "ParallelProjectionTable";
  constexpr int64_t kRows = 800;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable,
                  {Column("key", ValueType::kInt64),
                   Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({Value(key), Value("n" + std::to_string(key))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{1, 0});
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk, 64), 0U);
  EXPECT_EQ(scan.GetStatus().GetCode(), StatusCode::kInvalidArgument);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanSplitsLargeChunksWhenAskedSmallBatches) {
  constexpr std::string_view kTable = "ParallelSplitTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(key)})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  const StatusOr<std::shared_ptr<Table>> table_status = reader.GetTable(kTable);
  ASSERT_EQ(table_status.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_status.Value();
  ParallelScan scan(reader.txn_, *table, 4, 1);
  DataChunk chunk;
  ASSERT_GT(scan.NextBatch(&chunk, 1000), 0U);
  size_t total = chunk.Size();
  size_t batches = 0;
  while (true) {
    const size_t got = scan.NextBatch(&chunk, 1);
    if (got == 0) {
      break;
    }
    EXPECT_EQ(got, 1U);
    total += got;
    ++batches;
  }
  EXPECT_EQ(total, static_cast<size_t>(kRows));
  EXPECT_GT(batches, 10U);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

// ===== ParallelAggregationExecutor =====
TEST_F(ExecutorTest, ParallelAggregationCountStarAndExpressionChild) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(100);
  std::vector<NamedExpression> aggregates = {
      NamedExpression(
          "count_star",
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"))),
      NamedExpression("sum_plus_one",
                      AggregateExpressionExp(
                          AggregationType::kSum,
                          BinaryExpressionExp(ColumnValueExp("value"),
                                              BinaryOperation::kAdd,
                                              ConstantValueExp(Value(1)))))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(100));
  EXPECT_EQ(result[1], Value(5050));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelAggregationRespectsWhereFilter) {
  // PRODUCTION BUG (fixed): the typed fast paths (row count / column scan)
  // ignored an aggregate's FILTER(WHERE) predicate, so a pushed-down filter
  // silently over-counted. The generic path must apply it.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(100);  // values 0..99
  auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                   ColumnValueExp("value"));
  sum->SetWhereFilter(BinaryExpressionExp(ColumnValueExp("value"),
                                          BinaryOperation::kGreaterThan,
                                          ConstantValueExp(Value(50))));
  std::vector<NamedExpression> aggregates = {
      NamedExpression("sum_gt_50", std::move(sum))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  // SyntheticBatchExecutor yields 0..99; value > 50 keeps 51..99 = 3675
  // (the unfiltered sum would be 4950).
  EXPECT_EQ(result[0], Value(3675));
}

TEST_F(ExecutorTest, ParallelAggregationAverageOverEmptyInput) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "avg",
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_TRUE(result[0].IsNull());
}

TEST_F(ExecutorTest, ParallelAggregationNextBatchAndDump) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(50);
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "count", AggregateExpressionExp(AggregationType::kCount,
                                      ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        3);
  LOG(INFO) << aggregate;
  DataChunk chunk;
  EXPECT_EQ(aggregate.NextBatch(&chunk, 8), 1U);
  EXPECT_EQ(chunk.RowAt(0)[0], Value(50));
  EXPECT_EQ(aggregate.NextBatch(&chunk, 8), 0U);
}

TEST_F(ExecutorTest, ParallelAggregationMissingColumnThrows) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input =
      std::make_shared<ConstantExecutor>(std::vector<Row>{Row({Value(2)})});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum_missing", AggregateExpressionExp(AggregationType::kSum,
                                            ColumnValueExp("missing")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
  EXPECT_NE(aggregate.GetStatus(), Status::kSuccess);
}

}  // namespace tinylamb
