/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include <cstdlib>

#include "common/platform/process.hpp"
#include "common/test_util.hpp"
#include "executor/relational_regression_test.hpp"
#include "table/tlcs_file.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, ProjectedColumnStoresKeepQueriesEquivalent) {
  RelationalRun(*rs_,
                "CREATE TABLE ProjectionCache(unused INT64, k INT64, s STRING, "
                "n INT64, plain STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("ProjectionCache"));
    for (int64_t i = 0; i < 12000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  context.txn_,
                  Row({Value(-1), Value(i % 300),
                       i % 17 == 0 ? Value()
                                   : Value("key" + std::to_string(i % 300)),
                       i % 13 == 0 ? Value() : Value(i % 101),
                       Value("key" + std::to_string(i % 300))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  const std::vector<std::string> queries{
      "SELECT COUNT(*) FROM ProjectionCache WHERE n>90",
      "SELECT COUNT(*),SUM(n),MIN(n),MAX(n) FROM ProjectionCache WHERE k>5 AND "
      "n<90",
      "SELECT COUNT(*) FROM ProjectionCache WHERE s LIKE '%2%'",
      "SELECT COUNT(*) FROM ProjectionCache WHERE plain NOT LIKE '%2%' AND "
      "k>10",
      "SELECT COUNT(*) FROM ProjectionCache WHERE n IS NULL OR k=1",

      "SELECT COUNT(*) FROM ProjectionCache",
      "SELECT SUM(n),AVG(n),MIN(n),MAX(n) FROM ProjectionCache",
      "SELECT k,COUNT(DISTINCT n) d FROM ProjectionCache WHERE k>5 GROUP BY k "
      "ORDER BY d DESC,k LIMIT 20",
      "SELECT REGEXP_REPLACE(s,'[0-9]+','') k,COUNT(*) "
      "c,MIN(s),AVG(BYTE_LENGTH(s)) FROM ProjectionCache WHERE s<>'' GROUP BY "
      "k HAVING c>10 ORDER BY k",
      "SELECT k,n FROM ProjectionCache WHERE n>90 ORDER BY n DESC,k LIMIT 20",
      "SELECT COUNT(DISTINCT n) FROM ProjectionCache WHERE k>200",
      "SELECT k,AVG(CAST(BYTE_LENGTH(plain) AS FLOAT64)) l,COUNT(*) c FROM "
      "ProjectionCache WHERE plain<>'' GROUP BY k HAVING COUNT(*)>10 ORDER BY "
      "l "
      "DESC,k LIMIT 25"};
  std::vector<std::vector<Row>> expected;
  expected.reserve(queries.size());
  for (const auto& query : queries) {
    expected.push_back(RelationalRun(*rs_, query));
  }
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{7}}) {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("ProjectionCache"));
    table->SetLSTableStore(nullptr);
    table->SetProjectedLSTableStore(nullptr);
    table->RejectLSTableCapacity(table->LayoutGeneration());
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (size_t i = 0; i < queries.size(); ++i) {
      SCOPED_TRACE(queries[i]);
      EXPECT_EQ(RelationalRun(*rs_, queries[i]), expected[i]);
      EXPECT_EQ(table->GetLSTableStore(), nullptr);
    }
    ASSERT_NE(table->GetProjectedLSTableStore(), nullptr);
    ASSERT_SUCCESS(context.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, PartitionedGroupMergeKeepsKeysAndStates) {
  constexpr int64_t kGroups = 4096;
  constexpr int64_t kRepeats = 16;
  for (const bool composite : {false, true}) {
    std::vector<Row> previous;
    for (const size_t workers : {size_t{1}, size_t{4}, size_t{4}}) {
      std::atomic<size_t> active{1};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto context = rs_->BeginContext();
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      relational_detail::Relation input;
      input.schema = Schema("merge_groups", {Column("g", ValueType::kInt64),
                                             Column("h", ValueType::kVarChar),
                                             Column("v", ValueType::kDouble)});
      for (int64_t repeat = 0; repeat < kRepeats; ++repeat) {
        for (int64_t key = 0; key < kGroups; ++key) {
          input.AddRow(
              Row({key == 0 ? Value() : Value(key),
                   Value(key % 2 == 0 ? "even" : "odd"),
                   key == 1 || repeat % 4 == 0 ? Value() : Value(0.25)}));
        }
      }
      const auto aggregate = [](AggregationType type) {
        return AggregateExpressionExp(type, ColumnValueExp("v"));
      };
      SelectStatement statement(
          {NamedExpression("g", ColumnValueExp("g")),
           NamedExpression("n", aggregate(AggregationType::kCount)),
           NamedExpression("s", aggregate(AggregationType::kSum)),
           NamedExpression("a", aggregate(AggregationType::kAvg)),
           NamedExpression("lo", aggregate(AggregationType::kMin)),
           NamedExpression("hi", aggregate(AggregationType::kMax)),
           NamedExpression(
               "d", std::make_shared<AggregateExpression>(
                        AggregationType::kCount, ColumnValueExp("v"), true))},
          {}, nullptr);
      statement.SetGroupBy(composite
                               ? std::vector<Expression>{ColumnValueExp("g"),
                                                         ColumnValueExp("h")}
                               : std::vector<Expression>{ColumnValueExp("g")});
      // An outer scope keeps this test on the generic grouped Project path.
      const Row outer_row;
      const Schema outer_schema;
      const relational_detail::Scope outer{.row = &outer_row,
                                           .schema = &outer_schema};
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, output,
          relational_detail::FinishQuery(context, statement, std::move(input),
                                         &outer, {}, false));
      ASSERT_EQ(output.rows.size(), static_cast<size_t>(kGroups));
      EXPECT_EQ(runtime.partitioned_aggregate_merges, workers == 1 ? 0U : 1U);
      for (const auto& row : output.rows) {
        const bool all_null = row[0] == Value(1);
        EXPECT_EQ(row[1], Value(all_null ? 0 : 12));
        EXPECT_EQ(row[2], all_null ? Value() : Value(3.0));
        for (size_t column = 3; column < 6; ++column) {
          EXPECT_EQ(row[column], all_null ? Value() : Value(0.25));
        }
        EXPECT_EQ(row[6], Value(all_null ? 0 : 1));
      }
      if (workers == 4) {
        if (!previous.empty()) {
          EXPECT_EQ(previous, output.rows);
        }
        previous = output.rows;
      }
      context.set_execution_runtime(nullptr);
      EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
    }
  }
}

TEST_F(RelationalRegressionTest,
       GroupingSetsReleaseBuildReservationBeforeEmit) {
  auto context = rs_->BeginContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  relational_detail::Relation input;
  input.schema = Schema("group_budget", {Column("g", ValueType::kInt64),
                                         Column("h", ValueType::kInt64)});
  constexpr size_t kRows = 32768;
  for (size_t i = 0; i < kRows; ++i) {
    input.rows.emplace_back(
        Row({Value(static_cast<int64_t>(i % 8192)), Value(1)}));
  }
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("h"));
  SelectStatement fine(
      {NamedExpression("g", ColumnValueExp("g")), NamedExpression("n", count)},
      {}, nullptr);
  fine.SetGroupBy({ColumnValueExp("g"), ColumnValueExp("h")});
  SelectStatement coarse = fine;
  coarse.SetGroupBy({ColumnValueExp("g")});
  SelectStatement total({NamedExpression("g", ConstantValueExp(Value())),
                         NamedExpression("n", count)},
                        {}, nullptr);
  total.SetGroupAll(true);
  const std::vector<const SelectStatement*> sets{&fine, &coarse, &total};
  const size_t per_row =
      (3 * sizeof(size_t)) + (size_t{3} * (128 + 64)) + (6 * sizeof(Value));
  const size_t build_bound = (kRows + 4096) * per_row;
  struct RestoreBudget {
    RestoreBudget() = default;
    RestoreBudget(const RestoreBudget&) = delete;
    RestoreBudget& operator=(const RestoreBudget&) = delete;
    RestoreBudget(RestoreBudget&&) = delete;
    RestoreBudget& operator=(RestoreBudget&&) = delete;
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  // The initial worst-case reservation fits, but leaves no room for output.
  QueryMemoryBudget::Global().ResetForTest(((build_bound + 4096) * 5 / 4) + 8);
  std::atomic<size_t> active{1};
  ScopedQueryParallelismBudget budget(&active, 1);
  ASSIGN_OR_ASSERT_FAIL(
      std::optional<std::vector<relational_detail::Relation>>, result,
      relational_detail::TryCompactGroupingSets(context, sets, input));
  if (!result) {
    FAIL() << "expected specialized aggregation";
  }
  ASSERT_EQ(result->size(), 3U);
  EXPECT_EQ((*result)[0].TotalRows(), 8192U);
  EXPECT_EQ((*result)[1].TotalRows(), 8192U);
  EXPECT_EQ((*result)[2].TotalRows(), 1U);
  for (const auto& relation : *result) {
    EXPECT_FALSE(relation.HasSpill());
  }
  EXPECT_EQ(runtime.relation_spills, 0U);
  EXPECT_EQ((*result)[2].rows[0][1], Value(static_cast<int64_t>(kRows)));
  result.reset();
  context.set_execution_runtime(nullptr);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, GroupingSetsBudgetUsesPromotedSlimState) {
  auto context = rs_->BeginContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  relational_detail::Relation input;
  input.schema = Schema("group_budget", {Column("g", ValueType::kInt64),
                                         Column("h", ValueType::kInt64),
                                         Column("v", ValueType::kDouble)});
  constexpr size_t kRows = 32768;
  for (size_t i = 0; i < kRows; ++i) {
    input.rows.emplace_back(
        Row({Value(static_cast<int64_t>(i % 8192)), Value(1), Value(1.0)}));
  }
  const auto count = AggregateExpressionExp(
      AggregationType::kSum,
      FunctionCallExp("coalesce",
                      {ColumnValueExp("v"), ConstantValueExp(Value(0))}));
  SelectStatement fine(
      {NamedExpression("g", ColumnValueExp("g")), NamedExpression("n", count)},
      {}, nullptr);
  fine.SetGroupBy({ColumnValueExp("g"), ColumnValueExp("h")});
  SelectStatement coarse = fine;
  coarse.SetGroupBy({ColumnValueExp("g")});
  SelectStatement total({NamedExpression("g", ConstantValueExp(Value())),
                         NamedExpression("n", count)},
                        {}, nullptr);
  total.SetGroupAll(true);
  const std::vector<const SelectStatement*> sets{&fine, &coarse, &total};
  const size_t per_row =
      (3 * sizeof(size_t)) + (size_t{3} * (128 + 64)) + (6 * sizeof(Value));
  const size_t build_bound = (kRows + 4096) * per_row;
  struct RestoreBudget {
    RestoreBudget() = default;
    RestoreBudget(const RestoreBudget&) = delete;
    RestoreBudget& operator=(const RestoreBudget&) = delete;
    RestoreBudget(RestoreBudget&&) = delete;
    RestoreBudget& operator=(RestoreBudget&&) = delete;
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  // The initial worst-case reservation fits, but leaves no room for output.
  QueryMemoryBudget::Global().ResetForTest(((build_bound + 4096) * 5 / 4) + 8);
  std::atomic<size_t> active{1};
  ScopedQueryParallelismBudget budget(&active, 1);
  ASSIGN_OR_ASSERT_FAIL(
      std::optional<std::vector<relational_detail::Relation>>, result,
      relational_detail::TryCompactGroupingSets(context, sets, input));
  if (!result) {
    FAIL() << "expected specialized aggregation";
  }
  ASSERT_EQ(result->size(), 3U);
  EXPECT_EQ((*result)[0].TotalRows(), 8192U);
  EXPECT_EQ((*result)[1].TotalRows(), 8192U);
  EXPECT_EQ((*result)[2].TotalRows(), 1U);
  for (const auto& relation : *result) {
    EXPECT_FALSE(relation.HasSpill());
  }
  EXPECT_EQ(runtime.relation_spills, 0U);
  EXPECT_EQ((*result)[2].rows[0][1], Value(static_cast<double>(kRows)));
  result.reset();
  context.set_execution_runtime(nullptr);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, ParallelResidualProbePadsOnlyRejectedMatches) {
  auto context = rs_->BeginContext();
  constexpr size_t kRepeats = 8192;
  for (const bool text : {false, true}) {
    const auto key = [text](int value) {
      return text ? Value(std::to_string(value)) : Value(value);
    };
    for (const auto kind : {JoinType::kInner, JoinType::kLeft, JoinType::kRight,
                            JoinType::kFull}) {
      for (const size_t workers : {size_t{1}, size_t{4}}) {
        relational_detail::Relation left, right;
        const auto key_type = text ? ValueType::kVarChar : ValueType::kInt64;
        left.schema = Schema(
            "l", {Column("lk", key_type), Column("lv", ValueType::kInt64)});
        right.schema = Schema(
            "r", {Column("rk", key_type), Column("rv", ValueType::kInt64)});
        for (size_t i = 0; i < kRepeats; ++i) {
          left.rows.emplace_back(Row({key(1), Value(3)}));
          left.rows.emplace_back(Row({key(1), Value(7)}));
          left.rows.emplace_back(Row({Value(), Value(0)}));
          left.rows.emplace_back(Row({key(3), Value(0)}));
        }
        right.rows = {Row({key(1), Value(5)}), Row({key(1), Value(9)}),
                      Row({key(1), Value()}), Row({Value(), Value(0)}),
                      Row({key(4), Value(0)})};
        SelectSource source;
        source.join_type = kind;
        source.join_condition = BinaryExpressionExp(
            BinaryExpressionExp(ColumnValueExp("lk"), BinaryOperation::kEquals,
                                ColumnValueExp("rk")),
            BinaryOperation::kAnd,
            BinaryExpressionExp(ColumnValueExp("lv"),
                                BinaryOperation::kGreaterThan,
                                ColumnValueExp("rv")));
        std::atomic<size_t> active{1};
        ScopedQueryParallelismBudget budget(&active, workers);
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, result,
            relational_detail::Join(context, std::move(left), std::move(right),
                                    source, nullptr, {}));
        const size_t left_unmatched =
            kind == JoinType::kLeft || kind == JoinType::kFull ? kRepeats * 3
                                                               : 0;
        const size_t right_unmatched =
            kind == JoinType::kRight || kind == JoinType::kFull ? 4 : 0;
        EXPECT_EQ(result.TotalRows(),
                  kRepeats + left_unmatched + right_unmatched);
        size_t accepted = 0, padded_left = 0, padded_right = 0;
        ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
          if (row[1].IsNull()) {
            ++padded_left;
          } else if (row[2].IsNull() && row[3].IsNull()) {
            ++padded_right;
          } else {
            EXPECT_EQ(row[1], Value(7));
            EXPECT_EQ(row[3], Value(5));
            ++accepted;
          }
        }));
        EXPECT_EQ(accepted, kRepeats);
        EXPECT_EQ(padded_left, right_unmatched);
        EXPECT_EQ(padded_right, left_unmatched);
      }
    }
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ParallelResidualPreservesShortCircuitAndErrors) {
  auto context = rs_->BeginContext();
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const bool reject_first : {false, true}) {
      relational_detail::Relation left, right;
      left.schema = Schema("l", {Column("lk", ValueType::kInt64),
                                 Column("lv", ValueType::kDouble)});
      right.schema = Schema("r", {Column("rk", ValueType::kInt64),
                                  Column("rv", ValueType::kDouble)});
      left.rows.assign(32768, Row({Value(1), Value(1.0)}));
      right.rows.emplace_back(Row({Value(1), Value(0.0)}));
      auto residual = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("lv"), BinaryOperation::kDivide,
                              ColumnValueExp("rv")),
          BinaryOperation::kGreaterThan, ConstantValueExp(Value(0.0)));
      if (reject_first) {
        residual =
            BinaryExpressionExp(BinaryExpressionExp(ColumnValueExp("lv"),
                                                    BinaryOperation::kLessThan,
                                                    ColumnValueExp("rv")),
                                BinaryOperation::kAnd, residual);
      }
      SelectSource source;
      source.join_type = JoinType::kLeft;
      source.join_condition = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("lk"), BinaryOperation::kEquals,
                              ColumnValueExp("rk")),
          BinaryOperation::kAnd, residual);
      auto output = relational_detail::Join(
          context, std::move(left), std::move(right), source, nullptr, {});
      if (reject_first) {
        ASSERT_TRUE(output.HasValue()) << output.GetStatus();
        ASSERT_EQ(output.Value().TotalRows(), 32768U);
        for (const auto& row : output.Value().rows) {
          EXPECT_TRUE(row[2].IsNull());
          EXPECT_TRUE(row[3].IsNull());
        }
      } else {
        EXPECT_FALSE(output.HasValue());
      }
    }
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, CompactPartitionPreservesFloatOrderAndHaving) {
  auto context = rs_->BeginContext();
  std::vector<Row> reference;
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    relational_detail::Relation input;
    input.schema = Schema("compact", {Column("k", ValueType::kVarChar),
                                      Column("v", ValueType::kDouble)});
    for (size_t i = 0; i < 65536; ++i) {
      const size_t key = i % 4096;
      const size_t cycle = (i / 4096) % 4;
      const double value = cycle == 0   ? 1e16
                           : cycle == 1 ? 1.0
                           : cycle == 2 ? -1e16
                                        : 0.25;
      input.rows.emplace_back(
          Row({key == 1 ? Value() : Value("key" + std::to_string(key)),
               key == 0 ? Value() : Value(value)}));
    }
    const auto sum =
        AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"));
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("v"));
    SelectStatement statement(
        {NamedExpression("k", ColumnValueExp("k")), NamedExpression("s", sum),
         NamedExpression("n", count)},
        {}, nullptr);
    statement.SetGroupBy({ColumnValueExp("k")});
    statement.SetHaving(BinaryExpressionExp(
        count, BinaryOperation::kGreaterThan, ConstantValueExp(Value(0))));
    const std::vector<const AggregateExpression*> aggregates{
        &sum->AsAggregateExpression(), &count->AsAggregateExpression()};
    ASSIGN_OR_ASSERT_FAIL(std::optional<relational_detail::Relation>, result,
                          relational_detail::TryCompactRelationAggregate(
                              context, statement, input, aggregates));
    if (!result) {
      FAIL() << "expected specialized aggregation";
    }
    ASSERT_EQ(result->TotalRows(), 4095U);
    EXPECT_EQ(result->schema.GetColumn(1).Type(), ValueType::kDouble);
    EXPECT_EQ(runtime.typed_parallel_aggregations, workers == 1 ? 0U : 1U);
    for (const auto& row : result->rows) {
      EXPECT_EQ(row[1], Value(0.25));
      EXPECT_EQ(row[2], Value(16));
    }
    if (workers == 1) {
      reference = result->rows;
    } else {
      EXPECT_EQ(result->rows, reference);
    }
    context.set_execution_runtime(nullptr);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, JoinTypeMismatchedEqualityStaysResidual) {
  // A column equality whose sides live in different type domains can never
  // collide inside the hash-key encoding, so EqualityKeys must leave it in
  // the residual: evaluated per candidate pair it raises the same error the
  // AST evaluator produces, and as the sole join term the nested-loop path
  // evaluates it for every pair.
  RelationalRun(*rs_, "CREATE TABLE ResidualTypeA (k INT64, i INT64)");
  RelationalRun(*rs_, "CREATE TABLE ResidualTypeB (k INT64, s VARCHAR)");
  RelationalRun(*rs_, "INSERT INTO ResidualTypeA VALUES (1, 10)");
  RelationalRun(*rs_, "INSERT INTO ResidualTypeB VALUES (1, 'x')");
  TransactionContext context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  engine.SetForceRelationalFallback(true);

  // Sole mismatched equality: no usable equi key remains, so the join falls
  // back to nested-loop evaluation and surfaces the type error.
  auto sole = engine.Prepare(
      context,
      "SELECT a.k FROM ResidualTypeA a JOIN ResidualTypeB b ON a.i = b.s");
  ASSERT_TRUE(sole.HasValue()) << engine.LastError();
  Row row;
  while (sole.Value()->Next(&row, nullptr)) {
  }
  EXPECT_NE(sole.Value()->GetStatus(), Status::kSuccess);

  // Valid equi key plus the mismatched residual: the key collides, the
  // candidate pair forms, and the residual raises the same error.
  auto keyed =
      engine.Prepare(context,
                     "SELECT a.k FROM ResidualTypeA a JOIN ResidualTypeB b "
                     "ON a.k = b.k AND a.i = b.s");
  ASSERT_TRUE(keyed.HasValue()) << engine.LastError();
  while (keyed.Value()->Next(&row, nullptr)) {
  }
  EXPECT_NE(keyed.Value()->GetStatus(), Status::kSuccess);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, JoinNumericPromotionEqualityMatches) {
  // INT64 = DOUBLE is equality-compatible through numeric promotion even
  // though the hash-key encoding keeps the two type domains apart; leaving
  // the predicate as a residual preserves the matches a type-blind key
  // extraction would silently drop.
  RelationalRun(*rs_, "CREATE TABLE ResidualNumA (i INT64)");
  RelationalRun(*rs_, "CREATE TABLE ResidualNumB (d DOUBLE)");
  RelationalRun(*rs_, "INSERT INTO ResidualNumA VALUES (10)");
  RelationalRun(*rs_, "INSERT INTO ResidualNumB VALUES (10.0)");
  TransactionContext context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  engine.SetForceRelationalFallback(true);
  auto prepared = engine.Prepare(
      context,
      "SELECT a.i FROM ResidualNumA a JOIN ResidualNumB b ON a.i = b.d");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  ASSERT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].values_[0].value.int_value, 10);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, GroupOutputPushPreservesOuterFanoutAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE PushDim(d INT64)");
  RelationalRun(*rs_, "CREATE TABLE PushL(d INT64,k INT64,g INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE PushR(d INT64,k INT64,g INT64,v INT64)");
  RelationalRun(*rs_, "INSERT INTO PushDim VALUES (1)");
  RelationalRun(*rs_,
                "INSERT INTO PushL VALUES (1,1,10,2),(1,1,10,3),"
                "(1,2,11,1),(1,3,12,1),(1,NULL,13,4)");
  RelationalRun(*rs_,
                "INSERT INTO PushR VALUES (1,1,20,7),(1,1,21,8),"
                "(1,2,22,NULL),(1,NULL,23,9)");
  const std::string prefix =
      "WITH l AS (SELECT f.k,f.g,SUM(f.v) AS v FROM PushL f JOIN PushDim d "
      "ON f.d=d.d GROUP BY f.k,f.g), "
      "r AS (SELECT f.k,f.g,SUM(f.v) AS v FROM PushR f JOIN PushDim d "
      "ON f.d=d.d GROUP BY f.k,f.g) ";
  const std::string select =
      "SELECT l.k,l.g,l.v,r.g,r.v FROM l LEFT JOIN r ON l.k=r.k ";
  const std::string sql =
      prefix + select +
      "WHERE COALESCE(r.v,0)>0 OR l.k=3 OR l.k IS NULL ORDER BY l.g,r.g";
  const std::vector<Row> expected{
      Row({Value(1), Value(10), Value(5), Value(20), Value(7)}),
      Row({Value(1), Value(10), Value(5), Value(21), Value(8)}),
      Row({Value(3), Value(12), Value(1), Value(), Value()}),
      Row({Value(), Value(13), Value(4), Value(), Value()})};
  for (size_t workers : {1U, 4U}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql, /*force_relational=*/true), expected);
    EXPECT_NE(ExplainAnalyze(*rs_, sql, /*force_relational=*/true)
                  .find("pushed_group_joins=1"),
              std::string::npos);
  }
  EXPECT_EQ(RelationalRun(*rs_, prefix + select +
                                    "WHERE COALESCE(r.v,0)>0 ORDER BY l.g,r.g"),
            std::vector<Row>(expected.begin(), expected.begin() + 2));
  auto all = RelationalRun(*rs_, prefix + select + "ORDER BY l.g,r.g");
  ASSERT_EQ(all.size(), 5U);
  EXPECT_EQ(all[2], Row({Value(2), Value(11), Value(1), Value(22), Value()}));
  RelationalRun(*rs_, "DELETE FROM PushR");
  EXPECT_EQ(RelationalRun(*rs_, prefix + select + "WHERE COALESCE(r.v,0)>0"),
            std::vector<Row>{});
  EXPECT_EQ(RelationalRun(*rs_, prefix + select + "ORDER BY l.g").size(), 4U);
}

TEST_F(RelationalRegressionTest, GroupOutputPushChainsBuildsAndKeepsErrors) {
  const std::string prefix =
      "WITH a AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN KeyTable d "
      "ON f.k=d.k GROUP BY f.k),"
      "b AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN SmallKeys d "
      "ON f.k=d.k GROUP BY f.k),"
      "c AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN SmallKeys d "
      "ON f.k=d.k GROUP BY f.k) ";
  const std::string tail =
      "SELECT a.k,a.v,b.v,c.v FROM a LEFT JOIN b ON a.k=b.k "
      "LEFT JOIN c ON a.k=c.k ";
  const std::string sql =
      prefix + tail +
      "WHERE COALESCE(b.v,0)>0 OR COALESCE(c.v,0)>0 ORDER BY a.k";
  std::vector<Row> expected;
  expected.reserve(4);
  for (int k = 1; k <= 4; ++k) {
    expected.emplace_back(
        std::vector<Value>{Value(k), Value(20), Value(20), Value(20)});
  }
  EXPECT_EQ(RelationalRun(*rs_, sql, /*force_relational=*/true), expected);
  EXPECT_NE(ExplainAnalyze(*rs_, sql, /*force_relational=*/true)
                .find("pushed_group_joins=1"),
            std::string::npos);
  // A throwing WHERE stays on the general path; a rejecting right-side
  // conjunct must not license erasing its arithmetic.
  const std::string arithmetic = prefix + tail + "WHERE a.v/(b.v-20)>0";
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  engine.SetForceRelationalFallback(true);
  auto executed = engine.Execute(context, arithmetic);
  if (executed.HasValue()) {
    executed.Value().Drain();
    EXPECT_NE(executed.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(executed.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, GroupOutputSinkBatchesDeclinesAndErrors) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64)});
  constexpr size_t count = 32771;
  for (size_t k = 0; k < count; ++k) {
    left.rows.emplace_back(
        std::vector<Value>{Value(static_cast<int64_t>(k)), Value(3)});
    right.rows.emplace_back(std::vector<Value>{Value(static_cast<int64_t>(k))});
  }
  const auto key = ColumnValueExp(ColumnName("f", "k"));
  const auto sum = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp(ColumnName("f", "v")));
  const std::vector<Expression> predicates{BinaryExpressionExp(
      key, BinaryOperation::kEquals, ColumnValueExp(ColumnName("d", "k")))};
  std::atomic<size_t> active{1};
  ScopedQueryParallelismBudget budget(&active, 4);
  for (int mode = 0; mode < 3; ++mode) {
    std::vector<int64_t> received(count, -1);
    relational_detail::GroupedOutputSink sink{
        .begin = [&](const Schema& schema, size_t rows,
                     size_t) -> StatusOr<bool> {
          EXPECT_EQ(schema.ColumnCount(), 2U);
          EXPECT_EQ(rows, count);
          return mode != 1;
        },
        .push = [&](size_t, size_t first,
                    std::span<const Value> cells) -> Status {
          EXPECT_LE(cells.size(),
                    2 * relational_detail::GroupedOutputSink::kBatchRows);
          if (mode == 2) {
            return Status::kNoSpace;
          }
          for (size_t r = 0; r < cells.size() / 2; ++r) {
            EXPECT_EQ(cells[(r * 2) + 1], Value(3));
            received[first + r] = cells[r * 2].value.int_value;
          }
          return Status::kSuccess;
        }};
    relational_detail::GroupedJoinFusionSpec fusion;
    fusion.group_keys = {key};
    fusion.aggregates = {sum.get()};
    fusion.count_star = {false};
    fusion.select_items = {key, sum};
    fusion.select_names = {"k", "s"};
    fusion.eligible = true;
    fusion.output_sink = &sink;
    auto output = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    if (mode == 2) {
      EXPECT_EQ(output.GetStatus(), Status::kNoSpace);
      continue;
    }
    ASSERT_TRUE(output.HasValue());
    const auto& opt_rel = output.Value();
    if (!opt_rel.has_value()) {
      FAIL();
      return;
    }
    const auto& rel = *opt_rel;
    if (mode == 1) {
      EXPECT_FALSE(sink.used);
      EXPECT_EQ(rel.rows.size(), count);
    } else {
      EXPECT_TRUE(sink.used);
      EXPECT_TRUE(rel.rows.empty());
      std::sort(received.begin(), received.end());
      for (size_t k = 0; k < count; ++k) {
        EXPECT_EQ(received[k], static_cast<int64_t>(k));
      }
    }
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       GroupNumericBuildOwnsCellsAndPreservesWideKeys) {
  const size_t before = QueryMemoryBudget::Global().Used();
  {
    relational_detail::GroupedNumericHash table({0, 1});
    auto sink = table.Sink();
    Schema schema(
        "", {Column("k", ValueType::kInt64), Column("n", ValueType::kInt64),
             Column("v", ValueType::kDouble)});
    constexpr size_t count = 16387;
    auto begun = sink.begin(schema, count, 4);
    ASSERT_TRUE(begun.HasValue());
    ASSERT_TRUE(begun.Value());
    ASSERT_SUCCESS(RunParallelTasks(4, [&](size_t worker) {
      std::vector<Value> cells;
      for (size_t r = count * worker / 4; r < count * (worker + 1) / 4; ++r) {
        const int64_t key =
            std::numeric_limits<int64_t>::min() + static_cast<int64_t>(r % 97);
        cells = {Value(key), r % 11 == 0 ? Value() : Value(INT64_MAX),
                 Value(static_cast<double>(r))};
        EXPECT_EQ(sink.push(worker, r, cells), Status::kSuccess);
      }
      // No retained references to the transient input cells.
      cells.clear();
    }));
    ASSERT_TRUE(table.Supported());
    std::vector<int64_t> seen;
    const std::array<size_t, 2> keys{0, 1};
    const std::array<Value, 2> probe{Value(INT64_MIN + 7), Value(INT64_MAX)};
    table.Matches(probe, keys, [&](auto row) {
      seen.push_back(static_cast<int64_t>(row[2].Get().value.double_value));
    });
    std::sort(seen.begin(), seen.end());
    std::vector<int64_t> expected;
    expected.reserve(count);
    for (size_t r = 0; r < count; ++r) {
      if (r % 97 == 7 && r % 11 != 0) {
        expected.push_back(static_cast<int64_t>(r));
      }
    }
    EXPECT_EQ(seen, expected);
  }
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), before);
  // Tiny budget declines before receiving data; the normal spill-capable
  // relational path can still be selected by the caller.
  struct Restore {
    size_t limit = QueryMemoryBudget::Global().Limit();
    Restore() = default;
    Restore(const Restore&) = delete;
    Restore& operator=(const Restore&) = delete;
    Restore(Restore&&) = delete;
    Restore& operator=(Restore&&) = delete;
    ~Restore() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(128);
  relational_detail::GroupedNumericHash refused({0});
  auto sink = refused.Sink();
  auto begun =
      sink.begin(Schema("", {Column("k", ValueType::kInt64)}), 1000, 1);
  ASSERT_TRUE(begun.HasValue());
  EXPECT_FALSE(begun.Value());
}

TEST_F(RelationalRegressionTest, PredicateTransferPrunesPreservedGroupInputs) {
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            context, Schema("TransferFact", {Column("d", ValueType::kInt64),
                                             Column("k", ValueType::kInt64),
                                             Column("c", ValueType::kInt64),
                                             Column("v", ValueType::kInt64)})));
    for (int64_t k = 0; k < 2048; ++k) {
      for (int n = 0; n < 3; ++n) {
        ASSERT_SUCCESS(fact.Insert(context.txn_, Row({Value(1), Value(k),
                                                      Value(k % 7), Value(1)}))
                           .GetStatus());
      }
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  RelationalRun(*rs_, "CREATE TABLE TransferDim(d INT64)");
  RelationalRun(*rs_, "INSERT INTO TransferDim VALUES(1)");
  RelationalRun(*rs_,
                "CREATE TABLE TransferB(k INT64,c INT64,g INT64,v INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE TransferC(k INT64,c INT64,g INT64,v INT64)");
  RelationalRun(
      *rs_, "INSERT INTO TransferB VALUES(7,0,1,-3),(7,0,2,4),(NULL,0,3,100)");
  RelationalRun(
      *rs_, "INSERT INTO TransferC VALUES(7,0,1,5),(17,3,1,2),(9999,0,1,7)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, build,
                          context.GetTable("TransferB"));
    for (int64_t k = 10000; k < 12048; ++k) {
      ASSERT_SUCCESS(build
                         ->Insert(context.txn_, Row({Value(k), Value(k % 7),
                                                     Value(1), Value(1)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  const std::string prefix =
      "WITH a AS (SELECT f.k,f.c,SUM(f.v) AS v FROM TransferFact f JOIN "
      "TransferDim d ON f.d=d.d GROUP BY f.k,f.c),"
      "b AS (SELECT f.k,f.c,f.g,SUM(f.v) AS v FROM TransferB f JOIN "
      "TransferDim d ON d.d=1 GROUP BY f.k,f.c,f.g),"
      "c AS (SELECT f.k,f.c,f.g,SUM(f.v) AS v FROM TransferC f JOIN "
      "TransferDim d ON d.d=1 GROUP BY f.k,f.c,f.g) ";
  const std::string tail =
      "SELECT a.k,a.v,b.v,c.v FROM a LEFT JOIN b ON a.k=b.k AND a.c=b.c "
      "LEFT JOIN c ON c.c=a.c AND c.k=a.k ";
  const std::string sql =
      prefix + tail +
      "WHERE COALESCE(b.v,0)>0 OR COALESCE(c.v,0)>0 ORDER BY a.k,b.v";
  const std::vector<Row> expected{
      Row({Value(7), Value(3), Value(-3), Value(5)}),
      Row({Value(7), Value(3), Value(4), Value(5)}),
      Row({Value(17), Value(3), Value(), Value(2)})};
  const auto counter = [](const std::string& explain, std::string_view name) {
    const auto at = explain.find(name);
    if (at == std::string::npos) {
      return size_t{0};
    }
    return static_cast<size_t>(std::stoull(explain.substr(at + name.size())));
  };
  for (size_t workers : {1U, 4U}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql, /*force_relational=*/true), expected);
    const auto explain = ExplainAnalyze(*rs_, sql, /*force_relational=*/true);
    EXPECT_GE(counter(explain, "transfer_scan_tested="), 6144U) << explain;
    EXPECT_GT(counter(explain, "transfer_scan_rejected="), 6000U) << explain;
    EXPECT_EQ(counter(explain, "transfer_seed_rows="), 6144U) << explain;
  }
  // Without a null-rejecting WHERE, unmatched LEFT rows must be kept.
  EXPECT_EQ(
      RelationalRun(*rs_, prefix + tail, /*force_relational=*/true).size(),
      2049U);
  EXPECT_EQ(
      counter(ExplainAnalyze(*rs_, prefix + tail, /*force_relational=*/true),
              "transfer_scan_tested="),
      0U);
  // The key domain is query-local; subsequent writes cannot reuse old bits.
  RelationalRun(*rs_, "INSERT INTO TransferC VALUES(19,5,1,9)");
  EXPECT_EQ(RelationalRun(*rs_, sql, /*force_relational=*/true).size(), 4U);
  RelationalRun(*rs_, "DELETE FROM TransferB");
  RelationalRun(*rs_, "DELETE FROM TransferC");
  EXPECT_TRUE(RelationalRun(*rs_, sql, /*force_relational=*/true).empty());
}

TEST_F(RelationalRegressionTest, PredicateTransferKeepsRejectedGroupErrors) {
  RelationalRun(*rs_, "CREATE TABLE TransferOverflow(k INT64,v INT64)");
  RelationalRun(
      *rs_,
      "INSERT INTO TransferOverflow VALUES(1,1),(9,9223372036854775807),(9,1)");
  const std::string sql =
      "WITH a AS (SELECT f.k,SUM(f.v) AS v FROM TransferOverflow f JOIN "
      "KeyTable d ON f.k=d.k GROUP BY f.k),"
      "b AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN SmallKeys d ON "
      "f.k=d.k GROUP BY f.k) "
      "SELECT a.k,a.v FROM a LEFT JOIN b ON a.k=b.k WHERE COALESCE(b.v,0)>0";
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(context, sql);
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       PredicateTransferPreservesErrorsInGroupExpressions) {
  // Only direct group keys/aggregate arguments can cross the transfer gate.
  const std::string sql =
      "WITH a AS (SELECT f.k,SUM(f.v/(f.k-9)) AS v FROM AggTable f JOIN "
      "KeyTable d ON f.k=d.k GROUP BY f.k),"
      "b AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN SmallKeys d ON "
      "f.k=d.k GROUP BY f.k) "
      "SELECT a.k,a.v FROM a LEFT JOIN b ON a.k=b.k WHERE COALESCE(b.v,0)>0";
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(context, sql);
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, PredicateTransferKeepsFloatingOverflow) {
  RelationalRun(*rs_, "CREATE TABLE TransferFloat(k INT64,v DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO TransferFloat VALUES(1,1.5),(9,1e308),(9,1e308)");
  const std::string sql =
      "WITH a AS (SELECT f.k,SUM(f.v) AS v FROM TransferFloat f JOIN KeyTable "
      "d ON f.k=d.k GROUP BY f.k),"
      "b AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN SmallKeys d ON "
      "f.k=d.k GROUP BY f.k) "
      "SELECT a.k,a.v FROM a LEFT JOIN b ON a.k=b.k WHERE COALESCE(b.v,0)>0";
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(context, sql);
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, PredicateTransferKeepsCompleteFloatingGroups) {
  RelationalRun(*rs_,
                "CREATE TABLE TransferFloatSafe(k INT64,g INT64,v DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO TransferFloatSafe "
                "VALUES(1,1,1.5),(1,1,-0.5),(1,2,3.25),(9,1,7.0),(NULL,1,2.0)");
  const std::string sql =
      "WITH a AS (SELECT f.k,f.g,SUM(f.v) AS v FROM TransferFloatSafe f JOIN "
      "KeyTable d ON f.k=d.k GROUP BY f.k,f.g),"
      "b AS (SELECT f.k,SUM(f.v) AS v FROM AggTable f JOIN SmallKeys d ON "
      "f.k=d.k GROUP BY f.k) "
      "SELECT a.k,a.g,a.v FROM a LEFT JOIN b ON a.k=b.k WHERE "
      "COALESCE(b.v,0)>0 ORDER BY a.g";
  EXPECT_EQ(RelationalRun(*rs_, sql, /*force_relational=*/true),
            std::vector<Row>({Row({Value(1), Value(1), Value(1.0)}),
                              Row({Value(1), Value(2), Value(3.25)})}));
  const auto explain = ExplainAnalyze(*rs_, sql, /*force_relational=*/true);
  EXPECT_EQ(explain.find("transfer_scan_tested=0,"), std::string::npos)
      << explain;
}

TEST_F(RelationalRegressionTest, PredicateTransferDoesNotHideBuildOverflow) {
  RelationalRun(*rs_, "CREATE TABLE TransferLeftOnly(k INT64,v INT64)");
  RelationalRun(*rs_, "INSERT INTO TransferLeftOnly VALUES(1,1)");
  RelationalRun(*rs_, "CREATE TABLE TransferRightOverflow(k INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO TransferRightOverflow "
                "VALUES(1,1),(9,9223372036854775807),(9,1)");
  const std::string sql =
      "WITH a AS (SELECT f.k,SUM(f.v) AS v FROM TransferLeftOnly f JOIN "
      "KeyTable d ON f.k=d.k GROUP BY f.k),"
      "b AS (SELECT f.k,SUM(f.v) AS v FROM TransferRightOverflow f JOIN "
      "KeyTable d ON f.k=d.k GROUP BY f.k) "
      "SELECT a.k,a.v FROM a LEFT JOIN b ON a.k=b.k WHERE COALESCE(b.v,0)>0";
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(context, sql);
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ColumnarHashJoinElidesVarCharAndBiasedIntResidualsSafely) {
  RelationalRun(
      *rs_,
      "CREATE TABLE WinJoin5(k1 VARCHAR,k2 VARCHAR,k3 VARCHAR,k4 VARCHAR,"
      "rn INT64,v DOUBLE)");
  RelationalRun(
      *rs_,
      "INSERT INTO WinJoin5 VALUES "
      "('cat1','b1','s1','c1',1,10.0),('cat1','b1','s1','c1',2,20.0),"
      "('cat1','b1','s1','c1',3,30.0),('cat1','b1','s1','c1',NULL,40.0),"
      "(NULL,'b1','s1','c1',2,50.0),('cat2','b2','s2','c2',1,60.0)");

  const std::string sql =
      "SELECT a.k1,a.rn,b.v AS prev_v,c.v AS next_v "
      "FROM WinJoin5 a,WinJoin5 b,WinJoin5 c "
      "WHERE a.k1=b.k1 AND a.k1=c.k1 AND a.k2=b.k2 AND a.k2=c.k2 "
      "AND a.k3=b.k3 AND a.k3=c.k3 AND a.k4=b.k4 AND a.k4=c.k4 "
      "AND a.rn=b.rn+1 AND a.rn=c.rn-1";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value("cat1"), Value(2), Value(10.0), Value(30.0)})}));

  // Biased equality overflow (INT64_MAX + 1) must still raise rather than
  // silently wrapping in AddJoinKeyBias.
  RelationalRun(*rs_, "CREATE TABLE BiasEdgeL(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE BiasEdgeR(k INT64)");
  RelationalRun(*rs_, "INSERT INTO BiasEdgeL VALUES(-9223372036854775808),(5)");
  RelationalRun(*rs_, "INSERT INTO BiasEdgeR VALUES(9223372036854775807),(4)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto res = engine.Execute(context,
                            "SELECT l.k FROM BiasEdgeL l JOIN BiasEdgeR r "
                            "ON l.k = r.k + 1");
  if (res.HasValue()) {
    res.Value().Drain();
    EXPECT_NE(res.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(res.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       MultiKeyColumnarWindowsMatchGeneralSemanticsOnMixedSpecsAndNulls) {
  RelationalRun(
      *rs_,
      "CREATE TABLE MultiWinT(s1 VARCHAR,s2 VARCHAR,y INT64,m INT64,v DOUBLE)");
  {
    std::string insert_sql = "INSERT INTO MultiWinT VALUES ";
    insert_sql.reserve(2400);
    for (int i = 0; i < 2400; ++i) {
      if (i > 0) {
        insert_sql += ",";
      }
      const int g = i % 120;
      const int y = (i / 120) % 4;
      const int m = (i / 480);
      insert_sql += "('c" + std::to_string(g % 7) + "','b" + std::to_string(g) +
                    "'," + std::to_string(2000 + y) + "," +
                    std::to_string(m + 1) + "," +
                    std::to_string(static_cast<double>(i % 13) + 0.5) + ")";
    }
    insert_sql +=
        ",('c_sp','b_sp',NULL,1,1.0),('c_sp','b_sp',2001,NULL,2.0),"
        "(NULL,'b_null',2000,1,-0.0),(NULL,'b_null',2000,1,0.0)";
    RelationalRun(*rs_, insert_sql);
  }

  // Filter down to a few interesting groups after computing windows over all
  // >2400 rows (exercising the 64-shard parallel columnar path +
  // heterogeneity).
  const std::string sql =
      "SELECT s1,s2,y,m,avg_y,rnk,drnk,rownum,cnt_all "
      "FROM ("
      "  SELECT s1,s2,y,m,"
      "    AVG(v) OVER (PARTITION BY s1,s2,y) AS avg_y,"
      "    RANK() OVER (PARTITION BY s1,s2 ORDER BY y DESC NULLS FIRST, m ASC "
      "NULLS LAST) AS rnk,"
      "    DENSE_RANK() OVER (PARTITION BY s1,s2 ORDER BY y DESC NULLS FIRST, "
      "m ASC NULLS LAST) AS drnk,"
      "    ROW_NUMBER() OVER (PARTITION BY s1,s2 ORDER BY y DESC NULLS FIRST, "
      "m ASC NULLS LAST) AS rownum,"
      "    COUNT(*) OVER (PARTITION BY s1,s2) AS cnt_all"
      "  FROM MultiWinT"
      ") w "
      "WHERE s2 IN ('b_sp','b_null') "
      "ORDER BY s2,rnk,rownum";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 4U);
  // b_null group: two NULL s1 rows with (-0.0, 0.0) tie at rank 1, avg 0.0.
  EXPECT_EQ(rows[0][0], Value());
  EXPECT_EQ(rows[0][1], Value("b_null"));
  EXPECT_DOUBLE_EQ(rows[0][4].value.double_value, 0.0);
  EXPECT_EQ(rows[0][5], Value(int64_t{1}));
  EXPECT_EQ(rows[0][6], Value(int64_t{1}));
  EXPECT_EQ(rows[0][7], Value(int64_t{1}));
  EXPECT_EQ(rows[0][8], Value(int64_t{2}));
  EXPECT_EQ(rows[1][5], Value(int64_t{1}));
  EXPECT_EQ(rows[1][7], Value(int64_t{2}));

  // b_sp group: NULL y sorts first because of DESC NULLS FIRST.
  EXPECT_EQ(rows[2][1], Value("b_sp"));
  EXPECT_EQ(rows[2][2], Value());
  EXPECT_DOUBLE_EQ(rows[2][4].value.double_value, 1.0);
  EXPECT_EQ(rows[2][5], Value(int64_t{1}));
  EXPECT_EQ(rows[3][1], Value("b_sp"));
  EXPECT_EQ(rows[3][2], Value(int64_t{2001}));
  EXPECT_DOUBLE_EQ(rows[3][4].value.double_value, 2.0);
  EXPECT_EQ(rows[3][5], Value(int64_t{2}));
}

TEST_F(RelationalRegressionTest,
       MultiKeyColumnarWindowsFallbackOnInt64SumOverflow) {
  RelationalRun(*rs_, "CREATE TABLE WinOvT(s1 VARCHAR,s2 VARCHAR,v INT64)");
  RelationalRun(
      *rs_,
      "INSERT INTO WinOvT VALUES ('a','b',9223372036854775807),('a','b',1)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto res = engine.Execute(
      context, "SELECT SUM(v) OVER (PARTITION BY s1,s2) FROM WinOvT");
  if (res.HasValue()) {
    res.Value().Drain();
    EXPECT_NE(res.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(res.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       FusedGroupedJoinPackedDoubleCellsPreserveNullsAndInfinities) {
  RelationalRun(*rs_, "CREATE TABLE FgjDim(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE FgjFact(k INT64,v DOUBLE)");
  {
    std::string dim_sql = "INSERT INTO FgjDim VALUES ";
    dim_sql.reserve(8500);
    for (int i = 0; i < 8500; ++i) {
      if (i > 0) {
        dim_sql += ",";
      }
      dim_sql += "(" + std::to_string(i) + ")";
    }
    RelationalRun(*rs_, dim_sql);
    RelationalRun(*rs_,
                  "INSERT INTO FgjFact VALUES "
                  "(10,NULL),(10,NULL),"
                  "(30,5.0),(30,NULL),(30,7.0),"
                  "(40,-0.0),(40,0.0)");
  }

  const auto rows = RelationalRun(
      *rs_,
      "SELECT d.k,AVG(f.v) FROM FgjFact f JOIN FgjDim d ON f.k=d.k "
      "GROUP BY d.k ORDER BY d.k");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{10}));
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][0], Value(int64_t{30}));
  EXPECT_DOUBLE_EQ(rows[1][1].value.double_value, 6.0);
  EXPECT_EQ(rows[2][0], Value(int64_t{40}));
  EXPECT_DOUBLE_EQ(rows[2][1].value.double_value, 0.0);

  const auto sum_rows = RelationalRun(
      *rs_,
      "SELECT d.k,SUM(f.v) FROM FgjFact f JOIN FgjDim d ON f.k=d.k "
      "GROUP BY d.k ORDER BY d.k");
  ASSERT_EQ(sum_rows.size(), 3U);
  EXPECT_TRUE(sum_rows[0][1].IsNull());
  EXPECT_DOUBLE_EQ(sum_rows[1][1].value.double_value, 12.0);
  EXPECT_DOUBLE_EQ(sum_rows[2][1].value.double_value, 0.0);

  RelationalRun(*rs_, "CREATE TABLE FgjInfFact(k INT64,v DOUBLE)");
  RelationalRun(*rs_, "INSERT INTO FgjInfFact VALUES (20,1e308),(20,1e308)");
  const auto avg_overflow = RelationalRun(
      *rs_,
      "SELECT d.k,AVG(f.v) FROM FgjInfFact f JOIN FgjDim d ON f.k=d.k "
      "GROUP BY d.k");
  ASSERT_EQ(avg_overflow.size(), 1U);
  EXPECT_EQ(avg_overflow[0],
            (Row({Value(20), Value(std::numeric_limits<double>::infinity())})));

  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto sum_overflow = engine.Execute(
      context,
      "SELECT d.k,SUM(f.v) FROM FgjInfFact f JOIN FgjDim d ON f.k=d.k "
      "GROUP BY d.k");
  if (sum_overflow.HasValue()) {
    sum_overflow.Value().Drain();
    EXPECT_EQ(sum_overflow.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(sum_overflow.GetStatus(), Status::kIsInfinity);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, ParallelScanPushesNullCoalesce) {
  RelationalRun(*rs_, "CREATE TABLE CoalesceScan(a STRING, b STRING)");
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("CoalesceScan"));
    for (int i = 0; i < 9000; ++i) {
      ASSERT_TRUE(
          table
              ->Insert(ctx.txn_, Row({i % 2 ? Value() : Value("first"),
                                      i % 3 ? Value("second") : Value()}))
              .HasValue());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable("CoalesceScan"));
  relational_detail::PushedScanProjection projection{
      .expressions = {FunctionCallExp(
          "coalesce", {ColumnValueExp("a"), ColumnValueExp("b")})},
      .output_schema = Schema("out", {Column("v", ValueType::kVarChar)})};
  relational_detail::Relation result;
  Status error = Status::kSuccess;
  bool applied = false;
  ASSERT_TRUE(relational_detail::TryParallelTableScan(
      ctx, *table, nullptr, nullptr, ValueType::kNull, std::nullopt, false,
      nullptr, table->GetSchema(), nullptr, {}, &result, &error, nullptr, true,
      &projection, &applied));
  ASSERT_SUCCESS(error);
  ASSERT_TRUE(applied);
  // Column narrowing: the pushed projection must replace the scan's a/b
  // payload with the single projected column, not merely append to it.
  ASSERT_EQ(result.schema.ColumnCount(), 1U);
  EXPECT_EQ(result.schema, projection.output_schema);
  EXPECT_EQ(result.schema.Offset(ColumnName("a")), -1);
  EXPECT_EQ(result.schema.Offset(ColumnName("b")), -1);
  size_t first = 0, second = 0, nulls = 0;
  ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
    EXPECT_EQ(row.values_.size(), 1U);
    if (row[0].IsNull())
      ++nulls;
    else if (row[0] == Value("first"))
      ++first;
    else if (row[0] == Value("second"))
      ++second;
    else
      ADD_FAILURE() << "unexpected value";
  }));
  EXPECT_EQ(first, 4500U);
  EXPECT_EQ(second, 3000U);
  EXPECT_EQ(nulls, 1500U);
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.Abort());
}

TEST_F(RelationalRegressionTest, SelfSumHavingKeepsIntegerPrecision) {
  RelationalRun(*rs_, "CREATE TABLE HavingPrecision(k INT64, v INT64)");
  RelationalRun(
      *rs_, "INSERT INTO HavingPrecision VALUES (1,9007199254740993),(2,-1)");
  const std::string sql =
      "SELECT a.k, SUM(f.v) FROM HavingPrecision f "
      "JOIN KeyTable a ON f.k = a.k "
      "GROUP BY a.k HAVING SUM(f.v) > "
      "(SELECT SUM(f.v) * 1 FROM HavingPrecision f "
      "JOIN KeyTable a ON f.k = a.k)";
  const auto rows = RelationalRun(*rs_, sql, /*force_relational=*/true);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(int64_t{9007199254740993LL})}));
  // The single-pass Fubini fusion must actually fire: the generic grouped
  // route returns the same answer, so without a counter check a regression
  // that only refuses the fusion would go unnoticed.  Parse the counters
  // instead of matching "=1" so additional fusions do not make this brittle.
  const auto profile = ExplainAnalyze(*rs_, sql, /*force_relational=*/true);
  const auto fusion_counter = [&](std::string_view field) -> size_t {
    const size_t at = profile.find(field);
    if (at == std::string::npos) {
      return 0;
    }
    size_t value = 0;
    for (size_t i = at + field.size(); i < profile.size(); ++i) {
      if (profile[i] < '0' || profile[i] > '9') {
        break;
      }
      value = (value * 10) + static_cast<size_t>(profile[i] - '0');
    }
    return value;
  };
  EXPECT_NE(profile.find("grouped_join_fusions="), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("streamed_grouped_joins="), std::string::npos)
      << profile;
  EXPECT_GE(fusion_counter("grouped_join_fusions=") +
                fusion_counter("streamed_grouped_joins="),
            1U)
      << profile;
}

TEST_F(RelationalRegressionTest, SelfSumHavingRespectsSubqueryRowElimination) {
  RelationalRun(*rs_, "CREATE TABLE HavingSample(k INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO HavingSample VALUES (1, 10), (2, 20)");
  const std::string query =
      "SELECT a.k, SUM(f.v) FROM HavingSample f JOIN KeyTable a ON f.k = a.k "
      "GROUP BY a.k HAVING SUM(f.v) > ";

  EXPECT_EQ(RelationalRun(*rs_,
                          query + "(SELECT SUM(f.v) * 0 FROM HavingSample f "
                                  "JOIN KeyTable a ON f.k = a.k)",
                          true)
                .size(),
            2U);
  EXPECT_TRUE(RelationalRun(*rs_,
                            query + "(SELECT SUM(f.v) * 0 FROM HavingSample f "
                                    "JOIN KeyTable a ON f.k = a.k "
                                    "QUALIFY ROW_NUMBER() OVER () > 1)",
                            true)
                  .empty());
  EXPECT_TRUE(RelationalRun(*rs_,
                            query + "(SELECT SUM(f.v) * 0 FROM HavingSample f "
                                    "TABLESAMPLE SYSTEM(0 PERCENT) "
                                    "JOIN KeyTable a ON f.k = a.k)",
                            true)
                  .empty());
}

TEST_F(RelationalRegressionTest,
       StreamingProbePairBitsDenseRemapAndBloomFallback) {
  RelationalRun(*rs_, "CREATE TABLE Sb1(a INT64, b INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE Sf1(k1 INT64, k2 INT64, p INT64)");
  struct BRow {
    std::optional<int64_t> a;
    std::optional<int64_t> b;
    int64_t v;
  };
  std::vector<BRow> b1_rows;
  {
    std::string sql = "INSERT INTO Sb1 VALUES ";
    for (int64_t i = 0; i <= 210; ++i) {
      if (i > 0) {
        sql += ",";
      }
      const int64_t a = i * 500;
      const int64_t b = (i * 2) % 401;
      const int64_t v = i + 1;
      b1_rows.push_back({a, b, v});
      sql += "(" + std::to_string(a) + "," + std::to_string(b) + "," +
             std::to_string(v) + ")";
    }
    b1_rows.push_back({int64_t{0}, int64_t{0}, int64_t{1000}});
    b1_rows.push_back({std::nullopt, int64_t{0}, int64_t{500}});
    b1_rows.push_back({int64_t{0}, std::nullopt, int64_t{500}});
    sql += ",(0,0,1000),(NULL,0,500),(0,NULL,500)";
    RelationalRun(*rs_, sql);
  }
  std::map<std::pair<int64_t, int64_t>, std::vector<int64_t>> b1_map;
  for (const auto& br : b1_rows) {
    if (br.a.has_value() && br.b.has_value()) {
      b1_map[{*br.a, *br.b}].push_back(br.v);
    }
  }
  int64_t exp_cnt1 = 0;
  int64_t exp_sum1 = 0;
  {
    std::string sql = "INSERT INTO Sf1 VALUES ";
    for (int64_t j = 0; j < 4000; ++j) {
      if (j > 0) {
        sql += ",";
      }
      if (j % 17 == 0) {
        sql += "(NULL,0," + std::to_string(j) + ")";
      } else if (j % 19 == 0) {
        sql += "(500,NULL," + std::to_string(j) + ")";
      } else if (j % 13 == 0) {
        sql += "(100,0," + std::to_string(j) + ")";
      } else {
        const int64_t k1 = (j % 211) * 500;
        const int64_t k2 = (j % 211 == 0 && j % 3 == 0) ? 999 : ((j * 2) % 401);
        const int64_t p = j;
        sql += "(" + std::to_string(k1) + "," + std::to_string(k2) + "," +
               std::to_string(p) + ")";
        if (auto it = b1_map.find({k1, k2}); it != b1_map.end()) {
          for (int64_t v : it->second) {
            ++exp_cnt1;
            exp_sum1 += v + p;
          }
        }
      }
    }
    RelationalRun(*rs_, sql);
  }
  for (bool force_rel : {false, true}) {
    const auto out =
        RelationalRun(*rs_,
                      "SELECT COUNT(*), COALESCE(SUM(b.v + f.p), 0) "
                      "FROM Sb1 b JOIN Sf1 f ON b.a = f.k1 AND b.b = f.k2",
                      force_rel);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0][0], Value(exp_cnt1));
    EXPECT_EQ(out[0][1], Value(exp_sum1));
  }

  // Fallback packed_bloom case: d_outer = 31, d_inner = 3,500,001 -> product >
  // 64M.
  RelationalRun(*rs_, "CREATE TABLE Sb2(a INT64, b INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE Sf2(k1 INT64, k2 INT64, p INT64)");
  std::map<std::pair<int64_t, int64_t>, std::vector<int64_t>> b2_map;
  {
    std::string sql = "INSERT INTO Sb2 VALUES ";
    for (int64_t i = 0; i <= 30; ++i) {
      if (i > 0) {
        sql += ",";
      }
      const int64_t b = (i % 2 == 0) ? 0 : 3500000;
      const int64_t v = i + 1;
      b2_map[{i, b}].push_back(v);
      sql += "(" + std::to_string(i) + "," + std::to_string(b) + "," +
             std::to_string(v) + ")";
    }
    b2_map[{0, 0}].push_back(100);
    sql += ",(0,0,100),(NULL,0,999),(1,NULL,999)";
    RelationalRun(*rs_, sql);
  }
  int64_t exp_cnt2 = 0;
  int64_t exp_sum2 = 0;
  {
    std::string sql = "INSERT INTO Sf2 VALUES ";
    for (int64_t j = 0; j < 400; ++j) {
      if (j > 0) {
        sql += ",";
      }
      if (j % 11 == 0) {
        sql += "(NULL,0," + std::to_string(j) + ")";
      } else {
        const int64_t k1 = j % 35;
        const int64_t k2 = (j % 3 == 0) ? 0 : ((j % 3 == 1) ? 3500000 : 12345);
        sql += "(" + std::to_string(k1) + "," + std::to_string(k2) + "," +
               std::to_string(j) + ")";
        if (auto it = b2_map.find({k1, k2}); it != b2_map.end()) {
          for (int64_t v : it->second) {
            ++exp_cnt2;
            exp_sum2 += v + j;
          }
        }
      }
    }
    RelationalRun(*rs_, sql);
  }
  for (bool force_rel : {false, true}) {
    const auto out =
        RelationalRun(*rs_,
                      "SELECT COUNT(*), COALESCE(SUM(b.v + f.p), 0) "
                      "FROM Sb2 b JOIN Sf2 f ON b.a = f.k1 AND b.b = f.k2",
                      force_rel);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0][0], Value(exp_cnt2));
    EXPECT_EQ(out[0][1], Value(exp_sum2));
  }
}

}  // namespace
}  // namespace tinylamb
