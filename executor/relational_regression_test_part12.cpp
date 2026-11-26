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

#include "common/test_util.hpp"
#include "executor/relational_regression_test.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, StringSubqueryMembershipKeepsNullAndNotIn) {
  RelationalRun(*rs_, "CREATE TABLE StringMembers (s STRING)");
  RelationalRun(*rs_,
                "INSERT INTO StringMembers VALUES ('a'),('a'),('b'),(NULL)");
  RelationalRun(*rs_, "CREATE TABLE StringProbes (s STRING)");
  RelationalRun(*rs_,
                "INSERT INTO StringProbes VALUES ('a'),('b'),('c'),(NULL)");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT s FROM StringProbes WHERE s IN (SELECT s "
                          "FROM StringMembers) ORDER BY s"),
            std::vector<Row>({Row({Value("a")}), Row({Value("b")})}));
  EXPECT_TRUE(RelationalRun(*rs_,
                            "SELECT s FROM StringProbes WHERE s NOT IN (SELECT "
                            "s FROM StringMembers)")
                  .empty());
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT s FROM StringProbes WHERE s NOT IN "
                          "(SELECT s FROM StringMembers WHERE s IS NOT NULL)"),
            std::vector<Row>({Row({Value("c")})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM StringProbes WHERE s NOT IN "
                          "(SELECT s FROM StringMembers WHERE FALSE)"),
            std::vector<Row>({Row({Value(4)})}));
  // Spaced/structured members keep the canonical evaluator fallback.
  RelationalRun(*rs_, "INSERT INTO StringMembers VALUES ('two words')");
  RelationalRun(*rs_, "INSERT INTO StringProbes VALUES ('two words')");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM StringProbes WHERE s IN "
                          "(SELECT s FROM StringMembers)"),
            std::vector<Row>({Row({Value(3)})}));
}

TEST_F(RelationalRegressionTest, CrossedPrefixDisjunctionKeepsBothFilters) {
  RelationalRun(*rs_, "CREATE TABLE PrefixCross (k INT64,s STRING,t STRING)");
  RelationalRun(*rs_,
                "INSERT INTO PrefixCross VALUES "
                "(1,'ABx','CDx'),(2,'bad','CDx'),(3,'ABx','bad'),(4,NULL,'CDx')"
                ",(5,'ABx',NULL)");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k FROM PrefixCross WHERE "
                          "(SUBSTR(s,1,2) IN ('AB','XY') OR k=99) AND "
                          "(SUBSTR(t,1,2) IN ('CD','ZZ') OR k=98) ORDER BY k"),
            std::vector<Row>({Row({Value(1)})}));
}

TEST_F(RelationalRegressionTest, StreamHavingFoldsUncorrelatedScalar) {
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT k,SUM(v) FROM AggTable GROUP BY k "
                "HAVING SUM(v) > (SELECT COUNT(*) FROM SmallKeys) ORDER BY k"),
            RelationalRun(*rs_,
                          "SELECT k,SUM(v) FROM AggTable GROUP BY k HAVING "
                          "SUM(v)>4 ORDER BY k"));
}

TEST_F(RelationalRegressionTest, DerivedCrossJoinStarPreservesFromOrder) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT * FROM (SELECT 1 AS a) x, (SELECT 2 AS b) y, "
                    "(SELECT 3 AS c) z");
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0], Row({Value(1), Value(2), Value(3)}));
  const auto mixed =
      RelationalRun(*rs_,
                    "SELECT z.*, * EXCEPT(b), y.* FROM (SELECT 1 AS a) x, "
                    "(SELECT 2 AS b) y, (SELECT 3 AS c) z");
  ASSERT_EQ(mixed.size(), 1);
  EXPECT_EQ(mixed[0], Row({Value(3), Value(1), Value(3), Value(2)}));
}

TEST_F(RelationalRegressionTest, CorrelatedCteIndexTracksBindingAndNullKeys) {
  auto context = rs_->BeginContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  SelectSource source;
  source.table = "materialized";
  source.alias = "m";
  auto avg = std::make_shared<AggregateExpression>(
      AggregationType::kAvg, ColumnValueExp(ColumnName("m", "v")));
  auto equal = BinaryExpressionExp(ColumnValueExp(ColumnName("m", "k")),
                                   BinaryOperation::kEquals,
                                   ColumnValueExp(ColumnName("o", "k")));
  SelectStatement statement({NamedExpression("average", avg)}, {}, equal);
  statement.SetSources({source});
  const Schema outer_schema("o", {Column("k", ValueType::kInt64)});
  relational_detail::CteMap ctes;
  auto bind = [&](int value) {
    auto relation = std::make_shared<relational_detail::Relation>();
    relation->schema = Schema("materialized", {Column("k", ValueType::kInt64),
                                               Column("v", ValueType::kInt64)});
    relation->AddRow(Row({Value(1), Value(value)}));
    relation->AddRow(Row({Value(1), Value(value + 2)}));
    relation->AddRow(Row({Value(1), Value()}));
    relation->AddRow(Row({Value(), Value(999)}));
    ctes["materialized"] = relation;
  };
  auto probe = [&](Value key, Value expected) {
    const Row row({std::move(key)});
    const relational_detail::Scope scope{.row = &row, .schema = &outer_schema};
    auto result = relational_detail::ExecuteCorrelatedSingleSource(
        context, statement, scope, ctes);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->rows.size(), 1U);
    EXPECT_EQ(result->rows[0], Row({std::move(expected)}));
    ASSERT_TRUE(runtime.correlated_indexes.contains(&statement));
    EXPECT_EQ(runtime.correlated_indexes.at(&statement)->source_cte,
              ctes.at("materialized"));
  };
  bind(10);
  probe(Value(1), Value(11.0));
  probe(Value(1), Value(11.0));
  probe(Value(2), Value());
  probe(Value(), Value());
  // Same AST and outer key, new WITH/recursive binding: rebuild the index.
  bind(40);
  probe(Value(1), Value(41.0));
  context.set_execution_runtime(nullptr);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, StringCorrelationDomainTracksGeneration) {
  auto context = rs_->BeginContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  SelectSource source;
  source.table = "materialized";
  source.alias = "m";
  SelectStatement statement;
  statement.SetSources({source});
  statement.SetSelectList({NamedExpression(
      "n", std::make_shared<AggregateExpression>(
               AggregationType::kSum, ColumnValueExp(ColumnName("m", "v"))))});
  statement.SetWhereClause(BinaryExpressionExp(
      ColumnValueExp(ColumnName("m", "k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("o", "k"))));
  auto input = std::make_shared<relational_detail::Relation>();
  input->schema = Schema("materialized", {Column("k", ValueType::kVarChar),
                                          Column("v", ValueType::kInt64)});
  input->AddRow(Row({Value("alpha group"), Value(3)}));
  input->AddRow(Row({Value("beta group"), Value(7)}));
  input->AddRow(Row({Value("beta group"), Value(9)}));
  relational_detail::CteMap ctes{{"materialized", input}};
  relational_detail::Relation domain;
  domain.schema = Schema("o", {Column("k", ValueType::kVarChar)});
  auto probe = [&](const char* key, int expected) {
    Row row({Value(key)});
    const relational_detail::Scope scope{.row = &row, .schema = &domain.schema};
    auto result = relational_detail::ExecuteCorrelatedSingleSource(
        context, statement, scope, ctes);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->rows.size(), 1U);
    EXPECT_EQ(result->rows[0], Row({Value(expected)}));
  };
  domain.AddRow(Row({Value("alpha group")}));
  runtime.correlated_outer_relation = &domain;
  runtime.correlated_outer_serial = 1;
  probe("alpha group", 3);
  // Same domain object and AST, but a new generation must rebuild the index.
  domain.rows[0] = Row({Value("beta group")});
  runtime.correlated_outer_serial = 2;
  probe("beta group", 16);
  runtime.correlated_outer_relation = nullptr;
  probe("alpha group", 3);
  context.set_execution_runtime(nullptr);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ParallelGroupedFinishMergesMinMaxAcrossWorkers) {
  constexpr size_t kRowCount = 262144;
  TransactionContext context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema = Schema("parallel_group", {Column("g", ValueType::kInt64),
                                           Column("v", ValueType::kInt64)});
  input.rows.reserve(kRowCount);
  for (size_t i = 0; i < kRowCount; ++i) {
    int64_t value = 1;
    if (i == 0) {
      value = -100;
    } else if (i + 1 == kRowCount) {
      value = 100;
    }
    input.AddRow(Row({Value(int64_t{1}), Value(value)}));
  }

  const auto make_aggregate = [](AggregationType type, bool distinct) {
    return std::make_shared<AggregateExpression>(type, ColumnValueExp("v"),
                                                 distinct);
  };
  SelectStatement statement(
      {NamedExpression("g", ColumnValueExp("g")),
       NamedExpression("min_v", make_aggregate(AggregationType::kMin, false)),
       NamedExpression("max_v", make_aggregate(AggregationType::kMax, false)),
       NamedExpression("distinct_v",
                       make_aggregate(AggregationType::kCount, true))},
      {}, nullptr);
  statement.SetGroupBy({ColumnValueExp("g")});

  StatusOr<relational_detail::Relation> finished =
      relational_detail::FinishQuery(context, statement, std::move(input),
                                     nullptr, {}, false);
  ASSERT_TRUE(finished.HasValue()) << finished.GetStatus();
  relational_detail::Relation output = finished.MoveValue();
  ASSERT_EQ(output.rows.size(), 1U);
  EXPECT_EQ(output.rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(output.rows[0][1], Value(int64_t{-100}));
  EXPECT_EQ(output.rows[0][2], Value(int64_t{100}));
  EXPECT_EQ(output.rows[0][3], Value(int64_t{3}));
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ParallelGroupedFinishRejectsUnmergeableStatisticalStates) {
  constexpr size_t kRowCount = 32768;
  constexpr int64_t kNonNullPerGroup = 12288;
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{8}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const bool composite : {false, true}) {
      SCOPED_TRACE(::testing::Message()
                   << workers << " workers, composite=" << composite);
      auto context = rs_->BeginContext();
      relational_detail::Relation input;
      input.schema = Schema("stat_groups", {Column("g", ValueType::kInt64),
                                            Column("h", ValueType::kVarChar),
                                            Column("v", ValueType::kInt64)});
      for (size_t i = 0; i < kRowCount; ++i) {
        const int64_t value = static_cast<int64_t>((i / 2) % 4) * 10;
        input.AddRow(Row({Value(static_cast<int64_t>(i % 2)), Value("all"),
                          value == 0 ? Value() : Value(value)}));
      }
      const auto aggregate = [](AggregationType type) {
        return AggregateExpressionExp(type, ColumnValueExp("v"));
      };
      SelectStatement statement(
          {NamedExpression("g", ColumnValueExp("g")),
           NamedExpression("n", aggregate(AggregationType::kCount)),
           NamedExpression("mean", aggregate(AggregationType::kAvg)),
           NamedExpression("vp", aggregate(AggregationType::kVarPop)),
           NamedExpression("vs", aggregate(AggregationType::kVarSamp)),
           NamedExpression("sp", aggregate(AggregationType::kStddevPop)),
           NamedExpression("ss", aggregate(AggregationType::kStddevSamp))},
          {}, nullptr);
      statement.SetGroupBy(composite
                               ? std::vector<Expression>{ColumnValueExp("g"),
                                                         ColumnValueExp("h")}
                               : std::vector<Expression>{ColumnValueExp("g")});
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, output,
          relational_detail::FinishQuery(context, statement, std::move(input),
                                         nullptr, {}, false));
      ASSERT_EQ(output.rows.size(), 2U);
      const double variance = 200.0 / 3.0;
      const double sample =
          variance * kNonNullPerGroup / (kNonNullPerGroup - 1);
      for (const Row& row : output.rows) {
        EXPECT_EQ(row[1], Value(kNonNullPerGroup));
        EXPECT_DOUBLE_EQ(row[2].value.double_value, 20.0);
        EXPECT_NEAR(row[3].value.double_value, variance, 1e-10);
        EXPECT_NEAR(row[4].value.double_value, sample, 1e-10);
        EXPECT_NEAR(row[5].value.double_value, std::sqrt(variance), 1e-10);
        EXPECT_NEAR(row[6].value.double_value, std::sqrt(sample), 1e-10);
      }
      EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
    }
  }
}

TEST_F(RelationalRegressionTest, CompactStatisticsKeepSequentialMoments) {
  const std::vector<AggregationType> kinds{
      AggregationType::kCount,     AggregationType::kAvg,
      AggregationType::kVarPop,    AggregationType::kVarSamp,
      AggregationType::kStddevPop, AggregationType::kStddevSamp};
  std::vector<std::shared_ptr<AggregateExpression>> owned;
  owned.reserve(kinds.size());
  std::vector<const AggregateExpression*> aggregates;
  aggregates.reserve(kinds.size());
  std::vector<NamedExpression> items{NamedExpression("g", ColumnValueExp("g"))};
  items.reserve(kinds.size() + 2);
  for (size_t i = 0; i < kinds.size(); ++i) {
    owned.push_back(std::make_shared<AggregateExpression>(
        kinds[i], ColumnValueExp(i == 0 ? "*" : "v")));
    aggregates.push_back(owned.back().get());
    items.emplace_back("a" + std::to_string(i), owned.back());
  }
  items.emplace_back(
      "twice", BinaryExpressionExp(owned.back(), BinaryOperation::kMultiply,
                                   ConstantValueExp(Value(2.0))));
  SelectStatement statement(std::move(items), {}, nullptr);
  statement.SetGroupBy({ColumnValueExp("g")});
  statement.SetHaving(BinaryExpressionExp(owned.front(),
                                          BinaryOperation::kGreaterThan,
                                          ConstantValueExp(Value(0))));
  relational_detail::Relation input;
  input.schema = Schema("input", {Column("g", ValueType::kVarChar),
                                  Column("v", ValueType::kDouble)});
  std::array<std::vector<relational_detail::AggregateAccumulator>, 4> ground;
  for (auto& group : ground) {
    group.reserve(aggregates.size());
    for (const auto* aggregate : aggregates) {
      group.emplace_back(aggregate);
    }
  }
  constexpr size_t kRows = 32768;
  const std::array<Value, 5> cancellation{Value(1e16), Value(1.0), Value(-1e16),
                                          Value(0.25), Value()};
  input.rows.reserve(kRows);
  for (size_t i = 0; i < kRows; ++i) {
    const size_t group = i % ground.size();
    Value value;
    if (group == 0) {
      value = cancellation[(i / ground.size()) % cancellation.size()];
    } else if (group == 1) {
      value =
          i == 1 ? Value(std::numeric_limits<double>::infinity()) : Value(1.0);
    } else if (group == 2 && i == 2) {
      value = Value(2.0);
    }
    for (size_t a = 0; a < aggregates.size(); ++a) {
      ASSERT_SUCCESS(ground[group][a].Add(a == 0 ? Value(1) : value));
    }
    input.AddRow(
        Row({group == 3 ? Value() : Value(std::to_string(group)), value}));
  }
  std::array<std::vector<Value>, 4> expected;
  for (size_t g = 0; g < ground.size(); ++g) {
    for (auto& accumulator : ground[g]) {
      ASSIGN_OR_ASSERT_FAIL(Value, value, accumulator.TryFinish());
      expected[g].push_back(std::move(value));
    }
    ASSIGN_OR_ASSERT_FAIL(
        Value, twice,
        relational_detail::TryBinary(BinaryOperation::kMultiply,
                                     expected[g].back(), Value(2.0)));
    expected[g].push_back(std::move(twice));
  }
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginContext();
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(std::optional<relational_detail::Relation>, output,
                          relational_detail::TryCompactStatisticalAggregate(
                              context, statement, input, aggregates, {}));
    if (!output) {
      FAIL() << "expected compact moments";
    }
    ASSERT_EQ(output->rows.size(), ground.size());
    EXPECT_EQ(runtime.typed_parallel_aggregations, workers == 1 ? 0U : 1U);
    for (size_t g = 0; g < ground.size(); ++g) {
      const auto& row = output->rows[g];
      EXPECT_EQ(row[0], g == 3 ? Value() : Value(std::to_string(g)));
      ASSERT_EQ(row.Count(), expected[g].size() + 1);
      for (size_t a = 0; a < expected[g].size(); ++a) {
        EXPECT_TRUE(relational_detail::DistinctValueEqual{}(row[a + 1],
                                                            expected[g][a]));
      }
    }
    // A modifier must retain the ordinary aggregate's filtering semantics.
    owned.back()->SetWhereFilter(ConstantValueExp(Value(false)));
    ASSIGN_OR_ASSERT_FAIL(std::optional<relational_detail::Relation>, declined,
                          relational_detail::TryCompactStatisticalAggregate(
                              context, statement, input, aggregates, {}));
    EXPECT_FALSE(declined.has_value());
    owned.back()->SetWhereFilter(nullptr);
    context.set_execution_runtime(nullptr);
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, ParallelProbePadsNullAndNaNLeftKeys) {
  constexpr size_t kRowCount = 65536;
  TransactionContext context = rs_->BeginContext();
  relational_detail::Relation left;
  left.schema = Schema("parallel_left", {Column("lk", ValueType::kDouble)});
  for (size_t i = 0; i < kRowCount; ++i) {
    Value key(1.0);
    if (i == 0) {
      key = Value(std::numeric_limits<double>::quiet_NaN());
    } else if (i == 1) {
      key = Value();
    }
    left.AddRow(Row({std::move(key)}));
  }
  relational_detail::Relation right;
  right.schema = Schema("parallel_right", {Column("rk", ValueType::kDouble)});
  right.AddRow(Row({Value(1.0)}));

  SelectSource source;
  source.join_type = JoinType::kLeft;
  source.join_condition = BinaryExpressionExp(
      ColumnValueExp("lk"), BinaryOperation::kEquals, ColumnValueExp("rk"));
  StatusOr<relational_detail::Relation> joined = relational_detail::Join(
      context, std::move(left), std::move(right), source, nullptr, {});
  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  ASSERT_EQ(joined.Value().rows.size(), kRowCount);
  size_t padded = 0;
  for (const Row& row : joined.Value().rows) {
    if (row[1].IsNull()) {
      ++padded;
    }
  }
  EXPECT_EQ(padded, 2U);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ParallelFilteredCountStarUsesSerialAccumulation) {
  constexpr size_t kRowCount = 262144;
  TransactionContext context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema =
      Schema("parallel_filtered_count", {Column("v", ValueType::kInt64)});
  input.rows.reserve(kRowCount);
  for (size_t i = 0; i < kRowCount; ++i) {
    input.AddRow(Row({Value(static_cast<int64_t>(i + 1))}));
  }
  auto count = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                     ColumnValueExp("*"));
  count->SetWhereFilter(
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{kRowCount / 2}))));
  SelectStatement statement({NamedExpression("count", count)}, {}, nullptr);

  StatusOr<relational_detail::Relation> finished =
      relational_detail::FinishQuery(context, statement, std::move(input),
                                     nullptr, {}, false);
  ASSERT_TRUE(finished.HasValue()) << finished.GetStatus();
  relational_detail::Relation output = finished.MoveValue();
  ASSERT_EQ(output.rows.size(), 1U);
  EXPECT_EQ(output.rows[0][0], Value(int64_t{kRowCount / 2}));
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       GroupingSetsPreserveDynamicSumTypesAndOverflow) {
  auto context = rs_->BeginContext();
  const auto k = ColumnValueExp("k");
  const auto h = ColumnValueExp("h");
  const auto nil = ConstantValueExp(Value());
  const auto argument = FunctionCallExp(
      "coalesce", {ColumnValueExp("v"), ConstantValueExp(Value(0))});
  const auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, argument);
  SelectStatement fine({NamedExpression("k", k), NamedExpression("h", h),
                        NamedExpression("s", sum)},
                       {}, nullptr);
  fine.SetGroupBy({k, h});
  SelectStatement coarse({NamedExpression("k", k), NamedExpression("h", nil),
                          NamedExpression("s", sum)},
                         {}, nullptr);
  coarse.SetGroupBy({k});
  SelectStatement total({NamedExpression("k", nil), NamedExpression("h", nil),
                         NamedExpression("s", sum)},
                        {}, nullptr);
  total.SetGroupAll(true);
  const std::vector<const SelectStatement*> sets{&fine, &coarse, &total};
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("k", ValueType::kInt64), Column("h", ValueType::kInt64),
           Column("v", ValueType::kDouble)});
  input.AddRow(Row({Value(1), Value(1), Value(1.25)}));
  input.AddRow(Row({Value(1), Value(2), Value()}));
  input.AddRow(Row({Value(2), Value(1), Value()}));
  input.AddRow(Row({Value(2), Value(2), Value()}));
  auto computed =
      relational_detail::TryCompactGroupingSets(context, sets, input);
  ASSERT_TRUE(computed.HasValue()) << computed.GetStatus();
  auto output = computed.MoveValue();
  if (!output) {
    FAIL() << "grouping sets did not use the compact path";
  }
  ASSERT_EQ(output->size(), 3U);
  ASSERT_EQ((*output)[0].rows.size(), 4U);
  ASSERT_EQ((*output)[1].rows.size(), 2U);
  ASSERT_EQ((*output)[2].rows.size(), 1U);
  EXPECT_EQ((*output)[0].rows[0][2], Value(1.25));
  EXPECT_EQ((*output)[0].rows[1][2].type, ValueType::kInt64);
  EXPECT_EQ((*output)[1].rows[1][2].type, ValueType::kInt64);
  EXPECT_EQ((*output)[2].rows[0][2].type, ValueType::kDouble);
  EXPECT_EQ((*output)[2].rows[0][2], Value(1.25));
  // A dynamic row whose type differs from the declared schema keeps the
  // ordinary accumulator; both types remain observable after COALESCE.
  input.AddRow(Row({Value(1), Value(3), Value(2)}));
  auto mixed = relational_detail::TryCompactGroupingSets(context, sets, input);
  ASSERT_TRUE(mixed.HasValue()) << mixed.GetStatus();
  auto mixed_output = mixed.MoveValue();
  if (!mixed_output) {
    FAIL() << "missing mixed-type output";
  }
  EXPECT_EQ((*mixed_output)[2].rows[0][2], Value(3.25));
  input.rows.pop_back();
  relational_detail::Relation repeated;
  repeated.schema = input.schema;
  for (size_t i = 0; i < 8192; ++i) {
    repeated.rows.insert(repeated.rows.end(), input.rows.begin(),
                         input.rows.end());
  }
  repeated.rows.emplace_back(Row({Value(), Value(1), Value(-0.0)}));
  // Fine and coarse groups must each keep their input's floating-point order.
  repeated.rows.emplace_back(Row({Value(3), Value(1), Value(1e16)}));
  repeated.rows.emplace_back(Row({Value(3), Value(2), Value(1.0)}));
  repeated.rows.emplace_back(Row({Value(3), Value(1), Value(-1e16)}));
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  std::vector<std::vector<Row>> reference;
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto grouped =
        relational_detail::TryCompactGroupingSets(context, sets, repeated);
    ASSERT_TRUE(grouped.HasValue()) << grouped.GetStatus();
    auto relations = grouped.MoveValue();
    if (!relations) {
      FAIL() << "parallel grouping sets declined";
    }
    if (workers == 1) {
      for (const auto& relation : *relations) {
        reference.push_back(relation.rows);
      }
    } else {
      ASSERT_EQ(relations->size(), reference.size());
      for (size_t s = 0; s < reference.size(); ++s) {
        EXPECT_EQ((*relations)[s].rows, reference[s]);
        for (size_t r = 0; r < reference[s].size(); ++r) {
          EXPECT_EQ((*relations)[s].rows[r][2].type, reference[s][r][2].type);
        }
      }
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    }
  }
  const auto nullable_sum = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("v"));
  for (auto* statement : {&fine, &coarse, &total}) {
    auto projection = statement->SelectList();
    projection.back().expression = nullable_sum;
    statement->SetSelectList(std::move(projection));
  }
  reference.clear();
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto grouped =
        relational_detail::TryCompactGroupingSets(context, sets, repeated);
    ASSERT_TRUE(grouped.HasValue()) << grouped.GetStatus();
    auto relations = grouped.MoveValue();
    if (!relations) {
      FAIL() << "parallel nullable grouping sets declined";
    }
    ASSERT_TRUE((*relations)[1].rows[1][2].IsNull());
    if (workers == 1) {
      for (const auto& relation : *relations) {
        reference.push_back(relation.rows);
      }
    } else {
      ASSERT_EQ(relations->size(), reference.size());
      for (size_t set = 0; set < reference.size(); ++set) {
        EXPECT_EQ((*relations)[set].rows, reference[set]);
      }
    }
  }
  const auto overflow_sum = std::make_shared<AggregateExpression>(
      AggregationType::kSum,
      FunctionCallExp(
          "coalesce",
          {ColumnValueExp("v"),
           ConstantValueExp(Value(std::numeric_limits<int64_t>::max()))}));
  for (auto* statement : {&fine, &coarse, &total}) {
    auto projection = statement->SelectList();
    projection.back().expression = overflow_sum;
    statement->SetSelectList(std::move(projection));
  }
  const auto overflow =
      relational_detail::TryCompactGroupingSets(context, sets, input);
  EXPECT_EQ(overflow.GetStatus(), Status::kIsInfinity);
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto parallel_overflow =
        relational_detail::TryCompactGroupingSets(context, sets, repeated);
    EXPECT_EQ(parallel_overflow.GetStatus(), Status::kIsInfinity);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, CompactGroupingFoldsExpandedConstantOutputs) {
  auto context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("k", ValueType::kInt64), Column("v", ValueType::kDouble)});
  input.rows = {Row({Value(1), Value(2.5)}), Row({Value(1), Value()}),
                Row({Value(2), Value(3.0)})};
  const auto key = ColumnValueExp("k");
  const auto one = ConstantValueExp(Value(1));
  const auto zero = ConstantValueExp(Value(0));
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  const auto projected_key = CaseExpressionExp(
      {{BinaryExpressionExp(zero, BinaryOperation::kEquals, zero), key}},
      BinaryExpressionExp(one, BinaryOperation::kDivide, zero));
  SelectStatement statement(
      {NamedExpression("k", projected_key), NamedExpression("s", sum),
       NamedExpression("level",
                       BinaryExpressionExp(one, BinaryOperation::kAdd, zero))},
      {}, nullptr);
  statement.SetGroupBy({key});
  auto computed = relational_detail::TryCompactRelationAggregate(
      context, statement, input, {sum.get()});
  ASSERT_TRUE(computed.HasValue()) << computed.GetStatus();
  auto output = computed.MoveValue();
  if (!output) {
    FAIL() << "expanded constants did not use compact grouping";
  }
  EXPECT_EQ(output->rows,
            std::vector<Row>({Row({Value(1), Value(2.5), Value(1)}),
                              Row({Value(2), Value(3.0), Value(1)})}));
  auto projection = statement.SelectList();
  projection[0].expression = CaseExpressionExp(
      {{BinaryExpressionExp(key, BinaryOperation::kGreaterThan, zero), key}},
      ConstantValueExp(Value()));
  statement.SetSelectList(std::move(projection));
  auto conditional = relational_detail::TryCompactRelationAggregate(
      context, statement, input, {sum.get()});
  ASSERT_TRUE(conditional.HasValue()) << conditional.GetStatus();
  EXPECT_FALSE(conditional.Value().has_value());
}

std::vector<SelectStatement> MakeGroupingSetStatements(
    std::initializer_list<std::initializer_list<const char*>> key_sets,
    std::initializer_list<std::pair<const char*, Expression>> agg_selects) {
  std::vector<SelectStatement> statements;
  statements.reserve(key_sets.size());
  for (const auto keys : key_sets) {
    std::vector<Expression> group;
    std::vector<NamedExpression> projection;
    group.reserve(keys.size());
    projection.reserve(keys.size() + agg_selects.size());
    for (const char* name : keys) {
      group.push_back(ColumnValueExp(name));
      projection.emplace_back(name, group.back());
    }
    for (const auto& [alias, expr] : agg_selects) {
      projection.emplace_back(alias, expr);
    }
    statements.emplace_back(std::move(projection), std::vector<std::string>{},
                            nullptr);
    statements.back().SetGroupBy(std::move(group));
    statements.back().SetGroupAll(std::empty(keys));
  }
  return statements;
}

TEST_F(RelationalRegressionTest, GroupingSetParentMapsKeepOrderAndSubsets) {
  auto context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("a", ValueType::kVarChar), Column("b", ValueType::kInt64),
           Column("c", ValueType::kInt64), Column("v", ValueType::kDouble)});
  const std::vector<double> values{1.0e16, 1.0, -1.0e16, -0.0};
  for (int64_t i = 0; i < 65536; ++i) {
    input.AddRow(Row({i % 11 == 0 ? Value() : Value(std::to_string(i % 3)),
                      Value(i % 17), Value(i % 509),
                      Value(values[static_cast<size_t>(i % 4)])}));
  }
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  std::vector<SelectStatement> statements = MakeGroupingSetStatements(
      {{"a", "b", "c"}, {"a", "b"}, {"a"}, {"b"}, {}}, {{"s", sum}});
  std::vector<const SelectStatement*> sets;
  sets.reserve(statements.size());
  for (const auto& statement : statements) {
    sets.push_back(&statement);
  }
  // The finest set spans multiple output morsels; compare exact row order
  // and cancellation-sensitive sums against the serial route.
  std::vector<std::vector<Row>> reference;
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto grouped =
        relational_detail::TryCompactGroupingSets(context, sets, input);
    ASSERT_TRUE(grouped.HasValue()) << grouped.GetStatus();
    auto optional = grouped.MoveValue();
    if (!optional) {
      FAIL() << "grouping sets declined";
    }
    const auto& relations = *optional;
    for (size_t s = 0; s < relations.size(); ++s) {
      if (workers == 1) {
        reference.push_back(relations[s].rows);
      } else {
        EXPECT_EQ(relations[s].rows, reference[s]);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, GroupingSetUniqueFinestKeysKeepSumsAndOrder) {
  auto context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("a", ValueType::kVarChar), Column("b", ValueType::kInt64),
           Column("v", ValueType::kDouble)});
  for (int64_t i = 0; i < 8192; ++i) {
    const double value = i % 3 == 0 ? 1.0e16 : i % 3 == 1 ? 1.0 : -1.0e16;
    input.AddRow(Row({i % 7 == 0 ? Value() : Value("a"), Value(i),
                      i % 11 == 0 ? Value() : Value(value)}));
  }
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  auto statements =
      MakeGroupingSetStatements({{"a", "b"}, {"a"}, {}}, {{"s", sum}});
  std::vector<const SelectStatement*> sets;
  sets.reserve(statements.size());
  for (const auto& statement : statements) {
    sets.push_back(&statement);
  }
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 4);
  auto reference =
      relational_detail::TryCompactGroupingSets(context, sets, input);
  auto unique =
      relational_detail::TryCompactGroupingSets(context, sets, input,
                                                /*finest_keys_unique=*/true);
  ASSERT_TRUE(reference.HasValue()) << reference.GetStatus();
  ASSERT_TRUE(unique.HasValue()) << unique.GetStatus();
  const auto& reference_rows = reference.Value();
  const auto& unique_rows = unique.Value();
  if (!reference_rows.has_value() || !unique_rows.has_value()) {
    ADD_FAILURE() << "expected grouping set results";
    return;
  }
  ASSERT_EQ(unique_rows->size(), reference_rows->size());
  for (size_t s = 0; s < unique_rows->size(); ++s) {
    EXPECT_EQ(unique_rows->at(s).rows, reference_rows->at(s).rows);
  }
}

TEST_F(RelationalRegressionTest, RankedRollupFitsPackedGroupingBudget) {
  struct RestoreBudget {
    RestoreBudget() = default;
    RestoreBudget(const RestoreBudget&) = delete;
    RestoreBudget& operator=(const RestoreBudget&) = delete;
    RestoreBudget(RestoreBudget&&) = delete;
    RestoreBudget& operator=(RestoreBudget&&) = delete;
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
    const size_t limit = QueryMemoryBudget::Global().Limit();
  } restore;
  QueryMemoryBudget::Global().ResetForTest(size_t{5} << 20);

  relational_detail::ExecutionRuntime runtime;
  runtime.grouping_rank_limit = {
      .partition_name = "a", .order_name = "s", .limit = 2};
  auto context = rs_->BeginReadOnlyContext();
  context.set_execution_runtime(&runtime);
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
           Column("v", ValueType::kDouble)});
  constexpr int64_t kRows = 8192;
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kRows));
  for (int64_t i = 0; i < kRows; ++i) {
    const int64_t sum_value = i / 4;
    rows.emplace_back(
        Row({Value(i % 2), Value(i), Value(static_cast<double>(sum_value))}));
  }
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, columnar,
      relational_detail::BuildColumnarTableFromRows(input.schema, rows));
  input.SetColumnarTable(std::move(columnar), false);
  auto statements = MakeGroupingSetStatements(
      {{"a", "b"}, {"a"}, {}},
      {{"s",
        AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"))}});
  auto& total = statements.back();
  auto projection = total.SelectList();
  projection.emplace_back("a", ConstantValueExp(Value()));
  total.SetSelectList(std::move(projection));
  const std::vector<const SelectStatement*> sets{
      statements.data(), &statements[1], &statements[2]};

  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 4);
  // Isolate the accounting to the packed grouping: building the columnar
  // input above is not part of the path whose budget compliance we pin.
  QueryMemoryBudget::Global().ResetForTest(size_t{5} << 20);
  ASSIGN_OR_ASSERT_FAIL(
      std::optional<std::vector<relational_detail::Relation>>, grouped,
      relational_detail::TryCompactGroupingSets(context, sets, input,
                                                /*finest_keys_unique=*/true));
  if (!grouped) {
    FAIL() << "ranked rollup declined the packed grouping budget";
  }
  ASSERT_EQ(grouped->size(), 3U);
  // Checking only the grouped values would pass even if the packed path
  // ignored the limit and kept the whole input resident. Pin the OOC
  // invariant directly: the grouping's charges must fit its own budget.
  EXPECT_LE(QueryMemoryBudget::Global().Used(), size_t{5} << 20);
  std::vector<Row> finest;
  for (int64_t i = kRows - 4; i < kRows; ++i) {
    finest.emplace_back(Row({Value(i % 2), Value(i), Value(2047.0)}));
  }
  constexpr double kPartitionTotal = 4192256.0;
  EXPECT_EQ(grouped->at(0).rows, finest);
  EXPECT_EQ(grouped->at(1).rows,
            (std::vector<Row>{Row({Value(0), Value(kPartitionTotal)}),
                              Row({Value(1), Value(kPartitionTotal)})}));
  EXPECT_EQ(grouped->at(2).rows,
            (std::vector<Row>{Row({Value(2 * kPartitionTotal), Value()})}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest, GroupingSetBorrowedKeysBecomeOwnedOutputs) {
  auto context = rs_->BeginContext();
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  std::vector<SelectStatement> statements =
      MakeGroupingSetStatements({{"a", "b"}, {"a"}, {}}, {{"s", sum}});
  std::vector<const SelectStatement*> sets;
  sets.reserve(statements.size());
  for (const auto& statement : statements) {
    sets.push_back(&statement);
  }
  const std::string label =
      "a shared long grouping key that outlives its input";
  const std::vector<Row> pattern{
      Row({Value(std::string(label)), Value("x"), Value(1)}),
      Row({Value(std::string(label)), Value("x"), Value(2)}),
      Row({Value(), Value("x"), Value(3)}),
      Row({Value("other"), Value(), Value(4)})};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    relational_detail::Relation input;
    input.schema = Schema(
        "", {Column("a", ValueType::kVarChar), Column("b", ValueType::kVarChar),
             Column("v", ValueType::kInt64)});
    input.rows.reserve(40960);
    for (size_t i = 0; i < 40960; ++i) {
      input.rows.push_back(pattern[i % pattern.size()]);
    }
    auto result =
        relational_detail::TryCompactGroupingSets(context, sets, input);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "grouping sets declined";
    }
    input.ResetContents();
    EXPECT_EQ((*output)[0].rows,
              std::vector<Row>(
                  {Row({Value(std::string(label)), Value("x"), Value(30720)}),
                   Row({Value(), Value("x"), Value(30720)}),
                   Row({Value("other"), Value(), Value(40960)})}));
    EXPECT_EQ((*output)[1].rows,
              std::vector<Row>({Row({Value(std::string(label)), Value(30720)}),
                                Row({Value(), Value(30720)}),
                                Row({Value("other"), Value(40960)})}));
    EXPECT_EQ((*output)[2].rows, std::vector<Row>({Row({Value(102400)})}));
  }
}

TEST_F(RelationalRegressionTest, GroupingSetExactPartialsKeepNumericBounds) {
  auto context = rs_->BeginContext();
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  const auto avg = std::make_shared<AggregateExpression>(AggregationType::kAvg,
                                                         ColumnValueExp("v"));
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("v"));
  std::vector<SelectStatement> statements = MakeGroupingSetStatements(
      {{"a", "b"}, {"a"}, {}}, {{"s", sum}, {"a", avg}, {"n", count}});
  std::vector<const SelectStatement*> sets;
  sets.reserve(statements.size());
  for (const auto& statement : statements) {
    sets.push_back(&statement);
  }
  for (const bool large : {false, true}) {
    relational_detail::Relation input;
    input.schema = Schema(
        "", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
             Column("v", ValueType::kInt64)});
    const std::array<int64_t, 4> pattern{large ? 10000000000000000LL : 9, 1,
                                         large ? -10000000000000000LL : -9, 1};
    for (size_t r = 0; r < 32768; ++r) {
      input.rows.emplace_back(std::vector<Value>{
          Value(static_cast<int64_t>((r / 8) % 5)),
          Value(static_cast<int64_t>(r % 2)),
          r % 8 >= 4 ? Value() : Value(pattern[r % pattern.size()])});
    }
    std::vector<std::vector<Row>> reference;
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto grouped =
          relational_detail::TryCompactGroupingSets(context, sets, input);
      ASSERT_TRUE(grouped.HasValue()) << grouped.GetStatus();
      auto output = grouped.MoveValue();
      if (!output) {
        FAIL() << "grouping sets declined";
      }
      for (size_t s = 0; s < output->size(); ++s) {
        if (workers == 1) {
          reference.push_back((*output)[s].rows);
        } else {
          EXPECT_EQ((*output)[s].rows, reference[s]);
        }
      }
    }
    // A later negative value must not conceal a checked prefix overflow.
    input.rows[0].values_[2] = Value(INT64_MAX);
    input.rows[1].values_[2] = Value(1);
    input.rows[1].values_[1] = input.rows[0].values_[1];
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      const auto grouped =
          relational_detail::TryCompactGroupingSets(context, sets, input);
      EXPECT_FALSE(grouped.HasValue());
      EXPECT_EQ(grouped.GetStatus(), Status::kIsInfinity);
    }
  }
}

TEST_F(RelationalRegressionTest,
       SlimDoubleDistinctKeepsCanonicalPeersAcrossWorkers) {
  const Schema schema("FloatDistinct", {Column("k", ValueType::kInt64),
                                        Column("v", ValueType::kDouble)});
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    for (int64_t i = 0; i < 49152; ++i) {
      const int64_t slot = i % 24;
      Value value;
      if (slot < 16) {
        value = Value(static_cast<double>(slot));
      } else if (slot < 18) {
        value = Value(slot == 16 ? std::numeric_limits<double>::quiet_NaN()
                                 : -std::numeric_limits<double>::quiet_NaN());
      } else if (slot == 18) {
        value = Value(-0.0);
      } else if (slot == 19) {
        value = Value(std::numeric_limits<double>::infinity());
      } else if (slot == 20) {
        value = Value(-std::numeric_limits<double>::infinity());
      }
      ASSERT_TRUE(
          table.Insert(setup.txn_, Row({Value(i % 3), value})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  auto context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("FloatDistinct"));
  auto distinct = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("v"), true);
  auto count = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                     ColumnValueExp("v"));
  const std::vector<const AggregateExpression*> aggregates{distinct.get(),
                                                           count.get()};
  SelectStatement statement(
      {NamedExpression("n", distinct), NamedExpression("c", count)}, {},
      nullptr);
  SelectSource source;
  source.table = "FloatDistinct";
  statement.SetSources({source});
  std::atomic<size_t> active{1};
  for (size_t workers : {size_t{1}, size_t{2}}) {
    ScopedQueryParallelismBudget budget(&active, workers);
    auto plan = relational_detail::PlanTypedAggregation(statement, nullptr,
                                                        schema, aggregates, {});
    ASSERT_TRUE(plan.eligible);
    ASSERT_TRUE(plan.slim);
    ASSERT_EQ(plan.slim_kinds[0],
              relational_detail::SlimAggKind::kCountDistinctDouble);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::RunTypedParallelStreamAggregate(
            context, statement, nullptr, *table, "FloatDistinct", false, {0, 1},
            schema, schema, {}, aggregates, {}, std::move(plan)));
    EXPECT_EQ(result.rows, std::vector<Row>({Row({Value(19), Value(43008)})}));
  }
  for (const int64_t boundary : {(int64_t{1} << 53), (int64_t{1} << 53) + 1}) {
    const auto compare = relational_detail::TryCompileTypedFilterCompare(
        BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(boundary))),
        schema, true);
    EXPECT_EQ(compare.has_value(), boundary == (int64_t{1} << 53));
  }
  EXPECT_FALSE(relational_detail::TryCompileTypedFilterCompare(
                   BinaryExpressionExp(
                       ColumnValueExp("k"), BinaryOperation::kGreaterThan,
                       ConstantValueExp(Value(-1).WithUnsigned())),
                   schema, true)
                   .has_value());
  const auto disjunction = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(0))),
      BinaryOperation::kOr,
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(10))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("v"),
                              BinaryOperation::kLessThanEquals,
                              ConstantValueExp(Value(12)))));
  for (const bool residual : {false, true}) {
    ScopedQueryParallelismBudget budget(&active, 2);
    std::vector<Expression> predicates{disjunction};
    if (residual) {
      predicates.push_back(
          UnaryExpressionExp(ColumnValueExp("v"), UnaryOperation::kIsNotNull));
    }
    statement.SetWhereClause(CombineConjuncts(predicates));
    auto filtered_plan = relational_detail::PlanTypedAggregation(
        statement, nullptr, schema, aggregates, predicates);
    ASSERT_TRUE(filtered_plan.eligible);
    ASSERT_EQ(filtered_plan.filter_disjunctions.size(), 1U);
    EXPECT_EQ(filtered_plan.filter_programs.empty(), !residual);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, filtered,
        relational_detail::RunTypedParallelStreamAggregate(
            context, statement, nullptr, *table, "FloatDistinct", false, {0, 1},
            schema, schema, {}, aggregates, {}, std::move(filtered_plan)));
    EXPECT_EQ(filtered.rows, std::vector<Row>({Row({Value(8), Value(18432)})}));
  }
  statement.SetWhereClause(nullptr);
  statement.SetGroupBy({ColumnValueExp("k")});
  statement.SetSelectList({NamedExpression("k", ColumnValueExp("k")),
                           NamedExpression("n", distinct),
                           NamedExpression("c", count)});
  ScopedQueryParallelismBudget budget(&active, 2);
  auto plan = relational_detail::PlanTypedAggregation(statement, nullptr,
                                                      schema, aggregates, {});
  ASSERT_TRUE(plan.slim);
  ASSIGN_OR_ASSERT_FAIL(
      relational_detail::Relation, grouped,
      relational_detail::RunTypedParallelStreamAggregate(
          context, statement, nullptr, *table, "FloatDistinct", false, {0, 1},
          schema, schema, {}, aggregates, {}, std::move(plan)));
  ASSERT_EQ(grouped.TotalRows(), 3U);
  for (const Row& row : grouped.rows) {
    EXPECT_EQ(row[1], Value(row[0] == Value(0) ? 6 : 7));
    EXPECT_EQ(row[2], Value(14336));
  }
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(RelationalRegressionTest, NullLeadingProjectionRetainsUniformTypes) {
  auto context = rs_->BeginContext();
  for (const size_t width : {size_t{1}, size_t{3}}) {
    for (const bool computed : {false, true}) {
      for (const int domain : {0, 1, 2, 3}) {
        relational_detail::Relation input;
        input.schema = Schema("", {Column("v", ValueType::kDouble),
                                   Column("unused", ValueType::kInt64)});
        input.rows.emplace_back(Row({Value(), Value(0)}));
        if (domain == 0) {
          input.rows.emplace_back(Row({Value(2.5), Value(1)}));
          input.rows.emplace_back(Row({Value(3.5), Value(2)}));
        } else if (domain == 1) {
          input.rows.emplace_back(Row({Value(-1).WithUnsigned(), Value(1)}));
        } else if (domain == 2) {
          input.rows.emplace_back(Row({Value(2), Value(1)}));
          input.rows.emplace_back(Row({Value(2.5), Value(2)}));
        }
        const auto original = input.rows;
        std::vector<NamedExpression> items;
        items.reserve(width);
        for (size_t i = 0; i < width; ++i) {
          Expression value = ColumnValueExp("v");
          if (computed) {
            value =
                FunctionCallExp("coalesce", {value, ConstantValueExp(Value())});
          }
          items.emplace_back("v" + std::to_string(i), value);
        }
        SelectStatement statement(std::move(items), {}, nullptr);
        auto result = relational_detail::FinishQuery(
            context, statement, std::move(input), nullptr, {}, false);
        ASSERT_TRUE(result.HasValue()) << result.GetStatus();
        const auto& output = result.Value();
        ASSERT_EQ(output.rows.size(), original.size());
        for (size_t i = 0; i < width; ++i) {
          EXPECT_EQ(output.schema.GetColumn(i).Type(),
                    domain == 0 || (domain == 3 && !computed)
                        ? ValueType::kDouble
                    : domain == 1 ? ValueType::kInt64
                                  : ValueType::kNull);
          EXPECT_EQ(output.schema.GetColumn(i).IsUnsigned(), domain == 1);
          for (size_t r = 0; r < original.size(); ++r) {
            EXPECT_EQ(output.rows[r][i], original[r][0]);
            EXPECT_EQ(output.rows[r][i].IsUnsigned(),
                      original[r][0].IsUnsigned());
          }
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, CompactMaterializedGroupsPreserveBoundaries) {
  auto context = rs_->BeginContext();
  auto key = ColumnValueExp("k");
  auto value = ColumnValueExp("v");
  auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, value);
  auto count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, value);
  auto avg =
      std::make_shared<AggregateExpression>(AggregationType::kAvg, value);
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("s", sum),
       NamedExpression("n", count), NamedExpression("a", avg)},
      {}, nullptr);
  statement.SetGroupBy({key});
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  input.AddRow(Row({Value(1), Value(2)}));
  input.AddRow(Row({Value(1), Value()}));
  input.AddRow(Row({Value(1), Value(4)}));
  input.AddRow(Row({Value(), Value()}));
  const std::vector<const AggregateExpression*> aggregates{
      sum.get(), count.get(), avg.get()};
  auto compact = relational_detail::TryCompactRelationAggregate(
      context, statement, input, aggregates);
  ASSERT_TRUE(compact.HasValue()) << compact.GetStatus();
  auto grouped0 = compact.MoveValue();
  if (!grouped0) {
    FAIL() << "compact aggregation declined";
  }
  EXPECT_EQ(grouped0->rows,
            std::vector<Row>({Row({Value(1), Value(6), Value(2), Value(3.0)}),
                              Row({Value(), Value(), Value(0), Value()})}));
  input.ResetContents();
  compact = relational_detail::TryCompactRelationAggregate(context, statement,
                                                           input, aggregates);
  ASSERT_TRUE(compact.HasValue());
  auto grouped1 = compact.MoveValue();
  if (!grouped1) {
    FAIL() << "compact aggregation declined";
  }
  EXPECT_TRUE(grouped1->rows.empty());
  statement.SetGroupBy({});
  statement.SetSelectList({NamedExpression("s", sum),
                           NamedExpression("n", count),
                           NamedExpression("a", avg)});
  compact = relational_detail::TryCompactRelationAggregate(context, statement,
                                                           input, aggregates);
  ASSERT_TRUE(compact.HasValue());
  auto grouped2 = compact.MoveValue();
  if (!grouped2) {
    FAIL() << "compact aggregation declined";
  }
  EXPECT_EQ(grouped2->rows,
            std::vector<Row>({Row({Value(), Value(0), Value()})}));
  input.AddRow(Row({Value(1), Value()}));
  input.AddRow(Row({Value(1), Value(2)}));
  input.AddRow(Row({Value(1), Value(4)}));
  compact = relational_detail::TryCompactRelationAggregate(context, statement,
                                                           input, aggregates);
  ASSERT_TRUE(compact.HasValue());
  auto scalar = compact.MoveValue();
  if (!scalar) {
    FAIL() << "compact scalar aggregation declined";
  }
  EXPECT_EQ(scalar->rows,
            std::vector<Row>({Row({Value(6), Value(2), Value(3.0)})}));
  input.ResetContents();
  input.AddRow(Row({Value(1), Value(std::numeric_limits<int64_t>::max())}));
  input.AddRow(Row({Value(1), Value(1)}));
  compact = relational_detail::TryCompactRelationAggregate(context, statement,
                                                           input, aggregates);
  EXPECT_FALSE(compact.HasValue());
  EXPECT_EQ(compact.GetStatus().GetCode(), StatusCode::kIsInfinity);
  // Rejecting this shape for memory pressure lets the existing spilling
  // implementation take over before any partial group has been emitted.
  const size_t limit = QueryMemoryBudget::Global().Limit();
  QueryMemoryBudget::Global().ResetForTest(1);
  compact = relational_detail::TryCompactRelationAggregate(context, statement,
                                                           input, aggregates);
  QueryMemoryBudget::Global().ResetForTest(limit);
  ASSERT_TRUE(compact.HasValue());
  EXPECT_FALSE(compact.Value().has_value());
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, CompactBorrowedKeysPreserveCanonicalPeers) {
  auto context = rs_->BeginContext();
  auto text = ColumnValueExp("s"), number = ColumnValueExp("d");
  auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ConstantValueExp(Value(1)));
  SelectStatement statement(
      {NamedExpression("s", text), NamedExpression("d", number),
       NamedExpression("n", count)},
      {}, nullptr);
  statement.SetGroupBy({text, number});
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("s", ValueType::kVarChar), Column("d", ValueType::kDouble)});
  const double nan = std::numeric_limits<double>::quiet_NaN();
  input.AddRow(Row({Value("long text repeated across rows"), Value(nan)}));
  input.AddRow(Row({Value("long text repeated across rows"), Value(-nan)}));
  input.AddRow(Row({Value("zero"), Value(-0.0)}));
  input.AddRow(Row({Value("zero"), Value(0.0)}));
  input.AddRow(Row({Value("UPPER").WithCollation(2), Value(1.0)}));
  input.AddRow(Row({Value("upper").WithCollation(2), Value(1.0)}));
  input.AddRow(Row({Value(), Value()}));
  input.AddRow(Row({Value(), Value()}));
  // Repeated join images share text storage, while the first two peers own
  // independent buffers. Both forms must coalesce, including NaNs, signed
  // zero, collated text and NULLs.
  for (size_t i = 8; i < 8192; ++i) {
    input.AddRow(Row(input.rows[i % 8]));
  }
  auto result = relational_detail::TryCompactRelationAggregate(
      context, statement, input, {count.get()});
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  auto output = result.MoveValue();
  if (!output) {
    FAIL() << "compact grouping declined";
  }
  input.ResetContents();  // Grouped output must own its strings.
  ASSERT_EQ(output->rows.size(), 4U);
  for (const auto& row : output->rows) {
    EXPECT_EQ(row[2], Value(2048));
  }
  EXPECT_EQ(output->rows[0][0], Value("long text repeated across rows"));
  EXPECT_TRUE(std::isnan(output->rows[0][1].value.double_value));
  EXPECT_FALSE(std::signbit(output->rows[1][1].value.double_value));
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

// Computed GROUP BY keys and aggregate arguments join the parallel fold
// through bytecode programs (workers cannot run the contextual evaluator;
// a program that fails to compile keeps the serial loop).  The row count
// keeps the fold above kParallelGroupMinRows; a machine below the worker
// floor declines the parallel path and the serial loop must produce the
// same groups.
TEST_F(RelationalRegressionTest,
       ParallelGroupedFinishEvaluatesExpressionKeysAndArgs) {
  constexpr size_t kRowCount = 262144;
  TransactionContext context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema = Schema("parallel_expr_keys", {Column("g", ValueType::kInt64),
                                               Column("v", ValueType::kInt64)});
  input.rows.reserve(kRowCount);
  for (size_t i = 0; i < kRowCount; ++i) {
    input.AddRow(Row(
        {Value(static_cast<int64_t>(i)), Value(static_cast<int64_t>(i % 8))}));
  }
  const Expression key =
      BinaryExpressionExp(ColumnValueExp("g"), BinaryOperation::kModulo,
                          ConstantValueExp(Value(int64_t{4})));
  auto count = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                     ColumnValueExp("*"));
  auto sum = std::make_shared<AggregateExpression>(
      AggregationType::kSum,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kMultiply,
                          ConstantValueExp(Value(int64_t{2}))),
      false);
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("c", count),
       NamedExpression("s", sum)},
      {}, nullptr);
  statement.SetGroupBy({key});

  StatusOr<relational_detail::Relation> finished =
      relational_detail::FinishQuery(context, statement, std::move(input),
                                     nullptr, {}, false);
  ASSERT_TRUE(finished.HasValue()) << finished.GetStatus();
  relational_detail::Relation output = finished.MoveValue();
  ASSERT_EQ(output.rows.size(), 4U);
  std::map<int64_t, std::pair<int64_t, int64_t>> by_key;
  for (const Row& row : output.rows) {
    by_key.emplace(
        row[0].value.int_value,
        std::make_pair(row[1].value.int_value, row[2].value.int_value));
  }
  // Group q holds the 65536 rows with i ≡ q (mod 4); their v = i % 8 splits
  // evenly between q and q + 4, so SUM(v * 2) = 65536 * (2q + 4).
  for (const int64_t q : {int64_t{0}, int64_t{1}, int64_t{2}, int64_t{3}}) {
    const auto iter = by_key.find(q);
    ASSERT_TRUE(iter != by_key.end()) << "missing group " << q;
    EXPECT_EQ(iter->second.first, 65536) << "group " << q;
    EXPECT_EQ(iter->second.second, 65536 * (2 * q + 4)) << "group " << q;
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

// A single statement that first narrows AggTable through an uncorrelated IN
// list pushdown (BuildInput stores table_key_filters["AggTable"]) and then
// aggregates the same table in an uncorrelated scalar subquery. The subquery's
// stream_agg cache fill must stay unfiltered; only its own read may filter.
TEST_F(RelationalRegressionTest,
       Execute_StreamAggWithInListFilter_IgnoresStashedFilterInSubquery) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT COUNT(*), (SELECT SUM(k + v) FROM AggTable) "
                    "FROM AggTable WHERE k IN (SELECT k FROM SmallKeys);");
  ASSERT_EQ(rows.size(), 1U);
  // Outer aggregation sees exactly the IN-narrowed rows.
  EXPECT_EQ(rows[0][0], Value(80));
  // Inner aggregation must cover ALL 200 rows (1100), not the stashed subset
  // with k in {1,2,3,4} which would aggregate to 280.
  EXPECT_EQ(rows[0][1], Value(1100));
}

// Same invariant with a join-derived semi-join stash: loading the selective
// driver pushes its keys into the joined table's scan and stores the same key
// set for that table. Because every reference of a reusable table shares one
// projected cache entry, the driver below is a DIFFERENT table so the stash
// exists before AggTable's first (and only) cache fill.
TEST_F(RelationalRegressionTest,
       Execute_StreamAggWithJoinDerivedFilter_IgnoresStashedFilterInSubquery) {
  const auto rows = RelationalRun(
      *rs_,
      "SELECT COUNT(*), (SELECT SUM(k + v) FROM AggTable) "
      "FROM KeyTable AS s JOIN AggTable AS b ON s.k = b.k WHERE s.k < 5;");
  ASSERT_EQ(rows.size(), 1U);
  // Keys {0..4} of the driver meet 20 AggTable rows each.
  EXPECT_EQ(rows[0][0], Value(100));
  // Full-table aggregation again; the stash {0..4} would yield 300.
  EXPECT_EQ(rows[0][1], Value(1100));
}

// Two stream_agg consumers share one BaseRelationCacheKey entry because their
// required columns match. The first fills the cache while reading with
// `k < 3`; the second must observe rows outside that predicate, proving the
// cached content is not narrowed by the first consumer.
TEST_F(RelationalRegressionTest,
       Execute_SharedBaseRelationCache_StaysUnfilteredAcrossConsumers) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT (SELECT SUM(v) FROM AggTable WHERE k < 3), "
                    "       (SELECT SUM(v) FROM AggTable WHERE k >= 8) "
                    "FROM SmallKeys;");
  ASSERT_EQ(rows.size(), 4U);
  for (const Row& row : rows) {
    EXPECT_EQ(row[0], Value(60));
    // Would be NULL if the cache had been filled with the first consumer's
    // k < 3 subset: no remaining row could satisfy k >= 8.
    EXPECT_EQ(row[1], Value(40));
  }
}

// MIN/MAX accumulators never incremented `count`, so the parallel merge's
// empty-partial skip (`count == 0`) dropped every worker's MIN/MAX partial.
// A second uncorrelated scalar subquery re-aggregates the shared
// base-relation cache over row-sliced workers; before the fix it returned
// the first worker slice's row value instead of the true aggregate.
TEST_F(RelationalRegressionTest,
       Execute_SharedBaseRelationCache_MinMaxMergesAcrossWorkers) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT (SELECT MAX(k) FROM SmallKeys) AS a, "
                    "       (SELECT MAX(k) FROM SmallKeys) + 1 AS b, "
                    "       (SELECT MIN(k) FROM SmallKeys) AS c, "
                    "       (SELECT MIN(-k) FROM SmallKeys) - 10 AS d;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(4));
  EXPECT_EQ(rows[0][1], Value(5));
  EXPECT_EQ(rows[0][2], Value(1));
  // MIN(-k) over {1,2,3,4} is -4; before the fix the cache-hit merge kept
  // only the first worker slice (k = 1), producing -1 - 10 = -11.
  EXPECT_EQ(rows[0][3], Value(-14));
}

}  // namespace
}  // namespace tinylamb
