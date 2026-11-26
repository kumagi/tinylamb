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

#include "common/test_util.hpp"
#include "common/platform/process.hpp"
#include "executor/relational_regression_test.hpp"
#include "table/tlcs_file.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, ScalarSubqueryReadsEachInheritedCteBinding) {
  auto subquery = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("n", ColumnValueExp("n"))},
      std::vector<std::string>{}, nullptr);
  SelectSource source;
  source.table = "work";
  subquery->SetSources({source});
  const Expression expression = QueryExpressionExp(subquery);
  auto context = rs_->BeginReadOnlyContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  relational_detail::CteMap ctes;
  for (int n : {1, 2, 3}) {
    auto input = std::make_shared<relational_detail::Relation>();
    input->schema = Schema("work", {Column("n", ValueType::kInt64)});
    input->AddRow(Row({Value(n)}));
    ctes["work"] = std::move(input);
    // Give the outer scope a same-named column holding a different value.
    // A correlated scalar subquery must resolve `n` from its inherited CTE
    // binding (the local `work` source), never from the outer row; the
    // assertion below would then observe 1000+n instead of n.
    const Row outer_row({Value(1000 + n)});
    const Schema outer_schema("outer", {Column("n", ValueType::kInt64)});
    const relational_detail::Scope outer{.row = &outer_row,
                                         .schema = &outer_schema};
    ASSIGN_OR_ASSERT_FAIL(Value, result,
                          relational_detail::TryEvaluate(
                              expression, outer, nullptr, context, ctes));
    EXPECT_EQ(result, Value(n));
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest,
       CorrelatedScalarExtremaKeepNullAndNanSemantics) {
  using namespace relational_detail;
  auto ctx = rs_->BeginContext();
  ExecutionRuntime runtime;
  ctx.set_execution_runtime(&runtime);
  auto input = std::make_shared<Relation>();
  input->schema = Schema(
      "d", {Column("k", ValueType::kInt64), Column("v", ValueType::kDouble)});
  const Value nan(std::numeric_limits<double>::quiet_NaN());
  const Value inf(std::numeric_limits<double>::infinity());
  const std::vector<std::vector<Value>> groups{
      {},
      {Value(), Value()},
      {Value(3.0), nan, Value(-2.0)},
      {nan, Value(3.0)},
      {inf, Value(-std::numeric_limits<double>::infinity())},
      {Value(-0.0), Value(0.0)}};
  std::vector<Row> rows;
  for (size_t k = 0; k < groups.size(); ++k) {
    for (const auto& value : groups[k]) {
      rows.emplace_back(
          std::vector<Value>{Value(static_cast<int64_t>(k)), value});
    }
  }
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<ColumnarTable>, columns,
                        BuildColumnarTableFromRows(input->schema, rows));
  input->SetColumnarTable(std::move(columns), false);
  const Schema outer_schema("o",
                            {Column(ColumnName("o", "k"), ValueType::kInt64)});
  for (const auto kind : {AggregationType::kMin, AggregationType::kMax}) {
    auto aggregate = std::make_shared<AggregateExpression>(
        kind, ColumnValueExp(ColumnName("d", "v")));
    SelectStatement statement(
        {NamedExpression("v", aggregate)}, {"d"},
        BinaryExpressionExp(ColumnValueExp(ColumnName("d", "k")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName("o", "k"))));
    for (size_t k = 0; k < groups.size(); ++k) {
      Row outer_row({Value(static_cast<int64_t>(k))});
      Scope scope{.row = &outer_row, .schema = &outer_schema};
      std::optional<Value> scalar;
      auto result = ExecuteCorrelatedSingleSource(
          ctx, statement, scope, {{"d", input}}, false, nullptr, &scalar);
      if (!result.has_value() || !scalar.has_value()) {
        ADD_FAILURE() << "correlated aggregate returned no scalar";
        return;
      }
      const Value& actual = scalar.value();
      AggregateAccumulator reference(aggregate.get(), &input->schema);
      for (const auto& value : groups[k]) {
        ASSERT_SUCCESS(reference.Add(value));
      }
      ASSIGN_OR_ASSERT_FAIL(Value, expected, reference.TryFinish());
      if (expected.type == ValueType::kDouble &&
          std::isnan(expected.value.double_value)) {
        ASSERT_EQ(actual.type, ValueType::kDouble);
        EXPECT_TRUE(std::isnan(actual.value.double_value));
      } else {
        EXPECT_EQ(actual, expected);
      }
    }
  }
  ctx.set_execution_runtime(nullptr);
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.Abort());
}

TEST_F(RelationalRegressionTest, PackedProbeCountGroupsKeepSignedKeysAndNulls) {
  using namespace relational_detail;
  const Schema fact_schema("f",
                           {Column(ColumnName("f", "k"), ValueType::kInt64),
                            Column(ColumnName("f", "g"), ValueType::kInt64)});
  const auto probe_key = ColumnValueExp(ColumnName("f", "k"));
  const auto probe_group = ColumnValueExp(ColumnName("f", "g"));
  const auto build_key = ColumnValueExp(ColumnName("d", "k"));
  const auto build_group = ColumnValueExp(ColumnName("d", "tag"));
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  Relation dimension;
  dimension.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "tag"), ValueType::kInt64)});
  dimension.rows = {Row({Value(1), Value(10)}), Row({Value(2), Value(20)})};
  for (bool bounded : {true, false}) {
    const std::string table = bounded ? "PackedCount" : "WideCount";
    RelationalRun(*rs_, "CREATE TABLE " + table + "(k INT64,g INT64)");
    const Value first =
        bounded ? Value(std::numeric_limits<int32_t>::min()) : Value();
    const Value second =
        bounded ? Value(std::numeric_limits<int32_t>::max())
                : Value(int64_t{1} + std::numeric_limits<int32_t>::max());
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                            setup.GetTable(table));
      for (int i = 0; i < 4096; ++i) {
        ASSERT_SUCCESS(
            fact->Insert(setup.txn_,
                         Row({Value((i % 2) + 1), i % 4 < 2 ? first : second}))
                .GetStatus());
      }
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_, Row({Value(1), Value(3)})).GetStatus());
      ASSERT_SUCCESS(setup.PreCommit());
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact, ctx.GetTable(table));
    DeferredLSTableProbe deferred;
    deferred.store = GetOrBuildLSTableStore(ctx.txn_, *fact, true);
    ASSERT_NE(deferred.store, nullptr);
    deferred.projection = {0, 1};
    deferred.local_preds_covered_by_peeks = true;
    Relation probe;
    probe.schema = fact_schema;
    GroupedJoinFusionSpec fusion;
    fusion.eligible = true;
    fusion.select_items = {probe_group, build_group, count};
    fusion.select_names = {"g", "tag", "n"};
    fusion.group_keys = {probe_group, build_group};
    fusion.aggregates = {count.get()};
    fusion.count_star = {true};
    fusion.having = BinaryExpressionExp(count, BinaryOperation::kGreaterThan,
                                        ConstantValueExp(Value(4)));
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, 4);
    const auto equality =
        BinaryExpressionExp(probe_key, BinaryOperation::kEquals, build_key);
    for (const bool residual : {false, true}) {
      if (residual && !bounded) {
        break;
      }
      std::vector<Expression> predicates{equality};
      if (residual) {
        predicates.push_back(BinaryExpressionExp(
            probe_group, BinaryOperation::kNotEquals, build_group));
      }
      auto per_run_fusion = fusion;
      ASSIGN_OR_ASSERT_FAIL(
          std::optional<Relation>, result,
          TryFusedGroupedJoin(ctx, probe, dimension, predicates,
                              JoinType::kInner, nullptr, {}, &per_run_fusion,
                              &deferred));
      ASSERT_TRUE(result.has_value());
      EXPECT_EQ(result->rows.size(), 4U);
      for (const Value& key : {first, second}) {
        for (int tag : {10, 20}) {
          EXPECT_NE(std::ranges::find(result->rows,
                                      Row({key, Value(tag), Value(1024)})),
                    result->rows.end());
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, LargeUniqueBuildKeepsDirectAggregateGroups) {
  using namespace relational_detail;
  for (bool nullable : {false, true}) {
    const std::string table_name = nullable ? "RankedNullable" : "RankedPlain";
    Relation left, right;
    left.schema =
        Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                     Column(ColumnName("f", "v"), ValueType::kDouble),
                     Column(ColumnName("f", "flag"), ValueType::kInt64)});
    right.schema =
        Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                     Column(ColumnName("d", "a"), ValueType::kInt64)});
    auto build = std::make_shared<ColumnarTable>();
    constexpr size_t groups = 524300;
    // Cross a key-partition boundary to exercise concurrent disjoint writes.
    constexpr int64_t probe_begin = static_cast<int64_t>(groups / 3) - 512;
    constexpr int64_t key_bias = -1048600;
    for (size_t begin = 0; begin < groups; begin += 1024) {
      ColumnarChunk chunk;
      chunk.InitFromSchema(right.schema, 1024);
      for (size_t i = begin; i < std::min(groups, begin + 1024); ++i) {
        chunk.AppendRow(
            Row({i == static_cast<size_t>(probe_begin + 42)
                     ? Value()
                     : Value(static_cast<int64_t>(i * 4) + key_bias),
                 Value(static_cast<int64_t>(i % 7))}));
      }
      build->AppendChunk(std::move(chunk));
    }
    right.SetColumnarTable(build, true);
    auto probe = std::make_shared<ColumnarTable>();
    for (size_t begin = 0; begin < 40960; begin += 1024) {
      ColumnarChunk chunk;
      chunk.InitFromSchema(left.schema, 1024);
      for (size_t row = begin; row < begin + 1024; ++row) {
        const auto i = static_cast<int64_t>(row / 40);
        chunk.AppendRow(Row(
            {Value(((i + probe_begin) * 4) + key_bias + (i % 17 == 0 ? 1 : 0)),
             nullable && i % 11 == 0 ? Value() : Value(1.25),
             Value(static_cast<int64_t>(i % 13 == 0 ? 0 : row % 2))}));
      }
      probe->AppendChunk(std::move(chunk));
    }
    left.SetColumnarTable(std::move(probe), false);
    RelationalRun(
        *rs_, "CREATE TABLE " + table_name + "(k INT64,v DOUBLE,flag INT64)");
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                            setup.GetTable(table_name));
      Status inserted = Status::kSuccess;
      ASSERT_SUCCESS(left.ForEachRow([&](const Row& row) {
        if (inserted.ok()) inserted = fact->Insert(setup.txn_, row).GetStatus();
      }));
      ASSERT_SUCCESS(inserted);
      ASSERT_SUCCESS(setup.PreCommit());
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          ctx.GetTable(table_name));
    DeferredLSTableProbe deferred;
    deferred.store = GetOrBuildLSTableStore(ctx.txn_, *fact, true);
    ASSERT_NE(deferred.store, nullptr);
    ASSERT_TRUE(deferred.store->column_is_monotonic[0]);
    deferred.projection = {0, 1, 2};
    deferred.integer_peeks = {{2, BinaryOperation::kGreaterThan, 0}};
    deferred.local_preds_covered_by_peeks = true;
    auto key = ColumnValueExp(ColumnName("d", "k"));
    auto attr = ColumnValueExp(ColumnName("d", "a"));
    auto sum = std::make_shared<AggregateExpression>(
        AggregationType::kSum, ColumnValueExp(ColumnName("f", "v")));
    GroupedJoinFusionSpec fusion;
    fusion.eligible = true;
    fusion.group_keys = {key, attr};
    fusion.select_items = {key, attr, sum};
    fusion.select_names = {"k", "a", "s"};
    fusion.aggregates = {sum.get()};
    fusion.count_star = {false};
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, 3);
    Relation deferred_left;
    deferred_left.schema = left.schema;
    for (int mode : {0, 1, 2}) {
      const bool direct_scan = mode != 0;
      // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type
      // is non-copyable by design
      struct RestoreColumnarLimit {  // NOLINT(cppcoreguidelines-special-member-functions): local scope guard.
        size_t saved = ProcessMemoryBudget::Global().ColumnarPoolLimit();
        ~RestoreColumnarLimit() {
          ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(saved);
        }
      } restore;
      if (mode == 1) {
        ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(1);
      }
      ASSIGN_OR_ASSERT_FAIL(
          std::optional<Relation>, output,
          TryFusedGroupedJoin(
              ctx, direct_scan ? deferred_left : left, right,
              {BinaryExpressionExp(ColumnValueExp(ColumnName("f", "k")),
                                   BinaryOperation::kEquals, key)},
              JoinType::kInner, nullptr, {}, &fusion,
              direct_scan ? &deferred : nullptr));
      ASSERT_TRUE(output.has_value());
      if (!nullable && direct_scan) {
        if (mode == 1) {
          EXPECT_EQ(deferred.store->IntegerRuns(0), nullptr);
        } else {
          EXPECT_NE(deferred.store->IntegerRuns(0), nullptr);
          EXPECT_NE(deferred.store->IntegerRunBounds(0, 2), nullptr);
        }
      }
      std::map<int64_t, Row> actual;
      ASSERT_SUCCESS(output->ForEachRow([&](const Row& row) {
        actual.emplace(row[0].value.int_value, row);
      }));
      ASSERT_EQ(actual.size(), direct_scan ? 888U : 962U);
      for (int64_t i = 0; i < 1024; ++i) {
        if (i % 17 == 0 || i == 42 || (direct_scan && i % 13 == 0)) {
          continue;
        }
        EXPECT_EQ(
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence
            // is unambiguous
            actual.at(((i + probe_begin) * 4) + key_bias),
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence
            // is unambiguous
            Row({Value(((i + probe_begin) * 4) + key_bias),
                 Value((i + probe_begin) % 7),
                 nullable && i % 11 == 0 ? Value()
                                         : Value(direct_scan ? 25.0 : 50.0)}));
      }
    }
    if (!nullable) {
      // A duplicate in another worker partition must disable the unique-key
      // index, retaining both contributions through the ordinary buckets.
      ColumnarChunk duplicate;
      duplicate.InitFromSchema(right.schema, 1);
      duplicate.AppendRow(Row({Value(((probe_begin + 1) * 4) + key_bias),
                               Value((probe_begin + 1) % 7)}));
      build->AppendChunk(std::move(duplicate));
      right.SetColumnarTable(build, true);
      ASSIGN_OR_ASSERT_FAIL(
          std::optional<Relation>, output,
          TryFusedGroupedJoin(
              ctx, deferred_left, right,
              {BinaryExpressionExp(ColumnValueExp(ColumnName("f", "k")),
                                   BinaryOperation::kEquals, key)},
              JoinType::kInner, nullptr, {}, &fusion, &deferred));
      ASSERT_TRUE(output.has_value());
      size_t rows = 0;
      ASSERT_SUCCESS(output->ForEachRow([&](const Row& row) {
        ++rows;
        // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is
        // unambiguous
        const int64_t i = ((row[0].value.int_value - key_bias) / 4) - probe_begin;
        EXPECT_EQ(row[2], Value(i == 1 ? 50.0 : 25.0));
      }));
      EXPECT_EQ(rows, 888U);
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(RelationalRegressionTest,
       FlatMembershipColumnComparisonsKeepNullsAndDuplicates) {
  RelationalRun(*rs_, "CREATE TABLE MembershipProbe(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE MembershipFlat(k INT64,a INT64,b INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE MembershipNullable(k INT64,a INT64,b INT64)");
  std::array<std::array<std::array<bool, 33>, 6>, 2> matches{};
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, probe,
                          ctx.GetTable("MembershipProbe"));
    for (int64_t k = 0; k < 33; ++k) {
      ASSERT_SUCCESS(
          // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is
          // unambiguous
          probe->Insert(ctx.txn_, Row({Value((k * 1800000) - 28800000)}))
              .GetStatus());
    }
    for (size_t nullable = 0; nullable < 2; ++nullable) {
      ASSIGN_OR_ASSERT_FAIL(
          std::shared_ptr<Table>, build,
          ctx.GetTable(nullable ? "MembershipNullable" : "MembershipFlat"));
      for (int64_t k = 0; k < 33; ++k) {
        for (int64_t repeat = 0; repeat < 1024; ++repeat) {
          const int64_t a = k % 5 == 0 && repeat == 1023 ? 1 : (k % 3) - 1;
          const bool is_null = (nullable != 0U) && k % 7 == 0;
          ASSERT_SUCCESS(
              build
                  ->Insert(
                      ctx.txn_,
                      // NOLINTNEXTLINE(readability-math-missing-parentheses)
                      // precedence is unambiguous
                      Row({Value((k * 1800000) - 28800000),
                           is_null ? Value() : Value(a), Value(0)}))
                  .GetStatus());
          if (!is_null) {
            const std::array<bool, 6> passed = {a == 0, a != 0, a < 0,
                                                a <= 0, a > 0,  a >= 0};
            for (size_t op = 0; op < 6; ++op) {
              matches[nullable][op][k] |= passed[op];
            }
          }
        }
      }
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  for (const auto* name :
       {"MembershipProbe", "MembershipFlat", "MembershipNullable"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + name);
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::array<std::string, 6> operators = {"=",  "!=", "<",
                                                "<=", ">",  ">="};
  for (size_t workers : {size_t{1}, size_t{3}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (size_t nullable = 0; nullable < 2; ++nullable) {
      for (size_t op = 0; op < operators.size(); ++op) {
        int64_t expected = 0;
        for (bool found : matches[nullable][op]) {
          expected += static_cast<int64_t>(found);
        }
        const std::string table =
            (nullable != 0U) ? "MembershipNullable" : "MembershipFlat";
        const std::string body = "EXISTS (SELECT 1 FROM " + table +
                                 " b WHERE b.k=p.k AND b.a " + operators[op] +
                                 " b.b)";
        EXPECT_EQ(
            RelationalRun(
                *rs_, "SELECT COUNT(*) FROM MembershipProbe p WHERE " + body),
            std::vector<Row>{Row({Value(expected)})});
        EXPECT_EQ(
            RelationalRun(
                *rs_,
                "SELECT COUNT(*) FROM MembershipProbe p WHERE NOT " + body),
            std::vector<Row>{Row({Value(33 - expected)})});
      }
    }
  }
}

TEST_F(RelationalRegressionTest,
       CompositeColumnarGroupedJoinKeepsNullsBiasAndMultiplicity) {
  using namespace relational_detail;
  const Schema left_schema("l",
                           {Column(ColumnName("l", "a"), ValueType::kInt64),
                            Column(ColumnName("l", "b"), ValueType::kInt64),
                            Column(ColumnName("l", "c"), ValueType::kInt64),
                            Column(ColumnName("l", "g"), ValueType::kInt64),
                            Column(ColumnName("l", "v"), ValueType::kDouble)});
  const Schema right_schema(
      "r", {Column(ColumnName("r", "a"), ValueType::kInt64),
            Column(ColumnName("r", "b"), ValueType::kInt64),
            Column(ColumnName("r", "c"), ValueType::kInt64),
            Column(ColumnName("r", "g"), ValueType::kInt64),
            Column(ColumnName("r", "unused"), ValueType::kVarChar)});
  for (size_t key_count : {size_t{2}, size_t{3}}) {
    for (bool duplicate : {false, true}) {
      std::vector<Row> left_rows, right_rows;
      constexpr int64_t base = int64_t{1} << 40;
      for (int64_t i = 0; i < 17; ++i) {
        Row row({i == 8 ? Value() : Value(i), Value(i % 3),
                 i == 9 ? Value() : Value(base + i), Value(i % 2),
                 Value(std::string(1024, 'p'))});
        right_rows.push_back(row);
        if (duplicate && i == 3) {
          right_rows.push_back(row);
        }
      }
      for (int64_t i = 0; i < 41; ++i) {
        const int64_t key = i % 20;
        left_rows.emplace_back(std::vector<Value>{
            Value(key), i % 11 == 0 ? Value() : Value(key % 3),
            i == 12 ? Value() : Value(base + key + 1 + (i % 7 == 0 ? 100 : 0)),
            Value(i % 2), Value(static_cast<double>(i - 8))});
      }
      auto ctx = rs_->BeginContext();
      for (int layout : {0, 1, 2}) {
        Relation left, right;
        left.schema = left_schema;
        right.schema = right_schema;
        if (layout != 0) {
          const auto columns = [](const Schema& schema,
                                  const std::vector<Row>& rows) {
            auto table = std::make_shared<ColumnarTable>();
            for (size_t begin = 0; begin < rows.size(); begin += 7) {
              ColumnarChunk chunk;
              chunk.InitFromSchema(schema, 7);
              for (size_t i = begin; i < std::min(begin + 7, rows.size());
                   ++i) {
                chunk.AppendRow(rows[i]);
              }
              table->AppendChunk(std::move(chunk));
            }
            return table;
          };
          left.SetColumnarTable(columns(left_schema, left_rows), true);
          right.SetColumnarTable(columns(right_schema, right_rows), true);
          if (layout == 2) {
            for (Relation* relation : {&left, &right}) {
              for (int hop = 0; hop < 2; ++hop) {
                auto indices = std::make_shared<ChunkGatherIndex>();
                indices->source_table = relation->columnar;
                for (size_t c = relation->columnar->chunks.size(); c-- > 0;) {
                  for (size_t r = relation->columnar->chunks[c].rows;
                       r-- > 0;) {
                    indices->src_chunks.push_back(c);
                    indices->src_rows.push_back(r);
                  }
                }
                ColumnarChunk gathered;
                gathered.rows = indices->src_rows.size();
                gathered.columns.resize(
                    relation->columnar->chunks[0].columns.size());
                for (size_t c = 0; c < gathered.columns.size(); ++c) {
                  const auto& source = relation->columnar->chunks[0].columns[c];
                  gathered.columns[c].SetGathered(
                      indices, c, source.type, source.is_unsigned,
                      source.is_interval, true, true);
                }
                auto table = std::make_shared<ColumnarTable>();
                table->AppendChunk(std::move(gathered));
                relation->SetColumnarTable(std::move(table), true);
              }
            }
          }
        } else {
          left.rows = left_rows;
          right.rows = right_rows;
        }
        const auto col = [](const char* table, const char* name) {
          return ColumnValueExp(ColumnName(table, name));
        };
        std::vector<Expression> predicates = {
            BinaryExpressionExp(col("l", "a"), BinaryOperation::kEquals,
                                col("r", "a")),
            BinaryExpressionExp(col("l", "b"), BinaryOperation::kEquals,
                                col("r", "b")),
            BinaryExpressionExp(
                col("l", "c"), BinaryOperation::kEquals,
                BinaryExpressionExp(col("r", "c"), BinaryOperation::kAdd,
                                    ConstantValueExp(Value(1))))};
        predicates.resize(key_count);
        for (bool right_group : {false, true}) {
          auto key = col(right_group ? "r" : "l", "g");
          auto sum = std::make_shared<AggregateExpression>(
              AggregationType::kSum, col("l", "v"));
          for (JoinType kind : {JoinType::kInner, JoinType::kLeft}) {
            GroupedJoinFusionSpec fusion;
            fusion.eligible = true;
            fusion.group_keys = {key};
            fusion.select_items = {key, sum};
            fusion.select_names = {"g", "s"};
            fusion.aggregates = {sum.get()};
            fusion.count_star = {false};
            ASSIGN_OR_ASSERT_FAIL(
                std::optional<Relation>, result,
                TryFusedGroupedJoin(ctx, left, right, predicates, kind, nullptr,
                                    {}, &fusion));
            ASSERT_TRUE(result.has_value());
            std::map<int64_t, double> expected, actual;
            for (const Row& l : left_rows) {
              bool matched = false;
              for (const Row& r : right_rows) {
                if (l[0].IsNull() || l[1].IsNull() || r[0].IsNull() ||
                    r[1].IsNull() || l[0] != r[0] || l[1] != r[1] ||
                    (key_count == 3 &&
                     (l[2].IsNull() || r[2].IsNull() ||
                      l[2].value.int_value != r[2].value.int_value + 1))) {
                  continue;
                }
                matched = true;
                expected[(right_group ? r[3] : l[3]).value.int_value] +=
                    l[4].value.double_value;
              }
              if (!matched && kind == JoinType::kLeft) {
                expected[right_group ? -1 : l[3].value.int_value] +=
                    l[4].value.double_value;
              }
            }
            ASSERT_SUCCESS(result->ForEachRow([&](const Row& row) {
              EXPECT_TRUE(
                  actual
                      .emplace(row[0].IsNull() ? -1 : row[0].value.int_value,
                               row[1].value.double_value)
                      .second);
            }));
            EXPECT_EQ(actual, expected);
          }
        }
      }
      ASSERT_SUCCESS(ctx.PreCommit());
    }
  }
}

TEST_F(RelationalRegressionTest, GroupedJoinUsesProjectedFactStore) {
  RelationalRun(*rs_,
                "CREATE TABLE ProjectedFact(k INT64,x INT64,unused STRING)");
  RelationalRun(
      *rs_,
      "CREATE TABLE ProjectedDim(k INT64,v INT64,flag INT64,unused STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE ProjectedOther(k INT64,v INT64,unused STRING)");
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          ctx.GetTable("ProjectedFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          ctx.GetTable("ProjectedDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, other,
                          ctx.GetTable("ProjectedOther"));
    for (int64_t k = 0; k < 31; ++k) {
      ASSERT_SUCCESS(
          dim->Insert(
                 ctx.txn_,
                 Row({Value(k), Value(k + 10), Value(k % 2),
                      k % 3 == 0 ? Value() : Value(std::string(128, 'a'))}))
              .GetStatus());
      ASSERT_SUCCESS(
          other
              ->Insert(
                  ctx.txn_,
                  Row({Value(k), Value(k + 20),
                       k % 5 == 0 ? Value() : Value(std::string(128, 'b'))}))
              .GetStatus());
    }
    for (int64_t i = 0; i < 33000; ++i) {
      ASSERT_SUCCESS(
          fact->Insert(
                  ctx.txn_,
                  Row({i % 37 == 0 ? Value() : Value(i % 31),
                       // NOLINTNEXTLINE(readability-math-missing-parentheses)
                       // precedence is unambiguous
                       i % 19 == 0 ? Value() : Value((i % 7) - 2),
                       Value(std::string(64, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  const std::string sql =
      "SELECT f.k,d.v,e.v,SUM(f.x) FROM ProjectedDim d, "
      "ProjectedOther e,ProjectedFact f WHERE d.k=e.k AND e.k=f.k AND "
      "d.flag=1 AND f.x>0 GROUP BY f.k,d.v,e.v ORDER BY f.k";
  const auto expected = RelationalRun(*rs_, sql);
  ASSERT_FALSE(expected.empty());
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (size_t workers : {size_t{1}, size_t{3}}) {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          ctx.GetTable("ProjectedFact"));
    fact->SetLSTableStore(nullptr);
    fact->SetProjectedLSTableStore(nullptr);
    fact->RejectLSTableCapacity(fact->LayoutGeneration());
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql), expected);
    EXPECT_EQ(fact->GetLSTableStore(), nullptr);
    ASSERT_NE(fact->GetProjectedLSTableStore(), nullptr);
    EXPECT_FALSE(fact->GetProjectedLSTableStore()->HasColumn(2));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, ColumnarCorrelatedFilterRetainsPayloads) {
  using namespace relational_detail;
  auto ctx = rs_->BeginContext();
  ExecutionRuntime runtime;
  ctx.set_execution_runtime(&runtime);
  auto inner = std::make_shared<Relation>();
  inner->schema = Schema(
      "d", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
            Column("unused", ValueType::kVarChar)});
  auto inner_columns = std::make_shared<ColumnarTable>();
  ColumnarChunk inner_chunk;
  inner_chunk.InitFromSchema(inner->schema, 8);
  for (int i = 1; i <= 4; ++i) {
    for (int duplicate = 0; duplicate < 2; ++duplicate) {
      inner_chunk.AppendRow(
          Row({Value(i), i == 3 ? Value() : Value(i + duplicate),
               Value(std::string(1024, 'x'))}));
    }
  }
  inner_columns->AppendChunk(std::move(inner_chunk));
  inner->SetColumnarTable(std::move(inner_columns), false);
  auto subquery = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(
          "m", AggregateExpressionExp(AggregationType::kMin,
                                      ColumnValueExp(ColumnName("d", "v"))))},
      std::vector<std::string>{"d"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("d", "k")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("o", "k"))));
  SelectStatement statement(
      {NamedExpression("k", ColumnValueExp(ColumnName("o", "k"))),
       NamedExpression("p", ColumnValueExp(ColumnName("o", "payload")))},
      {},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("o", "k")),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(2))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("o", "k")),
                              BinaryOperation::kEquals,
                              QueryExpressionExp(subquery))));
  Relation input(&runtime);
  input.schema =
      Schema("o", {Column(ColumnName("o", "k"), ValueType::kInt64),
                   Column(ColumnName("o", "payload"), ValueType::kVarChar)});
  auto columns = std::make_shared<ColumnarTable>();
  std::vector<Row> expected;
  expected.reserve(12);
  for (int i = 0; i < 6; ++i) {
    ColumnarChunk chunk;
    chunk.InitFromSchema(input.schema, 2);
    // Separate chunks cover all-pass, all-rejected, NULL, and partial chunks.
    for (int j = 0; j < 2; ++j) {
      const int key = (i * 2 + j) % 5;
      Row row({key == 0 ? Value() : Value(key),
               Value(std::string(256, static_cast<char>('a' + (i * 2) + j)))});
      chunk.AppendRow(row);
      if (key == 2 || key == 4) {
        expected.push_back(row);
      }
    }
    columns->AppendChunk(std::move(chunk));
  }
  input.SetColumnarTable(std::move(columns), false);
  ASSIGN_OR_ASSERT_FAIL(Relation, output,
                        FinishQuery(ctx, statement, std::move(input), nullptr,
                                    {{"d", inner}}, true));
  inner.reset();
  std::vector<Row> actual;
  ASSERT_SUCCESS(
      output.ForEachRow([&](const Row& row) { actual.push_back(row); }));
  EXPECT_EQ(actual, expected);
  ctx.set_execution_runtime(nullptr);
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.Abort());
}

TEST_F(RelationalRegressionTest, CorrelationDomainReadsGatheredKeyOnly) {
  using namespace relational_detail;
  for (ValueType type : {ValueType::kInt64, ValueType::kDate}) {
    const auto key = [&](int64_t v) {
      return type == ValueType::kDate ? Value::DateFromDays(v) : Value(v);
    };
    const Schema schema(
        "o", {Column(ColumnName("o", "k"), type),
              Column(ColumnName("o", "payload"), ValueType::kVarChar)});
    const std::vector<Row> rows{Row({key(2), Value(std::string(1024, 'a'))}),
                                Row({Value(), Value("null key")}),
                                Row({key(-3), Value("negative key")}),
                                Row({Value(1.5), Value("foreign key type")})};
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<ColumnarTable>, base,
                          BuildColumnarTableFromRows(schema, rows));
    auto indices = std::make_shared<ChunkGatherIndex>();
    indices->source_table = base;
    indices->src_chunks.assign(6, 0);
    indices->src_rows = {0, 2, ChunkGatherIndex::kNullRow, 1, 0, 3};
    ColumnarChunk chunk;
    chunk.rows = indices->src_rows.size();
    chunk.columns.resize(2);
    chunk.columns[0].SetGathered(indices, 0, type, false, false, false, true);
    chunk.columns[1].SetGathered(indices, 1, ValueType::kVarChar, false, false,
                                 true, true);
    auto gathered = std::make_shared<ColumnarTable>();
    gathered->AppendChunk(std::move(chunk));
    Relation domain;
    domain.schema = schema;
    domain.SetColumnarTable(gathered, false);
    ExecutionRuntime runtime;
    runtime.correlated_outer_relation = &domain;
    const Schema source("s",
                        {Column("v", ValueType::kVarChar), Column("k", type)});
    const auto filter = CorrelationDomainFilter(source, 1, ColumnName("o", "k"),
                                                {1, 0}, runtime);
    if (!filter.has_value()) {
      ADD_FAILURE() << "correlation filter was not built";
      return;
    }
    const auto& filter_value = filter.value();
    EXPECT_EQ(filter_value.column, 0);
    EXPECT_EQ(filter_value.type, type);
    for (int64_t k = -10; k <= 10; ++k) {
      EXPECT_EQ(!filter_value.group->Reject(k), k == -3 || k == 2);
    }
    // A new domain must not retain keys from the previous gathered image.
    domain.columnar.reset();
    domain.rows = {Row({key(7), Value("replacement")})};
    const auto replacement = CorrelationDomainFilter(
        source, 1, ColumnName("o", "k"), {1, 0}, runtime);
    if (!replacement.has_value()) {
      ADD_FAILURE() << "replacement correlation filter was not built";
      return;
    }
    const auto& replacement_value = replacement.value();
    EXPECT_FALSE(replacement_value.group->Reject(7));
    EXPECT_TRUE(replacement_value.group->Reject(2));
  }
}

TEST_F(RelationalRegressionTest,
       ScanReloadsAggregateCacheMarkerBelowBackingThreshold) {
  RelationalRun(*rs_, "CREATE TABLE MarkerProbe(k INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO MarkerProbe VALUES (1,10),(2,20),(3,NULL)");
  for (bool columnar : {false, true}) {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("MarkerProbe"));
    ASSERT_LE(table->BackingColumnarRowCount(), 3U);
    relational_detail::ExecutionRuntime runtime;
    ctx.set_execution_runtime(&runtime);
    runtime.reusable_base_relations.insert("MarkerProbe");
    auto marker = std::make_shared<relational_detail::Relation>(&runtime);
    marker->schema = table->GetSchema();
    runtime.base_relations.emplace(
        relational_detail::BaseRelationCacheKey("MarkerProbe", nullptr),
        marker);
    SelectSource source;
    source.table = "MarkerProbe";
    const std::vector<Expression> predicates{
        BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kGreaterThan,
                            ConstantValueExp(Value(1)))};
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::LoadSource(ctx, source, nullptr, {}, nullptr,
                                      &predicates, nullptr, nullptr, columnar));
    std::map<int64_t, Row> actual;
    ASSERT_SUCCESS(result.ForEachRow(
        [&](const Row& row) { actual.emplace(row[0].value.int_value, row); }));
    EXPECT_EQ(actual, (std::map<int64_t, Row>{{2, Row({Value(2), Value(20)})},
                                              {3, Row({Value(3), Value()})}}));
    ctx.set_execution_runtime(nullptr);
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, ColumnarSetOperationsKeepEmptyRightSide) {
  RelationalRun(*rs_, "CREATE TABLE EmptySetProbe(k INT64)");
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("EmptySetProbe"));
    for (int64_t i = 0; i < 9000; ++i) {
      ASSERT_SUCCESS(
          table->Insert(ctx.txn_, Row({i % 7 == 0 ? Value() : Value(i % 3)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  for (const std::string op : {"UNION DISTINCT", "EXCEPT DISTINCT"}) {
    SCOPED_TRACE(op);
    const auto rows = RelationalRun(
        *rs_,
        "SELECT k FROM EmptySetProbe " + op +
            " SELECT k FROM EmptySetProbe WHERE k < 0 ORDER BY k NULLS FIRST");
    EXPECT_EQ(rows, std::vector<Row>({Row({Value()}), Row({Value(0)}),
                                      Row({Value(1)}), Row({Value(2)})}));
  }
}

TEST_F(RelationalRegressionTest,
       TypedUnsignedGroupsKeepTagsOnScanAndArenaReuse) {
  RelationalRun(*rs_, "CREATE TABLE UnsignedGroups (g UINT64, u UINT64)");
  RelationalRun(*rs_,
                "INSERT INTO UnsignedGroups VALUES "
                "(18446744073709551615,18446744073709551610),"
                "(18446744073709551615,5),(0,0),(NULL,NULL)");
  const std::string branch =
      "SELECT g,MIN(u) AS lo,MAX(u) AS hi FROM UnsignedGroups GROUP BY g";
  for (bool reuse : {false, true}) {
    SCOPED_TRACE(reuse);
    std::string sql = branch;
    if (reuse) {
      sql += " UNION ALL ";
      sql += branch;
    }
    sql += " ORDER BY g NULLS FIRST";
    const auto rows = RelationalRun(*rs_, sql);
    const size_t copies = reuse ? 2 : 1;
    ASSERT_EQ(rows.size(), 3 * copies);
    for (size_t i = 0; i < copies; ++i) {
      EXPECT_TRUE(rows[i][0].IsNull());
      EXPECT_TRUE(rows[i][1].IsNull());
      EXPECT_TRUE(rows[i][2].IsNull());
      const Row& zero = rows[copies + i];
      for (size_t c = 0; c < 3; ++c) {
        EXPECT_TRUE(zero[c].IsUnsigned());
        EXPECT_EQ(zero[c].AsString(), "0");
      }
      const Row& large = rows[(2 * copies) + i];
      for (size_t c = 0; c < 3; ++c) {
        EXPECT_TRUE(large[c].IsUnsigned());
      }
      EXPECT_EQ(large[0].AsString(), "18446744073709551615");
      EXPECT_EQ(large[1].AsString(), "5");
      EXPECT_EQ(large[2].AsString(), "18446744073709551610");
    }
  }
}

TEST_F(RelationalRegressionTest, ScanFilterCacheOwnsFrozenResidualBindings) {
  const Schema schema("cached_filter", {Column("k", ValueType::kInt64)});
  // A computed operand keeps this a residual after singleton IN lowering.
  const Expression key = FunctionCallExp("abs", {ColumnValueExp("k")});
  auto slot = std::make_shared<ParameterSlot>(0, Value(1));
  const Expression first = InExpressionExp(key, {slot});
  auto compiled = relational_detail::CompileScanFilter({first}, schema);
  ASSERT_EQ(compiled.residual.size(), 1U);
  slot->Bind(Value(9));
  const Expression same_values =
      InExpressionExp(key, {ConstantValueExp(Value(1))});
  auto reused = relational_detail::CompileScanFilter({same_values}, schema);
  ASSERT_EQ(reused.residual.size(), 1U);
  EXPECT_EQ(reused.residual[0]->Evaluate(Row({Value(1)}), schema), Value(true));
  EXPECT_EQ(reused.residual[0]->Evaluate(Row({Value(9)}), schema),
            Value(false));
  auto rebound = relational_detail::CompileScanFilter({first}, schema);
  ASSERT_EQ(rebound.residual.size(), 1U);
  EXPECT_EQ(rebound.residual[0]->Evaluate(Row({Value(9)}), schema),
            Value(true));
  EXPECT_EQ(compiled.residual[0]->Evaluate(Row({Value(1)}), schema),
            Value(true));
}

TEST_F(RelationalRegressionTest,
       FilteredScalarCountKeepsNullAndUnionSemantics) {
  RelationalRun(*rs_, "CREATE TABLE CountFilter (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO CountFilter VALUES "
                "(NULL,1),(0,1),(1,NULL),(2,1),(9,1),(10,1)");
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*) FROM CountFilter WHERE k < 2"),
            std::vector<Row>({Row({Value(2)})}));
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*), COUNT(*) FROM CountFilter WHERE k >= 9"),
      std::vector<Row>({Row({Value(2), Value(2)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*) FROM CountFilter WHERE k < 0"),
            std::vector<Row>({Row({Value(0)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT SUM(n) FROM (SELECT COUNT(*) AS n FROM CountFilter WHERE k<2 "
          "UNION ALL SELECT COUNT(*) AS n FROM CountFilter WHERE k>=9) u"),
      std::vector<Row>({Row({Value(4)})}));
}

TEST_F(RelationalRegressionTest, ScalarFloatingFiltersKeepAllComparisons) {
  RelationalRun(
      *rs_, "CREATE TABLE FloatSelection(k INT64,v DOUBLE,n DOUBLE NOT NULL)");
  std::vector<Row> rows;
  rows.reserve(32769);
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("FloatSelection"));
    for (int64_t i = 0; i < 32769; ++i) {
      double v = static_cast<double>((i % 11) - 5) * 0.5;
      if (i % 97 == 0) {
        v = std::numeric_limits<double>::quiet_NaN();
      } else if (i % 101 == 0) {
        v = std::numeric_limits<double>::infinity();
      } else if (i % 103 == 0) {
        v = -std::numeric_limits<double>::infinity();
      } else if (i % 107 == 0) {
        v = -0.0;
      }
      rows.emplace_back(std::vector<Value>{
          Value(i % 17), i % 13 == 0 ? Value() : Value(v), Value(v)});
      ASSERT_SUCCESS(table->Insert(ctx.txn_, rows.back()).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const std::string op : {"=", "<>", "<", "<=", ">", ">="}) {
    for (bool selected : {false, true}) {
      for (size_t column : {size_t{1}, size_t{2}}) {
        int64_t count = 0, sum = 0;
        for (const auto& row : rows) {
          const int64_t k = row[0].value.int_value;
          if (selected && (k < 3 || k > 9)) {
            continue;
          }
          if (row[column].IsNull()) {
            continue;
          }
          const double v = row[column].value.double_value;
          const bool hit = op == "="    ? v == 0.0
                           : op == "<>" ? v != 0.0
                           : op == "<"  ? v < 0.0
                           : op == "<=" ? v <= 0.0
                           : op == ">"  ? v > 0.0
                                        : v >= 0.0;
          if (hit) {
            ++count;
            sum += (k * k) + 1;
          }
        }
        const std::string sql =
            "SELECT SUM(k*k+1),COUNT(*) FROM FloatSelection WHERE " +
            std::string(selected ? "k BETWEEN 3 AND 9 AND " : "") +
            (column == 1 ? "v" : "n") + " " + op + " 0.0";
        EXPECT_EQ(RelationalRun(*rs_, sql),
                  std::vector<Row>{
                      Row({count ? Value(sum) : Value(), Value(count)})});
      }
    }
  }
  for (const auto [lo, hi] :
       std::vector<std::pair<double, double>>{{-1, 1}, {2, 1}, {-0.0, 0.0}}) {
    for (size_t column : {size_t{1}, size_t{2}}) {
      int64_t count = 0, sum = 0;
      double product = 0;
      for (const auto& row : rows) {
        const int64_t k = row[0].value.int_value;
        if (k < 3 || k > 9 || row[column].IsNull()) {
          continue;
        }
        const double value = row[column].value.double_value;
        if (value >= lo && value <= hi) {
          ++count;
          sum += (k * k) + 1;
          product += value * value;
        }
      }
      const std::string name = column == 1 ? "v" : "n";
      for (bool reversed : {false, true}) {
        const std::string lower = name + ">=" + std::to_string(lo);
        const std::string upper = name + "<=" + std::to_string(hi);
        std::string sql = "SELECT SUM(k*k+1),COUNT(*),SUM(";
        sql += name;
        sql += '*';
        sql += name;
        sql += ") FROM FloatSelection WHERE k BETWEEN 3 AND 9 AND ";
        if (reversed) {
          sql += upper;
          sql += " AND ";
          sql += lower;
        } else {
          sql += lower;
          sql += " AND ";
          sql += upper;
        }
        EXPECT_EQ(
            RelationalRun(*rs_, sql),
            std::vector<Row>{Row({count ? Value(sum) : Value(), Value(count),
                                  count ? Value(product) : Value()})});
      }
    }
  }
}

TEST_F(RelationalRegressionTest, ScalarRangeSelectionKeepsNullsAndInt64Bounds) {
  RelationalRun(
      *rs_, "CREATE TABLE RangeSelection(k INT64, j INT64 NOT NULL, v DOUBLE)");
  const std::vector<Value> keys = {
      Value(),   Value(std::numeric_limits<int64_t>::min()),
      Value(-2), Value(-1),
      Value(0),  Value(1),
      Value(2),  Value(std::numeric_limits<int64_t>::max())};
  std::vector<Row> input;
  input.reserve(18000);
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("RangeSelection"));
    for (int64_t i = 0; i < 18000; ++i) {
      Value v(static_cast<double>(i % 7));
      if (i % 11 == 0) {
        v = Value();
      } else if (i % 97 == 0) {
        v = Value(std::numeric_limits<double>::quiet_NaN());
      } else if (i % 101 == 0) {
        v = Value(std::numeric_limits<double>::infinity());
      } else if (i % 103 == 0) {
        v = Value(-std::numeric_limits<double>::infinity());
      }
      input.emplace_back(
          std::vector<Value>{keys[i % keys.size()], Value(i % 5), v});
      ASSERT_SUCCESS(table->Insert(ctx.txn_, input.back()).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const auto [lo, hi] : std::vector<std::pair<int64_t, int64_t>>{
           {std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::max()},
           {std::numeric_limits<int64_t>::min(), -1},
           {-1, 1},
           {2, std::numeric_limits<int64_t>::max()},
           {3, 4},
           {1, -1}}) {
    SCOPED_TRACE(std::to_string(lo) + ".." + std::to_string(hi));
    int64_t count = 0;
    double sum = 0;
    std::set<double> distinct;
    for (const Row& row : input) {
      if (row[0].IsNull() || row[0].value.int_value < lo ||
          row[0].value.int_value > hi || row[1].value.int_value < 1 ||
          row[1].value.int_value > 3 || row[2].IsNull()) {
        continue;
      }
      const double v = row[2].value.double_value;
      // SQL comparisons with NaN are false, so NaN fails both BETWEENs; the
      // negated form `(v < 0 || v > 1) && ...` would wrongly admit it.
      const bool in_range = (v >= 0 && v <= 1) || (v >= 4 && v <= 6);
      if (!in_range) {
        continue;
      }
      ++count;
      sum += v;
      distinct.insert(v);
    }
    const std::string branch =
        "SELECT AVG(v),COUNT(v),COUNT(DISTINCT v) FROM RangeSelection WHERE "
        "k BETWEEN " +
        std::to_string(lo) + " AND " + std::to_string(hi) +
        " AND j BETWEEN 1 AND 3 AND (v BETWEEN 0 AND 1 OR v BETWEEN 4 AND 6)";
    const Row expected({(count != 0) ? Value(sum / static_cast<double>(count)) : Value(),
                        Value(count),
                        Value(static_cast<int64_t>(distinct.size()))});
    EXPECT_EQ(RelationalRun(*rs_, branch), std::vector<Row>({expected}));
    std::string reversed = branch;
    const auto pos = reversed.find("v BETWEEN 0 AND 1");
    ASSERT_NE(pos, std::string::npos);
    reversed.replace(pos, std::string("v BETWEEN 0 AND 1").size(),
                     "(v <= 1 AND v >= 0)");
    std::string union_sql = branch;
    union_sql += " UNION ALL ";
    union_sql += reversed;
    EXPECT_EQ(RelationalRun(*rs_, union_sql),
              std::vector<Row>({expected, expected}));
  }
}

TEST_F(RelationalRegressionTest,
       SecondaryRuntimeFilterKeepsMonotonicProbeRows) {
  RelationalRun(*rs_,
                "CREATE TABLE SecondaryFilter(id INT64 NOT NULL,k INT64)");
  std::vector<Row> input;
  input.reserve(18000);
  RuntimeFilterBuilder primary_builder, sparse_builder;
  std::vector<std::unique_ptr<RuntimeFilter>> secondary_parts;
  secondary_parts.push_back(std::make_unique<ExactSetRuntimeFilter>(
      std::vector<int64_t>{-2, 0, 4, 9}));
  secondary_parts.push_back(std::make_unique<MinMaxRuntimeFilter>(-2, 4, 3));
  for (int64_t i = 1000; i < 17000; ++i) {
    if (i % 17 != 0) {
      primary_builder.Observe(i);
    }
    if (i % 257 == 0) {
      sparse_builder.Observe(i);
    }
  }
  const auto primary = primary_builder.Build();
  const auto sparse = sparse_builder.Build();
  const auto secondary =
      std::make_shared<RuntimeFilterGroup>(std::move(secondary_parts));
  ASSERT_TRUE(primary->has_dense_bits());
  ASSERT_TRUE(secondary->has_dense_bits());
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("SecondaryFilter"));
    for (int64_t i = 0; i < 18000; ++i) {
      input.emplace_back(std::vector<Value>{
          Value(i), i % 7 == 0 ? Value() : Value((i % 101) - 50)});
      ASSERT_SUCCESS(table->Insert(ctx.txn_, input.back()).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (size_t workers : {size_t{1}, size_t{3}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("SecondaryFilter"));
    const auto store = GetOrBuildLSTableStore(ctx.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_TRUE(store->column_is_monotonic[0]);
    ASSERT_TRUE(store->blocks[0].columns[1].HasRawIntCells());
    const auto& schema = table->GetSchema();
    relational_detail::CompiledScanFilter predicates;
    relational_detail::PrebuiltMembershipPredicate member;
    member.column = 1;
    member.domain_type = ValueType::kInt64;
    member.runtime_filter = secondary;
    predicates.prebuilt_membership.push_back(member);
    for (int primary_mode : {0, 1, 2}) {
      const auto* chosen = primary_mode == 0   ? nullptr
                           : primary_mode == 1 ? primary.get()
                                               : sparse.get();
      const bool with_primary = chosen != nullptr;
      for (bool columnar : {false, true}) {
        SCOPED_TRACE(std::to_string(workers) + "/" +
                     std::to_string(primary_mode) + "/" +
                     std::to_string(columnar));
        relational_detail::Relation result;
        result.schema = schema;
        Status error = Status::kSuccess;
        ASSERT_TRUE(relational_detail::TryParallelTableScan(
            ctx, *table, nullptr, chosen, ValueType::kInt64,
            with_primary ? std::optional<slot_t>{0} : std::nullopt, true,
            &predicates, schema, nullptr, {}, &result, &error, nullptr,
            columnar));
        ASSERT_SUCCESS(error);
        std::map<int64_t, Row> expected, actual;
        for (const Row& row : input) {
          const int64_t id = row[0].value.int_value;
          if (row[1].IsNull()) {
            continue;
          }
          const int64_t k = row[1].value.int_value;
          if (k != -2 && k != 0 && k != 4) {
            continue;
          }
          if (with_primary && (id < 1000 || id >= 17000)) {
            continue;
          }
          if (primary_mode == 1 && id % 17 == 0) {
            continue;
          }
          if (primary_mode == 2 && id % 257 != 0) {
            continue;
          }
          expected.emplace(id, row);
        }
        ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
          EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
        }));
        ASSERT_FALSE(expected.empty());
        EXPECT_EQ(actual, expected);
      }
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, IndexedRuntimeFilterKeepsRunsAndResiduals) {
  using namespace relational_detail;
  size_t case_id = 0;
  for (int64_t base : {std::numeric_limits<int64_t>::min(), int64_t{-5000},
                       std::numeric_limits<int64_t>::max() - 10000}) {
    const std::string name = "IndexedProbe" + std::to_string(case_id++);
    RelationalRun(*rs_, "CREATE TABLE " + name +
                            "(k INT64,v INT64,id INT64,d INT64 NOT NULL)");
    std::vector<Row> rows;
    rows.reserve(9000);
    RuntimeFilterBuilder builder;
    for (int64_t group = 0; group < 1300; ++group) {
      if (group % 31 == 0) {
        builder.Observe(base + (group * 4));
      }
      if (group % 29 == 0) {
        builder.Observe(base + (group * 4) + 1);  // absent keys
      }
    }
    const auto filter = builder.Build();
    {
      auto ctx = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable(name));
      for (int64_t i = 0; i < 9000; ++i) {
        rows.emplace_back(std::vector<Value>{
            Value(base + ((i / 7) * 4)), i % 13 == 0 ? Value() : Value(i % 5),
            Value(i), Value((i / 70) % 13)});
        ASSERT_SUCCESS(table->Insert(ctx.txn_, rows.back()).GetStatus());
      }
      ASSERT_SUCCESS(ctx.PreCommit());
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable(name));
    const auto store = GetOrBuildLSTableStore(ctx.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    const auto schema = table->GetSchema();
    auto predicates = CompileScanFilter(
        {BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kGreaterThan,
                             ConstantValueExp(Value(2))),
         BinaryExpressionExp(ColumnValueExp("d"),
                             BinaryOperation::kGreaterThanEquals,
                             ConstantValueExp(Value(3))),
         BinaryExpressionExp(ColumnValueExp("d"),
                             BinaryOperation::kLessThanEquals,
                             ConstantValueExp(Value(7)))},
        schema);
    RuntimeFilterBuilder secondary_builder;
    secondary_builder.Observe(3);
    PrebuiltMembershipPredicate secondary;
    secondary.column = 1;
    secondary.domain_type = ValueType::kInt64;
    secondary.runtime_filter = secondary_builder.Build();
    ASSERT_TRUE(secondary.runtime_filter->has_dense_bits());
    predicates.prebuilt_membership.push_back(secondary);
    for (bool allow_index : {false, true}) {
      // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type
      // is non-copyable by design
      struct RestoreLimit {  // NOLINT(cppcoreguidelines-special-member-functions): local scope guard.
        size_t saved = ProcessMemoryBudget::Global().ColumnarPoolLimit();
        ~RestoreLimit() {
          ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(saved);
        }
      } restore;
      if (!allow_index) {
        ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(1);
      }
      for (size_t workers : {size_t{1}, size_t{3}}) {
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        for (bool columnar : {false, true}) {
          for (bool reverse_filters : {false, true}) {
            auto scan_predicates = predicates;
            if (reverse_filters) {
              scan_predicates.prebuilt_membership.back().column = 0;
              scan_predicates.prebuilt_membership.back().runtime_filter =
                  filter;
            }
            Relation result;
            result.schema = schema;
            Status error = Status::kSuccess;
            ASSERT_TRUE(TryParallelTableScan(
                ctx, *table, nullptr,
                reverse_filters ? secondary.runtime_filter.get() : filter.get(),
                ValueType::kInt64, reverse_filters ? slot_t{1} : slot_t{0},
                true, &scan_predicates, schema, nullptr, {}, &result, &error,
                nullptr, columnar));
            ASSERT_SUCCESS(error);
            std::map<int64_t, Row> expected, actual;
            for (const auto& row : rows) {
              const int64_t id = row[2].value.int_value;
              if ((id / 7) % 31 == 0 && !row[1].IsNull() &&
                  row[1].value.int_value == 3 && row[3].value.int_value >= 3 &&
                  row[3].value.int_value <= 7) {
                expected.emplace(id, row);
              }
            }
            ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
              EXPECT_TRUE(actual.emplace(row[2].value.int_value, row).second);
            }));
            EXPECT_EQ(actual, expected);
          }
        }
      }
      EXPECT_EQ(store->IntegerRuns(0) != nullptr, allow_index);
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, RangePrefilterKeepsResidualsNullsAndBounds) {
  RelationalRun(*rs_,
                "CREATE TABLE RangeProbe(k INT64,j INT64,tag STRING,id INT64)");
  std::vector<Row> input;
  input.reserve(9000);
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("RangeProbe"));
    for (int64_t i = 0; i < 9000; ++i) {
      Value key(((i * 37) % 2000) - 1000);
      if (i == 111) {
        key = Value(std::numeric_limits<int64_t>::min());
      } else if (i == 112) {
        key = Value(std::numeric_limits<int64_t>::max());
      } else if (i % 23 == 0) {
        key = Value();
      }
      input.emplace_back(std::vector<Value>{
          key,
          i == 112 ? Value(std::numeric_limits<int64_t>::max())
                   : (i % 19 == 0 ? Value() : Value(i % 17)),
          Value(i % 3 == 0 || i == 112 ? "A" : "B"), Value(i)});
      ASSERT_SUCCESS(table->Insert(ctx.txn_, input.back()).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const auto [lo, hi] : std::vector<std::pair<int64_t, int64_t>>{
           {-10, 10},
           {std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::min() + 1},
           {std::numeric_limits<int64_t>::max() - 1,
            std::numeric_limits<int64_t>::max()},
           {10, -10}}) {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("RangeProbe"));
    const auto schema = table->GetSchema();
    const auto predicates = relational_detail::CompileScanFilter(
        {BinaryExpressionExp(ColumnValueExp("k"),
                             BinaryOperation::kGreaterThanEquals,
                             ConstantValueExp(Value(lo))),
         BinaryExpressionExp(ColumnValueExp("k"),
                             BinaryOperation::kLessThanEquals,
                             ConstantValueExp(Value(hi))),
         BinaryExpressionExp(ColumnValueExp("k"),
                             BinaryOperation::kLessThanEquals,
                             ColumnValueExp("j")),
         BinaryExpressionExp(ColumnValueExp("tag"), BinaryOperation::kEquals,
                             ConstantValueExp(Value("A")))},
        schema);
    std::map<int64_t, Row> expected;
    for (const Row& row : input) {
      if (row[0].IsNull() || row[1].IsNull()) {
        continue;
      }
      const int64_t k = row[0].value.int_value;
      if (k >= lo && k <= hi && k <= row[1].value.int_value &&
          row[2] == Value("A")) {
        expected.emplace(row[3].value.int_value, row);
      }
    }
    for (size_t workers : {size_t{1}, size_t{3}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      for (bool columnar : {false, true}) {
        relational_detail::Relation result;
        result.schema = schema;
        Status error = Status::kSuccess;
        ASSERT_TRUE(relational_detail::TryParallelTableScan(
            ctx, *table, nullptr, nullptr, ValueType::kNull, std::nullopt, true,
            &predicates, schema, nullptr, {}, &result, &error, nullptr,
            columnar));
        ASSERT_SUCCESS(error);
        std::map<int64_t, Row> actual;
        ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
          EXPECT_TRUE(actual.emplace(row[3].value.int_value, row).second);
        }));
        EXPECT_EQ(actual, expected);
      }
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

}  // namespace
}  // namespace tinylamb
