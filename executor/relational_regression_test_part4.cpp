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

#include "executor/relational_regression_test.hpp"

namespace tinylamb {
namespace {
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
      ASSERT_TRUE(result.has_value());
      ASSERT_TRUE(scalar.has_value());
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
      const Value& actual = *scalar;
      AggregateAccumulator reference(aggregate.get(), &input->schema);
      for (const auto& value : groups[k]) { ASSERT_SUCCESS(reference.Add(value));
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
  ctx.Abort();
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
          fact->Insert(setup.txn_, Row({Value(1), Value(3)}))
              .GetStatus());
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
      // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is non-copyable by design
      struct RestoreColumnarLimit {
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
        if (i % 17 == 0 || i == 42 || (direct_scan && i % 13 == 0)) { continue;
}
        EXPECT_EQ(
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is unambiguous
            actual.at((i + probe_begin) * 4 + key_bias),
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is unambiguous
            Row({Value((i + probe_begin) * 4 + key_bias),
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
        // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is unambiguous
        const int64_t i = (row[0].value.int_value - key_bias) / 4 - probe_begin;
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
          // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is unambiguous
          probe->Insert(ctx.txn_, Row({Value(k * 1800000 - 28800000)}))
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
                  ->Insert(ctx.txn_,
                           // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is unambiguous
                           Row({Value(k * 1800000 - 28800000),
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
        for (bool found : matches[nullable][op]) { expected += static_cast<int64_t>(found);
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
        if (duplicate && i == 3) { right_rows.push_back(row);
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
              for (size_t i = begin; i < std::min(begin + 7, rows.size()); ++i) {
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
          fact->Insert(ctx.txn_, Row({i % 37 == 0 ? Value() : Value(i % 31),
                                      // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence is unambiguous
                                      i % 19 == 0 ? Value() : Value(i % 7 - 2),
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
      if (key == 2 || key == 4) { expected.push_back(row);
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
  ctx.Abort();
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
    ASSERT_TRUE(filter.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
    EXPECT_EQ(filter->column, 0);
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
    EXPECT_EQ(filter->type, type);
    for (int64_t k = -10; k <= 10; ++k) {
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
      EXPECT_EQ(!filter->group->Reject(k), k == -3 || k == 2);
    }
    // A new domain must not retain keys from the previous gathered image.
    domain.columnar.reset();
    domain.rows = {Row({key(7), Value("replacement")})};
    const auto replacement = CorrelationDomainFilter(
        source, 1, ColumnName("o", "k"), {1, 0}, runtime);
    ASSERT_TRUE(replacement.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
    EXPECT_FALSE(replacement->group->Reject(7));
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
    EXPECT_TRUE(replacement->group->Reject(2));
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
          if (selected && (k < 3 || k > 9)) { continue;
}
          if (row[column].IsNull()) { continue;
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
        if (k < 3 || k > 9 || row[column].IsNull()) { continue;
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
        const std::string sql =
            // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
            "SELECT SUM(k*k+1),COUNT(*),SUM(" + name + "*" + name +
            ") FROM FloatSelection WHERE k BETWEEN 3 "
            "AND 9 AND " +
            // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
            (reversed ? upper + " AND " + lower : lower + " AND " + upper);
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
      if (!((v >= 0 && v <= 1) || (v >= 4 && v <= 6))) {  // NOLINT(readability-simplify-boolean-expr): NaN must fail both ranges
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
    const Row expected({(count != 0) ? Value(sum / count) : Value(), Value(count),
                        Value(static_cast<int64_t>(distinct.size()))});
    EXPECT_EQ(RelationalRun(*rs_, branch), std::vector<Row>({expected}));
    std::string reversed = branch;
    const auto pos = reversed.find("v BETWEEN 0 AND 1");
    ASSERT_NE(pos, std::string::npos);
    reversed.replace(pos, std::string("v BETWEEN 0 AND 1").size(),
                     "(v <= 1 AND v >= 0)");
    // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
    EXPECT_EQ(RelationalRun(*rs_, branch + " UNION ALL " + reversed),
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
    if (i % 17 != 0) { primary_builder.Observe(i);
}
    if (i % 257 == 0) { sparse_builder.Observe(i);
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
          if (row[1].IsNull()) { continue;
}
          const int64_t k = row[1].value.int_value;
          if (k != -2 && k != 0 && k != 4) { continue;
}
          if (with_primary && (id < 1000 || id >= 17000)) { continue;
}
          if (primary_mode == 1 && id % 17 == 0) { continue;
}
          if (primary_mode == 2 && id % 257 != 0) { continue;
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
      if (group % 31 == 0) { builder.Observe(base + (group * 4));
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
      // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is non-copyable by design
      struct RestoreLimit {
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
      if (row[0].IsNull() || row[1].IsNull()) { continue;
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

TEST_F(RelationalRegressionTest, FusedJoinKeepsMixedNumericCaseBranches) {
  RelationalRun(*rs_, "CREATE TABLE CaseDim (k INT64, label STRING)");
  RelationalRun(*rs_, "CREATE TABLE CaseFact (k INT64, v FLOAT64)");
  RelationalRun(
      *rs_, "INSERT INTO CaseDim VALUES (1,'PROMO X'),(2,'OTHER'),(3,NULL)");
  RelationalRun(*rs_,
                "INSERT INTO CaseFact VALUES (1,2.5),(1,3.5),(2,7.5),(3,10.5)");
  const std::string sql =
      "SELECT SUM(CASE WHEN d.label LIKE 'PROMO%' THEN f.v ELSE 0 END), "
      "SUM(f.v) "
      "FROM CaseFact f JOIN CaseDim d ON f.k=d.k";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(6.0), Value(24.0)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
}

TEST_F(RelationalRegressionTest, WideFusedAggregatesKeepNullsAndDuplicateKeys) {
  RelationalRun(*rs_, "CREATE TABLE WideDim (k INT64, g STRING)");
  RelationalRun(*rs_, "CREATE TABLE WideFact (k INT64, v FLOAT64)");
  RelationalRun(*rs_, "INSERT INTO WideDim VALUES (1,'a'),(1,'a'),(2,'b')");
  RelationalRun(*rs_,
                "INSERT INTO WideFact VALUES (1,2.5),(1,3.5),(1,NULL),"
                "(2,NULL),(3,99)");
  const std::string sql =
      "SELECT d.g, COUNT(*), COUNT(f.v), SUM(f.v), AVG(f.v), "
      "SUM(CASE WHEN f.v > 3 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.v <= 3 THEN 1 ELSE 0 END), "
      "COUNT(DISTINCT f.k), SUM(f.v * 2) "
      "FROM WideFact f JOIN WideDim d ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value("a"), Value(6), Value(4), Value(12.0), Value(3.0),
                      Value(2), Value(2), Value(1), Value(24.0)}),
                 Row({Value("b"), Value(1), Value(0), Value(), Value(),
                      Value(0), Value(0), Value(1), Value()})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
}

TEST_F(RelationalRegressionTest, FusedDirectEmissionKeepsRawCellsAndErrors) {
  RelationalRun(*rs_, "CREATE TABLE EmitDim(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE EmitFact(k INT64, i INT64, d DOUBLE)");
  RelationalRun(*rs_, "INSERT INTO EmitDim VALUES (1),(2),(3)");
  RelationalRun(*rs_,
                "INSERT INTO EmitFact VALUES "
                "(1,2,1.5),(1,4,2.5),(1,NULL,NULL),"
                "(2,NULL,NULL)");
  const std::string sql =
      "SELECT d.k, SUM(f.i), SUM(f.d), AVG(f.d), COUNT(f.d), COUNT(*), "
      "SUM(f.d), 17 FROM EmitDim d LEFT JOIN EmitFact f ON d.k=f.k "
      "GROUP BY d.k ORDER BY d.k";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1), Value(6), Value(4.0), Value(2.0),
                                   Value(2), Value(3), Value(4.0), Value(17)}),
                              Row({Value(2), Value(), Value(), Value(),
                                   Value(0), Value(1), Value(), Value(17)}),
                              Row({Value(3), Value(), Value(), Value(),
                                   Value(0), Value(1), Value(), Value(17)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  RelationalRun(*rs_, "INSERT INTO EmitFact VALUES (1,9223372036854775807,1)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(context, sql);
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_EQ(result.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(result.GetStatus(), Status::kIsInfinity);
  }
}

TEST_F(RelationalRegressionTest, FusedIntegerCaseRangesKeepNullAndOverflow) {
  RelationalRun(*rs_, "CREATE TABLE RangeDim (k INT64, g STRING)");
  RelationalRun(*rs_, "CREATE TABLE RangeFact (k INT64, a INT64, b INT64)");
  RelationalRun(*rs_, "INSERT INTO RangeDim VALUES (1,'a'),(1,'a'),(2,'b')");
  RelationalRun(*rs_,
                "INSERT INTO RangeFact VALUES (1,30,0),(1,60,0),(1,90,0),"
                "(1,NULL,0),(1,-1,0),(2,0,NULL)");
  const std::string sql =
      "SELECT d.g, SUM(CASE WHEN f.a-f.b<=30 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b>30 AND f.a-f.b<=60 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b>60 AND f.a-f.b<=90 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b>90 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b=30 THEN 1 ELSE 0 END), COUNT(*) "
      "FROM RangeFact f JOIN RangeDim d ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value("a"), Value(4), Value(2), Value(2),
                                   Value(0), Value(2), Value(10)}),
                              Row({Value("b"), Value(0), Value(0), Value(0),
                                   Value(0), Value(0), Value(1)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  RelationalRun(*rs_,
                "INSERT INTO RangeFact VALUES (2,9223372036854775807,-1)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto executed = engine.Execute(context, sql);
  ASSERT_TRUE(executed.HasValue());
  executed.Value().Collect();
  EXPECT_NE(executed.Value().GetStatus(), Status::kSuccess);
  context.Abort();
}

TEST_F(RelationalRegressionTest, InnerJoinCompiledResidualPreservesNulls) {
  RelationalRun(*rs_, "CREATE TABLE ResidualL (k INT64, v FLOAT64)");
  RelationalRun(*rs_, "CREATE TABLE ResidualR (k INT64, zip STRING)");
  RelationalRun(*rs_,
                "INSERT INTO ResidualL VALUES (1,2),(1,8),(2,NULL),(3,8)");
  RelationalRun(*rs_,
                "INSERT INTO ResidualR VALUES (1,'AB123'),(1,'CD123'),"
                "(2,'AB456'),(3,NULL)");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT l.k,l.v,r.zip FROM ResidualL l, ResidualR r "
                          "WHERE l.k=r.k AND (SUBSTR(r.zip,1,2) IN ('AB','XY') "
                          "OR l.v>5) ORDER BY l.k,l.v,r.zip"),
            std::vector<Row>({Row({Value(1), Value(2.0), Value("AB123")}),
                              Row({Value(1), Value(8.0), Value("AB123")}),
                              Row({Value(1), Value(8.0), Value("CD123")}),
                              Row({Value(2), Value(), Value("AB456")}),
                              Row({Value(3), Value(8.0), Value()})}));
}

TEST_F(RelationalRegressionTest, AbsorbedStringsKeepOwnershipAndSnapshots) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        rs_->CreateTable(
            setup,
            Schema("OwnedDim", {Column("k", ValueType::kInt64,
                                       Constraint(Constraint::kPrimaryKey)),
                                Column("label", ValueType::kVarChar)})));
    for (const auto& row : {Row({Value(1), Value("shared string payload")}),
                            Row({Value(2), Value()}),
                            Row({Value(3), Value("shared string payload")}),
                            Row({Value(4), Value("")})}) {
      ASSERT_SUCCESS(dim.Insert(setup.txn_, row).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("OwnedFact", {Column("k", ValueType::kInt64),
                                        Column("d", ValueType::kInt64)})));
    for (int64_t i = 0; i < 2560; ++i) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_, Row({Value((i % 5) + 1), Value(i % 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE TABLE OwnedExtra(d INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO OwnedExtra VALUES (0,10),(1,20)");
  for (const auto* table : {"OwnedDim", "OwnedFact", "OwnedExtra"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + table);
  }
  const std::string sql =
      "SELECT s.label,COUNT(*),SUM(e.v) FROM OwnedFact f "
      "JOIN OwnedDim s ON f.k=s.k JOIN OwnedExtra e ON f.d=e.d "
      "GROUP BY s.label ORDER BY s.label NULLS FIRST";
  const std::vector<Row> expected{
      Row({Value(), Value(512), Value(7680)}),
      Row({Value(""), Value(512), Value(7680)}),
      Row({Value("shared string payload"), Value(1024), Value(15360)})};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql), expected);
    const auto kept_key =
        RelationalRun(*rs_,
                      "SELECT f.k,s.label,COUNT(*),SUM(e.v) FROM OwnedFact f "
                      "JOIN OwnedDim s ON f.k=s.k JOIN OwnedExtra e ON f.d=e.d "
                      "GROUP BY f.k,s.label ORDER BY f.k");
    ASSERT_EQ(kept_key.size(), 4U);
    for (size_t i = 0; i < kept_key.size(); ++i) {
      EXPECT_EQ(kept_key[i][0], Value(static_cast<int64_t>(i + 1)));
      EXPECT_EQ(kept_key[i][2], Value(512));
      EXPECT_EQ(kept_key[i][3], Value(7680));
    }
  }
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  const auto read_old = [&] {
    auto result = engine.Execute(old, sql);
    EXPECT_TRUE(result.HasValue());
    if (!result.HasValue()) {
      return std::vector<Row>{};
    }
    auto rows = result.Value().Collect();
    EXPECT_TRUE(result.Value().GetStatus().ok());
    return rows;
  };
  EXPECT_EQ(read_old(), expected);
  RelationalRun(*rs_, "UPDATE OwnedDim SET label='changed' WHERE k=1");
  EXPECT_EQ(read_old(), expected);
  EXPECT_EQ(RelationalRun(*rs_, sql),
            (std::vector<Row>{expected[0], expected[1],
                              Row({Value("changed"), Value(512), Value(7680)}),
                              Row({Value("shared string payload"), Value(512),
                                   Value(7680)})}));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest, DeferredFilteredPkKeepsNullsAndSnapshots) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, days,
        rs_->CreateTable(
            setup,
            Schema("DeferredDays", {Column("day", ValueType::kInt64,
                                           Constraint(Constraint::kPrimaryKey)),
                                    Column("year", ValueType::kInt64)})));
    for (const auto& row :
         {Row({Value(1), Value(2001)}), Row({Value(2), Value()}),
          Row({Value(3), Value(std::numeric_limits<int64_t>::max())}),
          Row({Value(4), Value(std::numeric_limits<int32_t>::min())})}) {
      ASSERT_SUCCESS(days.Insert(setup.txn_, row).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("DeferredFact", {Column("k", ValueType::kInt64),
                                           Column("day", ValueType::kInt64),
                                           Column("v", ValueType::kInt64),
                                           Column("p", ValueType::kDate)})));
    for (int64_t i = 0; i < 40000; ++i) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_,
                      Row({Value((i % 2) + 1), Value((i % 4) + 1), Value(1),
                           i % 3 == 0 ? Value()
                                      : Value::DateFromDays((i % 3) - 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE TABLE DeferredDim (k INT64,g STRING)");
  RelationalRun(*rs_, "INSERT INTO DeferredDim VALUES (1,'a'),(2,'b'),(2,'b')");
  for (const auto* table : {"DeferredDays", "DeferredFact", "DeferredDim"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + table);
  }
  const std::string sql =
      "SELECT d.g,y.year,SUM(f.v),COUNT(*) FROM DeferredFact f "
      "JOIN DeferredDays y ON f.day=y.day JOIN DeferredDim d ON f.k=d.k "
      "WHERE y.day<=2 GROUP BY d.g,y.year ORDER BY d.g";
  const std::vector<Row> expected{
      Row({Value("a"), Value(2001), Value(10000), Value(10000)}),
      Row({Value("b"), Value(), Value(20000), Value(20000)})};
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto all = RelationalRun(
        *rs_,
        "SELECT d.g,y.year,SUM(f.v),COUNT(*) FROM DeferredFact f "
        "JOIN DeferredDays y ON f.day=y.day JOIN DeferredDim d ON f.k=d.k "
        "WHERE y.day<=4 GROUP BY d.g,y.year "
        "HAVING COUNT(*)>1 ORDER BY d.g,y.year");
    EXPECT_EQ(all,
              (std::vector<Row>{
                  expected[0],
                  Row({Value("a"), Value(std::numeric_limits<int64_t>::max()),
                       Value(10000), Value(10000)}),
                  expected[1],
                  Row({Value("b"), Value(std::numeric_limits<int32_t>::min()),
                       Value(20000), Value(20000)})}));
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT d.g,y.year+1,COUNT(*) FROM DeferredFact f "
                      "JOIN DeferredDays y ON f.day=y.day "
                      "JOIN DeferredDim d ON f.k=d.k WHERE y.day<=2 "
                      "GROUP BY d.g,y.year HAVING COUNT(*)>1 ORDER BY d.g"),
        (std::vector<Row>{Row({Value("a"), Value(2002), Value(10000)}),
                          Row({Value("b"), Value(), Value(20000)})}));
  }
  std::array<std::array<int64_t, 3>, 2> counts{};
  for (int64_t i = 0; i < 40000; ++i) {
    if (i % 4 < 2) {
      counts[static_cast<size_t>(i % 4)][static_cast<size_t>(i % 3)] +=
          i % 4 == 0 ? 1 : 2;
    }
  }
  std::vector<Row> date_expected;
  date_expected.reserve(counts.size() * counts[0].size());
  for (size_t g = 0; g < counts.size(); ++g) {
    for (size_t p = 0; p < counts[g].size(); ++p) {
      date_expected.push_back(Row(
          {Value(g == 0 ? "a" : "b"), g == 0 ? Value(2001) : Value(),
           p == 0 ? Value() : Value::DateFromDays(static_cast<int64_t>(p) - 2),
           Value(counts[g][p]), Value(counts[g][p])}));
    }
  }
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(
                  *rs_,
                  "SELECT d.g,y.year,f.p,SUM(f.v),COUNT(*) FROM DeferredFact f "
                  "JOIN DeferredDays y ON f.day=y.day "
                  "JOIN DeferredDim d ON f.k=d.k WHERE y.day<=2 "
                  "GROUP BY d.g,y.year,f.p HAVING COUNT(*)>1 "
                  "ORDER BY d.g,y.year,f.p"),
              date_expected);
  }
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const auto& [predicate, expect_null] :
         {std::pair{"f.p IS NULL", true}, std::pair{"NOT (f.p IS NULL)", false},
          std::pair{"NOT (NOT (f.p IS NULL))", true},
          std::pair{"f.p IS NOT NULL AND f.v=1", false}}) {
      std::vector<Row> filtered;
      filtered.reserve(date_expected.size());
      for (const Row& row : date_expected) {
        if (row[2].IsNull() == expect_null) {
          filtered.push_back(row);
        }
      }
      const std::string null_sql =
          "SELECT d.g,y.year,f.p,SUM(f.v),COUNT(*) FROM DeferredFact f "
          "JOIN DeferredDays y ON f.day=y.day "
          "JOIN DeferredDim d ON f.k=d.k WHERE y.day<=2 AND " +
          std::string(predicate) +
          " GROUP BY d.g,y.year,f.p HAVING COUNT(*)>1 "
          "ORDER BY d.g,y.year,f.p";
      EXPECT_EQ(RelationalRun(*rs_, null_sql), filtered) << predicate;
    }
  }
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  const auto read_old = [&] {
    std::vector<Row> rows;
    auto prepared = engine.Prepare(old, sql);
    EXPECT_TRUE(prepared.HasValue());
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
    return rows;
  };
  EXPECT_EQ(read_old(), expected);
  RelationalRun(*rs_, "UPDATE DeferredDays SET year=2003 WHERE day=1");
  EXPECT_EQ(read_old(), expected);
  auto fresh = expected;
  fresh[0][1] = Value(2003);
  EXPECT_EQ(RelationalRun(*rs_, sql), fresh);
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       StreamGroupedJoinKeepsDuplicatesNullsAndSnapshot) {
  RelationalRun(*rs_, "CREATE TABLE StreamDim (k INT64, g INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE StreamFact (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO StreamDim VALUES "
                "(1,0,10),(1,NULL,20),(2,0,NULL),(3,3,NULL),(99,9,100)");
  RelationalRun(*rs_,
                "INSERT INTO StreamFact VALUES "
                "(1,5),(1,NULL),(2,7),(3,NULL),(7,99),(NULL,100)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("StreamFact"));
    for (int i = 0; i < 2000; ++i) {
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_, Row({Value(100), Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE StreamDim");
  RelationalRun(*rs_, "ANALYZE StreamFact");
  const std::string sql =
      "SELECT d.g, SUM(f.v), SUM(d.v), COUNT(*), COUNT(f.v) FROM StreamDim d "
      "JOIN StreamFact f ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  const std::vector<Row> expected{
      Row({Value(), Value(5), Value(40), Value(2), Value(1)}),
      Row({Value(0), Value(12), Value(20), Value(3), Value(2)}),
      Row({Value(3), Value(), Value(), Value(1), Value(0)})};
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("streamed_grouped_joins=1"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("relation_spills=0"), std::string::npos) << profile;
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  const auto read_old = [&] {
    std::vector<Row> rows;
    auto prepared = engine.Prepare(old, sql);
    EXPECT_TRUE(prepared.HasValue());
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
    return rows;
  };
  EXPECT_EQ(read_old(), expected);
  RelationalRun(*rs_, "INSERT INTO StreamFact VALUES (2,11)");
  EXPECT_EQ(read_old(), expected);
  auto fresh = expected;
  fresh[1] = Row({Value(0), Value(23), Value(20), Value(4), Value(3)});
  EXPECT_EQ(RelationalRun(*rs_, sql), fresh);
  ASSERT_SUCCESS(old.PreCommit());
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(128);
  EXPECT_EQ(RelationalRun(*rs_, sql), fresh);
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("streamed_grouped_joins=0"),
            std::string::npos);
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), 0U);
}

TEST_F(RelationalRegressionTest,
       StreamGroupedJoinPreservesPhysicalSumPrefixes) {
  RelationalRun(*rs_, "CREATE TABLE PrefixDim (k INT64, g INT64)");
  RelationalRun(*rs_, "INSERT INTO PrefixDim VALUES (1,0)");
  RelationalRun(
      *rs_, "CREATE TABLE PrefixFact (k INT64, v INT64, padding STRING(MAX))");
  const auto maximum = std::numeric_limits<int64_t>::max();
  const auto run_case = [&](const std::vector<int64_t>& values, bool overflow,
                            int64_t total) {
    RelationalRun(*rs_, "DELETE FROM PrefixFact");
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("PrefixFact"));
    for (int64_t value : values) {
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_, Row({Value(1), Value(value),
                                        Value(std::string(12000, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
    RelationalRun(*rs_, "ANALYZE PrefixDim");
    RelationalRun(*rs_, "ANALYZE PrefixFact");
    auto read = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto prepared = engine.Prepare(read,
                                   "SELECT d.g, SUM(f.v) FROM PrefixFact f "
                                   "JOIN PrefixDim d ON f.k=d.k GROUP BY d.g");
    ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
    Row row;
    if (overflow) {
      EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
      EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
    } else {
      ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
      EXPECT_EQ(row, Row({Value(0), Value(total)}));
      EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
  };
  run_case({maximum, 0, 1, -1}, true, 0);
  run_case({-maximum, 0, maximum, maximum}, false, maximum);
}

TEST_F(RelationalRegressionTest,
       PartitionOwnedNumericGroupsKeepNullAndOverflowSemantics) {
  RelationalRun(*rs_, "CREATE TABLE NumericEdges(k INT64,j INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO NumericEdges VALUES "
                "(NULL,2,3),(NULL,2,4),(1,1,5),(1,1,6)");
  const auto* const sql =
      "SELECT k,j,COUNT(*) c,SUM(v) s FROM NumericEdges "
      "GROUP BY k,j ORDER BY k NULLS FIRST LIMIT 2";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            (std::vector<Row>{Row({Value(), Value(2), Value(2), Value(7)}),
                              Row({Value(1), Value(1), Value(2), Value(11)})}));
  // Even a small final LIMIT cannot hide overflow in a discarded group.
  RelationalRun(*rs_,
                "INSERT INTO NumericEdges VALUES "
                "(3,3,9223372036854775807),(3,3,1)");
  auto context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto result =
      engine.Execute(context,
                     "SELECT k,j,SUM(v) FROM NumericEdges GROUP BY k,j "
                     "ORDER BY k NULLS FIRST LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_EQ(result.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(result.GetStatus(), Status::kIsInfinity);
  }
  context.Abort();
}

TEST_F(RelationalRegressionTest,
       DictionaryFoldOwnsResultsAcrossPreparationWaves) {
  RelationalRun(*rs_, "CREATE TABLE WaveText(s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("WaveText"));
    for (int64_t i = 0; i < 36000; ++i) {
      std::string value(90, 'x');
      value += static_cast<char>('a' + ((i % 18000) % 11));
      value += '/' + std::to_string(i % 18000);
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_,
                       Row({i % 127 == 0 ? Value() : Value(std::move(value))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("WaveText"));
    auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_GT(store->dictionaries[0].entries.size(), 16384U);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto query = [](std::string_view argument) {
    return "SELECT REGEXP_REPLACE(" + std::string(argument) +
           ",'[0-9]+$','') k,COUNT(*),COUNT(s),MIN(s),MAX(s),"
           "AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM WaveText GROUP BY k "
           "HAVING COUNT(*)>10 ORDER BY k NULLS LAST";
  };
  const auto expected = RelationalRun(*rs_, query("s||''"));
  ASSERT_FALSE(expected.empty());
  const auto capture_query = [](std::string_view argument, int64_t threshold) {
    return "SELECT REGEXP_REPLACE(" + std::string(argument) +
           ",'^(.*)/[0-9]+$','\\\\1') k,COUNT(*),COUNT(s),MIN(s),MAX(s),"
           "AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM WaveText GROUP BY k "
           "HAVING COUNT(*)>" +
           std::to_string(threshold) + " ORDER BY k NULLS LAST";
  };
  const auto captured = RelationalRun(*rs_, capture_query("s||''", 10));
  ASSERT_FALSE(captured.empty());
  ASSERT_TRUE(RelationalRun(*rs_, capture_query("s||''", 4000)).empty());
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, query("s")), expected);
    EXPECT_EQ(RelationalRun(*rs_, capture_query("s", 10)), captured);
    EXPECT_TRUE(RelationalRun(*rs_, capture_query("s", 4000)).empty());
  }
}

TEST_F(RelationalRegressionTest, PartitionOwnedNumericGroupsKeepGlobalTopN) {
  struct Expected {
    int64_t key;
    int64_t second;
    int64_t count;
    int64_t sum;
    int64_t width_sum;
    size_t first;
  };
  std::map<std::pair<int64_t, int64_t>, Expected> expected_map;
  std::map<std::pair<int64_t, int64_t>, Expected> filtered_map;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(setup, Schema("NumericGroups",
                                       {Column("k", ValueType::kInt64),
                                        Column("j", ValueType::kInt64),
                                        Column("v", ValueType::kInt64),
                                        Column("w", ValueType::kInt64),
                                        Column("tag", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 60000; ++i) {
      const int64_t k = ((i % 50000) * 11259375) - 300000000000;
      const int64_t j = ((i * 1009) % 50000) * 2731;
      const int64_t v = (i % 17) - 8;
      const int64_t w = (i % 29) + 1024;
      ASSERT_SUCCESS(
          table
              .Insert(
                  setup.txn_,
                  Row({Value(k), Value(j), Value(v), Value(w),
                       i % 13 == 0 ? Value() : Value(i % 5 ? "present" : "")}))
              .GetStatus());
      auto [it, inserted] = expected_map.try_emplace(
          std::pair{k, j}, Expected{.key = k,
                                    .second = j,
                                    .count = 0,
                                    .sum = 0,
                                    .width_sum = 0,
                                    .first = static_cast<size_t>(i)});
      ++it->second.count;
      it->second.sum += v;
      it->second.width_sum += w;
      if (i % 13 != 0 && i % 5 != 0) {
        auto [f, fresh] = filtered_map.try_emplace(
            std::pair{k, j}, Expected{.key = k,
                                      .second = j,
                                      .count = 0,
                                      .sum = 0,
                                      .width_sum = 0,
                                      .first = static_cast<size_t>(i)});
        ++f->second.count;
        f->second.sum += v;
        f->second.width_sum += w;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NumericGroups"));
    ASSERT_NE(GetOrBuildLSTableStore(context.txn_, *table, true), nullptr);
    ASSERT_SUCCESS(context.PreCommit());
  }
  std::vector<Expected> groups;
  groups.reserve(expected_map.size());
  for (const auto& [key, value] : expected_map) {
    groups.push_back(value);
  }
  const auto rows = [](const std::vector<Expected>& expected_groups,
                       size_t offset, size_t limit) {
    std::vector<Row> result;
    for (size_t i = offset;
         i < std::min(expected_groups.size(), offset + limit); ++i) {
      const auto& g = expected_groups[i];
      result.emplace_back(std::vector<Value>{
          Value(g.key), Value(g.second), Value(g.count), Value(g.sum),
          Value(static_cast<double>(g.width_sum) / g.count)});
    }
    return result;
  };
  const std::string base =
      "SELECT k,j,COUNT(*) c,SUM(v) s,AVG(CAST(w AS FLOAT64)) a "
      "FROM NumericGroups GROUP BY k,j ";
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    std::ranges::sort(groups, [](const Expected& a, const Expected& b) {
      return std::tie(a.count, a.sum, a.key, a.second) >
             std::tie(b.count, b.sum, b.key, b.second);
    });
    const auto ordered =
        base + "ORDER BY c DESC,s DESC,k DESC,j DESC LIMIT 13 OFFSET 7";
    EXPECT_EQ(RelationalRun(*rs_, ordered), rows(groups, 7, 13));
    std::vector<Expected> filtered;
    filtered.reserve(filtered_map.size());
    for (const auto& [key, value] : filtered_map) {
      filtered.push_back(value);
    }
    std::ranges::sort(filtered, [](const Expected& a, const Expected& b) {
      return std::tie(a.count, a.sum, a.key, a.second) >
             std::tie(b.count, b.sum, b.key, b.second);
    });
    const auto* const filtered_sql =
        "SELECT k,j,COUNT(*) c,SUM(v) s,AVG(CAST(w AS FLOAT64)) a "
        "FROM NumericGroups WHERE tag<>'' GROUP BY k,j "
        "ORDER BY c DESC,s DESC,k DESC,j DESC LIMIT 13 OFFSET 7";
    EXPECT_EQ(RelationalRun(*rs_, filtered_sql), rows(filtered, 7, 13));
    EXPECT_NE(ExplainAnalyze(*rs_, filtered_sql)
                  .find("partitioned_numeric_aggregations=1"),
              std::string::npos);
    EXPECT_NE(ExplainAnalyze(*rs_, ordered)
                  .find("partitioned_numeric_aggregations=1"),
              std::string::npos);
    std::ranges::sort(groups, [](const Expected& a, const Expected& b) {
      return a.first < b.first;
    });
    EXPECT_EQ(RelationalRun(*rs_, base + "LIMIT 19 OFFSET 11"),
              rows(groups, 11, 19));
    std::stable_sort(
        groups.begin(), groups.end(),
        [](const Expected& a, const Expected& b) { return a.count > b.count; });
    EXPECT_EQ(RelationalRun(*rs_, base + "ORDER BY c DESC LIMIT 13 OFFSET 7"),
              rows(groups, 7, 13));
    EXPECT_TRUE(RelationalRun(*rs_, base + "ORDER BY c DESC LIMIT 0").empty());
  }
}

TEST_F(RelationalRegressionTest,
       SourceIntegerPeeksPreserveTypesNullsAndSnapshot) {
  for (const int64_t count : {257, 20000}) {
    const std::string name = "PeekSource" + std::to_string(count);
    RelationalRun(*rs_,
                  "CREATE TABLE " + name +
                      " (z STRING,i INT64,d DATE,f FLOAT64,u UINT64,p STRING)");
    std::vector<int64_t> expected;
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            setup.GetTable(name));
      for (int64_t i = 0; i < count; ++i) {
        Row row(
            {Value(std::string(64, 'z')), i % 13 == 0 ? Value() : Value(i),
             i % 11 == 0 ? Value() : Value::DateFromDays(i % 100),
             i % 7 == 0 ? Value() : Value(i % 2 == 0 ? 10.0 : 30.0),
             i % 3 == 0 ? Value(-1).WithUnsigned() : Value(0).WithUnsigned(),
             Value(std::string(128, 'p'))});
        ASSERT_TRUE(table->Insert(setup.txn_, row).HasValue());
        if (i % 13 != 0 && i % 11 != 0 && i % 7 != 0 && i >= count / 2 &&
            i % 100 >= 42 && i % 2 == 0 && i % 3 == 0) {
          expected.push_back(i);
        }
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    RelationalRun(*rs_, "ANALYZE " + name);
    SelectSource source;
    source.table = name;
    source.alias = "r";
    const std::vector<slot_t> projection{1, 2, 3, 4};
    const auto compare = [](const char* column, BinaryOperation operation,
                            const Value& value) {
      return BinaryExpressionExp(ColumnValueExp(ColumnName("r", column)),
                                 operation, ConstantValueExp(value));
    };
    const std::vector<Expression> predicates{
        compare("i", BinaryOperation::kGreaterThanEquals, Value(count / 2)),
        compare("i", BinaryOperation::kLessThan, Value(-1).WithUnsigned()),
        compare("d", BinaryOperation::kGreaterThanEquals,
                Value::DateFromDays(42)),
        compare("f", BinaryOperation::kLessThan, Value(25)),
        compare("u", BinaryOperation::kGreaterThan, Value(0))};
    auto read = [&](TransactionContext& context) {
      auto result = relational_detail::LoadSource(context, source, nullptr, {},
                                                  &projection, &predicates);
      EXPECT_TRUE(result.HasValue()) << result.GetStatus();
      std::vector<int64_t> ids;
      if (result.HasValue()) {
        EXPECT_SUCCESS(result.Value().ForEachRow(
            [&](const Row& row) { ids.push_back(row[0].value.int_value); }));
      }
      std::sort(ids.begin(), ids.end());
      return ids;
    };
    auto old = rs_->BeginReadOnlyContext();
    ASSERT_FALSE(expected.empty());
    EXPECT_EQ(read(old), expected);
    RelationalRun(*rs_, "UPDATE " + name + " SET d=DATE '1970-01-01' WHERE i=" +
                            std::to_string(expected.front()));
    EXPECT_EQ(read(old), expected);
    auto fresh = rs_->BeginReadOnlyContext();
    expected.erase(expected.begin());
    EXPECT_EQ(read(fresh), expected);
    old.Abort();
    fresh.Abort();
  }
}

TEST_F(RelationalRegressionTest,
       SelectiveLoadFrontierPrunesAcrossDimensionChain) {
  RelationalRun(*rs_, "CREATE TABLE FrontierRegion (r INT64, flag INT64)");
  RelationalRun(*rs_,
                "INSERT INTO FrontierRegion VALUES (0,0),(1,1),(1,1),(NULL,1)");
  RelationalRun(*rs_, "CREATE TABLE FrontierDim (k INT64, r INT64)");
  RelationalRun(*rs_, "CREATE TABLE FrontierFact (k INT64, v INT64, p STRING)");
  int64_t expected_count = 0, expected_sum = 0;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("FrontierDim"));
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
          dim->Insert(setup.txn_, Row({Value(i), Value(i % 10)})).HasValue());
    }
    ASSERT_TRUE(dim->Insert(setup.txn_, Row({Value(1), Value(1)})).HasValue());
    ASSERT_TRUE(dim->Insert(setup.txn_, Row({Value(), Value(1)})).HasValue());
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("FrontierFact"));
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_TRUE(
          fact->Insert(setup.txn_, Row({i % 17 == 0 ? Value() : Value(i % 100),
                                        i % 13 == 0 ? Value() : Value(i % 7),
                                        Value(std::string(128, 'p'))}))
              .HasValue());
      if (i % 17 != 0 && i % 13 != 0 && i % 10 == 1) {
        const int64_t multiplicity = i % 100 == 1 ? 4 : 2;
        expected_count += multiplicity;
        expected_sum += (i % 7) * multiplicity;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string query =
      "SELECT d.r,COUNT(*),SUM(f.v) FROM FrontierFact f "
      "JOIN FrontierDim d ON f.k=d.k JOIN FrontierRegion r ON d.r=r.r "
      "WHERE f.v>=0 AND r.flag=1 GROUP BY d.r";
  EXPECT_EQ(RelationalRun(*rs_, query),
            std::vector<Row>(
                {Row({Value(1), Value(expected_count), Value(expected_sum)})}));
  const auto profile = ExplainAnalyze(*rs_, query);
  const std::string field = "scan_output_rows=";
  const size_t position = profile.find(field);
  ASSERT_NE(position, std::string::npos) << profile;
  const auto rows = std::stoull(profile.substr(position + field.size()));
  EXPECT_LT(rows, 2500U) << profile;

  // The LEFT join keeps the ordered loading path. Its null-supplying side
  // must not seed a filter; the preserved region -> dimension -> fact chain
  // can still prune rows, without losing duplicate join multiplicities.
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    const std::string ordered_query =
        "SELECT d.r,COUNT(*),SUM(f.v),COUNT(t.k) FROM FrontierFact f "
        "JOIN FrontierDim d ON f.k=d.k JOIN FrontierRegion r ON d.r=r.r "
        "LEFT JOIN KeyTable t ON r.r=t.k AND t.k<0 "
        "WHERE f.v>=0 AND r.flag=1 AND t.k IS NULL GROUP BY d.r";
    EXPECT_EQ(RelationalRun(*rs_, ordered_query),
              std::vector<Row>({Row({Value(1), Value(expected_count),
                                     Value(expected_sum), Value(0)})}));
    const auto ordered_profile = ExplainAnalyze(*rs_, ordered_query);
    const size_t ordered_position = ordered_profile.find(field);
    ASSERT_NE(ordered_position, std::string::npos) << ordered_profile;
    const auto ordered_rows =
        std::stoull(ordered_profile.substr(ordered_position + field.size()));
    EXPECT_LT(ordered_rows, 2500U) << ordered_profile;
  }
}

TEST_F(RelationalRegressionTest,
       DenseBuildFrequencyFoldKeepsNullsAndNegativeFallback) {
  RelationalRun(*rs_, "CREATE TABLE DenseDim (k INT64, g INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE DenseFact (k INT64, padding STRING)");
  std::map<int64_t, std::array<int64_t, 3>> expected;
  int64_t ones = 0;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("DenseDim"));
    for (int64_t k = -128; k < 128; ++k) {
      ASSERT_TRUE(
          dim->Insert(setup.txn_,
                      Row({Value(k), k % 7 == 0 ? Value() : Value(k % 4),
                           k % 5 == 0 ? Value() : Value(2)}))
              .HasValue());
    }
    ASSERT_TRUE(dim->Insert(setup.txn_, Row({Value(129), Value(99), Value()}))
                    .HasValue());
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("DenseFact"));
    for (int64_t i = 0; i < 20000; ++i) {
      const int64_t k = (i % 300) - 150;
      ASSERT_TRUE(
          fact->Insert(setup.txn_, Row({i % 17 == 0 ? Value() : Value(k),
                                        Value(std::string(256, 'x'))}))
              .HasValue());
      if (i % 17 == 0) {
        continue;
      }
      if (k == 129) {
        ++expected[99][0];
        continue;
      }
      if (k < -128 || k >= 128) {
        continue;
      }
      auto& group = expected[k % 7 == 0 ? 1000 : k % 4];
      ++group[0];
      if (k % 5 != 0) {
        ++group[1];
        group[2] += 2;
      }
      if (k == 1) {
        ++ones;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,COUNT(*),COUNT(d.v),SUM(d.v) FROM DenseFact f "
      "JOIN DenseDim d ON f.k=d.k GROUP BY d.g";
  const auto check = [&](const std::vector<Row>& rows, bool negative) {
    EXPECT_EQ(rows.size(), expected.size());
    for (const auto& row : rows) {
      const int64_t key = row[0].IsNull() ? 1000 : row[0].value.int_value;
      const auto found = expected.find(key);
      ASSERT_NE(found, expected.end());
      EXPECT_EQ(row[1], Value(found->second[0]));
      EXPECT_EQ(row[2], Value(found->second[1]));
      EXPECT_EQ(row[3], found->second[1] == 0
                            ? Value()
                            : Value(found->second[2] -
                                    (negative && key == 1 ? 3 * ones : 0)));
    }
  };
  {
    struct
        RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
      size_t limit = QueryMemoryBudget::Global().Limit();
      ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
    } restore;
    // The fact spans many morsels. Its unselected payload must not make
    // nonnegative frequency folding allocate a group table for each morsel.
    QueryMemoryBudget::Global().ResetForTest(size_t{512} * 1024);
    check(RelationalRun(*rs_, sql), false);
    const auto profile = ExplainAnalyze(*rs_, sql);
    EXPECT_NE(profile.find("streamed_grouped_joins=1"), std::string::npos)
        << profile;
  }
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("streamed_grouped_joins=1"),
            std::string::npos);
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto prepared = engine.Prepare(old, sql);
  ASSERT_TRUE(prepared.HasValue());
  RelationalRun(*rs_, "UPDATE DenseDim SET v=-1 WHERE k=1");
  std::vector<Row> old_rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    old_rows.push_back(row);
  }
  ASSERT_SUCCESS(prepared.Value()->GetStatus());
  check(old_rows, false);
  check(RelationalRun(*rs_, sql), true);
  old.Abort();
  RelationalRun(*rs_, "CREATE TABLE DenseOverflow (k INT64,g INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO DenseOverflow VALUES (1,0,9223372036854775807)");
  RelationalRun(*rs_, "ANALYZE DenseOverflow");
  auto overflow_context = rs_->BeginReadOnlyContext();
  SqlEngine overflow_engine(*rs_);
  auto overflow =
      overflow_engine.Prepare(overflow_context,
                              "SELECT d.g,SUM(d.v) FROM DenseFact f JOIN "
                              "DenseOverflow d ON f.k=d.k GROUP BY d.g");
  ASSERT_TRUE(overflow.HasValue());
  EXPECT_FALSE(overflow.Value()->Next(&row, nullptr));
  EXPECT_EQ(overflow.Value()->GetStatus(), Status::kIsInfinity);
  overflow_context.Abort();
}

TEST_F(RelationalRegressionTest,
       IntegerKeyCountReuseStillReadsFreshDimensionAndOldFactSnapshots) {
  RelationalRun(*rs_, "CREATE TABLE CachedDim (k INT64,g INT64,v INT64)");
  RelationalRun(*rs_, "INSERT INTO CachedDim VALUES (1,0,3),(2,0,5)");
  RelationalRun(*rs_, "CREATE TABLE CachedFact (k INT64,p STRING)");
  RowPosition first;
  int64_t ones = 0, twos = 0;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("CachedFact"));
    for (int64_t i = 0; i < 2000; ++i) {
      const Value key = i % 7 == 0 ? Value() : Value((i % 2) + 1);
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, pos,
          fact->Insert(setup.txn_, Row({key, Value(std::string(64, 'p'))})));
      if (i == 1) {
        first = pos;  // key 2, changed below while old is open.
      }
      if (!key.IsNull()) {
        (i % 2 == 0 ? ones : twos)++;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,COUNT(*),SUM(d.v) FROM CachedFact f JOIN CachedDim d ON "
      "f.k=d.k GROUP BY d.g";
  SqlEngine engine(*rs_);
  const auto read = [&](TransactionContext& context) {
    std::vector<Row> rows;
    auto plan = engine.Prepare(context, sql);
    EXPECT_TRUE(plan.HasValue());
    if (!plan.HasValue()) {
      return rows;
    }
    Row row;
    while (plan.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    EXPECT_SUCCESS(plan.Value()->GetStatus());
    return rows;
  };
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                        old.GetTable("CachedFact"));
  const auto morsels = fact->BuildScanMorsels(old.txn_);
  std::shared_ptr<const Table::IntegerKeyCounts> counts;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  do {
    counts = fact->TryReadIntegerKeyCounts(old.txn_, morsels, 0, 1, 2);
    if (counts) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_NE(counts, nullptr);
  const std::vector<Row> initial{
      Row({Value(0), Value(ones + twos), Value((3 * ones) + (5 * twos))})};
  EXPECT_EQ(read(old), initial);
  // Reusing fact counts must never cache the dimension's aggregate payload.
  RelationalRun(*rs_, "UPDATE CachedDim SET v=7 WHERE k=1");
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh),
              std::vector<Row>({Row({Value(0), Value(ones + twos),
                                     Value((7 * ones) + (5 * twos))})}));
    fresh.Abort();
  }
  auto writer = rs_->BeginContext();
  ASSERT_TRUE(fact->Update(writer.txn_, first,
                           Row({Value(1), Value(std::string(64, 'p'))}))
                  .HasValue());
  ASSERT_SUCCESS(writer.PreCommit());
  EXPECT_EQ(read(old), initial);
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(
        read(fresh),
        std::vector<Row>({Row({Value(0), Value(ones + twos),
                               Value((7 * (ones + 1)) + (5 * (twos - 1)))})}));
    fresh.Abort();
  }
  old.Abort();
  // Warm the exact histogram after the fact update, then add a positive
  // duplicate dimension key. A cache hit counts it once per dimension row.
  {
    auto warm = rs_->BeginReadOnlyContext();
    std::shared_ptr<const Table::IntegerKeyCounts> warmed;
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
      warmed = fact->TryReadIntegerKeyCounts(warm.txn_, morsels, 0, 1, 2);
      if (warmed) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < until);
    ASSERT_NE(warmed, nullptr);
    RelationalRun(*rs_, "INSERT INTO CachedDim VALUES (2,0,4)");
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(
        read(fresh),
        std::vector<Row>({Row({Value(0), Value(ones + (2 * twos) - 1),
                               Value((7 * (ones + 1)) + (9 * (twos - 1)))})}));
    fresh.Abort();
    warm.Abort();
    RelationalRun(*rs_, "DELETE FROM CachedDim WHERE v=4");
  }
  // Columnar input must retain the canonical signed and duplicate-key folds.
  RelationalRun(*rs_, "UPDATE CachedDim SET v=-1 WHERE k=1");
  RelationalRun(*rs_, "INSERT INTO CachedDim VALUES (2,0,4)");
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh),
              std::vector<Row>({Row({Value(0), Value(ones + (2 * twos) - 1),
                                     Value(-(ones + 1) + (9 * (twos - 1)))})}));
    fresh.Abort();
  }
  RelationalRun(*rs_, "DELETE FROM CachedDim WHERE v=4");
  RelationalRun(*rs_, "UPDATE CachedDim SET k=100000 WHERE k=2");
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh), std::vector<Row>({Row({Value(0), Value(ones + 1),
                                                  Value(-(ones + 1))})}));
    fresh.Abort();
  }
  RelationalRun(*rs_, "UPDATE CachedDim SET k=2 WHERE k=100000");
  RelationalRun(*rs_, "UPDATE CachedDim SET v=9223372036854775807 WHERE k=1");
  auto overflow_context = rs_->BeginReadOnlyContext();
  auto overflow = engine.Prepare(overflow_context, sql);
  ASSERT_TRUE(overflow.HasValue());
  Row row;
  EXPECT_FALSE(overflow.Value()->Next(&row, nullptr));
  EXPECT_EQ(overflow.Value()->GetStatus(), Status::kIsInfinity);
  overflow_context.Abort();
  RelationalRun(*rs_, "DROP TABLE CachedFact");
  RelationalRun(*rs_, "CREATE TABLE CachedFact (k INT64,p STRING)");
  RelationalRun(*rs_, "INSERT INTO CachedFact VALUES (2,'new')");
  RelationalRun(*rs_, "ANALYZE CachedFact");
  auto recreated = rs_->BeginReadOnlyContext();
  EXPECT_EQ(read(recreated),
            std::vector<Row>({Row({Value(0), Value(1), Value(5)})}));
  recreated.Abort();
}

TEST_F(RelationalRegressionTest,
       ColumnarGroupedJoinKeepsDateNullGroupsAndSmallBudgetFallback) {
  RelationalRun(*rs_, "CREATE TABLE ColumnDim (k INT64,g DATE,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ColumnDim VALUES (0,DATE "
                "'2000-01-01',NULL),(1,NULL,2),(2,DATE '2000-01-01',3)");
  RelationalRun(*rs_, "CREATE TABLE ColumnFact (k INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("ColumnFact"));
    for (int64_t i = 0; i < 6000; ++i) {
      ASSERT_TRUE(fact->Insert(setup.txn_, Row({Value(i % 3)})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,COUNT(*),COUNT(d.v),SUM(d.v) FROM ColumnFact f JOIN "
      "ColumnDim d ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  const std::vector<Row> expected{
      Row({Value(), Value(2000), Value(2000), Value(4000)}),
      Row({Value::Date("2000-01-01"), Value(4000), Value(2000), Value(6000)})};
  const auto read = [&] {
    auto context = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto plan = engine.Prepare(context, sql);
    EXPECT_TRUE(plan.HasValue());
    std::vector<Row> rows;
    if (plan.HasValue()) {
      Row row;
      while (plan.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_SUCCESS(plan.Value()->GetStatus());
    }
    context.Abort();
    return rows;
  };
  EXPECT_EQ(read(), expected);
  EXPECT_EQ(read(), expected);
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(size_t{512} * 1024);
  EXPECT_EQ(read(), expected);
}

TEST_F(RelationalRegressionTest,
       ColumnarGroupedJoinFallsBackWhenDimensionExceedsBound) {
  RelationalRun(*rs_,
                "CREATE TABLE WideColumnDim (k INT64,g INT64,a INT64,b INT64,c "
                "INT64,d INT64,e INT64,f INT64)");
  RelationalRun(*rs_, "CREATE TABLE WideColumnFact (k INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("WideColumnDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("WideColumnFact"));
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_TRUE(
          dim->Insert(setup.txn_, Row({Value(i), Value(0), Value(1), Value(2),
                                       Value(3), Value(4), Value(5), Value(6)}))
              .HasValue());
    }
    for (int64_t i = 0; i < 40000; ++i) {
      ASSERT_TRUE(fact->Insert(setup.txn_, Row({Value(i % 20000)})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  auto context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto plan = engine.Prepare(
      context,
      "SELECT d.g,SUM(d.a),SUM(d.b),SUM(d.c),SUM(d.d),SUM(d.e),SUM(d.f) FROM "
      "WideColumnFact f JOIN WideColumnDim d ON f.k=d.k GROUP BY d.g");
  ASSERT_TRUE(plan.HasValue());
  Row row;
  ASSERT_TRUE(plan.Value()->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(0), Value(40000), Value(80000), Value(120000),
                      Value(160000), Value(200000), Value(240000)}));
  EXPECT_FALSE(plan.Value()->Next(&row, nullptr));
  EXPECT_SUCCESS(plan.Value()->GetStatus());
  context.Abort();
}

TEST_F(RelationalRegressionTest,
       SelectivityAdaptiveLSTableKernelsMatchAcrossAllRegimes) {
  RelationalRun(*rs_,
                "CREATE TABLE SelAdaptiveLineitem ("
                "shipday INT64, k_int INT64, rf VARCHAR(4), ls VARCHAR(4), "
                "qty DOUBLE, price DOUBLE, disc DOUBLE, tax DOUBLE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          setup.GetTable("SelAdaptiveLineitem"));
    for (int64_t i = 0; i < 2000; ++i) {
      const int64_t shipday = i % 1000;
      const int64_t k_int = i % 10;
      const char* rf = (i % 2 == 0) ? "A" : "B";
      const char* ls = (i % 4 < 2) ? "F" : "O";
      const auto qty = static_cast<double>((i % 50) + 1);
      const double price = 100.0 + static_cast<double>(i % 20);
      const double disc = 0.05 * static_cast<double>(i % 3);
      const double tax = 0.02 * static_cast<double>(i % 4);
      ASSERT_TRUE(
          tbl->Insert(setup.txn_,
                      Row({Value(shipday), Value(k_int), Value(rf), Value(ls),
                           Value(qty), Value(price), Value(disc), Value(tax)}))
              .HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  // Exercise Pattern C (s=100% kDenseAllPass), Pattern B (s=50%
  // kSelectionPrefetch), and Pattern A (s=1% kSparseGather) on the Q1 5-term
  // shape, bounded-int group key, general dict group key, and table scan.
  for (const int64_t threshold : {int64_t{999}, int64_t{499}, int64_t{9}}) {
    const int64_t expected_rows = (threshold + 1) * 2;
    // 1. Q1 5-term shape
    const auto q1_rows = RelationalRun(
        *rs_,
        "SELECT rf, ls, SUM(qty), SUM(price), SUM(price * (1.0 - disc)), "
        "SUM(price * (1.0 - disc) * (1.0 + tax)), AVG(qty), AVG(price), "
        "AVG(disc), COUNT(*) FROM SelAdaptiveLineitem "
        "WHERE shipday <= " +
            std::to_string(threshold) + " GROUP BY rf, ls ORDER BY rf, ls");
    ASSERT_EQ(q1_rows.size(), 4U);
    int64_t total_cnt = 0;
    for (const Row& r : q1_rows) {
      total_cnt += r[9].value.int_value;
    }
    EXPECT_EQ(total_cnt, expected_rows);

    // 2. Bounded-int group key
    const auto bint_rows = RelationalRun(
        *rs_,
        "SELECT k_int, SUM(price * (1.0 - disc)), COUNT(*) "
        "FROM SelAdaptiveLineitem WHERE shipday <= " +
            std::to_string(threshold) + " GROUP BY k_int ORDER BY k_int");
    ASSERT_EQ(bint_rows.size(), 10U);
    int64_t bint_cnt = 0;
    for (const Row& r : bint_rows) {
      bint_cnt += r[2].value.int_value;
    }
    EXPECT_EQ(bint_cnt, expected_rows);

    // 3. General dict group key
    const auto dict_rows = RelationalRun(
        *rs_,
        "SELECT rf, SUM(price * disc), COUNT(*) "
        "FROM SelAdaptiveLineitem WHERE shipday <= " +
            std::to_string(threshold) + " GROUP BY rf ORDER BY rf");
    ASSERT_EQ(dict_rows.size(), 2U);
    EXPECT_EQ(dict_rows[0][2].value.int_value + dict_rows[1][2].value.int_value,
              expected_rows);
  }
}

TEST_F(RelationalRegressionTest,
       WindowApproxQuantilesAndTypedAggregatesRejectInvalidTypes) {
  auto expect_query_error = [&](std::string_view sql) {
    TransactionContext ctx = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    if (!prepared.HasValue()) {
      ctx.Abort();
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    ctx.Abort();
  };

  // Bug 4: DOUBLE count in window APPROX_QUANTILES must not reinterpret IEEE754
  // bits as int64_t and loop 4.6e18 times: valid small DOUBLE converts to int64
  // count, while out-of-range / non-numeric counts fail cleanly.
  const auto quant_rows = RelationalRun(
      *rs_, "SELECT APPROX_QUANTILES(k, 2.0) OVER () FROM SmallKeys");
  ASSERT_EQ(quant_rows.size(), 4U);
  ASSERT_TRUE(quant_rows[0][0].IsArray());
  EXPECT_EQ(quant_rows[0][0].ArrayElements().size(), 3U);
  expect_query_error("SELECT APPROX_QUANTILES(k, 1e20) OVER () FROM SmallKeys");
  expect_query_error(
      "SELECT APPROX_QUANTILES(k, 1000000) OVER () FROM SmallKeys");

  // Bug 6: VARCHAR input to window SUM/AVG/BIT_OR and aggregate BIT_OR must
  // return an error rather than reading union int_value UB.
  RelationalRun(*rs_, "CREATE TABLE WinStr (s STRING)");
  RelationalRun(*rs_, "INSERT INTO WinStr VALUES ('abc'), ('def')");
  expect_query_error("SELECT SUM(s) OVER () FROM WinStr");
  expect_query_error("SELECT AVG(s) OVER () FROM WinStr");
  expect_query_error("SELECT BIT_OR(s) OVER () FROM WinStr");
  expect_query_error("SELECT BIT_OR(s) FROM WinStr");
}

TEST_F(RelationalRegressionTest,
       RelationStickySpillErrorPropagatesThroughFinishSpillAndMerge) {
  relational_detail::Relation rel;
  rel.schema = Schema("s", {Column("k", ValueType::kInt64)});
  rel.spill_tail_ = std::make_shared<SpillFile>();
  rel.AddRow(Row({Value(int64_t{0})}));
  ASSERT_SUCCESS(rel.spill_tail_->FinishWriting());
  // A failed append must preserve the successful tail and its row count.
  rel.AddRow(Row({Value(int64_t{1})}));
  EXPECT_EQ(rel.spilled_rows_, 1U);
  ASSERT_NE(rel.spill_tail_, nullptr);
  EXPECT_EQ(rel.spill_tail_->Count(), 1U);
  EXPECT_FALSE(rel.GetStatus().ok());
  EXPECT_FALSE(rel.FinishSpill().ok());
  EXPECT_FALSE(
      rel.ForEachRow([](const Row&) { return Status::kSuccess; }).ok());

  relational_detail::Relation dst;
  dst.schema = Schema("s", {Column("k", ValueType::kInt64)});
  dst.MergeInMemoryShard(std::move(rel));
  EXPECT_FALSE(dst.GetStatus().ok());
  EXPECT_FALSE(dst.FinishSpill().ok());
}

TEST_F(RelationalRegressionTest,
       FusedGroupedJoinPreservesHavingWhenOutputSpills) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(
          setup, Schema("SpillHavingFact", {Column("fk", ValueType::kInt64),
                                            Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 2000; ++i) {
    const int64_t fk = i % 10;
    const int64_t v = (fk < 5) ? 1 : 10;
    ASSERT_SUCCESS(
        fact.Insert(setup.txn_, Row({Value(fk), Value(v)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());

  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  // Constrain memory budget so the fused grouped-join output spills to disk,
  // and use MIN(f.v) so post_having must run on the post-grouped relation.
  QueryMemoryBudget::Global().ResetForTest(128);
  const std::string sql =
      "SELECT a.k, MIN(f.v), SUM(f.v) FROM SpillHavingFact f "
      "JOIN KeyTable a ON f.fk = a.k "
      "GROUP BY a.k HAVING SUM(f.v) > 500 ORDER BY a.k";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 5U);
  for (int64_t i = 0; i < 5; ++i) {
    EXPECT_EQ(rows[static_cast<size_t>(i)],
              Row({Value(i + 5), Value(10), Value(2000)}));
  }
}

TEST_F(RelationalRegressionTest, AuditSubqueryInPropagatesComparisonErrors) {
  RelationalRun(*rs_, "CREATE TABLE AuditInText (s STRING)");
  RelationalRun(*rs_, "INSERT INTO AuditInText VALUES ('abc')");
  for (const char* op : {" IN ", " NOT IN "}) {
    auto context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    const std::string sql =
        std::string("SELECT 1") + op + "(SELECT s FROM AuditInText)";
    auto prepared = engine.Prepare(context, sql);
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    }
    context.Abort();
  }
}

TEST_F(RelationalRegressionTest, AuditDateIntegerFiltersPreserveTypeErrors) {
  RelationalRun(*rs_, "CREATE TABLE AuditDates (d DATE, n INT64)");
  RelationalRun(*rs_, "INSERT INTO AuditDates VALUES (DATE '1970-01-06', 5)");
  for (const char* predicate : {"d = 5", "d <> 6", "d = n", "n = d"}) {
    auto context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    const std::string sql =
        std::string("SELECT d FROM AuditDates WHERE ") + predicate;
    auto prepared = engine.Prepare(context, sql);
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    }
    context.Abort();
  }
}

TEST_F(RelationalRegressionTest, AuditDoubleZoneMapsKeepNaNNotEqualsRows) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(setup,
                       Schema("AuditNaNs", {Column("k", ValueType::kInt64),
                                            Column("d", ValueType::kDouble)})));
  for (int64_t i = 0; i < 33000; ++i) {
    const Value value = i < 256 || i % 3 == 0
                            ? Value(std::numeric_limits<double>::quiet_NaN())
                            : (i % 3 == 1 ? Value(5.0) : Value());
    ASSERT_SUCCESS(
        table.Insert(setup.txn_, Row({Value(1), value})).GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  RelationalRun(*rs_, "ANALYZE AuditNaNs");
  const int64_t expected = 256 + (32999 / 3 - 255 / 3);
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*) FROM AuditNaNs WHERE d <> 5"),
            std::vector<Row>({Row({Value(expected)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_, "SELECT k, COUNT(*) FROM AuditNaNs WHERE d <> 5 GROUP BY k"),
      std::vector<Row>({Row({Value(1), Value(expected)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT COUNT(*) FROM (SELECT d FROM AuditNaNs WHERE d <> 5) t"),
      std::vector<Row>({Row({Value(expected)})}));
}

TEST_F(RelationalRegressionTest, AuditFilteredMembershipDoesNotRetainNulls) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(setup, Schema("AuditNullMembership",
                                     {Column("k", ValueType::kInt64)})));
  for (int64_t i = 0; i < 33000; ++i) {
    ASSERT_SUCCESS(table.Insert(setup.txn_, Row({i == 0 ? Value() : Value(1)}))
                       .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  RelationalRun(*rs_, "ANALYZE AuditNullMembership");
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT k FROM SmallKeys WHERE k NOT IN (SELECT k FROM "
                    "AuditNullMembership WHERE k > 0) ORDER BY k"),
      std::vector<Row>({Row({Value(2)}), Row({Value(3)}), Row({Value(4)})}));
}

TEST_F(RelationalRegressionTest, AuditFusedCountsKeepIndependentNullDomains) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64,
                                    Constraint(Constraint::kNotNull))});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                              Column(ColumnName("d", "v"), ValueType::kInt64)});
  left.rows = {Row({Value(1)}), Row({Value(2)})};
  right.rows = {Row({Value(1), Value()}), Row({Value(1), Value(7)})};
  const auto key = ColumnValueExp(ColumnName("f", "k"));
  const auto left_count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, key);
  const auto right_count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp(ColumnName("d", "v")));
  for (const auto kind : {JoinType::kInner, JoinType::kLeft}) {
    relational_detail::GroupedJoinFusionSpec fusion;
    fusion.group_keys = {key};
    fusion.select_items = {key, left_count, right_count};
    fusion.select_names = {"k", "l", "r"};
    fusion.aggregates = {left_count.get(), right_count.get()};
    fusion.count_star = {false, false};
    fusion.eligible = true;
    const std::vector<Expression> predicates{BinaryExpressionExp(
        key, BinaryOperation::kEquals, ColumnValueExp(ColumnName("d", "k")))};
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, kind, nullptr, {}, &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    ASSERT_TRUE(output.has_value());
    if (!output) {
      return;
    }
    std::sort(output->rows.begin(), output->rows.end(),
              [](const Row& a, const Row& b) {
                return a[0].value.int_value < b[0].value.int_value;
              });
    std::vector<Row> expected{Row({Value(1), Value(2), Value(1)})};
    if (kind == JoinType::kLeft) {
      expected.emplace_back(std::vector<Value>{Value(2), Value(1), Value(0)});
    }
    EXPECT_EQ(output->rows, expected);
  }
}

TEST_F(RelationalRegressionTest, AuditWindowQuantilesOrderNaNsConsistently) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          setup, Schema("AuditQuantiles", {Column("v", ValueType::kDouble)})));
  for (int64_t i = 0; i < 1024; ++i) {
    const double value = i % 3 == 0 ? std::numeric_limits<double>::quiet_NaN()
                                    : static_cast<double>(i);
    ASSERT_SUCCESS(table.Insert(setup.txn_, Row({Value(value)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  const auto rows = RelationalRun(
      *rs_,
      "SELECT APPROX_QUANTILES(v, 4) OVER () FROM AuditQuantiles LIMIT 1");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_TRUE(rows[0][0].IsArray());
  const auto& values = rows[0][0].ArrayElements();
  ASSERT_EQ(values.size(), 5U);
  EXPECT_TRUE(std::isnan(values.front().value.double_value));
  EXPECT_EQ(values.back(), Value(1022.0));
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_LE(CompareForOrderBy(values[i - 1], values[i]), 0);
  }
}

TEST_F(RelationalRegressionTest, AuditDirectWindowQuantilesUseTotalOrder) {
  auto context = rs_->BeginContext();
  std::vector<Row> rows;
  rows.reserve(1024);
  for (int64_t i = 0; i < 1024; ++i) {
    rows.emplace_back(
        Row({Value(i % 3 == 0 ? std::numeric_limits<double>::quiet_NaN()
                              : static_cast<double>(i))}));
  }
  const Schema schema("q", {Column("v", ValueType::kDouble)});
  WindowFunctionCallExpression window;
  window.function = "APPROX_QUANTILES";
  window.args = {ColumnValueExp("v"), ConstantValueExp(Value(4))};
  std::vector<Value> output;
  ASSERT_EQ(relational_detail::ComputeOneWindow(context, window, rows, schema,
                                                nullptr, {}, &output),
            Status::kSuccess);
  ASSERT_EQ(output.size(), rows.size());
  const Value expected =
      Value::Array({Value(std::numeric_limits<double>::quiet_NaN()),
                    Value(std::numeric_limits<double>::quiet_NaN()),
                    Value(254.0), Value(638.0), Value(1022.0)},
                   "FLOAT64");
  for (const Value& quantiles : output) {
    EXPECT_EQ(quantiles, expected);
  }
}

TEST_F(RelationalRegressionTest, AuditSubqueryStructEscapesFieldContents) {
  const Row injected({Value(R"(x","f2":"y)")});
  const Row two_fields({Value("x"), Value("y")});
  const Value single = relational_detail::ProjectSubqueryRow(injected, true);
  const Value pair = relational_detail::ProjectSubqueryRow(two_fields, true);
  EXPECT_NE(single, pair);
  EXPECT_EQ(single, Value("{\"f1\":\"x\\\",\\\"f2\\\":\\\"y\"}"));
}

TEST_F(RelationalRegressionTest, AuditWindowStatisticsRejectNonNumericValues) {
  auto context = rs_->BeginContext();
  const Schema schema("s", {Column("v", ValueType::kVarChar)});
  std::vector<Row> rows{Row({Value("12")}), Row({Value("34")})};
  for (const std::string function :
       {"APPROX_TOP_SUM", "VAR_POP", "STDDEV_POP", "CORR"}) {
    SCOPED_TRACE(function);
    WindowFunctionCallExpression window;
    window.function = function;
    window.args = {ColumnValueExp("v")};
    if (function == "APPROX_TOP_SUM" || function == "CORR") {
      window.args.push_back(ColumnValueExp("v"));
    }
    if (function == "APPROX_TOP_SUM") {
      window.args.push_back(ConstantValueExp(Value(2)));
    }
    std::vector<Value> output;
    EXPECT_NE(relational_detail::ComputeOneWindow(context, window, rows, schema,
                                                  nullptr, {}, &output),
              Status::kSuccess);
  }
}

TEST_F(RelationalRegressionTest, AuditFilteredInfinityCannotMaskSumOverflow) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          setup, Schema("AuditInfGroups", {Column("g", ValueType::kVarChar),
                                           Column("keep", ValueType::kInt64),
                                           Column("v", ValueType::kDouble)})));
  for (int64_t i = 0; i < 33000; ++i) {
    const double value =
        i == 0 ? std::numeric_limits<double>::infinity() : 1e308;
    ASSERT_TRUE(
        table
            .Insert(setup.txn_,
                    Row({Value("group"), Value(i == 0 ? 0 : 1), Value(value)}))
            .HasValue());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  RelationalRun(*rs_, "ANALYZE AuditInfGroups");
  SqlEngine engine(*rs_);
  auto context = rs_->BeginReadOnlyContext();
  auto prepared = engine.Prepare(
      context,
      "SELECT g,SUM(v),SUM(v) FROM AuditInfGroups WHERE keep=1 GROUP BY g");
  ASSERT_TRUE(prepared.HasValue()) << prepared.GetStatus();
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
}

TEST_F(RelationalRegressionTest, AuditRowFillWithVarcharRemainsReusable) {
  RelationalRun(*rs_, "CREATE TABLE AuditRowArena (g INT64,s STRING,v DOUBLE)");
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        setup.GetTable("AuditRowArena"));
  for (int64_t i = 0; i < 33000; ++i) {
    ASSERT_TRUE(
        table
            ->Insert(setup.txn_, Row({Value(i % 2), Value(i % 2 ? "yes" : "no"),
                                      Value(1.0)}))
            .HasValue());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  const std::string branch =
      "SELECT g,SUM(v) AS total FROM AuditRowArena WHERE s='yes' GROUP BY g";
  const auto rows = RelationalRun(*rs_, branch + " UNION ALL " + branch);
  EXPECT_EQ(rows, std::vector<Row>({Row({Value(1), Value(16500.0)}),
                                    Row({Value(1), Value(16500.0)})}));
}

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
      context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
}

}  // namespace
}  // namespace tinylamb
