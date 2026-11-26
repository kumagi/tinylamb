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
#include "executor/detail/build_input_internal.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/relational_regression_test.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, LargeBuildGroupsMergeParallelAggregateStates) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64)});
  constexpr int64_t kGroups = 8192;
  for (int64_t k = 0; k < kGroups; ++k) {
    right.rows.emplace_back(std::vector<Value>{Value(k)});
  }
  for (int repeat = 0; repeat < 8; ++repeat) {
    for (int64_t k = 0; k < kGroups; ++k) {
      left.rows.emplace_back(std::vector<Value>{
          Value(k), repeat % 2 == 0 || k == 0 ? Value() : Value(2.5)});
    }
  }
  const auto key = ColumnValueExp(ColumnName("d", "k"));
  const auto value = ColumnValueExp(ColumnName("f", "v"));
  const auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, value);
  const auto avg =
      std::make_shared<AggregateExpression>(AggregationType::kAvg, value);
  const auto count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, value);
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {key};
  fusion.aggregates = {sum.get(), avg.get(), count.get()};
  fusion.count_star = {false, false, false};
  fusion.select_items = {key, sum, avg, count};
  fusion.select_names = {"k", "s", "a", "n"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      ColumnValueExp(ColumnName("f", "k")), BinaryOperation::kEquals, key)};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "grouped join declined";
    }
    ASSERT_EQ(output->rows.size(), static_cast<size_t>(kGroups));
    for (const auto& row : output->rows) {
      const bool all_null = row[0] == Value(0);
      EXPECT_EQ(row[1], all_null ? Value() : Value(10.0));
      EXPECT_EQ(row[2], all_null ? Value() : Value(2.5));
      EXPECT_EQ(row[3], Value(all_null ? 0 : 4));
    }
  }
  for (const bool multiply : {false, true}) {
    const auto argument =
        multiply ? BinaryExpressionExp(value, BinaryOperation::kMultiply, value)
                 : value;
    const auto single_sum =
        std::make_shared<AggregateExpression>(AggregationType::kSum, argument);
    fusion.aggregates = {single_sum.get()};
    fusion.count_star = {false};
    fusion.select_items = {key, single_sum};
    fusion.select_names = {"k", "s"};
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kInner, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      if (!output) {
        FAIL() << "single SUM grouped join declined";
      }
      ASSERT_EQ(output->rows.size(), static_cast<size_t>(kGroups));
      for (const auto& row : output->rows) {
        EXPECT_EQ(row[1],
                  row[0] == Value(0) ? Value() : Value(multiply ? 25.0 : 10.0));
      }
    }
  }
  const std::array<double, 8> ordered_values{1e16, 1.0, -1e16, 1.0,
                                             0.0,  0.0, 0.0,   0.0};
  for (size_t repeat = 0; repeat < ordered_values.size(); ++repeat) {
    left.rows[(repeat * static_cast<size_t>(kGroups)) + 1].values_[1] =
        Value(ordered_values[repeat]);
  }
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = {key, sum};
  fusion.select_names = {"k", "s"};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "ordered SUM grouped join declined";
    }
    for (const auto& row : output->rows) {
      if (row[0] == Value(1)) {
        EXPECT_EQ(row[1], Value(1.0));
      }
    }
  }
}

TEST_F(RelationalRegressionTest,
       LargeOrderedFilterPreservesDuplicatesAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE OrderedFilterDim(k INT64,keep INT64)");
  RelationalRun(*rs_, "CREATE TABLE OrderedFilterFact(k INT64,p INT64)");
  RelationalRun(*rs_, "CREATE TABLE OrderedFilterPad(k INT64)");
  constexpr int64_t kDimension = 40000;
  constexpr int64_t kFact = 160000;
  int64_t expected_count = 0, expected_matched = 0;
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          context.GetTable("OrderedFilterDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          context.GetTable("OrderedFilterFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, pad,
                          context.GetTable("OrderedFilterPad"));
    for (int64_t k = 0; k < kDimension; ++k) {
      ASSERT_SUCCESS(
          dim->Insert(context.txn_, Row({Value(k), Value(k % 8 == 7 ? 1 : 0)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        dim->Insert(context.txn_, Row({Value(7), Value(1)})).GetStatus());
    ASSERT_SUCCESS(
        dim->Insert(context.txn_, Row({Value(), Value(1)})).GetStatus());
    for (int64_t k = 0; k < 8; ++k) {
      ASSERT_SUCCESS(pad->Insert(context.txn_, Row({Value(k)})).GetStatus());
    }
    for (int64_t i = 0; i < kFact; ++i) {
      const bool null_key = i % 101 == 0;
      const bool missing = i % 127 == 0;
      const bool null_pad = i % 107 == 0;
      const int64_t key = missing ? kDimension + 1000 : i % kDimension;
      ASSERT_SUCCESS(
          fact->Insert(context.txn_, Row({null_key ? Value() : Value(key),
                                          null_pad ? Value() : Value(i % 17)}))
              .GetStatus());
      if (!null_key && !missing && key % 8 == 7) {
        const int64_t copies = key == 7 ? 2 : 1;
        expected_count += copies;
        if (!null_pad && i % 17 < 8) {
          expected_matched += copies;
        }
      }
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT COUNT(*),COUNT(p.k) FROM OrderedFilterFact f "
      "JOIN OrderedFilterDim d ON f.k=d.k LEFT JOIN OrderedFilterPad p ON "
      "f.p=p.k "
      "WHERE d.keep=1";
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql),
              std::vector<Row>(
                  {Row({Value(expected_count), Value(expected_matched)})}));
    const auto profile = ExplainAnalyze(*rs_, sql);
    const std::string field = "scan_output_rows=";
    const auto position = profile.find(field);
    ASSERT_NE(position, std::string::npos) << profile;
    EXPECT_LT(std::stoull(profile.substr(position + field.size())), 100000U)
        << profile;
  }
}

TEST_F(RelationalRegressionTest, RelativeDimensionPreloadKeepsDeferredFact) {
  RelationalRun(*rs_,
                "CREATE TABLE PreloadDim (k INT64,g INT64,label STRING(MAX))");
  RelationalRun(*rs_, "CREATE TABLE PreloadDate (k INT64,y INT64,z INT64)");
  RelationalRun(*rs_, "CREATE TABLE PreloadFact (dk INT64,dt INT64,v INT64)");
  constexpr int64_t kDim = 40000;
  constexpr int64_t kFact = kDim * 4;
  constexpr std::array<std::string_view, 3> kLabels{"0", "1", "2"};
  std::map<std::pair<int64_t, std::string_view>, int64_t> counts;
  std::map<std::tuple<int64_t, std::string_view, int64_t>, int64_t> wide_counts;
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          context.GetTable("PreloadDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, date,
                          context.GetTable("PreloadDate"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          context.GetTable("PreloadFact"));
    for (int64_t i = 0; i < kDim; ++i) {
      ASSERT_TRUE(dim->Insert(context.txn_, Row({Value(i), Value(i % 7),
                                                 Value(std::to_string(i % 3))}))
                      .HasValue());
    }
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
          date->Insert(context.txn_, Row({Value(i), Value(i / 10),
                                          i % 3 == 0 ? Value() : Value(i % 3)}))
              .HasValue());
    }
    for (int64_t i = 0; i < kFact; ++i) {
      const bool null_key = i % 101 == 0;
      const int64_t key = i % kDim;
      ASSERT_TRUE(
          fact->Insert(context.txn_, Row({null_key ? Value() : Value(key),
                                          Value(i % 100), Value(1)}))
              .HasValue());
      if (!null_key && (i % 100) / 10 == 1) {
        const auto label = kLabels[static_cast<size_t>(key % 3)];
        ++counts[{key % 7, label}];
        ++wide_counts[{key % 7, label,
                       ((i % 100) % 3 == 0 ? -1 : (i % 100) % 3)}];
      }
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,d.label,t.y,SUM(f.v) FROM PreloadDim d,PreloadFact "
      "f,PreloadDate t "
      "WHERE d.k=f.dk AND t.k=f.dt AND t.y=1 GROUP BY d.g,d.label,t.y "
      "ORDER BY d.g,d.label,t.y";
  std::vector<Row> expected;
  expected.reserve(counts.size());
  for (const auto& [key, count] : counts) {
    expected.emplace_back(Row({Value(key.first), Value(std::string(key.second)),
                               Value(1), Value(count)}));
  }
  const std::string wide_sql =
      "SELECT d.g,d.label,t.y,t.z,SUM(f.v) FROM PreloadDim d,PreloadFact "
      "f,PreloadDate t "
      "WHERE d.k=f.dk AND t.k=f.dt AND t.y=1 GROUP BY d.g,d.label,t.y,t.z "
      "ORDER BY d.g,d.label,t.y,t.z";
  const auto wide_expected = [&] {
    std::vector<Row> rows;
    rows.reserve(wide_counts.size());
    for (const auto& [key, count] : wide_counts) {
      rows.emplace_back(Row(
          {Value(std::get<0>(key)), Value(std::string(std::get<1>(key))),
           Value(1), std::get<2>(key) == -1 ? Value() : Value(std::get<2>(key)),
           Value(count)}));
    }
    return rows;
  };
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql), expected);
    EXPECT_EQ(RelationalRun(*rs_, wide_sql), wide_expected());
    const auto profile = ExplainAnalyze(*rs_, wide_sql);
    const std::string field = "scan_rows=";
    const auto position = profile.find(field);
    ASSERT_NE(position, std::string::npos) << profile;
    EXPECT_LT(std::stoull(profile.substr(position + field.size())),
              static_cast<uint64_t>(kDim + 1000))
        << profile;
  }
  // A duplicate dimension key must retain join multiplicity via fallback.
  RelationalRun(*rs_, "INSERT INTO PreloadDate VALUES (10,1,1)");
  for (int64_t i = 10; i < kFact; i += 100) {
    if (i % 101 != 0) {
      const int64_t key = i % kDim;
      ++wide_counts[{key % 7, std::to_string(key % 3), 1}];
    }
  }
  EXPECT_EQ(RelationalRun(*rs_, wide_sql), wide_expected());
}

TEST_F(RelationalRegressionTest, RawPartitionMergeKeepsCompositeSums) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                             Column(ColumnName("f", "g"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("x")}), Row({Value(1), Value("x")}),
                Row({Value(2), Value()})};
  constexpr int64_t kGroups = 32768;
  for (int64_t repeat = 0; repeat < 4; ++repeat) {
    for (int64_t i = 0; i < kGroups; ++i) {
      left.rows.emplace_back(Row(
          {Value((i % 2) + 1), Value(i), i % 5 == 0 ? Value() : Value(0.25)}));
    }
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "g"), col("d", "s")};
  fusion.aggregates = {sum.get(), count.get()};
  fusion.count_star = {false, false};
  fusion.select_items = {col("f", "g"), col("d", "s"), sum, count};
  fusion.select_names = {"g", "s", "sum", "count"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "expected specialized aggregation";
    }
    ASSERT_EQ(output->rows.size(), static_cast<size_t>(kGroups));
    EXPECT_EQ(runtime.partitioned_aggregate_merges, workers == 1 ? 0U : 1U);
    std::vector<bool> seen(kGroups, false);
    for (const auto& row : output->rows) {
      const int64_t group = row[0].value.int_value;
      ASSERT_GE(group, 0);
      ASSERT_LT(group, kGroups);
      EXPECT_FALSE(seen[static_cast<size_t>(group)]);
      seen[static_cast<size_t>(group)] = true;
      const bool null_value = group % 5 == 0;
      const int64_t multiplicity = group % 2 == 0 ? 2 : 1;
      EXPECT_EQ(row[1], group % 2 == 0 ? Value("x") : Value());
      EXPECT_EQ(row[2], null_value ? Value()
                                   : Value(static_cast<double>(multiplicity)));
      EXPECT_EQ(row[3], Value(null_value ? 0 : multiplicity * 4));
    }
  }
}

TEST_F(RelationalRegressionTest, FusedSplitKeysMergeAcrossWorkers) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kVarChar),
                   Column(ColumnName("d", "t"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("same"), Value("r")}),
                Row({Value(2), Value("same"), Value("r")}),
                Row({Value(3), Value(), Value("r")}),
                Row({Value(3), Value(), Value("r")})};
  const auto wide = std::numeric_limits<int64_t>::max();
  const std::vector<Row> pattern{Row({Value(1), Value(0), Value(10)}),
                                 Row({Value(2), Value(0), Value(20)}),
                                 Row({Value(3), Value(0), Value(4)}),
                                 Row({Value(4), Value(0), Value(7)}),
                                 Row({Value(1), Value(), Value(2)}),
                                 Row({Value(2), Value(), Value(3)}),
                                 Row({Value(1), Value(wide), Value(6)})};
  constexpr int64_t kRepeats = 8192;
  for (int64_t i = 0; i < kRepeats; ++i) {
    left.rows.insert(left.rows.end(), pattern.begin(), pattern.end());
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "s"), col("d", "t")};
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = fusion.group_keys;
  fusion.select_items.push_back(sum);
  fusion.select_names = {"p", "s", "t", "n"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  std::vector<Row> expected{
      Row({Value(), Value("same"), Value("r"), Value(5 * kRepeats)}),
      Row({Value(0), Value(), Value(), Value(7 * kRepeats)}),
      Row({Value(0), Value(), Value("r"), Value(8 * kRepeats)}),
      Row({Value(0), Value("same"), Value("r"), Value(30 * kRepeats)}),
      Row({Value(wide), Value("same"), Value("r"), Value(6 * kRepeats)})};
  const auto verify = [&] {
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kLeft, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      if (!output) {
        FAIL() << "fused split-key aggregation declined";
      }
      ASSIGN_OR_ASSERT_FAIL(Relation, materialized,
                            relational_detail::MaterializeRelation(*output));
      *output = std::move(materialized);
      ASSERT_EQ(output->rows.size(), expected.size());
      for (const auto& row : expected) {
        EXPECT_TRUE(std::ranges::any_of(output->rows, [&](const Row& actual) {
          if (actual.Count() != row.Count()) {
            return false;
          }
          for (size_t column = 0; column < row.Count(); ++column) {
            if (!relational_detail::DistinctValueEqual{}(
                    relational_detail::CanonicalDistinctValue(actual[column]),
                    relational_detail::CanonicalDistinctValue(row[column]))) {
              return false;
            }
          }
          return true;
        }));
      }
    }
  };
  verify();
  // NULL and out-of-range probe keys use the fallback map even when the
  // dimension has no row representation. Keep its distinct dimension keys.
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, columnar_right,
      relational_detail::BuildColumnarTableFromRows(right.schema, right.rows));
  auto saved_right = std::move(right.rows);
  right.columnar = std::move(columnar_right);
  verify();
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, columnar_left,
      relational_detail::BuildColumnarTableFromRows(left.schema, left.rows));
  auto saved_left = std::move(left.rows);
  left.columnar = std::move(columnar_left);
  verify();
  left.columnar.reset();
  left.rows = std::move(saved_left);
  right.columnar.reset();
  right.rows = std::move(saved_right);
  const double nan = std::numeric_limits<double>::quiet_NaN();
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kDouble),
                   Column(ColumnName("d", "t"), ValueType::kVarChar)});
  right.rows[0].values_[1] = Value(nan);
  right.rows[1].values_[1] = Value(-nan);
  right.rows[0].values_[2] = Value("UPPER").WithCollation(2);
  right.rows[1].values_[2] = Value("upper").WithCollation(2);
  for (const size_t index : {size_t{0}, size_t{3}, size_t{4}}) {
    expected[index].values_[1] = Value(nan);
    expected[index].values_[2] = Value("upper").WithCollation(2);
  }
  verify();
}

TEST_F(RelationalRegressionTest, ColumnarSplitKeysKeepFallbackDimension) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("a")}), Row({Value(2), Value("b")})};
  const auto wide = std::numeric_limits<int64_t>::max();
  constexpr int64_t kRepeats = 4096;
  for (int64_t i = 0; i < kRepeats; ++i) {
    for (const auto& key : {Value(), Value(0), Value(wide)}) {
      left.rows.emplace_back(std::vector<Value>{Value(1), key, Value(2.0)});
      left.rows.emplace_back(std::vector<Value>{Value(2), key, Value(3.0)});
    }
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "s")};
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = fusion.group_keys;
  fusion.select_items.push_back(sum);
  fusion.select_names = {"p", "s", "n"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, build,
      relational_detail::BuildColumnarTableFromRows(right.schema, right.rows));
  right.columnar = std::move(build);
  right.rows.clear();
  for (const bool columnar_probe : {false, true}) {
    if (columnar_probe) {
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<relational_detail::ColumnarTable>,
                            probe,
                            relational_detail::BuildColumnarTableFromRows(
                                left.schema, left.rows));
      left.columnar = std::move(probe);
      left.rows.clear();
    }
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kInner, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      ASSERT_TRUE(output.has_value());
      ASSIGN_OR_ASSERT_FAIL(Relation, rows,
                            relational_detail::MaterializeRelation(*output));
      SCOPED_TRACE(::testing::Message()
                   << "columnar=" << columnar_probe << " workers=" << workers);
      EXPECT_EQ(rows.rows.size(), 6U) << ::testing::PrintToString(rows.rows);
      for (const auto& key : {Value(), Value(0), Value(wide)}) {
        for (const auto& label : {"a", "b"}) {
          const double total =
              (std::string_view(label) == "a" ? 2.0 : 3.0) * kRepeats;
          EXPECT_TRUE(std::ranges::any_of(rows.rows, [&](const Row& row) {
            return relational_detail::DistinctValueEqual{}(row[0], key) &&
                   row[1] == Value(label) && row[2] == Value(total);
          }));
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, FusedStringGroupHashesKeepCanonicalPeers) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema =
      Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                   Column(ColumnName("f", "label"), ValueType::kVarChar),
                   Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "label"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("dimension with shared long text")}),
                Row({Value(2), Value("dimension with shared long text")})};
  const std::vector<Value> labels{Value("1-0 0 0:0:0"),
                                  Value("01-00 0 00:00:00"),
                                  Value("shared long string grouping key"),
                                  Value(),
                                  Value("UPPER").WithCollation(2),
                                  Value("upper").WithCollation(2)};
  left.rows.reserve(49152);
  for (size_t i = 0; i < 49152; ++i) {
    left.rows.emplace_back(
        std::vector<Value>{Value(static_cast<int64_t>(i % 2) + 1),
                           labels[i % labels.size()], Value(1)});
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "label"), col("d", "label")};
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = {fusion.group_keys[0], fusion.group_keys[1], sum};
  fusion.select_names = {"a", "b", "s"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "string-key grouped join declined";
    }
    ASSERT_EQ(output->rows.size(), 5U);
    for (const auto& row : output->rows) {
      const bool single = !row[0].IsCaseInsensitive();
      EXPECT_EQ(row[2], Value(single ? 8192 : 16384));
      EXPECT_EQ(row[1], right.rows[0][1]);
    }
  }
}

TEST_F(RelationalRegressionTest, FusedCountHavingKeepsAllComparisonsAndNulls) {
  for (const std::string op : {"=", "!=", "<", "<=", ">", ">="}) {
    const std::string join =
        " FROM SmallKeys d LEFT JOIN AggTable a ON d.k=a.k AND a.k<3 ";
    std::string reference = "SELECT * FROM (SELECT d.k,COUNT(a.k) AS n";
    reference.append(join)
        .append("GROUP BY d.k) t WHERE n ")
        .append(op)
        .append(" 10 ORDER BY k");
    std::string fused = "SELECT d.k,COUNT(a.k) AS n";
    fused.append(join)
        .append("GROUP BY d.k HAVING COUNT(a.k) ")
        .append(op)
        .append(" 10 ORDER BY d.k");
    const auto expected = RelationalRun(*rs_, reference);
    EXPECT_EQ(RelationalRun(*rs_, fused), expected);
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT d.k,COUNT(*) AS n FROM SmallKeys d,AggTable a "
                      "WHERE d.k=a.k GROUP BY d.k HAVING COUNT(*) " +
                          op + " 20 ORDER BY d.k"),
        RelationalRun(*rs_,
                      "SELECT * FROM (SELECT d.k,COUNT(*) AS n FROM SmallKeys "
                      "d,AggTable a "
                      "WHERE d.k=a.k GROUP BY d.k) t WHERE n " +
                          op + " 20 ORDER BY k"));
  }
}

TEST_F(RelationalRegressionTest, CompactGroupingWithoutAggregatesKeepsPeers) {
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT k,v FROM (
      SELECT k,v FROM (SELECT 1 AS k,'a' AS v UNION ALL SELECT 1,'a'
                       UNION ALL SELECT 2,NULL UNION ALL SELECT 2,NULL
                       UNION ALL SELECT NULL,'b') t GROUP BY k,v
    ) g ORDER BY k NULLS FIRST
  )sql"),
      std::vector<Row>({Row({Value(), Value("b")}), Row({Value(1), Value("a")}),
                        Row({Value(2), Value()})}));
  EXPECT_TRUE(
      RelationalRun(
          *rs_, "SELECT k FROM (SELECT k FROM AggTable WHERE k<0 GROUP BY k) g")
          .empty());
}

TEST(RelationalPlanningTest, OuterJoinReductionResolvesUnqualifiedColumns) {
  std::vector<relational_detail::Relation> schemas(2);
  schemas[0].schema = Schema("l", {Column("lk", ValueType::kInt64)});
  schemas[1].schema = Schema("r", {Column("rk", ValueType::kInt64)});
  SelectStatement statement;
  SelectSource lhs;
  lhs.table = "l";
  SelectSource rhs;
  rhs.table = "r";
  rhs.join_type = JoinType::kLeft;
  statement.SetSources({lhs, rhs});
  statement.SetWhereClause(BinaryExpressionExp(ColumnValueExp("rk"),
                                               BinaryOperation::kGreaterThan,
                                               ConstantValueExp(Value(0))));
  bool reduced = false;
  auto sources =
      relational_detail::ReduceOuterJoinsToInner(statement, schemas, &reduced);
  EXPECT_TRUE(reduced);
  EXPECT_EQ(sources[1].join_type, JoinType::kInner);
  statement.SetWhereClause(
      UnaryExpressionExp(ColumnValueExp("rk"), UnaryOperation::kIsNull));
  sources =
      relational_detail::ReduceOuterJoinsToInner(statement, schemas, &reduced);
  EXPECT_FALSE(reduced);
  EXPECT_EQ(sources[1].join_type, JoinType::kLeft);
  // An ambiguous bare reference must never justify a rewrite.
  schemas[0].schema = Schema("l", {Column("rk", ValueType::kInt64)});
  statement.SetWhereClause(BinaryExpressionExp(ColumnValueExp("rk"),
                                               BinaryOperation::kGreaterThan,
                                               ConstantValueExp(Value(0))));
  sources =
      relational_detail::ReduceOuterJoinsToInner(statement, schemas, &reduced);
  EXPECT_FALSE(reduced);
  EXPECT_EQ(sources[1].join_type, JoinType::kLeft);
}

TEST(RelationalPlanningTest, JoinKeyEncodingMatchesValueEncoding) {
  const std::vector<Value> values = {
      Value(),
      Value(std::numeric_limits<int64_t>::min()),
      Value(std::numeric_limits<int64_t>::max()),
      Value(std::numeric_limits<int64_t>::max()).WithUnsigned(),
      Value::DateFromDays(42),
      Value(""),
      Value("a"),
      Value("abcdefgh"),
      Value("abcdefghi"),
      Value("abcdefghijklmnop"),
      Value(1.25),
      Value(-1.25),
      Value(0.0),
      Value(std::numeric_limits<double>::quiet_NaN()),
      Value::Array({Value(int64_t{1}), Value()}, "INT64")};
  for (const Value& value : values) {
    const Row input({value, Value("tail")});
    const Row expected({value, Value("tail")});
    EXPECT_EQ(relational_detail::EncodeJoinKey(input, {0, 1}),
              expected.EncodeMemcomparableFormat());
  }
  const Row negative_zero({Value(-0.0), Value("tail")});
  const Row positive_zero({Value(0.0), Value("tail")});
  EXPECT_EQ(relational_detail::EncodeJoinKey(negative_zero, {0, 1}),
            positive_zero.EncodeMemcomparableFormat());
}

TEST(RelationalPlanningTest, EstimateJoinRowsHandlesScalarKeyDomains) {
  const std::vector<Expression> int_keys = {BinaryExpressionExp(
      ColumnValueExp("key"), BinaryOperation::kEquals, ColumnValueExp("key"))};
  relational_detail::Relation int_left;
  int_left.schema = Schema("left", {Column("key", ValueType::kInt64)});
  int_left.AddRow(Row({Value(int64_t{1})}));
  int_left.AddRow(Row({Value(int64_t{2})}));
  int_left.AddRow(Row({Value(int64_t{2})}));
  int_left.AddRow(Row({Value()}));
  relational_detail::Relation int_right;
  int_right.schema = Schema("right", {Column("key", ValueType::kInt64)});
  int_right.AddRow(Row({Value(int64_t{2})}));
  int_right.AddRow(Row({Value(int64_t{2})}));
  int_right.AddRow(Row({Value(int64_t{3})}));
  int_right.AddRow(Row({Value()}));
  EXPECT_EQ(relational_detail::EstimateJoinRows(int_left, int_right, int_keys),
            4U);

  const std::vector<Expression> string_keys = {BinaryExpressionExp(
      ColumnValueExp("key"), BinaryOperation::kEquals, ColumnValueExp("key"))};
  relational_detail::Relation string_left;
  string_left.schema = Schema("left", {Column("key", ValueType::kVarChar)});
  string_left.AddRow(Row({Value("a")}));
  string_left.AddRow(Row({Value("b")}));
  string_left.AddRow(Row({Value("b")}));
  string_left.AddRow(Row({Value()}));
  relational_detail::Relation string_right;
  string_right.schema = Schema("right", {Column("key", ValueType::kVarChar)});
  string_right.AddRow(Row({Value("b")}));
  string_right.AddRow(Row({Value("b")}));
  string_right.AddRow(Row({Value("c")}));
  string_right.AddRow(Row({Value()}));
  EXPECT_EQ(relational_detail::EstimateJoinRows(string_left, string_right,
                                                string_keys),
            4U);
}

TEST_F(RelationalRegressionTest,
       FlatIntegerJoinHandlesCollisionsAndDuplicateOrder) {
  TransactionContext context = rs_->BeginContext();
  relational_detail::Relation left;
  left.schema = Schema("left", {Column("left_key", ValueType::kInt64)});
  left.AddRow(Row({Value(int64_t{1})}));
  relational_detail::Relation right;
  right.schema = Schema("right", {Column("right_key", ValueType::kInt64),
                                  Column("tag", ValueType::kVarChar)});
  right.AddRow(Row({Value(int64_t{1}), Value("first")}));
  right.AddRow(Row({Value(int64_t{1}), Value("second")}));
  right.AddRow(Row({Value(int64_t{9}), Value("collision")}));
  ASSERT_EQ(left.rows.size(), 1U);
  ASSERT_EQ(right.rows.size(), 3U);
  ASSERT_FALSE(right.HasSpill());
  const std::vector<Expression> predicates = {
      BinaryExpressionExp(ColumnValueExp("left_key"), BinaryOperation::kEquals,
                          ColumnValueExp("right_key"))};
  const std::vector<relational_detail::EqualityKey> equality_keys =
      relational_detail::EqualityKeys(left.schema, right.schema, predicates);
  ASSERT_EQ(equality_keys.size(), 1U);
  EXPECT_EQ(equality_keys[0].left, 0U);
  EXPECT_EQ(equality_keys[0].right, 0U);
  StatusOr<relational_detail::Relation> joined = relational_detail::InnerJoin(
      context, std::move(left), std::move(right), predicates, nullptr, {});
  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  EXPECT_EQ(joined.Value().join_comparisons, 2U);
  ASSERT_EQ(joined.Value().rows.size(), 2U);
  EXPECT_EQ(joined.Value().rows[0][2], Value("second"));
  EXPECT_EQ(joined.Value().rows[1][2], Value("first"));
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ParallelOuterProbeKeepsEveryUnmatchedBuildRow) {
  auto context = rs_->BeginContext();
  constexpr size_t kRepeats = 8192;
  for (const int key_shape : {0, 1, 2}) {
    const bool text = key_shape == 2;
    const ValueType key_type = text ? ValueType::kVarChar : ValueType::kInt64;
    const auto key = [text](int64_t value) {
      return text ? Value(std::to_string(value)) : Value(value);
    };
    for (const JoinType kind : {JoinType::kFull, JoinType::kRight}) {
      for (const size_t workers : {size_t{1}, size_t{2}}) {
        relational_detail::Relation left, right;
        left.schema =
            Schema("l", {Column("lk", key_type), Column("ld", ValueType::kDate),
                         Column("lv", ValueType::kInt64)});
        right.schema =
            Schema("r", {Column("rk", key_type), Column("rd", ValueType::kDate),
                         Column("rv", ValueType::kInt64)});
        const Value day = Value::DateFromDays(-10);
        for (size_t i = 0; i < kRepeats; ++i) {
          left.rows.emplace_back(Row({key(1), day, Value(3)}));
          left.rows.emplace_back(Row({key(1), day, Value(7)}));
          left.rows.emplace_back(Row({Value(), Value(), Value(0)}));
          left.rows.emplace_back(Row({key(3), day, Value(0)}));
        }
        right.rows = {
            Row({key(1), day, Value(5)}), Row({key(1), day, Value(9)}),
            Row({Value(), Value(), Value(0)}), Row({key(4), day, Value(0)})};
        SelectSource source;
        source.join_type = kind;
        source.join_condition =
            BinaryExpressionExp(ColumnValueExp("lk"), BinaryOperation::kEquals,
                                ColumnValueExp("rk"));
        if (key_shape != 0) {
          source.join_condition =
              BinaryExpressionExp(source.join_condition, BinaryOperation::kAnd,
                                  BinaryExpressionExp(ColumnValueExp("ld"),
                                                      BinaryOperation::kEquals,
                                                      ColumnValueExp("rd")));
        }
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        auto result = relational_detail::Join(
            context, std::move(left), std::move(right), source, nullptr, {});
        ASSERT_TRUE(result.HasValue()) << result.GetStatus();
        const size_t unmatched_left =
            kind == JoinType::kFull ? 2 * kRepeats : 0;
        EXPECT_EQ(result.Value().TotalRows(),
                  (4 * kRepeats) + unmatched_left + 2);
        EXPECT_EQ(result.Value().join_comparisons, 4 * kRepeats);
        size_t left_nulls = 0;
        size_t right_nulls = 0;
        size_t fives = 0;
        size_t nines = 0;
        for (const Row& row : result.Value().rows) {
          left_nulls += static_cast<size_t>(row[2].IsNull());
          right_nulls += static_cast<size_t>(row[5].IsNull());
          fives += static_cast<size_t>(row[5] == Value(5));
          nines += static_cast<size_t>(row[5] == Value(9));
        }
        EXPECT_EQ(left_nulls, 2U);
        EXPECT_EQ(right_nulls, unmatched_left);
        EXPECT_EQ(fives, 2 * kRepeats);
        EXPECT_EQ(nines, 2 * kRepeats);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, CompositeDateFullJoinKeepsNullsAndResiduals) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left;
  left.schema = Schema(
      "l", {Column("lk", ValueType::kInt64), Column("ld", ValueType::kDate),
            Column("lv", ValueType::kInt64)});
  relational_detail::Relation right;
  right.schema = Schema(
      "r", {Column("rk", ValueType::kInt64), Column("rd", ValueType::kDate),
            Column("rv", ValueType::kInt64)});
  const Value day = Value::DateFromDays(-10);
  const Value other = Value::DateFromDays(20);
  left.AddRow(Row({Value(1), day, Value(3)}));
  left.AddRow(Row({Value(1), day, Value(7)}));
  left.AddRow(Row({Value(2), Value(), Value(0)}));
  left.AddRow(Row({Value(3), other, Value(0)}));
  right.AddRow(Row({Value(1), day, Value(5)}));
  right.AddRow(Row({Value(1), day, Value(9)}));
  right.AddRow(Row({Value(2), Value(), Value(0)}));
  right.AddRow(Row({Value(4), other, Value(0)}));
  ASSERT_TRUE(relational_detail::CompositeIntJoinKey(left.schema, {0, 1},
                                                     right.schema, {0, 1}));
  const Schema mixed("mixed", {Column("rk", ValueType::kInt64),
                               Column("rd", ValueType::kInt64)});
  EXPECT_FALSE(relational_detail::CompositeIntJoinKey(left.schema, {0, 1},
                                                      mixed, {0, 1}));
  SelectSource source;
  source.join_type = JoinType::kFull;
  source.join_condition = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("lk"), BinaryOperation::kEquals,
                          ColumnValueExp("rk")),
      BinaryOperation::kAnd,
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("ld"), BinaryOperation::kEquals,
                              ColumnValueExp("rd")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("lv"), BinaryOperation::kLessThan,
                              ColumnValueExp("rv"))));
  auto joined = relational_detail::Join(context, std::move(left),
                                        std::move(right), source, nullptr, {});
  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  const std::vector<Row> expected{
      Row({Value(1), day, Value(3), Value(1), day, Value(5)}),
      Row({Value(1), day, Value(3), Value(1), day, Value(9)}),
      Row({Value(1), day, Value(7), Value(1), day, Value(9)}),
      Row({Value(2), Value(), Value(0), Value(), Value(), Value()}),
      Row({Value(3), other, Value(0), Value(), Value(), Value()}),
      Row({Value(), Value(), Value(), Value(2), Value(), Value(0)}),
      Row({Value(), Value(), Value(), Value(4), other, Value(0)})};
  EXPECT_TRUE(std::ranges::is_permutation(joined.Value().rows, expected));
  EXPECT_EQ(joined.Value().join_comparisons, 4U);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, SmallInnerBuildStreamsEverySpilledProbeRow) {
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  for (int variant = 0; variant < 6; ++variant) {
    SCOPED_TRACE(variant);
    const int shape = variant % 3;  // integer / composite integer / byte key
    auto context = rs_->BeginContext();
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    const ValueType key_type =
        shape == 2 ? ValueType::kVarChar : ValueType::kInt64;
    relational_detail::Relation left(&runtime);
    left.schema =
        Schema("l", {Column("lk", key_type), Column("lk2", ValueType::kInt64)});
    relational_detail::Relation right(&runtime);
    right.schema =
        Schema("r", {Column("rk", key_type), Column("rk2", ValueType::kInt64)});
    const auto key = [&](int n) {
      return shape == 2 ? Value(std::to_string(n)) : Value(n);
    };
    left.AddRow(Row({key(0), Value(9)}));
    left.AddRow(Row({key(1), Value(9)}));
    left.AddRow(Row({Value(), Value(9)}));
    size_t expected = 0;
    for (int i = 0; i < 8192; ++i) {
      const bool is_null = i % 17 == 0;
      right.AddRow(Row({is_null ? Value() : key(i % 5), Value(9)}));
      expected += !is_null && i % 5 < 2 ? 1 : 0;
    }
    right.EnsureSpill();
    ASSERT_SUCCESS(right.FinishSpill());
    ASSERT_TRUE(right.HasSpill());
    std::vector<Expression> predicates{BinaryExpressionExp(
        ColumnValueExp("lk"), BinaryOperation::kEquals, ColumnValueExp("rk"))};
    if (shape == 1) {
      predicates.push_back(BinaryExpressionExp(ColumnValueExp("lk2"),
                                               BinaryOperation::kEquals,
                                               ColumnValueExp("rk2")));
    }
    SelectSource source;
    source.join_type = JoinType::kInner;
    source.join_condition = CombineConjuncts(predicates);
    auto joined =
        variant < 3
            ? relational_detail::InnerJoin(context, std::move(left),
                                           std::move(right), predicates,
                                           nullptr, {})
            : relational_detail::Join(context, std::move(left),
                                      std::move(right), source, nullptr, {});
    ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
    EXPECT_EQ(joined.Value().hybrid_hash_joins, 0U);
    EXPECT_EQ(joined.Value().in_memory_hash_joins, 1U);
    size_t count = 0;
    ASSERT_SUCCESS(joined.Value().ForEachRow([&](const Row& row) {
      ASSERT_EQ(row.values_.size(), 4U);
      EXPECT_FALSE(row[0].IsNull());
      EXPECT_EQ(row[0], row[2]);
      EXPECT_EQ(row[1], Value(9));
      EXPECT_EQ(row[3], Value(9));
      ++count;
    }));
    EXPECT_EQ(count, expected);
    EXPECT_EQ(runtime.inner_hash_build_rows, 3U);
    EXPECT_EQ(runtime.inner_hash_left_builds, 1U);
    EXPECT_EQ(runtime.inner_hash_spilled_probe_rows, 8192U);
  }
}

TEST_F(RelationalRegressionTest,
       SpilledInnerNestedLoopRescansOuterPerInnerChunk) {
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  const std::vector<int64_t> left_keys = {3, 1, 7, 0, 9, 5};
  std::vector<int64_t> right_keys;
  right_keys.reserve(9000);
  for (int i = 0; i < 9000; ++i) {
    right_keys.push_back(i % 11);
  }
  auto build_sides = [&] {
    relational_detail::Relation left, right;
    left.schema = Schema("l", {Column("lk", ValueType::kInt64)});
    right.schema = Schema("r", {Column("rk", ValueType::kInt64)});
    for (const int64_t key : left_keys) {
      left.AddRow(Row({Value(key)}));
    }
    for (const int64_t key : right_keys) {
      right.AddRow(Row({Value(key)}));
    }
    right.EnsureSpill();
    EXPECT_EQ(right.FinishSpill(), Status::kSuccess);
    EXPECT_TRUE(right.HasSpill());
    return std::pair{std::move(left), std::move(right)};
  };
  auto expected_for = [&](JoinType kind) {
    std::map<std::string, size_t> expected;
    std::vector<char> right_matched(right_keys.size(), 0);
    for (const int64_t lk : left_keys) {
      bool matched = false;
      for (size_t j = 0; j < right_keys.size(); ++j) {
        if (lk < right_keys[j]) {
          matched = true;
          right_matched[j] = 1;
          ++expected[(Row({Value(lk)}) + Row({Value(right_keys[j])}))
                         .ToString()];
        }
      }
      if (!matched && (kind == JoinType::kLeft || kind == JoinType::kFull)) {
        ++expected[(Row({Value(lk)}) + Row(std::vector<Value>(1))).ToString()];
      }
    }
    if (kind == JoinType::kRight || kind == JoinType::kFull) {
      for (size_t j = 0; j < right_keys.size(); ++j) {
        if (right_matched[j] == 0) {
          ++expected[(Row(std::vector<Value>(1)) + Row({Value(right_keys[j])}))
                         .ToString()];
        }
      }
    }
    return expected;
  };
  const std::vector<Expression> predicates{BinaryExpressionExp(
      ColumnValueExp("lk"), BinaryOperation::kLessThan, ColumnValueExp("rk"))};
  for (const JoinType kind :
       {JoinType::kInner, JoinType::kLeft, JoinType::kRight, JoinType::kFull}) {
    SCOPED_TRACE(static_cast<int>(kind));
    auto context = rs_->BeginContext();
    auto [left, right] = build_sides();
    SelectSource source;
    source.join_type = kind;
    source.join_condition = CombineConjuncts(predicates);
    auto joined = relational_detail::Join(
        context, std::move(left), std::move(right), source, nullptr, {});
    ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
    EXPECT_GE(joined.Value().nested_loop_joins, 1U);
    std::map<std::string, size_t> actual;
    ASSERT_SUCCESS(joined.Value().ForEachRow(
        [&](const Row& row) { ++actual[row.ToString()]; }));
    EXPECT_EQ(actual, expected_for(kind));
  }
  {
    auto context = rs_->BeginContext();
    auto [left, right] = build_sides();
    auto joined = relational_detail::InnerJoin(
        context, std::move(left), std::move(right), predicates, nullptr, {});
    ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
    EXPECT_GE(joined.Value().nested_loop_joins, 1U);
    std::map<std::string, size_t> actual;
    ASSERT_SUCCESS(joined.Value().ForEachRow(
        [&](const Row& row) { ++actual[row.ToString()]; }));
    EXPECT_EQ(actual, expected_for(JoinType::kInner));
  }
}

TEST_F(RelationalRegressionTest,
       DenseRuntimeScanKeepsNullableKeysAndProjection) {
  RelationalRun(*rs_, "CREATE TABLE NullableDenseScan (k INT64,id INT64)");
  std::map<int64_t, Row> expected;
  RuntimeFilterBuilder builder;
  for (const int64_t key : {-2, 0, 4}) {
    builder.Observe(key);
  }
  const auto filter = builder.Build();
  ASSERT_TRUE(filter->has_dense_bits());
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NullableDenseScan"));
    for (int64_t i = 0; i < 18000; ++i) {
      const int64_t key = (i % 13) - 6;
      const Value cell = i % 7 == 0 ? Value() : Value(key);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({cell, Value(i)})).GetStatus());
      if (!cell.IsNull() && (key == -2 || key == 0 || key == 4)) {
        expected.emplace(i, Row({Value(i), cell}));
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const Schema schema("NullableDenseScan", {Column("id", ValueType::kInt64),
                                            Column("k", ValueType::kInt64)});
  const std::vector<slot_t> projection{1, 0};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NullableDenseScan"));
    for (const int mode : {0, 1, 2, 3}) {
      const bool columnar = (mode & 1) != 0;
      const bool local_predicate = mode >= 2;
      const auto predicates = relational_detail::CompileScanFilter(
          {BinaryExpressionExp(ColumnValueExp("k"),
                               BinaryOperation::kGreaterThanEquals,
                               ConstantValueExp(Value(0)))},
          schema);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, filter.get(), ValueType::kInt64,
          slot_t{0}, true, local_predicate ? &predicates : nullptr, schema,
          nullptr, {}, &result, &error, nullptr, columnar));
      ASSERT_SUCCESS(error);
      std::map<int64_t, Row> actual;
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
      }));
      auto filtered_expected = expected;
      if (local_predicate) {
        std::erase_if(filtered_expected, [](const auto& item) {
          return item.second[1].value.int_value < 0;
        });
      }
      EXPECT_EQ(actual, filtered_expected);
    }
  }
}

TEST_F(RelationalRegressionTest, DateStringPeeksKeepHolesAndTypeBoundaries) {
  RelationalRun(*rs_, "CREATE TABLE DatePeek (d DATE,id INT64)");
  std::vector<Row> projected;
  projected.reserve(9000);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("DatePeek"));
    for (int i = 0; i < 9000; ++i) {
      const Value date =
          i % 7 == 0 ? Value() : Value::DateFromDays((i % 100) - 20);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({date, Value(i)})).GetStatus());
      projected.emplace_back(Row({Value(i), date}));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const Schema schema("DatePeek", {Column("id", ValueType::kInt64),
                                   Column("d", ValueType::kDate)});
  const std::vector<slot_t> projection{1, 0};
  const auto compare = [](BinaryOperation op, const char* date) {
    return BinaryExpressionExp(ColumnValueExp("d"), op,
                               ConstantValueExp(Value(date)));
  };
  const Expression first = compare(BinaryOperation::kEquals, "1970-01-02");
  const Expression last = compare(BinaryOperation::kEquals, "1970-01-16");
  const auto alternatives =
      BinaryExpressionExp(first, BinaryOperation::kOr, last);
  const std::vector<std::vector<Expression>> predicates{
      {compare(BinaryOperation::kGreaterThan, "1969-12-31"),
       compare(BinaryOperation::kLessThanEquals, "1970-01-16")},
      {alternatives},
      {BinaryExpressionExp(
          alternatives, BinaryOperation::kOr,
          BinaryExpressionExp(ColumnValueExp("id"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(3))))}};
  for (size_t workers : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("DatePeek"));
    for (size_t c = 0; c < predicates.size(); ++c) {
      const auto filter =
          relational_detail::CompileScanFilter(predicates[c], schema);
      const auto peeks = relational_detail::BuildIntegerPeeks(
          filter, &projection, table->GetSchema());
      EXPECT_EQ(peeks.empty(), c == 2);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, nullptr, ValueType::kNull, std::nullopt,
          true, &filter, schema, nullptr, {}, &result, &error));
      ASSERT_SUCCESS(error);
      std::map<int64_t, Row> expected, actual;
      for (const Row& row : projected) {
        bool matches = true;
        for (const Expression& predicate : predicates[c]) {
          ASSIGN_OR_ASSERT_FAIL(Value, verdict,
                                predicate->TryEvaluate(row, schema));
          matches &= !verdict.IsNull() && verdict.value.int_value != 0;
        }
        if (matches) {
          expected.emplace(row[0].value.int_value, row);
        }
      }
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
      }));
      EXPECT_EQ(actual, expected);
    }
    const auto invalid = relational_detail::CompileScanFilter(
        {compare(BinaryOperation::kEquals, "not-a-date")}, schema);
    EXPECT_TRUE(relational_detail::BuildIntegerPeeks(invalid, &projection,
                                                     table->GetSchema())
                    .empty());
    const auto dates = relational_detail::CompileScanFilter({first}, schema);
    const Schema integer_schema("DatePeek", {Column("d", ValueType::kInt64),
                                             Column("id", ValueType::kInt64)});
    EXPECT_TRUE(
        relational_detail::BuildIntegerPeeks(dates, &projection, integer_schema)
            .empty());
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, PackedDateComparisonsKeepHolesAndProjection) {
  RelationalRun(*rs_, "CREATE TABLE PackedDatePeek (d DATE,id INT64)");
  std::vector<Row> projected;
  projected.reserve(9000);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("PackedDatePeek"));
    for (int i = 0; i < 9000; ++i) {
      const Value date =
          i % 7 == 0 ? Value() : Value::DateFromDays((i % 1024) - 20);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({date, Value(i)})).GetStatus());
      projected.emplace_back(Row({Value(i), date}));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const Schema schema("PackedDatePeek", {Column("id", ValueType::kInt64),
                                         Column("d", ValueType::kDate)});
  const std::vector<slot_t> projection{1, 0};
  const auto compare = [](BinaryOperation op, const char* date) {
    return BinaryExpressionExp(ColumnValueExp("d"), op,
                               ConstantValueExp(Value(date)));
  };
  const Expression first = compare(BinaryOperation::kEquals, "1970-01-02");
  const Expression last = compare(BinaryOperation::kEquals, "1970-01-16");
  const auto alternatives =
      BinaryExpressionExp(first, BinaryOperation::kOr, last);
  const std::vector<std::vector<Expression>> predicates{
      {compare(BinaryOperation::kGreaterThan, "1969-12-31"),
       compare(BinaryOperation::kLessThanEquals, "1970-01-16")},
      {alternatives},
      {BinaryExpressionExp(
          alternatives, BinaryOperation::kOr,
          BinaryExpressionExp(ColumnValueExp("id"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(3))))}};
  for (size_t workers : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("PackedDatePeek"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_NE(store->dictionaries[0].wide_code_bytes, 0);
    for (size_t c = 0; c < predicates.size(); ++c) {
      const auto filter =
          relational_detail::CompileScanFilter(predicates[c], schema);
      const auto peeks = relational_detail::BuildIntegerPeeks(
          filter, &projection, table->GetSchema());
      EXPECT_EQ(peeks.empty(), c == 2);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, nullptr, ValueType::kNull, std::nullopt,
          true, &filter, schema, nullptr, {}, &result, &error));
      ASSERT_SUCCESS(error);
      std::map<int64_t, Row> expected, actual;
      for (const Row& row : projected) {
        bool matches = true;
        for (const Expression& predicate : predicates[c]) {
          ASSIGN_OR_ASSERT_FAIL(Value, verdict,
                                predicate->TryEvaluate(row, schema));
          matches &= !verdict.IsNull() && verdict.value.int_value != 0;
        }
        if (matches) {
          expected.emplace(row[0].value.int_value, row);
        }
      }
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
      }));
      EXPECT_EQ(actual, expected);
    }
    const auto invalid = relational_detail::CompileScanFilter(
        {compare(BinaryOperation::kEquals, "not-a-date")}, schema);
    EXPECT_TRUE(relational_detail::BuildIntegerPeeks(invalid, &projection,
                                                     table->GetSchema())
                    .empty());
    const auto dates = relational_detail::CompileScanFilter({first}, schema);
    const Schema integer_schema(
        "PackedDatePeek",
        {Column("d", ValueType::kInt64), Column("id", ValueType::kInt64)});
    EXPECT_TRUE(
        relational_detail::BuildIntegerPeeks(dates, &projection, integer_schema)
            .empty());
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, ColumnarNullTestsKeepProjectionAndResiduals) {
  RelationalRun(*rs_,
                "CREATE TABLE NullScan (id INT64, k INT64, s STRING, "
                "v DOUBLE, absent INT64)");
  std::vector<Row> projected;
  projected.reserve(10000);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NullScan"));
    for (int i = 0; i < 10000; ++i) {
      const Value key = i >= 1000 && i % 3 == 0 ? Value() : Value(i % 7);
      const Value text =
          i >= 1000 && i % 5 == 0
              ? Value()
              : Value(std::string(96, static_cast<char>('a' + (i % 20))));
      const Value number = i % 2 == 0 ? Value() : Value(i * 0.5);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(i), key, text, number, Value()}))
              .GetStatus());
      projected.emplace_back(Row({number, Value(i), text, key, Value()}));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const std::vector<slot_t> projection{3, 0, 2, 1, 4};
  const Schema schema(
      "NullScan",
      {Column("v", ValueType::kDouble), Column("id", ValueType::kInt64),
       Column("s", ValueType::kVarChar), Column("k", ValueType::kInt64),
       Column("absent", ValueType::kInt64)});
  const auto is_null = [](const char* column) {
    return UnaryExpressionExp(ColumnValueExp(column), UnaryOperation::kIsNull);
  };
  const auto is_not_null = [](const char* column) {
    return UnaryExpressionExp(ColumnValueExp(column),
                              UnaryOperation::kIsNotNull);
  };
  const Expression extra = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(3)));
  const std::vector<std::vector<Expression>> predicates{
      {is_null("k")},
      {is_not_null("s")},
      {UnaryExpressionExp(is_null("k"), UnaryOperation::kNot)},
      {is_null("s"), is_not_null("v")},
      {is_null("id")},
      {is_null("absent")},
      {is_not_null("absent")},
      {is_null("s"), extra},
      {BinaryExpressionExp(is_null("k"), BinaryOperation::kOr, is_null("s"))},
      {UnaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1))),
          UnaryOperation::kIsNull)}};
  for (size_t workers : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NullScan"));
    for (const auto& conjunction : predicates) {
      const auto filter =
          relational_detail::CompileScanFilter(conjunction, schema);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, nullptr, ValueType::kNull, std::nullopt,
          true, &filter, schema, nullptr, {}, &result, &error));
      ASSERT_SUCCESS(error);
      ASSERT_SUCCESS(result.GetStatus());
      std::map<int64_t, Row> expected;
      for (const Row& row : projected) {
        bool matches = true;
        for (const Expression& predicate : conjunction) {
          ASSIGN_OR_ASSERT_FAIL(Value, verdict,
                                predicate->TryEvaluate(row, schema));
          matches &= !verdict.IsNull() && verdict.value.int_value != 0;
        }
        if (matches) {
          expected.emplace(row[1].value.int_value, row);
        }
      }
      std::map<int64_t, Row> actual;
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[1].value.int_value, row).second);
      }));
      EXPECT_EQ(actual, expected);
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, ScalarIntervalFoldKeepsNullsAndBoundaries) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          setup, Schema("IntervalFold", {Column("k", ValueType::kInt64),
                                         Column("v", ValueType::kDouble)})));
  const std::vector<Row> pattern{
      Row({Value(), Value(100.0)}),
      Row({Value(-2), Value(1.5)}),
      Row({Value(0), Value()}),
      Row({Value(1), Value(-2.0)}),
      Row({Value(2), Value(4.5)}),
      Row({Value(std::numeric_limits<int64_t>::min()), Value(8.0)}),
      Row({Value(std::numeric_limits<int64_t>::max()), Value(16.0)})};
  constexpr int kRepeats = 512;
  for (int i = 0; i < kRepeats; ++i) {
    for (const Row& row : pattern) {
      ASSERT_SUCCESS(table.Insert(setup.txn_, row).GetStatus());
    }
  }
  ASSERT_SUCCESS(setup.PreCommit());
  const auto run = [&](const std::string& predicate) {
    std::string sql = "SELECT ";
    for (const auto* aggregate : {"COUNT(*)", "SUM(v)", "AVG(v)"}) {
      if (sql != "SELECT ") {
        sql += ", ";
      }
      sql += "(SELECT ";
      sql += aggregate;
      sql += " FROM IntervalFold WHERE ";
      sql += predicate;
      sql += ")";
    }
    sql += " FROM KeyTable WHERE k = 0";
    return RelationalRun(*rs_, sql);
  };
  for (size_t worker_count : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, worker_count);
    EXPECT_EQ(run("k >= -2 AND k <= 2"),
              std::vector<Row>({Row({Value(kRepeats * 4), Value(kRepeats * 4.0),
                                     Value(4.0 / 3.0)})}));
    EXPECT_EQ(run("k > -2 AND k < 2"),
              std::vector<Row>({Row({Value(kRepeats * 2),
                                     Value(kRepeats * -2.0), Value(-2.0)})}));
    EXPECT_EQ(run("k = 0"),
              std::vector<Row>({Row({Value(kRepeats), Value(), Value()})}));
    for (const auto* predicate :
         {"k > 9223372036854775807", "k < -9223372036854775808",
          "k >= 2 AND k < 1"}) {
      EXPECT_EQ(run(predicate),
                std::vector<Row>({Row({Value(0), Value(), Value()})}));
    }
    EXPECT_EQ(run("k >= 1 AND k <= 2 AND v > 0"),
              std::vector<Row>(
                  {Row({Value(kRepeats), Value(kRepeats * 4.5), Value(4.5)})}));
  }
  RelationalRun(*rs_,
                "INSERT INTO IntervalFold VALUES (1, CAST('inf' AS FLOAT64))");
  const auto infinite = run("k = 1");
  ASSERT_EQ(infinite.size(), 1U);
  EXPECT_TRUE(std::isinf(infinite[0][1].value.double_value));
  EXPECT_TRUE(std::isinf(infinite[0][2].value.double_value));
  RelationalRun(*rs_,
                "INSERT INTO IntervalFold VALUES (1, CAST('nan' AS FLOAT64))");
  const auto nan = run("k = 1");
  ASSERT_EQ(nan.size(), 1U);
  EXPECT_TRUE(std::isnan(nan[0][1].value.double_value));
  EXPECT_TRUE(std::isnan(nan[0][2].value.double_value));
}

TEST_F(RelationalRegressionTest, NarrowScalarAggregatesKeepNullAndEmptyInput) {
  RelationalRun(*rs_,
                "CREATE TABLE narrow_scalar (padding STRING, a INT64, "
                "b FLOAT64, unused STRING)");
  RelationalRun(*rs_,
                "INSERT INTO narrow_scalar VALUES "
                "('x', NULL, NULL, 'y'), ('x', 3, 2.5, 'y'), "
                "('x', 5, NULL, 'y')");
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*), COUNT(a), SUM(a), SUM(b) "
                    "FROM narrow_scalar"),
      std::vector<Row>({Row({Value(3), Value(2), Value(8), Value(2.5)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*) FROM narrow_scalar"),
            std::vector<Row>({Row({Value(3)})}));
  RelationalRun(*rs_, "DELETE FROM narrow_scalar");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*), SUM(a), SUM(b) "
                          "FROM narrow_scalar"),
            std::vector<Row>({Row({Value(0), Value(), Value()})}));
}

}  // namespace
}  // namespace tinylamb
