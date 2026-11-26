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

#include <cstddef>

#include "common/test_util.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/relational_regression_test.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest,
       CompositeDictionaryScalarKeysKeepRowsAndErrors) {
  RelationalRun(*rs_,
                "CREATE TABLE CompositeTime(k INT64,s TIMESTAMP,g INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("CompositeTime"));
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text = "2020-01-01 00:";
      const int64_t minute = (i % 600) / 60;
      text.push_back(static_cast<char>('0' + (minute / 10)));
      text.push_back(static_cast<char>('0' + (minute % 10)));
      text.push_back(':');
      text.push_back(static_cast<char>('0' + ((i % 60) / 10)));
      text.push_back(static_cast<char>('0' + (i % 10)));
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({i % 17 == 0 ? Value() : Value((i % 300) * 1000000001),
                       i % 19 == 0 ? Value() : Value(std::move(text)),
                       i % 23 == 0 ? Value() : Value(i % 301)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const bool trunc : {false, true}) {
      const Expression key =
          trunc ? FunctionCallExp("timestamp_trunc", {ColumnValueExp("s"),
                                                      ColumnValueExp("MINUTE")})
                : FunctionCallExp("extract_minute", {ColumnValueExp("s")});
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      SelectStatement statement(
          {NamedExpression("k", ColumnValueExp("k")), NamedExpression("m", key),
           NamedExpression("g", ColumnValueExp("g")),
           NamedExpression("c", count)},
          {}, nullptr);
      SelectSource source;
      source.table = "CompositeTime";
      statement.SetSources({source});
      statement.SetGroupBy({ColumnValueExp("k"), key, ColumnValueExp("g")});
      statement.SetOrderBy({{AggregateExpressionExp(AggregationType::kCount,
                                                    ColumnValueExp("*")),
                             false, std::nullopt},
                            {ColumnValueExp("k"), true, true},
                            {key, true, false},
                            {ColumnValueExp("g"), true, true}});
      statement.SetLimit(13);
      statement.SetOffset(5);
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("CompositeTime"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      for (const auto& dictionary : store->dictionaries) {
        ASSERT_EQ(dictionary.wide_code_bytes, 2);
      }
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      std::string expected = "SELECT k,";
      expected += trunc ? "TIMESTAMP_TRUNC(s||'',MINUTE)"
                        : "EXTRACT(MINUTE FROM s||'')";
      expected +=
          " m,g,COUNT(*) c FROM CompositeTime GROUP BY k,m,g ORDER BY c DESC,"
          "k NULLS FIRST,m NULLS LAST,g NULLS FIRST LIMIT 13 OFFSET 5";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, expected));
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  // A filtered invalid timestamp must stay on the ordinary evaluator, while
  // an observed invalid value must raise even when LIMIT would discard it.
  RelationalRun(*rs_,
                "INSERT INTO CompositeTime VALUES "
                "(9223372036854775807,'invalid-timestamp',0)");
  EXPECT_FALSE(RelationalRun(*rs_,
                             "SELECT k,TIMESTAMP_TRUNC(s,MINUTE) m,g,COUNT(*) "
                             "FROM CompositeTime WHERE k<9223372036854775807 "
                             "GROUP BY k,m,g LIMIT 1")
                   .empty());
  const auto invalid_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), invalid_deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("CompositeTime"));
  const auto store = BuildLSTableStore(context.txn_, *table);
  ASSERT_NE(store, nullptr);
  SqlEngine engine(*rs_);
  auto result =
      engine.Execute(context,
                     "SELECT k,TIMESTAMP_TRUNC(s,MINUTE) m,g,COUNT(*) "
                     "FROM CompositeTime GROUP BY k,m,g ORDER BY k LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, DictionaryCaseKeysAndNonKeyFiltersKeepRows) {
  RelationalRun(*rs_,
                "CREATE TABLE CaseCodes(a INT64,b INT64,c INT64,s STRING,t "
                "STRING,f INT64,d DATE)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CaseCodes"));
    for (int64_t i = 0; i < 15000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  context.txn_,
                  Row({i % 31 == 0 ? Value() : Value(i % 3), Value(i % 5),
                       Value(i % 7),
                       i % 19 == 0   ? Value()
                       : i % 17 == 0 ? Value("")
                                     : Value(std::string(80, 's') +
                                             std::to_string(i % 307)),
                       i % 23 == 0 ? Value()
                                   : Value(std::string(70, 't') +
                                           std::to_string(i % 311)),
                       i % 11 == 0 ? Value() : Value(i % 5),
                       i % 29 == 0 ? Value() : Value::DateFromDays(18262)}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const auto& literal :
         std::vector<Value>{Value(""), Value("fallback"), Value()}) {
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("CaseCodes"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      ASSERT_TRUE(store->dictionaries[1].active);
      ASSERT_EQ(store->dictionaries[3].wide_code_bytes, 2);
      ASSERT_EQ(store->dictionaries[5].wide_code_bytes, 2);
      const auto a = ColumnValueExp("a");
      const auto b = ColumnValueExp("b");
      const auto c = ColumnValueExp("c");
      const auto text = ColumnValueExp("t");
      const auto condition =
          BinaryExpressionExp(BinaryExpressionExp(a, BinaryOperation::kEquals,
                                                  ConstantValueExp(Value(0))),
                              BinaryOperation::kAnd,
                              BinaryExpressionExp(b, BinaryOperation::kEquals,
                                                  ConstantValueExp(Value(0))));
      const auto key = CaseExpressionExp({{condition, ColumnValueExp("s")}},
                                         ConstantValueExp(literal));
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto predicate = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("f"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(2))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("d"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value("2020-01-01"))));
      SelectStatement statement(
          {NamedExpression("a", a), NamedExpression("b", b),
           NamedExpression("c", c), NamedExpression("src", key),
           NamedExpression("dst", text), NamedExpression("n", count)},
          {}, predicate);
      SelectSource source;
      source.table = "CaseCodes";
      statement.SetSources({source});
      statement.SetGroupBy({a, b, c, key, text});
      statement.SetOrderBy({{count, false, std::nullopt},
                            {a, true, true},
                            {b, true, false},
                            {c, true, false},
                            {key, true, true},
                            {text, true, false}});
      statement.SetLimit(13);
      statement.SetOffset(5);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      EXPECT_GT(runtime.scan_output_rows, 0U);
      EXPECT_LT(runtime.scan_output_rows, runtime.scan_rows);
      const std::string sql_literal =
          literal.IsNull() ? "NULL"
                           : "'" + std::string(literal.AsStringView()) + "'";
      const std::string reference =
          "SELECT a,b,c,CASE WHEN a=0 AND b=0 THEN s||'' ELSE " + sql_literal +
          " END src,t dst,COUNT(*) n FROM CaseCodes WHERE f>=2 AND "
          "d>='2020-01-01' GROUP BY a,b,c,src,dst ORDER BY n DESC,a NULLS "
          "FIRST,b,c,"
          "src NULLS FIRST,dst NULLS LAST LIMIT 13 OFFSET 5";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
      statement.SetWhereClause(BinaryExpressionExp(
          ColumnValueExp("f"), BinaryOperation::kGreaterThan,
          ConstantValueExp(Value(100))));
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, empty,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_TRUE(empty.rows.empty());
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  // An interval-shaped ELSE string is still ordinary text. The dictionary
  // path and the materialized string path must preserve the same groups.
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT a,b,c,CASE WHEN a=0 THEN s ELSE '0-0 0 24:0:0' END "
          "src,t,COUNT(*) "
          "FROM CaseCodes GROUP BY a,b,c,src,t ORDER BY a,b,c,src,t LIMIT 11"),
      RelationalRun(
          *rs_,
          "SELECT a,b,c,CASE WHEN a=0 THEN s||'' ELSE '0-0 0 24:0:0' END "
          "src,t,COUNT(*) "
          "FROM CaseCodes GROUP BY a,b,c,src,t ORDER BY a,b,c,src,t LIMIT 11"));
}

TEST_F(RelationalRegressionTest,
       DictionaryMembershipFiltersKeepNullsAndOffsets) {
  RelationalRun(*rs_,
                "CREATE TABLE ListCodes(k INT64,s STRING,f INT64,t STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("ListCodes"));
    for (int64_t i = 0; i < 12000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_,
                       Row({Value(i % 7),
                            i % 19 == 0 ? Value()
                                        : Value(std::string(80, 'A') +
                                                std::to_string(i % 301)),
                            i % 11 == 0 ? Value() : Value((i % 5) - 2),
                            i % 13 == 0 ? Value()
                                        : Value(i % 3 == 0   ? ""
                                                : i % 3 == 1 ? "alpha"
                                                             : "beta")}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const bool scalar : {false, true}) {
      auto context = rs_->BeginReadOnlyContext();
      const auto k = ColumnValueExp("k");
      const auto s = ColumnValueExp("s");
      const auto key = scalar ? FunctionCallExp("lower", {s}) : s;
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      std::vector<NamedExpression> select;
      std::vector<Expression> keys;
      if (!scalar) {
        select.emplace_back("k", k);
        keys.push_back(k);
      }
      select.emplace_back("s", key);
      select.emplace_back("c", count);
      keys.push_back(key);
      SelectStatement statement(std::move(select), {}, nullptr);
      SelectSource source;
      source.table = "ListCodes";
      statement.SetSources({source});
      statement.SetGroupBy(keys);
      std::vector<SelectStatement::OrderByTerm> order{
          {count, false, std::nullopt}};
      for (const auto& group : keys) {
        order.push_back({group, true, false});
      }
      statement.SetOrderBy(std::move(order));
      statement.SetLimit(13);
      statement.SetOffset(3);
      statement.SetWhereClause(BinaryExpressionExp(
          InExpressionExp(ColumnValueExp("f"),
                          {UnaryExpressionExp(ConstantValueExp(Value(1)),
                                              UnaryOperation::kMinus),
                           BinaryExpressionExp(ConstantValueExp(Value(1)),
                                               BinaryOperation::kAdd,
                                               ConstantValueExp(Value(1))),
                           ConstantValueExp(Value(-1))}),
          BinaryOperation::kAnd,
          InExpressionExp(ColumnValueExp("t"),
                          {ConstantValueExp(Value("")),
                           ConstantValueExp(Value("alpha"))})));
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      ASSERT_EQ(result.rows.size(), 13U);
      const std::string reference =
          scalar
              ? "SELECT LOWER(s||'') x,COUNT(*) c FROM ListCodes WHERE f IN "
                "(-1,2,-1) AND t IN ('','alpha') GROUP BY x ORDER BY c DESC,x "
                "NULLS LAST LIMIT 13 OFFSET 3"
              : "SELECT k,s||'' x,COUNT(*) c FROM ListCodes WHERE f IN "
                "(-1,2,-1) AND t IN ('','alpha') GROUP BY k,x ORDER BY c "
                "DESC,k NULLS LAST,x NULLS LAST LIMIT 13 OFFSET 3";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
      statement.SetWhereClause(
          InExpressionExp(ColumnValueExp("f"), {ConstantValueExp(Value(100))}));
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, empty,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_TRUE(empty.rows.empty());
      for (const int64_t target : {3, 100}) {
        statement.SetWhereClause(BinaryExpressionExp(
            BinaryExpressionExp(k, BinaryOperation::kEquals,
                                ConstantValueExp(Value(target))),
            BinaryOperation::kAnd,
            InExpressionExp(ColumnValueExp("f"),
                            {ConstantValueExp(Value(-1))})));
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, selective,
            relational_detail::ExecuteQuery(context, statement, nullptr, {}));
        const std::string predicate =
            " WHERE f IN (-1) AND k=" + std::to_string(target);
        const std::string oracle =
            scalar
                ? "SELECT LOWER(s||'') x,COUNT(*) c FROM ListCodes" +
                      predicate +
                      " GROUP BY x ORDER BY c DESC,x NULLS LAST LIMIT 13 "
                      "OFFSET 3"
                : "SELECT k,s||'' x,COUNT(*) c FROM ListCodes" + predicate +
                      " GROUP BY k,x ORDER BY c DESC,k NULLS LAST,x NULLS LAST "
                      "LIMIT 13 OFFSET 3";
        EXPECT_EQ(selective.rows, RelationalRun(*rs_, oracle));
        EXPECT_EQ(selective.rows.size(), target == 3 ? 13U : 0U);
        EXPECT_EQ(runtime.scan_values_decoded, 0U);
      }
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  // NULL list elements and NOT IN retain SQL's three-valued fallback.
  for (const std::string predicate : {"f IN (2,NULL)", "f NOT IN (2,NULL)"}) {
    EXPECT_EQ(RelationalRun(*rs_, "SELECT k,s,COUNT(*) FROM ListCodes WHERE " +
                                      predicate +
                                      " GROUP BY k,s ORDER BY k,s LIMIT 11"),
              RelationalRun(
                  *rs_, "SELECT k,s||'' x,COUNT(*) FROM ListCodes WHERE " +
                            predicate + " GROUP BY k,x ORDER BY k,x LIMIT 11"));
  }
}

TEST_F(RelationalRegressionTest, CompositeDictionaryUnsignedKeysKeepTags) {
  RelationalRun(*rs_, "CREATE TABLE UnsignedCodes(k UINT64,s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("UnsignedCodes"));
    for (int64_t i = 0; i < 12000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_, Row({Value(-1 - (i % 7)).WithUnsigned(),
                                          Value(std::string(80, 'x') +
                                                std::to_string(i % 300))}))
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
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("UnsignedCodes"));
  const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->dictionaries[0].active);
  ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
  const auto k = ColumnValueExp("k");
  const auto s = ColumnValueExp("s");
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  SelectStatement statement({NamedExpression("k", k), NamedExpression("s", s),
                             NamedExpression("c", count)},
                            {}, nullptr);
  SelectSource source;
  source.table = "UnsignedCodes";
  statement.SetSources({source});
  statement.SetGroupBy({k, s});
  statement.SetOrderBy({{k, true, std::nullopt}, {s, true, std::nullopt}});
  statement.SetLimit(11);
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  ASSIGN_OR_ASSERT_FAIL(
      relational_detail::Relation, result,
      relational_detail::ExecuteQuery(context, statement, nullptr, {}));
  ASSERT_EQ(result.rows.size(), 11U);
  EXPECT_TRUE(result.schema.GetColumn(0).IsUnsigned());
  for (const auto& row : result.rows) {
    EXPECT_TRUE(row[0].IsUnsigned());
  }
  EXPECT_EQ(
      result.rows,
      RelationalRun(
          *rs_,
          "SELECT k,s||'' text,COUNT(*) FROM UnsignedCodes GROUP BY k,text "
          "ORDER BY k,text LIMIT 11"));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest, CompositeDictionaryMixedWidthsFilterKeys) {
  RelationalRun(*rs_, "CREATE TABLE MixedCodes(k INT64,s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MixedCodes"));
    for (int64_t i = 0; i < 15000; ++i) {
      std::string text = std::string(80, 'x') + std::to_string(i % 601);
      ASSERT_SUCCESS(table
                         ->Insert(context.txn_,
                                  Row({Value(i % 7),
                                       i % 19 == 0   ? Value()
                                       : i % 17 == 0 ? Value("")
                                                     : Value(std::move(text))}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const int64_t low : {int64_t{-1}, int64_t{2}, int64_t{100}}) {
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("MixedCodes"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      ASSERT_TRUE(store->dictionaries[0].active);
      const auto& codes = store->dictionaries[0].narrow_codes;
      ASSERT_EQ(codes.size(), 15000U);
      size_t code_offset = 0;
      for (const auto& block : store->blocks) {
        EXPECT_EQ(block.columns[0].dict_codes.data(),
                  codes.data() + code_offset);
        for (size_t r = 0; r < block.row_count; ++r) {
          EXPECT_EQ(block.columns[0].DictionaryCodeAt(r),
                    codes[code_offset + r]);
        }
        code_offset += block.row_count;
      }
      EXPECT_EQ(code_offset, codes.size());
      ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
      const auto k = ColumnValueExp("k");
      const auto text = ColumnValueExp("s");
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto predicate = BinaryExpressionExp(
          BinaryExpressionExp(k, BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(low))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(text, BinaryOperation::kNotEquals,
                              ConstantValueExp(Value(""))));
      SelectStatement statement(
          {NamedExpression("k", k), NamedExpression("s", text),
           NamedExpression("c", count)},
          {}, predicate);
      SelectSource source;
      source.table = "MixedCodes";
      statement.SetSources({source});
      statement.SetGroupBy({k, text});
      statement.SetOrderBy({{count, false, std::nullopt},
                            {k, false, true},
                            {text, true, false}});
      statement.SetLimit(13);
      statement.SetOffset(5);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      EXPECT_EQ(
          result.rows,
          RelationalRun(
              *rs_,
              "SELECT k,s||'' AS text,COUNT(*) c FROM MixedCodes WHERE k>" +
                  std::to_string(low) +
                  " AND s<>'' GROUP BY k,text ORDER BY c DESC,k DESC NULLS "
                  "FIRST,"
                  "text NULLS LAST LIMIT 13 OFFSET 5"));
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
}

TEST_F(RelationalRegressionTest, CompositeDictionaryGroupsKeepNullsAndOrder) {
  const Schema schema("CompositeCodes", {Column("k", ValueType::kInt64),
                                         Column("s", ValueType::kVarChar)});
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    for (int64_t i = 0; i < 15000; ++i) {
      std::string text(80, 'x');
      text += std::to_string((i / 7) % 301);
      const Value key = i % 17 == 0 ? Value() : Value((i % 300) * 1000000001);
      const Value string = i % 19 == 0 ? Value() : Value(std::move(text));
      ASSERT_SUCCESS(table.Insert(setup.txn_, Row({key, string})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    const auto key = ColumnValueExp("k");
    const auto string = ColumnValueExp("s");
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("s", string),
         NamedExpression("c", count)},
        {}, nullptr);
    SelectSource source;
    source.table = "CompositeCodes";
    statement.SetSources({source});
    statement.SetGroupBy({key, string});
    statement.SetOrderBy({{count, false, std::nullopt},
                          {key, true, true},
                          {string, false, false}});
    statement.SetLimit(13);
    statement.SetOffset(7);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CompositeCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    EXPECT_EQ(
        result.rows,
        RelationalRun(
            *rs_,
            "SELECT MIN(k) k,s||'' s,COUNT(*) c FROM CompositeCodes "
            "GROUP BY CAST(k AS STRING),s||'' ORDER BY c DESC,k NULLS FIRST,"
            "s DESC NULLS LAST LIMIT 13 OFFSET 7"));
    ASSERT_SUCCESS(context.PreCommit());
    statement.SetOrderBy({});
    statement.SetLimit(11);
    statement.SetOffset(5);
    auto prefix_context = rs_->BeginReadOnlyContext();
    relational_detail::ExecutionRuntime prefix_runtime;
    prefix_context.set_execution_runtime(&prefix_runtime);
    ASSIGN_OR_ASSERT_FAIL(relational_detail::Relation, prefix_result,
                          relational_detail::ExecuteQuery(
                              prefix_context, statement, nullptr, {}));
    EXPECT_LE(prefix_runtime.aggregate_groups, 16U);
    EXPECT_EQ(
        prefix_result.rows,
        RelationalRun(*rs_,
                      "SELECT MIN(k),s||'',COUNT(*) FROM CompositeCodes "
                      "GROUP BY CAST(k AS STRING),s||'' LIMIT 11 OFFSET 5"));
    ASSERT_SUCCESS(prefix_context.PreCommit());
    // No ORDER BY retains first-occurrence order, independently of worker
    // completion order and the hash table's iteration order.
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT k,s,COUNT(*) FROM CompositeCodes "
                      "GROUP BY k,s LIMIT 11 OFFSET 5"),
        RelationalRun(*rs_,
                      "SELECT MIN(k),s||'',COUNT(*) FROM CompositeCodes "
                      "GROUP BY CAST(k AS STRING),s||'' LIMIT 11 OFFSET 5"));
    EXPECT_TRUE(RelationalRun(*rs_,
                              "SELECT k,s,COUNT(*) FROM CompositeCodes "
                              "GROUP BY k,s LIMIT 0")
                    .empty());
  }
  // Byte-distinct INTERVAL spellings are one SQL key. Rebuilding the
  // dictionary must revoke code-equality eligibility for the entire column.
  RelationalRun(*rs_,
                "INSERT INTO CompositeCodes VALUES "
                "(9223372036854775807,'0-12 0 0:0:0'),"
                "(9223372036854775807,'1-0 0 0:0:0')");
  const auto peer_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), peer_deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CompositeCodes"));
    // Lazy lookup deliberately declines a stale nonempty cache; explicitly
    // build the new committed snapshot for this eligibility regression.
    const auto store = BuildLSTableStore(context.txn_, *table);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    EXPECT_TRUE(store->dictionaries[1].bytewise_equality);
    ASSERT_SUCCESS(context.PreCommit());
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) FROM CompositeCodes "
                          "GROUP BY k,s ORDER BY k DESC NULLS LAST LIMIT 1"),
            std::vector<Row>{
                Row({Value(std::numeric_limits<int64_t>::max()), Value(1)})});
}

TEST_F(RelationalRegressionTest, CompositeDictionaryCountsMergeAcrossWorkers) {
  const Schema schema("MergedCodes", {Column("k", ValueType::kInt64),
                                      Column("s", ValueType::kVarChar)});
  const std::string common(80, 'x');
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    for (int64_t i = 0; i < 90000; ++i) {
      // A count above 16 bits and duplicate keys on every worker. The tail
      // also includes NULL and groups tied at the LIMIT boundary.
      const int64_t id = i % 5 == 0 ? (i % 307) + 1 : 0;
      const Value key = id == 1 ? Value() : Value(id * 1000000001);
      const Value text = id == 2 ? Value() : Value(common + std::to_string(id));
      ASSERT_SUCCESS(table.Insert(setup.txn_, Row({key, text})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const size_t workers :
       {size_t{1}, size_t{2}, size_t{3}, size_t{4}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto key = ColumnValueExp("k");
    const auto text = ColumnValueExp("s");
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("s", text),
         NamedExpression("c", count)},
        {}, nullptr);
    SelectSource source;
    source.table = "MergedCodes";
    statement.SetSources({source});
    statement.SetGroupBy({key, text});
    statement.SetOrderBy({{count, false, std::nullopt}});
    statement.SetLimit(11);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MergedCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    ASSERT_EQ(result.rows.size(), 11U);
    EXPECT_EQ(result.rows.front(),
              Row({Value(0), Value(common + "0"), Value(72000)}));
    EXPECT_EQ(result.rows,
              RelationalRun(*rs_,
                            "SELECT MIN(k),s||'',COUNT(*) c FROM MergedCodes "
                            "GROUP BY CAST(k AS STRING),s||'' ORDER BY c DESC "
                            "LIMIT 11"));
    ASSERT_SUCCESS(context.PreCommit());
    for (const std::string_view suffix :
         {" LIMIT 17 OFFSET 5", " ORDER BY c DESC LIMIT 13 OFFSET 3",
          " ORDER BY c ASC LIMIT 13 OFFSET 3"}) {
      EXPECT_EQ(RelationalRun(*rs_,
                              "SELECT k,s,COUNT(*) c FROM MergedCodes "
                              "WHERE k<>0 GROUP BY k,s" +
                                  std::string(suffix)),
                RelationalRun(*rs_,
                              "SELECT MIN(k),s||'',COUNT(*) c FROM MergedCodes "
                              "WHERE k<>0 GROUP BY CAST(k AS STRING),s||''" +
                                  std::string(suffix)));
    }
    EXPECT_TRUE(RelationalRun(*rs_,
                              "SELECT k,s,COUNT(*) FROM MergedCodes WHERE k<0 "
                              "GROUP BY k,s ORDER BY COUNT(*) DESC LIMIT 10")
                    .empty());
  }
}

TEST_F(RelationalRegressionTest,
       DictionaryDistinctCountsMergeAndKeepNullGroups) {
  RelationalRun(*rs_, "CREATE TABLE DistinctCodes(v INT64,k INT64,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("DistinctCodes"));
    for (int64_t i = 0; i < 90000; ++i) {
      const int64_t t = i % 30000;
      const int64_t k = (t * 37) % 307;
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({k == 0 || t % 17 == 0
                           ? Value()
                           : Value(((t * 19) % 1021) * 1000000001),
                       k == 1 ? Value() : Value(k * 10000001), Value(t % 7)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const auto key = ColumnValueExp("k");
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  for (const std::string_view argument : {"v", "n"}) {
    const auto distinct = AggregateExpressionExp(
        AggregationType::kCount, ColumnValueExp(std::string(argument)), true);
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("d", distinct),
         NamedExpression("c", count)},
        {}, nullptr);
    SelectSource source;
    source.table = "DistinctCodes";
    statement.SetSources({source});
    statement.SetGroupBy({key});
    statement.SetOrderBy(
        {{distinct, false, std::nullopt}, {count, false, std::nullopt}});
    statement.SetLimit(13);
    for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("DistinctCodes"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
      ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
      ASSERT_TRUE(store->dictionaries[2].active);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      const std::string prefix = "SELECT k,COUNT(DISTINCT " +
                                 std::string(argument) +
                                 ") d,COUNT(*) c FROM DistinctCodes";
      EXPECT_EQ(
          result.rows,
          RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING) ORDER BY "
                                       "d DESC,c DESC LIMIT 13"));
      ASSERT_SUCCESS(context.PreCommit());
      for (const std::string_view filter : {"", " WHERE k>0 AND n<3"}) {
        for (const std::string_view tail :
             {" LIMIT 11 OFFSET 3", " ORDER BY d DESC LIMIT 7 OFFSET 2",
              " ORDER BY k DESC NULLS FIRST LIMIT 19",
              " ORDER BY d DESC LIMIT 3 WITH TIES", " LIMIT 0"}) {
          EXPECT_EQ(RelationalRun(*rs_, prefix + std::string(filter) +
                                            " GROUP BY k" + std::string(tail)),
                    RelationalRun(*rs_, prefix + std::string(filter) +
                                            " GROUP BY k,CAST(k AS STRING)" +
                                            std::string(tail)));
        }
      }
    }
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(DISTINCT v) FROM DistinctCodes "
                          "WHERE k=0 GROUP BY k LIMIT 3"),
            (std::vector<Row>{Row({Value(0), Value(0)})}));
  EXPECT_TRUE(RelationalRun(*rs_,
                            "SELECT k,COUNT(DISTINCT v) FROM DistinctCodes "
                            "WHERE k<0 GROUP BY k LIMIT 3")
                  .empty());
  const std::string snapshot_query =
      "SELECT k,COUNT(DISTINCT v) d FROM DistinctCodes "
      "GROUP BY k ORDER BY d DESC,k NULLS FIRST LIMIT 7";
  const auto expected = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "INSERT INTO DistinctCodes VALUES (999999999999999,0,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(DISTINCT v) FROM DistinctCodes "
                          "WHERE k=0 GROUP BY k LIMIT 3"),
            (std::vector<Row>{Row({Value(0), Value(1)})}));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DictionaryDistinctWideCodesAndWorkspaceFallback) {
  RelationalRun(*rs_, "CREATE TABLE WideDistinctCodes(k INT64,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("WideDistinctCodes"));
    for (int64_t i = 0; i < 180000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_, Row({Value((i % 4099) * 10000001),
                                        Value((i % 70001) * 1000000001)}))
              .GetStatus());
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
                          context.GetTable("WideDistinctCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    EXPECT_EQ(store->dictionaries[0].wide_code_bytes, 2);
    EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 4);
    const size_t words = (store->dictionaries[1].int_entries.size() + 63) / 64;
    const size_t bytes = (store->dictionaries[0].int_entries.size() + 1) *
                         words * sizeof(uint64_t);
    EXPECT_LT(bytes, size_t{64} * 1024 * 1024);
    EXPECT_GT(bytes * 2, size_t{64} * 1024 * 1024);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const std::string prefix =
      "SELECT k,COUNT(DISTINCT v) d FROM WideDistinctCodes GROUP BY k";
  const std::string tail = " ORDER BY d DESC,k LIMIT 23 OFFSET 5";
  const auto expected =
      RelationalRun(*rs_, prefix + ",CAST(k AS STRING)" + tail);
  // A four-byte argument code works with one worker. With two workers the
  // exact bitmap product exceeds the cap and sparse code pairs are used.
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, prefix + tail), expected);
  }
  const size_t old_limit = QueryMemoryBudget::Global().Limit();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget parallelism(&active, 1);
  QueryMemoryBudget::Global().ResetForTest(size_t{1024} * 1024);
  const auto constrained = RelationalRun(*rs_, prefix + tail);
  QueryMemoryBudget::Global().ResetForTest(old_limit);
  EXPECT_EQ(constrained, expected);
}

TEST_F(RelationalRegressionTest,
       SparseDictionaryDistinctPairsDeduplicateAcrossWorkers) {
  RelationalRun(*rs_, "CREATE TABLE SparseDistinct(k STRING,v INT64,f INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("SparseDistinct"));
    for (int64_t i = 0; i < 180000; ++i) {
      // The second half repeats every pair across worker boundaries. The
      // Cartesian dictionary domain exceeds 64 MiB even with one worker.
      const int64_t t = i % 90000;
      const int64_t key = t % 10007;
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({key == 1 ? Value() : Value("key_" + std::to_string(key)),
                       key == 0 || t % 17 == 0
                           ? Value()
                           : Value((t % 70001) * 1000000001),
                       Value(t % 7)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::string prefix =
      "SELECT k,COUNT(DISTINCT v) d,COUNT(DISTINCT v) again,COUNT(*) c "
      "FROM SparseDistinct";
  const std::string tail =
      " ORDER BY d DESC,c DESC,k NULLS FIRST LIMIT 17 OFFSET 3";
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const std::string_view filter : {"", " WHERE k<>'key_7' AND f<3"}) {
      std::string reference_query = prefix;
      reference_query.append(filter);
      reference_query += " GROUP BY k,CAST(k AS STRING)";
      reference_query += tail;
      const auto expected = RelationalRun(*rs_, reference_query);
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("SparseDistinct"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      const size_t domain = store->dictionaries[0].entries.size() + 1;
      const size_t words =
          (store->dictionaries[1].int_entries.size() + 63) / 64;
      ASSERT_GT(domain * words * sizeof(uint64_t), size_t{64} * 1024 * 1024);
      const auto key = ColumnValueExp("k");
      const auto distinct = AggregateExpressionExp(AggregationType::kCount,
                                                   ColumnValueExp("v"), true);
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      Expression predicate;
      if (!filter.empty()) {
        predicate = BinaryExpressionExp(
            BinaryExpressionExp(key, BinaryOperation::kNotEquals,
                                ConstantValueExp(Value("key_7"))),
            BinaryOperation::kAnd,
            BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kLessThan,
                                ConstantValueExp(Value(3))));
      }
      SelectStatement statement(
          {NamedExpression("k", key), NamedExpression("d", distinct),
           NamedExpression("again", distinct), NamedExpression("c", count)},
          {}, predicate);
      SelectSource source;
      source.table = "SparseDistinct";
      statement.SetSources({source});
      statement.SetGroupBy({key});
      statement.SetOrderBy({{distinct, false, std::nullopt},
                            {count, false, std::nullopt},
                            {key, true, true}});
      statement.SetLimit(17);
      statement.SetOffset(3);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(result.rows, expected);
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      if (workers == 4 && filter.empty()) {
        // The same query must keep the encoded path when its observed pairs
        // exceed the available staging buffer; duplicates cross disk runs
        // and worker boundaries. Restore the global budget on assertion exit.
        struct RestoreBudget {
          RestoreBudget() = default;
          RestoreBudget(const RestoreBudget&) = delete;
          RestoreBudget& operator=(const RestoreBudget&) = delete;
          RestoreBudget(RestoreBudget&&) = delete;
          RestoreBudget& operator=(RestoreBudget&&) = delete;
          size_t limit = QueryMemoryBudget::Global().Limit();
          ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
        } restore;
        QueryMemoryBudget::Global().ResetForTest(size_t{4} << 20);
        runtime = {};
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, spilled,
            relational_detail::ExecuteQuery(context, statement, nullptr, {}));
        EXPECT_EQ(spilled.rows, expected);
        EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
        EXPECT_EQ(runtime.scan_values_decoded, 0U);
      }
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(DISTINCT v) FROM SparseDistinct "
                          "WHERE k='key_0' GROUP BY k LIMIT 1"),
            (std::vector<Row>{Row({Value("key_0"), Value(0)})}));
  EXPECT_TRUE(
      RelationalRun(*rs_, prefix + " WHERE f<0 GROUP BY k LIMIT 3").empty());
  const auto expected =
      RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING)" + tail);
  const size_t old_limit = QueryMemoryBudget::Global().Limit();
  QueryMemoryBudget::Global().ResetForTest(size_t{1024} * 1024);
  const auto constrained = RelationalRun(*rs_, prefix + " GROUP BY k" + tail);
  QueryMemoryBudget::Global().ResetForTest(old_limit);
  EXPECT_EQ(constrained, expected);
  for (const std::string_view suffix :
       {" ORDER BY k NULLS FIRST LIMIT 1", " LIMIT 0"}) {
    EXPECT_EQ(RelationalRun(*rs_, prefix + " GROUP BY k" + std::string(suffix)),
              RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING)" +
                                      std::string(suffix)));
  }
  const std::string snapshot_query =
      prefix + " GROUP BY k ORDER BY k NULLS FIRST LIMIT 1";
  const auto before_insert = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_,
                "INSERT INTO SparseDistinct VALUES (NULL,999999999999999,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(old_result.Collect(), before_insert);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_NE(RelationalRun(*rs_, snapshot_query), before_insert);
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       CompositeDictionaryNumericAggregatesKeepMergeAndFallbacks) {
  RelationalRun(*rs_,
                "CREATE TABLE CompositeNumbers(v INT64,k INT64,s INT64,"
                "w INT64,f STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("CompositeNumbers"));
    for (int64_t i = 0; i < 24000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value((i % 1009) - 504),
                            i % 19 == 0 ? Value() : Value((i % 307) * 10000001),
                            Value(i % 5), Value((i % 503) - 251),
                            Value(i % 7 == 0 ? "" : "keep")}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const auto key = ColumnValueExp("k");
  const auto text = ColumnValueExp("s");
  const auto sum =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"));
  const auto avg =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("w"));
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("s", text),
       NamedExpression("total", sum), NamedExpression("mean", avg),
       NamedExpression("c", count)},
      {}, nullptr);
  SelectSource source;
  source.table = "CompositeNumbers";
  statement.SetSources({source});
  statement.SetGroupBy({key, text});
  statement.SetOrderBy({{count, false, std::nullopt}});
  statement.SetLimit(13);
  statement.SetOffset(3);
  const std::string prefix =
      "SELECT k,s,SUM(v) total,AVG(w) mean,COUNT(*) c FROM CompositeNumbers";
  const auto query = [&](bool reference, std::string_view filter,
                         std::string_view tail) {
    return prefix + std::string(filter) +
           (reference ? " GROUP BY k,s,CAST(k AS STRING)" : " GROUP BY k,s") +
           std::string(tail);
  };
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CompositeNumbers"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    ASSERT_TRUE(store->dictionaries[2].active);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    EXPECT_EQ(result.rows,
              RelationalRun(*rs_, query(true, "",
                                        " ORDER BY c DESC LIMIT 13 "
                                        "OFFSET 3")));
    ASSERT_SUCCESS(context.PreCommit());
    for (const std::string_view filter :
         {"", " WHERE f<>'' AND k>=0", " WHERE f='absent'"}) {
      for (const std::string_view tail :
           {" ORDER BY total DESC,k,s LIMIT 17 OFFSET 5",
            " ORDER BY mean,c DESC,k NULLS FIRST,s NULLS LAST LIMIT 11",
            " ORDER BY c DESC LIMIT 13 OFFSET 3", " LIMIT 0"}) {
        EXPECT_EQ(RelationalRun(*rs_, query(false, filter, tail)),
                  RelationalRun(*rs_, query(true, filter, tail)));
      }
    }
    EXPECT_EQ(RelationalRun(*rs_,
                            "SELECT k,s,AVG(CAST(w AS DOUBLE)),SUM(v),AVG(v) "
                            "FROM CompositeNumbers WHERE f<>'' GROUP BY k,s "
                            "ORDER BY SUM(w) DESC,k,s LIMIT 17"),
              RelationalRun(*rs_,
                            "SELECT k,s,AVG(CAST(w AS DOUBLE)),SUM(v),AVG(v) "
                            "FROM CompositeNumbers WHERE f<>'' GROUP BY "
                            "k,s,CAST(k AS STRING) "
                            "ORDER BY SUM(w) DESC,k,s LIMIT 17"));
  }
  const std::string tail = " ORDER BY c DESC,k,s LIMIT 11";
  const auto expected = RelationalRun(*rs_, query(true, "", tail));
  const size_t old_limit = QueryMemoryBudget::Global().Limit();
  QueryMemoryBudget::Global().ResetForTest(size_t{1024} * 1024);
  const auto constrained = RelationalRun(*rs_, query(false, "", tail));
  QueryMemoryBudget::Global().ResetForTest(old_limit);
  EXPECT_EQ(constrained, expected);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_,
                "INSERT INTO CompositeNumbers VALUES (NULL,0,0,NULL,'keep')");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, query(false, "", tail)));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, query(false, "", tail)),
            RelationalRun(*rs_, query(true, "", tail)));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DictionaryNumericAggregatesKeepBoundsAndRounding) {
  RelationalRun(*rs_, "CREATE TABLE NumericCodes(v INT64,k INT64,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NumericCodes"));
    for (int64_t i = 0; i < 30000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({Value((i % 1009) - 504),
                       i % 19 == 0 ? Value() : Value((i % 307) * 10000001),
                       i % 13 == 0 ? Value() : Value((i % 1021) * 1000000001)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const auto key = ColumnValueExp("k");
  const auto sum =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"));
  const auto avg =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("v"));
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  const auto distinct = AggregateExpressionExp(AggregationType::kCount,
                                               ColumnValueExp("n"), true);
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("s", sum),
       NamedExpression("a", avg), NamedExpression("c", count),
       NamedExpression("d", distinct)},
      {}, nullptr);
  SelectSource source;
  source.table = "NumericCodes";
  statement.SetSources({source});
  statement.SetGroupBy({key});
  statement.SetOrderBy({{avg, false, std::nullopt}, {sum, true, std::nullopt}});
  statement.SetLimit(11);
  const std::string prefix =
      "SELECT k,SUM(v) s,AVG(v) a,COUNT(*) c,COUNT(DISTINCT n) d FROM "
      "NumericCodes";
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NumericCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    EXPECT_EQ(
        result.rows,
        RelationalRun(
            *rs_,
            prefix +
                " GROUP BY k,CAST(k AS STRING) ORDER BY a DESC,s LIMIT 11"));
    ASSERT_SUCCESS(context.PreCommit());
    for (const std::string_view filter : {"", " WHERE v<0 AND n<3000000003"}) {
      for (const std::string_view order :
           {" ORDER BY s DESC", " ORDER BY a,c DESC", ""}) {
        const std::string tail = std::string(order) + " LIMIT 13 OFFSET 3";
        std::string direct = prefix;
        direct += filter;
        direct += " GROUP BY k";
        std::string reference = direct;
        reference += ",CAST(k AS STRING)";
        direct += tail;
        reference += tail;
        EXPECT_EQ(RelationalRun(*rs_, direct), RelationalRun(*rs_, reference));
      }
    }
    EXPECT_EQ(RelationalRun(*rs_,
                            "SELECT k,AVG(CAST(v AS FLOAT64)),SUM(v),AVG(v) "
                            "FROM NumericCodes GROUP BY k ORDER BY "
                            "COUNT(DISTINCT n) DESC LIMIT 17"),
              RelationalRun(*rs_,
                            "SELECT k,AVG(CAST(v AS FLOAT64)),SUM(v),AVG(v) "
                            "FROM NumericCodes GROUP BY k,CAST(k AS STRING) "
                            "ORDER BY COUNT(DISTINCT n) DESC LIMIT 17"));
  }
  const std::string query =
      prefix + " GROUP BY k ORDER BY c DESC,k NULLS FIRST LIMIT 7";
  const auto expected = RelationalRun(*rs_, query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "INSERT INTO NumericCodes VALUES (NULL,0,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result, engine.Execute(old, query));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, query),
            RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING) ORDER "
                                         "BY c DESC,k NULLS FIRST LIMIT 7"));
  ASSERT_SUCCESS(old.PreCommit());
  // A fresh append-only table keeps the bounds checks independent of the
  // fallback for NULLs and in-place row rewrites exercised above.
  RelationalRun(*rs_, "CREATE TABLE NumericBounds(v INT64,k INT64,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NumericBounds"));
    for (int64_t i = 0; i < 30000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_, Row({Value((i % 1009) - 504),
                                        Value((i % 307) * 10000001),
                                        Value((i % 1021) * 1000000001)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto require_clean_numeric_store = [&] {
    const auto gc_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), gc_deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NumericBounds"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    ASSERT_EQ(store->column_has_null[0], 0);
    ASSERT_SUCCESS(context.PreCommit());
  };
  // Promoting each argument to DOUBLE rounds away +1 before the sum. The
  // exact-integer shortcut must refuse these values rather than return 0.5.
  RelationalRun(
      *rs_,
      "INSERT INTO NumericBounds VALUES "
      "(9007199254740993,999999999999,0),(-9007199254740992,999999999999,0),"
      "(9223372036854775807,888888888888,0),(1,888888888888,0)");
  require_clean_numeric_store();
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT AVG(v),AVG(CAST(v AS FLOAT64)) "
                "FROM NumericBounds WHERE k=999999999999 GROUP BY k LIMIT 1"),
            (std::vector<Row>{Row({Value(0.0), Value(0.0)})}));
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT AVG(v),AVG(CAST(v AS FLOAT64)) "
                "FROM NumericBounds WHERE k=999999999999 GROUP BY k,n LIMIT 1"),
            (std::vector<Row>{Row({Value(0.0), Value(0.0)})}));
  // The overflowing group loses COUNT-based TopN, but must still fail.
  auto current = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(
      QueryResult, overflow,
      engine.Execute(current,
                     "SELECT k,SUM(v),COUNT(*) c,COUNT(DISTINCT n) "
                     "FROM NumericBounds GROUP BY k ORDER BY c DESC LIMIT 1"));
  static_cast<void>(overflow.Collect());
  EXPECT_EQ(overflow.GetStatus(), Status::kIsInfinity);
  ASSERT_SUCCESS(current.PreCommit());
  auto composite_context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(
      QueryResult, composite_overflow,
      engine.Execute(composite_context,
                     "SELECT k,n,SUM(v),COUNT(*) c FROM NumericBounds "
                     "GROUP BY k,n ORDER BY c DESC LIMIT 1"));
  static_cast<void>(composite_overflow.Collect());
  EXPECT_EQ(composite_overflow.GetStatus(), Status::kIsInfinity);
  ASSERT_SUCCESS(composite_context.PreCommit());
}

TEST_F(RelationalRegressionTest, MoreThanEightSlimAggregatesFoldCorrectly) {
  // The cell-streaming folds index a fixed std::array<Value, 8> scratch by
  // aggregate slot.  A slim plan with 9+ aggregates must take the generic
  // path instead of writing past the array (both the LSTable-direct fold
  // and the typed-fill fold_cell_row path).
  const Schema schema("ManySlimAggs", {Column("k", ValueType::kInt64),
                                       Column("a", ValueType::kInt64),
                                       Column("b", ValueType::kInt64)});
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    // >16384 rows makes the LSTable snapshot skip the relation cache and
    // select the direct column-store fold.
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_SUCCESS(
          table.Insert(setup.txn_, Row({Value(i % 4), Value(i), Value(i * 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::string wide =
      "SELECT k, COUNT(*), SUM(a), MIN(a), MAX(a), SUM(b), MIN(b), MAX(b),"
      " AVG(a), COUNT(DISTINCT a) FROM ManySlimAggs GROUP BY k ORDER BY k";
  const std::vector<Row> expected = {
      Row({Value(0), Value(5000), Value(49990000), Value(0), Value(19996),
           Value(99980000), Value(0), Value(39992), Value(9998.0),
           Value(5000)}),
      Row({Value(1), Value(5000), Value(49995000), Value(1), Value(19997),
           Value(99990000), Value(2), Value(39994), Value(9999.0),
           Value(5000)}),
      Row({Value(2), Value(5000), Value(50000000), Value(2), Value(19998),
           Value(100000000), Value(4), Value(39996), Value(10000.0),
           Value(5000)}),
      Row({Value(3), Value(5000), Value(50005000), Value(3), Value(19999),
           Value(100010000), Value(6), Value(39998), Value(10001.0),
           Value(5000)})};
  // Generic morsel fold before the column store exists.
  EXPECT_EQ(RelationalRun(*rs_, wide), expected);
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("ManySlimAggs"));
    ASSERT_NE(GetOrBuildLSTableStore(context.txn_, *table, true), nullptr);
    ASSERT_SUCCESS(context.PreCommit());
  }
  // LSTable-direct fold over the column store.
  EXPECT_EQ(RelationalRun(*rs_, wide), expected);
}

// The flat columnar window path computes a dense slot span as
// uint64(max) - uint64(min) + 1. The full [INT64_MIN, INT64_MAX] range wraps
// that span to 0, which used to enter the dense path with a 0/1-slot array
// and write far out of bounds (heap corruption). It must fall back instead.
TEST_F(RelationalRegressionTest, ColumnarWindowFullInt64PartitionRange) {
  auto context = rs_->BeginContext();
  const Schema schema(
      "w", {Column("p", ValueType::kInt64), Column("k", ValueType::kInt64)});
  std::vector<Row> rows;
  rows.reserve(4096);
  for (int64_t i = 0; i < 4096; ++i) {
    const int64_t key = (i % 2 == 0) ? std::numeric_limits<int64_t>::min()
                                     : std::numeric_limits<int64_t>::max();
    rows.emplace_back(std::vector<Value>{Value(key), Value(i)});
  }
  auto table = relational_detail::BuildColumnarTableFromRows(schema, rows);
  ASSERT_TRUE(table.HasValue());
  relational_detail::Relation input;
  input.schema = schema;
  input.SetColumnarTable(table.MoveValue(), false);
  ASSERT_TRUE(input.HasColumnarOnly());

  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("k"))};
  window->partition_by = {ColumnValueExp(ColumnName("p"))};
  const std::vector<const WindowFunctionCallExpression*> windows{window.get()};
  const std::vector<std::string> names{"$win0"};

  auto result = relational_detail::ExtendColumnarRelationWithWindows(
      context, std::move(input), windows, names, nullptr, {});
  ASSERT_TRUE(result.HasValue()) << result.GetStatus().GetMessage();
  EXPECT_EQ(result.Value().TotalRows(), rows.size());
  ASSERT_SUCCESS(context.PreCommit());
}

}  // namespace
}  // namespace tinylamb
