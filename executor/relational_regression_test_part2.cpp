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
TEST_F(RelationalRegressionTest, TypedAggregateUsesResolvedOrderNodes) {
  // The ORDER aggregate is intentionally a different AST object. The generic
  // stream resolver already binds it to the SELECT aggregate; typed planning
  // must use that resolved node too, including aggregates absent from SELECT.
  for (const bool hidden : {false, true}) {
    for (const bool descending : {false, true}) {
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto order_count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      std::vector<NamedExpression> projection{
          NamedExpression("key", ColumnValueExp("k"))};
      if (!hidden) {
        projection.emplace_back("c", count);
      }
      SelectStatement statement(projection, {}, nullptr);
      SelectSource source;
      source.table = "AggTable";
      statement.SetSources({source});
      statement.SetGroupBy({ColumnValueExp("k")});
      statement.SetOrderBy({{order_count, !descending, std::nullopt},
                            {ColumnValueExp("k"), true, std::nullopt}});
      statement.SetLimit(3);
      statement.SetOffset(2);
      auto context = rs_->BeginReadOnlyContext();
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      std::vector<Row> expected;
      expected.reserve(3);
      for (int64_t k = 2; k < 5; ++k) {
        expected.emplace_back(hidden ? Row({Value(k)})
                                     : Row({Value(k), Value(20)}));
      }
      EXPECT_EQ(result.rows, expected);
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) FROM AggTable GROUP BY k "
                          "ORDER BY COUNT(*) DESC,k LIMIT 3 OFFSET 2"),
            RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM AggTable GROUP BY k "
                          "ORDER BY c DESC,k LIMIT 3 OFFSET 2"));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,SUM(v) FROM AggTable GROUP BY k "
                          "ORDER BY SUM(v) DESC,k LIMIT 3"),
            RelationalRun(*rs_,
                          "SELECT k,SUM(v) s FROM AggTable GROUP BY k "
                          "ORDER BY s DESC,k LIMIT 3"));
}

namespace {

void RunWideIntegerCodesKeepGroupingAndDistinct(Database& db,
                                                int64_t cardinality) {
  const std::string name = "IntCodeInput" + std::to_string(cardinality);
  RelationalRun(db, "CREATE TABLE " + name + "(n INT64,k INT64,d DATE)");
  const int64_t rows = cardinality == 300 ? 12000 : cardinality * 4;
  {
    auto setup = db.BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, setup.GetTable(name));
    for (int64_t i = 0; i < rows; ++i) {
      int64_t key = ((i % cardinality) - (cardinality / 2)) * 1000000001;
      if (i == 0) {
        key = std::numeric_limits<int64_t>::min();
      } else if (i == 1) {
        key = std::numeric_limits<int64_t>::max();
      }
      const Value value = i > 1 && i % 17 == 0 ? Value() : Value(key);
      const Value date =
          i % 19 == 0 ? Value() : Value::DateFromDays((i % 300) - 150);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(i), value, date})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(db, "ANALYZE " + name);
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string predicate :
         {"", " WHERE k<>0", " WHERE k>=0", " WHERE k=9223372036854775807"}) {
      std::string actual = "SELECT k,COUNT(*) c FROM ";
      actual += name;
      actual += predicate;
      actual += " GROUP BY k ORDER BY c DESC,k NULLS FIRST LIMIT 19";
      std::string reference = "SELECT MIN(k) AS k,COUNT(*) c FROM ";
      reference += name;
      reference += predicate;
      reference +=
          " GROUP BY CAST(k AS STRING) ORDER BY c DESC,k NULLS FIRST LIMIT "
          "19";
      EXPECT_EQ(RelationalRun(db, actual), RelationalRun(db, reference));
    }
    EXPECT_EQ(
        RelationalRun(db, "SELECT k,d,COUNT(*) c FROM " + name +
                              " GROUP BY k,d ORDER BY c DESC,k NULLS "
                              "FIRST,d NULLS LAST LIMIT 19"),
        RelationalRun(db, "SELECT MIN(k) k,d,COUNT(*) c FROM " + name +
                              " GROUP BY CAST(k AS STRING),d ORDER BY c "
                              "DESC,k NULLS FIRST,d NULLS LAST LIMIT 19"));
    EXPECT_EQ(RelationalRun(db, "SELECT COUNT(DISTINCT k) FROM " + name),
              RelationalRun(
                  db, "SELECT COUNT(DISTINCT CAST(k AS STRING)) FROM " + name));
    std::string distinct_query = "SELECT COUNT(DISTINCT k) FROM ";
    distinct_query += name;
    distinct_query += " WHERE k<>0";
    std::string distinct_reference =
        "SELECT COUNT(DISTINCT CAST(k AS STRING)) FROM ";
    distinct_reference += name;
    distinct_reference += " WHERE k<>0";
    EXPECT_EQ(RelationalRun(db, distinct_query),
              RelationalRun(db, distinct_reference));
    EXPECT_EQ(RelationalRun(db, "SELECT COUNT(DISTINCT d) FROM " + name),
              RelationalRun(
                  db, "SELECT COUNT(DISTINCT CAST(d AS STRING)) FROM " + name));
    EXPECT_EQ(
        RelationalRun(
            db, "SELECT n FROM " + name + " WHERE k<0 AND n<50 ORDER BY n"),
        RelationalRun(db, "SELECT n FROM " + name +
                              " WHERE CAST(k AS STRING) LIKE '-%' AND n<50 "
                              "ORDER BY n"));
    EXPECT_EQ(
        RelationalRun(db, "SELECT d,COUNT(*) c FROM " + name +
                              " GROUP BY d ORDER BY d NULLS LAST LIMIT 11 "
                              "OFFSET 3"),
        RelationalRun(db, "SELECT d,COUNT(*) c FROM " + name +
                              " GROUP BY d,BYTE_LENGTH(CAST(d AS STRING)) "
                              "ORDER BY d NULLS LAST LIMIT 11 OFFSET 3"));
  }
  auto reader = db.BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, cardinality == 300 ? 2 : 4);
  EXPECT_EQ(store->dictionaries[2].wide_code_bytes, 2);
  for (const auto& block : store->blocks) {
    const auto& column = block.columns[1];
    for (size_t row = 0; row < block.row_count; ++row) {
      if (!column.IsNull(row)) {
        EXPECT_EQ(
            store->dictionaries[1].int_entries[column.DictionaryCodeAt(row)],
            column.int_cells[row]);
      }
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

}  // namespace

TEST_F(RelationalRegressionTest, WideIntegerCodesKeepGroupingAndDistinctSmall) {
  RunWideIntegerCodesKeepGroupingAndDistinct(*rs_, 300);
}

TEST_F(RelationalRegressionTest, WideIntegerCodesKeepGroupingAndDistinctLarge) {
  RunWideIntegerCodesKeepGroupingAndDistinct(*rs_, 65536);
}

TEST_F(RelationalRegressionTest, DateStringFiltersCompileOnce) {
  const Schema schema("input", {Column("d", ValueType::kDate)});
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ConstantValueExp(Value(1)));
  const SelectStatement statement({NamedExpression("n", count)}, {}, nullptr);
  for (const std::string literal : {"1970-01-16", "1970-1-16"}) {
    const auto predicate = BinaryExpressionExp(
        ColumnValueExp("d"), BinaryOperation::kGreaterThanEquals,
        ConstantValueExp(Value(std::string(literal))));
    const auto plan = relational_detail::PlanTypedAggregation(
        statement, nullptr, schema, {count.get()}, {predicate});
    ASSERT_TRUE(plan.eligible);
    ASSERT_EQ(plan.filter_compares.size(), 1);
    EXPECT_TRUE(plan.filter_programs.empty());
    EXPECT_EQ(plan.filter_compares.front().int_constant, 15);
  }
  for (const std::string literal : {"1970-02-30", "1970-01-16suffix", ""}) {
    EXPECT_FALSE(relational_detail::TryCompileTypedFilterCompare(
        BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(std::string(literal)))),
        schema, true));
  }
}

TEST_F(RelationalRegressionTest,
       IntegerMembershipPreservesSnapshotAndCandidates) {
  RelationalRun(*rs_, "CREATE TABLE MembershipInput(k INT64,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("MembershipInput"));
    for (int64_t i = 0; i < 40000; ++i) {
      const Value key = i % 29 == 0 ? Value() : Value((i % 10003) * 3);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({key, Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(table
                       ->Insert(setup.txn_,
                                Row({Value(std::numeric_limits<int64_t>::min()),
                                     Value(-2)}))
                       .GetStatus());
    ASSERT_SUCCESS(table
                       ->Insert(setup.txn_,
                                Row({Value(std::numeric_limits<int64_t>::max()),
                                     Value(-1)}))
                       .GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE MembershipInput");
  for (int64_t target = 0; target < 20; ++target) {
    const std::string value = std::to_string(target);
    std::vector<Row> expected;
    expected.reserve(40000);
    for (int64_t i = 0; i < 40000; ++i) {
      if (i % 29 != 0 && (i % 10003) * 3 == target) {
        expected.emplace_back(std::vector<Value>{Value(target), Value(i)});
      }
    }
    const std::string sql = "SELECT k,v FROM MembershipInput WHERE k=" + value;
    EXPECT_EQ(RelationalRun(*rs_, sql + " ORDER BY v"), expected);
  }
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        old.GetTable("MembershipInput"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_TRUE(store->MayContainInteger(0, std::numeric_limits<int64_t>::min()));
  EXPECT_TRUE(store->MayContainInteger(0, std::numeric_limits<int64_t>::max()));
  int64_t absent = 1;
  while (store->MayContainInteger(0, absent)) {
    absent += 3;
    ASSERT_LT(absent, 30000);
  }
  const std::string literal = std::to_string(absent);
  std::string insert = "INSERT INTO MembershipInput VALUES (" + literal;
  insert += ",-3)";
  RelationalRun(*rs_, insert);
  const std::string sql = "SELECT v FROM MembershipInput WHERE k=" + literal;
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, result, engine.Execute(old, sql));
  EXPECT_TRUE(result.Collect().empty());
  ASSERT_SUCCESS(result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, sql), std::vector<Row>{Row({Value(-3)})});
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DependentDictionaryKeysKeepOrderErrorsAndSnapshots) {
  RelationalRun(*rs_, "CREATE TABLE DictionaryDependent(v INT64,k INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("DictionaryDependent"));
    for (int64_t i = 0; i < 90000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({Value(i % 5),
                       i % 19 == 0 ? Value() : Value((i % 20011) * 10000001)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto await_gc = [&] {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };
  await_gc();
  const auto key = ColumnValueExp("k");
  const auto minus = BinaryExpressionExp(key, BinaryOperation::kSubtract,
                                         ConstantValueExp(Value(1)));
  const auto twice = BinaryExpressionExp(key, BinaryOperation::kMultiply,
                                         ConstantValueExp(Value(2)));
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("d", minus),
       NamedExpression("e", twice), NamedExpression("c", count)},
      {}, nullptr);
  SelectSource source;
  source.table = "DictionaryDependent";
  statement.SetSources({source});
  statement.SetGroupBy({twice, key, minus});
  statement.SetOrderBy({{minus, false, true}, {count, false, std::nullopt}});
  statement.SetLimit(11);
  statement.SetOffset(3);
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("DictionaryDependent"));
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
    EXPECT_EQ(result.rows,
              RelationalRun(
                  *rs_,
                  "SELECT k,k-1 d,k*2 e,COUNT(*) c FROM DictionaryDependent "
                  "GROUP BY k*2,k,k-1,CAST(k AS STRING) "
                  "ORDER BY d DESC NULLS FIRST,c DESC LIMIT 11 OFFSET 3"));
    ASSERT_SUCCESS(context.PreCommit());
    EXPECT_EQ(RelationalRun(*rs_,
                            "SELECT k,((k+3)*7)-k d,COUNT(*) c FROM "
                            "DictionaryDependent GROUP BY k,((k+3)*7)-k "
                            "ORDER BY d DESC NULLS FIRST LIMIT 11 OFFSET 2"),
              RelationalRun(*rs_,
                            "SELECT k,((k+3)*7)-k d,COUNT(*) c FROM "
                            "DictionaryDependent GROUP BY k,((k+3)*7)-k,"
                            "CAST(k AS STRING) "
                            "ORDER BY d DESC NULLS FIRST LIMIT 11 OFFSET 2"));
    for (const std::string_view tail :
         {" ORDER BY k-1 NULLS FIRST LIMIT 13 OFFSET 2",
          " ORDER BY c DESC LIMIT 17", " ORDER BY c DESC LIMIT 3 WITH TIES",
          " LIMIT 0"}) {
      for (const std::string_view selected :
           {"k,k-1,COUNT(*) c", "COUNT(*) c"}) {
        const std::string query = "SELECT " + std::string(selected) +
                                  " FROM DictionaryDependent GROUP BY k,k-1";
        EXPECT_EQ(RelationalRun(*rs_, query + std::string(tail)),
                  RelationalRun(
                      *rs_, query + ",CAST(k AS STRING)" + std::string(tail)));
      }
    }
  }
  const std::string snapshot_query =
      "SELECT k,k-1,COUNT(*) c FROM DictionaryDependent "
      "GROUP BY k,k-1 ORDER BY c DESC,k NULLS FIRST LIMIT 7";
  const auto expected = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_,
                "INSERT INTO DictionaryDependent VALUES "
                "(0,-9223372036854775808)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  ASSERT_SUCCESS(old.PreCommit());
  await_gc();
  // Force the fresh dictionary snapshot: the invalid key must still be
  // evaluated even though its count cannot win the LIMIT below.
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("DictionaryDependent"));
    ASSERT_NE(BuildLSTableStore(context.txn_, *table), nullptr);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const Schema key_schema("", {Column("k", ValueType::kInt64)});
  const auto expected_error = minus->TryEvaluate(
      Row({Value(std::numeric_limits<int64_t>::min())}), key_schema);
  ASSERT_FALSE(expected_error.HasValue());
  for (const std::string_view selected : {"k,k-1,COUNT(*) c", "COUNT(*) c"}) {
    auto context = rs_->BeginReadOnlyContext();
    auto result =
        engine.Execute(context, "SELECT " + std::string(selected) +
                                    " FROM DictionaryDependent GROUP BY k,k-1 "
                                    "ORDER BY c DESC LIMIT 1");
    if (result.HasValue()) {
      result.Value().Drain();
      EXPECT_EQ(result.Value().GetStatus(), expected_error.GetStatus());
    } else {
      EXPECT_EQ(result.GetStatus(), expected_error.GetStatus());
    }
  }
  for (const std::string_view predicate : {"k>0", "k=-1"}) {
    const std::string query =
        "SELECT k,k-1,COUNT(*) c FROM DictionaryDependent WHERE " +
        std::string(predicate) + " GROUP BY k,k-1";
    EXPECT_EQ(RelationalRun(*rs_, query + " ORDER BY c DESC,k LIMIT 7"),
              RelationalRun(*rs_, query + ",CAST(k AS STRING) "
                                          "ORDER BY c DESC,k LIMIT 7"));
  }
}

TEST_F(RelationalRegressionTest, DependentIntegerGroupKeysKeepErrorsAndNulls) {
  const auto key = ColumnValueExp("k");
  const auto dependent = BinaryExpressionExp(key, BinaryOperation::kSubtract,
                                             ConstantValueExp(Value(1)));
  auto count = std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                     ColumnValueExp("*"));
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("d", dependent),
       NamedExpression("c", count)},
      {}, nullptr);
  statement.SetGroupBy({dependent, key});
  const Schema schema("input", {Column("k", ValueType::kInt64)});
  const auto plan = relational_detail::PlanTypedAggregation(
      statement, nullptr, schema, {count.get()}, {});
  ASSERT_TRUE(plan.eligible);
  ASSERT_EQ(plan.key_offsets.size(), 1U);
  ASSERT_EQ(plan.dependent_key_programs.size(), 1U);
  ASSERT_EQ(plan.dependent_key_inputs.size(), 1U);
  ASSERT_EQ(plan.emit_items[1].dependent_key, size_t{0});
  Column unsigned_key("k", ValueType::kInt64);
  unsigned_key.SetUnsigned(true);
  EXPECT_FALSE(relational_detail::PlanTypedAggregation(
                   statement, nullptr, Schema("input", {unsigned_key}),
                   {count.get()}, {})
                   .eligible);
  RelationalRun(*rs_, "CREATE TABLE DependentKeys(k INT64,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("DependentKeys"));
    for (int64_t i = 0; i < 40000; ++i) {
      ASSERT_SUCCESS(table
                         ->Insert(setup.txn_,
                                  Row({i % 19 == 0 ? Value() : Value(i % 10003),
                                       Value(i % 7)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE DependentKeys");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string tail :
         {" ORDER BY k", " ORDER BY k-1 DESC LIMIT 17 OFFSET 3",
          " HAVING COUNT(*)>2 ORDER BY k LIMIT 7"}) {
      const std::string base =
          "SELECT k,k-1,k-2,k-3,COUNT(*) FROM DependentKeys "
          "GROUP BY k-2,k,k-1,k-3";
      std::string reference = base + ",CAST(k AS STRING)";
      reference += tail;
      EXPECT_EQ(RelationalRun(*rs_, base + tail),
                RelationalRun(*rs_, reference));
    }
    const std::string multi =
        "SELECT k,v,k+v,COUNT(*) FROM DependentKeys GROUP BY k,v,k+v";
    EXPECT_EQ(RelationalRun(*rs_, multi + " ORDER BY k,v LIMIT 23"),
              RelationalRun(
                  *rs_, multi + ",CAST(k AS STRING) ORDER BY k,v LIMIT 23"));
  }
  RelationalRun(*rs_,
                "INSERT INTO DependentKeys VALUES (-9223372036854775808,0)");
  const auto expected_error = dependent->TryEvaluate(
      Row({Value(std::numeric_limits<int64_t>::min())}), schema);
  ASSERT_FALSE(expected_error.HasValue());
  for (const std::string output : {"k,k-1,COUNT(*) c", "COUNT(*) c"}) {
    auto context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    auto result = engine.Execute(
        context,
        "SELECT " + output +
            " FROM DependentKeys GROUP BY k,k-1 ORDER BY c DESC LIMIT 1");
    if (result.HasValue()) {
      result.Value().Drain();
      EXPECT_EQ(result.Value().GetStatus(), expected_error.GetStatus());
    } else {
      EXPECT_EQ(result.GetStatus(), expected_error.GetStatus());
    }
    context.Abort();
  }
}

TEST_F(RelationalRegressionTest, UnionAggregateArgumentsKeepPromotionAndOrder) {
  RelationalRun(*rs_, "CREATE TABLE UnionArgumentA(x INT64,y DOUBLE)");
  RelationalRun(*rs_, "CREATE TABLE UnionArgumentB(a INT64,b DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO UnionArgumentA VALUES "
                "(1,1e16),(1,1),(1,-1e16),(1,1),(NULL,9),(2,NULL)");
  RelationalRun(*rs_, "INSERT INTO UnionArgumentB VALUES (1,3),(1,3),(2,4)");
  const std::string terms =
      "SELECT x,y FROM UnionArgumentA UNION ALL SELECT a,b FROM UnionArgumentB";
  for (const std::string aggregate : {"SUM", "AVG", "MIN", "MAX"}) {
    const std::string prefix = "SELECT " + aggregate + "(u.x*u.y) FROM (";
    // A derived LIMIT keeps the reference arithmetic above the append.
    EXPECT_EQ(RelationalRun(*rs_, prefix + terms + ") u"),
              RelationalRun(*rs_, prefix + terms + " LIMIT 1000) u"));
    EXPECT_EQ(RelationalRun(*rs_, prefix + terms + " WHERE a<0) u"),
              RelationalRun(*rs_, prefix + terms + " WHERE a<0 LIMIT 1000) u"));
  }
  EXPECT_EQ(RelationalRun(*rs_, "SELECT SUM(x*y) FROM (" + terms + ") u"),
            std::vector<Row>{Row({Value(15.0)})});
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT AVG(x*y) FROM (SELECT x,y FROM UnionArgumentA "
                    "WHERE x<0 UNION ALL SELECT a,b FROM UnionArgumentB "
                    "WHERE a<0) u"),
      std::vector<Row>{Row({Value()})});
  // Promotion must precede multiplication: doing INT64 arithmetic in the
  // first branch would overflow, while the original UNION produces DOUBLE.
  RelationalRun(*rs_, "CREATE TABLE UnionArgumentInt(a INT64,b INT64)");
  RelationalRun(*rs_, "CREATE TABLE UnionArgumentDouble(a DOUBLE,b INT64)");
  RelationalRun(*rs_,
                "INSERT INTO UnionArgumentInt VALUES (9223372036854775807,2)");
  RelationalRun(*rs_, "INSERT INTO UnionArgumentDouble VALUES (1.0,2)");
  const std::string promoted =
      "SELECT a,b FROM UnionArgumentInt UNION ALL "
      "SELECT a,b FROM UnionArgumentDouble";
  const std::vector<Row> promoted_sum{Row({Value(std::ldexp(1.0, 64))})};
  EXPECT_EQ(RelationalRun(*rs_, "SELECT SUM(a*b) FROM (" + promoted + ") u"),
            promoted_sum);
  EXPECT_EQ(RelationalRun(
                *rs_, "SELECT SUM(a*b) FROM (" + promoted + " LIMIT 1000) u"),
            promoted_sum);
  const std::string distinct =
      "SELECT x,y FROM UnionArgumentA UNION DISTINCT SELECT a,b FROM "
      "UnionArgumentB";
  EXPECT_EQ(RelationalRun(*rs_, "SELECT SUM(x*y) FROM (" + distinct + ") u"),
            RelationalRun(
                *rs_, "SELECT SUM(x*y) FROM (" + distinct + " LIMIT 1000) u"));
}

TEST_F(RelationalRegressionTest, ParallelWindowPrefixesKeepArithmeticOrder) {
  auto context = rs_->BeginContext();
  Schema schema("",
                {Column("p", ValueType::kInt64), Column("k", ValueType::kInt64),
                 Column("v", ValueType::kDouble)});
  std::vector<Row> rows;
  rows.reserve(32768);
  const std::array<Value, 8> values{Value(1.0e16), Value(1.0), Value(-1.0e16),
                                    Value(1.0),    Value(),    Value(-0.0),
                                    Value(0.0),    Value(2.5)};
  for (size_t i = 0; i < 32768; ++i) {
    rows.emplace_back(std::vector<Value>{Value(i % 64), Value((i / 64) / 2),
                                         values[(i / 64) % values.size()]});
  }
  for (const std::string function : {"SUM", "AVG", "MIN", "MAX", "COUNT"}) {
    for (const bool whole : {false, true}) {
      for (const WindowFrameUnit unit :
           {WindowFrameUnit::kRows, WindowFrameUnit::kRange}) {
        WindowFunctionCallExpression window;
        window.function = function;
        window.args = {ColumnValueExp("v")};
        window.partition_by = {ColumnValueExp("p")};
        if (!whole) {
          window.order_by = {{ColumnValueExp("k"), true, false}};
          window.has_frame = true;
          window.frame_unit = unit;
          window.frame_start.type = WindowFrameBoundType::kUnboundedPreceding;
          window.frame_end.type = WindowFrameBoundType::kCurrentRow;
        }
        std::vector<Value> reference;
        for (const size_t workers : {size_t{1}, size_t{2}}) {
          std::atomic<size_t> active{0};
          ScopedQueryParallelismBudget budget(&active, workers);
          std::vector<Value> output;
          ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
              context, window, rows, schema, nullptr, {}, &output));
          if (workers == 1) {
            reference = std::move(output);
          } else {
            EXPECT_EQ(output, reference) << function << ' ' << whole;
          }
        }
      }
    }
  }
  // Parallel workers must propagate the same checked integer prefix error.
  for (auto& row : rows) {
    row.values_[2] = Value(1);
  }
  rows.front().values_[2] = Value(INT64_MAX);
  WindowFunctionCallExpression window;
  window.function = "SUM";
  window.args = {ColumnValueExp("v")};
  window.partition_by = {ColumnValueExp("p")};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    std::vector<Value> output;
    EXPECT_EQ(relational_detail::ComputeOneWindow(context, window, rows, schema,
                                                  nullptr, {}, &output),
              Status::kIsInfinity);
  }
}

TEST_F(RelationalRegressionTest, GroupedLeftInputsKeepMultiplicityAndLimits) {
  RelationalRun(*rs_, "CREATE TABLE DomainLeft(id INT64,a INT64,b INT64)");
  RelationalRun(*rs_,
                "INSERT INTO DomainLeft VALUES "
                "(1,1,10),(1,1,10),(2,1,10),(3,NULL,10),(4,9,99),(5,2,20)");
  RelationalRun(*rs_,
                "CREATE TABLE DomainRight(a INT64,b INT64,v INT64,"
                "__TINYlamb_group_domain0 INT64)");
  RelationalRun(*rs_,
                "INSERT INTO DomainRight VALUES "
                "(1,10,2,0),(1,10,3,0),(1,99,999,0),(2,20,NULL,0),"
                "(8,80,7,0),(NULL,10,4,0)");
  const auto query = [](std::string_view barrier) {
    const std::string body =
        "SELECT a,b,SUM(v+__TINYlamb_group_domain0) s FROM DomainRight "
        "GROUP BY a,b " +
        std::string(barrier);
    return "SELECT p.id,p.a,p.b,r.s,t.s FROM "
           "(SELECT id,a,b,COUNT(*) n FROM DomainLeft GROUP BY id,a,b) p "
           "LEFT JOIN (" +
           body +
           ") r ON p.a=r.a AND p.b=r.b "
           "LEFT JOIN (" +
           body + ") t ON p.a=t.a AND p.b=t.b ORDER BY p.id";
  };
  const auto rows = RelationalRun(*rs_, query(""));
  EXPECT_EQ(rows, RelationalRun(*rs_, query("HAVING COUNT(*)>=0")));
  EXPECT_EQ(rows,
            std::vector<Row>(
                {Row({Value(1), Value(1), Value(10), Value(5), Value(5)}),
                 Row({Value(2), Value(1), Value(10), Value(5), Value(5)}),
                 Row({Value(3), Value(), Value(10), Value(), Value()}),
                 Row({Value(4), Value(9), Value(99), Value(), Value()}),
                 Row({Value(5), Value(2), Value(20), Value(), Value()})}));
  // LIMIT selects an unmatched group before the outer join. It is a barrier
  // even though removing that group first would produce a matching one.
  const auto limited = RelationalRun(*rs_, query("ORDER BY b DESC,a LIMIT 1"));
  ASSERT_EQ(limited.size(), 5U);
  for (const auto& row : limited) {
    EXPECT_TRUE(row[3].IsNull());
    EXPECT_TRUE(row[4].IsNull());
  }
  const std::string bare_join =
      "SELECT id,pa,pb,s FROM "
      "(SELECT id,a pa,b pb,COUNT(*) n FROM DomainLeft GROUP BY id,a,b) p "
      "LEFT JOIN (SELECT a ra,b rb,SUM(v) s FROM DomainRight GROUP BY a,b) r ";
  const auto bare =
      RelationalRun(*rs_, bare_join + "ON pa=ra AND pb=rb ORDER BY id");
  EXPECT_EQ(bare,
            RelationalRun(
                *rs_, bare_join + "ON p.pa=r.ra AND p.pb=r.rb ORDER BY p.id"));
  ASSERT_EQ(bare.size(), 5U);
  EXPECT_EQ(bare[0][3], Value(5));
  EXPECT_EQ(bare[1][3], Value(5));
}

TEST_F(RelationalRegressionTest,
       OwnedProjectionPreservesReorderingAndDuplicates) {
  RelationalRun(*rs_,
                "CREATE TABLE OwnedProjection (k INT64, s STRING, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO OwnedProjection VALUES "
                "(1, 'long string payload for ownership transfer', 9), "
                "(2, NULL, 8)");
  const auto reordered =
      RelationalRun(*rs_,
                    "WITH c AS (SELECT v, s, k FROM OwnedProjection LIMIT 10) "
                    "SELECT k, s, v FROM c ORDER BY k");
  ASSERT_EQ(reordered.size(), 2U);
  EXPECT_EQ(reordered[0],
            Row({Value(1), Value("long string payload for ownership transfer"),
                 Value(9)}));
  EXPECT_EQ(reordered[1], Row({Value(2), Value(), Value(8)}));
  const auto duplicate = RelationalRun(
      *rs_,
      "WITH c AS (SELECT s, k, s AS t FROM OwnedProjection LIMIT 10) "
      "SELECT k, s, t FROM c ORDER BY k");
  ASSERT_EQ(duplicate.size(), 2U);
  EXPECT_EQ(duplicate[0][1], duplicate[0][2]);
  EXPECT_EQ(duplicate[0][1], reordered[0][1]);
  EXPECT_TRUE(duplicate[1][1].IsNull());
  EXPECT_TRUE(duplicate[1][2].IsNull());
  const auto narrowed = RelationalRun(
      *rs_,
      "SELECT * FROM (SELECT v FROM OwnedProjection LIMIT 10) c ORDER BY v");
  ASSERT_EQ(narrowed.size(), 2U);
  EXPECT_EQ(narrowed[0], Row({Value(8)}));
  EXPECT_EQ(narrowed[1], Row({Value(9)}));
}

TEST_F(RelationalRegressionTest, BoundedProjectionSelectsPositionsBeforeRows) {
  const std::string from =
      " FROM (SELECT 3 AS k,'b' AS s UNION ALL SELECT 1,'a' "
      "UNION ALL SELECT 2,NULL UNION ALL SELECT 4,'a' "
      "UNION ALL SELECT 5,'z' UNION ALL SELECT 6,'b') t ";
  const std::vector<std::string> queries{
      "SELECT *" + from + "ORDER BY s DESC NULLS LAST,k ASC",
      "SELECT s AS k,k AS s,k AS duplicate" + from +
          "ORDER BY k DESC NULLS LAST,t.k ASC",
      "SELECT s,k,s AS duplicate" + from + "ORDER BY 1 NULLS FIRST,2 DESC",
      "SELECT s" + from + "ORDER BY t.k DESC",
      "SELECT s,k" + from + "ORDER BY s COLLATE 'und:ci',k DESC"};
  for (const auto& query : queries) {
    SCOPED_TRACE(query);
    const auto all = RelationalRun(*rs_, query);
    ASSERT_EQ(all.size(), 6U);
    EXPECT_EQ(RelationalRun(*rs_, query + " LIMIT 3 OFFSET 1"),
              std::vector<Row>(all.begin() + 1, all.begin() + 4));
    EXPECT_EQ(RelationalRun(*rs_, query + " LIMIT 20"), all);
    EXPECT_TRUE(RelationalRun(*rs_, query + " LIMIT 0").empty());
    EXPECT_TRUE(RelationalRun(*rs_, query + " LIMIT 3 OFFSET 20").empty());
  }
}

TEST_F(RelationalRegressionTest, BoundRoundProjectionKeepsNullsAndPrecision) {
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT ROUND(x / 2.0, p) FROM (
      SELECT 15 AS x, 0 AS p UNION ALL SELECT -15,0
      UNION ALL SELECT NULL,0 UNION ALL SELECT 15,NULL
      UNION ALL SELECT 15,1
    ) t
  )sql"),
      std::vector<Row>({Row({Value(8.0)}), Row({Value(-8.0)}), Row({Value()}),
                        Row({Value()}), Row({Value(7.5)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT ROUND(k, -9223372036854775807-1)
    FROM (SELECT k FROM SmallKeys LIMIT 1) t
  )sql"),
            std::vector<Row>({Row({Value(0)})}));
}

TEST_F(RelationalRegressionTest,
       BoundProjectionReusesBuffersAfterReadingAllInputs) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT k+v,k*2,k FROM (
      SELECT k,v,k AS extra FROM AggTable WHERE k=2 LIMIT 2
    ) t
  )sql"),
            std::vector<Row>({Row({Value(3), Value(4), Value(2)}),
                              Row({Value(3), Value(4), Value(2)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COALESCE(a,b),a,a FROM (
      SELECT CAST(NULL AS STRING) AS a,'payload' AS b,0 AS spare
      UNION ALL SELECT 'second','unused',0
    ) t
  )sql"),
            std::vector<Row>(
                {Row({Value("payload"), Value(), Value()}),
                 Row({Value("second"), Value("second"), Value("second")})}));
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto prepared = engine.Prepare(context, R"sql(
    SELECT k/(v-v),k FROM (SELECT k,v FROM AggTable LIMIT 1) t
  )sql");
  ASSERT_TRUE(prepared.HasValue());
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
  context.Abort();
}

TEST_F(RelationalRegressionTest, NumericProjectionAccountsForLateTextValues) {
  auto context = rs_->BeginContext();
  for (const bool text : {false, true}) {
    relational_detail::Relation input;
    input.schema = Schema(
        "", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
             Column("s1", ValueType::kInt64), Column("s2", ValueType::kInt64),
             Column("s3", ValueType::kInt64)});
    input.AddRow(Row({Value(1), Value(), Value(), Value(), Value()}));
    input.AddRow(Row({Value(2), Value(), Value(), Value(), Value()}));
    const Value fallback = text ? Value(std::string(4096, 'x')) : Value(3);
    input.AddRow(Row({Value(), fallback, Value(), Value(), Value()}));
    input.AddRow(Row({Value(4), Value(), Value(), Value(), Value()}));
    const size_t original_charge = input.charged_bytes_;
    SelectStatement statement(
        {NamedExpression(
             "v", FunctionCallExp("coalesce",
                                  {ColumnValueExp("a"), ColumnValueExp("b")})),
         NamedExpression("a", ColumnValueExp("a"))},
        {}, nullptr);
    auto result = relational_detail::FinishQuery(
        context, statement, std::move(input), nullptr, {}, false);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    EXPECT_EQ(result.Value().rows,
              std::vector<Row>(
                  {Row({Value(1), Value(1)}), Row({Value(2), Value(2)}),
                   Row({fallback, Value()}), Row({Value(4), Value(4)})}));
    if (!text) {
      EXPECT_EQ(result.Value().charged_bytes_, original_charge);
    } else {
      size_t expected_charge = 0;
      for (const auto& row : result.Value().rows) {
        expected_charge += EstimateRowBytes(row);
      }
      EXPECT_EQ(result.Value().charged_bytes_, expected_charge);
    }
  }
}

TEST_F(RelationalRegressionTest, SharedGroupedCteResolvesQualifiedGroupKeys) {
  const std::string sql = R"sql(
    WITH c AS (SELECT a.k AS key, SUM(a.v) AS n FROM AggTable a GROUP BY k)
    SELECT SUM(x.n+y.n) FROM c x,c y WHERE x.key=1 AND y.key=2
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql), std::vector<Row>({Row({Value(40)})}));
  const std::string profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("input_rows=40"), std::string::npos) << profile;
}

TEST_F(RelationalRegressionTest, SharedGroupedCteWrapperKeepsDomainsAndLimits) {
  const std::string body =
      "SELECT key,n FROM (SELECT k AS key, SUM(v) AS n FROM AggTable "
      "GROUP BY k) g WHERE n > 0";
  const std::string consumers =
      " SELECT SUM(x.n+y.n) FROM c x,c y WHERE x.key=1 AND y.key=2";
  EXPECT_EQ(RelationalRun(*rs_, "WITH c AS (" + body + ")" + consumers),
            std::vector<Row>({Row({Value(40)})}));
  // A LIMIT applies before the outer restriction, even when the projected
  // columns themselves are direct group keys.
  EXPECT_EQ(RelationalRun(*rs_, "WITH c AS (" + body +
                                    " ORDER BY key DESC LIMIT 2)" + consumers),
            std::vector<Row>({Row({Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, "WITH c AS (" + body + R"sql(
    ) SELECT SUM(x.n+y.n) FROM c x,c y WHERE x.key=1
  )sql"),
            std::vector<Row>({Row({Value(400)})}));
}

TEST_F(RelationalRegressionTest, DerivedGroupFiltersRespectWindowAndRollup) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT g.key,g.n FROM (
      SELECT a.k AS key,SUM(a.v) AS n FROM AggTable a,SmallKeys d
      WHERE a.k=d.k GROUP BY a.k
    ) g WHERE (g.key=1 OR g.key=2) AND g.n>10 ORDER BY g.key
  )sql"),
            std::vector<Row>(
                {Row({Value(1), Value(20)}), Row({Value(2), Value(20)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT g.n FROM (
      SELECT k AS key,SUM(v) AS n FROM AggTable GROUP BY ROLLUP(k)
    ) g WHERE g.key IS NULL
  )sql"),
            std::vector<Row>({Row({Value(200)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT g.running FROM (
      SELECT k AS key,SUM(SUM(v)) OVER (ORDER BY k
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
      FROM AggTable GROUP BY k
    ) g WHERE g.key=2
  )sql"),
            std::vector<Row>({Row({Value(60)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT g.n FROM (
      SELECT k AS key,SUM(v) AS n FROM AggTable GROUP BY k ORDER BY k LIMIT 1
    ) g WHERE g.key=2
  )sql"),
            std::vector<Row>());
}

TEST_F(RelationalRegressionTest,
       UnionJoinProjectionFiltersKeepOwnersAndWindows) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT key,SUM(v) FROM (
      SELECT a.k AS key,d.k AS v FROM AggTable a
      LEFT JOIN SmallKeys d ON a.k=d.k
      UNION DISTINCT
      SELECT a.k AS key,d.k AS v FROM AggTable a
      LEFT JOIN SmallKeys d ON a.k=d.k
    ) u WHERE key=2 GROUP BY key
  )sql"),
            std::vector<Row>({Row({Value(2), Value(2)})}));
  // The selected key belongs to the second source, not the first one.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT key,COUNT(*) FROM (
      SELECT d.k AS key,a.v AS v FROM AggTable a
      LEFT JOIN SmallKeys d ON a.k=d.k
      UNION ALL
      SELECT d.k AS key,a.v AS v FROM AggTable a
      LEFT JOIN SmallKeys d ON a.k=d.k
    ) u WHERE key=2 GROUP BY key
  )sql"),
            std::vector<Row>({Row({Value(2), Value(40)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM (
      SELECT d.k AS key,a.v AS v FROM AggTable a
      LEFT JOIN SmallKeys d ON a.k=d.k
      UNION ALL
      SELECT d.k AS key,a.v AS v FROM AggTable a
      LEFT JOIN SmallKeys d ON a.k=d.k
    ) u WHERE key IS NULL
  )sql"),
            std::vector<Row>({Row({Value(240)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT rn FROM (
      SELECT k,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM SmallKeys
    ) u WHERE k=3
  )sql"),
            std::vector<Row>({Row({Value(3)})}));
}

TEST_F(RelationalRegressionTest,
       SharedGroupedDerivedCtePreservesUnionAndLimit) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH c AS (SELECT key, SUM(v) AS n FROM
      (SELECT k AS key, v FROM AggTable
       UNION DISTINCT SELECT k AS other,1 AS v FROM SmallKeys) u GROUP BY key)
    SELECT a.key,b.key,a.n,b.n FROM c a,c b WHERE a.key=1 AND b.key=2
  )sql"),
            std::vector<Row>({Row({Value(1), Value(2), Value(1), Value(1)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH c AS (SELECT key, SUM(v) AS n FROM
      (SELECT k AS key, v FROM AggTable ORDER BY k LIMIT 1) u GROUP BY key)
    SELECT COUNT(*) FROM c a,c b WHERE a.key=1 AND b.key=2
  )sql"),
            std::vector<Row>({Row({Value(0)})}));
}

TEST_F(RelationalRegressionTest, SharedGroupingInputPreservesSetsAndFilters) {
  const std::string sql = R"sql(
    SELECT a.k, b.k AS other, SUM(a.v) AS n FROM AggTable a, SmallKeys b
    WHERE a.k=b.k AND a.k<=2
    GROUP BY ROLLUP(a.k,b.k) ORDER BY a.k NULLS FIRST, other NULLS FIRST
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(), Value(), Value(40)}),
                              Row({Value(1), Value(), Value(20)}),
                              Row({Value(1), Value(1), Value(20)}),
                              Row({Value(2), Value(), Value(20)}),
                              Row({Value(2), Value(2), Value(20)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT a.k,b.k,SUM(a.v),COUNT(*) FROM AggTable a, SmallKeys b
    WHERE a.k=b.k AND a.k=99 GROUP BY ROLLUP(a.k,b.k)
  )sql"),
            std::vector<Row>({Row({Value(), Value(), Value(), Value(0)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT k, SUM(v) FROM AggTable WHERE k=1 GROUP BY k
    UNION ALL SELECT k,SUM(v) FROM AggTable WHERE k=2 GROUP BY k
    UNION ALL SELECT k,SUM(v) FROM AggTable WHERE k=3 GROUP BY k
    ORDER BY k
  )sql"),
      std::vector<Row>({Row({Value(1), Value(20)}), Row({Value(2), Value(20)}),
                        Row({Value(3), Value(20)})}));
}

TEST_F(RelationalRegressionTest,
       SharedGroupingWindowsAndMembershipKeepSemantics) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT k,v,SUM(v),RANK() OVER (PARTITION BY GROUPING(k)+GROUPING(v)
                                    ORDER BY SUM(v) DESC)
    FROM AggTable WHERE k IN (
      SELECT key FROM (SELECT k AS key, RANK() OVER (ORDER BY k) AS r
                       FROM SmallKeys) d WHERE r<=2)
    GROUP BY ROLLUP(k,v) ORDER BY k NULLS FIRST,v NULLS FIRST
  )sql"),
            std::vector<Row>({Row({Value(), Value(), Value(40), Value(1)}),
                              Row({Value(1), Value(), Value(20), Value(1)}),
                              Row({Value(1), Value(1), Value(20), Value(1)}),
                              Row({Value(2), Value(), Value(20), Value(1)}),
                              Row({Value(2), Value(1), Value(20), Value(1)})}));
}

TEST_F(RelationalRegressionTest, UnionTransferPreservesTypeCoercionAndNulls) {
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT 1 AS n UNION ALL SELECT 2.5 UNION ALL SELECT NULL "
                    "ORDER BY n NULLS LAST"),
      std::vector<Row>({Row({Value(1.0)}), Row({Value(2.5)}), Row({Value()})}));
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT NULL AS n UNION ALL SELECT 2 UNION ALL SELECT 2 "
                    "ORDER BY n NULLS FIRST"),
      std::vector<Row>({Row({Value()}), Row({Value(2)}), Row({Value(2)})}));
}

TEST_F(RelationalRegressionTest, ParallelRelationCleanupKeepsOwnersAndCharges) {
  const Value retained(std::string(128, 'x'));
  const size_t initial = QueryMemoryBudget::Global().Used();
  const auto fill = [&](relational_detail::Relation& relation) {
    relation.rows.reserve(65536);
    for (int64_t i = 0; i < 65536; ++i) {
      relation.AddRow(Row({Value(i), retained}));
    }
  };
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    {
      relational_detail::Relation relation;
      fill(relation);
      ASSERT_GT(QueryMemoryBudget::Global().Used(), initial);
      relation.ResetContents();
      EXPECT_TRUE(relation.rows.empty());
      EXPECT_EQ(QueryMemoryBudget::Global().Used(), initial);
      fill(relation);
      relation = relational_detail::Relation();
      EXPECT_TRUE(relation.rows.empty());
      EXPECT_EQ(QueryMemoryBudget::Global().Used(), initial);
      fill(relation);
    }
    EXPECT_EQ(QueryMemoryBudget::Global().Used(), initial);
    // A worker may destroy its own intermediate; nested cleanup must not
    // wait for the helper team that is already running this outer batch.
    ASSERT_SUCCESS(RunParallelTasks(2, [&](size_t) {
      relational_detail::Relation relation;
      fill(relation);
    }));
    EXPECT_EQ(QueryMemoryBudget::Global().Used(), initial);
    EXPECT_EQ(retained, Value(std::string(128, 'x')));
  }
}

TEST_F(RelationalRegressionTest, ParallelUnionTransferPromotesAcrossShards) {
  RelationalRun(*rs_, "CREATE TABLE UnionParallel(x INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("UnionParallel"));
    for (int64_t i = 0; i < 65536; ++i) {
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({i % 31 == 0 ? Value() : Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  for (const std::string tail :
       {" UNION ALL SELECT 0.5", " UNION ALL SELECT NULL",
        " UNION DISTINCT SELECT 0.5"}) {
    std::vector<Row> reference;
    for (const size_t workers : {size_t{1}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto rows = RelationalRun(*rs_, "SELECT x FROM UnionParallel" + tail +
                                          " ORDER BY x NULLS FIRST");
      if (workers == 1) {
        reference = std::move(rows);
      } else {
        EXPECT_EQ(rows, reference);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, BoundProjectionPreservesDynamicTypesAndScope) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT k, COALESCE(v, 0), CASE WHEN k=1 THEN v ELSE k+2 END
    FROM (SELECT 1 AS k, 1.5 AS v UNION ALL SELECT 2,NULL
          UNION ALL SELECT 3,4.5) source ORDER BY k
  )sql"),
            std::vector<Row>({Row({Value(1), Value(1.5), Value(1.5)}),
                              Row({Value(2), Value(0), Value(4)}),
                              Row({Value(3), Value(4.5), Value(5)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT k,a.k AS ak,b.k AS bk FROM (SELECT 1 AS k UNION ALL SELECT 2) a
    FULL JOIN (SELECT 2 AS k UNION ALL SELECT 3) b USING(k)
    ORDER BY k
  )sql"),
            std::vector<Row>({Row({Value(1), Value(1), Value()}),
                              Row({Value(2), Value(2), Value(2)}),
                              Row({Value(3), Value(), Value(3)})}));
}

TEST_F(RelationalRegressionTest,
       DistinctSetOperationsGroupEveryInputAndKeepAllMultiplicity) {
  for (const auto kind :
       {SetOperationKind::kIntersect, SetOperationKind::kExcept,
        SetOperationKind::kUnion, SetOperationKind::kIntersectAll,
        SetOperationKind::kExceptAll, SetOperationKind::kUnionAll}) {
    auto context = rs_->BeginContext();
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    const auto ak = ColumnValueExp(ColumnName("a", "k"));
    const auto dk = ColumnValueExp(ColumnName("d", "k"));
    SelectStatement leaf({NamedExpression("ak", ak), NamedExpression("dk", dk)},
                         {},
                         BinaryExpressionExp(ak, BinaryOperation::kEquals, dk));
    SelectSource fact;
    fact.table = "AggTable";
    fact.alias = "a";
    SelectSource dim;
    dim.table = "SmallKeys";
    dim.alias = "d";
    leaf.SetSources({fact, dim});
    SelectStatement statement(leaf);
    auto tree = std::make_shared<SetOperationTree>();
    tree->first = std::make_shared<SelectStatement>(leaf);
    for (size_t i = 0; i < 2; ++i) {
      auto branch = std::make_shared<SelectStatement>(leaf);
      statement.AddSetOperation(kind, branch);
      tree->branches.push_back(branch);
      tree->kinds.push_back(kind);
    }
    statement.SetSetOperationTree(tree);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    const bool distinct = kind == SetOperationKind::kIntersect ||
                          kind == SetOperationKind::kExcept ||
                          kind == SetOperationKind::kUnion;
    const size_t expected = (kind == SetOperationKind::kExcept ||
                             kind == SetOperationKind::kExceptAll)
                                ? 0
                            : kind == SetOperationKind::kUnionAll     ? 240
                            : kind == SetOperationKind::kIntersectAll ? 80
                                                                      : 4;
    EXPECT_EQ(result.TotalRows(), expected);
    EXPECT_EQ(runtime.grouped_join_fusions, distinct ? 3U : 0U);
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  }
}

TEST_F(RelationalRegressionTest, DistinctIntersectionFusesJoinGroups) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT a.k FROM AggTable a,KeyTable d WHERE a.k=d.k
    INTERSECT DISTINCT
    SELECT a.k FROM AggTable a,SmallKeys d WHERE a.k=d.k
    INTERSECT DISTINCT
    SELECT a.k FROM AggTable a,SmallKeys d WHERE a.k=d.k AND d.k<3
    ORDER BY k
  )sql"),
            std::vector<Row>({Row({Value(1)}), Row({Value(2)})}));
  // ALL still carries multiplicity; a branch LIMIT is applied before set
  // membership, and LEFT null extension must survive grouping-only fusion.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM (
      SELECT a.k FROM AggTable a,SmallKeys d WHERE a.k=d.k
      INTERSECT ALL SELECT a.k FROM AggTable a,SmallKeys d WHERE a.k=d.k
    ) t
  )sql"),
            std::vector<Row>({Row({Value(80)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT a.k,d.k AS dk FROM AggTable a LEFT JOIN SmallKeys d ON a.k=d.k
    GROUP BY a.k,d.k ORDER BY a.k LIMIT 2
  )sql"),
      std::vector<Row>({Row({Value(0), Value()}), Row({Value(1), Value(1)})}));
}

TEST_F(RelationalRegressionTest, ResidentSetOperationsPreserveMultiplicities) {
  const std::string left =
      "(SELECT 1.0 AS n UNION ALL SELECT 1.0 UNION ALL SELECT 2.0 "
      "UNION ALL SELECT NULL UNION ALL SELECT NULL)";
  const std::string right =
      "(SELECT 1 AS n UNION ALL SELECT 3 UNION ALL SELECT NULL)";
  for (const auto& [operation, expected] :
       std::vector<std::pair<std::string, std::vector<Row>>>{
           {" UNION DISTINCT ",
            {Row({Value()}), Row({Value(1.0)}), Row({Value(2.0)}),
             Row({Value(3.0)})}},
           {" INTERSECT DISTINCT ", {Row({Value()}), Row({Value(1.0)})}},
           {" INTERSECT ALL ", {Row({Value()}), Row({Value(1.0)})}},
           {" EXCEPT DISTINCT ", {Row({Value(2.0)})}},
           {" EXCEPT ALL ",
            {Row({Value()}), Row({Value(1.0)}), Row({Value(2.0)})}}}) {
    std::string query = left;
    query.append(operation).append(right).append(" ORDER BY n NULLS FIRST");
    EXPECT_EQ(RelationalRun(*rs_, query), expected) << operation;
  }
}

TEST_F(RelationalRegressionTest, WindowProjectionTransfersVisibleColumns) {
  std::vector<Row> expected;
  expected.reserve(60);
  for (int64_t key = 0; key < 3; ++key) {
    for (int64_t duplicate = 0; duplicate < 20; ++duplicate) {
      expected.emplace_back(Row({Value(key), Value(1), Value((key * 20) + 1)}));
    }
  }
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT * FROM (
      SELECT *, RANK() OVER (ORDER BY k) AS rk FROM AggTable
      QUALIFY rk <= 41
    ) q ORDER BY k
  )sql"),
            expected);
  for (Row& row : expected) {
    row = Row({row[2], row[0], row[0]});
  }
  // Repeated source columns must survive the fallback, and a hidden result
  // selected before its source must not be consumed by in-place reordering.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT * FROM (
      SELECT RANK() OVER (ORDER BY k) AS rk, k AS a, k AS b FROM AggTable
      QUALIFY rk <= 41
    ) q ORDER BY a
  )sql"),
            expected);
  for (Row& row : expected) {
    row.values_.resize(2);
  }
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT * FROM (
      SELECT RANK() OVER (ORDER BY k) AS rk, k FROM AggTable
      QUALIFY rk <= 41
    ) q ORDER BY k
  )sql"),
            expected);
}

TEST_F(RelationalRegressionTest, OuterMatchUnionKeepsNullsAndMultiplicity) {
  RelationalRun(*rs_,
                "CREATE TABLE MatchProbe(id INT64,a INT64,b INT64,c INT64)");
  RelationalRun(*rs_, "CREATE TABLE MatchA(a INT64,b INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE MatchB(a INT64,b INT64,v INT64)");
  RelationalRun(
      *rs_, "INSERT INTO MatchA VALUES (1,1,1),(1,1,2),(2,2,-1),(NULL,1,4)");
  RelationalRun(*rs_,
                "INSERT INTO MatchB VALUES (1,1,3),(3,3,5),(3,3,5),(NULL,1,4)");
  std::vector<Row> probes;
  probes.reserve(8192);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("MatchProbe"));
    for (int64_t i = 0; i < 8192; ++i) {
      probes.emplace_back(Row({Value(i), i % 23 == 0 ? Value() : Value(i % 97),
                               Value(i % 5), Value(i % 7)}));
      ASSERT_TRUE(table->Insert(setup.txn_, probes.back()).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::vector<Row> a_rows{Row({Value(1), Value(1), Value(1)}),
                                Row({Value(1), Value(1), Value(2)}),
                                Row({Value(2), Value(2), Value(-1)})};
  const std::vector<Row> b_rows{Row({Value(1), Value(1), Value(3)}),
                                Row({Value(3), Value(3), Value(5)}),
                                Row({Value(3), Value(3), Value(5)})};
  const Row padded({Value(), Value(), Value()});
  for (const bool different_key : {false, true}) {
    for (const bool accepts_null : {false, true}) {
      std::array<int64_t, 4> expected{};
      for (const Row& probe : probes) {
        std::vector<const Row*> a_matches, b_matches;
        if (!probe[1].IsNull()) {
          for (const auto& row : a_rows) {
            if (probe[1] == row[0] && probe[2] == row[1]) {
              a_matches.push_back(&row);
            }
          }
          for (const auto& row : b_rows) {
            if (probe[1] == row[0] && probe[different_key ? 3 : 2] == row[1]) {
              b_matches.push_back(&row);
            }
          }
        }
        if (a_matches.empty()) {
          a_matches.push_back(&padded);
        }
        if (b_matches.empty()) {
          b_matches.push_back(&padded);
        }
        for (const Row* a : a_matches) {
          for (const Row* b : b_matches) {
            const bool a_pass =
                accepts_null ? (*a)[2].IsNull()
                             : !(*a)[2].IsNull() && (*a)[2].value.int_value > 0;
            const bool b_pass =
                !(*b)[2].IsNull() && (*b)[2].value.int_value > 0;
            if (a_pass || b_pass) {
              ++expected[0];
              expected[1] += probe[0].value.int_value;
              expected[2] += static_cast<int64_t>(!(*a)[2].IsNull());
              expected[3] += static_cast<int64_t>(!(*b)[2].IsNull());
            }
          }
        }
      }
      const std::string sql =
          "SELECT COUNT(*),SUM(f.id),COUNT(a.v),COUNT(b.v) FROM MatchProbe f "
          "LEFT JOIN MatchA a ON f.a=a.a AND f.b=a.b "
          "LEFT JOIN MatchB b ON b.b=f." +
          std::string(different_key ? "c" : "b") + " AND f.a=b.a WHERE (" +
          std::string(accepts_null ? "a.v IS NULL" : "COALESCE(a.v,0)>0") +
          " OR COALESCE(b.v,0)>0)";
      for (const size_t workers : {size_t{1}, size_t{2}}) {
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        EXPECT_EQ(
            RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(expected[0]), Value(expected[1]),
                                   Value(expected[2]), Value(expected[3])})}));
      }
    }
  }
}

TEST_F(RelationalRegressionTest, PreservedDimensionFiltersOrderedFactScan) {
  RelationalRun(*rs_, "ANALYZE SmallKeys");
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*),SUM(a.v) FROM AggTable a
    LEFT JOIN KeyTable b ON a.k=b.k AND b.k<>2, SmallKeys d
    WHERE a.k=d.k AND d.k<=2 AND b.k IS NULL
  )sql"),
            std::vector<Row>({Row({Value(20), Value(20)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM AggTable a LEFT JOIN SmallKeys d ON a.k=d.k
    WHERE d.k IS NULL
  )sql"),
            std::vector<Row>({Row({Value(120)})}));
}

TEST_F(RelationalRegressionTest, OrderedPostJoinFilterWaitsForNullExtension) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT d.k,COUNT(*) AS n FROM AggTable a
    LEFT JOIN SmallKeys b ON a.k=b.k JOIN KeyTable d ON a.k=d.k
    WHERE b.k IS NULL AND d.k<6 GROUP BY d.k ORDER BY d.k
  )sql"),
            std::vector<Row>(
                {Row({Value(0), Value(20)}), Row({Value(5), Value(20)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM SmallKeys a LEFT JOIN AggTable b ON a.k=b.k
    RIGHT JOIN KeyTable c ON a.k=c.k WHERE b.k IS NULL
  )sql"),
            std::vector<Row>({Row({Value(6)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM SmallKeys a LEFT JOIN AggTable b ON a.k=b.k
    FULL JOIN KeyTable c ON a.k=c.k WHERE b.k IS NULL AND a.k IS NULL
  )sql"),
            std::vector<Row>({Row({Value(6)})}));
}

TEST_F(RelationalRegressionTest,
       EmptyInnerInputPreservesAggregateAndOuterJoin) {
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*), SUM(a.v) FROM AggTable a, SmallKeys d "
                    "WHERE a.k=d.k AND d.k=99"),
      std::vector<Row>({Row({Value(0), Value()})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT COUNT(*), COUNT(a.k) FROM SmallKeys d LEFT JOIN AggTable a "
          "ON a.k=d.k AND a.k=99"),
      std::vector<Row>({Row({Value(4), Value(0)})}));
}

TEST_F(RelationalRegressionTest,
       MaterializedCteScalarMapKeepsMissingAndNullGroups) {
  const std::string cte =
      "WITH m AS (SELECT 1 AS k,1.0 AS v UNION ALL SELECT 1,3.0 "
      "UNION ALL SELECT 2,1.0 UNION ALL SELECT 3,NULL) ";
  EXPECT_EQ(
      RelationalRun(
          *rs_, cte + "SELECT d.k FROM SmallKeys d WHERE d.k > "
                      "(SELECT AVG(v)*1.2 FROM m WHERE m.k=d.k) ORDER BY d.k"),
      std::vector<Row>({Row({Value(2)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          cte + "SELECT d.k FROM SmallKeys d WHERE "
                "(SELECT AVG(v)*1.2 FROM m WHERE m.k=d.k) < d.k ORDER BY d.k"),
      std::vector<Row>({Row({Value(2)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_, cte + "SELECT d.k FROM SmallKeys d WHERE 0 = "
                      "(SELECT COUNT(*) FROM m WHERE m.k=d.k) ORDER BY d.k"),
      std::vector<Row>({Row({Value(4)})}));
}

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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
      context.Abort();
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
    context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
  context.Abort();
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
