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
TEST_F(RelationalRegressionTest, DirectWindowOrderingKeepsPeersAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE WindowOrderKeys (p INT64,k INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO WindowOrderKeys VALUES "
                "(1,2,20),(1,NULL,30),(1,1,10),(1,2,21),"
                "(2,2,40),(2,1,50)");
  const auto rows = RelationalRun(*rs_, R"sql(
    SELECT p,k,v,
      SUM(v) OVER (PARTITION BY p ORDER BY k NULLS LAST) AS total,
      ROW_NUMBER() OVER (PARTITION BY p ORDER BY k DESC NULLS FIRST,v) AS rn
    FROM WindowOrderKeys ORDER BY p,v
  )sql");
  ASSERT_EQ(rows.size(), 6U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1), Value(10), Value(10), Value(4)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(2), Value(20), Value(51), Value(2)}));
  EXPECT_EQ(rows[2], Row({Value(1), Value(2), Value(21), Value(51), Value(3)}));
  EXPECT_EQ(rows[3], Row({Value(1), Value(), Value(30), Value(81), Value(1)}));
  EXPECT_EQ(rows[4], Row({Value(2), Value(2), Value(40), Value(90), Value(1)}));
  EXPECT_EQ(rows[5], Row({Value(2), Value(1), Value(50), Value(50), Value(2)}));
}

TEST_F(RelationalRegressionTest, MonotonicDictionarySumKeepsInfinity) {
  RelationalRun(*rs_, "CREATE TABLE MonoInf(k INT64, v FLOAT64)");
  const double infinity = std::numeric_limits<double>::infinity();
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MonoInf"));
    for (int64_t i = 0; i < 65536; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_,
                       Row({Value(i / 32768), Value(i == 0 ? infinity : 1.0)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MonoInf"));
    const auto store = table->RebuildLSTableStore(context.txn_);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->total_rows, 65536U);
    ASSERT_NE(store->column_is_monotonic[0], 0U);
    ASSERT_TRUE(store->dictionaries[1].active);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto result =
      RelationalRun(*rs_, "SELECT k, SUM(v) FROM MonoInf GROUP BY k", true);
  ASSERT_EQ(result.size(), 2U);
  for (const auto& row : result) {
    EXPECT_EQ(row[1], Value(row[0] == Value(0) ? infinity : 32768.0));
  }
  // The monotonic path reads dictionary codes per row. Check its row counter
  // so the same result from the generic fallback does not pass this check.
  const std::string sql = "SELECT k, SUM(v) FROM MonoInf GROUP BY k";
  const std::string profile =
      ExplainAnalyze(*rs_, sql, /*force_relational=*/true);
  constexpr std::string_view kCounter = "dictionary_aggregate_rows=";
  const auto pos = profile.find(kCounter);
  ASSERT_NE(pos, std::string::npos) << profile;
  EXPECT_GT(std::stoul(profile.substr(pos + kCounter.size())), 0U) << profile;
}

TEST_F(RelationalRegressionTest, DirectDoubleWindowOrderKeepsExceptionalPeers) {
  RelationalRun(*rs_, "CREATE TABLE DoubleWindowKeys(k FLOAT64)");
  RelationalRun(*rs_,
                "INSERT INTO DoubleWindowKeys VALUES (2.5),(NULL),(-0.0),(0.0),"
                "(2.5),(CAST('NaN' AS FLOAT64)),(CAST('NaN' AS FLOAT64)),"
                "(CAST('inf' AS FLOAT64)),(CAST('-inf' AS FLOAT64)),(-4.0)");
  for (const std::string_view order : {"ASC NULLS FIRST", "ASC NULLS LAST",
                                       "DESC NULLS FIRST", "DESC NULLS LAST"}) {
    const auto query = [&](std::string_view key) {
      const std::string spec = std::string(key) + " " + std::string(order);
      std::string sql = "SELECT RANK() OVER (ORDER BY ";
      sql += spec;
      sql += "), DENSE_RANK() OVER (ORDER BY ";
      sql += spec;
      sql += "), ROW_NUMBER() OVER (ORDER BY ";
      sql += spec;
      sql += ") FROM DoubleWindowKeys";
      return sql;
    };
    SCOPED_TRACE(order);
    EXPECT_EQ(RelationalRun(*rs_, query("k")),
              RelationalRun(*rs_, query("k+0.0")));
  }
}

TEST_F(RelationalRegressionTest, BorrowedWindowKeysKeepFramesAndSharedLayouts) {
  RelationalRun(*rs_,
                "CREATE TABLE WindowBorrow (id INT64,p INT64,k INT64,"
                "v FLOAT64,d DATE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("WindowBorrow"));
    for (int i = 0; i < 1600; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i), Value(i / 8),
                            i % 5 == 0 ? Value() : Value(i % 4),
                            i % 3 == 0 ? Value() : Value(i * 0.5),
                            i % 7 == 0 ? Value() : Value::DateFromDays(i % 4)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto query = [](bool computed) {
    const std::string key = computed ? "(k+0)" : "k";
    const std::string date = computed ? "(d+0)" : "d";
    const std::string ascending =
        "PARTITION BY p ORDER BY " + key + " ASC NULLS FIRST";
    const std::string descending =
        "PARTITION BY p ORDER BY " + key + " DESC NULLS LAST";
    return "SELECT id,k,v,d,RANK() OVER (" + ascending + "),SUM(v) OVER (" +
           ascending +
           " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),COUNT(v) OVER (" +
           ascending +
           " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW),"
           "LAG(v) OVER (" +
           ascending + "),LAST_VALUE(v) OVER (" + descending +
           " ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),DENSE_RANK() OVER ("
           "PARTITION BY p ORDER BY " +
           date + "),SUM(v) OVER (PARTITION BY p ORDER BY " + date +
           " RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) "
           "FROM WindowBorrow ORDER BY id";
  };
  const auto direct = RelationalRun(*rs_, query(false));
  const auto computed = RelationalRun(*rs_, query(true));
  ASSERT_EQ(direct.size(), 1600U);
  EXPECT_EQ(direct, computed);
  for (size_t i = 0; i < direct.size(); ++i) {
    EXPECT_EQ(direct[i][0], Value(i));
    EXPECT_EQ(direct[i][1], i % 5 == 0 ? Value() : Value(i % 4));
    EXPECT_EQ(direct[i][2],
              i % 3 == 0 ? Value() : Value(static_cast<double>(i) * 0.5));
    EXPECT_EQ(direct[i][3], i % 7 == 0 ? Value() : Value::DateFromDays(i % 4));
  }
}

TEST_F(RelationalRegressionTest,
       NumericWindowKeysKeepExceptionalPeersAndOrder) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  const std::vector<std::vector<Value>> domains{
      {Value(), Value(std::numeric_limits<int64_t>::min()), Value(-1), Value(0),
       Value(std::numeric_limits<int64_t>::max()), Value(0)},
      {Value(), Value(-std::numeric_limits<double>::infinity()), Value(-1.0),
       Value(-0.0), Value(0.0), Value(1.0),
       Value(std::numeric_limits<double>::infinity()),
       Value(std::numeric_limits<double>::quiet_NaN()),
       Value(-std::numeric_limits<double>::quiet_NaN())}};
  for (const auto& domain : domains) {
    Schema schema(
        "", {Column("id", ValueType::kInt64), Column("p", ValueType::kInt64),
             Column("k", domain[1].type)});
    std::vector<Row> rows;
    rows.reserve(32768);
    for (size_t i = 0; i < 32768; ++i) {
      rows.emplace_back(std::vector<Value>{Value(i), Value(i % 3),
                                           domain[i % domain.size()]});
    }
    for (const bool ascending : {false, true}) {
      for (const bool nulls_first : {false, true}) {
        for (const std::string function :
             {"ROW_NUMBER", "RANK", "DENSE_RANK", "LAG"}) {
          WindowFunctionCallExpression window;
          window.function = function;
          if (function == "LAG") {
            window.args = {ColumnValueExp("id")};
          }
          window.partition_by = {ColumnValueExp("p")};
          window.order_by = {{ColumnValueExp("k"), ascending, nulls_first}};
          std::vector<Value> encoded, reference;
          ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
              context, window, rows, schema, nullptr, {}, &encoded));
          // The contextual expression route retains the general comparator.
          window.order_by[0].expression = FunctionCallExp(
              "coalesce", {ColumnValueExp("k"), ColumnValueExp("k")});
          ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
              context, window, rows, schema, nullptr, {}, &reference));
          EXPECT_EQ(encoded, reference)
              << function << ' ' << ascending << ' ' << nulls_first;
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, RankBoundKeepsWholeBoundaryPeers) {
  auto context = rs_->BeginContext();
  const Schema schema(
      "", {Column("id", ValueType::kInt64), Column("p", ValueType::kInt64),
           Column("k", ValueType::kDouble)});
  std::vector<Row> source;
  source.reserve(32768);
  for (size_t i = 0; i < 32768; ++i) {
    Value value(static_cast<double>(i % 1024));
    if (i % 257 == 0) {
      value = Value();
    } else if (i % 509 == 0) {
      value = Value(std::numeric_limits<double>::quiet_NaN());
    } else if (i % 251 == 0) {
      value = Value(std::numeric_limits<double>::infinity());
    } else if (i % 127 == 0) {
      value = Value(-0.0);
    }
    source.emplace_back(std::vector<Value>{Value(i), Value(i % 3), value});
  }
  for (const bool ascending : {false, true}) {
    for (const bool nulls_first : {false, true}) {
      auto window = std::make_shared<WindowFunctionCallExpression>();
      window->function = "RANK";
      window->partition_by = {ColumnValueExp("p")};
      window->order_by = {{ColumnValueExp("k"), ascending, nulls_first}};
      WindowFunctionCallExpression generic;
      generic.function = "RANK";
      generic.partition_by = window->partition_by;
      generic.order_by = window->order_by;
      generic.order_by[0].expression = FunctionCallExp(
          "coalesce", {ColumnValueExp("k"), ColumnValueExp("k")});
      std::vector<Value> reference;
      ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
          context, generic, source, schema, nullptr, {}, &reference));
      for (const int64_t limit : {int64_t{1}, int64_t{100}, int64_t{32768}}) {
        for (const bool alias : {false, true}) {
          SelectStatement statement(
              {NamedExpression("id", ColumnValueExp("id")),
               NamedExpression("r", window)},
              {}, nullptr);
          statement.SetQualify(BinaryExpressionExp(
              alias ? ColumnValueExp("r") : window,
              alias ? BinaryOperation::kLessThan
                    : BinaryOperation::kLessThanEquals,
              ConstantValueExp(Value(limit + (alias ? 1 : 0)))));
          relational_detail::Relation input;
          input.schema = schema;
          input.rows = source;
          ASSIGN_OR_ASSERT_FAIL(
              relational_detail::WindowedInput, actual,
              relational_detail::ApplyWindows(context, statement,
                                              std::move(input), nullptr, {}));
          size_t kept = 0;
          for (size_t r = 0; r < source.size(); ++r) {
            if (reference[r].value.int_value > limit) {
              continue;
            }
            ASSERT_LT(kept, actual.input.rows.size());
            EXPECT_EQ(actual.input.rows[kept][0], Value(r));
            EXPECT_EQ(actual.input.rows[kept][3], reference[r]);
            ++kept;
          }
          EXPECT_EQ(actual.input.rows.size(), kept);
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, DerivedWindowFiltersEnterQualifyAfterRanking) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "ROW_NUMBER";
  window->partition_by = {ColumnValueExp("k")};
  window->order_by = {{ColumnValueExp("v"), true, false}};
  auto derived = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k")),
                                   NamedExpression("r", window)},
      std::vector<std::string>{"AggTable"}, nullptr);
  SelectStatement outer(
      {NamedExpression("k", ColumnValueExp("k")),
       NamedExpression("r", ColumnValueExp("r"))},
      {},
      BinaryExpressionExp(ColumnValueExp("r"), BinaryOperation::kLessThanEquals,
                          ConstantValueExp(Value(2))));
  SelectSource source;
  source.query = derived;
  source.alias = "w";
  outer.SetSources({source});
  const auto rewritten =
      relational_detail::OptimizeDerivedBoundaries(outer, {});
  ASSERT_NE(rewritten, nullptr);
  EXPECT_EQ(rewritten->WhereClause(), nullptr);
  EXPECT_NE(rewritten->Sources()[0].query->Qualify(), nullptr);
  derived->SetOrderBy({{ColumnValueExp("k"), true, false}});
  EXPECT_EQ(relational_detail::OptimizeDerivedBoundaries(outer, {}), nullptr);
  derived->SetOrderBy({});
  derived->SetLimit(1);
  EXPECT_EQ(relational_detail::OptimizeDerivedBoundaries(outer, {}), nullptr);

  const std::string inner =
      "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY v) r FROM AggTable";
  const auto nested = RelationalRun(
      *rs_, "SELECT * FROM (" + inner + ") w WHERE r<=2 ORDER BY k,v,r");
  const auto qualified =
      RelationalRun(*rs_, inner + " QUALIFY r<=2 ORDER BY k,v,r");
  EXPECT_EQ(nested, qualified);
  EXPECT_EQ(nested.size(), 20U);
  EXPECT_TRUE(RelationalRun(*rs_, "SELECT * FROM (" + inner +
                                      " ORDER BY r LIMIT 1) w WHERE r>1")
                  .empty());
  const std::string ties =
      "SELECT k,RANK() OVER(ORDER BY k) r FROM "
      "(SELECT 1 k UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t";
  EXPECT_EQ(RelationalRun(*rs_, "SELECT * FROM (" + ties + ") w WHERE r<=2"),
            RelationalRun(*rs_, ties + " QUALIFY r<=2"));
}

TEST_F(RelationalRegressionTest,
       ColumnByteLengthAggregatesKeepNullsAndSnapshot) {
  const Schema schema("input", {Column("s", ValueType::kVarChar)});
  const auto average = std::make_shared<AggregateExpression>(
      AggregationType::kAvg,
      CastExpressionExp(FunctionCallExp("byte_length", {ColumnValueExp("s")}),
                        "FLOAT64"));
  const SelectStatement statement({NamedExpression("n", average)}, {}, nullptr);
  const auto plan = relational_detail::PlanTypedAggregation(
      statement, nullptr, schema, {average.get()}, {});
  ASSERT_TRUE(plan.eligible);
  ASSERT_TRUE(plan.slim);
  const auto& program = plan.aggregates[0].child;
  ASSERT_TRUE(program);
  if (!program) {
    return;
  }
  ASSERT_EQ(program->Instructions().back().opcode, BytecodeOp::kByteLength);
  for (const bool nullable : {false, true}) {
    const std::string name =
        nullable ? "ByteLengthNullable" : "ByteLengthPlain";
    RelationalRun(*rs_, "CREATE TABLE " + name + "(k INT64,s STRING)");
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            setup.GetTable(name));
      for (int64_t i = 0; i < 40000; ++i) {
        Value text =
            nullable && i % 13 == 0
                ? Value()
                : Value(i % 7 == 0 ? std::string()
                                   : (i % 3 == 0 ? std::string("é東京")
                                                 : std::string("payload-") +
                                                       std::to_string(i)));
        ASSERT_SUCCESS(
            table->Insert(setup.txn_, Row({Value(i % 8), std::move(text)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    RelationalRun(*rs_, "ANALYZE " + name);
    const auto sql = [&](bool reference, bool scalar,
                         const std::string& predicate) {
      const std::string input = reference ? "s || ''" : "s";
      std::string result = scalar ? "SELECT " : "SELECT k,";
      result += "SUM(BYTE_LENGTH(" + input;
      result += ")),AVG(CAST(BYTE_LENGTH(" + input;
      result += ") AS FLOAT64)),COUNT(BYTE_LENGTH(" + input;
      result += ")),COUNT(*) FROM " + name;
      result += " WHERE " + predicate;
      result += scalar ? "" : " GROUP BY k ORDER BY k";
      return result;
    };
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      for (const bool scalar : {false, true}) {
        for (const std::string predicate : {"TRUE", "s<>''", "s=''", "k<0"}) {
          EXPECT_EQ(RelationalRun(*rs_, sql(false, scalar, predicate)),
                    RelationalRun(*rs_, sql(true, scalar, predicate)));
        }
      }
    }
    auto old = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    const auto read_old = [&] {
      std::vector<Row> rows;
      auto result = engine.Prepare(old, sql(false, false, "TRUE"));
      EXPECT_TRUE(result.HasValue()) << engine.LastError();
      if (result.HasValue()) {
        Row row;
        while (result.Value()->Next(&row, nullptr)) {
          rows.push_back(row);
        }
        EXPECT_TRUE(result.Value()->GetStatus().ok());
      }
      return rows;
    };
    const auto expected = read_old();
    RelationalRun(*rs_, "UPDATE " + name + " SET s='changed' WHERE k=0");
    EXPECT_EQ(read_old(), expected);
    EXPECT_EQ(RelationalRun(*rs_, sql(false, false, "TRUE")),
              RelationalRun(*rs_, sql(true, false, "TRUE")));
    ASSERT_SUCCESS(old.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, WideStringCodesKeepGroupingAndFilters) {
  RelationalRun(*rs_,
                "CREATE TABLE WideCodeInput(k INT64,s STRING,v INT64,d DATE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("WideCodeInput"));
    for (int64_t i = 0; i < 40000; ++i) {
      std::string text(80, i % 2 == 0 ? 'a' : 'b');
      text += i % 2 == 0 ? "é_" : "_";
      text += std::to_string(i % 303);
      Value value = i % 17 == 0 ? Value() : Value(i % 19 == 0 ? "" : text);
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({Value(i), std::move(value), Value(i % 7),
                       i % 13 == 0 ? Value() : Value::DateFromDays(i % 31)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE WideCodeInput");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string predicate :
         {" = ''", " <> ''", " LIKE '%é%'", " NOT LIKE '%é%'",
          " LIKE 'a%3' AND v=2", " LIKE '%é%' OR v=2"}) {
      const std::string actual =
          "SELECT k FROM WideCodeInput WHERE s" + predicate;
      const std::string reference =
          "SELECT k FROM WideCodeInput WHERE (s||'')" + predicate;
      EXPECT_EQ(RelationalRun(*rs_, actual + " ORDER BY k"),
                RelationalRun(*rs_, reference + " ORDER BY k"));
    }
    const std::string base =
        "SELECT s,COUNT(*),SUM(v),AVG(CAST(v AS FLOAT64)) "
        "FROM WideCodeInput GROUP BY s";
    EXPECT_EQ(RelationalRun(*rs_, base + " ORDER BY s"),
              RelationalRun(*rs_, base + ",BYTE_LENGTH(s) ORDER BY s"));
    for (const std::string predicate :
         {"", " WHERE s<>''", " WHERE v<>3", " WHERE v>=0", " WHERE v<0",
          " WHERE d>='1970-1-16'", " WHERE v=2 AND s<>''"}) {
      for (const std::string order :
           {" ORDER BY c DESC,s NULLS FIRST LIMIT 17 OFFSET 3",
            " ORDER BY c,s DESC NULLS LAST LIMIT 700",
            " ORDER BY s DESC NULLS FIRST LIMIT 11 OFFSET 600",
            " ORDER BY s NULLS LAST LIMIT 0"}) {
        std::string actual =
            "SELECT s,COUNT(*) c,COUNT(*) again,7 tag "
            "FROM WideCodeInput";
        actual += predicate;
        actual += " GROUP BY s";
        actual += order;
        std::string reference =
            "SELECT s||'' AS s,COUNT(*) c,COUNT(*) again,7 tag "
            "FROM WideCodeInput";
        reference += predicate;
        reference += " GROUP BY s||''";
        reference += order;
        EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, reference))
            << actual;
      }
    }
    const std::string filtered =
        "SELECT s,COUNT(*) c FROM WideCodeInput WHERE s<>'' GROUP BY s";
    EXPECT_EQ(
        RelationalRun(*rs_, filtered + " ORDER BY c DESC,s LIMIT 17"),
        RelationalRun(*rs_,
                      filtered + ",BYTE_LENGTH(s) ORDER BY c DESC,s LIMIT 17"));
    for (const std::string op : {"=", "<>", "<", "<=", ">", ">="}) {
      for (const bool reversed : {false, true}) {
        const auto query = [&](bool reference) {
          std::string literal = reference ? "DATE '1970-01-16'" : "'1970-1-16'";
          std::string condition = reversed ? literal : "d";
          condition += op;
          condition += reversed ? "d" : literal;
          std::string result = "SELECT s,COUNT(*) c FROM WideCodeInput WHERE ";
          result += condition;
          result += " GROUP BY s ORDER BY c DESC,s LIMIT 17";
          return result;
        };
        EXPECT_EQ(RelationalRun(*rs_, query(false)),
                  RelationalRun(*rs_, query(true)));
      }
    }
  }
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("WideCodeInput"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 2);
  EXPECT_TRUE(store->dictionaries[1].bytewise_equality);
  ASSERT_SUCCESS(context.PreCommit());
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT COUNT(DISTINCT s) FROM WideCodeInput"),
      RelationalRun(*rs_, "SELECT COUNT(DISTINCT (s||'')) FROM WideCodeInput"));
  // Distinct byte strings can be one SQL group. Rebuilding the dictionary
  // must revoke the proof and keep canonical grouping for these peers.
  RelationalRun(*rs_,
                "INSERT INTO WideCodeInput VALUES "
                "(40000,'0-12 0 0:0:0',1,DATE '1970-01-01'),"
                "(40001,'1-0 0 0:0:0',1,DATE '1970-01-01')");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT s,COUNT(*) c FROM WideCodeInput "
                          "GROUP BY s ORDER BY s LIMIT 700"),
            RelationalRun(*rs_,
                          "SELECT s||'' AS s,COUNT(*) c FROM WideCodeInput "
                          "GROUP BY s||'' ORDER BY s LIMIT 700"));
}

// Derived aggregate producers have no LIMIT: every group must survive into
// the next stage even when the outer query has a small LIMIT or HAVING.
TEST_F(RelationalRegressionTest,
       UnboundedDictionaryAggregatesFeedNestedStages) {
  RelationalRun(*rs_, "CREATE TABLE InnerCodes(k INT64,s STRING,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("InnerCodes"));
    for (int64_t i = 0; i < 24000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({i % 17 == 0 ? Value() : Value((i % 307) - 153),
                            i % 19 == 0 ? Value()
                                        : Value(std::string(80, 'x') +
                                                std::to_string(i % 311)),
                            Value((i % 13) - 6)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE InnerCodes");
  const std::string inner =
      "SELECT k,COUNT(*) c,COUNT(k) n,SUM(k) s,AVG(k) a,MIN(k) lo,MAX(k) hi "
      "FROM InnerCodes GROUP BY k";
  const std::string reference = inner + ",CAST(k AS STRING)";
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const std::string suffix :
         {"", " ORDER BY s NULLS LAST",
          " ORDER BY a DESC NULLS FIRST LIMIT 7 OFFSET 3"}) {
      EXPECT_EQ(RelationalRun(*rs_, inner + suffix),
                RelationalRun(*rs_, reference + suffix));
    }
    const std::string nested =
        "SELECT c,COUNT(*) group_count,SUM(n) nonnull,SUM(s) "
        "total,MIN(lo),MAX(hi) "
        "FROM (" +
        inner + ") GROUP BY c HAVING SUM(n)>0 ORDER BY c LIMIT 5";
    const std::string nested_reference =
        "SELECT c,COUNT(*) group_count,SUM(n) nonnull,SUM(s) "
        "total,MIN(lo),MAX(hi) "
        "FROM (" +
        reference + ") GROUP BY c HAVING SUM(n)>0 ORDER BY c LIMIT 5";
    EXPECT_EQ(RelationalRun(*rs_, nested),
              RelationalRun(*rs_, nested_reference));
    const auto explain = ExplainAnalyze(*rs_, nested);
    EXPECT_NE(explain.find("dictionary_aggregations=1"), std::string::npos)
        << explain;
    EXPECT_NE(explain.find("dictionary_summary_aggregations=1"),
              std::string::npos)
        << explain;
    EXPECT_NE(explain.find("dictionary_aggregate_rows=0"), std::string::npos)
        << explain;
    // Independent dictionaries must be packed jointly; multiplying their
    // marginal frequencies would invent combinations not present in the data.
    for (const std::string where : {"", " WHERE v<0", " WHERE v>100"}) {
      const std::string actual =
          "SELECT c,COUNT(*) FROM (SELECT k,s,COUNT(*) c FROM InnerCodes" +
          where + " GROUP BY k,s) GROUP BY c ORDER BY c";
      const std::string expected =
          "SELECT c,COUNT(*) FROM (SELECT MIN(k),s||'',COUNT(*) c FROM "
          "InnerCodes" +
          where + " GROUP BY CAST(k AS STRING),s||'') GROUP BY c ORDER BY c";
      EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, expected));
      EXPECT_NE(ExplainAnalyze(*rs_, actual).find("dictionary_aggregations=1"),
                std::string::npos);
    }
    const std::string scalar =
        "SELECT n,COUNT(*) FROM (SELECT LENGTH(s) len,COUNT(*) n FROM "
        "InnerCodes "
        "GROUP BY LENGTH(s)) GROUP BY n ORDER BY n";
    EXPECT_EQ(
        RelationalRun(*rs_, scalar),
        RelationalRun(
            *rs_,
            "SELECT n,COUNT(*) FROM (SELECT LENGTH(s||'') len,COUNT(*) n "
            "FROM InnerCodes GROUP BY LENGTH(s||'')) GROUP BY n ORDER BY n"));
    EXPECT_NE(
        ExplainAnalyze(*rs_, scalar).find("dictionary_summary_aggregations=1"),
        std::string::npos);
  }
  // A new snapshot may no longer have a safe SUM bound. It must not reuse
  // stale summaries, or let the outer LIMIT hide an overflowing inner group.
  RelationalRun(*rs_,
                "INSERT INTO InnerCodes VALUES "
                "(9223372036854775807,'overflow',0),"
                "(9223372036854775807,'overflow',0)");
  auto context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto result =
      engine.Execute(context, "SELECT SUM(s) FROM (" + inner + ") LIMIT 1");
  if (result.HasValue()) {
    result.Value().Collect();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest,
       ReusableDictionaryStagesKeepIndependentFilters) {
  RelationalRun(*rs_, "CREATE TABLE SharedCodes(k INT64,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("SharedCodes"));
    for (int64_t i = 0; i < 24000; ++i) {
      ASSERT_SUCCESS(table
                         ->Insert(setup.txn_,
                                  Row({Value((i % 401) * 10000), Value(i % 7)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE SharedCodes");
  const std::string actual =
      "SELECT SUM(c) FROM (SELECT k,COUNT(*) c FROM SharedCodes WHERE "
      "k<1000000 "
      "GROUP BY k) UNION ALL SELECT SUM(c) FROM (SELECT k,COUNT(*) c FROM "
      "SharedCodes WHERE k>=1000000 GROUP BY k)";
  const std::string expected =
      "SELECT COUNT(*) FROM SharedCodes WHERE k<1000000 UNION ALL "
      "SELECT COUNT(*) FROM SharedCodes WHERE k>=1000000";
  EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, expected));
  const auto explain = ExplainAnalyze(*rs_, actual);
  EXPECT_NE(explain.find("dictionary_summary_aggregations=2"),
            std::string::npos)
      << explain;
  EXPECT_NE(explain.find("dictionary_aggregate_rows=0"), std::string::npos)
      << explain;
  auto old = rs_->BeginReadOnlyContext();
  const auto before = RelationalRun(*rs_, actual);
  RelationalRun(*rs_, "INSERT INTO SharedCodes VALUES (0,0),(4000000,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result, engine.Execute(old, actual));
  EXPECT_EQ(old_result.Collect(), before);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, expected));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest, DictionaryCountTopNWith32BitCodes) {
  RelationalRun(*rs_, "CREATE TABLE Code32Input(k INT64,s STRING,g INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("Code32Input"));
    for (int64_t i = 0; i < int64_t{65536} * 3; ++i) {
      std::string text(80, 'x');
      text += std::to_string(i % 65536);
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i), Value(std::move(text)), Value(i % 101)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        table->Insert(setup.txn_, Row({Value(-1), Value(), Value(0)}))
            .GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE Code32Input");
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string predicate :
         {"", " WHERE k>=65536", " WHERE g<3", " WHERE g<0",
          " WHERE g IN (0,1,NULL)"}) {
      std::string actual = "SELECT s,COUNT(*) c FROM Code32Input";
      actual += predicate;
      actual += " GROUP BY s ORDER BY s NULLS FIRST LIMIT 17";
      std::string reference = "SELECT s||'' AS s,COUNT(*) c FROM Code32Input";
      reference += predicate;
      reference += " GROUP BY s||'' ORDER BY s NULLS FIRST LIMIT 17";
      EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, reference));
    }
  }
  const std::string selective =
      "SELECT s,COUNT(*) c FROM Code32Input WHERE g=0 GROUP BY s "
      "ORDER BY c DESC LIMIT 13 OFFSET 7";
  const auto expected = RelationalRun(
      *rs_,
      "SELECT s||'' s,COUNT(*) c FROM Code32Input WHERE g=0 GROUP BY s||'' "
      "ORDER BY c DESC LIMIT 13 OFFSET 7");
  EXPECT_EQ(RelationalRun(*rs_, selective), expected);
  EXPECT_NE(ExplainAnalyze(*rs_, selective)
                .find("partitioned_dictionary_aggregations=1"),
            std::string::npos);
  {
    struct RestoreBudget {
      size_t limit = QueryMemoryBudget::Global().Limit();
      RestoreBudget() = default;
      RestoreBudget(const RestoreBudget&) = delete;
      RestoreBudget& operator=(const RestoreBudget&) = delete;
      RestoreBudget(RestoreBudget&&) = delete;
      RestoreBudget& operator=(RestoreBudget&&) = delete;
      ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
    } restore;
    QueryMemoryBudget::Global().ResetForTest(size_t{8} << 20);
    EXPECT_EQ(RelationalRun(*rs_, selective), expected);
  }
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("Code32Input"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 4);
  EXPECT_TRUE(store->dictionaries[1].bytewise_equality);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest,
       ProvenSingletonGroupsPushLimitWithoutHidingErrors) {
  RelationalRun(
      *rs_,
      "CREATE TABLE UniqueGroups(k INT64, tag STRING, v INT64, d DOUBLE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("UniqueGroups"));
    for (int64_t i = 0; i < 8192; ++i) {
      const int64_t k = (i * 4093) % 8192;
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_, Row({Value(k), Value(i % 2 ? "odd" : "even"),
                                        i == 0 ? Value() : Value(i),
                                        Value(static_cast<double>(i) / 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::string sql =
      "SELECT k,tag,COUNT(*) c,SUM(v),AVG(CAST(v AS DOUBLE)),"
      "MIN(d),MAX(d),COUNT(DISTINCT v) FROM UniqueGroups "
      "GROUP BY k,tag ORDER BY c DESC LIMIT 6 OFFSET 3";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 6U);
  for (int64_t i = 3; i < 9; ++i) {
    EXPECT_EQ(rows[static_cast<size_t>(i - 3)],
              Row({Value((i * 4093) % 8192), Value(i % 2 ? "odd" : "even"),
                   Value(1), Value(i), Value(static_cast<double>(i)),
                   Value(static_cast<double>(i) / 2),
                   Value(static_cast<double>(i) / 2), Value(1)}));
  }
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("scan_rows=9,"), std::string::npos);
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC LIMIT 2 WITH TIES")
                .size(),
            8192U);
  const auto null_first =
      RelationalRun(*rs_,
                    "SELECT k,COUNT(*) c,SUM(v),AVG(v),COUNT(v) "
                    "FROM UniqueGroups GROUP BY k ORDER BY c DESC LIMIT 2");
  ASSERT_EQ(null_first.size(), 2U);
  EXPECT_EQ(null_first[0],
            Row({Value(0), Value(1), Value(), Value(), Value(0)}));
  EXPECT_EQ(null_first[1],
            Row({Value(4093), Value(1), Value(1), Value(1.0), Value(1)}));
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT k,COUNT(*) c FROM UniqueGroups WHERE tag='odd' "
                    "GROUP BY k ORDER BY c DESC LIMIT 2"),
      (std::vector<Row>{Row({Value(4093), Value(1)}),
                        Row({Value(4087), Value(1)})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC,k DESC LIMIT 1"),
            (std::vector<Row>{Row({Value(8191), Value(1)})}));
  EXPECT_TRUE(RelationalRun(*rs_,
                            "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                            "HAVING c>1 ORDER BY c DESC LIMIT 1")
                  .empty());
  for (const std::string bad :
       {"SELECT k,k+9223372036854775807,COUNT(*) c FROM UniqueGroups "
        "GROUP BY k,k+9223372036854775807 ORDER BY c DESC LIMIT 1",
        "SELECT k,COUNT(*) c,SUM(v+9223372036854775807) FROM UniqueGroups "
        "GROUP BY k ORDER BY c DESC LIMIT 1",
        "SELECT k,COUNT(*) c FROM UniqueGroups WHERE 1/(8191-k)>-1 "
        "GROUP BY k ORDER BY c DESC LIMIT 1"}) {
    auto context = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto result = engine.Execute(context, bad);
    if (result.HasValue()) {
      const auto discarded = result.Value().Collect();
      EXPECT_NE(result.Value().GetStatus(), Status::kSuccess) << bad;
    }
  }
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "INSERT INTO UniqueGroups VALUES (0,'even',NULL,0.0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old,
                                       "SELECT k,COUNT(*) c FROM UniqueGroups "
                                       "GROUP BY k ORDER BY c DESC LIMIT 1"));
  const auto old_rows = old_result.Collect();
  ASSERT_SUCCESS(old_result.GetStatus());
  ASSERT_EQ(old_rows.size(), 1U);
  EXPECT_EQ(old_rows[0][1], Value(1));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC LIMIT 1"),
            (std::vector<Row>{Row({Value(0), Value(2)})}));
  ASSERT_SUCCESS(old.PreCommit());
  auto before_update = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "UPDATE UniqueGroups SET k=0 WHERE k=8191");
  ASSIGN_OR_ASSERT_FAIL(
      QueryResult, before_update_result,
      engine.Execute(before_update,
                     "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                     "ORDER BY c DESC LIMIT 1"));
  EXPECT_EQ(before_update_result.Collect(),
            (std::vector<Row>{Row({Value(0), Value(2)})}));
  ASSERT_SUCCESS(before_update_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC LIMIT 1"),
            (std::vector<Row>{Row({Value(0), Value(3)})}));
  ASSERT_SUCCESS(before_update.PreCommit());
}

TEST_F(RelationalRegressionTest,
       LiteralLikeFiltersKeepDictionaryAndRowAnswers) {
  RelationalRun(
      *rs_,
      "CREATE TABLE LikeInput(k INT64,s STRING,t STRING,label STRING,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("LikeInput"));
    for (int64_t i = 0; i < 24000; ++i) {
      std::string s = i % 3 == 0 ? "alpha" : "beta";
      s += std::to_string(i % 1024);
      s += i % 5 == 0 ? "尾" : "end";
      std::string t = std::to_string(i);
      t += i % 7 == 0 ? "needle" : "other";
      t.append(80, 'x');
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i % 113),
                            i % 17 == 0 ? Value() : Value(std::move(s)),
                            i % 19 == 0 ? Value() : Value(std::move(t)),
                            i % 23 == 0 ? Value()
                                        : Value(i % 2 == 0 ? "alpha" : "beta"),
                            Value(i % 307)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE LikeInput");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string column : {"s", "t", "label"}) {
      for (const std::string pattern :
           {"'%alpha%'", "'alpha%'", "'%尾'", "'%needle%'", "''", "'%'",
            "'a_ph%'", "'%a%l%'"}) {
        for (const std::string op : {" LIKE ", " NOT LIKE "}) {
          const auto query = [&](bool reference) {
            std::string sql =
                "SELECT k,MIN(s),MIN(t),COUNT(*) c,COUNT(DISTINCT n) FROM "
                "LikeInput WHERE ";
            sql += reference ? "(" + column + "||'')" : column;
            sql += op + pattern;
            sql += " GROUP BY k ORDER BY c DESC,k LIMIT 13 OFFSET 2";
            return sql;
          };
          EXPECT_EQ(RelationalRun(*rs_, query(false)),
                    RelationalRun(*rs_, query(true)))
              << column << op << pattern;
        }
      }
    }
    for (const std::string column : {"s", "label"}) {
      const auto query = [&](bool reference) {
        std::string sql =
            "SELECT " + column + ",COUNT(*) c FROM LikeInput WHERE ";
        sql += reference ? "(" + column + "||'')" : column;
        sql += " NOT LIKE '%alpha%' AND t LIKE '%needle%' GROUP BY " + column;
        sql += " ORDER BY c DESC," + column + " LIMIT 19";
        return sql;
      };
      EXPECT_EQ(RelationalRun(*rs_, query(false)),
                RelationalRun(*rs_, query(true)));
    }
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT k,SUM(CASE WHEN s LIKE '%alpha%' THEN n ELSE 0 "
                      "END) FROM LikeInput GROUP BY k ORDER BY k"),
        RelationalRun(*rs_,
                      "SELECT k,SUM(CASE WHEN s||'' LIKE '%alpha%' THEN n ELSE "
                      "0 END) FROM LikeInput GROUP BY k ORDER BY k"));
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT k,COUNT(*) FROM LikeInput WHERE s LIKE 'alpha%' "
                      "OR label NOT LIKE '%beta%' GROUP BY k ORDER BY k"),
        RelationalRun(
            *rs_,
            "SELECT k,COUNT(*) FROM LikeInput WHERE s||'' LIKE 'alpha%' OR "
            "label||'' NOT LIKE '%beta%' GROUP BY k ORDER BY k"));
  }
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT k,MIN(s),COUNT(*) FROM LikeInput WHERE s LIKE "
                    "'%123尾' AND s NOT LIKE '%alpha%' GROUP BY k ORDER BY k"),
      RelationalRun(
          *rs_,
          "SELECT k,MIN(s),COUNT(*) FROM LikeInput WHERE s||'' LIKE '%123尾' "
          "AND s||'' NOT LIKE '%alpha%' GROUP BY k ORDER BY k"));
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("LikeInput"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 2);
  EXPECT_FALSE(store->dictionaries[2].active);
  EXPECT_EQ(store->dictionaries[2].wide_code_bytes, 0);
  EXPECT_TRUE(store->dictionaries[3].active);
  SqlEngine engine(*rs_);
  const std::string sql =
      "SELECT k,COUNT(*) FROM LikeInput WHERE label LIKE 'alpha%' GROUP BY k "
      "ORDER BY k";
  const auto before = RelationalRun(*rs_, sql);
  RelationalRun(*rs_, "UPDATE LikeInput SET label='beta' WHERE k=0");
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result, engine.Execute(context, sql));
  EXPECT_EQ(old_result.Collect(), before);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, sql),
            RelationalRun(*rs_,
                          "SELECT k,COUNT(*) FROM LikeInput WHERE label||'' "
                          "LIKE 'alpha%' GROUP BY k ORDER BY k"));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest, ExtractDayGroupsTimestampTextByCalendarDay) {
  RelationalRun(*rs_, "CREATE TABLE ExtractTimes(s TIMESTAMP)");
  RelationalRun(
      *rs_,
      "INSERT INTO ExtractTimes VALUES ('2024-02-29 12:34:56+00'),('2024-03-01 "
      "00:12:34+00'),('2024-02-29T12:34:56Z'),(NULL)");
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT EXTRACT(DAY FROM s) d,COUNT(*) FROM ExtractTimes "
                    "GROUP BY d ORDER BY d NULLS FIRST"),
      (std::vector<Row>{Row({Value(), Value(1)}), Row({Value(1), Value(1)}),
                        Row({Value(29), Value(2)})}));
}

TEST_F(RelationalRegressionTest, DictionaryScalarKeysCountAfterFiltering) {
  RelationalRun(*rs_, "CREATE TABLE TimeCodeInput(n INT64,s TIMESTAMP)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("TimeCodeInput"));
    for (int64_t i = 0; i < 24000; ++i) {
      std::string text = "2020-01-01 00:";
      const int64_t minute = (i % 600) / 60;
      text.push_back(static_cast<char>('0' + (minute / 10)));
      text.push_back(static_cast<char>('0' + (minute % 10)));
      text.push_back(':');
      text.push_back(static_cast<char>('0' + ((i % 60) / 10)));
      text.push_back(static_cast<char>('0' + (i % 10)));
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i),
                            i % 17 == 0 ? Value() : Value(std::move(text))}))
              .GetStatus());
    }
    // This value must not be evaluated when the row predicate rejects it.
    ASSERT_SUCCESS(table
                       ->Insert(setup.txn_,
                                Row({Value(24000), Value("invalid-timestamp")}))
                       .GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE TimeCodeInput");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const auto& [key, reference] :
         std::vector<std::pair<std::string, std::string>>{
             {"TIMESTAMP_TRUNC(s,MINUTE)", "TIMESTAMP_TRUNC(s||'',MINUTE)"},
             {"EXTRACT(MINUTE FROM s)", "EXTRACT(MINUTE FROM s||'')"},
             {"BYTE_LENGTH(s)", "BYTE_LENGTH(s||'')"}}) {
      for (const std::string predicate :
           {"n<24000", "n>=20000 AND n<24000", "n<0"}) {
        const auto query = [&](const std::string& expression) {
          std::string sql = "SELECT ";
          sql += expression;
          sql += " AS m,COUNT(*) c FROM TimeCodeInput WHERE ";
          sql += predicate;
          sql += " GROUP BY m ORDER BY m NULLS LAST LIMIT 20";
          return sql;
        };
        EXPECT_EQ(RelationalRun(*rs_, query(key)),
                  RelationalRun(*rs_, query(reference)))
            << key;
      }
    }
  }
  // Exercise the encoded path directly, including regrouping many physical
  // timestamp values into one minute and retaining the NULL group.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          reader.GetTable("TimeCodeInput"));
    const auto key = FunctionCallExp(
        "timestamp_trunc", {ColumnValueExp("s"), ColumnValueExp("MINUTE")});
    const auto count = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("*"));
    SelectStatement statement(
        {NamedExpression("m", key), NamedExpression("c", count)}, {}, nullptr);
    statement.SetGroupBy({key});
    statement.SetOrderBy({{key, true, false}});
    statement.SetLimit(20);
    relational_detail::ExecutionRuntime runtime;
    reader.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        std::optional<relational_detail::Relation>, grouped,
        relational_detail::TryDictionaryExpressionAggregate(
            reader, statement, *table, {}, table->GetSchema(), {count.get()},
            {BinaryExpressionExp(ColumnValueExp("n"),
                                 BinaryOperation::kLessThan,
                                 ConstantValueExp(Value(24000)))},
            {}));
    if (!grouped) {
      FAIL() << "dictionary scalar grouping declined";
    }
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(grouped->rows,
              RelationalRun(*rs_,
                            "SELECT TIMESTAMP_TRUNC(s||'',MINUTE) m,COUNT(*) "
                            "FROM TimeCodeInput WHERE n<24000 GROUP BY m "
                            "ORDER BY m NULLS LAST LIMIT 20"));
    ASSERT_SUCCESS(reader.PreCommit());
  }
  // LIMIT must not hide the invalid key, even if it would not be retained.
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(
      context,
      "SELECT TIMESTAMP_TRUNC(s,MINUTE) m,COUNT(*) FROM TimeCodeInput "
      "GROUP BY m ORDER BY m LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, PackedTimestampGroupsKeepFallbacksAndOrder) {
  RelationalRun(*rs_, "CREATE TABLE PackedTimeCodes(n INT64,s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("PackedTimeCodes"));
    const std::array<std::string_view, 4> dates{"1969-12-31", "2000-02-29",
                                                "2024-03-31", "9999-12-31"};
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text(dates[static_cast<size_t>((i / 600) % 4)]);
      text += i % 3 == 0 ? "T12:" : " 12:";
      const int64_t minute = (i % 600) / 60;
      text.push_back('0');
      text.push_back(static_cast<char>('0' + minute));
      text.push_back(':');
      text.push_back(static_cast<char>('0' + ((i % 60) / 10)));
      text.push_back(static_cast<char>('0' + (i % 10)));
      if (i % 3 != 1) {
        text += "+00";
      }
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i % 7),
                            i % 31 == 0 ? Value() : Value(std::move(text))}))
              .GetStatus());
    }
    for (const char* text :
         {"2024-03-31 12:00:01.1+00", "2024-03-31 13:00:01+01",
          "1969-12-31 23:59:60+00"}) {
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(-2), Value(std::string(text))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        table->Insert(setup.txn_, Row({Value(-3), Value("bad-time")}))
            .GetStatus());
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
                          context.GetTable("PackedTimeCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto query = [](std::string_view input, std::string_view unit,
                        std::string_view filter, std::string_view tail,
                        std::string_view zone = "UTC") {
    std::string sql = "SELECT TIMESTAMP_TRUNC(";
    sql += input;
    sql += ",";
    sql += unit;
    sql += ",'";
    sql += zone;
    sql += "') m,COUNT(*) c FROM PackedTimeCodes WHERE ";
    sql += filter;
    sql += " GROUP BY m";
    sql += tail;
    return sql;
  };
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const std::string_view unit : {"SECOND", "MINUTE", "DAY"}) {
      for (const std::string_view tail :
           {" ORDER BY m NULLS FIRST LIMIT 11 OFFSET 3",
            " ORDER BY m DESC NULLS LAST LIMIT 13", " ORDER BY c DESC LIMIT 7",
            " LIMIT 0"}) {
        EXPECT_EQ(RelationalRun(*rs_, query("s", unit, "n>=0", tail)),
                  RelationalRun(*rs_, query("s||''", unit, "n>=0", tail)));
      }
    }
    // Noncanonical valid values must restart ordinary evaluation for the
    // whole grouping operation, so peers never mix strings and packed keys.
    for (const std::string_view zone : {"UTC", "+09:00"}) {
      EXPECT_EQ(RelationalRun(*rs_, query("s", "MINUTE", "n>=-2",
                                          " ORDER BY m LIMIT 17", zone)),
                RelationalRun(*rs_, query("s||''", "MINUTE", "n>=-2",
                                          " ORDER BY m LIMIT 17", zone)));
    }
  }
  SqlEngine engine(*rs_);
  auto invalid = rs_->BeginReadOnlyContext();
  auto bad = engine.Execute(
      invalid, query("s", "MINUTE", "n>=-3", " ORDER BY c DESC LIMIT 1"));
  if (bad.HasValue()) {
    static_cast<void>(bad.Value().Collect());
    EXPECT_EQ(bad.Value().GetStatus(), Status::kInvalidArgument);
  } else {
    EXPECT_EQ(bad.GetStatus(), Status::kInvalidArgument);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, invalid.Abort());
  const std::string snapshot_query =
      query("s", "MINUTE", "n>=0", " ORDER BY c DESC,m NULLS FIRST LIMIT 13");
  const auto expected = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(
      *rs_, "INSERT INTO PackedTimeCodes VALUES (0,'2024-03-31 12:00:00+00')");
  ASSIGN_OR_ASSERT_FAIL(QueryResult, result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(result.Collect(), expected);
  ASSERT_SUCCESS(result.GetStatus());
  EXPECT_EQ(
      RelationalRun(*rs_, snapshot_query),
      RelationalRun(*rs_, query("s||''", "MINUTE", "n>=0",
                                " ORDER BY c DESC,m NULLS FIRST LIMIT 13")));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DictionaryMultiplicityKeepsFirstEncounterOrder) {
  for (const int64_t domain : {300, 70000}) {
    SCOPED_TRACE(domain);
    const std::string name = "FoldOrder" + std::to_string(domain);
    const Schema schema(name, {Column("s", ValueType::kVarChar)});
    const int64_t rows = domain == 300 ? 30000 : domain * 2;
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
      for (int64_t i = 0; i < rows; ++i) {
        // Permute values and put NULL first. Repeated values cross pages,
        // and some values first occur only after their initial NULL slot.
        const int64_t id = (i * 73) % domain;
        const Value value =
            i % 997 == 0
                ? Value()
                : Value(std::string(80, 'x') +
                        static_cast<char>('a' + (id % 7)) + std::to_string(id));
        ASSERT_SUCCESS(table.Insert(setup.txn_, Row({value})).GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable(name));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, domain == 300 ? 2 : 4);
    const auto column = ColumnValueExp("s");
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    SelectStatement statement(
        {NamedExpression("s", column), NamedExpression("c", count)}, {},
        nullptr);
    statement.SetGroupBy({column});
    std::vector<Row> folded;
    ASSIGN_OR_ASSERT_FAIL(
        bool, used,
        relational_detail::TryFoldDictionaryAggregateRows(
            context, statement, *table, {}, schema,
            {&count->AsAggregateExpression()}, {},
            [&](const Row& row, int64_t multiplicity) {
              folded.emplace_back(Row({row[0], Value(multiplicity)}));
              return Status::kSuccess;
            }));
    ASSERT_TRUE(used);
    EXPECT_EQ(folded, RelationalRun(*rs_, "SELECT s||'',COUNT(*) FROM " + name +
                                              " GROUP BY s||''"));
    ASSERT_FALSE(folded.empty());
    EXPECT_TRUE(folded.front()[0].IsNull());
    size_t visits = 0;
    const auto stopped = relational_detail::TryFoldDictionaryAggregateRows(
        context, statement, *table, {}, schema,
        {&count->AsAggregateExpression()}, {}, [&](const Row&, int64_t) {
          ++visits;
          return Status::kConflicts;
        });
    EXPECT_EQ(stopped.GetStatus(), Status::kConflicts);
    EXPECT_EQ(visits, 1U);
    for (const size_t workers : {size_t{3}, size_t{7}}) {
      std::atomic<size_t> active{1};
      ScopedQueryParallelismBudget parallelism(&active, workers);
      std::vector<Row> prepared_rows;
      size_t prepared_visits = 0;
      ASSIGN_OR_ASSERT_FAIL(
          bool, prepared_used,
          relational_detail::TryFoldDictionaryAggregateRows(
              context, statement, *table, {}, schema,
              {&count->AsAggregateExpression()}, {},
              [&](const Row& row, int64_t multiplicity) {
                prepared_rows.emplace_back(Row({row[0], Value(multiplicity)}));
                return Status::kSuccess;
              },
              [&](const Row& row, int64_t multiplicity, const Value& key,
                  const std::vector<relational_detail::AggregateAccumulator>&
                      arguments) {
                EXPECT_EQ(key, row[0]);
                EXPECT_EQ(arguments.size(), 1U);
                EXPECT_EQ(arguments[0].count, multiplicity);
                prepared_rows.emplace_back(Row({row[0], Value(multiplicity)}));
                ++prepared_visits;
                return Status::kSuccess;
              }));
      EXPECT_TRUE(prepared_used);
      EXPECT_GT(prepared_visits, 0U);
      EXPECT_EQ(prepared_rows, folded);
    }
    ASSERT_SUCCESS(context.PreCommit());
    // A scalar key can merge several source codes into one group. Preserve
    // its first-occurrence tie order after filtering and applying OFFSET.
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT REGEXP_REPLACE(s,'[0-9]+$','') k,COUNT(*) c,"
                      "MIN(s) FROM " +
                          name + " WHERE s<>'' GROUP BY k LIMIT 2 OFFSET 1"),
        RelationalRun(*rs_,
                      "SELECT REGEXP_REPLACE(s||'','[0-9]+$','') k,"
                      "COUNT(*) c,MIN(s) FROM " +
                          name + " WHERE s<>'' GROUP BY k LIMIT 2 OFFSET 1"));
  }
}

TEST_F(RelationalRegressionTest,
       ParallelDictionaryFoldKeepsTemporalUnitSemantics) {
  RelationalRun(*rs_, "CREATE TABLE TemporalFold(s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("TemporalFold"));
    for (int64_t i = 0; i < 6000; ++i) {
      std::string text = "2024-03-31 12:0";
      text += std::to_string((i % 300) / 60);
      text += i % 60 < 10 ? ":0" : ":";
      text += std::to_string(i % 60);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(std::move(text))})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  std::atomic<size_t> active{1};
  ScopedQueryParallelismBudget parallelism(&active, 3);
  for (const std::string_view function :
       {"TIMESTAMP_TRUNC", "DATETIME_TRUNC"}) {
    const auto query = [&](bool ordinary) {
      std::string sql = "SELECT ";
      sql += function;
      sql += ordinary ? "(s||'',MINUTE) k" : "(s,MINUTE) k";
      sql +=
          ",COUNT(*) c,MIN(s),AVG(BYTE_LENGTH(s)) FROM TemporalFold "
          "GROUP BY k HAVING c>0 ORDER BY k";
      return sql;
    };
    const auto expected = RelationalRun(*rs_, query(true));
    ASSERT_EQ(expected.size(), 5U);
    EXPECT_EQ(RelationalRun(*rs_, query(false)), expected);
  }
}

TEST_F(RelationalRegressionTest, ParallelScalarFoldWithoutGlobalDictionary) {
  RelationalRun(*rs_, "CREATE TABLE RawScalar(s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("RawScalar"));
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text(static_cast<size_t>(i % 19 == 0 ? 2000 : 1), 'x');
      text += std::to_string(i);
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({i % 97 == 0    ? Value()
                            : i % 101 == 0 ? Value("")
                                           : Value(std::move(text))}))
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
                          context.GetTable("RawScalar"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 0);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto query = [](bool ordinary) {
    std::string sql = "SELECT REGEXP_REPLACE(";
    sql += ordinary ? "s||''" : "s";
    sql +=
        ",'[0-9]+$','') k,COUNT(*) c,COUNT(s),MIN(s),MAX(s),"
        "AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM RawScalar "
        "WHERE s<>'' GROUP BY k HAVING c>3 ORDER BY k LIMIT 2";
    return sql;
  };
  const auto expected = RelationalRun(*rs_, query(true));
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{7}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, query(false)), expected);
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
    EXPECT_EQ(RelationalRun(*rs_, query(false)), expected);
  }
}

TEST_F(RelationalRegressionTest,
       DictionaryMultiplicityKeepsAggregatesAndHaving) {
  RelationalRun(*rs_, "CREATE TABLE RepeatedText(s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("RepeatedText"));
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text = "圧縮された文字列-";
      text.push_back(static_cast<char>('a' + (i % 7)));
      text += std::string(static_cast<size_t>(i % 5), 'x');
      text += std::to_string(i % 600);
      ASSERT_SUCCESS(table
                         ->Insert(context.txn_,
                                  Row({i % 19 == 0   ? Value()
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
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("RepeatedText"));
  const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
  ASSERT_NE(store, nullptr);
  ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{7}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const bool filtered : {false, true}) {
      const Expression column = ColumnValueExp("s");
      const Expression key = FunctionCallExp(
          "regexp_replace", {column, ConstantValueExp(Value("[0-9]+$")),
                             ConstantValueExp(Value(""))});
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto nonnull =
          AggregateExpressionExp(AggregationType::kCount, column);
      const auto minimum =
          AggregateExpressionExp(AggregationType::kMin, column);
      const auto maximum =
          AggregateExpressionExp(AggregationType::kMax, column);
      const auto average = AggregateExpressionExp(
          AggregationType::kAvg, FunctionCallExp("byte_length", {column}));
      SelectStatement statement(
          {NamedExpression("k", key), NamedExpression("c", count),
           NamedExpression("n", nonnull), NamedExpression("lo", minimum),
           NamedExpression("hi", maximum), NamedExpression("av", average)},
          {},
          filtered ? BinaryExpressionExp(column, BinaryOperation::kNotEquals,
                                         ConstantValueExp(Value("")))
                   : Expression{});
      SelectSource source;
      source.table = "RepeatedText";
      statement.SetSources({source});
      statement.SetGroupBy({key});
      statement.SetHaving(BinaryExpressionExp(
          count, BinaryOperation::kGreaterThan, ConstantValueExp(Value(100))));
      statement.SetOrderBy({{key, true, true}});
      statement.SetLimit(13);
      statement.SetOffset(1);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.scan_rows, 12000U);
      EXPECT_LT(runtime.scan_values_decoded, 5000U);
      EXPECT_GT(runtime.scan_output_rows, runtime.scan_values_decoded);
      std::string reference =
          "SELECT REGEXP_REPLACE(s||'','[0-9]+$','') k,COUNT(*) c,COUNT(s),"
          "MIN(s),MAX(s),AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM "
          "RepeatedText";
      if (filtered) {
        reference += " WHERE s<>''";
      }
      reference +=
          " GROUP BY k HAVING c>100 ORDER BY k NULLS FIRST LIMIT 13 OFFSET 1";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
      context.set_execution_runtime(nullptr);
    }
  }
  // Decline order-sensitive/overflow-sensitive payloads rather than changing
  // their accumulation order. The callback must never run on rejection.
  const auto key = FunctionCallExp("lower", {ColumnValueExp("s")});
  const auto sum = AggregateExpressionExp(
      AggregationType::kSum,
      FunctionCallExp("byte_length", {ColumnValueExp("s")}));
  SelectStatement unsupported(
      {NamedExpression("k", key), NamedExpression("v", sum)}, {}, nullptr);
  unsupported.SetGroupBy({key});
  bool visited = false;
  ASSIGN_OR_ASSERT_FAIL(
      bool, folded,
      relational_detail::TryFoldDictionaryAggregateRows(
          context, unsupported, *table, {}, table->GetSchema(),
          {&sum->AsAggregateExpression()}, {}, [&](const Row&, int64_t) {
            visited = true;
            return Status::kSuccess;
          }));
  EXPECT_FALSE(folded);
  EXPECT_FALSE(visited);
  ASSERT_SUCCESS(context.PreCommit());
  // Invalid scalar expressions are not evaluated for rejected dictionary
  // values, but LIMIT must not hide an error on an observed input.
  EXPECT_TRUE(
      RelationalRun(
          *rs_,
          "SELECT REGEXP_REPLACE(s,'[','') k,COUNT(*),MIN(s) FROM RepeatedText "
          "WHERE s='absent' GROUP BY k LIMIT 1")
          .empty());
  auto error_context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(
      error_context,
      "SELECT REGEXP_REPLACE(s,'[','') k,COUNT(*),MIN(s) FROM RepeatedText "
      "GROUP BY k LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, error_context.Abort());
}

}  // namespace
}  // namespace tinylamb
