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

// Regression tests for the stream_agg / shared base-relation cache in
// relational.cpp (improvements2.md §6.1 family):
//
//   ExecutionRuntime::base_relations entries are keyed by
//   BaseRelationCacheKey(table, projection) and MUST always hold the
//   UNFILTERED projection of the table. Every consumer applies its own
//   predicates while reading. If any producer ever stores filtered rows under
//   a plain key (e.g. applying stashed table_key_filters or scan predicates
//   during a cache fill), later consumers silently aggregate a subset as if it
//   were the whole table.
//
// Data shape used by every test:
//   AggTable: 200 rows, k = i % 10, v = 1  => SUM(v) over all rows = 200,
//                                             SUM(k + v) over all rows =
//                                               900 + 200 = 1100
//   SmallKeys: k in {1, 2, 3, 4}

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/query_parallelism.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "executor/relational.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// Executes SQL through the SqlEngine in a fresh committed context and returns
// every produced row.
std::vector<Row> RelationalRun(Database& database, std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    context.Abort();
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return rows;
}

std::string ExplainAnalyze(Database& database, std::string_view sql);

class RelationalRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "relational_regression-" + RandomString();
    rs_ = Database::Create(prefix_).MoveValue();
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, agg,
        rs_->CreateTable(ctx,
                         Schema("AggTable", {Column("k", ValueType::kInt64),
                                             Column("v", ValueType::kInt64)})));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          agg.Insert(ctx.txn_, Row({Value(i % 10), Value(1)})).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, small,
        rs_->CreateTable(
            ctx, Schema("SmallKeys", {Column("k", ValueType::kInt64)})));
    for (const int64_t key : {int64_t{1}, int64_t{2}, int64_t{3}, int64_t{4}}) {
      ASSERT_SUCCESS(small.Insert(ctx.txn_, Row({Value(key)})).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, driver,
        rs_->CreateTable(ctx,
                         Schema("KeyTable", {Column("k", ValueType::kInt64)})));
    for (int64_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(driver.Insert(ctx.txn_, Row({Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

TEST_F(RelationalRegressionTest,
       RuntimeFilterOwnerIncludesStatementGeneration) {
  SelectStatement statement;
  relational_detail::TableKeyFilter filter;
  filter.SetOwner(statement);
  ASSERT_TRUE(filter.OwnedBy(statement));
  statement.SetWhereClause(nullptr);
  EXPECT_FALSE(filter.OwnedBy(statement));
  filter.SetOwner(statement);
  SelectStatement replacement(statement);
  statement = replacement;
  EXPECT_FALSE(filter.OwnedBy(statement));
}

TEST_F(RelationalRegressionTest,
       FoldedHavingUnionDoesNotReuseSiblingKeyFilters) {
  const auto rows = RelationalRun(*rs_, R"sql(
    WITH cutoff AS (SELECT 0 AS n)
    SELECT k, SUM(n) AS total FROM (
      SELECT a.k, COUNT(*) AS n
      FROM AggTable a, KeyTable d, SmallKeys i
      WHERE a.k = d.k AND a.k = i.k AND d.k < 3
      GROUP BY a.k HAVING COUNT(*) > (SELECT n FROM cutoff)
      UNION ALL
      SELECT a.k, COUNT(*) AS n
      FROM AggTable a, KeyTable d, SmallKeys i
      WHERE a.k = d.k AND a.k = i.k AND d.k >= 3
      GROUP BY a.k HAVING COUNT(*) > (SELECT n FROM cutoff)
      UNION ALL
      SELECT a.k, COUNT(*) AS n
      FROM AggTable a, KeyTable d, SmallKeys i
      WHERE a.k = d.k AND a.k = i.k
      GROUP BY a.k HAVING COUNT(*) > (SELECT n FROM cutoff)
    ) u GROUP BY ROLLUP(k) ORDER BY k NULLS LAST
  )sql");
  ASSERT_EQ(rows.size(), 5);
  for (int64_t k = 1; k <= 4; ++k) {
    EXPECT_EQ(rows[k - 1], Row({Value(k), Value(40)}));
  }
  EXPECT_EQ(rows[4], Row({Value(), Value(160)}));
}

TEST_F(RelationalRegressionTest, CorrelatedDisjunctKeysKeepResidualAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE OrKeys (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO OrKeys VALUES (1,1),(1,2),(1,9),(2,2),(NULL,1)");
  const std::string sql =
      "SELECT d.k, (SELECT COUNT(*) FROM OrKeys a WHERE "
      "(a.k=d.k AND a.v=1) OR (d.k=a.k AND a.v=2)) AS n "
      "FROM (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT NULL "
      "UNION ALL SELECT 3) d ORDER BY d.k NULLS LAST";
  EXPECT_EQ(
      RelationalRun(*rs_, sql),
      std::vector<Row>({Row({Value(1), Value(2)}), Row({Value(2), Value(1)}),
                        Row({Value(3), Value(0)}), Row({Value(), Value(0)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("correlated_index_builds=1"),
            std::string::npos);
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT (SELECT COUNT(*) FROM OrKeys a WHERE "
                "(a.k=d.k AND a.v=1) OR a.v=2) FROM SmallKeys d ORDER BY d.k"),
            std::vector<Row>({Row({Value(3)}), Row({Value(2)}), Row({Value(2)}),
                              Row({Value(2)})}));
}

TEST_F(RelationalRegressionTest, CorrelatedStringDisjunctUsesIndexedAggregate) {
  RelationalRun(*rs_, "CREATE TABLE OrStrings (k STRING, v INT64)");
  RelationalRun(
      *rs_,
      "INSERT INTO OrStrings VALUES ('a',1),('a',2),('a',9),('A',2),(NULL,1)");
  const std::string sql =
      "SELECT d.k,(SELECT COUNT(*) FROM OrStrings a WHERE "
      "(a.k=d.k AND a.v=1) OR (a.k=d.k AND a.v=2)) FROM "
      "(SELECT 'a' AS k UNION ALL SELECT 'A' UNION ALL SELECT 'missing' "
      "UNION ALL SELECT NULL) d ORDER BY d.k NULLS LAST";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value("A"), Value(1)}), Row({Value("a"), Value(2)}),
                 Row({Value("missing"), Value(0)}), Row({Value(), Value(0)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_builds=1"), std::string::npos);
  EXPECT_NE(profile.find("correlated_result_cache_hits=2"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest,
       CorrelatedAliasRetainsResidualColumnsBeyondSharedProjection) {
  RelationalRun(*rs_, "CREATE TABLE AliasExists (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO AliasExists VALUES "
                "(1,1),(1,2),(1,NULL),(2,4),(2,4),(3,NULL),(NULL,9)");
  // The root's bare k can be shared, but the inner alias also needs v to
  // evaluate its residual. A cached projection must never discard that v.
  const std::string sql =
      "SELECT k FROM AliasExists a WHERE EXISTS "
      "(SELECT * FROM AliasExists b WHERE a.k=b.k AND a.v<>b.v) ORDER BY k";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1)}), Row({Value(1)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_builds=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("base_scan_cache_hits=0,"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, CorrelatedExistsStarPrunesUnusedJoinPayload) {
  RelationalRun(*rs_,
                "CREATE TABLE WideExists(k INT64, flag INT64, payload STRING)");
  RelationalRun(*rs_,
                "INSERT INTO WideExists VALUES "
                "(1,2,'a'),(1,NULL,'b'),(2,3,'c'),(3,1,'d')");
  const std::string sql = R"sql(
    SELECT d.k FROM SmallKeys d WHERE EXISTS (
      SELECT * FROM WideExists f,KeyTable b
      WHERE f.k=d.k AND f.flag=b.k AND b.k>=2) ORDER BY d.k
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1)}), Row({Value(2)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_builds=1,"), std::string::npos)
      << profile;
  // A row-valued dependency in the residual must still keep its payload.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT d.k FROM SmallKeys d WHERE EXISTS (
      SELECT * FROM WideExists f,KeyTable b WHERE f.k=d.k AND f.flag=b.k
      AND f.payload='c') ORDER BY d.k
  )sql"),
            std::vector<Row>({Row({Value(2)})}));
}

TEST_F(RelationalRegressionTest, SingleUseCteFiltersCrossLeftJoinTowardBuild) {
  const std::string sql = R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k),
         b AS (SELECT k AS bk, SUM(v) AS bv FROM AggTable GROUP BY k),
         c AS (SELECT k AS ck, SUM(v) AS cv FROM AggTable WHERE k <> 2 GROUP BY k)
    SELECT ak, av, bv, cv FROM a LEFT JOIN b ON ak=bk LEFT JOIN c ON bk=ck
    WHERE ak=2
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(2), Value(20), Value(20), Value()})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("aggregate_input_rows=40,"), std::string::npos)
      << profile;
  // A nullable-side ON condition must not restrict the preserved input.
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k),
         b AS (SELECT k AS bk FROM AggTable WHERE k=1 GROUP BY k)
    SELECT ak, bk FROM a LEFT JOIN b ON ak=bk WHERE ak>=1 AND ak<=2 ORDER BY ak
  )sql"),
      std::vector<Row>({Row({Value(1), Value(1)}), Row({Value(2), Value()})}));
}

TEST_F(RelationalRegressionTest, ParallelFilterMovesOwnedRowBuffers) {
  auto context = rs_->BeginContext();
  relational_detail::Relation relation;
  relation.schema = Schema("f", {Column("k", ValueType::kInt64),
                                 Column("text", ValueType::kVarChar)});
  std::vector<const Value*> buffers;
  for (int64_t k = 0; k < 40000; ++k) {
    relation.AddRow(
        Row({Value(k), k % 3 == 0 ? Value() : Value(std::string(80, 'x'))}));
    buffers.push_back(relation.rows.back().values_.data());
  }
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  ASSERT_SUCCESS(relational_detail::FilterRelation(
      context, &relation,
      {BinaryExpressionExp(ColumnValueExp(ColumnName("k")),
                           BinaryOperation::kLessThan,
                           ConstantValueExp(Value(30000)))},
      nullptr, {}));
  ASSERT_EQ(relation.rows.size(), 30000U);
  std::vector<bool> seen(30000);
  for (const auto& row : relation.rows) {
    const auto key = static_cast<size_t>(row[0].value.int_value);
    ASSERT_LT(key, seen.size());
    EXPECT_FALSE(seen[key]);
    seen[key] = true;
    EXPECT_EQ(row.values_.data(), buffers[key]);
    EXPECT_EQ(row[1].IsNull(), key % 3 == 0);
  }
}

TEST_F(RelationalRegressionTest, AuditParallelFilterPreservesDeferredRowOrder) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  struct RestoreBudget {
    RestoreBudget() = default;
    RestoreBudget(const RestoreBudget&) = delete;
    RestoreBudget& operator=(const RestoreBudget&) = delete;
    RestoreBudget(RestoreBudget&&) = delete;
    RestoreBudget& operator=(RestoreBudget&&) = delete;
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  for (bool spill_rows : {false, true}) {
    QueryMemoryBudget::Global().ResetForTest(spill_rows ? 65536
                                                        : restore.limit);
    for (bool move_rows : {false, true}) {
      relational_detail::Relation source;
      source.schema = Schema("f", {Column("k", ValueType::kInt64),
                                   Column("d", ValueType::kInt64)});
      for (int64_t i = 0; i < 40000; ++i) {
        source.rows.emplace_back(Row({Value(i), Value(i % 7 == 3 ? 0 : 1)}));
      }
      relational_detail::CompiledScanFilter filter;
      filter.disjunctive_branches.resize(2);
      filter.disjunctive_branches[0].residual.push_back(BinaryExpressionExp(
          ConstantValueExp(Value(1)), BinaryOperation::kDivide,
          ColumnValueExp(ColumnName("d"))));
      // A division error in the first OR branch is masked by this true branch.
      // Workers defer those rows; serial evaluation accepts them in-place.
      relational_detail::Relation result;
      result.schema = source.schema;
      Status error{Status::kSuccess};
      ASSERT_TRUE(relational_detail::TryParallelFilterRows(
          context, source, filter, source.schema, nullptr, {}, nullptr, &result,
          &error, move_rows ? &source : nullptr));
      ASSERT_SUCCESS(error);
      ASSERT_SUCCESS(result.FinishSpill());
      if (spill_rows) {
        EXPECT_TRUE(result.HasSpill());
      }
      size_t count = 0;
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_EQ(row[0].value.int_value, static_cast<int64_t>(count++));
      }));
      EXPECT_EQ(count, 40000U);
    }
  }
}

TEST_F(RelationalRegressionTest,
       AuditRelationalDumpDoesNotInventAdaptiveSwitch) {
  auto context = rs_->BeginContext();
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{
          NamedExpression("k", ColumnValueExp(ColumnName("a", "k")))},
      std::vector<std::string>{},
      BinaryExpressionExp(ColumnValueExp(ColumnName("a", "k")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("b", "k"))));
  SelectSource left;
  left.table = "AggTable";
  left.alias = "a";
  SelectSource right = left;
  right.alias = "b";
  statement->SetSources({left, right});
  RelationalExecutor executor(context, statement);
  Row row;
  size_t count = 0;
  while (executor.Next(&row, nullptr)) {
    ++count;
  }
  ASSERT_SUCCESS(executor.GetStatus());
  EXPECT_EQ(count, 4000U);
  std::ostringstream dump;
  executor.Dump(dump, 0);
  ASSERT_NE(dump.str().find(", hash_joins=1,"), std::string::npos)
      << dump.str();
  EXPECT_EQ(dump.str().find("AdaptiveJoin"), std::string::npos) << dump.str();
}

TEST_F(RelationalRegressionTest, SingleUseCteResolvesQualifiedGroupingKeys) {
  const std::string sql = R"sql(
    WITH a AS (SELECT x.k AS ak,SUM(x.v) AS av FROM AggTable x GROUP BY k),
         b AS (SELECT y.k AS bk,SUM(y.v) AS bv FROM AggTable y GROUP BY k)
    SELECT ak,av,bv FROM a LEFT JOIN b ON ak=bk WHERE ak=2
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(2), Value(20), Value(20)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("aggregate_input_rows=40,"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, CtePushdownPreservesSharedUsesAndBoundaries) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k)
    SELECT x.ak,y.ak FROM a x, a y WHERE x.ak=2 AND y.ak=3
  )sql"),
            std::vector<Row>({Row({Value(2), Value(3)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak FROM AggTable ORDER BY k LIMIT 1)
    SELECT ak FROM a WHERE ak=2
  )sql"),
            std::vector<Row>());
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k),
         b AS (SELECT SUM(av) AS total FROM a)
    SELECT ak,total FROM a,b WHERE ak=2
  )sql"),
            std::vector<Row>({Row({Value(2), Value(200)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, ROW_NUMBER() OVER (ORDER BY k) AS n FROM SmallKeys)
    SELECT n FROM a WHERE ak=3
  )sql"),
            std::vector<Row>({Row({Value(3)})}));
}

TEST_F(RelationalRegressionTest, UnionJoinBoundsPreserveDuplicatesAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE UnionDim (dk INT64, flag INT64)");
  RelationalRun(*rs_, "INSERT INTO UnionDim VALUES (2,1),(2,1),(NULL,1),(3,0)");
  RelationalRun(*rs_, "ANALYZE UnionDim");
  const std::string prefix = R"sql(
    SELECT SUM(u.v) FROM (
      SELECT k AS key, v FROM AggTable
      UNION ALL SELECT k AS other, 10 AS value FROM KeyTable
    ) u, UnionDim d WHERE u.key=d.dk AND d.flag=
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, prefix + "1"),
            std::vector<Row>({Row({Value(60)})}));
  EXPECT_EQ(RelationalRun(*rs_, prefix + "99"),
            std::vector<Row>({Row({Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM (
      SELECT k AS key, v FROM AggTable
      UNION ALL SELECT k AS other, 10 AS value FROM KeyTable
    ) u LEFT JOIN UnionDim d ON u.key=d.dk AND d.flag=1 WHERE u.key=9
  )sql"),
            std::vector<Row>({Row({Value(21)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT SUM(u.v) FROM (
      (SELECT k AS key,v FROM AggTable ORDER BY k LIMIT 1)
      UNION ALL SELECT k AS other,10 AS value FROM KeyTable
    ) u, UnionDim d WHERE u.key=d.dk AND d.flag=1
  )sql"),
            std::vector<Row>({Row({Value(20)})}));
  const std::string union_body =
      "(SELECT k AS key,v FROM AggTable UNION ALL "
      "SELECT k AS other,10 AS value FROM KeyTable) named";
  for (const auto& [modifier, expected] :
       std::vector<std::pair<std::string, Value>>{
           {"", Value(60)},
           {" WHERE key>5", Value()},
           {" ORDER BY key LIMIT 1", Value()}}) {
    std::string query = "SELECT SUM(u.v) FROM (SELECT key,v FROM ";
    query += union_body;
    query += modifier;
    query += ") u,UnionDim d WHERE u.key=d.dk AND d.flag=1";
    EXPECT_EQ(RelationalRun(*rs_, query), std::vector<Row>({Row({expected})}));
  }
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT SUM(u.v) FROM (SELECT DISTINCT key,v FROM " +
                              union_body +
                              ") u,UnionDim d WHERE u.key=d.dk AND d.flag=1"),
      std::vector<Row>({Row({Value(22)})}));
}

TEST_F(RelationalRegressionTest, SharedCteFiltersBeforeCopyingConsumerColumns) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  auto shared = std::make_shared<Relation>();
  shared->schema = Schema("shared", {Column("k", ValueType::kInt64),
                                     Column("v", ValueType::kVarChar),
                                     Column("unused", ValueType::kVarChar)});
  shared->rows = {Row({Value(1), Value("one"), Value("unused")}),
                  Row({Value(1), Value("one"), Value("unused")}),
                  Row({Value(2), Value(), Value("unused")}),
                  Row({Value(), Value("null"), Value("unused")})};
  const auto original = shared->rows;
  relational_detail::CteMap ctes{{"shared", shared}};
  SelectSource left;
  left.table = "shared";
  left.alias = "a";
  SelectSource right = left;
  right.alias = "b";
  const auto comparison = [](const char* alias, int64_t value) {
    return BinaryExpressionExp(ColumnValueExp(ColumnName(alias, "k")),
                               BinaryOperation::kEquals,
                               ConstantValueExp(Value(value)));
  };
  SelectStatement statement(
      {NamedExpression("v", ColumnValueExp(ColumnName("a", "v")))}, {},
      BinaryExpressionExp(comparison("a", 1), BinaryOperation::kAnd,
                          comparison("b", 2)));
  statement.SetSources({left, right});
  bool applied = false;
  auto result = relational_detail::BuildInput(context, statement, nullptr, ctes,
                                              &applied);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_TRUE(applied);
  EXPECT_EQ(result.Value().TotalRows(), 2U);
  // a keeps k and v; b only needs k. Neither copies the unused payload.
  EXPECT_EQ(result.Value().schema.ColumnCount(), 3U);
  EXPECT_EQ(shared->rows, original);
  EXPECT_EQ(shared->schema.ColumnCount(), 3U);

  statement.SetWhereClause(BinaryExpressionExp(
      comparison("a", 2), BinaryOperation::kAnd, comparison("b", 1)));
  result = relational_detail::BuildInput(context, statement, nullptr, ctes,
                                         &applied);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_EQ(result.Value().TotalRows(), 2U);
  const auto output = relational_detail::LocalColumnOffset(
      result.Value().schema, ColumnName("a", "v"));
  if (!output) {
    FAIL() << "missing projected CTE payload";
  }
  for (const Row& row : result.Value().rows) {
    EXPECT_TRUE(row[*output].IsNull());
  }
  EXPECT_EQ(shared->rows, original);
}

TEST_F(RelationalRegressionTest,
       MaterializedStarKeepsNullsDuplicatesAndResiduals) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  relational_detail::CteMap ctes;
  const auto bind = [&](const std::string& name, std::vector<Column> columns,
                        std::vector<Row> rows) {
    auto relation = std::make_shared<Relation>();
    relation->schema = Schema(name, std::move(columns));
    relation->rows = std::move(rows);
    ctes[name] = std::move(relation);
  };
  bind(
      "f",
      {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
       Column("v", ValueType::kInt64)},
      {Row({Value(1), Value(10), Value(5)}),
       Row({Value(1), Value(10), Value(5)}),
       Row({Value(2), Value(20), Value(7)}),
       Row({Value(), Value(10), Value(9)}), Row({Value(1), Value(), Value(11)}),
       Row({Value(3), Value(10), Value(13)}),
       Row({Value(1), Value(30), Value(17)})});
  bind("a",
       {Column("k", ValueType::kInt64), Column("payload", ValueType::kVarChar)},
       {Row({Value(1), Value("one")}), Row({Value(2), Value()}),
        Row({Value(), Value("null")})});
  bind("b",
       {Column("k", ValueType::kInt64), Column("payload", ValueType::kVarChar)},
       {Row({Value(10), Value("ten")}), Row({Value(20), Value("twenty")}),
        Row({Value(), Value("null")})});
  const auto column = [](const char* table, const char* name) {
    return ColumnValueExp(ColumnName(table, name));
  };
  const auto edge_a = BinaryExpressionExp(
      column("f", "a"), BinaryOperation::kEquals, column("a", "k"));
  const auto edge_b = BinaryExpressionExp(
      column("f", "b"), BinaryOperation::kEquals, column("b", "k"));
  SelectStatement statement(
      {NamedExpression("v", column("f", "v")),
       NamedExpression("pa", column("a", "payload")),
       NamedExpression("pb", column("b", "payload"))},
      {}, BinaryExpressionExp(edge_a, BinaryOperation::kAnd, edge_b));
  std::vector<SelectSource> sources;
  for (const char* name : {"f", "a", "b"}) {
    SelectSource source;
    source.table = name;
    sources.push_back(std::move(source));
  }
  statement.SetSources(std::move(sources));
  const auto run = [&](size_t count, size_t copies = 1) {
    std::vector<Row> serial;
    for (size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      bool applied = false;
      auto result = relational_detail::BuildInput(context, statement, nullptr,
                                                  ctes, &applied);
      ASSERT_TRUE(result.HasValue());
      EXPECT_TRUE(applied);
      EXPECT_EQ(result.Value().TotalRows(), count);
      if (copies > 1) {
        EXPECT_EQ(result.Value().schema.ColumnCount(), 3U);
      }
      size_t fives = 0;
      size_t sevens = 0;
      for (const Row& row : result.Value().rows) {
        const auto v = relational_detail::LocalColumnOffset(
            result.Value().schema, ColumnName("f", "v"));
        ASSERT_TRUE(v.has_value());
        if (row[*v] == Value(5)) {
          ++fives;
        } else {
          EXPECT_EQ(row[*v], Value(7));
          ++sevens;
        }
      }
      EXPECT_EQ(sevens, copies);
      EXPECT_EQ(fives, count - copies);
      if (workers == 1) {
        serial = result.Value().rows;
      } else {
        EXPECT_EQ(result.Value().rows, serial);
      }
    }
  };
  run(3);
  const auto original = ctes["f"]->rows;
  ctes["f"]->rows.reserve(original.size() * 8192);
  for (size_t repeat = 1; repeat < 8192; ++repeat) {
    ctes["f"]->rows.insert(ctes["f"]->rows.end(), original.begin(),
                           original.end());
  }
  run(size_t{3} * 8192, 8192);
  ctes["f"]->rows = original;

  // A duplicated build key must retain both matches via the general join.
  ctes["a"]->rows.push_back(Row({Value(1), Value("another")}));
  run(5);
  ctes["a"]->rows.pop_back();
  // A non-key cross-source predicate must still be evaluated.
  statement.SetWhereClause(BinaryExpressionExp(
      statement.WhereClause(), BinaryOperation::kAnd,
      BinaryExpressionExp(
          column("f", "v"), BinaryOperation::kGreaterThan,
          BinaryExpressionExp(column("a", "k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(4))))));
  run(1);
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(RelationalRegressionTest, RollupUnionNormalizesAllComputedNumericTerms) {
  RelationalRun(*rs_, "CREATE TABLE RollupNumeric (a INT64,b INT64,v DOUBLE)");
  RelationalRun(
      *rs_, "INSERT INTO RollupNumeric VALUES (1,1,NULL),(2,1,1.5),(2,2,2.5)");
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT a,b,SUM(COALESCE(v,0)) AS n FROM RollupNumeric
    GROUP BY ROLLUP(a,b) ORDER BY a NULLS LAST,b NULLS LAST
  )sql"),
            std::vector<Row>({Row({Value(1), Value(1), Value(0.0)}),
                              Row({Value(1), Value(), Value(0.0)}),
                              Row({Value(2), Value(1), Value(1.5)}),
                              Row({Value(2), Value(2), Value(2.5)}),
                              Row({Value(2), Value(), Value(4.0)}),
                              Row({Value(), Value(), Value(4.0)})}));
}

TEST_F(RelationalRegressionTest, SharedGroupedCteKeepsEveryConsumerDomain) {
  const std::string sql = R"sql(
    WITH c AS (
      SELECT k AS key, SUM(v) AS n FROM AggTable GROUP BY k
      UNION ALL SELECT k AS other, SUM(v) AS n FROM AggTable GROUP BY k
    )
    SELECT SUM(a.n+b.n) FROM c a, c b WHERE a.key=1 AND b.key=1+1
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql), std::vector<Row>({Row({Value(160)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH c AS (SELECT k AS key, SUM(v) AS n FROM AggTable GROUP BY k)
    SELECT SUM(a.n+b.n) FROM c a, c b WHERE a.key=1
  )sql"),
            std::vector<Row>({Row({Value(400)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH c AS (SELECT k AS key, SUM(v) AS n FROM AggTable GROUP BY k LIMIT 2)
    SELECT COUNT(*) FROM c a, c b WHERE a.key=9 AND b.key=8
  )sql"),
            std::vector<Row>({Row({Value(0)})}));
}

TEST_F(RelationalRegressionTest, SharedGroupedJoinDomainsKeepCompleteGroups) {
  RelationalRun(*rs_,
                "CREATE TABLE DomainDay (day INT64, week INT64, year INT64)");
  RelationalRun(*rs_, "CREATE TABLE DomainFact (day INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO DomainDay VALUES (1,10,2000),(2,10,2001),"
                "(3,20,2002),(4,30,2003),(5,NULL,2001)");
  RelationalRun(*rs_,
                "INSERT INTO DomainFact VALUES (1,10),(2,20),(3,40),(4,80)");
  RelationalRun(*rs_, "ANALYZE DomainDay");
  const std::string prefix = R"sql(
    WITH c AS (
      SELECT d.week, SUM(f.v) AS total FROM DomainFact f, DomainDay d
      WHERE f.day=d.day GROUP BY d.week
    )
  )sql";
  // Week 10 spans two years. A consumer's year is not a legal filter on
  // the aggregate input: all of the requested week's rows must survive.
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT SUM(a.total+b.total)
    FROM (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=2001) a,
         (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=2002) b
  )sql"),
            std::vector<Row>({Row({Value(70)})}));
  // An unrestricted site needs every group, including week 30.
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT SUM(a.total+b.total)
    FROM (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=2001) a, c b
  )sql"),
            std::vector<Row>({Row({Value(240)})}));
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT COUNT(*)
    FROM (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=1990) a,
         (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=1991) b
  )sql"),
            std::vector<Row>({Row({Value(0)})}));
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT COUNT(*) FROM c a LEFT JOIN c b ON a.week=b.week
    LEFT JOIN DomainDay d ON a.week=d.week AND d.year=2001
  )sql"),
            std::vector<Row>({Row({Value(3)})}));
}

TEST_F(RelationalRegressionTest, FusedDoubleArithmeticKeepsNullsAndErrors) {
  RelationalRun(*rs_,
                "CREATE TABLE DoubleFact "
                "(k INT64, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO DoubleFact VALUES "
                "(1, 10.0, 2.0, 3.0, 4.0), "
                "(1, NULL, 2.0, 3.0, 4.0), (2, 20.0, 4.0, 5.0, 6.0)");
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT t.k, SUM((f.a - f.b - f.c + f.d) / 2) "
                    "FROM KeyTable t LEFT JOIN DoubleFact f ON t.k = f.k "
                    "GROUP BY t.k ORDER BY t.k");
  ASSERT_EQ(rows.size(), 10U);
  for (size_t i = 0; i < rows.size(); ++i) {
    if (i == 1 || i == 2) {
      EXPECT_EQ(rows[i][1], Value(i == 1 ? 4.5 : 8.5));
    } else {
      EXPECT_TRUE(rows[i][1].IsNull());
    }
  }
  // A NULL on the left cannot suppress an error in a separately evaluated
  // right subtree. The grouped kernel must retain the AST's evaluation order.
  RelationalRun(*rs_, "INSERT INTO DoubleFact VALUES (3, NULL, 1.0, 0.0, 1.0)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto prepared = engine.Prepare(
      context,
      "SELECT t.k, SUM(f.a + (f.b / f.c)) "
      "FROM KeyTable t JOIN DoubleFact f ON t.k = f.k GROUP BY t.k");
  ASSERT_TRUE(prepared.HasValue());
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
  context.Abort();
}

TEST_F(RelationalRegressionTest,
       FusedSingletonCrossJoinKeepsAggregateSemantics) {
  const std::string sql = R"sql(
    SELECT a.k,SUM(a.v),MAX(c.m) FROM AggTable a,
        (SELECT MAX(k) AS m FROM SmallKeys) c
    GROUP BY a.k HAVING SUM(a.v)>MAX(c.m) ORDER BY a.k
  )sql";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 10U);
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i], Row({Value(i), Value(20), Value(4)}));
  }
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT c.m,COUNT(*),SUM(a.v),MAX(c.m) FROM AggTable a,
        (SELECT MAX(k) AS m FROM SmallKeys WHERE k=99) c GROUP BY c.m
  )sql"),
      std::vector<Row>({Row({Value(), Value(200), Value(200), Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*),SUM(a.v),MAX(c.m) FROM AggTable a,
        (SELECT MAX(k) AS m FROM SmallKeys) c WHERE a.k=99
  )sql"),
            std::vector<Row>({Row({Value(0), Value(), Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*),SUM(a.v) FROM AggTable a,
        (SELECT k AS m FROM SmallKeys WHERE k=99) c
  )sql"),
            std::vector<Row>({Row({Value(0), Value()})}));
  // Two build rows multiply every input row and must not enter the singleton
  // path, even if their values happen to be equal.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT a.k,COUNT(*),SUM(a.v) FROM AggTable a,
        (SELECT 1 AS m UNION ALL SELECT 1) c GROUP BY a.k ORDER BY a.k LIMIT 1
  )sql"),
            std::vector<Row>({Row({Value(0), Value(40), Value(40)})}));
}

TEST_F(RelationalRegressionTest, InnerOnEdgesReorderBeforePreservedOuterJoins) {
  RelationalRun(*rs_, "CREATE TABLE ReorderFact (k INT64, day INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE ReorderStock (k INT64, day INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE ReorderDay (day INT64, week INT64)");
  RelationalRun(*rs_, "CREATE TABLE ReorderExtra (k INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ReorderFact VALUES (1, 10), (2, 10), (3, 20)");
  RelationalRun(*rs_,
                "INSERT INTO ReorderStock VALUES "
                "(1, 11, 5), (1, 11, 6), (1, 20, 99), (2, 11, 7), (3, 20, 8)");
  RelationalRun(*rs_,
                "INSERT INTO ReorderDay VALUES (10, 1), (11, 1), (20, 2)");
  RelationalRun(*rs_, "INSERT INTO ReorderExtra VALUES (1), (1), (9)");
  const std::string prefix =
      "FROM ReorderFact f JOIN ReorderStock s ON f.k = s.k "
      "JOIN ReorderDay a ON f.day = a.day "
      "JOIN ReorderDay b ON s.day = b.day ";
  const auto rows = RelationalRun(
      *rs_, "SELECT f.k, SUM(s.v), COUNT(e.k) " + prefix +
                "LEFT JOIN ReorderExtra e ON f.k = e.k "
                "WHERE a.week = b.week GROUP BY f.k ORDER BY f.k");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(22), Value(4)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(7), Value(0)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(8), Value(0)}));
  const auto right = RelationalRun(
      *rs_,
      "SELECT e.k, COUNT(f.k) " + prefix +
          "RIGHT JOIN ReorderExtra e ON f.k = e.k "
          "WHERE a.week = b.week OR f.k IS NULL GROUP BY e.k ORDER BY e.k");
  ASSERT_EQ(right.size(), 2U);
  EXPECT_EQ(right[0], Row({Value(1), Value(4)}));
  EXPECT_EQ(right[1], Row({Value(9), Value(0)}));
}

TEST_F(RelationalRegressionTest, GroupedWindowFusesJoinBeforeWindowEvaluation) {
  RelationalRun(*rs_, "CREATE TABLE WindowKeys (k INT64)");
  RelationalRun(*rs_, "INSERT INTO WindowKeys VALUES (1), (2), (40)");
  const std::string sql =
      "SELECT a.k, SUM(SUM(b.v)) OVER (ORDER BY a.k "
      "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running, "
      "MAX(COUNT(b.v)) OVER () AS largest "
      "FROM WindowKeys a LEFT JOIN AggTable b ON a.k = b.k "
      "GROUP BY a.k QUALIFY running >= 40 ORDER BY a.k";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(2), Value(40), Value(20)}));
  EXPECT_EQ(rows[1], Row({Value(40), Value(40), Value(20)}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  const auto reused = RelationalRun(
      *rs_,
      "SELECT a.k, SUM(b.v) AS subtotal, SUM(SUM(b.v)) OVER "
      "(ORDER BY a.k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
      "AS running FROM WindowKeys a LEFT JOIN AggTable b ON a.k = b.k "
      "GROUP BY a.k ORDER BY a.k");
  ASSERT_EQ(reused.size(), 3U);
  EXPECT_EQ(reused[0], Row({Value(1), Value(20), Value(20)}));
  EXPECT_EQ(reused[1], Row({Value(2), Value(20), Value(40)}));
  EXPECT_EQ(reused[2], Row({Value(40), Value(), Value(40)}));
}

TEST_F(RelationalRegressionTest, GroupedWindowsReuseTemporaryInputColumns) {
  RelationalRun(*rs_, "CREATE TABLE WindowReuse (k INT64, p STRING, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO WindowReuse VALUES "
                "(1,'a',3),(1,'a',4),(2,'a',NULL),(3,'b',5)");
  const std::string grouped =
      "SELECT SUM(SUM(v)) OVER (PARTITION BY p ORDER BY k) AS running, "
      "k, MAX(COUNT(v)) OVER (PARTITION BY p) AS largest "
      "FROM WindowReuse GROUP BY k,p";
  const std::vector<Row> expected = {Row({Value(7), Value(1), Value(2)}),
                                     Row({Value(7), Value(2), Value(2)}),
                                     Row({Value(5), Value(3), Value(1)})};
  EXPECT_EQ(RelationalRun(*rs_, "SELECT * FROM (" + grouped + ") q ORDER BY k"),
            expected);
  EXPECT_EQ(RelationalRun(*rs_, grouped + " ORDER BY k"), expected);
  // Every projected item can be a window, and multiple windows can consume
  // the same hidden aggregate before its cell is reused.
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT * FROM (SELECT SUM(SUM(v)) OVER () AS s, "
                          "MAX(SUM(v)) OVER () AS m FROM WindowReuse "
                          "GROUP BY k,p) q"),
            std::vector<Row>(3, Row({Value(12), Value(7)})));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT * FROM (SELECT k, SUM(v) AS subtotal, "
                          "SUM(SUM(v)) OVER () AS total FROM WindowReuse "
                          "GROUP BY k) q ORDER BY k"),
            std::vector<Row>({Row({Value(1), Value(7), Value(12)}),
                              Row({Value(2), Value(), Value(12)}),
                              Row({Value(3), Value(5), Value(12)})}));
}

TEST_F(RelationalRegressionTest, FusedConstantOutputsKeepNullsAndTopN) {
  RelationalRun(*rs_, "CREATE TABLE ConstantKeys (k INT64)");
  RelationalRun(*rs_, "INSERT INTO ConstantKeys VALUES (1),(40)");
  const std::string from =
      " FROM ConstantKeys k LEFT JOIN AggTable a ON k.k=a.k ";
  const std::string sql =
      "SELECT k.k,'channel' AS tag,NULL AS empty,42 AS fixed,"
      "SUM(a.v),COUNT(a.v)" +
      from + "GROUP BY k.k ORDER BY tag,k.k LIMIT 2";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1), Value("channel"), Value(),
                                   Value(42), Value(20), Value(20)}),
                              Row({Value(40), Value("channel"), Value(),
                                   Value(42), Value(), Value(0)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  EXPECT_EQ(RelationalRun(*rs_, "SELECT 'count' AS tag,k.k,COUNT(a.v)" + from +
                                    "GROUP BY k.k ORDER BY tag,k.k LIMIT 1"),
            std::vector<Row>({Row({Value("count"), Value(1), Value(20)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT 'empty',NULL,SUM(a.v),COUNT(*) "
          "FROM ConstantKeys k JOIN AggTable a ON k.k=a.k WHERE k.k=40"),
      std::vector<Row>({Row({Value("empty"), Value(), Value(), Value(0)})}));
}

TEST_F(RelationalRegressionTest, FullJoinNullAggregatesKeepMultiplicity) {
  RelationalRun(*rs_, "CREATE TABLE NullLeft (k INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE NullRight (k INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO NullLeft VALUES (1,10),(1,NULL),(2,20),(NULL,30)");
  RelationalRun(
      *rs_, "INSERT INTO NullRight VALUES (1,100),(1,NULL),(3,200),(NULL,300)");
  const std::string select =
      "SELECT SUM(CASE WHEN l.k IS NOT NULL AND r.k IS NULL THEN 1 ELSE 0 END),"
      "SUM(CASE WHEN l.k IS NULL AND r.k IS NOT NULL THEN 1 ELSE 0 END),"
      "SUM(CASE WHEN l.k IS NOT NULL AND r.k IS NOT NULL THEN 1 ELSE 0 END),"
      "COUNT(*),SUM(CASE WHEN l.v IS NULL THEN 1 ELSE 0 END),"
      "SUM(CASE WHEN l.k IS NULL THEN 10 END),"
      "COUNT(CASE WHEN l.k IS NOT NULL THEN NULL ELSE 1 END) ";
  const std::string from = "FROM NullLeft l FULL JOIN NullRight r ON l.k=r.k";
  EXPECT_EQ(RelationalRun(*rs_, select + from),
            std::vector<Row>({Row({Value(1), Value(1), Value(4), Value(8),
                                   Value(4), Value(30), Value(3)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, select + from).find("grouped_join_fusions=1"),
            std::string::npos);
  // A rejected ON pair leaves BOTH input rows eligible for null extension.
  EXPECT_EQ(RelationalRun(*rs_, select + from + " AND l.v>r.v"),
            std::vector<Row>({Row({Value(3), Value(3), Value(0), Value(8),
                                   Value(5), Value(50), Value(5)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_, select + "FROM (SELECT * FROM NullLeft WHERE k<0) l FULL JOIN "
                         "(SELECT * FROM NullRight WHERE k<0) r ON l.k=r.k"),
      std::vector<Row>({Row(
          {Value(), Value(), Value(), Value(0), Value(), Value(), Value(0)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, select + "FROM (SELECT * FROM NullLeft WHERE k<0) l "
                                   "FULL JOIN NullRight r ON l.k=r.k"),
      std::vector<Row>({Row({Value(0), Value(3), Value(0), Value(4), Value(4),
                             Value(40), Value(4)})}));
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto overflow = engine.Execute(
      context,
      "SELECT SUM(CASE WHEN l.k IS NULL THEN 9223372036854775807 ELSE 0 END) " +
          from);
  if (overflow.HasValue()) {
    overflow.Value().Drain();
    EXPECT_EQ(overflow.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(overflow.GetStatus(), Status::kIsInfinity);
  }
  context.Abort();
}

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
    EXPECT_EQ(direct[i][2], i % 3 == 0 ? Value() : Value(i * 0.5));
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
          // The contextful expression route retains the general comparator.
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

TEST_F(RelationalRegressionTest, DictionaryCountTopNWith32BitCodes) {
  RelationalRun(*rs_, "CREATE TABLE Code32Input(k INT64,s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("Code32Input"));
    for (int64_t i = 0; i < int64_t{65536} * 3; ++i) {
      std::string text(80, 'x');
      text += std::to_string(i % 65536);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(i), Value(std::move(text))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        table->Insert(setup.txn_, Row({Value(-1), Value()})).GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE Code32Input");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string predicate : {"", " WHERE k>=65536"}) {
      std::string actual = "SELECT s,COUNT(*) c FROM Code32Input";
      actual += predicate;
      actual += " GROUP BY s ORDER BY s NULLS FIRST LIMIT 17";
      std::string reference = "SELECT s||'' AS s,COUNT(*) c FROM Code32Input";
      reference += predicate;
      reference += " GROUP BY s||'' ORDER BY s NULLS FIRST LIMIT 17";
      EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, reference));
    }
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
    EXPECT_EQ(rows[i - 3],
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
        relational_detail::TryDictionaryExpressionCountTopN(
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
  context.Abort();
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
    for (const std::string& text :
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
  invalid.Abort();
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
  for (const bool filtered : {false, true}) {
    const Expression column = ColumnValueExp("s");
    const Expression key = FunctionCallExp(
        "regexp_replace", {column, ConstantValueExp(Value("[0-9]+$")),
                           ConstantValueExp(Value(""))});
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    const auto nonnull =
        AggregateExpressionExp(AggregationType::kCount, column);
    const auto minimum = AggregateExpressionExp(AggregationType::kMin, column);
    const auto maximum = AggregateExpressionExp(AggregationType::kMax, column);
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
        "MIN(s),MAX(s),AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM RepeatedText";
    if (filtered) {
      reference += " WHERE s<>''";
    }
    reference +=
        " GROUP BY k HAVING c>100 ORDER BY k NULLS FIRST LIMIT 13 OFFSET 1";
    EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
    context.set_execution_runtime(nullptr);
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
  error_context.Abort();
}

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
  context.Abort();
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
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
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
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
         {" LIMIT 17 OFFSET 5", " ORDER BY c DESC LIMIT 13 OFFSET 3"}) {
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
  // exact bitmap product exceeds the cap and the ordinary aggregator runs.
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

TEST_F(RelationalRegressionTest, WideIntegerCodesKeepGroupingAndDistinct) {
  for (const int64_t cardinality : {int64_t{300}, int64_t{65536}}) {
    const std::string name = "IntCodeInput" + std::to_string(cardinality);
    RelationalRun(*rs_, "CREATE TABLE " + name + "(n INT64,k INT64,d DATE)");
    const int64_t rows = cardinality == 300 ? 12000 : cardinality * 4;
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            setup.GetTable(name));
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
        ASSERT_SUCCESS(table->Insert(setup.txn_, Row({Value(i), value, date}))
                           .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    RelationalRun(*rs_, "ANALYZE " + name);
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
        EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, reference));
      }
      EXPECT_EQ(RelationalRun(
                    *rs_, "SELECT k,d,COUNT(*) c FROM " + name +
                              " GROUP BY k,d ORDER BY c DESC,k NULLS FIRST,d "
                              "NULLS LAST LIMIT 19"),
                RelationalRun(
                    *rs_, "SELECT MIN(k) k,d,COUNT(*) c FROM " + name +
                              " GROUP BY CAST(k AS STRING),d ORDER BY c DESC,k "
                              "NULLS FIRST,d NULLS LAST LIMIT 19"));
      EXPECT_EQ(
          RelationalRun(*rs_, "SELECT COUNT(DISTINCT k) FROM " + name),
          RelationalRun(
              *rs_, "SELECT COUNT(DISTINCT CAST(k AS STRING)) FROM " + name));
      std::string distinct_query = "SELECT COUNT(DISTINCT k) FROM ";
      distinct_query += name;
      distinct_query += " WHERE k<>0";
      std::string distinct_reference =
          "SELECT COUNT(DISTINCT CAST(k AS STRING)) FROM ";
      distinct_reference += name;
      distinct_reference += " WHERE k<>0";
      EXPECT_EQ(RelationalRun(*rs_, distinct_query),
                RelationalRun(*rs_, distinct_reference));
      EXPECT_EQ(
          RelationalRun(*rs_, "SELECT COUNT(DISTINCT d) FROM " + name),
          RelationalRun(
              *rs_, "SELECT COUNT(DISTINCT CAST(d AS STRING)) FROM " + name));
      EXPECT_EQ(
          RelationalRun(
              *rs_, "SELECT n FROM " + name + " WHERE k<0 AND n<50 ORDER BY n"),
          RelationalRun(
              *rs_,
              "SELECT n FROM " + name +
                  " WHERE CAST(k AS STRING) LIKE '-%' AND n<50 ORDER BY n"));
      EXPECT_EQ(
          RelationalRun(
              *rs_, "SELECT d,COUNT(*) c FROM " + name +
                        " GROUP BY d ORDER BY d NULLS LAST LIMIT 11 OFFSET 3"),
          RelationalRun(*rs_, "SELECT d,COUNT(*) c FROM " + name +
                                  " GROUP BY d,BYTE_LENGTH(CAST(d AS STRING)) "
                                  "ORDER BY d NULLS LAST LIMIT 11 OFFSET 3"));
    }
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    const auto store = table->GetLSTableStore();
    ASSERT_NE(store, nullptr);
    EXPECT_EQ(store->dictionaries[1].wide_code_bytes,
              cardinality == 300 ? 2 : 4);
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
    for (int64_t i = 0; i < 30000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({Value(i % 5),
                       i % 19 == 0 ? Value() : Value((i % 1009) * 10000001)}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
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

TEST_F(RelationalRegressionTest, GroupingSetParentMapsKeepOrderAndSubsets) {
  auto context = rs_->BeginContext();
  relational_detail::Relation input;
  input.schema = Schema(
      "", {Column("a", ValueType::kVarChar), Column("b", ValueType::kInt64),
           Column("c", ValueType::kInt64), Column("v", ValueType::kDouble)});
  const std::vector<double> values{1.0e16, 1.0, -1.0e16, -0.0};
  for (int64_t i = 0; i < 32768; ++i) {
    input.AddRow(Row({i % 11 == 0 ? Value() : Value(std::to_string(i % 3)),
                      Value(i % 17), Value(i % 127),
                      Value(values[static_cast<size_t>(i % 4)])}));
  }
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  std::vector<SelectStatement> statements;
  for (const auto& keys : std::vector<std::vector<std::string>>{
           {"a", "b", "c"}, {"a", "b"}, {"a"}, {"b"}, {}}) {
    std::vector<NamedExpression> select;
    std::vector<Expression> group;
    for (const auto& name : keys) {
      group.push_back(ColumnValueExp(name));
      select.emplace_back(name, group.back());
    }
    select.emplace_back("s", sum);
    statements.emplace_back(std::move(select), std::vector<std::string>{},
                            nullptr);
    statements.back().SetGroupBy(std::move(group));
    statements.back().SetGroupAll(keys.empty());
  }
  std::vector<const SelectStatement*> sets;
  sets.reserve(statements.size());
  for (const auto& statement : statements) {
    sets.push_back(&statement);
  }
  std::vector<std::vector<Row>> reference;
  for (const size_t workers : {size_t{1}, size_t{2}}) {
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

TEST_F(RelationalRegressionTest, GroupingSetBorrowedKeysBecomeOwnedOutputs) {
  auto context = rs_->BeginContext();
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         ColumnValueExp("v"));
  std::vector<SelectStatement> statements;
  for (const auto& names :
       std::vector<std::vector<std::string>>{{"a", "b"}, {"a"}, {}}) {
    std::vector<Expression> keys;
    std::vector<NamedExpression> projection;
    for (const auto& name : names) {
      keys.push_back(ColumnValueExp(name));
      projection.emplace_back(name, keys.back());
    }
    projection.emplace_back("s", sum);
    statements.emplace_back(std::move(projection), std::vector<std::string>{},
                            nullptr);
    statements.back().SetGroupBy(std::move(keys));
    statements.back().SetGroupAll(names.empty());
  }
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
  std::vector<SelectStatement> statements;
  for (const auto& names :
       std::vector<std::vector<std::string>>{{"a", "b"}, {"a"}, {}}) {
    std::vector<Expression> keys;
    std::vector<NamedExpression> projection;
    for (const auto& name : names) {
      keys.push_back(ColumnValueExp(name));
      projection.emplace_back(name, keys.back());
    }
    projection.emplace_back("s", sum);
    projection.emplace_back("a", avg);
    projection.emplace_back("n", count);
    statements.emplace_back(std::move(projection), std::vector<std::string>{},
                            nullptr);
    statements.back().SetGroupBy(std::move(keys));
    statements.back().SetGroupAll(names.empty());
  }
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
// through bytecode programs (workers cannot run the contextful evaluator;
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

TEST_F(RelationalRegressionTest, WherePredicateError_PropagatesFailure) {
  TransactionContext context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(
      Table, t,
      rs_->CreateTable(context,
                       Schema("ErrTable", {Column("id", ValueType::kInt64),
                                           Column("val", ValueType::kInt64)})));
  ASSERT_SUCCESS(
      t.Insert(context.txn_,
               Row({Value(1), Value(std::numeric_limits<int64_t>::min())}))
          .GetStatus());
  ASSERT_SUCCESS(context.txn_.PreCommit());

  TransactionContext run_ctx = rs_->BeginContext();
  StatusOr<Executor> prepared =
      engine.Prepare(run_ctx, "SELECT id FROM ErrTable WHERE ABS(val) <= 0;");
  ASSERT_TRUE(prepared.HasValue());
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
  run_ctx.Abort();
}

TEST_F(RelationalRegressionTest,
       ConjunctEvaluationToleratesErrorOnRejectedRow) {
  TransactionContext context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(
      Table, t,
      rs_->CreateTable(context,
                       Schema("ErrTable2", {Column("id", ValueType::kInt64),
                                            Column("i", ValueType::kInt64),
                                            Column("f", ValueType::kDouble)})));
  ASSERT_SUCCESS(
      t.Insert(context.txn_,
               Row({Value(1), Value(std::numeric_limits<int64_t>::min()),
                    Value(-100.0)}))
          .GetStatus());
  ASSERT_SUCCESS(t.Insert(context.txn_, Row({Value(2), Value(0), Value(10.0)}))
                     .GetStatus());
  ASSERT_SUCCESS(context.txn_.PreCommit());

  // Simple compare rejects row 1 (f > 0 is false for row 1):
  // Both orders of conjuncts must succeed and return id = 2.
  {
    std::vector<Row> r1 = RelationalRun(
        *rs_, "SELECT id FROM ErrTable2 WHERE f > 0 AND ABS(i) <= 3;");
    ASSERT_EQ(r1.size(), 1U);
    EXPECT_EQ(r1[0][0], Value(2));

    std::vector<Row> r2 = RelationalRun(
        *rs_, "SELECT id FROM ErrTable2 WHERE ABS(i) <= 3 AND f > 0;");
    ASSERT_EQ(r2.size(), 1U);
    EXPECT_EQ(r2[0][0], Value(2));
  }

  // Residual compare rejects row 1 (SAFE_ADD(i, -1) = 0 is false/null for row
  // 1): Both orders of conjuncts must succeed and return empty.
  {
    std::vector<Row> r1 =
        RelationalRun(*rs_,
                      "SELECT id FROM ErrTable2 WHERE (SAFE_ADD(i, -1) = 0) "
                      "AND ABS(i) <= 3;");
    EXPECT_TRUE(r1.empty());

    std::vector<Row> r2 = RelationalRun(*rs_,
                                        "SELECT id FROM ErrTable2 WHERE ABS(i) "
                                        "<= 3 AND (SAFE_ADD(i, -1) = 0);");
    EXPECT_TRUE(r2.empty());
  }

  // When no conjunct rejects row 1 (f < 0 is true for row 1), ABS(i) must
  // throw.
  {
    TransactionContext run_ctx = rs_->BeginContext();
    StatusOr<Executor> prepared = engine.Prepare(
        run_ctx, "SELECT id FROM ErrTable2 WHERE f < 0 AND ABS(i) <= 3;");
    ASSERT_TRUE(prepared.HasValue());
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
    run_ctx.Abort();
  }
}

TEST_F(RelationalRegressionTest, WindowFunctions_ArgumentAndFrameValidation) {
  auto check_fails = [&](std::string_view sql) {
    TransactionContext run_ctx = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(run_ctx, sql);
    if (!prepared.HasValue()) {
      run_ctx.Abort();
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    run_ctx.Abort();
  };

  // NTILE validations
  check_fails("SELECT NTILE(NULL) OVER () FROM KeyTable;");
  check_fails("SELECT NTILE(-1) OVER () FROM KeyTable;");
  check_fails("SELECT NTILE(0) OVER () FROM KeyTable;");

  // LAG / LEAD validations
  check_fails("SELECT LAG(k, NULL) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT LAG(k, -1) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT LEAD(k, NULL) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT LEAD(k, -1) OVER (ORDER BY k) FROM KeyTable;");

  // Row-dependent offset evaluation in LAG
  {
    std::vector<Row> rows = RelationalRun(
        *rs_, "SELECT LAG(k, k) OVER (ORDER BY k) FROM KeyTable;");
    ASSERT_EQ(rows.size(), 10U);
    for (size_t i = 0; i < 10; ++i) {
      // Offset is k, so target is i - i = 0 for each row; always yields row 0
      // (Value(0)).
      EXPECT_EQ(rows[i][0], Value(0));
    }
  }

  // NTH_VALUE validations
  check_fails("SELECT NTH_VALUE(k, NULL) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT NTH_VALUE(k, 0) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT NTH_VALUE(k, -1) OVER (ORDER BY k) FROM KeyTable;");

  // Window frame bound validations
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k ROWS BETWEEN -1 PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k RANGE BETWEEN NULL PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k RANGE BETWEEN -1 PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k RANGE BETWEEN NULL PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
}

// Collects EXPLAIN ANALYZE output lines for a statement.
std::string ExplainAnalyze(Database& database, std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(
      context, std::string("EXPLAIN ANALYZE ") + std::string(sql));
  std::string profile;
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    context.Abort();
    return profile;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    profile += std::string(row[0].value.varchar_value) + "\n";
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return profile;
}

TEST_F(RelationalRegressionTest, DerivedSingleColumnEquiJoinUsesHashJoin) {
  // TPC-H Q20 at SF1: joining a base table against a single-column derived
  // table on `AggTable.k = s.k` must resolve the key into a hash join.
  // Previously the derived output schema stayed bare while outer predicates
  // reference it through the source alias, so the equality went unresolved
  // and execution fell back to cross product + filter (800K x derived rows
  // at SF1). LoadSource now alias-qualifies derived schemas, mirroring the
  // estimate path.
  const std::string profile = ExplainAnalyze(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k FROM SmallKeys) s "
      "WHERE AggTable.k = s.k;");
  EXPECT_NE(profile.find("nested_loop_joins=0"), std::string::npos) << profile;

  // Results are unchanged by the join shape (20 matches per key 1..4).
  const std::vector<Row> rows = RelationalRun(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k FROM SmallKeys) s "
      "WHERE AggTable.k = s.k;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{80}));
}

TEST_F(RelationalRegressionTest, DerivedSingleColumnBareEqualityUsesHashJoin) {
  // Same join with bare, disjoint column names (`v = kk`, the TPC-H Q20
  // `ps_partkey = p_partkey` shape). Previously IsVirtualValueTableField
  // claimed every bare predicate column against the single-column derived
  // schema, marking the equality ambiguous and forcing the same cross
  // product fallback.
  const std::string profile = ExplainAnalyze(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k AS kk FROM SmallKeys) s "
      "WHERE v = kk;");
  EXPECT_NE(profile.find("nested_loop_joins=0"), std::string::npos) << profile;

  // v is constantly 1 while derived kk ranges over {1,2,3,4}: every one
  // of the 200 AggTable rows matches exactly the kk = 1 row.
  const std::vector<Row> rows = RelationalRun(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k AS kk FROM SmallKeys) s "
      "WHERE v = kk;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{200}));
}

// Grouped-join fusion: the final join of a grouped statement feeds the
// aggregation directly and the joined relation never materializes.  These
// pins guard the fused emit against the materializing path's semantics:
// LEFT-join NULL padding feeds COUNT a NULL (not a skip), every outer row
// joins multiplicatively, and ORDER BY/LIMIT still resolve over the
// projected output.
TEST_F(RelationalRegressionTest,
       UniformExtremaKeepNullExtensionAndVaryingInputs) {
  RelationalRun(*rs_, "CREATE TABLE ConstantDimension(k INT64, v DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO ConstantDimension VALUES (1,7.5),(2,7.5),(2,7.5)");
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT a.k,SUM(a.v),MIN(d.v),MAX(d.v) FROM AggTable a
    LEFT JOIN ConstantDimension d ON a.k=d.k
    GROUP BY a.k ORDER BY a.k LIMIT 4
  )sql"),
      std::vector<Row>({Row({Value(0), Value(20), Value(), Value()}),
                        Row({Value(1), Value(20), Value(7.5), Value(7.5)}),
                        Row({Value(2), Value(40), Value(7.5), Value(7.5)}),
                        Row({Value(3), Value(20), Value(), Value()})}));
  RelationalRun(*rs_, "INSERT INTO ConstantDimension VALUES (1,9.0),(1,NULL)");
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT a.k,SUM(a.v),MIN(d.v),MAX(d.v) FROM AggTable a,ConstantDimension d
    WHERE a.k=d.k GROUP BY a.k ORDER BY a.k
  )sql"),
      std::vector<Row>({Row({Value(1), Value(60), Value(7.5), Value(9.0)}),
                        Row({Value(2), Value(40), Value(7.5), Value(7.5)})}));
}

TEST_F(RelationalRegressionTest, FusedConditionalAggregateKeepsNullElse) {
  RelationalRun(*rs_, "CREATE TABLE CaseFacts(k INT64, v DOUBLE)");
  RelationalRun(
      *rs_, "INSERT INTO CaseFacts VALUES (1,2.5),(1,NULL),(2,4.0),(3,NULL)");
  RelationalRun(*rs_, "ANALYZE CaseFacts");
  RelationalRun(*rs_, "ANALYZE SmallKeys");
  const std::string sql = R"sql(
    SELECT d.k, SUM(CASE WHEN d.k=1 THEN f.v ELSE NULL END),
      AVG(CASE WHEN d.k=1 THEN f.v END),
      COUNT(CASE WHEN d.k=1 THEN f.v END),
      SUM(CASE WHEN d.k=2 THEN f.v*(1.0-f.v) END)
    FROM CaseFacts f, SmallKeys d WHERE f.k=d.k GROUP BY d.k ORDER BY d.k
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value(1), Value(2.5), Value(2.5), Value(1), Value()}),
                 Row({Value(2), Value(), Value(), Value(0), Value(-12.0)}),
                 Row({Value(3), Value(), Value(), Value(0), Value()})}));
}

TEST_F(RelationalRegressionTest, GroupedJoinFusionKeepsGroupedJoinSemantics) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, customer,
      rs_->CreateTable(
          setup, Schema("FusedCustomer", {Column("ck", ValueType::kInt64),
                                          Column("cn", ValueType::kVarChar)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, orders,
      rs_->CreateTable(
          setup, Schema("FusedOrders", {Column("ok", ValueType::kInt64),
                                        Column("oc", ValueType::kInt64),
                                        Column("amt", ValueType::kInt64)})));
  // Customers 1 (2 orders), 2 (1 order), 3 (no orders), 4 (2 orders, one
  // filtered away only by a predicate that stays above the join).
  struct OrderRow {
    int64_t ok;
    int64_t oc;
    int64_t amt;
  };
  for (const OrderRow& order : {OrderRow{.ok = 10, .oc = 1, .amt = 100},
                                OrderRow{.ok = 11, .oc = 1, .amt = 50},
                                OrderRow{.ok = 12, .oc = 2, .amt = 70},
                                OrderRow{.ok = 13, .oc = 4, .amt = 20},
                                OrderRow{.ok = 14, .oc = 4, .amt = 80}}) {
    ASSERT_SUCCESS(
        orders
            .Insert(setup.txn_,
                    Row({Value(order.ok), Value(order.oc), Value(order.amt)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{1}), Value("a")}))
          .GetStatus());
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{2}), Value("b")}))
          .GetStatus());
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{3}), Value("c")}))
          .GetStatus());
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{4}), Value("d")}))
          .GetStatus());
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  // Q13 shape: LEFT JOIN grouped on the left key; the empty customer 3 must
  // survive with COUNT = 0 (COUNT(NULL) does not skip the group).
  const auto counts =
      RelationalRun(*rs_,
                    "SELECT ck, COUNT(ok) AS cnt FROM FusedCustomer "
                    "LEFT OUTER JOIN FusedOrders ON ck = oc GROUP BY ck "
                    "ORDER BY cnt DESC, ck ASC;");
  ASSERT_EQ(counts.size(), 4U);
  EXPECT_EQ(counts[0], (Row({Value(int64_t{1}), Value(int64_t{2})})));
  EXPECT_EQ(counts[1], (Row({Value(int64_t{4}), Value(int64_t{2})})));
  EXPECT_EQ(counts[2], (Row({Value(int64_t{2}), Value(int64_t{1})})));
  EXPECT_EQ(counts[3], (Row({Value(int64_t{3}), Value(int64_t{0})})));

  // Inner join + SUM + HAVING-free ORDER BY over an alias, matching the Q21
  // tail shape.
  const auto sums =
      RelationalRun(*rs_,
                    "SELECT cn, SUM(amt) AS total FROM FusedCustomer "
                    "JOIN FusedOrders ON ck = oc GROUP BY cn "
                    "ORDER BY total DESC, cn ASC;");
  ASSERT_EQ(sums.size(), 3U);
  EXPECT_EQ(sums[0], (Row({Value("a"), Value(int64_t{150})})));
  EXPECT_EQ(sums[1], (Row({Value("d"), Value(int64_t{100})})));
  EXPECT_EQ(sums[2], (Row({Value("b"), Value(int64_t{70})})));

  // LIMIT over the fused grouped output (TopN tail).
  const auto top_one =
      RelationalRun(*rs_,
                    "SELECT cn, SUM(amt) AS total FROM FusedCustomer "
                    "JOIN FusedOrders ON ck = oc GROUP BY cn "
                    "ORDER BY total DESC, cn ASC LIMIT 1;");
  ASSERT_EQ(top_one.size(), 1U);
  EXPECT_EQ(top_one[0], (Row({Value("a"), Value(int64_t{150})})));
}

TEST_F(RelationalRegressionTest,
       SmallGroupedJoinResetsExpressionScratchAfterAnError) {
  const std::string valid =
      "SELECT SUM(CASE WHEN a.k < 3 THEN a.v / 2 ELSE a.v END) "
      "FROM AggTable a JOIN SmallKeys s ON a.k = s.k";
  EXPECT_NE(ExplainAnalyze(*rs_, valid).find("grouped_join_fusions=1"),
            std::string::npos);
  // The small join has one probe team. Its CASE and floating arithmetic
  // reuse expression scratch after a failed evaluation on the same caller.
  for (int repetition = 0; repetition < 3; ++repetition) {
    const auto rows = RelationalRun(*rs_, valid);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0], Row({Value(60.0)}));
    auto context = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto prepared = engine.Prepare(
        context,
        "SELECT SUM(CASE WHEN a.k < 3 THEN a.v / (a.k - 1) ELSE a.v END) "
        "FROM AggTable a JOIN SmallKeys s ON a.k = s.k");
    if (prepared.HasValue()) {
      Row row;
      size_t emitted = 0;
      while (prepared.Value()->Next(&row, nullptr)) {
        ++emitted;
      }
      EXPECT_EQ(emitted, 0U);
      EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
      prepared.Value().reset();
    } else {
      EXPECT_NE(prepared.GetStatus(), Status::kSuccess);
    }
    context.Abort();
  }
  EXPECT_EQ(RelationalRun(*rs_, valid), std::vector<Row>({Row({Value(60.0)})}));
}

TEST_F(RelationalRegressionTest, FilteredAggregateOverJoinUsesPredicate) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left,
      rs_->CreateTable(
          setup, Schema("FilteredLeft", {Column("k", ValueType::kInt64),
                                         Column("v", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right,
      rs_->CreateTable(
          setup, Schema("FilteredRight", {Column("k", ValueType::kInt64)})));
  for (const Row& row : {Row({Value(int64_t{1}), Value(int64_t{10})}),
                         Row({Value(int64_t{1}), Value(int64_t{30})}),
                         Row({Value(int64_t{2}), Value(int64_t{40})})}) {
    ASSERT_SUCCESS(left.Insert(setup.txn_, row).GetStatus());
  }
  for (const int64_t key : {int64_t{1}, int64_t{2}}) {
    ASSERT_SUCCESS(right.Insert(setup.txn_, Row({Value(key)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  const auto countif =
      RelationalRun(*rs_,
                    "SELECT COUNTIF(t.v > 20) FROM FilteredLeft t "
                    "JOIN FilteredRight u ON t.k = u.k;");
  ASSERT_EQ(countif.size(), 1U);
  EXPECT_EQ(countif[0][0], Value(int64_t{2}));

  const auto countif_one =
      RelationalRun(*rs_,
                    "SELECT COUNTIF(t.v > 30) FROM FilteredLeft t "
                    "JOIN FilteredRight u ON t.k = u.k;");
  ASSERT_EQ(countif_one.size(), 1U);
  EXPECT_EQ(countif_one[0][0], Value(int64_t{1}));

  const auto filtered_count =
      RelationalRun(*rs_,
                    "SELECT COUNT(* WHERE t.v > 30) FROM FilteredLeft t "
                    "JOIN FilteredRight u ON t.k = u.k;");
  ASSERT_EQ(filtered_count.size(), 1U);
  EXPECT_EQ(filtered_count[0][0], Value(int64_t{1}));
}

// A correlated-subquery build whose scan was pruned by the outer
// correlation-key domain (ScopedCorrelatedOuter registration) must answer
// every outer key of THAT pipeline, and a later statement with a different
// outer domain must rebuild rather than reuse the pruned build.  NULL
// correlation keys on either side never match, so pruning them is
// observationally identical.
TEST_F(RelationalRegressionTest,
       CorrelatedDomainFilteredBuildsStaySoundAcrossOuterDomains) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(setup,
                       Schema("DomFact", {Column("fkey", ValueType::kInt64),
                                          Column("val", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, dim,
      rs_->CreateTable(setup,
                       Schema("DomDim", {Column("dkey", ValueType::kInt64),
                                         Column("flag", ValueType::kInt64)})));
  // Fact rows for keys 1..6 plus one NULL key; NULL never equals a probe.
  for (int64_t key = 1; key <= 6; ++key) {
    for (int64_t j = 0; j < 8; ++j) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_, Row({Value(key), Value((key * 10) + j)}))
              .GetStatus());
    }
  }
  ASSERT_SUCCESS(
      fact.Insert(setup.txn_, Row({Value(), Value(int64_t{99})})).GetStatus());
  for (int64_t key = 1; key <= 6; ++key) {
    ASSERT_SUCCESS(
        dim.Insert(setup.txn_, Row({Value(key), Value(key % 2)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  const auto avg_active =
      RelationalRun(*rs_,
                    "SELECT d.dkey, (SELECT AVG(f.val) FROM DomFact f "
                    "WHERE f.fkey = d.dkey) FROM DomDim d "
                    "WHERE d.flag = 1 ORDER BY 1;");
  ASSERT_EQ(avg_active.size(), 3U);
  EXPECT_EQ(avg_active[0][0], Value(int64_t{1}));
  EXPECT_EQ(avg_active[0][1], Value(13.5));
  EXPECT_EQ(avg_active[1][0], Value(int64_t{3}));
  EXPECT_EQ(avg_active[1][1], Value(33.5));
  EXPECT_EQ(avg_active[2][0], Value(int64_t{5}));
  EXPECT_EQ(avg_active[2][1], Value(53.5));

  // Same subquery shape, disjoint outer domain: keys 2/4/6.  A reused build
  // pruned to the {1, 3, 5} domain above would answer NULL/NULL here.
  const auto avg_inactive =
      RelationalRun(*rs_,
                    "SELECT d.dkey, (SELECT AVG(f.val) FROM DomFact f "
                    "WHERE f.fkey = d.dkey) FROM DomDim d "
                    "WHERE d.flag = 0 ORDER BY 1;");
  ASSERT_EQ(avg_inactive.size(), 3U);
  EXPECT_EQ(avg_inactive[0][0], Value(int64_t{2}));
  EXPECT_EQ(avg_inactive[0][1], Value(23.5));
  EXPECT_EQ(avg_inactive[1][0], Value(int64_t{4}));
  EXPECT_EQ(avg_inactive[1][1], Value(43.5));
  EXPECT_EQ(avg_inactive[2][0], Value(int64_t{6}));
  EXPECT_EQ(avg_inactive[2][1], Value(63.5));

  // Both domains in one pass over the same dim scan: the build (keyed once,
  // under whichever outer context triggered it) must still answer every key.
  const auto avg_all =
      RelationalRun(*rs_,
                    "SELECT d.dkey, (SELECT AVG(f.val) FROM DomFact f "
                    "WHERE f.fkey = d.dkey) FROM DomDim d ORDER BY 1;");
  ASSERT_EQ(avg_all.size(), 6U);
  for (int64_t key = 1; key <= 6; ++key) {
    EXPECT_EQ(avg_all[static_cast<size_t>(key) - 1][0], Value(key));
    EXPECT_EQ(avg_all[static_cast<size_t>(key) - 1][1],
              Value(static_cast<double>(key * 10) + 3.5));
  }
}

TEST(RelationalPlanningTest, NullRejectingPredicateClassification) {
  const Expression strict = BinaryExpressionExp(
      ColumnValueExp(ColumnName("r", "v")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));
  const Expression not_null = UnaryExpressionExp(
      ColumnValueExp(ColumnName("r", "v")), UnaryOperation::kIsNotNull);
  const Expression null_test = UnaryExpressionExp(
      ColumnValueExp(ColumnName("r", "v")), UnaryOperation::kIsNull);
  const Expression coalesced = BinaryExpressionExp(
      FunctionCallExp("coalesce", {ColumnValueExp(ColumnName("r", "v")),
                                   ConstantValueExp(Value(int64_t{0}))}),
      BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})));
  EXPECT_TRUE(relational_detail::IsNullRejectingPredicate(strict));
  EXPECT_TRUE(relational_detail::IsNullRejectingPredicate(not_null));
  EXPECT_TRUE(relational_detail::IsNullRejectingPredicate(
      BinaryExpressionExp(strict, BinaryOperation::kAnd, null_test)));
  EXPECT_FALSE(relational_detail::IsNullRejectingPredicate(null_test));
  EXPECT_FALSE(relational_detail::IsNullRejectingPredicate(coalesced));
}

TEST_F(RelationalRegressionTest,
       FusedDimensionTupleKeepsNullsAndWideProbeKeys) {
  RelationalRun(*rs_,
                "CREATE TABLE TupleDim(id INT64,label STRING,region STRING)");
  RelationalRun(*rs_,
                "INSERT INTO TupleDim VALUES "
                "(1,'same','r'),(2,'same','r'),(3,NULL,'r'),(3,NULL,'r')");
  RelationalRun(*rs_, "CREATE TABLE TupleFact(id INT64,p INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO TupleFact VALUES "
                "(1,0,10),(2,0,20),(3,0,4),(4,0,7),"
                "(1,NULL,2),(2,NULL,3),(1,9223372036854775807,6)");
  const auto rows = RelationalRun(*rs_, R"sql(
    SELECT f.p,d.label,d.region,SUM(f.v) FROM TupleFact f
    LEFT JOIN TupleDim d ON f.id=d.id GROUP BY f.p,d.label,d.region
    ORDER BY f.p NULLS FIRST,d.label NULLS FIRST,d.region NULLS FIRST
  )sql");
  EXPECT_EQ(rows, std::vector<Row>(
                      {Row({Value(), Value("same"), Value("r"), Value(5)}),
                       Row({Value(0), Value(), Value(), Value(7)}),
                       Row({Value(0), Value(), Value("r"), Value(8)}),
                       Row({Value(0), Value("same"), Value("r"), Value(30)}),
                       Row({Value(std::numeric_limits<int64_t>::max()),
                            Value("same"), Value("r"), Value(6)})}));
}

TEST_F(RelationalRegressionTest,
       CompactJoinedCountsKeepWorkerMergesAndFallback) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "label"), ValueType::kVarChar)});
  right.rows = {Row({Value(0), Value("a")}), Row({Value(0), Value("a")}),
                Row({Value(1), Value("b")}), Row({Value(2), Value()})};
  left.rows.reserve(32768);
  for (int64_t i = 0; i < 32768; ++i) {
    // A prime period keeps the capacity estimator's distributed sample
    // diverse while worker-local and cross-worker duplicate groups remain.
    left.rows.emplace_back(
        std::vector<Value>{Value(i % 3), Value(((i * 7919) % 8009) - 4000)});
  }
  const auto col = [](const char* owner, const char* name) {
    return ColumnValueExp(ColumnName(owner, name));
  };
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "k"), col("d", "label")};
  fusion.select_items = fusion.group_keys;
  fusion.select_items.push_back(count);
  fusion.select_names = {"p", "k", "label", "n"};
  fusion.aggregates = {count.get()};
  fusion.count_star = {true};
  fusion.eligible = true;
  fusion.post_having_agg_slot = 0;
  fusion.post_having_op = BinaryOperation::kGreaterThan;
  fusion.post_having_constant_or_factor = 4;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "k"), BinaryOperation::kEquals, col("d", "k"))};
  for (const bool nullable : {false, true}) {
    if (nullable) {
      left.rows.front().values_[1] = Value();
      left.rows.back().values_[1] = Value(std::numeric_limits<int64_t>::max());
    }
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      std::vector<Row> reference;
      for (const bool early : {false, true}) {
        fusion.post_having_active = early;
        fusion.post_having_applied = false;
        auto result = relational_detail::TryFusedGroupedJoin(
            context, left, right, predicates, JoinType::kInner, nullptr, {},
            &fusion);
        ASSERT_TRUE(result.HasValue()) << result.GetStatus();
        auto output = result.MoveValue();
        if (!output) {
          FAIL() << "counted grouped join declined";
        }
        if (!early) {
          for (const auto& row : output->rows) {
            if (row[3].value.int_value > 4) {
              reference.push_back(row);
            }
          }
        } else {
          EXPECT_TRUE(fusion.post_having_applied);
          EXPECT_EQ(output->rows, reference);
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, PresenceGroupingDeduplicatesWholeBuildRows) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                              Column(ColumnName("d", "v"), ValueType::kInt64)});
  left.rows = {Row({Value(1), Value(10)}), Row({Value(2), Value(20)}),
               Row({Value(3), Value()}), Row({Value(4), Value(1)})};
  for (size_t i = 0; i < 1024; ++i) {
    for (const auto& row :
         {Row({Value(1), Value(10)}), Row({Value(1), Value(11)}),
          Row({Value(2), Value(20)}), Row({Value(3), Value(30)}),
          Row({Value(4), Value()})}) {
      right.rows.push_back(row);
    }
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "k"), col("d", "v")};
  fusion.select_items = fusion.group_keys;
  fusion.select_names = {"k", "v"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{
      BinaryExpressionExp(col("f", "k"), BinaryOperation::kEquals,
                          col("d", "k")),
      BinaryExpressionExp(col("f", "v"), BinaryOperation::kNotEquals,
                          col("d", "v"))};
  for (const auto join : {JoinType::kInner, JoinType::kLeft}) {
    auto output = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, join, nullptr, {}, &fusion);
    ASSERT_TRUE(output.HasValue()) << output.GetStatus();
    auto relation = output.MoveValue();
    if (!relation) {
      FAIL() << "presence-only grouped join declined";
    }
    EXPECT_EQ(relation->rows.size(), join == JoinType::kInner ? 1U : 4U);
    EXPECT_EQ(std::ranges::count(relation->rows, Row({Value(1), Value(11)})),
              1);
    EXPECT_LT(relation->join_comparisons, 100U);
    if (join == JoinType::kLeft) {
      for (const int key : {2, 3, 4}) {
        EXPECT_EQ(
            std::ranges::count(relation->rows, Row({Value(key), Value()})), 1);
      }
    }
  }
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  fusion.aggregates = {count.get()};
  fusion.count_star = {true};
  fusion.select_items.emplace_back(count);
  fusion.select_names.emplace_back("n");
  auto counted = relational_detail::TryFusedGroupedJoin(
      context, left, right, predicates, JoinType::kInner, nullptr, {}, &fusion);
  ASSERT_TRUE(counted.HasValue()) << counted.GetStatus();
  auto counted_rows = counted.MoveValue();
  if (!counted_rows) {
    FAIL() << "counted grouped join declined";
  }
  EXPECT_EQ(counted_rows->rows,
            std::vector<Row>({Row({Value(1), Value(11), Value(1024)})}));
}

TEST_F(RelationalRegressionTest, FusedBuildCaseCacheKeepsNullAndCollation) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "tag"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("yes")}), Row({Value(2), Value("no")}),
                Row({Value(3), Value()}),
                Row({Value(4), Value("YES").WithCollation(2)})};
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto child = CaseExpressionExp(
      {{BinaryExpressionExp(col("d", "tag"), BinaryOperation::kEquals,
                            ConstantValueExp(Value("yes"))),
        col("f", "v")}},
      ConstantValueExp(Value()));
  const auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, child);
  const auto avg =
      std::make_shared<AggregateExpression>(AggregationType::kAvg, child);
  const auto count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, child);
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.aggregates = {sum.get(), avg.get(), count.get()};
  fusion.count_star = {false, false, false};
  fusion.select_names = {"k", "sum", "avg", "count"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "k"), BinaryOperation::kEquals, col("d", "k"))};
  for (const int64_t repeats : {1, 1024}) {
    left.rows.clear();
    for (int64_t i = 0; i < repeats; ++i) {
      for (int key = 1; key <= 5; ++key) {
        left.rows.emplace_back(std::vector<Value>{Value(key), Value(2.5)});
        left.rows.emplace_back(std::vector<Value>{Value(key), Value()});
      }
    }
    for (const auto* alias : {"f", "d"}) {
      fusion.group_keys = {col(alias, "k")};
      fusion.select_items = {fusion.group_keys[0], sum, avg, count};
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kLeft, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      if (!output) {
        FAIL() << "conditional grouped join declined";
      }
      ASSERT_EQ(output->rows.size(), 5U);
      for (int key = 1; key <= 5; ++key) {
        const bool accepted = key == 1 || key == 4;
        const Row expected(
            {key == 5 && std::string_view(alias) == "d" ? Value() : Value(key),
             accepted ? Value(2.5 * static_cast<double>(repeats)) : Value(),
             accepted ? Value(2.5) : Value(),
             Value(accepted ? repeats : int64_t{0})});
        EXPECT_EQ(std::ranges::count(output->rows, expected), 1);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, BorrowedJoinKeysKeepBytesBiasAndDuplicates) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  const std::string binary("a\0b", 3);
  const std::string long_text(96, 'x');
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const std::vector<Expression> predicates{
      BinaryExpressionExp(col("a", "s"), BinaryOperation::kEquals,
                          col("b", "s")),
      BinaryExpressionExp(
          col("a", "k"), BinaryOperation::kEquals,
          BinaryExpressionExp(col("b", "k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1))))};
  for (const auto& [repeats, with_residual] :
       {std::pair{1, false}, std::pair{8192, false}, std::pair{8192, true}}) {
    relational_detail::Relation left, right;
    left.schema =
        Schema("a", {Column(ColumnName("a", "id"), ValueType::kInt64),
                     Column(ColumnName("a", "s"), ValueType::kVarChar),
                     Column(ColumnName("a", "k"), ValueType::kInt64)});
    right.schema =
        Schema("b", {Column(ColumnName("b", "id"), ValueType::kInt64),
                     Column(ColumnName("b", "s"), ValueType::kVarChar),
                     Column(ColumnName("b", "k"), ValueType::kInt64)});
    const std::vector<Row> pattern{
        Row({Value(1), Value(std::string(long_text)), Value(2)}),
        Row({Value(2), Value(std::string(long_text)), Value(2)}),
        Row({Value(3), Value(""), Value(0)}),
        Row({Value(4), Value(std::string(binary)), Value(-3)}),
        Row({Value(5), Value(), Value(2)}),
        Row({Value(6), Value("other"), Value()})};
    for (int i = 0; i < repeats; ++i) {
      left.rows.insert(left.rows.end(), pattern.begin(), pattern.end());
    }
    right.rows = {Row({Value(10), Value(std::string(long_text)), Value(1)}),
                  Row({Value(11), Value(std::string(long_text)), Value(1)}),
                  Row({Value(12), Value(""), Value(-1)}),
                  Row({Value(13), Value(std::string(binary)), Value(-4)}),
                  Row({Value(14), Value(), Value(1)}),
                  Row({Value(15), Value(""), Value()}),
                  Row({Value(16), Value("a"), Value(-4)})};
    auto active_predicates = predicates;
    if (with_residual) {
      active_predicates.push_back(
          BinaryExpressionExp(col("a", "id"), BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(1))));
    }
    auto result =
        relational_detail::InnerJoin(context, std::move(left), std::move(right),
                                     active_predicates, nullptr, {});
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    std::map<std::pair<int64_t, int64_t>, int> counts;
    for (const auto& row : result.Value().rows) {
      ++counts[{row[0].value.int_value, row[3].value.int_value}];
    }
    std::map<std::pair<int64_t, int64_t>, int> expected{
        {{1, 10}, repeats}, {{1, 11}, repeats}, {{2, 10}, repeats},
        {{2, 11}, repeats}, {{3, 12}, repeats}, {{4, 13}, repeats}};
    if (with_residual) {
      expected.erase({1, 10});
      expected.erase({1, 11});
    }
    EXPECT_EQ(counts, expected);
  }
}

TEST_F(RelationalRegressionTest, PackedOuterKeysKeepRangesNullsAndDuplicates) {
  constexpr int kGroups = 1024;
  constexpr int kRows = 20480;
  const auto maximum = std::numeric_limits<int64_t>::max();
  const auto minimum = std::numeric_limits<int64_t>::min();
  auto context = rs_->BeginContext();
  for (bool wide : {false, true}) {
    for (size_t workers : {1U, 2U}) {
      for (JoinType kind : {JoinType::kLeft, JoinType::kFull}) {
        relational_detail::Relation left, right;
        left.schema = Schema("l", {Column("ld", ValueType::kDate),
                                   Column("la", ValueType::kInt64),
                                   Column("lc", ValueType::kInt64),
                                   Column("lid", ValueType::kInt64)});
        right.schema = Schema("r", {Column("ra", ValueType::kInt64),
                                    Column("rd", ValueType::kDate),
                                    Column("rc", ValueType::kInt64),
                                    Column("rid", ValueType::kInt64)});
        for (int g = 0; g < kGroups; ++g) {
          for (int duplicate = 0; duplicate < 2; ++duplicate) {
            right.AddRow(
                Row({Value(g - 500), Value::DateFromDays(g % 3),
                     Value(maximum - (g % 5)), Value((g * 2) + duplicate)}));
          }
        }
        // One unmatched row expands a single field to 64 bits in the wide
        // run, exercising the ordinary composite-key fallback.
        const Row extra({Value(50000), Value::DateFromDays(0),
                         Value(wide ? minimum : maximum), Value(-1)});
        const Row null_right(
            {Value(), Value::DateFromDays(0), Value(maximum), Value(-2)});
        right.AddRow(extra);
        right.AddRow(null_right);
        std::map<std::string, size_t> expected;
        for (int i = 0; i < kRows; ++i) {
          const int g = i % 1200;
          const bool null_key = i % 17 == 0;
          const bool outside = i % 13 == 0;
          const Row row(
              {Value::DateFromDays(g % 3), null_key ? Value() : Value(g - 501),
               Value(outside ? minimum : maximum - (g % 5)), Value(i)});
          left.AddRow(row);
          if (!null_key && !outside && g < kGroups) {
            for (int duplicate = 0; duplicate < 2; ++duplicate) {
              ++expected[(row + right.rows[(static_cast<size_t>(g) * 2) +
                                           static_cast<size_t>(duplicate)])
                             .ToString()];
            }
          } else {
            auto unmatched = row;
            unmatched.values_.resize(8);
            ++expected[unmatched.ToString()];
          }
        }
        if (kind == JoinType::kFull) {
          ++expected[(Row(std::vector<Value>(4)) + extra).ToString()];
          ++expected[(Row(std::vector<Value>(4)) + null_right).ToString()];
        }
        const auto equality = [](Expression l, const char* r) {
          return BinaryExpressionExp(std::move(l), BinaryOperation::kEquals,
                                     ColumnValueExp(r));
        };
        SelectSource source;
        source.join_type = kind;
        source.join_condition = BinaryExpressionExp(
            equality(
                BinaryExpressionExp(ColumnValueExp("la"), BinaryOperation::kAdd,
                                    ConstantValueExp(Value(1))),
                "ra"),
            BinaryOperation::kAnd,
            BinaryExpressionExp(equality(ColumnValueExp("ld"), "rd"),
                                BinaryOperation::kAnd,
                                equality(ColumnValueExp("lc"), "rc")));
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, result,
            relational_detail::Join(context, std::move(left), std::move(right),
                                    source, nullptr, {}));
        std::map<std::string, size_t> actual;
        ASSERT_SUCCESS(result.ForEachRow(
            [&](const Row& row) { ++actual[row.ToString()]; }));
        EXPECT_EQ(actual, expected);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, ParallelJoinConsumesOnlyLastMatchBuffer) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  const std::string text(96, 'p');
  const Value array = Value::Array({Value(3), Value(), Value("nested")}, "ANY");
  for (const auto kind : {JoinType::kInner, JoinType::kLeft, JoinType::kFull}) {
    for (const bool flip : {false, true}) {
      SCOPED_TRACE(static_cast<int>(kind));
      SCOPED_TRACE(flip);
      relational_detail::Relation left, right;
      left.schema = Schema("a", {Column("ak", ValueType::kInt64),
                                 Column("at", ValueType::kVarChar),
                                 Column("av", ValueType::kArray)});
      right.schema = Schema("b", {Column("bk", ValueType::kInt64),
                                  Column("bid", ValueType::kInt64)});
      constexpr int kRepeats = 4096;
      for (int i = 0; i < kRepeats; ++i) {
        for (int k = 0; k < 5; ++k) {
          Row row(
              {k == 0 ? Value() : Value(k), Value(std::string(text)), array});
          // Exercise both reuse with sufficient capacity and reallocation.
          if (i % 2 == 0) {
            row.values_.reserve(8);
          }
          left.AddRow(std::move(row));
        }
      }
      right.rows = {Row({Value(1), Value(10)}), Row({Value(1), Value(11)}),
                    Row({Value(2), Value(12)}), Row({Value(6), Value(16)}),
                    Row({Value(), Value(19)})};
      if (flip && kind == JoinType::kInner) {
        std::swap(left, right);
      }
      SelectSource source;
      source.join_type = kind;
      source.join_condition = BinaryExpressionExp(
          ColumnValueExp("ak"), BinaryOperation::kEquals, ColumnValueExp("bk"));
      auto result =
          kind == JoinType::kInner
              ? relational_detail::InnerJoin(
                    context, std::move(left), std::move(right),
                    {source.join_condition}, nullptr, {})
              : relational_detail::Join(context, std::move(left),
                                        std::move(right), source, nullptr, {});
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      const size_t a = flip && kind == JoinType::kInner ? 2 : 0;
      const size_t b = flip && kind == JoinType::kInner ? 0 : 3;
      std::map<std::pair<int64_t, int64_t>, int> counts;
      for (const auto& row : result.Value().rows) {
        const int64_t key = row[a].IsNull() ? -1 : row[a].value.int_value;
        const int64_t id =
            row[b + 1].IsNull() ? -1 : row[b + 1].value.int_value;
        ++counts[{key, id}];
        if (id != 16 && id != 19) {
          EXPECT_EQ(row[a + 1], Value(std::string(text)));
          EXPECT_EQ(row[a + 2], array);
        } else {
          EXPECT_TRUE(row[a + 1].IsNull());
          EXPECT_TRUE(row[a + 2].IsNull());
        }
      }
      std::map<std::pair<int64_t, int64_t>, int> expected{
          {{1, 10}, kRepeats}, {{1, 11}, kRepeats}, {{2, 12}, kRepeats}};
      if (kind != JoinType::kInner) {
        expected[{3, -1}] = kRepeats;
        expected[{4, -1}] = kRepeats;
        expected[{-1, -1}] = kRepeats;
      }
      if (kind == JoinType::kFull) {
        expected[{-1, 16}] = 1;
        expected[{-1, 19}] = 1;
      }
      EXPECT_EQ(counts, expected);
    }
  }
}

TEST_F(RelationalRegressionTest, BorrowedJoinKeysDoNotHideBiasedOverflow) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("a", {Column(ColumnName("a", "s"), ValueType::kVarChar),
                             Column(ColumnName("a", "k"), ValueType::kInt64)});
  right.schema = Schema("b", {Column(ColumnName("b", "s"), ValueType::kVarChar),
                              Column(ColumnName("b", "k"), ValueType::kInt64)});
  left.rows = {
      Row({Value("same"), Value(std::numeric_limits<int64_t>::min())})};
  right.rows = {
      Row({Value("same"), Value(std::numeric_limits<int64_t>::max())})};
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const std::vector<Expression> predicates{
      BinaryExpressionExp(col("a", "s"), BinaryOperation::kEquals,
                          col("b", "s")),
      BinaryExpressionExp(
          col("a", "k"), BinaryOperation::kEquals,
          BinaryExpressionExp(col("b", "k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1))))};
  auto result = relational_detail::InnerJoin(
      context, std::move(left), std::move(right), predicates, nullptr, {});
  EXPECT_FALSE(result.HasValue());
}

TEST_F(RelationalRegressionTest, UnobservedLeftBuildNeedsActualUniqueness) {
  RelationalRun(*rs_,
                "CREATE TABLE ElideFact (a INT64,b INT64,g INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE ElideDim (a INT64,b INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("ElideFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("ElideDim"));
    for (int i = 0; i < 2048; ++i) {
      ASSERT_SUCCESS(dim->Insert(setup.txn_, Row({Value(i / 4), Value(i % 4)}))
                         .GetStatus());
    }
    for (int i = 0; i < 8192; ++i) {
      const int key = i % 2500;
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_,
                       Row({i % 17 == 0 ? Value() : Value(key / 4),
                            Value(key % 4), Value(i % 31),
                            i % 5 == 0 ? Value() : Value((i % 7) - 3)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  for (bool duplicates : {false, true}) {
    if (duplicates) {
      RelationalRun(*rs_,
                    "INSERT INTO ElideDim VALUES (0,3),(NULL,0),(NULL,0)");
    }
    std::vector<int64_t> sums(31), counts(31), matches(31);
    for (int i = 0; i < 8192; ++i) {
      const int key = i % 2500;
      const auto group = static_cast<size_t>(i % 31);
      const bool matched = key < 2048 && i % 17 != 0;
      const int64_t copies = matched && duplicates && key == 3 ? 2 : 1;
      counts[group] += copies;
      matches[group] += matched ? copies : 0;
      if (i % 5 != 0) {
        sums[group] += copies * ((i % 7) - 3);
      }
    }
    for (size_t workers : {1U, 2U}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      for (bool observed : {false, true}) {
        const std::string sql =
            "SELECT f.g,SUM(f.v),COUNT(" + std::string(observed ? "d.a" : "*") +
            ") FROM ElideFact f LEFT JOIN ElideDim d ON f.a=d.a AND f.b=d.b "
            "GROUP BY f.g ORDER BY f.g";
        const auto rows = RelationalRun(*rs_, sql);
        ASSERT_EQ(rows.size(), 31U);
        for (size_t g = 0; g < rows.size(); ++g) {
          EXPECT_EQ(rows[g], Row({Value(g), Value(sums[g]),
                                  Value(observed ? matches[g] : counts[g])}));
        }
      }
    }
  }
}

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
  context.Abort();
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
  context.Abort();
}

TEST_F(RelationalRegressionTest, SmallInnerBuildStreamsEverySpilledProbeRow) {
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  for (int shape = 0; shape < 3; ++shape) {
    SCOPED_TRACE(shape);  // integer / composite integer / byte key
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
    auto joined = relational_detail::InnerJoin(
        context, std::move(left), std::move(right), predicates, nullptr, {});
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
    context.Abort();
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
      const Value text = i >= 1000 && i % 5 == 0
                             ? Value()
                             : Value(std::string(96, 'a' + (i % 20)));
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
    context.Abort();
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

TEST_F(RelationalRegressionTest, CorrelatedLocalFilterAvoidsBuildSpill) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(
          setup, Schema("LocalFact", {Column("fk", ValueType::kInt64),
                                      Column("flag", ValueType::kInt64)})));
  for (int64_t i = 0; i < 3000; ++i) {
    ASSERT_SUCCESS(
        fact.Insert(setup.txn_, Row({i % 101 == 0 ? Value() : Value(i % 10),
                                     Value(i % 100)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  const std::string sql =
      "SELECT COUNT(*) FROM KeyTable a WHERE EXISTS "
      "(SELECT 1 FROM LocalFact f WHERE f.fk = a.k AND f.flag = 1)";
  EXPECT_EQ(RelationalRun(*rs_, sql), std::vector<Row>({Row({Value(1)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("relation_spills=0"), std::string::npos) << profile;
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k FROM KeyTable a WHERE NOT EXISTS "
                          "(SELECT 1 FROM LocalFact f WHERE f.fk = a.k AND "
                          "f.flag = NULL) ORDER BY k")
                .size(),
            10U);
}

TEST_F(RelationalRegressionTest, SelectiveExistsBuildsOneTwoValuedKeySet) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, parent,
      rs_->CreateTable(
          setup, Schema("SelectiveParent", {Column("k", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, child,
      rs_->CreateTable(setup, Schema("SelectiveChild",
                                     {Column("k", ValueType::kInt64),
                                      Column("flag", ValueType::kInt64)})));
  int64_t expected = 0;
  for (int i = 0; i < 1000; ++i) {
    const Value key = i % 101 == 0 ? Value() : Value(i % 100);
    expected += static_cast<int64_t>(!key.IsNull() && key == Value(7));
    ASSERT_SUCCESS(parent.Insert(setup.txn_, Row({key})).GetStatus());
  }
  for (int i = 0; i < 2000; ++i) {
    ASSERT_SUCCESS(
        child.Insert(setup.txn_, Row({Value(i % 50), Value(i % 100)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(
      child.Insert(setup.txn_, Row({Value(), Value(7)})).GetStatus());
  ASSERT_SUCCESS(rs_->RefreshStatistics(setup, "SelectiveParent"));
  ASSERT_SUCCESS(rs_->RefreshStatistics(setup, "SelectiveChild"));
  ASSERT_SUCCESS(setup.PreCommit());
  const std::string body =
      "(SELECT 1 FROM SelectiveChild c WHERE c.k = p.k AND c.flag = 7)";
  const std::string sql =
      "SELECT COUNT(*) FROM SelectiveParent p WHERE EXISTS " + body;
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(expected)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_probes=0"), std::string::npos)
      << profile;
  // Inner NULLs cannot poison a NOT EXISTS miss; outer NULLs survive it.
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM SelectiveParent p "
                          "WHERE NOT EXISTS " +
                              body),
            std::vector<Row>({Row({Value(1000 - expected)})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM SelectiveParent p "
                          "WHERE NOT EXISTS (SELECT 1 FROM SelectiveChild c "
                          "WHERE c.k = p.k AND c.flag = NULL)"),
            std::vector<Row>({Row({Value(1000)})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM SelectiveParent p "
                          "WHERE EXISTS (SELECT 1 FROM SelectiveChild c "
                          "WHERE c.k = p.k AND c.flag = 7 LIMIT 0)"),
            std::vector<Row>({Row({Value(0)})}));
}

void BuildIndexedExistsTables(Database& database) {
  auto setup = database.BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, parent,
      database.CreateTable(
          setup, Schema("IndexedParent", {Column("k", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, child,
      database.CreateTable(
          setup,
          Schema("IndexedChild", {Column("k", ValueType::kInt64),
                                  Column("flag", ValueType::kInt64),
                                  Column("payload", ValueType::kInt64)})));
  for (int i = 0; i < 1000; ++i) {
    ASSERT_SUCCESS(
        parent.Insert(setup.txn_, Row({i == 999 ? Value() : Value(i)}))
            .GetStatus());
  }
  for (int i = 0; i < 6000; ++i) {
    ASSERT_SUCCESS(child
                       .Insert(setup.txn_, Row({Value(i % 500), Value(i % 100),
                                                Value(i % 13 == 0 ? -1 : 1)}))
                       .GetStatus());
  }
  ASSERT_SUCCESS(
      child.Insert(setup.txn_, Row({Value(), Value(7), Value(1)})).GetStatus());
  ASSERT_SUCCESS(
      child.Insert(setup.txn_, Row({Value(998), Value(7), Value(-1)}))
          .GetStatus());
  // A composite prefix whose payload does not cover the correlation key.
  ASSERT_SUCCESS(database.CreateIndex(
      setup, "IndexedChild",
      IndexSchema("flag_payload", {1, 2}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(database.RefreshStatistics(setup, "IndexedParent"));
  ASSERT_SUCCESS(database.RefreshStatistics(setup, "IndexedChild"));
  ASSERT_SUCCESS(setup.PreCommit());
}

constexpr const char* kIndexedExistsSql =
    "SELECT COUNT(*) FROM IndexedParent p WHERE EXISTS "
    "(SELECT 1 FROM IndexedChild c WHERE c.k = p.k AND c.flag = 7 "
    "AND c.payload > 0)";

TEST_F(RelationalRegressionTest,
       IndexedExistsPrefixRechecksRowsAndKeepsNullSemantics) {
  BuildIndexedExistsTables(*rs_);
  const size_t used = QueryMemoryBudget::Global().Used();
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(5)})}));
  const auto profile = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(profile.find("prebuilt_exists_index_builds=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("prebuilt_exists_index_rows=62,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("prebuilt_exists_keys=5,"), std::string::npos)
      << profile;
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*) FROM IndexedParent p WHERE NOT EXISTS "
                    "(SELECT NULL FROM IndexedChild c WHERE p.k = c.k "
                    "AND c.flag = 7 AND c.payload > 0)"),
      std::vector<Row>({Row({Value(995)})}));
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*) FROM IndexedParent p WHERE NOT EXISTS "
                    "(SELECT 1 FROM IndexedChild c WHERE c.k = p.k "
                    "AND c.flag = NULL)"),
      std::vector<Row>({Row({Value(1000)})}));
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), used);
}

TEST_F(RelationalRegressionTest,
       IndexedExistsKeepsSmallBuildDespiteUnderestimatedStatistics) {
  BuildIndexedExistsTables(*rs_);
  auto write = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, child,
                        write.GetTable("IndexedChild"));
  for (int i = 600; i < 695; ++i) {
    ASSERT_SUCCESS(
        child->Insert(write.txn_, Row({Value(i), Value(7), Value(1)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(write.PreCommit());
  // Old stats predict fewer member rows than the 100 distinct keys now
  // present. Capacity slack should absorb this small error without a rescan.
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(100)})}));
  const auto profile = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(profile.find("prebuilt_exists_index_builds=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("prebuilt_exists_keys=100,"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest,
       IndexedExistsFallsBackForStaleStatisticsAndMemoryPressure) {
  BuildIndexedExistsTables(*rs_);
  // Add more distinct matching keys than the old ANALYZE estimate can hold.
  auto write = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, child,
                        write.GetTable("IndexedChild"));
  for (int i = 600; i < 800; ++i) {
    ASSERT_SUCCESS(
        child->Insert(write.txn_, Row({Value(i), Value(7), Value(1)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(write.PreCommit());
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(205)})}));
  const auto stale = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(stale.find("prebuilt_exists_index_builds=0,"), std::string::npos)
      << stale;
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  // Even the smallest charged set cannot fit. The ordinary route must still
  // return the complete answer while its outer relation spills.
  QueryMemoryBudget::Global().ResetForTest(128);
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(205)})}));
  const auto small = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(small.find("prebuilt_exists_index_builds=0,"), std::string::npos)
      << small;
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), 0U);
}

TEST_F(RelationalRegressionTest,
       IndexedExistsUsesSnapshotAfterInnerIndexKeyUpdate) {
  BuildIndexedExistsTables(*rs_);
  auto old = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto read_old = [&]() {
    std::vector<Row> rows;
    auto prepared = engine.Prepare(old, kIndexedExistsSql);
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
    return rows;
  };
  EXPECT_EQ(read_old(), std::vector<Row>({Row({Value(5)})}));
  RelationalRun(*rs_, "UPDATE IndexedChild SET flag = 8 WHERE k = 7");
  // The index has removed old flag=7 entries, but this snapshot still sees
  // their old heap versions. Serving the new index would silently lose a key.
  EXPECT_EQ(read_old(), std::vector<Row>({Row({Value(5)})}));
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(4)})}));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest, SpilledProbeFeedsGroupedJoinDirectly) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(setup,
                       Schema("SpillFact", {Column("fk", ValueType::kInt64),
                                            Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 3000; ++i) {
    ASSERT_SUCCESS(
        fact.Insert(setup.txn_,
                    Row({i % 10 == 9 ? Value() : Value(i % 10), Value(2)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  const std::string sql =
      "SELECT a.k, SUM(f.v), COUNT(*) FROM SpillFact f "
      "JOIN KeyTable a ON f.fk = a.k GROUP BY a.k ORDER BY a.k";
  auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 9U);
  for (int64_t i = 0; i < 9; ++i) {
    EXPECT_EQ(rows[i], Row({Value(i), Value(600), Value(300)}));
  }
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("grouped_join_fusions=1"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, LeftNullKeyFilterKeepsOnlyUnmatchedRows) {
  RelationalRun(*rs_, "CREATE TABLE AntiFact (a INT64,b INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE AntiDim (a INT64,b INT64,p INT64)");
  RelationalRun(*rs_, R"sql(
    INSERT INTO AntiFact VALUES (1,10,1),(1,10,2),(2,10,3),(NULL,10,4),
                               (3,20,5),(1,NULL,6),(4,40,7),(2,10,8)
  )sql");
  RelationalRun(*rs_, R"sql(
    INSERT INTO AntiDim VALUES (1,10,NULL),(1,10,2),(2,NULL,9),
                              (NULL,10,9),(3,20,NULL)
  )sql");
  const std::string join =
      " FROM AntiFact f LEFT JOIN AntiDim d "
      "ON f.a=d.a AND f.b=d.b";
  std::vector<Row> expected;
  for (const int v : {3, 4, 6, 7, 8}) {
    expected.emplace_back(Row({Value(v), Value()}));
  }
  EXPECT_EQ(RelationalRun(*rs_, "SELECT f.v,d.p" + join +
                                    " WHERE d.a IS NULL ORDER BY f.v"),
            expected);
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT f.v,z.k" + join +
              " FULL JOIN (SELECT 1 AS k UNION ALL SELECT 2) z ON f.v=z.k "
              "WHERE d.a IS NULL ORDER BY f.v NULLS LAST,z.k"),
      expected);
  // NULL payloads can occur in matching rows; they do not prove absence.
  EXPECT_EQ(RelationalRun(*rs_, "SELECT f.v,d.p" + join +
                                    " WHERE d.p IS NULL ORDER BY f.v")
                .size(),
            8U);
  // A failed ON residual makes even an equal-key row unmatched.
  EXPECT_EQ(RelationalRun(*rs_, "SELECT f.v,d.p" + join +
                                    " AND d.p>2 WHERE d.a IS NULL ORDER BY f.v")
                .size(),
            8U);
  // NULL-safe equality may match a NULL key, whose payload must survive.
  const auto null_safe = RelationalRun(*rs_, R"sql(
    SELECT f.v,d.p FROM AntiFact f LEFT JOIN AntiDim d
    ON f.a IS NOT DISTINCT FROM d.a AND f.b=d.b
    WHERE d.a IS NULL ORDER BY f.v
  )sql");
  expected[1] = Row({Value(4), Value(9)});
  EXPECT_EQ(null_safe, expected);
}

TEST_F(RelationalRegressionTest, LeftOnLocalFilterKeepsNullExtensionAndWhere) {
  RelationalRun(*rs_, "CREATE TABLE OnRight (fk INT64, flag INT64)");
  RelationalRun(*rs_,
                "INSERT INTO OnRight VALUES (1, 1), (1, 1), (2, 0), (NULL, 1)");
  const auto rows = RelationalRun(
      *rs_,
      "SELECT a.k, COUNT(r.fk), COUNT(*) FROM SmallKeys a LEFT JOIN OnRight r "
      "ON a.k = r.fk AND r.flag = 1 GROUP BY a.k ORDER BY a.k");
  EXPECT_EQ(rows, std::vector<Row>({Row({Value(1), Value(2), Value(2)}),
                                    Row({Value(2), Value(0), Value(1)}),
                                    Row({Value(3), Value(0), Value(1)}),
                                    Row({Value(4), Value(0), Value(1)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT a.k FROM SmallKeys a LEFT JOIN OnRight r "
          "ON a.k = r.fk AND r.flag = 1 WHERE r.fk IS NULL ORDER BY a.k"),
      std::vector<Row>({Row({Value(2)}), Row({Value(3)}), Row({Value(4)})}));
}

TEST_F(RelationalRegressionTest, CorrelatedBuildKeepsItsSelectiveDomain) {
  RelationalRun(*rs_,
                "CREATE TABLE CorrFact (part INT64, supplier INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrSupplier (id INT64, flag INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrOuter (id INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("CorrFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, supplier,
                          setup.GetTable("CorrSupplier"));
    for (int64_t p = 0; p < 1000; ++p) {
      for (int64_t s = 0; s < 16; ++s) {
        ASSERT_SUCCESS(fact->Insert(setup.txn_, Row({Value(p), Value(s),
                                                     Value((p * 10) + s)}))
                           .GetStatus());
      }
    }
    for (int64_t s = 0; s < 16; ++s) {
      ASSERT_SUCCESS(supplier->Insert(setup.txn_, Row({Value(s), Value(s % 2)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "INSERT INTO CorrOuter VALUES (2),(503),(999),(NULL)");
  RelationalRun(*rs_, "CREATE INDEX corr_part ON CorrFact(part)");
  for (const auto* table : {"CorrFact", "CorrSupplier", "CorrOuter"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + table);
  }
  const std::string sql =
      "SELECT d.id FROM CorrOuter d WHERE d.id*10+1 = "
      "(SELECT MIN(f.v) FROM CorrFact f JOIN CorrSupplier s ON f.supplier=s.id "
      "WHERE f.part=d.id AND s.flag=1) ORDER BY d.id";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value(2)}), Row({Value(503)}), Row({Value(999)})}));
  std::string profile;
  for (const auto& row : RelationalRun(*rs_, "EXPLAIN ANALYZE " + sql)) {
    profile += row[0].AsString();
  }
  EXPECT_NE(profile.find("selective_index_scans=1"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, ParallelSourceRetainsAllCompiledFilterKinds) {
  RelationalRun(*rs_,
                "CREATE TABLE FilterFact (k INT64, label STRING, v INT64, "
                "padding STRING)");
  RelationalRun(*rs_, "CREATE TABLE FilterDim (k INT64, bucket INT64)");
  RelationalRun(
      *rs_,
      "CREATE TABLE FilterBlocked (k INT64, threshold INT64, reason STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("FilterFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("FilterDim"));
    for (int64_t i = 0; i < 32768; ++i) {
      const int64_t key = i % 16;
      ASSERT_SUCCESS(
          fact->Insert(
                  setup.txn_,
                  Row({Value(key),
                       key == 15 ? Value()
                                 : Value(key % 2 == 0 ? "ABvalue" : "ZZvalue"),
                       Value(i % 4), Value(std::string(100, 'x'))}))
              .GetStatus());
    }
    for (int64_t k = 0; k < 16; ++k) {
      ASSERT_SUCCESS(
          dim->Insert(setup.txn_, Row({Value(k), Value(k % 2)})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_,
                "INSERT INTO FilterBlocked VALUES (1,5,'Customer abc "
                "Complaints'),(3,NULL,'Customer xyz Complaints'),(4,2,'good')");
  RelationalRun(*rs_, "ANALYZE FilterFact");
  RelationalRun(*rs_, "ANALYZE FilterDim");
  RelationalRun(*rs_, "ANALYZE FilterBlocked");
  const std::string from =
      " FROM FilterFact f JOIN FilterDim d ON f.k=d.k WHERE ";
  const std::string members =
      "(SELECT k FROM FilterBlocked WHERE reason LIKE '%Customer%Complaints%')";
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*)" + from + "f.k IN " + members),
            std::vector<Row>({Row({Value(4096)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT COUNT(*)" + from + "f.k NOT IN " + members),
      std::vector<Row>({Row({Value(28672)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT d.bucket, COUNT(DISTINCT f.k)" + from +
                              "f.k NOT IN " + members +
                              " GROUP BY d.bucket ORDER BY d.bucket"),
      std::vector<Row>({Row({Value(0), Value(8)}), Row({Value(1), Value(6)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT d.bucket, COUNT(*)" + from +
                                    "SUBSTR(f.label,1,2) IN ('AB','CD') GROUP "
                                    "BY d.bucket ORDER BY d.bucket"),
            std::vector<Row>({Row({Value(0), Value(16384)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*)" + from +
                                    "f.v < (SELECT MIN(b.threshold) FROM "
                                    "FilterBlocked b WHERE b.k=f.k)"),
            std::vector<Row>({Row({Value(4096)})}));
  RelationalRun(
      *rs_,
      "INSERT INTO FilterBlocked VALUES (NULL,5,'Customer other Complaints')");
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT COUNT(*)" + from + "f.k NOT IN " + members),
      std::vector<Row>({Row({Value(0)})}));
}

TEST_F(RelationalRegressionTest,
       SelectiveIndexSourceKeepsHeapOrderAndSnapshot) {
  // The reader and private writer deliberately share a test thread; do not
  // park waiting for an undo reservation only this thread can release.
  rs_->SetDeadlockPolicy(Database::DeadlockPolicy::kNoWait);
  RelationalRun(*rs_, "CREATE TABLE RangeSource (id INT64, k INT64, v STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("RangeSource"));
    for (int64_t i = 0; i < 4000; ++i) {
      ASSERT_TRUE(table
                      ->Insert(setup.txn_, Row({Value(i), Value(3999 - i),
                                                Value("payload")}))
                      .HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE INDEX range_k ON RangeSource(k)");
  RelationalRun(*rs_, "ANALYZE RangeSource");
  auto old = rs_->BeginReadOnlyContext();
  SelectSource source;
  source.table = "RangeSource";
  source.alias = "r";
  const std::vector<slot_t> projection{0, 1, 2};
  const std::vector<Expression> predicates{
      BinaryExpressionExp(ColumnValueExp(ColumnName("r", "k")),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(3000))),
      BinaryExpressionExp(ColumnValueExp(ColumnName("r", "k")),
                          BinaryOperation::kLessThan,
                          ConstantValueExp(Value(3020)))};
  auto read = [&](TransactionContext& context, size_t* visited) {
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    auto loaded = relational_detail::LoadSource(context, source, nullptr, {},
                                                &projection, &predicates);
    context.set_execution_runtime(nullptr);
    EXPECT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    *visited = runtime.scan_rows;
    std::vector<Row> rows;
    if (loaded.HasValue()) {
      EXPECT_EQ(loaded.Value().ForEachRow(
                    [&](const Row& row) { rows.push_back(row); }),
                Status::kSuccess);
    }
    return rows;
  };
  size_t visited = 0;
  const auto expected = read(old, &visited);
  ASSERT_EQ(expected.size(), 20U);
  EXPECT_LT(visited, 100U);
  for (int64_t i = 0; i < 20; ++i) {
    EXPECT_EQ(expected[static_cast<size_t>(i)],
              Row({Value(980 + i), Value(3019 - i), Value("payload")}));
  }
  // A selective key enclosure remains useful with a local LIKE residual.
  // The canonical residual still decides the rows after heap order is restored.
  for (const auto op : {BinaryOperation::kLike, BinaryOperation::kNotLike}) {
    auto with_like = predicates;
    with_like.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName("r", "v")), op,
        ConstantValueExp(
            Value(op == BinaryOperation::kLike ? "%load" : "%other"))));
    relational_detail::ExecutionRuntime runtime;
    old.set_execution_runtime(&runtime);
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, &with_like);
    old.set_execution_runtime(nullptr);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    EXPECT_EQ(rows, expected);
    EXPECT_EQ(runtime.selective_index_scans, 1U);
    EXPECT_LT(runtime.scan_rows, 100U);
  }
  {
    relational_detail::ExecutionRuntime runtime;
    old.set_execution_runtime(&runtime);
    RuntimeFilterBuilder builder;
    builder.Observe(3000);
    builder.Observe(3005);
    const ScanRuntimeFilter filter{
        .group = builder.Build(), .column = 1, .type = ValueType::kInt64};
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, nullptr, &filter);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0][1], Value(3005));
    EXPECT_EQ(rows[1][1], Value(3000));
    EXPECT_EQ(runtime.scan_rows, 2U);
    old.set_execution_runtime(nullptr);
  }
  {
    relational_detail::ExecutionRuntime runtime;
    old.set_execution_runtime(&runtime);
    RuntimeFilterBuilder builder;
    for (int64_t k = 3000; k < 3065; ++k) {
      builder.Observe(k);
    }
    const ScanRuntimeFilter filter{
        .group = builder.Build(), .column = 1, .type = ValueType::kInt64};
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, nullptr, &filter);
    old.set_execution_runtime(nullptr);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    ASSERT_EQ(rows.size(), 65U);
    EXPECT_EQ(runtime.selective_index_scans, 1U);
    EXPECT_EQ(runtime.scan_rows, 65U);
    for (size_t i = 1; i < rows.size(); ++i) {
      EXPECT_GT(rows[i - 1][1].value.int_value, rows[i][1].value.int_value);
    }
  }
  {
    auto only_one = predicates;
    only_one.push_back(InExpressionExp(
        ColumnValueExp(ColumnName("r", "id")),
        {ConstantValueExp(Value(980)), ConstantValueExp(Value())}));
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, &only_one);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    EXPECT_EQ(rows, std::vector<Row>({expected.front()}));
  }
  // A private append can grow and then unlink the heap tail. Page-order
  // metadata must not leak its rows into another snapshot or misorder a
  // later append which reuses the freed page ids.
  for (int round = 0; round < 2; ++round) {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          writer.GetTable("RangeSource"));
    for (int64_t i = 0; i < 20; ++i) {
      ASSERT_TRUE(table
                      ->Insert(writer.txn_,
                               Row({Value(4000 + (round * 20) + i), Value(3001),
                                    Value(std::string(4096, 'a'))}))
                      .HasValue());
    }
    const auto own = read(writer, &visited);
    ASSERT_EQ(own.size(), expected.size() + 20);
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(own[i], expected[i]);
    }
    for (size_t i = expected.size(); i < own.size(); ++i) {
      EXPECT_EQ(own[i][0], Value(4000 + (round * 20) +
                                 static_cast<int64_t>(i - expected.size())));
    }
    // An index path may answer from the committed image. If it declines,
    // the heap scan must report its unpublished-tail wait failure rather
    // than quietly accepting the prefix as a complete scan (audit medium-44).
    relational_detail::ExecutionRuntime pending_runtime;
    old.set_execution_runtime(&pending_runtime);
    auto pending = relational_detail::LoadSource(old, source, nullptr, {},
                                                 &projection, &predicates);
    old.set_execution_runtime(nullptr);
    if (pending.HasValue()) {
      std::vector<Row> rows;
      ASSERT_SUCCESS(pending.Value().ForEachRow(
          [&](const Row& row) { rows.push_back(row); }));
      EXPECT_EQ(rows, expected);
    } else {
      EXPECT_EQ(pending.GetStatus(), Status::kConflicts);
    }
    writer.Abort();
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh, &visited), expected);
    fresh.Abort();
  }
  RelationalRun(*rs_, "UPDATE RangeSource SET k=0 WHERE id=980");
  EXPECT_EQ(read(old, &visited), expected);
  auto fresh = rs_->BeginReadOnlyContext();
  EXPECT_EQ(read(fresh, &visited).size(), 19U);
  fresh.Abort();
  old.Abort();
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
  auto slot = std::make_shared<ParameterSlot>(0, Value(1));
  const Expression first = InExpressionExp(ColumnValueExp("k"), {slot});
  auto compiled = relational_detail::CompileScanFilter({first}, schema);
  ASSERT_EQ(compiled.residual.size(), 1U);
  slot->Bind(Value(9));
  const Expression same_values =
      InExpressionExp(ColumnValueExp("k"), {ConstantValueExp(Value(1))});
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

}  // namespace

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

}  // namespace tinylamb
