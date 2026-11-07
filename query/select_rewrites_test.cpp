/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/select_rewrites.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::unique_ptr<SelectStatement> ParseSelect(const std::string& sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  if (!parsed.ok) {
    return nullptr;
  }
  auto ast = GoogleSqlAstParser::Parse(parsed.ast);
  if (!ast.HasValue()) {
    return nullptr;
  }
  std::unique_ptr<Statement> statement =
      GoogleSqlAstVisitor::Visit(*ast.Value(), sql).MoveValue();
  if (statement == nullptr || statement->Type() != StatementType::kSelect) {
    return nullptr;
  }
  return std::unique_ptr<SelectStatement>(
      static_cast<SelectStatement*>(statement.release()));
}

class SelectRewritesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "GoogleSQL frontend unavailable";
    }
    db_ = Database::Create("select-rewrites-" + RandomString()).MoveValue();
    ctx_ = std::make_unique<TransactionContext>(db_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*db_);
    ASSERT_TRUE(engine_
                    ->Execute(*ctx_,
                              "CREATE TABLE items (id INT64 PRIMARY KEY, "
                              "category INT64, price INT64, name STRING);")
                    .HasValue());
    ASSERT_TRUE(engine_
                    ->Execute(*ctx_,
                              "INSERT INTO items VALUES (1, 10, 100, 'a'), "
                              "(2, 10, 200, 'b'), (3, 20, 300, 'c');")
                    .HasValue());
  }

  void TearDown() override {
    engine_.reset();
    ctx_.reset();
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
    db_.reset();
  }

  std::unique_ptr<Database> db_;
  std::unique_ptr<TransactionContext> ctx_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SelectRewritesTest, InlineSingleUseCtesReplacesSingleReference) {
  auto stmt = ParseSelect(
      "WITH single_cte AS (SELECT id, price FROM items WHERE price > 50) "
      "SELECT id FROM single_cte WHERE id < 10");
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->WithQueries().size(), 1U);
  EXPECT_TRUE(InlineSingleUseCtes(stmt.get()));
  EXPECT_TRUE(stmt->WithQueries().empty());
  ASSERT_EQ(stmt->Sources().size(), 1U);
  EXPECT_NE(stmt->Sources()[0].query, nullptr);

  // Multi-use CTE must not be inlined by InlineSingleUseCtes.
  auto multi = ParseSelect(
      "WITH shared_cte AS (SELECT id, price FROM items) "
      "SELECT a.id FROM shared_cte a JOIN shared_cte b ON a.id = b.id");
  ASSERT_NE(multi, nullptr);
  EXPECT_FALSE(InlineSingleUseCtes(multi.get()));
  EXPECT_EQ(multi->WithQueries().size(), 1U);
}

TEST_F(SelectRewritesTest, ExpandStarWithModifiersAppliesExceptAndReplace) {
  auto stmt = ParseSelect(
      "SELECT * EXCEPT (category) REPLACE (price * 2 AS price) FROM items");
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(stmt->SelectList().size(), 1U);

  const Schema schema("items",
                      {
                          Column(ColumnName("id"), ValueType::kInt64),
                          Column(ColumnName("category"), ValueType::kInt64),
                          Column(ColumnName("price"), ValueType::kInt64),
                          Column(ColumnName("name"), ValueType::kVarChar),
                      });
  std::vector<NamedExpression> expanded;
  std::unordered_set<std::string> resolved_except;
  std::unordered_set<std::string> resolved_replace;
  ExpandStarWithModifiers(stmt->SelectList()[0], "items", schema, &expanded,
                          &resolved_except, &resolved_replace);
  ASSERT_EQ(expanded.size(), 3U);
  EXPECT_EQ(expanded[0].expression->AsColumnValue().GetColumnName().name, "id");
  EXPECT_EQ(expanded[1].name, "price");
  EXPECT_EQ(expanded[2].expression->AsColumnValue().GetColumnName().name,
            "name");
  EXPECT_TRUE(resolved_except.contains("category"));
  EXPECT_TRUE(resolved_replace.contains("price"));
}

TEST_F(SelectRewritesTest, MergeCompatibleUnionBranchesFusesSameTableScan) {
  auto stmt = ParseSelect(
      "SELECT id, name FROM items WHERE category = 10 "
      "UNION ALL "
      "SELECT id, name FROM items WHERE category = 20");
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(stmt->UnionAll().size(), 1U);
  EXPECT_TRUE(MergeCompatibleUnionBranches(stmt.get(), *ctx_));
  EXPECT_TRUE(stmt->UnionAll().empty());
  EXPECT_NE(stmt->WhereClause(), nullptr);
}

TEST_F(SelectRewritesTest, FlattenDerivedSourcesInlinesSimpleSubquery) {
  auto stmt = ParseSelect(
      "SELECT sub.id, sub.price FROM "
      "(SELECT id, price FROM items WHERE price >= 100) AS sub "
      "WHERE sub.id <= 2");
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(stmt->Sources().size(), 1U);
  ASSERT_NE(stmt->Sources()[0].query, nullptr);

  EXPECT_TRUE(FlattenDerivedSources(stmt.get(), *ctx_));
  ASSERT_EQ(stmt->Sources().size(), 1U);
  EXPECT_EQ(stmt->Sources()[0].query, nullptr);
  EXPECT_EQ(stmt->Sources()[0].table, "items");
}

TEST_F(SelectRewritesTest, TryNarrowCteBodyPushesOuterBoundsIntoCte) {
  auto stmt = ParseSelect(
      "WITH cte AS (SELECT id, price FROM items) "
      "SELECT c.id FROM cte c WHERE c.price >= 150");
  ASSERT_NE(stmt, nullptr);
  auto it = stmt->WithQueries().find("cte");
  ASSERT_NE(it, stmt->WithQueries().end());
  SelectStatement body = *it->second;
  EXPECT_EQ(body.WhereClause(), nullptr);

  EXPECT_TRUE(TryNarrowCteBody(&body, *stmt, {"c"}));
  EXPECT_NE(body.WhereClause(), nullptr);
}

TEST_F(SelectRewritesTest, ReduceGroupByFunctionalDependenciesDropsNonPkCols) {
  auto stmt =
      ParseSelect("SELECT id, name, SUM(price) FROM items GROUP BY id, name");
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(stmt->GroupBy().size(), 2U);
  EXPECT_TRUE(ReduceGroupByFunctionalDependencies(stmt.get(), *ctx_));
  EXPECT_EQ(stmt->GroupBy().size(), 1U);
}

TEST_F(SelectRewritesTest, RecursiveCteLiftableChecksSingleUseAndNoSiblingRef) {
  auto liftable = ParseSelect(
      "WITH RECURSIVE r AS ("
      "  SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5"
      ") SELECT n FROM r");
  ASSERT_NE(liftable, nullptr);
  EXPECT_TRUE(RecursiveCteLiftable(*liftable, "r"));

  auto multi_ref = ParseSelect(
      "WITH RECURSIVE r AS ("
      "  SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5"
      ") SELECT a.n FROM r a JOIN r b ON a.n = b.n");
  ASSERT_NE(multi_ref, nullptr);
  EXPECT_FALSE(RecursiveCteLiftable(*multi_ref, "r"));
}

}  // namespace
}  // namespace tinylamb
