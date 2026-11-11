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
#include "executor/detail/subquery_runtime.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
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
    auto inserted =
        engine_->Execute(*ctx_,
                         "INSERT INTO items VALUES (1, 10, 100, 'a'), "
                         "(2, 10, 200, 'b'), (3, 20, 300, 'c');");
    ASSERT_TRUE(inserted.HasValue());
    inserted.Value().Drain();
    ASSERT_EQ(inserted.Value().GetStatus(), Status::kSuccess);
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

TEST_F(SelectRewritesTest, SingleUseGroupedCtesRetainFiltersAndSharedBodies) {
  auto statement = ParseSelect(
      "WITH a AS (SELECT category AS ak,SUM(price) AS n FROM items GROUP BY "
      "category),"
      "b AS (SELECT category AS bk,COUNT(*) AS n FROM items GROUP BY category) "
      "SELECT ak,a.n,b.n FROM a LEFT JOIN b ON ak=bk WHERE ak=10");
  ASSERT_NE(statement, nullptr);
  auto original_body = statement->WithQueries().at("a");
  EXPECT_TRUE(InlineSingleUseGroupedCtes(statement.get(), *ctx_));
  EXPECT_TRUE(statement->WithQueries().empty());
  ASSERT_EQ(statement->Sources().size(), 2U);
  for (const auto& source : statement->Sources()) {
    ASSERT_NE(source.query, nullptr);
    EXPECT_NE(source.query->WhereClause(), nullptr);
    EXPECT_FALSE(source.is_lateral);
  }
  EXPECT_EQ(original_body->WhereClause(), nullptr);
  auto execution =
      relational_detail::ExecuteQuery(*ctx_, *statement, nullptr, {});
  ASSERT_TRUE(execution.HasValue()) << execution.GetStatus();
  EXPECT_EQ(execution.Value().rows,
            std::vector<Row>({Row({Value(10), Value(300), Value(2)})}));
  for (const auto& sql : {"WITH c AS (SELECT category,SUM(price) AS n FROM "
                          "items GROUP BY category) "
                          "SELECT a.n,b.n FROM c a,c b",
                          "WITH c AS (SELECT category,SUM(price) AS n FROM "
                          "items GROUP BY category LIMIT 1) "
                          "SELECT n FROM c",
                          "WITH c AS (SELECT category,SUM(price) AS n FROM "
                          "items GROUP BY ROLLUP(category)) "
                          "SELECT n FROM c"}) {
    auto fenced = ParseSelect(sql);
    ASSERT_NE(fenced, nullptr);
    EXPECT_FALSE(InlineSingleUseGroupedCtes(fenced.get(), *ctx_));
  }
}

TEST_F(SelectRewritesTest, NestedSingleUseWindowCtesKeepPartitionsAndTrees) {
  const std::string body =
      "SELECT id, SUM(SUM(price)) OVER (PARTITION BY category ORDER BY id) "
      "AS running FROM items GROUP BY id,category";
  auto statement = ParseSelect(
      "WITH totals AS (" + body +
      ") SELECT * FROM (SELECT id,running FROM totals WHERE id=2) q");
  ASSERT_NE(statement, nullptr);
  const auto original_consumer = statement->Sources().front().query;
  const auto original_body = statement->WithQueries().at("totals");
  ASSERT_TRUE(InlineSingleUseGroupedCtes(statement.get(), *ctx_));
  EXPECT_TRUE(statement->WithQueries().empty());
  EXPECT_EQ(original_consumer->Sources().front().table, "totals");
  EXPECT_EQ(original_consumer->Sources().front().query, nullptr);
  EXPECT_EQ(original_body->WhereClause(), nullptr);
  const auto& nested = statement->Sources().front().query;
  ASSERT_NE(nested, original_consumer);
  ASSERT_NE(nested->Sources().front().query, nullptr);
  EXPECT_EQ(nested->Sources().front().query->WhereClause(), nullptr);
  auto result = relational_detail::ExecuteQuery(*ctx_, *statement, nullptr, {});
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_EQ(result.Value().rows,
            std::vector<Row>({Row({Value(2), Value(300)})}));
  auto shared = ParseSelect("WITH totals AS (" + body +
                            ") SELECT a.id FROM totals a JOIN "
                            "(SELECT id FROM totals) b ON a.id=b.id");
  ASSERT_NE(shared, nullptr);
  EXPECT_FALSE(InlineSingleUseGroupedCtes(shared.get(), *ctx_));
  EXPECT_EQ(shared->WithQueries().size(), 1U);
  // An intervening WITH scope is not rewritten through the outer map.
  auto shadowed =
      ParseSelect("WITH totals AS (" + body +
                  ") SELECT * FROM (WITH totals AS (SELECT 9 AS id) "
                  "SELECT id FROM totals) q");
  ASSERT_NE(shadowed, nullptr);
  EXPECT_FALSE(InlineSingleUseGroupedCtes(shadowed.get(), *ctx_));
}

TEST_F(SelectRewritesTest, PresenceOnlyCteDropsUnusedColumnsAndDuplicates) {
  const std::string body =
      "SELECT a.category AS k,a.id AS unused FROM items a JOIN items b "
      "ON a.category=b.category WHERE a.id<>b.id";
  const std::string sql =
      "WITH candidates AS (" + body +
      ") SELECT id FROM items WHERE category IN (SELECT k FROM candidates) "
      "AND category IN (SELECT c.k FROM candidates c JOIN items x "
      "ON c.k=x.category) ORDER BY id";
  auto statement = ParseSelect(sql);
  ASSERT_NE(statement, nullptr);
  auto grouped =
      relational_detail::GroupPresenceOnlyCte(*statement, "candidates");
  ASSERT_NE(grouped, nullptr);
  ASSERT_EQ(grouped->SelectList().size(), 1U);
  EXPECT_EQ(grouped->SelectList()[0].name, "k");
  EXPECT_EQ(grouped->GroupBy().size(), 1U);
  EXPECT_EQ(statement->WithQueries().at("candidates")->SelectList().size(), 2U);
  auto cell = relational_detail::ExecuteQuery(*ctx_, *grouped, nullptr, {});
  ASSERT_TRUE(cell.HasValue()) << cell.GetStatus();
  EXPECT_EQ(cell.Value().rows, std::vector<Row>({Row({Value(10)})}));
  auto result = engine_->Execute(*ctx_, sql);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_EQ(result.Value().Collect(),
            std::vector<Row>({Row({Value(1)}), Row({Value(2)})}));
  for (const auto& consumer :
       {"SELECT COUNT(*) FROM candidates",
        "SELECT id FROM items WHERE category IN (SELECT SUM(k) FROM "
        "candidates)",
        "SELECT id FROM items WHERE category IN (SELECT k FROM candidates "
        "LIMIT 100 OFFSET 1)",
        "SELECT ARRAY(SELECT k FROM candidates)",
        "SELECT (SELECT k FROM candidates)",
        "SELECT k FROM candidates WHERE k IN (SELECT k FROM candidates)"}) {
    auto sensitive =
        ParseSelect("WITH candidates AS (" + body + ") " + consumer);
    ASSERT_NE(sensitive, nullptr) << consumer;
    EXPECT_EQ(relational_detail::GroupPresenceOnlyCte(*sensitive, "candidates"),
              nullptr)
        << consumer;
  }
}

TEST_F(SelectRewritesTest, SingleUseUnionInsideCteClonesItsConsumer) {
  const std::string sql =
      "WITH raw AS (SELECT category,price FROM ("
      "SELECT category,price FROM items UNION ALL SELECT category,price FROM "
      "items) u),"
      "totals AS (SELECT category,SUM(price) AS total FROM raw GROUP BY "
      "category) "
      "SELECT a.category,a.total FROM totals a JOIN totals b ON "
      "a.category=b.category "
      "ORDER BY a.category";
  auto statement = ParseSelect(sql);
  ASSERT_NE(statement, nullptr);
  const auto original_consumer = statement->WithQueries().at("totals");
  ASSERT_EQ(original_consumer->Sources()[0].table, "raw");
  EXPECT_TRUE(InlineSingleUseCtes(statement.get()));
  EXPECT_FALSE(statement->WithQueries().contains("raw"));
  const auto rewritten = statement->WithQueries().at("totals");
  EXPECT_NE(rewritten.get(), original_consumer.get());
  EXPECT_EQ(original_consumer->Sources()[0].table, "raw");
  EXPECT_EQ(original_consumer->Sources()[0].query, nullptr);
  EXPECT_NE(rewritten->Sources()[0].query, nullptr);
  EXPECT_EQ(rewritten->Sources()[0].alias, "raw");
  auto result = engine_->Execute(*ctx_, sql);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_EQ(result.Value().Collect(),
            std::vector<Row>(
                {Row({Value(10), Value(600)}), Row({Value(20), Value(600)})}));
  EXPECT_EQ(result.Value().GetStatus(), Status::kSuccess);
  for (const auto& body :
       {"SELECT id FROM items LIMIT 1",
        "SELECT id FROM items UNION DISTINCT SELECT id FROM items"}) {
    auto fenced = ParseSelect(
        std::string("WITH raw AS (") + body +
        "), totals AS (SELECT SUM(id) AS n FROM raw) SELECT n FROM totals");
    ASSERT_NE(fenced, nullptr);
    InlineSingleUseCtes(fenced.get());
    EXPECT_TRUE(fenced->WithQueries().contains("raw"));
  }
  auto shared = ParseSelect(
      "WITH raw AS (SELECT id FROM items), totals AS (SELECT SUM(id) AS n FROM "
      "raw) "
      "SELECT r.id,t.n FROM raw r,totals t");
  ASSERT_NE(shared, nullptr);
  InlineSingleUseCtes(shared.get());
  EXPECT_TRUE(shared->WithQueries().contains("raw"));
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
