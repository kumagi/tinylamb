/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/select_rewrites.hpp"

#include <gtest/gtest.h>

#include <algorithm>

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
      static_cast<SelectStatement*>(statement.release()));  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
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

TEST_F(SelectRewritesTest, DisjunctiveSemiFilterRequiresDirectComparisons) {
  const std::string prefix =
      "WITH a AS (SELECT category AS ak, SUM(price) AS a_total "
      "FROM items WHERE id = 1 GROUP BY category), "
      "b AS (SELECT category AS bk, SUM(price) AS b_total "
      "FROM items WHERE id = 2 GROUP BY category), "
      "s AS (SELECT category AS sk, SUM(price) AS total "
      "FROM items GROUP BY category) "
      "SELECT sk, total FROM s LEFT JOIN a ON ak = sk "
      "LEFT JOIN b ON bk = sk WHERE ";
  const std::string direct_sql = prefix + "a_total > 0 OR 0 < b_total";
  auto direct = ParseSelect(direct_sql);
  ASSERT_NE(direct, nullptr);
  EXPECT_TRUE(
      PushDisjunctiveNullRejectingLeftJoinSemiFilter(direct.get(), *ctx_));

  auto direct_result = engine_->Execute(*ctx_, direct_sql);
  ASSERT_TRUE(direct_result.HasValue()) << direct_result.GetStatus();
  EXPECT_EQ(direct_result.Value().Collect(),
            std::vector<Row>({Row({Value(10), Value(300)})}));
  EXPECT_EQ(direct_result.Value().GetStatus(), Status::kSuccess);

  const std::string sql =
      prefix + "COALESCE(a_total, 0) > 0 OR COALESCE(b_total, 0) > 0";
  auto computed = ParseSelect(sql);
  ASSERT_NE(computed, nullptr);
  EXPECT_FALSE(
      PushDisjunctiveNullRejectingLeftJoinSemiFilter(computed.get(), *ctx_));
  auto result = engine_->Execute(*ctx_, sql);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_EQ(result.Value().Collect(),
            std::vector<Row>({Row({Value(10), Value(300)})}));
  EXPECT_EQ(result.Value().GetStatus(), Status::kSuccess);
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
  const std::string sql =
      "SELECT id, name FROM items WHERE category = 10 "
      "UNION ALL "
      "SELECT id, name FROM items WHERE category = 20";
  auto stmt = ParseSelect(sql);
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(stmt->UnionAll().size(), 1U);
  EXPECT_TRUE(MergeCompatibleUnionBranches(stmt.get(), *ctx_));
  EXPECT_TRUE(stmt->UnionAll().empty());
  EXPECT_NE(stmt->WhereClause(), nullptr);
  auto result = engine_->Execute(*ctx_, sql);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  auto rows = result.Value().Collect();
  std::ranges::sort(rows, {}, [](const Row& row) { return row[0].AsInt64(); });
  EXPECT_EQ(rows, (std::vector<Row>{Row({Value(1), Value("a")}),
                                    Row({Value(2), Value("b")}),
                                    Row({Value(3), Value("c")})}));
  EXPECT_EQ(result.Value().GetStatus(), Status::kSuccess);
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

TEST_F(SelectRewritesTest,
       FlattenDerivedSourcesSkipsQueryExpressionOuterReferences) {
  // A kQueryExp is opaque to ExpressionChildren: a `d.col` reference inside
  // the subquery would survive substitution and silently rebind to the base
  // column once the derived alias is dropped. Flattening must refuse.
  auto stmt = ParseSelect(
      "SELECT d.id, (SELECT COUNT(*) FROM items i2 WHERE i2.price = d.price) "
      "FROM (SELECT id, price * 2 AS price FROM items) AS d");
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(stmt->Sources().size(), 1U);
  ASSERT_NE(stmt->Sources()[0].query, nullptr);
  EXPECT_FALSE(FlattenDerivedSources(stmt.get(), *ctx_));
  EXPECT_NE(stmt->Sources()[0].query, nullptr);

  // The gate is carrier-wide: WHERE, ORDER BY and join/unnest arguments get
  // the same protection, and an uncorrelated subquery stays conservative.
  for (const std::string& sql : {
           "SELECT d.id FROM (SELECT id, price FROM items) AS d "
           "WHERE d.id IN (SELECT id FROM items)",
           "SELECT d.id FROM (SELECT id, price FROM items) AS d "
           "ORDER BY (SELECT COUNT(*) FROM items)",
           "SELECT d.id, (SELECT COUNT(*) FROM items) FROM "
           "(SELECT id, price FROM items) AS d",
       }) {
    SCOPED_TRACE(sql);
    auto carrier = ParseSelect(sql);
    ASSERT_NE(carrier, nullptr);
    ASSERT_EQ(carrier->Sources().size(), 1U);
    ASSERT_NE(carrier->Sources()[0].query, nullptr);
    EXPECT_FALSE(FlattenDerivedSources(carrier.get(), *ctx_));
    EXPECT_NE(carrier->Sources()[0].query, nullptr);
  }
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

TEST_F(SelectRewritesTest, StripIdempotentDistinctEliminatesMinMaxOnly) {
  auto stmt = ParseSelect(
      "SELECT category, MIN(DISTINCT price), MAX(DISTINCT price), "
      "SUM(DISTINCT price) FROM items GROUP BY category");
  ASSERT_NE(stmt, nullptr);
  EXPECT_TRUE(StripIdempotentDistinctAggregates(stmt.get()));
  ASSERT_EQ(stmt->SelectList().size(), 4U);
  EXPECT_FALSE(
      stmt->SelectList()[1].expression->AsAggregateExpression().Distinct());
  EXPECT_FALSE(
      stmt->SelectList()[2].expression->AsAggregateExpression().Distinct());
  EXPECT_TRUE(
      stmt->SelectList()[3].expression->AsAggregateExpression().Distinct());
}

TEST_F(SelectRewritesTest,
       PushHavingExtremumBoundsPushesIntAndPreservesDoubleNaN) {
  auto int_stmt = ParseSelect(
      "SELECT category, MAX(price) AS mp FROM items "
      "GROUP BY category HAVING mp > 150");
  ASSERT_NE(int_stmt, nullptr);
  EXPECT_EQ(int_stmt->WhereClause(), nullptr);
  EXPECT_TRUE(PushHavingExtremumBounds(int_stmt.get(), *ctx_));
  ASSERT_NE(int_stmt->WhereClause(), nullptr);
  EXPECT_NE(int_stmt->Having(), nullptr);

  // End-to-end GoogleSQL DOUBLE NaN poisoning guard: group 1 has {20.0, NaN},
  // so MAX(val) = NaN, and `NaN > 10.0` is FALSE! Pushing `val > 10.0` into
  // WHERE would drop NaN and wrongly return 20.0.
  ASSERT_TRUE(
      engine_->Execute(*ctx_, "CREATE TABLE dbl_nan (grp INT64, val DOUBLE);")
          .HasValue());
  auto ins = engine_->Execute(*ctx_,
                              "INSERT INTO dbl_nan VALUES (1, 20.0), "
                              "(1, CAST('NaN' AS DOUBLE)), (2, 30.0);");
  ASSERT_TRUE(ins.HasValue());
  ins.Value().Drain();
  ASSERT_EQ(ins.Value().GetStatus(), Status::kSuccess);

  auto dbl_stmt = ParseSelect(
      "SELECT grp, MAX(val) FROM dbl_nan GROUP BY grp HAVING MAX(val) > 10.0");
  ASSERT_NE(dbl_stmt, nullptr);
  EXPECT_FALSE(PushHavingExtremumBounds(dbl_stmt.get(), *ctx_));

  auto res = engine_->Execute(
      *ctx_,
      "SELECT grp, MAX(val) FROM dbl_nan GROUP BY grp HAVING MAX(val) > 10.0");
  ASSERT_TRUE(res.HasValue());
  std::vector<Row> rows;
  Row row;
  while (res.Value().Next(&row)) {
    rows.push_back(row);
    row = Row();
  }
  ASSERT_EQ(res.Value().GetStatus(), Status::kSuccess);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
}

TEST_F(SelectRewritesTest,
       EliminateUnusedOuterJoinDropsNonUniqueLeftJoinUnderMaxAndDistinct) {
  // `tags` has NO primary key or unique constraint (M:N LEFT JOIN).
  ASSERT_TRUE(
      engine_->Execute(*ctx_, "CREATE TABLE tags (item_id INT64, tag STRING);")
          .HasValue());
  auto ins = engine_->Execute(
      *ctx_, "INSERT INTO tags VALUES (1, 'x'), (1, 'y'), (1, 'z'), (2, 'w');");
  ASSERT_TRUE(ins.HasValue());
  ins.Value().Drain();
  ASSERT_EQ(ins.Value().GetStatus(), Status::kSuccess);

  auto max_stmt = ParseSelect(
      "SELECT i.category, MAX(i.price), COUNT(DISTINCT i.id) "
      "FROM items i LEFT JOIN tags t ON i.id = t.item_id GROUP BY i.category");
  ASSERT_NE(max_stmt, nullptr);
  ASSERT_EQ(max_stmt->Sources().size(), 2U);
  EXPECT_TRUE(
      EliminateUnusedOuterJoinsForIdempotentResult(max_stmt.get(), *ctx_));
  EXPECT_EQ(max_stmt->Sources().size(), 1U);

  // Must NOT drop when SUM (multiplicity-sensitive) is present!
  auto sum_stmt = ParseSelect(
      "SELECT i.category, SUM(i.price) "
      "FROM items i LEFT JOIN tags t ON i.id = t.item_id GROUP BY i.category");
  ASSERT_NE(sum_stmt, nullptr);
  EXPECT_FALSE(
      EliminateUnusedOuterJoinsForIdempotentResult(sum_stmt.get(), *ctx_));
  EXPECT_EQ(sum_stmt->Sources().size(), 2U);

  // End-to-end SQL check on `SELECT * ... LEFT JOIN ... WHERE t.item_id IS
  // NULL` with nullable t.item_id AND left residual (`i.price >= 200`):
  auto anti_res = engine_->Execute(
      *ctx_,
      "SELECT i.id, t.tag FROM items i LEFT JOIN tags t ON i.id = t.item_id "
      "WHERE t.item_id IS NULL AND i.price >= 200");
  ASSERT_TRUE(anti_res.HasValue());
  std::vector<Row> rows;
  Row r;
  while (anti_res.Value().Next(&r)) {
    rows.push_back(r);
    r = Row();
  }
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{3}));
  EXPECT_TRUE(rows[0][1].IsNull());
}

TEST_F(SelectRewritesTest, TpcdsDisjointUnionGroupByBypassAndPlanCacheSafety) {
  ASSERT_TRUE(engine_->Execute(*ctx_, "CREATE TABLE ch_s (k INT64, v INT64);")
                  .HasValue());
  ASSERT_TRUE(engine_->Execute(*ctx_, "CREATE TABLE ch_w (k INT64, v INT64);")
                  .HasValue());
  auto ins1 = engine_->Execute(
      *ctx_, "INSERT INTO ch_s VALUES (1, 10), (1, 20), (2, 5);");
  ASSERT_TRUE(ins1.HasValue());
  ins1.Value().Drain();
  ASSERT_EQ(ins1.Value().GetStatus(), Status::kSuccess);
  auto ins2 = engine_->Execute(
      *ctx_, "INSERT INTO ch_w VALUES (1, 100), (2, 200), (2, 300);");
  ASSERT_TRUE(ins2.HasValue());
  ins2.Value().Drain();
  ASSERT_EQ(ins2.Value().GetStatus(), Status::kSuccess);

  const std::string disjoint_sql =
      "SELECT ch, k, COUNT(*), SUM(v) FROM ("
      "  SELECT 'store' AS ch, k, v FROM ch_s "
      "  UNION ALL "
      "  SELECT 'web' AS ch, k, v FROM ch_w"
      ") foo GROUP BY ch, k ORDER BY ch, k LIMIT 3";

  auto ast = ParseSelect(disjoint_sql);
  ASSERT_NE(ast, nullptr);
  EXPECT_TRUE(BypassDisjointUnionGroupBy(ast.get()));
  EXPECT_NE(ast->GetSetOperationTree(), nullptr);

  // Differential check against `force_relational_fallback_ = true`:
  auto run_rows = [&](bool fallback, const std::string& sql) {
    engine_->SetForceRelationalFallback(fallback);
    auto r = engine_->Execute(*ctx_, sql);
    EXPECT_TRUE(r.HasValue()) << engine_->LastError();
    std::vector<Row> out;
    if (r.HasValue()) {
      Row row;
      while (r.Value().Next(&row)) {
        out.push_back(row);
        row = Row();
      }
      EXPECT_EQ(r.Value().GetStatus(), Status::kSuccess) << sql;
    }
    engine_->SetForceRelationalFallback(false);
    return out;
  };
  const auto opt_disjoint = run_rows(false, disjoint_sql);
  const auto ref_disjoint = run_rows(true, disjoint_sql);
  EXPECT_EQ(opt_disjoint, ref_disjoint);

  // Plan-cache safety (`CompiledPlan` literal specialization): running the SAME
  // SQL template next with IDENTICAL tags `'same'` / `'same'` MUST NOT replay
  // the bypassed union plan and must merge `(same, 1)` across branches!
  const std::string colliding_sql =
      "SELECT ch, k, COUNT(*), SUM(v) FROM ("
      "  SELECT 'same' AS ch, k, v FROM ch_s "
      "  UNION ALL "
      "  SELECT 'same' AS ch, k, v FROM ch_w"
      ") foo GROUP BY ch, k ORDER BY ch, k LIMIT 3";
  const auto opt_colliding = run_rows(false, colliding_sql);
  const auto ref_colliding = run_rows(true, colliding_sql);
  EXPECT_EQ(opt_colliding, ref_colliding);
  ASSERT_EQ(opt_colliding.size(), 2U);
  EXPECT_EQ(opt_colliding[0][2], Value(int64_t{3}));
  EXPECT_EQ(opt_colliding[0][3], Value(int64_t{130}));
}

TEST_F(SelectRewritesTest, TpcdsSelfJoinWindowAndNullKeyGuard) {
  // Include NULL store_sk rows (`self_join_agg_window_null_key_bug`): SQL
  // `sb.store_sk = sc.store_sk` must drop NULL keys, whereas naive window
  // partitioning groups NULLs together!
  ASSERT_TRUE(
      engine_
          ->Execute(
              *ctx_,
              "CREATE TABLE sr_null (cust INT64, store_sk INT64, amt DOUBLE);")
          .HasValue());
  auto ins = engine_->Execute(*ctx_,
                              "INSERT INTO sr_null VALUES "
                              "(1, 10, 100.0), (2, 10, 20.0), "
                              "(3, NULL, 500.0), (4, NULL, 10.0);");
  ASSERT_TRUE(ins.HasValue());
  ins.Value().Drain();
  ASSERT_EQ(ins.Value().GetStatus(), Status::kSuccess);

  auto run_rows = [&](bool fallback, const std::string& sql) {
    engine_->SetForceRelationalFallback(fallback);
    auto r = engine_->Execute(*ctx_, sql);
    EXPECT_TRUE(r.HasValue()) << engine_->LastError();
    std::vector<Row> out;
    if (r.HasValue()) {
      Row row;
      while (r.Value().Next(&row)) {
        out.push_back(row);
        row = Row();
      }
      EXPECT_EQ(r.Value().GetStatus(), Status::kSuccess) << sql;
    }
    engine_->SetForceRelationalFallback(false);
    return out;
  };

  // Q65 sibling derived aggregate shape:
  const std::string q65_shape =
      "SELECT sc.cust, sc.rev FROM "
      "(SELECT store_sk, AVG(rev) AS ave FROM "
      "  (SELECT store_sk, cust, SUM(amt) AS rev FROM sr_null GROUP BY "
      "store_sk, cust) sa "
      " GROUP BY store_sk) sb, "
      "(SELECT store_sk, cust, SUM(amt) AS rev FROM sr_null GROUP BY store_sk, "
      "cust) sc "
      "WHERE sb.store_sk = sc.store_sk AND sc.rev <= 0.5 * sb.ave "
      "ORDER BY sc.cust";
  EXPECT_EQ(run_rows(false, q65_shape), run_rows(true, q65_shape));
  ASSERT_EQ(run_rows(false, q65_shape).size(), 1U);
  EXPECT_EQ(run_rows(false, q65_shape)[0][0], Value(int64_t{2}));

  // Q1 / Q30 / Q81 correlated CTE scalar AVG*1.2 shape:
  const std::string q1_shape =
      "WITH ctr AS ("
      "  SELECT cust, store_sk, SUM(amt) AS total_ret "
      "  FROM sr_null GROUP BY cust, store_sk"
      ") "
      "SELECT ctr1.cust FROM ctr ctr1 "
      "WHERE ctr1.total_ret > ("
      "  SELECT AVG(ctr2.total_ret) * 1.2 FROM ctr ctr2 "
      "  WHERE ctr1.store_sk = ctr2.store_sk"
      ") ORDER BY ctr1.cust";
  EXPECT_EQ(run_rows(false, q1_shape), run_rows(true, q1_shape));
  ASSERT_EQ(run_rows(false, q1_shape).size(), 1U);
  EXPECT_EQ(run_rows(false, q1_shape)[0][0], Value(int64_t{1}));
}

TEST_F(SelectRewritesTest, TpcdsUnionCtePivotZeroPaddedAndFullOuterIndicators) {
  for (const char* ddl : {
           "CREATE TABLE cust_pk (cid STRING PRIMARY KEY, cname STRING);",
           "CREATE TABLE s_fact (cid STRING, yr INT64, amt INT64);",
           "CREATE TABLE w_fact (cid STRING, yr INT64, amt INT64);",
       }) {
    ASSERT_TRUE(engine_->Execute(*ctx_, ddl).HasValue())
        << engine_->LastError();
  }
  for (const char* dml : {
           "INSERT INTO cust_pk VALUES ('c1', 'Alice'), ('c2', 'Bob'), ('c3', "
           "'Cara');",
           // c1 has both 1999 & 2000; c2 has ONLY 1999 (missing-year trap!);
           // NULL cid row tests `self_join_pivot_null_key_bug`!
           "INSERT INTO s_fact VALUES ('c1', 1999, 100), ('c1', 2000, 120), "
           "('c2', 1999, 50), (NULL, 1999, 10), (NULL, 2000, 20);",
           "INSERT INTO w_fact VALUES ('c1', 1999, 100), ('c1', 2000, 200), "
           "('c2', 1999, 50), ('c2', 2000, 0), (NULL, 1999, 10), (NULL, 2000, "
           "30);",
       }) {
    auto r = engine_->Execute(*ctx_, dml);
    ASSERT_TRUE(r.HasValue()) << engine_->LastError();
    r.Value().Drain();
    ASSERT_EQ(r.Value().GetStatus(), Status::kSuccess);
  }

  auto run_rows = [&](bool fallback, const std::string& sql) {
    engine_->SetForceRelationalFallback(fallback);
    auto r = engine_->Execute(*ctx_, sql);
    EXPECT_TRUE(r.HasValue()) << engine_->LastError();
    std::vector<Row> out;
    if (r.HasValue()) {
      Row row;
      while (r.Value().Next(&row)) {
        out.push_back(row);
        row = Row();
      }
      EXPECT_EQ(r.Value().GetStatus(), Status::kSuccess) << sql;
    }
    engine_->SetForceRelationalFallback(false);
    return out;
  };

  // 1) Q4 / Q11 / Q74 union CTE specialization + conditional pivot (including
  //    `1999 + 1` arithmetic constant and missing-year customer `'c2'`!):
  const std::string q74_shape =
      "WITH yt AS ("
      "  SELECT c.cid, c.cname, s.yr, SUM(s.amt) AS total, 's' AS stype "
      "  FROM cust_pk c, s_fact s WHERE c.cid = s.cid GROUP BY c.cid, c.cname, "
      "s.yr "
      "  UNION ALL "
      "  SELECT c.cid, c.cname, w.yr, SUM(w.amt) AS total, 'w' AS stype "
      "  FROM cust_pk c, w_fact w WHERE c.cid = w.cid GROUP BY c.cid, c.cname, "
      "w.yr"
      ") "
      "SELECT s2.cid, s2.cname "
      "FROM yt s1, yt s2, yt w1, yt w2 "
      "WHERE s2.cid = s1.cid AND s1.cid = w2.cid AND s1.cid = w1.cid "
      "  AND s1.stype = 's' AND s2.stype = 's' AND w1.stype = 'w' AND w2.stype "
      "= 'w' "
      "  AND s1.yr = 1999 AND s2.yr = 1999 + 1 AND w1.yr = 1999 AND w2.yr = "
      "1999 + 1 "
      "  AND s1.total > 0 AND w1.total > 0 "
      "  AND w2.total * s1.total >= s2.total * w1.total "
      "ORDER BY s2.cid";
  EXPECT_EQ(run_rows(false, q74_shape), run_rows(true, q74_shape));
  ASSERT_EQ(run_rows(false, q74_shape).size(), 1U);
  EXPECT_EQ(run_rows(false, q74_shape)[0][0], Value("c1"));

  // 2) Q97 FULL OUTER JOIN indicator sums with partial-NULL composite keys AND
  //    both-empty NULL result (`full_outer_indicator_partial_null_key_bug` &
  //    `full_outer_indicator_empty_null_bug`):
  for (const char* sql : {
           "CREATE TABLE foj_a (c INT64, i INT64);",
           "CREATE TABLE foj_b (c INT64, i INT64);",
           "INSERT INTO foj_a VALUES (1, 10), (1, 10), (2, NULL), (NULL, 30);",
           "INSERT INTO foj_b VALUES (1, 10), (2, NULL), (3, 40);",
       }) {
    auto r = engine_->Execute(*ctx_, sql);
    ASSERT_TRUE(r.HasValue()) << engine_->LastError();
    r.Value().Drain();
    ASSERT_EQ(r.Value().GetStatus(), Status::kSuccess);
  }
  const std::string q97_nonempty =
      "WITH ssci AS (SELECT c, i FROM foj_a GROUP BY c, i), "
      "     csci AS (SELECT c, i FROM foj_b GROUP BY c, i) "
      "SELECT "
      "  SUM(CASE WHEN ssci.c IS NOT NULL AND csci.c IS NULL THEN 1 ELSE 0 "
      "END), "
      "  SUM(CASE WHEN ssci.c IS NULL AND csci.c IS NOT NULL THEN 1 ELSE 0 "
      "END), "
      "  SUM(CASE WHEN ssci.c IS NOT NULL AND csci.c IS NOT NULL THEN 1 ELSE 0 "
      "END) "
      "FROM ssci FULL OUTER JOIN csci ON ssci.c = csci.c AND ssci.i = csci.i";
  EXPECT_EQ(run_rows(false, q97_nonempty), run_rows(true, q97_nonempty));

  const std::string q97_empty =
      "WITH ssci AS (SELECT c, i FROM foj_a WHERE c > 999 GROUP BY c, i), "
      "     csci AS (SELECT c, i FROM foj_b WHERE c > 999 GROUP BY c, i) "
      "SELECT "
      "  SUM(CASE WHEN ssci.c IS NOT NULL AND csci.c IS NULL THEN 1 ELSE 0 "
      "END), "
      "  SUM(CASE WHEN ssci.c IS NULL AND csci.c IS NOT NULL THEN 1 ELSE 0 "
      "END), "
      "  SUM(CASE WHEN ssci.c IS NOT NULL AND csci.c IS NOT NULL THEN 1 ELSE 0 "
      "END) "
      "FROM ssci FULL OUTER JOIN csci ON ssci.c = csci.c AND ssci.i = csci.i";
  EXPECT_EQ(run_rows(false, q97_empty), run_rows(true, q97_empty));
  ASSERT_EQ(run_rows(false, q97_empty).size(), 1U);
  EXPECT_TRUE(run_rows(false, q97_empty)[0][0].IsNull());

  // 3) Q5 zero-padded union + NULL sum preservation
  // (`zero_padded_union_naive_coalesce_bug`):
  for (const char* sql : {
           "CREATE TABLE zp_dim (sk INT64, name STRING);",
           "CREATE TABLE zp_sales (sk INT64, price INT64);",
           "CREATE TABLE zp_ret (sk INT64, loss INT64);",
           "INSERT INTO zp_dim VALUES (1, 's1'), (2, 's2');",
           "INSERT INTO zp_sales VALUES (1, NULL), (2, 50);",
           "INSERT INTO zp_ret VALUES (2, 15);",
       }) {
    auto r = engine_->Execute(*ctx_, sql);
    ASSERT_TRUE(r.HasValue()) << engine_->LastError();
    r.Value().Drain();
    ASSERT_EQ(r.Value().GetStatus(), Status::kSuccess);
  }
  const std::string q5_shape =
      "SELECT d.name, SUM(u.price), SUM(u.loss) "
      "FROM (SELECT sk, price, 0 AS loss FROM zp_sales "
      "      UNION ALL "
      "      SELECT sk, 0 AS price, loss FROM zp_ret) u, zp_dim d "
      "WHERE u.sk = d.sk GROUP BY d.name ORDER BY d.name";
  EXPECT_EQ(run_rows(false, q5_shape), run_rows(true, q5_shape));
  ASSERT_EQ(run_rows(false, q5_shape).size(), 2U);
  // Store 's1' has ONLY `(1, NULL, 0)` from zp_sales and NO rows in zp_ret ->
  // SUM(price) MUST stay NULL (not 0!), while SUM(loss) = 0!
  EXPECT_TRUE(run_rows(false, q5_shape)[0][1].IsNull());
  EXPECT_EQ(run_rows(false, q5_shape)[0][2], Value(int64_t{0}));

  // 4) Q78 intervening inner join pulled before LEFT JOIN:
  const std::string q78_shape =
      "SELECT d.name, SUM(s.price) "
      "FROM zp_sales s LEFT JOIN zp_ret r ON s.sk = r.sk "
      "JOIN zp_dim d ON s.sk = d.sk "
      "WHERE r.sk IS NULL GROUP BY d.name";
  EXPECT_EQ(run_rows(false, q78_shape), run_rows(true, q78_shape));
}

TEST_F(SelectRewritesTest,
       ClickBenchPruneRedundantGroupByKeysAndEmptyTableSafety) {
  auto q35_stmt = ParseSelect(
      "SELECT 1, name, COUNT(*) AS c FROM items GROUP BY 1, name, name "
      "ORDER BY c DESC LIMIT 10");
  ASSERT_NE(q35_stmt, nullptr);
  ASSERT_EQ(q35_stmt->GroupBy().size(), 3U);
  EXPECT_TRUE(PruneRedundantGroupByKeys(q35_stmt.get()));
  EXPECT_EQ(q35_stmt->GroupBy().size(), 1U);

  // All-constant GROUP BY (`GROUP BY 1`) must NOT be reduced to keyless
  // `GROUP BY`, or an empty table would emit 1 row instead of 0 rows!
  auto all_const = ParseSelect("SELECT 1, COUNT(*) FROM items GROUP BY 1");
  ASSERT_NE(all_const, nullptr);
  EXPECT_FALSE(PruneRedundantGroupByKeys(all_const.get()));
  EXPECT_EQ(all_const->GroupBy().size(), 1U);

  auto r = engine_->Execute(*ctx_, "CREATE TABLE cb_empty (x STRING);");
  ASSERT_TRUE(r.HasValue()) << engine_->LastError();
  r.Value().Drain();
  ASSERT_EQ(r.Value().GetStatus(), Status::kSuccess);
  auto res =
      engine_->Execute(*ctx_, "SELECT 1, COUNT(*) FROM cb_empty GROUP BY 1");
  ASSERT_TRUE(res.HasValue()) << engine_->LastError();
  EXPECT_EQ(res.Value().Collect().size(), 0U);
}

TEST_F(SelectRewritesTest, TpchSf10DiscoveryRulesAndAllSemanticTraps) {
  for (const char* sql : {
           "CREATE TABLE tpch_n (n_key INT64 PRIMARY KEY, n_name STRING);",
           "CREATE TABLE tpch_c (c_key INT64 PRIMARY KEY, c_nkey INT64, "
           "c_name STRING, c_addr STRING, c_comment STRING);",
           "CREATE TABLE tpch_c_nullable (c_key INT64, c_name STRING);",
           "CREATE TABLE tpch_o (o_key INT64 PRIMARY KEY, o_ckey INT64, "
           "o_total FLOAT64, o_comment STRING);",
           "CREATE TABLE tpch_p (p_key INT64 PRIMARY KEY, p_brand STRING, "
           "p_size INT64, p_name STRING);",
           "CREATE TABLE tpch_s (s_key INT64 PRIMARY KEY, s_nkey INT64, "
           "s_name STRING);",
           "CREATE TABLE tpch_ps (ps_pkey INT64, ps_skey INT64, "
           "ps_avail INT64, ps_cost FLOAT64);",
           "CREATE TABLE tpch_l (l_okey INT64, l_pkey INT64, l_skey INT64, "
           "l_qty FLOAT64, l_price FLOAT64, l_disc FLOAT64, "
           "l_flag STRING, l_mode STRING);",
           "INSERT INTO tpch_n VALUES (1, 'PERU'), (2, 'JAPAN');",
           // Note: customer 3 references non-existent nation 999 (Trap 5 FK
           // test)
           "INSERT INTO tpch_c VALUES "
           "(10, 1, 'Cust10', 'Addr10', 'Com10'), "
           "(20, 2, 'Cust20', 'Addr20', 'Com20'), "
           "(30, 1, 'Cust30', 'Addr30', 'Com30'), "
           "(40, 999, 'OrphanNationCust', 'Addr40', 'Com40');",
           // Trap 2a table: duplicate NULLs + duplicate non-null keys
           "INSERT INTO tpch_c_nullable VALUES "
           "(NULL, 'n1'), (NULL, 'n2'), (10, 'd1'), (10, 'd2'), (30, 'u1');",
           // Note: order 900 has orphan o_ckey=999 (not in tpch_c) with huge
           // revenue, and order 400 belongs to Cust40 (orphan nation 999)!
           "INSERT INTO tpch_o VALUES "
           "(100, 10, 500.0, 'normal order'), "
           "(101, 10, 300.0, 'unusual special packages here'), "
           "(200, 20, 400.0, 'fast delivery'), "
           "(400, 40, 8000.0, 'orphan nation order'), "
           "(900, 999, 9000.0, 'orphan customer order'), "
           "(901, NULL, 100.0, 'null custkey order');",
           "INSERT INTO tpch_p VALUES "
           "(1, 'Brand#53', 3, 'tan_alpha'), "
           "(2, 'Brand#41', 8, 'tan_beta'), "
           "(3, 'Brand#99', 20, 'other_gamma');",
           "INSERT INTO tpch_s VALUES "
           "(1, 1, 'Supp1'), (2, 1, 'Supp2'), (3, 2, 'Supp3');",
           "INSERT INTO tpch_ps VALUES "
           "(1, 1, 50, 10.0), (2, 2, 10, 20.0), (1, 2, 5, 15.0);",
           "INSERT INTO tpch_l VALUES "
           "(100, 1, 1, 8.0, 100.0, 0.10, 'R', 'AIR'), "
           "(100, 1, 1, 250.0, 200.0, 0.05, 'N', 'AIR'), "
           "(200, 2, 2, 18.0, 150.0, 0.00, 'R', 'AIR REG'), "
           "(400, 1, 1, 50.0, 5000.0, 0.00, 'R', 'AIR'), "
           "(900, 1, 1, 60.0, 9000.0, 0.00, 'R', 'AIR');",
       }) {
    auto r = engine_->Execute(*ctx_, sql);
    ASSERT_TRUE(r.HasValue()) << engine_->LastError();
    r.Value().Drain();
    ASSERT_EQ(r.Value().GetStatus(), Status::kSuccess);
  }

  auto run_rows = [&](bool fallback, const std::string& sql) {
    engine_->SetForceRelationalFallback(fallback);
    auto r = engine_->Execute(*ctx_, sql);
    EXPECT_TRUE(r.HasValue())
        << "SQL: " << sql << "\nErr: " << engine_->LastError();
    std::vector<Row> out;
    if (r.HasValue()) {
      Row row;
      while (r.Value().Next(&row)) {
        out.push_back(row);
        row = Row();
      }
      EXPECT_EQ(r.Value().GetStatus(), Status::kSuccess) << sql;
    }
    engine_->SetForceRelationalFallback(false);
    return out;
  };

  // --------------------------------------------------------------------------
  // TPCH-1: Common conjunct factoring + per-table disjunctive envelope + hull
  // --------------------------------------------------------------------------
  const std::string q19_sql =
      "SELECT SUM(l_price * (1.0 - l_disc)) FROM tpch_l, tpch_p "
      "WHERE p_key = l_pkey AND ("
      "  (p_key = l_pkey AND p_brand = 'Brand#53' AND l_qty >= 5 AND l_qty <= "
      "5 + 10 "
      "   AND p_size BETWEEN 1 AND 5 AND l_mode IN ('AIR', 'AIR REG')) "
      "  OR "
      "  (p_key = l_pkey AND p_brand = 'Brand#41' AND l_qty >= 15 AND l_qty <= "
      "15 + 10 "
      "   AND p_size BETWEEN 1 AND 10 AND l_mode IN ('AIR', 'AIR REG'))"
      ")";
  {
    auto parsed = ParseSelect(q19_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(FactorAndEnvelopeDisjunctiveWhere(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q19_sql), run_rows(true, q19_sql));

  // Trap 1 (`envelope_missing_branch_pred_drops_rows_bug`): Branch 2 has NO
  // `tpch_p` predicate, so synthesizing a `tpch_p` envelope from Branch 1 alone
  // would drop `Brand#99` rows matching Branch 2!
  const std::string trap1_sql =
      "SELECT COUNT(*) FROM tpch_l, tpch_p "
      "WHERE p_key = l_pkey AND ("
      "  (p_brand = 'Brand#53' AND l_qty = 8.0) "
      "  OR "
      "  (l_qty >= 18.0)"
      ")";
  EXPECT_EQ(run_rows(false, trap1_sql), run_rows(true, trap1_sql));

  // --------------------------------------------------------------------------
  // TPCH-2: Left-Join Right Pre-Aggregation & Traps 2a / 2b
  // --------------------------------------------------------------------------
  const std::string q13_sql =
      "SELECT c_count, COUNT(*) AS custdist FROM ("
      "  SELECT c_key, COUNT(o_key) AS c_count FROM tpch_c "
      "  LEFT OUTER JOIN tpch_o ON c_key = o_ckey "
      "    AND o_comment NOT LIKE '%unusual%packages%' "
      "  GROUP BY c_key"
      ") c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC";
  EXPECT_EQ(run_rows(false, q13_sql), run_rows(true, q13_sql));

  // Trap 2a (`nullable_unique_left_elides_groupby_bug`): duplicate NULLs and
  // duplicate keys on left MUST NOT elide GROUP BY!
  const std::string trap2a_sql =
      "SELECT c_key, COUNT(o_key) AS c_count FROM tpch_c_nullable "
      "LEFT OUTER JOIN tpch_o ON c_key = o_ckey "
      "  AND o_comment NOT LIKE '%unusual%packages%' "
      "GROUP BY c_key ORDER BY c_key";
  EXPECT_EQ(run_rows(false, trap2a_sql), run_rows(true, trap2a_sql));

  // Trap 2b (`left_join_count_nonstrict_expr_bug`): COUNT(*) and
  // COUNT(COALESCE(o_key, 0)) return 1 (not 0) on unmatched left rows!
  const std::string trap2b_star_sql =
      "SELECT c_key, COUNT(*) AS c_count FROM tpch_c "
      "LEFT OUTER JOIN tpch_o ON c_key = o_ckey GROUP BY c_key ORDER BY c_key";
  const std::string trap2b_coal_sql =
      "SELECT c_key, COUNT(COALESCE(o_key, 0)) AS c_count FROM tpch_c "
      "LEFT OUTER JOIN tpch_o ON c_key = o_ckey GROUP BY c_key ORDER BY c_key";
  {
    auto p1 = ParseSelect(trap2b_star_sql);
    auto p2 = ParseSelect(trap2b_coal_sql);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);
    EXPECT_FALSE(PreaggregateLeftJoinRightCounts(p1.get(), *ctx_));
    EXPECT_FALSE(PreaggregateLeftJoinRightCounts(p2.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, trap2b_star_sql), run_rows(true, trap2b_star_sql));
  EXPECT_EQ(run_rows(false, trap2b_coal_sql), run_rows(true, trap2b_coal_sql));

  // --------------------------------------------------------------------------
  // TPCH-3: Self-Semijoin HAVING Aggregate Absorption & Trap 3
  // --------------------------------------------------------------------------
  const std::string q18_sql =
      "SELECT c_name, c_key, o_key, o_total, SUM(l_qty) "
      "FROM tpch_c, tpch_o, tpch_l "
      "WHERE o_key IN (SELECT l_okey FROM tpch_l GROUP BY l_okey "
      "                HAVING SUM(l_qty) > 200.0) "
      "  AND c_key = o_ckey AND o_key = l_okey "
      "GROUP BY c_name, c_key, o_key, o_total ORDER BY o_total DESC LIMIT 10";
  {
    auto parsed = ParseSelect(q18_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(AbsorbSelfSemijoinHavingAggregate(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q18_sql), run_rows(true, q18_sql));

  // Trap 3 (`self_semijoin_mismatched_where_bug`): outer WHERE filters
  // `l_flag = 'R'` (qty=8 for o_key=100) while subquery HAVING sums ALL flags
  // (8 + 250 = 258 > 200)! Must NOT merge WHERE into HAVING subquery!
  const std::string trap3_sql =
      "SELECT c_name, o_key, SUM(l_qty) "
      "FROM tpch_c, tpch_o, tpch_l "
      "WHERE o_key IN (SELECT l_okey FROM tpch_l GROUP BY l_okey "
      "                HAVING SUM(l_qty) > 200.0) "
      "  AND c_key = o_ckey AND o_key = l_okey AND l_flag = 'R' "
      "GROUP BY c_name, o_key ORDER BY o_key";
  {
    auto parsed = ParseSelect(trap3_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_FALSE(AbsorbSelfSemijoinHavingAggregate(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, trap3_sql), run_rows(true, trap3_sql));
  ASSERT_EQ(run_rows(false, trap3_sql).size(), 1U);
  EXPECT_DOUBLE_EQ(run_rows(false, trap3_sql)[0][2].value.double_value, 8.0);

  // --------------------------------------------------------------------------
  // TPCH-4a: CTE Argmax Subquery -> Non-Null WITH TIES & Trap 4a + NaN guard
  // --------------------------------------------------------------------------
  // Add a tied revenue row for supplier 2 so both Supp1 and Supp2 tie at max!
  {
    auto ins = engine_->Execute(
        *ctx_,
        "INSERT INTO tpch_l VALUES (200, 2, 2, 1.0, 130.0, 0.0, 'R', 'AIR');");
    ASSERT_TRUE(ins.HasValue());
    ins.Value().Drain();
    ASSERT_EQ(ins.Value().GetStatus(), Status::kSuccess);
  }
  // Supp1 revenue: 100*0.9 + 200*0.95 + 5000 + 9000 = 14280; let's filter
  // `l_okey <= 200` where Supp1 = 280.0 and Supp2 = 150 + 130 = 280.0 (TIE!).
  const std::string q15_ties_sql =
      "WITH revenue AS ("
      "  SELECT l_skey AS supplier_no, SUM(l_price * (1.0 - l_disc)) AS "
      "total_rev "
      "  FROM tpch_l WHERE l_okey <= 200 GROUP BY l_skey"
      ") "
      "SELECT s_key, s_name, total_rev FROM tpch_s, revenue "
      "WHERE s_key = supplier_no "
      "  AND total_rev = (SELECT MAX(total_rev) FROM revenue) "
      "ORDER BY s_key";
  {
    auto parsed = ParseSelect(q15_ties_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(RewriteCteArgmaxSubqueryToTopTies(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q15_ties_sql), run_rows(true, q15_ties_sql));
  EXPECT_EQ(run_rows(false, q15_ties_sql).size(), 2U);

  // Trap 4a (`argmax_with_ties_without_is_not_null_bug`): all-NULL CTE column
  // MUST return 0 rows, not NULL ties!
  const std::string trap4a_all_null_sql =
      "WITH revenue AS ("
      "  SELECT s_key AS supplier_no, CAST(NULL AS FLOAT64) AS total_rev "
      "  FROM tpch_s"
      ") "
      "SELECT s_key FROM tpch_s, revenue "
      "WHERE s_key = supplier_no "
      "  AND total_rev = (SELECT MAX(total_rev) FROM revenue)";
  EXPECT_EQ(run_rows(false, trap4a_all_null_sql),
            run_rows(true, trap4a_all_null_sql));
  EXPECT_TRUE(run_rows(false, trap4a_all_null_sql).empty());

  // GoogleSQL NaN poisoning guard: when one CTE row has `NaN`, `MAX` is `NaN`
  // and `total_rev = NaN` is FALSE for every row -> 0 rows!
  const std::string trap4a_nan_sql =
      "WITH revenue AS ("
      "  SELECT s_key AS supplier_no, "
      "         CASE WHEN s_key = 1 THEN CAST('NaN' AS FLOAT64) ELSE 100.0 END "
      "           AS total_rev "
      "  FROM tpch_s"
      ") "
      "SELECT s_key FROM tpch_s, revenue "
      "WHERE s_key = supplier_no "
      "  AND total_rev = (SELECT MAX(total_rev) FROM revenue)";
  EXPECT_EQ(run_rows(false, trap4a_nan_sql), run_rows(true, trap4a_nan_sql));
  EXPECT_TRUE(run_rows(false, trap4a_nan_sql).empty());

  // --------------------------------------------------------------------------
  // TPCH-5: Late Unique-Dim Wide Attributes After Top-N & Trap 5 (Orphan FK)
  // --------------------------------------------------------------------------
  // Notice: o_key=900 (rev=9000, orphan o_ckey=999) and o_key=400 (rev=5000,
  // Cust40 with orphan c_nkey=999) MUST NOT steal `LIMIT 2` slots from valid
  // Cust20 (rev=280) and Cust10 (rev=90)!
  const std::string q10_trap5_sql =
      "SELECT c_key, c_name, SUM(l_price * (1.0 - l_disc)) AS revenue, "
      "       n_name, c_addr, c_comment "
      "FROM tpch_c, tpch_o, tpch_l, tpch_n "
      "WHERE c_key = o_ckey AND l_okey = o_key AND l_flag = 'R' "
      "  AND c_nkey = n_key "
      "GROUP BY c_key, c_name, n_name, c_addr, c_comment "
      "ORDER BY revenue DESC LIMIT 2";
  {
    auto parsed = ParseSelect(q10_trap5_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(DeferUniqueDimensionAttributesAfterTopN(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q10_trap5_sql), run_rows(true, q10_trap5_sql));
  ASSERT_EQ(run_rows(false, q10_trap5_sql).size(), 2U);
  EXPECT_EQ(run_rows(false, q10_trap5_sql)[0][0], Value(int64_t{20}));
  EXPECT_EQ(run_rows(false, q10_trap5_sql)[1][0], Value(int64_t{10}));

  // TPCH-5b: Transitive equijoin closure synthesis
  const std::string q9_sql =
      "SELECT n_name, SUM(l_price - ps_cost * l_qty) "
      "FROM tpch_p, tpch_s, tpch_l, tpch_ps, tpch_n "
      "WHERE s_key = l_skey AND ps_skey = l_skey "
      "  AND ps_pkey = l_pkey AND p_key = l_pkey AND s_nkey = n_key "
      "GROUP BY n_name ORDER BY n_name";
  {
    auto parsed = ParseSelect(q9_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(SynthesizeTransitiveEquijoins(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q9_sql), run_rows(true, q9_sql));

  // --------------------------------------------------------------------------
  // TPCH-6a: Correlated `> SUM` Decorrelation & Trap 6a (Kim's COUNT bug)
  // --------------------------------------------------------------------------
  // Note `(ps_pkey=1, ps_skey=2)` has `ps_avail=5` and 0 rows in `tpch_l`:
  // - For `0.5 * SUM(l_qty)`: `5 > NULL` is UNKNOWN -> rejected!
  // - For `COUNT(l_qty)` (Trap 6a): `5 > 0` is TRUE -> Supp2 MUST be included!
  const std::string corr_sum_top_sql =
      "SELECT s_name FROM tpch_s, tpch_n, tpch_ps, tpch_p "
      "WHERE s_key = ps_skey AND ps_pkey = p_key AND p_name LIKE 'tan%' "
      "  AND ps_avail > ("
      "    SELECT 0.5 * SUM(l_qty) FROM tpch_l "
      "    WHERE l_pkey = ps_pkey AND l_skey = ps_skey AND l_okey >= 100"
      "  ) AND s_nkey = n_key AND n_name = 'PERU' ORDER BY s_name";
  {
    auto parsed = ParseSelect(corr_sum_top_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(DecorrelateCorrelatedSumComparisons(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, corr_sum_top_sql),
            run_rows(true, corr_sum_top_sql));

  const std::string trap6a_count_top_sql =
      "SELECT s_name FROM tpch_s, tpch_n, tpch_ps, tpch_p "
      "WHERE s_key = ps_skey AND ps_pkey = p_key AND p_name LIKE 'tan%' "
      "  AND ps_avail > ("
      "    SELECT COUNT(l_qty) FROM tpch_l "
      "    WHERE l_pkey = ps_pkey AND l_skey = ps_skey AND l_okey >= 9999"
      "  ) AND s_nkey = n_key AND n_name = 'PERU' ORDER BY s_name";
  {
    auto parsed = ParseSelect(trap6a_count_top_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_FALSE(DecorrelateCorrelatedSumComparisons(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, trap6a_count_top_sql),
            run_rows(true, trap6a_count_top_sql));
  EXPECT_EQ(run_rows(false, trap6a_count_top_sql).size(), 3U);

  // Inside a `WHERE ... IN (SELECT ...)` semi-join, outer domain filters
  // (`n_name = 'PERU'`) live outside the `IN` subquery, so `TPCH-6a` leaves the
  // `IN` subquery to `subquery_runtime` while preserving exact answers.
  const std::string q20_sql =
      "SELECT s_name FROM tpch_s, tpch_n "
      "WHERE s_key IN ("
      "  SELECT ps_skey FROM tpch_ps, tpch_p "
      "  WHERE ps_pkey = p_key AND p_name LIKE 'tan%' "
      "    AND ps_avail > ("
      "      SELECT 0.5 * SUM(l_qty) FROM tpch_l "
      "      WHERE l_pkey = ps_pkey AND l_skey = ps_skey AND l_okey >= 100"
      "    )"
      ") AND s_nkey = n_key AND n_name = 'PERU' ORDER BY s_name";
  {
    auto parsed = ParseSelect(q20_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_FALSE(DecorrelateCorrelatedSumComparisons(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q20_sql), run_rows(true, q20_sql));
}

TEST_F(SelectRewritesTest, ClickBenchRound2AffineLinearSumsAndTraps) {
  for (const char* sql : {
           "CREATE TABLE cb_affine (grp INT64, v INT64, w INT64);",
           "INSERT INTO cb_affine VALUES "
           "(1, 10, 100), (1, NULL, 200), (1, 20, 300), "
           "(2, NULL, NULL), (2, NULL, 50);",
           "CREATE TABLE cb_affine_ovf (v INT64);",
           "INSERT INTO cb_affine_ovf VALUES (9223372036854775807), (0);",
       }) {
    auto r = engine_->Execute(*ctx_, sql);
    ASSERT_TRUE(r.HasValue()) << engine_->LastError();
    r.Value().Drain();
    ASSERT_EQ(r.Value().GetStatus(), Status::kSuccess);
  }
  ASSERT_EQ(ctx_->txn_.PreCommit(), Status::kSuccess);
  ctx_ = std::make_unique<TransactionContext>(db_->BeginReadOnlyContext());

  auto run_rows = [&](bool fallback, const std::string& sql,
                      std::vector<std::string>* col_names = nullptr) {
    engine_->SetForceRelationalFallback(fallback);
    auto r = engine_->Execute(*ctx_, sql);
    EXPECT_TRUE(r.HasValue())
        << "SQL: " << sql << "\nErr: " << engine_->LastError();
    if (col_names != nullptr) {
      *col_names = engine_->ResultColumnNames();
    }
    std::vector<Row> out;
    if (r.HasValue()) {
      Row row;
      while (r.Value().Next(&row)) {
        out.push_back(row);
        row = Row();
      }
      EXPECT_EQ(r.Value().GetStatus(), Status::kSuccess) << sql;
    }
    engine_->SetForceRelationalFallback(false);
    return out;
  };

  // 1. CB-9 (`affine_sum_homomorphism_sound`) on PAX `LSTableStore`:
  //    Multiple `SUM(v ± c)` + `SUM(c + v)` share a single `SUM(v)` +
  //    `COUNT(v)` while preserving NULL ignoring (`v = NULL` does not add `c`)
  //    and output column names.
  const std::string q30_shape_sql =
      "SELECT SUM(v + 1), SUM(v + 2), SUM(v - 5), SUM(10 + v) AS s4, "
      "SUM(w + 3) FROM cb_affine";
  {
    auto parsed = ParseSelect(q30_shape_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(RewriteAffineLinearSums(parsed.get(), *ctx_));
  }
  std::vector<std::string> fast_names;
  std::vector<std::string> ref_names;
  const auto fast_q30 = run_rows(false, q30_shape_sql, &fast_names);
  const auto ref_q30 = run_rows(true, q30_shape_sql, &ref_names);
  ASSERT_EQ(fast_q30.size(), 1U);
  // v has non-null (10, 20): SUM(v)=30, COUNT(v)=2 -> 32, 34, 20, 50;
  // w has non-null (100, 200, 300, 50): SUM(w)=650, COUNT(w)=4 -> 662.
  EXPECT_EQ(fast_q30[0][0], Value(int64_t{32}));
  EXPECT_EQ(fast_q30[0][1], Value(int64_t{34}));
  EXPECT_EQ(fast_q30[0][2], Value(int64_t{20}));
  EXPECT_EQ(fast_q30[0][3], Value(int64_t{50}));
  EXPECT_EQ(fast_q30[0][4], Value(int64_t{662}));
  EXPECT_EQ(fast_q30, ref_q30);
  EXPECT_EQ(fast_names, ref_names);

  // 2. Trap `affine_sum_coalesce_empty_bug`:
  //    Empty / filtered-empty / all-NULL groups MUST produce `NULL` (never 0!).
  const std::string empty_filter_sql =
      "SELECT SUM(v + 1), SUM(v - 2) FROM cb_affine WHERE grp = 999";
  const auto fast_empty = run_rows(false, empty_filter_sql);
  ASSERT_EQ(fast_empty.size(), 1U);
  EXPECT_TRUE(fast_empty[0][0].IsNull());
  EXPECT_TRUE(fast_empty[0][1].IsNull());
  EXPECT_EQ(fast_empty, run_rows(true, empty_filter_sql));

  const std::string all_null_grouped_sql =
      "SELECT grp, SUM(v + 3), SUM(w - 10) FROM cb_affine "
      "GROUP BY grp ORDER BY grp ASC";
  const auto fast_grp = run_rows(false, all_null_grouped_sql);
  ASSERT_EQ(fast_grp.size(), 2U);
  EXPECT_EQ(fast_grp[0][1], Value(int64_t{36}));
  EXPECT_TRUE(fast_grp[1][1].IsNull()) << "All-NULL grp=2 must stay NULL";
  EXPECT_EQ(fast_grp[1][2], Value(int64_t{40}));
  EXPECT_EQ(fast_grp, run_rows(true, all_null_grouped_sql));

  // 3. Trap `affine_sum_overflow_masked_bug`:
  //    When `v + 1` can overflow `INT64_MAX`, `RewriteAffineLinearSums` MUST
  //    decline so per-row checked evaluation still raises overflow!
  const std::string ovf_sql = "SELECT SUM(v + 1) FROM cb_affine_ovf";
  {
    auto parsed = ParseSelect(ovf_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_FALSE(RewriteAffineLinearSums(parsed.get(), *ctx_));
  }
  {
    auto prep = engine_->Prepare(*ctx_, ovf_sql);
    ASSERT_TRUE(prep.HasValue()) << prep.GetStatus();
    Row row;
    while (prep.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prep.Value()->GetStatus(), Status::kSuccess);
  }

  // 4. Compact `cb_affine` to `.tlcs` and verify `.tlcs` `DirGroup` bounds
  //    certify `RewriteAffineLinearSums` identically.
  ctx_.reset();
  {
    TransactionContext cctx = db_->BeginContext();
    ASSERT_EQ(db_->CompactTableToTlcs(cctx, "cb_affine"), Status::kSuccess);
    ASSERT_EQ(cctx.txn_.PreCommit(), Status::kSuccess);
  }
  ctx_ = std::make_unique<TransactionContext>(db_->BeginReadOnlyContext());
  {
    auto parsed = ParseSelect(q30_shape_sql);
    ASSERT_NE(parsed, nullptr);
    EXPECT_TRUE(RewriteAffineLinearSums(parsed.get(), *ctx_));
  }
  EXPECT_EQ(run_rows(false, q30_shape_sql), ref_q30);
  EXPECT_EQ(run_rows(false, empty_filter_sql), fast_empty);
  EXPECT_EQ(run_rows(false, all_null_grouped_sql), fast_grp);
}

}  // namespace
}  // namespace tinylamb
