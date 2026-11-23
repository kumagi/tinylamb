/**
 * Rule D: shared-CTE disjunctive restriction pushdown for GROUP BY bodies.
 *
 * The pushed WHERE is evaluated before GROUP BY, so only restrictions whose
 * columns are grouping keys may be pushed.  Aggregate outputs must keep the
 * body unfiltered.
 *
 * Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
 *   (shared_cte_disjunct_pushdown_sound, cte_volatile_inline_unsound)
 */

#include "query/query_test.hpp"

namespace tinylamb {

TEST_F(QueryTest, SharedGroupedCteTakesGroupingKeyRestriction) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE gd_t (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO gd_t VALUES (1, 10), (1, 20), (2, 30), (3, 40);");
  RunSql(ctx, *db_, "ANALYZE gd_t;");

  SqlEngine engine(*db_);
  const std::string sql =
      "WITH c AS (SELECT k, SUM(v) AS total FROM gd_t GROUP BY k) "
      "SELECT x.total, y.total FROM c x JOIN c y ON x.k = y.k "
      "WHERE x.k = 1 AND y.k = 1;";
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  ASSERT_EQ(prepared.GetStatus(), Status::kSuccess)
      << sql << "\n" << engine.LastError();
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 1U);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(30), Value(30)}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SharedGroupedCteRefusesAggregateOutputRestriction) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE gd_t (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO gd_t VALUES (1, 10), (1, 20), (2, 30), (3, 40);");
  RunSql(ctx, *db_, "ANALYZE gd_t;");

  SqlEngine engine(*db_);
  const std::string sql =
      "WITH c AS (SELECT k, SUM(v) AS total FROM gd_t GROUP BY k) "
      "SELECT x.k, y.k FROM c x JOIN c y ON x.k = y.k "
      "WHERE x.total > 25 AND y.total > 25;";
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  ASSERT_EQ(prepared.GetStatus(), Status::kSuccess)
      << sql << "\n" << engine.LastError();
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 0U);
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(2)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(3)}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

}  // namespace tinylamb
