/**
 * Regression tests for rule B: reordering a safe inner-join prefix in front of
 * a trailing LEFT JOIN, including USING joins.
 *
 * Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
 *   (using_join_inner_prefix_reorder_and_filter_sound,
 *    outer_and_using_join_prefix_reorder_sound)
 */

#include "query/query_test.hpp"

namespace tinylamb {

TEST_F(QueryTest, InnerPrefixPullupFiresForSafeUsingJoin) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ba (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE bb (k INT64, w INT64);");
  // `bc` deliberately has no `k`: bb's USING(k) cannot read the moved source.
  RunSql(ctx, *db_, "CREATE TABLE bc (cid INT64, u INT64);");
  RunSql(ctx, *db_, "INSERT INTO ba VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO bb VALUES (1, 100), (2, 200);");
  RunSql(ctx, *db_, "INSERT INTO bc VALUES (7, 100);");

  SqlEngine engine(*db_);
  const std::string sql =
      "SELECT k, v, w, u FROM ba LEFT JOIN bb USING(k) "
      "JOIN bc ON bc.u = 100 ORDER BY k;";
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  ASSERT_EQ(prepared.GetStatus(), Status::kSuccess)
      << sql << "\n" << engine.LastError();
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(engine.LastInnerPrefixPullups(), 1U);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(10), Value(100), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(20), Value(200), Value(100)}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, InnerPrefixPullupRefusesUsingColumnReadAcrossSwap) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ba (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE bb (k INT64, w INT64);");
  // `bc.k` makes bb's USING(k) read the source that would be moved in front.
  RunSql(ctx, *db_, "CREATE TABLE bc (k INT64, u INT64);");
  RunSql(ctx, *db_, "INSERT INTO ba VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO bb VALUES (1, 100), (2, 200);");
  RunSql(ctx, *db_, "INSERT INTO bc VALUES (1, 100);");

  SqlEngine engine(*db_);
  // Bare `k` would be ambiguous between the coalesced USING column and bc.k,
  // so select only the unambiguous columns here.
  const std::string sql =
      "SELECT v, w, u FROM ba LEFT JOIN bb USING(k) "
      "JOIN bc ON bc.u = 100 ORDER BY v;";
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  ASSERT_EQ(prepared.GetStatus(), Status::kSuccess)
      << sql << "\n" << engine.LastError();
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(engine.LastInnerPrefixPullups(), 0U);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(100), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(20), Value(200), Value(100)}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

}  // namespace tinylamb
