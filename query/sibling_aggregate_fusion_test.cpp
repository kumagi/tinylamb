/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

// Order-insensitive multiset comparison over the statement's textual form:
// the fusion must not change which rows come out, only how they are produced.
void ExpectSameMultiset(const std::vector<Row>& left,
                        const std::vector<Row>& right) {
  ASSERT_EQ(left.size(), right.size());
  std::vector<std::string> left_text;
  std::vector<std::string> right_text;
  left_text.reserve(left.size());
  right_text.reserve(right.size());
  for (const Row& row : left) {
    left_text.push_back(row.ToString());
  }
  for (const Row& row : right) {
    right_text.push_back(row.ToString());
  }
  std::sort(left_text.begin(), left_text.end());
  std::sort(right_text.begin(), right_text.end());
  EXPECT_EQ(left_text, right_text);
}

}  // namespace

class SiblingAggregateFusionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sibling-fusion-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    // Groups: g=1 has a NULL v row, g=3 has no row above the fusion member's
    // WHERE threshold, g=2 mixes both.  The fusion's witness conjunct and
    // NULL-skipping aggregates are both observable through this shape.
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE events (g INT64, v INT64);");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO events VALUES (1, NULL), (1, 4), (2, 10), (2, 20), "
           "(2, NULL), (3, 1), (4, 99);");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SiblingAggregateFusionTest, PlainSiblingsFuseToOneAggregate) {
  const auto fused =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.n, b.total FROM "
             "(SELECT g, COUNT(*) AS n FROM events GROUP BY g) a "
             "JOIN (SELECT g, SUM(v) AS total FROM events GROUP BY g) b "
             "ON a.g = b.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 1U);
  // Ground truth: the same aggregation written as a single scan.
  ExpectSameMultiset(
      fused, RunSql(engine_.get(), context_.get(),
                    "SELECT g, COUNT(*), SUM(v) FROM events GROUP BY g;"));
}

TEST_F(SiblingAggregateFusionTest, MemberWhereLiftsWithAWitnessConjunct) {
  // The filtered member's WHERE must (a) zero its own COUNT where nothing
  // matched and (b) drop groups the filtered member never saw -- here g=3
  // (v=1 only) has no row with v > 4, so the inner join must lose it even
  // though the unfiltered member b still aggregates it.
  const auto fused =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.cnt, b.mx FROM "
             "(SELECT g, COUNT(*) AS cnt FROM events WHERE v > 4 GROUP BY g) a "
             "JOIN (SELECT g, MAX(v) AS mx FROM events GROUP BY g) b "
             "ON a.g = b.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 1U);
  // Hand-computed: g=1 (NULL,4) has no row > 4 -> dropped by a's witness;
  // g=2 has 10,20 -> (2, 2, 20); g=4 has 99 -> (4, 1, 99).
  const std::vector<Row> expected = {{Value(2), Value(2), Value(20)},
                                     {Value(4), Value(1), Value(99)}};
  ExpectSameMultiset(fused, expected);
}

TEST_F(SiblingAggregateFusionTest, FilteredMemberAloneDropsItsEmptyGroups) {
  const auto fused = RunSql(
      engine_.get(), context_.get(),
      "SELECT a.g, a.cnt FROM "
      "(SELECT g, COUNT(*) AS cnt FROM events WHERE v > 50 GROUP BY g) a "
      "JOIN (SELECT g, SUM(v) AS total FROM events GROUP BY g) b "
      "ON a.g = b.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 1U);
  // Only g=4 (v=99) survives the threshold.
  const std::vector<Row> expected = {{Value(4), Value(1)}};
  ExpectSameMultiset(fused, expected);
}

TEST_F(SiblingAggregateFusionTest, AllNullGroupKeepsCountZeroAndNullSum) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE mixed (g INT64, v INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO mixed VALUES (7, NULL), (7, NULL);");
  const auto fused =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.c, b.s FROM "
             "(SELECT g, COUNT(*) AS c FROM mixed GROUP BY g) a "
             "JOIN (SELECT g, SUM(v) AS s FROM mixed GROUP BY g) b "
             "ON a.g = b.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 1U);
  ASSERT_EQ(fused.size(), 1U);
  EXPECT_EQ(fused[0][0], Value(7));
  EXPECT_EQ(fused[0][1], Value(2));
  EXPECT_TRUE(fused[0][2].IsNull());
}

TEST_F(SiblingAggregateFusionTest, HavingLiftsToTheOuterScope) {
  const auto fused =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.c, b.m FROM "
             "(SELECT g, COUNT(*) AS c FROM events GROUP BY g "
             " HAVING COUNT(*) > 1) a "
             "JOIN (SELECT g, MAX(v) AS m FROM events GROUP BY g) b "
             "ON a.g = b.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 1U);
  // Groups with more than one row: g=1 (2 rows) and g=2 (3 rows).
  const std::vector<Row> expected = {{Value(1), Value(2), Value(4)},
                                     {Value(2), Value(3), Value(20)}};
  ExpectSameMultiset(fused, expected);
}

TEST_F(SiblingAggregateFusionTest, DifferentGroupKeysStayUnfused) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE dim (h INT64, w INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO dim VALUES (1, 5), (2, 6), (2, 7);");
  const auto unfused =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.n, b.wsum FROM "
             "(SELECT g, COUNT(*) AS n FROM events GROUP BY g) a "
             "JOIN (SELECT h, SUM(w) AS wsum FROM dim GROUP BY h) b "
             "ON a.g = b.h;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 0U);
  // Different base tables: plain join of the two aggregates.
  const std::vector<Row> expected = {{Value(1), Value(2), Value(5)},
                                     {Value(2), Value(3), Value(13)}};
  ExpectSameMultiset(unfused, expected);
}

TEST_F(SiblingAggregateFusionTest, StarInOuterScopeAbortsFusion) {
  const auto result =
      RunSql(engine_.get(), context_.get(),
             "SELECT * FROM "
             "(SELECT g, COUNT(*) AS n FROM events GROUP BY g) a "
             "JOIN (SELECT g, SUM(v) AS total FROM events GROUP BY g) b "
             "ON a.g = b.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 0U);
  // Correctness never depends on the fusion: same rows as the plain join,
  // spelled with the star's column order (a.g, a.n, b.g, b.total).
  ExpectSameMultiset(
      result,
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.n, b.g, b.total FROM "
             "(SELECT g, COUNT(*) AS n FROM events GROUP BY g) a "
             "JOIN (SELECT g, SUM(v) AS total FROM events GROUP BY g) b "
             "ON a.g = b.g;"));
}

TEST_F(SiblingAggregateFusionTest, SingleSiblingNeverFuses) {
  const auto alone =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.n FROM "
             "(SELECT g, COUNT(*) AS n FROM events GROUP BY g) a;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 0U);
  ExpectSameMultiset(alone,
                     RunSql(engine_.get(), context_.get(),
                            "SELECT g, COUNT(*) FROM events GROUP BY g;"));
}

TEST_F(SiblingAggregateFusionTest, ThreeSiblingsFuseAndKeepEveryAggregate) {
  const auto fused =
      RunSql(engine_.get(), context_.get(),
             "SELECT a.g, a.n, b.total, c.mn FROM "
             "(SELECT g, COUNT(*) AS n FROM events GROUP BY g) a "
             "JOIN (SELECT g, SUM(v) AS total FROM events GROUP BY g) b "
             "ON a.g = b.g "
             "JOIN (SELECT g, MIN(v) AS mn FROM events GROUP BY g) c "
             "ON a.g = c.g;");
  EXPECT_EQ(engine_->LastFusedSiblingAggregates(), 1U);
  ExpectSameMultiset(fused,
                     RunSql(engine_.get(), context_.get(),
                            "SELECT g, COUNT(*), SUM(v), MIN(v) FROM events "
                            "GROUP BY g;"));
}

}  // namespace tinylamb
