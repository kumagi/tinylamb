/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// End-to-end contract of the clustered-index-order claim a full scan reports
// (plan/full_scan_plan.hpp): a provably clustered heap satisfies an ascending
// ORDER BY on its key, so the engine must not sort -- and must start sorting
// again the moment the proof stops holding, without ever returning rows in the
// wrong order.
//
// The queries deliberately carry a predicate the primary-key index cannot
// serve, so the plan really is a full scan; an ordered index scan would answer
// the same ORDER BY and hide the behavior under test.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "index/index_schema.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::vector<Row> RunSql(TransactionContext& context, Database& database,
                        std::string_view sql) {
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

// Concatenated EXPLAIN output, one string per rendered line.
std::string Explain(TransactionContext& context, Database& database,
                    std::string_view sql) {
  std::string plan;
  for (const Row& row : RunSql(context, database, sql)) {
    EXPECT_EQ(row.values_.size(), 1U);
    plan += row[0].AsString();
    plan += "\n";
  }
  return plan;
}

std::vector<int64_t> IntColumn(const std::vector<Row>& rows, size_t index) {
  std::vector<int64_t> values;
  values.reserve(rows.size());
  for (const Row& row : rows) {
    values.push_back(row[index].value.int_value);
  }
  return values;
}

bool IsAscending(const std::vector<int64_t>& values) {
  return std::is_sorted(values.begin(), values.end());
}

std::string InsertRows(std::string_view table, int64_t first, int64_t last) {
  std::string sql = "INSERT INTO " + std::string(table) + " (id, tag) VALUES ";
  for (int64_t i = first; i < last; ++i) {
    if (i > first) {
      sql += ", ";
    }
    sql += "(" + std::to_string(i) + ", 'tag" + std::to_string(i) + "')";
  }
  return sql + ";";
}

class SqlClusteredOrderTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_clustered_order_test-" + RandomString())
                    .MoveValue();
    TransactionContext setup = database_->BeginContext();
    RunSql(setup, *database_,
           "CREATE TABLE clustered (id INT64 PRIMARY KEY, tag STRING(MAX));");
    RunSql(setup, *database_, InsertRows("clustered", 0, 20));
    ASSERT_SUCCESS(setup.PreCommit());
  }

  void TearDown() override {
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
  }

  void Analyze() const {
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_, "ANALYZE clustered;");
    ASSERT_SUCCESS(ctx.PreCommit());
  }

  // ORDER BY the clustered key behind a predicate no index can serve.
  static constexpr std::string_view kScan = R"(
      SELECT id, tag FROM clustered WHERE tag LIKE '%ag%' ORDER BY id)";

  std::string ExplainScan(TransactionContext& ctx) const {
    return Explain(ctx, *database_, "EXPLAIN " + std::string(kScan) + ";");
  }

  std::vector<int64_t> ScanIds(TransactionContext& ctx) const {
    return IntColumn(RunSql(ctx, *database_, std::string(kScan) + ";"), 0);
  }

  // Waits (bounded) for the background GC worker to drop the committed
  // version chains, which is what arms the index-only visibility map.
  void WaitForChainFreePages() const {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (database_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "background GC did not drop version chains";
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  std::unique_ptr<Database> database_;
};

}  // namespace

TEST_F(SqlClusteredOrderTest, ClusteredHeapElidesTheSortAndStaysOrdered) {
  Analyze();
  auto ctx = database_->BeginContext();
  const std::string plan = ExplainScan(ctx);
  EXPECT_NE(plan.find("FullScan"), std::string::npos) << plan;
  EXPECT_NE(plan.find("clustered order: id ASC"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("Sort"), std::string::npos) << plan;
  const std::vector<int64_t> ids = ScanIds(ctx);
  EXPECT_EQ(ids.size(), 20U);
  EXPECT_TRUE(IsAscending(ids)) << "scan order must satisfy ORDER BY";
  EXPECT_EQ(ids.front(), 0);
  EXPECT_EQ(ids.back(), 19);
  ASSERT_SUCCESS(ctx.PreCommit());
}

// D-9: above the parallel-scan row threshold the clustered-order proof used
// to be dropped (a morsel-parallel scan was assumed to hand chunks out in
// worker-completion order).  The morsel-ordered handoff delivers stored-chain
// order, so the proof survives: the plan keeps ParallelScan, refuses row
// filter pushdown (deferred-row parking would reorder), and the output stays
// ascending without a Sort node.
TEST_F(SqlClusteredOrderTest, ParallelScanKeepsClusteredOrderAboveThreshold) {
  // Fill past kParallelScanMinRows (8192) in chain order.
  TransactionContext load = database_->BeginContext();
  for (int64_t batch = 0; batch < 6; ++batch) {
    RunSql(load, *database_,
           InsertRows("clustered", 20 + (batch * 4000),
                      20 + ((batch + 1) * 4000)));
  }
  const std::vector<int64_t> loaded =
      IntColumn(RunSql(load, *database_, "SELECT id FROM clustered;"), 0);
  EXPECT_EQ(loaded.size(), 24020U) << "fixture load";
  ASSERT_SUCCESS(load.PreCommit());
  Analyze();
  auto ctx = database_->BeginContext();
  const std::string plan = Explain(ctx, *database_,
                                   "EXPLAIN SELECT id, tag FROM clustered "
                                   "WHERE tag LIKE '%ag%' ORDER BY id;");
  EXPECT_NE(plan.find("ParallelScan"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("Sort"), std::string::npos) << plan;
  const std::vector<int64_t> ids = IntColumn(
      RunSql(ctx, *database_,
             "SELECT id, tag FROM clustered WHERE tag LIKE '%ag%' ORDER BY "
             "id;"),
      0);
  EXPECT_EQ(ids.size(), 24020U);
  EXPECT_TRUE(IsAscending(ids)) << "parallel scan must deliver chain order";
  EXPECT_EQ(ids.front(), 0);
  EXPECT_EQ(ids.back(), 24019);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, UnrelatedAndDescendingKeysKeepTheirSort) {
  Analyze();
  auto ctx = database_->BeginContext();
  // A non-key column is not the clustered one, so the scan cannot serve it.
  const std::string by_tag = Explain(
      ctx, *database_,
      "EXPLAIN SELECT id, tag FROM clustered WHERE tag LIKE '%ag%' ORDER BY "
      "tag;");
  EXPECT_NE(by_tag.find("Sort"), std::string::npos) << by_tag;
  // A descending request has no reverse chain walk.
  const std::string descending = Explain(
      ctx, *database_,
      "EXPLAIN SELECT id, tag FROM clustered WHERE tag LIKE '%ag%' ORDER BY "
      "id DESC;");
  EXPECT_NE(descending.find("Sort"), std::string::npos) << descending;
  const std::vector<int64_t> rows = IntColumn(
      RunSql(
          ctx, *database_,
          "SELECT id FROM clustered WHERE tag LIKE '%ag%' ORDER BY id DESC;"),
      0);
  EXPECT_EQ(rows.size(), 20U);
  EXPECT_TRUE(std::is_sorted(rows.begin(), rows.end(), std::greater<>()));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, ClusteredPrefixFeedsAnIncrementalSort) {
  Analyze();
  auto ctx = database_->BeginContext();
  const std::string plan = Explain(
      ctx, *database_,
      "EXPLAIN SELECT id, tag FROM clustered WHERE tag LIKE '%ag%' ORDER BY "
      "id, tag;");
  EXPECT_NE(plan.find("FullScan"), std::string::npos) << plan;
  EXPECT_NE(plan.find("IncrementalSort"), std::string::npos) << plan;
  // The clustered prefix is what makes the incremental sort possible: it only
  // re-sorts the suffix inside each already-sorted key group.
  EXPECT_NE(plan.find("presorted=id"), std::string::npos) << plan;
  const std::vector<Row> rows = RunSql(
      ctx, *database_,
      "SELECT id, tag FROM clustered WHERE tag LIKE '%ag%' ORDER BY id, tag;");
  const std::vector<int64_t> ids = IntColumn(rows, 0);
  EXPECT_EQ(ids.size(), 20U);
  EXPECT_TRUE(IsAscending(ids));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, LimitAndDistinctAboveAClusteredScanStayCorrect) {
  Analyze();
  auto ctx = database_->BeginContext();
  // Top-N over a clustered key returns the smallest keys; OFFSET walks the same
  // order, and the scan itself keeps its early stop.
  const std::vector<int64_t> top = IntColumn(
      RunSql(ctx, *database_,
             "SELECT id FROM clustered WHERE tag LIKE '%ag%' ORDER BY id LIMIT "
             "3;"),
      0);
  EXPECT_EQ(top, (std::vector<int64_t>{0, 1, 2}));
  const std::vector<int64_t> offset = IntColumn(
      RunSql(ctx, *database_,
             "SELECT id FROM clustered WHERE tag LIKE '%ag%' ORDER BY id LIMIT "
             "3 OFFSET 2;"),
      0);
  EXPECT_EQ(offset, (std::vector<int64_t>{2, 3, 4}));
  // DISTINCT above the clustered key: one row per key, in key order.
  const std::vector<int64_t> distinct = IntColumn(
      RunSql(ctx, *database_,
             "SELECT DISTINCT id FROM clustered WHERE tag LIKE '%ag%' ORDER BY "
             "id;"),
      0);
  EXPECT_EQ(distinct.size(), 20U);
  EXPECT_TRUE(IsAscending(distinct));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, OutOfOrderInsertRestoresTheSort) {
  Analyze();
  {
    auto ctx = database_->BeginContext();
    EXPECT_EQ(ExplainScan(ctx).find("Sort"), std::string::npos);
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    // The append lands below the tail, so the measurement no longer describes
    // the chain and the plan must sort again.
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_,
           "INSERT INTO clustered (id, tag) VALUES (-5, 'x');");
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = database_->BeginContext();
  const std::string plan = ExplainScan(ctx);
  EXPECT_EQ(plan.find("clustered order"), std::string::npos) << plan;
  EXPECT_NE(plan.find("Sort"), std::string::npos) << plan;
  const std::vector<int64_t> ids = ScanIds(ctx);
  EXPECT_EQ(ids.size(), 20U);
  EXPECT_TRUE(IsAscending(ids)) << "an out-of-order append must not leak out";
  // The out-of-order row is not matched by the predicate, so the visible keys
  // are still 0..19 -- but the scan sorts them because it no longer promises
  // an order.
  EXPECT_EQ(ids.front(), 0);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, UpdateRevokesTheClaimAndKeepsResultsOrdered) {
  Analyze();
  {
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_, "UPDATE clustered SET id = 1000 WHERE id = 3;");
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = database_->BeginContext();
  const std::string plan = ExplainScan(ctx);
  EXPECT_EQ(plan.find("clustered order"), std::string::npos) << plan;
  EXPECT_NE(plan.find("Sort"), std::string::npos) << plan;
  // The rewritten row keeps its payload, so it still matches the predicate:
  // 20 rows, and the moved key sorts last.
  const std::vector<int64_t> ids = ScanIds(ctx);
  EXPECT_EQ(ids.size(), 20U);
  EXPECT_TRUE(IsAscending(ids));
  EXPECT_EQ(ids.back(), 1000);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, DeleteRevokesTheClaimAndKeepsResultsOrdered) {
  Analyze();
  {
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_, "DELETE FROM clustered WHERE id = 3;");
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = database_->BeginContext();
  const std::string plan = ExplainScan(ctx);
  EXPECT_EQ(plan.find("clustered order"), std::string::npos) << plan;
  const std::vector<int64_t> ids = ScanIds(ctx);
  EXPECT_EQ(ids.size(), 19U);
  EXPECT_TRUE(IsAscending(ids));
  EXPECT_EQ(std::find(ids.begin(), ids.end(), 3), ids.end());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, UnanalyzedTableAlwaysSorts) {
  auto ctx = database_->BeginContext();
  const std::string plan = ExplainScan(ctx);
  EXPECT_EQ(plan.find("clustered order"), std::string::npos) << plan;
  EXPECT_NE(plan.find("Sort"), std::string::npos) << plan;
  const std::vector<int64_t> ids = ScanIds(ctx);
  EXPECT_EQ(ids.size(), 20U);
  EXPECT_TRUE(IsAscending(ids));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, EmptyClusteredTableIsVacuouslyOrdered) {
  TransactionContext setup = database_->BeginContext();
  RunSql(setup, *database_,
         "CREATE TABLE empty_clustered (id INT64 PRIMARY KEY, tag "
         "STRING(MAX));");
  ASSERT_SUCCESS(setup.PreCommit());
  {
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_, "ANALYZE empty_clustered;");
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = database_->BeginContext();
  const std::string plan = Explain(
      ctx, *database_,
      "EXPLAIN SELECT id FROM empty_clustered WHERE tag LIKE '%ag%' ORDER BY "
      "id;");
  EXPECT_NE(plan.find("clustered order: id ASC"), std::string::npos) << plan;
  const std::vector<Row> rows = RunSql(
      ctx, *database_,
      "SELECT id FROM empty_clustered WHERE tag LIKE '%ag%' ORDER BY id;");
  EXPECT_TRUE(rows.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, NullableUniqueKeyNeverClaimsAnOrder) {
  TransactionContext setup = database_->BeginContext();
  RunSql(setup, *database_,
         "CREATE TABLE nullable_key (id INT64, tag STRING(MAX));");
  std::string values;
  for (int64_t i = 0; i < 20; ++i) {
    if (i > 0) {
      values += ", ";
    }
    values += "(" + std::to_string(i) + ", 'tag" + std::to_string(i) + "')";
  }
  RunSql(setup, *database_,
         "INSERT INTO nullable_key (id, tag) VALUES " + values + ";");
  // Same unique index CREATE TABLE builds for a UNIQUE column, on a column the
  // schema does not declare NOT NULL.
  ASSERT_EQ(database_->CreateIndex(setup, "nullable_key",
                                   IndexSchema("nullable_key_id", {0})),
            Status::kSuccess);
  ASSERT_SUCCESS(setup.PreCommit());
  {
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_, "ANALYZE nullable_key;");
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = database_->BeginContext();
  const std::string plan = Explain(
      ctx, *database_,
      "EXPLAIN SELECT id, tag FROM nullable_key WHERE tag LIKE '%ag%' ORDER "
      "BY id;");
  EXPECT_EQ(plan.find("clustered order"), std::string::npos) << plan;
  EXPECT_NE(plan.find("Sort"), std::string::npos) << plan;
  const std::vector<int64_t> ids = IntColumn(
      RunSql(ctx, *database_,
             "SELECT id FROM nullable_key WHERE tag LIKE '%ag%' ORDER BY id;"),
      0);
  EXPECT_EQ(ids.size(), 20U);
  EXPECT_TRUE(IsAscending(ids));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, NullKeysSortAroundAClaimingScan) {
  // The clustered key is NOT NULL, so NULL placement is irrelevant to the
  // claim; a nullable *payload* column still sorts NULLs first/last as asked.
  TransactionContext setup = database_->BeginContext();
  RunSql(setup, *database_,
         "CREATE TABLE with_nulls (id INT64 PRIMARY KEY, note INT64, tag "
         "STRING(MAX));");
  RunSql(setup, *database_,
         "INSERT INTO with_nulls (id, note, tag) VALUES (1, 5, 'aga'), (2, "
         "NULL, 'agb'), (3, 7, 'agc');");
  ASSERT_SUCCESS(setup.PreCommit());
  {
    TransactionContext ctx = database_->BeginContext();
    RunSql(ctx, *database_, "ANALYZE with_nulls;");
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  auto ctx = database_->BeginContext();
  const std::string plan = Explain(
      ctx, *database_,
      "EXPLAIN SELECT id, note FROM with_nulls WHERE tag LIKE '%ag%' ORDER BY "
      "id;");
  EXPECT_NE(plan.find("FullScan"), std::string::npos) << plan;
  EXPECT_NE(plan.find("clustered order: id ASC"), std::string::npos) << plan;
  EXPECT_EQ(IntColumn(RunSql(ctx, *database_,
                             "SELECT id FROM with_nulls WHERE tag LIKE '%ag%' "
                             "ORDER BY id;"),
                      0),
            (std::vector<int64_t>{1, 2, 3}));
  // The nullable payload column is outside the claim, so it still sorts NULLs
  // where the statement asks for them.
  EXPECT_EQ(IntColumn(RunSql(ctx, *database_,
                             "SELECT id FROM with_nulls WHERE tag LIKE '%ag%' "
                             "ORDER BY note NULLS FIRST;"),
                      0),
            (std::vector<int64_t>{2, 1, 3}));
  EXPECT_EQ(IntColumn(RunSql(ctx, *database_,
                             "SELECT id FROM with_nulls WHERE tag LIKE '%ag%' "
                             "ORDER BY note NULLS LAST;"),
                      0),
            (std::vector<int64_t>{1, 3, 2}));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, FailedStatementLeavesTheClaimIntact) {
  Analyze();
  auto ctx = database_->BeginContext();
  SqlEngine engine(*database_);
  // A statement that cannot be planned must not disturb the proof.
  StatusOr<QueryResult> failed =
      engine.Execute(ctx, "SELECT id FROM no_such_table ORDER BY id;");
  EXPECT_FALSE(failed.HasValue());
  const std::string plan = ExplainScan(ctx);
  EXPECT_EQ(plan.find("Sort"), std::string::npos) << plan;
  EXPECT_EQ(ScanIds(ctx).size(), 20U);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, RepeatedQueriesStayOrderedAndUncached) {
  Analyze();
  SqlEngine engine(*database_);
  TransactionContext ctx = database_->BeginContext();
  const uint64_t fills_before = PlanCacheStats().fills.load();
  // The same shape twice. A plan that may have elided a sort on a revocable
  // proof is kept out of the compiled-plan cache, so each execution
  // re-validates the claim instead of replaying a shape that assumed it.
  for (int i = 0; i < 2; ++i) {
    StatusOr<QueryResult> result =
        engine.Execute(ctx, std::string(kScan) + ";");
    ASSERT_TRUE(result.HasValue()) << engine.LastError();
    std::vector<int64_t> ids;
    Row row;
    QueryResult drained = result.MoveValue();
    while (drained.Next(&row)) {
      ids.push_back(row[0].value.int_value);
    }
    EXPECT_EQ(ids.size(), 20U);
    EXPECT_TRUE(IsAscending(ids)) << "iteration " << i;
  }
  EXPECT_EQ(PlanCacheStats().fills.load(), fills_before);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(SqlClusteredOrderTest, ClaimedScanStaysOrderedAfterAConcurrentWrite) {
  // The regression the gate exists for: a proof that was valid when the plan
  // was built must never let unordered rows through.
  Analyze();
  TransactionContext setup = database_->BeginContext();
  RunSql(setup, *database_,
         "SELECT id FROM clustered WHERE tag LIKE '%ag%' ORDER BY id;");
  RunSql(setup, *database_,
         "INSERT INTO clustered (id, tag) VALUES (-9, 'tag-9');");
  ASSERT_SUCCESS(setup.PreCommit());
  auto ctx = database_->BeginContext();
  const std::vector<int64_t> ids =
      IntColumn(RunSql(ctx, *database_,
                       "SELECT id FROM clustered WHERE tag LIKE '%9%' ORDER BY "
                       "id;"),
                0);
  EXPECT_EQ(ids, (std::vector<int64_t>{-9, 9, 19}));
  const std::vector<int64_t> top = IntColumn(
      RunSql(ctx, *database_,
             "SELECT id FROM clustered WHERE tag LIKE '%9%' ORDER BY id LIMIT "
             "2;"),
      0);
  EXPECT_EQ(top, (std::vector<int64_t>{-9, 9}));
  ASSERT_SUCCESS(ctx.PreCommit());
}

// The SQL COUNT(*) index-only path serves its entries from the visibility
// map once the committed rows' version chains are gone: the answer is
// unchanged and the heap fetch is skipped.  (The plan shape itself is
// pinned by OptimizerTest.CountStarUsesUnboundedIndexOnlyScan.)
TEST_F(SqlClusteredOrderTest, CountStarIndexOnlyScanServesVisibilityMap) {
  WaitForChainFreePages();
  TransactionContext ctx = database_->BeginReadOnlyContext();
  const uint64_t serves_before =
      database_->TransactionStats().index_only_visible_serves;
  const std::vector<Row> rows =
      RunSql(ctx, *database_, "SELECT COUNT(*) FROM clustered;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{20}));
  EXPECT_GT(database_->TransactionStats().index_only_visible_serves,
            serves_before);
  ASSERT_SUCCESS(ctx.PreCommit());
}

}  // namespace tinylamb
