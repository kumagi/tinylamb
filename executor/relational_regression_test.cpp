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

#include <cstdint>
#include <memory>
#include <string_view>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "type/column.hpp"
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
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return rows;
}

class RelationalRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "relational_regression-" + RandomString();
    rs_ = std::make_unique<Database>(prefix_);
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, agg,
        rs_->CreateTable(ctx, Schema("AggTable", {Column("k", ValueType::kInt64),
                                                  Column("v", ValueType::kInt64)})));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(agg.Insert(ctx.txn_, Row({Value(i % 10), Value(1)}))
                          .GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, small,
        rs_->CreateTable(ctx, Schema("SmallKeys", {Column("k", ValueType::kInt64)})));
    for (const int64_t key : {int64_t{1}, int64_t{2}, int64_t{3}, int64_t{4}}) {
      ASSERT_SUCCESS(
          small.Insert(ctx.txn_, Row({Value(key)})).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, driver,
        rs_->CreateTable(ctx, Schema("KeyTable", {Column("k", ValueType::kInt64)})));
    for (int64_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(driver.Insert(ctx.txn_, Row({Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

// A single statement that first narrows AggTable through an uncorrelated IN
// list pushdown (BuildInput stores table_key_filters["AggTable"]) and then
// aggregates the same table in an uncorrelated scalar subquery. The subquery's
// stream_agg cache fill must stay unfiltered; only its own read may filter.
TEST_F(RelationalRegressionTest, Execute_StreamAggWithInListFilter_IgnoresStashedFilterInSubquery) {
  const auto rows = RelationalRun(
      *rs_,
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
TEST_F(RelationalRegressionTest, Execute_StreamAggWithJoinDerivedFilter_IgnoresStashedFilterInSubquery) {
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
TEST_F(RelationalRegressionTest, Execute_SharedBaseRelationCache_StaysUnfilteredAcrossConsumers) {
  const auto rows = RelationalRun(
      *rs_,
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

}  // namespace
}  // namespace tinylamb
