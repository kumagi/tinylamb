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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
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
    EXPECT_EQ(read(old, &visited), expected);
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
      const double qty = static_cast<double>((i % 50) + 1);
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
  ASSERT_SUCCESS(rel.spill_tail_->FinishWriting());
  // Appending to an already-finished SpillFile simulates a spill write error,
  // latching spill_error_ and resetting spill_tail_.
  rel.AddRow(Row({Value(int64_t{1})}));
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

}  // namespace tinylamb
