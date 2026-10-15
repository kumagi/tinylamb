/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// A full scan may only report the clustered index order when the catalog, the
// ANALYZE measurement and the physical layout all agree; every other shape must
// answer "unordered" so the caller keeps its sort. The gate is exercised
// directly here; the SQL-level consequences (EXPLAIN text, sort elision,
// plan-cache safety) live in query/sql_clustered_order_test.cpp.

#include "plan/full_scan_plan.hpp"

#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/parallel_scan.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "index/index.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

Column Keyed(const std::string& name, Constraint::ConstraintType type) {
  return Column(name, ValueType::kInt64, Constraint(type));
}

}  // namespace

class FullScanClusteredOrderTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "full_scan_clustered_order-" + RandomString();
    database_ = Database::Create(prefix_).MoveValue();
    TransactionContext ctx = database_->BeginContext();
    // Clustered: the primary key arrives in ascending order, so the chain the
    // scan walks is sorted.
    ASSIGN_OR_ASSERT_FAIL(
        Table, clustered,
        database_->CreateTable(
            ctx,
            Schema("Clustered", {Keyed("id", Constraint::kPrimaryKey),
                                 Column("payload", ValueType::kVarChar)})));
    for (int i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(
          clustered.Insert(ctx.txn_, Row({Value(i), Value("v")})).GetStatus());
    }
    // Same shape, inserted descending: nothing proves an order.
    ASSIGN_OR_ASSERT_FAIL(
        Table, shuffled,
        database_->CreateTable(
            ctx, Schema("Shuffled", {Keyed("id", Constraint::kPrimaryKey),
                                     Column("payload", ValueType::kVarChar)})));
    for (int i = 19; i >= 0; --i) {
      ASSERT_SUCCESS(
          shuffled.Insert(ctx.txn_, Row({Value(i), Value("v")})).GetStatus());
    }
    // NOT NULL but no unique key: an ordered column that is not an index key.
    ASSIGN_OR_ASSERT_FAIL(
        Table, keyless,
        database_->CreateTable(
            ctx, Schema("Keyless", {Keyed("id", Constraint::kNotNull)})));
    for (int i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(keyless.Insert(ctx.txn_, Row({Value(i)})).GetStatus());
    }
    // A unique index on a nullable column cannot prove an order: correlation
    // skips NULLs, and the engine stores NULL-bearing unique keys through the
    // multi-value encoding.
    ASSIGN_OR_ASSERT_FAIL(
        Table, nullable,
        database_->CreateTable(
            ctx, Schema("Nullable", {Keyed("id", Constraint::kUnique)})));
    for (int i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(nullable.Insert(ctx.txn_, Row({Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }

  void TearDown() override {
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
  }

  void Analyze(const std::string& name) const {
    TransactionContext ctx = database_->BeginContext();
    ASSERT_SUCCESS(database_->RefreshStatistics(ctx, name));
    ASSERT_SUCCESS(ctx.PreCommit());
  }

  std::string prefix_;
  std::unique_ptr<Database> database_;
};

TEST_F(FullScanClusteredOrderTest, NoAnalyzeMeansNoClaim) {
  auto ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable("Clustered"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Clustered"));
  // Correlation defaults to the fully-clustered 1.0, but nothing measured this
  // chain in this process, so the scan must not claim an order.
  EXPECT_DOUBLE_EQ(stats->Column(0).Correlation(), 1.0);
  EXPECT_EQ(stats->LayoutGeneration(), 0U);
  EXPECT_EQ(ClusteredOrderColumn(*table, *stats), kNoClusteredColumn);
  FullScanPlan plan(*table, *stats);
  EXPECT_FALSE(plan.ReportsClusteredOrder());
  EXPECT_FALSE(
      plan.IsOrderedBy({ColumnValueExp(ColumnName("Clustered.id"))}, {true}));
  EXPECT_EQ(plan.ToString().find("clustered order"), std::string::npos);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, AnalyzedAscendingPrimaryKeyIsReported) {
  Analyze("Clustered");
  auto ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable("Clustered"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Clustered"));
  EXPECT_NE(stats->LayoutGeneration(), 0U);
  EXPECT_EQ(stats->LayoutGeneration(), table->LayoutGeneration());
  EXPECT_EQ(ClusteredOrderColumn(*table, *stats), 0);

  FullScanPlan plan(*table, *stats);
  EXPECT_TRUE(plan.ReportsClusteredOrder());
  EXPECT_EQ(plan.ClusteredColumn(), 0);
  const Expression id = ColumnValueExp(ColumnName("Clustered.id"));
  EXPECT_TRUE(plan.IsOrderedBy({id}, {true}));
  // The unqualified spelling names the same column.
  EXPECT_TRUE(plan.IsOrderedBy({ColumnValueExp(ColumnName("id"))}, {true}));
  // NULL placement is vacuous on a NOT NULL key, so both answers match.
  EXPECT_TRUE(plan.IsOrderedBy({id}, {true}, {std::optional<bool>(true)}));
  EXPECT_TRUE(plan.IsOrderedBy({id}, {true}, {std::optional<bool>(false)}));
  // A descending request has no reverse chain walk, a second key is only a
  // prefix match, an expression is not the stored key, and another column is
  // not the clustered one.
  EXPECT_FALSE(plan.IsOrderedBy({id}, {false}));
  EXPECT_FALSE(plan.IsOrderedBy(
      {id, ColumnValueExp(ColumnName("Clustered.payload"))}, {true, true}));
  EXPECT_FALSE(plan.IsOrderedBy(
      {ColumnValueExp(ColumnName("Clustered.payload"))}, {true}));
  EXPECT_FALSE(
      plan.IsOrderedBy({BinaryExpressionExp(id, BinaryOperation::kAdd,
                                            ConstantValueExp(Value(1)))},
                       {true}));
  EXPECT_FALSE(plan.IsOrderedBy({}, {}));
  // The claim shows up in both EXPLAIN renderings.
  EXPECT_NE(plan.ToString().find("clustered order: id ASC"), std::string::npos);
  std::ostringstream dump;
  plan.Dump(dump, 0);
  EXPECT_NE(dump.str().find("clustered order: id ASC"), std::string::npos);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, DescendingDataAndWeakCatalogsFallBack) {
  for (const char* name : {"Shuffled", "Keyless", "Nullable"}) {
    Analyze(name);
  }
  auto ctx = database_->BeginContext();
  for (const char* name : {"Shuffled", "Keyless", "Nullable"}) {
    SCOPED_TRACE(name);
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable(name));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats(name));
    EXPECT_NE(stats->LayoutGeneration(), 0U);
    // Shuffled is measured but unordered; the other two are ordered without a
    // unique NOT NULL key. None of them is a clustered *index* order.
    EXPECT_EQ(ClusteredOrderColumn(*table, *stats), kNoClusteredColumn);
    FullScanPlan plan(*table, *stats);
    EXPECT_FALSE(plan.ReportsClusteredOrder());
    EXPECT_FALSE(plan.IsOrderedBy({ColumnValueExp(ColumnName("id"))}, {true}));
    EXPECT_EQ(plan.ToString().find("clustered order"), std::string::npos);
  }
  // The shuffled table's measurement is a real one, not a missing proof.
  {
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("Shuffled"));
    EXPECT_LT(stats->Column(0).Correlation(), 1.0);
  }
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, AnyInsertRevokesTheClaim) {
  Analyze("Clustered");
  {
    auto ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("Clustered"));
    EXPECT_TRUE(FullScanPlan(*table, *stats).ReportsClusteredOrder());
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    // A strictly ascending append keeps the heap sorted, but the measurement no
    // longer describes the chain, so the claim waits for the next ANALYZE.
    TransactionContext ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    ASSERT_SUCCESS(
        table->Insert(ctx.txn_, Row({Value(1000), Value("late")})).GetStatus());
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    auto ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("Clustered"));
    EXPECT_NE(stats->LayoutGeneration(), table->LayoutGeneration());
    EXPECT_EQ(ClusteredOrderColumn(*table, *stats), kNoClusteredColumn);
    EXPECT_FALSE(FullScanPlan(*table, *stats).ReportsClusteredOrder());
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  // Re-measuring a chain that an out-of-order append broke keeps it unordered.
  {
    TransactionContext ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    ASSERT_SUCCESS(
        table->Insert(ctx.txn_, Row({Value(-5), Value("out of order")}))
            .GetStatus());
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  Analyze("Clustered");
  {
    auto ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("Clustered"));
    EXPECT_LT(stats->Column(0).Correlation(), 1.0);
    EXPECT_EQ(ClusteredOrderColumn(*table, *stats), kNoClusteredColumn);
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(FullScanClusteredOrderTest, UpdateRevokesTheClaimForever) {
  // The in-place rewrite latch survives later ANALYZEs: an older snapshot can
  // still resolve a pre-image, and no measurement of the current images speaks
  // for it.
  Analyze("Clustered");
  {
    TransactionContext ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    Iterator it = table->BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    const RowPosition target = it.Position();
    ASSERT_SUCCESS(
        table->Update(ctx.txn_, target, Row({Value(1000000), Value("updated")}))
            .GetStatus());
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  Analyze("Clustered");
  auto ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable("Clustered"));
  EXPECT_TRUE(table->RowsRewrittenInPlace());
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Clustered"));
  EXPECT_NE(stats->LayoutGeneration(), 0U);
  EXPECT_EQ(ClusteredOrderColumn(*table, *stats), kNoClusteredColumn);
  EXPECT_FALSE(FullScanPlan(*table, *stats).ReportsClusteredOrder());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, DeleteRevokesTheClaim) {
  Analyze("Keyless");
  {
    TransactionContext ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Keyless"));
    Iterator it = table->BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    ASSERT_SUCCESS(table->Delete(ctx.txn_, it.Position()));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  Analyze("Keyless");
  auto ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Keyless"));
  EXPECT_TRUE(table->RowsRewrittenInPlace());
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Keyless"));
  EXPECT_EQ(ClusteredOrderColumn(*table, *stats), kNoClusteredColumn);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, ParallelShapeKeepsAndDmlSourceWithholds) {
  Analyze("Clustered");
  auto ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable("Clustered"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Clustered"));
  const std::vector<ColumnStats> columns{stats->Column(0)};
  const uint64_t proof = stats->LayoutGeneration();

  // D-9: at or above the parallel-scan threshold EmitExecutor picks the
  // morsel-parallel scan, whose morsel-ordered handoff still delivers the
  // stored chain order -- the claim survives, and the ordered parallel scan
  // refuses row-filter pushdown (deferred-row parking would reorder).
  {
    TableStatistics parallel(*stats);
    parallel.Assign(kParallelScanMinRows, columns, proof);
    FullScanPlan plan(*table, parallel);
    EXPECT_TRUE(plan.EmitsChainOrderedScan());
    EXPECT_TRUE(plan.ReportsClusteredOrder());
    EXPECT_TRUE(
        plan.IsOrderedBy({ColumnValueExp(ColumnName("Clustered.id"))}, {true}));
    EXPECT_NE(plan.ToString().find("clustered order"), std::string::npos);
    Executor executor = plan.EmitExecutor(ctx);
    auto* parallel_executor = dynamic_cast<ParallelScan*>(executor.get());
    ASSERT_NE(parallel_executor, nullptr);
    EXPECT_TRUE(parallel_executor->IsOrdered());
    EXPECT_FALSE(parallel_executor->CanTakeRowFilter());
  }
  // Below the threshold the serial scan keeps the claim the same way.
  {
    TableStatistics serial_stats(*stats);
    serial_stats.Assign(kParallelScanMinRows - 1, columns, proof);
    FullScanPlan plan(*table, serial_stats);
    EXPECT_TRUE(plan.EmitsChainOrderedScan());
    EXPECT_TRUE(plan.ReportsClusteredOrder());
    EXPECT_NE(plan.ToString().find("clustered order: id ASC"),
              std::string::npos);
  }
  // The executor the small-table claim promises is the single-threaded chain
  // walk.
  {
    FullScanPlan plan(*table, *stats);
    Executor executor = plan.EmitExecutor(ctx);
    EXPECT_NE(dynamic_cast<FullScan*>(executor.get()), nullptr);
  }
  // A DML source scan resolves head versions under a write intent: not the
  // snapshot order this claim is about.
  {
    FullScanPlan dml_source(*table, *stats, std::numeric_limits<size_t>::max(),
                            /*lock_rows=*/true);
    EXPECT_FALSE(dml_source.EmitsChainOrderedScan());
    EXPECT_FALSE(dml_source.ReportsClusteredOrder());
  }
  // A pushed-down row cap keeps the serial scan -- a prefix of a sorted chain
  // is still sorted -- so the claim survives.
  {
    FullScanPlan capped(*table, *stats, 5);
    EXPECT_TRUE(capped.EmitsChainOrderedScan());
    EXPECT_TRUE(capped.ReportsClusteredOrder());
  }
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, EmptyTableKeepsItsClaim) {
  TransactionContext ctx = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, empty,
      database_->CreateTable(
          ctx, Schema("Empty", {Keyed("id", Constraint::kPrimaryKey)})));
  ASSERT_SUCCESS(ctx.PreCommit());
  Analyze("Empty");
  auto read = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, read.GetTable("Empty"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        read.GetStats("Empty"));
  EXPECT_EQ(stats->Rows(), 0U);
  // A vacuous order: there is nothing to sort, and the executor still walks the
  // chain in order.
  EXPECT_EQ(ClusteredOrderColumn(*table, *stats), 0);
  FullScanPlan plan(*table, *stats);
  EXPECT_TRUE(
      plan.IsOrderedBy({ColumnValueExp(ColumnName("Empty.id"))}, {true}));
  Executor executor = plan.EmitExecutor(read);
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  Row row;
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(read.PreCommit());
}

TEST_F(FullScanClusteredOrderTest, ContextScopeReportsTheClaim) {
  Analyze("Clustered");
  {
    // The optimizer resolves every relation through the context's table and
    // statistics caches; the scope helper reads exactly those.
    auto ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("Clustered"));
    EXPECT_TRUE(AnyResolvedTableHasClusteredOrder(ctx));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    // A table resolved without statistics cannot have backed a claim.
    auto ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Clustered"));
    EXPECT_FALSE(AnyResolvedTableHasClusteredOrder(ctx));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  Analyze("Shuffled");
  {
    auto ctx = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("Shuffled"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("Shuffled"));
    EXPECT_FALSE(AnyResolvedTableHasClusteredOrder(ctx));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

}  // namespace tinylamb
