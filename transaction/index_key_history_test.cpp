/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "executor/index_scan.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "plan/index_scan_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

class IndexKeyHistoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_ = Database::Create("index_key_history-" + RandomString()).MoveValue();
    ASSERT_NE(db_, nullptr);
  }
  void TearDown() override { db_->DeleteAll(); }
  void Mark(Transaction& txn, page_id_t root, std::string_view key) {
    auto gate = txn.AcquireIndexWriteGuard(root);
    txn.MarkIndexKeysChanged(root, key);
  }
  std::optional<std::vector<std::string>> Changes(Transaction& txn,
                                                  page_id_t root) {
    auto gate = txn.AcquireIndexReadGuard(root);
    return txn.IndexKeyChangesSinceSnapshot(root, gate);
  }
  std::unique_ptr<Database> db_;
};

TEST_F(IndexKeyHistoryTest, PendingCommitAbortAndRootScope) {
  auto reader = db_->BeginContext();
  auto writer = db_->BeginContext();
  Mark(writer.txn_, 7, "key-a");
  Mark(writer.txn_, 7, "key-b");
  Mark(writer.txn_, 7, "key-a");
  Mark(writer.txn_, 71, "other-root");
  auto pending = Changes(reader.txn_, 7);
  ASSERT_TRUE(pending);
  EXPECT_EQ(*pending, (std::vector<std::string>{"key-a", "key-b"}));
  EXPECT_EQ(*Changes(reader.txn_, 71),
            (std::vector<std::string>{"other-root"}));
  ASSERT_SUCCESS(writer.PreCommit());
  EXPECT_EQ(*Changes(reader.txn_, 7), *pending);
  auto fresh = db_->BeginContext();
  EXPECT_TRUE(Changes(fresh.txn_, 7)->empty());
  auto aborting = db_->BeginContext();
  Mark(aborting.txn_, 7, "aborted");
  EXPECT_EQ(*Changes(fresh.txn_, 7), (std::vector<std::string>{"aborted"}));
  ASSERT_SUCCESS(aborting.txn_.Abort());
  EXPECT_TRUE(Changes(fresh.txn_, 7)->empty());
}

TEST_F(IndexKeyHistoryTest, GuardAndUnknownKeyCallsRemainConservative) {
  auto reader = db_->BeginContext();
  TransactionManager::IndexReadGuard empty;
  EXPECT_FALSE(reader.txn_.IndexKeyChangesSinceSnapshot(7, empty));
  auto writer = db_->BeginContext();
  Mark(writer.txn_, 7, "known");
  ASSERT_TRUE(Changes(reader.txn_, 7));
  // Legacy API cannot prove the complete changed-key set, even if this root
  // already had an exact registration. Its revocation is intentionally sticky.
  writer.txn_.MarkIndexKeysChanged(7);
  EXPECT_FALSE(Changes(reader.txn_, 7));
  ASSERT_SUCCESS(writer.PreCommit());
  auto fresh = db_->BeginContext();
  EXPECT_FALSE(Changes(fresh.txn_, 7));
}

TEST_F(IndexKeyHistoryTest, EvictedHistoryRejectsOldButNotFreshSnapshots) {
  db_->SetSynchronousCommit(false);
  auto old = db_->BeginContext();
  for (int i = 0; i < 600; ++i) {
    auto writer = db_->BeginContext();
    Mark(writer.txn_, 7, std::to_string(i));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  EXPECT_TRUE(old.txn_.IndexKeysMayBeStale(7));
  EXPECT_FALSE(Changes(old.txn_, 7));
  auto fresh = db_->BeginContext();
  ASSERT_TRUE(Changes(fresh.txn_, 7));
  EXPECT_TRUE(Changes(fresh.txn_, 7)->empty());
}

TEST_F(IndexKeyHistoryTest, OversizedPendingSetRevokesRatherThanLosesKeys) {
  auto reader = db_->BeginContext();
  auto writer = db_->BeginContext();
  for (int i = 0; i < 600; ++i) Mark(writer.txn_, 7, std::to_string(i));
  EXPECT_FALSE(Changes(reader.txn_, 7));
  EXPECT_TRUE(reader.txn_.IndexKeysMayBeStale(7));
  ASSERT_SUCCESS(writer.txn_.Abort());
  EXPECT_FALSE(reader.txn_.IndexKeysMayBeStale(7));
  EXPECT_FALSE(Changes(reader.txn_, 7));
}

TEST_F(IndexKeyHistoryTest, TransactionMovePreservesPendingIdentity) {
  auto reader = db_->BeginContext();
  auto context = db_->BeginContext();
  Mark(context.txn_, 7, "moved");
  Transaction moved(std::move(context.txn_));
  EXPECT_EQ(*Changes(reader.txn_, 7), (std::vector<std::string>{"moved"}));
  ASSERT_SUCCESS(moved.Abort());
  EXPECT_TRUE(Changes(reader.txn_, 7)->empty());
}

TEST_F(IndexKeyHistoryTest, BoundEncodingIsInclusivePrefixAndNullConservative) {
  Index index("compound", {0, 1}, 7, {}, IndexMode::kNonUnique);
  auto key = [](int64_t a, int64_t b) {
    return Row({Value(a), Value(b)}).EncodeMemcomparableFormat();
  };
  EXPECT_TRUE(IndexScanIterator::EncodedKeyMayOverlap(
      index, key(1, 5), {Value(1), Value(5)}, {Value(1), Value(5)}));
  EXPECT_FALSE(IndexScanIterator::EncodedKeyMayOverlap(
      index, key(1, 4), {Value(1), Value(5)}, {Value(1), Value(5)}));
  EXPECT_TRUE(IndexScanIterator::EncodedKeyMayOverlap(index, key(1, -100),
                                                      {Value(1)}, {Value(1)}));
  EXPECT_FALSE(IndexScanIterator::EncodedKeyMayOverlap(index, key(2, 0),
                                                       {Value(1)}, {Value(1)}));
  EXPECT_TRUE(IndexScanIterator::EncodedKeyMayOverlap(index, key(-1, 0), {},
                                                      {Value(1)}));
  EXPECT_TRUE(IndexScanIterator::EncodedKeyMayOverlap(index, key(100, 0),
                                                      {Value()}, {Value()}));
  const std::string text("a\0z", 3);
  const std::string encoded =
      Row({Value(std::string(text)), Value(4)}).EncodeMemcomparableFormat();
  EXPECT_TRUE(IndexScanIterator::EncodedKeyMayOverlap(
      index, encoded, {Value(std::string(text))}, {Value(std::string(text))}));
  EXPECT_FALSE(IndexScanIterator::EncodedKeyMayOverlap(
      index, encoded, {Value("a")}, {Value("a")}));
}

TEST_F(IndexKeyHistoryTest, DisjointCompoundRangeStaysIndexedAcrossCommit) {
  RowPosition changed;
  const Row first({Value(1), Value(1), Value(1), Value(10)});
  const Row before({Value(1), Value(2), Value(2), Value(20)});
  {
    auto setup = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        db_->CreateTable(setup,
                         Schema("rows", {Column("w", ValueType::kInt64),
                                         Column("d", ValueType::kInt64),
                                         Column("c", ValueType::kInt64),
                                         Column("o", ValueType::kInt64)})));
    ASSERT_SUCCESS(table.Insert(setup.txn_, first).GetStatus());
    ASSIGN_OR_ASSERT_FAIL(RowPosition, pos, table.Insert(setup.txn_, before));
    changed = pos;
    ASSERT_SUCCESS(db_->CreateIndex(
        setup, "rows",
        IndexSchema("by_customer", {0, 1, 2, 3}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(setup.PreCommit());
  }
  auto reader = db_->BeginContext();
  auto writer = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, writer.GetTable("rows"));
  ASSERT_SUCCESS(table
                     ->Update(writer.txn_, changed,
                              Row({Value(1), Value(2), Value(2), Value(21)}))
                     .GetStatus());

  auto check = [&](int64_t district, int64_t customer, const Row& expected,
                   bool full_scan) {
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, source,
                          reader.GetTable("rows"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          reader.GetStats("rows"));
    Expression predicate;
    const std::vector<Value> prefix{Value(1), Value(district), Value(customer)};
    for (size_t i = 0; i < prefix.size(); ++i) {
      Expression equal = BinaryExpressionExp(
          ColumnValueExp(source->GetSchema().GetColumn(i).Name()),
          BinaryOperation::kEquals, ConstantValueExp(prefix[i]));
      predicate = predicate ? BinaryExpressionExp(predicate,
                                                  BinaryOperation::kAnd, equal)
                            : equal;
    }
    IndexScanPlan plan(*source, source->GetIndex(0), *stats, prefix, prefix,
                       false, predicate);
    Executor executor = plan.EmitExecutor(reader);
    std::ostringstream dump;
    executor->Dump(dump, 0);
    EXPECT_EQ(dump.str().find("FullScan") != std::string::npos, full_scan)
        << dump.str();
    Row result;
    ASSERT_TRUE(executor->Next(&result, nullptr));
    EXPECT_EQ(result, expected);
    EXPECT_FALSE(executor->Next(&result, nullptr));
    EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  };
  check(1, 1, first, false);
  check(2, 2, before, true);
  ASSERT_SUCCESS(writer.PreCommit());
  check(1, 1, first, false);
  check(2, 2, before, true);
}

TEST_F(IndexKeyHistoryTest, PointRangesAvoidRedundantLookahead) {
  {
    auto setup = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        db_->CreateTable(setup,
                         Schema("points", {Column("k", ValueType::kInt64),
                                           Column("v", ValueType::kInt64)})));
    for (int64_t i = 0; i < 2048; ++i) {
      ASSERT_SUCCESS(
          table.Insert(setup.txn_, Row({Value(i), Value(i * 2)})).GetStatus());
    }
    ASSERT_SUCCESS(db_->CreateIndex(
        setup, "points",
        IndexSchema("points_key", {0}, {}, IndexMode::kUnique)));
    ASSERT_SUCCESS(setup.PreCommit());
  }
  auto context = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("points"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        context.GetStats("points"));
  std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges;
  std::vector<Row> expected;
  for (int64_t i = 0; i < 100; ++i) {
    ranges.push_back({{Value(i * 13)}, {Value(i * 13)}});
    expected.emplace_back(Row({Value(i * 13), Value(i * 26)}));
  }
  auto drain = [&](Executor executor) {
    std::vector<Row> rows;
    Row row;
    while (executor->Next(&row, nullptr)) rows.push_back(row);
    EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
    EXPECT_EQ(rows, expected);
  };
  auto reads = [&] { return db_->CacheHits() + db_->CacheMisses(); };
  IndexScanPlan plan(*table, table->GetIndex(0), *stats, ranges, true, {});
  const auto start = reads();
  drain(plan.EmitExecutor(context));
  const auto planned_reads = reads() - start;
  const auto eager_start = reads();
  drain(std::make_shared<IndexScan>(
      context.txn_, *table, table->GetIndex(0), ranges, true, Expression{},
      table->GetSchema(), false, true, TransactionManager::IndexReadGuard{},
      true));
  const auto eager_reads = reads() - eager_start;
  EXPECT_LT(planned_reads + ranges.size(), eager_reads);
}

}  // namespace
}  // namespace tinylamb
