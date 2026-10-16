/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// ParallelIndexScan: the RangePartitionedScan consumer.  The requested key
// range is partitioned at B+Tree leaf boundaries and scanned by workers; the
// morsel-ordered handoff must reproduce the serial IndexScan's rows (and
// their row positions) exactly, including keys whose duplicates span leaf
// boundaries.

#include "executor/parallel_index_scan.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/index_scan.hpp"
#include "index/index_schema.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

constexpr int64_t kRows = 5000;
// 10 consecutive rows share each key, so a leaf-boundary cut lands inside a
// duplicate run and the exclusive-end handoff must neither drop nor
// duplicate a row.
constexpr int64_t kRowsPerKey = 10;

std::vector<std::pair<Row, RowPosition>> DrainSerial(
    Transaction& txn, const Table& table, const Index& index,
    const std::vector<Value>& begin, const std::vector<Value>& end) {
  std::vector<std::pair<Row, RowPosition>> rows;
  IndexScan scan(txn, table, index, begin, end, true, Expression(),
                 table.GetSchema());
  Row row;
  RowPosition position;
  while (scan.Next(&row, &position)) {
    rows.emplace_back(row, position);
  }
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  return rows;
}

std::vector<std::pair<Row, RowPosition>> DrainParallel(
    Transaction& txn, const Table& table, const Index& index,
    const std::vector<Value>& begin, const std::vector<Value>& end,
    size_t workers, size_t leaves_per_morsel, size_t* morsels_out = nullptr) {
  ParallelIndexScan scan(txn, table, index, begin, end, true, workers,
                         leaves_per_morsel);
  if (morsels_out != nullptr) {
    *morsels_out = scan.MorselCount();
  }
  std::vector<std::pair<Row, RowPosition>> rows;
  Row row;
  RowPosition position;
  while (scan.Next(&row, &position)) {
    rows.emplace_back(row, position);
  }
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  return rows;
}

}  // namespace

class ParallelIndexScanTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "parallel_index_scan_test-" + RandomString();
    db_ = Database::Create(prefix_).MoveValue();
    TransactionContext writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        db_->CreateTable(
            writer,
            Schema("ParallelIdx", {Column("key", ValueType::kInt64),
                                   Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(writer.txn_, Row({Value(i / kRowsPerKey), Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(db_->CreateIndex(
        writer, "ParallelIdx",
        IndexSchema("KeyIdx", {0}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(writer.PreCommit());
  }

  void TearDown() override {
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
  }

 protected:
  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(ParallelIndexScanTest, MatchesSerialScanRowForRow) {
  TransactionContext ctx = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable("ParallelIdx"));
  const Index& index = table->GetIndex(0);
  const std::vector<Value> begin{Value(int64_t{10})};
  const std::vector<Value> end{Value(int64_t{400})};

  const auto serial = DrainSerial(ctx.txn_, *table, index, begin, end);
  size_t morsels = 0;
  const auto parallel =
      DrainParallel(ctx.txn_, *table, index, begin, end,
                    /*workers=*/4, /*leaves_per_morsel=*/1, &morsels);

  EXPECT_GE(morsels, 2U) << "expected a leaf-aligned partition";
  ASSERT_EQ(parallel.size(), serial.size());
  for (size_t i = 0; i < serial.size(); ++i) {
    EXPECT_EQ(parallel[i].first, serial[i].first) << "row " << i;
    EXPECT_EQ(parallel[i].second, serial[i].second) << "row " << i;
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(ParallelIndexScanTest, FullRangeMatchesSerialScan) {
  TransactionContext ctx = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable("ParallelIdx"));
  const Index& index = table->GetIndex(0);

  const auto serial = DrainSerial(ctx.txn_, *table, index, {}, {});
  const auto parallel =
      DrainParallel(ctx.txn_, *table, index, {}, {}, /*workers=*/3,
                    /*leaves_per_morsel=*/2);

  ASSERT_EQ(serial.size(), static_cast<size_t>(kRows));
  ASSERT_EQ(parallel.size(), serial.size());
  for (size_t i = 0; i < serial.size(); ++i) {
    EXPECT_EQ(parallel[i].first, serial[i].first) << "row " << i;
    EXPECT_EQ(parallel[i].second, serial[i].second) << "row " << i;
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(ParallelIndexScanTest, EmptyRangeYieldsNoRows) {
  TransactionContext ctx = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable("ParallelIdx"));
  const Index& index = table->GetIndex(0);
  // An inverted range is legitimately empty (SQL `BETWEEN 9 AND 3`).
  const auto parallel =
      DrainParallel(ctx.txn_, *table, index, {Value(int64_t{400})},
                    {Value(int64_t{10})}, /*workers=*/2,
                    /*leaves_per_morsel=*/1);
  EXPECT_TRUE(parallel.empty());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(ParallelIndexScanTest, BatchPathMatchesSerialScan) {
  TransactionContext ctx = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable("ParallelIdx"));
  const Index& index = table->GetIndex(0);
  const std::vector<Value> begin{Value(int64_t{100})};
  const std::vector<Value> end{Value(int64_t{300})};

  const auto serial = DrainSerial(ctx.txn_, *table, index, begin, end);

  ParallelIndexScan scan(ctx.txn_, *table, index, begin, end, true,
                         /*workers=*/4, /*leaves_per_morsel=*/1);
  std::vector<std::pair<Row, RowPosition>> batched;
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 64) != 0) {
    for (size_t i = 0; i < chunk.Size(); ++i) {
      batched.emplace_back(chunk.RowAt(i), chunk.PositionAt(i));
    }
  }
  ASSERT_EQ(scan.GetStatus(), Status::kSuccess);

  ASSERT_EQ(batched.size(), serial.size());
  for (size_t i = 0; i < serial.size(); ++i) {
    EXPECT_EQ(batched[i].first, serial[i].first) << "row " << i;
    EXPECT_EQ(batched[i].second, serial[i].second) << "row " << i;
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

}  // namespace tinylamb
