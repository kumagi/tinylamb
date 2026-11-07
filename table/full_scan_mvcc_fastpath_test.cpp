/**
 * Copyright 2023 KUMAZAKI Hiroki
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

// Visibility regression tests for the read-only MVCC fast path in
// FullScanIterator (see PhysicalReadEligible in full_scan_iterator.cpp).
// The fast path serves raw page bytes for pages with no MVCC version chain
// registered in TransactionManager::HasPageVersionChains: a page without
// chains carries no in-flight or uncommitted mutation, so its bytes are a
// committed state every snapshot may read directly.  These tests pin down
// that such readers never observe a concurrent writer's post-snapshot
// change, whether the page is served through the fast path or through the
// ReadVersion fallback.

#include <chrono>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "iterator.hpp"
#include "table/table.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

Schema SampleSchema() {
  return Schema("SampleTable", {Column("col1", ValueType::kInt64,
                                       Constraint(Constraint::kIndex)),
                                Column("col2", ValueType::kVarChar),
                                Column("col3", ValueType::kDouble)});
}

// Full-scans |table| under |txn| and asserts the col1 key multiset equals
// |expected| in scan order.
void ExpectScannedKeys(Table& table, Transaction& txn,
                       const std::vector<int64_t>& expected) {
  std::vector<int64_t> keys;
  for (Iterator it = table.BeginFullScan(txn); it.IsValid(); ++it) {
    keys.push_back((*it)[0].value.int_value);
  }
  ASSERT_EQ(keys, expected);
}

}  // namespace

class FullScanMvccFastPathTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "full_scan_mvcc_fastpath-" + RandomString();
    db_ = Database::Create(prefix_).MoveValue();
    TransactionContext ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->CreateTable(ctx, SampleSchema()).GetStatus());
    ASSERT_SUCCESS(ctx.PreCommit());
  }

  void TearDown() override { db_->DeleteAll(); }

  // Commits |count| empty write transactions so the stable commit timestamp
  // grows past the seeded page's write era (the fast path no longer compares
  // PageLSN values, but the age still exercises the GC drop discipline).
  void AdvanceCommitClock(int count) {
    for (int i = 0; i < count; ++i) {
      TransactionContext bump = db_->BeginContext();
      ASSERT_SUCCESS(bump.txn_.PreCommit());
    }
  }

 protected:
  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(FullScanMvccFastPathTest, LockedScanWaitDoesNotHoldThePageLatch) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, position,
      table.Insert(seed.txn_, Row({Value(0), Value("a"), Value(0.5)})));
  ASSERT_SUCCESS(seed.PreCommit());
  TransactionContext owner = db_->BeginContext();
  ASSERT_TRUE(owner.txn_.AddWriteSet(position));
  std::promise<void> started;
  std::promise<StatusOr<Row>> scanned;
  auto entered = started.get_future();
  auto result = scanned.get_future();
  std::thread waiter([&] {
    auto context = db_->BeginContext();
    TableScanOptions options;
    options.lock_rows = true;
    started.set_value();
    auto scan = table.BeginFullScan(context.txn_, options);
    if (scan.IsValid()) {
      scanned.set_value(*scan);
    } else {
      scanned.set_value(scan.GetStatus());
    }
  });
  entered.wait();
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  const auto begin = std::chrono::steady_clock::now();
  const auto updated = table.Update(
      owner.txn_, position, Row({Value(0), Value("updated"), Value(1.0)}));
  const Status committed = owner.PreCommit();
  waiter.join();
  EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(1));
  EXPECT_EQ(updated.GetStatus(), Status::kSuccess);
  EXPECT_EQ(committed, Status::kSuccess);
  const auto row = result.get();
  ASSERT_TRUE(row.HasValue()) << row.GetStatus();
  EXPECT_EQ(row.Value(), Row({Value(0), Value("updated"), Value(1.0)}));
}

TEST_F(FullScanMvccFastPathTest, DirectUpdateWaitDoesNotHoldThePageLatch) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, position,
      table.Insert(seed.txn_, Row({Value(0), Value("a"), Value(0.5)})));
  ASSERT_SUCCESS(seed.PreCommit());
  TransactionContext owner = db_->BeginContext();
  ASSERT_TRUE(owner.txn_.AddWriteSet(position));
  std::promise<void> started;
  std::promise<Status> completed;
  auto entered = started.get_future();
  auto result = completed.get_future();
  std::thread waiter([&] {
    auto context = db_->BeginContext();
    started.set_value();
    const auto update =
        table.Update(context.txn_, position,
                     Row({Value(0), Value("contender"), Value(2.0)}));
    context.Abort();
    completed.set_value(update.GetStatus());
  });
  entered.wait();
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  const auto begin = std::chrono::steady_clock::now();
  const auto updated = table.Update(
      owner.txn_, position, Row({Value(0), Value("updated"), Value(1.0)}));
  const Status committed = owner.PreCommit();
  waiter.join();
  EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(1));
  EXPECT_EQ(updated.GetStatus(), Status::kSuccess);
  EXPECT_EQ(committed, Status::kSuccess);
  const Status status = result.get();
  EXPECT_TRUE(status == Status::kSuccess || status == Status::kConflicts);
}

// Probe: txn B holds an uncommitted update on a row; txn A updating the
// same row must lose the write-intent race.
TEST_F(FullScanMvccFastPathTest, ConcurrentUpdateSameRowConflicts) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(seed.txn_, Row({Value(0), Value("a"), Value(0.5)}))
          .GetStatus());
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext ctx_a = db_->BeginContext();
  TransactionContext ctx_b = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, ta, db_->GetTable(ctx_a, "SampleTable"));
  ASSIGN_OR_ASSERT_FAIL(Table, tb, db_->GetTable(ctx_b, "SampleTable"));

  RowPosition rp;
  for (Iterator it = tb.BeginFullScan(ctx_b.txn_); it.IsValid(); ++it) {
    rp = it.Position();
  }
  ASSERT_TRUE(rp.IsValid());
  // B updates the row and keeps the write intent.
  ASSERT_SUCCESS(
      tb.Update(ctx_b.txn_, rp, Row({Value(0), Value("a"), Value(9.9)}))
          .GetStatus());
  // B no-op-updates the same row again (same-txn second write).
  ASSERT_SUCCESS(
      tb.Update(ctx_b.txn_, rp, Row({Value(0), Value("a"), Value(9.9)}))
          .GetStatus());
  // A's update of the same row must conflict, not succeed.
  StatusOr<RowPosition> clashed =
      ta.Update(ctx_a.txn_, rp, Row({Value(0), Value("a"), Value(7.7)}));
  EXPECT_NE(clashed.GetStatus(), Status::kSuccess)
      << "concurrent update acquired the intent another txn holds";
}

// A read-only scan over a page that qualifies for the physical path (no
// version chain) must return exactly the seeded rows -- no more, no fewer,
// correct values.
TEST_F(FullScanMvccFastPathTest,
       LockedScanFilterDoesNotReleasePreexistingIntent) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              seed.GetTable("SampleTable"));
  ASSERT_SUCCESS(
      table->Insert(seed.txn_, Row({Value(0), Value("a"), Value(0.5)}))
          .GetStatus());
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext owner = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, owner_table,
                              owner.GetTable("SampleTable"));
  RowPosition target;
  Iterator locate = owner_table->BeginFullScan(owner.txn_);
  ASSERT_TRUE(locate.IsValid());
  target = locate.Position();
  ASSERT_TRUE(owner.txn_.TryAddWriteSet(target));

  const std::unordered_set<int64_t> key_filter{999};
  TableScanOptions options;
  options.key_filter = &key_filter;
  options.key_column = 0;
  options.lock_rows = true;
  options.wait_for_write_intent = false;
  Iterator scan = owner_table->BeginFullScan(owner.txn_, options);
  while (scan.IsValid()) {
    ++scan;
  }
  ASSERT_EQ(scan.GetStatus(), Status::kSuccess);

  TransactionContext contender = db_->BeginContext();
  EXPECT_FALSE(contender.txn_.TryAddWriteSet(target));
  contender.Abort();
  owner.Abort();
}

TEST_F(FullScanMvccFastPathTest,
       BeginFullScan_ReadOnlyScan_ReturnsSeededRowsVerbatim) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  for (int i = 0; i < 5; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(seed.PreCommit());
  AdvanceCommitClock(256);

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSERT_NO_FATAL_FAILURE(
      ExpectScannedKeys(table, reader.txn_, {0, 1, 2, 3, 4}));
  ASSERT_SUCCESS(reader.PreCommit());

  // A second read-only transaction over the same pages agrees.
  TransactionContext again = db_->BeginReadOnlyContext();
  ASSERT_NO_FATAL_FAILURE(
      ExpectScannedKeys(table, again.txn_, {0, 1, 2, 3, 4}));
  ASSERT_SUCCESS(again.PreCommit());
}

// THE core regression: a read-only transaction begins, THEN a writer updates
// a row on the very same page and commits.  The reader keeps observing the
// pre-update image for its whole snapshot, through plain scans as well as
// morsel scans, while fresh readers see the new value.
TEST_F(FullScanMvccFastPathTest,
       BeginFullScan_WhenWriterUpdatesSamePageAfterBegin_ReaderKeepsOldValue) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  RowPosition target;
  for (int i = 0; i < 3; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, inserted,
        table.Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                     Value(0.1 + i)})));
    if (i == 1) {
      target = inserted;
    }
  }
  ASSERT_SUCCESS(seed.PreCommit());
  AdvanceCommitClock(64);

  TransactionContext reader = db_->BeginReadOnlyContext();

  // Writer mutates the SAME page after the reader's snapshot began.
  TransactionContext writer = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, updated,
      table.Update(writer.txn_, target,
                   Row({Value(100), Value("updated"), Value(9.9)})));
  ASSERT_EQ(updated, target);
  ASSERT_SUCCESS(writer.PreCommit());

  // The open snapshot still walks the old image.
  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(table, reader.txn_, {0, 1, 2}));

  // Morsel scans under the same open snapshot agree.
  int64_t morsel_sum = 0;
  for (const Table::ScanMorsel& morsel : table.BuildScanMorsels(reader.txn_)) {
    for (Iterator it = table.BeginMorselScan(reader.txn_, morsel); it.IsValid();
         ++it) {
      morsel_sum += (*it)[0].value.int_value;
    }
  }
  EXPECT_EQ(morsel_sum, 3);

  ASSERT_SUCCESS(reader.PreCommit());

  // A reader starting after the commit observes the new value exactly once.
  TransactionContext fresh = db_->BeginReadOnlyContext();
  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(table, fresh.txn_, {0, 100, 2}));
  ASSERT_SUCCESS(fresh.PreCommit());
}

// Same contract for deletions: an open read-only snapshot keeps seeing a
// physically removed row; snapshots opened after the delete do not.
TEST_F(FullScanMvccFastPathTest,
       BeginFullScan_WhenUncommittedWriterPredatesReader_HidesItsUpdate) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, inserted,
      table.Insert(seed.txn_, Row({Value(4), Value("old"), Value(0.4)})));
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext writer = db_->BeginContext();
  ASSERT_TRUE(table
                  .Update(writer.txn_, inserted,
                          Row({Value(40), Value("pending"), Value(4.0)}))
                  .HasValue());
  AdvanceCommitClock(128);

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(table, reader.txn_, {4}));
  ASSERT_SUCCESS(reader.PreCommit());
  ASSERT_SUCCESS(writer.txn_.Abort());
}

TEST_F(
    FullScanMvccFastPathTest,
    BeginFullScan_WhenWriterDeletesRowAfterBegin_ReaderKeepsDeletedRowVisible) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  RowPosition tail;
  for (int i = 0; i < 2; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, inserted,
        table.Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                     Value(0.1 + i)})));
    tail = inserted;
  }
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext reader = db_->BeginReadOnlyContext();

  TransactionContext writer = db_->BeginContext();
  ASSERT_SUCCESS(table.Delete(writer.txn_, tail));
  ASSERT_SUCCESS(writer.PreCommit());

  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(table, reader.txn_, {0, 1}));
  ASSERT_SUCCESS(reader.PreCommit());

  TransactionContext fresh = db_->BeginReadOnlyContext();
  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(table, fresh.txn_, {0}));
  ASSERT_SUCCESS(fresh.PreCommit());
}

// Regression: version chains are keyed by RowPosition{page_id, slot}.  A
// dropped table's row page returns to the free list with its committed
// version chains intact; when a later CREATE TABLE recycles that page id,
// scans of the new table must never be served the dropped table's rows --
// neither for physically vacant slots (undo left row_max_ covering them)
// nor under a snapshot that predates the new rows' commit.
TEST_F(FullScanMvccFastPathTest,
       BeginFullScan_RecycledPageNeverServesDroppedTableRows) {
  const Schema plain("PlainTable", {Column("col1", ValueType::kInt64),
                                    Column("col2", ValueType::kVarChar),
                                    Column("col3", ValueType::kDouble)});

  // Seed two committed rows in a table whose page will be recycled.
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, dropped, db_->CreateTable(seed, plain));
  for (int i = 0; i < 2; ++i) {
    ASSERT_SUCCESS(dropped
                       .Insert(seed.txn_, Row({Value(100 + i), Value("old"),
                                               Value(0.5 + i)}))
                       .GetStatus());
  }
  ASSERT_SUCCESS(seed.PreCommit());
  const page_id_t recycled_pid = dropped.FirstPageId();

  // Drop, commit, then recreate: the free list is LIFO, so once the
  // destroyer has settled the new table's row page reuses the dropped page
  // id while its version chains still describe the dropped table's rows.
  // (An unsettled destroyer's page is never reissued -- see
  // SameTransactionCannotReusePageOfOwnPendingDestroy.)
  TransactionContext ddl = db_->BeginContext();
  ASSERT_SUCCESS(db_->DropTable(ddl, "PlainTable"));
  ASSERT_SUCCESS(ddl.PreCommit());
  TransactionContext ddl2 = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, recycled,
      db_->CreateTable(
          ddl2, Schema("RecycledTable", {Column("col1", ValueType::kInt64),
                                         Column("col2", ValueType::kVarChar),
                                         Column("col3", ValueType::kDouble)})));
  ASSERT_EQ(recycled.FirstPageId(), recycled_pid);
  ASSERT_SUCCESS(ddl2.PreCommit());

  // Aborted inserts leave vacant slots below row_max_: a scan probing them
  // must not resurrect the dropped table's committed versions.
  TransactionContext writer = db_->BeginContext();
  for (int i = 0; i < 2; ++i) {
    ASSERT_SUCCESS(
        recycled
            .Insert(writer.txn_, Row({Value(i), Value("new"), Value(1.5 + i)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(writer.txn_.Abort());

  TransactionContext check = db_->BeginContext();
  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(recycled, check.txn_, {}));
  ASSERT_SUCCESS(check.PreCommit());
}

// A destroy pushes the page onto the free list immediately, before the
// transaction commits.  If another transaction allocates that id and the
// destroyer later aborts, the undo's PageInit+body restore must not clobber
// the page's new owner.
TEST_F(FullScanMvccFastPathTest, AbortedDestroyMustNotClobberReallocatedPage) {
  const Schema plain("PlainTable", {Column("col1", ValueType::kInt64),
                                    Column("col2", ValueType::kVarChar),
                                    Column("col3", ValueType::kDouble)});

  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, dropped, db_->CreateTable(seed, plain));
  ASSERT_SUCCESS(
      dropped.Insert(seed.txn_, Row({Value(7), Value("doomed"), Value(1.0)}))
          .GetStatus());
  ASSERT_SUCCESS(seed.PreCommit());
  const page_id_t recycled_pid = dropped.FirstPageId();

  // Dropper destroys the page but stays uncommitted.
  TransactionContext dropper = db_->BeginContext();
  ASSERT_SUCCESS(db_->DropTable(dropper, "PlainTable"));

  // Another transaction must NOT be handed the contested id: while the
  // destroy is still undoable it cannot be reissued, so the new table grows
  // past the high-water mark instead.
  TransactionContext owner = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, claimed,
      db_->CreateTable(
          owner, Schema("ClaimedTable", {Column("col1", ValueType::kInt64),
                                         Column("col2", ValueType::kVarChar),
                                         Column("col3", ValueType::kDouble)})));
  EXPECT_NE(claimed.FirstPageId(), recycled_pid);
  ASSERT_SUCCESS(
      claimed.Insert(owner.txn_, Row({Value(9), Value("kept"), Value(2.0)}))
          .GetStatus());
  ASSERT_SUCCESS(owner.PreCommit());

  // The abort restores the dropped table wholesale: catalog entry and its
  // original page, intact.  The claimed table keeps its own page.
  ASSERT_SUCCESS(dropper.txn_.Abort());

  TransactionContext check = db_->BeginContext();
  ASSERT_NO_FATAL_FAILURE(ExpectScannedKeys(claimed, check.txn_, {9}));
  StatusOr<Table> resurrected = db_->GetTable(check, "PlainTable");
  ASSERT_SUCCESS(resurrected.GetStatus());
  ASSERT_EQ(resurrected.Value().FirstPageId(), recycled_pid);
  ASSERT_NO_FATAL_FAILURE(
      ExpectScannedKeys(resurrected.Value(), check.txn_, {7}));
  ASSERT_SUCCESS(check.PreCommit());
}

// Same-transaction variant: a transaction that destroys a page must not be
// reissued that page id either.  If it were, the allocation would erase the
// dropped incarnation's version chains; when the transaction later aborts,
// the destroy undo restores the old page image (slots populated again) but
// the chains that made those rows visible are gone -- the resurrected table
// scans as empty.
TEST_F(FullScanMvccFastPathTest,
       SameTransactionCannotReusePageOfOwnPendingDestroy) {
  const Schema plain("PlainTable", {Column("col1", ValueType::kInt64),
                                    Column("col2", ValueType::kVarChar),
                                    Column("col3", ValueType::kDouble)});

  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, dropped, db_->CreateTable(seed, plain));
  ASSERT_SUCCESS(
      dropped.Insert(seed.txn_, Row({Value(3), Value("keep"), Value(0.5)}))
          .GetStatus());
  ASSERT_SUCCESS(seed.PreCommit());
  const page_id_t doomed_pid = dropped.FirstPageId();

  // One transaction: drop the table, create another, insert into it, abort.
  // The create must grow past the high-water mark rather than reissue the
  // just-destroyed page.
  TransactionContext txn = db_->BeginContext();
  ASSERT_SUCCESS(db_->DropTable(txn, "PlainTable"));
  ASSIGN_OR_ASSERT_FAIL(
      Table, successor,
      db_->CreateTable(
          txn, Schema("SuccessorTable", {Column("col1", ValueType::kInt64),
                                         Column("col2", ValueType::kVarChar),
                                         Column("col3", ValueType::kDouble)})));
  EXPECT_NE(successor.FirstPageId(), doomed_pid);
  ASSERT_SUCCESS(
      successor.Insert(txn.txn_, Row({Value(8), Value("new"), Value(1.5)}))
          .GetStatus());
  ASSERT_SUCCESS(txn.txn_.Abort());

  // The abort resurrected PlainTable with its row still visible.
  TransactionContext check = db_->BeginContext();
  StatusOr<Table> resurrected = db_->GetTable(check, "PlainTable");
  ASSERT_SUCCESS(resurrected.GetStatus());
  ASSERT_EQ(resurrected.Value().FirstPageId(), doomed_pid);
  ASSERT_NO_FATAL_FAILURE(
      ExpectScannedKeys(resurrected.Value(), check.txn_, {3}));
  EXPECT_FALSE(db_->GetTable(check, "SuccessorTable").HasValue());
  ASSERT_SUCCESS(check.PreCommit());
}

// Bug 32: undoing a destroy restored the page image in the pool but wrote
// no compensation record.  A crash afterwards replayed only the destroy
// (page -> free) and lost the restore, leaving the resurrected catalog
// entry pointing at a free page -- the table came back empty.
TEST_F(FullScanMvccFastPathTest, AbortedDestroyRestoreSurvivesCrash) {
  const Schema plain("PlainTable", {Column("col1", ValueType::kInt64),
                                    Column("col2", ValueType::kVarChar),
                                    Column("col3", ValueType::kDouble)});

  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, dropped, db_->CreateTable(seed, plain));
  ASSERT_SUCCESS(
      dropped.Insert(seed.txn_, Row({Value(7), Value("keep"), Value(1.0)}))
          .GetStatus());
  ASSERT_SUCCESS(seed.PreCommit());
  const page_id_t pid = dropped.FirstPageId();

  TransactionContext dropper = db_->BeginContext();
  ASSERT_SUCCESS(db_->DropTable(dropper, "PlainTable"));

  // Make the destroy record durable, then force the free image out to
  // disk: the abort's pool-side restore is then the only place the old
  // image lives, and only a logged CLR can bring it back after a crash.
  TransactionContext durability = db_->BeginContext();
  ASSERT_SUCCESS(durability.txn_.PreCommit());
  ASSERT_SUCCESS(
      dropper.txn_.GetPageManager()->GetPool()->FlushPageForTest(pid));

  ASSERT_SUCCESS(dropper.txn_.Abort());
  // Push the CLR + abort terminator out of the logger buffer so the crash
  // below leaves the full compensation trail in the WAL file.
  TransactionContext durability2 = db_->BeginContext();
  ASSERT_SUCCESS(durability2.txn_.PreCommit());

  db_->EmulateCrash();
  db_.reset();
  db_ = Database::Create(prefix_).MoveValue();

  TransactionContext check = db_->BeginContext();
  StatusOr<Table> resurrected = db_->GetTable(check, "PlainTable");
  ASSERT_SUCCESS(resurrected.GetStatus());
  ASSERT_NO_FATAL_FAILURE(
      ExpectScannedKeys(resurrected.Value(), check.txn_, {7}));
  ASSERT_SUCCESS(check.PreCommit());
}

TEST_F(FullScanMvccFastPathTest, CachedPageVerdictIsInvalidatedAfterLatchDrop) {
  auto setup = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, "SampleTable"));
  std::vector<RowPosition> positions;
  for (int i = 0; i < 3; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, position,
        table.Insert(setup.txn_,
                     Row({Value(i), Value("payload"), Value(0.5)})));
    positions.push_back(position);
  }
  ASSERT_SUCCESS(setup.PreCommit());
  AdvanceCommitClock(64);
  auto reader = db_->BeginReadOnlyContext();
  Iterator iterator = table.BeginFullScan(reader.txn_);
  ASSERT_TRUE(iterator.IsValid());
  EXPECT_EQ((*iterator)[0], Value(0));
  std::atomic<bool> writer_ok{false};
  std::jthread writer([&] {
    auto context = db_->BeginContext();
    const auto updated = table.Update(
        context.txn_, positions[2], Row({Value(99), Value("new"), Value(9.5)}));
    writer_ok = updated.HasValue() && context.PreCommit().ok();
  });
  bool chain_registered = false;
  for (int i = 0; i < 1000; ++i) {
    if (reader.txn_.HasPageVersionChains(positions[2].page_id)) {
      chain_registered = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  // A registered writer is blocked by our shared latch. The unchanged
  // physical image remains valid while that latch is held.
  ++iterator;
  const Value second = (*iterator)[0];
  iterator.DropPageLatch();
  writer.join();
  ASSERT_TRUE(chain_registered);
  ASSERT_TRUE(writer_ok.load());
  EXPECT_EQ(second, Value(1));
  ++iterator;  // re-pin must discard the cached physical-read verdict
  ASSERT_TRUE(iterator.IsValid());
  EXPECT_EQ((*iterator)[0], Value(2));
  iterator.DropPageLatch();
  TableScanOptions options;
  options.projection = std::vector<slot_t>{0};
  Iterator typed = table.BeginFullScan(reader.txn_, options);
  TypedCellBatch batch;
  batch.capture_positions = true;
  ASSERT_EQ(typed.DecodeNextTypedBatch(&batch, 1), 1U);
  typed.DropPageLatch();
  {
    auto update = db_->BeginContext();
    ASSERT_SUCCESS(table
                       .Update(update.txn_, positions[1],
                               Row({Value(77), Value("newer"), Value(7.5)}))
                       .GetStatus());
    ASSERT_SUCCESS(update.PreCommit());
  }
  ASSERT_EQ(typed.DecodeNextTypedBatch(&batch, 4), 2U);
  EXPECT_EQ(batch.cells[0], 1);
  EXPECT_EQ(batch.cells[1], 2);
  EXPECT_EQ(batch.positions,
            std::vector<RowPosition>({positions[1], positions[2]}));
  EXPECT_TRUE(typed.GetStatus().ok());
}

TEST_F(FullScanMvccFastPathTest, TypedProjectionEndsBeforeUnneededSuffix) {
  auto seed = db_->BeginContext();
  ASSERT_SUCCESS(
      db_->CreateTable(seed,
                       Schema("Narrow", {Column("a", ValueType::kInt64),
                                         Column("b", ValueType::kVarChar),
                                         Column("c", ValueType::kInt64),
                                         Column("d", ValueType::kVarChar),
                                         Column("e", ValueType::kDouble)}))
          .GetStatus());
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "Narrow"));
  std::vector<Row> expected;
  std::vector<RowPosition> positions;
  for (int i = 0; i < 73; ++i) {
    expected.emplace_back(
        Row({i % 3 ? Value(i) : Value(), Value(std::string(i, 'b')),
             i % 5 ? Value(100 + i) : Value(),
             i % 2 ? Value(std::string(128, 'd')) : Value(), Value(i + 0.5)}));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, position,
                          table.Insert(seed.txn_, expected.back()));
    positions.push_back(position);
  }
  ASSERT_SUCCESS(seed.PreCommit());
  auto reader = db_->BeginReadOnlyContext();
  for (int phase = 0; phase < 2; ++phase) {
    if (phase == 1) {
      auto writer = db_->BeginContext();
      ASSERT_SUCCESS(table
                         .Update(writer.txn_, positions[7],
                                 Row({Value(999), Value("changed"), Value(999),
                                      Value(), Value(9.5)}))
                         .GetStatus());
      ASSERT_SUCCESS(writer.PreCommit());
    }
    for (const std::vector<slot_t>& projection :
         {std::vector<slot_t>{0}, std::vector<slot_t>{2},
          std::vector<slot_t>{0, 2}}) {
      TableScanOptions options;
      options.projection = projection;
      Iterator iterator = table.BeginFullScan(reader.txn_, options);
      TypedCellBatch batch;
      batch.capture_positions = true;
      size_t seen = 0;
      while (size_t count = iterator.DecodeNextTypedBatch(&batch, 17)) {
        ASSERT_EQ(batch.stride, projection.size());
        ASSERT_LE(seen + count, expected.size());
        for (size_t row = 0; row < count; ++row) {
          EXPECT_EQ(batch.positions[row], positions[seen + row]);
          for (size_t c = 0; c < projection.size(); ++c) {
            const Value& value = expected[seen + row][projection[c]];
            const size_t cell = row * batch.stride + c;
            EXPECT_EQ(batch.nulls[cell] != 0, value.IsNull());
            if (!value.IsNull()) {
              EXPECT_EQ(batch.cells[cell], value.value.int_value);
            }
          }
        }
        seen += count;
        iterator.DropPageLatch();
      }
      EXPECT_EQ(seen, expected.size());
      EXPECT_TRUE(iterator.GetStatus().ok());
      EXPECT_FALSE(iterator.IsValid());
    }
  }
}

TEST_F(FullScanMvccFastPathTest,
       PlannedPeeksSkipFixedSpansAroundVariableCells) {
  auto seed = db_->BeginContext();
  ASSERT_SUCCESS(
      db_->CreateTable(seed,
                       Schema("MixedPeek", {Column("a", ValueType::kInt64),
                                            Column("b", ValueType::kDouble),
                                            Column("c", ValueType::kVarChar),
                                            Column("d", ValueType::kInt64),
                                            Column("e", ValueType::kVarChar),
                                            Column("f", ValueType::kDate),
                                            Column("g", ValueType::kInt64)}))
          .GetStatus());
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "MixedPeek"));
  std::vector<Row> rows;
  std::vector<RowPosition> positions;
  for (int i = 0; i < 97; ++i) {
    rows.emplace_back(
        Row({Value(i), i % 7 ? Value(i + .5) : Value(),
             i % 9 ? Value(std::string(i, 'c')) : Value(),
             i % 11 ? Value(i % 13) : Value(),
             i % 8 ? Value(std::string(i % 19, 'e')) : Value(),
             i % 6 ? Value::Date(i % 2 ? "1996-01-02" : "1996-01-03") : Value(),
             i % 5 ? Value(i % 17) : Value()}));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, position,
                          table.Insert(seed.txn_, rows.back()));
    positions.push_back(position);
  }
  ASSERT_SUCCESS(seed.PreCommit());
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (db_->TransactionStats().version_chains != 0 &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_EQ(db_->TransactionStats().version_chains, 0U);
  auto reader = db_->BeginReadOnlyContext();
  for (int phase = 0; phase < 2; ++phase) {
    if (phase == 1) {
      auto writer = db_->BeginContext();
      Row changed = rows[13];
      changed[2] = Value("new variable width");
      changed[3] = Value(1000);
      changed[5] = Value::Date("2000-01-01");
      changed[6] = Value(1000);
      ASSERT_SUCCESS(
          table.Update(writer.txn_, positions[13], changed).GetStatus());
      ASSERT_SUCCESS(writer.PreCommit());
    }
    for (slot_t column : {slot_t{0}, slot_t{3}, slot_t{5}, slot_t{6}}) {
      const int64_t low =
          column == 5 ? Value::Date("1996-01-02").value.int_value : 3;
      const int64_t high = column == 5 ? low : 9;
      const std::vector<IntegerPeekCompare> peeks{
          {column, BinaryOperation::kGreaterThanEquals, low},
          {column, BinaryOperation::kLessThanEquals, high}};
      const std::unordered_set<int64_t> keys{low, high};
      for (bool use_key_filter : {false, true}) {
        TableScanOptions options;
        options.projection = std::vector<slot_t>{0};
        options.peek_compares = &peeks;
        if (use_key_filter) {
          options.key_column = column;
          options.key_filter = &keys;
        }
        std::vector<int64_t> expected;
        for (const auto& row : rows) {
          if (row[column].IsNull()) continue;
          const int64_t value = row[column].value.int_value;
          if (low <= value && value <= high &&
              (!use_key_filter || keys.contains(value)))
            expected.push_back(row[0].value.int_value);
        }
        std::vector<int64_t> actual;
        for (auto it = table.BeginFullScan(reader.txn_, options); it.IsValid();
             ++it)
          actual.push_back((*it)[0].value.int_value);
        EXPECT_EQ(actual, expected)
            << "column=" << column << " phase=" << phase;
      }
    }
  }
}

TEST_F(FullScanMvccFastPathTest,
       IntegerKeyCountsValidatePagesAndRetainSnapshotFallback) {
  auto setup = db_->BeginContext();
  ASSERT_TRUE(
      db_->CreateTable(
             setup, Schema("CountedKeys", {Column("k", ValueType::kInt64),
                                           Column("p", ValueType::kVarChar)}))
          .HasValue());
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, "CountedKeys"));
  std::vector<RowPosition> positions;
  std::vector<uint64_t> expected(7);
  for (int64_t i = 0; i < 1201; ++i) {
    const Value key = i % 11 == 0 ? Value() : Value(i % 7 - 3);
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, pos,
        table.Insert(setup.txn_, Row({key, Value(std::string(128, 'p'))})));
    positions.push_back(pos);
    if (!key.IsNull()) ++expected[static_cast<size_t>(i % 7)];
  }
  ASSERT_SUCCESS(setup.PreCommit());
  auto old = db_->BeginReadOnlyContext();
  auto pages = table.BuildScanMorsels(old.txn_, 2);
  ASSERT_GT(pages.size(), 1U);
  const auto await_counts = [&](Transaction& txn) {
    std::shared_ptr<const Table::IntegerKeyCounts> counts;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
      counts = table.TryReadIntegerKeyCounts(txn, pages, 0, -3, 7);
      if (counts) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    return counts;
  };
  EXPECT_EQ(table.FindCachedIntegerKeyCounts(old.txn_, 0, -3, 7), nullptr);
  auto first = await_counts(old.txn_);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->rows, 1201U);
  EXPECT_EQ(first->counts, expected);
  EXPECT_EQ(table.FindCachedIntegerKeyCounts(old.txn_, 0, -3, 7), first);
  EXPECT_EQ(table.FindCachedIntegerKeyCounts(old.txn_, 0, -2, 7), nullptr);
  EXPECT_EQ(table.TryReadIntegerKeyCounts(old.txn_, pages, 0, -3, 7), first);
  Table copy = table;
  EXPECT_EQ(copy.TryReadIntegerKeyCounts(old.txn_, pages, 0, -3, 7), first);
  EXPECT_EQ(table.TryReadIntegerKeyCounts(old.txn_, pages, 1, -3, 7), nullptr);
  EXPECT_EQ(table.TryReadIntegerKeyCounts(old.txn_, pages, 0, 0, 65537),
            nullptr);
  EXPECT_EQ(table.TryReadIntegerKeyCounts(
                old.txn_, pages, 0, std::numeric_limits<int64_t>::max(), 2),
            nullptr);
  auto shifted = table.TryReadIntegerKeyCounts(old.txn_, pages, 0, -1, 3);
  ASSERT_NE(shifted, nullptr);
  EXPECT_EQ(shifted->counts,
            std::vector<uint64_t>({expected[2], expected[3], expected[4]}));
  auto incomplete = pages;
  incomplete.pop_back();
  EXPECT_EQ(table.TryReadIntegerKeyCounts(old.txn_, incomplete, 0, -3, 7),
            nullptr);
  auto writer = db_->BeginContext();
  EXPECT_EQ(table.TryReadIntegerKeyCounts(writer.txn_, pages, 0, -3, 7),
            nullptr);
  ASSERT_TRUE(table
                  .Update(writer.txn_, positions[2],
                          Row({Value(3), Value(std::string(128, 'p'))}))
                  .HasValue());
  EXPECT_EQ(table.TryReadIntegerKeyCounts(old.txn_, pages, 0, -3, 7), nullptr);
  EXPECT_EQ(table.FindCachedIntegerKeyCounts(old.txn_, 0, -3, 7), nullptr);
  ASSERT_SUCCESS(writer.PreCommit());
  EXPECT_EQ(table.TryReadIntegerKeyCounts(old.txn_, pages, 0, -3, 7), nullptr);
  // The canonical fallback still resolves the old image; a fresh snapshot
  // must not reuse a count produced for that old image.
  std::vector<uint64_t> old_counts(7);
  for (auto it = table.BeginFullScan(old.txn_); it.IsValid(); ++it) {
    const auto row = *it;
    if (!row[0].IsNull())
      ++old_counts[static_cast<size_t>(row[0].value.int_value + 3)];
  }
  EXPECT_EQ(old_counts, expected);
  auto fresh = db_->BeginReadOnlyContext();
  EXPECT_EQ(table.TryReadIntegerKeyCounts(fresh.txn_, pages, 0, -3, 7),
            nullptr);
  old.Abort();
  --expected[2];
  ++expected[6];
  auto changed = await_counts(fresh.txn_);
  ASSERT_NE(changed, nullptr);
  EXPECT_EQ(changed->counts, expected);
  EXPECT_EQ(table.FindCachedIntegerKeyCounts(fresh.txn_, 0, -3, 7), changed);
  EXPECT_NE(changed, first);
  // Abort rewrites the physical page through CLR without another Table DML
  // call. A previously cached histogram must be invalidated by page LSN.
  auto aborted = db_->BeginContext();
  ASSERT_TRUE(table
                  .Update(aborted.txn_, positions[2],
                          Row({Value(-3), Value(std::string(128, 'p'))}))
                  .HasValue());
  EXPECT_EQ(table.TryReadIntegerKeyCounts(fresh.txn_, pages, 0, -3, 7),
            nullptr);
  aborted.Abort();
  auto restored = await_counts(fresh.txn_);
  ASSERT_NE(restored, nullptr);
  EXPECT_EQ(restored->counts, expected);
  fresh.Abort();
  auto deleter = db_->BeginContext();
  ASSERT_SUCCESS(table.Delete(deleter.txn_, positions[1]));
  ASSERT_SUCCESS(deleter.PreCommit());
  auto after_delete = db_->BeginReadOnlyContext();
  --expected[1];
  auto holes = await_counts(after_delete.txn_);
  ASSERT_NE(holes, nullptr);
  EXPECT_EQ(holes->rows, 1200U);
  EXPECT_EQ(holes->counts, expected);
  EXPECT_EQ(table.FindCachedIntegerKeyCounts(after_delete.txn_, 0, -3, 7),
            holes);
  after_delete.Abort();
}

TEST_F(FullScanMvccFastPathTest,
       IntegerKeyCountsRejectAnUnpublishedTailOutsideSavedMorsels) {
  auto setup = db_->BeginContext();
  ASSERT_TRUE(
      db_->CreateTable(
             setup, Schema("CountedTail", {Column("k", ValueType::kInt64),
                                           Column("p", ValueType::kVarChar)}))
          .HasValue());
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, "CountedTail"));
  ASSERT_TRUE(
      table.Insert(setup.txn_, Row({Value(1), Value(std::string(30000, 'x'))}))
          .HasValue());
  ASSERT_SUCCESS(setup.PreCommit());
  auto reader = db_->BeginReadOnlyContext();
  const auto pages = table.BuildScanMorsels(reader.txn_);
  auto writer = db_->BeginContext();
  ASSERT_TRUE(
      table.Insert(writer.txn_, Row({Value(1), Value(std::string(30000, 'y'))}))
          .HasValue());
  EXPECT_EQ(table.TryReadIntegerKeyCounts(reader.txn_, pages, 0, 1, 1),
            nullptr);
  writer.Abort();
  reader.Abort();
}

}  // namespace tinylamb
