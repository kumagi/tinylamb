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
// The fast path serves raw page bytes for pages whose PageLSN does not
// exceed the reader snapshot's commit timestamp; these tests pin down that
// such readers never observe a concurrent writer's post-snapshot change,
// whether the page is served through the fast path or through the
// ReadVersion fallback.

#include <cstdint>
#include <memory>
#include <string>
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
  // grows past the early pages' PageLSN values.
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

// A read-only scan over pages whose stamp qualifies for the physical path
// must return exactly the seeded rows -- no more, no fewer, correct values.
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

}  // namespace tinylamb
