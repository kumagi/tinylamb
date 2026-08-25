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
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

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
    db_ = std::make_unique<Database>(prefix_);
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

// A read-only scan over pages whose stamp qualifies for the physical path
// must return exactly the seeded rows -- no more, no fewer, correct values.
TEST_F(FullScanMvccFastPathTest, ReadOnlyScanReturnsSeededRowsVerbatim) {
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
       ReaderKeepsOldValueWhenWriterUpdatesSamePageAfterBegin) {
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
       ReaderKeepsDeletedRowVisibleWhileSnapshotIsOpen) {
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

}  // namespace tinylamb
