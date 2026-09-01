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

#include "table/table.hpp"

#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/iterator.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
static constexpr std::string_view kTableName = "SampleTable";

class TableTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "table_test-" + RandomString();
    Recover();
    TransactionContext ctx = rs_->BeginContext();
    Schema schema(kTableName, {Column("col1", ValueType::kInt64,
                                      Constraint(Constraint::kIndex)),
                               Column("col2", ValueType::kVarChar),
                               Column("col3", ValueType::kDouble)});
    rs_->CreateTable(ctx, schema);
    IndexSchema idx("idx1", {0, 1});
    rs_->CreateIndex(ctx, schema.Name(), idx);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

TEST_F(TableTest, Construct_Default_Succeeds) {}

TEST_F(TableTest, Insert_SingleRow_Succeeds) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  Row r({Value(1), Value("fuga"), Value(3.3)});

  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
}

TEST_F(TableTest, Read_AfterInsert_ReturnsInsertedRow) {
  TransactionContext ctx = rs_->BeginContext();
  Row r({Value(1), Value("string"), Value(3.3)});
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));

  ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, r));
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, rp));

  ASSERT_EQ(read, r);
}

TEST_F(TableTest, Update_DuplicateKeyCompensation_DoesNotDeadlockOrLoseRow) {
  // PRODUCTION BUG (fixed): restore_physical_row() re-entered GetPage() on
  // the page whose exclusive latch the in-place update still held.  A failed
  // index insert (e.g. PK/UNIQUE violation) therefore threw
  // "Resource deadlock avoided" with the row already physically deleted.
  TransactionContext ctx = rs_->BeginContext();
  Schema schema("uniq_upd_tbl", {Column("id", ValueType::kInt64,
                                       Constraint(Constraint::kUnique)),
                                 Column("val", ValueType::kVarChar)});
  ASSIGN_OR_ASSERT_FAIL(Table, created, rs_->CreateTable(ctx, schema));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());

  TransactionContext ctx2 = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx2.GetTable("uniq_upd_tbl"));
  RowPosition first = tbl->Insert(ctx2.txn_, Row({Value(1), Value("a")})).Value();
  RowPosition second = tbl->Insert(ctx2.txn_, Row({Value(2), Value("b")})).Value();

  // Updating row 2 to id == 1 collides with row 1's unique key.
  const Status status = tbl->Update(ctx2.txn_, second,
                                    Row({Value(1), Value("b2")}))
                            .GetStatus();
  EXPECT_EQ(status, Status::kDuplicates);

  // Both rows must survive and read back their original values.
  ASSERT_SUCCESS_AND_EQ(tbl->Read(ctx2.txn_, first),
                        Row({Value(1), Value("a")}));
  ASSERT_SUCCESS_AND_EQ(tbl->Read(ctx2.txn_, second),
                        Row({Value(2), Value("b")}));
}

TEST_F(TableTest, Update_NoSpaceRecovery_PreservesRowWithoutIndex) {
  // PRODUCTION BUG (fixed): index-less tables never read the previous image,
  // so a failed relocation rewrote a 0-column (empty) image into the slot,
  // silently destroying the row.
  TransactionContext ctx = rs_->BeginContext();
  Schema schema("no_idx_tbl", {Column("c1", ValueType::kInt64),
                               Column("c2", ValueType::kVarChar)});
  ASSIGN_OR_ASSERT_FAIL(Table, created, rs_->CreateTable(ctx, schema));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());

  TransactionContext ctx2 = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx2.GetTable("no_idx_tbl"));
  const RowPosition rp = tbl->Insert(ctx2.txn_,
                                     Row({Value(1), Value("original")}))
                             .Value();

  // A 40000-byte varchar exceeds any page body; relocation must fail with
  // kNoSpace and the original row must survive.
  const Status status =
      tbl->Update(ctx2.txn_, rp,
                  Row({Value(1), Value(std::string(40000, 'y'))}))
          .GetStatus();
  EXPECT_EQ(status, Status::kNoSpace);
  ASSERT_SUCCESS_AND_EQ(tbl->Read(ctx2.txn_, rp),
                        Row({Value(1), Value("original")}));
}

TEST_F(TableTest, Update_SingleRow_UpdatesRowSuccessfully) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  Row new_row({Value(1), Value("hogefuga"), Value(99e8)});

  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));
  ASSERT_SUCCESS(tbl->Update(ctx.txn_, rp, new_row).GetStatus());
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, rp));

  ASSERT_EQ(read, new_row);
}

TEST_F(TableTest, Update_MultipleRowsRepeatedly_UpdatesSuccessfully) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  std::vector<RowPosition> rps;

  for (int i = 0; i < 30; ++i) {
    Row new_row({Value(i), Value(RandomString(20)), Value(i * 99e8)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.push_back(rp);
  }
  for (int i = 0; i < 260; ++i) {
    Row new_row({Value(i), Value(RandomString(40)), Value(i * 99e8)});
    RowPosition pos = rps[i % rps.size()];
    ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos,
                          tbl->Update(ctx.txn_, pos, new_row));
    rps[i % rps.size()] = new_pos;
  }
}

TEST_F(TableTest, Delete_ExistingRow_MakesRowUnreadable) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));

  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));
  ASSERT_SUCCESS(tbl->Delete(ctx.txn_, rp));

  ASSERT_FAIL(tbl->Read(ctx.txn_, rp).GetStatus());
}

TEST_F(TableTest, Insert_MultipleRowsWithIndex_Succeeds) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));

  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(4.8)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(1.5)}))
          .GetStatus());
}

TEST_F(TableTest, Update_RowWithIndex_PreservesRowPosition) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));

  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp0,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(4.8)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp2,
      tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(1.5)})));
  ASSERT_NE(rp0, rp2);
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp3,
      tbl->Update(ctx.txn_, rp1, Row({Value(2), Value("baz"), Value(5.8)})));

  ASSERT_EQ(rp1, rp3);
}

TEST_F(TableTest, Delete_RowWithIndex_PreservesRemainingRowPositions) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));

  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp2,
      tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(4.8)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp3,
      tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(1.5)})));
  ASSERT_SUCCESS(tbl->Delete(ctx.txn_, rp1));

  ASSERT_NE(rp2, rp3);
}

TEST_F(TableTest, Insert_DuplicateUniqueKey_ReturnsDuplicatesStatus) {
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, kTableName, IndexSchema("col1_unique", {0})));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_,
                             Row({Value(1), Value("first"), Value(3.3)}))
                     .GetStatus());

  StatusOr<RowPosition> dup = tbl->Insert(
      ctx.txn_, Row({Value(1), Value("second"), Value(4.4)}));

  ASSERT_EQ(dup.GetStatus(), Status::kDuplicates);
}

TEST_F(TableTest, Insert_DuplicateUniqueKey_LeavesNoOrphanRow) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_,
                             Row({Value(1), Value("dup"), Value(3.3)}))
                     .GetStatus());

  StatusOr<RowPosition> dup = tbl->Insert(
      ctx.txn_, Row({Value(1), Value("dup"), Value(5.5)}));
  ASSERT_EQ(dup.GetStatus(), Status::kDuplicates);

  size_t count = 0;
  Iterator it = tbl->BeginFullScan(ctx.txn_);
  while (it.IsValid()) {
    ++count;
    ++it;
  }
  ASSERT_EQ(count, 1U);
}

TEST_F(TableTest, IndexScan_VersionedUniqueIndex_ServesOldAndNewSnapshots) {
  {
    TransactionContext setup = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateIndex(
        setup, kTableName,
        IndexSchema("col3_versioned", {2}, {},
                    IndexMode::kVersionedUnique)));
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                setup.GetTable(kTableName));
    ASSERT_SUCCESS(
        table->Insert(setup.txn_,
                      Row({Value(1), Value("old"), Value(3.3)}))
            .GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }

  TransactionContext old_reader = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, old_table,
                              old_reader.GetTable(kTableName));
  const Index& versioned = old_table->GetIndex(old_table->IndexCount() - 1);

  {
    TransactionContext deleter = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                deleter.GetTable(kTableName));
    Iterator rows = table->BeginFullScan(deleter.txn_);
    ASSERT_TRUE(rows.IsValid());
    ASSERT_SUCCESS(table->Delete(deleter.txn_, rows.Position()));
    ASSERT_SUCCESS(deleter.PreCommit());
  }

  EXPECT_FALSE(old_reader.txn_.IndexKeysMayBeStale(versioned.Root()));

  {
    TransactionContext inserter = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                inserter.GetTable(kTableName));
    ASSERT_SUCCESS(
        table->Insert(inserter.txn_,
                      Row({Value(2), Value("new"), Value(3.3)}))
            .GetStatus());
    ASSERT_SUCCESS(inserter.PreCommit());
  }

  auto visible_rows = [&](TransactionContext& context,
                          const std::shared_ptr<Table>& table) {
    const Index& idx = table->GetIndex(table->IndexCount() - 1);
    Iterator scan = table->BeginIndexScan(context.txn_, idx, Value(3.3),
                                          Value(3.3));
    std::vector<Row> rows;
    while (scan.IsValid()) {
      if ((*scan).IsValid()) { rows.push_back(*scan); }
      ++scan;
    }
    return rows;
  };

  const std::vector<Row> old_rows = visible_rows(old_reader, old_table);
  ASSERT_EQ(old_rows.size(), 1U);
  EXPECT_EQ(old_rows[0][1], Value("old"));

  TransactionContext current = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, current_table,
                              current.GetTable(kTableName));
  const std::vector<Row> current_rows = visible_rows(current, current_table);
  ASSERT_EQ(current_rows.size(), 1U);
  EXPECT_EQ(current_rows[0][1], Value("new"));
  EXPECT_EQ(current_table
                ->Insert(current.txn_,
                         Row({Value(3), Value("duplicate"), Value(3.3)}))
                .GetStatus(),
            Status::kDuplicates);
  current.Abort();
  old_reader.Abort();
}

TEST_F(TableTest, Insert_WhenSlotIsConflicted_SkipsHoleAndMaintainsIndexEntry) {
  TransactionContext setup = rs_->BeginContext();
  ASSERT_SUCCESS(
      rs_->CreateIndex(setup, kTableName, IndexSchema("probe_unique", {0})));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl0,
                              setup.GetTable(kTableName));
  ASSERT_SUCCESS(tbl0->Insert(setup.txn_,
                              Row({Value(10), Value("ctrl"), Value(9.9)}))
                     .GetStatus());
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  TransactionContext t1 = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl1,
                              t1.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl1->Insert(t1.txn_, Row({Value(1), Value("a"), Value(1.1)})));
  ASSERT_SUCCESS(tbl1->Delete(t1.txn_, rp1));

  TransactionContext t2 = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl2,
                              t2.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, inserted,
      tbl2->Insert(t2.txn_, Row({Value(2), Value("b"), Value(2.2)})));
  EXPECT_NE(inserted, rp1);

  size_t rows = 0;
  Iterator scan = tbl2->BeginFullScan(t2.txn_);
  while (scan.IsValid()) {
    ++rows;
    ++scan;
  }
  ASSERT_EQ(rows, 2U);

  size_t probe_entries = 0;
  for (size_t i = 0; i < tbl2->IndexCount(); ++i) {
    if (tbl2->GetIndex(i).sc_.name_ == "probe_unique") {
      Iterator key_scan = tbl2->BeginIndexScan(t2.txn_, tbl2->GetIndex(i),
                                               Value(2), Value(2), true);
      while (key_scan.IsValid()) {
        ++probe_entries;
        ++key_scan;
      }
    }
  }
  ASSERT_EQ(probe_entries, 1U);

  for (size_t i = 0; i < tbl2->IndexCount(); ++i) {
    if (tbl2->GetIndex(i).sc_.name_ == "probe_unique") {
      Iterator ctrl_scan = tbl2->BeginIndexScan(t2.txn_, tbl2->GetIndex(i),
                                                Value(10), Value(10), true);
      ASSERT_TRUE(ctrl_scan.IsValid());
    }
  }
  ASSERT_SUCCESS(t2.PreCommit());
  t1.Abort();
}

TEST_F(TableTest, CreateIndex_WhenBackfillFails_CleansUpHalfBuiltIndex) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(7), Value("a"), Value(3.3)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(7), Value("b"), Value(4.4)}))
          .GetStatus());
  const size_t indexes_before = tbl->IndexCount();

  Status created =
      rs_->CreateIndex(ctx, kTableName, IndexSchema("dup_idx", {0}));
  ASSERT_EQ(created, Status::kDuplicates);

  ASSERT_EQ(tbl->IndexCount(), indexes_before);
  TransactionContext fresh_ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, reloaded,
                              fresh_ctx.GetTable(kTableName));
  ASSERT_EQ(reloaded->IndexCount(), indexes_before);

  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(8), Value("c"), Value(5.5)}))
          .GetStatus());

  ASSERT_SUCCESS(rs_->CreateIndex(fresh_ctx, kTableName,
                                  IndexSchema("col3_idx", {2})));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Delete_WhenRowDeleted_RemovesOnlyItsOwnIndexEntries) {
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, kTableName, IndexSchema("col1_idx", {0})));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(1), Value("one"), Value(1.1)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(2), Value("two"), Value(2.2)}))
          .GetStatus());

  std::vector<RowPosition> victims;
  const Index& by_col1 = tbl->GetIndex(1);
  {
    Iterator it = tbl->BeginIndexScan(ctx.txn_, by_col1);
    while (it.IsValid()) {
      victims.push_back(it.Position());
      ++it;
    }
  }
  ASSERT_EQ(victims.size(), 2U);
  ASSERT_SUCCESS(tbl->Delete(ctx.txn_, victims.front()));

  size_t remaining = 0;
  Iterator it = tbl->BeginIndexScan(ctx.txn_, by_col1);
  while (it.IsValid()) {
    ++remaining;
    EXPECT_NE(it.Position(), victims.front());
    ++it;
  }
  ASSERT_EQ(remaining, 1U);
}

namespace {
std::string KeyPayload(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}
}  // namespace

TEST_F(TableTest, Insert_ManyRows_AllRowsAreReadable) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  std::unordered_set<Row> rows;
  std::unordered_set<RowPosition> rps;

  for (int i = 0; i < 1000; ++i) {
    std::string key = KeyPayload(i, 1000);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.insert(rp);
    ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, rp));
    ASSERT_EQ(read, new_row);
    rows.insert(new_row);
  }

  for (const auto& row : rps) {
    ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, row));
    ASSERT_NE(rows.find(read), rows.end());
  }
}

TEST_F(TableTest, Update_ManyRowsUnderHeavyLoad_UpdatesSuccessfully) {
  constexpr int kCount = 50;
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  std::unordered_set<Row> rows;
  std::vector<RowPosition> rps;
  rps.reserve(kCount);

  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString(((19937 * i) % 120) + 10, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.push_back(rp);
  }
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    const size_t target = (static_cast<size_t>(i) * 63) % rps.size();
    RowPosition& pos = rps[target];
    std::string key = RandomString(((19937 * i) % 1000) + 800, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Update(ctx.txn_, pos, new_row));
    rps[target] = rp;
  }
}

TEST_F(TableTest, Update_ReadOnlyTransaction_ReturnsConflicts) {
  RowPosition rp;
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable(kTableName));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, inserted,
                          tbl->Insert(ctx.txn_,
                                      Row({Value(1), Value("string"),
                                           Value(3.3)})));
    rp = inserted;
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  TransactionContext ctx = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSERT_EQ(tbl->Update(ctx.txn_, rp,
                        Row({Value(1), Value("new"), Value(4.4)}))
                .GetStatus(),
            Status::kConflicts);
  ASSERT_EQ(tbl->Delete(ctx.txn_, rp), Status::kConflicts);
}

TEST_F(TableTest, Insert_WhenPageIsFull_AllocatesNewPage) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  std::vector<RowPosition> rps;
  std::string payload(300, 'x');

  for (int i = 0; i < 600; ++i) {
    Row r({Value(i), Value(std::string(payload)), Value(i * 1.5)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, r));
    rps.push_back(rp);
  }

  ASSERT_NE(rps.front().page_id, rps.back().page_id);
  for (int i = 0; i < 600; ++i) {
    ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, rps[i]));
    ASSERT_EQ(read, Row({Value(i), Value(std::string(payload)), Value(i * 1.5)}));
  }
}

TEST_F(TableTest, Update_NonIndexedColumn_PreservesRowPosition) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));

  Row updated({Value(1), Value("string"), Value(9.9)});
  ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos,
                        tbl->Update(ctx.txn_, rp, updated));

  ASSERT_EQ(new_pos, rp);
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, new_pos));
  ASSERT_EQ(read, updated);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Update_IncludeColumn_UpdatesRowSuccessfully) {
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, kTableName, IndexSchema("idx_inc", {0}, {2})));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));

  Row updated({Value(1), Value("string"), Value(7.7)});
  ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos,
                        tbl->Update(ctx.txn_, rp, updated));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());

  TransactionContext read_ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl2,
                              read_ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl2->Read(read_ctx.txn_, new_pos));
  ASSERT_EQ(read, updated);
  ASSERT_SUCCESS(read_ctx.txn_.PreCommit());
}

TEST_F(TableTest, BeginMorselScan_WithMorsels_ScansAllRows) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  std::string payload(200, 'x');
  for (int i = 0; i < 400; ++i) {
    Row r({Value(i), Value(std::string(payload)), Value(i * 1.5)});
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }

  std::vector<Table::ScanMorsel> morsels = tbl->BuildScanMorsels(ctx.txn_, 2);
  ASSERT_FALSE(morsels.empty());

  size_t count = 0;
  for (const auto& morsel : morsels) {
    Iterator it = tbl->BeginMorselScan(ctx.txn_, morsel);
    while (it.IsValid()) {
      ++count;
      ++it;
    }
  }
  ASSERT_EQ(count, 400U);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, BeginIndexScan_FullRange_ScansAllRows) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 20; ++i) {
    Row r({Value(i), Value("str" + std::to_string(i)), Value(1.0 * i)});
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }
  const Index& index = tbl->GetIndex(0);

  Iterator asc = tbl->BeginIndexScan(ctx.txn_, index, Value(), Value(), true);
  int asc_count = 0;
  while (asc.IsValid()) {
    ++asc_count;
    ++asc;
  }
  Iterator desc =
      tbl->BeginIndexScan(ctx.txn_, index, Value(), Value(), false);
  int desc_count = 0;
  while (desc.IsValid()) {
    ++desc_count;
    ++desc;
  }
  Iterator vec = tbl->BeginIndexScan(ctx.txn_, index, std::vector<Value>{},
                                     std::vector<Value>{}, true);
  int vec_count = 0;
  while (vec.IsValid()) {
    ++vec_count;
    ++vec;
  }

  ASSERT_EQ(asc_count, 20);
  ASSERT_EQ(desc_count, 20);
  ASSERT_EQ(vec_count, 20);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, AvailableKeyIndex_WhenQueried_ReturnsSchemaSlotsAndOutputsStream) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));

  auto available = tbl->AvailableKeyIndex();
  ASSERT_EQ(available.size(), 1U);
  ASSERT_EQ(available.count(0), 1U);

  std::stringstream ss;
  ss << *tbl;
  EXPECT_NE(ss.str().find("Table(schema="), std::string::npos);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Recover_AfterCrashWithIndex_RestoresCatalogAndIndex) {
  Recover();
  TransactionContext ctx = rs_->BeginContext();

  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  EXPECT_EQ(tbl->GetSchema().Name(), kTableName);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Recover_AfterCommitAndCrash_RestoresCommittedRow) {
  RowPosition rp;
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable(kTableName));
    const Row r({Value(42), Value("survivor"), Value(3.5)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, inserted, tbl->Insert(ctx.txn_, r));
    rp = inserted;
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  Recover();

  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL_CONST(Row, read, tbl->Read(ctx.txn_, rp));
  EXPECT_EQ(read, (Row({Value(42), Value("survivor"), Value(3.5)})));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Recover_AcrossTwoCrashCycles_AccumulatesAllCommittedRows) {
  auto InsertRow = [this](int64_t id, const char* name) {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable(kTableName));
    EXPECT_TRUE(tbl->Insert(ctx.txn_,
                            Row({Value(id), Value(name), Value(1.0)}))
                    .HasValue());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  };
  InsertRow(1, "before-crash");
  Recover();
  InsertRow(2, "after-crash");
  Recover();

  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  size_t seen = 0;
  for (Iterator iter = tbl->BeginFullScan(ctx.txn_); iter.IsValid(); ++iter) {
    ++seen;
    const Row& row = *iter;
    ASSERT_EQ(row.values_.size(), 3U);
    EXPECT_TRUE(row.values_[0].value.int_value == 1 ||
                row.values_[0].value.int_value == 2);
  }
  EXPECT_EQ(seen, 2U);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, BeginFullScan_AcrossMultiplePages_ReturnsAllRows) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 20; ++i) {
    std::string large_payload(4096, 'a');
    Row r({Value(i), Value(std::move(large_payload)), Value(i * 1.5)});
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }

  size_t count = 0;
  for (Iterator iter = tbl->BeginFullScan(ctx.txn_); iter.IsValid(); ++iter) {
    ++count;
    EXPECT_EQ((*iter).values_.size(), 3U);
  }
  EXPECT_EQ(count, 20U);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Update_IndexedRow_ReadsBackUpdatedValues) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  Row initial_row({Value(100), Value("initial"), Value(1.0)});
  ASSIGN_OR_ASSERT_FAIL(RowPosition, pos, tbl->Insert(ctx.txn_, initial_row));

  Row updated_row({Value(200), Value("updated"), Value(2.0)});
  ASSERT_SUCCESS(tbl->Update(ctx.txn_, pos, updated_row).GetStatus());

  ASSIGN_OR_ASSERT_FAIL_CONST(Row, read_back, tbl->Read(ctx.txn_, pos));
  EXPECT_EQ(read_back[0], Value(200));
  EXPECT_EQ(read_back[1], Value("updated"));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, Serialize_TooLargeRow_ThrowsRuntimeException) {
  std::string huge_payload(65536, 'x');
  Row huge_row({Value(1), Value(std::move(huge_payload)), Value(1.0)});
  std::vector<char> buf(huge_row.Size());
  EXPECT_THROW((void)huge_row.Serialize(buf.data()), std::runtime_error);
}

TEST_F(TableTest, BeginFullScan_WithProjectionAndKeyFilter_ReturnsMatchingRows) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 10; ++i) {
    Row r({Value(i), Value("row_" + std::to_string(i)), Value(static_cast<double>(i))});
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }

  std::vector<slot_t> proj = {0, 2};
  std::unordered_set<int64_t> key_set = {2, 5, 8};
  Iterator iter = tbl->BeginFullScan(ctx.txn_, proj, &key_set, 0, nullptr);
  size_t count = 0;
  for (; iter.IsValid(); ++iter) {
    ++count;
    Row r = *iter;
    EXPECT_EQ(r.values_.size(), 2U);
  }
  EXPECT_EQ(count, 3U);

  auto morsels = tbl->BuildScanMorsels(ctx.txn_, 2);
  EXPECT_FALSE(morsels.empty());
  Iterator m_iter = tbl->BeginMorselScan(ctx.txn_, morsels.front(), proj);
  EXPECT_TRUE(m_iter.IsValid());

  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

}  // namespace tinylamb






