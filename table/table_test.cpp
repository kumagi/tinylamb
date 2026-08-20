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

#include <memory>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
static const char* kTableName = "SampleTable";

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

TEST_F(TableTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest death on crash, gtest green on pass
}

TEST_F(TableTest, Insert) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Row r({Value(1), Value("fuga"), Value(3.3)});

  // Act
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());

  // Assert -- implicit; gtest death on crash, gtest green on pass
}

TEST_F(TableTest, Read) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  Row r({Value(1), Value("string"), Value(3.3)});
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act
  ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, r));
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rp));

  // Assert
  ASSERT_EQ(read, r);
}

TEST_F(TableTest, Update) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Row new_row({Value(1), Value("hogefuga"), Value(99e8)});

  // Act
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));
  ASSERT_SUCCESS(tbl->Update(ctx.txn_, rp, new_row).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rp));

  // Assert
  ASSERT_EQ(read, new_row);
}

TEST_F(TableTest, UpdateMany) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::vector<RowPosition> rps;

  // Act -- insert 30 rows then update each 260 times via round-robin
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

  // Assert -- implicit; gtest death on crash, gtest green on pass
}

TEST_F(TableTest, Delete) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));
  ASSERT_SUCCESS(tbl->Delete(ctx.txn_, rp));

  // Assert -- reading deleted row position should fail
  ASSERT_FAIL(tbl->Read(ctx.txn_, rp).GetStatus());
}

TEST_F(TableTest, IndexRead) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- insert three rows into the indexed table
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(4.8)}))
          .GetStatus());
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(1.5)}))
          .GetStatus());

  // Assert -- TODO(kumagi): do index scan to verify indexed reads
}

TEST_F(TableTest, IndexUpdateRead) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- insert three rows then update one via index
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

  // Assert -- update produced same row position; TODO(kumagi): do index scan
  ASSERT_EQ(rp1, rp3);
}

TEST_F(TableTest, IndexUpdateDelete) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- insert three rows then delete one via index
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

  // Assert -- remaining row positions are distinct; TODO(kumagi): do index scan
  ASSERT_NE(rp2, rp3);
}

// Reproduces a table_fuzzer crash.  Inserting a second row whose key
// duplicates an existing UNIQUE index entry must be rejected with
// Status::kDuplicates instead of blowing up.  The fuzzer hit this through
// BPlusTree::Insert: Table::IndexInsert (table/table.cpp:218-223) forwards a
// unique-key duplicate straight into the tree, and BPlusTree::LeafInsert
// (index/b_plus_tree.cpp:187-208) treated LeafPage::Insert's kDuplicates
// (page/leaf_page.cpp:66-68) like "page full": it split the leaf and recursed
// into the right half, where the same duplicate still sat, so it never
// terminated and exhausted the stack (ASan: stack-overflow).  The B+ tree now
// propagates kDuplicates; this test pins that contract at the table level.
TEST_F(TableTest, InsertDuplicateUniqueKey) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, kTableName, IndexSchema("col1_unique", {0})));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_,
                             Row({Value(1), Value("first"), Value(3.3)}))
                     .GetStatus());

  // Act -- second row reuses the uniquely indexed col1 value
  StatusOr<RowPosition> dup = tbl->Insert(
      ctx.txn_, Row({Value(1), Value("second"), Value(4.4)}));

  // Assert -- the unique-key violation is reported as an error status.
  ASSERT_EQ(dup.GetStatus(), Status::kDuplicates);
}

// Follow-up on the same unique-key path: Table::Insert writes the row into the
// row page BEFORE it maintains the indexes, and when an index insert fails
// (kDuplicates above) it returns the error without rolling the row-page write
// back.  The rejected row therefore stays in the table and shows up in a full
// scan, so a statement the caller was told failed still modified the table.
TEST_F(TableTest, InsertDuplicateUniqueKeyLeavesNoOrphan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_,
                             Row({Value(1), Value("dup"), Value(3.3)}))
                     .GetStatus());

  // Act -- identical indexed key must be rejected...
  StatusOr<RowPosition> dup = tbl->Insert(
      ctx.txn_, Row({Value(1), Value("dup"), Value(5.5)}));
  ASSERT_EQ(dup.GetStatus(), Status::kDuplicates);

  // Assert -- the rejected insert must not have left a row behind; the table
  // may only contain the first row.
  size_t count = 0;
  Iterator it = tbl->BeginFullScan(ctx.txn_);
  while (it.IsValid()) {
    ++count;
    ++it;
  }
  ASSERT_EQ(count, 1u);
}

std::string KeyPayload(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(TableTest, InsertMany) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::unordered_set<Row> rows;
  std::unordered_set<RowPosition> rps;

  // Act -- insert 1000 rows with sequential keys and read each back
  for (int i = 0; i < 1000; ++i) {
    std::string key = KeyPayload(i, 1000);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.insert(rp);
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rp));
    ASSERT_EQ(read, new_row);
    rows.insert(new_row);
  }

  // Assert -- every inserted row is readable via its row position
  for (const auto& row : rps) {
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, row));
    ASSERT_NE(rows.find(read), rows.end());
  }
}

TEST_F(TableTest, UpdateHeavy) {
  // Arrange
  constexpr int kCount = 50;
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::unordered_set<Row> rows;
  std::vector<RowPosition> rps;
  rps.reserve(kCount);

  // Act -- insert kCount rows then update each kCount*4 times via round-robin
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.push_back(rp);
  }
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    const size_t target = (i * 63) % rps.size();
    RowPosition& pos = rps[target];
    std::string key = RandomString((19937 * i) % 1000 + 800, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Update(ctx.txn_, pos, new_row));
    rps[target] = rp;
  }

  // Assert -- implicit; gtest death on crash, gtest green on pass
}

TEST_F(TableTest, ReadOnlyUpdateConflicts) {
  // Arrange -- insert a committed row
  RowPosition rp;
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, inserted,
                          tbl->Insert(ctx.txn_,
                                      Row({Value(1), Value("string"),
                                           Value(3.3)})));
    rp = inserted;
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- update and delete through a read-only transaction
  TransactionContext ctx = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_EQ(tbl->Update(ctx.txn_, rp,
                        Row({Value(1), Value("new"), Value(4.4)}))
                .GetStatus(),
            Status::kConflicts);
  ASSERT_EQ(tbl->Delete(ctx.txn_, rp), Status::kConflicts);
}

TEST_F(TableTest, InsertAllocatesNewPageWhenFull) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::vector<RowPosition> rps;
  std::string payload(300, 'x');

  // Act -- insert 600 rows; the first row page fills and later rows spill
  for (int i = 0; i < 600; ++i) {
    Row r({Value(i), Value(std::string(payload)), Value(i * 1.5)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, r));
    rps.push_back(rp);
  }

  // Assert -- rows landed on more than one row page
  ASSERT_NE(rps.front().page_id, rps.back().page_id);
  // Assert -- every inserted row reads back intact across page boundaries
  for (int i = 0; i < 600; ++i) {
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rps[i]));
    ASSERT_EQ(read, Row({Value(i), Value(std::string(payload)), Value(i * 1.5)}));
  }
}

TEST_F(TableTest, UpdateNonIndexedColumnFastPath) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));

  // Act -- update only the (non-indexed) double column
  Row updated({Value(1), Value("string"), Value(9.9)});
  ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos,
                        tbl->Update(ctx.txn_, rp, updated));

  // Assert -- the indexed columns are unchanged so the row stays in place
  ASSERT_EQ(new_pos, rp);
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, new_pos));
  ASSERT_EQ(read, updated);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, IndexWithIncludeColumnUpdate) {
  // Arrange -- add an index whose include column is col3 (the double)
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(
      rs_->CreateIndex(ctx, kTableName, IndexSchema("idx_inc", {0}, {2})));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)})));

  // Act -- update the include column; the key column is unchanged
  Row updated({Value(1), Value("string"), Value(7.7)});
  ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos,
                        tbl->Update(ctx.txn_, rp, updated));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());

  // Assert -- the updated row reads back through a fresh transaction
  TransactionContext read_ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2,
                        read_ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl2->Read(read_ctx.txn_, new_pos));
  ASSERT_EQ(read, updated);
  ASSERT_SUCCESS(read_ctx.txn_.PreCommit());
}

TEST_F(TableTest, BuildScanMorselsAndMorselScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::string payload(200, 'x');
  for (int i = 0; i < 400; ++i) {
    Row r({Value(i), Value(std::string(payload)), Value(i * 1.5)});
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }

  // Act -- split the table into morsels of two pages each
  std::vector<Table::ScanMorsel> morsels = tbl->BuildScanMorsels(ctx.txn_, 2);
  ASSERT_FALSE(morsels.empty());

  // Assert -- scanning every morsel returns every inserted row exactly once
  size_t count = 0;
  for (const auto& morsel : morsels) {
    Iterator it = tbl->BeginMorselScan(ctx.txn_, morsel);
    while (it.IsValid()) {
      ++count;
      ++it;
    }
  }
  ASSERT_EQ(count, 400u);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, IndexScanFullRange) {
  // Arrange -- insert 20 rows into the indexed table
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  for (int i = 0; i < 20; ++i) {
    Row r({Value(i), Value("str" + std::to_string(i)), Value(1.0 * i)});
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }
  const Index& index = tbl->GetIndex(0);

  // Act -- scan the whole index ascending, descending, and via vector keys
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

  // Assert -- every scan sees the full 20-row table
  ASSERT_EQ(asc_count, 20);
  ASSERT_EQ(desc_count, 20);
  ASSERT_EQ(vec_count, 20);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(TableTest, AvailableKeyIndexAndStreamOperator) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- query which schema slots back the table's indexes
  auto available = tbl->AvailableKeyIndex();
  ASSERT_EQ(available.size(), 1u);
  ASSERT_EQ(available.count(0), 1u);

  // Act -- stream the table
  std::stringstream ss;
  ss << *tbl;
  EXPECT_NE(ss.str().find("Table(schema="), std::string::npos);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}
}  // namespace tinylamb
