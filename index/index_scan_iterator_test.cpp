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

#include "index_scan_iterator.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "index.hpp"
#include "index_schema.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
class IndexScanIteratorTest : public ::testing::Test {
 public:
  static constexpr std::string_view kTableName = "SampleTable";

  void SetUp() override {
    prefix_ = "index_scan_iterator_test-" + RandomString();
    Recover();
    Schema sc(kTableName, {Column("col1", ValueType::kInt64,
                                  Constraint(Constraint::kIndex)),
                           Column("col2", ValueType::kVarChar),
                           Column("col3", ValueType::kDouble)});
    TransactionContext ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->CreateTable(ctx, sc).GetStatus());
    ASSERT_SUCCESS(db_->CreateIndex(ctx, kTableName, IndexSchema("PK", {0})));
    ASSERT_SUCCESS(db_->CreateIndex(
        ctx, kTableName,
        IndexSchema("NameIdx", {1}, {2}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(db_->CreateIndex(ctx, kTableName,
                                    IndexSchema("KeyScore", {0, 2}, {1})));
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                ctx.GetTable(kTableName));
    ASSERT_EQ(table->IndexCount(), 3);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (db_) {
      db_->EmulateCrash();
    }
    db_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(IndexScanIteratorTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(IndexScanIteratorTest, ScanAscending) {
  // Arrange -- begin context, get table, insert 230 rows with ascending PK
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }

  // Act -- begin index scan on PK index between 43 and 180, iterate forward
  Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(43),
                                      Value(180));
  ASSERT_TRUE(it.IsValid());

  // Assert -- iterator yields rows 43..180 in ascending order with expected
  // values
  for (int i = 43; i <= 180; ++i) {
    Row cur = *it;
    ASSERT_EQ(cur[0], Value(i));
    ASSERT_EQ(cur[1], Value("v" + std::to_string(i)));
    ASSERT_EQ(cur[2], Value(0.1 + i));
    ++it;
  }
  ASSERT_FALSE(it.IsValid());
}

TEST_F(IndexScanIteratorTest, NonUniqueAscending) {
  // Arrange -- begin context, get table, insert 120 rows with duplicate NameIdx
  // values
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 120; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_,
                     Row({Value(i), Value("v" + std::to_string(i % 10)),
                          Value(static_cast<double>(i * 2))}))
            .GetStatus());
  }

  // Act 1 -- partial scan on NameIdx "v2".."v7" (12 rows per value, 6 values)
  {
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1),
                                        Value("v2"), Value("v7"));
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      // Assert -- each row has int_value * 2 == double_value
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      ++it;
      ++counter;
    }
    // Assert -- partial scan covers 12*(7-2+1) = 72 rows
    ASSERT_EQ(counter, 12 * (7 - 2 + 1));
    ASSERT_FALSE(it.IsValid());
  }

  // Act 2 -- full scan on NameIdx (all 120 rows)
  {
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1));
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      ++it;
      ++counter;
    }
    // Assert -- full scan covers 120 rows
    ASSERT_EQ(counter, 120);
    ASSERT_FALSE(it.IsValid());
  }

  // Act 3 -- delete rows where PK % 5 == 0 via full table scan
  {
    Iterator it = table->BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    while (it.IsValid()) {
      Row row = *it;
      if (row[0].value.int_value % 5 == 0) {
        table->Delete(ctx.txn_, it.Position());
      }
      ++it;
    }
    ASSERT_FALSE(it.IsValid());
  }

  // Act 4 -- full scan again after deletion
  {
    Iterator it = table->BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      ++it;
      ++counter;
    }
    // Assert -- after deleting 24 rows (PK%5==0 out of 120), 96 remain
    ASSERT_EQ(counter, 96);
    ASSERT_FALSE(it.IsValid());
  }
}

TEST_F(IndexScanIteratorTest, ScanDescending) {
  // Arrange -- begin context, get table, insert 230 rows with ascending PK
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }

  // Act -- begin descending index scan on PK between 104 and 200, iterate
  // backward
  Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(104),
                                      Value(200), false);
  ASSERT_TRUE(it.IsValid());

  // Assert -- iterator yields rows 200 down to 104 in descending order
  for (int i = 200; i >= 104; --i) {
    Row cur = *it;
    ASSERT_EQ(cur[0], Value(i));
    ASSERT_EQ(cur[1], Value("v" + std::to_string(i)));
    ASSERT_EQ(cur[2], Value(0.1 + i));
    --it;
  }
  ASSERT_FALSE(it.IsValid());
}

TEST_F(IndexScanIteratorTest, NonUniqueDescending) {
  // Arrange -- begin context, get table, insert 120 rows with duplicate NameIdx
  // values
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 120; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_,
                     Row({Value(i), Value("v" + std::to_string(i % 10)),
                          Value(static_cast<double>(i * 2))}))
            .GetStatus());
  }

  // Act 1 -- partial descending scan on NameIdx "v2".."v7"
  {
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1),
                                        Value("v2"), Value("v7"), false);
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      // Assert -- each row has int_value * 2 == double_value
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      --it;
      ++counter;
    }
    // Assert -- partial scan covers 12*(7-2+1) = 72 rows
    ASSERT_EQ(counter, 12 * (7 - 2 + 1));
    ASSERT_FALSE(it.IsValid());
  }

  // Act 2 -- full descending scan on NameIdx (all 120 rows)
  {
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1), Value(),
                                        Value(), false);
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      --it;
      ++counter;
    }
    // Assert -- full scan covers 120 rows
    ASSERT_EQ(counter, 120);
    ASSERT_FALSE(it.IsValid());
  }
}

TEST_F(IndexScanIteratorTest, CompositeEqualityPointLookup) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 40; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  const Index& key_score = table->GetIndex(2);
  Iterator it =
      table->BeginIndexScan(ctx.txn_, key_score, {Value(32), Value(0.1 + 32)},
                            {Value(32), Value(0.1 + 32)});
  ASSERT_TRUE(it.IsValid());
  Row row = *it;
  EXPECT_EQ(row[0], Value(32));
  EXPECT_EQ(row[1], Value("v32"));
  ++it;
  EXPECT_FALSE(it.IsValid());
}

TEST_F(IndexScanIteratorTest, NonUniqueDescendingIncrement) {
  // Arrange -- insert rows with duplicate NameIdx values
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 120; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_,
                     Row({Value(i), Value("v" + std::to_string(i % 10)),
                          Value(static_cast<double>(i * 2))}))
            .GetStatus());
  }

  // Act -- walk a descending non-unique scan using operator++ (moves across
  // value_offset_ within one key and across keys when it wraps)
  Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1), Value("v0"),
                                      Value("v3"), false);
  ASSERT_TRUE(it.IsValid());
  int counter = 0;
  while (it.IsValid()) {
    Row row = *it;
    ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
    ++it;
    ++counter;
  }

  // Assert -- v0..v3 inclusive covers 4 distinct values x 12 rows
  ASSERT_EQ(counter, 12 * 4);
  ASSERT_FALSE(it.IsValid());
}

TEST_F(IndexScanIteratorTest, DirectScanConstDereferenceAndAccessors) {
  // Arrange -- insert rows and construct an IndexScanIterator directly
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 20; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  const Index& pk = table->GetIndex(0);
  IndexScanIterator scan(*table, pk, ctx.txn_, Value(3), Value(8), true);
  ASSERT_TRUE(scan.IsValid());

  // Act -- dereference through a const reference (const operator*)
  const IndexScanIterator& cscan = scan;
  const Row& first = *cscan;
  EXPECT_EQ(first[0], Value(3));
  EXPECT_EQ(scan.GetKey()[0], Value(3));
  EXPECT_FALSE(scan.GetValue().empty());
  EXPECT_TRUE(scan.IsUnique());
  RowPosition pos = scan.Position();
  EXPECT_TRUE(pos.IsValid());

  // Act -- exhaust the scan and query the invalid position
  while (scan.IsValid()) {
    ++scan;
  }
  ASSERT_FALSE(scan.IsValid());
  EXPECT_FALSE(scan.Position().IsValid());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

// Regression for improvements2.md 4.11: a cleared iterator must report
// invalid (and an invalid Position) instead of leaking a valid-looking
// {0,0} row position to IndexScan::Next.
TEST_F(IndexScanIteratorTest, ClearInvalidatesTheIterator) {
  // Arrange -- insert rows and construct a direct scan over the PK index
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 10; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  const Index& pk = table->GetIndex(0);
  IndexScanIterator scan(*table, pk, ctx.txn_, Value(3), Value(8), true);
  ASSERT_TRUE(scan.IsValid());

  // Act -- clear while positioned on a live entry
  scan.Clear();

  // Assert -- every validity probe reports exhausted
  EXPECT_FALSE(scan.IsValid());
  EXPECT_FALSE(scan.Position().IsValid());
  ++scan;
  EXPECT_FALSE(scan.IsValid());
  --scan;
  EXPECT_FALSE(scan.IsValid());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, CompositeKeyAndIncludeAccessors) {
  // Arrange -- insert rows, then construct a direct scan over the composite
  // KeyScore index whose include set carries col1
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 10; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  const Index& key_score = table->GetIndex(2);
  IndexScanIterator scan(*table, key_score, ctx.txn_,
                         {Value(5), Value(0.1 + 5)},
                         {Value(5), Value(0.1 + 5)});
  ASSERT_TRUE(scan.IsValid());

  // Assert -- GetKey yields the composite key, Include yields the extra column
  EXPECT_EQ(scan.GetKey()[0], Value(5));
  EXPECT_EQ(scan.GetKey()[1], Value(0.1 + 5));
  EXPECT_EQ(scan.Include()[0], Value("v5"));
  EXPECT_FALSE(scan.GetValue().empty());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, EmptyRangeAndDump) {
  // Arrange -- insert a handful of rows
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 5; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }

  // Act -- a range entirely above the data yields an invalid iterator
  Iterator empty = table->BeginIndexScan(ctx.txn_, table->GetIndex(0),
                                         Value(100), Value(200));
  ASSERT_FALSE(empty.IsValid());
  EXPECT_FALSE(empty.Position().IsValid());

  // Act -- dump an ascending and a descending scan (the descending dump prints
  // end -> begin)
  Iterator ascending =
      table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(0), Value(3));
  std::ostringstream asc_dump;
  asc_dump << ascending;
  EXPECT_NE(asc_dump.str().find("SampleTable"), std::string::npos);

  Iterator descending = table->BeginIndexScan(ctx.txn_, table->GetIndex(0),
                                              Value(0), Value(3), false);
  std::ostringstream desc_dump;
  desc_dump << descending;
  EXPECT_NE(desc_dump.str().find("SampleTable"), std::string::npos);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, UniqueDescendingIncrement) {
  // Arrange -- insert rows with a unique PK
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 20; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }

  // Act -- walk a descending unique scan with operator++ (moves via --iter_)
  Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(0),
                                      Value(10), false);
  ASSERT_TRUE(it.IsValid());
  int expected = 10;
  while (it.IsValid()) {
    Row row = *it;
    EXPECT_EQ(row[0], Value(expected));
    ++it;
    --expected;
  }
  EXPECT_EQ(expected, -1);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, CompositePrefixRangeScan) {
  // Arrange -- insert rows, then scan the two-part KeyScore index with a
  // single-part bound so the end key is widened with a 0xff terminator
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 40; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }

  // Act -- prefix scan covering col0 in [10, 20]
  Iterator it =
      table->BeginIndexScan(ctx.txn_, table->GetIndex(2), Value(10), Value(20));
  ASSERT_TRUE(it.IsValid());
  int counter = 0;
  while (it.IsValid()) {
    Row row = *it;
    EXPECT_NEAR(row[0].value.int_value, row[2].value.double_value - 0.1, 1e-9);
    ++it;
    ++counter;
  }
  // Assert -- 11 distinct col0 keys survive the prefix bound
  ASSERT_EQ(counter, 11);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, DumpCompositeAndPointScans) {
  // Arrange -- insert rows and commit so a new index can be built on a fresh
  // context (the cached Table object does not observe later index creation)
  {
    TransactionContext ctx = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                ctx.GetTable(kTableName));
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                      Value(0.1 + i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_SUCCESS(
        db_->CreateIndex(ctx, kTableName, IndexSchema("TwoInc", {0}, {1, 2})));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  ASSERT_EQ(table->IndexCount(), 4);

  // Act -- dump a scan over the include-bearing NameIdx
  Iterator name_it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1),
                                           Value("v0"), Value("v3"));
  std::ostringstream name_dump;
  name_dump << name_it;
  EXPECT_NE(name_dump.str().find("NameIdx"), std::string::npos);
  EXPECT_NE(name_dump.str().find("Include:"), std::string::npos);

  // Act -- dump a scan over the two-key-column KeyScore index
  Iterator key_it =
      table->BeginIndexScan(ctx.txn_, table->GetIndex(2), Value(0), Value(3));
  std::ostringstream key_dump;
  key_dump << key_it;
  EXPECT_NE(key_dump.str().find("KeyScore"), std::string::npos);

  // Act -- dump a scan over the two-include TwoInc index
  Iterator two_inc_it =
      table->BeginIndexScan(ctx.txn_, table->GetIndex(3), Value(0), Value(3));
  std::ostringstream two_inc_dump;
  two_inc_dump << two_inc_it;
  EXPECT_NE(two_inc_dump.str().find("TwoInc"), std::string::npos);

  // Act -- dump a point scan where begin equals end
  Iterator point =
      table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(2), Value(2));
  std::ostringstream point_dump;
  point_dump << point;
  EXPECT_NE(point_dump.str().find("SampleTable"), std::string::npos);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}
}  // namespace tinylamb
