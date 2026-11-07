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

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "index.hpp"
#include "index_schema.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

void ForceIndexScanWalFailure(Logger* logger) {
  const int full_fd = ::open("/dev/full", O_WRONLY);
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(::dup2(full_fd, logger->Fd()), logger->Fd());
  ASSERT_EQ(::close(full_fd), 0);
  std::ignore = logger->AddLog("dead");
  ASSERT_NE(logger->WaitForDurable(logger->BufferedLSN()), Status::kSuccess);
  ASSERT_TRUE(logger->Failed());
}

}  // namespace

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
      db_.reset();
    }
    db_ = Database::Create(prefix_).MoveValue();
  }

  // Waits (bounded) for the background GC worker to drop every committed
  // version chain: a page without chains is the index-only visibility-map
  // condition, so tests that need the fast path to fire wait here first.
  void WaitForChainFreePages() {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (db_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "background GC did not drop version chains";
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
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

TEST_F(IndexScanIteratorTest, ProjectedDecodeCanRebindWithoutChangingPosition) {
  auto setup = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        setup.GetTable(kTableName));
  ASSERT_TRUE(
      table->Insert(setup.txn_, Row({Value(1), Value("text"), Value(2.5)}))
          .HasValue());
  ASSERT_SUCCESS(setup.PreCommit());
  auto read = db_->BeginReadOnlyContext();
  IndexScanIterator iterator(*table, table->GetIndex(0), read.txn_, Value(1),
                             Value(1));
  ASSERT_TRUE(iterator.IsValid());
  const auto position = iterator.Position();
  EXPECT_EQ(*iterator, Row({Value(1), Value("text"), Value(2.5)}));
  ASSERT_SUCCESS(iterator.SetProjection({0, 2}));
  EXPECT_EQ(*iterator, Row({Value(1), Value(2.5)}));
  ASSERT_SUCCESS(iterator.SetProjection({1}));
  EXPECT_EQ(*iterator, Row({Value("text")}));
  EXPECT_EQ(iterator.Position(), position);
  EXPECT_NE(iterator.SetProjection({99}), Status::kSuccess);
  EXPECT_EQ(*iterator, Row({Value("text")}));
  ++iterator;
  EXPECT_FALSE(iterator.IsValid());
  ASSERT_SUCCESS(iterator.GetStatus());
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
      ASSERT_DOUBLE_EQ(static_cast<double>(row[0].value.int_value * 2),
                       row[2].value.double_value);
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
      ASSERT_DOUBLE_EQ(static_cast<double>(row[0].value.int_value * 2),
                       row[2].value.double_value);
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
      ASSERT_DOUBLE_EQ(static_cast<double>(row[0].value.int_value * 2),
                       row[2].value.double_value);
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
      ASSERT_DOUBLE_EQ(static_cast<double>(row[0].value.int_value * 2),
                       row[2].value.double_value);
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
      ASSERT_DOUBLE_EQ(static_cast<double>(row[0].value.int_value * 2),
                       row[2].value.double_value);
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
    ASSERT_DOUBLE_EQ(static_cast<double>(row[0].value.int_value * 2),
                     row[2].value.double_value);
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

TEST_F(IndexScanIteratorTest, UniqueDescendingAcrossNullKey) {
  TransactionContext setup = db_->BeginContext();
  ASSERT_SUCCESS(db_->CreateIndex(
      setup, kTableName,
      IndexSchema("NullableUnique", {1, 2}, {}, IndexMode::kUnique)));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              setup.GetTable(kTableName));
  ASSERT_SUCCESS(
      table->Insert(setup.txn_, Row({Value(0), Value("0"), Value(9.0)}))
          .GetStatus());
  ASSERT_SUCCESS(table->Insert(setup.txn_, Row({Value(1), Value("1"), Value()}))
                     .GetStatus());
  ASSERT_SUCCESS(
      table->Insert(setup.txn_, Row({Value(2), Value("1"), Value(2.0)}))
          .GetStatus());

  const Index& index = table->GetIndex(table->IndexCount() - 1);
  Iterator scan =
      table->BeginIndexScan(setup.txn_, index, Value(), Value(), false);
  ASSERT_TRUE(scan.IsValid());
  EXPECT_EQ((*scan)[0], Value(2));
  ++scan;
  ASSERT_TRUE(scan.IsValid());
  EXPECT_EQ((*scan)[0], Value(1));
  ++scan;
  ASSERT_TRUE(scan.IsValid());
  EXPECT_EQ((*scan)[0], Value(0));
  ++scan;
  EXPECT_FALSE(scan.IsValid());
  ASSERT_SUCCESS(setup.txn_.PreCommit());
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
    EXPECT_NEAR(static_cast<double>(row[0].value.int_value),
                row[2].value.double_value - 0.1, 1e-9);
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

TEST_F(IndexScanIteratorTest, ReadVersionFailureTerminatesScanSticky) {
  const std::string db_name = "index_scan_failure-" + RandomString() + ".db";
  const std::string log_name = "index_scan_failure-" + RandomString() + ".log";
  {
    auto page_manager = PageManager::Create(db_name, 10).MoveValue();
    auto logger = Logger::Create(log_name).MoveValue();
    RecoveryManager recovery(log_name, page_manager->GetPool());
    TransactionManager transactions(page_manager.get(), logger.get(),
                                    &recovery);

    Transaction pin = transactions.Begin(true);
    Transaction seed = transactions.Begin();
    PageRef allocated =
        page_manager->AllocateNewPage(seed, PageType::kRowPage).MoveValue();
    const page_id_t page_id = allocated->PageID();
    allocated.PageUnlock();
    Table table(Schema("failure_table", {Column("value", ValueType::kInt64)}),
                page_id);
    ASSERT_SUCCESS(table.Insert(seed, Row({Value(7)})).GetStatus());
    ASSERT_SUCCESS(table.CreateIndex(seed, IndexSchema("failure_idx", {0})));
    const Index& index = table.GetIndex(0);
    ASSERT_SUCCESS(seed.PreCommit());

    Transaction reader = transactions.Begin(true);
    Transaction broken = transactions.Begin();
    ASSERT_TRUE(broken.AddWriteSet(RowPosition(page_id + 1000, 0)));
    ForceIndexScanWalFailure(logger.get());
    EXPECT_NE(broken.PreCommit(), Status::kSuccess);
    ASSERT_TRUE(transactions.FailStopped());

    Iterator scan = table.BeginIndexScan(reader, index, Value(7), Value(7));
    ASSERT_TRUE(scan.IsValid());
    (void)*scan;
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kIOError);
  }
  std::ignore = std::remove(db_name.c_str());
  tinylamb::RemoveLogAndSegments(log_name);
}

// ---------------------------------------------------------------------------
// Index-only visibility map: a non-retained index entry on a page with no
// version chains is served straight from the B+Tree image (key + INCLUDE
// payload) without the heap fetch.  See IndexScanIterator::TryReadIndexOnly.
// ---------------------------------------------------------------------------

TEST_F(IndexScanIteratorTest, IndexOnlyVisibilityMapServesChainFreePages) {
  // Arrange -- commit rows, then let the background GC drop their version
  // chains: a chain-free page is the visibility-map condition.
  {
    TransactionContext seed = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                seed.GetTable(kTableName));
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                       Value(0.1 + i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(seed.txn_.PreCommit());
  }
  WaitForChainFreePages();

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  const uint64_t serves_before =
      db_->TransactionStats().index_only_visible_serves;

  // Act -- NameIdx (kNonUnique key {1} include {2}) serves key + payload.
  IndexScanIterator scan(*table, table->GetIndex(1), reader.txn_);
  std::vector<Row> from_index;
  Row row({Value("stale"), Value(99), Value(100)});
  while (scan.IsValid()) {
    ASSERT_TRUE(scan.TryReadIndexOnly(&row));
    // The heap-resolved full row agrees with the entry image.
    const Row& heap = *scan;
    EXPECT_EQ(row[0], heap[1]);
    EXPECT_EQ(row[1], heap[2]);
    ASSERT_EQ(row.Count(), 2U);
    from_index.push_back(row);
    row[0] = Value("changed owned result");
    EXPECT_EQ(scan.GetKey()[0], heap[1]);
    ++scan;
  }

  // Assert -- the expected rows arrive in key order, one serve per row.
  ASSERT_EQ(from_index.size(), 10U);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(from_index[static_cast<size_t>(i)],
              Row({Value("v" + std::to_string(i)), Value(0.1 + i)}));
  }
  EXPECT_EQ(db_->TransactionStats().index_only_visible_serves,
            serves_before + from_index.size());
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, IndexOnlyVisibilityMapRefusesRetainedEntries) {
  // kVersionedUnique retains deleted entries for old snapshots, so the heap
  // must keep deciding which entry version is visible: the fast path must
  // refuse even on a chain-free page.
  TransactionContext ddl = db_->BeginContext();
  ASSERT_SUCCESS(db_->CreateIndex(
      ddl, kTableName,
      IndexSchema("VersionedIdx", {0}, {1}, IndexMode::kVersionedUnique)));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ddl.GetTable(kTableName));
  for (int i = 0; i < 3; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ddl.txn_, Row({Value(i), Value("r" + std::to_string(i)),
                                    Value(0.5 + i)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(ddl.txn_.PreCommit());
  WaitForChainFreePages();

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, r_table,
                              reader.GetTable(kTableName));
  const uint64_t serves_before =
      db_->TransactionStats().index_only_visible_serves;
  IndexScanIterator scan(*r_table, r_table->GetIndex(3), reader.txn_);
  int seen = 0;
  while (scan.IsValid()) {
    Row row;
    EXPECT_FALSE(scan.TryReadIndexOnly(&row));
    EXPECT_TRUE((*scan).IsValid());
    ++seen;
    ++scan;
  }
  EXPECT_EQ(seen, 3);
  EXPECT_EQ(db_->TransactionStats().index_only_visible_serves, serves_before);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, IndexOnlyVisibilityMapRefusesPagesWithChains) {
  // Arrange -- seed and keep a writer's uncommitted update: the pending
  // version registers a chain on the row page, so the entry may not be
  // trusted for visibility and the heap path must decide.
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              seed.GetTable(kTableName));
  for (int i = 0; i < 3; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                     Value(0.1 + i)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(seed.txn_.PreCommit());

  TransactionContext writer = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, w_table,
                              writer.GetTable(kTableName));
  RowPosition target;
  for (Iterator it = w_table->BeginFullScan(writer.txn_); it.IsValid(); ++it) {
    if ((*it)[0] == Value(1)) {
      target = it.Position();
    }
  }
  ASSERT_TRUE(target.IsValid());
  ASSERT_SUCCESS(w_table
                     ->Update(writer.txn_, target,
                              Row({Value(1), Value("pending"), Value(9.9)}))
                     .GetStatus());

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, r_table,
                              reader.GetTable(kTableName));
  const uint64_t serves_before =
      db_->TransactionStats().index_only_visible_serves;
  Row observed;
  {
    // The scan must be destroyed before the writer aborts: its shared index
    // read guard would block the abort's exclusive index-gate acquisition
    // (ClearPendingIndexMutations) on this same thread.
    IndexScanIterator scan(*r_table, r_table->GetIndex(0), reader.txn_,
                           Value(1), Value(1));
    ASSERT_TRUE(scan.IsValid());

    // Act / Assert -- the chain blocks the fast path; the heap serves the
    // committed pre-update image to this snapshot.
    Row row;
    EXPECT_FALSE(scan.TryReadIndexOnly(&row));
    observed = *scan;
  }
  EXPECT_EQ(observed[1], Value("v1"));
  EXPECT_EQ(observed[2], Value(0.1 + 1));
  EXPECT_EQ(db_->TransactionStats().index_only_visible_serves, serves_before);
  ASSERT_SUCCESS(writer.txn_.Abort());
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, IndexOnlyVisibilityMapServesUpdatedInclude) {
  // Arrange -- seed rows and rewrite one row's INCLUDE column with the same
  // key; the committed image must be reflected by the entry's payload once
  // the version chain is gone.
  {
    TransactionContext seed = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                seed.GetTable(kTableName));
    for (int i = 0; i < 5; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                       Value(0.1 + i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(seed.txn_.PreCommit());
  }
  {
    TransactionContext writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    RowPosition target;
    {
      IndexScanIterator locate(*table, table->GetIndex(1), writer.txn_,
                               Value("v2"), Value("v2"));
      ASSERT_TRUE(locate.IsValid());
      target = locate.Position();
    }
    ASSERT_SUCCESS(table
                       ->Update(writer.txn_, target,
                                Row({Value(2), Value("v2"), Value(99.5)}))
                       .GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  WaitForChainFreePages();

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  IndexScanIterator scan(*table, table->GetIndex(1), reader.txn_, Value("v2"),
                         Value("v2"));
  ASSERT_TRUE(scan.IsValid());
  Row row;
  ASSERT_TRUE(scan.TryReadIndexOnly(&row));
  EXPECT_EQ(row, Row({Value("v2"), Value(99.5)}));
  ++scan;
  EXPECT_FALSE(scan.IsValid());
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, IndexOnlyVisibilityMapDropsMovedKeyEntries) {
  // A non-retained index physically removes the old key's entry on a
  // key-changing update, so the visibility map never resurrects it.
  {
    TransactionContext seed = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                seed.GetTable(kTableName));
    for (int i = 0; i < 3; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                       Value(0.1 + i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(seed.txn_.PreCommit());
  }
  {
    TransactionContext writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    RowPosition target;
    {
      IndexScanIterator locate(*table, table->GetIndex(1), writer.txn_,
                               Value("v1"), Value("v1"));
      ASSERT_TRUE(locate.IsValid());
      target = locate.Position();
    }
    ASSERT_SUCCESS(table
                       ->Update(writer.txn_, target,
                                Row({Value(1), Value("v1x"), Value(1.1)}))
                       .GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  WaitForChainFreePages();

  TransactionContext reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  {
    IndexScanIterator gone(*table, table->GetIndex(1), reader.txn_, Value("v1"),
                           Value("v1"));
    EXPECT_FALSE(gone.IsValid());
  }
  IndexScanIterator moved(*table, table->GetIndex(1), reader.txn_, Value("v1x"),
                          Value("v1x"));
  ASSERT_TRUE(moved.IsValid());
  Row row;
  ASSERT_TRUE(moved.TryReadIndexOnly(&row));
  EXPECT_EQ(row, Row({Value("v1x"), Value(1.1)}));
  ++moved;
  EXPECT_FALSE(moved.IsValid());
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(IndexScanIteratorTest, PartitionBoundariesTileTheLeafChain) {
  // RangePartitionedScan: boundaries are the first keys of every leaf group
  // strictly greater than the scan's begin and not past its end, strictly
  // ascending, so half-open morsels built from them tile the range.
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              ctx.GetTable(kTableName));
  for (int i = 0; i < 4000; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  const Index& pk = table->GetIndex(0);
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, boundaries,
                        IndexScanIterator::PartitionBoundaries(
                            pk, ctx.txn_, {Value(10)}, {Value(3990)}, 1));
  ASSERT_GE(boundaries.size(), 2U);
  for (size_t i = 0; i < boundaries.size(); ++i) {
    EXPECT_GT(boundaries[i][0], Value(10));
    EXPECT_LE(boundaries[i][0], Value(3990));
    if (i > 0) {
      EXPECT_LT(boundaries[i - 1][0], boundaries[i][0]);
    }
  }
  // A single group covers the whole range when the leaf count fits inside it.
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, one_group,
                        IndexScanIterator::PartitionBoundaries(
                            pk, ctx.txn_, {Value(10)}, {Value(3990)}, 1000));
  EXPECT_TRUE(one_group.empty());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

}  // namespace tinylamb

namespace tinylamb {
TEST_F(IndexScanIteratorTest, SnapshotDecodesDuplicateListsOncePerKey) {
  {
    auto writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    for (int i = 0; i < 600; ++i) {
      const Value name =
          i % 5 == 0 ? Value() : Value("g" + std::to_string(i % 5));
      ASSERT_SUCCESS(
          table->Insert(writer.txn_, Row{Value(i), name, Value(i + .5)})
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  auto reader = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  for (const bool ascending : {true, false}) {
    IndexScanIterator scan(*table, table->GetIndex(1), reader.txn_, Value(),
                           Value(), ascending);
    for (int group = 0; group < 5; ++group) {
      const int key = ascending ? group : 4 - group;
      for (int offset = 0; offset < 120; ++offset) {
        const int id = key + 5 * (ascending ? offset : 119 - offset);
        ASSERT_TRUE(scan.IsValid());
        EXPECT_EQ((*scan)[0], Value(id));
        EXPECT_EQ(scan.Include()[0], Value(id + .5));
        if (ascending || offset % 2 == 0) {
          ++scan;
        } else {
          --scan;
        }
      }
    }
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
    EXPECT_EQ(scan.ValueListsDecoded(), 5);
    EXPECT_EQ(scan.ValueEntriesDecoded(), 600);
  }
}
TEST_F(IndexScanIteratorTest,
       SnapshotLeafCacheKeepsBoundsAcrossLeafTransitions) {
  {
    auto writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    for (int64_t i = 0; i < 4000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(writer.txn_,
                       Row({Value(i), Value(std::to_string(i)), Value(i + .5)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto reader = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  for (const bool ascending : {true, false}) {
    const auto before = db_->CacheHits();
    IndexScanIterator scan(*table, table->GetIndex(0), reader.txn_, Value(17),
                           Value(3988), ascending);
    for (int64_t i = 0; i < 3972; ++i) {
      ASSERT_TRUE(scan.IsValid());
      EXPECT_EQ(scan.GetKey()[0], Value(ascending ? 17 + i : 3988 - i));
      EXPECT_TRUE(scan.Position().IsValid());
      ++scan;
    }
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
    EXPECT_LT(db_->CacheHits() - before, 1986U);
  }
}
TEST_F(IndexScanIteratorTest, SnapshotInvalidKeyReportsStickyError) {
  {
    auto writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    ASSERT_SUCCESS(
        table->Insert(writer.txn_, Row({Value(1), Value("one"), Value(1.5)}))
            .GetStatus());
    BPlusTree tree(table->GetIndex(0).Root());
    ASSERT_SUCCESS(tree.Insert(writer.txn_, std::string(1, '\xff'), "invalid"));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto reader = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  IndexScanIterator scan(*table, table->GetIndex(0), reader.txn_);
  ASSERT_TRUE(scan.IsValid());
  EXPECT_EQ(scan.GetKey()[0], Value(1));
  ++scan;
  EXPECT_FALSE(scan.IsValid());
  EXPECT_NE(scan.GetStatus(), Status::kSuccess);
  const Status first = scan.GetStatus();
  ++scan;
  EXPECT_EQ(scan.GetStatus(), first);
}
}  // namespace tinylamb

namespace tinylamb {
TEST_F(IndexScanIteratorTest, ReadOnlyHeapImagesKeepBoundsHolesAndDirections) {
  {
    auto writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    for (int i = 0; i < 600; ++i) {
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, position,
          table->Insert(
              writer.txn_,
              Row({Value(i), Value(std::string(128, 'x')), Value(i + .5)})));
      if (i % 11 == 0) {
        ASSERT_SUCCESS(table->Delete(writer.txn_, position));
      }
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  for (bool ascending : {true, false}) {
    IndexScanIterator scan(*table, table->GetIndex(0), reader.txn_, Value(17),
                           Value(588), ascending);
    for (int step = 0; step <= 571; ++step) {
      const int key = ascending ? 17 + step : 588 - step;
      if (key % 11 == 0) continue;
      ASSERT_TRUE(scan.IsValid());
      EXPECT_EQ((*scan), Row({Value(key), Value(std::string(128, 'x')),
                              Value(key + .5)}));
      ++scan;
    }
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
    EXPECT_GT(scan.HeapPageImagesCopied(), 0);
    EXPECT_LT(scan.HeapPageImagesCopied(), scan.RowsDecoded() / 10);
  }
}

TEST_F(IndexScanIteratorTest, HeapImagesNeverHideOlderSnapshotOrOwnUpdates) {
  std::vector<RowPosition> positions;
  {
    auto writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    for (int i = 0; i < 8; ++i) {
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, position,
          table->Insert(writer.txn_,
                        Row({Value(i), Value("old"), Value(i + .5)})));
      positions.push_back(position);
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto old = db_->BeginReadOnlyContext();
  {
    auto writer = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                writer.GetTable(kTableName));
    ASSERT_SUCCESS(table
                       ->Update(writer.txn_, positions[4],
                                Row({Value(4), Value("new"), Value(99.5)}))
                       .GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              old.GetTable(kTableName));
  {
    IndexScanIterator scan(*table, table->GetIndex(0), old.txn_, Value(0),
                           Value(7));
    for (int i = 0; i < 8; ++i) {
      ASSERT_TRUE(scan.IsValid());
      EXPECT_EQ((*scan)[1], Value("old"));
      EXPECT_EQ((*scan)[2], Value(i + .5));
      ++scan;
    }
    EXPECT_EQ(scan.HeapPageImagesCopied(), 0);
  }
  ASSERT_SUCCESS(old.PreCommit());
  WaitForChainFreePages();
  auto writer = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, writable,
                              writer.GetTable(kTableName));
  ASSERT_SUCCESS(writable
                     ->Update(writer.txn_, positions[6],
                              Row({Value(6), Value("own"), Value(100.5)}))
                     .GetStatus());
  IndexScanIterator scan(*writable, writable->GetIndex(0), writer.txn_,
                         Value(0), Value(7));
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(scan.IsValid());
    EXPECT_EQ((*scan)[1], Value(i == 4 ? "new" : i == 6 ? "own" : "old"));
    ++scan;
  }
  EXPECT_EQ(scan.HeapPageImagesCopied(), 0);
}
}  // namespace tinylamb

namespace tinylamb {
TEST_F(IndexScanIteratorTest, CompactLeafImagesKeepBinaryIncludesAcrossLeaves) {
  auto payload = [](int key) {
    std::string value = std::to_string(key) + std::string(120, 'x');
    value[15] = '\0';
    return value;
  };
  {
    auto seed = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                                seed.GetTable(kTableName));
    for (int i = 0; i < 800; ++i) {
      ASSERT_SUCCESS(table
                         ->Insert(seed.txn_, Row({Value(i), Value(payload(i)),
                                                  Value(i + .5)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(seed.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, table,
                              reader.GetTable(kTableName));
  for (const bool ascending : {true, false}) {
    IndexScanIterator scan(*table, table->GetIndex(2), reader.txn_,
                           std::vector<Value>{Value(17)},
                           std::vector<Value>{Value(782)}, ascending);
    for (int step = 0; step < 766; ++step) {
      int key = ascending ? 17 + step : 782 - step;
      ASSERT_TRUE(scan.IsValid());
      EXPECT_EQ(scan.GetKey(), Row({Value(key), Value(key + .5)}));
      EXPECT_EQ(scan.Include(), Row({Value(payload(key))}));
      ++scan;
    }
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  }
}
}  // namespace tinylamb

namespace tinylamb {
TEST_F(IndexScanIteratorTest, CompactPositionDecoderRejectsTruncatedPayloads) {
  const size_t position_bytes = RowPosition::Size();
  for (const bool nonunique : {false, true}) {
    for (const size_t cut : {size_t{1}, position_bytes, position_bytes + 7}) {
      const std::string table_name =
          "broken_" + std::to_string(nonunique) + "_" + std::to_string(cut);
      auto writer = db_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(
          Table, table,
          db_->CreateTable(
              writer, Schema(table_name, {Column("id", ValueType::kInt64)})));
      ASSIGN_OR_ASSERT_FAIL(RowPosition, position,
                            table.Insert(writer.txn_, Row({Value(7)})));
      ASSERT_SUCCESS(db_->CreateIndex(
          writer, table_name,
          IndexSchema("broken_idx", {0}, {},
                      nonunique ? IndexMode::kNonUnique : IndexMode::kUnique)));
      ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, indexed,
                                  writer.GetTable(table_name));
      BPlusTree tree(indexed->GetIndex(0).Root());
      std::string encoded;
      if (nonunique) {
        encoded.resize(sizeof(uint64_t));
        SerializeU64(encoded.data(), 1);
      }
      encoded += position.Serialize();
      const size_t prefix = encoded.size();
      encoded.resize(prefix + sizeof(uint64_t));
      SerializeU64(encoded.data() + prefix, 0);
      encoded.resize((nonunique ? sizeof(uint64_t) : 0) + cut);
      ASSERT_SUCCESS(tree.Update(
          writer.txn_, Value(7).EncodeMemcomparableFormat(), encoded));
      ASSERT_SUCCESS(writer.PreCommit());
      auto reader = db_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, current,
                                  reader.GetTable(table_name));
      IndexScanIterator scan(*current, current->GetIndex(0), reader.txn_,
                             Value(7), Value(7));
      EXPECT_FALSE(scan.IsValid());
      EXPECT_EQ(scan.GetStatus(), Status::kCorrupt);
      ++scan;
      EXPECT_EQ(scan.GetStatus(), Status::kCorrupt);
    }
  }
}
}  // namespace tinylamb
