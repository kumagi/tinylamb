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

#include "full_scan_iterator.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "iterator.hpp"
#include "page/page_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class FullScanIteratorTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "full_scan_iterator_test-" + RandomString();
    Recover();
    Schema sc("SampleTable", {Column("col1", ValueType::kInt64,
                                     Constraint(Constraint::kIndex)),
                              Column("col2", ValueType::kVarChar),
                              Column("col3", ValueType::kDouble)});
    TransactionContext ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->CreateTable(ctx, sc).GetStatus());
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

TEST_F(FullScanIteratorTest, Construct_Default_Succeeds) {}

TEST_F(FullScanIteratorTest, BeginFullScan_AllRows_ScansSuccessfully) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));

  for (int i = 0; i < 130; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                   Value(0.1 + i)}))
            .GetStatus());
  }
  Iterator it = table.BeginFullScan(ctx.txn_);
  while (it.IsValid()) {
    LOG(TRACE) << *it;
    ++it;
  }
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_WithProjection_ReturnsRequestedColumnsInOrder) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(ctx.txn_, Row({Value(42), Value("not copied"), Value(3.5)}))
          .GetStatus());

  Iterator projected = table.BeginFullScan(ctx.txn_, {0, 2});
  ASSERT_TRUE(projected.IsValid());
  ASSERT_EQ((*projected).values_.size(), 2);
  EXPECT_EQ((*projected)[0], Value(42));
  EXPECT_EQ((*projected)[1], Value(3.5));

  Iterator no_columns = table.BeginFullScan(ctx.txn_, std::vector<slot_t>{});
  ASSERT_TRUE(no_columns.IsValid());
  EXPECT_TRUE((*no_columns).values_.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_AfterDeletingFirstSlot_ScansRemainingRows) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  RowPosition first;
  for (int i = 0; i < 6; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, position,
        table.Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)})));
    if (i == 0) {
      first = position;
    }
  }
  ASSERT_SUCCESS(table.Delete(ctx.txn_, first));

  int count = 0;
  int64_t maximum = 0;
  for (Iterator it = table.BeginFullScan(ctx.txn_); it.IsValid(); ++it) {
    ++count;
    maximum = std::max(maximum, (*it)[0].value.int_value);
  }

  EXPECT_EQ(count, 5);
  EXPECT_EQ(maximum, 5);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_OldSnapshotAfterDelete_ScansPhysicallyDeletedTailRow) {
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

  TransactionContext old_reader = db_->BeginContext();
  TransactionContext writer = db_->BeginContext();
  ASSERT_SUCCESS(table.Delete(writer.txn_, tail));
  ASSERT_SUCCESS(writer.PreCommit());

  size_t old_count = 0;
  for (Iterator it = table.BeginFullScan(old_reader.txn_); it.IsValid(); ++it) {
    ++old_count;
  }
  EXPECT_EQ(old_count, 2);
  ASSERT_SUCCESS(old_reader.PreCommit());

  TransactionContext fresh_reader = db_->BeginContext();
  size_t fresh_count = 0;
  for (Iterator it = table.BeginFullScan(fresh_reader.txn_); it.IsValid();
       ++it) {
    ++fresh_count;
  }
  EXPECT_EQ(fresh_count, 1);
  ASSERT_SUCCESS(fresh_reader.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginMorselScan_WithMorsels_IteratesOnlyRequestedPages) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  for (int i = 0; i < 130; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                   Value(0.1 + i)}))
            .GetStatus());
  }
  const std::vector<Table::ScanMorsel> morsels =
      table.BuildScanMorsels(ctx.txn_, 2);
  ASSERT_FALSE(morsels.empty());

  int64_t seen = 0;
  for (const Table::ScanMorsel& morsel : morsels) {
    Iterator it = table.BeginMorselScan(ctx.txn_, morsel);
    while (it.IsValid()) {
      ASSERT_EQ((*it).values_.size(), 3U);
      ++seen;
      ++it;
    }
  }
  EXPECT_EQ(seen, 130);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, BeginFullScan_EmptyTable_YieldsNoRows) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(Table, table, db_->GetTable(ctx, "SampleTable"));
  Iterator it = table.BeginFullScan(ctx.txn_);

  EXPECT_FALSE(it.IsValid());
  EXPECT_FALSE(it.Position().IsValid());
  ++it;
  EXPECT_FALSE(it.IsValid());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, OutputOperator_WhenCalled_PrintsScanName) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(ctx.txn_, Row({Value(7), Value("seven"), Value(0.7)}))
          .GetStatus());
  Iterator it = table.BeginFullScan(ctx.txn_);
  ASSERT_TRUE(it.IsValid());

  std::ostringstream output;
  output << it;

  EXPECT_NE(output.str().find("FullScan"), std::string::npos);
  EXPECT_NE(output.str().find("SampleTable"), std::string::npos);
  ASSERT_SUCCESS(ctx.PreCommit());
}

}  // namespace tinylamb
