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

#include <memory>
#include <thread>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TableConcurrentTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::string current_test =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    prefix_ = "table_concurrent_test-" + current_test + RandomString();
    Recover();
    Schema sc("SampleTable", {Column("col1", ValueType::kInt64,
                                     Constraint(Constraint::kIndex)),
                              Column("col2", ValueType::kVarChar),
                              Column("col3", ValueType::kDouble)});
    TransactionContext txn = db_->BeginContext();
    db_->CreateTable(txn, sc);
    ASSERT_SUCCESS(txn.PreCommit());
  }

  void Recover() {
    if (db_) {
      db_->Storage().LostAllPageForTest();
    }
    db_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override {
    std::ignore = std::remove(db_->Storage().DBName().c_str());
    std::ignore = std::remove(db_->Storage().LogName().c_str());
    std::ignore = std::remove(db_->Storage().MasterRecordName().c_str());
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};
constexpr int kThreads = 5;

TEST_F(TableConcurrentTest, InsertInsert) {
  constexpr int kSize = 5000;
  TransactionContext ro_ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ro_ctx, "SampleTable"));
  ASSERT_SUCCESS(ro_ctx.PreCommit());
  std::vector<std::unordered_map<RowPosition, Row>> rows;
  rows.resize(kThreads);
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i]() {
      TransactionContext ctx = db_->BeginContext();
      for (int j = 0; j < kSize; ++j) {
        Row new_row({Value(j * kSize + i), Value(RandomString(32)),
                     Value((double)2 * j * kSize + i)});
        ASSIGN_OR_ASSERT_FAIL(RowPosition, inserted_pos,
                              table.Insert(ctx.txn_, new_row));
        rows[i].emplace(inserted_pos, new_row);
      }
      ASSERT_SUCCESS(ctx.txn_.PreCommit());
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  {
    TransactionContext ctx = db_->BeginContext();
    for (const auto& rows_per_thread : rows) {
      for (const auto& row : rows_per_thread) {
        ASSIGN_OR_ASSERT_FAIL(Row, read_row, table.Read(ctx.txn_, row.first));
        ASSERT_EQ(read_row, row.second);
      }
    }
  }
}

}  // namespace tinylamb
