//
// Created by kumagi on 2022/04/21.
//

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
    prefix_ = "table_concurrent_test-" + RandomString();
    Recover();
    Schema sc("SampleTable", {Column("col1", ValueType::kInt64,
                                     Constraint(Constraint::kIndex)),
                              Column("col2", ValueType::kVarChar),
                              Column("col3", ValueType::kDouble)});
    auto txn = db_->Begin();
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
    std::remove(db_->Storage().DBName().c_str());
    std::remove(db_->Storage().LogName().c_str());
    std::remove(db_->Storage().MasterRecordName().c_str());
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};
constexpr int kThreads = 2;

TEST_F(TableConcurrentTest, InsertInsert) {
  constexpr int kSize = 2000;
  auto ro_txn = db_->Begin();
  Table table;
  ASSERT_SUCCESS(db_->GetTable(ro_txn, "SampleTable", &table));
  ASSERT_SUCCESS(ro_txn.PreCommit());
  std::vector<std::unordered_map<RowPosition, Row>> rows;
  rows.resize(kThreads);
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i]() {
      auto txn = db_->Begin();
      for (int j = 0; j < kSize; ++j) {
        Row new_row({Value(j * kSize + i), Value(RandomString(32)),
                     Value((double)2 * j * kSize + i)});
        RowPosition inserted_pos;
        ASSERT_SUCCESS(table.Insert(txn, new_row, &inserted_pos));
        rows[i].emplace(inserted_pos, new_row);
      }
      ASSERT_SUCCESS(txn.PreCommit());
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  {
    auto txn = db_->Begin();
    for (const auto& rows_per_thread : rows) {
      for (const auto& row : rows_per_thread) {
        Row read_row;
        ASSERT_SUCCESS(table.Read(txn, row.first, &read_row));
        ASSERT_EQ(read_row, row.second);
      }
    }
  }
}

}  // namespace tinylamb
