//
// Created by kumagi on 2022/02/09.
//
#include "full_scan_iterator.hpp"

#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/page_storage.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

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

TEST_F(FullScanIteratorTest, Construct) {}

TEST_F(FullScanIteratorTest, Scan) {
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

}  // namespace tinylamb
