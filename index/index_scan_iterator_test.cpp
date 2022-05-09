//
// Created by kumagi on 2022/02/21.
//

#include "index_scan_iterator.hpp"

#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "index.hpp"
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

class IndexScanIteratorTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "index_scan_iterator_test-" + RandomString();
    Recover();
    Schema sc("SampleTable", {Column("col1", ValueType::kInt64,
                                     Constraint(Constraint::kIndex)),
                              Column("col2", ValueType::kVarChar),
                              Column("col3", ValueType::kDouble)});
    Transaction txn = db_->Begin();
    IndexSchema is("idx", {0});
    ASSERT_SUCCESS(db_->CreateTable(txn, sc));
    ASSERT_SUCCESS(db_->CreateIndex(txn, "SampleTable", is));
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

TEST_F(IndexScanIteratorTest, Construct) {}

TEST_F(IndexScanIteratorTest, ScanAscending) {
  Transaction txn = db_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(txn, "SampleTable"));
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(txn, Row({Value(i), Value("v" + std::to_string(i)),
                              Value(0.1 + i)}))
            .GetStatus());
  }
  Row iter_begin({Value(43)});
  Row iter_end({Value(180)});
  Iterator it = table.BeginIndexScan(txn, "bt", iter_begin, iter_end);
  ASSERT_TRUE(it.IsValid());
  for (int i = 43; i <= 180; ++i) {
    Row cur = *it;
    ASSERT_EQ(cur[0], Value(i));
    ASSERT_EQ(cur[1], Value("v" + std::to_string(i)));
    ASSERT_EQ(cur[2], Value(0.1 + i));
    ++it;
  }
  ASSERT_FALSE(it.IsValid());
}

TEST_F(IndexScanIteratorTest, ScanDecending) {
  Transaction txn = db_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(txn, "SampleTable"));
  RowPosition rp;
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(txn, Row({Value(i), Value("v" + std::to_string(i)),
                              Value(0.1 + i)}))
            .GetStatus());
  }
  Row iter_begin({Value(104)});
  Row iter_end({Value(200)});
  Iterator it = table.BeginIndexScan(txn, "bt", iter_begin, iter_end, false);
  ASSERT_TRUE(it.IsValid());
  for (int i = 200; i >= 104; --i) {
    Row cur = *it;
    ASSERT_EQ(cur[0], Value(i));
    ASSERT_EQ(cur[1], Value("v" + std::to_string(i)));
    ASSERT_EQ(cur[2], Value(0.1 + i));
    --it;
  }
  ASSERT_FALSE(it.IsValid());
}

}  // namespace tinylamb
