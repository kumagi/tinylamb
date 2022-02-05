//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include <memory>

#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TableTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "table_test.db";
  static constexpr char kLogName[] = "table_test.log";
  static constexpr char kMasterRecordName[] = "table_master.log";

 public:
  void SetUp() override {
    schema_ = Schema("SampleTable", {Column("col1", ValueType::kInt64,
                                            Constraint(Constraint::kPrimary)),
                                     Column("col2", ValueType::kVarChar),
                                     Column("col3", ValueType::kDouble)});
    Recover();
    Transaction txn = tm_->Begin();
    PageRef row_page = p_->AllocateNewPage(txn, PageType::kRowPage);
    std::vector<Index> ind;
    table_ =
        std::make_unique<Table>(p_.get(), schema_, row_page->PageID(), ind);
  }

  void Flush(page_id_t pid) { p_->GetPool()->FlushPageForTest(pid); }

  void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    table_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
                                              p_->GetPool(), 1);
  }

  void TearDown() override {
    table_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

 public:
  Schema schema_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<Table> table_;
};

TEST_F(TableTest, Construct) {}

TEST_F(TableTest, Insert) {
  Transaction txn = tm_->Begin();
  Row r({Value(1), Value("23"), Value(3.3)});
  RowPosition rp;
  ASSERT_SUCCESS(table_->Insert(txn, r, &rp));
}

TEST_F(TableTest, Read) {
  Transaction txn = tm_->Begin();
  Row r({Value(1), Value("string"), Value(3.3)});
  RowPosition rp;
  ASSERT_SUCCESS(table_->Insert(txn, r, &rp));
  Row read;
  ASSERT_SUCCESS(table_->Read(txn, rp, &read));
  ASSERT_EQ(read, r);
  LOG(INFO) << read;
}

TEST_F(TableTest, Update) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  Row new_row({Value(1), Value("hogefuga"), Value(99e8)});
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &rp));
  ASSERT_SUCCESS(table_->Update(txn, rp, new_row));
  Row read;
  ASSERT_SUCCESS(table_->Read(txn, rp, &read));
  ASSERT_EQ(read, new_row);
  LOG(INFO) << read;
}

TEST_F(TableTest, Delete) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &rp));
  ASSERT_SUCCESS(table_->Delete(txn, rp));
  Row read;
  ASSERT_FAIL(table_->Read(txn, rp, &read));
}

}  // namespace tinylamb