//
// Created by kumagi on 2022/01/09.
//
#include "recovery/checkpoint_manager.hpp"

#include "gtest/gtest.h"
#include "page/row_page_test.hpp"

namespace tinylamb {

class CheckpointTest : public RowPageTest {
 protected:
  void SetUp() override {
    std::string prefix = "checkpoint_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    master_record_name_ = prefix + ".master.log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void TearDown() override {
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
    std::remove(master_record_name_.c_str());
  }

  void Recover() override {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    cm_.reset();
    tm_.reset();
    lm_.reset();
    l_.reset();
    r_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               nullptr);
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
  }
  std::string db_name_;
  std::string log_name_;
  std::string master_record_name_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<CheckpointManager> cm_;
};

TEST_F(CheckpointTest, Construct) {}

TEST_F(CheckpointTest, DoCheckpoint) {
  InsertRow("expect this operation did not rerun");
  Transaction txn = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, "inserted"));
    p_->GetPool()->FlushPageForTest(page_id_);
    cm_->WriteCheckpoint();
    page->Update(txn, slot, "expect to be redone");
    txn.PreCommit();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());
}

TEST_F(CheckpointTest, CheckpointRecovery) {
  InsertRow("expect this operation did not rerun");
  Transaction txn = tm_->Begin();
  lsn_t restart_point;
  slot_t result;
  {
    PageRef page = p_->GetPage(page_id_);
    ASSIGN_OR_ASSERT_FAIL(slot_t, inserted, page->Insert(txn, "inserted"));
    restart_point = cm_->WriteCheckpoint();
    page->Update(txn, inserted, "expect to be redone");
    txn.PreCommit();
    result = inserted;
  }
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());
  EXPECT_EQ(ReadRow(result), "expect to be redone");
}

TEST_F(CheckpointTest, CheckpointAbortRecovery) {
  ASSERT_TRUE(InsertRow("original message"));
  Transaction txn = tm_->Begin();
  slot_t slot = 0;
  lsn_t restart_point;
  {
    PageRef page = p_->GetPage(page_id_);
    restart_point = cm_->WriteCheckpoint();
    page->Update(txn, slot, "aborted");
    ASSIGN_OR_ASSERT_FAIL(slot_t, will_be_deleted_row,
                          page->Insert(txn, "will be deleted"));
  }
  // Note that the txn is not committed.
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(slot), "original message");
}

TEST_F(CheckpointTest, CheckpointUpdateAfterBeginCheckpoint) {
  ASSERT_TRUE(InsertRow("original message"));
  Transaction txn = tm_->Begin();
  slot_t slot = 0;
  lsn_t restart_point;
  {
    PageRef page = p_->GetPage(page_id_);
    restart_point = cm_->WriteCheckpoint([&]() {
      page->Update(txn, slot, "aborted");
      ASSIGN_OR_ASSERT_FAIL(slot_t, will_be_deleted_row,
                            page->Insert(txn, "will be deleted"));
    });
  }
  // Note that the txn is not committed.
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(slot), "original message");
}

}  // namespace tinylamb