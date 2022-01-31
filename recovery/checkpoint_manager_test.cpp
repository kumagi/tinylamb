//
// Created by kumagi on 2022/01/09.
//
#include "recovery/checkpoint_manager.hpp"

#include "gtest/gtest.h"
#include "page/row_page_test.hpp"

namespace tinylamb {

class CheckpointTest : public RowPageTest {
 protected:
  static constexpr char kDBFileName[] = "checkpoint_test.db";
  static constexpr char kLogName[] = "checkpoint_test.log";
  static constexpr char kMasterRecordName[] = "checkpoint_test.master_record";

  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageID();
    EXPECT_TRUE(txn.PreCommit());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
    std::remove(kMasterRecordName);
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
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), nullptr);
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
                                              p_->GetPool(), 1);
  }

  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<CheckpointManager> cm_;
};

TEST_F(CheckpointTest, Construct) {}

TEST_F(CheckpointTest, DoCheckpoint) {
  InsertRow("expect this operation did not rerun");
  Transaction txn = tm_->Begin();
  slot_t slot;
  {
    PageRef page = p_->GetPage(page_id_);
    page->Insert(txn, "inserted", &slot);
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
  slot_t inserted;
  lsn_t restart_point;
  {
    PageRef page = p_->GetPage(page_id_);
    page->Insert(txn, "inserted", &inserted);
    restart_point = cm_->WriteCheckpoint();
    page->Update(txn, inserted, "expect to be redone");
    txn.PreCommit();
  }
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());
  EXPECT_EQ(ReadRow(inserted), "expect to be redone");
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
    RowPosition insert_position;
    slot_t will_be_deleted_row;
    page->Insert(txn, "will be deleted", &will_be_deleted_row);
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
      slot_t will_be_deleted_row;
      page->Insert(txn, "will be deleted", &will_be_deleted_row);
    });
  }
  // Note that the txn is not committed.
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(slot), "original message");
}

}  // namespace tinylamb