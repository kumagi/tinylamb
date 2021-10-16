#ifndef TINYLAMB_ROW_PAGE_TEST_HPP
#define TINYLAMB_ROW_PAGE_TEST_HPP

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class RowPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "row_page_test.db";
  static constexpr char kLogName[] = "row_page_test.log";

  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    auto* page = reinterpret_cast<RowPage*>(
        p_->AllocateNewPage(txn, PageType::kRowPage));
    page_id_ = page->PageId();
    p_->Unpin(page_id_);
  }

  virtual void Recover() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    c_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    c_ = std::make_unique<Catalog>(p_.get());
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  void WaitForCommit(uint64_t target_lsn, size_t timeout_ms = 1000) {
    size_t counter = 0;
    while (l_->CommittedLSN() != target_lsn && counter < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ++counter;
    }
    EXPECT_LT(counter, timeout_ms);
  }

  bool InsertRow(std::string_view str) {
    auto txn = tm_->Begin();
    Row r;
    r.data = str;
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kRowPage);

    const uint16_t before_size = p->FreeSizeForTest();
    RowPosition pos;
    bool success = p->Insert(txn, r, pos);
    if (success) {
      EXPECT_EQ(p->FreeSizeForTest(),
                before_size - r.data.size() - sizeof(RowPage::RowPointer));
    }
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return success;
  }

  void UpdateRow(int slot, std::string_view str) {
    auto txn = tm_->Begin();
    Row r;
    r.data = str;
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    ASSERT_NE(p, nullptr);
    ASSERT_EQ(p->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, slot);
    ASSERT_TRUE(p->Update(txn, pos, r));
    p_->Unpin(p->PageId());
    ASSERT_TRUE(txn.PreCommit());
    txn.CommitWait();
  }

  void DeleteRow(int slot) {
    auto txn = tm_->Begin();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    ASSERT_NE(p, nullptr);
    ASSERT_EQ(p->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, slot);
    ASSERT_TRUE(p->Delete(txn, pos));
    p_->Unpin(p->PageId());
    ASSERT_TRUE(txn.PreCommit());
    txn.CommitWait();
  }

  std::string ReadRow(int slot) {
    auto txn = tm_->Begin();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kRowPage);
    Row dst;
    RowPosition pos(page_id_, slot);
    EXPECT_TRUE(p->Read(txn, pos, dst));
    dst.MakeOwned();
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return dst.owned_data;
  }

  size_t GetRowCount() {
    auto txn = tm_->Begin();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kRowPage);
    size_t row_count = p->RowCount();
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return row_count;
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Catalog> c_;
  int page_id_ = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_PAGE_TEST_HPP
