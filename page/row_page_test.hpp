#ifndef TINYLAMB_ROW_PAGE_TEST_HPP
#define TINYLAMB_ROW_PAGE_TEST_HPP

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

class RowPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "row_page_test.db";
  static constexpr char kLogName[] = "row_page_test.log";

  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageId();
    EXPECT_TRUE(txn.PreCommit());
  }

  virtual void Recover() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), nullptr);
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  bool InsertRow(std::string_view str, bool commit = true) {
    auto txn = tm_->Begin();
    Row r;
    r.data = str;
    PageRef page = p_->GetPage(page_id_);
    const RowPage& rp = page.GetRowPage();
    EXPECT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kRowPage);

    const uint16_t before_size = rp.FreeSizeForTest();
    RowPosition pos;
    bool success = page->Insert(txn, r, pos);
    if (success) {
      EXPECT_EQ(rp.FreeSizeForTest(),
                before_size - r.data.size() - sizeof(RowPage::RowPointer));
    }
    if (commit) {
      EXPECT_TRUE(txn.PreCommit());
    } else {
      page.PageUnlock();
      txn.Abort();
    }
    txn.CommitWait();
    return success;
  }

  void UpdateRow(int slot, std::string_view str, bool commit = true) {
    auto txn = tm_->Begin();
    Row r;
    r.data = str;
    PageRef page = p_->GetPage(page_id_);
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, slot);
    ASSERT_TRUE(page->Update(txn, pos, r));
    if (commit) {
      ASSERT_TRUE(txn.PreCommit());
    } else {
      page.PageUnlock();
      txn.Abort();
    }
    txn.CommitWait();
  }

  void DeleteRow(int slot, bool commit = true) {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, slot);
    ASSERT_TRUE(page->Delete(txn, pos));
    if (commit) {
      ASSERT_TRUE(txn.PreCommit());
    } else {
      page.PageUnlock();
      txn.Abort();
    }
    txn.CommitWait();
  }

  std::string ReadRow(int slot) {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    EXPECT_FALSE(page.IsNull());
    Row dst;
    RowPosition pos(page_id_, slot);
    EXPECT_TRUE(page->Read(txn, pos, dst));
    dst.MakeOwned();
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return dst.owned_data;
  }

  size_t GetRowCount() {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    EXPECT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kRowPage);
    size_t row_count = page->RowCount();
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return row_count;
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t page_id_ = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_PAGE_TEST_HPP