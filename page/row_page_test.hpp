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

#ifndef TINYLAMB_ROW_PAGE_TEST_HPP
#define TINYLAMB_ROW_PAGE_TEST_HPP

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

class RowPageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    file_name_ = "row_page_test-" + RandomString();
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(file_name_ + ".db", 10);
    l_ = std::make_unique<Logger>(file_name_ + ".log");
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(file_name_ + ".log", p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
  }

  void TearDown() override {
    std::remove((file_name_ + ".db").c_str());
    std::remove((file_name_ + ".log").c_str());
  }

  void Flush() { p_->GetPool()->FlushPageForTest(page_id_); }

  bool InsertRow(std::string_view str, bool commit = true) {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    const RowPage& rp = page.GetRowPage();
    EXPECT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kRowPage);
    const bin_size_t before_size = rp.FreeSizeForTest();
    Status insert_result = page->Insert(txn, str).GetStatus();
    if (insert_result == Status::kSuccess) {
      EXPECT_EQ(rp.FreeSizeForTest(),
                before_size - str.size() - sizeof(RowPointer));
    }
    if (commit) {
      EXPECT_SUCCESS(txn.PreCommit());
    } else {
      page.PageUnlock();
      txn.Abort();
    }
    txn.CommitWait();
    return insert_result == Status::kSuccess;
  }

  void UpdateRow(int slot, std::string_view str, bool commit = true) {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Update(txn, slot, str));
    if (commit) {
      ASSERT_SUCCESS(txn.PreCommit());
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
    ASSERT_SUCCESS(page->Delete(txn, slot));
    if (commit) {
      ASSERT_SUCCESS(txn.PreCommit());
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
    ASSIGN_OR_CRASH(std::string_view, dst, page->Read(txn, slot));
    EXPECT_SUCCESS(txn.PreCommit());
    txn.CommitWait();
    return std::string(dst);
  }

  size_t GetRowCount() {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    EXPECT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kRowPage);
    size_t row_count = page->RowCount();
    EXPECT_SUCCESS(txn.PreCommit());
    txn.CommitWait();
    return row_count;
  }

  std::string file_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t page_id_ = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_PAGE_TEST_HPP
