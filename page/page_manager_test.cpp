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

#include "page_manager.hpp"

#include <cstdio>
#include <string>
#include <tuple>

#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "page/free_page.hpp"
#include "page/page_ref.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string prefix = "page_manager_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Reset();
  }

  void Reset() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               nullptr);
  }

  void TearDown() override {
    std::ignore = std::remove(db_name_.c_str());
    std::ignore = std::remove(log_name_.c_str());
  }

  PageRef AllocatePage(PageType expected_type) {
    Transaction system_txn = tm_->Begin();
    PageRef new_page = p_->AllocateNewPage(system_txn, expected_type);
    system_txn.PreCommit();
    EXPECT_FALSE(new_page.IsNull());
    EXPECT_EQ(new_page->Type(), PageType::kFreePage);
    return new_page;
  }

  PageRef GetPage(uint64_t page_id) {
    Transaction system_txn = tm_->Begin();
    PageRef got_page = p_->GetPage(page_id);
    EXPECT_TRUE(!got_page.IsNull());
    return got_page;
  }

  void DestroyPage(Page* target) {
    Transaction system_txn = tm_->Begin();
    p_->DestroyPage(system_txn, target);
    system_txn.PreCommit();
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(PageManagerTest, Construct) {}

TEST_F(PageManagerTest, AllocateNewPage) {
  PageRef page = AllocatePage(PageType::kFreePage);
  char* buff = page->body.free_page.FreeBody();
  for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
    // Make sure no SEGV happen.
    buff[j] = static_cast<char>((page->PageID() + j) & 0xff);
  }
}

TEST_F(PageManagerTest, AllocateMultipleNewPage) {
  constexpr int kPages = 15;
  std::set<page_id_t> allocated_ids;
  for (int i = 0; i <= kPages; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    char* buff = page->body.free_page.FreeBody();
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      buff[j] = static_cast<char>((page->PageID() + j) & 0xff);
    }
    allocated_ids.insert(page->PageID());
  }
  Reset();
  for (const auto& id : allocated_ids) {
    PageRef ref = GetPage(id);
    FreePage& page = ref.GetFreePage();
    char* buff = page.FreeBody();
    for (size_t j = 0; j < kFreeBodySize; ++j) {
      ASSERT_EQ(buff[j], static_cast<char>((id + j) & 0xff));
    }
  }
}

TEST_F(PageManagerTest, DestroyPage) {
  for (int i = 0; i < 15; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    DestroyPage(page.get());
  }
  for (int i = 0; i < 15; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    ASSERT_LE(page->PageID(), 15);
  }
}

}  // namespace tinylamb