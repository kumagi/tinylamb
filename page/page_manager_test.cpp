#include "page_manager.hpp"

#include <string>

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
  static constexpr char kFileName[] = "page_manager_test.db";
  static constexpr char kLogName[] = "page_manager_test.log";
  void SetUp() override {
    std::remove(kFileName);
    std::remove(kLogName);
    Reset();
  }

  void Reset() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
  }

  void TearDown() override {
    std::remove(kFileName);
    std::remove(kLogName);
  }

  PageRef AllocatePage(PageType expected_type) {
    Transaction system_txn = tm_->Begin();
    PageRef new_page = p_->AllocateNewPage(system_txn, expected_type);
    system_txn.PreCommit();
    EXPECT_FALSE(new_page.IsNull());
    EXPECT_EQ(new_page->Type(), PageType::kFreePage);
    return std::move(new_page);
  }

  PageRef GetPage(uint64_t page_id) {
    Transaction system_txn = tm_->Begin();
    PageRef got_page = p_->GetPage(page_id);
    EXPECT_TRUE(!got_page.IsNull());
    return std::move(got_page);
  }

  void DestroyPage(Page* target) {
    Transaction system_txn = tm_->Begin();
    p_->DestroyPage(system_txn, target);
    system_txn.PreCommit();
  }

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
    buff[j] = static_cast<char>((page->PageId() + j) & 0xff);
  }
}

TEST_F(PageManagerTest, AllocateMultipleNewPage) {
  constexpr int kPages = 15;
  std::set<uint64_t> allocated_ids;
  for (int i = 0; i <= kPages; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    char* buff = page->body.free_page.FreeBody();
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      buff[j] = static_cast<char>((page->PageId() + j) & 0xff);
    }
    allocated_ids.insert(page->PageId());
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
    ASSERT_LE(page->PageId(), 15);
  }
}

}  // namespace tinylamb