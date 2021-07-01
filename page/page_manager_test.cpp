#include "page_manager.hpp"

#include <string>
#include <transaction/lock_manager.hpp>
#include <transaction/transaction_manager.hpp>

#include "free_page.hpp"
#include "gtest/gtest.h"

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

  Page* AllocatePage(PageType expected_type) {
    Transaction system_txn = tm_->BeginTransaction();
    Page* new_page = p_->AllocateNewPage(system_txn, expected_type);
    EXPECT_NE(new_page, nullptr);
    EXPECT_EQ(new_page->Type(), PageType::kFreePage);
    return new_page;
  }

  void DestroyPage(Page* target) {
    Transaction system_txn = tm_->BeginTransaction();
    p_->DestroyPage(system_txn, target);
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(PageManagerTest, Construct) {}

TEST_F(PageManagerTest, AllocateNewPage) {
  constexpr int kPages = 15;
  std::set<uint64_t> allocated_ids;
  for (int i = 0; i <= kPages; ++i) {
    auto* page = reinterpret_cast<FreePage*>(AllocatePage(PageType::kFreePage));
    char* buff = page->FreeBody();
    for (size_t j = 0; j < FreePage::kFreeBodySize; ++j) {
      buff[j] = static_cast<char>((page->PageId() + j) & 0xff);
    }
    allocated_ids.insert(page->PageId());
    ASSERT_TRUE(p_->Unpin(page->PageId()));
  }
  Reset();
  for (const auto& id : allocated_ids) {
    auto* page = reinterpret_cast<FreePage*>(p_->GetPage(id));
    char* buff = page->FreeBody();
    for (size_t j = 0; j < FreePage::kFreeBodySize; ++j) {
      ASSERT_EQ(buff[j], static_cast<char>((id + j) & 0xff));
    }
    ASSERT_TRUE(p_->Unpin(page->PageId()));
  }
}

TEST_F(PageManagerTest, DestroyPage) {
  for (int i = 0; i < 15; ++i) {
    Page* page = AllocatePage(PageType::kFreePage);
    DestroyPage(page);
    p_->Unpin(page->PageId());
  }
  for (int i = 0; i < 15; ++i) {
    Page* page = AllocatePage(PageType::kFreePage);
    p_->Unpin(page->PageId());
    ASSERT_LE(page->PageId(), 15);
  }
}

}  // namespace tinylamb