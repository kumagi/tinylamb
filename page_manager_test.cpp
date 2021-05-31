#include <string>

#include "gtest/gtest.h"
#include "page_manager.hpp"

namespace tinylamb {

class PageManagerTest : public ::testing::Test {
 protected:
  static constexpr char kFileName[] = "page_manager_test.db";
  void SetUp() override { Reset(); }

  void Reset() { pp_ = std::make_unique<PageManager>(kFileName, 10); }

  void TearDown() override { std::remove(kFileName); }

  std::unique_ptr<PageManager> pp_;
};

TEST_F(PageManagerTest, Construct) {}

TEST_F(PageManagerTest, AllocateNewPage) {
  for (int i = 0; i < 15; ++i) {
    Page* p = pp_->AllocateNewPage();
    uint8_t* buff = p->payload;
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < Page::PayloadSize(); ++j) {
      buff[j] = i;
    }
    pp_->Unpin(p->header.page_id);
  }
  Reset();
  for (int i = 0; i < 15; ++i) {
    Page* p = pp_->GetPage(i + 1);
    uint8_t* buff = p->payload;
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < Page::PayloadSize(); ++j) {
      ASSERT_EQ(buff[j], i);
    }
    pp_->Unpin(i + 1);
  }
}

TEST_F(PageManagerTest, DestroyPage) {
  for (int i = 0; i < 15; ++i) {
    Page* page = pp_->AllocateNewPage();
    pp_->DestroyPage(page);
  }
  for (int i = 0; i < 15; ++i) {
    Page* page = pp_->AllocateNewPage();
    pp_->Unpin(page->header.page_id);
    ASSERT_LE(page->header.page_id, 15);
  }
}


}  // namespace tinylamb