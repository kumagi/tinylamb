#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "page_pool.hpp"

namespace tinylamb {

class PagePoolTest : public ::testing::Test {
 protected:
  static constexpr char kFileName[] = "page_pool_test.db";
  static constexpr int kDefaultCapacity = 10;
  void SetUp() override {
    Reset();
  }
  void Reset() {
    pp = std::make_unique<PagePool>(kFileName, kDefaultCapacity);
  }
  void TearDown() override {
    std::remove(kFileName);
  }

  std::unique_ptr<PagePool> pp = nullptr;
};

TEST_F(PagePoolTest, Construct) {
  ASSERT_EQ(pp->Size(), 0);
}

TEST_F(PagePoolTest, GetPage) {
  Page* page = pp->GetPage(0);
  ASSERT_EQ(pp->Size(), 1);
  pp->Unpin(0);
}

TEST_F(PagePoolTest, GetPageSeveralpattern) {
  std::vector<int> pattern = {0, 0, 1, 0, 2};
  for (int & i : pattern) {
    Page* page = pp->GetPage(i);
    ASSERT_EQ(page->PageId(), i);
    pp->Unpin(i);
  }
}

TEST_F(PagePoolTest, GetManyPage) {
  for (int i = 0; i < 5; ++i) {
    Page* p = pp->GetPage(i);
    ASSERT_EQ(p->PageId(), i);
    ASSERT_EQ(pp->Size(), i + 1);
    pp->Unpin(i);
  }
}

TEST_F(PagePoolTest, EvictPage) {
  for (int i = 0; i < 15; ++i) {
    Page* p = pp->GetPage(i);
    ASSERT_EQ(p->PageId(), i);
    ASSERT_EQ(pp->Size(), std::min(i + 1, kDefaultCapacity));
    pp->Unpin(i);
  }
}

TEST_F(PagePoolTest, PersistencyWithReset) {
  constexpr size_t kPages = 11;
  for (int i = 0; i < kPages; ++i) {
    Page* p = pp->GetPage(i);
    char* buff = p->page_body;
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < kPageBodySize; ++j) {
      buff[j] = i;
    }
    pp->Unpin(i);
  }
  // Reset();
  for (int i = 0; i < kPages; ++i) {
    Page* p = pp->GetPage(i);
    char* buff = p->page_body;
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < kPageBodySize; ++j) {
      EXPECT_EQ(buff[j], i);
    }
    pp->Unpin(i);
  }
}

}  // namespace tinylamb
