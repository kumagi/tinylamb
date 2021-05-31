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
    // std::remove(kFileName);
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

TEST_F(PagePoolTest, GetManyPage) {
  for (int i = 0; i < 5; ++i) {
    Page* p = pp->GetPage(i);
    ASSERT_EQ(p->header.page_id, i);
    ASSERT_EQ(pp->Size(), i + 1);
    pp->Unpin(i);
  }
}

TEST_F(PagePoolTest, EvictPage) {
  for (int i = 0; i < 15; ++i) {
    Page* p = pp->GetPage(i);
    ASSERT_EQ(p->header.page_id, i);
    ASSERT_EQ(pp->Size(), std::min(i + 1, kDefaultCapacity));
    pp->Unpin(i);
  }
}

TEST_F(PagePoolTest, PersistencyWithReset) {
  for (int i = 0; i < 30; ++i) {
    Page* p = pp->GetPage(i);
    uint8_t* buff = p->payload;
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < Page::PayloadSize(); ++j) {
      buff[j] = i;
    }
    pp->Unpin(i);
  }
  Reset();
  for (int i = 0; i < 30; ++i) {
    Page* p = pp->GetPage(i);
    uint8_t* buff = p->payload;
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < Page::PayloadSize(); ++j) {
      ASSERT_EQ(buff[j], i);
    }
    pp->Unpin(i);
  }
}

}  // namespace tinylamb
