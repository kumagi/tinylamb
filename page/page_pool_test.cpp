#include "page_pool.hpp"

#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "page_ref.hpp"

namespace tinylamb {

class PagePoolTest : public ::testing::Test {
 protected:
  static constexpr int kDefaultCapacity = 10;
  void SetUp() override {
    filename_ = "page_pool_test-" + RandomString();
    Reset();
  }
  void Reset() { pp = std::make_unique<PagePool>(filename_, kDefaultCapacity); }
  void TearDown() override { std::remove(filename_.c_str()); }

  std::string filename_;
  std::unique_ptr<PagePool> pp = nullptr;
};

TEST_F(PagePoolTest, Construct) { ASSERT_EQ(pp->Size(), 0); }

TEST_F(PagePoolTest, GetPage) {
  PageRef page = pp->GetPage(0, nullptr);
  ASSERT_EQ(pp->Size(), 1);
}

TEST_F(PagePoolTest, GetPageSeveralpattern) {
  std::vector<int> pattern = {0, 0, 1, 0, 2};
  for (int& i : pattern) {
    PageRef page = pp->GetPage(i, nullptr);
    ASSERT_EQ(page->PageID(), i);
  }
}

TEST_F(PagePoolTest, GetManyPage) {
  for (int i = 0; i < 5; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
    ASSERT_EQ(p->PageID(), i);
    ASSERT_EQ(pp->Size(), i + 1);
  }
}

TEST_F(PagePoolTest, EvictPage) {
  for (int i = 0; i < 15; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
    ASSERT_EQ(p->PageID(), i);
    ASSERT_EQ(pp->Size(), std::min(i + 1, kDefaultCapacity));
  }
}

TEST_F(PagePoolTest, PersistencyWithReset) {
  constexpr size_t kPages = 11;
  for (size_t i = 0; i < kPages; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      buff[j] = i;
    }
  }
  // Reset();
  for (size_t i = 0; i < kPages; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      EXPECT_EQ(buff[j], i);
    }
  }
}

}  // namespace tinylamb
