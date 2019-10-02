//
// Created by kumagi on 2019/09/07.
//
#include "gtest/gtest.h"

#include "Page.hpp"

namespace pedasos {

class PageTest : public ::testing::Test {
public:
  void SetUp() override {
    fp = ::fopen("tmp.db", "rb+");
    if (fp == nullptr) {
      fp = ::fopen("tmp.db", "wb+");
    }
    ASSERT_EQ(0, ::fseek(fp, 0, SEEK_SET));
    page = new Page(fp, 0);
  }
  void TearDown() override {
    ASSERT_TRUE(page->Flush(fp, 0));
    ::fclose(fp);
  }

protected:
  pedasos::Page* page;
  FILE *fp;
};

TEST_F(PageTest, Create) {}

TEST_F(PageTest, Insert) {
  static constexpr char* kMsg = "hello this is payload.";
  std::unique_ptr<uint8_t[]>
      buff(new uint8_t[Row::EmptyRowSize() + strlen(kMsg)]);
  Row& r = reinterpret_cast<Row&>(*buff.get());
  r.tid = 10;
  r.bytes = strlen(kMsg);
  memcpy(r.payload, kMsg, strlen(kMsg));
  page->Insert(r);
  std::cout << *page << "\n";
}

}  // namespace pedasos