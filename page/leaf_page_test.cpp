//
// Created by kumagi on 2022/01/15.
//
#include "page/leaf_page.hpp"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class LeafPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "leaf_page_test.db";
  static constexpr char kLogName[] = "leaf_page_test.log";

  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    leaf_page_id_ = page->PageID();
    EXPECT_TRUE(txn.PreCommit());
  }

  void Flush() { p_->GetPool()->FlushPageForTest(leaf_page_id_); }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
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

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t leaf_page_id_;
};

TEST_F(LeafPageTest, Construct) {}

TEST_F(LeafPageTest, Insert) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_TRUE(page->Insert(txn, "hello", "world"));
  // Inserting with existing key will fail.
  ASSERT_FALSE(page->Insert(txn, "hello", "baby"));

  // We can read inserted value.
  std::string_view out;
  ASSERT_TRUE(page->Read(txn, "hello", &out));
  ASSERT_EQ(out, "world");

  // We cannot read wrong key.
  ASSERT_FALSE(page->Read(txn, "foo", &out));
  ASSERT_NE(out, "world");
}

TEST_F(LeafPageTest, InsertMany) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);
  for (size_t i = 0; i < 20; ++i) {
    ASSERT_TRUE(page->Insert(txn, std::to_string(i) + ":key",
                             std::to_string(i) + ":value"));
  }
  std::string_view out;
  for (size_t i = 0; i < 20; ++i) {
    ASSERT_TRUE(page->Read(txn, std::to_string(i) + ":key", &out));
    ASSERT_EQ(std::to_string(i) + ":value", out);
  }
}

TEST_F(LeafPageTest, Update) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_TRUE(page->Insert(txn, "hello", "world"));
  // Inserting with existing key will fail.
  ASSERT_TRUE(page->Update(txn, "hello", "baby"));

  // We can read updated value.
  std::string_view out;
  ASSERT_TRUE(page->Read(txn, "hello", &out));
  ASSERT_EQ(out, "baby");
}

TEST_F(LeafPageTest, UpdateMany) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_TRUE(page->Insert(txn, "hello", "world"));
  // Inserting with existing key will fail.
  for (size_t i = 1; i <= 1000000; i *= 10) {
    ASSERT_TRUE(page->Update(txn, "hello", "baby" + std::to_string(i)));
  }

  // We can read updated value.
  std::string_view out;
  ASSERT_TRUE(page->Read(txn, "hello", &out));
  ASSERT_EQ(out, "baby1000000");
}

TEST_F(LeafPageTest, Delete) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_TRUE(page->Insert(txn, "hello", "world"));
  // Deleting unexciting value will fail.
  ASSERT_FALSE(page->Delete(txn, "hello1"));
  // Delete value.
  ASSERT_TRUE(page->Delete(txn, "hello"));
  // Cannot delete twice.
  ASSERT_FALSE(page->Delete(txn, "hello"));

  // We cannot update deleted value.
  ASSERT_FALSE(page->Update(txn, "hello", "hoge"));
  // We cannot read deleted value.
  std::string_view out;
  ASSERT_FALSE(page->Read(txn, "hello", &out));
}

TEST_F(LeafPageTest, DeleteMany) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_TRUE(page->Insert(txn, "k" + std::to_string(i),
                             "v" + std::to_string(i + 1)));
  }
  // Deleting odd values.
  for (size_t i = 0; i < 100; i += 2) {
    ASSERT_TRUE(page->Delete(txn, "k" + std::to_string(i)));
  }
  // Check all.
  for (size_t i = 0; i < 100; ++i) {
    std::string_view out;
    if (i % 2 == 0) {
      ASSERT_FALSE(page->Read(txn, "k" + std::to_string(i), &out));
      ASSERT_TRUE(out.empty());
    } else {
      ASSERT_TRUE(page->Read(txn, "k" + std::to_string(i), &out));
      ASSERT_EQ(out, "v" + std::to_string(i + 1));
    }
  }
}
}  // namespace tinylamb