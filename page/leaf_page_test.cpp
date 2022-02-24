//
// Created by kumagi on 2022/01/15.
//
#include "page/leaf_page.hpp"

#include <memory>
#include <string>

#include "common/test_util.hpp"
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
    EXPECT_SUCCESS(txn.PreCommit());
  }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t leaf_page_id_;
};

TEST_F(LeafPageTest, Construct) {}

TEST_F(LeafPageTest, Insert) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_SUCCESS(page->Insert(txn, "hello", "world"));
  // Inserting with existing key will fail.
  ASSERT_FAIL(page->Insert(txn, "hello", "baby"));

  // We can read inserted value.
  std::string_view out;
  ASSERT_SUCCESS(page->Read(txn, "hello", &out));
  ASSERT_EQ(out, "world");

  // We cannot read wrong key.
  ASSERT_FAIL(page->Read(txn, "foo", &out));
  ASSERT_NE(out, "world");
}

TEST_F(LeafPageTest, InsertMany) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);
  for (size_t i = 0; i < 20; ++i) {
    ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                std::to_string(i) + ":value"));
  }
  std::string_view out;
  for (size_t i = 0; i < 20; ++i) {
    ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
    ASSERT_EQ(std::to_string(i) + ":value", out);
  }
}

TEST_F(LeafPageTest, InsertMany2) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);
  for (const auto& c : {'a', 'b', 'c', 'd'}) {
    ASSERT_SUCCESS(page->Insert(txn, std::string(100, c), std::string(10, c)));
  }
  std::string_view out;
  ASSERT_EQ(page->body.leaf_page.RowCount(), 4);
  ASSERT_SUCCESS(page->Read(txn, std::string(100, 'a'), &out));
  ASSERT_EQ(std::string(10, 'a'), out);
  ASSERT_SUCCESS(page->Read(txn, std::string(100, 'b'), &out));
  ASSERT_EQ(std::string(10, 'b'), out);
  ASSERT_SUCCESS(page->Read(txn, std::string(100, 'c'), &out));
  ASSERT_EQ(std::string(10, 'c'), out);
  ASSERT_SUCCESS(page->Read(txn, std::string(100, 'd'), &out));
  ASSERT_EQ(std::string(10, 'd'), out);
}

TEST_F(LeafPageTest, Update) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_SUCCESS(page->Insert(txn, "hello", "world"));
  // Inserting with existing key will fail.
  ASSERT_SUCCESS(page->Update(txn, "hello", "baby"));

  // We can read updated value.
  std::string_view out;
  ASSERT_SUCCESS(page->Read(txn, "hello", &out));
  ASSERT_EQ(out, "baby");
}

TEST_F(LeafPageTest, UpdateMany) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_SUCCESS(page->Insert(txn, "hello", "world"));
  // Inserting with existing key will fail.
  for (size_t i = 1; i <= 1000000; i *= 10) {
    ASSERT_SUCCESS(page->Update(txn, "hello", "baby" + std::to_string(i)));
  }

  // We can read updated value.
  std::string_view out;
  ASSERT_SUCCESS(page->Read(txn, "hello", &out));
  ASSERT_EQ(out, "baby1000000");
}

TEST_F(LeafPageTest, Delete) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  ASSERT_SUCCESS(page->Insert(txn, "hello", "world"));
  // Deleting unexciting value will fail.
  ASSERT_FAIL(page->Delete(txn, "hello1"));
  // Delete value.
  ASSERT_SUCCESS(page->Delete(txn, "hello"));
  // Cannot delete twice.
  ASSERT_FAIL(page->Delete(txn, "hello"));

  // We cannot update deleted value.
  ASSERT_FAIL(page->Update(txn, "hello", "hoge"));
  // We cannot read deleted value.
  std::string_view out;
  ASSERT_FAIL(page->Read(txn, "hello", &out));
}

TEST_F(LeafPageTest, DeleteMany) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_SUCCESS(page->Insert(txn, "k" + std::to_string(i),
                                "v" + std::to_string(i + 1)));
  }
  // Deleting odd values.
  for (size_t i = 0; i < 100; i += 2) {
    ASSERT_SUCCESS(page->Delete(txn, "k" + std::to_string(i)));
  }
  // Check all.
  for (size_t i = 0; i < 100; ++i) {
    std::string_view out;
    if (i % 2 == 0) {
      ASSERT_FAIL(page->Read(txn, "k" + std::to_string(i), &out));
      ASSERT_TRUE(out.empty());
    } else {
      ASSERT_SUCCESS(page->Read(txn, "k" + std::to_string(i), &out));
      ASSERT_EQ(out, "v" + std::to_string(i + 1));
    }
  }
}

TEST_F(LeafPageTest, InsertDeflag) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  // Insert value.
  std::string value;
  value.resize(10000);
  for (char& i : value) i = '1';
  ASSERT_SUCCESS(page->Insert(txn, "key1", value));  // About 10000 bytes used.
  for (char& i : value) i = '2';
  ASSERT_SUCCESS(page->Insert(txn, "key2", value));  // About 20000 bytes used.
  for (char& i : value) i = '3';
  ASSERT_SUCCESS(page->Insert(txn, "key3", value));  // About 30000 bytes used.
  ASSERT_FAIL(page->Insert(txn, "key4", value));     // No space left.
  ASSERT_SUCCESS(page->Delete(txn, "key2"));         // Make new space.
  for (char& i : value) i = '4';
  ASSERT_SUCCESS(page->Insert(txn, "key4", value));  // Should success.
  ASSERT_FAIL(page->Insert(txn, "key5", value));     // No space left.
  ASSERT_SUCCESS(page->Delete(txn, "key1"));         // Make new space.
  for (char& i : value) i = '5';
  ASSERT_SUCCESS(page->Insert(txn, "key5", value));  // Should success.

  std::string_view row;
  ASSERT_SUCCESS(page->Read(txn, "key3", &row));
  for (const char& i : row) ASSERT_EQ(i, '3');
  ASSERT_SUCCESS(page->Read(txn, "key4", &row));
  for (const char& i : row) ASSERT_EQ(i, '4');
  ASSERT_SUCCESS(page->Read(txn, "key5", &row));
  for (const char& i : row) ASSERT_EQ(i, '5');
}

TEST_F(LeafPageTest, LowestHighestKey) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(leaf_page_id_);

  ASSERT_SUCCESS(page->Insert(txn, "C", "foo"));
  ASSERT_SUCCESS(page->Insert(txn, "A", "bar"));
  ASSERT_SUCCESS(page->Insert(txn, "B", "baz"));
  ASSERT_SUCCESS(page->Insert(txn, "D", "piy"));

  std::string_view out;
  ASSERT_SUCCESS(page->LowestKey(txn, &out));
  ASSERT_EQ(out, "A");

  ASSERT_SUCCESS(page->HighestKey(txn, &out));
  ASSERT_EQ(out, "D");
}

TEST_F(LeafPageTest, Split) {
  auto txn = tm_->Begin();
  for (int i = 0; i < 8; ++i) {
    PageRef left = p_->AllocateNewPage(txn, PageType::kLeafPage);
    PageRef right = p_->AllocateNewPage(txn, PageType::kLeafPage);
    {
      for (const auto& c : {'1', '2', '3', '4', '5', '6', '7'}) {
        ASSERT_SUCCESS(
            left->Insert(txn, std::string(2000, c), std::string(2500, c)));
      }
      ASSERT_FAIL(
          left->Insert(txn, std::string(2000, '8'), std::string(2000, '8')));
      left->body.leaf_page.Split(left->PageID(), txn,
                                 std::string(2000, '0' + i) + "k",
                                 std::string(2000, 'p'), right.get());
    }
    if (i < 5) {
      ASSERT_SUCCESS(left->Insert(txn, std::string(2000, '0' + i) + "k",
                                  std::string(2000, 'p')));
    } else {
      ASSERT_SUCCESS(right->Insert(txn, std::string(200, '0' + i) + "k",
                                   std::string(2000, 'p')));
    }
    LOG(TRACE) << *left;
    LOG(WARN) << *right;
  }
}

TEST_F(LeafPageTest, InsertCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                  std::to_string(i) + ":value"));
    }
    txn.PreCommit();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    std::string_view out;
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, InsertAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 20; i += 2) {
      ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                  std::to_string(i) + ":value"));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(leaf_page_id_);
      for (size_t i = 1; i < 20; i += 2) {
        ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                    std::to_string(i) + ":value"));
      }
    }
    txn.Abort();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    std::string_view out;
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 20; ++i) {
      if (i % 2 == 0) {
        ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
        ASSERT_EQ(std::to_string(i) + ":value", out);
      } else {
        ASSERT_FAIL(page->Read(txn, std::to_string(i) + ":key", &out));
      }
    }
  }
}

TEST_F(LeafPageTest, UpdateCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                  std::to_string(i) + ":value"));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Update(txn, std::to_string(i) + ":key",
                                  std::to_string(i * 2) + ":value"));
    }
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    std::string_view out;
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, UpdateAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                  std::to_string(i) + ":value"));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(leaf_page_id_);
      for (size_t i = 0; i < 10; ++i) {
        ASSERT_SUCCESS(page->Update(txn, std::to_string(i) + ":key",
                                    std::to_string(i * 2) + ":value"));
      }
    }
    txn.Abort();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    std::string_view out;
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, DeleteCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                  std::to_string(i) + ":value"));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 1; i < 10; i += 2) {
      ASSERT_SUCCESS(page->Delete(txn, std::to_string(i) + ":key"));
    }
    txn.PreCommit();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    std::string_view out;
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      if (i % 2 == 0) {
        ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
        ASSERT_EQ(std::to_string(i) + ":value", out);
      } else {
        ASSERT_FAIL(page->Read(txn, std::to_string(i) + ":key", &out));
      }
    }
  }
}

TEST_F(LeafPageTest, DeleteAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Insert(txn, std::to_string(i) + ":key",
                                  std::to_string(i) + ":value"));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(leaf_page_id_);
      for (size_t i = 0; i < 10; i += 2) {
        ASSERT_SUCCESS(page->Delete(txn, std::to_string(i) + ":key"));
      }
    }
    txn.Abort();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    std::string_view out;
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(leaf_page_id_);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Read(txn, std::to_string(i) + ":key", &out));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

}  // namespace tinylamb