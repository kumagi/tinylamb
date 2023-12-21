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

#include "page/leaf_page.hpp"

#include <memory>
#include <string>

#include "common/random_string.hpp"
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
  void SetUp() override {
    std::string prefix = "leaf_page_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    leaf_page_id_ = page->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
  }

  PageRef Page() { return p_->GetPage(leaf_page_id_); }

  void TearDown() override {
    std::ignore = std::remove(db_name_.c_str());
    std::ignore = std::remove(log_name_.c_str());
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t leaf_page_id_{0};
};

TEST_F(LeafPageTest, Construct) {}

TEST_F(LeafPageTest, InsertLeaf) {
  auto txn = tm_->Begin();
  PageRef page = Page();

  // InsertBranch a value.
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  // Inserting with existing key will fail.
  ASSERT_FAIL(page->InsertLeaf(txn, "hello", "baby"));

  // We can read inserted value.
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out1, page->Read(txn, "hello"));
  ASSERT_EQ(out1, "world");

  // We cannot read wrong key.
  ASSERT_FAIL(page->Read(txn, "foo").GetStatus());
}

TEST_F(LeafPageTest, InsertMany) {
  constexpr int kRows = 20;
  auto txn = tm_->Begin();
  PageRef page = Page();
  for (size_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                    std::to_string(i) + ":value"));
  }

  for (size_t i = 0; i < kRows; ++i) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                          page->Read(txn, std::to_string(i) + ":key"));
    ASSERT_EQ(std::to_string(i) + ":value", out);
  }
}

TEST_F(LeafPageTest, InsertMany2) {
  auto txn = tm_->Begin();
  PageRef page = Page();
  for (const auto& c : {'a', 'b', 'c', 'd'}) {
    ASSERT_SUCCESS(
        page->InsertLeaf(txn, std::string(100, c), std::string(10, c)));
  }
  ASSERT_EQ(page->body.leaf_page.RowCount(), 4);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out1,
                        page->Read(txn, std::string(100, 'a')));
  ASSERT_EQ(std::string(10, 'a'), out1);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out2,
                        page->Read(txn, std::string(100, 'b')));
  ASSERT_EQ(std::string(10, 'b'), out2);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out3,
                        page->Read(txn, std::string(100, 'c')));
  ASSERT_EQ(std::string(10, 'c'), out3);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out4,
                        page->Read(txn, std::string(100, 'd')));
  ASSERT_EQ(std::string(10, 'd'), out4);
}

TEST_F(LeafPageTest, Update) {
  auto txn = tm_->Begin();
  PageRef page = Page();

  // InsertBranch value.
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  // Inserting with existing key will fail.
  ASSERT_SUCCESS(page->Update(txn, "hello", "baby"));

  // We can read updated value.
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out, page->Read(txn, "hello"));
  ASSERT_EQ(out, "baby");
}

TEST_F(LeafPageTest, UpdateMany) {
  auto txn = tm_->Begin();
  PageRef page = Page();

  // InsertBranch value.
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  // Overwrite value.
  for (size_t i = 1; i <= 1000000; i *= 10) {
    ASSERT_SUCCESS(page->Update(txn, "hello", "baby" + std::to_string(i)));
  }

  // We can read updated value.
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out, page->Read(txn, "hello"));
  ASSERT_EQ(out, "baby1000000");
}

TEST_F(LeafPageTest, Delete) {
  auto txn = tm_->Begin();
  PageRef page = Page();

  // InsertBranch value.
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  // Deleting unexciting value will fail.
  ASSERT_FAIL(page->Delete(txn, "hello1"));
  // Delete value.
  ASSERT_SUCCESS(page->Delete(txn, "hello"));
  // Cannot delete twice.
  ASSERT_FAIL(page->Delete(txn, "hello"));

  // We cannot update deleted value.
  ASSERT_FAIL(page->Update(txn, "hello", "hoge"));
  // We cannot read deleted value.
  ASSERT_FAIL(page->Read(txn, "hello").GetStatus());
}

TEST_F(LeafPageTest, DeleteMany) {
  constexpr int kRows = 10;
  auto txn = tm_->Begin();
  PageRef page = Page();

  // InsertBranch value.
  for (size_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS(page->InsertLeaf(txn, "k" + std::to_string(i),
                                    "v" + std::to_string(i + 1)));
  }
  // Deleting odd values.
  for (size_t i = 0; i < kRows; i += 2) {
    ASSERT_SUCCESS(page->Delete(txn, "k" + std::to_string(i)));
  }
  // Check all.
  for (size_t i = 0; i < kRows; ++i) {
    if (i % 2 == 0) {
      ASSERT_FAIL(page->Read(txn, "k" + std::to_string(i)).GetStatus());
    } else {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                            page->Read(txn, "k" + std::to_string(i)));
      ASSERT_EQ(out, "v" + std::to_string(i + 1));
    }
  }
}

TEST_F(LeafPageTest, InsertDefrag) {
  auto txn = tm_->Begin();
  PageRef page = Page();

  // InsertBranch value.
  std::string value;
  value.resize(5000);
  for (char& i : value) i = '1';
  ASSERT_SUCCESS(
      page->InsertLeaf(txn, "key1", value));  // About 5000 bytes used.
  ASSERT_SUCCESS(
      page->InsertLeaf(txn, "key2", value));  // About 10000 bytes used.
  for (char& i : value) i = '2';
  ASSERT_SUCCESS(
      page->InsertLeaf(txn, "key3", value));  // About 15000 bytes used.
  ASSERT_SUCCESS(
      page->InsertLeaf(txn, "key4", value));  // About 20000 bytes used.
  for (char& i : value) i = '3';
  ASSERT_SUCCESS(
      page->InsertLeaf(txn, "key5", value));  // About 25000 bytes used.
  ASSERT_SUCCESS(
      page->InsertLeaf(txn, "key6", value));          // About 30000 bytes used.
  ASSERT_FAIL(page->InsertLeaf(txn, "key7", value));  // No space left.
  ASSERT_SUCCESS(page->Delete(txn, "key2"));          // Make new space.
  for (char& i : value) i = '4';
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key7", value));  // Should success.
  ASSERT_FAIL(page->InsertLeaf(txn, "key8", value));     // No space left.
  ASSERT_SUCCESS(page->Delete(txn, "key1"));             // Make new space.
  for (char& i : value) i = '5';
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key8", value));  // Should success.

  ASSIGN_OR_ASSERT_FAIL(std::string_view, row1, page->Read(txn, "key3"));
  for (const char& i : row1) ASSERT_EQ(i, '2');
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row2, page->Read(txn, "key4"));
  for (const char& i : row2) ASSERT_EQ(i, '2');
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row3, page->Read(txn, "key5"));
  for (const char& i : row3) ASSERT_EQ(i, '3');
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row4, page->Read(txn, "key7"));
  for (const char& i : row4) ASSERT_EQ(i, '4');
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row5, page->Read(txn, "key8"));
  for (const char& i : row5) ASSERT_EQ(i, '5');
}

TEST_F(LeafPageTest, LowestHighestKey) {
  auto txn = tm_->Begin();
  PageRef page = Page();

  ASSERT_SUCCESS(page->InsertLeaf(txn, "C", "foo"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "A", "bar"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "B", "baz"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "D", "piy"));

  ASSIGN_OR_ASSERT_FAIL(std::string_view, lowest, page->LowestKey(txn));
  ASSERT_EQ(lowest, "A");

  ASSIGN_OR_ASSERT_FAIL(std::string_view, highest, page->HighestKey(txn));
  ASSERT_EQ(highest, "D");
}

TEST_F(LeafPageTest, Split) {
  auto txn = tm_->Begin();
  for (int i = 0; i < 8; ++i) {
    PageRef left = p_->AllocateNewPage(txn, PageType::kLeafPage);
    PageRef right = p_->AllocateNewPage(txn, PageType::kLeafPage);
    std::string key = std::string(2000, '0' + i) + "k";
    {
      for (const auto& c : {'1', '2', '3', '4', '5', '6', '7'}) {
        ASSERT_SUCCESS(
            left->InsertLeaf(txn, std::string(2000, c), std::string(2500, c)));
      }
      ASSERT_FAIL(left->InsertLeaf(txn, std::string(2000, '8'),
                                   std::string(2000, '8')));
      left->body.leaf_page.Split(left->PageID(), txn, key,
                                 std::string(2000, 'p'), right.get());
    }

    StatusOr<std::string_view> lowest_key = right->LowestKey(txn);
    if (lowest_key.HasValue() && key < lowest_key.Value()) {
      ASSERT_SUCCESS(left->InsertLeaf(txn, std::string(2000, '0' + i) + "k",
                                      std::string(2000, 'p')));
    } else {
      ASSERT_SUCCESS(right->InsertLeaf(txn, std::string(200, '0' + i) + "k",
                                       std::string(2000, 'p')));
    }
  }
}

TEST_F(LeafPageTest, InsertCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 20; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                            page->Read(txn, std::to_string(i) + ":key"));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, InsertAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 20; i += 2) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
      for (size_t i = 1; i < 20; i += 2) {
        ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                        std::to_string(i) + ":value"));
      }
    }
    txn.Abort();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 20; ++i) {
      if (i % 2 == 0) {
        ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                              page->Read(txn, std::to_string(i) + ":key"));
        ASSERT_EQ(std::to_string(i) + ":value", out);
      } else {
        ASSERT_FAIL(page->Read(txn, std::to_string(i) + ":key").GetStatus());
      }
    }
  }
}

TEST_F(LeafPageTest, UpdateCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Update(txn, std::to_string(i) + ":key",
                                  std::to_string(i * 2) + ":value"));
    }
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                            page->Read(txn, std::to_string(i) + ":key"));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, UpdateAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
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
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                            page->Read(txn, std::to_string(i) + ":key"));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, DeleteCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 1; i < 10; i += 2) {
      ASSERT_SUCCESS(page->Delete(txn, std::to_string(i) + ":key"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      if (i % 2 == 0) {
        ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                              page->Read(txn, std::to_string(i) + ":key"));
        ASSERT_EQ(std::to_string(i) + ":value", out);
      } else {
        ASSERT_FAIL(page->Read(txn, std::to_string(i) + ":key").GetStatus());
      }
    }
  }
}

TEST_F(LeafPageTest, DeleteAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
      for (size_t i = 0; i < 10; i += 2) {
        ASSERT_SUCCESS(page->Delete(txn, std::to_string(i) + ":key"));
      }
    }
    txn.Abort();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                            page->Read(txn, std::to_string(i) + ":key"));
      ASSERT_EQ(std::to_string(i) + ":value", out);
    }
  }
}

TEST_F(LeafPageTest, UpdateHeavy) {
  constexpr int kCount = 40;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  PageRef page = Page();
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10, false);
    std::string value = RandomString((19937 * i) % 120 + 10, false);
    ASSERT_SUCCESS(page->InsertLeaf(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }
  for (int i = 0; i < kCount * 8; ++i) {
    const std::string& key = keys[(i * 63) % keys.size()];
    std::string value = RandomString((19937 * i) % 320 + 100, false);
    ASSERT_SUCCESS(page->Update(txn, key, value));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, page->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(LeafPageTest, InsertDeleteHeavy) {
  constexpr int kCount = 40;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  PageRef page = Page();
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10, false);
    std::string value = RandomString((19937 * i) % 120 + 10, false);
    ASSERT_SUCCESS(page->InsertLeaf(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }
  for (int i = 0; i < kCount * 8; ++i) {
    const std::string& key = keys[(i * 63) % keys.size()];
    std::string value = RandomString((19937 * i) % 320 + 100, false);
    ASSERT_SUCCESS(page->Delete(txn, key));
    ASSERT_SUCCESS(page->InsertLeaf(txn, key, value));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, page->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(LeafPageTest, FosterChild) {
  for (int i = 0; i < 5; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10000, false);
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSERT_SUCCESS(page->SetFoster(txn, {key, page_id_t(i)}));
      ASSIGN_OR_ASSERT_FAIL(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_SUCCESS(page->SetFoster(txn, {}));
      ASSERT_SUCCESS(page->SetFoster(txn, FosterPair()));
      if (auto f = page->GetFoster(txn)) {
        ASSERT_TRUE(!"never reach here");
      }
      ASSERT_EQ(result.child_pid, i);
    }
  }
}

TEST_F(LeafPageTest, Fences) {
  Transaction txn = tm_->Begin();
  PageRef page = Page();
  for (int i = 0; i < 100; ++i) {
    std::string low = RandomString((19937 * i) % 12 + 10000, false);
    std::string high = RandomString((19937 * i) % 12 + 10000, false);
    ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey(low)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey(high)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_EQ(page->GetHighFence(txn), IndexKey(high));
  }
  ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey::MinusInfinity()));
  ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey::PlusInfinity()));
  ASSERT_TRUE(page->GetLowFence(txn).IsMinusInfinity());
  ASSERT_TRUE(page->GetHighFence(txn).IsPlusInfinity());
}

TEST_F(LeafPageTest, FencesCrash) {
  std::string low = RandomString(1234, false);
  std::string high = RandomString(4567, false);
  {
    Transaction txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey(low)));
    ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey(high)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_EQ(page->GetHighFence(txn), IndexKey(high));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());
  {
    auto restarted_txn = tm_->Begin();
    PageRef recovered_page = Page();
    ASSERT_EQ(recovered_page->GetLowFence(restarted_txn), IndexKey(low));
    ASSERT_EQ(recovered_page->GetHighFence(restarted_txn), IndexKey(high));
    ASSERT_SUCCESS(restarted_txn.PreCommit());
  }
}

TEST_F(LeafPageTest, FosterChildCrash) {
  for (int i = 0; i < 5; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10000, false);
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSERT_SUCCESS(page->SetFoster(txn, {key, page_id_t(i)}));
      ASSIGN_OR_ASSERT_FAIL(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_EQ(result.child_pid, i);
      ASSERT_SUCCESS(txn.PreCommit());
    }
    Recover();
    r_->RecoverFrom(0, tm_.get());
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSIGN_OR_ASSERT_FAIL(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_EQ(result.child_pid, i);
    }
  }
}

}  // namespace tinylamb