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

#include <cstddef>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
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
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), r_.get());
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

TEST_F(LeafPageTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest death on crash, gtest green on pass
}

TEST_F(LeafPageTest, InsertLeaf) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert a key-value pair, then attempt duplicate insert (should fail)
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  ASSERT_FAIL(page->InsertLeaf(txn, "hello", "baby"));

  // Assert -- reading the inserted key returns the value; reading wrong key
  // fails
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out1, page->Read(txn, "hello"));
  ASSERT_EQ(out1, "world");
  ASSERT_FAIL(page->Read(txn, "foo").GetStatus());
}

TEST_F(LeafPageTest, InsertMany) {
  // Arrange
  constexpr int kRows = 20;
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert kRows key-value pairs
  for (size_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                    std::to_string(i) + ":value"));
  }

  // Assert -- reading each inserted key returns the corresponding value
  for (size_t i = 0; i < kRows; ++i) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, out,
                          page->Read(txn, std::to_string(i) + ":key"));
    ASSERT_EQ(std::to_string(i) + ":value", out);
  }
}

TEST_F(LeafPageTest, InsertMany2) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert four 100-char keys with 10-char values
  for (const auto& c : {'a', 'b', 'c', 'd'}) {
    ASSERT_SUCCESS(
        page->InsertLeaf(txn, std::string(100, c), std::string(10, c)));
  }

  // Assert -- row count is 4 and each key reads back its expected value
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
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert "hello" then update it to "baby"
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  ASSERT_SUCCESS(page->Update(txn, "hello", "baby"));

  // Assert -- reading the updated key returns the new value
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out, page->Read(txn, "hello"));
  ASSERT_EQ(out, "baby");
}

TEST_F(LeafPageTest, UpdateMany) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert "hello" then update it 6 times with progressively longer
  // values
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  for (size_t i = 1; i <= 1000000; i *= 10) {
    ASSERT_SUCCESS(page->Update(txn, "hello", "baby" + std::to_string(i)));
  }

  // Assert -- final read returns the last updated value
  ASSIGN_OR_ASSERT_FAIL(std::string_view, out, page->Read(txn, "hello"));
  ASSERT_EQ(out, "baby1000000");
}

TEST_F(LeafPageTest, Delete) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert "hello", then attempt deletes of non-existent and existing
  // keys
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  ASSERT_FAIL(page->Delete(txn, "hello1"));
  ASSERT_SUCCESS(page->Delete(txn, "hello"));
  ASSERT_FAIL(page->Delete(txn, "hello"));

  // Assert -- deleted key cannot be updated or read
  ASSERT_FAIL(page->Update(txn, "hello", "hoge"));
  ASSERT_FAIL(page->Read(txn, "hello").GetStatus());
}

TEST_F(LeafPageTest, DeleteMany) {
  // Arrange
  constexpr int kRows = 10;
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act 1 -- insert kRows key-value pairs
  for (size_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS(page->InsertLeaf(txn, "k" + std::to_string(i),
                                    "v" + std::to_string(i + 1)));
  }
  // Act 2 -- delete even-indexed keys
  for (size_t i = 0; i < kRows; i += 2) {
    ASSERT_SUCCESS(page->Delete(txn, "k" + std::to_string(i)));
  }

  // Assert -- even-indexed keys are gone, odd-indexed keys still readable
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
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  std::string value;
  value.resize(5000);
  for (char& i : value) {
    i = '1';
  }

  // Act 1 -- insert 6 large keys (~5000 bytes each) until page is full at key7
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key1", value));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key2", value));
  for (char& i : value) {
    i = '2';
  }
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key3", value));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key4", value));
  for (char& i : value) {
    i = '3';
  }
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key5", value));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key6", value));
  ASSERT_FAIL(page->InsertLeaf(txn, "key7", value));

  // Act 2 -- delete key2 to free space, then insert key7 (should succeed)
  ASSERT_SUCCESS(page->Delete(txn, "key2"));
  for (char& i : value) {
    i = '4';
  }
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key7", value));
  ASSERT_FAIL(page->InsertLeaf(txn, "key8", value));

  // Act 3 -- delete key1 to free more space, then insert key8 (should succeed)
  ASSERT_SUCCESS(page->Delete(txn, "key1"));
  for (char& i : value) {
    i = '5';
  }
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key8", value));

  // Assert -- surviving keys have the expected values; key2/key1 are gone
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row1, page->Read(txn, "key3"));
  for (const char& i : row1) {
    ASSERT_EQ(i, '2');
  }
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row2, page->Read(txn, "key4"));
  for (const char& i : row2) {
    ASSERT_EQ(i, '2');
  }
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row3, page->Read(txn, "key5"));
  for (const char& i : row3) {
    ASSERT_EQ(i, '3');
  }
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row4, page->Read(txn, "key7"));
  for (const char& i : row4) {
    ASSERT_EQ(i, '4');
  }
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row5, page->Read(txn, "key8"));
  for (const char& i : row5) {
    ASSERT_EQ(i, '5');
  }
}

TEST_F(LeafPageTest, LowestHighestKey) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert four keys out of order to test internal sorting
  ASSERT_SUCCESS(page->InsertLeaf(txn, "C", "foo"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "A", "bar"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "B", "baz"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "D", "piy"));

  // Assert -- LowestKey returns "A", HighestKey returns "D"
  ASSIGN_OR_ASSERT_FAIL(std::string_view, lowest, page->LowestKey(txn));
  ASSERT_EQ(lowest, "A");
  ASSIGN_OR_ASSERT_FAIL(std::string_view, highest, page->HighestKey(txn));
  ASSERT_EQ(highest, "D");
}

TEST_F(LeafPageTest, Split) {
  // Arrange -- nothing more than fixture setup; Split test works on allocated
  // pages
  auto txn = tm_->Begin();

  // Act -- for 8 iterations, fill a left leaf page, split it into a right page,
  //        then insert a separator key into either left or right depending on
  //        the lowest key of the right page after the split
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

  // Assert -- implicit; split logic verified by successful inserts and no
  // crashes
}

TEST_F(LeafPageTest, InsertCrash) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 20 key-value pairs and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- emulate crash, then recover and verify all 20 pairs survived
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

  // Assert -- all 20 key-value pairs survived the crash/recovery round-trip
  // (implicit in Act 2 assertions)
}

TEST_F(LeafPageTest, InsertAbort) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 10 even-indexed key-value pairs and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 20; i += 2) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- insert 10 odd-indexed key-value pairs then abort the transaction
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

  // Act 3 -- recover and verify only even-indexed pairs survived
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

  // Assert -- even-indexed pairs durable, odd-indexed pairs aborted
  // (implicit in Act 3 assertions)
}

TEST_F(LeafPageTest, UpdateCrash) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 10 key-value pairs and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- update all 10 values to i*2 then crash before commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->Update(txn, std::to_string(i) + ":key",
                                  std::to_string(i * 2) + ":value"));
    }
  }

  // Act 3 -- recover; updates were not committed so original values must remain
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

  // Assert -- uncommitted updates were discarded; original values preserved
  // (implicit in Act 3 assertions)
}

TEST_F(LeafPageTest, UpdateAbort) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 10 key-value pairs and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- update all 10 values to i*2 then abort the transaction
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

  // Act 3 -- recover; aborted updates leave original values intact
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

  // Assert -- aborted updates were discarded; original values preserved
  // (implicit in Act 3 assertions)
}

TEST_F(LeafPageTest, DeleteCrash) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 10 key-value pairs and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- delete odd-indexed keys and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 1; i < 10; i += 2) {
      ASSERT_SUCCESS(page->Delete(txn, std::to_string(i) + ":key"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 3 -- recover; committed deletes must persist, even-indexed keys remain
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

  // Assert -- committed deletes durable; even-indexed pairs survived
  // (implicit in Act 3 assertions)
}

TEST_F(LeafPageTest, DeleteAbort) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 10 key-value pairs and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(page->InsertLeaf(txn, std::to_string(i) + ":key",
                                      std::to_string(i) + ":value"));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- delete even-indexed keys then abort the transaction
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

  // Act 3 -- recover; aborted deletes leave all 10 pairs intact
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

  // Assert -- aborted deletes discarded; all 10 pairs preserved
  // (implicit in Act 3 assertions)
}

TEST_F(LeafPageTest, UpdateHeavy) {
  // Arrange
  constexpr int kCount = 40;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  PageRef page = Page();

  // Act 1 -- insert kCount random key-value pairs
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10, false);
    std::string value = RandomString(((19937 * i) % 120) + 10, false);
    ASSERT_SUCCESS(page->InsertLeaf(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }

  // Act 2 -- update each key kCount*8 times with new random values
  for (int i = 0; i < kCount * 8; ++i) {
    const std::string& key = keys[(static_cast<size_t>(i) * 63) % keys.size()];
    std::string value = RandomString(((19937 * i) % 320) + 100, false);
    ASSERT_SUCCESS(page->Update(txn, key, value));
    kvp[key] = value;
  }

  // Assert -- every key reads back the last value written via Update
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, page->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(LeafPageTest, InsertDeleteHeavy) {
  // Arrange
  constexpr int kCount = 40;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  PageRef page = Page();

  // Act 1 -- insert kCount random key-value pairs
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10, false);
    std::string value = RandomString(((19937 * i) % 120) + 10, false);
    ASSERT_SUCCESS(page->InsertLeaf(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }

  // Act 2 -- for kCount*8 iterations, delete then re-insert each key with new
  // value
  for (int i = 0; i < kCount * 8; ++i) {
    const std::string& key = keys[(static_cast<size_t>(i) * 63) % keys.size()];
    std::string value = RandomString(((19937 * i) % 320) + 100, false);
    ASSERT_SUCCESS(page->Delete(txn, key));
    ASSERT_SUCCESS(page->InsertLeaf(txn, key, value));
    kvp[key] = value;
  }

  // Assert -- every key reads back the last re-inserted value
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, page->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(LeafPageTest, FosterChild) {
  // Arrange -- nothing more than fixture setup

  // Act -- for 5 iterations, set foster pairs with random keys and child page
  // IDs
  for (int i = 0; i < 5; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10000, false);
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSERT_SUCCESS(page->SetFoster(txn, {key, page_id_t(i)}));
      ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_SUCCESS(page->SetFoster(txn, {}));
      ASSERT_SUCCESS(page->SetFoster(txn, FosterPair()));
      if (auto f = page->GetFoster(txn)) {
        ASSERT_TRUE(!"never reach here");
      }
      ASSERT_EQ(result.child_pid, i);
    }
  }

  // Assert -- foster pair get/set round-trip preserves key and child_pid
  // (implicit in Act assertions)
}

TEST_F(LeafPageTest, Fences) {
  // Arrange
  Transaction txn = tm_->Begin();
  PageRef page = Page();

  // Act -- for 100 iterations, set low/high fences with random strings and
  // verify
  for (int i = 0; i < 100; ++i) {
    std::string low = RandomString(((19937 * i) % 12) + 10000, false);
    std::string high = RandomString(((19937 * i) % 12) + 10000, false);
    ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey(low)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey(high)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_EQ(page->GetHighFence(txn), IndexKey(high));
  }

  // Act -- set fences to minus/plus infinity and verify
  ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey::MinusInfinity()));
  ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey::PlusInfinity()));

  // Assert -- fences at infinity are correctly reported
  ASSERT_TRUE(page->GetLowFence(txn).IsMinusInfinity());
  ASSERT_TRUE(page->GetHighFence(txn).IsPlusInfinity());
}

TEST_F(LeafPageTest, SetLowFenceOobOnFullPage) {
  // Reproduces a real bug in LeafPage::SetLowFence (page/leaf_page.cpp:327):
  // the free-space precheck only rejects the call when free_size_ < 2 and
  // never accounts for either the OLD fence size (freed inside UpdateSlotImpl)
  // or the actual NEW fence size. On a nearly-full page a growing fence update
  // triggers DeFragment inside UpdateSlotImpl, then subtracts the new fence
  // size from the freshly packed free_ptr_ -- underflowing the uint16 counter
  // -- and finally memcpy()s the fence payload to a wild offset far beyond the
  // page (heap-buffer-overflow). Setting a fence that does not fit must be
  // rejected with kNoSpace instead of scribbling out of bounds.
  auto txn = tm_->Begin();
  PageRef page = Page();
  // Fill the page with 292-byte keys and 292-byte values until full.
  for (int i = 0; i < 60; ++i) {
    std::string key = std::string(284, static_cast<char>('a' + (i / 10))) +
                      std::string(8, static_cast<char>('0' + (i % 10)));
    if (page->InsertLeaf(txn, key, std::string(292, 'v')) != Status::kSuccess) {
      break;
    }
  }
  // A 5000-byte low fence cannot possibly fit; the API must refuse it.
  std::string big_fence(5000, 'f');
  ASSERT_FAIL(page->SetLowFence(txn, IndexKey(big_fence)));
}

TEST_F(LeafPageTest, SetHighFenceOobOnFullPage) {
  // Same root cause as SetLowFenceOobOnFullPage, but through the high-fence
  // path: LeafPage::SetHighFence (page/leaf_page.cpp:342) reuses the same
  // broken free-space precheck, so a fence that does not fit underflows
  // free_ptr_ in UpdateSlotImpl and memcpy()s the fence payload past the end
  // of the page. The API must reject the oversized fence with kNoSpace.
  auto txn = tm_->Begin();
  PageRef page = Page();
  for (int i = 0; i < 60; ++i) {
    std::string key = std::string(284, static_cast<char>('a' + (i / 10))) +
                      std::string(8, static_cast<char>('0' + (i % 10)));
    if (page->InsertLeaf(txn, key, std::string(292, 'v')) != Status::kSuccess) {
      break;
    }
  }
  std::string big_fence(5000, 'f');
  ASSERT_FAIL(page->SetHighFence(txn, IndexKey(big_fence)));
}

TEST_F(LeafPageTest, FencesCrash) {
  // Arrange
  std::string low = RandomString(1234, false);
  std::string high = RandomString(4567, false);

  // Act 1 -- set low/high fences and commit
  {
    Transaction txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey(low)));
    ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey(high)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_EQ(page->GetHighFence(txn), IndexKey(high));
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 2 -- emulate crash, then recover and verify fences survived
  Recover();
  r_->RecoverFrom(0, tm_.get());
  {
    auto restarted_txn = tm_->Begin();
    PageRef recovered_page = Page();
    ASSERT_EQ(recovered_page->GetLowFence(restarted_txn), IndexKey(low));
    ASSERT_EQ(recovered_page->GetHighFence(restarted_txn), IndexKey(high));
    ASSERT_SUCCESS(restarted_txn.PreCommit());
  }

  // Assert -- fences survived the crash/recovery round-trip
  // (implicit in Act 2 assertions)
}

TEST_F(LeafPageTest, FosterChildCrash) {
  // Arrange -- nothing more than fixture setup

  // Act -- for 5 iterations, set foster pair, commit, crash, recover, verify
  for (int i = 0; i < 5; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10000, false);
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSERT_SUCCESS(page->SetFoster(txn, {key, page_id_t(i)}));
      ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_EQ(result.child_pid, i);
      ASSERT_SUCCESS(txn.PreCommit());
    }
    Recover();
    r_->RecoverFrom(0, tm_.get());
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_EQ(result.child_pid, i);
    }
  }

  // Assert -- foster pair survived each crash/recovery round-trip
  // (implicit in Act assertions)
}

TEST_F(LeafPageTest, InsertTooBigData) {
  // Arrange -- nothing more than fixture setup
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- a key/value pair larger than the per-entry limit must be rejected
  Status result =
      page->InsertLeaf(txn, std::string(20000, 'k'), std::string(100, 'v'));

  // Assert -- kTooBigData
  ASSERT_EQ(result, Status::kTooBigData);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(LeafPageTest, UpdateTooBigValue) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));

  // Act -- updating with an oversized value must be rejected up-front
  Status result = page->Update(txn, "hello", std::string(20000, 'v'));

  // Assert -- kTooBigData
  ASSERT_EQ(result, Status::kTooBigData);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(LeafPageTest, ReadSlotOutOfRange) {
  // Arrange -- empty leaf page
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- read a non-existent slot through both slot-based readers
  ASSERT_EQ(page->Read(txn, 0).GetStatus(), Status::kNotExists);
  ASSERT_EQ(page->ReadKey(txn, 0).GetStatus(), Status::kNotExists);

  // Assert -- populated slot is readable via both readers
  ASSERT_SUCCESS(page->InsertLeaf(txn, "k", "v"));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, value, page->Read(txn, 0));
  ASSERT_EQ(value, "v");
  ASSIGN_OR_ASSERT_FAIL(std::string_view, key, page->ReadKey(txn, 0));
  ASSERT_EQ(key, "k");
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(LeafPageTest, DumpLeafPage) {
  // Arrange -- populated page with a foster pair
  auto txn = tm_->Begin();
  PageRef page = Page();
  ASSERT_SUCCESS(page->InsertLeaf(txn, "hello", "world"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "key", "value"));
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair("zz", 42)));

  // Act -- stream the page through the Dump path
  std::ostringstream oss;
  oss << *page;

  // Assert -- header, rows and foster key are all rendered
  const std::string dumped = oss.str();
  EXPECT_NE(dumped.find("LeafPage"), std::string::npos);
  EXPECT_NE(dumped.find("FosterKey"), std::string::npos);
  EXPECT_NE(dumped.find("hello"), std::string::npos);
  ASSERT_SUCCESS(txn.PreCommit());
}

// SanityCheckForTest returns false on fence/foster violations (no abort).
TEST_F(LeafPageTest, SanityCheckDetectsFenceAndFosterViolations) {
  // Arrange -- keys that fences/foster can be moved outside of
  auto txn = tm_->Begin();
  PageRef page = Page();
  ASSERT_SUCCESS(page->InsertLeaf(txn, "a", "1"));
  ASSERT_SUCCESS(page->InsertLeaf(txn, "c", "3"));

  // Act/Assert -- a low fence above the first key violates sanity
  ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey("b")));
  EXPECT_FALSE(page->body.leaf_page.SanityCheckForTest());

  // Act/Assert -- a high fence below the last key violates sanity
  ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey::MinusInfinity()));
  ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey("b")));
  EXPECT_FALSE(page->body.leaf_page.SanityCheckForTest());

  // Act/Assert -- a foster key not after the last key violates sanity
  ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey::PlusInfinity()));
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair("b", 99)));
  EXPECT_FALSE(page->body.leaf_page.SanityCheckForTest());

  // Act/Assert -- restoring consistent fences/foster restores sanity
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair()));
  ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey::MinusInfinity()));
  ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey::PlusInfinity()));
  EXPECT_TRUE(page->body.leaf_page.SanityCheckForTest());
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(LeafPageTest, InsertHugeKeyOverflowsPhysicalSize) {
  // Real bug: LeafPage::Insert (page/leaf_page.cpp:57-59) accumulates the
  // serialized sizes of key and value into a bin_size_t (uint16).  A key and
  // value whose combined serialized size exceeds 65535 bytes make physical_size
  // wrap around to a tiny value, so both the kFanoutThreshold guard and the
  // free_size_ precheck at page/leaf_page.cpp:67 are bypassed.  InsertImpl then
  // trusts the wrapped size and memcpy()s the full key bytes from a free_ptr_
  // near the top of the page (page/leaf_page.cpp:91) -- a heap-buffer-overflow
  // of the 32KiB page allocation.  An oversized entry must be rejected with
  // kTooBigData instead of scribbling out of bounds.
  auto txn = tm_->Begin();
  PageRef page = Page();
  // key(32766) serializes to 32768 bytes; value(32767) to 32769 bytes; the sum
  // 65537 wraps physical_size to 1, fooling every size check.
  std::string key(32766, 'k');
  std::string value(32767, 'v');
  Status result = page->InsertLeaf(txn, key, value);
  ASSERT_EQ(result, Status::kTooBigData);
}

TEST_F(LeafPageTest, UpdateHugeValueOverflowsPhysicalSize) {
  // Real bug: the same uint16 overflow as InsertHugeKeyOverflowsPhysicalSize,
  // reached through LeafPage::Update (page/leaf_page.cpp:104-110).  For an
  // existing key, a value large enough that key+value serialized size wraps
  // physical_size below kFanoutThreshold bypasses the size guard; UpdateImpl
  // then carries a real payload that is thousands of bytes, UpdateSlotImpl
  // underflows free_ptr_ and memcpy()s the value past the end of the page
  // (page/leaf_page.cpp:148).  An oversized value must be rejected with
  // kTooBigData instead of overflowing the page.
  auto txn = tm_->Begin();
  PageRef page = Page();
  ASSERT_SUCCESS(page->InsertLeaf(txn, "k", "v"));
  // key("k") serializes to 3 bytes; value(65534) to 65536 bytes; the sum 65539
  // wraps physical_size to 3, below the fanout threshold, so Update proceeds.
  std::string value(65534, 'v');
  Status result = page->Update(txn, "k", value);
  ASSERT_EQ(result, Status::kTooBigData);
}
TEST_F(LeafPageTest, ImplOperationsAreIdempotent) {
  // D2 (docs/design.md): redo/undo apply through the *Impl entry points, so
  // re-applying the same records must keep row count, slot and key content
  // intact instead of duplicating entries or removing live neighbours.
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->body.leaf_page.InsertImpl("k1", "v1");
  page->body.leaf_page.InsertImpl("k2", "v2");
  page->body.leaf_page.InsertImpl("k1", "v1");  // re-applied insert
  ASSERT_EQ(page->body.leaf_page.RowCount(), 2);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, v1, page->Read(txn, "k1"));
  EXPECT_EQ(v1, "v1");

  page->body.leaf_page.UpdateImpl("k1", "v1b");
  page->body.leaf_page.UpdateImpl("missing", "x");  // absent key: no-op
  ASSERT_EQ(page->body.leaf_page.RowCount(), 2);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, v1b, page->Read(txn, "k1"));
  EXPECT_EQ(v1b, "v1b");

  page->body.leaf_page.DeleteImpl("k1");
  page->body.leaf_page.DeleteImpl("k1");           // re-applied delete
  page->body.leaf_page.DeleteImpl("never-there");  // absent key: no-op
  ASSERT_EQ(page->body.leaf_page.RowCount(), 1);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, v2, page->Read(txn, "k2"));
  EXPECT_EQ(v2, "v2");
}

}  // namespace tinylamb
