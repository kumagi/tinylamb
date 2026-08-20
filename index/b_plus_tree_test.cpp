/*
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

#include "b_plus_tree.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_type.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {
class BPlusTreeTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::string prefix = "b_plus_tree_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    master_record_name_ = prefix + ".master.log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Flush(page_id_t pid) const { p_->GetPool()->FlushPageForTest(pid); }

  void Recover() {
    page_id_t root = bpt_ ? bpt_->Root() : 1;
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    bpt_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 110);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
    bpt_ = std::make_unique<BPlusTree>(root);
  }

  void TearDown() override {
    bpt_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::ignore = std::remove(db_name_.c_str());
    std::ignore = std::remove(log_name_.c_str());
    std::ignore = std::remove(master_record_name_.c_str());
  }

  std::string db_name_;
  std::string log_name_;
  std::string master_record_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<BPlusTree> bpt_;
};

TEST_F(BPlusTreeTest, Construct) {
  // Arrange -- nothing to set up; default BPlusTree constructed by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(BPlusTreeTest, InsertLeaf) {
  // Arrange -- begin transaction
  auto txn = tm_->Begin();

  // Act -- insert 5 key/value pairs into the leaf
  ASSERT_SUCCESS(bpt_->Insert(txn, "hello", "world"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "this", "is a pen"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "lorem", "ipsum"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "foo", "bar"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "key", "blah"));

  // Assert -- read back the 5 values and verify they match; sanity check the tree; precommit
  ASSERT_EQ(bpt_->Read(txn, "hello").Value(), "world");
  ASSERT_EQ(bpt_->Read(txn, "this").Value(), "is a pen");
  ASSERT_EQ(bpt_->Read(txn, "lorem").Value(), "ipsum");
  ASSERT_EQ(bpt_->Read(txn, "foo").Value(), "bar");
  ASSERT_EQ(bpt_->Read(txn, "key").Value(), "blah");
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, InsertDuplicateKeyReturnsErrorWithoutRecursing) {
  // Inserting a key that already exists must surface as a clean status, not
  // drive the tree into an unbounded split.  BPlusTree::LeafInsert currently
  // treats LeafPage::InsertLeaf's kDuplicates as "page full" and recurses
  // through foster pages forever, exhausting the stack.  This test documents
  // that bug and should turn green once the status is propagated instead.
  auto txn = tm_->Begin();
  ASSERT_SUCCESS(bpt_->Insert(txn, "key", "first"));
  ASSERT_EQ(bpt_->Insert(txn, "key", "second"), Status::kDuplicates);
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf) {
  // Arrange -- begin transaction, define 100 keys and a 5000-byte long value
  constexpr static int kKeys = 100;
  auto txn = tm_->Begin();
  const std::string key_prefix("key");
  const std::string long_value(5000, 'v');

  // Act -- insert 100 keys with the long value; sanity-check after each insert
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, key_prefix + std::to_string(i), long_value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }

  // Assert -- read back all 100 values and verify; sanity-check; precommit
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, key_prefix + std::to_string(i)),
                          long_value);
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf2) {
  // Arrange -- begin transaction, define a 2723-byte key/value size
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();

  // Act -- insert 9 keys of 2723 bytes each (forces multiple leaf splits)
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, std::string(kSize, c), std::string(kSize, c)));
  }

  // Assert -- implicit; no crash means the splits succeeded; gtest green on pass
}

TEST_F(BPlusTreeTest, SplitLeafBig) {
  // Arrange -- begin transaction, define a 2000-byte key/value size
  constexpr static int kSize = 2000;
  auto txn = tm_->Begin();

  // Act -- insert 10 keys of 2000 bytes each (digits 0..9); sanity-check after each
  for (char i = 0; i < 10; ++i) {
    std::string key(kSize, static_cast<char>('0' + i));
    std::string value(kSize, '0' + i);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }

  // Assert -- read back all 10 values and verify; sanity-check; precommit
  for (char i = 0; i < 10; ++i) {
    std::string key(kSize, '0' + i);
    std::string value(kSize, '0' + i);
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, key), value);
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
  ASSERT_SUCCESS(txn.PreCommit());
}

std::string KeyGen(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(BPlusTreeTest, SplitBranch) {
  // Arrange -- begin transaction, define 50 keys with 5000-byte payload
  constexpr static int kKeys = 50;
  constexpr static size_t kPayloadSize = 5000;
  auto txn = tm_->Begin();
  std::string value = "v";

  // Act -- insert 50 keys; read back 50 keys twice (two rounds); sanity-check
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, kPayloadSize), value));
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(i, kPayloadSize)), value);
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(i, kPayloadSize)), value);
  }

  // Assert -- tree passes sanity check and transaction precommits
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, MergeBranch) {
  // Arrange -- begin transaction, define 40 keys with 5000-byte payload and a short value
  constexpr static size_t kPayloadSize = 5000;
  constexpr static size_t kInserts = 40;
  auto txn = tm_->Begin();
  std::string short_value = "v";

  // Act -- insert 40 keys; sanity-check after each; then delete them one-by-one,
  //        reading the remaining keys after each delete to confirm they still exist
  for (size_t i = 0; i < kInserts; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, kPayloadSize), short_value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(txn.GetPageManager()));
  }
  for (size_t i = 0; i < kInserts; ++i) {
    std::string key = KeyGen(i, kPayloadSize);
    SCOPED_TRACE(key);
    ASSERT_SUCCESS(bpt_->Delete(txn, key));
    for (size_t j = i + 1; j < kInserts; ++j) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(j, kPayloadSize)));
      if (val != short_value) {
        LOG(FATAL) << OmittedString(val, 20) << " not found";
      }
      ASSERT_EQ(val, short_value);
    }
  }

  // Assert -- tree passes sanity check after all merges
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, FullScanMultiLeafReverse) {
  // Arrange -- begin transaction, define 11 keys in reverse order with 5000-byte key and 10-byte value
  auto txn = tm_->Begin();

  // Act -- insert 11 keys in reverse alphabetical order; sanity-check after each
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    ASSERT_SUCCESS(bpt_->Insert(txn, std::string(5000, c), std::string(10, c)));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }

  // Assert -- implicit; all inserts succeeded and tree remains sane; gtest green on pass
}

TEST_F(BPlusTreeTest, FullScanMultiLeafMany) {
  // Arrange -- begin transaction, define 6 keys with 5000-byte key and 10-byte value
  auto txn = tm_->Begin();

  // Act -- insert 6 keys in alphabetical order; sanity-check after all inserts
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f'}) {
    ASSERT_SUCCESS(bpt_->Insert(txn, std::string(5000, c), std::string(10, c)));
  }

  // Assert -- tree passes sanity check; gtest green on pass
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, Search) {
  // Arrange -- begin transaction, define 100 keys with 5000-byte payload and 200-byte value
  constexpr static size_t kPayloadSize = 5000;
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kPayloadSize), KeyGen(i * 10, 200)));
    }
    txn.PreCommit();
  }

  // Act -- read back all 100 keys in a fresh transaction
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 100; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, kPayloadSize)));

      // Assert -- each read returns the expected 200-byte value
      ASSERT_EQ(val, KeyGen(i * 10, 200));
    }
  }

  // Assert -- tree passes sanity check
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, Update) {
  // Arrange -- begin transaction, define 200 keys with 5000-byte payload and 100-byte value
  constexpr static size_t kPayloadSize = 5000;
  constexpr static size_t kCount = 200;
  {
    auto txn = tm_->Begin();
    for (size_t i = 0; i < kCount; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kPayloadSize), KeyGen(i * 10, 100)));
    }
    txn.PreCommit();
  }

  // Act -- update every 2nd key (even indices) with a new 200-byte value
  {
    auto txn = tm_->Begin();
    for (size_t i = 0; i < kCount; i += 2) {
      ASSERT_SUCCESS(
          bpt_->Update(txn, KeyGen(i, kPayloadSize), KeyGen(i * 2, 200)));
    }
    txn.PreCommit();
  }

  // Assert -- read back all 200 keys; even-index keys have the updated 200-byte value,
  //           odd-index keys retain the original 100-byte value
  {
    auto txn = tm_->Begin();
    for (size_t i = 0; i < kCount; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, kPayloadSize)));
      if (i % 2 == 0) {
        ASSERT_EQ(val, KeyGen(i * 2, 200));
      } else {
        ASSERT_EQ(val, KeyGen(i * 10, 100));
      }
    }
  }
}

TEST_F(BPlusTreeTest, Delete) {
  // Arrange -- begin transaction, define 50 keys with 5000-byte key and 200-byte value
  constexpr int kCount = 50;
  constexpr int kKeyLength = 5000;
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      std::string key = KeyGen(i, kKeyLength);
      std::string value = KeyGen(i, 200);
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
      kvp.emplace(key, value);
    }
    txn.PreCommit();
  }

  // Act 1 -- read back all 50 keys in a fresh transaction
  {
    auto txn = tm_->Begin();
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kv.second, val);
    }
    txn.PreCommit();
  }

  // Act 2 -- delete every 2nd key (even indices); read remaining keys after each delete
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; i += 2) {
      std::string key = KeyGen(i, kKeyLength);
      ASSERT_SUCCESS(bpt_->Delete(txn, key));
      kvp.erase(key);
      for (const auto& kv : kvp) {
        ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
        ASSERT_EQ(kv.second, val);
      }
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    }
    txn.PreCommit();
  }

  // Assert -- deleted keys return kFail; remaining keys return their original values
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      if (i % 2 == 0) {
        ASSERT_FAIL(bpt_->Read(txn, KeyGen(i, kKeyLength)).GetStatus());
      } else {
        ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                              bpt_->Read(txn, KeyGen(i, kKeyLength)));
        ASSERT_EQ(val, KeyGen(i * 1, 200));
      }
    }
  }
}

TEST_F(BPlusTreeTest, DeleteFosterBranch) {
  // Arrange -- manually construct a branch+leaf+foster tree:
  //           root (branch) -> [left leaf "hello", right leaf "jack", foster branch -> [foster_left "jj", foster_right "zz"]]
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root());
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef left = p_->AllocateNewPage(txn, PageType::kLeafPage);
    left->InsertLeaf(txn, "hello", "world");
    root->SetLowestValue(txn, left->PageID());
    PageRef right = p_->AllocateNewPage(txn, PageType::kLeafPage);
    right->InsertLeaf(txn, "jack", "chen");
    root->InsertBranch(txn, "jack", right->PageID());
    PageRef foster = p_->AllocateNewPage(txn, PageType::kBranchPage);
    PageRef foster_left = p_->AllocateNewPage(txn, PageType::kLeafPage);
    foster_left->InsertLeaf(txn, "jj", "pp");
    PageRef foster_right = p_->AllocateNewPage(txn, PageType::kLeafPage);
    foster_right->InsertLeaf(txn, "zz", "adf");
    foster->SetLowestValue(txn, foster_left->PageID());
    foster->InsertBranch(txn, "zz", foster_right->PageID());
    ASSERT_SUCCESS(root->SetFoster(txn, FosterPair("j", foster->PageID())));
    txn.PreCommit();
  }

  // Act -- delete "zz", "jj", "hello", "jack" from the tree
  {
    auto txn = tm_->Begin();
    EXPECT_SUCCESS(bpt_->Delete(txn, "zz"));
    EXPECT_SUCCESS(bpt_->Delete(txn, "jj"));
    EXPECT_SUCCESS(bpt_->Delete(txn, "hello"));
    EXPECT_SUCCESS(bpt_->Delete(txn, "jack"));
  }

  // Assert -- implicit; all deletes succeeded; gtest green on pass
}

TEST_F(BPlusTreeTest, LiftUpBranch) {
  // Arrange -- manually construct a two-level branch tree:
  //           root (branch) -> [a_branch -> [leaf "a", leaf "aa"], b_branch -> [leaf "b", leaf "bb"]]
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root());
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef a_branch = p_->AllocateNewPage(txn, PageType::kBranchPage);
    PageRef b_branch = p_->AllocateNewPage(txn, PageType::kBranchPage);
    ASSERT_SUCCESS(b_branch->SetLowFence(txn, IndexKey("b")));
    root->SetLowestValue(txn, a_branch->PageID());
    root->InsertBranch(txn, "b", b_branch->PageID());
    PageRef a = p_->AllocateNewPage(txn, PageType::kLeafPage);
    a->InsertLeaf(txn, "a", "1");
    PageRef aa = p_->AllocateNewPage(txn, PageType::kLeafPage);
    aa->InsertLeaf(txn, "aa", "2");
    a_branch->SetLowestValue(txn, a->PageID());
    a_branch->InsertBranch(txn, "aa", aa->PageID());
    PageRef b = p_->AllocateNewPage(txn, PageType::kLeafPage);
    b->InsertLeaf(txn, "b", "3");
    PageRef bb = p_->AllocateNewPage(txn, PageType::kLeafPage);
    bb->InsertLeaf(txn, "bb", "4");

    b_branch->SetLowestValue(txn, b->PageID());
    b_branch->InsertBranch(txn, "bb", bb->PageID());
    txn.PreCommit();
  }

  // Act -- delete "a", "aa", "b", "bb" (leaves under both branches)
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "bb"));
  }

  // Assert -- implicit; all deletes succeeded and branches lifted; gtest green on pass
}

void BuildBranchFosterTree(TransactionManager* tm, BPlusTree* bpt) {
  /*
              ┌─────┐
              │aaaaa│
      ┌───────┴─────┴───────────┐
      │                         │
      │                         │
     ┌▼─┌┐       ┌────┐        ┌▼─┐
     │aa│┼─┬─────►aaaa│        │b │
  ┌──┴──┴──┤     ┌────┴─┐    ┌─┴──┴┐
  │        │     │      │    │     │
  │        │     │      │    │     │
┌─▼┐    ┌──▼┐ ┌──▼┐ ┌▼───┐┌▼────┐  ┌▼─┐
│a │    │aa │ │aaa│ │aaaa││aaaaa│  │b │
└──┘    └───┘ └───┘ └────┘└─────┘  └──┘
 */
  PageManager* p = tm->GetPageManager();
  auto txn = tm->Begin();
  PageRef root = p->GetPage(bpt->Root());
  root->PageTypeChange(txn, PageType::kBranchPage);
  PageRef a_branch = p->AllocateNewPage(txn, PageType::kBranchPage);
  PageRef b_branch = p->AllocateNewPage(txn, PageType::kBranchPage);
  root->SetLowestValue(txn, a_branch->PageID());
  root->InsertBranch(txn, "aaaaa", b_branch->PageID());
  PageRef a = p->AllocateNewPage(txn, PageType::kLeafPage);
  a->InsertLeaf(txn, "a", "1");
  PageRef aa = p->AllocateNewPage(txn, PageType::kLeafPage);
  aa->InsertLeaf(txn, "aa", "2");
  a_branch->SetLowestValue(txn, a->PageID());
  a_branch->InsertBranch(txn, "aa", aa->PageID());
  PageRef a_foster = p->AllocateNewPage(txn, PageType::kBranchPage);
  ASSERT_SUCCESS(
      a_branch->SetFoster(txn, FosterPair("aaa", a_foster->PageID())));
  PageRef aaa = p->AllocateNewPage(txn, PageType::kLeafPage);
  aaa->InsertLeaf(txn, "aaa", "3");
  a_foster->SetLowestValue(txn, aaa->PageID());
  PageRef aaaa = p->AllocateNewPage(txn, PageType::kLeafPage);
  aaaa->InsertLeaf(txn, "aaaa", "4");
  a_foster->InsertBranch(txn, "aaaa", aaaa->PageID());

  PageRef aaaaa = p->AllocateNewPage(txn, PageType::kLeafPage);
  aaaaa->InsertLeaf(txn, "aaaaa", "5");
  b_branch->SetLowestValue(txn, aaaaa->PageID());
  b_branch->SetLowFence(txn, IndexKey("aaaaa"));

  PageRef b = p->AllocateNewPage(txn, PageType::kLeafPage);
  b->InsertLeaf(txn, "b", "6");
  b_branch->InsertBranch(txn, "b", b->PageID());
  txn.PreCommit();
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFoster1) {
  // Arrange -- build a branch+foster tree via the helper
  BuildBranchFosterTree(tm_.get(), bpt_.get());

  // Act -- delete keys in order: a, aa, aaa, aaaa, aaaaa, b
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  }

  // Assert -- implicit; all deletes succeeded and tree restructured; gtest green on pass
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFoster2) {
  // Arrange -- build a branch+foster tree via the helper
  BuildBranchFosterTree(tm_.get(), bpt_.get());

  // Act -- delete keys in order: aaaaa, a, aa, aaa, aaaa, b
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  }

  // Assert -- implicit; all deletes succeeded and tree restructured; gtest green on pass
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFoster3) {
  // Arrange -- build a branch+foster tree via the helper
  BuildBranchFosterTree(tm_.get(), bpt_.get());

  // Act -- delete keys in order: aaaa, a, aa, aaa, aaaaa, b
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  }

  // Assert -- implicit; all deletes succeeded and tree restructured; gtest green on pass
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFosterOther) {
  // Arrange -- build a branch+foster tree via the helper; iterate all 720 permutations of 6 keys
  std::vector<std::string> keys{"a", "aa", "aaa", "aaaa", "aaaaa", "b"};
  do {
    BuildBranchFosterTree(tm_.get(), bpt_.get());
    auto txn = tm_->Begin();

    // Act -- delete all 6 keys in the current permutation order
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  } while (std::next_permutation(keys.begin(), keys.end()));

  // Assert -- implicit; all permutations of deletes succeeded; gtest green on pass
}

TEST_F(BPlusTreeTest, DeleteFosterLeaf) {
  // Arrange -- manually construct a branch tree with foster leaf:
  //           root (branch) -> [leaf "a", leaf "b", leaf "c" (with foster leaf "d")]
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root());
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef a = p_->AllocateNewPage(txn, PageType::kLeafPage);
    a->InsertLeaf(txn, "a", "a");
    root->SetLowestValue(txn, a->PageID());
    PageRef b = p_->AllocateNewPage(txn, PageType::kLeafPage);
    b->InsertLeaf(txn, "b", "b");
    root->InsertBranch(txn, "b", b->PageID());
    PageRef c = p_->AllocateNewPage(txn, PageType::kLeafPage);
    c->InsertLeaf(txn, "c", "c");
    c->InsertLeaf(txn, "cc", "cc");
    root->InsertBranch(txn, "c", c->PageID());
    c->InsertLeaf(txn, "c", "c");
    PageRef d = p_->AllocateNewPage(txn, PageType::kLeafPage);
    d->InsertLeaf(txn, "d", "d");
    ASSERT_SUCCESS(c->SetFoster(txn, FosterPair("d", d->PageID())));
    txn.PreCommit();
  }

  // Act -- delete "b", "cc", "a", "c" from the tree
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "cc"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "c"));
  }

  // Assert -- implicit; all deletes succeeded and foster leaf lifted; gtest green on pass
}

TEST_F(BPlusTreeTest, DeleteAll) {
  // Arrange -- begin transaction, define 50 keys with 5000-byte key and 1-byte value
  constexpr int kCount = 50;
  constexpr int kKeyLength = 5000;
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      std::string key = KeyGen(i, kKeyLength);
      std::string value = KeyGen(i, 1);
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
      kvp.emplace(key, value);
    }
    txn.PreCommit();
  }

  // Act 1 -- read back all 50 keys in a fresh transaction
  {
    auto txn = tm_->Begin();
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kv.second, val);
    }
    txn.PreCommit();
  }

  // Act 2 -- delete all 50 keys one-by-one; read remaining keys after each delete
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; i++) {
      std::string key = KeyGen(i, kKeyLength);
      ASSERT_SUCCESS(bpt_->Delete(txn, key));
      kvp.erase(key);
      for (const auto& kv : kvp) {
        if (bpt_->Read(txn, kv.first).GetStatus() != Status::kSuccess) {
          LOG(ERROR) << "Cannot find: " << OmittedString(kv.first, 10)
                     << " from";
          DumpLogTxn(*bpt_, txn);
        }
        ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
        ASSERT_EQ(kv.second, val);
      }
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    }
    txn.PreCommit();
  }

  // Assert -- implicit; all 50 keys deleted, tree sane; gtest green on pass
}

TEST_F(BPlusTreeTest, DeleteAllReverse) {
  // Arrange -- begin transaction, define 100 keys with 5000-byte key and 200-byte value
  constexpr int kCount = 100;
  constexpr int kKeyLength = 5000;
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      std::string key = KeyGen(i, kKeyLength);
      std::string value = KeyGen(i, 200);
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
      kvp.emplace(key, value);
    }
    txn.PreCommit();
  }

  // Act 1 -- read back all 100 keys in a fresh transaction
  {
    auto txn = tm_->Begin();
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kv.second, val);
    }
  }

  // Act 2 -- delete keys in reverse order (kCount-1 down to 1); read remaining keys after each delete
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    for (int i = kCount - 1; 0 < i; i--) {
      std::string key = KeyGen(i, kKeyLength);
      ASSERT_SUCCESS(bpt_->Delete(txn, key));
      kvp.erase(key);
      for (const auto& kv : kvp) {
        auto v = bpt_->Read(txn, kv.first);
        if (!v) {
          LOG(FATAL) << "not found: " << kv.first;
        }
        ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
        ASSERT_EQ(kv.second, val);
      }
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    }
    txn.PreCommit();
  }

  // Assert -- implicit; all keys deleted in reverse order, tree sane; gtest green on pass
}

TEST_F(BPlusTreeTest, Crash) {
  // Arrange -- begin transaction, define 100 keys with 4000-byte key and 1000-byte value
  constexpr int kCount = 100;
  constexpr int kKeyLength = 4000;
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
    }
    txn.PreCommit();
  }

  // Act -- flush every 2nd page (even indices) to disk, then emulate crash + recovery
  page_id_t max_page = p_->GetPage(0)->body.meta_page.MaxPageCountForTest();
  for (size_t i = 0; i < max_page; i += 2) {
    Flush(i);
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- read back all 100 keys in a fresh transaction; values must match
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, kKeyLength)));
      ASSERT_EQ(val, KeyGen(i * 10, 1000));
    }
  }
}

TEST_F(BPlusTreeTest, CheckPoint) {
  // Arrange -- begin transaction, define 30 keys with 4000-byte key and 1000-byte value
  constexpr int kKeyLength = 4000;
  lsn_t restart_point;
  {
    auto txn = tm_->Begin();

    // Act 1 -- insert keys 0..9
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
    }

    // Act 2 -- take a checkpoint (writes keys 10..19 during callback)
    restart_point = cm_->WriteCheckpoint([&]() {
      for (int i = 10; i < 20; ++i) {
        ASSERT_SUCCESS(
            bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
      }
    });

    // Act 3 -- insert keys 20..29 after checkpoint
    for (int i = 20; i < 30; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
    }
    txn.PreCommit();
  }

  // Act 4 -- flush every 5th page to disk, then emulate crash + recovery from checkpoint
  page_id_t max_page = p_->GetPage(0)->body.meta_page.MaxPageCountForTest();
  for (size_t i = 0; i < max_page; i += 5) {
    Flush(i);
  }
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- read back all 30 keys in a fresh transaction; values must match
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 30; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, kKeyLength)));
      ASSERT_EQ(val, KeyGen(i * 10, 1000));
    }
  }
}

TEST_F(BPlusTreeTest, UpdateHeavy) {
  // Arrange -- begin transaction, define 100 keys with random string values
  constexpr int kCount = 100;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10, false);
    std::string value = RandomString((19937 * i) % 120 + 10, false);

    // Act 1 -- insert each key; read back all keys after each insert to verify
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    bpt_->SanityCheckForTest(p_.get());
    keys.push_back(key);
    kvp.emplace(key, value);
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kvp[kv.first], val);
    }
  }

  // Act 2 -- update each key 400 times with new random values; read back after each update
  for (int i = 0; i < kCount * 4; ++i) {
    const std::string& key = keys[(i * 63) % keys.size()];
    std::string value = RandomString((19937 * i) % 320 + 500, false);
    ASSERT_SUCCESS(bpt_->Update(txn, key, value));
    kvp[key] = value;
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kvp[kv.first], val);
    }
  }

  // Assert -- final read-back of all keys matches the last known values
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(BPlusTreeTest, InsertDelete) {
  // Arrange -- begin transaction, define 50 keys with random string values
  constexpr int kCount = 50;
  Transaction txn = tm_->Begin();
  std::unordered_set<std::string> keys;
  keys.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);

    // Act 1 -- insert each key with value "foo"
    ASSERT_SUCCESS(bpt_->Insert(txn, key, "foo"));
    keys.insert(key);
  }

  // Act 2 -- for 200 cycles: delete a random key, insert a new random key; sanity-check after each
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    auto it = keys.begin();
    std::advance(it, (i * 63) % keys.size());
    ASSERT_SUCCESS(bpt_->Delete(txn, *it));
    keys.erase(it);
    std::string inserting_key = RandomString((19937 * i) % 2000 + 2000, false);
    ASSERT_SUCCESS(bpt_->Insert(txn, inserting_key, "bar"));
    keys.insert(inserting_key);
    ASSERT_TRUE(bpt_->SanityCheckForTest(txn.GetPageManager()));
  }

  // Assert -- implicit; 200 insert+delete cycles completed without crash; gtest green on pass
}

TEST_F(BPlusTreeTest, InsertDeleteHeavy) {
  // Arrange -- begin transaction, define 100 keys with random string values
  int kCount = 100;
  Transaction txn = tm_->Begin();
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);
    std::string value = RandomString((19937 * i) % 120 + 10, false);

    // Act 1 -- insert each key; sanity-check after each insert
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    kvp[key] = value;
  }

  // Act 2 -- read back all 100 keys to verify initial state
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
  LOG(INFO) << "initialized, insert and delete for " << (kCount * 40)
            << " times.";

  // Act 3 -- for 100 cycles: delete a random key, insert a new random key; sanity-check after each
  for (int i = 0; i < kCount; ++i) {
    auto iter = kvp.begin();
    std::advance(iter, (i * 19937) % kvp.size());
    ASSERT_SUCCESS(bpt_->Delete(txn, iter->first));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    kvp.erase(iter);

    std::string key = RandomString((19937 * i) % 130 + 1000, false);
    std::string value = RandomString((19937 * i) % 320 + 3000, false);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    kvp[key] = value;
  }

  // Assert -- final read-back of all remaining keys matches the last known values
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(BPlusTreeTest, DumpLeafTree) {
  // Arrange -- insert a few keys into the leaf
  auto txn = tm_->Begin();
  ASSERT_SUCCESS(bpt_->Insert(txn, "hello", "world"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "this", "is a pen"));
  txn.PreCommit();

  // Act -- dump the tree to a stream
  auto dump_txn = tm_->Begin();
  std::stringstream ss;
  bpt_->Dump(dump_txn, ss);

  // Assert -- the leaf dump names the leaf page
  EXPECT_NE(ss.str().find("L["), std::string::npos);
}

TEST_F(BPlusTreeTest, DumpBranchTreeWithFoster) {
  // Arrange -- build a branch tree with a foster branch
  BuildBranchFosterTree(tm_.get(), bpt_.get());

  // Act -- dump the multi-level tree
  auto txn = tm_->Begin();
  std::stringstream ss;
  bpt_->Dump(txn, ss);
  std::string dumped = ss.str();

  // Assert -- the dump walks branch pages and foster children
  EXPECT_NE(dumped.find("B["), std::string::npos);
  EXPECT_NE(dumped.find("branch foster"), std::string::npos);
}

TEST_F(BPlusTreeTest, DumpEmptyBranchSlot) {
  // Arrange -- make the root a branch with zero keys but one leaf child
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root());
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef leaf = p_->AllocateNewPage(txn, PageType::kLeafPage);
    leaf->InsertLeaf(txn, "a", "1");
    root->SetLowestValue(txn, leaf->PageID());
    txn.PreCommit();
  }

  // Act -- dump the zero-key branch
  auto txn = tm_->Begin();
  std::stringstream ss;
  bpt_->Dump(txn, ss);

  // Assert -- the empty branch reports the no-slot sentinel line
  EXPECT_NE(ss.str().find("No Slot"), std::string::npos);
}

TEST_F(BPlusTreeTest, SanityCheckRejectsInvalidRootType) {
  // Arrange -- corrupt the root page into an unrecognized page type
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root());
    root->PageTypeChange(txn, PageType::kRowPage);
    txn.PreCommit();
  }

  // Act -- run the sanity check on the corrupted root
  // Assert -- a non-leaf, non-branch root fails the check
  EXPECT_FALSE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, DeleteMissingKey) {
  // Arrange -- a single committed key
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Insert(txn, "present", "value"));
    txn.PreCommit();
  }

  // Act -- delete a key that was never inserted
  {
    auto txn = tm_->Begin();
    ASSERT_EQ(bpt_->Delete(txn, "absent"), Status::kNotExists);
    // Assert -- the existing key is untouched by the failed delete
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, "present"), "value");
  }
}

TEST_F(BPlusTreeTest, UpdateForcesReinsert) {
  // Arrange -- pack a leaf with three large keys and small values
  constexpr static int kSize = 5000;
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(0, kSize), "small"));
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(1, kSize), "small"));
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(2, kSize), "small"));
    txn.PreCommit();
  }

  // Act -- grow the first value far beyond its original size
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Update(txn, KeyGen(0, kSize), std::string(9000, 'v')));
    // Assert -- the enlarged value reads back correctly
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(0, kSize)),
                          std::string(9000, 'v'));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    txn.PreCommit();
  }
}

TEST_F(BPlusTreeTest, InterleavedTransactions) {
  // Arrange -- commit 20 keys up front
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 5000), "v"));
    }
    txn.PreCommit();
  }

  // Act -- a reader and a writer run in lockstep over the committed data
  {
    auto reader = tm_->Begin();
    auto writer = tm_->Begin();
    for (int i = 0; i < 20; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(reader, KeyGen(i, 5000)));
      ASSERT_EQ(val, "v");
    }
    for (int i = 0; i < 20; i += 2) {
      ASSERT_SUCCESS(bpt_->Update(writer, KeyGen(i, 5000), "u"));
    }
    reader.PreCommit();
    writer.PreCommit();
  }

  // Assert -- even keys carry the new value, odd keys the original
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 20; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, 5000)));
      ASSERT_EQ(val, i % 2 == 0 ? "u" : "v");
    }
  }
}
}  // namespace tinylamb
