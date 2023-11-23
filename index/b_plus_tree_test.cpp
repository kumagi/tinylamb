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

#include "b_plus_tree.hpp"

#include <memory>
#include <string>

#include "common/debug.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

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
      p_->GetPool()->LostAllPageForTest();
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
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
    std::remove(master_record_name_.c_str());
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

TEST_F(BPlusTreeTest, Construct) {}

TEST_F(BPlusTreeTest, InsertLeaf) {
  auto txn = tm_->Begin();
  ASSERT_SUCCESS(bpt_->Insert(txn, "hello", "world"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "this", "is a pen"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "lorem", "ipsum"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "foo", "bar"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "key", "blah"));

  ASSERT_EQ(bpt_->Read(txn, "hello").Value(), "world");
  ASSERT_EQ(bpt_->Read(txn, "this").Value(), "is a pen");
  ASSERT_EQ(bpt_->Read(txn, "lorem").Value(), "ipsum");
  ASSERT_EQ(bpt_->Read(txn, "foo").Value(), "bar");
  ASSERT_EQ(bpt_->Read(txn, "key").Value(), "blah");
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf) {
  constexpr static int kKeys = 100;
  auto txn = tm_->Begin();
  const std::string key_prefix("key");
  const std::string long_value(5000, 'v');
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, key_prefix + std::to_string(i), long_value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, key_prefix + std::to_string(i)),
                          long_value);
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf2) {
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, std::string(kSize, c), std::string(kSize, c)));
  }
}

TEST_F(BPlusTreeTest, SplitLeafBig) {
  constexpr static int kSize = 2000;
  auto txn = tm_->Begin();
  for (char i = 0; i < 10; ++i) {
    std::string key(kSize, static_cast<char>('0' + i));
    std::string value(kSize, '0' + i);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
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
  constexpr static int kKeys = 50;
  constexpr static size_t kPayloadSize = 5000;
  auto txn = tm_->Begin();
  std::string value = "v";
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, kPayloadSize), value));
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(i, kPayloadSize)), value);
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(i, kPayloadSize)), value);
  }
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, MergeBranch) {
  constexpr static size_t kPayloadSize = 5000;
  constexpr static size_t kInserts = 40;
  auto txn = tm_->Begin();
  std::string short_value = "v";
  for (size_t i = 0; i < kInserts; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, kPayloadSize), short_value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(txn.PageManager()));
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
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, FullScanMultiLeafReverse) {
  auto txn = tm_->Begin();
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    ASSERT_SUCCESS(bpt_->Insert(txn, std::string(5000, c), std::string(10, c)));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
}

TEST_F(BPlusTreeTest, FullScanMultiLeafMany) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f'}) {
    ASSERT_SUCCESS(bpt_->Insert(txn, std::string(5000, c), std::string(10, c)));
  }
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, Search) {
  constexpr static size_t kPayloadSize = 5000;
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kPayloadSize), KeyGen(i * 10, 200)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    for (int i = 0; i < 100; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, kPayloadSize)));
      ASSERT_EQ(val, KeyGen(i * 10, 200));
    }
  }
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
}

TEST_F(BPlusTreeTest, Update) {
  constexpr static size_t kPayloadSize = 5000;
  constexpr static size_t kCount = 200;
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (size_t i = 0; i < kCount; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kPayloadSize), KeyGen(i * 10, 100)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    for (size_t i = 0; i < kCount; i += 2) {
      ASSERT_SUCCESS(
          bpt_->Update(txn, KeyGen(i, kPayloadSize), KeyGen(i * 2, 200)));
    }
    txn.PreCommit();
  }
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
  constexpr int kCount = 50;
  constexpr int kKeyLength = 5000;
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < kCount; ++i) {
      std::string key = KeyGen(i, kKeyLength);
      std::string value = KeyGen(i, 200);
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
      kvp.emplace(key, value);
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kv.second, val);
    }
    txn.PreCommit();
  }
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
  {
    auto txn = tm_->Begin();
    EXPECT_SUCCESS(bpt_->Delete(txn, "zz"));
    EXPECT_SUCCESS(bpt_->Delete(txn, "jj"));
    EXPECT_SUCCESS(bpt_->Delete(txn, "hello"));
    EXPECT_SUCCESS(bpt_->Delete(txn, "jack"));
  }
}

TEST_F(BPlusTreeTest, LiftUpBranch) {
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
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "bb"));
  }
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
  BuildBranchFosterTree(tm_.get(), bpt_.get());
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  }
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFoster2) {
  BuildBranchFosterTree(tm_.get(), bpt_.get());
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  }
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFoster3) {
  BuildBranchFosterTree(tm_.get(), bpt_.get());
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  }
}

TEST_F(BPlusTreeTest, LiftUpBranchWithFosterOther) {
  std::vector<std::string> keys{"a", "aa", "aaa", "aaaa", "aaaaa", "b"};
  do {
    BuildBranchFosterTree(tm_.get(), bpt_.get());
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "aaaaa"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
  } while (std::next_permutation(keys.begin(), keys.end()));
}

TEST_F(BPlusTreeTest, DeleteFosterLeaf) {
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
  {
    auto txn = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Delete(txn, "b"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "cc"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "a"));
    ASSERT_SUCCESS(bpt_->Delete(txn, "c"));
  }
}

TEST_F(BPlusTreeTest, DeleteAll) {
  constexpr int kCount = 50;
  constexpr int kKeyLength = 5000;
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < kCount; ++i) {
      std::string key = KeyGen(i, kKeyLength);
      std::string value = KeyGen(i, 1);
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
      kvp.emplace(key, value);
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kv.second, val);
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(200, 'v');
    for (int i = 0; i < kCount; i++) {
      std::string key = KeyGen(i, kKeyLength);
      ASSERT_SUCCESS(bpt_->Delete(txn, key));
      kvp.erase(key);
      for (const auto& kv : kvp) {
        if (bpt_->Read(txn, kv.first).GetStatus() != Status::kSuccess) {
          LOG(ERROR) << "Cannot find: " << OmittedString(kv.first, 10)
                     << " from";
          bpt_->Dump(txn, std::cerr);
        }
        ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
        ASSERT_EQ(kv.second, val);
      }
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    }
    txn.PreCommit();
  }
}

TEST_F(BPlusTreeTest, DeleteAllReverse) {
  constexpr int kCount = 100;
  constexpr int kKeyLength = 5000;
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < kCount; ++i) {
      std::string key = KeyGen(i, kKeyLength);
      std::string value = KeyGen(i, 200);
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
      kvp.emplace(key, value);
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kv.second, val);
    }
  }
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
}

TEST_F(BPlusTreeTest, Crash) {
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

  page_id_t max_page = p_->GetPage(0)->body.meta_page.MaxPageCountForTest();
  for (size_t i = 0; i < max_page; i += 2) {
    Flush(i);
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());
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
  constexpr int kKeyLength = 4000;
  lsn_t restart_point;
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
    }
    restart_point = cm_->WriteCheckpoint([&]() {
      for (int i = 10; i < 20; ++i) {
        ASSERT_SUCCESS(
            bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
      }
    });
    for (int i = 20; i < 30; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kKeyLength), KeyGen(i * 10, 1000)));
    }
    txn.PreCommit();
  }
  page_id_t max_page = p_->GetPage(0)->body.meta_page.MaxPageCountForTest();
  for (size_t i = 0; i < max_page; i += 5) {
    Flush(i);
  }
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());
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
  constexpr int kCount = 100;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10, false);
    std::string value = RandomString((19937 * i) % 120 + 10, false);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    bpt_->SanityCheckForTest(p_.get());
    keys.push_back(key);
    kvp.emplace(key, value);
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      ASSERT_EQ(kvp[kv.first], val);
    }
  }
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
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

TEST_F(BPlusTreeTest, InsertDelete) {
  constexpr int kCount = 50;
  Transaction txn = tm_->Begin();
  std::unordered_set<std::string> keys;
  keys.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, "foo"));
    keys.insert(key);
  }
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    auto it = keys.begin();
    std::advance(it, (i * 63) % keys.size());
    ASSERT_SUCCESS(bpt_->Delete(txn, *it));
    keys.erase(it);
    std::string inserting_key = RandomString((19937 * i) % 2000 + 2000, false);
    ASSERT_SUCCESS(bpt_->Insert(txn, inserting_key, "bar"));
    keys.insert(inserting_key);
    ASSERT_TRUE(bpt_->SanityCheckForTest(txn.PageManager()));
  }
}

TEST_F(BPlusTreeTest, InsertDeleteHeavy) {
  int kCount = 100;
  Transaction txn = tm_->Begin();
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);
    std::string value = RandomString((19937 * i) % 120 + 10, false);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
  LOG(INFO) << "initialized, insert and delete for " << (kCount * 40)
            << " times.";
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
  for (const auto& kv : kvp) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
    ASSERT_EQ(kvp[kv.first], val);
  }
}

}  // namespace tinylamb