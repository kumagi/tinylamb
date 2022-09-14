//
// Created by kumagi on 2022/02/02.
//

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
    PageRef page_ = p_->AllocateNewPage(txn, PageType::kLeafPage);
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
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf) {
  constexpr static int kKeys = 100;
  auto txn = tm_->Begin();
  std::string key_prefix("key");
  std::string long_value(5000, 'v');
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, key_prefix + std::to_string(i), long_value));
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, key_prefix + std::to_string(i)),
                          long_value);
  }
  ASSERT_SUCCESS(txn.PreCommit());
}

std::string KeyGen(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(BPlusTreeTest, SplitBranch) {
  constexpr static int kKeys = 20;
  auto txn = tm_->Begin();
  std::string value = "v";
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 5000), value));
  }
  bpt_->Dump(txn, std::cerr);
  std::cerr << "\n\n";
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(i, 5000)), value);
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_SUCCESS_AND_EQ(bpt_->Read(txn, KeyGen(i, 5000)), value);
  }
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, MergeBranch) {
  constexpr static size_t kPayloadSize = 5000;
  constexpr static size_t kInserts = 50;
  auto txn = tm_->Begin();
  std::string short_value = "v";
  for (size_t i = 0; i < kInserts; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, kPayloadSize), short_value));
  }
  bpt_->Dump(txn, std::cerr);
  std::cerr << "\n";
  for (size_t i = 0; i < kInserts; ++i) {
    std::string key = KeyGen(i, kPayloadSize);
    // SCOPED_TRACE(key);
    ASSERT_SUCCESS(bpt_->Delete(txn, key));
    std::cerr << "deleted: " << i << "\n";
    bpt_->Dump(txn, std::cerr);
    for (size_t j = i + 1; j < kInserts; ++j) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(j, kPayloadSize)));
      ASSERT_EQ(val, short_value);
    }
  }
  bpt_->Dump(txn, std::cerr);
}

TEST_F(BPlusTreeTest, FullScanMultiLeafReverse) {
  auto txn = tm_->Begin();
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, std::string(5000, c), std::string(5000, c)));
  }
}

TEST_F(BPlusTreeTest, FullScanMultiLeafMany) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f'}) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, std::string(5000, c), std::string(5000, c)));
  }
  bpt_->Dump(txn, std::cerr);
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
    bpt_->Dump(txn, std::cerr);
    std::string long_value(2000, 'v');
    for (int i = 0; i < 100; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val,
                            bpt_->Read(txn, KeyGen(i, kPayloadSize)));
      ASSERT_EQ(val, KeyGen(i * 10, 200));
    }
  }
}

TEST_F(BPlusTreeTest, Update) {
  constexpr static size_t kPayloadSize = 5000;
  constexpr static size_t kCount = 200;
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < kCount; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, KeyGen(i, kPayloadSize), KeyGen(i * 10, 100)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    for (int i = 0; i < kCount; i += 2) {
      ASSERT_SUCCESS(
          bpt_->Update(txn, KeyGen(i, kPayloadSize), KeyGen(i * 2, 200)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
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

TEST_F(BPlusTreeTest, DeleteAll) {
  constexpr int kCount = 100;
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
      std::cerr << "delete: " << i << "\n";
      ASSERT_SUCCESS(bpt_->Delete(txn, key));
      kvp.erase(key);
      bpt_->Dump(txn, std::cerr, 0);
      std::cerr << "\n";
      for (const auto& kv : kvp) {
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
      assert(kvp[kv.first] == val);
    }
  }
  for (int i = 0; i < kCount * 4; ++i) {
    const std::string& key = keys[(i * 63) % keys.size()];
    std::string value = RandomString((19937 * i) % 320 + 500, false);
    LOG(TRACE) << "Update: " << key;
    ASSERT_SUCCESS(bpt_->Update(txn, key, value));
    kvp[key] = value;
    bpt_->Dump(txn, std::cerr, 0);
    std::cerr << "\n";
    for (const auto& kv : kvp) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, val, bpt_->Read(txn, kv.first));
      if (kvp[kv.first] != val) {
        LOG(ERROR) << kv.first;
        LOG(ERROR) << kvp[kv.first] << " vs " << val;
      }
      assert(kvp[kv.first] == val);
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
    Row r({Value(i), Value(RandomString((19937 * i) % 120 + 10, false))});
    std::string key = r.EncodeMemcomparableFormat();
    ASSERT_SUCCESS(bpt_->Insert(txn, key, "foo"));
    keys.insert(key);
  }
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    auto it = keys.begin();
    std::advance(it, (i * 63) % keys.size());
    bpt_->Dump(txn, std::cerr, 0);
    std::cerr << "\n";
    LOG(INFO) << "delete: " << OmittedString(*it, 20);
    ASSERT_SUCCESS(bpt_->Delete(txn, *it));
    keys.erase(it);
    Row r({Value(i), Value(RandomString((19937 * i) % 2200 + 3000, false))});
    std::string inserting_key = r.EncodeMemcomparableFormat();
    bpt_->Insert(txn, inserting_key, "bar");
    keys.insert(inserting_key);
  }
}

TEST_F(BPlusTreeTest, InsertDeleteHeavy) {
  constexpr int kCount = 100;
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
  for (int i = 0; i < kCount * 4; ++i) {
    auto iter = kvp.begin();
    std::advance(iter, (i * 19937) % kvp.size());
    ASSERT_SUCCESS(bpt_->Delete(txn, iter->first));
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    kvp.erase(iter);

    std::string key = RandomString((19937 * i) % 130 + 1000, false);
    std::string value = RandomString((19937 * i) % 320 + 5000, false);
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