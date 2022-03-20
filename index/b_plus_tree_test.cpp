//
// Created by kumagi on 2022/02/02.
//

#include "b_plus_tree.hpp"

#include <memory>
#include <string>

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
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
    bpt_ = std::make_unique<BPlusTree>(root, p_.get());
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
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf) {
  auto txn = tm_->Begin();
  std::string key_prefix("key");
  std::string long_value(8000, 'v');
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, key_prefix + std::to_string(i), long_value));
  }
}

std::string KeyGen(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(BPlusTreeTest, SplitInternal) {
  auto txn = tm_->Begin();
  std::string key_prefix("key");
  std::string long_value(2000, 'v');
  for (int i = 0; i < 6; ++i) {
    ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), long_value));
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
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 2000)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    bpt_->Dump(txn, std::cerr);
    std::string long_value(2000, 'v');
    std::string_view val;
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(bpt_->Read(txn, KeyGen(i, 10000), &val));
      ASSERT_EQ(val, KeyGen(i * 10, 2000));
    }
  }
}

TEST_F(BPlusTreeTest, Update) {
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < 50; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 1000)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    for (int i = 0; i < 50; i += 2) {
      ASSERT_SUCCESS(bpt_->Update(txn, KeyGen(i, 10000), KeyGen(i * 2, 100)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    std::string_view val;
    for (int i = 0; i < 50; ++i) {
      ASSERT_SUCCESS(bpt_->Read(txn, KeyGen(i, 10000), &val));
      if (i % 2 == 0) {
        ASSERT_EQ(val, KeyGen(i * 2, 100));
      } else {
        ASSERT_EQ(val, KeyGen(i * 10, 1000));
      }
    }
  }
}

TEST_F(BPlusTreeTest, Delete) {
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < 50; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 1000)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    for (int i = 0; i < 50; i += 2) {
      ASSERT_SUCCESS(bpt_->Delete(txn, KeyGen(i, 10000)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    std::string long_value(2000, 'v');
    std::string_view val;
    for (int i = 0; i < 50; ++i) {
      if (i % 2 == 0) {
        ASSERT_FAIL(bpt_->Read(txn, KeyGen(i, 10000), &val));
        ASSERT_EQ(val, "");
      } else {
        ASSERT_SUCCESS(bpt_->Read(txn, KeyGen(i, 10000), &val));
        ASSERT_EQ(val, KeyGen(i * 10, 1000));
      }
    }
  }
}

TEST_F(BPlusTreeTest, Crash) {
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 1000)));
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
    std::string_view val;
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(bpt_->Read(txn, KeyGen(i, 10000), &val));
      ASSERT_EQ(val, KeyGen(i * 10, 1000));
    }
  }
}

TEST_F(BPlusTreeTest, CheckPoint) {
  lsn_t restart_point;
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 1000)));
    }
    restart_point = cm_->WriteCheckpoint([&]() {
      for (int i = 10; i < 20; ++i) {
        ASSERT_SUCCESS(
            bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 1000)));
      }
    });
    for (int i = 20; i < 30; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 1000)));
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
    std::string_view val;
    for (int i = 0; i < 30; ++i) {
      ASSERT_SUCCESS(bpt_->Read(txn, KeyGen(i, 10000), &val));
      ASSERT_EQ(val, KeyGen(i * 10, 1000));
    }
  }
}

TEST_F(BPlusTreeTest, UpdateHeavy) {
  constexpr int kCount = 50;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> kvp;
  keys.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10);
    std::string value = RandomString((19937 * i) % 120 + 10);
    ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }
  for (int i = 0; i < kCount; ++i) {
    const std::string& key = keys[(i * 63) % keys.size()];
    std::string value = RandomString((19937 * i) % 320 + 500);
    ASSERT_SUCCESS(bpt_->Update(txn, key, value));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    std::string_view val;
    bpt_->Read(txn, kv.first, &val);
    ASSERT_EQ(kvp[kv.first], val);
  }
}

}  // namespace tinylamb