//
// Created by kumagi on 2022/02/02.
//

#include "b_plus_tree.hpp"

#include <memory>
#include <string>

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
 protected:
  static constexpr char kDBFileName[] = "b_plus_tree_test.db";
  static constexpr char kLogName[] = "b_plus_tree_test.log";
  static constexpr char kMasterRecordName[] = "b_plus_tree_master.log";

 public:
  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page_ = p_->AllocateNewPage(txn, PageType::kLeafPage);
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Flush(page_id_t pid) { p_->GetPool()->FlushPageForTest(pid); }

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
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
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
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

 public:
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
  ASSERT_SUCCESS(bpt_->Insert(txn, "key", "blar"));
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
}

TEST_F(BPlusTreeTest, Search) {
  {
    auto txn = tm_->Begin();
    std::string key_prefix("key");
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(bpt_->Insert(txn, KeyGen(i, 10000), KeyGen(i * 10, 2000)));
    }
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
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
  for (int i = 0; i < max_page; i += 2) {
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
  for (int i = 0; i < max_page; i += 5) {
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
}  // namespace tinylamb