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

#include <memory>
#include <string>
#include <unordered_map>

#include "b_plus_tree.hpp"
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

class BPlusTreeConcurrentTest : public ::testing::Test {
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

 protected:
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

constexpr int kThreads = 2;
constexpr int kSize = 2;

TEST_F(BPlusTreeConcurrentTest, InsertInsert) {
  std::vector<std::unordered_map<std::string, std::string>> rows;
  rows.resize(kThreads);
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i]() {
      std::mt19937 rand(i);
      auto txn = tm_->Begin();
      for (int j = 0; j < kSize; ++j) {
        std::string key(RandomString(rand() % 1000 + 100));
        std::string value(RandomString(rand() % 1000 + 100));
        Status s = bpt_->Insert(txn, key, value);
        if (s == Status::kDuplicates) {
          --j;
          continue;
        }
        rows[i].emplace(key, value);
      }
      ASSERT_SUCCESS(txn.PreCommit());
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  {
    auto txn = tm_->Begin();
    for (const auto& rows_per_thread : rows) {
      for (const auto& row : rows_per_thread) {
        ASSIGN_OR_ASSERT_FAIL(std::string_view, value,
                              bpt_->Read(txn, row.first));
        ASSERT_EQ(value, row.second);
      }
    }
    txn.PreCommit();
  }
}

}  // namespace tinylamb