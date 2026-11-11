/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "index/lsm_index.hpp"

#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

class LsmIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "lsm_index_test-" + RandomString();
    Reopen();
  }

  void Reopen() {
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    pm_ = PageManager::Create(prefix_ + ".db", 128, false).MoveValue();
    logger_ = Logger::Create(prefix_ + ".log").MoveValue();
    rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                            logger_->SegmentSize());
    rm_->SetLogger(logger_.get());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), logger_.get(),
                                               rm_.get());
    ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));
  }

  void TearDown() override {
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    std::ignore = std::remove((prefix_ + ".db").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt.tmp").c_str());
    RemoveLogAndSegments(prefix_ + ".log");
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(LsmIndexTest, InsertReadUpdateDeleteAndRangeScan) {
  page_id_t root = 0;
  {
    Transaction txn = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(txn));
    root = idx.Root();
    EXPECT_GT(root, 0U);
    ASSERT_SUCCESS(idx.Insert(txn, "b", "val_b"));
    ASSERT_SUCCESS(idx.Insert(txn, "a", "val_a"));
    ASSERT_SUCCESS(idx.Insert(txn, "c", "val_c"));
    EXPECT_EQ(idx.Insert(txn, "b", "dup"), Status::kDuplicates);

    ASSIGN_OR_ASSERT_FAIL(std::string, v, idx.Read(txn, "b"));
    EXPECT_EQ(v, "val_b");
    ASSERT_SUCCESS(idx.Update(txn, "b", "val_b2"));
    ASSIGN_OR_ASSERT_FAIL(std::string, v2, idx.Read(txn, "b"));
    EXPECT_EQ(v2, "val_b2");

    std::vector<std::string> asc;
    for (LsmIndexIterator it = idx.Begin(txn, "", "", true); it.IsValid();
         ++it) {
      asc.push_back(it.Key() + "=" + it.Value());
    }
    EXPECT_EQ(asc,
              (std::vector<std::string>{"a=val_a", "b=val_b2", "c=val_c"}));

    std::vector<std::string> desc;
    for (LsmIndexIterator it = idx.Begin(txn, "", "", false); it.IsValid();
         --it) {
      desc.push_back(it.Key() + "=" + it.Value());
    }
    EXPECT_EQ(desc,
              (std::vector<std::string>{"c=val_c", "b=val_b2", "a=val_a"}));

    ASSERT_SUCCESS(idx.Delete(txn, "b"));
    EXPECT_EQ(idx.Read(txn, "b").GetStatus(), Status::kNotExists);
    ASSERT_SUCCESS(txn.PreCommit());
  }

  Reopen();
  {
    Transaction txn = tm_->Begin();
    LsmIndex idx(root);
    ASSIGN_OR_ASSERT_FAIL(std::string, va, idx.Read(txn, "a"));
    EXPECT_EQ(va, "val_a");
    EXPECT_EQ(idx.Read(txn, "b").GetStatus(), Status::kNotExists);
    ASSIGN_OR_ASSERT_FAIL(std::string, vc, idx.Read(txn, "c"));
    EXPECT_EQ(vc, "val_c");
    ASSERT_SUCCESS(txn.PreCommit());
  }
}

TEST_F(LsmIndexTest, UnboundedKeyAndValueSizes) {
  Transaction txn = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(txn));

  // 64 KiB keys and 256 KiB values -- far beyond any fixed-page B+Tree limit.
  const std::string huge_key_1 = std::string(65536, 'k') + "_1";
  const std::string huge_key_2 = std::string(65536, 'k') + "_2";
  const std::string huge_val_1(262144, 'v');
  const std::string huge_val_2(262144, 'w');

  ASSERT_SUCCESS(idx.Insert(txn, huge_key_2, huge_val_2));
  ASSERT_SUCCESS(idx.Insert(txn, huge_key_1, huge_val_1));

  ASSIGN_OR_ASSERT_FAIL(std::string, r1, idx.Read(txn, huge_key_1));
  EXPECT_EQ(r1, huge_val_1);
  ASSIGN_OR_ASSERT_FAIL(std::string, r2, idx.Read(txn, huge_key_2));
  EXPECT_EQ(r2, huge_val_2);

  LsmIndexIterator it = idx.Begin(txn);
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), huge_key_1);
  EXPECT_EQ(it.Value(), huge_val_1);
  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), huge_key_2);
  EXPECT_EQ(it.Value(), huge_val_2);
  ++it;
  EXPECT_FALSE(it.IsValid());

  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(LsmIndexTest, MemTableFlushAndCompactionWithPartitionBoundaries) {
  Transaction txn = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(txn));

  // Insert 5000 keys to trigger MemTable flushes into immutable sorted runs.
  for (int i = 0; i < 5000; ++i) {
    std::array<char, 16> buf{};
    ASSERT_GT(std::snprintf(buf.data(), buf.size(), "key_%05d", i), 0);
    ASSERT_SUCCESS(idx.Insert(txn, buf.data(), "val_" + std::to_string(i)));
  }

  ASSIGN_OR_ASSERT_FAIL(std::vector<std::string>, boundaries,
                        idx.ScanPartitionBoundaries(txn, "", "", 1));
  EXPECT_GE(boundaries.size(), 4U);
  for (size_t i = 1; i < boundaries.size(); ++i) {
    EXPECT_LT(boundaries[i - 1], boundaries[i]);
  }

  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(LsmIndexTest, AbortRevertsUncommittedMutations) {
  page_id_t root = 0;
  {
    Transaction txn = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(txn));
    root = idx.Root();
    ASSERT_SUCCESS(idx.Insert(txn, "keep", "v1"));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    Transaction txn = tm_->Begin();
    LsmIndex idx(root);
    ASSERT_SUCCESS(idx.Update(txn, "keep", "v2"));
    ASSERT_SUCCESS(idx.Insert(txn, "temp", "v_temp"));
    ASSERT_SUCCESS(txn.Abort());
  }
  {
    Transaction txn = tm_->Begin();
    LsmIndex idx(root);
    ASSIGN_OR_ASSERT_FAIL(std::string, v, idx.Read(txn, "keep"));
    EXPECT_EQ(v, "v1");
    EXPECT_EQ(idx.Read(txn, "temp").GetStatus(), Status::kNotExists);
    std::ostringstream oss;
    idx.Dump(oss, 2);
    EXPECT_NE(oss.str().find("LsmIndex"), std::string::npos);
    EXPECT_TRUE(LsmIndex::CheckSanity(txn));
    ASSERT_SUCCESS(txn.PreCommit());
  }
}

TEST_F(LsmIndexTest, AuditForeignUpdateCannotReplaceCommittedRangeValue) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(LsmIndex, index, LsmIndex::Open(setup));
  ASSERT_SUCCESS(index.Insert(setup, "key", "committed"));
  ASSERT_SUCCESS(setup.PreCommit());
  auto writer = tm_->Begin();
  ASSERT_SUCCESS(index.Update(writer, "key", "pending"));
  auto reader = tm_->Begin();
  auto scan = index.Begin(reader, "key", "key");
  ASSERT_TRUE(scan.IsValid());
  scan.EnableStableLeafCache();
  EXPECT_EQ(scan.Value(), "committed");
  auto own = index.Begin(writer, "key", "key");
  own.EnableStableLeafCache();
  EXPECT_EQ(own.Value(), "pending");
  ASSERT_SUCCESS(writer.Abort());
  ASSERT_SUCCESS(reader.PreCommit());
}

}  // namespace
}  // namespace tinylamb
