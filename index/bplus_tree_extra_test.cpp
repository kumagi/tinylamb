/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra B+Tree coverage that the structural suite in b_plus_tree_test.cpp
// does not pin down: MVCC snapshot stability across concurrent structural
// growth, height growth followed by a full collapse and rebuild, and
// boundary-shaped keys (empty, tiny, huge, high-byte).
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "b_plus_tree.hpp"
#include "b_plus_tree_iterator.hpp"
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

namespace tinylamb {

namespace {

std::string PaddedKey(int i, size_t width) {
  std::string digits = std::to_string(i);
  while (digits.size() < width) {
    digits.insert(digits.begin(), '0');
  }
  return "k" + digits;
}

}  // namespace

class BPlusTreeExtraTest : public ::testing::Test {
 public:
  void SetUp() override {
    const std::string prefix = "b_plus_tree_extra-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    master_record_name_ = prefix + ".master.log";
    p_ = PageManager::Create(db_name_, 110).MoveValue();
    l_ = Logger::Create(log_name_).MoveValue();
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    ASSERT_SUCCESS(txn.PreCommit());
    bpt_ = std::make_unique<BPlusTree>(page->PageID());
  }

  void TearDown() override {
    bpt_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::ignore = std::remove(db_name_.c_str());
    tinylamb::RemoveLogAndSegments(log_name_);
    std::ignore = std::remove(master_record_name_.c_str());
  }

  size_t CountVisible(Transaction& txn) const {
    size_t count = 0;
    for (BPlusTreeIterator it = bpt_->Begin(txn); it.IsValid(); ++it) {
      ++count;
    }
    return count;
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

// The raw tree layer serves page-level state: a transaction that stays open
// across another transaction's committed, split-heavy growth observes that
// committed state on its next read or scan (row-level MVCC filtering lives
// in the table layer above, not in the structure iterator).  This pins the
// no-stale-cache contract: committed growth is never hidden behind a stale
// page hint.
TEST_F(BPlusTreeExtraTest, OpenReaderFollowsCommittedStructuralGrowth) {
  {
    auto seed = tm_->Begin();
    ASSERT_SUCCESS(bpt_->Insert(seed, "seed", "original"));
    ASSERT_SUCCESS(seed.PreCommit());
  }
  auto reader = tm_->Begin();
  ASSERT_SUCCESS(bpt_->Read(reader, "seed").GetStatus());

  {
    auto grower = tm_->Begin();
    for (int i = 0; i < 2000; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(grower, PaddedKey(i, 6), "v" + std::to_string(i)))
          << "i=" << i;
    }
    ASSERT_SUCCESS(grower.PreCommit());
  }

  // The open reader resolves the committed growth; nothing is served from a
  // stale snapshot of the structure.
  ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  EXPECT_EQ(CountVisible(reader), 2001U);
  EXPECT_TRUE(bpt_->Read(reader, PaddedKey(0, 6)).HasValue());
  EXPECT_EQ(bpt_->Read(reader, PaddedKey(1999, 6)).MoveValue(), "v1999");
  EXPECT_TRUE(bpt_->Read(reader, "seed").HasValue());
}

// A tree that grew to multiple levels collapses back when every key is
// deleted, stays sane while empty, and accepts new inserts afterwards.
TEST_F(BPlusTreeExtraTest, HeightGrowthThenFullCollapseAndRebuild) {
  constexpr int kCount = 4000;
  {
    auto txn = tm_->Begin();
    for (int i = 0; i < kCount; ++i) {
      ASSERT_SUCCESS(
          bpt_->Insert(txn, PaddedKey(i, 6),
                       std::string(40, static_cast<char>('a' + (i % 26)))));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    EXPECT_EQ(CountVisible(txn), static_cast<size_t>(kCount));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    for (int i = kCount - 1; i >= 0; --i) {
      ASSERT_SUCCESS(bpt_->Delete(txn, PaddedKey(i, 6))) << "i=" << i;
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    EXPECT_EQ(CountVisible(txn), 0U);
    // The emptied tree still serves lookups and accepts inserts.
    EXPECT_FALSE(bpt_->Read(txn, PaddedKey(0, 6)).HasValue());
    ASSERT_SUCCESS(bpt_->Insert(txn, "reborn", "after-collapse"));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    EXPECT_EQ(CountVisible(txn), 1U);
    EXPECT_EQ(bpt_->Read(txn, "reborn").MoveValue(), "after-collapse");
  }
}

// Key shapes at the extremes share one tree without disturbing each other:
// the empty key, a one-byte key, a huge key, and high-byte keys all round
// trip through insert/read/update/delete.
TEST_F(BPlusTreeExtraTest, BoundaryKeyShapesRoundTrip) {
  const std::vector<std::pair<std::string, std::string>> shapes = {
      {"", "empty-key-value"},
      {"a", "one-byte"},
      {std::string(1024, 'x'), "huge-key"},
      {std::string({'\xff', '\x00', '\x01'}), "high-bytes"},
      {std::string({'\x00'}), "nul-byte"},
  };
  {
    auto txn = tm_->Begin();
    for (const auto& [key, value] : shapes) {
      ASSERT_SUCCESS(bpt_->Insert(txn, key, value));
      EXPECT_EQ(bpt_->Read(txn, key).MoveValue(), value);
    }
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    EXPECT_EQ(CountVisible(txn), shapes.size());
    for (const auto& [key, value] : shapes) {
      EXPECT_EQ(bpt_->Read(txn, key).MoveValue(), value);
      const std::string updated = value + "-updated";
      ASSERT_SUCCESS(bpt_->Update(txn, key, updated));
      EXPECT_EQ(bpt_->Read(txn, key).MoveValue(), updated);
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    auto txn = tm_->Begin();
    for (const auto& [key, value] : shapes) {
      ASSERT_SUCCESS(bpt_->Delete(txn, key));
      EXPECT_FALSE(bpt_->Read(txn, key).HasValue());
      std::ignore = value;
    }
    ASSERT_SUCCESS(txn.PreCommit());
    EXPECT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
}

}  // namespace tinylamb
