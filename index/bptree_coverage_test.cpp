/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "index/b_plus_tree.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "index/lsm_tree.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

namespace {
// One exclusive-size key+value per leaf page: the proven split topology used
// by the sibling iterator tests.
constexpr int kSize = 2723;
}  // namespace

class BptreeCoverageTest : public ::testing::Test {
 public:
  void SetUp() override {
    const std::string prefix = "bptree_coverage_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    ASSERT_EQ(page->PageID(), 1);
    EXPECT_SUCCESS(txn.PreCommit());
  }

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
    p_ = PageManager::Create(db_name_, 10).MoveValue();
    l_ = Logger::Create(log_name_).MoveValue();
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), r_.get());
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
    tinylamb::RemoveLogAndSegments(log_name_);
  }

  void Insert(Transaction& txn, char c) const {
    EXPECT_SUCCESS(
        bpt_->Insert(txn, std::string(kSize, c), std::string(kSize, c)));
  }

  // Collects the leading characters of a walk driven by step().
  template <typename Step>
  [[nodiscard]] [[nodiscard]] [[nodiscard]] [[nodiscard]] [[nodiscard]]
  [[nodiscard]] [[nodiscard]] [[nodiscard]] [[nodiscard]] [[nodiscard]]
  [[nodiscard]] [[nodiscard]] [[nodiscard]] [[nodiscard]] std::vector<char>
  Walk(BPlusTreeIterator it, Step step) const {
    std::vector<char> keys;
    while (it.IsValid()) {
      const std::string k = it.Key();
      if (k.empty()) {
        ADD_FAILURE() << "iterator produced an empty key";
        return keys;
      }
      keys.push_back(k[0]);
      step(it);
    }
    return keys;
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<BPlusTree> bpt_;
};

// ----------------------------------------------------- leftmost walking ---

// An empty leftmost leaf with a finite high fence must forward the scan past
// its fence instead of reporting an empty tree.
TEST_F(BptreeCoverageTest, ScanCrossesEmptyLeftmostLeafByHighFence) {
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root()).MoveValue();
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef empty = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    COERCE(empty->SetHighFence(txn, IndexKey("m")));
    root->SetLowestValue(txn, empty->PageID());
    PageRef full = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    full->InsertLeaf(txn, "z", "1");
    COERCE(full->SetLowFence(txn, IndexKey("m")));
    root->InsertBranch(txn, "m", full->PageID());
    ASSERT_SUCCESS(txn.PreCommit());
  }
  auto txn = tm_->Begin();
  BPlusTreeIterator it = bpt_->Begin(txn);
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), "z");
  ++it;
  EXPECT_FALSE(it.IsValid());

  // The same walk with an end bound below the surviving key stays empty.
  BPlusTreeIterator bounded = bpt_->Begin(txn, "", "y");
  EXPECT_FALSE(bounded.IsValid());
}

// An empty leftmost leaf whose foster link holds the rows must be crossed
// through the foster chain, in both scan directions.
TEST_F(BptreeCoverageTest, ScanCrossesEmptyLeftmostLeafByFoster) {
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root()).MoveValue();
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef empty = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    COERCE(empty->SetLowFence(txn, IndexKey("m")));
    root->SetLowestValue(txn, empty->PageID());
    PageRef full = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    full->InsertLeaf(txn, "z", "1");
    COERCE(full->SetLowFence(txn, IndexKey("m")));
    root->InsertBranch(txn, "m", full->PageID());
    ASSERT_SUCCESS(empty->SetFoster(txn, FosterPair("z", full->PageID())));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  auto txn = tm_->Begin();
  BPlusTreeIterator it = bpt_->Begin(txn);
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), "z");
  ++it;
  EXPECT_FALSE(it.IsValid());

  BPlusTreeIterator desc = bpt_->Begin(txn, "", "", false);
  ASSERT_TRUE(desc.IsValid());
  EXPECT_EQ(desc.Key(), "z");
  --desc;
  EXPECT_FALSE(desc.IsValid());
}

// A prefix seek that exhausts its landing leaf must continue into the next
// leaf instead of reporting an empty range.
TEST_F(BptreeCoverageTest, PrefixSeekExhaustingLandingLeafContinues) {
  auto txn = tm_->Begin();
  for (const char c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c);
  }
  ASSERT_SUCCESS(txn.PreCommit());

  auto scan_txn = tm_->Begin();
  // "f~" sorts between the stored f-key and the foster-held g-key.
  const std::string seek = std::string("f") + static_cast<char>(0x7f);
  BPlusTreeIterator it = bpt_->Begin(scan_txn, seek, "");
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), std::string(kSize, 'g'));
  ++it;
  EXPECT_FALSE(it.IsValid());
}

// ---------------------------------------------- leaves emptied by deletes ---

// Deleting a contiguous key band leaves empty leaves mid-tree; scans in both
// directions must skip the empty pages without dropping survivors.
TEST_F(BptreeCoverageTest, ScansSkipLeavesEmptiedByDeletes) {
  auto txn = tm_->Begin();
  for (const char c : {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}) {
    Insert(txn, c);
  }
  for (const char c : {'d', 'e', 'f', 'g'}) {
    EXPECT_SUCCESS(bpt_->Delete(txn, std::string(kSize, c)));
  }
  ASSERT_SUCCESS(txn.PreCommit());

  auto scan_txn = tm_->Begin();
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn), [](BPlusTreeIterator& i) { ++i; })),
            (std::vector<char>{'a', 'b', 'c', 'h', 'i', 'j'}));
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn, "", "", false),
                  [](BPlusTreeIterator& i) { --i; })),
            (std::vector<char>{'j', 'i', 'h', 'c', 'b', 'a'}));

  // Bounded scans over the gap in both directions.
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn, std::string(kSize, 'c'),
                              std::string(kSize, 'i')),
                  [](BPlusTreeIterator& i) { ++i; })),
            (std::vector<char>{'c', 'h', 'i'}));
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn, std::string(kSize, 'c'),
                              std::string(kSize, 'h'), false),
                  [](BPlusTreeIterator& i) { --i; })),
            (std::vector<char>{'h', 'c'}));
}

// Deleting every key leaves a tree of empty leaves; scans must exhaust
// immediately rather than surfacing stale keys.
TEST_F(BptreeCoverageTest, ScansOnAllEmptyTreeExhaustImmediately) {
  auto txn = tm_->Begin();
  for (const char c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c);
  }
  for (const char c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    EXPECT_SUCCESS(bpt_->Delete(txn, std::string(kSize, c)));
  }
  ASSERT_SUCCESS(txn.PreCommit());

  auto scan_txn = tm_->Begin();
  EXPECT_FALSE(bpt_->Begin(scan_txn).IsValid());
  EXPECT_FALSE(bpt_->Begin(scan_txn, "", "", false).IsValid());
  EXPECT_FALSE(bpt_->Begin(scan_txn, std::string(kSize, 'a'), "").IsValid());
  EXPECT_FALSE(
      bpt_->Begin(scan_txn, "", std::string(kSize, 'z'), false).IsValid());
}

// ---------------------------------------------------- bounded descending ---

// A descending scan bounded on both sides yields exactly the in-range keys.
TEST_F(BptreeCoverageTest, DescendingScanHonorsBothBounds) {
  auto txn = tm_->Begin();
  for (const char c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    EXPECT_SUCCESS(bpt_->Insert(txn, std::string(1, c), std::string(1, c)));
  }
  ASSERT_SUCCESS(txn.PreCommit());

  auto scan_txn = tm_->Begin();
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn, "c", "f", false),
                  [](BPlusTreeIterator& i) { --i; })),
            (std::vector<char>{'f', 'e', 'd', 'c'}));
}

// -- from the first key of a middle leaf crosses the low fence into the
// predecessor leaf.
TEST_F(BptreeCoverageTest, BackwardFromFirstKeyCrossesLowFence) {
  auto txn = tm_->Begin();
  for (const char c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c);
  }
  ASSERT_SUCCESS(txn.PreCommit());

  auto scan_txn = tm_->Begin();
  // The ascending seek positions on 'd'; one step back leaves [d, inf).
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn, std::string(kSize, 'd'), ""),
                  [](BPlusTreeIterator& i) { --i; })),
            (std::vector<char>{'d'}));

  // An open lower bound lets the backward walk cross the low fence into the
  // predecessor leaf (the exclusive-seek / chain-head probe path).
  EXPECT_EQ((Walk(bpt_->Begin(scan_txn, std::string(kSize, 'a'),
                              std::string(kSize, 'g'), false),
                  [](BPlusTreeIterator& i) { --i; })),
            (std::vector<char>{'g', 'f', 'e', 'd', 'c', 'b', 'a'}));
}

// ---------------------------------------------------- LSM restore paths ---

class LsmRestoreCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("lsm_restore_cov-" + RandomString());
  }
  void TearDown() override {
    tree_.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  std::filesystem::path dir_;
  std::unique_ptr<LSMTree> tree_;
};

TEST_F(LsmRestoreCoverageTest, OpenCreatesDirectoryAndBlob) {
  auto tree = LSMTree::Create(dir_);
  ASSERT_TRUE(tree.HasValue());
  EXPECT_SUCCESS(tree.MoveValue()->Write("a", "1", true));
}

TEST_F(LsmRestoreCoverageTest, RestoreKeepsFlushedRunsReadable) {
  {
    auto created = LSMTree::Create(dir_);
    ASSERT_TRUE(created.HasValue());
    tree_ = created.MoveValue();
    EXPECT_SUCCESS(tree_->Write("k1", "v1", true));
    EXPECT_SUCCESS(tree_->Sync());
  }
  auto reopened = LSMTree::Create(dir_);
  ASSERT_TRUE(reopened.HasValue());
  tree_ = reopened.MoveValue();
  auto value = tree_->Read("k1");
  ASSERT_TRUE(value.HasValue());
  EXPECT_EQ(value.MoveValue(), "v1");
}

TEST_F(LsmRestoreCoverageTest, RestoreQuarantinesCorruptRunFile) {
  {
    auto created = LSMTree::Create(dir_);
    ASSERT_TRUE(created.HasValue());
    created.MoveValue()->Write("a", "1", true);
  }
  // A run file whose payload is garbage cannot be restored and is renamed
  // next to the original with a .bad suffix.
  const std::filesystem::path bogus = dir_ / "123-999";
  {
    std::ofstream f(bogus, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(f.good());
    f << "this is not a sorted run";
  }
  auto reopened = LSMTree::Create(dir_);
  ASSERT_TRUE(reopened.HasValue());
  tree_ = reopened.MoveValue();
  EXPECT_FALSE(std::filesystem::exists(bogus));
  EXPECT_TRUE(std::filesystem::exists(bogus.string() + ".bad"));
}

TEST_F(LsmRestoreCoverageTest, RestoreIgnoresUnparseableRunName) {
  {
    auto created = LSMTree::Create(dir_);
    ASSERT_TRUE(created.HasValue());
    created.MoveValue()->Write("a", "1", true);
  }
  const std::filesystem::path weird = dir_ / "merged-not-a-run-name";
  {
    std::ofstream f(weird, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(f.good());
    f << "junk";
  }
  auto reopened = LSMTree::Create(dir_);
  ASSERT_TRUE(reopened.HasValue());
  tree_ = reopened.MoveValue();
  // Unparseable names are rejected outright: the file is removed.
  EXPECT_FALSE(std::filesystem::exists(weird));
  auto value = tree_->Read("a");
  ASSERT_TRUE(value.HasValue());
  EXPECT_EQ(value.MoveValue(), "1");
}

TEST_F(LsmRestoreCoverageTest, RestoreQuarantinesDuplicateGeneration) {
  std::filesystem::path run_file;
  {
    auto created = LSMTree::Create(dir_);
    ASSERT_TRUE(created.HasValue());
    tree_ = created.MoveValue();
    EXPECT_SUCCESS(tree_->Write("dup", "1", true));
    EXPECT_SUCCESS(tree_->Sync());
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      const std::string name = entry.path().filename().string();
      const bool run_like =
          !name.empty() &&
          std::all_of(name.begin(), name.end(),
                      [](char c) {
                        return std::isdigit(static_cast<unsigned char>(c)) !=
                                   0 ||
                               c == '-';
                      }) &&
          name.find('-') != std::string::npos;
      if (run_like) {
        run_file = entry.path();
        break;
      }
    }
    ASSERT_FALSE(run_file.empty());
    tree_.reset();
  }
  // A copy of a valid run under a different name repeats its generation.
  std::error_code ec;
  std::filesystem::copy_file(run_file, dir_ / "4242-0", ec);
  ASSERT_FALSE(ec) << ec.message();
  auto reopened = LSMTree::Create(dir_);
  ASSERT_TRUE(reopened.HasValue());
  tree_ = reopened.MoveValue();
  const std::filesystem::path duplicate = dir_ / "4242-0";
  const bool original_quarantined =
      std::filesystem::exists(run_file.string() + ".bad");
  const bool duplicate_quarantined =
      std::filesystem::exists(duplicate.string() + ".bad");
  EXPECT_TRUE(original_quarantined != duplicate_quarantined);
  EXPECT_EQ(std::filesystem::exists(run_file), !original_quarantined);
  EXPECT_EQ(std::filesystem::exists(duplicate), !duplicate_quarantined);
  auto value = tree_->Read("dup");
  ASSERT_TRUE(value.HasValue());
  EXPECT_EQ(value.MoveValue(), "1");
}

}  // namespace tinylamb
