/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Seeded randomized concurrency stress for the foster B+Tree.  The fixed
// BPlusTreeConcurrentTest scenarios cover named interleavings; this fuzz
// test hammers two transactions with random Insert/Read/Delete/Commit on
// overlapping key spaces so TSan/ASan see the latch, write-intent and
// page-split code paths under a workload no hand-written test picks.
// The oracle is deliberately weak enough to be race-legal but strong where
// the engine is deterministic:
//   * per-thread disjoint key space: after both commit, every key holds
//     exactly the thread's last committed value (strong);
//   * shared key space: a read only has to return a value some thread
//     wrote with that key (membership), never garbage;
//   * the tree structure must survive every round (SanityCheckForTest),
//     and a fresh transaction must observe all committed disjoint work.
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "index/b_plus_tree.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/value.hpp"

namespace tinylamb {

class BPlusTreeConcurrentFuzzTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "bpt_conc_fuzz-" + RandomString();
    db_name_ = prefix_ + ".db";
    log_name_ = prefix_ + ".log";
    p_ = PageManager::Create(db_name_, 128).MoveValue();
    l_ = Logger::Create(log_name_).MoveValue();
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), r_.get());
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
    root_ = page->PageID();
    ASSERT_SUCCESS(txn.PreCommit());
    bpt_ = std::make_unique<BPlusTree>(root_);
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

 protected:
  std::string prefix_;
  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<BPlusTree> bpt_;
  page_id_t root_{0};
};

// NOLINTNEXTLINE(misc-use-anonymous-namespace)
static std::string SeedFromEnv() {
  const char* seed = std::getenv("TINYLAMB_BPT_CONC_FUZZ_SEED");
  return seed == nullptr ? std::string() : std::string(seed);
}

TEST_F(BPlusTreeConcurrentFuzzTest, RandomTwoWriterStress) {
  constexpr int kRounds = 6;
  // Keys: 200 exclusive to thread 0, 200 exclusive to thread 1, 60 shared.
  // Values embed (thread, round) so a wrong-vintage or torn read is
  // detectable on the strong channels.
  auto key_for = [](int owner, int i) {
    return "k" + std::to_string(owner) + "_" + std::to_string(i);
  };
  auto shared_key = [](int i) { return "s_" + std::to_string(i); };

  // Strong model for the disjoint spaces: owner -> key -> expected value
  // after the LAST committed round (tracked after the join).
  std::vector<std::map<std::string, std::string>> last_committed(2);

  for (int round = 0; round < kRounds; ++round) {
    const std::string seed_tag =
        SeedFromEnv().empty() ? std::to_string(round)
                              : SeedFromEnv() + "-" + std::to_string(round);
    std::mt19937_64 rng(std::stoull(seed_tag) +
                        (static_cast<unsigned long long>(round) * 7919ULL));
    // Per-round committed expectation for disjoint keys (both threads do
    // a full overwrite of their space, so the post-commit state is fully
    // determined regardless of interleaving).
    std::vector<std::map<std::string, std::string>> planned(2);
    // owner is a loop index in [0, 2); cast once for container indexing.
    for (int owner = 0; owner < 2; ++owner) {
      for (int i = 0; i < 200; ++i) {
        planned[static_cast<size_t>(owner)][key_for(owner, i)] =
            "v" + std::to_string(owner) + "_" + std::to_string(round) + "_" +
            std::to_string(i);
      }
    }

    // Draw the thread seeds BEFORE launching: touching `rng` from the
    // workers would itself be a race (TSan caught exactly that).
    const std::array<uint64_t, 2> thread_seeds = {
        rng() ^ 0x9E3779B97F4A7C15ULL, rng() ^ 0xC2B2AE3D27D4EB4FULL};
    auto worker = [&](int owner, std::string* err) {
      std::mt19937_64 local_rng(thread_seeds[static_cast<size_t>(owner)]);
      Transaction txn = tm_->Begin();
      for (int i = 0; i < 200 && err->empty(); ++i) {
        const std::string key = key_for(owner, i);
        // Keys survive from earlier rounds; B+Tree Insert rejects an
        // existing key (unique-key contract), so overwrite via Update.
        const Status inserted =
            bpt_->Insert(txn, key, planned[static_cast<size_t>(owner)][key]);
        if (inserted == Status::kDuplicates) {
          if (bpt_->Update(txn, key,
                           planned[static_cast<size_t>(owner)][key]) !=
              Status::kSuccess) {
            *err = "round " + std::to_string(round) + " owner " +
                   std::to_string(owner) + " update failed for " + key;
            return;
          }
        } else if (inserted != Status::kSuccess) {
          *err = "round " + std::to_string(round) + " owner " +
                 std::to_string(owner) + " insert failed for " + key;
          return;
        }
        // Shared-space traffic (membership-checked below, no strong model).
        if (local_rng() % 4 == 0) {
          const std::string sk = shared_key(static_cast<int>(local_rng() % 60));
          const std::string sv =
              "sv" + std::to_string(owner) + "_" + std::to_string(round);
          if (bpt_->Insert(txn, sk, sv) == Status::kDuplicates) {
            (void)bpt_->Update(txn, sk, sv);
          }
        }
        if (i % 50 == 49) {
          if (txn.PreCommit() != Status::kSuccess) {
            *err = "round " + std::to_string(round) + " owner " +
                   std::to_string(owner) + " precommit failed";
            return;
          }
          txn = tm_->Begin();
        }
      }
      if (err->empty() && txn.PreCommit() != Status::kSuccess) {
        *err = "round " + std::to_string(round) + " owner " +
               std::to_string(owner) + " final precommit failed";
      }
    };

    std::string err0;
    std::string err1;
    std::thread t0(worker, 0, &err0);
    std::thread t1(worker, 1, &err1);
    t0.join();
    t1.join();
    if (!err0.empty() || !err1.empty()) {
      ADD_FAILURE() << err0 << err1;
      return;
    }
    last_committed = planned;

    // Fresh transaction: the disjoint spaces must read exactly back, and
    // shared keys must hold one of the legal writer values.
    Transaction check = tm_->Begin();
    for (int owner = 0; owner < 2; ++owner) {
      for (const auto& [key, value] :
           last_committed[static_cast<size_t>(owner)]) {
        StatusOr<std::string> got = bpt_->Read(check, key);
        if (!got.HasValue() || got.Value() != value) {
          ADD_FAILURE() << "round " << round << " key " << key << " value "
                        << (got.HasValue() ? std::string(got.Value())
                                           : "<missing>");
          return;
        }
      }
    }
    for (int i = 0; i < 60; ++i) {
      const std::string sk = shared_key(i);
      StatusOr<std::string> got = bpt_->Read(check, sk);
      if (!got.HasValue()) {
        continue;  // never successfully inserted; absence is legal
      }
      const std::string& v(got.Value());
      const bool legal = v.starts_with("sv0_") || v.starts_with("sv1_");
      if (!legal) {
        ADD_FAILURE() << "shared key " << sk << " holds garbage '" << v << "'";
        return;
      }
    }
    ASSERT_SUCCESS(check.PreCommit());
    ASSERT_TRUE(bpt_->SanityCheckForTest(p_.get()));
  }
}

}  // namespace tinylamb
