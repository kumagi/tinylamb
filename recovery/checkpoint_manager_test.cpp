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

#include "recovery/checkpoint_manager.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "index/lsm_index.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

class CheckpointManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "checkpoint_manager_test-" + RandomString();
    Open(0);
  }

  void Open(lsn_t checkpoint_lsn) {
    cm_.reset();
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    pm_ = PageManager::Create(prefix_ + ".db", 64, false).MoveValue();
    logger_ = Logger::Create(prefix_ + ".log").MoveValue();
    rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                            logger_->SegmentSize());
    rm_->SetLogger(logger_.get());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), logger_.get(),
                                               rm_.get());
    cm_ = std::make_unique<CheckpointManager>(prefix_,
                                              tm_.get(), pm_->GetPool(), 0);
    ASSERT_SUCCESS(rm_->RecoverFrom(checkpoint_lsn, tm_.get()));
  }

  void TearDown() override {
    cm_.reset();
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    std::ignore = std::remove((prefix_ + ".db").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt.tmp").c_str());
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(".", ec)) {
      const std::string fname = entry.path().filename().string();
      // LSM compaction / page eviction leave <db>.lsm.<gen> and
      // <db>.blocks.<gen> files behind; clean them up like the sibling tests.
      if (fname.starts_with(prefix_ + ".db.lsm.") ||
          fname.starts_with(prefix_ + ".db.blocks.")) {
        std::filesystem::remove(entry.path(), ec);
      }
    }
    RemoveLogAndSegments(prefix_ + ".log");
    std::ignore = std::remove((prefix_ + ".last_checkpoint").c_str());
    std::ignore = std::remove((prefix_ + ".last_checkpoint.tmp").c_str());
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
};

TEST_F(CheckpointManagerTest,
       FuzzyCheckpointWithInFlightAndPostCheckpointTxns) {
  page_id_t pid = 0;
  page_id_t lsm_root = 0;
  slot_t s0 = 0;

  // 1. Pre-checkpoint committed transaction.
  {
    Transaction t1 = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(t1, PageType::kRowPage));
    pid = page->PageID();
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(t1, "before_ckpt"));
    s0 = slot;
    page.PageUnlock();

    ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(t1));
    lsm_root = idx.Root();
    ASSERT_SUCCESS(idx.Insert(t1, "k1", "v1"));
    ASSERT_SUCCESS(t1.PreCommit());
  }

  // 2. In-flight transaction that starts before the fuzzy checkpoint and
  // commits AFTER the fuzzy checkpoint.
  Transaction in_flight_winner = tm_->Begin();
  {
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(in_flight_winner, "k_inflight_win", "v_win"));
  }

  // 3. In-flight loser that starts before the fuzzy checkpoint and never
  // commits.
  Transaction in_flight_loser = tm_->Begin();
  {
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(in_flight_loser, "k_inflight_lose", "v_lose"));
  }

  // 4. Write lock-free fuzzy checkpoint while both transactions are active!
  ASSIGN_OR_ASSERT_FAIL(lsn_t, ckpt_lsn, cm_->WriteCheckpoint());
  EXPECT_GT(ckpt_lsn, 0U);

  // 5. Commit in_flight_winner, leave in_flight_loser uncommitted, and also
  // commit a post-checkpoint transaction.
  ASSERT_SUCCESS(in_flight_winner.PreCommit());
  {
    Transaction post = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
    ASSERT_SUCCESS(page->Update(post, s0, "after_ckpt"));
    page.PageUnlock();
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(post, "k_post", "v_post"));
    ASSERT_SUCCESS(post.PreCommit());
  }

  // Simulate crash.
  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  pm_->DropAllPages();
  in_flight_loser = Transaction();

  // Recover from the fuzzy checkpoint!
  Open(ckpt_lsn);

  Transaction check = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row, page->Read(check, s0));
  EXPECT_EQ(row, "after_ckpt");
  page.PageUnlock();

  LsmIndex idx(lsm_root);
  const auto k1 = idx.Read(check, "k1");
  ASSERT_TRUE(k1.HasValue()) << k1.GetStatus();
  EXPECT_EQ(k1.Value(), "v1");
  const auto k_inflight_win = idx.Read(check, "k_inflight_win");
  ASSERT_TRUE(k_inflight_win.HasValue()) << k_inflight_win.GetStatus();
  EXPECT_EQ(k_inflight_win.Value(), "v_win");
  const auto k_post = idx.Read(check, "k_post");
  ASSERT_TRUE(k_post.HasValue()) << k_post.GetStatus();
  EXPECT_EQ(k_post.Value(), "v_post");
  EXPECT_EQ(idx.Read(check, "k_inflight_lose").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS(check.PreCommit());
}

// Before-sync gate for the flush worker: once armed, the next fdatasync parks
// until released. The worker holds segment_mu_ while parked, so records
// appended in the meantime stay in the ring buffer and never reach the WAL
// file -- what a power loss at that moment would leave behind.
struct SyncGate {
  std::mutex mu;
  std::condition_variable cv;
  bool armed = false;
  bool entered = false;
  bool released = false;
};

void CopyDatabaseFiles(const std::string& from, const std::string& to) {
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(".", ec)) {
    const std::string fname = entry.path().filename().string();
    if (fname.starts_with(from + ".")) {
      std::filesystem::copy_file(
          entry.path(), to + fname.substr(from.size()),
          std::filesystem::copy_options::overwrite_existing, ec);
    }
  }
}

void RemoveDatabaseFiles(const std::string& prefix) {
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(".", ec)) {
    if (entry.path().filename().string().starts_with(prefix + ".")) {
      std::filesystem::remove(entry.path(), ec);
    }
  }
}

// tlaplus/TxSpill.tla (cfg/current_harm.cfg): a commit reflected into a page
// after the checkpoint's replay anchor was taken must not reach the snapshot
// before its kCommit is durable. Blocks-file writes wait only for the page's
// max slot LSN, i.e. the update record's LSN, not the commit's. If the
// snapshot is published and the WAL tail holding the kCommit is lost, restart
// has no undo phase and the never-committed value stays visible.
TEST_F(CheckpointManagerTest,
       SnapshotDoesNotPersistCommitWhoseCommitRecordIsNotDurable) {
  page_id_t pid = 0;
  slot_t slot = 0;
  {
    Transaction t = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(t, PageType::kRowPage));
    pid = page->PageID();
    ASSIGN_OR_ASSERT_FAIL(slot_t, s, page->Insert(t, "old"));
    slot = s;
    page.PageUnlock();
    ASSERT_SUCCESS(t.PreCommit());
  }

  // T1 updates the row before the checkpoint and commits after its anchor.
  Transaction t1 = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
    ASSERT_SUCCESS(page->Update(t1, slot, "new"));
  }
  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  // CheckpointManager::WriteCheckpoint would pick the same anchor: T1 is the
  // oldest registered transaction that has logged.
  const lsn_t anchor = t1.FirstLSN();
  ASSERT_GT(anchor, 0U);

  auto gate = std::make_shared<SyncGate>();
  Logger::SetBeforeSyncHookForTest([gate](int) {
    std::unique_lock lk(gate->mu);
    if (!gate->armed || gate->released) {
      return 0;
    }
    gate->entered = true;
    gate->cv.notify_all();
    gate->cv.wait(lk, [&] { return gate->released; });
    return 0;
  });
  std::thread committer;
  std::thread checkpointer;
  auto release = [&] {
    {
      std::scoped_lock lk(gate->mu);
      gate->released = true;
    }
    gate->cv.notify_all();
  };
  struct Finally {
    std::function<void()> f;
    ~Finally() { f(); }
  } finally{[&] {
    release();
    if (checkpointer.joinable()) {
      checkpointer.join();
    }
    if (committer.joinable()) {
      committer.join();
    }
    Logger::SetBeforeSyncHookForTest({});
  }};

  {
    std::scoped_lock lk(gate->mu);
    gate->armed = true;
  }
  // An unrelated uncommitted write: the worker writes it, then parks in the
  // gated fdatasync.
  Transaction t0 = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
    ASSIGN_OR_ASSERT_FAIL(slot_t, unrelated, page->Insert(t0, "unrelated"));
    (void)unrelated;
  }
  {
    std::unique_lock lk(gate->mu);
    ASSERT_TRUE(gate->cv.wait_for(lk, std::chrono::seconds(10),
                                  [&] { return gate->entered; }));
  }

  // T1's kCommit goes to the ring buffer only. Without the WAL-before-data
  // gate on reflected commits, the commit is reflected into the page and
  // published while PreCommit parks in WaitForDurable. With the gate, an
  // eviction inside the reflection already waits for that kCommit, so
  // publication does not happen while the WAL is held; that is fine too.
  const lsn_t buffered_before_t1_commit = logger_->BufferedLSN();
  Status commit_status = Status::kSuccess;
  committer = std::thread([&] { commit_status = t1.PreCommit(); });
  // Wait until T1's commit record has actually entered the WAL buffer. The
  // unrelated write above already made BufferedLSN nonzero, so checking only
  // DurableLSN < BufferedLSN would not establish that this commit is pending.
  const auto commit_record_deadline = std::chrono::steady_clock::now() +
                                      std::chrono::seconds(10);
  while (logger_->BufferedLSN() <= buffered_before_t1_commit &&
         std::chrono::steady_clock::now() < commit_record_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_GT(logger_->BufferedLSN(), buffered_before_t1_commit);
  bool published = false;
  for (int i = 0; i < 100 && !published; ++i) {
    Transaction check = tm_->Begin();
    {
      ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
      ASSIGN_OR_ASSERT_FAIL(std::string_view, row, page->Read(check, slot));
      published = row == "new";
    }
    ASSERT_SUCCESS(check.Abort());
    if (!published) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  ASSERT_LT(logger_->DurableLSN(), logger_->BufferedLSN());

  // The fuzzy checkpoint body. A correct implementation either refuses to
  // publish this image yet (and blocks until the kCommit is durable) or
  // publishes a snapshot without T1's value; both are fine.
  std::promise<Status> ckpt_promise;
  std::future<Status> ckpt_future = ckpt_promise.get_future();
  checkpointer = std::thread(
      [&] { ckpt_promise.set_value(pm_->WriteFuzzyCheckpoint(anchor)); });
  const bool ckpt_published = ckpt_future.wait_for(std::chrono::seconds(2)) ==
                              std::future_status::ready;

  // Power loss here: only what reached the files survives. T1's kCommit is
  // the last record appended, so a written end below the buffered end means
  // it is not in the WAL file.
  ASSERT_LT(logger_->CommittedLSN(), logger_->BufferedLSN());
  const std::string crash_prefix = prefix_ + "-crash";
  CopyDatabaseFiles(prefix_, crash_prefix);

  release();
  checkpointer.join();
  committer.join();
  ASSERT_SUCCESS(ckpt_future.get());
  ASSERT_SUCCESS(commit_status);
  ASSERT_SUCCESS(t0.Abort());
  t0 = Transaction();
  t1 = Transaction();

  const std::string original = prefix_;
  prefix_ = crash_prefix;
  Open(0);
  RemoveDatabaseFiles(original);

  Transaction check = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row, page->Read(check, slot));
  EXPECT_EQ(std::string(row), "old")
      << "T1's kCommit never reached the WAL, but "
      << (ckpt_published ? "the published snapshot already holds T1"
                         : "restart still shows T1");
  page.PageUnlock();
  ASSERT_SUCCESS(check.PreCommit());
}

TEST_F(CheckpointManagerTest,
       CompactionShortensWalReplayWithoutCheckpointSidecar) {
  page_id_t lsm_root = 0;
  {
    Transaction t1 = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(t1));
    lsm_root = idx.Root();
    for (int i = 0; i < 32; ++i) {
      ASSERT_SUCCESS(
          idx.Insert(t1, "k" + std::to_string(i), "v" + std::to_string(i)));
    }
    ASSERT_SUCCESS(t1.PreCommit());
  }

  const lsn_t wal_before_ckpt = logger_->BufferedLSN();
  ASSIGN_OR_ASSERT_FAIL(lsn_t, replay_start, cm_->WriteCheckpoint());
  // Compaction persistence does not append a kBeginCheckpoint record to WAL
  // and does not create a .last_checkpoint sidecar file.
  EXPECT_EQ(logger_->BufferedLSN(), wal_before_ckpt);
  EXPECT_EQ(replay_start, wal_before_ckpt);
  EXPECT_FALSE(std::filesystem::exists(prefix_ + ".last_checkpoint"));

  // Simulate crash and recover with checkpoint_lsn = 0 (loaded from .db).
  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  pm_->DropAllPages();
  Open(0);

  // Next transaction ID must advance past pre-compaction transactions even
  // though Pass 1 started at replay_start == LogEnd().
  Transaction check = tm_->Begin();
  EXPECT_GT(check.ID(), 1U);
  LsmIndex idx(lsm_root);
  for (int i = 0; i < 32; ++i) {
    EXPECT_EQ(idx.Read(check, "k" + std::to_string(i)).MoveValue(),
              "v" + std::to_string(i));
  }
  ASSERT_SUCCESS(check.PreCommit());
}

TEST_F(CheckpointManagerTest, RapidConstructAndStopDoesNotLoseWakeup) {
  for (int i = 0; i < 64; ++i) {
    SCOPED_TRACE(i);
    CheckpointManager transient(prefix_ + "_transient.db", tm_.get(), pm_.get(),
                                5);
    transient.Start();
    // Measure each Stop() on its own: a single lost wakeup must be caught
    // even when the other 63 iterations return quickly. A cumulative bound
    // would dilute one hang across all iterations and could also fail
    // spuriously when a loaded ctest worker slows every iteration slightly.
    const auto begin = std::chrono::steady_clock::now();
    transient.Stop();
    // Stop() must return promptly: a lost wakeup would park the worker and
    // reappear as a multi-second hang instead of an assertion failure.
    EXPECT_LT(std::chrono::steady_clock::now() - begin,
              std::chrono::seconds(10));
  }
}

TEST_F(CheckpointManagerTest,
       WalRetentionWarningFiresWhenPinnedAboveThreshold) {
  cm_->SetWalRetentionWarnBytesForTest(256);

  // An uncommitted active transaction that wrote a WAL record pins the
  // checkpoint replay anchor at its FirstLSN.
  Transaction pinner = tm_->Begin();
  ASSERT_TRUE(pinner.AddWriteSet(RowPosition(1, 0), "before"));
  ASSERT_SUCCESS(
      pinner.UpdateLog(1, 0, std::string(128, 'a'), std::string(128, 'b'))
          .GetStatus());

  // Subsequent commits advance DurableLSN while LowestRetainedLSN remains
  // pinned.
  for (int i = 0; i < 8; ++i) {
    Transaction t = tm_->Begin();
    const RowPosition rp(2, static_cast<slot_t>(i));
    ASSERT_TRUE(t.AddWriteSet(rp, "old"));
    ASSERT_SUCCESS(
        t.UpdateLog(2, static_cast<slot_t>(i), "old", std::string(200, 'x'))
            .GetStatus());
    t.RegisterVersionWrite(rp, "old", "new");
    ASSERT_SUCCESS(t.PreCommit());
  }

  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  ASSIGN_OR_ASSERT_FAIL(lsn_t, anchor, cm_->WriteCheckpoint());
  EXPECT_LE(anchor, pinner.FirstLSN());
  EXPECT_EQ(cm_->WalRetentionWarnCountForTest(), 1U);

  // Second checkpoint within the rate-limit window does not spam warnings.
  ASSERT_SUCCESS(cm_->WriteCheckpoint().GetStatus());
  EXPECT_EQ(cm_->WalRetentionWarnCountForTest(), 1U);

  ASSERT_SUCCESS(pinner.Abort());
}

TEST_F(CheckpointManagerTest,
       IntervalWatchdogCheckpointsWalLagWithoutCompaction) {
  // T4262: the worker wakes every interval_seconds_ and checkpoints retained
  // WAL above the lag threshold even when no compaction callback fires;
  // previously the interval was only an enable/disable boolean and the WAL
  // grew unboundedly without LSM activity.
  CheckpointManager watchdog(prefix_ + "_lag.db.last_checkpoint", tm_.get(),
                             pm_->GetPool(), 1);
  watchdog.SetWalLagCheckpointBytesForTest(1);
  watchdog.Start();
  for (int i = 0; i < 8; ++i) {
    Transaction t = tm_->Begin();
    const RowPosition rp(3, static_cast<slot_t>(i));
    ASSERT_TRUE(t.AddWriteSet(rp, "old"));
    ASSERT_SUCCESS(
        t.UpdateLog(3, static_cast<slot_t>(i), "old", std::string(200, 'y'))
            .GetStatus());
    t.RegisterVersionWrite(rp, "old", "new");
    ASSERT_SUCCESS(t.PreCommit());
  }
  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  const uint64_t before = watchdog.WorkerCheckpointsForTest();
  // The watchdog wakes at most once per interval_seconds_ (1 s here), so a
  // handful of intervals is plenty. Keep the budget small: with the old
  // 15 s literal a stalled worker burned a quarter of the 60 s ctest timeout
  // before the EXPECT below finally reported the regression.
  constexpr int kWatchdogBudgetMs = 5000;
  for (int wait_ms = 0; wait_ms < kWatchdogBudgetMs &&
                        watchdog.WorkerCheckpointsForTest() == before;
       wait_ms += 50) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_GT(watchdog.WorkerCheckpointsForTest(), before)
      << "interval watchdog never checkpointed the WAL lag within "
      << kWatchdogBudgetMs << " ms";
  watchdog.Stop();
}

TEST_F(CheckpointManagerTest, StopThenStartRestartsWorker) {
  // T3687: Stop() joins the worker thread out of existence; Start() must
  // re-arm and spawn a fresh worker.  Before the fix Start() after Stop()
  // was a silent no-op and this checkpoint never arrived.
  CheckpointManager manager(prefix_ + "_restart.db.last_checkpoint", tm_.get(),
                            pm_->GetPool(), 1);
  manager.SetWalLagCheckpointBytesForTest(1);
  manager.Start();
  manager.Stop();
  manager.Start();
  for (int i = 0; i < 8; ++i) {
    Transaction t = tm_->Begin();
    const RowPosition rp(4, static_cast<slot_t>(i));
    ASSERT_TRUE(t.AddWriteSet(rp, "old"));
    ASSERT_SUCCESS(
        t.UpdateLog(4, static_cast<slot_t>(i), "old", std::string(200, 'z'))
            .GetStatus());
    t.RegisterVersionWrite(rp, "old", "new");
    ASSERT_SUCCESS(t.PreCommit());
  }
  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  for (int wait_ms = 0;
       wait_ms < 15000 && manager.WorkerCheckpointsForTest() == 0;
       wait_ms += 50) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_GE(manager.WorkerCheckpointsForTest(), 1U)
      << "worker did not resume after Stop() -> Start()";
  manager.Stop();
}

TEST_F(CheckpointManagerTest, ConcurrentStartStopDoesNotDeadlock) {
  // #10850: Start()/Stop() used to race on the PageManager compaction
  // callback registration (outside worker_wait_mu_), leaving a stale or
  // missing callback. Hammer the lifecycle transition from two threads and
  // make sure neither path deadlocks and the manager stays reusable.
  CheckpointManager manager(prefix_ + "_race.db.last_checkpoint", tm_.get(),
                            pm_.get(), 1);
  std::thread starter([&] {
    for (int i = 0; i < 32; ++i) {
      manager.Start();
    }
  });
  std::thread stopper([&] {
    for (int i = 0; i < 32; ++i) {
      manager.Stop();
    }
  });
  starter.join();
  stopper.join();
  // Final state must be well-defined and the worker restartable.
  manager.Stop();
  manager.Start();
  manager.Stop();
}

TEST(CheckpointManagerNullTest, WriteCheckpointWithoutLoggerReturnsError) {
  CheckpointManager manager("null_logger_ckpt-" + RandomString(), nullptr,
                            nullptr, 0);
  const auto result = manager.WriteCheckpoint();
  ASSERT_FALSE(result.HasValue());
  EXPECT_EQ(result.GetStatus(), Status::kInvalidArgument);
}

}  // namespace
}  // namespace tinylamb
