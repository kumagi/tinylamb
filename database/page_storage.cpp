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

//
// Created by kumagi on 22/05/04.
//

#include "page_storage.hpp"

#include <array>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ios>
#include <ostream>
#include <string>
#include <string_view>
#include <system_error>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
namespace {
size_t PagePoolCapacityFromEnv() {
  const char* env = std::getenv("TINYLAMB_PAGE_POOL_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return kDefaultPagePoolCapacity;
  }
  const unsigned long long bytes = std::strtoull(env, nullptr, 10);
  if (bytes < kPageSize) {
    return kDefaultPagePoolCapacity;
  }
  return static_cast<size_t>(bytes / kPageSize);
}

// WAL segment size override for tests that exercise segment rollover or
// recycling without writing tens of megabytes. 0 / unset keeps the default.
size_t LogSegmentSizeFromEnv() {
  const char* env = std::getenv("TINYLAMB_LOG_SEGMENT_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return Logger::kDefaultLogSegmentSize;
  }
  const unsigned long long bytes = std::strtoull(env, nullptr, 10);
  if (bytes < kPageSize) {
    return Logger::kDefaultLogSegmentSize;
  }
  return static_cast<size_t>(bytes);
}

size_t MinimumLogSegmentSize() {
  const std::string body(kPageBodySize, '\0');
  return LogRecord::FullPageImageLogRecord(0, PageType::kRowPage, body)
      .Serialize()
      .size();
}

// Reads the checkpoint LSN from the master record written by
// CheckpointManager. Missing/truncated files resolve to 0; callers
// additionally verify the value against the WAL itself.
lsn_t ReadMasterRecordLsn(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return 0;
  }
  std::array<char, kCheckpointMasterRecordSize> buffer{};
  in.read(buffer.data(), buffer.size());
  const auto bytes = in.gcount();
  if (std::cmp_equal(bytes, static_cast<std::streamsize>(sizeof(lsn_t)))) {
    lsn_t lsn = 0;
    DeserializeU64(buffer.data(), &lsn);
    return lsn;
  }
  if (std::cmp_not_equal(
          bytes, static_cast<std::streamsize>(kCheckpointMasterRecordSize))) {
    return 0;
  }
  uint32_t magic = 0;
  uint32_t version = 0;
  lsn_t lsn = 0;
  uint32_t stored_crc = 0;
  DeserializeU32(buffer.data(), &magic);
  DeserializeU32(buffer.data() + sizeof(uint32_t), &version);
  DeserializeU64(buffer.data() + (sizeof(uint32_t) * 2), &lsn);
  DeserializeU32(buffer.data() + kCheckpointMasterRecordSize - sizeof(uint32_t),
                 &stored_crc);
  if (magic != kCheckpointMasterMagic || version != kCheckpointMasterVersion) {
    return 0;
  }
  const uint32_t computed =
      Crc32C(buffer.data(), kCheckpointMasterRecordSize - sizeof(uint32_t));
  return stored_crc == computed ? lsn : 0;
}
}  // namespace

StatusOr<std::unique_ptr<PageStorage>> PageStorage::Create(
    std::string_view dbname, size_t wal_sync_ms) {
  const std::string dbname_str(dbname);
  const size_t requested_segment_size = LogSegmentSizeFromEnv();
  const size_t minimum_segment_size = MinimumLogSegmentSize();
  ASSIGN_OR_RETURN(std::unique_ptr<PageManager>, pm,
                   PageManager::Create(dbname_str + ".db",
                                       PagePoolCapacityFromEnv(), false));
  ASSIGN_OR_RETURN(
      std::unique_ptr<Logger>, logger,
      Logger::Create(dbname_str + ".log", static_cast<size_t>(8 * 1024 * 1024),
                     wal_sync_ms, requested_segment_size));
  const size_t segment_size = logger->SegmentSize();
  if (segment_size < minimum_segment_size) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "persisted WAL segment geometry is too small for a full-page image: " +
            std::to_string(segment_size) + " < " +
            std::to_string(minimum_segment_size));
  }
  auto rm = std::make_unique<RecoveryManager>(dbname_str + ".log",
                                              pm->GetPool(), segment_size);
  auto tm =
      std::make_unique<TransactionManager>(pm.get(), logger.get(), rm.get());
  // Periodic checkpointing keeps the WAL bounded: without it fdatasync cost
  // scales with the file's dirty page cache footprint, which on a long TPC-C
  // run makes every commit barrier approach 1 ms regardless of group size.
  // TINYLAMB_CHECKPOINT_SECONDS tunes the period (0 disables); the default
  // 10 s balances checkpoint overhead with WAL growth. The value must reach
  // the CheckpointManager itself -- a stale draft read it only to decide
  // whether to call Start(), leaving every database on the compiled-in
  // default period.
  size_t checkpoint_interval = 10;
  const char* ckpt_env = std::getenv("TINYLAMB_CHECKPOINT_SECONDS");
  if (ckpt_env != nullptr && ckpt_env[0] != '\0') {
    const unsigned long long seconds = std::strtoull(ckpt_env, nullptr, 10);
    checkpoint_interval = static_cast<size_t>(std::min<unsigned long long>(
        seconds, 1ULL << (sizeof(size_t) * 8 - 5)));
  }
  auto cm = std::make_unique<CheckpointManager>(dbname_str + ".last_checkpoint",
                                                tm.get(), pm->GetPool(),
                                                checkpoint_interval);
  auto storage = std::unique_ptr<PageStorage>(
      new PageStorage(dbname, std::move(logger), std::move(pm), std::move(rm),
                      std::move(tm), std::move(cm)));
  // WAL rule for evictions: a dirty page must never reach disk ahead of the
  // log records its page_lsn covers. The gate fires outside the pool latch,
  // so blocking on the group-commit flush here is deadlock-free. It MUST
  // call the logger directly: the pool is destroyed AFTER the transaction
  // manager (declared-member teardown order), so ~PagePool's final
  // write-backs would touch a dead manager through this lambda.  Write-intent
  // waits still see these stalls through the group-commit backlog signal in
  // ExtendIntentDeadlineForDurabilityBarrier.
  storage->pm_->GetPool()->SetDurabilityGate([s = storage.get()](lsn_t lsn) {
    return s->logger_->WaitForDurable(lsn);
  });
  // Full-page-image (FPW) rule: a dirty page whose WAL snapshot is no longer
  // retained gets its complete image logged before the data file is touched,
  // keeping torn pages rebuildable across WAL recycling.
  storage->pm_->GetPool()->SetWalSnapshotSink(
      [s = storage.get()](page_id_t pid, PageType type,
                          std::string_view body) -> StatusOr<lsn_t> {
        const LogRecord record =
            LogRecord::FullPageImageLogRecord(pid, type, body);
        const std::string serialized = record.Serialize();
        ASSIGN_OR_RETURN(lsn_t, start, s->logger_->AddLog(serialized));
        return start + static_cast<lsn_t>(serialized.size());
      });
  // Recycling may only cut at record boundaries: a record straddling a
  // segment boundary keeps its head in the older segment until it is gone
  // from the WAL entirely.
  storage->logger_->SetRecycleBoundaryValidator(
      [rm_raw = storage->rm_.get()](lsn_t lsn) {
        LogRecord probe;
        return rm_raw->ReadLog(lsn, &probe);
      });
  // Runtime undo walks read their own WAL records back through the logger's
  // ring buffer, which lets TransactionManager::Abort skip the pre-undo
  // group-commit flush entirely (the ring serves records that are still
  // volatile; losing them to a crash replays the same rollback at restart).
  storage->rm_->SetLogger(storage->logger_.get());
  std::error_code db_size_error;
  const auto db_size =
      std::filesystem::file_size(dbname_str + ".db", db_size_error);
  if (db_size_error) {
    return StatusError(StatusCode::kIOError, "failed to stat database file: " +
                                                 db_size_error.message());
  }
  if (db_size != 0 && storage->rm_->LogEnd() == 0) {
    return StatusError(StatusCode::kCorrupt,
                       "database file is nonempty but WAL is empty");
  }
  // Resume from the last durable checkpoint when the master record points at
  // a real one; otherwise fall back to replaying the whole log. The hint is
  // only trusted when it lands on a decodable kBeginCheckpoint record -- a
  // garbage or torn hint must widen the recovery window, never skip it.
  lsn_t checkpoint = ReadMasterRecordLsn(storage->MasterRecordName());
  if (checkpoint != 0) {
    LogRecord probe;
    if (!storage->rm_->ReadLog(checkpoint, &probe) ||
        probe.type != LogType::kBeginCheckpoint) {
      LOG(WARN) << "Ignoring invalid master record at " << checkpoint << " for "
                << dbname;
      checkpoint = 0;
    }
  }
  // Full-page images below the anchor may already have been recycled by a
  // previous incarnation; the pool must not trust them.
  storage->pm_->GetPool()->SetSnapshotRetentionFloor(checkpoint);
  const Status recovered =
      storage->rm_->RecoverFrom(checkpoint, storage->tm_.get());
  if (recovered != Status::kSuccess) {
    // Recovery failed closed: whatever the replay managed to apply is
    // untrustworthy, and ~PagePool writes back every page whose checksum does
    // not match. Retire the buffer without writing anything so the data file
    // stays exactly as the crash left it and the next boot retries from the
    // same master record.
    storage->pm_->GetPool()->DropAllPages();
    return recovered;
  }
  RETURN_IF_FAIL(storage->pm_->InitializeMetaPage());
  // End-of-recovery checkpoint: anchor the replayed state durably before the
  // first user transaction. Without this, the whole recovery window replays
  // again on every restart until the first periodic checkpoint lands -- a
  // crash 5 seconds after a long recovery re-pays the full pass, and the WAL
  // the recovery needed cannot be recycled until a checkpoint publishes a new
  // master record. Failure is not fatal: the periodic checkpoint (when
  // enabled) retries, and the recovered pages stay protected by the retained
  // WAL either way. TINYLAMB_DISABLE_EOR_CHECKPOINT=1 opts out (A/B tests).
  if (storage->rm_->LogEnd() > 0 &&
      std::getenv("TINYLAMB_DISABLE_EOR_CHECKPOINT") == nullptr) {
    const StatusOr<lsn_t> anchored = storage->cm_->WriteCheckpoint();
    if (!anchored.HasValue()) {
      LOG(WARN) << "Post-recovery checkpoint failed (the periodic checkpoint "
                   "will retry): "
                << anchored.GetStatus();
    }
  }
  if (checkpoint_interval != 0) {
    storage->cm_->Start();
  }
  return storage;
}

PageStorage::PageStorage(std::string_view dbname,
                         std::unique_ptr<Logger> logger,
                         std::unique_ptr<PageManager> pm,
                         std::unique_ptr<RecoveryManager> rm,
                         std::unique_ptr<TransactionManager> tm,
                         std::unique_ptr<CheckpointManager> cm)
    : dbname_(dbname),
      logger_(std::move(logger)),
      pm_(std::move(pm)),
      rm_(std::move(rm)),
      tm_(std::move(tm)),
      cm_(std::move(cm)) {}

void PageStorage::DiscardAllUpdates() {
  cm_->Stop();
  pm_->GetPool()->DropAllPages();
}

Transaction PageStorage::Begin() { return tm_->Begin(); }

Transaction PageStorage::BeginReadOnly() { return tm_->Begin(true); }

std::string PageStorage::DBName() const { return dbname_ + ".db"; }
std::string PageStorage::LogName() const { return dbname_ + ".log"; }
std::string PageStorage::MasterRecordName() const {
  return dbname_ + ".last_checkpoint";
}

std::ostream& operator<<(std::ostream& o, const PageStorage& ps) {
  o << "PageStorage(dbname=" << ps.dbname_ << ", logger=" << *ps.logger_
    << ", page_manager=" << *ps.pm_ << ", recovery_manager=" << *ps.rm_
    << ", transaction_manager=" << *ps.tm_ << ", checkpoint_manager=" << *ps.cm_
    << ")";
  return o;
}

}  // namespace tinylamb
