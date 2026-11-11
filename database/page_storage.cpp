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

#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <ostream>
#include <string>
#include <string_view>
#include <system_error>

#include "common/constants.hpp"
#include "common/log_message.hpp"
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
}  // namespace

StatusOr<std::unique_ptr<PageStorage>> PageStorage::Create(
    std::string_view dbname, size_t wal_sync_ms, bool preallocate_new_wal) {
  const std::string dbname_str(dbname);
  const size_t requested_segment_size = LogSegmentSizeFromEnv();
  ASSIGN_OR_RETURN(std::unique_ptr<PageManager>, pm,
                   PageManager::Create(dbname_str + ".db",
                                       PagePoolCapacityFromEnv(), false));
  ASSIGN_OR_RETURN(
      std::unique_ptr<Logger>, logger,
      Logger::Create(dbname_str + ".log", static_cast<size_t>(8 * 1024 * 1024),
                     wal_sync_ms, requested_segment_size, preallocate_new_wal,
                     RecoveryManager::TornTailTruncationAllowed()));
  const size_t segment_size = logger->SegmentSize();
  auto rm = std::make_unique<RecoveryManager>(dbname_str + ".log",
                                              pm->GetPool(), segment_size);
  auto tm =
      std::make_unique<TransactionManager>(pm.get(), logger.get(), rm.get());
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
  // Recycling may only cut at record boundaries: a record straddling a
  // segment boundary keeps its head in the older segment until it is gone
  // from the WAL entirely.
  storage->logger_->SetRecycleBoundaryValidator(
      [rm_raw = storage->rm_.get()](lsn_t lsn) {
        LogRecord probe;
        return rm_raw->ReadLog(lsn, &probe);
      });
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
  const Status recovered = storage->rm_->RecoverFrom(0, storage->tm_.get());
  if (recovered != Status::kSuccess) {
    storage->pm_->GetPool()->DropAllPages();
    return recovered;
  }
  RETURN_IF_FAIL(PageManager::InitializeMetaPage());
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
