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

#include "common/platform/process.hpp"
#include "page_storage.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "recovery/log_record.hpp"
#include "table/tlcs_file.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
namespace {
// WAL segment size override for tests that exercise segment rollover or
// recycling without writing tens of megabytes. 0 / unset keeps the default.
size_t LogSegmentSizeFromEnv() {
  const char* env = ::tinylamb::platform::GetEnv("TINYLAMB_LOG_SEGMENT_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return Logger::kDefaultLogSegmentSize;
  }
  // strtoull wraps a leading '-' to ULLONG_MAX; reject signs explicitly.
  if (env[0] == '-' || env[0] == '+' ||
      std::isspace(static_cast<unsigned char>(env[0])) != 0) {
    return Logger::kDefaultLogSegmentSize;
  }
  char* end = nullptr;
  errno = 0;
  const unsigned long long bytes = std::strtoull(env, &end, 10);
  if (end == env || *end != '\0' || errno == ERANGE ||
      (sizeof(size_t) < sizeof(bytes) &&
       bytes > static_cast<unsigned long long>(
                   std::numeric_limits<size_t>::max()))) {
    return Logger::kDefaultLogSegmentSize;
  }
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
  // Pages live in RAM under PageManager; a non-zero capacity only installs a
  // test buffer-pool byte limit (SetBufferPoolLimitBytesForTest). This path
  // passes 0, so no test override is applied.
  ASSIGN_OR_RETURN(std::unique_ptr<PageManager>, pm,
                   PageManager::Create(dbname_str + ".db",
                                       /*capacity=*/0, false));
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
  const char* ckpt_env = ::tinylamb::platform::GetEnv("TINYLAMB_CHECKPOINT_SECONDS");
  if (ckpt_env != nullptr && ckpt_env[0] != '\0') {
    // strtoull wraps a leading '-' to ULLONG_MAX and accepts '+' and
    // leading whitespace; reject signs/space explicitly like
    // LogSegmentSizeFromEnv does.
    const bool bad_sign =
        ckpt_env[0] == '-' || ckpt_env[0] == '+' ||
        std::isspace(static_cast<unsigned char>(ckpt_env[0])) != 0;
    char* end = nullptr;
    errno = 0;
    const unsigned long long seconds =
        bad_sign ? 0 : std::strtoull(ckpt_env, &end, 10);
    // 0 is a valid value: CheckpointManager treats interval_seconds_ == 0 as
    // "periodic checkpointing disabled", so let it through unchanged.
    if (bad_sign || errno != 0 || end == ckpt_env || *end != '\0') {
      LOG(WARN) << "invalid TINYLAMB_CHECKPOINT_SECONDS='" << ckpt_env
                << "'; using default 10s";
    } else {
      // CheckpointManager waits with std::chrono::seconds; durations are
      // nanoseconds internally, so cap at a value whose *1e9 stays inside
      // int64 (~9.2e9 s). The size_t-width clamp still applies on 32-bit.
      constexpr unsigned long long kMaxCheckpointSeconds =
          9'000'000'000ULL;
      checkpoint_interval = static_cast<size_t>(std::min<unsigned long long>(
          {seconds,
          
              kMaxCheckpointSeconds,
              1ULL << (sizeof(size_t) * 8 - 5)}));
    }
  }
  auto cm = std::make_unique<CheckpointManager>(dbname_str,
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
  storage->pm_->SetDurableWaiter([logger = storage->logger_.get()](lsn_t lsn) {
    return logger->WaitForDurable(lsn);
  });
  std::error_code db_size_error;
  const auto db_size =
      std::filesystem::file_size(dbname_str + ".db", db_size_error);
  if (db_size_error) {
    return StatusError(StatusCode::kIOError, "failed to stat database file: " +
                                                 db_size_error.message());
  }
  if (db_size != 0) {
    // `LogEnd()` folds a real I/O failure into 0, which would misdiagnose an
    // unreadable WAL as corruption; consult the status-preserving variant
    // and only claim corruption for a genuinely empty log (P642).
    ASSIGN_OR_RETURN(lsn_t, wal_end, storage->rm_->LogEndStatus());
    if (wal_end == 0) {
      return StatusError(StatusCode::kCorrupt,
                         "database file is nonempty but WAL is empty");
    }
  }
  struct TlcsRecoveryEntry {
    std::shared_ptr<TlcsFile> file;
    Schema schema;
  };
  bool tlcs_scanned = false;
  std::vector<TlcsRecoveryEntry> tlcs_entries;
  storage->pm_->SetColumnarStubHydrator(
      [db_path = dbname_str + ".db", &tlcs_scanned,
       &tlcs_entries](page_id_t pid) -> StatusOr<std::vector<std::string>> {
        if (!tlcs_scanned) {
          tlcs_scanned = true;
          const std::filesystem::path p(db_path);
          const std::filesystem::path dir = p.parent_path().empty()
                                                ? std::filesystem::path(".")
                                                : p.parent_path();
          const std::string prefix = p.filename().string() + ".tlcs.";
          std::error_code ec;
          std::filesystem::directory_iterator it(dir, ec);
          if (ec) {
            return StatusError(
                StatusCode::kIOError,
                "cannot enumerate TLCS recovery directory: " + ec.message());
          }
          for (const auto& entry : it) {
            const std::string fname = entry.path().filename().string();
            if (!fname.starts_with(prefix)) {
              continue;
            }
            const std::string suffix = fname.substr(prefix.size());
            if (suffix.empty() || !std::ranges::all_of(suffix, [](char c) {
                  return c >= '0' && c <= '9';
                })) {
              continue;
            }
            Schema file_schema;
            StatusOr<std::shared_ptr<TlcsFile>> opened =
                TlcsFile::OpenWithStoredSchema(entry.path().string(),
                                               &file_schema);
            if (opened.HasValue()) {
              tlcs_entries.push_back(TlcsRecoveryEntry{
                  .file = opened.MoveValue(),
                  .schema = std::move(file_schema),
              });
            } else {
              LOG(LogLevel::kWarn)
                  << "recovery: cannot open TLCS file "
                  << entry.path().string() << ": " << opened.GetStatus();
            }
          }
        }
        for (const TlcsRecoveryEntry& item : tlcs_entries) {
          if (item.file->FindBlockIndex(pid) !=
              LSTableStore::kInvalidBlockIndex) {
            std::vector<std::string> rows;
            RETURN_IF_FAIL(item.file->DecodeBlockEncodedRowsByPid(
                pid, item.schema, &rows));
            return rows;
          }
        }
        return StatusError(
            StatusCode::kNotExists,
            "no backing tlcs file covers stub page " + std::to_string(pid));
      });
  const Status recovered = storage->rm_->RecoverFrom(0, storage->tm_.get());
  storage->pm_->SetColumnarStubHydrator({});
  tlcs_entries.clear();
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
  // WAL. Failure is not fatal: the periodic checkpoint (when
  // enabled) retries, and the recovered pages stay protected by the retained
  // WAL either way. Presence of TINYLAMB_DISABLE_EOR_CHECKPOINT (any value)
  // opts out (A/B tests).
  // T4256: LogEnd() folded a transient I/O failure into 0, which silently
  // skipped the end-of-recovery checkpoint below with no trace. Query the
  // status-preserving variant: skipping stays non-fatal (the periodic
  // checkpoint retries; the retained WAL still protects replayed pages),
  // but the skip is now visible.
  const StatusOr<lsn_t> wal_end_or = storage->rm_->LogEndStatus();
  const lsn_t wal_end = wal_end_or.HasValue() ? wal_end_or.Value() : lsn_t{0};
  if (!wal_end_or.HasValue()) {
    LOG(LogLevel::kWarn) << "recovery: LogEnd failed ("
                         << ToString(wal_end_or.GetStatus())
                         << "); skipping end-of-recovery checkpoint";
  }
  // If recovery's WAL replay anchor already reaches the end of the log and no
  // torn tail was repaired, the persisted snapshot covers every committed
  // record: a full .db serialization here would be a pure no-op that only
  // rewrites the file and its fsync on every clean open.
  const bool snapshot_already_current =
      storage->rm_->RecoveryAnchorLSN() >= wal_end &&
      !storage->rm_->RecoveryTornTailTruncated();
  if (wal_end > 0 && !snapshot_already_current &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_EOR_CHECKPOINT") == nullptr) {
    const StatusOr<lsn_t> anchored = storage->cm_->WriteCheckpoint();
    if (!anchored.HasValue()) {
      LOG(WARN)
          << "Post-recovery checkpoint failed (WAL retained; no automatic "
             "retry when the periodic worker is disabled): "
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
