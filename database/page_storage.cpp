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
#include <string_view>

#include "common/constants.hpp"
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

// Reads the checkpoint LSN from the master record written by
// CheckpointManager. Missing/truncated files resolve to 0; callers
// additionally verify the value against the WAL itself.
lsn_t ReadMasterRecordLsn(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return 0;
  }
  std::array<char, sizeof(lsn_t)> buffer{};
  in.read(buffer.data(), sizeof(buffer));
  if (in.gcount() != static_cast<std::streamsize>(sizeof(buffer))) {
    return 0;
  }
  lsn_t lsn = 0;
  DeserializeU64(buffer.data(), &lsn);
  return lsn;
}
}  // namespace

StatusOr<std::unique_ptr<PageStorage>> PageStorage::Create(
    std::string_view dbname, size_t wal_sync_ms) {
  const std::string dbname_str(dbname);
  ASSIGN_OR_RETURN(
      std::unique_ptr<Logger>, logger,
      Logger::Create(dbname_str + ".log", static_cast<size_t>(8 * 1024 * 1024),
                     wal_sync_ms));
  ASSIGN_OR_RETURN(
      std::unique_ptr<PageManager>, pm,
      PageManager::Create(dbname_str + ".db", PagePoolCapacityFromEnv()));
  auto rm =
      std::make_unique<RecoveryManager>(dbname_str + ".log", pm->GetPool());
  auto tm =
      std::make_unique<TransactionManager>(pm.get(), logger.get(), rm.get());
  auto cm = std::make_unique<CheckpointManager>(dbname_str + ".last_checkpoint",
                                                tm.get(), pm->GetPool());
  auto storage = std::unique_ptr<PageStorage>(
      new PageStorage(dbname, std::move(logger), std::move(pm), std::move(rm),
                      std::move(tm), std::move(cm)));
  // WAL rule for evictions: a dirty page must never reach disk ahead of the
  // log records its page_lsn covers. The gate fires outside the pool latch,
  // so blocking on the group-commit flush here is deadlock-free. Declared
  // member order guarantees the pool drains before the logger shuts down.
  storage->pm_->GetPool()->SetDurabilityGate([s = storage.get()](lsn_t lsn) {
    return s->logger_->WaitForDurable(lsn);
  });
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
  RETURN_IF_FAIL(storage->rm_->RecoverFrom(checkpoint, storage->tm_.get()));
  // Periodic checkpointing keeps the WAL bounded: without it fdatasync cost
  // scales with the file's dirty page cache footprint, which on a long TPC-C
  // run makes every commit barrier approach 1 ms regardless of group size.
  // Honor TINYLAMB_CHECKPOINT_SECONDS for tests (0 disables); the default 10 s
  // balances checkpoint overhead with WAL growth.
  const char* ckpt_env = std::getenv("TINYLAMB_CHECKPOINT_SECONDS");
  if (ckpt_env == nullptr || ckpt_env[0] == '\0') {
    storage->cm_->Start();
  } else {
    const unsigned long long seconds = std::strtoull(ckpt_env, nullptr, 10);
    if (seconds > 0) {
      storage->cm_->Start();
    }
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

void PageStorage::DiscardAllUpdates() { pm_->GetPool()->DropAllPages(); }

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
