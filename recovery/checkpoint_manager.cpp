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

#include "recovery/checkpoint_manager.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <limits>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "page/page_manager.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

Status WriteMasterRecord(const std::filesystem::path& path, lsn_t lsn) {
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      return StatusError(StatusCode::kIOError,
                         "Failed to open master record: " + tmp.string());
    }
    std::array<char, kCheckpointMasterRecordSize> encoded{};
    SerializeU32(encoded.data(), kCheckpointMasterMagic);
    SerializeU32(encoded.data() + sizeof(uint32_t), kCheckpointMasterVersion);
    SerializeU64(encoded.data() + (sizeof(uint32_t) * 2), lsn);
    const uint32_t crc =
        Crc32C(encoded.data(), kCheckpointMasterRecordSize - sizeof(uint32_t));
    SerializeU32(
        encoded.data() + kCheckpointMasterRecordSize - sizeof(uint32_t), crc);
    out.write(encoded.data(), encoded.size());
    out.flush();
    if (!out) {
      std::error_code ec;
      std::filesystem::remove(tmp, ec);
      return StatusError(StatusCode::kIOError,
                         "Failed to write master record: " + tmp.string());
    }
    out.close();
    const int fd = ::open(tmp.c_str(), O_RDONLY | O_CLOEXEC);
    int rc = -1;
    int sync_errno = errno;
    if (fd >= 0) {
      do {
        rc = ::fdatasync(fd);
      } while (rc < 0 && errno == EINTR);
      sync_errno = errno;
      ::close(fd);
    }
    if (rc != 0) {
      std::error_code ec;
      std::filesystem::remove(tmp, ec);
      return StatusError(StatusCode::kIOError,
                         "Failed to fsync master record: " + tmp.string() +
                             ": " + std::strerror(sync_errno));
    }
  }
  std::error_code rename_ec;
  std::filesystem::rename(tmp, path, rename_ec);
  if (rename_ec) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return StatusError(StatusCode::kIOError,
                       "Failed to rename master record to " + path.string() +
                           ": " + rename_ec.message());
  }
  return Status::kSuccess;
}

}  // namespace

CheckpointManager::~CheckpointManager() { Stop(); }

void CheckpointManager::Stop() {
  stop_.store(true, std::memory_order_release);
  worker_wait_cv_.notify_all();
  if (checkpoint_worker_.joinable()) {
    checkpoint_worker_.join();
  }
}

void CheckpointManager::WorkerThreadTask() {
  {
    std::unique_lock wait_lock(worker_wait_mu_);
    worker_wait_cv_.wait(wait_lock, [this] {
      return start_.load(std::memory_order_acquire) ||
             stop_.load(std::memory_order_acquire);
    });
  }
  while (!stop_ && interval_seconds_ != 0) {
    for (size_t waited = 0; waited < interval_seconds_ * 10 && !stop_;
         ++waited) {
      std::unique_lock wait_lock(worker_wait_mu_);
      worker_wait_cv_.wait_for(wait_lock, std::chrono::milliseconds(100),
                               [this] { return stop_.load(); });
    }
    if (!stop_) {
      try {
        const auto res = WriteCheckpoint();
        if (!res.HasValue()) {
          LOG(ERROR) << "Periodic checkpoint failed: " << res.GetStatus();
        }
      } catch (const std::exception& error) {
        LOG(ERROR) << "Periodic checkpoint threw: " << error.what();
      } catch (...) {
        LOG(ERROR) << "Periodic checkpoint threw an unknown exception";
      }
    }
  }
}

StatusOr<lsn_t> CheckpointManager::WriteCheckpoint(
    const std::function<void()>& func_for_test) {
  std::scoped_lock checkpoint_guard(checkpoint_mu_);
  if (tm_->FailStopped() || tm_->logger_->Failed()) {
    return StatusError(StatusCode::kIOError,
                       "cannot checkpoint a failed transaction engine");
  }

  lsn_t oldest_active_first_lsn = std::numeric_limits<lsn_t>::max();
  {
    std::scoped_lock lk(tm_->transaction_table_lock);
    for (const auto& it : tm_->active_transactions_) {
      std::scoped_lock chain_guard(it.second->chain_mu_);
      if (it.second->HasLogRecord()) {
        oldest_active_first_lsn =
            std::min(oldest_active_first_lsn, it.second->FirstLSN());
      }
    }
  }

  const LogRecord begin = LogRecord::BeginCheckpointLogRecord();
  ASSIGN_OR_RETURN(lsn_t, begin_lsn, tm_->logger_->AddLog(begin.Serialize()));
  const lsn_t replay_start_lsn = std::min(begin_lsn, oldest_active_first_lsn);

  func_for_test();

  const lsn_t durable_end = tm_->logger_->BufferedLSN();
  RETURN_IF_FAIL(tm_->logger_->WaitForDurable(durable_end));

  if (pm_ != nullptr) {
    RETURN_IF_FAIL(pm_->WriteFuzzyCheckpoint(replay_start_lsn));
  }

  RETURN_IF_FAIL(WriteMasterRecord(master_record_path, begin_lsn));
  tm_->logger_->AdviseOldBytesDurable(replay_start_lsn);
  tm_->logger_->RecycleUpTo(replay_start_lsn);
  return begin_lsn;
}

void CheckpointManager::RemoveStaleMasterTmp() const {
  const std::filesystem::path tmp = master_record_path + ".tmp";
  std::error_code ec;
  if (std::filesystem::exists(tmp, ec) && !ec) {
    std::filesystem::remove(tmp, ec);
  }
}

}  // namespace tinylamb
