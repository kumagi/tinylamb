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

#include "recovery/recovery_manager.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/platform/file_io.hpp"
#include "common/status_or.hpp"
#include "page/page_manager.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

RecoveryManager::RecoveryManager(std::string_view log_path, PageManager* pm,
                                 size_t segment_size)
    : log_name_(log_path), segment_size_(segment_size), pm_(pm) {
  auto format = Logger::FixedSizeSegmentFormat(log_name_);
  fixed_size_segments_ = format.HasValue() && format.Value();
}

RecoveryManager::ReadFd::~ReadFd() {
  if (fd >= 0) {
    platform::CloseFile(fd);
    fd = -1;
  }
}

RecoveryManager::~RecoveryManager() {
  std::scoped_lock lock(read_fd_mutex_);
  segment_fds_.clear();
}

std::shared_ptr<RecoveryManager::ReadFd> RecoveryManager::SegmentFdFor(
    lsn_t lsn) const {
  const size_t index = Logger::SegmentIndex(lsn, segment_size_);
  std::scoped_lock lock(read_fd_mutex_);
  const auto found = segment_fds_.find(index);
  if (found != segment_fds_.end()) {
    return found->second;
  }
  const std::filesystem::path path = Logger::SegmentPath(log_name_, index);
  std::error_code ec;
  const bool present = std::filesystem::exists(path, ec);
  if (ec || !present) {
    return {};
  }
  const int raw_fd =
      platform::OpenFile(path, {.mode = platform::OpenMode::kReadOnly});
  if (raw_fd < 0) {
    return {};
  }
  if (fixed_size_segments_ &&
      (logger_ == nullptr ||
       index < Logger::SegmentIndex(logger_->CommittedLSN(), segment_size_))) {
    const auto logical = Logger::SegmentLogicalSize(
        log_name_, index, segment_size_, TornTailTruncationAllowed());
    if (!logical.HasValue()) {
      platform::CloseFile(raw_fd);
      return {};
    }
  }
  std::shared_ptr<ReadFd> fd;
  try {
    fd = std::make_shared<ReadFd>(raw_fd);
  } catch (...) {
    platform::CloseFile(raw_fd);
    return {};
  }
  if (segment_fds_.size() >= kMaxCachedReadFds) {
    auto oldest = segment_fds_.begin();
    for (auto it = segment_fds_.begin(); it != segment_fds_.end(); ++it) {
      if (it->first < oldest->first) {
        oldest = it;
      }
    }
    segment_fds_.erase(oldest);
  }
  segment_fds_.emplace(index, fd);
  return fd;
}

lsn_t RecoveryManager::LogStart() const {
  const auto status = LogStartStatus();
  return status.HasValue() ? status.Value() : 0;
}

StatusOr<lsn_t> RecoveryManager::LogStartStatus() const {
  bool any = false;
  auto scanned = Logger::FindLowestSegment(log_name_, &any);
  if (!scanned.HasValue()) {
    return scanned.GetStatus();
  }
  if (!any) {
    return lsn_t{0};
  }
  return static_cast<lsn_t>(scanned.Value()) *
         static_cast<lsn_t>(segment_size_);
}

lsn_t RecoveryManager::LogEnd() const {
  const auto status = LogEndStatus();
  return status.HasValue() ? status.Value() : 0;
}

StatusOr<lsn_t> RecoveryManager::LogEndStatus() const {
  if (fixed_size_segments_ && logger_ != nullptr) {
    return logger_->CommittedLSN();
  }
  bool any = false;
  auto scanned = Logger::FindHighestSegment(log_name_, &any);
  if (!scanned.HasValue()) {
    return scanned.GetStatus();
  }
  if (!any) {
    return lsn_t{0};
  }
  const uint64_t highest = scanned.Value();
  const lsn_t base =
      static_cast<lsn_t>(highest) * static_cast<lsn_t>(segment_size_);
  ASSIGN_OR_RETURN(size_t, size,
                   Logger::SegmentLogicalSize(log_name_, highest, segment_size_,
                                              TornTailTruncationAllowed()));
  if (size > std::numeric_limits<lsn_t>::max() - base) {
    return StatusError(StatusCode::kIOError, "WAL end LSN overflows");
  }
  return base + static_cast<lsn_t>(size);
}

RecoveryManager::ReadAtResult RecoveryManager::ReadAtDetailed(
    lsn_t lsn, char* dst, size_t len) const {
  if (len == 0) {
    return {.bytes = 0, .status = ReadAtStatus::kComplete};
  }
  if (logger_ != nullptr && logger_->TryCopyBuffered(lsn, dst, len)) {
    return {.bytes = len, .status = ReadAtStatus::kComplete};
  }
  lsn_t logical_end = std::numeric_limits<lsn_t>::max();
  if (fixed_size_segments_) {
    const auto end = LogEndStatus();
    if (!end.HasValue()) {
      return {.bytes = 0, .status = ReadAtStatus::kUnavailable};
    }
    logical_end = end.Value();
  }
  size_t done = 0;
  while (done < len) {
    const lsn_t absolute = lsn + static_cast<lsn_t>(done);
    if (absolute >= logical_end) {
      return {
          .bytes = done,
          .status = done == 0 ? ReadAtStatus::kEof : ReadAtStatus::kTruncated};
    }
    const auto fd = SegmentFdFor(absolute);
    if (!fd) {
      return {.bytes = done, .status = ReadAtStatus::kUnavailable};
    }
    const auto offset =
        static_cast<size_t>(absolute % static_cast<lsn_t>(segment_size_));
    const auto want = std::min<size_t>(
        {len - done, segment_size_ - offset,
         static_cast<size_t>(std::min<lsn_t>(
             logical_end - absolute, std::numeric_limits<size_t>::max()))});
    int64_t file_size = 0;
    if (platform::FileSize(fd->fd, &file_size) != 0) {
      return {.bytes = done, .status = ReadAtStatus::kUnavailable};
    }
    ssize_t nread = 0;
    do {
      nread = platform::Pread(fd->fd, dst + done, want,
                              static_cast<int64_t>(offset));
    } while (nread < 0 && errno == EINTR);
    if (nread < 0) {
      return {.bytes = done, .status = ReadAtStatus::kUnavailable};
    }
    const auto bytes_read = static_cast<size_t>(std::max<ssize_t>(nread, 0));
    const bool reached_file_end =
        std::cmp_greater_equal(offset + bytes_read, file_size);
    if (nread == 0) {
      if (!reached_file_end) {
        return {.bytes = done, .status = ReadAtStatus::kShort};
      }
      return {
          .bytes = done,
          .status = done == 0 ? ReadAtStatus::kEof : ReadAtStatus::kTruncated};
    }
    done += bytes_read;
    if (bytes_read < want) {
      if (!reached_file_end) {
        return {.bytes = done, .status = ReadAtStatus::kShort};
      }
      if (done >= len) {
        return {.bytes = done, .status = ReadAtStatus::kTruncated};
      }
      const lsn_t next_lsn = lsn + static_cast<lsn_t>(done);
      const auto next = SegmentFdFor(next_lsn);
      const size_t current_index =
          Logger::SegmentIndex(absolute, segment_size_);
      const size_t next_index = Logger::SegmentIndex(next_lsn, segment_size_);
      if (next && next_index != current_index) {
        if (offset + bytes_read == segment_size_ &&
            offset + want == segment_size_) {
          continue;
        }
        return {.bytes = done, .status = ReadAtStatus::kUnavailable};
      }
      return {.bytes = done, .status = ReadAtStatus::kTruncated};
    }
  }
  return {.bytes = done, .status = ReadAtStatus::kComplete};
}

std::optional<lsn_t> RecoveryManager::PreRollPaddingEnd(
    lsn_t lsn, bool* missing_successor) const {
  if (missing_successor != nullptr) {
    *missing_successor = false;
  }
  if (segment_size_ == 0) {
    return std::nullopt;
  }
  const size_t segment = Logger::SegmentIndex(lsn, segment_size_);
  if (fixed_size_segments_) {
    const auto logical =
        Logger::SegmentLogicalSize(log_name_, segment, segment_size_);
    if (!logical.HasValue() || lsn % segment_size_ != logical.Value()) {
      return std::nullopt;
    }
    const lsn_t next = (static_cast<lsn_t>(segment) + 1) * segment_size_;
    if (SegmentFdFor(next)) {
      return next;
    }
    if (missing_successor != nullptr) {
      *missing_successor = true;
    }
    return std::nullopt;
  }
  const lsn_t boundary =
      (static_cast<lsn_t>(segment) + 1) * static_cast<lsn_t>(segment_size_);
  if (lsn >= boundary) {
    return std::nullopt;
  }
  std::array<char, 512> buffer{};
  lsn_t at = lsn;
  while (at < boundary) {
    const size_t want =
        static_cast<size_t>(std::min<lsn_t>(buffer.size(), boundary - at));
    const ReadAtResult read = ReadAtDetailed(at, buffer.data(), want);
    if (read.status != ReadAtStatus::kComplete || read.bytes != want) {
      return std::nullopt;
    }
    for (size_t i = 0; i < read.bytes; ++i) {
      if (buffer[i] != 0) {
        return std::nullopt;
      }
    }
    at += read.bytes;
  }
  if (SegmentFdFor(boundary) == nullptr) {
    // A legacy roll pads the current file before creating its successor.
    // A crash between those writes leaves a zero suffix in the last file,
    // not a hole in the retained WAL. A later file still proves a hole.
    bool any = false;
    const auto highest = Logger::FindHighestSegment(log_name_, &any);
    if (missing_successor != nullptr) {
      *missing_successor =
          !highest.HasValue() || !any || highest.Value() != segment;
    }
    return std::nullopt;
  }
  return boundary;
}

RecoveryManager::LogReadResult RecoveryManager::ReadLogDetailed(
    lsn_t lsn, LogRecord* dst) const {
  constexpr size_t kMinHeader = 8;
  std::array<char, kMinHeader> header{};
  const ReadAtResult header_read =
      ReadAtDetailed(lsn, header.data(), header.size());
  if (header_read.status != ReadAtStatus::kComplete) {
    if (header_read.status == ReadAtStatus::kUnavailable) {
      return {.status = LogReadStatus::kUnavailable};
    }
    if (header_read.status == ReadAtStatus::kEof) {
      return {.status = LogReadStatus::kEof};
    }
    return {.status = header_read.status == ReadAtStatus::kTruncated
                          ? LogReadStatus::kShort
                          : LogReadStatus::kCorrupt};
  }
  uint16_t magic = 0;
  uint16_t version = 0;
  uint32_t total_len = 0;
  std::memcpy(&magic, header.data(), sizeof(uint16_t));
  std::memcpy(&version, header.data() + 2, sizeof(uint16_t));
  std::memcpy(&total_len, header.data() + 4, sizeof(uint32_t));
  if (magic != LogRecord::kLogRecordMagic ||
      version != LogRecord::kCurrentVersion || total_len < 30 ||
      total_len > (64U << 20)) {
    return {.status = LogReadStatus::kCorrupt};
  }

  std::string buffer(total_len, '\0');
  const ReadAtResult read = ReadAtDetailed(lsn, buffer.data(), total_len);
  if (read.status == ReadAtStatus::kUnavailable) {
    return {.status = LogReadStatus::kUnavailable};
  }
  if (read.bytes < total_len) {
    if (read.status == ReadAtStatus::kEof) {
      return {.status = LogReadStatus::kEof};
    }
    return {.status = read.status == ReadAtStatus::kTruncated
                          ? LogReadStatus::kShort
                          : LogReadStatus::kCorrupt};
  }
  const size_t consumed = dst->Deserialize(buffer.data(), buffer.size());
  if (consumed == 0 || consumed != total_len) {
    return {.status = LogReadStatus::kCorrupt};
  }
  return {.status = LogReadStatus::kComplete};
}

bool RecoveryManager::ReadLog(lsn_t lsn, LogRecord* dst) const {
  return ReadLogDetailed(lsn, dst).status == LogReadStatus::kComplete;
}

uint8_t RecoveryManager::DebugReadStatusFor(lsn_t lsn) const {
  LogRecord scratch;
  return static_cast<uint8_t>(ReadLogDetailed(lsn, &scratch).status);
}

lsn_t RecoveryManager::ValidLogEnd(lsn_t from) const {
  const auto status = ValidLogEndStatus(from);
  return status.HasValue() ? status.Value() : 0;
}

StatusOr<lsn_t> RecoveryManager::ValidLogEndStatus(lsn_t from) const {
  ASSIGN_OR_RETURN(lsn_t, filesize, LogEndStatus());
  ASSIGN_OR_RETURN(lsn_t, log_start, LogStartStatus());
  lsn_t offset = std::max(from, log_start);
  LogRecord log;
  while (offset < filesize) {
    const LogReadResult read = ReadLogDetailed(offset, &log);
    if (read.status == LogReadStatus::kUnavailable) {
      return StatusError(StatusCode::kIOError,
                         "WAL read failed at LSN " + std::to_string(offset));
    }
    if (read.status != LogReadStatus::kComplete) {
      bool missing_successor = false;
      if (const auto next = PreRollPaddingEnd(offset, &missing_successor);
          next.has_value()) {
        offset = *next;
        continue;
      }
      if (missing_successor) {
        return StatusError(
            StatusCode::kIOError,
            "missing WAL segment after pre-roll padding at LSN " +
                std::to_string(offset));
      }
      return offset;
    }
    if (log.Size() == 0) {
      return offset;
    }
    offset += log.Size();
  }
  return offset;
}

RecoveryManager::TailDamage RecoveryManager::ClassifyTailDamage(
    lsn_t valid_end, lsn_t on_disk) const {
  if (valid_end >= on_disk) {
    return TailDamage::kNone;
  }
  if (fixed_size_segments_) {
    return TailDamage::kSuspect;
  }
  bool any = false;
  auto scanned = Logger::FindHighestSegment(log_name_, &any);
  if (!scanned.HasValue() || !any ||
      Logger::SegmentIndex(valid_end, segment_size_) != scanned.Value()) {
    return TailDamage::kSuspect;
  }

  constexpr size_t kZeroProbe = 512;
  std::array<char, kZeroProbe> probe{};
  bool all_zero = true;
  for (lsn_t at = valid_end; at < on_disk; at += kZeroProbe) {
    const size_t want =
        static_cast<size_t>(std::min<lsn_t>(kZeroProbe, on_disk - at));
    const ReadAtResult read = ReadAtDetailed(at, probe.data(), want);
    if (read.status == ReadAtStatus::kUnavailable ||
        read.status == ReadAtStatus::kEof ||
        read.status == ReadAtStatus::kShort) {
      return TailDamage::kSuspect;
    }
    if (std::any_of(probe.begin(),
                    probe.begin() + static_cast<ptrdiff_t>(read.bytes),
                    [](char byte) { return byte != 0; })) {
      all_zero = false;
      break;
    }
    if (read.status == ReadAtStatus::kTruncated || read.bytes != want) {
      break;
    }
  }
  if (all_zero) {
    const lsn_t segment_end =
        (static_cast<lsn_t>(Logger::SegmentIndex(valid_end, segment_size_)) +
         1) *
        static_cast<lsn_t>(segment_size_);
    return on_disk > segment_end ? TailDamage::kSuspect : TailDamage::kZeroTail;
  }

  LogRecord probe_record;
  const LogReadResult record_read = ReadLogDetailed(valid_end, &probe_record);
  if (record_read.status == LogReadStatus::kShort) {
    return TailDamage::kTornRecord;
  }
  return TailDamage::kSuspect;
}

Status RecoveryManager::RecoverFrom(lsn_t checkpoint_lsn,
                                    TransactionManager* tm) {
  lsn_t ckpt_replay_lsn = 0;
  bool ckpt_loaded = false;
  if (pm_ != nullptr) {
    RETURN_IF_FAIL(pm_->LoadFuzzyCheckpoint(&ckpt_replay_lsn, &ckpt_loaded));
  }
  ASSIGN_OR_RETURN(lsn_t, on_disk, LogEndStatus());
  if (on_disk == 0) {
    if (checkpoint_lsn != 0 || ckpt_loaded) {
      return StatusError(StatusCode::kCorrupt,
                         "checkpoint exists but WAL is empty: " + log_name_);
    }
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(lsn_t, log_start, LogStartStatus());

  lsn_t anchor_lsn = checkpoint_lsn;
  if (ckpt_loaded) {
    anchor_lsn = anchor_lsn == 0 ? ckpt_replay_lsn
                                 : std::min(anchor_lsn, ckpt_replay_lsn);
  }
  if (anchor_lsn == 0 && log_start > 0) {
    return StatusError(StatusCode::kCorrupt,
                       "WAL prefix was recycled without a checkpoint anchor");
  }
  if (anchor_lsn < log_start) {
    return StatusError(StatusCode::kCorrupt,
                       "checkpoint LSN points into recycled WAL");
  }

  const lsn_t scan_start = std::max(anchor_lsn, log_start);
  ASSIGN_OR_RETURN(lsn_t, valid_end, ValidLogEndStatus(scan_start));
  if (valid_end < on_disk) {
    const TailDamage damage = ClassifyTailDamage(valid_end, on_disk);
    if (!TornTailTruncationAllowed() && damage == TailDamage::kSuspect) {
      return StatusError(StatusCode::kCorrupt, "WAL corruption at offset " +
                                                   std::to_string(valid_end) +
                                                   " of " + log_name_);
    }
    LOG(WARN) << "Truncating torn WAL tail: " << on_disk << " -> " << valid_end;
    if (tm != nullptr && tm->logger_ != nullptr) {
      RETURN_IF_FAIL(tm->logger_->TruncateTo(valid_end));
    }
  }
  const lsn_t filesize = std::min(on_disk, valid_end);

  // Pass 1: discover all committed transactions and maximum transaction ID
  // from scan_start (compacted prefix below scan_start is already persisted).
  std::unordered_set<txn_id_t> committed_txns;
  txn_id_t max_seen_txn = pm_ != nullptr ? pm_->MaxCommittedTxnId() : 0;
  {
    lsn_t offset = scan_start;
    LogRecord log;
    while (offset < filesize) {
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status != LogReadStatus::kComplete) {
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        break;
      }
      if (log.type != LogType::kBeginCheckpoint &&
          log.type != LogType::kEndCheckpoint) {
        max_seen_txn = std::max(max_seen_txn, log.owner);
      }
      if (log.type == LogType::kCommit) {
        committed_txns.insert(log.owner);
      }
      offset += log.Size();
    }
  }

  if (tm != nullptr && max_seen_txn != std::numeric_limits<txn_id_t>::max()) {
    tm->SeedNextTransactionId(max_seen_txn + 1);
  }

  // Pass 2: replay only committed transactions from scan_start.
  if (pm_ != nullptr) {
    lsn_t offset = scan_start;
    LogRecord log;
    while (offset < filesize) {
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status != LogReadStatus::kComplete) {
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (committed_txns.contains(log.owner)) {
        RETURN_IF_FAIL(pm_->ReplayCommittedRecord(offset, log));
      }
      offset += log.Size();
    }
  }

  return Status::kSuccess;
}

}  // namespace tinylamb
