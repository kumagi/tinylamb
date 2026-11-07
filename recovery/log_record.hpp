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

#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "page/page_type.hpp"

namespace tinylamb {

enum class LogType : uint8_t {
  kUnknown = 0,
  kBegin,
  kInsertRow,
  kUpdateRow,
  kDeleteRow,
  kCompensateUpdate,
  kSetRowPageNext,
  kSetRowPagePrev,
  kSystemAllocPage,
  kSystemDestroyPage,
  kLsmPut,
  kLsmDelete,
  kLsmDestroy,
  kCommit,
  kAbort,
  kBeginCheckpoint,
  kEndCheckpoint,
};

// Logical LSM & block mutation record for WAL persistence and fuzzy recovery.
struct LogRecord {
  static constexpr uint16_t kLogRecordMagic = 0x4C53;  // "LS"
  static constexpr uint16_t kCurrentVersion = 7;

  LogRecord() = default;
  LogRecord(lsn_t prev, txn_id_t txn, LogType log_type);

  [[nodiscard]] static LogRecord BeginLogRecord(lsn_t prev, txn_id_t txn) {
    return {prev, txn, LogType::kBegin};
  }
  [[nodiscard]] static LogRecord CommitLogRecord(lsn_t prev, txn_id_t txn) {
    return {prev, txn, LogType::kCommit};
  }
  [[nodiscard]] static LogRecord AbortLogRecord(lsn_t prev, txn_id_t txn) {
    return {prev, txn, LogType::kAbort};
  }
  [[nodiscard]] static LogRecord BeginCheckpointLogRecord() {
    return {0, 0, LogType::kBeginCheckpoint};
  }
  [[nodiscard]] static LogRecord InsertingLogRecord(lsn_t prev, txn_id_t txn,
                                                    page_id_t pid, slot_t slot,
                                                    std::string_view redo);
  [[nodiscard]] static LogRecord UpdatingLogRecord(lsn_t prev, txn_id_t txn,
                                                   page_id_t pid, slot_t slot,
                                                   std::string_view redo,
                                                   std::string_view undo);
  [[nodiscard]] static LogRecord CompensateUpdatingLogRecord(
      lsn_t prev, txn_id_t txn, page_id_t pid, slot_t slot,
      std::string_view redo);
  [[nodiscard]] static LogRecord DeletingLogRecord(lsn_t prev, txn_id_t txn,
                                                   page_id_t pid, slot_t slot,
                                                   std::string_view undo);
  [[nodiscard]] static LogRecord SetRowPageNextLogRecord(
      lsn_t prev, txn_id_t txn, page_id_t pid, page_id_t redo, page_id_t undo);
  [[nodiscard]] static LogRecord SetRowPagePrevLogRecord(
      lsn_t prev, txn_id_t txn, page_id_t pid, page_id_t redo, page_id_t undo);
  [[nodiscard]] static LogRecord AllocatePageLogRecord(lsn_t prev, txn_id_t txn,
                                                       page_id_t page_id,
                                                       PageType new_page_type);
  [[nodiscard]] static LogRecord DestroyPageLogRecord(lsn_t prev, txn_id_t txn,
                                                      page_id_t page_id);
  [[nodiscard]] static LogRecord LsmPutLogRecord(
      lsn_t prev, txn_id_t txn, page_id_t tree_id, std::string_view key,
      std::string_view value, std::string_view prev_value = {},
      bool had_prev = false);
  [[nodiscard]] static LogRecord LsmDeleteLogRecord(
      lsn_t prev, txn_id_t txn, page_id_t tree_id, std::string_view key,
      std::string_view prev_value = {});
  [[nodiscard]] static LogRecord LsmDestroyLogRecord(lsn_t prev, txn_id_t txn,
                                                     page_id_t tree_id);

  [[nodiscard]] size_t Size() const;
  [[nodiscard]] std::string Serialize() const;
  void SerializeTo(std::string* dst) const;
  void SerializeTo(std::string& dst) const { SerializeTo(&dst); }
  size_t Deserialize(const char* src,
                     size_t max_bytes = std::numeric_limits<size_t>::max());

  bool operator==(const LogRecord& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& o, const LogRecord& r);

  lsn_t record_lsn{0};
  lsn_t prev_lsn{0};
  txn_id_t owner{0};
  LogType type{LogType::kUnknown};
  page_id_t pid{std::numeric_limits<page_id_t>::max()};
  slot_t slot{std::numeric_limits<slot_t>::max()};
  bool lsm_had_prev{false};
  std::string key{};
  std::string redo_data{};
  std::string undo_data{};
  page_id_t redo_pid{std::numeric_limits<page_id_t>::max()};
  page_id_t undo_pid{std::numeric_limits<page_id_t>::max()};
  PageType allocated_page_type{PageType::kUnknown};
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
