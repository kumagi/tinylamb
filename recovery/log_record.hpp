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
#include <istream>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "checkpoint_manager.hpp"
#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "page/foster_pair.hpp"
#include "page/index_key.hpp"
#include "page/page_type.hpp"

namespace tinylamb {

// WAL record format version (docs/design.md D3, D9).
// v1 (== kSerdesVersion): kSystemDestroyPage carried only the page id and
//     records ended after their payload (no CRC).
// v2: kSystemDestroyPage additionally carries the destroyed page's type and
//     an optional full body image so undo restores the page exactly.
// v3 (D9): every new record ends with a CRC32C over all preceding record
//     bytes; readers verify it and treat a mismatch as the valid log end.
// Readers must keep accepting v1/v2 (no CRC); writers always emit the
// current version.
inline constexpr uint32_t kWalRecordVersion = 3U;
inline constexpr uint32_t kLegacyWalRecordVersion = 1U;
// First wire version whose kSystemDestroyPage carries type + body (D3).
inline constexpr uint32_t kDestroyPayloadWalVersion = 2U;
// D9: the CRC field itself is not covered by the checksum.
inline constexpr size_t kWalRecordCrcSize = sizeof(uint32_t);

// The 16-bit base is part of the WAL record format: records serialize the
// type as a raw uint16_t (see the Decoder/Encoder operators below), so the
// narrower base suggested by performance-enum-size must not be applied.
enum class LogType : uint16_t {  // NOLINT(performance-enum-size)
  kUnknown,
  kBegin,
  kInsertRow,
  kInsertLeaf,
  kInsertBranch,
  kUpdateRow,
  kUpdateLeaf,
  kUpdateBranch,
  kDeleteRow,
  kDeleteLeaf,
  kDeleteBranch,
  kSetLowFence,
  kSetHighFence,
  kSetFoster,
  kCompensateInsertRow,
  kCompensateInsertLeaf,
  kCompensateInsertBranch,
  kCompensateUpdateRow,
  kCompensateUpdateLeaf,
  kCompensateUpdateBranch,
  kCompensateDeleteRow,
  kCompensateDeleteLeaf,
  kCompensateDeleteBranch,
  kCompensateSetLowFence,
  kCompensateSetHighFence,
  kCompensateSetFoster,
  kCommit,
  kBeginCheckpoint,
  kEndCheckpoint,
  kSystemAllocPage,
  kSystemDestroyPage,
  kLowestValue,
};
inline std::istream& operator>>(std::istream& in, LogType& val) {
  uint16_t raw = 0;
  in >> raw;
  val = static_cast<LogType>(raw);
  return in;
}

std::ostream& operator<<(std::ostream& o, const LogType& type);

struct LogRecord {
  LogRecord() = default;

  LogRecord(lsn_t p, txn_id_t txn, LogType t);

  [[nodiscard]] bool HasSlot() const {
    return slot != std::numeric_limits<slot_t>::max();
  }
  [[nodiscard]] bool HasPageID() const {
    return pid != std::numeric_limits<page_id_t>::max();
  }

  static LogRecord InsertingLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                      page_id_t pid, slot_t key,
                                      std::string_view redo);
  static LogRecord InsertingLeafLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                          page_id_t pid, std::string_view key,
                                          std::string_view redo);
  static LogRecord InsertingBranchLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                            page_id_t pid, std::string_view key,
                                            page_id_t redo);

  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                               slot_t key);
  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                               std::string_view key);
  static LogRecord CompensatingInsertBranchLogRecord(txn_id_t txn,
                                                     page_id_t pid,
                                                     std::string_view key);

  static LogRecord UpdatingLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                     page_id_t pid, slot_t key,
                                     std::string_view redo,
                                     std::string_view undo);
  static LogRecord UpdatingLeafLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                         page_id_t pid, std::string_view key,
                                         std::string_view redo,
                                         std::string_view undo);
  static LogRecord UpdatingBranchLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                           page_id_t pid, std::string_view key,
                                           page_id_t redo, page_id_t undo);

  static LogRecord CompensatingUpdateLogRecord(txn_id_t txn, page_id_t pid,
                                               slot_t key,
                                               std::string_view redo);
  static LogRecord CompensatingUpdateLeafLogRecord(lsn_t txn, page_id_t pid,
                                                   std::string_view key,
                                                   std::string_view redo);
  static LogRecord CompensatingUpdateBranchLogRecord(lsn_t txn, page_id_t pid,
                                                     std::string_view key,
                                                     page_id_t redo);

  static LogRecord DeletingLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                     page_id_t pid, slot_t slot,
                                     std::string_view undo);
  static LogRecord DeletingLeafLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                         page_id_t pid, std::string_view key,
                                         std::string_view undo);
  static LogRecord DeletingBranchLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                           page_id_t pid, std::string_view key,
                                           page_id_t undo);

  static LogRecord CompensatingDeleteLogRecord(txn_id_t txn, page_id_t pid,
                                               slot_t slot,
                                               std::string_view redo);
  static LogRecord CompensatingDeleteLeafLogRecord(txn_id_t txn, page_id_t pid,
                                                   std::string_view key,
                                                   std::string_view redo);
  static LogRecord CompensatingDeleteBranchLogRecord(txn_id_t txn,
                                                     page_id_t pid,
                                                     std::string_view slot,
                                                     page_id_t redo);

  static LogRecord SetLowFenceLogRecord(lsn_t prev_lsn, txn_id_t tx,
                                        page_id_t pid, const IndexKey& redo,
                                        const IndexKey& undo);
  static LogRecord SetHighFenceLogRecord(lsn_t prev_lsn, txn_id_t tx,
                                         page_id_t pid, const IndexKey& redo,
                                         const IndexKey& undo);
  static LogRecord CompensateSetLowFenceLogRecord(lsn_t prev_lsn, txn_id_t tx,
                                                  page_id_t pid,
                                                  const IndexKey& redo);
  static LogRecord CompensateSetHighFenceLogRecord(lsn_t prev_lsn, txn_id_t tx,
                                                   page_id_t pid,
                                                   const IndexKey& redo);
  static LogRecord SetFosterLogRecord(lsn_t prev_lsn, txn_id_t tx,
                                      page_id_t pid, const FosterPair& redo,
                                      const FosterPair& undo);
  static LogRecord CompensateSetFosterLogRecord(lsn_t prev_lsn, txn_id_t tx,
                                                page_id_t pid,
                                                const FosterPair& redo);

  static LogRecord SetLowestLogRecord(lsn_t prev_lsn, txn_id_t tid,
                                      page_id_t pid, page_id_t redo,
                                      page_id_t undo);
  static LogRecord CompensateSetLowestValueLogRecord(txn_id_t tid,
                                                     page_id_t pid,
                                                     page_id_t redo);

  static LogRecord AllocatePageLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                         page_id_t pid, PageType new_page_type);

  // D3 (docs/design.md): the destroy record carries the destroyed page's
  // type and (unless provably all-zero) its body image so that undo can
  // restore an aborted destroy exactly; redo re-initializes it as kFreePage.
  static LogRecord DestroyPageLogRecord(
      lsn_t prev_lsn, txn_id_t txn, page_id_t pid,
      PageType old_page_type = PageType::kUnknown,
      std::string old_page_body = {});

  static LogRecord BeginCheckpointLogRecord();

  static LogRecord EndCheckpointLogRecord(
      const std::vector<std::pair<page_id_t, lsn_t>>& dpt,
      const std::vector<CheckpointManager::ActiveTransactionEntry>& att);

  void Clear();

  [[nodiscard]] size_t Size() const;

  friend Encoder& operator<<(Encoder& e, const LogRecord& l);
  friend Decoder& operator>>(Decoder& d, LogRecord& l);
  [[nodiscard]] std::string Serialize() const;

  bool operator==(const LogRecord& r) const = default;

  void DumpPosition(std::ostream& o) const;

  friend std::ostream& operator<<(std::ostream& o, const LogRecord& l);

  // Log size in bytes.
  LogType type = LogType::kUnknown;
  lsn_t prev_lsn = 0;
  txn_id_t txn_id = 0;
  page_id_t pid = std::numeric_limits<page_id_t>::max();
  slot_t slot = std::numeric_limits<slot_t>::max();
  std::string key;
  std::string undo_data;
  std::string redo_data;
  page_id_t redo_page = 0;
  page_id_t undo_page = 0;
  std::vector<std::pair<page_id_t, lsn_t>> dirty_page_table;
  std::vector<CheckpointManager::ActiveTransactionEntry>
      active_transaction_table;
  // Page alloc/destroy target.b
  PageType allocated_page_type = PageType::kUnknown;
  // WAL wire format that produced (or will receive) this record.  Written
  // records always use kWalRecordVersion; decoded records keep the version
  // read from the header so Size() matches the on-disk byte count for both
  // v1 and v2 destroy records.
  uint32_t wire_version = kWalRecordVersion;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
