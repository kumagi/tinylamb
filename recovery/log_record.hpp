#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

#include <cassert>
#include <unordered_set>
#include <utility>

#include "checkpoint_manager.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "page/page.hpp"

namespace tinylamb {

enum class LogType : uint16_t {
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
  kCompensateInsertRow,
  kCompensateInsertLeaf,
  kCompensateInsertBranch,
  kCompensateUpdateRow,
  kCompensateUpdateLeaf,
  kCompensateUpdateBranch,
  kCompensateDeleteRow,
  kCompensateDeleteLeaf,
  kCompensateDeleteBranch,
  kCommit,
  kBeginCheckpoint,
  kEndCheckpoint,
  kSystemAllocPage,
  kSystemDestroyPage,
  kLowestValue,
  kSetPrevNext,
};
inline std::istream& operator>>(std::istream& in, const LogType& val) {
  in >> (uint16_t&)val;
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

  static LogRecord InsertingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                      slot_t key, std::string_view redo);
  static LogRecord InsertingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                          std::string_view key,
                                          std::string_view redo);
  static LogRecord InsertingBranchLogRecord(lsn_t p, txn_id_t txn,
                                            page_id_t pid, std::string_view key,
                                            page_id_t redo);

  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                               slot_t key);
  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                               std::string_view key);
  static LogRecord CompensatingInsertBranchLogRecord(txn_id_t txn,
                                                     page_id_t pid,
                                                     std::string_view key);

  static LogRecord UpdatingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                     slot_t key, std::string_view redo,
                                     std::string_view undo);
  static LogRecord UpdatingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                         std::string_view key,
                                         std::string_view redo,
                                         std::string_view undo);
  static LogRecord UpdatingBranchLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                           std::string_view key, page_id_t redo,
                                           page_id_t undo);

  static LogRecord CompensatingUpdateLogRecord(lsn_t txn, page_id_t pid,
                                               slot_t key,
                                               std::string_view redo);
  static LogRecord CompensatingUpdateLeafLogRecord(lsn_t txn, page_id_t pid,
                                                   std::string_view key,
                                                   std::string_view redo);
  static LogRecord CompensatingUpdateBranchLogRecord(lsn_t txn, page_id_t pid,
                                                     std::string_view key,
                                                     page_id_t redo);

  static LogRecord DeletingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                     slot_t slot, std::string_view undo);
  static LogRecord DeletingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                         std::string_view key,
                                         std::string_view undo);
  static LogRecord DeletingBranchLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                           std::string_view key,
                                           page_id_t undo);

  static LogRecord CompensatingDeleteLogRecord(txn_id_t txn, page_id_t pid,
                                               slot_t slot,
                                               std::string_view redo);
  static LogRecord CompensatingDeleteLeafLogRecord(txn_id_t txn, page_id_t pid,
                                                   std::string_view key,
                                                   std::string_view redo);
  static LogRecord ComnensatingDeleteBranchLogRecord(txn_id_t txn,
                                                     page_id_t pid,
                                                     std::string_view slot,
                                                     page_id_t redo);

  static LogRecord SetLowestLogRecord(lsn_t p, txn_id_t tid, page_id_t pid,
                                      page_id_t lowest_value);

  static LogRecord AllocatePageLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                         PageType new_page_type);

  static LogRecord DestroyPageLogRecord(lsn_t p, txn_id_t txn, page_id_t pid);

  static LogRecord BeginCheckpointLogRecord();

  static LogRecord EndCheckpointLogRecord(
      const std::vector<std::pair<page_id_t, lsn_t>>& dpt,
      const std::vector<CheckpointManager::ActiveTransactionEntry>& att);

  static LogRecord SetPrevNextLogRecord(lsn_t prev_lsn, txn_id_t tid,
                                        page_id_t pid, page_id_t undo_prev,
                                        page_id_t undo_next,
                                        page_id_t redo_prev,
                                        page_id_t redo_next);
  void PrevNextLogRecordRedo(page_id_t& prev, page_id_t& next) const;
  void PrevNextLogRecordUndo(page_id_t& prev, page_id_t& next) const;

  void Clear();

  [[nodiscard]] size_t Size() const;

  friend Encoder& operator<<(Encoder& e, const LogRecord& l);
  friend Decoder& operator>>(Decoder& d, LogRecord& l);
  [[nodiscard]] std::string Serialize() const;

  bool operator==(const LogRecord& r) const;

  void DumpPosition(std::ostream& o) const;

  friend std::ostream& operator<<(std::ostream& o, const LogRecord& l);

  // Log size in bytes.
  LogType type = LogType::kUnknown;
  lsn_t prev_lsn = 0;
  txn_id_t txn_id = 0;
  page_id_t pid = std::numeric_limits<page_id_t>::max();
  slot_t slot = std::numeric_limits<slot_t>::max();
  std::string key{};
  std::string undo_data{};
  std::string redo_data{};
  page_id_t redo_page = 0;
  page_id_t undo_page = 0;
  std::vector<std::pair<page_id_t, lsn_t>> dirty_page_table{};
  std::vector<CheckpointManager::ActiveTransactionEntry>
      active_transaction_table{};
  // Page alloc/destroy target.b
  PageType allocated_page_type = PageType::kUnknown;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
