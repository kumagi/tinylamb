#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

#include <cassert>
#include <unordered_set>
#include <utility>

#include "checkpoint_manager.hpp"
#include "page/page.hpp"

namespace tinylamb {

enum class LogType : uint16_t {
  kUnknown,
  kBegin,
  kInsertRow,
  kInsertLeaf,
  kInsertInternal,
  kUpdateRow,
  kUpdateLeaf,
  kUpdateInternal,
  kDeleteRow,
  kDeleteLeaf,
  kDeleteInternal,
  kCompensateInsertRow,
  kCompensateInsertLeaf,
  kCompensateInsertInternal,
  kCompensateUpdateRow,
  kCompensateUpdateLeaf,
  kCompensateUpdateInternal,
  kCompensateDeleteRow,
  kCompensateDeleteLeaf,
  kCompensateDeleteInternal,
  kCommit,
  kBeginCheckpoint,
  kEndCheckpoint,
  kSystemAllocPage,
  kSystemDestroyPage,
  kLowestValue,
};

std::ostream& operator<<(std::ostream& o, const LogType& type);

struct LogRecord {
  LogRecord() = default;

  LogRecord(lsn_t p, txn_id_t txn, LogType t);

  [[nodiscard]] bool HasSlot() const {
    return slot != std::numeric_limits<uint16_t>::max();
  }
  [[nodiscard]] bool HasPageID() const {
    return pid != std::numeric_limits<page_id_t>::max();
  }

  static bool ParseLogRecord(const char* src, LogRecord* dst);

  static LogRecord InsertingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                      uint16_t key, std::string_view redo);
  static LogRecord InsertingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                          std::string_view key,
                                          std::string_view redo);
  static LogRecord InsertingInternalLogRecord(lsn_t p, txn_id_t txn,
                                              page_id_t pid,
                                              std::string_view key,
                                              page_id_t redo);

  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                               uint16_t key);
  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                               std::string_view key);
  static LogRecord CompensatingInsertInternalLogRecord(txn_id_t txn,
                                                       page_id_t pid,
                                                       std::string_view key);

  static LogRecord UpdatingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                     uint16_t key, std::string_view redo,
                                     std::string_view undo);
  static LogRecord UpdatingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                         std::string_view key,
                                         std::string_view redo,
                                         std::string_view undo);
  static LogRecord UpdatingInternalLogRecord(lsn_t p, txn_id_t txn,
                                             page_id_t pid,
                                             std::string_view key,
                                             page_id_t redo, page_id_t undo);

  static LogRecord CompensatingUpdateLogRecord(lsn_t txn, page_id_t pid,
                                               uint16_t key,
                                               std::string_view redo);
  static LogRecord CompensatingUpdateLeafLogRecord(lsn_t txn, page_id_t pid,
                                                   std::string_view key,
                                                   std::string_view redo);
  static LogRecord CompensatingUpdateInternalLogRecord(lsn_t txn, page_id_t pid,
                                                       std::string_view key,
                                                       page_id_t redo);

  static LogRecord DeletingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                     uint16_t key, std::string_view undo);
  static LogRecord DeletingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                         std::string_view key,
                                         std::string_view undo);
  static LogRecord DeletingInternalLogRecord(lsn_t p, txn_id_t txn,
                                             page_id_t pid,
                                             std::string_view key,
                                             uint16_t undo);

  static LogRecord CompensatingDeleteLogRecord(txn_id_t txn, page_id_t pid,
                                               uint16_t slot,
                                               std::string_view redo);
  static LogRecord CompensatingDeleteLeafLogRecord(txn_id_t txn, page_id_t pid,
                                                   std::string_view key,
                                                   std::string_view redo);
  static LogRecord CompensatingDeleteInternalLogRecord(txn_id_t txn,
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

  void Clear();

  [[nodiscard]] size_t Size() const;

  [[nodiscard]] std::string Serialize() const;

  bool operator==(const LogRecord& rhs) const;

  void DumpPosition(std::ostream& o) const;

  friend std::ostream& operator<<(std::ostream& o, const LogRecord& l) {
    o << l.type;
    switch (l.type) {
      case LogType::kUnknown:
        LOG(ERROR) << "kUnknownLog";
        break;
      case LogType::kCompensateUpdateRow:
      case LogType::kCompensateDeleteRow:
      case LogType::kInsertRow:
      case LogType::kInsertLeaf:
        l.DumpPosition(o);
        o << "\t\tRedo: " << l.redo_data.size() << " bytes ";
        break;
      case LogType::kUpdateRow:
        l.DumpPosition(o);
        o << "\t\t" << l.undo_data.size() << " -> " << l.redo_data.size()
          << "bytes ";
        break;
      case LogType::kDeleteRow:
        l.DumpPosition(o);
        o << "\t\t" << l.undo_data.size() << " bytes ";
        break;
      case LogType::kCompensateInsertRow:
        l.DumpPosition(o);
        break;
      case LogType::kBegin:
      case LogType::kCommit:
      case LogType::kSystemAllocPage:
      case LogType::kSystemDestroyPage:
        break;
      case LogType::kBeginCheckpoint:
        return o;
      case LogType::kEndCheckpoint:
        o << "\tdpt: {";
        for (const auto& dpt : l.dirty_page_table) {
          o << dpt.first << ": " << dpt.second << ", ";
        }
        o << "}\ttt: {";
        for (const auto& tt : l.active_transaction_table) {
          o << tt << ", ";
        }
        o << "}";
        return o;
      case LogType::kInsertInternal:
        l.DumpPosition(o);
        o << "\t\t"
          << "Insert: " << l.key << " -> " << l.redo_page << " ";
        break;
      case LogType::kUpdateLeaf:
      case LogType::kDeleteLeaf:
      case LogType::kCompensateInsertLeaf:
      case LogType::kCompensateUpdateLeaf:
      case LogType::kCompensateDeleteLeaf:
      case LogType::kUpdateInternal:
      case LogType::kDeleteInternal:
      case LogType::kCompensateInsertInternal:
      case LogType::kCompensateUpdateInternal:
      case LogType::kCompensateDeleteInternal:
        o << "\t";
        l.DumpPosition(o);
        break;
      case LogType::kLowestValue:
        o << "\t"
          << "Lowest: " << l.redo_page;
        break;
    }
    o << "\tprev_lsn: " << l.prev_lsn << "\ttxn_id: " << l.txn_id;
    return o;
  }

  // Log size in bytes.
  LogType type = LogType::kUnknown;
  lsn_t prev_lsn = 0;
  txn_id_t txn_id = 0;
  page_id_t pid = std::numeric_limits<page_id_t>::max();
  uint16_t slot = std::numeric_limits<uint16_t>::max();
  std::string_view key{};
  std::string_view undo_data{};
  std::string_view redo_data{};
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
