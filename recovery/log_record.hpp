#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

#include <cassert>
#include <page/page.hpp>
#include <unordered_set>
#include <utility>

#include "checkpoint_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

enum class LogType : uint16_t {
  kUnknown,
  kBegin,
  kInsertRow,
  kUpdateRow,
  kDeleteRow,
  kCompensateInsertRow,
  kCompensateUpdateRow,
  kCompensateDeleteRow,
  kCommit,
  kBeginCheckpoint,
  kEndCheckpoint,
  kSystemAllocPage,
  kSystemDestroyPage,
};

std::ostream& operator<<(std::ostream& o, const LogType& type);

struct LogRecord {
  LogRecord() = default;

  LogRecord(lsn_t p, txn_id_t txn, LogType t);

  static bool ParseLogRecord(const char* src, LogRecord* dst);

  static LogRecord InsertingLogRecord(lsn_t p, txn_id_t txn, RowPosition po,
                                      std::string_view r);

  static LogRecord CompensatingInsertLogRecord(txn_id_t txn, RowPosition po);

  static LogRecord UpdatingLogRecord(lsn_t p, txn_id_t txn, RowPosition po,
                                     std::string_view redo,
                                     std::string_view undo);

  static LogRecord CompensatingUpdateLogRecord(lsn_t txn, RowPosition po,
                                               std::string_view redo);

  static LogRecord DeletingLogRecord(lsn_t p, txn_id_t txn, RowPosition po,
                                     std::string_view undo);

  static LogRecord CompensatingDeleteLogRecord(txn_id_t txn, RowPosition po,
                                               std::string_view redo);

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

  friend std::ostream& operator<<(std::ostream& o, const LogRecord& l) {
    o << l.type;
    switch (l.type) {
      case LogType::kCompensateUpdateRow:
      case LogType::kCompensateDeleteRow:
      case LogType::kInsertRow:
        o << "\t\t" << l.redo_data.size() << " bytes\tpos: " << l.pos;
        break;
      case LogType::kUpdateRow:
        o << "\t\t" << l.undo_data.size() << " -> " << l.redo_data.size()
          << "bytes\tpos: " << l.pos;
        break;
      case LogType::kDeleteRow:
        o << "\t\t" << l.undo_data.size() << " bytes\tpos: " << l.pos;
        break;
      case LogType::kCompensateInsertRow:
        o << "\t\t" << l.pos;
        break;
      case LogType::kSystemAllocPage:
      case LogType::kSystemDestroyPage:
        o << "\t" << l.pos.page_id;
        break;
      case LogType::kBegin:
      case LogType::kCommit:
        o << "\t";
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
      case LogType::kUnknown:
        LOG(ERROR) << "kUnknownLog";
        break;
    }
    o << "\tprev_lsn: " << l.prev_lsn << "\ttxn_id: " << l.txn_id;
    return o;
  }

  // Log size in bytes.
  LogType type = LogType::kUnknown;
  lsn_t prev_lsn = 0;
  txn_id_t txn_id = 0;
  RowPosition pos = RowPosition();
  std::string_view undo_data{};
  std::string_view redo_data{};
  std::vector<std::pair<page_id_t, lsn_t>> dirty_page_table{};
  std::vector<CheckpointManager::ActiveTransactionEntry>
      active_transaction_table{};

  // Page alloc/destroy target.b
  PageType allocated_page_type = PageType::kUnknown;

  uint32_t length = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
