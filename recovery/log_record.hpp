#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

#include <cassert>
#include <page/page.hpp>
#include <unordered_set>
#include <utility>

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

  LogRecord(uint64_t p, uint64_t txn, LogType t);

  static bool ParseLogRecord(std::string_view src, LogRecord* dst);

  static LogRecord InsertingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                      std::string_view r);

  static LogRecord CompensatingInsertLogRecord(uint16_t p,
                                               uint64_t undo_next_lsn,
                                               uint64_t txn, RowPosition po);

  static LogRecord UpdatingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                     std::string_view redo,
                                     std::string_view undo);

  static LogRecord CompensatingUpdateLogRecord(uint16_t p,
                                               uint64_t undo_next_lsn,
                                               uint64_t txn, RowPosition po,
                                               std::string_view redo);

  static LogRecord DeletingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                     std::string_view undo);

  static LogRecord CompensatingDeleteLogRecord(uint16_t p,
                                               uint64_t undo_next_lsn,
                                               uint64_t txn, RowPosition po,
                                               std::string_view redo);

  static LogRecord CheckpointLogRecord(uint64_t p, uint64_t txn,
                                       std::unordered_set<uint64_t> dpt,
                                       std::unordered_set<uint64_t> tt);

  static LogRecord AllocatePageLogRecord(uint64_t p, uint64_t txn, uint64_t pid,
                                         PageType new_page_type);

  static LogRecord DestroyPageLogRecord(uint64_t p, uint64_t txn, uint64_t pid);

  size_t Size() const;

  [[nodiscard]] std::string Serialize() const;

  bool operator==(const LogRecord& rhs) const;

  friend std::ostream& operator<<(std::ostream& o, const LogRecord& l) {
    o << l.type;
    switch (l.type) {
      case LogType::kInsertRow:
        o << "\t\t" << l.redo_data.size() << "bytes\tpos: " << l.pos;
        break;
      case LogType::kUpdateRow:
        o << "\t\t" << l.undo_data.size() << " -> " << l.redo_data.size()
          << "bytes\tpos: " << l.pos;
        break;
      case LogType::kDeleteRow:
        o << "\t\t" << l.undo_data.size() << "bytes\tpos: " << l.pos;
        break;
      case LogType::kCompensateInsertRow:
        o << "\t\tUndoNextLSN: " << l.undo_next_lsn << "\tpos: " << l.pos;
        break;
      case LogType::kCompensateUpdateRow:
        o << "\t\tUndoNextLSN: \" << l.undo_next_lsn << \"pos: " << l.pos
          << l.redo_data.size() << "bytes";
        break;
      case LogType::kCompensateDeleteRow:
        o << "\t\tUndoNextLSN: \" << l.undo_next_lsn << \"pos: " << l.pos
          << l.redo_data.size() << "bytes";
        break;
      case LogType::kSystemAllocPage:
        o << "\t" << l.allocated_page_id;
        break;
      case LogType::kSystemDestroyPage:
        o << "\t" << l.destroy_page_id;
        break;
      case LogType::kBegin:
      case LogType::kCommit:
        o << "\t";
      case LogType::kBeginCheckpoint:
        break;
      case LogType::kEndCheckpoint:
        o << "\tdpt: {";
        for (const auto& dpt : l.dirty_page_table) {
          o << dpt << ", ";
        }
        o << "}\ttt: {";
        for (const auto& tt : l.transaction_table) {
          o << tt << ", ";
        }
        o << "}";
        break;
      case LogType::kUnknown:
        LOG(ERROR) << "kUnknownLog";
        break;
    }
    o << "\tprev_lsn: " << l.prev_lsn
      << "\ttxn_id: " << l.txn_id << "\tundo: " << l.undo_data.size()
      << " \tredo: " << l.redo_data.size();
    return o;
  }

  // Log size in bytes.
  LogType type = LogType::kUnknown;
  uint64_t prev_lsn = 0;
  uint64_t undo_next_lsn = 0;  // Only used for CLR.
  uint64_t txn_id = 0;
  RowPosition pos = RowPosition();
  std::string_view undo_data{};
  std::string_view redo_data{};
  std::unordered_set<uint64_t> dirty_page_table{};
  std::unordered_set<uint64_t> transaction_table{};

  // Page alloc/destroy target.b
  uint64_t allocated_page_id = 0;
  PageType allocated_page_type = PageType::kUnknown;
  uint64_t destroy_page_id = 0;

  uint16_t length = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
