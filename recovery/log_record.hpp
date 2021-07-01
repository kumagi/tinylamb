#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

#include <cassert>
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
  kCommit,
  kBeginCheckpoint,
  kEndCheckpoint,
  kSystemAllocPage,
  kSystemDestroyPage,
};

struct LogRecord {
 private:
  LogRecord() = default;

 public:
  LogRecord(uint64_t p, uint64_t txn, LogType t);

  static LogRecord InsertingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                      std::string_view r);

  static LogRecord UpdatingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                     std::string_view redo,
                                     std::string_view undo);

  static LogRecord DeletingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                     std::string_view undo);

  static LogRecord CheckpointLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                       std::unordered_set<uint64_t> dpt,
                                       std::unordered_set<uint64_t> tt);

  static LogRecord AllocatePageLogRecord(uint64_t p, uint64_t txn,
                                         uint64_t pid);

  static LogRecord DestroyPageLogRecord(uint64_t p, uint64_t txn, uint64_t pid);

  void SetLSN(uint64_t new_lsn);

  size_t Size() const;

  [[nodiscard]] std::string Serialize() const;

  friend std::ostream& operator<<(std::ostream& o, const LogRecord& l) {
    o << "type: ";
    switch (l.type) {
      case LogType::kUnknown:
        o << "(unknown) ";
        break;
      case LogType::kBegin:
        o << "BEGIN ";
        break;
      case LogType::kInsertRow:
        o << "INSERT " << l.redo_data << " ";
        break;
      case LogType::kUpdateRow:
        o << "UPDATE " << l.undo_data << " -> " << l.redo_data << " ";
        break;
      case LogType::kDeleteRow:
        o << "DELETE " << l.undo_data << " ";
        break;
      case LogType::kCommit:
        o << "COMMIT ";
        break;
      case LogType::kBeginCheckpoint:
        o << "BEGIN CHECKPOINT ";
        break;
      case LogType::kEndCheckpoint:
        o << "END CHECKPOINT ";
        break;
      case LogType::kSystemAllocPage:
        o << "ALLOCATE PAGE " << l.allocate_page_id << " ";
        break;
      case LogType::kSystemDestroyPage:
        o << "DESTROY PAGE " << l.destroy_page_id << " ";
        break;
    }
    o << " lsn: " << l.lsn << " prev_lsn:" << l.prev_lsn
      << " txn_id: " << l.txn_id;
    return o;
  }

  // Log size in bytes.
  LogType type;
  uint64_t lsn = 0;
  uint64_t prev_lsn = 0;
  uint64_t txn_id = 0;
  RowPosition pos;
  std::string_view undo_data;
  std::string_view redo_data;
  std::unordered_set<uint64_t> dirty_page_table;
  std::unordered_set<uint64_t> transaction_table;

  // Page alloc/destroy target.b
  uint64_t allocate_page_id;
  uint64_t destroy_page_id;

  uint16_t length;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
