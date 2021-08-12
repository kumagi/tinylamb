#include "log_record.hpp"

#include <cassert>
#include <iostream>
#include <unordered_set>
#include <utility>

#include "type/row.hpp"

namespace tinylamb {

LogRecord::LogRecord(uint64_t prev, uint64_t txn, LogType t)
    : prev_lsn(prev), txn_id(txn), type(t), length(Size()) {}

LogRecord LogRecord::InsertingLogRecord(uint64_t p, uint64_t txn,
                                        RowPosition po, std::string_view r) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kInsertRow;
  l.redo_data = r;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::UpdatingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                       std::string_view redo,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kUpdateRow;
  l.redo_data = redo;
  l.undo_data = undo;
  l.length = l.Size();
  return l;
}
LogRecord LogRecord::DeletingLogRecord(uint64_t p, uint64_t txn, RowPosition po,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kDeleteRow;
  l.undo_data = undo;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::CheckpointLogRecord(uint64_t p, uint64_t txn,
                                         RowPosition po,
                                         std::unordered_set<uint64_t> dpt,
                                         std::unordered_set<uint64_t> tt) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kEndCheckpoint;
  l.dirty_page_table = std::move(dpt);
  l.transaction_table = std::move(tt);
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::AllocatePageLogRecord(uint64_t p, uint64_t txn,
                                           uint64_t pid,
                                           PageType new_page_type) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.type = LogType::kSystemAllocPage;
  l.allocated_page_id = pid;
  l.allocated_page_type = new_page_type;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::DestroyPageLogRecord(uint64_t p, uint64_t txn,
                                          uint64_t pid) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.type = LogType::kSystemDestroyPage;
  l.destroy_page_id = pid;
  l.length = l.Size();
  return l;
}

void LogRecord::SetLSN(uint64_t new_lsn) { lsn = new_lsn; }

size_t LogRecord::Size() const {
  size_t size = sizeof(length) + sizeof(lsn) + sizeof(prev_lsn) +
                sizeof(txn_id) + sizeof(type);
  switch (type) {
    case LogType::kUnknown:
      assert(!"Don't call Size() of unknown log");
    case LogType::kBegin:
    case LogType::kBeginCheckpoint:
      break;
    case LogType::kInsertRow:
      size += sizeof(pos) + sizeof(uint32_t) + redo_data.length();
      break;
    case LogType::kUpdateRow:
      size += sizeof(pos) + sizeof(uint32_t) + redo_data.length() +
              sizeof(uint32_t) + undo_data.length();
      break;
    case LogType::kDeleteRow:
      size += sizeof(pos) + sizeof(uint32_t) + undo_data.length();
      ;
      break;
    case LogType::kEndCheckpoint:
      size += sizeof(pos) + sizeof(uint32_t) + redo_data.length();
      break;
    case LogType::kCommit:
      break;
    case LogType::kSystemAllocPage:
      size += sizeof(allocated_page_id) + sizeof(PageType);
      break;
    case LogType::kSystemDestroyPage:
      size += sizeof(destroy_page_id);
      break;
  }
  return size;
}

[[nodiscard]] std::string LogRecord::Serialize() const {
  std::string result(Size(), 0);
  size_t offset = 0;
  memcpy(result.data() + offset, &length, sizeof(length));
  offset += sizeof(length);
  memcpy(result.data() + offset, &type, sizeof(type));
  offset += sizeof(type);
  memcpy(result.data() + offset, &lsn, sizeof(lsn));
  offset += sizeof(lsn);
  memcpy(result.data() + offset, &prev_lsn, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  memcpy(result.data() + offset, &txn_id, sizeof(txn_id));
  offset += sizeof(txn_id);
  switch (type) {
    case LogType::kUnknown:
      assert(!"unknown type log must not be serialized");
    case LogType::kInsertRow: {
      offset += pos.Serialize(result.data() + offset);

      uint32_t redo_size = redo_data.length();
      memcpy(result.data() + offset, &redo_size, sizeof(redo_size));
      offset += sizeof(redo_size);
      memcpy(result.data() + offset, redo_data.data(), redo_data.size());
      offset += redo_data.size();
      break;
    }
    case LogType::kUpdateRow: {
      offset += pos.Serialize(result.data() + offset);

      uint32_t redo_size = redo_data.length();
      memcpy(result.data() + offset, &redo_size, sizeof(redo_size));
      offset += sizeof(redo_size);
      memcpy(result.data() + offset, redo_data.data(), redo_data.size());
      offset += redo_data.size();

      uint32_t undo_size = undo_data.length();
      memcpy(result.data() + offset, &undo_size, sizeof(undo_size));
      offset += sizeof(undo_size);
      memcpy(result.data() + offset, undo_data.data(), undo_data.size());
      offset += undo_data.size();
      break;
    }
    case LogType::kDeleteRow: {
      offset += pos.Serialize(result.data() + offset);

      uint32_t undo_size = undo_data.length();
      memcpy(result.data() + offset, &undo_size, sizeof(undo_size));
      offset += sizeof(undo_size);
      memcpy(result.data() + offset, undo_data.data(), undo_data.size());
      offset += undo_data.size();
      break;
    }
    case LogType::kBeginCheckpoint:
    case LogType::kBegin:
    case LogType::kCommit:
      // Nothing to do.
      break;
    case LogType::kEndCheckpoint: {
      uint64_t dpt_size = dirty_page_table.size();
      memcpy(result.data() + offset, &dpt_size, sizeof(dpt_size));
      offset += sizeof(dpt_size);
      for (const auto& dpt : dirty_page_table) {
        memcpy(result.data() + offset, &dpt, sizeof(dpt));
        offset += sizeof(dpt);
      }

      uint64_t tt_size = transaction_table.size();
      memcpy(result.data() + offset, &tt_size, sizeof(tt_size));
      offset += sizeof(tt_size);
      for (const auto& tt : transaction_table) {
        memcpy(result.data() + offset, &tt, sizeof(tt));
        offset += sizeof(tt);
      }
      break;
    }
    case LogType::kSystemAllocPage:
      memcpy(result.data() + offset, &allocated_page_id,
             sizeof(allocated_page_id));
      offset += sizeof(allocated_page_id);
      memcpy(result.data() + offset, &allocated_page_type, sizeof(allocated_page_type));
      offset += sizeof(allocated_page_type);
      break;
    case LogType::kSystemDestroyPage:
      memcpy(result.data() + offset, &destroy_page_id, sizeof(destroy_page_id));
      offset += sizeof(allocated_page_id);
      break;
  }
  return result;
}

}  // namespace tinylamb
