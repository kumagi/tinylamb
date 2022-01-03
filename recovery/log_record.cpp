#include "log_record.hpp"

#include <cassert>
#include <iostream>
#include <unordered_set>
#include <utility>

#include "type/row.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const LogType& type) {
  switch (type) {
    case LogType::kUnknown:
      o << "(unknown) ";
      break;
    case LogType::kBegin:
      o << "BEGIN";
      break;
    case LogType::kInsertRow:
      o << "INSERT";
      break;
    case LogType::kUpdateRow:
      o << "UPDATE";
      break;
    case LogType::kDeleteRow:
      o << "DELETE";
      break;
    case LogType::kCommit:
      o << "COMMIT";
      break;
    case LogType::kCompensateInsertRow:
      o << "COMPENSATE INSERT";
      break;
    case LogType::kCompensateUpdateRow:
      o << "COMPENSATE UPDATE";
      break;
    case LogType::kCompensateDeleteRow:
      o << "COMPENSATE DELETE";
      break;
    case LogType::kBeginCheckpoint:
      o << "BEGIN CHECKPOINT";
      break;
    case LogType::kEndCheckpoint:
      o << "END CHECKPOINT";
      break;
    case LogType::kSystemAllocPage:
      o << "ALLOCATE";
      break;
    case LogType::kSystemDestroyPage:
      o << "DESTROY";
      break;
    default:
      o << "(undefined: " << static_cast<uint16_t>(type) << ")";
  }
  return o;
}

LogRecord::LogRecord(uint64_t prev, uint64_t txn, LogType t)
    : prev_lsn(prev), txn_id(txn), type(t), length(Size()) {}

bool LogRecord::ParseLogRecord(const char* src, tinylamb::LogRecord* dst) {
  size_t offset = 0;
  memcpy(&dst->length, src, sizeof(dst->length));
  src += sizeof(dst->length);
  memcpy(&dst->type, src, sizeof(dst->type));
  src += sizeof(dst->type);
  memcpy(&dst->prev_lsn, src, sizeof(dst->prev_lsn));
  src += sizeof(dst->prev_lsn);
  memcpy(&dst->txn_id, src, sizeof(dst->txn_id));
  src += sizeof(dst->txn_id);
  switch (dst->type) {
    case LogType::kUnknown:
      LOG(ERROR) << "unknown log type";
      return false;
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
      return true;
    case LogType::kInsertRow: {
      src += dst->pos.Parse(src);
      uint32_t redo_size;
      memcpy(&redo_size, src, sizeof(redo_size));
      src += sizeof(redo_size);
      dst->redo_data = std::string_view(src, redo_size);
      src += redo_size;
      return true;
    }
    case LogType::kUpdateRow: {
      src += dst->pos.Parse(src);
      uint32_t redo_size;
      memcpy(&redo_size, src, sizeof(redo_size));
      src += sizeof(redo_size);
      dst->redo_data = std::string_view(src, redo_size);
      src += redo_size;

      uint32_t undo_size;
      memcpy(&undo_size, src, sizeof(undo_size));
      src += sizeof(undo_size);
      dst->undo_data = std::string_view(src, undo_size);
      src += undo_size;
      return true;
    }
    case LogType::kDeleteRow: {
      src += dst->pos.Parse(src);
      uint32_t undo_size;
      memcpy(&undo_size, src, sizeof(undo_size));
      src += sizeof(undo_size);
      dst->undo_data = std::string_view(src, undo_size);
      src += undo_size;
      return true;
    }
    case LogType::kCompensateInsertRow: {
      src += dst->pos.Parse(src);
      return true;
    }
    case LogType::kCompensateUpdateRow: {
      return true;
    }
    case LogType::kCompensateDeleteRow: {
      return true;
    }
    case LogType::kEndCheckpoint: {
      uint64_t dpt_size;
      memcpy(&dpt_size, src, sizeof(dpt_size));
      src += sizeof(dpt_size);
      for (size_t i = 0; i < dpt_size; ++i) {
        const auto* dirty_page = reinterpret_cast<const uint64_t*>(src);
        dst->dirty_page_table.insert(*dirty_page);
        src += sizeof(*dirty_page);
      }

      uint64_t tt_size;
      memcpy(&tt_size, src, sizeof(tt_size));
      src += sizeof(tt_size);
      for (size_t i = 0; i < tt_size; ++i) {
        const auto* txn_id = reinterpret_cast<const uint64_t*>(src);
        dst->transaction_table.insert(*txn_id);
        src += sizeof(*txn_id);
      }
      return true;
    }
    case LogType::kSystemAllocPage:
      memcpy(&dst->allocated_page_id, src,
             sizeof(dst->allocated_page_id));
      src += sizeof(dst->allocated_page_id);
      memcpy(&dst->allocated_page_type, src,
             sizeof(dst->allocated_page_type));
      src += sizeof(dst->allocated_page_type);
      return true;
    case LogType::kSystemDestroyPage:
      memcpy(&dst->destroy_page_id, src, sizeof(dst->destroy_page_id));
      src += sizeof(dst->destroy_page_id);
      return true;
  }
  LOG(ERROR) << "unexpected execution path: " << dst->type;
  return false;
}

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

LogRecord LogRecord::CompensatingInsertLogRecord(uint16_t p,
                                                 uint64_t undo_next_lsn,
                                                 uint64_t txn, RowPosition po) {
  LogRecord l;
  l.prev_lsn = p;
  l.undo_next_lsn = undo_next_lsn;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kCompensateInsertRow;
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

LogRecord LogRecord::CompensatingUpdateLogRecord(uint16_t p,
                                                 uint64_t undo_next_lsn,
                                                 uint64_t txn, RowPosition po,
                                                 std::string_view redo) {
  LogRecord l;
  l.prev_lsn = p;
  l.undo_next_lsn = undo_next_lsn;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kCompensateUpdateRow;
  l.redo_data = redo;
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

LogRecord LogRecord::CompensatingDeleteLogRecord(uint16_t p,
                                                 uint64_t undo_next_lsn,
                                                 uint64_t txn, RowPosition po,
                                                 std::string_view redo) {
  LogRecord l;
  l.prev_lsn = p;
  l.undo_next_lsn = undo_next_lsn;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kCompensateUpdateRow;
  l.redo_data = redo;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::CheckpointLogRecord(uint64_t p, uint64_t txn,
                                         std::unordered_set<uint64_t> dpt,
                                         std::unordered_set<uint64_t> tt) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
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

size_t LogRecord::Size() const {
  size_t size =
      sizeof(length) + sizeof(prev_lsn) + sizeof(txn_id) + sizeof(type);
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
      break;
    case LogType::kEndCheckpoint:
      size += sizeof(pos) + sizeof(uint32_t) +
              dirty_page_table.size() * sizeof(uint64_t) +
              transaction_table.size() * sizeof(uint64_t);
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
      memcpy(result.data() + offset, &allocated_page_type,
             sizeof(allocated_page_type));
      offset += sizeof(allocated_page_type);
      break;
    case LogType::kSystemDestroyPage:
      memcpy(result.data() + offset, &destroy_page_id, sizeof(destroy_page_id));
      offset += sizeof(allocated_page_id);
      break;
  }
  return result;
}

bool LogRecord::operator==(const LogRecord& rhs) const {
  return type == rhs.type && prev_lsn == rhs.prev_lsn && txn_id == rhs.txn_id &&
         pos == rhs.pos && undo_data == rhs.undo_data &&
         redo_data == rhs.redo_data &&
         dirty_page_table == rhs.dirty_page_table &&
         transaction_table == rhs.transaction_table &&
         allocated_page_id == rhs.allocated_page_id &&
         destroy_page_id == rhs.destroy_page_id && length == rhs.length;
}

}  // namespace tinylamb
