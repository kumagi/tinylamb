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
  dst->Clear();
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
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow: {
      src += dst->pos.Parse(src);
      uint32_t redo_size;
      memcpy(&redo_size, src, sizeof(redo_size));
      src += sizeof(redo_size);
      dst->redo_data = std::string_view(src, redo_size);
      src += redo_size;
      return true;
    }
    case LogType::kEndCheckpoint: {
      uint64_t dpt_size;
      memcpy(&dpt_size, src, sizeof(dpt_size));
      src += sizeof(dpt_size);
      dst->dirty_page_table.reserve(dpt_size);
      for (size_t i = 0; i < dpt_size; ++i) {
        const auto* dirty_page_id = reinterpret_cast<const uint64_t*>(src);
        src += sizeof(*dirty_page_id);
        const auto* recovery_lsn = reinterpret_cast<const uint64_t*>(src);
        src += sizeof(*recovery_lsn);
        dst->dirty_page_table.emplace_back(*dirty_page_id, *recovery_lsn);
      }

      uint64_t tt_size;
      memcpy(&tt_size, src, sizeof(tt_size));
      src += sizeof(tt_size);
      dst->active_transaction_table.reserve(tt_size);
      for (size_t i = 0; i < tt_size; ++i) {
        const auto* txn_id = reinterpret_cast<const uint64_t*>(src);
        src += sizeof(*txn_id);
        const auto* status = reinterpret_cast<const TransactionStatus*>(src);
        src += sizeof(*status);
        const auto* last_lsn = reinterpret_cast<const uint64_t*>(src);
        src += sizeof(*last_lsn);
        dst->active_transaction_table.emplace_back(*txn_id, *status, *last_lsn);
      }
      return true;
    }
    case LogType::kSystemAllocPage:
      src += dst->pos.Parse(src);
      memcpy(&dst->allocated_page_type, src, sizeof(dst->allocated_page_type));
      src += sizeof(dst->allocated_page_type);
      return true;
    case LogType::kSystemDestroyPage:
      src += dst->pos.Parse(src);
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

LogRecord LogRecord::CompensatingInsertLogRecord(uint64_t txn, RowPosition po) {
  LogRecord l;
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

LogRecord LogRecord::CompensatingUpdateLogRecord(uint64_t txn, RowPosition po,
                                                 std::string_view redo) {
  LogRecord l;
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

LogRecord LogRecord::CompensatingDeleteLogRecord(uint64_t txn, RowPosition po,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pos = po;
  l.type = LogType::kCompensateDeleteRow;
  l.redo_data = redo;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::AllocatePageLogRecord(uint64_t p, uint64_t txn,
                                           uint64_t pid,
                                           PageType new_page_type) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pos = RowPosition(pid, -1);
  l.type = LogType::kSystemAllocPage;
  l.allocated_page_type = new_page_type;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::DestroyPageLogRecord(uint64_t p, uint64_t txn,
                                          uint64_t pid) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pos = RowPosition(pid, -1);
  l.type = LogType::kSystemDestroyPage;
  l.length = l.Size();
  return l;
}

LogRecord LogRecord::BeginCheckpointLogRecord() {
  LogRecord l;
  l.type = LogType::kBeginCheckpoint;
  return l;
}

LogRecord LogRecord::EndCheckpointLogRecord(
    const std::vector<std::pair<uint64_t, uint64_t>>& dpt,
    const std::vector<CheckpointManager::ActiveTransactionEntry>& att) {
  LogRecord l;
  l.type = LogType::kEndCheckpoint;
  l.dirty_page_table = dpt;
  l.active_transaction_table = att;
  return l;
}

void LogRecord::Clear() {
  type = LogType::kUnknown;
  pos = RowPosition();
  prev_lsn = 0;
  txn_id = 0;
  undo_data = "";
  redo_data = "";
  dirty_page_table.clear();
  active_transaction_table.clear();
  allocated_page_type = PageType::kUnknown;
  length = 0;
}

size_t LogRecord::Size() const {
  size_t size =
      sizeof(length) + sizeof(prev_lsn) + sizeof(txn_id) + sizeof(type);
  switch (type) {
    case LogType::kUnknown:
      assert(!"Don't call Size() of unknown log");
    case LogType::kBeginCheckpoint:
      return sizeof(length) + sizeof(type);  // This is fixed size.
    case LogType::kBegin:
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
      size += sizeof(pos) + sizeof(dirty_page_table.size()) +
              dirty_page_table.size() * sizeof(std::pair<uint64_t, uint64_t>) +
              sizeof(active_transaction_table.size()) +
              active_transaction_table.size() *
                  sizeof(CheckpointManager::ActiveTransactionEntry);
      break;
    case LogType::kCommit:
      break;
    case LogType::kSystemAllocPage:
      size += sizeof(pos) + sizeof(PageType);
      break;
    case LogType::kSystemDestroyPage:
      size += sizeof(pos);
      break;
    case LogType::kCompensateInsertRow:
      size += sizeof(pos);
      break;
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      size += sizeof(pos) + sizeof(uint32_t) + redo_data.size();
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

      uint64_t tt_size = active_transaction_table.size();
      memcpy(result.data() + offset, &tt_size, sizeof(tt_size));
      offset += sizeof(tt_size);
      for (const auto& tt : active_transaction_table) {
        memcpy(result.data() + offset, &tt.txn_id, sizeof(tt.txn_id));
        offset += sizeof(tt.txn_id);
        memcpy(result.data() + offset, &tt.status, sizeof(tt.status));
        offset += sizeof(tt.status);
        memcpy(result.data() + offset, &tt.last_lsn, sizeof(tt.last_lsn));
        offset += sizeof(tt.last_lsn);
      }
      break;
    }
    case LogType::kSystemAllocPage:
      offset += pos.Serialize(result.data() + offset);
      memcpy(result.data() + offset, &allocated_page_type,
             sizeof(allocated_page_type));
      offset += sizeof(allocated_page_type);
      break;
    case LogType::kSystemDestroyPage:
      offset += pos.Serialize(result.data() + offset);
      break;
    case LogType::kCompensateInsertRow:
      offset += pos.Serialize(result.data() + offset);
      break;
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      offset += pos.Serialize(result.data() + offset);
      uint32_t redo_size = redo_data.length();
      memcpy(result.data() + offset, &redo_size, sizeof(redo_size));
      offset += sizeof(redo_size);
      memcpy(result.data() + offset, redo_data.data(), redo_data.size());
      offset += redo_data.size();
      break;
  }
  // assert(offset == Size());
  return result;
}

bool LogRecord::operator==(const LogRecord& rhs) const {
  return type == rhs.type && prev_lsn == rhs.prev_lsn && txn_id == rhs.txn_id &&
         pos == rhs.pos && undo_data == rhs.undo_data &&
         redo_data == rhs.redo_data &&
         dirty_page_table == rhs.dirty_page_table &&
         active_transaction_table == rhs.active_transaction_table;
}

}  // namespace tinylamb
