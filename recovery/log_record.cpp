#include "log_record.hpp"

#include <cassert>
#include <iostream>
#include <unordered_set>
#include <utility>

#include "type/row.hpp"

namespace {

enum KeyTypes {
  kHasPageID = 0x1,
  kHasSlot = 0x2,
  kHasKey = 0x4,
};

size_t BinSerialize(char* pos, std::string_view bin) {
  const uint16_t size = bin.size();
  memcpy(pos, &size, sizeof(size));
  memcpy(pos + sizeof(size), bin.data(), bin.size());
  return sizeof(size) + bin.size();
}

size_t BinDeserialize(const char* pos, std::string_view* dst) {
  const uint16_t key_length = *reinterpret_cast<const uint16_t*>(pos);
  *dst = {pos + sizeof(uint16_t), key_length};
  return sizeof(uint16_t) + key_length;
}

size_t BinSize(std::string_view bin) { return sizeof(uint16_t) + bin.size(); }

size_t PidSlotKeyDeserialize(const char* pos, tinylamb::LogRecord& out) {
  const auto types = *reinterpret_cast<const uint8_t*>(pos);
  size_t offset = 1;
  if (types & KeyTypes::kHasPageID) {
    out.pid = *reinterpret_cast<const tinylamb::page_id_t*>(pos + offset);
    offset += sizeof(tinylamb::page_id_t);
  }
  if (types & KeyTypes::kHasSlot) {
    out.slot = *reinterpret_cast<const uint16_t*>(pos + offset);
    offset += sizeof(uint16_t);
  }
  if (types & KeyTypes::kHasKey) {
    uint16_t size = *reinterpret_cast<const uint16_t*>(pos + offset);
    offset += sizeof(uint16_t);
    out.key = {pos + offset, size};
    offset += size;
  }
  return offset;
}

size_t PidSlotKeySize(const tinylamb::LogRecord& lr) {
  size_t offset = 1;
  if (lr.HasPageID()) {
    offset += sizeof(tinylamb::page_id_t);
  }
  if (lr.HasSlot()) {
    offset += sizeof(uint16_t);
  }
  if (!lr.key.empty()) {
    offset += sizeof(uint16_t) + lr.key.length();
  }
  return offset;
}

size_t PidSlotKeySerialize(char* pos, const tinylamb::LogRecord& lr) {
  uint8_t types = 0;
  size_t offset = 1;
  if (lr.HasPageID()) {
    types |= KeyTypes::kHasPageID;
    *reinterpret_cast<tinylamb::page_id_t*>(pos + offset) = lr.pid;
    offset += sizeof(tinylamb::page_id_t);
  }
  if (lr.HasSlot()) {
    types |= KeyTypes::kHasSlot;
    *reinterpret_cast<uint16_t*>(pos + offset) = lr.slot;
    offset += sizeof(uint16_t);
  }
  if (!lr.key.empty()) {
    types |= KeyTypes::kHasKey;
    *reinterpret_cast<uint16_t*>(pos + offset) = lr.key.length();
    offset += sizeof(uint16_t);
    memcpy(pos + offset, lr.key.data(), lr.key.size());
    offset += lr.key.size();
  }
  *reinterpret_cast<uint8_t*>(pos) = types;
  return offset;
}

}  // namespace

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

LogRecord::LogRecord(lsn_t prev, txn_id_t txn, LogType t)
    : prev_lsn(prev), txn_id(txn), type(t) {}

bool LogRecord::ParseLogRecord(const char* src, tinylamb::LogRecord* dst) {
  dst->Clear();
  memcpy(&dst->type, src, sizeof(dst->type));
  src += sizeof(dst->type);
  memcpy(&dst->prev_lsn, src, sizeof(dst->prev_lsn));
  src += sizeof(dst->prev_lsn);
  memcpy(&dst->txn_id, src, sizeof(dst->txn_id));
  src += sizeof(dst->txn_id);
  src += PidSlotKeyDeserialize(src, *dst);
  switch (dst->type) {
    case LogType::kUnknown:
      LOG(ERROR) << "unknown log type";
      return false;
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
      return true;
    case LogType::kInsertRow: {
      BinDeserialize(src, &dst->redo_data);
      return true;
    }
    case LogType::kUpdateRow: {
      src += BinDeserialize(src, &dst->redo_data);
      BinDeserialize(src, &dst->undo_data);
      return true;
    }
    case LogType::kDeleteRow: {
      BinDeserialize(src, &dst->undo_data);
      return true;
    }
    case LogType::kCompensateInsertRow: {
      return true;
    }
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow: {
      BinDeserialize(src, &dst->redo_data);
      return true;
    }
    case LogType::kEndCheckpoint: {
      uint64_t dpt_size;
      memcpy(&dpt_size, src, sizeof(dpt_size));
      src += sizeof(dpt_size);
      dst->dirty_page_table.reserve(dpt_size);
      for (size_t i = 0; i < dpt_size; ++i) {
        const auto* dirty_page_id = reinterpret_cast<const page_id_t*>(src);
        src += sizeof(*dirty_page_id);
        const auto* recovery_lsn = reinterpret_cast<const lsn_t*>(src);
        src += sizeof(*recovery_lsn);
        dst->dirty_page_table.emplace_back(*dirty_page_id, *recovery_lsn);
      }

      uint64_t tt_size;
      memcpy(&tt_size, src, sizeof(tt_size));
      src += sizeof(tt_size);
      dst->active_transaction_table.reserve(tt_size);
      for (size_t i = 0; i < tt_size; ++i) {
        CheckpointManager::ActiveTransactionEntry ate;
        src +=
            CheckpointManager::ActiveTransactionEntry::Deserialize(src, &ate);
        dst->active_transaction_table.push_back(ate);
      }
      return true;
    }
    case LogType::kSystemAllocPage:
      memcpy(&dst->allocated_page_type, src, sizeof(dst->allocated_page_type));
      sizeof(dst->allocated_page_type);
      return true;
    case LogType::kSystemDestroyPage:
      return true;
  }
  LOG(ERROR) << "unexpected execution path: " << dst->type;
  return false;
}

LogRecord LogRecord::InsertingLogRecord(lsn_t p, txn_id_t txn, RowPosition po,
                                        std::string_view r) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = po.page_id;
  l.slot = po.slot;
  l.type = LogType::kInsertRow;
  l.redo_data = r;
  return l;
}

LogRecord LogRecord::InsertingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                        std::string_view key,
                                        std::string_view value) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kInsertRow;
  l.redo_data = value;
  return l;
}

LogRecord LogRecord::CompensatingInsertLogRecord(txn_id_t txn, RowPosition po) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = po.page_id;
  l.slot = po.slot;
  l.type = LogType::kCompensateInsertRow;
  return l;
}

LogRecord LogRecord::CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                                 std::string_view key) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateInsertRow;
  return l;
}

LogRecord LogRecord::UpdatingLogRecord(lsn_t p, txn_id_t txn, RowPosition po,
                                       std::string_view redo,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = po.page_id;
  l.slot = po.slot;
  l.type = LogType::kUpdateRow;
  l.redo_data = redo;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::UpdatingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                       std::string_view key,
                                       std::string_view redo,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kUpdateRow;
  l.redo_data = redo;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateLogRecord(txn_id_t txn, RowPosition po,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = po.page_id;
  l.slot = po.slot;
  l.type = LogType::kCompensateUpdateRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateLogRecord(lsn_t txn, page_id_t pid,
                                                 std::string_view key,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateUpdateRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::DeletingLogRecord(lsn_t p, txn_id_t txn, RowPosition po,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = po.page_id;
  l.slot = po.slot;
  l.type = LogType::kDeleteRow;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::DeletingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                       std::string_view key,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kDeleteRow;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteLogRecord(uint64_t txn, RowPosition po,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = po.page_id;
  l.slot = po.slot;
  l.type = LogType::kCompensateDeleteRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteLogRecord(txn_id_t txn, page_id_t pid,
                                                 std::string_view key,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateDeleteRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::AllocatePageLogRecord(uint64_t p, uint64_t txn,
                                           uint64_t pid,
                                           PageType new_page_type) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSystemAllocPage;
  l.allocated_page_type = new_page_type;
  return l;
}

LogRecord LogRecord::DestroyPageLogRecord(uint64_t p, uint64_t txn,
                                          uint64_t pid) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSystemDestroyPage;
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
  pid = std::numeric_limits<page_id_t>::max();
  slot = std::numeric_limits<uint16_t>::max();
  prev_lsn = 0;
  txn_id = 0;
  undo_data = "";
  redo_data = "";
  dirty_page_table.clear();
  active_transaction_table.clear();
  allocated_page_type = PageType::kUnknown;
}

size_t LogRecord::Size() const {
  size_t size = sizeof(type) + sizeof(prev_lsn) + sizeof(txn_id);
  size += PidSlotKeySize(*this);
  switch (type) {
    case LogType::kUnknown:
      assert(!"Don't call Size() of unknown log");
    case LogType::kInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      size += BinSize(redo_data);
      break;
    case LogType::kUpdateRow:
      size += BinSize(redo_data);
      size += BinSize(undo_data);
      break;
    case LogType::kDeleteRow:
      size += BinSize(undo_data);
      break;
    case LogType::kEndCheckpoint:
      size += sizeof(dirty_page_table.size()) +
              dirty_page_table.size() * sizeof(std::pair<page_id_t, lsn_t>);
      size += sizeof(active_transaction_table.size()) +
              active_transaction_table.size() *
                  CheckpointManager::ActiveTransactionEntry::Size();
      break;
    case LogType::kCommit:
      break;
    case LogType::kSystemAllocPage:
      size += sizeof(PageType);
      break;
    case LogType::kBegin:
    case LogType::kBeginCheckpoint:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateInsertRow:
      break;
  }
  return size;
}

[[nodiscard]] std::string LogRecord::Serialize() const {
  const size_t length = Size();
  std::string result(length, 0);
  size_t offset = 0;
  memcpy(result.data() + offset, &type, sizeof(type));
  offset += sizeof(type);
  memcpy(result.data() + offset, &prev_lsn, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  memcpy(result.data() + offset, &txn_id, sizeof(txn_id));
  offset += sizeof(txn_id);
  offset += PidSlotKeySerialize(result.data() + offset, *this);
  switch (type) {
    case LogType::kUnknown:
      assert(!"unknown type log must not be serialized");
    case LogType::kInsertRow: {
      offset += BinSerialize(result.data() + offset, redo_data);
      break;
    }
    case LogType::kUpdateRow: {
      offset += BinSerialize(result.data() + offset, redo_data);
      offset += BinSerialize(result.data() + offset, undo_data);
      break;
    }
    case LogType::kCompensateInsertRow:
      break;
    case LogType::kDeleteRow: {
      offset += BinSerialize(result.data() + offset, undo_data);
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
        offset += tt.Serialize(result.data() + offset);
      }
      break;
    }
    case LogType::kSystemAllocPage:
      memcpy(result.data() + offset, &allocated_page_type,
             sizeof(allocated_page_type));
      offset += sizeof(allocated_page_type);
      break;
    case LogType::kSystemDestroyPage:
      break;
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      offset += BinSerialize(result.data() + offset, redo_data);
      break;
  }
  // LOG(ERROR) << offset << " vs " << Size();
  assert(offset == Size());
  return result;
}

bool LogRecord::operator==(const LogRecord& rhs) const {
  return type == rhs.type && prev_lsn == rhs.prev_lsn && txn_id == rhs.txn_id &&
         pid == rhs.pid && slot == rhs.slot && undo_data == rhs.undo_data &&
         redo_data == rhs.redo_data &&
         dirty_page_table == rhs.dirty_page_table &&
         active_transaction_table == rhs.active_transaction_table;
}

void LogRecord::DumpPosition(std::ostream& o) const {
  o << "{";
  if (HasPageID()) {
    o << "Page: " << pid;
  }
  if (HasSlot()) {
    o << "| " << slot;
  }
  if (!key.empty()) {
    o << "| " << key;
  }
  o << "}";
}

}  // namespace tinylamb
