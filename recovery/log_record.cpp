#include "log_record.hpp"

#include <cassert>
#include <iostream>
#include <unordered_set>
#include <utility>

#include "common/serdes.hpp"
#include "type/row.hpp"

namespace {

enum KeyTypes : uint8_t {
  kHasPageID = 0x1,
  kHasSlot = 0x2,
  kHasKey = 0x4,
};

enum ValueTypes : uint8_t {
  kHasPageRef = 0x1,
  kHasBinary = 0x2,
};

template <typename T>
void Read(std::istream& in, T& dst) {
  in.read(reinterpret_cast<char*>(&dst), sizeof(T));
}

void PidSlotKeyDeserialize(std::istream& in, tinylamb::LogRecord& out) {
  uint8_t types;
  Read(in, types);
  if (types & KeyTypes::kHasPageID) {
    Read(in, out.pid);
  }
  if (types & KeyTypes::kHasSlot) {
    Read(in, out.slot);
  }
  if (types & KeyTypes::kHasKey) {
    tinylamb::DeserializeString(in, &out.key);
  }
}

size_t PidSlotKeySize(const tinylamb::LogRecord& lr) {
  size_t offset = 1;
  if (lr.HasPageID()) {
    offset += sizeof(tinylamb::page_id_t);
  }
  if (lr.HasSlot()) {
    offset += sizeof(tinylamb::slot_t);
  }
  if (!lr.key.empty()) {
    offset += sizeof(tinylamb::bin_size_t) + lr.key.length();
  }
  return offset;
}

size_t PidSlotKeySerialize(char* pos, const tinylamb::LogRecord& lr) {
  uint8_t types = 0;
  size_t offset = 1;
  if (lr.HasPageID()) {
    types |= KeyTypes::kHasPageID;
    offset += tinylamb::SerializePID(pos + offset, lr.pid);
  }
  if (lr.HasSlot()) {
    types |= KeyTypes::kHasSlot;
    offset += tinylamb::SerializeSlot(pos + offset, lr.slot);
  }
  if (!lr.key.empty()) {
    types |= KeyTypes::kHasKey;
    offset += tinylamb::SerializeStringView(pos + offset, lr.key);
  }
  *reinterpret_cast<uint8_t*>(pos) = types;
  return offset;
}

std::string OmittedString(std::string_view original, int length) {
  if (length < original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 4) + "bytes)..";
    omitted_key += original.substr(original.length() - 8);
    return omitted_key;
  } else {
    return std::string(original);
  }
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
      o << "INSERT ROW\t";
      break;
    case LogType::kInsertLeaf:
      o << "INSERT LEAF\t";
      break;
    case LogType::kInsertInternal:
      o << "INSERT INTERNAL\t";
      break;
    case LogType::kUpdateRow:
      o << "UPDATE ROW\t";
      break;
    case LogType::kUpdateLeaf:
      o << "UPDATE LEAF\t";
      break;
    case LogType::kUpdateInternal:
      o << "UPDATE INTERNAL\t";
      break;
    case LogType::kDeleteRow:
      o << "DELETE ROW\t";
      break;
    case LogType::kDeleteLeaf:
      o << "DELETE LEAF\t";
      break;
    case LogType::kDeleteInternal:
      o << "DELETE INTERNAL\t";
      break;
    case LogType::kCommit:
      o << "COMMIT\t\t";
      break;
    case LogType::kCompensateInsertRow:
      o << "COMPENSATE INSERT ROW\t";
      break;
    case LogType::kCompensateInsertLeaf:
      o << "COMPENSATE INSERT LEAF\t";
      break;
    case LogType::kCompensateInsertInternal:
      o << "COMPENSATE INSERT INTERNAL\t";
      break;
    case LogType::kCompensateUpdateRow:
      o << "COMPENSATE UPDATE ROW\t";
      break;
    case LogType::kCompensateUpdateLeaf:
      o << "COMPENSATE UPDATE LEAF\t";
      break;
    case LogType::kCompensateUpdateInternal:
      o << "COMPENSATE UPDATE INTERNAL\t";
      break;
    case LogType::kCompensateDeleteRow:
      o << "COMPENSATE DELETE ROW\t";
      break;
    case LogType::kCompensateDeleteLeaf:
      o << "COMPENSATE DELETE LEAF\t";
      break;
    case LogType::kCompensateDeleteInternal:
      o << "COMPENSATE DELETE INTERNAL\t";
      break;
    case LogType::kLowestValue:
      o << "SET LOWEST VALUE\t";
      break;
    case LogType::kBeginCheckpoint:
      o << "BEGIN CHECKPOINT\t";
      break;
    case LogType::kEndCheckpoint:
      o << "END CHECKPOINT\t";
      break;
    case LogType::kSystemAllocPage:
      o << "ALLOCATE\t";
      break;
    case LogType::kSystemDestroyPage:
      o << "DESTROY\t";
      break;
    default:
      o << "(undefined: " << static_cast<uint16_t>(type) << ")";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, const LogRecord& l) {
  o << l.type;
  switch (l.type) {
    case LogType::kUnknown:
      LOG(ERROR) << "kUnknownLog";
      break;
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kInsertRow:
    case LogType::kInsertLeaf:
      l.DumpPosition(o);
      o << "\t\tRedo: " << l.redo_data.size() << " bytes ";
      break;
    case LogType::kUpdateRow:
    case LogType::kUpdateLeaf:
      l.DumpPosition(o);
      o << "\t\t" << l.undo_data.size() << " -> " << l.redo_data.size()
        << "bytes ";
      break;
    case LogType::kDeleteRow:
    case LogType::kDeleteLeaf:
      l.DumpPosition(o);
      o << "\t\t" << l.undo_data.size() << " bytes ";
      break;
    case LogType::kCompensateInsertInternal:
    case LogType::kCompensateUpdateInternal:
    case LogType::kCompensateDeleteInternal:
    case LogType::kInsertInternal:
      l.DumpPosition(o);
      o << "\tInsert: " << l.redo_page;
      break;
    case LogType::kUpdateInternal:
      l.DumpPosition(o);
      o << "\tUpdate: " << l.undo_page << " -> " << l.redo_page;
      break;
    case LogType::kDeleteInternal:
      l.DumpPosition(o);
      o << "\tDelete: " << l.undo_page;
      break;
    case LogType::kLowestValue:
      l.DumpPosition(o);
      o << "\tLowest: " << l.redo_page;
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
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
      break;
  }
  o << "\tprev_lsn: " << l.prev_lsn << "\ttxn_id: " << l.txn_id;
  return o;
}

LogRecord::LogRecord(lsn_t prev, txn_id_t txn, LogType t)
    : prev_lsn(prev), txn_id(txn), type(t) {}

bool LogRecord::ParseLogRecord(std::istream& in, tinylamb::LogRecord* dst) {
  dst->Clear();
  Read(in, dst->type);
  Read(in, dst->prev_lsn);
  Read(in, dst->txn_id);
  PidSlotKeyDeserialize(in, *dst);
  switch (dst->type) {
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertInternal:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
      return true;
    case LogType::kInsertRow:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
      DeserializeString(in, &dst->redo_data);
      return true;
    case LogType::kUpdateRow:
    case LogType::kUpdateLeaf:
      DeserializeString(in, &dst->redo_data);
      DeserializeString(in, &dst->undo_data);
      return true;
    case LogType::kDeleteRow:
    case LogType::kDeleteLeaf:
      DeserializeString(in, &dst->undo_data);
      return true;
    case LogType::kInsertInternal:
    case LogType::kCompensateUpdateInternal:
    case LogType::kCompensateDeleteInternal:
    case LogType::kLowestValue:
      Read(in, dst->redo_page);
      return true;
    case LogType::kUpdateInternal:
      Read(in, dst->redo_page);
      Read(in, dst->undo_page);
      return true;
    case LogType::kDeleteInternal:
      Read(in, dst->undo_page);
      return true;
    case LogType::kSystemAllocPage:
      Read(in, dst->allocated_page_type);
      return true;
    case LogType::kEndCheckpoint: {
      uint64_t dpt_size;
      Read(in, dpt_size);
      dst->dirty_page_table.reserve(dpt_size);
      for (size_t i = 0; i < dpt_size; ++i) {
        page_id_t dirty_page_id;
        lsn_t recovery_lsn;
        Read(in, dirty_page_id);
        Read(in, recovery_lsn);
        dst->dirty_page_table.emplace_back(dirty_page_id, recovery_lsn);
      }

      uint64_t tt_size;
      Read(in, tt_size);
      dst->active_transaction_table.reserve(tt_size);
      for (size_t i = 0; i < tt_size; ++i) {
        CheckpointManager::ActiveTransactionEntry ate;
        CheckpointManager::ActiveTransactionEntry::Deserialize(in, &ate);
        dst->active_transaction_table.push_back(ate);
      }
      return true;
    }
    default:
      LOG(ERROR) << "unknown log type: " << dst->type;
      assert(!"unknown log");
      return false;
  }
  assert(!"never reach here");
}

LogRecord LogRecord::InsertingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                        slot_t slot, std::string_view r) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = slot;
  l.type = LogType::kInsertRow;
  l.redo_data = r;
  return l;
}

LogRecord LogRecord::InsertingLeafLogRecord(lsn_t p, txn_id_t txn,
                                            page_id_t pid, std::string_view key,
                                            std::string_view value) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kInsertLeaf;
  l.redo_data = value;
  return l;
}

LogRecord LogRecord::InsertingInternalLogRecord(lsn_t p, txn_id_t txn,
                                                page_id_t pid,
                                                std::string_view key,
                                                page_id_t value) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kInsertInternal;
  l.redo_page = value;
  return l;
}

LogRecord LogRecord::CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                                 slot_t key) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = key;
  l.type = LogType::kCompensateInsertRow;
  return l;
}

LogRecord LogRecord::CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                                 std::string_view key) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateInsertLeaf;
  return l;
}

LogRecord LogRecord::CompensatingInsertInternalLogRecord(txn_id_t txn,
                                                         page_id_t pid,
                                                         std::string_view key) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateInsertInternal;
  return l;
}

LogRecord LogRecord::UpdatingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                       slot_t key, std::string_view redo,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = key;
  l.type = LogType::kUpdateRow;
  l.redo_data = redo;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::UpdatingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                           std::string_view key,
                                           std::string_view redo,
                                           std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kUpdateLeaf;
  l.redo_data = redo;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::UpdatingInternalLogRecord(lsn_t p, txn_id_t txn,
                                               page_id_t pid,
                                               std::string_view key,
                                               page_id_t redo, page_id_t undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kUpdateInternal;
  l.redo_page = redo;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateLogRecord(lsn_t txn, page_id_t pid,
                                                 slot_t slot,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = slot;
  l.type = LogType::kCompensateUpdateRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateLeafLogRecord(lsn_t txn, page_id_t pid,
                                                     std::string_view key,
                                                     std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateUpdateLeaf;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateInternalLogRecord(lsn_t txn,
                                                         page_id_t pid,
                                                         std::string_view key,
                                                         page_id_t redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateUpdateInternal;
  l.redo_page = redo;
  return l;
}

LogRecord LogRecord::DeletingLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                       slot_t slot, std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = slot;
  l.type = LogType::kDeleteRow;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::DeletingLeafLogRecord(lsn_t p, txn_id_t txn, page_id_t pid,
                                           std::string_view key,
                                           std::string_view undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kDeleteLeaf;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::DeletingInternalLogRecord(lsn_t p, txn_id_t txn,
                                               page_id_t pid,
                                               std::string_view key,
                                               page_id_t undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kDeleteInternal;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteLogRecord(txn_id_t txn, page_id_t pid,
                                                 slot_t slot,
                                                 std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = slot;
  l.type = LogType::kCompensateDeleteRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteLeafLogRecord(txn_id_t txn,
                                                     page_id_t pid,
                                                     std::string_view key,
                                                     std::string_view redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateDeleteLeaf;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteInternalLogRecord(txn_id_t txn,
                                                         page_id_t pid,
                                                         std::string_view key,
                                                         page_id_t redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateDeleteInternal;
  l.redo_page = redo;
  return l;
}

LogRecord LogRecord::SetLowestLogRecord(lsn_t p, txn_id_t tid, page_id_t pid,
                                        page_id_t lowest_value) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = tid;
  l.pid = pid;
  l.type = LogType::kLowestValue;
  l.redo_page = lowest_value;
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
  slot = std::numeric_limits<slot_t>::max();
  prev_lsn = 0;
  txn_id = 0;
  key.clear();
  undo_data.clear();
  redo_data.clear();
  redo_page = 0;
  undo_page = 0;
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
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
      size += SerializeSize(redo_data);
      break;
    case LogType::kUpdateLeaf:
    case LogType::kUpdateRow:
      size += SerializeSize(redo_data);
      size += SerializeSize(undo_data);
      break;
    case LogType::kDeleteLeaf:
    case LogType::kDeleteRow:
      size += SerializeSize(undo_data);
      break;
    case LogType::kLowestValue:
    case LogType::kInsertInternal:
    case LogType::kDeleteInternal:
    case LogType::kCompensateUpdateInternal:
    case LogType::kCompensateDeleteInternal:
      size += sizeof(page_id_t);
      break;
    case LogType::kUpdateInternal:
      size += sizeof(page_id_t) * 2;
      break;
    case LogType::kEndCheckpoint:
      size += sizeof(dirty_page_table.size()) +
              dirty_page_table.size() * sizeof(std::pair<page_id_t, lsn_t>);
      size += sizeof(active_transaction_table.size()) +
              active_transaction_table.size() *
                  CheckpointManager::ActiveTransactionEntry::Size();
      break;
    case LogType::kSystemAllocPage:
      size += sizeof(PageType);
      break;
    case LogType::kBegin:
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertInternal:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateInsertRow:
    case LogType::kCommit:
    case LogType::kCompensateInsertLeaf:
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
    case LogType::kInsertRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
      offset += SerializeStringView(result.data() + offset, redo_data);
      break;
    case LogType::kUpdateLeaf:
    case LogType::kUpdateRow:
      offset += SerializeStringView(result.data() + offset, redo_data);
      offset += SerializeStringView(result.data() + offset, undo_data);
      break;
    case LogType::kDeleteLeaf:
    case LogType::kDeleteRow:
      offset += SerializeStringView(result.data() + offset, undo_data);
      break;
    case LogType::kInsertInternal:
    case LogType::kCompensateUpdateInternal:
    case LogType::kCompensateDeleteInternal:
    case LogType::kLowestValue:
      offset += SerializePID(result.data() + offset, redo_page);
      break;
    case LogType::kUpdateInternal:
      offset += SerializePID(result.data() + offset, redo_page);
      offset += SerializePID(result.data() + offset, undo_page);
      break;
    case LogType::kDeleteInternal:
      offset += SerializePID(result.data() + offset, undo_page);
      break;
    case LogType::kEndCheckpoint: {
      uint64_t dpt_size = dirty_page_table.size();
      memcpy(result.data() + offset, &dpt_size, sizeof(dpt_size));
      offset += sizeof(dpt_size);
      for (const auto& dpt : dirty_page_table) {
        offset += SerializePID(result.data() + offset, dpt.first);
        offset += SerializePID(result.data() + offset, dpt.second);
      }
      const uint64_t tt_size = active_transaction_table.size();
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
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertInternal:
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kSystemDestroyPage:
      // Nothing to do.
      break;
  }
  // LOG(ERROR) << offset << " : " << Size();
  assert(offset == Size());
  return result;
}

bool LogRecord::operator==(const LogRecord& rhs) const {
  return type == rhs.type && prev_lsn == rhs.prev_lsn && txn_id == rhs.txn_id &&
         pid == rhs.pid && slot == rhs.slot && undo_data == rhs.undo_data &&
         redo_data == rhs.redo_data &&
         dirty_page_table == rhs.dirty_page_table &&
         redo_page == rhs.redo_page && undo_page == rhs.undo_page &&
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
    o << " key: " << OmittedString(key, 20);
  }
  o << "}";
}

}  // namespace tinylamb
