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

template <typename T>
void Read(std::istream& in, T& dst) {
  in.read(reinterpret_cast<char*>(&dst), sizeof(T));
}

std::string OmittedString(std::string_view original, size_t length) {
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
    case LogType::kInsertBranch:
      o << "INSERT BRANCH\t";
      break;
    case LogType::kUpdateRow:
      o << "UPDATE ROW\t";
      break;
    case LogType::kUpdateLeaf:
      o << "UPDATE LEAF\t";
      break;
    case LogType::kUpdateBranch:
      o << "UPDATE BRANCH\t";
      break;
    case LogType::kDeleteRow:
      o << "DELETE ROW\t";
      break;
    case LogType::kDeleteLeaf:
      o << "DELETE LEAF\t";
      break;
    case LogType::kDeleteBranch:
      o << "DELETE BRANCH\t";
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
    case LogType::kCompensateInsertBranch:
      o << "COMPENSATE INSERT BRANCH\t";
      break;
    case LogType::kCompensateUpdateRow:
      o << "COMPENSATE UPDATE ROW\t";
      break;
    case LogType::kCompensateUpdateLeaf:
      o << "COMPENSATE UPDATE LEAF\t";
      break;
    case LogType::kCompensateUpdateBranch:
      o << "COMPENSATE UPDATE BRANCH\t";
      break;
    case LogType::kCompensateDeleteRow:
      o << "COMPENSATE DELETE ROW\t";
      break;
    case LogType::kCompensateDeleteLeaf:
      o << "COMPENSATE DELETE LEAF\t";
      break;
    case LogType::kCompensateDeleteBranch:
      o << "COMPENSATE DELETE BRANCH\t";
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
    case LogType::kSetPrevNext:
      o << "SET PREV NEXT\t";
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
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
    case LogType::kInsertBranch:
      l.DumpPosition(o);
      o << "\tInsert: " << l.redo_page;
      break;
    case LogType::kUpdateBranch:
      l.DumpPosition(o);
      o << "\tUpdate: " << l.undo_page << " -> " << l.redo_page;
      break;
    case LogType::kDeleteBranch:
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
    case LogType::kSetPrevNext: {
      l.DumpPosition(o);
      page_id_t undo_prev, undo_next, redo_prev, redo_next;
      memcpy(&undo_prev, l.undo_data.data(), sizeof(undo_prev));
      memcpy(&undo_next, l.undo_data.data() + sizeof(undo_prev),
             sizeof(undo_prev));
      memcpy(&redo_prev, l.redo_data.data(), sizeof(redo_prev));
      memcpy(&redo_next, l.redo_data.data() + sizeof(redo_prev),
             sizeof(redo_prev));
      o << "[" << undo_prev << ", " << undo_next << "] to be "
        << "[" << redo_prev << ", " << redo_next << "]";
      break;
    }
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
    : type(t), prev_lsn(prev), txn_id(txn) {}

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

LogRecord LogRecord::InsertingBranchLogRecord(lsn_t p, txn_id_t txn,
                                              page_id_t pid,
                                              std::string_view key,
                                              page_id_t redo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kInsertBranch;
  l.redo_page = redo;
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

LogRecord LogRecord::CompensatingInsertBranchLogRecord(txn_id_t txn,
                                                       page_id_t pid,
                                                       std::string_view key) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateInsertBranch;
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

LogRecord LogRecord::UpdatingBranchLogRecord(lsn_t p, txn_id_t txn,
                                             page_id_t pid,
                                             std::string_view key,
                                             page_id_t redo, page_id_t undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kUpdateBranch;
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

LogRecord LogRecord::CompensatingUpdateBranchLogRecord(lsn_t txn, page_id_t pid,
                                                       std::string_view key,
                                                       page_id_t redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateUpdateBranch;
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

LogRecord LogRecord::DeletingBranchLogRecord(lsn_t p, txn_id_t txn,
                                             page_id_t pid,
                                             std::string_view key,
                                             page_id_t undo) {
  LogRecord l;
  l.prev_lsn = p;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kDeleteBranch;
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

LogRecord LogRecord::ComnensatingDeleteBranchLogRecord(txn_id_t txn,
                                                       page_id_t pid,
                                                       std::string_view slot,
                                                       page_id_t redo) {
  LogRecord l;
  l.txn_id = txn;
  l.pid = pid;
  l.key = slot;
  l.type = LogType::kCompensateDeleteBranch;
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

LogRecord LogRecord::SetPrevNextLogRecord(lsn_t prev_lsn, txn_id_t tid,
                                          page_id_t pid, page_id_t undo_prev,
                                          page_id_t undo_next,
                                          page_id_t redo_prev,
                                          page_id_t redo_next) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = tid;
  l.pid = pid;
  l.type = LogType::kSetPrevNext;
  l.undo_data = std::string(sizeof(undo_prev) + sizeof(undo_next), '\0');
  memcpy(l.undo_data.data(), &undo_prev, sizeof(undo_prev));
  memcpy(l.undo_data.data() + sizeof(undo_prev), &undo_next, sizeof(undo_next));
  l.redo_data = std::string(sizeof(redo_prev) + sizeof(redo_next), '\0');
  memcpy(l.redo_data.data(), &redo_prev, sizeof(redo_prev));
  memcpy(l.redo_data.data() + sizeof(redo_prev), &redo_next, sizeof(redo_next));
  return l;
}

void LogRecord::PrevNextLogRecordRedo(page_id_t& prev, page_id_t& next) const {
  memcpy(&prev, redo_data.data(), sizeof(prev));
  memcpy(&next, redo_data.data() + sizeof(prev), sizeof(next));
}

void LogRecord::PrevNextLogRecordUndo(page_id_t& prev, page_id_t& next) const {
  memcpy(&prev, undo_data.data(), sizeof(prev));
  memcpy(&next, undo_data.data() + sizeof(prev), sizeof(next));
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
  size_t offset = 1;
  if ((*this).HasPageID()) {
    offset += sizeof(page_id_t);
  }
  if ((*this).HasSlot()) {
    offset += sizeof(slot_t);
  }
  if (!(*this).key.empty()) {
    offset += sizeof(bin_size_t) + (*this).key.length();
  }
  size += offset;
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
    case LogType::kSetPrevNext:
    case LogType::kUpdateRow:
      size += SerializeSize(redo_data);
      size += SerializeSize(undo_data);
      break;
    case LogType::kDeleteLeaf:
    case LogType::kDeleteRow:
      size += SerializeSize(undo_data);
      break;
    case LogType::kLowestValue:
    case LogType::kInsertBranch:
    case LogType::kDeleteBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
      size += sizeof(page_id_t);
      break;
    case LogType::kUpdateBranch:
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
    case LogType::kCompensateInsertBranch:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateInsertRow:
    case LogType::kCommit:
    case LogType::kCompensateInsertLeaf:
      break;
  }
  return size;
}

Encoder& operator<<(Encoder& e, const LogRecord& l) {
  e << (uint16_t&)l.type << l.prev_lsn << l.txn_id;
  const uint8_t types = (l.HasPageID() ? kHasPageID : 0) |
                        (l.HasSlot() ? kHasSlot : 0) |
                        (!l.key.empty() ? kHasKey : 0);
  e << types;
  if (l.HasPageID()) {
    e << l.pid;
  }
  if (l.HasSlot()) {
    e << l.slot;
  }
  if (!l.key.empty()) {
    e << l.key;
  }
  switch (l.type) {
    case LogType::kUnknown:
      assert(!"unknown type log must not be serialized");
    case LogType::kInsertRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
      e << l.redo_data;
      break;
    case LogType::kUpdateLeaf:
    case LogType::kUpdateRow:
    case LogType::kSetPrevNext:
      e << l.redo_data << l.undo_data;
      break;
    case LogType::kDeleteLeaf:
    case LogType::kDeleteRow:
      e << l.undo_data;
      break;
    case LogType::kInsertBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
    case LogType::kLowestValue:
      e << l.redo_page;
      break;
    case LogType::kUpdateBranch:
      e << l.redo_page << l.undo_page;
      break;
    case LogType::kDeleteBranch:
      e << l.undo_page;
      break;
    case LogType::kEndCheckpoint: {
      e << l.dirty_page_table << l.active_transaction_table;
      break;
    }
    case LogType::kSystemAllocPage:
      e << (uint64_t&)l.allocated_page_type;
      break;
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertBranch:
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kSystemDestroyPage:
      // Nothing to do.
      break;
  }
  return e;
}

Decoder& operator>>(Decoder& d, LogRecord& l) {
  l.Clear();
  d >> (uint16_t&)l.type >> l.prev_lsn >> l.txn_id;
  uint8_t types;
  d >> types;
  if (types & kHasPageID) {
    d >> l.pid;
  }
  if (types & kHasSlot) {
    d >> l.slot;
  }
  if (types & kHasKey) {
    d >> l.key;
  }
  switch (l.type) {
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertBranch:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
      break;
    case LogType::kInsertRow:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
      d >> l.redo_data;
      break;
    case LogType::kUpdateRow:
    case LogType::kUpdateLeaf:
    case LogType::kSetPrevNext:
      d >> l.redo_data >> l.undo_data;
      break;
    case LogType::kDeleteRow:
    case LogType::kDeleteLeaf:
      d >> l.undo_data;
      break;
    case LogType::kInsertBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
    case LogType::kLowestValue:
      d >> l.redo_page;
      break;
    case LogType::kUpdateBranch:
      d >> l.redo_page >> l.undo_page;
      break;
    case LogType::kDeleteBranch:
      d >> l.undo_page;
      break;
    case LogType::kSystemAllocPage:
      d >> (uint64_t&)l.allocated_page_type;
      break;
    case LogType::kEndCheckpoint: {
      d >> l.dirty_page_table >> l.active_transaction_table;
      break;
    }
    default:
      LOG(ERROR) << "unknown log type: " << l.type;
      assert(!"unknown log");
  }
  return d;
}

std::string LogRecord::Serialize() const {
  std::stringstream ss;
  Encoder enc(ss);
  enc << *this;
  return ss.str();
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
