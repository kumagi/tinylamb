/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "log_record.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "checkpoint_manager.hpp"
#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/debug.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "page/foster_pair.hpp"
#include "page/index_key.hpp"
#include "page/page_type.hpp"

namespace {

enum KeyTypes : uint8_t {
  kHasPageID = 0x1,
  kHasSlot = 0x2,
  kHasKey = 0x4,
};

[[maybe_unused]] std::string OmittedString(std::string_view original,
                                           size_t length) {
  // Mirrors common/debug.cpp: abbreviate only when the 8-byte head and tail
  // stay disjoint, so the elided-byte arithmetic cannot underflow for
  // mid-sized keys (it printed a huge garbage count for those before).
  if (length + 16 <= original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 4) + "bytes)..";
    omitted_key += original.substr(original.length() - 8);
    return omitted_key;
  }
  return std::string(original);
}

uint32_t SerializedWireVersion(const tinylamb::LogRecord& record) {
  if (record.type == tinylamb::LogType::kSystemAllocPage &&
      record.wire_version < tinylamb::kAllocationPayloadWalVersion) {
    return record.wire_version;
  }
  return tinylamb::kWalRecordVersion;
}

bool ValidV6AllocationPayload(const tinylamb::LogRecord& record) {
  using tinylamb::PageAllocationKind;
  using tinylamb::PageType;
  tinylamb::page_id_t first_free = 0;
  tinylamb::page_id_t max_page = 0;
  if (record.pid == 0 || record.allocated_page_type == PageType::kUnknown ||
      record.allocated_page_type == PageType::kMetaPage ||
      !tinylamb::DecodeAllocationMetaPayload(record.redo_data, &first_free,
                                             &max_page)) {
    return false;
  }
  switch (record.allocation_kind) {
    case PageAllocationKind::kFresh:
      return record.previous_page_type == PageType::kFreePage &&
             record.undo_data.empty() &&
             max_page != std::numeric_limits<tinylamb::page_id_t>::max() &&
             max_page + 1 == record.pid;
    case PageAllocationKind::kRecycled: {
      if (record.previous_page_type != PageType::kFreePage ||
          record.undo_data.size() != tinylamb::kAllocationFreePayloadSize ||
          first_free != record.pid) {
        return false;
      }
      tinylamb::page_id_t next_free = 0;
      tinylamb::txn_id_t destroyer = 0;
      return tinylamb::DecodeAllocationFreePayload(record.undo_data, &next_free,
                                                   &destroyer) &&
             (next_free == 0 || next_free != record.pid);
    }
    case PageAllocationKind::kInPlace:
      return record.previous_page_type != PageType::kMetaPage &&
             first_free <= max_page &&
             record.undo_data.size() == tinylamb::kPageBodySize;
  }
  return false;
}

class StringAppendBuffer final : public std::streambuf {
 public:
  explicit StringAppendBuffer(std::string& output) : output_(output) {}

 protected:
  std::streamsize xsputn(const char* data, std::streamsize count) override {
    if (count <= 0) {
      return 0;
    }
    output_.append(data, static_cast<size_t>(count));
    return count;
  }

  int_type overflow(int_type ch) override {
    if (std::char_traits<char>::eq_int_type(ch,
                                            std::char_traits<char>::eof())) {
      return std::char_traits<char>::not_eof(ch);
    }
    output_.push_back(std::char_traits<char>::to_char_type(ch));
    return ch;
  }

 private:
  std::string& output_;
};

}  // namespace

namespace tinylamb {

bool IsCompensation(LogType type) {
  switch (type) {
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateDeleteBranch:
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
    case LogType::kCompensateDestroyPage:
    case LogType::kCompensateSetLowestValue:
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
    case LogType::kCompensateAllocPage:
      return true;
    default:
      return false;
  }
}

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
    case LogType::kSetLowFence:
      o << "SET LOW FENCE\t";
      break;
    case LogType::kSetHighFence:
      o << "SET HIGH FENCE\t";
      break;
    case LogType::kSetFoster:
      o << "SET FOSTER\t";
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
    case LogType::kCompensateSetLowFence:
      o << "COMPENSATE SET LOW FENCE\t";
      break;
    case LogType::kCompensateSetHighFence:
      o << "COMPENSATE SET HIGH FENCE\t";
      break;
    case LogType::kCompensateSetFoster:
      o << "COMPENSATE SET FOSTER\t";
      break;
    case LogType::kCompensateSetLowestValue:
      o << "COMPENSATE SET LOWEST VALUE\t";
      break;
    case LogType::kSetRowPageNext:
      o << "SET ROW PAGE NEXT\t";
      break;
    case LogType::kSetRowPagePrev:
      o << "SET ROW PAGE PREV\t";
      break;
    case LogType::kCompensateSetRowPageNext:
      o << "COMPENSATE SET ROW PAGE NEXT\t";
      break;
    case LogType::kCompensateSetRowPagePrev:
      o << "COMPENSATE SET ROW PAGE PREV\t";
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
    case LogType::kCompensateDestroyPage:
      o << "COMPENSATE DESTROY\t";
      break;
    case LogType::kCompensateAllocPage:
      o << "COMPENSATE ALLOCATE\t";
      break;
    case LogType::kFullPageImage:
      o << "FULL PAGE IMAGE\t";
      break;
    default:
      o << "(undefined: " << static_cast<uint16_t>(type) << ")";
  }
  return o;
}

std::string EncodeAllocationMetaPayload(page_id_t first_free_page,
                                        page_id_t max_page_count) {
  std::string payload(kAllocationMetaPayloadSize, '\0');
  SerializeU64(payload.data(), first_free_page);
  SerializeU64(payload.data() + sizeof(uint64_t), max_page_count);
  return payload;
}

std::string EncodeAllocationFreePayload(page_id_t next_free_page,
                                        txn_id_t destroyer) {
  std::string payload(kAllocationFreePayloadSize, '\0');
  SerializeU64(payload.data(), next_free_page);
  SerializeU64(payload.data() + sizeof(uint64_t), destroyer);
  return payload;
}

bool DecodeAllocationMetaPayload(std::string_view payload,
                                 page_id_t* first_free_page,
                                 page_id_t* max_page_count) {
  if (payload.size() != kAllocationMetaPayloadSize ||
      first_free_page == nullptr || max_page_count == nullptr) {
    return false;
  }
  // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
  DeserializeU64(payload.data(), first_free_page);
  // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
  DeserializeU64(payload.data() + sizeof(uint64_t), max_page_count);
  return true;
}

bool DecodeAllocationFreePayload(std::string_view payload,
                                 page_id_t* next_free_page,
                                 txn_id_t* destroyer) {
  if (payload.size() != kAllocationFreePayloadSize ||
      next_free_page == nullptr || destroyer == nullptr) {
    return false;
  }
  // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
  DeserializeU64(payload.data(), next_free_page);
  // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
  DeserializeU64(payload.data() + sizeof(uint64_t), destroyer);
  return true;
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
      o << "\t Insert: " << l.redo_page;
      break;
    case LogType::kUpdateBranch:
      l.DumpPosition(o);
      o << "\t Update: " << l.undo_page << " -> " << l.redo_page;
      break;
    case LogType::kDeleteBranch:
      l.DumpPosition(o);
      o << "\t Delete: " << l.undo_page;
      break;
    case LogType::kLowestValue:
    case LogType::kCompensateSetLowestValue:
      l.DumpPosition(o);
      o << "\t Lowest: " << l.undo_page << " -> " << l.redo_page;
      break;
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      l.DumpPosition(o);
      o << "\t Link: " << l.undo_page << " -> " << l.redo_page;
      break;
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
      l.DumpPosition(o);
      o << "\t Link: " << l.redo_page;
      break;
    case LogType::kBeginCheckpoint:
      return o;
    case LogType::kEndCheckpoint:
      o << "\t DPT: {";
      for (const auto& dpt : l.dirty_page_table) {
        o << dpt.first << ": " << dpt.second << ", ";
      }
      o << "}\t TT: {";
      for (const auto& tt : l.active_transaction_table) {
        o << tt << ", ";
      }
      o << "}";
      return o;
    case LogType::kSetLowFence:
    case LogType::kSetHighFence:
    case LogType::kSetFoster:
      o << "\t Update: " << l.undo_data.size() << " -> " << l.redo_data.size();
      break;
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
      o << "\t Update: " << l.redo_data.size();
      break;
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateDestroyPage:
    case LogType::kCompensateAllocPage:
    case LogType::kFullPageImage:
      break;
  }
  o << "\tprev_lsn: " << l.prev_lsn << "\ttxn_id: " << l.txn_id;
  return o;
}

LogRecord LogRecord::FullPageImageLogRecord(page_id_t pid, PageType page_type,
                                            std::string_view body) {
  LogRecord l;
  l.pid = pid;
  l.type = LogType::kFullPageImage;
  l.allocated_page_type = page_type;
  l.redo_data = body;
  return l;
}

LogRecord::LogRecord(lsn_t prev, txn_id_t txn, LogType t)
    : type(t), prev_lsn(prev), txn_id(txn) {}

LogRecord LogRecord::InsertingLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                        page_id_t pid, slot_t key,
                                        std::string_view redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = key;
  l.type = LogType::kInsertRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::InsertingLeafLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                            page_id_t pid, std::string_view key,
                                            std::string_view redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kInsertLeaf;
  l.redo_data = redo;
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
                                                 slot_t key, lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = key;
  l.type = LogType::kCompensateInsertRow;
  return l;
}

LogRecord LogRecord::CompensatingInsertLogRecord(txn_id_t txn, page_id_t pid,
                                                 std::string_view key,
                                                 lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateInsertLeaf;
  return l;
}

LogRecord LogRecord::CompensatingInsertBranchLogRecord(txn_id_t txn,
                                                       page_id_t pid,
                                                       std::string_view key,
                                                       lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
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

LogRecord LogRecord::UpdatingBranchLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                             page_id_t pid,
                                             std::string_view key,
                                             page_id_t redo, page_id_t undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kUpdateBranch;
  l.redo_page = redo;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateLogRecord(txn_id_t txn, page_id_t pid,
                                                 slot_t key,
                                                 std::string_view redo,
                                                 lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = key;
  l.type = LogType::kCompensateUpdateRow;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateLeafLogRecord(lsn_t txn, page_id_t pid,
                                                     std::string_view key,
                                                     std::string_view redo,
                                                     lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateUpdateLeaf;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingUpdateBranchLogRecord(lsn_t txn, page_id_t pid,
                                                       std::string_view key,
                                                       page_id_t redo,
                                                       lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateUpdateBranch;
  l.redo_page = redo;
  return l;
}

LogRecord LogRecord::DeletingLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                       page_id_t pid, slot_t slot,
                                       std::string_view undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.slot = slot;
  l.type = LogType::kDeleteRow;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::DeletingLeafLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                           page_id_t pid, std::string_view key,
                                           std::string_view undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kDeleteLeaf;
  l.undo_data = undo;
  return l;
}

LogRecord LogRecord::DeletingBranchLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                             page_id_t pid,
                                             std::string_view key,
                                             page_id_t undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kDeleteBranch;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteLogRecord(txn_id_t txn, page_id_t pid,
                                                 slot_t slot,
                                                 std::string_view redo,
                                                 lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
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
                                                     std::string_view redo,
                                                     lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = key;
  l.type = LogType::kCompensateDeleteLeaf;
  l.redo_data = redo;
  return l;
}

LogRecord LogRecord::CompensatingDeleteBranchLogRecord(txn_id_t txn,
                                                       page_id_t pid,
                                                       std::string_view slot,
                                                       page_id_t redo,
                                                       lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.key = slot;
  l.type = LogType::kCompensateDeleteBranch;
  l.redo_page = redo;
  return l;
}

LogRecord LogRecord::SetLowFenceLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                          page_id_t pid, const IndexKey& redo,
                                          const IndexKey& undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSetLowFence;
  l.redo_data = Encode(redo);
  l.undo_data = Encode(undo);
  return l;
}

LogRecord LogRecord::SetHighFenceLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                           page_id_t pid, const IndexKey& redo,
                                           const IndexKey& undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSetHighFence;
  l.redo_data = Encode(redo);
  l.undo_data = Encode(undo);
  return l;
}

LogRecord LogRecord::CompensateSetLowFenceLogRecord(lsn_t prev_lsn,
                                                    txn_id_t txn, page_id_t pid,
                                                    const IndexKey& redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kCompensateSetLowFence;
  l.redo_data = Encode(redo);
  return l;
}

LogRecord LogRecord::CompensateSetHighFenceLogRecord(lsn_t prev_lsn,
                                                     txn_id_t txn,
                                                     page_id_t pid,
                                                     const IndexKey& redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kCompensateSetHighFence;
  l.redo_data = Encode(redo);
  return l;
}

LogRecord LogRecord::SetFosterLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                        page_id_t pid, const FosterPair& redo,
                                        const FosterPair& undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSetFoster;
  l.redo_data = Encode(redo);
  l.undo_data = Encode(undo);
  return l;
}
LogRecord LogRecord::CompensateSetFosterLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                                  page_id_t pid,
                                                  const FosterPair& redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kCompensateSetFoster;
  l.redo_data = Encode(redo);
  return l;
}

LogRecord LogRecord::SetLowestLogRecord(lsn_t prev_lsn, txn_id_t tid,
                                        page_id_t pid, page_id_t redo,
                                        page_id_t undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = tid;
  l.pid = pid;
  l.type = LogType::kLowestValue;
  l.redo_page = redo;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::CompensateSetLowestValueLogRecord(txn_id_t tid,
                                                       page_id_t pid,
                                                       page_id_t redo,
                                                       lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = tid;
  l.pid = pid;
  l.type = LogType::kCompensateSetLowestValue;
  l.redo_page = redo;
  l.undo_page = redo;
  return l;
}

LogRecord LogRecord::SetRowPageNextLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                             page_id_t pid, page_id_t redo,
                                             page_id_t undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSetRowPageNext;
  l.redo_page = redo;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::SetRowPagePrevLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                             page_id_t pid, page_id_t redo,
                                             page_id_t undo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSetRowPagePrev;
  l.redo_page = redo;
  l.undo_page = undo;
  return l;
}

LogRecord LogRecord::CompensateSetRowPageNextLogRecord(lsn_t prev_lsn,
                                                       txn_id_t txn,
                                                       page_id_t pid,
                                                       page_id_t redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kCompensateSetRowPageNext;
  l.redo_page = redo;
  return l;
}

LogRecord LogRecord::CompensateSetRowPagePrevLogRecord(lsn_t prev_lsn,
                                                       txn_id_t txn,
                                                       page_id_t pid,
                                                       page_id_t redo) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kCompensateSetRowPagePrev;
  l.redo_page = redo;
  return l;
}

LogRecord LogRecord::AllocatePageLogRecord(
    lsn_t prev_lsn, txn_id_t txn, page_id_t pid, PageType new_page_type,
    PageAllocationKind allocation_kind, PageType old_page_type,
    std::string old_target_payload, std::string old_meta_payload) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSystemAllocPage;
  l.allocated_page_type = new_page_type;
  l.previous_page_type = old_page_type;
  l.allocation_kind = allocation_kind;
  l.undo_data = std::move(old_target_payload);
  l.redo_data = std::move(old_meta_payload);
  CHECK_MSG(ValidV6AllocationPayload(l), "invalid v6 allocation payload");
  return l;
}

LogRecord LogRecord::AllocatePageLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                           page_id_t pid,
                                           PageType new_page_type) {
  CHECK_MSG(pid > 0, "fresh allocation page id must be positive");
  return AllocatePageLogRecord(
      prev_lsn, txn, pid, new_page_type, PageAllocationKind::kFresh,
      PageType::kFreePage, {}, EncodeAllocationMetaPayload(pid - 1, pid - 1));
}

LogRecord LogRecord::CompensateAllocatePageLogRecord(
    const LogRecord& allocation, lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = allocation.txn_id;
  l.pid = allocation.pid;
  l.type = LogType::kCompensateAllocPage;
  l.allocated_page_type = allocation.allocated_page_type;
  l.previous_page_type = allocation.previous_page_type;
  l.allocation_kind = allocation.allocation_kind;
  l.undo_data = allocation.undo_data;
  l.redo_data = allocation.redo_data;
  CHECK_MSG(ValidV6AllocationPayload(l), "invalid v6 allocation CLR payload");
  return l;
}

LogRecord LogRecord::DestroyPageLogRecord(lsn_t prev_lsn, txn_id_t txn,
                                          page_id_t pid, PageType old_page_type,
                                          std::string old_page_body) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kSystemDestroyPage;
  l.allocated_page_type = old_page_type;
  l.undo_data = std::move(old_page_body);
  return l;
}

LogRecord LogRecord::CompensateDestroyPageLogRecord(txn_id_t txn, page_id_t pid,
                                                    PageType restored_type,
                                                    std::string restored_body,
                                                    lsn_t prev_lsn) {
  LogRecord l;
  l.prev_lsn = prev_lsn;
  l.txn_id = txn;
  l.pid = pid;
  l.type = LogType::kCompensateDestroyPage;
  l.allocated_page_type = restored_type;
  l.undo_data = std::move(restored_body);
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
  previous_page_type = PageType::kUnknown;
  allocation_kind = PageAllocationKind::kFresh;
  wire_version = kWalRecordVersion;
}

size_t LogRecord::Size() const {
  size_t size = sizeof(kSerdesMagic) + sizeof(kSerdesVersion) + sizeof(type) +
                sizeof(prev_lsn) + sizeof(txn_id);
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
      // kUnknown has no defined record layout: this is a caller bug, not a
      // runtime condition (recovery rejects unknown types before sizing).
      CHECK_MSG(false, "Don't call Size() of unknown log");
      break;
    case LogType::kInsertRow:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
      size += SerializeSize(redo_data);
      break;
    case LogType::kUpdateLeaf:
    case LogType::kUpdateRow:
    case LogType::kSetLowFence:
    case LogType::kSetHighFence:
    case LogType::kSetFoster:
      size += SerializeSize(redo_data);
      size += SerializeSize(undo_data);
      break;
    case LogType::kDeleteLeaf:
    case LogType::kDeleteRow:
      size += SerializeSize(undo_data);
      break;
    case LogType::kInsertBranch:
    case LogType::kDeleteBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
      size += sizeof(page_id_t);
      break;
    case LogType::kUpdateBranch:
    case LogType::kLowestValue:
    case LogType::kCompensateSetLowestValue:
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      size += sizeof(page_id_t) * 2;
      break;
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
      size += sizeof(page_id_t);
      break;
    case LogType::kEndCheckpoint:
      size += sizeof(dirty_page_table.size()) +
              (dirty_page_table.size() * sizeof(std::pair<page_id_t, lsn_t>));
      size += sizeof(active_transaction_table.size()) +
              (active_transaction_table.size() *
               CheckpointManager::ActiveTransactionEntry::Size());
      break;
    case LogType::kSystemAllocPage:
      size += sizeof(uint64_t);
      if (wire_version >= kAllocationPayloadWalVersion) {
        size += sizeof(uint64_t);
        size += sizeof(uint8_t);
        size += SerializeSize(undo_data);
        size += SerializeSize(redo_data);
      }
      break;
    case LogType::kCompensateAllocPage:
      size += sizeof(uint64_t);
      size += sizeof(uint64_t);
      size += sizeof(uint8_t);
      size += SerializeSize(undo_data);
      size += SerializeSize(redo_data);
      break;
    case LogType::kSystemDestroyPage:
      // D3 (docs/design.md): v2+ carries the destroyed page's type and an
      // optional body image; v1 destroy records were pid-only.
      if (wire_version >= kDestroyPayloadWalVersion) {
        size += sizeof(uint64_t);
        size += SerializeSize(undo_data);
      }
      break;
    case LogType::kCompensateDestroyPage:
      // Only ever written at the current version: restored type + body.
      size += sizeof(uint64_t);
      size += SerializeSize(undo_data);
      break;
    case LogType::kFullPageImage:
      // Type + the complete page body in redo_data.
      size += sizeof(uint64_t);
      size += SerializeSize(redo_data);
      break;
    case LogType::kBegin:
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateInsertRow:
    case LogType::kCommit:
    case LogType::kCompensateInsertLeaf:
      break;
  }
  // D9 (docs/design.md): v3 records end with a CRC32C field that Size()
  // includes so the recovery scan advances byte-exactly over the checksum.
  if (wire_version >= kCrcWalRecordVersion) {
    size += kWalRecordCrcSize;
  }
  return size;
}

Encoder& operator<<(Encoder& e, const LogRecord& l) {
  e << kSerdesMagic << SerializedWireVersion(l) << static_cast<uint16_t>(l.type)
    << l.prev_lsn << l.txn_id;
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
      // Fail loudly instead of serializing a garbage record.
      CHECK_MSG(false, "unknown type log must not be serialized");
      break;
    case LogType::kInsertRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
      e << l.redo_data;
      break;
    case LogType::kSetLowFence:
    case LogType::kSetHighFence:
    case LogType::kSetFoster:
    case LogType::kUpdateLeaf:
    case LogType::kUpdateRow:
      e << l.redo_data << l.undo_data;
      break;
    case LogType::kDeleteLeaf:
    case LogType::kDeleteRow:
      e << l.undo_data;
      break;
    case LogType::kInsertBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
      e << l.redo_page;
      break;
    case LogType::kUpdateBranch:
    case LogType::kLowestValue:
    case LogType::kCompensateSetLowestValue:
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      e << l.redo_page << l.undo_page;
      break;
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
      e << l.redo_page;
      break;
    case LogType::kDeleteBranch:
      e << l.undo_page;
      break;
    case LogType::kEndCheckpoint: {
      e << l.dirty_page_table << l.active_transaction_table;
      break;
    }
    case LogType::kSystemAllocPage:
      e << static_cast<uint64_t>(l.allocated_page_type);
      if (l.wire_version >= kAllocationPayloadWalVersion) {
        e << static_cast<uint64_t>(l.previous_page_type)
          << static_cast<uint8_t>(l.allocation_kind) << l.undo_data
          << l.redo_data;
      }
      break;
    case LogType::kCompensateAllocPage:
      e << static_cast<uint64_t>(l.allocated_page_type)
        << static_cast<uint64_t>(l.previous_page_type)
        << static_cast<uint8_t>(l.allocation_kind) << l.undo_data
        << l.redo_data;
      break;
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateDestroyPage:
      // D3 (docs/design.md): type plus optional body image of the destroyed
      // page, so undo of an aborted destroy restores it exactly. The undo
      // (compensate) record shares the destroy record's payload shape.
      e << static_cast<uint64_t>(l.allocated_page_type) << l.undo_data;
      break;
    case LogType::kFullPageImage:
      // System record: page type plus the complete body image in redo_data.
      e << static_cast<uint64_t>(l.allocated_page_type) << l.redo_data;
      break;
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertBranch:
    case LogType::kBegin:
    case LogType::kCommit:
      // Nothing to do.
      break;
  }
  return e;
}

// Deserializes a LogRecord.
//
// Stream-state contract: this operator does not consume the Decoder's
// underlying istream state itself; a truncated payload yields zero-filled
// fields (Decoder has no fail accessor). Callers that must reject torn
// records (e.g. RecoveryManager::ReadLog) validate the stream state and the
// decoded LogType around this call. An undefined/unknown LogType fails
// loudly below instead of returning a half-decoded record silently.
Decoder& operator>>(Decoder& d, LogRecord& l) {
  l.Clear();
  uint32_t magic = 0;
  uint32_t version = 0;
  d >> magic >> version;
  if (magic != kSerdesMagic) {
    d.Fail();
    return d;
  }
  if (version < kLegacyWalRecordVersion || version > kWalRecordVersion) {
    d.Fail();
    return d;
  }
  l.wire_version = version;
  uint16_t type_raw = 0;
  d >> type_raw >> l.prev_lsn >> l.txn_id;
  l.type = static_cast<LogType>(type_raw);
  // NOTE: a truncated header cannot be observed here (Decoder exposes no
  // stream-state accessor); ReadLog validates the stream around this call.
  uint8_t types = 0;
  d >> types;
  if ((types & kHasPageID) != 0) {
    d >> l.pid;
  }
  if ((types & kHasSlot) != 0) {
    d >> l.slot;
  }
  if ((types & kHasKey) != 0) {
    d >> l.key;
  }
  switch (l.type) {
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
      break;
    case LogType::kSystemDestroyPage:
      // D3 (docs/design.md): v2+ destroy records carry the destroyed page's
      // type and optional body image.  A v1 record decodes to the legacy
      // pid-only form (kUnknown type, empty body).
      if (l.wire_version >= kDestroyPayloadWalVersion) {
        d >> l.allocated_page_type;
        d >> l.undo_data;
      }
      break;
    case LogType::kCompensateDestroyPage:
      d >> l.allocated_page_type;
      d >> l.undo_data;
      break;
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      if (l.wire_version < kRowPageLinkWalVersion) {
        d.Fail();
        return d;
      }
      d >> l.redo_page >> l.undo_page;
      break;
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
      if (l.wire_version < kRowPageLinkWalVersion) {
        d.Fail();
        return d;
      }
      d >> l.redo_page;
      break;
    case LogType::kFullPageImage:
      d >> l.allocated_page_type;
      d >> l.redo_data;
      break;
    case LogType::kInsertRow:
    case LogType::kInsertLeaf:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
      d >> l.redo_data;
      break;
    case LogType::kUpdateRow:
    case LogType::kUpdateLeaf:
    case LogType::kSetLowFence:
    case LogType::kSetHighFence:
    case LogType::kSetFoster:
      d >> l.redo_data >> l.undo_data;
      break;
    case LogType::kDeleteRow:
    case LogType::kDeleteLeaf:
      d >> l.undo_data;
      break;
    case LogType::kInsertBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
      d >> l.redo_page;
      break;
    case LogType::kUpdateBranch:
    case LogType::kLowestValue:
    case LogType::kCompensateSetLowestValue:
      d >> l.redo_page >> l.undo_page;
      break;
    case LogType::kDeleteBranch:
      d >> l.undo_page;
      break;
    case LogType::kSystemAllocPage:
      d >> l.allocated_page_type;
      if (l.wire_version >= kAllocationPayloadWalVersion) {
        uint8_t allocation_kind = 0;
        d >> l.previous_page_type >> allocation_kind >> l.undo_data >>
            l.redo_data;
        if (allocation_kind >
            static_cast<uint8_t>(PageAllocationKind::kInPlace)) {
          d.Fail();
          return d;
        }
        l.allocation_kind = static_cast<PageAllocationKind>(allocation_kind);
        if (!ValidV6AllocationPayload(l)) {
          d.Fail();
          return d;
        }
      }
      break;
    case LogType::kCompensateAllocPage:
      if (l.wire_version < kAllocationPayloadWalVersion) {
        d.Fail();
        return d;
      }
      {
        uint8_t allocation_kind = 0;
        d >> l.allocated_page_type >> l.previous_page_type >> allocation_kind >>
            l.undo_data >> l.redo_data;
        if (allocation_kind >
            static_cast<uint8_t>(PageAllocationKind::kInPlace)) {
          d.Fail();
          return d;
        }
        l.allocation_kind = static_cast<PageAllocationKind>(allocation_kind);
        if (!ValidV6AllocationPayload(l)) {
          d.Fail();
          return d;
        }
      }
      break;
    case LogType::kEndCheckpoint: {
      d >> l.dirty_page_table >> l.active_transaction_table;
      break;
    }
    default:
      // Never return a half-decoded record with an undefined type: reject
      // through the decoder's sticky fail state so RecoveryManager can skip
      // torn tails and the fuzzers treat it as ordinary rejection.
      LOG(ERROR) << "unknown log type: " << l.type;
      d.Fail();
      break;
  }
  return d;
}

void LogRecord::SerializeTo(std::string& output) const {
  const uint32_t output_version = SerializedWireVersion(*this);
  size_t current_size = Size();
  if (wire_version < kCrcWalRecordVersion &&
      output_version >= kCrcWalRecordVersion) {
    current_size += kWalRecordCrcSize;
  }
  if (type == LogType::kSystemDestroyPage &&
      wire_version < kDestroyPayloadWalVersion) {
    current_size += sizeof(uint64_t) + SerializeSize(undo_data);
  }
  const size_t start = output.size();
  output.reserve(start + current_size);

  StringAppendBuffer buffer(output);
  std::ostream stream(&buffer);
  Encoder encoder(stream);
  encoder << *this;

  const size_t body_size = output.size() - start;
  if (output_version < kCrcWalRecordVersion) {
    return;
  }
  const uint32_t crc = Crc32C(output.data() + start, body_size);
  output.resize(start + body_size + kWalRecordCrcSize);
  char* crc_bytes = output.data() + start + body_size;
  crc_bytes[0] = static_cast<char>((crc >> 24) & 0xFF);
  crc_bytes[1] = static_cast<char>((crc >> 16) & 0xFF);
  crc_bytes[2] = static_cast<char>((crc >> 8) & 0xFF);
  crc_bytes[3] = static_cast<char>(crc & 0xFF);
}

std::string LogRecord::Serialize() const {
  std::string record;
  SerializeTo(record);
  return record;
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
