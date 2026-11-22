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

#include "recovery/log_record.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>

#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"

namespace tinylamb {

namespace {
constexpr size_t kHeaderBytes = 2 + 2 + 4 + 8 + 8 + 1 + 1;  // 26 bytes
constexpr size_t kTrailerBytes = 4;                         // CRC32C

void WriteU32(char* dst, uint32_t v) { std::memcpy(dst, &v, sizeof(uint32_t)); }
uint32_t ReadU32(const char* src) {
  uint32_t v = 0;
  std::memcpy(&v, src, sizeof(uint32_t));
  return v;
}
void WriteString32(char* dst, size_t* offset, std::string_view sv) {
  // T4253: callers must bound the record before serializing; truncating
  // here would make Size() and the written length disagree. The guard has
  // to survive NDEBUG (assert vanished in release fuzz/CI builds), and a
  // >4GiB payload is a caller contract violation -- Logger caps whole
  // records at kMaxRecordSize well below 4GiB -- so abort with a named
  // diagnosis instead of silently corrupting the frame.
  if (sv.size() > std::numeric_limits<uint32_t>::max()) {
    LOG(FATAL) << "WAL string payload exceeds 4GiB: " << sv.size();
  }
  const auto len = static_cast<uint32_t>(sv.size());
  WriteU32(dst + *offset, len);
  *offset += sizeof(uint32_t);
  if (len > 0) {
    std::memcpy(dst + *offset, sv.data(), len);
    *offset += len;
  }
}
bool ReadString32(const char* src, size_t max_bytes, size_t* offset,
                  std::string* out) {
  if (*offset + sizeof(uint32_t) > max_bytes) {
    return false;
  }
  const uint32_t len = ReadU32(src + *offset);
  *offset += sizeof(uint32_t);
  if (*offset + len > max_bytes) {
    return false;
  }
  out->assign(src + *offset, len);
  *offset += len;
  return true;
}
}  // namespace

LogRecord::LogRecord(lsn_t prev, txn_id_t txn, LogType log_type)
    : prev_lsn(prev), owner(txn), type(log_type) {}

LogRecord LogRecord::InsertingLogRecord(lsn_t prev, txn_id_t txn, page_id_t pid,
                                        slot_t slot, std::string_view redo) {
  LogRecord ret(prev, txn, LogType::kInsertRow);
  ret.pid = pid;
  ret.slot = slot;
  ret.redo_data.assign(redo);
  return ret;
}

LogRecord LogRecord::UpdatingLogRecord(lsn_t prev, txn_id_t txn, page_id_t pid,
                                       slot_t slot, std::string_view redo,
                                       std::string_view undo) {
  LogRecord ret(prev, txn, LogType::kUpdateRow);
  ret.pid = pid;
  ret.slot = slot;
  ret.redo_data.assign(redo);
  ret.undo_data.assign(undo);
  return ret;
}

LogRecord LogRecord::CompensateUpdatingLogRecord(lsn_t prev, txn_id_t txn,
                                                 page_id_t pid, slot_t slot,
                                                 std::string_view redo) {
  LogRecord ret(prev, txn, LogType::kCompensateUpdate);
  ret.pid = pid;
  ret.slot = slot;
  ret.redo_data.assign(redo);
  return ret;
}

LogRecord LogRecord::DeletingLogRecord(lsn_t prev, txn_id_t txn, page_id_t pid,
                                       slot_t slot, std::string_view undo) {
  LogRecord ret(prev, txn, LogType::kDeleteRow);
  ret.pid = pid;
  ret.slot = slot;
  ret.undo_data.assign(undo);
  return ret;
}

LogRecord LogRecord::SetRowPageNextLogRecord(lsn_t prev, txn_id_t txn,
                                             page_id_t pid, page_id_t redo,
                                             page_id_t undo) {
  LogRecord ret(prev, txn, LogType::kSetRowPageNext);
  ret.pid = pid;
  ret.redo_pid = redo;
  ret.undo_pid = undo;
  return ret;
}

LogRecord LogRecord::SetRowPagePrevLogRecord(lsn_t prev, txn_id_t txn,
                                             page_id_t pid, page_id_t redo,
                                             page_id_t undo) {
  LogRecord ret(prev, txn, LogType::kSetRowPagePrev);
  ret.pid = pid;
  ret.redo_pid = redo;
  ret.undo_pid = undo;
  return ret;
}

LogRecord LogRecord::AllocatePageLogRecord(lsn_t prev, txn_id_t txn,
                                           page_id_t page_id,
                                           PageType new_page_type) {
  LogRecord ret(prev, txn, LogType::kSystemAllocPage);
  ret.pid = page_id;
  ret.allocated_page_type = new_page_type;
  return ret;
}

LogRecord LogRecord::DestroyPageLogRecord(lsn_t prev, txn_id_t txn,
                                          page_id_t page_id) {
  LogRecord ret(prev, txn, LogType::kSystemDestroyPage);
  ret.pid = page_id;
  return ret;
}

LogRecord LogRecord::LsmPutLogRecord(lsn_t prev, txn_id_t txn,
                                     page_id_t tree_id, std::string_view key,
                                     std::string_view value,
                                     std::string_view prev_value,
                                     bool had_prev) {
  LogRecord ret(prev, txn, LogType::kLsmPut);
  ret.pid = tree_id;
  ret.key.assign(key);
  ret.redo_data.assign(value);
  ret.undo_data.assign(prev_value);
  ret.lsm_had_prev = had_prev;
  return ret;
}

LogRecord LogRecord::LsmDeleteLogRecord(lsn_t prev, txn_id_t txn,
                                        page_id_t tree_id, std::string_view key,
                                        std::string_view prev_value) {
  LogRecord ret(prev, txn, LogType::kLsmDelete);
  ret.pid = tree_id;
  ret.key.assign(key);
  ret.undo_data.assign(prev_value);
  ret.lsm_had_prev = true;
  return ret;
}

LogRecord LogRecord::LsmDestroyLogRecord(lsn_t prev, txn_id_t txn,
                                         page_id_t tree_id) {
  LogRecord ret(prev, txn, LogType::kLsmDestroy);
  ret.pid = tree_id;
  return ret;
}

size_t LogRecord::Size() const {
  size_t payload = 0;
  switch (type) {
    case LogType::kUnknown:
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kAbort:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      payload = 0;
      break;
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
    case LogType::kLsmDestroy:
      payload = sizeof(page_id_t);
      break;
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      payload = sizeof(page_id_t) * 3;
      break;
    case LogType::kInsertRow:
    case LogType::kCompensateUpdate:
      payload = sizeof(page_id_t) + sizeof(slot_t) + 4 + redo_data.size();
      break;
    case LogType::kDeleteRow:
      payload = sizeof(page_id_t) + sizeof(slot_t) + 4 + undo_data.size();
      break;
    case LogType::kUpdateRow:
      payload = sizeof(page_id_t) + sizeof(slot_t) + 4 + redo_data.size() + 4 +
                undo_data.size();
      break;
    case LogType::kLsmPut:
      payload = sizeof(page_id_t) + 4 + key.size() + 4 + redo_data.size() + 4 +
                undo_data.size();
      break;
    case LogType::kLsmDelete:
      payload = sizeof(page_id_t) + 4 + key.size() + 4 + undo_data.size();
      break;
  }
  return kHeaderBytes + payload + kTrailerBytes;
}

void LogRecord::SerializeTo(std::string* dst) const {
  const size_t total = Size();
  // Size() and the type switch below cover [0, total) completely; zero-filling
  // first would overwrite every byte a second time on every WAL record.
  dst->resize(total);
  char* buf = dst->data();
  size_t off = 0;

  const uint16_t magic = kLogRecordMagic;
  const uint16_t version = kCurrentVersion;
  std::memcpy(buf + off, &magic, sizeof(uint16_t));
  off += sizeof(uint16_t);
  std::memcpy(buf + off, &version, sizeof(uint16_t));
  off += sizeof(uint16_t);
  WriteU32(buf + off, static_cast<uint32_t>(total));
  off += sizeof(uint32_t);
  off += SerializePID(buf + off, prev_lsn);
  off += SerializeU64(buf + off, owner);
  buf[off++] = static_cast<char>(type);
  const auto flags = static_cast<uint8_t>(
      (lsm_had_prev ? 1U : 0U) |
      (static_cast<unsigned>(static_cast<uint8_t>(allocated_page_type)) << 1U));
  buf[off++] = static_cast<char>(flags);

  switch (type) {
    case LogType::kUnknown:
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kAbort:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      break;
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
    case LogType::kLsmDestroy:
      off += SerializePID(buf + off, pid);
      break;
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      off += SerializePID(buf + off, pid);
      off += SerializePID(buf + off, redo_pid);
      off += SerializePID(buf + off, undo_pid);
      break;
    case LogType::kInsertRow:
    case LogType::kCompensateUpdate:
      off += SerializePID(buf + off, pid);
      off += SerializeSlot(buf + off, slot);
      WriteString32(buf, &off, redo_data);
      break;
    case LogType::kDeleteRow:
      off += SerializePID(buf + off, pid);
      off += SerializeSlot(buf + off, slot);
      WriteString32(buf, &off, undo_data);
      break;
    case LogType::kUpdateRow:
      off += SerializePID(buf + off, pid);
      off += SerializeSlot(buf + off, slot);
      WriteString32(buf, &off, redo_data);
      WriteString32(buf, &off, undo_data);
      break;
    case LogType::kLsmPut:
      off += SerializePID(buf + off, pid);
      WriteString32(buf, &off, key);
      WriteString32(buf, &off, redo_data);
      WriteString32(buf, &off, undo_data);
      break;
    case LogType::kLsmDelete:
      off += SerializePID(buf + off, pid);
      WriteString32(buf, &off, key);
      WriteString32(buf, &off, undo_data);
      break;
  }

  // The CRC lives in the fixed trailer slot derived from Size(), not at
  // whatever offset the type switch happened to reach.  If Size() and the
  // switch ever drift, writing at `off` would put the CRC outside the trailer
  // (or leave uninitialized bytes) and make every record look corrupt.
  const size_t crc_offset = total - sizeof(uint32_t);
  const uint32_t crc = Crc32C(buf, crc_offset);
  WriteU32(buf + crc_offset, crc);
}

std::string LogRecord::Serialize() const {
  std::string out;
  SerializeTo(&out);
  return out;
}

size_t LogRecord::Deserialize(const char* src, size_t max_bytes) {
  // A 0 return means "no bytes consumed" and callers may reuse this LogRecord
  // for the next record. Never leave a half-decoded record behind on failure:
  // reset to a fully invalid state on every path so stale type/pid/key data
  // from a previous decode cannot be mistaken for the skipped record.
  const auto fail = [this]() -> size_t {
    *this = LogRecord{};
    return size_t{0};
  };
  *this = LogRecord{};

  constexpr size_t kMinRecordBytes = kHeaderBytes + kTrailerBytes;
  if (src == nullptr || max_bytes < kMinRecordBytes) {
    return fail();
  }
  uint16_t magic = 0;
  uint16_t version = 0;
  std::memcpy(&magic, src, sizeof(uint16_t));
  std::memcpy(&version, src + sizeof(uint16_t), sizeof(uint16_t));
  if (magic != kLogRecordMagic || version != kCurrentVersion) {
    return fail();
  }
  const uint32_t total = ReadU32(src + 4);
  if (total < kMinRecordBytes || total > max_bytes) {
    return fail();
  }
  const uint32_t stored_crc = ReadU32(src + total - sizeof(uint32_t));
  const uint32_t computed_crc = Crc32C(src, total - sizeof(uint32_t));
  if (stored_crc != computed_crc) {
    return fail();
  }

  size_t off = 8;
  const size_t body_end = total - sizeof(uint32_t);
  if (body_end < off + sizeof(uint32_t) + sizeof(uint64_t) + 2) {
    return fail();  // too short to hold type/flags before the CRC trailer
  }
  off += DeserializePID(src + off, &prev_lsn);
  off += DeserializeU64(src + off, &owner);
  const auto raw_type = static_cast<uint8_t>(src[off++]);
  if (raw_type == 0 ||
      raw_type > static_cast<uint8_t>(LogType::kEndCheckpoint)) {
    return fail();
  }
  type = static_cast<LogType>(raw_type);
  const auto flags = static_cast<uint8_t>(src[off++]);
  lsm_had_prev = (flags & 1U) != 0;
  const uint8_t raw_page_type = flags >> 1U;
  // Writers only ever store kUnknown(0)/kRowPage(1); a hostile or torn flags
  // byte must not manufacture an undefined PageType enum value.
  if (raw_page_type > static_cast<uint8_t>(PageType::kRowPage)) {
    return fail();
  }
  allocated_page_type = static_cast<PageType>(raw_page_type);

  switch (type) {
    case LogType::kUnknown:
      return fail();
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kAbort:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      break;
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
    case LogType::kLsmDestroy:
      if (off + sizeof(page_id_t) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      break;
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
      if (off + (sizeof(page_id_t) * 3) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      off += DeserializePID(src + off, &redo_pid);
      off += DeserializePID(src + off, &undo_pid);
      break;
    case LogType::kInsertRow:
    case LogType::kCompensateUpdate:
      if (off + sizeof(page_id_t) + sizeof(slot_t) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      off += DeserializeSlot(src + off, &slot);
      if (!ReadString32(src, body_end, &off, &redo_data)) {
        return fail();
      }
      break;
    case LogType::kDeleteRow:
      if (off + sizeof(page_id_t) + sizeof(slot_t) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      off += DeserializeSlot(src + off, &slot);
      if (!ReadString32(src, body_end, &off, &undo_data)) {
        return fail();
      }
      break;
    case LogType::kUpdateRow:
      if (off + sizeof(page_id_t) + sizeof(slot_t) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      off += DeserializeSlot(src + off, &slot);
      if (!ReadString32(src, body_end, &off, &redo_data) ||
          !ReadString32(src, body_end, &off, &undo_data)) {
        return fail();
      }
      break;
    case LogType::kLsmPut:
      if (off + sizeof(page_id_t) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      if (!ReadString32(src, body_end, &off, &key) ||
          !ReadString32(src, body_end, &off, &redo_data) ||
          !ReadString32(src, body_end, &off, &undo_data)) {
        return fail();
      }
      break;
    case LogType::kLsmDelete:
      if (off + sizeof(page_id_t) > body_end) {
        return fail();
      }
      off += DeserializePID(src + off, &pid);
      if (!ReadString32(src, body_end, &off, &key) ||
          !ReadString32(src, body_end, &off, &undo_data)) {
        return fail();
      }
      break;
  }

  if (off != body_end) {
    return fail();
  }
  return total;
}

const char* LogTypeName(LogType type) {
  switch (type) {
    case LogType::kUnknown:
      return "kUnknown";
    case LogType::kBegin:
      return "kBegin";
    case LogType::kInsertRow:
      return "kInsertRow";
    case LogType::kUpdateRow:
      return "kUpdateRow";
    case LogType::kDeleteRow:
      return "kDeleteRow";
    case LogType::kCompensateUpdate:
      return "kCompensateUpdate";
    case LogType::kSetRowPageNext:
      return "kSetRowPageNext";
    case LogType::kSetRowPagePrev:
      return "kSetRowPagePrev";
    case LogType::kSystemAllocPage:
      return "kSystemAllocPage";
    case LogType::kSystemDestroyPage:
      return "kSystemDestroyPage";
    case LogType::kLsmPut:
      return "kLsmPut";
    case LogType::kLsmDelete:
      return "kLsmDelete";
    case LogType::kLsmDestroy:
      return "kLsmDestroy";
    case LogType::kCommit:
      return "kCommit";
    case LogType::kAbort:
      return "kAbort";
    case LogType::kBeginCheckpoint:
      return "kBeginCheckpoint";
    case LogType::kEndCheckpoint:
      return "kEndCheckpoint";
  }
  return "kInvalid";
}

std::ostream& operator<<(std::ostream& o, const LogRecord& r) {
  // T2115: the type renders symbolically (was a bare uint8), and pid/slot
  // only appear when they carry meaning: BEGIN/COMMIT/ABORT/checkpoint
  // records never touch them, so printing the numeric_limits::max()
  // sentinel made traces claim every record targeted page 4294967295.
  o << "{prev:" << r.prev_lsn << " txn:" << r.owner
    << " type:" << LogTypeName(r.type);
  if (r.pid != std::numeric_limits<page_id_t>::max()) {
    o << " pid:" << r.pid;
  }
  if (r.slot != std::numeric_limits<slot_t>::max()) {
    o << " slot:" << r.slot;
  }
  o << "}";
  return o;
}

}  // namespace tinylamb
