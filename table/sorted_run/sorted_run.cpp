/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "table/sorted_run/sorted_run.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

int SortedRun::Entry::Compare(std::string_view rhs,
                              const std::vector<char>& blob) const {
  // Keys are stored length-prefixed: key_offset_ points at a 4-byte BE
  // length, the key bytes follow at key_offset_+4. Entry stays a compact
  // (4+4+1) pointer pair; the length lives in the blob. (The former
  // implementation first built an unused string_view over the remaining
  // blob on every comparison -- dead code, removed.)
  uint32_t my_key_len = 0;
  // size_t arithmetic: a hostile key_offset_ near UINT32_MAX wrapped the
  // uint32 sum past blob.size() and read OOB (or the clamp subtraction
  // underflowed).
  if (static_cast<size_t>(key_offset_) + sizeof(uint32_t) > blob.size()) {
    // Corrupt/hostile offset: compare as empty rather than read OOB.
    return std::string_view().compare(rhs);
  }
  memcpy(&my_key_len, blob.data() + key_offset_, sizeof(uint32_t));
  my_key_len = be32toh(my_key_len);
  if (static_cast<size_t>(key_offset_) + sizeof(uint32_t) + my_key_len >
      blob.size()) {
    my_key_len = static_cast<uint32_t>(
        blob.size() - static_cast<size_t>(key_offset_) - sizeof(uint32_t));
  }
  std::string_view my_key_bytes(blob.data() + key_offset_ + sizeof(uint32_t),
                                my_key_len);
  return my_key_bytes.compare(rhs);  // neg/0/pos like string_view::compare
}

int SortedRun::Entry::Compare(const Entry& rhs,
                              const std::vector<char>& blob) const {
  // Compare keys: derive key bytes and lengths from both entries using the
  // 4-byte length prefix at key_offset_.
  auto key_at = [&blob](uint32_t off) -> std::string_view {
    if (static_cast<size_t>(off) + sizeof(uint32_t) > blob.size()) return {};
    uint32_t len = 0;
    memcpy(&len, blob.data() + off, sizeof(uint32_t));
    len = be32toh(len);
    if (static_cast<size_t>(off) + sizeof(uint32_t) + len > blob.size()) {
      len = static_cast<uint32_t>(blob.size() - static_cast<size_t>(off) -
                                  sizeof(uint32_t));
    }
    return {blob.data() + off + sizeof(uint32_t), len};
  };
  return key_at(key_offset_).compare(key_at(rhs.key_offset_));
}

std::string SortedRun::Entry::BuildKey(const std::vector<char>& blob) const {
  if (static_cast<size_t>(key_offset_) + sizeof(uint32_t) > blob.size())
    return {};
  uint32_t len = 0;
  memcpy(&len, blob.data() + key_offset_, sizeof(uint32_t));
  len = be32toh(len);
  if (static_cast<size_t>(key_offset_) + sizeof(uint32_t) + len > blob.size()) {
    len = static_cast<uint32_t>(blob.size() - static_cast<size_t>(key_offset_) -
                                sizeof(uint32_t));
  }
  return std::string(blob.data() + key_offset_ + sizeof(uint32_t), len);
}

Row SortedRun::Entry::BuildRow(const std::vector<char>& blob,
                               const Schema& sc) const {
  // A tombstone (MemTable::Sync sets row_off = key_off for deletes) has no
  // row payload at row_offset_: deserializing there interprets the 4-byte
  // BE key-length prefix as a row header and reads well past the entry
  // into unrelated blob bytes. Callers must gate on IsDeleted(); the
  // assert is stripped in Release, so refuse in code as well.
  if (is_deleted_) return Row{};
  if (row_offset_ >= blob.size()) return Row{};
  // DeserializeBounded validates the column count and every varchar
  // length prefix against the remaining bytes BEFORE touching them, so a
  // corrupted blob throws instead of reading past the end (the unbounded
  // Deserialize happily trusted hostile `count`/`len` fields).
  Row r;
  r.DeserializeBounded(blob.data() + row_offset_, blob.size() - row_offset_,
                       sc);
  return r;
}

bool SortedRun::Entry::BuildRowInto(const std::vector<char>& blob,
                                    const Schema& sc, Row* out) const {
  // Same guards as BuildRow: a tombstone's row_offset_ points at the key
  // length prefix, not at a payload.
  if (is_deleted_) return false;
  if (row_offset_ >= blob.size()) return false;
  // DeserializeBounded clears *out's cells and refills them; a caller
  // feeding rows of a stable shape in a loop therefore reuses the cell
  // buffer (and its capacity) instead of allocating per row.
  out->DeserializeBounded(blob.data() + row_offset_, blob.size() - row_offset_,
                          sc);
  return true;
}

std::ostream& operator<<(std::ostream& o, const SortedRun::Entry& e) {
  o << "key_off=" << e.key_offset_ << " row_off=" << e.row_offset_;
  if (e.is_deleted_) o << "(deleted)";
  return o;
}

size_t SortedRun::LowerBound(std::string_view key) const {
  size_t lo = 0, hi = index_.size();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (index_[mid].Compare(key, blob_) < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

Status SortedRun::Lookup(std::string_view key, const Schema& sc,
                         Row* row_out) const {
  size_t pos = LowerBound(key);
  if (pos >= index_.size()) return Status::kNotExists;
  if (index_[pos].Compare(key, blob_) != 0) return Status::kNotExists;
  if (index_[pos].is_deleted_) return Status::kDeleted;
  // BuildRow throws std::runtime_error on a corrupt blob, but this is a
  // Status API: translate so corruption never escapes as an exception.
  try {
    *row_out = index_[pos].BuildRow(blob_, sc);
  } catch (const std::exception&) {
    return Status::kCorruption;
  }
  return Status::kSuccess;
}

StatusOr<Row> SortedRun::Find(std::string_view key, const Schema& sc) const {
  // User-facing lookup: a tombstone in this run hides the row entirely.
  Row row;
  Status st = Lookup(key, sc, &row);
  if (st == Status::kSuccess) return Row(std::move(row));
  if (st == Status::kDeleted) return Status::kNotExists;
  return st;
}

std::string SortedRun::Iterator::Key() const {
  assert(IsValid());
  return GetEntry().BuildKey(*blob_);
}

Row SortedRun::Iterator::Value() const {
  assert(IsValid());
  return GetEntry().BuildRow(*blob_, *schema_);
}

int SortedRun::Iterator::Compare(const Iterator& rhs) const {
  // The assert guarded cross-blob misuse in debug only; under NDEBUG the
  // comparison silently interpreted one run's offsets against the other
  // run's blob (release UB). Iterator::Compare has no Status channel, so
  // refuse loudly in release too.
  if (blob_ != rhs.blob_) {
    throw std::invalid_argument(
        "SortedRun::Iterator::Compare across different runs");
  }
  return GetEntry().Compare(rhs.GetEntry(), *blob_);
}

std::ostream& operator<<(std::ostream& o, const SortedRun& s) {
  o << "SortedRun{ gen=" << s.generation_ << " size=" << s.index_.size()
    << " blob=" << s.blob_.size() << " bytes }";
  return o;
}

}  // namespace tinylamb
