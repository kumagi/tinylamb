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

#ifndef TINYLAMB_ROW_ENTRY_HPP
#define TINYLAMB_ROW_ENTRY_HPP

#include "type/row.hpp"

namespace tinylamb {

// One memtable record: a live row or a tombstone marker. `version` is a
// global MVCC version (see common/version.hpp) stamped at write time when the
// owning MemTable runs in global-version mode; reads pinned at read_version V
// treat entries with version > V as invisible. 0 means "unversioned" (always
// visible), which is the default for the non-MVCC path.
struct RowEntry {
  Row row;
  bool is_deleted{false};
  uint64_t version{0};
  RowEntry() = default;
  explicit RowEntry(Row r) : row(std::move(r)), is_deleted(false) {}
  RowEntry(Row r, bool del) : row(std::move(r)), is_deleted(del) {}
  RowEntry(Row r, bool del, uint64_t v)
      : row(std::move(r)), is_deleted(del), version(v) {}
  RowEntry(const RowEntry&) = default;
  RowEntry(RowEntry&&) = default;
  RowEntry& operator=(const RowEntry&) = default;
  RowEntry& operator=(RowEntry&&) = default;
  ~RowEntry() = default;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_ENTRY_HPP
