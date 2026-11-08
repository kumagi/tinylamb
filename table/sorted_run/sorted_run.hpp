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

#ifndef TINYLAMB_SORTED_RUN_HPP
#define TINYLAMB_SORTED_RUN_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class BlobFile;

// A serialized buffer of row payloads plus a sorted array of
// (key_offset, row_offset) pointer-entries. Sorting moves only pointers, not
// row data, so write amplification stays at O(N log N) pointer moves plus one
// final sequential write of the row buffer. The final merge step reads this
// pointer array in order to emit a PAX chunk.
class SortedRun final {
 public:
  struct Entry {
    uint32_t key_offset_{0};
    uint32_t row_offset_{0};
    bool is_deleted_{false};
    Entry() = default;
    Entry(uint32_t k, uint32_t r, bool del)
        : key_offset_(k), row_offset_(r), is_deleted_(del) {}
    [[nodiscard]] int Compare(std::string_view rhs,
                              const std::vector<char>& blob) const;
    [[nodiscard]] int Compare(const Entry& rhs,
                              const std::vector<char>& blob) const;
    [[nodiscard]] std::string BuildKey(const std::vector<char>& blob) const;
    [[nodiscard]] Row BuildRow(const std::vector<char>& blob,
                               const Schema& sc) const;
    // Deserialize the payload into *out, reusing its cell buffer (no
    // per-row allocation once *out holds a row of the same shape). Returns
    // false -- leaving *out untouched -- for tombstones and offsets past
    // the blob, mirroring BuildRow's guards.
    bool BuildRowInto(const std::vector<char>& blob, const Schema& sc,
                      Row* out) const;
    [[nodiscard]] bool IsDeleted() const { return is_deleted_; }
    friend std::ostream& operator<<(std::ostream& o, const Entry& e);
  };

  class Iterator {
   public:
    Iterator(const SortedRun* parent, const std::vector<char>& /*blob*/,
             const Schema& sc, size_t offset = 0)
        : parent_(parent), schema_(&sc), offset_(offset) {}
    ~Iterator() = default;
    Iterator(Iterator&&) = default;
    Iterator(const Iterator&) = default;
    Iterator& operator=(Iterator&&) = default;
    Iterator& operator=(const Iterator&) = default;
    [[nodiscard]] std::string Key() const;
    [[nodiscard]] Row Value() const;
    [[nodiscard]] int Compare(const Iterator& rhs) const;
    [[nodiscard]] Entry GetEntry() const { return parent_->GetEntry(offset_); }
    [[nodiscard]] bool IsValid() const { return offset_ < parent_->Size(); }
    Iterator& operator++() {
      ++offset_;
      return *this;
    }
    bool operator==(const Iterator& rhs) const {
      return offset_ == rhs.offset_ && parent_ == rhs.parent_;
    }
    bool operator!=(const Iterator& rhs) const { return !operator==(rhs); }
    [[nodiscard]] size_t Generation() const { return parent_->generation_; }
    [[nodiscard]] bool IsDeleted() const {
      return parent_->GetEntry(offset_).IsDeleted();
    }

   private:
    const SortedRun* parent_;
    const Schema* schema_;
    size_t offset_;
  };

  SortedRun() = default;
  SortedRun(std::vector<char> blob, std::vector<Entry> index, size_t gen)
      : blob_(std::move(blob)), index_(std::move(index)), generation_(gen) {}
  SortedRun(const SortedRun&) = default;
  SortedRun(SortedRun&&) = default;
  SortedRun& operator=(const SortedRun&) = default;
  SortedRun& operator=(SortedRun&&) = default;
  ~SortedRun() = default;

  [[nodiscard]] StatusOr<Row> Find(std::string_view key,
                                   const Schema& sc) const;
  // Three-state point lookup for LSM traversal: kSuccess (row_out filled),
  // kDeleted (tombstone present here; stop searching older layers),
  // kNotExists (absent from this run; keep searching).
  [[nodiscard]] Status Lookup(std::string_view key, const Schema& sc,
                              Row* row_out) const;
  // Index of the first entry whose key is >= probe (Size() if none).
  // Binary search over the pointer array comparing memcomparable bytes.
  [[nodiscard]] size_t LowerBound(std::string_view key) const;
  [[nodiscard]] size_t Size() const { return index_.size(); }
  [[nodiscard]] size_t Generation() const { return generation_; }
  [[nodiscard]] Iterator Begin(const Schema& sc) const {
    return {this, blob_, sc, 0};
  }
  [[nodiscard]] Entry GetEntry(size_t offset) const {
    if (offset >= index_.size()) {
      throw std::out_of_range("SortedRun::GetEntry offset out of range");
    }
    return index_[offset];
  }
  [[nodiscard]] const std::vector<char>& Blob() const { return blob_; }
  [[nodiscard]] const std::vector<Entry>& Index() const { return index_; }
  friend std::ostream& operator<<(std::ostream& o, const SortedRun& s);

 private:
  std::vector<char> blob_;
  std::vector<Entry> index_;
  size_t generation_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_SORTED_RUN_HPP
