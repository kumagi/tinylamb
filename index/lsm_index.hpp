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

#ifndef TINYLAMB_LSM_INDEX_HPP
#define TINYLAMB_LSM_INDEX_HPP

#include <cstddef>
#include <deque>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class Transaction;
class LsmIndex;
class LsmScanCursor;

class LsmIndexIterator {
 public:
  LsmIndexIterator() = default;
  LsmIndexIterator(LsmIndex* tree, Transaction* txn,
                   std::string_view begin = "", std::string_view end = "",
                   bool ascending = true);
  ~LsmIndexIterator();
  LsmIndexIterator(LsmIndexIterator&&) noexcept;
  LsmIndexIterator& operator=(LsmIndexIterator&&) noexcept;

  [[nodiscard]] bool IsValid() const { return valid_; }
  [[nodiscard]] Status GetStatus() const { return status_; }
  [[nodiscard]] std::string Key() const;
  [[nodiscard]] std::string Value() const;

  // Defined out-of-line: resetting scan_cursor_ needs the complete
  // LsmScanCursor type, which lives in page/page_lsm_store.hpp.
  void Clear();

  void EnableStableLeafCache() { stable_leaf_cache_ = true; }
  void DisableStableLeafCache() { stable_leaf_cache_ = false; }
  [[nodiscard]] bool StableLeafCacheEnabled() const { return stable_leaf_cache_; }
  [[nodiscard]] bool EnsureLeafCache() const {
    return valid_ && EntryAt(idx_) != nullptr;
  }
  [[nodiscard]] std::string_view CachedKey(size_t i) const {
    const auto* e = EntryAt(i);
    return e != nullptr ? std::string_view(e->first) : std::string_view{};
  }
  [[nodiscard]] std::string_view CachedValue(size_t i) const {
    const auto* e = EntryAt(i);
    return e != nullptr ? std::string_view(e->second) : std::string_view{};
  }

  LsmIndexIterator& operator++();
  LsmIndexIterator& operator--();

  friend std::ostream& operator<<(std::ostream& o, const LsmIndexIterator& it);

  bool stable_leaf_cache_{false};
  size_t idx_{0};

 private:
  // Absolute-position accessor shared by the eager (entries_) and lazy
  // (emitted_) representations; nullptr when `i` is not materialized.
  [[nodiscard]] const std::pair<std::string, std::string>* EntryAt(
      size_t i) const;
  [[nodiscard]] size_t EntryCount() const;
  // Lazy mode only: pulls the next resolved entry from scan_cursor_ into
  // emitted_. Returns false at end-of-range or on cursor error.
  bool PullNext();

  LsmIndex* tree_{nullptr};
  Transaction* txn_{nullptr};
  std::vector<std::pair<std::string, std::string>> entries_;
  // Ascending lazy scans only: entries pulled so far from scan_cursor_.
  // `idx_` remains the absolute position, so operator-- revisits earlier
  // entries without rewinding the forward-only cursor.
  std::deque<std::pair<std::string, std::string>> emitted_;
  std::unique_ptr<LsmScanCursor> scan_cursor_;
  bool valid_{false};
  Status status_{Status::kSuccess};
};

// Unbounded LSM-Tree index over PageManager's MemTable + immutable sorted runs.
// No key-size or value-size limits exist.
class LsmIndex {
 public:
  static StatusOr<LsmIndex> Open(Transaction& txn, page_id_t root = 0);

  LsmIndex() = default;
  explicit LsmIndex(page_id_t root) : root_(root) {}

  bool operator==(const LsmIndex& rhs) const = default;
  [[nodiscard]] page_id_t Root() const { return root_; }

  // Read the committed baseline plus this transaction's own staged writes.
  [[nodiscard]] StatusOr<std::string> ReadVisible(Transaction& txn,
                                                  std::string_view key) const;

  Status Insert(Transaction& txn, std::string_view key, std::string_view value,
                page_id_t* hint_leaf = nullptr) const;
  Status Update(Transaction& txn, std::string_view key, std::string_view value,
                page_id_t* hint_leaf = nullptr) const;
  Status Delete(Transaction& txn, std::string_view key) const;
  [[nodiscard]] StatusOr<std::string> Read(
      Transaction& txn, std::string_view key,
      page_id_t* hint_leaf = nullptr) const;

  [[nodiscard]] LsmIndexIterator Begin(Transaction& txn,
                                       std::string_view begin = "",
                                       std::string_view end = "",
                                       bool ascending = true);

  [[nodiscard]] StatusOr<std::vector<std::string>> ScanPartitionBoundaries(
      Transaction& txn, std::string_view begin = "", std::string_view end = "",
      size_t leaves_per_partition = 4) const;

  Status Destroy(Transaction& txn) const;

  void Dump(std::ostream& o, int indent = 0) const;
  [[nodiscard]] static bool CheckSanity(Transaction& /*txn*/) { return true; }

 private:
  page_id_t root_{0};
};

using BPlusTree = LsmIndex;
using BPlusTreeIterator = LsmIndexIterator;

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_INDEX_HPP
