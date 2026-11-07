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
#include <iosfwd>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class Transaction;
class LsmIndex;

class LsmIndexIterator {
 public:
  LsmIndexIterator() = default;
  LsmIndexIterator(LsmIndex* tree, Transaction* txn,
                   std::string_view begin = "", std::string_view end = "",
                   bool ascending = true);

  [[nodiscard]] bool IsValid() const { return valid_; }
  [[nodiscard]] Status GetStatus() const { return status_; }
  [[nodiscard]] std::string Key() const;
  [[nodiscard]] std::string Value() const;

  void Clear() {
    valid_ = false;
    entries_.clear();
    idx_ = 0;
  }

  void EnableStableLeafCache() { stable_leaf_cache_ = true; }
  [[nodiscard]] bool EnsureLeafCache() const {
    return valid_ && idx_ < entries_.size();
  }
  [[nodiscard]] std::string_view CachedKey(size_t i) const {
    return i < entries_.size() ? std::string_view(entries_[i].first)
                               : std::string_view{};
  }
  [[nodiscard]] std::string_view CachedValue(size_t i) const {
    return i < entries_.size() ? std::string_view(entries_[i].second)
                               : std::string_view{};
  }

  LsmIndexIterator& operator++();
  LsmIndexIterator& operator--();

  friend std::ostream& operator<<(std::ostream& o, const LsmIndexIterator& it);

  bool stable_leaf_cache_{false};
  size_t idx_{0};

 private:
  LsmIndex* tree_{nullptr};
  Transaction* txn_{nullptr};
  std::vector<std::pair<std::string, std::string>> entries_;
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

  Status Insert(Transaction& txn, std::string_view key, std::string_view value,
                page_id_t* hint_leaf = nullptr);
  Status Update(Transaction& txn, std::string_view key, std::string_view value,
                page_id_t* hint_leaf = nullptr);
  Status Delete(Transaction& txn, std::string_view key);
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

  Status Destroy(Transaction& txn);

  void Dump(std::ostream& o, int indent = 0) const;
  [[nodiscard]] bool CheckSanity(Transaction& /*txn*/) const { return true; }

 private:
  page_id_t root_{0};
};

using BPlusTree = LsmIndex;
using BPlusTreeIterator = LsmIndexIterator;

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_INDEX_HPP
