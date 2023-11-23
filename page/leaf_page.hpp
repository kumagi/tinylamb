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


#ifndef TINYLAMB_LEAF_PAGE_HPP
#define TINYLAMB_LEAF_PAGE_HPP

#include <cassert>
#include <cstddef>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "foster_pair.hpp"
#include "page/index_key.hpp"
#include "page/row_pointer.hpp"

namespace tinylamb {

class Transaction;
class Page;
class BranchPage;
class IndexKey;

class LeafPage final {
  char* Payload() { return reinterpret_cast<char*>(rows_); }
  [[nodiscard]] const char* Payload() const {
    return reinterpret_cast<const char*>(rows_);
  }

 public:
  void Initialize() {
    row_count_ = 0;
    free_ptr_ = kPageBodySize - offsetof(LeafPage, rows_);
    free_size_ = kPageBodySize - offsetof(LeafPage, rows_);
    low_fence_ = kMinusInfinity;
    high_fence_ = kPlusInfinity;
  }

  Status Insert(page_id_t page_id, Transaction& txn, std::string_view key,
                std::string_view value);
  Status Update(page_id_t page_id, Transaction& txn, std::string_view key,
                std::string_view value);
  Status Delete(page_id_t page_id, Transaction& txn, std::string_view key);
  StatusOr<std::string_view> Read(page_id_t pid, Transaction& txn,
                                  slot_t slot) const;
  StatusOr<std::string_view> ReadKey(page_id_t pid, Transaction& txn,
                                     slot_t slot) const;
  StatusOr<std::string_view> Read(page_id_t pid, Transaction& txn,
                                  std::string_view key) const;

  [[nodiscard]] std::string_view GetKey(size_t idx) const;
  [[nodiscard]] std::string_view GetValue(size_t idx) const;
  StatusOr<std::string_view> LowestKey(Transaction& txn) const;
  StatusOr<std::string_view> HighestKey(Transaction& txn) const;
  [[nodiscard]] slot_t RowCount() const;
  [[nodiscard]] StatusOr<FosterPair> GetFoster() const;

  // Split utils.
  void Split(page_id_t pid, Transaction& txn, std::string_view key,
             std::string_view value, Page* right);

  void InsertImpl(std::string_view key, std::string_view value);
  void UpdateImpl(std::string_view key, std::string_view value);
  void DeleteImpl(std::string_view key);
  void SetFence(RowPointer& fence_pos, const IndexKey& new_fence);
  Status SetLowFence(page_id_t pid, Transaction& txn, const IndexKey& lf);
  Status SetHighFence(page_id_t pid, Transaction& txn, const IndexKey& hf);
  void SetLowFenceImpl(const IndexKey& lf);
  void SetHighFenceImpl(const IndexKey& hf);
  [[nodiscard]] IndexKey GetLowFence() const;
  [[nodiscard]] IndexKey GetHighFence() const;

  Status SetFoster(page_id_t pid, Transaction& txn,
                   const FosterPair& new_foster);
  void SetFosterImpl(const FosterPair& foster);

  Status MoveRightToFoster(Transaction& txn, Page& right);
  Status MoveLeftFromFoster(Transaction& txn, Page& right);

  [[nodiscard]] bool SanityCheckForTest() const;

 private:
  void UpdateSlotImpl(RowPointer& pos, std::string_view payload);
  void DeFragment();
  void Dump(std::ostream& o, int indent) const;
  [[nodiscard]] size_t Find(std::string_view key) const;

  friend class BPlusTree;
  friend class BPlusTreeIterator;
  friend class Page;
  friend class BranchPage;
  friend class std::hash<LeafPage>;

  slot_t row_count_ = 0;
  bin_size_t free_ptr_ = kPageBodySize - offsetof(LeafPage, rows_);
  bin_size_t free_size_ = kPageBodySize - offsetof(LeafPage, rows_);
  RowPointer low_fence_;
  RowPointer high_fence_;
  RowPointer foster_;
  RowPointer rows_[];
};

static_assert(std::is_trivially_destructible<LeafPage>::value,
              "RowPage must be trivially destructible");

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::LeafPage> {
 public:
  uint64_t operator()(const tinylamb::LeafPage& r) const;
};

}  // namespace std

#endif  // TINYLAMB_LEAF_PAGE_HPP
