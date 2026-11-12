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

#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class Transaction;

// Unbounded in-memory/LSM row block.
// Each block holds up to kMaxSlotsPerBlock logical row slots before rolling
// over to a new block (for parallel morsel scan partitioning), while each slot
// stores a dynamically sized std::string with no page-size cap.
class RowPage {
 public:
  static constexpr slot_t kMaxSlotsPerBlock = 4096;
  static constexpr uint32_t kMaxColumnarRows = 32768;
  static constexpr size_t kSlotEntryBytes = 4;
  static constexpr size_t kBlockBudgetBytes = kPageBodySize - 24;

  void Initialize() {
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    columnar_row_count_ = 0;
    slots_.clear();
    hole_sizes_.clear();
    committed_allocated_ = false;
    committed_prev_page_id_ = 0;
    committed_next_page_id_ = 0;
    committed_link_lsn_ = 0;
    committed_columnar_row_count_ = 0;
    committed_slots_.clear();
    committed_lsns_.clear();
  }

  void ClearLiveState() {
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    columnar_row_count_ = 0;
    slots_.clear();
    hole_sizes_.clear();
  }

  [[nodiscard]] bool IsColumnarStub() const noexcept {
    return columnar_row_count_ != 0 && slots_.empty();
  }

  [[nodiscard]] uint32_t ColumnarRowCount() const noexcept {
    return columnar_row_count_;
  }

  [[nodiscard]] size_t UsedBytes() const {
    if (IsColumnarStub()) {
      return kBlockBudgetBytes;
    }
    size_t used = slots_.size() * kSlotEntryBytes;
    for (const std::string& s : slots_) {
      used += s.size();
    }
    return used;
  }

  [[nodiscard]] bin_size_t FreeSize() const {
    if (IsColumnarStub()) {
      return 0;
    }
    const size_t used = UsedBytes();
    return used < kBlockBudgetBytes
               ? static_cast<bin_size_t>(kBlockBudgetBytes - used)
               : 0;
  }

  [[nodiscard]] bin_size_t FreeSizeForTest() const { return FreeSize(); }

  StatusOr<std::string_view> Read(page_id_t page_id, Transaction& txn,
                                  slot_t slot, bool resolve_head = true) const;

  StatusOr<slot_t> Insert(page_id_t page_id, Transaction& txn,
                          std::string_view record);

  Status Update(page_id_t page_id, Transaction& txn, slot_t slot,
                std::string_view record);

  StatusOr<lsn_t> RestorePhysicalUpdate(page_id_t page_id, Transaction& txn,
                                        slot_t slot, std::string_view record);

  Status Delete(page_id_t page_id, Transaction& txn, slot_t slot);

  Status SetNextPage(page_id_t page_id, Transaction& txn, page_id_t next);
  Status SetPrevPage(page_id_t page_id, Transaction& txn, page_id_t prev);
  void SetNextPageImpl(page_id_t next) { next_page_id_ = next; }
  void SetPrevPageImpl(page_id_t prev) { prev_page_id_ = prev; }

  [[nodiscard]] slot_t RowCount() const {
    return IsColumnarStub() ? static_cast<slot_t>(columnar_row_count_)
                            : row_count_;
  }
  [[nodiscard]] slot_t RowMax() const {
    return IsColumnarStub() ? static_cast<slot_t>(columnar_row_count_)
                            : static_cast<slot_t>(slots_.size());
  }
  [[nodiscard]] bool IsSlotOccupied(slot_t slot) const {
    if (IsColumnarStub()) {
      return slot < columnar_row_count_;
    }
    return slot < slots_.size() && !slots_[slot].empty();
  }
  [[nodiscard]] bool ShouldPreallocateNextBlock() const {
    return (IsColumnarStub() || FreeSize() < 1024 ||
            slots_.size() + 8 >= kMaxSlotsPerBlock) &&
           next_page_id_ == 0;
  }

  [[nodiscard]] std::string_view GetRow(slot_t slot) const {
    if (slot >= slots_.size() || slots_[slot].empty()) {
      return {};
    }
    return slots_[slot];
  }

  StatusOr<slot_t> InsertRow(std::string_view new_row);
  StatusOr<slot_t> InsertRowAt(slot_t slot, std::string_view new_row);
  Status UpdateRow(slot_t slot, std::string_view record);
  void DeleteRow(slot_t slot);

  void Dump(std::ostream& o, int indent) const;
  friend std::ostream& operator<<(std::ostream& o, const RowPage& r) {
    r.Dump(o, 0);
    return o;
  }

  page_id_t prev_page_id_ = 0;
  page_id_t next_page_id_ = 0;
  slot_t row_count_ = 0;
  uint32_t columnar_row_count_ = 0;
  std::vector<std::string> slots_;
  std::vector<size_t> hole_sizes_;

  // Committed baseline state used by fuzzy checkpoint and fuzzy recovery.
  bool committed_allocated_ = false;
  page_id_t committed_prev_page_id_ = 0;
  page_id_t committed_next_page_id_ = 0;
  lsn_t committed_link_lsn_ = 0;
  uint32_t committed_columnar_row_count_ = 0;
  std::vector<std::string> committed_slots_;
  std::vector<lsn_t> committed_lsns_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_PAGE_HPP
