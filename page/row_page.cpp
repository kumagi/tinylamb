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

#include "row_page.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

StatusOr<std::string_view> RowPage::Read(page_id_t page_id, Transaction& txn,
                                         slot_t slot) const {
  const RowPosition position(page_id, slot);
  if (row_max_ <= slot || rows_[slot].offset == 0) {
    return txn.ReadVersion(position, std::nullopt);
  }
  return txn.ReadVersion(position, GetRow(slot));
}

/*
 *           = before =
 * +-------------------------------+
 * | RowPointer(0, 0) |            |
 * +-------------------------------+
 * |                               |
 * +-------------------------------+
 *                                 ^ free_ptr_
 *           = after =
 * *-------------------------------+
 * | RowPointer(PosX, Size) |      |
 * +-------------------------------+
 * |                      | Record |
 * +-------------------------------+
 *                        ^ PosX == free_ptr_
 */
StatusOr<slot_t> RowPage::Insert(page_id_t page_id, Transaction& txn,
                                 std::string_view record) {
  if (free_size_ <= record.size() + sizeof(RowPointer)) {
    return Status::kNoSpace;
  }
  // Scan the first vacant slot.
  slot_t result = InsertRow(record);
  if (!txn.AddWriteSet(RowPosition(page_id, result))) {
    return Status::kConflicts;
  }
  txn.RegisterVersionWrite(RowPosition(page_id, result), std::nullopt, record);
  txn.InsertLog(page_id, result, record);
  return result;
}

namespace {

size_t SlotArrayBytes(slot_t row_max) {
  return sizeof(RowPage) + static_cast<size_t>(row_max) * sizeof(RowPointer);
}

}  // namespace

bool RowPage::ReclaimUntilContiguous(size_t bytes, slot_t protected_slot,
                                     slot_t prospective_row_max) {
  auto fits = [&] {
    return SlotArrayBytes(prospective_row_max) + bytes <= free_ptr_;
  };
  if (fits()) {
    return true;
  }
  DeFragmentExcept(protected_slot);
  if (fits()) {
    return true;
  }
  while (!fits()) {
    slot_t victim = row_max_;
    bool found = false;
    while (victim > 0) {
      --victim;
      if (victim == protected_slot || rows_[victim].offset == 0) {
        continue;
      }
      found = true;
      break;
    }
    if (!found) {
      return false;
    }
    DeleteRow(victim);
    DeFragmentExcept(protected_slot);
  }
  return true;
}

slot_t RowPage::InsertRow(std::string_view new_row) {
  assert(new_row.size() <= std::numeric_limits<slot_t>::max());
  slot_t slot = 0;
  for (; slot < row_max_; ++slot) {
    if (rows_[slot].offset == 0) {
      break;
    }
  }

  // row_count_ cannot describe the end of the slot array when deletions have
  // left holes.  Reserve metadata through the highest live/prospective slot;
  // otherwise tuple bytes can overwrite RowPointers that are still in use.
  const slot_t prospective_row_max = std::max<slot_t>(row_max_, slot + 1);
  if (!ReclaimUntilContiguous(new_row.size(), slot, prospective_row_max)) {
    return slot;
  }

  free_size_ -= new_row.size() + sizeof(RowPointer);
  free_ptr_ -= new_row.size();
  rows_[slot].offset = free_ptr_;
  rows_[slot].size = new_row.size();
  memcpy(Payload() + free_ptr_, new_row.data(), new_row.size());
  row_count_++;
  row_max_ = prospective_row_max;
  return slot;
}

Status RowPage::Update(page_id_t page_id, Transaction& txn, slot_t slot,
                       std::string_view record) {
  if (row_max_ <= slot || rows_[slot].offset == 0) {
    return Status::kNotExists;
  }
  std::string_view prev_row = GetRow(slot);
  if (prev_row.size() < record.size() &&
      free_size_ < record.size() - prev_row.size()) {
    return Status::kNoSpace;
  }
  RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos)) {
    LOG(ERROR) << "cannot add write-set";
    return Status::kConflicts;
  }
  txn.RegisterVersionWrite(pos, prev_row, record);
  txn.UpdateLog(page_id, slot, record, prev_row);
  UpdateRow(pos.slot, record);
  return Status::kSuccess;
}

void RowPage::UpdateRow(slot_t slot, std::string_view record) {
  const size_t previous_size = rows_[slot].size;
  if (record.size() <= previous_size) {
    // There is already enough space, just overwrite.
    free_size_ += previous_size - record.size();
    rows_[slot].size = record.size();
    memcpy(Payload() + rows_[slot].offset, record.data(), record.size());
    return;
  }
  // Allocate a new field and leave the old field as fragmented free space.
  // Recovery undo may need to restore a larger image after another
  // transaction reused the bytes an uncommitted shrink released. Compact,
  // and drop later rows if needed, instead of writing below the slot array.
  const bool already_contiguous =
      SlotArrayBytes(row_max_) + record.size() <= free_ptr_;
  if (!ReclaimUntilContiguous(record.size(), slot, row_max_)) {
    return;
  }
  free_ptr_ -= record.size();
  rows_[slot].offset = free_ptr_;
  if (already_contiguous) {
    free_size_ += previous_size;
  }
  free_size_ -= record.size();
  rows_[slot].size = record.size();
  memcpy(Payload() + rows_[slot].offset, record.data(), record.size());
}

Status RowPage::Delete(page_id_t page_id, Transaction& txn, slot_t slot) {
  if (row_max_ <= slot || rows_[slot].offset == 0) {
    return Status::kNotExists;
  }
  RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  const std::string_view previous = GetRow(slot);
  txn.RegisterVersionWrite(pos, previous, std::nullopt);
  txn.DeleteLog(page_id, slot, previous);
  DeleteRow(slot);
  return Status::kSuccess;
}

void RowPage::DeleteRow(slot_t slot) {
  row_count_--;
  free_size_ += rows_[slot].size;
  rows_[slot].size = rows_[slot].offset = 0;
  // Keep the high-water mark: an older MV2PL snapshot may still need to ask
  // the version store for a physically deleted tail slot. Vacant slots remain
  // reusable by InsertRow.
}

slot_t RowPage::RowCount() const { return row_count_; }

void RowPage::DeFragmentExcept(slot_t excluded_slot) {
  // FIXME: replace it with inplace one?
  std::vector<std::string> tmp_buffer;
  tmp_buffer.reserve(row_max_);
  for (size_t i = 0; i < row_max_; ++i) {
    if (rows_[i].offset == 0 || i == excluded_slot) {
      tmp_buffer.emplace_back("");
    } else {
      tmp_buffer.emplace_back(GetRow(i));
    }
  }
  free_ptr_ = kPageBodySize;
  for (size_t i = 0; i < row_max_; ++i) {
    if (rows_[i].offset == 0 || i == excluded_slot) {
      if (i == excluded_slot) rows_[i].size = rows_[i].offset = 0;
      continue;
    }

    free_ptr_ -= tmp_buffer[i].size();
    rows_[i].offset = free_ptr_;
    memcpy(Payload() + free_ptr_, tmp_buffer[i].data(), tmp_buffer[i].size());
  }
  const size_t slot_bytes = static_cast<size_t>(
      reinterpret_cast<char*>(&rows_[row_max_]) - Payload());
  assert(slot_bytes <= free_ptr_);
  free_size_ = free_ptr_ - slot_bytes;
}

void RowPage::DeFragment() {
  DeFragmentExcept(std::numeric_limits<slot_t>::max());
}

void RowPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " Prev: " << prev_page_id_
    << " Next: " << next_page_id_ << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_;
  for (size_t i = 0; i < row_max_; ++i) {
    o << "\n" << Indent(indent) << i << ": " << OmittedString(GetRow(i), 40);
  }
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::RowPage>::operator()(
    const tinylamb::RowPage& r) const {
  uint64_t ret = 0;
  ret += std::hash<tinylamb::page_id_t>()(r.prev_page_id_);
  ret += std::hash<tinylamb::page_id_t>()(r.next_page_id_);
  ret += std::hash<tinylamb::slot_t>()(r.row_max_);
  ret += std::hash<tinylamb::slot_t>()(r.row_count_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_ptr_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_size_);
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetRow(i));
  }
  return ret;
}
