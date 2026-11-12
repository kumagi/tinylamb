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

#include "page/row_page.hpp"

#include <optional>
#include <ostream>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

StatusOr<std::string_view> RowPage::Read(page_id_t page_id, Transaction& txn,
                                         slot_t slot, bool resolve_head) const {
  const RowPosition position(page_id, slot);
  if (slot >= slots_.size() || slots_[slot].empty()) {
    return txn.ReadVersion(position, std::nullopt, resolve_head);
  }
  return txn.ReadVersion(position, GetRow(slot), resolve_head);
}

StatusOr<slot_t> RowPage::Insert(page_id_t page_id, Transaction& txn,
                                 std::string_view record) {
  if (IsColumnarStub()) {
    return Status::kNoSpace;
  }
  size_t locked_hole_bytes = 0;
  const auto row_max = static_cast<slot_t>(slots_.size());
  slot_t hole_slot = row_max;
  for (slot_t i = 0; i < row_max; ++i) {
    if (!slots_[i].empty()) {
      continue;
    }
    const RowPosition hole(page_id, i);
    if (txn.TryAddWriteSet(hole)) {
      if (hole_slot == row_max && !txn.SnapshotSeesRow(hole)) {
        hole_slot = i;
        continue;
      }
      txn.ReleaseWriteIntent(hole);
      continue;
    }
    if (i < hole_sizes_.size()) {
      locked_hole_bytes += hole_sizes_[i];
    }
  }
  const size_t free_bytes = FreeSize();
  const size_t available =
      free_bytes >= locked_hole_bytes ? free_bytes - locked_hole_bytes : 0;
  auto release_acquired_hole = [&] {
    if (hole_slot != row_max) {
      txn.ReleaseWriteIntent(RowPosition(page_id, hole_slot));
      hole_slot = row_max;
    }
  };
  if (hole_slot != row_max) {
    if (available < record.size()) {
      release_acquired_hole();
      return Status::kNoSpace;
    }
    const StatusOr<slot_t> inserted = InsertRowAt(hole_slot, record);
    if (!inserted.HasValue()) {
      release_acquired_hole();
      return inserted.GetStatus();
    }
    txn.RegisterVersionWrite(RowPosition(page_id, inserted.Value()),
                             std::nullopt, record);
    const Status append =
        txn.InsertLog(page_id, inserted.Value(), record).GetStatus();
    if (append != Status::kSuccess) {
      DeleteRow(inserted.Value());
      (void)txn.CancelStagedVersion(RowPosition(page_id, inserted.Value()));
      txn.MarkFailed();
      return append;
    }
    return inserted.Value();
  }
  if (slots_.size() >= kMaxSlotsPerBlock ||
      available < record.size() + kSlotEntryBytes) {
    return Status::kNoSpace;
  }
  const RowPosition pos(page_id, row_max);
  if (!txn.TryAddWriteSet(pos)) {
    return Status::kConflicts;
  }
  const StatusOr<slot_t> inserted = InsertRowAt(row_max, record);
  if (!inserted.HasValue()) {
    txn.ReleaseWriteIntent(pos);
    return inserted.GetStatus();
  }
  txn.RegisterVersionWrite(pos, std::nullopt, record);
  const Status append =
      txn.InsertLog(page_id, inserted.Value(), record).GetStatus();
  if (append != Status::kSuccess) {
    DeleteRow(inserted.Value());
    (void)txn.CancelStagedVersion(pos);
    txn.MarkFailed();
    return append;
  }
  return inserted.Value();
}

StatusOr<slot_t> RowPage::InsertRow(std::string_view new_row) {
  slot_t slot = 0;
  while (slot < slots_.size() && !slots_[slot].empty()) {
    ++slot;
  }
  return InsertRowAt(slot, new_row);
}

StatusOr<slot_t> RowPage::InsertRowAt(slot_t slot, std::string_view new_row) {
  if (new_row.size() > kBlockBudgetBytes) {
    return Status::kNoSpace;
  }
  if (slot < slots_.size() && !slots_[slot].empty()) {
    return Status::kConflicts;
  }
  const size_t new_slots = slot >= slots_.size()
                               ? (static_cast<size_t>(slot) + 1 - slots_.size())
                               : 0;
  if (UsedBytes() + (new_slots * kSlotEntryBytes) + new_row.size() >
      kBlockBudgetBytes) {
    return Status::kNoSpace;
  }
  if (slot >= slots_.size()) {
    slots_.resize(static_cast<size_t>(slot) + 1);
    hole_sizes_.resize(static_cast<size_t>(slot) + 1, 0);
  }
  slots_[slot].assign(new_row);
  if (slot < hole_sizes_.size()) {
    hole_sizes_[slot] = 0;
  }
  ++row_count_;
  return slot;
}

Status RowPage::Update(page_id_t page_id, Transaction& txn, slot_t slot,
                       std::string_view record) {
  if (slot >= slots_.size() || slots_[slot].empty()) {
    return Status::kNotExists;
  }
  const std::string prev_row(GetRow(slot));
  if (prev_row.size() < record.size() &&
      FreeSize() < record.size() - prev_row.size()) {
    return Status::kNoSpace;
  }
  const RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos, prev_row)) {
    return Status::kConflicts;
  }
  StatusOr<std::string_view> head = txn.ReadVersion(
      pos, std::string_view(GetRow(slot)), /*resolve_head=*/true);
  if (!head.HasValue() || head.Value() != prev_row) {
    return Status::kConflicts;
  }
  RETURN_IF_FAIL(UpdateRow(pos.slot, record));
  txn.RegisterVersionWrite(pos, prev_row, record);
  Status append = txn.UpdateLog(page_id, slot, record, prev_row).GetStatus();
  if (append != Status::kSuccess) {
    std::ignore = UpdateRow(pos.slot, prev_row);
    (void)txn.CancelStagedVersion(pos);
    txn.MarkFailed();
    return append;
  }
  return Status::kSuccess;
}

StatusOr<lsn_t> RowPage::RestorePhysicalUpdate(page_id_t page_id,
                                               Transaction& txn, slot_t slot,
                                               std::string_view record) {
  if (slot >= slots_.size() || slots_[slot].empty()) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(lsn_t, lsn,
                   txn.AppendPhysicalUpdateCompensation(page_id, slot, record));
  RETURN_IF_FAIL(UpdateRow(slot, record));
  return lsn;
}

Status RowPage::UpdateRow(slot_t slot, std::string_view record) {
  if (slot >= slots_.size()) {
    slots_.resize(static_cast<size_t>(slot) + 1);
    hole_sizes_.resize(static_cast<size_t>(slot) + 1, 0);
  }
  const size_t prev_size = slots_[slot].size();
  if (record.size() > prev_size && FreeSize() < record.size() - prev_size) {
    return Status::kNoSpace;
  }
  if (slots_[slot].empty() && !record.empty()) {
    ++row_count_;
  }
  slots_[slot].assign(record);
  if (slot < hole_sizes_.size()) {
    hole_sizes_[slot] = 0;
  }
  return Status::kSuccess;
}

Status RowPage::Delete(page_id_t page_id, Transaction& txn, slot_t slot) {
  if (slot >= slots_.size() || slots_[slot].empty()) {
    return Status::kNotExists;
  }
  const std::string previous(GetRow(slot));
  const RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos, previous)) {
    return Status::kConflicts;
  }
  StatusOr<std::string_view> head = txn.ReadVersion(
      pos, std::string_view(GetRow(slot)), /*resolve_head=*/true);
  if (!head.HasValue() || head.Value() != previous) {
    return Status::kConflicts;
  }
  txn.RegisterVersionWrite(pos, previous, std::nullopt);
  Status append = txn.DeleteLog(page_id, slot, previous).GetStatus();
  if (append != Status::kSuccess) {
    (void)txn.CancelStagedVersion(pos);
    txn.MarkFailed();
    return append;
  }
  DeleteRow(slot);
  return Status::kSuccess;
}

void RowPage::DeleteRow(slot_t slot) {
  if (slot < slots_.size() && !slots_[slot].empty()) {
    if (hole_sizes_.size() < slots_.size()) {
      hole_sizes_.resize(slots_.size(), 0);
    }
    hole_sizes_[slot] = slots_[slot].size();
    slots_[slot].clear();
    --row_count_;
  }
}

Status RowPage::SetNextPage(page_id_t page_id, Transaction& txn,
                            page_id_t next) {
  if (next_page_id_ == next) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(
      txn.SetRowPageNextLog(page_id, next, next_page_id_).GetStatus());
  next_page_id_ = next;
  return Status::kSuccess;
}

Status RowPage::SetPrevPage(page_id_t page_id, Transaction& txn,
                            page_id_t prev) {
  if (prev_page_id_ == prev) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(
      txn.SetRowPagePrevLog(page_id, prev, prev_page_id_).GetStatus());
  prev_page_id_ = prev;
  return Status::kSuccess;
}

void RowPage::Dump(std::ostream& o, int /*indent*/) const {
  o << "[RowCount: " << row_count_ << " PrevPage: " << prev_page_id_
    << " NextPage: " << next_page_id_ << "]";
}

}  // namespace tinylamb
