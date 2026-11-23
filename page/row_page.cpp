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

#include <atomic>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {
// Instrumentation for the #1110 first-fit contract (see
// RowPage::HoleAcquireProbesForTest); one relaxed store per acquire attempt.
thread_local uint64_t g_hole_acquire_probes{0};
}  // namespace

void RowPage::ResetHoleAcquireProbesForTest() { g_hole_acquire_probes = 0; }

uint64_t RowPage::HoleAcquireProbesForTest() { return g_hole_acquire_probes; }

StatusOr<std::string_view> RowPage::Read(page_id_t page_id, Transaction& txn,
                                         slot_t slot, bool resolve_head) const {
  const RowPosition position(page_id, slot);
  if (LivePacked().EmptySlot(slot)) {
    return txn.ReadVersion(position, std::nullopt, resolve_head);
  }
  return txn.ReadVersion(position, GetRow(slot), resolve_head);
}

StatusOr<slot_t> RowPage::Insert(page_id_t page_id, Transaction& txn,
                                 std::string_view record) {
  if (IsColumnarStub()) {
    return Status::kNoSpace;
  }
  const size_t free_bytes = FreeSize();
  if (free_bytes < record.size()) {
    // Neither hole reuse (available = free - locked <= free) nor append
    // (+ slot entry bytes) can fit; every hole used to be probed with a
    // write-intent acquire/release pair before this verdict was reached.
    return Status::kNoSpace;
  }
  const size_t fit_margin = free_bytes - record.size();
  size_t locked_hole_bytes = 0;
  const PackedSlots& live = LivePacked();
  if (live.size() > kMaxColumnarRows) {
    return Status::kCorrupt;
  }
  const auto row_max = static_cast<slot_t>(live.size());
  slot_t hole_slot = row_max;
  if (row_count_ < row_max || !hole_sizes_.empty()) {
    for (slot_t i = 0; i < row_max; ++i) {
      const RowPosition hole(page_id, i);
      if (!live.EmptySlot(i)) {
        // P666: UPDATE shrinks leave the slot occupied but carry a restore
        // reservation in hole_sizes_; count it like a foreign-locked hole,
        // but never offer it as an insert candidate.
        if (i < hole_sizes_.size() && hole_sizes_[i] != 0 &&
            txn.WriteIntentUnavailable(hole)) {
          locked_hole_bytes += hole_sizes_[i];
          if (locked_hole_bytes > fit_margin) {
            break;
          }
        }
        continue;
      }
      bool foreign_locked = false;
      if (hole_slot == row_max) {
        // Candidate undecided: keep the original acquire/see-test flow.
        ++g_hole_acquire_probes;
        if (txn.TryAddWriteSet(hole)) {
          if (!txn.SnapshotSeesRow(hole)) {
            hole_slot = i;  // Intent held for the chosen hole.
          } else {
            txn.ReleaseWriteIntent(hole);
          }
          continue;
        }
        foreign_locked = true;
      } else {
        // Candidate secured; the remaining holes only feed the locked-byte
        // accounting, so probe read-only instead of another acquire/release
        // pair (#1110).
        foreign_locked = txn.WriteIntentUnavailable(hole);
      }
      if (foreign_locked && i < hole_sizes_.size()) {
        locked_hole_bytes += hole_sizes_[i];
        if (locked_hole_bytes > fit_margin) {
          // Foreign-locked hole bytes alone already disqualify both the
          // hole candidate (available < record) and the append path
          // (available < record + slot bytes); stop probing.
          break;
        }
      }
    }
  }
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
  if (LivePacked().size() >= kMaxSlotsPerBlock ||
      available < record.size() + kSlotEntryBytes) {
    return Status::kNoSpace;
  }
  const RowPosition pos(page_id, row_max);
  if (!txn.TryAddWriteSet(pos)) {
    return txn.LastWriteSetFailureStatus();
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
  const PackedSlots& live = LivePacked();
  auto slot = static_cast<slot_t>(live.size());
  if (row_count_ < live.size()) {
    slot = 0;
    while (slot < live.size() && !live.EmptySlot(slot)) {
      ++slot;
    }
  }
  return InsertRowAt(slot, new_row);
}

StatusOr<slot_t> RowPage::InsertRowAt(slot_t slot, std::string_view new_row) {
  if (new_row.size() > kBlockBudgetBytes) {
    return Status::kNoSpace;
  }
  // T1936: the public entry point must police the slot-count cap itself.
  // Internal callers gate on capacity first (:134) and the WAL path copied
  // that check ("a corrupt WAL slot must not push the page past its
  // slot-count"), but a direct call with a slot past the cap would grow
  // the packed index toward 65535 entries (~256 KiB of slot table) on a
  // page contractually capped at kMaxSlotsPerBlock.
  if (slot >= kMaxSlotsPerBlock) {
    return Status::kNoSpace;
  }
  EnsureLiveMutable();
  if (!live_packed_.EmptySlot(slot)) {
    return Status::kConflicts;
  }
  const size_t new_slots =
      slot >= live_packed_.size()
          ? (static_cast<size_t>(slot) + 1 - live_packed_.size())
          : 0;
  if (UsedBytes() + (new_slots * kSlotEntryBytes) + new_row.size() >
      kBlockBudgetBytes) {
    return Status::kNoSpace;
  }
  live_packed_.Set(slot, new_row);
  if (slot < hole_sizes_.size()) {
    hole_sizes_[slot] = 0;
  }
  if (!new_row.empty()) {
    ++row_count_;
  }
  return slot;
}

Status RowPage::Update(page_id_t page_id, Transaction& txn, slot_t slot,
                       std::string_view record) {
  if (LivePacked().EmptySlot(slot)) {
    return Status::kNotExists;
  }
  const std::string prev_row(GetRow(slot));
  if (prev_row.size() < record.size() &&
      FreeSize() < record.size() - prev_row.size()) {
    return Status::kNoSpace;
  }
  const RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos, prev_row)) {
    return txn.LastWriteSetFailureStatus();
  }
  StatusOr<std::string_view> head = txn.ReadVersion(
      pos, std::string_view(GetRow(slot)), /*resolve_head=*/true);
  RETURN_IF_FAIL(head.GetStatus());
  if (head.Value() != prev_row) {
    return Status::kConflicts;
  }
  RETURN_IF_FAIL(UpdateRow(pos.slot, record));
  txn.RegisterVersionWrite(pos, prev_row, record);
  Status append = txn.UpdateLog(page_id, slot, record, prev_row).GetStatus();
  if (append != Status::kSuccess) {
    // Report a rollback failure rather than hiding it: otherwise the page
    // keeps the new image while the caller only sees the WAL error.
    const Status rollback = UpdateRow(pos.slot, prev_row);
    const bool cancel_ok = txn.CancelStagedVersion(pos);
    txn.MarkFailed();
    if (rollback != Status::kSuccess) {
      // NOLINTNEXTLINE(performance-no-automatic-move) move not beneficial for this type
      return rollback;
    }
    if (!cancel_ok) {
      return StatusError(StatusCode::kCorrupt,
                         "failed to cancel staged MVCC version on WAL error");
    }
    return append;
  }
  return Status::kSuccess;
}

StatusOr<lsn_t> RowPage::RestorePhysicalUpdate(page_id_t page_id,
                                               Transaction& txn, slot_t slot,
                                               std::string_view record) {
  if (LivePacked().EmptySlot(slot)) {
    return Status::kNotExists;
  }
  // Check capacity before writing the compensation record: otherwise a
  // kNoSpace from UpdateRow leaves WAL claiming an applied LSN that the
  // page never received.
  const size_t old_size = LivePacked().View(slot).size();
  if (record.size() > old_size && FreeSize() < record.size() - old_size) {
    return Status::kNoSpace;
  }
  ASSIGN_OR_RETURN(lsn_t, lsn,
                   txn.AppendPhysicalUpdateCompensation(page_id, slot, record));
  RETURN_IF_FAIL(UpdateRow(slot, record));
  return lsn;
}

Status RowPage::UpdateRow(slot_t slot, std::string_view record) {
  EnsureLiveMutable();
  const size_t new_slots = (slot >= live_packed_.size())
                               ? static_cast<size_t>(slot) + 1 -
                                     live_packed_.size()
                               : 0;
  // Charge the new end-offset entries too (InsertRowAt does); a corrupt WAL
  // slot must not push the page past its slot-count or byte budget.
  if (slot >= kMaxSlotsPerBlock ||
      new_slots * kSlotEntryBytes > FreeSize()) {
    return Status::kNoSpace;
  }
  if (slot >= live_packed_.size()) {
    live_packed_.ResizeSlots(static_cast<size_t>(slot) + 1);
  }
  const size_t prev_size = live_packed_.View(slot).size();
  if (record.size() > prev_size && FreeSize() < record.size() - prev_size) {
    return Status::kNoSpace;
  }
  if (live_packed_.EmptySlot(slot) && !record.empty()) {
    ++row_count_;
  } else if (!live_packed_.EmptySlot(slot) && record.empty()) {
    // Restoring an empty image frees the slot; keep row_count_ in sync so
    // Insert() can reuse the hole instead of treating the page as full.
    --row_count_;
  }
  live_packed_.Set(slot, record);
  // P666: a shrink frees bytes that this txn's abort-restore may reclaim;
  // reserve the delta exactly like DeleteRow reserves a full hole, so
  // foreign Inserts keep the restore head-room and the unchecked
  // RestoreLiveSlot cannot push the page past kBlockBudgetBytes.  A growth
  // consumes any prior reservation, so it resets to 0.
  const size_t shrink_reservation =
      record.size() < prev_size ? prev_size - record.size() : 0;
  if (shrink_reservation != 0 && slot >= hole_sizes_.size()) {
    hole_sizes_.resize(live_packed_.size(), 0);
  }
  if (slot < hole_sizes_.size()) {
    hole_sizes_[slot] = shrink_reservation;
  }
  return Status::kSuccess;
}

Status RowPage::Delete(page_id_t page_id, Transaction& txn, slot_t slot) {
  if (LivePacked().EmptySlot(slot)) {
    return Status::kNotExists;
  }
  const std::string previous(GetRow(slot));
  const RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos, previous)) {
    return txn.LastWriteSetFailureStatus();
  }
  StatusOr<std::string_view> head = txn.ReadVersion(
      pos, std::string_view(GetRow(slot)), /*resolve_head=*/true);
  RETURN_IF_FAIL(head.GetStatus());
  if (head.Value() != previous) {
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
  EnsureLiveMutable();
  const size_t old_len = live_packed_.View(slot).size();
  if (old_len > 0) {
    if (hole_sizes_.size() < live_packed_.size()) {
      hole_sizes_.resize(live_packed_.size(), 0);
    }
    hole_sizes_[slot] = old_len;
    live_packed_.Set(slot, {});
    --row_count_;
  }
}

void RowPage::RestoreLiveSlot(slot_t slot, std::string_view image) {
  EnsureLiveMutable();
  const bool occupied = !live_packed_.EmptySlot(slot);
  live_packed_.Set(slot, image);
  if (!occupied && !image.empty()) {
    ++row_count_;
  } else if (occupied && image.empty()) {
    if (row_count_ > 0) {
      --row_count_;
    }
  }
  if (slot < hole_sizes_.size()) {
    hole_sizes_[slot] = 0;
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
  ++live_seq_;
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
  ++live_seq_;
  return Status::kSuccess;
}

void RowPage::Dump(std::ostream& o, int indent) const {
  o << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
    << "[RowCount: " << row_count_ << " PrevPage: " << prev_page_id_
    << " NextPage: " << next_page_id_ << " ColumnarRows: "
    << columnar_row_count_ << " Evicted: " << evicted_
    << " DiskOffset: " << disk_offset_ << " DiskLength: " << disk_length_
    << "]";
}

}  // namespace tinylamb
