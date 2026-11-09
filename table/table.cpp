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

#include "table/table.hpp"

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "full_scan_iterator.hpp"
#include "index/index_scan_iterator.hpp"
#include "index/index_schema.hpp"
#include "index/lsm_index.hpp"
#include "iterator.hpp"
#include "page/page_manager.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"
#include "table/lstable_bridge.hpp"
#include "table/scan_options.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Encoder& operator<<(Encoder& e, const Table::IndexValueType& v) {
  e << v.pos << v.include;
  return e;
}

Decoder& operator>>(Decoder& d, Table::IndexValueType& t) {
  d >> t.pos >> t.include;
  return d;
}

Status Table::CreateIndex(Transaction& txn, const IndexSchema& idx) {
  ASSIGN_OR_RETURN(TableLease, lease, AcquireLease(txn.ID()));
  page_id_t root_pid = 0;
  {
    ASSIGN_OR_RETURN(
        PageRef, root_page,
        txn.GetPageManager()->AllocateNewPage(txn, PageType::kRowPage));
    root_pid = root_page->PageID();
    indexes_.emplace_back(idx.name_, idx.key_, root_pid, idx.include_,
                          idx.mode_);
  }

  std::map<std::string, std::vector<IndexValueType>> grouped_multi;
  Iterator it = BeginFullScan(txn);
  while (it.IsValid()) {
    const Row row = *it;
    const bool key_has_null = std::ranges::any_of(
        indexes_.back().sc_.key_,
        [&row](const slot_t key_column) { return row[key_column].IsNull(); });
    if (!indexes_.back().IsUnique() || key_has_null) {
      std::vector<Value> include_values;
      include_values.reserve(indexes_.back().sc_.include_.size());
      for (const slot_t& i : indexes_.back().sc_.include_) {
        include_values.push_back(row[i]);
      }
      grouped_multi[indexes_.back().GenerateKey(row)].push_back(
          IndexValueType{it.Position(), Row(std::move(include_values))});
      ++it;
      continue;
    }
    Status status = IndexInsert(txn, indexes_.back(), row, it.Position());
    if (status != Status::kSuccess) {
      indexes_.pop_back();
      std::ignore = LsmIndex(root_pid).Destroy(txn);
      return status;
    }
    ++it;
  }
  if (!grouped_multi.empty()) {
    auto index_write_guard = txn.AcquireIndexWriteGuard(root_pid);
    BPlusTree bpt(root_pid);
    page_id_t cursor = 0;
    for (const auto& [key, rps] : grouped_multi) {
      Status status = bpt.Insert(txn, key, Encode(rps), &cursor);
      if (status != Status::kSuccess) {
        index_write_guard.unlock();
        indexes_.pop_back();
        std::ignore = LsmIndex(root_pid).Destroy(txn);
        return status;
      }
    }
  }
  return Status::kSuccess;
}

namespace {
// Best-effort compensations run after the primary failure is already decided;
// their own failures must stay observable instead of vanishing into
// std::ignore, or heap/index divergence goes undiagnosed.
void LogCompensationFailure(std::string_view operation, const Status& status) {
  if (status != Status::kSuccess) {
    LOG(WARN) << "Table compensation " << operation << " failed: " << status;
  }
}
}  // namespace

Status Table::CheckRowConstraints(const Row& row) const {
  if (row.values_.size() != schema_.ColumnCount()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "row column count does not match table schema");
  }
  for (slot_t i = 0; i < schema_.ColumnCount(); ++i) {
    const Constraint::ConstraintType type =
        schema_.GetColumn(i).GetConstraint().ctype;
    if ((type == Constraint::kNotNull || type == Constraint::kPrimaryKey) &&
        row[i].IsNull()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULL value in non-nullable column " +
                             std::string(schema_.GetColumn(i).Name().name));
    }
  }
  return Status::kSuccess;
}

Status Table::LinkReservedTableTail(Transaction& txn, PageRef& predecessor,
                                    PageRef& successor) const {
  PageManager* const page_manager = txn.GetPageManager();
  Status reserved = page_manager->LinkReservedTableTail(
      txn.ID(), first_pid_, predecessor->PageID(), successor->PageID());
  if (reserved != Status::kSuccess) {
    txn.MarkFailed();
    return reserved;
  }
  Status previous = successor->SetPrevPage(txn, predecessor->PageID());
  if (previous != Status::kSuccess) {
    txn.MarkFailed();
    return previous;
  }
  Status next = predecessor->SetNextPage(txn, successor->PageID());
  if (next != Status::kSuccess) {
    txn.MarkFailed();
    return next;
  }
  return Status::kSuccess;
}

StatusOr<RowPosition> Table::Insert(Transaction& txn, const Row& row) {
  // Opening the generation window first (and closing it on every exit below)
  // is what makes a concurrent ANALYZE unable to certify the chain: the row
  // this call appends may commit after the ANALYZE's snapshot yet still be
  // visible to a later reader.
  const RowMutationScope mutation(*this, /*rewrites_in_place=*/false);
  ASSIGN_OR_RETURN(TableLease, lease, AcquireLease(txn.ID()));
  PageManager* const page_manager = txn.GetPageManager();
  RETURN_IF_FAIL(CheckRowConstraints(row));
  RETURN_IF_FAIL(row.CheckSerializable());
  const page_id_t cached_tail =
      page_manager->GetTableTailFor(txn.ID(), first_pid_, last_pid_);
  ASSIGN_OR_RETURN(PageRef, ref, page_manager->GetPage(cached_tail));
  if (ref.IsNull() && cached_tail != first_pid_) {
    // The tail cache can outrun reality: it is seeded from the catalog's
    // DDL-persisted hint, and a healer may have published a page id whose
    // allocation an abort later undid. GetPage answers such an id with an
    // empty PageRef; restart the walk at the chain head, whose live links
    // lead back to the real tail (or to a clean grow-from-scratch below).
    LOG(ERROR) << "table " << schema_.Name() << " tail page " << cached_tail
               << " is unreadable; falling back to the chain head";
    ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
    ref = std::move(head);
  }
  if (ref.IsNull()) {
    return StatusError(StatusCode::kCorrupt, "table head page is unreadable: " +
                                                 std::to_string(first_pid_));
  }
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  txn.DebugSetPhase("rowpage-insert");
  StatusOr<slot_t> pos = ref->Insert(txn, serialized_row);
  txn.DebugSetPhase(nullptr);
  RowPosition rp;
  if (pos.HasValue()) {
    rp.page_id = ref->PageID();
    rp.slot = pos.Value();
  } else if (pos.GetStatus() == Status::kNoSpace) {
    for (;;) {
      const page_id_t predecessor = ref->PageID();
      const page_id_t next = ref->body.row_page.next_page_id_;
      if (next != 0) {
        const Status visible =
            page_manager->CheckTableTailPage(txn.ID(), first_pid_, next);
        if (visible == Status::kSuccess) {
          // Heal the tail cache while walking committed links: the cache is
          // seeded from the catalog's `last_pid_` hint, which only DDL
          // persists, so after a restart it is arbitrarily stale and
          // ReserveTableTail's expected-match guard below would reject every
          // growth attempt (kConflicts) for the life of the process.
          page_manager->AdvanceTableTail(first_pid_, predecessor, next);
          ASSIGN_OR_RETURN(PageRef, next_page, page_manager->GetPage(next));
          if (next_page.IsNull()) {
            // The link this walk followed outran a later abort: the growth
            // that installed it lost its transaction, and its undo -- which
            // needs this page's latch -- is still pending. Yield the latch
            // and give the undo a real chance to land before re-reading.
            LOG(ERROR) << "table " << schema_.Name() << " chain page " << next
                       << " is unreadable; waiting for its undo";
            ref.PageUnlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            ASSIGN_OR_RETURN(PageRef, walk_retry,
                             page_manager->GetPage(predecessor));
            ref = std::move(walk_retry);
            continue;
          }
          ref = std::move(next_page);
          StatusOr<slot_t> next_pos = ref->Insert(txn, serialized_row);
          if (next_pos.GetStatus() == Status::kSuccess) {
            rp.page_id = ref->PageID();
            rp.slot = next_pos.Value();
            break;
          }
          if (next_pos.GetStatus() != Status::kNoSpace) {
            ref.PageUnlock();
            return next_pos.GetStatus();
          }
          continue;
        }
        ref.PageUnlock();
        RETURN_IF_FAIL(
            page_manager->WaitForTableTailPage(txn, first_pid_, next));
        ASSIGN_OR_RETURN(PageRef, retry, page_manager->GetPage(predecessor));
        if (retry.IsNull()) {
          ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
          retry = std::move(head);
        }
        ref = std::move(retry);
        continue;
      }

      ref.PageUnlock();
      const Status reserved =
          page_manager->ReserveTableTail(txn, first_pid_, predecessor);
      if (reserved == Status::kConflicts && reserved.GetMessage().empty()) {
        // A concurrent growth linked and published a successor between this
        // walk's kNoSpace read and the claim, so the expected predecessor is
        // stale. That is a "retry the walk" answer, not a failed append:
        // re-lock the page and follow the chain (the 5s-wait timeout below
        // carries a message and stays a hard failure).
        ASSIGN_OR_RETURN(PageRef, retry, page_manager->GetPage(predecessor));
        if (retry.IsNull()) {
          ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
          retry = std::move(head);
        }
        ref = std::move(retry);
        continue;
      }
      RETURN_IF_FAIL(reserved);
      StatusOr<PageRef> retry_result = page_manager->GetPage(predecessor);
      if (!retry_result.HasValue()) {
        page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
        return retry_result.GetStatus();
      }
      if (retry_result.Value().IsNull()) {
        page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
        ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
        ref = std::move(head);
        continue;
      }
      ref = retry_result.MoveValue();
      if (ref->body.row_page.next_page_id_ != 0) {
        continue;
      }
      StatusOr<PageRef> new_page_result =
          page_manager->AllocateNewPage(txn, PageType::kRowPage);
      if (!new_page_result.HasValue()) {
        page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
        return new_page_result.GetStatus();
      }
      PageRef new_page = new_page_result.MoveValue();
      StatusOr<slot_t> new_pos = new_page->Insert(txn, serialized_row);
      if (!new_pos.HasValue()) {
        page_manager->DestroyPage(txn, new_page.get());
        page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
        return new_pos.GetStatus();
      }
      rp.page_id = new_page->PageID();
      rp.slot = new_pos.Value();
      RETURN_IF_FAIL(LinkReservedTableTail(txn, ref, new_page));
      new_page.PageUnlock();
      break;
    }
  } else {
    return pos.GetStatus();
  }
  // Release the latch early: the index updates below re-enter row pages via
  // GetPage(), which would otherwise self-deadlock on this page's latch.
  // PageRef::PageUnlock is idempotent, so the destructor stays safe.
  //
  // Phase1-4 hotspot spread: while still holding the landing page's latch,
  // pre-link an empty successor when it is nearly full and still the chain
  // tail. Concurrent appenders then find last_pid_ already advanced instead
  // of piling onto the same nearly-full page (and paying the allocation
  // under its latch). The `next_page_id_ == 0` guard makes the rotation
  // idempotent under latch serialization: whoever runs second sees the link
  // and skips. Chain order and last_pid_'s "tail" meaning are unchanged.
  if (ref->body.row_page.ShouldPreallocateNextBlock() &&
      ref->body.row_page.next_page_id_ == 0) {
    const Status claim =
        page_manager->TryReserveTableTail(txn.ID(), first_pid_, ref->PageID());
    if (claim != Status::kSuccess && claim != Status::kConflicts) {
      LOG(ERROR) << "TCHAIN6 status=" << claim;
      return claim;
    }
    if (claim == Status::kSuccess) {
      StatusOr<PageRef> successor_result =
          page_manager->AllocateNewPage(txn, PageType::kRowPage);
      if (!successor_result.HasValue()) {
        LOG(ERROR) << "TCHAIN7 alloc status=" << successor_result.GetStatus();
        page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
        return successor_result.GetStatus();
      }
      PageRef successor = successor_result.MoveValue();
      RETURN_IF_FAIL(LinkReservedTableTail(txn, ref, successor));
      successor.PageUnlock();
    }
  }
  ref.PageUnlock();
  for (size_t i = 0; i < indexes_.size(); ++i) {
    const Status status = IndexInsert(txn, indexes_[i], row, rp);
    if (status != Status::kSuccess) {
      // A rejected insert (e.g. a unique-key violation) must be atomic: undo
      // the index entries added before the failing one and remove the row
      // that was already written to the row page.
      for (size_t j = 0; j < i; ++j) {
        LogCompensationFailure("insert-rollback IndexDelete",
                               IndexDelete(txn, indexes_[j], rp));
      }
      ASSIGN_OR_RETURN(PageRef, written,
                       txn.GetPageManager()->GetPage(rp.page_id));
      if (written.IsNull()) {
        LogCompensationFailure(
            "insert-rollback heap Delete",
            StatusError(StatusCode::kCorrupt, "inserted page is unreadable"));
      } else {
        LogCompensationFailure("insert-rollback heap Delete",
                               written->Delete(txn, rp.slot));
      }
      return status;
    }
  }
  return rp;
}

namespace {
// Phase2-5: index-maintenance decisions come from comparing column offsets,
// not from regenerating every index key. Only columns some index consumes
// are examined.
// Key columns feed GenerateKey, i.e. EncodeMemcomparableFormat: equality
// must mirror the encoded bytes exactly, so doubles compare bit-wise here
// (Value::operator== applies an epsilon that would miss real key drift).
bool KeyColumnChanged(const Value& a, const Value& b) {
  if (a.type != b.type) {
    return true;
  }
  switch (a.type) {
    case ValueType::kInt64:
      if (a.IsUnsigned() != b.IsUnsigned()) {
        return true;
      }
      return a.value.int_value != b.value.int_value;
    case ValueType::kDate:
      return a.value.int_value != b.value.int_value;
    case ValueType::kVarChar:
      return a.value.varchar_value != b.value.varchar_value;
    case ValueType::kDouble:
      return std::bit_cast<uint64_t>(a.value.double_value) !=
             std::bit_cast<uint64_t>(b.value.double_value);
    case ValueType::kArray:
      return a.ArrayElementSqlType() != b.ArrayElementSqlType() ||
             a.ArrayElements() != b.ArrayElements();
    case ValueType::kNull:
      return false;
  }
  return true;
}

// True when no column feeding this index (key or include) differs between
// the original and the updated row.
bool IndexCoversUnchanged(const Index& idx,
                          const std::vector<slot_t>& changed_key_columns,
                          const std::vector<slot_t>& changed_include_columns) {
  const auto untouched = [](const std::vector<slot_t>& columns,
                            const std::vector<slot_t>& changed) {
    return std::ranges::all_of(columns, [&](slot_t slot) {
      return std::find(changed.begin(), changed.end(), slot) == changed.end();
    });
  };
  return untouched(idx.sc_.key_, changed_key_columns) &&
         untouched(idx.sc_.include_, changed_include_columns);
}
}  // namespace

StatusOr<RowPosition> Table::Update(Transaction& txn, const RowPosition& pos,
                                    const Row& row) {
  const auto contextualize = [&](Status status, std::string_view phase) {
    if (status == Status::kSuccess) {
      return status;
    }
    return StatusError(status.GetCode(),
                       "table update " + std::string(phase) + " at " +
                           std::to_string(pos.page_id) + "/" +
                           std::to_string(pos.slot) + ": " + ToString(status));
  };
  // An UPDATE rewrites the slot in place, so the pre-image stays reachable for
  // older snapshots: the in-process rewrite latch keeps any clustering proof
  // from outliving it (see Table::RowsRewrittenInPlace).
  const RowMutationScope mutation(*this, /*rewrites_in_place=*/true);
  StatusOr<TableLease> acquired = AcquireLease(txn.ID());
  if (!acquired.HasValue()) {
    return contextualize(acquired.GetStatus(), "lease");
  }
  TableLease lease = acquired.MoveValue();
  PageManager* const page_manager = txn.GetPageManager();
  RETURN_IF_FAIL(contextualize(
      page_manager->CheckTableTailPage(txn.ID(), first_pid_, pos.page_id),
      "tail visibility"));
  RETURN_IF_FAIL(CheckRowConstraints(row));
  // Phase2-3: the previous image feeds index maintenance only. Tables
  // without indexes skip the deserialize roundtrip entirely; RowPage::Update
  // still rejects absent/invisible slots with the same status Table::Read
  // would have produced. Read before RowPage::Update reserves the write
  // intent: an unstaged intent is deliberately invisible even to its owner.
  // The reservation still enforces first-updater-wins against any writer
  // that commits between this snapshot read and the physical update.
  // PRODUCTION FIX: the previous image is needed not only by index
  // maintenance but also by restore_physical_row(); an index-less table used
  // to leave original_row empty and rewrite a 0-column image into the slot,
  // destroying the row on every failed UPDATE.
  Row original_row;
  {
    StatusOr<Row> visible = Read(txn, pos);
    RETURN_IF_FAIL(contextualize(visible.GetStatus(), "before-image read"));
    original_row = visible.MoveValue();
  }
  RowPosition new_pos = pos;
  // Changed-column offsets among the columns indexes consume. Two tiny
  // sorted-unique lists replace two GenerateKey() string builds per index.
  std::vector<slot_t> changed_key_columns;
  std::vector<slot_t> changed_include_columns;
  bool indexes_unchanged = true;
  if (!indexes_.empty()) {
    for (const auto& idx : indexes_) {
      changed_key_columns.insert(changed_key_columns.end(),
                                 idx.sc_.key_.begin(), idx.sc_.key_.end());
      changed_include_columns.insert(changed_include_columns.end(),
                                     idx.sc_.include_.begin(),
                                     idx.sc_.include_.end());
    }
    std::sort(changed_key_columns.begin(), changed_key_columns.end());
    changed_key_columns.erase(
        std::unique(changed_key_columns.begin(), changed_key_columns.end()),
        changed_key_columns.end());
    std::sort(changed_include_columns.begin(), changed_include_columns.end());
    changed_include_columns.erase(std::unique(changed_include_columns.begin(),
                                              changed_include_columns.end()),
                                  changed_include_columns.end());
    const auto changed = [&](const std::vector<slot_t>& watched, bool key) {
      std::vector<slot_t> diff;
      for (slot_t slot : watched) {
        if (slot < original_row.values_.size() && slot < row.values_.size()) {
          const Value& before = original_row[slot];
          const Value& after = row[slot];
          const bool differs =
              key ? KeyColumnChanged(before, after) : before != after;
          if (differs) {
            diff.push_back(slot);
          }
        }
      }
      return diff;
    };
    changed_key_columns = changed(changed_key_columns, true);
    changed_include_columns = changed(changed_include_columns, false);
    indexes_unchanged = std::ranges::all_of(indexes_, [&](const Index& idx) {
      return IndexCoversUnchanged(idx, changed_key_columns,
                                  changed_include_columns);
    });
  }
  // Per-index B+Tree landing-leaf cursors: shared by the delete and insert
  // phases below so the second descent on the same tree reuses the leaf the
  // first one landed on whenever the live routing agrees.
  std::vector<page_id_t> idx_cursors(indexes_.size(), 0);
  RETURN_IF_FAIL(row.CheckSerializable());
  // Pre-acquire the write intent BEFORE latching the target page (the same
  // protocol Table::Delete already follows): RowPage::Update's internal
  // AddWriteSet then short-circuits on the write-set entry, so a genuine
  // same-row conflict parks LATCH-FREE here. Waiting latched used to hold
  // the page content latch for the whole intent wait -- which blocked the
  // conflicting owner's abort undo (it needs that latch) and manufactured
  // the multi-second write-intent escape-hatch stalls the detector cannot
  // see (the latch edge is not in the wait-for graph).
  if (!txn.AddWriteSet(new_pos)) {
    return contextualize(Status::kConflicts, "write intent");
  }
  ASSIGN_OR_RETURN(PageRef, page,
                   txn.GetPageManager()->GetPage(new_pos.page_id));
  if (page.IsNull()) {
    return StatusError(
        StatusCode::kCorrupt,
        "update target page is unreadable: " + std::to_string(new_pos.page_id));
  }
  txn.DebugSetPhase("rowpage-update");
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  // Best-effort compensations: a failed index mutation must not leave applied
  // entries behind (old keys deleted / new keys partially inserted).
  const auto reinstate_old_keys = [&](size_t count, const RowPosition& at) {
    for (size_t j = 0; j < count; ++j) {
      const Status status =
          IndexInsert(txn, indexes_[j], original_row, at, &idx_cursors[j]);
      LogCompensationFailure("update-rollback IndexInsert", status);
      if (status != Status::kSuccess) {
        txn.MarkFailed();
      }
    }
  };
  const auto undo_index_inserts = [&](size_t count) {
    for (size_t j = 0; j < count; ++j) {
      const Status status =
          IndexDelete(txn, indexes_[j], new_pos, row, &idx_cursors[j]);
      LogCompensationFailure("update-rollback IndexDelete", status);
      if (status != Status::kSuccess) {
        txn.MarkFailed();
      }
    }
  };
  // Phase2-3: serialize the pre-update image once, on first use, instead of
  // rebuilding it for every compensation attempt.
  std::string original_image;
  const auto restore_physical_row = [&](const RowPosition& target_pos,
                                        bool target_was_inserted) -> Status {
    if (original_image.empty()) {
      original_image.resize(original_row.Size());
      std::ignore = original_row.Serialize(original_image.data());
    }
    StatusOr<PageRef> written =
        txn.GetPageManager()->GetPage(target_pos.page_id);
    if (written.GetStatus() != Status::kSuccess) {
      txn.MarkFailed();
      return written.GetStatus();
    }
    if (written.Value().IsNull() ||
        written.Value()->Type() != PageType::kRowPage) {
      txn.MarkFailed();
      return Status::kCorrupt;
    }
    StatusOr<lsn_t> compensation_end =
        written.Value()->body.row_page.RestorePhysicalUpdate(
            target_pos.page_id, txn, target_pos.slot, original_image);
    if (compensation_end.GetStatus() != Status::kSuccess) {
      txn.MarkFailed();
      return compensation_end.GetStatus();
    }
    written.Value()->SetPageLSN(compensation_end.Value());
    written.Value()->SetRecLSN(txn.PrevLSN());
    try {
      txn.RegisterVersionWrite(
          target_pos,
          target_was_inserted ? std::optional<std::string_view>(std::nullopt)
                              : std::optional<std::string_view>(original_image),
          original_image);
    } catch (...) {
      txn.MarkFailed();
      return Status::kRuntimeError;
    }
    return Status::kSuccess;
  };

  Status s = page->Update(txn, new_pos.slot, serialized_row);
  txn.DebugSetPhase(nullptr);
  if (s == Status::kSuccess && indexes_unchanged) {
    return new_pos;
  }
  if (s == Status::kSuccess) {
    // Release the in-place page's exclusive latch BEFORE index maintenance:
    // holding a RowPage latch while IndexDelete/IndexInsert acquires
    // AcquireIndexWriteGuard inverts lock order against IndexScan, which holds
    // AcquireIndexReadGuard while pinning RowPages via Table::Read.
    page.PageUnlock();
    Status failure = Status::kSuccess;
    size_t deleted = 0;
    for (; deleted < indexes_.size(); ++deleted) {
      failure = IndexDelete(txn, indexes_[deleted], pos, original_row,
                            &idx_cursors[deleted]);
      if (failure != Status::kSuccess) {
        break;
      }
    }
    if (deleted < indexes_.size()) {
      reinstate_old_keys(deleted, pos);
      const Status restore_status =
          restore_physical_row(pos, /*target_was_inserted=*/false);
      return contextualize(
          restore_status == Status::kSuccess ? failure : restore_status,
          "in-place index update");
    }
    size_t inserted = 0;
    for (; inserted < indexes_.size(); ++inserted) {
      failure = IndexInsert(txn, indexes_[inserted], row, new_pos,
                            &idx_cursors[inserted]);
      if (failure != Status::kSuccess) {
        break;
      }
    }
    if (inserted < indexes_.size()) {
      // Fully restore the pre-update state: undo the applied inserts,
      // reinstate every original key, and rewrite the original row image.
      undo_index_inserts(inserted);
      reinstate_old_keys(indexes_.size(), pos);
      const Status restore_status =
          restore_physical_row(pos, /*target_was_inserted=*/false);
      return contextualize(
          restore_status == Status::kSuccess ? failure : restore_status,
          "in-place index update");
    }
    return new_pos;
  }
  if (s != Status::kNoSpace) {
    return contextualize(s, "in-place row write");
  }
  // PRODUCTION RESTRUCTURE: reserve the relocation target BEFORE deleting
  // the original row.  The old delete-first ordering compensated failures by
  // rewriting the old image, but RowPage::Update cannot resurrect a deleted
  // (empty) slot, so every failed relocation silently destroyed the row.
  // With reserve-first, a failure leaves the row untouched at pos.
  // Use the same owner-visible append cursor as INSERT. Walking from the
  // source row repeatedly scans the rest of a large table, and cannot heal
  // a conservative catalog hint lying BEFORE that source after a restart.
  // Only a walk beginning at the cached cursor can advance its expected-link
  // proof all the way to the tail before reserving growth.
  page.PageUnlock();
  const page_id_t cached_tail =
      page_manager->GetTableTailFor(txn.ID(), first_pid_, last_pid_);
  ASSIGN_OR_RETURN(PageRef, target, page_manager->GetPage(cached_tail));
  if (target.IsNull() && cached_tail != first_pid_) {
    ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
    target = std::move(head);
  }
  if (target.IsNull()) {
    return contextualize(Status::kCorrupt, "relocation target");
  }
  page = std::move(target);
  for (;;) {
    StatusOr<slot_t> inserted = page->Insert(txn, serialized_row);
    if (inserted.HasValue()) {
      new_pos = RowPosition(page->PageID(), inserted.Value());
      page.PageUnlock();
      break;
    }
    if (inserted.GetStatus() != Status::kNoSpace) {
      return contextualize(inserted.GetStatus(), "relocation insert");
    }
    const page_id_t predecessor = page->PageID();
    const page_id_t next_page = page->body.row_page.next_page_id_;
    if (next_page != 0) {
      const Status visible =
          page_manager->CheckTableTailPage(txn.ID(), first_pid_, next_page);
      if (visible == Status::kSuccess) {
        page_manager->AdvanceTableTail(first_pid_, predecessor, next_page);
        ASSIGN_OR_RETURN(PageRef, next_ref, page_manager->GetPage(next_page));
        if (next_ref.IsNull()) {
          LOG(ERROR) << "table " << schema_.Name() << " chain page "
                     << next_page << " is unreadable; waiting for its undo";
          page.PageUnlock();
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
          ASSIGN_OR_RETURN(PageRef, walk_retry,
                           page_manager->GetPage(predecessor));
          page = std::move(walk_retry);
          continue;
        }
        page = std::move(next_ref);
        continue;
      }
      page.PageUnlock();
      RETURN_IF_FAIL(
          page_manager->WaitForTableTailPage(txn, first_pid_, next_page));
      ASSIGN_OR_RETURN(PageRef, retry, page_manager->GetPage(predecessor));
      if (retry.IsNull()) {
        ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
        retry = std::move(head);
      }
      page = std::move(retry);
      continue;
    }

    page.PageUnlock();
    const Status reserved =
        page_manager->ReserveTableTail(txn, first_pid_, predecessor);
    if (reserved == Status::kConflicts && reserved.GetMessage().empty()) {
      // A successor was published between the full-page observation and the
      // reservation. As in INSERT, re-read the link; a timeout/cycle error
      // has a message and remains a statement failure.
      ASSIGN_OR_RETURN(PageRef, retry, page_manager->GetPage(predecessor));
      if (retry.IsNull()) {
        ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
        retry = std::move(head);
      }
      page = std::move(retry);
      continue;
    }
    RETURN_IF_FAIL(contextualize(reserved, "tail reservation"));
    StatusOr<PageRef> retry_result = page_manager->GetPage(predecessor);
    if (!retry_result.HasValue()) {
      page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
      return retry_result.GetStatus();
    }
    if (retry_result.Value().IsNull()) {
      page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
      ASSIGN_OR_RETURN(PageRef, head, page_manager->GetPage(first_pid_));
      page = std::move(head);
      continue;
    }
    page = retry_result.MoveValue();
    if (page->body.row_page.next_page_id_ != 0) {
      continue;
    }
    StatusOr<PageRef> new_page_result =
        page_manager->AllocateNewPage(txn, PageType::kRowPage);
    if (!new_page_result.HasValue()) {
      page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
      return new_page_result.GetStatus();
    }
    PageRef new_page = new_page_result.MoveValue();
    StatusOr<slot_t> new_slot = new_page->Insert(txn, serialized_row);
    if (!new_slot.HasValue()) {
      page_manager->DestroyPage(txn, new_page.get());
      page_manager->CancelTableTailReservation(txn.ID(), first_pid_);
      return contextualize(new_slot.GetStatus(), "new tail insert");
    }
    new_pos = RowPosition(new_page->PageID(), new_slot.Value());
    RETURN_IF_FAIL(contextualize(LinkReservedTableTail(txn, page, new_page),
                                 "tail publication"));
    new_page.PageUnlock();
    page.PageUnlock();
    break;
  }
  // The copy at new_pos exists; now remove index entries and the original
  // physical row.  Every failure below compensates by dropping the copy and
  // keeping the original intact.
  Status failure = Status::kSuccess;
  size_t deleted = 0;
  for (; deleted < indexes_.size(); ++deleted) {
    failure = IndexDelete(txn, indexes_[deleted], pos, original_row,
                          &idx_cursors[deleted]);
    if (failure != Status::kSuccess) {
      break;
    }
  }
  if (deleted < indexes_.size()) {
    // Drop the reserved copy; the original row survives at pos, but the
    // first `deleted` index entries were already removed and must be
    // reinstated or the row would go missing from those index scans.
    ASSIGN_OR_RETURN(PageRef, copy_page,
                     txn.GetPageManager()->GetPage(new_pos.page_id));
    LogCompensationFailure("update-rollback reserved-copy Delete",
                           copy_page.IsNull()
                               ? StatusError(StatusCode::kCorrupt,
                                             "reserved copy page is unreadable")
                               : copy_page->Delete(txn, new_pos.slot));
    copy_page.PageUnlock();
    reinstate_old_keys(deleted, pos);
    return contextualize(failure, "relocation index update");
  }
  ASSIGN_OR_RETURN(PageRef, source_page,
                   txn.GetPageManager()->GetPage(pos.page_id));
  const Status delete_status = source_page.IsNull()
                                   ? Status::kNotExists
                                   : source_page->Delete(txn, pos.slot);
  source_page.PageUnlock();
  if (delete_status != Status::kSuccess) {
    // The original row survives at pos; drop the reserved copy and roll the
    // index deletes back.
    ASSIGN_OR_RETURN(PageRef, copy_page,
                     txn.GetPageManager()->GetPage(new_pos.page_id));
    LogCompensationFailure("update-rollback reserved-copy Delete",
                           copy_page.IsNull()
                               ? StatusError(StatusCode::kCorrupt,
                                             "reserved copy page is unreadable")
                               : copy_page->Delete(txn, new_pos.slot));
    copy_page.PageUnlock();
    reinstate_old_keys(indexes_.size(), pos);
    return contextualize(delete_status, "relocation source delete");
  }
  size_t inserted = 0;
  for (; inserted < indexes_.size(); ++inserted) {
    failure = IndexInsert(txn, indexes_[inserted], row, new_pos,
                          &idx_cursors[inserted]);
    if (failure != Status::kSuccess) {
      break;
    }
  }
  if (inserted < indexes_.size()) {
    // The row lives at new_pos now; undo the partial inserts, restore the
    // original row image at new_pos, and repoint the original keys at the
    // relocated position.
    undo_index_inserts(inserted);
    const Status restore_status =
        restore_physical_row(new_pos, /*target_was_inserted=*/true);
    reinstate_old_keys(indexes_.size(), new_pos);
    return contextualize(
        restore_status == Status::kSuccess ? failure : restore_status,
        "relocation index update");
  }
  return new_pos;
}
Status Table::Delete(Transaction& txn, RowPosition pos) {
  // A DELETE vacates the slot for a later reuse, so the row an older snapshot
  // still resolves and the row a later INSERT may put in the hole differ; the
  // rewrite latch covers both (see Table::RowsRewrittenInPlace).
  const RowMutationScope mutation(*this, /*rewrites_in_place=*/true);
  ASSIGN_OR_RETURN(TableLease, lease, AcquireLease(txn.ID()));
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  // Read before reserving the intent, then install the old image as the
  // chain's base version: concurrent readers must keep seeing this row
  // between the intent reservation and the physical delete.
  std::string delete_image(original_row.Size(), '\0');
  original_row.Serialize(delete_image.data());
  if (!txn.AddWriteSet(pos, delete_image)) {
    return Status::kConflicts;
  }
  Status failure = Status::kSuccess;
  size_t deleted = 0;
  for (; deleted < indexes_.size(); ++deleted) {
    failure = IndexDelete(txn, indexes_[deleted], pos, original_row);
    if (failure != Status::kSuccess) {
      break;
    }
  }
  if (deleted < indexes_.size()) {
    // The row survives, so every applied index deletion must be undone:
    // "row present x some index entries gone" is not a legal state.
    for (size_t j = 0; j < deleted; ++j) {
      LogCompensationFailure("delete-rollback IndexInsert",
                             IndexInsert(txn, indexes_[j], original_row, pos));
    }
    return failure;
  }
  ASSIGN_OR_RETURN(PageRef, source_page,
                   txn.GetPageManager()->GetPage(pos.page_id));
  if (source_page.IsNull()) {
    return Status::kNotExists;
  }
  txn.DebugSetPhase("rowpage-delete");
  Status delete_status = source_page->Delete(txn, pos.slot);
  txn.DebugSetPhase(nullptr);
  if (delete_status == Status::kNotExists) {
    // The snapshot read above proved the row is logically visible, yet its
    // physical image is gone from the slot: RowPage::Read fell back to the
    // MVCC version chain because the slot is vacant.  That displaced state
    // is produced by abort-time undo restoring a deleted row into the first
    // free slot instead of its original one (recovery LogUndo ->
    // Page::InsertImpl -> RowPage::InsertRow), leaving the index pointing at
    // an empty slot over a still-visible version.  Failing here turned every
    // later DELETE of such a row into a hard error -- the TPC-C Delivery
    // "delivery queue delete affected too few rows" outage.  Complete the
    // delete logically instead: the index keys are already removed above and
    // the tombstone below hides the row from future snapshots while older
    // snapshots keep reading their own version.  No kDeleteRow record is
    // written on purpose: redoing a physical delete against a vacant slot
    // would corrupt row accounting after restart.
    std::string image(original_row.Size(), '\0');
    original_row.Serialize(image.data());
    txn.RegisterVersionWrite(pos, image, std::nullopt);
    return Status::kSuccess;
  }
  if (delete_status != Status::kSuccess && !indexes_.empty()) {
    // The physical delete failed after all keys were removed: reinstate them
    // so the surviving row stays reachable from every index.
    for (size_t i = indexes_.size(); 0 < i; --i) {
      LogCompensationFailure(
          "delete-rollback IndexInsert",
          IndexInsert(txn, indexes_[i - 1], original_row, pos));
    }
  }
  return delete_status;
}

StatusOr<Row> Table::Read(Transaction& txn, RowPosition pos) const {
  ASSIGN_OR_RETURN(TableLease, lease, AcquireLease(txn.ID()));
  RETURN_IF_FAIL(txn.GetPageManager()->CheckTableTailPage(txn.ID(), first_pid_,
                                                          pos.page_id));
  // Keep the PageRef alive while decoding: read_row points into the page
  // buffer, which is only guaranteed stable while the page stays pinned.
  // (The resulting Row owns its strings; Value::Deserialize copies varchar
  // data out of the buffer.)
  ASSIGN_OR_RETURN(PageRef, page,
                   txn.GetPageManager()->GetPage(pos.page_id, true));
  if (page.IsNull()) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(std::string_view, read_row, page->Read(txn, pos.slot));
  Row result;
  // Length-prefixed row image from a checksummed page: consumed by offsets,
  // never as a C string.
  result.Deserialize(
      read_row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
      schema_);
  return result;
}

StatusOr<Row> Table::ReadLatest(Transaction& txn, RowPosition pos) const {
  ASSIGN_OR_RETURN(TableLease, lease, AcquireLease(txn.ID()));
  RETURN_IF_FAIL(txn.GetPageManager()->CheckTableTailPage(txn.ID(), first_pid_,
                                                          pos.page_id));
  ASSIGN_OR_RETURN(PageRef, page,
                   txn.GetPageManager()->GetPage(pos.page_id, true));
  if (page.IsNull()) {
    return Status::kNotExists;
  }
  std::optional<std::string_view> physical;
  if (page->body.row_page.IsSlotOccupied(pos.slot)) {
    physical = page->body.row_page.GetRow(pos.slot);
  }
  ASSIGN_OR_RETURN(std::optional<std::string>, latest_version,
                   txn.ReadLatestVersion(pos, physical));
  std::string_view read_row;
  if (latest_version.has_value()) {
    read_row = *latest_version;
  } else if (physical.has_value()) {
    read_row = *physical;
  } else {
    return Status::kNotExists;
  }
  Row result;
  result.Deserialize(
      read_row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
      schema_);
  return result;
}

// A NULL anywhere in the key columns; SQL unique semantics say NULL !=
// NULL, so such keys never collide (they still occupy the 1-byte NULL
// encoding in the B+Tree so scans can reach them).
namespace {
bool KeyHasNull(const Index& idx, const Row& row) {
  return std::ranges::any_of(idx.sc_.key_, [&row](const slot_t key_column) {
    return row[key_column].IsNull();
  });
}
}  // namespace

namespace {
// Ceiling for the gate-free escalation below: once the in-gate drain budget
// expired, the RMW re-runs without the write gate so reservation owners can
// finish; this bounds how long one statement parks there before surfacing a
// retryable conflict.
constexpr std::chrono::milliseconds kIndexEscalationDrainBudget{2500};
constexpr int kMaxIndexEscalations = 4;
}  // namespace

Status Table::IndexInsert(Transaction& txn, const Index& idx,
                          const Row& new_row, const RowPosition& pos,
                          page_id_t* hint_leaf) const {
  for (int escalation = 0;; ++escalation) {
    // Pre-drain foreign index-undo reservations BEFORE taking this index's
    // write gate: the drain is latch/gate-free here, while inside the guarded
    // body a slow reservation owner turns into a gate x reservation cycle
    // (the owner's next index op needs the same gate). Best-effort: a
    // reservation that appears after this point is handled by the short
    // in-tree drain budget. The A/B that removed this pass (and scoped the
    // in-tree checks to the refusing page) collapsed TPC-C throughput ~100x:
    // the gate-free drains are part of the admission equilibrium, and
    // releasing RMWs past unrelated reservations wedged waiters of waiters
    // into write-intent escape-hatch timeouts.
    if (txn.HasForeignIndexReservations()) {
      (void)txn.WaitAllIndexReservationsDrain(std::chrono::milliseconds(250));
    }
    RowPosition wait_pos;
    auto index_write_guard = txn.AcquireIndexWriteGuard(idx.Root());
    Status status =
        IndexInsertGuarded(txn, idx, new_row, pos, hint_leaf, &wait_pos);
    index_write_guard.unlock();
    if (status == Status::kDuplicates && wait_pos.IsValid()) {
      const Status waited = txn.WaitForRowWriteIntent(wait_pos);
      if (waited == Status::kSuccess) {
        --escalation;
        continue;
      }
      return waited == Status::kDuplicates ? Status::kDuplicates : waited;
    }
    if (status == Status::kSuccess ||
        !txn.ConsumeIndexReservationDrainExhausted() ||
        escalation >= kMaxIndexEscalations) {
      return status;
    }
    // The guarded body's short in-gate drain budget expired: a foreign
    // index-undo reservation outlived it WHILE this thread held the write
    // gate, and the returned kNoSpace is a contention artifact, not a full
    // page. Returning it would fail the statement with a status the client
    // cannot distinguish from real exhaustion. Instead run the RMW again
    // with the gate released -- the release lets the owners progress, so
    // the drain here actually drains instead of manufacturing the very
    // cycle the short budget exists to escape. Wound-aware: our own abort
    // releases every reservation we hold and is frequently the event the
    // others are waiting for.
    const Status drained =
        txn.WaitAllIndexReservationsDrain(kIndexEscalationDrainBudget);
    if (drained != Status::kSuccess) {
      return StatusError(StatusCode::kConflicts,
                         "index-undo reservation drain timed out; retry");
    }
  }
}

Status Table::IndexInsertGuarded(Transaction& txn, const Index& idx,
                                 const Row& new_row, const RowPosition& pos,
                                 page_id_t* hint_leaf,
                                 RowPosition* wait_pos) const {
  BPlusTree bpt(idx.pid_);
  const std::string key = idx.GenerateKey(new_row);
  std::vector<Value> include_values;
  include_values.reserve(idx.sc_.include_.size());
  for (const auto& in : idx.sc_.include_) {
    include_values.push_back(new_row[in]);
  }
  Row include(include_values);
  // One landing-leaf cursor serves the read-modify-write pair below.
  page_id_t cursor = 0;
  if (hint_leaf != nullptr) {
    cursor = *hint_leaf;
  }
  // NULL-bearing keys use the multi-value list encoding even for a UNIQUE
  // index: SQL says NULL != NULL, so two rows with a NULL key must both be
  // insertable, while the single-value encoding would collide on the shared
  // NULL key image and reject the second row with kDuplicates.
  if (idx.StoresSingleValue() && !KeyHasNull(idx, new_row)) {
    // BTree value is encoded pair<RowPosition, vector<Value>>.
    IndexValueType val;
    val.pos = pos;
    val.include = include;
    const Status ins = bpt.Insert(txn, key, Encode(val), &cursor);
    if (ins == Status::kDuplicates) {
      StatusOr<std::string> existing_value = bpt.Read(txn, key, &cursor);
      if (!existing_value.HasValue()) {
        RETURN_IF_FAIL(bpt.Insert(txn, key, Encode(val), &cursor));
      } else {
        std::string_view existing_data = existing_value.Value();
        ASSIGN_OR_RETURN(IndexValueType, existing,
                         Decode<IndexValueType>(existing_data));
        if (existing.pos == pos) {
          if (existing.include != include) {
            RETURN_IF_FAIL(bpt.Update(txn, key, Encode(val), &cursor));
          }
        } else {
          if (txn.HasForeignWriteIntent(existing.pos)) {
            if (wait_pos != nullptr) {
              *wait_pos = existing.pos;
            }
            return Status::kDuplicates;
          }
          StatusOr<Row> conflicting = ReadLatest(txn, existing.pos);
          if (conflicting.HasValue() &&
              idx.GenerateKey(conflicting.Value()) == key) {
            return Status::kDuplicates;
          }
          RETURN_IF_FAIL(bpt.Update(txn, key, Encode(val), &cursor));
        }
      }
    } else {
      RETURN_IF_FAIL(ins);
    }
  } else {
    // BTree value is encoded vector<pair<RowPosition, vector<Value>>>.
    StatusOr<std::string> existing_value = bpt.Read(txn, key, &cursor);
    if (existing_value.HasValue()) {
      std::string_view existing_data = existing_value.Value();
      ASSIGN_OR_RETURN(std::vector<IndexValueType>, rps,
                       Decode<std::vector<IndexValueType>>(existing_data));
      if (idx.IsUnique() && !KeyHasNull(idx, new_row)) {
        for (IndexValueType& value : rps) {
          if (value.pos == pos) {
            // An UPDATE whose key did not change needs no second entry. The
            // heap supplies the versioned row; update the latest unversioned
            // INCLUDE columns in place if they changed.
            if (value.include != include) {
              value.include = include;
              RETURN_IF_FAIL(bpt.Update(txn, key, Encode(rps), &cursor));
            }
            if (hint_leaf != nullptr) {
              *hint_leaf = cursor;
            }
            return Status::kSuccess;
          }
          if (txn.HasForeignWriteIntent(value.pos)) {
            if (wait_pos != nullptr) {
              *wait_pos = value.pos;
            }
            return Status::kDuplicates;
          }
          // A retained entry only blocks this insert if the row it points
          // at STILL owns this key at the latest committed head: an UPDATE
          // that moved the row to a different key leaves the old entry behind
          // for old snapshots, and the key here is free.
          StatusOr<Row> conflicting = ReadLatest(txn, value.pos);
          if (conflicting.HasValue() &&
              idx.GenerateKey(conflicting.Value()) == key) {
            return Status::kDuplicates;
          }
        }
      }
      rps.push_back({pos, include});
      RETURN_IF_FAIL(bpt.Update(txn, key, Encode(rps), &cursor));
    } else {
      std::vector<IndexValueType> rps{{pos, include}};
      RETURN_IF_FAIL(bpt.Insert(txn, key, Encode(rps), &cursor));
    }
  }
  if (hint_leaf != nullptr) {
    *hint_leaf = cursor;
  }
  return Status::kSuccess;
}

Status Table::IndexDelete(Transaction& txn, const Index& idx,
                          const RowPosition& pos) const {
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  return IndexDelete(txn, idx, pos, original_row);
}

Status Table::IndexDelete(Transaction& txn, const Index& idx,
                          const RowPosition& pos, const Row& original_row,
                          page_id_t* hint_leaf) {
  // IndexDelete carries no unique-check: unlike IndexInsert there is no
  // read-modify-write pair that needs the gate for correctness -- the gate
  // here only serializes structure-boundary ops against index readers.
  // The escalation loop mirrors IndexInsert's: an in-gate drain-budget
  // expiry re-runs the tree op gate-free instead of surfacing a bare
  // kNoSpace for what is a contention artifact.
  for (int escalation = 0;; ++escalation) {
    if (txn.HasForeignIndexReservations()) {
      (void)txn.WaitAllIndexReservationsDrain(std::chrono::milliseconds(250));
    }
    auto index_write_guard = txn.AcquireIndexWriteGuard(idx.Root());
    Status status = IndexDeleteGuarded(txn, idx, pos, original_row, hint_leaf);
    index_write_guard.unlock();
    if (status == Status::kSuccess ||
        !txn.ConsumeIndexReservationDrainExhausted() ||
        escalation >= kMaxIndexEscalations) {
      return status;
    }
    const Status drained =
        txn.WaitAllIndexReservationsDrain(kIndexEscalationDrainBudget);
    if (drained != Status::kSuccess) {
      return StatusError(StatusCode::kConflicts,
                         "index-undo reservation drain timed out; retry");
    }
  }
}

Status Table::IndexDeleteGuarded(Transaction& txn, const Index& idx,
                                 const RowPosition& pos,
                                 const Row& original_row,
                                 page_id_t* hint_leaf) {
  BPlusTree bpt(idx.pid_);
  const std::string key = idx.GenerateKey(original_row);
  page_id_t cursor = 0;
  if (idx.RetainsDeletedEntries()) {
    // Keep the key -> RowPosition edge.  RowPage MVCC decides whether the
    // pointed version is visible, which makes the same index usable by both
    // a pre-delete reader and a post-delete reader without a table scan.
    if (hint_leaf != nullptr) {
      *hint_leaf = cursor;
    }
    return Status::kSuccess;
  }
  txn.MarkIndexKeysChanged(idx.Root(), key);
  // Rows whose key bears a NULL were inserted through the multi-value list
  // encoding (see IndexInsert); deleting them must read that list, not the
  // single-value image, or the stale entry keeps blocking scans.
  if (idx.StoresSingleValue() && !KeyHasNull(idx, original_row)) {
    RETURN_IF_FAIL(bpt.Delete(txn, key));
  } else {
    ASSIGN_OR_RETURN(std::string, existing_data, bpt.Read(txn, key, &cursor));
    ASSIGN_OR_RETURN(std::vector<IndexValueType>, values,
                     Decode<std::vector<IndexValueType>>(existing_data));
    std::erase_if(values,
                  [&](const IndexValueType& v) { return v.pos == pos; });
    if (values.empty()) {
      RETURN_IF_FAIL(bpt.Delete(txn, key));
    } else {
      RETURN_IF_FAIL(bpt.Update(txn, key, Encode(values), &cursor));
    }
  }
  if (hint_leaf != nullptr) {
    *hint_leaf = cursor;
  }
  return Status::kSuccess;
}

Iterator Table::BeginFullScan(Transaction& txn,
                              const TableScanOptions& options) const {
  // FullScanIterator's ctors are private with Table as friend, so make_unique
  // is unavailable; the unique_ptr is built from new inside this member and
  // Iterator's explicit adopting ctor never throws.
  auto owned = std::unique_ptr<FullScanIterator>(new FullScanIterator(
      this, &txn, options.projection, options.key_filter, options.key_column,
      options.peek_compares, options.lock_rows, options.wait_for_write_intent,
      options.runtime_filter, options.runtime_filter_type));
  return Iterator(owned.release());
}

Iterator Table::BeginMorselScan(
    Transaction& txn, const ScanMorsel& pages,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column,
    const std::vector<IntegerPeekCompare>* peek_compares,
    const RuntimeFilterGroup* runtime_filter,
    ValueType runtime_filter_type) const {
  auto owned = std::unique_ptr<FullScanIterator>(new FullScanIterator(
      this, &txn, pages, std::move(projection), key_filter, key_column,
      peek_compares, runtime_filter, runtime_filter_type));
  return Iterator(owned.release());
}

std::shared_ptr<const LSTableStore> Table::GetLSTableStore() const {
  if (incarnation_state_ == nullptr) {
    return nullptr;
  }
  return std::static_pointer_cast<const LSTableStore>(
      incarnation_state_->GetLSTableStoreRaw());
}

void Table::SetLSTableStore(std::shared_ptr<const LSTableStore> store) const {
  if (incarnation_state_ != nullptr) {
    incarnation_state_->SetLSTableStoreRaw(std::move(store));
  }
}

std::shared_ptr<const LSTableStore> Table::RebuildLSTableStore(
    Transaction& txn) const {
  return BuildLSTableStore(txn, *this);
}

std::vector<Table::ScanMorsel> Table::BuildScanMorsels(
    Transaction& txn, size_t pages_per_morsel) const {
  StatusOr<TableLease> lease = AcquireLease(txn.ID());
  if (!lease.HasValue()) {
    return {};
  }
  pages_per_morsel = std::max<size_t>(1, pages_per_morsel);
  if (txn.IsReadOnly() && !txn.IsFailed() && !RowsRewrittenInPlace()) {
    std::shared_ptr<const LSTableStore> store =
        GetOrBuildLSTableStore(txn, *this, /*allow_lazy_build=*/false);
    if (store != nullptr && !store->page_ids.empty()) {
      bool clean = true;
      if (txn.HasAnyPageVersionChains()) {
        for (const page_id_t pid : store->page_ids) {
          if (txn.HasPageVersionChains(pid)) {
            clean = false;
            break;
          }
        }
      }
      if (clean && store->layout_generation == LayoutGeneration()) {
        std::vector<ScanMorsel> morsels;
        morsels.reserve((store->page_ids.size() + pages_per_morsel - 1) /
                        pages_per_morsel);
        for (const page_id_t pid : store->page_ids) {
          if (morsels.empty() || morsels.back().size() == pages_per_morsel) {
            morsels.emplace_back();
            morsels.back().reserve(pages_per_morsel);
          }
          morsels.back().push_back(pid);
        }
        return morsels;
      }
    }
  }
  std::vector<ScanMorsel> morsels;
  page_id_t page_id = first_pid_;
  std::unordered_set<page_id_t> visited;
  while (page_id != 0) {
    if (!visited.insert(page_id).second) {
      txn.MarkFailed();
      return {};
    }
    StatusOr<PageRef> page = txn.GetPageManager()->GetPage(page_id, true);
    if (!page.HasValue() || page.Value().IsNull() ||
        page.Value()->Type() != PageType::kRowPage) {
      txn.MarkFailed();
      return {};
    }
    const page_id_t predecessor = page_id;
    const page_id_t next_page = page.Value()->body.row_page.next_page_id_;
    if (next_page == 0) {
      if (morsels.empty() || morsels.back().size() == pages_per_morsel) {
        morsels.emplace_back();
        morsels.back().reserve(pages_per_morsel);
      }
      morsels.back().push_back(predecessor);
      page.Value().PageUnlock();
      break;
    }
    const Status visible = txn.GetPageManager()->CheckTableTailPage(
        txn.ID(), first_pid_, next_page);
    if (visible != Status::kSuccess) {
      page.Value().PageUnlock();
      const Status waited = txn.GetPageManager()->WaitForTableTailPage(
          txn, first_pid_, next_page);
      if (waited != Status::kSuccess) {
        txn.MarkFailed();
        return {};
      }
      visited.erase(predecessor);
      page_id = predecessor;
      continue;
    }
    if (morsels.empty() || morsels.back().size() == pages_per_morsel) {
      morsels.emplace_back();
      morsels.back().reserve(pages_per_morsel);
    }
    morsels.back().push_back(predecessor);
    page_id = next_page;
  }
  return morsels;
}

std::shared_ptr<const Table::IntegerKeyCounts> Table::TryReadIntegerKeyCounts(
    Transaction& txn, const std::vector<ScanMorsel>& morsels, slot_t column,
    int64_t minimum, size_t span) const {
  return ReadIntegerKeyCounts(txn, morsels, column, minimum, span, false);
}

std::shared_ptr<const Table::IntegerKeyCounts>
Table::FindCachedIntegerKeyCounts(Transaction& txn, slot_t column,
                                  int64_t minimum, size_t span) const {
  return ReadIntegerKeyCounts(txn, {}, column, minimum, span, true);
}

std::shared_ptr<const Table::IntegerKeyCounts> Table::ReadIntegerKeyCounts(
    Transaction& txn, const std::vector<ScanMorsel>& morsels, slot_t column,
    int64_t minimum, size_t span, bool cached_pages_only) const {
  // This is an exact physical projection, never an optimizer estimate. Just
  // like FullScanIterator's physical path, every page must have no MVCC
  // chains while its content latch is held. A stable layout generation
  // rejects a mutation crossing the pass; page LSNs also cover runtime undo.
  constexpr size_t kCacheBytes = size_t{1024} * 1024;
  if (!txn.IsReadOnly() || txn.IsFailed() || !incarnation_state_ || span == 0 ||
      span > 65536 ||
      static_cast<__int128_t>(minimum) + span - 1 >
          std::numeric_limits<int64_t>::max() ||
      column >= schema_.ColumnCount() ||
      schema_.GetColumn(column).Type() != ValueType::kInt64 ||
      schema_.GetColumn(column).IsUnsigned()) {
    return nullptr;
  }
  auto lease = AcquireLease(txn.ID());
  if (!lease.HasValue()) {
    return nullptr;
  }
  size_t pages = 0;
  for (const auto& morsel : morsels) {
    if (morsel.size() >
        (kCacheBytes / sizeof(std::pair<page_id_t, lsn_t>)) - pages) {
      return nullptr;
    }
    pages += morsel.size();
  }
  if ((pages * sizeof(std::pair<page_id_t, lsn_t>)) +
          (span * sizeof(uint64_t)) >
      kCacheBytes) {
    return nullptr;
  }
  struct Cache {
    std::weak_ptr<TableIncarnation> owner;
    uint64_t generation{};
    page_id_t head{};
    slot_t column{};
    int64_t minimum{};
    size_t span{};
    std::vector<std::pair<page_id_t, lsn_t>> stamps;
    std::shared_ptr<const IntegerKeyCounts> counts;
  };
  // One entry per caller thread. Replacing a table/domain drops the old
  // entry; no SQL text, dimension payload, or query result is retained.
  thread_local Cache cache;
  const uint64_t generation = LayoutGeneration();
  const bool matching =
      cache.counts && cache.owner.lock() == incarnation_state_ &&
      cache.generation == generation && cache.head == first_pid_ &&
      cache.column == column && cache.minimum == minimum && cache.span == span;
  if (cached_pages_only) {
    if (!matching) {
      return nullptr;
    }
    page_id_t expected = first_pid_;
    for (const auto& [pid, lsn] : cache.stamps) {
      if (pid != expected) {
        return nullptr;
      }
      auto page = txn.GetPageManager()->GetPage(pid, true);
      if (!page.HasValue() || page.Value().IsNull() ||
          page.Value()->Type() != PageType::kRowPage ||
          txn.HasPageVersionChains(pid)) {
        return nullptr;
      }
      if (page.Value()->PageLSN() != lsn) {
        cache.counts.reset();
        return nullptr;
      }
      expected = page.Value()->body.row_page.next_page_id_;
    }
    if (expected != 0 || generation != LayoutGeneration() || txn.IsFailed()) {
      return nullptr;
    }
    return cache.counts;
  }
  const bool hit = matching && cache.stamps.size() == pages;
  auto counts = hit ? nullptr : std::make_shared<IntegerKeyCounts>();
  if (counts) {
    counts->counts.assign(span, 0);
  }
  std::vector<std::pair<page_id_t, lsn_t>> stamps;
  if (!hit) {
    stamps.reserve(pages);
  }
  size_t index = 0;
  page_id_t expected_page = first_pid_;
  Row row;
  const std::vector<slot_t> projection{column};
  for (const auto& morsel : morsels) {
    for (page_id_t pid : morsel) {
      auto ref = txn.GetPageManager()->GetPage(pid, true);
      if (!ref.HasValue() || ref.Value().IsNull() ||
          ref.Value()->Type() != PageType::kRowPage ||
          txn.HasPageVersionChains(pid)) {
        return nullptr;
      }
      const Page& page = *ref.Value();
      if (pid != expected_page) {
        return nullptr;
      }
      expected_page = page.body.row_page.next_page_id_;
      if (hit) {
        if (cache.stamps[index++] != std::pair{pid, page.PageLSN()}) {
          cache.counts.reset();
          return nullptr;
        }
        continue;
      }
      stamps.emplace_back(pid, page.PageLSN());
      const RowPage& rows = page.body.row_page;
      for (slot_t slot = 0; slot < rows.RowMax(); ++slot) {
        const std::string_view image = rows.GetRow(slot);
        if (image.empty()) {
          continue;
        }
        const auto decoded = row.TryDeserializeProjected(
            image.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
            schema_, projection);
        if (!decoded.HasValue() || decoded.Value() > image.size() ||
            row.Count() != 1) {
          return nullptr;
        }
        ++counts->rows;
        if (row[0].IsNull()) {
          continue;
        }
        const uint64_t offset = static_cast<uint64_t>(row[0].value.int_value) -
                                static_cast<uint64_t>(minimum);
        if (offset < span) {
          ++counts->counts[offset];
        }
      }
    }
  }
  if (expected_page != 0 || generation == 0 ||
      generation != LayoutGeneration() || txn.IsFailed()) {
    return nullptr;
  }
  if (hit) {
    return cache.counts;
  }
  cache = {.owner = incarnation_state_,
           .generation = generation,
           .head = first_pid_,
           .column = column,
           .minimum = minimum,
           .span = span,
           .stamps = std::move(stamps),
           .counts = counts};
  return counts;
}

Iterator Table::BeginIndexScan(Transaction& txn, const Index& index,
                               const Value& begin, const Value& end,
                               bool ascending) const {
  return BeginIndexScan(
      txn, index,
      begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
      end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end}, ascending);
}

Iterator Table::BeginIndexScan(Transaction& txn, const Index& index,
                               const std::vector<Value>& begin_key,
                               const std::vector<Value>& end_key,
                               bool ascending) const {
  // IndexScanIterator's vector-key ctor is public and Iterator's adopting
  // ctor never throws.
  auto owned = std::make_unique<IndexScanIterator>(*this, index, txn, begin_key,
                                                   end_key, ascending);
  return Iterator(owned.release());
}

std::unordered_map<slot_t, size_t> Table::AvailableKeyIndex() const {
  std::unordered_map<slot_t, size_t> ret;
  for (size_t i = 0; i < indexes_.size(); ++i) {
    // Skip key-less indexes instead of reading key_[0] out of bounds.
    if (indexes_[i].sc_.key_.empty()) {
      continue;
    }
    ret.emplace(indexes_[i].sc_.key_[0], i);
  }
  return ret;
}

bool Table::operator==(const Table& rhs) const {
  return schema_ == rhs.schema_ && first_pid_ == rhs.first_pid_ &&
         last_pid_ == rhs.last_pid_ && incarnation_ == rhs.incarnation_ &&
         indexes_ == rhs.indexes_;
}

Encoder& operator<<(Encoder& e, const Table& t) {
  e << t.schema_ << t.first_pid_ << t.last_pid_ << t.indexes_ << t.incarnation_;
  return e;
}

std::ostream& operator<<(std::ostream& o, const Table& t) {
  o << "Table(schema=" << t.schema_ << ", first_pid=" << t.first_pid_
    << ", last_pid=" << t.last_pid_ << ", indexes=[";
  for (size_t i = 0; i < t.indexes_.size(); i++) {
    if (i != 0) {
      o << ", ";
    }
    o << t.indexes_[i];
  }
  o << "])";
  return o;
}

Decoder& operator>>(Decoder& d, Table& t) {
  d >> t.schema_ >> t.first_pid_ >> t.last_pid_ >> t.indexes_;
  t.incarnation_ = 0;
  if (!d.AtEnd()) {
    d >> t.incarnation_;
  }
  return d;
}

}  // namespace tinylamb
