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
#include <cstddef>
#include <cstring>
#include <optional>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/status_or.hpp"
#include "full_scan_iterator.hpp"
#include "index/b_plus_tree.hpp"
#include "index/index_scan_iterator.hpp"
#include "index/index_schema.hpp"
#include "iterator.hpp"
#include "page/page_manager.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"
#include "table/scan_options.hpp"
#include "common/log_message.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

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
  page_id_t root_pid = 0;
  {
    PageRef root_page =
        txn.GetPageManager()->AllocateNewPage(txn, PageType::kLeafPage);
    root_pid = root_page->PageID();
    indexes_.emplace_back(idx.name_, idx.key_, root_pid, idx.include_,
                          idx.mode_);
  }

  Iterator it = BeginFullScan(txn);
  while (it.IsValid()) {
    const Status status = IndexInsert(txn, indexes_.back(), *it, it.Position());
    if (status != Status::kSuccess) {
      // Drop the half-built index so later Insert/Delete stop touching it,
      // and recycle the root page onto the free list.
      indexes_.pop_back();
      PageRef root = txn.GetPageManager()->GetPage(root_pid);
      txn.GetPageManager()->DestroyPage(txn, root.get());
      return status;
    }
    ++it;
  }
  return Status::kSuccess;
}

namespace {
// Phase1-4: once the append point has less free space left than this, the
// next Insert call pre-allocates and links a fresh tail page so the
// allocation cost (and the page-full chain walk) leaves the hot path.
constexpr bin_size_t kTailPreallocateThreshold = kPageBodySize / 16;
}  // namespace

StatusOr<RowPosition> Table::Insert(Transaction& txn, const Row& row) {
  PageManager* const page_manager = txn.GetPageManager();
  const page_id_t cached_tail =
      page_manager->GetTableTail(first_pid_, last_pid_);
  PageRef ref = page_manager->GetPage(cached_tail);
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  StatusOr<slot_t> pos = ref->Insert(txn, serialized_row);
  RowPosition rp;
  if (pos.HasValue()) {
    rp.page_id = ref->PageID();
    rp.slot = pos.Value();
  } else if (pos.GetStatus() == Status::kNoSpace) {
    bool finished = false;
    while (ref->body.row_page.next_page_id_ != 0) {
      const page_id_t previous = ref->PageID();
      PageRef next =
          page_manager->GetPage(ref->body.row_page.next_page_id_);
      page_manager->AdvanceTableTail(first_pid_, previous, next->PageID());
      ref = std::move(next);
      StatusOr<slot_t> next_pos = ref->Insert(txn, serialized_row);
      if (next_pos.GetStatus() == Status::kSuccess) {
        rp.page_id = ref->PageID();
        rp.slot = next_pos.Value();
        finished = true;
        break;
      }
      if (next_pos.GetStatus() != Status::kNoSpace) {
        // Only a full page justifies walking the chain; anything else
        // (corruption, conflicts) must abort the insert.
        ref.PageUnlock();
        return next_pos.GetStatus();
      }
    }
    if (!finished) {
      PageRef new_page =
          txn.GetPageManager()->AllocateNewPage(txn, PageType::kRowPage);
      StatusOr<slot_t> new_pos = new_page->Insert(txn, serialized_row);
      if (!new_pos.HasValue()) {
        // The page is still unlinked, so recycle it instead of leaking it;
        // the error itself must propagate without touching the indexes.
        txn.GetPageManager()->DestroyPage(txn, new_page.get());
        return new_pos.GetStatus();
      }
      rp.page_id = new_page->PageID();
      rp.slot = new_pos.Value();
      ref->body.row_page.next_page_id_ = new_page->PageID();
      new_page->body.row_page.prev_page_id_ = ref->PageID();
      page_manager->AdvanceTableTail(first_pid_, ref->PageID(),
                                     new_page->PageID());
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
  if (ref->body.row_page.free_size_ < kTailPreallocateThreshold &&
      ref->body.row_page.next_page_id_ == 0) {
    PageRef successor =
        txn.GetPageManager()->AllocateNewPage(txn, PageType::kRowPage);
    successor->body.row_page.prev_page_id_ = ref->PageID();
    ref->body.row_page.next_page_id_ = successor->PageID();
    page_manager->AdvanceTableTail(first_pid_, ref->PageID(),
                                   successor->PageID());
  }
  ref.PageUnlock();
  for (size_t i = 0; i < indexes_.size(); ++i) {
    const Status status = IndexInsert(txn, indexes_[i], row, rp);
    if (status != Status::kSuccess) {
      // A rejected insert (e.g. a unique-key violation) must be atomic: undo
      // the index entries added before the failing one and remove the row
      // that was already written to the row page.
      for (size_t j = 0; j < i; ++j) {
        std::ignore = IndexDelete(txn, indexes_[j], rp);
      }
      PageRef written = txn.GetPageManager()->GetPage(rp.page_id);
      std::ignore = written->Delete(txn, rp.slot);
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
    case ValueType::kDate:
      return a.value.int_value != b.value.int_value;
    case ValueType::kVarChar:
      return a.value.varchar_value != b.value.varchar_value;
    case ValueType::kDouble:
      return std::memcmp(&a.value.double_value, &b.value.double_value,
                         sizeof(double)) != 0;
    case ValueType::kNull:
      return false;
  }
  return true;
}

// True when no column feeding this index (key or include) differs between
// the original and the updated row.
bool IndexCoversUnchanged(
    const Index& idx, const std::vector<slot_t>& changed_key_columns,
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
  // Phase2-3: the previous image feeds index maintenance only. Tables
  // without indexes skip the deserialize roundtrip entirely; RowPage::Update
  // still rejects absent/invisible slots with the same status Table::Read
  // would have produced. Read before RowPage::Update reserves the write
  // intent: an unstaged intent is deliberately invisible even to its owner.
  // The reservation still enforces first-updater-wins against any writer
  // that commits between this snapshot read and the physical update.
  Row original_row;
  if (!indexes_.empty()) {
    StatusOr<Row> visible = Read(txn, pos);
    RETURN_IF_FAIL(visible.GetStatus());
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
    changed_include_columns.erase(
        std::unique(changed_include_columns.begin(),
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
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  // Best-effort compensations: a failed index mutation must not leave applied
  // entries behind (old keys deleted / new keys partially inserted).
  const auto reinstate_old_keys = [&](size_t count, const RowPosition& at) {
    for (size_t j = 0; j < count; ++j) {
      std::ignore = IndexInsert(txn, indexes_[j], original_row, at,
                                &idx_cursors[j]);
    }
  };
  const auto undo_index_inserts = [&](size_t count) {
    for (size_t j = 0; j < count; ++j) {
      std::ignore = IndexDelete(txn, indexes_[j], new_pos, row,
                                &idx_cursors[j]);
    }
  };
  // Phase2-3: serialize the pre-update image once, on first use, instead of
  // rebuilding it for every compensation attempt.
  std::string original_image;
  const auto restore_physical_row = [&]() {
    if (original_image.empty()) {
      original_image.resize(original_row.Size());
      std::ignore = original_row.Serialize(original_image.data());
    }
    PageRef written = txn.GetPageManager()->GetPage(pos.page_id);
    std::ignore = written->Update(txn, pos.slot, original_image);
  };

  PageRef page = txn.GetPageManager()->GetPage(new_pos.page_id);
  Status s = page->Update(txn, new_pos.slot, serialized_row);
  if (s == Status::kSuccess && indexes_unchanged) {
    return new_pos;
  }
  if (s == Status::kSuccess) {
    Status failure = Status::kSuccess;
    size_t deleted = 0;
    for (; deleted < indexes_.size(); ++deleted) {
      failure = IndexDelete(txn, indexes_[deleted], pos, original_row,
                            &idx_cursors[deleted]);
      if (failure != Status::kSuccess) { break;
}
    }
    if (deleted < indexes_.size()) {
      // The physical row was rewritten in place: put back the deleted keys
      // and roll the row image back to the pre-update one.
      reinstate_old_keys(deleted, pos);
      restore_physical_row();
      return failure;
    }
    size_t inserted = 0;
    for (; inserted < indexes_.size(); ++inserted) {
      failure = IndexInsert(txn, indexes_[inserted], row, new_pos,
                            &idx_cursors[inserted]);
      if (failure != Status::kSuccess) { break;
}
    }
    if (inserted < indexes_.size()) {
      // Fully restore the pre-update state: undo the applied inserts,
      // reinstate every original key, and rewrite the original row image.
      undo_index_inserts(inserted);
      reinstate_old_keys(indexes_.size(), pos);
      restore_physical_row();
      return failure;
    }
    return new_pos;
  }
  if (s != Status::kNoSpace) { return s;
}
  Status failure = Status::kSuccess;
  size_t deleted = 0;
  for (; deleted < indexes_.size(); ++deleted) {
    failure = IndexDelete(txn, indexes_[deleted], pos, original_row,
                          &idx_cursors[deleted]);
    if (failure != Status::kSuccess) { break;
}
  }
  if (deleted < indexes_.size()) {
    // The physical row is untouched at pos; reinstating the keys suffices.
    reinstate_old_keys(deleted, pos);
    return failure;
  }
  const Status delete_status = page->Delete(txn, new_pos.slot);
  if (delete_status != Status::kSuccess) {
    // The old row image survives at pos; roll the index deletes back instead
    // of leaving "row present × all index entries gone".
    reinstate_old_keys(indexes_.size(), pos);
    return delete_status;
  }
  bool finished = false;
  while (page->body.row_page.next_page_id_ != 0) {
    page_id_t next_page = page->body.row_page.next_page_id_;
    page = txn.GetPageManager()->GetPage(next_page);
    StatusOr<slot_t> next_pos = page->Insert(txn, serialized_row);
    if (next_pos.HasValue()) {
      new_pos.page_id = page->PageID();
      new_pos.slot = next_pos.Value();
      finished = true;
      break;
    }
    if (next_pos.GetStatus() != Status::kNoSpace) {
      // A rejected insertion (lock conflict, corruption) must not be mistaken
      // for a full page: stop the walk and compensate below.
      reinstate_old_keys(indexes_.size(), pos);
      return next_pos.GetStatus();
    }
  }
  if (!finished) {
    PageRef new_page =
        txn.GetPageManager()->AllocateNewPage(txn, PageType::kRowPage);
    StatusOr<slot_t> new_slot = new_page->Insert(txn, serialized_row);
    if (!new_slot.HasValue()) {
      txn.GetPageManager()->DestroyPage(txn, new_page.get());
      reinstate_old_keys(indexes_.size(), pos);
      return new_slot.GetStatus();
    }
    RowPosition new_row_pos(new_page->PageID(), new_slot.Value());
    // Release the latch early: relinking below re-pins the tail page. PageUnlock is
    // idempotent, so the destructor stays safe.
    const page_id_t tail_page_id = page->PageID();
    page.PageUnlock();
    {
      PageRef last_page = txn.GetPageManager()->GetPage(tail_page_id);
      last_page->body.row_page.next_page_id_ = new_page->PageID();
      new_page->body.row_page.prev_page_id_ = last_page->PageID();
    }
    new_pos = new_row_pos;
    txn.GetPageManager()->AdvanceTableTail(first_pid_, tail_page_id,
                                           new_page->PageID());
  }
  size_t inserted = 0;
  for (; inserted < indexes_.size(); ++inserted) {
    failure = IndexInsert(txn, indexes_[inserted], row, new_pos,
                          &idx_cursors[inserted]);
    if (failure != Status::kSuccess) { break;
}
  }
  if (inserted < indexes_.size()) {
    // The row has already been relocated to new_pos, so the exact pre-update
    // state is unrecoverable; undo the partial inserts and repoint the
    // original keys at the relocated position (best effort).
    undo_index_inserts(inserted);
    reinstate_old_keys(indexes_.size(), new_pos);
    return failure;
  }
  return new_pos;
}

Status Table::Delete(Transaction& txn, RowPosition pos) {
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  Status failure = Status::kSuccess;
  size_t deleted = 0;
  for (; deleted < indexes_.size(); ++deleted) {
    failure = IndexDelete(txn, indexes_[deleted], pos, original_row);
    if (failure != Status::kSuccess) { break;
}
  }
  if (deleted < indexes_.size()) {
    // The row survives, so every applied index deletion must be undone:
    // "row present x some index entries gone" is not a legal state.
    for (size_t j = 0; j < deleted; ++j) {
      std::ignore = IndexInsert(txn, indexes_[j], original_row, pos);
    }
    return failure;
  }
  const Status delete_status =
      txn.GetPageManager()->GetPage(pos.page_id)->Delete(txn, pos.slot);
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
      std::ignore = IndexInsert(txn, indexes_[i - 1], original_row, pos);
    }
  }
  return delete_status;
}

StatusOr<Row> Table::Read(Transaction& txn, RowPosition pos) const {
  // Keep the PageRef alive while decoding: read_row points into the page
  // buffer, which is only guaranteed stable while the page stays pinned.
  // (The resulting Row owns its strings; Value::Deserialize copies varchar
  // data out of the buffer.)
  PageRef page = txn.GetPageManager()->GetPage(pos.page_id, true);
  ASSIGN_OR_RETURN(std::string_view, read_row, page->Read(txn, pos.slot));
  Row result;
  // Length-prefixed row image from a checksummed page: consumed by offsets,
  // never as a C string.
  result.Deserialize(
      read_row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
      schema_);
  return result;
}

Status Table::IndexInsert(Transaction& txn, const Index& idx,
                          const Row& new_row, const RowPosition& pos,
                          page_id_t* hint_leaf) const {
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
  if (idx.StoresSingleValue()) {
    // BTree value is encoded pair<RowPosition, vector<Value>>.
    IndexValueType val;
    val.pos = pos;
    val.include = include;
    RETURN_IF_FAIL(bpt.Insert(txn, key, Encode(val), &cursor));
  } else {
    // BTree value is encoded vector<pair<RowPosition, vector<Value>>>.
    StatusOr<std::string_view> existing_value = bpt.Read(txn, key, &cursor);
    if (existing_value.HasValue()) {
      std::string_view existing_data = existing_value.Value();
      auto rps = Decode<std::vector<IndexValueType>>(existing_data);
      if (idx.IsUnique()) {
        for (const IndexValueType& value : rps) {
          if (value.pos == pos) {
            // An UPDATE whose key did not change needs no second entry.  The
            // heap supplies the versioned row; index-only scans are not used
            // for this mode because INCLUDE values are not versioned here.
            if (hint_leaf != nullptr) { *hint_leaf = cursor; }
            return Status::kSuccess;
          }
          if (Read(txn, value.pos).HasValue()) {
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
  BPlusTree bpt(idx.pid_);
  const std::string key = idx.GenerateKey(original_row);
  page_id_t cursor = 0;
  if (idx.RetainsDeletedEntries()) {
    // Keep the key -> RowPosition edge.  RowPage MVCC decides whether the
    // pointed version is visible, which makes the same index usable by both
    // a pre-delete reader and a post-delete reader without a table scan.
    if (hint_leaf != nullptr) { *hint_leaf = cursor; }
    return Status::kSuccess;
  }
  if (idx.StoresSingleValue()) {
    RETURN_IF_FAIL(bpt.Delete(txn, key));
  } else {
    ASSIGN_OR_RETURN(std::string_view, existing_data, bpt.Read(txn, key, &cursor));
    auto values = Decode<std::vector<IndexValueType>>(existing_data);
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
  txn.MarkIndexKeysChanged(idx.Root());
  return Status::kSuccess;
}

Iterator Table::BeginFullScan(Transaction& txn,
                              const TableScanOptions& options) const {
  return Iterator(new FullScanIterator(
      this, &txn, options.projection, options.key_filter,
      options.key_column, options.peek_compares));
}

Iterator Table::BeginMorselScan(
    Transaction& txn, const ScanMorsel& pages,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column,
    const std::vector<IntegerPeekCompare>* peek_compares) const {
  return Iterator(new FullScanIterator(this, &txn, pages, std::move(projection),
                                       key_filter, key_column, peek_compares));
}

std::vector<Table::ScanMorsel> Table::BuildScanMorsels(
    Transaction& txn, size_t pages_per_morsel) const {
  pages_per_morsel = std::max<size_t>(1, pages_per_morsel);
  std::vector<ScanMorsel> morsels;
  page_id_t page_id = first_pid_;
  // Guard against a cyclic next_page_id_ chain (corrupt page / fuzzer input):
  // a revisited page terminates the walk instead of looping forever.
  std::unordered_set<page_id_t> visited;
  while (page_id != 0 && visited.insert(page_id).second) {
    if (morsels.empty() || morsels.back().size() == pages_per_morsel) {
      morsels.emplace_back();
      morsels.back().reserve(pages_per_morsel);
    }
    morsels.back().push_back(page_id);
    PageRef page = txn.GetPageManager()->GetPage(page_id, true);
    page_id = page->body.row_page.next_page_id_;
  }
  return morsels;
}

Iterator Table::BeginIndexScan(Transaction& txn, const Index& index,
                               const Value& begin, const Value& end,
                               bool ascending) const {
  return BeginIndexScan(txn, index,
                        begin.IsNull() ? std::vector<Value>{}
                                       : std::vector<Value>{begin},
                        end.IsNull() ? std::vector<Value>{}
                                     : std::vector<Value>{end},
                        ascending);
}

Iterator Table::BeginIndexScan(Transaction& txn, const Index& index,
                               const std::vector<Value>& begin_key,
                               const std::vector<Value>& end_key,
                               bool ascending) const {
  return Iterator(
      new IndexScanIterator(*this, index, txn, begin_key, end_key, ascending));
}

std::unordered_map<slot_t, size_t> Table::AvailableKeyIndex() const {
  std::unordered_map<slot_t, size_t> ret;
  for (size_t i = 0; i < indexes_.size(); ++i) {
    // Skip key-less indexes instead of reading key_[0] out of bounds.
    if (indexes_[i].sc_.key_.empty()) { continue;
}
    ret.emplace(indexes_[i].sc_.key_[0], i);
  }
  return ret;
}

Encoder& operator<<(Encoder& e, const Table& t) {
  e << t.schema_ << t.first_pid_ << t.last_pid_ << t.indexes_;
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
  return d;
}

}  // namespace tinylamb
