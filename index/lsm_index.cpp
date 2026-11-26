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

#include "index/lsm_index.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/page_lsm_store.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

// Up-front capacity for the lazy ascending scan's append-only buffer. Avoids a
// geometric series of reallocations/copies for scans that read a large range.
constexpr size_t kLazyScanInitialCapacity = 64;

void LsmIndexIterator::Clear() {
  valid_ = false;
  entries_.clear();
  emitted_.clear();
  scan_cursor_.reset();
  idx_ = 0;
  status_ = Status::kSuccess;
  stable_leaf_cache_ = false;
  // A cleared iterator must return to the same state as a default-constructed
  // one; leaving the tree/txn bound would let a later misuse read through a
  // stale transaction after the cursor was reset.
  tree_ = nullptr;
  txn_ = nullptr;
}

LsmIndexIterator::~LsmIndexIterator() = default;
LsmIndexIterator::LsmIndexIterator(LsmIndexIterator&&) noexcept = default;
LsmIndexIterator& LsmIndexIterator::operator=(LsmIndexIterator&&) noexcept =
    default;

LsmIndexIterator::LsmIndexIterator(LsmIndex* tree, Transaction* txn,
                                   std::string_view begin, std::string_view end,
                                   bool ascending)
    : tree_(tree), txn_(txn) {
  if (tree_ == nullptr || txn_ == nullptr ||
      txn_->GetPageManager() == nullptr) {
    valid_ = false;
    return;
  }
  PageManager* pm = txn_->GetPageManager();
  if (ascending) {
    // Lazy path: the cursor snapshots the tree once, then resolves and
    // yields entries one at a time, so consumers that stop early (LIMIT
    // pushdown) never pay to materialize the whole range.
    emitted_.reserve(kLazyScanInitialCapacity);
    scan_cursor_ = pm->OpenLsmScanCursor(tree_->Root(), begin, end, txn_);
    if (PullNext()) {
      valid_ = true;
      idx_ = 0;
      return;
    }
    valid_ = false;
    return;
  }
  // Descending scans start at the last entry, which requires knowing the
  // whole range anyway; keep the eager materialization for them.
  entries_ = pm->LsmScanRange(tree_->Root(), begin, end, true, txn_, &status_);
  // The range reader already resolved every value against this transaction's
  // snapshot. Re-reading the live tree from Value() would repeat an LSM lookup
  // for every materialized entry without improving visibility.
  stable_leaf_cache_ = true;
  if (entries_.empty()) {
    valid_ = false;
    return;
  }
  valid_ = true;
  idx_ = entries_.size() - 1;
}

const std::pair<std::string, std::string>* LsmIndexIterator::EntryAt(
    size_t i) const {
  if (scan_cursor_ != nullptr) {
    return i < emitted_.size() ? &emitted_[i] : nullptr;
  }
  return i < entries_.size() ? &entries_[i] : nullptr;
}

size_t LsmIndexIterator::EntryCount() const {
  return scan_cursor_ != nullptr ? emitted_.size() : entries_.size();
}

bool LsmIndexIterator::PullNext() {
  std::pair<std::string, std::string> entry;
  if (scan_cursor_ == nullptr || !scan_cursor_->Next(&entry)) {
    if (scan_cursor_ != nullptr) {
      status_ = scan_cursor_->status();
    }
    return false;
  }
  emitted_.push_back(std::move(entry));
  return true;
}

std::string LsmIndexIterator::Key() const {
  // A lazy cursor may have failed while previous entries are still buffered
  // (e.g. after operator--). Never hand back one of those stale keys as if it
  // were current -- Key() must agree with GetStatus()/Value(), so report no
  // key once the cursor has recorded an error.
  if (status_ != Status::kSuccess) {
    return {};
  }
  const auto* e = valid_ ? EntryAt(idx_) : nullptr;
  return e != nullptr ? e->first : std::string{};
}

std::string LsmIndexIterator::Value() const {
  // Mirror Key(): a lazy cursor may have failed while older entries are still
  // buffered. Returning one of those while Key() reports no key would hand
  // callers an inconsistent (empty key, non-empty value) entry and hide the
  // error, so once status_ is set both accessors report no data.
  if (status_ != Status::kSuccess) {
    return {};
  }
  const auto* e = valid_ ? EntryAt(idx_) : nullptr;
  if (e == nullptr) {
    return {};
  }
  if (!stable_leaf_cache_ && txn_ != nullptr && tree_ != nullptr &&
      txn_->GetPageManager() != nullptr) {
    StatusOr<std::string> live =
        txn_->GetPageManager()->LsmRead(tree_->Root(), e->first, txn_);
    if (live.HasValue()) {
      return live.MoveValue();
    }
    if (live.GetStatus() == Status::kNotExists) {
      // The key vanished between cursor creation and this live re-read. The
      // 8 all-zero bytes are the canonical encoding of an *empty* value list
      // (a u64 count of zero), not a tombstone with an opaque payload -- this
      // is exactly what a deleted entry's index image looks like. Consumers
      // (IndexScanIterator::UpdateIteratorState) recognise this encoding and
      // skip the stale entry instead of decoding it as a RowPosition.
      return {sizeof(uint64_t), '\0'};
    }
    // A real read failure (I/O error, corruption, ...) must reach GetStatus()
    // instead of silently falling through to the stale snapshot value.
    status_ = live.GetStatus();
    return {};
  }
  return e->second;
}

LsmIndexIterator& LsmIndexIterator::operator++() {
  // IsValid() also checks the sticky cursor status; do not advance past an
  // entry once an error has been recorded.
  if (!IsValid()) {
    return *this;
  }
  ++idx_;
  if (idx_ < EntryCount()) {
    // Eager mode always lands here inside entries_; lazy mode may also be
    // revisiting an already-pulled entry after operator--. The cursor can
    // fail while such buffered entries remain (PullNext has not been called
    // again yet), so surface that error now instead of letting GetStatus()
    // keep reporting a clean exhaustion.
    if (scan_cursor_ != nullptr && status_ == Status::kSuccess) {
      const Status cursor_status = scan_cursor_->status();
      if (cursor_status != Status::kSuccess) {
        status_ = cursor_status;
      }
    }
    return *this;
  }
  if (scan_cursor_ != nullptr && PullNext()) {
    // The pulled entry now sits at absolute position idx_.
    return *this;
  }
  valid_ = false;
  return *this;
}

LsmIndexIterator& LsmIndexIterator::operator--() {
  if (!IsValid()) {
    return *this;
  }
  if (idx_ == 0) {
    valid_ = false;
  } else {
    --idx_;
  }
  return *this;
}

bool LsmIndexIterator::SeekBefore(std::string_view key) {
  if (scan_cursor_ != nullptr || status_ != Status::kSuccess) {
    return false;
  }
  const auto it = std::lower_bound(
      entries_.begin(), entries_.end(), key,
      [](const auto& entry, std::string_view bound) {
        return entry.first < bound;
      });
  if (it == entries_.begin()) {
    valid_ = false;
    idx_ = 0;
    return false;
  }
  idx_ = static_cast<size_t>(it - entries_.begin() - 1);
  valid_ = true;
  return true;
}

std::ostream& operator<<(std::ostream& o, const LsmIndexIterator& it) {
  if (!it.IsValid()) {
    o << "<invalid LsmIndexIterator>";
  } else {
    o << "LsmIndexIterator(idx=" << it.idx_ << "/" << it.EntryCount() << ")";
  }
  return o;
}

StatusOr<LsmIndex> LsmIndex::Open(Transaction& txn, page_id_t root) {
  if (root == 0) {
    ASSIGN_OR_RETURN(
        PageRef, page,
        txn.GetPageManager()->AllocateNewPage(txn, PageType::kRowPage));
    root = page->PageID();
  }
  return LsmIndex(root);
}

Status LsmIndex::Insert(Transaction& txn, std::string_view key,
                        std::string_view value, page_id_t* hint_leaf) const {
  if (hint_leaf != nullptr) {
    *hint_leaf = root_;
  }
  return txn.GetPageManager()->LsmInsert(txn, root_, key, value);
}

Status LsmIndex::Update(Transaction& txn, std::string_view key,
                        std::string_view value, page_id_t* hint_leaf) const {
  if (hint_leaf != nullptr) {
    *hint_leaf = root_;
  }
  return txn.GetPageManager()->LsmUpdate(txn, root_, key, value);
}

Status LsmIndex::Delete(Transaction& txn, std::string_view key) const {
  return txn.GetPageManager()->LsmDelete(txn, root_, key);
}

StatusOr<std::string> LsmIndex::Read(Transaction& txn, std::string_view key,
                                     page_id_t* hint_leaf) const {
  if (hint_leaf != nullptr) {
    *hint_leaf = root_;
  }
  return txn.GetPageManager()->LsmRead(root_, key, &txn);
}

StatusOr<std::string> LsmIndex::ReadVisible(Transaction& txn,
                                            std::string_view key) const {
  Status status;
  auto entries =
      txn.GetPageManager()->LsmScanRange(root_, key, key, true, &txn, &status);
  RETURN_IF_FAIL(status);
  if (entries.empty() || entries.front().first != key) {
    return Status::kNotExists;
  }
  return std::move(entries.front().second);
}

LsmIndexIterator LsmIndex::Begin(Transaction& txn, std::string_view begin,
                                 std::string_view end, bool ascending) {
  return {this, &txn, begin, end, ascending};
}

StatusOr<std::vector<std::string>> LsmIndex::ScanPartitionBoundaries(
    Transaction& txn, std::string_view begin, std::string_view end,
    size_t leaves_per_partition) const {
  Status status;
  const std::vector<std::pair<std::string, std::string>> entries =
      txn.GetPageManager()->LsmScanRange(root_, begin, end, true, &txn,
                                         &status);
  RETURN_IF_FAIL(status);
  constexpr size_t kKeysPerVirtualLeaf = 128;
  const size_t step =
      std::max<size_t>(1, leaves_per_partition) * kKeysPerVirtualLeaf;
  std::vector<std::string> boundaries;
  // The loop visits every step-th entry and emits at most one boundary per
  // visited entry, so the tight bound is entries.size()/step (step >= 1).
  boundaries.reserve((entries.size() / step) + 1);
  for (size_t i = step; i < entries.size(); i += step) {
    if (begin.empty() || entries[i].first > begin) {
      if (boundaries.empty() || entries[i].first > boundaries.back()) {
        boundaries.push_back(entries[i].first);
      }
    }
  }
  return boundaries;
}

Status LsmIndex::Destroy(Transaction& txn) const {
  return txn.GetPageManager()->LsmDestroy(txn, root_);
}

void LsmIndex::Dump(std::ostream& o, int indent) const {
  o << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
    << "LsmIndex(root=" << root_ << ")";
}

}  // namespace tinylamb
