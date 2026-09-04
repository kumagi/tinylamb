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

#include "table/full_scan_iterator.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "iterator_base.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {

// Read-only MVCC fast path gate.
//
// Writer-side contract (see Page::Insert/Update/Delete in page/page.cpp):
// every COMPLETED page mutation stamps the page with SetPageLSN(txn.PrevLSN())
// -- the position of that operation's just-appended WAL record -- and logger
// positions only grow over time.  A writer whose mutation happens after this
// transaction's snapshot was taken therefore always stamps a PageLSN greater
// than the snapshot's commit timestamp: each committing transaction emits at
// least two log records before it can advance the commit counter, so log
// positions strictly outrun published commit-timestamp values, and any
// post-snapshot mutation forces PageLSN above the snapshot threshold.
//
// Consequently, when IsReadOnly() and PageLSN() <= SnapshotTimestamp() hold,
// no transaction that BEGAN after our snapshot can have modified the page:
// either its last modifier committed before our snapshot began (its published
// version is byte-identical to the physical image) or the physical image is
// exactly what Transaction::ReadVersion would hand back for this snapshot.
// The per-row ReadVersion cost (version-shard mutex + hash probe + cache copy)
// is then pure overhead, and the raw page bytes are deserialized directly.
// Pages stamped above the threshold fall back to ReadVersion as before.
// The shared page latch is held across the whole pass, so no writer can slip
// a modification (and a fresh PageLSN) underneath a decision already taken.
//
// Boundary: a writer that mutated the page while still uncommitted BEFORE
// this snapshot began keeps its low op-LSNs below any threshold that later
// unrelated commits push past it; such pages are not distinguished here and
// would be served physically. Closing that window needs writer-side support
// (page commit timestamps or copy-on-write -- tpch Phase1-2), not a reader
// heuristic; bulk loads commit before read-only scans in production flows,
// so they never hit it.
bool PhysicalReadEligible(const Transaction& txn, const Page& page) {
  return txn.IsReadOnly() && page.PageLSN() <= txn.SnapshotTimestamp();
}

}  // namespace

FullScanIterator::FullScanIterator(
    const Table* table, Transaction* txn,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column,
    const std::vector<IntegerPeekCompare>* peek_compares)
    : table_(table),
      txn_(txn),
      pos_(table_->first_pid_, 0),
      projection_(std::move(projection)),
      key_filter_(key_filter),
      key_column_(key_column),
      peek_compares_(peek_compares) {
  // Scans only read: always take a shared page latch, even for writable
  // transactions, so concurrent scan workers never convoy on exclusive
  // latches.  Row-level unpinning below keeps same-thread mutations safe.
  page_ = std::make_unique<PageRef>(
      txn->GetPageManager()->GetPage(pos_.page_id, /*shared=*/true));
  SeekVisibleRow();
}

FullScanIterator::FullScanIterator(
    const Table* table, Transaction* txn, std::vector<page_id_t> pages,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column,
    const std::vector<IntegerPeekCompare>* peek_compares)
    : table_(table),
      txn_(txn),
      pos_(pages.empty() ? ~0ULL : pages.front(), 0),
      projection_(std::move(projection)),
      pages_(std::move(pages)),
      key_filter_(key_filter),
      key_column_(key_column),
      peek_compares_(peek_compares) {
  if (!pos_.IsValid()) {
    return;
  }
  page_ = std::make_unique<PageRef>(
      txn->GetPageManager()->GetPage(pos_.page_id, /*shared=*/true));
  SeekVisibleRow();
}

IteratorBase& FullScanIterator::operator++() {
  if (!pos_.IsValid()) {
    return *this;
  }
  ++pos_.slot;
  if (page_ == nullptr) {
    page_ = std::make_unique<PageRef>(
        txn_->GetPageManager()->GetPage(pos_.page_id, /*shared=*/true));
  }
  SeekVisibleRow();
  return *this;
}

bool FullScanIterator::PassesPeekFilters(std::string_view row) const {
  if (peek_compares_ == nullptr || peek_compares_->empty()) {
    return true;
  }
  std::optional<slot_t> cached_column;
  std::optional<int64_t> cached_value;
  // The cache holds only the immediately preceding column: it pays off when
  // consecutive predicates target the same column.
  for (const IntegerPeekCompare& pred : *peek_compares_) {
    if (!cached_column || *cached_column != pred.column) {
      cached_column = pred.column;
      cached_value = Row::TryPeekInteger(
          row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
          table_->GetSchema(), pred.column);
    }
    if (!cached_value ||
        !MatchIntegerCompare(*cached_value, pred.op, pred.constant)) {
      return false;
    }
  }
  return true;
}

void FullScanIterator::SeekVisibleRow() {
  // Precondition: page_ is pinned (the constructors and operator++ re-pin it
  // after DropPageLatch or a per-row unpin before calling this method).
  while (pos_.IsValid()) {
    // One verdict per pinned page: the shared latch freezes PageLSN for the
    // whole pass, and a dropped latch (DropPageLatch / AdvancePage) re-enters
    // here, re-pins, and re-evaluates against the possibly newer stamp.
    const bool physical_read = PhysicalReadEligible(*txn_, **page_);
    while (pos_.slot < (*page_)->body.row_page.RowMax()) {
      std::optional<std::string_view> row;
      if (physical_read) {
        // Fast path: vacant slots (offset == 0, i.e. deleted or never
        // filled) are invisible to every snapshot that qualifies above --
        // any historical deletion would have stamped PageLSN above the
        // threshold and routed the page to ReadVersion, which is what still
        // resurrects rows for older snapshots.
        const RowPage& body = (*page_)->body.row_page;
        if (body.rows_[pos_.slot].offset != 0) {
          row = body.GetRow(pos_.slot);
        }
      } else {
        StatusOr<std::string_view> version = (*page_)->Read(*txn_, pos_.slot);
        if (version.HasValue()) {
          row = version.Value();
        }
      }
      if (row.has_value()) {
        if (key_filter_ != nullptr && key_column_.has_value()) {
          const auto key = Row::TryPeekInteger(
              row->data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
              table_->GetSchema(), *key_column_);
          if (!key || !key_filter_->contains(*key)) {
            ++pos_.slot;
            continue;
          }
        }
        if (!PassesPeekFilters(*row)) {
          ++pos_.slot;
          continue;
        }
        DeserializeCurrent(*row);
        // current_row_ is safe to use after the page is unpinned below:
        // Row::Deserialize copies varchar data into Value::owned_data, so no
        // decoded value keeps a view into the page buffer.
        // Morsel scans (pages_ set) are consumed by dedicated worker threads
        // that drop their latch before blocking, so they can hold the shared
        // page latch across rows.  Plain scans may feed a same-thread
        // mutation (e.g. a caller updating while iterating), so they unpin
        // per row to avoid self-deadlock on the page latch.
        if (!txn_->IsReadOnly() && !pages_) {
          page_.reset();
        }
        return;
      }
      ++pos_.slot;
    }
    if (!AdvancePage()) {
      break;
    }
  }
  pos_.page_id = ~0ULL;
  current_row_.Clear();
}

bool FullScanIterator::AdvancePage() {
  page_id_t next_page = 0;
  if (pages_) {
    ++page_index_;
    if (page_index_ < pages_->size()) {
      next_page = (*pages_)[page_index_];
    }
  } else {
    next_page = (*page_)->body.row_page.next_page_id_;
  }
  page_.reset();
  if (next_page == 0) {
    return false;
  }
  pos_ = RowPosition(next_page, 0);
  page_ = std::make_unique<PageRef>(
      txn_->GetPageManager()->GetPage(next_page, /*shared=*/true));
  return true;
}

void FullScanIterator::DeserializeCurrent(std::string_view row) {
  // Length-prefixed page images: decoded by offsets, never as C strings.
  if (projection_) {
    current_row_.DeserializeProjected(
        row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_->schema_, *projection_);
  } else {
    current_row_.Deserialize(
        row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_->schema_);
  }
}

IteratorBase& FullScanIterator::operator--() {
  // Backward iteration is not implemented; the interface keeps operator--
  // because IndexScanIterator supports it.  Fail loudly instead of silently
  // breaking the iteration contract in NDEBUG builds.
  throw std::logic_error("FullScanIterator does not support operator--");
}

void FullScanIterator::DropPageLatch() { page_.reset(); }

bool FullScanIterator::IsValid() const { return pos_.IsValid(); }

const Row& FullScanIterator::operator*() const { return current_row_; }
Row& FullScanIterator::operator*() { return current_row_; }

void FullScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->schema_.Name();
}

}  // namespace tinylamb
