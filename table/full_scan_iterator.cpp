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

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "common/constants.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "iterator_base.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {

#ifdef __x86_64__
// Four-cell prefix decode: serialization is big-endian, so each 8-byte cell
// needs its bytes reversed; a single pshufb with a lane-reversal mask does a
// whole 32-byte vector at once.  Page payloads are only 2-byte aligned, so
// both loads and stores are unaligned.
__attribute__((target("avx2"))) inline void BswapCellsAvx2(const char* src,
                                                           int64_t* dst) {
  const __m256i shuffle =
      _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7,
                       6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
  const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst),
                      _mm256_shuffle_epi8(v, shuffle));
}
#endif

// Decode `count` consecutive 8-byte big-endian cells into little-endian
// int64s.  Used by the no-null-bitmap constant-prefix decode, where the kept
// cells are contiguous on both sides.
inline void DecodePrefixCells(const char* src, int64_t* dst, size_t count) {
#ifdef __x86_64__
  static const bool kAvx2 = __builtin_cpu_supports("avx2");
  size_t k = 0;
  if (kAvx2) {
    for (; k + 4 <= count; k += 4) {
      BswapCellsAvx2(src + (k * 8), dst + k);
    }
  }
#else
  size_t k = 0;
#endif
  for (; k < count; ++k) {
    uint64_t bits = 0;
    std::memcpy(&bits, src + (k * 8), sizeof(bits));
    dst[k] = static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
  }
}

// Read-only MVCC fast path gate.
//
// The gate asks one question: is the raw page image the version this
// snapshot must see, for every row on the page? Two invariants answer it
// without comparing the page's WAL-byte PageLSN against the MVCC commit
// counter (units that only ever drift apart):
//
//  1. Write-before-latch: a mutator registers its write set -- creating a
//     version chain for the target row -- BEFORE it can touch page bytes,
//     and modifying the bytes needs the page's exclusive latch. A page with
//     no version chain therefore carries no in-flight or uncommitted-but-
//     staged mutation; its bytes are a committed state. This is the same
//     trust Transaction::ReadVersion's per-row fast path already places in
//     a chain-free row, checked here once per page instead of once per row.
//  2. GC drop discipline: a chain is erased only when no active snapshot
//     can need anything older than the latest committed version -- whose
//     bytes ARE the physical image (the committed write physically landed
//     before publish). After the drop, physical == latest == what every
//     active snapshot sees.
//
// Both facts are summarized by the exact per-page chain map
// (TransactionManager::HasPageVersionChains): absent chain entry => the
// physical image is authoritative for every row. The check runs under the
// shared page latch, and the verdict is recomputed on every re-pin, so a
// writer arriving mid-pass is caught at the next latch acquisition (its
// bytes cannot change while the shared latch is held, and pre-write bytes
// are exactly the snapshot-visible state).
//
// The previous PageLSN()-vs-SnapshotTimestamp() clause compared a WAL byte
// offset against a commit counter: sound only while log positions strictly
// outrun published commit-timestamp values, which fails for every page a
// bulk load touched (PageLSN in the hundreds of millions against a
// four-digit counter), silently disabling this fast path -- and the whole
// read path fell back to the per-row ReadVersion ladder.
bool PhysicalReadEligible(const Transaction& txn, const Page& page) {
  return txn.IsReadOnly() && !txn.HasPageVersionChains(page.PageID());
}

}  // namespace

FullScanIterator::FullScanIterator(
    const Table* table, Transaction* txn,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column,
    const std::vector<IntegerPeekCompare>* peek_compares, bool lock_rows,
    bool wait_for_write_intent, const RuntimeFilterGroup* runtime_filter,
    ValueType runtime_filter_type)
    : table_(table),
      txn_(txn),
      pos_(table_->first_pid_, 0),
      projection_(std::move(projection)),
      key_filter_(key_filter),
      key_column_(key_column),
      peek_compares_(peek_compares),
      runtime_filter_(
          AcceptsColumn(runtime_filter, key_column, runtime_filter_type)),
      runtime_filter_column_(runtime_filter != nullptr ? key_column
                                                       : std::nullopt),
      runtime_filter_type_(runtime_filter_type),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent) {
  StatusOr<TableLease> lease = table_->AcquireLease(txn_->ID());
  if (!lease.HasValue()) {
    status_ = lease.GetStatus();
    pos_.page_id = ~0ULL;
    return;
  }
  table_lease_ = lease.MoveValue();
  // Scans only read: always take a shared page latch, even for writable
  // transactions, so concurrent scan workers never convoy on exclusive
  // latches.  Row-level unpinning below keeps same-thread mutations safe.
  if (!PinPage(pos_.page_id)) {
    pos_.page_id = ~0ULL;
    return;
  }
  SeekVisibleRow();
}

FullScanIterator::FullScanIterator(
    const Table* table, Transaction* txn, std::vector<page_id_t> pages,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column,
    const std::vector<IntegerPeekCompare>* peek_compares,
    const RuntimeFilterGroup* runtime_filter, ValueType runtime_filter_type)
    : table_(table),
      txn_(txn),
      pos_(pages.empty() ? ~0ULL : pages.front(), 0),
      projection_(std::move(projection)),
      pages_(std::move(pages)),
      key_filter_(key_filter),
      key_column_(key_column),
      peek_compares_(peek_compares),
      runtime_filter_(
          AcceptsColumn(runtime_filter, key_column, runtime_filter_type)),
      runtime_filter_column_(runtime_filter != nullptr ? key_column
                                                       : std::nullopt),
      runtime_filter_type_(runtime_filter_type) {
  StatusOr<TableLease> lease = table_->AcquireLease(txn_->ID());
  if (!lease.HasValue()) {
    status_ = lease.GetStatus();
    pos_.page_id = ~0ULL;
    return;
  }
  table_lease_ = lease.MoveValue();
  if (!pos_.IsValid()) {
    return;
  }
  if (!PinPage(pos_.page_id)) {
    pos_.page_id = ~0ULL;
    return;
  }
  SeekVisibleRow();
}

IteratorBase& FullScanIterator::operator++() {
  if (!pos_.IsValid()) {
    return *this;
  }
  ++pos_.slot;
  if (page_ == nullptr && !PinPage(pos_.page_id)) {
    pos_.page_id = ~0ULL;
    return *this;
  }
  SeekVisibleRow();
  return *this;
}

FullScanIterator::PlannedPeek FullScanIterator::BuildPeek(slot_t column) const {
  PlannedPeek peek;
  peek.column = column;
  const Schema& schema = table_->GetSchema();
  if (column >= schema.ColumnCount()) {
    return peek;
  }
  const ValueType type = schema.GetColumn(column).Type();
  if (type != ValueType::kInt64 && type != ValueType::kDate) {
    return peek;
  }
  // Constant offsets hold only while every earlier column serializes as a
  // fixed 8-byte payload (i.e. the column sits in the leading prefix).
  for (slot_t i = 0; i < column; ++i) {
    if (FixedSerializedSize(schema.GetColumn(i).Type()) != sizeof(uint64_t)) {
      return peek;
    }
  }
  peek.valid = true;
  peek.stored_count = schema.ColumnCount();
  peek.offset = static_cast<uint32_t>(sizeof(uint64_t)) * column;
  return peek;
}

FullScanIterator::PeekOutcome FullScanIterator::PeekIntegerPlanned(
    const PlannedPeek& peek, const char* row) {
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count = 0;
  DeserializeSlot(row, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const auto count = static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
  if (count != peek.stored_count) {
    return {.kind = PeekOutcome::Kind::kNull,
            .value = 0};  // corrupt count: reject the row
  }
  if (has_null) {
    // A bitmap row may NULL out prefix columns, whose payload then shifts
    // every constant offset — take the walked path for those rows.
    return {.kind = PeekOutcome::Kind::kFallback, .value = 0};
  }
  int64_t key = 0;
  DeserializeInteger(row + sizeof(slot_t) + peek.offset, &key);
  return {.kind = PeekOutcome::Kind::kValue, .value = key};
}

bool FullScanIterator::PassesPeekFilters(std::string_view row) const {
  if (peek_compares_ == nullptr || peek_compares_->empty()) {
    return true;
  }
  if (!peek_plan_built_) {
    for (const IntegerPeekCompare& pred : *peek_compares_) {
      peek_plan_.push_back(BuildPeek(pred.column));
    }
    peek_plan_built_ = true;
  }
  std::optional<slot_t> cached_column;
  std::optional<int64_t> cached_value;
  // The cache holds only the immediately preceding column: it pays off when
  // consecutive predicates target the same column.
  for (size_t k = 0; k < peek_compares_->size(); ++k) {
    const IntegerPeekCompare& pred = (*peek_compares_)[k];
    if (!cached_column || *cached_column != pred.column) {
      cached_column = pred.column;
      const PlannedPeek& peek = peek_plan_[k];
      if (peek.valid) {
        const PeekOutcome outcome = PeekIntegerPlanned(
            peek,
            row.data());  // NOLINT(bugprone-suspicious-stringview-data-usage)
        switch (outcome.kind) {
          case PeekOutcome::Kind::kValue:
            cached_value = outcome.value;
            break;
          case PeekOutcome::Kind::kNull:
            cached_value = std::nullopt;
            break;
          case PeekOutcome::Kind::kFallback:
            cached_value = Row::TryPeekInteger(
                row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                table_->GetSchema(), pred.column);
            break;
        }
      } else {
        cached_value = Row::TryPeekInteger(
            row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
            table_->GetSchema(), pred.column);
      }
    }
    if (!cached_value ||
        !MatchIntegerCompare(*cached_value, pred.op, pred.constant)) {
      return false;
    }
  }
  return true;
}

std::optional<std::string_view> FullScanIterator::NextVisibleImage() {
  // Precondition: page_ is pinned (the constructors and operator++ re-pin it
  // after DropPageLatch or a per-row unpin before calling this method).
  while (pos_.IsValid()) {
    // One verdict per pinned page: the shared latch freezes PageLSN for the
    // whole pass, and a dropped latch (DropPageLatch / AdvancePage) re-enters
    // here, re-pins, and re-evaluates against the possibly newer stamp.
    const bool physical_read = PhysicalReadEligible(*txn_, **page_);
    // The slot table is page-stable for the whole inner pass (page_ only
    // changes across AdvancePage, i.e. after this loop exits), so hold the
    // body reference instead of re-walking the PageRef chain per row.
    const RowPage& body = (*page_)->body.row_page;
    while (pos_.slot < body.RowMax()) {
      std::optional<std::string_view> row;
      const bool already_reserved = lock_rows_ && txn_->HasWriteSet(pos_);
      if (lock_rows_) {
        // DML source scan: pin the position's head version with a write
        // intent BEFORE resolving the row, so the image handed to the
        // predicate is the newest committed version -- the same contract
        // IndexScan's Position/AddWriteSet/ResolveRow sequence provides.
        // Vacant slots cannot carry a live head row, so they are skipped
        // without spending an intent.
        const RowPage& body = (*page_)->body.row_page;
        if (body.rows_[pos_.slot].offset == 0) {
          ++pos_.slot;
          continue;
        }
        const bool locked = wait_for_write_intent_ ? txn_->AddWriteSet(pos_)
                                                   : txn_->TryAddWriteSet(pos_);
        if (!locked) {
          // Undo the intents this scan acquired so a failed statement
          // leaves no residual locks behind for other transactions.
          for (const RowPosition& held : scan_acquired_) {
            txn_->ReleaseWriteIntent(held);
          }
          scan_acquired_.clear();
          status_ = Status::kConflicts;
          pos_.page_id = ~0ULL;
          return std::nullopt;
        }
        StatusOr<std::string_view> version = (*page_)->Read(*txn_, pos_.slot);
        if (!version.HasValue()) {
          if (!already_reserved) {
            txn_->ReleaseWriteIntent(pos_);
          }
          if (version.GetStatus() != Status::kNotExists) {
            for (const RowPosition& held : scan_acquired_) {
              txn_->ReleaseWriteIntent(held);
            }
            scan_acquired_.clear();
            status_ = version.GetStatus();
            pos_.page_id = ~0ULL;
            return std::nullopt;
          }
          ++pos_.slot;
          continue;
        }
        row = version.Value();
      } else if (physical_read) {
        // Fast path: vacant slots (offset == 0, i.e. deleted or never
        // filled) are invisible to every snapshot that qualifies above --
        // any historical deletion would have stamped PageLSN above the
        // threshold and routed the page to ReadVersion, which is what still
        // resurrects rows for older snapshots.
        // Sequential scans see dense pages: filled slots dominate.
        if (body.rows_[pos_.slot].offset != 0) [[likely]] {
          if (pos_.slot + 4 < body.RowMax()) {
            __builtin_prefetch(&body.rows_[pos_.slot + 4], 0, 1);
            // Warm the row IMAGE too: the descriptor prefetch above only
            // covers 16 bytes of slot table, while the decode below walks
            // 100-200 bytes of payload a few slots later in the page.
            const RowPointer& ahead = body.rows_[pos_.slot + 4];
            if (ahead.offset != 0) {
              __builtin_prefetch(
                  reinterpret_cast<const char*>(&body) + ahead.offset, 0, 1);
            }
          }
          row = body.GetRow(pos_.slot);
        }
      } else {
        // Pure snapshot read (resolve_head=false): an unstaged write intent
        // this transaction happens to hold on the position -- e.g. left over
        // from an earlier DML source scan -- must not upgrade this read to
        // the newest committed version.
        StatusOr<std::string_view> version =
            (*page_)->Read(*txn_, pos_.slot, /*resolve_head=*/false);
        if (version.HasValue()) {
          row = version.Value();
        } else if (version.GetStatus() != Status::kNotExists) {
          for (const RowPosition& held : scan_acquired_) {
            txn_->ReleaseWriteIntent(held);
          }
          scan_acquired_.clear();
          status_ = version.GetStatus();
          pos_.page_id = ~0ULL;
          return std::nullopt;
        }
      }
      if (row.has_value()) {
        // Pre-decode key rejection: the legacy exact set and the dynamic
        // (min/max / Bloom) group both answer a single int64 question, so they
        // share one peek.  TryPeekInteger yields nullopt for NULL and for any
        // non INT64/DATE column, both of which an equi-join key filter rejects
        // correctly.
        if ((runtime_filter_ != nullptr || key_filter_ != nullptr) &&
            key_column_.has_value()) {
          if (!key_peek_built_) {
            key_peek_plan_ = BuildPeek(*key_column_);
            key_peek_built_ = true;
          }
          std::optional<int64_t> key;
          if (key_peek_plan_.valid) {
            const PeekOutcome outcome = PeekIntegerPlanned(
                key_peek_plan_,
                row->data());  // NOLINT(bugprone-suspicious-stringview-data-usage)
            switch (outcome.kind) {
              case PeekOutcome::Kind::kValue:
                key = outcome.value;
                break;
              case PeekOutcome::Kind::kNull:
                key = std::nullopt;
                break;
              case PeekOutcome::Kind::kFallback:
                key = Row::TryPeekInteger(
                    row->data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                    table_->GetSchema(), *key_column_);
                break;
            }
          } else {
            key = Row::TryPeekInteger(
                row->data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                table_->GetSchema(), *key_column_);
          }
          const bool rejected = !key || (runtime_filter_ != nullptr
                                             ? runtime_filter_->Reject(*key)
                                             : !key_filter_->contains(*key));
          if (rejected) {
            if (lock_rows_ && !already_reserved) {
              txn_->ReleaseWriteIntent(pos_);
            }
            ++pos_.slot;
            continue;
          }
        }
        if (!PassesPeekFilters(*row)) {
          if (lock_rows_ && !already_reserved) {
            txn_->ReleaseWriteIntent(pos_);
          }
          ++pos_.slot;
          continue;
        }
        if (lock_rows_ && !already_reserved) {
          scan_acquired_.push_back(pos_);
        }
        // The returned image lives in the pinned page: typed-batch consumers
        // decode it before dropping the latch, and the Row consumer below
        // copies varchar bytes out (see DeserializeCurrent).  Morsel scans
        // (pages_ set) are consumed by dedicated worker threads that drop
        // their latch before blocking, so they can hold the shared page latch
        // across rows.  Plain scans may feed a same-thread mutation (e.g. a
        // caller updating while iterating), so they unpin per row to avoid
        // self-deadlock on the page latch.
        if (!txn_->IsReadOnly() && !pages_) {
          page_.reset();
        }
        // Iterator contract: pos_ stays ON the returned row; operator++ (or
        // DecodeNextTypedBatch) advances past it before the next seek.
        return row;
      }
      ++pos_.slot;
    }
    if (!AdvancePage()) {
      break;
    }
  }
  pos_.page_id = ~0ULL;
  return std::nullopt;
}

void FullScanIterator::SeekVisibleRow() {
  std::optional<std::string_view> row = NextVisibleImage();
  if (!row.has_value()) {
    current_row_.Clear();
    return;
  }
  DeserializeCurrent(*row);
}

bool FullScanIterator::AdvancePage() {
  if (page_ == nullptr || (*page_)->Type() != PageType::kRowPage) {
    status_ = Status::kCorrupt;
    return false;
  }
  page_id_t next_page = 0;
  if (pages_) {
    ++page_index_;
    if (page_index_ < pages_->size()) {
      next_page = (*pages_)[page_index_];
    }
  } else {
    for (;;) {
      const page_id_t predecessor = (*page_)->PageID();
      next_page = (*page_)->body.row_page.next_page_id_;
      if (next_page == 0) {
        page_.reset();
        return false;
      }
      const Status visible = txn_->GetPageManager()->CheckTableTailPage(
          txn_->ID(), table_->first_pid_, next_page);
      if (visible == Status::kSuccess) {
        break;
      }
      page_.reset();
      const Status waited = txn_->GetPageManager()->WaitForTableTailPage(
          *txn_, table_->first_pid_, next_page);
      if (waited != Status::kSuccess) {
        status_ = waited;
        return false;
      }
      StatusOr<PageRef> retry =
          txn_->GetPageManager()->GetPage(predecessor, /*shared=*/true);
      if (!retry.HasValue()) {
        status_ = retry.GetStatus();
        return false;
      }
      if (retry.Value().IsNull() ||
          retry.Value()->Type() != PageType::kRowPage) {
        status_ = StatusError(StatusCode::kCorrupt,
                              "full scan lost its row-page predecessor");
        return false;
      }
      page_ = std::make_unique<PageRef>(retry.MoveValue());
    }
  }
  if (next_page == 0) {
    return false;
  }
  pos_ = RowPosition(next_page, 0);
  return PinPage(next_page);
}

bool FullScanIterator::PinPage(page_id_t page_id) {
  const Status visible = txn_->GetPageManager()->CheckTableTailPage(
      txn_->ID(), table_->first_pid_, page_id);
  if (visible != Status::kSuccess) {
    status_ = visible;
    page_.reset();
    return false;
  }
  StatusOr<PageRef> ref =
      txn_->GetPageManager()->GetPage(page_id, /*shared=*/true);
  if (!ref.HasValue()) {
    status_ = ref.GetStatus();
    page_.reset();
    return false;
  }
  if (ref.Value().IsNull() || ref.Value()->Type() != PageType::kRowPage) {
    status_ =
        StatusError(StatusCode::kCorrupt, "full scan reached a non-row page: " +
                                              std::to_string(page_id));
    page_.reset();
    return false;
  }
  page_ = std::make_unique<PageRef>(ref.MoveValue());
  return true;
}

const RowDecodePlan* FullScanIterator::RowPlan() {
  if (row_plan_failed_ || !projection_.has_value()) {
    return nullptr;
  }
  if (row_plan_.has_value()) {
    return &*row_plan_;
  }
  const Schema& schema = table_->schema_;
  const std::vector<slot_t>& kept = *projection_;
  // Every kept column must be in range and strictly ascending; the single-
  // element case in particular used to skip the range check and let
  // stored_slot[kept[0]] = 0 write out of bounds.
  for (size_t k = 0; k < kept.size(); ++k) {
    if (kept[k] >= schema.ColumnCount() || (k > 0 && kept[k - 1] >= kept[k])) {
      // Same malformed-projection contract as TryDeserializeProjected: fall
      // back to the legacy per-row path for every row of this scan.
      row_plan_failed_ = true;
      return nullptr;
    }
  }
  RowDecodePlan plan;
  plan.stored_count = schema.ColumnCount();
  plan.stored_types.reserve(schema.ColumnCount());
  plan.stored_slot.assign(schema.ColumnCount(), RowDecodePlan::kNoSlot);
  bool all_fixed = true;
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    plan.stored_types.push_back(schema.GetColumn(i).Type());
    if (FixedSerializedSize(schema.GetColumn(i).Type()) == 0) {
      all_fixed = false;
    }
  }
  plan.all_fixed = all_fixed;
  plan.kept_count = kept.size();
  plan.slot_unsigned.assign(kept.size(), 0);
  bool has_unsigned = false;
  for (size_t k = 0; k < kept.size(); ++k) {
    plan.stored_slot[kept[k]] = k;
    plan.slot_unsigned[k] =
        schema.GetColumn(kept[k]).IsUnsigned() &&
                plan.stored_types[kept[k]] == ValueType::kInt64
            ? uint8_t{1}
            : uint8_t{0};
    has_unsigned = has_unsigned || plan.slot_unsigned[k] != 0;
  }
  plan.has_unsigned = has_unsigned;
  row_plan_.emplace(std::move(plan));
  return &*row_plan_;
}

void FullScanIterator::DeserializeCurrent(std::string_view row) {
  // Length-prefixed page images: decoded by offsets, never as C strings.
  const std::vector<slot_t>* const kept =
      projection_.has_value() ? &*projection_ : nullptr;
  if (kept != nullptr) {
    if (const RowDecodePlan* plan = RowPlan()) {
      current_row_.DeserializePlanned(
          row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
          *plan);
    } else {
      current_row_.DeserializeProjected(
          row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
          table_->schema_, *kept);
    }
  } else {
    current_row_.Deserialize(
        row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_->schema_);
  }
}

void FullScanIterator::BuildTypedDecodePlan() {
  typed_slot_types_.clear();
  typed_slot_unsigned_.clear();
  typed_keep_slots_.clear();
  typed_varchar_slot_.clear();
  typed_const_prefix_ = 0;
  typed_const_prefix_end_ = 0;
  const Schema& schema = table_->schema_;
  const std::vector<slot_t>* kept =
      projection_.has_value() ? &*projection_ : nullptr;
  size_t kept_cursor = 0;
  size_t varchar_ordinal = 0;
  bool variable_seen = false;
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const bool keep = kept == nullptr ||
                      (kept_cursor < kept->size() && (*kept)[kept_cursor] == i);
    const ValueType type = schema.GetColumn(i).Type();
    if (type != ValueType::kInt64 && type != ValueType::kDate &&
        type != ValueType::kDouble && type != ValueType::kVarChar) {
      // ARRAY (and any future variable-shape type) has no cell payload; the
      // caller falls back to the Row loop.
      typed_keep_slots_.clear();
      return;
    }
    if (keep) {
      typed_keep_slots_.push_back(i);
      typed_slot_types_.push_back(type);
      typed_slot_unsigned_.push_back(schema.GetColumn(i).IsUnsigned() &&
                                     type == ValueType::kInt64);
      typed_varchar_slot_.push_back(
          type == ValueType::kVarChar ? varchar_ordinal : SIZE_MAX);
      if (type == ValueType::kVarChar) {
        ++varchar_ordinal;
      }
      ++kept_cursor;
    }
    // The constant-offset prefix runs until the first VARCHAR payload: every
    // earlier stored column (kept or skipped) serializes as exactly 8 bytes,
    // so kept slots inside it read at schema-constant image offsets.
    if (type == ValueType::kVarChar) {
      variable_seen = true;
    } else if (!variable_seen) {
      typed_const_prefix_ = i + 1;
    }
  }
  if (typed_const_prefix_ != 0) {
    typed_const_prefix_end_ =
        sizeof(uint64_t) * typed_const_prefix_;  // payload-relative
  }
  // Prefix fast-path bookkeeping: kept slots are ascending, so the ones
  // inside the constant prefix are a leading run of typed_keep_slots_.
  typed_prefix_kept_ = 0;
  while (typed_prefix_kept_ < typed_keep_slots_.size() &&
         typed_keep_slots_[typed_prefix_kept_] < typed_const_prefix_) {
    ++typed_prefix_kept_;
  }
  typed_prefix_all_kept_ = !typed_keep_slots_.empty() &&
                           typed_prefix_kept_ == typed_keep_slots_.size();
  typed_prefix_identity_ = true;
  for (size_t k = 0; k < typed_prefix_kept_; ++k) {
    if (typed_keep_slots_[k] != static_cast<slot_t>(k)) {
      typed_prefix_identity_ = false;
      break;
    }
  }
}

size_t FullScanIterator::DecodeNextTypedBatch(TypedCellBatch* batch,
                                              size_t max_rows) {
  if (typed_keep_slots_.empty() && typed_slot_types_.empty()) {
    BuildTypedDecodePlan();
    if (typed_keep_slots_.empty()) {
      return 0;  // Schema carries a type the cell layout cannot hold.
    }
  }
  if (batch->slot_types.size() != typed_slot_types_.size() ||
      batch->slot_unsigned != typed_slot_unsigned_ ||
      !std::equal(batch->slot_types.begin(), batch->slot_types.end(),
                  typed_slot_types_.begin())) {
    batch->Configure(typed_slot_types_.size(), typed_slot_types_,
                     typed_slot_unsigned_);
  }
  batch->Reset();
  // Sticky full-batch sizing: cells/nulls stay at max_rows*stride across
  // calls so the per-row append below writes in place instead of paying a
  // value-initializing resize per row.  Cells under a NULL or VARCHAR slot
  // are never observable (every reader null-checks first or reads the
  // varchar arenas), so stale bytes are harmless; the per-row null memset
  // below resets the one region decode only ever sets.
  {
    const size_t needed = max_rows * batch->stride;
    if (batch->cells.size() < needed) {
      batch->cells.resize(needed);
      batch->nulls.resize(needed);
    }
    for (auto& ends : batch->varchar_ends) {
      ends.reserve(max_rows + 1);
    }
  }
  if (!pos_.IsValid()) {
    return 0;
  }
  const Schema& schema = table_->schema_;
  size_t count = 0;
  while (count < max_rows) {
    std::optional<std::string_view> row = NextVisibleImage();
    if (!row.has_value()) {
      break;
    }
    const size_t index = count;
    // The batch's cells/nulls are sticky-sized (max_rows*stride); this row
    // writes its slot region in place.  Only the null flags need a reset:
    // decode below only SETS bytes for NULL cells, and non-null slots rely
    // on zero.
    const size_t base = index * batch->stride;
    std::memset(batch->nulls.data() + base, 0, batch->stride);
    for (auto& ends : batch->varchar_ends) {
      ends.push_back(static_cast<uint32_t>(ends.empty() ? 0 : ends.back()));
    }
    const char* const row_base =
        row->data();  // NOLINT(bugprone-suspicious-stringview-data-usage)
    const char* cursor = row_base;
    constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
    slot_t encoded_count = 0;
    cursor += DeserializeSlot(cursor, &encoded_count);
    const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
    const auto stored_count =
        static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
    if (stored_count != schema.ColumnCount()) {
      status_ =
          StatusError(StatusCode::kCorrupt, "row image column count mismatch");
      pos_.page_id = ~0ULL;
      batch->rows = count;
      return count;
    }
    const char* bitmap = nullptr;
    if (has_null) {
      bitmap = cursor;
      cursor += (stored_count + 7) / 8;
    }
    // Leading fixed-width prefix: for rows WITHOUT a null bitmap, kept slots
    // load at payload-relative constant offsets (independent loads instead of
    // a serial cursor chain) and skipped prefix columns cost nothing at all.
    // Rows carrying a bitmap decode through the cursor: a NULL column
    // contributes no payload, so constant offsets would shift for it.
    const bool const_prefix = typed_const_prefix_ != 0 && !has_null;
    const char* payload = cursor;
    size_t kept_cursor = 0;
    slot_t i = 0;
    if (const_prefix) {
      // Tight prefix decode: the kept slots inside the prefix are a leading
      // run of the plan with schema-constant offsets, so they decode without
      // the column loop's per-column keep/skip branch work.
      if (typed_prefix_identity_) {
        DecodePrefixCells(payload, batch->cells.data() + base,
                          typed_prefix_kept_);
        kept_cursor = typed_prefix_kept_;
      } else {
        for (; kept_cursor < typed_prefix_kept_; ++kept_cursor) {
          uint64_t bits = 0;
          std::memcpy(
              &bits,
              payload + (sizeof(uint64_t) * typed_keep_slots_[kept_cursor]),
              sizeof(bits));
          batch->cells[base + kept_cursor] =
              static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
        }
      }
      cursor = payload + typed_const_prefix_end_;
      i = typed_const_prefix_;
    }
    for (; i < stored_count && i < schema.ColumnCount(); ++i) {
      const bool is_null =
          has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
      const bool keep = kept_cursor < typed_keep_slots_.size() &&
                        typed_keep_slots_[kept_cursor] == i;
      if (is_null) {
        if (keep) {
          batch->nulls[base + kept_cursor] = 1;
          if (typed_varchar_slot_[kept_cursor] != SIZE_MAX) {
            const size_t varchar_slot = typed_varchar_slot_[kept_cursor];
            batch->varchar_ends[varchar_slot][index] = static_cast<uint32_t>(
                batch->varchar_bytes[varchar_slot].size());
          } else {
            batch->cells[base + kept_cursor] = 0;  // keep unread cells zeroed
          }
          ++kept_cursor;
          continue;
        }
        continue;  // NULL serializes no payload; skipping is the decode.
      }
      if (!keep) {
        if (const_prefix && i < typed_const_prefix_) {
          continue;  // fixed-width skip = no cursor movement to account for
        }
        const ValueType type = schema.GetColumn(i).Type();
        Status skip_error = Status::kSuccess;
        const size_t skipped =
            Value::SkipSerializedInto(cursor, type, &skip_error);
        if (skip_error != Status::kSuccess) [[unlikely]] {
          status_ = skip_error;
          pos_.page_id = ~0ULL;
          return count;
        }
        cursor += skipped;
        continue;
      }
      if (const_prefix && i < typed_const_prefix_) {
        uint64_t bits = 0;
        std::memcpy(
            &bits,
            payload + (sizeof(uint64_t) * typed_keep_slots_[kept_cursor]),
            sizeof(bits));
        batch->cells[base + kept_cursor] =
            static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
        ++kept_cursor;
        continue;
      }
      const ValueType type = schema.GetColumn(i).Type();
      switch (type) {
        case ValueType::kInt64:
        case ValueType::kDate:
        case ValueType::kDouble: {
          uint64_t bits = 0;
          std::memcpy(&bits, cursor, sizeof(bits));
          batch->cells[base + kept_cursor] =
              static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
          cursor += sizeof(bits);
          break;
        }
        case ValueType::kVarChar: {
          bin_size_t length = 0;
          DeserializeU16(cursor, &length);
          cursor += sizeof(length);
          const size_t varchar_slot = typed_varchar_slot_[kept_cursor];
          auto& bytes = batch->varchar_bytes[varchar_slot];
          bytes.insert(bytes.end(), cursor, cursor + length);
          cursor += length;
          batch->varchar_ends[varchar_slot][index] =
              static_cast<uint32_t>(bytes.size());
          break;
        }
        default:
          status_ = StatusError(StatusCode::kCorrupt,
                                "typed batch decode met an unplanned type");
          pos_.page_id = ~0ULL;
          return count;
      }
      ++kept_cursor;
    }
    ++count;
    // Iterator contract: NextVisibleImage left pos_ on the decoded row;
    // advance past it (operator++'s movement without the Row decode).
    ++pos_.slot;
    if (page_ == nullptr && pos_.IsValid() && !PinPage(pos_.page_id)) {
      break;
    }
  }
  batch->rows = count;
  return count;
}

IteratorBase& FullScanIterator::operator--() {
  // Backward iteration is not implemented; the interface keeps operator--
  // because IndexScanIterator supports it.  Fail loudly instead of silently
  // breaking the iteration contract in NDEBUG builds.
  CHECK_MSG(false, "FullScanIterator does not support operator--");
  return *this;
}

void FullScanIterator::DropPageLatch() { page_.reset(); }

const RuntimeFilterGroup* FullScanIterator::AcceptsColumn(
    const RuntimeFilterGroup* group, std::optional<slot_t> column,
    ValueType type) const {
  if (group == nullptr || !column.has_value()) {
    return nullptr;
  }
  const Schema& schema = table_->GetSchema();
  if (*column >= schema.ColumnCount()) {
    return nullptr;
  }
  const ValueType declared = schema.GetColumn(*column).Type();
  if (declared != ValueType::kInt64 && declared != ValueType::kDate) {
    return nullptr;
  }
  // A group that rejects unconditionally compares no values, so the build
  // side's type (unknown when it had no comparable key) cannot mismatch.
  if (type != ValueType::kNull && declared != type && !group->RejectsAll()) {
    return nullptr;
  }
  return group;
}

void FullScanIterator::SetRuntimeFilter(const RuntimeFilterGroup* group,
                                        slot_t column, ValueType type) {
  const RuntimeFilterGroup* accepted =
      AcceptsColumn(group, std::optional<slot_t>(column), type);
  runtime_filter_ = accepted;
  runtime_filter_type_ = type;
  runtime_filter_column_ =
      accepted != nullptr ? std::optional<slot_t>(column) : std::nullopt;
  // The pre-decode peek reads its column from key_column_, so a filter
  // installed after construction has to publish the column there too. A
  // legacy exact-set filter on the same column is superseded, not combined.
  key_column_ = runtime_filter_column_;
  if (runtime_filter_ != nullptr) {
    key_filter_ = nullptr;
  }
  if (runtime_filter_ == nullptr || !pos_.IsValid()) {
    return;
  }
  // The constructor already seeked to the first visible row, which is exactly
  // the row a filter installed later would have rejected. Re-seek from the
  // current position so the filter sees it too; the contract is that nothing
  // has consumed a row yet, so no row is ever skipped unfiltered. A writable
  // transaction's iterator drops its page latch after every row, so the page
  // has to be re-pinned before the re-seek (AdvancePage needs it too).
  //
  // A failed re-pin leaves `pos_` invalid on purpose: PinPage latched the
  // failure into `status_`, and an iterator that still claims to be valid would
  // hand the caller the cleared row instead of the error.
  if (page_ == nullptr && !PinPage(pos_.page_id)) {
    pos_.page_id = ~0ULL;
    current_row_.Clear();
    return;
  }
  SeekVisibleRow();
}

bool FullScanIterator::IsValid() const { return pos_.IsValid(); }

const Row& FullScanIterator::operator*() const { return current_row_; }
Row& FullScanIterator::operator*() { return current_row_; }

void FullScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->schema_.Name();
  if (runtime_filter_ != nullptr && runtime_filter_column_.has_value()) {
    o << " (runtime filter: " << runtime_filter_->Describe() << " on column "
      << *runtime_filter_column_ << ")";
  }
}

}  // namespace tinylamb
