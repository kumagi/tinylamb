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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
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
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
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

struct FullScanIterator::StubBlockCache {
  TlcsFile::DecodedBlock block;
  TlcsFile::DecodedBlock delta;
  std::vector<TlcsFile::DecodedColumnChunk> free_chunks;
  std::vector<slot_t> col_chunk_idx;
  std::vector<uint8_t> io_scratch;
};

FullScanIterator::~FullScanIterator() = default;

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
  if (TryInitLSTableScan()) {
    return;
  }
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
  if (TryInitLSTableScan()) {
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
  if ((++advance_steps_ & 255U) == 0 && txn_ != nullptr &&
      txn_->HasStatementTimeout()) {
    const Status ts = txn_->CheckStatementTimeout();
    if (ts != Status::kSuccess) {
      status_ = ts;
      pos_.page_id = ~0ULL;
      current_row_.Clear();
      return *this;
    }
  }
  if (lstable_store_ != nullptr) {
    ++lstable_row_cursor_;
    SeekLSTableRow();
    return *this;
  }
  if (!lock_rows_ && stub_block_idx_ != LSTableStore::kInvalidBlockIndex) {
    ++pos_.slot;
    if (pos_.slot >= stub_block_row_count_) {
      if (!AdvanceFromStubPage()) {
        pos_.page_id = ~0ULL;
        current_row_.Clear();
        stub_row_materialized_ = true;
        return *this;
      }
    }
    SeekVisibleRow();
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
  // Collapse adjacent fixed-width cells. Only variable-width cells need
  // inspection per row; this also covers dates after VARCHAR columns.
  for (slot_t i = 0; i < column; ++i) {
    const ValueType previous = schema.GetColumn(i).Type();
    const auto fixed = static_cast<uint32_t>(FixedSerializedSize(previous));
    if (fixed == 0) {
      peek.variable_skips.push_back({previous, 0});
    } else if (peek.variable_skips.empty()) {
      peek.offset += fixed;
    } else {
      peek.variable_skips.back().fixed_after += fixed;
    }
  }
  peek.valid = true;
  peek.stored_count = schema.ColumnCount();
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
    // A column-count mismatch is a corrupt row image, not a NULL key. Report
    // it through the error channel so a filtered scan fails with kCorrupt
    // exactly like the unfiltered decode path, instead of silently dropping
    // the row (which used to look like an ordinary filter mismatch).
    return {.kind = PeekOutcome::Kind::kError,
            .value = 0,
            .status = StatusError(StatusCode::kCorrupt,
                                  "row image column count mismatch")};
  }
  if (has_null) {
    // A bitmap row may NULL out prefix columns, whose payload then shifts
    // every constant offset — take the walked path for those rows.
    return {.kind = PeekOutcome::Kind::kFallback, .value = 0};
  }
  const char* payload = row + sizeof(slot_t) + peek.offset;
  for (const auto& skip : peek.variable_skips) {
    Status status = Status::kSuccess;
    const size_t consumed =
        Value::SkipSerializedInto(payload, skip.type, &status);
    if (status != Status::kSuccess) [[unlikely]] {
      // Same corrupt-image failure channel as Row::TryPeekInteger: a Status,
      // not a C++ exception across the caller's StatusOr boundary.
      return {.kind = PeekOutcome::Kind::kError, .value = 0, .status = status};
    }
    payload += consumed + skip.fixed_after;
  }
  int64_t key = 0;
  DeserializeInteger(payload, &key);
  return {.kind = PeekOutcome::Kind::kValue, .value = key};
}

StatusOr<bool> FullScanIterator::PassesPeekFilters(std::string_view row) const {
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
  bool has_cached_value = false;
  int64_t cached_value = 0;
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
            has_cached_value = true;
            cached_value = outcome.value;
            break;
          case PeekOutcome::Kind::kNull:
            has_cached_value = false;
            break;
          case PeekOutcome::Kind::kFallback: {
            ASSIGN_OR_RETURN(
                std::optional<int64_t>, peeked,
                Row::TryPeekInteger(
                    row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                    table_->GetSchema(), pred.column));
            has_cached_value = peeked.has_value();
            cached_value = peeked.value_or(0);
          } break;
          case PeekOutcome::Kind::kError:
            return outcome.status;
        }
      } else {
        {
          ASSIGN_OR_RETURN(
              std::optional<int64_t>, peeked,
              Row::TryPeekInteger(
                  row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                  table_->GetSchema(), pred.column));
          has_cached_value = peeked.has_value();
          cached_value = peeked.value_or(0);
        }
      }
    }
    if (!has_cached_value ||
        !MatchIntegerCompare(cached_value, pred.op, pred.constant)) {
      return false;
    }
  }
  return true;
}

std::optional<std::string_view> FullScanIterator::NextVisibleImage() {
  // Precondition: page_ is pinned (the constructors and operator++ re-pin it
  // after DropPageLatch or a per-row unpin before calling this method).
  while (pos_.IsValid()) {
    // The shared latch freezes the page image. A writer may register a
    // version chain meanwhile, but cannot change these bytes before we drop
    // the latch. Re-pinning invalidates this verdict before any newer image
    // is read. A cached false verdict only keeps the ordinary MVCC resolver.
    bool is_stub = (*page_)->body.row_page.IsColumnarStub();
    // P356/426: a stub carries the current TLCS image with no version
    // chains, so it may only serve transactions entitled to a physical read
    // (PhysicalReadEligible below).  Locking (DML) scans hydrate always; a
    // pinned/read-version scan of a chained or mutating transaction hydrates
    // too instead of answering with rows that "became newer".
    if (is_stub && (lock_rows_ || !PhysicalReadEligible(*txn_, **page_))) {
      const page_id_t pid = pos_.page_id;
      page_.reset();
      const Status h = table_->EnsurePageHydratedIfStub(*txn_, pid);
      if (h != Status::kSuccess) {
        status_ = h;
        txn_->MarkFailed();
        pos_.page_id = ~0ULL;
        return std::nullopt;
      }
      if (!PinPage(pid)) {
        return std::nullopt;
      }
      is_stub = (*page_)->body.row_page.IsColumnarStub();
      physical_read_eligible_ = PhysicalReadEligible(*txn_, **page_);
    }
    if (is_stub && stub_decoded_pid_ != pos_.page_id) {
      std::shared_ptr<const TlcsFile> tlcs = table_->GetBackingTlcsStore();
      if (tlcs == nullptr) {
        status_ = StatusError(StatusCode::kNotExists,
                              "stub page missing backing tlcs store");
        pos_.page_id = ~0ULL;
        return std::nullopt;
      }
      // #1111: pin the block's shared cached encoded images; the decode and
      // Row -> Serialize round trip now happen at most once per block.
      auto rows =
          tlcs->GetCachedEncodedBlockByPid(pos_.page_id, table_->GetSchema());
      if (!rows.HasValue()) {
        status_ = rows.GetStatus();
        pos_.page_id = ~0ULL;
        return std::nullopt;
      }
      stub_decoded_rows_ = rows.MoveValue();
      stub_decoded_pid_ = pos_.page_id;
    }
    if (!physical_read_eligible_.has_value()) {
      physical_read_eligible_ = PhysicalReadEligible(*txn_, **page_);
    }
    const bool physical_read = *physical_read_eligible_;
    // A locking scan may release and re-pin this page while waiting for an
    // intent, so no body reference may survive across that wait.
    while (pos_.slot < (*page_)->body.row_page.RowMax()) {
      std::optional<std::string_view> row;
      const bool already_reserved = lock_rows_ && txn_->HasWriteSet(pos_);
      if (is_stub) {
        if (pos_.slot < stub_decoded_rows_->size() &&
            !(*stub_decoded_rows_)[pos_.slot].empty()) {
          row = std::string_view((*stub_decoded_rows_)[pos_.slot]);
        }
      } else if (lock_rows_) {
        // DML source scan: pin the position's head version with a write
        // intent BEFORE resolving the row, so the image handed to the
        // predicate is the newest committed version -- the same contract
        // IndexScan's Position/AddWriteSet/ResolveRow sequence provides.
        // Vacant slots cannot carry a live head row, so they are skipped
        // without spending an intent.
        if (!(*page_)->body.row_page.IsSlotOccupied(pos_.slot)) {
          ++pos_.slot;
          continue;
        }
        if (wait_for_write_intent_ && !already_reserved) {
          page_.reset();
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
          status_ = txn_->LastWriteSetFailureStatus();
          pos_.page_id = ~0ULL;
          return std::nullopt;
        }
        if (page_ == nullptr && !PinPage(pos_.page_id)) {
          if (!already_reserved) {
            txn_->ReleaseWriteIntent(pos_);
          }
          for (const RowPosition& held : scan_acquired_) {
            txn_->ReleaseWriteIntent(held);
          }
          scan_acquired_.clear();
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
        const RowPage& body = (*page_)->body.row_page;
        if (body.IsSlotOccupied(pos_.slot)) [[likely]] {
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
          StatusOr<std::optional<int64_t>> peeked = std::nullopt;
          if (key_peek_plan_.valid) {
            const PeekOutcome outcome = PeekIntegerPlanned(
                key_peek_plan_,
                row->data());  // NOLINT(bugprone-suspicious-stringview-data-usage)
            switch (outcome.kind) {
              case PeekOutcome::Kind::kValue:
                peeked = std::optional<int64_t>{outcome.value};
                break;
              case PeekOutcome::Kind::kNull:
                peeked = std::nullopt;
                break;
              case PeekOutcome::Kind::kFallback:
                peeked = Row::TryPeekInteger(
                    row->data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                    table_->GetSchema(), *key_column_);
                break;
              case PeekOutcome::Kind::kError:
                peeked = outcome.status;
                break;
            }
          } else {
            peeked = Row::TryPeekInteger(
                row->data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
                table_->GetSchema(), *key_column_);
          }
          if (!peeked.HasValue()) {
            status_ = peeked.GetStatus();
            txn_->MarkFailed();
            pos_.page_id = ~0ULL;
            return std::nullopt;
          }
          const std::optional<int64_t> key = peeked.Value();
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
        auto passed = PassesPeekFilters(*row);
        if (!passed.HasValue()) {
          status_ = passed.GetStatus();
          txn_->MarkFailed();
          pos_.page_id = ~0ULL;
          return std::nullopt;
        }
        if (!passed.Value()) {
          if (lock_rows_ && !already_reserved) {
            txn_->ReleaseWriteIntent(pos_);
          }
          ++pos_.slot;
          continue;
        }
        if (lock_rows_ && !already_reserved) {
          scan_acquired_.push_back(pos_);
        }
        // Keep the image pinned until the caller has decoded/copied it.
        // SeekVisibleRow and DecodeNextTypedBatch release writable plain
        // scans after consuming the bytes, before returning to a mutator.
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
  while (pos_.IsValid()) {
    if (!lock_rows_ && stub_block_idx_ == LSTableStore::kInvalidBlockIndex &&
        page_ != nullptr && (*page_)->body.row_page.IsColumnarStub()) {
      if (!TryEnterStubPage()) {
        if (!pos_.IsValid()) {
          current_row_.Clear();
          stub_row_materialized_ = true;
          return;
        }
        break;
      }
    }
    if (lock_rows_ || stub_block_idx_ == LSTableStore::kInvalidBlockIndex) {
      break;
    }
    if (!stub_zone_maps_pass_ && pos_.slot < stub_block_row_count_) {
      stub_zone_maps_pass_ = backing_tlcs_->BlockMatchesZoneMaps(
          stub_block_idx_, peek_compares_, runtime_filter_, key_column_);
    }
    if (!stub_zone_maps_pass_ || pos_.slot >= stub_block_row_count_) {
      if (!AdvanceFromStubPage()) {
        pos_.page_id = ~0ULL;
        current_row_.Clear();
        stub_row_materialized_ = true;
        return;
      }
      continue;
    }
    if (stub_all_rows_match_ < 0) {
      stub_all_rows_match_ =
          StubBlockAllRowsMatch(*backing_tlcs_, stub_block_idx_) ? 1 : 0;
    }
    if (stub_all_rows_match_ == 1) {
      stub_row_materialized_ = false;
      return;
    }
    std::vector<slot_t> filter_cols;
    GatherStubFilterCols(/*include_keep_cols=*/false, &filter_cols);
    if (!EnsureStubBlockColumns(stub_block_idx_, pos_.page_id, filter_cols)) {
      pos_.page_id = ~0ULL;
      current_row_.Clear();
      stub_row_materialized_ = true;
      return;
    }
    if (!stub_matches_computed_) {
      ComputeStubBlockMatches();
    }
    while (stub_match_cursor_ < stub_match_rows_.size() &&
           stub_match_rows_[stub_match_cursor_] < pos_.slot) {
      ++stub_match_cursor_;
    }
    if (stub_match_cursor_ < stub_match_rows_.size()) {
      pos_.slot = static_cast<slot_t>(stub_match_rows_[stub_match_cursor_]);
      stub_row_materialized_ = false;
      return;
    }
    pos_.slot = static_cast<slot_t>(stub_block_row_count_);
    if (!AdvanceFromStubPage()) {
      pos_.page_id = ~0ULL;
      current_row_.Clear();
      stub_row_materialized_ = true;
      return;
    }
  }
  if (!pos_.IsValid()) {
    current_row_.Clear();
    page_.reset();
    return;
  }
  std::optional<std::string_view> row = NextVisibleImage();
  if (!row.has_value()) {
    current_row_.Clear();
    page_.reset();
    return;
  }
  DeserializeCurrent(*row);
  if (!txn_->IsReadOnly() && !pages_) {
    page_.reset();
  }
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
      if (txn_->GetPageManager()->TailPageInvisibleToSnapshot(
              *txn_, table_->first_pid_, next_page)) {
        // Another transaction's uncommitted growth: the visible chain ends.
        return false;
      }
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
      physical_read_eligible_.reset();
    }
  }
  if (next_page == 0) {
    return false;
  }
  pos_ = RowPosition(next_page, 0);
  return PinPage(next_page);
}

bool FullScanIterator::PinPage(page_id_t page_id) {
  physical_read_eligible_.reset();
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
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (schema.GetColumn(i).IsInterval()) {
      row_plan_failed_ = true;
      return nullptr;
    }
  }
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
  plan.slot_unsigned.resize(kept.size());
  plan.slot_interval.resize(kept.size());
  bool has_unsigned = false;
  bool has_interval = false;
  for (size_t k = 0; k < kept.size(); ++k) {
    plan.stored_slot[kept[k]] = k;
    plan.slot_unsigned[k] =
        schema.GetColumn(kept[k]).IsUnsigned() &&
                plan.stored_types[kept[k]] == ValueType::kInt64
            ? uint8_t{1}
            : uint8_t{0};
    has_unsigned = has_unsigned || plan.slot_unsigned[k] != 0;
    plan.slot_interval[k] =
        schema.GetColumn(kept[k]).IsInterval() ? uint8_t{1} : uint8_t{0};
    has_interval = has_interval || plan.slot_interval[k] != 0;
  }
  plan.has_unsigned = has_unsigned;
  plan.has_interval = has_interval;
  row_plan_.emplace(std::move(plan));
  return &*row_plan_;
}

void FullScanIterator::DeserializeCurrent(std::string_view row) {
  // Length-prefixed page images: decoded by offsets, never as C strings.
  const std::vector<slot_t>* const kept =
      projection_.has_value() ? &*projection_ : nullptr;
  StatusOr<size_t> decoded = Status::kCorrupt;
  if (kept != nullptr) {
    if (const RowDecodePlan* plan = RowPlan()) {
      decoded = current_row_.TryDeserializePlanned(
          row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
          *plan);
    } else {
      decoded = current_row_.TryDeserializeProjected(
          row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
          table_->schema_, *kept);
    }
  } else {
    decoded = current_row_.TryDeserialize(
        row.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
        table_->schema_);
  }
  if (!decoded.HasValue()) {
    status_ = decoded.GetStatus();
    pos_.page_id = ~0ULL;
    current_row_.Clear();
    page_.reset();
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
  // Mirror RowPlan's contract: every kept slot must be in range and strictly
  // ascending. Without this check a malformed projection silently drops
  // columns (e.g. {2,1} on a 3-column schema keeps only slot 2), diverging
  // from the Row path which rejects it and falls back.
  if (kept != nullptr) {
    for (size_t k = 0; k < kept->size(); ++k) {
      if ((*kept)[k] >= schema.ColumnCount() ||
          (k > 0 && (*kept)[k - 1] >= (*kept)[k])) {
        typed_keep_slots_.clear();
        return;
      }
    }
  }
  size_t kept_cursor = 0;
  size_t varchar_ordinal = 0;
  bool variable_seen = false;
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const bool keep = kept == nullptr ||
                      (kept_cursor < kept->size() && (*kept)[kept_cursor] == i);
    const ValueType type = schema.GetColumn(i).Type();
    if (keep && (schema.GetColumn(i).IsInterval() ||
                 (type != ValueType::kInt64 && type != ValueType::kDate &&
                  type != ValueType::kDouble && type != ValueType::kVarChar))) {
      // ARRAY (and any future variable-shape type) has no cell payload; the
      // caller falls back to the Row loop when projected.
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
    // The constant-offset prefix runs until the first variable-width payload
    // (VARCHAR or ARRAY): every earlier stored column (kept or skipped)
    // serializes as exactly 8 bytes, so kept slots inside it read at
    // schema-constant image offsets.
    if (type != ValueType::kInt64 && type != ValueType::kDate &&
        type != ValueType::kDouble) {
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
  if (lstable_store_ != nullptr) {
    return DecodeNextTypedBatchFromLSTable(batch, max_rows);
  }
  const Schema& schema = table_->schema_;
  size_t count = 0;
  while (count < max_rows) {
    if (!pos_.IsValid()) {
      break;
    }
    // The typed-batch path advances pos_ itself and never calls operator++,
    // so it must check the statement budget on its own cadence (same 256-step
    // rule) instead of letting a large batch run without a timeout check.
    if ((++advance_steps_ & 255U) == 0 && txn_ != nullptr &&
        txn_->HasStatementTimeout()) {
      const Status ts = txn_->CheckStatementTimeout();
      if (ts != Status::kSuccess) {
        status_ = ts;
        pos_.page_id = ~0ULL;
        current_row_.Clear();
        batch->rows = count;
        return count;
      }
    }
    if (!lock_rows_ && stub_block_idx_ != LSTableStore::kInvalidBlockIndex) {
      count += DecodeStubBlockBatch(batch, count, max_rows - count);
      continue;
    }
    if (page_ == nullptr && pos_.IsValid() && !PinPage(pos_.page_id)) {
      pos_.page_id = ~0ULL;
      break;
    }
    if (!lock_rows_ && page_ != nullptr &&
        (*page_)->body.row_page.IsColumnarStub() && TryEnterStubPage()) {
      count += DecodeStubBlockBatch(batch, count, max_rows - count);
      continue;
    }
    if (!pos_.IsValid()) {
      break;
    }
    // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
    //   (fast_readonly_row_page_filter_eq)
    // Tight in-page batch loop for clean read-only all-fixed-width RowPages
    // (e.g. uncompacted 133M-row `inventory` in TPC-DS Q21/Q22/Q72): avoids
    // per-row NextVisibleImage() dispatch, PeekIntegerPlanned switches, and
    // per-row null-flag memset. Falls through to NextVisibleImage() on page
    // exhaustion, NULL-bitmap rows, or corrupt headers.
    if (!lock_rows_ && page_ != nullptr &&
        !(*page_)->body.row_page.IsColumnarStub() &&
        typed_const_prefix_ == schema.ColumnCount() &&
        !batch->capture_positions &&
        (peek_compares_ == nullptr || peek_compares_->empty()) &&
        key_filter_ == nullptr) {
      if (!physical_read_eligible_.has_value()) {
        physical_read_eligible_ = PhysicalReadEligible(*txn_, **page_);
      }
      if (*physical_read_eligible_) {
        const RowPage& body = (*page_)->body.row_page;
        const slot_t row_max = body.RowMax();
        const slot_t ncols = schema.ColumnCount();
        const size_t stride = batch->stride;
        const bool has_rf = (runtime_filter_ != nullptr &&
                             key_column_.has_value() && *key_column_ < ncols);
        const bool rf_dense = has_rf && runtime_filter_->has_dense_bits();
        const uint64_t rf_lo_u =
            rf_dense ? static_cast<uint64_t>(runtime_filter_->Lo()) : 0ULL;
        const uint64_t rf_span_u =
            rf_dense ? (static_cast<uint64_t>(runtime_filter_->Hi()) - rf_lo_u)
                     : 0ULL;
        const uint64_t* const rf_bits =
            rf_dense ? runtime_filter_->dense_bits_data() : nullptr;
        const size_t rf_byte_off =
            has_rf ? (sizeof(slot_t) +
                      (sizeof(uint64_t) * static_cast<size_t>(*key_column_)))
                   : 0;
        constexpr slot_t kNullBitmapFlagFast = slot_t{1} << 15;
        const size_t min_nonnull_bytes =
            sizeof(slot_t) + (sizeof(uint64_t) * static_cast<size_t>(ncols));
        const size_t start_count = count;
        int64_t* const cells_data = batch->cells.data();
        while (pos_.slot < row_max && count < max_rows) {
          if (!body.IsSlotOccupied(pos_.slot)) {
            ++pos_.slot;
            continue;
          }
          const std::string_view sv = body.GetRow(pos_.slot);
          if (sv.size() < min_nonnull_bytes) [[unlikely]] {
            break;
          }
          const char* const row_base = sv.data();
          slot_t encoded_count = 0;
          const size_t hdr_bytes = DeserializeSlot(row_base, &encoded_count);
          const bool has_null = (encoded_count & kNullBitmapFlagFast) != 0;
          const auto stored_count =
              static_cast<slot_t>(encoded_count & ~kNullBitmapFlagFast);
          if (has_null || stored_count != ncols) [[unlikely]] {
            break;
          }
          if (has_rf) {
            uint64_t kbits = 0;
            std::memcpy(&kbits, row_base + rf_byte_off, sizeof(uint64_t));
            const int64_t key = static_cast<int64_t>(TINYLAMB_BSWAP64(kbits));
            if (rf_dense) {
              const uint64_t off = static_cast<uint64_t>(key) - rf_lo_u;
              if (off > rf_span_u ||
                  ((rf_bits[off >> 6] >> (off & 63)) & uint64_t{1}) == 0) {
                ++pos_.slot;
                continue;
              }
            } else if (runtime_filter_->Reject(key)) {
              ++pos_.slot;
              continue;
            }
          }
          if (!extra_runtime_filters_.empty()) {
            bool extra_rejected = false;
            for (const ExtraRuntimeFilter& erf : extra_runtime_filters_) {
              const size_t e_off =
                  sizeof(slot_t) +
                  (sizeof(uint64_t) * static_cast<size_t>(erf.column));
              uint64_t ebits = 0;
              std::memcpy(&ebits, row_base + e_off, sizeof(uint64_t));
              const int64_t ekey =
                  static_cast<int64_t>(TINYLAMB_BSWAP64(ebits));
              if (erf.group->has_dense_bits()) {
                const uint64_t lo_u = static_cast<uint64_t>(erf.group->Lo());
                const uint64_t span_u =
                    static_cast<uint64_t>(erf.group->Hi()) - lo_u;
                const uint64_t off = static_cast<uint64_t>(ekey) - lo_u;
                if (off > span_u ||
                    ((erf.group->dense_bits_data()[off >> 6] >> (off & 63)) &
                     uint64_t{1}) == 0) {
                  extra_rejected = true;
                  break;
                }
              } else if (erf.group->Reject(ekey)) {
                extra_rejected = true;
                break;
              }
            }
            if (extra_rejected) {
              ++pos_.slot;
              continue;
            }
          }
          const char* const payload = row_base + hdr_bytes;
          const size_t base = count * stride;
          if (typed_prefix_identity_) {
            DecodePrefixCells(payload, cells_data + base, typed_prefix_kept_);
          } else {
            for (size_t k = 0; k < typed_prefix_kept_; ++k) {
              uint64_t bits = 0;
              std::memcpy(&bits,
                          payload + (sizeof(uint64_t) * typed_keep_slots_[k]),
                          sizeof(uint64_t));
              cells_data[base + k] =
                  static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
            }
          }
          ++count;
          ++pos_.slot;
        }
        if (count > start_count) {
          std::memset(batch->nulls.data() + (start_count * stride), 0,
                      (count - start_count) * stride);
        }
        if (count >= max_rows) {
          break;
        }
      }
    }
    std::optional<std::string_view> row = NextVisibleImage();
    if (!row.has_value()) {
      page_.reset();
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
    // Projection is ascending. Once its final cell is decoded, later stored
    // columns cannot contribute to this batch (including variable-width
    // suffixes); advancing the row cursor is independent of their payload.
    for (; i < stored_count && i < schema.ColumnCount() &&
           kept_cursor < typed_keep_slots_.size();
         ++i) {
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
          batch->rows = count;
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
          batch->rows = count;
          return count;
      }
      ++kept_cursor;
    }
    if (batch->capture_positions) {
      batch->positions.push_back(pos_);
    }
    ++count;
    // Iterator contract: NextVisibleImage left pos_ on the decoded row;
    // advance past it (operator++'s movement without the Row decode).
    ++pos_.slot;
    if (!txn_->IsReadOnly() && !pages_) {
      page_.reset();
    }
  }
  if (pos_.IsValid() && !lock_rows_ &&
      (stub_block_idx_ != LSTableStore::kInvalidBlockIndex ||
       (page_ != nullptr && (*page_)->body.row_page.IsColumnarStub()))) {
    SeekVisibleRow();
  }
  batch->rows = count;
  return count;
}

IteratorBase& FullScanIterator::operator--() {
  // Backward iteration is not implemented for a physical row-page scan; the
  // interface keeps operator-- because IndexScanIterator supports it. Surface
  // the unsupported operation as a sticky Status (IteratorBase::GetStatus)
  // instead of aborting the process, and invalidate the cursor so a generic
  // caller's loop terminates and can tell "unsupported" from "exhausted".
  status_ = StatusError(StatusCode::kInvalidArgument,
                        "FullScanIterator does not support operator--");
  pos_.page_id = ~0ULL;
  current_row_.Clear();
  return *this;
}

void FullScanIterator::DropPageLatch() {
  page_.reset();
  physical_read_eligible_.reset();
  // status_ is deliberately NOT cleared here: IteratorBase defines it as a
  // sticky first-failure record, and callers (e.g. parallel morsel workers)
  // check GetStatus() immediately after dropping the latch. A worker that
  // wants a fresh scan over a different page uses a fresh iterator; a single
  // iterator is never handed to another thread/page while a sticky failure
  // from this one is still pending.
}

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
  // side's type (unknown when it had no comparable key, i.e. kNull) cannot
  // mismatch.  kNull is therefore accepted ONLY for such a group: on any
  // other group an unknown build domain could reject probe keys that share
  // the int64 bit pattern but not the comparison domain.
  if (declared != type && !group->RejectsAll()) {
    return nullptr;
  }
  return group;
}

void FullScanIterator::AddExtraRuntimeFilter(const RuntimeFilterGroup* group,
                                             slot_t column, ValueType type) {
  const RuntimeFilterGroup* accepted =
      AcceptsColumn(group, std::optional<slot_t>(column), type);
  if (accepted == nullptr || extra_runtime_filters_.size() >= 4) {
    return;
  }
  extra_runtime_filters_.push_back(
      ExtraRuntimeFilter{.group = accepted, .column = column});
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
  if (runtime_filter_ != nullptr) {
    key_column_ = runtime_filter_column_;
    key_filter_ = nullptr;
    key_peek_built_ = false;
  }
  if (runtime_filter_ == nullptr || !pos_.IsValid()) {
    return;
  }
  if (lstable_store_ != nullptr) {
    SeekLSTableRow();
    return;
  }
  if (!lock_rows_ && stub_block_idx_ != LSTableStore::kInvalidBlockIndex) {
    stub_zone_maps_pass_ = false;
    stub_all_rows_match_ = -1;
    stub_matches_computed_ = false;
    stub_match_cursor_ = 0;
    SeekVisibleRow();
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

bool FullScanIterator::TryInitLSTableScan() {
  if (table_ == nullptr || txn_ == nullptr || lock_rows_ || txn_->IsFailed() ||
      table_->RowsRewrittenInPlace()) {
    return false;
  }
  if (projection_.has_value() && RowPlan() == nullptr) {
    return false;
  }
  std::shared_ptr<const LSTableStore> store =
      GetOrBuildLSTableStore(*txn_, *table_, /*allow_lazy_build=*/false);
  if (store == nullptr ||
      store->layout_generation != table_->LayoutGeneration() ||
      store->first_pid != table_->FirstPageId()) {
    return false;
  }
  if (store->uncommitted_owner_txn != 0) {
    if (store->uncommitted_owner_txn != txn_->ID()) {
      return false;
    }
  } else if (!txn_->IsReadOnly() && !txn_->WriteSetEmpty()) {
    return false;
  }
  const bool check_chains =
      store->uncommitted_owner_txn == 0 && txn_->HasAnyPageVersionChains();
  if (!pages_.has_value()) {
    if (check_chains) {
      for (const page_id_t pid : store->page_ids) {
        if (txn_->HasPageVersionChains(pid)) {
          return false;
        }
      }
    }
  } else {
    for (const page_id_t pid : *pages_) {
      if ((check_chains && txn_->HasPageVersionChains(pid)) ||
          store->FindBlock(pid) == nullptr) {
        return false;
      }
    }
  }
  lstable_store_ = std::move(store);
  lstable_page_cursor_ = 0;
  lstable_row_cursor_ = 0;
  lstable_row_materialized_ = true;
  SeekLSTableRow();
  return true;
}

const LSTablePageBlock* FullScanIterator::CurrentLSTableBlock() const noexcept {
  if (lstable_store_ == nullptr) {
    return nullptr;
  }
  if (pages_.has_value()) {
    if (lstable_page_cursor_ >= pages_->size()) {
      return nullptr;
    }
    return lstable_store_->FindBlock((*pages_)[lstable_page_cursor_]);
  }
  if (lstable_page_cursor_ >= lstable_store_->blocks.size()) {
    return nullptr;
  }
  return &lstable_store_->blocks[lstable_page_cursor_];
}

bool FullScanIterator::LSTableRowMatches(const LSTablePageBlock& blk,
                                         uint32_t row) const noexcept {
  if ((runtime_filter_ != nullptr || key_filter_ != nullptr) &&
      key_column_.has_value()) {
    const slot_t col_idx = *key_column_;
    if (col_idx >= blk.columns.size()) {
      return false;
    }
    const LSTableColumnBlock& col = blk.columns[col_idx];
    if (col.IsNull(row) ||
        (col.type != ValueType::kInt64 && col.type != ValueType::kDate)) {
      return false;
    }
    const int64_t key = col.IntAt(row);
    const bool rejected = runtime_filter_ != nullptr
                              ? runtime_filter_->Reject(key)
                              : !key_filter_->contains(key);
    if (rejected) {
      return false;
    }
  }
  if (peek_compares_ != nullptr && !peek_compares_->empty()) {
    for (const IntegerPeekCompare& pred : *peek_compares_) {
      if (pred.column >= blk.columns.size()) {
        return false;
      }
      const LSTableColumnBlock& col = blk.columns[pred.column];
      if (col.IsNull(row) ||
          (col.type != ValueType::kInt64 && col.type != ValueType::kDate)) {
        return false;
      }
      if (!MatchIntegerCompare(col.IntAt(row), pred.op, pred.constant)) {
        return false;
      }
    }
  }
  return true;
}

bool FullScanIterator::LSTableBlockAllRowsMatch(
    const LSTablePageBlock& blk) const noexcept {
  if ((runtime_filter_ != nullptr || key_filter_ != nullptr) &&
      key_column_.has_value()) {
    return false;
  }
  if (peek_compares_ == nullptr || peek_compares_->empty()) {
    return true;
  }
  for (const IntegerPeekCompare& cmp : *peek_compares_) {
    if (cmp.column >= blk.columns.size()) {
      return false;
    }
    const LSTableColumnBlock& col = blk.columns[cmp.column];
    if (col.has_null || !col.has_min_max ||
        (col.type != ValueType::kInt64 && col.type != ValueType::kDate)) {
      return false;
    }
    switch (cmp.op) {
      case BinaryOperation::kEquals:
        if (col.min_int != cmp.constant || col.max_int != cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kNotEquals:
        if (cmp.constant >= col.min_int && cmp.constant <= col.max_int) {
          return false;
        }
        break;
      case BinaryOperation::kLessThan:
        if (col.max_int >= cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kLessThanEquals:
        if (col.max_int > cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThan:
        if (col.min_int <= cmp.constant) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThanEquals:
        if (col.min_int < cmp.constant) {
          return false;
        }
        break;
      default:
        return false;
    }
  }
  return true;
}

void FullScanIterator::SeekLSTableRow() {
  if (lstable_store_ == nullptr) {
    pos_.page_id = ~0ULL;
    current_row_.Clear();
    lstable_row_materialized_ = true;
    return;
  }
  const size_t total_pages =
      pages_.has_value() ? pages_->size() : lstable_store_->blocks.size();
  while (lstable_page_cursor_ < total_pages) {
    const LSTablePageBlock* blk = CurrentLSTableBlock();
    if (blk == nullptr || blk->row_count == 0) {
      ++lstable_page_cursor_;
      lstable_row_cursor_ = 0;
      continue;
    }
    if (lstable_row_cursor_ == 0 &&
        !blk->MatchesZoneMaps(peek_compares_, runtime_filter_, key_column_)) {
      ++lstable_page_cursor_;
      continue;
    }
    for (; lstable_row_cursor_ < blk->row_count; ++lstable_row_cursor_) {
      if (LSTableRowMatches(*blk, lstable_row_cursor_)) {
        pos_ = RowPosition(blk->page_id, blk->SlotAt(lstable_row_cursor_));
        lstable_row_materialized_ = false;
        return;
      }
    }
    ++lstable_page_cursor_;
    lstable_row_cursor_ = 0;
  }
  pos_.page_id = ~0ULL;
  current_row_.Clear();
  lstable_row_materialized_ = true;
}

void FullScanIterator::EnsureLSTableRowMaterialized() const {
  if (lstable_row_materialized_ || lstable_store_ == nullptr) {
    return;
  }
  if (!pos_.IsValid()) {
    current_row_.Clear();
    lstable_row_materialized_ = true;
    return;
  }
  const LSTablePageBlock* blk = CurrentLSTableBlock();
  if (blk == nullptr || lstable_row_cursor_ >= blk->row_count) {
    current_row_.Clear();
    lstable_row_materialized_ = true;
    return;
  }
  const uint32_t r = lstable_row_cursor_;
  const size_t col_count =
      projection_.has_value() ? projection_->size() : blk->columns.size();
  current_row_.values_.resize(col_count);
  for (size_t k = 0; k < col_count; ++k) {
    const slot_t col_idx =
        projection_.has_value() ? (*projection_)[k] : static_cast<slot_t>(k);
    const LSTableColumnBlock& col = blk->columns[col_idx];
    if (col.IsNull(r)) {
      current_row_.values_[k] = Value();
      continue;
    }
    switch (col.type) {
      case ValueType::kInt64:
        current_row_.values_[k] = Value(col.IntAt(r));
        if (col.is_unsigned) {
          current_row_.values_[k].SetUnsigned();
        }
        break;
      case ValueType::kDate:
        current_row_.values_[k] = Value::DateFromDays(col.IntAt(r));
        break;
      case ValueType::kDouble: {
        double d = 0.0;
        const int64_t bits = col.IntAt(r);
        std::memcpy(&d, &bits, sizeof(d));
        current_row_.values_[k] = Value(d);
        break;
      }
      case ValueType::kVarChar:
        current_row_.values_[k] = Value(std::string(col.VarcharAt(r)));
        break;
      default:
        current_row_.values_[k] = Value();
        break;
    }
  }
  lstable_row_materialized_ = true;
}

size_t FullScanIterator::DecodeNextTypedBatchFromLSTable(TypedCellBatch* batch,
                                                         size_t max_rows) {
  const size_t total_pages =
      pages_.has_value() ? pages_->size() : lstable_store_->blocks.size();
  const size_t stride = batch->stride;
  const size_t kept_count = typed_keep_slots_.size();
  size_t count = 0;
  while (count < max_rows && lstable_page_cursor_ < total_pages) {
    const LSTablePageBlock* blk = CurrentLSTableBlock();
    if (blk == nullptr || blk->row_count == 0) {
      ++lstable_page_cursor_;
      lstable_row_cursor_ = 0;
      continue;
    }
    if (lstable_row_cursor_ == 0 &&
        !blk->MatchesZoneMaps(peek_compares_, runtime_filter_, key_column_)) {
      ++lstable_page_cursor_;
      continue;
    }
    if (!blk->has_any_null && !batch->capture_positions &&
        LSTableBlockAllRowsMatch(*blk)) {
      const uint32_t r0 = lstable_row_cursor_;
      const size_t take =
          std::min<size_t>(blk->row_count - r0, max_rows - count);
      std::memset(batch->nulls.data() + (count * stride), 0, take * stride);
      for (size_t k = 0; k < kept_count; ++k) {
        const slot_t col_idx = typed_keep_slots_[k];
        const LSTableColumnBlock& col = blk->columns[col_idx];
        const size_t vslot = typed_varchar_slot_[k];
        if (vslot == SIZE_MAX) {
          if (stride == 1) {
            col.MaterializeIntSlice(r0, take, batch->cells.data() + count);
          } else if (col.HasRawIntCells()) {
            const int64_t* __restrict src = col.int_cells.data() + r0;
            int64_t* __restrict dst =
                batch->cells.data() + (count * stride) + k;
            for (size_t r = 0; r < take; ++r) {
              dst[r * stride] = src[r];
            }
          } else {
            int64_t* __restrict dst =
                batch->cells.data() + (count * stride) + k;
            for (size_t r = 0; r < take; ++r) {
              dst[r * stride] = col.IntAt(r0 + r);
            }
          }
        } else if (col.all_single_byte_varchar &&
                   col.VarcharBytes().size() >= blk->row_count) {
          auto& dst_bytes = batch->varchar_bytes[vslot];
          auto& dst_ends = batch->varchar_ends[vslot];
          const auto dst_base = static_cast<uint32_t>(dst_bytes.size());
          const size_t old_ends = dst_ends.size();
          const std::span<const uint8_t> src_bytes = col.VarcharBytes();
          dst_bytes.insert(dst_bytes.end(), src_bytes.data() + r0,
                           src_bytes.data() + r0 + take);
          dst_ends.resize(old_ends + take);
          uint32_t* __restrict out_ends = dst_ends.data() + old_ends;
          for (size_t r = 0; r < take; ++r) {
            out_ends[r] = dst_base + static_cast<uint32_t>(r + 1);
          }
        } else if (col.dict != nullptr &&
                   (col.dict->active || col.dict->wide_code_bytes != 0)) {
          auto& dst_bytes = batch->varchar_bytes[vslot];
          auto& dst_ends = batch->varchar_ends[vslot];
          for (size_t r = 0; r < take; ++r) {
            const std::string_view sv = col.VarcharAt(r0 + r);
            if (!sv.empty()) {
              dst_bytes.insert(dst_bytes.end(), sv.begin(), sv.end());
            }
            dst_ends.push_back(static_cast<uint32_t>(dst_bytes.size()));
          }
        } else {
          auto& dst_bytes = batch->varchar_bytes[vslot];
          auto& dst_ends = batch->varchar_ends[vslot];
          const std::span<const uint8_t> src_bytes = col.VarcharBytes();
          const std::span<const uint32_t> src_ends_all = col.VarcharEnds();
          if (src_ends_all.size() < r0 + take) {
            status_ = StatusError(StatusCode::kCorrupt,
                                  "LSTable VARCHAR ends are incomplete");
            return 0;
          }
          const uint32_t src_byte_begin = r0 == 0 ? 0U : src_ends_all[r0 - 1];
          const uint32_t src_byte_end = src_ends_all[r0 + take - 1];
          if (src_byte_end < src_byte_begin ||
              src_byte_end > src_bytes.size()) {
            status_ = StatusError(StatusCode::kCorrupt,
                                  "LSTable VARCHAR offsets are out of range");
            return 0;
          }
          const auto dst_base = static_cast<uint32_t>(dst_bytes.size());
          if (src_byte_end > src_byte_begin) {
            dst_bytes.insert(dst_bytes.end(), src_bytes.data() + src_byte_begin,
                             src_bytes.data() + src_byte_end);
          }
          const uint32_t offset_delta = dst_base - src_byte_begin;
          const uint32_t* __restrict src_ends = src_ends_all.data() + r0;
          const size_t old_ends = dst_ends.size();
          dst_ends.resize(old_ends + take);
          uint32_t* __restrict out_ends = dst_ends.data() + old_ends;
          for (size_t r = 0; r < take; ++r) {
            out_ends[r] = src_ends[r] + offset_delta;
          }
        }
      }
      count += take;
      lstable_row_cursor_ = static_cast<uint32_t>(r0 + take);
      if (lstable_row_cursor_ >= blk->row_count) {
        ++lstable_page_cursor_;
        lstable_row_cursor_ = 0;
      }
      continue;
    }

    if (!blk->has_any_null && !batch->capture_positions) {
      std::array<uint32_t, 2048> match_rows{};
      const size_t max_take = std::min<size_t>(max_rows - count, 2048);
      size_t n_match = 0;
      for (; lstable_row_cursor_ < blk->row_count && n_match < max_take;
           ++lstable_row_cursor_) {
        if (LSTableRowMatches(*blk, lstable_row_cursor_)) {
          match_rows[n_match++] = lstable_row_cursor_;
        }
      }
      if (n_match > 0) {
        std::memset(batch->nulls.data() + (count * stride), 0,
                    n_match * stride);
        for (size_t k = 0; k < kept_count; ++k) {
          const slot_t col_idx = typed_keep_slots_[k];
          const LSTableColumnBlock& col = blk->columns[col_idx];
          const size_t vslot = typed_varchar_slot_[k];
          if (vslot == SIZE_MAX) {
            int64_t* __restrict dst =
                batch->cells.data() + (count * stride) + k;
            if (col.HasRawIntCells()) {
              const int64_t* __restrict src = col.int_cells.data();
              for (size_t i = 0; i < n_match; ++i) {
                dst[i * stride] = src[match_rows[i]];
              }
            } else {
              for (size_t i = 0; i < n_match; ++i) {
                dst[i * stride] = col.IntAt(match_rows[i]);
              }
            }
          } else if (col.all_single_byte_varchar &&
                     col.VarcharBytes().size() >= blk->row_count) {
            auto& dst_bytes = batch->varchar_bytes[vslot];
            auto& dst_ends = batch->varchar_ends[vslot];
            const auto dst_base = static_cast<uint32_t>(dst_bytes.size());
            const size_t old_ends = dst_ends.size();
            dst_bytes.resize(dst_base + n_match);
            dst_ends.resize(old_ends + n_match);
            const uint8_t* __restrict src_bytes = col.VarcharBytes().data();
            uint8_t* __restrict out_bytes = dst_bytes.data() + dst_base;
            uint32_t* __restrict out_ends = dst_ends.data() + old_ends;
            for (size_t i = 0; i < n_match; ++i) {
              out_bytes[i] = src_bytes[match_rows[i]];
              out_ends[i] = dst_base + static_cast<uint32_t>(i + 1);
            }
          } else {
            auto& dst_bytes = batch->varchar_bytes[vslot];
            auto& dst_ends = batch->varchar_ends[vslot];
            for (size_t i = 0; i < n_match; ++i) {
              const std::string_view sv = col.VarcharAt(match_rows[i]);
              if (!sv.empty()) {
                dst_bytes.insert(dst_bytes.end(), sv.begin(), sv.end());
              }
              dst_ends.push_back(static_cast<uint32_t>(dst_bytes.size()));
            }
          }
        }
        count += n_match;
      }
      if (lstable_row_cursor_ >= blk->row_count) {
        ++lstable_page_cursor_;
        lstable_row_cursor_ = 0;
      }
      continue;
    }

    for (; lstable_row_cursor_ < blk->row_count && count < max_rows;
         ++lstable_row_cursor_) {
      const uint32_t r = lstable_row_cursor_;
      if (!LSTableRowMatches(*blk, r)) {
        continue;
      }
      const size_t base = count * stride;
      std::memset(batch->nulls.data() + base, 0, stride);
      for (size_t k = 0; k < kept_count; ++k) {
        const slot_t col_idx = typed_keep_slots_[k];
        const LSTableColumnBlock& col = blk->columns[col_idx];
        const size_t vslot = typed_varchar_slot_[k];
        if (col.IsNull(r)) {
          batch->nulls[base + k] = 1;
          if (vslot != SIZE_MAX) {
            batch->varchar_ends[vslot].push_back(
                static_cast<uint32_t>(batch->varchar_bytes[vslot].size()));
          } else {
            batch->cells[base + k] = 0;
          }
          continue;
        }
        if (vslot == SIZE_MAX) {
          batch->cells[base + k] = col.IntAt(r);
        } else {
          const std::string_view sv = col.VarcharAt(r);
          auto& dst_bytes = batch->varchar_bytes[vslot];
          dst_bytes.insert(dst_bytes.end(), sv.data(), sv.data() + sv.size());
          batch->varchar_ends[vslot].push_back(
              static_cast<uint32_t>(dst_bytes.size()));
        }
      }
      if (batch->capture_positions) {
        batch->positions.emplace_back(blk->page_id, blk->SlotAt(r));
      }
      ++count;
    }
    if (lstable_row_cursor_ >= blk->row_count) {
      ++lstable_page_cursor_;
      lstable_row_cursor_ = 0;
    }
  }
  SeekLSTableRow();
  batch->rows = count;
  return count;
}

const std::shared_ptr<const TlcsFile>& FullScanIterator::EnsureBackingTlcs()
    const {
  if (!backing_tlcs_checked_) {
    backing_tlcs_ = table_->GetBackingTlcsStore();
    backing_tlcs_checked_ = true;
  }
  return backing_tlcs_;
}

void FullScanIterator::ClearStubBlockState() {
  stub_block_idx_ = LSTableStore::kInvalidBlockIndex;
  stub_zone_maps_pass_ = false;
  stub_all_rows_match_ = -1;
  stub_current_pid_ = 0;
  stub_next_page_id_ = 0;
  stub_block_row_count_ = 0;
  stub_matches_computed_ = false;
  stub_match_cursor_ = 0;
  stub_match_rows_.clear();
  stub_row_materialized_ = true;
}

bool FullScanIterator::TryEnterStubPage() {
  if (lock_rows_ || page_ == nullptr ||
      !(*page_)->body.row_page.IsColumnarStub()) {
    return false;
  }
  if (projection_.has_value() && RowPlan() == nullptr) {
    return false;
  }
  const Schema& schema = table_->GetSchema();
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const ValueType type = schema.GetColumn(i).Type();
    if (schema.GetColumn(i).IsInterval() ||
        (type != ValueType::kInt64 && type != ValueType::kDate &&
         type != ValueType::kDouble && type != ValueType::kVarChar)) {
      return false;
    }
  }
  const auto& tlcs = EnsureBackingTlcs();
  if (tlcs == nullptr) {
    status_ = StatusError(StatusCode::kNotExists,
                          "stub page missing backing tlcs store");
    pos_.page_id = ~0ULL;
    FullScanIterator::DropPageLatch();
    ClearStubBlockState();
    return false;
  }
  const page_id_t pid = pos_.page_id;
  const page_id_t next_pid = (*page_)->body.row_page.next_page_id_;
  FullScanIterator::DropPageLatch();
  const uint32_t bidx = tlcs->FindBlockIndex(pid);
  if (bidx == LSTableStore::kInvalidBlockIndex) {
    status_ = StatusError(StatusCode::kCorrupt,
                          "stub page missing from backing tlcs directory");
    pos_.page_id = ~0ULL;
    ClearStubBlockState();
    return false;
  }
  StatusOr<uint32_t> rc = tlcs->BlockRowCount(bidx);
  if (!rc.HasValue()) {
    status_ = rc.GetStatus();
    pos_.page_id = ~0ULL;
    ClearStubBlockState();
    return false;
  }
  stub_block_idx_ = bidx;
  stub_zone_maps_pass_ = false;
  stub_all_rows_match_ = -1;
  stub_current_pid_ = pid;
  stub_next_page_id_ = next_pid;
  stub_block_row_count_ = rc.Value();
  stub_row_materialized_ = false;
  return true;
}

bool FullScanIterator::AdvanceFromStubPage() {
  const page_id_t predecessor = stub_current_pid_;
  const page_id_t saved_next = stub_next_page_id_;
  ClearStubBlockState();
  if (pages_) {
    ++page_index_;
    if (page_index_ < pages_->size()) {
      pos_ = RowPosition((*pages_)[page_index_], 0);
      return PinPage(pos_.page_id);
    }
    return false;
  }
  page_id_t next_page = saved_next;
  for (;;) {
    if (next_page == 0) {
      return false;
    }
    const Status visible = txn_->GetPageManager()->CheckTableTailPage(
        txn_->ID(), table_->first_pid_, next_page);
    if (visible == Status::kSuccess) {
      pos_ = RowPosition(next_page, 0);
      return PinPage(next_page);
    }
    if (txn_->GetPageManager()->TailPageInvisibleToSnapshot(
            *txn_, table_->first_pid_, next_page)) {
      return false;
    }
    const Status waited = txn_->GetPageManager()->WaitForTableTailPage(
        *txn_, table_->first_pid_, next_page);
    if (waited != Status::kSuccess) {
      status_ = waited;
      return false;
    }
    if (!PinPage(predecessor)) {
      return false;
    }
    next_page = (*page_)->body.row_page.next_page_id_;
    FullScanIterator::DropPageLatch();
  }
}

bool FullScanIterator::StubBlockAllRowsMatch(
    const TlcsFile& tlcs, uint32_t block_idx) const noexcept {
  if ((runtime_filter_ != nullptr || key_filter_ != nullptr) &&
      key_column_.has_value()) {
    return false;
  }
  if (peek_compares_ == nullptr || peek_compares_->empty()) {
    return true;
  }
  return tlcs.BlockAllRowsMatchPeek(block_idx, *peek_compares_);
}

void FullScanIterator::GatherStubFilterCols(bool include_keep_cols,
                                            std::vector<slot_t>* out) const {
  out->clear();
  const Schema& schema = table_->GetSchema();
  const slot_t ncols = schema.ColumnCount();
  if (include_keep_cols) {
    out->insert(out->end(), typed_keep_slots_.begin(), typed_keep_slots_.end());
  }
  if ((runtime_filter_ != nullptr || key_filter_ != nullptr) &&
      key_column_.has_value() && *key_column_ < ncols) {
    out->push_back(*key_column_);
  }
  if (peek_compares_ != nullptr) {
    for (const IntegerPeekCompare& pred : *peek_compares_) {
      if (pred.column < ncols) {
        out->push_back(pred.column);
      }
    }
  }
  std::sort(out->begin(), out->end());
  out->erase(std::unique(out->begin(), out->end()), out->end());
}

bool FullScanIterator::EnsureStubBlockColumns(
    uint32_t block_idx, page_id_t pid, std::span<const slot_t> columns) const {
  const auto& tlcs = EnsureBackingTlcs();
  if (tlcs == nullptr) {
    status_ = StatusError(StatusCode::kNotExists,
                          "stub page missing backing tlcs store");
    return false;
  }
  const slot_t ncols = table_->GetSchema().ColumnCount();
  constexpr slot_t kNoChunk = std::numeric_limits<slot_t>::max();
  if (stub_cache_ == nullptr) {
    stub_cache_ = std::make_unique<StubBlockCache>();
  }
  if (stub_cache_->col_chunk_idx.size() != ncols) {
    stub_cache_->col_chunk_idx.assign(ncols, kNoChunk);
    stub_cache_->block.pid = 0;
    stub_cache_->block.block_index = LSTableStore::kInvalidBlockIndex;
  }
  if (stub_cache_->block.pid != pid ||
      stub_cache_->block.block_index != block_idx) {
    StatusOr<uint32_t> rc = tlcs->BlockRowCount(block_idx);
    if (!rc.HasValue()) {
      status_ = rc.GetStatus();
      return false;
    }
    stub_cache_->block.block_index = block_idx;
    stub_cache_->block.pid = pid;
    stub_cache_->block.row_count = rc.Value();
    stub_cache_->block.column_ids.clear();
    for (auto& ch : stub_cache_->block.columns) {
      stub_cache_->free_chunks.push_back(std::move(ch));
    }
    stub_cache_->block.columns.clear();
    std::fill(stub_cache_->col_chunk_idx.begin(),
              stub_cache_->col_chunk_idx.end(), kNoChunk);
  }
  std::vector<slot_t> missing;
  missing.reserve(columns.size());
  for (const slot_t c : columns) {
    if (c < ncols && stub_cache_->col_chunk_idx[c] == kNoChunk) {
      missing.push_back(c);
    }
  }
  if (missing.empty()) {
    return true;
  }
  std::sort(missing.begin(), missing.end());
  missing.erase(std::unique(missing.begin(), missing.end()), missing.end());
  TlcsFile::DecodedBlock& delta = stub_cache_->delta;
  delta.column_ids.clear();
  delta.columns.clear();
  delta.columns.reserve(missing.size());
  for (size_t i = 0; i < missing.size() && !stub_cache_->free_chunks.empty();
       ++i) {
    delta.columns.push_back(std::move(stub_cache_->free_chunks.back()));
    stub_cache_->free_chunks.pop_back();
  }
  const Status ds = tlcs->DecodeBlockColumns(
      block_idx, missing, table_->GetSchema(), &delta,
      /*materialize_dict_ints=*/true, &stub_cache_->io_scratch);
  if (!ds.ok()) {
    status_ = ds;
    return false;
  }
  stub_cache_->block.row_count = delta.row_count;
  for (size_t i = 0; i < delta.column_ids.size(); ++i) {
    const slot_t cid = delta.column_ids[i];
    if (stub_cache_->block.columns.size() >=
        std::numeric_limits<slot_t>::max()) {
      status_ = StatusError(StatusCode::kCorrupt,
                            "columnar stub has too many columns");
      return false;
    }
    const auto idx = static_cast<slot_t>(stub_cache_->block.columns.size());
    stub_cache_->block.column_ids.push_back(cid);
    stub_cache_->block.columns.push_back(std::move(delta.columns[i]));
    if (cid < stub_cache_->col_chunk_idx.size()) {
      stub_cache_->col_chunk_idx[cid] = idx;
    }
  }
  return true;
}

// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (filterColumnAtATime_eq_row_at_a_time)
void FullScanIterator::ComputeStubBlockMatches() noexcept {
  stub_match_rows_.clear();
  stub_match_cursor_ = 0;
  stub_matches_computed_ = true;
  if (stub_cache_ == nullptr || stub_block_row_count_ == 0) {
    return;
  }
  const uint32_t rc = stub_block_row_count_;
  bool initialized = false;

  auto apply_pred = [&](auto&& fn) {
    if (!initialized) {
      stub_match_rows_.resize(rc);
      uint32_t* out = stub_match_rows_.data();
      size_t k = 0;
      for (uint32_t r = 0; r < rc; ++r) {
        out[k] = r;
        k += static_cast<size_t>(fn(r));
      }
      stub_match_rows_.resize(k);
      initialized = true;
    } else {
      uint32_t* out = stub_match_rows_.data();
      const size_t n = stub_match_rows_.size();
      size_t k = 0;
      for (size_t i = 0; i < n; ++i) {
        const uint32_t r = out[i];
        out[k] = r;
        k += static_cast<size_t>(fn(r));
      }
      stub_match_rows_.resize(k);
    }
  };

  auto run_runtime_filter = [&]() -> bool {
    if ((runtime_filter_ == nullptr && key_filter_ == nullptr) ||
        !key_column_.has_value()) {
      return true;
    }
    const slot_t col_idx = *key_column_;
    if (col_idx >= stub_cache_->col_chunk_idx.size()) {
      stub_match_rows_.clear();
      return false;
    }
    const slot_t chunk_idx = stub_cache_->col_chunk_idx[col_idx];
    if (chunk_idx >= stub_cache_->block.columns.size()) {
      stub_match_rows_.clear();
      return false;
    }
    const TlcsFile::DecodedColumnChunk& col =
        stub_cache_->block.columns[chunk_idx];
    if (col.type != ValueType::kInt64 && col.type != ValueType::kDate) {
      stub_match_rows_.clear();
      return false;
    }
    const int64_t* cells = col.int_cells.data();
    const bool no_null = !col.has_null;
    if (runtime_filter_ != nullptr && runtime_filter_->has_dense_bits()) {
      const auto lo_u = static_cast<uint64_t>(runtime_filter_->Lo());
      const auto span_u = static_cast<uint64_t>(runtime_filter_->Hi()) - lo_u;
      const uint64_t* bits = runtime_filter_->dense_bits_data();
      if (no_null) {
        apply_pred([cells, lo_u, span_u, bits](uint32_t r) noexcept {
          const uint64_t off = static_cast<uint64_t>(cells[r]) - lo_u;
          return off <= span_u &&
                 ((bits[off >> 6] >> (off & 63)) & uint64_t{1}) != 0;
        });
      } else {
        apply_pred([&col, cells, lo_u, span_u, bits](uint32_t r) noexcept {
          if (col.IsNull(r)) {
            return false;
          }
          const uint64_t off = static_cast<uint64_t>(cells[r]) - lo_u;
          return off <= span_u &&
                 ((bits[off >> 6] >> (off & 63)) & uint64_t{1}) != 0;
        });
      }
    } else if (runtime_filter_ != nullptr) {
      const RuntimeFilterGroup* rf = runtime_filter_;
      if (no_null) {
        apply_pred(
            [cells, rf](uint32_t r) noexcept { return !rf->Reject(cells[r]); });
      } else {
        apply_pred([&col, cells, rf](uint32_t r) noexcept {
          return !col.IsNull(r) && !rf->Reject(cells[r]);
        });
      }
    } else {
      const auto* kf = key_filter_;
      if (no_null) {
        apply_pred([cells, kf](uint32_t r) noexcept {
          return kf->contains(cells[r]);
        });
      } else {
        apply_pred([&col, cells, kf](uint32_t r) noexcept {
          return !col.IsNull(r) && kf->contains(cells[r]);
        });
      }
    }
    return !stub_match_rows_.empty();
  };

  auto run_peek_compares = [&]() -> bool {
    if (peek_compares_ == nullptr || peek_compares_->empty()) {
      return true;
    }
    for (const IntegerPeekCompare& pred : *peek_compares_) {
      if (pred.column >= stub_cache_->col_chunk_idx.size()) {
        stub_match_rows_.clear();
        return false;
      }
      const slot_t chunk_idx = stub_cache_->col_chunk_idx[pred.column];
      if (chunk_idx >= stub_cache_->block.columns.size()) {
        stub_match_rows_.clear();
        return false;
      }
      const TlcsFile::DecodedColumnChunk& col =
          stub_cache_->block.columns[chunk_idx];
      if (col.type != ValueType::kInt64 && col.type != ValueType::kDate) {
        stub_match_rows_.clear();
        return false;
      }
      const bool no_null = !col.has_null;
      const int64_t c = pred.constant;
      // Lean proof: filter_skip_always_true.
      if (no_null && col.has_min_max && !col.is_unsigned) {
        const int64_t lo = col.min_int;
        const int64_t hi = col.max_int;
        const bool always_true =
            (pred.op == BinaryOperation::kEquals && lo == c && hi == c) ||
            (pred.op == BinaryOperation::kNotEquals && (hi < c || c < lo)) ||
            (pred.op == BinaryOperation::kLessThan && hi < c) ||
            (pred.op == BinaryOperation::kLessThanEquals && hi <= c) ||
            (pred.op == BinaryOperation::kGreaterThan && lo > c) ||
            (pred.op == BinaryOperation::kGreaterThanEquals && lo >= c);
        if (always_true) {
          continue;
        }
      }
      const int64_t* cells = col.int_cells.data();
      auto run_op = [&](auto&& cmp_fn) {
        if (no_null) {
          apply_pred([cells, cmp_fn](uint32_t r) noexcept {
            return cmp_fn(cells[r]);
          });
        } else {
          apply_pred([&col, cells, cmp_fn](uint32_t r) noexcept {
            return !col.IsNull(r) && cmp_fn(cells[r]);
          });
        }
      };
      switch (pred.op) {
        case BinaryOperation::kEquals:
          run_op([c](int64_t v) noexcept { return v == c; });
          break;
        case BinaryOperation::kNotEquals:
          run_op([c](int64_t v) noexcept { return v != c; });
          break;
        case BinaryOperation::kLessThan:
          run_op([c](int64_t v) noexcept { return v < c; });
          break;
        case BinaryOperation::kLessThanEquals:
          run_op([c](int64_t v) noexcept { return v <= c; });
          break;
        case BinaryOperation::kGreaterThan:
          run_op([c](int64_t v) noexcept { return v > c; });
          break;
        case BinaryOperation::kGreaterThanEquals:
          run_op([c](int64_t v) noexcept { return v >= c; });
          break;
        default:
          run_op([op = pred.op, c](int64_t v) noexcept {
            return MatchIntegerCompare(v, op, c);
          });
          break;
      }
      if (stub_match_rows_.empty()) {
        return false;
      }
    }
    return true;
  };

  // Run dense runtime_filter_ FIRST when available (typically 0.5%–10%
  // selective on fact->dim joins) before peek_compares_.
  if (runtime_filter_ != nullptr && runtime_filter_->has_dense_bits() &&
      key_column_.has_value()) {
    if (!run_runtime_filter() || !run_peek_compares()) {
      return;
    }
  } else {
    if (!run_peek_compares() || !run_runtime_filter()) {
      return;
    }
  }

  if (!initialized) {
    stub_match_rows_.resize(rc);
    for (uint32_t r = 0; r < rc; ++r) {
      stub_match_rows_[r] = r;
    }
  }
}

bool FullScanIterator::StubRowMatches(uint32_t row) const noexcept {
  if (stub_cache_ == nullptr) {
    return false;
  }
  if ((runtime_filter_ != nullptr || key_filter_ != nullptr) &&
      key_column_.has_value()) {
    const slot_t col_idx = *key_column_;
    if (col_idx >= stub_cache_->col_chunk_idx.size()) {
      return false;
    }
    const slot_t chunk_idx = stub_cache_->col_chunk_idx[col_idx];
    if (chunk_idx >= stub_cache_->block.columns.size()) {
      return false;
    }
    const TlcsFile::DecodedColumnChunk& col =
        stub_cache_->block.columns[chunk_idx];
    if (col.IsNull(row) ||
        (col.type != ValueType::kInt64 && col.type != ValueType::kDate)) {
      return false;
    }
    const int64_t key = col.int_cells[row];
    const bool rejected = runtime_filter_ != nullptr
                              ? runtime_filter_->Reject(key)
                              : !key_filter_->contains(key);
    if (rejected) {
      return false;
    }
  }
  if (peek_compares_ != nullptr && !peek_compares_->empty()) {
    for (const IntegerPeekCompare& pred : *peek_compares_) {
      if (pred.column >= stub_cache_->col_chunk_idx.size()) {
        return false;
      }
      const slot_t chunk_idx = stub_cache_->col_chunk_idx[pred.column];
      if (chunk_idx >= stub_cache_->block.columns.size()) {
        return false;
      }
      const TlcsFile::DecodedColumnChunk& col =
          stub_cache_->block.columns[chunk_idx];
      if (col.IsNull(row) ||
          (col.type != ValueType::kInt64 && col.type != ValueType::kDate)) {
        return false;
      }
      if (!MatchIntegerCompare(col.int_cells[row], pred.op, pred.constant)) {
        return false;
      }
    }
  }
  return true;
}

void FullScanIterator::EnsureStubRowMaterialized() const {
  if (stub_row_materialized_ ||
      stub_block_idx_ == LSTableStore::kInvalidBlockIndex) {
    return;
  }
  if (!pos_.IsValid() || pos_.slot >= stub_block_row_count_) {
    current_row_.Clear();
    stub_row_materialized_ = true;
    return;
  }
  const Schema& schema = table_->GetSchema();
  std::vector<slot_t> cols;
  if (projection_.has_value()) {
    cols = *projection_;
  } else {
    cols.resize(schema.ColumnCount());
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      cols[c] = c;
    }
  }
  if (!EnsureStubBlockColumns(stub_block_idx_, pos_.page_id, cols)) {
    current_row_.Clear();
    stub_row_materialized_ = true;
    return;
  }
  const uint32_t r = pos_.slot;
  current_row_.values_.resize(cols.size());
  for (size_t k = 0; k < cols.size(); ++k) {
    const slot_t col_idx = cols[k];
    const slot_t chunk_idx = stub_cache_->col_chunk_idx[col_idx];
    const TlcsFile::DecodedColumnChunk& col =
        stub_cache_->block.columns[chunk_idx];
    if (col.IsNull(r)) {
      current_row_.values_[k] = Value();
      continue;
    }
    switch (col.type) {
      case ValueType::kInt64:
        current_row_.values_[k] = Value(col.int_cells[r]);
        if (col.is_unsigned) {
          current_row_.values_[k].SetUnsigned();
        }
        break;
      case ValueType::kDate:
        current_row_.values_[k] = Value::DateFromDays(col.int_cells[r]);
        break;
      case ValueType::kDouble: {
        double d = 0.0;
        std::memcpy(&d, &col.int_cells[r], sizeof(d));
        current_row_.values_[k] = Value(d);
        break;
      }
      case ValueType::kVarChar:
        current_row_.values_[k] = Value(std::string(col.VarcharAt(r)));
        break;
      default:
        current_row_.values_[k] = Value();
        break;
    }
  }
  stub_row_materialized_ = true;
}

size_t FullScanIterator::DecodeStubBlockBatch(TypedCellBatch* batch,
                                              size_t count_offset,
                                              size_t max_rows) {
  if (stub_block_idx_ == LSTableStore::kInvalidBlockIndex || max_rows == 0) {
    return 0;
  }
  const auto& tlcs = EnsureBackingTlcs();
  if (tlcs == nullptr) {
    status_ = StatusError(StatusCode::kNotExists,
                          "stub page missing backing tlcs store");
    pos_.page_id = ~0ULL;
    ClearStubBlockState();
    return 0;
  }
  if (!tlcs->BlockMatchesZoneMaps(stub_block_idx_, peek_compares_,
                                  runtime_filter_, key_column_) ||
      pos_.slot >= stub_block_row_count_) {
    if (!AdvanceFromStubPage()) {
      pos_.page_id = ~0ULL;
    }
    return 0;
  }
  const bool all_match = StubBlockAllRowsMatch(*tlcs, stub_block_idx_);
  if (!all_match) {
    // Two-stage lazy decode: decode ONLY filter columns first and evaluate
    // column-at-a-time; skip decoding payload columns on 0-match blocks.
    if (!stub_matches_computed_) {
      std::vector<slot_t> filter_cols;
      GatherStubFilterCols(/*include_keep_cols=*/false, &filter_cols);
      if (!EnsureStubBlockColumns(stub_block_idx_, pos_.page_id, filter_cols)) {
        pos_.page_id = ~0ULL;
        ClearStubBlockState();
        return 0;
      }
      ComputeStubBlockMatches();
    }
    while (stub_match_cursor_ < stub_match_rows_.size() &&
           stub_match_rows_[stub_match_cursor_] < pos_.slot) {
      ++stub_match_cursor_;
    }
    if (stub_match_cursor_ >= stub_match_rows_.size()) {
      if (!AdvanceFromStubPage()) {
        pos_.page_id = ~0ULL;
      }
      return 0;
    }
    if (!EnsureStubBlockColumns(stub_block_idx_, pos_.page_id,
                                typed_keep_slots_)) {
      pos_.page_id = ~0ULL;
      ClearStubBlockState();
      return 0;
    }
  } else {
    if (!EnsureStubBlockColumns(stub_block_idx_, pos_.page_id,
                                typed_keep_slots_)) {
      pos_.page_id = ~0ULL;
      ClearStubBlockState();
      return 0;
    }
  }
  const size_t stride = batch->stride;
  const size_t kept_count = typed_keep_slots_.size();
  bool kept_has_null = false;
  for (size_t k = 0; k < kept_count; ++k) {
    const slot_t col_idx = typed_keep_slots_[k];
    const TlcsFile::DecodedColumnChunk& chunk =
        stub_cache_->block.columns[stub_cache_->col_chunk_idx[col_idx]];
    if (chunk.has_null) {
      kept_has_null = true;
      break;
    }
  }
  size_t emitted = 0;
  if (!kept_has_null && !batch->capture_positions && all_match) {
    const uint32_t r0 = pos_.slot;
    const size_t take = std::min<size_t>(stub_block_row_count_ - r0, max_rows);
    std::memset(batch->nulls.data() + (count_offset * stride), 0,
                take * stride);
    for (size_t k = 0; k < kept_count; ++k) {
      const slot_t col_idx = typed_keep_slots_[k];
      const TlcsFile::DecodedColumnChunk& col =
          stub_cache_->block.columns[stub_cache_->col_chunk_idx[col_idx]];
      const size_t vslot = typed_varchar_slot_[k];
      if (vslot == SIZE_MAX) {
        const int64_t* __restrict src = col.int_cells.data() + r0;
        if (stride == 1) {
          std::memcpy(batch->cells.data() + count_offset, src,
                      take * sizeof(int64_t));
        } else {
          int64_t* __restrict dst =
              batch->cells.data() + (count_offset * stride) + k;
          for (size_t r = 0; r < take; ++r) {
            dst[r * stride] = src[r];
          }
        }
      } else if (!col.dict_encoded && col.varchar_ends.size() >= r0 + take) {
        auto& dst_bytes = batch->varchar_bytes[vslot];
        auto& dst_ends = batch->varchar_ends[vslot];
        const uint32_t src_byte_begin = r0 == 0 ? 0U : col.varchar_ends[r0 - 1];
        const uint32_t src_byte_end = col.varchar_ends[r0 + take - 1];
        const auto dst_base = static_cast<uint32_t>(dst_bytes.size());
        if (src_byte_end > src_byte_begin) {
          dst_bytes.insert(dst_bytes.end(),
                           col.varchar_bytes.data() + src_byte_begin,
                           col.varchar_bytes.data() + src_byte_end);
        }
        const uint32_t offset_delta = dst_base - src_byte_begin;
        const uint32_t* __restrict src_ends = col.varchar_ends.data() + r0;
        const size_t old_ends = dst_ends.size();
        dst_ends.resize(old_ends + take);
        uint32_t* __restrict out_ends = dst_ends.data() + old_ends;
        for (size_t r = 0; r < take; ++r) {
          out_ends[r] = src_ends[r] + offset_delta;
        }
      } else {
        auto& dst_bytes = batch->varchar_bytes[vslot];
        auto& dst_ends = batch->varchar_ends[vslot];
        for (size_t r = 0; r < take; ++r) {
          const std::string_view sv = col.VarcharAt(r0 + r);
          dst_bytes.insert(dst_bytes.end(), sv.data(), sv.data() + sv.size());
          dst_ends.push_back(static_cast<uint32_t>(dst_bytes.size()));
        }
      }
    }
    emitted = take;
    pos_.slot = static_cast<slot_t>(r0 + take);
  } else if (!kept_has_null && !batch->capture_positions) {
    const size_t avail = stub_match_rows_.size() - stub_match_cursor_;
    const size_t n_match = std::min<size_t>(max_rows, avail);
    const uint32_t* match_rows = stub_match_rows_.data() + stub_match_cursor_;
    stub_match_cursor_ += n_match;
    pos_.slot = stub_match_cursor_ < stub_match_rows_.size()
                    ? static_cast<slot_t>(stub_match_rows_[stub_match_cursor_])
                    : static_cast<slot_t>(stub_block_row_count_);
    if (n_match > 0) {
      std::memset(batch->nulls.data() + (count_offset * stride), 0,
                  n_match * stride);
      for (size_t k = 0; k < kept_count; ++k) {
        const slot_t col_idx = typed_keep_slots_[k];
        const TlcsFile::DecodedColumnChunk& col =
            stub_cache_->block.columns[stub_cache_->col_chunk_idx[col_idx]];
        const size_t vslot = typed_varchar_slot_[k];
        if (vslot == SIZE_MAX) {
          const int64_t* __restrict src = col.int_cells.data();
          int64_t* __restrict dst =
              batch->cells.data() + (count_offset * stride) + k;
          for (size_t i = 0; i < n_match; ++i) {
            dst[i * stride] = src[match_rows[i]];
          }
        } else {
          auto& dst_bytes = batch->varchar_bytes[vslot];
          auto& dst_ends = batch->varchar_ends[vslot];
          for (size_t i = 0; i < n_match; ++i) {
            const std::string_view sv = col.VarcharAt(match_rows[i]);
            dst_bytes.insert(dst_bytes.end(), sv.data(), sv.data() + sv.size());
            dst_ends.push_back(static_cast<uint32_t>(dst_bytes.size()));
          }
        }
      }
      emitted = n_match;
    }
  } else {
    uint32_t r = pos_.slot;
    for (; r < stub_block_row_count_ && emitted < max_rows; ++r) {
      if (!all_match && !StubRowMatches(r)) {
        continue;
      }
      const size_t base = (count_offset + emitted) * stride;
      std::memset(batch->nulls.data() + base, 0, stride);
      for (size_t k = 0; k < kept_count; ++k) {
        const slot_t col_idx = typed_keep_slots_[k];
        const TlcsFile::DecodedColumnChunk& col =
            stub_cache_->block.columns[stub_cache_->col_chunk_idx[col_idx]];
        const size_t vslot = typed_varchar_slot_[k];
        if (col.IsNull(r)) {
          batch->nulls[base + k] = 1;
          if (vslot != SIZE_MAX) {
            batch->varchar_ends[vslot].push_back(
                static_cast<uint32_t>(batch->varchar_bytes[vslot].size()));
          } else {
            batch->cells[base + k] = 0;
          }
          continue;
        }
        if (vslot == SIZE_MAX) {
          batch->cells[base + k] = col.int_cells[r];
        } else {
          const std::string_view sv = col.VarcharAt(r);
          auto& dst_bytes = batch->varchar_bytes[vslot];
          dst_bytes.insert(dst_bytes.end(), sv.data(), sv.data() + sv.size());
          batch->varchar_ends[vslot].push_back(
              static_cast<uint32_t>(dst_bytes.size()));
        }
      }
      if (batch->capture_positions) {
        batch->positions.emplace_back(pos_.page_id, static_cast<slot_t>(r));
      }
      ++emitted;
    }
    pos_.slot = static_cast<slot_t>(r);
  }
  if (pos_.slot >= stub_block_row_count_) {
    if (!AdvanceFromStubPage()) {
      pos_.page_id = ~0ULL;
    }
  }
  return emitted;
}

bool FullScanIterator::IsValid() const { return pos_.IsValid(); }

const Row& FullScanIterator::operator*() const {
  EnsureLSTableRowMaterialized();
  EnsureStubRowMaterialized();
  return current_row_;
}
Row& FullScanIterator::operator*() {
  EnsureLSTableRowMaterialized();
  EnsureStubRowMaterialized();
  return current_row_;
}

void FullScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->schema_.Name();
  if (runtime_filter_ != nullptr && runtime_filter_column_.has_value()) {
    o << " (runtime filter: " << runtime_filter_->Describe() << " on column "
      << *runtime_filter_column_ << ")";
  }
}

}  // namespace tinylamb
