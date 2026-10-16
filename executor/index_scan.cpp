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

//
// Created by kumagi on 22/06/19.
//

#include "index_scan.hpp"

#include <cstddef>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// PrefetchingIndexScan: how many entries the look-ahead cursor runs ahead of
// the main iterator.  One prefetch per main step keeps the lead constant, so
// the page the scan is about to need is fetched while the previous row is
// still being processed -- the I/O is grouped ahead of the consumer instead
// of interleaved one page at a time.  A/B hatch:
// TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH=1.
inline constexpr size_t kIndexScanPrefetchDistance = 8;

bool IndexScanPrefetchDisabled() {
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH") != nullptr;
  return disabled;
}

TransactionManager::IndexReadGuard AcquireScanIndexReadGuard(
    Transaction& txn, const Index& index, bool lock_rows,
    TransactionManager::IndexReadGuard index_read_guard) {
  if (index_read_guard.HoldsLock()) {
    return index_read_guard;
  }
  if (lock_rows) {
    return {};
  }
  return txn.AcquireIndexReadGuard(index.Root());
}

}  // namespace

IndexScan::IndexScan(Transaction& txn, const Table& table, const Index& index,
                     const Value& begin, const Value& end, bool ascending,
                     Expression where, const Schema& sc, bool lock_rows,
                     bool wait_for_write_intent,
                     TransactionManager::IndexReadGuard index_read_guard)
    : IndexScan(
          txn, table, index,
          begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
          end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
          ascending, std::move(where), sc, lock_rows, wait_for_write_intent,
          std::move(index_read_guard)) {}

IndexScan::IndexScan(Transaction& txn, const Table& table, const Index& index,
                     const std::vector<Value>& begin_key,
                     const std::vector<Value>& end_key, bool ascending,
                     Expression where, Schema sc, bool lock_rows,
                     bool wait_for_write_intent,
                     TransactionManager::IndexReadGuard index_read_guard)
    : txn_(txn),
      table_(table),
      index_(index),
      ascending_(ascending),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      index_read_guard_(AcquireScanIndexReadGuard(txn, index, lock_rows,
                                                  std::move(index_read_guard))),
      iter_(new IndexScanIterator(table, index, txn, begin_key, end_key,
                                  ascending, lock_rows, &index_read_guard_)),
      cond_(std::move(where)),
      cond_conjuncts_(SplitConjuncts(cond_)),
      schema_(std::move(sc)) {
  OpenPrefetch(begin_key, end_key);
}

IndexScan::IndexScan(
    Transaction& txn, const Table& table, const Index& index,
    std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges,
    bool ascending, Expression where, Schema sc, bool lock_rows,
    bool wait_for_write_intent,
    TransactionManager::IndexReadGuard index_read_guard)
    : txn_(txn),
      table_(table),
      index_(index),
      ascending_(ascending),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      index_read_guard_(AcquireScanIndexReadGuard(txn, index, lock_rows,
                                                  std::move(index_read_guard))),
      // Contract: at least one range. Callers build one entry per distinct
      // IN value.
      iter_(new IndexScanIterator(
          table, index, txn,
          ranges.empty() ? std::vector<Value>{} : ranges.front().first,
          ranges.empty() ? std::vector<Value>{} : ranges.front().second,
          ascending, lock_rows, &index_read_guard_)),
      cond_(std::move(where)),
      cond_conjuncts_(SplitConjuncts(cond_)),
      schema_(std::move(sc)) {
  range_count_ = ranges.size();
  if (!ranges.empty()) {
    pending_.assign(std::make_move_iterator(ranges.begin()) + 1,
                    std::make_move_iterator(ranges.end()));
    OpenPrefetch(ranges.front().first, ranges.front().second);
  }
}

void IndexScan::OpenRange(const std::vector<Value>& begin_key,
                          const std::vector<Value>& end_key) {
  iter_ = Iterator(new IndexScanIterator(table_, index_, txn_, begin_key,
                                         end_key, ascending_, lock_rows_,
                                         &index_read_guard_));
  OpenPrefetch(begin_key, end_key);
}

void IndexScan::OpenPrefetch(const std::vector<Value>& begin_key,
                             const std::vector<Value>& end_key) {
  prefetch_iter_.reset();
  if (IndexScanPrefetchDisabled()) {
    return;
  }
  prefetch_iter_ = std::make_unique<IndexScanIterator>(
      table_, index_, txn_, begin_key, end_key, ascending_, lock_rows_,
      &index_read_guard_);
  for (size_t i = 0;
       i < kIndexScanPrefetchDistance && prefetch_iter_->IsValid(); ++i) {
    AdvancePrefetch();
  }
}

void IndexScan::AdvancePrefetch() {
  if (prefetch_iter_ == nullptr || !prefetch_iter_->IsValid()) {
    return;
  }
  ++(*prefetch_iter_);
  if (!prefetch_iter_->IsValid()) {
    return;
  }
  const RowPosition ahead = prefetch_iter_->Position();
  if (!ahead.IsValid()) {
    return;
  }
  // Best-effort warm: fetch and immediately release the row page so the main
  // iterator's own fetch becomes a buffer-pool hit.  A failed warm is
  // ignored; the main path re-fetches and surfaces the error with its own
  // status.
  StatusOr<PageRef> warm = txn_.GetPageManager()->GetPage(ahead.page_id, true);
  if (warm.HasValue()) {
    txn_.RecordIndexScanPrefetch();
  }
}

void IndexScan::StepIterators() {
  ++iter_;
  AdvancePrefetch();
}

void IndexScan::ReleaseIndexReadGuard() {
  index_read_guard_ = TransactionManager::IndexReadGuard();
}

bool IndexScan::Next(Row* dst, RowPosition* rp) {
  for (;;) {
    while (!iter_.IsValid()) {
      // A range iterator that stopped early latched the reason (page-pool /
      // IO failure) in GetStatus(); advancing to the next pending range
      // would silently truncate the result set (table/iterator_base.hpp).
      const Status status = iter_.GetStatus();
      if (status != Status::kSuccess) {
        ReleaseIndexReadGuard();
        return FailWith(status);
      }
      if (pending_offset_ == pending_.size()) {
        ReleaseIndexReadGuard();
        return false;
      }
      auto range = std::move(pending_[pending_offset_++]);
      OpenRange(range.first, range.second);
    }
    const RowPosition pointed_row = iter_.Position();
    const bool already_reserved = lock_rows_ && txn_.HasWriteSet(pointed_row);
    txn_.DebugSetPhase("indexscan-lock");
    const bool locked = !lock_rows_ || (wait_for_write_intent_
                                            ? txn_.AddWriteSet(pointed_row)
                                            : txn_.TryAddWriteSet(pointed_row));
    txn_.DebugSetPhase(nullptr);
    if (!locked) {
      // The bare "timed out" text predated the distinct failure reasons and
      // misattributed deadlock-victim wounds (the common case) to the 5s
      // wait limit, sending postmortems down the wrong path.  This value is
      // also the conservative default for any failure kind added later.
      const char* reason = "conflicting writer holds the write intent";
      using Failure = Transaction::IntentWaitFailure;
      switch (txn_.LastIntentWaitFailure()) {
        case Failure::kWounded:
          reason = "aborted as a deadlock/wound victim";
          break;
        case Failure::kHolderAborting:
          reason = "intent holder is aborting (rolled back)";
          break;
        case Failure::kTimeout:
          reason = "wait timed out (latch-cycle escape hatch)";
          break;
        case Failure::kFailStopped:
          reason = "engine is fail-stopped";
          break;
        case Failure::kConflict:
        case Failure::kNone:
          break;
      }
      return FailWith(StatusError(StatusCode::kConflicts,
                                  "write intent unavailable on table " +
                                      std::string(table_.GetSchema().Name()) +
                                      ": " + reason));
    }
    if (cond_ || lock_rows_) {
      *dst = *iter_;
    } else {
      *dst = std::move(*iter_);
    }
    StepIterators();
    if (!dst->IsValid()) {
      if (lock_rows_ && !already_reserved) {
        txn_.ReleaseWriteIntent(pointed_row);
      }
      continue;
    }
    if (rp != nullptr) {
      *rp = pointed_row;
    }
    if (cond_) {
      StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
          cond_conjuncts_, [&](const Expression& conjunct) {
            return conjunct->TryEvaluate(*dst, schema_);
          });
      if (!pass.HasValue()) {
        if (lock_rows_ && !already_reserved) {
          txn_.ReleaseWriteIntent(pointed_row);
        }
        return FailWith(pass.GetStatus());
      }
      if (!pass.Value()) {
        if (lock_rows_ && !already_reserved) {
          txn_.ReleaseWriteIntent(pointed_row);
        }
        continue;
      }
    }
    return true;
  }
}

void IndexScan::Dump(std::ostream& o, int /*indent*/) const {
  if (range_count_ > 1) {
    o << "IndexScan: " << table_.GetSchema().Name() << " x" << range_count_
      << " points";
  } else {
    o << "IndexScan: " << iter_;
  }
  if (!ascending_) {
    o << " reverse";
  }
  if (cond_) {
    o << " WHERE " << *cond_;
  }
}

}  // namespace tinylamb
namespace tinylamb {

void IndexSkipScanExecutor::Dump(std::ostream& o, int indent) const {
  o << "IndexSkipScan\n" << std::string(static_cast<size_t>(indent) + 2, ' ');
  inner_->Dump(o, indent + 2);
}

}  // namespace tinylamb
