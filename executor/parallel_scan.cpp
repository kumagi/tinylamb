/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <mutex>
#include <new>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/scan_filter.hpp"
#include "page/row_position.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void ParallelScan::SetRowFilter(
    std::vector<Expression> conjuncts,
    relational_detail::CompiledScanFilter filter,
    relational_detail::WorkerScanFilterPrograms programs) {
  if (!projection_) {
    const auto peeks = relational_detail::BuildIntegerPeeks(
        filter, nullptr, table_->GetSchema());
    peek_compares_.insert(peek_compares_.end(), peeks.begin(), peeks.end());
  }
  row_filter_conjuncts_ = std::move(conjuncts);
  row_filter_ = std::make_unique<relational_detail::CompiledScanFilter>(
      std::move(filter));
  row_filter_programs_ =
      std::make_unique<relational_detail::WorkerScanFilterPrograms>(
          std::move(programs));
}

ParallelScan::ParallelScan(Transaction& txn, const Table& table,
                           size_t worker_count, size_t pages_per_morsel,
                           std::optional<std::vector<slot_t>> projection,
                           std::vector<IntegerPeekCompare> peek_compares,
                           bool ordered)
    : txn_(&txn),
      table_(&table),
      projection_(std::move(projection)),
      peek_compares_(std::move(peek_compares)),
      ordered_(ordered),
      worker_count_(1),
      max_ready_chunks_(2) {
  StatusOr<TableLease> lease = table_->AcquireLease(txn_->ID());
  if (!lease.HasValue()) {
    worker_status_ = lease.GetStatus();
    return;
  }
  table_lease_ = lease.MoveValue();
  if ((txn_->IsReadOnly() || txn_->WriteSetEmpty()) &&
      !txn_->HasAnyPageVersionChains()) {
    std::vector<slot_t> needed;
    if (projection_) {
      needed = *projection_;
      for (const auto& compare : peek_compares_) {
        needed.push_back(compare.column);
      }
    }
    lstable_store_ = GetOrBuildLSTableStore(*txn_, *table_, true,
                                            projection_ ? &needed : nullptr);
    if (lstable_store_ != nullptr &&
        (lstable_store_->uncommitted_owner_txn != 0 ||
         lstable_store_->layout_generation != table_->LayoutGeneration() ||
         lstable_store_->first_pid != table_->FirstPageId())) {
      lstable_store_.reset();
    }
  }
  // A narrow cached snapshot still proves the page layout. It can supply
  // morsels without pretending that unrequested columns were decoded.
  auto scheduling_store = lstable_store_;
  if (!scheduling_store && (txn_->IsReadOnly() || txn_->WriteSetEmpty()) &&
      !txn_->HasAnyPageVersionChains()) {
    scheduling_store = table_->GetProjectedLSTableStore();
    if (scheduling_store &&
        (scheduling_store->uncommitted_owner_txn != 0 ||
         scheduling_store->layout_generation != table_->LayoutGeneration() ||
         scheduling_store->first_pid != table_->FirstPageId())) {
      scheduling_store.reset();
    }
  }
  if (scheduling_store != nullptr) {
    const size_t step = std::max<size_t>(1, pages_per_morsel);
    morsels_.reserve((scheduling_store->page_ids.size() + step - 1) / step);
    for (size_t i = 0; i < scheduling_store->page_ids.size(); i += step) {
      const size_t end = std::min(scheduling_store->page_ids.size(), i + step);
      Table::ScanMorsel morsel(
          scheduling_store->page_ids.begin() + static_cast<ptrdiff_t>(i),
          scheduling_store->page_ids.begin() + static_cast<ptrdiff_t>(end));
      morsels_.push_back(std::move(morsel));
    }
  } else {
    morsels_ = table_->BuildScanMorsels(*txn_, pages_per_morsel);
  }
  morsels_done_.assign(morsels_.size(), false);
  worker_count_ = std::min(QueryParallelWorkerLimit(worker_count),
                           std::max<size_t>(1, morsels_.size()));
  max_ready_chunks_ = std::max<size_t>(2, worker_count_ * 2);
}

ParallelScan::~ParallelScan() {
  {
    std::scoped_lock lock(mutex_);
    cancelled_.store(true, std::memory_order_release);
  }
  ready_cv_.notify_all();
  space_cv_.notify_all();
  // jthread joins in its destructor.  Join while the mutexes and condition
  // variables used by workers are still alive.
  workers_.clear();
}

bool ParallelScan::RejectedByIntegerMembership() const {
  return lstable_store_ != nullptr && !txn_->HasAnyPageVersionChains() &&
         lstable_store_->layout_generation == table_->LayoutGeneration() &&
         std::ranges::any_of(peek_compares_, [&](const auto& compare) {
           return compare.op == BinaryOperation::kEquals &&
                  !lstable_store_->MayContainInteger(compare.column,
                                                     compare.constant);
         });
}

StatusOr<bool> ParallelScan::TryFoldTypedMorsels(
    const std::function<Status(size_t, const TypedCellBatch&)>& consume,
    bool capture_positions) {
  if (started_ || row_filter_ != nullptr) {
    return false;
  }
  const auto supported = [](ValueType type) {
    return type == ValueType::kInt64 || type == ValueType::kDate ||
           type == ValueType::kDouble;
  };
  if (projection_) {
    if (projection_->empty()) {
      return false;
    }
    for (slot_t slot : *projection_) {
      if (slot >= table_->GetSchema().ColumnCount() ||
          !supported(table_->GetSchema().GetColumn(slot).Type())) {
        return false;
      }
    }
  } else {
    for (size_t slot = 0; slot < table_->GetSchema().ColumnCount(); ++slot) {
      if (!supported(table_->GetSchema().GetColumn(slot).Type())) {
        return false;
      }
    }
  }
  if (worker_status_ != Status::kSuccess) {
    return worker_status_;
  }
  started_ = true;
  typed_folded_ = true;
  if (RejectedByIntegerMembership()) {
    return true;
  }
  std::atomic<size_t> next{0};
  std::atomic<bool> failed{false};
  Status error = Status::kSuccess;
  std::mutex error_mutex;
  const auto fail = [&](Status status) {
    std::scoped_lock lock(error_mutex);
    if (!failed.load(std::memory_order_relaxed)) {
      error = std::move(status);
      failed.store(true, std::memory_order_release);
    }
  };
  const auto run = [&] {
    try {
      TypedCellBatch batch;
      batch.capture_positions = capture_positions;
      while (!failed.load(std::memory_order_acquire)) {
        const size_t index = next.fetch_add(1, std::memory_order_relaxed);
        if (index >= morsels_.size()) {
          break;
        }
        Iterator iterator = table_->BeginMorselScan(
            *txn_, morsels_[index], projection_, nullptr, std::nullopt,
            peek_compares_.empty() ? nullptr : &peek_compares_);
        while (!failed.load(std::memory_order_acquire) &&
               iterator.DecodeNextTypedBatch(&batch, kDefaultVectorSize) != 0) {
          // Batches own all decoded bytes; do not hold a page latch across a
          // consumer callback, which may need other pages or shared state.
          iterator.DropPageLatch();
          if (iterator.GetStatus() != Status::kSuccess) {
            fail(iterator.GetStatus());
            break;
          }
          const Status status = consume(index, batch);
          if (status != Status::kSuccess) {
            fail(status);
            break;
          }
        }
        if (iterator.GetStatus() != Status::kSuccess) {
          fail(iterator.GetStatus());
        } else if (iterator.IsValid() &&
                   !failed.load(std::memory_order_acquire)) {
          fail(StatusError(StatusCode::kRuntimeError,
                           "typed scan stopped before exhaustion"));
        }
      }
    } catch (const std::exception& exception) {
      fail(StatusError(StatusCode::kRuntimeError, exception.what()));
    } catch (...) {
      fail(StatusError(StatusCode::kRuntimeError, "typed scan worker failed"));
    }
  };
  std::vector<std::jthread> workers;
  try {
    // A few thousand fixed-width rows cost less than starting helper threads.
    // The SQL worker already supplies parallelism across concurrent queries.
    const size_t fold_workers = morsels_.size() <= 4 ? 1 : worker_count_;
    workers.reserve(fold_workers - 1);
    for (size_t i = 1; i < fold_workers; ++i) {
      workers.emplace_back(run);
    }
    run();
  } catch (const std::exception& exception) {
    fail(StatusError(StatusCode::kRuntimeError, exception.what()));
  }
  workers.clear();
  if (failed.load(std::memory_order_acquire)) {
    FailWith(error);
    return error;
  }
  return true;
}

StatusOr<bool> ParallelScan::TryFoldLSTableStore(
    const std::function<Status(const LSTableStore&)>& consume) {
  if (started_ || lstable_store_ == nullptr || row_filter_ != nullptr ||
      !peek_compares_.empty() || txn_->HasAnyPageVersionChains() ||
      !txn_->WriteSetEmpty() || lstable_store_->uncommitted_owner_txn != 0 ||
      lstable_store_->layout_generation != table_->LayoutGeneration() ||
      lstable_store_->first_pid != table_->FirstPageId()) {
    return false;
  }
  if (worker_status_ != Status::kSuccess) {
    return worker_status_;
  }
  if (projection_ && std::ranges::any_of(*projection_, [&](slot_t slot) {
        return slot >= table_->GetSchema().ColumnCount();
      })) {
    return false;
  }
  started_ = true;
  typed_folded_ = true;
  Status status;
  try {
    status = consume(*lstable_store_);
  } catch (const std::exception& exception) {
    status = StatusError(StatusCode::kRuntimeError, exception.what());
  } catch (...) {
    status = StatusError(StatusCode::kRuntimeError, "column store fold failed");
  }
  if (status != Status::kSuccess) {
    FailWith(status);
    return status;
  }
  return true;
}

StatusOr<bool> ParallelScan::TryFoldLSTableBlocks(
    size_t worker_count,
    const std::function<Status(size_t, const LSTablePageBlock&, const uint32_t*,
                               size_t)>& consume,
    const std::vector<slot_t>* required_columns) try {
  if (started_ || txn_->HasAnyPageVersionChains()) {
    return false;
  }
  if (worker_status_ != Status::kSuccess) {
    return worker_status_;
  }
  if (projection_) {
    for (slot_t slot : *projection_) {
      if (slot >= table_->GetSchema().ColumnCount()) {
        return false;
      }
    }
  }
  const bool can_inline_simple =
      row_filter_ != nullptr && !row_filter_->compile_decided_reject &&
      row_filter_->disjunctive_branches.empty() && !projection_;
  if (row_filter_ != nullptr && !row_filter_->compile_decided_reject) {
    if (!can_inline_simple || !row_filter_->prefix_in.empty() ||
        !row_filter_->prebuilt_membership.empty() ||
        !row_filter_->prebuilt_scalar_compare.empty() ||
        !row_filter_->residual.empty()) {
      return false;
    }
    for (const auto& pred : row_filter_->simple) {
      if (pred.column_payload ||
          pred.column >= table_->GetSchema().ColumnCount()) {
        return false;
      }
      const auto& col_def = table_->GetSchema().GetColumn(pred.column);
      const bool ok_double =
          col_def.Type() == ValueType::kDouble &&
          (pred.double_payload ||
           (pred.int_payload && !pred.constant.IsUnsigned() &&
            pred.int_constant >= -(int64_t{1} << 53) &&
            pred.int_constant <= (int64_t{1} << 53)));
      const bool ok_int = (col_def.Type() == ValueType::kInt64 ||
                           col_def.Type() == ValueType::kDate) &&
                          !col_def.IsUnsigned() && pred.int_payload &&
                          !pred.constant.IsUnsigned();
      const bool ok_string =
          col_def.Type() == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar &&
          !pred.constant.IsCaseInsensitive() &&
          (pred.plain_string_payload || pred.op == BinaryOperation::kLike ||
           pred.op == BinaryOperation::kNotLike);
      if (!ok_double && !ok_int && !ok_string) {
        return false;
      }
    }
  }
  if (!lstable_store_ && required_columns != nullptr &&
      (txn_->IsReadOnly() || txn_->WriteSetEmpty())) {
    std::vector<slot_t> needed = *required_columns;
    for (const auto& pred : peek_compares_) {
      needed.push_back(pred.column);
    }
    if (row_filter_) {
      for (const auto& pred : row_filter_->simple) {
        needed.push_back(static_cast<slot_t>(pred.column));
      }
    }
    auto store = GetOrBuildLSTableStore(*txn_, *table_, true, &needed);
    if (store && store->uncommitted_owner_txn == 0 &&
        store->layout_generation == table_->LayoutGeneration() &&
        store->first_pid == table_->FirstPageId() &&
        !txn_->HasAnyPageVersionChains()) {
      lstable_store_ = std::move(store);
    }
  }
  if (!lstable_store_) {
    return false;
  }
  started_ = true;
  typed_folded_ = true;
  if (RejectedByIntegerMembership() ||
      (row_filter_ != nullptr && row_filter_->compile_decided_reject)) {
    return true;
  }
  const size_t total_blocks = lstable_store_->blocks.size();
  if (total_blocks == 0) {
    return true;
  }
  const size_t fold_workers =
      std::min({std::max<size_t>(1, worker_count), worker_count_,
                ParallelWorkerCount(total_blocks)});
  const std::vector<IntegerPeekCompare>* peek_ptr =
      peek_compares_.empty() ? nullptr : &peek_compares_;
  std::atomic<bool> failed{false};
  Status error = Status::kSuccess;
  std::mutex error_mutex;
  const auto fail = [&](Status status) {
    std::scoped_lock lock(error_mutex);
    if (!failed.load(std::memory_order_relaxed)) {
      error = std::move(status);
      failed.store(true, std::memory_order_release);
    }
  };
  const auto run_worker = [&](size_t w) {
    try {
      const size_t b_begin = (total_blocks * w) / fold_workers;
      const size_t b_end = (total_blocks * (w + 1)) / fold_workers;
      std::vector<uint32_t> matched_rows;
      matched_rows.reserve(256);
      for (size_t b = b_begin; b < b_end; ++b) {
        if (failed.load(std::memory_order_acquire)) {
          return;
        }
        const LSTablePageBlock& blk = lstable_store_->blocks[b];
        if (blk.row_count == 0) {
          continue;
        }
        if (!blk.MatchesZoneMaps(peek_ptr, nullptr, std::nullopt)) {
          continue;
        }
        bool all_rows_pass = true;
        if (peek_ptr != nullptr) {
          for (const IntegerPeekCompare& pred : *peek_ptr) {
            if (pred.column >= blk.columns.size()) {
              all_rows_pass = false;
              break;
            }
            const LSTableColumnBlock& pcol = blk.columns[pred.column];
            if (pcol.has_null || !pcol.has_min_max ||
                !MatchIntegerCompare(pcol.min_int, pred.op, pred.constant) ||
                !MatchIntegerCompare(pcol.max_int, pred.op, pred.constant) ||
                (pred.op == BinaryOperation::kNotEquals &&
                 pcol.min_int <= pred.constant &&
                 pred.constant <= pcol.max_int)) {
              all_rows_pass = false;
              break;
            }
          }
        }
        if (can_inline_simple) {
          bool zone_rejected = false;
          for (const auto& pred : row_filter_->simple) {
            const LSTableColumnBlock& col = blk.columns[pred.column];
            if (col.type == ValueType::kVarChar) {
              all_rows_pass = false;
              continue;
            }
            const bool is_double_cmp = col.type == ValueType::kDouble;
            if (is_double_cmp) {
              if (col.has_nan ||
                  (pred.double_payload && std::isnan(pred.double_constant))) {
                all_rows_pass = false;
                continue;
              }
              if (!col.has_double_min_max) {
                zone_rejected = true;
                break;
              }
              const double c = pred.double_payload
                                   ? pred.double_constant
                                   : static_cast<double>(pred.int_constant);
              switch (pred.op) {
                case BinaryOperation::kEquals:
                  if (c < col.min_double || c > col.max_double) {
                    zone_rejected = true;
                  } else if (col.has_null || col.min_double != c ||
                             col.max_double != c) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  if (col.min_double == c && col.max_double == c) {
                    zone_rejected = true;
                  } else if (col.has_null ||
                             (col.min_double <= c && c <= col.max_double)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kLessThan:
                  if (col.min_double >= c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.max_double < c)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  if (col.min_double > c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.max_double <= c)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kGreaterThan:
                  if (col.max_double <= c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.min_double > c)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (col.max_double < c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.min_double >= c)) {
                    all_rows_pass = false;
                  }
                  break;
                default:
                  all_rows_pass = false;
                  break;
              }
            } else {
              if (!col.has_min_max) {
                zone_rejected = true;
                break;
              }
              const int64_t c = pred.int_constant;
              switch (pred.op) {
                case BinaryOperation::kEquals:
                  if (c < col.min_int || c > col.max_int) {
                    zone_rejected = true;
                  } else if (col.has_null || col.min_int != c ||
                             col.max_int != c) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  if (col.min_int == c && col.max_int == c) {
                    zone_rejected = true;
                  } else if (col.has_null ||
                             (col.min_int <= c && c <= col.max_int)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kLessThan:
                  if (col.min_int >= c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.max_int < c)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  if (col.min_int > c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.max_int <= c)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kGreaterThan:
                  if (col.max_int <= c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.min_int > c)) {
                    all_rows_pass = false;
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (col.max_int < c) {
                    zone_rejected = true;
                  } else if (col.has_null || !(col.min_int >= c)) {
                    all_rows_pass = false;
                  }
                  break;
                default:
                  all_rows_pass = false;
                  break;
              }
            }
            if (zone_rejected) {
              break;
            }
          }
          if (zone_rejected) {
            continue;
          }
        }
        if (all_rows_pass) {
          if (Status st = consume(w, blk, nullptr, blk.row_count);
              st != Status::kSuccess) {
            fail(st);
            return;
          }
          continue;
        }
        matched_rows.clear();
        if (peek_ptr == nullptr && can_inline_simple &&
            row_filter_->simple.size() == 1) {
          const auto& pred = row_filter_->simple[0];
          const LSTableColumnBlock& col = blk.columns[pred.column];
          if (!col.has_null &&
              (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
            matched_rows.resize(blk.row_count);
            uint32_t* __restrict out = matched_rows.data();
            const int64_t* __restrict vals = col.int_cells.data();
            const int64_t c = pred.int_constant;
            const uint32_t n = blk.row_count;
            size_t out_idx = 0;
            switch (pred.op) {
              case BinaryOperation::kEquals:
                for (uint32_t r = 0; r < n; ++r) {
                  out[out_idx] = r;
                  out_idx += static_cast<size_t>(vals[r] == c);
                }
                break;
              case BinaryOperation::kNotEquals:
                for (uint32_t r = 0; r < n; ++r) {
                  out[out_idx] = r;
                  out_idx += static_cast<size_t>(vals[r] != c);
                }
                break;
              case BinaryOperation::kLessThan:
                for (uint32_t r = 0; r < n; ++r) {
                  out[out_idx] = r;
                  out_idx += static_cast<size_t>(vals[r] < c);
                }
                break;
              case BinaryOperation::kLessThanEquals:
                for (uint32_t r = 0; r < n; ++r) {
                  out[out_idx] = r;
                  out_idx += static_cast<size_t>(vals[r] <= c);
                }
                break;
              case BinaryOperation::kGreaterThan:
                for (uint32_t r = 0; r < n; ++r) {
                  out[out_idx] = r;
                  out_idx += static_cast<size_t>(vals[r] > c);
                }
                break;
              case BinaryOperation::kGreaterThanEquals:
                for (uint32_t r = 0; r < n; ++r) {
                  out[out_idx] = r;
                  out_idx += static_cast<size_t>(vals[r] >= c);
                }
                break;
              default:
                for (uint32_t r = 0; r < n; ++r) {
                  if (MatchIntegerCompare(vals[r], pred.op, c)) {
                    out[out_idx++] = r;
                  }
                }
                break;
            }
            matched_rows.resize(out_idx);
            if (matched_rows.size() == blk.row_count) {
              if (Status st = consume(w, blk, nullptr, blk.row_count);
                  st != Status::kSuccess) {
                fail(st);
                return;
              }
            } else if (!matched_rows.empty()) {
              if (Status st =
                      consume(w, blk, matched_rows.data(), matched_rows.size());
                  st != Status::kSuccess) {
                fail(st);
                return;
              }
            }
            continue;
          }
        }
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if (peek_ptr != nullptr) {
            bool peek_ok = true;
            for (const IntegerPeekCompare& pred : *peek_ptr) {
              const LSTableColumnBlock& pcol = blk.columns[pred.column];
              if (pcol.IsNull(r) ||
                  !MatchIntegerCompare(pcol.int_cells[r], pred.op,
                                       pred.constant)) {
                peek_ok = false;
                break;
              }
            }
            if (!peek_ok) {
              continue;
            }
          }
          if (can_inline_simple) {
            bool simple_ok = true;
            for (const auto& pred : row_filter_->simple) {
              const LSTableColumnBlock& col = blk.columns[pred.column];
              if (col.IsNull(r)) {
                simple_ok = false;
                break;
              }
              if (col.type == ValueType::kVarChar) {
                const std::string_view value = col.VarcharAt(r);
                const std::string_view constant =
                    pred.constant.value.varchar_value;
                bool ok = false;
                switch (pred.op) {
                  case BinaryOperation::kEquals:
                    ok = value == constant;
                    break;
                  case BinaryOperation::kNotEquals:
                    ok = value != constant;
                    break;
                  case BinaryOperation::kLessThan:
                    ok = value < constant;
                    break;
                  case BinaryOperation::kLessThanEquals:
                    ok = value <= constant;
                    break;
                  case BinaryOperation::kGreaterThan:
                    ok = value > constant;
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    ok = value >= constant;
                    break;
                  case BinaryOperation::kLike:
                  case BinaryOperation::kNotLike:
                    ok = relational_detail::FastLikeMatch(value, constant) ==
                         (pred.op == BinaryOperation::kLike);
                    break;
                  default:
                    break;
                }
                if (!ok) {
                  simple_ok = false;
                  break;
                }
              } else if (col.type == ValueType::kDouble) {
                const auto v = std::bit_cast<double>(col.int_cells[r]);
                const double c = pred.double_payload
                                     ? pred.double_constant
                                     : static_cast<double>(pred.int_constant);
                bool ok = false;
                switch (pred.op) {
                  case BinaryOperation::kEquals:
                    ok = (v == c);
                    break;
                  case BinaryOperation::kNotEquals:
                    ok = (v != c);
                    break;
                  case BinaryOperation::kLessThan:
                    ok = (v < c);
                    break;
                  case BinaryOperation::kLessThanEquals:
                    ok = (v <= c);
                    break;
                  case BinaryOperation::kGreaterThan:
                    ok = (v > c);
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    ok = (v >= c);
                    break;
                  default:
                    ok = true;
                    break;
                }
                if (!ok) {
                  simple_ok = false;
                  break;
                }
              } else {
                if (!MatchIntegerCompare(col.int_cells[r], pred.op,
                                         pred.int_constant)) {
                  simple_ok = false;
                  break;
                }
              }
            }
            if (!simple_ok) {
              continue;
            }
          }
          matched_rows.push_back(r);
        }
        if (matched_rows.size() == blk.row_count) {
          if (Status st = consume(w, blk, nullptr, blk.row_count);
              st != Status::kSuccess) {
            fail(st);
            return;
          }
        } else if (!matched_rows.empty()) {
          if (Status st =
                  consume(w, blk, matched_rows.data(), matched_rows.size());
              st != Status::kSuccess) {
            fail(st);
            return;
          }
        }
      }
    } catch (const std::bad_alloc&) {
      fail(Status::kNoSpace);
    } catch (const std::exception& ex) {
      fail(StatusError(StatusCode::kRuntimeError, ex.what()));
    } catch (...) {
      fail(StatusError(StatusCode::kRuntimeError,
                       "lstable fold scan worker failed"));
    }
  };
  const Status parallel_status = RunParallelTasks(fold_workers, run_worker);
  if (parallel_status != Status::kSuccess) {
    fail(parallel_status);
  }
  if (failed.load(std::memory_order_acquire)) {
    FailWith(error);
    return error;
  }
  return true;
} catch (const std::bad_alloc&) {
  // Store preparation and the shared-pool callback wrapper allocate before
  // entering a worker. Keep those failures inside the same Status boundary.
  FailWith(Status::kNoSpace);
  return Status::kNoSpace;
} catch (...) {
  FailWith(Status::kRuntimeError);
  return Status::kRuntimeError;
}

bool ParallelScan::HasSignedIntegerColumn(size_t offset) const {
  if (projection_) {
    if (offset >= projection_->size()) {
      return false;
    }
    offset = (*projection_)[offset];
  }
  return offset < table_->GetSchema().ColumnCount() &&
         table_->GetSchema().GetColumn(offset).Type() == ValueType::kInt64 &&
         !table_->GetSchema().GetColumn(offset).IsUnsigned();
}

void ParallelScan::Start(size_t batch_size) {
  std::scoped_lock lock(mutex_);
  if (started_) {
    return;
  }
  started_ = true;
  if (RejectedByIntegerMembership()) {
    next_out_morsel_ = morsels_.size();
    return;
  }
  if (morsels_.empty()) {
    return;
  }
  workers_.reserve(worker_count_);
  size_t launched = 0;
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    try {
      workers_.emplace_back(
          [this, batch_size, worker] { RunWorker(batch_size, worker); });
      ++launched;
    } catch (...) {
      // Thread creation can fail (e.g. resource_unavailable). Account only
      // for the workers that actually started and wake the consumer via
      // worker_error_ instead of blocking on active_workers_ forever.
      if (!worker_error_) {
        worker_error_ = std::current_exception();
      }
      break;
    }
  }
  active_workers_ = launched;
  ready_cv_.notify_all();
}

void ParallelScan::RunWorker(size_t batch_size, size_t worker_index) {
  try {
    // The projection layout never changes during the scan, so build the type
    // vector once and reuse it for every chunk refill.
    std::vector<ValueType> projection_types;
    if (projection_) {
      projection_types.reserve(projection_->size());
      for (slot_t slot : *projection_) {
        projection_types.push_back(table_->GetSchema().GetColumn(slot).Type());
      }
    } else {
      projection_types.reserve(table_->GetSchema().ColumnCount());
      for (size_t i = 0; i < table_->GetSchema().ColumnCount(); ++i) {
        projection_types.push_back(table_->GetSchema().GetColumn(i).Type());
      }
    }
    // A/B hatch: TINYLAMB_DISABLE_CHUNK_COPY_APPEND=1 restores move-append.
    static const bool copy_append_disabled =
        std::getenv("TINYLAMB_DISABLE_CHUNK_COPY_APPEND") != nullptr;
    // When the chunk layout holds only fixed-width cells (INT64/DATE/DOUBLE,
    // NULLs), appending by const reference is strictly cheaper than moving:
    // the copy is a scalar push per column, and the iterator keeps reusing
    // its decode buffer instead of re-allocating a fresh values vector (plus
    // fresh string cells) after every accepted row hands its old one to the
    // chunk.  VARCHAR/ARRAY layouts keep the move so long text still changes
    // hands without a deep copy.
    bool copy_append = !copy_append_disabled;
    for (const ValueType type : projection_types) {
      if (type == ValueType::kVarChar || type == ValueType::kArray) {
        copy_append = false;
        break;
      }
    }
    // Static rotation: worker w decodes morsels w, w+W, w+2W, ... .  The
    // atomic counter this replaced handed morsels to whichever worker grabbed
    // them; combined with the ordered drain below it would even deadlock (a
    // worker blocked on a full queue holds a later morsel while the morsel
    // the consumer waits for is still unclaimed).  Fixed ownership keeps
    // every morsel in flight under its owner from the start.
    for (size_t morsel_index = worker_index; morsel_index < morsels_.size();
         morsel_index += worker_count_) {
      if (cancelled_.load(std::memory_order_relaxed)) {
        return;
      }
      const std::vector<IntegerPeekCompare>* peek_ptr =
          peek_compares_.empty() ? nullptr : &peek_compares_;
      if (lstable_store_ != nullptr && !txn_->HasAnyPageVersionChains()) {
        DataChunk chunk;
        if (projection_) {
          chunk.Initialize(projection_types, batch_size);
        } else {
          chunk.Initialize(table_->GetSchema(), batch_size);
        }
        const size_t out_cols = projection_types.size();
        Row row_scratch{std::vector<Value>(out_cols)};
        const bool can_inline_simple =
            row_filter_ != nullptr && !row_filter_->compile_decided_reject &&
            row_filter_->disjunctive_branches.empty() && !projection_;
        bool all_simple_inline = can_inline_simple;
        if (can_inline_simple) {
          for (const auto& pred : row_filter_->simple) {
            if (pred.column_payload ||
                pred.column >= table_->GetSchema().ColumnCount()) {
              all_simple_inline = false;
              break;
            }
            const auto& col_def = table_->GetSchema().GetColumn(pred.column);
            const bool ok_double =
                col_def.Type() == ValueType::kDouble &&
                (pred.double_payload ||
                 (pred.int_payload && !pred.constant.IsUnsigned() &&
                  pred.int_constant >= -(int64_t{1} << 53) &&
                  pred.int_constant <= (int64_t{1} << 53)));
            const bool ok_int = (col_def.Type() == ValueType::kInt64 ||
                                 col_def.Type() == ValueType::kDate) &&
                                !col_def.IsUnsigned() && pred.int_payload &&
                                !pred.constant.IsUnsigned();
            if (!ok_double && !ok_int) {
              all_simple_inline = false;
              break;
            }
          }
        }
        const bool skip_worker_filter =
            all_simple_inline && row_filter_->prefix_in.empty() &&
            row_filter_->prebuilt_membership.empty() &&
            row_filter_->prebuilt_scalar_compare.empty() &&
            row_filter_->residual.empty();

        for (const page_id_t pid : morsels_[morsel_index]) {
          if (cancelled_.load(std::memory_order_relaxed)) {
            return;
          }
          if (row_filter_ != nullptr && row_filter_->compile_decided_reject) {
            continue;
          }
          const LSTablePageBlock* blk = lstable_store_->FindBlock(pid);
          if (blk == nullptr || blk->row_count == 0) {
            continue;
          }
          if (!blk->MatchesZoneMaps(peek_ptr, nullptr, std::nullopt)) {
            continue;
          }
          if (can_inline_simple) {
            bool zone_rejected = false;
            for (const auto& pred : row_filter_->simple) {
              if (pred.column_payload || pred.column >= blk->columns.size()) {
                continue;
              }
              const LSTableColumnBlock& col = blk->columns[pred.column];
              const bool is_double_cmp =
                  col.type == ValueType::kDouble &&
                  (pred.double_payload ||
                   (pred.int_payload && !pred.constant.IsUnsigned() &&
                    pred.int_constant >= -(int64_t{1} << 53) &&
                    pred.int_constant <= (int64_t{1} << 53)));
              const bool is_int_cmp = (col.type == ValueType::kInt64 ||
                                       col.type == ValueType::kDate) &&
                                      !col.is_unsigned && pred.int_payload &&
                                      !pred.constant.IsUnsigned();
              if (is_double_cmp) {
                if (col.has_nan ||
                    (pred.double_payload && std::isnan(pred.double_constant))) {
                  continue;
                }
                if (!col.has_double_min_max) {
                  zone_rejected = true;
                  break;
                }
                const double c = pred.double_payload
                                     ? pred.double_constant
                                     : static_cast<double>(pred.int_constant);
                switch (pred.op) {
                  case BinaryOperation::kEquals:
                    if (c < col.min_double || c > col.max_double) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    if (col.min_double == c && col.max_double == c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    if (col.min_double >= c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kLessThanEquals:
                    if (col.min_double > c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    if (col.max_double <= c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    if (col.max_double < c) {
                      zone_rejected = true;
                    }
                    break;
                  default:
                    break;
                }
              } else if (is_int_cmp) {
                if (!col.has_min_max) {
                  zone_rejected = true;
                  break;
                }
                const int64_t c = pred.int_constant;
                switch (pred.op) {
                  case BinaryOperation::kEquals:
                    if (c < col.min_int || c > col.max_int) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    if (col.min_int == c && col.max_int == c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    if (col.min_int >= c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kLessThanEquals:
                    if (col.min_int > c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    if (col.max_int <= c) {
                      zone_rejected = true;
                    }
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    if (col.max_int < c) {
                      zone_rejected = true;
                    }
                    break;
                  default:
                    break;
                }
              }
              if (zone_rejected) {
                break;
              }
            }
            if (zone_rejected) {
              continue;
            }
          }

          for (uint32_t r = 0; r < blk->row_count; ++r) {
            if (peek_ptr != nullptr) {
              bool peek_ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk->columns[pred.column];
                if (pcol.IsNull(r) ||
                    !MatchIntegerCompare(pcol.int_cells[r], pred.op,
                                         pred.constant)) {
                  peek_ok = false;
                  break;
                }
              }
              if (!peek_ok) {
                continue;
              }
            }
            if (can_inline_simple) {
              bool simple_ok = true;
              for (const auto& pred : row_filter_->simple) {
                if (pred.column_payload || pred.column >= blk->columns.size()) {
                  continue;
                }
                const LSTableColumnBlock& col = blk->columns[pred.column];
                const bool is_double_cmp =
                    col.type == ValueType::kDouble &&
                    (pred.double_payload ||
                     (pred.int_payload && !pred.constant.IsUnsigned() &&
                      pred.int_constant >= -(int64_t{1} << 53) &&
                      pred.int_constant <= (int64_t{1} << 53)));
                const bool is_int_cmp = (col.type == ValueType::kInt64 ||
                                         col.type == ValueType::kDate) &&
                                        !col.is_unsigned && pred.int_payload &&
                                        !pred.constant.IsUnsigned();
                if (!is_double_cmp && !is_int_cmp) {
                  continue;
                }
                if (col.IsNull(r)) {
                  simple_ok = false;
                  break;
                }
                if (is_double_cmp) {
                  const auto v = std::bit_cast<double>(col.int_cells[r]);
                  const double c = pred.double_payload
                                       ? pred.double_constant
                                       : static_cast<double>(pred.int_constant);
                  bool ok = false;
                  switch (pred.op) {
                    case BinaryOperation::kEquals:
                      ok = (v == c);
                      break;
                    case BinaryOperation::kNotEquals:
                      ok = (v != c);
                      break;
                    case BinaryOperation::kLessThan:
                      ok = (v < c);
                      break;
                    case BinaryOperation::kLessThanEquals:
                      ok = (v <= c);
                      break;
                    case BinaryOperation::kGreaterThan:
                      ok = (v > c);
                      break;
                    case BinaryOperation::kGreaterThanEquals:
                      ok = (v >= c);
                      break;
                    default:
                      ok = true;
                      break;
                  }
                  if (!ok) {
                    simple_ok = false;
                    break;
                  }
                } else {
                  if (!MatchIntegerCompare(col.int_cells[r], pred.op,
                                           pred.int_constant)) {
                    simple_ok = false;
                    break;
                  }
                }
              }
              if (!simple_ok) {
                continue;
              }
            }
            for (size_t i = 0; i < out_cols; ++i) {
              const slot_t full_col =
                  projection_ ? (*projection_)[i] : static_cast<slot_t>(i);
              const LSTableColumnBlock& col = blk->columns[full_col];
              Value& cell = row_scratch[i];
              if (col.IsNull(r)) {
                cell = Value();
                continue;
              }
              switch (col.type) {
                case ValueType::kInt64:
                  cell = Value(col.int_cells[r]);
                  if (col.is_unsigned) {
                    cell.SetUnsigned();
                  }
                  break;
                case ValueType::kDate:
                  cell.type = ValueType::kDate;
                  cell.value.int_value = col.int_cells[r];
                  break;
                case ValueType::kDouble:
                  cell = Value(std::bit_cast<double>(col.int_cells[r]));
                  break;
                case ValueType::kVarChar:
                  cell = Value(std::string(col.VarcharAt(r)));
                  break;
                default:
                  cell = Value();
                  break;
              }
            }
            const RowPosition position(pid, r < blk->slots.size()
                                                ? blk->slots[r]
                                                : static_cast<slot_t>(r));
            if (row_filter_ != nullptr && !skip_worker_filter) {
              const int decision = relational_detail::MatchScanFilterWorker(
                  row_scratch, table_->GetSchema(), *row_filter_,
                  *row_filter_programs_);
              if (decision == 0) {
                continue;
              }
              if (decision == 2) {
                std::scoped_lock deferred_lock(deferred_mutex_);
                deferred_.emplace_back(row_scratch, position);
                continue;
              }
            }
            chunk.Append(static_cast<const Row&>(row_scratch), position);
            if (chunk.Size() == batch_size) {
              if (!Enqueue(morsel_index, std::move(chunk))) {
                return;
              }
              chunk = DataChunk{};
              if (projection_) {
                chunk.Initialize(projection_types, batch_size);
              } else {
                chunk.Initialize(table_->GetSchema(), batch_size);
              }
            }
          }
        }
        if (!chunk.Empty()) {
          if (!Enqueue(morsel_index, std::move(chunk))) {
            return;
          }
          chunk = DataChunk{};
        }
        {
          std::scoped_lock lock(mutex_);
          morsels_done_[morsel_index] = true;
        }
        ready_cv_.notify_all();
        continue;
      }
      Iterator iterator =
          table_->BeginMorselScan(*txn_, morsels_[morsel_index], projection_,
                                  nullptr, std::nullopt, peek_ptr);
      // The layout never changes across refill, so Initialize is the single
      // reset path for both the first and subsequent chunks.
      DataChunk chunk;
      if (projection_) {
        chunk.Initialize(projection_types, batch_size);
      } else {
        chunk.Initialize(table_->GetSchema(), batch_size);
      }
      while (iterator.IsValid()) {
        if (cancelled_.load(std::memory_order_relaxed)) {
          iterator.DropPageLatch();
          return;
        }
        const RowPosition position = iterator.Position();
        if (row_filter_ != nullptr) {
          // Decide the row inside the decode worker: rejected rows never
          // reach a chunk, so the consumer thread pays nothing for them.
          const int decision = relational_detail::MatchScanFilterWorker(
              *iterator, table_->GetSchema(), *row_filter_,
              *row_filter_programs_);
          if (decision == 0) {
            ++iterator;
            continue;
          }
          if (decision == 2) {
            // The program raised; park the row for the context-free AST
            // re-check on the consumer thread (error semantics must match
            // the serial Selection, and AST evaluation is not worker-safe).
            std::scoped_lock deferred_lock(deferred_mutex_);
            deferred_.emplace_back(*iterator, position);
            ++iterator;
            continue;
          }
        }
        if (copy_append) {
          chunk.Append(static_cast<const Row&>(*iterator), position);
        } else {
          chunk.Append(std::move(*iterator), position);
        }
        ++iterator;
        if (chunk.Size() == batch_size) {
          // Never block on a full output queue while holding a page latch:
          // a writer waiting for that latch could be the very consumer that
          // would otherwise drain the queue.
          iterator.DropPageLatch();
          if (!Enqueue(morsel_index, std::move(chunk))) {
            return;
          }
          // The chunk above was handed off; start the refill from a
          // well-defined state instead of re-initializing a moved-from one.
          chunk = DataChunk{};
          if (projection_) {
            chunk.Initialize(projection_types, batch_size);
          } else {
            chunk.Initialize(table_->GetSchema(), batch_size);
          }
        }
      }
      bool scan_failed = false;
      if (iterator.GetStatus() != Status::kSuccess) {
        std::scoped_lock lock(mutex_);
        if (worker_status_ == Status::kSuccess) {
          worker_status_ = iterator.GetStatus();
        }
        cancelled_.store(true, std::memory_order_release);
        scan_failed = true;
      }
      iterator.DropPageLatch();

      if (!scan_failed && !chunk.Empty()) {
        if (!Enqueue(morsel_index, std::move(chunk))) {
          return;
        }
        // Handing off moved out of `chunk`; restore a well-defined state so
        // the next morsel iteration starts from a fresh object.
        chunk = DataChunk{};
      }
      {
        // The morsel is fully decoded and all of its chunks are queued; the
        // consumer may now advance past it once it drained them (an empty
        // morsel records only this flag).
        std::scoped_lock lock(mutex_);
        morsels_done_[morsel_index] = true;
      }
      ready_cv_.notify_all();
    }
  } catch (...) {
    std::scoped_lock lock(mutex_);
    if (!worker_error_) {
      worker_error_ = std::current_exception();
    }
    cancelled_.store(true, std::memory_order_release);
  }
  {
    std::scoped_lock lock(mutex_);
    --active_workers_;
  }
  ready_cv_.notify_all();
  space_cv_.notify_all();
}

bool ParallelScan::Enqueue(size_t morsel_index, DataChunk chunk) {
  std::unique_lock lock(mutex_);
  // Backpressure with an escape hatch: the queue cap applies to chunks of
  // morsels ahead of the consumer, but a chunk the consumer is currently
  // waiting for (morsel_index <= next_out_morsel_) is always admitted.  With
  // static morsel ownership this is what makes progress unconditional: the
  // owner of the waited-for morsel is the only worker that works on morsels
  // <= next_out_morsel_ before it, so it can never be the one blocked.
  space_cv_.wait(lock, [&] {
    return cancelled_.load(std::memory_order_relaxed) ||
           inflight_chunks_ < max_ready_chunks_ ||
           morsel_index <= next_out_morsel_;
  });
  if (cancelled_.load(std::memory_order_relaxed)) {
    return false;
  }
  ++inflight_chunks_;
  inflight_[morsel_index].push_back(std::move(chunk));
  lock.unlock();
  ready_cv_.notify_one();
  return true;
}

size_t ParallelScan::NextBatch(DataChunk* destination, size_t max_rows) {
  if (typed_folded_) {
    destination->Reset();
    return 0;
  }
  max_rows = std::max<size_t>(1, max_rows);
  destination->Reset();

  // Workers use a fixed chunk size; per-consumer max_rows is absorbed by the
  // pending_ split below so the first caller cannot dictate global capacity.
  Start(kDefaultVectorSize);
  while (destination->Size() < max_rows) {
    if (pending_) {
      while (pending_offset_ < pending_->Size() &&
             destination->Size() < max_rows) {
        destination->Append(*pending_, pending_offset_++);
      }
      if (pending_offset_ == pending_->Size()) {
        pending_.reset();
        pending_offset_ = 0;
      } else {
        return destination->Size();
      }
    }

    std::unique_lock lock(mutex_);
    ready_cv_.wait(lock, [&] {
      return cancelled_.load(std::memory_order_relaxed) || worker_error_ ||
             worker_status_ != Status::kSuccess ||
             next_out_morsel_ >= morsels_.size() ||
             morsels_done_[next_out_morsel_] ||
             !inflight_[next_out_morsel_].empty();
    });
    if (worker_error_) {
      // Surface the worker failure as a sticky Status instead of unwinding
      // through NextBatch (no-exception executor contract).
      try {
        std::rethrow_exception(worker_error_);
      } catch (const std::exception& error) {
        FailWith(StatusError(StatusCode::kRuntimeError, error.what()));
      } catch (...) {
        FailWith(StatusError(StatusCode::kRuntimeError,
                             "parallel scan worker failed"));
      }
      return 0;
    }
    if (worker_status_ != Status::kSuccess) {
      FailWith(worker_status_);
      return 0;
    }
    if (cancelled_.load(std::memory_order_relaxed)) {
      return destination->Size();
    }
    if (next_out_morsel_ >= morsels_.size()) {
      lock.unlock();
      // Every morsel is decoded and drained; rows parked by the worker
      // filter (programs raised) are re-checked here before reporting
      // end-of-scan, so their survivors are not silently dropped.
      DrainDeferred(destination, max_rows);
      return destination->Size();
    }
    std::deque<DataChunk>& queue = inflight_[next_out_morsel_];
    if (queue.empty()) {
      // The morsel finished with nothing queued (all rows filtered out):
      // advance past it instead of reporting a false end-of-scan.
      inflight_.erase(next_out_morsel_);
      ++next_out_morsel_;
      // Advancing the admission boundary can turn a parked producer's
      // Enqueue predicate (morsel_index <= next_out_morsel_) true.  Without
      // a wakeup here the consumer sleeps on ready_cv_ waiting for chunks
      // only that producer may push while the producer sleeps on space_cv_
      // with a stale predicate evaluation — a permanent deadlock.  notify_
      // all, not one: a single wakeup can be consumed by a producer of a
      // later morsel whose predicate stays false, sending it straight back
      // to sleep while the admissible producer keeps waiting.
      space_cv_.notify_all();
      continue;
    }
    DataChunk chunk = std::move(queue.front());
    queue.pop_front();
    --inflight_chunks_;
    lock.unlock();
    // Same reasoning as above: whichever parked producer is admissible (the
    // freed slot or the admission hatch) must actually observe the state
    // change, and one random wakeup cannot guarantee that.
    space_cv_.notify_all();

    if (destination->Size() == 0 && chunk.Size() <= max_rows) {
      // Whole-chunk handover keeps the chunk's zone maps intact (move
      // instead of a row-wise re-append).
      *destination = std::move(chunk);
      return destination->Size();
    }
    const size_t room = max_rows - destination->Size();
    if (chunk.Size() > room) {
      pending_ = std::move(chunk);
      pending_offset_ = 0;
      while (destination->Size() < max_rows) {
        destination->Append(*pending_, pending_offset_++);
      }
      return destination->Size();
    }
    for (size_t index = 0; index < chunk.Size(); ++index) {
      destination->Append(chunk, index);
    }
  }
  return destination->Size();
}

size_t ParallelScan::DrainDeferred(DataChunk* destination, size_t max_rows) {
  if (row_filter_ == nullptr || deferred_drained_) {
    return 0;
  }
  if (deferred_offset_ >= deferred_.size()) {
    deferred_drained_ = true;
    return 0;
  }
  // All workers are done, so deferred_ is stable; take the lock once for
  // the happens-before edge with the worker pushes.  Park order was racy
  // across workers; drain in physical row order so a raised program cannot
  // make the surviving row order scheduling-dependent.
  {
    std::scoped_lock deferred_lock(deferred_mutex_);
    std::sort(deferred_.begin() + static_cast<std::ptrdiff_t>(deferred_offset_),
              deferred_.end(), [](const auto& lhs, const auto& rhs) {
                return std::tie(lhs.second.page_id, lhs.second.slot) <
                       std::tie(rhs.second.page_id, rhs.second.slot);
              });
  }
  if (destination->Size() == 0) {
    destination->Reset(table_->GetSchema(), max_rows);
  }
  while (deferred_offset_ < deferred_.size() &&
         destination->Size() < max_rows) {
    const auto& [row, position] = deferred_[deferred_offset_++];
    StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
        row_filter_conjuncts_, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(row, table_->GetSchema());
        });
    if (!pass.HasValue()) {
      deferred_drained_ = true;
      FailWith(pass.GetStatus());
      return destination->Size();
    }
    if (pass.Value()) {
      destination->Append(row, position);
    }
  }
  if (deferred_offset_ >= deferred_.size()) {
    deferred_drained_ = true;
  }
  return destination->Size();
}

void ParallelScan::Dump(std::ostream& out, int /*indent*/) const {
  out << "ParallelScan: " << table_->GetSchema().Name() << " (" << worker_count_
      << " workers, " << morsels_.size() << " morsels)";
  if (projection_.has_value()) {
    out << " columns=" << projection_->size() << "/"
        << table_->GetSchema().ColumnCount();
  }
  if (row_filter_ != nullptr) {
    out << " worker-filter";
  }
  if (typed_folded_) {
    out << " fused-typed-fold";
  }
}

}  // namespace tinylamb
