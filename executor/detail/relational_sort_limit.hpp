/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/bytecode.hpp"
#include "type/collation.hpp"

namespace tinylamb {
class SelectStatement;
class TransactionContext;
}  // namespace tinylamb

namespace tinylamb::relational_detail {

struct TopNOutputSlot {
  size_t col{0};
  bool is_coalesce{false};
  size_t fallback_col{0};
};

struct ColumnarTopNKey {
  slot_t col{0};
  bool is_coalesce{false};
  slot_t fallback_col{0};
  std::optional<BytecodeProgram> program;
  ValueType result_type{ValueType::kNull};
  bool result_unsigned{false};
  bool result_interval{false};

  static ColumnarTopNKey Direct(slot_t c) {
    ColumnarTopNKey k;
    k.col = c;
    return k;
  }
  static ColumnarTopNKey Coalesce(slot_t c, slot_t fb) {
    ColumnarTopNKey k;
    k.col = c;
    k.is_coalesce = true;
    k.fallback_col = fb;
    return k;
  }
  static ColumnarTopNKey Computed(BytecodeProgram p, const Column& res_col) {
    ColumnarTopNKey k;
    k.program = std::move(p);
    k.result_type = res_col.Type();
    k.result_unsigned = res_col.IsUnsigned();
    k.result_interval = res_col.IsInterval();
    return k;
  }
};

using ColumnarTopNResult = std::optional<
    std::pair<std::shared_ptr<relational_detail::ColumnarTable>, bool>>;

StatusOr<ColumnarTopNResult> TryColumnarBoundedTopN(
    const relational_detail::ColumnarTable& src_table, const Schema& src_schema,
    const Schema& out_schema, std::span<const TopNOutputSlot> output_slots,
    std::span<const ColumnarTopNKey> keys,
    std::span<const SortKeyOrder> order_specs, size_t topn_capacity);

struct KeyedRow {
  std::vector<Value> keys;
  Row row;
  size_t sequence{0};
};

size_t EstimateKeyedRowBytes(const KeyedRow& keyed);
bool KeyedRowPrecedes(const std::vector<SortKeyOrder>& order_specs,
                      const KeyedRow& left, const KeyedRow& right);
Row EncodeKeyedRunRow(KeyedRow&& keyed);
KeyedRow DecodeKeyedRunRow(Row&& encoded, size_t key_count);

template <typename EmitFn>
Status MergeKeyedSpillRuns(const std::vector<SortKeyOrder>& order_specs,
                           std::vector<SpillFile>* runs,
                           std::optional<size_t> max_emit, EmitFn&& emit_row) {
  constexpr size_t kMergeFanIn = 32;
  const size_t key_count = order_specs.size();
  struct HeapItem {
    KeyedRow entry;
    size_t reader_idx{0};
  };
  const auto heap_greater = [&](const HeapItem& a, const HeapItem& b) {
    return KeyedRowPrecedes(order_specs, b.entry, a.entry);
  };

  while (runs->size() > kMergeFanIn) {
    std::vector<SpillFile> next_level;
    next_level.reserve((runs->size() + kMergeFanIn - 1) / kMergeFanIn);
    for (size_t start = 0; start < runs->size(); start += kMergeFanIn) {
      const size_t end = std::min(runs->size(), start + kMergeFanIn);
      SpillFile merged;
      std::vector<SpillFile::RowReader> readers;
      readers.reserve(end - start);
      std::vector<HeapItem> heap;
      heap.reserve(end - start);
      for (size_t idx = start; idx < end; ++idx) {
        ASSIGN_OR_RETURN(SpillFile::RowReader, reader,
                         (*runs)[idx].OpenRowReader());
        Row first;
        bool has_row = false;
        RETURN_IF_FAIL(reader.Next(&first, &has_row));
        readers.push_back(std::move(reader));
        if (has_row) {
          heap.push_back(
              HeapItem{.entry = DecodeKeyedRunRow(std::move(first), key_count),
                       .reader_idx = readers.size() - 1});
        }
      }
      std::make_heap(heap.begin(), heap.end(), heap_greater);
      size_t written = 0;
      while (!heap.empty() && (!max_emit.has_value() || written < *max_emit)) {
        std::pop_heap(heap.begin(), heap.end(), heap_greater);
        HeapItem top = std::move(heap.back());
        heap.pop_back();
        const size_t ridx = top.reader_idx;
        RETURN_IF_FAIL(merged.Append(EncodeKeyedRunRow(std::move(top.entry))));
        ++written;
        if (max_emit.has_value() && written >= *max_emit) {
          break;
        }
        Row next_raw;
        bool has_row = false;
        RETURN_IF_FAIL(readers[ridx].Next(&next_raw, &has_row));
        if (has_row) {
          heap.push_back(HeapItem{
              .entry = DecodeKeyedRunRow(std::move(next_raw), key_count),
              .reader_idx = ridx});
          std::push_heap(heap.begin(), heap.end(), heap_greater);
        }
      }
      readers.clear();
      for (size_t idx = start; idx < end; ++idx) {
        (*runs)[idx] = SpillFile{};
      }
      RETURN_IF_FAIL(merged.FinishWriting());
      next_level.push_back(std::move(merged));
    }
    *runs = std::move(next_level);
  }
  if (runs->empty()) {
    return Status::kSuccess;
  }

  std::vector<SpillFile::RowReader> readers;
  readers.reserve(runs->size());
  std::vector<HeapItem> heap;
  heap.reserve(runs->size());
  for (size_t idx = 0; idx < runs->size(); ++idx) {
    ASSIGN_OR_RETURN(SpillFile::RowReader, reader,
                     (*runs)[idx].OpenRowReader());
    Row first;
    bool has_row = false;
    RETURN_IF_FAIL(reader.Next(&first, &has_row));
    readers.push_back(std::move(reader));
    if (has_row) {
      heap.push_back(
          HeapItem{.entry = DecodeKeyedRunRow(std::move(first), key_count),
                   .reader_idx = idx});
    }
  }
  std::make_heap(heap.begin(), heap.end(), heap_greater);
  size_t emitted = 0;
  while (!heap.empty() && (!max_emit.has_value() || emitted < *max_emit)) {
    std::pop_heap(heap.begin(), heap.end(), heap_greater);
    HeapItem top = std::move(heap.back());
    heap.pop_back();
    const size_t ridx = top.reader_idx;
    RETURN_IF_FAIL(emit_row(std::move(top.entry.row)));
    ++emitted;
    if (max_emit.has_value() && emitted >= *max_emit) {
      break;
    }
    Row next_raw;
    bool has_row = false;
    RETURN_IF_FAIL(readers[ridx].Next(&next_raw, &has_row));
    if (has_row) {
      heap.push_back(
          HeapItem{.entry = DecodeKeyedRunRow(std::move(next_raw), key_count),
                   .reader_idx = ridx});
      std::push_heap(heap.begin(), heap.end(), heap_greater);
    }
  }
  readers.clear();
  runs->clear();
  return Status::kSuccess;
}

class ExternalKeyedSorter {
 public:
  ExternalKeyedSorter(std::vector<SortKeyOrder> order_specs,
                      bool bounded_requested, size_t topn_capacity)
      : order_specs_(std::move(order_specs)),
        bounded_requested_(bounded_requested),
        topn_capacity_(topn_capacity) {
    use_bounded_heap_ =
        bounded_requested && topn_capacity <= 1000000 &&
        (topn_capacity <= 4096 ||
         QueryMemoryBudget::Global().CanReserve(topn_capacity * 160));
  }

  Status Add(KeyedRow keyed) {
    keyed.sequence = next_sequence_++;
    const size_t entry_bytes = EstimateKeyedRowBytes(keyed);
    auto& budget = QueryMemoryBudget::Global();
    if (use_bounded_heap_) {
      const auto precedes = [&](const KeyedRow& a, const KeyedRow& b) {
        return KeyedRowPrecedes(order_specs_, a, b);
      };
      if (sortable_.size() < topn_capacity_) {
        if (!budget.Unlimited() && !sortable_.empty() &&
            !budget.CanReserve(entry_bytes)) {
          use_bounded_heap_ = false;
          RETURN_IF_FAIL(FlushRun());
        } else {
          charge_.Add(entry_bytes);
          buffered_bytes_ += entry_bytes;
          sortable_.push_back(std::move(keyed));
          std::push_heap(sortable_.begin(), sortable_.end(), precedes);
          return Status::kSuccess;
        }
      } else if (topn_capacity_ != 0 && precedes(keyed, sortable_.front())) {
        const size_t old_bytes = EstimateKeyedRowBytes(sortable_.front());
        if (entry_bytes > old_bytes) {
          const size_t delta = entry_bytes - old_bytes;
          if (!budget.Unlimited() && !budget.CanReserve(delta)) {
            use_bounded_heap_ = false;
            RETURN_IF_FAIL(FlushRun());
          } else {
            charge_.Add(delta);
            buffered_bytes_ += delta;
            std::pop_heap(sortable_.begin(), sortable_.end(), precedes);
            sortable_.back() = std::move(keyed);
            std::push_heap(sortable_.begin(), sortable_.end(), precedes);
            return Status::kSuccess;
          }
        } else {
          const size_t diff = old_bytes - entry_bytes;
          charge_.Release(diff);
          buffered_bytes_ -= std::min(buffered_bytes_, diff);
          std::pop_heap(sortable_.begin(), sortable_.end(), precedes);
          sortable_.back() = std::move(keyed);
          std::push_heap(sortable_.begin(), sortable_.end(), precedes);
          return Status::kSuccess;
        }
      } else {
        return Status::kSuccess;
      }
    }

    if (!budget.Unlimited() && !sortable_.empty()) {
      const size_t run_cap = std::max<size_t>(
          64ULL << 10, std::min<size_t>(128ULL << 20, budget.Limit() / 8));
      const size_t min_flush = std::max<size_t>(
          16ULL << 10, std::min<size_t>(1ULL << 20, run_cap / 4));
      if (buffered_bytes_ >= run_cap ||
          (buffered_bytes_ >= min_flush &&
           !budget.CanReserve(entry_bytes + (buffered_bytes_ / 4)))) {
        RETURN_IF_FAIL(FlushRun());
      }
    }
    charge_.Add(entry_bytes);
    buffered_bytes_ += entry_bytes;
    sortable_.push_back(std::move(keyed));
    return Status::kSuccess;
  }

  [[nodiscard]] bool Empty() const {
    return sortable_.empty() && sorted_runs_.empty();
  }
  [[nodiscard]] bool UsedBoundedHeap() const { return use_bounded_heap_; }
  [[nodiscard]] size_t TotalInputRows() const { return next_sequence_; }
  [[nodiscard]] size_t TopnCapacity() const { return topn_capacity_; }
  [[nodiscard]] const std::vector<SortKeyOrder>& OrderSpecs() const {
    return order_specs_;
  }

  template <typename EmitFn>
  Status EmitSorted(EmitFn&& emit_row) {
    const auto precedes = [&](const KeyedRow& a, const KeyedRow& b) {
      return KeyedRowPrecedes(order_specs_, a, b);
    };
    const std::optional<size_t> limit =
        bounded_requested_ ? std::optional<size_t>(topn_capacity_)
                           : std::nullopt;
    if (sorted_runs_.empty()) {
      if (use_bounded_heap_) {
        std::ranges::sort(sortable_, precedes);
      } else {
        std::ranges::stable_sort(sortable_, precedes);
      }
      const size_t count = limit.has_value()
                               ? std::min(sortable_.size(), *limit)
                               : sortable_.size();
      for (size_t i = 0; i < count; ++i) {
        std::vector<Value>().swap(sortable_[i].keys);
        RETURN_IF_FAIL(emit_row(std::move(sortable_[i].row)));
      }
      std::vector<KeyedRow>().swap(sortable_);
      buffered_bytes_ = 0;
      charge_.ReleaseAll();
      return Status::kSuccess;
    }

    if (!sortable_.empty()) {
      RETURN_IF_FAIL(FlushRun());
    }
    return MergeKeyedSpillRuns(order_specs_, &sorted_runs_, limit,
                               std::forward<EmitFn>(emit_row));
  }

 private:
  Status FlushRun() {
    if (sortable_.empty()) {
      return Status::kSuccess;
    }
    const auto precedes = [&](const KeyedRow& a, const KeyedRow& b) {
      return KeyedRowPrecedes(order_specs_, a, b);
    };
    std::ranges::stable_sort(sortable_, precedes);
    SpillFile run;
    const size_t emit_limit = bounded_requested_
                                  ? std::min(sortable_.size(), topn_capacity_)
                                  : sortable_.size();
    for (size_t i = 0; i < emit_limit; ++i) {
      RETURN_IF_FAIL(run.Append(EncodeKeyedRunRow(std::move(sortable_[i]))));
    }
    RETURN_IF_FAIL(run.FinishWriting());
    sorted_runs_.push_back(std::move(run));
    std::vector<KeyedRow>().swap(sortable_);
    buffered_bytes_ = 0;
    charge_.ReleaseAll();
    return Status::kSuccess;
  }

  std::vector<SortKeyOrder> order_specs_;
  bool bounded_requested_{false};
  bool use_bounded_heap_{false};
  size_t topn_capacity_{0};
  size_t next_sequence_{0};
  std::vector<KeyedRow> sortable_;
  size_t buffered_bytes_{0};
  QueryMemoryCharge charge_;
  std::vector<SpillFile> sorted_runs_;
};

Column ProjectionDomainColumn(std::string_view name, const Column& source);
Column ProjectionExpressionColumn(std::string_view name,
                                  const Expression& expression,
                                  const Schema& schema);
bool IsScalarBytecodeCompilable(const Expression& node);

StatusOr<Relation> DistinctOf(Relation input,
                              const std::vector<Expression>& distinct_on = {},
                              TransactionContext* ctx = nullptr,
                              const Scope* outer = nullptr,
                              const CteMap* ctes = nullptr);

Status ApplyOrderBy(TransactionContext& context,
                    const SelectStatement& statement, Relation* output,
                    const Scope* outer, const CteMap& ctes,
                    bool allow_bounded = true);

StatusOr<Relation> LimitedRows(const SelectStatement& statement,
                               Relation&& input,
                               TransactionContext* context = nullptr,
                               const Scope* outer = nullptr,
                               const CteMap* ctes = nullptr);

}  // namespace tinylamb::relational_detail
