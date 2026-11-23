/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_RELATION_HPP
#define TINYLAMB_EXECUTOR_DETAIL_RELATION_HPP

#include <cstddef>
#include <memory>
#include <span>
#include <type_traits>
#include <vector>

#include "common/exc_shim.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class QueryMemoryCharge;
}

namespace tinylamb::relational_detail {

// A shared lifetime for auxiliary relational state which cannot spill. A
// failed reservation leaves the caller on its ordinary spill-capable route.
std::shared_ptr<QueryMemoryCharge> TryReserveQueryMemory(size_t bytes);

constexpr size_t kSpillPartitions = 32;
struct ExecutionRuntime;

struct Relation {
  Schema schema;
  std::vector<Row> rows;
  // Structure-of-Arrays columnar batches. Producers may leave `rows` empty and
  // populate `columnar` instead; legacy consumers materialize `rows` via
  // EnsureRowsMaterialized() when needed, while columnar operators consume
  // `columnar` directly with zero Row/Value boxing.
  std::shared_ptr<const ColumnarTable> columnar;
  // Spill generations are stored oldest first; any remaining in-memory rows
  // form a newer suffix after these files.
  std::shared_ptr<SpillFile> spill;
  std::vector<std::shared_ptr<SpillFile>> extra_spills;
  std::shared_ptr<SpillFile> spill_tail_;
  // USING join merge metadata: names whose duplicates across the joined
  // sides coalesce for bare references and star expansion.  Set by Join().
  std::shared_ptr<const std::vector<std::string>> using_columns;
  size_t charged_bytes_{0};
  size_t hash_joins{0};
  size_t hybrid_hash_joins{0};
  size_t in_memory_hash_joins{0};
  size_t nested_loop_joins{0};
  size_t join_comparisons{0};
  size_t peak_intermediate_rows{0};
  size_t spilled_rows_{0};
  // True only when !HasSpill(), every row has schema.ColumnCount() cells,
  // and every non-NULL cell matches its declared schema column ValueType
  // (never kArray, never IsCaseInsensitive()).
  bool uniform_cell_types{false};

  explicit Relation(ExecutionRuntime* runtime = nullptr) : runtime_(runtime) {}
  Relation(const Relation&) = delete;
  Relation& operator=(const Relation&) = delete;
  Relation(Relation&& other) noexcept;
  Relation& operator=(Relation&& other) noexcept;
  ~Relation();

  [[nodiscard]] bool HasColumnar() const noexcept {
    return !HasSpill() && columnar != nullptr;
  }

  [[nodiscard]] bool HasColumnarOnly() const noexcept {
    return !HasSpill() && rows.empty() && columnar != nullptr &&
           columnar->total_rows > 0;
  }

  void SetColumnarTable(std::shared_ptr<const ColumnarTable> table,
                        bool uniform_types);
  [[nodiscard]] Status EnsureRowsMaterializedChecked();
  // T4539: used to be `void` and discard EnsureRowsMaterializedChecked()'s
  // Status, so a failed columnar->rows materialization let callers walk an
  // EMPTY rows vector as if it were the real data. It now forwards the
  // Status and is [[nodiscard]]: every call site must propagate or handle.
  [[nodiscard]] Status EnsureRowsMaterialized();

  void ReleaseCharge();
  [[nodiscard]] static bool HasAmpleMemoryBudget();
  void EnsureSpill();
  void AddRow(Row row);
  void AddRowWithKnownBytes(Row row, size_t bytes, size_t flush_granularity) {
    if (columnar == nullptr && !spill && !spill_tail_ && extra_spills.empty() &&
        QueryMemoryBudget::Global().CanReserve(bytes + pending_charge_) &&
        pending_charge_ + bytes < flush_granularity) [[likely]] {
      uniform_cell_types = false;
      charged_bytes_ += bytes;
      pending_charge_ += bytes;
      rows.push_back(std::move(row));
      peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
      return;
    }
    AddRowSlowWithBytes(std::move(row), bytes, flush_granularity);
  }
  void MergeInMemoryShard(Relation&& shard);
  Status AdoptSpilledShard(Relation&& shard);
  static Status MergeShardsInto(std::vector<Relation>& shards, Relation* out);
  Status FinishSpill();
  Status TakeRowAndSpillBacking(
      std::vector<Row>* out_rows,
      std::vector<std::shared_ptr<SpillFile>>* out_spills);
  [[nodiscard]] static size_t ChargeFlushGranularity();
  // Drop every buffered or spilled row (and its memory charge). Callers must
  // have copied the contents out beforehand (e.g. via ForEachRow); re-adding
  // rows afterwards starts a fresh spill cycle instead of appending to the
  // finished spill files.
  void ResetContents();
  void set_runtime(ExecutionRuntime* runtime) { runtime_ = runtime; }
  [[nodiscard]] ExecutionRuntime* runtime() const { return runtime_; }
  // Sticky first-error for spill-write failures on the void AddRow() path
  // (the degraded in-memory fallback and the dropped-row branch).  Callers
  // with an error channel can surface the truncation instead of reporting
  // success over a partial result.
  [[nodiscard]] Status GetStatus() const { return spill_error_; }

  // Mutable iteration over the in-memory rows only; returns false when the
  // relation has columnar/spill payloads (callers must fall back to the
  // read-only ForEachRow path there).
  template <typename Fn>
  bool ForEachMutableRow(Fn&& fn) {
    if (HasColumnarOnly() || spill || spill_tail_ || !extra_spills.empty()) {
      return false;
    }
    for (Row& row : rows) {
      fn(row);
    }
    return true;
  }

  template <typename Fn>
  Status ForEachRow(Fn&& fn) {
    Status first{Status::kSuccess};
    if (HasColumnarOnly()) {
      Row scratch;
      for (const ColumnarChunk& chunk : columnar->chunks) {
        for (size_t r = 0; r < chunk.rows; ++r) {
          chunk.MaterializeRowInto(r, &scratch);
          fn(scratch);
        }
      }
      if (spill_error_ != Status::kSuccess) {
        first = spill_error_;
      }
      return first;
    }
    if (spill) {
      const Status st = spill->ForEachRow(fn);
      if (st != Status::kSuccess) {
        first = st;
      }
    }
    for (const auto& extra : extra_spills) {
      if (extra) {
        const Status st = extra->ForEachRow(fn);
        if (st != Status::kSuccess && first == Status::kSuccess) {
          first = st;
        }
      }
    }
    if (spill_tail_) {
      // SpillFile stores its count only after FinishWriting; flush the tail
      // before reading or appended-but-unfinished rows are silently lost.
      const Status fs = FinishSpill();
      if (fs != Status::kSuccess && first == Status::kSuccess) {
        first = fs;
      }
      if (spill_tail_) {
        const Status st = spill_tail_->ForEachRow(fn);
        if (st != Status::kSuccess && first == Status::kSuccess) {
          first = st;
        }
      }
    }
    for (const Row& row : rows) {
      fn(row);
    }
    if (first == Status::kSuccess && spill_error_ != Status::kSuccess) {
      first = spill_error_;
    }
    return first;
  }

  // NOTE: SpillFile::ForEachRow advances a single internal read cursor and is
  // non-const; the const_cast calls below mean a shared spilled Relation must
  // not be traversed concurrently without external synchronization.
  template <typename Fn>
  [[nodiscard]] Status ForEachRow(Fn&& fn) const {
    Status first{Status::kSuccess};
    if (HasColumnarOnly()) {
      Row scratch;
      for (const ColumnarChunk& chunk : columnar->chunks) {
        for (size_t r = 0; r < chunk.rows; ++r) {
          chunk.MaterializeRowInto(r, &scratch);
          fn(scratch);
        }
      }
      if (spill_error_ != Status::kSuccess) {
        first = spill_error_;
      }
      return first;
    }
    if (spill) {
      const Status st = const_cast<SpillFile*>(spill.get())->ForEachRow(fn);
      if (st != Status::kSuccess) {
        first = st;
      }
    }
    for (const auto& extra : extra_spills) {
      if (extra) {
        const Status st = const_cast<SpillFile*>(extra.get())->ForEachRow(fn);
        if (st != Status::kSuccess && first == Status::kSuccess) {
          first = st;
        }
      }
    }
    if (spill_tail_) {
      // FinishSpill finalizes the tail spill file (a one-way state change);
      // the only mutation in this const path. Callers sharing a spilled
      // Relation across threads must provide external synchronization.
      const Status fs = const_cast<Relation*>(this)->FinishSpill();
      if (fs != Status::kSuccess && first == Status::kSuccess) {
        first = fs;
      }
      const Status st =
          const_cast<SpillFile*>(spill_tail_.get())->ForEachRow(fn);
      if (st != Status::kSuccess && first == Status::kSuccess) {
        first = st;
      }
    }
    for (const Row& row : rows) {
      fn(row);
    }
    if (first == Status::kSuccess && spill_error_ != Status::kSuccess) {
      first = spill_error_;
    }
    return first;
  }

  // Visit rows in ForEachRow() order. Stop after `max_rows` rows or when
  // a visitor returning bool returns false.
  // Spilled parts stream through independent buffered RowReaders so a
  // sampling caller bounds the decode instead of paying a full pass.
  template <typename Fn>
  [[nodiscard]] Status ForEachRowBounded(size_t max_rows, Fn&& fn) const {
    Status first{Status::kSuccess};
    size_t seen = 0;
    const auto visit = [&](auto& row) {
      ++seen;
      if constexpr (std::is_same_v<std::invoke_result_t<Fn&, decltype(row)>,
                                   bool>) {
        if (!fn(row)) {
          seen = max_rows;
        }
      } else {
        fn(row);
      }
    };
    const auto drain = [&](const std::shared_ptr<SpillFile>& file) -> Status {
      if (file == nullptr || seen >= max_rows) {
        return Status::kSuccess;
      }
      ASSIGN_OR_RETURN(SpillFile::RowReader, reader, file->OpenRowReader());
      Row row;
      bool has_row = false;
      while (seen < max_rows) {
        RETURN_IF_FAIL(reader.Next(&row, &has_row));
        if (!has_row) {
          break;
        }
        visit(row);
      }
      return Status::kSuccess;
    };
    if (HasColumnarOnly()) {
      Row scratch;
      for (const ColumnarChunk& chunk : columnar->chunks) {
        for (size_t r = 0; r < chunk.rows && seen < max_rows; ++r) {
          chunk.MaterializeRowInto(r, &scratch);
          visit(scratch);
        }
      }
      if (spill_error_ != Status::kSuccess) {
        first = spill_error_;
      }
      return first;
    }
    if (const Status st = drain(spill); st != Status::kSuccess) {
      first = st;
    }
    for (const auto& extra : extra_spills) {
      if (const Status st = drain(extra);
          st != Status::kSuccess && first == Status::kSuccess) {
        first = st;
      }
    }
    if (spill_tail_ && seen < max_rows) {
      // FinishSpill finalises the tail spill file (a one-way state change);
      // the only mutation in this const path. Only do it when the tail is
      // actually about to be read: finalising on an early exit (caller stopped
      // before max_rows, or an earlier spill already filled the budget) would
      // make any later Append fail with "Append after FinishWriting".
      // Callers sharing a spilled Relation across threads must provide external
      // synchronization.
      const Status fs = const_cast<Relation*>(this)->FinishSpill();
      if (fs != Status::kSuccess && first == Status::kSuccess) {
        first = fs;
      }
      if (const Status st = drain(spill_tail_);
          st != Status::kSuccess && first == Status::kSuccess) {
        first = st;
      }
    }
    for (const Row& row : rows) {
      if (seen >= max_rows) {
        break;
      }
      visit(row);
    }
    if (first == Status::kSuccess && spill_error_ != Status::kSuccess) {
      first = spill_error_;
    }
    return first;
  }

  [[nodiscard]] bool HasSpill() const {
    return spill != nullptr || !extra_spills.empty() || spill_tail_ != nullptr;
  }

  // Bytes charged by AddRow-style paths; 0 when rows were assigned directly.
  [[nodiscard]] size_t ChargedBytes() const noexcept { return charged_bytes_; }

  [[nodiscard]] size_t EstimatedMemoryBytes() const noexcept {
    if (HasColumnarOnly()) {
      return columnar->EstimatedBytes();
    }
    return std::max(charged_bytes_, rows.size() * 64);
  }

  [[nodiscard]] size_t TotalRows() const {
    // Row and columnar storage are disjoint: SetColumnarTable clears rows and
    // a full materialization resets columnar. During a partial materialization
    // both can hold rows, so count both instead of skipping columnar whenever
    // HasColumnarOnly() is false (which would under-report the row count).
    size_t total = rows.size();
    if (columnar) {
      total += columnar->total_rows;
    }
    if (spill) {
      total += spill->Count();
    }
    for (const auto& extra : extra_spills) {
      if (extra) {
        total += extra->Count();
      }
    }
    if (spill_tail_) {
      total += spill_tail_->Count();
    }
    return total;
  }

 private:
  // Apply the per-relation pending charge to the global budget.  AddRow
  // charges in bounded batches (never concurrently: parallel operators merge
  // separately built Relations) so the shared atomic stops being the
  // serialization point of every parallel scan/join row append.
  void FlushPendingCharge();
  Status SealSpillTail();
  void AddRowSlowWithBytes(Row row, size_t bytes, size_t flush_granularity);

  ExecutionRuntime* runtime_{nullptr};
  Status spill_error_{Status::kSuccess};
  // charged_bytes_ = total estimate owned by this Relation; pending_charge_
  // is the not-yet-applied suffix of it.
  size_t pending_charge_{0};
};

struct ColumnTypeTracker {
  bool seen_non_null{false};
  bool mixed{false};
  bool saw_unsafe{false};
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};

  void Observe(const Value& cell) {
    if (cell.type == ValueType::kArray || cell.IsCaseInsensitive()) {
      saw_unsafe = true;
    }
    if (cell.IsNull()) {
      return;
    }
    if (!seen_non_null) {
      seen_non_null = true;
      type = cell.type;
      is_unsigned = cell.IsUnsigned();
      is_interval = cell.IsInterval();
    } else if (type != cell.type || is_unsigned != cell.IsUnsigned() ||
               is_interval != cell.IsInterval()) {
      mixed = true;
    }
  }

  void MergeFrom(const ColumnTypeTracker& other) {
    saw_unsafe |= other.saw_unsafe;
    if (other.mixed) {
      mixed = true;
      return;
    }
    if (!other.seen_non_null) {
      return;
    }
    if (!seen_non_null) {
      seen_non_null = true;
      type = other.type;
      is_unsigned = other.is_unsigned;
      is_interval = other.is_interval;
    } else if (type != other.type || is_unsigned != other.is_unsigned ||
               is_interval != other.is_interval) {
      mixed = true;
    }
  }

  // Applies merged per-column type observations to `columns` and returns
  // whether every column satisfies Relation::uniform_cell_types.
  [[nodiscard]] static bool FinalizeColumns(
      std::span<const std::vector<ColumnTypeTracker>> per_worker,
      std::vector<Column>& columns);
};

using RelationPtr = std::shared_ptr<Relation>;

StatusOr<Relation> MaterializeRelation(const Relation& source);

void CopyExecutionStats(Relation* destination, const Relation& source);

void NoteRelationSpill(ExecutionRuntime* runtime);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_RELATION_HPP
