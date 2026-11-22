/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/relation.hpp"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <utility>

#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/row.hpp"

namespace tinylamb::relational_detail {
namespace {
// Large intermediates are built by workers, but their destruction used to
// return every row allocation from the coordinator. Reclaim disjoint ranges
// while the query admission scope is alive. Outside a query (including static
// teardown), and during exception unwinding, retain ordinary serial cleanup.
void ClearRelationRows(std::vector<Row>& rows) noexcept {
  if (rows.size() >= 16384 && query_parallelism_budget.outstanding != nullptr &&
      query_parallelism_budget.cpu_slots > 1 &&
      std::uncaught_exceptions() == 0) {
    try {
      const size_t workers = ParallelWorkerCount(rows.size() / 4096);
      if (workers > 1) {
        (void)RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = rows.size() * w / workers;
          const size_t end = rows.size() * (w + 1) / workers;
          for (size_t r = begin; r < end; ++r) {
            // swap is noexcept and leaves an empty, valid row. The ordinary
            // clear below also handles any work not started on a failure.
            std::vector<Value>().swap(rows[r].values_);
          }
        });
      }
    } catch (...) {  // NOLINT(bugprone-empty-catch)
      // Destruction must remain noexcept even if scheduling cannot allocate.
    }
  }
  rows.clear();
}
}  // namespace

std::shared_ptr<QueryMemoryCharge> TryReserveQueryMemory(size_t bytes) {
  // T3724: reserve atomically.  The previous CanReserve() pre-check plus a
  // QueryMemoryCharge constructor that forced the charge (ReserveForced)
  // let concurrent callers all pass the check and then each overshoot the
  // soft query budget.  QueryMemoryBudget::TryReserve CAS-updates the used
  // counter (and the process pool) and fails exactly the losers.
  if (!QueryMemoryBudget::Global().TryReserve(bytes)) {
    return nullptr;
  }
  return std::make_shared<QueryMemoryCharge>(
      QueryMemoryCharge::kAlreadyReserved{}, bytes);
}

Relation::Relation(Relation&& other) noexcept
    : schema(std::move(other.schema)),
      rows(std::move(other.rows)),
      columnar(std::move(other.columnar)),
      spill(std::move(other.spill)),
      extra_spills(std::move(other.extra_spills)),
      spill_tail_(std::move(other.spill_tail_)),
      using_columns(std::move(other.using_columns)),
      charged_bytes_(other.charged_bytes_),
      hash_joins(other.hash_joins),
      hybrid_hash_joins(other.hybrid_hash_joins),
      in_memory_hash_joins(other.in_memory_hash_joins),
      nested_loop_joins(other.nested_loop_joins),
      join_comparisons(other.join_comparisons),
      peak_intermediate_rows(other.peak_intermediate_rows),
      spilled_rows_(other.spilled_rows_),
      uniform_cell_types(other.uniform_cell_types),
      runtime_(other.runtime_),
      spill_error_(std::move(other.spill_error_)),
      pending_charge_(other.pending_charge_) {
  other.charged_bytes_ = 0;
  other.pending_charge_ = 0;
  other.hash_joins = 0;
  other.hybrid_hash_joins = 0;
  other.in_memory_hash_joins = 0;
  other.nested_loop_joins = 0;
  other.join_comparisons = 0;
  other.peak_intermediate_rows = 0;
  other.spilled_rows_ = 0;
  other.uniform_cell_types = false;
}

Relation& Relation::operator=(Relation&& other) noexcept {
  if (this != &other) {
    ClearRelationRows(rows);
    columnar.reset();
    ReleaseCharge();
    schema = std::move(other.schema);
    rows = std::move(other.rows);
    columnar = std::move(other.columnar);
    spill = std::move(other.spill);
    extra_spills = std::move(other.extra_spills);
    spill_tail_ = std::move(other.spill_tail_);
    using_columns = std::move(other.using_columns);
    charged_bytes_ = other.charged_bytes_;
    pending_charge_ = other.pending_charge_;
    other.charged_bytes_ = 0;
    other.pending_charge_ = 0;
    hash_joins = other.hash_joins;
    hybrid_hash_joins = other.hybrid_hash_joins;
    in_memory_hash_joins = other.in_memory_hash_joins;
    nested_loop_joins = other.nested_loop_joins;
    join_comparisons = other.join_comparisons;
    peak_intermediate_rows = other.peak_intermediate_rows;
    spilled_rows_ = other.spilled_rows_;
    uniform_cell_types = other.uniform_cell_types;
    spill_error_ = std::move(other.spill_error_);
    runtime_ = other.runtime_;
    other.hash_joins = 0;
    other.hybrid_hash_joins = 0;
    other.in_memory_hash_joins = 0;
    other.nested_loop_joins = 0;
    other.join_comparisons = 0;
    other.peak_intermediate_rows = 0;
    other.spilled_rows_ = 0;
    other.uniform_cell_types = false;
  }
  return *this;
}

Relation::~Relation() {
  ClearRelationRows(rows);
  columnar.reset();
  ReleaseCharge();
}

void Relation::SetColumnarTable(std::shared_ptr<const ColumnarTable> table,
                                bool uniform_types) {
  ClearRelationRows(rows);
  columnar.reset();
  ReleaseCharge();
  columnar = std::move(table);
  uniform_cell_types = uniform_types;
  if (columnar != nullptr) {
    peak_intermediate_rows =
        std::max(peak_intermediate_rows, columnar->total_rows);
    if (columnar->memory_charge == nullptr) {
      const size_t bytes = columnar->EstimatedBytes();
      charged_bytes_ = bytes;
      pending_charge_ = bytes;
      if (pending_charge_ >= ChargeFlushGranularity()) {
        FlushPendingCharge();
      }
    }
  }
}

Status Relation::EnsureRowsMaterializedChecked() {
  if (!HasColumnarOnly()) {
    return spill_error_;
  }
  bool uniform_ok = false;
  const Status st =
      MaterializeColumnarTableToRows(*columnar, schema, &rows, &uniform_ok);
  if (st != Status::kSuccess) {
    if (spill_error_ == Status::kSuccess) {
      spill_error_ = st;
    }
    uniform_cell_types = false;
    ClearRelationRows(rows);
    // NOLINTNEXTLINE(performance-no-automatic-move) move not beneficial for
    // this type
    return st;  // NOLINT(performance-no-automatic-move)
  }
  uniform_cell_types = uniform_ok;
  peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
  columnar.reset();
  return Status::kSuccess;
}

Status Relation::EnsureRowsMaterialized() {
  return EnsureRowsMaterializedChecked();
}

size_t Relation::ChargeFlushGranularity() {
  // Scale the batching window to the budget so tiny test budgets keep
  // near-exact accounting (overshoot <= 1/64 of the limit, min 4 KiB) while
  // production-size budgets amortize the shared atomic over megabytes.
  const size_t limit = QueryMemoryBudget::Global().Limit();
  if (QueryMemoryBudget::Global().Unlimited()) {
    return size_t{4} << 20;
  }
  return std::clamp(limit / 64, size_t{4096}, size_t{262144});
}

bool Relation::HasAmpleMemoryBudget() {
  return QueryMemoryBudget::Global().Remaining() >= (size_t{512} << 20);
}

void Relation::FlushPendingCharge() {
  if (pending_charge_ != 0) {
    QueryMemoryBudget::Global().ReserveForced(pending_charge_);
    pending_charge_ = 0;
  }
}

void Relation::ReleaseCharge() {
  // Charges never applied to the global counter simply evaporate. Clamp the
  // subtraction: if the pending portion ever exceeds the recorded charge, an
  // unguarded size_t subtraction would wrap and release a huge value into the
  // query pool instead of just dropping the pending bookkeeping.
  if (pending_charge_ != 0) {
    charged_bytes_ -= std::min(charged_bytes_, pending_charge_);
    pending_charge_ = 0;
  }
  if (charged_bytes_ != 0) {
    QueryMemoryBudget::Global().Release(charged_bytes_);
    charged_bytes_ = 0;
  }
}

void Relation::EnsureSpill() {
  if (columnar != nullptr) {
    if (EnsureRowsMaterializedChecked() != Status::kSuccess) {
      return;
    }
    columnar.reset();
  }
  if (HasSpill() && !rows.empty()) {
    if (SealSpillTail() != Status::kSuccess) {
      return;
    }
    NoteRelationSpill(runtime_);
    auto generation = std::make_shared<SpillFile>();
    Status st = Status::kSuccess;
    for (const Row& row : rows) {
      st = generation->Append(row);
      if (st != Status::kSuccess) {
        break;
      }
    }
    if (st == Status::kSuccess) {
      st = generation->FinishWriting();
    }
    if (st != Status::kSuccess) {
      if (spill_error_ == Status::kSuccess) {
        spill_error_ = st;
      }
      return;
    }
    // These rows follow the earlier files and the tail just sealed above.
    extra_spills.push_back(std::move(generation));
    spilled_rows_ += rows.size();
    ClearRelationRows(rows);
    rows.shrink_to_fit();
    ReleaseCharge();
  }
  if (spill && spill_tail_) {
    return;
  }
  if (spill || !extra_spills.empty()) {
    if (!spill_tail_) {
      spill_tail_ = std::make_shared<SpillFile>();
    }
    return;
  }
  NoteRelationSpill(runtime_);
  spill = std::make_shared<SpillFile>();
  for (const Row& row : rows) {
    // Spill failure (e.g. disk quota exceeded) keeps the rows in memory
    // rather than crashing; record the sticky error and fall back cleanly.
    const Status st = spill->Append(row);
    if (st != Status::kSuccess) {
      if (spill_error_ == Status::kSuccess) {
        spill_error_ = st;
      }
      spill.reset();
      return;
    }
  }
  const Status finish_st = spill->FinishWriting();
  if (finish_st != Status::kSuccess) {
    if (spill_error_ == Status::kSuccess) {
      spill_error_ = finish_st;
    }
    spill.reset();
    return;
  }
  spilled_rows_ += rows.size();
  ClearRelationRows(rows);
  rows.shrink_to_fit();
  ReleaseCharge();
  spill_tail_ = std::make_shared<SpillFile>();
}

void Relation::AddRow(Row row) {
  const size_t bytes = EstimateRowBytes(row);
  AddRowWithKnownBytes(std::move(row), bytes, ChargeFlushGranularity());
}

void Relation::AddRowSlowWithBytes(Row row, size_t bytes,
                                   size_t flush_granularity) {
  if (columnar != nullptr) {
    // T4539: on materialization failure keep the columnar side intact and
    // drop the incoming row: the failure is latched in spill_error_, so
    // every subsequent ForEachRow()/GetStatus() on this relation surfaces
    // the error instead of silently seeing an empty row set.
    if (EnsureRowsMaterialized() != Status::kSuccess) {
      return;
    }
    columnar.reset();
  }
  uniform_cell_types = false;
  // The pending (not yet applied) charge joins the soft-limit check so spill
  // decisions see the same total usage the immediate-charge version tracked.
  if (spill || spill_tail_ || !extra_spills.empty() ||
      !QueryMemoryBudget::Global().CanReserve(bytes + pending_charge_)) {
    FlushPendingCharge();
    if (!spill_tail_) {
      EnsureSpill();
    }
    if (!spill_tail_) {
      // EnsureSpill failed (e.g. disk quota).  Keep rows in memory.
      if (spill_error_ == Status::kSuccess) {
        spill_error_ = StatusError(StatusCode::kNoSpace,
                                   "relation spill failed; keeping rows in "
                                   "memory");
      }
      charged_bytes_ += bytes;
      pending_charge_ += bytes;
      FlushPendingCharge();
      rows.push_back(std::move(row));
      peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
      return;
    }
    if (Status append = spill_tail_->Append(row); append != Status::kSuccess) {
      // Preserve previously appended rows and their count. The failed row
      // is not part of the spill; the sticky error prevents success over a
      // partial result.
      if (spill_error_ == Status::kSuccess) {
        spill_error_ = append;
      }
      return;
    }
    peak_intermediate_rows =
        std::max(peak_intermediate_rows, rows.size() + spilled_rows_ + 1);
    ++spilled_rows_;
    return;
  }
  charged_bytes_ += bytes;
  pending_charge_ += bytes;
  if (pending_charge_ >= flush_granularity) {
    FlushPendingCharge();
  }
  rows.push_back(std::move(row));
  peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
}

Status Relation::MergeShardsInto(std::vector<Relation>& shards, Relation* out) {
  Status first_error{Status::kSuccess};
  size_t total_in_memory = 0;
  size_t total_pending_charge = 0;
  bool any_spilled = out->HasSpill();
  for (Relation& shard : shards) {
    const Status fs = shard.FinishSpill();
    if (first_error.ok() && fs != Status::kSuccess) {
      first_error = fs;
    }
    const Status gs = shard.GetStatus();
    if (first_error.ok() && gs != Status::kSuccess) {
      first_error = gs;
    }
    if (shard.spill_error_ != Status::kSuccess &&
        out->spill_error_ == Status::kSuccess) {
      out->spill_error_ = shard.spill_error_;
    }
    if (shard.HasSpill()) {
      any_spilled = true;
    } else {
      total_in_memory += shard.rows.size();
      total_pending_charge += shard.pending_charge_;
    }
  }
  if (!any_spilled && total_in_memory == 0 && out->rows.empty()) {
    bool any_columnar = out->HasColumnarOnly();
    for (const Relation& shard : shards) {
      if (shard.HasColumnarOnly()) {
        any_columnar = true;
      }
    }
    if (any_columnar) {
      auto merged = std::make_shared<ColumnarTable>();
      if (out->columnar != nullptr) {
        for (const auto& owner : out->columnar->keepalives) {
          merged->AppendKeepalive(owner);
        }
        for (const ColumnarChunk& chunk : out->columnar->chunks) {
          merged->total_rows += chunk.rows;
          merged->chunks.push_back(chunk);
        }
      }
      bool keep_uniform = (out->columnar == nullptr || out->uniform_cell_types);
      for (Relation& shard : shards) {
        if (shard.columnar != nullptr) {
          if (!shard.uniform_cell_types) {
            keep_uniform = false;
          }
          for (const auto& owner : shard.columnar->keepalives) {
            merged->AppendKeepalive(owner);
          }
          for (const ColumnarChunk& chunk : shard.columnar->chunks) {
            merged->total_rows += chunk.rows;
            merged->chunks.push_back(chunk);
          }
        }
        shard.ResetContents();
      }
      out->SetColumnarTable(std::move(merged), keep_uniform);
      return first_error;
    }
  }
  if (out->HasColumnarOnly()) {
    const Status ms = out->EnsureRowsMaterializedChecked();
    if (first_error.ok() && ms != Status::kSuccess) {
      first_error = ms;
    }
    out->columnar.reset();
  }
  for (Relation& shard : shards) {
    if (shard.HasColumnarOnly()) {
      const Status ms = shard.EnsureRowsMaterializedChecked();
      if (first_error.ok() && ms != Status::kSuccess) {
        first_error = ms;
      }
      total_in_memory += shard.rows.size();
      shard.columnar.reset();
    }
  }
  if (!any_spilled && total_in_memory > 0 &&
      QueryMemoryBudget::Global().CanReserve(total_pending_charge +
                                             out->pending_charge_)) {
    const bool initial_empty = out->rows.empty();
    bool all_shards_uniform = true;
    for (const Relation& shard : shards) {
      if (!shard.rows.empty() && !shard.uniform_cell_types) {
        all_shards_uniform = false;
        break;
      }
    }
    const bool keep_uniform =
        (initial_empty || out->uniform_cell_types) && all_shards_uniform;
    for (Relation& shard : shards) {
      out->charged_bytes_ += shard.charged_bytes_;
      out->pending_charge_ += shard.pending_charge_;
      shard.charged_bytes_ = 0;
      shard.pending_charge_ = 0;
    }
    if (out->pending_charge_ >= ChargeFlushGranularity()) {
      out->FlushPendingCharge();
    }
    const size_t base = out->rows.size();
    std::vector<size_t> shard_offsets(shards.size(), base);
    size_t cursor = base;
    for (size_t w = 0; w < shards.size(); ++w) {
      shard_offsets[w] = cursor;
      cursor += shards[w].rows.size();
    }
    out->rows.resize(cursor);
    const size_t merge_workers =
        total_in_memory >= 16384
            ? std::min(ParallelWorkerCount(total_in_memory / 8192),
                       shards.size())
            : 1;
    if (merge_workers > 1) {
      const Status merge_status =
          RunParallelTasks(merge_workers, [&](size_t worker) {
            for (size_t w = worker; w < shards.size(); w += merge_workers) {
              auto& src = shards[w].rows;
              if (!src.empty()) {
                std::move(src.begin(), src.end(),
                          out->rows.begin() +
                              static_cast<std::ptrdiff_t>(shard_offsets[w]));
                std::vector<Row>().swap(src);
              }
            }
          });
      if (first_error.ok() && merge_status != Status::kSuccess) {
        first_error = merge_status;
      }
    } else {
      for (size_t w = 0; w < shards.size(); ++w) {
        auto& src = shards[w].rows;
        if (!src.empty()) {
          std::move(src.begin(), src.end(),
                    out->rows.begin() +
                        static_cast<std::ptrdiff_t>(shard_offsets[w]));
          std::vector<Row>().swap(src);
        }
      }
    }
    out->uniform_cell_types = keep_uniform;
    out->peak_intermediate_rows =
        std::max(out->peak_intermediate_rows, out->rows.size());
    for (Relation& shard : shards) {
      shard.ResetContents();
    }
    return first_error;
  }
  if (!any_spilled && !out->HasSpill() && total_in_memory > 0) {
    out->rows.reserve(out->rows.size() + total_in_memory);
  }
  for (Relation& shard : shards) {
    if (shard.HasSpill()) {
      const Status it = out->AdoptSpilledShard(std::move(shard));
      if (first_error.ok() && it != Status::kSuccess) {
        first_error = it;
      }
    } else {
      out->MergeInMemoryShard(std::move(shard));
      shard.ResetContents();  // NOLINT(bugprone-use-after-move)
    }
  }
  return first_error;
}

Status Relation::AdoptSpilledShard(Relation&& shard) {
  Status first{Status::kSuccess};
  if (shard.spill_error_ != Status::kSuccess) {
    if (spill_error_ == Status::kSuccess) {
      spill_error_ = shard.spill_error_;
    }
    first = shard.spill_error_;
  }
  const Status fs = shard.FinishSpill();
  if (fs != Status::kSuccess) {
    if (spill_error_ == Status::kSuccess) {
      spill_error_ = fs;
    }
    if (first == Status::kSuccess) {
      first = fs;
    }
  }
  if (HasColumnarOnly()) {
    const Status ms = EnsureRowsMaterializedChecked();
    if (first == Status::kSuccess && ms != Status::kSuccess) {
      first = ms;
    }
    columnar.reset();
  }
  if (!shard.HasSpill()) {
    MergeInMemoryShard(std::move(shard));
    if (first == Status::kSuccess && spill_error_ != Status::kSuccess) {
      first = spill_error_;
    }
    return first;
  }
  uniform_cell_types = false;
  // Existing buffered rows precede the incoming shard. Persist that prefix
  // before linking the shard's files into the append sequence.
  if (!rows.empty()) {
    EnsureSpill();
    if (!rows.empty()) {
      return spill_error_;
    }
  }
  if (const Status sealed = SealSpillTail(); sealed != Status::kSuccess) {
    return first == Status::kSuccess ? sealed : first;
  }
  auto adopt_file = [&](std::shared_ptr<SpillFile> f) {
    if (!f || f->Count() == 0) {
      return;
    }
    spilled_rows_ += f->Count();
    if (!spill && extra_spills.empty()) {
      spill = std::move(f);
    } else {
      extra_spills.push_back(std::move(f));
    }
  };
  adopt_file(std::move(shard.spill));
  for (auto& extra : shard.extra_spills) {
    adopt_file(std::move(extra));
  }
  adopt_file(std::move(shard.spill_tail_));
  for (Row& row : shard.rows) {
    AddRow(std::move(row));
  }
  peak_intermediate_rows = std::max(peak_intermediate_rows, TotalRows());
  shard.ResetContents();
  if (first == Status::kSuccess && spill_error_ != Status::kSuccess) {
    first = spill_error_;
  }
  return first;
}

void Relation::MergeInMemoryShard(Relation&& shard) {
  if (shard.spill_error_ != Status::kSuccess &&
      spill_error_ == Status::kSuccess) {
    spill_error_ = shard.spill_error_;
  }
  if (shard.rows.empty() && !shard.HasSpill()) {
    shard.ReleaseCharge();
    return;
  }
  if (shard.HasSpill()) {
    (void)AdoptSpilledShard(std::move(shard));
    return;
  }
  if (HasSpill() || !shard.uniform_cell_types) {
    uniform_cell_types = false;
  }
  if (!HasSpill() && QueryMemoryBudget::Global().CanReserve(
                         shard.pending_charge_ + pending_charge_)) {
    charged_bytes_ += shard.charged_bytes_;
    pending_charge_ += shard.pending_charge_;
    shard.charged_bytes_ = 0;
    shard.pending_charge_ = 0;
    if (pending_charge_ >= ChargeFlushGranularity()) {
      FlushPendingCharge();
    }
    if (rows.empty() && rows.capacity() <= shard.rows.capacity()) {
      rows = std::move(shard.rows);
    } else {
      rows.insert(rows.end(), std::make_move_iterator(shard.rows.begin()),
                  std::make_move_iterator(shard.rows.end()));
      shard.rows.clear();
    }
    peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
    return;
  }
  for (Row& row : shard.rows) {
    AddRow(std::move(row));
  }
  shard.rows.clear();
  shard.ReleaseCharge();
}

Status Relation::SealSpillTail() {
  if (!spill_tail_) {
    return Status::kSuccess;
  }
  const Status status = spill_tail_->FinishWriting();
  if (status != Status::kSuccess) {
    if (spill_error_ == Status::kSuccess) {
      spill_error_ = status;
    }
    return status;
  }
  if (spill_tail_->Count() == 0) {
    spill_tail_.reset();
  } else if (!spill && extra_spills.empty()) {
    spill = std::move(spill_tail_);
  } else {
    extra_spills.push_back(std::move(spill_tail_));
  }
  return Status::kSuccess;
}

Status Relation::FinishSpill() {
  if (spill_tail_) {
    const Status status = spill_tail_->FinishWriting();
    if (status != Status::kSuccess && spill_error_ == Status::kSuccess) {
      spill_error_ = status;
    }
    return spill_error_;
  }
  return spill_error_;
}

Status Relation::TakeRowAndSpillBacking(
    std::vector<Row>* out_rows,
    std::vector<std::shared_ptr<SpillFile>>* out_spills) {
  if (HasSpill() && !rows.empty()) {
    EnsureSpill();
  }
  RETURN_IF_FAIL(FinishSpill());
  out_rows->swap(rows);
  ClearRelationRows(rows);
  rows.shrink_to_fit();
  out_spills->clear();
  auto append_spill = [&](std::shared_ptr<SpillFile>& f) {
    if (f && f->Count() > 0) {
      out_spills->push_back(std::move(f));
    } else {
      f.reset();
    }
  };
  append_spill(spill);
  for (auto& extra : extra_spills) {
    append_spill(extra);
  }
  extra_spills.clear();
  append_spill(spill_tail_);
  spilled_rows_ = 0;
  uniform_cell_types = false;
  ReleaseCharge();
  return Status::kSuccess;
}

void Relation::ResetContents() {
  uniform_cell_types = false;
  ClearRelationRows(rows);
  rows.shrink_to_fit();
  columnar.reset();
  spill.reset();
  extra_spills.clear();
  spill_tail_.reset();
  spilled_rows_ = 0;
  ReleaseCharge();
}

void NoteRelationSpill(ExecutionRuntime* runtime) {
  if (runtime != nullptr) {
    ++runtime->relation_spills;
  }
}

StatusOr<Relation> MaterializeRelation(const Relation& source) {
  Relation out(source.runtime());
  out.schema = source.schema;
  out.using_columns = source.using_columns;
  Status copied = source.GetStatus();
  if (copied == Status::kSuccess && source.HasColumnarOnly()) {
    out.SetColumnarTable(source.columnar, source.uniform_cell_types);
  } else if (copied == Status::kSuccess && !source.HasSpill() &&
             !source.rows.empty()) {
    const size_t workers = ParallelWorkerCount(source.rows.size() / 4096);
    if (workers > 1) {
      std::vector<ExecutionRuntime> runtimes(workers);
      std::vector<Relation> shards(workers);
      for (size_t w = 0; w < workers; ++w) {
        shards[w].set_runtime(&runtimes[w]);
      }
      copied = RunParallelTasks(workers, [&](size_t w) {
        const size_t begin = source.rows.size() * w / workers;
        const size_t end = source.rows.size() * (w + 1) / workers;
        shards[w].rows.reserve(end - begin);
        for (size_t r = begin; r < end; ++r) {
          shards[w].AddRow(source.rows[r]);
        }
        if (source.uniform_cell_types && !shards[w].HasSpill()) {
          shards[w].uniform_cell_types = true;
        }
      });
      if (copied == Status::kSuccess) {
        for (size_t w = 0; w < workers; ++w) {
          if (out.runtime() != nullptr) {
            out.runtime()->relation_spills += runtimes[w].relation_spills;
          }
        }
        const Status merged = Relation::MergeShardsInto(shards, &out);
        if (copied == Status::kSuccess) {
          copied = merged;
        }
        if (source.uniform_cell_types && !out.HasSpill()) {
          out.uniform_cell_types = true;
        }
      }
    } else {
      out.rows.reserve(source.rows.size());
      for (const Row& row : source.rows) {
        out.AddRow(row);
      }
      if (source.uniform_cell_types && !out.HasSpill()) {
        out.uniform_cell_types = true;
      }
    }
  } else if (copied == Status::kSuccess) {
    copied = source.ForEachRow([&](const Row& row) { out.AddRow(row); });
  }
  if (copied == Status::kSuccess) {
    copied = source.GetStatus();
  }
  if (copied == Status::kSuccess) {
    copied = out.FinishSpill();
  }
  if (copied == Status::kSuccess) {
    copied = out.GetStatus();
  }
  if (copied != Status::kSuccess) {
    return copied;
  }
  return out;
}

void CopyExecutionStats(Relation* destination, const Relation& source) {
  destination->set_runtime(source.runtime());
  destination->hash_joins = source.hash_joins;
  destination->hybrid_hash_joins = source.hybrid_hash_joins;
  destination->in_memory_hash_joins = source.in_memory_hash_joins;
  destination->nested_loop_joins = source.nested_loop_joins;
  destination->join_comparisons = source.join_comparisons;
  destination->peak_intermediate_rows = source.peak_intermediate_rows;
  destination->using_columns = source.using_columns;
}

bool ColumnTypeTracker::FinalizeColumns(
    std::span<const std::vector<ColumnTypeTracker>> per_worker,
    std::vector<Column>& columns) {
  bool all_uniform = !columns.empty();
  for (size_t i = 0; i < columns.size(); ++i) {
    ColumnTypeTracker merged;
    for (const auto& worker : per_worker) {
      if (i < worker.size()) {
        merged.MergeFrom(worker[i]);
      }
    }
    if (merged.mixed) {
      columns[i] = Column(columns[i].Name(), ValueType::kNull);
      all_uniform = false;
    } else if (merged.seen_non_null) {
      columns[i].SetType(merged.type);
      columns[i].SetInterval(merged.is_interval);
      columns[i].SetUnsigned(merged.is_unsigned);
      if (merged.saw_unsafe || merged.type == ValueType::kNull ||
          merged.type == ValueType::kArray) {
        all_uniform = false;
      }
    } else if (merged.saw_unsafe || columns[i].Type() == ValueType::kNull ||
               columns[i].Type() == ValueType::kArray) {
      all_uniform = false;
    }
  }
  return all_uniform;
}

}  // namespace tinylamb::relational_detail
