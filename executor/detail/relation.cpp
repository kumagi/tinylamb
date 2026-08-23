/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/relation.hpp"

#include <algorithm>
#include <utility>
#include <memory>
#include <cstddef>

#include "executor/detail/subquery_runtime.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/row.hpp"

namespace tinylamb::relational_detail {

Relation::Relation(Relation&& other) noexcept
    : schema(std::move(other.schema)),
      rows(std::move(other.rows)),
      spill(std::move(other.spill)),
      spill_tail_(std::move(other.spill_tail_)),
      charged_bytes_(other.charged_bytes_),
      hash_joins(other.hash_joins),
      hybrid_hash_joins(other.hybrid_hash_joins),
      in_memory_hash_joins(other.in_memory_hash_joins),
      nested_loop_joins(other.nested_loop_joins),
      join_comparisons(other.join_comparisons),
      peak_intermediate_rows(other.peak_intermediate_rows),
      spilled_rows_(other.spilled_rows_) {
  other.charged_bytes_ = 0;
  other.hash_joins = 0;
  other.hybrid_hash_joins = 0;
  other.in_memory_hash_joins = 0;
  other.nested_loop_joins = 0;
  other.join_comparisons = 0;
  other.peak_intermediate_rows = 0;
  other.spilled_rows_ = 0;
}

Relation& Relation::operator=(Relation&& other) noexcept {
  if (this != &other) {
    ReleaseCharge();
    schema = std::move(other.schema);
    rows = std::move(other.rows);
    spill = std::move(other.spill);
    spill_tail_ = std::move(other.spill_tail_);
    charged_bytes_ = other.charged_bytes_;
    other.charged_bytes_ = 0;
    hash_joins = other.hash_joins;
    hybrid_hash_joins = other.hybrid_hash_joins;
    in_memory_hash_joins = other.in_memory_hash_joins;
    nested_loop_joins = other.nested_loop_joins;
    join_comparisons = other.join_comparisons;
    peak_intermediate_rows = other.peak_intermediate_rows;
    spilled_rows_ = other.spilled_rows_;
    other.hash_joins = 0;
    other.hybrid_hash_joins = 0;
    other.in_memory_hash_joins = 0;
    other.nested_loop_joins = 0;
    other.join_comparisons = 0;
    other.peak_intermediate_rows = 0;
    other.spilled_rows_ = 0;
  }
  return *this;
}

Relation::~Relation() { ReleaseCharge(); }

void Relation::ReleaseCharge() {
  if (charged_bytes_ != 0) {
    QueryMemoryBudget::Global().Release(charged_bytes_);
    charged_bytes_ = 0;
  }
}

void Relation::EnsureSpill() {
  if (spill) {
    return;
  }
  NoteRelationSpill();
  spill = std::make_shared<SpillFile>();
  for (const Row& row : rows) {
    spill->Append(row);
  }
  spill->FinishWriting();
  rows.clear();
  rows.shrink_to_fit();
  ReleaseCharge();
  spill_tail_ = std::make_shared<SpillFile>();
}

void Relation::AddRow(Row row) {
  const size_t bytes = EstimateRowBytes(row);
  if (spill_tail_ || !QueryMemoryBudget::Global().CanReserve(bytes)) {
    if (!spill_tail_) {
      EnsureSpill();
    }
    spill_tail_->Append(row);
    peak_intermediate_rows =
        std::max(peak_intermediate_rows, rows.size() + spilled_rows_ + 1);
    ++spilled_rows_;
    return;
  }
  QueryMemoryBudget::Global().ReserveForced(bytes);
  charged_bytes_ += bytes;
  rows.push_back(std::move(row));
  peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
}

void Relation::FinishSpill() {
  if (spill_tail_) {
    spill_tail_->FinishWriting();
  }
}

void Relation::ResetContents() {
  rows.clear();
  rows.shrink_to_fit();
  spill.reset();
  spill_tail_.reset();
  spilled_rows_ = 0;
  ReleaseCharge();
}

void NoteRelationSpill() {
  if (active_runtime != nullptr) {
    ++active_runtime->relation_spills;
  }
}

Relation MaterializeRelation(const Relation& source) {
  Relation out;
  out.schema = source.schema;
  source.ForEachRow([&](const Row& row) { out.AddRow(row); });
  return out;
}

void CopyExecutionStats(Relation* destination, const Relation& source) {
  destination->hash_joins = source.hash_joins;
  destination->hybrid_hash_joins = source.hybrid_hash_joins;
  destination->in_memory_hash_joins = source.in_memory_hash_joins;
  destination->nested_loop_joins = source.nested_loop_joins;
  destination->join_comparisons = source.join_comparisons;
  destination->peak_intermediate_rows = source.peak_intermediate_rows;
}

}  // namespace tinylamb::relational_detail
