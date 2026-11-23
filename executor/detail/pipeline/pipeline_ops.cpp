/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/pipeline/pipeline_ops.hpp"

#include <algorithm>
#include <utility>

namespace tinylamb {

StatusOr<bool> ChunkVectorSource::NextBatch(PipelineContext* /*ctx*/,
                                            DataChunk* out, size_t max_rows) {
  out->Reset();
  // A zero-row request produces no batch; false here means "no rows emitted
  // by this call", not that the input is exhausted. State is deliberately left
  // untouched so a later call with a positive limit still drains the remaining
  // chunks, matching the other pipeline sources' contract.
  if (max_rows == 0) {
    return false;
  }
  while (idx_ < chunks_.size() &&
         (chunks_[idx_].Empty() || row_in_chunk_ >= chunks_[idx_].Size())) {
    ++idx_;
    row_in_chunk_ = 0;
  }
  if (idx_ >= chunks_.size()) {
    chunks_.clear();
    return false;
  }
  DataChunk& cur = chunks_[idx_];
  if (row_in_chunk_ == 0 && cur.Size() <= max_rows) {
    *out = std::move(cur);
    ++idx_;
    if (idx_ >= chunks_.size()) {
      chunks_.clear();
    }
    return true;
  }
  const size_t end = row_in_chunk_ +
                     std::min(cur.Size() - row_in_chunk_, max_rows);
  // Append column-wise from the source so ColumnVector metadata (collation,
  // unsigned flag, heterogeneous layout) is carried over through
  // ObserveSourceCollation; RowAt()+Append() would infer it from the row's
  // per-value tags only and lose a column-level collation.
  for (size_t r = row_in_chunk_; r < end; ++r) {
    out->AppendFrom(cur, r);
  }
  row_in_chunk_ = end;
  if (row_in_chunk_ >= cur.Size()) {
    cur.Reset();
    ++idx_;
    row_in_chunk_ = 0;
    if (idx_ >= chunks_.size()) {
      chunks_.clear();
    }
  }
  return true;
}

StatusOr<bool> SpillableStoreSource::NextBatch(PipelineContext* /*ctx*/,
                                               DataChunk* out,
                                               size_t max_rows) {
  out->Reset();
  if (!store_) {
    return false;
  }
  // As with ChunkVectorSource, a zero-row request is "no rows this call", not
  // end of input: do not consume or clear the store, so a later positive-limit
  // call can still read from where this one stopped.
  if (max_rows == 0) {
    return false;
  }
  while (active_row_ >= active_chunk_.Size()) {
    active_chunk_.Reset();
    active_row_ = 0;
    ASSIGN_OR_RETURN(bool, has, store_->NextChunk(&active_chunk_));
    if (!has) {
      return false;
    }
  }
  if (active_row_ == 0 && active_chunk_.Size() <= max_rows) {
    *out = std::move(active_chunk_);
    active_chunk_.Reset();
    return true;
  }
  const size_t end = active_row_ +
                     std::min(active_chunk_.Size() - active_row_, max_rows);
  out->Reserve(end - active_row_);
  for (size_t r = active_row_; r < end; ++r) {
    out->Append(active_chunk_.RowAt(r), active_chunk_.PositionAt(r));
  }
  active_row_ = end;
  if (active_row_ >= active_chunk_.Size()) {
    active_chunk_.Reset();
    active_row_ = 0;
  }
  return true;
}

StatusOr<bool> RowVectorSource::NextBatch(PipelineContext* /*ctx*/,
                                          DataChunk* out, size_t max_rows) {
  out->Reset();
  if (max_rows == 0) {
    return false;
  }
  if (idx_ >= rows_.size()) {
    rows_.clear();
    return false;
  }
  const size_t end = idx_ + std::min(rows_.size() - idx_, max_rows);
  out->Reserve(end - idx_);
  for (size_t i = idx_; i < end; ++i) {
    out->Append(std::move(rows_[i]));
  }
  idx_ = end;
  if (idx_ >= rows_.size()) {
    rows_.clear();
  }
  return true;
}

FilterOp::FilterOp(Expression predicate, Schema input_schema)
    : predicate_(std::move(predicate)),
      input_schema_(std::move(input_schema)),
      selection_(
          std::make_unique<Selection>(predicate_, input_schema_, nullptr)) {}

Status FilterOp::Process(PipelineContext* /*ctx*/, const DataChunk& input,
                         DataChunk* output, bool* exhausted) {
  (void)exhausted;
  output->Reset();
  if (input.Empty()) {
    return Status::kSuccess;
  }
  output->Reset(input_schema_, input.Size());
  selection_->AppendSelected(input, output);
  if (selection_->GetStatus() != Status::kSuccess) {
    return selection_->GetStatus();
  }
  return Status::kSuccess;
}

void FilterOp::DumpExplain(std::ostream& out, int indent) const {
  if (selection_ != nullptr) {
    selection_->DumpHeader(out, indent, /*leading_indent=*/true);
    out << "\n";
  } else {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << Describe() << "\n";
  }
}

ProjectOp::ProjectOp(std::vector<NamedExpression> expressions,
                     Schema input_schema)
    : expressions_(std::move(expressions)),
      input_schema_(std::move(input_schema)),
      projection_(
          std::make_unique<Projection>(expressions_, input_schema_, nullptr)) {}

Status ProjectOp::Process(PipelineContext* /*ctx*/, const DataChunk& input,
                          DataChunk* output, bool* exhausted) {
  (void)exhausted;
  output->Reset();
  if (input.Empty()) {
    return Status::kSuccess;
  }
  output->Reserve(input.Size());
  projection_->ProjectChunk(input, output);
  if (projection_->GetStatus() != Status::kSuccess) {
    return projection_->GetStatus();
  }
  return Status::kSuccess;
}

void ProjectOp::DumpExplain(std::ostream& out, int indent) const {
  if (projection_ != nullptr) {
    projection_->DumpHeader(out, indent, /*leading_indent=*/true);
    out << "\n";
  } else {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << Describe() << "\n";
  }
}

void RenameOp::DumpExplain(std::ostream& out, int indent) const {
  out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << "Rename: ";
  if (!physical_.empty()) {
    out << physical_ << " AS ";
  }
  out << relation_ << "\n";
}

Status LimitOp::Process(PipelineContext* /*ctx*/, const DataChunk& input,
                        DataChunk* output, bool* exhausted) {
  output->Reset();
  if (has_limit_ && emitted_ >= limit_) {
    early_stop_ = true;
    *exhausted = true;
    return Status::kSuccess;
  }
  const size_t in_rows = input.Size();
  size_t start = 0;
  if (skipped_ < offset_) {
    const size_t need_skip = offset_ - skipped_;
    if (in_rows <= need_skip) {
      skipped_ += in_rows;
      consumed_rows_ += in_rows;
      return Status::kSuccess;
    }
    start = need_skip;
    consumed_rows_ += need_skip;
    skipped_ = offset_;
  }
  size_t take = in_rows - start;
  if (has_limit_) {
    const size_t rem = limit_ - emitted_;
    if (take >= rem) {
      take = rem;
      early_stop_ = true;
      *exhausted = true;
    }
  }
  for (size_t i = 0; i < take; ++i) {
    output->AppendFrom(input, start + i);
  }
  consumed_rows_ += take;
  emitted_ += take;
  return Status::kSuccess;
}

std::string LimitOp::Describe() const {
  if (!has_limit_) {
    return "Offset count=" + std::to_string(offset_);
  }
  return "Limit count=" + std::to_string(limit_) +
         (offset_ > 0 ? " offset=" + std::to_string(offset_) : "");
}

void LimitOp::Explain(std::ostream& out, int indent) const {
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  const size_t lim = has_limit_ ? limit_ : 0;
  out << pad << "Limit: " << lim << " offset " << offset_
      << " (Limit count=" << lim << " offset=" << offset_ << ")\n";
}

void LimitOp::DumpExplain(std::ostream& out, int indent) const {
  Explain(out, indent);
  if (early_stop_) {
    const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
    out << pad << "Limit early_stop=true consumed_rows=" << consumed_rows_
        << "\n";
  }
}

Status AdjacentDistinctOp::Process(PipelineContext* /*ctx*/,
                                   const DataChunk& input, DataChunk* output,
                                   bool* /*exhausted*/) {
  output->Reset();
  output->Reserve(input.Size());
  for (size_t r = 0; r < input.Size(); ++r) {
    Row row = input.RowAt(r);
    if (!have_prev_ || !DistinctRowEqual{}(prev_, row)) {
      prev_ = row;
      have_prev_ = true;
      output->Append(std::move(row), input.PositionAt(r));
    }
  }
  return Status::kSuccess;
}

}  // namespace tinylamb
