/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/pipeline/pipeline_ops.hpp"

#include <algorithm>
#include <utility>

namespace tinylamb {

StatusOr<bool> ChunkVectorSource::NextBatch(PipelineContext* /*ctx*/,
                                            DataChunk* out, size_t max_rows) {
  out->Reset();
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
  const size_t end = std::min(cur.Size(), row_in_chunk_ + max_rows);
  out->Reserve(end - row_in_chunk_);
  for (size_t r = row_in_chunk_; r < end; ++r) {
    out->Append(cur.RowAt(r), cur.PositionAt(r));
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
  if (!store_ || max_rows == 0) {
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
  const size_t end = std::min(active_chunk_.Size(), active_row_ + max_rows);
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
  if (idx_ >= rows_.size() || max_rows == 0) {
    rows_.clear();
    return false;
  }
  const size_t end = std::min(rows_.size(), idx_ + max_rows);
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

class FilterOp::FeedAdapter final : public ExecutorBase {
 public:
  void SetChunk(const DataChunk* chunk) {
    chunk_ = chunk;
    fed_ = false;
  }
  size_t NextBatch(DataChunk* destination, size_t /*max_rows*/) override {
    destination->Reset();
    if (fed_ || chunk_ == nullptr || chunk_->Empty()) {
      return 0;
    }
    fed_ = true;
    *destination = *chunk_;
    return destination->Size();
  }
  void Dump(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ') << "FeedAdapter";
  }

 private:
  const DataChunk* chunk_{nullptr};
  bool fed_{false};
};

FilterOp::FilterOp(Expression predicate, Schema input_schema)
    : predicate_(std::move(predicate)),
      input_schema_(std::move(input_schema)),
      feed_(std::make_shared<FeedAdapter>()),
      selection_(
          std::make_unique<Selection>(predicate_, input_schema_, feed_)) {}

Status FilterOp::Process(PipelineContext* /*ctx*/, const DataChunk& input,
                         DataChunk* output, bool* exhausted) {
  (void)exhausted;
  output->Reset();
  if (input.Empty()) {
    return Status::kSuccess;
  }
  feed_->SetChunk(&input);
  // The feed only exposes the actual input chunk, so asking for a full
  // 1024-row batch makes the child reserve storage it never fills.
  const size_t n = std::max<size_t>(input.Size(), 1);
  selection_->NextBatch(output, n);
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

class ProjectOp::FeedAdapter final : public ExecutorBase {
 public:
  void SetChunk(const DataChunk* chunk) {
    chunk_ = chunk;
    fed_ = false;
  }
  size_t NextBatch(DataChunk* destination, size_t /*max_rows*/) override {
    destination->Reset();
    if (fed_ || chunk_ == nullptr || chunk_->Empty()) {
      return 0;
    }
    fed_ = true;
    *destination = *chunk_;
    return destination->Size();
  }
  void Dump(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ') << "FeedAdapter";
  }

 private:
  const DataChunk* chunk_{nullptr};
  bool fed_{false};
};

ProjectOp::ProjectOp(std::vector<NamedExpression> expressions,
                     Schema input_schema)
    : expressions_(std::move(expressions)),
      input_schema_(std::move(input_schema)),
      feed_(std::make_shared<FeedAdapter>()),
      projection_(
          std::make_unique<Projection>(expressions_, input_schema_, feed_)) {}

Status ProjectOp::Process(PipelineContext* /*ctx*/, const DataChunk& input,
                          DataChunk* output, bool* exhausted) {
  (void)exhausted;
  output->Reset();
  if (input.Empty()) {
    return Status::kSuccess;
  }
  feed_->SetChunk(&input);
  // The feed only exposes the actual input chunk, so asking for a full
  // 1024-row batch makes the child reserve storage it never fills.
  const size_t n = std::max<size_t>(input.Size(), 1);
  projection_->NextBatch(output, n);
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
