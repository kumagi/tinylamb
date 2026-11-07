/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/incremental_sort.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <iostream>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/pdqsort.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// A bounded view of one peer group. The sort owns this view while consuming
// the group, and one lookahead row is returned to the outer streaming cursor.
class PrefixRun final : public ExecutorBase {
 public:
  PrefixRun(Executor source, Row first, RowPosition position,
            std::optional<std::pair<Row, RowPosition>> second,
            std::function<StatusOr<bool>(const Row&, const Row&)> peers,
            std::function<void(Row, RowPosition)> next_group,
            std::function<void()> exhausted)
      : source_(std::move(source)),
        first_(std::move(first)),
        position_(position),
        second_(std::move(second)),
        peers_(std::move(peers)),
        next_group_(std::move(next_group)),
        exhausted_(std::move(exhausted)) {}
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    while (!done_ && destination->Size() < max_rows) {
      if (first_pending_) {
        first_pending_ = false;
        destination->Append(first_, position_);
        continue;
      }
      if (second_) {
        destination->Append(std::move(second_->first), second_->second);
        second_.reset();
        continue;
      }
      DataChunk candidate_chunk;
      if (source_->NextBatch(&candidate_chunk, 1) == 0) {
        done_ = true;
        exhausted_();
        FailWithChildOf(*source_);
        break;
      }
      Row candidate = candidate_chunk.RowAt(0);
      const RowPosition candidate_position = candidate_chunk.PositionAt(0);
      auto equal = peers_(first_, candidate);
      if (!equal.HasValue()) {
        done_ = true;
        FailWith(equal.GetStatus());
        return 0;
      }
      if (!equal.Value()) {
        done_ = true;
        next_group_(std::move(candidate), candidate_position);
        break;
      }
      destination->Append(std::move(candidate), candidate_position);
    }
    return destination->Size();
  }
  void Dump(std::ostream& out, int) const override { out << "PrefixRun"; }

 private:
  Executor source_;
  Row first_;
  RowPosition position_;
  std::optional<std::pair<Row, RowPosition>> second_;
  std::function<StatusOr<bool>(const Row&, const Row&)> peers_;
  std::function<void(Row, RowPosition)> next_group_;
  std::function<void()> exhausted_;
  bool first_pending_{true};
  bool done_{false};
};
}  // namespace

IncrementalSortExecutor::IncrementalSortExecutor(
    Executor source, Schema schema, std::vector<SortExecutor::Key> prefix_keys,
    std::vector<SortExecutor::Key> suffix_keys)
    : source_(std::move(source)),
      schema_(std::move(schema)),
      prefix_keys_(std::move(prefix_keys)),
      suffix_keys_(std::move(suffix_keys)) {
  singleton_passthrough_ =
      !suffix_keys_.empty() &&
      std::ranges::all_of(suffix_keys_, [&](const auto& key) {
        return key.expression &&
               key.expression->Type() == TypeTag::kColumnValue &&
               schema_.Offset(
                   key.expression->AsColumnValue().GetColumnName()) >= 0;
      });
}

bool IncrementalSortExecutor::ArePrefixEqual(const Row& a, const Row& b) {
  return std::ranges::all_of(prefix_keys_, [&](const auto& key) {
    StatusOr<Value> va_or = key.expression->TryEvaluate(a, schema_);
    StatusOr<Value> vb_or = key.expression->TryEvaluate(b, schema_);
    if (!va_or.HasValue() || !vb_or.HasValue()) {
      if (sort_error_ == Status::kSuccess) {
        sort_error_ = va_or.HasValue() ? vb_or.GetStatus() : va_or.GetStatus();
      }
      return false;
    }
    // Peers must collapse the way the ordering groups them, so a
    // case-insensitive prefix key starts a new run at 'a' vs 'A' instead of
    // treating the already-sorted run as a single group. A binary prefix key
    // keeps Value's equality.
    return EqualUnderCollation(va_or.Value(), vb_or.Value());
  });
}

bool IncrementalSortExecutor::NextStreaming(Row* dst, RowPosition* rp) {
  if (GetStatus() != Status::kSuccess) return false;
  if (sort_error_ != Status::kSuccess) return FailWith(sort_error_);
  for (;;) {
    if (group_sort_) {
      DataChunk group_chunk;
      if (group_sort_->NextBatch(&group_chunk, 1) != 0) {
        *dst = group_chunk.RowAt(0);
        if (rp) *rp = group_chunk.PositionAt(0);
        return true;
      }
      if (FailWithChildOf(*group_sort_)) return false;
      group_sort_.reset();
    }
    if (!has_pending_) {
      if (input_exhausted_ || !source_) return false;
      DataChunk pending_chunk;
      if (source_->NextBatch(&pending_chunk, 1) == 0) {
        input_exhausted_ = true;
        FailWithChildOf(*source_);
        return false;
      }
      pending_row_ = pending_chunk.RowAt(0);
      pending_position_ = pending_chunk.PositionAt(0);
    }
    has_pending_ = false;
    std::optional<std::pair<Row, RowPosition>> second;
    if (singleton_passthrough_ &&
        pending_row_.Count() == schema_.ColumnCount()) {
      Row first = std::move(pending_row_);
      const RowPosition first_position = pending_position_;
      DataChunk candidate_chunk;
      if (source_->NextBatch(&candidate_chunk, 1) == 0) {
        input_exhausted_ = true;
        if (FailWithChildOf(*source_)) return false;
        *dst = std::move(first);
        if (rp) *rp = first_position;
        ++singleton_runs_;
        return true;
      }
      Row candidate = candidate_chunk.RowAt(0);
      const RowPosition candidate_position = candidate_chunk.PositionAt(0);
      const bool equal = ArePrefixEqual(first, candidate);
      if (sort_error_ != Status::kSuccess) return FailWith(sort_error_);
      if (!equal) {
        pending_row_ = std::move(candidate);
        pending_position_ = candidate_position;
        has_pending_ = true;
        *dst = std::move(first);
        if (rp) *rp = first_position;
        ++singleton_runs_;
        return true;
      }
      pending_row_ = std::move(first);
      pending_position_ = first_position;
      second.emplace(std::move(candidate), candidate_position);
    }
    auto run = std::make_shared<PrefixRun>(
        source_, std::move(pending_row_), pending_position_, std::move(second),
        [this](const Row& a, const Row& b) -> StatusOr<bool> {
          const bool equal = ArePrefixEqual(a, b);
          if (sort_error_ != Status::kSuccess) return sort_error_;
          return equal;
        },
        [this](Row next, RowPosition position) {
          pending_row_ = std::move(next);
          pending_position_ = position;
          has_pending_ = true;
        },
        [this] { input_exhausted_ = true; });
    // SortExecutor already enforces the memory budget and spills a large
    // peer group. Only that group, rather than the entire child, is consumed.
    group_sort_ = std::make_shared<SortExecutor>(run, schema_, suffix_keys_);
  }
}

void IncrementalSortExecutor::ExecuteIncrementalSort() {
  Row row;
  RowPosition position;
  while (NextStreaming(&row, &position)) {
    charge_.Add(EstimateRowBytes(row) + sizeof(RowPosition));
    output_.emplace_back(std::move(row), position);
  }
}

void IncrementalSortExecutor::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  ExecuteIncrementalSort();
  materialized_ = true;
}

void IncrementalSortExecutor::MaterializePipeline() { EnsureMaterialized(); }

size_t IncrementalSortExecutor::NextBatch(DataChunk* destination,
                                          size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  destination->Reset();
  if (GetStatus() != Status::kSuccess) return 0;
  if (!materialized_) {
    Row row;
    RowPosition position;
    while (destination->Size() < max_rows && NextStreaming(&row, &position)) {
      destination->Append(std::move(row), position);
    }
    return destination->Size();
  }
  if (sort_error_ != Status::kSuccess) {
    FailWith(sort_error_);
    return 0;
  }
  while (destination->Size() < max_rows && output_offset_ < output_.size()) {
    destination->Append(output_[output_offset_].first,
                        output_[output_offset_].second);
    ++output_offset_;
  }
  return destination->Size();
}

void IncrementalSortExecutor::Dump(std::ostream& o, int indent) const {
  o << "IncrementalSort";
  if (singleton_runs_ != 0) o << " singleton_runs=" << singleton_runs_;
  if (!prefix_keys_.empty() && prefix_keys_.front().expression) {
    const Expression& prefix = prefix_keys_.front().expression;
    if (prefix->Type() == TypeTag::kColumnValue) {
      o << " presorted=" << prefix->AsColumnValue().GetColumnName().name;
    } else {
      o << " presorted=" << prefix->ToString();
    }
  }
  o << " (prefix_keys=" << prefix_keys_.size()
    << ", suffix_keys=" << suffix_keys_.size() << ")\n"
    << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(o, indent + 2);
}

void IncrementalSortExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
