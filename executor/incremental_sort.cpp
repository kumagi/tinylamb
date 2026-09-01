/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/incremental_sort.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/pdqsort.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

IncrementalSortExecutor::IncrementalSortExecutor(
    Executor source, Schema schema, std::vector<SortExecutor::Key> prefix_keys,
    std::vector<SortExecutor::Key> suffix_keys)
    : source_(std::move(source)),
      schema_(std::move(schema)),
      prefix_keys_(std::move(prefix_keys)),
      suffix_keys_(std::move(suffix_keys)) {}

bool IncrementalSortExecutor::ArePrefixEqual(const Row& a, const Row& b) const {
  for (const auto& key : prefix_keys_) {
    Value va = key.expression->Evaluate(a, schema_);
    Value vb = key.expression->Evaluate(b, schema_);
    if (va != vb) {
      return false;
    }
  }
  return true;
}

void IncrementalSortExecutor::ExecuteIncrementalSort() {
  output_.clear();
  output_offset_ = 0;

  std::vector<std::pair<Row, RowPosition>> input_rows;
  Row row;
  RowPosition rp;
  size_t total_bytes = 0;

  while (source_ && source_->Next(&row, &rp)) {
    total_bytes += EstimateRowBytes(row) + sizeof(RowPosition);
    input_rows.emplace_back(std::move(row), rp);
  }

  if (input_rows.empty()) {
    return;
  }

  size_t start = 0;
  while (start < input_rows.size()) {
    size_t end = start + 1;
    while (end < input_rows.size() &&
           ArePrefixEqual(input_rows[start].first, input_rows[end].first)) {
      ++end;
    }

    // Sort sub-range [start, end) on suffix_keys_
    std::vector<std::pair<Row, RowPosition>> group_rows(
        std::make_move_iterator(input_rows.begin() + start),
        std::make_move_iterator(input_rows.begin() + end));

    if (!suffix_keys_.empty() && group_rows.size() > 1) {
      PdqSort::Sort(group_rows, schema_, suffix_keys_);
    }

    for (auto& item : group_rows) {
      output_.push_back(std::move(item));
    }

    start = end;
  }

  charge_.Add(total_bytes);
}

void IncrementalSortExecutor::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  ExecuteIncrementalSort();
}

void IncrementalSortExecutor::MaterializePipeline() {
  EnsureMaterialized();
}

bool IncrementalSortExecutor::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();
  if (output_offset_ >= output_.size()) {
    return false;
  }
  *dst = output_[output_offset_].first;
  if (rp != nullptr) {
    *rp = output_[output_offset_].second;
  }
  ++output_offset_;
  return true;
}

size_t IncrementalSortExecutor::NextBatch(DataChunk* destination,
                                         size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  EnsureMaterialized();
  if (output_offset_ >= output_.size()) {
    return 0;
  }
  const size_t count = std::min(max_rows, output_.size() - output_offset_);
  for (size_t i = 0; i < count; ++i) {
    destination->Append(output_[output_offset_ + i].first,
                        output_[output_offset_ + i].second);
  }
  output_offset_ += count;
  return count;
}

void IncrementalSortExecutor::Dump(std::ostream& o, int indent) const {
  o << "IncrementalSort";
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
    << Indent(indent + 2);
  source_->Dump(o, indent + 2);
}

void IncrementalSortExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
