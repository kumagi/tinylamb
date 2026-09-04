/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/distributed_agg_finalize.hpp"

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

DistributedAggFinalize::DistributedAggFinalize(
    std::vector<Executor> partial_sources, Schema output_schema,
    std::vector<slot_t> group_cols, std::vector<AggregateSpec> aggregates)
    : sources_(std::move(partial_sources)),
      output_schema_(std::move(output_schema)),
      group_cols_(std::move(group_cols)),
      aggregates_(std::move(aggregates)) {}

std::string DistributedAggFinalize::EncodeGroupKey(const Row& row) const {
  std::string key;
  for (slot_t col : group_cols_) {
    if (col < row.Size()) {
      key += row[col].EncodeMemcomparableFormat();
    }
  }
  return key;
}

void DistributedAggFinalize::MergePartialStreams() {
  group_map_.clear();

  for (auto& src : sources_) {
    if (!src) continue;
    Row row;
    RowPosition rp;
    while (src->Next(&row, &rp)) {
      const std::string key = EncodeGroupKey(row);
      auto& group = group_map_[key];
      if (group.group_values.empty()) {
        group.group_values.reserve(group_cols_.size());
        for (slot_t col : group_cols_) {
          group.group_values.push_back(col < row.Size() ? row[col] : Value());
        }
        group.states.resize(aggregates_.size());
      }

      for (size_t a = 0; a < aggregates_.size(); ++a) {
        const auto& spec = aggregates_[a];
        auto& state = group.states[a];
        const Value val = spec.partial_val_slot < row.Size()
                              ? row[spec.partial_val_slot]
                              : Value();

        switch (spec.type) {
          case AggType::kCount:
            if (!val.IsNull()) {
              state.count += val.value.int_value;
            }
            break;
          case AggType::kSum:
            if (!val.IsNull()) {
              state.has_val = true;
              if (val.type == ValueType::kInt64) {
                state.sum += static_cast<double>(val.value.int_value);
              } else if (val.type == ValueType::kDouble) {
                state.sum += val.value.double_value;
              }
            }
            break;
          case AggType::kAvg: {
            if (!val.IsNull()) {
              state.has_val = true;
              if (val.type == ValueType::kInt64) {
                state.sum += static_cast<double>(val.value.int_value);
              } else if (val.type == ValueType::kDouble) {
                state.sum += val.value.double_value;
              }
            }
            const Value cnt_val = spec.partial_count_slot < row.Size()
                                      ? row[spec.partial_count_slot]
                                      : Value();
            if (!cnt_val.IsNull()) {
              state.count += cnt_val.value.int_value;
            }
            break;
          }
          case AggType::kMin:
            if (!val.IsNull()) {
              if (!state.has_val || val < state.min_val) {
                state.min_val = val;
                state.has_val = true;
              }
            }
            break;
          case AggType::kMax:
            if (!val.IsNull()) {
              if (!state.has_val || state.max_val < val) {
                state.max_val = val;
                state.has_val = true;
              }
            }
            break;
        }
      }
    }
  }
}

void DistributedAggFinalize::FinalizeGroups() {
  finalized_rows_.clear();
  finalized_rows_.reserve(group_map_.size());

  for (auto& [key, group] : group_map_) {
    std::vector<Value> row_values;
    row_values.reserve(group.group_values.size() + aggregates_.size());
    for (const auto& gv : group.group_values) {
      row_values.push_back(gv);
    }

    for (size_t a = 0; a < aggregates_.size(); ++a) {
      const auto& spec = aggregates_[a];
      const auto& state = group.states[a];

      switch (spec.type) {
        case AggType::kCount:
          row_values.emplace_back(state.count);
          break;
        case AggType::kSum:
          row_values.push_back(state.has_val ? Value(state.sum) : Value());
          break;
        case AggType::kAvg:
          row_values.push_back(
              state.count > 0
                  ? Value(state.sum / static_cast<double>(state.count))
                  : Value());
          break;
        case AggType::kMin:
          row_values.push_back(state.has_val ? state.min_val : Value());
          break;
        case AggType::kMax:
          row_values.push_back(state.has_val ? state.max_val : Value());
          break;
      }
    }

    finalized_rows_.emplace_back(Row(std::move(row_values)), RowPosition());
  }

  size_t bytes = finalized_rows_.size() * sizeof(std::pair<Row, RowPosition>);
  charge_.Add(bytes);
}

void DistributedAggFinalize::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  output_offset_ = 0;
  MergePartialStreams();
  FinalizeGroups();
}

void DistributedAggFinalize::MaterializePipeline() { EnsureMaterialized(); }

bool DistributedAggFinalize::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();
  if (output_offset_ >= finalized_rows_.size()) {
    return false;
  }
  *dst = finalized_rows_[output_offset_].first;
  if (rp != nullptr) {
    *rp = finalized_rows_[output_offset_].second;
  }
  ++output_offset_;
  return true;
}

size_t DistributedAggFinalize::NextBatch(DataChunk* destination,
                                         size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  destination->Reset();
  EnsureMaterialized();
  if (output_offset_ >= finalized_rows_.size()) {
    return 0;
  }
  const size_t count =
      std::min(max_rows, finalized_rows_.size() - output_offset_);
  for (size_t i = 0; i < count; ++i) {
    destination->Append(finalized_rows_[output_offset_ + i].first,
                        finalized_rows_[output_offset_ + i].second);
  }
  output_offset_ += count;
  return count;
}

void DistributedAggFinalize::Dump(std::ostream& o, int /*indent*/) const {
  o << "DistributedAggFinalize(sources=" << sources_.size()
    << ", groups=" << finalized_rows_.size() << ")";
}

void DistributedAggFinalize::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
