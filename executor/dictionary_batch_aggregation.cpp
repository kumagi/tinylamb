/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/dictionary_batch_aggregation.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

void DictionaryBatchAggregation::Accumulator::Accumulate(const Value& val,
                                                         AggOp op) {
  if (val.IsNull()) {
    if (op == AggOp::kCount) {
      // count(col) ignores null, but count(*) counts nulls
    }
    return;
  }
  ++count;
  if (!has_val) {
    has_val = true;
    min_val = val;
    max_val = val;
  } else {
    if (val < min_val) min_val = val;
    if (max_val < val) max_val = val;
  }

  if (val.type == ValueType::kInt64) {
    sum += static_cast<double>(val.value.int_value);
  } else if (val.type == ValueType::kDouble) {
    sum += val.value.double_value;
  }
}

Value DictionaryBatchAggregation::Accumulator::Finalize(AggOp op) const {
  switch (op) {
    case AggOp::kCount:
      return Value(count);
    case AggOp::kSum:
      return has_val ? Value(sum) : Value();
    case AggOp::kAvg:
      return (count > 0) ? Value(sum / static_cast<double>(count)) : Value();
    case AggOp::kMin:
      return has_val ? min_val : Value();
    case AggOp::kMax:
      return has_val ? max_val : Value();
  }
  return Value();
}

DictionaryBatchAggregation::DictionaryBatchAggregation(Schema schema,
                                                       slot_t group_slot,
                                                       slot_t agg_slot,
                                                       AggOp op)
    : schema_(std::move(schema)),
      group_slot_(group_slot),
      agg_slot_(agg_slot),
      op_(op) {}

uint32_t DictionaryBatchAggregation::GetOrCreateCode(const Value& group_val) {
  auto it = code_map_.find(group_val);
  if (it != code_map_.end()) {
    return it->second;
  }
  const uint32_t new_code = static_cast<uint32_t>(dictionary_.size());
  dictionary_.push_back(group_val);
  code_map_[group_val] = new_code;
  accumulators_.emplace_back();
  return new_code;
}

void DictionaryBatchAggregation::AccumulateChunk(const DataChunk& chunk) {
  const auto& group_col = chunk.ColumnAt(group_slot_);
  const auto& agg_col = chunk.ColumnAt(agg_slot_);
  const size_t rows = chunk.Size();

  for (size_t i = 0; i < rows; ++i) {
    const Value group_val = group_col.ValueAt(i);
    const uint32_t code = GetOrCreateCode(group_val);
    const Value agg_val = agg_col.ValueAt(i);
    accumulators_[code].Accumulate(agg_val, op_);
  }
}

DataChunk DictionaryBatchAggregation::EmitResult(
    const Schema& output_schema) const {
  DataChunk result(output_schema, dictionary_.size());
  for (size_t i = 0; i < dictionary_.size(); ++i) {
    const Value final_agg = accumulators_[i].Finalize(op_);
    result.Append(Row({dictionary_[i], final_agg}));
  }
  return result;
}

}  // namespace tinylamb
