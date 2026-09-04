/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/partial_aggregate.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

struct RowKeyHash {
  size_t operator()(const Row& row) const {
    size_t seed = 0;
    relational_detail::DistinctValueHash hasher;
    for (const Value& v : row.values_) {
      seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

struct RowKeyEqual {
  bool operator()(const Row& a, const Row& b) const {
    if (a.values_.size() != b.values_.size()) {
      return false;
    }
    relational_detail::DistinctValueEqual eq;
    for (size_t i = 0; i < a.values_.size(); ++i) {
      if (!eq(a[i], b[i])) {
        return false;
      }
    }
    return true;
  }
};

ValueType TypeTagToValueType(TypeTag tag) {
  switch (tag) {
    case TypeTag::kInteger:
    case TypeTag::kBigInt:
      return ValueType::kInt64;
    case TypeTag::kDouble:
      return ValueType::kDouble;
    case TypeTag::kVarChar:
      return ValueType::kVarChar;
    case TypeTag::kDate:
      return ValueType::kDate;
    case TypeTag::kArray:
      return ValueType::kArray;
    default:
      return ValueType::kVarChar;
  }
}

Schema MakePartialSchema(const std::vector<NamedExpression>& group_by_keys,
                         const std::vector<NamedExpression>& aggregates,
                         const Schema& input_schema) {
  std::vector<Column> cols;
  for (size_t i = 0; i < group_by_keys.size(); ++i) {
    std::string name = group_by_keys[i].name.empty()
                           ? ("group_key_" + std::to_string(i))
                           : group_by_keys[i].name;
    ValueType vt = ValueType::kVarChar;
    try {
      vt = TypeTagToValueType(
          group_by_keys[i].expression->ResultType(input_schema).GetType());
    } catch (...) {
      vt = ValueType::kVarChar;
    }
    cols.emplace_back(name, vt);
  }
  for (const NamedExpression& named : aggregates) {
    const auto& agg = named.expression->AsAggregateExpression();
    if (agg.GetType() == AggregationType::kAvg) {
      cols.emplace_back(named.name + "_sum", ValueType::kDouble);
      cols.emplace_back(named.name + "_count", ValueType::kInt64);
    } else if (agg.GetType() == AggregationType::kCount) {
      cols.emplace_back(named.name, ValueType::kInt64);
    } else if (agg.GetType() == AggregationType::kSum) {
      cols.emplace_back(named.name, ValueType::kDouble);
    } else {
      cols.emplace_back(named.name, ValueType::kVarChar);
    }
  }
  return Schema("partial_agg", std::move(cols));
}

Schema MakeFinalizeSchema(const std::vector<NamedExpression>& group_by_keys,
                          const std::vector<NamedExpression>& aggregates,
                          const Schema& input_schema) {
  std::vector<Column> cols;
  for (size_t i = 0; i < group_by_keys.size(); ++i) {
    std::string name = group_by_keys[i].name.empty()
                           ? ("group_key_" + std::to_string(i))
                           : group_by_keys[i].name;
    ValueType vt = ValueType::kVarChar;
    try {
      vt = TypeTagToValueType(
          group_by_keys[i].expression->ResultType(input_schema).GetType());
    } catch (...) {
      vt = ValueType::kVarChar;
    }
    cols.emplace_back(name, vt);
  }
  for (const NamedExpression& named : aggregates) {
    const auto& agg = named.expression->AsAggregateExpression();
    if (agg.GetType() == AggregationType::kCount) {
      cols.emplace_back(named.name, ValueType::kInt64);
    } else if (agg.GetType() == AggregationType::kAvg ||
               agg.GetType() == AggregationType::kSum) {
      cols.emplace_back(named.name, ValueType::kDouble);
    } else {
      cols.emplace_back(named.name, ValueType::kVarChar);
    }
  }
  return Schema("finalize_agg", std::move(cols));
}

}  // namespace

// ===== PartialAggregate =====

PartialAggregate::PartialAggregate(Executor child, Schema input_schema,
                                   std::vector<NamedExpression> aggregates)
    : PartialAggregate(std::move(child), std::move(input_schema),
                       std::vector<NamedExpression>{}, std::move(aggregates)) {}

PartialAggregate::PartialAggregate(Executor child, Schema input_schema,
                                   std::vector<NamedExpression> group_by_keys,
                                   std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      group_by_keys_(std::move(group_by_keys)),
      aggregates_(std::move(aggregates)),
      output_schema_(
          MakePartialSchema(group_by_keys_, aggregates_, input_schema_)) {}

PartialAggregate::PartialAggregate(Executor child, Schema input_schema,
                                   std::vector<Expression> group_by_keys,
                                   std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)) {
  for (size_t i = 0; i < group_by_keys.size(); ++i) {
    group_by_keys_.emplace_back("group_key_" + std::to_string(i),
                                std::move(group_by_keys[i]));
  }
  output_schema_ =
      MakePartialSchema(group_by_keys_, aggregates_, input_schema_);
}

void PartialAggregate::Materialize() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  output_rows_.clear();
  cursor_ = 0;

  struct GroupPartialState {
    std::vector<int64_t> counts;
    std::vector<Value> sums;
    std::vector<Value> mins;
    std::vector<Value> maxs;
    std::vector<Value> logical_ands;
    std::vector<Value> logical_ors;
  };

  auto init_state = [&](GroupPartialState& s) {
    s.counts.resize(aggregates_.size(), 0);
    s.sums.resize(aggregates_.size(), Value());
    s.mins.resize(aggregates_.size(), Value());
    s.maxs.resize(aggregates_.size(), Value());
    s.logical_ands.resize(aggregates_.size(), Value());
    s.logical_ors.resize(aggregates_.size(), Value());
  };

  std::vector<Row> group_order;
  std::unordered_map<Row, GroupPartialState, RowKeyHash, RowKeyEqual> groups;
  GroupPartialState scalar_state;
  init_state(scalar_state);

  Row in_row;
  RowPosition in_pos;
  while (child_->Next(&in_row, &in_pos)) {
    GroupPartialState* curr_state = nullptr;
    if (group_by_keys_.empty()) {
      curr_state = &scalar_state;
    } else {
      std::vector<Value> key_vals;
      key_vals.reserve(group_by_keys_.size());
      for (const auto& g_named : group_by_keys_) {
        key_vals.push_back(relational_detail::CanonicalDistinctValue(
            g_named.expression->Evaluate(in_row, input_schema_)));
      }
      Row g_key(std::move(key_vals));
      auto [it, inserted] = groups.try_emplace(g_key, GroupPartialState{});
      if (inserted) {
        init_state(it->second);
        group_order.push_back(g_key);
      }
      curr_state = &it->second;
    }

    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      if (agg.WhereFilter()) {
        Value fval = agg.WhereFilter()->Evaluate(in_row, input_schema_);
        if (fval.IsNull() || !fval.Truthy()) {
          continue;
        }
      }

      Value val;
      if (!IsCountStar(agg) && agg.Child()) {
        val = agg.Child()->Evaluate(in_row, input_schema_);
      }

      switch (agg.GetType()) {
        case AggregationType::kCount:
          if (IsCountStar(agg) || !val.IsNull()) {
            curr_state->counts[i]++;
          }
          break;
        case AggregationType::kSum:
          if (!val.IsNull()) {
            curr_state->sums[i] = curr_state->sums[i].IsNull()
                                      ? val
                                      : (curr_state->sums[i] + val);
          }
          break;
        case AggregationType::kAvg:
          if (!val.IsNull()) {
            curr_state->sums[i] = curr_state->sums[i].IsNull()
                                      ? val
                                      : (curr_state->sums[i] + val);
            curr_state->counts[i]++;
          }
          break;
        case AggregationType::kMin:
          if (!val.IsNull() &&
              (curr_state->mins[i].IsNull() || val < curr_state->mins[i])) {
            curr_state->mins[i] = val;
          }
          break;
        case AggregationType::kMax:
          if (!val.IsNull() &&
              (curr_state->maxs[i].IsNull() || curr_state->maxs[i] < val)) {
            curr_state->maxs[i] = val;
          }
          break;
        case AggregationType::kLogicalAnd:
          if (!val.IsNull()) {
            curr_state->logical_ands[i] =
                curr_state->logical_ands[i].IsNull()
                    ? Value(val.Truthy() ? int64_t{1} : int64_t{0})
                    : Value(
                          (curr_state->logical_ands[i].Truthy() && val.Truthy())
                              ? int64_t{1}
                              : int64_t{0});
          }
          break;
        case AggregationType::kLogicalOr:
          if (!val.IsNull()) {
            curr_state->logical_ors[i] =
                curr_state->logical_ors[i].IsNull()
                    ? Value(val.Truthy() ? int64_t{1} : int64_t{0})
                    : Value(
                          (curr_state->logical_ors[i].Truthy() || val.Truthy())
                              ? int64_t{1}
                              : int64_t{0});
          }
          break;
        default:
          if (!val.IsNull()) {
            curr_state->sums[i] = curr_state->sums[i].IsNull()
                                      ? val
                                      : (curr_state->sums[i] + val);
          }
          break;
      }
    }
  }

  auto emit_group = [&](const Row* key, const GroupPartialState& s) {
    std::vector<Value> row_vals;
    if (key != nullptr) {
      for (const Value& kv : key->values_) {
        row_vals.push_back(kv);
      }
    }
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      if (agg.GetType() == AggregationType::kAvg) {
        row_vals.push_back(s.sums[i]);
        row_vals.emplace_back(s.counts[i]);
      } else if (agg.GetType() == AggregationType::kCount) {
        row_vals.emplace_back(s.counts[i]);
      } else if (agg.GetType() == AggregationType::kMin) {
        row_vals.push_back(s.mins[i]);
      } else if (agg.GetType() == AggregationType::kMax) {
        row_vals.push_back(s.maxs[i]);
      } else if (agg.GetType() == AggregationType::kLogicalAnd) {
        row_vals.push_back(s.logical_ands[i]);
      } else if (agg.GetType() == AggregationType::kLogicalOr) {
        row_vals.push_back(s.logical_ors[i]);
      } else {
        row_vals.push_back(s.sums[i]);
      }
    }
    output_rows_.emplace_back(std::move(row_vals));
  };

  if (group_by_keys_.empty()) {
    emit_group(nullptr, scalar_state);
  } else {
    for (const Row& k : group_order) {
      emit_group(&k, groups[k]);
    }
  }
}

bool PartialAggregate::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    Materialize();
  }
  if (cursor_ >= output_rows_.size()) {
    return false;
  }
  *dst = output_rows_[cursor_++];
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  return true;
}

size_t PartialAggregate::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(output_schema_, max_rows);
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (count < max_rows && Next(&row, &pos)) {
    destination->Append(row, pos);
    ++count;
  }
  return count;
}

void PartialAggregate::Dump(std::ostream& o, int indent) const {
  o << "PartialAggregate: \n" << Indent(indent + 2);
  child_->Dump(o, indent + 2);
}

// ===== FinalizeAggregate =====

FinalizeAggregate::FinalizeAggregate(Executor child, Schema input_schema,
                                     std::vector<NamedExpression> aggregates)
    : FinalizeAggregate(std::move(child), std::move(input_schema),
                        std::vector<NamedExpression>{}, std::move(aggregates)) {
}

FinalizeAggregate::FinalizeAggregate(Executor child, Schema input_schema,
                                     std::vector<NamedExpression> group_by_keys,
                                     std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      group_by_keys_(std::move(group_by_keys)),
      aggregates_(std::move(aggregates)),
      output_schema_(
          MakeFinalizeSchema(group_by_keys_, aggregates_, input_schema_)) {}

FinalizeAggregate::FinalizeAggregate(Executor child, Schema input_schema,
                                     std::vector<Expression> group_by_keys,
                                     std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)) {
  for (size_t i = 0; i < group_by_keys.size(); ++i) {
    group_by_keys_.emplace_back("group_key_" + std::to_string(i),
                                std::move(group_by_keys[i]));
  }
  output_schema_ =
      MakeFinalizeSchema(group_by_keys_, aggregates_, input_schema_);
}

void FinalizeAggregate::Materialize() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  output_rows_.clear();
  cursor_ = 0;

  struct FinalizeState {
    std::vector<int64_t> counts;
    std::vector<Value> sums;
    std::vector<Value> mins;
    std::vector<Value> maxs;
    std::vector<Value> logical_ands;
    std::vector<Value> logical_ors;
  };

  auto init_state = [&](FinalizeState& s) {
    s.counts.resize(aggregates_.size(), 0);
    s.sums.resize(aggregates_.size(), Value());
    s.mins.resize(aggregates_.size(), Value());
    s.maxs.resize(aggregates_.size(), Value());
    s.logical_ands.resize(aggregates_.size(), Value());
    s.logical_ors.resize(aggregates_.size(), Value());
  };

  std::vector<Row> group_order;
  std::unordered_map<Row, FinalizeState, RowKeyHash, RowKeyEqual> groups;
  FinalizeState scalar_state;
  init_state(scalar_state);

  const size_t g_count = group_by_keys_.size();

  Row partial_row;
  RowPosition pos;
  while (child_->Next(&partial_row, &pos)) {
    FinalizeState* curr_state = nullptr;
    if (g_count == 0) {
      curr_state = &scalar_state;
    } else {
      std::vector<Value> key_vals;
      key_vals.reserve(g_count);
      for (size_t k = 0; k < g_count; ++k) {
        key_vals.push_back(
            relational_detail::CanonicalDistinctValue(partial_row[k]));
      }
      Row g_key(std::move(key_vals));
      auto [it, inserted] = groups.try_emplace(g_key, FinalizeState{});
      if (inserted) {
        init_state(it->second);
        group_order.push_back(g_key);
      }
      curr_state = &it->second;
    }

    size_t col_idx = g_count;
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      if (agg.GetType() == AggregationType::kAvg) {
        const Value& p_sum = partial_row[col_idx++];
        const Value& p_count = partial_row[col_idx++];
        if (!p_sum.IsNull() && p_count.value.int_value > 0) {
          curr_state->sums[i] = curr_state->sums[i].IsNull()
                                    ? p_sum
                                    : (curr_state->sums[i] + p_sum);
        }
        curr_state->counts[i] += p_count.value.int_value;
      } else if (agg.GetType() == AggregationType::kCount) {
        const Value& p_count = partial_row[col_idx++];
        curr_state->counts[i] += p_count.value.int_value;
      } else if (agg.GetType() == AggregationType::kSum) {
        const Value& p_sum = partial_row[col_idx++];
        if (!p_sum.IsNull()) {
          curr_state->sums[i] = curr_state->sums[i].IsNull()
                                    ? p_sum
                                    : (curr_state->sums[i] + p_sum);
        }
      } else if (agg.GetType() == AggregationType::kMin) {
        const Value& p_min = partial_row[col_idx++];
        if (!p_min.IsNull() &&
            (curr_state->mins[i].IsNull() || p_min < curr_state->mins[i])) {
          curr_state->mins[i] = p_min;
        }
      } else if (agg.GetType() == AggregationType::kMax) {
        const Value& p_max = partial_row[col_idx++];
        if (!p_max.IsNull() &&
            (curr_state->maxs[i].IsNull() || curr_state->maxs[i] < p_max)) {
          curr_state->maxs[i] = p_max;
        }
      } else if (agg.GetType() == AggregationType::kLogicalAnd) {
        const Value& p_and = partial_row[col_idx++];
        if (!p_and.IsNull()) {
          curr_state->logical_ands[i] =
              curr_state->logical_ands[i].IsNull()
                  ? p_and
                  : Value(
                        (curr_state->logical_ands[i].Truthy() && p_and.Truthy())
                            ? int64_t{1}
                            : int64_t{0});
        }
      } else if (agg.GetType() == AggregationType::kLogicalOr) {
        const Value& p_or = partial_row[col_idx++];
        if (!p_or.IsNull()) {
          curr_state->logical_ors[i] =
              curr_state->logical_ors[i].IsNull()
                  ? p_or
                  : Value((curr_state->logical_ors[i].Truthy() || p_or.Truthy())
                              ? int64_t{1}
                              : int64_t{0});
        }
      } else {
        const Value& p_val = partial_row[col_idx++];
        if (!p_val.IsNull()) {
          curr_state->sums[i] = curr_state->sums[i].IsNull()
                                    ? p_val
                                    : (curr_state->sums[i] + p_val);
        }
      }
    }
  }

  auto emit_group = [&](const Row* key, const FinalizeState& s) {
    std::vector<Value> row_vals;
    if (key != nullptr) {
      for (const Value& kv : key->values_) {
        row_vals.push_back(kv);
      }
    }
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      if (agg.GetType() == AggregationType::kAvg) {
        if (s.counts[i] == 0) {
          row_vals.emplace_back();  // NULL
        } else {
          double total = 0.0;
          if (s.sums[i].type == ValueType::kInt64) {
            total = static_cast<double>(s.sums[i].value.int_value);
          } else if (s.sums[i].type == ValueType::kDouble) {
            total = s.sums[i].value.double_value;
          }
          row_vals.emplace_back(total / static_cast<double>(s.counts[i]));
        }
      } else if (agg.GetType() == AggregationType::kCount) {
        row_vals.emplace_back(s.counts[i]);
      } else if (agg.GetType() == AggregationType::kMin) {
        row_vals.push_back(s.mins[i]);
      } else if (agg.GetType() == AggregationType::kMax) {
        row_vals.push_back(s.maxs[i]);
      } else if (agg.GetType() == AggregationType::kLogicalAnd) {
        row_vals.push_back(s.logical_ands[i]);
      } else if (agg.GetType() == AggregationType::kLogicalOr) {
        row_vals.push_back(s.logical_ors[i]);
      } else {
        row_vals.push_back(s.sums[i]);
      }
    }
    output_rows_.emplace_back(std::move(row_vals));
  };

  if (g_count == 0) {
    emit_group(nullptr, scalar_state);
  } else {
    for (const Row& k : group_order) {
      emit_group(&k, groups[k]);
    }
  }
}

bool FinalizeAggregate::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    Materialize();
  }
  if (cursor_ >= output_rows_.size()) {
    return false;
  }
  *dst = output_rows_[cursor_++];
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  return true;
}

size_t FinalizeAggregate::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(output_schema_, max_rows);
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (count < max_rows && Next(&row, &pos)) {
    destination->Append(row, pos);
    ++count;
  }
  return count;
}

void FinalizeAggregate::Dump(std::ostream& o, int indent) const {
  o << "FinalizeAggregate: \n" << Indent(indent + 2);
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
