/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/two_phase_distinct_agg.hpp"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/aggregation.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
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

Schema MakeTwoPhaseDistinctSchema(
    const std::vector<NamedExpression>& group_by_keys,
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
  return {"two_phase_distinct_agg", std::move(cols)};
}

}  // namespace

TwoPhaseDistinctAgg::TwoPhaseDistinctAgg(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> aggregates)
    : TwoPhaseDistinctAgg(std::move(child), std::move(input_schema),
                          std::vector<NamedExpression>{},
                          std::move(aggregates)) {}

TwoPhaseDistinctAgg::TwoPhaseDistinctAgg(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> group_by_keys,
    std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      group_by_keys_(std::move(group_by_keys)),
      aggregates_(std::move(aggregates)),
      output_schema_(MakeTwoPhaseDistinctSchema(group_by_keys_, aggregates_,
                                                input_schema_)) {}

TwoPhaseDistinctAgg::TwoPhaseDistinctAgg(
    Executor child, Schema input_schema, std::vector<Expression> group_by_keys,
    std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)) {
  for (size_t i = 0; i < group_by_keys.size(); ++i) {
    group_by_keys_.emplace_back("group_key_" + std::to_string(i),
                                std::move(group_by_keys[i]));
  }
  output_schema_ =
      MakeTwoPhaseDistinctSchema(group_by_keys_, aggregates_, input_schema_);
}

Status TwoPhaseDistinctAgg::Materialize() {
  if (materialized_) {
    return Status::kSuccess;
  }
  materialized_ = true;
  output_rows_.clear();
  cursor_ = 0;

  struct GroupState {
    // For distinct aggregates: set of unique values seen
    std::vector<relational_detail::DistinctValueSet> distinct_sets;
    // For non-distinct aggregates: running partial states
    std::vector<int64_t> non_distinct_counts;
    std::vector<Value> non_distinct_sums;
    // AVG accumulates in double (ground truth does): an int64 Value sum
    // would raise integer overflow where AggregationExecutor succeeds.
    std::vector<double> non_distinct_dsums;
    std::vector<Value> non_distinct_mins;
    std::vector<Value> non_distinct_maxs;
  };

  auto init_group_state = [&](GroupState& s) {
    s.distinct_sets.resize(aggregates_.size());
    s.non_distinct_counts.resize(aggregates_.size(), 0);
    s.non_distinct_sums.resize(aggregates_.size(), Value());
    s.non_distinct_dsums.resize(aggregates_.size(), 0.0);
    s.non_distinct_mins.resize(aggregates_.size(), Value());
    s.non_distinct_maxs.resize(aggregates_.size(), Value());
  };

  std::vector<Row> group_order;
  std::unordered_map<Row, GroupState, RowKeyHash, RowKeyEqual> groups;
  GroupState scalar_state;
  init_group_state(scalar_state);

  const size_t g_count = group_by_keys_.size();

  // Phase 1: Group by key + distinct column to eliminate duplicate (group_key,
  // distinct_val)
  Row in_row;
  RowPosition pos;
  while (child_->Next(&in_row, &pos)) {
    GroupState* curr_state = nullptr;
    if (g_count == 0) {
      curr_state = &scalar_state;
    } else {
      std::vector<Value> key_vals;
      key_vals.reserve(g_count);
      for (const auto& g_named : group_by_keys_) {
        StatusOr<Value> key =
            g_named.expression->TryEvaluate(in_row, input_schema_);
        if (!key.HasValue()) {
          FailWith(key.GetStatus());
          return key.GetStatus();
        }
        key_vals.push_back(
            relational_detail::CanonicalDistinctValue(key.MoveValue()));
      }
      Row g_key(std::move(key_vals));
      auto [it, inserted] = groups.try_emplace(g_key, GroupState{});
      if (inserted) {
        init_group_state(it->second);
        group_order.push_back(g_key);
      }
      curr_state = &it->second;
    }

    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      if (agg.WhereFilter()) {
        StatusOr<Value> fval =
            agg.WhereFilter()->TryEvaluate(in_row, input_schema_);
        if (!fval.HasValue()) {
          FailWith(fval.GetStatus());
          return fval.GetStatus();
        }
        if (fval.Value().IsNull() || !fval.Value().Truthy()) {
          continue;
        }
      }

      Value val;
      if (!IsCountStar(agg) && agg.Child()) {
        StatusOr<Value> input = agg.Child()->TryEvaluate(in_row, input_schema_);
        if (!input.HasValue()) {
          FailWith(input.GetStatus());
          return input.GetStatus();
        }
        val = input.MoveValue();
      }

      if (agg.Distinct()) {
        if (!val.IsNull()) {
          // Phase 1 partial distinct: deduplicate against group's distinct set
          curr_state->distinct_sets[i].insert(
              relational_detail::CanonicalDistinctValue(val));
        }
      } else {
        // Non-distinct aggregation path
        switch (agg.GetType()) {
          case AggregationType::kCount:
            if (IsCountStar(agg) || !val.IsNull()) {
              curr_state->non_distinct_counts[i]++;
            }
            break;
          case AggregationType::kSum:
            if (!val.IsNull()) {
              curr_state->non_distinct_sums[i] =
                  curr_state->non_distinct_sums[i].IsNull()
                      ? val
                      : (curr_state->non_distinct_sums[i] + val);
            }
            break;
          case AggregationType::kAvg:
            if (!val.IsNull()) {
              curr_state->non_distinct_dsums[i] +=
                  val.type == ValueType::kDouble
                      ? val.value.double_value
                      : static_cast<double>(val.value.int_value);
              curr_state->non_distinct_counts[i]++;
            }
            break;
          case AggregationType::kMin:
            if (!val.IsNull()) {
              if (val.type == ValueType::kDouble &&
                  std::isnan(val.value.double_value)) {
                // Any NaN makes the group MIN/MAX NaN (see ground truth).
                curr_state->non_distinct_mins[i] =
                    Value(std::numeric_limits<double>::quiet_NaN());
              } else if (curr_state->non_distinct_mins[i].IsNull() ||
                         val < curr_state->non_distinct_mins[i]) {
                curr_state->non_distinct_mins[i] = val;
              }
            }
            break;
          case AggregationType::kMax:
            if (!val.IsNull()) {
              if (val.type == ValueType::kDouble &&
                  std::isnan(val.value.double_value)) {
                curr_state->non_distinct_maxs[i] =
                    Value(std::numeric_limits<double>::quiet_NaN());
              } else if (curr_state->non_distinct_maxs[i].IsNull() ||
                         curr_state->non_distinct_maxs[i] < val) {
                curr_state->non_distinct_maxs[i] = val;
              }
            }
            break;
          default:
            if (!val.IsNull()) {
              curr_state->non_distinct_sums[i] =
                  curr_state->non_distinct_sums[i].IsNull()
                      ? val
                      : (curr_state->non_distinct_sums[i] + val);
            }
            break;
        }
      }
    }
  }

  // Phase 2: Finalize aggregation over deduplicated distinct sets and
  // non-distinct partials
  auto emit_group = [&](const Row* key, const GroupState& s) {
    std::vector<Value> row_vals;
    if (key != nullptr) {
      for (const Value& kv : key->values_) {
        row_vals.push_back(kv);
      }
    }
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      if (agg.Distinct()) {
        const auto& dset = s.distinct_sets[i];
        switch (agg.GetType()) {
          case AggregationType::kCount:
            row_vals.emplace_back(static_cast<int64_t>(dset.size()));
            break;
          case AggregationType::kSum: {
            if (dset.empty()) {
              row_vals.emplace_back();  // NULL
            } else {
              Value total;
              for (const Value& v : dset) {
                total = total.IsNull() ? v : (total + v);
              }
              // The declared schema carries SUM/AVG as kDouble; an int64
              // total would make ColumnVector::Append throw on NextBatch.
              if (total.type == ValueType::kInt64) {
                row_vals.emplace_back(
                    static_cast<double>(total.value.int_value));
              } else {
                row_vals.push_back(total);
              }
            }
            break;
          }
          case AggregationType::kAvg: {
            if (dset.empty()) {
              row_vals.emplace_back();  // NULL
            } else {
              double total = 0.0;
              for (const Value& v : dset) {
                if (v.type == ValueType::kInt64) {
                  total += static_cast<double>(v.value.int_value);
                } else if (v.type == ValueType::kDouble) {
                  total += v.value.double_value;
                }
              }
              row_vals.emplace_back(total / static_cast<double>(dset.size()));
            }
            break;
          }
          case AggregationType::kMin: {
            if (dset.empty()) {
              row_vals.emplace_back();  // NULL
            } else {
              Value min_val;
              for (const Value& v : dset) {
                if (min_val.IsNull() || v < min_val) {
                  min_val = v;
                }
              }
              row_vals.push_back(min_val);
            }
            break;
          }
          case AggregationType::kMax: {
            if (dset.empty()) {
              row_vals.emplace_back();  // NULL
            } else {
              Value max_val;
              for (const Value& v : dset) {
                if (max_val.IsNull() || max_val < v) {
                  max_val = v;
                }
              }
              row_vals.push_back(max_val);
            }
            break;
          }
          default:
            row_vals.emplace_back(static_cast<int64_t>(dset.size()));
            break;
        }
      } else {
        // Non-distinct finalize
        switch (agg.GetType()) {
          case AggregationType::kCount:
            row_vals.emplace_back(s.non_distinct_counts[i]);
            break;
          case AggregationType::kSum:
            // Match the declared kDouble schema (see the distinct path).
            if (s.non_distinct_sums[i].IsNull()) {
              row_vals.emplace_back();
            } else if (s.non_distinct_sums[i].type == ValueType::kInt64) {
              row_vals.emplace_back(
                  static_cast<double>(s.non_distinct_sums[i].value.int_value));
            } else {
              row_vals.push_back(s.non_distinct_sums[i]);
            }
            break;
          case AggregationType::kAvg:
            if (s.non_distinct_counts[i] == 0) {
              row_vals.emplace_back();  // NULL
            } else {
              row_vals.emplace_back(
                  s.non_distinct_dsums[i] /
                  static_cast<double>(s.non_distinct_counts[i]));
            }
            break;
          case AggregationType::kMin:
            row_vals.push_back(s.non_distinct_mins[i]);
            break;
          case AggregationType::kMax:
            row_vals.push_back(s.non_distinct_maxs[i]);
            break;
          default:
            row_vals.push_back(s.non_distinct_sums[i]);
            break;
        }
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
  RETURN_IF_FAIL(child_->GetStatus());
  return Status::kSuccess;
}

bool TwoPhaseDistinctAgg::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    if (Materialize() != Status::kSuccess) {
      return false;
    }
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

size_t TwoPhaseDistinctAgg::NextBatch(DataChunk* destination, size_t max_rows) {
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

void TwoPhaseDistinctAgg::Dump(std::ostream& o, int indent) const {
  o << "TwoPhaseDistinctAgg: \n" << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
