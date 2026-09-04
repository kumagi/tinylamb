/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/grouping_sets.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/aggregate_expression.hpp"
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

Schema MakeGroupingSetsSchema(
    const std::vector<NamedExpression>& all_group_keys,
    const std::vector<NamedExpression>& aggregates,
    const Schema& input_schema) {
  std::vector<Column> cols;
  for (size_t i = 0; i < all_group_keys.size(); ++i) {
    std::string name = all_group_keys[i].name.empty()
                           ? ("group_key_" + std::to_string(i))
                           : all_group_keys[i].name;
    ValueType vt = ValueType::kVarChar;
    try {
      vt = TypeTagToValueType(
          all_group_keys[i].expression->ResultType(input_schema).GetType());
    } catch (...) {
      vt = ValueType::kVarChar;
    }
    cols.emplace_back(name, vt);
  }
  for (const NamedExpression& named : aggregates) {
    // Delegate to the aggregate's own static result type (COUNT -> INT64,
    // AVG/statistics/sketches -> DOUBLE/VARCHAR, everything else such as
    // SUM/MIN/MAX/BIT_* -> the child type). The previous hand listing
    // declared SUM(int) as Double and BIT_*/MIN(int) as VarChar while the
    // executor emits the child-typed values.
    ValueType vt = ValueType::kVarChar;
    try {
      vt = TypeTagToValueType(
          named.expression->ResultType(input_schema).GetType());
    } catch (...) {
      vt = ValueType::kVarChar;
    }
    cols.emplace_back(named.name, vt);
  }
  return Schema("grouping_sets_agg", std::move(cols));
}

}  // namespace

GroupingSetsExecutor::GroupingSetsExecutor(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> all_group_keys,
    std::vector<std::vector<size_t>> grouping_sets,
    std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      all_group_keys_(std::move(all_group_keys)),
      grouping_sets_(std::move(grouping_sets)),
      aggregates_(std::move(aggregates)),
      output_schema_(MakeGroupingSetsSchema(all_group_keys_, aggregates_,
                                            input_schema_)) {}

GroupingSetsExecutor GroupingSetsExecutor::Rollup(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> all_group_keys,
    std::vector<NamedExpression> aggregates) {
  const size_t n = all_group_keys.size();
  std::vector<std::vector<size_t>> sets;
  sets.reserve(n + 1);
  for (size_t len = n; len > 0; --len) {
    std::vector<size_t> s(len);
    for (size_t i = 0; i < len; ++i) {
      s[i] = i;
    }
    sets.push_back(std::move(s));
  }
  sets.push_back({});  // grand total
  return GroupingSetsExecutor(std::move(child), std::move(input_schema),
                              std::move(all_group_keys), std::move(sets),
                              std::move(aggregates));
}

GroupingSetsExecutor GroupingSetsExecutor::Cube(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> all_group_keys,
    std::vector<NamedExpression> aggregates) {
  const size_t n = all_group_keys.size();
  std::vector<std::vector<size_t>> sets;
  const size_t total_sets = 1ULL << n;

  std::vector<std::pair<size_t, size_t>> mask_with_popcount;
  mask_with_popcount.reserve(total_sets);
  for (size_t mask = 0; mask < total_sets; ++mask) {
    mask_with_popcount.emplace_back(
        mask, static_cast<size_t>(__builtin_popcountll(mask)));
  }

  std::stable_sort(mask_with_popcount.begin(), mask_with_popcount.end(),
                   [](const auto& a, const auto& b) {
                     if (a.second != b.second) {
                       return a.second > b.second;  // Largest sets first
                     }
                     return a.first > b.first;
                   });

  for (const auto& [mask, _] : mask_with_popcount) {
    std::vector<size_t> s;
    for (size_t i = 0; i < n; ++i) {
      if ((mask & (1ULL << i)) != 0) {
        s.push_back(i);
      }
    }
    sets.push_back(std::move(s));
  }

  return GroupingSetsExecutor(std::move(child), std::move(input_schema),
                              std::move(all_group_keys), std::move(sets),
                              std::move(aggregates));
}

void GroupingSetsExecutor::Materialize() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  output_rows_.clear();
  cursor_ = 0;

  // Ingest all child rows
  std::vector<Row> child_rows;
  Row in_row;
  RowPosition in_pos;
  while (child_->Next(&in_row, &in_pos)) {
    child_rows.push_back(in_row);
  }

  struct GroupAggState {
    std::vector<int64_t> counts;
    std::vector<Value> sums;
    std::vector<Value> mins;
    std::vector<Value> maxs;
    std::vector<Value> any_values;
    std::vector<bool> any_value_seen;
    std::vector<relational_detail::DistinctValueSet> distinct_sets;
  };

  auto init_group_state = [&](GroupAggState& s) {
    s.counts.resize(aggregates_.size(), 0);
    s.sums.resize(aggregates_.size(), Value());
    s.mins.resize(aggregates_.size(), Value());
    s.maxs.resize(aggregates_.size(), Value());
    s.any_values.resize(aggregates_.size(), Value());
    s.any_value_seen.resize(aggregates_.size(), false);
    s.distinct_sets.resize(aggregates_.size());
  };

  const size_t key_count = all_group_keys_.size();

  for (const std::vector<size_t>& gset : grouping_sets_) {
    std::unordered_set<size_t> active(gset.begin(), gset.end());

    if (child_rows.empty()) {
      if (gset.empty()) {
        std::vector<Value> row_vals(key_count, Value());
        for (const NamedExpression& named : aggregates_) {
          const auto& agg = named.expression->AsAggregateExpression();
          if (agg.GetType() == AggregationType::kCount) {
            row_vals.emplace_back(int64_t{0});
          } else {
            row_vals.emplace_back();  // NULL
          }
        }
        output_rows_.emplace_back(std::move(row_vals));
      }
      continue;
    }

    std::vector<Row> group_order;
    std::unordered_map<Row, GroupAggState, RowKeyHash, RowKeyEqual> groups;

    for (const Row& row : child_rows) {
      std::vector<Value> key_vals;
      key_vals.reserve(key_count);
      for (size_t k = 0; k < key_count; ++k) {
        if (active.contains(k)) {
          key_vals.push_back(relational_detail::CanonicalDistinctValue(
              all_group_keys_[k].expression->Evaluate(row, input_schema_)));
        } else {
          key_vals.emplace_back();  // NULL
        }
      }
      Row g_key(std::move(key_vals));
      auto [it, inserted] = groups.try_emplace(g_key, GroupAggState{});
      if (inserted) {
        init_group_state(it->second);
        group_order.push_back(g_key);
      }
      GroupAggState& s = it->second;

      for (size_t i = 0; i < aggregates_.size(); ++i) {
        const auto& agg = aggregates_[i].expression->AsAggregateExpression();
        if (agg.WhereFilter()) {
          Value fval = agg.WhereFilter()->Evaluate(row, input_schema_);
          if (fval.IsNull() || !fval.Truthy()) {
            continue;
          }
        }

        Value val;
        if (!IsCountStar(agg) && agg.Child()) {
          val = agg.Child()->Evaluate(row, input_schema_);
        }

        if (agg.Distinct()) {
          if (!val.IsNull()) {
            s.distinct_sets[i].insert(
                relational_detail::CanonicalDistinctValue(val));
          }
          continue;
        }

        switch (agg.GetType()) {
          case AggregationType::kCount:
            if (IsCountStar(agg) || !val.IsNull()) {
              s.counts[i]++;
            }
            break;
          case AggregationType::kSum:
            if (!val.IsNull()) {
              s.sums[i] = s.sums[i].IsNull() ? val : (s.sums[i] + val);
            }
            break;
          case AggregationType::kAvg:
            if (!val.IsNull()) {
              s.sums[i] = s.sums[i].IsNull() ? val : (s.sums[i] + val);
              s.counts[i]++;
            }
            break;
          case AggregationType::kMin:
            if (!val.IsNull() && (s.mins[i].IsNull() || val < s.mins[i])) {
              s.mins[i] = val;
            }
            break;
          case AggregationType::kMax:
            if (!val.IsNull() && (s.maxs[i].IsNull() || s.maxs[i] < val)) {
              s.maxs[i] = val;
            }
            break;
          case AggregationType::kAnyValue:
            if (!s.any_value_seen[i] && !val.IsNull()) {
              s.any_values[i] = val;
              s.any_value_seen[i] = true;
            }
            break;
          default:
            if (!val.IsNull()) {
              s.sums[i] = s.sums[i].IsNull() ? val : (s.sums[i] + val);
            }
            break;
        }
      }
    }

    for (const Row& g_key : group_order) {
      const GroupAggState& s = groups[g_key];
      std::vector<Value> row_vals = g_key.values_;

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
                row_vals.emplace_back();
              } else {
                Value total;
                for (const Value& v : dset) {
                  total = total.IsNull() ? v : (total + v);
                }
                row_vals.push_back(total);
              }
              break;
            }
            case AggregationType::kAvg: {
              if (dset.empty()) {
                row_vals.emplace_back();
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
                row_vals.emplace_back();
              } else {
                Value min_v;
                for (const Value& v : dset) {
                  if (min_v.IsNull() || v < min_v) {
                    min_v = v;
                  }
                }
                row_vals.push_back(min_v);
              }
              break;
            }
            case AggregationType::kMax: {
              if (dset.empty()) {
                row_vals.emplace_back();
              } else {
                Value max_v;
                for (const Value& v : dset) {
                  if (max_v.IsNull() || max_v < v) {
                    max_v = v;
                  }
                }
                row_vals.push_back(max_v);
              }
              break;
            }
            case AggregationType::kAnyValue: {
              if (dset.empty()) {
                row_vals.emplace_back();
              } else {
                row_vals.push_back(*dset.begin());
              }
              break;
            }
            default:
              row_vals.emplace_back(static_cast<int64_t>(dset.size()));
              break;
          }
        } else {
          switch (agg.GetType()) {
            case AggregationType::kCount:
              row_vals.emplace_back(s.counts[i]);
              break;
            case AggregationType::kSum:
              row_vals.push_back(s.sums[i]);
              break;
            case AggregationType::kAvg:
              if (s.counts[i] == 0) {
                row_vals.emplace_back();
              } else {
                double total = 0.0;
                if (s.sums[i].type == ValueType::kInt64) {
                  total = static_cast<double>(s.sums[i].value.int_value);
                } else if (s.sums[i].type == ValueType::kDouble) {
                  total = s.sums[i].value.double_value;
                }
                row_vals.emplace_back(total / static_cast<double>(s.counts[i]));
              }
              break;
            case AggregationType::kMin:
              row_vals.push_back(s.mins[i]);
              break;
            case AggregationType::kMax:
              row_vals.push_back(s.maxs[i]);
              break;
            case AggregationType::kAnyValue:
              row_vals.push_back(s.any_values[i]);
              break;
            default:
              row_vals.push_back(s.sums[i]);
              break;
          }
        }
      }
      output_rows_.emplace_back(std::move(row_vals));
    }
  }
}

bool GroupingSetsExecutor::Next(Row* dst, RowPosition* rp) {
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

size_t GroupingSetsExecutor::NextBatch(DataChunk* destination,
                                       size_t max_rows) {
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

void GroupingSetsExecutor::Dump(std::ostream& o, int indent) const {
  o << "GroupingSetsExecutor: \n" << Indent(indent + 2);
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
