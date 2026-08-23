/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/planning_heuristics.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <vector>
#include <utility>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

std::vector<PredicateInfo> AnalyzePredicates(
    const Expression& where, const std::vector<Relation>& relations) {
  std::vector<PredicateInfo> result;
  for (const Expression& expression : SplitConjuncts(where)) {
    PredicateInfo predicate{.expression=expression, .sources={}, .resolved=true, .contains_query=false};
    predicate.contains_query = ContainsQuery(expression);
    for (const ColumnName& column : expression->TouchedColumns()) {
      std::optional<size_t> owner;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!LocalColumnOffset(relations[i].schema, column)) { continue;
}
        if (owner) {
          predicate.resolved = false;
          break;
        }
        owner = i;
      }
      if (!owner) { predicate.resolved = false;
}
      if (owner) { predicate.sources.insert(*owner);
}
    }
    result.push_back(std::move(predicate));
  }
  return result;
}
Expression NecessaryLocalDisjunction(const Expression& expression,
                                     size_t source,
                                     const std::vector<Relation>& relations) {
  const std::vector<Expression> branches = SplitDisjuncts(expression);
  if (branches.size() < 2) { return nullptr;
}
  std::vector<Expression> local_branches;
  local_branches.reserve(branches.size());
  for (const Expression& branch : branches) {
    std::vector<Expression> local;
    for (const Expression& conjunct : SplitConjuncts(branch)) {
      const std::vector<PredicateInfo> analyzed =
          AnalyzePredicates(conjunct, relations);
      if (analyzed.size() == 1 && analyzed[0].resolved &&
          !analyzed[0].contains_query && analyzed[0].sources.size() == 1 &&
          analyzed[0].sources.contains(source)) {
        local.push_back(conjunct);
      }
    }
    // A branch without a condition on this source can be true for every row;
    // in that case no source-local condition is implied by the disjunction.
    if (local.empty()) { return nullptr;
}
    local_branches.push_back(CombineConjuncts(local));
  }
  return CombineDisjuncts(local_branches);
}

bool IsColumnEqualityPredicate(const Expression& predicate) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) { return false;
}
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  return binary.Op() == BinaryOperation::kEquals &&
         binary.Left()->Type() == TypeTag::kColumnValue &&
         binary.Right()->Type() == TypeTag::kColumnValue;
}

bool MapsToEqualityKey(const Schema& left, const Schema& right,
                       const Expression& predicate) {
  if (!IsColumnEqualityPredicate(predicate)) { return false;
}
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
  const auto lhs_left = LocalColumnOffset(left, lhs);
  const auto lhs_right = LocalColumnOffset(right, lhs);
  const auto rhs_left = LocalColumnOffset(left, rhs);
  const auto rhs_right = LocalColumnOffset(right, rhs);
  return (lhs_left && rhs_right) || (rhs_left && lhs_right);
}

std::vector<Expression> ResidualJoinPredicates(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  std::vector<Expression> residual;
  residual.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    if (MapsToEqualityKey(left, right, predicate)) { continue;
}
    residual.push_back(predicate);
  }
  return residual;
}

std::vector<EqualityKey> EqualityKeys(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  std::vector<EqualityKey> keys;
  for (const Expression& predicate : predicates) {
    if (!IsColumnEqualityPredicate(predicate)) { continue;
}
    const BinaryExpression& binary = predicate->AsBinaryExpression();
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    const auto lhs_left = LocalColumnOffset(left, lhs);
    const auto lhs_right = LocalColumnOffset(right, lhs);
    const auto rhs_left = LocalColumnOffset(left, rhs);
    const auto rhs_right = LocalColumnOffset(right, rhs);
    if (lhs_left && rhs_right) {
      keys.push_back({*lhs_left, *rhs_right});
    } else if (rhs_left && lhs_right) {
      keys.push_back({*rhs_left, *lhs_right});
    }
  }
  return keys;
}

bool SingleIntegerJoinKey(const Schema& schema,
                          const std::vector<slot_t>& columns) {
  if (columns.size() != 1) { return false;
}
  const ValueType type = schema.GetColumn(columns[0]).Type();
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

bool HasNullKey(const Row& row, const std::vector<slot_t>& columns) {
  return std::ranges::any_of(columns,
                             [&](slot_t column) { return row[column].IsNull(); });
}

int64_t IntegerJoinKey(const Row& row, slot_t column) {
  return row[column].value.int_value;
}

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns) {
  return row.Extract(columns).EncodeMemcomparableFormat();
}

size_t EstimateJoinRows(const Relation& left, const Relation& right,
                        const std::vector<Expression>& predicates) {
  const std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, right.schema, predicates);
  if (keys.empty()) {
    if (left.TotalRows() == 0 || right.TotalRows() == 0) { return 0;
}
    if (left.TotalRows() >
        std::numeric_limits<size_t>::max() / right.TotalRows()) {
      return std::numeric_limits<size_t>::max();
    }
    return left.TotalRows() * right.TotalRows();
  }
  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  for (const EqualityKey& key : keys) {
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  std::unordered_map<std::string, size_t> frequencies;
  frequencies.reserve(right.rows.size());
  right.ForEachRow([&](const Row& row) {
    if (!HasNullKey(row, right_columns)) {
      ++frequencies[row.Extract(right_columns).EncodeMemcomparableFormat()];
    }
  });
  size_t estimate = 0;
  left.ForEachRow([&](const Row& row) {
    if (HasNullKey(row, left_columns)) { return;
}
    const auto found =
        frequencies.find(row.Extract(left_columns).EncodeMemcomparableFormat());
    if (found != frequencies.end()) { estimate += found->second;
}
  });
  return estimate;
}

size_t SpillPartitionOf(const std::string& key, size_t partitions) {
  return std::hash<std::string>{}(key) % partitions;
}

size_t SpillPartitionOf(int64_t key, size_t partitions) {
  return static_cast<size_t>(std::hash<int64_t>{}(key) % partitions);
}

// DeWitt-style Hybrid Hash Join: keep partition 0 resident; spill the rest.
Relation HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons) {
  size_t build_estimate = 0;
  if (right.HasSpill()) {
    build_estimate = right.TotalRows() * 128;
  } else {
    for (const Row& row : right.rows) {
      build_estimate += EstimateRowBytes(row) + 64;
    }
  }
  const size_t partitions = HybridPartitionCount(build_estimate);
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  const bool integer_key =
      SingleIntegerJoinKey(left.schema, left_columns) &&
      SingleIntegerJoinKey(right.schema, right_columns);
  const slot_t left_key_column =
      integer_key ? left_columns[0] : static_cast<slot_t>(0);
  const slot_t right_key_column =
      integer_key ? right_columns[0] : static_cast<slot_t>(0);

  std::vector<Row> resident_right;
  QueryMemoryCharge resident_charge;
  std::vector<SpillFile> left_parts(partitions);
  std::vector<SpillFile> right_parts(partitions);

  left.FinishSpill();
  right.FinishSpill();

  auto right_partition = [&](const Row& row) -> size_t {
    if (integer_key) {
      return SpillPartitionOf(IntegerJoinKey(row, right_key_column),
                              partitions);
    }
    return SpillPartitionOf(EncodeJoinKey(row, right_columns), partitions);
  };
  auto left_partition = [&](const Row& row) -> size_t {
    if (integer_key) {
      return SpillPartitionOf(IntegerJoinKey(row, left_key_column), partitions);
    }
    return SpillPartitionOf(EncodeJoinKey(row, left_columns), partitions);
  };

  right.ForEachRow([&](const Row& row) {
    if (HasNullKey(row, right_columns)) {
      return;
    }
    const size_t part = right_partition(row);
    const size_t bytes = EstimateRowBytes(row);
    if (part == 0 && budget.CanReserve(bytes)) {
      resident_charge.Add(bytes);
      resident_right.push_back(row);
    } else {
      right_parts[part].Append(row);
    }
  });
  right.rows.clear();
  right.rows.shrink_to_fit();
  right.ReleaseCharge();

  std::unordered_multimap<int64_t, const Row*> int_buckets;
  std::unordered_multimap<std::string, const Row*> str_buckets;
  if (integer_key) {
    int_buckets.reserve(resident_right.size());
    for (const Row& row : resident_right) {
      int_buckets.emplace(IntegerJoinKey(row, right_key_column), &row);
    }
  } else {
    str_buckets.reserve(resident_right.size());
    for (const Row& row : resident_right) {
      str_buckets.emplace(EncodeJoinKey(row, right_columns), &row);
    }
  }

  Relation result;
  result.schema = left.schema + right.schema;
  const size_t right_width = right.schema.ColumnCount();

  auto probe_resident = [&](const Row& left_row) {
    bool matched = false;
    if (integer_key) {
      const int64_t key = IntegerJoinKey(left_row, left_key_column);
      const auto [begin, end] = int_buckets.equal_range(key);
      for (auto iter = begin; iter != end; ++iter) {
        ++(*join_comparisons);
        Row combined = left_row + *iter->second;
        if (matches(combined)) {
          result.AddRow(std::move(combined));
          matched = true;
        }
      }
    } else {
      const std::string key = EncodeJoinKey(left_row, left_columns);
      const auto [begin, end] = str_buckets.equal_range(key);
      for (auto iter = begin; iter != end; ++iter) {
        ++(*join_comparisons);
        Row combined = left_row + *iter->second;
        if (matches(combined)) {
          result.AddRow(std::move(combined));
          matched = true;
        }
      }
    }
    if (!matched && left_join) {
      std::vector<Value> nulls(right_width);
      result.AddRow(left_row + Row(std::move(nulls)));
    }
  };

  left.ForEachRow([&](const Row& left_row) {
    if (HasNullKey(left_row, left_columns)) {
      if (left_join) {
        std::vector<Value> nulls(right_width);
        result.AddRow(left_row + Row(std::move(nulls)));
      }
      return;
    }
    const size_t part = left_partition(left_row);
    if (part == 0 && right_parts[0].Empty()) {
      probe_resident(left_row);
    } else {
      left_parts[part].Append(left_row);
    }
  });
  left.rows.clear();
  left.rows.shrink_to_fit();
  left.ReleaseCharge();

  int_buckets.clear();
  str_buckets.clear();
  if (!right_parts[0].Empty()) {
    for (const Row& row : resident_right) {
      right_parts[0].Append(row);
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t part = 0; part < partitions; ++part) {
    left_parts[part].FinishWriting();
    right_parts[part].FinishWriting();
  }

  for (size_t part = 0; part < partitions; ++part) {
    if (left_parts[part].Empty() && right_parts[part].Empty()) {
      continue;
    }
    std::vector<Row> right_rows = right_parts[part].ReadAllRows();
    QueryMemoryCharge part_charge;
    for (const Row& row : right_rows) {
      part_charge.Add(EstimateRowBytes(row));
    }
    std::unordered_multimap<int64_t, const Row*> part_int_buckets;
    std::unordered_multimap<std::string, const Row*> part_str_buckets;
    if (integer_key) {
      part_int_buckets.reserve(right_rows.size());
      for (const Row& row : right_rows) {
        part_int_buckets.emplace(IntegerJoinKey(row, right_key_column), &row);
      }
    } else {
      part_str_buckets.reserve(right_rows.size());
      for (const Row& row : right_rows) {
        part_str_buckets.emplace(EncodeJoinKey(row, right_columns), &row);
      }
    }
    left_parts[part].ForEachRow([&](const Row& left_row) {
      bool matched = false;
      if (integer_key) {
        const int64_t key = IntegerJoinKey(left_row, left_key_column);
        const auto [begin, end] = part_int_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = left_row + *iter->second;
          if (matches(combined)) {
            result.AddRow(std::move(combined));
            matched = true;
          }
        }
      } else {
        const std::string key = EncodeJoinKey(left_row, left_columns);
        const auto [begin, end] = part_str_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = left_row + *iter->second;
          if (matches(combined)) {
            result.AddRow(std::move(combined));
            matched = true;
          }
        }
      }
      if (!matched && left_join) {
        std::vector<Value> nulls(right_width);
        result.AddRow(left_row + Row(std::move(nulls)));
      }
    });
  }
  result.FinishSpill();
  return result;
}

bool ShouldHybridJoin(const Relation& left, const Relation& right) {
  if (left.HasSpill() || right.HasSpill()) {
    return true;
  }
  // Rough hash-table estimate: keys + pointers for the build (right) side.
  size_t estimate = 0;
  for (const Row& row : right.rows) {
    estimate += EstimateRowBytes(row) + 64;
  }
  return PreferHybridHashJoin(estimate);
}

Relation Join(TransactionContext& context, Relation left, Relation right,
              const SelectSource& source, const Scope* outer,
              const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result;
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  const std::vector<Expression> predicates =
      SplitConjuncts(source.join_condition);
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residual =
      ResidualJoinPredicates(left.schema, right.schema, predicates);

  auto matches = [&](const Row& combined) {
    if (residual.empty()) { return true;
}
    Scope scope{.row=&combined, .schema=&result.schema, .outer=outer};
    return std::ranges::all_of(
        residual, [&](const Expression& predicate) {
          return Truthy(Evaluate(predicate, scope, nullptr, context, ctes));
        });
  };
  auto emit_unmatched = [&](const Row& left_row) {
    if (source.join_type != JoinType::kLeft) { return;
}
    std::vector<Value> nulls(right.schema.ColumnCount());
    result.AddRow(left_row + Row(std::move(nulls)));
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    left.FinishSpill();
    right.FinishSpill();
    left.ForEachRow([&](const Row& left_row) {
      bool matched = false;
      right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = left_row + right_row;
        if (matches(combined)) {
          result.AddRow(std::move(combined));
          matched = true;
        }
      });
      if (!matched) { emit_unmatched(left_row);
}
    });
  } else {
    ++result.hash_joins;
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
    }
    if (ShouldHybridJoin(left, right)) {
      ++result.hybrid_hash_joins;
      Relation joined =
          HybridHashJoin(std::move(left), std::move(right), left_columns,
                         right_columns, matches,
                         source.join_type == JoinType::kLeft,
                         &result.join_comparisons);
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      if (active_runtime != nullptr) { active_runtime->join_ms += ElapsedMs(join_begin);
}
      return joined;
    }
    ++result.in_memory_hash_joins;
    const bool integer_key =
        SingleIntegerJoinKey(left.schema, left_columns) &&
        SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      std::unordered_multimap<int64_t, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (!HasNullKey(row, right_columns)) {
          buckets.emplace(IntegerJoinKey(row, right_columns[0]), &row);
        }
      }
      for (const Row& left_row : left.rows) {
        bool matched = false;
        if (!HasNullKey(left_row, left_columns)) {
          const int64_t key = IntegerJoinKey(left_row, left_columns[0]);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = left_row + *iter->second;
            if (matches(combined)) {
              result.AddRow(std::move(combined));
              matched = true;
            }
          }
        }
        if (!matched) { emit_unmatched(left_row);
}
      }
    } else {
      std::unordered_multimap<std::string, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (!HasNullKey(row, right_columns)) {
          buckets.emplace(EncodeJoinKey(row, right_columns), &row);
        }
      }
      for (const Row& left_row : left.rows) {
        bool matched = false;
        if (!HasNullKey(left_row, left_columns)) {
          const std::string key = EncodeJoinKey(left_row, left_columns);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = left_row + *iter->second;
            if (matches(combined)) {
              result.AddRow(std::move(combined));
              matched = true;
            }
          }
        }
        if (!matched) { emit_unmatched(left_row);
}
      }
    }
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (active_runtime != nullptr) { active_runtime->join_ms += ElapsedMs(join_begin);
}
  return result;
}

Relation InnerJoin(TransactionContext& context, Relation left, Relation right,
                   const std::vector<Expression>& predicates,
                   const Scope* outer, const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result;
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residual =
      ResidualJoinPredicates(left.schema, right.schema, predicates);

  auto matches = [&](const Row& combined) {
    if (residual.empty()) { return true;
}
    Scope scope{.row=&combined, .schema=&result.schema, .outer=outer};
    return std::ranges::all_of(
        residual, [&](const Expression& predicate) {
          return Truthy(Evaluate(predicate, scope, nullptr, context, ctes));
        });
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    left.FinishSpill();
    right.FinishSpill();
    left.ForEachRow([&](const Row& left_row) {
      right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = left_row + right_row;
        if (matches(combined)) { result.AddRow(std::move(combined));
}
      });
    });
  } else {
    ++result.hash_joins;
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    left_columns.reserve(equality_keys.size());
    right_columns.reserve(equality_keys.size());
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
    }
    if (ShouldHybridJoin(left, right)) {
      ++result.hybrid_hash_joins;
      Relation joined = HybridHashJoin(
          std::move(left), std::move(right), left_columns, right_columns,
          matches, false, &result.join_comparisons);
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      if (active_runtime != nullptr) { active_runtime->join_ms += ElapsedMs(join_begin);
}
      return joined;
    }
    ++result.in_memory_hash_joins;
    const bool integer_key =
        SingleIntegerJoinKey(left.schema, left_columns) &&
        SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      std::unordered_multimap<int64_t, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (HasNullKey(row, right_columns)) { continue;
}
        buckets.emplace(IntegerJoinKey(row, right_columns[0]), &row);
      }
      for (const Row& left_row : left.rows) {
        if (HasNullKey(left_row, left_columns)) { continue;
}
        const int64_t key = IntegerJoinKey(left_row, left_columns[0]);
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result.join_comparisons;
          Row combined = left_row + *iter->second;
          if (matches(combined)) { result.AddRow(std::move(combined));
}
        }
      }
    } else {
      std::unordered_multimap<std::string, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (HasNullKey(row, right_columns)) { continue;
}
        buckets.emplace(EncodeJoinKey(row, right_columns), &row);
      }
      for (const Row& left_row : left.rows) {
        if (HasNullKey(left_row, left_columns)) { continue;
}
        const std::string key = EncodeJoinKey(left_row, left_columns);
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result.join_comparisons;
          Row combined = left_row + *iter->second;
          if (matches(combined)) { result.AddRow(std::move(combined));
}
        }
      }
    }
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (active_runtime != nullptr) { active_runtime->join_ms += ElapsedMs(join_begin);
}
  return result;
}

bool IsSubset(const std::unordered_set<size_t>& values,
              const std::unordered_set<size_t>& superset) {
  return std::ranges::all_of(values,
                             [&](size_t value) { return superset.contains(value); });
}
Relation BuildInput(TransactionContext& context,
                    const SelectStatement& statement, const Scope* outer,
                    const CteMap& ctes, bool* where_fully_applied) {
  *where_fully_applied = false;
  if (statement.Sources().empty()) {
    Relation singleton;
    singleton.rows.emplace_back();
    singleton.peak_intermediate_rows = 1;
    return singleton;
  }

  std::vector<Relation> relations(statement.Sources().size());
  std::vector<bool> base_sources(statement.Sources().size(), false);
  std::vector<std::vector<slot_t>> projections(statement.Sources().size());
  for (size_t i = 0; i < statement.Sources().size(); ++i) {
    const SelectSource& source = statement.Sources()[i];
    base_sources[i] = !source.query && !ctes.contains(source.table);
    if (!base_sources[i]) {
      relations[i] = LoadSource(context, source, outer, ctes);
      continue;
    }
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      throw std::runtime_error("table " + source.table + " not found");
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    relations[i].schema = qualifier.empty()
                              ? table.Value()->GetSchema()
                              : QualifySchema(table.Value()->GetSchema(),
                                              qualifier);
    projections[i] = RequiredColumns(statement, relations[i].schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(source.table)) {
      projections[i] = *shared;
    }
  }

  // Multi-alias tables share one projected cache entry: take the union of
  // columns required by every reference so each alias can reuse the scan.
  {
    std::unordered_map<std::string, std::set<slot_t>> column_union;
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] || !ReusesBaseRelation(statement.Sources()[i])) {
        continue;
      }
      auto& columns = column_union[statement.Sources()[i].table];
      for (slot_t column : projections[i]) { columns.insert(column);
}
    }
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] || !ReusesBaseRelation(statement.Sources()[i])) {
        continue;
      }
      const auto& columns = column_union[statement.Sources()[i].table];
      projections[i].assign(columns.begin(), columns.end());
    }
  }

  // LEFT JOIN predicates have null-extension semantics, so retain their
  // syntactic order. The optimizer below is valid for inner/cross joins.
  const bool has_left_join =
      std::any_of(statement.Sources().begin() + 1, statement.Sources().end(),
                  [](const SelectSource& source) {
                    return source.join_type == JoinType::kLeft;
                  });
  if (has_left_join) {
    for (size_t i = 0; i < relations.size(); ++i) {
      if (base_sources[i]) {
        relations[i] = LoadSource(context, statement.Sources()[i], outer,
                                  ctes, &projections[i]);
      }
    }
    Relation result = std::move(relations.front());
    for (size_t i = 1; i < relations.size(); ++i) {
      result = Join(context, std::move(result), std::move(relations[i]),
                    statement.Sources()[i], outer, ctes);
    }
    return result;
  }

  std::vector<Expression> all_predicates =
      SplitConjuncts(statement.WhereClause());
  for (size_t i = 1; i < statement.Sources().size(); ++i) {
    const Expression& condition = statement.Sources()[i].join_condition;
    if (condition) { all_predicates.push_back(condition);
}
  }
  std::vector<PredicateInfo> predicates = AnalyzePredicates(
      all_predicates.empty() ? Expression() : CombineConjuncts(all_predicates),
      relations);
  const std::vector<PredicateInfo> where_predicates =
      AnalyzePredicates(statement.WhereClause(), relations);
  auto locally_evaluable = [&](const PredicateInfo& predicate) {
    if (!predicate.resolved || predicate.sources.size() != 1) { return false;
}
    if (!predicate.contains_query) { return true;
}
    return ContainsOnlyUncorrelatedQueries(context, predicate.expression, ctes);
  };
  *where_fully_applied =
      std::ranges::all_of(where_predicates,
                  [&](const PredicateInfo& predicate) {
                    return locally_evaluable(predicate) ||
                           (predicate.resolved && !predicate.contains_query &&
                            predicate.sources.size() > 1);
                  });
  std::vector<std::vector<Expression>> local_predicates(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    std::vector<Expression>& local = local_predicates[i];
    for (const PredicateInfo& predicate : predicates) {
      if (predicate.sources.contains(i) && locally_evaluable(predicate)) {
        local.push_back(predicate.expression);
      } else if (predicate.resolved && !predicate.contains_query &&
                 predicate.sources.size() > 1) {
        Expression implied =
            NecessaryLocalDisjunction(predicate.expression, i, relations);
        if (implied) { local.push_back(std::move(implied));
}
      }
    }
  }

  // Push uncorrelated `col IN (SELECT ...)` results as integer key filters
  // before scanning the owning base table (TPC-H Q18).
  if (active_runtime != nullptr) {
    for (const Expression& expression :
         SplitConjuncts(statement.WhereClause())) {
      if (!expression || expression->Type() != TypeTag::kQueryExp) { continue;
}
      const QueryExpression& query = expression->AsQueryExpression();
      if (query.Exists() || query.Negated() || !query.Test() ||
          query.Test()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const Relation* membership =
          ExecuteCachedUncorrelated(context, *query.Query(), ctes);
      if (membership == nullptr || membership->rows.empty()) {
        continue;
}
      std::unordered_set<int64_t> keys;
      keys.reserve(membership->rows.size());
      bool all_int = true;
      auto consume_row = [&](const Row& row) {
        if (row.values_.empty() || row[0].IsNull()) { return;
}
        if (row[0].type != ValueType::kInt64 &&
            row[0].type != ValueType::kDate) {
          all_int = false;
          return;
        }
        keys.insert(row[0].value.int_value);
      };
      if (membership->HasSpill()) {
        membership->ForEachRow(consume_row);
      } else {
        for (const Row& row : membership->rows) { consume_row(row);
}
      }
      if (!all_int || keys.empty() || keys.size() >= 2'000'000) { continue;
}
      const ColumnName& column =
          query.Test()->AsColumnValue().GetColumnName();
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!base_sources[i]) { continue;
}
        const auto offset = LocalColumnOffset(relations[i].schema, column);
        if (!offset) { continue;
}
        if (!SingleIntegerJoinKey(relations[i].schema,
                                  {static_cast<slot_t>(*offset)})) {
          continue;
        }
        const std::string& table = statement.Sources()[i].table;
        TableKeyFilter& stored = active_runtime->table_key_filters[table];
        if (stored.keys.empty() || keys.size() < stored.keys.size()) {
          stored.keys = keys;
          stored.column = static_cast<slot_t>(*offset);
          stored.owner = &statement;
        }
      }
    }
  }

  // Load selective relations first, then push their integer join keys into
  // later scans (TPC-H Q9: filter lineitem by partkeys matching LIKE).
  std::vector<size_t> load_order;
  load_order.reserve(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) { load_order.push_back(i);
}
  std::ranges::stable_sort(load_order, [&](size_t a, size_t b) {
    const auto rank = [&](size_t i) {
      if (!local_predicates[i].empty()) { return 0;
}
      const auto stored = active_runtime->table_key_filters.find(
          statement.Sources()[i].table);
      if (stored != active_runtime->table_key_filters.end() &&
          stored->second.owner == &statement) {
        return 0;
      }
      return 1;
    };
    const int a_rank = rank(a);
    const int b_rank = rank(b);
    if (a_rank != b_rank) { return a_rank < b_rank;
}
    return a < b;
  });
  std::vector<bool> loaded(relations.size(), false);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i]) { loaded[i] = true;  // already materialized above
}
  }
  for (size_t idx : load_order) {
    if (!base_sources[idx]) {
      FilterRelation(context, &relations[idx], local_predicates[idx], outer,
                     ctes);
      loaded[idx] = true;
      continue;
    }
    std::unordered_set<int64_t> key_filter;
    std::optional<slot_t> key_column;  // offset in the projected scan row
    size_t best_driver_rows = std::numeric_limits<size_t>::max();
    bool best_driver_selective = false;
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 ||
          !predicate.sources.contains(idx) ||
          !IsColumnEqualityPredicate(predicate.expression)) {
        continue;
      }
      size_t other = *predicate.sources.begin();
      if (other == idx) { other = *std::next(predicate.sources.begin());
}
      if (!loaded[other]) { continue;
}
      const bool other_selective =
          !local_predicates[other].empty() ||
          (active_runtime != nullptr &&
           [&] {
             const auto stored = active_runtime->table_key_filters.find(
                 statement.Sources()[other].table);
             return stored != active_runtime->table_key_filters.end() &&
                    stored->second.owner == &statement;
           }());
      const size_t driver_rows = relations[other].TotalRows();
      // Prefer drivers that were themselves filtered (e.g. part LIKE), even
      // if an unfiltered neighbor has slightly fewer rows (supplier).
      if (key_column) {
        if (best_driver_selective && !other_selective) { continue;
}
        if (best_driver_selective == other_selective &&
            driver_rows >= best_driver_rows) {
          continue;
        }
      }
      const BinaryExpression& binary =
          predicate.expression->AsBinaryExpression();
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto idx_left = LocalColumnOffset(relations[idx].schema, lhs);
      const auto idx_right = LocalColumnOffset(relations[idx].schema, rhs);
      const auto other_left = LocalColumnOffset(relations[other].schema, lhs);
      const auto other_right = LocalColumnOffset(relations[other].schema, rhs);
      std::optional<slot_t> idx_col;
      std::optional<slot_t> other_col;
      if (idx_left && other_right) {
        idx_col = static_cast<slot_t>(*idx_left);
        other_col = static_cast<slot_t>(*other_right);
      } else if (idx_right && other_left) {
        idx_col = static_cast<slot_t>(*idx_right);
        other_col = static_cast<slot_t>(*other_left);
      }
      if (!idx_col || !other_col) { continue;
}
      if (!SingleIntegerJoinKey(relations[idx].schema, {*idx_col}) ||
          !SingleIntegerJoinKey(relations[other].schema, {*other_col})) {
        continue;
      }
      // Map full-schema column index into the projected scan layout.
      const auto& proj = projections[idx];
      const auto proj_it = std::ranges::find(proj, *idx_col);
      if (proj_it == proj.end()) { continue;
}
      const auto projected_col =
          static_cast<slot_t>(std::distance(proj.begin(), proj_it));

      std::unordered_set<int64_t> candidate;
      relations[other].FinishSpill();
      relations[other].ForEachRow([&](const Row& row) {
        const Value& value = row[*other_col];
        if (!value.IsNull()) { candidate.insert(value.value.int_value);
}
      });
      if (candidate.empty() || candidate.size() >= 2'000'000) { continue;
}
      // Skip near-full key domains (e.g. all supplier keys vs lineitem).
      if (!other_selective && candidate.size() >= driver_rows &&
          driver_rows >= 1000) {
        continue;
      }
      key_filter = std::move(candidate);
      key_column = projected_col;
      best_driver_rows = driver_rows;
      best_driver_selective = other_selective;
    }
    const bool use_key_filter = key_column.has_value();
    const std::unordered_set<int64_t>* filter_ptr = nullptr;
    std::optional<slot_t> filter_col = use_key_filter ? key_column : std::nullopt;
    if (use_key_filter && active_runtime != nullptr) {
      ++active_runtime->key_filter_scans;
      active_runtime->key_filter_keys += key_filter.size();
      // Remember for later scans of this base table within this statement.
      // The stash is owner-scoped so other statements never inherit it.
      // Column is stored as the full-schema slot so other projections can
      // remap.
      const auto& proj = projections[idx];
      const slot_t full_slot = proj[*key_column];
      TableKeyFilter& stored =
          active_runtime->table_key_filters[statement.Sources()[idx].table];
      stored.keys = std::move(key_filter);
      stored.column = full_slot;
      stored.owner = &statement;
      filter_ptr = &stored.keys;
    } else if (!use_key_filter && active_runtime != nullptr) {
      // Fall back to a stash created earlier within this same statement (e.g.
      // an uncorrelated IN pushdown). Entries owned by other statements are
      // never applied here.
      const auto stored = active_runtime->table_key_filters.find(
          statement.Sources()[idx].table);
      if (stored != active_runtime->table_key_filters.end() &&
          stored->second.owner == &statement) {
        const auto& proj = projections[idx];
        const auto proj_it = std::ranges::find(proj, stored->second.column);
        if (proj_it != proj.end()) {
          filter_ptr = &stored->second.keys;
          filter_col = static_cast<slot_t>(
              std::distance(proj.begin(), proj_it));
        }
      }
    }
    relations[idx] = LoadSource(context, statement.Sources()[idx], outer, ctes,
                                &projections[idx], &local_predicates[idx],
                                filter_ptr, filter_col);
    loaded[idx] = true;
  }

  size_t first = 0;
  for (size_t i = 1; i < relations.size(); ++i) {
    if (relations[i].TotalRows() < relations[first].TotalRows()) { first = i;
}
  }
  Relation result = std::move(relations[first]);
  std::unordered_set<size_t> joined{first};
  std::unordered_set<size_t> remaining;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (i != first) { remaining.insert(i);
}
  }

  while (!remaining.empty()) {
    size_t next = *remaining.begin();
    size_t next_estimate = std::numeric_limits<size_t>::max();
    bool next_connected = false;
    for (size_t candidate : remaining) {
      std::unordered_set<size_t> after = joined;
      after.insert(candidate);
      std::vector<Expression> applicable;
      for (const PredicateInfo& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() < 2 ||
            !predicate.sources.contains(candidate) ||
            !IsSubset(predicate.sources, after)) {
          continue;
        }
        applicable.push_back(predicate.expression);
      }
      const size_t estimate =
          EstimateJoinRows(result, relations[candidate], applicable);
      const bool connected = !applicable.empty();
      const bool cheaper =
          estimate < next_estimate ||
          (estimate == next_estimate &&
           relations[candidate].TotalRows() < relations[next].TotalRows());
      if ((connected && !next_connected) ||
          (connected == next_connected && cheaper)) {
        next = candidate;
        next_estimate = estimate;
        next_connected = connected;
      }
    }

    std::unordered_set<size_t> after = joined;
    after.insert(next);
    std::vector<Expression> applicable;
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() < 2 || !predicate.sources.contains(next) ||
          !IsSubset(predicate.sources, after)) {
        continue;
      }
      applicable.push_back(predicate.expression);
    }
    result = InnerJoin(context, std::move(result), std::move(relations[next]),
                       applicable, outer, ctes);
    joined.insert(next);
    remaining.erase(next);
  }
  return result;
}


}  // namespace tinylamb::relational_detail
