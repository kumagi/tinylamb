/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/implementation_rules.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregation_plan.hpp"
#include "common/constants.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/join_kind.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/named_expression.hpp"
#include "full_scan_plan.hpp"
#include "index/index.hpp"
#include "index_only_scan_plan.hpp"
#include "index_scan_plan.hpp"
#include "limit_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "projection_plan.hpp"
#include "query/query_data.hpp"
#include "selection_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

using cascades::BestPlan;
using cascades::PlanAlternative;
using cascades::PhysicalProperties;

struct Range {
  std::optional<Value> min;
  std::optional<Value> max;
  bool min_inclusive{false};
  bool max_inclusive{false};

  enum class Direction : bool { kRight, kLeft };

  void Update(BinaryOperation operation, const Value& value,
              Direction direction) {
    switch (operation) {
      case BinaryOperation::kEquals:
        min = max = value;
        min_inclusive = max_inclusive = true;
        return;
      case BinaryOperation::kNotEquals:
        return;
      case BinaryOperation::kLessThan:
      case BinaryOperation::kGreaterThan: {
        const bool upper = (direction == Direction::kRight &&
                            operation == BinaryOperation::kLessThan) ||
                           (direction == Direction::kLeft &&
                            operation == BinaryOperation::kGreaterThan);
        if (upper) {
          if (!max || value < *max) { max = value;
}
          max_inclusive = false;
        } else {
          if (!min || *min < value) { min = value;
}
          min_inclusive = false;
        }
        return;
      }
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThanEquals: {
        const bool upper = (direction == Direction::kRight &&
                            operation == BinaryOperation::kLessThanEquals) ||
                           (direction == Direction::kLeft &&
                            operation == BinaryOperation::kGreaterThanEquals);
        if (upper) {
          if (!max || value <= *max) { max = value;
}
          max_inclusive = true;
        } else {
          if (!min || *min <= value) { min = value;
}
          min_inclusive = true;
        }
        return;
      }
      default:
        return;
    }
  }
};

template <typename T>
bool Covered(const std::unordered_set<T>& available,
             const std::unordered_set<T>& required) {
  return std::ranges::all_of(
      required, [&](const T& value) { return available.contains(value); });
}

// Selectivity of a (possibly null) predicate; 1.0 when absent. Estimator
// limitation: equality/range heuristics from per-column NDV, no histograms.
double Selectivity(const TableStatistics& stats, const Schema& schema,
                   const Expression& predicate) {
  if (!predicate) { return 1.0;
}
  return std::clamp(stats.EstimateSelectivity(schema, predicate), 0.0, 1.0);
}

// Rewrites column qualifiers equal to `relation` into `physical`. Scan-level
// machinery (range extraction, executor evaluation) speaks physical table
// names while memo conjuncts speak relation identities (Phase 8).
Expression QualifyDown(const Expression& expression,  // NOLINT(misc-no-recursion) // AST rewrite recursion mirrors expression tree depth; bounded by parser depth guard.
                       const std::string& relation,
                       const std::string& physical) {
  if (!expression || relation == physical) { return expression;
}
  if (expression->Type() == TypeTag::kColumnValue) {
    ColumnName column = expression->AsColumnValue().GetColumnName();
    if (column.schema == relation) {
      column.schema = physical;
      return ColumnValueExp(column);
    }
    return expression;
  }
  std::vector<Expression> children;
  children.reserve(ExpressionChildren(expression).size());
  for (const Expression& child : ExpressionChildren(expression)) {
    children.push_back(QualifyDown(child, relation, physical));
  }
  return WithExpressionChildren(expression, children);
}

// Wraps a scan-level candidate so its output schema is named by the relation
// identity instead of the physical table (self-join support): rows stream
// through unchanged while downstream operators resolve `alias.column`
// references against the renamed schema.
Plan RenameToRelation(Plan candidate, const std::string& relation,
                      const std::string& physical) {
  if (relation == physical) { return candidate;
}
  // Positional pass-through rename; IsOrderedBy translates requests back to
  // physical names so index-provided order stays visible above the rename.
  return std::make_shared<RelationRenamePlan>(std::move(candidate), relation,
                                              physical);
}

Plan BuildIndexScan(const Table& table, const Index& index,
                    const TableStatistics& statistics,
                    std::vector<Value> begin_key, std::vector<Value> end_key,
                    bool ascending, const Expression& predicate,
                    const std::vector<NamedExpression>& select,
                    bool require_row_position,
                    bool wait_for_write_intent,
                    std::vector<ColumnName> provided_order) {
  std::unordered_set<ColumnName> touched = predicate->TouchedColumns();
  for (const NamedExpression& item : select) {
    touched.merge(item.expression->TouchedColumns());
  }
  std::unordered_set<slot_t> touched_offsets;
  for (const ColumnName& column : touched) {
    const int offset = table.GetSchema().Offset(column);
    if (offset >= 0) { touched_offsets.insert(static_cast<slot_t>(offset));
}
  }
  if (!require_row_position && !index.RetainsDeletedEntries() &&
      Covered(index.CoveredColumns(), touched_offsets)) {
    return std::make_shared<IndexOnlyScanPlan>(
        table, index, statistics, std::move(begin_key), std::move(end_key),
        ascending, predicate, std::move(provided_order));
  }
  return std::make_shared<IndexScanPlan>(
      table, index, statistics, std::move(begin_key), std::move(end_key),
      ascending, predicate, std::move(provided_order), require_row_position,
      wait_for_write_intent);
}

// Top-K costing (Phase 5): an alternative that already delivers the required
// ordering only streams limit-hint rows because the executor stack is lazy.
void ApplyLimitHint(const Plan& plan, const PhysicalProperties& required,
                    const cascades::RuleContext& context, double selectivity,
                    double* local_cost, double* estimated_rows) {
  if (required.limit_hint == std::numeric_limits<size_t>::max() ||
      context.query == nullptr) {
    return;
  }
  if (!plan->IsOrderedBy(context.query->order_expressions_,
                         context.query->order_ascending_)) {
    return;
  }
  const auto rows = static_cast<double>(required.limit_hint);
  *local_cost = std::min(*local_cost, rows / std::max(selectivity, 0.01));
  *estimated_rows = std::min(*estimated_rows, rows);
}

// Independent-column statistics are sufficient to distinguish two common
// composite-index choices even without multi-column histograms.  In
// particular, (warehouse,district,customer,order) is far narrower for three
// equality predicates than (warehouse,district,order).  Costing only the
// first key made both look identical and a LIMIT 1 tie selected the latter,
// scanning and sorting an entire TPC-C district.
double EqualityPrefixRows(const TableStatistics& statistics,
                          const Index& index,
                          const std::vector<Value>& equality_values) {
  if (statistics.Rows() == 0) { return 0;
}
  double rows = static_cast<double>(statistics.Rows());
  for (size_t key = 0;
       key < equality_values.size() && key < index.sc_.key_.size(); ++key) {
    const slot_t column = index.sc_.key_[key];
    if (column >= statistics.Columns()) { break;
}
    const ColumnStats& stats = statistics.Column(column);
    double selectivity = 1.0;
    if (stats.NonNullCount() != 0) {
      const double frequency = stats.EstimateEqual(equality_values[key]);
      if (frequency > 0) {
        selectivity = frequency / stats.NonNullCount();
      } else if (stats.Distinct() != 0) {
        selectivity = 1.0 / stats.Distinct();
      }
    }
    rows *= std::clamp(selectivity, 0.0, 1.0);
  }
  return std::max(1.0, rows);
}

std::vector<Expression> SplitDisjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (expression && expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kOr) {
    std::vector<Expression> result =
        SplitDisjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitDisjuncts(expression->AsBinaryExpression().Right());
    result.insert(result.end(), std::make_move_iterator(right.begin()),
                  std::make_move_iterator(right.end()));
    return result;
  }
  return {expression};
}

std::optional<std::vector<std::vector<Value>>> DisjunctiveIndexPrefixes(
    const Expression& filter, const Schema& schema, const Index& index) {
  const std::vector<Expression> disjuncts = SplitDisjuncts(filter);
  if (disjuncts.size() < 2) { return std::nullopt;
}
  std::vector<std::vector<Value>> prefixes;
  prefixes.reserve(disjuncts.size());
  size_t common_size = 0;
  for (const Expression& disjunct : disjuncts) {
    std::unordered_map<slot_t, Value> equalities;
    for (const Expression& conjunct : SplitConjuncts(disjunct)) {
      if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) { continue;
}
      const auto& binary = conjunct->AsBinaryExpression();
      if (binary.Op() != BinaryOperation::kEquals) { continue;
}
      const ColumnValue* column = nullptr;
      const ConstantValue* constant = nullptr;
      if (binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kConstantValue) {
        column = &binary.Left()->AsColumnValue();
        constant = &binary.Right()->AsConstantValue();
      } else if (binary.Right()->Type() == TypeTag::kColumnValue &&
                 binary.Left()->Type() == TypeTag::kConstantValue) {
        column = &binary.Right()->AsColumnValue();
        constant = &binary.Left()->AsConstantValue();
      }
      if (column == nullptr || constant == nullptr) { continue;
}
      const int offset = schema.Offset(column->GetColumnName());
      if (offset >= 0) {
        equalities.insert_or_assign(static_cast<slot_t>(offset),
                                    constant->GetValue());
      }
    }
    std::vector<Value> prefix;
    for (slot_t slot : index.sc_.key_) {
      const auto value = equalities.find(slot);
      if (value == equalities.end()) { break;
}
      prefix.push_back(value->second);
    }
    if (prefix.empty()) { return std::nullopt;
}
    if (common_size == 0) {
      common_size = prefix.size();
    } else if (prefix.size() != common_size) {
      return std::nullopt;
    }
    prefixes.push_back(std::move(prefix));
  }
  std::ranges::sort(prefixes);
  prefixes.erase(std::ranges::unique(prefixes).begin(), prefixes.end());
  if (prefixes.size() != disjuncts.size()) {
    // Overlapping prefixes could emit a physical row more than once.
    return std::nullopt;
  }
  return prefixes;
}

std::vector<PlanAlternative> ScanAlternatives(
    const cascades::Memo& memo, cascades::GroupId group,
    const cascades::LogicalExpression& logical,
    const PhysicalProperties& required, const cascades::RuleContext& context,
    bool include_indexes, bool include_full_scan) {
  const Table& table = *context.tables.at(logical.table);
  const TableStatistics& statistics = *context.statistics.at(logical.table);
  const Schema& schema = table.GetSchema();
  const std::string relation = logical.table;
  const std::string physical = std::string(schema.Name());
  // The group filter carries the single-relation conjuncts of the WHERE
  // clause (Phase 2 pushdown); it must be applied in full because the root
  // SelectionPlan wrap no longer exists. Conjuncts speak relation identities
  // while scan machinery speaks physical table names, so translate down.
  const Expression filter =
      QualifyDown(memo.Get(group).filter, relation, physical);
  // The scan predicate handed to the executor: the group filter, or the
  // neutral `true` constant when the group has none.
  const Expression scan_predicate =
      filter ? filter : ConstantValueExp(Value(true));
  std::vector<NamedExpression> fallback_select;
  if (const auto found = context.scan_projections.find(relation);
      found != context.scan_projections.end() && !found->second.empty()) {
    fallback_select = found->second;
  } else {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      fallback_select.emplace_back(schema.GetColumn(i).Name());
    }
  }
  // Exposes the candidate under the relation identity so self-joins of one
  // physical table stay distinguishable downstream (no-op when unaliased).
  const auto finalize = [&relation, &physical](const Plan& plan) {
    return RenameToRelation(plan, relation, physical);
  };

  const double filter_selectivity = Selectivity(statistics, schema, filter);

  std::vector<PlanAlternative> candidates;
  if (include_indexes) {
    std::unordered_map<slot_t, Range> ranges;
    // Constant IN lists per column (Phase 8 B): each distinct value becomes
    // one point-lookup range on a leading index key.
    std::unordered_map<slot_t, std::vector<Value>> point_sets;
    std::vector<Expression> range_predicates;
    for (const Expression& conjunct : SplitConjuncts(filter)) {
      if (conjunct->Type() == TypeTag::kInExp) {
        const auto& in = conjunct->AsInExpression();
        if (in.child_ == nullptr ||
            in.child_->Type() != TypeTag::kColumnValue ||
            in.list_.empty()) {
          continue;
        }
        bool all_constant = true;
        for (const Expression& item : in.list_) {
          if (item->Type() != TypeTag::kConstantValue) {
            all_constant = false;
            break;
          }
        }
        if (!all_constant) { continue;
}
        const int offset =
            schema.Offset(in.child_->AsColumnValue().GetColumnName());
        if (offset < 0) { continue;
}
        for (const Expression& item : in.list_) {
          point_sets[static_cast<slot_t>(offset)].push_back(
              item->AsConstantValue().GetValue());
        }
        continue;
      }
      if (conjunct->Type() != TypeTag::kBinaryExp) { continue;
}
      const auto& binary = conjunct->AsBinaryExpression();
      if (!IsComparison(binary.Op())) { continue;
}

      const ColumnValue* column = nullptr;
      const ConstantValue* constant = nullptr;
      Range::Direction direction = Range::Direction::kRight;
      if (binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kConstantValue) {
        column = &binary.Left()->AsColumnValue();
        constant = &binary.Right()->AsConstantValue();
      } else if (binary.Left()->Type() == TypeTag::kConstantValue &&
                 binary.Right()->Type() == TypeTag::kColumnValue) {
        column = &binary.Right()->AsColumnValue();
        constant = &binary.Left()->AsConstantValue();
        direction = Range::Direction::kLeft;
      }
      if (column == nullptr || constant == nullptr) { continue;
}
      const int offset = schema.Offset(column->GetColumnName());
      if (offset < 0) { continue;
}
      ranges[static_cast<slot_t>(offset)].Update(binary.Op(),
                                                 constant->GetValue(), direction);
      range_predicates.push_back(conjunct);
    }

    for (size_t index_offset = 0; index_offset < table.IndexCount();
         ++index_offset) {
      const Index& index = table.GetIndex(index_offset);

      // OR-of-conjunctions over distinct composite-key prefixes is a union
      // of disjoint index ranges.  This keeps batched OLTP DML on its indexes
      // instead of turning `(...pk...) OR (...pk...)` into a table scan.
      if (const auto prefixes =
              DisjunctiveIndexPrefixes(filter, schema, index)) {
        std::vector<
            std::pair<std::vector<Value>, std::vector<Value>>> scan_ranges;
        scan_ranges.reserve(prefixes->size());
        double access_rows = 0;
        for (const std::vector<Value>& prefix : *prefixes) {
          scan_ranges.emplace_back(prefix, prefix);
          access_rows += EqualityPrefixRows(statistics, index, prefix);
        }
        Plan disjunctive = std::make_shared<IndexScanPlan>(
            table, index, statistics, std::move(scan_ranges), true,
            scan_predicate, std::vector<ColumnName>{},
            required.require_row_position,
            required.wait_for_write_intent);
        if (fallback_select.size() != disjunctive->GetSchema().ColumnCount()) {
          disjunctive = std::make_shared<ProjectionPlan>(
              disjunctive, fallback_select);
        }
        disjunctive = finalize(disjunctive);
        candidates.push_back(PlanAlternative{
            .plan=std::move(disjunctive),
            .local_cost=std::max(1.0, access_rows),
            .estimated_rows=std::max(
                1.0, access_rows * filter_selectivity)});
      }

      // Point-union alternative for a constant IN list after an equality
      // prefix. Examples: `id IN (...)` and a composite `(warehouse,item)`
      // index queried by `warehouse = ? AND item IN (...)`. Each point is a
      // complete seek key through the IN column; the residual predicate keeps
      // correctness independent of bound construction.
      size_t point_key_offset = 0;
      std::vector<Value> equality_prefix_values;
      while (point_key_offset < index.sc_.key_.size()) {
        const slot_t slot = index.sc_.key_[point_key_offset];
        if (point_sets.contains(slot)) { break;
}
        const auto range = ranges.find(slot);
        if (range == ranges.end() || !range->second.min ||
            !range->second.max || *range->second.min != *range->second.max) {
          point_key_offset = index.sc_.key_.size();
          break;
        }
        equality_prefix_values.push_back(*range->second.min);
        ++point_key_offset;
      }
      const bool has_point_column = point_key_offset < index.sc_.key_.size();
      const slot_t point_key_slot =
          has_point_column ? index.sc_.key_[point_key_offset] : slot_t{0};
      const auto points = has_point_column ? point_sets.find(point_key_slot)
                                           : point_sets.end();
      if (points != point_sets.end() &&
          ranges.find(point_key_slot) == ranges.end()) {
        std::vector<Value> values = points->second;
        std::ranges::sort(values);
        values.erase(std::ranges::unique(values).begin(), values.end());
        if (!values.empty()) {
          std::vector<std::pair<std::vector<Value>, std::vector<Value>>>
              scan_ranges;
          scan_ranges.reserve(values.size());
          for (const Value& value : values) {
            std::vector<Value> key = equality_prefix_values;
            key.push_back(value);
            std::vector<Value> end_key = key;
            scan_ranges.emplace_back(std::move(key), std::move(end_key));
          }
          std::vector<ColumnName> point_order;
          if (scan_ranges.size() == 1) {
            for (size_t key = point_key_offset + 1;
                 key < index.sc_.key_.size(); ++key) {
              point_order.push_back(
                  schema.GetColumn(index.sc_.key_[key]).Name());
            }
          }
          Plan point_candidate = std::make_shared<IndexScanPlan>(
              table, index, statistics, std::move(scan_ranges), true,
              scan_predicate, std::move(point_order),
              required.require_row_position,
              required.wait_for_write_intent);
          if (fallback_select.size() !=
              point_candidate->GetSchema().ColumnCount()) {
            point_candidate = std::make_shared<ProjectionPlan>(
                point_candidate, fallback_select);
          }
          point_candidate = finalize(point_candidate);
          auto point_cost =
              static_cast<double>(point_candidate->AccessRowCount());
          // Per-point cardinality: rows / NDV of the leading key column.
          double ndv = 1.0;
          if (point_key_slot < statistics.Columns()) {
            ndv = std::max<double>(
                1.0, statistics.Column(point_key_slot).Distinct());
          }
          double point_rows =
              std::min<double>(static_cast<double>(statistics.Rows()),
                               static_cast<double>(values.size()) *
                                   std::max(1.0, static_cast<double>(
                                                     statistics.Rows()) /
                                                     ndv));
          point_rows = std::max(point_rows, 1.0);
          ApplyLimitHint(point_candidate, required, context,
                         filter_selectivity, &point_cost, &point_rows);
          candidates.push_back(PlanAlternative{
              .plan=std::move(point_candidate),
              .local_cost=point_cost, .estimated_rows=point_rows});
        }
      }

      std::vector<Value> begin_key;
      std::vector<Value> end_key;
      std::unordered_set<slot_t> consumed;
      // Slots fully pinned by equality. Only these are provably exact in the
      // scan output; range-bound slots may leak boundary rows, so predicates
      // touching them must be re-checked by a Selection (see `covered`).
      std::unordered_set<slot_t> equality_slots;
      size_t equality_prefix = 0;
      // Key construction contract: begin/end vectors may differ in length
      // beyond an equality prefix. EncodeParts encodes begin verbatim while
      // EncodeEndParts appends 0xff to a SHORT end vector, turning it into a
      // prefix ceiling; the scan predicate re-checks every conjunct either
      // way, so boundary inclusivity is always corrected per row.
      for (slot_t slot : index.sc_.key_) {
        const auto range = ranges.find(slot);
        if (range == ranges.end()) { break;
}
        const bool equality = range->second.min && range->second.max &&
                              *range->second.min == *range->second.max;
        if (equality) {
          begin_key.push_back(*range->second.min);
          end_key.push_back(*range->second.max);
          consumed.insert(slot);
          equality_slots.insert(slot);
          ++equality_prefix;
          continue;
        }
        if (range->second.min) {
          // Lower bound: seek starts at this key.
          begin_key.push_back(*range->second.min);
        } else if (!begin_key.empty()) {
          // Upper bound only after an equality prefix: end keeps the prefix,
          // relying on the short-end ceiling described above.
          if (range->second.max) { end_key.push_back(*range->second.max);
}
        } else {
          // A one-sided bound on the first key position with no equality
          // prefix gives no usable seek start; skip this index explicitly
          // and let the full-scan alternative handle it.
          break;
        }
        consumed.insert(slot);
        break;
      }
      if (begin_key.empty()) { continue;
}

      std::vector<ColumnName> provided_order;
      for (size_t key = equality_prefix; key < index.sc_.key_.size(); ++key) {
        provided_order.push_back(
            schema.GetColumn(index.sc_.key_[key]).Name());
      }
      // Prefix encoding is correct for forward scans. DESC still needs a
      // sort until the B+tree iterator can land on the last key of a prefix.

      Plan candidate = BuildIndexScan(
          table, index, statistics, begin_key, end_key, true, scan_predicate,
          fallback_select, required.require_row_position,
          required.wait_for_write_intent,
          std::move(provided_order));
      // The scan filter is applied either by the scan itself or, when its
      // columns are not covered by the index key, by a Selection on top.
      // Only equality-pinned slots are provably exact in the scan output;
      // range bounds (and the short-end ceiling) can leak boundary rows, so
      // any predicate touching them is re-checked by a Selection.
      const bool covered = !filter || std::ranges::all_of(
          filter->TouchedColumns(), [&](const ColumnName& column) {
            const int offset = schema.Offset(column);
            return offset >= 0 &&
                   equality_slots.contains(static_cast<slot_t>(offset));
          });
      if (!covered) {
        candidate = std::make_shared<SelectionPlan>(candidate, filter,
                                                    statistics);
      }
      if (fallback_select.size() != candidate->GetSchema().ColumnCount()) {
        candidate = std::make_shared<ProjectionPlan>(candidate, fallback_select);
      }
      candidate = finalize(candidate);
      auto local_cost = static_cast<double>(candidate->AccessRowCount());
      local_cost = std::min(
          local_cost,
          EqualityPrefixRows(statistics, index, equality_prefix_values));
      // Residual (non-range) conjuncts narrow the output further.
      double estimated_rows =
          static_cast<double>(candidate->EmitRowCount()) * filter_selectivity;
      estimated_rows = std::max(estimated_rows, 1.0);
      ApplyLimitHint(candidate, required, context, filter_selectivity,
                     &local_cost, &estimated_rows);
      candidates.push_back(
          PlanAlternative{.plan=std::move(candidate), .local_cost=local_cost, .estimated_rows=estimated_rows});
    }
  }

  if (include_full_scan) {
    Plan full_scan = std::make_shared<FullScanPlan>(table, statistics);
    if (filter) {
      full_scan = std::make_shared<SelectionPlan>(full_scan, filter,
                                                  statistics);
    }
    if (fallback_select.size() != full_scan->GetSchema().ColumnCount()) {
      full_scan = std::make_shared<ProjectionPlan>(full_scan, fallback_select);
    }
    full_scan = finalize(full_scan);
    auto local_cost = static_cast<double>(statistics.Rows());
    // access_method hint (Phase 5): a soft preference against full scans.
    if (required.access_method == cascades::AccessMethod::kPreferIndex &&
        table.IndexCount() > 0) {
      local_cost *= 2.0;
    }
    auto estimated_rows = static_cast<double>(full_scan->EmitRowCount());
    ApplyLimitHint(full_scan, required, context, filter_selectivity,
                   &local_cost, &estimated_rows);
    candidates.push_back(
        PlanAlternative{.plan=std::move(full_scan), .local_cost=local_cost, .estimated_rows=estimated_rows});
  }
  return candidates;
}

// D3 join cardinality: |L|*|R| / max NDV of the equality keys, capped by the
// cross-product size; falls back to the cross product when stats are missing.
double JoinCardinality(const BestPlan& left, const BestPlan& right,
                       const std::vector<std::pair<ColumnName, ColumnName>>&
                           equalities) {
  double max_ndv = 0;
  for (const auto& [left_column, right_column] : equalities) {
    const int lo = left.plan->GetSchema().Offset(left_column);
    const int ro = right.plan->GetSchema().Offset(right_column);
    double ndv = 0;
    if (lo >= 0 && static_cast<size_t>(lo) < left.plan->GetStats().Columns()) {
      ndv = std::max<double>(ndv,
                             left.plan->GetStats().Column(lo).Distinct());
    }
    if (ro >= 0 && static_cast<size_t>(ro) < right.plan->GetStats().Columns()) {
      ndv = std::max<double>(ndv,
                             right.plan->GetStats().Column(ro).Distinct());
    }
    max_ndv = std::max(max_ndv, ndv);
  }
  const double cross = left.estimated_rows * right.estimated_rows;
  if (max_ndv < 1) { return cross;
}
  return std::min(cross, cross / max_ndv);
}

std::vector<PlanAlternative> JoinAlternatives(
    const cascades::Memo& memo, cascades::GroupId right_group,
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, const cascades::RuleContext& context, bool hash,
    bool index, bool cross) {
  const Expression predicate = condition ? *condition : nullptr;
  std::vector<std::pair<ColumnName, ColumnName>> equalities;
  std::vector<Expression> equi_conjuncts;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (conjunct->Type() != TypeTag::kBinaryExp) { continue;
}
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    if (left.plan->GetSchema().Offset(lhs) >= 0 &&
        right.plan->GetSchema().Offset(rhs) >= 0) {
      equalities.emplace_back(lhs, rhs);
    } else if (left.plan->GetSchema().Offset(rhs) >= 0 &&
               right.plan->GetSchema().Offset(lhs) >= 0) {
      equalities.emplace_back(rhs, lhs);
    } else {
      continue;
    }
    equi_conjuncts.push_back(conjunct);
  }
  std::vector<Expression> residual_conjuncts;
  if (predicate) {
    std::unordered_set<std::string> equi;
    for (const Expression& conjunct : equi_conjuncts) {
      equi.insert(conjunct->ToString());
    }
    for (const Expression& conjunct : SplitConjuncts(predicate)) {
      if (!equi.contains(conjunct->ToString())) {
        residual_conjuncts.push_back(conjunct);
      }
    }
  }
  const Expression residual = CombineConjuncts(residual_conjuncts);

  const double l_rows = left.estimated_rows;
  const double r_rows = right.estimated_rows;
  const double equi_estimate = JoinCardinality(left, right, equalities);

  // Wraps the plan in a Selection applying the residual conjuncts (Phase 4:
  // the root SelectionPlan no longer re-applies them).
  const auto with_residual = [&](Plan plan, double estimate) {
    double est = estimate;
    if (residual) {
      plan = std::make_shared<SelectionPlan>(plan, residual,
                                             plan->GetStats());
      est = std::min(est, static_cast<double>(plan->EmitRowCount()));
    }
    return std::pair<Plan, double>{std::move(plan), est};
  };

  std::vector<PlanAlternative> candidates;
  if (cross) {
    Plan product = std::make_shared<ProductPlan>(left.plan, right.plan);
    const double cross_estimate = equi_conjuncts.empty()
                                      ? l_rows * r_rows
                                      : equi_estimate;
    // The cross-product executor does not consume join keys.  A nested-loop
    // implementation must therefore evaluate the complete join predicate,
    // including equality conjuncts; `with_residual` is only valid for hash
    // and index joins that already enforce those equalities themselves.
    if (predicate) {
      product = std::make_shared<SelectionPlan>(product, predicate,
                                                product->GetStats());
    }
    const double local_cost = (l_rows * r_rows) + l_rows + r_rows;
    candidates.push_back(PlanAlternative{
        .plan=std::move(product),
        .local_cost=local_cost,
        .estimated_rows=cross_estimate});
  }
  if (equi_conjuncts.empty()) { return candidates;
}

  std::vector<ColumnName> left_columns;
  std::vector<ColumnName> right_columns;
  for (const auto& [left_column, right_column] : equalities) {
    left_columns.push_back(left_column);
    right_columns.push_back(right_column);
  }

  if (hash) {
    // Offer both physical strategies; the in-memory build is penalized when
    // its estimated footprint exceeds the query-memory soft budget.
    for (const HashJoinMode mode :
         {HashJoinMode::kInMemory, HashJoinMode::kHybrid}) {
      Plan join = std::make_shared<ProductPlan>(
          left.plan, left_columns, right.plan, right_columns, mode);
      double local_cost = l_rows + r_rows;
      const double build_bytes = r_rows * kHashJoinRowBytesEstimate;
      if (mode == HashJoinMode::kInMemory && PreferHybridHashJoin(build_bytes)) {
        local_cost += r_rows * 3;
      }
      auto [plan, est] = with_residual(std::move(join), equi_estimate);
      candidates.push_back(PlanAlternative{.plan=std::move(plan), .local_cost=local_cost, .estimated_rows=est});
    }
  }

  if (index) {
    const Table* right_table = right.plan->ScanSource();
    if (right_table != nullptr) {
      // The IndexJoin executor bypasses the right child plan entirely and
      // probes the raw table through its index. A filter merged into the
      // right scan group is implemented by that child plan, so offering an
      // index join here would silently drop it; hash/NL joins consume the
      // child executor and stay safe.
      if (!memo.Get(right_group).filter) {
        // Locate the relation key owning this physical table: aliased
        // self-joins rename scan schemas, and statistics are keyed by the
        // relation identity.
        std::string right_relation;
        for (const auto& [relation, table] : context.tables) {
          if (table.get() == right_table) {
            right_relation = relation;
            break;
          }
        }
        const auto stats_it =
            right_relation.empty()
                ? context.statistics.end()
                : context.statistics.find(right_relation);
        if (stats_it != context.statistics.end()) {
          const Schema& physical_schema = right_table->GetSchema();
          const auto to_physical = [&](const ColumnName& column) {
            if (column.schema == right_relation &&
                right_relation != std::string(physical_schema.Name())) {
              return ColumnName(physical_schema.Name(), column.name);
            }
            return column;
          };
          // Declared output renames the full-width right side so the raw
          // rows emitted by the executor stay positionally aligned with the
          // schema consumed above us.
          std::vector<Column> renamed_columns;
          renamed_columns.reserve(physical_schema.ColumnCount());
          for (size_t i = 0; i < physical_schema.ColumnCount(); ++i) {
            renamed_columns.emplace_back(
                ColumnName(right_relation,
                           physical_schema.GetColumn(i).Name().name));
          }
          const Schema declared_output =
              left.plan->GetSchema() + Schema("", std::move(renamed_columns));
          for (size_t i = 0; i < right_table->IndexCount(); ++i) {
            const Index& index_handle = right_table->GetIndex(i);
            for (const ColumnName& column : right_columns) {
              if (to_physical(column) ==
                  physical_schema.GetColumn(index_handle.sc_.key_.front())
                      .Name()) {
                std::vector<ColumnName> right_physical;
                right_physical.reserve(right_columns.size());
                for (const ColumnName& c : right_columns) {
                  right_physical.push_back(to_physical(c));
                }
                Plan join = std::make_shared<ProductPlan>(
                    left.plan, left_columns, *right_table, index_handle,
                    right_physical, *stats_it->second, declared_output);
                const double local_cost = (l_rows * 2.0) + equi_estimate;
                auto [plan, est] =
                    with_residual(std::move(join), equi_estimate);
                candidates.push_back(
                    PlanAlternative{.plan=std::move(plan), .local_cost=local_cost, .estimated_rows=est});
              }
            }
          }
        }
      }
    }
  }
  return candidates;
}

// Semi/anti hash join alternatives (executor P0): map the memo's semi/anti
// join payload onto the existing HashJoin executor's JoinKind. Every conjunct
// must be an equality across both sides -- residual predicates cannot be
// re-applied above a membership join without changing semantics, so other
// shapes stay on their existing decorrelation / relational paths.
std::vector<PlanAlternative> SemiAntiJoinAlternatives(
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, cascades::LogicalJoinKind kind) {
  if (!condition) { return {};
}
  std::vector<std::pair<ColumnName, ColumnName>> equalities;
  for (const Expression& conjunct : SplitConjuncts(*condition)) {
    if (conjunct->Type() != TypeTag::kBinaryExp ||
        conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return {};
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      return {};
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    if (left.plan->GetSchema().Offset(lhs) >= 0 &&
        right.plan->GetSchema().Offset(rhs) >= 0) {
      equalities.emplace_back(lhs, rhs);
    } else if (left.plan->GetSchema().Offset(rhs) >= 0 &&
               right.plan->GetSchema().Offset(lhs) >= 0) {
      equalities.emplace_back(rhs, lhs);
    } else {
      return {};
    }
  }
  if (equalities.empty()) { return {};
}
  std::vector<ColumnName> left_columns;
  std::vector<ColumnName> right_columns;
  left_columns.reserve(equalities.size());
  right_columns.reserve(equalities.size());
  for (const auto& [left_column, right_column] : equalities) {
    left_columns.push_back(left_column);
    right_columns.push_back(right_column);
  }
  // Membership joins emit each probe row at most once: semi keeps matching
  // rows (bounded by the smaller side), anti keeps unmatched rows.
  const double l_rows = left.estimated_rows;
  const double r_rows = right.estimated_rows;
  const double estimate =
      kind == cascades::LogicalJoinKind::kSemi ? std::min(l_rows, r_rows)
                                               : l_rows;
  const JoinKind exec_kind =
      kind == cascades::LogicalJoinKind::kSemi ? SemiJoinKind() : AntiJoinKind();
  std::vector<PlanAlternative> candidates;
  for (const HashJoinMode mode :
       {HashJoinMode::kInMemory, HashJoinMode::kHybrid}) {
    Plan join = std::make_shared<ProductPlan>(
        left.plan, left_columns, right.plan, right_columns, mode, exec_kind);
    double local_cost = l_rows + r_rows;
    const double build_bytes = r_rows * kHashJoinRowBytesEstimate;
    if (mode == HashJoinMode::kInMemory && PreferHybridHashJoin(build_bytes)) {
      local_cost += r_rows * 3;
    }
    candidates.push_back(PlanAlternative{
        .plan=std::move(join), .local_cost=local_cost, .estimated_rows=estimate});
  }
  return candidates;
}

}  // namespace

StatusOr<Plan> OptimizeSingleRelation(
    const QueryData& query, const Expression& predicate,
    const std::vector<NamedExpression>& projection_items, bool has_aggregate,
    const cascades::PhysicalProperties& required,
    const cascades::RuleContext& context) {
  if (query.from_.size() != 1) { return Status::kNotImplemented; }

  const std::string& relation = query.from_.front();
  cascades::Memo memo;
  const cascades::GroupId group = memo.Build(
      query.from_, {{predicate, std::vector<std::string>{relation}}});
  const cascades::Group& scan_group = memo.Get(group);
  if (scan_group.expressions.size() != 1 ||
      scan_group.expressions.front().operation !=
          cascades::LogicalOperator::kScan) {
    return Status::kNotImplemented;
  }

  std::vector<PlanAlternative> alternatives = ScanAlternatives(
      memo, group, scan_group.expressions.front(), required, context,
      /*include_indexes=*/true, /*include_full_scan=*/true);
  if (alternatives.empty()) { return Status::kNotImplemented; }
  auto ordered = [&](const PlanAlternative& alternative) {
    return query.order_expressions_.empty() ||
           alternative.plan->IsOrderedBy(query.order_expressions_,
                                         query.order_ascending_);
  };
  const bool ordered_candidate =
      std::ranges::any_of(alternatives, ordered);
  const auto best = std::ranges::min_element(
      alternatives, {}, [&](const PlanAlternative& alternative) {
        // A physically ordered candidate avoids the engine-side sort.  The
        // general Cascades search enforces this required property before
        // comparing costs; mirror that behavior in the direct path.
        return std::pair{ordered_candidate && !ordered(alternative),
                         alternative.local_cost};
      });
  Plan plan = best->plan;
  if (has_aggregate) {
    if (required.require_row_position) { return Status::kNotImplemented; }
    plan = std::make_shared<AggregationPlan>(plan, projection_items);
  } else {
    plan = std::make_shared<ProjectionPlan>(plan, projection_items);
  }

  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    const bool needs_ordering = !query.order_expressions_.empty();
    if (!needs_ordering ||
        plan->IsOrderedBy(query.order_expressions_, query.order_ascending_)) {
      plan = std::make_shared<LimitPlan>(plan, query.limit_count_,
                                         query.limit_offset_);
    }
  }
  return plan;
}

const cascades::ImplementationRuleSet& DefaultImplementationRules() {
  using cascades::dsl::Any;
  using cascades::dsl::Scan;
  using cascades::dsl::Selection;
  using cascades::dsl::Join;
  using cascades::dsl::OuterJoin;
  namespace c = cascades;
  static const c::ImplementationRuleSet rules = [] {
    c::ImplementationRuleSet built;
    built.Add(c::ImplementationRule(
        "relational_ir",
        c::Pattern::Op(c::LogicalOperator::kRelational, {}),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>&, const PhysicalProperties&,
           const c::RuleContext&) {
          if (!logical.relational_statement) {
            return std::vector<PlanAlternative>{};
          }
          Plan plan = std::make_shared<RelationalPlan>(
              logical.relational_statement, logical.output_schema);
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan=std::move(plan), .local_cost=1.0, .estimated_rows=1.0}};
        },
        c::LogicalOperator::kRelational));
    built.Add(c::ImplementationRule(
        "index_scan", Scan(),
        [](c::GroupId group, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>&, const PhysicalProperties& required,
           const c::RuleContext& context) {
          return ScanAlternatives(memo, group, logical, required, context,
                                  true, false);
        },
        c::LogicalOperator::kScan));
    built.Add(c::ImplementationRule(
        "full_scan", Scan(),
        [](c::GroupId group, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>&, const PhysicalProperties& required,
           const c::RuleContext& context) {
          return ScanAlternatives(memo, group, logical, required, context,
                                  false, true);
        },
        c::LogicalOperator::kScan));
    built.Add(c::ImplementationRule(
        "selection", Selection(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (children.size() != 1 || !logical.predicate) {
            return std::vector<PlanAlternative>{};
          }
          Plan selection = std::make_shared<SelectionPlan>(
              children[0].plan, *logical.predicate,
              children[0].plan->GetStats());
          const double input = children[0].estimated_rows;
          const auto emitted = static_cast<double>(selection->EmitRowCount());
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan=std::move(selection), .local_cost=input, .estimated_rows=emitted}};
        },
        c::LogicalOperator::kSelection));
    built.Add(c::ImplementationRule(
        "projection", cascades::dsl::Projection(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (children.size() != 1) { return std::vector<PlanAlternative>{};
}
          Plan projection = std::make_shared<ProjectionPlan>(
              children[0].plan, logical.target_list);
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan=std::move(projection), .local_cost=children[0].estimated_rows,
              .estimated_rows=children[0].estimated_rows}};
        },
        c::LogicalOperator::kProjection));
    built.Add(c::ImplementationRule(
        "aggregation", cascades::dsl::Aggregation(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          // Aggregated output carries no row positions.
          if (children.size() != 1 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          Plan aggregation = std::make_shared<AggregationPlan>(
              children[0].plan, logical.target_list);
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan=std::move(aggregation), .local_cost=children[0].estimated_rows, .estimated_rows=1.0}};
        },
        c::LogicalOperator::kAggregation));
    built.Add(c::ImplementationRule(
        "limit", cascades::dsl::Limit(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext& context) {
          if (children.size() != 1) { return std::vector<PlanAlternative>{};
}
          // Soundness guard: folding LIMIT below an engine-side sort would
          // truncate before ordering, yielding wrong top-N rows. When a
          // required ordering is not delivered by the child, pass the child
          // through unchanged; the engine's LimitExecutor above its
          // SortExecutor remains responsible (D6).
          const bool needs_ordering =
              context.query != nullptr &&
              !context.query->order_expressions_.empty();
          if (needs_ordering &&
              !children[0].plan->IsOrderedBy(context.query->order_expressions_,
                                             context.query->order_ascending_)) {
            return std::vector<PlanAlternative>{
                PlanAlternative{.plan=children[0].plan, .local_cost=0,
                                .estimated_rows=children[0].estimated_rows}};
          }
          const double rows = std::min(
              children[0].estimated_rows,
              static_cast<double>(logical.limit_offset + logical.limit_count));
          Plan limit = std::make_shared<LimitPlan>(children[0].plan,
                                                   logical.limit_count,
                                                   logical.limit_offset);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan=std::move(limit), .local_cost=rows, .estimated_rows=rows}};
        },
        c::LogicalOperator::kLimit));
    built.Add(c::ImplementationRule(
        "hash_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position ||
              logical.join_kind != c::LogicalJoinKind::kInner) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate, children[0], children[1],
                                  context, true, false, false);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "semi_hash_join", Join(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position ||
              logical.join_kind != c::LogicalJoinKind::kSemi) {
            return std::vector<PlanAlternative>{};
          }
          return SemiAntiJoinAlternatives(logical.predicate, children[0],
                                          children[1],
                                          c::LogicalJoinKind::kSemi);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "anti_hash_join", Join(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position ||
              logical.join_kind != c::LogicalJoinKind::kAnti) {
            return std::vector<PlanAlternative>{};
          }
          return SemiAntiJoinAlternatives(logical.predicate, children[0],
                                          children[1],
                                          c::LogicalJoinKind::kAnti);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "index_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position ||
              logical.join_kind != c::LogicalJoinKind::kInner) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate, children[0], children[1],
                                  context, false, true, false);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "nested_loop_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position ||
              logical.join_kind != c::LogicalJoinKind::kInner) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate, children[0], children[1],
                                  context, false, false, true);
        },
        c::LogicalOperator::kJoin));
    // outer_hash_join: LEFT JOIN with hash-based NULL padding.  Builds an
    // index on the right side, probes with the left side.  Every left row
    // that matches at least one right row emits joined pairs (like inner).
    // Every unmatched left row emits left + NULL-padded right columns.
    built.Add(c::ImplementationRule(
        "outer_hash_join", OuterJoin(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          if (!logical.predicate) { return std::vector<PlanAlternative>{}; }
          std::vector<std::pair<ColumnName, ColumnName>> equalities;
          for (const Expression& conjunct :
               SplitConjuncts(*logical.predicate)) {
            if (conjunct->Type() != TypeTag::kBinaryExp ||
                conjunct->AsBinaryExpression().Op() !=
                    BinaryOperation::kEquals) {
              return std::vector<PlanAlternative>{};
            }
            const auto& binary = conjunct->AsBinaryExpression();
            if (binary.Left()->Type() != TypeTag::kColumnValue ||
                binary.Right()->Type() != TypeTag::kColumnValue) {
              return std::vector<PlanAlternative>{};
            }
            const ColumnName& lhs =
                binary.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                binary.Right()->AsColumnValue().GetColumnName();
            if (children[0].plan->GetSchema().Offset(lhs) >= 0 &&
                children[1].plan->GetSchema().Offset(rhs) >= 0) {
              equalities.emplace_back(lhs, rhs);
            } else if (children[0].plan->GetSchema().Offset(rhs) >= 0 &&
                       children[1].plan->GetSchema().Offset(lhs) >= 0) {
              equalities.emplace_back(rhs, lhs);
            } else {
              return std::vector<PlanAlternative>{};
            }
          }
          if (equalities.empty()) { return std::vector<PlanAlternative>{}; }
          std::vector<ColumnName> left_columns;
          std::vector<ColumnName> right_columns;
          for (const auto& [l, r] : equalities) {
            left_columns.push_back(l);
            right_columns.push_back(r);
          }
          const double l_rows = children[0].estimated_rows;
          const double r_rows = children[1].estimated_rows;
          // Left outer join: every left row appears at least once.
          const double estimate = std::max(l_rows, l_rows * r_rows * 0.01);
          std::vector<PlanAlternative> candidates;
          for (const HashJoinMode mode :
               {HashJoinMode::kInMemory, HashJoinMode::kHybrid}) {
            Plan join = std::make_shared<ProductPlan>(
                children[0].plan, left_columns, children[1].plan,
                right_columns, mode, JoinKind::kLeftOuter);
            double local_cost = l_rows + r_rows;
            const double build_bytes =
                r_rows * kHashJoinRowBytesEstimate;
            if (mode == HashJoinMode::kInMemory &&
                PreferHybridHashJoin(build_bytes)) {
              local_cost += r_rows * 3;
            }
            candidates.push_back(PlanAlternative{
                .plan = std::move(join),
                .local_cost = local_cost,
                .estimated_rows = estimate});
          }
          return candidates;
        },
        c::LogicalOperator::kOuterJoin));
    // right_hash_join: RIGHT JOIN with hash-based NULL padding.  Builds an
    // index on the left side, probes with the right side.  Every right row
    // that matches at least one left row emits joined pairs.  Every unmatched
    // right row emits right + NULL-padded left columns.
    built.Add(c::ImplementationRule(
        "right_hash_join", OuterJoin(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          if (!logical.predicate) { return std::vector<PlanAlternative>{}; }
          std::vector<std::pair<ColumnName, ColumnName>> equalities;
          for (const Expression& conjunct :
               SplitConjuncts(*logical.predicate)) {
            if (conjunct->Type() != TypeTag::kBinaryExp ||
                conjunct->AsBinaryExpression().Op() !=
                    BinaryOperation::kEquals) {
              return std::vector<PlanAlternative>{};
            }
            const auto& binary = conjunct->AsBinaryExpression();
            if (binary.Left()->Type() != TypeTag::kColumnValue ||
                binary.Right()->Type() != TypeTag::kColumnValue) {
              return std::vector<PlanAlternative>{};
            }
            const ColumnName& lhs =
                binary.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                binary.Right()->AsColumnValue().GetColumnName();
            if (children[0].plan->GetSchema().Offset(lhs) >= 0 &&
                children[1].plan->GetSchema().Offset(rhs) >= 0) {
              equalities.emplace_back(lhs, rhs);
            } else if (children[0].plan->GetSchema().Offset(rhs) >= 0 &&
                       children[1].plan->GetSchema().Offset(lhs) >= 0) {
              equalities.emplace_back(rhs, lhs);
            } else {
              return std::vector<PlanAlternative>{};
            }
          }
          if (equalities.empty()) { return std::vector<PlanAlternative>{}; }
          // For RIGHT JOIN, swap: the executor's left = logical's right,
          // executor's right = logical's left.  The join condition is
          // symmetric, so the equality columns map correctly.
          std::vector<ColumnName> left_columns;
          std::vector<ColumnName> right_columns;
          for (const auto& [l, r] : equalities) {
            left_columns.push_back(r);
            right_columns.push_back(l);
          }
          const double l_rows = children[0].estimated_rows;
          const double r_rows = children[1].estimated_rows;
          const double estimate = std::max(r_rows, l_rows * r_rows * 0.01);
          std::vector<PlanAlternative> candidates;
          for (const HashJoinMode mode :
               {HashJoinMode::kInMemory, HashJoinMode::kHybrid}) {
            // Swap children: executor's left = logical's right,
            // executor's right = logical's left.
            Plan join = std::make_shared<ProductPlan>(
                children[1].plan, left_columns, children[0].plan,
                right_columns, mode, JoinKind::kRightOuter);
            double local_cost = l_rows + r_rows;
            const double build_bytes =
                l_rows * kHashJoinRowBytesEstimate;
            if (mode == HashJoinMode::kInMemory &&
                PreferHybridHashJoin(build_bytes)) {
              local_cost += l_rows * 3;
            }
            candidates.push_back(PlanAlternative{
                .plan = std::move(join),
                .local_cost = local_cost,
                .estimated_rows = estimate});
          }
          return candidates;
        },
        c::LogicalOperator::kOuterJoin));
    return built;
  }();
  return rules;
}

}  // namespace tinylamb
