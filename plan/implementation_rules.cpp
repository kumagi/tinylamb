/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/implementation_rules.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregation_plan.hpp"
#include "bitmap_scan_plan.hpp"
#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "distinct_plan.hpp"
#include "empty_plan.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/hash_join_mode.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "full_scan_plan.hpp"
#include "incremental_sort_plan.hpp"
#include "index/index.hpp"
#include "index_only_scan_plan.hpp"
#include "index_scan_plan.hpp"
#include "limit_plan.hpp"
#include "max1_row_plan.hpp"
#include "merge_join_plan.hpp"
#include "minmax_index_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "projection_plan.hpp"
#include "query/query_data.hpp"
#include "selection_plan.hpp"
#include "set_operation_plan.hpp"
#include "sort_distinct_plan.hpp"
#include "sort_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "topn_plan.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "values_plan.hpp"

namespace tinylamb {
namespace {

using cascades::BestPlan;
using cascades::PhysicalProperties;
using cascades::PlanAlternative;

// True when any subexpression is a subquery: join residuals must stay
// side-effect free scalar comparisons because the merge executor evaluates
// them through the plain AST path.
bool ResidualContainsQuery(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ResidualContainsQuery(child); });
}

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
          if (!max || value < *max) {
            max = value;
          }
          max_inclusive = false;
        } else {
          if (!min || *min < value) {
            min = value;
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
          if (!max || value <= *max) {
            max = value;
          }
          max_inclusive = true;
        } else {
          if (!min || *min <= value) {
            min = value;
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

enum class JoinAlternativeKind : uint8_t {
  kInner,
  kSemi,
  kAnti,
  kLeftOuter,
  kRightOuter,
  kFullOuter,
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
  if (!predicate) {
    return 1.0;
  }
  return std::clamp(stats.EstimateSelectivity(schema, predicate), 0.0, 1.0);
}

bool ContainsAggregateExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             ContainsAggregateExpression);
}

bool CanPushLimitIntoFullScan(const Expression& filter,
                              const PhysicalProperties& required,
                              const cascades::RuleContext& context) {
  if (filter || context.query == nullptr || context.query->limit_count_ == 0 ||
      context.query->distinct_ || !context.query->order_expressions_.empty() ||
      required.limit_hint == std::numeric_limits<size_t>::max() ||
      required.limit_hint == 0) {
    return false;
  }
  // Aggregation must see every input row even when its result has a LIMIT.
  return !std::ranges::any_of(context.query->select_, [](const auto& output) {
    return ContainsAggregateExpression(output.expression);
  });
}

std::optional<std::pair<std::string, std::string>> LikePrefixBounds(
    std::string_view pattern) {
  if (pattern.size() < 2 || pattern.back() != '%') {
    return std::nullopt;
  }
  const std::string_view prefix = pattern.substr(0, pattern.size() - 1);
  if (prefix.empty() ||
      prefix.find_first_of("%_\\") != std::string_view::npos) {
    return std::nullopt;
  }
  for (const unsigned char byte : prefix) {
    // The storage comparison currently has no collation contract. Restrict
    // the rewrite to byte-stable ASCII so a locale/UTF-8 collation cannot
    // turn the computed half-open range into a different language predicate.
    if (byte >= 0x80) {
      return std::nullopt;
    }
  }
  std::string upper(prefix);
  while (!upper.empty() && static_cast<unsigned char>(upper.back()) == 0xff) {
    upper.pop_back();
  }
  if (upper.empty()) {
    return std::nullopt;
  }
  ++upper.back();
  return std::pair{std::string(prefix), std::move(upper)};
}

// Rewrites column qualifiers equal to `relation` into `physical`. Scan-level
// machinery (range extraction, executor evaluation) speaks physical table
// names while memo conjuncts speak relation identities (Phase 8).
Expression QualifyDown(
    const Expression& expression,  // NOLINT(misc-no-recursion) // AST rewrite
                                   // recursion mirrors expression tree depth;
                                   // bounded by parser depth guard.
    const std::string& relation, const std::string& physical) {
  if (!expression || relation == physical) {
    return expression;
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
  if (relation == physical) {
    return candidate;
  }
  // Positional pass-through rename; IsOrderedBy translates requests back to
  // physical names so index-provided order stays visible above the rename.
  return std::make_shared<RelationRenamePlan>(std::move(candidate), relation,
                                              physical);
}

bool SameColumnLayout(const Schema& left, const Schema& right) {
  if (left.ColumnCount() != right.ColumnCount()) {
    return false;
  }
  for (size_t i = 0; i < left.ColumnCount(); ++i) {
    if (left.GetColumn(i).Name() != right.GetColumn(i).Name()) {
      return false;
    }
  }
  return true;
}

Plan RemoveIdentityProjection(Plan child,
                              const std::vector<NamedExpression>& columns) {
  Plan projection = std::make_shared<ProjectionPlan>(child, columns);
  return SameColumnLayout(projection->GetSchema(), child->GetSchema())
             ? std::move(child)
             : std::move(projection);
}

std::vector<Expression> NormalizeOrderingForOutput(
    const std::vector<Expression>& ordering,
    const std::vector<NamedExpression>& outputs) {
  std::vector<Expression> normalized;
  normalized.reserve(ordering.size());
  for (const Expression& expression : ordering) {
    const auto found =
        std::ranges::find_if(outputs, [&](const NamedExpression& output) {
          return output.expression->ToString() == expression->ToString();
        });
    if (found != outputs.end() && !found->name.empty()) {
      normalized.push_back(ColumnValueExp(found->name));
    } else {
      normalized.push_back(expression);
    }
  }
  return normalized;
}

double LimitOutputRows(double input, size_t limit, size_t offset) {
  if (input <= static_cast<double>(offset)) {
    return 0;
  }
  const double remaining = input - static_cast<double>(offset);
  return limit == 0 ? remaining
                    : std::min(remaining, static_cast<double>(limit));
}

double LimitReadRows(double input, size_t limit, size_t offset) {
  if (limit == 0) {
    return input;
  }
  return std::min(input,
                  static_cast<double>(offset) + static_cast<double>(limit));
}

Plan BuildIndexScan(const Table& table, const Index& index,
                    const TableStatistics& statistics,
                    std::vector<Value> begin_key, std::vector<Value> end_key,
                    bool ascending, const Expression& predicate,
                    const std::vector<NamedExpression>& select,
                    bool require_row_position, bool wait_for_write_intent,
                    std::vector<ColumnName> provided_order) {
  std::unordered_set<ColumnName> touched = predicate->TouchedColumns();
  for (const NamedExpression& item : select) {
    touched.merge(item.expression->TouchedColumns());
  }
  std::unordered_set<slot_t> touched_offsets;
  for (const ColumnName& column : touched) {
    const int offset = table.GetSchema().Offset(column);
    if (offset >= 0) {
      touched_offsets.insert(static_cast<slot_t>(offset));
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
                         context.query->order_ascending_,
                         context.query->order_nulls_first_)) {
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
double EqualityPrefixRows(const TableStatistics& statistics, const Index& index,
                          const std::vector<Value>& equality_values) {
  if (statistics.Rows() == 0) {
    return 0;
  }
  double rows = static_cast<double>(statistics.Rows());
  for (size_t key = 0;
       key < equality_values.size() && key < index.sc_.key_.size(); ++key) {
    const slot_t column = index.sc_.key_[key];
    if (column >= statistics.Columns()) {
      break;
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

std::optional<std::vector<std::vector<Value>>> DisjunctiveIndexPrefixes(
    const Expression& filter, const Schema& schema, const Index& index) {
  const std::vector<Expression> disjuncts =
      relational_detail::SplitDisjuncts(filter);
  if (disjuncts.size() < 2) {
    return std::nullopt;
  }
  std::vector<std::vector<Value>> prefixes;
  prefixes.reserve(disjuncts.size());
  size_t common_size = 0;
  for (const Expression& disjunct : disjuncts) {
    std::unordered_map<slot_t, Value> equalities;
    for (const Expression& conjunct : SplitConjuncts(disjunct)) {
      if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& binary = conjunct->AsBinaryExpression();
      if (binary.Op() != BinaryOperation::kEquals) {
        continue;
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
      if (column == nullptr || constant == nullptr) {
        continue;
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
      if (value == equalities.end()) {
        break;
      }
      prefix.push_back(value->second);
    }
    if (prefix.empty()) {
      return std::nullopt;
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

std::unordered_map<slot_t, Range> ComparisonRanges(const Expression& predicate,
                                                   const Schema& schema) {
  std::unordered_map<slot_t, Range> result;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (!IsComparison(binary.Op())) {
      continue;
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
    if (column == nullptr || constant == nullptr) {
      continue;
    }
    const int offset = schema.Offset(column->GetColumnName());
    if (offset < 0) {
      continue;
    }
    result[static_cast<slot_t>(offset)].Update(binary.Op(),
                                               constant->GetValue(), direction);
  }
  return result;
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
  Expression filter = QualifyDown(memo.Get(group).filter, relation, physical);
  // Canonicalize the neutral predicate before access-path costing. Keeping a
  // literal TRUE as a non-null filter would disable safe unordered LIMIT
  // pushdown into FullScan even though it cannot reject a row.
  if (filter && filter->Type() == TypeTag::kConstantValue &&
      filter->AsConstantValue().GetValue().Truthy()) {
    filter = nullptr;
  }
  // The scan predicate handed to the executor: the group filter, or the
  // neutral `true` constant when the group has none.
  const Expression scan_predicate =
      filter ? filter : ConstantValueExp(Value(true));
  const bool no_filter =
      !filter || (filter->Type() == TypeTag::kConstantValue &&
                  filter->AsConstantValue().GetValue().Truthy());
  std::vector<NamedExpression> fallback_select;
  if (const auto found = context.scan_projections.find(relation);
      found != context.scan_projections.end()) {
    // An empty required-column list is meaningful: queries such as
    // COUNT(*) do not need heap payload columns at all. Keep it empty so a
    // covering index can be selected instead of expanding it back to every
    // table column.
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

  // A descending leading ORDER BY key is deliverable natively: the B+Tree
  // iterator positions below `end` and walks left, so the same begin/end
  // keys serve a reverse scan.  Offer a reverse twin only when the root
  // ordering actually asks for it so unordered queries keep a stable
  // candidate set (the forward twin is always emitted first).
  const bool wants_descending = [&] {
    if (required.ordering.empty() || context.query == nullptr) {
      return false;
    }
    const std::vector<bool>& ascending = context.query->order_ascending_;
    if (ascending.empty() || ascending.size() != required.ordering.size()) {
      return false;
    }
    return !ascending.front();
  }();

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
            in.child_->Type() != TypeTag::kColumnValue || in.list_.empty()) {
          continue;
        }
        bool all_constant = true;
        for (const Expression& item : in.list_) {
          if (item->Type() != TypeTag::kConstantValue) {
            all_constant = false;
            break;
          }
        }
        if (!all_constant) {
          continue;
        }
        const int offset =
            schema.Offset(in.child_->AsColumnValue().GetColumnName());
        if (offset < 0) {
          continue;
        }
        for (const Expression& item : in.list_) {
          point_sets[static_cast<slot_t>(offset)].push_back(
              item->AsConstantValue().GetValue());
        }
        continue;
      }
      if (conjunct->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& binary = conjunct->AsBinaryExpression();
      if (binary.Op() == BinaryOperation::kLike &&
          binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kConstantValue) {
        const Value& pattern = binary.Right()->AsConstantValue().GetValue();
        if (!pattern.IsNull() && pattern.type == ValueType::kVarChar) {
          if (const auto bounds =
                  LikePrefixBounds(pattern.value.varchar_value)) {
            const int offset =
                schema.Offset(binary.Left()->AsColumnValue().GetColumnName());
            if (offset >= 0) {
              Range& range = ranges[static_cast<slot_t>(offset)];
              const Value lower(std::string(bounds->first));
              const Value upper(std::string(bounds->second));
              if (!range.min || *range.min < lower) {
                range.min = lower;
                range.min_inclusive = true;
              }
              if (!range.max || upper < *range.max) {
                range.max = upper;
                range.max_inclusive = false;
              }
              range_predicates.push_back(conjunct);
            }
          }
        }
        continue;
      }
      if (!IsComparison(binary.Op())) {
        continue;
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
      if (column == nullptr || constant == nullptr) {
        continue;
      }
      const int offset = schema.Offset(column->GetColumnName());
      if (offset < 0) {
        continue;
      }
      ranges[static_cast<slot_t>(offset)].Update(
          binary.Op(), constant->GetValue(), direction);
      range_predicates.push_back(conjunct);
    }

    for (size_t index_offset = 0; index_offset < table.IndexCount();
         ++index_offset) {
      const Index& index = table.GetIndex(index_offset);

      // An unconstrained covering index is a valid access path as well.  It
      // is especially important for COUNT(*) and for ORDER BY/LIMIT shapes:
      // the index can provide the required order and the lazy executor can
      // stop after the requested prefix without touching the heap.  Do not
      // offer this fallback for filtered scans; without a selective bound it
      // would only create a tie that could displace the normal full scan.
      if (no_filter && !required.distinct) {
        std::vector<ColumnName> full_index_order;
        full_index_order.reserve(index.sc_.key_.size());
        for (const slot_t slot : index.sc_.key_) {
          full_index_order.push_back(schema.GetColumn(slot).Name());
        }
        for (const bool scan_ascending : {true, false}) {
          // The forward twin is unconditional; the reverse twin appears only
          // when the query ordering is descending, keeping unordered
          // candidate sets unchanged.
          if (!scan_ascending && !wants_descending) {
            break;
          }
          Plan unconstrained = BuildIndexScan(
              table, index, statistics, {}, {}, scan_ascending, scan_predicate,
              fallback_select, required.require_row_position,
              required.wait_for_write_intent, full_index_order);
          unconstrained = finalize(unconstrained);
          const double unconstrained_cost =
              static_cast<double>(statistics.Rows()) *
              (std::dynamic_pointer_cast<IndexOnlyScanPlan>(unconstrained)
                   ? 0.5
                   : 1.0);
          candidates.push_back(PlanAlternative{
              .plan = std::move(unconstrained),
              .local_cost = unconstrained_cost,
              .estimated_rows = static_cast<double>(statistics.Rows())});
        }
      }

      // Combine independent bitmap index scans before fetching heap rows.
      // The final predicate is still rechecked by BitmapHeapScan, so the
      // bitmap is only an access-path optimization and never a semantic
      // shortcut.  Each participating index needs its range on the leading
      // key: single-key indexes qualify directly, composite indexes when the
      // leading column carries the bound (a leading-prefix range scan is
      // well defined; deeper-key bounds need a prefix equality proof and are
      // left to the regular range scan).  At most one index per leading key
      // slot participates: two indexes sharing the leading column produce
      // near-identical bitmaps whose AND cannot beat the regular composite
      // range scan (e.g. TPC-C customer lookups hit the (w,d,id) primary
      // index, not two overlapping w-prefix bitmaps).
      auto leading_key_range =
          [&](const Index& candidate) -> const std::pair<const slot_t, Range>* {
        const auto found = ranges.find(candidate.sc_.key_.front());
        if (found == ranges.end() ||
            (!found->second.min && !found->second.max)) {
          return nullptr;
        }
        return &*found;
      };
      if (filter && leading_key_range(index) != nullptr) {
        // AND: every independent indexed predicate contributes one bitmap.
        std::vector<BitmapIndexRange> and_ranges;
        std::unordered_set<slot_t> seen_leading_slots;
        for (size_t other_offset = 0; other_offset < table.IndexCount();
             ++other_offset) {
          const Index& other = table.GetIndex(other_offset);
          const auto other_range = leading_key_range(other);
          if (other_range == nullptr ||
              !seen_leading_slots.insert(other.sc_.key_.front()).second) {
            continue;
          }
          BitmapIndexRange range;
          range.index = &other;
          if (other_range->second.min) {
            range.begin_key.push_back(*other_range->second.min);
          }
          if (other_range->second.max) {
            range.end_key.push_back(*other_range->second.max);
          }
          and_ranges.push_back(std::move(range));
        }
        if (and_ranges.size() >= 2 &&
            relational_detail::SplitDisjuncts(filter).size() == 1) {
          Plan bitmap = std::make_shared<BitmapScanPlan>(
              table, statistics, std::move(and_ranges), BitmapCombine::kAnd,
              scan_predicate, 1, 1);
          if (fallback_select.size() != bitmap->GetSchema().ColumnCount()) {
            bitmap = std::make_shared<ProjectionPlan>(bitmap, fallback_select);
          }
          bitmap = finalize(bitmap);
          candidates.push_back(PlanAlternative{.plan = std::move(bitmap),
                                               .local_cost = 0.0,
                                               .estimated_rows = 1.0});
        }
      }

      // OR: build one bitmap range per disjunct on this index.  Only
      // comparison ranges are admitted; residual rechecking handles strict
      // versus inclusive boundaries and any additional conjuncts.
      if (filter && index.sc_.key_.size() == 1) {
        const std::vector<Expression> disjuncts =
            relational_detail::SplitDisjuncts(filter);
        if (disjuncts.size() >= 2) {
          std::vector<BitmapIndexRange> or_ranges;
          for (const Expression& disjunct : disjuncts) {
            const auto disjunct_ranges = ComparisonRanges(disjunct, schema);
            const auto found = disjunct_ranges.find(index.sc_.key_.front());
            if (found == disjunct_ranges.end() ||
                (!found->second.min && !found->second.max)) {
              or_ranges.clear();
              break;
            }
            BitmapIndexRange range;
            range.index = &index;
            if (found->second.min) {
              range.begin_key.push_back(*found->second.min);
            }
            if (found->second.max) {
              range.end_key.push_back(*found->second.max);
            }
            or_ranges.push_back(std::move(range));
          }
          if (or_ranges.size() == disjuncts.size()) {
            Plan bitmap = std::make_shared<BitmapScanPlan>(
                table, statistics, std::move(or_ranges), BitmapCombine::kOr,
                scan_predicate, statistics.Rows(), statistics.Rows());
            if (fallback_select.size() != bitmap->GetSchema().ColumnCount()) {
              bitmap =
                  std::make_shared<ProjectionPlan>(bitmap, fallback_select);
            }
            bitmap = finalize(bitmap);
            candidates.push_back(PlanAlternative{
                .plan = std::move(bitmap),
                .local_cost = 0.0,
                .estimated_rows = filter_selectivity * statistics.Rows()});
          }
        }
      }

      // OR-of-conjunctions over distinct composite-key prefixes is a union
      // of disjoint index ranges.  This keeps batched OLTP DML on its indexes
      // instead of turning `(...pk...) OR (...pk...)` into a table scan.
      if (const auto prefixes =
              DisjunctiveIndexPrefixes(filter, schema, index)) {
        std::vector<std::pair<std::vector<Value>, std::vector<Value>>>
            scan_ranges;
        scan_ranges.reserve(prefixes->size());
        double access_rows = 0;
        for (const std::vector<Value>& prefix : *prefixes) {
          scan_ranges.emplace_back(prefix, prefix);
          access_rows += EqualityPrefixRows(statistics, index, prefix);
        }
        Plan disjunctive = std::make_shared<IndexScanPlan>(
            table, index, statistics, std::move(scan_ranges), true,
            scan_predicate, std::vector<ColumnName>{},
            required.require_row_position, required.wait_for_write_intent);
        if (fallback_select.size() != disjunctive->GetSchema().ColumnCount()) {
          disjunctive =
              std::make_shared<ProjectionPlan>(disjunctive, fallback_select);
        }
        disjunctive = finalize(disjunctive);
        candidates.push_back(PlanAlternative{
            .plan = std::move(disjunctive),
            .local_cost = std::max(1.0, access_rows),
            .estimated_rows = std::max(1.0, access_rows * filter_selectivity)});
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
        if (point_sets.contains(slot)) {
          break;
        }
        const auto range = ranges.find(slot);
        if (range == ranges.end() || !range->second.min || !range->second.max ||
            *range->second.min != *range->second.max) {
          point_key_offset = index.sc_.key_.size();
          break;
        }
        equality_prefix_values.push_back(*range->second.min);
        ++point_key_offset;
      }
      const bool has_point_column = point_key_offset < index.sc_.key_.size();
      const slot_t point_key_slot =
          has_point_column ? index.sc_.key_[point_key_offset] : slot_t{0};
      const auto points =
          has_point_column ? point_sets.find(point_key_slot) : point_sets.end();
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
            for (size_t key = point_key_offset + 1; key < index.sc_.key_.size();
                 ++key) {
              point_order.push_back(
                  schema.GetColumn(index.sc_.key_[key]).Name());
            }
          }
          Plan point_candidate = std::make_shared<IndexScanPlan>(
              table, index, statistics, std::move(scan_ranges), true,
              scan_predicate, std::move(point_order),
              required.require_row_position, required.wait_for_write_intent);
          if (fallback_select.size() !=
              point_candidate->GetSchema().ColumnCount()) {
            point_candidate = std::make_shared<ProjectionPlan>(point_candidate,
                                                               fallback_select);
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
          double point_rows = std::min<double>(
              static_cast<double>(statistics.Rows()),
              static_cast<double>(values.size()) *
                  std::max(1.0, static_cast<double>(statistics.Rows()) / ndv));
          point_rows = std::max(point_rows, 1.0);
          ApplyLimitHint(point_candidate, required, context, filter_selectivity,
                         &point_cost, &point_rows);
          candidates.push_back(
              PlanAlternative{.plan = std::move(point_candidate),
                              .local_cost = point_cost,
                              .estimated_rows = point_rows});
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
        if (range == ranges.end()) {
          break;
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
          // A two-sided range on the current key can carry both bounds.  The
          // residual predicate remains authoritative for inclusive/exclusive
          // edges, while the iterator avoids scanning the entire suffix.
          if (range->second.max) {
            end_key.push_back(*range->second.max);
          }
        } else if (!begin_key.empty()) {
          // Upper bound only after an equality prefix: end keeps the prefix,
          // relying on the short-end ceiling described above.
          if (range->second.max) {
            end_key.push_back(*range->second.max);
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
      if (begin_key.empty()) {
        continue;
      }

      std::vector<ColumnName> provided_order;
      for (size_t key = equality_prefix; key < index.sc_.key_.size(); ++key) {
        provided_order.push_back(schema.GetColumn(index.sc_.key_[key]).Name());
      }
      // Prefix encoding serves both directions: EncodeParts gives the verbatim
      // begin (the reverse scan's stop condition) while EncodeEndParts turns a
      // short end into a prefix ceiling (the reverse scan's start position,
      // resolved by BPlusTree::PositionBelow). The residual predicate stays
      // authoritative for inclusive/exclusive edges in either direction.
      const bool reverse_twin = wants_descending && !provided_order.empty();
      for (const bool scan_ascending : {true, false}) {
        if (!scan_ascending && !reverse_twin) {
          break;
        }
        Plan candidate = BuildIndexScan(
            table, index, statistics, begin_key, end_key, scan_ascending,
            scan_predicate, fallback_select, required.require_row_position,
            required.wait_for_write_intent, provided_order);
        // The scan filter is applied either by the scan itself or, when its
        // columns are not covered by the index key, by a Selection on top.
        // Only equality-pinned slots are provably exact in the scan output;
        // range bounds (and the short-end ceiling) can leak boundary rows, so
        // any predicate touching them is re-checked by a Selection.
        const bool covered =
            !filter ||
            std::ranges::all_of(
                filter->TouchedColumns(), [&](const ColumnName& column) {
                  const int offset = schema.Offset(column);
                  return offset >= 0 &&
                         equality_slots.contains(static_cast<slot_t>(offset));
                });
        if (!covered) {
          candidate =
              std::make_shared<SelectionPlan>(candidate, filter, statistics);
        }
        if (fallback_select.size() != candidate->GetSchema().ColumnCount()) {
          candidate =
              std::make_shared<ProjectionPlan>(candidate, fallback_select);
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
        candidates.push_back(PlanAlternative{.plan = std::move(candidate),
                                             .local_cost = local_cost,
                                             .estimated_rows = estimated_rows});
      }
    }
  }

  if (include_full_scan) {
    const size_t scan_limit =
        CanPushLimitIntoFullScan(filter, required, context)
            ? required.limit_hint
            : std::numeric_limits<size_t>::max();
    // Extract PeekCompare predicates from the filter so the scan iterator
    // can reject rows at the raw-byte level before full deserialization.
    std::vector<IntegerPeekCompare> scan_peeks;
    if (filter) {
      const std::vector<Expression> conjuncts = SplitConjuncts(filter);
      for (const Expression& conjunct : conjuncts) {
        if (auto simple =
                relational_detail::TryCompileSimpleCompare(conjunct, schema)) {
          if (simple->int_payload) {
            IntegerPeekCompare peek;
            peek.column = simple->column;
            peek.op = simple->op;
            peek.constant = simple->int_constant;
            scan_peeks.push_back(peek);
          }
        }
      }
    }
    Plan full_scan =
        scan_peeks.empty()
            ? static_cast<Plan>(
                  std::make_shared<FullScanPlan>(table, statistics, scan_limit))
            : static_cast<Plan>(std::make_shared<FullScanPlan>(
                  table, statistics, std::move(scan_peeks), scan_limit));
    if (filter) {
      full_scan =
          std::make_shared<SelectionPlan>(full_scan, filter, statistics);
    }
    if (fallback_select.size() != full_scan->GetSchema().ColumnCount()) {
      full_scan = std::make_shared<ProjectionPlan>(full_scan, fallback_select);
    }
    full_scan = finalize(full_scan);
    auto local_cost = static_cast<double>(full_scan->AccessRowCount());
    // access_method hint (Phase 5): a soft preference against full scans.
    if (required.access_method == cascades::AccessMethod::kPreferIndex &&
        table.IndexCount() > 0) {
      local_cost *= 2.0;
    }
    auto estimated_rows = static_cast<double>(full_scan->EmitRowCount());
    ApplyLimitHint(full_scan, required, context, filter_selectivity,
                   &local_cost, &estimated_rows);
    candidates.push_back(PlanAlternative{.plan = std::move(full_scan),
                                         .local_cost = local_cost,
                                         .estimated_rows = estimated_rows});
  }
  return candidates;
}

// D3 join cardinality: |L|*|R| / max NDV of the equality keys, capped by the
// cross-product size; falls back to the cross product when stats are missing.
double JoinCardinality(
    const BestPlan& left, const BestPlan& right,
    const std::vector<std::pair<ColumnName, ColumnName>>& equalities) {
  double max_ndv = 0;
  for (const auto& [left_column, right_column] : equalities) {
    const int lo = left.plan->GetSchema().Offset(left_column);
    const int ro = right.plan->GetSchema().Offset(right_column);
    double ndv = 0;
    if (lo >= 0 && static_cast<size_t>(lo) < left.plan->GetStats().Columns()) {
      ndv = std::max<double>(ndv, left.plan->GetStats().Column(lo).Distinct());
    }
    if (ro >= 0 && static_cast<size_t>(ro) < right.plan->GetStats().Columns()) {
      ndv = std::max<double>(ndv, right.plan->GetStats().Column(ro).Distinct());
    }
    max_ndv = std::max(max_ndv, ndv);
  }
  const double cross = left.estimated_rows * right.estimated_rows;
  if (max_ndv < 1) {
    return cross;
  }
  return std::min(cross, cross / max_ndv);
}

std::vector<PlanAlternative> JoinAlternatives(
    const cascades::Memo& memo, cascades::GroupId right_group,
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, const cascades::RuleContext& context, bool hash,
    bool index, bool cross,
    JoinAlternativeKind kind = JoinAlternativeKind::kInner) {
  const Expression predicate = condition ? *condition : nullptr;
  std::vector<std::pair<ColumnName, ColumnName>> equalities;
  std::vector<Expression> equi_conjuncts;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
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
  const Expression residual = residual_conjuncts.empty()
                                  ? nullptr
                                  : CombineConjuncts(residual_conjuncts);

  // A semi/anti hash join emits only the probe side. A residual predicate
  // mentioning the build side cannot be evaluated after that reduction, so
  // leave such shapes for the existing relational fallback until a
  // residual-aware mark join is available.
  if (kind != JoinAlternativeKind::kInner && residual) {
    return {};
  }

  const double l_rows = left.estimated_rows;
  const double r_rows = right.estimated_rows;
  const double equi_estimate = JoinCardinality(left, right, equalities);

  // Wraps the plan in a Selection applying the residual conjuncts (Phase 4:
  // the root SelectionPlan no longer re-applies them).
  const auto with_residual = [&](Plan plan, double estimate) {
    double est = estimate;
    if (residual) {
      plan = std::make_shared<SelectionPlan>(plan, residual, plan->GetStats());
      est = std::min(est, static_cast<double>(plan->EmitRowCount()));
    }
    return std::pair<Plan, double>{std::move(plan), est};
  };

  std::vector<PlanAlternative> candidates;
  if (cross) {
    Plan product = std::make_shared<ProductPlan>(left.plan, right.plan);
    const double cross_estimate =
        equi_conjuncts.empty() ? l_rows * r_rows : equi_estimate;
    // The cross-product executor does not consume join keys.  A nested-loop
    // implementation must therefore evaluate the complete join predicate,
    // including equality conjuncts; `with_residual` is only valid for hash
    // and index joins that already enforce those equalities themselves.
    if (predicate) {
      product = std::make_shared<SelectionPlan>(product, predicate,
                                                product->GetStats());
    }
    const double local_cost = (l_rows * r_rows) + l_rows + r_rows;
    candidates.push_back(PlanAlternative{.plan = std::move(product),
                                         .local_cost = local_cost,
                                         .estimated_rows = cross_estimate});
  }
  if (equi_conjuncts.empty()) {
    return candidates;
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
      Plan join;
      if (kind == JoinAlternativeKind::kInner) {
        join = std::make_shared<ProductPlan>(left.plan, left_columns,
                                             right.plan, right_columns, mode);
      } else {
        JoinKind physical_kind = AntiJoinKind();
        if (kind == JoinAlternativeKind::kSemi) {
          physical_kind = SemiJoinKind();
        } else if (kind == JoinAlternativeKind::kLeftOuter) {
          physical_kind = LeftOuterJoinKind();
        } else if (kind == JoinAlternativeKind::kRightOuter) {
          physical_kind = RightOuterJoinKind();
        } else if (kind == JoinAlternativeKind::kFullOuter) {
          physical_kind = FullOuterJoinKind();
        }
        join =
            std::make_shared<ProductPlan>(left.plan, left_columns, right.plan,
                                          right_columns, mode, physical_kind);
      }
      double local_cost = l_rows + r_rows;
      const double build_bytes = r_rows * kHashJoinRowBytesEstimate;
      if (mode == HashJoinMode::kInMemory &&
          PreferHybridHashJoin(build_bytes)) {
        local_cost += r_rows * 3;
      }
      const double estimate =
          kind == JoinAlternativeKind::kAnti
              ? l_rows
              : (kind == JoinAlternativeKind::kLeftOuter ||
                         kind == JoinAlternativeKind::kRightOuter ||
                         kind == JoinAlternativeKind::kFullOuter
                     ? std::max(l_rows, r_rows)
                     : std::min(l_rows, equi_estimate));
      auto [plan, est] = with_residual(std::move(join), estimate);
      candidates.push_back(PlanAlternative{.plan = std::move(plan),
                                           .local_cost = local_cost,
                                           .estimated_rows = est});
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
        const auto stats_it = right_relation.empty()
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
            renamed_columns.emplace_back(ColumnName(
                right_relation, physical_schema.GetColumn(i).Name().name));
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
                candidates.push_back(PlanAlternative{.plan = std::move(plan),
                                                     .local_cost = local_cost,
                                                     .estimated_rows = est});
              }
            }
          }
        }
      }
    }
  }
  return candidates;
}

// A merge join is only sound when both children deliver the equality keys in
// the same ascending order.  Existing ordering is reused; an unordered child
// is made sound by inserting a local SortPlan below the merge join.  The
// alternative's cost includes that sort, so a cheaper hash/index alternative
// can still win the Cascades search.
std::vector<PlanAlternative> MergeJoinAlternative(
    const cascades::Memo& memo, cascades::GroupId right_group,
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, const cascades::RuleContext& context,
    JoinAlternativeKind kind = JoinAlternativeKind::kInner) {
  const Expression predicate = condition ? *condition : nullptr;
  std::vector<ColumnName> left_columns;
  std::vector<ColumnName> right_columns;
  std::vector<Expression> equality_expressions;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
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
      left_columns.push_back(lhs);
      right_columns.push_back(rhs);
    } else if (left.plan->GetSchema().Offset(rhs) >= 0 &&
               right.plan->GetSchema().Offset(lhs) >= 0) {
      left_columns.push_back(rhs);
      right_columns.push_back(lhs);
    } else {
      continue;
    }
    equality_expressions.push_back(conjunct);
  }
  if (equality_expressions.empty()) {
    return {};
  }
  std::unordered_set<std::string> equalities;
  for (const Expression& expression : equality_expressions) {
    equalities.insert(expression->ToString());
  }
  std::vector<Expression> residual_conjuncts;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (!equalities.contains(conjunct->ToString())) {
      residual_conjuncts.push_back(conjunct);
    }
  }
  // Non-inner merge joins carry the residual inside the plan node so the
  // executor can apply it while pairing (outer NULL-padding and semi/anti
  // matching respect it). Inner keeps the plain merge + Selection shape.
  Expression merge_residual;
  if (kind != JoinAlternativeKind::kInner && !residual_conjuncts.empty()) {
    if (std::ranges::any_of(residual_conjuncts, [](const Expression& conjunct) {
          return ResidualContainsQuery(conjunct);
        })) {
      return {};
    }
    merge_residual = CombineConjuncts(residual_conjuncts);
    residual_conjuncts.clear();
  }
  std::vector<Expression> ordering_left;
  std::vector<Expression> ordering_right;
  ordering_left.reserve(left_columns.size());
  ordering_right.reserve(right_columns.size());
  for (const ColumnName& column : left_columns) {
    ordering_left.push_back(ColumnValueExp(column));
  }
  for (const ColumnName& column : right_columns) {
    ordering_right.push_back(ColumnValueExp(column));
  }
  const std::vector<bool> ascending(left_columns.size(), true);
  const bool left_ordered = left.plan->IsOrderedBy(ordering_left, ascending);
  const bool right_ordered = right.plan->IsOrderedBy(ordering_right, ascending);

  const auto sort_cost = [](double rows) {
    return rows <= 1 ? rows : rows * std::log2(rows);
  };
  Plan left_plan = left.plan;
  Plan right_plan = right.plan;
  double local_cost = left.estimated_rows + right.estimated_rows;
  if (!left_ordered) {
    std::vector<SortKey> keys;
    keys.reserve(left_columns.size());
    for (const ColumnName& column : left_columns) {
      keys.push_back(SortKey{ColumnValueExp(column), true, std::nullopt});
    }
    left_plan =
        std::make_shared<SortPlan>(std::move(left_plan), std::move(keys));
    local_cost += sort_cost(left.estimated_rows);
  }
  if (!right_ordered) {
    std::vector<SortKey> keys;
    keys.reserve(right_columns.size());
    for (const ColumnName& column : right_columns) {
      keys.push_back(SortKey{ColumnValueExp(column), true, std::nullopt});
    }
    right_plan =
        std::make_shared<SortPlan>(std::move(right_plan), std::move(keys));
    local_cost += sort_cost(right.estimated_rows);
  }
  JoinKind physical_kind = JoinKind{};
  if (kind == JoinAlternativeKind::kSemi) {
    physical_kind = SemiJoinKind();
  } else if (kind == JoinAlternativeKind::kAnti) {
    physical_kind = AntiJoinKind();
  } else if (kind == JoinAlternativeKind::kLeftOuter) {
    physical_kind = LeftOuterJoinKind();
  } else if (kind == JoinAlternativeKind::kRightOuter) {
    physical_kind = RightOuterJoinKind();
  } else if (kind == JoinAlternativeKind::kFullOuter) {
    physical_kind = FullOuterJoinKind();
  }
  Plan merge = std::make_shared<MergeJoinPlan>(
      std::move(left_plan), left_columns, std::move(right_plan), right_columns,
      physical_kind, std::move(merge_residual));
  double estimated_rows = JoinCardinality(left, right, [&] {
    std::vector<std::pair<ColumnName, ColumnName>> pairs;
    pairs.reserve(left_columns.size());
    for (size_t i = 0; i < left_columns.size(); ++i) {
      pairs.emplace_back(left_columns[i], right_columns[i]);
    }
    return pairs;
  }());
  if (!residual_conjuncts.empty()) {
    const TableStatistics merge_stats = merge->GetStats();
    merge = std::make_shared<SelectionPlan>(
        std::move(merge), CombineConjuncts(residual_conjuncts), merge_stats);
    estimated_rows =
        std::min(estimated_rows, static_cast<double>(merge->EmitRowCount()));
  }
  if (kind == JoinAlternativeKind::kAnti) {
    estimated_rows = left.estimated_rows;
  } else if (kind == JoinAlternativeKind::kSemi) {
    estimated_rows = std::min(left.estimated_rows, estimated_rows);
  }
  (void)memo;
  (void)right_group;
  (void)context;
  return {PlanAlternative{.plan = std::move(merge),
                          .local_cost = local_cost,
                          .estimated_rows = estimated_rows}};
}

}  // namespace

StatusOr<Plan> OptimizeSingleRelation(
    const QueryData& query, const Expression& predicate,
    const std::vector<NamedExpression>& projection_items, bool has_aggregate,
    bool distinct, const cascades::PhysicalProperties& required,
    const cascades::RuleContext& context) {
  if (query.from_.size() != 1) {
    return Status::kNotImplemented;
  }

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
  if (alternatives.empty()) {
    return Status::kNotImplemented;
  }

  // A scalar MIN/MAX of a non-null single-column key can be answered by the
  // first entry of that key's ordered index.  This is both cheaper and more
  // precise than scanning the relation and feeding every row to an
  // aggregate.  Restrict the rewrite to a NOT NULL/PRIMARY KEY column: a
  // general nullable index needs a separate null-position contract and must
  // retain the ordinary aggregate path.
  if (has_aggregate && !distinct && query.from_.size() == 1 &&
      projection_items.size() == 1 &&
      (!predicate || (predicate->Type() == TypeTag::kConstantValue &&
                      predicate->AsConstantValue().GetValue().Truthy()))) {
    const NamedExpression& output = projection_items.front();
    if (output.expression &&
        output.expression->Type() == TypeTag::kAggregateExp) {
      const auto& aggregate = output.expression->AsAggregateExpression();
      const bool is_min = aggregate.GetType() == AggregationType::kMin;
      const bool is_max = aggregate.GetType() == AggregationType::kMax;
      const Expression& child = aggregate.Child();
      if ((is_min || is_max) && !aggregate.Distinct() &&
          !aggregate.NeedsGroupContext() && child &&
          child->Type() == TypeTag::kColumnValue) {
        const ColumnName target = child->AsColumnValue().GetColumnName();
        const Table& table = *context.tables.at(relation);
        const Schema& schema = table.GetSchema();
        const int target_slot = schema.Offset(target);
        if (target_slot >= 0 &&
            schema.GetColumn(static_cast<size_t>(target_slot))
                    .GetConstraint()
                    .ctype != Constraint::kNothing) {
          const auto& column_constraint =
              schema.GetColumn(static_cast<size_t>(target_slot))
                  .GetConstraint();
          const bool non_null =
              column_constraint.ctype == Constraint::kNotNull ||
              column_constraint.ctype == Constraint::kPrimaryKey;
          if (non_null) {
            const TableStatistics& statistics =
                *context.statistics.at(relation);
            for (size_t index_offset = 0; index_offset < table.IndexCount();
                 ++index_offset) {
              const Index& index = table.GetIndex(index_offset);
              if (index.sc_.key_.size() != 1 ||
                  index.sc_.key_.front() != static_cast<slot_t>(target_slot) ||
                  index.RetainsDeletedEntries()) {
                continue;
              }
              const ColumnName provided = schema.GetColumn(target_slot).Name();
              Plan scan = std::make_shared<IndexOnlyScanPlan>(
                  table, index, statistics, std::vector<Value>{},
                  std::vector<Value>{}, is_min, ConstantValueExp(Value(true)),
                  std::vector<ColumnName>{provided});
              Plan minmax = std::make_shared<MinMaxIndexPlan>(
                  std::move(scan), output, 0, is_max);
              return minmax;
            }
          }
        }
      }
    }
  }
  auto ordered = [&](const PlanAlternative& alternative) {
    return query.order_expressions_.empty() ||
           alternative.plan->IsOrderedBy(query.order_expressions_,
                                         query.order_ascending_,
                                         query.order_nulls_first_);
  };
  const bool ordered_candidate = std::ranges::any_of(alternatives, ordered);
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
    if (required.require_row_position) {
      return Status::kNotImplemented;
    }
    plan = std::make_shared<HashAggregatePlan>(plan, projection_items);
  } else {
    plan = RemoveIdentityProjection(std::move(plan), projection_items);
  }

  if (distinct) {
    plan = std::make_shared<DistinctPlan>(std::move(plan));
  }

  const std::vector<Expression> sort_expressions =
      NormalizeOrderingForOutput(query.order_expressions_, projection_items);
  // NormalizeOrderingForOutput preserves key positions, so null placement
  // slices positionally alongside the normalized keys.
  auto slice_nulls = [&](size_t count) {
    std::vector<std::optional<bool>> sliced;
    sliced.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      sliced.push_back(i < query.order_nulls_first_.size()
                           ? query.order_nulls_first_[i]
                           : std::nullopt);
    }
    return sliced;
  };
  bool ordered_plan =
      sort_expressions.empty() ||
      plan->IsOrderedBy(sort_expressions, query.order_ascending_,
                        slice_nulls(sort_expressions.size()));
  if (!ordered_plan) {
    // If the child already arrives in the leading ORDER BY prefix, sort only
    // each contiguous prefix group. This preserves the requested global order
    // while avoiding a relation-wide sort for clustered/covering indexes.
    size_t prefix_length = 0;
    for (size_t length = sort_expressions.size(); length > 1; --length) {
      std::vector<Expression> prefix_expressions(
          sort_expressions.begin(), sort_expressions.begin() + length - 1);
      std::vector<bool> prefix_ascending(
          query.order_ascending_.begin(),
          query.order_ascending_.begin() + length - 1);
      if (plan->IsOrderedBy(prefix_expressions, prefix_ascending,
                            slice_nulls(length - 1))) {
        prefix_length = length - 1;
        break;
      }
    }
    if (prefix_length != 0) {
      std::vector<SortKey> prefix_keys;
      std::vector<SortKey> suffix_keys;
      prefix_keys.reserve(prefix_length);
      suffix_keys.reserve(sort_expressions.size() - prefix_length);
      for (size_t i = 0; i < sort_expressions.size(); ++i) {
        SortKey key{sort_expressions[i], query.order_ascending_[i],
                    i < query.order_nulls_first_.size()
                        ? query.order_nulls_first_[i]
                        : std::nullopt};
        (i < prefix_length ? prefix_keys : suffix_keys)
            .push_back(std::move(key));
      }
      plan = std::make_shared<IncrementalSortPlan>(
          std::move(plan), std::move(prefix_keys), std::move(suffix_keys));
      ordered_plan = true;
    }
  }
  if (!ordered_plan) {
    if (query.limit_count_ != 0 && !sort_expressions.empty()) {
      std::vector<TopNKey> keys;
      keys.reserve(sort_expressions.size());
      for (size_t i = 0; i < sort_expressions.size(); ++i) {
        keys.push_back(TopNKey{sort_expressions[i], query.order_ascending_[i],
                               i < query.order_nulls_first_.size()
                                   ? query.order_nulls_first_[i]
                                   : std::nullopt});
      }
      plan =
          std::make_shared<TopNPlan>(std::move(plan), std::move(keys),
                                     query.limit_count_, query.limit_offset_);
    } else {
      std::vector<SortKey> keys;
      keys.reserve(sort_expressions.size());
      for (size_t i = 0; i < sort_expressions.size(); ++i) {
        keys.push_back(SortKey{sort_expressions[i], query.order_ascending_[i],
                               i < query.order_nulls_first_.size()
                                   ? query.order_nulls_first_[i]
                                   : std::nullopt});
      }
      plan = std::make_shared<SortPlan>(std::move(plan), std::move(keys));
    }
  }

  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    const bool needs_ordering = !sort_expressions.empty();
    const bool topn = std::dynamic_pointer_cast<TopNPlan>(plan) != nullptr;
    if (!topn && (!needs_ordering ||
                  plan->IsOrderedBy(sort_expressions, query.order_ascending_,
                                    slice_nulls(sort_expressions.size())))) {
      plan = std::make_shared<LimitPlan>(std::move(plan), query.limit_count_,
                                         query.limit_offset_);
    }
  }
  return plan;
}

const cascades::ImplementationRuleSet& DefaultImplementationRules() {
  using cascades::dsl::Any;
  using cascades::dsl::Join;
  using cascades::dsl::OuterJoin;
  using cascades::dsl::Scan;
  using cascades::dsl::Selection;
  namespace c = cascades;
  static const c::ImplementationRuleSet rules = [] {
    c::ImplementationRuleSet built;
    built.Add(c::ImplementationRule(
        "relational_ir", c::Pattern::Op(c::LogicalOperator::kRelational, {}),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical, const std::vector<BestPlan>&,
           const PhysicalProperties&, const c::RuleContext&) {
          if (!logical.relational_statement) {
            return std::vector<PlanAlternative>{};
          }
          Plan plan = std::make_shared<RelationalPlan>(
              logical.relational_statement, logical.output_schema);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(plan),
                              .local_cost = 1.0,
                              .estimated_rows = 1.0}};
        },
        c::LogicalOperator::kRelational));
    built.Add(c::ImplementationRule(
        "dummy_scan", c::dsl::DummyScan(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression&, const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (!children.empty()) {
            return std::vector<PlanAlternative>{};
          }
          Plan plan = std::make_shared<DummyScanPlan>();
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(plan), .local_cost = 0, .estimated_rows = 1}};
        },
        c::LogicalOperator::kDummyScan));
    built.Add(c::ImplementationRule(
        "values", c::dsl::Values(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (!children.empty()) {
            return std::vector<PlanAlternative>{};
          }
          Plan plan = std::make_shared<ValuesPlan>(logical.output_schema,
                                                   logical.values);
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(plan),
              .local_cost = static_cast<double>(logical.values.size()),
              .estimated_rows = static_cast<double>(logical.values.size())}};
        },
        c::LogicalOperator::kValues));
    built.Add(c::ImplementationRule(
        "index_scan", Scan(),
        [](c::GroupId group, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical, const std::vector<BestPlan>&,
           const PhysicalProperties& required, const c::RuleContext& context) {
          return ScanAlternatives(memo, group, logical, required, context, true,
                                  false);
        },
        c::LogicalOperator::kScan));
    built.Add(c::ImplementationRule(
        "full_scan", Scan(),
        [](c::GroupId group, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical, const std::vector<BestPlan>&,
           const PhysicalProperties& required, const c::RuleContext& context) {
          return ScanAlternatives(memo, group, logical, required, context,
                                  false, true);
        },
        c::LogicalOperator::kScan));
    built.Add(c::ImplementationRule(
        "selection", Selection(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1 || !logical.predicate) {
            return std::vector<PlanAlternative>{};
          }
          if ((*logical.predicate)->Type() == TypeTag::kConstantValue) {
            const Value value =
                (*logical.predicate)->AsConstantValue().GetValue();
            if (value.IsNull() || !value.Truthy()) {
              Plan empty = std::make_shared<EmptyPlan>(children[0].plan);
              return std::vector<PlanAlternative>{
                  PlanAlternative{.plan = std::move(empty),
                                  .local_cost = 0,
                                  .estimated_rows = 0}};
            }
          }
          Plan selection = std::make_shared<SelectionPlan>(
              children[0].plan, *logical.predicate,
              children[0].plan->GetStats());
          const double input = children[0].estimated_rows;
          const auto emitted = static_cast<double>(selection->EmitRowCount());
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(selection),
                              .local_cost = input,
                              .estimated_rows = emitted}};
        },
        c::LogicalOperator::kSelection));
    built.Add(c::ImplementationRule(
        "projection", cascades::dsl::Projection(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          Plan projection =
              RemoveIdentityProjection(children[0].plan, logical.target_list);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(projection),
                              .local_cost = children[0].estimated_rows,
                              .estimated_rows = children[0].estimated_rows}};
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
          const double rows = children[0].estimated_rows;
          Plan hash = std::make_shared<HashAggregatePlan>(children[0].plan,
                                                          logical.target_list);
          Plan sort = std::make_shared<SortAggregatePlan>(children[0].plan,
                                                          logical.target_list);
          // Scalar aggregation has one group, but keep both physical
          // alternatives in the memo so a future grouping-key payload can
          // reuse the same cost boundary. Hash is the cheap default; SortAgg
          // remains selectable by rule disabling/hints and is costed as a
          // materializing sort plus the accumulator pass.
          const double sort_cost =
              rows <= 1.0 ? rows : rows * std::log2(rows) + rows;
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(hash),
                              .local_cost = rows,
                              .estimated_rows = 1.0},
              PlanAlternative{.plan = std::move(sort),
                              .local_cost = sort_cost,
                              .estimated_rows = 1.0}};
        },
        c::LogicalOperator::kAggregation));
    built.Add(c::ImplementationRule(
        "sort", cascades::dsl::Sort(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1 ||
              logical.target_list.size() != logical.sort_ascending.size()) {
            return std::vector<PlanAlternative>{};
          }
          std::vector<SortKey> keys;
          std::vector<Expression> expressions;
          keys.reserve(logical.target_list.size());
          expressions.reserve(logical.target_list.size());
          for (size_t i = 0; i < logical.target_list.size(); ++i) {
            expressions.push_back(logical.target_list[i].expression);
            keys.push_back(SortKey{logical.target_list[i].expression,
                                   logical.sort_ascending[i],
                                   i < logical.sort_nulls_first.size()
                                       ? logical.sort_nulls_first[i]
                                       : std::nullopt});
          }
          if (children[0].plan->IsOrderedBy(expressions,
                                            logical.sort_ascending)) {
            return std::vector<PlanAlternative>{
                PlanAlternative{.plan = children[0].plan,
                                .local_cost = 0,
                                .estimated_rows = children[0].estimated_rows}};
          }
          Plan sort =
              std::make_shared<SortPlan>(children[0].plan, std::move(keys));
          const double rows = children[0].estimated_rows;
          const double cost = rows <= 1 ? rows : rows * std::log2(rows);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(sort),
                              .local_cost = cost,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kSort));
    built.Add(c::ImplementationRule(
        "topn", cascades::dsl::TopN(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1 ||
              logical.target_list.size() != logical.sort_ascending.size() ||
              logical.limit_count == 0) {
            return std::vector<PlanAlternative>{};
          }
          std::vector<TopNKey> keys;
          keys.reserve(logical.target_list.size());
          for (size_t i = 0; i < logical.target_list.size(); ++i) {
            keys.push_back(TopNKey{logical.target_list[i].expression,
                                   logical.sort_ascending[i],
                                   i < logical.sort_nulls_first.size()
                                       ? logical.sort_nulls_first[i]
                                       : std::nullopt});
          }
          Plan topn = std::make_shared<TopNPlan>(
              children[0].plan, std::move(keys), logical.limit_count,
              logical.limit_offset);
          const double rows = children[0].estimated_rows;
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(topn),
              .local_cost = rows <= 1 ? rows : rows * std::log2(rows),
              .estimated_rows = static_cast<double>(LimitOutputRows(
                  rows, logical.limit_count, logical.limit_offset))}};
        },
        c::LogicalOperator::kTopN));
    built.Add(c::ImplementationRule(
        "empty", cascades::dsl::Empty(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression&, const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          Plan empty = std::make_shared<EmptyPlan>(children[0].plan);
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(empty), .local_cost = 0, .estimated_rows = 0}};
        },
        c::LogicalOperator::kEmpty));
    built.Add(c::ImplementationRule(
        "distinct", cascades::dsl::Distinct(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression&, const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          Plan distinct = std::make_shared<DistinctPlan>(children[0].plan);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(distinct),
                              .local_cost = children[0].estimated_rows,
                              .estimated_rows = children[0].estimated_rows}};
        },
        c::LogicalOperator::kDistinct));
    built.Add(c::ImplementationRule(
        "sort_distinct", cascades::dsl::Distinct(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression&, const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          const Schema& schema = children[0].plan->GetSchema();
          std::vector<Expression> ordering;
          std::vector<SortKey> keys;
          ordering.reserve(schema.ColumnCount());
          keys.reserve(schema.ColumnCount());
          for (size_t i = 0; i < schema.ColumnCount(); ++i) {
            const ColumnName column = schema.GetColumn(i).Name();
            ordering.push_back(ColumnValueExp(column));
            keys.push_back(SortKey{ColumnValueExp(column), true, std::nullopt});
          }
          Plan input = children[0].plan;
          double cost = children[0].estimated_rows;
          const std::vector<bool> ascending(ordering.size(), true);
          if (!input->IsOrderedBy(ordering, ascending)) {
            const double rows = children[0].estimated_rows;
            cost += rows <= 1 ? rows : rows * std::log2(rows);
            input =
                std::make_shared<SortPlan>(std::move(input), std::move(keys));
          }
          Plan distinct = std::make_shared<SortDistinctPlan>(std::move(input));
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(distinct),
                              .local_cost = cost,
                              .estimated_rows = children[0].estimated_rows}};
        },
        c::LogicalOperator::kDistinct));
    built.Add(c::ImplementationRule(
        "max1_row", cascades::dsl::Max1Row(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression&, const std::vector<BestPlan>& children,
           const PhysicalProperties&, const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          Plan max1 = std::make_shared<Max1RowPlan>(children[0].plan);
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(max1),
              .local_cost = children[0].estimated_rows,
              .estimated_rows = std::min(1.0, children[0].estimated_rows)}};
        },
        c::LogicalOperator::kMax1Row));
    const auto implement_set_operation =
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() < 2) {
            return std::vector<PlanAlternative>{};
          }
          SetOperationKind operation;
          switch (logical.operation) {
            case c::LogicalOperator::kUnion:
              operation = SetOperationKind::kUnion;
              break;
            case c::LogicalOperator::kUnionAll:
              operation = SetOperationKind::kUnionAll;
              break;
            case c::LogicalOperator::kIntersect:
              operation = SetOperationKind::kIntersect;
              break;
            case c::LogicalOperator::kIntersectAll:
              operation = SetOperationKind::kIntersectAll;
              break;
            case c::LogicalOperator::kExcept:
              operation = SetOperationKind::kExcept;
              break;
            case c::LogicalOperator::kExceptAll:
              operation = SetOperationKind::kExceptAll;
              break;
            default:
              return std::vector<PlanAlternative>{};
          }
          std::vector<Plan> plans;
          plans.reserve(children.size());
          double input_rows = 0;
          std::vector<SortKey> order_keys;
          std::vector<Expression> order_expressions;
          std::vector<bool> ascending;
          if (operation == SetOperationKind::kUnionAll &&
              !required.ordering.empty()) {
            order_expressions.reserve(required.ordering.size());
            ascending.assign(required.ordering.size(), true);
            for (const ColumnName& column : required.ordering) {
              order_expressions.push_back(ColumnValueExp(column));
            }
            if (context.query != nullptr &&
                context.query->order_ascending_.size() == ascending.size()) {
              ascending = context.query->order_ascending_;
            }
            for (size_t i = 0; i < order_expressions.size(); ++i) {
              order_keys.push_back(
                  SortKey{order_expressions[i], ascending[i], std::nullopt});
            }
          }
          for (const BestPlan& child : children) {
            Plan plan = child.plan;
            if (!order_expressions.empty() &&
                !plan->IsOrderedBy(order_expressions, ascending)) {
              std::vector<SortKey> child_keys;
              child_keys.reserve(order_keys.size());
              for (const SortKey& key : order_keys) {
                child_keys.push_back(key);
              }
              plan = std::make_shared<SortPlan>(std::move(plan),
                                                std::move(child_keys));
            }
            plans.push_back(std::move(plan));
            input_rows += child.estimated_rows;
          }
          Plan set_operation = std::make_shared<SetOperationPlan>(
              std::move(plans), operation, std::move(order_keys));
          const double output_rows = operation == SetOperationKind::kUnionAll
                                         ? input_rows
                                         : children.front().estimated_rows;
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(set_operation),
                              .local_cost = input_rows,
                              .estimated_rows = output_rows}};
        };
    built.Add(c::ImplementationRule("union", cascades::dsl::Union(),
                                    implement_set_operation,
                                    c::LogicalOperator::kUnion));
    built.Add(c::ImplementationRule("union_all", cascades::dsl::UnionAll(),
                                    implement_set_operation,
                                    c::LogicalOperator::kUnionAll));
    built.Add(c::ImplementationRule("intersect", cascades::dsl::Intersect(),
                                    implement_set_operation,
                                    c::LogicalOperator::kIntersect));
    built.Add(c::ImplementationRule(
        "intersect_all", cascades::dsl::IntersectAll(), implement_set_operation,
        c::LogicalOperator::kIntersectAll));
    built.Add(c::ImplementationRule("except", cascades::dsl::Except(),
                                    implement_set_operation,
                                    c::LogicalOperator::kExcept));
    built.Add(c::ImplementationRule("except_all", cascades::dsl::ExceptAll(),
                                    implement_set_operation,
                                    c::LogicalOperator::kExceptAll));
    built.Add(c::ImplementationRule(
        "limit", cascades::dsl::Limit(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext& context) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          // Soundness guard: folding LIMIT below an engine-side sort would
          // truncate before ordering, yielding wrong top-N rows. When a
          // required ordering is not delivered by the child, pass the child
          // through unchanged; the engine's LimitExecutor above its
          // SortExecutor remains responsible (D6).
          const bool needs_ordering =
              context.query != nullptr &&
              !context.query->order_expressions_.empty();
          if (logical.limit_count != 0) {
            if (const auto sort =
                    std::dynamic_pointer_cast<SortPlan>(children[0].plan)) {
              std::vector<TopNKey> keys;
              keys.reserve(sort->Keys().size());
              for (const SortKey& key : sort->Keys()) {
                keys.push_back(
                    TopNKey{key.expression, key.ascending, key.nulls_first});
              }
              Plan topn = std::make_shared<TopNPlan>(
                  sort->Child(), std::move(keys), logical.limit_count,
                  logical.limit_offset);
              return std::vector<PlanAlternative>{PlanAlternative{
                  .plan = std::move(topn),
                  .local_cost = children[0].estimated_rows,
                  .estimated_rows = static_cast<double>(
                      std::min(children[0].estimated_rows,
                               static_cast<double>(logical.limit_count)))}};
            }
          }
          const bool explicit_sort =
              std::dynamic_pointer_cast<SortPlan>(children[0].plan) != nullptr;
          if (needs_ordering && !explicit_sort &&
              !children[0].plan->IsOrderedBy(context.query->order_expressions_,
                                             context.query->order_ascending_)) {
            return std::vector<PlanAlternative>{
                PlanAlternative{.plan = children[0].plan,
                                .local_cost = 0,
                                .estimated_rows = children[0].estimated_rows}};
          }
          const double rows =
              LimitReadRows(children[0].estimated_rows, logical.limit_count,
                            logical.limit_offset);
          const double emitted =
              LimitOutputRows(children[0].estimated_rows, logical.limit_count,
                              logical.limit_offset);
          Plan limit = std::make_shared<LimitPlan>(
              children[0].plan, logical.limit_count, logical.limit_offset);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(limit),
                              .local_cost = rows,
                              .estimated_rows = emitted}};
        },
        c::LogicalOperator::kLimit));
    built.Add(c::ImplementationRule(
        "hash_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, true,
                                  false, false);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "merge_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return MergeJoinAlternative(memo, logical.children[1],
                                      logical.predicate, children[0],
                                      children[1], context);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "outer_hash_join", OuterJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          JoinAlternativeKind kind = JoinAlternativeKind::kLeftOuter;
          if (logical.join_type == 1) {
            kind = JoinAlternativeKind::kRightOuter;
          } else if (logical.join_type == 2) {
            kind = JoinAlternativeKind::kFullOuter;
          }
          std::vector<PlanAlternative> alternatives = JoinAlternatives(
              memo, logical.children[1], logical.predicate, children[0],
              children[1], context, true, false, false, kind);
          std::vector<PlanAlternative> merge =
              MergeJoinAlternative(memo, logical.children[1], logical.predicate,
                                   children[0], children[1], context, kind);
          alternatives.insert(alternatives.end(),
                              std::make_move_iterator(merge.begin()),
                              std::make_move_iterator(merge.end()));
          return alternatives;
        },
        c::LogicalOperator::kOuterJoin));
    built.Add(c::ImplementationRule(
        "cross_join", c::dsl::CrossJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], std::nullopt,
                                  children[0], children[1], context, false,
                                  false, true);
        },
        c::LogicalOperator::kCrossJoin));
    built.Add(c::ImplementationRule(
        "semi_hash_join", c::dsl::SemiJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, true,
                                  false, false, JoinAlternativeKind::kSemi);
        },
        c::LogicalOperator::kSemiJoin));
    built.Add(c::ImplementationRule(
        "anti_hash_join", c::dsl::AntiJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, true,
                                  false, false, JoinAlternativeKind::kAnti);
        },
        c::LogicalOperator::kAntiJoin));
    built.Add(c::ImplementationRule(
        "semi_merge_join", c::dsl::SemiJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const c::PhysicalProperties& required,
           const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return MergeJoinAlternative(
              memo, logical.children[1], logical.predicate, children[0],
              children[1], context, JoinAlternativeKind::kSemi);
        },
        c::LogicalOperator::kSemiJoin));
    built.Add(c::ImplementationRule(
        "anti_merge_join", c::dsl::AntiJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const c::PhysicalProperties& required,
           const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return MergeJoinAlternative(
              memo, logical.children[1], logical.predicate, children[0],
              children[1], context, JoinAlternativeKind::kAnti);
        },
        c::LogicalOperator::kAntiJoin));
    built.Add(c::ImplementationRule(
        "index_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, false,
                                  true, false);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "nested_loop_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, false,
                                  false, true);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "single_hash_join", c::dsl::SingleJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, true,
                                  false, false,
                                  JoinAlternativeKind::kLeftOuter);
        },
        c::LogicalOperator::kSingleJoin));
    built.Add(c::ImplementationRule(
        "mark_hash_join", c::dsl::MarkJoin(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, true,
                                  false, false, JoinAlternativeKind::kSemi);
        },
        c::LogicalOperator::kMarkJoin));
    built.Add(c::ImplementationRule(
        "constant_table", c::dsl::ConstantTable(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical, const std::vector<BestPlan>&,
           const PhysicalProperties&, const c::RuleContext& context) {
          if (logical.values.empty() && !logical.table.empty()) {
            // COUNT(*) is rewritten to this leaf for optimizer purposes, but
            // its result must still be computed from the transaction-visible
            // table.  Catalog statistics are estimates and become stale after
            // INSERT/DELETE, so materializing the count from them is wrong.
            const auto table = context.tables.find(logical.table);
            const auto statistics = context.statistics.find(logical.table);
            if (table == context.tables.end() ||
                statistics == context.statistics.end()) {
              return std::vector<PlanAlternative>{};
            }
            Plan scan = std::make_shared<FullScanPlan>(*table->second,
                                                       *statistics->second);
            Plan aggregate = std::make_shared<HashAggregatePlan>(
                std::move(scan), logical.target_list);
            return std::vector<PlanAlternative>{PlanAlternative{
                .plan = std::move(aggregate),
                .local_cost = static_cast<double>(statistics->second->Rows()),
                .estimated_rows = 1.0}};
          }
          Plan values = std::make_shared<ValuesPlan>(logical.output_schema,
                                                     logical.values);
          const double rows = static_cast<double>(logical.values.size());
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(values),
                              .local_cost = rows,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kConstantTable));
    built.Add(c::ImplementationRule(
        "generate_series", c::dsl::GenerateSeries(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical, const std::vector<BestPlan>&,
           const PhysicalProperties&, const c::RuleContext&) {
          Plan values = std::make_shared<ValuesPlan>(logical.output_schema,
                                                     logical.values);
          const double rows = static_cast<double>(logical.values.size());
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(values),
                              .local_cost = rows,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kGenerateSeries));
    return built;
  }();
  return rules;
}

}  // namespace tinylamb
