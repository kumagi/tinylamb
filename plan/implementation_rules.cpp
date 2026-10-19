/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/implementation_rules.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregation_plan.hpp"
#include "apply_plan.hpp"
#include "band_join_plan.hpp"
#include "bitmap_scan_plan.hpp"
#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/log_message.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "distinct_plan.hpp"
#include "empty_plan.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/hash_join_mode.hpp"
#include "expression/binary_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "full_scan_plan.hpp"
#include "incremental_sort_plan.hpp"
#include "index/index.hpp"
#include "index_only_scan_plan.hpp"
#include "index_scan_plan.hpp"
#include "index_skip_scan_plan.hpp"
#include "late_materialization_join_plan.hpp"
#include "limit_plan.hpp"
#include "lsm_scan_plan.hpp"
#include "merge_join_plan.hpp"
#include "minmax_index_plan.hpp"
#include "multi_index_covering_scan_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "projection_plan.hpp"
#include "query/query_data.hpp"
#include "query/statement.hpp"
#include "recursive_cte_plan.hpp"
#include "sample_plan.hpp"
#include "selection_plan.hpp"
#include "set_operation_plan.hpp"
#include "skip_scan_distinct_plan.hpp"
#include "sort_distinct_plan.hpp"
#include "sort_plan.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "topn_plan.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"
#include "unnest_plan.hpp"
#include "values_plan.hpp"
#include "window_plan.hpp"
#include "zigzag_join_plan.hpp"

namespace tinylamb {
namespace {

// Decode-floor hatch: narrow full scans to the required columns at the
// iterator (TINYLAMB_SCAN_PROJECTION=1). Default OFF: narrowing only pays on
// plans that execute through Cascades cores (the bridge route) -- the
// default relational route prunes scan columns itself -- and under plan
// feedback the narrowed shape flipped TPC-H Q19's cross-join-shaped plan to
// a 10x slower alternative. Revisit after that interaction is root-caused.
bool ScanProjectionEnabled() {
  static const bool enabled =
      std::getenv("TINYLAMB_SCAN_PROJECTION") != nullptr;
  return enabled;
}

// Diagnostic gate for join-order quality investigations (zero cost when
// unset): prints per-key selectivity inputs and hash-join orientation
// candidates so a collapsed estimate or a wrong build side is visible.
bool JoinCardinalityDebug() {
  static const bool join_card_debug =
      std::getenv("TINYLAMB_JOIN_CARD_DEBUG") != nullptr;
  return join_card_debug;
}

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

// Pushdown safety: moving a conjunct below a join changes how often it
// evaluates (once per child row instead of once per candidate pair), which
// is only observable for volatile functions like RANDOM().
bool DeterministicForPushdown(const Expression& expression) {
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return DeterministicForPushdown(child); });
}

size_t RowEstimate(double rows);

struct Range {
  std::optional<Value> min;
  std::optional<Value> max;
  bool min_inclusive{false};
  bool max_inclusive{false};

  enum class Direction : bool { kRight, kLeft };

  void Update(BinaryOperation operation, const Value& value,
              Direction direction) {
    // A comparison against a literal NULL constant evaluates to UNKNOWN for
    // every row, so it proves nothing about the column's range: leave the
    // envelope unconstrained (the Selection above the scan drops the rows).
    // Without this guard the min/max narrowing below runs Value::operator<
    // (ExcShim) with a NULL bound and planning aborts with
    // "Different type cannot be compared." on e.g. `WHERE a > NULL`.
    if (value.IsNull()) {
      return;
    }
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

bool ContainsWindowFunctionExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             ContainsWindowFunctionExpression);
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
  // Window functions are equally input-holistic: the default frame is the
  // whole partition, and any frame reaching past the current row
  // (UNBOUNDED FOLLOWING, LEAD, NTILE, CUME_DIST, ...) reads rows the
  // limit prefix would truncate away.
  return !std::ranges::any_of(
             context.query->select_,
             [](const auto& output) {
               return ContainsAggregateExpression(output.expression) ||
                      ContainsWindowFunctionExpression(output.expression);
             }) &&
         !ContainsAggregateExpression(context.query->qualify_) &&
         !ContainsWindowFunctionExpression(context.query->qualify_);
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
  for (const char byte : prefix) {
    // The storage comparison currently has no collation contract. Restrict
    // the rewrite to byte-stable ASCII so a locale/UTF-8 collation cannot
    // turn the computed half-open range into a different language predicate.
    if (static_cast<unsigned char>(byte) >= 0x80) {
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

double EstimateDistinctRows(const Plan& input,
                            const std::vector<Expression>& columns) {
  const auto rows = static_cast<double>(input->GetStats().Rows());
  if (rows <= 0.0) {
    return 0.0;
  }
  if (columns.empty()) {
    return rows;
  }
  const Schema& schema = input->GetSchema();
  const TableStatistics& statistics = input->GetStats();
  std::vector<size_t> offsets;
  for (const Expression& column : columns) {
    if (!column || column->Type() != TypeTag::kColumnValue) {
      return rows;
    }
    const int offset = schema.Offset(column->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return rows;
    }
    offsets.push_back(static_cast<size_t>(offset));
  }
  return statistics.EstimateDistinctRows(offsets);
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
  Plan scan = std::make_shared<IndexScanPlan>(
      table, index, statistics, std::move(begin_key), std::move(end_key),
      ascending, predicate, std::move(provided_order), require_row_position,
      wait_for_write_intent);
  // A range over a secondary index belongs to the skip-scan access family:
  // the executor seeks range boundaries in the index instead of reading the
  // table.  Covering (index-only) scans keep their dedicated node because
  // DISTINCT planning pattern-matches on it.
  if (!index.IsUnique()) {
    return std::make_shared<IndexSkipScanPlan>(std::move(scan));
  }
  return scan;
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
  // The discount models "stop the scan after the requested prefix". Only a
  // structurally ordered scan can: a measured clustered order saves a sort, but
  // the LIMIT stays above a scan that still walks the whole chain, so
  // discounting it would price a full read as a prefix read.
  if (plan->OrderIsMeasured()) {
    return;
  }
  const auto rows = static_cast<double>(required.limit_hint);
  *local_cost = std::min(*local_cost, rows / std::max(selectivity, 0.01));
  *estimated_rows = std::min(*estimated_rows, rows);
}

// Clustering penalty for range index scans (TODO.md item 1b). A range scan
// over a physically clustered column reads heap rows mostly sequentially,
// while the same range over randomly ordered rows pays a seek per row.
// Point lookups (full equality prefix) are excluded by the caller: they
// fetch scattered rows either way, so correlation carries no signal there.
// The penalty is multiplicative on the heap-fetch portion and bounded at
// 1+k for a fully uncorrelated column.
inline constexpr double kIndexRangeClusteringPenalty = 2.0;

// Covering (index-only) scans read the index alone: no heap fetch per row,
// and the executor skips version resolution entirely on chain-free pages
// (the index-only visibility map).  Price them as a fraction of a heap row
// touch so they win against a non-covering alternative of the same range and
// stay competitive with the sequential full scan.
inline constexpr double kCoveringIndexCostFactor = 0.5;

double RangeClusteringPenalty(const TableStatistics& statistics, slot_t slot) {
  double correlation = 1.0;
  if (slot < statistics.Columns()) {
    correlation = statistics.Column(slot).ClusteringStrength();
  }
  return 1.0 + (kIndexRangeClusteringPenalty * (1.0 - correlation));
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
  auto rows = static_cast<double>(statistics.Rows());
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
      selectivity = frequency / static_cast<double>(stats.NonNullCount());
    }
    rows *= std::clamp(selectivity, 0.0, 1.0);
  }
  return rows;
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
      // Only a same-typed, non-NULL constant may occupy a seek-key slot:
      // NULL encodes as a truncated prefix (over-inclusive) and a
      // cross-typed key can never reach the entries its numeric value
      // would equal (e.g. `u = 2.0` vs INT64 2).
      if (offset >= 0 && !constant->GetValue().IsNull() &&
          constant->GetValue().type ==
              schema.GetColumn(static_cast<size_t>(offset)).Type()) {
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
    // Same cross-type/NULL exclusion as the conjunct collector in
    // ScanAlternatives: a bound that cannot encode into the index key
    // domain must stay residual-only.
    if (constant->GetValue().type !=
        schema.GetColumn(static_cast<size_t>(offset)).Type()) {
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
  if (memo.Get(group).sample_barrier) {
    return {};
  }
  // Materialized CTE singletons (M4) carry a stray kScan alongside their
  // kValues leaf; with no catalog objects the scan offers no alternative
  // (the kValues implementation wins by default).
  const auto table_it = context.tables.find(logical.table);
  const auto statistics_it = context.statistics.find(logical.table);
  if (table_it == context.tables.end() ||
      statistics_it == context.statistics.end()) {
    return {};
  }
  const Table& table = *table_it->second;
  const TableStatistics& statistics = *statistics_it->second;
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
        const ValueType column_type =
            schema.GetColumn(static_cast<size_t>(offset)).Type();
        std::vector<Value> points;
        bool indexable = true;
        for (const Expression& item : in.list_) {
          const Value& point = item->AsConstantValue().GetValue();
          // A NULL point can never satisfy an equality seek; keeping it
          // would also break the sort below (NULL is not ordered against
          // typed values).
          if (point.IsNull()) {
            continue;
          }
          // Cross-typed constants cannot be probed through this index's
          // key encoding: `u IN (0, 2.0)` must still match INT64 2 via
          // numeric comparison, which a DOUBLE-encoded seek key would
          // miss.  Leave the conjunct to the residual predicate instead.
          if (point.type != column_type) {
            indexable = false;
            break;
          }
          points.push_back(point);
        }
        if (indexable && !points.empty()) {
          auto& bucket = point_sets[static_cast<slot_t>(offset)];
          bucket.insert(bucket.end(), std::make_move_iterator(points.begin()),
                        std::make_move_iterator(points.end()));
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
            // LIKE bounds are VARCHAR-encoded; they can only probe a
            // VARCHAR column's index.  On any other column the seek would
            // silently return nothing where the residual predicate should
            // have raised a type error.
            if (offset >= 0 &&
                schema.GetColumn(static_cast<size_t>(offset)).Type() ==
                    ValueType::kVarChar) {
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
      // A bound whose ValueType differs from the column's cannot probe the
      // index: the memcomparable key encoding is type-tagged, so e.g. a
      // DOUBLE seek misses the INT64 entry that `u = 2.0` must find, and
      // narrowing mixed-type bounds throws inside Value::operator<.  The
      // conjunct stays in the residual predicate, which is also what the
      // no-index plan does.
      if (constant->GetValue().type !=
          schema.GetColumn(static_cast<size_t>(offset)).Type()) {
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
      if (no_filter) {
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
          // Detect covering before finalize wraps the node (aliased
          // relations get a rename node that hides the scan type).
          const bool covering = std::dynamic_pointer_cast<IndexOnlyScanPlan>(
                                    unconstrained) != nullptr;
          unconstrained = finalize(unconstrained);
          const double unconstrained_cost =
              static_cast<double>(statistics.Rows()) *
              (covering ? kCoveringIndexCostFactor : 1.0);
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
      // Bitmap heap scans fetch rows by position through the snapshot read
      // path with no write-intent acquisition, so they cannot serve a DML
      // source that must lock and re-resolve each row's committed head
      // (same restriction as index-only scans).
      if (!required.require_row_position && filter &&
          leading_key_range(index) != nullptr) {
        // AND: every independent indexed predicate contributes one bitmap.
        std::vector<BitmapIndexRange> and_ranges;
        std::unordered_set<slot_t> seen_leading_slots;
        for (size_t other_offset = 0; other_offset < table.IndexCount();
             ++other_offset) {
          const Index& other = table.GetIndex(other_offset);
          const auto* const other_range = leading_key_range(other);
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
          const double bitmap_rows = std::max(
              1.0, static_cast<double>(statistics.Rows()) * filter_selectivity);
          const double index_access = std::max(
              1.0, bitmap_rows * static_cast<double>(and_ranges.size()));
          Plan bitmap = std::make_shared<BitmapScanPlan>(
              table, statistics, std::move(and_ranges), BitmapCombine::kAnd,
              scan_predicate, RowEstimate(bitmap_rows),
              RowEstimate(index_access + bitmap_rows));
          if (fallback_select.size() != bitmap->GetSchema().ColumnCount()) {
            bitmap = std::make_shared<ProjectionPlan>(bitmap, fallback_select);
          }
          bitmap = finalize(bitmap);
          candidates.push_back(PlanAlternative{
              .plan = std::move(bitmap),
              .local_cost =
                  std::max(1.0, index_access * 0.25 + bitmap_rows * 0.25),
              .estimated_rows = bitmap_rows});
        }
      }

      // OR: build one bitmap range per disjunct on this index.  Only
      // comparison ranges are admitted; residual rechecking handles strict
      // versus inclusive boundaries and any additional conjuncts.
      if (!required.require_row_position && filter &&
          index.sc_.key_.size() == 1) {
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
            const double bitmap_rows =
                std::max(1.0, filter_selectivity *
                                  static_cast<double>(statistics.Rows()));
            const double index_access =
                std::max(1.0, static_cast<double>(statistics.Rows()));
            Plan bitmap = std::make_shared<BitmapScanPlan>(
                table, statistics, std::move(or_ranges), BitmapCombine::kOr,
                scan_predicate, RowEstimate(bitmap_rows),
                RowEstimate(index_access + bitmap_rows));
            if (fallback_select.size() != bitmap->GetSchema().ColumnCount()) {
              bitmap =
                  std::make_shared<ProjectionPlan>(bitmap, fallback_select);
            }
            bitmap = finalize(bitmap);
            candidates.push_back(PlanAlternative{
                .plan = std::move(bitmap),
                .local_cost =
                    std::max(1.0, index_access * 0.25 + bitmap_rows * 0.25),
                .estimated_rows = bitmap_rows});
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
        // The prefixes are sorted, disjoint and of equal size, so every
        // entry of one range precedes every entry of the next: scanning the
        // ranges in sequence delivers the index key order.  Advertise the
        // full key list so an ORDER BY on the key (or any prefix) needs no
        // Sort.
        std::vector<ColumnName> prefix_order;
        prefix_order.reserve(index.sc_.key_.size());
        for (const slot_t slot : index.sc_.key_) {
          prefix_order.push_back(schema.GetColumn(slot).Name());
        }
        Plan disjunctive = std::make_shared<IndexScanPlan>(
            table, index, statistics, std::move(scan_ranges), true,
            scan_predicate, std::move(prefix_order),
            required.require_row_position, required.wait_for_write_intent);
        if (fallback_select.size() != disjunctive->GetSchema().ColumnCount()) {
          disjunctive =
              std::make_shared<ProjectionPlan>(disjunctive, fallback_select);
        }
        disjunctive = finalize(disjunctive);
        candidates.push_back(PlanAlternative{.plan = std::move(disjunctive),
                                             .local_cost = access_rows,
                                             .estimated_rows = access_rows});
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
      if (points != point_sets.end() && !ranges.contains(point_key_slot)) {
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
          // The point ranges are sorted and disjoint, so scanning them in
          // sequence preserves the index key order: the equality prefix is
          // constant across ranges and the IN column ascends, so every entry
          // of one point precedes every entry of the next.  Advertise the
          // full key list so an ORDER BY on the key (or any prefix) needs no
          // Sort.
          std::vector<ColumnName> point_order;
          point_order.reserve(index.sc_.key_.size());
          for (const slot_t slot : index.sc_.key_) {
            point_order.push_back(schema.GetColumn(slot).Name());
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
          auto point_rows =
              static_cast<double>(point_candidate->EmitRowCount());
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
        // Covering-ness is a property of the scan BuildIndexScan picked; the
        // Selection/Projection/Rename wrappers below hide the node type, so
        // capture it before wrapping.
        const bool covering =
            std::dynamic_pointer_cast<IndexOnlyScanPlan>(candidate) != nullptr;
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
        auto local_cost =
            std::max(1.0, static_cast<double>(candidate->AccessRowCount()));
        local_cost = std::min(
            local_cost,
            EqualityPrefixRows(statistics, index, equality_prefix_values));
        // Random heap I/O for range scans (TODO.md item 1b): a range over a
        // clustered column reads mostly sequentially, while the same range
        // over randomly ordered rows seeks per matching row and can cost
        // more than a full sequential scan. Model it as
        // matched-rows x clustering penalty. Equality-pinned slots are
        // exact point lookups with no clustering signal, so a pure point
        // scan keeps the base cost above. Statistics-free tables keep the
        // old cost exactly (no datapoint to penalize with).
        //
        // A covering (index-only) candidate reads only the index, whose
        // entries arrive in key order: it has no scattered heap seek to
        // penalize, and its cost is the index traversal times the covering
        // factor.  This is what lets a covering index win over a
        // non-covering index with the same range and over a full scan on an
        // unclustered column.
        bool has_range_slot = false;
        double range_penalty = 1.0;
        for (const slot_t slot : consumed) {
          if (!equality_slots.contains(slot)) {
            has_range_slot = true;
            range_penalty *= RangeClusteringPenalty(statistics, slot);
          }
        }
        if (has_range_slot && candidate->AccessRowCount() > 0) {
          local_cost = static_cast<double>(candidate->AccessRowCount()) *
                       (covering ? 1.0 : range_penalty);
        }
        if (covering) {
          local_cost *= kCoveringIndexCostFactor;
        }
        auto estimated_rows = static_cast<double>(candidate->GetStats().Rows());
        ApplyLimitHint(candidate, required, context, filter_selectivity,
                       &local_cost, &estimated_rows);
        candidates.push_back(PlanAlternative{.plan = std::move(candidate),
                                             .local_cost = local_cost,
                                             .estimated_rows = estimated_rows});
      }
    }

    // Multi-index covering: two or more indexes whose stored columns together
    // cover every column the predicate and the output list touch, while no
    // single index covers them alone (that case is the index-only path
    // above).  Each participant contributes a leading-key range from the
    // filter, so the intersection of their entries is a superset of the
    // matching rows and the recheck on the covered columns decides.  A
    // participant without a range would force a full index walk per position
    // and stays with the bitmap / single-index paths.
    if (!required.require_row_position && filter &&
        relational_detail::SplitDisjuncts(filter).size() == 1) {
      std::unordered_set<slot_t> touched;
      for (const ColumnName& column : filter->TouchedColumns()) {
        const int offset = schema.Offset(column);
        if (offset >= 0) {
          touched.insert(static_cast<slot_t>(offset));
        }
      }
      for (const NamedExpression& item : fallback_select) {
        for (const ColumnName& column : item.expression->TouchedColumns()) {
          const int offset = schema.Offset(column);
          if (offset >= 0) {
            touched.insert(static_cast<slot_t>(offset));
          }
        }
      }
      // Participants: one per distinct leading key slot, each with a usable
      // range on a non-retained index (a retained entry is not the committed
      // image the covering read serves).
      std::vector<BitmapIndexRange> participants;
      std::unordered_set<slot_t> seen_leading;
      for (size_t index_offset = 0; index_offset < table.IndexCount();
           ++index_offset) {
        const Index& candidate = table.GetIndex(index_offset);
        if (candidate.RetainsDeletedEntries()) {
          continue;
        }
        const auto bound = ranges.find(candidate.sc_.key_.front());
        if (bound == ranges.end() ||
            (!bound->second.min && !bound->second.max)) {
          continue;
        }
        if (!seen_leading.insert(candidate.sc_.key_.front()).second) {
          continue;
        }
        BitmapIndexRange range;
        range.index = &candidate;
        if (bound->second.min) {
          range.begin_key.push_back(*bound->second.min);
        }
        if (bound->second.max) {
          range.end_key.push_back(*bound->second.max);
        }
        participants.push_back(std::move(range));
      }
      // Greedy minimal cover over the touched columns.
      std::vector<BitmapIndexRange> chosen;
      {
        std::unordered_set<slot_t> covered;
        std::vector<bool> used(participants.size(), false);
        while (!Covered(covered, touched)) {
          size_t best = participants.size();
          size_t best_gain = 0;
          for (size_t i = 0; i < participants.size(); ++i) {
            if (used[i]) {
              continue;
            }
            size_t gain = 0;
            for (const slot_t slot : participants[i].index->CoveredColumns()) {
              if (touched.contains(slot) && !covered.contains(slot)) {
                ++gain;
              }
            }
            if (gain > best_gain) {
              best_gain = gain;
              best = i;
            }
          }
          if (best == participants.size()) {
            break;
          }
          used[best] = true;
          for (const slot_t slot : participants[best].index->CoveredColumns()) {
            if (touched.contains(slot)) {
              covered.insert(slot);
            }
          }
          chosen.push_back(std::move(participants[best]));
        }
        if (!Covered(covered, touched)) {
          chosen.clear();
        }
      }
      // Drop participants whose touched columns the rest already provide:
      // they would only widen the intersection work.  At least two stay -- a
      // single covering index is the index-only path's job.
      bool pruned = true;
      while (pruned && chosen.size() > 2) {
        pruned = false;
        for (size_t i = 0; i < chosen.size() && !pruned; ++i) {
          std::unordered_set<slot_t> others;
          for (size_t j = 0; j < chosen.size(); ++j) {
            if (i == j) {
              continue;
            }
            for (const slot_t slot : chosen[j].index->CoveredColumns()) {
              if (touched.contains(slot)) {
                others.insert(slot);
              }
            }
          }
          const bool redundant = std::ranges::all_of(
              chosen[i].index->CoveredColumns(), [&](const slot_t slot) {
                return !touched.contains(slot) || others.contains(slot);
              });
          if (redundant) {
            chosen.erase(chosen.begin() + static_cast<std::ptrdiff_t>(i));
            pruned = true;
          }
        }
      }
      if (chosen.size() >= 2) {
        // The narrowest range is materialized first: the executor seeds its
        // intersection table from the first participant.
        const auto range_rows = [&](const BitmapIndexRange& range) {
          if (!range.begin_key.empty() && range.begin_key == range.end_key) {
            return EqualityPrefixRows(statistics, *range.index,
                                      range.begin_key);
          }
          return static_cast<double>(statistics.Rows()) * filter_selectivity;
        };
        // Stable, so two participants with the same estimated width keep
        // their catalog (creation) order: the participant order is visible in
        // EXPLAIN and must not wobble between runs of the same statement.
        std::ranges::stable_sort(
            chosen, [&](const BitmapIndexRange& a, const BitmapIndexRange& b) {
              return range_rows(a) < range_rows(b);
            });
        const double covering_rows = std::max(
            1.0, static_cast<double>(statistics.Rows()) * filter_selectivity);
        const double index_access =
            std::max(1.0, covering_rows * static_cast<double>(chosen.size()));
        Plan covering = std::make_shared<MultiIndexCoveringScanPlan>(
            table, statistics, std::move(chosen), scan_predicate,
            RowEstimate(covering_rows),
            RowEstimate(index_access + covering_rows));
        covering =
            RemoveIdentityProjection(std::move(covering), fallback_select);
        covering = finalize(std::move(covering));
        // The same shape as the bitmap AND price, discounted like every
        // covering (index-only) read: the intersection costs one index walk
        // per participant but saves every heap fetch the recheck would need.
        candidates.push_back(PlanAlternative{
            .plan = std::move(covering),
            .local_cost =
                std::max(1.0, (index_access * 0.25 + covering_rows * 0.25) *
                                  kCoveringIndexCostFactor),
            .estimated_rows = covering_rows});
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
        // A ParameterSlot's peeked integer would stay sniffed after rebind
        // and drop rows the residual predicate would have kept.
        if (ContainsParameterSlot(conjunct)) {
          continue;
        }
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
    // Required-column narrowing (Phase 3, decode floor): when the required
    // list is a strict subset of plain table columns, emit only those
    // columns from the scan itself (the iterator skips the others before
    // deserialize). A ProjectionPlan wrapper would decode every column of
    // every row first, which dominates wide-table scans. The narrowed scan
    // keeps table-column order so its statistics subset stays positionally
    // aligned; DML sources (require_row_position maps to lock_rows) and
    // peek-compare scans keep the previous shapes.
    std::vector<slot_t> scan_slots;
    // Decode-floor hatch (default OFF): narrowing only pays on plans that
    // execute through Cascades cores (the bridge route); the default
    // relational route prunes scan columns itself, and under plan feedback
    // the narrowed shape flipped TPC-H Q19's cross-join-shaped plan to a
    // 10x slower alternative. Revisit after the feedback interaction is
    // root-caused.
    bool narrow_scan = ScanProjectionEnabled() &&
                       !required.require_row_position && scan_peeks.empty() &&
                       fallback_select.size() < schema.ColumnCount();
    if (narrow_scan) {
      for (const auto& named : fallback_select) {
        const int offset =
            named.expression &&
                    named.expression->Type() == TypeTag::kColumnValue
                ? schema.Offset(
                      named.expression->AsColumnValue().GetColumnName())
                : -1;
        if (offset < 0) {
          narrow_scan = false;
          break;
        }
        scan_slots.push_back(static_cast<slot_t>(offset));
      }
    }
    Plan full_scan;
    if (narrow_scan) {
      const FullScanPlan base(table, statistics, scan_limit,
                              required.require_row_position,
                              required.wait_for_write_intent);
      full_scan = std::make_shared<FullScanPlan>(base, std::move(scan_slots));
    } else {
      full_scan = scan_peeks.empty()
                      ? static_cast<Plan>(std::make_shared<FullScanPlan>(
                            table, statistics, scan_limit,
                            required.require_row_position,
                            required.wait_for_write_intent))
                      : static_cast<Plan>(std::make_shared<FullScanPlan>(
                            table, statistics, std::move(scan_peeks),
                            scan_limit, required.require_row_position,
                            required.wait_for_write_intent));
    }
    if (filter) {
      full_scan = std::make_shared<SelectionPlan>(full_scan, filter,
                                                  full_scan->GetStats());
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
    auto estimated_rows = static_cast<double>(full_scan->GetStats().Rows());
    ApplyLimitHint(full_scan, required, context, filter_selectivity,
                   &local_cost, &estimated_rows);
    candidates.push_back(PlanAlternative{.plan = std::move(full_scan),
                                         .local_cost = local_cost,
                                         .estimated_rows = estimated_rows});
  }
  return candidates;
}

// D3 join cardinality: |L|*|R| times EstimateEquijoinSelectivity of the
// per-key 1/max(NDV) values, capped by the cross product. A single equality
// is the historical |L|*|R|/max(NDV) model. Missing stats fall back to the
// cross product.
double JoinCardinality(
    const BestPlan& left, const BestPlan& right,
    const std::vector<std::pair<ColumnName, ColumnName>>& equalities,
    const Expression& residual = nullptr,
    const std::vector<bool>* null_safe = nullptr) {
  std::vector<double> key_selectivities;
  key_selectivities.reserve(equalities.size());
  for (size_t key = 0; key < equalities.size(); ++key) {
    const auto& [left_column, right_column] = equalities[key];
    const bool matches_null =
        null_safe != nullptr && key < null_safe->size() && (*null_safe)[key];
    const int lo = left.plan->GetSchema().Offset(left_column);
    const int ro = right.plan->GetSchema().Offset(right_column);
    const auto& left_stats = left.plan->GetStats();
    const auto& right_stats = right.plan->GetStats();
    if (lo >= 0 && ro >= 0 && static_cast<size_t>(lo) < left_stats.Columns() &&
        static_cast<size_t>(ro) < right_stats.Columns()) {
      const auto& l = left_stats.Column(static_cast<size_t>(lo));
      const auto& r = right_stats.Column(static_cast<size_t>(ro));
      const double population =
          (static_cast<double>(l.NonNullCount() + l.NullCount())) *
          (static_cast<double>(r.NonNullCount() + r.NullCount()));
      if (population > 0.0) {
        key_selectivities.push_back(std::clamp(
            l.EstimateJoinMatches(r, matches_null) / population, 0.0, 1.0));
      }
    }
  }
  const double cross = left.estimated_rows * right.estimated_rows;
  double estimate = cross;
  if (!key_selectivities.empty()) {
    const double backoff =
        cascades::EstimateEquijoinSelectivity(key_selectivities);
    // Composite equality keys within one join conjunct are rarely
    // independent: they are typically a composite foreign key meeting a
    // unique key ((l_partkey, l_suppkey) = (ps_partkey, ps_suppkey)), so the
    // per-key selectivities are strongly correlated. The independence
    // backoff multiplies them into the ground (observed 100x under-estimates
    // on TPC-H/TPC-DS FK joins), which poisons join order and build-side
    // choices. Floor the combination at the most selective single key: under
    // correlation the strongest key alone bounds the matching pairs from
    // below, and the cross-product cap still applies. Single-key joins are
    // unaffected (the floor equals the key itself).
    const double min_key = *std::ranges::min_element(key_selectivities);
    estimate = std::min(cross, cross * std::max(backoff, min_key));
  }
  // Diagnostic for join-order quality investigations (zero cost when unset):
  // per-key selectivity inputs are the difference between a sound join order
  // and a collapsed estimate, so print both sides of each key.
  if (JoinCardinalityDebug()) {
    std::string keys;
    for (size_t key = 0; key < equalities.size(); ++key) {
      const auto& [left_column, right_column] = equalities[key];
      const auto& left_stats = left.plan->GetStats();
      const auto& right_stats = right.plan->GetStats();
      const int lo = left.plan->GetSchema().Offset(left_column);
      const int ro = right.plan->GetSchema().Offset(right_column);
      keys += " [";
      keys += left_column.ToString();
      if (lo >= 0 && static_cast<size_t>(lo) < left_stats.Columns()) {
        const auto& l = left_stats.Column(static_cast<size_t>(lo));
        keys += " nn=" + std::to_string(l.NonNullCount()) +
                " ndv=" + std::to_string(l.Distinct()) +
                (l.Unmeasured() ? " UNMEASURED" : "");
      } else {
        keys += " no-stats";
      }
      keys += "=";
      keys += right_column.ToString();
      if (ro >= 0 && static_cast<size_t>(ro) < right_stats.Columns()) {
        const auto& r = right_stats.Column(static_cast<size_t>(ro));
        keys += " nn=" + std::to_string(r.NonNullCount()) +
                " ndv=" + std::to_string(r.Distinct()) +
                (r.Unmeasured() ? " UNMEASURED" : "");
      } else {
        keys += " no-stats";
      }
      keys += " sel=";
      keys += key < key_selectivities.size()
                  ? std::to_string(key_selectivities[key])
                  : std::string("dropped");
      keys += "]";
    }
    LOG(INFO) << "join-card l_rows=" << left.estimated_rows
              << " r_rows=" << right.estimated_rows << " est=" << estimate
              << keys;
  }
  if (!residual) {
    return estimate;
  }
  double residual_factor = 1.0;
  for (const Expression& conjunct : SplitConjuncts(residual)) {
    if (!conjunct) {
      return estimate;
    }
    bool touches_left = false;
    bool touches_right = false;
    for (const ColumnName& column : conjunct->TouchedColumns()) {
      const int lo = left.plan->GetSchema().Offset(column);
      const int ro = right.plan->GetSchema().Offset(column);
      if (lo >= 0) {
        touches_left = true;
      } else if (ro >= 0) {
        touches_right = true;
      } else {
        return estimate;
      }
    }
    if (touches_left && touches_right) {
      return estimate;
    }
    const BestPlan& side = touches_right ? right : left;
    residual_factor *= std::clamp(side.plan->GetStats().EstimateSelectivity(
                                      side.plan->GetSchema(), conjunct),
                                  0.0, 1.0);
  }
  return estimate * residual_factor;
}

size_t RowEstimate(double rows) {
  if (!std::isfinite(rows) ||
      rows >= static_cast<double>(std::numeric_limits<size_t>::max())) {
    return std::numeric_limits<size_t>::max();
  }
  if (rows <= 0.0) {
    return 0;
  }
  return static_cast<size_t>(std::ceil(rows));
}

bool SamePhysicalJoinType(const Schema& left_schema, const ColumnName& left,
                          const Schema& right_schema, const ColumnName& right) {
  const int left_offset = left_schema.Offset(left);
  const int right_offset = right_schema.Offset(right);
  if (left_offset < 0 || right_offset < 0) {
    return false;
  }
  return left_schema.GetColumn(static_cast<size_t>(left_offset)).Type() ==
         right_schema.GetColumn(static_cast<size_t>(right_offset)).Type();
}

bool HasNullSafeCrossJoinEquality(const Expression& predicate, const Plan& left,
                                  const Plan& right) {
  return std::ranges::any_of(
      SplitConjuncts(predicate), [&](const Expression& conjunct) {
        if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = conjunct->AsBinaryExpression();
        if (binary.Op() != BinaryOperation::kIsNotDistinctFrom ||
            !binary.Left() || !binary.Right() ||
            binary.Left()->Type() != TypeTag::kColumnValue ||
            binary.Right()->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
        return (left->GetSchema().Offset(lhs) >= 0 &&
                right->GetSchema().Offset(rhs) >= 0) ||
               (left->GetSchema().Offset(rhs) >= 0 &&
                right->GetSchema().Offset(lhs) >= 0);
      });
}

bool SupportsLateMaterializationKeyType(ValueType type) {
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

// Dynamic ("shared information passing") join filter, Phase 2 "Dynamic
// Filter Join / SIP / Bloom".
//
// The `dynamic_filter_pushdown_join` logical rule annotates a probe group with
// the join keys a build side can publish a sound filter over. This is the
// physical gate:
//
//   * the probe group must carry that annotation and it must name exactly the
//     equi-join key -- an annotation over a different key set describes a
//     different filter, and applying it would be a guess;
//   * the key must be a single INT64/DATE column, the only domain the runtime
//     filter is defined over (a composite or non-integer key keeps the plain
//     hash join);
//   * the join must be inner and free of null-safe keys, because a
//     null-rejecting filter is not a membership test for `<=>`;
//   * the build side must be materially smaller than the probe side, or
//     building and testing the filter costs more than the rows it saves.
[[nodiscard]] bool DynamicFilterIsProfitable(double build_rows,
                                             double probe_rows) {
  if (!(build_rows >= 1.0) || !(probe_rows > build_rows)) {
    return false;
  }
  // Either a clearly smaller build side, or a small build side with a probe
  // side big enough to amortize the filter's construction.
  return build_rows * 4.0 <= probe_rows ||
         (build_rows <= 1024.0 && probe_rows >= 8192.0);
}

[[nodiscard]] bool DynamicFilterAnnotated(
    const cascades::Memo& memo, const BestPlan& probe,
    const std::vector<ColumnName>& probe_keys) {
  // A hand-built BestPlan (unit tests calling ImplementationRule::Apply
  // directly) carries no group; only a searched plan can be annotated.
  if (probe.group == cascades::kInvalidGroup) {
    return false;
  }
  const std::vector<ColumnName>& annotated =
      memo.Get(probe.group).dynamic_filter_keys;
  if (annotated.empty() || annotated.size() != probe_keys.size()) {
    return false;
  }
  return std::ranges::all_of(probe_keys, [&](const ColumnName& key) {
    return std::ranges::find(annotated, key) != annotated.end();
  });
}

// Hash semi reduction (Phase 2 "Hash Semi Reduction"): the physical gate.
//
// `SemiReductionIsProfitable` / `JoinKeyDomainEstimate` live in
// plan/product_plan.cpp because the subquery-decorrelation path builds the same
// ProductPlan shape and has to reach the same verdict.
//
//   * only a semi / anti kind qualifies. Outer joins null-pad build rows that
//     the probe never matches, so the build cannot be cut to the probe domain;
//     inner joins emit every matching build row;
//   * a null-safe key (IS NOT DISTINCT FROM) is left to the plain pipeline: it
//     is a legal shape, but the reduction is not worth a second code path over
//     a key that is rare in practice;
//   * the probe group must carry exactly the semi-reduction key annotation the
//     `semi_reduction_pushdown_semi_join` / `..._anti_join` rules recorded, so
//     the domain is built over the columns that actually carry the join key;
//   * the build side must be large enough that cutting it pays.
[[nodiscard]] bool SemiReductionAnnotated(const cascades::Memo& memo,
                                          const BestPlan& probe,
                                          const std::vector<ColumnName>& keys) {
  if (probe.group == cascades::kInvalidGroup) {
    return false;
  }
  const std::vector<ColumnName>& annotated =
      memo.Get(probe.group).semi_reduction_probe_keys;
  if (annotated.size() != keys.size()) {
    return false;
  }
  return std::ranges::all_of(keys, [&](const ColumnName& key) {
    return std::ranges::find(annotated, key) != annotated.end();
  });
}

// Fraction of the probe side a sound filter over the build side's keys can
// prune: a probe row survives only if its key is one of the build side's keys,
// so the survival rate is min(1, build_rows / probe_rows).
//
// Two estimates are deliberately NOT used here. The join cardinality estimate
// multiplies independent assumptions and routinely exceeds the probe row count
// (a 5k x 40k equi-join on unique keys estimates ~49k matches over 40k probe
// rows), which would make every dynamic filter look useless. The per-column
// distinct estimate saturates at the ANALYZE sample size, so an NDV ratio
// reports 1.0 for every wide column. The row-count ratio is the coarsest honest
// proxy and stays monotone in the quantity the filter actually exploits: how
// much smaller the build domain is than the probe domain.
[[nodiscard]] double DynamicFilterPrunableRows(double build_rows,
                                               double probe_rows) {
  const double survival = std::min(1.0, build_rows / probe_rows);
  return probe_rows * (1.0 - survival);
}

[[nodiscard]] bool DynamicFilterKeyTypeSupported(
    const Schema& build_schema, const ColumnName& build_column,
    const Schema& probe_schema, const ColumnName& probe_column) {
  const int build_offset = build_schema.Offset(build_column);
  const int probe_offset = probe_schema.Offset(probe_column);
  if (build_offset < 0 || probe_offset < 0) {
    return false;
  }
  const ValueType build_type =
      build_schema.GetColumn(static_cast<size_t>(build_offset)).Type();
  const ValueType probe_type =
      probe_schema.GetColumn(static_cast<size_t>(probe_offset)).Type();
  // Type equality is part of the filter's soundness: INT64 and DATE share the
  // int64 bit domain but not the comparison domain.
  return build_type == probe_type &&
         SupportsLateMaterializationKeyType(build_type);
}

bool IsBandComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

// The index nested-loop probe always evaluates its range bound as
// `right_index_column <op> left_bound_column`, so a predicate written the
// other way round is mirrored here.
BinaryOperation MirrorRangeOperation(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

struct LookupRangeCandidate {
  ColumnName left_column;
  // Relation-qualified, i.e. the name the residual predicate and the join's
  // declared output schema use.
  ColumnName right_column;
  // Physical (table-qualified), i.e. the name the probe's right column list
  // and the index key use.
  ColumnName right_physical;
  BinaryOperation operation = BinaryOperation::kEquals;
  Expression conjunct;
};

// Picks the range conjuncts that can narrow a probe whose equality prefix is
// already fixed: at most one lower and one upper bound on the same key column
// (an interval probe). Only column-vs-column comparisons qualify: a constant
// bound belongs to the right child's scan filter, and the index join bypasses
// the right child plan entirely. `right_key_column` is the relation-qualified
// name of the first index key column the prefix leaves unpinned.
std::vector<LookupRangeCandidate> FindLookupRanges(
    const std::vector<Expression>& conjuncts, const Schema& left_schema,
    const Schema& right_schema, const ColumnName& right_key_column,
    const ColumnName& right_key_physical) {
  std::vector<LookupRangeCandidate> found;
  bool have_lower = false;
  bool have_upper = false;
  for (const Expression& conjunct : conjuncts) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (!IsBandComparison(binary.Op()) || !binary.Left() || !binary.Right()) {
      continue;
    }
    if (binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    LookupRangeCandidate candidate;
    if (left_schema.Offset(lhs) >= 0 && right_schema.Offset(rhs) >= 0) {
      candidate.left_column = lhs;
      candidate.right_column = rhs;
      candidate.operation = MirrorRangeOperation(binary.Op());
    } else if (left_schema.Offset(rhs) >= 0 && right_schema.Offset(lhs) >= 0) {
      candidate.left_column = rhs;
      candidate.right_column = lhs;
      candidate.operation = binary.Op();
    } else {
      continue;
    }
    if (candidate.right_column != right_key_column) {
      continue;
    }
    // One bound per side: the first conjunct on each side wins; the rest stay
    // with the residual predicate.
    const bool lower = IsLowerLookupRange(candidate.operation);
    if ((lower && have_lower) || (!lower && have_upper)) {
      continue;
    }
    (lower ? have_lower : have_upper) = true;
    candidate.right_physical = right_key_physical;
    candidate.conjunct = conjunct;
    found.push_back(std::move(candidate));
    if (have_lower && have_upper) {
      break;
    }
  }
  return found;
}

std::vector<PlanAlternative> JoinAlternatives(
    const cascades::Memo& memo, cascades::GroupId right_group,
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, const cascades::RuleContext& context, bool hash,
    bool index, bool cross,
    JoinAlternativeKind kind = JoinAlternativeKind::kInner,
    bool allow_outer_residual = false) {
  const Expression predicate = condition ? *condition : nullptr;
  std::vector<std::pair<ColumnName, ColumnName>> equalities;
  std::vector<bool> equality_null_safe;
  std::vector<Expression> equi_conjuncts;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    const bool null_safe = binary.Op() == BinaryOperation::kIsNotDistinctFrom;
    if ((binary.Op() != BinaryOperation::kEquals && !null_safe) ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    ColumnName left_column;
    ColumnName right_column;
    if (left.plan->GetSchema().Offset(lhs) >= 0 &&
        right.plan->GetSchema().Offset(rhs) >= 0) {
      left_column = lhs;
      right_column = rhs;
    } else if (left.plan->GetSchema().Offset(rhs) >= 0 &&
               right.plan->GetSchema().Offset(lhs) >= 0) {
      left_column = rhs;
      right_column = lhs;
    } else {
      continue;
    }
    if (!SamePhysicalJoinType(left.plan->GetSchema(), left_column,
                              right.plan->GetSchema(), right_column)) {
      continue;
    }
    equalities.emplace_back(left_column, right_column);
    equality_null_safe.push_back(null_safe);
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
  // Inner joins allow moving a single-side residual conjunct below the join:
  // it evaluates to the same value for every candidate pair, so filtering
  // the child first is exactly equivalent while shrinking the join input
  // (the join executor materializes its build side and every output pair,
  // so dropped probe/build rows save real work).  Conjuncts referencing both
  // sides, constants only, unresolved (outer) columns, subqueries, or
  // non-immutable functions stay at the join.
  std::vector<Expression> join_residual_conjuncts;
  Plan left_child = left.plan;
  Plan right_child = right.plan;
  std::vector<Expression> left_pushed;
  std::vector<Expression> right_pushed;
  if (kind == JoinAlternativeKind::kInner) {
    for (const Expression& conjunct : residual_conjuncts) {
      bool left_only = false;
      bool right_only = false;
      bool keep_at_join = conjunct->TouchedColumns().empty();
      if (!ResidualContainsQuery(conjunct) && !keep_at_join &&
          DeterministicForPushdown(conjunct)) {
        left_only = true;
        right_only = true;
        for (const ColumnName& column : conjunct->TouchedColumns()) {
          const int lo = left.plan->GetSchema().Offset(column);
          const int ro = right.plan->GetSchema().Offset(column);
          if (lo >= 0 && ro < 0) {
            right_only = false;
          } else if (ro >= 0 && lo < 0) {
            left_only = false;
          } else {
            left_only = false;
            right_only = false;
            break;
          }
        }
      } else {
        left_only = false;
        right_only = false;
      }
      if (left_only) {
        left_pushed.push_back(conjunct);
      } else if (right_only) {
        right_pushed.push_back(conjunct);
      } else {
        join_residual_conjuncts.push_back(conjunct);
      }
    }
    if (!left_pushed.empty()) {
      left_child = std::make_shared<SelectionPlan>(
          left_child, CombineConjuncts(left_pushed), left_child->GetStats());
    }
    if (!right_pushed.empty()) {
      right_child = std::make_shared<SelectionPlan>(
          right_child, CombineConjuncts(right_pushed), right_child->GetStats());
    }
  } else {
    join_residual_conjuncts = residual_conjuncts;
  }
  const Expression residual = join_residual_conjuncts.empty()
                                  ? nullptr
                                  : CombineConjuncts(join_residual_conjuncts);

  // A semi/anti hash join emits only the probe side. A residual predicate
  // mentioning the build side cannot be evaluated after that reduction. Outer
  // joins keep the residual in the join node so failed pairs still produce
  // their preserved-side NULL padding.
  if (kind != JoinAlternativeKind::kInner && residual &&
      (!allow_outer_residual ||
       (kind != JoinAlternativeKind::kLeftOuter &&
        kind != JoinAlternativeKind::kRightOuter &&
        kind != JoinAlternativeKind::kFullOuter) ||
       ResidualContainsQuery(residual))) {
    return {};
  }

  const double l_rows = left.estimated_rows;
  const double r_rows = right.estimated_rows;
  const double equi_estimate =
      JoinCardinality(left, right, equalities, residual, &equality_null_safe);
  const std::optional<size_t> equi_rows = RowEstimate(equi_estimate);

  const auto with_residual = [&](Plan plan) {
    if (residual && kind == JoinAlternativeKind::kInner) {
      plan = std::make_shared<SelectionPlan>(plan, residual, plan->GetStats());
    }
    const auto rows = static_cast<double>(plan->EmitRowCount());
    return std::pair<Plan, double>{std::move(plan), rows};
  };

  std::vector<PlanAlternative> candidates;
  if (cross) {
    auto product_plan = std::make_shared<ProductPlan>(left.plan, right.plan);
    Plan product = std::move(product_plan);
    // The cross-product executor does not consume join keys.  A nested-loop
    // implementation must therefore evaluate the complete join predicate,
    // including equality conjuncts; `with_residual` is only valid for hash
    // and index joins that already enforce those equalities themselves.
    if (predicate) {
      // A cross product that filters every pair by the join predicate IS a
      // nested-loop join: carry the predicate into the plan node so the
      // executor lowers to NestedLoopJoin.
      std::static_pointer_cast<ProductPlan>(product)->SetJoinNotes({},
                                                                   predicate);
      if (kind == JoinAlternativeKind::kInner) {
        product = std::make_shared<SelectionPlan>(product, predicate,
                                                  product->GetStats());
      }
    }
    const double local_cost = (l_rows * r_rows) + l_rows + r_rows;
    const auto product_rows = static_cast<double>(product->EmitRowCount());
    candidates.push_back(PlanAlternative{.plan = std::move(product),
                                         .local_cost = local_cost,
                                         .estimated_rows = product_rows});
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
        join =
            std::make_shared<ProductPlan>(left_child, left_columns, right_child,
                                          right_columns, mode, equi_rows);
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
        join = std::make_shared<ProductPlan>(left_child, left_columns,
                                             right_child, right_columns, mode,
                                             physical_kind, equi_rows);
      }
      // The shared-build hash join materializes its (right) build side and
      // streams the (left) probe side, so the build orientation is not
      // cost-symmetric.  Weigh build rows extra only when the build side
      // dwarfs the probe side, where materializing the hash table dominates
      // the join; this steers enumeration towards smaller build sides
      // without reshaping plans over evenly sized inputs.
      const double build_weight = r_rows > l_rows * 4.0 ? 2.0 : 1.0;
      double local_cost = l_rows + (r_rows * build_weight);
      if (kind == JoinAlternativeKind::kInner && r_rows > l_rows) {
        local_cost += (r_rows - l_rows) * 0.5;
      }
      const double build_bytes = r_rows * kHashJoinRowBytesEstimate;
      if (mode == HashJoinMode::kInMemory &&
          PreferHybridHashJoin(static_cast<size_t>(build_bytes))) {
        local_cost += r_rows * 3;
      }
      if (auto* pp = dynamic_cast<ProductPlan*>(join.get())) {
        pp->SetJoinNotes(equality_null_safe, residual);
        if (kind == JoinAlternativeKind::kInner &&
            mode == HashJoinMode::kInMemory && right_columns.size() == 1 &&
            left_columns.size() == 1 &&
            !std::ranges::any_of(equality_null_safe,
                                 [](bool safe) { return safe; })) {
          // The logical rule annotates children[1], so the annotated side is
          // the probe and children[0] is the build. join_commutativity emits
          // the mirrored expression, whose children[1] is the other side, so
          // this single rule already offers both orientations and cost picks
          // the one whose build side is the smaller estimate.
          const double build_rows = l_rows;
          const double probe_rows = r_rows;
          if (DynamicFilterAnnotated(memo, right, right_columns) &&
              DynamicFilterKeyTypeSupported(
                  left.plan->GetSchema(), left_columns.front(),
                  right.plan->GetSchema(), right_columns.front()) &&
              DynamicFilterIsProfitable(build_rows, probe_rows)) {
            pp->PreferDynamicFilter(/*build_left=*/true);
            const double prunable =
                DynamicFilterPrunableRows(build_rows, probe_rows);
            // Cost model, on a SYMMETRIC base. The plain hash join's local cost
            // presumes the right side builds, so it is orientation-asymmetric
            // (l + r*weight + (r-l)/2); a dynamic filter makes the build side
            // a free choice, because the executor publishes the filter from
            // whichever side it drains first. `l + r` is a lower bound on the
            // plain cost (weight >= 1 and (r-l)/2 >= 0), so the decision is
            // made purely by the filter's own terms: build one filter row,
            // one membership test per probe row, and one saved hash lookup plus
            // output row per rejected probe row. A build domain that barely
            // narrows the probe side loses to the plain hash join on cost, not
            // on a flag.
            constexpr double kBuildRowFilterCost = 0.5;
            constexpr double kProbeMembershipCost = 0.25;
            constexpr double kPrunedRowSaving = 0.75;
            const double filtered_cost = (build_rows * kBuildRowFilterCost) +
                                         (probe_rows * kProbeMembershipCost) -
                                         (prunable * kPrunedRowSaving);
            local_cost = l_rows + r_rows + filtered_cost;
          }
        }
        // Hash semi reduction (Phase 2 "Hash Semi Reduction"). Only an
        // existence-only kind qualifies: semi / anti emit the probe side only,
        // so the build rows exist to answer "is this key present?" and can be
        // cut to the probe side's key domain (and to one row per distinct key)
        // without changing an output row. Outer / inner / mark / single kinds
        // null-pad or emit build columns, so they are structurally excluded --
        // and JoinAlternatives already refuses a semi/anti join that carries a
        // residual, because a residual needs the build row it would discard.
        if ((kind == JoinAlternativeKind::kSemi ||
             kind == JoinAlternativeKind::kAnti) &&
            !std::ranges::any_of(equality_null_safe,
                                 [](bool safe) { return safe; }) &&
            SemiReductionAnnotated(memo, left, left_columns) &&
            SemiReductionProfitable(left.plan, left_columns, right.plan,
                                    right_columns)) {
          const double kept = SemiReductionKeptRows(
              l_rows, r_rows, JoinKeyDomainEstimate(left.plan, left_columns),
              JoinKeyDomainEstimate(right.plan, right_columns));
          pp->PreferSemiReduction();
          // Cost model. The plain semi/anti hash cost is `l + r*weight`; the
          // reduction adds one domain insert per probe row and one membership
          // test per build row, and saves the row handling (hash insert, chain
          // walk, resident memory) of every build row the domain removes.
          constexpr double kDomainInsertCost = 0.25;
          constexpr double kBuildMembershipCost = 0.25;
          constexpr double kPrunedRowSaving = 1.0;
          const double reduced_cost = (l_rows * kDomainInsertCost) +
                                      (r_rows * kBuildMembershipCost) -
                                      ((r_rows - kept) * kPrunedRowSaving);
          local_cost += reduced_cost;
        }
      }
      auto [plan, est] = with_residual(std::move(join));
      if (JoinCardinalityDebug()) {
        std::ostringstream left_dump;
        std::ostringstream right_dump;
        left_child->Dump(left_dump, 0);
        right_child->Dump(right_dump, 0);
        LOG(INFO) << "join-alt kind=" << static_cast<int>(kind)
                  << " mode=" << static_cast<int>(mode) << " l_rows=" << l_rows
                  << " r_rows=" << r_rows << " local_cost=" << local_cost
                  << " est=" << est << " left=[" << left_dump.str()
                  << "] right=[" << right_dump.str() << "]";
      }
      candidates.push_back(PlanAlternative{.plan = std::move(plan),
                                           .local_cost = local_cost,
                                           .estimated_rows = est});
    }
  }

  if (index && !std::ranges::any_of(equality_null_safe,
                                    [](bool safe) { return safe; })) {
    const Table* right_table = right.plan->ScanSource();
    if (right_table != nullptr) {
      // The index nested-loop executor bypasses the right child plan entirely
      // and probes the raw table through its index, so anything that child
      // plan would have filtered has to be enforced HERE instead.  That is the
      // contract: an inner filter the index probe cannot express is not a
      // reason to refuse the shape, it is a conjunct the residual carries.
      //
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
          Column col = physical_schema.GetColumn(i);
          col.Name() = ColumnName(right_relation, col.Name().name);
          renamed_columns.push_back(std::move(col));
        }
        const Schema declared_output =
            left.plan->GetSchema() + Schema("", std::move(renamed_columns));
        std::vector<ColumnName> right_physical;
        right_physical.reserve(right_columns.size());
        for (const ColumnName& c : right_columns) {
          right_physical.push_back(to_physical(c));
        }
        // Every conjunct of the join predicate. The probe decides only a
        // prefix of them, so this is also the source of the residual that
        // restores the rest.
        const std::vector<Expression> all_conjuncts =
            predicate ? SplitConjuncts(predicate) : std::vector<Expression>{};
        // Only the LEFT child is consumed by this executor, so only its
        // pushed conjuncts are genuinely already applied.  A conjunct
        // pushed into the RIGHT child plan is NOT: the probe bypasses that
        // plan, so skipping it here would silently answer a different
        // query.
        std::unordered_set<std::string> pushed;
        for (const Expression& conjunct : left_pushed) {
          pushed.insert(conjunct->ToString());
        }
        // Everything this executor still has to enforce itself, from three
        // sources: the join predicate's own conjuncts, the conjuncts pushed
        // into the bypassed right child plan, and the inner relation's
        // scan-group filter.  The joined row carries every right column
        // (declared_output renames them under the right relation), so an
        // inner conjunct evaluates on it exactly as it would on the child.
        // Deduplicated because the same conjunct commonly arrives by two
        // of these routes.
        std::vector<Expression> residual_pool = all_conjuncts;
        std::unordered_set<std::string> pooled;
        for (const Expression& conjunct : all_conjuncts) {
          pooled.insert(conjunct->ToString());
        }
        const auto pool = [&residual_pool,
                           &pooled](const Expression& conjunct) {
          if (!conjunct) {
            return;
          }
          if (pooled.insert(conjunct->ToString()).second) {
            residual_pool.push_back(conjunct);
          }
        };
        for (const Expression& conjunct : right_pushed) {
          pool(conjunct);
        }
        if (const Expression& inner_filter = memo.Get(right_group).filter;
            inner_filter) {
          for (const Expression& conjunct : SplitConjuncts(inner_filter)) {
            pool(conjunct);
          }
        }
        for (size_t i = 0; i < right_table->IndexCount(); ++i) {
          const Index& index_handle = right_table->GetIndex(i);
          // Equality prefix in the index's own key order. Deriving it (rather
          // than reusing the equi-join's key order) is what makes a
          // composite index usable when only some of its key columns are
          // join keys: the seek key has to be a prefix of the stored key
          // image, not an unrelated byte range.
          std::optional<LookupSeek> probe = DeriveLookupSeek(
              index_handle, physical_schema, right_physical, left_columns);
          if (!probe) {
            continue;
          }
          // Conjuncts this probe decides by itself: the pinned prefix
          // equalities, plus the range bound below when it is admissible.
          std::unordered_set<std::string> enforced;
          for (const ColumnName& key_column : probe->right_columns) {
            const auto equality = std::ranges::find_if(
                equalities,
                [&](const std::pair<ColumnName, ColumnName>& entry) {
                  return to_physical(entry.second) == key_column;
                });
            if (equality != equalities.end()) {
              enforced.insert(equi_conjuncts[static_cast<size_t>(
                                                 equality - equalities.begin())]
                                  ->ToString());
            }
          }
          // Range bounds on the first index key column the equality prefix
          // leaves open: at most one lower and one upper conjunct (an
          // interval probe). SetLookupRange rejects the shapes that cannot
          // be probed safely (a later key column, mismatched operand types,
          // a type outside the range whitelist) and the conjunct then simply
          // stays in the residual predicate below: an explicit fallback,
          // never a silently dropped one.
          std::vector<LookupRangeCandidate> ranges;
          auto range_key_slot = slot_t{0};
          const size_t prefix_len = probe->right_columns.size();
          if (prefix_len < index_handle.sc_.key_.size()) {
            const slot_t next_key = index_handle.sc_.key_[prefix_len];
            if (next_key < physical_schema.ColumnCount()) {
              const ColumnName& key_physical =
                  physical_schema.GetColumn(next_key).Name();
              ranges = FindLookupRanges(
                  all_conjuncts, left.plan->GetSchema(),
                  right.plan->GetSchema(),
                  ColumnName(right_relation, key_physical.name), key_physical);
              range_key_slot = next_key;
            }
          }
          const bool point_probe =
              ranges.empty() && prefix_len == index_handle.sc_.key_.size();
          // A unique index caps the output at the left row count only for a
          // point probe on the complete key; with a range bound (or a prefix
          // shorter than the key) one left row can still match several right
          // rows.
          std::optional<size_t> index_rows = equi_rows;
          if (point_probe && index_handle.IsUnique() &&
              index_rows.has_value()) {
            index_rows = std::min(*index_rows, RowEstimate(l_rows));
          }
          Plan join = std::make_shared<ProductPlan>(
              left_child, left_columns, *right_table, index_handle,
              right_physical, *stats_it->second, declared_output, index_rows);
          bool range_adopted = false;
          for (const LookupRangeCandidate& range : ranges) {
            if (static_cast<ProductPlan&>(*join).SetLookupRange(
                    prefix_len, range.left_column, range.right_physical,
                    range.operation)) {
              range_adopted = true;
              enforced.insert(range.conjunct->ToString());
            }
          }
          // Everything the probe does not decide, and that this
          // executor is not already applying through a child it consumes,
          // becomes the residual predicate. This is where a second range
          // column, an equality on a non-key column, a range the whitelist
          // rejected and the inner relation's whole filter all end up: the
          // probe over-fetches and the predicate decides the rows.
          std::vector<Expression> lookup_residual;
          for (const Expression& conjunct : residual_pool) {
            const std::string printed = conjunct->ToString();
            if (enforced.contains(printed) || pushed.contains(printed)) {
              continue;
            }
            lookup_residual.push_back(conjunct);
          }
          Plan lookup_plan = std::move(join);
          if (!lookup_residual.empty()) {
            lookup_plan = std::make_shared<SelectionPlan>(
                lookup_plan, CombineConjuncts(lookup_residual),
                lookup_plan->GetStats());
          }
          // One B+Tree probe plus row assembly per left row, plus the rows
          // the probe is expected to hand back. A range bound turns the
          // point probe into a range walk over the heap, so it pays the same
          // clustering penalty the range-sliced index scan applies
          // (see RangeClusteringPenalty).
          double local_cost = (l_rows * 2.0) + equi_estimate;
          if (range_adopted) {
            local_cost = std::max(
                1.0, local_cost * RangeClusteringPenalty(*stats_it->second,
                                                         range_key_slot));
          }
          // Read the estimate before the move: designated initializers are
          // evaluated in declaration order, so `.plan = std::move(...)` runs
          // before a later member's initializer would touch the plan.
          const auto lookup_rows =
              static_cast<double>(lookup_plan->EmitRowCount());
          candidates.push_back(PlanAlternative{.plan = std::move(lookup_plan),
                                               .local_cost = local_cost,
                                               .estimated_rows = lookup_rows});
        }
      }
    }
  }
  return candidates;
}

std::vector<PlanAlternative> LateMaterializationAlternative(
    const cascades::Memo& memo, cascades::GroupId right_group,
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, const cascades::PhysicalProperties& required,
    const cascades::RuleContext& context) {
  if (!condition || !*condition || required.require_row_position ||
      memo.Get(right_group).filter) {
    return {};
  }
  const Table* right_table = right.plan->ScanSource();
  if (right_table == nullptr || left.plan == nullptr || right.plan == nullptr) {
    return {};
  }
  const Schema& left_schema = left.plan->GetSchema();
  const Schema& right_schema = right.plan->GetSchema();
  const Schema& physical_schema = right_table->GetSchema();
  const auto has_array = [](const Schema& schema) {
    return std::ranges::any_of(
        std::views::iota(size_t{0}, schema.ColumnCount()),
        [&schema](size_t index) {
          return schema.GetColumn(index).Type() == ValueType::kArray;
        });
  };
  if (has_array(left_schema) || has_array(right_schema) ||
      has_array(physical_schema)) {
    return {};
  }
  std::vector<std::pair<ColumnName, ColumnName>> equalities;
  for (const Expression& conjunct : SplitConjuncts(*condition)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() == BinaryOperation::kIsNotDistinctFrom) {
      return {};
    }
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    ColumnName left_column;
    ColumnName right_column;
    if (left_schema.Offset(lhs) >= 0 && right_schema.Offset(rhs) >= 0) {
      left_column = lhs;
      right_column = rhs;
    } else if (left_schema.Offset(rhs) >= 0 && right_schema.Offset(lhs) >= 0) {
      left_column = rhs;
      right_column = lhs;
    } else {
      continue;
    }
    if (!SamePhysicalJoinType(left_schema, left_column, right_schema,
                              right_column)) {
      return {};
    }
    equalities.emplace_back(std::move(left_column), std::move(right_column));
  }
  if (equalities.empty() || ResidualContainsQuery(*condition) ||
      !DeterministicForPushdown(*condition)) {
    return {};
  }
  for (const ColumnName& column : (*condition)->TouchedColumns()) {
    if (left_schema.Offset(column) < 0 && right_schema.Offset(column) < 0) {
      return {};
    }
  }
  const double left_rows = left.estimated_rows;
  const double right_rows = right.estimated_rows;
  const bool small_build_side =
      right_rows <= 0.0 || left_rows >= right_rows * 4.0;
  if (!small_build_side && right_schema.ColumnCount() < 6) {
    return {};
  }
  std::string right_relation;
  for (const auto& [relation, table] : context.tables) {
    if (table.get() == right_table) {
      right_relation = relation;
      break;
    }
  }
  const auto statistics = context.statistics.find(right_relation);
  if (statistics == context.statistics.end()) {
    return {};
  }
  std::vector<ColumnName> left_columns;
  std::vector<ColumnName> right_columns;
  for (const auto& [left_column, right_column] : equalities) {
    left_columns.push_back(left_column);
    right_columns.push_back(right_column);
  }
  const auto to_physical = [&](const ColumnName& column) {
    const int offset = physical_schema.Offset(ColumnName("", column.name));
    return offset < 0
               ? ColumnName("", column.name)
               : physical_schema.GetColumn(static_cast<size_t>(offset)).Name();
  };
  std::vector<ColumnName> physical_right_columns;
  physical_right_columns.reserve(right_columns.size());
  for (const ColumnName& column : right_columns) {
    physical_right_columns.push_back(to_physical(column));
  }
  for (size_t index_offset = 0; index_offset < right_table->IndexCount();
       ++index_offset) {
    const Index& index = right_table->GetIndex(index_offset);
    if (index.RetainsDeletedEntries()) {
      continue;
    }
    const std::optional<LookupSeek> probe = DeriveLookupSeek(
        index, physical_schema, physical_right_columns, left_columns);
    if (!probe || probe->right_columns.empty() ||
        right_schema.ColumnCount() <= probe->right_columns.size()) {
      continue;
    }
    bool supported = true;
    for (size_t i = 0; i < probe->right_columns.size(); ++i) {
      const int offset = physical_schema.Offset(probe->right_columns[i]);
      const int left_offset = left_schema.Offset(probe->left_columns[i]);
      if (offset < 0 || left_offset < 0 ||
          !SupportsLateMaterializationKeyType(
              physical_schema.GetColumn(static_cast<size_t>(offset)).Type()) ||
          !SupportsLateMaterializationKeyType(
              left_schema.GetColumn(static_cast<size_t>(left_offset)).Type()) ||
          physical_schema.GetColumn(static_cast<size_t>(offset)).IsUnsigned() ||
          left_schema.GetColumn(static_cast<size_t>(left_offset))
              .IsUnsigned()) {
        supported = false;
        break;
      }
    }
    if (!supported) {
      continue;
    }
    std::vector<slot_t> right_projection;
    right_projection.reserve(right_schema.ColumnCount());
    for (size_t i = 0; i < right_schema.ColumnCount(); ++i) {
      const int offset = physical_schema.Offset(
          ColumnName("", right_schema.GetColumn(i).Name().name));
      if (offset < 0) {
        supported = false;
        break;
      }
      right_projection.push_back(static_cast<slot_t>(offset));
    }
    if (!supported) {
      continue;
    }
    double equi_estimate = JoinCardinality(left, right, equalities, *condition);
    const size_t estimated_rows = RowEstimate(equi_estimate);
    const bool unique_winner =
        index.StoresSingleValue() &&
        probe->right_columns.size() == index.sc_.key_.size();
    const Schema declared_output = left_schema + right_schema;
    Plan late = std::make_shared<LateMaterializationJoinPlan>(
        left.plan, probe->left_columns, *right_table, index,
        probe->right_columns, *statistics->second, *condition, declared_output,
        std::move(right_projection), unique_winner, estimated_rows);
    double local_cost = (left_rows * 0.5) + (equi_estimate * 0.5);
    ApplyLimitHint(late, required, context, 1.0, &local_cost, &equi_estimate);
    return {PlanAlternative{.plan = std::move(late),
                            .local_cost = local_cost,
                            .estimated_rows = equi_estimate}};
  }
  return {};
}

struct ZigZagJoinSpec {
  ColumnName left;
  ColumnName right;
  BinaryOperation operation;
  Expression comparison;
};

bool IsZigZagComparison(BinaryOperation operation) {
  return operation == BinaryOperation::kEquals || IsBandComparison(operation);
}

bool IsZigZagKeyNotNull(const Expression& conjunct, const ColumnName& left,
                        const ColumnName& right) {
  if (!conjunct || conjunct->Type() != TypeTag::kUnaryExp) {
    return false;
  }
  const auto& unary = conjunct->AsUnaryExpression();
  if (unary.Op() != UnaryOperation::kIsNotNull || !unary.Child() ||
      unary.Child()->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const ColumnName& column = unary.Child()->AsColumnValue().GetColumnName();
  return column == left || column == right;
}

std::optional<ZigZagJoinSpec> TryZigZagJoinSpec(const Expression& predicate,
                                                const Plan& left,
                                                const Plan& right) {
  const std::vector<Expression> conjuncts = SplitConjuncts(predicate);
  std::optional<ZigZagJoinSpec> spec;
  for (const Expression& conjunct : conjuncts) {
    if (!conjunct) {
      return std::nullopt;
    }
    if (conjunct->Type() == TypeTag::kUnaryExp) {
      continue;
    }
    if (conjunct->Type() != TypeTag::kBinaryExp || spec) {
      return std::nullopt;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (!IsZigZagComparison(binary.Op()) || !binary.Left() || !binary.Right() ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    const int left_lhs = left->GetSchema().Offset(lhs);
    const int left_rhs = left->GetSchema().Offset(rhs);
    const int right_lhs = right->GetSchema().Offset(lhs);
    const int right_rhs = right->GetSchema().Offset(rhs);
    if (left_lhs >= 0 && right_rhs >= 0 && left_rhs < 0 && right_lhs < 0) {
      spec = ZigZagJoinSpec{.left = lhs,
                            .right = rhs,
                            .operation = binary.Op(),
                            .comparison = conjunct};
    } else if (left_rhs >= 0 && right_lhs >= 0 && left_lhs < 0 &&
               right_rhs < 0) {
      spec = ZigZagJoinSpec{.left = rhs,
                            .right = lhs,
                            .operation = MirrorRangeOperation(binary.Op()),
                            .comparison = conjunct};
    } else {
      return std::nullopt;
    }
  }
  if (!spec) {
    return std::nullopt;
  }
  for (const Expression& conjunct : conjuncts) {
    if (conjunct == spec->comparison ||
        IsZigZagKeyNotNull(conjunct, spec->left, spec->right)) {
      continue;
    }
    return std::nullopt;
  }
  const int left_offset = left->GetSchema().Offset(spec->left);
  const int right_offset = right->GetSchema().Offset(spec->right);
  if (left_offset < 0 || right_offset < 0) {
    return std::nullopt;
  }
  const Column& left_column =
      left->GetSchema().GetColumn(static_cast<size_t>(left_offset));
  const Column& right_column =
      right->GetSchema().GetColumn(static_cast<size_t>(right_offset));
  if (left_column.Type() != ValueType::kInt64 ||
      right_column.Type() != ValueType::kInt64 || left_column.IsUnsigned() ||
      right_column.IsUnsigned()) {
    return std::nullopt;
  }
  return spec;
}

bool IsOrderedIndexPlan(const Plan& plan, const ColumnName& key) {
  const bool index_access =
      dynamic_cast<const IndexScanPlan*>(plan.get()) != nullptr ||
      dynamic_cast<const IndexOnlyScanPlan*>(plan.get()) != nullptr ||
      dynamic_cast<const IndexSkipScanPlan*>(plan.get()) != nullptr;
  return index_access && plan->IsOrderedBy({ColumnValueExp(key)}, {true});
}

std::vector<PlanAlternative> ZigZagJoinAlternative(
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right) {
  if (!condition || !*condition) {
    return {};
  }
  const auto spec = TryZigZagJoinSpec(*condition, left.plan, right.plan);
  if (!spec || !IsOrderedIndexPlan(left.plan, spec->left) ||
      !IsOrderedIndexPlan(right.plan, spec->right)) {
    return {};
  }

  double estimated_rows = 0.0;
  if (spec->operation == BinaryOperation::kEquals) {
    estimated_rows =
        JoinCardinality(left, right,
                        std::vector<std::pair<ColumnName, ColumnName>>{
                            {spec->left, spec->right}});
  } else {
    estimated_rows = left.estimated_rows * right.estimated_rows * 0.5;
  }
  const size_t output_rows = RowEstimate(estimated_rows);
  Plan plan = std::make_shared<ZigZagJoinPlan>(
      left.plan, spec->left, right.plan, spec->right, spec->operation,
      *condition, output_rows);
  const double local_cost = 0.8 * (left.estimated_rows + right.estimated_rows);
  return {PlanAlternative{.plan = std::move(plan),
                          .local_cost = local_cost,
                          .estimated_rows = static_cast<double>(output_rows)}};
}

struct BandJoinSpec {
  ColumnName left;
  ColumnName right;
  BinaryOperation comparison;
};

std::optional<BandJoinSpec> TryBandJoinSpec(const Expression& predicate,
                                            const Plan& left,
                                            const Plan& right) {
  const std::vector<Expression> conjuncts = SplitConjuncts(predicate);
  if (conjuncts.size() != 1 || !conjuncts.front() ||
      conjuncts.front()->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const auto& binary = conjuncts.front()->AsBinaryExpression();
  if (!IsBandComparison(binary.Op()) || !binary.Left() || !binary.Right() ||
      binary.Left()->Type() != TypeTag::kColumnValue ||
      binary.Right()->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
  const int left_lhs = left->GetSchema().Offset(lhs);
  const int left_rhs = left->GetSchema().Offset(rhs);
  const int right_lhs = right->GetSchema().Offset(lhs);
  const int right_rhs = right->GetSchema().Offset(rhs);

  ColumnName left_column;
  ColumnName right_column;
  BinaryOperation comparison = binary.Op();
  if (left_lhs >= 0 && right_rhs >= 0 && left_rhs < 0 && right_lhs < 0) {
    left_column = lhs;
    right_column = rhs;
  } else if (left_rhs >= 0 && right_lhs >= 0 && left_lhs < 0 && right_rhs < 0) {
    left_column = rhs;
    right_column = lhs;
    // The predicate reads `lhs <op> rhs` but this join's children are in the
    // opposite order, so the spec must describe `left_column <comparison>
    // right_column` -- the form the executor's candidate range is computed
    // from. Keeping the written operator here would make the range enumerate
    // the mirrored condition (e.g. treat `f.a <= d.k` as `d.k <= f.a` after
    // join_commutativity swapped the children), so the range and the residual
    // re-check would disagree and the join would silently drop rows.
    comparison = MirrorRangeOperation(binary.Op());
  } else {
    return std::nullopt;
  }

  const int left_offset = left->GetSchema().Offset(left_column);
  const int right_offset = right->GetSchema().Offset(right_column);
  if (left_offset < 0 || right_offset < 0 ||
      left->GetSchema().GetColumn(static_cast<size_t>(left_offset)).Type() !=
          right->GetSchema()
              .GetColumn(static_cast<size_t>(right_offset))
              .Type()) {
    return std::nullopt;
  }
  const ValueType type =
      left->GetSchema().GetColumn(static_cast<size_t>(left_offset)).Type();
  if ((type != ValueType::kInt64 && type != ValueType::kDate) ||
      left->GetSchema()
          .GetColumn(static_cast<size_t>(left_offset))
          .IsUnsigned() ||
      right->GetSchema()
          .GetColumn(static_cast<size_t>(right_offset))
          .IsUnsigned()) {
    return std::nullopt;
  }
  return BandJoinSpec{.left = std::move(left_column),
                      .right = std::move(right_column),
                      .comparison = comparison};
}

std::vector<PlanAlternative> BandJoinAlternative(
    const cascades::Memo& memo, cascades::GroupId right_group,
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, JoinKind kind) {
  if (!condition || !*condition) {
    return {};
  }
  const auto spec = TryBandJoinSpec(*condition, left.plan, right.plan);
  if (!spec) {
    return {};
  }

  const Expression left_order = ColumnValueExp(spec->left);
  const Expression right_order = ColumnValueExp(spec->right);
  const std::vector<bool> ascending{true};
  const bool left_ordered = left.plan->IsOrderedBy({left_order}, ascending);
  const bool right_ordered = right.plan->IsOrderedBy({right_order}, ascending);
  const auto sort_cost = [](double rows) {
    return rows * std::log2(std::max(2.0, rows));
  };

  Plan left_plan = left.plan;
  Plan right_plan = right.plan;
  double local_cost = left.estimated_rows + right.estimated_rows;
  if (!left_ordered) {
    left_plan =
        MakeSortPlan(std::move(left_plan),
                     std::vector<SortKey>{SortKey{.expression = left_order,
                                                  .ascending = true,
                                                  .nulls_first = std::nullopt}},
                     left.estimated_rows);
    local_cost += sort_cost(left.estimated_rows);
  }
  if (!right_ordered) {
    right_plan =
        MakeSortPlan(std::move(right_plan),
                     std::vector<SortKey>{SortKey{.expression = right_order,
                                                  .ascending = true,
                                                  .nulls_first = std::nullopt}},
                     right.estimated_rows);
    local_cost += sort_cost(right.estimated_rows);
  }

  const double match_estimate =
      left.estimated_rows * right.estimated_rows * 0.5;
  const size_t match_rows = RowEstimate(match_estimate);
  const size_t output_rows = EstimatedJoinRowCount(
      kind, static_cast<size_t>(left.estimated_rows),
      static_cast<size_t>(right.estimated_rows), match_rows);
  local_cost += static_cast<double>(output_rows);
  if (left_ordered && right_ordered) {
    local_cost *= 0.8;
  }
  Plan band = std::make_shared<BandJoinPlan>(
      std::move(left_plan), std::vector<ColumnName>{spec->left},
      std::move(right_plan), std::vector<ColumnName>{spec->right}, kind,
      spec->comparison, *condition, output_rows);
  (void)memo;
  (void)right_group;
  return {PlanAlternative{.plan = std::move(band),
                          .local_cost = local_cost,
                          .estimated_rows = static_cast<double>(output_rows)}};
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
  if (HasNullSafeCrossJoinEquality(predicate, left.plan, right.plan)) {
    return {};
  }
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
    ColumnName left_column;
    ColumnName right_column;
    if (left.plan->GetSchema().Offset(lhs) >= 0 &&
        right.plan->GetSchema().Offset(rhs) >= 0) {
      left_column = lhs;
      right_column = rhs;
    } else if (left.plan->GetSchema().Offset(rhs) >= 0 &&
               right.plan->GetSchema().Offset(lhs) >= 0) {
      left_column = rhs;
      right_column = lhs;
    } else {
      continue;
    }
    if (!SamePhysicalJoinType(left.plan->GetSchema(), left_column,
                              right.plan->GetSchema(), right_column)) {
      continue;
    }
    left_columns.push_back(left_column);
    right_columns.push_back(right_column);
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
    return rows * std::log2(std::max(2.0, rows));
  };
  Plan left_plan = left.plan;
  Plan right_plan = right.plan;
  double local_cost = left.estimated_rows + right.estimated_rows;
  constexpr double kOrderedMergeJoinRowsThreshold = 1024.0;
  if (left_ordered && right_ordered &&
      std::max(left.estimated_rows, right.estimated_rows) >=
          kOrderedMergeJoinRowsThreshold) {
    local_cost *= 0.8;
  }
  if (!left_ordered) {
    std::vector<SortKey> keys;
    keys.reserve(left_columns.size());
    for (const ColumnName& column : left_columns) {
      keys.push_back(SortKey{.expression = ColumnValueExp(column),
                             .ascending = true,
                             .nulls_first = std::nullopt});
    }
    left_plan = MakeSortPlan(std::move(left_plan), std::move(keys),
                             left.estimated_rows);
    local_cost += sort_cost(left.estimated_rows);
  }
  if (!right_ordered) {
    std::vector<SortKey> keys;
    keys.reserve(right_columns.size());
    for (const ColumnName& column : right_columns) {
      keys.push_back(SortKey{.expression = ColumnValueExp(column),
                             .ascending = true,
                             .nulls_first = std::nullopt});
    }
    right_plan = MakeSortPlan(std::move(right_plan), std::move(keys),
                              right.estimated_rows);
    local_cost += sort_cost(right.estimated_rows);
  }
  auto physical_kind = JoinKind{};
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
  std::vector<std::pair<ColumnName, ColumnName>> equality_pairs;
  equality_pairs.reserve(left_columns.size());
  for (size_t i = 0; i < left_columns.size(); ++i) {
    equality_pairs.emplace_back(left_columns[i], right_columns[i]);
  }
  const Expression estimate_residual =
      kind == JoinAlternativeKind::kInner
          ? (residual_conjuncts.empty() ? nullptr
                                        : CombineConjuncts(residual_conjuncts))
          : merge_residual;
  const double equi_estimate =
      JoinCardinality(left, right, equality_pairs, estimate_residual);
  Plan merge = std::make_shared<MergeJoinPlan>(
      std::move(left_plan), left_columns, std::move(right_plan), right_columns,
      physical_kind, std::move(merge_residual), RowEstimate(equi_estimate));
  if (!residual_conjuncts.empty()) {
    const TableStatistics merge_stats = merge->GetStats();
    merge = std::make_shared<SelectionPlan>(
        std::move(merge), CombineConjuncts(residual_conjuncts), merge_stats);
  }
  const auto estimated_rows = static_cast<double>(merge->EmitRowCount());
  (void)memo;
  (void)right_group;
  (void)context;
  return {PlanAlternative{.plan = std::move(merge),
                          .local_cost = local_cost,
                          .estimated_rows = estimated_rows}};
}

// True when the predicate holds at least one equi column-pair spanning the
// two children (mirrors the equi detection in JoinAlternatives, including
// IS NOT DISTINCT FROM). Hash/merge implementations own those shapes; the
// blocked nested loop only competes where no equi pair exists.
bool HasEquiColumnPair(const Expression& predicate, const Plan& left,
                       const Plan& right) {
  return std::ranges::any_of(
      SplitConjuncts(predicate), [&left, &right](const Expression& conjunct) {
        if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = conjunct->AsBinaryExpression();
        const bool null_safe =
            binary.Op() == BinaryOperation::kIsNotDistinctFrom;
        if ((binary.Op() != BinaryOperation::kEquals && !null_safe) ||
            binary.Left()->Type() != TypeTag::kColumnValue ||
            binary.Right()->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
        return (left->GetSchema().Offset(lhs) >= 0 &&
                right->GetSchema().Offset(rhs) >= 0) ||
               (left->GetSchema().Offset(rhs) >= 0 &&
                right->GetSchema().Offset(lhs) >= 0);
      });
}

bool HasCatalogTypeCompatibleEqui(const cascades::Memo& memo,
                                  const Expression& predicate) {
  const auto column_type =
      [&](const ColumnName& column) -> std::optional<ValueType> {
    if (column.schema.empty()) {
      return std::nullopt;
    }
    const auto found = memo.GetTableSchemas().find(column.schema);
    if (found == memo.GetTableSchemas().end()) {
      return std::nullopt;
    }
    const int offset = found->second.Offset(ColumnName("", column.name));
    if (offset < 0) {
      return std::nullopt;
    }
    return found->second.GetColumn(static_cast<size_t>(offset)).Type();
  };
  return std::ranges::any_of(
      SplitConjuncts(predicate), [&](const Expression& conjunct) {
        if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = conjunct->AsBinaryExpression();
        if ((binary.Op() != BinaryOperation::kEquals &&
             binary.Op() != BinaryOperation::kIsNotDistinctFrom) ||
            binary.Left()->Type() != TypeTag::kColumnValue ||
            binary.Right()->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const auto left =
            column_type(binary.Left()->AsColumnValue().GetColumnName());
        const auto right =
            column_type(binary.Right()->AsColumnValue().GetColumnName());
        return left && right && *left == *right;
      });
}

// Shared builder for the batch_nested_loop family: a cross-shaped
// ProductPlan carrying the full predicate, marked for the blocked executor.
// The gate (usable predicate, no equi pair) keeps hash/merge shapes — and
// single-key equi NOT IN, the only source of null-aware anti joins — out of
// reach, so the rule only fires where hash/merge offer nothing:
// - inner: the blocked kernel has a lower per-pair cost than the tuple
//   nested loop, so ordinary SQL can select it.
// - semi/anti/full/right-outer: no tuple implementation exists, so the
//   blocked form is new capability (previously relational fallback).
// Semi/anti/outer carry no Selection wrapper: the executor null-pads
// unmatched rows (outer) and reduces to the probe side (semi/anti) while
// pairing, and a Selection above would filter those rows back out.
std::vector<PlanAlternative> BatchNestedLoopFor(
    const std::optional<Expression>& condition, const BestPlan& left,
    const BestPlan& right, const std::optional<JoinKind>& kind,
    bool wrap_selection) {
  if (!condition.has_value() || !*condition) {
    return {};
  }
  const Expression& predicate = *condition;
  if (!wrap_selection && predicate->Type() == TypeTag::kConstantValue) {
    // ON FALSE/NULL over an outer/semi/anti shape is short-circuited by the
    // logical layer; like outer_nested_loop, leave those shapes out. (ON
    // TRUE stays: the blocked executor pads correctly around it.)
    const Value constant = predicate->AsConstantValue().GetValue();
    if (constant.IsNull() || !constant.Truthy()) {
      return {};
    }
  }
  if (HasEquiColumnPair(predicate, left.plan, right.plan)) {
    return {};
  }
  const double l_rows = left.estimated_rows;
  const double r_rows = right.estimated_rows;
  Plan join;
  if (!kind.has_value()) {
    join = std::make_shared<ProductPlan>(left.plan, right.plan);
  } else if (IsSemiJoinKind(*kind) || IsAntiJoinKind(*kind)) {
    join = std::make_shared<ProductPlan>(left.plan, std::vector<ColumnName>{},
                                         right.plan, std::vector<ColumnName>{},
                                         HashJoinMode::kInMemory, *kind);
  } else {
    join = std::make_shared<ProductPlan>(left.plan, right.plan, *kind,
                                         RowEstimate(l_rows * r_rows));
  }
  std::static_pointer_cast<ProductPlan>(join)->SetJoinNotes({}, predicate);
  std::static_pointer_cast<ProductPlan>(join)->PreferBatchNestedLoop();
  if (wrap_selection) {
    join = std::make_shared<SelectionPlan>(join, predicate, join->GetStats());
  }
  const auto estimated_rows = static_cast<double>(join->EmitRowCount());
  return {
      PlanAlternative{.plan = std::move(join),
                      .local_cost = (l_rows * r_rows * 0.8) + l_rows + r_rows,
                      .estimated_rows = estimated_rows}};
}

// What the two sort shapes agree on, so the rules that pick between them differ
// only in the memory decision. The ORDER BY contract (keys, directions, NULL
// placement, collation) and the elision check are shared verbatim: eliding a
// sort is only sound when the child delivers the order under the collation the
// parent asked for, because a case-insensitive ORDER BY is not a binary order.
struct SortShapeInput {
  Plan child;
  std::vector<SortKey> keys;
  double rows{0.0};
  bool elided{false};
};

// Nullopt when the shape does not apply to this node at all.
std::optional<SortShapeInput> PrepareSortShape(
    const cascades::LogicalExpression& logical,
    const PhysicalProperties& required, const std::vector<BestPlan>& children) {
  if (children.size() != 1 ||
      logical.target_list.size() != logical.sort_ascending.size()) {
    return std::nullopt;
  }
  SortShapeInput input;
  input.rows = children[0].estimated_rows;
  input.keys.reserve(logical.target_list.size());
  std::vector<Expression> expressions;
  expressions.reserve(logical.target_list.size());
  for (size_t i = 0; i < logical.target_list.size(); ++i) {
    expressions.push_back(logical.target_list[i].expression);
    input.keys.push_back(SortKey{
        .expression = logical.target_list[i].expression,
        .ascending = logical.sort_ascending[i],
        .nulls_first = i < logical.sort_nulls_first.size()
                           ? logical.sort_nulls_first[i]
                           : std::nullopt,
        .collation = CollationOfExpression(logical.target_list[i].expression)});
  }
  if (cascades::SatisfiesOrderingCollation(children[0].plan, required) &&
      children[0].plan->IsOrderedBy(expressions, logical.sort_ascending,
                                    logical.sort_nulls_first)) {
    input.child = children[0].plan;
    input.elided = true;
    return input;
  }
  input.child = children[0].plan;
  return input;
}

// In-memory comparison cost, shared by both shapes.
double SortComparisonCost(double rows) {
  return rows * std::log2(std::max(2.0, rows));
}

// The two shapes are the same ORDER BY, so the choice between them is a
// memory one: the buffered sort while the estimated input fits the memory
// budget, the external merge sort above it. Each rule owns one side of that
// split, so exactly one of them applies to a node and the selection is a
// deterministic function of the estimate (which the contract tests pin down).
// Both keep the operator's own spill selection as the last resort, so a
// mis-estimated shape still returns identical rows in identical order.
std::vector<PlanAlternative> ImplementBufferedSort(
    cascades::GroupId /*group*/, const cascades::Memo& /*memo*/,
    const cascades::Bindings& /*bindings*/,
    const cascades::LogicalExpression& logical,
    const std::vector<BestPlan>& children, const PhysicalProperties& required,
    const cascades::RuleContext& /*context*/) {
  const std::optional<SortShapeInput> prepared =
      PrepareSortShape(logical, required, children);
  if (!prepared) {
    return {};
  }
  if (prepared->elided) {
    return {PlanAlternative{.plan = prepared->child,
                            .local_cost = 0,
                            .estimated_rows = prepared->rows}};
  }
  // Only the buffered side of the split: above the row capacity the estimate
  // belongs to the external merge shape, which offers the alternative instead.
  const SortMemoryPolicy memory;
  if (SortStrategyForRows(prepared->rows, memory) != SortStrategy::kBuffered) {
    return {};
  }
  Plan sort =
      MakeSortPlan(prepared->child, prepared->keys, prepared->rows, memory);
  return {PlanAlternative{.plan = std::move(sort),
                          .local_cost = SortComparisonCost(prepared->rows),
                          .estimated_rows = prepared->rows}};
}

std::vector<PlanAlternative> ImplementExternalMergeSort(
    cascades::GroupId /*group*/, const cascades::Memo& /*memo*/,
    const cascades::Bindings& /*bindings*/,
    const cascades::LogicalExpression& logical,
    const std::vector<BestPlan>& children, const PhysicalProperties& required,
    const cascades::RuleContext& /*context*/) {
  const std::optional<SortShapeInput> prepared =
      PrepareSortShape(logical, required, children);
  if (!prepared || prepared->elided) {
    return {};
  }
  // Only the spill side of the split, and never a second copy of the elision
  // the buffered rule already offered.
  const SortMemoryPolicy memory;
  if (SortStrategyForRows(prepared->rows, memory) !=
      SortStrategy::kExternalMerge) {
    return {};
  }
  Plan sort =
      MakeSortPlan(prepared->child, prepared->keys, prepared->rows, memory);
  // Writing and re-merging the runs is what the buffered shape avoids, so the
  // shape is priced with the same n log n term plus the spill the estimate
  // says is unavoidable (MemoryBudget in the same unit as the policy).
  cascades::MemoryBudget budget;
  budget.max_memory_bytes = static_cast<double>(memory.memory_budget_bytes);
  budget.row_size_bytes = static_cast<double>(kSortEstimatedRowBytes);
  const double cost =
      SortComparisonCost(prepared->rows) +
      cascades::EstimateMemorySpillCost(cascades::OperatorCostKind::kSort,
                                        prepared->rows, budget);
  return {PlanAlternative{.plan = std::move(sort),
                          .local_cost = cost,
                          .estimated_rows = prepared->rows}};
}

// The optimizer's memory model in the unit the aggregate policy counts in
// (bytes over groups of the estimated row width), so the rule's spill decision
// and the operator's accounting speak the same language.
cascades::MemoryBudget AggregateMemoryBudget(
    const AggregateMemoryPolicy& memory = AggregateMemoryPolicy{}) {
  cascades::MemoryBudget budget;
  budget.max_memory_bytes = static_cast<double>(memory.memory_budget_bytes);
  budget.row_size_bytes = static_cast<double>(kAggregateEstimatedRowBytes);
  return budget;
}

// Whether a child already delivers the group-key order the stream shape
// requires: the keys ascending with NULLs first, which is the engine default
// for an ascending key. Anything else (a different direction, a different key
// list, a missing order) leaves the shape unusable, because folding an
// unordered input would silently produce an order it never had.
bool ChildDeliversGroupKeyOrder(const BestPlan& child,
                                const std::vector<Expression>& keys) {
  if (child.plan == nullptr || keys.empty()) {
    return false;
  }
  std::vector<bool> ascending(keys.size(), true);
  std::vector<std::optional<bool>> nulls_first(keys.size(), true);
  return child.plan->IsOrderedBy(keys, ascending, nulls_first);
}

}  // namespace

StatusOr<Plan> OptimizeMinMaxIndex(
    const QueryData& query, const Expression& predicate,
    const std::vector<NamedExpression>& projection_items,
    const cascades::RuleContext& context) {
  if (query.from_.size() != 1 || projection_items.size() != 1 ||
      query.distinct_ || query.require_row_position_ ||
      (predicate && (predicate->Type() != TypeTag::kConstantValue ||
                     !predicate->AsConstantValue().GetValue().Truthy()))) {
    return Status::kNotImplemented;
  }
  const std::string& relation = query.from_.front();
  if (!context.tables.contains(relation) ||
      !context.statistics.contains(relation)) {
    return Status::kNotImplemented;
  }
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
      if (target_slot >= 0 && schema.GetColumn(static_cast<size_t>(target_slot))
                                      .GetConstraint()
                                      .ctype != Constraint::kNothing) {
        const auto& column_constraint =
            schema.GetColumn(static_cast<size_t>(target_slot)).GetConstraint();
        const bool non_null =
            column_constraint.ctype == Constraint::kNotNull ||
            column_constraint.ctype == Constraint::kPrimaryKey;
        if (non_null) {
          const TableStatistics& statistics = *context.statistics.at(relation);
          for (size_t index_offset = 0; index_offset < table.IndexCount();
               ++index_offset) {
            const Index& index = table.GetIndex(index_offset);
            if (index.sc_.key_.size() != 1 ||
                index.sc_.key_.front() != static_cast<slot_t>(target_slot) ||
                index.RetainsDeletedEntries()) {
              continue;
            }
            const ColumnName provided =
                schema.GetColumn(static_cast<size_t>(target_slot)).Name();
            Plan scan = std::make_shared<IndexOnlyScanPlan>(
                table, index, statistics, std::vector<Value>{},
                std::vector<Value>{}, is_min, ConstantValueExp(Value(true)),
                std::vector<ColumnName>{provided});
            Plan minmax = std::make_shared<MinMaxIndexPlan>(std::move(scan),
                                                            output, 0, is_max);
            return minmax;
          }
        }
      }
    }
  }
  return Status::kNotImplemented;
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
    // LSM / WiscKey scan: the kLsmScan node carries a caller-owned tree and
    // its row-count estimate; SQL does not produce the node (plan-level
    // callers and tests build the memo directly).
    built.Add(c::ImplementationRule(
        "lsm_scan", c::dsl::LsmScan(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (!children.empty() || logical.lsm_tree == nullptr) {
            return std::vector<PlanAlternative>{};
          }
          Plan plan = std::make_shared<LsmScanPlan>(*logical.lsm_tree,
                                                    logical.lsm_estimated_rows);
          const auto rows = static_cast<double>(logical.lsm_estimated_rows);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(plan),
                              .local_cost = rows,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kLsmScan));
    built.Add(c::ImplementationRule(
        "unnest", c::dsl::Unnest(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          Expression unnest_expr =
              logical.predicate ? *logical.predicate : nullptr;
          Plan plan = std::make_shared<UnnestPlan>(
              children[0].plan, std::move(unnest_expr), logical.unnest_alias,
              logical.offset_alias, logical.output_schema);
          const double rows = children[0].estimated_rows * 10.0;
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(plan),
                              .local_cost = children[0].estimated_rows * 1.0,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kUnnest));
    built.Add(c::ImplementationRule(
        "apply", c::Pattern::Op(c::LogicalOperator::kApply, {}),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.empty()) {
            return std::vector<PlanAlternative>{};
          }
          auto kind = static_cast<JoinKind>(logical.join_type);
          Expression predicate =
              logical.predicate ? *logical.predicate : nullptr;
          Plan plan;
          if (children.size() == 1) {
            plan = std::make_shared<ApplyPlan>(
                children[0].plan, logical.relational_statement, logical.table,
                std::move(predicate), kind, logical.output_schema);
          } else if (children.size() == 2) {
            plan = std::make_shared<ApplyPlan>(
                children[0].plan, children[1].plan,
                logical.relational_statement, logical.table,
                std::move(predicate), kind, logical.output_schema);
          } else {
            return std::vector<PlanAlternative>{};
          }
          const double rows = children[0].estimated_rows * 10.0;
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(plan),
                              .local_cost = children[0].estimated_rows * 2.0,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kApply));
    built.Add(c::ImplementationRule(
        "recursive_cte", c::Pattern::Op(c::LogicalOperator::kRecursiveCte, {}),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          Plan plan;
          std::optional<RecursiveDepthSpec> depth_spec;
          if (logical.depth_spec.has_value()) {
            depth_spec = logical.depth_spec;
          } else if (logical.depth_limit > 0) {
            depth_spec = RecursiveDepthSpec{
                .column = "depth",
                .lower = 0,
                .upper = static_cast<int64_t>(logical.depth_limit),
            };
          }
          if (children.empty()) {
            plan = std::make_shared<RecursiveCtePlan>(
                nullptr, logical.cte_name, logical.relational_statement,
                std::move(depth_spec), logical.output_schema);
          } else if (children.size() == 1) {
            plan = std::make_shared<RecursiveCtePlan>(
                children[0].plan, logical.cte_name,
                logical.relational_statement, std::move(depth_spec),
                logical.output_schema);
          } else if (children.size() == 2) {
            plan = std::make_shared<RecursiveCtePlan>(
                children[0].plan, children[1].plan, logical.cte_name,
                logical.relational_statement, std::move(depth_spec),
                logical.output_schema);
          } else {
            return std::vector<PlanAlternative>{};
          }
          const double rows =
              children.empty() ? 10.0 : children[0].estimated_rows * 10.0;
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(plan),
              .local_cost =
                  children.empty() ? 10.0 : children[0].estimated_rows * 2.0,
              .estimated_rows = rows}};
        },
        c::LogicalOperator::kRecursiveCte));
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
        "sample", cascades::dsl::Sample(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1 || !logical.sample_size) {
            return std::vector<PlanAlternative>{};
          }
          TableSampleSpec spec;
          spec.method = logical.sample_method;
          spec.unit = logical.sample_unit;
          spec.size = logical.sample_size;
          spec.repeatable_seed = logical.sample_seed;
          spec.partition_by = logical.partition_by;
          spec.with_weight = logical.sample_with_weight;
          spec.weight_alias = logical.sample_weight_alias;
          Plan sample =
              std::make_shared<SamplePlan>(children[0].plan, std::move(spec));
          const auto rows = static_cast<double>(sample->EmitRowCount());
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(sample),
                              .local_cost = children[0].estimated_rows,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kSample));
    built.Add(c::ImplementationRule(
        "projection", cascades::dsl::Projection(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          for (const auto& target : logical.target_list) {
            if (!target.expression) {
              return std::vector<PlanAlternative>{};
            }
            for (const auto& column : target.expression->TouchedColumns()) {
              if (children[0].plan->GetSchema().Offset(column) < 0) {
                return std::vector<PlanAlternative>{};
              }
            }
          }
          // Scan projection: when the child chain is a stack of row-shaping
          // wrappers (projections, filters) over a plain full scan, narrow
          // the scan to the columns the chain actually reads. Full-row decode
          // is the scan's dominant cost on wide tables and the relational
          // route has always decoded only the required columns; this carries
          // the same win into Cascades plans. Gates: no DML source
          // (head-image reads need every column), no peek compares or
          // max-rows (other scan shapes), no clustered-order claim (the
          // claiming column may not be emitted), and the scan must not
          // already be narrowed. The wrapper stack is rebuilt over the
          // narrowed scan because each wrapper caches its child-schema
          // derived output.
          Plan scan_input = children[0].plan;
          {
            std::vector<const ProjectionPlan*> projections;
            std::vector<const SelectionPlan*> filters;
            const FullScanPlan* scan = nullptr;
            std::set<slot_t> needed;
            const Schema* scan_schema = nullptr;
            const auto collect = [&](const Expression& expression) {
              if (!expression || scan_schema == nullptr) {
                return;
              }
              for (const ColumnName& column : expression->TouchedColumns()) {
                const int offset = scan_schema->Offset(column);
                if (offset >= 0) {
                  needed.insert(static_cast<slot_t>(offset));
                }
              }
            };
            Plan holder = children[0].plan;
            while (holder != nullptr) {
              if (const auto* projection =
                      dynamic_cast<const ProjectionPlan*>(holder.get())) {
                for (const auto& named : projection->Columns()) {
                  collect(named.expression);
                }
                projections.push_back(projection);
                holder = projection->GetSource();
              } else if (const auto* selection =
                             dynamic_cast<const SelectionPlan*>(holder.get())) {
                collect(selection->Predicate());
                filters.push_back(selection);
                holder = selection->Src();
              } else {
                scan = dynamic_cast<const FullScanPlan*>(holder.get());
                break;
              }
            }
            if (scan != nullptr && scan->CanNarrow() &&
                ScanProjectionEnabled()) {
              scan_schema = &scan->GetSchema();
              needed.clear();
              for (const auto* projection : projections) {
                for (const auto& named : projection->Columns()) {
                  collect(named.expression);
                }
              }
              for (const auto* selection : filters) {
                collect(selection->Predicate());
              }
              for (const auto& target : logical.target_list) {
                collect(target.expression);
              }
              if (!needed.empty() &&
                  needed.size() < scan->GetSchema().ColumnCount()) {
                Plan rebuilt = std::make_shared<FullScanPlan>(
                    *scan, std::vector<slot_t>(needed.begin(), needed.end()));
                // Rebuild the wrapper stack bottom-up over the narrowed scan.
                for (const auto& filter : std::ranges::reverse_view(filters)) {
                  rebuilt = std::make_shared<SelectionPlan>(
                      rebuilt, filter->Predicate(), rebuilt->GetStats());
                }
                for (const auto& projection :
                     std::ranges::reverse_view(projections)) {
                  rebuilt = std::make_shared<ProjectionPlan>(
                      rebuilt, projection->Columns());
                }
                scan_input = std::move(rebuilt);
              }
            }
          }
          Plan projection =
              RemoveIdentityProjection(scan_input, logical.target_list);
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
          const std::vector<Expression>& grouping_keys =
              logical.grouping_sets.empty() ? logical.partition_by
                                            : logical.grouping_sets;
          const bool has_aggregate =
              std::ranges::any_of(logical.target_list, [](const auto& item) {
                return ContainsAggregateExpression(item.expression);
              });
          if (!has_aggregate) {
            // A grouping-only alternative is executable as projected DISTINCT
            // when every grouping key survives, and every output is a key.
            if (grouping_keys.empty() || logical.predicate) {
              return std::vector<PlanAlternative>{};
            }
            const auto same = [](const Expression& l, const Expression& r) {
              return l && r && l->ToString() == r->ToString();
            };
            if (!std::ranges::all_of(
                    grouping_keys,
                    [&](const auto& key) {
                      return std::ranges::any_of(
                          logical.target_list, [&](const auto& target) {
                            return same(key, target.expression);
                          });
                    }) ||
                !std::ranges::all_of(
                    logical.target_list, [&](const auto& target) {
                      return std::ranges::any_of(
                          grouping_keys, [&](const auto& key) {
                            return same(key, target.expression);
                          });
                    })) {
              return std::vector<PlanAlternative>{};
            }
            Plan projection =
                RemoveIdentityProjection(children[0].plan, logical.target_list);
            Plan distinct = std::make_shared<DistinctPlan>(projection);
            return std::vector<PlanAlternative>{PlanAlternative{
                .plan = std::move(distinct),
                .local_cost = rows * 2.0,
                .estimated_rows =
                    EstimateDistinctRows(children[0].plan, grouping_keys)}};
          }
          const double output_rows =
              grouping_keys.empty()
                  ? 1.0
                  : EstimateDistinctRows(children[0].plan, grouping_keys);
          // The GROUP BY payload every shape receives: the keys (named after
          // the column they reference, so the projection above resolves them by
          // the name it asked for), the aggregate target list, the estimated
          // group count and the memory contract. Nothing here names an
          // algorithm.
          AggregatePayload payload;
          payload.group_keys.reserve(grouping_keys.size());
          for (size_t i = 0; i < grouping_keys.size(); ++i) {
            const Expression& key = grouping_keys[i];
            std::string name = "group_key_" + std::to_string(i);
            if (key && key->Type() == TypeTag::kColumnValue) {
              name = key->AsColumnValue().GetColumnName().name;
            }
            payload.group_keys.emplace_back(name, key);
          }
          payload.aggregates = logical.target_list;
          // A HAVING on the aggregation node itself is a predicate over the
          // group's output row (keys then aggregates), which is the scope every
          // shape filters in. A HAVING that survived the rewrite as a
          // kSelection above the node keeps that shape instead -- the two never
          // overlap.
          if (logical.predicate && *logical.predicate) {
            payload.having = *logical.predicate;
          }
          payload.estimated_groups = output_rows;
          payload.memory = AggregateMemoryPolicy{};

          // COUNT(column) evaluates an argument and checks NULL per row;
          // COUNT(*) only increments the accumulator. Without this work in
          // all three shape costs, the not-null rewrite always ties its
          // original and can never be selected by the memo.
          double count_argument_cost = 0.0;
          for (const auto& target : logical.target_list) {
            if (target.expression &&
                target.expression->Type() == TypeTag::kAggregateExp) {
              const auto& aggregate =
                  target.expression->AsAggregateExpression();
              if (aggregate.GetType() == AggregationType::kCount &&
                  aggregate.Child()) {
                count_argument_cost += rows * 0.1;
              }
            }
          }

          // HashAgg: one hash table over the group keys, valid over any input
          // order and the cheapest single pass. Above the group capacity the
          // table partitions its input and re-groups it per partition, so the
          // cost carries that spill exactly where the estimate says it happens.
          Plan hash =
              std::make_shared<HashAggregatePlan>(children[0].plan, payload);
          const double hash_cost =
              rows + cascades::EstimateMemorySpillCost(
                         cascades::OperatorCostKind::kSort, output_rows,
                         AggregateMemoryBudget());
          // SortAgg: sorted runs over the group keys, then one pass over the
          // equal runs. It materializes an ordering the hash shape does not, so
          // it is priced as a sort plus the accumulator pass and stays
          // selectable by rule disabling or a hint.
          Plan sort =
              std::make_shared<SortAggregatePlan>(children[0].plan, payload);
          const double sort_cost =
              (rows * std::log2(std::max(2.0, rows))) + rows;
          std::vector<PlanAlternative> agg_alternatives{
              PlanAlternative{.plan = std::move(hash),
                              .local_cost = hash_cost + count_argument_cost,
                              .estimated_rows = output_rows},
              PlanAlternative{.plan = std::move(sort),
                              .local_cost = sort_cost + count_argument_cost,
                              .estimated_rows = output_rows}};
          // stream_aggregate_if_sorted: the streaming shape needs no hash table
          // and no sort, so it is offered at a single-pass cost — but ONLY when
          // the input already arrives in group-key order, which is the order
          // contract it promises. Without keys there is exactly one group,
          // which makes the shape valid over any input order.
          if (grouping_keys.empty() ||
              ChildDeliversGroupKeyOrder(children[0], grouping_keys)) {
            Plan stream = std::make_shared<StreamAggregatePlan>(
                children[0].plan, payload);
            agg_alternatives.push_back(PlanAlternative{
                .plan = std::move(stream),
                .local_cost = (rows * 0.8) + count_argument_cost,
                .estimated_rows = output_rows});
          }
          return agg_alternatives;
        },
        c::LogicalOperator::kAggregation));
    // sort / sort_external_merge: the same logical ORDER BY, planned as one of
    // two memory shapes (see PrepareSortShape). The rules are exclusive halves
    // of one decision — buffered at or below the policy's row capacity, spill
    // runs above it — so exactly one of them offers an alternative.
    built.Add(c::ImplementationRule(
        "sort_incremental", cascades::dsl::Sort(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          const auto prepared = PrepareSortShape(logical, required, children);
          if (!prepared || prepared->elided || prepared->keys.size() < 2 ||
              !c::SatisfiesOrderingCollation(prepared->child, required)) {
            return std::vector<PlanAlternative>{};
          }
          for (size_t count = prepared->keys.size() - 1; count > 0; --count) {
            std::vector<Expression> expressions;
            std::vector<bool> directions;
            std::vector<std::optional<bool>> nulls;
            for (size_t i = 0; i < count; ++i) {
              expressions.push_back(prepared->keys[i].expression);
              directions.push_back(prepared->keys[i].ascending);
              nulls.push_back(prepared->keys[i].nulls_first);
            }
            if (!prepared->child->IsOrderedBy(expressions, directions, nulls)) {
              continue;
            }
            std::vector<SortKey> prefix(
                prepared->keys.begin(),
                prepared->keys.begin() + static_cast<std::ptrdiff_t>(count));
            std::vector<SortKey> suffix(
                prepared->keys.begin() + static_cast<std::ptrdiff_t>(count),
                prepared->keys.end());
            const double groups = std::max(
                1.0, EstimateDistinctRows(prepared->child, expressions));
            Plan plan = std::make_shared<IncrementalSortPlan>(
                prepared->child, std::move(prefix), std::move(suffix));
            return std::vector<PlanAlternative>{PlanAlternative{
                .plan = std::move(plan),
                .local_cost = prepared->rows *
                              std::log2(std::max(2.0, prepared->rows / groups)),
                .estimated_rows = prepared->rows}};
          }
          return std::vector<PlanAlternative>{};
        },
        c::LogicalOperator::kSort));
    built.Add(c::ImplementationRule("sort", cascades::dsl::Sort(),
                                    ImplementBufferedSort,
                                    c::LogicalOperator::kSort));
    built.Add(c::ImplementationRule(
        "sort_external_merge", cascades::dsl::Sort(),
        ImplementExternalMergeSort, c::LogicalOperator::kSort));
    // window: one WindowPlan per kWindow node. Window evaluation preserves
    // the row count; the local cost prices the internal partition ordering
    // like a sort (the executor delegates to the canonical evaluator, so
    // framing and NULL placement agree with the relational path by
    // construction).
    built.Add(c::ImplementationRule(
        "window", cascades::dsl::Window(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 1 || required.require_row_position ||
              logical.target_list.empty()) {
            return std::vector<PlanAlternative>{};
          }
          for (const NamedExpression& item : logical.target_list) {
            if (!item.expression ||
                item.expression->Type() != TypeTag::kWindowFunctionExp) {
              return std::vector<PlanAlternative>{};
            }
          }
          Plan window = std::make_shared<WindowPlan>(children[0].plan,
                                                     logical.target_list);
          const double rows = children[0].estimated_rows;
          const double cost = rows * std::log2(std::max(2.0, rows));
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(window),
                              .local_cost = cost,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kWindow));
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
            keys.push_back(TopNKey{
                .expression = logical.target_list[i].expression,
                .ascending = logical.sort_ascending[i],
                .nulls_first = i < logical.sort_nulls_first.size()
                                   ? logical.sort_nulls_first[i]
                                   : std::nullopt,
                .collation =
                    CollationOfExpression(logical.target_list[i].expression)});
          }
          std::vector<Expression> ordering;
          ordering.reserve(logical.target_list.size());
          for (const auto& key : logical.target_list) {
            ordering.push_back(key.expression);
          }
          if (!logical.with_ties &&
              children[0].plan->IsOrderedBy(ordering, logical.sort_ascending,
                                            logical.sort_nulls_first)) {
            Plan limit = std::make_shared<LimitPlan>(
                children[0].plan, logical.limit_count, logical.limit_offset);
            return std::vector<PlanAlternative>{PlanAlternative{
                .plan = std::move(limit),
                .local_cost = 0.0,
                .estimated_rows = LimitOutputRows(children[0].estimated_rows,
                                                  logical.limit_count,
                                                  logical.limit_offset)}};
          }
          Plan topn = std::make_shared<TopNPlan>(
              children[0].plan, std::move(keys), logical.limit_count,
              logical.limit_offset, logical.with_ties);
          const double rows = children[0].estimated_rows;
          const double capacity =
              std::min(rows, static_cast<double>(logical.limit_count) +
                                 static_cast<double>(logical.limit_offset));
          const double cost = logical.with_ties
                                  ? rows * std::log2(std::max(2.0, rows))
                                  : rows * std::log2(std::max(2.0, capacity));
          return std::vector<PlanAlternative>{PlanAlternative{
              .plan = std::move(topn),
              .local_cost = cost,
              .estimated_rows = LimitOutputRows(rows, logical.limit_count,
                                                logical.limit_offset)}};
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
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          if (children.size() != 1) {
            return std::vector<PlanAlternative>{};
          }
          std::vector<Expression> columns;
          columns.reserve(logical.target_list.empty()
                              ? children[0].plan->GetSchema().ColumnCount()
                              : logical.target_list.size());
          if (logical.target_list.empty()) {
            for (size_t i = 0; i < children[0].plan->GetSchema().ColumnCount();
                 ++i) {
              columns.push_back(ColumnValueExp(
                  children[0].plan->GetSchema().GetColumn(i).Name()));
            }
          } else {
            for (const NamedExpression& item : logical.target_list) {
              columns.push_back(item.expression);
            }
          }
          const double output_rows =
              EstimateDistinctRows(children[0].plan, columns);
          Plan distinct = std::make_shared<DistinctPlan>(children[0].plan);
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(distinct),
                              .local_cost = children[0].estimated_rows,
                              .estimated_rows = output_rows}};
        },
        c::LogicalOperator::kDistinct));
    built.Add(c::ImplementationRule(
        "sort_distinct", cascades::dsl::Distinct(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children, const PhysicalProperties&,
           const c::RuleContext&) {
          (void)logical;
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
            keys.push_back(SortKey{.expression = ColumnValueExp(column),
                                   .ascending = true,
                                   .nulls_first = std::nullopt});
          }
          Plan input = children[0].plan;
          // Adjacent-row comparison avoids building a hash set. Include this
          // advantage even when sorting is needed, so the ordered parent can
          // compare sort+dedup against hash+sort without an artificial tie.
          double cost = children[0].estimated_rows * 0.8;
          const std::vector<bool> ascending(ordering.size(), true);
          if (!input->IsOrderedBy(ordering, ascending)) {
            const double rows = children[0].estimated_rows;
            cost += rows * std::log2(std::max(2.0, rows));
            input = MakeSortPlan(std::move(input), std::move(keys), rows);
          }
          const double output_rows =
              EstimateDistinctRows(children[0].plan, ordering);
          Plan distinct = std::make_shared<SortDistinctPlan>(std::move(input));
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(distinct),
                              .local_cost = cost,
                              .estimated_rows = output_rows}};
        },
        c::LogicalOperator::kDistinct));
    // DISTINCT over a single-column covering index becomes a skip scan: the
    // executor jumps past duplicate index keys instead of hashing every row,
    // and the index order satisfies an ascending ORDER BY on that column.
    built.Add(c::ImplementationRule(
        "skip_scan_distinct", cascades::dsl::Distinct(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression&, const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          std::vector<PlanAlternative> alternatives;
          if (children.size() != 1 || required.require_row_position) {
            return alternatives;
          }
          // Accept both a bare index-only scan and a pure projection of the
          // distinct column over it (DISTINCT is planned above a projection
          // group).
          Plan scan_plan = children[0].plan;
          if (const auto* projection =
                  dynamic_cast<const ProjectionPlan*>(scan_plan.get())) {
            scan_plan = projection->GetSource();
          }
          const auto* index_only =
              dynamic_cast<const IndexOnlyScanPlan*>(scan_plan.get());
          if (index_only == nullptr) {
            return alternatives;
          }
          const Index& index = index_only->GetIndex();
          if (index.RetainsDeletedEntries() || index.sc_.key_.size() != 1 ||
              !index_only->BeginKey().empty() ||
              !index_only->EndKey().empty()) {
            return alternatives;
          }
          const Schema& scan_schema = index_only->GetSchema();
          if (scan_schema.ColumnCount() != 1) {
            return alternatives;
          }
          const ColumnName distinct_column = scan_schema.GetColumn(0).Name();
          const Table* table = index_only->ScanSource();
          const int offset = table->GetSchema().Offset(distinct_column);
          if (offset < 0 ||
              index.sc_.key_.front() != static_cast<slot_t>(offset)) {
            return alternatives;
          }
          std::vector<NamedExpression> select_items = index_only->SelectItems();
          if (select_items.empty()) {
            select_items.emplace_back(distinct_column.name,
                                      ColumnValueExp(distinct_column));
          }
          Plan skip_scan = std::make_shared<SkipScanDistinctPlan>(
              *table, index, index_only->GetStats(), index_only->IsAscending(),
              std::move(select_items), scan_schema);
          // IndexOnlyScan statistics are already projected: the base-table
          // slot is not a statistics offset. Include NULL as one distinct key.
          const double rows = index_only->GetStats().EstimateDistinctRows({0});
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(skip_scan),
                              .local_cost = rows,
                              .estimated_rows = rows}};
        },
        c::LogicalOperator::kDistinct));

    const auto implement_set_operation =
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext& context) {
          if (children.size() < 2) {
            return std::vector<PlanAlternative>{};
          }
          SetOperationKind operation{};
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
              order_keys.push_back(SortKey{.expression = order_expressions[i],
                                           .ascending = ascending[i],
                                           .nulls_first = std::nullopt});
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
              plan = MakeSortPlan(std::move(plan), std::move(child_keys),
                                  child.estimated_rows);
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
                keys.push_back(TopNKey{.expression = key.expression,
                                       .ascending = key.ascending,
                                       .nulls_first = key.nulls_first});
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
        "band_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<c::BestPlan>& children,
           const c::PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return BandJoinAlternative(memo, logical.children[1],
                                     logical.predicate, children[0],
                                     children[1], JoinKind::kInner);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "zigzag_join", Join(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<c::BestPlan>& children,
           const c::PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return ZigZagJoinAlternative(logical.predicate, children[0],
                                       children[1]);
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
              children[1], context, true, false, false, kind, true);
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
           const c::PhysicalProperties& required,
           const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position ||
              (memo.JoinGraphConnected() && memo.HasColumnEqualityConjunct())) {
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
        "late_materialization_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<c::BestPlan>& children,
           const c::PhysicalProperties& required,
           const c::RuleContext& context) {
          if (children.size() != 2) {
            return std::vector<PlanAlternative>{};
          }
          return LateMaterializationAlternative(memo, logical.children[1],
                                                logical.predicate, children[0],
                                                children[1], required, context);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "nested_loop_join", Join(),
        [](c::GroupId, const c::Memo& memo, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const c::PhysicalProperties& required,
           const c::RuleContext& context) {
          if (children.size() != 2 || required.require_row_position ||
              (logical.predicate && *logical.predicate &&
               HasEquiColumnPair(*logical.predicate, children[0].plan,
                                 children[1].plan) &&
               HasCatalogTypeCompatibleEqui(memo, *logical.predicate))) {
            return std::vector<PlanAlternative>{};
          }
          return JoinAlternatives(memo, logical.children[1], logical.predicate,
                                  children[0], children[1], context, false,
                                  false, true);
        },
        c::LogicalOperator::kJoin));

    // outer_nested_loop: LEFT OUTER join via block nested loop. Hash/merge
    // cover equi-outer joins; this rule carries non-equi (and small) outer
    // joins into cost-based planning instead of the heuristic fallback. The
    // full predicate rides in the plan node (evaluated per pair) and the
    // executor null-pads unmatched outer rows. RIGHT arrives normalized to
    // LEFT (right_to_left_outer_join); FULL stays on hash/merge/fallback
    // because the executor has no full-outer nested loop.
    built.Add(c::ImplementationRule(
        "outer_nested_loop", c::dsl::LeftOuterJoin(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position ||
              logical.operation != c::LogicalOperator::kOuterJoin ||
              logical.join_type != 0 || !logical.predicate ||
              !*logical.predicate) {
            return std::vector<PlanAlternative>{};
          }
          if ((*logical.predicate)->Type() == TypeTag::kConstantValue) {
            const Value constant =
                (*logical.predicate)->AsConstantValue().GetValue();
            if (!constant.IsNull() && constant.Truthy()) {
              // ON TRUE over an empty inner side must still pad; the plain
              // cross shape cannot prove non-emptiness, so leave it out.
              return std::vector<PlanAlternative>{};
            }
            if (constant.IsNull() ||
                (!constant.IsNull() && !constant.Truthy())) {
              // ON FALSE / NULL never matches: every outer row pads. That is
              // a scan-wide projection, not a join worth nesting; the logical
              // layer already short-circuits it.
              return std::vector<PlanAlternative>{};
            }
          }
          Plan join = std::make_shared<ProductPlan>(
              children[0].plan, children[1].plan, LeftOuterJoinKind(),
              RowEstimate(children[0].estimated_rows *
                          children[1].estimated_rows));
          // The executor evaluates the full predicate while pairing and
          // null-pads unmatched outer rows, so unlike the inner nested loop
          // no Selection wrapper may be added: it would filter the padded
          // rows back out (same reason JoinAlternatives refuses residuals
          // for non-inner hash joins).
          std::static_pointer_cast<ProductPlan>(join)->SetJoinNotes(
              {}, *logical.predicate);
          const auto estimate = static_cast<double>(join->EmitRowCount());
          const double local_cost =
              children[0].estimated_rows * children[1].estimated_rows;
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = std::move(join),
                              .local_cost = local_cost,
                              .estimated_rows = estimate}};
        },
        c::LogicalOperator::kOuterJoin));
    // batch_nested_loop family: non-equi joins for the blocked executor.
    // The gate (no equi column-pair) means hash/merge offer nothing here, so
    // these alternatives only compete with the tuple nested loop (inner,
    // exact tie broken by registration order) or fill shapes with no tuple
    // implementation at all (semi/anti/full/right-outer, previously
    // relational fallback). LEFT stays with outer_nested_loop to avoid a
    // second tie; null-aware anti never reaches the gate (single-key equi
    // NOT IN always carries an equi pair).
    built.Add(c::ImplementationRule(
        "batch_nested_loop", Join(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return BatchNestedLoopFor(logical.predicate, children[0], children[1],
                                    std::nullopt,
                                    /*wrap_selection=*/true);
        },
        c::LogicalOperator::kJoin));
    built.Add(c::ImplementationRule(
        "batch_nested_loop_semi", c::dsl::SemiJoin(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return BatchNestedLoopFor(logical.predicate, children[0], children[1],
                                    SemiJoinKind(),
                                    /*wrap_selection=*/false);
        },
        c::LogicalOperator::kSemiJoin));
    built.Add(c::ImplementationRule(
        "batch_nested_loop_anti", c::dsl::AntiJoin(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position) {
            return std::vector<PlanAlternative>{};
          }
          return BatchNestedLoopFor(logical.predicate, children[0], children[1],
                                    AntiJoinKind(),
                                    /*wrap_selection=*/false);
        },
        c::LogicalOperator::kAntiJoin));
    built.Add(c::ImplementationRule(
        "batch_nested_loop_outer", OuterJoin(),
        [](c::GroupId, const c::Memo&, const c::Bindings&,
           const c::LogicalExpression& logical,
           const std::vector<BestPlan>& children,
           const PhysicalProperties& required, const c::RuleContext&) {
          if (children.size() != 2 || required.require_row_position ||
              logical.operation != c::LogicalOperator::kOuterJoin ||
              (logical.join_type != 1 && logical.join_type != 2) ||
              !logical.predicate || !*logical.predicate) {
            return std::vector<PlanAlternative>{};
          }
          const JoinKind kind = logical.join_type == 1 ? RightOuterJoinKind()
                                                       : FullOuterJoinKind();
          return BatchNestedLoopFor(logical.predicate, children[0], children[1],
                                    kind,
                                    /*wrap_selection=*/false);
        },
        c::LogicalOperator::kOuterJoin));
    return built;
  }();
  return rules;
}

}  // namespace tinylamb
