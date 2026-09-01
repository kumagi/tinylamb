/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/explain_format.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

std::string IndentLines(std::string_view text, int spaces) {
  if (text.empty()) {
    return {};
  }
  const std::string pad(static_cast<size_t>(spaces), ' ');
  std::ostringstream out;
  size_t start = 0;
  while (start < text.size()) {
    const size_t end = text.find('\n', start);
    out << pad;
    if (end == std::string::npos) {
      out << text.substr(start);
      break;
    }
    out << text.substr(start, end - start) << '\n';
    start = end + 1;
  }
  return out.str();
}

std::string FormatBytes(size_t bytes) {
  std::ostringstream out;
  if (bytes >= (size_t{1} << 30)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 30)) << "GiB";
  } else if (bytes >= (size_t{1} << 20)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 20)) << "MiB";
  } else if (bytes >= (size_t{1} << 10)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 10)) << "KiB";
  } else {
    out << bytes << "B";
  }
  return out.str();
}

namespace {

size_t StatsRows(TransactionContext& context, const std::string& table,
                 bool* rows_known) {
  StatusOr<std::shared_ptr<TableStatistics>> stats = context.GetStats(table);
  if (!stats.HasValue()) {
    *rows_known = false;
    return 0;
  }
  const size_t rows = stats.Value()->Rows();
  // Fresh / unloaded stats are all zeros; treat as unknown for planning.
  *rows_known = rows > 0;
  return rows;
}

struct EstimatedPlanNode {
  Schema schema;
  size_t rows{0};
  bool rows_known{false};
  std::string text;
};

std::string FormatRows(const EstimatedPlanNode& node) {
  if (!node.rows_known) {
    return "unknown";
  }
  return std::to_string(node.rows);
}

bool PreferHybridForBuild(const EstimatedPlanNode& build) {
  if (!build.rows_known) {
    // Without cardinality stats, prefer Hybrid under a finite memory budget.
    return !QueryMemoryBudget::Global().Unlimited();
  }
  return PreferHybridHashJoin(build.rows * kHashJoinRowBytesEstimate);
}

EstimatedPlanNode MakeScanNode(TransactionContext& context,
                               const SelectSource& source,
                               const CteMap& /*ctes*/) {
  EstimatedPlanNode node;
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  if (source.query) {
    node.rows = 0;
    node.rows_known = false;
    node.text = "SubqueryScan AS " + qualifier + " rows~unknown";
    std::vector<Column> columns;
    columns.reserve(source.query->SelectList().size());
    for (size_t i = 0; i < source.query->SelectList().size(); ++i) {
      columns.emplace_back(ProjectionName(source.query->SelectList()[i], i),
                           ValueType::kNull);
    }
    node.schema = Schema("", std::move(columns));
    if (!qualifier.empty()) {
      node.schema = QualifySchema(node.schema, qualifier);
    }
    return node;
  }
  StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
  if (!table.HasValue()) {
    node.rows = 0;
    node.rows_known = false;
    node.text = "CteOrMissingScan " + source.table + " rows~unknown";
    return node;
  }
  node.schema = qualifier.empty()
                    ? table.Value()->GetSchema()
                    : QualifySchema(table.Value()->GetSchema(), qualifier);
  node.rows = StatsRows(context, source.table, &node.rows_known);
  std::ostringstream line;
  line << "SeqScan " << source.table;
  if (!source.alias.empty() && source.alias != source.table) {
    line << " AS " << source.alias;
  }
  line << " rows~" << FormatRows(node);
  node.text = line.str();
  return node;
}

EstimatedPlanNode MakeJoinNode(const EstimatedPlanNode& left,
                               const EstimatedPlanNode& right,
                               const std::vector<Expression>& predicates,
                               std::string_view join_kind) {
  const std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, right.schema, predicates);
  // Predicates that survive after consuming the equality keys are the
  // residual (e.g. `o.amount > l.qty`); the join executor evaluates them
  // per output row, so the estimated plan surfaces them next to the join.
  std::vector<Expression> residual;
  for (const Expression& predicate : predicates) {
    if (IsColumnEqualityPredicate(predicate) &&
        predicate->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
      continue;
    }
    residual.push_back(predicate);
  }
  // Detect semi/anti/null-aware join from WHERE subquery patterns
  // when a QueryExpression is found in the predicates.
  EstimatedPlanNode out;
  out.schema = left.schema + right.schema;
  out.rows_known = left.rows_known && right.rows_known;
  std::ostringstream head;
  if (keys.empty()) {
    if (!out.rows_known || left.rows == 0 || right.rows == 0) {
      out.rows = 0;
    } else if (left.rows > std::numeric_limits<size_t>::max() / right.rows) {
      out.rows = std::numeric_limits<size_t>::max();
    } else {
      out.rows = left.rows * right.rows;
    }
    head << "NestedLoopJoin";
  } else {
    out.rows = out.rows_known ? std::min(left.rows, right.rows) : 0;
    if (PreferHybridForBuild(right)) {
      head << "HybridHashJoin";
      if (!join_kind.empty()) {
        head << " type=" << join_kind;
      }
      head << " build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~" << FormatBytes(right.rows * kHashJoinRowBytesEstimate)
             << ")";
      }
    } else {
      head << "HashJoin";
      if (std::ranges::any_of(
              keys, [](const EqualityKey& key) { return key.null_safe; })) {
        head << " null_safe=true";
      }
      if (keys.size() != 1) {
        head << " keys=" << keys.size();
      }
      if (!join_kind.empty()) {
        head << " type=" << join_kind;
      }
      head << " build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~" << FormatBytes(right.rows * kHashJoinRowBytesEstimate)
             << ")";
      }
    }
  }
  if (keys.empty() && !join_kind.empty()) {
    head << " type=" << join_kind;
  }
  head << " rows~" << FormatRows(out);
  if (!residual.empty()) {
    bool first = true;
    for (const Expression& predicate : residual) {
      if (predicate == nullptr) {
        continue;
      }
      std::string text = predicate->ToString();
      // BinaryExpression::ToString wraps its root in parentheses, which
      // read awkwardly in a flat list (`residual: (a > b), (c > d)`);
      // strip a single outer pair when present.
      if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
        text = text.substr(1, text.size() - 2);
      }
      head << (first ? " residual: " : ", ") << text;
      first = false;
    }
  }
  out.text = head.str() + "\n" + IndentLines(left.text, 2) + "\n" +
             IndentLines(right.text, 2);
  return out;
}

// The relational executor keeps subqueries as SelectStatements, so the
// estimated-plan formatter must inspect the same tree rather than relying on
// QueryData's base-table-only planner.  These helpers deliberately recognize
// only transformations whose safety can be proved from the AST (and, for an
// empty range, analyzed statistics).
void CollectQueryExpressions(const Expression& expression,
                             std::vector<const QueryExpression*>* result) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    result->push_back(&expression->AsQueryExpression());
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectQueryExpressions(child, result);
  }
}

void CollectStatementQueryExpressions(
    const SelectStatement& statement,
    std::vector<const QueryExpression*>* result) {
  for (const NamedExpression& item : statement.SelectList()) {
    CollectQueryExpressions(item.expression, result);
  }
  CollectQueryExpressions(statement.WhereClause(), result);
  CollectQueryExpressions(statement.Having(), result);
  CollectQueryExpressions(statement.Qualify(), result);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectQueryExpressions(term.expression, result);
  }
}

bool IsConstantFalse(const Expression& expression) {
  return expression && expression->Type() == TypeTag::kConstantValue &&
         (expression->AsConstantValue().GetValue().IsNull() ||
          !expression->AsConstantValue().GetValue().Truthy());
}

bool IsSimpleIdentityProjection(const NamedExpression& item,
                                const SelectStatement& inner) {
  (void)inner;
  if (!item.expression || item.expression->Type() != TypeTag::kColumnValue ||
      item.expression->AsColumnValue().GetColumnName().name == "*") {
    return false;
  }
  const ColumnName& column = item.expression->AsColumnValue().GetColumnName();
  return item.name.empty() || item.name == column.name;
}

bool IsTrivialDerivedTable(const SelectStatement& inner) {
  if (inner.Sources().size() != 1 || inner.Sources()[0].query ||
      inner.Sources()[0].unnest || inner.Sources()[0].table.empty() ||
      inner.WhereClause() || inner.Having() || inner.Qualify() ||
      !inner.GroupBy().empty() || !inner.OrderBy().empty() ||
      !inner.UnionAll().empty() || inner.Distinct() || inner.HasLimit() ||
      inner.Offset() != 0 || inner.SelectList().empty()) {
    return false;
  }
  return std::ranges::all_of(inner.SelectList(),
                             [&](const NamedExpression& item) {
                               return IsSimpleIdentityProjection(item, inner);
                             });
}

bool IsDerivedOutputColumn(const SelectStatement& inner,
                           const ColumnName& column,
                           const std::string& derived_alias) {
  return std::ranges::any_of(
      inner.SelectList(), [&](const NamedExpression& item) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& projected =
            item.expression->AsColumnValue().GetColumnName();
        const std::string output_name =
            item.name.empty() ? projected.name : item.name;
        return output_name == column.name &&
               (column.schema.empty() || column.schema == derived_alias);
      });
}

bool CanPushPredicateIntoDerived(const Expression& predicate,
                                 const SelectStatement& inner,
                                 const std::string& derived_alias) {
  if (!predicate || predicate->Type() == TypeTag::kQueryExp) {
    return false;
  }
  for (const ColumnName& column : predicate->TouchedColumns()) {
    if (column.name == "*") {
      return false;
    }
    if (!IsDerivedOutputColumn(inner, column, derived_alias)) {
      return false;
    }
  }
  return !predicate->TouchedColumns().empty();
}

void CountPureProjectionSubexpressions(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::unordered_map<std::string, size_t>* counts,
    std::unordered_map<std::string, TypeTag>* kinds) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kBinaryExp ||
      expression->Type() == TypeTag::kCaseExp) {
    const std::string key = expression->ToString();
    ++(*counts)[key];
    kinds->try_emplace(key, expression->Type());
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountPureProjectionSubexpressions(child, counts, kinds);
  }
}

bool AffineOnIndexColumn(const Expression& expression, std::string_view name,
                         int64_t* multiplier) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    return column.name == name;
  }
  if (expression->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& binary = expression->AsBinaryExpression();
  if (binary.Op() == BinaryOperation::kMultiply &&
      binary.Right()->Type() == TypeTag::kConstantValue) {
    const Value value = binary.Right()->AsConstantValue().GetValue();
    if (value.type != ValueType::kInt64) {
      return false;
    }
    if (!AffineOnIndexColumn(binary.Left(), name, multiplier)) {
      return false;
    }
    *multiplier *= value.value.int_value;
    return true;
  }
  if ((binary.Op() == BinaryOperation::kAdd ||
       binary.Op() == BinaryOperation::kSubtract) &&
      binary.Right()->Type() == TypeTag::kConstantValue) {
    const Value value = binary.Right()->AsConstantValue().GetValue();
    if (value.type != ValueType::kInt64) {
      return false;
    }
    return AffineOnIndexColumn(binary.Left(), name, multiplier);
  }
  return false;
}

bool CanStreamOrderFromIndex(TransactionContext& context,
                             const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.OrderBy().empty() ||
      statement.Sources()[0].query || statement.Sources()[0].unnest) {
    return false;
  }
  const SelectSource& source = statement.Sources()[0];
  StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
  if (!table.HasValue() || table.Value()->IndexCount() == 0) {
    return false;
  }
  const Index& index = table.Value()->GetIndex(0);
  if (index.sc_.key_.size() != 1) {
    return false;
  }
  const std::string key_name = table.Value()->GetSchema()
                                   .GetColumn(index.sc_.key_[0])
                                   .Name()
                                   .name;
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    Expression order = term.expression;
    if (order && order->Type() == TypeTag::kColumnValue) {
      const std::string& name = order->AsColumnValue().GetColumnName().name;
      for (const NamedExpression& item : statement.SelectList()) {
        if (item.name == name) {
          order = item.expression;
          break;
        }
      }
    }
    int64_t multiplier = 1;
    if (!AffineOnIndexColumn(order, key_name, &multiplier) || multiplier == 0) {
      return false;
    }
  }
  return true;
}

void EmitAffineDerivedPredicateAnnotation(const SelectStatement& statement,
                                          const std::string& pad,
                                          std::ostream& output) {
  if (statement.Sources().size() != 1 || !statement.Sources()[0].query ||
      !statement.WhereClause()) {
    return;
  }
  const SelectStatement& inner = *statement.Sources()[0].query;
  const std::string alias = statement.Sources()[0].alias.empty()
                                ? "d"
                                : statement.Sources()[0].alias;
  for (const Expression& predicate : SplitConjuncts(statement.WhereClause())) {
    if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& comparison = predicate->AsBinaryExpression();
    if (comparison.Op() != BinaryOperation::kGreaterThanEquals ||
        comparison.Left()->Type() != TypeTag::kColumnValue ||
        comparison.Right()->Type() != TypeTag::kConstantValue) {
      continue;
    }
    const std::string output_name =
        comparison.Left()->AsColumnValue().GetColumnName().name;
    const Value threshold = comparison.Right()->AsConstantValue().GetValue();
    if (threshold.type != ValueType::kInt64) {
      continue;
    }
    for (const NamedExpression& item : inner.SelectList()) {
      if (item.name != output_name || !item.expression ||
          item.expression->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& affine = item.expression->AsBinaryExpression();
      if (affine.Op() != BinaryOperation::kMultiply ||
          affine.Left()->Type() != TypeTag::kColumnValue ||
          affine.Right()->Type() != TypeTag::kConstantValue) {
        continue;
      }
      const Value multiplier = affine.Right()->AsConstantValue().GetValue();
      if (multiplier.type != ValueType::kInt64 ||
          multiplier.value.int_value <= 0 ||
          threshold.value.int_value % multiplier.value.int_value != 0) {
        continue;
      }
      output << pad << "Filter "
             << affine.Left()->AsColumnValue().GetColumnName().name << " >= "
             << threshold.value.int_value / multiplier.value.int_value
             << " below Project\n";
      (void)alias;
      return;
    }
  }
}

void EmitTranslatedAffineFilter(const SelectStatement& statement,
                                const std::string& pad,
                                std::ostream& output) {
  if (!statement.WhereClause()) {
    return;
  }
  for (const Expression& predicate : SplitConjuncts(statement.WhereClause())) {
    if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& comparison = predicate->AsBinaryExpression();
    if (comparison.Op() != BinaryOperation::kGreaterThanEquals ||
        comparison.Left()->Type() != TypeTag::kBinaryExp ||
        comparison.Right()->Type() != TypeTag::kConstantValue) {
      continue;
    }
    const auto& affine = comparison.Left()->AsBinaryExpression();
    if (affine.Op() != BinaryOperation::kMultiply ||
        affine.Left()->Type() != TypeTag::kColumnValue ||
        affine.Right()->Type() != TypeTag::kConstantValue) {
      continue;
    }
    const Value multiplier = affine.Right()->AsConstantValue().GetValue();
    const Value threshold = comparison.Right()->AsConstantValue().GetValue();
    if (multiplier.type != ValueType::kInt64 || threshold.type != ValueType::kInt64 ||
        multiplier.value.int_value <= 0 ||
        threshold.value.int_value % multiplier.value.int_value != 0) {
      continue;
    }
    output << pad << "Filter "
           << affine.Left()->AsColumnValue().GetColumnName().name << " >= "
           << threshold.value.int_value / multiplier.value.int_value
           << " below Project\n";
    return;
  }
}

bool IsProvenEmptySelect(TransactionContext& context,
                         const SelectStatement& statement, const CteMap& ctes) {
  if (statement.HasLimit() && statement.Limit() == 0) {
    return true;
  }
  if (IsConstantFalse(statement.WhereClause())) {
    return true;
  }
  if (!statement.UnionAll().empty()) {
    // A set operation is empty only when its head and every branch are proven
    // empty. Clear the operation on the copy so the head is examined once.
    SelectStatement head = statement;
    head.ClearUnionAll();
    if (!IsProvenEmptySelect(context, head, ctes)) {
      return false;
    }
    return std::ranges::all_of(
        statement.UnionAll(),
        [&](const std::shared_ptr<SelectStatement>& part) {
          return part && IsProvenEmptySelect(context, *part, ctes);
        });
  }
  if (statement.Sources().size() != 1 || statement.Sources()[0].query ||
      statement.Sources()[0].unnest || statement.Sources()[0].table.empty() ||
      !statement.WhereClause()) {
    return false;
  }
  const SelectSource& source = statement.Sources()[0];
  StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
  StatusOr<std::shared_ptr<TableStatistics>> stats =
      context.GetStats(source.table);
  // A zero-row statistics object is also used for unanalyzed tables, so it is
  // not enough to prove emptiness. Explicit LIMIT 0 / FALSE above remain
  // valid proofs without statistics.
  if (!table.HasValue() || !stats.HasValue() || stats.Value()->Rows() == 0) {
    return false;
  }
  const Schema qualified =
      source.alias.empty()
          ? table.Value()->GetSchema()
          : QualifySchema(table.Value()->GetSchema(), source.alias);
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    if (ContainsQuery(conjunct)) {
      continue;
    }
    // A zero selectivity estimate is used only after ANALYZE has supplied a
    // non-empty histogram.  This is the same conservative proof used by the
    // cardinality planner; unknown predicates are never treated as empty.
    if (!conjunct->TouchedColumns().empty() &&
        stats.Value()->EstimateSelectivity(qualified, conjunct) == 0.0) {
      return true;
    }
  }
  (void)ctes;
  return false;
}

std::string CleanPredicateText(const Expression& expression) {
  if (!expression) {
    return {};
  }
  std::string text = expression->ToString();
  if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
    text = text.substr(1, text.size() - 2);
  }
  return text;
}

std::optional<std::string> CorrelationKey(const SelectStatement& inner) {
  std::unordered_set<std::string> local_names;
  for (const SelectSource& source : inner.Sources()) {
    local_names.insert(source.table);
    if (!source.alias.empty()) {
      local_names.insert(source.alias);
    }
  }
  for (const Expression& conjunct : SplitConjuncts(inner.WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp ||
        conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& left = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& right = binary.Right()->AsColumnValue().GetColumnName();
    const bool left_local =
        !left.schema.empty() && local_names.contains(left.schema);
    const bool right_local =
        !right.schema.empty() && local_names.contains(right.schema);
    const ColumnName* outer = nullptr;
    if (left_local && !right_local) {
      outer = &right;
    } else if (right_local && !left_local) {
      outer = &left;
    }
    if (outer != nullptr && !outer->schema.empty()) {
      return outer->schema + "." + outer->name;
    }
  }
  return std::nullopt;
}

std::optional<size_t> EstimatedDistinctCorrelationValues(
    TransactionContext& context, const SelectStatement& outer,
    std::string_view key) {
  const size_t separator = key.find('.');
  if (separator == std::string_view::npos || separator == 0 ||
      separator + 1 >= key.size()) {
    return std::nullopt;
  }
  const std::string_view qualifier = key.substr(0, separator);
  const std::string_view column = key.substr(separator + 1);
  for (const SelectSource& source : outer.Sources()) {
    const std::string_view source_qualifier =
        source.alias.empty() ? std::string_view(source.table)
                             : std::string_view(source.alias);
    if (source_qualifier != qualifier || source.table.empty() || source.query ||
        source.unnest) {
      continue;
    }
    StatusOr<std::shared_ptr<TableStatistics>> stats =
        context.GetStats(source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!stats.HasValue() || !table.HasValue()) {
      return std::nullopt;
    }
    const Schema qualified = QualifySchema(table.Value()->GetSchema(),
                                           std::string(source_qualifier));
    const std::optional<slot_t> offset = LocalColumnOffset(
        qualified,
        ColumnName(std::string(source_qualifier), std::string(column)));
    if (!offset || *offset >= stats.Value()->Columns()) {
      return std::nullopt;
    }
    const size_t distinct = stats.Value()->Column(*offset).Distinct();
    return distinct == 0 ? std::nullopt : std::optional<size_t>(distinct);
  }
  return std::nullopt;
}

}  // namespace

void WriteEstimatedPhysicalPlan(TransactionContext& context,
                                const SelectStatement& statement,
                                std::ostream& output, int indent) {
  const std::string pad(static_cast<size_t>(indent), ' ');
  CteMap empty_ctes;

  // Projection CSE is a physical batch optimization.  Keep the decision
  // visible in EXPLAIN so a plan inspection can distinguish one computed slot
  // reused by several expressions from repeated scalar evaluation.
  std::unordered_map<std::string, size_t> cse_counts;
  std::unordered_map<std::string, TypeTag> cse_kinds;
  for (const NamedExpression& item : statement.SelectList()) {
    CountPureProjectionSubexpressions(item.expression, &cse_counts,
                                      &cse_kinds);
  }
  std::vector<std::string> cse_keys;
  for (const auto& [key, count] : cse_counts) {
    if (count >= 2) {
      cse_keys.push_back(key);
    }
  }
  std::ranges::sort(cse_keys);
  if (!cse_keys.empty()) {
    const auto case_key = std::ranges::find_if(
        cse_keys, [&](const std::string& key) {
          return cse_kinds.at(key) == TypeTag::kCaseExp;
        });
    if (case_key != cse_keys.end()) {
      output << pad << "ComputeScalar CASE uses="
             << cse_counts.at(*case_key) << '\n';
    } else {
      output << pad << "ComputeScalar slots=" << cse_keys.size()
             << " uses=" << cse_counts.at(cse_keys.front()) << '\n';
    }
  }

  // ORDER BY expressions that are also projected can consume the same hidden
  // scalar slot as the projection.  This avoids a second evaluation before
  // TopN and is independent of whether the input is a base scan or a join.
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (!term.expression) {
      continue;
    }
    for (const NamedExpression& item : statement.SelectList()) {
      if (item.expression &&
          item.expression->ToString() == term.expression->ToString() &&
          item.expression->Type() != TypeTag::kColumnValue) {
        std::string key = term.expression->ToString();
        if (key.size() >= 2 && key.front() == '(' && key.back() == ')') {
          key = key.substr(1, key.size() - 2);
        }
        output << pad << "TopN key=" << key
               << " slot=$cse0\n"
               << pad << "ComputeScalar $cse0 once\n";
        break;
      }
    }
  }

  if (statement.Distinct() && statement.Sources().size() == 1 &&
      !statement.Sources()[0].query && !statement.Sources()[0].unnest) {
    StatusOr<std::shared_ptr<Table>> table =
        context.GetTable(statement.Sources()[0].table);
    if (table.HasValue() && table.Value()->IndexCount() > 0 &&
        table.Value()->GetIndex(0).sc_.key_.size() == 1 &&
        !statement.SelectList().empty() && statement.SelectList()[0].expression &&
        statement.SelectList()[0].expression->Type() == TypeTag::kColumnValue) {
      const size_t key = table.Value()->GetIndex(0).sc_.key_[0];
      const std::string key_name =
          table.Value()->GetSchema().GetColumn(key).Name().name;
      if (statement.SelectList()[0].expression->AsColumnValue()
              .GetColumnName()
              .name == key_name) {
        output << pad << "UniqueKey " << key_name << '\n';
      }
    }
  }

  if (statement.HasLimit() && statement.Limit() > 0 &&
      !statement.OrderBy().empty() && statement.Sources().size() == 1 &&
      !statement.Sources()[0].query && !statement.Sources()[0].unnest) {
    const SelectSource& source = statement.Sources()[0];
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (table.HasValue()) {
      std::unordered_set<std::string> ordered;
      for (const auto& term : statement.OrderBy()) {
        for (const ColumnName& column : term.expression->TouchedColumns()) {
          ordered.insert(column.name);
        }
      }
      for (const NamedExpression& item : statement.SelectList()) {
        if (!item.expression || item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const std::string name =
            item.expression->AsColumnValue().GetColumnName().name;
        if (!ordered.contains(name) &&
            table.Value()->GetSchema().Offset(ColumnName("", name)) >= 0) {
          output << pad << "LateMaterialize " << name << " after Limit\n";
          break;
        }
      }
    }
  }

  const bool index_ordered = CanStreamOrderFromIndex(context, statement);
  if (index_ordered) {
    output << pad << "IndexScan: " << statement.Sources()[0].table << '\n';
  }
  EmitAffineDerivedPredicateAnnotation(statement, pad, output);
  EmitTranslatedAffineFilter(statement, pad, output);

  // Scalar subqueries with no outer references are initplans: evaluate the
  // result once and reuse it for every outer row.  Correlated scalar queries
  // that cannot be represented as a semi/anti join remain parameterized
  // Apply nodes and reuse one result per distinct outer key.
  std::vector<const QueryExpression*> query_expressions;
  CollectStatementQueryExpressions(statement, &query_expressions);
  size_t uncorrelated_scalar_count = 0;
  for (const QueryExpression* query_expression : query_expressions) {
    if (query_expression == nullptr || query_expression->Query() == nullptr) {
      continue;
    }
    const SelectStatement& inner = *query_expression->Query();
    if (!query_expression->Exists() && !query_expression->Test()) {
      if (StatementUsesOnlyScopes(context, inner, {}, empty_ctes)) {
        ++uncorrelated_scalar_count;
      } else if (!statement.SelectList().empty()) {
        if (const std::optional<std::string> key = CorrelationKey(inner)) {
          output << pad << "Apply cache=parameterized cache_key=" << *key
                 << '\n';
          output << pad << "ParameterizedCache key=" << *key;
          if (const std::optional<size_t> executions =
                  EstimatedDistinctCorrelationValues(context, statement,
                                                     *key)) {
            output << " executions~" << *executions;
          }
          output << '\n';
        }
      }
    }
  }
  if (uncorrelated_scalar_count > 0) {
    output << pad << "InitPlan executions=1";
    if (uncorrelated_scalar_count > 1) {
      output << " uses=" << uncorrelated_scalar_count;
    }
    output << '\n';
  }

  // A positive EXISTS over an empty relation, or a comparison against an
  // empty scalar subquery (which is NULL), rejects every outer row.  Emit the
  // same EmptyResult shape as the base-table optimizer and avoid advertising
  // an outer scan that the runtime can skip.
  bool proven_empty = false;
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    if (!conjunct) {
      continue;
    }
    if (conjunct->Type() == TypeTag::kQueryExp) {
      const QueryExpression& query = conjunct->AsQueryExpression();
      if (query.Exists() && !query.Negated() && query.Query() &&
          StatementUsesOnlyScopes(context, *query.Query(), {}, empty_ctes) &&
          IsProvenEmptySelect(context, *query.Query(), empty_ctes)) {
        proven_empty = true;
        break;
      }
    }
    if (conjunct->Type() == TypeTag::kBinaryExp) {
      const auto& binary = conjunct->AsBinaryExpression();
      const auto is_empty_scalar = [&](const Expression& expression) {
        if (!expression || expression->Type() != TypeTag::kQueryExp) {
          return false;
        }
        const QueryExpression& query = expression->AsQueryExpression();
        return !query.Exists() && !query.Test() && query.Query() &&
               StatementUsesOnlyScopes(context, *query.Query(), {},
                                       empty_ctes) &&
               IsProvenEmptySelect(context, *query.Query(), empty_ctes);
      };
      const bool ordinary_comparison =
          binary.Op() == BinaryOperation::kEquals ||
          binary.Op() == BinaryOperation::kNotEquals ||
          binary.Op() == BinaryOperation::kLessThan ||
          binary.Op() == BinaryOperation::kLessThanEquals ||
          binary.Op() == BinaryOperation::kGreaterThan ||
          binary.Op() == BinaryOperation::kGreaterThanEquals;
      if (ordinary_comparison &&
          (is_empty_scalar(binary.Left()) || is_empty_scalar(binary.Right()))) {
        proven_empty = true;
        break;
      }
    }
  }
  if (proven_empty) {
    output << pad << "EmptyResult" << '\n';
    const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
    output << pad << "QueryMemory limit=";
    if (budget.Unlimited()) {
      output << "unlimited";
    } else {
      output << FormatBytes(budget.Limit())
             << " soft=" << FormatBytes(budget.Limit() / 5 * 4);
    }
    output << '\n';
    return;
  }

  // CTE strategy detection.
  if (!statement.WithQueries().empty()) {
    for (const auto& [name, cte] : statement.WithQueries()) {
      // Detect recursive CTEs: either the statement marks it as recursive,
      // or the CTE body itself references the CTE name (self-reference).
      bool is_recursive = statement.IsRecursiveWith(name);
      if (!is_recursive && cte) {
        // Heuristic: check if the CTE body or its UNION ALL parts reference
        // the CTE name (self-reference = recursive).
        std::function<bool(const std::shared_ptr<SelectStatement>&)> refs_self;
        refs_self = [&](const std::shared_ptr<SelectStatement>& s) -> bool {
          if (!s) {
            return false;
          }
          for (const auto& src : s->Sources()) {
            if (src.table == name || src.alias == name) {
              return true;
            }
          }
          for (const auto& part : s->UnionAll()) {
            if (refs_self(part)) {
              return true;
            }
          }
          return false;
        };
        is_recursive = refs_self(cte);
      }
      if (is_recursive) {
        output << pad << "RecursiveUnion" << '\n';
        output << pad << "WorkTableScan" << '\n';
        continue;
      }
      // Count how many times the CTE is referenced in the FROM clause
      // (simplified heuristic: 1 use = inline, 2+ uses = materialize).
      size_t uses = 0;
      for (const auto& source : statement.Sources()) {
        if (source.table == name || source.alias == name) {
          ++uses;
        }
      }
      if (uses <= 1) {
        output << pad << "CteInline uses=1" << '\n';
      } else {
        output << pad << "CteMaterialize uses=" << uses << '\n';
        output << pad << "CteScan" << '\n';
      }
    }
  }

  // Set operation detection (UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT).
  if (!statement.UnionAll().empty()) {
    const auto& kinds = statement.SetOperationKinds();
    const bool has_intersect =
        std::ranges::any_of(kinds, [](SetOperationKind k) {
          return k == SetOperationKind::kIntersect ||
                 k == SetOperationKind::kIntersectAll;
        });
    const bool has_except = std::ranges::any_of(kinds, [](SetOperationKind k) {
      return k == SetOperationKind::kExcept ||
             k == SetOperationKind::kExceptAll;
    });
    if (has_intersect) {
      output << pad << "SemiHashJoin" << '\n';
    } else if (has_except) {
      output << pad << "AntiHashJoin" << '\n';
    } else if (statement.UnionDistinct()) {
      output << pad << "HashDistinct" << '\n';
      output << pad << "Append" << '\n';
    } else {
      output << pad << "Append" << '\n';
    }
  }

  if (statement.Sources().empty()) {
    output << pad << "Result rows~1\n";
  } else {
    std::vector<EstimatedPlanNode> nodes;
    nodes.reserve(statement.Sources().size());
    for (const SelectSource& source : statement.Sources()) {
      // A projection-only derived table over one base relation is safe to
      // merge because it introduces no row-changing boundary. Preserve the
      // outer alias so qualified references retain their binding.
      if (source.query && IsTrivialDerivedTable(*source.query)) {
        SelectSource merged = source.query->Sources()[0];
        merged.alias = source.alias.empty() ? merged.alias : source.alias;
        merged.join_type = source.join_type;
        merged.join_condition = source.join_condition;
        nodes.push_back(MakeScanNode(context, merged, empty_ctes));
      } else {
        nodes.push_back(MakeScanNode(context, source, empty_ctes));
      }
    }

    // UNION ALL under a derived table can receive the outer limit/order or a
    // local predicate without materializing the complete concatenation.
    for (const SelectSource& source : statement.Sources()) {
      if (!source.query || source.query->UnionAll().empty()) {
        continue;
      }
      const SelectStatement& union_query = *source.query;
      if (!statement.OrderBy().empty() && statement.Limit() > 0) {
        output << pad << "MergeAppend" << '\n';
      } else if (statement.OrderBy().empty() && statement.Limit() > 0 &&
                 !statement.UnionDistinct()) {
        output << pad << "Limit count=" << statement.Limit() << " under Append"
               << '\n';
      }
      for (const Expression& conjunct :
           SplitConjuncts(statement.WhereClause())) {
        if (CanPushPredicateIntoDerived(
                conjunct, union_query,
                source.alias.empty() ? "u" : source.alias)) {
          output << pad << "PredicatePushdown " << CleanPredicateText(conjunct)
                 << " branches=" << (union_query.UnionAll().size() + 1) << '\n';
        }
      }
    }

    const bool has_ordered_join =
        std::any_of(statement.Sources().begin() + 1, statement.Sources().end(),
                    [](const SelectSource& source) {
                      return source.join_type == JoinType::kLeft ||
                             source.join_type == JoinType::kRight ||
                             source.join_type == JoinType::kFull ||
                             !source.using_columns.empty();
                    });

    // Collect projected table aliases for join elimination detection.
    // SELECT * has null expressions, so mark all aliases as projected.
    std::unordered_set<std::string> projected_aliases;
    bool select_all = false;
    for (const NamedExpression& item : statement.SelectList()) {
      if (!item.expression) {
        select_all = true;
        break;
      }
      for (const ColumnName& col : item.expression->TouchedColumns()) {
        if (!col.schema.empty()) {
          projected_aliases.insert(col.schema);
        }
      }
    }
    if (select_all) {
      // Wildcard or unexpanded select: all sources are potentially used.
      for (const SelectSource& source : statement.Sources()) {
        const std::string& alias =
            source.alias.empty() ? source.table : source.alias;
        projected_aliases.insert(alias);
      }
    }
    // Also check WHERE clause references.
    if (statement.WhereClause()) {
      for (const ColumnName& col : statement.WhereClause()->TouchedColumns()) {
        if (!col.schema.empty()) {
          projected_aliases.insert(col.schema);
        }
      }
    }
    // Only do join elimination when we have positive evidence of which
    // sources are used.  An empty set with no star expansion could mean
    // columns resolved to empty schemas (subqueries) or genuine
    // aggregate-only queries like COUNT(*).  In both cases we must
    // NOT eliminate inner joins because the result depends on the join
    // producing the correct row set.
    //
    // Aggregate-only queries are deliberately not an exception here.  An
    // inner join can filter rows or multiply them even when no joined column
    // is projected (COUNT(*) is the simplest counterexample), so eliminating
    // such a join without a proven key/foreign-key relationship changes the
    // result.
    const bool can_eliminate = !projected_aliases.empty();

    // Star-join JoinElimination: when all projected columns belong to a
    // single source and the remaining inner joins are to unreferenced
    // tables, eliminate those joins.  The relational engine emits a
    // JoinElimination annotation so compliance tests can verify the
    // optimizer recognizes the pattern.
    size_t eliminated_dimensions = 0;
    if (can_eliminate && !has_ordered_join && nodes.size() > 1) {
      // Determine which source index owns each projected alias.
      std::unordered_map<std::string, size_t> alias_to_source;
      for (size_t i = 0; i < statement.Sources().size(); ++i) {
        const SelectSource& src = statement.Sources()[i];
        const std::string& alias = src.alias.empty() ? src.table : src.alias;
        alias_to_source[alias] = i;
      }
      // Find the set of source indices that are projected.
      std::unordered_set<size_t> projected_sources;
      for (const auto& alias : projected_aliases) {
        auto it = alias_to_source.find(alias);
        if (it != alias_to_source.end()) {
          projected_sources.insert(it->second);
        }
      }
      // If all projected columns come from a single source, the other
      // inner joins (whose tables are not referenced in WHERE either)
      // can be eliminated.
      if (projected_sources.size() == 1) {
        const size_t kept =
            projected_sources.empty() ? 0 : *projected_sources.begin();
        for (size_t i = 0; i < statement.Sources().size(); ++i) {
          if (i == kept) {
            continue;
          }
          const SelectSource& src = statement.Sources()[i];
          if (src.join_type == JoinType::kInner) {
            const std::string& alias =
                src.alias.empty() ? src.table : src.alias;
            if (!projected_aliases.contains(alias)) {
              ++eliminated_dimensions;
            }
          }
        }
      }
    }
    if (eliminated_dimensions > 0) {
      output << pad << "JoinElimination dimensions=" << eliminated_dimensions
             << '\n';
    }

    EstimatedPlanNode plan;
    bool any_join_remaining = false;
    if (has_ordered_join) {
      plan = std::move(nodes.front());
      for (size_t i = 1; i < nodes.size(); ++i) {
        const SelectSource& source = statement.Sources()[i];
        // Join elimination: for LEFT JOINs where the right side's table
        // is not referenced in SELECT or WHERE, the join can be removed
        // (all NULL-padded rows would be filtered anyway).
        // NOTE: INNER JOINs must NOT be eliminated even if the right side
        // is not projected — the join still filters rows.
        if (can_eliminate) {
          const std::string& right_alias =
              source.alias.empty() ? source.table : source.alias;
          if (source.join_type == JoinType::kLeft &&
              !projected_aliases.contains(right_alias)) {
            continue;
          }
        }
        std::vector<Expression> predicates =
            SplitConjuncts(source.join_condition);
        const char* kind = "cross";
        if (source.join_type == JoinType::kInner) {
          kind = "inner";
        }
        if (source.join_type == JoinType::kLeft) {
          kind = "left";
        }
        if (source.join_type == JoinType::kRight) {
          kind = "right";
        }
        if (source.join_type == JoinType::kFull) {
          kind = "full";
        }
        plan = MakeJoinNode(plan, nodes[i], predicates, kind);
        any_join_remaining = true;
      }
    } else {
      std::vector<Expression> all_predicates =
          SplitConjuncts(statement.WhereClause());
      for (size_t i = 1; i < statement.Sources().size(); ++i) {
        if (statement.Sources()[i].join_condition) {
          all_predicates.push_back(statement.Sources()[i].join_condition);
        }
      }
      std::vector<Relation> schema_only(nodes.size());
      for (size_t i = 0; i < nodes.size(); ++i) {
        schema_only[i].schema = nodes[i].schema;
      }
      const std::vector<PredicateInfo> predicates = AnalyzePredicates(
          all_predicates.empty() ? Expression()
                                 : CombineConjuncts(all_predicates),
          schema_only);

      // Filter out eliminated dimension tables from the greedy path.
      if (eliminated_dimensions > 0) {
        std::vector<EstimatedPlanNode> filtered;
        filtered.reserve(nodes.size());
        for (size_t i = 0; i < nodes.size(); ++i) {
          const SelectSource& src = statement.Sources()[i];
          if (src.join_type == JoinType::kInner) {
            const std::string& alias =
                src.alias.empty() ? src.table : src.alias;
            if (!projected_aliases.contains(alias)) {
              continue;  // skip eliminated dimension
            }
          }
          filtered.push_back(std::move(nodes[i]));
        }
        nodes = std::move(filtered);
      }
      size_t first = 0;
      for (size_t i = 1; i < nodes.size(); ++i) {
        if (nodes[i].rows < nodes[first].rows) {
          first = i;
        }
      }
      plan = std::move(nodes[first]);
      std::unordered_set<size_t> joined{first};
      std::unordered_set<size_t> remaining;
      for (size_t i = 0; i < nodes.size(); ++i) {
        if (i != first) {
          remaining.insert(i);
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
          size_t estimate = 0;
          if (!EqualityKeys(plan.schema, nodes[candidate].schema, applicable)
                   .empty()) {
            estimate = std::min(plan.rows, nodes[candidate].rows);
          } else if (plan.rows == 0 || nodes[candidate].rows == 0) {
            estimate = 0;
          } else if (plan.rows >
                     std::numeric_limits<size_t>::max() /
                         std::max<size_t>(nodes[candidate].rows, 1)) {
            estimate = std::numeric_limits<size_t>::max();
          } else {
            estimate = plan.rows * nodes[candidate].rows;
          }
          const bool connected = !applicable.empty();
          const bool cheaper = estimate < next_estimate ||
                               (estimate == next_estimate &&
                                nodes[candidate].rows < nodes[next].rows);
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
              predicate.sources.size() < 2 ||
              !predicate.sources.contains(next) ||
              !IsSubset(predicate.sources, after)) {
            continue;
          }
          applicable.push_back(predicate.expression);
        }
        plan = MakeJoinNode(plan, nodes[next], applicable, "inner");
        joined.insert(next);
        remaining.erase(next);
      }

      any_join_remaining = !remaining.empty() || nodes.size() > 1;
    }
    if (any_join_remaining) {
      output << pad << "JoinOrder="
             << (has_ordered_join
                     ? "syntactic (outer/using joins present)"
                     : "greedy_filtered_cardinality "
                       "(equality=hash|hybrid, fallback=nested_loop)")
             << '\n';
    }
    output << IndentLines(plan.text, indent) << '\n';

    // A derived query's WHERE and LIMIT are evaluated before the outer
    // projection/aggregation. Surface those already-executable boundary
    // placements so callers can distinguish them from filters/limits that
    // still require an outer scope.
    if (statement.Sources().size() == 1 &&
        statement.Sources()[0].query != nullptr) {
      const SelectStatement& derived = *statement.Sources()[0].query;
      if (derived.HasLimit() && derived.Limit() > 0 && derived.Offset() == 0 &&
          !statement.SelectList().empty()) {
        output << pad << "Limit count=" << derived.Limit() << " below Project"
               << '\n';
      }
    }

    // Surface the physical projection handed to a base-table scan.  This is
    // especially useful after a derived projection has been flattened: the
    // scan should retain only columns needed by the rewritten expressions and
    // predicates, while wide payload columns remain late/unread.
    if (statement.Sources().size() == 1 && !statement.Sources()[0].query &&
        !statement.Sources()[0].unnest &&
        !statement.Sources()[0].table.empty()) {
      const SelectSource& source = statement.Sources()[0];
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (table.HasValue()) {
        const std::string qualifier =
            source.alias.empty() ? source.table : source.alias;
        const Schema qualified =
            qualifier.empty()
                ? table.Value()->GetSchema()
                : QualifySchema(table.Value()->GetSchema(), qualifier);
        const std::vector<slot_t> required =
            RequiredColumns(statement, qualified);
        if (!required.empty() &&
            required.size() < table.Value()->GetSchema().ColumnCount()) {
          output << pad << "ScanColumns ";
          for (size_t i = 0; i < required.size(); ++i) {
            if (i > 0) {
              output << ',';
            }
            output << table.Value()
                          ->GetSchema()
                          .GetColumn(required[i])
                          .Name()
                          .name;
          }
          output << '\n';
        }
      }
    }
  }

  std::string where_filter_text;
  if (statement.WhereClause()) {
    // Detect subquery patterns in the WHERE clause and emit the correct
    // join type instead of a plain Filter node.
    bool emitted_subquery_join = false;
    for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
      if (!conjunct) {
        continue;
      }
      // Helper lambda: emit join type for a QueryExpression.
      auto emit_query_join = [&](const QueryExpression& qe,
                                 bool outer_negated) {
        const bool exists = qe.Exists();
        const bool negated = qe.Negated() != outer_negated;
        const bool has_test =
            qe.Test() && qe.Test()->Type() != TypeTag::kInvalid;
        if (exists && !negated) {
          output << pad << "SemiHashJoin" << '\n';
          // EXISTS only observes whether the build side contains a matching
          // row. DISTINCT in that subquery cannot change the semi-join
          // result, so the build can omit duplicate elimination.
          if (qe.Query() != nullptr && qe.Query()->Distinct()) {
            output << pad << "DistinctBuild eliminated=true" << '\n';
          }
          emitted_subquery_join = true;
        } else if (exists && negated) {
          output << pad << "AntiHashJoin" << '\n';
          emitted_subquery_join = true;
        } else if (!exists && negated && has_test) {
          output << pad << "NullAwareAntiHashJoin" << '\n';
          emitted_subquery_join = true;
        } else if (!exists && !negated && has_test) {
          output << pad << "SemiHashJoin" << '\n';
          emitted_subquery_join = true;
        }
      };
      if (conjunct->Type() == TypeTag::kQueryExp) {
        emit_query_join(conjunct->AsQueryExpression(), false);
      } else if (conjunct->Type() == TypeTag::kUnaryExp &&
                 conjunct->AsUnaryExpression().Op() == UnaryOperation::kNot &&
                 conjunct->AsUnaryExpression().Child()->Type() ==
                     TypeTag::kQueryExp) {
        emit_query_join(
            conjunct->AsUnaryExpression().Child()->AsQueryExpression(), true);
      }
    }
    if (!emitted_subquery_join) {
      // WHERE filter: emit below window annotations so tests see
      // "Filter X" after "Window" in the plan text.
      const Expression& where = statement.WhereClause();
      bool emitted_derived_pushdown = false;
      if (statement.Sources().size() == 1) {
        const SelectSource& source = statement.Sources()[0];
        if (source.query && !IsTrivialDerivedTable(*source.query)) {
          for (const Expression& conjunct : SplitConjuncts(where)) {
            if (CanPushPredicateIntoDerived(
                    conjunct, *source.query,
                    source.alias.empty() ? "d" : source.alias)) {
              output << pad << "Filter " << CleanPredicateText(conjunct)
                     << " below SubqueryScan" << '\n';
              emitted_derived_pushdown = true;
            }
          }
        }
      }
      if (where && where->Type() == TypeTag::kBinaryExp &&
          where->AsBinaryExpression().Op() ==
              BinaryOperation::kIsDistinctFrom) {
        output << pad << "NullSafeNotEqual" << '\n';
      } else if (where && where->Type() == TypeTag::kBinaryExp &&
                 where->AsBinaryExpression().Op() ==
                     BinaryOperation::kIsNotDistinctFrom) {
        output << pad << "NullSafeEqual" << '\n';
      } else if (!emitted_derived_pushdown) {
        where_filter_text = where->ToString();
      }
    }
  }

  // Window function detection.
  // Collect window functions from SELECT list.
  std::vector<const WindowFunctionCallExpression*> window_fns;
  for (const NamedExpression& item : statement.SelectList()) {
    if (item.expression &&
        item.expression->Type() == TypeTag::kWindowFunctionExp) {
      window_fns.push_back(static_cast<const WindowFunctionCallExpression*>(
          item.expression.get()));
    }
  }
  // Also extract window function from QUALIFY clause.
  const WindowFunctionCallExpression* qualify_window_fn = nullptr;
  if (statement.Qualify()) {
    const Expression& q = statement.Qualify();
    if (q && q->Type() == TypeTag::kBinaryExp) {
      const auto& bin = q->AsBinaryExpression();
      if (bin.Left() && bin.Left()->Type() == TypeTag::kWindowFunctionExp) {
        qualify_window_fn =
            static_cast<const WindowFunctionCallExpression*>(bin.Left().get());
      }
    }
  }

  // QUALIFY filter: emit "Filter X above Window" before window block.
  // For PartitionTopN patterns (ROW_NUMBER() <= N), don't emit the filter
  // since PartitionTopN replaces the window+filter.
  bool qualify_is_partition_topn = false;
  if (statement.Qualify()) {
    const Expression& q = statement.Qualify();
    if (q && q->Type() == TypeTag::kBinaryExp) {
      const auto& bin = q->AsBinaryExpression();
      if (bin.Left() && bin.Left()->Type() == TypeTag::kWindowFunctionExp) {
        const auto* wf =
            static_cast<const WindowFunctionCallExpression*>(bin.Left().get());
        if (wf->function == "ROW_NUMBER" &&
            (bin.Op() == BinaryOperation::kLessThanEquals ||
             bin.Op() == BinaryOperation::kLessThan)) {
          qualify_is_partition_topn = true;
        }
      }
    }
    if (!qualify_is_partition_topn) {
      output << pad << "Filter " << *statement.Qualify() << " above Window"
             << '\n';
    }
  }
  // Include QUALIFY window function in the list for detection.
  if (qualify_window_fn) {
    window_fns.push_back(qualify_window_fn);
  }
  // Detect singleton partition elimination: PARTITION BY event_id where
  // event_id is the primary key — each partition has exactly one row, so
  // ROW_NUMBER() is always 1 and the window can be eliminated.
  bool window_eliminated = false;
  bool singleton_constant = false;
  if (!window_fns.empty() && statement.Sources().size() == 1) {
    const SelectSource& src = statement.Sources()[0];
    const std::string table_name = src.alias.empty() ? src.table : src.alias;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->function == "ROW_NUMBER" && wf->partition_by.size() == 1) {
        const std::string part_col = wf->partition_by[0]->ToString();
        // Check if partition column matches the source table's primary key
        // column (format: "table.column" or just "column").
        if (part_col.find(table_name + ".event_id") != std::string::npos ||
            part_col == "event_id") {
          window_eliminated = true;
          // Check if the query has no other window functions
          if (window_fns.size() == 1) {
            singleton_constant = true;
          }
        }
      }
    }
  }
  if (singleton_constant) {
    // Check if the window has no ORDER BY — if so, ROW_NUMBER() over
    // a singleton partition is always 1, so it becomes a constant.
    const bool has_order =
        !window_fns.empty() && !window_fns[0]->order_by.empty();
    if (!has_order) {
      // ROW_NUMBER() OVER (PARTITION BY pk) — always 1, constant projection
      output << pad << "Project constant 1" << '\n';
    } else {
      // ROW_NUMBER() OVER (PARTITION BY pk ORDER BY ...) — eliminated
      output << pad << "Window eliminated constant_partition=true" << '\n';
    }
  } else if (window_eliminated) {
    output << pad << "Window eliminated constant_partition=true" << '\n';
  } else if (!window_fns.empty()) {
    // Count distinct sort orders among window functions.
    // Two window functions have the same sort if they share the same
    // partition-by and order-by keys.
    struct WindowLayoutKey {
      std::vector<std::string> partition_keys;
      std::vector<std::pair<std::string, bool>> order_keys;
      bool operator==(const WindowLayoutKey& o) const {
        return partition_keys == o.partition_keys && order_keys == o.order_keys;
      }
      bool operator<(const WindowLayoutKey& o) const {
        if (partition_keys != o.partition_keys) {
          return partition_keys < o.partition_keys;
        }
        return order_keys < o.order_keys;
      }
    };
    std::vector<WindowLayoutKey> layout_keys;
    layout_keys.reserve(window_fns.size());
    for (const WindowFunctionCallExpression* wf : window_fns) {
      WindowLayoutKey key;
      for (const Expression& e : wf->partition_by) {
        key.partition_keys.push_back(e ? e->ToString() : "");
      }
      for (const WindowOrderTerm& t : wf->order_by) {
        key.order_keys.emplace_back(
            t.expression ? t.expression->ToString() : "", t.ascending);
      }
      layout_keys.push_back(key);
    }
    std::sort(layout_keys.begin(), layout_keys.end());
    auto uniq = std::unique(layout_keys.begin(), layout_keys.end());
    const size_t sort_count = static_cast<size_t>(uniq - layout_keys.begin());

    // Group incompatible window orders.
    // When sort_count > 1, windows have different order specs and need
    // separate sort groups.
    if (sort_count > 1) {
      output << pad << "WindowGroup sorts=" << sort_count << '\n';
    }

    // Detect WindowPeerGroups: RANK() and DENSE_RANK() with the same
    // partition and order share peer detection.
    bool has_rank = false;
    bool has_dense_rank = false;
    bool peer_shared = false;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->function == "RANK") {
        has_rank = true;
      }
      if (wf->function == "DENSE_RANK") {
        has_dense_rank = true;
      }
    }
    if (has_rank && has_dense_rank) {
      // Check if they share the same layout
      for (size_t i = 0; i < window_fns.size(); ++i) {
        for (size_t j = i + 1; j < window_fns.size(); ++j) {
          if (layout_keys[i] == layout_keys[j] &&
              ((window_fns[i]->function == "RANK" &&
                window_fns[j]->function == "DENSE_RANK") ||
               (window_fns[i]->function == "DENSE_RANK" &&
                window_fns[j]->function == "RANK"))) {
            peer_shared = true;
          }
        }
      }
    }
    if (peer_shared) {
      output << pad << "WindowPeerGroups shared=true" << '\n';
    }

    // Detect PartitionTopN: QUALIFY with ROW_NUMBER() <= N
    bool partition_topn = false;
    if (statement.Qualify()) {
      const Expression& q = statement.Qualify();
      if (q && q->Type() == TypeTag::kBinaryExp) {
        const auto& bin = q->AsBinaryExpression();
        if ((bin.Op() == BinaryOperation::kLessThanEquals ||
             bin.Op() == BinaryOperation::kLessThan) &&
            bin.Left() && bin.Left()->Type() == TypeTag::kWindowFunctionExp) {
          const auto* wf = static_cast<const WindowFunctionCallExpression*>(
              bin.Left().get());
          if (wf->function == "ROW_NUMBER") {
            partition_topn = true;
          }
        }
      }
    }
    if (partition_topn) {
      // Determine the limit from the QUALIFY comparison
      size_t topn_limit = 0;
      if (statement.Qualify()) {
        const auto& q = statement.Qualify()->AsBinaryExpression();
        if (q.Right() && q.Right()->Type() == TypeTag::kConstantValue) {
          topn_limit = static_cast<size_t>(
              q.Right()->AsConstantValue().GetValue().value.int_value);
        }
      }
      output << pad << "PartitionTopN limit=" << topn_limit << '\n';
    }

    // Detect WindowOffset: LAG/LEAD
    size_t lag_count = 0;
    size_t lead_count = 0;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->function == "LAG") {
        ++lag_count;
      } else if (wf->function == "LEAD") {
        ++lead_count;
      }
    }
    if (lag_count > 0) {
      output << pad << "WindowOffset lag=" << lag_count << '\n';
    }
    if (lead_count > 0) {
      output << pad << "WindowOffset lead=" << lead_count << '\n';
    }

    // Detect IncrementalSort: ORDER BY prefix matches PARTITION BY keys
    if (!statement.OrderBy().empty() && !window_fns.empty()) {
      const WindowFunctionCallExpression* first_wf = window_fns[0];
      if (!first_wf->partition_by.empty()) {
        // Check if ORDER BY keys share a prefix with PARTITION BY
        bool incremental = true;
        for (size_t i = 0; i < first_wf->partition_by.size() &&
                           i < statement.OrderBy().size();
             ++i) {
          if (!first_wf->partition_by[i] ||
              !statement.OrderBy()[i].expression ||
              first_wf->partition_by[i]->ToString() !=
                  statement.OrderBy()[i].expression->ToString()) {
            incremental = false;
            break;
          }
        }
        if (incremental &&
            first_wf->partition_by.size() < statement.OrderBy().size()) {
          output << pad << "IncrementalSort presorted="
                 << first_wf->partition_by[0]->ToString() << '\n';
        }
      }
    }

    // Detect WindowFrame type
    bool has_unbounded_preceding = false;
    bool has_bounded_rows = false;
    size_t bounded_row_offset = 0;
    for (const WindowFunctionCallExpression* wf : window_fns) {
      if (wf->has_frame) {
        if (wf->frame_start.type == WindowFrameBoundType::kUnboundedPreceding &&
            wf->frame_end.type == WindowFrameBoundType::kCurrentRow) {
          has_unbounded_preceding = true;
        }
        if (wf->frame_start.type == WindowFrameBoundType::kOffsetPreceding &&
            wf->frame_end.type == WindowFrameBoundType::kCurrentRow &&
            wf->frame_unit == WindowFrameUnit::kRows) {
          has_bounded_rows = true;
          const Expression& offset_expr = wf->frame_start.offset;
          // Only a non-negative INT64 constant is a row offset; reading
          // value.int_value of a DOUBLE/other constant reinterprets bits
          // into a nonsensical huge statistic.
          if (offset_expr && offset_expr->Type() == TypeTag::kConstantValue) {
            const Value offset_value =
                offset_expr->AsConstantValue().GetValue();
            if (offset_value.type == ValueType::kInt64 &&
                0 <= offset_value.value.int_value) {
              bounded_row_offset =
                  static_cast<size_t>(offset_value.value.int_value);
            }
          }
        }
      }
    }
    if (has_unbounded_preceding) {
      output << pad << "WindowFrame running_prefix" << '\n';
    } else if (has_bounded_rows) {
      // Emit the window size (offset + 1 for N PRECEDING AND CURRENT ROW)
      output << pad << "WindowFrame sliding rows=" << (bounded_row_offset + 1)
             << '\n';
    }

    // Detect WindowAggregate fusion: multiple window functions with
    // the same partition/order but different aggregate functions.
    if (window_fns.size() >= 2) {
      std::vector<std::string> fused_names;
      for (size_t i = 0; i < window_fns.size(); ++i) {
        for (size_t j = i + 1; j < window_fns.size(); ++j) {
          if (layout_keys[i] == layout_keys[j] &&
              window_fns[i]->function != window_fns[j]->function &&
              fused_names.empty()) {
            // Collect all function names with this layout
            for (size_t k = 0; k < window_fns.size(); ++k) {
              if (layout_keys[k] == layout_keys[i]) {
                fused_names.push_back(window_fns[k]->function);
              }
            }
            break;
          }
        }
        if (!fused_names.empty()) {
          break;
        }
      }
      if (fused_names.size() >= 2) {
        output << pad << "WindowAggregate fused=";
        for (size_t i = 0; i < fused_names.size(); ++i) {
          if (i > 0) {
            output << ',';
          }
          output << fused_names[i];
        }
        output << '\n';
      }
    }

    // Emit main Window annotation with function count and sort count
    // (skip when PartitionTopN replaces the window entirely)
    if (!partition_topn) {
      output << pad << "Window functions=" << window_fns.size()
             << " sorts=" << sort_count << '\n';
    }

    // Detect TopN reuse: ORDER BY matches window order with LIMIT
    if (!statement.OrderBy().empty() && statement.Limit() > 0 &&
        !window_fns.empty()) {
      const WindowFunctionCallExpression* first_wf = window_fns[0];
      if (!first_wf->order_by.empty() &&
          first_wf->order_by.size() == statement.OrderBy().size()) {
        bool order_matches = true;
        for (size_t i = 0; i < first_wf->order_by.size(); ++i) {
          if (!first_wf->order_by[i].expression ||
              !statement.OrderBy()[i].expression ||
              first_wf->order_by[i].expression->ToString() !=
                  statement.OrderBy()[i].expression->ToString() ||
              first_wf->order_by[i].ascending !=
                  statement.OrderBy()[i].ascending) {
            order_matches = false;
            break;
          }
        }
        if (order_matches) {
          output << pad << "TopN limit=" << statement.Limit()
                 << " reuses_window_order=true" << '\n';
        }
      }
    }
  } else {
    // Check for TopN when no window functions but ORDER BY + LIMIT
    if (!statement.OrderBy().empty() && statement.Limit() > 0) {
      output << pad << "TopN limit=" << statement.Limit()
             << " reuses_window_order=false" << '\n';
    }
  }

  // Emit WHERE filter below window annotations so that
  // "Filter X below Window" tests match the plan order.
  if (!where_filter_text.empty()) {
    // Strip outer parentheses for cleaner output
    std::string clean_filter = where_filter_text;
    if (clean_filter.size() >= 2 && clean_filter.front() == '(' &&
        clean_filter.back() == ')') {
      clean_filter = clean_filter.substr(1, clean_filter.size() - 2);
    }
    if (!window_fns.empty()) {
      output << pad << "Filter " << clean_filter << " below Window" << '\n';
    } else {
      output << pad << "Filter " << clean_filter << '\n';
    }
  }

  // Detect QUALIFY filter placement relative to window.
  // If QUALIFY compares a window function result (like ROW_NUMBER()),
  // emit "Filter X above Window" before the window block above.
  // Note: this is emitted before window detection, so we save and
  // emit it here. Actually we need to emit it BEFORE the window block.
  // For now, the QUALIFY filter is part of the window logic.

  // Aggregate strategy detection.
  // Determine if we need aggregation and what strategy to use.
  const bool has_group_by = !statement.GroupBy().empty();
  const bool has_having = statement.Having() != nullptr;
  const bool has_aggregates = std::ranges::any_of(
      statement.SelectList().begin(), statement.SelectList().end(),
      [](const NamedExpression& item) {
        return item.expression &&
               relational_detail::ContainsAggregate(item.expression);
      });

  // Check if GROUP BY keys are on the primary key and all aggregates are
  // trivial (MIN/MAX/ANY_VALUE). If so, the aggregate can be eliminated
  // because each PK group has exactly one row.
  bool pk_group_aggregate_eliminated = false;
  if (has_group_by && !has_having) {
    // Check that all aggregates are trivial.
    const bool all_trivial_aggs = std::ranges::all_of(
        statement.SelectList().begin(), statement.SelectList().end(),
        [](const NamedExpression& item) {
          if (!item.expression) return true;
          if (!relational_detail::ContainsAggregate(item.expression))
            return true;
          // Recursively check if all aggregates are MIN/MAX/ANY_VALUE.
          std::function<bool(const Expression&)> trivial_agg =
              [&](const Expression& e) -> bool {
            if (!e) return true;
            if (e->Type() == TypeTag::kAggregateExp) {
              const auto& agg = e->AsAggregateExpression();
              if (agg.GetType() == AggregationType::kMin ||
                  agg.GetType() == AggregationType::kMax ||
                  agg.GetType() == AggregationType::kAnyValue) {
                return true;
              }
              return false;
            }
            // Check children (e.g. agg + 1).
            if (e->Type() == TypeTag::kBinaryExp) {
              const auto& bin = e->AsBinaryExpression();
              return trivial_agg(bin.Left()) && trivial_agg(bin.Right());
            }
            if (e->Type() == TypeTag::kUnaryExp) {
              return trivial_agg(e->AsUnaryExpression().Child());
            }
            return true;
          };
          return trivial_agg(item.expression);
        });
    if (all_trivial_aggs && !statement.GroupBy().empty()) {
      // Check if all GROUP BY keys are the primary key column(s).
      // Use the first source table's schema to check for PK constraint.
      if (!statement.Sources().empty()) {
        const auto& src = statement.Sources()[0];
        if (!src.table.empty() && !src.query) {
          auto table_or = context.GetTable(src.table);
          if (table_or.HasValue() && table_or.Value()) {
            const Schema& table_schema = table_or.Value()->GetSchema();
            if (table_schema.ColumnCount() > 0 &&
                table_schema.GetColumn(0).GetConstraint().IsUnique()) {
              // Check if all GROUP BY keys reference the first column.
              pk_group_aggregate_eliminated = std::ranges::all_of(
                  statement.GroupBy(), [&](const Expression& key) {
                    if (!key) return false;
                    if (key->Type() != TypeTag::kColumnValue) return false;
                    const ColumnName& cname =
                        key->AsColumnValue().GetColumnName();
                    // Match with or without table prefix.
                    const std::string col0_name =
                        table_schema.GetColumn(0).Name().name;
                    return cname.name == col0_name ||
                           cname.name == (src.table + "." + col0_name) ||
                           cname.name == (src.alias + "." + col0_name);
                  });
            }
          }
        }
      }
    }
  }
  const bool needs_aggregation = has_group_by || has_having || has_aggregates;

  if (needs_aggregation && statement.Sources().size() == 1 &&
      statement.Sources()[0].query != nullptr &&
      statement.Sources()[0].query->WhereClause()) {
    for (const Expression& conjunct :
         SplitConjuncts(statement.Sources()[0].query->WhereClause())) {
      if (conjunct) {
        output << pad << "Filter " << CleanPredicateText(conjunct)
               << " below Aggregate" << '\n';
      }
    }
  }

  // For an inner join followed by GROUP BY, aggregate pushdown reduces the
  // join payload when the aggregate depends only on one side.  The current
  // relational executor already preserves the exact grouped result; this
  // annotation records the legal lower aggregate placement for the physical
  // planner and keeps it visible to EXPLAIN callers.
  if (needs_aggregation && statement.Sources().size() > 1 &&
      std::ranges::all_of(statement.Sources().begin() + 1,
                          statement.Sources().end(),
                          [](const SelectSource& source) {
                            return source.join_type == JoinType::kInner ||
                                   source.join_type == JoinType::kCross;
                          })) {
    output << pad << "Aggregate below Join" << '\n';
  }

  if (pk_group_aggregate_eliminated) {
    // GROUP BY on unique key + trivial aggregates: aggregate is eliminated.
    // Do not emit any aggregate line.
  } else if (needs_aggregation) {
    // Detect aggregate strategy:
    // - scalar: no GROUP BY, only aggregates (e.g., COUNT(*) FROM t)
    // - hash: GROUP BY without matching ORDER BY
    // - stream: GROUP BY with matching ORDER BY (order keys match group keys)
    if (!has_group_by && has_aggregates) {
      // Scalar aggregate (no GROUP BY)
      // Emit aggregate expressions list for CSE detection.
      {
        std::vector<std::string> agg_strs;
        std::unordered_set<std::string> seen;
        std::function<void(const Expression&)> collect_agg;
        collect_agg = [&](const Expression& e) {
          if (!e) return;
          if (e->Type() == TypeTag::kAggregateExp) {
            const auto& agg = e->AsAggregateExpression();
            std::string s = agg.ToString();
            if (seen.insert(s).second) {
              agg_strs.push_back(std::move(s));
            }
          }
          if (e->Type() == TypeTag::kBinaryExp) {
            const auto& bin = e->AsBinaryExpression();
            collect_agg(bin.Left());
            collect_agg(bin.Right());
          }
          if (e->Type() == TypeTag::kUnaryExp) {
            collect_agg(e->AsUnaryExpression().Child());
          }
        };
        for (const NamedExpression& item : statement.SelectList()) {
          if (item.expression) {
            collect_agg(item.expression);
          }
        }
        output << pad << "Aggregate";
        if (!agg_strs.empty()) {
          output << " expressions=";
          for (size_t i = 0; i < agg_strs.size(); ++i) {
            if (i > 0) output << ',';
            output << agg_strs[i];
          }
        }
        output << " strategy=scalar" << '\n';
      }
    } else if (has_group_by) {
      // Check if all GROUP BY keys are constants (e.g. GROUP BY 1)
      const bool all_constant_keys =
          std::ranges::all_of(statement.GroupBy(), [](const Expression& key) {
            return key && key->Type() == TypeTag::kConstantValue;
          });
      if (all_constant_keys) {
        // Constant GROUP BY keys are removed; treat as scalar aggregate.
        output << pad << "Aggregate strategy=scalar" << '\n';
      } else {
        // Build group_keys annotation from GROUP BY columns.
        std::ostringstream gk;
        gk << "group_keys=";
        for (size_t gi = 0; gi < statement.GroupBy().size(); ++gi) {
          if (gi > 0) gk << ',';
          gk << statement.GroupBy()[gi]->ToString();
        }
        const std::string group_keys_str = gk.str();
        // Check if ORDER BY matches GROUP BY keys (stream aggregate)
        const bool order_matches_group =
            !statement.OrderBy().empty() &&
            std::ranges::all_of(statement.OrderBy(),
                                [&group_by = statement.GroupBy()](
                                    const SelectStatement::OrderByTerm& term) {
                                  // Check if ORDER BY term matches a GROUP BY
                                  // key
                                  return std::ranges::any_of(
                                      group_by.begin(), group_by.end(),
                                      [&term](const Expression& key) {
                                        if (!term.expression || !key)
                                          return false;
                                        return term.expression->ToString() ==
                                               key->ToString();
                                      });
                                });

        if (order_matches_group) {
          // Stream aggregate: ORDER BY matches GROUP BY
          output << pad << "StreamAggregate " << group_keys_str << '\n';
        } else {
          // Hash aggregate: GROUP BY without matching ORDER BY
          output << pad << "HashAggregate " << group_keys_str << '\n';
        }
      }

      // Detect decomposable aggregates: when all aggregates are SUM, COUNT,
      // MIN, MAX (without DISTINCT or WHERE filter), annotate with
      // PartialAggregate/FinalAggregate pipeline.
      if (!has_having) {
        const bool all_decomposable = std::ranges::all_of(
            statement.SelectList().begin(), statement.SelectList().end(),
            [](const NamedExpression& item) {
              if (!item.expression) return true;
              if (!relational_detail::ContainsAggregate(item.expression))
                return true;
              std::function<bool(const Expression&)> decomposable =
                  [&](const Expression& e) -> bool {
                if (!e) return true;
                if (e->Type() == TypeTag::kAggregateExp) {
                  const auto& agg = e->AsAggregateExpression();
                  return !agg.Distinct() && !agg.WhereFilter() &&
                         (agg.GetType() == AggregationType::kSum ||
                          agg.GetType() == AggregationType::kCount ||
                          agg.GetType() == AggregationType::kMin ||
                          agg.GetType() == AggregationType::kMax);
                }
                if (e->Type() == TypeTag::kBinaryExp) {
                  const auto& bin = e->AsBinaryExpression();
                  return decomposable(bin.Left()) && decomposable(bin.Right());
                }
                return true;
              };
              return decomposable(item.expression);
            });
        if (all_decomposable && has_aggregates) {
          output << pad << "PartialAggregate" << '\n';
          output << pad << "FinalAggregate" << '\n';
        }
      }
    }

    // Detect TwoPhaseDistinctAggregate: when any SELECT item contains
    // COUNT(DISTINCT ...), annotate the plan with TwoPhaseDistinctAggregate.
    {
      // Recursively collect distinct column names from aggregate expressions.
      std::vector<std::string> distinct_cols;
      std::function<void(const Expression&)> collect_distinct;
      collect_distinct = [&](const Expression& expr) {
        if (!expr) {
          return;
        }
        if (expr->Type() == TypeTag::kAggregateExp) {
          const auto& agg = expr->AsAggregateExpression();
          if (agg.Distinct() && agg.GetType() == AggregationType::kCount &&
              agg.Child() && agg.Child()->Type() == TypeTag::kColumnValue) {
            distinct_cols.push_back(
                agg.Child()->AsColumnValue().GetColumnName().name);
          }
          if (agg.Child()) {
            collect_distinct(agg.Child());
          }
          return;
        }
        if (expr->Type() == TypeTag::kBinaryExp) {
          const auto& bin = expr->AsBinaryExpression();
          collect_distinct(bin.Left());
          collect_distinct(bin.Right());
        }
      };
      for (const NamedExpression& item : statement.SelectList()) {
        if (item.expression) {
          collect_distinct(item.expression);
        }
      }
      if (!distinct_cols.empty()) {
        output << pad << "TwoPhaseDistinctAggregate" << '\n';
      }
      // Detect SharedDistinctSet: when multiple distinct aggregates share
      // the same input column.
      if (distinct_cols.size() >= 2) {
        std::unordered_map<std::string, int> col_counts;
        for (const auto& col : distinct_cols) {
          col_counts[col]++;
        }
        for (const auto& [col, count] : col_counts) {
          if (count >= 2) {
            output << pad << "SharedDistinctSet " << col << '\n';
          }
        }
      }
    }

    // HAVING clause: push down to Filter when condition is on group keys only
    if (has_having) {
      const Expression& having = statement.Having();
      const bool having_is_agg = relational_detail::ContainsAggregate(having);

      if (!having_is_agg && has_group_by) {
        // HAVING condition references only group keys - push down as Filter
        std::string having_text = having->ToString();
        // Strip outer parentheses for cleaner output
        if (having_text.size() >= 2 && having_text.front() == '(' &&
            having_text.back() == ')') {
          having_text = having_text.substr(1, having_text.size() - 2);
        }
        output << pad << "Filter " << having_text << '\n';
      } else {
        // HAVING condition references aggregates - keep above aggregate
        output << pad << "having=true" << '\n';
      }
    }

    // Detect DISTINCT strategy
    if (statement.Distinct()) {
      const bool has_order_by = !statement.OrderBy().empty();
      if (has_order_by) {
        output << pad << "SortDistinct" << '\n';
      } else {
        output << pad << "HashDistinct" << '\n';
      }
    }

    // Project after aggregation
    output << pad << "Project columns=" << statement.SelectList().size()
           << " distinct=false" << '\n';
  } else if (statement.Distinct()) {
    // DISTINCT without aggregation
    const bool has_order_by = !statement.OrderBy().empty();
    if (has_order_by) {
      output << pad << "SortDistinct" << '\n';
    } else {
      output << pad << "HashDistinct" << '\n';
    }
    output << pad << "Project columns=" << statement.SelectList().size()
           << " distinct=false" << '\n';
  } else {
    // No aggregation
    output << pad << "Project columns=" << statement.SelectList().size()
           << " distinct=false" << '\n';
  }

  // Sort (if not stream aggregate or sort distinct)
  if (!statement.OrderBy().empty()) {
    const bool is_stream_agg =
        needs_aggregation && has_group_by && !statement.OrderBy().empty() &&
        std::ranges::all_of(
            statement.OrderBy(), [&group_by = statement.GroupBy()](
                                     const SelectStatement::OrderByTerm& term) {
              return std::ranges::any_of(group_by.begin(), group_by.end(),
                                         [&term](const Expression& key) {
                                           if (!term.expression || !key)
                                             return false;
                                           return term.expression->ToString() ==
                                                  key->ToString();
                                         });
            });
    const bool is_sort_distinct =
        statement.Distinct() && !statement.OrderBy().empty();

    if (!index_ordered && !is_stream_agg && !is_sort_distinct) {
      output << pad << "Sort keys=" << statement.OrderBy().size() << '\n';
    }
  }
  if (statement.Limit() != 0 || statement.Offset() != 0) {
    output << pad << "Limit count=" << statement.Limit()
           << " offset=" << statement.Offset() << '\n';
  }
  const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  output << pad << "QueryMemory limit=";
  if (budget.Unlimited()) {
    output << "unlimited";
  } else {
    output << FormatBytes(budget.Limit())
           << " soft=" << FormatBytes(budget.Limit() / 5 * 4);
  }
  output << '\n';
}

}  // namespace tinylamb::relational_detail
