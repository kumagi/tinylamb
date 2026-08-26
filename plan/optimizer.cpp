/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "plan/optimizer.hpp"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/cascades.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/empty_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/limit_plan.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/values_plan.hpp"
#include "query/query_data.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Expands a literal "*" into every column of every FROM relation (Phase 8):
// columns are qualified with the relation identity (alias when given, else
// the table name) so they resolve against the renamed scan schemas.
std::vector<NamedExpression> ExpandSelect(const QueryData& query,
                                          TransactionContext& context) {
  const bool has_star =
      std::ranges::any_of(query.select_, [](const NamedExpression& selected) {
        return selected.expression->Type() == TypeTag::kColumnValue &&
               selected.expression->AsColumnValue().GetColumnName().name == "*";
      });
  if (!has_star) {
    return query.select_;
  }
  std::vector<NamedExpression> expanded;
  for (const std::string& relation : query.from_) {
    const auto aliased = query.aliases_.find(relation);
    const std::string& physical =
        aliased == query.aliases_.end() ? relation : aliased->second;
    const StatusOr<std::shared_ptr<Table>> found = context.GetTable(physical);
    if (UNLIKELY(!found.HasValue())) {
      // Matches ASSIGN_OR_CRASH semantics; LOG(FATAL) aborts the process.
      LOG(FATAL) << "Crashed: " << found.GetStatus();
    }
    const std::shared_ptr<Table>& table = found.Value();
    for (size_t i = 0; i < table->GetSchema().ColumnCount(); ++i) {
      expanded.emplace_back(
          ColumnName(relation, table->GetSchema().GetColumn(i).Name().name));
    }
  }
  return expanded;
}

bool IsAggregate(const NamedExpression& expression) {
  return expression.expression->Type() == TypeTag::kAggregateExp;
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

// Relations a conjunct touches. Qualified names are relation identities
// (alias when given, else table name); unqualified names map to every FROM
// relation whose physical schema owns such a column (ambiguous names
// therefore stay above the join that combines their tables, preserving the
// executor's first-match resolution semantics). Returns false when the
// conjunct references something outside the FROM clause; such conjuncts stay
// in the root Selection fallback.
bool ConjunctRelations(
    const Expression& conjunct,
    const std::unordered_map<std::string, std::shared_ptr<Table>>& tables,
    std::unordered_set<std::string>* relations) {
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    if (!column.schema.empty()) {
      if (!tables.contains(column.schema)) {
        return false;
      }
      relations->insert(column.schema);
      continue;
    }
    size_t matches = 0;
    for (const auto& [relation, table] : tables) {
      if (table->GetSchema().Offset(column) >= 0) {
        relations->insert(relation);
        ++matches;
      }
    }
    if (matches == 0) {
      return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Subquery decorrelation (tpch Phase2-4 / P1-5): the canonical correlated
// shapes `x IN (SELECT c ...)`, `EXISTS (SELECT ... WHERE outer = inner)` and
// their negations become semi/anti hash joins. The tuned relational path keeps
// every other subquery shape via subquery_runtime; only conjuncts that reach
// this optimizer AND match a pattern exactly are rewritten.
// ---------------------------------------------------------------------------

constexpr size_t kMaxDecorrelationDepth = 8;
thread_local size_t tls_decorrelation_depth = 0;

enum class ColumnSide { kOuter, kInner };

struct ScopeMaps {
  // Column resolution mirrors QueryData::Rewrite: qualified names must name a
  // FROM relation; unqualified names map first-match with an ambiguity set.
  std::unordered_set<std::string> relations;
  std::unordered_map<std::string, std::string> by_name;
  std::unordered_set<std::string> ambiguous;
  // relation identity -> physical table name (for catalog lookups).
  std::unordered_map<std::string, std::string> physical_of;

  [[nodiscard]] std::optional<ColumnName> Resolve(
      const ColumnName& column) const {
    if (!column.schema.empty()) {
      if (!relations.contains(column.schema)) {
        return std::nullopt;
      }
      return column;
    }
    if (ambiguous.contains(column.name)) {
      return std::nullopt;
    }
    const auto found = by_name.find(column.name);
    if (found == by_name.end()) {
      return std::nullopt;
    }
    return ColumnName(found->second, column.name);
  }
};

ScopeMaps BuildScopeMaps(
    const std::vector<std::pair<std::string, const Schema*>>& relations) {
  ScopeMaps maps;
  for (const auto& [relation, schema] : relations) {
    maps.relations.insert(relation);
    maps.physical_of.emplace(relation, schema->Name());
    for (size_t i = 0; i < schema->ColumnCount(); ++i) {
      const std::string& name = schema->GetColumn(i).Name().name;
      if (!maps.by_name.contains(name)) {
        maps.by_name.emplace(name, relation);
      } else {
        maps.ambiguous.insert(name);
      }
    }
  }
  return maps;
}

// Resolves one column against the inner scope first (subquery shadowing),
// falling back to the outer scope. Unresolvable names reject the rewrite.
std::optional<std::pair<ColumnSide, ColumnName>> ResolveScoped(
    const ColumnName& column, const ScopeMaps& inner, const ScopeMaps& outer) {
  if (column.name == "*") {
    return std::nullopt;
  }
  if (!column.schema.empty()) {
    if (inner.relations.contains(column.schema)) {
      return std::make_pair(ColumnSide::kInner, column);
    }
    if (outer.relations.contains(column.schema)) {
      return std::make_pair(ColumnSide::kOuter, column);
    }
    return std::nullopt;
  }
  if (const auto resolved = inner.Resolve(column)) {
    return std::make_pair(ColumnSide::kInner, *resolved);
  }
  if (const auto resolved = outer.Resolve(column)) {
    return std::make_pair(ColumnSide::kOuter, *resolved);
  }
  return std::nullopt;
}

// Rewrites every column reference into its scope-resolved qualified form.
std::optional<Expression> QualifyExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const ScopeMaps& inner,
    const ScopeMaps& outer) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const auto resolved = ResolveScoped(
        expression->AsColumnValue().GetColumnName(), inner, outer);
    if (!resolved) {
      return std::nullopt;
    }
    return ColumnValueExp(resolved->second);
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> qualified;
  qualified.reserve(children.size());
  for (const Expression& child : children) {
    auto rewritten = QualifyExpression(child, inner, outer);
    if (!rewritten) {
      return std::nullopt;
    }
    qualified.push_back(std::move(*rewritten));
  }
  return WithExpressionChildren(expression, qualified);
}

bool ContainsAggregateExp(
    const Expression& expression) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ContainsAggregateExp(child); });
}

struct DecorrelationSpec {
  JoinKind kind{};
  Expression outer_key;  // resolved qualified column of the outer query
  Expression inner_key;  // resolved qualified column inside the subquery
  std::vector<std::string> from;
  std::unordered_map<std::string, std::string> aliases;
  std::vector<Expression> inner_conjuncts;
};

using TablesMap = std::unordered_map<std::string, std::shared_ptr<Table>>;

// True when the underlying table column cannot hold NULL. When this is false,
// NOT IN uses the null-aware anti implementation instead of the regular anti
// join.
bool ColumnIsNonNull(const ScopeMaps& scope, const ColumnName& column,
                     TransactionContext& ctx) {
  const auto found_relation = scope.physical_of.find(column.schema);
  if (found_relation == scope.physical_of.end()) {
    return false;
  }
  const StatusOr<std::shared_ptr<Table>> table =
      ctx.GetTable(found_relation->second);
  if (!table.HasValue()) {
    return false;
  }
  const int offset =
      table.Value()->GetSchema().Offset(ColumnName("", column.name));
  if (offset < 0) {
    return false;
  }
  const Constraint::ConstraintType ctype =
      table.Value()->GetSchema().GetColumn(offset).GetConstraint().ctype;
  return ctype == Constraint::kNotNull || ctype == Constraint::kPrimaryKey;
}

enum class ConjunctClass { kInnerOnly, kCrossEquality, kReject };

// Sorts one subquery conjunct: an equality whose operands live on opposite
// sides is the correlation key candidate; anything referencing only the inner
// scope rides along as build-side filter; everything else rejects.
ConjunctClass ClassifySubqueryConjunct(const Expression& conjunct,
                                       const ScopeMaps& inner,
                                       const ScopeMaps& outer,
                                       Expression* outer_key,
                                       Expression* inner_key) {
  ColumnSide left_side{ColumnSide::kInner};
  ColumnSide right_side{ColumnSide::kInner};
  Expression left_exp;
  Expression right_exp;
  const auto resolve_operand = [&](const Expression& operand, ColumnSide* side,
                                   Expression* qualified) {
    if (!operand || operand->Type() != TypeTag::kColumnValue) {
      return false;
    }
    if (operand->TouchedColumns().size() != 1) {
      return false;
    }
    const auto resolved =
        ResolveScoped(operand->AsColumnValue().GetColumnName(), inner, outer);
    if (!resolved) {
      return false;
    }
    *side = resolved->first;
    *qualified = ColumnValueExp(resolved->second);
    return true;
  };
  if (conjunct->Type() == TypeTag::kBinaryExp &&
      conjunct->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
    const auto& binary = conjunct->AsBinaryExpression();
    if (resolve_operand(binary.Left(), &left_side, &left_exp) &&
        resolve_operand(binary.Right(), &right_side, &right_exp)) {
      if (left_side == right_side) {
        // Equality within one scope is an ordinary predicate of that scope.
        return left_side == ColumnSide::kInner ? ConjunctClass::kInnerOnly
                                               : ConjunctClass::kReject;
      }
      if (left_side == ColumnSide::kOuter) {
        *outer_key = std::move(left_exp);
        *inner_key = std::move(right_exp);
      } else {
        *outer_key = std::move(right_exp);
        *inner_key = std::move(left_exp);
      }
      return ConjunctClass::kCrossEquality;
    }
  }
  // Non-equality conjuncts survive only when purely inner-local.
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    const auto resolved = ResolveScoped(column, inner, outer);
    if (!resolved || resolved->first != ColumnSide::kInner) {
      return ConjunctClass::kReject;
    }
  }
  return ConjunctClass::kInnerOnly;
}

// Attempts to turn one WHERE conjunct into a semi/anti join spec. Anything
// that does not match a canonical pattern exactly stays untouched so the
// existing evaluation paths keep handling it.
std::optional<DecorrelationSpec> TryDecorrelate(const Expression& conjunct,
                                                const ScopeMaps& outer_scope,
                                                TransactionContext& ctx) {
  Expression current = conjunct;
  bool negated = false;
  while (current->Type() == TypeTag::kUnaryExp &&
         current->AsUnaryExpression().Op() == UnaryOperation::kNot) {
    negated = !negated;
    current = current->AsUnaryExpression().Child();
  }
  if (current->Type() != TypeTag::kQueryExp) {
    return std::nullopt;
  }
  const QueryExpression& query_expression = current->AsQueryExpression();
  const bool exists_form = query_expression.Exists();
  negated = negated != query_expression.Negated();

  const SelectStatement& sub = *query_expression.Query();
  // Shapes the planner cannot preserve semantically stay on their existing
  // paths: aggregation/LIMIT/DISTINCT change the membership set; CTEs and
  // FROM-subqueries need their own runtime.
  if (!sub.WithQueries().empty() || !sub.GroupBy().empty() || sub.Having() ||
      sub.HasLimit() || sub.Offset() != 0 || sub.Distinct() ||
      sub.Sources().empty()) {
    return std::nullopt;
  }

  DecorrelationSpec spec;
  spec.kind = negated ? AntiJoinKind() : SemiJoinKind();

  // IN-form projects exactly one plain column; EXISTS ignores the list but
  // still rejects aggregates (EXISTS(SELECT COUNT(*)) is constant true).
  Expression test_column;
  if (!exists_form) {
    if (sub.SelectList().size() != 1) {
      return std::nullopt;
    }
    test_column = sub.SelectList().front().expression;
    if (!test_column || test_column->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
  }
  for (const NamedExpression& item : sub.SelectList()) {
    if (ContainsAggregateExp(item.expression)) {
      return std::nullopt;
    }
  }
  if (exists_form && query_expression.Test()) {
    return std::nullopt;
  }

  std::vector<std::pair<std::string, const Schema*>> schemas;
  std::vector<Expression> join_conditions;
  for (const SelectSource& source : sub.Sources()) {
    if (source.query != nullptr || source.join_type == JoinType::kLeft ||
        source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull) {
      return std::nullopt;
    }
    const std::string relation =
        source.alias.empty() ? source.table : source.alias;
    spec.from.push_back(relation);
    if (!source.alias.empty() && source.alias != source.table) {
      spec.aliases.emplace(source.alias, source.table);
    }
    if (source.join_condition) {
      join_conditions.push_back(source.join_condition);
    }
  }
  for (const std::string& relation : spec.from) {
    const auto aliased = spec.aliases.find(relation);
    const std::string& physical =
        aliased == spec.aliases.end() ? relation : aliased->second;
    const StatusOr<std::shared_ptr<Table>> table = ctx.GetTable(physical);
    if (!table.HasValue()) {
      return std::nullopt;
    }
    schemas.emplace_back(relation, &table.Value()->GetSchema());
  }
  const ScopeMaps inner_scope = BuildScopeMaps(schemas);

  bool have_key = false;
  for (const Expression& extra : join_conditions) {
    Expression outer_key;
    Expression inner_key;
    switch (ClassifySubqueryConjunct(extra, inner_scope, outer_scope,
                                     &outer_key, &inner_key)) {
      case ConjunctClass::kInnerOnly: {
        auto qualified = QualifyExpression(extra, inner_scope, outer_scope);
        if (!qualified) {
          return std::nullopt;
        }
        spec.inner_conjuncts.push_back(std::move(*qualified));
        break;
      }
      case ConjunctClass::kCrossEquality:
        // Correlated IN and second key pairs are not supported in V1.
        if (!exists_form || have_key) {
          return std::nullopt;
        }
        spec.outer_key = std::move(outer_key);
        spec.inner_key = std::move(inner_key);
        have_key = true;
        break;
      case ConjunctClass::kReject:
        return std::nullopt;
    }
  }
  for (const Expression& predicate : SplitConjuncts(sub.WhereClause())) {
    Expression outer_key;
    Expression inner_key;
    switch (ClassifySubqueryConjunct(predicate, inner_scope, outer_scope,
                                     &outer_key, &inner_key)) {
      case ConjunctClass::kInnerOnly: {
        auto qualified = QualifyExpression(predicate, inner_scope, outer_scope);
        if (!qualified) {
          return std::nullopt;
        }
        spec.inner_conjuncts.push_back(std::move(*qualified));
        break;
      }
      case ConjunctClass::kCrossEquality:
        if (!exists_form || have_key) {
          return std::nullopt;
        }
        spec.outer_key = std::move(outer_key);
        spec.inner_key = std::move(inner_key);
        have_key = true;
        break;
      case ConjunctClass::kReject:
        return std::nullopt;
    }
  }

  if (exists_form) {
    // Uncorrelated EXISTS has no join keys; leave it to the runtime path.
    if (!have_key) {
      return std::nullopt;
    }
  } else {
    // The IN probe belongs to the outer scope, the projected column to the
    // subquery.
    if (!query_expression.Test() ||
        query_expression.Test()->Type() != TypeTag::kColumnValue ||
        query_expression.Test()->TouchedColumns().size() != 1) {
      return std::nullopt;
    }
    const auto probe =
        ResolveScoped(query_expression.Test()->AsColumnValue().GetColumnName(),
                      inner_scope, outer_scope);
    if (!probe || probe->first != ColumnSide::kOuter) {
      return std::nullopt;
    }
    spec.outer_key = ColumnValueExp(probe->second);
    const auto member = ResolveScoped(
        sub.SelectList().front().expression->AsColumnValue().GetColumnName(),
        inner_scope, outer_scope);
    if (!member || member->first != ColumnSide::kInner) {
      return std::nullopt;
    }
    spec.inner_key = ColumnValueExp(member->second);
    if (have_key) {
      return std::nullopt;  // correlated IN: not in V1
    }
    // NOT IN uses the cheaper regular anti join when both keys are known
    // non-null. Otherwise retain SQL's three-valued behavior in the
    // null-aware anti executor; NOT EXISTS has no such hazard.
    if (spec.kind == AntiJoinKind()) {
      const ColumnName& outer_column =
          spec.outer_key->AsColumnValue().GetColumnName();
      if (!ColumnIsNonNull(outer_scope, outer_column, ctx) ||
          !ColumnIsNonNull(inner_scope,
                           spec.inner_key->AsColumnValue().GetColumnName(),
                           ctx)) {
        spec.kind = NullAwareAntiJoinKind();
      }
    }
  }
  return spec;
}

}  // namespace

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx) {
  return Optimize(query, ctx, OptimizerOptions::Default());
}

StatusOr<Plan> Optimizer::OptimizeRelational(
    std::shared_ptr<const SelectStatement> statement, Schema output_schema,
    TransactionContext& ctx) {
  (void)ctx;
  cascades::Memo memo;
  const cascades::GroupId root =
      memo.EnsureDerivedGroup({}, "relational-ir-root");
  cascades::LogicalExpression logical;
  logical.operation = cascades::LogicalOperator::kRelational;
  logical.relational_statement = std::move(statement);
  logical.output_schema = std::move(output_schema);
  memo.AddExpression(root, std::move(logical));

  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  const std::optional<cascades::BestPlan> best = search.Optimize(
      root, cascades::PhysicalProperties{}, DefaultImplementationRules());
  if (!best) {
    return Status::kNotImplemented;
  }
  return best->plan;
}

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx,
                                   const OptimizerOptions& options) {
  // ORDER BY literals do not distinguish rows. Remove them before building
  // the logical sort/top-N layers so direct optimizer callers and the SQL
  // facade share the same property and limit behavior.
  if (std::ranges::any_of(query.order_expressions_, [](const Expression& key) {
        return key && key->Type() == TypeTag::kConstantValue;
      })) {
    QueryData normalized = query;
    normalized.order_expressions_.clear();
    normalized.order_ascending_.clear();
    normalized.order_nulls_first_.clear();
    for (size_t i = 0; i < query.order_expressions_.size(); ++i) {
      const Expression& key = query.order_expressions_[i];
      if (key && key->Type() == TypeTag::kConstantValue) {
        continue;
      }
      normalized.order_expressions_.push_back(key);
      if (i < query.order_ascending_.size()) {
        normalized.order_ascending_.push_back(query.order_ascending_[i]);
      }
      if (i < query.order_nulls_first_.size()) {
        normalized.order_nulls_first_.push_back(query.order_nulls_first_[i]);
      }
    }
    return Optimize(normalized, ctx, options);
  }
  const std::vector<NamedExpression> expanded_select = ExpandSelect(query, ctx);

  const bool has_aggregate = std::ranges::any_of(expanded_select, IsAggregate);
  if (has_aggregate && !std::ranges::all_of(expanded_select, IsAggregate)) {
    return Status::kNotImplemented;
  }

  if (query.from_.empty()) {
    if (has_aggregate || expanded_select.empty()) {
      return Status::kNotImplemented;
    }
    Plan source = std::make_shared<DummyScanPlan>();
    const Expression predicate =
        query.where_
            ? ExpressionRewriter(options.expression_rules).Rewrite(query.where_)
            : ConstantValueExp(Value(true));
    if (predicate && predicate->Type() == TypeTag::kConstantValue) {
      const Value value = predicate->AsConstantValue().GetValue();
      if (value.IsNull() || !value.Truthy()) {
        source = std::make_shared<EmptyPlan>(std::move(source));
      }
    } else if (predicate) {
      return Status::kNotImplemented;
    }
    Plan plan =
        std::make_shared<ProjectionPlan>(std::move(source), expanded_select);
    if (query.distinct_) {
      plan = std::make_shared<DistinctPlan>(std::move(plan));
    }
    if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
      plan = std::make_shared<LimitPlan>(std::move(plan), query.limit_count_,
                                         query.limit_offset_);
    }
    return plan;
  }

  const Expression source_predicate =
      query.where_ ? query.where_ : ConstantValueExp(Value(true));
  const Expression predicate =
      ExpressionRewriter(options.expression_rules).Rewrite(source_predicate);

  cascades::RuleContext rule_context;
  rule_context.transaction = &ctx;
  rule_context.query = &query;
  // Catalog objects are keyed by relation identity (alias when given, else
  // table name); scans rename their output schemas to this identity so
  // self-joins of one physical table stay distinguishable end-to-end.
  for (const std::string& relation : query.from_) {
    const auto aliased = query.aliases_.find(relation);
    const std::string& physical =
        aliased == query.aliases_.end() ? relation : aliased->second;
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, ctx.GetTable(physical));
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, table_statistics,
                     ctx.GetStats(physical));
    rule_context.tables.emplace(relation, std::move(table));
    rule_context.statistics.emplace(relation, std::move(table_statistics));
  }

  // Subquery decorrelation (tpch Phase2-4 / P1-5): canonical IN / EXISTS /
  // NOT EXISTS conjuncts become semi/anti hash joins wrapped around the
  // optimized core. Skipped when a LIMIT/OFFSET folds into the plan: the
  // semi join must sit below any truncation to preserve SQL evaluation
  // order, and the memo has no operator between the two.
  std::vector<DecorrelationSpec> decorrelations;
  std::vector<NamedExpression> projection_items = expanded_select;
  const Expression effective_predicate = [&] {
    std::vector<Expression> kept;
    if (query.limit_count_ == 0 && query.limit_offset_ == 0 &&
        tls_decorrelation_depth < kMaxDecorrelationDepth &&
        predicate->Type() != TypeTag::kConstantValue) {
      ++tls_decorrelation_depth;
      struct DepthGuard {
        ~DepthGuard() { --tls_decorrelation_depth; }
      } guard;
      std::vector<std::pair<std::string, const Schema*>> outer_schemas;
      for (const auto& [relation, table] : rule_context.tables) {
        outer_schemas.emplace_back(relation, &table->GetSchema());
      }
      const ScopeMaps outer_scope = BuildScopeMaps(outer_schemas);
      std::vector<std::pair<DecorrelationSpec, Expression>> candidates;
      for (const Expression& conjunct : SplitConjuncts(predicate)) {
        if (std::optional<DecorrelationSpec> spec =
                TryDecorrelate(conjunct, outer_scope, ctx)) {
          candidates.emplace_back(std::move(*spec), conjunct);
        } else {
          kept.push_back(conjunct);
        }
      }
      // The semi/anti join wraps ABOVE the root projection, so every probe
      // key must survive into the projection output; hidden `$semiN` items
      // keep unselected keys visible and the engine trims them afterwards.
      for (size_t i = 0; i < candidates.size(); ++i) {
        const ColumnName& key =
            candidates[i].first.outer_key->AsColumnValue().GetColumnName();
        bool covered = std::ranges::any_of(
            expanded_select, [&](const NamedExpression& item) {
              return item.expression->Type() == TypeTag::kColumnValue &&
                     item.expression->AsColumnValue().GetColumnName() == key;
            });
        if (!covered && !has_aggregate) {
          projection_items.emplace_back("$semi" + std::to_string(i),
                                        candidates[i].first.outer_key);
          covered = true;
        }
        if (covered) {
          decorrelations.push_back(std::move(candidates[i].first));
        } else {
          kept.push_back(candidates[i].second);  // keep the old route
        }
      }
    }
    if (!decorrelations.empty()) {
      return kept.empty() ? Expression(ConstantValueExp(Value(true)))
                          : CombineConjuncts(kept);
    }
    return predicate;
  }();

  std::unordered_set<ColumnName> touched =
      effective_predicate->TouchedColumns();
  for (const NamedExpression& selected : projection_items) {
    touched.merge(selected.expression->TouchedColumns());
  }

  // Required-column computation (Phase 3): every touched column is needed on
  // each root-to-leaf path of a conjunctive query, so the per-relation
  // projection lists follow directly from the global touched set.
  for (const auto& [relation, table] : rule_context.tables) {
    const std::string physical_name{table->GetSchema().Name()};
    std::vector<NamedExpression> projection;
    for (size_t i = 0; i < table->GetSchema().ColumnCount(); ++i) {
      const ColumnName& table_column = table->GetSchema().GetColumn(i).Name();
      if (std::ranges::any_of(touched, [&](const ColumnName& column) {
            return table_column.name == column.name &&
                   (column.schema.empty() || column.schema == relation ||
                    column.schema == physical_name);
          })) {
        projection.emplace_back(table_column);
      }
    }
    rule_context.scan_projections.emplace(relation, std::move(projection));
  }

  // WHERE decomposition (Phase 2): conjuncts enter the memo; single-relation
  // conjuncts become scan-group filters, spanning conjuncts become join
  // conditions at their deepest covering join.
  std::vector<cascades::ConjunctInfo> conjuncts;
  bool needs_root_selection = false;
  for (const Expression& conjunct : SplitConjuncts(effective_predicate)) {
    std::unordered_set<std::string> relations;
    if (!ConjunctRelations(conjunct, rule_context.tables, &relations) ||
        relations.empty()) {
      needs_root_selection = true;
      continue;
    }
    conjuncts.push_back({conjunct, {relations.begin(), relations.end()}});
  }
  // Custom relational rules may add join expressions without condition
  // payloads; the root Selection keeps the old guarantee that no residual
  // clause is silently dropped for them.
  if (options.relational_rules.Names() !=
      cascades::RuleSet::Default().Names()) {
    needs_root_selection = true;
  }

  cascades::Memo memo;
  cascades::GroupId search_root = memo.Build(query.from_, conjuncts);
  if (needs_root_selection) {
    const cascades::GroupId selection =
        memo.EnsureDerivedGroup(query.from_, "selection");
    memo.AddExpression(selection,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kSelection,
                           .children = {search_root},
                           .predicate = effective_predicate});
    search_root = selection;
  }
  if (has_aggregate) {
    const cascades::GroupId aggregation =
        memo.EnsureDerivedGroup(query.from_, "aggregation");
    memo.AddExpression(aggregation,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kAggregation,
                           .children = {search_root},
                           .table = "",
                           .predicate = std::nullopt,
                           .target_list = projection_items});
    search_root = aggregation;
  } else {
    const cascades::GroupId projection =
        memo.EnsureDerivedGroup(query.from_, "projection");
    memo.AddExpression(projection,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kProjection,
                           .children = {search_root},
                           .table = "",
                           .predicate = std::nullopt,
                           .target_list = projection_items});
    search_root = projection;
  }
  if (query.distinct_) {
    const cascades::GroupId distinct =
        memo.EnsureDerivedGroup(query.from_, "distinct");
    memo.AddExpression(distinct,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kDistinct,
                           .children = {search_root}});
    search_root = distinct;
  }
  if (!query.order_expressions_.empty() &&
      query.order_expressions_.size() == query.order_ascending_.size() &&
      query.limit_count_ != 0) {
    const std::vector<Expression> sort_expressions =
        NormalizeOrderingForOutput(query.order_expressions_, projection_items);
    const cascades::GroupId topn = memo.EnsureDerivedGroup(query.from_, "topn");
    std::vector<NamedExpression> topn_keys;
    topn_keys.reserve(sort_expressions.size());
    for (const Expression& expression : sort_expressions) {
      topn_keys.emplace_back("", expression);
    }
    memo.AddExpression(topn, cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kTopN,
                                 .children = {search_root},
                                 .target_list = std::move(topn_keys),
                                 .sort_ascending = query.order_ascending_,
                                 .sort_nulls_first = query.order_nulls_first_,
                                 .limit_count = query.limit_count_,
                                 .limit_offset = query.limit_offset_});
    search_root = topn;
  } else if (!query.order_expressions_.empty() &&
             query.order_expressions_.size() == query.order_ascending_.size()) {
    const std::vector<Expression> sort_expressions =
        NormalizeOrderingForOutput(query.order_expressions_, projection_items);
    const cascades::GroupId sort = memo.EnsureDerivedGroup(query.from_, "sort");
    std::vector<NamedExpression> sort_keys;
    sort_keys.reserve(sort_expressions.size());
    for (const Expression& expression : sort_expressions) {
      sort_keys.emplace_back("", expression);
    }
    memo.AddExpression(sort, cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kSort,
                                 .children = {search_root},
                                 .target_list = std::move(sort_keys),
                                 .sort_ascending = query.order_ascending_,
                                 .sort_nulls_first = query.order_nulls_first_});
    search_root = sort;
  }
  if ((query.limit_count_ != 0 || query.limit_offset_ != 0) &&
      !(query.limit_count_ != 0 && !query.order_expressions_.empty() &&
        query.order_expressions_.size() == query.order_ascending_.size())) {
    const cascades::GroupId limit =
        memo.EnsureDerivedGroup(query.from_, "limit");
    memo.AddExpression(limit,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kLimit,
                           .children = {search_root},
                           .table = "",
                           .predicate = std::nullopt,
                           .target_list = {},
                           .limit_count = query.limit_count_,
                           .limit_offset = query.limit_offset_});
    search_root = limit;
  }

  cascades::PhysicalProperties properties;
  properties.require_row_position = query.require_row_position_;
  properties.wait_for_write_intent = query.wait_for_write_intent_;
  properties.access_method = options.access_method;
  if (query.order_expressions_.size() == query.order_ascending_.size() &&
      !query.order_expressions_.empty()) {
    std::vector<ColumnName> ordering;
    bool all_columns = true;
    for (const Expression& order : query.order_expressions_) {
      if (order->Type() != TypeTag::kColumnValue) {
        all_columns = false;
        break;
      }
      ordering.push_back(order->AsColumnValue().GetColumnName());
    }
    if (all_columns) {
      properties.ordering = std::move(ordering);
    }
  }
  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    properties.limit_hint = query.limit_offset_ + query.limit_count_;
  }

  // OLTP statements overwhelmingly consist of one indexed relation.  Their
  // search space has no joins and therefore no useful Cascades exploration:
  // cost the exact same scan alternatives directly and build the root layers
  // without memo worklists, rule matching, or best-property hash tables.
  // Keep the general search for custom rule sets and decorrelated subqueries.
  const bool default_rules = options.relational_rules.Names() ==
                                 cascades::RuleSet::Default().Names() &&
                             options.disabled_implementation_rules.empty() &&
                             options.extra_implementation_rules.empty();
  if (query.from_.size() == 1 && default_rules && decorrelations.empty() &&
      !needs_root_selection) {
    return OptimizeSingleRelation(query, effective_predicate, projection_items,
                                  has_aggregate, query.distinct_, properties,
                                  rule_context);
  }

  const cascades::ImplementationRuleSet* implementation_rules =
      &DefaultImplementationRules();
  cascades::ImplementationRuleSet customized;
  if (!options.disabled_implementation_rules.empty() ||
      !options.extra_implementation_rules.empty()) {
    customized = DefaultImplementationRules();
    for (const std::string& disabled : options.disabled_implementation_rules) {
      customized.Remove(disabled);
    }
    for (const cascades::ImplementationRule& extra :
         options.extra_implementation_rules) {
      customized.Add(extra);
    }
    implementation_rules = &customized;
  }

  cascades::SearchEngine search(std::move(memo), options.relational_rules);
  std::optional<cascades::BestPlan> best = search.Optimize(
      search_root, properties, *implementation_rules, rule_context);
  if (!best) {
    return Status::kNotImplemented;
  }

  // Emit the decorrelated semi/anti joins around the optimized core: the
  // inner side is planned recursively (nested subqueries recurse further),
  // then the wrap keeps the outer schema, rows, and row positions.
  for (DecorrelationSpec& spec : decorrelations) {
    QueryData inner_query;
    inner_query.from_ = spec.from;
    inner_query.aliases_ = spec.aliases;
    inner_query.where_ = CombineConjuncts(spec.inner_conjuncts);
    if (!inner_query.where_) {
      inner_query.where_ = ConstantValueExp(Value(true));
    }
    inner_query.select_.emplace_back(
        spec.inner_key->AsColumnValue().GetColumnName());
    ASSIGN_OR_RETURN(Plan, inner_plan, Optimize(inner_query, ctx, options));
    best->plan = std::make_shared<ProductPlan>(
        best->plan,
        std::vector<ColumnName>{
            spec.outer_key->AsColumnValue().GetColumnName()},
        inner_plan,
        std::vector<ColumnName>{
            spec.inner_key->AsColumnValue().GetColumnName()},
        spec.kind);
  }

  if (options.dump_memo) {
    std::ostringstream dump;
    search.GetMemo().Dump(dump);
    dump << "chosen plan:\n" << *best->plan;
    LOG(INFO) << "cascades memo:\n" << dump.str();
  }
  return best->plan;
}

}  // namespace tinylamb
