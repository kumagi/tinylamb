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
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "plan/cascades.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/plan.hpp"
#include "query/query_data.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
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
               selected.expression->AsColumnValue().GetColumnName().name ==
                   "*";
      });
  if (!has_star) { return query.select_;
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

// Relations a conjunct touches. Qualified names are relation identities
// (alias when given, else table name); unqualified names map to every FROM
// relation whose physical schema owns such a column (ambiguous names
// therefore stay above the join that combines their tables, preserving the
// executor's first-match resolution semantics). Returns false when the
// conjunct references something outside the FROM clause; such conjuncts stay
// in the root Selection fallback.
bool ConjunctRelations(const Expression& conjunct,
                       const std::unordered_map<std::string, std::shared_ptr<Table>>& tables,
                       std::unordered_set<std::string>* relations) {
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    if (!column.schema.empty()) {
      if (!tables.contains(column.schema)) { return false;
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
    if (matches == 0) { return false;
}
  }
  return true;
}

}  // namespace

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx) {
  return Optimize(query, ctx, OptimizerOptions::Default());
}

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx,
                                   const OptimizerOptions& options) {
  if (query.from_.empty()) { throw std::runtime_error("No table specified");
}

  const std::vector<NamedExpression> expanded_select =
      ExpandSelect(query, ctx);

  const bool has_aggregate =
      std::ranges::any_of(expanded_select, IsAggregate);
  if (has_aggregate && !std::ranges::all_of(expanded_select, IsAggregate)) {
    return Status::kNotImplemented;
  }

  const Expression source_predicate =
      query.where_ ? query.where_ : ConstantValueExp(Value(true));
  const Expression predicate =
      ExpressionRewriter(options.expression_rules).Rewrite(source_predicate);

  std::unordered_set<ColumnName> touched = predicate->TouchedColumns();
  for (const NamedExpression& selected : expanded_select) {
    touched.merge(selected.expression->TouchedColumns());
  }

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
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    std::unordered_set<std::string> relations;
    if (!ConjunctRelations(conjunct, rule_context.tables, &relations) ||
        relations.empty()) {
      needs_root_selection = true;
      continue;
    }
    conjuncts.push_back(
        {conjunct, {relations.begin(), relations.end()}});
  }
  // Custom relational rules may add join expressions without condition
  // payloads; the root Selection keeps the old guarantee that no residual
  // clause is silently dropped for them.
  if (options.relational_rules.Names() != cascades::RuleSet::Default().Names()) {
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
                           .predicate = predicate});
    search_root = selection;
  }
  if (has_aggregate) {
    const cascades::GroupId aggregation =
        memo.EnsureDerivedGroup(query.from_, "aggregation");
    memo.AddExpression(aggregation, cascades::LogicalExpression{
                                        .operation=cascades::LogicalOperator::kAggregation,
                                        .children={search_root}, .table="", .predicate=std::nullopt,
                                        .target_list=expanded_select});
    search_root = aggregation;
  } else {
    const cascades::GroupId projection =
        memo.EnsureDerivedGroup(query.from_, "projection");
    memo.AddExpression(projection, cascades::LogicalExpression{
                                       .operation=cascades::LogicalOperator::kProjection,
                                       .children={search_root}, .table="", .predicate=std::nullopt,
                                       .target_list=expanded_select});
    search_root = projection;
  }
  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    const cascades::GroupId limit =
        memo.EnsureDerivedGroup(query.from_, "limit");
    memo.AddExpression(limit, cascades::LogicalExpression{
                                  .operation=cascades::LogicalOperator::kLimit,
                                  .children={search_root}, .table="", .predicate=std::nullopt, .target_list={},
                                  .limit_count=query.limit_count_, .limit_offset=query.limit_offset_});
    search_root = limit;
  }

  cascades::PhysicalProperties properties;
  properties.require_row_position = query.require_row_position_;
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
    if (all_columns) { properties.ordering = std::move(ordering);
}
  }
  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    properties.limit_hint = query.limit_offset_ + query.limit_count_;
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
  std::optional<cascades::BestPlan> best =
      search.Optimize(search_root, properties, *implementation_rules,
                      rule_context);
  if (!best) { return Status::kNotImplemented;
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
