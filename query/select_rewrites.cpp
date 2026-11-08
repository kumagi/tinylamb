/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/select_rewrites.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
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
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/query_data.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

bool IdentifierEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

bool ContainsQueryExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ContainsQueryExpression(child); });
}

}  // namespace

// M4 CTE inlining (first slice): replace a singly-referenced non-recursive
// CTE by its defining query at the single reference site. The reference
// becomes an ordinary derived source, so the M5 flattener (run right after)
// can inline single-table bodies further, and outer predicates push through
// the normal rules. Multi-referenced CTEs keep the materialized relational
// path (shared rescan would re-evaluate an inlined body per reference).
//
// Soundness gates (anything else keeps the map entry for the existing
// materialized path):
// - non-recursive, exactly one reference in the whole statement (FROM
//   sources at any depth, expression subqueries, set-op branches, sibling
//   bodies), none of them inside its own body (cyclic shapes keep the
//   existing error path);
// - the defining body carries no nested WITH of its own (nested-map scope
//   is a follow-up);
// - the single site is a FROM source; the inlined source keeps the CTE name
//   as its alias (or the reference alias when the reference already renames
//   it and nothing still qualifies the CTE name).
// Bodies are top-level queries, so they cannot see the consumer's row scope:
// the inlined source is uncorrelated by construction (a stale lateral flag
// is recomputed with the visitor's own locality rule).
namespace {
void CountCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts);

void CountCteReferencesInStatement(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
    const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts) {
  for (const SelectSource& source : statement.Sources()) {
    if (!source.table.empty() && names.contains(source.table)) {
      (*counts)[source.table]++;
    }
    if (source.query != nullptr) {
      CountCteReferencesInStatement(*source.query, names, counts);
    }
    CountCteReferencesInExpression(source.join_condition, names, counts);
    CountCteReferencesInExpression(source.unnest, names, counts);
  }
  CountCteReferencesInExpression(statement.WhereClause(), names, counts);
  for (const NamedExpression& item : statement.SelectList()) {
    CountCteReferencesInExpression(item.expression, names, counts);
  }
  for (const Expression& expression : statement.GroupBy()) {
    CountCteReferencesInExpression(expression, names, counts);
  }
  CountCteReferencesInExpression(statement.Having(), names, counts);
  CountCteReferencesInExpression(statement.Qualify(), names, counts);
  for (const auto& term : statement.OrderBy()) {
    CountCteReferencesInExpression(term.expression, names, counts);
  }
  for (const auto& branch : statement.UnionAll()) {
    if (branch != nullptr) {
      CountCteReferencesInStatement(*branch, names, counts);
    }
  }
  if (const auto& tree = statement.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      CountCteReferencesInStatement(*tree->first, names, counts);
    }
    for (const auto& branch : tree->branches) {
      if (branch != nullptr) {
        CountCteReferencesInStatement(*branch, names, counts);
      }
    }
  }
  for (const auto& [name, body] : statement.WithQueries()) {
    (void)name;
    if (body != nullptr) {
      CountCteReferencesInStatement(*body, names, counts);
    }
  }
}

void CountCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    if (query.Query() != nullptr) {
      CountCteReferencesInStatement(*query.Query(), names, counts);
    }
    CountCteReferencesInExpression(query.Test(), names, counts);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountCteReferencesInExpression(child, names, counts);
  }
}

// Finds the single FROM source naming `name`. Sources inside sibling CTE
// bodies are reported through `in_map_body`: inlining there only churns the
// materialized path (the body stays mapped), so the slice leaves them alone
// and lets chained CTEs resolve inside-out as outer sites open up.
struct CteReferenceSite {
  SelectSource* source = nullptr;
  bool in_map_body = false;
};

CteReferenceSite FindSingleCteReference(  // NOLINT(misc-no-recursion)
    SelectStatement* statement, const std::string& name) {
  CteReferenceSite result;
  int matches = 0;
  // Whether the visit is currently inside a map-owned CTE body (as opposed
  // to the main statement tree or an already-inlined derived source).
  const std::function<void(SelectStatement*, bool)> visit =
      [&](SelectStatement* current, bool in_map) {
        if (current == nullptr) {
          return;
        }
        for (SelectSource& source :
             const_cast<std::vector<SelectSource>&>(current->Sources())) {
          if (source.table == name) {
            result.source = &source;
            result.in_map_body = in_map;
            ++matches;
          }
          if (source.query != nullptr) {
            visit(source.query.get(), in_map);
          }
        }
        const auto visit_expression = [&](const Expression& expression) {
          std::vector<Expression> stack;
          if (expression) {
            stack.push_back(expression);
          }
          while (!stack.empty()) {
            Expression current_expr = std::move(stack.back());
            stack.pop_back();
            if (!current_expr) {
              continue;
            }
            if (current_expr->Type() == TypeTag::kQueryExp) {
              const QueryExpression& query = current_expr->AsQueryExpression();
              if (query.Query() != nullptr) {
                visit(const_cast<SelectStatement*>(query.Query().get()),
                      in_map);
              }
            }
            for (const Expression& child : ExpressionChildren(current_expr)) {
              if (child) {
                stack.push_back(child);
              }
            }
          }
        };
        visit_expression(current->WhereClause());
        for (const NamedExpression& item : current->SelectList()) {
          visit_expression(item.expression);
        }
        for (const Expression& expression : current->GroupBy()) {
          visit_expression(expression);
        }
        visit_expression(current->Having());
        visit_expression(current->Qualify());
        for (const auto& term : current->OrderBy()) {
          visit_expression(term.expression);
        }
        for (const auto& branch : current->UnionAll()) {
          visit(branch.get(), in_map);
        }
        if (const auto& tree = current->GetSetOperationTree();
            tree != nullptr) {
          visit(tree->first.get(), in_map);
          for (const auto& branch : tree->branches) {
            visit(branch.get(), in_map);
          }
        }
        for (const auto& [entry, body] : current->WithQueries()) {
          (void)entry;
          // Bodies owned by a WITH map (at any depth) count as map bodies;
          // the top-level statement itself does not.
          visit(body.get(), true);
        }
      };
  visit(statement, false);
  if (matches != 1) {
    return CteReferenceSite{};
  }
  return result;
}

// True when every qualified reference in `expression` outside the lateral
// locality check below is irrelevant: mirrors the visitor's own rule that a
// FROM-subquery whose WHERE touches only its local tables is not lateral.
bool DerivedSourceIsLocal(const SelectSource& source) {
  if (source.query == nullptr || !source.query->WhereClause()) {
    return true;
  }
  std::unordered_set<std::string> local;
  for (const SelectSource& inner : source.query->Sources()) {
    if (!inner.alias.empty()) {
      local.insert(inner.alias);
    }
    if (!inner.table.empty()) {
      local.insert(inner.table);
    }
  }
  return std::ranges::all_of(source.query->WhereClause()->TouchedColumns(),
                             [&local](const ColumnName& column) {
                               return column.schema.empty() ||
                                      local.contains(column.schema);
                             });
}

// True when any expression in the statement (any depth, including sibling
// CTE bodies and set-op branches) qualifies a column with `name`.
bool StatementQualifiesName(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement, const std::string& name) {
  bool found = false;
  const auto scan_expression = [&](const Expression& expression) {
    std::vector<Expression> stack;
    if (expression) {
      stack.push_back(expression);
    }
    while (!stack.empty() && !found) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      for (const ColumnName& column : current->TouchedColumns()) {
        if (!column.schema.empty() && column.schema == name) {
          found = true;
          return;
        }
      }
      for (const Expression& child : ExpressionChildren(current)) {
        if (child) {
          stack.push_back(child);
        }
      }
    }
  };
  const std::function<void(const SelectStatement&)> visit =
      [&](const SelectStatement& current) {
        if (found) {
          return;
        }
        for (const SelectSource& source : current.Sources()) {
          scan_expression(source.join_condition);
          scan_expression(source.unnest);
          if (source.query != nullptr) {
            visit(*source.query);
          }
          if (found) {
            return;
          }
        }
        scan_expression(current.WhereClause());
        for (const NamedExpression& item : current.SelectList()) {
          scan_expression(item.expression);
        }
        for (const Expression& expression : current.GroupBy()) {
          scan_expression(expression);
        }
        scan_expression(current.Having());
        scan_expression(current.Qualify());
        for (const auto& term : current.OrderBy()) {
          scan_expression(term.expression);
        }
        for (const auto& branch : current.UnionAll()) {
          if (branch != nullptr) {
            visit(*branch);
          }
        }
        if (const auto& tree = current.GetSetOperationTree(); tree != nullptr) {
          if (tree->first != nullptr) {
            visit(*tree->first);
          }
          for (const auto& branch : tree->branches) {
            if (branch != nullptr) {
              visit(*branch);
            }
          }
        }
        for (const auto& [entry, body] : current.WithQueries()) {
          (void)entry;
          if (body != nullptr) {
            visit(*body);
          }
        }
      };
  visit(statement);
  return found;
}

}  // namespace

bool InlineSingleUseCtes(SelectStatement* outer) {
  if (outer == nullptr || outer->WithQueries().empty()) {
    return false;
  }
  bool changed = false;
  // One inline per pass; each pass removes a map entry, so the loop ends
  // after at most map-size passes (chained CTEs resolve inside-out).
  for (size_t pass = 0; pass <= outer->WithQueries().size(); ++pass) {
    std::unordered_set<std::string> names;
    for (const auto& [name, body] : outer->WithQueries()) {
      (void)body;
      if (!outer->IsRecursiveWith(name)) {
        names.insert(name);
      }
    }
    if (names.empty()) {
      break;
    }
    std::unordered_map<std::string, size_t> counts;
    CountCteReferencesInStatement(*outer, names, &counts);
    bool inlined = false;
    for (const std::string& name : names) {
      const auto body_it = outer->WithQueries().find(name);
      if (body_it == outer->WithQueries().end() || body_it->second == nullptr ||
          !body_it->second->WithQueries().empty()) {
        continue;
      }
      if (counts[name] != 1) {
        continue;
      }
      // A self reference keeps the existing cyclic-error path.
      std::unordered_map<std::string, size_t> self_counts;
      CountCteReferencesInStatement(*body_it->second, {name}, &self_counts);
      if (self_counts[name] != 0) {
        continue;
      }
      CteReferenceSite site = FindSingleCteReference(outer, name);
      if (site.source == nullptr || site.source->query != nullptr ||
          site.in_map_body) {
        continue;
      }
      // Slice boundary: inline only single-table row-preserving bodies —
      // exactly what the M5 flattener can consume next. Anything richer
      // (multi-table, grouping, ...) stays mapped for the existing
      // materialized path instead of churning its EXPLAIN shape.
      const SelectStatement& body = *body_it->second;
      if (body.Sources().size() != 1 || !body.GroupBy().empty() ||
          body.Having() || body.Distinct() || !body.DistinctOn().empty() ||
          body.HasLimit() || body.Offset() != 0 || body.Qualify() ||
          !body.UnionAll().empty() || body.GetSetOperationTree() != nullptr ||
          relational_detail::HasWindowFunctions(body)) {
        continue;
      }
      const SelectSource& body_source = body.Sources().front();
      if (body_source.query != nullptr || body_source.unnest ||
          body_source.is_lateral || body_source.table.empty() ||
          !body_source.using_columns.empty() || body_source.from_nested_join ||
          body_source.join_condition) {
        continue;
      }
      bool body_has_aggregate = false;
      for (const NamedExpression& item : body.SelectList()) {
        if (relational_detail::ContainsAggregate(item.expression) ||
            ContainsQueryExpression(item.expression)) {
          body_has_aggregate = true;
          break;
        }
      }
      if (body_has_aggregate) {
        continue;
      }
      // A reference that already renames the CTE keeps its alias, but only
      // when nothing still qualifies the CTE name itself (such references
      // would lose their scope after inlining).
      std::string alias =
          site.source->alias.empty() ? name : site.source->alias;
      if (alias != name && StatementQualifiesName(*outer, name)) {
        continue;
      }
      site.source->query = body_it->second;
      site.source->table.clear();
      site.source->alias = std::move(alias);
      site.source->is_lateral = !DerivedSourceIsLocal(*site.source);
      outer->RemoveWithQuery(name);
      inlined = true;
      changed = true;
      break;
    }
    if (!inlined) {
      break;
    }
  }
  return changed;
}

namespace {

// --- shared-CTE restriction pushdown (M4 cost gate) -------------------------
//
// A CTE with several references cannot take any single consumer's filter: the
// cell is built once and every reference reads it.  Every consumer still only
// needs the rows its own filter keeps, so the *disjunction* of the per-site
// restrictions is exact -- it keeps every row some consumer can emit and drops
// the rest before the cell is built.  (Pushing a conjunction would delete rows
// another consumer needs.)
//
// Gates (anything else keeps the unfiltered materialization):
// - non-recursive entry with at least two reference sites, every one a plain
//   FROM source of this statement or of a sibling CTE body, joined by
//   CROSS/INNER (a null-extended side can turn "removed" rows into padding);
// - no reference anywhere else (derived table, subquery, set-op branch), so
//   every restriction is readable from a WHERE clause;
// - every site has at least one restriction on that site's alias (a site
//   without one makes the disjunction TRUE);
// - the body is `SELECT <plain columns> FROM <one base table>` (no renaming,
//   grouping, DISTINCT, LIMIT/OFFSET, QUALIFY, window, subquery, set-op), so
//   stripping a qualifier still names a body column;
// - the cost estimate says the disjunction actually reduces the row count
//   (`EstimateSelectivity < 1`), so a tautology is not pushed just because it
//   is syntactically present.
Expression StripCteQualifier(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::string& qualifier) {
  if (!expression || expression->Type() == TypeTag::kQueryExp) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.schema != qualifier) {
      return expression;
    }
    return ColumnValueExp(ColumnName("", column.name));
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    Expression rewritten = StripCteQualifier(child, qualifier);
    if (rewritten != child) {
      changed = true;
      child = std::move(rewritten);
    }
  }
  if (!changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

// Counts references to `name` inside *nested* scopes of `statement` (derived
// tables, subqueries, set-operation branches) but not in the statement's own
// FROM list or in its sibling WITH bodies.
size_t CountNestedCteReferences(const SelectStatement& statement,
                                const std::string& name);

void CountNestedCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::string& name, size_t* count) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    if (query.Query() != nullptr) {
      *count += CountNestedCteReferences(*query.Query(), name);
    }
    CountNestedCteReferencesInExpression(query.Test(), name, count);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountNestedCteReferencesInExpression(child, name, count);
  }
}

size_t CountNestedCteReferences(const SelectStatement& statement,
                                const std::string& name) {
  size_t count = 0;
  for (const SelectSource& source : statement.Sources()) {
    if (source.query != nullptr) {
      count += CountNestedCteReferences(*source.query, name);
    }
    CountNestedCteReferencesInExpression(source.join_condition, name, &count);
    CountNestedCteReferencesInExpression(source.unnest, name, &count);
  }
  CountNestedCteReferencesInExpression(statement.WhereClause(), name, &count);
  for (const NamedExpression& item : statement.SelectList()) {
    CountNestedCteReferencesInExpression(item.expression, name, &count);
  }
  for (const Expression& expression : statement.GroupBy()) {
    CountNestedCteReferencesInExpression(expression, name, &count);
  }
  CountNestedCteReferencesInExpression(statement.Having(), name, &count);
  CountNestedCteReferencesInExpression(statement.Qualify(), name, &count);
  for (const auto& term : statement.OrderBy()) {
    CountNestedCteReferencesInExpression(term.expression, name, &count);
  }
  for (const auto& branch : statement.UnionAll()) {
    if (branch != nullptr) {
      count += CountNestedCteReferences(*branch, name);
    }
  }
  if (const auto& tree = statement.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      count += CountNestedCteReferences(*tree->first, name);
    }
    for (const auto& branch : tree->branches) {
      if (branch != nullptr) {
        count += CountNestedCteReferences(*branch, name);
      }
    }
  }
  return count;
}

struct SharedCteSite {
  SelectStatement* consumer{nullptr};
  std::string alias;
};

// Collects every reference site of `name`, or returns false when a reference
// exists that the pushdown cannot reason about.  `name`'s own body is not a
// consumer: a self-named source there is not visible to the entry itself (it
// resolves to the enclosing WITH or to a catalog table), so it is somebody
// else's CTE.
bool CollectSharedCteSites(SelectStatement* outer, const std::string& name,
                           std::vector<SharedCteSite>* sites) {
  std::vector<SelectStatement*> consumers{outer};
  for (const auto& [entry, body] : outer->WithQueries()) {
    if (entry != name && body != nullptr) {
      consumers.push_back(body.get());
    }
  }
  for (SelectStatement* consumer : consumers) {
    if (CountNestedCteReferences(*consumer, name) != 0) {
      return false;
    }
    for (const SelectSource& source : consumer->Sources()) {
      if (source.table != name) {
        continue;
      }
      if (source.query != nullptr || source.unnest || source.is_lateral ||
          source.sample.has_value() ||
          (source.join_type != JoinType::kCross &&
           source.join_type != JoinType::kInner)) {
        return false;
      }
      sites->push_back(
          SharedCteSite{.consumer = consumer,
                        .alias = source.alias.empty() ? name : source.alias});
    }
  }
  return true;
}

// The pushed predicate is written against the body's own scope, so the body
// has to be a plain projection of one base table whose output names are the
// base column names.
bool CteBodyTakesRestriction(const SelectStatement& body,
                             std::unordered_set<std::string>* outputs) {
  if (body.Sources().size() != 1 || !body.GroupBy().empty() || body.Having() ||
      body.Distinct() || !body.DistinctOn().empty() || body.HasLimit() ||
      body.Offset() != 0 || body.Qualify() || !body.UnionAll().empty() ||
      body.GetSetOperationTree() != nullptr || !body.WithQueries().empty() ||
      relational_detail::HasWindowFunctions(body)) {
    return false;
  }
  const SelectSource& source = body.Sources().front();
  if (source.query != nullptr || source.unnest || source.is_lateral ||
      source.table.empty() || source.sample.has_value() ||
      !source.using_columns.empty() || source.from_nested_join ||
      source.join_condition) {
    return false;
  }
  for (const NamedExpression& item : body.SelectList()) {
    if (item.expression == nullptr ||
        item.expression->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& column = item.expression->AsColumnValue().GetColumnName();
    if (!column.schema.empty() && column.schema != source.alias &&
        column.schema != source.table) {
      return false;
    }
    if (column.name != item.name) {
      return false;
    }
    outputs->insert(item.name);
  }
  return !outputs->empty();
}

}  // namespace

size_t PushSharedCteRestrictions(SelectStatement* outer,
                                 TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty()) {
    return 0;
  }
  std::vector<std::string> entries;
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr &&
        outer->RecursiveDepthOf(name) == nullptr) {
      entries.push_back(name);
    }
  }
  std::ranges::sort(entries);
  size_t pushed = 0;
  for (const std::string& name : entries) {
    SelectStatement& body = *outer->WithQueries().at(name);
    std::unordered_set<std::string> outputs;
    if (!CteBodyTakesRestriction(body, &outputs)) {
      continue;
    }
    std::vector<SharedCteSite> sites;
    if (!CollectSharedCteSites(outer, name, &sites) || sites.size() < 2) {
      continue;
    }
    std::vector<Expression> restrictions;
    bool every_site_restricts = true;
    for (const SharedCteSite& site : sites) {
      std::vector<Expression> local;
      for (const Expression& conjunct :
           SplitConjuncts(site.consumer->WhereClause())) {
        if (!conjunct || !ReferencesOnly(conjunct, {site.alias})) {
          continue;
        }
        Expression stripped = StripCteQualifier(conjunct, site.alias);
        const bool names_outputs = std::ranges::all_of(
            stripped->TouchedColumns(), [&outputs](const ColumnName& column) {
              return column.schema.empty() && outputs.contains(column.name);
            });
        if (!names_outputs) {
          local.clear();
          every_site_restricts = false;
          break;
        }
        local.push_back(std::move(stripped));
      }
      if (local.empty()) {
        every_site_restricts = false;
        break;
      }
      restrictions.push_back(CombineConjuncts(local));
    }
    if (!every_site_restricts) {
      continue;
    }
    Expression disjunction = restrictions.front();
    for (size_t i = 1; i < restrictions.size(); ++i) {
      disjunction = BinaryExpressionExp(disjunction, BinaryOperation::kOr,
                                        restrictions[i]);
    }
    // Cost gate: only materialize a narrower cell when the estimate says the
    // disjunction drops rows.  No statistics (no ANALYZE yet) means the
    // reduction is unknown, and a shape the estimator cannot model answers
    // 1.0; both keep the cell unfiltered.
    const std::string& base_table = body.Sources().front().table;
    const StatusOr<std::shared_ptr<Table>> found_table =
        ctx.GetTable(base_table);
    const StatusOr<std::shared_ptr<TableStatistics>> found_stats =
        ctx.GetStats(base_table);
    if (!found_table.HasValue() || !found_stats.HasValue() ||
        found_stats.Value()->Rows() == 0) {
      continue;
    }
    const double selectivity = found_stats.Value()->EstimateSelectivity(
        found_table.Value()->GetSchema(), disjunction);
    if (!(selectivity < 1.0)) {
      continue;
    }
    std::vector<Expression> conjuncts;
    if (body.WhereClause()) {
      conjuncts.push_back(body.WhereClause());
    }
    conjuncts.push_back(std::move(disjunction));
    body.SetWhereClause(CombineConjuncts(conjuncts));
    ++pushed;
  }
  return pushed;
}

namespace {}  // namespace

// M5 derived-table flattening (first slice): inline a FROM-subquery over a
// single base table when the rewrite provably preserves the row multiset.
// The derived source `s` over `SELECT <outputs> FROM t [WHERE w]` becomes the
// base table under alias `s`, output references `s.name` substitute the
// (immutable) inner expression, and the inner WHERE merges into the outer
// WHERE — or into the ON condition when the flattened source is the
// null-supplying side of an outer join (filtering it after padding would
// drop padded rows instead of merely failing the match).
//
// Soundness gates (anything else keeps the relational path):
// - single plain-table inner source; no CTEs (inner or outer), grouping,
//   aggregates, DISTINCT, LIMIT/OFFSET, QUALIFY, window functions, set-ops,
//   subqueries, or stars the catalog cannot expand;
// - non-correlated: every inner column reference binds inside the inner
//   table (qualified with the inner alias/table, or bare and present there);
// - substituted outputs are immutable per row (a volatile output referenced
//   twice must evaluate once, as in the materialized derived table);
// - no observability shift for outer scopes: qualified `s.name` must name a
//   derived output, and no bare outer reference may collide with a base
//   column hidden by the derived table.
namespace {
bool DerivedExpressionIsImmutable(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kQueryExp:
    case TypeTag::kAggregateExp:
    case TypeTag::kWindowFunctionExp:
      return false;
    case TypeTag::kFunctionCallExp:
      if (GetFunctionVolatility(
              expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
        return false;
      }
      break;
    default:
      break;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             [](const Expression& child) {
                               return DerivedExpressionIsImmutable(child);
                             });
}

struct DerivedScope {
  std::string derived_alias;
  std::string inner_alias;
  std::string base_table;
  Schema base_schema;
  // Lowered output name -> rebound inner expression (qualifiers already name
  // the derived alias).
  std::unordered_map<std::string, Expression> outputs;
  // Lowered base column names hidden by the derived table (for the bare-name
  // collision gate).
  std::unordered_set<std::string> hidden_columns;
};

// Rebinds an inner-scope expression to the derived alias: qualifiers naming
// the inner alias/table become the derived alias, bare names must exist in
// the base schema. Nullopt on any outer reference or unknown column.
std::optional<Expression> RebindInnerExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const DerivedScope& scope) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return std::nullopt;
    }
    if (!column.schema.empty()) {
      if (!IdentifierEquals(column.schema, scope.inner_alias) &&
          !IdentifierEquals(column.schema, scope.base_table)) {
        return std::nullopt;
      }
      return ColumnValueExp(ColumnName(scope.derived_alias, column.name));
    }
    if (scope.base_schema.Offset(ColumnName("", column.name)) < 0) {
      return std::nullopt;
    }
    return ColumnValueExp(ColumnName(scope.derived_alias, column.name));
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> rebound;
  rebound.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped = RebindInnerExpression(child, scope);
    if (!mapped.has_value()) {
      return std::nullopt;
    }
    rebound.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(rebound));
}

// Substitutes derived-output references in an outer-scope expression with
// the rebound inner expression. Qualified `alias.name` must name a derived
// output; bare names matching an output substitute only when they cannot
// name a base column (otherwise the reference is left for normal scope
// resolution, which errors on ambiguity exactly as before).
std::optional<Expression>
SubstituteDerivedOutputs(  // NOLINT(misc-no-recursion)
    const Expression& expression, const DerivedScope& scope,
    const std::unordered_set<std::string>& base_column_names) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return expression;
    }
    if (!column.schema.empty()) {
      if (!IdentifierEquals(column.schema, scope.derived_alias)) {
        return expression;
      }
      std::string lowered = column.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      const auto found = scope.outputs.find(lowered);
      if (found == scope.outputs.end()) {
        return std::nullopt;
      }
      return found->second;
    }
    std::string lowered = column.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!scope.outputs.contains(lowered)) {
      return expression;
    }
    if (base_column_names.contains(lowered)) {
      return std::nullopt;
    }
    return scope.outputs.at(lowered);
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> substituted;
  substituted.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped =
        SubstituteDerivedOutputs(child, scope, base_column_names);
    if (!mapped.has_value()) {
      return std::nullopt;
    }
    substituted.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(substituted));
}

// Lowers an identifier for case-insensitive star-modifier matching.
std::string LoweredStarName(std::string_view text) {
  std::string lowered(text);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return lowered;
}

// Rebinds a REPLACE expression so bare names that match a base column of the
// star target read that relation's column: the substitution must observe the
// original columns, not the replaced outputs.  Qualified references are kept
// for normal scope resolution.
Expression RebindStarReplaceExpression(
    const Expression& expression, const std::string& relation,
    const std::unordered_set<std::string>& base_column_names) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name != "*" && column.schema.empty() &&
        base_column_names.contains(LoweredStarName(column.name))) {
      return ColumnValueExp(ColumnName(relation, column.name));
    }
    return expression;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    Expression rebound =
        RebindStarReplaceExpression(child, relation, base_column_names);
    changed |= rebound->ToString() != child->ToString();
    child = std::move(rebound);
  }
  if (!changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

// Expands one star select item carrying EXCEPT / REPLACE modifiers over one
// star-target schema, appending the surviving columns in order.  A REPLACE
// target keeps its output position and name but emits the rebound expression.
// Entries that resolve against *this* schema are reported in the resolved
// sets; a bare star over several relations resolves each modifier against
// whichever relation owns the column, and the caller turns names that match
// no relation into a loud error.
}  // namespace

void ExpandStarWithModifiers(
    const NamedExpression& item, const std::string& relation,
    const Schema& schema, std::vector<NamedExpression>* out,
    std::unordered_set<std::string>* resolved_except,
    std::unordered_set<std::string>* resolved_replace) {
  const StarModifiers& modifiers = *item.star_modifiers;
  std::unordered_set<std::string> base_column_names;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    base_column_names.insert(LoweredStarName(schema.GetColumn(i).Name().name));
  }
  const std::string lowered_relation = LoweredStarName(relation);
  std::unordered_set<std::string> except;
  for (const std::string& entry : modifiers.except_columns) {
    const std::string lowered = LoweredStarName(entry);
    const size_t dot = lowered.find('.');
    std::string name = lowered;
    if (dot != std::string::npos) {
      // A qualified spelling must name this relation to resolve here.
      if (lowered.substr(0, dot) != lowered_relation) {
        continue;
      }
      name = lowered.substr(dot + 1);
    }
    if (!base_column_names.contains(name)) {
      continue;
    }
    except.insert(name);
    resolved_except->insert(lowered);
  }
  std::unordered_map<std::string, const Expression*> replace;
  for (const StarReplaceItem& item_replace : modifiers.replace_items) {
    const std::string lowered = LoweredStarName(item_replace.column);
    if (!base_column_names.contains(lowered)) {
      continue;
    }
    replace.emplace(lowered, &item_replace.expression);
    resolved_replace->insert(lowered);
  }
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const std::string& column_name = schema.GetColumn(i).Name().name;
    const std::string lowered = LoweredStarName(column_name);
    if (except.contains(lowered)) {
      continue;
    }
    const auto replaced = replace.find(lowered);
    if (replaced != replace.end()) {
      out->emplace_back(column_name,
                        RebindStarReplaceExpression(*replaced->second, relation,
                                                    base_column_names));
      continue;
    }
    out->emplace_back(ColumnName(relation, column_name));
  }
}

namespace {

bool FlattenOneDerivedSource(SelectStatement* outer, size_t index,
                             TransactionContext& ctx);
bool FlattenGroupedDerivedJoinCore(SelectStatement* outer);
// True when the expression carries a `*` column reference at any depth,
// including the lone-star value-table wrapper (`__value_table_value(*)`,
// which selects the row as a value rather than expanding columns). Star
// expansion over derived tables keeps its dedicated (relational) path in
// this slice.
bool ContainsStarReference(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> stack{expression};
  while (!stack.empty()) {
    Expression current = std::move(stack.back());
    stack.pop_back();
    if (!current) {
      continue;
    }
    if (current->Type() == TypeTag::kColumnValue &&
        current->AsColumnValue().GetColumnName().name == "*") {
      return true;
    }
    for (const Expression& child : ExpressionChildren(current)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

// M-union: merge UNION ALL / UNION DISTINCT branches over one shared table
// into a single scan. UNION ALL with pairwise-disjoint branch predicates is
// bag-identical to one OR-filtered scan; UNION DISTINCT additionally survives
// dedup either way. Anything else keeps the branch-wise set-operation path.
//
// Soundness gates (all must hold; the statement is untouched otherwise):
// - flat UNION ALL list, every pair kind ALL (bag merge) or every pair kind
//   DISTINCT (dedup merge); no INTERSECT/EXCEPT mixing, no grouped
//   SetOperationTree, default positional column matching only;
// - every part (head + branches) is a single plain-table source over the
//   SAME table and alias (so no qualifier rebinding is needed), with no
//   branch-level CTE map, grouping, aggregates, DISTINCT, LIMIT/OFFSET,
//   ORDER BY, QUALIFY, join condition, or USING clause. Head-level ORDER
//   BY/LIMIT/OFFSET are result-level modifiers and stay put; plain window
//   calls ride along (the merged shape routes through the normal window
//   lowering or relational fallback);
// - equal-width select lists with pairwise identical expressions (the head
//   part's output names win, matching set-operation semantics); `*`
//   expands over the shared table first;
// - UNION ALL additionally proves pairwise disjointness through one shared
//   column pinned to pairwise-different same-type constants (IS NULL counts
//   as its own constant; anything else stays relational).
bool UnionProjectionExpressionSafe(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp ||
      expression->Type() == TypeTag::kAggregateExp ||
      expression->Type() == TypeTag::kWindowFunctionExp) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             UnionProjectionExpressionSafe);
}

bool UnionBranchShapeOk(const SelectStatement& part, bool is_head) {
  if (part.Sources().size() != 1 || !part.WithQueries().empty() ||
      !part.GroupBy().empty() || part.Having() || part.Distinct() ||
      !part.DistinctOn().empty() || part.Qualify()) {
    return false;
  }
  // Parenthesized (grouped) set operations change meaning when flattened;
  // the top-level gate already confined this merge to ungrouped trees, and
  // branches must not nest set operations at all.
  const auto& tree = part.GetSetOperationTree();
  if (tree != nullptr && (tree->grouped || !is_head)) {
    return false;
  }
  // The head's own UNION ALL list is what is being merged (not a nested set
  // operation); its ORDER BY / LIMIT / OFFSET are result-level modifiers
  // that stay put. Branches must be bare.
  if (!is_head && (!part.UnionAll().empty() || part.HasLimit() ||
                   part.Offset() != 0 || !part.OrderBy().empty())) {
    return false;
  }
  const SelectSource& source = part.Sources().front();
  if (source.query != nullptr || source.unnest || source.is_lateral ||
      source.sample || source.table.empty() || !source.using_columns.empty() ||
      source.from_nested_join || source.join_condition ||
      (source.join_type != JoinType::kCross &&
       source.join_type != JoinType::kInner)) {
    return false;
  }
  // A window call is evaluated over the whole query block, so merging the
  // branch into one shared scan would recompute it over the union of every
  // branch's rows (wrong frames/ranks). Keep window-bearing branches on the
  // set-operation path.
  if (relational_detail::HasWindowFunctions(part)) {
    return false;
  }
  return std::ranges::all_of(
      part.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               !relational_detail::ContainsAggregate(item.expression) &&
               UnionProjectionExpressionSafe(item.expression);
      });
}

// Expands `*` select items of a single-table part over the catalog. False
// when unexpandable (unknown table stays relational).
bool ExpandUnionBranchStars(SelectStatement* part, TransactionContext& ctx) {
  if (part == nullptr || part->Sources().size() != 1) {
    return false;
  }
  bool has_star = false;
  for (const NamedExpression& item : part->SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      has_star = true;
      break;
    }
  }
  if (!has_star) {
    return true;
  }
  const SelectSource& source = part->Sources().front();
  StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(source.table);
  if (!found.HasValue()) {
    return false;
  }
  const std::string relation =
      source.alias.empty() ? source.table : source.alias;
  std::vector<NamedExpression> expanded;
  for (const NamedExpression& item : part->SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      const ColumnName& star = item.expression->AsColumnValue().GetColumnName();
      if (!star.schema.empty() && !IdentifierEquals(star.schema, relation) &&
          !IdentifierEquals(star.schema, source.table)) {
        return false;
      }
      if (item.star_modifiers) {
        std::unordered_set<std::string> resolved_except;
        std::unordered_set<std::string> resolved_replace;
        ExpandStarWithModifiers(item, relation, found.Value()->GetSchema(),
                                &expanded, &resolved_except, &resolved_replace);
        if (resolved_except.size() !=
                item.star_modifiers->except_columns.size() ||
            resolved_replace.size() !=
                item.star_modifiers->replace_items.size()) {
          return false;
        }
        continue;
      }
      for (size_t i = 0; i < found.Value()->GetSchema().ColumnCount(); ++i) {
        const std::string& col =
            found.Value()->GetSchema().GetColumn(i).Name().name;
        expanded.emplace_back(col, ColumnValueExp(ColumnName(relation, col)));
      }
      continue;
    }
    expanded.push_back(item);
  }
  part->SetSelectList(std::move(expanded));
  return true;
}

// Collects single-column equality-to-constant (or IS NULL) conjuncts for the
// disjointness proof: lowered column name -> constant, or nullopt for
// IS NULL (equal only to itself, so two IS NULL branches correctly fail the
// all-different proof while IS NULL vs a constant passes it).
void CollectEqualityConsts(
    const Expression& where,
    std::unordered_map<std::string, std::optional<Value>>* out) {
  if (!where || out == nullptr) {
    return;
  }
  for (const Expression& conjunct : SplitConjuncts(where)) {
    if (!conjunct) {
      continue;
    }
    if (conjunct->Type() == TypeTag::kBinaryExp &&
        conjunct->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
      const auto& binary = conjunct->AsBinaryExpression();
      const Expression* column_side = nullptr;
      const Expression* const_side = nullptr;
      if (binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kConstantValue) {
        column_side = &binary.Left();
        const_side = &binary.Right();
      } else if (binary.Right()->Type() == TypeTag::kColumnValue &&
                 binary.Left()->Type() == TypeTag::kConstantValue) {
        column_side = &binary.Right();
        const_side = &binary.Left();
      }
      if (column_side != nullptr && const_side != nullptr) {
        const ColumnName& column =
            (*column_side)->AsColumnValue().GetColumnName();
        std::string lowered = column.name;
        for (char& c : lowered) {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        const Value& constant = (*const_side)->AsConstantValue().GetValue();
        if (!constant.IsNull()) {
          out->emplace(lowered, constant);
        }
      }
      continue;
    }
    if (conjunct->Type() == TypeTag::kUnaryExp &&
        conjunct->AsUnaryExpression().Op() == UnaryOperation::kIsNull &&
        conjunct->AsUnaryExpression().Child()->Type() ==
            TypeTag::kColumnValue) {
      const ColumnName& column = conjunct->AsUnaryExpression()
                                     .Child()
                                     ->AsColumnValue()
                                     .GetColumnName();
      std::string lowered = column.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      out->emplace(lowered, std::nullopt);
    }
  }
}

bool UnionBranchesDisjoint(const std::vector<const SelectStatement*>& parts) {
  // One shared column pinned to pairwise-different same-type constants.
  // Extra conjuncts only narrow further, preserving disjointness.
  std::unordered_map<std::string, std::vector<std::optional<Value>>> by_column;
  for (const SelectStatement* part : parts) {
    std::unordered_map<std::string, std::optional<Value>> consts;
    CollectEqualityConsts(part->WhereClause(), &consts);
    for (const auto& [column, value] : consts) {
      by_column[column].push_back(value);
    }
  }
  for (const auto& [column, values] : by_column) {
    (void)column;
    if (values.size() != parts.size()) {
      continue;
    }
    bool pairwise_different = true;
    for (size_t i = 0; i < values.size() && pairwise_different; ++i) {
      for (size_t j = i + 1; j < values.size(); ++j) {
        const std::optional<Value>& left = values[i];
        const std::optional<Value>& right = values[j];
        if (!left.has_value() || !right.has_value()) {
          pairwise_different = left.has_value() != right.has_value();
        } else if (left->type != right->type || *left == *right) {
          pairwise_different = false;
        }
        if (!pairwise_different) {
          break;
        }
      }
    }
    if (pairwise_different) {
      return true;
    }
  }
  return false;
}

}  // namespace

bool MergeCompatibleUnionBranches(SelectStatement* outer,
                                  TransactionContext& ctx) {
  if (outer == nullptr || outer->UnionAll().empty()) {
    return false;
  }
  if (outer->GetSetOperationTree() != nullptr &&
      outer->GetSetOperationTree()->grouped) {
    return false;
  }
  if (outer->UnionByName()) {
    return false;
  }
  for (const SetOperationMatch& match : outer->Matches()) {
    if (match.corresponding || match.by_name || !match.columns.empty()) {
      return false;
    }
  }
  bool all_all = true;
  bool all_distinct = true;
  for (size_t i = 0; i < outer->UnionAll().size(); ++i) {
    const SetOperationKind kind = i < outer->SetOperationKinds().size()
                                      ? outer->SetOperationKinds()[i]
                                      : SetOperationKind::kUnionAll;
    all_all = all_all && kind == SetOperationKind::kUnionAll;
    all_distinct = all_distinct && kind == SetOperationKind::kUnion;
  }
  if (!all_all && !all_distinct) {
    return false;
  }
  // Parts: the head (this statement's own clauses; its ORDER BY / LIMIT /
  // OFFSET are result-level and stay put) plus every branch.
  std::vector<SelectStatement*> parts{outer};
  for (const auto& branch : outer->UnionAll()) {
    if (branch == nullptr) {
      return false;
    }
    parts.push_back(branch.get());
  }
  if (!UnionBranchShapeOk(*parts[0], /*is_head=*/true)) {
    return false;
  }
  for (size_t i = 1; i < parts.size(); ++i) {
    if (!UnionBranchShapeOk(*parts[i], /*is_head=*/false)) {
      return false;
    }
  }
  const SelectSource& head_source = parts[0]->Sources().front();
  for (size_t i = 1; i < parts.size(); ++i) {
    const SelectSource& source = parts[i]->Sources().front();
    if (!IdentifierEquals(source.table, head_source.table) ||
        !IdentifierEquals(source.alias.empty() ? source.table : source.alias,
                          head_source.alias.empty() ? head_source.table
                                                    : head_source.alias)) {
      return false;
    }
  }
  for (SelectStatement* part : parts) {
    if (!ExpandUnionBranchStars(part, ctx)) {
      return false;
    }
  }
  const std::vector<NamedExpression>& head_list = parts[0]->SelectList();
  if (head_list.empty()) {
    return false;
  }
  for (size_t i = 1; i < parts.size(); ++i) {
    const std::vector<NamedExpression>& branch_list = parts[i]->SelectList();
    if (branch_list.size() != head_list.size()) {
      return false;
    }
    for (size_t c = 0; c < head_list.size(); ++c) {
      if (!head_list[c].expression || !branch_list[c].expression ||
          head_list[c].expression->ToString() !=
              branch_list[c].expression->ToString()) {
        return false;
      }
    }
  }
  if (all_all) {
    std::vector<const SelectStatement*> const_parts(parts.begin(), parts.end());
    if (!UnionBranchesDisjoint(const_parts)) {
      return false;
    }
  }
  Expression merged;
  for (SelectStatement* part : parts) {
    Expression branch_where = part->WhereClause()
                                  ? part->WhereClause()
                                  : ConstantValueExp(Value(true));
    merged =
        merged ? BinaryExpressionExp(merged, BinaryOperation::kOr, branch_where)
               : branch_where;
  }
  if (!merged) {
    return false;
  }
  // An integer ORDER BY item is a SELECT-list ordinal. The set-operation
  // path translates it, but this merge collapses to a plain SELECT whose
  // ordinary path would treat the constant as a no-op sort key (silently
  // ignoring the ordering and any LIMIT). Bind the ordinal to the head
  // projection here, after every bail-out so a rejected merge leaves the
  // statement untouched.
  if (!outer->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> rebound_order;
    rebound_order.reserve(outer->OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : outer->OrderBy()) {
      if (term.expression &&
          term.expression->Type() == TypeTag::kConstantValue) {
        const Value& value = term.expression->AsConstantValue().GetValue();
        if (value.type == ValueType::kInt64 && !value.IsNull()) {
          const int64_t ordinal = value.value.int_value;
          if (ordinal < 1 || static_cast<size_t>(ordinal) > head_list.size()) {
            return false;
          }
          const NamedExpression& target =
              head_list[static_cast<size_t>(ordinal) - 1];
          if (!target.expression) {
            return false;
          }
          rebound_order.push_back(
              {target.expression, term.ascending, term.nulls_first});
          continue;
        }
      }
      rebound_order.push_back(term);
    }
    outer->SetOrderBy(std::move(rebound_order));
  }
  outer->SetWhereClause(std::move(merged));
  outer->ClearUnionAll();
  // Drop the (now stale) set-operation tree alongside the flat list: any
  // downstream reader, including relational fallbacks, must see the merged
  // single-query shape only.
  outer->SetSetOperationTree(nullptr);
  if (all_distinct) {
    outer->SetDistinct(true);
  }
  return true;
}

namespace {
// LATERAL decorrelation, minimal slice (TODO.md item 3a): a LATERAL site
// whose subquery is a single plain base table, row-preserving, with all
// outer references confined to column-to-column equality correlations in its
// WHERE, is convertible to a decorrelated join. The correlation conjuncts
// move to the site's ON condition (rebound to the derived alias, extending
// the inner projection with the key columns when they are not outputs),
// the lateral flag clears, and the proven M5 flattener consumes the now
// uncorrelated site when its own gates pass. Per-row lateral evaluation and
// the decorrelated join produce the same row multiset: each outer row pairs
// with exactly the inner rows satisfying the local predicates plus the
// equalities (inner join), or null-pads when none match (LEFT kept as-is).
// Volatile inner expressions would observe different evaluation counts
// across the two shapes, so they block the rewrite.
bool DecorrelateOneLateralSource(SelectStatement* outer, size_t index,
                                 TransactionContext& ctx) {
  std::vector<SelectSource> sources = outer->Sources();
  if (index >= sources.size()) {
    return false;
  }
  SelectSource site = sources[index];
  if (site.query == nullptr || !site.is_lateral || site.unnest ||
      site.alias.empty() || !site.using_columns.empty() ||
      site.from_nested_join) {
    return false;
  }
  if (site.join_type != JoinType::kCross &&
      site.join_type != JoinType::kInner && site.join_type != JoinType::kLeft) {
    return false;
  }
  const std::shared_ptr<SelectStatement>& inner = site.query;
  if (inner->Sources().size() != 1 || !inner->WithQueries().empty() ||
      !inner->GroupBy().empty() || inner->Having() || inner->Distinct() ||
      !inner->DistinctOn().empty() || inner->HasLimit() ||
      inner->Offset() != 0 || inner->Qualify() || !inner->UnionAll().empty() ||
      inner->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*inner) ||
      inner->SelectList().empty()) {
    return false;
  }
  const SelectSource& base = inner->Sources()[0];
  if (base.query != nullptr || base.unnest || base.is_lateral ||
      base.table.empty() || !base.using_columns.empty() ||
      base.from_nested_join || base.join_condition) {
    return false;
  }
  if (outer->IsRecursiveWith(base.table)) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> base_table = ctx.GetTable(base.table);
  if (!base_table.HasValue()) {
    return false;
  }
  const Schema& base_schema = base_table.Value()->GetSchema();
  const std::string inner_alias = base.alias.empty() ? base.table : base.alias;
  // Inner SELECT: named, immutable, subquery-free, star-free (a star would
  // hide whether a correlation key is projected).
  for (const NamedExpression& item : inner->SelectList()) {
    if (item.name.empty() || !item.expression ||
        relational_detail::ContainsAggregate(item.expression) ||
        ContainsQueryExpression(item.expression) ||
        !DerivedExpressionIsImmutable(item.expression) ||
        ContainsStarReference(item.expression)) {
      return false;
    }
  }
  // Outer scope identities: preceding sources only (later sources cannot be
  // referenced, and the site itself is the derived alias, not the base).
  std::unordered_set<std::string> outer_identities;
  for (size_t j = 0; j < index; ++j) {
    if (!sources[j].alias.empty()) {
      outer_identities.insert(sources[j].alias);
    }
    if (!sources[j].table.empty()) {
      outer_identities.insert(sources[j].table);
    }
  }
  auto is_inner = [&](const ColumnName& column) {
    if (column.schema.empty()) {
      return base_schema.Offset(ColumnName("", column.name)) >= 0;
    }
    return IdentifierEquals(column.schema, inner_alias) ||
           IdentifierEquals(column.schema, base.table);
  };
  auto is_outer = [&](const ColumnName& column) {
    if (column.schema.empty()) {
      return false;
    }
    return std::ranges::any_of(
        outer_identities, [&](const std::string& identity) {
          return IdentifierEquals(column.schema, identity);
        });
  };
  // Partition the inner WHERE into correlation equalities and the local
  // remainder. Every other outer reference anywhere in the inner statement
  // aborts the rewrite.
  struct Correlation {
    Expression outer_column;
    std::string inner_name;
  };
  std::vector<Correlation> correlations;
  std::vector<Expression> remainder;
  for (const Expression& conjunct : SplitConjuncts(inner->WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp ||
        conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
        conjunct->AsBinaryExpression().Left()->Type() !=
            TypeTag::kColumnValue ||
        conjunct->AsBinaryExpression().Right()->Type() !=
            TypeTag::kColumnValue) {
      if (conjunct && !DerivedExpressionIsImmutable(conjunct)) {
        return false;
      }
      if (conjunct) {
        for (const ColumnName& column : conjunct->TouchedColumns()) {
          if (!is_inner(column)) {
            return false;
          }
        }
        remainder.push_back(conjunct);
      }
      continue;
    }
    const ColumnName& left =
        conjunct->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
    const ColumnName& right =
        conjunct->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
    const ColumnName* inner_col = nullptr;
    const Expression* outer_expr = nullptr;
    if (is_inner(left) && is_outer(right)) {
      inner_col = &left;
      outer_expr = &conjunct->AsBinaryExpression().Right();
    } else if (is_inner(right) && is_outer(left)) {
      inner_col = &right;
      outer_expr = &conjunct->AsBinaryExpression().Left();
    } else {
      // Both sides inner (a local predicate), or unresolvable: the latter
      // may still name the outer scope, so only provably-local stays.
      if (!is_inner(left) || !is_inner(right)) {
        return false;
      }
      remainder.push_back(conjunct);
      continue;
    }
    correlations.push_back(Correlation{.outer_column = *outer_expr,
                                       .inner_name = inner_col->name});
  }
  if (correlations.empty()) {
    return false;
  }
  auto touches_outer = [&](const Expression& expression) {
    if (!expression) {
      return false;
    }
    return std::ranges::any_of(expression->TouchedColumns(),
                               [&](const ColumnName& column) {
                                 return column.name != "*" && !is_inner(column);
                               });
  };
  for (const NamedExpression& item : inner->SelectList()) {
    if (touches_outer(item.expression)) {
      return false;
    }
  }
  for (const auto& term : inner->OrderBy()) {
    if (touches_outer(term.expression)) {
      return false;
    }
  }
  // Extend the inner projection with correlation keys that are not outputs
  // (the ON condition below addresses them through the derived alias).
  // A clashing output name with different meaning aborts the rewrite.
  std::shared_ptr<SelectStatement> new_inner =
      std::make_shared<SelectStatement>(*inner);
  for (const Correlation& correlation : correlations) {
    std::string lowered = correlation.inner_name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    bool projected = false;
    for (const NamedExpression& item : new_inner->SelectList()) {
      std::string item_lowered = item.name;
      for (char& c : item_lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      if (item_lowered != lowered) {
        continue;
      }
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& projected_col =
          item.expression->AsColumnValue().GetColumnName();
      const bool same_base =
          (projected_col.schema.empty() &&
           base_schema.Offset(ColumnName("", projected_col.name)) >= 0 &&
           IdentifierEquals(projected_col.name, correlation.inner_name)) ||
          ((IdentifierEquals(projected_col.schema, inner_alias) ||
            IdentifierEquals(projected_col.schema, base.table)) &&
           IdentifierEquals(projected_col.name, correlation.inner_name));
      if (!same_base) {
        return false;
      }
      projected = true;
      break;
    }
    if (!projected) {
      std::vector<NamedExpression> extended = new_inner->SelectList();
      extended.emplace_back(
          correlation.inner_name,
          ColumnValueExp(ColumnName(inner_alias, correlation.inner_name)));
      new_inner->SetSelectList(std::move(extended));
    }
  }
  new_inner->SetWhereClause(remainder.empty() ? Expression{}
                                              : CombineConjuncts(remainder));
  // Publish the rewrite on the site: uncorrelated, explicitly joined, with
  // the correlations as the ON condition addressed through the derived
  // alias (the flattener rebinds them when it consumes the site).
  Expression on;
  for (const Correlation& correlation : correlations) {
    Expression term = BinaryExpressionExp(
        correlation.outer_column, BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(site.alias, correlation.inner_name)));
    on = on ? BinaryExpressionExp(on, BinaryOperation::kAnd, term) : term;
  }
  if (site.join_condition) {
    on = BinaryExpressionExp(site.join_condition, BinaryOperation::kAnd, on);
  }
  site.query = std::move(new_inner);
  site.is_lateral = false;
  if (site.join_type == JoinType::kCross) {
    site.join_type = JoinType::kInner;
  }
  site.join_condition = std::move(on);
  sources[index] = std::move(site);
  outer->SetSources(std::move(sources));
  // The proven M5 flattener consumes the uncorrelated site when its gates
  // pass; otherwise the site stays a single-evaluation uncorrelated derived
  // table, still cheaper than per-row lateral re-execution.
  (void)FlattenOneDerivedSource(outer, index, ctx);
  return true;
}

}  // namespace

bool DecorrelateSingleTableLaterals(SelectStatement* outer,
                                    TransactionContext& ctx) {
  if (outer == nullptr) {
    return false;
  }
  bool changed = false;
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    if (outer->Sources()[i].query != nullptr &&
        DecorrelateOneLateralSource(outer, i, ctx)) {
      changed = true;
    }
  }
  return changed;
}

namespace {

// ---------------------------------------------------------------------------
// Sibling derived-aggregate fusion (TPC-H Q21 shape: `multi` + `only_late`
// both aggregate lineitem under GROUP BY l_orderkey).  Two or more derived
// tables over the SAME base table with the SAME GROUP BY key list compute
// one fused aggregation over a single scan instead of one scan + fold +
// join build per sibling:
//
//   FROM t, (SELECT k, a1 FROM t WHERE p1 GROUP BY k HAVING h1) d1,
//           (SELECT k, a2 FROM t WHERE p2 GROUP BY k HAVING h2) d2
//   WHERE ... d1.k = X AND d2.k = X ...
// ==> FROM t, (SELECT k AS __fk0, <p1?a1> AS __fa0_0, <p2?a2> AS __fa1_0,
//              COUNT(CASE WHEN p1 THEN 1 END) AS __fw0) __fagg
//      GROUP BY k
//      WHERE ... __fagg.__fk0 = X ... AND __fw0 > 0 AND h1-over-__fa0_* ...
//
// Correctness contract:
// - CASE wrapping is exact for every membership aggregate (COUNT/SUM/AVG/
//   MIN/MAX incl. DISTINCT): rows failing the member's WHERE contribute
//   NULL, which each of those aggregates skips, so the fused value equals
//   the member's own filtered aggregation.
// - Group membership is NOT carried by the wrapped aggregates (COUNT(x)=0
//   cannot distinguish "no row satisfied p" from "rows existed but x was
//   NULL"), so a member with a WHERE adds an explicit
//   COUNT(CASE WHEN p THEN 1 END) > 0 witness conjunct.
// - Each member's HAVING lifts to an outer conjunct with its aggregate
//   references replaced by fused output columns (aggregates appearing only
//   in HAVING are exposed as fused columns for that purpose).
//
// The rewrite is all-or-nothing per group: every derived piece is built
// before the first mutation, and any obstacle (bare-name ambiguity, star or
// subquery in the outer scope, un-mappable reference, unsupported aggregate
// shape) aborts with the statement untouched.  TINYLAMB_FUSE_SIBLING_AGG=0
// disables.
struct FuseMember {
  size_t index{0};
  std::string alias;  // derived-table alias used by outer references
  std::shared_ptr<SelectStatement> body;
  std::vector<Expression> keys;
  // Lowered outer output name -> fused column name.
  std::unordered_map<std::string, std::string> output_to_fused;
  // Aggregate ToString -> fused column name (select-list and HAVING-only).
  std::unordered_map<std::string, std::string> agg_to_fused;
  // Fused column name -> original aggregate expression.
  std::vector<std::pair<std::string, Expression>> aggs;
  Expression where;
  Expression having;
};

bool FusableAggregateShape(const AggregateExpression& agg) {
  switch (agg.GetType()) {
    case AggregationType::kCount:
    case AggregationType::kSum:
    case AggregationType::kAvg:
    case AggregationType::kMin:
    case AggregationType::kMax:
      break;
    default:
      return false;
  }
  if (agg.Having() != AggregateHavingModifier::kNone ||
      agg.HavingCondition() != nullptr || !agg.InnerOrderBy().empty() ||
      agg.InnerLimit().has_value() || !agg.TrailingArgs().empty() ||
      agg.SecondaryArg() != nullptr) {
    return false;
  }
  const Expression& child = agg.Child();
  if (!child) {
    return false;
  }
  if (child->Type() == TypeTag::kColumnValue &&
      child->AsColumnValue().GetColumnName().name == "*") {
    return agg.GetType() == AggregationType::kCount && !agg.Distinct();
  }
  return DerivedExpressionIsImmutable(child) && !ContainsQueryExpression(child);
}

// A lifted HAVING may reference aggregates (replaced by fused columns); every
// other part of it must be immutable and free of subqueries.
bool HavingLiftIsSafe(
    const Expression& expression) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    return FusableAggregateShape(expression->AsAggregateExpression());
  }
  if (ContainsQueryExpression(expression)) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return HavingLiftIsSafe(child); });
}

bool FusableSiblingBody(const SelectStatement& query) {
  if (!query.WithQueries().empty() || query.GetSetOperationTree() != nullptr ||
      !query.UnionAll().empty() || query.Distinct() || query.HasDistinctOn() ||
      query.HasLimit() || query.Offset() != 0 || query.Qualify() ||
      relational_detail::HasWindowFunctions(query) || query.GroupBy().empty() ||
      query.Sources().size() != 1) {
    return false;
  }
  const SelectSource& source = query.Sources()[0];
  // The BODY base alias may be empty (bare `FROM lineitem`): members share
  // the same (table, alias) pair via the fusion signature, so the fused body
  // reuses the representative's source verbatim.  Only the derived table's
  // own alias (checked by the caller) must be non-empty for qualified outer
  // references.
  if (source.query != nullptr || source.unnest != nullptr ||
      source.is_lateral || source.from_nested_join ||
      source.sample.has_value() || !source.using_columns.empty() ||
      source.table.empty() ||
      (source.join_type != JoinType::kCross &&
       source.join_type != JoinType::kInner)) {
    return false;
  }
  if (query.WhereClause() &&
      (!DerivedExpressionIsImmutable(query.WhereClause()) ||
       relational_detail::ContainsAggregate(query.WhereClause()))) {
    return false;
  }
  for (const Expression& key : query.GroupBy()) {
    if (!key || key->Type() == TypeTag::kAggregateExp ||
        relational_detail::ContainsAggregate(key) ||
        ContainsQueryExpression(key) || !DerivedExpressionIsImmutable(key)) {
      return false;
    }
  }
  for (const NamedExpression& item : query.SelectList()) {
    if (item.name.empty() || !item.expression) {
      return false;
    }
    const Expression& expression = item.expression;
    if (expression->Type() == TypeTag::kColumnValue &&
        expression->AsColumnValue().GetColumnName().name == "*") {
      return false;
    }
    if (ContainsQueryExpression(expression)) {
      return false;
    }
    const bool is_key = std::ranges::any_of(
        query.GroupBy(), [&expression](const Expression& key) {
          return key->ToString() == expression->ToString();
        });
    if (is_key) {
      continue;
    }
    if (expression->Type() != TypeTag::kAggregateExp ||
        !FusableAggregateShape(expression->AsAggregateExpression())) {
      return false;
    }
  }
  return query.Having() == nullptr ||
         (!ContainsQueryExpression(query.Having()) &&
          HavingLiftIsSafe(query.Having()));
}

// True when `expression` contains a bare column whose lowered name is in
// `forbidden`, or a subquery (whose inner scope could reference the member
// aliases in ways column substitution cannot reach).  Star children of
// aggregates (COUNT(*)) are expected here and deliberately ignored; star
// select items are rejected separately.
bool OuterReferenceIsUnsafe(  // NOLINT(misc-no-recursion)
    const Expression& expression,
    const std::unordered_set<std::string>& forbidden) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return false;
    }
    return column.schema.empty() &&
           forbidden.contains(LoweredStarName(column.name));
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             [&](const Expression& child) {
                               return OuterReferenceIsUnsafe(child, forbidden);
                             });
}

// Rewrites references to a member's outputs: `alias.name` becomes
// `__fagg.<fused>`; a bare name maps only when `lift_bare_names` (lifted
// HAVINGs still speak the member body's scope).  An aggregate node whose
// ToString is in `agg_to_fused` becomes its fused column.  Returns nullopt
// when a reference cannot be mapped (the caller aborts the whole fusion).
std::optional<Expression> RebindToFused(  // NOLINT(misc-no-recursion)
    const Expression& expression, const FuseMember& member,
    const std::string& fused_alias, bool lift_bare_names,
    const std::unordered_map<std::string, std::string>* agg_to_fused) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    if (agg_to_fused == nullptr) {
      // Outer-scope mode: this aggregate belongs to the outer query (e.g.
      // COUNT(*) over the join); leave it untouched.  Member aggregates
      // only occur inside a lifted HAVING, which passes its own map.
      return expression;
    }
    const auto found =
        agg_to_fused->find(expression->AsAggregateExpression().ToString());
    if (found == agg_to_fused->end()) {
      return std::nullopt;
    }
    return ColumnValueExp(ColumnName(fused_alias, found->second));
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return std::nullopt;
    }
    const bool qualified =
        !column.schema.empty() && IdentifierEquals(column.schema, member.alias);
    const bool bare = column.schema.empty() && lift_bare_names;
    if (qualified || bare) {
      const auto found =
          member.output_to_fused.find(LoweredStarName(column.name));
      if (found == member.output_to_fused.end()) {
        return std::nullopt;
      }
      return ColumnValueExp(ColumnName(fused_alias, found->second));
    }
    return expression;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> rebound;
  rebound.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped = RebindToFused(
        child, member, fused_alias, lift_bare_names, agg_to_fused);
    if (!mapped.has_value()) {
      return std::nullopt;
    }
    rebound.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(rebound));
}

Expression ConditionalAggregate(const Expression& where,
                                const AggregateExpression& agg) {
  const Expression& child = agg.Child();
  Expression branch;
  if (child->Type() == TypeTag::kColumnValue &&
      child->AsColumnValue().GetColumnName().name == "*") {
    branch = ConstantValueExp(Value(static_cast<int64_t>(1)));
  } else {
    branch = child;
  }
  return AggregateExpressionExp(agg.GetType(),
                                CaseExpressionExp({{where, branch}}, nullptr),
                                agg.Distinct());
}

}  // namespace

size_t FuseSiblingDerivedAggregates(SelectStatement* outer) {
  const bool fusion_disabled = [] {
    const char* hatch = std::getenv("TINYLAMB_FUSE_SIBLING_AGG");
    return hatch != nullptr && std::string_view(hatch) == "0";
  }();
  if (outer == nullptr || fusion_disabled || outer->Sources().size() < 2 ||
      outer->GetSetOperationTree() != nullptr || !outer->UnionAll().empty()) {
    return 0;
  }
  // Collect candidate members keyed by (table, alias, GROUP BY signature).
  std::unordered_map<std::string, std::vector<FuseMember>> candidates;
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    const SelectSource& source = outer->Sources()[i];
    if (source.query == nullptr || source.alias.empty() ||
        (source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kInner) ||
        !FusableSiblingBody(*source.query)) {
      continue;
    }
    const SelectStatement& body = *source.query;
    std::string signature = body.Sources()[0].table;
    signature.push_back('\x1f');
    signature += body.Sources()[0].alias;
    signature.push_back('\x1f');
    for (const Expression& key : body.GroupBy()) {
      signature += key->ToString();
      signature.push_back('\x1f');
    }
    FuseMember member;
    member.index = i;
    member.alias = source.alias;
    member.body = source.query;
    member.keys = body.GroupBy();
    member.where = body.WhereClause();
    member.having = body.Having();
    candidates[std::move(signature)].push_back(std::move(member));
  }
  // Deterministic pick: the qualifying group whose representative (first
  // member) sits at the lowest source index.
  std::vector<FuseMember>* group = nullptr;
  for (auto& [signature, members] : candidates) {
    if (members.size() < 2) {
      continue;
    }
    if (group == nullptr || members.front().index < group->front().index) {
      group = &members;
    }
  }
  if (group == nullptr) {
    return 0;
  }
  std::vector<FuseMember>& members = *group;
  // Distinct aliases are a precondition for unambiguous outer references.
  for (size_t a = 0; a < members.size(); ++a) {
    for (size_t b = a + 1; b < members.size(); ++b) {
      if (IdentifierEquals(members[a].alias, members[b].alias)) {
        return 0;
      }
    }
  }
  const FuseMember& representative = members.front();
  const std::string fused_alias =
      "__fagg" + std::to_string(representative.index);

  // Per-member maps; fused aggregate columns are "__fa<i>_<k>" in select
  // order, then HAVING-only aggregates; witness columns are "__fw<i>".
  for (size_t m = 0; m < members.size(); ++m) {
    const SelectStatement& body = *members[m].body;
    size_t agg_slot = 0;
    for (const NamedExpression& item : body.SelectList()) {
      std::string lowered = LoweredStarName(item.name);
      const bool is_key =
          std::ranges::any_of(members[m].keys, [&item](const Expression& key) {
            return key->ToString() == item.expression->ToString();
          });
      if (is_key) {
        for (size_t k = 0; k < members[m].keys.size(); ++k) {
          if (members[m].keys[k]->ToString() == item.expression->ToString()) {
            members[m].output_to_fused[lowered] = "__fk" + std::to_string(k);
            break;
          }
        }
        continue;
      }
      const std::string fused_name =
          "__fa" + std::to_string(m) + "_" + std::to_string(agg_slot++);
      members[m]
          .agg_to_fused[item.expression->AsAggregateExpression().ToString()] =
          fused_name;
      members[m].output_to_fused[lowered] = fused_name;
      members[m].aggs.emplace_back(fused_name, item.expression);
    }
    // HAVING-only aggregates (e.g. Q21's HAVING COUNT(DISTINCT l_suppkey)).
    const std::function<std::optional<bool>(const Expression&)> collect =
        [&](const Expression& expression) -> std::optional<bool> {
      if (!expression) {
        return true;
      }
      if (expression->Type() == TypeTag::kAggregateExp) {
        const std::string key = expression->AsAggregateExpression().ToString();
        if (!members[m].agg_to_fused.contains(key)) {
          const std::string fused_name =
              "__fa" + std::to_string(m) + "_" + std::to_string(agg_slot++);
          members[m].agg_to_fused[key] = fused_name;
          members[m].aggs.emplace_back(fused_name, expression);
        }
        return true;
      }
      for (const Expression& child : ExpressionChildren(expression)) {
        if (!collect(child).has_value()) {
          return std::nullopt;
        }
      }
      return true;
    };
    if (members[m].having != nullptr &&
        !collect(members[m].having).has_value()) {
      return 0;
    }
  }

  // Bare-name / star / subquery hazards anywhere in the outer scope.
  std::unordered_set<std::string> forbidden;
  for (const FuseMember& member : members) {
    for (const auto& [name, fused] : member.output_to_fused) {
      forbidden.insert(name);
    }
  }
  const auto unsafe_here = [&](const Expression& expression) {
    return OuterReferenceIsUnsafe(expression, forbidden);
  };
  if (unsafe_here(outer->WhereClause()) || unsafe_here(outer->Having()) ||
      unsafe_here(outer->Qualify())) {
    return 0;
  }
  for (const NamedExpression& item : outer->SelectList()) {
    if (item.expression != nullptr &&
        item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      // SELECT * / rel.* expands from the FROM schema; the fusion renames
      // the member outputs, so expansion must not observe the pre-fusion
      // shape.
      return 0;
    }
    if (unsafe_here(item.expression)) {
      return 0;
    }
  }
  for (const Expression& key : outer->GroupBy()) {
    if (unsafe_here(key)) {
      return 0;
    }
  }
  for (const Expression& term : outer->DistinctOn()) {
    if (unsafe_here(term)) {
      return 0;
    }
    // DISTINCT ON keys resolve near the projection boundary; a fused member
    // reference has no output column to bind to.  Keep those statements
    // unfused rather than guessing the evaluation scope.
    const bool references_member =
        std::ranges::any_of(members, [&term](const FuseMember& member) {
          bool found = false;
          const auto walk = [&](const auto& self,
                                const Expression& expression) {
            if (!expression || found) {
              return;
            }
            if (expression->Type() == TypeTag::kColumnValue) {
              const ColumnName& column =
                  expression->AsColumnValue().GetColumnName();
              if (!column.schema.empty() &&
                  IdentifierEquals(column.schema, member.alias)) {
                found = true;
              }
              return;
            }
            for (const Expression& child : ExpressionChildren(expression)) {
              self(self, child);
            }
          };
          walk(walk, term);
          return found;
        });
    if (references_member) {
      return 0;
    }
  }
  for (const SelectStatement::OrderByTerm& term : outer->OrderBy()) {
    if (unsafe_here(term.expression)) {
      return 0;
    }
  }
  for (const SelectSource& source : outer->Sources()) {
    if (unsafe_here(source.join_condition)) {
      return 0;
    }
  }

  // Build every derived piece before the first mutation.
  std::vector<NamedExpression> fused_list;
  fused_list.reserve(representative.keys.size());
  for (size_t k = 0; k < representative.keys.size(); ++k) {
    fused_list.emplace_back("__fk" + std::to_string(k), representative.keys[k]);
  }
  for (const FuseMember& member : members) {
    for (const auto& [fused_name, agg_expression] : member.aggs) {
      Expression fused_expression =
          member.where != nullptr
              ? ConditionalAggregate(member.where,
                                     agg_expression->AsAggregateExpression())
              : agg_expression;
      fused_list.emplace_back(fused_name, fused_expression);
    }
    if (member.where != nullptr) {
      fused_list.emplace_back(
          "__fw" + std::to_string(member.index),
          AggregateExpressionExp(
              AggregationType::kCount,
              CaseExpressionExp(
                  {{member.where, ConstantValueExp(Value(int64_t(1)))}},
                  nullptr)));
    }
  }
  std::vector<Expression> appended;
  appended.reserve(members.size());
  Expression inner_having;
  auto append_inner_having = [&](Expression conjunct) {
    if (!conjunct) {
      return;
    }
    inner_having =
        inner_having
            ? BinaryExpressionExp(inner_having, BinaryOperation::kAnd, conjunct)
            : std::move(conjunct);
  };
  for (const FuseMember& member : members) {
    if (member.having != nullptr) {
      std::optional<Expression> lifted = RebindToFused(
          member.having, member, fused_alias, true, &member.agg_to_fused);
      if (!lifted.has_value()) {
        return 0;
      }
      appended.push_back(std::move(*lifted));
      std::optional<Expression> inner_lifted =
          RebindToFused(member.having, member, "", true, &member.agg_to_fused);
      if (inner_lifted.has_value()) {
        append_inner_having(std::move(*inner_lifted));
      }
    }
    if (member.where != nullptr) {
      appended.push_back(BinaryExpressionExp(
          ColumnValueExp(
              ColumnName(fused_alias, "__fw" + std::to_string(member.index))),
          BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t(0)))));
      append_inner_having(BinaryExpressionExp(
          ColumnValueExp(ColumnName("", "__fw" + std::to_string(member.index))),
          BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t(0)))));
    }
  }
  // Members that disappear from FROM keep their join predicates. Each
  // member's ON clause sits in the outer scope and may reference any member
  // in the group (e.g. `ON d1.a = d2.a`), so rebind through every member.
  // When the representative is the leading FROM source (index 0,
  // JoinType::kCross), `Sources()[0].join_condition` is never evaluated by
  // execution routes, so lifted join predicates must be appended to the outer
  // WHERE clause instead.
  Expression rep_condition;
  std::vector<Expression> rebound_member_joins;
  for (const FuseMember& member : members) {
    const Expression& condition = outer->Sources()[member.index].join_condition;
    if (!condition) {
      continue;
    }
    std::optional<Expression> current = condition;
    for (const FuseMember& m : members) {
      current = RebindToFused(*current, m, fused_alias, false, &m.agg_to_fused);
      if (!current.has_value()) {
        return 0;
      }
    }
    rebound_member_joins.push_back(std::move(*current));
  }
  if (!rebound_member_joins.empty()) {
    if (representative.index == 0) {
      for (Expression& jc : rebound_member_joins) {
        appended.push_back(std::move(jc));
      }
    } else {
      rep_condition = rebound_member_joins.front();
      for (size_t i = 1; i < rebound_member_joins.size(); ++i) {
        rep_condition = BinaryExpressionExp(
            rep_condition, BinaryOperation::kAnd, rebound_member_joins[i]);
      }
    }
  }
  // Kept (non-member) sources may reference member aliases in their own
  // join conditions; rebind through every member (conditions naming no
  // member alias pass through unchanged).
  std::vector<Expression> kept_conditions(outer->Sources().size());
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    if (std::ranges::any_of(members, [i](const FuseMember& member) {
          return member.index == i;
        })) {
      continue;
    }
    const Expression& condition = outer->Sources()[i].join_condition;
    if (!condition) {
      continue;
    }
    std::optional<Expression> current = condition;
    for (const FuseMember& member : members) {
      current = RebindToFused(*current, member, fused_alias, false, nullptr);
      if (!current.has_value()) {
        return 0;
      }
    }
    kept_conditions[i] = std::move(*current);
  }
  // Same treatment for every remaining outer expression.
  const auto rebind_outer =
      [&](const Expression& expression) -> std::optional<Expression> {
    if (!expression) {
      return expression;
    }
    std::optional<Expression> current = expression;
    for (const FuseMember& member : members) {
      current = RebindToFused(*current, member, fused_alias, false, nullptr);
      if (!current.has_value()) {
        return std::nullopt;
      }
    }
    return current;
  };
  std::optional<Expression> rebound_where = rebind_outer(outer->WhereClause());
  if (!rebound_where.has_value()) {
    return 0;
  }
  std::vector<Expression> rebound_select;
  for (const NamedExpression& item : outer->SelectList()) {
    std::optional<Expression> rebound = rebind_outer(item.expression);
    if (!rebound.has_value()) {
      return 0;
    }
    rebound_select.push_back(std::move(*rebound));
  }
  std::vector<Expression> rebound_group;
  for (const Expression& key : outer->GroupBy()) {
    std::optional<Expression> rebound = rebind_outer(key);
    if (!rebound.has_value()) {
      return 0;
    }
    rebound_group.push_back(std::move(*rebound));
  }
  std::optional<Expression> rebound_having = rebind_outer(outer->Having());
  if (!rebound_having.has_value()) {
    return 0;
  }
  std::optional<Expression> rebound_qualify = rebind_outer(outer->Qualify());
  if (!rebound_qualify.has_value()) {
    return 0;
  }
  std::vector<Expression> rebound_distinct_on;
  for (const Expression& term : outer->DistinctOn()) {
    std::optional<Expression> rebound = rebind_outer(term);
    if (!rebound.has_value()) {
      return 0;
    }
    rebound_distinct_on.push_back(std::move(*rebound));
  }
  std::vector<SelectStatement::OrderByTerm> rebound_order;
  rebound_order.reserve(outer->OrderBy().size());
  for (const SelectStatement::OrderByTerm& term : outer->OrderBy()) {
    SelectStatement::OrderByTerm copied = term;
    if (term.expression != nullptr) {
      // ApplyOrderBy evaluates keys against the POST-projection output
      // schema, so a fused member reference must name an output column:
      // rewrite `d1.k` to the outer select item that spells the same
      // expression (its output name). A member reference with no matching
      // select item could not be resolved after projection before the fusion
      // either; refuse instead of guessing.
      const bool references_member =
          std::ranges::any_of(members, [&term](const FuseMember& member) {
            return !IdentifierEquals(member.alias, "") && [&] {
              bool found = false;
              const auto walk = [&](const auto& self,
                                    const Expression& expression) -> void {
                if (!expression || found) {
                  return;
                }
                if (expression->Type() == TypeTag::kColumnValue) {
                  const ColumnName& column =
                      expression->AsColumnValue().GetColumnName();
                  if (!column.schema.empty() &&
                      IdentifierEquals(column.schema, member.alias)) {
                    found = true;
                  }
                  return;
                }
                for (const Expression& child : ExpressionChildren(expression)) {
                  self(self, child);
                }
              };
              walk(walk, term.expression);
              return found;
            }();
          });
      if (references_member) {
        const NamedExpression* match = nullptr;
        for (const NamedExpression& item : outer->SelectList()) {
          if (item.expression != nullptr &&
              item.expression->ToString() == term.expression->ToString()) {
            match = &item;
            break;
          }
        }
        if (match == nullptr || match->name.empty()) {
          return 0;
        }
        copied.expression = ColumnValueExp(ColumnName("", match->name));
      } else {
        std::optional<Expression> rebound = rebind_outer(term.expression);
        if (!rebound.has_value()) {
          return 0;
        }
        copied.expression = std::move(*rebound);
      }
    }
    rebound_order.push_back(std::move(copied));
  }

  // Commit: sources first (drops the non-representative member queries),
  // then the fused body, then the outer expressions.
  std::vector<SelectSource> sources;
  sources.reserve(outer->Sources().size() - members.size() + 1);
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    const SelectSource& source = outer->Sources()[i];
    if (i == representative.index) {
      sources.emplace_back("", fused_alias, source.query, source.join_type,
                           rep_condition);
      continue;
    }
    if (std::ranges::any_of(members, [i](const FuseMember& member) {
          return member.index == i;
        })) {
      continue;
    }
    SelectSource kept = source;
    kept.join_condition = kept_conditions[i];
    sources.push_back(std::move(kept));
  }

  representative.body->SetSelectList(std::move(fused_list));
  representative.body->SetWhereClause(nullptr);
  representative.body->SetHaving(std::move(inner_having));
  outer->SetSources(std::move(sources));

  Expression where = *rebound_where;
  for (Expression& conjunct : appended) {
    where = where ? BinaryExpressionExp(where, BinaryOperation::kAnd, conjunct)
                  : conjunct;
  }
  outer->SetWhereClause(where);
  std::vector<NamedExpression> select_list;
  select_list.reserve(outer->SelectList().size());
  for (size_t i = 0; i < outer->SelectList().size(); ++i) {
    select_list.emplace_back(outer->SelectList()[i].name,
                             std::move(rebound_select[i]));
  }
  outer->SetSelectList(std::move(select_list));
  outer->SetGroupBy(std::move(rebound_group));
  outer->SetHaving(*rebound_having);
  outer->SetQualify(*rebound_qualify);
  if (!rebound_distinct_on.empty()) {
    outer->SetDistinctOn(std::move(rebound_distinct_on));
  }
  outer->SetOrderBy(std::move(rebound_order));
  return 1;
}

namespace {}  // namespace

bool FlattenDerivedSources(SelectStatement* outer, TransactionContext& ctx,
                           const LiftedCtes* lifted) {
  // A covered layer (LiftedCtes::fully_covered) leaves only vestigial map
  // entries, which no longer block flattening. Call adjacency guarantees
  // this: nothing mutates the map between lifting and this call.
  if (outer == nullptr || (!outer->WithQueries().empty() &&
                           (lifted == nullptr || !lifted->fully_covered))) {
    return false;
  }
  bool changed = false;
  // Depth-first: an inner derived table flattens before its consumer, so a
  // nested derived source is already a base table when the outer level runs.
  for (const SelectSource& source : outer->Sources()) {
    if (source.query != nullptr &&
        FlattenDerivedSources(source.query.get(), ctx, lifted)) {
      changed = true;
    }
  }
  // M5+ slice: a lone derived table over a plain inner-join core (TPC-H
  // Q9's profit subquery) flattens even under GROUP BY, whose output
  // references substitute to the inner expressions verbatim.  It fires only
  // under TINYLAMB_SCALAR_FN_CASCADES, the "grouped scalar functions through
  // the Cascades bridge" hatch: the flattening exists to expose the join
  // core to the bridge, and un-gated it measurably slows the default
  // relational route (Q9 SF1 paired runs: ~2.46s derived vs ~2.93s
  // flattened -- the derived path materializes the filtered core once and
  // the grouped finish reads the small relation).
  static const bool scalar_functions_cascades = [] {
    const char* hatch = std::getenv("TINYLAMB_SCALAR_FN_CASCADES");
    return hatch != nullptr && std::string_view(hatch) == "1";
  }();
  if (scalar_functions_cascades && outer->Sources().size() == 1 &&
      outer->Sources()[0].query != nullptr &&
      FlattenGroupedDerivedJoinCore(outer)) {
    return true;
  }
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    if (outer->Sources()[i].query != nullptr &&
        FlattenOneDerivedSource(outer, i, ctx)) {
      changed = true;
    }
  }
  return changed;
}

namespace {

// Flattens `SELECT ... FROM (SELECT ... FROM t1 JOIN t2 ... WHERE ...) d
// [GROUP BY ...]` when the outer statement has exactly that one source: the
// inner join core is spliced into the outer FROM verbatim, the inner WHERE
// merges into the outer WHERE (inner joins only), and every outer reference
// to a derived output substitutes the inner item's expression.  Inner
// expressions keep their own qualifiers/bare names, which stay valid because
// the whole inner scope moves with them.
//
// Kept deliberately narrow: rejects outer joins, lateral/unnest/using/nested
// sources, aggregates/set-ops/limit/offset/distinct/window/CTE inside the
// derived table, volatile or subquery-bearing select items, star items, and
// any outer reference a plain substitution cannot name.
bool FlattenGroupedDerivedJoinCore(SelectStatement* outer) {
  const SelectSource& derived = outer->Sources()[0];
  const std::shared_ptr<SelectStatement>& inner = derived.query;
  if (inner == nullptr || derived.alias.empty() || derived.is_lateral ||
      derived.unnest || !derived.using_columns.empty() ||
      derived.from_nested_join || derived.sample ||
      derived.join_type != JoinType::kCross) {
    return false;
  }
  // Row-preserving inner only: no grouping, dedup, truncation or hidden
  // scopes.  A window/set-op/aggregate inside would change row counts, and a
  // CTE inside brings its own materialization lifetime.
  if (!inner->WithQueries().empty() || !inner->GroupBy().empty() ||
      inner->Having() || inner->Distinct() || !inner->DistinctOn().empty() ||
      inner->HasLimit() || inner->Offset() != 0 || inner->Qualify() ||
      !inner->UnionAll().empty() || inner->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*inner) ||
      inner->SelectList().empty() || inner->Sources().empty()) {
    return false;
  }
  // Plain inner-join core of base tables only.
  for (const SelectSource& source : inner->Sources()) {
    if (source.query != nullptr || source.unnest || source.is_lateral ||
        !source.using_columns.empty() || source.from_nested_join ||
        source.sample || source.table.empty()) {
      return false;
    }
    if (source.join_type != JoinType::kCross &&
        source.join_type != JoinType::kInner) {
      return false;
    }
  }
  // Each output must be a named, immutable, subquery-free expression; the
  // map stores it verbatim (the inner scope survives the splice).
  std::unordered_map<std::string, Expression> outputs;
  for (const NamedExpression& item : inner->SelectList()) {
    if (item.name.empty() || !item.expression ||
        (item.expression->Type() == TypeTag::kColumnValue &&
         item.expression->AsColumnValue().GetColumnName().name == "*")) {
      return false;
    }
    if (relational_detail::ContainsAggregate(item.expression) ||
        ContainsQueryExpression(item.expression) ||
        !DerivedExpressionIsImmutable(item.expression)) {
      return false;
    }
    std::string lowered = item.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!outputs.try_emplace(std::move(lowered), item.expression).second) {
      return false;
    }
  }
  if (inner->WhereClause() &&
      (ContainsQueryExpression(inner->WhereClause()) ||
       !DerivedExpressionIsImmutable(inner->WhereClause()))) {
    return false;
  }
  // Substitute derived-output references across the outer statement.  The
  // outer has no other source, so any bare name that is not a derived output
  // (and not an ordinal constant) resolved nowhere before and must resolve
  // nowhere after: reject instead of letting it bind to a spliced base
  // column.
  const std::function<std::optional<Expression>(const Expression&)> substitute =
      [&](const Expression& expression) -> std::optional<Expression> {
    if (!expression) {
      return expression;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (column.name == "*") {
        return std::nullopt;
      }
      if (!column.schema.empty()) {
        if (!IdentifierEquals(column.schema, derived.alias)) {
          return std::nullopt;
        }
        std::string lowered = column.name;
        for (char& c : lowered) {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        const auto found = outputs.find(lowered);
        if (found == outputs.end()) {
          return std::nullopt;
        }
        return found->second;
      }
      std::string lowered = column.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      const auto found = outputs.find(lowered);
      if (found == outputs.end()) {
        return std::nullopt;
      }
      return found->second;
    }
    std::vector<Expression> children = ExpressionChildren(expression);
    std::vector<Expression> substituted;
    substituted.reserve(children.size());
    for (const Expression& child : children) {
      std::optional<Expression> mapped = substitute(child);
      if (!mapped.has_value()) {
        return std::nullopt;
      }
      substituted.push_back(std::move(*mapped));
    }
    return WithExpressionChildren(expression, std::move(substituted));
  };
  std::vector<NamedExpression> new_select;
  new_select.reserve(outer->SelectList().size());
  for (const NamedExpression& item : outer->SelectList()) {
    std::optional<Expression> mapped = substitute(item.expression);
    if (!mapped.has_value()) {
      return false;
    }
    new_select.emplace_back(item.name, std::move(*mapped));
  }
  std::optional<Expression> new_where = substitute(outer->WhereClause());
  std::optional<Expression> new_having = substitute(outer->Having());
  std::optional<Expression> new_qualify = substitute(outer->Qualify());
  if (!new_where.has_value() || !new_having.has_value() ||
      !new_qualify.has_value()) {
    return false;
  }
  std::vector<Expression> new_group;
  new_group.reserve(outer->GroupBy().size());
  for (const Expression& expression : outer->GroupBy()) {
    std::optional<Expression> mapped = substitute(expression);
    if (!mapped.has_value()) {
      return false;
    }
    new_group.push_back(std::move(*mapped));
  }
  std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
  for (SelectStatement::OrderByTerm& term : new_order) {
    std::optional<Expression> mapped = substitute(term.expression);
    if (!mapped.has_value()) {
      return false;
    }
    term.expression = std::move(*mapped);
  }
  // Publish the inner join core as the outer FROM and merge the WHERE.
  Expression merged_where = std::move(*new_where);
  if (inner->WhereClause()) {
    merged_where =
        merged_where ? BinaryExpressionExp(merged_where, BinaryOperation::kAnd,
                                           inner->WhereClause())
                     : inner->WhereClause();
  }
  outer->SetSources(inner->Sources());
  outer->SetWhereClause(std::move(merged_where));
  outer->SetSelectList(std::move(new_select));
  outer->SetGroupBy(std::move(new_group));
  outer->SetHaving(std::move(*new_having));
  outer->SetQualify(std::move(*new_qualify));
  outer->SetOrderBy(std::move(new_order));
  return true;
}

bool FlattenOneDerivedSource(SelectStatement* outer, size_t index,
                             TransactionContext& ctx) {
  std::vector<SelectSource> sources = outer->Sources();
  if (index >= sources.size()) {
    return false;
  }
  const SelectSource& derived = sources[index];
  const auto& inner = derived.query;
  if (inner == nullptr || derived.alias.empty() || derived.is_lateral ||
      derived.unnest || !derived.using_columns.empty() ||
      derived.from_nested_join || derived.join_type == JoinType::kFull) {
    return false;
  }
  // Grouped constructs route to the grouped bridge / relational finish paths
  // whose scope contracts assume unflattened shapes (in particular, the
  // single-source grouped path cannot resolve qualified references). Window
  // evaluation likewise owns its pre-computed scope. Keep those queries on
  // the existing path in this slice.
  if (!outer->GroupBy().empty() || outer->Having() ||
      relational_detail::HasWindowFunctions(*outer) ||
      std::ranges::any_of(outer->SelectList(), [](const NamedExpression& item) {
        return relational_detail::ContainsAggregate(item.expression);
      })) {
    return false;
  }
  // Stars (including the lone-star value-table wrapper) keep the dedicated
  // expansion paths: expanding them here would diverge from the row-value
  // semantics the visitor marked.
  for (const NamedExpression& item : outer->SelectList()) {
    if (ContainsStarReference(item.expression)) {
      return false;
    }
  }
  // The immediate join decides where the inner WHERE may go: into the outer
  // WHERE everywhere except the null-supplying side of an outer join, where
  // it must ride the ON condition (see the header comment). Chains beyond
  // two relations only qualify when every join is inner.
  const bool two_sources = outer->Sources().size() == 2;
  bool inner_where_to_on = false;
  if (index > 0) {
    if (derived.join_type == JoinType::kLeft) {
      if (!two_sources) {
        return false;
      }
      inner_where_to_on = true;
    } else if (derived.join_type == JoinType::kRight) {
      if (!two_sources) {
        return false;
      }
      inner_where_to_on = false;
    } else if (derived.join_type != JoinType::kCross &&
               derived.join_type != JoinType::kInner) {
      return false;
    }
  } else if (outer->Sources().size() > 1) {
    const JoinType next = outer->Sources()[1].join_type;
    if (next == JoinType::kRight) {
      if (!two_sources) {
        return false;
      }
      inner_where_to_on = true;
    } else if (next != JoinType::kCross && next != JoinType::kInner &&
               next != JoinType::kLeft) {
      return false;
    }
    if (!two_sources &&
        std::ranges::any_of(outer->Sources(), [](const SelectSource& source) {
          return source.join_type == JoinType::kLeft ||
                 source.join_type == JoinType::kRight ||
                 source.join_type == JoinType::kFull;
        })) {
      return false;
    }
  }
  // Inner shape: one plain base table, row-preserving, uncorrelated.
  if (inner->Sources().size() != 1 || !inner->WithQueries().empty() ||
      !inner->GroupBy().empty() || inner->Having() || inner->Distinct() ||
      !inner->DistinctOn().empty() || inner->HasLimit() ||
      inner->Offset() != 0 || inner->Qualify() || !inner->UnionAll().empty() ||
      inner->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*inner) ||
      inner->SelectList().empty()) {
    return false;
  }
  const SelectSource& base = inner->Sources()[0];
  if (derived.sample && base.sample) {
    return false;
  }
  if (base.query != nullptr || base.unnest || base.is_lateral ||
      base.table.empty() || !base.using_columns.empty() ||
      base.from_nested_join || base.join_condition) {
    return false;
  }
  if (outer->IsRecursiveWith(base.table)) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> base_table = ctx.GetTable(base.table);
  if (!base_table.HasValue()) {
    return false;
  }
  const Schema& base_schema = base_table.Value()->GetSchema();
  const std::string inner_alias = base.alias.empty() ? base.table : base.alias;
  const std::string derived_alias = derived.alias;

  // Expand an inner `*` over the base schema; anything else unexpandable
  // keeps the relational path.
  std::vector<NamedExpression> inner_items;
  for (const NamedExpression& item : inner->SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      const ColumnName& star = item.expression->AsColumnValue().GetColumnName();
      if (!star.schema.empty() && !IdentifierEquals(star.schema, inner_alias) &&
          !IdentifierEquals(star.schema, base.table)) {
        return false;
      }
      if (item.star_modifiers) {
        std::unordered_set<std::string> resolved_except;
        std::unordered_set<std::string> resolved_replace;
        ExpandStarWithModifiers(item, inner_alias, base_schema, &inner_items,
                                &resolved_except, &resolved_replace);
        if (resolved_except.size() !=
                item.star_modifiers->except_columns.size() ||
            resolved_replace.size() !=
                item.star_modifiers->replace_items.size()) {
          return false;
        }
        continue;
      }
      for (size_t i = 0; i < base_schema.ColumnCount(); ++i) {
        const std::string& col = base_schema.GetColumn(i).Name().name;
        inner_items.emplace_back(col, ColumnValueExp(ColumnName(col)));
      }
      continue;
    }
    inner_items.push_back(item);
  }
  DerivedScope scope;
  scope.derived_alias = derived_alias;
  scope.inner_alias = inner_alias;
  scope.base_table = base.table;
  scope.base_schema = base_schema;
  for (size_t i = 0; i < base_schema.ColumnCount(); ++i) {
    std::string lowered = base_schema.GetColumn(i).Name().name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    scope.hidden_columns.insert(std::move(lowered));
  }
  for (const NamedExpression& item : inner_items) {
    if (item.name.empty() || !item.expression ||
        relational_detail::ContainsAggregate(item.expression) ||
        ContainsQueryExpression(item.expression) ||
        !DerivedExpressionIsImmutable(item.expression)) {
      return false;
    }
    std::optional<Expression> rebound =
        RebindInnerExpression(item.expression, scope);
    if (!rebound.has_value()) {
      return false;
    }
    std::string lowered = item.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!scope.outputs.try_emplace(lowered, std::move(*rebound)).second) {
      return false;
    }
    scope.hidden_columns.erase(lowered);
  }
  std::optional<Expression> rebound_where =
      RebindInnerExpression(inner->WhereClause(), scope);
  if (!rebound_where.has_value()) {
    return false;
  }
  // Base column names visible in the outer scope (other sources plus the
  // flattened base itself): a bare outer reference colliding with one never
  // substitutes a derived output.
  std::unordered_set<std::string> base_column_names;
  for (size_t i = 0; i < sources.size(); ++i) {
    if (i == index) {
      continue;
    }
    const SelectSource& sibling = sources[i];
    if (sibling.query != nullptr) {
      for (const NamedExpression& item : sibling.query->SelectList()) {
        if (item.name.empty()) {
          continue;
        }
        std::string lowered = item.name;
        for (char& c : lowered) {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        base_column_names.insert(std::move(lowered));
      }
      continue;
    }
    if (sibling.table.empty()) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> sibling_table =
        ctx.GetTable(sibling.table);
    if (!sibling_table.HasValue()) {
      continue;
    }
    for (size_t c = 0; c < sibling_table.Value()->GetSchema().ColumnCount();
         ++c) {
      std::string lowered =
          sibling_table.Value()->GetSchema().GetColumn(c).Name().name;
      for (char& ch : lowered) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
      }
      base_column_names.insert(std::move(lowered));
    }
  }
  // No bare outer reference may name a base column hidden by the derived
  // table: it errored (or bound elsewhere) before flattening and must not
  // silently start resolving to the base table.
  const auto outer_bare_names = [&](const Expression& expression) {
    std::unordered_set<std::string> names;
    if (!expression) {
      return names;
    }
    std::vector<Expression> stack{expression};
    while (!stack.empty()) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      if (current->Type() == TypeTag::kColumnValue) {
        const ColumnName& column = current->AsColumnValue().GetColumnName();
        if (column.schema.empty() && column.name != "*") {
          std::string lowered = column.name;
          for (char& c : lowered) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          names.insert(std::move(lowered));
        }
        continue;
      }
      for (const Expression& child : ExpressionChildren(current)) {
        if (child) {
          stack.push_back(child);
        }
      }
    }
    return names;
  };
  std::vector<Expression> outer_exprs;
  if (outer->WhereClause()) {
    outer_exprs.push_back(outer->WhereClause());
  }
  for (const NamedExpression& item : outer->SelectList()) {
    if (item.expression) {
      outer_exprs.push_back(item.expression);
    }
  }
  for (const auto& term : outer->OrderBy()) {
    if (term.expression) {
      outer_exprs.push_back(term.expression);
    }
  }
  for (const Expression& expression : outer->GroupBy()) {
    outer_exprs.push_back(expression);
  }
  if (outer->Having()) {
    outer_exprs.push_back(outer->Having());
  }
  for (const SelectSource& source : sources) {
    if (source.join_condition) {
      outer_exprs.push_back(source.join_condition);
    }
  }
  for (const Expression& expression : outer_exprs) {
    for (const std::string& name : outer_bare_names(expression)) {
      if (scope.hidden_columns.contains(name)) {
        return false;
      }
    }
    // A derived output used as a qualifier (`pb.field` where `pb` is an
    // output, not a relation) addresses the row value through the derived
    // scope; substituting the alias away would orphan it. The derived alias
    // itself is fine (handled by substitution below).
    std::vector<Expression> stack;
    if (expression) {
      stack.push_back(expression);
    }
    while (!stack.empty()) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      if (current->Type() == TypeTag::kColumnValue) {
        const ColumnName& column = current->AsColumnValue().GetColumnName();
        if (!column.schema.empty() &&
            !IdentifierEquals(column.schema, derived_alias)) {
          std::string lowered = column.schema;
          for (char& c : lowered) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          if (scope.outputs.contains(lowered)) {
            return false;
          }
        }
        continue;
      }
      for (const Expression& child : ExpressionChildren(current)) {
        if (child) {
          stack.push_back(child);
        }
      }
    }
  }
  if (outer->Qualify()) {
    for (const std::string& name : outer_bare_names(outer->Qualify())) {
      if (scope.hidden_columns.contains(name) || scope.outputs.contains(name)) {
        return false;
      }
    }
    for (const ColumnName& column : outer->Qualify()->TouchedColumns()) {
      if (!column.schema.empty() &&
          IdentifierEquals(column.schema, derived_alias)) {
        return false;
      }
    }
  }
  // Substitute derived references across the outer statement. (Stars were
  // rejected above and need no handling here.)
  std::vector<NamedExpression> new_select;
  for (const NamedExpression& item : outer->SelectList()) {
    std::optional<Expression> mapped =
        SubstituteDerivedOutputs(item.expression, scope, base_column_names);
    if (!mapped.has_value()) {
      return false;
    }
    new_select.emplace_back(item.name, std::move(*mapped));
  }
  const auto substitute_field = [&](const Expression& expression) {
    return SubstituteDerivedOutputs(expression, scope, base_column_names);
  };
  std::optional<Expression> new_where = substitute_field(outer->WhereClause());
  std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
  for (auto& term : new_order) {
    std::optional<Expression> mapped = substitute_field(term.expression);
    if (!mapped.has_value()) {
      return false;
    }
    term.expression = std::move(*mapped);
  }
  std::vector<Expression> new_group;
  for (const Expression& expression : outer->GroupBy()) {
    std::optional<Expression> mapped = substitute_field(expression);
    if (!mapped.has_value()) {
      return false;
    }
    new_group.push_back(std::move(*mapped));
  }
  std::optional<Expression> new_having = substitute_field(outer->Having());
  if (!new_where.has_value() || !new_having.has_value()) {
    return false;
  }
  // Merge the inner WHERE into the outer WHERE — or into the ON condition
  // when the flattened source is null-supplying for its immediate outer
  // join (a post-padding residual would drop padded rows outright).
  Expression merged_where = std::move(*new_where);
  // The join carrying the ON lives on the later source of each pair. When
  // the derived table is the right operand (index > 0) that is its own slot;
  // when it is the left operand of `d RIGHT JOIN next`, the ON sits on the
  // next source.  Every source's ON must be rebound through the derived
  // scope first: the derived alias disappears after flattening, so an
  // unrebound `d.col` would silently bind to the base table or fail.
  const size_t on_index =
      index > 0 ? index : (sources.size() > 1 ? size_t{1} : index);
  for (auto& source : sources) {
    if (!source.join_condition) {
      continue;
    }
    std::optional<Expression> mapped = substitute_field(source.join_condition);
    if (!mapped.has_value()) {
      return false;
    }
    source.join_condition = std::move(*mapped);
  }
  if (inner_where_to_on) {
    Expression on = sources[on_index].join_condition;
    if (*rebound_where) {
      on = on ? BinaryExpressionExp(on, BinaryOperation::kAnd, *rebound_where)
              : *rebound_where;
    }
    sources[on_index].join_condition = std::move(on);
  } else if (*rebound_where) {
    merged_where =
        merged_where ? BinaryExpressionExp(merged_where, BinaryOperation::kAnd,
                                           *rebound_where)
                     : *rebound_where;
  }
  // Publish the base table under the derived alias and drop the subquery.
  sources[index].table = base.table;
  sources[index].query = nullptr;
  outer->SetSources(std::move(sources));
  outer->SetWhereClause(std::move(merged_where));
  outer->SetSelectList(std::move(new_select));
  outer->SetOrderBy(std::move(new_order));
  outer->SetGroupBy(std::move(new_group));
  outer->SetHaving(std::move(*new_having));
  return true;
}

// Budget for eager CTE cells (M4): planning-time memory plus memo
// fingerprint cost stay trivial; anything bigger keeps the materialized
// relational path.
constexpr size_t kMaxMaterializedCteRows = 1024;

// CTE predicate pushdown (TODO.md item 2a): before a lifted CTE body is
// executed into a shared cell, narrow it with outer WHERE conjuncts. The
// cell serves every reference site, so an injected predicate must be a
// logical consequence of *each* site's filters (a weakening): per output
// column it keeps the minimum lower bound, the maximum upper bound, and a
// unanimous equality across all live sites. Single-site CTEs fall out as
// the special case (their own bounds). The outer filters are kept
// everywhere, so the injection only ever narrows the prefetched cell; it
// must still be exact (an over-narrowing injection would drop rows a site
// keeps), hence the passthrough-only mapping and the sargable-operator
// gate. A site whose own conjuncts contradict each other needs nothing and
// is excluded from the combination.
// Returns true when at least one conjunct was injected.
}  // namespace

bool TryNarrowCteBody(SelectStatement* body, const SelectStatement& outer,
                      const std::vector<std::string>& site_aliases) {
  if (body == nullptr || site_aliases.empty() || body->Sources().size() != 1) {
    return false;
  }
  const SelectSource& base = body->Sources().front();
  if (base.table.empty() || base.query != nullptr || base.unnest ||
      base.is_lateral || base.from_nested_join || !base.using_columns.empty() ||
      base.join_condition || base.join_type != JoinType::kCross) {
    return false;
  }
  // Row-preserving bodies only (same shape the M5 flattener consumes, plus
  // no post-window/row-count operators whose input the filter would change).
  if (!body->GroupBy().empty() || body->Having() || body->Distinct() ||
      body->HasDistinctOn() || body->HasLimit() || body->Offset() != 0 ||
      !body->UnionAll().empty() || body->GetSetOperationTree() != nullptr ||
      body->Qualify() || body->WithTies() ||
      relational_detail::HasWindowFunctions(*body) ||
      !body->WithQueries().empty()) {
    return false;
  }
  for (const NamedExpression& item : body->SelectList()) {
    if (ContainsStarReference(item.expression)) {
      return false;
    }
  }
  // Lowered output-name -> base column for bare passthrough targets only.
  // Expression outputs have no mapping (conservative: no injection).
  auto lower = [](std::string text) {
    for (char& c : text) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return text;
  };
  std::unordered_map<std::string, ColumnName> output_to_base;
  for (const NamedExpression& item : body->SelectList()) {
    if (item.name.empty() || !item.expression ||
        item.expression->Type() != TypeTag::kColumnValue) {
      continue;
    }
    output_to_base.emplace(lower(item.name),
                           item.expression->AsColumnValue().GetColumnName());
  }
  if (output_to_base.empty()) {
    return false;
  }
  struct SiteBounds {
    bool dead{false};
    struct ColumnBounds {
      bool usable{true};
      bool has_lower{false};
      bool has_upper{false};
      bool has_eq{false};
      Value lower_value;
      Value upper_value;
      Value eq_value;
      bool lower_inclusive{true};
      bool upper_inclusive{true};
    };
    std::unordered_map<std::string, ColumnBounds> columns;
  };
  std::unordered_map<std::string, size_t> site_index;
  for (size_t i = 0; i < site_aliases.size(); ++i) {
    site_index.emplace(lower(site_aliases[i]), i);
  }
  std::vector<SiteBounds> sites(site_aliases.size());
  for (const Expression& conjunct : SplitConjuncts(outer.WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    BinaryOperation normalized_op = binary.Op();
    const Expression* column_side = nullptr;
    const Expression* const_side = nullptr;
    if (binary.Left()->Type() == TypeTag::kColumnValue &&
        binary.Right()->Type() == TypeTag::kConstantValue) {
      column_side = &binary.Left();
      const_side = &binary.Right();
    } else if (binary.Left()->Type() == TypeTag::kConstantValue &&
               binary.Right()->Type() == TypeTag::kColumnValue) {
      column_side = &binary.Right();
      const_side = &binary.Left();
      switch (binary.Op()) {
        case BinaryOperation::kEquals:
          normalized_op = BinaryOperation::kEquals;
          break;
        case BinaryOperation::kLessThan:
          normalized_op = BinaryOperation::kGreaterThan;
          break;
        case BinaryOperation::kLessThanEquals:
          normalized_op = BinaryOperation::kGreaterThanEquals;
          break;
        case BinaryOperation::kGreaterThan:
          normalized_op = BinaryOperation::kLessThan;
          break;
        case BinaryOperation::kGreaterThanEquals:
          normalized_op = BinaryOperation::kLessThanEquals;
          break;
        default:
          continue;
      }
    } else {
      continue;
    }
    if (normalized_op != BinaryOperation::kEquals &&
        normalized_op != BinaryOperation::kLessThan &&
        normalized_op != BinaryOperation::kLessThanEquals &&
        normalized_op != BinaryOperation::kGreaterThan &&
        normalized_op != BinaryOperation::kGreaterThanEquals) {
      continue;
    }
    const Value& constant = (*const_side)->AsConstantValue().GetValue();
    if (constant.IsNull()) {
      continue;
    }
    const ColumnName& outer_col =
        (*column_side)->AsColumnValue().GetColumnName();
    const auto site_it = site_index.find(lower(outer_col.schema));
    if (site_it == site_index.end()) {
      continue;
    }
    SiteBounds& site = sites[site_it->second];
    if (site.dead) {
      continue;
    }
    const std::string key = lower(outer_col.name);
    if (!output_to_base.contains(key)) {
      continue;
    }
    SiteBounds::ColumnBounds& bounds = site.columns[key];
    if (!bounds.usable) {
      continue;
    }
    if (normalized_op == BinaryOperation::kEquals) {
      if (bounds.has_eq) {
        if (bounds.eq_value.type != constant.type) {
          // Mixed-type equalities may still agree under numeric coercion;
          // the column simply becomes unrepresentable for this site (the
          // column is then skipped globally, never mis-narrowed).
          bounds.usable = false;
          bounds.has_lower = bounds.has_upper = bounds.has_eq = false;
          continue;
        }
        if (!(bounds.eq_value == constant)) {
          // Contradictory equalities: this site needs nothing.
          site.dead = true;
          site.columns.clear();
          continue;
        }
      } else {
        bounds.has_eq = true;
        bounds.eq_value = constant;
      }
      continue;
    }
    // Range bound: mixed-type comparisons on one column are skipped.
    bool* has = nullptr;
    Value* stored = nullptr;
    bool* inclusive = nullptr;
    const bool is_lower = normalized_op == BinaryOperation::kGreaterThan ||
                          normalized_op == BinaryOperation::kGreaterThanEquals;
    if (is_lower) {
      has = &bounds.has_lower;
      stored = &bounds.lower_value;
      inclusive = &bounds.lower_inclusive;
    } else {
      has = &bounds.has_upper;
      stored = &bounds.upper_value;
      inclusive = &bounds.upper_inclusive;
    }
    if (*has && stored->type != constant.type) {
      bounds.usable = false;
      bounds.has_lower = bounds.has_upper = bounds.has_eq = false;
      continue;
    }
    const bool edge_inclusive =
        normalized_op == BinaryOperation::kGreaterThanEquals ||
        normalized_op == BinaryOperation::kLessThanEquals;
    if (!*has) {
      *has = true;
      *stored = constant;
      *inclusive = edge_inclusive;
    } else if (is_lower) {
      if (constant.type == stored->type && *stored < constant) {
        *stored = constant;
        *inclusive = edge_inclusive;
      } else if (!(constant.type == stored->type) || constant == *stored) {
        *inclusive = *inclusive && edge_inclusive;
      }
    } else {
      if (constant.type == stored->type && constant < *stored) {
        *stored = constant;
        *inclusive = edge_inclusive;
      } else if (!(constant.type == stored->type) || constant == *stored) {
        *inclusive = *inclusive && edge_inclusive;
      }
    }
  }
  // Combine across live sites: the minimum lower bound, the maximum upper
  // bound, and a unanimous equality. The inclusive edge is always sound for
  // the weakening (a strict site bound implies its inclusive form).
  std::vector<Expression> injected;
  for (const auto& [output, base_column] : output_to_base) {
    bool all_lower = true;
    bool all_upper = true;
    bool all_eq = true;
    bool all_usable = true;
    bool any_live = false;
    Value lower_value;
    Value upper_value;
    Value eq_value;
    bool lower_set = false;
    bool upper_set = false;
    bool eq_set = false;
    for (const SiteBounds& site : sites) {
      if (site.dead) {
        continue;
      }
      any_live = true;
      const auto found = site.columns.find(output);
      if (found == site.columns.end() || !found->second.usable) {
        all_lower = all_upper = all_eq = false;
        if (found != site.columns.end()) {
          all_usable = false;
        }
        continue;
      }
      const SiteBounds::ColumnBounds& bounds = found->second;
      if (bounds.has_lower) {
        if (!lower_set) {
          lower_value = bounds.lower_value;
          lower_set = true;
        } else if (lower_value.type != bounds.lower_value.type) {
          all_lower = false;
        } else if (bounds.lower_value < lower_value) {
          lower_value = bounds.lower_value;
        }
      } else {
        all_lower = false;
      }
      if (bounds.has_upper) {
        if (!upper_set) {
          upper_value = bounds.upper_value;
          upper_set = true;
        } else if (upper_value.type != bounds.upper_value.type) {
          all_upper = false;
        } else if (upper_value < bounds.upper_value) {
          // The weakest (largest) upper bound is the only sound shared
          // bound: each site's row only needs to satisfy its OWN filter,
          // so a row the widest site admits must survive the injected
          // predicate even when a narrower site would reject it.
          upper_value = bounds.upper_value;
        }
      } else {
        all_upper = false;
      }
      if (bounds.has_eq) {
        if (!eq_set) {
          eq_value = bounds.eq_value;
          eq_set = true;
        } else if (eq_value.type != bounds.eq_value.type ||
                   !(eq_value == bounds.eq_value)) {
          all_eq = false;
        }
      } else {
        all_eq = false;
      }
    }
    if (!any_live || !all_usable) {
      continue;
    }
    if (all_eq && eq_set) {
      injected.push_back(BinaryExpressionExp(ColumnValueExp(base_column),
                                             BinaryOperation::kEquals,
                                             ConstantValueExp(eq_value)));
      continue;
    }
    if (all_lower && lower_set) {
      injected.push_back(BinaryExpressionExp(
          ColumnValueExp(base_column), BinaryOperation::kGreaterThanEquals,
          ConstantValueExp(lower_value)));
    }
    if (all_upper && upper_set) {
      injected.push_back(BinaryExpressionExp(ColumnValueExp(base_column),
                                             BinaryOperation::kLessThanEquals,
                                             ConstantValueExp(upper_value)));
    }
  }
  if (injected.empty()) {
    return false;
  }
  std::vector<Expression> body_conjuncts = SplitConjuncts(body->WhereClause());
  body_conjuncts.insert(body_conjuncts.end(), injected.begin(), injected.end());
  body->SetWhereClause(CombineConjuncts(body_conjuncts));
  return true;
}

namespace {

struct GroupingRelation {
  std::string name;
  const Schema* schema{nullptr};
};

bool ResolveGroupingColumn(const std::vector<GroupingRelation>& relations,
                           const ColumnName& column,
                           GroupingRelation* relation) {
  if (column.name.empty() || column.name == "*") {
    return false;
  }
  if (!column.schema.empty()) {
    return std::ranges::any_of(
        relations, [&](const GroupingRelation& candidate) {
          if (!IdentifierEquals(candidate.name, column.schema) ||
              candidate.schema->Offset(ColumnName("", column.name)) < 0) {
            return false;
          }
          if (relation != nullptr) {
            *relation = candidate;
          }
          return true;
        });
  }
  GroupingRelation* found = nullptr;
  for (const GroupingRelation& candidate : relations) {
    if (candidate.schema->Offset(ColumnName("", column.name)) < 0) {
      continue;
    }
    if (found != nullptr) {
      return false;
    }
    found = const_cast<GroupingRelation*>(&candidate);
  }
  if (found == nullptr) {
    return false;
  }
  if (relation != nullptr) {
    *relation = *found;
  }
  return true;
}

bool ColumnIsPrimaryKey(const GroupingRelation& relation,
                        std::string_view name) {
  const int offset = relation.schema->Offset(ColumnName("", name));
  return offset >= 0 && relation.schema->GetColumn(static_cast<size_t>(offset))
                                .GetConstraint()
                                .ctype == Constraint::kPrimaryKey;
}

}  // namespace

bool ReduceGroupByFunctionalDependencies(SelectStatement* select,
                                         TransactionContext& ctx) {
  if (select == nullptr || select->GroupBy().size() < 2 ||
      select->IsGroupAll() || select->HasDistinctOn() || select->WithTies()) {
    return false;
  }
  std::vector<std::shared_ptr<Table>> table_holders;
  std::vector<GroupingRelation> relations;
  table_holders.reserve(select->Sources().size());
  relations.reserve(select->Sources().size());
  for (const SelectSource& source : select->Sources()) {
    if (source.table.empty() || source.query != nullptr || source.unnest ||
        source.is_lateral || source.from_nested_join ||
        !source.using_columns.empty() ||
        (source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kInner)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> table = ctx.GetTable(source.table);
    if (!table.HasValue()) {
      return false;
    }
    table_holders.push_back(table.MoveValue());
    relations.push_back(
        {.name = source.alias.empty() ? source.table : source.alias,
         .schema = &table_holders.back()->GetSchema()});
  }
  if (relations.empty()) {
    return false;
  }

  std::vector<GroupingRelation> key_relations;
  std::vector<const ColumnName*> key_columns;
  key_relations.reserve(select->GroupBy().size());
  key_columns.reserve(select->GroupBy().size());
  for (const Expression& key : select->GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName* column = &key->AsColumnValue().GetColumnName();
    GroupingRelation relation;
    if (!ResolveGroupingColumn(relations, *column, &relation)) {
      return false;
    }
    key_relations.push_back(std::move(relation));
    key_columns.push_back(column);
  }

  std::unordered_set<std::string> certified;
  for (size_t i = 0; i < key_relations.size(); ++i) {
    if (ColumnIsPrimaryKey(key_relations[i], key_columns[i]->name)) {
      certified.insert(key_relations[i].name);
    }
  }
  if (certified.empty()) {
    return false;
  }

  std::vector<Expression> reduced;
  reduced.reserve(select->GroupBy().size());
  std::vector<std::pair<std::string, std::string>> retained_keys;
  retained_keys.reserve(select->GroupBy().size());
  for (size_t i = 0; i < key_relations.size(); ++i) {
    const bool drop =
        certified.contains(key_relations[i].name) &&
        !ColumnIsPrimaryKey(key_relations[i], key_columns[i]->name);
    if (drop) {
      continue;
    }
    retained_keys.emplace_back(key_relations[i].name, key_columns[i]->name);
    reduced.push_back(select->GroupBy()[i]);
  }
  if (reduced.size() == select->GroupBy().size() || reduced.empty()) {
    return false;
  }

  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.name.empty() && item.expression) {
      aliases.emplace(std::string(item.name), item.expression);
    }
  }
  const std::function<Expression(const Expression&)> substitute_aliases =
      [&](const Expression& expression) -> Expression {
    if (!expression) {
      return expression;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (!column.schema.empty()) {
        return expression;
      }
      const auto found = aliases.find(column.name);
      if (found == aliases.end()) {
        return expression;
      }
      return found->second;
    }
    std::vector<Expression> children;
    for (const Expression& child : ExpressionChildren(expression)) {
      children.push_back(substitute_aliases(child));
    }
    return WithExpressionChildren(expression, std::move(children));
  };
  const std::function<bool(const Expression&)> columns_are_functional =
      [&](const Expression& expression) -> bool {
    if (!expression) {
      return true;
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      return true;
    }
    if (expression->Type() == TypeTag::kWindowFunctionExp ||
        expression->Type() == TypeTag::kQueryExp) {
      return false;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      GroupingRelation relation;
      if (!ResolveGroupingColumn(relations, column, &relation)) {
        return false;
      }
      if (certified.contains(relation.name)) {
        return true;
      }
      return std::ranges::any_of(retained_keys, [&](const auto& retained) {
        return retained.first == relation.name &&
               IdentifierEquals(retained.second, column.name);
      });
    }
    return std::ranges::all_of(
        ExpressionChildren(expression),
        [&](const Expression& child) { return columns_are_functional(child); });
  };
  for (const NamedExpression& item : select->SelectList()) {
    if (!columns_are_functional(item.expression)) {
      return false;
    }
  }
  if (!columns_are_functional(substitute_aliases(select->Having()))) {
    return false;
  }
  if (select->Qualify() &&
      !columns_are_functional(substitute_aliases(select->Qualify()))) {
    return false;
  }
  for (const auto& order : select->OrderBy()) {
    if (!columns_are_functional(substitute_aliases(order.expression))) {
      return false;
    }
  }

  select->SetGroupBy(std::move(reduced));
  return true;
}

namespace {}  // namespace

void SqlEngine::MaterializeCtes(SelectStatement* outer, TransactionContext& ctx,
                                LiftedCtes* lifted) {
  if (outer == nullptr || lifted == nullptr || outer->WithQueries().empty() ||
      force_relational_fallback_) {
    return;
  }
  // Grouped outer queries never reach the cell-reading paths (grouping
  // routes relational), so lifting would only waste eager execution.
  if (!outer->GroupBy().empty() || outer->Having()) {
    return;
  }
  bool outer_has_aggregate = false;
  for (const NamedExpression& item : outer->SelectList()) {
    if (relational_detail::ContainsAggregate(item.expression)) {
      outer_has_aggregate = true;
      break;
    }
  }
  if (outer_has_aggregate) {
    return;
  }
  // Relational-only features never compose with cells either (same set as
  // PostRewriteNeedsRelational minus the CTE/derived clauses this lifting
  // resolves; keep in sync). Lifting there would execute eagerly only to
  // discard the rows on the relational fallback.
  for (const SelectSource& source : outer->Sources()) {
    if (source.unnest || source.is_lateral || !source.using_columns.empty() ||
        source.from_nested_join || source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull ||
        NeedsRelationalEvaluation(source.join_condition)) {
      return;
    }
  }
  for (const NamedExpression& item : outer->SelectList()) {
    if (NeedsRelationalEvaluation(item.expression)) {
      return;
    }
    if (item.expression &&
        item.expression->Type() == TypeTag::kFunctionCallExp &&
        (item.expression->AsFunctionCallExpression().FuncName() ==
             "__value_table_value" ||
         item.expression->AsFunctionCallExpression().FuncName() ==
             "__proto_new")) {
      return;
    }
  }
  if (NeedsRelationalEvaluation(outer->WhereClause()) ||
      relational_detail::HasWindowFunctions(*outer) || outer->Qualify() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      outer->HasDistinctOn() || outer->WithTies()) {
    return;
  }
  for (const auto& term : outer->OrderBy()) {
    if (NeedsRelationalEvaluation(term.expression)) {
      return;
    }
  }
  // Deterministic declaration order for reproducible plans.
  std::vector<std::string> names;
  for (const std::string& name : outer->WithQueryOrder()) {
    if (outer->WithQueries().contains(name)) {
      names.push_back(name);
    }
  }
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    if (!outer->IsRecursiveWith(name) &&
        std::ranges::find(names, name) == names.end()) {
      names.push_back(name);
    }
  }
  // Plain (non-recursive) names lift here; recursive ones belong to
  // LiftRecursiveCtes below and stay mapped meanwhile. Reference counting
  // still spans every name so a plain CTE referenced from a recursive body
  // (or anywhere off the top level) keeps the layer mapped.
  std::vector<std::string> plain_names;
  for (const std::string& name : names) {
    if (!outer->IsRecursiveWith(name)) {
      plain_names.push_back(name);
    }
  }
  if (plain_names.empty()) {
    return;
  }
  std::unordered_set<std::string> name_set(names.begin(), names.end());
  std::unordered_map<std::string, size_t> total_refs;
  CountCteReferencesInStatement(*outer, name_set, &total_refs);
  // Every reference must be a top-level FROM source: nested placements
  // (expression subqueries, set-op branches, sibling bodies) keep the
  // map-scoped execution they rely on.
  std::unordered_map<std::string, std::vector<size_t>> top_sites;
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    const std::string& table = outer->Sources()[i].table;
    if (name_set.contains(table)) {
      top_sites[table].push_back(i);
    }
  }
  size_t top_total = 0;
  for (const auto& [name, sites] : top_sites) {
    (void)name;
    top_total += sites.size();
  }
  size_t grand_total = 0;
  for (const auto& [name, count] : total_refs) {
    (void)name;
    grand_total += count;
  }
  if (top_total != grand_total) {
    return;
  }
  // Every mapped CTE participates (no dead entries left behind to block
  // routing, no unmapped stragglers): each needs at least one top-level
  // site, distinct normalized aliases per CTE, and a CTE-free body with
  // unique output names.
  for (const std::string& name : plain_names) {
    const auto sites_it = top_sites.find(name);
    if (sites_it == top_sites.end() || sites_it->second.empty()) {
      return;
    }
    std::unordered_set<std::string> aliases;
    for (size_t index : sites_it->second) {
      const std::string& alias = outer->Sources()[index].alias;
      if (!aliases.insert(alias.empty() ? name : alias).second) {
        return;
      }
    }
    const auto body_it = outer->WithQueries().find(name);
    if (body_it == outer->WithQueries().end() || body_it->second == nullptr) {
      return;
    }
    // Set-operation bodies keep the map-scoped path: a lifted UNION ALL cell
    // would replace the shared scan the map guarantees for multi-site
    // references, and the eager drain below would duplicate each branch's
    // execution per reference site instead of materializing once.
    {
      const SelectStatement& body = *body_it->second;
      if (!body.UnionAll().empty() || body.GetSetOperationTree() != nullptr) {
        return;
      }
    }
    std::unordered_map<std::string, size_t> body_refs;
    CountCteReferencesInStatement(*body_it->second, name_set, &body_refs);
    for (const auto& [ref, count] : body_refs) {
      (void)ref;
      if (count > 0) {
        return;
      }
    }
    std::unordered_set<std::string> output_names;
    for (const NamedExpression& item : body_it->second->SelectList()) {
      if (item.name.empty()) {
        return;
      }
      std::string lowered = item.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      if (!output_names.insert(std::move(lowered)).second) {
        return;
      }
    }
    if (output_names.empty()) {
      return;
    }
  }
  // All gates passed: plan + execute each body through a fresh engine (same
  // transaction snapshot as the outer query; isolated facade state so the
  // outer preparation is undisturbed). Bodies are loaned out of the map and
  // moved straight back afterwards, so the map is complete on every return
  // path and the abort path is identical to never having tried (bodies may
  // come back statement-rewritten by the inner preparation, which preserves
  // their row multisets).
  SqlEngine body_engine(*database_);
  struct LiftedRows {
    std::string name;
    std::vector<std::string> labels;
    std::vector<Row> rows;
  };
  std::vector<LiftedRows> lifted_rows;
  for (const std::string& name : plain_names) {
    const auto body_it = outer->WithQueries().find(name);
    if (body_it == outer->WithQueries().end() || body_it->second == nullptr) {
      return;
    }
    // Shared ownership: the inner preparation may statement-rewrite the
    // (shared) body, which preserves its row multiset by construction; the
    // map entry itself is never disturbed, so every return path below keeps
    // a complete map and the abort path is identical to never having tried.
    // Single-use bodies additionally get outer filters pushed in (TODO.md
    // item 2a): the copy is narrowed, never the mapped body, and the outer
    // filter stays as the residual.
    auto body_copy = std::make_unique<SelectStatement>(*body_it->second);
    {
      // Every top-level reference site shares the cell, so the narrowing
      // must be a weakening over all of them (TODO.md item 2a).
      std::vector<std::string> site_aliases;
      for (const SelectSource& source : outer->Sources()) {
        if (source.table == name) {
          site_aliases.push_back(source.alias.empty() ? name : source.alias);
        }
      }
      TryNarrowCteBody(body_copy.get(), *outer, site_aliases);
    }
    StatusOr<Executor> planned =
        body_engine.PrepareStatement(ctx, std::move(body_copy));
    if (!planned.HasValue()) {
      return;
    }
    // The facade contract serves rows in ResultColumnNames order.
    const std::vector<std::string>& labels = body_engine.ResultColumnNames();
    if (labels.empty()) {
      return;
    }
    LiftedRows lifted_body;
    lifted_body.name = name;
    lifted_body.labels = labels;
    DataChunk batch;
    size_t drained = 0;
    while (planned.Value()->NextBatch(&batch) != 0) {
      for (size_t i = 0; i < batch.Size(); ++i) {
        Row row = batch.RowAt(i);
        if (row.values_.size() != labels.size()) {
          return;
        }
        if (++drained > kMaxMaterializedCteRows) {
          return;
        }
        lifted_body.rows.push_back(std::move(row));
      }
    }
    if (planned.Value()->GetStatus() != Status::kSuccess) {
      return;
    }
    lifted_rows.push_back(std::move(lifted_body));
  }
  // Commit: normalize site aliases (FROM identities for the memo groups) and
  // record one alias-qualified cell per reference site sharing the rows.
  std::unordered_map<std::string, std::shared_ptr<MaterializedCte>> cells;
  std::vector<SelectSource> sources = outer->Sources();
  for (LiftedRows& lifted_body : lifted_rows) {
    // First-non-null type inference per column; all-null stays Null (typed
    // rewrites treat it conservatively, row semantics stay dynamic).
    std::vector<ValueType> column_types(lifted_body.labels.size(),
                                        ValueType::kNull);
    for (const Row& row : lifted_body.rows) {
      for (size_t i = 0; i < row.values_.size() && i < column_types.size();
           ++i) {
        if (!row.values_[i].IsNull() && column_types[i] == ValueType::kNull) {
          column_types[i] = row.values_[i].type;
        }
      }
    }
    for (SelectSource& source : sources) {
      if (source.table != lifted_body.name) {
        continue;
      }
      const std::string alias =
          source.alias.empty() ? lifted_body.name : source.alias;
      source.alias = alias;
      std::vector<Column> columns;
      columns.reserve(lifted_body.labels.size());
      for (size_t c = 0; c < lifted_body.labels.size(); ++c) {
        columns.emplace_back(ColumnName(alias, lifted_body.labels[c]),
                             column_types[c]);
      }
      auto cell = std::make_shared<MaterializedCte>();
      cell->schema = Schema(alias, std::move(columns));
      cell->rows = lifted_body.rows;
      cells.emplace(alias, std::move(cell));
    }
  }
  outer->SetSources(std::move(sources));
  lifted->cells = std::move(cells);
  // Every remaining non-recursive entry lifted (atomic gates above), so all
  // of them are covered from here on.
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    if (!outer->IsRecursiveWith(name)) {
      lifted->covered.insert(name);
    }
  }
  if (!lifted->cells.empty()) {
    plan_cache_fingerprint_.clear();
    plan_cache_parameters_.clear();
  }
}

// A recursive CTE lifts to an opaque memo leaf when exactly one top-level
// FROM site references it and its body references no mapped CTE except
// itself (the worktable). Sibling-scoped references keep the map-scoped
// execution they rely on; multiple sites would re-run the fixpoint per
// site instead of sharing it.
namespace {}  // namespace

bool RecursiveCteLiftable(const SelectStatement& outer,
                          const std::string& name) {
  int top_sites = 0;
  for (const SelectSource& source : outer.Sources()) {
    if (source.table == name) {
      ++top_sites;
    }
  }
  if (top_sites != 1) {
    return false;
  }
  const auto body_it = outer.WithQueries().find(name);
  if (body_it == outer.WithQueries().end() || body_it->second == nullptr) {
    return false;
  }
  const SelectStatement& body = *body_it->second;
  std::unordered_set<std::string> names;
  for (const auto& [entry, entry_body] : outer.WithQueries()) {
    (void)entry_body;
    names.insert(entry);
  }
  std::unordered_map<std::string, size_t> total_refs;
  CountCteReferencesInStatement(outer, names, &total_refs);
  std::unordered_map<std::string, size_t> body_refs;
  CountCteReferencesInStatement(body, names, &body_refs);
  // Exactly one EXTERNAL reference, living at the top level: the body's own
  // worktable self-references do not count (they resolve inside the
  // fixpoint, not through the map).
  size_t self_refs = 0;
  const auto self_it = body_refs.find(name);
  if (self_it != body_refs.end()) {
    self_refs = self_it->second;
  }
  const auto total_it = total_refs.find(name);
  const size_t total = total_it == total_refs.end() ? 0 : total_it->second;
  if (top_sites != 1 || total != 1 + self_refs) {
    return false;
  }
  for (const auto& [ref, count] : body_refs) {
    if (ref != name && count > 0) {
      return false;
    }
  }
  // Unique output names for the leaf schema (mirrors the cell gate: the
  // downstream name maps cannot tell duplicates apart).
  std::unordered_set<std::string> output_names;
  for (const NamedExpression& item : body.SelectList()) {
    if (item.name.empty()) {
      return false;
    }
    std::string lowered = item.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!output_names.insert(std::move(lowered)).second) {
      return false;
    }
  }
  return !output_names.empty();
}

namespace {}  // namespace

void SqlEngine::LiftRecursiveCtes(SelectStatement* outer, LiftedCtes* lifted) {
  auto seal_coverage = [&] {
    if (lifted == nullptr) {
      return;
    }
    lifted->fully_covered = true;
    if (outer != nullptr) {
      for (const auto& [name, body] : outer->WithQueries()) {
        (void)body;
        if (!lifted->covered.contains(name)) {
          lifted->fully_covered = false;
          break;
        }
      }
    }
  };
  if (outer == nullptr || lifted == nullptr || outer->WithQueries().empty() ||
      force_relational_fallback_) {
    seal_coverage();
    return;
  }
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) || body == nullptr) {
      continue;
    }
    if (!RecursiveCteLiftable(*outer, name)) {
      continue;
    }
    // Normalize the single site's alias (FROM identity for the memo group).
    for (SelectSource& source :
         const_cast<std::vector<SelectSource>&>(outer->Sources())) {
      if (source.table == name && source.alias.empty()) {
        source.alias = name;
      }
    }
    std::string alias;
    for (const SelectSource& source : outer->Sources()) {
      if (source.table == name) {
        alias = source.alias.empty() ? name : source.alias;
        break;
      }
    }
    if (alias.empty()) {
      continue;
    }
    // A reference that already renames the CTE keeps its alias, but only
    // when nothing still qualifies the CTE name itself.
    if (alias != name && StatementQualifiesName(*outer, name)) {
      continue;
    }
    std::vector<Column> columns;
    for (const NamedExpression& item : body->SelectList()) {
      columns.emplace_back(ColumnName(alias, item.name), ValueType::kNull);
    }
    RecursiveCteRef ref;
    ref.body = body;
    ref.output_schema = Schema(alias, std::move(columns));
    if (const RecursiveDepthSpec* depth = outer->RecursiveDepthOf(name)) {
      ref.depth_spec = *depth;
    }
    lifted->recursive.emplace(alias, std::move(ref));
    lifted->covered.insert(name);
  }
  seal_coverage();
}

}  // namespace tinylamb
