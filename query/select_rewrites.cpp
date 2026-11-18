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
#include "executor/detail/subquery_runtime.hpp"
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
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
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
          !body_it->second->WithQueries().empty() ||
          name.starts_with("__tpcds_pivcte_")) {
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
      if (site.source == nullptr || site.source->query != nullptr) {
        continue;
      }
      if (site.in_map_body) {
        if (site.source->sample || site.source->is_lateral ||
            (!site.source->alias.empty() && site.source->alias != name &&
             StatementQualifiesName(*outer, name))) {
          continue;
        }
        // A single-use projection/UNION ALL feeding another CTE need not be
        // materialized separately. Clone the consumer before changing its
        // source: parsed trees and WITH bodies can be shared by cached plans.
        const auto plain_projection = [&](const auto& self,
                                          const SelectStatement& part,
                                          size_t depth) -> bool {
          if (depth > 16 || part.Sources().size() != 1 ||
              !part.WithQueries().empty() || !part.GroupBy().empty() ||
              part.IsGroupAll() || part.WhereClause() || part.Having() ||
              part.Qualify() || part.Distinct() || !part.DistinctOn().empty() ||
              part.HasLimit() || part.Offset() != 0 ||
              !part.OrderBy().empty() ||
              (part.GetSetOperationTree() &&
               part.GetSetOperationTree()->grouped)) {
            return false;
          }
          const auto& input = part.Sources().front();
          if (input.unnest || input.sample || input.is_lateral ||
              input.from_nested_join || input.join_condition ||
              !input.using_columns.empty() ||
              (!input.table.empty() &&
               outer->WithQueries().contains(input.table))) {
            return false;
          }
          const std::string owner =
              input.alias.empty() ? input.table : input.alias;
          for (const auto& item : part.SelectList()) {
            if (!item.expression ||
                item.expression->Type() != TypeTag::kColumnValue) {
              return false;
            }
            const auto& column =
                item.expression->AsColumnValue().GetColumnName();
            if (column.name == "*" ||
                (!column.schema.empty() && column.schema != owner)) {
              return false;
            }
          }
          if (input.query && !self(self, *input.query, depth + 1)) {
            return false;
          }
          for (size_t b = 0; b < part.UnionAll().size(); ++b) {
            const auto match = part.MatchFor(b);
            if ((b < part.SetOperationKinds().size() &&
                 part.SetOperationKinds()[b] != SetOperationKind::kUnionAll) ||
                match.by_name || match.corresponding ||
                !match.columns.empty() ||
                !self(self, *part.UnionAll()[b], depth + 1)) {
              return false;
            }
          }
          return true;
        };
        if (!plain_projection(plain_projection, *body_it->second, 0)) {
          continue;
        }
        std::string consumer_name;
        std::shared_ptr<SelectStatement> replacement;
        for (const auto& [entry, consumer] : outer->WithQueries()) {
          if (outer->IsRecursiveWith(entry) ||
              !consumer->WithQueries().empty()) {
            continue;
          }
          for (size_t i = 0; i < consumer->Sources().size(); ++i) {
            if (&consumer->Sources()[i] != site.source) {
              continue;
            }
            replacement = std::make_shared<SelectStatement>(*consumer);
            auto sources = consumer->Sources();
            sources[i].query = body_it->second;
            sources[i].alias =
                sources[i].alias.empty() ? name : sources[i].alias;
            sources[i].table.clear();
            replacement->SetSources(std::move(sources));
            consumer_name = entry;
            break;
          }
        }
        if (!replacement) {
          continue;
        }
        outer->RemoveWithQuery(consumer_name);
        outer->AddWithQuery(consumer_name, std::move(replacement));
        outer->RemoveWithQuery(name);
        changed = true;
        inlined = true;
        break;
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

bool InlineSingleUseGroupedCtes(SelectStatement* outer,
                                TransactionContext& context) {
  if (outer == nullptr || outer->WithQueries().empty()) {
    return false;
  }
  std::unordered_set<std::string> names;
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    names.insert(name);
  }
  std::unordered_map<std::string, size_t> counts;
  CountCteReferencesInStatement(*outer, names, &counts);
  std::vector<std::string> inlined;
  const auto rewrite_sources = [&](const auto& self, SelectStatement& consumer,
                                   size_t depth) -> bool {
    if (depth > 16) {
      return false;
    }
    auto sources = consumer.Sources();
    bool changed = false;
    for (auto& source : sources) {
      if (source.query && source.query->WithQueries().empty() &&
          !source.is_lateral && !source.sample && !source.from_nested_join) {
        auto nested = std::make_shared<SelectStatement>(*source.query);
        if (self(self, *nested, depth + 1)) {
          source.query = std::move(nested);
          changed = true;
        }
        continue;
      }
      const auto found = outer->WithQueries().find(source.table);
      if (found == outer->WithQueries().end() || counts[source.table] != 1 ||
          outer->IsRecursiveWith(source.table) || source.query ||
          source.unnest || source.sample || source.is_lateral ||
          source.from_nested_join ||
          source.table.starts_with("__tpcds_pivcte_")) {
        continue;
      }
      const auto& body = *found->second;
      if (body.GroupBy().empty() || body.IsGroupAll() || body.Having() ||
          body.HasLimit() || body.Offset() != 0 || body.Distinct() ||
          body.HasDistinctOn() || body.Qualify() ||
          !body.WithQueries().empty() || !body.UnionAll().empty() ||
          body.GetSetOperationTree() ||
          relational_detail::StatementContainsVolatileFunction(body) ||
          ContainsQueryExpression(body.WhereClause()) ||
          std::ranges::any_of(body.GroupBy(), ContainsQueryExpression) ||
          std::ranges::any_of(body.OrderBy(),
                              [](const auto& term) {
                                return ContainsQueryExpression(term.expression);
                              }) ||
          std::ranges::any_of(body.SelectList(),
                              [](const auto& item) {
                                return ContainsQueryExpression(item.expression);
                              }) ||
          std::ranges::any_of(body.Sources(), [&](const auto& inner) {
            return inner.query || inner.unnest || inner.sample ||
                   inner.is_lateral || inner.from_nested_join ||
                   names.contains(inner.table) ||
                   ContainsQueryExpression(inner.join_condition);
          })) {
        continue;
      }
      const std::string name = source.table;
      if (!source.alias.empty() && source.alias != name &&
          StatementQualifiesName(*outer, name)) {
        continue;
      }
      // Compute all restrictions against the complete WITH scope. In
      // particular, LEFT JOIN key constants flow toward the nullable input,
      // never backwards. A window must see the complete partition. Nested
      // consumers also keep their predicates outside this boundary; only the
      // existing top-level grouped-key proof is allowed to restrict the input
      // here.
      auto restricted =
          depth == 0 && !relational_detail::HasWindowFunctions(body)
              ? relational_detail::PushSingleUseCtePredicates(context, *outer,
                                                              name)
              : nullptr;
      source.query = restricted ? std::move(restricted)
                                : std::make_shared<SelectStatement>(body);
      source.alias = source.alias.empty() ? name : source.alias;
      source.table.clear();
      inlined.push_back(name);
      changed = true;
    }
    if (changed) {
      consumer.SetSources(std::move(sources));
    }
    return changed;
  };
  if (!rewrite_sources(rewrite_sources, *outer, 0)) {
    return false;
  }
  for (const auto& name : inlined) {
    outer->RemoveWithQuery(name);
  }
  return true;
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
// tables, subqueries, set-operation branches, nested WITH bodies) but not in
// the top-level statement's own FROM list.
size_t CountNestedCteReferences(const SelectStatement& statement,
                                const std::string& name,
                                bool count_own_sources = false,
                                bool walk_with_queries = true);

void CountNestedCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::string& name, size_t* count) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    if (query.Query() != nullptr) {
      *count += CountNestedCteReferences(*query.Query(), name, true, true);
    }
    CountNestedCteReferencesInExpression(query.Test(), name, count);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountNestedCteReferencesInExpression(child, name, count);
  }
}

size_t CountNestedCteReferences(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement, const std::string& name,
    bool count_own_sources, bool walk_with_queries) {
  size_t count = 0;
  for (const SelectSource& source : statement.Sources()) {
    if (count_own_sources && source.query == nullptr && !source.unnest &&
        source.table == name) {
      ++count;
    }
    if (source.query != nullptr) {
      count += CountNestedCteReferences(*source.query, name, true, true);
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
      count += CountNestedCteReferences(*branch, name, true, true);
    }
  }
  if (const auto& tree = statement.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      count += CountNestedCteReferences(*tree->first, name, true, true);
    }
    for (const auto& branch : tree->branches) {
      if (branch != nullptr) {
        count += CountNestedCteReferences(*branch, name, true, true);
      }
    }
  }
  if (walk_with_queries) {
    for (const auto& [cte_name, cte_body] : statement.WithQueries()) {
      if (cte_body != nullptr &&
          !(cte_name == name && statement.IsRecursiveWith(cte_name))) {
        count += CountNestedCteReferences(*cte_body, name, true, true);
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
    if (CountNestedCteReferences(*consumer, name, /*count_own_sources=*/false,
                                 /*walk_with_queries=*/consumer != outer) !=
        0) {
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
        // TouchedColumns / StripCteQualifier cannot see inside a subquery,
        // so a correlated reference to another FROM source would be
        // re-scoped into the CTE body. Leave such conjuncts at the consumer.
        if (ContainsQueryExpression(conjunct)) {
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
//   calls do NOT ride along: window-bearing branches stay on the set-op
//   path because merging would recompute frames over all branches
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

// Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
//   (merge_compatible_union_branches_sound)
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
// Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
//   (lateral_decorrelate_inner_and_left_sound,
//    cpp_lateral_decorrelate_star_phantom_col_bug)
bool DecorrelateOneLateralSource(SelectStatement* outer, size_t index,
                                 TransactionContext& ctx) {
  std::vector<SelectSource> sources = outer->Sources();
  if (index >= sources.size()) {
    return false;
  }
  for (const NamedExpression& item : outer->SelectList()) {
    if (ContainsStarReference(item.expression)) {
      return false;
    }
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
      base.from_nested_join || base.join_condition || base.sample ||
      site.sample) {
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
  // A qualifier that names both the LATERAL body's own source and a
  // preceding outer source (FROM t1, LATERAL (... FROM t1 WHERE t1.a =
  // t1.b)) cannot be classified syntactically here; bail out instead of
  // guessing and moving a local predicate into the correlated ON clause.
  for (const Expression& conjunct : SplitConjuncts(inner->WhereClause())) {
    if (conjunct && std::ranges::any_of(conjunct->TouchedColumns(),
                                        [&](const ColumnName& column) {
                                          return is_inner(column) &&
                                                 is_outer(column);
                                        })) {
      return false;
    }
  }
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
  bool appended_hidden_column = false;
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
      appended_hidden_column = true;
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
  const SelectStatement saved_outer = *outer;
  site.query = std::move(new_inner);
  site.is_lateral = false;
  if (site.join_type == JoinType::kCross) {
    site.join_type = JoinType::kInner;
  }
  site.join_condition = std::move(on);
  sources[index] = std::move(site);
  outer->SetSources(std::move(sources));
  // The proven M5 flattener consumes the uncorrelated site when its gates
  // pass; if hidden correlation columns had to be appended and the flattener
  // declines (e.g. outer GROUP BY / WINDOW), roll back so the synthetic column
  // never introduces unqualified name ambiguity in the outer scope.
  if (!FlattenOneDerivedSource(outer, index, ctx) && appended_hidden_column) {
    *outer = saved_outer;
    return false;
  }
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
      !query.OrderBy().empty() ||
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
    if (lift_bare_names && !column.schema.empty()) {
      // A qualified column that is not the member alias belongs to the
      // member's base scope (e.g. `events.g` in `HAVING events.g > 1`).
      // A lifted HAVING moves to the outer query, where the member's base
      // scope no longer exists, so leaving it untouched would produce an
      // unresolved (or wrongly bound) reference. Refuse the fusion instead.
      return std::nullopt;
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
  // Fusion collapses all members into one row per GROUP BY key. That is only
  // equivalent to the original cross/inner join when every member is tied to
  // the representative by an equality on its GROUP BY keys (either in an ON
  // clause or in the outer WHERE). Without that proof an unconditional CROSS
  // JOIN (kCross passes the shape gate above) would lose the cross-product
  // multiplicity, and an equality on non-key columns (`ON a.n = b.total`)
  // would compare different rows after rebinding.
  auto key_column_name = [](const Expression& key) -> std::string {
    if (key && key->Type() == TypeTag::kColumnValue) {
      return key->AsColumnValue().GetColumnName().name;
    }
    return {};
  };
  auto member_projects_key_as = [](const FuseMember& mem,
                                   std::string_view out_name,
                                   std::string_view key_col) -> bool {
    for (const NamedExpression& item : mem.body->SelectList()) {
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
      const std::string_view published = item.name.empty()
                                             ? std::string_view(ic.name)
                                             : std::string_view(item.name);
      if (IdentifierEquals(published, out_name) &&
          IdentifierEquals(ic.name, key_col)) {
        return true;
      }
    }
    return false;
  };
  const FuseMember& representative = members.front();
  auto binds_keys = [&](const FuseMember& member) -> bool {
    const Expression& condition = outer->Sources()[member.index].join_condition;
    const Expression& where = outer->WhereClause();
    for (const Expression& key : member.keys) {
      const std::string kn = key_column_name(key);
      if (kn.empty()) {
        return false;  // computed key: cannot prove a column equality
      }
      const auto binds_one = [&](const Expression& expr) -> bool {
        if (!expr || expr->Type() != TypeTag::kBinaryExp ||
            expr->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
          return false;
        }
        const auto& eq = expr->AsBinaryExpression();
        if (eq.Left()->Type() != TypeTag::kColumnValue ||
            eq.Right()->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& lhs = eq.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rhs = eq.Right()->AsColumnValue().GetColumnName();
        const bool rep_lhs = IdentifierEquals(lhs.schema, representative.alias);
        const bool rep_rhs = IdentifierEquals(rhs.schema, representative.alias);
        const bool mem_lhs = IdentifierEquals(lhs.schema, member.alias);
        const bool mem_rhs = IdentifierEquals(rhs.schema, member.alias);
        return (rep_lhs && mem_rhs &&
                member_projects_key_as(representative, lhs.name, kn) &&
                member_projects_key_as(member, rhs.name, kn)) ||
               (mem_lhs && rep_rhs &&
                member_projects_key_as(member, lhs.name, kn) &&
                member_projects_key_as(representative, rhs.name, kn));
      };
      const auto scan_and = [&](const auto& self,
                                const Expression& expr) -> bool {
        if (!expr) {
          return false;
        }
        if (binds_one(expr)) {
          return true;
        }
        if (expr->Type() == TypeTag::kBinaryExp &&
            expr->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
          return self(self, expr->AsBinaryExpression().Left()) ||
                 self(self, expr->AsBinaryExpression().Right());
        }
        return false;
      };
      if (!scan_and(scan_and, condition) && !scan_and(scan_and, where)) {
        return false;
      }
    }
    return true;
  };
  for (const FuseMember& member : members) {
    if (member.index == representative.index) {
      continue;
    }
    if (!binds_keys(member)) {
      return 0;  // no key-equality proof: keep the original join semantics
    }
  }
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
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto& agg = expression->AsAggregateExpression();
      if (agg.GetType() == AggregationType::kCount && !agg.Distinct() &&
          !agg.NeedsGroupContext() && !agg.HasWhereFilter() &&
          agg.InnerGroupBy().empty() && agg.InnerOrderBy().empty() &&
          agg.Child() && agg.Child()->Type() == TypeTag::kColumnValue &&
          agg.Child()->AsColumnValue().GetColumnName().schema.empty() &&
          agg.Child()->AsColumnValue().GetColumnName().name == "*") {
        return expression;
      }
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
  if (!two_sources &&
      std::ranges::any_of(outer->Sources(), [](const SelectSource& source) {
        return source.join_type == JoinType::kLeft ||
               source.join_type == JoinType::kRight ||
               source.join_type == JoinType::kFull;
      })) {
    return false;
  }
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
  // The flattened source keeps neither sampling clause in the right place:
  // an inner TABLESAMPLE would be dropped outright, and an outer one would
  // move below the inner WHERE (RESERVOIR n would cap the wrong rows).
  if (derived.sample || base.sample) {
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

// CTE predicate pushdown (the weakening contract): before a lifted CTE body is
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

namespace {

constexpr bool IsIdempotentDistinctAggType(AggregationType type) noexcept {
  switch (type) {
    case AggregationType::kMin:
    case AggregationType::kMax:
    case AggregationType::kLogicalAnd:
    case AggregationType::kLogicalOr:
    case AggregationType::kBitAnd:
    case AggregationType::kBitOr:
      return true;
    default:
      return false;
  }
}

std::shared_ptr<AggregateExpression> CloneAggregateWithDistinct(
    const AggregateExpression& source, bool distinct) {
  auto copy = std::make_shared<AggregateExpression>(source.GetType(),
                                                    source.Child(), distinct);
  copy->SetIgnoreNulls(source.IgnoreNulls());
  if (source.Having() != AggregateHavingModifier::kNone) {
    copy->SetHaving(source.Having(), source.HavingCondition());
  }
  copy->SetInnerOrderBy(source.InnerOrderBy());
  copy->SetInnerLimit(source.InnerLimit());
  copy->SetSecondaryArg(source.SecondaryArg());
  copy->SetTrailingArgs(source.TrailingArgs());
  if (source.HasWhereFilter()) {
    copy->SetWhereFilter(source.WhereFilter());
  }
  copy->SetArrayElementSqlType(source.ArrayElementSqlType());
  copy->SetInnerGroupBy(source.InnerGroupBy());
  return copy;
}

Expression StripIdempotentDistinctExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, bool* changed) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    const auto& agg = expr->AsAggregateExpression();
    if (agg.Distinct() && !agg.NeedsGroupContext() &&
        IsIdempotentDistinctAggType(agg.GetType())) {
      *changed = true;
      return CloneAggregateWithDistinct(agg, /*distinct=*/false);
    }
    return expr;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp ||
      expr->Type() == TypeTag::kQueryExp) {
    return expr;
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  bool child_changed = false;
  std::vector<Expression> updated;
  updated.reserve(children.size());
  for (const Expression& child : children) {
    updated.push_back(StripIdempotentDistinctExpr(child, &child_changed));
  }
  if (!child_changed) {
    return expr;
  }
  *changed = true;
  return WithExpressionChildren(expr, std::move(updated));
}

}  // namespace

// Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
//   (min_max_distinct_elim_sound)
bool StripIdempotentDistinctAggregates(SelectStatement* select) {
  if (select == nullptr) {
    return false;
  }
  bool any_changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    (void)name;
    if (body != nullptr && StripIdempotentDistinctAggregates(body.get())) {
      any_changed = true;
    }
  }
  for (SelectSource& src :
       const_cast<std::vector<SelectSource>&>(select->Sources())) {
    if (src.query != nullptr &&
        StripIdempotentDistinctAggregates(src.query.get())) {
      any_changed = true;
    }
  }
  for (const auto& branch : select->UnionAll()) {
    if (branch != nullptr && StripIdempotentDistinctAggregates(branch.get())) {
      any_changed = true;
    }
  }
  if (const auto& tree = select->GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr &&
        StripIdempotentDistinctAggregates(tree->first.get())) {
      any_changed = true;
    }
    for (const auto& branch : tree->branches) {
      if (branch != nullptr &&
          StripIdempotentDistinctAggregates(branch.get())) {
        any_changed = true;
      }
    }
  }

  bool select_changed = false;
  std::vector<NamedExpression> items = select->SelectList();
  for (NamedExpression& item : items) {
    item.expression =
        StripIdempotentDistinctExpr(item.expression, &select_changed);
  }
  if (select_changed) {
    select->SetSelectList(std::move(items));
    any_changed = true;
  }
  if (select->Having()) {
    bool having_changed = false;
    Expression h =
        StripIdempotentDistinctExpr(select->Having(), &having_changed);
    if (having_changed) {
      select->SetHaving(std::move(h));
      any_changed = true;
    }
  }
  if (!select->OrderBy().empty()) {
    bool order_changed = false;
    std::vector<SelectStatement::OrderByTerm> terms = select->OrderBy();
    for (auto& term : terms) {
      term.expression =
          StripIdempotentDistinctExpr(term.expression, &order_changed);
    }
    if (order_changed) {
      select->SetOrderBy(std::move(terms));
      any_changed = true;
    }
  }
  return any_changed;
}

// Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
//   (having_max_gt_pushdown_to_where_sound,
//    having_min_lt_pushdown_to_where_sound,
//    having_min_gt_pushed_to_where_bug, having_max_gt_breaks_sibling_avg_bug)
bool PushHavingExtremumBounds(SelectStatement* select,
                              TransactionContext& ctx) {
  if (select == nullptr || !select->Having() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->Qualify() ||
      select->HasDistinctOn() || ContainsQueryExpression(select->Having())) {
    return false;
  }
  std::vector<std::shared_ptr<Table>> table_holders;
  std::vector<GroupingRelation> relations;
  table_holders.reserve(select->Sources().size());
  relations.reserve(select->Sources().size());
  std::unordered_set<std::string> seen_rel_names;
  for (const SelectSource& source : select->Sources()) {
    if (source.table.empty() || source.query != nullptr || source.unnest ||
        source.is_lateral || source.from_nested_join ||
        !source.using_columns.empty() || source.sample.has_value()) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> table = ctx.GetTable(source.table);
    if (!table.HasValue()) {
      return false;
    }
    std::string rel_name = source.alias.empty() ? source.table : source.alias;
    std::string lowered = rel_name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!seen_rel_names.insert(std::move(lowered)).second) {
      return false;
    }
    table_holders.push_back(table.MoveValue());
    relations.push_back({.name = std::move(rel_name),
                         .schema = &table_holders.back()->GetSchema()});
  }
  if (relations.empty()) {
    return false;
  }

  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.name.empty() && item.expression) {
      aliases.emplace(item.name, item.expression);
    }
  }
  const std::function<Expression(const Expression&)> substitute_aliases =
      [&](const Expression& expr) -> Expression {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.schema.empty()) {
        const auto found = aliases.find(col.name);
        if (found != aliases.end()) {
          return found->second;
        }
      }
      return expr;
    }
    if (expr->Type() == TypeTag::kAggregateExp ||
        expr->Type() == TypeTag::kWindowFunctionExp ||
        expr->Type() == TypeTag::kQueryExp) {
      return expr;
    }
    const std::vector<Expression> children = ExpressionChildren(expr);
    if (children.empty()) {
      return expr;
    }
    std::vector<Expression> mapped;
    mapped.reserve(children.size());
    for (const Expression& child : children) {
      mapped.push_back(substitute_aliases(child));
    }
    return WithExpressionChildren(expr, std::move(mapped));
  };

  bool ok = true;
  std::vector<const AggregateExpression*> aggs;
  const std::function<void(const Expression&)> collect_aggs =
      [&](const Expression& expr) {
        if (!expr || !ok) {
          return;
        }
        if (expr->Type() == TypeTag::kWindowFunctionExp ||
            expr->Type() == TypeTag::kQueryExp) {
          ok = false;
          return;
        }
        if (expr->Type() == TypeTag::kAggregateExp) {
          aggs.push_back(&expr->AsAggregateExpression());
          return;
        }
        for (const Expression& child : ExpressionChildren(expr)) {
          collect_aggs(child);
        }
      };

  for (const NamedExpression& item : select->SelectList()) {
    collect_aggs(item.expression);
  }
  const Expression resolved_having = substitute_aliases(select->Having());
  collect_aggs(resolved_having);
  for (const auto& term : select->OrderBy()) {
    collect_aggs(substitute_aliases(term.expression));
  }
  if (!ok || aggs.empty()) {
    return false;
  }

  // Reject queries that read ungrouped representative-row columns outside
  // aggregates, since dropping non-extremum rows could alter which arbitrary
  // row represents the group.
  std::unordered_set<ColumnName> grouped_cols;
  if (!select->IsGroupAll()) {
    for (const Expression& gk : select->GroupBy()) {
      const Expression resolved_gk = substitute_aliases(gk);
      if (!resolved_gk || resolved_gk->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& raw = resolved_gk->AsColumnValue().GetColumnName();
      GroupingRelation rel;
      if (!ResolveGroupingColumn(relations, raw, &rel) ||
          rel.schema == nullptr) {
        return false;
      }
      const int off = rel.schema->Offset(ColumnName("", raw.name));
      if (off < 0) {
        return false;
      }
      grouped_cols.emplace(
          rel.name,
          rel.schema->GetColumn(static_cast<size_t>(off)).Name().name);
    }
  }
  const std::function<bool(const Expression&)> bare_cols_grouped =
      [&](const Expression& expr) -> bool {
    if (!expr || expr->Type() == TypeTag::kAggregateExp) {
      return true;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& raw = expr->AsColumnValue().GetColumnName();
      GroupingRelation rel;
      if (!ResolveGroupingColumn(relations, raw, &rel) ||
          rel.schema == nullptr) {
        return false;
      }
      const int off = rel.schema->Offset(ColumnName("", raw.name));
      if (off < 0) {
        return false;
      }
      return grouped_cols.contains(ColumnName(
          rel.name,
          rel.schema->GetColumn(static_cast<size_t>(off)).Name().name));
    }
    return std::ranges::all_of(ExpressionChildren(expr), bare_cols_grouped);
  };
  for (const NamedExpression& item : select->SelectList()) {
    if (!bare_cols_grouped(item.expression)) {
      return false;
    }
  }
  if (!bare_cols_grouped(resolved_having)) {
    return false;
  }
  for (const auto& term : select->OrderBy()) {
    if (!bare_cols_grouped(substitute_aliases(term.expression))) {
      return false;
    }
  }

  const AggregationType ext_type = aggs.front()->GetType();
  if (ext_type != AggregationType::kMax && ext_type != AggregationType::kMin) {
    return false;
  }
  auto resolve_agg_col = [&](const AggregateExpression& agg,
                             ColumnName* canonical_out,
                             ValueType* type_out) -> bool {
    if (agg.GetType() != ext_type || agg.NeedsGroupContext() ||
        agg.HasWhereFilter() || !agg.InnerGroupBy().empty() || !agg.Child() ||
        agg.Child()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& raw_col = agg.Child()->AsColumnValue().GetColumnName();
    GroupingRelation rel;
    if (!ResolveGroupingColumn(relations, raw_col, &rel) ||
        rel.schema == nullptr) {
      return false;
    }
    const int off = rel.schema->Offset(ColumnName("", raw_col.name));
    if (off < 0) {
      return false;
    }
    const Column& col_def = rel.schema->GetColumn(static_cast<size_t>(off));
    if (canonical_out != nullptr) {
      *canonical_out = ColumnName(rel.name, col_def.Name().name);
    }
    if (type_out != nullptr) {
      *type_out = col_def.Type();
    }
    return true;
  };

  ColumnName target_col;
  ValueType target_domain = ValueType::kNull;
  if (!resolve_agg_col(*aggs.front(), &target_col, &target_domain)) {
    return false;
  }
  // Strictly non-DOUBLE reflexive domain: GoogleSQL DOUBLE MIN/MAX propagates
  // NaN across the entire group, making WHERE pre-filtering unsound on DOUBLE.
  if (target_domain != ValueType::kInt64 && target_domain != ValueType::kDate &&
      target_domain != ValueType::kVarChar) {
    return false;
  }
  for (size_t i = 1; i < aggs.size(); ++i) {
    ColumnName cand_col;
    if (!resolve_agg_col(*aggs[i], &cand_col, nullptr) ||
        cand_col != target_col) {
      return false;
    }
  }

  // If target_col belongs to the preserved side of an outer join (e.g. `l` in
  // `l LEFT JOIN r` or `r` in `l RIGHT JOIN r`), pushing into WHERE is still
  // kRight/kFull joins are refused outright: a padded-side target column can
  // change the group's extremum when unmatched rows are filtered, so the
  // rewrite is not applied to those join types.
  for (size_t i = 1; i < select->Sources().size(); ++i) {
    const JoinType jt = select->Sources()[i].join_type;
    if (jt == JoinType::kRight || jt == JoinType::kFull) {
      return false;
    }
  }

  std::unordered_set<std::string> existing_where_strs;
  for (const Expression& w : SplitConjuncts(select->WhereClause())) {
    if (w) {
      existing_where_strs.insert(w->ToString());
    }
  }

  std::vector<Expression> new_bounds;
  for (const Expression& conj : SplitConjuncts(resolved_having)) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    BinaryOperation op = bin.Op();
    Expression lhs = bin.Left();
    Expression rhs = bin.Right();
    if (lhs && rhs && lhs->Type() == TypeTag::kConstantValue &&
        rhs->Type() == TypeTag::kAggregateExp) {
      switch (op) {
        case BinaryOperation::kLessThan:
          op = BinaryOperation::kGreaterThan;
          std::swap(lhs, rhs);
          break;
        case BinaryOperation::kLessThanEquals:
          op = BinaryOperation::kGreaterThanEquals;
          std::swap(lhs, rhs);
          break;
        case BinaryOperation::kGreaterThan:
          op = BinaryOperation::kLessThan;
          std::swap(lhs, rhs);
          break;
        case BinaryOperation::kGreaterThanEquals:
          op = BinaryOperation::kLessThanEquals;
          std::swap(lhs, rhs);
          break;
        default:
          break;
      }
    }
    if (!lhs || !rhs || lhs->Type() != TypeTag::kAggregateExp ||
        rhs->Type() != TypeTag::kConstantValue) {
      continue;
    }
    const Value& cval = rhs->AsConstantValue().GetValue();
    if (cval.IsNull() || cval.type != target_domain) {
      continue;
    }
    Expression bound;
    if (ext_type == AggregationType::kMax &&
        (op == BinaryOperation::kGreaterThan ||
         op == BinaryOperation::kGreaterThanEquals)) {
      bound = BinaryExpressionExp(ColumnValueExp(target_col), op, rhs);
    } else if (ext_type == AggregationType::kMin &&
               (op == BinaryOperation::kLessThan ||
                op == BinaryOperation::kLessThanEquals)) {
      bound = BinaryExpressionExp(ColumnValueExp(target_col), op, rhs);
    }
    if (bound && existing_where_strs.insert(bound->ToString()).second) {
      new_bounds.push_back(std::move(bound));
    }
  }
  if (new_bounds.empty()) {
    return false;
  }

  Expression updated_where = select->WhereClause();
  for (Expression& bound : new_bounds) {
    updated_where = updated_where ? BinaryExpressionExp(
                                        std::move(updated_where),
                                        BinaryOperation::kAnd, std::move(bound))
                                  : std::move(bound);
  }
  select->SetWhereClause(std::move(updated_where));
  return true;
}

// Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
//   (unused_left_join_elim_idempotent_aggs_sound)
bool EliminateUnusedOuterJoinsForIdempotentResult(SelectStatement* select,
                                                  TransactionContext& ctx) {
  if (select == nullptr || select->Sources().size() < 2 ||
      !select->UnionAll().empty() || select->GetSetOperationTree() != nullptr ||
      select->Qualify() || select->ForUpdate() || select->HasDistinctOn()) {
    return false;
  }
  std::vector<SelectSource> sources = select->Sources();
  std::vector<std::shared_ptr<Table>> table_holders;
  std::vector<GroupingRelation> relations;
  table_holders.reserve(sources.size());
  relations.reserve(sources.size());
  std::unordered_set<std::string> seen_names;
  for (size_t i = 0; i < sources.size(); ++i) {
    const SelectSource& src = sources[i];
    if (src.table.empty() || src.query != nullptr || src.unnest ||
        src.is_lateral || src.from_nested_join || !src.using_columns.empty() ||
        src.sample.has_value()) {
      return false;
    }
    if (i > 0 && (src.join_type == JoinType::kRight ||
                  src.join_type == JoinType::kFull)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
    if (!tbl.HasValue()) {
      return false;
    }
    std::string rel_name = src.alias.empty() ? src.table : src.alias;
    std::string lowered = rel_name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!seen_names.insert(std::move(lowered)).second) {
      return false;
    }
    table_holders.push_back(tbl.MoveValue());
    relations.push_back({.name = std::move(rel_name),
                         .schema = &table_holders.back()->GetSchema()});
  }

  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.name.empty() && item.expression) {
      aliases.emplace(item.name, item.expression);
    }
  }
  const std::function<Expression(const Expression&)> substitute_aliases =
      [&](const Expression& expr) -> Expression {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.schema.empty()) {
        const auto found = aliases.find(col.name);
        if (found != aliases.end()) {
          return found->second;
        }
      }
      return expr;
    }
    if (expr->Type() == TypeTag::kAggregateExp ||
        expr->Type() == TypeTag::kWindowFunctionExp ||
        expr->Type() == TypeTag::kQueryExp) {
      return expr;
    }
    const std::vector<Expression> children = ExpressionChildren(expr);
    if (children.empty()) {
      return expr;
    }
    std::vector<Expression> mapped;
    mapped.reserve(children.size());
    for (const Expression& child : children) {
      mapped.push_back(substitute_aliases(child));
    }
    return WithExpressionChildren(expr, std::move(mapped));
  };

  bool ok = true;
  std::vector<const AggregateExpression*> aggs;
  std::unordered_set<std::string> used_relations;
  const std::function<void(const Expression&)> collect_usage =
      [&](const Expression& expr) {
        if (!expr || !ok) {
          return;
        }
        if (expr->Type() == TypeTag::kWindowFunctionExp ||
            expr->Type() == TypeTag::kQueryExp) {
          ok = false;
          return;
        }
        if (expr->Type() == TypeTag::kAggregateExp) {
          const auto& agg = expr->AsAggregateExpression();
          aggs.push_back(&agg);
          if (agg.Child()) {
            collect_usage(agg.Child());
          }
          if (agg.HasWhereFilter()) {
            collect_usage(agg.WhereFilter());
          }
          return;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& col = expr->AsColumnValue().GetColumnName();
          if (col.name == "*" || col.name.empty()) {
            ok = false;
            return;
          }
          GroupingRelation resolved;
          if (!ResolveGroupingColumn(relations, col, &resolved)) {
            ok = false;
            return;
          }
          used_relations.insert(resolved.name);
          return;
        }
        for (const Expression& child : ExpressionChildren(expr)) {
          collect_usage(child);
        }
      };

  for (const NamedExpression& item : select->SelectList()) {
    collect_usage(item.expression);
  }
  if (!select->IsGroupAll()) {
    for (const Expression& gk : select->GroupBy()) {
      collect_usage(substitute_aliases(gk));
    }
  }
  collect_usage(substitute_aliases(select->Having()));
  for (const auto& term : select->OrderBy()) {
    collect_usage(substitute_aliases(term.expression));
  }
  collect_usage(select->WhereClause());
  if (!ok) {
    return false;
  }

  const bool multiplicity_insensitive = [&]() {
    if (aggs.empty()) {
      return select->Distinct() && select->GroupBy().empty() &&
             !select->Having();
    }
    return std::ranges::all_of(aggs, [](const AggregateExpression* agg) {
      if (agg->NeedsGroupContext() || !agg->InnerGroupBy().empty()) {
        return false;
      }
      if (IsIdempotentDistinctAggType(agg->GetType())) {
        return true;
      }
      return agg->Distinct() && agg->Child() != nullptr &&
             (agg->GetType() == AggregationType::kCount ||
              agg->GetType() == AggregationType::kSum ||
              agg->GetType() == AggregationType::kAvg);
    });
  }();
  if (!multiplicity_insensitive) {
    return false;
  }

  // Peel unused kLeft sources from right to left so chained unused LEFT JOINs
  // collapse in one pass.
  const auto resolve_scalar_type =
      [&](const Expression& expr, const std::vector<GroupingRelation>& scope)
      -> std::optional<ValueType> {
    if (!expr) {
      return std::nullopt;
    }
    if (expr->Type() == TypeTag::kConstantValue) {
      return expr->AsConstantValue().GetValue().type;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      GroupingRelation rel;
      if (!ResolveGroupingColumn(scope, col, &rel) || rel.schema == nullptr) {
        return std::nullopt;
      }
      const int off = rel.schema->Offset(ColumnName("", col.name));
      if (off < 0) {
        return std::nullopt;
      }
      return rel.schema->GetColumn(static_cast<size_t>(off)).Type();
    }
    return std::nullopt;
  };
  const std::function<bool(const Expression&,
                           const std::vector<GroupingRelation>&)>
      on_cannot_throw =
          [&](const Expression& expr,
              const std::vector<GroupingRelation>& scope) -> bool {
    if (!expr) {
      return false;
    }
    if (ExpressionCannotThrowForPlanner(expr)) {
      return true;
    }
    switch (expr->Type()) {
      case TypeTag::kBinaryExp: {
        const auto& bin = expr->AsBinaryExpression();
        if (!on_cannot_throw(bin.Left(), scope) ||
            !on_cannot_throw(bin.Right(), scope)) {
          return false;
        }
        switch (bin.Op()) {
          case BinaryOperation::kAnd:
          case BinaryOperation::kOr:
          case BinaryOperation::kXor:
            return true;
          case BinaryOperation::kEquals:
          case BinaryOperation::kNotEquals:
          case BinaryOperation::kLessThan:
          case BinaryOperation::kLessThanEquals:
          case BinaryOperation::kGreaterThan:
          case BinaryOperation::kGreaterThanEquals:
          case BinaryOperation::kIsDistinctFrom:
          case BinaryOperation::kIsNotDistinctFrom: {
            const auto lt = resolve_scalar_type(bin.Left(), scope);
            const auto rt = resolve_scalar_type(bin.Right(), scope);
            if (!lt || !rt || *lt == ValueType::kArray ||
                *rt == ValueType::kArray) {
              return false;
            }
            if (*lt == ValueType::kNull || *rt == ValueType::kNull ||
                *lt == *rt) {
              return true;
            }
            return (*lt == ValueType::kInt64 || *lt == ValueType::kDouble) &&
                   (*rt == ValueType::kInt64 || *rt == ValueType::kDouble);
          }
          default:
            return false;
        }
      }
      case TypeTag::kUnaryExp: {
        const auto& u = expr->AsUnaryExpression();
        switch (u.Op()) {
          case UnaryOperation::kNot:
          case UnaryOperation::kIsNull:
          case UnaryOperation::kIsNotNull:
          case UnaryOperation::kIsTrue:
          case UnaryOperation::kIsFalse:
          case UnaryOperation::kIsNotTrue:
          case UnaryOperation::kIsNotFalse:
            return on_cannot_throw(u.Child(), scope);
          default:
            return false;
        }
      }
      default:
        return false;
    }
  };

  std::vector<bool> keep(sources.size(), true);
  bool dropped_any = false;
  for (size_t idx = sources.size(); idx-- > 1;) {
    const SelectSource& cand = sources[idx];
    if (cand.join_type != JoinType::kLeft || !cand.join_condition ||
        ContainsQueryExpression(cand.join_condition)) {
      continue;
    }
    const std::string& cand_name = relations[idx].name;
    if (used_relations.contains(cand_name)) {
      continue;
    }
    // Check no surviving later source's ON condition references cand_name.
    bool referenced_by_later_on = false;
    for (size_t j = idx + 1; j < sources.size(); ++j) {
      if (!keep[j] || !sources[j].join_condition) {
        continue;
      }
      for (const ColumnName& col :
           sources[j].join_condition->TouchedColumns()) {
        GroupingRelation res;
        if (!ResolveGroupingColumn(relations, col, &res) ||
            res.name == cand_name) {
          referenced_by_later_on = true;
          break;
        }
      }
      if (referenced_by_later_on) {
        break;
      }
    }
    if (referenced_by_later_on) {
      continue;
    }
    // Validate cand.join_condition columns resolve within prefix [0..idx] and
    // cannot throw at runtime.
    std::vector<GroupingRelation> prefix_rels;
    for (size_t p = 0; p <= idx; ++p) {
      if (keep[p]) {
        prefix_rels.push_back(relations[p]);
      }
    }
    bool on_valid = on_cannot_throw(cand.join_condition, prefix_rels);
    if (on_valid) {
      for (const ColumnName& col : cand.join_condition->TouchedColumns()) {
        if (!ResolveGroupingColumn(prefix_rels, col, nullptr)) {
          on_valid = false;
          break;
        }
      }
    }
    if (!on_valid) {
      continue;
    }
    keep[idx] = false;
    dropped_any = true;
  }
  if (!dropped_any) {
    return false;
  }

  std::vector<SelectSource> remaining;
  remaining.reserve(sources.size());
  for (size_t i = 0; i < sources.size(); ++i) {
    if (keep[i]) {
      remaining.push_back(std::move(sources[i]));
    }
  }
  select->SetSources(std::move(remaining));
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
  // This is a speculative small-cell optimization. A blocking body may
  // compute its entire input before the first batch crosses the row cap;
  // abandoning it then executes that same work again on the fallback path.
  // Cost large, unbounded bodies before starting any of them. Estimates only
  // select a strategy: ordinary map-scoped CTE execution remains authoritative.
  static const bool cost_guard_disabled =
      std::getenv("TINYLAMB_DISABLE_EAGER_CTE_COST_GUARD") != nullptr;
  if (!cost_guard_disabled) {
    for (const auto& name : plain_names) {
      const auto& body = *outer->WithQueries().at(name);
      const bool bounded_limit = body.HasLimit() && !body.WithTies() &&
                                 body.Limit() <= kMaxMaterializedCteRows;
      const bool scalar =
          body.GroupBy().empty() &&
          std::ranges::any_of(
              body.SelectList(), [](const NamedExpression& item) {
                return relational_detail::ContainsAggregate(item.expression);
              });
      if (bounded_limit || scalar) {
        continue;
      }
      std::unordered_map<std::string, size_t> tables;
      relational_detail::CountStatementTables(body, &tables);
      size_t estimated_rows = 0;
      for (const auto& [table, references] : tables) {
        const auto stats = ctx.GetStats(table);
        if (!stats.HasValue()) {
          continue;
        }
        const size_t rows = stats.Value()->Rows();
        if (references != 0 &&
            rows > (kMaxMaterializedCteRows - estimated_rows) / references) {
          return;
        }
        estimated_rows += rows * references;
      }
    }
  }
  // All gates passed: plan + execute each body through a fresh engine (same
  // transaction snapshot as the outer query; isolated facade state so the
  // outer preparation is undisturbed). Bodies are loaned out of the map and
  // (the map entry is never disturbed). A deep copy is prepared instead, so
    // the map is complete on every return
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
    auto restricted =
        relational_detail::PushSingleUseCtePredicates(ctx, *outer, name);
    if (!restricted) {
      restricted =
          relational_detail::PushSharedGroupedCtePredicates(ctx, *outer, name);
    }
    if (!restricted) {
      restricted =
          relational_detail::PushGroupedCteJoinDomains(ctx, *outer, name, {});
    }
    if (!restricted) {
      restricted = relational_detail::GroupPresenceOnlyCte(*outer, name, &ctx);
    }
    auto body_copy = std::make_unique<SelectStatement>(
        restricted ? *restricted : *body_it->second);
    {
      // Every top-level reference site shares the cell, so the narrowing
      // must be a weakening over all of them (the weakening contract).
      std::vector<std::string> site_aliases;
      for (const SelectSource& source : outer->Sources()) {
        if (source.table == name) {
          site_aliases.push_back(source.alias.empty() ? name : source.alias);
        }
      }
      TryNarrowCteBody(body_copy.get(), *outer, site_aliases);
    }
    ++cte_materialization_attempts_;
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

namespace {

bool TpcdsExprHasParam(const Expression& expr) {  // NOLINT(misc-no-recursion)
  if (!expr) {
    return false;
  }
  if (dynamic_cast<const ParameterSlot*>(expr.get()) != nullptr) {
    return true;
  }
  if (expr->Type() == TypeTag::kQueryExp) {
    const auto& q = expr->AsQueryExpression();
    if (q.Test() && TpcdsExprHasParam(q.Test())) {
      return true;
    }
    if (q.Query() != nullptr) {
      const SelectStatement& s = *q.Query();
      if (TpcdsExprHasParam(s.WhereClause()) || TpcdsExprHasParam(s.Having()) ||
          TpcdsExprHasParam(s.Qualify())) {
        return true;
      }
      for (const NamedExpression& item : s.SelectList()) {
        if (TpcdsExprHasParam(item.expression)) {
          return true;
        }
      }
    }
    return false;
  }
  return std::ranges::any_of(ExpressionChildren(expr), TpcdsExprHasParam);
}

bool TpcdsStmtHasParam(  // NOLINT(misc-no-recursion)
    const SelectStatement& stmt) {
  if (TpcdsExprHasParam(stmt.WhereClause()) ||
      TpcdsExprHasParam(stmt.Having()) || TpcdsExprHasParam(stmt.Qualify())) {
    return true;
  }
  for (const NamedExpression& item : stmt.SelectList()) {
    if (TpcdsExprHasParam(item.expression)) {
      return true;
    }
  }
  for (const Expression& g : stmt.GroupBy()) {
    if (TpcdsExprHasParam(g)) {
      return true;
    }
  }
  for (const auto& ord : stmt.OrderBy()) {
    if (TpcdsExprHasParam(ord.expression)) {
      return true;
    }
  }
  for (const SelectSource& src : stmt.Sources()) {
    if (TpcdsExprHasParam(src.join_condition) ||
        TpcdsExprHasParam(src.unnest) ||
        (src.query != nullptr && TpcdsStmtHasParam(*src.query))) {
      return true;
    }
  }
  for (const auto& br : stmt.UnionAll()) {
    if (br != nullptr && TpcdsStmtHasParam(*br)) {
      return true;
    }
  }
  if (const auto& tree = stmt.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr && TpcdsStmtHasParam(*tree->first)) {
      return true;
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr && TpcdsStmtHasParam(*br)) {
        return true;
      }
    }
  }
  for (const auto& [name, body] : stmt.WithQueries()) {
    (void)name;
    if (body != nullptr && TpcdsStmtHasParam(*body)) {
      return true;
    }
  }
  return false;
}

bool IsSafeDisjointTagValue(const Value& val) noexcept {
  if (val.IsNull() || val.Collation() != 0) {
    return false;
  }
  return val.type == ValueType::kVarChar || val.type == ValueType::kInt64 ||
         val.type == ValueType::kDate;
}

std::optional<Value> TpcdsEvalClosedConstant(const Expression& expr) {
  if (!expr || TpcdsExprHasParam(expr)) {
    return std::nullopt;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    return expr->AsConstantValue().GetValue();
  }
  if (TpcdsExprHasParam(expr) || ContainsQueryExpression(expr) ||
      relational_detail::ContainsAggregate(expr) ||
      !DerivedExpressionIsImmutable(expr) || !expr->TouchedColumns().empty()) {
    return std::nullopt;
  }
  StatusOr<Value> evaluated = expr->TryEvaluate(Row{}, Schema{});
  if (!evaluated.HasValue()) {
    return std::nullopt;
  }
  return evaluated.MoveValue();
}

bool SelectListHasStarOrModifiers(const SelectStatement& stmt) {
  for (const NamedExpression& item : stmt.SelectList()) {
    if (item.star_modifiers != nullptr || !item.expression) {
      return true;
    }
    if (item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      return true;
    }
  }
  return false;
}

bool ExtractPureUnionAllBranches(
    const SelectStatement& union_stmt,
    std::vector<std::shared_ptr<SelectStatement>>* out_branches) {
  if (union_stmt.UnionDistinct() || union_stmt.UnionByName() ||
      union_stmt.HasLimit() || union_stmt.Offset() != 0 ||
      !union_stmt.OrderBy().empty() || !union_stmt.WithQueries().empty()) {
    return false;
  }
  for (const SetOperationMatch& m : union_stmt.Matches()) {
    if (m.by_name || m.corresponding || !m.columns.empty()) {
      return false;
    }
  }
  if (const auto& tree = union_stmt.GetSetOperationTree(); tree != nullptr) {
    if (tree->first == nullptr || tree->branches.empty() ||
        tree->kinds.size() != tree->branches.size()) {
      return false;
    }
    for (SetOperationKind k : tree->kinds) {
      if (k != SetOperationKind::kUnionAll) {
        return false;
      }
    }
    out_branches->push_back(std::make_shared<SelectStatement>(*tree->first));
    for (const auto& br : tree->branches) {
      if (br == nullptr) {
        return false;
      }
      out_branches->push_back(std::make_shared<SelectStatement>(*br));
    }
    return out_branches->size() >= 2;
  }
  if (union_stmt.UnionAll().empty() ||
      union_stmt.SetOperationKinds().size() != union_stmt.UnionAll().size()) {
    return false;
  }
  for (SetOperationKind k : union_stmt.SetOperationKinds()) {
    if (k != SetOperationKind::kUnionAll) {
      return false;
    }
  }
  auto head = std::make_shared<SelectStatement>(union_stmt);
  head->ClearUnionAll();
  out_branches->push_back(std::move(head));
  for (const auto& br : union_stmt.UnionAll()) {
    if (br == nullptr) {
      return false;
    }
    out_branches->push_back(std::make_shared<SelectStatement>(*br));
  }
  return out_branches->size() >= 2;
}

std::string ExplicitOrColumnOutputName(const NamedExpression& item) {
  if (!item.name.empty()) {
    return item.name;
  }
  if (item.expression && item.expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& col = item.expression->AsColumnValue().GetColumnName();
    if (!col.name.empty() && col.name != "*") {
      return col.name;
    }
  }
  return {};
}

std::string InferSelectItemOutputName(const NamedExpression& item,
                                      size_t index) {
  std::string explicit_or_col = ExplicitOrColumnOutputName(item);
  if (!explicit_or_col.empty()) {
    return explicit_or_col;
  }
  return "$col_" + std::to_string(index);
}

bool PullInnerJoinsBeforeLeftJoinsOne(
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        ctes) {
  if (stmt == nullptr || stmt->Sources().size() < 3 ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*stmt)) {
    return false;
  }
  struct SourceInfo {
    std::string alias;
    std::string table;
    std::unordered_set<std::string> lowered_cols;
  };
  auto resolve_source_info =
      [&](const SelectSource& src) -> std::optional<SourceInfo> {
    if (src.is_lateral || src.unnest || src.sample || src.from_nested_join ||
        !src.using_columns.empty() ||
        (src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner &&
         src.join_type != JoinType::kLeft)) {
      return std::nullopt;
    }
    SourceInfo info;
    info.table = src.table;
    info.alias = src.alias.empty() ? src.table : src.alias;
    if (info.alias.empty()) {
      return std::nullopt;
    }
    if (src.query != nullptr) {
      if (SelectListHasStarOrModifiers(*src.query)) {
        return std::nullopt;
      }
      for (const NamedExpression& item : src.query->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return std::nullopt;
        }
        info.lowered_cols.insert(LoweredStarName(out_nm));
      }
      return info;
    }
    if (const auto cte_it = ctes.find(src.table); cte_it != ctes.end()) {
      if (cte_it->second == nullptr ||
          SelectListHasStarOrModifiers(*cte_it->second)) {
        return std::nullopt;
      }
      for (const NamedExpression& item : cte_it->second->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return std::nullopt;
        }
        info.lowered_cols.insert(LoweredStarName(out_nm));
      }
      return info;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
    if (!tbl.HasValue()) {
      return std::nullopt;
    }
    const Schema& schema = tbl.Value()->GetSchema();
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      info.lowered_cols.insert(
          LoweredStarName(schema.GetColumn(i).Name().name));
    }
    return info;
  };

  std::vector<SelectSource> sources = stmt->Sources();
  std::vector<SourceInfo> infos;
  infos.reserve(sources.size());
  for (const SelectSource& s : sources) {
    std::optional<SourceInfo> inf = resolve_source_info(s);
    if (!inf.has_value()) {
      return false;
    }
    infos.push_back(std::move(*inf));
  }

  bool any_swapped = false;
  bool progress = true;
  while (progress) {
    progress = false;
    for (size_t i = 1; i + 1 < sources.size(); ++i) {
      if (sources[i].join_type != JoinType::kLeft ||
          (sources[i + 1].join_type != JoinType::kInner &&
           sources[i + 1].join_type != JoinType::kCross)) {
        continue;
      }
      const SelectSource& c_src = sources[i + 1];
      const SourceInfo& b_info = infos[i];
      bool ok = true;
      if (c_src.join_condition) {
        if (ContainsQueryExpression(c_src.join_condition) ||
            relational_detail::ContainsAggregate(c_src.join_condition) ||
            !DerivedExpressionIsImmutable(c_src.join_condition)) {
          ok = false;
        } else {
          for (const ColumnName& col : c_src.join_condition->TouchedColumns()) {
            if (col.name == "*") {
              ok = false;
              break;
            }
            if (!col.schema.empty()) {
              if (IdentifierEquals(col.schema, b_info.alias) ||
                  (!b_info.table.empty() &&
                   IdentifierEquals(col.schema, b_info.table))) {
                ok = false;
                break;
              }
            } else if (b_info.lowered_cols.contains(
                           LoweredStarName(col.name))) {
              ok = false;
              break;
            }
          }
        }
      }
      if (!ok) {
        continue;
      }
      std::swap(sources[i], sources[i + 1]);
      std::swap(infos[i], infos[i + 1]);
      any_swapped = true;
      progress = true;
    }
  }
  if (any_swapped) {
    stmt->SetSources(std::move(sources));
  }
  return any_swapped;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (outer_join_intervening_inner_to_antijoin_sound,
//    inner_prefix_reorder_and_filter_under_left_joins_sound)
bool PullInnerJoinsBeforeLeftJoins(SelectStatement* select,
                                   TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    if (select->IsRecursiveWith(name) || body == nullptr) {
      continue;
    }
    changed |= PullInnerJoinsBeforeLeftJoinsOne(body.get(), ctx,
                                                select->WithQueries());
  }
  for (SelectSource& src :
       const_cast<std::vector<SelectSource>&>(select->Sources())) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= PullInnerJoinsBeforeLeftJoinsOne(src.query.get(), ctx,
                                                  select->WithQueries());
    }
  }
  changed |=
      PullInnerJoinsBeforeLeftJoinsOne(select, ctx, select->WithQueries());
  return changed;
}

namespace {

bool DistributeOneUnionGroupBy(SelectStatement* stmt, TransactionContext& ctx) {
  if (stmt == nullptr || stmt->GroupBy().empty() || stmt->IsGroupAll() ||
      stmt->Having() || stmt->Qualify() || stmt->Distinct() ||
      stmt->HasDistinctOn() || stmt->ForUpdate() || stmt->AsStruct() ||
      stmt->WithTies() || !stmt->WithQueries().empty() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt) ||
      stmt->Sources().size() < 2) {
    return false;
  }
  std::optional<size_t> u_idx;
  for (size_t i = 0; i < stmt->Sources().size(); ++i) {
    const SelectSource& s = stmt->Sources()[i];
    if (s.is_lateral || s.unnest || s.sample || s.from_nested_join ||
        !s.using_columns.empty() ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    if (s.query != nullptr) {
      if (u_idx.has_value() || s.alias.empty()) {
        return false;
      }
      u_idx = i;
    } else if (s.table.empty()) {
      return false;
    }
  }
  if (!u_idx.has_value()) {
    return false;
  }
  const SelectSource& u_src = stmt->Sources()[*u_idx];
  std::vector<std::shared_ptr<SelectStatement>> branches;
  if (!ExtractPureUnionAllBranches(*u_src.query, &branches)) {
    return false;
  }

  std::vector<std::shared_ptr<Table>> dim_holders;
  std::vector<GroupingRelation> dim_rels;
  std::unordered_set<std::string> dim_names_lower;
  for (size_t i = 0; i < stmt->Sources().size(); ++i) {
    if (i == *u_idx) {
      continue;
    }
    const SelectSource& ds = stmt->Sources()[i];
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(ds.table);
    if (!tbl.HasValue()) {
      return false;
    }
    dim_holders.push_back(tbl.MoveValue());
    const std::string alias = ds.alias.empty() ? ds.table : ds.alias;
    if (!dim_names_lower.insert(LoweredStarName(alias)).second) {
      return false;
    }
    dim_rels.push_back(
        {.name = alias, .schema = &dim_holders.back()->GetSchema()});
  }

  const size_t u_arity = branches[0]->SelectList().size();
  if (u_arity == 0) {
    return false;
  }
  std::vector<std::string> u_col_names;
  std::unordered_map<std::string, size_t> u_col_index;
  for (size_t c = 0; c < u_arity; ++c) {
    const std::string out_nm =
        ExplicitOrColumnOutputName(branches[0]->SelectList()[c]);
    if (out_nm.empty()) {
      return false;
    }
    std::string low = LoweredStarName(out_nm);
    if (!u_col_index.emplace(low, c).second) {
      return false;
    }
    u_col_names.push_back(out_nm);
  }

  auto qualify_against_rels =
      [&](auto& self, const Expression& expr,
          const std::vector<GroupingRelation>& rels,
          const std::unordered_map<std::string, size_t>* allow_u,
          const std::string& u_alias) -> std::optional<Expression> {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kQueryExp ||
        expr->Type() == TypeTag::kWindowFunctionExp) {
      return std::nullopt;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.name == "*") {
        return expr;
      }
      if (!col.schema.empty()) {
        if (allow_u != nullptr && IdentifierEquals(col.schema, u_alias)) {
          if (!allow_u->contains(LoweredStarName(col.name))) {
            return std::nullopt;
          }
          return ColumnValueExp(ColumnName(u_alias, col.name));
        }
        GroupingRelation resolved;
        if (!ResolveGroupingColumn(rels, col, &resolved)) {
          return std::nullopt;
        }
        return ColumnValueExp(ColumnName(resolved.name, col.name));
      }
      const std::string low = LoweredStarName(col.name);
      const bool in_u = allow_u != nullptr && allow_u->contains(low);
      GroupingRelation resolved_dim;
      const bool in_dim = ResolveGroupingColumn(rels, col, &resolved_dim);
      if (in_u && in_dim) {
        return std::nullopt;
      }
      if (in_u) {
        return ColumnValueExp(ColumnName(u_alias, col.name));
      }
      if (in_dim) {
        return ColumnValueExp(ColumnName(resolved_dim.name, col.name));
      }
      return std::nullopt;
    }
    std::vector<Expression> kids = ExpressionChildren(expr);
    for (Expression& k : kids) {
      std::optional<Expression> qk = self(self, k, rels, allow_u, u_alias);
      if (!qk.has_value()) {
        return std::nullopt;
      }
      k = std::move(*qk);
    }
    return WithExpressionChildren(expr, std::move(kids));
  };

  // Validate and qualify outer GroupBy, WhereClause, D join conditions, and
  // SelectList aggregates.
  std::vector<Expression> qual_group_keys;
  for (const Expression& gk : stmt->GroupBy()) {
    if (!gk || gk->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::optional<Expression> qgk = qualify_against_rels(
        qualify_against_rels, gk, dim_rels, &u_col_index, u_src.alias);
    if (!qgk.has_value()) {
      return false;
    }
    qual_group_keys.push_back(std::move(*qgk));
  }

  std::vector<Expression> outer_where_parts;
  if (stmt->WhereClause()) {
    if (!DerivedExpressionIsImmutable(stmt->WhereClause()) ||
        relational_detail::ContainsAggregate(stmt->WhereClause())) {
      return false;
    }
    std::optional<Expression> qw =
        qualify_against_rels(qualify_against_rels, stmt->WhereClause(),
                             dim_rels, &u_col_index, u_src.alias);
    if (!qw.has_value()) {
      return false;
    }
    outer_where_parts.push_back(std::move(*qw));
  }
  for (const auto & i : stmt->Sources()) {
    if (i.join_condition) {
      if (!DerivedExpressionIsImmutable(i.join_condition) ||
          relational_detail::ContainsAggregate(
              i.join_condition)) {
        return false;
      }
      std::optional<Expression> qjc = qualify_against_rels(
          qualify_against_rels, i.join_condition, dim_rels,
          &u_col_index, u_src.alias);
      if (!qjc.has_value()) {
        return false;
      }
      outer_where_parts.push_back(std::move(*qjc));
    }
  }

  struct OuterSelectSlot {
    std::string out_name;
    bool is_agg{false};
    size_t key_idx{0};
    AggregationType agg_type{AggregationType::kSum};
    Expression qual_agg_arg;
  };
  std::vector<OuterSelectSlot> select_slots;
  for (size_t idx = 0; idx < stmt->SelectList().size(); ++idx) {
    const NamedExpression& item = stmt->SelectList()[idx];
    OuterSelectSlot slot;
    slot.out_name = InferSelectItemOutputName(item, idx);
    if (item.expression->Type() == TypeTag::kAggregateExp) {
      const auto& agg = item.expression->AsAggregateExpression();
      if (agg.Distinct() || agg.NeedsGroupContext() || agg.HasWhereFilter() ||
          !agg.InnerGroupBy().empty() || !agg.InnerOrderBy().empty() ||
          !agg.Child() || !DerivedExpressionIsImmutable(agg.Child())) {
        return false;
      }
      if (agg.GetType() != AggregationType::kSum &&
          agg.GetType() != AggregationType::kCount &&
          agg.GetType() != AggregationType::kMin &&
          agg.GetType() != AggregationType::kMax) {
        return false;
      }
      std::optional<Expression> qarg =
          qualify_against_rels(qualify_against_rels, agg.Child(), dim_rels,
                               &u_col_index, u_src.alias);
      if (!qarg.has_value()) {
        return false;
      }
      slot.is_agg = true;
      slot.agg_type = agg.GetType();
      slot.qual_agg_arg = std::move(*qarg);
      select_slots.push_back(std::move(slot));
      continue;
    }
    std::optional<Expression> qitem =
        qualify_against_rels(qualify_against_rels, item.expression, dim_rels,
                             &u_col_index, u_src.alias);
    if (!qitem.has_value() || (*qitem)->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& item_col = (*qitem)->AsColumnValue().GetColumnName();
    std::optional<size_t> matched_gk;
    for (size_t g = 0; g < qual_group_keys.size(); ++g) {
      const ColumnName& gcol =
          qual_group_keys[g]->AsColumnValue().GetColumnName();
      if (IdentifierEquals(gcol.schema, item_col.schema) &&
          IdentifierEquals(gcol.name, item_col.name)) {
        matched_gk = g;
        break;
      }
    }
    if (!matched_gk.has_value()) {
      return false;
    }
    slot.is_agg = false;
    slot.key_idx = *matched_gk;
    select_slots.push_back(std::move(slot));
  }

  // Rebind outer OrderBy terms against outer SelectList output names or group
  // keys.
  std::vector<SelectStatement::OrderByTerm> new_outer_order;
  for (const auto& term : stmt->OrderBy()) {
    SelectStatement::OrderByTerm nt = term;
    if (term.expression && term.expression->Type() == TypeTag::kConstantValue) {
      new_outer_order.push_back(std::move(nt));
      continue;
    }
    if (term.expression && term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& oc = term.expression->AsColumnValue().GetColumnName();
      if (oc.schema.empty()) {
        bool matched_alias = false;
        for (auto & select_slot : select_slots) {
          if (IdentifierEquals(select_slot.out_name, oc.name)) {
            nt.expression =
                ColumnValueExp(ColumnName("", select_slot.out_name));
            matched_alias = true;
            break;
          }
        }
        if (matched_alias) {
          new_outer_order.push_back(std::move(nt));
          continue;
        }
      }
      std::optional<Expression> qord =
          qualify_against_rels(qualify_against_rels, term.expression, dim_rels,
                               &u_col_index, u_src.alias);
      if (qord.has_value() && (*qord)->Type() == TypeTag::kColumnValue) {
        const ColumnName& qc = (*qord)->AsColumnValue().GetColumnName();
        bool matched_gk = false;
        for (size_t g = 0; g < qual_group_keys.size(); ++g) {
          const ColumnName& gcol =
              qual_group_keys[g]->AsColumnValue().GetColumnName();
          if (IdentifierEquals(gcol.schema, qc.schema) &&
              IdentifierEquals(gcol.name, qc.name)) {
            nt.expression = ColumnValueExp(
                ColumnName("__tpcds_upre", "__gk_" + std::to_string(g)));
            matched_gk = true;
            break;
          }
        }
        if (matched_gk) {
          new_outer_order.push_back(std::move(nt));
          continue;
        }
      }
    }
    return false;
  }

  // Build each pre-aggregated branch.
  std::vector<std::shared_ptr<SelectStatement>> pre_branches;
  pre_branches.reserve(branches.size());
  for (const auto& br : branches) {
    if (!br->GroupBy().empty() || br->Having() || br->Distinct() ||
        br->HasDistinctOn() || br->HasLimit() || br->Offset() != 0 ||
        br->Qualify() || !br->WithQueries().empty() ||
        !br->UnionAll().empty() || br->GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(*br) ||
        br->SelectList().size() != u_arity ||
        SelectListHasStarOrModifiers(*br) || br->Sources().empty()) {
      return false;
    }
    std::vector<std::shared_ptr<Table>> b_holders;
    std::vector<GroupingRelation> b_rels;
    for (const SelectSource& bs : br->Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.is_lateral ||
          bs.unnest || bs.sample || bs.from_nested_join ||
          !bs.using_columns.empty() ||
          (bs.join_type != JoinType::kCross &&
           bs.join_type != JoinType::kInner &&
           bs.join_type != JoinType::kLeft)) {
        return false;
      }
      const std::string balias = bs.alias.empty() ? bs.table : bs.alias;
      if (dim_names_lower.contains(LoweredStarName(balias)) ||
          dim_names_lower.contains(LoweredStarName(bs.table))) {
        return false;
      }
      StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
      if (!btbl.HasValue()) {
        return false;
      }
      b_holders.push_back(btbl.MoveValue());
      b_rels.push_back(
          {.name = balias, .schema = &b_holders.back()->GetSchema()});
    }

    std::vector<Expression> b_outputs(u_arity);
    for (size_t c = 0; c < u_arity; ++c) {
      const Expression& bexpr = br->SelectList()[c].expression;
      if (!bexpr || ContainsQueryExpression(bexpr) ||
          relational_detail::ContainsAggregate(bexpr) ||
          !DerivedExpressionIsImmutable(bexpr)) {
        return false;
      }
      std::optional<Expression> qb = qualify_against_rels(
          qualify_against_rels, bexpr, b_rels, nullptr, "");
      if (!qb.has_value()) {
        return false;
      }
      b_outputs[c] = std::move(*qb);
    }

    auto subst_u = [&](auto& self, const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (IdentifierEquals(col.schema, u_src.alias)) {
          return b_outputs.at(u_col_index.at(LoweredStarName(col.name)));
        }
        return expr;
      }
      std::vector<Expression> kids = ExpressionChildren(expr);
      for (Expression& k : kids) {
        k = self(self, k);
      }
      return WithExpressionChildren(expr, std::move(kids));
    };

    std::vector<SelectSource> merged_sources;
    for (SelectSource bs : br->Sources()) {
      if (bs.alias.empty()) {
        bs.alias = bs.table;
      }
      if (bs.join_condition) {
        if (!DerivedExpressionIsImmutable(bs.join_condition) ||
            ContainsQueryExpression(bs.join_condition) ||
            relational_detail::ContainsAggregate(bs.join_condition)) {
          return false;
        }
        std::optional<Expression> qbjc = qualify_against_rels(
            qualify_against_rels, bs.join_condition, b_rels, nullptr, "");
        if (!qbjc.has_value()) {
          return false;
        }
        bs.join_condition = std::move(*qbjc);
      }
      merged_sources.push_back(std::move(bs));
    }
    for (size_t i = 0; i < stmt->Sources().size(); ++i) {
      if (i == *u_idx) {
        continue;
      }
      SelectSource ds = stmt->Sources()[i];
      if (ds.alias.empty()) {
        ds.alias = ds.table;
      }
      ds.join_type = JoinType::kCross;
      ds.join_condition = nullptr;
      merged_sources.push_back(std::move(ds));
    }

    std::vector<Expression> b_where_parts;
    if (br->WhereClause()) {
      if (!DerivedExpressionIsImmutable(br->WhereClause()) ||
          ContainsQueryExpression(br->WhereClause()) ||
          relational_detail::ContainsAggregate(br->WhereClause())) {
        return false;
      }
      std::optional<Expression> qbw = qualify_against_rels(
          qualify_against_rels, br->WhereClause(), b_rels, nullptr, "");
      if (!qbw.has_value()) {
        return false;
      }
      b_where_parts.push_back(std::move(*qbw));
    }
    for (const Expression& ow : outer_where_parts) {
      b_where_parts.push_back(subst_u(subst_u, ow));
    }

    std::vector<Expression> b_group;
    std::vector<NamedExpression> b_select;
    for (size_t g = 0; g < qual_group_keys.size(); ++g) {
      Expression sub_g = subst_u(subst_u, qual_group_keys[g]);
      b_select.emplace_back("__gk_" + std::to_string(g), sub_g);
      if (sub_g->Type() != TypeTag::kConstantValue) {
        b_group.push_back(std::move(sub_g));
      }
    }
    if (b_group.empty()) {
      b_group.push_back(subst_u(subst_u, qual_group_keys[0]));
    }
    size_t agg_seq = 0;
    for (const OuterSelectSlot& slot : select_slots) {
      if (!slot.is_agg) {
        continue;
      }
      Expression sub_arg = subst_u(subst_u, slot.qual_agg_arg);
      b_select.emplace_back(
          "__pagg_" + std::to_string(agg_seq++),
          AggregateExpressionExp(slot.agg_type, std::move(sub_arg), false));
    }

    auto pre = std::make_shared<SelectStatement>();
    pre->SetSources(std::move(merged_sources));
    pre->SetWhereClause(
        b_where_parts.empty() ? nullptr : CombineConjuncts(b_where_parts));
    pre->SetGroupBy(std::move(b_group));
    pre->SetSelectList(std::move(b_select));
    pre_branches.push_back(std::move(pre));
  }

  auto union_subq = std::make_shared<SelectStatement>(*pre_branches[0]);
  for (size_t b = 1; b < pre_branches.size(); ++b) {
    union_subq->AddUnionAll(pre_branches[b]);
  }

  std::vector<Expression> new_outer_group;
  new_outer_group.reserve(qual_group_keys.size());
for (size_t g = 0; g < qual_group_keys.size(); ++g) {
    new_outer_group.push_back(ColumnValueExp(
        ColumnName("__tpcds_upre", "__gk_" + std::to_string(g))));
  }
  std::vector<NamedExpression> new_outer_select;
  size_t agg_seq = 0;
  for (const OuterSelectSlot& slot : select_slots) {
    if (!slot.is_agg) {
      new_outer_select.emplace_back(
          slot.out_name,
          ColumnValueExp(ColumnName("__tpcds_upre",
                                    "__gk_" + std::to_string(slot.key_idx))));
    } else {
      const AggregationType outer_op = slot.agg_type == AggregationType::kCount
                                           ? AggregationType::kSum
                                           : slot.agg_type;
      new_outer_select.emplace_back(
          slot.out_name,
          AggregateExpressionExp(
              outer_op,
              ColumnValueExp(ColumnName("__tpcds_upre",
                                        "__pagg_" + std::to_string(agg_seq++))),
              false));
    }
  }

  stmt->SetSources({SelectSource("", "__tpcds_upre", std::move(union_subq),
                                 JoinType::kCross, nullptr)});
  stmt->SetWhereClause(nullptr);
  stmt->SetGroupBy(std::move(new_outer_group));
  stmt->SetSelectList(std::move(new_outer_select));
  stmt->SetOrderBy(std::move(new_outer_order));
  return true;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (zero_padded_union_branch_preagg_sound,
//    zero_padded_union_naive_null_bug,
//    zero_padded_union_naive_coalesce_bug)
bool DistributeZeroPaddedUnionGroupBy(SelectStatement* select,
                                      TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    if (select->IsRecursiveWith(name) || body == nullptr) {
      continue;
    }
    changed |= DistributeOneUnionGroupBy(body.get(), ctx);
  }
  changed |= DistributeOneUnionGroupBy(select, ctx);
  return changed;
}

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (union_cte_disjunctive_pushdown_and_branch_prune_sound,
//    grouped_self_join_conditional_pivot_sound,
//    self_join_pivot_null_key_bug,
//    self_join_pivot_missing_year_bug)
bool SpecializeAndPivotUnionCtes(SelectStatement* outer,
                                 TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*outer) || TpcdsStmtHasParam(*outer)) {
    return false;
  }
  bool any_changed = false;

  // Step 3a: Split tag-discriminated UNION ALL CTEs into per-branch CTEs.
  std::vector<std::string> cte_names = outer->WithQueryOrder();
  for (const std::string& cte_name : cte_names) {
    if (outer->IsRecursiveWith(cte_name)) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    std::unordered_set<std::string> single_set{cte_name};
    std::unordered_map<std::string, size_t> ref_counts;
    CountCteReferencesInStatement(*outer, single_set, &ref_counts);
    const size_t total_refs = ref_counts[cte_name];
    if (total_refs < 2) {
      continue;
    }

    std::vector<size_t> site_indices;
    bool all_top_inner = true;
    std::unordered_set<std::string> seen_aliases;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& s = outer->Sources()[i];
      if (s.table == cte_name) {
        const std::string alias = s.alias.empty() ? s.table : s.alias;
        if (s.query != nullptr || s.is_lateral || s.unnest || s.sample ||
            s.from_nested_join || !s.using_columns.empty() ||
            (s.join_type != JoinType::kCross &&
             s.join_type != JoinType::kInner) ||
            !seen_aliases.insert(LoweredStarName(alias)).second) {
          all_top_inner = false;
          break;
        }
        if (alias != cte_name && StatementQualifiesName(*outer, cte_name)) {
          all_top_inner = false;
          break;
        }
        site_indices.push_back(i);
      }
    }
    if (!all_top_inner || site_indices.size() != total_refs) {
      continue;
    }

    std::vector<std::shared_ptr<SelectStatement>> branches;
    if (!ExtractPureUnionAllBranches(*cte_it->second, &branches)) {
      continue;
    }
    if (relational_detail::StatementContainsVolatileFunction(*cte_it->second)) {
      continue;
    }
    const size_t arity = branches[0]->SelectList().size();
    if (arity == 0 || SelectListHasStarOrModifiers(*branches[0])) {
      continue;
    }
    bool arity_ok = true;
    for (size_t b = 1; b < branches.size(); ++b) {
      if (branches[b]->SelectList().size() != arity ||
          SelectListHasStarOrModifiers(*branches[b])) {
        arity_ok = false;
        break;
      }
    }
    if (!arity_ok) {
      continue;
    }

    std::vector<Expression> outer_conjuncts =
        SplitConjuncts(outer->WhereClause());
    for (size_t si : site_indices) {
      if (outer->Sources()[si].join_condition) {
        for (const Expression& c :
             SplitConjuncts(outer->Sources()[si].join_condition)) {
          outer_conjuncts.push_back(c);
        }
      }
    }

    std::optional<size_t> matched_tag_col;
    std::vector<size_t> site_branch_map(site_indices.size(), 0);
    for (size_t col_idx = 0; col_idx < arity; ++col_idx) {
      const std::string& tag_name = branches[0]->SelectList()[col_idx].name;
      if (tag_name.empty()) {
        continue;
      }
      std::vector<Value> branch_tags;
      bool const_ok = true;
      for (const auto& br : branches) {
        std::optional<Value> v =
            TpcdsEvalClosedConstant(br->SelectList()[col_idx].expression);
        if (!v.has_value() || !IsSafeDisjointTagValue(*v) ||
            (!branch_tags.empty() && v->type != branch_tags.front().type)) {
          const_ok = false;
          break;
        }
        branch_tags.push_back(std::move(*v));
      }
      if (!const_ok) {
        continue;
      }
      bool distinct = true;
      for (size_t a = 0; a < branch_tags.size() && distinct; ++a) {
        for (size_t b = a + 1; b < branch_tags.size(); ++b) {
          if (branch_tags[a] == branch_tags[b]) {
            distinct = false;
            break;
          }
        }
      }
      if (!distinct) {
        continue;
      }

      bool all_sites_pinned = true;
      for (size_t s = 0; s < site_indices.size(); ++s) {
        const SelectSource& src = outer->Sources()[site_indices[s]];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        std::optional<size_t> found_b;
        for (const Expression& conj : outer_conjuncts) {
          if (!conj || conj->Type() != TypeTag::kBinaryExp ||
              conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            continue;
          }
          const auto& bin = conj->AsBinaryExpression();
          auto check_side =
              [&](const Expression& col_side,
                  const Expression& val_side) -> std::optional<Value> {
            if (!col_side || col_side->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = col_side->AsColumnValue().GetColumnName();
            if (!IdentifierEquals(cn.schema, salias) ||
                !IdentifierEquals(cn.name, tag_name)) {
              return std::nullopt;
            }
            return TpcdsEvalClosedConstant(val_side);
          };
          std::optional<Value> sv = check_side(bin.Left(), bin.Right());
          if (!sv.has_value()) {
            sv = check_side(bin.Right(), bin.Left());
          }
          if (!sv.has_value()) {
            continue;
          }
          for (size_t b = 0; b < branch_tags.size(); ++b) {
            if (*sv == branch_tags[b]) {
              found_b = b;
              break;
            }
          }
          if (found_b.has_value()) {
            break;
          }
        }
        if (!found_b.has_value()) {
          all_sites_pinned = false;
          break;
        }
        site_branch_map[s] = *found_b;
      }
      if (all_sites_pinned) {
        matched_tag_col = col_idx;
        break;
      }
    }
    if (!matched_tag_col.has_value()) {
      continue;
    }

    std::vector<SelectSource> new_sources = outer->Sources();
    std::unordered_map<size_t, std::string> created_branch_ctes;
    for (size_t s = 0; s < site_indices.size(); ++s) {
      const size_t b = site_branch_map[s];
      auto it = created_branch_ctes.find(b);
      if (it == created_branch_ctes.end()) {
        auto specialized = std::make_shared<SelectStatement>(*branches[b]);
        std::vector<NamedExpression> aligned = specialized->SelectList();
        for (size_t c = 0; c < arity; ++c) {
          aligned[c].name = branches[0]->SelectList()[c].name;
        }
        specialized->SetSelectList(std::move(aligned));
        std::string b_name =
            "__tpcds_uch_" + cte_name + "_" + std::to_string(b);
        outer->AddWithQuery(b_name, std::move(specialized));
        it = created_branch_ctes.emplace(b, std::move(b_name)).first;
      }
      SelectSource& target_src = new_sources[site_indices[s]];
      if (target_src.alias.empty()) {
        target_src.alias = cte_name;
      }
      target_src.table = it->second;
    }
    outer->SetSources(std::move(new_sources));
    outer->RemoveWithQuery(cte_name);
    any_changed = true;
  }

  // Run FD GROUP BY reduction on non-recursive CTE bodies (reduces Q4/Q11/Q74
  // 8-column customer GROUP BY down to `(c_customer_id, d_year)`).
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr) {
      any_changed |= ReduceGroupByFunctionalDependencies(body.get(), ctx);
    }
  }

  // Step 3b & 3c: Push disjunctive group-key bounds into shared grouped CTEs,
  // and pivot multi-year self-joins into single-pass conditional aggregates.
  std::vector<std::string> post_cte_names = outer->WithQueryOrder();
  for (const std::string& cte_name : post_cte_names) {
    if (outer->IsRecursiveWith(cte_name)) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    SelectStatement& body = *cte_it->second;
    if (body.GroupBy().size() < 2 || body.IsGroupAll() || body.Having() ||
        body.Distinct() || body.HasDistinctOn() || body.HasLimit() ||
        body.Offset() != 0 || body.Qualify() || !body.WithQueries().empty() ||
        !body.UnionAll().empty() || body.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(body) ||
        relational_detail::StatementContainsVolatileFunction(body) ||
        SelectListHasStarOrModifiers(body)) {
      continue;
    }

    std::unordered_set<std::string> single_set{cte_name};
    std::unordered_map<std::string, size_t> ref_counts;
    CountCteReferencesInStatement(*outer, single_set, &ref_counts);
    const size_t total_refs = ref_counts[cte_name];
    if (total_refs < 2) {
      continue;
    }

    // Require all outer sources to be plain inner/cross joins so site WHERE
    // predicates and conditional-aggregate pivots preserve join multiplicity.
    bool outer_all_inner = true;
    std::vector<size_t> site_indices;
    std::unordered_set<std::string> site_aliases_lower;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& s = outer->Sources()[i];
      if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
        outer_all_inner = false;
        break;
      }
      if (s.table == cte_name) {
        const std::string alias = s.alias.empty() ? s.table : s.alias;
        if (s.query != nullptr || s.is_lateral || s.unnest || s.sample ||
            s.from_nested_join || !s.using_columns.empty() ||
            !site_aliases_lower.insert(LoweredStarName(alias)).second ||
            (alias != cte_name && StatementQualifiesName(*outer, cte_name))) {
          outer_all_inner = false;
          break;
        }
        site_indices.push_back(i);
      }
    }
    if (!outer_all_inner || site_indices.size() != total_refs) {
      continue;
    }

    // Resolve body relations and certified PK relations.
    std::vector<std::shared_ptr<Table>> body_holders;
    std::vector<GroupingRelation> body_rels;
    bool body_rels_ok = true;
    for (const SelectSource& bs : body.Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.is_lateral ||
          bs.unnest || bs.sample || bs.from_nested_join ||
          !bs.using_columns.empty() ||
          (bs.join_type != JoinType::kCross &&
           bs.join_type != JoinType::kInner)) {
        body_rels_ok = false;
        break;
      }
      StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
      if (!btbl.HasValue()) {
        body_rels_ok = false;
        break;
      }
      body_holders.push_back(btbl.MoveValue());
      body_rels.push_back({.name = bs.alias.empty() ? bs.table : bs.alias,
                           .schema = &body_holders.back()->GetSchema()});
    }
    if (!body_rels_ok || body_rels.empty()) {
      continue;
    }

    struct BodyGroupKey {
      Expression expr;
      GroupingRelation rel;
      std::string col_name;
      std::string out_name;
    };
    std::vector<BodyGroupKey> gkeys;
    bool gkeys_ok = true;
    for (const Expression& gk : body.GroupBy()) {
      if (!gk || gk->Type() != TypeTag::kColumnValue) {
        gkeys_ok = false;
        break;
      }
      const ColumnName& gc = gk->AsColumnValue().GetColumnName();
      GroupingRelation grel;
      if (!ResolveGroupingColumn(body_rels, gc, &grel)) {
        gkeys_ok = false;
        break;
      }
      std::string matched_out;
      for (const NamedExpression& item : body.SelectList()) {
        if (item.name.empty() || !item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        GroupingRelation irel;
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        if (ResolveGroupingColumn(body_rels, ic, &irel) &&
            irel.name == grel.name && IdentifierEquals(ic.name, gc.name)) {
          matched_out = item.name;
          break;
        }
      }
      if (matched_out.empty()) {
        gkeys_ok = false;
        break;
      }
      gkeys.push_back({.expr = gk,
                       .rel = std::move(grel),
                       .col_name = gc.name,
                       .out_name = std::move(matched_out)});
    }
    if (!gkeys_ok) {
      continue;
    }

    std::vector<Expression> outer_conjuncts =
        SplitConjuncts(outer->WhereClause());
    for (size_t si : site_indices) {
      if (outer->Sources()[si].join_condition) {
        for (const Expression& c :
             SplitConjuncts(outer->Sources()[si].join_condition)) {
          outer_conjuncts.push_back(c);
        }
      }
    }

    // Look for a group-key column `disc_idx` pinned to a closed constant at
    // every consumer site.
    std::optional<size_t> disc_idx;
    std::vector<Value> site_disc_vals(site_indices.size());
    for (size_t k = 0; k < gkeys.size(); ++k) {
      bool all_pinned = true;
      std::vector<Value> vals;
      vals.reserve(site_indices.size());
      for (unsigned long site_indice : site_indices) {
        const SelectSource& src = outer->Sources()[site_indice];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        std::optional<Value> pinned;
        for (const Expression& conj : outer_conjuncts) {
          if (!conj || conj->Type() != TypeTag::kBinaryExp ||
              conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            continue;
          }
          const auto& bin = conj->AsBinaryExpression();
          auto extract_const =
              [&](const Expression& cside,
                  const Expression& vside) -> std::optional<Value> {
            if (!cside || cside->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = cside->AsColumnValue().GetColumnName();
            if (!IdentifierEquals(cn.schema, salias) ||
                !IdentifierEquals(cn.name, gkeys[k].out_name)) {
              return std::nullopt;
            }
            return TpcdsEvalClosedConstant(vside);
          };
          std::optional<Value> cv = extract_const(bin.Left(), bin.Right());
          if (!cv.has_value()) {
            cv = extract_const(bin.Right(), bin.Left());
          }
          if (cv.has_value() && IsSafeDisjointTagValue(*cv)) {
            pinned = std::move(*cv);
            break;
          }
        }
        if (!pinned.has_value() ||
            (!vals.empty() && pinned->type != vals.front().type)) {
          all_pinned = false;
          break;
        }
        vals.push_back(std::move(*pinned));
      }
      if (all_pinned) {
        disc_idx = k;
        site_disc_vals = std::move(vals);
        break;
      }
    }
    if (!disc_idx.has_value()) {
      continue;
    }

    // Step 3b: Push disjunctive filter `(disc = v_0 OR ... OR disc = v_{r-1})`
    // into `body.WhereClause()` (sound even if Step 3c below declines!).
    std::vector<Value> unique_disc_vals;
    for (const Value& v : site_disc_vals) {
      if (!std::ranges::any_of(unique_disc_vals,
                               [&](const Value& u) { return u == v; })) {
        unique_disc_vals.push_back(v);
      }
    }
    Expression disc_disjunction;
    for (const Value& uv : unique_disc_vals) {
      Expression eq =
          BinaryExpressionExp(gkeys[*disc_idx].expr, BinaryOperation::kEquals,
                              ConstantValueExp(uv));
      disc_disjunction =
          disc_disjunction
              ? BinaryExpressionExp(disc_disjunction, BinaryOperation::kOr,
                                    std::move(eq))
              : std::move(eq);
    }
    if (disc_disjunction) {
      Expression new_bw =
          body.WhereClause()
              ? BinaryExpressionExp(body.WhereClause(), BinaryOperation::kAnd,
                                    disc_disjunction)
              : disc_disjunction;
      body.SetWhereClause(std::move(new_bw));
      any_changed = true;
    }

    // Step 3c: Check if all `r` sites have pairwise-distinct `site_disc_vals`
    // and equate all remaining group keys `g != *disc_idx` across all sites.
    if (unique_disc_vals.size() != site_indices.size()) {
      continue;
    }

    // Build transitive column-equality graph over `outer_conjuncts`.
    std::unordered_map<std::string, std::vector<std::string>> eq_adj;
    auto col_token = [](std::string_view schema, std::string_view col) {
      return LoweredStarName(schema) + "." + LoweredStarName(col);
    };
    for (const Expression& conj : outer_conjuncts) {
      if (!conj || conj->Type() != TypeTag::kBinaryExp ||
          conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        continue;
      }
      const auto& bin = conj->AsBinaryExpression();
      if (!bin.Left() || !bin.Right() ||
          bin.Left()->Type() != TypeTag::kColumnValue ||
          bin.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& lc = bin.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rc = bin.Right()->AsColumnValue().GetColumnName();
      if (lc.schema.empty() || rc.schema.empty()) {
        continue;
      }
      const std::string lt = col_token(lc.schema, lc.name);
      const std::string rt = col_token(rc.schema, rc.name);
      eq_adj[lt].push_back(rt);
      eq_adj[rt].push_back(lt);
    }
    auto are_equated = [&](const std::string& a, const std::string& b) {
      if (a == b) {
        return true;
      }
      std::unordered_set<std::string> visited{a};
      std::vector<std::string> queue{a};
      while (!queue.empty()) {
        std::string cur = std::move(queue.back());
        queue.pop_back();
        const auto it = eq_adj.find(cur);
        if (it == eq_adj.end()) {
          continue;
        }
        for (const std::string& nxt : it->second) {
          if (nxt == b) {
            return true;
          }
          if (visited.insert(nxt).second) {
            queue.push_back(nxt);
          }
        }
      }
      return false;
    };

    const std::string rep_alias =
        outer->Sources()[site_indices[0]].alias.empty()
            ? outer->Sources()[site_indices[0]].table
            : outer->Sources()[site_indices[0]].alias;
    bool all_shared_keys_equated = true;
    std::unordered_set<std::string> certified_shared_rels;
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      if (ColumnIsPrimaryKey(gkeys[k].rel, gkeys[k].col_name)) {
        certified_shared_rels.insert(gkeys[k].rel.name);
      }
      const std::string rep_tok = col_token(rep_alias, gkeys[k].out_name);
      for (size_t s = 1; s < site_indices.size(); ++s) {
        const SelectSource& src = outer->Sources()[site_indices[s]];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        if (!are_equated(rep_tok, col_token(salias, gkeys[k].out_name))) {
          all_shared_keys_equated = false;
          break;
        }
      }
      if (!all_shared_keys_equated) {
        break;
      }
    }
    if (!all_shared_keys_equated) {
      continue;
    }

    // Classify every item in `body.SelectList()`.
    enum class ItemKind : uint8_t { kSharedOrFd, kDiscKey, kConst, kAgg };
    struct BodyItemSpec {
      std::string out_name;
      ItemKind kind{ItemKind::kSharedOrFd};
      Expression expr;
      AggregationType agg_type{AggregationType::kSum};
      Expression agg_arg;
    };
    std::vector<BodyItemSpec> item_specs;
    bool items_ok = true;
    std::unordered_set<std::string> seen_body_out;
    for (const NamedExpression& item : body.SelectList()) {
      if (item.name.empty() || !item.expression ||
          !seen_body_out.insert(LoweredStarName(item.name)).second) {
        items_ok = false;
        break;
      }
      BodyItemSpec spec;
      spec.out_name = item.name;
      spec.expr = item.expression;
      if (std::optional<Value> cv = TpcdsEvalClosedConstant(item.expression);
          cv.has_value()) {
        spec.kind = ItemKind::kConst;
        spec.expr = ConstantValueExp(*cv);
        item_specs.push_back(std::move(spec));
        continue;
      }
      if (item.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        GroupingRelation irel;
        if (!ResolveGroupingColumn(body_rels, ic, &irel)) {
          items_ok = false;
          break;
        }
        if (irel.name == gkeys[*disc_idx].rel.name &&
            IdentifierEquals(ic.name, gkeys[*disc_idx].col_name)) {
          spec.kind = ItemKind::kDiscKey;
          item_specs.push_back(std::move(spec));
          continue;
        }
        bool is_shared_or_fd = certified_shared_rels.contains(irel.name);
        for (size_t k = 0; k < gkeys.size() && !is_shared_or_fd; ++k) {
          if (k != *disc_idx && irel.name == gkeys[k].rel.name &&
              IdentifierEquals(ic.name, gkeys[k].col_name)) {
            is_shared_or_fd = true;
          }
        }
        if (!is_shared_or_fd) {
          items_ok = false;
          break;
        }
        spec.kind = ItemKind::kSharedOrFd;
        item_specs.push_back(std::move(spec));
        continue;
      }
      if (item.expression->Type() == TypeTag::kAggregateExp) {
        const auto& agg = item.expression->AsAggregateExpression();
        if (agg.Distinct() || agg.NeedsGroupContext() || agg.HasWhereFilter() ||
            !agg.InnerGroupBy().empty() || !agg.InnerOrderBy().empty() ||
            !agg.Child() || ContainsQueryExpression(agg.Child()) ||
            !DerivedExpressionIsImmutable(agg.Child())) {
          items_ok = false;
          break;
        }
        if (agg.GetType() != AggregationType::kSum &&
            agg.GetType() != AggregationType::kCount &&
            agg.GetType() != AggregationType::kMin &&
            agg.GetType() != AggregationType::kMax) {
          items_ok = false;
          break;
        }
        spec.kind = ItemKind::kAgg;
        spec.agg_type = agg.GetType();
        spec.agg_arg = agg.Child();
        item_specs.push_back(std::move(spec));
        continue;
      }
      items_ok = false;
      break;
    }
    if (!items_ok) {
      continue;
    }

    // Build replacement map `(lowered_site_alias, lowered_col_name) ->
    // Expression`.
    std::vector<NamedExpression> piv_select;
    std::unordered_map<std::string, Expression> site_col_map;
    for (size_t idx = 0; idx < item_specs.size(); ++idx) {
      const BodyItemSpec& spec = item_specs[idx];
      if (spec.kind == ItemKind::kSharedOrFd) {
        piv_select.emplace_back(spec.out_name, spec.expr);
        for (unsigned long site_indice : site_indices) {
          const SelectSource& src = outer->Sources()[site_indice];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          site_col_map.emplace(
              col_token(salias, spec.out_name),
              ColumnValueExp(ColumnName(rep_alias, spec.out_name)));
        }
      } else if (spec.kind == ItemKind::kConst) {
        for (unsigned long site_indice : site_indices) {
          const SelectSource& src = outer->Sources()[site_indice];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          site_col_map.emplace(col_token(salias, spec.out_name), spec.expr);
        }
      } else if (spec.kind == ItemKind::kDiscKey) {
        for (size_t s = 0; s < site_indices.size(); ++s) {
          const SelectSource& src = outer->Sources()[site_indices[s]];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          site_col_map.emplace(col_token(salias, spec.out_name),
                               ConstantValueExp(site_disc_vals[s]));
        }
      } else if (spec.kind == ItemKind::kAgg) {
        for (size_t s = 0; s < site_indices.size(); ++s) {
          const SelectSource& src = outer->Sources()[site_indices[s]];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          const std::string cname =
              "__tpcds_piv_" + std::to_string(s) + "_" + std::to_string(idx);
          Expression cond = BinaryExpressionExp(
              gkeys[*disc_idx].expr, BinaryOperation::kEquals,
              ConstantValueExp(site_disc_vals[s]));
          Expression branch_val =
              (spec.agg_arg->Type() == TypeTag::kColumnValue &&
               spec.agg_arg->AsColumnValue().GetColumnName().name == "*")
                  ? ConstantValueExp(Value(static_cast<int64_t>(1)))
                  : spec.agg_arg;
          piv_select.emplace_back(
              cname,
              AggregateExpressionExp(
                  spec.agg_type,
                  CaseExpressionExp({{std::move(cond), std::move(branch_val)}},
                                    nullptr),
                  false));
          site_col_map.emplace(col_token(salias, spec.out_name),
                               ColumnValueExp(ColumnName(rep_alias, cname)));
        }
      }
    }
    for (size_t s = 0; s < site_indices.size(); ++s) {
      const std::string cnt_name = "__tpcds_yr_cnt_" + std::to_string(s);
      Expression cond =
          BinaryExpressionExp(gkeys[*disc_idx].expr, BinaryOperation::kEquals,
                              ConstantValueExp(site_disc_vals[s]));
      piv_select.emplace_back(
          cnt_name,
          AggregateExpressionExp(
              AggregationType::kCount,
              CaseExpressionExp(
                  {{std::move(cond),
                    ConstantValueExp(Value(static_cast<int64_t>(1)))}},
                  nullptr),
              false));
    }

    auto rebind_outer_expr =
        [&](auto& self, const Expression& expr) -> std::optional<Expression> {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kQueryExp) {
        return std::nullopt;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = expr->AsColumnValue().GetColumnName();
        if (cn.name == "*") {
          return std::nullopt;
        }
        if (cn.schema.empty()) {
          if (seen_body_out.contains(LoweredStarName(cn.name))) {
            return std::nullopt;
          }
          return expr;
        }
        if (site_aliases_lower.contains(LoweredStarName(cn.schema))) {
          const auto found = site_col_map.find(col_token(cn.schema, cn.name));
          if (found == site_col_map.end()) {
            return std::nullopt;
          }
          return found->second;
        }
        return expr;
      }
      std::vector<Expression> kids = ExpressionChildren(expr);
      for (Expression& k : kids) {
        std::optional<Expression> rk = self(self, k);
        if (!rk.has_value()) {
          return std::nullopt;
        }
        k = std::move(*rk);
      }
      return WithExpressionChildren(expr, std::move(kids));
    };

    std::vector<NamedExpression> new_outer_sel;
    bool rebind_ok = true;
    for (size_t i = 0; i < outer->SelectList().size(); ++i) {
      const NamedExpression& item = outer->SelectList()[i];
      std::string preserved_name = InferSelectItemOutputName(item, i);
      std::optional<Expression> r =
          rebind_outer_expr(rebind_outer_expr, item.expression);
      if (!r.has_value()) {
        rebind_ok = false;
        break;
      }
      new_outer_sel.emplace_back(std::move(preserved_name), std::move(*r));
    }
    std::optional<Expression> new_outer_where =
        rebind_outer_expr(rebind_outer_expr, outer->WhereClause());
    std::optional<Expression> new_outer_having =
        rebind_outer_expr(rebind_outer_expr, outer->Having());
    std::optional<Expression> new_outer_qualify =
        rebind_outer_expr(rebind_outer_expr, outer->Qualify());
    if (!rebind_ok || !new_outer_where.has_value() ||
        !new_outer_having.has_value() || !new_outer_qualify.has_value()) {
      continue;
    }
    std::vector<Expression> new_outer_group;
    for (const Expression& g : outer->GroupBy()) {
      std::optional<Expression> rg = rebind_outer_expr(rebind_outer_expr, g);
      if (!rg.has_value()) {
        rebind_ok = false;
        break;
      }
      new_outer_group.push_back(std::move(*rg));
    }
    std::vector<SelectStatement::OrderByTerm> new_outer_order =
        outer->OrderBy();
    for (auto& term : new_outer_order) {
      std::optional<Expression> ro =
          rebind_outer_expr(rebind_outer_expr, term.expression);
      if (!ro.has_value()) {
        rebind_ok = false;
        break;
      }
      term.expression = std::move(*ro);
    }
    if (!rebind_ok) {
      continue;
    }

    std::unordered_set<size_t> drop_set(site_indices.begin() + 1,
                                        site_indices.end());
    std::vector<SelectSource> new_outer_sources;
    std::vector<Expression> extra_where;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      SelectSource s = outer->Sources()[i];
      if (s.join_condition) {
        std::optional<Expression> rjc =
            rebind_outer_expr(rebind_outer_expr, s.join_condition);
        if (!rjc.has_value()) {
          rebind_ok = false;
          break;
        }
        if (drop_set.contains(i)) {
          extra_where.push_back(std::move(*rjc));
        } else {
          s.join_condition = std::move(*rjc);
        }
      }
      if (drop_set.contains(i)) {
        continue;
      }
      if (i == site_indices[0]) {
        s.alias = rep_alias;
        s.table = "__tpcds_pivcte_" + cte_name;
      }
      new_outer_sources.push_back(std::move(s));
    }
    if (!rebind_ok) {
      continue;
    }

    // Build pivoted CTE body with explicit NOT NULL guards on shared keys
    // (`self_join_pivot_null_key_bug`) and per-year count > 0 guards
    // (`self_join_pivot_missing_year_bug`).
    auto piv_body = std::make_shared<SelectStatement>(body);
    std::vector<Expression> piv_group;
    std::vector<Expression> piv_where = SplitConjuncts(piv_body->WhereClause());
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      piv_group.push_back(gkeys[k].expr);
      piv_where.push_back(
          UnaryExpressionExp(gkeys[k].expr, UnaryOperation::kIsNotNull));
    }
    piv_body->SetWhereClause(CombineConjuncts(piv_where));
    piv_body->SetGroupBy(std::move(piv_group));
    piv_body->SetSelectList(std::move(piv_select));

    std::vector<Expression> final_outer_where;
    if (*new_outer_where) {
      final_outer_where.push_back(std::move(*new_outer_where));
    }
    for (Expression& ew : extra_where) {
      final_outer_where.push_back(std::move(ew));
    }
    for (size_t s = 0; s < site_indices.size(); ++s) {
      final_outer_where.push_back(BinaryExpressionExp(
          ColumnValueExp(
              ColumnName(rep_alias, "__tpcds_yr_cnt_" + std::to_string(s))),
          BinaryOperation::kGreaterThan,
          ConstantValueExp(Value(static_cast<int64_t>(0)))));
    }

    outer->AddWithQuery("__tpcds_pivcte_" + cte_name, std::move(piv_body));
    outer->RemoveWithQuery(cte_name);
    outer->SetSources(std::move(new_outer_sources));
    outer->SetSelectList(std::move(new_outer_sel));
    outer->SetWhereClause(CombineConjuncts(final_outer_where));
    outer->SetGroupBy(std::move(new_outer_group));
    outer->SetHaving(std::move(*new_outer_having));
    outer->SetQualify(std::move(*new_outer_qualify));
    outer->SetOrderBy(std::move(new_outer_order));
    any_changed = true;
  }

  return any_changed;
}

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (full_outer_indicator_inclusion_exclusion_sound,
//    full_outer_indicator_partial_null_key_bug,
//    full_outer_indicator_empty_null_bug)
bool RewriteFullOuterIndicatorSumsToSemiJoin(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() != 2 ||
      select->WhereClause() || !select->GroupBy().empty() || select->Having() ||
      select->Distinct() || select->HasDistinctOn() || select->Qualify() ||
      !select->OrderBy().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->ForUpdate() ||
      select->AsStruct() || select->WithTies() ||
      SelectListHasStarOrModifiers(*select) ||
      (select->HasLimit() && (select->Limit() == 0 || select->Offset() != 0))) {
    return false;
  }
  const SelectSource& l_src = select->Sources()[0];
  const SelectSource& r_src = select->Sources()[1];
  if (l_src.join_type != JoinType::kCross ||
      r_src.join_type != JoinType::kFull || !r_src.join_condition ||
      l_src.is_lateral || r_src.is_lateral || l_src.unnest || r_src.unnest ||
      l_src.sample || r_src.sample || l_src.from_nested_join ||
      r_src.from_nested_join || !l_src.using_columns.empty() ||
      !r_src.using_columns.empty() || l_src.table.empty() ||
      r_src.table.empty()) {
    return false;
  }
  const std::string l_alias = l_src.alias.empty() ? l_src.table : l_src.alias;
  const std::string r_alias = r_src.alias.empty() ? r_src.table : r_src.alias;
  if (IdentifierEquals(l_alias, r_alias)) {
    return false;
  }

  const auto l_cte = select->WithQueries().find(l_src.table);
  const auto r_cte = select->WithQueries().find(r_src.table);
  if (l_cte == select->WithQueries().end() ||
      r_cte == select->WithQueries().end() || l_cte->second == nullptr ||
      r_cte->second == nullptr || select->IsRecursiveWith(l_src.table) ||
      select->IsRecursiveWith(r_src.table)) {
    return false;
  }

  // Extract ON equalities `L.lk_j = R.rk_j`.
  std::vector<std::pair<std::string, std::string>> key_pairs;
  for (const Expression& conj : SplitConjuncts(r_src.join_condition)) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& bin = conj->AsBinaryExpression();
    if (!bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& lc = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rc = bin.Right()->AsColumnValue().GetColumnName();
    if (IdentifierEquals(lc.schema, l_alias) &&
        IdentifierEquals(rc.schema, r_alias)) {
      key_pairs.emplace_back(lc.name, rc.name);
    } else if (IdentifierEquals(lc.schema, r_alias) &&
               IdentifierEquals(rc.schema, l_alias)) {
      key_pairs.emplace_back(rc.name, lc.name);
    } else {
      return false;
    }
  }
  if (key_pairs.empty()) {
    return false;
  }

  auto body_is_nodup_on_keys = [&](const SelectStatement& body,
                                   bool left_side) -> bool {
    if (!body.UnionAll().empty() || body.GetSetOperationTree() != nullptr ||
        body.HasLimit() || body.Offset() != 0 ||
        relational_detail::StatementContainsVolatileFunction(body) ||
        SelectListHasStarOrModifiers(body)) {
      return false;
    }
    std::unordered_map<std::string, Expression> out_to_expr;
    for (const NamedExpression& item : body.SelectList()) {
      if (item.name.empty() || !item.expression) {
        return false;
      }
      out_to_expr.emplace(LoweredStarName(item.name), item.expression);
    }
    std::vector<std::string> key_expr_strs;
    for (const auto& [lk, rk] : key_pairs) {
      const std::string& kn = left_side ? lk : rk;
      const auto it = out_to_expr.find(LoweredStarName(kn));
      if (it == out_to_expr.end()) {
        return false;
      }
      key_expr_strs.push_back(it->second->ToString());
    }
    if (body.Distinct() && !body.HasDistinctOn() &&
        body.SelectList().size() == key_pairs.size()) {
      return true;
    }
    if (!body.GroupBy().empty() && !body.IsGroupAll()) {
      return std::ranges::all_of(body.GroupBy(), [&](const Expression& gk) {
        return gk && std::ranges::find(key_expr_strs, gk->ToString()) !=
                         key_expr_strs.end();
      });
    }
    return false;
  };
  if (!body_is_nodup_on_keys(*l_cte->second, true) ||
      !body_is_nodup_on_keys(*r_cte->second, false)) {
    return false;
  }

  // Classify every SELECT item as LeftOnly, RightOnly, or Both on some key
  // pair `probe_idx` (all items must share the same `probe_idx`).
  enum class IndicatorKind : uint8_t { kLeftOnly, kRightOnly, kBoth };
  struct IndicatorSlot {
    std::string out_name;
    IndicatorKind kind{IndicatorKind::kBoth};
  };
  std::optional<size_t> shared_probe_idx;
  std::vector<IndicatorSlot> slots;
  for (size_t idx = 0; idx < select->SelectList().size(); ++idx) {
    const NamedExpression& item = select->SelectList()[idx];
    if (!item.expression || item.expression->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = item.expression->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
        agg.NeedsGroupContext() || agg.HasWhereFilter() || !agg.Child() ||
        agg.Child()->Type() != TypeTag::kCaseExp) {
      return false;
    }
    const auto& ce = agg.Child()->AsCaseExpression();
    if (ce.when_clauses_.size() != 1 || !ce.else_clause_) {
      return false;
    }
    const auto then_v = TpcdsEvalClosedConstant(ce.when_clauses_[0].second);
    const auto else_v = TpcdsEvalClosedConstant(ce.else_clause_);
    if (!then_v.has_value() || !else_v.has_value() ||
        then_v->type != ValueType::kInt64 ||
        else_v->type != ValueType::kInt64 || then_v->IsNull() ||
        else_v->IsNull() || then_v->value.int_value != 1 ||
        else_v->value.int_value != 0) {
      return false;
    }
    std::vector<Expression> cond_parts =
        SplitConjuncts(ce.when_clauses_[0].first);
    if (cond_parts.size() != 2) {
      return false;
    }
    std::optional<std::pair<size_t, bool>> l_test;
    std::optional<std::pair<size_t, bool>> r_test;
    for (const Expression& p : cond_parts) {
      if (!p || p->Type() != TypeTag::kUnaryExp) {
        return false;
      }
      const UnaryOperation uop = p->AsUnaryExpression().Op();
      if (uop != UnaryOperation::kIsNull && uop != UnaryOperation::kIsNotNull) {
        return false;
      }
      const Expression& ch = p->AsUnaryExpression().Child();
      if (!ch || ch->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = ch->AsColumnValue().GetColumnName();
      for (size_t k = 0; k < key_pairs.size(); ++k) {
        if (IdentifierEquals(cn.schema, l_alias) &&
            IdentifierEquals(cn.name, key_pairs[k].first)) {
          l_test = {k, uop == UnaryOperation::kIsNotNull};
        } else if (IdentifierEquals(cn.schema, r_alias) &&
                   IdentifierEquals(cn.name, key_pairs[k].second)) {
          r_test = {k, uop == UnaryOperation::kIsNotNull};
        }
      }
    }
    if (!l_test.has_value() || !r_test.has_value() ||
        l_test->first != r_test->first ||
        (!l_test->second && !r_test->second)) {
      return false;
    }
    if (shared_probe_idx.has_value() && *shared_probe_idx != l_test->first) {
      return false;
    }
    shared_probe_idx = l_test->first;
    IndicatorKind kind = IndicatorKind::kBoth;
    if (l_test->second && !r_test->second) {
      kind = IndicatorKind::kLeftOnly;
    } else if (!l_test->second && r_test->second) {
      kind = IndicatorKind::kRightOnly;
    }
    slots.push_back(
        {.out_name = InferSelectItemOutputName(item, idx), .kind = kind});
  }
  if (!shared_probe_idx.has_value() || slots.empty()) {
    return false;
  }

  auto build_side_stats_query = [&](const std::string& cte_table,
                                    bool left_side) {
    const std::string sal = left_side ? "__ls" : "__rs";
    std::vector<Expression> all_not_null;
    Expression any_other_null;
    for (size_t k = 0; k < key_pairs.size(); ++k) {
      const std::string& kn =
          left_side ? key_pairs[k].first : key_pairs[k].second;
      all_not_null.push_back(UnaryExpressionExp(
          ColumnValueExp(ColumnName(sal, kn)), UnaryOperation::kIsNotNull));
      if (k != *shared_probe_idx) {
        Expression is_null = UnaryExpressionExp(
            ColumnValueExp(ColumnName(sal, kn)), UnaryOperation::kIsNull);
        any_other_null =
            any_other_null
                ? BinaryExpressionExp(any_other_null, BinaryOperation::kOr,
                                      std::move(is_null))
                : std::move(is_null);
      }
    }
    const std::string& probe_kn = left_side
                                      ? key_pairs[*shared_probe_idx].first
                                      : key_pairs[*shared_probe_idx].second;
    Expression one = ConstantValueExp(Value(static_cast<int64_t>(1)));
    Expression part_expr =
        any_other_null
            ? AggregateExpressionExp(
                  AggregationType::kCount,
                  CaseExpressionExp(
                      {{BinaryExpressionExp(
                            UnaryExpressionExp(
                                ColumnValueExp(ColumnName(sal, probe_kn)),
                                UnaryOperation::kIsNotNull),
                            BinaryOperation::kAnd, std::move(any_other_null)),
                        one}},
                      nullptr),
                  false)
            : ConstantValueExp(Value(static_cast<int64_t>(0)));

    auto q = std::make_shared<SelectStatement>();
    q->SetSources(
        {SelectSource(cte_table, sal, nullptr, JoinType::kCross, nullptr)});
    q->SetSelectList({
        NamedExpression("tot", AggregateExpressionExp(
                                   AggregationType::kCount,
                                   ColumnValueExp(ColumnName("", "*")), false)),
        NamedExpression(
            "ok", AggregateExpressionExp(
                      AggregationType::kCount,
                      CaseExpressionExp({{CombineConjuncts(all_not_null), one}},
                                        nullptr),
                      false)),
        NamedExpression("part", std::move(part_expr)),
    });
    return q;
  };

  auto both_q = std::make_shared<SelectStatement>();
  both_q->SetSources({
      SelectSource(l_src.table, "__bl", nullptr, JoinType::kCross, nullptr),
      SelectSource(r_src.table, "__br", nullptr, JoinType::kCross, nullptr),
  });
  std::vector<Expression> both_eqs;
  both_eqs.reserve(key_pairs.size());
for (const auto& [lk, rk] : key_pairs) {
    both_eqs.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName("__bl", lk)), BinaryOperation::kEquals,
        ColumnValueExp(ColumnName("__br", rk))));
  }
  both_q->SetWhereClause(CombineConjuncts(both_eqs));
  both_q->SetSelectList({NamedExpression(
      "inter",
      AggregateExpressionExp(AggregationType::kCount,
                             ColumnValueExp(ColumnName("", "*")), false))});

  Expression non_empty = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("__foj_l", "tot")),
                          BinaryOperation::kAdd,
                          ColumnValueExp(ColumnName("__foj_r", "tot"))),
      BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(static_cast<int64_t>(0))));
  Expression typed_null = ConstantValueExp(Value());
  Expression inter_ref = ColumnValueExp(ColumnName("__foj_b", "inter"));

  std::vector<NamedExpression> new_select;
  for (const IndicatorSlot& slot : slots) {
    Expression val_expr;
    if (slot.kind == IndicatorKind::kLeftOnly) {
      val_expr = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("__foj_l", "ok")),
                              BinaryOperation::kSubtract, inter_ref),
          BinaryOperation::kAdd, ColumnValueExp(ColumnName("__foj_l", "part")));
    } else if (slot.kind == IndicatorKind::kRightOnly) {
      val_expr = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("__foj_r", "ok")),
                              BinaryOperation::kSubtract, inter_ref),
          BinaryOperation::kAdd, ColumnValueExp(ColumnName("__foj_r", "part")));
    } else {
      val_expr = inter_ref;
    }
    new_select.emplace_back(
        slot.out_name,
        CaseExpressionExp({{non_empty, std::move(val_expr)}}, typed_null));
  }

  select->SetSources({
      SelectSource("", "__foj_l", build_side_stats_query(l_src.table, true),
                   JoinType::kCross, nullptr),
      SelectSource("", "__foj_r", build_side_stats_query(r_src.table, false),
                   JoinType::kCross, nullptr),
      SelectSource("", "__foj_b", std::move(both_q), JoinType::kCross, nullptr),
  });
  select->SetSelectList(std::move(new_select));
  return true;
}

namespace {

std::optional<std::string> WindowNameForAggType(AggregationType t,
                                                bool allow_count) {
  switch (t) {
    case AggregationType::kAvg:
      return "AVG";
    case AggregationType::kSum:
      return "SUM";
    case AggregationType::kMin:
      return "MIN";
    case AggregationType::kMax:
      return "MAX";
    case AggregationType::kCount:
      return allow_count ? std::optional<std::string>("COUNT") : std::nullopt;
    default:
      return std::nullopt;
  }
}

bool RewriteSiblingDerivedAggToWindow(SelectStatement* outer) {
  if (outer == nullptr || outer->Sources().size() < 2 ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*outer)) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }

  std::vector<Expression> outer_conjuncts =
      SplitConjuncts(outer->WhereClause());
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_condition) {
      for (const Expression& c : SplitConjuncts(s.join_condition)) {
        outer_conjuncts.push_back(c);
      }
    }
  }

  for (size_t sb_idx = 0; sb_idx < outer->Sources().size(); ++sb_idx) {
    const SelectSource& sb = outer->Sources()[sb_idx];
    if (sb.query == nullptr || sb.alias.empty() || sb.is_lateral || sb.unnest ||
        sb.sample || sb.from_nested_join || !sb.using_columns.empty()) {
      continue;
    }
    const SelectStatement& sb_q = *sb.query;
    if (sb_q.Sources().size() != 1 || sb_q.Sources()[0].query == nullptr ||
        sb_q.Sources()[0].is_lateral || sb_q.GroupBy().empty() ||
        sb_q.IsGroupAll() || sb_q.Having() || sb_q.WhereClause() ||
        sb_q.Distinct() || sb_q.HasDistinctOn() || sb_q.HasLimit() ||
        sb_q.Offset() != 0 || sb_q.Qualify() || !sb_q.WithQueries().empty() ||
        !sb_q.UnionAll().empty() || sb_q.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(sb_q) ||
        SelectListHasStarOrModifiers(sb_q)) {
      continue;
    }
    const SelectStatement& sa_q = *sb_q.Sources()[0].query;
    if (relational_detail::StatementContainsVolatileFunction(sa_q) ||
        TpcdsStmtHasParam(sa_q) || SelectListHasStarOrModifiers(sa_q)) {
      continue;
    }
    const std::string sa_alias = sb_q.Sources()[0].alias;
    const std::string sa_fp = sa_q.Fingerprint();

    std::unordered_set<std::string> sa_cols_lower;
    std::vector<std::string> sa_col_names;
    bool sa_cols_ok = true;
    for (const NamedExpression& item : sa_q.SelectList()) {
      const std::string out_nm = ExplicitOrColumnOutputName(item);
      if (out_nm.empty() ||
          !sa_cols_lower.insert(LoweredStarName(out_nm)).second) {
        sa_cols_ok = false;
        break;
      }
      sa_col_names.push_back(out_nm);
    }
    if (!sa_cols_ok || sa_col_names.empty()) {
      continue;
    }

    std::vector<std::string> sb_key_cols;
    bool keys_ok = true;
    for (const Expression& gk : sb_q.GroupBy()) {
      if (!gk || gk->Type() != TypeTag::kColumnValue) {
        keys_ok = false;
        break;
      }
      const ColumnName& gc = gk->AsColumnValue().GetColumnName();
      if ((!gc.schema.empty() && !IdentifierEquals(gc.schema, sa_alias)) ||
          !sa_cols_lower.contains(LoweredStarName(gc.name))) {
        keys_ok = false;
        break;
      }
      sb_key_cols.push_back(gc.name);
    }
    if (!keys_ok) {
      continue;
    }

    struct WinAggSpec {
      std::string sb_out_name;
      std::string win_col_name;
      std::string fn_name;
      Expression arg_on_base;
    };
    std::unordered_map<std::string, std::string> sb_key_out_to_sa;
    std::vector<WinAggSpec> win_aggs;
    bool sb_sel_ok = true;
    for (const NamedExpression& item : sb_q.SelectList()) {
      const std::string sb_out = ExplicitOrColumnOutputName(item);
      if (sb_out.empty() || !item.expression) {
        sb_sel_ok = false;
        break;
      }
      if (item.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        if ((!ic.schema.empty() && !IdentifierEquals(ic.schema, sa_alias)) ||
            !std::ranges::any_of(sb_key_cols, [&](const std::string& k) {
              return IdentifierEquals(k, ic.name);
            })) {
          sb_sel_ok = false;
          break;
        }
        sb_key_out_to_sa.emplace(LoweredStarName(sb_out), ic.name);
        continue;
      }
      if (item.expression->Type() == TypeTag::kAggregateExp) {
        const auto& agg = item.expression->AsAggregateExpression();
        const auto fn = WindowNameForAggType(agg.GetType(), true);
        if (!fn.has_value() || agg.Distinct() || agg.NeedsGroupContext() ||
            agg.HasWhereFilter() || !agg.InnerGroupBy().empty() ||
            !agg.InnerOrderBy().empty() || !agg.Child() ||
            agg.Child()->Type() != TypeTag::kColumnValue) {
          sb_sel_ok = false;
          break;
        }
        const ColumnName& ac = agg.Child()->AsColumnValue().GetColumnName();
        Expression base_arg;
        if (ac.name == "*" && agg.GetType() == AggregationType::kCount) {
          base_arg = ColumnValueExp(ColumnName("", "*"));
        } else if ((ac.schema.empty() ||
                    IdentifierEquals(ac.schema, sa_alias)) &&
                   sa_cols_lower.contains(LoweredStarName(ac.name))) {
          base_arg = ColumnValueExp(ColumnName("__tpcds_sc_base", ac.name));
        } else {
          sb_sel_ok = false;
          break;
        }
        win_aggs.push_back({.sb_out_name = item.name,
                            .win_col_name = "__tpcds_sb_win_" +
                                            std::to_string(win_aggs.size()),
                            .fn_name = *fn,
                            .arg_on_base = std::move(base_arg)});
        continue;
      }
      sb_sel_ok = false;
      break;
    }
    if (!sb_sel_ok || win_aggs.empty()) {
      continue;
    }

    for (size_t sc_idx = 0; sc_idx < outer->Sources().size(); ++sc_idx) {
      if (sc_idx == sb_idx) {
        continue;
      }
      const SelectSource& sc = outer->Sources()[sc_idx];
      if (sc.query == nullptr || sc.alias.empty() || sc.is_lateral ||
          sc.unnest || sc.sample || sc.from_nested_join ||
          !sc.using_columns.empty() || IdentifierEquals(sb.alias, sc.alias) ||
          sc.query->Fingerprint() != sa_fp) {
        continue;
      }

      // Check that every `sb_key_col` is equated between `sb.alias` and
      // `sc.alias` in `outer_conjuncts`.
      bool all_keys_bound = true;
      for (const std::string& kn : sb_key_cols) {
        std::string sb_out_for_kn;
        for (const auto& [sb_out_low, sa_kn] : sb_key_out_to_sa) {
          if (IdentifierEquals(sa_kn, kn)) {
            sb_out_for_kn = sb_out_low;
            break;
          }
        }
        if (sb_out_for_kn.empty()) {
          all_keys_bound = false;
          break;
        }
        bool bound = false;
        for (const Expression& conj : outer_conjuncts) {
          if (!conj || conj->Type() != TypeTag::kBinaryExp ||
              conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            continue;
          }
          const auto& bin = conj->AsBinaryExpression();
          if (!bin.Left() || !bin.Right() ||
              bin.Left()->Type() != TypeTag::kColumnValue ||
              bin.Right()->Type() != TypeTag::kColumnValue) {
            continue;
          }
          const ColumnName& lc = bin.Left()->AsColumnValue().GetColumnName();
          const ColumnName& rc = bin.Right()->AsColumnValue().GetColumnName();
          const bool m1 = IdentifierEquals(lc.schema, sb.alias) &&
                          LoweredStarName(lc.name) == sb_out_for_kn &&
                          IdentifierEquals(rc.schema, sc.alias) &&
                          IdentifierEquals(rc.name, kn);
          const bool m2 = IdentifierEquals(rc.schema, sb.alias) &&
                          LoweredStarName(rc.name) == sb_out_for_kn &&
                          IdentifierEquals(lc.schema, sc.alias) &&
                          IdentifierEquals(lc.name, kn);
          if (m1 || m2) {
            bound = true;
            break;
          }
        }
        if (!bound) {
          all_keys_bound = false;
          break;
        }
      }
      if (!all_keys_bound) {
        continue;
      }

      std::unordered_map<std::string, Expression> sb_col_repl;
      for (const auto& [sb_out_low, sa_kn] : sb_key_out_to_sa) {
        sb_col_repl.emplace(sb_out_low,
                            ColumnValueExp(ColumnName(sc.alias, sa_kn)));
      }
      std::unordered_set<std::string> sb_only_agg_names;
      for (const WinAggSpec& w : win_aggs) {
        const std::string low = LoweredStarName(w.sb_out_name);
        sb_col_repl.emplace(
            low, ColumnValueExp(ColumnName(sc.alias, w.win_col_name)));
        if (!sa_cols_lower.contains(low)) {
          sb_only_agg_names.insert(low);
        }
      }

      auto map_expr = [&](auto& self,
                          const Expression& expr) -> std::optional<Expression> {
        if (!expr) {
          return expr;
        }
        if (expr->Type() == TypeTag::kQueryExp) {
          return std::nullopt;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = expr->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return std::nullopt;
          }
          if (!cn.schema.empty()) {
            if (IdentifierEquals(cn.schema, sb.alias)) {
              const auto it = sb_col_repl.find(LoweredStarName(cn.name));
              if (it == sb_col_repl.end()) {
                return std::nullopt;
              }
              return it->second;
            }
            return expr;
          }
          const std::string low = LoweredStarName(cn.name);
          if (sb_only_agg_names.contains(low)) {
            return sb_col_repl.at(low);
          }
          if (sb_key_out_to_sa.contains(low)) {
            return std::nullopt;
          }
          return expr;
        }
        std::vector<Expression> kids = ExpressionChildren(expr);
        for (Expression& k : kids) {
          std::optional<Expression> mk = self(self, k);
          if (!mk.has_value()) {
            return std::nullopt;
          }
          k = std::move(*mk);
        }
        return WithExpressionChildren(expr, std::move(kids));
      };

      std::vector<NamedExpression> new_sel;
      bool ok = true;
      for (size_t i = 0; i < outer->SelectList().size(); ++i) {
        const NamedExpression& item = outer->SelectList()[i];
        std::optional<Expression> m = map_expr(map_expr, item.expression);
        if (!m.has_value()) {
          ok = false;
          break;
        }
        new_sel.emplace_back(InferSelectItemOutputName(item, i), std::move(*m));
      }
      std::optional<Expression> new_where =
          map_expr(map_expr, outer->WhereClause());
      std::optional<Expression> new_having =
          map_expr(map_expr, outer->Having());
      std::optional<Expression> new_qualify =
          map_expr(map_expr, outer->Qualify());
      if (!ok || !new_where.has_value() || !new_having.has_value() ||
          !new_qualify.has_value()) {
        continue;
      }
      std::vector<Expression> new_group;
      for (const Expression& g : outer->GroupBy()) {
        std::optional<Expression> mg = map_expr(map_expr, g);
        if (!mg.has_value()) {
          ok = false;
          break;
        }
        new_group.push_back(std::move(*mg));
      }
      std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
      for (auto& term : new_order) {
        std::optional<Expression> mo = map_expr(map_expr, term.expression);
        if (!mo.has_value()) {
          ok = false;
          break;
        }
        term.expression = std::move(*mo);
      }
      if (!ok) {
        continue;
      }

      std::vector<SelectSource> new_sources;
      std::vector<Expression> extra_where;
      for (size_t i = 0; i < outer->Sources().size(); ++i) {
        SelectSource s = outer->Sources()[i];
        if (s.join_condition) {
          std::optional<Expression> mjc = map_expr(map_expr, s.join_condition);
          if (!mjc.has_value()) {
            ok = false;
            break;
          }
          if (i == sb_idx) {
            extra_where.push_back(std::move(*mjc));
          } else {
            s.join_condition = std::move(*mjc);
          }
        }
        if (i != sb_idx && s.query != nullptr && i != sc_idx) {
          // Reject if a sibling subquery correlated onto `sb`.
          if (s.is_lateral) {
            ok = false;
            break;
          }
        }
        if (i == sb_idx) {
          continue;
        }
        if (i == sc_idx) {
          std::vector<Expression> part_keys;
          part_keys.reserve(sb_key_cols.size());
for (const std::string& kn : sb_key_cols) {
            part_keys.push_back(
                ColumnValueExp(ColumnName("__tpcds_sc_base", kn)));
          }
          std::vector<NamedExpression> win_sel;
          win_sel.reserve(sa_col_names.size());
for (const std::string& c : sa_col_names) {
            win_sel.emplace_back(
                c, ColumnValueExp(ColumnName("__tpcds_sc_base", c)));
          }
          for (const WinAggSpec& w : win_aggs) {
            win_sel.emplace_back(
                w.win_col_name, WindowFunctionCallExp(
                                    w.fn_name, {w.arg_on_base}, part_keys, {}));
          }
          auto wrapped = std::make_shared<SelectStatement>();
          wrapped->SetSources({SelectSource("", "__tpcds_sc_base", s.query,
                                            JoinType::kCross, nullptr)});
          wrapped->SetSelectList(std::move(win_sel));
          wrapped->MarkComplex();
          s.query = std::move(wrapped);
        }
        new_sources.push_back(std::move(s));
      }
      if (!ok) {
        continue;
      }

      std::vector<Expression> final_where;
      if (*new_where) {
        final_where.push_back(std::move(*new_where));
      }
      for (Expression& ew : extra_where) {
        final_where.push_back(std::move(ew));
      }
      for (const std::string& kn : sb_key_cols) {
        final_where.push_back(
            UnaryExpressionExp(ColumnValueExp(ColumnName(sc.alias, kn)),
                               UnaryOperation::kIsNotNull));
      }

      outer->SetSources(std::move(new_sources));
      outer->SetSelectList(std::move(new_sel));
      outer->SetWhereClause(CombineConjuncts(final_where));
      outer->SetGroupBy(std::move(new_group));
      outer->SetHaving(std::move(*new_having));
      outer->SetQualify(std::move(*new_qualify));
      outer->SetOrderBy(std::move(new_order));
      return true;
    }
  }
  return false;
}

bool RewriteCorrelatedCteScalarAggToWindow(SelectStatement* outer) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->WhereClause() || !outer->UnionAll().empty() ||
      outer->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*outer)) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }

  std::unordered_set<std::string> all_ctes;
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    all_ctes.insert(name);
  }
  std::unordered_map<std::string, size_t> counts;
  CountCteReferencesInStatement(*outer, all_ctes, &counts);

  for (const std::string& cte_name : outer->WithQueryOrder()) {
    if (outer->IsRecursiveWith(cte_name) || counts[cte_name] != 2) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    const SelectStatement& cte_body = *cte_it->second;
    if (relational_detail::StatementContainsVolatileFunction(cte_body) ||
        SelectListHasStarOrModifiers(cte_body) ||
        cte_body.SelectList().empty()) {
      continue;
    }
    std::unordered_set<std::string> cte_cols_lower;
    std::vector<std::string> cte_col_names;
    bool cols_ok = true;
    for (const NamedExpression& item : cte_body.SelectList()) {
      if (item.name.empty() ||
          !cte_cols_lower.insert(LoweredStarName(item.name)).second) {
        cols_ok = false;
        break;
      }
      cte_col_names.push_back(item.name);
    }
    if (!cols_ok) {
      continue;
    }

    std::optional<size_t> ctr1_idx;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& s = outer->Sources()[i];
      if (s.table == cte_name) {
        if (ctr1_idx.has_value() || s.query != nullptr || s.is_lateral ||
            s.unnest || s.sample || s.from_nested_join ||
            !s.using_columns.empty()) {
          ctr1_idx.reset();
          break;
        }
        ctr1_idx = i;
      }
    }
    if (!ctr1_idx.has_value()) {
      continue;
    }
    const SelectSource& ctr1 = outer->Sources()[*ctr1_idx];
    const std::string ctr1_alias = ctr1.alias.empty() ? ctr1.table : ctr1.alias;
    if (ctr1_alias != cte_name && StatementQualifiesName(*outer, cte_name)) {
      continue;
    }

    std::vector<Expression> where_conjuncts =
        SplitConjuncts(outer->WhereClause());
    for (size_t ci = 0; ci < where_conjuncts.size(); ++ci) {
      const Expression& conj = where_conjuncts[ci];
      if (!conj || conj->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& cmp = conj->AsBinaryExpression();
      const BinaryOperation op = cmp.Op();
      if (op != BinaryOperation::kEquals && op != BinaryOperation::kNotEquals &&
          op != BinaryOperation::kLessThan &&
          op != BinaryOperation::kLessThanEquals &&
          op != BinaryOperation::kGreaterThan &&
          op != BinaryOperation::kGreaterThanEquals) {
        continue;
      }
      for (int qside = 0; qside < 2; ++qside) {
        const Expression& qcand = qside == 0 ? cmp.Left() : cmp.Right();
        const Expression& other = qside == 0 ? cmp.Right() : cmp.Left();
        if (!qcand || qcand->Type() != TypeTag::kQueryExp || !other ||
            ContainsQueryExpression(other)) {
          continue;
        }
        const auto& qe = qcand->AsQueryExpression();
        if (qe.Exists() || qe.Negated() || qe.ArrayResult() || qe.Test() ||
            qe.Query() == nullptr) {
          continue;
        }
        const SelectStatement& sq = *qe.Query();
        if (sq.Sources().size() != 1 || sq.Sources()[0].table != cte_name ||
            sq.Sources()[0].query != nullptr || sq.Sources()[0].is_lateral ||
            sq.Sources()[0].unnest || sq.Sources()[0].sample ||
            !sq.WithQueries().empty() || !sq.GroupBy().empty() || sq.Having() ||
            sq.Distinct() || sq.HasDistinctOn() || sq.HasLimit() ||
            sq.Offset() != 0 || sq.Qualify() || !sq.UnionAll().empty() ||
            sq.GetSetOperationTree() != nullptr ||
            sq.SelectList().size() != 1 || !sq.WhereClause()) {
          continue;
        }
        const std::string ctr2_alias = sq.Sources()[0].alias.empty()
                                           ? sq.Sources()[0].table
                                           : sq.Sources()[0].alias;
        if (IdentifierEquals(ctr1_alias, ctr2_alias)) {
          continue;
        }

        // Unwrap `AGG(col)` or `AGG(col) * const` / `const * AGG(col)`.
        const Expression& proj = sq.SelectList()[0].expression;
        const AggregateExpression* agg_ptr = nullptr;
        Expression scale_factor;
        bool scale_on_left = false;
        if (proj && proj->Type() == TypeTag::kAggregateExp) {
          agg_ptr = &proj->AsAggregateExpression();
        } else if (proj && proj->Type() == TypeTag::kBinaryExp &&
                   proj->AsBinaryExpression().Op() ==
                       BinaryOperation::kMultiply) {
          const auto& mul = proj->AsBinaryExpression();
          if (mul.Left() && mul.Left()->Type() == TypeTag::kAggregateExp &&
              TpcdsEvalClosedConstant(mul.Right()).has_value()) {
            agg_ptr = &mul.Left()->AsAggregateExpression();
            scale_factor = mul.Right();
            scale_on_left = false;
          } else if (mul.Right() &&
                     mul.Right()->Type() == TypeTag::kAggregateExp &&
                     TpcdsEvalClosedConstant(mul.Left()).has_value()) {
            agg_ptr = &mul.Right()->AsAggregateExpression();
            scale_factor = mul.Left();
            scale_on_left = true;
          }
        }
        if (agg_ptr == nullptr || agg_ptr->Distinct() ||
            agg_ptr->NeedsGroupContext() || agg_ptr->HasWhereFilter() ||
            !agg_ptr->InnerGroupBy().empty() ||
            !agg_ptr->InnerOrderBy().empty() || !agg_ptr->Child() ||
            agg_ptr->Child()->Type() != TypeTag::kColumnValue) {
          continue;
        }
        // Reject COUNT: COUNT(*) over an empty match (when ctr1.key IS NULL) is
        // 0 rather than NULL, whereas AVG/SUM/MIN/MAX return NULL.
        const auto fn = WindowNameForAggType(agg_ptr->GetType(), false);
        if (!fn.has_value()) {
          continue;
        }
        const ColumnName& ac =
            agg_ptr->Child()->AsColumnValue().GetColumnName();
        if ((!ac.schema.empty() && !IdentifierEquals(ac.schema, ctr2_alias)) ||
            !cte_cols_lower.contains(LoweredStarName(ac.name))) {
          continue;
        }

        std::vector<std::string> part_col_names;
        bool sq_where_ok = true;
        for (const Expression& sconj : SplitConjuncts(sq.WhereClause())) {
          if (!sconj || sconj->Type() != TypeTag::kBinaryExp ||
              sconj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            sq_where_ok = false;
            break;
          }
          const auto& sbin = sconj->AsBinaryExpression();
          if (!sbin.Left() || !sbin.Right() ||
              sbin.Left()->Type() != TypeTag::kColumnValue ||
              sbin.Right()->Type() != TypeTag::kColumnValue) {
            sq_where_ok = false;
            break;
          }
          const ColumnName& lc = sbin.Left()->AsColumnValue().GetColumnName();
          const ColumnName& rc = sbin.Right()->AsColumnValue().GetColumnName();
          std::string matched_col;
          if (IdentifierEquals(lc.schema, ctr1_alias) &&
              IdentifierEquals(rc.schema, ctr2_alias) &&
              IdentifierEquals(lc.name, rc.name) &&
              cte_cols_lower.contains(LoweredStarName(lc.name))) {
            matched_col = lc.name;
          } else if (IdentifierEquals(rc.schema, ctr1_alias) &&
                     IdentifierEquals(lc.schema, ctr2_alias) &&
                     IdentifierEquals(lc.name, rc.name) &&
                     cte_cols_lower.contains(LoweredStarName(rc.name))) {
            matched_col = rc.name;
          }
          if (matched_col.empty()) {
            sq_where_ok = false;
            break;
          }
          if (!std::ranges::any_of(part_col_names, [&](const std::string& p) {
                return IdentifierEquals(p, matched_col);
              })) {
            part_col_names.push_back(std::move(matched_col));
          }
        }
        if (!sq_where_ok || part_col_names.empty()) {
          continue;
        }

        const std::string win_col = "__tpcds_ctr_win";
        std::vector<Expression> part_exprs;
        part_exprs.reserve(part_col_names.size());
for (const std::string& p : part_col_names) {
          part_exprs.push_back(
              ColumnValueExp(ColumnName("__tpcds_ctr_base", p)));
        }
        std::vector<NamedExpression> win_sel;
        win_sel.reserve(cte_col_names.size());
for (const std::string& c : cte_col_names) {
          win_sel.emplace_back(
              c, ColumnValueExp(ColumnName("__tpcds_ctr_base", c)));
        }
        win_sel.emplace_back(
            win_col,
            WindowFunctionCallExp(
                *fn, {ColumnValueExp(ColumnName("__tpcds_ctr_base", ac.name))},
                std::move(part_exprs), {}));
        auto win_subq = std::make_shared<SelectStatement>();
        win_subq->SetSources({SelectSource(
            cte_name, "__tpcds_ctr_base", nullptr, JoinType::kCross, nullptr)});
        win_subq->SetSelectList(std::move(win_sel));
        win_subq->MarkComplex();

        Expression replacement =
            ColumnValueExp(ColumnName(ctr1_alias, win_col));
        if (scale_factor) {
          replacement = scale_on_left
                            ? BinaryExpressionExp(scale_factor,
                                                  BinaryOperation::kMultiply,
                                                  std::move(replacement))
                            : BinaryExpressionExp(std::move(replacement),
                                                  BinaryOperation::kMultiply,
                                                  scale_factor);
        }
        where_conjuncts[ci] =
            qside == 0 ? BinaryExpressionExp(std::move(replacement), op, other)
                       : BinaryExpressionExp(other, op, std::move(replacement));
        for (const std::string& p : part_col_names) {
          where_conjuncts.push_back(
              UnaryExpressionExp(ColumnValueExp(ColumnName(ctr1_alias, p)),
                                 UnaryOperation::kIsNotNull));
        }

        std::vector<SelectSource> new_sources = outer->Sources();
        new_sources[*ctr1_idx].table.clear();
        new_sources[*ctr1_idx].alias = ctr1_alias;
        new_sources[*ctr1_idx].query = std::move(win_subq);
        outer->SetSources(std::move(new_sources));
        outer->SetWhereClause(CombineConjuncts(where_conjuncts));
        return true;
      }
    }
  }
  return false;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (self_join_group_agg_to_window_sound,
//    self_join_agg_window_null_key_bug)
bool RewriteSelfJoinAggregateToWindow(SelectStatement* outer) {
  if (outer == nullptr) {
    return false;
  }
  bool changed = false;
  while (RewriteSiblingDerivedAggToWindow(outer)) {
    changed = true;
  }
  while (RewriteCorrelatedCteScalarAggToWindow(outer)) {
    changed = true;
  }
  return changed;
}

namespace {

bool DerivedQueryIsSelfContainedAndExpensive(const SelectStatement& q) {
  if (!q.WithQueries().empty() || !q.UnionAll().empty() ||
      q.GetSetOperationTree() != nullptr ||
      relational_detail::StatementContainsVolatileFunction(q) ||
      TpcdsStmtHasParam(q) || SelectListHasStarOrModifiers(q) ||
      q.SelectList().empty() || q.Sources().empty()) {
    return false;
  }
  const bool expensive = !q.GroupBy().empty() || q.Having() != nullptr ||
                         ContainsQueryExpression(q.WhereClause()) ||
                         ContainsQueryExpression(q.Having());
  if (!expensive) {
    return false;
  }
  std::unordered_set<std::string> seen_out;
  for (const NamedExpression& item : q.SelectList()) {
    if (item.name.empty() ||
        !seen_out.insert(LoweredStarName(item.name)).second) {
      return false;
    }
  }
  std::unordered_set<std::string> local_aliases;
  for (const SelectSource& s : q.Sources()) {
    if (s.table.empty() || s.query != nullptr || s.is_lateral || s.unnest ||
        s.sample || s.from_nested_join) {
      return false;
    }
    local_aliases.insert(LoweredStarName(s.table));
    if (!s.alias.empty()) {
      local_aliases.insert(LoweredStarName(s.alias));
    }
  }
  // Ensure no outer-qualified column references exist in `q`'s own scope.
  auto expr_self_contained = [&](auto& self, const Expression& e) -> bool {
    if (!e) {
      return true;
    }
    if (e->Type() == TypeTag::kQueryExp) {
      const auto& sq = e->AsQueryExpression();
      if (sq.Test() && !self(self, sq.Test())) {
        return false;
      }
      if (sq.Query() == nullptr || !sq.Query()->WithQueries().empty() ||
          sq.Query()->Sources().empty()) {
        return false;
      }
      std::unordered_set<std::string> sq_local = local_aliases;
      for (const SelectSource& ss : sq.Query()->Sources()) {
        if (ss.table.empty() || ss.query != nullptr || ss.is_lateral) {
          return false;
        }
        sq_local.insert(LoweredStarName(ss.table));
        if (!ss.alias.empty()) {
          sq_local.insert(LoweredStarName(ss.alias));
        }
      }
      auto check_sq_cols = [&](const Expression& se) {
        if (!se) {
          return true;
        }
        for (const ColumnName& cn : se->TouchedColumns()) {
          if (!cn.schema.empty() &&
              !sq_local.contains(LoweredStarName(cn.schema))) {
            return false;
          }
        }
        return true;
      };
      if (!check_sq_cols(sq.Query()->WhereClause()) ||
          !check_sq_cols(sq.Query()->Having())) {
        return false;
      }
      for (const NamedExpression& si : sq.Query()->SelectList()) {
        if (!check_sq_cols(si.expression)) {
          return false;
        }
      }
      return true;
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      return cn.schema.empty() ||
             local_aliases.contains(LoweredStarName(cn.schema));
    }
    return std::ranges::all_of(ExpressionChildren(e), [&](const Expression& c) {
      return self(self, c);
    });
  };
  if (!expr_self_contained(expr_self_contained, q.WhereClause()) ||
      !expr_self_contained(expr_self_contained, q.Having()) ||
      !expr_self_contained(expr_self_contained, q.Qualify())) {
    return false;
  }
  for (const NamedExpression& item : q.SelectList()) {
    if (!expr_self_contained(expr_self_contained, item.expression)) {
      return false;
    }
  }
  for (const Expression& g : q.GroupBy()) {
    if (!expr_self_contained(expr_self_contained, g)) {
      return false;
    }
  }
  return true;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (shared subquery factorization / multiset reflexivity)
bool HoistIdenticalDerivedSubqueriesToCte(SelectStatement* outer) {
  if (outer == nullptr || !outer->UnionAll().empty() ||
      outer->GetSetOperationTree() != nullptr) {
    return false;
  }
  std::unordered_map<std::string, std::vector<SelectSource*>> groups;
  std::vector<std::string> fp_order;
  auto collect = [&](auto& self, SelectStatement* cur, int depth) -> void {
    if (cur == nullptr || depth > 3) {
      return;
    }
    for (SelectSource& src :
         const_cast<std::vector<SelectSource>&>(cur->Sources())) {
      if (src.query == nullptr || src.alias.empty() || src.is_lateral ||
          src.unnest || src.sample || src.from_nested_join ||
          !src.using_columns.empty()) {
        continue;
      }
      if (DerivedQueryIsSelfContainedAndExpensive(*src.query)) {
        const std::string fp = src.query->Fingerprint();
        if (!groups.contains(fp)) {
          fp_order.push_back(fp);
        }
        groups[fp].push_back(&src);
        continue;
      }
      if (src.query->WithQueries().empty()) {
        self(self, src.query.get(), depth + 1);
      }
    }
  };
  collect(collect, outer, 0);

  bool hoisted = false;
  size_t seq = 0;
  for (const std::string& fp : fp_order) {
    std::vector<SelectSource*>& sites = groups[fp];
    if (sites.size() < 2) {
      continue;
    }
    std::string cte_name = "__tpcds_hoisted_sq_" + std::to_string(seq++);
    while (outer->WithQueries().contains(cte_name)) {
      cte_name = "__tpcds_hoisted_sq_" + std::to_string(seq++);
    }
    auto body = std::make_shared<SelectStatement>(*sites[0]->query);
    outer->AddWithQuery(cte_name, std::move(body));
    for (SelectSource* site : sites) {
      site->table = cte_name;
      site->query = nullptr;
    }
    hoisted = true;
  }
  return hoisted;
}

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (disjoint_union_group_by_bypass_sound,
//    agg_union_null_tag_collision_bug)
bool BypassDisjointUnionGroupBy(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() != 1 ||
      select->GroupBy().empty() || select->IsGroupAll() || select->Qualify() ||
      select->Distinct() || select->HasDistinctOn() || select->ForUpdate() ||
      select->AsStruct() || select->WithTies() ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*select) ||
      SelectListHasStarOrModifiers(*select) || TpcdsStmtHasParam(*select)) {
    return false;
  }
  const SelectSource& foo = select->Sources()[0];
  if (foo.query == nullptr || foo.alias.empty() || foo.is_lateral ||
      foo.unnest || foo.sample || foo.from_nested_join ||
      !foo.using_columns.empty() || foo.join_type != JoinType::kCross) {
    return false;
  }

  std::vector<std::shared_ptr<SelectStatement>> branches;
  if (!ExtractPureUnionAllBranches(*foo.query, &branches)) {
    return false;
  }
  const size_t arity = branches[0]->SelectList().size();
  if (arity == 0 || SelectListHasStarOrModifiers(*branches[0])) {
    return false;
  }
  std::vector<std::string> branch0_names;
  std::unordered_map<std::string, size_t> col_to_idx;
  for (size_t c = 0; c < arity; ++c) {
    const std::string nm =
        ExplicitOrColumnOutputName(branches[0]->SelectList()[c]);
    if (nm.empty() || !col_to_idx.emplace(LoweredStarName(nm), c).second) {
      return false;
    }
    branch0_names.push_back(nm);
  }
  for (size_t b = 1; b < branches.size(); ++b) {
    if (branches[b]->SelectList().size() != arity ||
        SelectListHasStarOrModifiers(*branches[b])) {
      return false;
    }
  }

  // Check if at least one outer GroupBy key maps to a column projected as
  // pairwise-distinct safe constants across all branches.
  bool found_disjoint_key = false;
  for (const Expression& gk : select->GroupBy()) {
    if (!gk || gk->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& gc = gk->AsColumnValue().GetColumnName();
    if (!gc.schema.empty() && !IdentifierEquals(gc.schema, foo.alias)) {
      continue;
    }
    const auto it = col_to_idx.find(LoweredStarName(gc.name));
    if (it == col_to_idx.end()) {
      continue;
    }
    const size_t cidx = it->second;
    std::vector<Value> tags;
    bool all_const = true;
    for (const auto& br : branches) {
      const Expression& bexpr = br->SelectList()[cidx].expression;
      if (!bexpr || bexpr->Type() != TypeTag::kConstantValue) {
        all_const = false;
        break;
      }
      const Value& v = bexpr->AsConstantValue().GetValue();
      if (!IsSafeDisjointTagValue(v) ||
          (!tags.empty() && v.type != tags.front().type)) {
        all_const = false;
        break;
      }
      tags.push_back(v);
    }
    if (!all_const) {
      continue;
    }
    bool distinct = true;
    for (size_t a = 0; a < tags.size() && distinct; ++a) {
      for (size_t b = a + 1; b < tags.size(); ++b) {
        if (tags[a] == tags[b]) {
          distinct = false;
          break;
        }
      }
    }
    if (distinct) {
      found_disjoint_key = true;
      break;
    }
  }
  if (!found_disjoint_key) {
    return false;
  }

  // Ensure every outer SelectList item has a deterministic unique output name
  // and every outer OrderBy term binds cleanly to an ordinal or output column
  // for `ExecuteSetOperation`'s post-union merge sort!
  std::vector<std::string> outer_out_names;
  std::unordered_set<std::string> seen_outer_names;
  for (size_t i = 0; i < select->SelectList().size(); ++i) {
    std::string nm = InferSelectItemOutputName(select->SelectList()[i], i);
    if (!seen_outer_names.insert(LoweredStarName(nm)).second) {
      nm = nm + "_" + std::to_string(i);
      seen_outer_names.insert(LoweredStarName(nm));
    }
    outer_out_names.push_back(std::move(nm));
  }

  std::vector<SelectStatement::OrderByTerm> merged_outer_order;
  for (const auto& term : select->OrderBy()) {
    SelectStatement::OrderByTerm mt = term;
    if (!term.expression) {
      return false;
    }
    if (term.expression->Type() == TypeTag::kConstantValue) {
      merged_outer_order.push_back(std::move(mt));
      continue;
    }
    std::optional<size_t> matched_out;
    for (size_t i = 0; i < select->SelectList().size(); ++i) {
      const NamedExpression& si = select->SelectList()[i];
      if (term.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& oc = term.expression->AsColumnValue().GetColumnName();
        if (oc.schema.empty() &&
            IdentifierEquals(outer_out_names[i], oc.name)) {
          matched_out = i;
          break;
        }
        if (si.expression && si.expression->Type() == TypeTag::kColumnValue) {
          const ColumnName& sc = si.expression->AsColumnValue().GetColumnName();
          if ((oc.schema.empty() || IdentifierEquals(oc.schema, foo.alias)) &&
              (sc.schema.empty() || IdentifierEquals(sc.schema, foo.alias)) &&
              IdentifierEquals(oc.name, sc.name)) {
            matched_out = i;
            break;
          }
        }
      }
      if (si.expression &&
          si.expression->ToString() == term.expression->ToString()) {
        matched_out = i;
        break;
      }
    }
    if (!matched_out.has_value()) {
      return false;
    }
    mt.expression =
        ColumnValueExp(ColumnName("", outer_out_names[*matched_out]));
    merged_outer_order.push_back(std::move(mt));
  }

  const bool has_outer_limit = select->HasLimit();
  const size_t outer_limit = select->Limit();
  const size_t outer_offset = select->Offset();
  const std::optional<size_t> branch_limit =
      has_outer_limit ? std::optional<size_t>(outer_limit + outer_offset)
                      : std::nullopt;

  std::vector<std::shared_ptr<SelectStatement>> flat_branches;
  flat_branches.reserve(branches.size());
  for (auto & branche : branches) {
    auto aligned_b = std::make_shared<SelectStatement>(*branche);
    std::vector<NamedExpression> b_sel = aligned_b->SelectList();
    for (size_t c = 0; c < arity; ++c) {
      b_sel[c].name = branch0_names[c];
    }
    aligned_b->SetSelectList(std::move(b_sel));

    auto b_outer = std::make_shared<SelectStatement>(*select);
    std::vector<NamedExpression> named_sel = b_outer->SelectList();
    for (size_t i = 0; i < named_sel.size(); ++i) {
      named_sel[i].name = outer_out_names[i];
    }
    b_outer->SetSelectList(std::move(named_sel));
    if (!has_outer_limit) {
      b_outer->SetOrderBy({});
    }
    b_outer->SetLimit(branch_limit);
    b_outer->SetOffset(0);
    std::vector<SelectSource> b_srcs = b_outer->Sources();
    b_srcs[0].query = std::move(aligned_b);
    b_outer->SetSources(std::move(b_srcs));

    if (!FlattenGroupedDerivedJoinCore(b_outer.get())) {
      return false;
    }

    // Strip constant expressions from `b_outer->GroupBy()` as long as at least
    // one non-constant grouping key remains.
    std::vector<Expression> non_const_group;
    for (const Expression& g : b_outer->GroupBy()) {
      if (g && g->Type() != TypeTag::kConstantValue) {
        non_const_group.push_back(g);
      }
    }
    if (!non_const_group.empty() &&
        non_const_group.size() < b_outer->GroupBy().size()) {
      b_outer->SetGroupBy(std::move(non_const_group));
    }
    flat_branches.push_back(std::move(b_outer));
  }

  bool disjoint_ordered_by_head = false;
  if (has_outer_limit && !merged_outer_order.empty()) {
    const Expression& lead_ord = merged_outer_order[0].expression;
    std::optional<size_t> lead_idx;
    if (lead_ord && lead_ord->Type() == TypeTag::kConstantValue) {
      const Value& ov = lead_ord->AsConstantValue().GetValue();
      if (!ov.IsNull() && ov.type == ValueType::kInt64 &&
          ov.value.int_value >= 1 &&
          static_cast<size_t>(ov.value.int_value) <= outer_out_names.size()) {
        lead_idx = static_cast<size_t>(ov.value.int_value) - 1;
      }
    } else if (lead_ord && lead_ord->Type() == TypeTag::kColumnValue) {
      const ColumnName& oc = lead_ord->AsColumnValue().GetColumnName();
      for (size_t i = 0; i < outer_out_names.size(); ++i) {
        if (oc.schema.empty() &&
            IdentifierEquals(outer_out_names[i], oc.name)) {
          lead_idx = i;
          break;
        }
      }
    }
    if (lead_idx.has_value()) {
      std::vector<std::pair<Value, std::shared_ptr<SelectStatement>>> keyed;
      bool all_lead_const = true;
      for (const auto& fb : flat_branches) {
        const Expression& e = fb->SelectList()[*lead_idx].expression;
        if (!e || e->Type() != TypeTag::kConstantValue ||
            TpcdsExprHasParam(e)) {
          all_lead_const = false;
          break;
        }
        const Value& v = e->AsConstantValue().GetValue();
        if (!IsSafeDisjointTagValue(v) ||
            (!keyed.empty() && v.type != keyed.front().first.type)) {
          all_lead_const = false;
          break;
        }
        keyed.emplace_back(v, fb);
      }
      if (all_lead_const) {
        bool distinct = true;
        for (size_t a = 0; a < keyed.size() && distinct; ++a) {
          for (size_t b = a + 1; b < keyed.size(); ++b) {
            if (keyed[a].first == keyed[b].first) {
              distinct = false;
              break;
            }
          }
        }
        if (distinct) {
          const bool asc = merged_outer_order[0].ascending;
          std::ranges::sort(keyed, [&](const auto& lhs, const auto& rhs) {
            return asc ? (lhs.first < rhs.first) : (rhs.first < lhs.first);
          });
          for (size_t i = 0; i < keyed.size(); ++i) {
            flat_branches[i] = std::move(keyed[i].second);
          }
          disjoint_ordered_by_head = true;
        }
      }
    }
  }

  auto tree = std::make_shared<SetOperationTree>();
  tree->grouped = true;
  tree->disjoint_ordered_by_head = disjoint_ordered_by_head;
  tree->first = flat_branches[0];
  for (size_t b = 1; b < flat_branches.size(); ++b) {
    tree->branches.push_back(flat_branches[b]);
    tree->kinds.push_back(SetOperationKind::kUnionAll);
  }

  *select = *flat_branches[0];
  select->ClearUnionAll();
  for (size_t b = 1; b < flat_branches.size(); ++b) {
    select->AddUnionAll(flat_branches[b]);
  }
  select->SetSetOperationTree(std::move(tree));
  select->SetOrderBy(std::move(merged_outer_order));
  select->SetLimit(has_outer_limit ? std::optional<size_t>(outer_limit)
                                   : std::nullopt);
  select->SetOffset(outer_offset);
  return true;
}

// Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
// (group_by_functional_expr_reduction_sound)
bool PruneRedundantGroupByKeys(SelectStatement* select) {
  if (select == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    if (!select->IsRecursiveWith(name) && body != nullptr) {
      changed |= PruneRedundantGroupByKeys(body.get());
    }
  }
  for (const SelectSource& src : select->Sources()) {
    if (src.query != nullptr) {
      changed |= PruneRedundantGroupByKeys(src.query.get());
    }
  }
  for (const auto& branch : select->UnionAll()) {
    if (branch != nullptr) {
      changed |= PruneRedundantGroupByKeys(branch.get());
    }
  }
  if (select->IsGroupAll() || select->GroupBy().size() < 2 ||
      select->HasDistinctOn() || select->WithTies()) {
    return changed;
  }

  std::vector<Expression> reduced;
  reduced.reserve(select->GroupBy().size());
  std::unordered_set<std::string> seen_deterministic;
  for (const Expression& key : select->GroupBy()) {
    if (!key) {
      return changed;
    }
    if (key->Type() == TypeTag::kConstantValue) {
      continue;
    }
    if (!ContainsQueryExpression(key) && !ContainsParameterSlot(key) &&
        DerivedExpressionIsImmutable(key)) {
      const std::string repr = key->ToString();
      if (!seen_deterministic.insert(repr).second) {
        continue;
      }
    }
    reduced.push_back(key);
  }
  // Never reduce an all-constant GROUP BY (`SELECT COUNT(*) FROM empty GROUP BY
  // 1` emits 0 rows whereas keyless `SELECT COUNT(*) FROM empty` emits 1 row).
  if (!reduced.empty() && reduced.size() < select->GroupBy().size()) {
    select->SetGroupBy(std::move(reduced));
    changed = true;
  }
  return changed;
}

}  // namespace tinylamb
