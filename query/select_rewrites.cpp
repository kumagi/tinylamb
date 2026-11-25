/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/select_rewrites.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/process.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/relational.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
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
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Defined in a later anonymous-namespace block of this translation unit.
std::string LoweredStarName(std::string_view text);

// Stack-buffer decimal formatting for repeated signature/name construction in
// the rewrite loops (avoids one std::string allocation per call).
template <typename T>
std::string IntegerToName(T value) {
  std::array<char, 32> buffer{};
  const auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (ec == std::errc{}) {
    return std::string(buffer.data(), ptr);
  }
  return std::to_string(value);
}

// IdentifierEquals comes from common/string_util.hpp (case-insensitive ASCII
// identifier equality); the former file-local copy was removed as a duplicate.

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
  local.reserve(source.query->Sources().size() * 2);
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
    names.reserve(outer->WithQueries().size());
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
  names.reserve(outer->WithQueries().size());
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    names.insert(name);
  }
  std::unordered_map<std::string, size_t> counts;
  CountCteReferencesInStatement(*outer, names, &counts);
  std::vector<std::string> inlined;
  inlined.reserve(names.size());
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
  consumers.reserve(1 + outer->WithQueries().size());
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
// Folds simple signed-int64 constant addition/subtraction (e.g. `1 + 1` -> `2`)
// inside consumer restrictions so pushed CTE disjunctions match table stats and
// integer zone-map/peek filters.
Expression FoldSimpleIntArith(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const auto& bin = expr->AsBinaryExpression();
    Expression lhs = FoldSimpleIntArith(bin.Left());
    Expression rhs = FoldSimpleIntArith(bin.Right());
    if (lhs && rhs && lhs->Type() == TypeTag::kConstantValue &&
        rhs->Type() == TypeTag::kConstantValue) {
      const Value& lv = lhs->AsConstantValue().GetValue();
      const Value& rv = rhs->AsConstantValue().GetValue();
      if (lv.type == ValueType::kInt64 && rv.type == ValueType::kInt64) {
        int64_t out = 0;
        if (bin.Op() == BinaryOperation::kAdd &&
            !__builtin_add_overflow(lv.value.int_value, rv.value.int_value,
                                    &out)) {
          return ConstantValueExp(Value(out));
        }
        if (bin.Op() == BinaryOperation::kSubtract &&
            !__builtin_sub_overflow(lv.value.int_value, rv.value.int_value,
                                    &out)) {
          return ConstantValueExp(Value(out));
        }
      }
    }
    if (lhs.get() != bin.Left().get() || rhs.get() != bin.Right().get()) {
      return BinaryExpressionExp(std::move(lhs), bin.Op(), std::move(rhs));
    }
  }
  return expr;
}

bool CteBodyTakesRestriction(SelectStatement& body,
                             std::vector<SelectStatement*>* targets_out,
                             std::unordered_set<std::string>* outputs,
                             std::vector<std::string>* target_tables_out) {
  // GROUP BY bodies are allowed when the pushed predicate names only grouping
  // keys (see the group_keys filter below): the disjunction is a WHERE on the
  // base relation, so aggregate outputs do not exist yet at that point.
  // Also allow:
  //   (1) a projection/filter wrapper over a single inner-join GROUP BY
  //       derived table (TPC-DS Q39 `inv`/`foo`), and
  //   (2) a GROUP BY over a UNION [ALL|DISTINCT] derived table whose branches
  //       each expose the grouping key from an inner-joined base table
  //       (TPC-DS Q75 `all_sales`).
  // Lean proofs:
  //   lean/TinylambOpt/TpcdsNextDiscovery.lean
  //     (shared_cte_disjunct_pushdown_sound)
  //   lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
  //     (CteVarianceCoeffSelfJoinPivot_sound)
  //   lean/TinylambOpt/ResearchProofs/IndividualQueryGaps.lean
  //     (LeftJoinRightSideKeyFilterPushdown_sound)
  if (body.Sources().size() != 1 || body.Having() || body.Distinct() ||
      !body.DistinctOn().empty() || body.HasLimit() || body.Offset() != 0 ||
      body.Qualify() || !body.UnionAll().empty() ||
      body.GetSetOperationTree() != nullptr || !body.WithQueries().empty() ||
      relational_detail::HasWindowFunctions(body)) {
    return false;
  }
  const SelectSource& source = body.Sources().front();
  if (source.unnest || source.is_lateral || source.sample.has_value() ||
      !source.using_columns.empty() || source.from_nested_join ||
      source.join_condition) {
    return false;
  }
  if (source.query != nullptr) {
    SelectStatement& inner = *source.query;
    // Case (2): GROUP BY over a UNION [ALL|DISTINCT] derived table (Q75).
    if (!body.GroupBy().empty() &&
        (!inner.UnionAll().empty() || inner.GetSetOperationTree() != nullptr)) {
      std::vector<SelectStatement*> branches;
      if (const auto& tree = inner.GetSetOperationTree(); tree != nullptr) {
        if (!tree->first || tree->branches.empty() ||
            tree->kinds.size() != tree->branches.size()) {
          return false;
        }
        for (SetOperationKind k : tree->kinds) {
          if (k != SetOperationKind::kUnionAll &&
              k != SetOperationKind::kUnion) {
            return false;
          }
        }
        branches.reserve(1 + tree->branches.size());
        branches.push_back(tree->first.get());
        for (const auto& br : tree->branches) {
          if (!br) {
            return false;
          }
          branches.push_back(br.get());
        }
      } else {
        if (inner.SetOperationKinds().size() != inner.UnionAll().size()) {
          return false;
        }
        for (SetOperationKind k : inner.SetOperationKinds()) {
          if (k != SetOperationKind::kUnionAll &&
              k != SetOperationKind::kUnion) {
            return false;
          }
        }
        branches.reserve(1 + inner.UnionAll().size());
        branches.push_back(&inner);
        for (const auto& br : inner.UnionAll()) {
          if (!br) {
            return false;
          }
          branches.push_back(br.get());
        }
      }
      std::unordered_set<std::string> body_gkeys;
      for (const Expression& key : body.GroupBy()) {
        if (!key || key->Type() != TypeTag::kColumnValue) {
          return false;
        }
        body_gkeys.insert(
            LoweredStarName(key->AsColumnValue().GetColumnName().name));
      }
      const SelectStatement& head_branch = *branches.front();
      const size_t ncols = head_branch.SelectList().size();
      if (ncols == 0) {
        return false;
      }
      std::vector<std::string> shared_inner_tables;
      std::unordered_set<std::string> branch_passthrough;
      for (size_t bi = 0; bi < branches.size(); ++bi) {
        SelectStatement* br = branches[bi];
        if (br->Sources().empty() || !br->GroupBy().empty() ||
            br->IsGroupAll() || br->Having() || br->Distinct() ||
            !br->DistinctOn().empty() || br->HasLimit() || br->Offset() != 0 ||
            br->Qualify() || !br->WithQueries().empty() ||
            br->SelectList().size() != ncols ||
            relational_detail::HasWindowFunctions(*br)) {
          return false;
        }
        std::vector<std::string> br_inner_tables;
        for (size_t si = 0; si < br->Sources().size(); ++si) {
          const SelectSource& isrc = br->Sources()[si];
          if (isrc.query != nullptr || isrc.unnest || isrc.is_lateral ||
              isrc.table.empty() || isrc.sample.has_value() ||
              !isrc.using_columns.empty() || isrc.from_nested_join) {
            return false;
          }
          if (si == 0 || isrc.join_type == JoinType::kInner ||
              isrc.join_type == JoinType::kCross) {
            br_inner_tables.push_back(isrc.table);
          } else if (isrc.join_type != JoinType::kLeft) {
            return false;
          }
        }
        if (bi == 0) {
          shared_inner_tables = std::move(br_inner_tables);
        } else {
          // Keep only inner-joined base tables present in every branch (e.g.
          // `date_dim`, `item`).
          std::erase_if(shared_inner_tables, [&](const std::string& t) {
            return std::ranges::none_of(
                br_inner_tables, [&](const std::string& u) {
                  return LoweredStarName(u) == LoweredStarName(t);
                });
          });
        }
        std::unordered_set<std::string> br_cols;
        for (size_t ci = 0; ci < ncols; ++ci) {
          const NamedExpression& it = br->SelectList()[ci];
          if (!it.expression ||
              it.expression->Type() != TypeTag::kColumnValue) {
            continue;
          }
          const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
          const NamedExpression& head_it = head_branch.SelectList()[ci];
          const std::string head_name =
              !head_it.name.empty()
                  ? head_it.name
                  : (head_it.expression &&
                             head_it.expression->Type() == TypeTag::kColumnValue
                         ? head_it.expression->AsColumnValue()
                               .GetColumnName()
                               .name
                         : std::string());
          if (!head_name.empty() && cn.schema.empty() &&
              LoweredStarName(cn.name) == LoweredStarName(head_name)) {
            br_cols.insert(head_name);
          }
        }
        if (bi == 0) {
          branch_passthrough = std::move(br_cols);
        } else {
          std::erase_if(branch_passthrough, [&](const std::string& c) {
            return !br_cols.contains(c);
          });
        }
      }
      if (shared_inner_tables.empty() || branch_passthrough.empty()) {
        return false;
      }
      for (const NamedExpression& item : body.SelectList()) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& col =
            item.expression->AsColumnValue().GetColumnName();
        if (!col.schema.empty() && col.schema != source.alias) {
          continue;
        }
        if (col.name == item.name &&
            body_gkeys.contains(LoweredStarName(col.name)) &&
            branch_passthrough.contains(col.name)) {
          outputs->insert(item.name);
        }
      }
      if (outputs->empty()) {
        return false;
      }
      *targets_out = std::move(branches);
      *target_tables_out = std::move(shared_inner_tables);
      return true;
    }

    if (!body.GroupBy().empty()) {
      return false;
    }
    if (inner.Sources().empty() || inner.GroupBy().empty() || inner.Having() ||
        inner.Distinct() || !inner.DistinctOn().empty() || inner.HasLimit() ||
        inner.Offset() != 0 || inner.Qualify() || !inner.UnionAll().empty() ||
        inner.GetSetOperationTree() != nullptr ||
        !inner.WithQueries().empty() ||
        relational_detail::HasWindowFunctions(inner)) {
      return false;
    }
    std::vector<std::string> tables;
    tables.reserve(inner.Sources().size());
    for (const SelectSource& isrc : inner.Sources()) {
      if (isrc.query != nullptr || isrc.unnest || isrc.is_lateral ||
          isrc.table.empty() || isrc.sample.has_value() ||
          !isrc.using_columns.empty() || isrc.from_nested_join ||
          (isrc.join_type != JoinType::kInner &&
           isrc.join_type != JoinType::kCross)) {
        return false;
      }
      tables.push_back(isrc.table);
    }
    std::unordered_set<std::string> group_keys;
    for (const Expression& key : inner.GroupBy()) {
      if (!key || ContainsQueryExpression(key)) {
        return false;
      }
      if (key->Type() == TypeTag::kColumnValue) {
        group_keys.insert(
            LoweredStarName(key->AsColumnValue().GetColumnName().name));
      }
    }
    if (group_keys.empty()) {
      return false;
    }
    std::unordered_set<std::string> inner_outputs;
    for (const NamedExpression& item : inner.SelectList()) {
      if (item.expression == nullptr ||
          item.expression->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& col = item.expression->AsColumnValue().GetColumnName();
      if (col.name == item.name &&
          group_keys.contains(LoweredStarName(col.name))) {
        inner_outputs.insert(item.name);
      }
    }
    for (const NamedExpression& item : body.SelectList()) {
      if (item.expression == nullptr ||
          item.expression->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& col = item.expression->AsColumnValue().GetColumnName();
      if (!col.schema.empty() && col.schema != source.alias) {
        continue;
      }
      if (col.name == item.name && inner_outputs.contains(col.name)) {
        outputs->insert(item.name);
      }
    }
    if (outputs->empty()) {
      return false;
    }
    targets_out->push_back(&inner);
    *target_tables_out = std::move(tables);
    return true;
  }
  if (source.table.empty()) {
    return false;
  }
  std::unordered_set<std::string> group_keys;
  if (!body.GroupBy().empty()) {
    for (const Expression& key : body.GroupBy()) {
      if (!key || ContainsQueryExpression(key)) {
        return false;
      }
      for (const ColumnName& col : key->TouchedColumns()) {
        group_keys.insert(LoweredStarName(col.name));
      }
    }
  }
  for (const NamedExpression& item : body.SelectList()) {
    if (item.expression == nullptr ||
        item.expression->Type() != TypeTag::kColumnValue) {
      // An aggregate output cannot be named by a pushed WHERE; skip it rather
      // than rejecting the whole body (other grouping-key outputs may remain).
      if (!body.GroupBy().empty()) {
        continue;
      }
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
    if (!group_keys.empty() &&
        !group_keys.contains(LoweredStarName(column.name))) {
      continue;
    }
    outputs->insert(item.name);
  }
  if (outputs->empty()) {
    return false;
  }
  targets_out->push_back(&body);
  target_tables_out->push_back(source.table);
  return true;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
// (shared_cte_disjunct_pushdown_sound, cte_materialize_halves_body_evaluations,
//  cte_volatile_inline_unsound — the body must not be inlined twice when
//  volatile calls are present)
// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
// (CteVarianceCoeffSelfJoinPivot_sound)
size_t PushSharedCteRestrictions(SelectStatement* outer,
                                 TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty()) {
    return 0;
  }
  std::vector<std::string> entries;
  entries.reserve(outer->WithQueries().size());
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
    std::vector<SelectStatement*> targets;
    std::unordered_set<std::string> outputs;
    std::vector<std::string> target_tables;
    if (!CteBodyTakesRestriction(body, &targets, &outputs, &target_tables) ||
        targets.empty() || target_tables.empty()) {
      continue;
    }
    std::vector<SharedCteSite> sites;
    if (!CollectSharedCteSites(outer, name, &sites) || sites.size() < 2) {
      continue;
    }
    std::vector<Expression> restrictions;
    restrictions.reserve(sites.size());
    bool every_site_restricts = true;
    for (const SharedCteSite& site : sites) {
      std::vector<Expression> local;
      const auto conjuncts = SplitConjuncts(site.consumer->WhereClause());
      local.reserve(conjuncts.size());
      for (const Expression& conjunct : conjuncts) {
        if (!conjunct || !ReferencesOnly(conjunct, {site.alias})) {
          continue;
        }
        // TouchedColumns / StripCteQualifier cannot see inside a subquery,
        // so a correlated reference to another FROM source would be
        // re-scoped into the CTE body. Leave such conjuncts at the consumer.
        if (ContainsQueryExpression(conjunct)) {
          continue;
        }
        Expression stripped =
            FoldSimpleIntArith(StripCteQualifier(conjunct, site.alias));
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
    std::string chosen_table;
    if (target_tables.size() == 1) {
      chosen_table = target_tables.front();
    } else {
      const auto touched = disjunction->TouchedColumns();
      if (touched.empty()) {
        continue;
      }
      size_t matching_tables = 0;
      for (const std::string& tname : target_tables) {
        const StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(tname);
        if (!t.HasValue()) {
          matching_tables = 0;
          break;
        }
        const Schema& sch = t.Value()->GetSchema();
        bool has_any = false;
        bool has_all = true;
        for (const ColumnName& col : touched) {
          if (sch.Offset(col) >= 0) {
            has_any = true;
          } else {
            has_all = false;
          }
        }
        if (has_any) {
          if (!has_all) {
            matching_tables = 2;
            break;
          }
          chosen_table = tname;
          ++matching_tables;
        }
      }
      if (matching_tables != 1) {
        continue;
      }
    }
    const StatusOr<std::shared_ptr<Table>> found_table =
        ctx.GetTable(chosen_table);
    const StatusOr<std::shared_ptr<TableStatistics>> found_stats =
        ctx.GetStats(chosen_table);
    if (!found_table.HasValue()) {
      continue;
    }
    if (found_stats.HasValue() && found_stats.Value()->Rows() > 0) {
      const double selectivity = found_stats.Value()->EstimateSelectivity(
          found_table.Value()->GetSchema(), disjunction);
      if (!(selectivity < 1.0)) {
        continue;
      }
    } else if (targets.size() <= 1) {
      continue;
    }
    for (SelectStatement* target : targets) {
      std::vector<Expression> conjuncts;
      conjuncts.reserve(2);
      if (target->WhereClause()) {
        conjuncts.push_back(target->WhereClause());
      }
      conjuncts.push_back(disjunction);
      target->SetWhereClause(CombineConjuncts(conjuncts));
    }
    ++pushed;
  }
  return pushed;
}

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
  // A query expression is opaque here: ExpressionChildren exposes only its
  // Test() leg, so a `d.col` reference inside the subquery would survive
  // substitution and silently rebind once the derived alias is dropped.
  if (expression->Type() == TypeTag::kQueryExp) {
    return std::nullopt;
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
    const auto found = scope.outputs.find(lowered);
    if (found == scope.outputs.end()) {
      return expression;
    }
    if (base_column_names.contains(lowered)) {
      return std::nullopt;
    }
    return found->second;
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
    // The rebind helper returns the original shared_ptr when nothing changed;
    // comparing pointers avoids re-serializing the whole subtree per node.
    changed |= (rebound.get() != child.get());
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
  base_column_names.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    base_column_names.insert(LoweredStarName(schema.GetColumn(i).Name().name));
  }
  const std::string lowered_relation = LoweredStarName(relation);
  std::unordered_set<std::string> except;
  except.reserve(modifiers.except_columns.size());
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
  replace.reserve(modifiers.replace_items.size());
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
    // Parameter slots masquerade as constants after slotization: their value
    // comes from the prepared statement's parameters and can change on replay,
    // so a "disjoint" proof from such a literal would silently break UNION ALL
    // when a later binding makes the branches overlap.
    if (ContainsParameterSlot(part->WhereClause())) {
      return false;
    }
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
  std::vector<SelectStatement*> parts;
  parts.reserve(outer->UnionAll().size() + 1);
  parts.push_back(outer);
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
  // ignoring the ordering and any LIMIT). Validate the ordinals here, after
  // every other bail-out, but bind them only after star expansion: binding
  // against a literal `*` head item first would leave a `*` sort key in the
  // merged plain SELECT after its select list has been expanded to real
  // columns.
  std::vector<bool> is_ordinal(outer->OrderBy().size(), false);
  for (size_t i = 0; i < outer->OrderBy().size(); ++i) {
    const Expression& term = outer->OrderBy()[i].expression;
    if (!term || term->Type() != TypeTag::kConstantValue) {
      continue;
    }
    const Value& value = term->AsConstantValue().GetValue();
    if (value.type != ValueType::kInt64 || value.IsNull()) {
      continue;
    }
    const int64_t ordinal = value.value.int_value;
    // Do not check the upper bound against head_list: it is the pre-expansion
    // list, so `SELECT t.*` has width 1 even though ORDER BY 2 is valid once
    // the star expands. The post-expansion check below is authoritative for
    // both widening and narrowing (EXCEPT/REPLACE) stars.
    if (ordinal < 1) {
      return false;
    }
    is_ordinal[i] = true;
  }
  // Expand branch stars only after every bail-out, so a rejected merge
  // leaves the statement untouched.
  for (SelectStatement* part : parts) {
    if (!ExpandUnionBranchStars(part, ctx)) {
      return false;
    }
  }
  if (std::ranges::any_of(is_ordinal, [](bool flag) { return flag; })) {
    const std::vector<NamedExpression>& expanded_head = outer->SelectList();
    std::vector<SelectStatement::OrderByTerm> rebound_order;
    rebound_order.reserve(outer->OrderBy().size());
    for (size_t i = 0; i < outer->OrderBy().size(); ++i) {
      const SelectStatement::OrderByTerm& term = outer->OrderBy()[i];
      if (!is_ordinal[i]) {
        rebound_order.push_back(term);
        continue;
      }
      const Value& value = term.expression->AsConstantValue().GetValue();
      const auto ordinal = static_cast<size_t>(value.value.int_value);
      // Re-read the live head list: a star (with optional EXCEPT/REPLACE
      // modifiers) may have changed both the width and the item expressions.
      if (ordinal > expanded_head.size() ||
          !expanded_head[ordinal - 1].expression ||
          ContainsStarReference(expanded_head[ordinal - 1].expression)) {
        return false;
      }
      rebound_order.push_back({expanded_head[ordinal - 1].expression,
                               term.ascending, term.nulls_first});
    }
    outer->SetOrderBy(std::move(rebound_order));
  }
  outer->SetWhereClause(std::move(merged));
  outer->ClearUnionAll();
  // Drop the (now stale) set-operation tree alongside the flat list: any
  // downstream reader, including relational fallbacks, must see the merged
  // single-query shape only.
  outer->SetSetOperationTree(nullptr);
  // P388: the union head flags describe the flattened-away operation too;
  // reset them so `UnionDistinct()`/`UnionByName()` readers (static schema
  // resolution, LIMIT-cap gates, plan-cache fingerprints) see the merged
  // single-query shape only.  The DISTINCT semantics move to SetDistinct.
  outer->ClearUnionDistinct();
  if (all_distinct) {
    outer->SetDistinct(true);
  }
  return true;
}

namespace {
// LATERAL decorrelation, minimal slice: a LATERAL site
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
  // A row-level `FILTER (WHERE p)` is a per-aggregate modifier, not the
  // member's WHERE clause. The fusion below would drop it when building the
  // CASE-wrapped aggregate, silently changing SUM(x WHERE p) into SUM(x), so
  // such aggregates must stay unfused (both in SELECT and inside HAVING).
  if (agg.Having() != AggregateHavingModifier::kNone ||
      agg.HavingCondition() != nullptr || agg.HasWhereFilter() ||
      !agg.InnerOrderBy()
           .empty() ||  // ORDER BY changes the old projection shape
      agg.InnerLimit().has_value() ||
      !agg.TrailingArgs().empty() || agg.SecondaryArg() != nullptr ||
      // A CASE-wrapped aggregate sees synthetic NULLs on the false branch,
      // which changes what IGNORE NULLS means for the fused payload.
      agg.IgnoreNulls()) {
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
  // FusableAggregateShape rejects every per-aggregate modifier that cannot
  // survive the CASE wrap, including IGNORE NULLS: the ELSE NULL rows would
  // change its meaning. Consequently the fused aggregate is always RESPECT
  // NULLS here and carries no modifier other than DISTINCT.
  return std::make_shared<AggregateExpression>(
      agg.GetType(), CaseExpressionExp({{where, branch}}, nullptr),
      agg.Distinct());
}

}  // namespace

size_t FuseSiblingDerivedAggregates(SelectStatement* outer) {
  const bool fusion_disabled = [] {
    const char* hatch =
        ::tinylamb::platform::GetEnv("TINYLAMB_FUSE_SIBLING_AGG");
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
    signature.reserve(body.GroupBy().size());
    signature.push_back('\x1f');
    signature += body.Sources()[0].alias;
    signature.push_back('\x1f');
    // Length-prefix keys: an unescaped 0x1f inside a quoted identifier or
    // string literal can otherwise shift the boundaries and make distinct
    // GROUP BY key sets collide (silent wrong aggregation).
    for (const Expression& key : body.GroupBy()) {
      const std::string text = key->ToString();
      signature += std::to_string(text.size());
      signature.push_back(':');
      signature += text;
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
    // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer here
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
          "__fw" + IntegerToName(member.index),
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
      const std::string fw_name = "__fw" + IntegerToName(member.index);
      appended.push_back(BinaryExpressionExp(
          ColumnValueExp(ColumnName(fused_alias, fw_name)),
          BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t(0)))));
      append_inner_having(BinaryExpressionExp(
          ColumnValueExp(ColumnName("", fw_name)),
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
  rebound_member_joins.reserve(members.size());
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
  rebound_select.reserve(outer->SelectList().size());
  for (const NamedExpression& item : outer->SelectList()) {
    std::optional<Expression> rebound = rebind_outer(item.expression);
    if (!rebound.has_value()) {
      return 0;
    }
    rebound_select.push_back(std::move(*rebound));
  }
  std::vector<Expression> rebound_group;
  rebound_group.reserve(outer->GroupBy().size());
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
  rebound_distinct_on.reserve(outer->DistinctOn().size());
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
  // WITH TIES compares the ORDER BY keys again on projected rows. Expanding
  // a derived column there can introduce input columns absent from those rows.
  if (outer->WithTies()) {
    return changed;
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
    const char* hatch =
        ::tinylamb::platform::GetEnv("TINYLAMB_SCALAR_FN_CASCADES");
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
    // A query expression is opaque to ExpressionChildren (only its Test()
    // leg is exposed): a `d.col` reference inside the subquery would
    // survive substitution and silently rebind to a spliced base column.
    if (expression->Type() == TypeTag::kQueryExp) {
      return std::nullopt;
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
  // DISTINCT ON resolves in the projection scope too: a reference to the
  // derived output must be substituted before the derived source is spliced
  // away, or it would bind to an unrelated base column (or nowhere).
  std::vector<Expression> new_distinct_on;
  new_distinct_on.reserve(outer->DistinctOn().size());
  for (const Expression& key : outer->DistinctOn()) {
    std::optional<Expression> mapped = substitute(key);
    if (!mapped.has_value()) {
      return false;
    }
    new_distinct_on.push_back(std::move(*mapped));
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
  if (!new_distinct_on.empty()) {
    outer->SetDistinctOn(std::move(new_distinct_on));
  }
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
  // A subquery inside the inner WHERE / select list carries its own scope.
  // Rebinding it here (RebindInnerExpression on the WHERE below) would
  // silently re-point a correlated reference at an outer table of the same
  // name. Keep such queries unflattened, matching the sister path's gate.
  if (ContainsQueryExpression(inner->WhereClause()) ||
      std::ranges::any_of(inner->SelectList(), [](const NamedExpression& item) {
        return ContainsQueryExpression(item.expression);
      })) {
    return false;
  }
  // A lateral sibling's inner query is another nested scope that can
  // reference the derived alias (`d.col`, or a bare name resolving
  // outward): SubstituteDerivedOutputs never descends into it, so the
  // reference would silently rebind to a base column after flattening.
  // (derived.is_lateral itself is rejected by the shape gate above.)
  if (std::ranges::any_of(sources, [](const SelectSource& source) {
        return source.is_lateral;
      })) {
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
  base_column_names.reserve(sources.size());
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
    // An UNNEST argument is an outer-scope expression too: e.g.
    // `UNNEST(d.items)` must be scanned for removed-alias references, or
    // flattening silently rebinds `d.items` to a base-table column.
    if (source.unnest) {
      outer_exprs.push_back(source.unnest);
    }
  }
  for (const Expression& expression : outer_exprs) {
    // A kQueryExp is opaque to ExpressionChildren (only its Test() leg is
    // exposed), so neither the scans below nor SubstituteDerivedOutputs can
    // see a `d.col` reference inside a scalar/EXISTS/IN subquery:
    // flattening would drop the `d` alias and silently rebind that
    // reference to a base column. Keep such queries unflattened, matching
    // the inner-side subquery gate above.
    if (ContainsQueryExpression(expression)) {
      return false;
    }
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
    // Same kQueryExp opacity as the outer_exprs gate above: QUALIFY is
    // scanned but never substituted, so a subquery hiding a `d.col`
    // reference would survive flattening and silently rebind.
    if (ContainsQueryExpression(outer->Qualify())) {
      return false;
    }
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
    if (source.join_condition) {
      std::optional<Expression> mapped =
          substitute_field(source.join_condition);
      if (!mapped.has_value()) {
        return false;
      }
      source.join_condition = std::move(*mapped);
    }
    if (source.unnest) {
      std::optional<Expression> mapped = substitute_field(source.unnest);
      if (!mapped.has_value()) {
        return false;
      }
      source.unnest = std::move(*mapped);
    }
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

struct RuntimeUniqueColInfo {
  bool unique_non_null{false};
  int64_t min_int{0};
  int64_t max_int{-1};
  size_t row_count{0};
};

struct RuntimeUniqueColCacheKey {
  const Table* table_ptr{nullptr};
  page_id_t first_pid{0};
  uint64_t layout_gen{0};
  slot_t col_slot{0};
  bool operator==(const RuntimeUniqueColCacheKey&) const noexcept = default;
};

struct RuntimeUniqueColCacheKeyHash {
  size_t operator()(const RuntimeUniqueColCacheKey& k) const noexcept {
    auto h =
        reinterpret_cast<uintptr_t>(k.table_ptr);  // NOLINT(modernize-use-auto)
    h ^= k.layout_gen * 0x9e3779b97f4a7c15ULL;
    h ^= (static_cast<uint64_t>(k.first_pid) << 16U) |
         static_cast<uint64_t>(k.col_slot);
    h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    return static_cast<size_t>(h ^ (h >> 31U));
  }
};

// Runtime-verifies on the current transaction snapshot that
// `table_name.col_name` (on a bounded dimension table <= 250,000 rows) contains
// zero NULLs and zero duplicate values.
RuntimeUniqueColInfo InspectRuntimeUniqueNonNullColumn(
    TransactionContext& ctx, const std::string& table_name,
    std::string_view col_name) {
  RuntimeUniqueColInfo info;
  if (table_name.empty() || col_name.empty()) {
    return info;
  }
  StatusOr<std::shared_ptr<Table>> tbl_or = ctx.GetTable(table_name);
  if (!tbl_or.HasValue() || tbl_or.Value() == nullptr) {
    return info;
  }
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  uint64_t est = tbl->BackingColumnarRowCount();
  if (auto st = ctx.GetStats(table_name); st.HasValue()) {
    est = std::max<uint64_t>(est, st.Value()->Rows());
  }
  if (est > 250000) {
    return info;
  }
  const Schema& schema = tbl->GetSchema();
  const int off = schema.Offset(ColumnName("", col_name));
  if (off < 0) {
    return info;
  }
  const auto slot = static_cast<slot_t>(off);
  const Column& col_meta = schema.GetColumn(slot);
  if (col_meta.IsUnsigned() || col_meta.IsInterval() ||
      (col_meta.Type() != ValueType::kInt64 &&
       col_meta.Type() != ValueType::kDate &&
       col_meta.Type() != ValueType::kVarChar)) {
    return info;
  }

  // Process-lifetime memo shared by all threads; the static mutex is exactly
  // the lock that makes it safe. Every access below (lookup and store) holds
  // it, and the two critical sections are disjoint scopes rather than nested,
  // so there is no second mutex and no lock-order cycle here.
  static std::mutex cache_mu;
  static std::unordered_map<RuntimeUniqueColCacheKey, RuntimeUniqueColInfo,
                            RuntimeUniqueColCacheKeyHash>
      cache;
  const bool clean_snapshot =
      tbl->LayoutGeneration() != 0 && !tbl->RowsRewrittenInPlace() &&
      !ctx.txn_.HasAnyPageVersionChains() &&
      (ctx.txn_.IsReadOnly() || ctx.txn_.WriteSetEmpty());
  const RuntimeUniqueColCacheKey ckey{.table_ptr = tbl.get(),
                                      .first_pid = tbl->FirstPageId(),
                                      .layout_gen = tbl->LayoutGeneration(),
                                      .col_slot = slot};
  if (clean_snapshot) {
    std::scoped_lock lk(cache_mu);
    if (auto it = cache.find(ckey); it != cache.end()) {
      return it->second;
    }
  }
  const auto remember_and_return =
      [&](RuntimeUniqueColInfo res) -> RuntimeUniqueColInfo {
    if (clean_snapshot) {
      std::scoped_lock lk(cache_mu);
      if (cache.size() >= 4096) {
        cache.clear();
      }
      cache[ckey] = res;
    }
    return res;
  };

  if (clean_snapshot) {
    if (tbl->BackingColumnarRowCount() > 250000) {
      return remember_and_return(info);
    }
    if (const auto tlcs = tbl->GetBackingTlcsStore();
        tlcs != nullptr &&
        tlcs->LayoutGeneration() == tbl->LayoutGeneration() &&
        tlcs->FirstPageId() == tbl->FirstPageId() &&
        tlcs->HasColumnSynopses() && slot < tlcs->ColumnSynopses().size()) {
      if (tlcs->TotalRows() > 250000) {
        return remember_and_return(info);
      }
      info.row_count = static_cast<size_t>(tlcs->TotalRows());
      if (info.row_count == 0) {
        info.unique_non_null = true;
        return remember_and_return(info);
      }
      const auto& syn = tlcs->ColumnSynopses()[slot];
      if ((col_meta.Type() == ValueType::kInt64 ||
           col_meta.Type() == ValueType::kDate) &&
          syn.is_unique_nonnull) {
        info.min_int = syn.min_int;
        info.max_int = syn.max_int;
        info.unique_non_null = true;
        return remember_and_return(info);
      }
      if (syn.has_null) {
        return remember_and_return(info);
      }
    }
    const std::vector<slot_t> proj_slot{slot};
    std::shared_ptr<const LSTableStore> lst = GetOrBuildLSTableStore(
        ctx.txn_, *tbl, /*allow_lazy_build=*/true, &proj_slot,
        /*allow_block_lazy=*/true);
    if (lst != nullptr && lst->uncommitted_owner_txn == 0 &&
        lst->layout_generation == tbl->LayoutGeneration() &&
        lst->first_pid == tbl->FirstPageId() && lst->HasColumn(slot)) {
      if (lst->total_rows > 250000) {
        return remember_and_return(info);
      }
      info.row_count = static_cast<size_t>(lst->total_rows);
      if (info.row_count == 0) {
        info.unique_non_null = true;
        return remember_and_return(info);
      }
      if ((col_meta.Type() == ValueType::kInt64 ||
           col_meta.Type() == ValueType::kDate) &&
          slot < lst->column_is_unique_nonnull.size() &&
          lst->column_is_unique_nonnull[slot] != 0 &&
          slot < lst->column_min_int.size() &&
          slot < lst->column_max_int.size()) {
        info.min_int = lst->column_min_int[slot];
        info.max_int = lst->column_max_int[slot];
        info.unique_non_null = true;
        return remember_and_return(info);
      }
      if (slot < lst->column_has_null.size() &&
          lst->column_has_null[slot] != 0) {
        return remember_and_return(info);
      }
    }
  }

  SelectStatement probe;
  SelectSource psrc;
  psrc.table = table_name;
  probe.SetSources({std::move(psrc)});
  probe.SetSelectList({NamedExpression(
      "__c", ColumnValueExp(ColumnName("", col_meta.Name().name)))});
  StatusOr<relational_detail::Relation> rel_or =
      relational_detail::ExecuteQuery(ctx, probe, nullptr, {}, nullptr, true);
  if (!rel_or.HasValue() || rel_or.Value().HasSpill()) {
    return info;
  }
  const relational_detail::Relation& rel = rel_or.Value();
  const size_t total = rel.TotalRows();
  if (total > 250000) {
    return remember_and_return(info);
  }
  info.row_count = total;
  if (total == 0) {
    info.unique_non_null = true;
    return remember_and_return(info);
  }

  if (col_meta.Type() == ValueType::kInt64 ||
      col_meta.Type() == ValueType::kDate) {
    int64_t lo = std::numeric_limits<int64_t>::max();
    int64_t hi = std::numeric_limits<int64_t>::min();
    if (rel.columnar != nullptr) {
      for (const relational_detail::ColumnarChunk& ch : rel.columnar->chunks) {
        if (ch.columns.empty() || ch.columns[0].has_null) {
          if (ch.columns.empty()) {
            return info;
          }
          for (size_t r = 0; r < ch.rows; ++r) {
            if (ch.columns[0].IsNull(r)) {
              return remember_and_return(info);
            }
          }
        }
        for (size_t r = 0; r < ch.rows; ++r) {
          const int64_t v = ch.columns[0].IntAt(r);
          lo = std::min(lo, v);
          hi = std::max(hi, v);
        }
      }
      if (hi >= lo &&
          static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) <= 2000000ULL) {
        std::vector<uint8_t> seen(
            static_cast<size_t>(static_cast<uint64_t>(hi) -
                                static_cast<uint64_t>(lo) + 1ULL),
            0);
        for (const relational_detail::ColumnarChunk& ch :
             rel.columnar->chunks) {
          for (size_t r = 0; r < ch.rows; ++r) {
            const auto idx = static_cast<size_t>(
                static_cast<uint64_t>(ch.columns[0].IntAt(r)) -
                static_cast<uint64_t>(lo));
            if (seen[idx] != 0) {
              return remember_and_return(info);
            }
            seen[idx] = 1;
          }
        }
      } else {
        relational_detail::FlatIntKeyMap<uint8_t> seen;
        seen.Reserve(total);
        for (const relational_detail::ColumnarChunk& ch :
             rel.columnar->chunks) {
          for (size_t r = 0; r < ch.rows; ++r) {
            bool emplaced = false;
            uint8_t* slot_ptr =
                seen.TryEmplace(ch.columns[0].IntAt(r), &emplaced);
            if (!emplaced) {
              return remember_and_return(info);
            }
            *slot_ptr = 1;
          }
        }
      }
    } else {
      relational_detail::FlatIntKeyMap<uint8_t> seen;
      seen.Reserve(total);
      for (const Row& row : rel.rows) {
        if (row.Size() == 0 || row[0].IsNull() || row[0].IsUnsigned()) {
          return remember_and_return(info);
        }
        const int64_t v = row[0].value.int_value;
        lo = std::min(lo, v);
        hi = std::max(hi, v);
        bool emplaced = false;
        uint8_t* slot_ptr = seen.TryEmplace(v, &emplaced);
        if (!emplaced) {
          return remember_and_return(info);
        }
        *slot_ptr = 1;
      }
    }
    info.min_int = lo;
    info.max_int = hi;
    info.unique_non_null = true;
    return remember_and_return(info);
  }

  // VARCHAR column
  if (rel.columnar != nullptr && !rel.columnar->chunks.empty() &&
      !rel.columnar->chunks[0].columns.empty()) {
    const LSTableColumnDictionary* first_dict =
        rel.columnar->chunks[0].columns[0].dict;
    bool all_same_dict = first_dict != nullptr && first_dict->bytewise_equality;
    for (const relational_detail::ColumnarChunk& ch : rel.columnar->chunks) {
      if (ch.columns.empty() ||
          ch.columns[0].encoding !=
              relational_detail::ColumnarColumn::Encoding::kDictionary ||
          ch.columns[0].dict != first_dict) {
        all_same_dict = false;
        break;
      }
    }
    if (all_same_dict) {
      const size_t dict_sz = first_dict->StringEntryCount();
      if (dict_sz < total) {
        return remember_and_return(info);
      }
      std::vector<uint8_t> seen(dict_sz, 0);
      for (const relational_detail::ColumnarChunk& ch : rel.columnar->chunks) {
        const relational_detail::ColumnarColumn& cc = ch.columns[0];
        for (size_t r = 0; r < ch.rows; ++r) {
          if (cc.IsNull(r)) {
            return remember_and_return(info);
          }
          const uint32_t code = cc.DictCodeAt(r);
          if (code >= dict_sz || seen[code] != 0) {
            return remember_and_return(info);
          }
          seen[code] = 1;
        }
      }
      info.unique_non_null = true;
      return remember_and_return(info);
    }
  }
  std::unordered_set<std::string> seen_str;
  seen_str.reserve(total);
  if (rel.columnar != nullptr) {
    for (const relational_detail::ColumnarChunk& ch : rel.columnar->chunks) {
      if (ch.columns.empty()) {
        return info;
      }
      const auto& cc = ch.columns[0];
      for (size_t r = 0; r < ch.rows; ++r) {
        if (cc.IsNull(r)) {
          return remember_and_return(info);
        }
        const std::string_view sv = cc.StringAt(r);
        if (!PlainStringComparable(sv) || !seen_str.emplace(sv).second) {
          return remember_and_return(info);
        }
      }
    }
  } else {
    for (const Row& row : rel.rows) {
      if (row.Size() == 0 || row[0].IsNull() ||
          row[0].type != ValueType::kVarChar || row[0].Collation() != 0) {
        return remember_and_return(info);
      }
      const std::string_view sv = row[0].AsStringView();
      if (!PlainStringComparable(sv) || !seen_str.emplace(sv).second) {
        return remember_and_return(info);
      }
    }
  }
  info.unique_non_null = true;
  return remember_and_return(info);
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
  grouped_cols.reserve(select->GroupBy().size());
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
         // NOLINTNEXTLINE(bugprone-branch-clone) distinct intent in each branch
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
      // T4157: TouchedColumns() of a kQueryExp only reports the Test()
      // operand (expression/query_expression.cpp), so a correlated
      // subquery in a later ON hides the columns its subquery body
      // reads.  Refuse elimination for any later ON containing a
      // subquery, mirroring the candidate's own guard above.
      if (ContainsQueryExpression(sources[j].join_condition)) {
        referenced_by_later_on = true;
        break;
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
  // Relational-only features never compose with cells either. This is a
  // deliberately *conservative subset* of PostRewriteNeedsRelational (minus
  // the CTE/derived clauses this lifting resolves): unlike that router we
  // reject every RIGHT/FULL join, including the single-edge M6+1 case it
  // accepts, and every __value_table_value/__proto_new call, including the
  // ordinary bare-star value-table form it excepts. Lifting there would
  // execute eagerly only to discard the rows on the relational fallback;
  // widen these gates together with that router, not by assumption.
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
  names.reserve(outer->WithQueries().size());
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
  // Aliases must be unique across *all* lifted CTEs, not just within one
  // CTE: the commit below keys cells by alias alone, so a collision between
  // two CTEs would silently drop one cell (and waste its eager execution).
  std::unordered_set<std::string> all_site_aliases;
  for (const std::string& name : plain_names) {
    const auto sites_it = top_sites.find(name);
    if (sites_it == top_sites.end() || sites_it->second.empty()) {
      return;
    }
    for (size_t index : sites_it->second) {
      const std::string& alias = outer->Sources()[index].alias;
      // Identifiers are case-insensitive: two sites aliased `A` and `a` name
      // the same cell, so the collision gate must fold them before inserting.
      std::string lowered = alias.empty() ? name : alias;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      if (!all_site_aliases.insert(std::move(lowered)).second) {
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
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_EAGER_CTE_COST_GUARD") !=
      nullptr;
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
  lifted_rows.reserve(plain_names.size());
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
      if (!cells.emplace(alias, std::move(cell)).second) {
        // Defensive: the gate above rejects cross-CTE alias collisions before
        // any body is executed, so this must never trigger.
        return;
      }
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
    columns.reserve(body->SelectList().size());
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
  // Template-bound SELECT literals (`!UsesPreparedValues()`) already carry the
  // concrete bound Value for this execution; callers disarm compiled-plan
  // caching (`dml_fold_applied_ = true`) whenever a literal-dependent rewrite
  // fires. Only deferred INSERT PreparedValues slots lack a prepare-time Value.
  if (const auto* slot = dynamic_cast<const ParameterSlot*>(expr.get());
      slot != nullptr && slot->UsesPreparedValues()) {
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
      // The statement-level scanner covers these clauses; the expression-level
      // one must too, or a parameter hidden in a subquery's GROUP BY / ORDER BY
      // / DISTINCT ON would be treated as a closed constant at plan time.
      for (const Expression& e : s.GroupBy()) {
        if (TpcdsExprHasParam(e)) {
          return true;
        }
      }
      for (const auto& ord : s.OrderBy()) {
        if (TpcdsExprHasParam(ord.expression)) {
          return true;
        }
      }
      for (const Expression& d : s.DistinctOn()) {
        if (TpcdsExprHasParam(d)) {
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
  for (const Expression& d : stmt.DistinctOn()) {
    if (TpcdsExprHasParam(d)) {
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
      // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
      // here
      if (br != nullptr && TpcdsStmtHasParam(*br)) {
        return true;
      }
    }
  }
  return std::ranges::any_of(stmt.WithQueries(), [](const auto& entry) {
    const auto& [name, body] = entry;
    (void)name;
    return body != nullptr && TpcdsStmtHasParam(*body);
  });
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
  // Parameter slots were already rejected above; this second guard only
  // needs to rule out subqueries, aggregates, volatile nodes and free
  // columns before a constant expression is evaluated at plan time.
  if (ContainsQueryExpression(expr) ||
      relational_detail::ContainsAggregate(expr) ||
      !DerivedExpressionIsImmutable(expr) || !expr->TouchedColumns().empty()) {
    return std::nullopt;
  }
  StatusOr<Value> evaluated = expr->TryEvaluate(Row{}, Schema{});
  if (!evaluated.HasValue()) {
    return std::nullopt;
    // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer here
  }
  return evaluated.MoveValue();
}

bool SelectListHasStarOrModifiers(const SelectStatement& stmt) {
  return std::ranges::any_of(
      stmt.SelectList(), [](const NamedExpression& item) {
        if (item.star_modifiers != nullptr || !item.expression) {
          // The frontend can represent a (possibly qualified) star with a null
          // expression. Treat it conservatively as a star: rewrites that
          // project named columns must not run on such an item, since its
          // output arity is not known from the select list alone.
          return true;
        }
        if (item.expression->Type() == TypeTag::kColumnValue &&
            item.expression->AsColumnValue().GetColumnName().name == "*") {
          return true;
        }
        return false;
      });
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
  // Synthetic name for internal slot bookkeeping only. It intentionally
  // mirrors the engine's fallback label for an unnamed expression, but must
  // never be written back into a SELECT item's `name`: that would make the
  // client-visible RowDescription label depend on whether a rewrite fired.
  return "$col_" + std::to_string(index);
}

bool ExtractPureUnionAllBranches(  // NOLINT(misc-no-recursion)
    const SelectStatement& union_stmt,
    std::vector<std::shared_ptr<SelectStatement>>* out_branches) {
  if (union_stmt.UnionDistinct() || union_stmt.UnionByName() ||
      union_stmt.HasLimit() || union_stmt.Offset() != 0 ||
      !union_stmt.OrderBy().empty() || !union_stmt.WithQueries().empty()) {
    return false;
  }
  // Unwrap a trivial pass-through projection wrapper `SELECT c1, c2 FROM
  // (SELECT ... UNION ALL SELECT ...) sq1` (e.g. TPC-DS Q02 `wscs`).
  if (union_stmt.UnionAll().empty() &&
      union_stmt.GetSetOperationTree() == nullptr &&
      union_stmt.GroupBy().empty() && !union_stmt.IsGroupAll() &&
      !union_stmt.Having() && !union_stmt.Distinct() &&
      !union_stmt.HasDistinctOn() && !union_stmt.WhereClause() &&
      !union_stmt.Qualify() && !union_stmt.ForUpdate() &&
      !union_stmt.AsStruct() && !union_stmt.WithTies() &&
      !SelectListHasStarOrModifiers(union_stmt) &&
      union_stmt.Sources().size() == 1) {
    const SelectSource& sq = union_stmt.Sources()[0];
    if (sq.query != nullptr && !sq.unnest && !sq.is_lateral && !sq.sample &&
        sq.using_columns.empty() && !sq.from_nested_join &&
        !sq.join_condition) {
      std::vector<std::shared_ptr<SelectStatement>> inner_branches;
      if (ExtractPureUnionAllBranches(*sq.query, &inner_branches) &&
          !inner_branches.empty() &&
          inner_branches[0]->SelectList().size() ==
              union_stmt.SelectList().size()) {
        bool passthrough = true;
        for (size_t c = 0; c < union_stmt.SelectList().size(); ++c) {
          const NamedExpression& outer_item = union_stmt.SelectList()[c];
          const std::string inner_nm =
              ExplicitOrColumnOutputName(inner_branches[0]->SelectList()[c]);
          const std::string outer_nm = ExplicitOrColumnOutputName(outer_item);
          if (inner_nm.empty() || outer_nm.empty() ||
              !IdentifierEquals(outer_nm, inner_nm) || !outer_item.expression ||
              outer_item.expression->Type() != TypeTag::kColumnValue) {
            passthrough = false;
            break;
          }
          const ColumnName& oc =
              outer_item.expression->AsColumnValue().GetColumnName();
          if ((!oc.schema.empty() &&
               (sq.alias.empty() || !IdentifierEquals(oc.schema, sq.alias))) ||
              !IdentifierEquals(oc.name, inner_nm)) {
            passthrough = false;
            break;
          }
        }
        if (passthrough) {
          *out_branches = std::move(inner_branches);
          return true;
        }
      }
    }
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

// Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
// (using_join_inner_prefix_reorder_and_filter_sound,
//  outer_and_using_join_prefix_reorder_sound)
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
    std::vector<std::string> using_columns;
  };
  auto resolve_source_info =
      [&](const SelectSource& src) -> std::optional<SourceInfo> {
    if (src.is_lateral || src.unnest || src.sample || src.from_nested_join ||
        (src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner &&
         src.join_type != JoinType::kLeft)) {
      return std::nullopt;
    }
    SourceInfo info;
    info.table = src.table;
    info.alias = src.alias.empty() ? src.table : src.alias;
    info.using_columns = src.using_columns;
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
      // USING joins carry their predicate in `using_columns` rather than in
      // `join_condition`.  Moving `c_src` below the LEFT JOIN changes what its
      // USING columns resolve against, so refuse the swap whenever a USING
      // column names a column of the source being crossed.  Symmetrically, the
      // LEFT source's own USING predicate must not read the moved-in source.
      // This is the C++ guard corresponding to the two Lean theorems above.
      if (ok) {
        const auto using_reads = [](const std::vector<std::string>& cols,
                                    const SourceInfo& other) {
          return std::ranges::any_of(cols, [&](const std::string& col) {
            return other.lowered_cols.contains(LoweredStarName(col));
          });
        };
        if (using_reads(c_src.using_columns, b_info) ||
            using_reads(sources[i].using_columns, infos[i + 1])) {
          ok = false;
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
  u_col_names.reserve(u_arity);
  std::unordered_map<std::string, size_t> u_col_index;
  u_col_index.reserve(u_arity);
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
  for (const auto& i : stmt->Sources()) {
    if (i.join_condition) {
      if (!DerivedExpressionIsImmutable(i.join_condition) ||
          relational_detail::ContainsAggregate(i.join_condition)) {
        return false;
      }
      std::optional<Expression> qjc =
          qualify_against_rels(qualify_against_rels, i.join_condition, dim_rels,
                               &u_col_index, u_src.alias);
      if (!qjc.has_value()) {
        return false;
      }
      outer_where_parts.push_back(std::move(*qjc));
    }
  }
  bool has_dim_only_filter = false;
  for (const Expression& ow : outer_where_parts) {
    for (const Expression& conj : SplitConjuncts(ow)) {
      const auto touched = conj->TouchedColumns();
      if (!touched.empty() &&
          std::ranges::none_of(touched, [&](const ColumnName& cn) {
            return IdentifierEquals(cn.schema, u_src.alias);
          })) {
        has_dim_only_filter = true;
        break;
      }
    }
    if (has_dim_only_filter) {
      break;
    }
  }
  if (!has_dim_only_filter) {
    return false;
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
        for (auto& select_slot : select_slots) {
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

    auto subst_u = [&](auto& self,
                       const Expression& expr) -> std::optional<Expression> {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (IdentifierEquals(col.schema, u_src.alias)) {
          const auto found = u_col_index.find(LoweredStarName(col.name));
          if (found == u_col_index.end() || found->second >= b_outputs.size()) {
            return std::nullopt;
          }
          return b_outputs[found->second];
        }
        return expr;
      }
      std::vector<Expression> kids = ExpressionChildren(expr);
      for (Expression& k : kids) {
        auto mapped = self(self, k);
        if (!mapped.has_value()) {
          return std::nullopt;
        }
        k = std::move(*mapped);
      }
      return WithExpressionChildren(expr, std::move(kids));
    };

    std::vector<SelectSource> merged_sources;
    merged_sources.reserve(br->Sources().size());
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
      auto sub_where = subst_u(subst_u, ow);
      if (!sub_where.has_value()) {
        return false;
      }
      b_where_parts.push_back(std::move(*sub_where));
    }

    std::vector<Expression> b_group;
    std::vector<NamedExpression> b_select;
    for (size_t g = 0; g < qual_group_keys.size(); ++g) {
      auto sub_g = subst_u(subst_u, qual_group_keys[g]);
      if (!sub_g.has_value() || !*sub_g) {
        return false;
      }
      b_select.emplace_back("__gk_" + std::to_string(g), *sub_g);
      if ((*sub_g)->Type() != TypeTag::kConstantValue) {
        b_group.push_back(std::move(*sub_g));
      }
    }
    if (b_group.empty()) {
      b_group.push_back(b_select.front().expression);
    }
    size_t agg_seq = 0;
    for (const OuterSelectSlot& slot : select_slots) {
      if (!slot.is_agg) {
        continue;
      }
      auto sub_arg = subst_u(subst_u, slot.qual_agg_arg);
      if (!sub_arg.has_value()) {
        return false;
      }
      b_select.emplace_back(
          "__pagg_" + std::to_string(agg_seq++),
          AggregateExpressionExp(slot.agg_type, std::move(*sub_arg), false));
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
        // Generated names must not collide with a user-declared CTE:
        // AddWithQuery would keep the user's body while redirecting the site
        // to this name (and push a duplicate into with_query_order_).
        for (size_t suffix = 0; outer->WithQueries().contains(b_name);
             ++suffix) {
          b_name = "__tpcds_uch_" + cte_name + "_" + std::to_string(b) + "_" +
                   std::to_string(suffix);
        }
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
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
    //   (YearPairPivotSelfJoinElim_sound)
    // Also supports a single derived-table source (e.g. TPC-DS Q75 `all_sales`
    // over a UNION DISTINCT `sales_detail` subquery) so the discriminator
    // predicate pushes into every UNION branch and the outer year-pair self-
    // join collapses into a single grouped scan.
    std::vector<std::shared_ptr<Table>> body_holders;
    body_holders.reserve(body.Sources().size());
    std::vector<Schema> derived_schemas;
    std::vector<GroupingRelation> body_rels;
    body_rels.reserve(body.Sources().size());
    bool body_rels_ok = true;
    bool body_is_single_derived = false;
    if (body.Sources().size() == 1 && body.Sources()[0].query != nullptr &&
        !body.Sources()[0].is_lateral && !body.Sources()[0].unnest &&
        !body.Sources()[0].sample && !body.Sources()[0].from_nested_join &&
        body.Sources()[0].using_columns.empty() &&
        (body.Sources()[0].join_type == JoinType::kCross ||
         body.Sources()[0].join_type == JoinType::kInner)) {
      const SelectSource& bs = body.Sources()[0];
      const SelectStatement& dq = *bs.query;
      std::vector<Column> dcols;
      dcols.reserve(dq.SelectList().size());
      std::unordered_set<std::string> seen_dcols;
      bool dcols_ok = !dq.SelectList().empty();
      for (const NamedExpression& item : dq.SelectList()) {
        std::string pub = item.name;
        if (pub.empty() && item.expression &&
            item.expression->Type() == TypeTag::kColumnValue) {
          pub = item.expression->AsColumnValue().GetColumnName().name;
        }
        if (pub.empty() || !item.expression ||
            (item.expression->Type() == TypeTag::kColumnValue &&
             item.expression->AsColumnValue().GetColumnName().name == "*") ||
            !seen_dcols.insert(LoweredStarName(pub)).second) {
          dcols_ok = false;
          break;
        }
        dcols.emplace_back(std::move(pub), ValueType::kInt64);
      }
      if (dcols_ok) {
        const std::string dname = bs.alias.empty() ? "__derived" : bs.alias;
        derived_schemas.emplace_back(dname, std::move(dcols));
        body_rels.push_back({.name = dname, .schema = &derived_schemas.back()});
        body_is_single_derived = true;
      } else {
        body_rels_ok = false;
      }
    } else {
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
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        GroupingRelation irel;
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        const std::string pub = item.name.empty() ? ic.name : item.name;
        if (!pub.empty() && ResolveGroupingColumn(body_rels, ic, &irel) &&
            irel.name == grel.name && IdentifierEquals(ic.name, gc.name)) {
          matched_out = pub;
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
    unique_disc_vals.reserve(site_disc_vals.size());
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
      // When the CTE body aggregates over a single derived SELECT or UNION
      // [ALL|DISTINCT] subquery (e.g. TPC-DS Q75 `all_sales` over
      // `sales_detail`), push the discriminator disjunction directly into every
      // branch WHERE clause so all base-table scans and UNION DISTINCT drop
      // non-target years upfront.
      // Lean proof:
      // lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
      //   (YearPairPivotSelfJoinElim_sound)
      if (body_is_single_derived && !body.Sources().empty() &&
          body.Sources()[0].query != nullptr) {
        SelectStatement* dq = body.Sources()[0].query.get();
        std::optional<size_t> disc_pos;
        for (size_t idx = 0; idx < dq->SelectList().size(); ++idx) {
          const NamedExpression& ditem = dq->SelectList()[idx];
          std::string pub = ditem.name;
          if (pub.empty() && ditem.expression &&
              ditem.expression->Type() == TypeTag::kColumnValue) {
            pub = ditem.expression->AsColumnValue().GetColumnName().name;
          }
          if (IdentifierEquals(pub, gkeys[*disc_idx].col_name)) {
            disc_pos = idx;
            break;
          }
        }
        bool push_ok = disc_pos.has_value() && !dq->HasLimit() &&
                       dq->Offset() == 0 && dq->OrderBy().empty() &&
                       !dq->Qualify() && dq->WithQueries().empty();
        for (size_t b = 0; push_ok && b < dq->UnionAll().size(); ++b) {
          const SetOperationKind kind = b < dq->SetOperationKinds().size()
                                            ? dq->SetOperationKinds()[b]
                                            : SetOperationKind::kUnionAll;
          const SetOperationMatch match = dq->MatchFor(b);
          if ((kind != SetOperationKind::kUnionAll &&
               kind != SetOperationKind::kUnion) ||
              match.corresponding || match.by_name || !match.columns.empty() ||
              dq->UnionAll()[b] == nullptr) {
            push_ok = false;
          }
        }
        if (push_ok) {
          std::vector<SelectStatement*> branches;
          std::unordered_set<SelectStatement*> seen_branches;
          auto add_branch = [&](SelectStatement* br) {
            if (br != nullptr && seen_branches.insert(br).second) {
              branches.push_back(br);
            }
          };
          add_branch(dq);
          for (const auto& br : dq->UnionAll()) {
            add_branch(br.get());
          }
          if (const auto& tree = dq->GetSetOperationTree(); tree != nullptr) {
            for (const SetOperationKind kind : tree->kinds) {
              if (kind != SetOperationKind::kUnionAll &&
                  kind != SetOperationKind::kUnion) {
                push_ok = false;
                break;
              }
            }
            if (push_ok) {
              add_branch(tree->first.get());
              for (const auto& br : tree->branches) {
                add_branch(br.get());
              }
            }
          }
          for (const SelectStatement* br : branches) {
            if (!br->GroupBy().empty() || br->Having() || br->HasLimit() ||
                br->Offset() != 0 || br->Qualify() ||
                !br->WithQueries().empty() ||
                *disc_pos >= br->SelectList().size() ||
                !br->SelectList()[*disc_pos].expression ||
                br->SelectList()[*disc_pos].expression->Type() !=
                    TypeTag::kColumnValue) {
              push_ok = false;
              break;
            }
          }
          if (push_ok) {
            for (SelectStatement* br : branches) {
              const Expression& b_disc = br->SelectList()[*disc_pos].expression;
              Expression b_disj;
              for (const Value& uv : unique_disc_vals) {
                Expression eq = BinaryExpressionExp(
                    b_disc, BinaryOperation::kEquals, ConstantValueExp(uv));
                b_disj = b_disj ? BinaryExpressionExp(std::move(b_disj),
                                                      BinaryOperation::kOr,
                                                      std::move(eq))
                                : std::move(eq);
              }
              if (b_disj) {
                Expression bw = br->WhereClause()
                                    ? BinaryExpressionExp(br->WhereClause(),
                                                          BinaryOperation::kAnd,
                                                          std::move(b_disj))
                                    : std::move(b_disj);
                br->SetWhereClause(std::move(bw));
              }
            }
          }
        }
      }
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
    // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
    // (bijective_dim_key_cte_deferral_sound)
    auto find_body_table_name =
        [&](const std::string& rel_name) -> std::string {
      for (const SelectSource& bs : body.Sources()) {
        const std::string bal = bs.alias.empty() ? bs.table : bs.alias;
        if (IdentifierEquals(bal, rel_name)) {
          return bs.table;
        }
      }
      return {};
    };
    std::vector<bool> key_equated_across_sites(gkeys.size(), false);
    std::unordered_set<std::string> certified_shared_rels;
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      const std::string rep_tok = col_token(rep_alias, gkeys[k].out_name);
      bool eq_all = true;
      for (size_t s = 1; s < site_indices.size(); ++s) {
        const SelectSource& src = outer->Sources()[site_indices[s]];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        if (!are_equated(rep_tok, col_token(salias, gkeys[k].out_name))) {
          eq_all = false;
          break;
        }
      }
      key_equated_across_sites[k] = eq_all;
      if (eq_all) {
        if (ColumnIsPrimaryKey(gkeys[k].rel, gkeys[k].col_name) ||
            InspectRuntimeUniqueNonNullColumn(
                ctx, find_body_table_name(gkeys[k].rel.name), gkeys[k].col_name)
                .unique_non_null) {
          certified_shared_rels.insert(gkeys[k].rel.name);
        }
      }
    }
    bool all_shared_keys_equated = true;
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      if (!key_equated_across_sites[k] &&
          !certified_shared_rels.contains(gkeys[k].rel.name)) {
        all_shared_keys_equated = false;
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
    item_specs.reserve(body.SelectList().size());
    bool items_ok = true;
    std::unordered_set<std::string> seen_body_out;
    for (const NamedExpression& item : body.SelectList()) {
      std::string pub = item.name;
      if (pub.empty() && item.expression &&
          item.expression->Type() == TypeTag::kColumnValue) {
        pub = item.expression->AsColumnValue().GetColumnName().name;
      }
      if (pub.empty() || !item.expression ||
          !seen_body_out.insert(LoweredStarName(pub)).second) {
        items_ok = false;
        break;
      }
      BodyItemSpec spec;
      spec.out_name = std::move(pub);
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
    piv_select.reserve(item_specs.size());
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
    // DISTINCT ON lives in the same scope as the select list and ORDER BY;
    // leaving a site-alias reference there after the sites are merged would
    // dangle. A key that cannot be rebound keeps the statement untouched.
    std::vector<Expression> new_outer_distinct_on;
    new_outer_distinct_on.reserve(outer->DistinctOn().size());
    for (const Expression& key : outer->DistinctOn()) {
      std::optional<Expression> rd = rebind_outer_expr(rebind_outer_expr, key);
      if (!rd.has_value()) {
        rebind_ok = false;
        break;
      }
      new_outer_distinct_on.push_back(std::move(*rd));
    }
    if (!rebind_ok) {
      continue;
    }

    std::string pivcte_name = "__tpcds_pivcte_" + cte_name;
    for (size_t suffix = 0; outer->WithQueries().contains(pivcte_name);
         ++suffix) {
      pivcte_name = "__tpcds_pivcte_" + cte_name + "_" + std::to_string(suffix);
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
        s.table = pivcte_name;
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
      if (key_equated_across_sites[k]) {
        piv_where.push_back(
            UnaryExpressionExp(gkeys[k].expr, UnaryOperation::kIsNotNull));
      }
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

    outer->AddWithQuery(pivcte_name, std::move(piv_body));
    outer->RemoveWithQuery(cte_name);
    outer->SetSources(std::move(new_outer_sources));
    outer->SetSelectList(std::move(new_outer_sel));
    outer->SetWhereClause(CombineConjuncts(final_outer_where));
    outer->SetGroupBy(std::move(new_outer_group));
    outer->SetHaving(std::move(*new_outer_having));
    outer->SetQualify(std::move(*new_outer_qualify));
    if (!new_outer_distinct_on.empty()) {
      outer->SetDistinctOn(std::move(new_outer_distinct_on));
    }
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
        win_aggs.push_back(
            {.sb_out_name = item.name,
             .win_col_name = "__tpcds_sb_win_" + IntegerToName(win_aggs.size()),
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
      // The sb source is dropped below, so its output references in DISTINCT
      // ON must be remapped onto the surviving window source as well.
      std::vector<Expression> new_distinct_on;
      new_distinct_on.reserve(outer->DistinctOn().size());
      for (const Expression& key : outer->DistinctOn()) {
        std::optional<Expression> md = map_expr(map_expr, key);
        if (!md.has_value()) {
          ok = false;
          break;
        }
        new_distinct_on.push_back(std::move(*md));
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
      if (!new_distinct_on.empty()) {
        outer->SetDistinctOn(std::move(new_distinct_on));
      }
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
    cte_col_names.reserve(cte_body.SelectList().size());
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
        // Both loops are intentionally inside this body (8-space scope) and
        // follow the collected declaration order; keep them formatted that
        // way so the partition/SELECT list boundaries stay unambiguous.
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
        // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
        // here
      }
      auto check_sq_cols = [&](const Expression& se) {
        if (!se) {
          return true;
        }
        return std::ranges::all_of(
            se->TouchedColumns(), [&](const ColumnName& cn) {
              return cn.schema.empty() ||
                     sq_local.contains(LoweredStarName(cn.schema));
            });
        // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
        // here
      };
      // Every clause that can contain an expression must be checked, not just
      // WHERE/HAVING/SELECT: an outer-qualified reference hiding in the
      // subquery's ORDER BY (or DISTINCT ON / GROUP BY / QUALIFY) would be
      // re-bound to the wrong scope once the derived query is hoisted.
      if (!check_sq_cols(sq.Query()->WhereClause()) ||
          !check_sq_cols(sq.Query()->Having()) ||
          !check_sq_cols(sq.Query()->Qualify())) {
        return false;
      }
      for (const Expression& group_expr : sq.Query()->GroupBy()) {
        if (!check_sq_cols(group_expr)) {
          return false;
        }
      }
      for (const Expression& distinct_expr : sq.Query()->DistinctOn()) {
        if (!check_sq_cols(distinct_expr)) {
          return false;
        }
      }
      for (const auto& term : sq.Query()->OrderBy()) {
        if (!check_sq_cols(term.expression)) {
          return false;
        }
      }
      return std::ranges::all_of(sq.Query()->SelectList(),
                                 [&](const NamedExpression& si) {
                                   return check_sq_cols(si.expression);
                                 });
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
  for (const Expression& d : q.DistinctOn()) {
    if (!expr_self_contained(expr_self_contained, d)) {
      return false;
    }
  }
  for (const auto& term : q.OrderBy()) {
    if (!expr_self_contained(expr_self_contained, term.expression)) {
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
  col_to_idx.reserve(arity);
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
  // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed
  // concatenations
  std::vector<std::string> outer_out_names;
  std::unordered_set<std::string> seen_outer_names;
  for (size_t i = 0; i < select->SelectList().size(); ++i) {
    std::string nm = InferSelectItemOutputName(select->SelectList()[i], i);
    if (!seen_outer_names.insert(LoweredStarName(nm)).second) {
      nm += "_";
      nm += std::to_string(i);
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
  if (has_outer_limit &&
      outer_offset > std::numeric_limits<size_t>::max() - outer_limit) {
    return false;
  }
  const std::optional<size_t> branch_limit =
      has_outer_limit ? std::optional<size_t>(outer_limit + outer_offset)
                      : std::nullopt;

  std::vector<std::shared_ptr<SelectStatement>> flat_branches;
  flat_branches.reserve(branches.size());
  for (const auto& branch : branches) {
    auto aligned_b = std::make_shared<SelectStatement>(*branch);
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
    non_const_group.reserve(b_outer->GroupBy().size());
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
      // A bare integer GROUP BY ordinal has already been resolved to its
      // SELECT item by this stage; grouping by a constant cannot split rows,
      // so dropping it is safe as long as at least one real key remains (the
      // all-constant case is guarded below because it changes the empty-input
      // row count).
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

namespace {

// CTE declarations in a stable order: the declared order first, then any
// entry missing from WithQueryOrder in name order.  Iterating WithQueries()
// directly would give an unordered_map-dependent (and thus non-reproducible)
// order for hand-built statements whose WithQueryOrder is empty.
std::vector<std::string> OrderedCteNames(const SelectStatement& src) {
  std::vector<std::string> names;
  names.reserve(src.WithQueries().size());
  for (const std::string& name : src.WithQueryOrder()) {
    if (src.WithQueries().contains(name)) {
      names.push_back(name);
    }
  }
  std::vector<std::string> rest;
  for (const auto& [name, body] : src.WithQueries()) {
    (void)body;
    if (std::find(names.begin(), names.end(), name) == names.end()) {
      rest.push_back(name);
    }
  }
  std::sort(rest.begin(), rest.end());
  names.insert(names.end(), rest.begin(), rest.end());
  return names;
}

void CopyCte(SelectStatement* container, const SelectStatement& src,
             const std::string& name) {
  const auto it = src.WithQueries().find(name);
  if (it == src.WithQueries().end()) {
    return;
  }
  if (src.IsRecursiveWith(name)) {
    container->AddRecursiveWithQuery(name, it->second);
  } else {
    container->AddWithQuery(name, it->second);
  }
  if (const RecursiveDepthSpec* depth = src.RecursiveDepthOf(name)) {
    container->SetRecursiveDepth(name, *depth);
  }
}

void SyncSetOpHeadFields(
    SelectStatement* container, const SelectStatement& head,
    const std::vector<const SelectStatement*>* other_leaves = nullptr) {
  container->SetSources(head.Sources());
  container->SetWhereClause(head.WhereClause());
  container->SetSelectList(head.SelectList());
  container->SetGroupBy(head.GroupBy());
  container->SetHaving(head.Having());
  container->SetQualify(head.Qualify());
  container->SetDistinct(head.Distinct());
  // A set operation has no single row position per output row, so FOR UPDATE
  // must never be inherited from a leaf: copying it here would bypass the
  // explicit diagnostic that refuses locking reads on set operations.
  container->SetForUpdate(false);
  // Leaf rewrites can add/remove CTE declarations. Keep the container's view
  // in lockstep, carrying declaration order and the recursive/depth markers
  // that a plain AddWithQuery would not preserve. Compare the order too: a
  // rewrite that only reorders declarations leaves the map identical.
  if (container->WithQueries() != head.WithQueries() ||
      container->WithQueryOrder() != head.WithQueryOrder()) {
    container->ClearWithQueries();
    for (const std::string& name : OrderedCteNames(head)) {
      CopyCte(container, head, name);
    }
  }
  // Rewrites of a non-head branch can mint auxiliary CTEs as well; those must
  // be visible in the set-operation container's WITH scope too.
  if (other_leaves != nullptr) {
    for (const SelectStatement* leaf : *other_leaves) {
      if (leaf == nullptr) {
        continue;
      }
      for (const std::string& name : OrderedCteNames(*leaf)) {
        if (!container->WithQueries().contains(name)) {
          CopyCte(container, *leaf, name);
        }
      }
    }
  }
  // ORDER BY / LIMIT / OFFSET, DISTINCT ON and WITH TIES are set-operation
  // container-level clauses: callers set them explicitly after the leaves are
  // rewritten, so they are deliberately not copied from a leaf here.
}

template <typename LeafFn>
bool VisitSetOpLeavesAndSync(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited,
    LeafFn&& leaf_fn) {
  if (stmt == nullptr || !visited->insert(stmt).second) {
    return false;
  }
  if (stmt->UnionAll().empty() && stmt->GetSetOperationTree() == nullptr) {
    return leaf_fn(stmt);
  }
  bool changed = false;
  const auto& tree = stmt->GetSetOperationTree();
  if (tree != nullptr) {
    if (tree->first != nullptr) {
      changed |= VisitSetOpLeavesAndSync(tree->first.get(), visited, leaf_fn);
    }
    std::vector<const SelectStatement*> other_leaves;
    other_leaves.reserve(tree->branches.size());
    for (const auto& br : tree->branches) {
      changed |= VisitSetOpLeavesAndSync(br.get(), visited, leaf_fn);
      other_leaves.push_back(br.get());
    }
    if (changed && !tree->grouped && tree->first != nullptr &&
        tree->first->UnionAll().empty() &&
        tree->first->GetSetOperationTree() == nullptr) {
      // Flattening promotes branch-local WITH declarations into a common
      // scope. Preserve distinct lexical bindings by executing the original
      // operands separately when two branches declare the same name.
      auto bindings = tree->first->WithQueries();
      bool separate_scopes = false;
      for (const SelectStatement* leaf : other_leaves) {
        if (leaf == nullptr) {
          continue;
        }
        for (const auto& [name, body] : leaf->WithQueries()) {
          auto [at, inserted] = bindings.emplace(name, body);
          if (!inserted && at->second != body) {
            separate_scopes = true;
          }
        }
      }
      if (separate_scopes) {
        auto scoped = std::make_shared<SetOperationTree>(*tree);
        scoped->grouped = true;
        stmt->SetSetOperationTree(std::move(scoped));
      } else {
        SyncSetOpHeadFields(stmt, *tree->first, &other_leaves);
      }
    }
  } else {
    SelectStatement head = *stmt;
    head.ClearUnionAll();
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    if (leaf_fn(&head)) {
      SyncSetOpHeadFields(stmt, head);
      changed = true;
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    changed |= VisitSetOpLeavesAndSync(br.get(), visited, leaf_fn);
  }
  return changed;
}

bool ExprContainsWindowLocal(const Expression& expr) {
  if (!expr) {
    return false;
  }
  std::vector<Expression> stack{expr};
  while (!stack.empty()) {
    Expression cur = std::move(stack.back());
    stack.pop_back();
    if (!cur) {
      continue;
    }
    if (cur->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (cur->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(cur)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

bool CanonicalizeTwoValuedStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited);

Expression CanonicalizeTwoValuedExprImpl(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::unordered_set<SelectStatement*>* visited,
    bool* changed) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kQueryExp) {
    const auto& q = expr->AsQueryExpression();
    if (q.Query() != nullptr) {
      if (CanonicalizeTwoValuedStmtImpl(q.Query().get(), visited)) {
        *changed = true;
      }
    }
    if (q.Test()) {
      bool test_changed = false;
      Expression new_test =
          CanonicalizeTwoValuedExprImpl(q.Test(), visited, &test_changed);
      if (test_changed) {
        *changed = true;
        // A kQueryExp exposes only its Test() leg through ExpressionChildren;
        // WithExpressionChildren with this single child rebuilds the
        // QueryExpression with the original Query/Exists/Negated/Op/Mode
        // discriminators intact (no query=nullptr fallback).
        return WithExpressionChildren(expr, {std::move(new_test)});
      }
    }
    return expr;
  }
  std::vector<Expression> children = ExpressionChildren(expr);
  bool child_changed = false;
  for (Expression& c : children) {
    if (c) {
      c = CanonicalizeTwoValuedExprImpl(c, visited, &child_changed);
    }
  }
  Expression rebuilt =
      child_changed ? WithExpressionChildren(expr, std::move(children)) : expr;
  if (child_changed) {
    *changed = true;
  }
  if (rebuilt->Type() == TypeTag::kUnaryExp &&
      rebuilt->AsUnaryExpression().Op() == UnaryOperation::kNot) {
    const Expression& inner = rebuilt->AsUnaryExpression().Child();
    if (inner && inner->Type() == TypeTag::kUnaryExp) {
      std::optional<UnaryOperation> dual;
      switch (inner->AsUnaryExpression().Op()) {
        case UnaryOperation::kIsNull:
          dual = UnaryOperation::kIsNotNull;
          break;
        case UnaryOperation::kIsNotNull:
          dual = UnaryOperation::kIsNull;
          break;
        case UnaryOperation::kIsTrue:
          dual = UnaryOperation::kIsNotTrue;
          break;
        case UnaryOperation::kIsNotTrue:
          dual = UnaryOperation::kIsTrue;
          break;
        case UnaryOperation::kIsFalse:
          dual = UnaryOperation::kIsNotFalse;
          break;
        case UnaryOperation::kIsNotFalse:
          dual = UnaryOperation::kIsFalse;
          break;
        default:
          break;
      }
      if (dual.has_value()) {
        *changed = true;
        return UnaryExpressionExp(inner->AsUnaryExpression().Child(), *dual);
      }
    }
  }
  return rebuilt;
}

bool CanonicalizeTwoValuedLeaf(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited) {
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    (void)name;
    if (body != nullptr) {
      changed |= CanonicalizeTwoValuedStmtImpl(body.get(), visited);
    }
  }
  std::vector<SelectSource> sources = stmt->Sources();
  bool src_changed = false;
  for (SelectSource& src : sources) {
    if (src.query != nullptr) {
      changed |= CanonicalizeTwoValuedStmtImpl(src.query.get(), visited);
    }
    if (src.join_condition) {
      bool jc_changed = false;
      src.join_condition = CanonicalizeTwoValuedExprImpl(src.join_condition,
                                                         visited, &jc_changed);
      src_changed |= jc_changed;
    }
    if (src.unnest) {
      bool un_changed = false;
      src.unnest =
          CanonicalizeTwoValuedExprImpl(src.unnest, visited, &un_changed);
      src_changed |= un_changed;
    }
  }
  if (src_changed) {
    stmt->SetSources(std::move(sources));
    changed = true;
  }
  if (stmt->WhereClause()) {
    bool w_changed = false;
    Expression nw =
        CanonicalizeTwoValuedExprImpl(stmt->WhereClause(), visited, &w_changed);
    if (w_changed) {
      stmt->SetWhereClause(std::move(nw));
      changed = true;
    }
  }
  if (stmt->Having()) {
    bool h_changed = false;
    Expression nh =
        CanonicalizeTwoValuedExprImpl(stmt->Having(), visited, &h_changed);
    if (h_changed) {
      stmt->SetHaving(std::move(nh));
      changed = true;
    }
  }
  if (stmt->Qualify()) {
    bool q_changed = false;
    Expression nq =
        CanonicalizeTwoValuedExprImpl(stmt->Qualify(), visited, &q_changed);
    if (q_changed) {
      stmt->SetQualify(std::move(nq));
      changed = true;
    }
  }
  if (!stmt->SelectList().empty()) {
    std::vector<NamedExpression> sel = stmt->SelectList();
    bool s_changed = false;
    for (NamedExpression& item : sel) {
      if (item.expression) {
        item.expression =
            CanonicalizeTwoValuedExprImpl(item.expression, visited, &s_changed);
      }
    }
    if (s_changed) {
      stmt->SetSelectList(std::move(sel));
      changed = true;
    }
  }
  if (!stmt->GroupBy().empty()) {
    std::vector<Expression> grp = stmt->GroupBy();
    bool g_changed = false;
    for (Expression& g : grp) {
      if (g) {
        g = CanonicalizeTwoValuedExprImpl(g, visited, &g_changed);
      }
    }
    if (g_changed) {
      stmt->SetGroupBy(std::move(grp));
      changed = true;
    }
  }
  if (!stmt->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> ord = stmt->OrderBy();
    bool o_changed = false;
    for (auto& term : ord) {
      if (term.expression) {
        term.expression =
            CanonicalizeTwoValuedExprImpl(term.expression, visited, &o_changed);
      }
    }
    if (o_changed) {
      stmt->SetOrderBy(std::move(ord));
      changed = true;
    }
  }
  return changed;
}

bool CanonicalizeTwoValuedStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    for (const auto& [name, body] : stmt->WithQueries()) {
      (void)name;
      if (body != nullptr) {
        changed |= CanonicalizeTwoValuedStmtImpl(body.get(), visited);
      }
    }
    if (!stmt->OrderBy().empty()) {
      std::vector<SelectStatement::OrderByTerm> ord = stmt->OrderBy();
      bool o_changed = false;
      for (auto& term : ord) {
        if (term.expression) {
          term.expression = CanonicalizeTwoValuedExprImpl(term.expression,
                                                          visited, &o_changed);
        }
      }
      if (o_changed) {
        stmt->SetOrderBy(std::move(ord));
        changed = true;
      }
    }
  }
  changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* leaf) {
    return CanonicalizeTwoValuedLeaf(leaf, visited);
  });
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (two_valued_predicate_not_involution_sound, sqlIsNull_never_tNull)
bool CanonicalizeTwoValuedNegations(SelectStatement* select) {
  std::unordered_set<SelectStatement*> visited;
  return CanonicalizeTwoValuedStmtImpl(select, &visited);
}

namespace {

bool CanonicalizeAliasGroupByStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        enclosing_ctes,
    std::unordered_set<SelectStatement*>* visited);

bool CanonicalizeAliasGroupByLeaf(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        enclosing_ctes,
    std::unordered_set<SelectStatement*>* visited) {
  bool changed = false;
  std::unordered_map<std::string, std::shared_ptr<SelectStatement>>
      active_ctes = enclosing_ctes;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= CanonicalizeAliasGroupByStmtImpl(body.get(), ctx, active_ctes,
                                                  visited);
      active_ctes[name] = body;
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= CanonicalizeAliasGroupByStmtImpl(src.query.get(), ctx,
                                                  active_ctes, visited);
    }
  }
  if (stmt->GroupBy().empty() || stmt->IsGroupAll() || stmt->HasDistinctOn() ||
      stmt->WithTies() || SelectListHasStarOrModifiers(*stmt) ||
      TpcdsStmtHasParam(*stmt) || stmt->Sources().empty()) {
    return changed;
  }

  struct RelCols {
    std::string alias;
    std::string table;
    // A multimap: two output columns whose names differ only by case collapse
    // to the same lowered key, and that must stay *ambiguous* rather than
    // silently losing one of the entries to emplace().
    std::unordered_multimap<std::string, std::string> lowered_to_exact;
  };
  std::vector<RelCols> rels;
  rels.reserve(stmt->Sources().size());
  for (const SelectSource& src : stmt->Sources()) {
    if (src.unnest || src.is_lateral || src.from_nested_join ||
        !src.using_columns.empty()) {
      return changed;
    }
    RelCols rc;
    rc.table = src.table;
    rc.alias = src.alias.empty() ? src.table : src.alias;
    if (rc.alias.empty()) {
      return changed;
    }
    if (src.query != nullptr) {
      if (SelectListHasStarOrModifiers(*src.query)) {
        return changed;
      }
      for (const NamedExpression& item : src.query->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return changed;
        }
        rc.lowered_to_exact.emplace(LoweredStarName(out_nm), out_nm);
      }
    } else if (const auto cte_it = active_ctes.find(src.table);
               cte_it != active_ctes.end()) {
      if (cte_it->second == nullptr ||
          SelectListHasStarOrModifiers(*cte_it->second)) {
        return changed;
      }
      for (const NamedExpression& item : cte_it->second->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return changed;
        }
        rc.lowered_to_exact.emplace(LoweredStarName(out_nm), out_nm);
      }
    } else {
      StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
      if (!tbl.HasValue()) {
        return changed;
      }
      const Schema& schema = tbl.Value()->GetSchema();
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const std::string& cn = schema.GetColumn(i).Name().name;
        rc.lowered_to_exact.emplace(LoweredStarName(cn), cn);
      }
    }
    rels.push_back(std::move(rc));
  }

  auto col_resolves_uniquely = [&](const ColumnName& col) -> bool {
    if (col.name.empty() || col.name == "*") {
      return false;
    }
    const std::string low = LoweredStarName(col.name);
    size_t hits = 0;
    for (const RelCols& rc : rels) {
      if (!col.schema.empty()) {
        if (IdentifierEquals(col.schema, rc.alias) ||
            (!rc.table.empty() && IdentifierEquals(col.schema, rc.table))) {
          hits += rc.lowered_to_exact.count(low);
        }
      } else {
        hits += rc.lowered_to_exact.count(low);
      }
    }
    return hits == 1;
  };

  std::vector<Expression> new_group = stmt->GroupBy();
  bool local_changed = false;
  for (Expression& gk : new_group) {
    if (!gk || gk->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& gcol = gk->AsColumnValue().GetColumnName();
    if (!gcol.schema.empty() || gcol.name.empty() || gcol.name == "*") {
      continue;
    }
    const std::string low = LoweredStarName(gcol.name);
    size_t src_hits = 0;
    const RelCols* matched_rel = nullptr;
    std::string exact_col;
    for (const RelCols& rc : rels) {
      const size_t n = rc.lowered_to_exact.count(low);
      if (n == 0) {
        continue;
      }
      // More than one column in the same relation matches the lowered name
      // (e.g. `Foo` and `foo`): the reference is ambiguous, so do not record a
      // single match and let the `src_hits != 1` guard skip qualification.
      src_hits += n;
      if (n == 1) {
        matched_rel = &rc;
        exact_col = rc.lowered_to_exact.find(low)->second;
      }
    }
    if (src_hits == 1) {
      // Base columns win over SELECT aliases in GoogleSQL: qualify when a
      // SELECT item reuses the same alias name for a derived expression (Q08).
      bool shadowed_by_diff_expr = false;
      for (const NamedExpression& item : stmt->SelectList()) {
        if (!item.name.empty() && IdentifierEquals(item.name, gcol.name) &&
            (!item.expression ||
             item.expression->Type() != TypeTag::kColumnValue ||
             !IdentifierEquals(
                 item.expression->AsColumnValue().GetColumnName().name,
                 gcol.name))) {
          shadowed_by_diff_expr = true;
          break;
        }
      }
      if (shadowed_by_diff_expr) {
        gk = ColumnValueExp(ColumnName(matched_rel->alias, exact_col));
        local_changed = true;
      }
      continue;
    }
    if (src_hits == 0) {
      const NamedExpression* alias_item = nullptr;
      size_t alias_hits = 0;
      for (const NamedExpression& item : stmt->SelectList()) {
        if (!item.name.empty() && IdentifierEquals(item.name, gcol.name)) {
          alias_item = &item;
          ++alias_hits;
        }
      }
      if (alias_hits != 1 || alias_item->expression == nullptr ||
          alias_item->expression->Type() == TypeTag::kConstantValue ||
          ContainsQueryExpression(alias_item->expression) ||
          relational_detail::ContainsAggregate(alias_item->expression) ||
          ExprContainsWindowLocal(alias_item->expression) ||
          !DerivedExpressionIsImmutable(alias_item->expression)) {
        continue;
      }
      const auto touched = alias_item->expression->TouchedColumns();
      if (touched.empty() ||
          !std::ranges::all_of(touched, col_resolves_uniquely)) {
        continue;
      }
      if (stmt->Having()) {
        const auto having_cols = stmt->Having()->TouchedColumns();
        const bool having_uses_alias =
            std::ranges::any_of(having_cols, [&](const ColumnName& hc) {
              return hc.schema.empty() && IdentifierEquals(hc.name, gcol.name);
            });
        if (having_uses_alias) {
          continue;
        }
      }
      gk = alias_item->expression;
      local_changed = true;
    }
  }
  if (local_changed) {
    stmt->SetGroupBy(std::move(new_group));
    changed = true;
  }
  return changed;
}

bool CanonicalizeAliasGroupByStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        enclosing_ctes,
    std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  std::unordered_map<std::string, std::shared_ptr<SelectStatement>>
      active_ctes = enclosing_ctes;
  const bool is_set_op =
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      if (is_set_op) {
        // A set-op container owns its CTEs directly; process them here before
        // the leaf visit (the leaves do not see the container's WITH list).
        changed |= CanonicalizeAliasGroupByStmtImpl(body.get(), ctx,
                                                    active_ctes, visited);
      }
      // Register every non-recursive CTE for the leaf visit, not only the
      // set-op case, so forward references resolve there too. Recursive
      // entries stay opaque (their body is its own work-table scope).
      active_ctes[name] = body;
    }
  }
  changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* leaf) {
    return CanonicalizeAliasGroupByLeaf(leaf, ctx, active_ctes, visited);
  });
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (select_alias_group_by_canonicalization_sound)
bool CanonicalizeSelectAliasGroupBy(SelectStatement* select,
                                    TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return CanonicalizeAliasGroupByStmtImpl(select, ctx, {}, &visited);
}

namespace {

bool ExprHasGroupContextMod(const Expression& expr) {
  if (!expr) {
    return false;
  }
  std::vector<Expression> stack{expr};
  while (!stack.empty()) {
    Expression cur = std::move(stack.back());
    stack.pop_back();
    if (!cur) {
      continue;
    }
    if (cur->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (cur->Type() == TypeTag::kAggregateExp) {
      const auto& agg = cur->AsAggregateExpression();
      if (agg.NeedsGroupContext() || !agg.InnerGroupBy().empty()) {
        return true;
      }
    }
    if (cur->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(cur)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

bool ValidateGroupedCteTarget(
    SelectStatement* grouped_stmt,
    std::unordered_map<std::string, ColumnName>* out_to_group_col) {
  if (grouped_stmt == nullptr || grouped_stmt->GroupBy().empty() ||
      grouped_stmt->IsGroupAll() || grouped_stmt->Distinct() ||
      grouped_stmt->HasDistinctOn() || grouped_stmt->HasLimit() ||
      grouped_stmt->Offset() != 0 || grouped_stmt->Qualify() ||
      grouped_stmt->WithTies() || !grouped_stmt->UnionAll().empty() ||
      grouped_stmt->GetSetOperationTree() != nullptr ||
      !grouped_stmt->WithQueries().empty() ||
      relational_detail::HasWindowFunctions(*grouped_stmt) ||
      SelectListHasStarOrModifiers(*grouped_stmt) ||
      TpcdsStmtHasParam(*grouped_stmt) ||
      ExprHasGroupContextMod(grouped_stmt->Having())) {
    return false;
  }
  for (const NamedExpression& item : grouped_stmt->SelectList()) {
    if (ExprHasGroupContextMod(item.expression)) {
      return false;
    }
  }
  for (size_t idx = 0; idx < grouped_stmt->SelectList().size(); ++idx) {
    const NamedExpression& item = grouped_stmt->SelectList()[idx];
    if (!item.expression || item.expression->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& sc = item.expression->AsColumnValue().GetColumnName();
    for (const Expression& gk : grouped_stmt->GroupBy()) {
      if (!gk || gk->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& gc = gk->AsColumnValue().GetColumnName();
      if (IdentifierEquals(sc.name, gc.name) &&
          (sc.schema.empty() || gc.schema.empty() ||
           IdentifierEquals(sc.schema, gc.schema))) {
        const std::string out_nm = InferSelectItemOutputName(item, idx);
        const ColumnName chosen = !gc.schema.empty() ? gc : sc;
        const auto [it, inserted] =
            out_to_group_col->emplace(LoweredStarName(out_nm), chosen);
        if (!inserted && !(it->second == chosen)) {
          // Two output columns share a (lowercased) name but resolve to
          // different GROUP BY columns. Pushing either one's predicate would
          // silently drop rows the other site needs, so refuse the mapping.
          return false;
        }
        break;
      }
    }
  }
  return !out_to_group_col->empty();
}

Expression MakeValueDomainPredicate(const ColumnName& col,
                                    const std::vector<Value>& vals) {
  if (vals.size() == 1) {
    return BinaryExpressionExp(ColumnValueExp(col), BinaryOperation::kEquals,
                               ConstantValueExp(vals[0]));
  }
  if (!vals.empty() && !vals[0].IsNull() && !vals[0].IsUnsigned() &&
      (vals[0].type == ValueType::kInt64 || vals[0].type == ValueType::kDate)) {
    bool all_same_int = true;
    int64_t lo = vals[0].value.int_value;
    int64_t hi = vals[0].value.int_value;
    std::vector<int64_t> uniq;
    uniq.reserve(vals.size());
    for (const Value& v : vals) {
      if (v.IsNull() || v.IsUnsigned() || v.type != vals[0].type) {
        all_same_int = false;
        break;
      }
      lo = std::min(lo, v.value.int_value);
      hi = std::max(hi, v.value.int_value);
      if (std::ranges::find(uniq, v.value.int_value) == uniq.end()) {
        uniq.push_back(v.value.int_value);
      }
    }
    if (all_same_int && !uniq.empty()) {
      Value lo_val = vals[0];
      lo_val.value.int_value = lo;
      Value hi_val = vals[0];
      hi_val.value.int_value = hi;
      Expression hull = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(col),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(lo_val)),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(col),
                              BinaryOperation::kLessThanEquals,
                              ConstantValueExp(hi_val)));
      if (hi >= lo &&
          static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) + 1ULL ==
              uniq.size()) {
        return hull;
      }
      std::vector<Expression> list;
      list.reserve(vals.size());
      for (const Value& v : vals) {
        list.push_back(ConstantValueExp(v));
      }
      return BinaryExpressionExp(
          std::move(hull), BinaryOperation::kAnd,
          InExpressionExp(ColumnValueExp(col), std::move(list)));
    }
  }
  std::vector<Expression> list;
  list.reserve(vals.size());
  for (const Value& v : vals) {
    list.push_back(ConstantValueExp(v));
  }
  return InExpressionExp(ColumnValueExp(col), std::move(list));
}

void PushPredicateIntoUnionQueryIfSafe(
    SelectStatement* u_stmt, std::string_view target_out_col,
    const std::vector<Value>& domain_vals,
    std::optional<std::pair<Value, Value>> closed_range,
    TransactionContext& ctx) {
  if (u_stmt == nullptr) {
    return;
  }
  std::string cur_col(target_out_col);
  while (
      u_stmt != nullptr && u_stmt->UnionAll().empty() &&
      u_stmt->GetSetOperationTree() == nullptr &&
      u_stmt->Sources().size() == 1 && u_stmt->Sources()[0].query != nullptr &&
      !u_stmt->Sources()[0].unnest && !u_stmt->Sources()[0].is_lateral &&
      !u_stmt->Sources()[0].sample &&
      u_stmt->Sources()[0].using_columns.empty() &&
      !u_stmt->Sources()[0].from_nested_join &&
      !u_stmt->Sources()[0].join_condition && !u_stmt->WhereClause() &&
      u_stmt->GroupBy().empty() && !u_stmt->IsGroupAll() && !u_stmt->Having() &&
      !u_stmt->Distinct() && !u_stmt->HasDistinctOn() && !u_stmt->HasLimit() &&
      u_stmt->Offset() == 0 && !u_stmt->Qualify() &&
      u_stmt->WithQueries().empty() && !SelectListHasStarOrModifiers(*u_stmt)) {
    const std::string sub_al = u_stmt->Sources()[0].alias;
    std::string mapped_inner;
    for (size_t idx = 0; idx < u_stmt->SelectList().size(); ++idx) {
      const NamedExpression& item = u_stmt->SelectList()[idx];
      if (!IdentifierEquals(InferSelectItemOutputName(item, idx), cur_col)) {
        continue;
      }
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue) {
        return;
      }
      const ColumnName& cn = item.expression->AsColumnValue().GetColumnName();
      if (!cn.schema.empty() && !sub_al.empty() &&
          !IdentifierEquals(cn.schema, sub_al)) {
        return;
      }
      mapped_inner = cn.name;
      break;
    }
    if (mapped_inner.empty()) {
      return;
    }
    cur_col = std::move(mapped_inner);
    u_stmt = u_stmt->Sources()[0].query.get();
  }
  if (u_stmt == nullptr ||
      (u_stmt->UnionAll().empty() &&
       u_stmt->GetSetOperationTree() == nullptr) ||
      u_stmt->UnionByName() || u_stmt->HasLimit() || u_stmt->Offset() != 0 ||
      !u_stmt->OrderBy().empty() || !u_stmt->WithQueries().empty()) {
    return;
  }
  for (const SetOperationMatch& m : u_stmt->Matches()) {
    if (m.by_name || m.corresponding || !m.columns.empty()) {
      return;
    }
  }
  for (SetOperationKind k : u_stmt->SetOperationKinds()) {
    if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
      return;
    }
  }
  if (const auto& tree = u_stmt->GetSetOperationTree(); tree != nullptr) {
    for (SetOperationKind k : tree->kinds) {
      if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
        return;
      }
    }
  }

  std::vector<SelectStatement*> leaves;
  std::unordered_set<SelectStatement*> seen;
  auto collect_leaves = [&](auto& self, SelectStatement* s) -> bool {
    while (
        s != nullptr && s->UnionAll().empty() &&
        s->GetSetOperationTree() == nullptr && s->Sources().size() == 1 &&
        s->Sources()[0].query != nullptr && !s->Sources()[0].unnest &&
        !s->Sources()[0].is_lateral && !s->Sources()[0].sample &&
        s->Sources()[0].using_columns.empty() &&
        !s->Sources()[0].from_nested_join && !s->Sources()[0].join_condition &&
        !s->WhereClause() && s->GroupBy().empty() && !s->IsGroupAll() &&
        !s->Having() && !s->Distinct() && !s->HasDistinctOn() &&
        !s->HasLimit() && s->Offset() == 0 && !s->Qualify() &&
        s->WithQueries().empty() && s->SelectList().size() == 1 &&
        s->SelectList()[0].expression &&
        s->SelectList()[0].expression->Type() == TypeTag::kColumnValue &&
        s->SelectList()[0].expression->AsColumnValue().GetColumnName().name ==
            "*") {
      s = s->Sources()[0].query.get();
    }
    if (s == nullptr) {
      return false;
    }
    if (!seen.insert(s).second) {
      return true;
    }
    if (s->UnionAll().empty() && s->GetSetOperationTree() == nullptr) {
      leaves.push_back(s);
      return true;
    }
    if (const auto& tree = s->GetSetOperationTree(); tree != nullptr) {
      for (SetOperationKind k : tree->kinds) {
        if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
          return false;
        }
      }
      if (!self(self, tree->first.get())) {
        return false;
      }
      for (const auto& br : tree->branches) {
        if (!self(self, br.get())) {
          return false;
        }
      }
    } else {
      for (SetOperationKind k : s->SetOperationKinds()) {
        if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
          return false;
        }
      }
      leaves.push_back(s);
    }
    for (const auto& br : s->UnionAll()) {
      if (!self(self, br.get())) {
        return false;
      }
    }
    return true;
  };
  if (!collect_leaves(collect_leaves, u_stmt) || leaves.empty()) {
    return;
  }

  std::optional<size_t> col_idx;
  for (size_t c = 0; c < leaves[0]->SelectList().size(); ++c) {
    const std::string out_nm =
        ExplicitOrColumnOutputName(leaves[0]->SelectList()[c]);
    if (!out_nm.empty() && IdentifierEquals(out_nm, cur_col)) {
      if (col_idx.has_value()) {
        return;
      }
      col_idx = c;
    }
  }
  if (!col_idx.has_value()) {
    return;
  }

  std::vector<ColumnName> branch_cols;
  branch_cols.reserve(leaves.size());
  for (SelectStatement* br : leaves) {
    // Both the flat UnionAll and the SetOperationTree paths funnel through
    // collect_leaves, so this guard (including ORDER BY, which previously was
    // only checked on the set-op container) must cover every leaf.
    if (!br->GroupBy().empty() || br->IsGroupAll() || br->Having() ||
        br->HasLimit() || br->Offset() != 0 || !br->OrderBy().empty() ||
        br->Qualify() || br->WithTies() || !br->WithQueries().empty() ||
        relational_detail::HasWindowFunctions(*br) ||
        SelectListHasStarOrModifiers(*br) ||
        *col_idx >= br->SelectList().size()) {
      return;
    }
    const Expression& bexpr = br->SelectList()[*col_idx].expression;
    if (!bexpr || bexpr->Type() != TypeTag::kColumnValue) {
      return;
    }
    const ColumnName& bcol = bexpr->AsColumnValue().GetColumnName();
    if (bcol.name.empty() || bcol.name == "*") {
      return;
    }
    size_t hits = 0;
    ColumnName resolved_bcol = bcol;
    for (const SelectSource& bs : br->Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.unnest ||
          bs.is_lateral) {
        return;
      }
      StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
      if (!btbl.HasValue()) {
        return;
      }
      const std::string balias = bs.alias.empty() ? bs.table : bs.alias;
      if (!bcol.schema.empty() && !IdentifierEquals(bcol.schema, balias) &&
          !IdentifierEquals(bcol.schema, bs.table)) {
        continue;
      }
      const Schema& bschema = btbl.Value()->GetSchema();
      for (size_t i = 0; i < bschema.ColumnCount(); ++i) {
        if (IdentifierEquals(bschema.GetColumn(i).Name().name, bcol.name)) {
          if (bs.join_type != JoinType::kCross &&
              bs.join_type != JoinType::kInner) {
            return;
          }
          ++hits;
          resolved_bcol = ColumnName(balias, bschema.GetColumn(i).Name().name);
          break;
        }
      }
    }
    if (hits != 1) {
      return;
    }
    branch_cols.push_back(std::move(resolved_bcol));
  }

  for (size_t i = 0; i < leaves.size(); ++i) {
    Expression pred =
        closed_range.has_value()
            ? BinaryExpressionExp(
                  BinaryExpressionExp(ColumnValueExp(branch_cols[i]),
                                      BinaryOperation::kGreaterThanEquals,
                                      ConstantValueExp(closed_range->first)),
                  BinaryOperation::kAnd,
                  BinaryExpressionExp(ColumnValueExp(branch_cols[i]),
                                      BinaryOperation::kLessThanEquals,
                                      ConstantValueExp(closed_range->second)))
            : MakeValueDomainPredicate(branch_cols[i], domain_vals);
    Expression new_where;
    if (leaves[i]->WhereClause()) {
      new_where = BinaryExpressionExp(leaves[i]->WhereClause(),
                                      BinaryOperation::kAnd, std::move(pred));
    } else {
      new_where = std::move(pred);
    }
    leaves[i]->SetWhereClause(std::move(new_where));
  }
  const auto& tree = u_stmt->GetSetOperationTree();
  if (tree != nullptr && !tree->grouped && tree->first != nullptr &&
      tree->first->UnionAll().empty() &&
      tree->first->GetSetOperationTree() == nullptr) {
    SyncSetOpHeadFields(u_stmt, *tree->first);
  }
}

void PushDomainIntoUnionDerivedIfSafe(SelectStatement* outer_with,
                                      SelectStatement* grouped_stmt,
                                      const ColumnName& gk_col,
                                      const std::vector<Value>& domain_vals,
                                      TransactionContext& ctx) {
  auto resolve_single_use_union_target =
      [&](const SelectSource& src) -> SelectStatement* {
    if (src.query != nullptr) {
      return src.query.get();
    }
    if (outer_with == nullptr || src.table.empty() ||
        outer_with->IsRecursiveWith(src.table) ||
        outer_with->RecursiveDepthOf(src.table) != nullptr) {
      return nullptr;
    }
    const auto wit = outer_with->WithQueries().find(src.table);
    if (wit == outer_with->WithQueries().end() || wit->second == nullptr ||
        CountNestedCteReferences(*outer_with, src.table, true, true) != 1) {
      return nullptr;
    }
    return wit->second.get();
  };

  if (grouped_stmt->Sources().size() == 1) {
    const SelectSource& u_src = grouped_stmt->Sources()[0];
    SelectStatement* u_target = resolve_single_use_union_target(u_src);
    const std::string ualias = u_src.alias.empty() ? u_src.table : u_src.alias;
    if (u_target == nullptr || u_src.unnest || u_src.is_lateral ||
        u_src.sample || !u_src.using_columns.empty() ||
        u_src.from_nested_join || u_src.join_condition) {
      return;
    }
    if (!gk_col.schema.empty() && !ualias.empty() &&
        !IdentifierEquals(gk_col.schema, ualias) &&
        (u_src.table.empty() ||
         !IdentifierEquals(gk_col.schema, u_src.table))) {
      return;
    }
    PushPredicateIntoUnionQueryIfSafe(u_target, gk_col.name, domain_vals,
                                      std::nullopt, ctx);
    return;
  }
  if (grouped_stmt->Sources().size() != 2) {
    return;
  }
  // 2-source grouped CTE `FROM union_sub u, dim d WHERE d.pk = u.fk AND d.gk IN
  // (...)` (e.g. TPC-DS Q02 `wswscs`): derive `[min_pk, max_pk]` on `d` and
  // push the SARGable range filter into `u`'s UNION branches.
  size_t u_si = 2;
  size_t d_si = 2;
  SelectStatement* u_target = nullptr;
  for (size_t si = 0; si < 2; ++si) {
    const SelectSource& s = grouped_stmt->Sources()[si];
    if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return;
    }
    if (SelectStatement* cand = resolve_single_use_union_target(s);
        cand != nullptr && u_si >= 2) {
      u_si = si;
      u_target = cand;
    } else if (!s.table.empty() &&
               (outer_with == nullptr ||
                !outer_with->WithQueries().contains(s.table))) {
      d_si = si;
    }
  }
  if (u_si >= 2 || d_si >= 2 || u_target == nullptr) {
    return;
  }
  const SelectSource& u_src = grouped_stmt->Sources()[u_si];
  const SelectSource& d_src = grouped_stmt->Sources()[d_si];
  StatusOr<std::shared_ptr<Table>> dtbl_or = ctx.GetTable(d_src.table);
  if (!dtbl_or.HasValue() || dtbl_or.Value() == nullptr ||
      dtbl_or.Value()->BackingColumnarRowCount() > 100000) {
    return;
  }
  const Schema& dschema = dtbl_or.Value()->GetSchema();
  const std::string dalias = d_src.alias.empty() ? d_src.table : d_src.alias;
  const std::string ualias = u_src.alias.empty() ? u_src.table : u_src.alias;
  if ((!gk_col.schema.empty() && !IdentifierEquals(gk_col.schema, dalias) &&
       !IdentifierEquals(gk_col.schema, d_src.table)) ||
      dschema.Offset(ColumnName("", gk_col.name)) < 0) {
    return;
  }
  std::vector<Expression> all_conjs =
      SplitConjuncts(grouped_stmt->WhereClause());
  all_conjs.reserve(all_conjs.size() + grouped_stmt->Sources().size());
  for (const SelectSource& gs : grouped_stmt->Sources()) {
    if (gs.join_condition) {
      for (Expression& jc : SplitConjuncts(gs.join_condition)) {
        all_conjs.push_back(std::move(jc));
      }
    }
  }
  std::string dim_pk_col;
  std::string union_fk_col;
  for (const Expression& conj : all_conjs) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const Expression& l = conj->AsBinaryExpression().Left();
    const Expression& r = conj->AsBinaryExpression().Right();
    if (!l || !r || l->Type() != TypeTag::kColumnValue ||
        r->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lc = l->AsColumnValue().GetColumnName();
    const ColumnName& rc = r->AsColumnValue().GetColumnName();
    auto is_dim_int_col = [&](const ColumnName& cn, std::string* out) -> bool {
      if (!cn.schema.empty() && !IdentifierEquals(cn.schema, dalias) &&
          !IdentifierEquals(cn.schema, d_src.table)) {
        return false;
      }
      const int off = dschema.Offset(ColumnName("", cn.name));
      if (off < 0) {
        return false;
      }
      const Column& cm = dschema.GetColumn(static_cast<size_t>(off));
      if (cm.IsUnsigned() ||
          (cm.Type() != ValueType::kInt64 && cm.Type() != ValueType::kDate)) {
        return false;
      }
      *out = cm.Name().name;
      return true;
    };
    auto is_union_col = [&](const ColumnName& cn, std::string* out) -> bool {
      if (!cn.schema.empty() && !ualias.empty() &&
          !IdentifierEquals(cn.schema, ualias) &&
          (u_src.table.empty() || !IdentifierEquals(cn.schema, u_src.table))) {
        return false;
      }
      if (cn.schema.empty() && dschema.Offset(ColumnName("", cn.name)) >= 0) {
        return false;
      }
      *out = cn.name;
      return !out->empty();
    };
    std::string d_cand;
    std::string u_cand;
    if (is_dim_int_col(lc, &d_cand) && is_union_col(rc, &u_cand)) {
      dim_pk_col = std::move(d_cand);
      union_fk_col = std::move(u_cand);
      break;
    }
    if (is_dim_int_col(rc, &d_cand) && is_union_col(lc, &u_cand)) {
      dim_pk_col = std::move(d_cand);
      union_fk_col = std::move(u_cand);
      break;
    }
  }
  if (dim_pk_col.empty() || union_fk_col.empty()) {
    return;
  }
  SelectStatement d_bounds;
  SelectSource dscan;
  dscan.table = d_src.table;
  dscan.alias = dalias;
  d_bounds.SetSources({std::move(dscan)});
  d_bounds.SetWhereClause(
      MakeValueDomainPredicate(ColumnName(dalias, gk_col.name), domain_vals));
  const Expression pk_exp = ColumnValueExp(ColumnName(dalias, dim_pk_col));
  d_bounds.SetSelectList({
      NamedExpression(
          "__lo", AggregateExpressionExp(AggregationType::kMin, pk_exp, false)),
      NamedExpression(
          "__hi", AggregateExpressionExp(AggregationType::kMax, pk_exp, false)),
  });
  StatusOr<relational_detail::Relation> brel = relational_detail::ExecuteQuery(
      ctx, d_bounds, nullptr, {}, nullptr, false);
  if (brel.HasValue() && brel.Value().HasColumnarOnly()) {
    if (brel.Value().EnsureRowsMaterialized() != Status::kSuccess) {
      return;
    }
  }
  if (!brel.HasValue() || brel.Value().HasSpill() ||
      brel.Value().rows.size() != 1 || brel.Value().rows[0].Count() != 2 ||
      brel.Value().rows[0][0].IsNull() || brel.Value().rows[0][1].IsNull()) {
    return;
  }
  PushPredicateIntoUnionQueryIfSafe(
      u_target, union_fk_col, {},
      std::pair{brel.Value().rows[0][0], brel.Value().rows[0][1]}, ctx);
}

bool CollectSharedGroupedCteSitesInConsumer(  // NOLINT(misc-no-recursion)
    SelectStatement* consumer, const std::string& name,
    std::vector<SharedCteSite>* sites) {
  if (consumer == nullptr || !consumer->UnionAll().empty() ||
      consumer->GetSetOperationTree() != nullptr) {
    return false;
  }
  size_t non_src_refs = 0;
  CountNestedCteReferencesInExpression(consumer->WhereClause(), name,
                                       &non_src_refs);
  for (const NamedExpression& item : consumer->SelectList()) {
    CountNestedCteReferencesInExpression(item.expression, name, &non_src_refs);
  }
  for (const Expression& gk : consumer->GroupBy()) {
    CountNestedCteReferencesInExpression(gk, name, &non_src_refs);
  }
  CountNestedCteReferencesInExpression(consumer->Having(), name, &non_src_refs);
  CountNestedCteReferencesInExpression(consumer->Qualify(), name,
                                       &non_src_refs);
  for (const auto& ot : consumer->OrderBy()) {
    CountNestedCteReferencesInExpression(ot.expression, name, &non_src_refs);
  }
  if (non_src_refs != 0) {
    return false;
  }
  for (const SelectSource& source : consumer->Sources()) {
    size_t cond_refs = 0;
    CountNestedCteReferencesInExpression(source.join_condition, name,
                                         &cond_refs);
    CountNestedCteReferencesInExpression(source.unnest, name, &cond_refs);
    if (cond_refs != 0) {
      return false;
    }
    if (source.query != nullptr) {
      if (CountNestedCteReferences(*source.query, name, true, true) == 0) {
        continue;
      }
      if (source.unnest || source.is_lateral || source.sample.has_value() ||
          !source.using_columns.empty() || source.from_nested_join ||
          source.query->WithQueries().contains(name) ||
          (source.join_type != JoinType::kCross &&
           source.join_type != JoinType::kInner)) {
        return false;
      }
      if (!CollectSharedGroupedCteSitesInConsumer(source.query.get(), name,
                                                  sites)) {
        return false;
      }
      continue;
    }
    if (source.table != name) {
      continue;
    }
    if (source.unnest || source.is_lateral || source.sample.has_value() ||
        (source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kInner)) {
      return false;
    }
    sites->push_back(
        SharedCteSite{.consumer = consumer,
                      .alias = source.alias.empty() ? name : source.alias});
  }
  return true;
}

bool CollectSharedGroupedCteSites(SelectStatement* outer,
                                  const std::string& name,
                                  std::vector<SharedCteSite>* sites) {
  if (!CollectSharedGroupedCteSitesInConsumer(outer, name, sites)) {
    return false;
  }
  return std::ranges::all_of(outer->WithQueries(), [&](const auto& item) {
    const auto& [entry, body] = item;
    if (entry == name || body == nullptr) {
      return true;
    }
    if (CountNestedCteReferences(*body, name, true, true) == 0) {
      return true;
    }
    return !body->WithQueries().contains(name) &&
           CollectSharedGroupedCteSitesInConsumer(body.get(), name, sites);
  });
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (shared_grouped_union_distinct_cte_disjunctive_pushdown_sound)
bool PushSharedGroupedCteKeyRestrictions(SelectStatement* outer,
                                         TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      TpcdsStmtHasParam(*outer)) {
    return false;
  }
  bool any_pushed = false;
  std::vector<std::string> cte_names = outer->WithQueryOrder();
  for (const std::string& name : cte_names) {
    if (outer->IsRecursiveWith(name) ||
        outer->RecursiveDepthOf(name) != nullptr) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    SelectStatement& body = *cte_it->second;
    if (TpcdsStmtHasParam(body) || !body.WithQueries().empty()) {
      continue;
    }

    std::vector<SharedCteSite> sites;
    if (!CollectSharedGroupedCteSites(outer, name, &sites) ||
        sites.size() < 2) {
      continue;
    }
    bool consumers_safe = true;
    for (const SharedCteSite& site : sites) {
      if (!site.consumer->UnionAll().empty() ||
          site.consumer->GetSetOperationTree() != nullptr ||
          TpcdsStmtHasParam(*site.consumer)) {
        consumers_safe = false;
        break;
      }
      for (const SelectSource& cs : site.consumer->Sources()) {
        if (cs.join_type == JoinType::kRight ||
            cs.join_type == JoinType::kFull) {
          consumers_safe = false;
          break;
        }
      }
      if (!consumers_safe) {
        break;
      }
    }
    if (!consumers_safe) {
      continue;
    }

    SelectStatement* grouped_target = nullptr;
    std::unordered_map<std::string, ColumnName> cte_out_to_group_col;
    if (!body.GroupBy().empty()) {
      if (ValidateGroupedCteTarget(&body, &cte_out_to_group_col)) {
        grouped_target = &body;
      }
    } else if (!body.Having() && !body.Distinct() && !body.HasDistinctOn() &&
               !body.HasLimit() && body.Offset() == 0 && !body.Qualify() &&
               !body.WithTies() && body.UnionAll().empty() &&
               body.GetSetOperationTree() == nullptr &&
               !relational_detail::HasWindowFunctions(body) &&
               !SelectListHasStarOrModifiers(body) &&
               body.Sources().size() == 1 &&
               (!body.WhereClause() ||
                (!ContainsQueryExpression(body.WhereClause()) &&
                 !relational_detail::ContainsAggregate(body.WhereClause()) &&
                 DerivedExpressionIsImmutable(body.WhereClause())))) {
      const SelectSource& s0 = body.Sources()[0];
      const bool clean_select = std::ranges::all_of(
          body.SelectList(), [](const NamedExpression& item) {
            return item.expression != nullptr &&
                   !ContainsQueryExpression(item.expression) &&
                   !relational_detail::ContainsAggregate(item.expression) &&
                   DerivedExpressionIsImmutable(item.expression);
          });
      if (clean_select && s0.query != nullptr && !s0.alias.empty() &&
          !s0.unnest && !s0.is_lateral && !s0.sample &&
          s0.using_columns.empty() && !s0.from_nested_join &&
          !s0.join_condition) {
        std::unordered_map<std::string, ColumnName> inner_map;
        if (ValidateGroupedCteTarget(s0.query.get(), &inner_map)) {
          for (size_t idx = 0; idx < body.SelectList().size(); ++idx) {
            const NamedExpression& item = body.SelectList()[idx];
            if (item.expression->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName& col =
                item.expression->AsColumnValue().GetColumnName();
            if (!col.schema.empty() &&
                !IdentifierEquals(col.schema, s0.alias)) {
              continue;
            }
            if (const auto it = inner_map.find(LoweredStarName(col.name));
                it != inner_map.end()) {
              cte_out_to_group_col.emplace(
                  LoweredStarName(InferSelectItemOutputName(item, idx)),
                  it->second);
            }
          }
          if (!cte_out_to_group_col.empty()) {
            grouped_target = s0.query.get();
          }
        }
      }
    }
    if (grouped_target == nullptr || cte_out_to_group_col.empty()) {
      continue;
    }

    std::vector<std::string> candidate_keys;
    candidate_keys.reserve(cte_out_to_group_col.size());
    for (const auto& [k, v] : cte_out_to_group_col) {
      (void)v;
      candidate_keys.push_back(k);
    }
    std::ranges::sort(candidate_keys);

    for (const std::string& out_low : candidate_keys) {
      const ColumnName& gk_col = cte_out_to_group_col.at(out_low);
      std::vector<Value> union_vals;
      bool all_sites_bound = true;

      auto matches_site_col = [&](const Expression& e,
                                  const SharedCteSite& site) -> bool {
        if (!e || e->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& cn = e->AsColumnValue().GetColumnName();
        if (!IdentifierEquals(cn.name, out_low)) {
          return false;
        }
        if (!cn.schema.empty()) {
          return IdentifierEquals(cn.schema, site.alias);
        }
        return site.consumer->Sources().size() == 1;
      };

      for (const SharedCteSite& site : sites) {
        std::vector<Value> site_vals;
        for (const Expression& conj :
             SplitConjuncts(site.consumer->WhereClause())) {
          if (!conj) {
            continue;
          }
          if (conj->Type() == TypeTag::kBinaryExp &&
              conj->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
            const Expression& lhs = conj->AsBinaryExpression().Left();
            const Expression& rhs = conj->AsBinaryExpression().Right();
            std::optional<Value> cv;
            if (matches_site_col(lhs, site)) {
              cv = TpcdsEvalClosedConstant(rhs);
            } else if (matches_site_col(rhs, site)) {
              cv = TpcdsEvalClosedConstant(lhs);
            }
            if (cv.has_value() && IsSafeDisjointTagValue(*cv)) {
              site_vals.push_back(std::move(*cv));
              break;
            }
          } else if (conj->Type() == TypeTag::kInExp) {
            const auto& in_exp = conj->AsInExpression();
            if (matches_site_col(in_exp.child_, site) &&
                !in_exp.list_.empty() && in_exp.list_.size() <= 32) {
              std::vector<Value> parsed;
              bool ok = true;
              for (const Expression& item_e : in_exp.list_) {
                std::optional<Value> cv = TpcdsEvalClosedConstant(item_e);
                if (!cv.has_value() || !IsSafeDisjointTagValue(*cv) ||
                    (!parsed.empty() && cv->type != parsed.front().type)) {
                  ok = false;
                  break;
                }
                parsed.push_back(std::move(*cv));
              }
              if (ok && !parsed.empty()) {
                site_vals = std::move(parsed);
                break;
              }
            }
          }
        }
        if (site_vals.empty() && site.consumer->Sources().size() == 2) {
          // Extract domain from an inner-joined selective dimension table
          // (e.g. TPC-DS Q02 `FROM wswscs, date_dim WHERE d_week_seq = ... AND
          // d_year = 2001`).
          size_t dim_si = 2;
          for (size_t si = 0; si < 2; ++si) {
            const SelectSource& s = site.consumer->Sources()[si];
            const std::string al = s.alias.empty() ? s.table : s.alias;
            if (!IdentifierEquals(al, site.alias) && !s.table.empty() &&
                s.query == nullptr && !s.unnest && !s.is_lateral && !s.sample &&
                s.using_columns.empty() && !s.from_nested_join &&
                (s.join_type == JoinType::kCross ||
                 s.join_type == JoinType::kInner)) {
              dim_si = si;
            }
          }
          if (dim_si < 2) {
            const SelectSource& dsrc = site.consumer->Sources()[dim_si];
            StatusOr<std::shared_ptr<Table>> dtbl_or = ctx.GetTable(dsrc.table);
            if (dtbl_or.HasValue() && dtbl_or.Value() != nullptr &&
                dtbl_or.Value()->BackingColumnarRowCount() <= 100000) {
              const Schema& dschema = dtbl_or.Value()->GetSchema();
              const std::string dalias =
                  dsrc.alias.empty() ? dsrc.table : dsrc.alias;
              std::string matched_dim_col;
              std::vector<Expression> dim_local_preds;
              std::vector<Expression> site_conjs =
                  SplitConjuncts(site.consumer->WhereClause());
              site_conjs.reserve(site_conjs.size() +
                                 site.consumer->Sources().size());
              for (const SelectSource& cs : site.consumer->Sources()) {
                if (cs.join_condition) {
                  for (Expression& jc : SplitConjuncts(cs.join_condition)) {
                    site_conjs.push_back(std::move(jc));
                  }
                }
              }
              for (const Expression& conj : site_conjs) {
                if (!conj || ContainsQueryExpression(conj) ||
                    relational_detail::ContainsAggregate(conj) ||
                    !DerivedExpressionIsImmutable(conj)) {
                  continue;
                }
                if (conj->Type() == TypeTag::kBinaryExp &&
                    conj->AsBinaryExpression().Op() ==
                        BinaryOperation::kEquals) {
                  const Expression& l = conj->AsBinaryExpression().Left();
                  const Expression& r = conj->AsBinaryExpression().Right();
                  auto extract_dcol =
                      [&](const Expression& side) -> std::string {
                    if (!side || side->Type() != TypeTag::kColumnValue) {
                      return {};
                    }
                    const ColumnName& cn =
                        side->AsColumnValue().GetColumnName();
                    if (!cn.schema.empty() &&
                        !IdentifierEquals(cn.schema, dalias) &&
                        !IdentifierEquals(cn.schema, dsrc.table)) {
                      return {};
                    }
                    const int doff = dschema.Offset(ColumnName("", cn.name));
                    return doff >= 0
                               ? dschema.GetColumn(static_cast<size_t>(doff))
                                     .Name()
                                     .name
                               : std::string{};
                  };
                  if (matches_site_col(l, site)) {
                    std::string dc = extract_dcol(r);
                    if (!dc.empty()) {
                      matched_dim_col = std::move(dc);
                      continue;
                    }
                  } else if (matches_site_col(r, site)) {
                    std::string dc = extract_dcol(l);
                    if (!dc.empty()) {
                      matched_dim_col = std::move(dc);
                      continue;
                    }
                  }
                }
                bool only_dim = true;
                bool any_dim = false;
                for (const ColumnName& cn : conj->TouchedColumns()) {
                  if (!cn.schema.empty()) {
                    if (IdentifierEquals(cn.schema, dalias) ||
                        IdentifierEquals(cn.schema, dsrc.table)) {
                      any_dim = true;
                    } else {
                      only_dim = false;
                      break;
                    }
                  } else if (dschema.Offset(ColumnName("", cn.name)) >= 0 &&
                             !IdentifierEquals(cn.name, out_low)) {
                    any_dim = true;
                  } else {
                    only_dim = false;
                    break;
                  }
                }
                if (only_dim && any_dim) {
                  dim_local_preds.push_back(conj);
                }
              }
              if (!matched_dim_col.empty() && !dim_local_preds.empty()) {
                SelectStatement dquery;
                SelectSource dscan;
                dscan.table = dsrc.table;
                dscan.alias = dalias;
                dquery.SetSources({std::move(dscan)});
                dquery.SetWhereClause(CombineConjuncts(dim_local_preds));
                dquery.SetDistinct(true);
                dquery.SetSelectList({NamedExpression(
                    "__dk",
                    ColumnValueExp(ColumnName(dalias, matched_dim_col)))});
                StatusOr<relational_detail::Relation> drel_or =
                    relational_detail::ExecuteQuery(ctx, dquery, nullptr, {},
                                                    nullptr, false);
                if (drel_or.HasValue() && drel_or.Value().HasColumnarOnly()) {
                  if (drel_or.Value().EnsureRowsMaterialized() !=
                      Status::kSuccess) {
                    return false;
                  }
                }
                if (drel_or.HasValue() && !drel_or.Value().HasSpill() &&
                    drel_or.Value().TotalRows() > 0 &&
                    drel_or.Value().TotalRows() <= 128) {
                  std::vector<Value> extracted;
                  bool ext_ok = true;
                  for (const Row& r : drel_or.Value().rows) {
                    if (r.Count() != 1 || r[0].IsNull()) {
                      continue;
                    }
                    if (!IsSafeDisjointTagValue(r[0]) ||
                        (!extracted.empty() &&
                         r[0].type != extracted.front().type)) {
                      ext_ok = false;
                      break;
                    }
                    extracted.push_back(r[0]);
                  }
                  if (ext_ok && !extracted.empty()) {
                    site_vals = std::move(extracted);
                  }
                }
              }
            }
          }
        }
        if (site_vals.empty()) {
          all_sites_bound = false;
          break;
        }
        for (const Value& sv : site_vals) {
          if (!union_vals.empty() && sv.type != union_vals.front().type) {
            all_sites_bound = false;
            break;
          }
          const bool dup = std::ranges::any_of(
              union_vals, [&](const Value& u) { return u == sv; });
          if (!dup) {
            union_vals.push_back(sv);
          }
        }
        if (!all_sites_bound || union_vals.size() > 256) {
          all_sites_bound = false;
          break;
        }
      }
      if (!all_sites_bound || union_vals.empty()) {
        continue;
      }

      // Verify `gk_col` resolves to a non-null-supplying source of
      // `grouped_target`.
      bool gk_safe_in_target = false;
      size_t gk_hits = 0;
      ColumnName resolved_gk_col = gk_col;
      for (const SelectSource& gs : grouped_target->Sources()) {
        if (gs.unnest || gs.is_lateral || gs.from_nested_join ||
            !gs.using_columns.empty()) {
          gk_hits = 2;
          break;
        }
        const std::string galias = gs.alias.empty() ? gs.table : gs.alias;
        if (!gk_col.schema.empty() &&
            !IdentifierEquals(gk_col.schema, galias) &&
            (gs.table.empty() || !IdentifierEquals(gk_col.schema, gs.table))) {
          continue;
        }
        bool has_col = false;
        std::string matched_col_name = gk_col.name;
        if (gs.query != nullptr) {
          const SelectStatement* qhead = gs.query.get();
          if (qhead->SelectList().empty() &&
              qhead->GetSetOperationTree() != nullptr &&
              qhead->GetSetOperationTree()->first != nullptr) {
            qhead = qhead->GetSetOperationTree()->first.get();
          }
          for (const NamedExpression& si : qhead->SelectList()) {
            const std::string nm = ExplicitOrColumnOutputName(si);
            if (!nm.empty() && IdentifierEquals(nm, gk_col.name)) {
              has_col = true;
              matched_col_name = nm;
              break;
            }
          }
        } else if (!gs.table.empty()) {
          if (const auto gcte_it = outer->WithQueries().find(gs.table);
              gcte_it != outer->WithQueries().end()) {
            if (outer->IsRecursiveWith(gs.table) ||
                gcte_it->second == nullptr) {
              gk_hits = 2;
              break;
            }
            const SelectStatement* qhead = gcte_it->second.get();
            if (qhead->SelectList().empty() &&
                qhead->GetSetOperationTree() != nullptr &&
                qhead->GetSetOperationTree()->first != nullptr) {
              qhead = qhead->GetSetOperationTree()->first.get();
            }
            for (const NamedExpression& si : qhead->SelectList()) {
              const std::string nm = ExplicitOrColumnOutputName(si);
              if (!nm.empty() && IdentifierEquals(nm, gk_col.name)) {
                has_col = true;
                matched_col_name = nm;
                break;
              }
            }
          } else {
            StatusOr<std::shared_ptr<Table>> gtbl = ctx.GetTable(gs.table);
            if (!gtbl.HasValue()) {
              gk_hits = 2;
              break;
            }
            const Schema& gschema = gtbl.Value()->GetSchema();
            for (size_t i = 0; i < gschema.ColumnCount(); ++i) {
              if (IdentifierEquals(gschema.GetColumn(i).Name().name,
                                   gk_col.name)) {
                has_col = true;
                matched_col_name = gschema.GetColumn(i).Name().name;
                break;
              }
            }
          }
        }
        if (has_col) {
          if (gs.join_type != JoinType::kCross &&
              gs.join_type != JoinType::kInner) {
            gk_hits = 2;
            break;
          }
          ++gk_hits;
          gk_safe_in_target = true;
          resolved_gk_col = ColumnName(galias, matched_col_name);
        }
      }
      if (!gk_safe_in_target || gk_hits != 1) {
        continue;
      }

      Expression domain_pred =
          MakeValueDomainPredicate(resolved_gk_col, union_vals);
      grouped_target->SetWhereClause(
          grouped_target->WhereClause()
              ? BinaryExpressionExp(grouped_target->WhereClause(),
                                    BinaryOperation::kAnd,
                                    std::move(domain_pred))
              : std::move(domain_pred));
      PushDomainIntoUnionDerivedIfSafe(outer, grouped_target, resolved_gk_col,
                                       union_vals, ctx);
      any_pushed = true;
    }
  }
  return any_pushed;
}

namespace {

bool RewriteOneDistinctBranchSurrogateKeys(SelectStatement* br,
                                           TransactionContext& ctx) {
  if (br == nullptr || !br->Distinct() || br->HasDistinctOn() ||
      !br->GroupBy().empty() || br->IsGroupAll() || br->Having() ||
      br->Qualify() || br->HasLimit() || br->Offset() != 0 ||
      !br->OrderBy().empty() || br->WithTies() || br->ForUpdate() ||
      br->AsStruct() || !br->WithQueries().empty() || !br->UnionAll().empty() ||
      br->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*br) ||
      SelectListHasStarOrModifiers(*br) || TpcdsStmtHasParam(*br) ||
      br->Sources().size() < 2 || br->SelectList().empty()) {
    return false;
  }

  std::vector<std::shared_ptr<Table>> holders;
  std::vector<GroupingRelation> rels;
  std::vector<size_t> est_rows;
  std::unordered_set<std::string> seen_aliases;
  holders.reserve(br->Sources().size());
  rels.reserve(br->Sources().size());
  est_rows.reserve(br->Sources().size());

  for (const SelectSource& s : br->Sources()) {
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample || !s.using_columns.empty() || s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(s.table);
    if (!tbl.HasValue()) {
      return false;
    }
    const std::string alias = s.alias.empty() ? s.table : s.alias;
    if (!seen_aliases.insert(LoweredStarName(alias)).second) {
      return false;
    }
    size_t rows = 0;
    if (StatusOr<std::shared_ptr<TableStatistics>> st = ctx.GetStats(s.table);
        st.HasValue() && st.Value() != nullptr) {
      rows = st.Value()->Rows();
    }
    holders.push_back(tbl.MoveValue());
    rels.push_back({.name = alias, .schema = &holders.back()->GetSchema()});
    est_rows.push_back(rows);
  }

  struct ResolvedCol {
    size_t rel_idx{0};
    std::string alias;
    std::string exact_col;
    ValueType type{ValueType::kNull};
    // Name mirrors what is actually stored: an UNSIGNED or INTERVAL integer
    // domain. It is NOT a collation flag (Column has no collation API here);
    // it marks keys this rewrite does not treat as fixed signed scalars.
    bool has_unsigned_or_interval{false};
  };
  auto resolve_col = [&](const ColumnName& col) -> std::optional<ResolvedCol> {
    if (col.name.empty() || col.name == "*") {
      return std::nullopt;
    }
    std::optional<ResolvedCol> found;
    for (size_t r = 0; r < rels.size(); ++r) {
      const SelectSource& src = br->Sources()[r];
      if (!col.schema.empty() && !IdentifierEquals(col.schema, rels[r].name) &&
          !IdentifierEquals(col.schema, src.table)) {
        continue;
      }
      const Schema& schema = *rels[r].schema;
      for (size_t c = 0; c < schema.ColumnCount(); ++c) {
        const Column& sc = schema.GetColumn(c);
        if (IdentifierEquals(sc.Name().name, col.name)) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = ResolvedCol{
              .rel_idx = r,
              .alias = rels[r].name,
              .exact_col = sc.Name().name,
              .type = sc.Type(),
              .has_unsigned_or_interval = sc.IsUnsigned() || sc.IsInterval(),
          };
          break;
        }
      }
    }
    return found;
  };

  auto qualify_expr = [&](auto& self,
                          const Expression& expr) -> std::optional<Expression> {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kQueryExp ||
        expr->Type() == TypeTag::kWindowFunctionExp ||
        expr->Type() == TypeTag::kAggregateExp) {
      return std::nullopt;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      std::optional<ResolvedCol> rc =
          resolve_col(expr->AsColumnValue().GetColumnName());
      if (!rc.has_value()) {
        return std::nullopt;
      }
      return ColumnValueExp(ColumnName(rc->alias, rc->exact_col));
    }
    std::vector<Expression> kids = ExpressionChildren(expr);
    for (Expression& k : kids) {
      std::optional<Expression> qk = self(self, k);
      if (!qk.has_value()) {
        return std::nullopt;
      }
      k = std::move(*qk);
    }
    return WithExpressionChildren(expr, std::move(kids));
  };

  struct SelectSlot {
    std::string out_name;
    ResolvedCol col;
  };
  std::vector<SelectSlot> slots;
  slots.reserve(br->SelectList().size());
  for (size_t i = 0; i < br->SelectList().size(); ++i) {
    const NamedExpression& item = br->SelectList()[i];
    if (!item.expression || item.expression->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::optional<ResolvedCol> rc =
        resolve_col(item.expression->AsColumnValue().GetColumnName());
    if (!rc.has_value()) {
      return false;
    }
    slots.push_back(SelectSlot{
        .out_name = InferSelectItemOutputName(item, i),
        .col = std::move(*rc),
    });
  }

  std::vector<Expression> all_conjuncts;
  auto add_pred = [&](const Expression& pred) -> bool {
    if (!pred) {
      return true;
    }
    if (ContainsQueryExpression(pred) ||
        relational_detail::ContainsAggregate(pred) ||
        !DerivedExpressionIsImmutable(pred)) {
      return false;
    }
    for (const Expression& c : SplitConjuncts(pred)) {
      std::optional<Expression> qc = qualify_expr(qualify_expr, c);
      if (!qc.has_value()) {
        return false;
      }
      all_conjuncts.push_back(std::move(*qc));
    }
    return true;
  };
  if (!add_pred(br->WhereClause())) {
    return false;
  }
  for (const SelectSource& s : br->Sources()) {
    if (!add_pred(s.join_condition)) {
      return false;
    }
  }
  if (all_conjuncts.empty()) {
    return false;
  }

  for (size_t dim_idx = 0; dim_idx < rels.size(); ++dim_idx) {
    if (est_rows[dim_idx] < 10000) {
      continue;
    }
    size_t max_core_rows = 0;
    for (size_t i = 0; i < rels.size(); ++i) {
      if (i != dim_idx) {
        max_core_rows = std::max(max_core_rows, est_rows[i]);
      }
    }
    if (max_core_rows < 50000) {
      continue;
    }

    size_t dim_varchar_slots = 0;
    bool core_slots_fixed_scalar = true;
    for (const SelectSlot& sl : slots) {
      if (sl.col.rel_idx == dim_idx) {
        if (sl.col.type == ValueType::kVarChar) {
          ++dim_varchar_slots;
        }
      } else if (sl.col.type == ValueType::kVarChar ||
                 sl.col.type == ValueType::kArray ||
                 sl.col.has_unsigned_or_interval) {
        core_slots_fixed_scalar = false;
        break;
      }
    }
    if (dim_varchar_slots == 0 || !core_slots_fixed_scalar) {
      continue;
    }

    std::vector<Expression> core_conjuncts;
    std::optional<ResolvedCol> core_fk;
    std::optional<ResolvedCol> dim_pk;
    size_t dim_conjunct_count = 0;
    bool ok = true;
    for (const Expression& conj : all_conjuncts) {
      const auto touched = conj->TouchedColumns();
      const bool touches_dim =
          std::ranges::any_of(touched, [&](const ColumnName& cn) {
            return IdentifierEquals(cn.schema, rels[dim_idx].name);
          });
      if (!touches_dim) {
        core_conjuncts.push_back(conj);
        continue;
      }
      ++dim_conjunct_count;
      if (dim_conjunct_count > 1 || conj->Type() != TypeTag::kBinaryExp ||
          conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        ok = false;
        break;
      }
      const Expression& lhs = conj->AsBinaryExpression().Left();
      const Expression& rhs = conj->AsBinaryExpression().Right();
      if (!lhs || !rhs || lhs->Type() != TypeTag::kColumnValue ||
          rhs->Type() != TypeTag::kColumnValue) {
        ok = false;
        break;
      }
      std::optional<ResolvedCol> lrc =
          resolve_col(lhs->AsColumnValue().GetColumnName());
      std::optional<ResolvedCol> rrc =
          resolve_col(rhs->AsColumnValue().GetColumnName());
      if (!lrc.has_value() || !rrc.has_value()) {
        ok = false;
        break;
      }
      if (lrc->rel_idx == dim_idx && rrc->rel_idx != dim_idx) {
        dim_pk = std::move(*lrc);
        core_fk = std::move(*rrc);
      } else if (rrc->rel_idx == dim_idx && lrc->rel_idx != dim_idx) {
        core_fk = std::move(*lrc);
        dim_pk = std::move(*rrc);
      } else {
        ok = false;
        break;
      }
    }
    if (!ok || dim_conjunct_count != 1 || !core_fk.has_value() ||
        !dim_pk.has_value() || core_fk->type != dim_pk->type ||
        (core_fk->type != ValueType::kInt64 &&
         core_fk->type != ValueType::kDate) ||
        core_fk->has_unsigned_or_interval || dim_pk->has_unsigned_or_interval) {
      continue;
    }

    std::vector<SelectSource> pre_sources;
    pre_sources.reserve(br->Sources().size() - 1);
    for (size_t i = 0; i < br->Sources().size(); ++i) {
      if (i == dim_idx) {
        continue;
      }
      SelectSource cs = br->Sources()[i];
      if (cs.alias.empty()) {
        cs.alias = cs.table;
      }
      cs.join_type = JoinType::kCross;
      cs.join_condition = nullptr;
      pre_sources.push_back(std::move(cs));
    }

    // `core_fk = dim_pk` rejects NULL `core_fk`; filtering NULL `core_fk`
    // before GROUP BY avoids hashing unmatched NULL foreign keys.
    core_conjuncts.push_back(UnaryExpressionExp(
        ColumnValueExp(ColumnName(core_fk->alias, core_fk->exact_col)),
        UnaryOperation::kIsNotNull));

    std::vector<ColumnName> pre_key_cols;
    auto ensure_pre_key = [&](const ColumnName& cn) -> size_t {
      for (size_t k = 0; k < pre_key_cols.size(); ++k) {
        if (IdentifierEquals(pre_key_cols[k].schema, cn.schema) &&
            IdentifierEquals(pre_key_cols[k].name, cn.name)) {
          return k;
        }
      }
      const size_t idx = pre_key_cols.size();
      pre_key_cols.push_back(cn);
      return idx;
    };
    const size_t fk_slot =
        ensure_pre_key(ColumnName(core_fk->alias, core_fk->exact_col));
    std::vector<size_t> slot_pre_idx(slots.size(), 0);
    for (size_t s = 0; s < slots.size(); ++s) {
      if (slots[s].col.rel_idx != dim_idx) {
        slot_pre_idx[s] = ensure_pre_key(
            ColumnName(slots[s].col.alias, slots[s].col.exact_col));
      }
    }

    std::vector<NamedExpression> pre_select;
    std::vector<Expression> pre_group;
    pre_select.reserve(pre_key_cols.size());
    pre_group.reserve(pre_key_cols.size());
    for (size_t k = 0; k < pre_key_cols.size(); ++k) {
      pre_select.emplace_back("__k" + std::to_string(k),
                              ColumnValueExp(pre_key_cols[k]));
      pre_group.push_back(ColumnValueExp(pre_key_cols[k]));
    }

    auto pre_stmt = std::make_shared<SelectStatement>();
    pre_stmt->SetSources(std::move(pre_sources));
    pre_stmt->SetWhereClause(CombineConjuncts(core_conjuncts));
    pre_stmt->SetGroupBy(std::move(pre_group));
    pre_stmt->SetSelectList(std::move(pre_select));

    SelectSource pre_src("", "__tpcds_sk_pre", std::move(pre_stmt),
                         JoinType::kCross, nullptr);
    SelectSource dim_src = br->Sources()[dim_idx];
    if (dim_src.alias.empty()) {
      dim_src.alias = dim_src.table;
    }
    dim_src.join_type = JoinType::kCross;
    dim_src.join_condition = nullptr;

    std::vector<NamedExpression> outer_select;
    outer_select.reserve(slots.size());
    for (size_t s = 0; s < slots.size(); ++s) {
      if (slots[s].col.rel_idx == dim_idx) {
        outer_select.emplace_back(
            slots[s].out_name,
            ColumnValueExp(ColumnName(dim_src.alias, slots[s].col.exact_col)));
      } else {
        outer_select.emplace_back(
            slots[s].out_name,
            ColumnValueExp(ColumnName("__tpcds_sk_pre",
                                      "__k" + IntegerToName(slot_pre_idx[s]))));
      }
    }

    br->SetSources({std::move(pre_src), std::move(dim_src)});
    br->SetWhereClause(BinaryExpressionExp(
        ColumnValueExp(
            ColumnName("__tpcds_sk_pre", "__k" + std::to_string(fk_slot))),
        BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(dim_pk->alias, dim_pk->exact_col))));
    br->SetSelectList(std::move(outer_select));
    br->SetDistinct(true);
    return true;
  }
  return false;
}

bool PushDistinctSurrogateKeysImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= PushDistinctSurrogateKeysImpl(body.get(), ctx, visited);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= PushDistinctSurrogateKeysImpl(src.query.get(), ctx, visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return RewriteOneDistinctBranchSurrogateKeys(br, ctx);
    });
  }
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (eager_distinct_surrogate_keys_setop_sound,
//    except_below_non_injective_dim_unsound)
bool PushDistinctSurrogateKeysBeforeDimJoins(SelectStatement* outer,
                                             TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return PushDistinctSurrogateKeysImpl(outer, ctx, &visited);
}

namespace {

template <typename Fn>
Expression ReplaceSubExpressions(  // NOLINT(misc-no-recursion)
    const Expression& expr, Fn&& fn) {
  if (!expr) {
    return expr;
  }
  if (std::optional<Expression> direct = fn(expr); direct.has_value()) {
    return std::move(*direct);
  }
  std::vector<Expression> children = ExpressionChildren(expr);
  bool changed = false;
  for (Expression& child : children) {
    Expression next = ReplaceSubExpressions(child, fn);
    if (next.get() != child.get()) {
      changed = true;
    }
    child = std::move(next);
  }
  Expression rebuilt =
      changed ? WithExpressionChildren(expr, std::move(children)) : expr;
  if (rebuilt && rebuilt->Type() == TypeTag::kAggregateExp) {
    const auto& agg = rebuilt->AsAggregateExpression();
    if (agg.WhereFilter()) {
      Expression next_filter = ReplaceSubExpressions(agg.WhereFilter(), fn);
      if (next_filter.get() != agg.WhereFilter().get()) {
        auto fresh = std::make_shared<AggregateExpression>(
            agg.GetType(), agg.Child(), agg.Distinct());
        fresh->SetIgnoreNulls(agg.IgnoreNulls());
        if (agg.Having() != AggregateHavingModifier::kNone ||
            agg.HavingCondition()) {
          fresh->SetHaving(agg.Having(), agg.HavingCondition());
        }
        if (!agg.InnerOrderBy().empty()) {
          fresh->SetInnerOrderBy(agg.InnerOrderBy());
        }
        if (agg.InnerLimit().has_value()) {
          fresh->SetInnerLimit(agg.InnerLimit());
        }
        if (agg.SecondaryArg()) {
          fresh->SetSecondaryArg(agg.SecondaryArg());
        }
        if (!agg.TrailingArgs().empty()) {
          fresh->SetTrailingArgs(agg.TrailingArgs());
        }
        fresh->SetWhereFilter(std::move(next_filter));
        if (!agg.ArrayElementSqlType().empty()) {
          fresh->SetArrayElementSqlType(agg.ArrayElementSqlType());
        }
        if (agg.HasInnerGroupBy()) {
          fresh->SetInnerGroupBy(agg.InnerGroupBy());
        }
        rebuilt = std::move(fresh);
      }
    }
  }
  return rebuilt;
}

// Unwraps `(SELECT expr AS c1, ..., * FROM base_tbl)` inside FROM so
// downstream grouped/PK rewrites see the base table directly (e.g. Q23 `sq1`).
bool UnwrapStarPlusComputedBaseSubqueries(SelectStatement* stmt,
                                          TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().empty() ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt)) {
    return false;
  }
  bool any = false;
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    SelectSource src = stmt->Sources()[si];
    if (src.query == nullptr || src.alias.empty() || src.unnest ||
        src.is_lateral || src.sample || !src.using_columns.empty() ||
        src.from_nested_join ||
        (src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner)) {
      continue;
    }
    const SelectStatement& sq = *src.query;
    if (sq.Sources().size() != 1 || sq.WhereClause() || !sq.GroupBy().empty() ||
        sq.IsGroupAll() || sq.Having() || sq.Distinct() || sq.HasDistinctOn() ||
        sq.HasLimit() || sq.Offset() != 0 || sq.Qualify() ||
        !sq.OrderBy().empty() || !sq.WithQueries().empty() ||
        !sq.UnionAll().empty() || sq.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(sq) ||
        sq.SelectList().size() < 2) {
      continue;
    }
    const SelectSource& bsrc = sq.Sources()[0];
    if (bsrc.table.empty() || bsrc.query != nullptr || bsrc.unnest ||
        bsrc.is_lateral || bsrc.sample || !bsrc.using_columns.empty() ||
        bsrc.from_nested_join) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bsrc.table);
    if (!btbl.HasValue() || btbl.Value() == nullptr) {
      continue;
    }
    const Schema& bschema = btbl.Value()->GetSchema();
    const std::string balias = bsrc.alias.empty() ? bsrc.table : bsrc.alias;
    const NamedExpression& last_it = sq.SelectList().back();
    if (!last_it.expression ||
        last_it.expression->Type() != TypeTag::kColumnValue ||
        last_it.expression->AsColumnValue().GetColumnName().name != "*") {
      continue;
    }
    std::unordered_map<std::string, Expression> computed_map;
    bool ok = true;
    for (size_t i = 0; i + 1 < sq.SelectList().size(); ++i) {
      const NamedExpression& ci = sq.SelectList()[i];
      if (ci.name.empty() || !ci.expression ||
          ContainsQueryExpression(ci.expression) ||
          relational_detail::ContainsAggregate(ci.expression) ||
          !DerivedExpressionIsImmutable(ci.expression) ||
          bschema.Offset(ColumnName("", ci.name)) >= 0) {
        ok = false;
        break;
      }
      Expression rebound = ReplaceSubExpressions(
          ci.expression,
          [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (!cn.schema.empty() && !IdentifierEquals(cn.schema, balias) &&
                !IdentifierEquals(cn.schema, bsrc.table)) {
              ok = false;
              return std::nullopt;
            }
            const int boff = bschema.Offset(ColumnName("", cn.name));
            if (boff < 0) {
              ok = false;
              return std::nullopt;
            }
            return ColumnValueExp(ColumnName(
                src.alias,
                bschema.GetColumn(static_cast<size_t>(boff)).Name().name));
          });
      if (!ok ||
          !computed_map.emplace(LoweredStarName(ci.name), std::move(rebound))
               .second) {
        ok = false;
        break;
      }
    }
    if (!ok || computed_map.empty()) {
      continue;
    }
    auto subst = [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      return ReplaceSubExpressions(
          expr, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (!cn.schema.empty() && !IdentifierEquals(cn.schema, src.alias)) {
              return std::nullopt;
            }
            const auto it = computed_map.find(LoweredStarName(cn.name));
            return it != computed_map.end()
                       ? std::optional<Expression>(it->second)
                       : std::nullopt;
          });
    };
    std::vector<NamedExpression> new_sel;
    new_sel.reserve(stmt->SelectList().size());
    for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
      new_sel.emplace_back(InferSelectItemOutputName(stmt->SelectList()[i], i),
                           subst(stmt->SelectList()[i].expression));
    }
    std::vector<Expression> new_grp;
    new_grp.reserve(stmt->GroupBy().size());
    for (const Expression& g : stmt->GroupBy()) {
      new_grp.push_back(subst(g));
    }
    std::vector<SelectStatement::OrderByTerm> new_ord = stmt->OrderBy();
    for (auto& t : new_ord) {
      t.expression = subst(t.expression);
    }
    std::vector<SelectSource> new_srcs = stmt->Sources();
    for (SelectSource& s : new_srcs) {
      if (s.join_condition) {
        s.join_condition = subst(s.join_condition);
      }
    }
    new_srcs[si].table = bsrc.table;
    new_srcs[si].query = nullptr;
    stmt->SetSources(std::move(new_srcs));
    stmt->SetSelectList(std::move(new_sel));
    stmt->SetGroupBy(std::move(new_grp));
    stmt->SetOrderBy(std::move(new_ord));
    if (stmt->WhereClause()) {
      stmt->SetWhereClause(subst(stmt->WhereClause()));
    }
    if (stmt->Having()) {
      stmt->SetHaving(subst(stmt->Having()));
    }
    if (stmt->Qualify()) {
      stmt->SetQualify(subst(stmt->Qualify()));
    }
    any = true;
  }
  return any;
}

bool DeferOneUniqueDimJoinPastGroupBy(SelectStatement* stmt,
                                      TransactionContext& ctx) {
  if (stmt == nullptr || stmt->GroupBy().empty() || stmt->IsGroupAll() ||
      stmt->Distinct() || stmt->HasDistinctOn() || stmt->Qualify() ||
      stmt->HasLimit() || stmt->Offset() != 0 || !stmt->OrderBy().empty() ||
      stmt->WithTies() || stmt->ForUpdate() || stmt->AsStruct() ||
      !stmt->WithQueries().empty() || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      relational_detail::StatementContainsVolatileFunction(*stmt) ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt) ||
      stmt->Sources().size() < 2) {
    return false;
  }
  (void)UnwrapStarPlusComputedBaseSubqueries(stmt, ctx);

  struct RelMeta {
    std::string table_name;
    std::string alias;
    std::shared_ptr<Table> table;
    const Schema* schema{nullptr};
    uint64_t est_rows{0};
  };
  std::vector<RelMeta> rels(stmt->Sources().size());
  std::unordered_set<std::string> seen_aliases;
  uint64_t max_est = 0;
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    const SelectSource& s = stmt->Sources()[si];
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample || !s.using_columns.empty() || s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(s.table);
    if (!tbl.HasValue() || tbl.Value() == nullptr) {
      return false;
    }
    const std::string al = s.alias.empty() ? s.table : s.alias;
    if (!seen_aliases.insert(LoweredStarName(al)).second) {
      return false;
    }
    uint64_t est = tbl.Value()->BackingColumnarRowCount();
    if (auto st = ctx.GetStats(s.table); st.HasValue()) {
      est = std::max<uint64_t>(est, st.Value()->Rows());
    }
    max_est = std::max(max_est, est);
    rels[si] = RelMeta{.table_name = s.table,
                       .alias = al,
                       .table = tbl.Value(),
                       .schema = &tbl.Value()->GetSchema(),
                       .est_rows = est};
  }

  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
    if (cn.name.empty() || cn.name == "*") {
      return std::nullopt;
    }
    if (!cn.schema.empty()) {
      for (size_t si = 0; si < rels.size(); ++si) {
        if (IdentifierEquals(cn.schema, rels[si].alias) ||
            (stmt->Sources()[si].alias.empty() &&
             IdentifierEquals(cn.schema, rels[si].table_name))) {
          const int off = rels[si].schema->Offset(ColumnName("", cn.name));
          if (off >= 0) {
            return std::pair{si, static_cast<size_t>(off)};
          }
        }
      }
      return std::nullopt;
    }
    std::optional<std::pair<size_t, size_t>> hit;
    for (size_t si = 0; si < rels.size(); ++si) {
      const int off = rels[si].schema->Offset(ColumnName("", cn.name));
      if (off >= 0) {
        if (hit.has_value()) {
          return std::nullopt;
        }
        hit = std::pair{si, static_cast<size_t>(off)};
      }
    }
    return hit;
  };

  auto expr_sources = [&](const Expression& expr,
                          std::unordered_set<size_t>* out) {
    if (!expr) {
      return true;
    }
    if (ContainsQueryExpression(expr)) {
      return false;
    }
    for (const ColumnName& cn : expr->TouchedColumns()) {
      auto r = resolve_col(cn);
      if (!r.has_value()) {
        return false;
      }
      out->insert(r->first);
    }
    return true;
  };

  std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
  all_preds.reserve(all_preds.size() + stmt->Sources().size() - 1);
  for (size_t si = 1; si < stmt->Sources().size(); ++si) {
    if (stmt->Sources()[si].join_condition) {
      for (Expression& c : SplitConjuncts(stmt->Sources()[si].join_condition)) {
        all_preds.push_back(std::move(c));
      }
    }
  }

  for (size_t dim_si = 0; dim_si < rels.size(); ++dim_si) {
    if (rels[dim_si].est_rows > 250000 ||
        rels[dim_si].est_rows * 2 >= max_est) {
      continue;
    }
    // Tiny PK lookup tables (<= 64 rows, e.g. warehouse/store/call_center)
    // inside a >=3-source join execute directly as L1-resident
    // DeferredLSTablePkLookup dictionaries inside FusedGroupedJoin (enabling
    // split_key_gid + def_cell=1), whereas deferring them past GROUP BY adds a
    // post-aggregate join and breaks single-probe-key packing. Keep deferral
    // for 2-source -> 1-table reduction.
    if (rels.size() >= 3 && rels[dim_si].est_rows <= 64) {
      continue;
    }
    // Check predicates touching dim_si: must be ONLY one `fk_col = dim.pk_col`
    // plus optional non-throwing local `dim.col IS NOT NULL` guards.
    size_t dim_preds = 0;
    size_t pk_slot = 0;
    std::pair<size_t, size_t> fk_ref{0, 0};
    std::vector<Expression> core_preds;
    std::vector<Expression> dim_local_preds;
    bool pred_ok = true;
    for (const Expression& pr : all_preds) {
      std::unordered_set<size_t> ps;
      if (!expr_sources(pr, &ps)) {
        pred_ok = false;
        break;
      }
      if (!ps.contains(dim_si)) {
        core_preds.push_back(pr);
        continue;
      }
      if (ps.size() == 1 && pr->Type() == TypeTag::kUnaryExp &&
          pr->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull &&
          pr->AsUnaryExpression().Child() &&
          pr->AsUnaryExpression().Child()->Type() == TypeTag::kColumnValue) {
        auto dr = resolve_col(
            pr->AsUnaryExpression().Child()->AsColumnValue().GetColumnName());
        if (!dr.has_value() || dr->first != dim_si) {
          pred_ok = false;
          break;
        }
        dim_local_preds.push_back(UnaryExpressionExp(
            ColumnValueExp(ColumnName(
                rels[dim_si].alias,
                rels[dim_si].schema->GetColumn(dr->second).Name().name)),
            UnaryOperation::kIsNotNull));
        continue;
      }
      ++dim_preds;
      if (dim_preds > 1 || pr->Type() != TypeTag::kBinaryExp ||
          pr->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        pred_ok = false;
        break;
      }
      const Expression& l = pr->AsBinaryExpression().Left();
      const Expression& r = pr->AsBinaryExpression().Right();
      if (!l || !r || l->Type() != TypeTag::kColumnValue ||
          r->Type() != TypeTag::kColumnValue) {
        pred_ok = false;
        break;
      }
      auto lr = resolve_col(l->AsColumnValue().GetColumnName());
      auto rr = resolve_col(r->AsColumnValue().GetColumnName());
      if (!lr.has_value() || !rr.has_value() ||
          (lr->first == dim_si) == (rr->first == dim_si)) {
        pred_ok = false;
        break;
      }
      const auto dim_r = (lr->first == dim_si) ? *lr : *rr;
      const auto oth_r = (lr->first == dim_si) ? *rr : *lr;
      const Column& pkc = rels[dim_si].schema->GetColumn(dim_r.second);
      const Column& fkc = rels[oth_r.first].schema->GetColumn(oth_r.second);
      if (pkc.Type() != ValueType::kInt64 || pkc.IsUnsigned() ||
          fkc.Type() != ValueType::kInt64 || fkc.IsUnsigned()) {
        pred_ok = false;
        break;
      }
      pk_slot = dim_r.second;
      fk_ref = oth_r;
    }
    if (!pred_ok || dim_preds != 1) {
      continue;
    }
    if (!dim_local_preds.empty() && stmt->Having() &&
        !DerivedExpressionIsImmutable(stmt->Having())) {
      continue;
    }

    // Check GroupBy: at least one unique dim key `dim.uk_col` AND at least one
    // extra dim attribute/expr in GroupBy or SelectList that benefits from
    // deferral, while non-dim GroupBy items are pure ColumnValues.
    bool has_unique_dim_gkey = false;
    size_t dim_referenced_cols = 0;
    std::vector<std::pair<size_t, size_t>> retained_non_dim_gkeys;
    bool gkeys_ok = true;
    for (const Expression& gk : stmt->GroupBy()) {
      std::unordered_set<size_t> gs;
      if (!gk || !DerivedExpressionIsImmutable(gk) || !expr_sources(gk, &gs)) {
        gkeys_ok = false;
        break;
      }
      if (gs.contains(dim_si)) {
        if (gs.size() != 1) {
          gkeys_ok = false;
          break;
        }
        ++dim_referenced_cols;
        if (!has_unique_dim_gkey && gk->Type() == TypeTag::kColumnValue) {
          auto gr = resolve_col(gk->AsColumnValue().GetColumnName());
          if (gr.has_value() && gr->first == dim_si) {
            const std::string& uk_name =
                rels[dim_si].schema->GetColumn(gr->second).Name().name;
            if (InspectRuntimeUniqueNonNullColumn(ctx, rels[dim_si].table_name,
                                                  uk_name)
                    .unique_non_null) {
              has_unique_dim_gkey = true;
            }
          }
        }
      } else {
        if (gk->Type() != TypeTag::kColumnValue) {
          gkeys_ok = false;
          break;
        }
        auto gr = resolve_col(gk->AsColumnValue().GetColumnName());
        if (!gr.has_value()) {
          gkeys_ok = false;
          break;
        }
        if (std::ranges::find(retained_non_dim_gkeys, *gr) ==
            retained_non_dim_gkeys.end()) {
          retained_non_dim_gkeys.push_back(*gr);
        }
      }
    }
    if (!gkeys_ok || !has_unique_dim_gkey) {
      continue;
    }
    // Also verify `pk_slot` is unique non-null on `dim_si`.
    const std::string& pk_name =
        rels[dim_si].schema->GetColumn(pk_slot).Name().name;
    if (!InspectRuntimeUniqueNonNullColumn(ctx, rels[dim_si].table_name,
                                           pk_name)
             .unique_non_null) {
      continue;
    }

    // Check Having and SelectList: no AggregateExpression may touch `dim_si`,
    // and Having may not touch `dim_si` outside aggregates either.
    std::vector<const AggregateExpression*> aggs;
    std::unordered_set<std::string> seen_agg_str;
    std::unordered_map<std::string, size_t> agg_to_idx;
    bool exprs_ok = true;
    auto check_and_collect = [&](auto& self, const Expression& e,
                                 bool allow_dim_scalar) -> bool {
      if (!e) {
        return true;
      }
      if (e->Type() == TypeTag::kQueryExp ||
          e->Type() == TypeTag::kWindowFunctionExp) {
        return false;
      }
      if (e->Type() ==
          TypeTag::
              kAggregateExp) {  // NOLINT(readability-simplify-boolean-expr)
        const auto& agg = e->AsAggregateExpression();
        if (agg.Distinct() || agg.NeedsGroupContext() || agg.HasWhereFilter() ||
            !agg.InnerGroupBy().empty() || !agg.InnerOrderBy().empty() ||
            agg.InnerLimit().has_value() ||
            agg.Having() != AggregateHavingModifier::kNone) {
          return false;
        }
        if (agg.Child() &&
            (agg.Child()->Type() !=
                 TypeTag::
                     kColumnValue ||  // NOLINT(readability-simplify-boolean-expr)
             agg.Child()->AsColumnValue().GetColumnName().name != "*")) {
          std::unordered_set<size_t> cs;
          if (!expr_sources(agg.Child(), &cs) || cs.contains(dim_si)) {
            return false;
          }
        }
        const std::string key = e->ToString();
        if (seen_agg_str.insert(key).second) {
          agg_to_idx.emplace(key, aggs.size());
          aggs.push_back(&agg);
        }
        return true;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        auto r = resolve_col(e->AsColumnValue().GetColumnName());
        if (!r.has_value()) {
          return false;
        }
        if (r->first == dim_si) {
          ++dim_referenced_cols;
          return allow_dim_scalar;
        }
        return *r == fk_ref || std::ranges::find(retained_non_dim_gkeys, *r) !=
                                   retained_non_dim_gkeys.end();
      }
      for (const Expression& ch : ExpressionChildren(e)) {
        if (!self(self, ch, allow_dim_scalar)) {
          return false;
        }
      }
      return true;
    };

    if (stmt->Having() && !check_and_collect(check_and_collect, stmt->Having(),
                                             /*allow_dim_scalar=*/false)) {
      exprs_ok = false;
    }
    for (const NamedExpression& it : stmt->SelectList()) {
      if (!exprs_ok || !check_and_collect(check_and_collect, it.expression,
                                          /*allow_dim_scalar=*/true)) {
        exprs_ok = false;
        break;
      }
    }
    if (!exprs_ok || dim_referenced_cols < 2) {
      continue;
    }
    // Build inner pre-grouped subquery `__tpcds_dim_pre`.
    std::vector<SelectSource> pre_sources;
    pre_sources.reserve(stmt->Sources().size() - 1);
    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      if (si == dim_si) {
        continue;
      }
      SelectSource cp = stmt->Sources()[si];
      cp.alias = rels[si].alias;
      cp.join_type = JoinType::kCross;
      cp.join_condition = nullptr;
      pre_sources.push_back(std::move(cp));
    }
    auto qualify_core_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (cn.name == "*") {
              return std::nullopt;
            }
            auto r = resolve_col(cn);
            if (!r.has_value()) {
              return std::nullopt;
            }
            return ColumnValueExp(ColumnName(
                rels[r->first].alias,
                rels[r->first].schema->GetColumn(r->second).Name().name));
          });
    };

    const Expression fk_col_expr = ColumnValueExp(ColumnName(
        rels[fk_ref.first].alias,
        rels[fk_ref.first].schema->GetColumn(fk_ref.second).Name().name));
    std::vector<Expression> pre_where;
    pre_where.reserve(core_preds.size() + 1);
    for (const Expression& cp : core_preds) {
      pre_where.push_back(qualify_core_expr(cp));
    }
    pre_where.push_back(
        UnaryExpressionExp(fk_col_expr, UnaryOperation::kIsNotNull));

    std::vector<NamedExpression> pre_select;
    pre_select.reserve(1 + retained_non_dim_gkeys.size() + aggs.size());
    std::vector<Expression> pre_group;
    pre_select.emplace_back("__fk", fk_col_expr);
    pre_group.push_back(fk_col_expr);
    std::vector<size_t> gk_pre_slot(retained_non_dim_gkeys.size(), 0);
    for (size_t gi = 0; gi < retained_non_dim_gkeys.size(); ++gi) {
      if (retained_non_dim_gkeys[gi] == fk_ref) {
        gk_pre_slot[gi] = 0;
        continue;
      }
      const size_t sl = pre_select.size();
      gk_pre_slot[gi] = sl;
      const Expression ge = ColumnValueExp(
          ColumnName(rels[retained_non_dim_gkeys[gi].first].alias,
                     rels[retained_non_dim_gkeys[gi].first]
                         .schema->GetColumn(retained_non_dim_gkeys[gi].second)
                         .Name()
                         .name));
      pre_select.emplace_back("__gk" + std::to_string(sl), ge);
      pre_group.push_back(ge);
    }
    for (size_t ai = 0; ai < aggs.size(); ++ai) {
      const AggregateExpression& a = *aggs[ai];
      Expression qual_child =
          (a.Child() && a.Child()->Type() == TypeTag::kColumnValue &&
           a.Child()->AsColumnValue().GetColumnName().name == "*")
              ? a.Child()
              : qualify_core_expr(a.Child());
      pre_select.emplace_back(
          "__agg" + std::to_string(ai),
          AggregateExpressionExp(a.GetType(), std::move(qual_child), false));
    }

    auto pre_stmt = std::make_shared<SelectStatement>();
    pre_stmt->SetSources(std::move(pre_sources));
    pre_stmt->SetWhereClause(CombineConjuncts(pre_where));
    pre_stmt->SetGroupBy(std::move(pre_group));
    if (stmt->Having()) {
      pre_stmt->SetHaving(qualify_core_expr(stmt->Having()));
    }
    pre_stmt->SetSelectList(std::move(pre_select));

    const std::string pre_alias = "__tpcds_dim_pre";
    SelectSource pre_src("", pre_alias, std::move(pre_stmt), JoinType::kCross,
                         nullptr);
    SelectSource dim_src = stmt->Sources()[dim_si];
    dim_src.alias = rels[dim_si].alias;
    dim_src.join_type = JoinType::kCross;
    dim_src.join_condition = nullptr;

    auto rewrite_outer_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node) {
              return std::nullopt;
            }
            if (node->Type() == TypeTag::kAggregateExp) {
              const auto it = agg_to_idx.find(node->ToString());
              if (it != agg_to_idx.end()) {
                return ColumnValueExp(ColumnName(
                    pre_alias, "__agg" + std::to_string(it->second)));
              }
              return std::nullopt;
            }
            if (node->Type() == TypeTag::kColumnValue) {
              auto r = resolve_col(node->AsColumnValue().GetColumnName());
              if (!r.has_value()) {
                return std::nullopt;
              }
              if (r->first == dim_si) {
                return ColumnValueExp(ColumnName(
                    rels[dim_si].alias,
                    rels[dim_si].schema->GetColumn(r->second).Name().name));
              }
              if (*r == fk_ref) {
                return ColumnValueExp(ColumnName(pre_alias, "__fk"));
              }
              for (size_t gi = 0; gi < retained_non_dim_gkeys.size(); ++gi) {
                if (retained_non_dim_gkeys[gi] == *r) {
                  const size_t sl = gk_pre_slot[gi];
                  return ColumnValueExp(ColumnName(
                      pre_alias,
                      sl == 0 ? "__fk" : ("__gk" + std::to_string(sl))));
                }
              }
            }
            return std::nullopt;
          });
    };

    std::vector<NamedExpression> out_sel;
    out_sel.reserve(stmt->SelectList().size());
    for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
      out_sel.emplace_back(
          InferSelectItemOutputName(stmt->SelectList()[i], i),
          rewrite_outer_expr(stmt->SelectList()[i].expression));
    }

    std::vector<Expression> outer_where;
    outer_where.reserve(1 + dim_local_preds.size());
    outer_where.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName(pre_alias, "__fk")), BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(rels[dim_si].alias, pk_name))));
    for (Expression& dlp : dim_local_preds) {
      outer_where.push_back(std::move(dlp));
    }
    stmt->SetSources({std::move(pre_src), std::move(dim_src)});
    stmt->SetWhereClause(CombineConjuncts(outer_where));
    stmt->SetGroupBy({});
    stmt->SetHaving(nullptr);
    stmt->SetSelectList(std::move(out_sel));
    return true;
  }
  return false;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (rollup_sum_homomorphic_arbitrary_expr_sound)
// Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
//   (rollup_two_level_state_combine_sound,
//   rollup_three_level_state_combine_sound,
//    rollup_grand_total_requires_coalesce)
// Preserves identical FROM+WHERE input shape across ROLLUP / GROUPING SETS
// UNION ALL branches (including DOUBLE aggregates such as Q67) so the executor
// scans and joins the base tables once instead of per-branch.
bool HasSharedScanUnionAllBranches(const SelectStatement& stmt) {
  if (stmt.UnionAll().size() < 2 || stmt.GetSetOperationTree() != nullptr) {
    return false;
  }
  auto shareable_part = [](const SelectStatement& part) {
    return !part.GroupBy().empty() && !part.Distinct() &&
           !part.HasDistinctOn() && !part.Having() && !part.Qualify() &&
           !relational_detail::StatementContainsVolatileFunction(part);
  };
  if (!shareable_part(stmt)) {
    return false;
  }
  auto input_shape = [](const SelectStatement& part) {
    SelectStatement shape(part);
    shape.SetSelectList({});
    shape.SetGroupBy({});
    shape.SetGroupAll(false);
    shape.SetHaving(nullptr);
    shape.SetOrderBy({});
    shape.SetLimit(std::nullopt);
    shape.SetOffset(0);
    shape.SetWithTies(false);
    shape.ClearUnionAll();
    shape.SetSetOperationTree(nullptr);
    shape.ClearWithQueries();
    return std::string(shape.Fingerprint());
  };
  const std::string head_shape = input_shape(stmt);
  for (size_t i = 0; i < stmt.UnionAll().size(); ++i) {
    if (stmt.UnionAll()[i] == nullptr) {
      return false;
    }
    const SelectStatement& part = *stmt.UnionAll()[i];
    const auto match = stmt.MatchFor(i);
    if (!shareable_part(part) || part.HasLimit() || part.Offset() != 0 ||
        !part.OrderBy().empty() || !part.WithQueries().empty() ||
        !part.UnionAll().empty() || match.by_name || match.corresponding ||
        !match.columns.empty() ||
        (i < stmt.SetOperationKinds().size() &&
         stmt.SetOperationKinds()[i] != SetOperationKind::kUnionAll) ||
        input_shape(part) != head_shape) {
      return false;
    }
  }
  return true;
}

bool DeferUniqueDimJoinPastGroupByImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= DeferUniqueDimJoinPastGroupByImpl(body.get(), ctx, visited);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |=
          DeferUniqueDimJoinPastGroupByImpl(src.query.get(), ctx, visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    if (!stmt->UnionAll().empty() &&
        (relational_detail::CanUseExactIntegerGroupingSets(ctx, *stmt) ||
         HasSharedScanUnionAllBranches(*stmt))) {
      return changed;
    }
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return DeferOneUniqueDimJoinPastGroupBy(br, ctx);
    });
  } else {
    changed |= DeferOneUniqueDimJoinPastGroupBy(stmt, ctx);
  }
  return changed;
}

bool SubqueryMayReferenceDimSource(  // NOLINT(misc-no-recursion)
    const SelectStatement& q, std::string_view dim_alias,
    std::string_view dim_table, const Schema& dim_schema);

bool ExprMayReferenceDimInsideSubquery(  // NOLINT(misc-no-recursion)
    const Expression& e, std::string_view dim_alias, std::string_view dim_table,
    const Schema& dim_schema) {
  if (!e) {
    return false;
  }
  if (e->Type() == TypeTag::kQueryExp) {
    const auto& q = e->AsQueryExpression();
    if (q.Test() && ExprMayReferenceDimInsideSubquery(q.Test(), dim_alias,
                                                      dim_table, dim_schema)) {
      return true;
    }
    return q.Query() != nullptr &&
           SubqueryMayReferenceDimSource(*q.Query(), dim_alias, dim_table,
                                         dim_schema);
  }
  if (e->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = e->AsColumnValue().GetColumnName();
    if (cn.name == "*") {
      return !cn.schema.empty() &&
             (IdentifierEquals(cn.schema, dim_alias) ||
              (!dim_table.empty() &&
               IdentifierEquals(
                   cn.schema,
                   dim_table)));  // NOLINT(readability-use-anyofallof)
    }
    if (!cn.schema.empty()) {
      return IdentifierEquals(cn.schema, dim_alias) ||
             (!dim_table.empty() && IdentifierEquals(cn.schema, dim_table));
    }
    return dim_schema.Offset(ColumnName("", cn.name)) >= 0;
  }
  return std::ranges::any_of(ExpressionChildren(e), [&](const Expression& ch) {
    return ExprMayReferenceDimInsideSubquery(ch, dim_alias, dim_table,
                                             dim_schema);
  });
}

bool SubqueryMayReferenceDimSource(  // NOLINT(misc-no-recursion)
    const SelectStatement& q, std::string_view dim_alias,
    std::string_view dim_table, const Schema& dim_schema) {
  for (const auto& [name, body] : q.WithQueries()) {
    (void)name;
    if (body != nullptr && SubqueryMayReferenceDimSource(
                               *body, dim_alias, dim_table, dim_schema)) {
      return true;
    }
  }
  for (const SelectSource& s : q.Sources()) {
    if (s.query != nullptr && SubqueryMayReferenceDimSource(
                                  *s.query, dim_alias, dim_table, dim_schema)) {
      return true;
    }
    if (ExprMayReferenceDimInsideSubquery(s.join_condition, dim_alias,
                                          dim_table, dim_schema) ||
        ExprMayReferenceDimInsideSubquery(s.unnest, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  if (ExprMayReferenceDimInsideSubquery(q.WhereClause(), dim_alias, dim_table,
                                        dim_schema) ||
      ExprMayReferenceDimInsideSubquery(q.Having(), dim_alias, dim_table,
                                        dim_schema) ||
      ExprMayReferenceDimInsideSubquery(q.Qualify(), dim_alias, dim_table,
                                        dim_schema)) {
    return true;
  }
  for (const NamedExpression& it : q.SelectList()) {
    if (ExprMayReferenceDimInsideSubquery(it.expression, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  for (const Expression& g : q.GroupBy()) {
    if (ExprMayReferenceDimInsideSubquery(g, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  for (const auto& ot : q.OrderBy()) {
    if (ExprMayReferenceDimInsideSubquery(ot.expression, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  for (const auto& br : q.UnionAll()) {
    if (br != nullptr &&
        SubqueryMayReferenceDimSource(*br, dim_alias, dim_table, dim_schema)) {
      return true;
    }
  }
  if (const auto& tree = q.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr &&
        SubqueryMayReferenceDimSource(*tree->first, dim_alias, dim_table,
                                      dim_schema)) {
      return true;
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr && SubqueryMayReferenceDimSource(
                               *br, dim_alias, dim_table, dim_schema)) {
        return true;
      }
    }
  }
  return false;
}

bool EliminateUnreferencedPkInOneSelect(
    SelectStatement* stmt,
    const std::unordered_map<std::string, const SelectStatement*>& visible_ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 2 || stmt->ForUpdate() ||
      stmt->AsStruct() || SelectListHasStarOrModifiers(*stmt) ||
      TpcdsStmtHasParam(*stmt) || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr) {
    return false;
  }
  bool any_eliminated = false;
  for (size_t iter = 0; iter < 4 && stmt->Sources().size() >= 2; ++iter) {
    struct SrcInfo {
      std::string table;
      std::string alias;
      std::shared_ptr<Table> tbl;
      Schema schema;
      bool is_base{false};
      uint64_t est_rows{0};
    };
    std::vector<SrcInfo> srcs(stmt->Sources().size());
    std::unordered_set<std::string> seen_al;
    bool srcs_ok = true;
    auto populate_query_schema = [&](const SelectStatement& q,
                                     Schema* out_schema) -> bool {
      const SelectStatement* qhead = &q;
      if (qhead->SelectList().empty() &&
          qhead->GetSetOperationTree() != nullptr &&
          qhead->GetSetOperationTree()->first != nullptr) {
        qhead = qhead->GetSetOperationTree()->first.get();
      }
      if (qhead->SelectList().empty() || SelectListHasStarOrModifiers(*qhead)) {
        return false;
      }
      std::vector<Column> cols;
      cols.reserve(qhead->SelectList().size());
      for (size_t ci = 0; ci < qhead->SelectList().size(); ++ci) {
        cols.emplace_back(
            InferSelectItemOutputName(qhead->SelectList()[ci], ci),
            ValueType::kNull);
      }
      *out_schema = Schema("", std::move(cols));
      return true;
    };
    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      const SelectSource& s = stmt->Sources()[si];
      if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
          s.from_nested_join ||
          (s.join_type != JoinType::kCross &&
           s.join_type != JoinType::kInner)) {
        srcs_ok = false;
        break;
      }
      const std::string al = s.alias.empty() ? s.table : s.alias;
      if (al.empty() || !seen_al.insert(LoweredStarName(al)).second) {
        srcs_ok = false;
        break;
      }
      srcs[si].table = s.table;
      srcs[si].alias = al;
      if (s.query != nullptr) {
        if (!populate_query_schema(*s.query, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
      } else if (const auto cte_it =
                     visible_ctes.find(LoweredStarName(s.table));
                 cte_it != visible_ctes.end()) {
        if (cte_it->second == nullptr ||
            !populate_query_schema(*cte_it->second, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
      } else {
        StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(s.table);
        if (!t.HasValue() || t.Value() == nullptr) {
          srcs_ok = false;
          break;
        }
        srcs[si].tbl = t.Value();
        srcs[si].schema = t.Value()->GetSchema();
        srcs[si].is_base = true;
        uint64_t est = t.Value()->BackingColumnarRowCount();
        if (auto st = ctx.GetStats(s.table); st.HasValue()) {
          est = std::max<uint64_t>(est, st.Value()->Rows());
        }
        srcs[si].est_rows = est;
      }
    }
    if (!srcs_ok) {
      break;
    }

    auto resolve_col =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
      if (cn.name.empty() || cn.name == "*") {
        return std::nullopt;
      }
      if (!cn.schema.empty()) {
        for (size_t si = 0; si < srcs.size(); ++si) {
          if (IdentifierEquals(cn.schema, srcs[si].alias) ||
              (stmt->Sources()[si].alias.empty() && !srcs[si].table.empty() &&
               IdentifierEquals(cn.schema, srcs[si].table))) {
            const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
            if (off >= 0) {
              return std::pair{si, static_cast<size_t>(off)};
            }
          }
        }
        return std::nullopt;
      }
      std::optional<std::pair<size_t, size_t>> hit;
      for (size_t si = 0; si < srcs.size(); ++si) {
        const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
        if (off >= 0) {
          if (hit.has_value()) {
            return std::nullopt;
          }
          hit = std::pair{si, static_cast<size_t>(off)};
        }
      }
      return hit;
    };

    std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
    for (size_t si = 1; si < stmt->Sources().size(); ++si) {
      if (stmt->Sources()[si].join_condition) {
        for (Expression& c :
             SplitConjuncts(stmt->Sources()[si].join_condition)) {
          all_preds.push_back(std::move(c));
        }
      }
    }

    bool dropped_one = false;
    for (size_t dim_si = 0; dim_si < srcs.size(); ++dim_si) {
      if (!srcs[dim_si].is_base || srcs[dim_si].est_rows > 250000) {
        continue;
      }
      size_t dim_pred_cnt = 0;
      size_t pk_slot = 0;
      Expression fk_col_expr;
      std::vector<Expression> other_preds;
      bool preds_ok = true;
      for (const Expression& pr : all_preds) {
        if (!pr) {
          continue;
        }
        bool touches_dim = false;
        if (ContainsQueryExpression(pr)) {
          // Conservatively block if correlated to dim_si.
          for (const ColumnName& cn : pr->TouchedColumns()) {
            auto r = resolve_col(cn);
            if (!r.has_value() || r->first == dim_si) {
              preds_ok = false;
              break;
            }
          }
          if (!preds_ok ||
              ExprMayReferenceDimInsideSubquery(
                  pr, srcs[dim_si].alias,
                  stmt->Sources()[dim_si].alias.empty() ? srcs[dim_si].table
                                                        : std::string_view{},
                  srcs[dim_si].schema)) {
            preds_ok = false;
            break;
          }
          other_preds.push_back(pr);
          continue;
        }
        for (const ColumnName& cn : pr->TouchedColumns()) {
          auto r = resolve_col(cn);
          if (!r.has_value()) {
            preds_ok = false;
            break;
          }
          if (r->first == dim_si) {
            touches_dim = true;
          }
        }
        if (!preds_ok) {
          break;
        }
        if (!touches_dim) {
          other_preds.push_back(pr);
          continue;
        }
        ++dim_pred_cnt;
        if (dim_pred_cnt > 1 || pr->Type() != TypeTag::kBinaryExp ||
            pr->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
          preds_ok = false;
          break;
        }
        const Expression& l = pr->AsBinaryExpression().Left();
        const Expression& r = pr->AsBinaryExpression().Right();
        if (!l || !r || l->Type() != TypeTag::kColumnValue ||
            r->Type() != TypeTag::kColumnValue) {
          preds_ok = false;
          break;
        }
        auto lr = resolve_col(l->AsColumnValue().GetColumnName());
        auto rr = resolve_col(r->AsColumnValue().GetColumnName());
        if (!lr.has_value() || !rr.has_value() ||
            (lr->first == dim_si) == (rr->first == dim_si)) {
          preds_ok = false;
          break;
        }
        const auto dim_r = (lr->first == dim_si) ? *lr : *rr;
        const auto oth_r = (lr->first == dim_si) ? *rr : *lr;
        if (!srcs[oth_r.first].is_base) {
          preds_ok = false;
          break;
        }
        const Column& pkc = srcs[dim_si].schema.GetColumn(dim_r.second);
        const Column& fkc = srcs[oth_r.first].schema.GetColumn(oth_r.second);
        if (pkc.Type() != ValueType::kInt64 || pkc.IsUnsigned() ||
            fkc.Type() != ValueType::kInt64 || fkc.IsUnsigned()) {
          preds_ok = false;
          break;
        }
        pk_slot = dim_r.second;
        fk_col_expr = ColumnValueExp(
            ColumnName(srcs[oth_r.first].alias, fkc.Name().name));
      }
      if (!preds_ok || dim_pred_cnt != 1 || !fk_col_expr) {
        continue;
      }

      // Check that every reference to `dim_si` in SelectList/GroupBy/Having/
      // OrderBy/Qualify/DISTINCT ON is ONLY `dim_si.pk_slot` (which equals
      // `fk_col_expr` bit-for-bit on INT64!).
      bool only_pk_refs = true;
      auto check_expr = [&](auto& self, const Expression& e) -> bool {
        if (!e) {
          return true;
        }
        if (e->Type() == TypeTag::kQueryExp) {
          const auto& q = e->AsQueryExpression();
          if (q.Test() && !self(self, q.Test())) {
            return false;
          }
          return q.Query() != nullptr &&
                 !SubqueryMayReferenceDimSource(
                     *q.Query(), srcs[dim_si].alias,
                     stmt->Sources()[dim_si].alias.empty() ? srcs[dim_si].table
                                                           : std::string_view{},
                     srcs[dim_si].schema);
        }
        if (e->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = e->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return true;
          }
          auto r = resolve_col(cn);
          if (!r.has_value()) {
            return false;
          }
          if (r->first == dim_si) {
            return r->second == pk_slot;
          }
          return true;
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          if (!self(self, ch)) {
            return false;
          }
        }
        return true;
      };
      for (const NamedExpression& it : stmt->SelectList()) {
        if (!check_expr(check_expr, it.expression)) {
          only_pk_refs = false;
          break;
        }
      }
      for (const Expression& g : stmt->GroupBy()) {
        if (!only_pk_refs || !check_expr(check_expr, g)) {
          only_pk_refs = false;
          break;
        }
      }
      if (!only_pk_refs || !check_expr(check_expr, stmt->Having()) ||
          !check_expr(check_expr, stmt->Qualify())) {
        continue;
      }
      for (const auto& ot : stmt->OrderBy()) {
        if (!check_expr(check_expr, ot.expression)) {
          only_pk_refs = false;
          break;
        }
      }
      for (const Expression& key : stmt->DistinctOn()) {
        if (!check_expr(check_expr, key)) {
          only_pk_refs = false;
          break;
        }
      }
      if (!only_pk_refs) {
        continue;
      }

      const std::string& pk_name =
          srcs[dim_si].schema.GetColumn(pk_slot).Name().name;
      const RuntimeUniqueColInfo uinfo =
          InspectRuntimeUniqueNonNullColumn(ctx, srcs[dim_si].table, pk_name);
      if (!uinfo.unique_non_null) {
        continue;
      }
      if (uinfo.row_count > 0 &&
          (uinfo.max_int < uinfo.min_int ||
           static_cast<uint64_t>(uinfo.max_int) -
                   static_cast<uint64_t>(uinfo.min_int) + 1ULL !=
               uinfo.row_count)) {
        continue;
      }

      auto subst_pk = [&](const Expression& e) -> Expression {
        if (!e) {
          return e;
        }
        return ReplaceSubExpressions(
            e, [&](const Expression& node) -> std::optional<Expression> {
              if (!node || node->Type() != TypeTag::kColumnValue) {
                return std::nullopt;
              }
              const ColumnName& cn = node->AsColumnValue().GetColumnName();
              if (cn.name == "*") {
                return std::nullopt;
              }
              auto r = resolve_col(cn);
              if (r.has_value() && r->first == dim_si && r->second == pk_slot) {
                return fk_col_expr;
              }
              return std::nullopt;
            });
      };

      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
        new_sel.emplace_back(
            InferSelectItemOutputName(stmt->SelectList()[i], i),
            subst_pk(stmt->SelectList()[i].expression));
      }
      std::vector<Expression> new_grp;
      new_grp.reserve(stmt->GroupBy().size());
      for (const Expression& g : stmt->GroupBy()) {
        new_grp.push_back(subst_pk(g));
      }
      std::vector<SelectStatement::OrderByTerm> new_ord = stmt->OrderBy();
      for (auto& ot : new_ord) {
        ot.expression = subst_pk(ot.expression);
      }

      std::vector<SelectSource> new_sources;
      new_sources.reserve(stmt->Sources().size() - 1);
      for (size_t si = 0; si < stmt->Sources().size(); ++si) {
        if (si == dim_si) {
          continue;
        }
        SelectSource cp = stmt->Sources()[si];
        if (cp.alias.empty() && !cp.table.empty()) {
          cp.alias = cp.table;
        }
        cp.join_type = JoinType::kCross;
        cp.join_condition = nullptr;
        new_sources.push_back(std::move(cp));
      }

      if (uinfo.row_count == 0) {
        other_preds.push_back(BinaryExpressionExp(
            ConstantValueExp(Value(int64_t{0})), BinaryOperation::kEquals,
            ConstantValueExp(Value(int64_t{1}))));
      } else {
        other_preds.push_back(BinaryExpressionExp(
            fk_col_expr, BinaryOperation::kGreaterThanEquals,
            ConstantValueExp(Value(uinfo.min_int))));
        other_preds.push_back(
            BinaryExpressionExp(fk_col_expr, BinaryOperation::kLessThanEquals,
                                ConstantValueExp(Value(uinfo.max_int))));
      }

      std::vector<Expression> new_distinct_on;
      new_distinct_on.reserve(stmt->DistinctOn().size());
      for (const Expression& key : stmt->DistinctOn()) {
        new_distinct_on.push_back(subst_pk(key));
      }
      stmt->SetSources(std::move(new_sources));
      stmt->SetWhereClause(CombineConjuncts(other_preds));
      stmt->SetSelectList(std::move(new_sel));
      stmt->SetGroupBy(std::move(new_grp));
      stmt->SetOrderBy(std::move(new_ord));
      if (!new_distinct_on.empty()) {
        stmt->SetDistinctOn(std::move(new_distinct_on));
      }
      if (stmt->Having()) {
        stmt->SetHaving(subst_pk(stmt->Having()));
      }
      if (stmt->Qualify()) {
        stmt->SetQualify(subst_pk(stmt->Qualify()));
      }
      any_eliminated = true;
      dropped_one = true;
      break;
    }
    if (!dropped_one) {
      break;
    }
  }
  return any_eliminated;
}

bool EliminateUnreferencedPkImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt,
    std::unordered_map<std::string, const SelectStatement*> visible_ctes,
    TransactionContext& ctx, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const std::string& name : stmt->WithQueryOrder()) {
    const auto it = stmt->WithQueries().find(name);
    const SelectStatement* body_ptr =
        (it != stmt->WithQueries().end()) ? it->second.get() : nullptr;
    if (!stmt->IsRecursiveWith(name) && body_ptr != nullptr) {
      changed |= EliminateUnreferencedPkImpl(it->second.get(), visible_ctes,
                                             ctx, visited);
      visible_ctes[LoweredStarName(name)] = body_ptr;
    } else {
      visible_ctes[LoweredStarName(name)] = nullptr;
    }
  }
  for (const auto& [name, body] : stmt->WithQueries()) {
    (void)body;
    if (!visible_ctes.contains(LoweredStarName(name))) {
      visible_ctes[LoweredStarName(name)] = nullptr;
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= EliminateUnreferencedPkImpl(src.query.get(), visible_ctes, ctx,
                                             visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    if (!stmt->UnionAll().empty() &&
        (relational_detail::CanUseExactIntegerGroupingSets(ctx, *stmt) ||
         HasSharedScanUnionAllBranches(*stmt))) {
      return changed;
    }
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return EliminateUnreferencedPkInOneSelect(br, visible_ctes, ctx);
    });
  } else {
    changed |= EliminateUnreferencedPkInOneSelect(stmt, visible_ctes, ctx);
  }
  return changed;
}

bool DistributeDistinctUnionDimJoinsOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& cte_names,
    TransactionContext& ctx) {
  if (stmt == nullptr || !stmt->Distinct() || stmt->HasDistinctOn() ||
      !stmt->GroupBy().empty() || stmt->IsGroupAll() || stmt->Having() ||
      stmt->Qualify() || stmt->HasLimit() || stmt->Offset() != 0 ||
      !stmt->OrderBy().empty() || stmt->WithTies() || stmt->ForUpdate() ||
      stmt->AsStruct() || !stmt->WithQueries().empty() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt) ||
      stmt->Sources().size() < 3) {
    return false;
  }

  std::optional<size_t> u_idx;
  std::vector<std::shared_ptr<SelectStatement>> u_branches;
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    const SelectSource& s = stmt->Sources()[si];
    if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    if (s.query != nullptr) {
      if (u_idx.has_value() || s.alias.empty() ||
          !ExtractPureUnionAllBranches(*s.query, &u_branches) ||
          u_branches.size() < 2) {
        return false;
      }
      u_idx = si;
    } else if (s.table.empty() ||
               cte_names.contains(LoweredStarName(s.table))) {
      return false;
    }
  }
  if (!u_idx.has_value()) {
    return false;
  }

  const SelectSource& u_src = stmt->Sources()[*u_idx];
  const std::string u_alias = u_src.alias;
  const size_t arity = u_branches[0]->SelectList().size();
  if (arity == 0 || SelectListHasStarOrModifiers(*u_branches[0])) {
    return false;
  }
  std::vector<std::string> u_col_names;
  u_col_names.reserve(arity);
  std::unordered_set<std::string> seen_ucols;  // NOLINT(modernize-loop-convert)
  for (size_t c = 0; c < arity; ++c) {
    std::string nm = ExplicitOrColumnOutputName(u_branches[0]->SelectList()[c]);
    if (nm.empty() || !seen_ucols.insert(LoweredStarName(nm)).second) {
      return false;
    }
    u_col_names.push_back(std::move(nm));
  }
  for (auto& u_branche : u_branches) {  // NOLINT(modernize-loop-convert)
    const SelectStatement& br = *u_branche;
    if (br.SelectList().size() != arity || SelectListHasStarOrModifiers(br) ||
        !br.GroupBy().empty() || br.IsGroupAll() || br.Having() ||
        br.Distinct() || br.HasLimit() || br.Offset() != 0 ||
        !br.WithQueries().empty() ||
        relational_detail::HasWindowFunctions(br)) {
      return false;
    }
  }

  struct OuterRel {
    std::string table;
    std::string alias;
    Schema schema;  // NOLINT(performance-inefficient-vector-operation)
    uint64_t est_rows{0};
  };
  std::vector<OuterRel> rels(stmt->Sources().size());
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    if (si == *u_idx) {
      std::vector<Column> cols;
      cols.reserve(u_col_names.size());
      for (const std::string& nm : u_col_names) {
        cols.emplace_back(
            nm, ValueType::
                    kNull);  // NOLINT(performance-inefficient-vector-operation)
      }
      rels[si] = {
          .table = "", .alias = u_alias, .schema = Schema("", std::move(cols))};
      continue;
    }
    const SelectSource& s = stmt->Sources()[si];
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(s.table);
    if (!tbl.HasValue() || tbl.Value() == nullptr) {
      return false;
    }
    uint64_t est = tbl.Value()->BackingColumnarRowCount();
    if (auto st = ctx.GetStats(s.table); st.HasValue()) {
      est = std::max<uint64_t>(est, st.Value()->Rows());
    }
    rels[si] = {.table = s.table,
                .alias = s.alias.empty() ? s.table : s.alias,
                .schema = tbl.Value()->GetSchema(),
                .est_rows = est};
  }

  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
    if (cn.name.empty() || cn.name == "*") {
      return std::nullopt;
    }
    if (!cn.schema.empty()) {
      for (size_t si = 0; si < rels.size(); ++si) {
        if (IdentifierEquals(cn.schema, rels[si].alias) ||
            (stmt->Sources()[si].alias.empty() && !rels[si].table.empty() &&
             IdentifierEquals(cn.schema, rels[si].table))) {
          const int off = rels[si].schema.Offset(ColumnName("", cn.name));
          if (off >= 0) {
            return std::pair{si, static_cast<size_t>(off)};
          }
        }
      }
      return std::nullopt;
    }
    std::optional<std::pair<size_t, size_t>> hit;
    for (size_t si = 0; si < rels.size(); ++si) {
      const int off = rels[si].schema.Offset(ColumnName("", cn.name));
      if (off >= 0) {
        if (hit.has_value()) {
          return std::nullopt;
        }
        hit = std::pair{si, static_cast<size_t>(off)};
      }
    }
    return hit;
  };

  // Identify sources referenced in `SelectList`.
  std::unordered_set<size_t> select_srcs;
  for (const NamedExpression& it : stmt->SelectList()) {
    if (!it.expression || ContainsQueryExpression(it.expression) ||
        relational_detail::ContainsAggregate(it.expression)) {
      return false;
    }
    for (const ColumnName& cn : it.expression->TouchedColumns()) {
      auto r = resolve_col(cn);
      if (!r.has_value()) {
        return false;
      }
      select_srcs.insert(r->first);
    }
  }
  if (select_srcs.contains(*u_idx)) {
    return false;
  }

  std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
  for (size_t si = 1; si < stmt->Sources().size(); ++si) {
    if (stmt->Sources()[si].join_condition) {
      for (Expression& c : SplitConjuncts(stmt->Sources()[si].join_condition)) {
        all_preds.push_back(std::move(c));
      }
    }
  }

  // Pushable dimensions: small base tables not in `select_srcs`, with >= 1
  // local filter predicate, whose cross-predicates ONLY connect to `*u_idx`.
  std::vector<bool> has_local_pred(rels.size(), false);
  std::vector<bool> touches_keep(rels.size(), false);
  for (const Expression& pr : all_preds) {
    if (!pr || ContainsQueryExpression(pr) ||
        !DerivedExpressionIsImmutable(pr)) {
      return false;
    }
    std::unordered_set<size_t> ps;
    for (const ColumnName& cn : pr->TouchedColumns()) {
      auto r = resolve_col(cn);
      if (!r.has_value()) {
        return false;
      }
      ps.insert(r->first);
    }
    if (ps.size() == 1) {
      has_local_pred[*ps.begin()] = true;
    } else {
      for (size_t a : ps) {
        for (size_t b : ps) {
          if (a != b && a != *u_idx && select_srcs.contains(b)) {
            touches_keep[a] = true;
          }
        }
      }
    }
  }

  std::unordered_set<size_t> push_set;
  for (size_t si = 0; si < rels.size(); ++si) {
    if (si != *u_idx && !select_srcs.contains(si) && has_local_pred[si] &&
        !touches_keep[si] && rels[si].est_rows <= 100000) {
      push_set.insert(si);
    }
  }
  if (push_set.empty()) {
    return false;
  }
  // Verify no push_set table joins to a non-push_set non-u_idx table.
  std::vector<Expression> pushed_preds;
  std::vector<Expression> kept_preds;
  std::vector<size_t> bridge_u_cols;
  for (const Expression& pr : all_preds) {
    std::unordered_set<size_t> ps;
    std::vector<size_t> u_cols_in_pr;
    for (const ColumnName& cn : pr->TouchedColumns()) {
      auto r = resolve_col(cn);
      ps.insert(r->first);
      if (r->first == *u_idx) {
        u_cols_in_pr.push_back(r->second);
      }
    }
    bool any_push = false;
    bool any_keep = false;
    for (size_t s : ps) {
      if (push_set.contains(s)) {
        any_push = true;
      } else if (s != *u_idx) {
        any_keep = true;
      }
    }
    if (any_push && any_keep) {
      return false;
    }
    if (any_push || (ps.size() == 1 && ps.contains(*u_idx))) {
      pushed_preds.push_back(pr);
    } else {
      kept_preds.push_back(pr);
      for (size_t uc : u_cols_in_pr) {
        if (std::ranges::find(bridge_u_cols, uc) == bridge_u_cols.end()) {
          bridge_u_cols.push_back(uc);
        }
      }
    }
  }
  if (bridge_u_cols.empty() || bridge_u_cols.size() > 2) {
    return false;
  }

  // Rewrite each branch of `u_branches` as a wrapped derived query joining
  // `push_set` and grouping on `bridge_u_cols`, combined via UNION DISTINCT.
  std::vector<std::shared_ptr<SelectStatement>> new_branches;
  new_branches.reserve(u_branches.size());
  for (size_t bi = 0; bi < u_branches.size(); ++bi) {
    const std::string sub_u_alias = "__tpcds_ubr_" + std::to_string(bi);
    auto br_copy = std::make_shared<SelectStatement>(*u_branches[bi]);
    std::vector<SelectSource> br_srcs;
    br_srcs.emplace_back("", sub_u_alias, std::move(br_copy), JoinType::kCross,
                         nullptr);
    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      if (!push_set.contains(si)) {
        continue;
      }
      SelectSource ps = stmt->Sources()[si];
      ps.alias = rels[si].alias;
      ps.join_type = JoinType::kCross;
      ps.join_condition = nullptr;
      br_srcs.push_back(std::move(ps));
    }
    auto rebind_pushed = [&](const Expression& e) -> Expression {
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            auto r = resolve_col(node->AsColumnValue().GetColumnName());
            if (!r.has_value()) {
              return std::nullopt;
            }
            if (r->first == *u_idx) {
              return ColumnValueExp(
                  ColumnName(sub_u_alias, u_col_names[r->second]));
            }
            return ColumnValueExp(ColumnName(
                rels[r->first].alias,
                rels[r->first].schema.GetColumn(r->second).Name().name));
          });
    };
    std::vector<Expression> br_where;
    br_where.reserve(pushed_preds.size());
    for (const Expression& pp : pushed_preds) {
      br_where.push_back(rebind_pushed(pp));
    }
    std::vector<NamedExpression> br_sel;
    std::vector<Expression> br_grp;
    for (size_t uc : bridge_u_cols) {
      Expression ce = ColumnValueExp(ColumnName(sub_u_alias, u_col_names[uc]));
      br_sel.emplace_back(u_col_names[uc], ce);
      br_grp.push_back(ce);
    }
    auto wrapped = std::make_shared<SelectStatement>();
    wrapped->SetSources(std::move(br_srcs));
    wrapped->SetWhereClause(CombineConjuncts(br_where));
    wrapped->SetGroupBy(std::move(br_grp));
    wrapped->SetSelectList(std::move(br_sel));
    new_branches.push_back(std::move(wrapped));
  }

  auto union_stmt = std::make_shared<SelectStatement>(*new_branches[0]);
  for (size_t bi = 1; bi < new_branches.size(); ++bi) {
    union_stmt->AddSetOperation(SetOperationKind::kUnion, new_branches[bi]);
  }

  auto qualify_kept = [&](const Expression& e) -> Expression {
    return ReplaceSubExpressions(
        e, [&](const Expression& node) -> std::optional<Expression> {
          if (!node || node->Type() != TypeTag::kColumnValue) {
            return std::nullopt;
          }
          auto r = resolve_col(node->AsColumnValue().GetColumnName());
          if (!r.has_value()) {
            return std::nullopt;
          }
          if (r->first == *u_idx) {
            return ColumnValueExp(ColumnName(u_alias, u_col_names[r->second]));
          }
          return ColumnValueExp(ColumnName(
              rels[r->first].alias,
              rels[r->first].schema.GetColumn(r->second).Name().name));
        });
  };

  std::vector<SelectSource> new_outer_srcs;
  new_outer_srcs.reserve(1 + stmt->Sources().size());
  new_outer_srcs.emplace_back("", u_alias, std::move(union_stmt),
                              JoinType::kCross, nullptr);
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    if (si == *u_idx || push_set.contains(si)) {
      continue;
    }
    SelectSource ks =
        stmt->Sources()
            [si];  // NOLINT(performance-inefficient-vector-operation)
    ks.alias = rels[si].alias;
    ks.join_type = JoinType::kCross;
    ks.join_condition = nullptr;
    new_outer_srcs.push_back(
        std::move(ks));  // NOLINT(performance-inefficient-vector-operation)
  }
  std::vector<Expression> new_outer_where;
  new_outer_where.reserve(kept_preds.size());
  for (const Expression& kp : kept_preds) {
    new_outer_where.push_back(
        qualify_kept(kp));  // NOLINT(performance-inefficient-vector-operation)
  }
  std::vector<NamedExpression> new_outer_sel;
  new_outer_sel.reserve(stmt->SelectList().size());
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    new_outer_sel
        .emplace_back(  // NOLINT(performance-inefficient-vector-operation)
            InferSelectItemOutputName(stmt->SelectList()[i], i),
            qualify_kept(stmt->SelectList()[i].expression));
  }
  stmt->SetSources(std::move(new_outer_srcs));
  stmt->SetWhereClause(CombineConjuncts(new_outer_where));
  stmt->SetSelectList(std::move(new_outer_sel));
  return true;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (bijective_dim_key_cte_deferral_sound)
bool DeferUniqueDimJoinPastGroupBy(SelectStatement* outer,
                                   TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return DeferUniqueDimJoinPastGroupByImpl(outer, ctx, &visited);
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (unreferenced_unique_pk_inner_join_to_semi_sound)
bool EliminateUnreferencedUniquePkInnerJoins(SelectStatement* outer,
                                             TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return EliminateUnreferencedPkImpl(outer, {}, ctx, &visited);
}

namespace {

bool IsGuaranteedSingletonScalarAggQuery(const SelectStatement& q,
                                         std::string* out_col_name) {
  if (!q.GroupBy().empty() || q.IsGroupAll() || q.Having() || q.Qualify() ||
      q.Distinct() || q.HasDistinctOn() || q.HasLimit() || q.Offset() != 0 ||
      q.WithTies() || q.ForUpdate() || q.AsStruct() || !q.OrderBy().empty() ||
      !q.UnionAll().empty() || q.GetSetOperationTree() != nullptr ||
      !q.WithQueries().empty() || q.Sources().empty() ||
      q.SelectList().size() != 1 || SelectListHasStarOrModifiers(q) ||
      TpcdsStmtHasParam(q) || relational_detail::HasWindowFunctions(q)) {
    return false;
  }
  const Expression& se = q.SelectList()[0].expression;
  if (!se || se->Type() != TypeTag::kAggregateExp) {
    return false;
  }
  const auto& agg = se->AsAggregateExpression();
  if ((agg.GetType() != AggregationType::kMin &&
       agg.GetType() != AggregationType::kMax &&
       agg.GetType() != AggregationType::kSum &&
       agg.GetType() != AggregationType::kAvg &&
       agg.GetType() != AggregationType::kCount) ||
      agg.NeedsGroupContext() || !agg.InnerGroupBy().empty() ||
      agg.WhereFilter() || agg.Having() != AggregateHavingModifier::kNone ||
      agg.HavingCondition() || !agg.InnerOrderBy().empty() ||
      agg.SecondaryArg() || !agg.TrailingArgs().empty() ||
      (agg.Child() != nullptr && !DerivedExpressionIsImmutable(agg.Child()))) {
    return false;
  }
  std::string col = InferSelectItemOutputName(q.SelectList()[0], 0);
  if (col.empty()) {
    return false;
  }
  if (out_col_name != nullptr) {
    *out_col_name = std::move(col);
  }
  return true;
}

bool EliminateSingletonScalarCrossJoinInOneSelect(
    SelectStatement* stmt,
    const std::unordered_map<std::string, const SelectStatement*>& visible_ctes,
    const std::unordered_set<std::string>& top_level_singleton_ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 2 ||
      stmt->GroupBy().empty() || stmt->IsGroupAll() || stmt->ForUpdate() ||
      stmt->AsStruct() || SelectListHasStarOrModifiers(*stmt) ||
      TpcdsStmtHasParam(*stmt) || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt)) {
    return false;
  }

  bool any_changed = false;
  for (size_t iter = 0; iter < 4 && stmt->Sources().size() >= 2; ++iter) {
    struct SrcEntry {
      std::string table;
      std::string alias;
      Schema schema;
      std::shared_ptr<SelectStatement> scalar_subquery;
    };
    std::vector<SrcEntry> srcs(stmt->Sources().size());
    std::unordered_set<std::string> seen_al;
    bool srcs_ok = true;
    auto populate_query_schema = [&](const SelectStatement& q,
                                     Schema* out_schema) -> bool {
      const SelectStatement* qhead = &q;
      if (qhead->SelectList().empty() &&
          qhead->GetSetOperationTree() != nullptr &&
          qhead->GetSetOperationTree()->first != nullptr) {
        qhead = qhead->GetSetOperationTree()->first.get();
      }
      if (qhead->SelectList().empty() || SelectListHasStarOrModifiers(*qhead)) {
        return false;
      }
      std::vector<Column> cols;
      cols.reserve(qhead->SelectList().size());
      for (size_t ci = 0; ci < qhead->SelectList().size(); ++ci) {
        cols.emplace_back(
            InferSelectItemOutputName(qhead->SelectList()[ci], ci),
            ValueType::kNull);
      }
      *out_schema = Schema("", std::move(cols));
      return true;
    };

    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      const SelectSource& s = stmt->Sources()[si];
      if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
          s.from_nested_join ||
          (s.join_type != JoinType::kCross &&
           s.join_type != JoinType::kInner)) {
        srcs_ok = false;
        break;
      }
      const std::string al = s.alias.empty() ? s.table : s.alias;
      if (al.empty() || !seen_al.insert(LoweredStarName(al)).second) {
        srcs_ok = false;
        break;
      }
      srcs[si].table = s.table;
      srcs[si].alias = al;
      if (s.query != nullptr) {
        if (!populate_query_schema(*s.query, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
        std::string sc_col;
        if (!s.join_condition &&
            IsGuaranteedSingletonScalarAggQuery(*s.query, &sc_col) &&
            relational_detail::StatementUsesOnlyScopes(ctx, *s.query, {}, {})) {
          srcs[si].scalar_subquery = s.query;
        }
      } else if (const auto cte_it =
                     visible_ctes.find(LoweredStarName(s.table));
                 cte_it != visible_ctes.end()) {
        if (cte_it->second == nullptr ||
            !populate_query_schema(*cte_it->second, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
        if (!s.join_condition &&
            top_level_singleton_ctes.contains(LoweredStarName(s.table)) &&
            srcs[si].schema.ColumnCount() == 1) {
          const std::string col_nm = srcs[si].schema.GetColumn(0).Name().name;
          auto sq = std::make_shared<SelectStatement>();
          SelectSource csrc;
          csrc.table = s.table;
          csrc.alias = s.table;
          sq->SetSources({std::move(csrc)});
          sq->SetSelectList({NamedExpression(
              col_nm, ColumnValueExp(ColumnName(s.table, col_nm)))});
          srcs[si].scalar_subquery = std::move(sq);
        }
      } else {
        StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(s.table);
        if (!t.HasValue() || t.Value() == nullptr) {
          srcs_ok = false;
          break;
        }
        srcs[si].schema = t.Value()->GetSchema();
      }
    }
    if (!srcs_ok) {
      break;
    }

    auto resolve_col =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
      if (cn.name.empty() || cn.name == "*") {
        return std::nullopt;
      }
      if (!cn.schema.empty()) {
        for (size_t si = 0; si < srcs.size(); ++si) {
          if (IdentifierEquals(cn.schema, srcs[si].alias) ||
              (stmt->Sources()[si].alias.empty() && !srcs[si].table.empty() &&
               IdentifierEquals(cn.schema, srcs[si].table))) {
            const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
            if (off >= 0) {
              return std::pair{si, static_cast<size_t>(off)};
            }
          }
        }
        return std::nullopt;
      }
      std::optional<std::pair<size_t, size_t>> hit;
      for (size_t si = 0; si < srcs.size(); ++si) {
        const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
        if (off >= 0) {
          if (hit.has_value()) {
            return std::nullopt;
          }
          hit = std::pair{si, static_cast<size_t>(off)};
        }
      }
      return hit;
    };

    std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
    for (size_t si = 1; si < stmt->Sources().size(); ++si) {
      if (stmt->Sources()[si].join_condition) {
        for (Expression& c :
             SplitConjuncts(stmt->Sources()[si].join_condition)) {
          all_preds.push_back(std::move(c));
        }
      }
    }

    bool dropped = false;
    for (size_t sc_si = 0; sc_si < srcs.size(); ++sc_si) {
      if (srcs[sc_si].scalar_subquery == nullptr ||
          srcs[sc_si].schema.ColumnCount() != 1) {
        continue;
      }
      const std::string_view sc_table = stmt->Sources()[sc_si].alias.empty()
                                            ? srcs[sc_si].table
                                            : std::string_view{};

      auto expr_touches_sc = [&](const Expression& e) -> bool {
        if (!e) {
          return false;
        }
        for (const ColumnName& cn : e->TouchedColumns()) {
          auto r = resolve_col(cn);
          if (!r.has_value() || r->first == sc_si) {
            return true;
          }
        }
        return ContainsQueryExpression(e) &&
               ExprMayReferenceDimInsideSubquery(e, srcs[sc_si].alias, sc_table,
                                                 srcs[sc_si].schema);
      };

      bool used_in_where_or_grp = false;
      for (const Expression& pr : all_preds) {
        if (expr_touches_sc(pr)) {
          used_in_where_or_grp = true;
          break;
        }
      }
      for (const Expression& gk : stmt->GroupBy()) {
        if (used_in_where_or_grp || expr_touches_sc(gk)) {
          used_in_where_or_grp = true;
          break;
        }
      }
      // DISTINCT ON is evaluated in the same scope as the select list; an
      // sc.quality reference there would dangle once the sc source is dropped.
      for (const Expression& key : stmt->DistinctOn()) {
        if (used_in_where_or_grp || expr_touches_sc(key)) {
          used_in_where_or_grp = true;
          break;
        }
      }
      if (used_in_where_or_grp || expr_touches_sc(stmt->Qualify())) {
        continue;
      }

      auto is_idem_sc_agg = [&](const Expression& node) -> bool {
        if (!node || node->Type() != TypeTag::kAggregateExp) {
          return false;
        }
        const auto& agg = node->AsAggregateExpression();
        if ((agg.GetType() != AggregationType::kMin &&
             agg.GetType() != AggregationType::kMax) ||
            agg.WhereFilter() ||
            agg.Having() != AggregateHavingModifier::kNone ||
            !agg.InnerOrderBy().empty() || agg.SecondaryArg() ||
            !agg.TrailingArgs().empty() || !agg.Child() ||
            agg.Child()->Type() != TypeTag::kColumnValue) {
          return false;
        }
        auto r = resolve_col(agg.Child()->AsColumnValue().GetColumnName());
        return r.has_value() && r->first == sc_si && r->second == 0;
      };

      auto validate_expr = [&](auto& self, const Expression& e) -> bool {
        if (!e) {
          return true;
        }
        if (is_idem_sc_agg(e)) {
          return true;
        }
        if (e->Type() == TypeTag::kQueryExp) {
          const auto& q = e->AsQueryExpression();
          if (q.Test() && !self(self, q.Test())) {
            return false;
          }
          return q.Query() != nullptr &&
                 !SubqueryMayReferenceDimSource(*q.Query(), srcs[sc_si].alias,
                                                sc_table, srcs[sc_si].schema);
        }
        if (e->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = e->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return false;
          }
          auto r = resolve_col(cn);
          return r.has_value() && r->first != sc_si;
        }
        if (e->Type() == TypeTag::kAggregateExp) {
          const auto& agg = e->AsAggregateExpression();
          if ((agg.WhereFilter() && !self(self, agg.WhereFilter())) ||
              (agg.HavingCondition() && !self(self, agg.HavingCondition())) ||
              (agg.SecondaryArg() && !self(self, agg.SecondaryArg()))) {
            return false;
          }
          for (const Expression& ta : agg.TrailingArgs()) {
            if (!self(self, ta)) {
              return false;
            }
          }
          for (const auto& io : agg.InnerOrderBy()) {
            if (!self(self, io.expression)) {
              return false;
            }
          }
          for (const Expression& ig : agg.InnerGroupBy()) {
            if (!self(self, ig)) {
              return false;
            }
          }
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          if (!self(self, ch)) {
            return false;
          }
        }
        return true;
      };

      bool refs_ok = true;
      for (const NamedExpression& it : stmt->SelectList()) {
        if (!validate_expr(validate_expr, it.expression)) {
          refs_ok = false;
          break;
        }
      }
      if (!refs_ok || !validate_expr(validate_expr, stmt->Having())) {
        continue;
      }
      for (const auto& ot : stmt->OrderBy()) {
        if (!validate_expr(validate_expr, ot.expression)) {
          refs_ok = false;
          break;
        }
      }
      for (const Expression& key : stmt->DistinctOn()) {
        if (!validate_expr(validate_expr, key)) {
          refs_ok = false;
          break;
        }
      }
      if (!refs_ok) {
        continue;
      }

      Expression replacement = QueryExpressionExp(srcs[sc_si].scalar_subquery);
      auto rewrite_expr = [&](const Expression& e) -> Expression {
        if (!e) {
          return e;
        }
        return ReplaceSubExpressions(
            e, [&](const Expression& node) -> std::optional<Expression> {
              if (is_idem_sc_agg(node)) {
                return replacement;
              }
              return std::nullopt;
            });
      };

      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
        new_sel.emplace_back(
            InferSelectItemOutputName(stmt->SelectList()[i], i),
            rewrite_expr(stmt->SelectList()[i].expression));
      }
      std::vector<SelectStatement::OrderByTerm> new_ord = stmt->OrderBy();
      for (auto& ot : new_ord) {
        ot.expression = rewrite_expr(ot.expression);
      }
      std::vector<SelectSource> new_sources;
      new_sources.reserve(stmt->Sources().size() - 1);
      for (size_t si = 0; si < stmt->Sources().size(); ++si) {
        if (si == sc_si) {
          continue;
        }
        new_sources.push_back(stmt->Sources()[si]);
      }
      if (!new_sources.empty()) {
        new_sources[0].join_type = JoinType::kCross;
      }
      std::vector<Expression> new_distinct_on;
      new_distinct_on.reserve(stmt->DistinctOn().size());
      for (const Expression& key : stmt->DistinctOn()) {
        new_distinct_on.push_back(rewrite_expr(key));
      }
      stmt->SetSources(std::move(new_sources));
      stmt->SetSelectList(std::move(new_sel));
      stmt->SetOrderBy(std::move(new_ord));
      if (!new_distinct_on.empty()) {
        stmt->SetDistinctOn(std::move(new_distinct_on));
      }
      if (stmt->Having()) {
        stmt->SetHaving(rewrite_expr(stmt->Having()));
      }
      any_changed = true;
      dropped = true;
      break;
    }
    if (!dropped) {
      break;
    }
  }
  return any_changed;
}

bool EliminateSingletonScalarCrossJoinsImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt,
    std::unordered_map<std::string, const SelectStatement*> visible_ctes,
    std::unordered_set<std::string> top_level_singleton_ctes, bool is_top_level,
    TransactionContext& ctx, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const std::string& name : stmt->WithQueryOrder()) {
    const auto it = stmt->WithQueries().find(name);
    const SelectStatement* body_ptr =
        (it != stmt->WithQueries().end()) ? it->second.get() : nullptr;
    const std::string low = LoweredStarName(name);
    if (!stmt->IsRecursiveWith(name) && body_ptr != nullptr) {
      changed |= EliminateSingletonScalarCrossJoinsImpl(
          it->second.get(), visible_ctes, top_level_singleton_ctes, false, ctx,
          visited);
      visible_ctes[low] = body_ptr;
      std::string sc_col;
      if (is_top_level &&
          IsGuaranteedSingletonScalarAggQuery(*body_ptr, &sc_col)) {
        top_level_singleton_ctes.insert(low);
      } else {
        top_level_singleton_ctes.erase(low);
      }
    } else {
      visible_ctes[low] = nullptr;
      top_level_singleton_ctes.erase(low);
    }
  }
  for (const auto& [name, body] : stmt->WithQueries()) {
    (void)body;
    const std::string low = LoweredStarName(name);
    if (!visible_ctes.contains(low)) {
      visible_ctes[low] = nullptr;
      top_level_singleton_ctes.erase(low);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= EliminateSingletonScalarCrossJoinsImpl(
          src.query.get(), visible_ctes, top_level_singleton_ctes, false, ctx,
          visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    if (!stmt->UnionAll().empty() &&
        (relational_detail::CanUseExactIntegerGroupingSets(ctx, *stmt) ||
         HasSharedScanUnionAllBranches(*stmt))) {
      return changed;
    }
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return EliminateSingletonScalarCrossJoinInOneSelect(
          br, visible_ctes, top_level_singleton_ctes, ctx);
    });
  } else {
    changed |= EliminateSingletonScalarCrossJoinInOneSelect(
        stmt, visible_ctes, top_level_singleton_ctes, ctx);
  }
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (singleton_scalar_cross_join_elim_sound,
//    trap_singleton_cross_join_sum_not_idempotent)
bool EliminateSingletonScalarCrossJoins(SelectStatement* outer,
                                        TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return EliminateSingletonScalarCrossJoinsImpl(outer, {}, {}, true, ctx,
                                                &visited);
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (distinct_union_all_dim_distribution_sound)
bool DistributeDistinctUnionDimJoins(SelectStatement* outer,
                                     TransactionContext& ctx) {
  if (outer == nullptr) {
    return false;
  }
  std::unordered_set<std::string> cte_names;
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    cte_names.insert(LoweredStarName(name));
  }
  bool changed = false;
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr) {
      changed |=
          DistributeDistinctUnionDimJoinsOneSelect(body.get(), cte_names, ctx);
    }
  }
  changed |= DistributeDistinctUnionDimJoinsOneSelect(outer, cte_names, ctx);
  return changed;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (adjacent_rank_self_join_to_lag_lead_sound)
bool RewriteAdjacentRankSelfJoinToLagLead(SelectStatement* outer) {
  if (outer == nullptr || outer->WithQueries().size() < 2 ||
      TpcdsStmtHasParam(*outer)) {
    return false;
  }
  bool any_changed = false;
  const std::vector<std::string> order = outer->WithQueryOrder();
  for (const std::string& v2_name : order) {
    if (outer->IsRecursiveWith(v2_name)) {
      continue;
    }
    const auto v2_it = outer->WithQueries().find(v2_name);
    if (v2_it == outer->WithQueries().end() || v2_it->second == nullptr) {
      continue;
    }
    SelectStatement& v2 = *v2_it->second;
    if (v2.Sources().size() != 3 || !v2.GroupBy().empty() || v2.IsGroupAll() ||
        v2.Having() || v2.Distinct() || v2.HasDistinctOn() || v2.HasLimit() ||
        v2.Offset() != 0 || v2.Qualify() || !v2.OrderBy().empty() ||
        !v2.WithQueries().empty() || !v2.UnionAll().empty() ||
        v2.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(v2) ||
        SelectListHasStarOrModifiers(v2) || !v2.WhereClause()) {
      continue;
    }
    const std::string v1_name = v2.Sources()[0].table;
    if (v1_name.empty() || outer->IsRecursiveWith(v1_name) ||
        IdentifierEquals(v1_name, v2_name)) {
      continue;
    }
    const auto v1_it = outer->WithQueries().find(v1_name);
    if (v1_it == outer->WithQueries().end() || v1_it->second == nullptr) {
      continue;
    }
    // `v1` must be referenced ONLY inside `v2` (3 times).
    std::unordered_map<std::string, size_t> total_v1_refs;
    CountCteReferencesInStatement(*outer, {v1_name}, &total_v1_refs);
    if (total_v1_refs[v1_name] != 3) {
      continue;
    }
    std::array<std::string, 3> aliases;
    bool srcs_ok = true;
    for (size_t i = 0; i < 3; ++i) {
      const SelectSource& s = v2.Sources()[i];
      if (!IdentifierEquals(s.table, v1_name) || s.query != nullptr ||
          s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
          s.from_nested_join ||
          (s.join_type != JoinType::kCross &&
           s.join_type != JoinType::kInner)) {
        srcs_ok = false;
        break;
      }
      aliases[i] = s.alias.empty() ? s.table : s.alias;
      for (size_t j = 0; j < i; ++j) {
        if (IdentifierEquals(aliases[i], aliases[j])) {
          srcs_ok = false;
        }
      }
    }
    if (!srcs_ok) {
      continue;
    }

    SelectStatement& v1 = *v1_it->second;
    if (v1.GroupBy().empty() || v1.IsGroupAll() || v1.Distinct() ||
        v1.HasDistinctOn() || v1.HasLimit() || v1.Offset() != 0 ||
        v1.Qualify() || !v1.OrderBy().empty() || !v1.WithQueries().empty() ||
        !v1.UnionAll().empty() || v1.GetSetOperationTree() != nullptr ||
        SelectListHasStarOrModifiers(v1) || TpcdsStmtHasParam(v1)) {
      continue;
    }

    // Locate the `RANK()` / `ROW_NUMBER()` / `DENSE_RANK()` column in `v1`.
    std::unordered_map<std::string, size_t> v1_out_map;
    std::optional<size_t> rn_idx;
    const WindowFunctionCallExpression* rn_win = nullptr;
    for (size_t i = 0; i < v1.SelectList().size(); ++i) {
      const std::string nm = InferSelectItemOutputName(v1.SelectList()[i], i);
      if (nm.empty() || !v1_out_map.emplace(LoweredStarName(nm), i).second) {
        rn_idx.reset();
        v1_out_map.clear();
        break;
      }
      const Expression& e = v1.SelectList()[i].expression;
      if (e && e->Type() == TypeTag::kWindowFunctionExp) {
        const auto& wf = e->AsWindowFunctionCallExpression();
        if ((IdentifierEquals(wf.function, "rank") ||
             IdentifierEquals(wf.function, "row_number") ||
             IdentifierEquals(wf.function, "dense_rank")) &&
            wf.args.empty() && !wf.where_filter && !wf.distinct &&
            wf.inner_order_by.empty() && !wf.inner_limit.has_value() &&
            !wf.has_frame && !wf.partition_by.empty() && !wf.order_by.empty()) {
          rn_idx = i;
          rn_win = &wf;
        }
      }
    }
    if (!rn_idx.has_value() || rn_win == nullptr || v1_out_map.empty()) {
      continue;
    }
    const std::string rn_out_name =
        InferSelectItemOutputName(v1.SelectList()[*rn_idx], *rn_idx);

    // Verify `rn_win->partition_by` U `rn_win->order_by` covers ALL of
    // `v1.GroupBy()` (guaranteeing 0 ties per partition!).
    std::unordered_set<std::string> po_exprs;
    std::vector<std::string> part_out_names;
    bool po_ok = true;
    for (const Expression& pe : rn_win->partition_by) {
      if (!pe || pe->Type() != TypeTag::kColumnValue) {
        po_ok = false;
        break;
      }
      po_exprs.insert(pe->ToString());
      std::string found_out;
      for (size_t i = 0; i < v1.SelectList().size(); ++i) {
        if (v1.SelectList()[i].expression &&
            v1.SelectList()[i].expression->ToString() == pe->ToString()) {
          found_out = InferSelectItemOutputName(v1.SelectList()[i], i);
          break;
        }
      }
      if (found_out.empty()) {
        po_ok = false;
        break;
      }
      part_out_names.push_back(std::move(found_out));
    }
    for (const WindowOrderTerm& ot : rn_win->order_by) {
      if (!ot.expression || ot.expression->Type() != TypeTag::kColumnValue) {
        po_ok = false;
        break;
      }
      po_exprs.insert(ot.expression->ToString());
    }
    if (!po_ok) {
      continue;
    }
    for (const Expression& gk : v1.GroupBy()) {
      if (!gk || !po_exprs.contains(gk->ToString())) {
        po_ok = false;
        break;
      }
    }
    if (!po_ok) {
      continue;
    }

    // The three sources are matched in the fixed order
    // (center, lag, lead) = (0, 1, 2), i.e. (v1, v1_lag, v1_lead).
    // Other orderings of the same self-join shape are not considered here.
    std::vector<Expression> v2_conjs = SplitConjuncts(v2.WhereClause());
    for (size_t si = 1; si < 3; ++si) {
      if (v2.Sources()[si].join_condition) {
        for (Expression& jc : SplitConjuncts(v2.Sources()[si].join_condition)) {
          v2_conjs.push_back(std::move(jc));
        }
      }
    }

    auto match_col_of = [&](const Expression& e, size_t src_i,
                            std::string* col_out) -> bool {
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      if (!IdentifierEquals(cn.schema, aliases[src_i])) {
        return false;
      }
      *col_out = cn.name;
      return true;
    };

    // Check `(center = 0, lag = 1, lead = 2)`:
    const size_t c_i = 0;
    const size_t lg_i = 1;
    const size_t ld_i = 2;
    std::unordered_set<std::string> eq_lg_cols;
    eq_lg_cols.reserve(v2_conjs.size());
    std::unordered_set<std::string> eq_ld_cols;
    bool has_lag_rn = false;
    bool has_lead_rn = false;
    bool conjs_ok = true;
    for (const Expression& cj : v2_conjs) {
      if (!cj || cj->Type() != TypeTag::kBinaryExp ||
          cj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        conjs_ok = false;
        break;
      }
      const Expression& l = cj->AsBinaryExpression().Left();
      const Expression& r = cj->AsBinaryExpression().Right();
      std::string lc;
      std::string rc;
      if (match_col_of(l, c_i, &lc) && match_col_of(r, lg_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_lg_cols.insert(LoweredStarName(lc));
        continue;
      }
      if (match_col_of(r, c_i, &lc) && match_col_of(l, lg_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_lg_cols.insert(LoweredStarName(lc));
        continue;
      }
      if (match_col_of(l, c_i, &lc) && match_col_of(r, ld_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_ld_cols.insert(LoweredStarName(lc));
        continue;
      }
      if (match_col_of(r, c_i, &lc) && match_col_of(l, ld_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_ld_cols.insert(LoweredStarName(lc));
        continue;
      }
      // Match `v1.rn = v1_lag.rn + 1` or `v1.rn = v1_lead.rn - 1`.
      auto check_rn_offset = [&](const Expression& lhs_c,
                                 const Expression& rhs_bin) -> bool {
        std::string c_rn;
        if (!match_col_of(lhs_c, c_i, &c_rn) ||
            !IdentifierEquals(c_rn, rn_out_name) || !rhs_bin ||
            rhs_bin->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& b = rhs_bin->AsBinaryExpression();
        if ((b.Op() != BinaryOperation::kAdd &&
             b.Op() != BinaryOperation::kSubtract) ||
            !b.Right() || b.Right()->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const Value& kv = b.Right()->AsConstantValue().GetValue();
        if (kv.IsNull() || kv.type != ValueType::kInt64 ||
            kv.value.int_value != 1) {
          return false;
        }
        std::string o_rn;
        if (b.Op() == BinaryOperation::kAdd &&
            match_col_of(b.Left(), lg_i, &o_rn) &&
            IdentifierEquals(o_rn, rn_out_name)) {
          has_lag_rn = true;
          return true;
        }
        if (b.Op() == BinaryOperation::kSubtract &&
            match_col_of(b.Left(), ld_i, &o_rn) &&
            IdentifierEquals(o_rn, rn_out_name)) {
          has_lead_rn = true;
          return true;
        }
        return false;
      };
      if (check_rn_offset(l, r) || check_rn_offset(r, l)) {
        continue;
      }
      conjs_ok = false;
      break;
    }
    if (!conjs_ok || !has_lag_rn || !has_lead_rn ||
        eq_lg_cols.size() != part_out_names.size() ||
        eq_ld_cols.size() != part_out_names.size()) {
      continue;
    }
    bool exact_part_match = true;
    for (const std::string& pnm : part_out_names) {
      const std::string plow = LoweredStarName(pnm);
      if (!eq_lg_cols.contains(plow) || !eq_ld_cols.contains(plow)) {
        exact_part_match = false;
        break;
      }
    }
    if (!exact_part_match) {
      continue;
    }

    // Inspect `v2.SelectList()`: center refs allowed, plus lag/lead refs to
    // pure aggregate/column items of `v1`.
    std::unordered_map<std::string, std::string> lag_col_to_win;
    std::unordered_map<std::string, std::string> lead_col_to_win;
    bool rn_used_in_v2 = false;
    bool v2_sel_ok = true;
    size_t synth_seq = 0;
    std::vector<NamedExpression> extra_v1_wins;

    auto make_offset_win = [&](std::string fn_name,
                               Expression arg) -> Expression {
      auto w = std::make_shared<WindowFunctionCallExpression>();
      w->function = std::move(fn_name);
      w->args = {std::move(arg), ConstantValueExp(Value(int64_t{1}))};
      w->partition_by = rn_win->partition_by;
      w->order_by = rn_win->order_by;
      return w;
    };

    auto ensure_neighbor_win = [&](const std::string& v1_col, bool is_lag,
                                   std::string* out_win_name) -> bool {
      auto& map = is_lag ? lag_col_to_win : lead_col_to_win;
      const std::string clow = LoweredStarName(v1_col);
      if (const auto it = map.find(clow); it != map.end()) {
        *out_win_name = it->second;
        return true;
      }
      const auto vit = v1_out_map.find(clow);
      if (vit == v1_out_map.end()) {
        return false;
      }
      const Expression& v1_expr = v1.SelectList()[vit->second].expression;
      auto expr_is_pure_group_output = [&](auto& self,
                                           const Expression& node) -> bool {
        if (!node) {
          return true;
        }
        if (node->Type() == TypeTag::kQueryExp ||
            node->Type() == TypeTag::kWindowFunctionExp ||
            TpcdsExprHasParam(node)) {
          return false;
        }
        if (node->Type() == TypeTag::kFunctionCallExp &&
            GetFunctionVolatility(
                node->AsFunctionCallExpression().FuncName()) !=
                Volatility::kImmutable) {
          return false;
        }
        if (node->Type() == TypeTag::kAggregateExp) {
          const auto& agg = node->AsAggregateExpression();
          if (agg.NeedsGroupContext() || !agg.InnerGroupBy().empty() ||
              agg.WhereFilter() ||
              agg.Having() != AggregateHavingModifier::kNone ||
              agg.HavingCondition() || !agg.InnerOrderBy().empty() ||
              agg.SecondaryArg() || !agg.TrailingArgs().empty()) {
            return false;
          }
        }
        for (const Expression& ch : ExpressionChildren(node)) {
          if (!self(self, ch)) {
            return false;
          }
        }
        return true;
      };
      if (!v1_expr ||
          !expr_is_pure_group_output(expr_is_pure_group_output, v1_expr)) {
        return false;
      }
      const std::string wname = (is_lag ? "__tpcds_lag_" : "__tpcds_lead_") +
                                std::to_string(synth_seq++);
      extra_v1_wins.emplace_back(
          wname, make_offset_win(is_lag ? "LAG" : "LEAD", v1_expr));
      map.emplace(clow, wname);
      *out_win_name = wname;
      return true;
    };

    std::vector<NamedExpression> new_v2_sel;
    new_v2_sel.reserve(v2.SelectList().size());
    for (size_t i = 0; i < v2.SelectList().size(); ++i) {
      std::string out_nm = InferSelectItemOutputName(v2.SelectList()[i], i);
      Expression rewritten = ReplaceSubExpressions(
          v2.SelectList()[i].expression,
          [&](const Expression& node) -> std::optional<Expression> {
            if (!node) {
              return std::nullopt;
            }
            if (node->Type() == TypeTag::kQueryExp) {
              v2_sel_ok = false;
              return std::nullopt;
            }
            if (node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (cn.schema.empty()) {
              v2_sel_ok = false;
              return std::nullopt;
            }
            if (IdentifierEquals(cn.schema, aliases[c_i])) {
              if (IdentifierEquals(cn.name, rn_out_name)) {
                rn_used_in_v2 = true;
              }
              return std::nullopt;
            }
            if (IdentifierEquals(cn.schema, aliases[lg_i])) {
              std::string wn;
              if (!ensure_neighbor_win(cn.name, /*is_lag=*/true, &wn)) {
                v2_sel_ok = false;
                return std::nullopt;
              }
              return ColumnValueExp(ColumnName(aliases[c_i], wn));
            }
            if (IdentifierEquals(cn.schema, aliases[ld_i])) {
              std::string wn;
              if (!ensure_neighbor_win(cn.name, /*is_lag=*/false, &wn)) {
                v2_sel_ok = false;
                return std::nullopt;
              }
              return ColumnValueExp(ColumnName(aliases[c_i], wn));
            }
            v2_sel_ok = false;
            return std::nullopt;
          });
      if (!v2_sel_ok) {
        break;
      }
      new_v2_sel.emplace_back(std::move(out_nm), std::move(rewritten));
    }
    if (!v2_sel_ok) {
      continue;
    }

    const std::string has_lag_col = "__tpcds_has_lag";
    const std::string has_lead_col = "__tpcds_has_lead";
    extra_v1_wins.emplace_back(
        has_lag_col,
        make_offset_win("LAG", ConstantValueExp(Value(int64_t{1}))));
    extra_v1_wins.emplace_back(
        has_lead_col,
        make_offset_win("LEAD", ConstantValueExp(Value(int64_t{1}))));

    std::vector<NamedExpression> new_v1_sel;
    new_v1_sel.reserve(v1.SelectList().size() + extra_v1_wins.size());
    for (size_t i = 0; i < v1.SelectList().size(); ++i) {
      if (i == *rn_idx && !rn_used_in_v2) {
        continue;
      }
      new_v1_sel.push_back(v1.SelectList()[i]);
    }
    for (NamedExpression& ew : extra_v1_wins) {
      new_v1_sel.push_back(std::move(ew));
    }
    v1.SetSelectList(std::move(new_v1_sel));

    SelectSource center_src = v2.Sources()[c_i];
    center_src.alias = aliases[c_i];
    center_src.join_type = JoinType::kCross;
    center_src.join_condition = nullptr;
    std::vector<Expression> new_v2_where;
    new_v2_where.reserve(part_out_names.size() + 2);
    for (const std::string& pnm : part_out_names) {
      new_v2_where.push_back(
          UnaryExpressionExp(ColumnValueExp(ColumnName(aliases[c_i], pnm)),
                             UnaryOperation::kIsNotNull));
    }
    new_v2_where.push_back(UnaryExpressionExp(
        ColumnValueExp(ColumnName(aliases[c_i], has_lag_col)),
        UnaryOperation::kIsNotNull));
    new_v2_where.push_back(UnaryExpressionExp(
        ColumnValueExp(ColumnName(aliases[c_i], has_lead_col)),
        UnaryOperation::kIsNotNull));

    v2.SetSources({std::move(center_src)});
    v2.SetWhereClause(CombineConjuncts(new_v2_where));
    v2.SetSelectList(std::move(new_v2_sel));
    any_changed = true;
  }
  return any_changed;
}

namespace {

bool FoldSingletonRankInStatement(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr || !visited->insert(stmt).second) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= FoldSingletonRankInStatement(body.get(), visited);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr) {
      changed |= FoldSingletonRankInStatement(src.query.get(), visited);
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    if (br != nullptr) {
      changed |= FoldSingletonRankInStatement(br.get(), visited);
    }
  }
  if (const auto& tree = stmt->GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      changed |= FoldSingletonRankInStatement(tree->first.get(), visited);
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr) {
        changed |= FoldSingletonRankInStatement(br.get(), visited);
      }
    }
  }

  auto fold_subqueries_in_expr = [&](auto& self, const Expression& e) -> void {
    if (!e) {
      return;
    }
    if (e->Type() == TypeTag::kQueryExp) {
      const auto& q = e->AsQueryExpression();
      if (q.Query() != nullptr) {
        changed |= FoldSingletonRankInStatement(q.Query().get(), visited);
      }
      self(self, q.Test());
      return;
    }
    for (const Expression& ch : ExpressionChildren(e)) {
      self(self, ch);
    }
  };
  fold_subqueries_in_expr(fold_subqueries_in_expr, stmt->WhereClause());
  fold_subqueries_in_expr(fold_subqueries_in_expr, stmt->Having());
  fold_subqueries_in_expr(fold_subqueries_in_expr, stmt->Qualify());

  // Check if `stmt` is a plain single-grouping-set GROUP BY query whose
  // `GroupBy()` keys are all covered by a
  // `RANK()`/`DENSE_RANK()`/`ROW_NUMBER()` window's `partition_by`.
  if (!stmt->GroupBy().empty() && !stmt->IsGroupAll() &&
      stmt->UnionAll().empty() && stmt->GetSetOperationTree() == nullptr) {
    auto norm_col = [](const Expression& e) -> std::string {
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return {};
      }
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      return LoweredStarName(cn.name);
    };
    std::vector<std::string> gb_names;
    bool gb_simple = true;
    for (const Expression& g : stmt->GroupBy()) {
      std::string nm = norm_col(g);
      if (nm.empty()) {
        gb_simple = false;
        break;
      }
      gb_names.push_back(std::move(nm));
    }
    if (gb_simple) {
      bool any_folded_here = false;
      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
        std::string out_nm =
            InferSelectItemOutputName(stmt->SelectList()[i], i);
        Expression replaced = ReplaceSubExpressions(
            stmt->SelectList()[i].expression,
            [&](const Expression& node) -> std::optional<Expression> {
              if (!node || node->Type() != TypeTag::kWindowFunctionExp) {
                return std::nullopt;
              }
              const auto& wf = node->AsWindowFunctionCallExpression();
              if ((!IdentifierEquals(wf.function, "rank") &&
                   !IdentifierEquals(wf.function, "dense_rank") &&
                   !IdentifierEquals(wf.function, "row_number")) ||
                  !wf.args.empty() || wf.where_filter || wf.distinct ||
                  !wf.inner_order_by.empty() || wf.inner_limit.has_value() ||
                  wf.has_frame || wf.partition_by.empty()) {
                return std::nullopt;
              }
              std::unordered_set<std::string> pnames;
              for (const Expression& pe : wf.partition_by) {
                std::string pn = norm_col(pe);
                if (!pn.empty()) {
                  pnames.insert(std::move(pn));
                }
              }
              for (const std::string& gn : gb_names) {
                if (!pnames.contains(gn)) {
                  return std::nullopt;
                }
              }
              any_folded_here = true;
              return ConstantValueExp(Value(int64_t{1}));
            });
        new_sel.emplace_back(std::move(out_nm), std::move(replaced));
      }
      if (any_folded_here) {
        stmt->SetSelectList(std::move(new_sel));
        changed = true;
      }
    }
  }

  // Also absorb `col IN (SELECT col FROM (SELECT col, 1 AS ranking FROM <same>
  // GROUP BY col) WHERE ranking <= K)` (where 1 <= K is TRUE and <same> matches
  // `stmt`'s own base tables + non-subquery predicates) into `col IS NOT NULL`.
  if (stmt->WhereClause() && !stmt->Sources().empty()) {
    auto canonical_base_sig = [](const SelectStatement& s,
                                 const Expression* skip_conj,
                                 std::string* sig_out) -> bool {
      std::unordered_map<std::string, std::string> alias_to_tbl;
      std::vector<std::string> tbl_multiset;
      tbl_multiset.reserve(s.Sources().size());
      for (const SelectSource& src : s.Sources()) {
        if (src.table.empty() || src.query != nullptr || src.unnest ||
            src.is_lateral || src.sample || !src.using_columns.empty() ||
            src.from_nested_join || src.join_condition ||
            (src.join_type != JoinType::kCross &&
             src.join_type != JoinType::kInner)) {
          return false;
        }
        const std::string tlow = LoweredStarName(src.table);
        tbl_multiset.push_back(tlow);
        alias_to_tbl.emplace(
            LoweredStarName(src.alias.empty() ? src.table : src.alias), tlow);
      }
      std::ranges::sort(tbl_multiset);
      // Reject self-joins of the same base table so alias stripping is exact.
      for (size_t i = 1; i < tbl_multiset.size(); ++i) {
        if (tbl_multiset[i] == tbl_multiset[i - 1]) {
          return false;
        }
      }
      std::vector<std::string> norm_preds;
      for (const Expression& c : SplitConjuncts(s.WhereClause())) {
        if (skip_conj != nullptr && c.get() == skip_conj->get()) {
          continue;
        }
        if (!c || ContainsQueryExpression(c) ||
            !DerivedExpressionIsImmutable(c)) {
          return false;
        }
        Expression stripped = ReplaceSubExpressions(
            c, [&](const Expression& node) -> std::optional<Expression> {
              if (!node || node->Type() != TypeTag::kColumnValue) {
                return std::nullopt;
              }
              const ColumnName& cn = node->AsColumnValue().GetColumnName();
              return ColumnValueExp(ColumnName("", LoweredStarName(cn.name)));
            });
        // Canonicalize commutative `a = b` order.
        if (stripped->Type() == TypeTag::kBinaryExp &&
            stripped->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
          std::string ls = stripped->AsBinaryExpression().Left()->ToString();
          std::string rs = stripped->AsBinaryExpression().Right()->ToString();
          if (rs < ls) {
            stripped =
                BinaryExpressionExp(stripped->AsBinaryExpression().Right(),
                                    BinaryOperation::kEquals,
                                    stripped->AsBinaryExpression().Left());
          }
        }
        norm_preds.push_back(stripped->ToString());
      }
      std::ranges::sort(norm_preds);
      std::string sig;
      for (const std::string& t : tbl_multiset) {
        sig += t + ",";
      }
      sig += "|";
      for (const std::string& p : norm_preds) {
        sig += p + "&";
      }
      *sig_out = std::move(sig);
      return true;
    };

    std::vector<Expression> conjs = SplitConjuncts(stmt->WhereClause());
    bool where_changed = false;
    for (Expression& cj : conjs) {
      if (!cj || cj->Type() != TypeTag::kQueryExp) {
        continue;
      }
      const auto& qexp = cj->AsQueryExpression();
      if (qexp.Exists() || qexp.Negated() || qexp.ArrayResult() ||
          !qexp.Test() || qexp.Test()->Type() != TypeTag::kColumnValue ||
          qexp.Query() == nullptr) {
        continue;
      }
      const std::string outer_col_low =
          LoweredStarName(qexp.Test()->AsColumnValue().GetColumnName().name);
      const SelectStatement* cur_sq = qexp.Query().get();
      // Unwrap `SELECT s_state FROM (<inner>) tmp1 WHERE ranking <= 5` when
      // `ranking` in `<inner>` is ConstantValue(1) and `1 <= 5` folds to true!
      if (cur_sq->Sources().size() == 1 &&
          cur_sq->Sources()[0].query != nullptr && cur_sq->GroupBy().empty() &&
          !cur_sq->Having() && !cur_sq->HasLimit() && cur_sq->Offset() == 0 &&
          cur_sq->UnionAll().empty() &&
          cur_sq->GetSetOperationTree() == nullptr &&
          cur_sq->SelectList().size() == 1 &&
          cur_sq->SelectList()[0].expression &&
          cur_sq->SelectList()[0].expression->Type() == TypeTag::kColumnValue) {
        const SelectStatement& inner_sq = *cur_sq->Sources()[0].query;
        const std::string proj_nm = cur_sq->SelectList()[0]
                                        .expression->AsColumnValue()
                                        .GetColumnName()
                                        .name;
        std::unordered_map<std::string, Expression> const_cols;
        std::string inner_key_col;
        for (size_t i = 0; i < inner_sq.SelectList().size(); ++i) {
          const std::string inm =
              InferSelectItemOutputName(inner_sq.SelectList()[i], i);
          const Expression& ie = inner_sq.SelectList()[i].expression;
          if (IdentifierEquals(inm, proj_nm) && ie &&
              ie->Type() == TypeTag::kColumnValue) {
            inner_key_col =
                LoweredStarName(ie->AsColumnValue().GetColumnName().name);
          } else if (ie && ie->Type() == TypeTag::kConstantValue) {
            const_cols.emplace(LoweredStarName(inm), ie);
          }
        }
        bool wrapper_true = !cur_sq->WhereClause();
        if (cur_sq->WhereClause() && !const_cols.empty()) {
          Expression subst_w = ReplaceSubExpressions(
              cur_sq->WhereClause(),
              [&](const Expression& node) -> std::optional<Expression> {
                if (!node || node->Type() != TypeTag::kColumnValue) {
                  return std::nullopt;
                }
                const auto it = const_cols.find(LoweredStarName(
                    node->AsColumnValue().GetColumnName().name));
                return it != const_cols.end()
                           ? std::optional<Expression>(it->second)
                           : std::nullopt;
              });
          if (std::optional<Value> wv = TpcdsEvalClosedConstant(subst_w);
              wv.has_value() && !wv->IsNull() &&
              wv->type == ValueType::kInt64 && wv->value.int_value != 0) {
            wrapper_true = true;
          }
        }
        if (wrapper_true && inner_key_col == outer_col_low &&
            inner_sq.GroupBy().size() == 1 && !inner_sq.Having() &&
            !inner_sq.HasLimit() && inner_sq.Offset() == 0 &&
            inner_sq.UnionAll().empty() &&
            inner_sq.GetSetOperationTree() == nullptr &&
            inner_sq.GroupBy()[0] &&
            inner_sq.GroupBy()[0]->Type() == TypeTag::kColumnValue &&
            LoweredStarName(
                inner_sq.GroupBy()[0]->AsColumnValue().GetColumnName().name) ==
                outer_col_low) {
          std::string outer_sig;
          std::string inner_sig;
          if (canonical_base_sig(*stmt, &cj, &outer_sig) &&
              canonical_base_sig(inner_sq, nullptr, &inner_sig) &&
              outer_sig == inner_sig) {
            cj = UnaryExpressionExp(qexp.Test(), UnaryOperation::kIsNotNull);
            where_changed = true;
          }
        }
      }
    }
    if (where_changed) {
      stmt->SetWhereClause(CombineConjuncts(conjs));
      changed = true;
    }
  }

  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (singleton_partition_rank_const_one_sound)
bool FoldSingletonPartitionRankToOne(SelectStatement* outer) {
  std::unordered_set<SelectStatement*> visited;
  return FoldSingletonRankInStatement(outer, &visited);
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (multi_join_shared_unique_dim_deferral_sound,
//    trap_non_unique_uk_dim_collapse_unsound)
bool CollapseSharedUniqueDimAcrossJoinedSubqueries(SelectStatement* outer,
                                                   TransactionContext& ctx) {
  if (outer == nullptr || !outer->UnionAll().empty() ||
      outer->GetSetOperationTree() != nullptr || outer->ForUpdate() ||
      outer->AsStruct() || SelectListHasStarOrModifiers(*outer) ||
      TpcdsStmtHasParam(*outer) || outer->Sources().size() < 2) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if ((s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) ||
        s.is_lateral || s.unnest || s.sample || s.from_nested_join ||
        !s.using_columns.empty()) {
      return false;
    }
  }

  struct DeferredDimSite {
    size_t src_idx{0};
    std::string outer_alias;
    std::string pre_alias;
    std::string fk_pre_col;
    std::string dim_table;
    std::string dim_alias;
    std::string pk_col;
    std::shared_ptr<Table> dim_tbl;
    // Lowered outer output col name -> actual dim schema col name.
    std::unordered_map<std::string, std::string> out_to_dim_col;
  };

  std::vector<DeferredDimSite> sites;
  sites.reserve(outer->Sources().size());
  for (size_t si = 0; si < outer->Sources().size(); ++si) {
    const SelectSource& src = outer->Sources()[si];
    if (src.query == nullptr || src.alias.empty()) {
      continue;
    }
    const SelectStatement& dq = *src.query;
    if (!dq.WithQueries().empty() || !dq.UnionAll().empty() ||
        dq.GetSetOperationTree() != nullptr || !dq.GroupBy().empty() ||
        dq.IsGroupAll() || dq.Having() || dq.Distinct() || dq.HasDistinctOn() ||
        dq.Qualify() || dq.HasLimit() || dq.Offset() != 0 ||
        !dq.OrderBy().empty() || dq.WithTies() || dq.Sources().size() != 2 ||
        SelectListHasStarOrModifiers(dq)) {
      continue;
    }
    const SelectSource& pre_src = dq.Sources()[0];
    const SelectSource& dim_src = dq.Sources()[1];
    if (pre_src.query == nullptr || pre_src.alias.empty() ||
        pre_src.is_lateral || pre_src.unnest || pre_src.sample ||
        pre_src.join_condition || dim_src.table.empty() ||
        dim_src.query != nullptr || dim_src.is_lateral || dim_src.unnest ||
        dim_src.sample || dim_src.join_condition ||
        (dim_src.join_type != JoinType::kCross &&
         dim_src.join_type != JoinType::kInner) ||
        outer->WithQueries().contains(dim_src.table)) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> dim_tbl_or = ctx.GetTable(dim_src.table);
    if (!dim_tbl_or.HasValue() || dim_tbl_or.Value() == nullptr) {
      continue;
    }
    std::shared_ptr<Table> dim_tbl = dim_tbl_or.MoveValue();
    const Schema& dim_schema = dim_tbl->GetSchema();
    const std::string dim_alias =
        dim_src.alias.empty() ? dim_src.table : dim_src.alias;
    const std::vector<Expression> dq_conjs = SplitConjuncts(dq.WhereClause());
    if (dq_conjs.size() != 1 || !dq_conjs[0] ||
        dq_conjs[0]->Type() != TypeTag::kBinaryExp ||
        dq_conjs[0]->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const BinaryExpression& eq = dq_conjs[0]->AsBinaryExpression();
    if (!eq.Left() || !eq.Right() ||
        eq.Left()->Type() != TypeTag::kColumnValue ||
        eq.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lc = eq.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rc = eq.Right()->AsColumnValue().GetColumnName();
    std::string fk_pre_col;
    std::string pk_col;
    if (IdentifierEquals(lc.schema, pre_src.alias) &&
        IdentifierEquals(rc.schema, dim_alias)) {
      fk_pre_col = lc.name;
      pk_col = rc.name;
    } else if (IdentifierEquals(rc.schema, pre_src.alias) &&
               IdentifierEquals(lc.schema, dim_alias)) {
      fk_pre_col = rc.name;
      pk_col = lc.name;
    } else {
      continue;
    }
    const int pk_off = dim_schema.Offset(ColumnName("", pk_col));
    if (pk_off < 0) {
      continue;
    }
    const std::string actual_pk =
        dim_schema.GetColumn(static_cast<size_t>(pk_off)).Name().name;
    const GroupingRelation dim_grel{.name = dim_alias, .schema = &dim_schema};
    if (!ColumnIsPrimaryKey(dim_grel, actual_pk) &&
        !InspectRuntimeUniqueNonNullColumn(ctx, dim_src.table, actual_pk)
             .unique_non_null) {
      continue;
    }
    // Verify `fk_pre_col` is projected by `pre_src.query`.
    bool fk_found = false;
    for (size_t i = 0; i < pre_src.query->SelectList().size(); ++i) {
      if (IdentifierEquals(
              InferSelectItemOutputName(pre_src.query->SelectList()[i], i),
              fk_pre_col)) {
        fk_found = true;
        break;
      }
    }
    if (!fk_found) {
      continue;
    }
    std::unordered_map<std::string, std::string> out_to_dim;
    bool items_ok = true;
    std::unordered_set<std::string> seen_out;
    for (size_t i = 0; i < dq.SelectList().size(); ++i) {
      const NamedExpression& it = dq.SelectList()[i];
      const std::string out_nm = InferSelectItemOutputName(it, i);
      if (out_nm.empty() || !seen_out.insert(LoweredStarName(out_nm)).second ||
          !it.expression || it.expression->Type() != TypeTag::kColumnValue) {
        items_ok = false;
        break;
      }
      const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
      if (IdentifierEquals(cn.schema, dim_alias)) {
        const int doff = dim_schema.Offset(ColumnName("", cn.name));
        if (doff < 0) {
          items_ok = false;
          break;
        }
        out_to_dim.emplace(
            LoweredStarName(out_nm),
            dim_schema.GetColumn(static_cast<size_t>(doff)).Name().name);
      } else if (!IdentifierEquals(cn.schema, pre_src.alias)) {
        items_ok = false;
        break;
      }
    }
    if (!items_ok || out_to_dim.empty()) {
      continue;
    }
    sites.push_back(DeferredDimSite{.src_idx = si,
                                    .outer_alias = src.alias,
                                    .pre_alias = pre_src.alias,
                                    .fk_pre_col = fk_pre_col,
                                    .dim_table = dim_src.table,
                                    .dim_alias = dim_alias,
                                    .pk_col = actual_pk,
                                    .dim_tbl = std::move(dim_tbl),
                                    .out_to_dim_col = std::move(out_to_dim)});
  }
  if (sites.size() < 2) {
    return false;
  }

  // Build transitive column-equality graph over outer WHERE + INNER ON.
  auto col_token = [](std::string_view schema, std::string_view col) {
    return LoweredStarName(schema) + "." + LoweredStarName(col);
  };
  std::vector<Expression> outer_conjs = SplitConjuncts(outer->WhereClause());
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_condition) {
      for (const Expression& c : SplitConjuncts(s.join_condition)) {
        outer_conjs.push_back(c);
      }
    }
  }
  std::unordered_map<std::string, std::vector<std::string>> eq_adj;
  for (const Expression& conj : outer_conjs) {
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

  // Group sites by `(dim_table, pk_col)`.
  std::map<std::pair<std::string, std::string>, std::vector<size_t>> groups;
  for (size_t i = 0; i < sites.size(); ++i) {
    groups[{LoweredStarName(sites[i].dim_table),
            LoweredStarName(sites[i].pk_col)}]
        .push_back(i);
  }

  for (const auto& [gkey, gidxs] : groups) {
    (void)gkey;
    if (gidxs.size() < 2) {
      continue;
    }
    const DeferredDimSite& s0 = sites[gidxs[0]];
    const Schema& dim_schema = s0.dim_tbl->GetSchema();
    const GroupingRelation dim_grel{.name = s0.dim_alias,
                                    .schema = &dim_schema};

    // Find a certified unique non-null column `uk_col` of `dim_table` on which
    // ALL `gidxs` are transitively equated in `outer`.
    bool certified_uk_equated = false;
    for (size_t ci = 0; ci < dim_schema.ColumnCount(); ++ci) {
      const std::string& cand_uk = dim_schema.GetColumn(ci).Name().name;
      // Quick check: every site must project `cand_uk` first before paying
      // `InspectRuntimeUniqueNonNullColumn`.
      std::vector<std::string> site_uk_tokens;
      site_uk_tokens.reserve(gidxs.size());
      bool all_project_cand = true;
      for (const size_t gi : gidxs) {
        std::string found_out;
        for (const auto& [out_low, dim_col] : sites[gi].out_to_dim_col) {
          if (IdentifierEquals(dim_col, cand_uk)) {
            found_out = out_low;
            break;
          }
        }
        if (found_out.empty()) {
          all_project_cand = false;
          break;
        }
        site_uk_tokens.push_back(col_token(sites[gi].outer_alias, found_out));
      }
      if (!all_project_cand) {
        continue;
      }
      bool all_eq = true;
      for (size_t j = 1; j < site_uk_tokens.size(); ++j) {
        if (!are_equated(site_uk_tokens[0], site_uk_tokens[j])) {
          all_eq = false;
          break;
        }
      }
      if (!all_eq) {
        continue;
      }
      if (ColumnIsPrimaryKey(dim_grel, cand_uk) ||
          InspectRuntimeUniqueNonNullColumn(ctx, s0.dim_table, cand_uk)
              .unique_non_null) {
        certified_uk_equated = true;
        break;
      }
    }
    if (!certified_uk_equated) {
      continue;
    }

    // Mint a fresh outer alias for the single shared dimension join.
    std::unordered_set<std::string> used_outer_names;
    for (const SelectSource& s : outer->Sources()) {
      if (!s.alias.empty()) {
        used_outer_names.insert(LoweredStarName(s.alias));
      }
      if (!s.table.empty()) {
        used_outer_names.insert(LoweredStarName(s.table));
      }
    }
    std::string shared_dim_alias =
        "__tpcds_shared_dim_" + LoweredStarName(s0.dim_table);
    size_t suffix = 1;
    while (used_outer_names.contains(LoweredStarName(shared_dim_alias))) {
      shared_dim_alias = "__tpcds_shared_dim_" + LoweredStarName(s0.dim_table) +
                         "_" + std::to_string(suffix++);
    }

    // Map `(outer_site_alias.out_col) -> shared_dim_alias.dim_col`, and rewrite
    // each site's derived query to drop `dim_src` and expose `__shared_fk`.
    std::unordered_map<std::string, Expression> qualified_dim_map;
    std::unordered_set<std::string> collapsed_site_aliases;
    collapsed_site_aliases.reserve(gidxs.size());
    std::vector<SelectSource> new_outer_sources = outer->Sources();
    std::vector<std::pair<std::string, std::string>> site_fk_cols;
    site_fk_cols.reserve(gidxs.size());

    for (const size_t gi : gidxs) {
      const DeferredDimSite& site = sites[gi];
      collapsed_site_aliases.insert(LoweredStarName(site.outer_alias));
      for (const auto& [out_low, dim_col] : site.out_to_dim_col) {
        qualified_dim_map.emplace(
            col_token(site.outer_alias, out_low),
            ColumnValueExp(ColumnName(shared_dim_alias, dim_col)));
      }
      const SelectStatement& old_dq = *new_outer_sources[site.src_idx].query;
      std::vector<NamedExpression> stripped_sel;
      std::unordered_set<std::string> existing_names;
      std::string fk_out_name;
      for (size_t i = 0; i < old_dq.SelectList().size(); ++i) {
        const NamedExpression& it = old_dq.SelectList()[i];
        const std::string out_nm = InferSelectItemOutputName(it, i);
        const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
        if (IdentifierEquals(cn.schema, site.pre_alias)) {
          stripped_sel.emplace_back(out_nm, it.expression);
          existing_names.insert(LoweredStarName(out_nm));
          if (fk_out_name.empty() &&
              IdentifierEquals(cn.name, site.fk_pre_col)) {
            fk_out_name = out_nm;
          }
        }
      }
      if (fk_out_name.empty()) {
        fk_out_name = "__shared_fk";
        size_t fk_suf = 1;
        while (existing_names.contains(LoweredStarName(fk_out_name))) {
          fk_out_name = "__shared_fk_" + IntegerToName(fk_suf++);
        }
        stripped_sel.emplace_back(
            fk_out_name,
            ColumnValueExp(ColumnName(site.pre_alias, site.fk_pre_col)));
      }
      auto stripped_dq = std::make_shared<SelectStatement>();
      stripped_dq->SetSources({old_dq.Sources()[0]});
      stripped_dq->SetSelectList(std::move(stripped_sel));
      new_outer_sources[site.src_idx].query = std::move(stripped_dq);
      site_fk_cols.emplace_back(site.outer_alias, fk_out_name);
    }

    // Abort if any outer expression references an unqualified column that could
    // have come from a collapsed site's stripped dimension outputs.
    bool unqualified_conflict = false;
    auto rewrite_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (cn.schema.empty()) {
              for (const size_t gi : gidxs) {
                if (sites[gi].out_to_dim_col.contains(
                        LoweredStarName(cn.name))) {
                  unqualified_conflict = true;
                  break;
                }
              }
              return std::nullopt;
            }
            const auto it =
                qualified_dim_map.find(col_token(cn.schema, cn.name));
            if (it != qualified_dim_map.end()) {
              return it->second;
            }
            return std::nullopt;
          });
    };

    std::vector<NamedExpression> new_sel;
    new_sel.reserve(outer->SelectList().size());
    for (const NamedExpression& it : outer->SelectList()) {
      new_sel.emplace_back(it.name, rewrite_expr(it.expression));
    }
    for (SelectSource& s : new_outer_sources) {
      if (s.join_condition) {
        s.join_condition = rewrite_expr(s.join_condition);
      }
    }
    Expression new_where = rewrite_expr(outer->WhereClause());
    std::vector<Expression> new_group;
    new_group.reserve(outer->GroupBy().size());
    for (const Expression& g : outer->GroupBy()) {
      new_group.push_back(rewrite_expr(g));
    }
    Expression new_having = rewrite_expr(outer->Having());
    Expression new_qualify = rewrite_expr(outer->Qualify());
    std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
    for (auto& ot : new_order) {
      ot.expression = rewrite_expr(ot.expression);
    }
    if (unqualified_conflict) {
      continue;
    }

    // Drop redundant `shared_dim.col = shared_dim.col` conjuncts on certified
    // unique non-null columns, and append `__fk` equijoins + shared dim PK
    // join.
    std::vector<Expression> final_where;
    for (const Expression& c : SplitConjuncts(new_where)) {
      if (c && c->Type() == TypeTag::kBinaryExp &&
          c->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
          c->AsBinaryExpression().Left() && c->AsBinaryExpression().Right() &&
          c->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
          c->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
        const ColumnName& l =
            c->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
        const ColumnName& r =
            c->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
        if (IdentifierEquals(l.schema, shared_dim_alias) &&
            IdentifierEquals(r.schema, shared_dim_alias) &&
            IdentifierEquals(l.name, r.name) &&
            (ColumnIsPrimaryKey(dim_grel, l.name) ||
             InspectRuntimeUniqueNonNullColumn(ctx, s0.dim_table, l.name)
                 .unique_non_null)) {
          continue;
        }
      }
      final_where.push_back(c);
    }
    const Expression rep_fk = ColumnValueExp(
        ColumnName(site_fk_cols[0].first, site_fk_cols[0].second));
    for (size_t j = 1; j < site_fk_cols.size(); ++j) {
      final_where.push_back(BinaryExpressionExp(
          rep_fk, BinaryOperation::kEquals,
          ColumnValueExp(
              ColumnName(site_fk_cols[j].first, site_fk_cols[j].second))));
    }
    final_where.push_back(BinaryExpressionExp(
        rep_fk, BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(shared_dim_alias, s0.pk_col))));

    new_outer_sources.emplace_back(s0.dim_table, shared_dim_alias, nullptr,
                                   JoinType::kCross, nullptr);
    outer->SetSources(std::move(new_outer_sources));
    outer->SetSelectList(std::move(new_sel));
    outer->SetWhereClause(CombineConjuncts(final_where));
    outer->SetGroupBy(std::move(new_group));
    outer->SetHaving(std::move(new_having));
    outer->SetQualify(std::move(new_qualify));
    outer->SetOrderBy(std::move(new_order));
    return true;
  }
  return false;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (left_join_disjunctive_null_rejecting_semi_filter_sound,
//    trap_left_join_non_strict_predicate_semi_unsound,
//    packed_int32_pair_bijection_sound)
bool PushDisjunctiveNullRejectingLeftJoinSemiFilter(SelectStatement* outer,
                                                    TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      outer->ForUpdate() || outer->AsStruct() ||
      SelectListHasStarOrModifiers(*outer) || TpcdsStmtHasParam(*outer) ||
      outer->Sources().size() < 2 || !outer->WhereClause()) {
    return false;
  }
  const SelectSource& s0 = outer->Sources()[0];
  if (s0.table.empty() || s0.query != nullptr || s0.unnest || s0.sample ||
      s0.is_lateral || outer->IsRecursiveWith(s0.table) ||
      !outer->WithQueries().contains(s0.table)) {
    return false;
  }
  const std::string s0_alias = s0.alias.empty() ? s0.table : s0.alias;
  SelectStatement* s0_body = outer->WithQueries().at(s0.table).get();
  std::unordered_map<std::string, ColumnName> s0_out_to_gk;
  if (s0_body == nullptr || s0_body->Sources().empty() ||
      !ValidateGroupedCteTarget(s0_body, &s0_out_to_gk) ||
      relational_detail::StatementContainsVolatileFunction(*s0_body)) {
    return false;
  }
  for (const SelectSource& bs : s0_body->Sources()) {
    if (bs.join_type == JoinType::kRight || bs.join_type == JoinType::kFull) {
      return false;
    }
  }
  const SelectSource& fact_src = s0_body->Sources()[0];
  if (fact_src.table.empty() || fact_src.query != nullptr || fact_src.unnest ||
      fact_src.sample || fact_src.is_lateral ||
      outer->WithQueries().contains(fact_src.table)) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> fact_tbl_or = ctx.GetTable(fact_src.table);
  if (!fact_tbl_or.HasValue() || fact_tbl_or.Value() == nullptr) {
    return false;
  }
  const Schema& fact_schema = fact_tbl_or.Value()->GetSchema();
  const std::string fact_alias =
      fact_src.alias.empty() ? fact_src.table : fact_src.alias;

  // Ensure none of the other sources in `s0_body` share `fact_schema` column
  // names ambiguously when resolving unqualified `s0_out_to_gk` columns.
  std::unordered_set<std::string> other_s0_cols;
  for (size_t bi = 1; bi < s0_body->Sources().size(); ++bi) {
    const SelectSource& bs = s0_body->Sources()[bi];
    if (bs.table.empty() || bs.query != nullptr ||
        outer->WithQueries().contains(bs.table)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
    if (!btbl.HasValue() || btbl.Value() == nullptr) {
      return false;
    }
    for (size_t ci = 0; ci < btbl.Value()->GetSchema().ColumnCount(); ++ci) {
      other_s0_cols.insert(
          LoweredStarName(btbl.Value()->GetSchema().GetColumn(ci).Name().name));
    }
  }

  // Count CTE references across `*outer`; require `s0.table` and all `LEFT
  // JOIN` CTEs to be single-use before this rewrite.
  std::unordered_set<std::string> all_cte_names;
  for (const auto& [nm, body] : outer->WithQueries()) {
    (void)body;
    all_cte_names.insert(nm);
  }
  std::unordered_map<std::string, size_t> cte_ref_counts;
  CountCteReferencesInStatement(*outer, all_cte_names, &cte_ref_counts);
  if (cte_ref_counts[s0.table] != 1) {
    return false;
  }

  const std::vector<std::string>& w_order = outer->WithQueryOrder();
  const auto s0_ord_it = std::ranges::find(w_order, s0.table);
  if (s0_ord_it == w_order.end()) {
    return false;
  }
  const size_t s0_ord = static_cast<size_t>(s0_ord_it - w_order.begin());

  struct RightLeftJoinCte {
    size_t src_idx{0};
    std::string cte_name;
    std::string alias;
    SelectStatement* body{nullptr};
    std::unordered_map<std::string, ValueType> col_types;
    std::unordered_map<std::string, std::string> out_actual_name;
    std::unordered_map<std::string, ColumnName> out_to_gk;
    // `(r_out_low, s0_out_low)` equijoin pairs from `ON`.
    std::vector<std::pair<std::string, std::string>> eq_pairs;
  };

  std::unordered_map<std::string, std::string> s0_out_actual;
  std::unordered_map<std::string, ValueType> s0_col_types;
  for (size_t i = 0; i < s0_body->SelectList().size(); ++i) {
    const std::string nm =
        InferSelectItemOutputName(s0_body->SelectList()[i], i);
    s0_out_actual.emplace(LoweredStarName(nm), nm);
  }
  for (const auto& [out_low, gk] : s0_out_to_gk) {
    if (gk.schema.empty() || IdentifierEquals(gk.schema, fact_alias)) {
      const int foff = fact_schema.Offset(ColumnName("", gk.name));
      if (foff >= 0 && !other_s0_cols.contains(LoweredStarName(gk.name))) {
        s0_col_types.emplace(
            out_low, fact_schema.GetColumn(static_cast<size_t>(foff)).Type());
      }
    }
  }

  std::unordered_set<std::string> seen_outer_aliases{LoweredStarName(s0_alias)};
  std::vector<RightLeftJoinCte> right_ctes;
  right_ctes.reserve(outer->Sources().size() - 1);
  for (size_t si = 1; si < outer->Sources().size(); ++si) {
    const SelectSource& rs = outer->Sources()[si];
    if (rs.join_type != JoinType::kLeft || rs.table.empty() ||
        rs.query != nullptr || rs.unnest || rs.sample || rs.is_lateral ||
        !rs.using_columns.empty() || rs.from_nested_join ||
        !rs.join_condition || outer->IsRecursiveWith(rs.table) ||
        !outer->WithQueries().contains(rs.table)) {
      return false;
    }
    const std::string ralias = rs.alias.empty() ? rs.table : rs.alias;
    if (!seen_outer_aliases.insert(LoweredStarName(ralias)).second) {
      return false;
    }
    SelectStatement* rbody = outer->WithQueries().at(rs.table).get();
    if (rbody == nullptr ||
        relational_detail::StatementContainsVolatileFunction(*rbody)) {
      return false;
    }
    RightLeftJoinCte rinfo;
    rinfo.src_idx = si;
    rinfo.cte_name = rs.table;
    rinfo.alias = ralias;
    rinfo.body = rbody;
    // The group-key mapping is an optional pushdown hint: a body that is not
    // a simple grouped CTE simply contributes no mapping.
    // ValidateGroupedCteTarget can, however, have inserted entries before it
    // decides to fail, so clear any partial result instead of letting it drive
    // an unsafe pushdown.
    if (!ValidateGroupedCteTarget(rbody, &rinfo.out_to_gk)) {
      rinfo.out_to_gk.clear();
    }
    for (size_t i = 0; i < rbody->SelectList().size(); ++i) {
      const std::string nm =
          InferSelectItemOutputName(rbody->SelectList()[i], i);
      rinfo.out_actual_name.emplace(LoweredStarName(nm), nm);
    }
    // Parse `rs.join_condition`: require pure conjunction of `r_col = s0_col`.
    const std::vector<Expression> on_conjs = SplitConjuncts(rs.join_condition);
    if (on_conjs.empty()) {
      return false;
    }
    bool on_ok = true;
    for (const Expression& oc : on_conjs) {
      if (!oc || oc->Type() != TypeTag::kBinaryExp ||
          oc->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
          !oc->AsBinaryExpression().Left() ||
          !oc->AsBinaryExpression().Right() ||
          oc->AsBinaryExpression().Left()->Type() != TypeTag::kColumnValue ||
          oc->AsBinaryExpression().Right()->Type() != TypeTag::kColumnValue) {
        on_ok = false;
        break;
      }
      const ColumnName& lc =
          oc->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
      const ColumnName& rc =
          oc->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
      auto belongs_to_r = [&](const ColumnName& cn) {
        if (!cn.schema.empty()) {
          return IdentifierEquals(cn.schema, ralias);
        }
        return rinfo.out_actual_name.contains(LoweredStarName(cn.name)) &&
               !s0_out_actual.contains(LoweredStarName(cn.name));
      };
      auto belongs_to_s0 = [&](const ColumnName& cn) {
        if (!cn.schema.empty()) {
          return IdentifierEquals(cn.schema, s0_alias);
        }
        return s0_out_actual.contains(LoweredStarName(cn.name)) &&
               !rinfo.out_actual_name.contains(LoweredStarName(cn.name));
      };
      if (belongs_to_r(lc) && belongs_to_s0(rc)) {
        rinfo.eq_pairs.emplace_back(LoweredStarName(lc.name),
                                    LoweredStarName(rc.name));
      } else if (belongs_to_r(rc) && belongs_to_s0(lc)) {
        rinfo.eq_pairs.emplace_back(LoweredStarName(rc.name),
                                    LoweredStarName(lc.name));
      } else {
        on_ok = false;
        break;
      }
    }
    if (!on_ok || rinfo.eq_pairs.empty()) {
      return false;
    }
    right_ctes.push_back(std::move(rinfo));
  }

  auto resolve_unique_outer_owner = [&](const ColumnName& cn, int* owner_out,
                                        std::string* col_low_out) {
    *col_low_out = LoweredStarName(cn.name);
    if (!cn.schema.empty()) {
      if (IdentifierEquals(cn.schema, s0_alias) &&
          s0_out_actual.contains(*col_low_out)) {
        *owner_out = 0;
        return true;
      }
      for (size_t i = 0; i < right_ctes.size(); ++i) {
        if (IdentifierEquals(cn.schema, right_ctes[i].alias) &&
            right_ctes[i].out_actual_name.contains(*col_low_out)) {
          *owner_out = static_cast<int>(i + 1);
          return true;
        }
      }
      return false;
    }
    int hits = 0;
    int last = -1;
    if (s0_out_actual.contains(*col_low_out)) {
      ++hits;
      last = 0;
    }
    for (size_t i = 0; i < right_ctes.size(); ++i) {
      if (right_ctes[i].out_actual_name.contains(*col_low_out)) {
        ++hits;
        last = static_cast<int>(i + 1);
      }
    }
    if (hits != 1) {
      return false;
    }
    *owner_out = last;
    return true;
  };

  // Collect `s0.<gk_out> = const_val` equalities from `outer->WhereClause()`.
  const std::vector<Expression> outer_conjs =
      SplitConjuncts(outer->WhereClause());
  std::unordered_map<std::string, Value> pinned_s0_consts;
  for (const Expression& conj : outer_conjs) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    auto check_const = [&](const Expression& cside, const Expression& vside) {
      if (!cside || cside->Type() != TypeTag::kColumnValue) {
        return;
      }
      int owner = -1;
      std::string clow;
      if (!resolve_unique_outer_owner(cside->AsColumnValue().GetColumnName(),
                                      &owner, &clow) ||
          owner != 0 || !s0_out_to_gk.contains(clow)) {
        return;
      }
      std::optional<Value> cv = TpcdsEvalClosedConstant(vside);
      if (cv.has_value() && IsSafeDisjointTagValue(*cv)) {
        pinned_s0_consts.emplace(clow, *cv);
      }
    };
    check_const(bin.Left(), bin.Right());
    check_const(bin.Right(), bin.Left());
  }

  // Flatten top-level `OR` branches of a candidate disjunctive conjunct.
  auto split_or = [](auto&& self, const Expression& e,
                     std::vector<Expression>* out) -> void {
    if (e && e->Type() == TypeTag::kBinaryExp &&
        e->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      self(self, e->AsBinaryExpression().Left(), out);
      self(self, e->AsBinaryExpression().Right(), out);
      return;
    }
    out->push_back(e);
  };

  for (const Expression& conj : outer_conjs) {
    std::vector<Expression> or_branches;
    split_or(split_or, conj, &or_branches);
    if (or_branches.size() < 2) {
      continue;
    }
    std::vector<Expression> exists_branches;
    std::unordered_set<size_t> used_r_indices;
    bool disj_ok = true;

    for (const Expression& br : or_branches) {
      // This pushdown adds a correlated probe to the fact scan. Require a
      // column-to-literal comparison so the EXISTS build can compile its
      // local filter. A computed operand such as COALESCE(qty, 0) would
      // otherwise send the whole disjunction through per-row subqueries.
      if (!br || br->Type() != TypeTag::kBinaryExp) {
        disj_ok = false;
        break;
      }
      const auto& comparison = br->AsBinaryExpression();
      const auto column_and_literal = [](const Expression& column,
                                         const Expression& literal) {
        return column && column->Type() == TypeTag::kColumnValue && literal &&
               literal->Type() == TypeTag::kConstantValue;
      };
      if (!IsComparison(comparison.Op()) ||
          (!column_and_literal(comparison.Left(), comparison.Right()) &&
           !column_and_literal(comparison.Right(), comparison.Left()))) {
        disj_ok = false;
        break;
      }
      // Every column in `br` must belong to ONE right CTE `r_owner` (`>= 1`).
      int r_owner = -1;
      bool cols_ok = true;
      Expression null_subst = ReplaceSubExpressions(
          br, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            int owner = -1;
            std::string clow;
            if (!resolve_unique_outer_owner(
                    node->AsColumnValue().GetColumnName(), &owner, &clow) ||
                owner < 1) {
              cols_ok = false;
              return std::nullopt;
            }
            if (r_owner < 0) {
              r_owner = owner;
            } else if (r_owner != owner) {
              cols_ok = false;
              return std::nullopt;
            }
            return ConstantValueExp(Value());
          });
      if (!cols_ok || r_owner < 1) {
        disj_ok = false;
        break;
      }
      // Mechanical Theorem 29 check (`evalBool p_i nullRow != some true`):
      // evaluate `br` on all-NULL right columns via ground-truth AST evaluator!
      std::optional<Value> on_null = TpcdsEvalClosedConstant(null_subst);
      if (!on_null.has_value() || on_null->Truthy()) {
        disj_ok = false;
        break;
      }

      const auto ri_idx = static_cast<size_t>(r_owner - 1);
      const RightLeftJoinCte& rcte = right_ctes[ri_idx];
      const auto r_ord_it = std::ranges::find(w_order, rcte.cte_name);
      if (r_ord_it == w_order.end() ||
          std::cmp_greater_equal(r_ord_it - w_order.begin(), s0_ord) ||
          cte_ref_counts[rcte.cte_name] != 1) {
        disj_ok = false;
        break;
      }
      for (const SelectSource& rbs : rcte.body->Sources()) {
        if (rbs.join_type == JoinType::kRight ||
            rbs.join_type == JoinType::kFull) {
          disj_ok = false;
          break;
        }
      }
      if (!disj_ok) {
        break;
      }

      // Build `EXISTS (SELECT 1 FROM <rcte.cte_name> AS __semi_r WHERE ...)`.
      const std::string semi_alias = "__semi_r";
      std::vector<Expression> sub_where;
      size_t correlated_fact_keys = 0;
      for (const auto& [r_col_low, s0_col_low] : rcte.eq_pairs) {
        const auto r_act_it = rcte.out_actual_name.find(r_col_low);
        if (r_act_it == rcte.out_actual_name.end()) {
          disj_ok = false;
          break;
        }
        const Expression r_col_exp =
            ColumnValueExp(ColumnName(semi_alias, r_act_it->second));
        if (const auto pin_it = pinned_s0_consts.find(s0_col_low);
            pin_it != pinned_s0_consts.end()) {
          sub_where.push_back(
              BinaryExpressionExp(r_col_exp, BinaryOperation::kEquals,
                                  ConstantValueExp(pin_it->second)));
          continue;
        }
        const auto gk_it = s0_out_to_gk.find(s0_col_low);
        const auto vt_it = s0_col_types.find(s0_col_low);
        if (gk_it == s0_out_to_gk.end() || vt_it == s0_col_types.end() ||
            (vt_it->second != ValueType::kInt64 &&
             vt_it->second != ValueType::kDate)) {
          disj_ok = false;
          break;
        }
        const int foff = fact_schema.Offset(ColumnName("", gk_it->second.name));
        if (foff < 0 ||
            fact_schema.GetColumn(static_cast<size_t>(foff)).IsUnsigned()) {
          disj_ok = false;
          break;
        }
        const std::string actual_fact_col =
            fact_schema.GetColumn(static_cast<size_t>(foff)).Name().name;
        sub_where.push_back(BinaryExpressionExp(
            r_col_exp, BinaryOperation::kEquals,
            ColumnValueExp(ColumnName(fact_alias, actual_fact_col))));
        ++correlated_fact_keys;
      }
      if (!disj_ok || correlated_fact_keys == 0 || correlated_fact_keys > 2) {
        disj_ok = false;
        break;
      }

      Expression qual_br = ReplaceSubExpressions(
          br, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            const auto r_act_it =
                rcte.out_actual_name.find(LoweredStarName(cn.name));
            if (r_act_it == rcte.out_actual_name.end()) {
              return std::nullopt;
            }
            return ColumnValueExp(ColumnName(semi_alias, r_act_it->second));
          });
      sub_where.push_back(std::move(qual_br));

      auto sub_stmt = std::make_shared<SelectStatement>();
      sub_stmt->SetSources({SelectSource(rcte.cte_name, semi_alias, nullptr,
                                         JoinType::kCross, nullptr)});
      sub_stmt->SetSelectList(
          {NamedExpression("", ConstantValueExp(Value(int64_t{1})))});
      sub_stmt->SetWhereClause(CombineConjuncts(sub_where));
      exists_branches.push_back(
          QueryExpressionExp(std::move(sub_stmt), nullptr, true, false));
      used_r_indices.insert(ri_idx);
    }

    if (!disj_ok || exists_branches.size() != or_branches.size()) {
      continue;
    }

    // Push `pinned_s0_consts` into `s0_body` and into each single-use right CTE
    // equated on that key so the materialized CTEs only aggregate the active
    // year/slice!
    for (const auto& [s0_col_low, cval] : pinned_s0_consts) {
      const ColumnName& s0_gk = s0_out_to_gk.at(s0_col_low);
      Expression s0_eq =
          BinaryExpressionExp(ColumnValueExp(s0_gk), BinaryOperation::kEquals,
                              ConstantValueExp(cval));
      s0_body->SetWhereClause(s0_body->WhereClause()
                                  ? BinaryExpressionExp(s0_body->WhereClause(),
                                                        BinaryOperation::kAnd,
                                                        std::move(s0_eq))
                                  : std::move(s0_eq));
      for (RightLeftJoinCte& rcte : right_ctes) {
        if (cte_ref_counts[rcte.cte_name] != 1) {
          continue;
        }
        bool r_no_right_full = true;
        for (const SelectSource& rbs : rcte.body->Sources()) {
          if (rbs.join_type == JoinType::kRight ||
              rbs.join_type == JoinType::kFull) {
            r_no_right_full = false;
            break;
          }
        }
        if (!r_no_right_full) {
          continue;
        }
        for (const auto& [r_col_low, mapped_s0_low] : rcte.eq_pairs) {
          if (mapped_s0_low == s0_col_low) {
            if (const auto rgk_it = rcte.out_to_gk.find(r_col_low);
                rgk_it != rcte.out_to_gk.end()) {
              Expression req = BinaryExpressionExp(
                  ColumnValueExp(rgk_it->second), BinaryOperation::kEquals,
                  ConstantValueExp(cval));
              rcte.body->SetWhereClause(
                  rcte.body->WhereClause()
                      ? BinaryExpressionExp(rcte.body->WhereClause(),
                                            BinaryOperation::kAnd,
                                            std::move(req))
                      : std::move(req));
            }
          }
        }
      }
    }

    Expression semi_or = exists_branches[0];
    for (size_t i = 1; i < exists_branches.size(); ++i) {
      semi_or = BinaryExpressionExp(std::move(semi_or), BinaryOperation::kOr,
                                    std::move(exists_branches[i]));
    }

    // Wrap `s0_body->Sources()[0]` (`fact_src`) as a filtered single-table
    // derived source so `ScanBaseTable` evaluates `semi_or` directly on
    // `LSTableStore` via packed `(int32, int32)` `PrebuiltMembershipPredicate`
    // before `LEFT JOIN` and `GROUP BY`!
    auto fact_sub = std::make_shared<SelectStatement>();
    fact_sub->SetSources({SelectSource(fact_src.table, fact_alias, nullptr,
                                       JoinType::kCross, nullptr)});
    std::vector<NamedExpression> fact_sel;
    fact_sel.reserve(fact_schema.ColumnCount());
    for (size_t ci = 0; ci < fact_schema.ColumnCount(); ++ci) {
      const std::string& cnm = fact_schema.GetColumn(ci).Name().name;
      fact_sel.emplace_back(cnm, ColumnValueExp(ColumnName(fact_alias, cnm)));
    }
    fact_sub->SetSelectList(std::move(fact_sel));
    fact_sub->SetWhereClause(std::move(semi_or));

    std::vector<SelectSource> new_s0_sources = s0_body->Sources();
    new_s0_sources[0] = SelectSource("", fact_alias, std::move(fact_sub),
                                     JoinType::kCross, nullptr);
    s0_body->SetSources(std::move(new_s0_sources));
    return true;
  }
  return false;
}

namespace {

// Returns true when `expr` contains any kAggregateExp node.
bool ContainsAggregateExpression(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expr),
                             [](const Expression& child) {
                               return ContainsAggregateExpression(child);
                             });
}

void CollectWindowFunctionCalls(  // NOLINT(misc-no-recursion)
    const Expression& expr,
    std::vector<const WindowFunctionCallExpression*>* out) {
  if (!expr) {
    return;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp) {
    out->push_back(&expr->AsWindowFunctionCallExpression());
  }
  for (const Expression& child : ExpressionChildren(expr)) {
    CollectWindowFunctionCalls(child, out);
  }
}

bool QualifyPartitionColumnMatches(const ColumnName& a, const ColumnName& b,
                                   size_t source_count) {
  if (!IdentifierEquals(a.name, b.name)) {
    return false;
  }
  if (!a.schema.empty() && !b.schema.empty()) {
    return IdentifierEquals(a.schema, b.schema);
  }
  if (a.schema.empty() && b.schema.empty()) {
    return true;
  }
  return source_count == 1;
}

bool IsVolatileOrUnsafeQualifyFunction(std::string_view fn) {
  static constexpr std::array<std::string_view, 6> kAllowed = {
      "coalesce", "ifnull", "nullif", "greatest", "least", "between"};
  return std::ranges::none_of(kAllowed, [&](std::string_view allowed) {
    return IdentifierEquals(fn, allowed);
  });
}

bool IsSafeQualifyPushdownConjunct(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kColumnValue:
      return expr->AsColumnValue().GetColumnName().name != "*";
    case TypeTag::kConstantValue:
      return dynamic_cast<const ParameterSlot*>(expr.get()) == nullptr;
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      switch (bin.Op()) {
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
        case BinaryOperation::kAnd:
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
          return IsSafeQualifyPushdownConjunct(bin.Left()) &&
                 IsSafeQualifyPushdownConjunct(bin.Right());
        default:
          return false;
      }
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expr->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
        case UnaryOperation::kNot:
          return IsSafeQualifyPushdownConjunct(unary.Child());
        default:
          return false;
      }
    }
    case TypeTag::kInExp:
    case TypeTag::kCaseExp: {
      const auto children = ExpressionChildren(expr);
      return !children.empty() &&
             std::ranges::all_of(children, [](const Expression& child) {
               return IsSafeQualifyPushdownConjunct(child);
             });
    }
    case TypeTag::kFunctionCallExp: {
      const auto& fn = expr->AsFunctionCallExpression();
      if (IsVolatileOrUnsafeQualifyFunction(fn.FuncName())) {
        return false;
      }
      return std::ranges::all_of(fn.Args(), [](const Expression& arg) {
        return IsSafeQualifyPushdownConjunct(arg);
      });
    }
    default:
      return false;
  }
}

bool IsCountStarOrNonNullConst(const Expression& expr) {
  if (!expr || expr->Type() != TypeTag::kAggregateExp) {
    return false;
  }
  const auto& agg = expr->AsAggregateExpression();
  if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
      agg.HasWhereFilter() || agg.NeedsGroupContext() ||
      agg.HasInnerGroupBy() || agg.IgnoreNulls()) {
    return false;
  }
  const Expression& child = agg.Child();
  if (!child) {
    return true;
  }
  if (child->Type() == TypeTag::kColumnValue) {
    return child->AsColumnValue().GetColumnName().name == "*";
  }
  if (child->Type() == TypeTag::kConstantValue &&
      dynamic_cast<const ParameterSlot*>(child.get()) == nullptr) {
    return !child->AsConstantValue().GetValue().IsNull();
  }
  return false;
}

}  // namespace

// Lean proof: lean/TinylambOpt/ProposedProofs/QualifyPushdown.lean (sound)
bool PushPartitionOnlyQualifyToWhere(  // NOLINT(misc-no-recursion)
    SelectStatement* select) {
  if (select == nullptr) {
    return false;
  }
  const char* disable_env =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_QUALIFY_PUSHDOWN");
  if (disable_env != nullptr && disable_env[0] != '\0' &&
      disable_env[0] != '0') {
    return false;
  }

  bool changed = false;
  // A CTE body may be shared by multiple reference sites. Rewriting its
  // QUALIFY in place according to this statement's use can change the rows
  // seen by every other consumer, so leave CTE definitions at their own
  // statement boundary. Source subqueries below are uniquely owned.
  for (const SelectSource& source : select->Sources()) {
    if (source.query != nullptr &&
        PushPartitionOnlyQualifyToWhere(source.query.get())) {
      changed = true;
    }
  }

  if (!select->Qualify() || select->HasDistinctOn() || select->ForUpdate() ||
      !select->UnionAll().empty() || select->GetSetOperationTree() != nullptr ||
      select->IsGroupAll()) {
    return changed;
  }

  const bool is_grouped = !select->GroupBy().empty();
  if (select->Having() != nullptr) {
    return changed;
  }

  std::vector<const WindowFunctionCallExpression*> windows;
  for (const NamedExpression& item : select->SelectList()) {
    CollectWindowFunctionCalls(item.expression, &windows);
    if (!is_grouped && ContainsAggregateExpression(item.expression)) {
      return changed;
    }
  }
  CollectWindowFunctionCalls(select->Qualify(), &windows);
  if (!is_grouped && ContainsAggregateExpression(select->Qualify())) {
    return changed;
  }
  for (const auto& term : select->OrderBy()) {
    CollectWindowFunctionCalls(term.expression, &windows);
    if (!is_grouped && ContainsAggregateExpression(term.expression)) {
      return changed;
    }
  }
  if (windows.empty()) {
    return changed;
  }

  const size_t source_count = select->Sources().size();
  std::vector<ColumnName> common_partitions;
  for (const Expression& part_expr : windows.front()->partition_by) {
    if (part_expr && part_expr->Type() == TypeTag::kColumnValue) {
      common_partitions.push_back(part_expr->AsColumnValue().GetColumnName());
    }
  }
  if (common_partitions.empty()) {
    return changed;
  }

  for (size_t wi = 1; wi < windows.size(); ++wi) {
    std::vector<ColumnName> win_cols;
    for (const Expression& part_expr : windows[wi]->partition_by) {
      if (part_expr && part_expr->Type() == TypeTag::kColumnValue) {
        win_cols.push_back(part_expr->AsColumnValue().GetColumnName());
      }
    }
    std::erase_if(common_partitions, [&](const ColumnName& cand) {
      return std::ranges::none_of(win_cols, [&](const ColumnName& wc) {
        return QualifyPartitionColumnMatches(cand, wc, source_count);
      });
    });
    if (common_partitions.empty()) {
      return changed;
    }
  }

  if (is_grouped) {
    std::vector<ColumnName> group_cols;
    for (const Expression& g : select->GroupBy()) {
      if (g && g->Type() == TypeTag::kColumnValue) {
        group_cols.push_back(g->AsColumnValue().GetColumnName());
      }
    }
    std::erase_if(common_partitions, [&](const ColumnName& cand) {
      return std::ranges::none_of(group_cols, [&](const ColumnName& gc) {
        return QualifyPartitionColumnMatches(cand, gc, source_count);
      });
    });
    if (common_partitions.empty()) {
      return changed;
    }
  }

  std::vector<std::string> shadowed_aliases;
  for (const NamedExpression& item : select->SelectList()) {
    if (item.name.empty()) {
      continue;
    }
    const bool same_named_column =
        item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        IdentifierEquals(item.expression->AsColumnValue().GetColumnName().name,
                         item.name);
    if (!same_named_column) {
      shadowed_aliases.push_back(item.name);
    }
  }

  const std::vector<Expression> qualify_conjuncts =
      SplitConjuncts(select->Qualify());
  std::vector<Expression> pushable;
  std::vector<Expression> remaining;
  pushable.reserve(qualify_conjuncts.size());
  remaining.reserve(qualify_conjuncts.size());
  for (const Expression& conjunct : qualify_conjuncts) {
    if (!conjunct) {
      continue;
    }
    bool can_push = IsSafeQualifyPushdownConjunct(conjunct) &&
                    !ContainsQueryExpression(conjunct);
    std::unordered_set<ColumnName> touched;
    if (can_push) {
      touched = conjunct->TouchedColumns();
      if (touched.empty()) {
        can_push = false;
      }
    }
    if (can_push) {
      for (const ColumnName& col : touched) {
        const bool shadowed = std::ranges::any_of(
            shadowed_aliases, [&](const std::string& alias) {
              return col.schema.empty() && IdentifierEquals(alias, col.name);
            });
        const bool in_common = std::ranges::any_of(
            common_partitions, [&](const ColumnName& part_col) {
              return QualifyPartitionColumnMatches(col, part_col, source_count);
            });
        if (shadowed || !in_common) {
          can_push = false;
          break;
        }
      }
    }
    if (can_push) {
      pushable.push_back(conjunct);
    } else {
      remaining.push_back(conjunct);
    }
  }

  if (pushable.empty()) {
    return changed;
  }

  Expression pushed = CombineConjuncts(pushable);
  if (select->WhereClause() != nullptr) {
    select->SetWhereClause(BinaryExpressionExp(
        select->WhereClause(), BinaryOperation::kAnd, std::move(pushed)));
  } else {
    select->SetWhereClause(std::move(pushed));
  }
  select->SetQualify(remaining.empty() ? nullptr : CombineConjuncts(remaining));
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/OuterJoinCounts.lean (sound)
bool RewriteOuterJoinGroupedCountToHistograms(  // NOLINT(misc-no-recursion)
    SelectStatement* select, TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  const char* disable_env =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_OUTER_JOIN_COUNTS");
  if (disable_env != nullptr && disable_env[0] != '\0' &&
      disable_env[0] != '0') {
    return false;
  }

  bool changed = false;
  for (const auto& [cte_name, cte_body] : select->WithQueries()) {
    (void)cte_name;
    if (cte_body != nullptr &&
        RewriteOuterJoinGroupedCountToHistograms(cte_body.get(), ctx)) {
      changed = true;
    }
  }
  for (const SelectSource& source : select->Sources()) {
    if (source.query != nullptr &&
        RewriteOuterJoinGroupedCountToHistograms(source.query.get(), ctx)) {
      changed = true;
    }
  }

  if (select->Sources().size() != 2 || select->WhereClause() != nullptr ||
      select->Having() != nullptr || select->Qualify() != nullptr ||
      select->Distinct() || select->HasDistinctOn() || select->WithTies() ||
      select->ForUpdate() || select->IsGroupAll() || select->AsStruct() ||
      !select->UnionAll().empty() || select->GetSetOperationTree() != nullptr ||
      select->GroupBy().size() != 1 || select->SelectList().empty()) {
    return changed;
  }

  const SelectSource& s0 = select->Sources()[0];
  const SelectSource& s1 = select->Sources()[1];
  if (s0.table.empty() || s1.table.empty() || s0.query != nullptr ||
      s1.query != nullptr || s0.unnest != nullptr || s1.unnest != nullptr ||
      select->IsRecursiveWith(s0.table) || select->IsRecursiveWith(s1.table) ||
      s0.sample.has_value() || s1.sample.has_value()) {
    return changed;
  }
  for (const auto& [cte_name, cte_body] : select->WithQueries()) {
    (void)cte_body;
    if (IdentifierEquals(cte_name, s0.table) ||
        IdentifierEquals(cte_name, s1.table)) {
      return changed;
    }
  }

  const JoinType jt = s1.join_type;
  if (jt != JoinType::kFull && jt != JoinType::kLeft &&
      jt != JoinType::kInner) {
    return changed;
  }
  if (!s1.join_condition || s1.join_condition->Type() != TypeTag::kBinaryExp) {
    return changed;
  }
  const auto& eq = s1.join_condition->AsBinaryExpression();
  if (eq.Op() != BinaryOperation::kEquals || !eq.Left() || !eq.Right() ||
      eq.Left()->Type() != TypeTag::kColumnValue ||
      eq.Right()->Type() != TypeTag::kColumnValue) {
    return changed;
  }

  const StatusOr<std::shared_ptr<Table>> t0_or = ctx.GetTable(s0.table);
  const StatusOr<std::shared_ptr<Table>> t1_or = ctx.GetTable(s1.table);
  if (!t0_or.HasValue() || !t1_or.HasValue() || t0_or.Value() == nullptr ||
      t1_or.Value() == nullptr) {
    return changed;
  }
  const Schema& schema0 = t0_or.Value()->GetSchema();
  const Schema& schema1 = t1_or.Value()->GetSchema();

  const std::string l_alias = s0.alias.empty() ? s0.table : s0.alias;
  const std::string r_alias = s1.alias.empty() ? s1.table : s1.alias;

  auto find_col = [](const Schema& schema,
                     const std::string& name) -> std::optional<Column> {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const Column& c = schema.GetColumn(i);
      if (IdentifierEquals(c.Name().name, name)) {
        return c;
      }
    }
    return std::nullopt;
  };
  auto is_side_col = [&](const ColumnName& col, bool left_side,
                         std::string* resolved_col_name) -> bool {
    const std::string& own_alias = left_side ? l_alias : r_alias;
    const std::string& other_alias = left_side ? r_alias : l_alias;
    const Schema& own_schema = left_side ? schema0 : schema1;
    const Schema& other_schema = left_side ? schema1 : schema0;
    const std::optional<Column> own_col = find_col(own_schema, col.name);
    if (!own_col.has_value()) {
      return false;
    }
    if (!col.schema.empty()) {
      if (!IdentifierEquals(col.schema, own_alias)) {
        return false;
      }
      if (IdentifierEquals(l_alias, r_alias)) {
        return false;
      }
    } else if (find_col(other_schema, col.name).has_value() ||
               IdentifierEquals(own_alias, other_alias)) {
      return false;
    }
    if (resolved_col_name != nullptr) {
      *resolved_col_name = own_col->Name().name;
    }
    return true;
  };

  const ColumnName& eq_a = eq.Left()->AsColumnValue().GetColumnName();
  const ColumnName& eq_b = eq.Right()->AsColumnValue().GetColumnName();
  std::string l_key_name;
  std::string r_key_name;
  const bool side_a_first = is_side_col(eq_a, true, &l_key_name) &&
                            is_side_col(eq_b, false, &r_key_name);
  const bool side_b_first = !side_a_first &&
                            is_side_col(eq_b, true, &l_key_name) &&
                            is_side_col(eq_a, false, &r_key_name);
  if (!side_a_first && !side_b_first) {
    return changed;
  }

  const std::optional<Column> lcol = find_col(schema0, l_key_name);
  const std::optional<Column> rcol = find_col(schema1, r_key_name);
  if (!lcol.has_value() || !rcol.has_value() || lcol->Type() != rcol->Type() ||
      lcol->IsUnsigned() != rcol->IsUnsigned()) {
    return changed;
  }
  if (lcol->Type() != ValueType::kInt64 && lcol->Type() != ValueType::kDate &&
      lcol->Type() != ValueType::kVarChar) {
    return changed;
  }

  auto is_left_key_expr = [&](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::string resolved;
    return is_side_col(e->AsColumnValue().GetColumnName(), true, &resolved) &&
           IdentifierEquals(resolved, l_key_name);
  };
  auto is_right_key_expr = [&](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::string resolved;
    return is_side_col(e->AsColumnValue().GetColumnName(), false, &resolved) &&
           IdentifierEquals(resolved, r_key_name);
  };
  auto is_coalesce_keys_expr = [&](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kFunctionCallExp) {
      return false;
    }
    const auto& fn = e->AsFunctionCallExpression();
    if (!IdentifierEquals(fn.FuncName(), "coalesce") || fn.Args().size() != 2) {
      return false;
    }
    return (is_left_key_expr(fn.Args()[0]) &&
            is_right_key_expr(fn.Args()[1])) ||
           (jt != JoinType::kLeft && is_right_key_expr(fn.Args()[0]) &&
            is_left_key_expr(fn.Args()[1]));
  };
  auto matches_group_key = [&](const Expression& e) -> bool {
    if (jt == JoinType::kFull) {
      return is_coalesce_keys_expr(e);
    }
    if (jt == JoinType::kLeft) {
      return is_left_key_expr(e) || is_coalesce_keys_expr(e);
    }
    return is_left_key_expr(e) || is_right_key_expr(e) ||
           is_coalesce_keys_expr(e);
  };

  if (!matches_group_key(select->GroupBy()[0])) {
    return changed;
  }

  size_t count_items = 0;
  for (const NamedExpression& item : select->SelectList()) {
    if (matches_group_key(item.expression)) {
      continue;
    }
    if (IsCountStarOrNonNullConst(item.expression)) {
      ++count_items;
      continue;
    }
    return changed;
  }
  if (count_items == 0) {
    return changed;
  }

  for (const auto& term : select->OrderBy()) {
    if (!term.expression) {
      return changed;
    }
    if (matches_group_key(term.expression) ||
        IsCountStarOrNonNullConst(term.expression)) {
      continue;
    }
    bool matches_alias = false;
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty()) {
        size_t hits = 0;
        for (const NamedExpression& item : select->SelectList()) {
          const std::string label =
              item.name.empty() ? ExplicitOrColumnOutputName(item) : item.name;
          if (!label.empty() && IdentifierEquals(label, cn.name)) {
            ++hits;
          }
        }
        matches_alias = (hits == 1);
      }
    }
    if (!matches_alias) {
      return changed;
    }
  }

  auto make_hist = [](const std::string& table_name,
                      const std::string& src_alias,
                      const std::string& key_name) {
    auto hist = std::make_shared<SelectStatement>();
    hist->SetSources({SelectSource(table_name, src_alias, nullptr,
                                   JoinType::kCross, nullptr)});
    Expression key_col = ColumnValueExp(ColumnName(src_alias, key_name));
    hist->SetSelectList({
        NamedExpression("__k", key_col),
        NamedExpression(
            "__cnt",
            AggregateExpressionExp(AggregationType::kCount,
                                   ColumnValueExp(ColumnName("", "*")), false)),
    });
    hist->SetGroupBy({key_col});
    return hist;
  };

  std::shared_ptr<SelectStatement> hist_l =
      make_hist(s0.table, "__ojc_ls", l_key_name);
  std::shared_ptr<SelectStatement> hist_r =
      make_hist(s1.table, "__ojc_rs", r_key_name);

  Expression join_cond = BinaryExpressionExp(
      ColumnValueExp(ColumnName("__ojc_l", "__k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("__ojc_r", "__k")));
  select->SetSources({
      SelectSource("", "__ojc_l", std::move(hist_l), JoinType::kCross, nullptr),
      SelectSource("", "__ojc_r", std::move(hist_r), jt, std::move(join_cond)),
  });

  Expression outer_key;
  Expression outer_cnt;
  if (jt == JoinType::kFull) {
    outer_key = FunctionCallExp("coalesce",
                                {ColumnValueExp(ColumnName("__ojc_l", "__k")),
                                 ColumnValueExp(ColumnName("__ojc_r", "__k"))});
    Expression prod = BinaryExpressionExp(
        FunctionCallExp("coalesce",
                        {ColumnValueExp(ColumnName("__ojc_l", "__cnt")),
                         ConstantValueExp(Value(int64_t{1}))}),
        BinaryOperation::kMultiply,
        FunctionCallExp("coalesce",
                        {ColumnValueExp(ColumnName("__ojc_r", "__cnt")),
                         ConstantValueExp(Value(int64_t{1}))}));
    outer_cnt =
        AggregateExpressionExp(AggregationType::kSum, std::move(prod), false);
    select->SetGroupBy({outer_key});
  } else if (jt == JoinType::kLeft) {
    outer_key = ColumnValueExp(ColumnName("__ojc_l", "__k"));
    outer_cnt = BinaryExpressionExp(
        ColumnValueExp(ColumnName("__ojc_l", "__cnt")),
        BinaryOperation::kMultiply,
        FunctionCallExp("coalesce",
                        {ColumnValueExp(ColumnName("__ojc_r", "__cnt")),
                         ConstantValueExp(Value(int64_t{1}))}));
    select->SetGroupBy({});
  } else {
    outer_key = ColumnValueExp(ColumnName("__ojc_l", "__k"));
    outer_cnt =
        BinaryExpressionExp(ColumnValueExp(ColumnName("__ojc_l", "__cnt")),
                            BinaryOperation::kMultiply,
                            ColumnValueExp(ColumnName("__ojc_r", "__cnt")));
    select->SetGroupBy({});
  }

  std::vector<NamedExpression> new_select;
  new_select.reserve(select->SelectList().size());
  for (const NamedExpression& item : select->SelectList()) {
    if (matches_group_key(item.expression)) {
      std::string out_name = item.name;
      if (out_name.empty()) {
        out_name = ExplicitOrColumnOutputName(item);
      }
      new_select.emplace_back(std::move(out_name), outer_key);
    } else {
      new_select.emplace_back(item.name, outer_cnt);
    }
  }
  select->SetSelectList(std::move(new_select));

  if (!select->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> new_order = select->OrderBy();
    for (auto& term : new_order) {
      if (matches_group_key(term.expression)) {
        term.expression = outer_key;
      } else if (IsCountStarOrNonNullConst(term.expression)) {
        term.expression = outer_cnt;
      }
    }
    select->SetOrderBy(std::move(new_order));
  }

  return true;
}

namespace {

bool ContainsExpensiveStringOp(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kCaseExp) {
    return true;
  }
  if (expr->Type() == TypeTag::kFunctionCallExp) {
    const std::string fn =
        LoweredStarName(expr->AsFunctionCallExpression().FuncName());
    if (fn.starts_with("extract") || fn == "date_part" || fn == "date_trunc" ||
        fn == "year" || fn == "month" || fn == "day" || fn == "hour" ||
        fn == "minute" || fn == "second") {
      return false;
    }
    return true;
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const BinaryOperation op = expr->AsBinaryExpression().Op();
    if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
      return true;
    }
  }
  return std::ranges::any_of(
      ExpressionChildren(expr),
      [](const Expression& child) { return ContainsExpensiveStringOp(child); });
}

Expression RequalifySingleSourceColumns(  // NOLINT(misc-no-recursion)
    const Expression& expr, const std::string& old_table,
    const std::string& old_alias, const std::string& new_alias) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = expr->AsColumnValue().GetColumnName();
    if (cn.schema.empty() || IdentifierEquals(cn.schema, old_table) ||
        (!old_alias.empty() && IdentifierEquals(cn.schema, old_alias))) {
      return ColumnValueExp(ColumnName(new_alias, cn.name));
    }
    return expr;
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  bool changed = false;
  std::vector<Expression> updated;
  updated.reserve(children.size());
  for (const Expression& child : children) {
    Expression next =
        RequalifySingleSourceColumns(child, old_table, old_alias, new_alias);
    if (next.get() != child.get()) {
      changed = true;
    }
    updated.push_back(std::move(next));
  }
  if (!changed) {
    return expr;
  }
  return WithExpressionChildren(expr, std::move(updated));
}

bool IsHoistableGroupedDerived(const SelectSource& src) {
  if (src.query == nullptr || src.is_lateral || src.unnest ||
      src.sample.has_value() || !src.using_columns.empty() ||
      src.from_nested_join || src.join_condition != nullptr) {
    return false;
  }
  const SelectStatement& q = *src.query;
  if (q.GroupBy().empty() || !q.WithQueries().empty() ||
      !q.UnionAll().empty() || q.GetSetOperationTree() != nullptr ||
      q.HasLimit() || q.Offset() != 0 || q.Qualify() || q.Distinct() ||
      !q.DistinctOn().empty() || relational_detail::HasWindowFunctions(q) ||
      q.Sources().empty()) {
    return false;
  }
  for (const SelectSource& bs : q.Sources()) {
    if (bs.table.empty() || bs.query != nullptr || bs.unnest || bs.is_lateral ||
        bs.sample.has_value() || !bs.using_columns.empty() ||
        bs.from_nested_join) {
      return false;
    }
    if (bs.join_condition && ContainsQueryExpression(bs.join_condition)) {
      return false;
    }
  }
  if (q.WhereClause() && ContainsQueryExpression(q.WhereClause())) {
    return false;
  }
  if (q.Having() && ContainsQueryExpression(q.Having())) {
    return false;
  }
  for (const Expression& g : q.GroupBy()) {
    if (!g || ContainsQueryExpression(g)) {
      return false;
    }
  }
  return std::ranges::all_of(q.SelectList(), [](const NamedExpression& item) {
    return item.expression && !ContainsQueryExpression(item.expression);
  });
}

std::string CanonicalGroupedDerivedFingerprint(const SelectStatement& q) {
  std::string fp;
  fp.reserve(256);
  fp += "S:";
  for (const SelectSource& bs : q.Sources()) {
    fp += bs.table;
    fp += '/';
    fp += bs.alias;
    fp += '/';
    fp += std::to_string(static_cast<int>(bs.join_type));
    fp += '/';
    if (bs.join_condition) {
      fp += bs.join_condition->ToString();
    }
    fp += ';';
  }
  fp += "|W:";
  if (q.WhereClause()) {
    fp += q.WhereClause()->ToString();
  }
  fp += "|G:";
  for (const Expression& g : q.GroupBy()) {
    fp += g->ToString();
    fp += ',';
  }
  fp += "|H:";
  if (q.Having()) {
    fp += q.Having()->ToString();
  }
  fp += "|P:";
  for (const NamedExpression& item : q.SelectList()) {
    fp += item.name;
    fp += '=';
    fp += item.expression->ToString();
    fp += ',';
  }
  return fp;
}

}  // namespace

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
//   (GroupAvgThresholdTwoPassSplit_sound)
bool HoistDuplicateDerivedTablesToCte(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() < 2) {
    return false;
  }
  auto& top_sources = const_cast<std::vector<SelectSource>&>(select->Sources());
  std::unordered_map<std::string, std::vector<SelectSource*>> by_fp;
  std::vector<std::string> fp_order;
  const auto record = [&](SelectSource* s) {
    if (!IsHoistableGroupedDerived(*s)) {
      return;
    }
    std::string fp = CanonicalGroupedDerivedFingerprint(*s->query);
    auto& bucket = by_fp[fp];
    if (bucket.empty()) {
      fp_order.push_back(std::move(fp));
    }
    bucket.push_back(s);
  };
  for (SelectSource& src : top_sources) {
    if (src.query == nullptr) {
      continue;
    }
    record(&src);
    if (!src.is_lateral && !src.unnest && !src.sample.has_value() &&
        src.query->WithQueries().empty() && src.query->UnionAll().empty() &&
        src.query->GetSetOperationTree() == nullptr) {
      auto& inner_sources =
          const_cast<std::vector<SelectSource>&>(src.query->Sources());
      for (SelectSource& isrc : inner_sources) {
        if (isrc.query != nullptr) {
          record(&isrc);
        }
      }
    }
  }

  bool hoisted = false;
  size_t next_id = 0;
  for (const std::string& fp : fp_order) {
    auto& sites = by_fp[fp];
    if (sites.size() < 2) {
      continue;
    }
    std::string cte_name;
    do {
      cte_name = "__cse_dt_" + std::to_string(next_id++);
    } while (select->WithQueries().contains(cte_name));
    auto body = std::make_shared<SelectStatement>(*sites[0]->query);
    select->AddWithQuery(cte_name, std::move(body));
    for (SelectSource* site : sites) {
      site->table = cte_name;
      site->query.reset();
    }
    hoisted = true;
  }
  return hoisted;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/CloudBI/PublicBI.lean
//   (sound_GroupBeforeSuffix)
bool PreGroupBeforeExpressionGroupBy(SelectStatement* select,
                                     TransactionContext& ctx) {
  if (select == nullptr || select->Sources().size() != 1 ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->Having() ||
      select->Qualify() || select->Distinct() || select->HasLimit() ||
      select->Offset() != 0 || !select->DistinctOn().empty() ||
      relational_detail::HasWindowFunctions(*select) ||
      select->GroupBy().empty() || select->GroupBy().size() > 4) {
    return false;
  }
  const SelectSource& src = select->Sources().front();
  if (src.table.empty() || src.query != nullptr || src.unnest ||
      src.is_lateral || src.sample.has_value() || !src.using_columns.empty() ||
      src.from_nested_join || src.join_condition != nullptr) {
    return false;
  }
  if (select->WhereClause() && ContainsQueryExpression(select->WhereClause())) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> table_or = ctx.GetTable(src.table);
  if (!table_or.HasValue()) {
    return false;
  }
  const Schema& schema = table_or.Value()->GetSchema();
  const std::string src_qual = src.alias.empty() ? src.table : src.alias;

  std::vector<std::string> base_cols;
  std::unordered_set<std::string> base_col_set;
  bool has_expensive_varchar_key = false;
  std::unordered_set<std::string> group_expr_strs;
  for (const Expression& key : select->GroupBy()) {
    if (!key || ContainsQueryExpression(key)) {
      return false;
    }
    group_expr_strs.insert(key->ToString());
    const bool is_expr_key = (key->Type() != TypeTag::kColumnValue);
    const bool is_expensive = is_expr_key && ContainsExpensiveStringOp(key);
    bool touches_varchar = false;
    for (const ColumnName& cn : key->TouchedColumns()) {
      if (!cn.schema.empty() && !IdentifierEquals(cn.schema, src.table) &&
          (src.alias.empty() || !IdentifierEquals(cn.schema, src.alias))) {
        return false;
      }
      const int off = schema.Offset(ColumnName("", cn.name));
      if (off < 0) {
        return false;
      }
      const Column& col_meta = schema.GetColumn(static_cast<slot_t>(off));
      if (col_meta.Type() == ValueType::kVarChar) {
        touches_varchar = true;
      }
      const std::string canonical_name = col_meta.Name().name;
      if (base_col_set.insert(LoweredStarName(canonical_name)).second) {
        base_cols.push_back(canonical_name);
      }
    }
    if (is_expensive && touches_varchar) {
      has_expensive_varchar_key = true;
    }
  }
  if (!has_expensive_varchar_key || base_cols.empty() || base_cols.size() > 3) {
    return false;
  }

  size_t count_items = 0;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.expression || ContainsQueryExpression(item.expression)) {
      return false;
    }
    if (IsCountStarOrNonNullConst(item.expression)) {
      ++count_items;
      continue;
    }
    if (group_expr_strs.contains(item.expression->ToString())) {
      continue;
    }
    return false;
  }
  if (count_items == 0) {
    return false;
  }

  for (const auto& term : select->OrderBy()) {
    if (!term.expression || ContainsQueryExpression(term.expression)) {
      return false;
    }
    if (group_expr_strs.contains(term.expression->ToString()) ||
        IsCountStarOrNonNullConst(term.expression)) {
      continue;
    }
    bool matches_alias = false;
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty()) {
        size_t hits = 0;
        for (const NamedExpression& item : select->SelectList()) {
          const std::string label =
              item.name.empty() ? ExplicitOrColumnOutputName(item) : item.name;
          if (!label.empty() && IdentifierEquals(label, cn.name)) {
            ++hits;
          }
        }
        matches_alias = (hits == 1);
      }
    }
    if (!matches_alias) {
      return false;
    }
  }

  constexpr const char* kPreAlias = "__pre_g";
  constexpr const char* kPreCnt = "__pre_cnt";
  auto inner = std::make_shared<SelectStatement>();
  inner->SetSources({src});
  if (select->WhereClause()) {
    inner->SetWhereClause(select->WhereClause());
  }
  std::vector<NamedExpression> inner_select;
  std::vector<Expression> inner_group;
  inner_select.reserve(base_cols.size() + 1);
  inner_group.reserve(base_cols.size());
  for (const std::string& col : base_cols) {
    Expression col_ref = ColumnValueExp(ColumnName(src_qual, col));
    inner_select.emplace_back(col, col_ref);
    inner_group.push_back(std::move(col_ref));
  }
  inner_select.emplace_back(
      kPreCnt,
      AggregateExpressionExp(AggregationType::kCount,
                             ColumnValueExp(ColumnName("", "*")), false));
  inner->SetSelectList(std::move(inner_select));
  inner->SetGroupBy(std::move(inner_group));

  Expression outer_sum = AggregateExpressionExp(
      AggregationType::kSum, ColumnValueExp(ColumnName(kPreAlias, kPreCnt)),
      false);

  std::vector<Expression> new_group;
  new_group.reserve(select->GroupBy().size());
  for (const Expression& key : select->GroupBy()) {
    new_group.push_back(
        RequalifySingleSourceColumns(key, src.table, src.alias, kPreAlias));
  }

  std::vector<NamedExpression> new_select;
  new_select.reserve(select->SelectList().size());
  for (const NamedExpression& item : select->SelectList()) {
    if (IsCountStarOrNonNullConst(item.expression)) {
      new_select.emplace_back(item.name, outer_sum);
    } else {
      std::string out_name = item.name;
      if (out_name.empty()) {
        out_name = ExplicitOrColumnOutputName(item);
      }
      new_select.emplace_back(
          std::move(out_name),
          RequalifySingleSourceColumns(item.expression, src.table, src.alias,
                                       kPreAlias));
    }
  }

  if (!select->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> new_order = select->OrderBy();
    for (auto& term : new_order) {
      if (IsCountStarOrNonNullConst(term.expression)) {
        term.expression = outer_sum;
      } else if (group_expr_strs.contains(term.expression->ToString())) {
        term.expression = RequalifySingleSourceColumns(
            term.expression, src.table, src.alias, kPreAlias);
      }
    }
    select->SetOrderBy(std::move(new_order));
  }

  select->SetSources({SelectSource("", kPreAlias, std::move(inner),
                                   JoinType::kCross, nullptr)});
  select->SetWhereClause(nullptr);
  select->SetGroupBy(std::move(new_group));
  select->SetSelectList(std::move(new_select));
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/CloudBI/PublicBI.lean
//   (sound_ConstantGroup)
bool CollapseConstantGroupBy(SelectStatement* select, TransactionContext& ctx) {
  if (select == nullptr || select->Sources().size() != 1 ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->Having() ||
      select->Qualify() || select->Distinct() ||
      !select->DistinctOn().empty() ||
      relational_detail::HasWindowFunctions(*select) ||
      select->GroupBy().empty() || !select->WhereClause() ||
      ContainsQueryExpression(select->WhereClause())) {
    return false;
  }
  const SelectSource& src = select->Sources().front();
  if (src.table.empty() || src.query != nullptr || src.unnest ||
      src.is_lateral || src.sample.has_value() || !src.using_columns.empty() ||
      src.from_nested_join || src.join_condition != nullptr) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> table_or = ctx.GetTable(src.table);
  if (!table_or.HasValue()) {
    return false;
  }
  const Schema& schema = table_or.Value()->GetSchema();

  auto matches_src = [&](const std::string& qual) {
    return qual.empty() || IdentifierEquals(qual, src.table) ||
           (!src.alias.empty() && IdentifierEquals(qual, src.alias));
  };

  std::unordered_map<std::string, Expression> const_by_col;
  for (const Expression& conj : SplitConjuncts(select->WhereClause())) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kEquals) {
      continue;
    }
    const Expression* col_expr = nullptr;
    const Expression* val_expr = nullptr;
    if (bin.Left()->Type() == TypeTag::kColumnValue &&
        bin.Right()->Type() == TypeTag::kConstantValue) {
      col_expr = &bin.Left();
      val_expr = &bin.Right();
    } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
               bin.Left()->Type() == TypeTag::kConstantValue) {
      col_expr = &bin.Right();
      val_expr = &bin.Left();
    } else {
      continue;
    }
    const ColumnName& cn = (*col_expr)->AsColumnValue().GetColumnName();
    if (!matches_src(cn.schema)) {
      continue;
    }
    const int off = schema.Offset(ColumnName("", cn.name));
    if (off < 0) {
      continue;
    }
    const Value& val = (*val_expr)->AsConstantValue().GetValue();
    if (val.IsNull() ||
        val.type != schema.GetColumn(static_cast<slot_t>(off)).Type()) {
      continue;
    }
    const_by_col.try_emplace(LoweredStarName(cn.name), *val_expr);
  }
  if (const_by_col.empty()) {
    return false;
  }

  for (const Expression& key : select->GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& cn = key->AsColumnValue().GetColumnName();
    if (!matches_src(cn.schema) ||
        !const_by_col.contains(LoweredStarName(cn.name))) {
      return false;
    }
  }

  size_t agg_count = 0;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.expression || ContainsQueryExpression(item.expression)) {
      return false;
    }
    if (item.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = item.expression->AsColumnValue().GetColumnName();
      if (!matches_src(cn.schema) ||
          !const_by_col.contains(LoweredStarName(cn.name))) {
        return false;
      }
      continue;
    }
    if (item.expression->Type() == TypeTag::kAggregateExp) {
      const auto& agg = item.expression->AsAggregateExpression();
      if (agg.NeedsGroupContext()) {
        return false;
      }
      ++agg_count;
      continue;
    }
    return false;
  }
  if (agg_count == 0) {
    return false;
  }

  constexpr const char* kCgAlias = "__cg";
  constexpr const char* kCgCnt = "__cg_cnt";
  auto inner = std::make_shared<SelectStatement>();
  inner->SetSources({src});
  inner->SetWhereClause(select->WhereClause());

  std::vector<NamedExpression> inner_select;
  inner_select.reserve(agg_count + 1);
  inner_select.emplace_back(
      kCgCnt,
      AggregateExpressionExp(AggregationType::kCount,
                             ColumnValueExp(ColumnName("", "*")), false));

  std::vector<NamedExpression> outer_select;
  outer_select.reserve(select->SelectList().size());
  size_t next_agg = 0;
  for (const NamedExpression& item : select->SelectList()) {
    std::string out_name = item.name;
    if (out_name.empty()) {
      out_name = ExplicitOrColumnOutputName(item);
    }
    if (item.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = item.expression->AsColumnValue().GetColumnName();
      outer_select.emplace_back(std::move(out_name),
                                const_by_col.at(LoweredStarName(cn.name)));
    } else {
      const std::string slot_name = "__cg_a_" + std::to_string(next_agg++);
      inner_select.emplace_back(slot_name, item.expression);
      outer_select.emplace_back(
          std::move(out_name), ColumnValueExp(ColumnName(kCgAlias, slot_name)));
    }
  }
  inner->SetSelectList(std::move(inner_select));

  select->SetSources({SelectSource("", kCgAlias, std::move(inner),
                                   JoinType::kCross, nullptr)});
  select->SetWhereClause(BinaryExpressionExp(
      ColumnValueExp(ColumnName(kCgAlias, kCgCnt)),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t{0}))));
  select->SetGroupBy({});
  select->SetOrderBy({});
  select->SetSelectList(std::move(outer_select));
  return true;
}

namespace {

int SourceIndexForColumn(const ColumnName& cn,
                         const std::vector<SelectSource>& sources) {
  if (cn.schema.empty()) {
    return -1;
  }
  for (size_t i = 0; i < sources.size(); ++i) {
    const std::string& alias =
        sources[i].alias.empty() ? sources[i].table : sources[i].alias;
    if (IdentifierEquals(cn.schema, alias) ||
        IdentifierEquals(cn.schema, sources[i].table)) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

uint32_t TouchedSourceMask(const Expression& expr,
                           const std::vector<SelectSource>& sources) {
  if (!expr) {
    return 0;
  }
  uint32_t mask = 0;
  for (const ColumnName& cn : expr->TouchedColumns()) {
    const int idx = SourceIndexForColumn(cn, sources);
    if (idx < 0) {
      return ~uint32_t{0};
    }
    mask |= (uint32_t{1} << static_cast<uint32_t>(idx));
  }
  return mask;
}

bool TryFactorThreeTableInSubquery(SelectStatement* sq) {
  if (sq == nullptr || sq->Sources().size() != 3 ||
      !sq->WithQueries().empty() || !sq->GroupBy().empty() || sq->Having() ||
      sq->Distinct() || !sq->DistinctOn().empty() || sq->HasLimit() ||
      sq->Offset() != 0 || sq->Qualify() || !sq->UnionAll().empty() ||
      sq->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*sq) ||
      sq->SelectList().size() != 1) {
    return false;
  }
  const auto& sources = sq->Sources();
  for (const SelectSource& s : sources) {
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kInner && s.join_type != JoinType::kCross)) {
      return false;
    }
  }
  if (TouchedSourceMask(sq->SelectList()[0].expression, sources) != 1U) {
    return false;
  }

  std::vector<Expression> all_conj;
  for (size_t i = 1; i < 3; ++i) {
    if (sources[i].join_condition) {
      auto parts = SplitConjuncts(sources[i].join_condition);
      all_conj.insert(all_conj.end(), parts.begin(), parts.end());
    }
  }
  if (sq->WhereClause()) {
    auto parts = SplitConjuncts(sq->WhereClause());
    all_conj.insert(all_conj.end(), parts.begin(), parts.end());
  }

  std::vector<Expression> x_only;
  std::vector<Expression> xf_conds;
  std::vector<Expression> xe_conds;
  std::vector<Expression> fe_conds;
  for (const Expression& c : all_conj) {
    if (!c || ContainsQueryExpression(c)) {
      return false;
    }
    const uint32_t m = TouchedSourceMask(c, sources);
    if (m == ~uint32_t{0} || m == 7U) {
      return false;
    }
    if (m == 0U || m == 1U) {
      x_only.push_back(c);
    } else if (m == 3U) {
      xf_conds.push_back(c);
    } else if (m == 5U) {
      xe_conds.push_back(c);
    } else {
      // m in {2, 4, 6}: touches only F and/or E.
      fe_conds.push_back(c);
    }
  }
  if (xf_conds.empty() || xe_conds.empty() || fe_conds.empty()) {
    return false;
  }

  // Require at least one expression equijoin between X and E (`expr(X) =
  // expr(E)` where `expr(E)` is non-column, e.g. `COALESCE(x.k,-1) =
  // COALESCE(e.k,-1)`), and all XF/XE conditions to be binary equalities
  // separating X from F or E.
  struct JoinPair {
    Expression x_side;
    Expression right_side;
    bool from_e{false};
  };
  std::vector<JoinPair> join_pairs;
  bool has_expr_eq_e = false;
  for (const Expression& c : xf_conds) {
    if (c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& b = c->AsBinaryExpression();
    const uint32_t lm = TouchedSourceMask(b.Left(), sources);
    const uint32_t rm = TouchedSourceMask(b.Right(), sources);
    if (lm == 1U && rm == 2U) {
      join_pairs.push_back({b.Left(), b.Right(), false});
    } else if (lm == 2U && rm == 1U) {
      join_pairs.push_back({b.Right(), b.Left(), false});
    } else {
      return false;
    }
  }
  for (const Expression& c : xe_conds) {
    if (c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& b = c->AsBinaryExpression();
    const uint32_t lm = TouchedSourceMask(b.Left(), sources);
    const uint32_t rm = TouchedSourceMask(b.Right(), sources);
    if (lm == 1U && rm == 4U) {
      if (b.Right()->Type() != TypeTag::kColumnValue) {
        has_expr_eq_e = true;
      }
      join_pairs.push_back({b.Left(), b.Right(), true});
    } else if (lm == 4U && rm == 1U) {
      if (b.Left()->Type() != TypeTag::kColumnValue) {
        has_expr_eq_e = true;
      }
      join_pairs.push_back({b.Right(), b.Left(), true});
    } else {
      return false;
    }
  }
  if (!has_expr_eq_e) {
    return false;
  }

  constexpr const char* kFkAlias = "__fk_del";
  auto inner = std::make_shared<SelectStatement>();
  SelectSource s_f = sources[1];
  SelectSource s_e = sources[2];
  s_f.join_type = JoinType::kCross;
  s_f.join_condition = nullptr;
  s_e.join_type = JoinType::kCross;
  s_e.join_condition = nullptr;
  inner->SetSources({std::move(s_f), std::move(s_e)});
  inner->SetWhereClause(CombineConjuncts(fe_conds));

  std::vector<NamedExpression> inner_select;
  std::vector<Expression> inner_group;
  std::vector<Expression> outer_join_conds;
  inner_select.reserve(join_pairs.size());
  inner_group.reserve(join_pairs.size());
  outer_join_conds.reserve(join_pairs.size());
  for (size_t i = 0; i < join_pairs.size(); ++i) {
    const std::string col_name = "__fk_k" + std::to_string(i);
    inner_select.emplace_back(col_name, join_pairs[i].right_side);
    inner_group.push_back(join_pairs[i].right_side);
    outer_join_conds.push_back(
        BinaryExpressionExp(join_pairs[i].x_side, BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName(kFkAlias, col_name))));
  }
  inner->SetSelectList(std::move(inner_select));
  inner->SetGroupBy(std::move(inner_group));

  SelectSource s_x = sources[0];
  sq->SetSources({
      std::move(s_x),
      SelectSource("", kFkAlias, std::move(inner), JoinType::kInner,
                   CombineConjuncts(outer_join_conds)),
  });
  sq->SetWhereClause(x_only.empty() ? Expression{} : CombineConjuncts(x_only));
  return true;
}

}  // namespace

// Lean proof: lean/TinylambOpt/ResearchProofs/DuckDBGaps/FileMasks.lean
//   (FileKeyAntiJoin_sound)
bool FactorDeleteAntiJoinSubquery(SelectStatement* select) {
  if (select == nullptr || !select->WhereClause()) {
    return false;
  }
  bool changed = false;
  for (const Expression& conj : SplitConjuncts(select->WhereClause())) {
    if (!conj) {
      continue;
    }
    Expression target = conj;
    if (target->Type() == TypeTag::kUnaryExp &&
        target->AsUnaryExpression().Op() == UnaryOperation::kNot) {
      target = target->AsUnaryExpression().Child();
    }
    if (!target || target->Type() != TypeTag::kQueryExp) {
      continue;
    }
    const auto& qe = target->AsQueryExpression();
    if (qe.Exists() || qe.ArrayResult() || !qe.Test() ||
        qe.Query() == nullptr) {
      continue;
    }
    if (TryFactorThreeTableInSubquery(
            const_cast<SelectStatement*>(qe.Query().get()))) {
      changed = true;
    }
  }
  return changed;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
//   (SurrogateKeyPreAggRollup_sound)
bool PreAggregateSurrogateKeyBeforeRollup(  // NOLINT(misc-no-recursion)
    SelectStatement* outer, TransactionContext& ctx) {
  if (outer == nullptr) {
    return false;
  }
  bool any_changed = false;
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr) {
      any_changed |= PreAggregateSurrogateKeyBeforeRollup(body.get(), ctx);
    }
  }
  for (const SelectSource& src : outer->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      any_changed |= PreAggregateSurrogateKeyBeforeRollup(src.query.get(), ctx);
    }
  }
  if (outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      outer->Distinct() || outer->HasDistinctOn() || outer->Having() ||
      outer->Qualify() || outer->GroupBy().size() < 2 ||
      outer->Sources().size() < 2 ||
      relational_detail::HasWindowFunctions(*outer) ||
      relational_detail::StatementContainsVolatileFunction(*outer)) {
    return any_changed;
  }

  // Collect all branches (head + UnionAll) and verify identical FROM+WHERE
  // input shape across every UNION ALL branch (ROLLUP / GROUPING SETS shape).
  std::vector<SelectStatement*> branches;
  branches.reserve(1 + outer->UnionAll().size());
  branches.push_back(outer);
  auto input_shape = [](const SelectStatement& part) {
    SelectStatement shape(part);
    shape.SetSelectList({});
    shape.SetGroupBy({});
    shape.SetGroupAll(false);
    shape.SetHaving(nullptr);
    shape.SetOrderBy({});
    shape.SetLimit(std::nullopt);
    shape.SetOffset(0);
    shape.SetWithTies(false);
    shape.ClearUnionAll();
    shape.SetSetOperationTree(nullptr);
    shape.ClearWithQueries();
    return std::string(shape.Fingerprint());
  };
  const std::string head_shape = input_shape(*outer);
  for (size_t i = 0; i < outer->UnionAll().size(); ++i) {
    const auto& br = outer->UnionAll()[i];
    if (br == nullptr) {
      return false;
    }
    const auto match = outer->MatchFor(i);
    if (br->Distinct() || br->HasDistinctOn() || br->Having() ||
        br->Qualify() || br->HasLimit() || br->Offset() != 0 ||
        !br->OrderBy().empty() || !br->WithQueries().empty() ||
        !br->UnionAll().empty() || br->GetSetOperationTree() != nullptr ||
        match.by_name || match.corresponding || !match.columns.empty() ||
        (i < outer->SetOperationKinds().size() &&
         outer->SetOperationKinds()[i] != SetOperationKind::kUnionAll) ||
        relational_detail::HasWindowFunctions(*br) ||
        relational_detail::StatementContainsVolatileFunction(*br) ||
        input_shape(*br) != head_shape) {
      return false;
    }
    branches.push_back(br.get());
  }

  const auto& sources = outer->Sources();
  std::vector<std::shared_ptr<Table>> tables;
  tables.reserve(sources.size());
  for (const SelectSource& s : sources) {
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kInner && s.join_type != JoinType::kCross)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(s.table);
    if (!t.HasValue() || t.Value() == nullptr) {
      return false;
    }
    tables.push_back(t.MoveValue());
  }

  auto resolve_source_idx = [&](const ColumnName& cn) -> int {
    if (!cn.schema.empty()) {
      for (size_t i = 0; i < sources.size(); ++i) {
        const std::string& al =
            sources[i].alias.empty() ? sources[i].table : sources[i].alias;
        if (IdentifierEquals(cn.schema, al) ||
            IdentifierEquals(cn.schema, sources[i].table)) {
          return static_cast<int>(i);
        }
      }
      return -1;
    }
    int found = -1;
    for (size_t i = 0; i < tables.size(); ++i) {
      if (tables[i]->GetSchema().Offset(ColumnName("", cn.name)) >= 0) {
        if (found >= 0) {
          return -1;
        }
        found = static_cast<int>(i);
      }
    }
    return found;
  };

  // All GROUP BY columns across every branch must belong to one single
  // dimension source `dim_idx`.
  int dim_idx = -1;
  for (const SelectStatement* br : branches) {
    for (const Expression& gk : br->GroupBy()) {
      if (!gk) {
        return false;
      }
      if (br->IsGroupAll() && gk->Type() == TypeTag::kConstantValue) {
        continue;
      }
      if (gk->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const int si = resolve_source_idx(gk->AsColumnValue().GetColumnName());
      if (si < 0) {
        return false;
      }
      if (dim_idx < 0) {
        dim_idx = si;
      } else if (dim_idx != si) {
        return false;
      }
    }
  }
  if (dim_idx < 0) {
    return false;
  }

  // Collect all WHERE + JOIN ON conjuncts and verify that `dim_idx` is touched
  // by exactly one equijoin `fk_col = dim_pk` (no other filters on `dim_idx`).
  std::vector<Expression> all_conj;
  for (const SelectSource& s : sources) {
    if (s.join_condition) {
      auto parts = SplitConjuncts(s.join_condition);
      all_conj.insert(all_conj.end(), parts.begin(), parts.end());
    }
  }
  if (outer->WhereClause()) {
    auto parts = SplitConjuncts(outer->WhereClause());
    all_conj.insert(all_conj.end(), parts.begin(), parts.end());
  }

  Expression fk_expr;
  Expression dim_pk_expr;
  std::vector<Expression> pre_where;
  size_t dim_conj_count = 0;
  for (const Expression& c : all_conj) {
    if (!c || ContainsQueryExpression(c)) {
      return false;
    }
    bool touches_dim = false;
    for (const ColumnName& cn : c->TouchedColumns()) {
      const int si = resolve_source_idx(cn);
      if (si < 0) {
        return false;
      }
      if (si == dim_idx) {
        touches_dim = true;
      }
    }
    if (!touches_dim) {
      pre_where.push_back(c);
      continue;
    }
    ++dim_conj_count;
    if (dim_conj_count > 1 || c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& bin = c->AsBinaryExpression();
    if (bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const int lsi =
        resolve_source_idx(bin.Left()->AsColumnValue().GetColumnName());
    const int rsi =
        resolve_source_idx(bin.Right()->AsColumnValue().GetColumnName());
    if (lsi == dim_idx && rsi >= 0 && rsi != dim_idx) {
      dim_pk_expr = bin.Left();
      fk_expr = bin.Right();
    } else if (rsi == dim_idx && lsi >= 0 && lsi != dim_idx) {
      dim_pk_expr = bin.Right();
      fk_expr = bin.Left();
    } else {
      return false;
    }
  }
  if (dim_conj_count != 1 || !fk_expr || !dim_pk_expr) {
    return false;
  }

  const std::string& dim_table = sources[static_cast<size_t>(dim_idx)].table;
  const std::string dim_pk_name =
      dim_pk_expr->AsColumnValue().GetColumnName().name;
  const RuntimeUniqueColInfo uinfo =
      InspectRuntimeUniqueNonNullColumn(ctx, dim_table, dim_pk_name);
  if (!uinfo.unique_non_null) {
    return false;
  }

  // Verify every SELECT item across every branch is either a constant/NULL,
  // a bare column from `dim_idx`, or a decomposable aggregate (`AVG(col)`,
  // `SUM(col)`, `COUNT(col)`, `COUNT(*)`) whose argument does not touch
  // `dim_idx`.
  struct PreAggEntry {
    AggregationType type{AggregationType::kCount};
    Expression arg;  // nullptr for COUNT(*)
    std::string sum_col;
    std::string cnt_col;
  };
  std::vector<PreAggEntry> agg_entries;
  std::unordered_map<std::string, size_t> agg_by_str;

  for (const SelectStatement* br : branches) {
    for (const NamedExpression& item : br->SelectList()) {
      if (!item.expression || ContainsQueryExpression(item.expression)) {
        return false;
      }
      const Expression& e = item.expression;
      if (e->Type() == TypeTag::kConstantValue) {
        continue;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        if (resolve_source_idx(e->AsColumnValue().GetColumnName()) != dim_idx) {
          return false;
        }
        continue;
      }
      if (e->Type() == TypeTag::kAggregateExp) {
        const auto& agg = e->AsAggregateExpression();
        if (agg.Distinct() || agg.NeedsGroupContext()) {
          return false;
        }
        const AggregationType at = agg.GetType();
        if (at != AggregationType::kAvg && at != AggregationType::kSum &&
            at != AggregationType::kCount) {
          return false;
        }
        const Expression& arg = agg.Child();
        const bool is_count_star =
            at == AggregationType::kCount &&
            (!arg || (arg->Type() == TypeTag::kColumnValue &&
                      arg->AsColumnValue().GetColumnName().name == "*"));
        if (!is_count_star) {
          if (!arg || arg->Type() != TypeTag::kColumnValue) {
            return false;
          }
          const int asi =
              resolve_source_idx(arg->AsColumnValue().GetColumnName());
          if (asi < 0 || asi == dim_idx) {
            return false;
          }
          const ColumnName& acn = arg->AsColumnValue().GetColumnName();
          const Schema& asch = tables[static_cast<size_t>(asi)]->GetSchema();
          const int aoff = asch.Offset(ColumnName("", acn.name));
          if (aoff < 0) {
            return false;
          }
          const Column& acol = asch.GetColumn(static_cast<slot_t>(aoff));
          if (at != AggregationType::kCount &&
              (acol.IsUnsigned() || acol.IsInterval() ||
               (acol.Type() != ValueType::kInt64 &&
                acol.Type() != ValueType::kDouble))) {
            return false;
          }
        }
        const std::string key = e->ToString();
        if (!agg_by_str.contains(key)) {
          const size_t idx = agg_entries.size();
          agg_by_str.emplace(key, idx);
          PreAggEntry entry;
          entry.type = at;
          entry.arg = is_count_star ? Expression{} : arg;
          if (at == AggregationType::kAvg || at == AggregationType::kSum) {
            entry.sum_col = "__rp_s" + std::to_string(idx);
          }
          if (at == AggregationType::kAvg || at == AggregationType::kCount) {
            entry.cnt_col = "__rp_c" + std::to_string(idx);
          }
          agg_entries.push_back(std::move(entry));
        }
        continue;
      }
      // Allow `CAST(NULL AS ...)` emitted by ROLLUP expansion.
      if (e->TouchedColumns().empty()) {
        continue;
      }
      return false;
    }
  }
  if (agg_entries.empty()) {
    return false;
  }

  std::string cte_name = "__rollup_pre";
  for (size_t k = 0; outer->WithQueries().contains(cte_name); ++k) {
    cte_name = "__rollup_pre_" + std::to_string(k);
  }
  constexpr const char* kRpAlias = "__rp";
  constexpr const char* kRpFk = "__rp_fk";

  auto pre = std::make_shared<SelectStatement>();
  std::vector<SelectSource> pre_sources;
  pre_sources.reserve(sources.size() - 1);
  for (size_t i = 0; i < sources.size(); ++i) {
    if (std::cmp_equal(i, dim_idx)) {
      continue;
    }
    SelectSource s = sources[i];
    s.join_type = JoinType::kCross;
    s.join_condition = nullptr;
    pre_sources.push_back(std::move(s));
  }
  pre->SetSources(std::move(pre_sources));
  pre_where.push_back(UnaryExpressionExp(fk_expr, UnaryOperation::kIsNotNull));
  pre->SetWhereClause(CombineConjuncts(pre_where));
  pre->SetGroupBy({fk_expr});

  std::vector<NamedExpression> pre_select;
  pre_select.emplace_back(kRpFk, fk_expr);
  for (const PreAggEntry& entry : agg_entries) {
    if (!entry.sum_col.empty()) {
      pre_select.emplace_back(
          entry.sum_col,
          AggregateExpressionExp(AggregationType::kSum, entry.arg, false));
    }
    if (!entry.cnt_col.empty()) {
      Expression cnt_arg =
          entry.arg ? entry.arg : ColumnValueExp(ColumnName("", "*"));
      pre_select.emplace_back(
          entry.cnt_col, AggregateExpressionExp(AggregationType::kCount,
                                                std::move(cnt_arg), false));
    }
  }
  pre->SetSelectList(std::move(pre_select));
  outer->AddWithQuery(cte_name, std::move(pre));

  SelectSource dim_src = sources[static_cast<size_t>(dim_idx)];
  dim_src.join_type = JoinType::kCross;
  dim_src.join_condition = nullptr;
  Expression rp_join_eq =
      BinaryExpressionExp(ColumnValueExp(ColumnName(kRpAlias, kRpFk)),
                          BinaryOperation::kEquals, dim_pk_expr);

  for (SelectStatement* br : branches) {
    br->SetSources({
        SelectSource(cte_name, kRpAlias, nullptr, JoinType::kCross, nullptr),
        dim_src,
    });
    br->SetWhereClause(rp_join_eq);
    std::vector<NamedExpression> new_sel;
    new_sel.reserve(br->SelectList().size());
    for (const NamedExpression& item : br->SelectList()) {
      if (item.expression &&
          item.expression->Type() == TypeTag::kAggregateExp) {
        const size_t idx = agg_by_str.at(item.expression->ToString());
        const PreAggEntry& entry = agg_entries[idx];
        Expression combined;
        if (entry.type == AggregationType::kAvg) {
          Expression sum_s = AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(ColumnName(kRpAlias, entry.sum_col)), false);
          Expression sum_c = AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(ColumnName(kRpAlias, entry.cnt_col)), false);
          Expression is_zero =
              BinaryExpressionExp(sum_c, BinaryOperation::kEquals,
                                  ConstantValueExp(Value(int64_t{0})));
          Expression num =
              BinaryExpressionExp(ConstantValueExp(Value(1.0)),
                                  BinaryOperation::kMultiply, std::move(sum_s));
          Expression ratio = BinaryExpressionExp(
              std::move(num), BinaryOperation::kDivide, std::move(sum_c));
          combined = CaseExpressionExp(
              {{std::move(is_zero), ConstantValueExp(Value())}},
              std::move(ratio));
        } else if (entry.type == AggregationType::kSum) {
          combined = AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(ColumnName(kRpAlias, entry.sum_col)), false);
        } else {
          combined = FunctionCallExp(
              "coalesce",
              {AggregateExpressionExp(
                   AggregationType::kSum,
                   ColumnValueExp(ColumnName(kRpAlias, entry.cnt_col)), false),
               ConstantValueExp(Value(int64_t{0}))});
        }
        new_sel.emplace_back(item.name, std::move(combined));
      } else {
        new_sel.push_back(item);
      }
    }
    br->SetSelectList(std::move(new_sel));
  }
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/CloudBI/NestedBI.lean
//   (sound_FlattenSum, sound_BranchHistogram)
bool FoldNestedSumUnionAllBranches(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() != 1 ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->WhereClause() ||
      !select->GroupBy().empty() || select->Having() || select->Qualify() ||
      select->Distinct() || select->HasLimit() || select->Offset() != 0 ||
      !select->OrderBy().empty() || select->SelectList().size() != 1) {
    return false;
  }
  const auto is_simple_sum_of_col = [](const SelectStatement& q,
                                       std::string* col_name_out) -> bool {
    if (q.SelectList().size() != 1) {
      return false;
    }
    const Expression& expr = q.SelectList()[0].expression;
    if (!expr || expr->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = expr->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
        agg.HasWhereFilter() || !agg.Child() ||
        agg.Child()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    if (col_name_out != nullptr) {
      *col_name_out = agg.Child()->AsColumnValue().GetColumnName().name;
    }
    return true;
  };
  const auto is_pure_union_all_stmt = [](const SelectStatement& q) -> bool {
    if (q.UnionDistinct() || q.UnionByName() || !q.WithQueries().empty() ||
        q.HasLimit() || q.Offset() != 0 || !q.OrderBy().empty()) {
      return false;
    }
    if (const auto& tree = q.GetSetOperationTree(); tree != nullptr) {
      if (!tree->first || tree->branches.empty()) {
        return false;
      }
      for (SetOperationKind k : tree->kinds) {
        if (k != SetOperationKind::kUnionAll) {
          return false;
        }
      }
      return true;
    }
    if (q.UnionAll().empty()) {
      return false;
    }
    for (SetOperationKind k : q.SetOperationKinds()) {
      if (k != SetOperationKind::kUnionAll) {
        return false;
      }
    }
    return true;
  };
  const auto is_simple_leaf_sum = [](const SelectStatement& q) -> bool {
    if (q.Sources().empty() || !q.WithQueries().empty() ||
        !q.UnionAll().empty() || q.GetSetOperationTree() != nullptr ||
        !q.GroupBy().empty() || q.Having() || q.Qualify() || q.Distinct() ||
        q.HasLimit() || q.Offset() != 0 || !q.OrderBy().empty() ||
        q.SelectList().size() != 1) {
      return false;
    }
    for (const SelectSource& s : q.Sources()) {
      if (s.query != nullptr || s.table.empty() || s.is_lateral || s.unnest ||
          s.sample.has_value()) {
        return false;
      }
    }
    const Expression& expr = q.SelectList()[0].expression;
    if (!expr || expr->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = expr->AsAggregateExpression();
    return agg.GetType() == AggregationType::kSum && !agg.Distinct() &&
           !agg.HasWhereFilter() && agg.Child() != nullptr &&
           !ContainsQueryExpression(agg.Child());
  };

  std::string outer_sum_col;
  if (!is_simple_sum_of_col(*select, &outer_sum_col)) {
    return false;
  }
  auto& top_sources = const_cast<std::vector<SelectSource>&>(select->Sources());
  SelectSource& top_src = top_sources[0];
  if (top_src.query == nullptr || top_src.is_lateral || top_src.unnest ||
      top_src.sample.has_value()) {
    return false;
  }

  std::vector<std::shared_ptr<SelectStatement>> leaves;
  bool flattened_any = false;
  auto collect_leaves = [&](auto&& self, const SelectStatement& node,
                            int depth) -> bool {
    if (depth > 8) {
      return false;
    }
    if (is_pure_union_all_stmt(node)) {
      if (const auto& tree = node.GetSetOperationTree(); tree != nullptr) {
        if (!tree->first || !self(self, *tree->first, depth + 1)) {
          return false;
        }
        for (const auto& br : tree->branches) {
          if (!br || !self(self, *br, depth + 1)) {
            return false;
          }
        }
        return true;
      }
      // Split head (copy without UnionAll) and tail branches.
      auto head = std::make_shared<SelectStatement>(node);
      head->ClearUnionAll();
      if (!self(self, *head, depth + 1)) {
        return false;
      }
      for (const auto& br : node.UnionAll()) {
        if (!br || !self(self, *br, depth + 1)) {
          return false;
        }
      }
      return true;
    }
    // Nested `SELECT SUM(col) FROM (... UNION ALL ...) alias`
    std::string inner_col;
    if (node.Sources().size() == 1 && node.Sources()[0].query != nullptr &&
        !node.Sources()[0].is_lateral && !node.Sources()[0].unnest &&
        !node.Sources()[0].sample.has_value() && node.WithQueries().empty() &&
        node.UnionAll().empty() && node.GetSetOperationTree() == nullptr &&
        !node.WhereClause() && node.GroupBy().empty() && !node.Having() &&
        !node.Qualify() && !node.Distinct() && !node.HasLimit() &&
        node.Offset() == 0 && node.OrderBy().empty() &&
        is_simple_sum_of_col(node, &inner_col) &&
        is_pure_union_all_stmt(*node.Sources()[0].query)) {
      flattened_any = true;
      return self(self, *node.Sources()[0].query, depth + 1);
    }
    if (is_simple_leaf_sum(node)) {
      leaves.push_back(std::make_shared<SelectStatement>(node));
      return true;
    }
    return false;
  };

  if (!collect_leaves(collect_leaves, *top_src.query, 0) || leaves.size() < 2) {
    return false;
  }

  // Deduplicate structurally identical leaf branches and count multiplicity.
  std::unordered_map<std::string, size_t> fp_to_idx;
  std::vector<std::shared_ptr<SelectStatement>> unique_leaves;
  std::vector<int64_t> multiplicities;
  unique_leaves.reserve(leaves.size());
  multiplicities.reserve(leaves.size());
  for (auto& leaf : leaves) {
    // Normalize output alias to `outer_sum_col` before fingerprinting so
    // branches differing only in alias still fold.
    std::vector<NamedExpression> norm_sel = leaf->SelectList();
    norm_sel[0].name = outer_sum_col;
    leaf->SetSelectList(std::move(norm_sel));
    std::string fp = CanonicalGroupedDerivedFingerprint(*leaf);
    auto [it, inserted] =
        fp_to_idx.emplace(std::move(fp), unique_leaves.size());
    if (inserted) {
      unique_leaves.push_back(std::move(leaf));
      multiplicities.push_back(1);
    } else {
      ++multiplicities[it->second];
    }
  }

  if (!flattened_any && unique_leaves.size() == leaves.size()) {
    return false;
  }

  for (size_t i = 0; i < unique_leaves.size(); ++i) {
    if (multiplicities[i] > 1) {
      std::vector<NamedExpression> sel = unique_leaves[i]->SelectList();
      sel[0].expression =
          BinaryExpressionExp(sel[0].expression, BinaryOperation::kMultiply,
                              ConstantValueExp(Value(multiplicities[i])));
      unique_leaves[i]->SetSelectList(std::move(sel));
    }
  }

  auto folded = std::make_shared<SelectStatement>(*unique_leaves[0]);
  folded->ClearUnionAll();
  for (size_t i = 1; i < unique_leaves.size(); ++i) {
    folded->AddUnionAll(std::move(unique_leaves[i]));
  }
  top_src.query = std::move(folded);
  return true;
}

}  // namespace tinylamb
