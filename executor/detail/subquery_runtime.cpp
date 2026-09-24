/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/subquery_runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <ratio>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

// EXISTS observes only whether rows remain after the query's relational
// operators. Ordering those rows cannot change that boolean, including when
// OFFSET is present; the offset depends on cardinality, not row identity.
// Preserve LIMIT 0, and cap an unbounded/LIMIT>1 query at one row.
std::shared_ptr<SelectStatement> PrepareExistsExecution(
    const SelectStatement& statement, bool* capped) {
  const bool needs_cap = !statement.HasLimit() || statement.Limit() > 1;
  if (!needs_cap && statement.OrderBy().empty()) {
    *capped = false;
    return nullptr;
  }
  auto prepared = std::make_shared<SelectStatement>(statement);
  prepared->SetOrderBy({});
  if (needs_cap) {
    prepared->SetLimit(1);
  }
  *capped = needs_cap;
  return prepared;
}

bool AliasEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

const Expression* FindSelectAlias(
    const std::vector<NamedExpression>& select_list, std::string_view name) {
  for (const NamedExpression& item : select_list) {
    if (!item.name.empty() && AliasEquals(item.name, name)) {
      return &item.expression;
    }
  }
  return nullptr;
}

// Rebuilds `expression` replacing bare column references that resolve to a
// select-list alias (and not to an input column).  Aggregate subtrees are
// kept intact: their children always refer to base columns.
Expression ResolveAliasInTree(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema,
    const std::vector<NamedExpression>& select_list) {
  if (!expression) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (!name.schema.empty() || name.name == "*") {
        return expression;
      }
      if (LocalColumnOffset(schema, name)) {
        return expression;
      }
      if (const Expression* aliased = FindSelectAlias(select_list, name.name)) {
        return *aliased;
      }
      return expression;
    }
    case TypeTag::kBinaryExp:
      return BinaryExpressionExp(
          ResolveAliasInTree(expression->AsBinaryExpression().Left(), schema,
                             select_list),
          expression->AsBinaryExpression().Op(),
          ResolveAliasInTree(expression->AsBinaryExpression().Right(), schema,
                             select_list));
    case TypeTag::kUnaryExp:
      return UnaryExpressionExp(
          ResolveAliasInTree(expression->AsUnaryExpression().Child(), schema,
                             select_list),
          expression->AsUnaryExpression().Op());
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(value.when_clauses_.size());
      for (const auto& [condition, result] : value.when_clauses_) {
        clauses.emplace_back(ResolveAliasInTree(condition, schema, select_list),
                             ResolveAliasInTree(result, schema, select_list));
      }
      return CaseExpressionExp(
          std::move(clauses),
          ResolveAliasInTree(value.else_clause_, schema, select_list));
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      std::vector<Expression> items;
      items.reserve(value.list_.size());
      for (const Expression& item : value.list_) {
        items.push_back(ResolveAliasInTree(item, schema, select_list));
      }
      return InExpressionExp(
          ResolveAliasInTree(value.child_, schema, select_list),
          std::move(items));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(
          expression->AsFunctionCallExpression().FuncName(), [&] {
            std::vector<Expression> args;
            args.reserve(expression->AsFunctionCallExpression().Args().size());
            for (const Expression& argument :
                 expression->AsFunctionCallExpression().Args()) {
              args.push_back(ResolveAliasInTree(argument, schema, select_list));
            }
            return args;
          }());
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(ResolveAliasInTree(element, schema, select_list));
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    default:
      // Aggregates, casts of aggregates, subqueries and constants stay as-is.
      return expression;
  }
}

}  // namespace

std::shared_ptr<SelectStatement> ResolveGroupingAliases(
    const SelectStatement& statement, const Schema& input_schema) {
  const std::vector<NamedExpression>& select_list = statement.SelectList();
  if (select_list.empty()) {
    return nullptr;
  }
  bool changed = false;
  std::vector<Expression> group_by;
  group_by.reserve(statement.GroupBy().size());
  for (const Expression& key : statement.GroupBy()) {
    Expression resolved = key;
    if (key && key->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = key->AsColumnValue().GetColumnName();
      // GoogleSQL grouping items may be a select-list alias or an ordinal
      // position; base columns take precedence over both.
      const bool unqualified = name.schema.empty() && name.name != "*";
      if (unqualified && !LocalColumnOffset(input_schema, name)) {
        if (const Expression* aliased =
                FindSelectAlias(select_list, name.name)) {
          resolved = *aliased;
          changed = true;
        } else if (name.name.find_first_not_of("0123456789") ==
                   std::string::npos) {
          try {
            const size_t ordinal = std::stoul(name.name);
            if (ordinal >= 1 && ordinal <= select_list.size()) {
              resolved = select_list[ordinal - 1].expression;
              changed = true;
            }
          } catch (const std::exception&) {
            // An out-of-range ordinal must keep its key: dropping it would
            // silently regroup the whole relation into one group.
            group_by.push_back(std::move(resolved));
            continue;
          }
        }
      }
    } else if (key) {
      resolved = ResolveAliasInTree(key, input_schema, select_list);
      if (!(resolved == key)) {
        changed = true;
      }
    }
    group_by.push_back(std::move(resolved));
  }
  Expression having =
      statement.Having()
          ? ResolveAliasInTree(statement.Having(), input_schema, select_list)
          : statement.Having();
  if (statement.Having() && !(having == statement.Having())) {
    changed = true;
  }
  if (!changed) {
    return nullptr;
  }
  auto adjusted = std::make_shared<SelectStatement>(statement);
  adjusted->SetGroupBy(std::move(group_by));
  if (statement.Having()) {
    adjusted->SetHaving(std::move(having));
  }
  return adjusted;
}

Value ProjectSubqueryRow(const Row& row, bool as_struct, const Schema* schema) {
  if (row.values_.empty()) {
    return {};
  }
  if (!as_struct) {
    return row[0];
  }
  if (row.values_.size() == 1) {
    const Value& only = row.values_[0];
    // A fully-NULL single-field struct is a scalar NULL.
    if (only.IsNull()) {
      return {};
    }
  }
  auto field_key = [&](size_t index) -> std::string {
    if (schema != nullptr && index < schema->ColumnCount()) {
      const std::string& name = schema->GetColumn(index).Name().name;
      if (!name.empty() && !name.starts_with("$expr")) {
        return name;
      }
    }
    return "f" + std::to_string(index + 1);
  };
  auto scalar_to_json = [](const Value& v) -> std::string {
    switch (v.type) {
      case ValueType::kNull:
        return "null";
      case ValueType::kInt64:
        return std::to_string(v.value.int_value);
      case ValueType::kDouble:
        return FormatDoubleShortest(v.value.double_value);
      case ValueType::kVarChar:
        return "\"" + std::string(v.value.varchar_value) + "\"";
      case ValueType::kArray: {
        std::string inner;
        const std::vector<Value>& elements = v.ArrayElements();
        for (size_t i = 0; i < elements.size(); ++i) {
          if (i > 0) {
            inner += ",";
          }
          const Value& element = elements[i];
          switch (element.type) {
            case ValueType::kNull:
              inner += "null";
              break;
            case ValueType::kInt64:
              inner += std::to_string(element.value.int_value);
              break;
            case ValueType::kDouble:
              inner += FormatDoubleShortest(element.value.double_value);
              break;
            case ValueType::kVarChar:
              inner += "\"" + std::string(element.value.varchar_value) + "\"";
              break;
            default:
              inner += element.AsString();
          }
        }
        return "[" + inner + "]";
      }
      default:
        return v.AsString();
    }
  };
  std::string json = "{";
  for (size_t i = 0; i < row.values_.size(); ++i) {
    if (i > 0) {
      json += ",";
    }
    json += "\"" + field_key(i) + "\":" + scalar_to_json(row.values_[i]);
  }
  json += "}";
  return Value(std::move(json));
}

double ElapsedMs(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

namespace {

void CountStatementTablesImpl(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
    std::unordered_map<std::string, size_t>* counts,
    const std::unordered_set<std::string>& inherited_ctes);

void CountExpressionTables(  // NOLINT(misc-no-recursion)
    const Expression& expression,
    std::unordered_map<std::string, size_t>* counts,
    const std::unordered_set<std::string>& visible_ctes) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    CountStatementTablesImpl(*query.Query(), counts, visible_ctes);
    CountExpressionTables(query.Test(), counts, visible_ctes);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountExpressionTables(child, counts, visible_ctes);
  }
}

void CountStatementTablesImpl(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
    std::unordered_map<std::string, size_t>* counts,
    const std::unordered_set<std::string>& inherited_ctes) {
  std::unordered_set<std::string> visible_ctes = inherited_ctes;
  for (const auto& [name, query] : statement.WithQueries()) {
    (void)query;
    visible_ctes.insert(name);
  }
  for (const auto& [name, query] : statement.WithQueries()) {
    (void)name;
    CountStatementTablesImpl(*query, counts, visible_ctes);
  }
  for (const SelectSource& source : statement.Sources()) {
    if (source.query) {
      CountStatementTablesImpl(*source.query, counts, visible_ctes);
    } else if (source.unnest) {
      // unnest has no base table
    } else if (!source.table.empty() && !visible_ctes.contains(source.table)) {
      ++(*counts)[source.table];
    }

    CountExpressionTables(source.join_condition, counts, visible_ctes);
  }
  CountExpressionTables(statement.WhereClause(), counts, visible_ctes);
  for (const NamedExpression& item : statement.SelectList()) {
    CountExpressionTables(item.expression, counts, visible_ctes);
  }
  for (const Expression& expression : statement.GroupBy()) {
    CountExpressionTables(expression, counts, visible_ctes);
  }
  CountExpressionTables(statement.Having(), counts, visible_ctes);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CountExpressionTables(term.expression, counts, visible_ctes);
  }
}
}  // namespace

void CountStatementTables(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
    std::unordered_map<std::string, size_t>* counts) {
  CountStatementTablesImpl(statement, counts, {});
}
bool ContainsOnlyUncorrelatedQueries(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const Expression& expression,
    const CteMap& ctes) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    return StatementUsesOnlyScopes(context, *query.Query(), {}, ctes) &&
           ContainsOnlyUncorrelatedQueries(context, query.Test(), ctes);
  }
  return std::ranges::all_of(
      ExpressionChildren(expression),
      [&](const Expression& child) {  // NOLINT(misc-no-recursion)
        return ContainsOnlyUncorrelatedQueries(context, child, ctes);
      });
}
void EnsureReusableProjections(TransactionContext& context,
                               ExecutionRuntime* runtime) {
  if (runtime == nullptr || runtime->root_statement == nullptr ||
      runtime->reusable_base_relations.empty() ||
      !runtime->reusable_projections.empty()) {
    return;
  }
  for (const std::string& table : runtime->reusable_base_relations) {
    StatusOr<std::shared_ptr<Table>> loaded = context.GetTable(table);
    if (!loaded.HasValue()) {
      continue;
    }
    runtime->reusable_projections[table] =
        RequiredColumns(*runtime->root_statement, loaded.Value()->GetSchema());
  }
}

const std::vector<slot_t>* ReusableProjection(TransactionContext& context,
                                              std::string_view table) {
  if (context.execution_runtime() == nullptr) {
    return nullptr;
  }
  const auto found = context.execution_runtime()->reusable_projections.find(
      std::string(table));
  if (found == context.execution_runtime()->reusable_projections.end() ||
      found->second.empty()) {
    return nullptr;
  }
  return &found->second;
}
// True when the cache entry keyed by `statement`'s address was created for
// this very statement (address recycling guard). Records `fresh` when absent.
bool CacheEntryIsCurrent(const SelectStatement& statement,
                         ExecutionRuntime& runtime) {
  const std::string& fresh = statement.Fingerprint();
  const auto [it, inserted] =
      runtime.cache_fingerprints.try_emplace(&statement, fresh);
  if (!inserted && it->second != fresh) {
    it->second = fresh;
    return false;
  }
  return true;
}

void DropCacheEntry(ExecutionRuntime& runtime, const SelectStatement* key) {
  runtime.cache_fingerprints.erase(key);
}

std::optional<Relation> ExecuteCorrelatedSingleSource(
    TransactionContext& context, const SelectStatement& statement,
    const Scope& outer, const CteMap& ctes, bool exists_only) {
  if (context.execution_runtime() == nullptr || statement.Sources().empty() ||
      std::ranges::any_of(
          statement.Sources(),
          [](const SelectSource& source) { return source.query != nullptr; }) ||
      !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Qualify() ||
      HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement) ||
      context.execution_runtime()->unindexable_queries.contains(&statement)) {
    return std::nullopt;
  }
  {
    // CTE sources (inherited or declared by this statement's own WITH clause)
    // have no base table; the correlated-index fast path cannot load them.
    const SelectSource& from = statement.Sources()[0];
    if ((!from.table.empty() &&
         (ctes.contains(from.table) ||
          statement.WithQueries().contains(from.table)))) {
      return std::nullopt;
    }
  }

  CorrelatedIndex* index = nullptr;
  const auto cached =
      context.execution_runtime()->correlated_indexes.find(&statement);
  if (cached != context.execution_runtime()->correlated_indexes.end()) {
    if (CacheEntryIsCurrent(statement, *context.execution_runtime())) {
      index = cached->second.get();
    } else {
      // Address recycled: the entry belongs to a dead statement.
      DropCacheEntry(*context.execution_runtime(), &statement);
      context.execution_runtime()->correlated_indexes.erase(cached);
      context.execution_runtime()->unindexable_queries.erase(&statement);
    }
  }
  if (index == nullptr &&
      !context.execution_runtime()->unindexable_queries.contains(&statement)) {
    const SelectSource& from = statement.Sources()[0];
    Schema peek_schema;
    if (statement.Sources().size() == 1 && !from.query && !from.table.empty()) {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(from.table);
      if (table.HasValue()) {
        peek_schema = table.Value()->GetSchema();
        // Alias-qualified view so qualified references resolve; order is
        // identical to the raw table schema.
        const std::string peek_qualifier =
            from.alias.empty() ? from.table : from.alias;
        if (!peek_qualifier.empty()) {
          peek_schema = QualifySchema(peek_schema, peek_qualifier);
        }
      }
    }
    auto has_correlated_equality = [&](const Schema& schema) {
      if (schema.ColumnCount() == 0) {
        return false;
      }
      return std::ranges::any_of(
          SplitConjuncts(statement.WhereClause()),
          [&](const Expression& predicate) {
            if (predicate->Type() != TypeTag::kBinaryExp) {
              return false;
            }
            const BinaryExpression& binary = predicate->AsBinaryExpression();
            if (binary.Op() != BinaryOperation::kEquals ||
                binary.Left()->Type() != TypeTag::kColumnValue ||
                binary.Right()->Type() != TypeTag::kColumnValue) {
              return false;
            }
            const ColumnName& left =
                binary.Left()->AsColumnValue().GetColumnName();
            const ColumnName& right =
                binary.Right()->AsColumnValue().GetColumnName();
            const auto left_local = LocalColumnOffset(schema, left);
            const auto right_local = LocalColumnOffset(schema, right);
            return (left_local && !right_local) || (right_local && !left_local);
          });
    };
    if (statement.Sources().size() == 1 && peek_schema.ColumnCount() > 0 &&
        !has_correlated_equality(peek_schema)) {
      context.execution_runtime()->unindexable_queries.insert(&statement);
      return std::nullopt;
    }
    Relation source;
    if (statement.Sources().size() == 1) {
      std::vector<slot_t> projection;
      if (peek_schema.ColumnCount() > 0) {
        if (const std::vector<slot_t>* shared =
                ReusableProjection(context, from.table)) {
          projection = *shared;
        } else {
          // peek_schema already carries the alias-qualified names.
          projection = RequiredColumns(statement, peek_schema, true);
        }
        if (projection.empty()) {
          projection.push_back(0);
        }
      }
      // NOTE: table_key_filters are intentionally NOT applied here. The stash
      // is owned by the statement that derived it; applying it to correlated
      // subqueries silently narrowed unrelated scans.
      StatusOr<Relation> loaded =
          LoadSource(context, from, &outer, ctes,
                     projection.empty() ? nullptr : &projection, nullptr,
                     nullptr, std::nullopt);
      if (!loaded.HasValue()) {
        return std::nullopt;
      }
      source = loaded.MoveValue();
    } else {
      bool predicates_applied = false;
      StatusOr<Relation> built =
          BuildInput(context, statement, &outer, ctes, &predicates_applied);
      if (!built.HasValue()) {
        return std::nullopt;
      }
      source = built.MoveValue();
    }
    auto created = std::make_unique<CorrelatedIndex>();
    created->schema = source.schema;
    std::vector<Expression> indexed_equalities;
    for (const Expression& predicate :
         SplitConjuncts(statement.WhereClause())) {
      if (predicate->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const BinaryExpression& binary = predicate->AsBinaryExpression();
      if (binary.Op() != BinaryOperation::kEquals ||
          binary.Left()->Type() != TypeTag::kColumnValue ||
          binary.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& left = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& right = binary.Right()->AsColumnValue().GetColumnName();
      const auto left_local = LocalColumnOffset(source.schema, left);
      const auto right_local = LocalColumnOffset(source.schema, right);
      if (left_local && !right_local) {
        created->local_columns.push_back(static_cast<slot_t>(*left_local));
        created->outer_columns.push_back(right);
        indexed_equalities.push_back(predicate);
      } else if (right_local && !left_local) {
        created->local_columns.push_back(static_cast<slot_t>(*right_local));
        created->outer_columns.push_back(left);
        indexed_equalities.push_back(predicate);
      }
    }
    if (created->local_columns.empty()) {
      context.execution_runtime()->unindexable_queries.insert(&statement);
      return std::nullopt;
    }
    std::vector<Expression> correlated_expressions =
        SplitConjuncts(statement.WhereClause());
    for (const NamedExpression& item : statement.SelectList()) {
      correlated_expressions.push_back(item.expression);
    }
    correlated_expressions.insert(correlated_expressions.end(),
                                  statement.GroupBy().begin(),
                                  statement.GroupBy().end());
    if (statement.Having()) {
      correlated_expressions.push_back(statement.Having());
    }
    for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
      correlated_expressions.push_back(term.expression);
    }
    for (const Expression& expression : correlated_expressions) {
      for (const ColumnName& column : expression->TouchedColumns()) {
        if (column.name == "*") {
          continue;
        }
        if (LocalColumnOffset(source.schema, column)) {
          continue;
        }
        if (std::ranges::find(created->cache_outer_columns, column) ==
            created->cache_outer_columns.end()) {
          created->cache_outer_columns.push_back(column);
        }
      }
    }

    const bool aggregate_only =
        statement.GroupBy().empty() && !statement.SelectList().empty() &&
        std::all_of(statement.SelectList().begin(),
                    statement.SelectList().end(),
                    [](const NamedExpression& item) {
                      return ContainsAggregate(item.expression);
                    });
    std::vector<Expression> local_predicates;
    for (const Expression& predicate :
         SplitConjuncts(statement.WhereClause())) {
      const bool indexed = std::ranges::find(indexed_equalities, predicate) !=
                           indexed_equalities.end();
      if (indexed) {
        continue;
      }
      local_predicates.push_back(predicate);
    }

    // Expressions evaluated while building the index below run without the
    // outer scope, so anything referencing outer columns cannot be handled
    // here. Treat such statements as unindexable and let the generic
    // correlated path (which evaluates WHERE with the outer scope) take over.
    {
      bool references_outer = false;
      auto check = [&](const Expression& expression) {
        if (!expression) {
          return;
        }
        for (const ColumnName& column : expression->TouchedColumns()) {
          if (column.name == "*") {
            continue;
          }
          if (!LocalColumnOffset(source.schema, column)) {
            references_outer = true;
          }
        }
      };
      for (const Expression& predicate : local_predicates) {
        check(predicate);
      }
      if (aggregate_only) {
        for (const NamedExpression& item : statement.SelectList()) {
          check(item.expression);
        }
        check(statement.Having());
      }
      if (references_outer) {
        context.execution_runtime()->unindexable_queries.insert(&statement);
        return std::nullopt;
      }
    }

    source.FinishSpill();
    Status group_error{Status::kSuccess};
    const bool integer_key =
        SingleIntegerJoinKey(source.schema, created->local_columns);
    const auto integer_key_value = [&](int64_t key) {
      const Column& column =
          source.schema.GetColumn(created->local_columns.front());
      Value value = column.Type() == ValueType::kDate ? Value::DateFromDays(key)
                                                      : Value(key);
      if (column.IsUnsigned()) {
        value = value.WithUnsigned();
      }
      return value;
    };
    if (aggregate_only) {
      // One-pass hash aggregate into finished scalar results. Storing only
      // aggregates (not every lineitem row) keeps Q17-style subqueries small.
      created->preaggregated = true;
      std::vector<const AggregateExpression*> aggregate_expressions;
      std::unordered_set<const AggregateExpression*> seen_aggregates;
      for (const NamedExpression& item : statement.SelectList()) {
        CollectAggregates(item.expression, &aggregate_expressions,
                          &seen_aggregates);
      }
      // HAVING may reference aggregates that the select list does not emit
      // (e.g. `SELECT COUNT(*) ... HAVING SUM(x) > 0`); each one needs its
      // own accumulator so the group filter below can evaluate it.
      CollectAggregates(statement.Having(), &aggregate_expressions,
                        &seen_aggregates);
      struct GroupAggs {
        std::vector<AggregateAccumulator> accumulators;
      };
      std::unordered_map<int64_t, GroupAggs> int_groups;
      std::unordered_map<std::string, GroupAggs> str_groups;
      const CompiledScanFilter local_filter =
          CompileScanFilter(local_predicates, source.schema);
      // Child column fast path: an aggregate whose input is a plain column
      // (the Q17 avg(l_quantity) shape) skips per-row AST evaluation, which
      // would otherwise resolve the column by name for every build row.
      std::vector<std::optional<slot_t>> aggregate_child_columns(
          aggregate_expressions.size(), std::nullopt);
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateExpression& aggregate = *aggregate_expressions[i];
        if (!IsCountStar(aggregate) && aggregate.Child() &&
            aggregate.Child()->Type() == TypeTag::kColumnValue) {
          aggregate_child_columns[i] = LocalColumnOffset(
              source.schema,
              aggregate.Child()->AsColumnValue().GetColumnName());
        }
      }
      // Worker-safe aggregation check: plain-column inputs (or count(*))
      // for streaming SUM/AVG/COUNT/MIN/MAX let the build run as a parallel
      // row-slice reduction; anything else (per-row filters, DISTINCT,
      // buffered aggregates) falls back to the serial pass.
      bool build_parallel =
          integer_key && !source.HasSpill() && source.TotalRows() >= 65536;
      std::vector<bool> worker_safe(aggregate_expressions.size(), false);
      for (size_t i = 0; build_parallel && i < aggregate_expressions.size();
           ++i) {
        const AggregateExpression& aggregate = *aggregate_expressions[i];
        const bool safe =
            (IsCountStar(aggregate) ||
             (aggregate_child_columns[i].has_value() &&
              !aggregate.WhereFilter() &&
              aggregate.Having() == AggregateHavingModifier::kNone &&
              aggregate.InnerOrderBy().empty() && !aggregate.SecondaryArg() &&
              !aggregate.Distinct() && aggregate.TrailingArgs().empty())) &&
            (IsCountStar(aggregate) ||
             LocalColumnOffset(
                 source.schema,
                 aggregate.Child()->AsColumnValue().GetColumnName())
                 .has_value());
        worker_safe[i] = safe;
        build_parallel = build_parallel && safe;
      }
      struct BuildWorker {
        std::unordered_map<int64_t, GroupAggs> int_groups;
        std::vector<Row> deferred;
      };
      auto accumulate_one = [&](const Row& row) -> void {
        if (HasNullKey(row, created->local_columns)) {
          return;
        }
        if (!MatchScanFilter(row, source.schema, local_filter, nullptr, context,
                             ctes, &group_error)) {
          return;
        }
        GroupAggs* group = nullptr;
        std::string str_key;
        int64_t int_key = 0;
        if (integer_key) {
          int_key = IntegerJoinKey(row, created->local_columns[0]);
          group = &int_groups[int_key];
        } else {
          str_key = EncodeJoinKey(row, created->local_columns);
          group = &str_groups[str_key];
        }
        if (group->accumulators.empty()) {
          group->accumulators.reserve(aggregate_expressions.size());
          for (const AggregateExpression* aggregate : aggregate_expressions) {
            group->accumulators.emplace_back(aggregate, &source.schema);
          }
        }
        Scope scope{.row = &row, .schema = &source.schema, .outer = nullptr};
        auto eval_group_value =
            [&](const Expression& expr) -> std::optional<Value> {
          if (group_error != Status::kSuccess) {
            return std::nullopt;
          }
          StatusOr<Value> v = TryEvaluate(expr, scope, nullptr, context, ctes);
          if (!v.HasValue()) {
            group_error = v.GetStatus();
            return std::nullopt;
          }
          return v.MoveValue();
        };
        for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
          const AggregateExpression& aggregate = *aggregate_expressions[i];
          if (aggregate.WhereFilter()) {
            std::optional<Value> keep =
                eval_group_value(aggregate.WhereFilter());
            if (!keep.has_value()) {
              return;
            }
            if (!Truthy(*keep)) {
              continue;
            }
          }
          if (IsCountStar(aggregate)) {
            if (Status st = group->accumulators[i].Add(Value(1));
                st != Status::kSuccess) {
              group_error = st;
              return;
            }
            continue;
          }
          AggregateInput input;
          std::optional<Value> value;
          if (aggregate_child_columns[i].has_value()) {
            value = row[*aggregate_child_columns[i]];
          } else {
            value = eval_group_value(aggregate.Child());
          }
          if (!value.has_value()) {
            return;
          }
          input.value = std::move(*value);
          if (aggregate.Having() != AggregateHavingModifier::kNone &&
              aggregate.HavingCondition()) {
            std::optional<Value> cond =
                eval_group_value(aggregate.HavingCondition());
            if (!cond.has_value()) {
              return;
            }
            input.condition = std::move(*cond);
          }
          for (const auto& term : aggregate.InnerOrderBy()) {
            std::optional<Value> key = eval_group_value(term.expression);
            if (!key.has_value()) {
              return;
            }
            input.order_keys.push_back(std::move(*key));
          }
          if (aggregate.GetType() == AggregationType::kStringAgg &&
              aggregate.SecondaryArg()) {
            std::optional<Value> aux =
                eval_group_value(aggregate.SecondaryArg());
            if (!aux.has_value()) {
              return;
            }
            input.auxiliary = std::move(*aux);
          }
          for (const Expression& extra : aggregate.TrailingArgs()) {
            if (extra) {
              std::optional<Value> trail = eval_group_value(extra);
              if (!trail.has_value()) {
                return;
              }
              input.trailing_values.push_back(std::move(*trail));
            }
          }
          if (Status st = group->accumulators[i].Add(std::move(input));
              st != Status::kSuccess) {
            group_error = st;
            return;
          }
        }
      };
      if (build_parallel) {
        // Parallel reduction: workers slice the materialized rows, keep
        // per-key accumulator maps, and the main thread merges the scalar
        // streaming states (AggregateAccumulator::Merge).  Worker filters
        // use the worker-safe subset (simple compares + compiled residual
        // bytecode); rows they cannot decide are deferred to the serial
        // pass below.
        const auto build_rows = source.TotalRows();
        const auto workers =
            std::min<size_t>({std::thread::hardware_concurrency(), 16UL,
                              std::max<size_t>(1, build_rows / 32768)});
        if (workers > 1) {
          std::vector<BuildWorker> build_workers(workers);
          std::vector<Status> build_errors(workers, Status::kSuccess);
          std::atomic<size_t> next_chunk{0};
          const size_t chunk_rows = 8192;
          const size_t chunk_count = (build_rows + chunk_rows - 1) / chunk_rows;
          std::mutex build_error_mu;
          std::exception_ptr build_error_ex;
          const std::vector<Row>& build_vector = source.rows;
          const CompiledScanFilter& shared_filter = local_filter;
          {
            std::vector<std::jthread> threads;
            threads.reserve(workers);
            for (size_t w = 0; w < workers; ++w) {
              try {
                threads.emplace_back([&, w] {
                  BuildWorker& local = build_workers[w];
                  try {
                    while (true) {
                      const size_t ci = next_chunk.fetch_add(1);
                      if (ci >= chunk_count) {
                        break;
                      }
                      const size_t begin = ci * chunk_rows;
                      const size_t end =
                          std::min(build_rows, begin + chunk_rows);
                      for (size_t ri = begin; ri < end; ++ri) {
                        const Row& row = build_vector[ri];
                        if (HasNullKey(row, created->local_columns)) {
                          continue;
                        }
                        bool matches = true;
                        for (const SimpleComparePredicate& pred :
                             shared_filter.simple) {
                          if (!MatchSimpleCompare(row, pred)) {
                            matches = false;
                            break;
                          }
                        }
                        bool decided = matches;
                        if (matches && shared_filter.residual_bytecode) {
                          StatusOr<Value> result =
                              shared_filter.residual_bytecode->TryEvaluateRow(
                                  row);
                          if (result.HasValue()) {
                            matches = !result.Value().IsNull() &&
                                      result.Value().Truthy();
                          } else {
                            decided = false;
                          }
                        } else if (matches && !shared_filter.residual.empty()) {
                          decided = false;
                        }
                        if (matches && !decided) {
                          local.deferred.push_back(row);
                        } else if (matches) {
                          int64_t key =
                              IntegerJoinKey(row, created->local_columns[0]);
                          auto [iter, inserted] =
                              local.int_groups.try_emplace(key);
                          if (inserted) {
                            iter->second.accumulators.reserve(
                                aggregate_expressions.size());
                            for (const AggregateExpression* aggregate :
                                 aggregate_expressions) {
                              iter->second.accumulators.emplace_back(
                                  aggregate, &source.schema);
                            }
                          }
                          for (size_t i = 0; i < aggregate_expressions.size();
                               ++i) {
                            if (!worker_safe[i]) {
                              continue;
                            }
                            if (IsCountStar(*aggregate_expressions[i])) {
                              if (Status st = iter->second.accumulators[i].Add(
                                      Value(1));
                                  st != Status::kSuccess) {
                                build_errors[w] = st;
                                return;
                              }
                              continue;
                            }
                            if (Status st = iter->second.accumulators[i].Add(
                                    row[*aggregate_child_columns[i]]);
                                st != Status::kSuccess) {
                              build_errors[w] = st;
                              return;
                            }
                          }
                        }
                      }
                    }
                  } catch (...) {
                    std::scoped_lock lock(build_error_mu);
                    if (!build_error_ex) {
                      build_error_ex = std::current_exception();
                    }
                  }
                });
              } catch (...) {
                std::scoped_lock lock(build_error_mu);
                if (!build_error_ex) {
                  build_error_ex = std::current_exception();
                }
                break;
              }
            }
          }
          if (build_error_ex) {
            std::rethrow_exception(build_error_ex);
          }
          for (const Status& st : build_errors) {
            if (st != Status::kSuccess) {
              return std::nullopt;
            }
          }
          for (BuildWorker& local : build_workers) {
            for (auto& [key, group] : local.int_groups) {
              GroupAggs& dst = int_groups[key];
              if (dst.accumulators.empty()) {
                dst.accumulators = std::move(group.accumulators);
                continue;
              }
              for (size_t i = 0; i < dst.accumulators.size(); ++i) {
                if (group.accumulators[i].count == 0) {
                  continue;
                }
                if (Status st =
                        dst.accumulators[i].Merge(group.accumulators[i]);
                    st != Status::kSuccess) {
                  return std::nullopt;
                }
              }
            }
          }
          for (BuildWorker& local : build_workers) {
            for (const Row& row : local.deferred) {
              accumulate_one(row);
              if (group_error != Status::kSuccess) {
                break;
              }
            }
          }
          if (group_error != Status::kSuccess) {
            return std::nullopt;
          }
          build_parallel = true;
        } else {
          build_parallel = false;
        }
      }
      if (!build_parallel) {
        source.ForEachRow(accumulate_one);
      }
      if (group_error != Status::kSuccess) {
        return std::nullopt;  // Fall back to the generic (diagnostic) path.
      }
      auto emit_group = [&](const std::string& key, GroupAggs& group) {
        AggregateResultMap aggregate_results;
        aggregate_results.reserve(group.accumulators.size());
        for (const AggregateAccumulator& accumulator : group.accumulators) {
          StatusOr<Value> finished = accumulator.TryFinish();
          if (!finished.HasValue()) {
            group_error = finished.GetStatus();
            return;
          }
          aggregate_results.emplace(accumulator.expression,
                                    finished.MoveValue());
        }
        Row representative;
        Scope scope{
            .row = &representative, .schema = &source.schema, .outer = nullptr};
        // A HAVING predicate eliminates whole groups.  The probe path
        // treats a cache miss as an empty relation, so filtered-out groups
        // must simply not enter the cache; evaluating the group as if it
        // survived would return a value where SQL requires the scalar NULL
        // / zero-row semantics.
        if (statement.Having()) {
          if (group_error != Status::kSuccess) {
            return;
          }
          StatusOr<Value> keep = TryEvaluate(statement.Having(), scope,
                                             &aggregate_results, context, ctes);
          if (!keep.HasValue()) {
            group_error = keep.GetStatus();
            return;
          }
          if (!Truthy(keep.Value())) {
            return;
          }
        }
        std::vector<Value> values;
        values.reserve(statement.SelectList().size());
        for (const NamedExpression& item : statement.SelectList()) {
          if (group_error != Status::kSuccess) {
            return;
          }
          StatusOr<Value> v = TryEvaluate(item.expression, scope,
                                          &aggregate_results, context, ctes);
          if (!v.HasValue()) {
            group_error = v.GetStatus();
            return;
          }
          values.push_back(v.MoveValue());
        }
        // Build the schema from the local values directly: AddRow may spill
        // and empty `finished.rows`, so rows[0] is not safe to read back.
        std::vector<Column> columns;
        columns.reserve(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
          columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                               ValueTypeOf(values[i]));
          columns.back().SetUnsigned(values[i].IsUnsigned());
        }
        Relation finished;
        finished.schema = Schema("", std::move(columns));
        finished.AddRow(Row(std::move(values)));
        created->cached_results.emplace(
            key, std::make_shared<Relation>(std::move(finished)));
      };
      if (integer_key) {
        for (auto& [key, group] : int_groups) {
          emit_group(integer_key_value(key).EncodeMemcomparableFormat(), group);
        }
      } else {
        for (auto& [key, group] : str_groups) {
          emit_group(key, group);
        }
      }
    } else if (exists_only &&
               std::ranges::all_of(
                   created->cache_outer_columns,
                   [&](const ColumnName& column) {
                     // Outer references beyond the correlation equalities
                     // themselves would make existence depend on outer
                     // values the key set cannot see.
                     return std::ranges::find(created->outer_columns, column) !=
                            created->outer_columns.end();
                   }) &&
               statement.GroupBy().empty() && statement.Having() == nullptr &&
               !statement.Distinct() && !statement.HasLimit() &&
               statement.Offset() == 0 &&
               SingleIntegerJoinKey(source.schema, created->local_columns)) {
      // EXISTS key-set build: row existence keyed by the local correlation
      // column is the entire observable result, so the matching rows never
      // need to be retained (the generic branch below copies every one of
      // them into the multimap).
      const CompiledScanFilter local_filter =
          CompileScanFilter(local_predicates, source.schema);
      source.ForEachRow([&](const Row& row) {
        if (group_error != Status::kSuccess ||
            HasNullKey(row, created->local_columns)) {
          return;
        }
        if (!MatchScanFilter(row, source.schema, local_filter, nullptr, context,
                             ctes, &group_error)) {
          return;
        }
        created->exists_keys.insert(
            IntegerJoinKey(row, created->local_columns[0]));
      });
      if (group_error != Status::kSuccess) {
        return std::nullopt;  // Fall back to the generic (diagnostic) path.
      }
      created->exists_key_set = true;
    } else {
      source.ForEachRow([&](const Row& row) {
        if (HasNullKey(row, created->local_columns)) {
          return;
        }
        if (!local_predicates.empty()) {
          Scope scope{.row = &row, .schema = &source.schema, .outer = nullptr};
          for (const Expression& predicate : local_predicates) {
            if (group_error != Status::kSuccess) {
              return;
            }
            StatusOr<Value> keep =
                TryEvaluate(predicate, scope, nullptr, context, ctes);
            if (!keep.HasValue()) {
              group_error = keep.GetStatus();
              return;
            }
            if (!Truthy(keep.Value())) {
              return;
            }
          }
        }
        const std::string key = EncodeJoinKey(row, created->local_columns);
        created->rows.emplace(key, row);
      });
      if (group_error != Status::kSuccess) {
        return std::nullopt;  // Fall back to the generic (diagnostic) path.
      }
    }
    source.rows.clear();
    source.rows.shrink_to_fit();
    source.ReleaseCharge();
    source.spill.reset();
    source.spill_tail_.reset();
    index = created.get();
    CacheEntryIsCurrent(statement, *context.execution_runtime());
    context.execution_runtime()->correlated_indexes.emplace(&statement,
                                                            std::move(created));
    ++context.execution_runtime()->correlated_index_builds;
  }

  ++context.execution_runtime()->correlated_index_probes;

  if (index->local_columns.size() != index->outer_columns.size()) {
    return std::nullopt;
  }
  for (size_t i = 0; i < index->outer_columns.size(); ++i) {
    StatusOr<Value> indexed_outer_value =
        TryLookup(index->outer_columns[i], outer);
    if (!indexed_outer_value.HasValue()) {
      return std::nullopt;
    }
    const Value& outer_value = indexed_outer_value.Value();
    if (outer_value.IsNull()) {
      continue;
    }
    const Column& local_column =
        index->schema.GetColumn(index->local_columns[i]);
    const bool same_domain =
        (local_column.Type() == ValueType::kDate &&
         outer_value.type == ValueType::kDate) ||
        (local_column.Type() == ValueType::kInt64 &&
         outer_value.type == ValueType::kInt64 &&
         local_column.IsUnsigned() == outer_value.IsUnsigned());
    if (!same_domain) {
      return std::nullopt;
    }
  }

  std::vector<Value> cache_values;
  cache_values.reserve(index->cache_outer_columns.size());
  for (const ColumnName& column : index->cache_outer_columns) {
    StatusOr<Value> cached_value = TryLookup(column, outer);
    if (!cached_value.HasValue()) {
      return std::nullopt;  // Fall back to the generic (diagnostic) path.
    }
    cache_values.push_back(cached_value.MoveValue());
  }
  // A NULL correlated value is not a valid equality key.  In particular,
  // LEFT JOIN null-extension reaches this path before the indexed equality
  // guard below; encoding it as a memcomparable row would throw instead of
  // producing the SQL UNKNOWN/empty-result semantics.  Skip the result cache
  // for such probes and let the normal NULL guard handle them.
  const bool cacheable = std::ranges::all_of(
      cache_values, [](const Value& value) { return !value.IsNull(); });
  std::optional<std::string> cache_key;
  if (cacheable) {
    cache_key = Row(std::move(cache_values)).EncodeMemcomparableFormat();
    index->parameter_keys_seen.insert(*cache_key);
    if (const auto cached_result = index->cached_results.find(*cache_key);
        cached_result != index->cached_results.end()) {
      ++context.execution_runtime()->correlated_result_cache_hits;
      StatusOr<Relation> replayed = MaterializeRelation(*cached_result->second);
      if (!replayed.HasValue()) {
        return std::nullopt;
      }
      return replayed.MoveValue();
    }
  } else {
    // NULL equality parameters do not participate in the result cache, but
    // they are still one distinct parameter tuple for profiling purposes.
    index->parameter_keys_seen.insert("\xfftinylamb-null-parameter");
  }

  if (index->exists_key_set) {
    // Key-set probes answer directly from the build; neither the per-key
    // result cache nor FinishQuery runs.  A NULL correlation key matches
    // nothing under `=` (empty relation), mirroring the generic NULL guard.
    StatusOr<Value> outer_value = TryLookup(index->outer_columns[0], outer);
    if (!outer_value.HasValue()) {
      return std::nullopt;  // Fall back to the generic (diagnostic) path.
    }
    const Value& probe = outer_value.Value();
    if (probe.IsNull()) {
      Relation empty;
      empty.schema = index->schema;
      return empty;
    }
    if (probe.type != ValueType::kInt64 && probe.type != ValueType::kDate) {
      // Type-promoted equality is the generic path's decision, not the
      // bit-equality key set's.
      return std::nullopt;
    }
    if (!index->exists_keys.contains(probe.value.int_value)) {
      Relation empty;
      empty.schema = index->schema;
      return empty;
    }
    Relation one;
    one.schema = index->schema;
    one.AddRow(Row(std::vector<Value>(index->schema.ColumnCount())));
    return one;
  }

  if (index->preaggregated) {
    Relation empty;
    empty.schema = index->schema;
    const SelectStatement* result_statement = &statement;
    std::shared_ptr<SelectStatement> capped;
    bool capped_by_limit = false;
    if (exists_only) {
      capped = PrepareExistsExecution(statement, &capped_by_limit);
      if (capped) {
        result_statement = capped.get();
      }
    }
    if (capped_by_limit) {
      ++context.execution_runtime()->exists_short_circuit_queries;
    }
    StatusOr<Relation> finished = FinishQuery(
        context, *result_statement, std::move(empty), &outer, ctes, false);
    if (!finished.HasValue()) {
      return std::nullopt;
    }
    return finished.MoveValue();
  }

  std::vector<Value> outer_values;
  outer_values.reserve(index->outer_columns.size());
  for (const ColumnName& column : index->outer_columns) {
    StatusOr<Value> outer_value = TryLookup(column, outer);
    if (!outer_value.HasValue()) {
      return std::nullopt;  // Fall back to the generic (diagnostic) path.
    }
    Value value = outer_value.MoveValue();
    if (value.IsNull()) {
      Relation empty;
      empty.schema = index->schema;
      StatusOr<Relation> finished =
          FinishQuery(context, statement, std::move(empty), &outer, ctes);
      if (!finished.HasValue()) {
        return std::nullopt;
      }
      return finished.MoveValue();
    }
    outer_values.push_back(std::move(value));
  }
  const std::string key =
      Row(std::move(outer_values)).EncodeMemcomparableFormat();
  Relation candidates;
  candidates.schema = index->schema;
  const auto [begin, end] = index->rows.equal_range(key);
  for (auto iter = begin; iter != end; ++iter) {
    candidates.AddRow(iter->second);
  }
  candidates.peak_intermediate_rows = candidates.rows.size();
  const SelectStatement* result_statement = &statement;
  std::shared_ptr<SelectStatement> capped;
  bool capped_by_limit = false;
  if (exists_only) {
    capped = PrepareExistsExecution(statement, &capped_by_limit);
    if (capped) {
      result_statement = capped.get();
    }
  }
  if (capped_by_limit) {
    ++context.execution_runtime()->exists_short_circuit_queries;
  }
  StatusOr<Relation> finished_final = FinishQuery(
      context, *result_statement, std::move(candidates), &outer, ctes);
  if (!finished_final.HasValue()) {
    return std::nullopt;
  }
  Relation result = finished_final.MoveValue();
  if (!cache_key.has_value()) {
    StatusOr<Relation> copied = MaterializeRelation(result);
    if (!copied.HasValue()) {
      return std::nullopt;
    }
    return copied.MoveValue();
  }
  auto [iter, inserted] = index->cached_results.emplace(
      *cache_key, std::make_shared<Relation>(std::move(result)));
  StatusOr<Relation> copied = MaterializeRelation(*iter->second);
  if (!copied.HasValue()) {
    return std::nullopt;
  }
  return copied.MoveValue();
}

bool ExpressionUsesOnlyScopes(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const Expression& expression,
    const std::vector<Relation>& sources, const CteMap& ctes) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return true;
    }
    return std::ranges::any_of(sources, [&](const Relation& source) {
      return LocalColumnOffset(source.schema, column).has_value();
    });
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    return ExpressionUsesOnlyScopes(context, query.Test(), sources, ctes) &&
           StatementUsesOnlyScopes(context, *query.Query(), sources, ctes);
  }
  return std::ranges::all_of(
      ExpressionChildren(expression),
      [&](const Expression& child) {  // NOLINT(misc-no-recursion)
        return ExpressionUsesOnlyScopes(context, child, sources, ctes);
      });
}

bool StatementUsesOnlyScopes(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const std::vector<Relation>& outer_sources, const CteMap& ctes) {
  std::vector<Relation> scopes;
  scopes.reserve(outer_sources.size());
  for (const Relation& source : outer_sources) {
    Relation metadata;
    metadata.schema = source.schema;
    scopes.push_back(std::move(metadata));
  }
  for (const SelectSource& source : statement.Sources()) {
    if (source.query || source.unnest) {
      return false;
    }
    Relation metadata;

    if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      metadata.schema = cte->second->schema;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        return false;
      }
      metadata.schema = table.Value()->GetSchema();
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    if (!qualifier.empty()) {
      metadata.schema = QualifySchema(metadata.schema, qualifier);
    }
    scopes.push_back(std::move(metadata));
    if (!ExpressionUsesOnlyScopes(context, source.join_condition, scopes,
                                  ctes)) {
      return false;
    }
  }
  std::vector<Expression> expressions = SplitConjuncts(statement.WhereClause());
  for (const NamedExpression& item : statement.SelectList()) {
    expressions.push_back(item.expression);
  }
  expressions.insert(expressions.end(), statement.GroupBy().begin(),
                     statement.GroupBy().end());
  if (statement.Having()) {
    expressions.push_back(statement.Having());
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    expressions.push_back(term.expression);
  }
  return std::ranges::all_of(
      expressions,
      [&](const Expression& expression) {  // NOLINT(misc-no-recursion)
        return ExpressionUsesOnlyScopes(context, expression, scopes, ctes);
      });
}

bool ExpressionsAreLocal(TransactionContext& context,
                         const SelectStatement& statement,
                         const std::vector<Relation>& sources,
                         const CteMap& ctes) {
  std::vector<Expression> expressions = SplitConjuncts(statement.WhereClause());
  for (const NamedExpression& item : statement.SelectList()) {
    expressions.push_back(item.expression);
  }
  expressions.insert(expressions.end(), statement.GroupBy().begin(),
                     statement.GroupBy().end());
  if (statement.Having()) {
    expressions.push_back(statement.Having());
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    expressions.push_back(term.expression);
  }
  for (const Expression& expression : expressions) {
    if (!ExpressionUsesOnlyScopes(context, expression, sources, ctes)) {
      return false;
    }
    for (const ColumnName& column : expression->TouchedColumns()) {
      bool found = false;
      for (const Relation& source : sources) {
        if (LocalColumnOffset(source.schema, column)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
  }
  return true;
}

bool StatementContainsVolatileFunction(const SelectStatement& statement) {
  const auto expression_contains_volatile = [](const Expression& expression,
                                               const auto& self) -> bool {
    if (!expression) {
      return false;
    }
    if (expression->Type() == TypeTag::kFunctionCallExp &&
        GetFunctionVolatility(
            expression->AsFunctionCallExpression().FuncName()) ==
            Volatility::kVolatile) {
      return true;
    }
    if (expression->Type() == TypeTag::kQueryExp) {
      const QueryExpression& query = expression->AsQueryExpression();
      if (query.Query() && StatementContainsVolatileFunction(*query.Query())) {
        return true;
      }
      return self(query.Test(), self);
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto& aggregate = expression->AsAggregateExpression();
      return self(aggregate.Child(), self) ||
             self(aggregate.HavingCondition(), self) ||
             self(aggregate.SecondaryArg(), self) ||
             self(aggregate.WhereFilter(), self) ||
             std::ranges::any_of(aggregate.TrailingArgs(), [&](const auto& arg) {
               return self(arg, self);
             }) ||
             std::ranges::any_of(
                 aggregate.InnerOrderBy(), [&](const auto& term) {
                   return self(term.expression, self);
                 }) ||
             std::ranges::any_of(
                 aggregate.InnerGroupBy(), [&](const auto& key) {
                   return self(key, self);
                 });
    }
    if (expression->Type() == TypeTag::kWindowFunctionExp) {
      const auto& window = expression->AsWindowFunctionCallExpression();
      return std::ranges::any_of(window.args, [&](const auto& arg) {
               return self(arg, self);
             }) ||
             self(window.where_filter, self) ||
             std::ranges::any_of(
                 window.partition_by, [&](const auto& key) {
                   return self(key, self);
                 }) ||
             std::ranges::any_of(window.order_by, [&](const auto& term) {
               return self(term.expression, self);
             }) ||
             std::ranges::any_of(
                 window.inner_order_by, [&](const auto& term) {
                   return self(term.expression, self);
                 }) ||
             self(window.frame_start.offset, self) ||
             self(window.frame_end.offset, self);
    }
    return std::ranges::any_of(
        ExpressionChildren(expression),
        [&](const Expression& child) { return self(child, self); });
  };
  const auto statement_contains_volatile = [&](const SelectStatement& current,
                                               const auto& self) -> bool {
    for (const NamedExpression& item : current.SelectList()) {
      if (expression_contains_volatile(item.expression,
                                       expression_contains_volatile)) {
        return true;
      }
    }
    for (const Expression& expression : current.GroupBy()) {
      if (expression_contains_volatile(expression,
                                       expression_contains_volatile)) {
        return true;
      }
    }
    if (expression_contains_volatile(current.WhereClause(),
                                     expression_contains_volatile) ||
        expression_contains_volatile(current.Having(),
                                     expression_contains_volatile) ||
        expression_contains_volatile(current.Qualify(),
                                     expression_contains_volatile)) {
      return true;
    }
    for (const SelectStatement::OrderByTerm& term : current.OrderBy()) {
      if (expression_contains_volatile(term.expression,
                                       expression_contains_volatile)) {
        return true;
      }
    }
    for (const SelectSource& source : current.Sources()) {
      if (expression_contains_volatile(source.join_condition,
                                       expression_contains_volatile) ||
          expression_contains_volatile(source.unnest,
                                       expression_contains_volatile)) {
        return true;
      }
      if (source.query && self(*source.query, self)) {
        return true;
      }
    }
    for (const auto& [name, query] : current.WithQueries()) {
      (void)name;
      if (query && self(*query, self)) {
        return true;
      }
    }
    return std::ranges::any_of(
        current.UnionAll(),
        [&self](const std::shared_ptr<SelectStatement>& query) {
          return query && self(*query, self);
        });
  };
  return statement_contains_volatile(statement, statement_contains_volatile);
}

std::string UncorrelatedCacheKey(const SelectStatement& statement,
                                 const CteMap& ctes, bool exists_only) {
  std::string key = statement.Fingerprint();
  key += exists_only ? "|mode=exists" : "|mode=rows";
  if (ctes.empty()) {
    return key;
  }
  std::vector<std::string> names;
  names.reserve(ctes.size());
  for (const auto& [name, relation] : ctes) {
    std::ostringstream part;
    part << name << '@' << static_cast<const void*>(relation.get());
    names.push_back(part.str());
  }
  std::ranges::sort(names);
  key += "|ctes=";
  for (const std::string& name : names) {
    key += name;
    key.push_back(';');
  }
  return key;
}

std::string CorrelatedApplyCacheKey(const SelectStatement& statement,
                                    const Scope* outer, const CteMap& ctes,
                                    bool exists_only) {
  std::string key = UncorrelatedCacheKey(statement, ctes, exists_only);
  key += "|outer=";
  std::unordered_set<ColumnName> referenced_columns;
  std::function<void(const SelectStatement&)> collect_statement;
  std::function<void(const Expression&)> collect_expression;
  collect_expression = [&](const Expression& expression) {
    if (!expression) {
      return;
    }
    if (expression->Type() == TypeTag::kQueryExp) {
      const QueryExpression& query = expression->AsQueryExpression();
      collect_expression(query.Test());
      if (query.Query()) {
        collect_statement(*query.Query());
      }
      return;
    }
    for (const ColumnName& column : expression->TouchedColumns()) {
      referenced_columns.insert(column);
    }
    for (const Expression& child : ExpressionChildren(expression)) {
      collect_expression(child);
    }
  };
  collect_statement = [&](const SelectStatement& current) {
    for (const NamedExpression& item : current.SelectList()) {
      collect_expression(item.expression);
    }
    for (const Expression& item : current.GroupBy()) {
      collect_expression(item);
    }
    collect_expression(current.WhereClause());
    collect_expression(current.Having());
    collect_expression(current.Qualify());
    for (const SelectStatement::OrderByTerm& item : current.OrderBy()) {
      collect_expression(item.expression);
    }
    for (const SelectSource& source : current.Sources()) {
      collect_expression(source.join_condition);
      collect_expression(source.unnest);
    }
  };
  collect_statement(statement);
  size_t depth = 0;
  for (const Scope* current = outer; current != nullptr;
       current = current->outer, ++depth) {
    key += std::to_string(depth);
    key.push_back(':');
    if (current->aggregates != nullptr && !current->aggregates->empty()) {
      std::vector<std::pair<const AggregateExpression*, std::string>>
          aggregate_values;
      aggregate_values.reserve(current->aggregates->size());
      for (const auto& [expression, value] : *current->aggregates) {
        aggregate_values.emplace_back(expression,
                                      value.EncodeMemcomparableFormat());
      }
      std::ranges::sort(aggregate_values, [](const auto& left,
                                             const auto& right) {
        return std::less<const AggregateExpression*>()(left.first, right.first);
      });
      key += "|aggregates=";
      for (const auto& [expression, value] : aggregate_values) {
        key += std::to_string(reinterpret_cast<uintptr_t>(expression));
        key.push_back(':');
        key += value;
        key.push_back(';');
      }
    }
    if (current->row == nullptr) {
      key += "<null-row>;";
      continue;
    }
    std::vector<Value> parameters;
    std::string parameter_names;
    if (current->schema != nullptr) {
      for (size_t column = 0; column < current->schema->ColumnCount();
           ++column) {
        const ColumnName& candidate = current->schema->GetColumn(column).Name();
        const bool referenced = std::ranges::any_of(
            referenced_columns, [&](const ColumnName& requested) {
              return requested == candidate ||
                     requested.ToString() == candidate.ToString() ||
                     (requested.schema.empty() &&
                      requested.name == candidate.name);
            });
        if (referenced && column < current->row->values_.size()) {
          parameters.push_back((*current->row)[column]);
          parameter_names += candidate.ToString();
          parameter_names.push_back(',');
        }
      }
    }
    // A query with an opaque/derived source may have no resolvable local
    // metadata. Retain the conservative full-row key in that case.
    if (parameters.empty()) {
      parameters = current->row->values_;
      parameter_names = "<full-row>,";
    }
    key += parameter_names;
    key.push_back('|');
    // The memcomparable index encoding rejects NULL. Apply parameters may be
    // NULL, so use Row's null-bitmap serializer instead.
    const Row parameter_row(std::move(parameters));
    std::string encoded(parameter_row.Size(), '\0');
    const size_t serialized = parameter_row.Serialize(encoded.data());
    encoded.resize(serialized);
    key.append(encoded);
    key.push_back(';');
  }
  return key;
}

const Relation* ExecuteCachedUncorrelated(TransactionContext& context,
                                          const SelectStatement& statement,
                                          const CteMap& ctes,
                                          bool exists_only) {
  if (context.execution_runtime() == nullptr ||
      context.execution_runtime()->noncacheable_queries.contains(&statement) ||
      StatementContainsVolatileFunction(statement)) {
    return nullptr;
  }
  // Separate AST nodes can describe the same uncorrelated subquery (for
  // example two identical scalar expressions in one SELECT list).  Use a
  // structural key for those nodes, but include the identity of inherited
  // CTE relations so a name resolving to different materialized contents can
  // never reuse an unrelated result.
  // EXISTS may safely execute a capped copy, while scalar/IN consumers need
  // the complete relation (and scalar consumers must still observe a second
  // row as a cardinality violation). Never let the capped representation be
  // reused by a full-result consumer.
  const std::string structural_key =
      UncorrelatedCacheKey(statement, ctes, exists_only);
  if (const auto structural =
          context.execution_runtime()->uncorrelated_results_by_fingerprint.find(
              structural_key);
      structural !=
      context.execution_runtime()->uncorrelated_results_by_fingerprint.end()) {
    ++context.execution_runtime()->uncorrelated_cache_hits;
    context.execution_runtime()->uncorrelated_results.emplace(
        &statement, structural->second);
    return structural->second.get();
  }
  const auto cached =
      context.execution_runtime()->uncorrelated_results.find(&statement);
  if (cached != context.execution_runtime()->uncorrelated_results.end()) {
    if (CacheEntryIsCurrent(statement, *context.execution_runtime())) {
      ++context.execution_runtime()->uncorrelated_cache_hits;
      return cached->second.get();
    }
    DropCacheEntry(*context.execution_runtime(), &statement);
    context.execution_runtime()->uncorrelated_results.erase(cached);
  }
  CacheEntryIsCurrent(statement, *context.execution_runtime());

  std::vector<Relation> schemas;
  schemas.reserve(statement.Sources().size());
  for (const SelectSource& source : statement.Sources()) {
    Relation metadata;
    if (source.query) {
      if (!StatementUsesOnlyScopes(context, *source.query, {}, ctes)) {
        context.execution_runtime()->noncacheable_queries.insert(&statement);
        return nullptr;
      }
      std::vector<Column> columns;
      columns.reserve(source.query->SelectList().size());
      for (size_t i = 0; i < source.query->SelectList().size(); ++i) {
        const NamedExpression& projection = source.query->SelectList()[i];
        if (projection.expression->Type() == TypeTag::kColumnValue &&
            projection.expression->AsColumnValue().GetColumnName().name ==
                "*") {
          context.execution_runtime()->noncacheable_queries.insert(&statement);
          return nullptr;
        }
        std::string col_name = ProjectionName(projection, i);
        if (source.query->SelectList().size() == 1 && !source.alias.empty() &&
            projection.name.empty()) {
          col_name = source.alias;
        }
        columns.emplace_back(std::move(col_name), ValueType::kNull);
      }
      metadata.schema = Schema("", std::move(columns));
    } else if (source.unnest) {
      std::string col_name = source.alias.empty() ? "unnest" : source.alias;
      std::vector<Column> columns;
      columns.emplace_back(col_name, ValueType::kNull);
      if (!source.offset_alias.empty()) {
        columns.emplace_back(source.offset_alias, ValueType::kInt64);
      }
      metadata.schema = Schema("", std::move(columns));
      // The array expression itself must be resolvable from the local scope:
      // cached execution runs without an outer scope, so an UNNEST over an
      // outer column (UNNEST(outer.a) WITH OFFSET) is correlated.
      for (const ColumnName& column : source.unnest->TouchedColumns()) {
        if (!LocalColumnOffset(metadata.schema, column)) {
          context.execution_runtime()->noncacheable_queries.insert(&statement);
          return nullptr;
        }
      }
    } else if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      metadata.schema = cte->second->schema;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        context.execution_runtime()->noncacheable_queries.insert(&statement);
        return nullptr;
      }
      metadata.schema = table.Value()->GetSchema();
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    if (!qualifier.empty() && !source.unnest) {
      metadata.schema = QualifySchema(metadata.schema, qualifier);
    }
    schemas.push_back(std::move(metadata));
  }
  // An UNNEST source whose argument references columns outside this
  // statement's own sources is inherently correlated (`FROM t.Info.str_value`
  // inside a subquery); it must never run with a detached (null) outer scope.
  for (const SelectSource& source : statement.Sources()) {
    if (!source.unnest) {
      continue;
    }
    if (!ExpressionUsesOnlyScopes(context, source.unnest, schemas, ctes)) {
      context.execution_runtime()->noncacheable_queries.insert(&statement);
      return nullptr;
    }
  }
  if (!ExpressionsAreLocal(context, statement, schemas, ctes)) {
    context.execution_runtime()->noncacheable_queries.insert(&statement);
    return nullptr;
  }
  const SelectStatement* execution_statement = &statement;
  std::shared_ptr<SelectStatement> capped;
  bool capped_by_limit = false;
  // EXISTS is insensitive to the order or payload of rows after the first
  // qualifying row.  Apply the cap only when it cannot override an explicit
  // LIMIT 0, and retain the original statement as the cache key.
  if (exists_only) {
    capped = PrepareExistsExecution(statement, &capped_by_limit);
    if (capped) {
      execution_statement = capped.get();
    }
  }
  if (capped_by_limit) {
    context.execution_runtime()->retained_statements.push_back(capped);
    ++context.execution_runtime()->exists_short_circuit_queries;
  }
  StatusOr<Relation> executed =
      ExecuteQuery(context, *execution_statement, nullptr, ctes);
  if (!executed.HasValue()) {
    return nullptr;
  }
  Relation result = executed.MoveValue();
  if (std::getenv("TINYLAMB_TRACE_UNCORRELATED") !=
      nullptr) {  // NOLINT(cert-err33-c)
    (void)fprintf(stderr, "[UNCORR] rows=%zu schema_cols=%zu exists=%d\n",
            result.TotalRows(),
            static_cast<size_t>(result.schema.ColumnCount()),
            static_cast<int>(exists_only));
  }
  auto [iter, inserted] =
      context.execution_runtime()->uncorrelated_results.emplace(
          &statement, std::make_shared<Relation>(std::move(result)));
  context.execution_runtime()->uncorrelated_results_by_fingerprint.emplace(
      structural_key, iter->second);
  return iter->second.get();
}

}  // namespace tinylamb::relational_detail
