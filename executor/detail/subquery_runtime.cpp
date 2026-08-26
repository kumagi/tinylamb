/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/subquery_runtime.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ratio>
#include <string>
#include <string_view>
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
  if (!expression) { return expression;
}
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (!name.schema.empty() || name.name == "*") { return expression; }
      if (LocalColumnOffset(schema, name)) { return expression; }
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
      for (const auto& [condition, result] : value.when_clauses_) {
        clauses.emplace_back(ResolveAliasInTree(condition, schema, select_list),
                             ResolveAliasInTree(result, schema, select_list));
      }
      return CaseExpressionExp(std::move(clauses),
                               ResolveAliasInTree(value.else_clause_, schema,
                                                  select_list));
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      std::vector<Expression> items;
      items.reserve(value.list_.size());
      for (const Expression& item : value.list_) {
        items.push_back(ResolveAliasInTree(item, schema, select_list));
      }
      return InExpressionExp(ResolveAliasInTree(value.child_, schema,
                                                select_list),
                             std::move(items));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(
          expression->AsFunctionCallExpression().FuncName(),
          [&] {
            std::vector<Expression> args;
            args.reserve(expression->AsFunctionCallExpression().Args().size());
            for (const Expression& argument :
                 expression->AsFunctionCallExpression().Args()) {
              args.push_back(
                  ResolveAliasInTree(argument, schema, select_list));
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
      return ArrayExpressionExp(std::move(elements),
                                array.ElementSqlType());
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
  if (select_list.empty()) { return nullptr; }
  bool changed = false;
  std::vector<Expression> group_by;
  group_by.reserve(statement.GroupBy().size());
  for (const Expression& key : statement.GroupBy()) {
    Expression resolved = key;
    if (key && key->Type() == TypeTag::kColumnValue) {
      const ColumnName& name =
          key->AsColumnValue().GetColumnName();
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
          } catch (...) {
          }
        }
      }
    } else if (key) {
      resolved = ResolveAliasInTree(key, input_schema, select_list);
      if (!(resolved == key)) { changed = true;
}
    }
    group_by.push_back(std::move(resolved));
  }
  Expression having =
      statement.Having()
          ? ResolveAliasInTree(statement.Having(), input_schema, select_list)
          : statement.Having();
  if (statement.Having() && !(having == statement.Having())) { changed = true;
}
  if (!changed) { return nullptr; }
  auto adjusted = std::make_shared<SelectStatement>(statement);
  adjusted->SetGroupBy(std::move(group_by));
  if (statement.Having()) { adjusted->SetHaving(std::move(having)); }
  return adjusted;
}

Value ProjectSubqueryRow(const Row& row, bool as_struct,
                         const Schema* schema) {
  if (row.values_.empty()) { return {};
}
  if (!as_struct) { return row[0];
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
      if (!name.empty() && !name.starts_with("$expr")) { return name; }
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
          if (i > 0) { inner += ","; }
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
    if (i > 0) { json += ","; }
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
    const Scope& outer, const CteMap& ctes) {
  if (context.execution_runtime() == nullptr || statement.Sources().empty() ||
      std::ranges::any_of(
          statement.Sources(),
          [](const SelectSource& source) { return source.query != nullptr; }) ||
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
      !context.execution_runtime()->unindexable_queries.contains(
          &statement)) {
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
      source = LoadSource(context, from, &outer, ctes,
                          projection.empty() ? nullptr : &projection, nullptr,
                          nullptr, std::nullopt);
    } else {
      bool predicates_applied = false;
      source =
          BuildInput(context, statement, &outer, ctes, &predicates_applied);
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
    const bool integer_key =
        SingleIntegerJoinKey(source.schema, created->local_columns);
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
      struct GroupAggs {
        std::vector<AggregateAccumulator> accumulators;
      };
      std::unordered_map<int64_t, GroupAggs> int_groups;
      std::unordered_map<std::string, GroupAggs> str_groups;
      const CompiledScanFilter local_filter =
          CompileScanFilter(local_predicates, source.schema);
      source.ForEachRow([&](const Row& row) {
        if (HasNullKey(row, created->local_columns)) {
          return;
        }
        if (!MatchScanFilter(row, source.schema, local_filter, nullptr, context,
                             ctes)) {
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
            group->accumulators.emplace_back(aggregate);
          }
        }
        Scope scope{.row = &row, .schema = &source.schema, .outer = nullptr};
        for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
          const AggregateExpression& aggregate = *aggregate_expressions[i];
          if (aggregate.WhereFilter() &&
              !Truthy(Evaluate(aggregate.WhereFilter(), scope, nullptr, context,
                               ctes))) {
            continue;
          }
          if (IsCountStar(aggregate)) {
            group->accumulators[i].Add(Value(1));
            continue;
          }
          AggregateInput input;
          input.value =
              Evaluate(aggregate.Child(), scope, nullptr, context, ctes);
          if (aggregate.Having() != AggregateHavingModifier::kNone &&
              aggregate.HavingCondition()) {
            input.condition = Evaluate(aggregate.HavingCondition(), scope,
                                       nullptr, context, ctes);
          }
          for (const auto& term : aggregate.InnerOrderBy()) {
            input.order_keys.push_back(
                Evaluate(term.expression, scope, nullptr, context, ctes));
          }
          if (aggregate.GetType() == AggregationType::kStringAgg &&
              aggregate.SecondaryArg()) {
            input.auxiliary = Evaluate(aggregate.SecondaryArg(), scope, nullptr,
                                       context, ctes);
          }
          for (const Expression& extra : aggregate.TrailingArgs()) {
            if (extra) {
              input.trailing_values.push_back(
                  Evaluate(extra, scope, nullptr, context, ctes));
            }
          }
          group->accumulators[i].Add(std::move(input));
        }
      });
      auto emit_group = [&](const std::string& key, GroupAggs& group) {
        AggregateResultMap aggregate_results;
        aggregate_results.reserve(group.accumulators.size());
        for (const AggregateAccumulator& accumulator : group.accumulators) {
          aggregate_results.emplace(accumulator.expression,
                                    accumulator.Finish());
        }
        Row representative;
        Scope scope{
            .row = &representative, .schema = &source.schema, .outer = nullptr};
        std::vector<Value> values;
        values.reserve(statement.SelectList().size());
        for (const NamedExpression& item : statement.SelectList()) {
          values.push_back(Evaluate(item.expression, scope, &aggregate_results,
                                    context, ctes));
        }
        // Build the schema from the local values directly: AddRow may spill
        // and empty `finished.rows`, so rows[0] is not safe to read back.
        std::vector<Column> columns;
        columns.reserve(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
          columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                               ValueTypeOf(values[i]));
        }
        Relation finished;
        finished.schema = Schema("", std::move(columns));
        finished.AddRow(Row(std::move(values)));
        created->cached_results.emplace(
            key, std::make_shared<Relation>(std::move(finished)));
      };
      if (integer_key) {
        for (auto& [key, group] : int_groups) {
          emit_group(Row({Value(key)}).EncodeMemcomparableFormat(), group);
        }
      } else {
        for (auto& [key, group] : str_groups) {
          emit_group(key, group);
        }
      }
    } else {
      source.ForEachRow([&](const Row& row) {
        if (HasNullKey(row, created->local_columns)) {
          return;
        }
        if (!local_predicates.empty()) {
          Scope scope{.row = &row, .schema = &source.schema, .outer = nullptr};
          for (const Expression& predicate : local_predicates) {
            if (!Truthy(Evaluate(predicate, scope, nullptr, context, ctes))) {
              return;
            }
          }
        }
        const std::string key = EncodeJoinKey(row, created->local_columns);
        created->rows.emplace(key, row);
      });
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

  std::vector<Value> cache_values;
  cache_values.reserve(index->cache_outer_columns.size());
  for (const ColumnName& column : index->cache_outer_columns) {
    cache_values.push_back(Lookup(column, outer));
  }
  const std::string cache_key =
      Row(std::move(cache_values)).EncodeMemcomparableFormat();
  if (const auto cached_result = index->cached_results.find(cache_key);
      cached_result != index->cached_results.end()) {
    ++context.execution_runtime()->correlated_result_cache_hits;
    return MaterializeRelation(*cached_result->second);
  }

  if (index->preaggregated) {
    Relation empty;
    empty.schema = index->schema;
    return FinishQuery(context, statement, std::move(empty), &outer, ctes,
                       false);
  }

  std::vector<Value> outer_values;
  outer_values.reserve(index->outer_columns.size());
  for (const ColumnName& column : index->outer_columns) {
    Value value = Lookup(column, outer);
    if (value.IsNull()) {
      Relation empty;
      empty.schema = index->schema;
      return FinishQuery(context, statement, std::move(empty), &outer, ctes);
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
  Relation result =
      FinishQuery(context, statement, std::move(candidates), &outer, ctes);
  auto [iter, inserted] = index->cached_results.emplace(
      cache_key, std::make_shared<Relation>(std::move(result)));
  return MaterializeRelation(*iter->second);
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

const Relation* ExecuteCachedUncorrelated(TransactionContext& context,
                                          const SelectStatement& statement,
                                          const CteMap& ctes) {
  if (context.execution_runtime() == nullptr ||
      context.execution_runtime()->noncacheable_queries.contains(&statement)) {
    return nullptr;
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
    if (!source.unnest) { continue;
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
  Relation result = ExecuteQuery(context, statement, nullptr, ctes);
  auto [iter, inserted] =
      context.execution_runtime()->uncorrelated_results.emplace(
          &statement, std::make_shared<Relation>(std::move(result)));
  return iter->second.get();
}

}  // namespace tinylamb::relational_detail
