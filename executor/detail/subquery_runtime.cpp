/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/subquery_runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
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
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
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

// Runtime-selected backing store for int64-keyed group maps: the flat
// open-addressing table by default, the node-based unordered_map under the
// TINYLAMB_DISABLE_FLAT_GROUP_MAP hatch.  One shape serves every use site.
template <typename Group>
class IntGroupMapAdapter {
 public:
  IntGroupMapAdapter() : use_flat_(true) {}
  explicit IntGroupMapAdapter(bool use_flat) : use_flat_(use_flat) {}

  std::pair<Group*, bool> TryEmplace(int64_t key) {
    if (use_flat_) {
      bool inserted = false;
      return {flat_.TryEmplace(key, &inserted), inserted};
    }
    auto [iter, inserted] = node_.try_emplace(key);
    return {&iter->second, inserted};
  }

  template <typename Fn>
  void ForEach(Fn&& fn) {
    if (use_flat_) {
      flat_.ForEach(std::forward<Fn>(fn));
      return;
    }
    for (auto& [key, group] : node_) {
      fn(key, group);
    }
  }

  void set_use_flat(bool value) { use_flat_ = value; }

 private:
  bool use_flat_;
  FlatIntKeyMap<Group> flat_;
  std::unordered_map<int64_t, Group> node_;
};
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

ScopedCorrelatedOuter::ScopedCorrelatedOuter(TransactionContext& context,
                                             const Relation& relation)
    : context_(&context),
      saved_relation_(
          context.execution_runtime() != nullptr
              ? context.execution_runtime()->correlated_outer_relation
              : nullptr),
      saved_serial_(context.execution_runtime() != nullptr
                        ? context.execution_runtime()->correlated_outer_serial
                        : 0) {
  ExecutionRuntime* runtime = context.execution_runtime();
  if (runtime == nullptr) {
    return;
  }
  runtime->correlated_outer_serials += 1;
  runtime->correlated_outer_relation = &relation;
  runtime->correlated_outer_serial = runtime->correlated_outer_serials;
}

ScopedCorrelatedOuter::~ScopedCorrelatedOuter() {
  ExecutionRuntime* runtime = context_->execution_runtime();
  if (runtime == nullptr) {
    return;
  }
  runtime->correlated_outer_relation = saved_relation_;
  runtime->correlated_outer_serial = saved_serial_;
}

std::optional<ScanRuntimeFilter> CorrelationDomainFilter(
    const Schema& source_schema, slot_t local_column,
    const ColumnName& outer_column, const std::vector<slot_t>& projection,
    const ExecutionRuntime& runtime) {
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_CORRELATED_DOMAIN_FILTER") != nullptr;
  if (disabled || runtime.correlated_outer_relation == nullptr ||
      local_column >= source_schema.ColumnCount()) {
    return std::nullopt;
  }
  const ValueType type = source_schema.GetColumn(local_column).Type();
  if (type != ValueType::kInt64 && type != ValueType::kDate) {
    return std::nullopt;
  }
  const Relation& outer = *runtime.correlated_outer_relation;
  const std::optional<size_t> outer_offset =
      LocalColumnOffset(outer.schema, outer_column);
  if (!outer_offset || *outer_offset >= outer.schema.ColumnCount()) {
    return std::nullopt;
  }
  // The extraction walks every outer row once per build; skip shapes whose
  // outer pipeline is large enough to make that walk its own regression.
  // Spilled outer relations are also skipped outright: iteration then reads
  // through the spill layer, and a misread value here would silently shrink
  // the build into a wrong empty result (the spill-memory-budget regime is
  // one where the unfiltered build was affordable anyway).
  if (outer.HasSpill() || outer.TotalRows() == 0 ||
      outer.TotalRows() > 2000000) {
    return std::nullopt;
  }
  RuntimeFilterBuilder builder;
  const Status iteration = outer.ForEachRow([&](const Row& row) {
    const Value& value = row[*outer_offset];
    // NULL never satisfies an equality, so it is correctly outside the
    // domain; a value of any other type is foreign to the comparison domain.
    if (value.IsNull() ||
        static_cast<int8_t>(value.type) != static_cast<int8_t>(type)) {
      return;
    }
    builder.Observe(value.value.int_value);
  });
  if (iteration != Status::kSuccess || builder.overflowed() ||
      builder.empty() || builder.DistinctKeys() > 1000000) {
    return std::nullopt;
  }
  auto group = builder.Build(false);
  if (group == nullptr) {
    return std::nullopt;
  }
  for (size_t i = 0; i < projection.size(); ++i) {
    if (projection[i] == local_column) {
      ScanRuntimeFilter filter;
      filter.group = std::move(group);
      filter.column = static_cast<slot_t>(i);
      filter.type = type;
      return filter;
    }
  }
  return std::nullopt;
}

// True when the cache entry keyed by `statement`'s address was created for
// this very statement (address recycling guard). Records `fresh` when absent.
// The fingerprint string is memoized per statement pointer for the execution:
// subquery statements are retained (retained_statements) and immutable within
// one runtime, so rebuilding the AST walk per probe (Q22: ~61K) is waste.
bool CacheEntryIsCurrent(const SelectStatement& statement,
                         ExecutionRuntime& runtime) {
  const uint64_t id = statement.RuntimeId();
  auto memo = runtime.fingerprint_memo.find(&statement);
  if (memo != runtime.fingerprint_memo.end() && memo->second.first != id) {
    memo->second = {id, statement.Fingerprint()};
  } else if (memo == runtime.fingerprint_memo.end()) {
    memo = runtime.fingerprint_memo
               .emplace(&statement, std::make_pair(id, statement.Fingerprint()))
               .first;
  }
  const std::string& fresh = memo->second.second;
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
  runtime.fingerprint_memo.erase(key);
  runtime.uncorrelated_key_memo.erase(key);
  runtime.volatile_memo.erase(key);
  runtime.noncacheable_queries.erase(key);
  runtime.unindexable_queries.erase(key);
}

namespace {

// Count of the statement's base-table sources whose (alias-qualified) schema
// resolves `name`, with the resolving source's ordinal when exactly one
// does.  CTE sources have no base table to peek and never resolve here.
std::optional<size_t> ResolvingSource(TransactionContext& context,
                                      const SelectStatement& statement,
                                      const CteMap& ctes,
                                      const ColumnName& name) {
  std::optional<size_t> found;
  for (size_t i = 0; i < statement.Sources().size(); ++i) {
    const SelectSource& source = statement.Sources()[i];
    if (source.query != nullptr || source.unnest != nullptr ||
        source.sample.has_value() || source.table.empty()) {
      continue;
    }
    if (ctes.contains(source.table) ||
        statement.WithQueries().contains(source.table)) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      continue;
    }
    Schema schema = table.Value()->GetSchema();
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    if (!qualifier.empty()) {
      schema = QualifySchema(schema, qualifier);
    }
    if (LocalColumnOffset(schema, name).has_value()) {
      if (found.has_value()) {
        return std::nullopt;  // Ambiguous: two sources resolve the name.
      }
      found = i;
    }
  }
  return found;
}

// The `local = outer` column pair of the statement's first correlation
// equality, with the local side resolving in exactly one base-table source.
// Drives the multi-source build's domain stash.
std::optional<std::pair<ColumnName, ColumnName>> CorrelationEqualityOf(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  for (const Expression& predicate : SplitConjuncts(statement.WhereClause())) {
    if (predicate == nullptr || predicate->Type() != TypeTag::kBinaryExp) {
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
    const std::optional<size_t> left_source =
        ResolvingSource(context, statement, ctes, left);
    const std::optional<size_t> right_source =
        ResolvingSource(context, statement, ctes, right);
    if (left_source.has_value() != right_source.has_value()) {
      if (left_source.has_value()) {
        return std::make_pair(left, right);
      }
      return std::make_pair(right, left);
    }
  }
  return std::nullopt;
}

// Stashes the correlation-domain key filter for the source table the
// statement's correlation equality resolves against, so BuildInput's own
// scan of that table prunes foreign keys pre-decode.  Owner-scoped to this
// statement; the caller erased any previous entries.
void StashBuildDomainFilter(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes, const std::pair<ColumnName, ColumnName>& correlation) {
  ExecutionRuntime& runtime = *context.execution_runtime();
  const std::optional<size_t> index =
      ResolvingSource(context, statement, ctes, correlation.first);
  if (!index.has_value()) {
    return;
  }
  const SelectSource& source = statement.Sources()[*index];
  StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
  if (!table.HasValue()) {
    return;
  }
  Schema schema = table.Value()->GetSchema();
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  if (!qualifier.empty()) {
    schema = QualifySchema(schema, qualifier);
  }
  const std::optional<size_t> local_offset =
      LocalColumnOffset(schema, correlation.first);
  if (!local_offset) {
    return;
  }
  // Identity projection: the filter binds the full-schema slot; the
  // consuming scan remaps it into its own projection.
  std::vector<slot_t> identity(schema.ColumnCount());
  for (size_t i = 0; i < identity.size(); ++i) {
    identity[i] = static_cast<slot_t>(i);
  }
  std::optional<ScanRuntimeFilter> filter =
      CorrelationDomainFilter(schema, static_cast<slot_t>(*local_offset),
                              correlation.second, identity, runtime);
  if (!filter.has_value()) {
    return;
  }
  // A self-join over the same table would apply one column's domain to both
  // sides; only stash unambiguous (single-source) tables.
  size_t same_table = 0;
  for (const SelectSource& other : statement.Sources()) {
    if (other.table == source.table) {
      ++same_table;
    }
  }
  if (same_table != 1) {
    return;
  }
  TableKeyFilter& stored = runtime.table_key_filters[TableKeyFilterKey(source)];
  stored.filter = filter->group;
  stored.type = filter->type;
  stored.key_count = filter->group->DistinctKeys();
  stored.column = static_cast<slot_t>(*local_offset);
  stored.owner = &statement;
}

}  // namespace

std::optional<Relation> ExecuteCorrelatedSingleSource(
    TransactionContext& context, const SelectStatement& statement,
    const Scope& outer, const CteMap& ctes, bool exists_only,
    std::optional<bool>* exists_answer, std::optional<Value>* scalar_answer) {
  if (context.execution_runtime() == nullptr) {
    return std::nullopt;
  }

  CorrelatedIndex* index = nullptr;
  const auto cached =
      context.execution_runtime()->correlated_indexes.find(&statement);
  if (cached != context.execution_runtime()->correlated_indexes.end()) {
    if (CacheEntryIsCurrent(statement, *context.execution_runtime())) {
      CorrelatedIndex* candidate = cached->second.get();
      // A build made under a correlation-domain filter only carries keys of
      // the outer context it was built for; reusing it under any other
      // context (including no context) would silently miss rows.  Unfiltered
      // builds (serial 0) stay universally valid.
      if (candidate->outer_filter_serial == 0 ||
          (candidate->outer_filter_relation ==
               context.execution_runtime()->correlated_outer_relation &&
           candidate->outer_filter_serial ==
               context.execution_runtime()->correlated_outer_serial)) {
        index = candidate;
      } else {
        DropCacheEntry(*context.execution_runtime(), &statement);
        context.execution_runtime()->correlated_indexes.erase(cached);
        context.execution_runtime()->unindexable_queries.erase(&statement);
      }
    } else {
      // Address recycled: the entry belongs to a dead statement.
      DropCacheEntry(*context.execution_runtime(), &statement);
      context.execution_runtime()->correlated_indexes.erase(cached);
      context.execution_runtime()->unindexable_queries.erase(&statement);
    }
  }
  if (index == nullptr) {
    if (statement.Sources().empty() ||
        std::ranges::any_of(statement.Sources(),
                            [](const SelectSource& source) {
                              return source.query != nullptr;
                            }) ||
        !statement.UnionAll().empty() ||
        statement.GetSetOperationTree() != nullptr || statement.Qualify() ||
        HasWindowFunctions(statement) ||
        StatementContainsVolatileFunctionCached(context.execution_runtime(),
                                                statement) ||
        context.execution_runtime()->unindexable_queries.contains(&statement)) {
      return std::nullopt;
    }
    const SelectSource& from = statement.Sources()[0];
    if (!from.table.empty() && (ctes.contains(from.table) ||
                                statement.WithQueries().contains(from.table))) {
      return std::nullopt;
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
    // Set when the build scan gets pruned by the correlation-key domain of
    // the currently registered outer relation; recorded on the cache entry
    // so a later probe from a different context rebuilds.
    const void* domain_relation = nullptr;
    uint64_t domain_serial = 0;
    std::vector<Expression> applied_scan_predicates;
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
      // Correlation-domain pruning: the registered outer relation (the
      // WHERE-filter pipeline evaluating this subquery) carries every outer
      // value the probes will ever present, so rows whose correlation key
      // falls outside that domain can be rejected before decode — the same
      // sound shape a hash join uses to prune its probe side.  The stash
      // note above stays true for TableKeyFilter: this filter is derived
      // from THIS statement's own correlation equality and recorded on the
      // cache entry, so an unrelated or later outer context can never
      // narrow another scan.
      std::vector<std::pair<slot_t, ColumnName>> correlation_equalities;
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
        const ColumnName& right =
            binary.Right()->AsColumnValue().GetColumnName();
        const auto left_local = LocalColumnOffset(peek_schema, left);
        const auto right_local = LocalColumnOffset(peek_schema, right);
        if (left_local && !right_local) {
          correlation_equalities.emplace_back(static_cast<slot_t>(*left_local),
                                              right);
        } else if (right_local && !left_local) {
          correlation_equalities.emplace_back(static_cast<slot_t>(*right_local),
                                              left);
        }
      }
      ScanRuntimeFilter domain_filter;
      const ScanRuntimeFilter* domain_filter_ptr = nullptr;
      if (!correlation_equalities.empty() &&
          context.execution_runtime()->correlated_outer_relation != nullptr) {
        for (const auto& [local_slot, outer_col] : correlation_equalities) {
          std::optional<ScanRuntimeFilter> filter =
              CorrelationDomainFilter(peek_schema, local_slot, outer_col,
                                      projection, *context.execution_runtime());
          if (!filter.has_value()) {
            continue;
          }
          if (domain_filter_ptr == nullptr) {
            domain_filter = std::move(*filter);
            domain_filter_ptr = &domain_filter;
            domain_relation =
                context.execution_runtime()->correlated_outer_relation;
            domain_serial =
                context.execution_runtime()->correlated_outer_serial;
          } else {
            domain_filter.extra.push_back(std::move(*filter));
          }
          ++context.execution_runtime()->correlated_domain_filter_builds;
        }
      }
      // NOTE: table_key_filters are intentionally NOT applied here. The stash
      // is owned by the statement that derived it; applying it to correlated
      // subqueries silently narrowed unrelated scans.
      std::vector<Expression> scan_predicates;
      for (const Expression& predicate :
           SplitConjuncts(statement.WhereClause())) {
        if (IsNonThrowingScanComparison(predicate, peek_schema)) {
          scan_predicates.push_back(predicate);
        }
      }
      StatusOr<Relation> loaded =
          LoadSource(context, from, &outer, ctes,
                     projection.empty() ? nullptr : &projection,
                     scan_predicates.empty() ? nullptr : &scan_predicates,
                     domain_filter_ptr);
      if (!loaded.HasValue()) {
        return std::nullopt;
      }
      if (peek_schema.ColumnCount() > 0) {
        applied_scan_predicates = std::move(scan_predicates);
      }
      source = loaded.MoveValue();
    } else {
      // Multi-source build: prune the correlation table's scan to the outer
      // key domain.  The registered outer relation (the WHERE-filter
      // pipeline probing this subquery) carries every correlation value a
      // probe can present, so rows with foreign keys are rejected before
      // decode — the same sound shape the single-source path derives above.
      // Routed through the BuildInput stash (owner = this statement) so the
      // join's own scan of the correlation table consumes it and no other
      // statement ever sees it.  A build made without a registration, or
      // under a different registration, must never inherit a previous
      // domain: stale entries are erased first and the cache entry records
      // the serial it was built under.
      {
        ExecutionRuntime& runtime = *context.execution_runtime();
        for (auto iter = runtime.table_key_filters.begin();
             iter != runtime.table_key_filters.end();) {
          if (iter->second.owner == &statement) {
            iter = runtime.table_key_filters.erase(iter);
          } else {
            ++iter;
          }
        }
        std::optional<std::pair<ColumnName, ColumnName>> correlation =
            CorrelationEqualityOf(context, statement, ctes);
        if (correlation.has_value() &&
            runtime.correlated_outer_relation != nullptr) {
          domain_relation = runtime.correlated_outer_relation;
          domain_serial = runtime.correlated_outer_serial;
          StashBuildDomainFilter(context, statement, ctes, *correlation);
        }
      }
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
    created->outer_filter_relation = domain_relation;
    created->outer_filter_serial = domain_serial;
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
      if (std::ranges::find(applied_scan_predicates, predicate) !=
          applied_scan_predicates.end()) {
        continue;
      }
      local_predicates.push_back(predicate);
    }

    const bool integer_key =
        SingleIntegerJoinKey(source.schema, created->local_columns);
    const bool packed_two_int_key =
        !integer_key && created->local_columns.size() == 2 &&
        std::ranges::all_of(created->local_columns, [&](slot_t col_idx) {
          if (col_idx >= source.schema.ColumnCount()) {
            return false;
          }
          const ValueType t = source.schema.GetColumn(col_idx).Type();
          return t == ValueType::kInt64 || t == ValueType::kDate;
        });
    const auto try_pack_two_int_key = [&](const Row& row,
                                          int64_t* out_key) -> bool {
      const int64_t k0 = IntegerJoinKey(row, created->local_columns[0]);
      const int64_t k1 = IntegerJoinKey(row, created->local_columns[1]);
      if (k0 < std::numeric_limits<int32_t>::min() ||
          k0 > std::numeric_limits<int32_t>::max() ||
          k1 < std::numeric_limits<int32_t>::min() ||
          k1 > std::numeric_limits<int32_t>::max()) {
        return false;
      }
      const uint64_t u = (static_cast<uint64_t>(
                              static_cast<uint32_t>(static_cast<int32_t>(k0)))
                          << 32) |
                         static_cast<uint64_t>(
                             static_cast<uint32_t>(static_cast<int32_t>(k1)));
      *out_key = static_cast<int64_t>(u);
      return true;
    };
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
    const auto packed_two_int_key_encoded = [&](int64_t key) {
      const auto u = static_cast<uint64_t>(key);
      const int64_t k0 = static_cast<int32_t>(static_cast<uint32_t>(u >> 32));
      const int64_t k1 =
          static_cast<int32_t>(static_cast<uint32_t>(u & 0xFFFFFFFFULL));
      const Column& c0 = source.schema.GetColumn(created->local_columns[0]);
      const Column& c1 = source.schema.GetColumn(created->local_columns[1]);
      Value v0 =
          c0.Type() == ValueType::kDate ? Value::DateFromDays(k0) : Value(k0);
      if (c0.IsUnsigned()) {
        v0 = v0.WithUnsigned();
      }
      Value v1 =
          c1.Type() == ValueType::kDate ? Value::DateFromDays(k1) : Value(k1);
      if (c1.IsUnsigned()) {
        v1 = v1.WithUnsigned();
      }
      return Row({std::move(v0), std::move(v1)}).EncodeMemcomparableFormat();
    };

    std::vector<Expression> build_predicates;
    std::vector<Expression> probe_predicates;
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
      bool predicate_references_outer = false;
      if (predicate) {
        for (const ColumnName& column : predicate->TouchedColumns()) {
          if (column.name != "*" && !LocalColumnOffset(source.schema, column)) {
            predicate_references_outer = true;
            break;
          }
        }
      }
      references_outer |= predicate_references_outer;
      if (predicate_references_outer) {
        probe_predicates.push_back(predicate);
      } else {
        build_predicates.push_back(predicate);
      }
    }
    if (aggregate_only) {
      for (const NamedExpression& item : statement.SelectList()) {
        check(item.expression);
      }
      check(statement.Having());
    }
    const bool simple_exists_projection =
        exists_only && statement.GroupBy().empty() &&
        statement.Having() == nullptr && statement.OrderBy().empty() &&
        !statement.Distinct() && !statement.HasLimit() &&
        statement.Offset() == 0 && !statement.Sources()[0].sample &&
        !statement.Sources()[0].join_condition &&
        std::ranges::all_of(
            statement.SelectList(),
            [&](const NamedExpression& item) {
              if (!item.expression) {
                return false;
              }
              if (item.expression->Type() == TypeTag::kConstantValue) {
                return true;
              }
              if (item.expression->Type() != TypeTag::kColumnValue) {
                return false;
              }
              const ColumnName& name =
                  item.expression->AsColumnValue().GetColumnName();
              return name.name == "*" ||
                     LocalColumnOffset(source.schema, name).has_value();
            }) &&
        std::ranges::all_of(local_predicates, [](const Expression& predicate) {
          return predicate && !ContainsQuery(predicate) &&
                 !ContainsAggregate(predicate);
        });
    const bool residual_exists_fast_path =
        simple_exists_projection && !aggregate_only && integer_key &&
        created->local_columns.size() == 1 && !probe_predicates.empty();
    if (references_outer && !residual_exists_fast_path) {
      context.execution_runtime()->unindexable_queries.insert(&statement);
      return std::nullopt;
    }
    if (residual_exists_fast_path) {
      created->exists_residual_fast_path = true;
      created->exists_residual_predicates = probe_predicates;
    }

    source.FinishSpill();
    Status group_error{Status::kSuccess};
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
      // A/B hatch: TINYLAMB_DISABLE_FLAT_GROUP_MAP=1 restores the node-based
      // unordered_map for int group keys; the adapter keeps every use site
      // identical across the two backends.
      static const bool flat_map_disabled =
          std::getenv("TINYLAMB_DISABLE_FLAT_GROUP_MAP") != nullptr;
      using IntGroupMap = IntGroupMapAdapter<GroupAggs>;
      IntGroupMap int_groups{!flat_map_disabled};
      std::unordered_map<std::string, GroupAggs> str_groups;
      // Column-vs-column conjuncts over same-type INT64/DATE columns (Q21's
      // l_receiptdate > l_commitdate) evaluate positionally: peeling them out
      // of the bytecode residual keeps the per-row filter allocation-free.
      // Both conjunct styles must pass, and a positional compare can never
      // raise, so the tolerant-combination semantics are unchanged.
      struct ColumnCompare {
        slot_t a;
        slot_t b;
        BinaryOperation op;
      };
      // A/B hatch: TINYLAMB_DISABLE_AGG_COLUMN_PEEK=1 keeps column-vs-column
      // conjuncts in the bytecode residual (the pre-change behavior).
      static const bool column_peek_disabled =
          std::getenv("TINYLAMB_DISABLE_AGG_COLUMN_PEEK") != nullptr;
      std::vector<ColumnCompare> column_compares;
      std::vector<Expression> residual_predicates;
      residual_predicates.reserve(local_predicates.size());
      for (const Expression& predicate : local_predicates) {
        bool peeled = false;
        if (!column_peek_disabled && predicate &&
            predicate->Type() == TypeTag::kBinaryExp) {
          const BinaryExpression& binary = predicate->AsBinaryExpression();
          const BinaryOperation op = binary.Op();
          if (op == BinaryOperation::kEquals ||
              op == BinaryOperation::kNotEquals ||
              op == BinaryOperation::kGreaterThan ||
              op == BinaryOperation::kGreaterThanEquals ||
              op == BinaryOperation::kLessThan ||
              op == BinaryOperation::kLessThanEquals) {
            if (binary.Left()->Type() == TypeTag::kColumnValue &&
                binary.Right()->Type() == TypeTag::kColumnValue) {
              const std::optional<slot_t> a = LocalColumnOffset(
                  source.schema,
                  binary.Left()->AsColumnValue().GetColumnName());
              const std::optional<slot_t> b = LocalColumnOffset(
                  source.schema,
                  binary.Right()->AsColumnValue().GetColumnName());
              if (a && b) {
                const ValueType ta = source.schema.GetColumn(*a).Type();
                const ValueType tb = source.schema.GetColumn(*b).Type();
                if (ta == tb &&
                    (ta == ValueType::kInt64 || ta == ValueType::kDate)) {
                  column_compares.push_back({*a, *b, op});
                  peeled = true;
                }
              }
            }
          }
        }
        if (!peeled) {
          residual_predicates.push_back(predicate);
        }
      }
      const CompiledScanFilter local_filter =
          CompileScanFilter(residual_predicates, source.schema);
      auto column_compares_pass = [&](const Row& row) -> bool {
        for (const ColumnCompare& compare : column_compares) {
          const Value& left = row[compare.a];
          const Value& right = row[compare.b];
          if (left.IsNull() || right.IsNull()) {
            return false;
          }
          const int64_t lv = left.value.int_value;
          const int64_t rv = right.value.int_value;
          bool keep = false;
          switch (compare.op) {
            case BinaryOperation::kEquals:
              keep = lv == rv;
              break;
            case BinaryOperation::kNotEquals:
              keep = lv != rv;
              break;
            case BinaryOperation::kGreaterThan:
              keep = lv > rv;
              break;
            case BinaryOperation::kGreaterThanEquals:
              keep = lv >= rv;
              break;
            case BinaryOperation::kLessThan:
              keep = lv < rv;
              break;
            case BinaryOperation::kLessThanEquals:
              keep = lv <= rv;
              break;
            default:
              return false;
          }
          if (!keep) {
            return false;
          }
        }
        return true;
      };
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
      // row-slice reduction; anything else (per-row filters, buffered
      // aggregates) falls back to the serial pass.  COUNT(DISTINCT
      // plain-column) also qualifies: its partials merge as a set union
      // (AggregateAccumulator::Merge), which keeps Q21-style whole-table
      // COUNT(DISTINCT l_suppkey) builds parallel.  DISTINCT on SUM/AVG has
      // no exact set-union merge, so it stays serial.
      bool build_parallel = (integer_key || packed_two_int_key) &&
                            !source.HasSpill() && source.TotalRows() >= 16384;
      std::vector<bool> worker_safe(aggregate_expressions.size(), false);
      for (size_t i = 0; build_parallel && i < aggregate_expressions.size();
           ++i) {
        const AggregateExpression& aggregate = *aggregate_expressions[i];
        static const bool distinct_count_disabled =
            std::getenv("TINYLAMB_DISABLE_PARALLEL_COUNT_DISTINCT") != nullptr;
        const bool distinct_count =
            !distinct_count_disabled && aggregate.Distinct() &&
            aggregate.GetType() == AggregationType::kCount;
        // Only accumulator kinds whose parallel partials merge exactly
        // (AggregateAccumulator::Merge) may aggregate inside a worker.
        // Buffered aggregates (ARRAY_AGG/STRING_AGG/group-context kinds)
        // keep their inputs in `buffer_`, which Merge never combines, and
        // kinds like LOGICAL_*/BIT_*/ANY_VALUE neither merge nor set the
        // `count` marker the merge below uses to detect empty partials.
        const bool mergeable_type =
            aggregate.GetType() == AggregationType::kCount ||
            aggregate.GetType() == AggregationType::kSum ||
            aggregate.GetType() == AggregationType::kAvg ||
            aggregate.GetType() == AggregationType::kMin ||
            aggregate.GetType() == AggregationType::kMax;
        const bool safe =
            mergeable_type &&
            (IsCountStar(aggregate) ||
             (aggregate_child_columns[i].has_value() &&
              !aggregate.WhereFilter() &&
              aggregate.Having() == AggregateHavingModifier::kNone &&
              aggregate.InnerOrderBy().empty() && !aggregate.SecondaryArg() &&
              (!aggregate.Distinct() || distinct_count) &&
              aggregate.TrailingArgs().empty())) &&
            (IsCountStar(aggregate) ||
             LocalColumnOffset(
                 source.schema,
                 aggregate.Child()->AsColumnValue().GetColumnName())
                 .has_value());
        worker_safe[i] = safe;
        build_parallel = build_parallel && safe;
      }
      struct BuildWorker {
        IntGroupMap int_groups;
        std::vector<Row> deferred;
      };
      // Hatch plumbing: worker maps share the outer map's backend choice.
      const bool use_flat_groups = !flat_map_disabled;
      auto accumulate_one = [&](const Row& row) -> void {
        if (HasNullKey(row, created->local_columns)) {
          return;
        }
        if (!column_compares_pass(row)) {
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
          group = int_groups.TryEmplace(int_key).first;
        } else if (packed_two_int_key && try_pack_two_int_key(row, &int_key)) {
          group = int_groups.TryEmplace(int_key).first;
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
                              std::max<size_t>(1, build_rows / 4096)});
        if (workers > 1) {
          std::vector<BuildWorker> build_workers(workers);
          for (BuildWorker& local : build_workers) {
            local.int_groups.set_use_flat(use_flat_groups);
          }
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
                        if (!column_compares_pass(row)) {
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
                          StatusOr<Value> result = TryEvaluateRowAuto(
                              *shared_filter.residual_bytecode, row);
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
                          int64_t key = 0;
                          if (integer_key) {
                            key =
                                IntegerJoinKey(row, created->local_columns[0]);
                          } else if (!try_pack_two_int_key(row, &key)) {
                            local.deferred.push_back(row);
                            continue;
                          }
                          auto [group, inserted] =
                              local.int_groups.TryEmplace(key);
                          if (inserted) {
                            group->accumulators.reserve(
                                aggregate_expressions.size());
                            for (const AggregateExpression* aggregate :
                                 aggregate_expressions) {
                              group->accumulators.emplace_back(aggregate,
                                                               &source.schema);
                            }
                          }
                          for (size_t i = 0; i < aggregate_expressions.size();
                               ++i) {
                            if (!worker_safe[i]) {
                              continue;
                            }
                            if (IsCountStar(*aggregate_expressions[i])) {
                              if (Status st =
                                      group->accumulators[i].Add(Value(1));
                                  st != Status::kSuccess) {
                                build_errors[w] = st;
                                return;
                              }
                              continue;
                            }
                            if (Status st = group->accumulators[i].Add(
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
            local.int_groups.ForEach([&](int64_t key, GroupAggs& group) {
              auto [dst, inserted] = int_groups.TryEmplace(key);
              if (inserted) {
                dst->accumulators = std::move(group.accumulators);
                return;
              }
              for (size_t i = 0; i < dst->accumulators.size(); ++i) {
                if (group.accumulators[i].count == 0) {
                  continue;
                }
                if (Status st =
                        dst->accumulators[i].Merge(group.accumulators[i]);
                    st != Status::kSuccess) {
                  group_error = st;
                }
              }
            });
            if (group_error != Status::kSuccess) {
              return std::nullopt;
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
      // Projection names are group-invariant; building them per group showed
      // up as a multi-million-string cost on Q18/Q21-sized builds.
      std::vector<std::string> emit_names(statement.SelectList().size());
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        emit_names[i] = ProjectionName(statement.SelectList()[i], i);
      }
      const bool can_use_preagg_int_scalars =
          scalar_answer != nullptr && !exists_only &&
          statement.SelectList().size() == 1 && !statement.AsStruct() &&
          !statement.HasLimit() && statement.Offset() == 0 &&
          created->cache_outer_columns.size() ==
              created->outer_columns.size() &&
          (integer_key || (packed_two_int_key && str_groups.empty()));
      const Expression& select0_expr =
          !statement.SelectList().empty() ? statement.SelectList()[0].expression
                                          : Expression{};
      const bool fast_bare_agg = statement.Having() == nullptr &&
                                 aggregate_expressions.size() == 1 &&
                                 select0_expr != nullptr &&
                                 select0_expr.get() == aggregate_expressions[0];
      bool fast_const_op_agg = false;
      bool fast_const_on_left = false;
      BinaryOperation fast_agg_bin_op = BinaryOperation::kMultiply;
      double fast_agg_const = 0.0;
      if (!fast_bare_agg && statement.Having() == nullptr &&
          aggregate_expressions.size() == 1 && select0_expr != nullptr &&
          select0_expr->Type() == TypeTag::kBinaryExp) {
        const BinaryExpression& bin = select0_expr->AsBinaryExpression();
        if (bin.Op() == BinaryOperation::kMultiply ||
            bin.Op() == BinaryOperation::kAdd ||
            bin.Op() == BinaryOperation::kSubtract) {
          const Expression& lhs = bin.Left();
          const Expression& rhs = bin.Right();
          const Value* cval = nullptr;
          if (lhs && rhs && lhs->Type() == TypeTag::kConstantValue &&
              rhs.get() == aggregate_expressions[0]) {
            cval = &lhs->AsConstantValue().GetValue();
            fast_const_on_left = true;
          } else if (lhs && rhs && rhs->Type() == TypeTag::kConstantValue &&
                     lhs.get() == aggregate_expressions[0]) {
            cval = &rhs->AsConstantValue().GetValue();
            fast_const_on_left = false;
          }
          if (cval != nullptr && !cval->IsNull() && !cval->IsUnsigned() &&
              (cval->type == ValueType::kDouble ||
               cval->type == ValueType::kInt64)) {
            fast_agg_const = cval->type == ValueType::kDouble
                                 ? cval->value.double_value
                                 : static_cast<double>(cval->value.int_value);
            if (std::isfinite(fast_agg_const)) {
              fast_const_op_agg = true;
              fast_agg_bin_op = bin.Op();
            }
          }
        }
      }
      auto eval_group_scalar = [&](GroupAggs& group, bool* kept,
                                   Value* out_val) {
        *kept = false;
        if (fast_bare_agg && group.accumulators.size() == 1) {
          StatusOr<Value> finished = group.accumulators[0].TryFinish();
          if (!finished.HasValue()) {
            group_error = finished.GetStatus();
            return;
          }
          *out_val = finished.MoveValue();
          *kept = true;
          return;
        }
        if (fast_const_op_agg && group.accumulators.size() == 1) {
          StatusOr<Value> finished = group.accumulators[0].TryFinish();
          if (!finished.HasValue()) {
            group_error = finished.GetStatus();
            return;
          }
          const Value& fv = finished.Value();
          if (fv.IsNull()) {
            *out_val = Value();
            *kept = true;
            return;
          }
          if (fv.type == ValueType::kDouble &&
              std::isfinite(fv.value.double_value)) {
            const double a =
                fast_const_on_left ? fast_agg_const : fv.value.double_value;
            const double b =
                fast_const_on_left ? fv.value.double_value : fast_agg_const;
            double res = 0.0;
            if (fast_agg_bin_op == BinaryOperation::kMultiply) {
              res = a * b;
            } else if (fast_agg_bin_op == BinaryOperation::kAdd) {
              res = a + b;
            } else {
              res = a - b;
            }
            if (std::isfinite(res)) {
              *out_val = Value(res);
              *kept = true;
              return;
            }
          }
        }
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
        if (group_error != Status::kSuccess) {
          return;
        }
        StatusOr<Value> v =
            TryEvaluate(statement.SelectList()[0].expression, scope,
                        &aggregate_results, context, ctes);
        if (!v.HasValue()) {
          group_error = v.GetStatus();
          return;
        }
        *out_val = v.MoveValue();
        *kept = true;
      };
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
          columns.emplace_back(emit_names[i], ValueTypeOf(values[i]));
          columns.back().SetUnsigned(values[i].IsUnsigned());
        }
        Relation finished;
        finished.schema = Schema("", std::move(columns));
        finished.AddRow(Row(std::move(values)));
        created->cached_results.emplace(
            key, std::make_shared<Relation>(std::move(finished)));
      };
      if (can_use_preagg_int_scalars) {
        GroupAggs empty_group;
        empty_group.accumulators.reserve(aggregate_expressions.size());
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          empty_group.accumulators.emplace_back(aggregate, &source.schema);
        }
        bool empty_kept = false;
        Value empty_val;
        eval_group_scalar(empty_group, &empty_kept, &empty_val);
        if (group_error == Status::kSuccess) {
          created->has_preagg_int_scalars = true;
          created->preagg_two_int_key = packed_two_int_key;
          created->preagg_empty_has_row = empty_kept;
          created->preagg_empty_scalar =
              empty_kept ? std::move(empty_val) : Value();
          size_t group_cnt = 0;
          int_groups.ForEach([&](int64_t, GroupAggs&) { ++group_cnt; });
          created->preagg_int_scalars.Reserve(group_cnt + 16);
          int_groups.ForEach([&](int64_t key, GroupAggs& group) {
            if (group_error != Status::kSuccess) {
              return;
            }
            bool kept = false;
            Value val;
            eval_group_scalar(group, &kept, &val);
            if (group_error == Status::kSuccess && kept) {
              *created->preagg_int_scalars.TryEmplace(key) = std::move(val);
            }
          });
        }
      } else if (integer_key) {
        int_groups.ForEach([&](int64_t key, GroupAggs& group) {
          emit_group(integer_key_value(key).EncodeMemcomparableFormat(), group);
        });
      } else if (packed_two_int_key) {
        int_groups.ForEach([&](int64_t key, GroupAggs& group) {
          emit_group(packed_two_int_key_encoded(key), group);
        });
        for (auto& [key, group] : str_groups) {
          emit_group(key, group);
        }
      } else {
        for (auto& [key, group] : str_groups) {
          emit_group(key, group);
        }
      }
      if (group_error != Status::kSuccess) {
        return std::nullopt;
      }
    } else if (created->exists_residual_fast_path) {
      // Sibling build sharing (TPC-H Q21: an EXISTS/NOT EXISTS pair over the
      // same table with the same correlation column).  Published builds
      // carry no build-time predicates, so a consumer defers its build-side
      // conjuncts into exists_residual_predicates and probes the shared
      // rows instead of re-scanning the base table.  Domain-filtered
      // entries are only shared under the same registration context.
      static const bool sharing_disabled =
          std::getenv("TINYLAMB_DISABLE_SHARED_EXISTS_BUILD") != nullptr;
      const bool shareable = !sharing_disabled && !source.HasSpill() &&
                             statement.Sources().size() == 1 && !from.query &&
                             !from.table.empty() &&
                             created->local_columns.size() == 1;
      std::string share_key;
      if (shareable) {
        // Sibling statements qualify the same physical columns under their
        // own aliases (l2.ord vs l3.ord), so the key uses the bare column
        // name sequence: equal sequences mean the loaded rows are
        // positionally compatible even though the qualifier prefixes
        // differ.  The consumer keeps its own (alias-qualified) schema for
        // residual evaluation and borrows only the row storage.
        share_key = from.table;
        for (size_t i = 0; i < source.schema.ColumnCount(); ++i) {
          share_key.push_back('\x1f');
          share_key += std::string(source.schema.GetColumn(i).Name().name);
        }
      }
      const auto shared =
          shareable ? context.execution_runtime()->shared_exists_builds.find(
                          share_key)
                    : context.execution_runtime()->shared_exists_builds.end();
      const bool same_domain =
          shared != context.execution_runtime()->shared_exists_builds.end() &&
          (shared->second.outer_filter_relation == nullptr ||
           (shared->second.outer_filter_relation ==
                context.execution_runtime()->correlated_outer_relation &&
            shared->second.outer_filter_serial ==
                context.execution_runtime()->correlated_outer_serial));
      if (same_domain) {
        created->shared_int_rows = shared->second.rows;
        created->outer_filter_relation = shared->second.outer_filter_relation;
        created->outer_filter_serial = shared->second.outer_filter_serial;
        // The published build applies no predicates; this statement's full
        // residual conjunction (inner-local and outer-referencing alike)
        // evaluates per candidate row at probe time.
        created->exists_residual_predicates = local_predicates;
        created->exists_residual_predicates.insert(
            created->exists_residual_predicates.end(),
            applied_scan_predicates.begin(), applied_scan_predicates.end());
        ++context.execution_runtime()->shared_exists_build_reuses;
      } else {
        const CompiledScanFilter local_filter =
            CompileScanFilter(build_predicates, source.schema);
        created->int_rows.reserve(source.TotalRows());
        const Status source_status = source.ForEachRow([&](const Row& row) {
          if (group_error != Status::kSuccess ||
              HasNullKey(row, created->local_columns)) {
            return;
          }
          if (!build_predicates.empty() &&
              !MatchScanFilter(row, source.schema, local_filter, nullptr,
                               context, ctes, &group_error)) {
            return;
          }
          created->int_rows.emplace_back(
              IntegerJoinKey(row, created->local_columns.front()), row);
        });
        if (source_status != Status::kSuccess ||
            group_error != Status::kSuccess) {
          return std::nullopt;
        }
        std::sort(created->int_rows.begin(), created->int_rows.end(),
                  [](const auto& left, const auto& right) {
                    return left.first < right.first;
                  });
        if (shareable && build_predicates.empty() &&
            applied_scan_predicates.empty()) {
          // Only builds without build-time predicates may be published: a
          // consumer of this entry probes every loaded row under its own
          // residual conjunction.
          auto rows =
              std::make_shared<const std::vector<std::pair<int64_t, Row>>>(
                  std::move(created->int_rows));
          created->shared_int_rows = rows;
          context.execution_runtime()->shared_exists_builds.emplace(
              std::move(share_key),
              ExecutionRuntime::SharedExistsBuild{
                  .schema = source.schema,
                  .rows = std::move(rows),
                  .outer_filter_relation = domain_relation,
                  .outer_filter_serial = domain_serial});
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
  if (index->has_preagg_int_scalars && scalar_answer != nullptr) {
    const size_t nkeys = index->preagg_two_int_key ? 2U : 1U;
    bool use_cached_slots = false;
    if (outer.outer == nullptr && outer.row != nullptr &&
        outer.schema != nullptr) {
      if (index->cached_outer_schema != outer.schema) {
        index->cached_outer_schema = outer.schema;
        index->cached_outer_slots_valid = true;
        for (size_t i = 0; i < nkeys; ++i) {
          const auto off =
              LocalColumnOffset(*outer.schema, index->outer_columns[i]);
          if (!off.has_value() || *off >= outer.schema->ColumnCount()) {
            index->cached_outer_slots_valid = false;
            break;
          }
          const Column& oc = outer.schema->GetColumn(*off);
          const Column& lc = index->schema.GetColumn(index->local_columns[i]);
          if (oc.Type() != lc.Type() || oc.IsUnsigned() != lc.IsUnsigned()) {
            index->cached_outer_slots_valid = false;
            break;
          }
          index->cached_outer_slots[i] = static_cast<slot_t>(*off);
        }
      }
      use_cached_slots = index->cached_outer_slots_valid;
    }
    if (!index->preagg_two_int_key) {
      const Value* v0_ptr = nullptr;
      StatusOr<Value> ov(Status::kSuccess);
      if (use_cached_slots &&
          index->cached_outer_slots[0] < outer.row->Count()) {
        v0_ptr = &(*outer.row)[index->cached_outer_slots[0]];
      } else {
        ov = TryLookup(index->outer_columns[0], outer);
        if (!ov.HasValue()) {
          return std::nullopt;
        }
        v0_ptr = &ov.Value();
      }
      const Value& v0 = *v0_ptr;
      if (v0.IsNull()) {
        index->null_parameter_seen = true;
        *scalar_answer = index->preagg_empty_scalar;
        return Relation{};
      }
      if (!use_cached_slots) {
        const Column& c0 = index->schema.GetColumn(index->local_columns[0]);
        const bool ok0 =
            (c0.Type() == ValueType::kDate && v0.type == ValueType::kDate) ||
            (c0.Type() == ValueType::kInt64 && v0.type == ValueType::kInt64 &&
             c0.IsUnsigned() == v0.IsUnsigned());
        if (!ok0) {
          return std::nullopt;
        }
      }
      const int64_t k0 = v0.value.int_value;
      bool first_probe = false;
      index->preagg_seen_keys.TryEmplace(k0, &first_probe);
      if (first_probe) {
        ++index->preagg_distinct_keys;
      }
      if (const Value* hit = index->preagg_int_scalars.Find(k0);
          hit != nullptr) {
        ++context.execution_runtime()->correlated_result_cache_hits;
        *scalar_answer = *hit;
      } else {
        *scalar_answer = index->preagg_empty_scalar;
      }
      return Relation{};
    }
    const Value* v0_ptr = nullptr;
    const Value* v1_ptr = nullptr;
    StatusOr<Value> ov0(Status::kSuccess);
    StatusOr<Value> ov1(Status::kSuccess);
    if (use_cached_slots && index->cached_outer_slots[0] < outer.row->Count() &&
        index->cached_outer_slots[1] < outer.row->Count()) {
      v0_ptr = &(*outer.row)[index->cached_outer_slots[0]];
      v1_ptr = &(*outer.row)[index->cached_outer_slots[1]];
    } else {
      ov0 = TryLookup(index->outer_columns[0], outer);
      ov1 = TryLookup(index->outer_columns[1], outer);
      if (!ov0.HasValue() || !ov1.HasValue()) {
        return std::nullopt;
      }
      v0_ptr = &ov0.Value();
      v1_ptr = &ov1.Value();
    }
    const Value& v0 = *v0_ptr;
    const Value& v1 = *v1_ptr;
    if (!use_cached_slots) {
      for (size_t i = 0; i < 2; ++i) {
        const Value& vi = i == 0 ? v0 : v1;
        if (vi.IsNull()) {
          continue;
        }
        const Column& ci = index->schema.GetColumn(index->local_columns[i]);
        const bool oki =
            (ci.Type() == ValueType::kDate && vi.type == ValueType::kDate) ||
            (ci.Type() == ValueType::kInt64 && vi.type == ValueType::kInt64 &&
             ci.IsUnsigned() == vi.IsUnsigned());
        if (!oki) {
          return std::nullopt;
        }
      }
    }
    if (v0.IsNull() || v1.IsNull()) {
      index->null_parameter_seen = true;
      *scalar_answer = index->preagg_empty_scalar;
      return Relation{};
    }
    const int64_t k0 = v0.value.int_value;
    const int64_t k1 = v1.value.int_value;
    if (k0 >= std::numeric_limits<int32_t>::min() &&
        k0 <= std::numeric_limits<int32_t>::max() &&
        k1 >= std::numeric_limits<int32_t>::min() &&
        k1 <= std::numeric_limits<int32_t>::max()) {
      const uint64_t u = (static_cast<uint64_t>(
                              static_cast<uint32_t>(static_cast<int32_t>(k0)))
                          << 32) |
                         static_cast<uint64_t>(
                             static_cast<uint32_t>(static_cast<int32_t>(k1)));
      const auto packed = static_cast<int64_t>(u);
      bool first_probe = false;
      index->preagg_seen_keys.TryEmplace(packed, &first_probe);
      if (first_probe) {
        ++index->preagg_distinct_keys;
      }
      if (const Value* hit = index->preagg_int_scalars.Find(packed);
          hit != nullptr) {
        ++context.execution_runtime()->correlated_result_cache_hits;
        *scalar_answer = *hit;
      } else {
        *scalar_answer = index->preagg_empty_scalar;
      }
      return Relation{};
    }
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
      index->null_parameter_seen = true;
      if (exists_only && (exists_answer != nullptr)) {
        *exists_answer = false;
        return Relation{};
      }
      Relation empty;
      empty.schema = index->schema;
      return empty;
    }
    if (probe.type != ValueType::kInt64 && probe.type != ValueType::kDate) {
      // Type-promoted equality is the generic path's decision, not the
      // bit-equality key set's.
      return std::nullopt;
    }
    index->parameter_int_keys_seen.insert(probe.value.int_value);
    const bool found = index->exists_keys.contains(probe.value.int_value);
    if (exists_only && (exists_answer != nullptr)) {
      *exists_answer = found;
      return Relation{};
    }
    if (!found) {
      Relation empty;
      empty.schema = index->schema;
      return empty;
    }
    Relation one;
    one.schema = index->schema;
    one.AddRow(Row(std::vector<Value>(index->schema.ColumnCount())));
    return one;
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

  if (index->exists_residual_fast_path) {
    StatusOr<Value> outer_value =
        TryLookup(index->outer_columns.front(), outer);
    if (!outer_value.HasValue()) {
      return std::nullopt;
    }
    const Value& probe = outer_value.Value();
    if (probe.IsNull()) {
      Relation empty;
      empty.schema = index->schema;
      return empty;
    }
    if (probe.type != ValueType::kInt64 && probe.type != ValueType::kDate) {
      return std::nullopt;
    }
    const auto& exist_rows = index->ExistRows();
    const auto begin = std::lower_bound(
        exist_rows.begin(), exist_rows.end(), probe.value.int_value,
        [](const auto& entry, int64_t key) { return entry.first < key; });
    const auto end = std::upper_bound(
        begin, exist_rows.end(), probe.value.int_value,
        [](int64_t key, const auto& entry) { return key < entry.first; });
    bool matched = false;
    Status residual_error{Status::kSuccess};
    for (auto iter = begin; iter != end; ++iter) {
      const Scope scope{
          .row = &iter->second, .schema = &index->schema, .outer = &outer};
      bool rejected = false;
      for (const Expression& predicate : index->exists_residual_predicates) {
        StatusOr<Value> value =
            TryEvaluate(predicate, scope, nullptr, context, ctes);
        if (!value.HasValue()) {
          if (residual_error.ok()) {
            residual_error = value.GetStatus();
          }
          continue;
        }
        if (value.Value().IsNull() || !value.Value().Truthy()) {
          rejected = true;
          break;
        }
      }
      if (rejected) {
        continue;
      }
      if (residual_error != Status::kSuccess) {
        return std::nullopt;
      }
      matched = true;
      break;
    }
    if (residual_error != Status::kSuccess) {
      return std::nullopt;
    }
    const auto make_result = [&](bool present) {
      Relation result(context.execution_runtime());
      result.schema = index->schema;
      if (present) {
        result.AddRow(Row(std::vector<Value>(index->schema.ColumnCount())));
      }
      return result;
    };
    if (cache_key.has_value()) {
      auto cached_relation =
          std::make_shared<Relation>(context.execution_runtime());
      cached_relation->schema = index->schema;
      if (matched) {
        cached_relation->AddRow(
            Row(std::vector<Value>(index->schema.ColumnCount())));
      }
      index->cached_results.emplace(*cache_key, std::move(cached_relation));
    }
    return make_result(matched);
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
             std::ranges::any_of(
                 aggregate.TrailingArgs(),
                 [&](const auto& arg) { return self(arg, self); }) ||
             std::ranges::any_of(aggregate.InnerOrderBy(),
                                 [&](const auto& term) {
                                   return self(term.expression, self);
                                 }) ||
             std::ranges::any_of(
                 aggregate.InnerGroupBy(),
                 [&](const auto& key) { return self(key, self); });
    }
    if (expression->Type() == TypeTag::kWindowFunctionExp) {
      const auto& window = expression->AsWindowFunctionCallExpression();
      return std::ranges::any_of(
                 window.args,
                 [&](const auto& arg) { return self(arg, self); }) ||
             self(window.where_filter, self) ||
             std::ranges::any_of(
                 window.partition_by,
                 [&](const auto& key) { return self(key, self); }) ||
             std::ranges::any_of(window.order_by,
                                 [&](const auto& term) {
                                   return self(term.expression, self);
                                 }) ||
             std::ranges::any_of(window.inner_order_by,
                                 [&](const auto& term) {
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

bool StatementContainsVolatileFunctionCached(ExecutionRuntime* runtime,
                                             const SelectStatement& statement) {
  if (runtime == nullptr) {
    return StatementContainsVolatileFunction(statement);
  }
  const uint64_t id = statement.RuntimeId();
  auto [it, inserted] =
      runtime->volatile_memo.try_emplace(&statement, id, false);
  if (inserted || it->second.first != id) {
    it->second = {id, StatementContainsVolatileFunction(statement)};
  }
  return it->second.second;
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
      StatementContainsVolatileFunctionCached(context.execution_runtime(),
                                              statement)) {
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
  // Fast path first: a current per-node entry answers the overwhelmingly
  // common repeat probe (TPC-H Q22 re-probes its uncorrelated subqueries
  // once per outer row, 61128 times) with one pointer-keyed find. The
  // structural fingerprint lookup below hashes a multi-hundred-byte key
  // string and only needs to run when the node entry is missing.
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
  const uint64_t stmt_id = statement.RuntimeId();
  auto key_memo =
      context.execution_runtime()->uncorrelated_key_memo.find(&statement);
  if (key_memo != context.execution_runtime()->uncorrelated_key_memo.end() &&
      key_memo->second.first != stmt_id) {
    key_memo->second = {stmt_id,
                        UncorrelatedCacheKey(statement, ctes, exists_only)};
  } else if (key_memo ==
             context.execution_runtime()->uncorrelated_key_memo.end()) {
    key_memo =
        context.execution_runtime()
            ->uncorrelated_key_memo
            .emplace(&statement,
                     std::make_pair(
                         stmt_id,
                         UncorrelatedCacheKey(statement, ctes, exists_only)))
            .first;
  }
  const std::string& structural_key = key_memo->second.second;
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
  if (capped) {
    context.execution_runtime()->retained_statements.push_back(capped);
  }
  if (capped_by_limit) {
    ++context.execution_runtime()->exists_short_circuit_queries;
  }
  StatusOr<Relation> executed =
      ExecuteQuery(context, *execution_statement, nullptr, ctes);
  if (!executed.HasValue()) {
    return nullptr;
  }
  Relation result = executed.MoveValue();
  auto [iter, inserted] =
      context.execution_runtime()->uncorrelated_results.emplace(
          &statement, std::make_shared<Relation>(std::move(result)));
  context.execution_runtime()->uncorrelated_results_by_fingerprint.emplace(
      structural_key, iter->second);
  return iter->second.get();
}

void MaterializeArenaRange(const BaseRelationArena& arena, const Schema& schema,
                           size_t begin, size_t end, std::vector<Row>* rows) {
  const size_t stride = arena.stride;
  for (size_t index = begin; index < end; ++index) {
    const int64_t* cells = arena.cells.data() + (index * stride);
    Row& row = (*rows)[index];
    row.values_.resize(stride);
    for (size_t c = 0; c < stride; ++c) {
      if (arena.IsNull(index, c)) {
        row.values_[c] = Value();
        continue;
      }
      const ValueType type = arena.slot_types.size() == stride
                                 ? arena.slot_types[c]
                                 : schema.GetColumn(c).Type();
      if (type == ValueType::kVarChar) {
        row.values_[c] = Value(std::string(arena.Varchar(index, c)));
      } else if (type == ValueType::kDouble) {
        Value value;
        value.type = ValueType::kDouble;
        value.value.double_value = std::bit_cast<double>(cells[c]);
        row.values_[c] = value;
      } else {
        Value value;
        value.type = type;
        value.value.int_value = cells[c];
        if (type == ValueType::kInt64 && schema.ColumnCount() > c &&
            schema.GetColumn(c).IsUnsigned()) {
          value.SetUnsigned();
        }
        row.values_[c] = value;
      }
    }
  }
}

void MaterializeCacheRowsIfNeeded(const std::string& cache_key,
                                  Relation* cached, ExecutionRuntime* runtime) {
  if (cached == nullptr || runtime == nullptr || !cached->rows.empty()) {
    return;
  }
  const auto arena = runtime->base_relation_arenas.find(cache_key);
  if (arena == runtime->base_relation_arenas.end() ||
      arena->second->rows == 0 || !arena->second->published_rowsless ||
      arena->second->rows_boxed) {
    return;
  }
  cached->rows.resize(arena->second->rows);
  MaterializeArenaRange(*arena->second, cached->schema, 0, arena->second->rows,
                        &cached->rows);
  arena->second->rows_boxed = true;
}

void MaterializeCacheRowsParallel(const std::string& cache_key,
                                  Relation* cached, ExecutionRuntime* runtime) {
  if (cached == nullptr || runtime == nullptr || !cached->rows.empty()) {
    return;
  }
  const auto arena = runtime->base_relation_arenas.find(cache_key);
  if (arena == runtime->base_relation_arenas.end() ||
      arena->second->rows == 0 || !arena->second->published_rowsless ||
      arena->second->rows_boxed) {
    return;
  }
  BaseRelationArena& a = *arena->second;
  cached->rows.resize(a.rows);
  const size_t workers =
      std::clamp<size_t>(std::thread::hardware_concurrency(), 1, 16);
  std::vector<std::jthread> threads;
  for (size_t w = 1; w < workers; ++w) {
    threads.emplace_back([&, w] {
      const size_t begin = a.rows * w / workers;
      const size_t end = a.rows * (w + 1) / workers;
      MaterializeArenaRange(a, cached->schema, begin, end, &cached->rows);
    });
  }
  MaterializeArenaRange(a, cached->schema, 0, a.rows / workers, &cached->rows);
  threads.clear();
  a.rows_boxed = true;
}

namespace {

void CollectExpressionTableRefs(const Expression& expression,
                                const std::unordered_set<std::string>& shadowed,
                                std::unordered_set<std::string>* names) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    if (query.Query() != nullptr) {
      CollectStatementTableRefs(*query.Query(), shadowed, names);
    }
    CollectExpressionTableRefs(query.Test(), shadowed, names);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectExpressionTableRefs(child, shadowed, names);
  }
}

}  // namespace

void CollectStatementTableRefs(const SelectStatement& statement,
                               const std::unordered_set<std::string>& shadowed,
                               std::unordered_set<std::string>* names) {
  std::unordered_set<std::string> local = shadowed;
  for (const auto& [cte_name, body] : statement.WithQueries()) {
    (void)body;
    local.insert(cte_name);
  }
  for (const SelectSource& source : statement.Sources()) {
    if (!source.table.empty() && !local.contains(source.table)) {
      names->insert(source.table);
    }
    if (source.query != nullptr) {
      CollectStatementTableRefs(*source.query, local, names);
    }
    CollectExpressionTableRefs(source.join_condition, local, names);
    CollectExpressionTableRefs(source.unnest, local, names);
  }
  CollectExpressionTableRefs(statement.WhereClause(), local, names);
  for (const NamedExpression& item : statement.SelectList()) {
    CollectExpressionTableRefs(item.expression, local, names);
  }
  for (const Expression& expression : statement.GroupBy()) {
    CollectExpressionTableRefs(expression, local, names);
  }
  CollectExpressionTableRefs(statement.Having(), local, names);
  CollectExpressionTableRefs(statement.Qualify(), local, names);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectExpressionTableRefs(term.expression, local, names);
  }
  for (const auto& branch : statement.UnionAll()) {
    if (branch != nullptr) {
      CollectStatementTableRefs(*branch, local, names);
    }
  }
  if (const auto& tree = statement.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      CollectStatementTableRefs(*tree->first, local, names);
    }
    for (const auto& branch : tree->branches) {
      if (branch != nullptr) {
        CollectStatementTableRefs(*branch, local, names);
      }
    }
  }
  for (const auto& [cte_name, body] : statement.WithQueries()) {
    if (body == nullptr) {
      continue;
    }
    std::unordered_set<std::string> body_scope = local;
    if (!statement.IsRecursiveWith(cte_name) && !shadowed.contains(cte_name)) {
      // A non-recursive entry is not visible inside its own definition, so a
      // self-named reference there resolves to the enclosing scope (which may
      // be an outer CTE) instead of the entry itself.
      body_scope.erase(cte_name);
    }
    CollectStatementTableRefs(*body, body_scope, names);
  }
}

void CollectCteBodyOuterRefs(const SelectStatement& statement,
                             const std::string& entry_name,
                             std::unordered_set<std::string>* names) {
  const auto body_it = statement.WithQueries().find(entry_name);
  if (body_it == statement.WithQueries().end() || body_it->second == nullptr) {
    return;
  }
  // `{}` keeps the siblings visible (they are the entries this one may wait
  // on); the body's own nested WITH names are added by the scan itself.
  CollectStatementTableRefs(*body_it->second, {}, names);
  names->erase(entry_name);
}

Status CteCycleStatus(std::vector<std::string> names) {
  std::sort(names.begin(), names.end());
  std::string listed;
  for (const std::string& name : names) {
    if (!listed.empty()) {
      listed += ", ";
    }
    listed += name;
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "cyclic CTE reference among " + listed +
                         "; mutual references have no dependency order");
}

Status ValidateCteDependencies(const SelectStatement& statement) {
  if (statement.WithQueries().empty()) {
    return Status::kSuccess;
  }
  std::unordered_set<std::string> pending;
  for (const auto& [name, body] : statement.WithQueries()) {
    (void)body;
    pending.insert(name);
  }
  while (!pending.empty()) {
    bool progress = false;
    for (auto iter = pending.begin(); iter != pending.end();) {
      const std::string& name = *iter;
      std::unordered_set<std::string> referenced;
      CollectCteBodyOuterRefs(statement, name, &referenced);
      bool ready = true;
      for (const std::string& table : referenced) {
        if (pending.contains(table)) {
          ready = false;
          break;
        }
      }
      if (ready) {
        iter = pending.erase(iter);
        progress = true;
      } else {
        ++iter;
      }
    }
    if (!progress) {
      return CteCycleStatus({pending.begin(), pending.end()});
    }
  }
  return Status::kSuccess;
}

}  // namespace tinylamb::relational_detail
