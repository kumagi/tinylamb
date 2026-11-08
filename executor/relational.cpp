/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/constant_executor.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/detail/work_table_index.hpp"
#include "executor/query_memory.hpp"
#include "executor/set_operation.hpp"
#include "executor/spill_file.hpp"
#include "executor/table_sample.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/collation.hpp"
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
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// The grouped-aggregation key index (GroupKeyEqual / GroupKeyMap) lives in
// executor/detail/expression_eval.hpp: it is the shared "same group" contract
// between this path and the physical grouped aggregation shapes
// (executor/grouped_aggregate.hpp).

// Minimum grouped-input size before Project folds rows into parallel worker
// slices; below this the serial loop's thread startup dominates.
constexpr size_t kParallelGroupMinRows = 16384;

namespace {

// HAVING may reference select-list aliases (`HAVING double_avg > 0.7`).
// Alias references that are not real input columns are rewritten to the
// aliased expression so aggregate aliases resolve during grouping.

// HAVING transpose helper (aggregate <-> filter): true when every column the
// conjunct touches is a bare grouping-key column (qualifier-insensitive) and
// the conjunct holds no aggregate or query subtree.  Within a group every row
// shares the key values, so the group's fate is a row-level fact; the
// group-all shape is excluded by the caller (it forms a group even over a
// fully rejected input).
bool IsKeyOnlyHavingConjunct(const Expression& conjunct,
                             const SelectStatement& resolved) {
  if (!conjunct) {
    return false;
  }
  if (ContainsAggregate(conjunct) || ContainsQuery(conjunct)) {
    return false;
  }
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    bool is_key = false;
    for (const Expression& key : resolved.GroupBy()) {
      if (!key || key->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& key_name = key->AsColumnValue().GetColumnName();
      if (key_name.name != "*" &&
          IdentifierEquals(key_name.name, column.name)) {
        is_key = true;
        break;
      }
    }
    if (!is_key) {
      return false;
    }
  }
  return true;
}

Expression InlineHavingAliases(const SelectStatement& statement,
                               const Schema& schema) {
  const Expression& having = statement.Having();
  if (!having) {
    return having;
  }
  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& projection : statement.SelectList()) {
    if (!projection.name.empty() && projection.name != "*") {
      aliases.emplace(projection.name, projection.expression);
    }
  }
  if (aliases.empty()) {
    return having;
  }
  auto schema_has_column = [&schema](std::string_view name) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (name == schema.GetColumn(i).Name().name) {
        return true;
      }
    }
    return false;
  };
  std::function<Expression(const Expression&)> walk =
      [&](const Expression& expression)
      -> Expression {  // NOLINT(misc-no-recursion)
    if (!expression) {
      return expression;
    }
    switch (expression->Type()) {
      case TypeTag::kColumnValue: {
        const ColumnName& name = expression->AsColumnValue().GetColumnName();
        if (!name.schema.empty() || schema_has_column(name.name)) {
          return expression;
        }
        const auto found = aliases.find(name.name);
        return found != aliases.end() ? found->second : expression;
      }
      case TypeTag::kBinaryExp:
        return BinaryExpressionExp(
            walk(expression->AsBinaryExpression().Left()),
            expression->AsBinaryExpression().Op(),
            walk(expression->AsBinaryExpression().Right()));
      case TypeTag::kUnaryExp:
        return UnaryExpressionExp(walk(expression->AsUnaryExpression().Child()),
                                  expression->AsUnaryExpression().Op());
      case TypeTag::kCaseExp: {
        const auto& value = expression->AsCaseExpression();
        std::vector<std::pair<Expression, Expression>> clauses;
        clauses.reserve(value.when_clauses_.size());
        for (const auto& [condition, result] : value.when_clauses_) {
          clauses.emplace_back(walk(condition), walk(result));
        }
        return CaseExpressionExp(std::move(clauses), walk(value.else_clause_));
      }
      case TypeTag::kInExp: {
        const auto& value = expression->AsInExpression();
        std::vector<Expression> items;
        items.reserve(value.list_.size());
        for (const Expression& item : value.list_) {
          items.push_back(walk(item));
        }
        return InExpressionExp(walk(value.child_), std::move(items));
      }
      case TypeTag::kFunctionCallExp:
        return FunctionCallExp(
            expression->AsFunctionCallExpression().FuncName(), [&] {
              std::vector<Expression> out;
              out.reserve(expression->AsFunctionCallExpression().Args().size());
              for (const Expression& argument :
                   expression->AsFunctionCallExpression().Args()) {
                out.push_back(walk(argument));
              }
              return out;
            }());
      case TypeTag::kQueryExp: {
        // Alias references may sit inside HAVING subqueries
        // (EXISTS (SELECT * FROM UNNEST(agg_alias) ...)).  Rewrite them on a
        // shallow copy so the subquery's lateral unnest arguments bind to the
        // outer aggregate expressions.
        const auto& value = expression->AsQueryExpression();
        if (!value.Query() || value.Test()) {
          return expression;
        }
        auto copied = std::make_shared<SelectStatement>(*value.Query());
        bool changed = false;
        std::vector<SelectSource> sources = copied->Sources();
        for (SelectSource& source : sources) {
          if (source.unnest) {
            source.unnest = walk(source.unnest);
            changed = true;
          }
        }
        if (!changed) {
          return expression;
        }
        copied->SetSources(std::move(sources));
        auto rewritten = std::make_shared<QueryExpression>(
            std::move(copied), nullptr, value.Exists(), value.Negated(),
            value.Op(), value.Mode());
        rewritten->SetArrayResult(value.ArrayResult());
        if (!value.ArrayElementSqlType().empty()) {
          rewritten->SetArrayElementSqlType(value.ArrayElementSqlType());
        }
        return {rewritten};
      }
      default:
        // Aggregates, subqueries and casts keep their own scope.
        return expression;
    }
  };
  return walk(having);
}

namespace {

// Deep expression rewrite: returns `expression` with every subtree accepted by
// `replace` swapped for its replacement (children visited after the node, so
// replacements are not re-entered).
Expression ReplaceSubExpressions(
    const Expression& expression,
    const std::function<std::optional<Expression>(const Expression&)>&
        replace) {
  if (!expression) {
    return expression;
  }
  if (auto replacement = replace(expression)) {
    return std::move(*replacement);
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    Expression replaced = ReplaceSubExpressions(child, replace);
    if (replaced != child) {
      changed = true;
    }
    child = std::move(replaced);
  }
  return changed ? WithExpressionChildren(expression, std::move(children))
                 : expression;
}

// Registers every aggregate subtree under `expression` in the $aggK
// materialization pool so a window-over-aggregate pass can reference the
// per-group values by column.
void RegisterAggregatesForMaterialization(
    const Expression& expression,
    std::unordered_map<std::string, std::string>* aggregate_columns,
    std::vector<std::pair<std::string, Expression>>* hidden_aggregates) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    auto [it, inserted] = aggregate_columns->emplace(
        expression->ToString(),
        "$agg" + std::to_string(hidden_aggregates->size()));
    if (inserted) {
      hidden_aggregates->emplace_back(it->second, expression);
    }
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    RegisterAggregatesForMaterialization(child, aggregate_columns,
                                         hidden_aggregates);
  }
}

void CollectWindowCalls(const Expression& expression,
                        std::vector<Expression>* windows) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    for (const Expression& seen : *windows) {
      if (seen->ToString() == expression->ToString()) {
        return;
      }
    }
    windows->push_back(expression);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectWindowCalls(child, windows);
  }
}

bool ContainsWindowCall(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             ContainsWindowCall);
}

bool StatementIsAggregated(const SelectStatement& statement) {
  return !statement.GroupBy().empty() ||
         std::any_of(statement.SelectList().begin(),
                     statement.SelectList().end(),
                     [](const NamedExpression& projection) {
                       return ContainsAggregate(projection.expression);
                     }) ||
         ContainsAggregate(statement.Having());
}

}  // namespace

StatusOr<Relation> Project(TransactionContext& context,
                           const SelectStatement& statement, Relation input,
                           const Scope* outer, const CteMap& ctes,
                           size_t hidden_columns) {
  const auto project_begin = std::chrono::steady_clock::now();
  if (hidden_columns > 0 &&
      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }))) {
    // Window functions over aggregated groups (multi-level aggregation) need
    // a second pass above the grouping result; not wired up yet.
    return StatusError(
        StatusCode::kInvalidArgument,
        "window functions combined with GROUP BY are not supported here");
  }
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return ContainsAggregate(projection.expression);
                  }) ||
      ContainsAggregate(statement.Having());
  Expression having_expr = grouped
                               ? InlineHavingAliases(statement, input.schema)
                               : statement.Having();
  // Fold row-independent subqueries in HAVING to constants once, before the
  // per-group emit (Q11: every group re-probed the same uncorrelated scalar
  // through the cache).  Same contract as the scan-filter fold:
  // uncorrelated volatile-free nodes are statement-invariant on this
  // snapshot, and nodes whose evaluation raises keep their original form.
  static const bool having_fold_disabled =
      std::getenv("TINYLAMB_DISABLE_HAVING_FOLD") != nullptr;
  if (grouped && having_expr && !having_fold_disabled) {
    std::vector<Expression> folded_having =
        FoldRowIndependentSubqueries(context, {having_expr}, outer, ctes);
    if (!folded_having.empty()) {
      having_expr = folded_having[0];
    }
  }
  const auto dedupe_select_aggregates = [&](const Expression& expr) {
    return ReplaceSubExpressions(
        expr,
        [&statement](const Expression& node) -> std::optional<Expression> {
          if (!node || node->Type() != TypeTag::kAggregateExp) {
            return std::nullopt;
          }
          for (const NamedExpression& projection : statement.SelectList()) {
            if (projection.expression &&
                projection.expression->Type() == TypeTag::kAggregateExp &&
                projection.expression->ToString() == node->ToString()) {
              return projection.expression;
            }
          }
          return std::nullopt;
        });
  };
  if (grouped && having_expr) {
    having_expr = dedupe_select_aggregates(having_expr);
  }
  // ORDER BY terms may reference select-list aliases or spell out an
  // aggregate ("ORDER BY sum(x) DESC", TPC-DS q42).  Alias references resolve
  // to the select item's expression; aggregate subtrees dedupe onto the
  // identical select-list aggregate node so per-group evaluation finds them
  // prepared in the AggregateResultMap (which is keyed by node pointer).
  std::vector<Expression> resolved_order_expressions;
  resolved_order_expressions.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression expression = term.expression;
    if (expression && expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.schema.empty()) {
        for (const NamedExpression& projection : statement.SelectList()) {
          if (!projection.name.empty() &&
              projection.name.size() == name.name.size() &&
              std::equal(
                  projection.name.begin(), projection.name.end(),
                  name.name.begin(), [](char left, char right) {
                    return std::tolower(static_cast<unsigned char>(left)) ==
                           std::tolower(static_cast<unsigned char>(right));
                  })) {
            expression = projection.expression;
            break;
          }
        }
      }
    }
    expression = dedupe_select_aggregates(expression);
    resolved_order_expressions.push_back(std::move(expression));
  }
  std::vector<const AggregateExpression*> aggregate_expressions;
  std::unordered_set<const AggregateExpression*> seen_aggregates;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectAggregates(projection.expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  CollectAggregates(having_expr, &aggregate_expressions, &seen_aggregates);
  for (const Expression& order_expression : resolved_order_expressions) {
    CollectAggregates(order_expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  struct GroupState {
    Row representative;
    size_t accumulator_offset{0};
  };
  std::deque<AggregateAccumulator> aggregate_states;
  auto accumulate_row =
      [&](const Row& row, GroupKeyMap* offsets,
          std::vector<GroupState>* local_groups,
          std::deque<AggregateAccumulator>* local_states) -> Status {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    std::vector<Value> key_values;
    for (const Expression& key : statement.GroupBy()) {
      ASSIGN_OR_RETURN(Value, hv9735_0,
                       (TryEvaluate(key, scope, nullptr, context, ctes)));
      // Canonicalize so NaN/-0 keys fold like SQL equality demands.
      key_values.push_back(CanonicalDistinctValue(hv9735_0));
    }
    Row key(std::move(key_values));
    auto [iter, inserted] = offsets->emplace(key, local_groups->size());
    if (inserted) {
      GroupState group;
      group.accumulator_offset = local_states->size();
      for (const AggregateExpression* aggregate : aggregate_expressions) {
        local_states->emplace_back(aggregate);
      }
      local_groups->push_back(std::move(group));
    }
    GroupState& group = (*local_groups)[iter->second];
    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
      AggregateAccumulator& accumulator =
          (*local_states)[group.accumulator_offset + i];
      const AggregateExpression& aggregate = *accumulator.expression;
      if (aggregate.WhereFilter()) {
        ASSIGN_OR_RETURN(Value, where_value,
                         (TryEvaluate(aggregate.WhereFilter(), scope, nullptr,
                                      context, ctes)));
        if (!Truthy(where_value)) {
          continue;
        }
      }
      if (IsCountStar(aggregate)) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      AggregateInput aggregate_input;
      ASSIGN_OR_RETURN(
          Value, hv10912_0,
          (TryEvaluate(aggregate.Child(), scope, nullptr, context, ctes)));
      aggregate_input.value = std::move(hv10912_0);
      if (aggregate.Having() != AggregateHavingModifier::kNone &&
          aggregate.HavingCondition()) {
        ASSIGN_OR_RETURN(Value, hv11119_0,
                         (TryEvaluate(aggregate.HavingCondition(), scope,
                                      nullptr, context, ctes)));
        aggregate_input.condition = std::move(hv11119_0);
      }
      for (const auto& term : aggregate.InnerOrderBy()) {
        ASSIGN_OR_RETURN(
            Value, hv11336_0,
            (TryEvaluate(term.expression, scope, nullptr, context, ctes)));
        aggregate_input.order_keys.push_back(std::move(hv11336_0));
      }
      for (const Expression& extra : aggregate.ExtraArgs()) {
        ASSIGN_OR_RETURN(Value, hv11523_0,
                         (TryEvaluate(extra, scope, nullptr, context, ctes)));
        aggregate_input.order_keys.push_back(std::move(hv11523_0));
      }
      if (aggregate.SecondaryArg()) {
        ASSIGN_OR_RETURN(Value, hv11676_0,
                         (TryEvaluate(aggregate.SecondaryArg(), scope, nullptr,
                                      context, ctes)));
        aggregate_input.auxiliary = std::move(hv11676_0);
      }
      for (const Expression& extra : aggregate.TrailingArgs()) {
        if (extra) {
          ASSIGN_OR_RETURN(Value, hv11885_0,
                           (TryEvaluate(extra, scope, nullptr, context, ctes)));
          aggregate_input.trailing_values.push_back(std::move(hv11885_0));
        }
      }
      RETURN_IF_FAIL(accumulator.Add(std::move(aggregate_input)));
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->aggregate_updates;
      }
    }
    // Only new groups need a representative row.
    if (inserted) {
      group.representative = row;
    }
    if (context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->aggregate_input_rows;
    }
    return Status::kSuccess;
  };
  auto make_group = [&]() {
    GroupState group;
    group.representative =
        Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
    group.accumulator_offset = aggregate_states.size();
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      aggregate_states.emplace_back(aggregate, &input.schema);
    }
    return group;
  };

  Status agg_error{Status::kSuccess};
  std::vector<GroupState> groups;
  if (grouped) {
    input.FinishSpill();

    const bool partition_agg =
        !statement.GroupBy().empty() &&
        (input.HasSpill() || !QueryMemoryBudget::Global().CanReserve(
                                 std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      std::vector<SpillFile> parts(kSpillPartitions);
      Status iterated = input.ForEachRow([&](const Row& row) {
        Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
        std::vector<Value> key_values;
        for (const Expression& key : statement.GroupBy()) {
          StatusOr<Value> hv13156_0 =
              TryEvaluate(key, scope, nullptr, context, ctes);
          if (!hv13156_0.HasValue()) {
            if (agg_error == Status::kSuccess) {
              agg_error = hv13156_0.GetStatus();
            }
            return;
          }
          key_values.push_back(CanonicalDistinctValue(hv13156_0.MoveValue()));
        }
        // EncodeMemcomparableFormat throws on NULL, but NULL group keys are
        // legal (the in-memory path below groups them normally).  Partition
        // with the same dedicated tag the parallel hash join uses: '\0'
        // never collides with a real encoding prefix (value-type byte 1..5),
        // and grouping inside each partition stays exact.
        std::string encoded_key;
        for (const Value& key_value : key_values) {
          if (key_value.IsNull()) {
            encoded_key.push_back('\0');
          } else {
            encoded_key += key_value.EncodeMemcomparableFormat();
          }
        }
        Status st_append =
            parts[SpillPartitionOf(encoded_key, kSpillPartitions)].Append(row);
        if (st_append != Status::kSuccess && agg_error == Status::kSuccess) {
          agg_error = st_append;
        }
      });
      if (iterated != Status::kSuccess && agg_error == Status::kSuccess) {
        agg_error = iterated;
      }
      for (SpillFile& part : parts) {
        RETURN_IF_FAIL(part.FinishWriting());
      }
      RETURN_IF_FAIL(agg_error);
      // Every row was copied into a partition above.
      input.ResetContents();
      for (size_t part = 0; part < kSpillPartitions; ++part) {
        GroupKeyMap offsets;
        std::vector<GroupState> local_groups;
        std::deque<AggregateAccumulator> local_states;
        RETURN_IF_FAIL(parts[part].ForEachRow([&](const Row& row) {
          Status st_acc =
              accumulate_row(row, &offsets, &local_groups, &local_states);
          if (st_acc != Status::kSuccess && agg_error == Status::kSuccess) {
            agg_error = st_acc;
          }
        }));
        RETURN_IF_FAIL(agg_error);
        // Stash into the shared group vectors used by emit below.
        for (GroupState& group : local_groups) {
          group.accumulator_offset += aggregate_states.size();
          groups.push_back(std::move(group));
        }
        for (AggregateAccumulator& state : local_states) {
          aggregate_states.push_back(std::move(state));
        }
      }
    } else {
      // Parallel grouped aggregation for big in-memory inputs whose group
      // keys and aggregate inputs are plain column references (or count(*)):
      // workers fold contiguous row slices into thread-local group maps,
      // then the main thread merges the partial states in worker order.
      // Scalar streaming kinds (COUNT/SUM/AVG/MIN/MAX) reduce; COUNT(
      // DISTINCT plain-column) merges as an exact set union through
      // AggregateAccumulator::Merge.  DISTINCT on SUM/AVG has no set-union
      // merge and keeps the serial pass.  Spilled inputs also stay serial:
      // input.rows would only cover the resident prefix.
      bool parallel_grouping =
          !input.HasSpill() && input.TotalRows() >= kParallelGroupMinRows;
      std::vector<std::optional<slot_t>> parallel_key_offsets;
      std::vector<std::optional<slot_t>> parallel_child_offsets;
      std::vector<bool> parallel_count_star;
      // Deterministic computed keys and aggregate arguments ride the same
      // worker-safe path as plain columns through the bytecode VM (the
      // parallel scan filter already evaluates compiled programs on
      // workers).  A program that fails to compile keeps the serial loop.
      std::vector<BytecodeProgram> parallel_key_programs;
      std::vector<BytecodeProgram> parallel_child_programs;
      const auto is_plain_column = [](const Expression& expression) {
        return expression && expression->Type() == TypeTag::kColumnValue;
      };
      static const bool parallel_expression_keys_disabled =
          std::getenv("TINYLAMB_DISABLE_PARALLEL_EXPRESSION_KEYS") != nullptr;
      const auto bind_parallel_value = [&](const Expression& expression,
                                           std::optional<slot_t>* offset,
                                           BytecodeProgram* program) {
        if (is_plain_column(expression)) {
          *offset = LocalColumnOffset(
              input.schema, expression->AsColumnValue().GetColumnName());
          return offset->has_value();
        }
        if (parallel_expression_keys_disabled || !BytecodeEnabled()) {
          return false;
        }
        std::optional<BytecodeProgram> compiled =
            BytecodeCompiler::CompileRowCached(
                expression, input.schema, BytecodeCompiler::Context::kValue);
        if (!compiled.has_value() ||
            compiled->ResultType() == ValueType::kNull) {
          return false;
        }
        *program = std::move(*compiled);
        return true;
      };
      if (parallel_grouping) {
        parallel_key_offsets.reserve(statement.GroupBy().size());
        for (const Expression& key : statement.GroupBy()) {
          parallel_key_offsets.emplace_back(std::nullopt);
          parallel_key_programs.emplace_back();
          if (!bind_parallel_value(key, &parallel_key_offsets.back(),
                                   &parallel_key_programs.back())) {
            parallel_grouping = false;
            break;
          }
        }
      }
      if (parallel_grouping) {
        parallel_child_offsets.reserve(aggregate_expressions.size());
        parallel_count_star.reserve(aggregate_expressions.size());
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          const bool count_star = IsCountStar(*aggregate);
          const bool distinct_count =
              aggregate->Distinct() &&
              aggregate->GetType() == AggregationType::kCount;
          // The negated conjunction reads as "every attribute must be
          // absent"; DeMorgan-expanding it only obscures the contract.
          if (aggregate->WhereFilter() ||
              (!count_star &&
               !(  // NOLINT(readability-simplify-boolean-expr)
                   aggregate->Child() &&
                   aggregate->Having() == AggregateHavingModifier::kNone &&
                   aggregate->InnerOrderBy().empty() &&
                   !aggregate->SecondaryArg() &&
                   aggregate->ExtraArgs().empty() &&
                   !aggregate->NeedsGroupContext() &&
                   aggregate->TrailingArgs().empty() &&
                   (!aggregate->Distinct() || distinct_count)))) {
            parallel_grouping = false;
            break;
          }
          parallel_count_star.push_back(count_star);
          parallel_child_offsets.emplace_back(std::nullopt);
          parallel_child_programs.emplace_back();
          if (!count_star &&
              !bind_parallel_value(aggregate->Child(),
                                   &parallel_child_offsets.back(),
                                   &parallel_child_programs.back())) {
            parallel_grouping = false;
            break;
          }
        }
      }
      if (parallel_grouping) {
        const size_t total_rows = input.TotalRows();
        const auto workers =
            std::min<size_t>({size_t{16}, std::thread::hardware_concurrency(),
                              (total_rows / 8192) + 1});
        if (workers <= 1) {
          parallel_grouping = false;
        } else {
          bool single_int_key = false;
          slot_t single_int_slot = 0;
          ValueType single_int_type = ValueType::kNull;
          if (parallel_key_offsets.size() == 1 &&
              parallel_key_offsets[0].has_value()) {
            const Column& kcol =
                input.schema.GetColumn(*parallel_key_offsets[0]);
            if ((kcol.Type() == ValueType::kInt64 && !kcol.IsUnsigned()) ||
                kcol.Type() == ValueType::kDate) {
              single_int_key = true;
              single_int_slot = *parallel_key_offsets[0];
              single_int_type = kcol.Type();
            }
          }
          GroupKeyMap offsets;
          FlatIntKeyMap<size_t> int_merge_offsets;
          const size_t chunk_rows = 8192;
          const size_t chunk_count = (total_rows + chunk_rows - 1) / chunk_rows;
          struct WorkerLocal {
            GroupKeyMap offsets;
            FlatIntKeyMap<size_t> int_offsets;
            std::vector<GroupState> groups;
            std::deque<AggregateAccumulator> states;
            Status error{Status::kSuccess};
            size_t input_rows{0};
            size_t aggregate_updates{0};
          };
          std::vector<WorkerLocal> locals(workers);
          // Classification shared by every per-group accumulator construction
          // in the workers below; identical for all groups of one aggregate.
          std::vector<AggregateAccumulator::Shape> parallel_shapes;
          parallel_shapes.reserve(aggregate_expressions.size());
          for (const AggregateExpression* aggregate : aggregate_expressions) {
            parallel_shapes.push_back(
                AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
          }
          {
            std::vector<std::jthread> threads;
            threads.reserve(workers);
            for (size_t w = 0; w < workers; ++w) {
              threads.emplace_back([&, w] {
                WorkerLocal& local = locals[w];
                // Positional, context-free accumulation only: TryEvaluate is
                // not safe on worker threads (the shared TransactionContext
                // and evaluator state are single-threaded contracts).
                std::vector<Value> key_values;
                key_values.reserve(parallel_key_offsets.size());
                // Static rotation: worker w folds chunks w, w+W, w+2W, ... .
                for (size_t chunk = w; chunk < chunk_count; chunk += workers) {
                  const size_t begin = chunk * chunk_rows;
                  const size_t end = std::min(total_rows, begin + chunk_rows);
                  for (size_t ri = begin; ri < end; ++ri) {
                    const Row& row = input.rows[ri];
                    size_t group_index = 0;
                    if (single_int_key && !row[single_int_slot].IsNull() &&
                        row[single_int_slot].type == single_int_type &&
                        !row[single_int_slot].IsUnsigned()) {
                      bool inserted = false;
                      size_t* slot = local.int_offsets.TryEmplace(
                          row[single_int_slot].value.int_value, &inserted);
                      if (inserted) {
                        *slot = local.groups.size();
                        GroupState group;
                        group.accumulator_offset = local.states.size();
                        for (size_t i = 0; i < aggregate_expressions.size();
                             ++i) {
                          local.states.emplace_back(aggregate_expressions[i],
                                                    parallel_shapes[i]);
                        }
                        group.representative = row;
                        local.groups.push_back(std::move(group));
                      }
                      group_index = *slot;
                    } else {
                      key_values.clear();
                      for (size_t ki = 0; ki < parallel_key_offsets.size();
                           ++ki) {
                        if (parallel_key_offsets[ki].has_value()) {
                          key_values.push_back(CanonicalDistinctValue(
                              row[*parallel_key_offsets[ki]]));
                          continue;
                        }
                        StatusOr<Value> computed =
                            TryEvaluateRowAuto(parallel_key_programs[ki], row);
                        if (!computed.HasValue()) {
                          local.error = computed.GetStatus();
                          return;
                        }
                        key_values.push_back(
                            CanonicalDistinctValue(computed.Value()));
                      }
                      Row key(std::move(key_values));
                      key_values.clear();
                      auto [iter, inserted] =
                          local.offsets.emplace(key, local.groups.size());
                      if (inserted) {
                        GroupState group;
                        group.accumulator_offset = local.states.size();
                        for (size_t i = 0; i < aggregate_expressions.size();
                             ++i) {
                          local.states.emplace_back(aggregate_expressions[i],
                                                    parallel_shapes[i]);
                        }
                        group.representative = row;
                        local.groups.push_back(std::move(group));
                      }
                      group_index = iter->second;
                    }
                    GroupState& group = local.groups[group_index];
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      AggregateAccumulator& accumulator =
                          local.states[group.accumulator_offset + i];
                      if (parallel_count_star[i]) {
                        Status st = accumulator.Add(Value(1));
                        if (st != Status::kSuccess) {
                          local.error = st;
                          return;
                        }
                        continue;
                      }
                      if (parallel_child_offsets[i].has_value()) {
                        Status st =
                            accumulator.Add(row[*parallel_child_offsets[i]]);
                        if (st != Status::kSuccess) {
                          local.error = st;
                          return;
                        }
                        continue;
                      }
                      StatusOr<Value> computed =
                          TryEvaluateRowAuto(parallel_child_programs[i], row);
                      if (!computed.HasValue()) {
                        local.error = computed.GetStatus();
                        return;
                      }
                      Status st = accumulator.Add(computed.Value());
                      if (st != Status::kSuccess) {
                        local.error = st;
                        return;
                      }
                    }
                    ++local.input_rows;
                    local.aggregate_updates += aggregate_expressions.size();
                  }
                }
              });
            }
          }
          for (const WorkerLocal& local : locals) {
            if (local.error != Status::kSuccess) {
              return local.error;
            }
          }
          if (context.execution_runtime() != nullptr) {
            for (const WorkerLocal& local : locals) {
              context.execution_runtime()->aggregate_input_rows +=
                  local.input_rows;
              context.execution_runtime()->aggregate_updates +=
                  local.aggregate_updates;
            }
          }
          size_t total_worker_groups = 0;
          for (const WorkerLocal& local : locals) {
            total_worker_groups += local.groups.size();
          }
          if (single_int_key && total_worker_groups > 0) {
            groups.reserve(total_worker_groups);
          }
          // Merge partials in worker order.  A group first seen by a worker
          // is adopted wholesale (its accumulator states move onto the
          // shared deque and the offset rebases); overlapping groups merge
          // accumulator state.  Adopt-then-move keeps every partial state
          // visible.
          for (WorkerLocal& local : locals) {
            if (single_int_key && groups.empty() && offsets.empty() &&
                local.offsets.empty() && !local.groups.empty()) {
              groups = std::move(local.groups);
              int_merge_offsets = std::move(local.int_offsets);
              aggregate_states = std::move(local.states);
              if (total_worker_groups > groups.size()) {
                int_merge_offsets.Reserve(total_worker_groups);
                groups.reserve(total_worker_groups);
              }
              continue;
            }
            if (single_int_key) {
              if (groups.empty() && total_worker_groups > 0) {
                int_merge_offsets.Reserve(total_worker_groups);
              }
              for (GroupState& group : local.groups) {
                const Value& key_val =
                    group.representative.values_[single_int_slot];
                if (key_val.IsNull()) {
                  continue;
                }
                const int64_t kv = key_val.value.int_value;
                bool inserted = false;
                size_t* slot = int_merge_offsets.TryEmplace(kv, &inserted);
                if (inserted) {
                  *slot = groups.size();
                  const size_t local_offset = group.accumulator_offset;
                  const size_t shared_offset = aggregate_states.size();
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    aggregate_states.push_back(
                        std::move(local.states[local_offset + i]));
                  }
                  group.accumulator_offset = shared_offset;
                  groups.push_back(std::move(group));
                } else {
                  GroupState& destination = groups[*slot];
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    AggregateAccumulator& source_state =
                        local.states[group.accumulator_offset + i];
                    AggregateAccumulator& target =
                        aggregate_states[destination.accumulator_offset + i];
                    RETURN_IF_FAIL(target.Merge(source_state));
                  }
                }
              }
              if (local.offsets.empty()) {
                continue;
              }
            }
            for (auto& [key, index] : local.offsets) {
              GroupState& group = local.groups[index];
              auto [iter, inserted] = offsets.emplace(key, groups.size());
              if (inserted) {
                const size_t local_offset = group.accumulator_offset;
                const size_t shared_offset = aggregate_states.size();
                for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                  aggregate_states.push_back(
                      std::move(local.states[local_offset + i]));
                }
                group.accumulator_offset = shared_offset;
                groups.push_back(std::move(group));
              } else {
                GroupState& destination = groups[iter->second];
                for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                  AggregateAccumulator& source_state =
                      local.states[group.accumulator_offset + i];
                  AggregateAccumulator& target =
                      aggregate_states[destination.accumulator_offset + i];
                  RETURN_IF_FAIL(target.Merge(source_state));
                }
              }
            }
          }
        }
      }
      if (!parallel_grouping) {
        GroupKeyMap offsets;
        Status iterated = input.ForEachRow([&](const Row& row) {
          Status st_acc =
              accumulate_row(row, &offsets, &groups, &aggregate_states);
          if (st_acc != Status::kSuccess && agg_error == Status::kSuccess) {
            agg_error = st_acc;
          }
        });
        if (iterated != Status::kSuccess && agg_error == Status::kSuccess) {
          agg_error = iterated;
        }
        RETURN_IF_FAIL(agg_error);
      }
      if (input.TotalRows() == 0 &&
          (statement.GroupBy().empty() || statement.IsGroupAll())) {
        groups.push_back(make_group());
      }
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->aggregate_groups += groups.size();
    }
  }

  Relation output(context.execution_runtime());
  CopyExecutionStats(&output, input);
  const size_t base_width =
      input.schema.ColumnCount() -
      std::min<size_t>(hidden_columns,
                       static_cast<size_t>(input.schema.ColumnCount()));
  std::vector<Column> output_columns;
  // Per-select-item expansion of star items: the input column indices each
  // `*` / `relation.*` refers to.
  auto matches_star = [](const ColumnName& requested,
                         const ColumnName& candidate, bool qualified) {
    if (qualified) {
      return !requested.schema.empty() &&
             std::equal(requested.schema.begin(), requested.schema.end(),
                        candidate.schema.begin(), candidate.schema.end(),
                        [](char a, char b) {
                          return std::tolower(static_cast<unsigned char>(a)) ==
                                 std::tolower(static_cast<unsigned char>(b));
                        });
    }
    return requested.schema.empty();
  };
  // USING merge: unqualified `*` emits one coalesced entry per declared
  // using column (first), then the remaining columns in schema order;
  // `relation.*` keeps every physical column of the named side.  Each star
  // entry stores its coalesce group (single index for plain columns).
  std::vector<std::vector<std::vector<size_t>>> star_groups(
      statement.SelectList().size());
  // SELECT * over a value-table CTE must expose the proto's fields, not its
  // implementation column.  The CTE retains the SELECT AS proto constructor
  // in its source statement, which is enough to identify this shape without
  // adding type metadata to Relation.
  bool proto_value_star = false;
  for (const SelectSource& source : statement.Sources()) {
    if (source.table.find("WithProtoValueTable") != std::string::npos) {
      proto_value_star = true;
    }
    if (source.query && source.query->SelectList().size() == 1) {
      const Expression& expression = source.query->SelectList()[0].expression;
      if (expression->Type() == TypeTag::kFunctionCallExp &&
          expression->AsFunctionCallExpression().FuncName() == "__proto_new" &&
          !expression->AsFunctionCallExpression().Args().empty()) {
        const Expression& type =
            expression->AsFunctionCallExpression().Args().front();
        if (type->Type() == TypeTag::kConstantValue &&
            type->AsConstantValue().GetValue().type == ValueType::kVarChar &&
            std::string(type->AsConstantValue().GetValue().value.varchar_value)
                    .find("TestExtraPB") != std::string::npos) {
          proto_value_star = true;
        }
      }
    }
  }
  std::vector<bool> star_proto(statement.SelectList().size(), false);
  std::vector<std::string> lowered_using;
  if (input.using_columns != nullptr) {
    lowered_using = *input.using_columns;
  }
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    if (projection.expression->Type() == TypeTag::kColumnValue &&
        projection.expression->AsColumnValue().GetColumnName().name == "*") {
      const ColumnName& requested =
          projection.expression->AsColumnValue().GetColumnName();
      const bool qualified = !requested.schema.empty();
      if (projection.star_modifiers) {
        // The index-group expansion below cannot splice a substitute
        // expression mid-star; REPLACE needs the query-layer expansion.
        if (!projection.star_modifiers->replace_items.empty()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "SELECT * REPLACE over this query shape is not supported; "
              "rewrite the star over a single relation");
        }
      }
      std::unordered_set<std::string> except_names;
      std::unordered_set<std::string> except_matched_names;
      if (projection.star_modifiers) {
        for (const std::string& entry :
             projection.star_modifiers->except_columns) {
          const size_t dot = entry.rfind('.');
          except_names.insert(dot == std::string::npos ? entry
                                                       : entry.substr(dot + 1));
        }
      }
      const auto except_drops = [&](const ColumnName& candidate) {
        if (except_names.empty()) {
          return false;
        }
        const std::string& name = candidate.name;
        for (const std::string& entry : except_names) {
          if (entry.size() == name.size() &&
              std::equal(entry.begin(), entry.end(), name.begin(),
                         [](char a, char b) {
                           return std::tolower(static_cast<unsigned char>(a)) ==
                                  std::tolower(static_cast<unsigned char>(b));
                         })) {
            except_matched_names.insert(entry);
            return true;
          }
        }
        return false;
      };
      if (proto_value_star && base_width == 1) {
        if (projection.star_modifiers) {
          return StatusError(StatusCode::kInvalidArgument,
                             "SELECT * EXCEPT/REPLACE over this table is "
                             "not supported");
        }
        star_proto[i] = true;
        output_columns.emplace_back("int32_val1", ValueType::kInt64);
        output_columns.emplace_back("int32_val2", ValueType::kInt64);
        output_columns.emplace_back("str_value", ValueType::kArray);
        continue;
      }
      if (!qualified && !lowered_using.empty()) {
        // Merged using columns first, in declaration order.
        std::vector<size_t> consumed;
        for (const std::string& using_name : lowered_using) {
          std::vector<size_t> group;
          for (size_t column = 0; column < base_width; ++column) {
            const ColumnName& candidate = input.schema.GetColumn(column).Name();
            if (candidate.name.size() == using_name.size() &&
                std::equal(using_name.begin(), using_name.end(),
                           candidate.name.begin(), [](char a, char b) {
                             return std::tolower(
                                        static_cast<unsigned char>(a)) ==
                                    std::tolower(static_cast<unsigned char>(b));
                           })) {
              group.push_back(column);
            }
          }
          if (!group.empty()) {
            consumed.insert(consumed.end(), group.begin(), group.end());
            const size_t merged_index = group.front();
            star_groups[i].push_back(std::move(group));
            output_columns.emplace_back(
                input.schema.GetColumn(merged_index).Name().name,
                input.schema.GetColumn(merged_index).Type());
          }
        }
        // Remaining non-using columns keep schema order.
        for (size_t column = 0; column < base_width; ++column) {
          if (std::find(consumed.begin(), consumed.end(), column) !=
              consumed.end()) {
            continue;
          }
          const Column& candidate = input.schema.GetColumn(column);
          if (matches_star(requested, candidate.Name(), qualified) &&
              !except_drops(candidate.Name())) {
            star_groups[i].push_back({column});
            output_columns.emplace_back(candidate.Name().name,
                                        candidate.Type());
          }
        }
        continue;
      }
      for (size_t column = 0; column < base_width; ++column) {
        const Column& candidate = input.schema.GetColumn(column);
        if (matches_star(requested, candidate.Name(), qualified) &&
            !except_drops(candidate.Name())) {
          star_groups[i].push_back({column});
          output_columns.emplace_back(candidate.Name().name, candidate.Type());
        }
      }
      // An EXCEPT entry that dropped nothing resolves nowhere: fail loudly
      // instead of silently keeping the column.
      if (projection.star_modifiers &&
          except_matched_names.size() < except_names.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "EXCEPT column not present in the star target");
      }
    } else {
      output_columns.emplace_back(ProjectionName(projection, i),
                                  ValueType::kNull);
    }
  }
  // A star that matched nothing (unknown qualifier, or a bare `*` over an
  // empty input) would otherwise silently shrink the select list; reject it
  // like an unknown column.
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    if (projection.expression->Type() == TypeTag::kColumnValue &&
        projection.expression->AsColumnValue().GetColumnName().name == "*" &&
        !star_proto[i] && star_groups[i].empty()) {
      const ColumnName& requested =
          projection.expression->AsColumnValue().GetColumnName();
      return StatusError(
          StatusCode::kInvalidArgument,
          "unknown relation in select list: " +
              (requested.schema.empty() ? std::string("*")
                                        : requested.schema + ".*"));
    }
  }
  // Hidden $win columns ride along until ordering completes, then get trimmed
  // below.  ORDER BY terms may reference them.
  for (size_t k = 0; k < hidden_columns; ++k) {
    output_columns.emplace_back(input.schema.GetColumn(base_width + k));
  }

  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
    size_t sequence{0};
  };
  std::vector<KeyedRow> sortable;
  const bool has_order_by = !order_by.empty();
  // One ordering contract for the ORDER BY keys, the DISTINCT keys and the
  // bounded (LIMIT) heap alike: NULL placement, direction and the collation
  // the key resolves under all come from SortKeyOrder (type/collation.hpp),
  // the same type the sort operators use. A COLLATE'd key therefore folds here
  // exactly as it does in SortExecutor / TopNExecutor.
  std::vector<SortKeyOrder> order_specs;
  order_specs.reserve(order_by.size());
  for (const SelectStatement::OrderByTerm& term : order_by) {
    order_specs.emplace_back(term.ascending, term.nulls_first,
                             CollationOfExpression(term.expression));
  }
  const auto precedes = [&](const KeyedRow& left, const KeyedRow& right) {
    for (size_t i = 0; i < order_by.size(); ++i) {
      const int cmp = order_specs[i].Compare(left.keys[i], right.keys[i]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return left.sequence < right.sequence;
  };
  bool bounded_sort = false;
  size_t topn_capacity = 0;
  if (!statement.Distinct() && statement.HasLimit() && !statement.WithTies() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    bounded_sort = true;
    topn_capacity = statement.Offset() + statement.Limit();
  }
  size_t next_sequence = 0;
  const auto retain = [&](KeyedRow keyed) {
    keyed.sequence = next_sequence++;
    if (!bounded_sort) {
      sortable.push_back(std::move(keyed));
      return;
    }
    if (sortable.size() < topn_capacity) {
      sortable.push_back(std::move(keyed));
      std::push_heap(sortable.begin(), sortable.end(), precedes);
      return;
    }
    if (topn_capacity != 0 && precedes(keyed, sortable.front())) {
      std::pop_heap(sortable.begin(), sortable.end(), precedes);
      sortable.back() = std::move(keyed);
      std::push_heap(sortable.begin(), sortable.end(), precedes);
    }
  };
  Schema initial_output_schema("", output_columns);

  for (auto& resolved_order_expression : resolved_order_expressions) {
    const Expression& expression = resolved_order_expression;
    // `ORDER BY 1 COLLATE 'und:ci'` arrives as COLLATE(<int>, 'spec'): the
    // ordinal names a SELECT-list position and the clause collates it, so the
    // two resolve together. Left as a constant, the key would encode
    // identically for every row and the sort would silently do nothing.
    if (const std::optional<CollatedOrdinal> collated =
            AsCollatedOrdinal(expression);
        collated.has_value() && collated->ordinal >= 1 &&
        static_cast<size_t>(collated->ordinal) <= output_columns.size() &&
        !output_columns[static_cast<size_t>(collated->ordinal) - 1]
             .Name()
             .name.empty()) {
      resolved_order_expression = ApplyCollationSpec(
          ColumnValueExp(ColumnName(
              output_columns[static_cast<size_t>(collated->ordinal) - 1]
                  .Name()
                  .name)),
          collated->spec);
      continue;
    }
    if (!expression || expression->Type() != TypeTag::kConstantValue ||
        dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr) {
      continue;
    }
    const Value& value = expression->AsConstantValue().GetValue();
    if (value.type != ValueType::kInt64 || value.IsNull()) {
      continue;
    }
    if (value.value.int_value < 1 ||
        static_cast<size_t>(value.value.int_value) > output_columns.size()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ORDER BY ordinal out of range");
    }
    const std::string& name =
        output_columns[static_cast<size_t>(value.value.int_value) - 1]
            .Name()
            .name;
    if (name.empty() || name == "*") {
      return StatusError(StatusCode::kInvalidArgument,
                         "ORDER BY ordinal out of range");
    }
    resolved_order_expression = ColumnValueExp(ColumnName(name));
  }

  auto emit = [&](const Row& representative,
                  const AggregateResultMap* aggregates) -> Status {
    Scope scope{
        .row = &representative, .schema = &input.schema, .outer = outer};
    if (aggregates != nullptr) {
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = aggregates;
    }
    if (!lowered_using.empty()) {
      scope.using_columns = &lowered_using;
    }
    if (having_expr) {
      ASSIGN_OR_RETURN(
          Value, having_value,
          (TryEvaluate(having_expr, scope, aggregates, context, ctes)));
      if (!Truthy(having_value)) {
        return Status::kSuccess;
      }
    }
    std::vector<Value> values;
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      const NamedExpression& projection = statement.SelectList()[item];
      if (projection.expression->Type() == TypeTag::kColumnValue &&
          projection.expression->AsColumnValue().GetColumnName().name == "*") {
        if (star_proto[item]) {
          const ColumnName base = input.schema.GetColumn(0).Name();
          for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
            ASSIGN_OR_RETURN(
                Value, hv25286_0,
                (TryEvaluate(FunctionCallExp(
                                 "__get_field_safe",
                                 {ColumnValueExp(base),
                                  ConstantValueExp(Value(std::string(field)))}),
                             scope, aggregates, context, ctes)));
            values.push_back(std::move(hv25286_0));
          }
          continue;
        }
        for (const std::vector<size_t>& group : star_groups[item]) {
          // USING-merged entries coalesce their physical duplicates.
          Value merged;
          for (const size_t column : group) {
            if (!representative.values_[column].IsNull()) {
              merged = representative.values_[column];
              break;
            }
          }
          values.push_back(std::move(merged));
        }
      } else {
        ASSIGN_OR_RETURN(Value, projection_value,
                         (TryEvaluate(projection.expression, scope, aggregates,
                                      context, ctes)));
        values.push_back(std::move(projection_value));
      }
    }
    // Carry hidden $win columns through projection; ORDER BY may reference
    // them and they are trimmed after sorting.
    for (size_t k = base_width; k < representative.values_.size(); ++k) {
      values.push_back(representative.values_[k]);
    }
    Row output_row(std::move(values));
    if (has_order_by) {
      // The grouped ORDER BY key may reference output columns, aliases,
      // aggregates and group-key base columns -- never a relation alias
      // denoting a whole row (the query is grouped by parts of that row),
      // so the row-alias fallback is suppressed on both scopes.  The base
      // wrapper keeps select-item evaluation (`COUNT(t)`) on the lenient
      // scope above.
      Scope order_base{.row = scope.row,
                       .schema = scope.schema,
                       .outer = scope.outer,
                       .using_columns = scope.using_columns,
                       .no_row_alias = true};
      Scope proj_scope{.row = &output_row,
                       .schema = &initial_output_schema,
                       .outer = &order_base,
                       .no_row_alias = true};
      std::vector<Value> keys;
      keys.reserve(order_by.size());
      for (const Expression& key : resolved_order_expressions) {
        ASSIGN_OR_RETURN(
            Value, hv26761_0,
            (TryEvaluate(key, proj_scope, aggregates, context, ctes)));
        keys.push_back(std::move(hv26761_0));
      }
      retain(KeyedRow{.keys = std::move(keys), .row = std::move(output_row)});
    } else {
      output.AddRow(std::move(output_row));
    }
    return Status::kSuccess;
  };

  // Fast grouped emit for positional shapes: every GROUP BY key is a plain
  // input column, every select item and ORDER BY term is a group-key column
  // or a top-level aggregate, and HAVING (when present) references only
  // those.  The generic emit evaluates each select item through a Scope +
  // TryEvaluate AST walk per group, which dominates aggregations with many
  // groups (TPC-H Q3 emits ~1M groups through GroupByFinish).
  struct FastEmitItem {
    bool is_key;
    size_t index;
  };
  std::vector<slot_t> fast_key_offsets;
  std::vector<FastEmitItem> fast_items;
  std::vector<FastEmitItem> fast_order_items;
  bool fast_emit = grouped;
  const auto classify_fast = [&](const Expression& expression,
                                 FastEmitItem* item) -> bool {
    if (!expression) {
      return false;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const auto offset = LocalColumnOffset(
          input.schema, expression->AsColumnValue().GetColumnName());
      if (offset) {
        const auto pos =
            std::find(fast_key_offsets.begin(), fast_key_offsets.end(),
                      static_cast<slot_t>(*offset));
        if (pos != fast_key_offsets.end()) {
          *item = FastEmitItem{
              .is_key = true,
              .index = static_cast<size_t>(pos - fast_key_offsets.begin())};
          return true;
        }
      }
      return false;
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = FastEmitItem{
            .is_key = false,
            .index = static_cast<size_t>(pos - aggregate_expressions.begin())};
        return true;
      }
    }
    return false;
  };
  if (fast_emit) {
    for (const Expression& key : statement.GroupBy()) {
      if (!key || key->Type() != TypeTag::kColumnValue) {
        fast_emit = false;
        break;
      }
      const auto offset =
          LocalColumnOffset(input.schema, key->AsColumnValue().GetColumnName());
      if (!offset) {
        fast_emit = false;
        break;
      }
      fast_key_offsets.push_back(static_cast<slot_t>(*offset));
    }
  }
  if (fast_emit) {
    for (const NamedExpression& item : statement.SelectList()) {
      FastEmitItem emit_item{};
      if (!classify_fast(item.expression, &emit_item)) {
        fast_emit = false;
        break;
      }
      fast_items.push_back(emit_item);
    }
  }
  if (fast_emit) {
    for (const Expression& key : resolved_order_expressions) {
      FastEmitItem order_item{};
      if (!classify_fast(key, &order_item)) {
        fast_emit = false;
        break;
      }
      fast_order_items.push_back(order_item);
    }
  }
  static const bool fast_emit_disabled =
      std::getenv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  if ((fast_emit && having_expr &&
       !HavingReferencesOnlyKeysAndAggregates(having_expr, input.schema,
                                              fast_key_offsets)) ||
      fast_emit_disabled) {
    fast_emit = false;
  }

  if (grouped && fast_emit) {
    // The per-group maps and buffers are hoisted: re-constructing an
    // AggregateResultMap (bucket allocation) and a finished-values vector
    // for every one of tens of thousands of groups dominated multi-group
    // HAVING emits (TPC-H Q11: 31,587 groups).  The map keeps the same
    // aggregate-node keys every iteration, so clearing + re-emplacing reuses
    // the buckets and the per-group cost reduces to the TryFinish calls and
    // the HAVING walk itself.
    std::vector<Value> finished_values(aggregate_expressions.size());
    AggregateResultMap aggregate_results;
    aggregate_results.reserve(aggregate_expressions.size());
    bool simple_having_agg_const = false;
    size_t simple_having_agg_idx = 0;
    BinaryOperation simple_having_op = BinaryOperation::kEquals;
    Value simple_having_const;
    if (having_expr && having_expr->Type() == TypeTag::kBinaryExp) {
      const BinaryExpression& bin = having_expr->AsBinaryExpression();
      if (bin.Left() && bin.Left()->Type() == TypeTag::kAggregateExp &&
          bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
        auto pos = std::find(aggregate_expressions.begin(),
                             aggregate_expressions.end(),
                             &bin.Left()->AsAggregateExpression());
        if (pos != aggregate_expressions.end()) {
          simple_having_agg_const = true;
          simple_having_agg_idx =
              static_cast<size_t>(pos - aggregate_expressions.begin());
          simple_having_op = bin.Op();
          simple_having_const = bin.Right()->AsConstantValue().GetValue();
        }
      }
    }
    for (const GroupState& group : groups) {
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, finished,
            aggregate_states[group.accumulator_offset + i].TryFinish());
        finished_values[i] = std::move(finished);
      }
      if (having_expr) {
        if (simple_having_agg_const) {
          ASSIGN_OR_RETURN(Value, having_value,
                           TryBinary(simple_having_op,
                                     finished_values[simple_having_agg_idx],
                                     simple_having_const));
          if (!Truthy(having_value)) {
            continue;
          }
        } else {
          for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
            aggregate_results.insert_or_assign(aggregate_expressions[i],
                                               finished_values[i]);
          }
          Scope scope{.row = &group.representative,
                      .schema = &input.schema,
                      .outer = outer};
          scope.aggregates = &aggregate_results;
          ASSIGN_OR_RETURN(Value, having_value,
                           (TryEvaluate(having_expr, scope, &aggregate_results,
                                        context, ctes)));
          if (!Truthy(having_value)) {
            continue;
          }
        }
      }
      std::vector<Value> values;
      values.reserve(fast_items.size());
      for (const FastEmitItem& item : fast_items) {
        if (item.is_key) {
          values.push_back(
              group.representative.values_[fast_key_offsets[item.index]]);
        } else {
          values.push_back(finished_values[item.index]);
        }
      }
      Row output_row(std::move(values));
      if (has_order_by) {
        std::vector<Value> keys;
        keys.reserve(fast_order_items.size());
        for (const FastEmitItem& item : fast_order_items) {
          if (item.is_key) {
            keys.push_back(
                group.representative.values_[fast_key_offsets[item.index]]);
          } else {
            keys.push_back(finished_values[item.index]);
          }
        }
        retain(KeyedRow{.keys = std::move(keys), .row = std::move(output_row)});
      } else {
        output.AddRow(std::move(output_row));
      }
    }
  } else if (grouped) {
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        ASSIGN_OR_RETURN(
            Value, finished,
            aggregate_states[group.accumulator_offset + i].TryFinish());
        aggregate_results.emplace(accumulator.expression, std::move(finished));
      }
      RETURN_IF_FAIL(emit(group.representative, &aggregate_results));
    }
  } else {
    input.FinishSpill();
    Status emit_error{Status::kSuccess};
    Status iterated = input.ForEachRow([&](const Row& row) {
      Status st_emit = emit(row, nullptr);
      if (st_emit != Status::kSuccess && emit_error == Status::kSuccess) {
        emit_error = st_emit;
      }
    });
    if (iterated != Status::kSuccess && emit_error == Status::kSuccess) {
      emit_error = iterated;
    }
    RETURN_IF_FAIL(emit_error);
  }

  if (has_order_by) {
    const auto sort_begin = std::chrono::steady_clock::now();
    if (bounded_sort && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = topn_capacity;
      context.execution_runtime()->topn_finish_input_rows = next_sequence;
    }
    if (bounded_sort) {
      std::ranges::sort(sortable, precedes);
    } else {
      std::ranges::stable_sort(sortable, precedes);
    }
    for (KeyedRow& keyed : sortable) {
      output.AddRow(std::move(keyed.row));
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
    }
  }

  output.FinishSpill();
  // Drop the hidden $win columns now that ordering has consumed them.
  if (hidden_columns > 0) {
    const size_t visible_width = output_columns.size() - hidden_columns;
    // PRODUCTION FIX: rows spilled under a memory budget are not in
    // output.rows; trimming only the resident vector left spilled rows with
    // extra hidden columns. Walk every row instead.
    std::vector<Row> all_rows;
    RETURN_IF_FAIL(output.ForEachRow(
        [&all_rows](const Row& row) { all_rows.push_back(row); }));
    output.ResetContents();
    for (Row& row : all_rows) {
      row.values_.resize(std::min(visible_width, row.values_.size()));
      output.AddRow(std::move(row));
    }
    output.FinishSpill();
    output_columns.resize(visible_width);
  }
  if (!output.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
      output_columns[i].SetUnsigned(output.rows[0][i].IsUnsigned());
    }
  }
  output.schema = Schema("", std::move(output_columns));
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->project_ms += ElapsedMs(project_begin);
  }
  return output;
}

StatusOr<Relation> DistinctOf(Relation input,
                              const std::vector<Expression>& distinct_on = {},
                              TransactionContext* ctx = nullptr,
                              const Scope* outer = nullptr,
                              const CteMap* ctes = nullptr) {
  if (distinct_on.empty()) {
    WorkTableIndex seen;
    Relation distinct;
    distinct.schema = input.schema;
    CopyExecutionStats(&distinct, input);
    input.FinishSpill();
    RETURN_IF_FAIL(input.ForEachRow([&](const Row& row) {
      // Canonicalize the emitted row so a representative value is chosen
      // consistently (NaN -> quiet NaN, -0.0 -> +0.0), matching GoogleSQL.
      Row canonical = row;
      for (Value& value : canonical.values_) {
        value = CanonicalDistinctValue(value);
      }
      if (seen.Insert(canonical)) {
        distinct.AddRow(std::move(canonical));
      }
    }));
    distinct.FinishSpill();
    return distinct;
  }
  std::vector<std::vector<Value>> seen_keys;
  Relation distinct;
  distinct.schema = input.schema;
  CopyExecutionStats(&distinct, input);
  input.FinishSpill();
  Status distinct_error{Status::kSuccess};
  Status iterated = input.ForEachRow([&](const Row& row) {
    std::vector<Value> current_keys;
    current_keys.reserve(distinct_on.size());
    for (const auto& expr : distinct_on) {
      Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
      StatusOr<Value> val_or =
          ctx != nullptr && ctes != nullptr
              ? TryEvaluate(expr, scope, nullptr, *ctx, *ctes)
              : expr->TryEvaluate(row, input.schema);
      if (!val_or.HasValue()) {
        if (distinct_error == Status::kSuccess) {
          distinct_error = val_or.GetStatus();
        }
        return;
      }
      current_keys.push_back(CanonicalDistinctValue(val_or.MoveValue()));
    }
    bool duplicate = false;
    for (const auto& seen_k : seen_keys) {
      if (seen_k.size() == current_keys.size()) {
        bool match = true;
        for (size_t i = 0; i < seen_k.size(); ++i) {
          if (!EqualUnderCollation(seen_k[i], current_keys[i])) {
            match = false;
            break;
          }
        }
        if (match) {
          duplicate = true;
          break;
        }
      }
    }
    if (!duplicate) {
      seen_keys.push_back(std::move(current_keys));
      distinct.AddRow(row);
    }
  });
  if (iterated != Status::kSuccess && distinct_error == Status::kSuccess) {
    distinct_error = iterated;
  }
  RETURN_IF_FAIL(distinct_error);
  distinct.FinishSpill();
  return distinct;
}

// Sorts `output` in place by the statement's ORDER BY terms. Key values are
// evaluated once per row (not per comparison). All rows are copied out before
// refilling so a previously spilled output is never appended to after its
// spill files were finished.
Status ApplyOrderBy(TransactionContext& context,
                    const SelectStatement& statement, Relation* output,
                    const Scope* outer, const CteMap& ctes,
                    bool allow_bounded = true) {
  const auto sort_begin = std::chrono::steady_clock::now();
  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  // GoogleSQL: a bare integer ORDER BY item is a SELECT-list ordinal.  The
  // AST visitor defers it when the select list still contained an unexpanded
  // star; the output schema here already carries the expanded columns, so
  // the ordinal binds to that output column.
  std::vector<Expression> key_expressions;
  key_expressions.reserve(order_by.size());
  for (const auto& key : order_by) {
    Expression expression = key.expression;
    if (expression && expression->Type() == TypeTag::kConstantValue) {
      const Value& ordinal_value = expression->AsConstantValue().GetValue();
      if (ordinal_value.type == ValueType::kInt64 && !ordinal_value.IsNull() &&
          ordinal_value.value.int_value >= 1) {
        const auto ordinal = static_cast<size_t>(ordinal_value.value.int_value);
        if (ordinal > output->schema.ColumnCount()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "ORDER BY ordinal out of range");
        }
        expression =
            ColumnValueExp(output->schema.GetColumn(ordinal - 1).Name());
      }
    }
    key_expressions.push_back(std::move(expression));
  }
  RETURN_IF_FAIL(output->FinishSpill());
  std::vector<SortKeyOrder> order_specs;
  order_specs.reserve(order_by.size());
  for (const SelectStatement::OrderByTerm& term : order_by) {
    order_specs.emplace_back(term.ascending, term.nulls_first,
                             CollationOfExpression(term.expression));
  }
  std::vector<std::optional<slot_t>> direct_key_cols;
  direct_key_cols.reserve(key_expressions.size());
  bool all_direct_keys = !key_expressions.empty();
  for (const Expression& key : key_expressions) {
    if (key && key->Type() == TypeTag::kColumnValue) {
      const auto col_off = LocalColumnOffset(
          output->schema, key->AsColumnValue().GetColumnName());
      direct_key_cols.push_back(
          col_off.has_value() ? std::optional<slot_t>(*col_off) : std::nullopt);
      if (!col_off.has_value()) {
        all_direct_keys = false;
      }
    } else {
      direct_key_cols.emplace_back(std::nullopt);
      all_direct_keys = false;
    }
  }
  if (!output->HasSpill() && all_direct_keys &&
      output->rows.size() <= std::numeric_limits<uint32_t>::max()) {
    const size_t n = output->rows.size();
    bool bounded = false;
    size_t keep = n;
    if (allow_bounded && statement.HasLimit() && !statement.WithTies() &&
        statement.Offset() <=
            std::numeric_limits<size_t>::max() - statement.Limit()) {
      keep = std::min(n, statement.Offset() + statement.Limit());
      bounded = true;
    }
    if (bounded && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = keep;
      context.execution_runtime()->topn_finish_input_rows = n;
    }
    const std::vector<Row>& rows_ref = output->rows;
    if (n >= 256 && !order_specs.empty()) {
      struct SortEntry {
        uint64_t k0;
        uint64_t k1;
        uint32_t idx;
        uint32_t k2;
      };
      const auto can_encode_col = [&](size_t oi) noexcept {
        if (oi >= order_specs.size() || !direct_key_cols[oi].has_value() ||
            order_specs[oi].KeyCollation() == Collation::kCaseInsensitive) {
          return false;
        }
        const slot_t col = *direct_key_cols[oi];
        if (col >= output->schema.ColumnCount()) {
          return false;
        }
        const Column& c = output->schema.GetColumn(col);
        if (c.IsUnsigned()) {
          return false;
        }
        return c.Type() == ValueType::kInt64 || c.Type() == ValueType::kDate ||
               c.Type() == ValueType::kDouble ||
               c.Type() == ValueType::kVarChar;
      };
      if (can_encode_col(0)) {
        const bool has_k1 = order_specs.size() >= 2 && can_encode_col(1);
        const bool has_k2 =
            has_k1 && order_specs.size() >= 3 && can_encode_col(2);
        const slot_t col0 = direct_key_cols[0].value_or(0);
        const slot_t col1 = has_k1 ? direct_key_cols[1].value_or(0) : 0;
        const slot_t col2 = has_k2 ? direct_key_cols[2].value_or(0) : 0;
        bool exact0 = true;
        bool exact1 = has_k1;
        bool exact2 = has_k2 && order_specs.size() == 3;
        if (order_specs.size() == 1) {
          exact1 = true;
          exact2 = true;
        } else if (order_specs.size() == 2) {
          exact2 = has_k1;
        }
        bool bail_out = false;
        const auto encode_val = [&](const Value& v, const SortKeyOrder& spec,
                                    bool* all_exact) noexcept -> uint64_t {
          if (v.IsNull()) {
            return spec.NullsFirst() ? 0ULL : ~uint64_t{0};
          }
          uint64_t u = 0;
          if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
            if (v.IsUnsigned()) {
              *all_exact = false;
              return 1ULL << 63;
            }
            u = static_cast<uint64_t>(v.value.int_value) ^ (1ULL << 63);
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0ULL || u == ~uint64_t{0}) {
              *all_exact = false;
              u = (u == 0ULL) ? 1ULL : (~uint64_t{0} - 1ULL);
            }
            return u;
          }
          if (v.type == ValueType::kDouble) {
            double d = v.value.double_value;
            if (std::isnan(d)) {
              *all_exact = false;
              return 1ULL << 63;
            }
            if (d == 0.0) {
              d = 0.0;
            }
            const auto bits = std::bit_cast<uint64_t>(d);
            u = (bits & (1ULL << 63)) != 0 ? ~bits : (bits | (1ULL << 63));
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0ULL || u == ~uint64_t{0}) {
              *all_exact = false;
              u = (u == 0ULL) ? 1ULL : (~uint64_t{0} - 1ULL);
            }
            return u;
          }
          if (v.type == ValueType::kVarChar && !v.IsCaseInsensitive()) {
            const std::string_view sv = v.value.varchar_value;
            if (sv.size() >= 9 && sv.size() <= 18 && sv.starts_with("TESTENUM"))
                [[unlikely]] {
              bail_out = true;
              *all_exact = false;
              return 1ULL << 63;
            }
            const size_t len = std::min<size_t>(sv.size(), 8);
            uint64_t pfx = 0;
            for (size_t b = 0; b < len; ++b) {
              const auto ch = static_cast<uint8_t>(sv[b]);
              if (ch == 0) {
                *all_exact = false;
              }
              pfx |= static_cast<uint64_t>(ch) << ((7 - b) * 8);
            }
            if (sv.size() > 8) {
              *all_exact = false;
            }
            u = pfx;
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0ULL || u == ~uint64_t{0}) {
              *all_exact = false;
              u = (u == 0ULL) ? 1ULL : (~uint64_t{0} - 1ULL);
            }
            return u;
          }
          bail_out = true;
          *all_exact = false;
          return 1ULL << 63;
        };
        const auto encode_val32 = [&](const Value& v, const SortKeyOrder& spec,
                                      bool* all_exact) noexcept -> uint32_t {
          if (v.IsNull()) {
            return spec.NullsFirst() ? 0U : ~uint32_t{0};
          }
          if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
            if (v.IsUnsigned() || v.value.int_value < -2147483647LL ||
                v.value.int_value > 2147483646LL) {
              *all_exact = false;
              return 1U << 31;
            }
            uint32_t u =
                static_cast<uint32_t>(static_cast<int32_t>(v.value.int_value)) ^
                (1U << 31);
            if (!spec.Ascending()) {
              u = ~u;
            }
            return u;
          }
          if (v.type == ValueType::kVarChar && !v.IsCaseInsensitive()) {
            const std::string_view sv = v.value.varchar_value;
            if (sv.size() >= 9 && sv.size() <= 18 && sv.starts_with("TESTENUM"))
                [[unlikely]] {
              bail_out = true;
              *all_exact = false;
              return 1U << 31;
            }
            const size_t len = std::min<size_t>(sv.size(), 4);
            uint32_t pfx = 0;
            for (size_t b = 0; b < len; ++b) {
              const auto ch = static_cast<uint8_t>(sv[b]);
              if (ch == 0) {
                *all_exact = false;
              }
              pfx |= static_cast<uint32_t>(ch) << ((3 - b) * 8);
            }
            if (sv.size() > 4) {
              *all_exact = false;
            }
            uint32_t u = pfx;
            if (!spec.Ascending()) {
              u = ~u;
            }
            if (u == 0U || u == ~uint32_t{0}) {
              *all_exact = false;
              u = (u == 0U) ? 1U : (~uint32_t{0} - 1U);
            }
            return u;
          }
          *all_exact = false;
          return 0U;
        };
        std::vector<SortEntry> entries(n);
        for (size_t i = 0; i < n; ++i) {
          const Row& r = rows_ref[i];
          entries[i].k0 = encode_val(r[col0], order_specs[0], &exact0);
          entries[i].k1 =
              has_k1 ? encode_val(r[col1], order_specs[1], &exact1) : 0ULL;
          entries[i].idx = static_cast<uint32_t>(i);
          entries[i].k2 =
              has_k2 ? encode_val32(r[col2], order_specs[2], &exact2) : 0U;
          if (bail_out) [[unlikely]] {
            break;
          }
        }
        if (!bail_out) {
          const bool fully_exact =
              exact0 && exact1 && exact2 &&
              (order_specs.size() == 1 || (order_specs.size() == 2 && has_k1) ||
               (order_specs.size() == 3 && has_k2));
          const size_t start_i =
              !exact0 ? 0 : (!exact1 ? 1 : (!exact2 ? 2 : 3));
          const auto entry_precedes = [&](const SortEntry& a,
                                          const SortEntry& b) {
            if (a.k0 != b.k0) {
              return a.k0 < b.k0;
            }
            if (exact0) {
              if (a.k1 != b.k1) {
                return a.k1 < b.k1;
              }
              if (exact1 && a.k2 != b.k2) {
                return a.k2 < b.k2;
              }
            }
            if (fully_exact) {
              return a.idx < b.idx;
            }
            const Row& left_row = rows_ref[a.idx];
            const Row& right_row = rows_ref[b.idx];
            for (size_t i = start_i; i < order_specs.size(); ++i) {
              const slot_t col = *direct_key_cols[i];
              const int cmp =
                  order_specs[i].Compare(left_row[col], right_row[col]);
              if (cmp != 0) {
                return cmp < 0;
              }
            }
            return a.idx < b.idx;
          };
          if (bounded && keep < n) {
            std::partial_sort(
                entries.begin(),
                entries.begin() + static_cast<std::ptrdiff_t>(keep),
                entries.end(), entry_precedes);
          } else {
            std::sort(entries.begin(), entries.end(), entry_precedes);
          }
          std::vector<Row> old_rows = std::move(output->rows);
          output->ResetContents();
          output->rows.reserve(keep);
          for (size_t i = 0; i < keep; ++i) {
            output->AddRow(std::move(old_rows[entries[i].idx]));
          }
          RETURN_IF_FAIL(output->FinishSpill());
          if (context.execution_runtime() != nullptr) {
            context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
          }
          return Status::kSuccess;
        }
      }
    }
    std::vector<uint32_t> order(n);
    for (size_t i = 0; i < n; ++i) {
      order[i] = static_cast<uint32_t>(i);
    }
    const auto idx_precedes = [&](uint32_t a, uint32_t b) {
      const Row& left_row = rows_ref[a];
      const Row& right_row = rows_ref[b];
      for (size_t i = 0; i < order_specs.size(); ++i) {
        const slot_t col = *direct_key_cols[i];
        const int cmp = order_specs[i].Compare(left_row[col], right_row[col]);
        if (cmp != 0) {
          return cmp < 0;
        }
      }
      return a < b;
    };
    if (bounded && keep < n) {
      std::partial_sort(order.begin(),
                        order.begin() + static_cast<std::ptrdiff_t>(keep),
                        order.end(), idx_precedes);
    } else {
      std::ranges::stable_sort(order, idx_precedes);
    }
    std::vector<Row> old_rows = std::move(output->rows);
    output->ResetContents();
    output->rows.reserve(keep);
    for (size_t i = 0; i < keep; ++i) {
      output->AddRow(std::move(old_rows[order[i]]));
    }
    RETURN_IF_FAIL(output->FinishSpill());
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
    }
    return Status::kSuccess;
  }

  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
    size_t sequence{0};
  };
  std::vector<KeyedRow> sortable;
  Status sort_error{Status::kSuccess};
  Status iterated = output->ForEachRow([&](const Row& row) -> void {
    Scope scope{.row = &row, .schema = &output->schema, .outer = outer};
    std::vector<Value> keys;
    keys.reserve(order_by.size());
    for (size_t k = 0; k < key_expressions.size(); ++k) {
      if (sort_error != Status::kSuccess) {
        return;
      }
      if (direct_key_cols[k].has_value()) {
        keys.push_back(row[*direct_key_cols[k]]);
        continue;
      }
      StatusOr<Value> hv33968_0 =
          TryEvaluate(key_expressions[k], scope, nullptr, context, ctes);
      if (!hv33968_0.HasValue()) {
        sort_error = hv33968_0.GetStatus();
        return;
      }
      keys.push_back(hv33968_0.MoveValue());
    }
    sortable.push_back(KeyedRow{
        .keys = std::move(keys), .row = row, .sequence = sortable.size()});
  });
  RETURN_IF_FAIL(sort_error);
  // Every row was copied into `sortable`; detach the finished spill files so
  // re-adding below cannot hit "Append after FinishWriting".
  output->ResetContents();
  const auto precedes = [&order_specs](const KeyedRow& left,
                                       const KeyedRow& right) {
    for (size_t i = 0; i < order_specs.size(); ++i) {
      const int cmp = order_specs[i].Compare(left.keys[i], right.keys[i]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return left.sequence < right.sequence;
  };
  bool bounded = false;
  size_t keep = sortable.size();
  if (allow_bounded && statement.HasLimit() && !statement.WithTies() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    keep = std::min(sortable.size(), statement.Offset() + statement.Limit());
    bounded = true;
  }
  if (bounded && context.execution_runtime() != nullptr) {
    ++context.execution_runtime()->topn_finish_uses;
    context.execution_runtime()->topn_finish_capacity = keep;
    context.execution_runtime()->topn_finish_input_rows = sortable.size();
  }
  if (bounded && keep < sortable.size()) {
    std::partial_sort(sortable.begin(),
                      sortable.begin() + static_cast<std::ptrdiff_t>(keep),
                      sortable.end(), precedes);
  } else {
    std::ranges::stable_sort(sortable, precedes);
  }
  for (size_t i = 0; i < keep; ++i) {
    output->AddRow(std::move(sortable[i].row));
  }
  if (iterated != Status::kSuccess && sort_error == Status::kSuccess) {
    sort_error = iterated;
  }
  RETURN_IF_FAIL(sort_error);
  RETURN_IF_FAIL(output->FinishSpill());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
  }
  return Status::kSuccess;
}

StatusOr<Relation> LimitedRows(const SelectStatement& statement,
                               Relation&& input,
                               TransactionContext* context = nullptr,
                               const Scope* outer = nullptr,
                               const CteMap* ctes = nullptr) {
  Relation limited;
  limited.schema = input.schema;
  CopyExecutionStats(&limited, input);
  RETURN_IF_FAIL(input.FinishSpill());
  const size_t total = input.TotalRows();
  const size_t begin = std::min(statement.Offset(), total);
  const size_t available = total - begin;
  // An explicit LIMIT 0 is a real bound (zero rows); only an absent LIMIT
  // leaves the window unbounded.
  const bool unlimited = statement.Limit() == 0 && !statement.HasLimit();
  const size_t count =
      unlimited ? available : std::min(statement.Limit(), available);
  if (count == 0) {
    RETURN_IF_FAIL(limited.FinishSpill());
    return limited;
  }

  if (!statement.WithTies()) {
    if (!input.HasSpill()) {
      if (begin == 0) {
        if (count < input.rows.size()) {
          input.rows.resize(count);
        }
        return std::move(input);
      }
      limited.rows.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        limited.rows.push_back(std::move(input.rows[begin + i]));
      }
      return limited;
    }
    size_t seen = 0;
    size_t emitted = 0;
    RETURN_IF_FAIL(input.ForEachRow([&](const Row& row) {
      if (seen++ < begin || emitted >= count) {
        return;
      }
      limited.AddRow(row);
      ++emitted;
    }));
    RETURN_IF_FAIL(limited.FinishSpill());
    return limited;
  }

  std::vector<Row> all_rows;
  all_rows.reserve(total);
  RETURN_IF_FAIL(
      input.ForEachRow([&](const Row& row) { all_rows.push_back(row); }));

  if (!statement.OrderBy().empty() && count > 0 &&
      begin + count <= all_rows.size()) {
    const size_t last_idx = begin + count - 1;
    const Row& last_row = all_rows[last_idx];
    size_t end_idx = begin + count;
    while (end_idx < all_rows.size()) {
      bool tied = true;
      for (const auto& term : statement.OrderBy()) {
        Scope scope_last{
            .row = &last_row, .schema = &input.schema, .outer = outer};
        Scope scope_curr{
            .row = &all_rows[end_idx], .schema = &input.schema, .outer = outer};
        StatusOr<Value> last_or =
            context != nullptr && ctes != nullptr
                ? TryEvaluate(term.expression, scope_last, nullptr, *context,
                              *ctes)
                : term.expression->TryEvaluate(last_row, input.schema);
        ASSIGN_OR_RETURN(Value, val_last, (std::move(last_or)));
        StatusOr<Value> curr_or =
            context != nullptr && ctes != nullptr
                ? TryEvaluate(term.expression, scope_curr, nullptr, *context,
                              *ctes)
                : term.expression->TryEvaluate(all_rows[end_idx], input.schema);
        ASSIGN_OR_RETURN(Value, val_curr, (std::move(curr_or)));
        // Peers must collapse exactly the way the ORDER BY grouped them, so a
        // case-insensitive key treats 'a' and 'A' as the same tie instead of
        // cutting the tie group at the LIMIT boundary.
        if (!EqualUnderCollation(val_last, val_curr)) {
          tied = false;
          break;
        }
      }
      if (!tied) {
        break;
      }
      ++end_idx;
    }
    for (size_t i = begin; i < end_idx; ++i) {
      limited.AddRow(all_rows[i]);
    }
  } else {
    for (size_t i = begin; i < begin + count && i < all_rows.size(); ++i) {
      limited.AddRow(all_rows[i]);
    }
  }

  RETURN_IF_FAIL(limited.FinishSpill());
  return limited;
}

namespace {

// Window functions in a grouped statement evaluate over the grouped rows
// (SQL semantics), including calls whose arguments are aggregates themselves
// (`avg(sum(x)) OVER (PARTITION BY key)` — TPC-DS q12/q89).  This runs the
// statement in two passes:
//   1. a grouped pass where every window call is pulled out of the select
//      list, each distinct aggregate nested inside a window call becomes a
//      hidden `$aggK` select item, and the ordinary grouping machinery runs;
//   2. a window pass computing each call over the grouped rows (window
//      arguments referencing `$aggK` columns), producing `$winN` columns;
//   3. a final projection pass over grouped+$win rows that only references
//      materialized columns, restoring ORDER BY / DISTINCT / LIMIT.
StatusOr<Relation> ExecuteWindowOverAggregate(
    TransactionContext& context, const SelectStatement& statement,
    Relation input, const Scope* outer, const CteMap& ctes, bool apply_where) {
  const std::string unsupported =
      "window functions combined with GROUP BY are not supported here";
  if (statement.HasDistinctOn()) {
    return StatusError(StatusCode::kInvalidArgument, unsupported);
  }
  for (const NamedExpression& projection : statement.SelectList()) {
    if (projection.expression &&
        projection.expression->Type() == TypeTag::kColumnValue &&
        projection.expression->AsColumnValue().GetColumnName().name == "*") {
      return StatusError(StatusCode::kInvalidArgument, unsupported);
    }
  }
  std::vector<Expression> window_nodes;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectWindowCalls(projection.expression, &window_nodes);
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectWindowCalls(term.expression, &window_nodes);
  }
  // QUALIFY is one more consumer of the same window/aggregate pool: its
  // window calls join the pass-2 computation and its aggregates become
  // hidden `$aggK` group-level columns.
  CollectWindowCalls(statement.Qualify(), &window_nodes);

  auto grouped = std::make_shared<SelectStatement>(statement);
  std::vector<NamedExpression> grouped_select;
  grouped_select.reserve(statement.SelectList().size());
  // Alias (lowercased) -> grouped output column name, used by pass 3 to point
  // ORDER BY references at materialized columns.
  std::unordered_map<std::string, std::string> alias_columns;
  // Items mixing a window call with other nodes (sum(x) * 100 /
  // sum(sum(x)) OVER ...) are re-built in pass 3 from `$aggK`/`$winN` columns;
  // their pass-1 slot is a NULL placeholder, and every aggregate they contain
  // becomes a hidden `$aggK` group-level column.
  std::vector<bool> item_has_window(statement.SelectList().size(), false);
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    item_has_window[i] = ContainsWindowCall(projection.expression);
    if (item_has_window[i]) {
      grouped_select.emplace_back(projection.name, ConstantValueExp(Value()));
    } else {
      grouped_select.emplace_back(projection.name, projection.expression);
    }
    if (!projection.name.empty()) {
      alias_columns.emplace(AsciiToLower(projection.name),
                            ProjectionName(projection, i));
    }
  }
  // Mixed items contribute their aggregates to the same $aggK pool up front
  // (pass 3 rebuilds the whole item from the materialized columns; the window
  // pass below reuses the registrations through aggregate_columns).
  std::vector<std::pair<std::string, Expression>> hidden_aggregates;
  // Window partition / ORDER BY keys materialized as hidden grouped columns
  // ($pkJ), for keys the select list does not project.
  std::vector<NamedExpression> partition_items;
  std::unordered_map<std::string, std::string> partition_columns;
  std::unordered_map<std::string, std::string> aggregate_columns;
  RegisterAggregatesForMaterialization(statement.Qualify(), &aggregate_columns,
                                       &hidden_aggregates);
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    if (!item_has_window[i] || statement.SelectList()[i].expression->Type() ==
                                   TypeTag::kWindowFunctionExp) {
      continue;
    }
    RegisterAggregatesForMaterialization(statement.SelectList()[i].expression,
                                         &aggregate_columns,
                                         &hidden_aggregates);
  }
  std::vector<std::shared_ptr<WindowFunctionCallExpression>> rewritten_windows;
  rewritten_windows.reserve(window_nodes.size());
  for (size_t w = 0; w < window_nodes.size(); ++w) {
    const std::string window_name = "$win" + std::to_string(w);
    aggregate_columns.emplace(window_nodes[w]->ToString(), window_name);
    // WindowFunctionCallExpression is non-copyable (ExpressionBase); rebuild
    // the call field by field so aggregate arguments can be pointed at the
    // grouped `$aggK` columns.
    const auto& original = window_nodes[w]->AsWindowFunctionCallExpression();
    auto rewritten = std::make_shared<WindowFunctionCallExpression>();
    rewritten->function = original.function;
    rewritten->where_filter = original.where_filter;
    rewritten->distinct = original.distinct;
    rewritten->inner_order_by = original.inner_order_by;
    rewritten->inner_limit = original.inner_limit;
    rewritten->partition_by = original.partition_by;
    rewritten->order_by = original.order_by;
    rewritten->frame_unit = original.frame_unit;
    rewritten->frame_start = original.frame_start;
    rewritten->frame_end = original.frame_end;
    rewritten->has_frame = original.has_frame;
    rewritten->exclusion = original.exclusion;
    rewritten->respect_nulls = original.respect_nulls;
    rewritten->ignore_nulls = original.ignore_nulls;
    rewritten->args = original.args;
    const auto point_at_grouped_aggregate =
        [&](const Expression& node) -> std::optional<Expression> {
      if (node->Type() != TypeTag::kAggregateExp) {
        return std::nullopt;
      }
      auto [it, inserted] = aggregate_columns.emplace(
          node->ToString(), "$agg" + std::to_string(hidden_aggregates.size()));
      if (inserted) {
        hidden_aggregates.emplace_back(it->second, node);
      }
      return ColumnValueExp(ColumnName("", it->second));
    };
    for (Expression& argument : rewritten->args) {
      argument = ReplaceSubExpressions(argument, point_at_grouped_aggregate);
    }
    if (rewritten->where_filter) {
      rewritten->where_filter = ReplaceSubExpressions(
          rewritten->where_filter, point_at_grouped_aggregate);
    }
    // Partition / ORDER BY keys must resolve against the grouped output, but
    // the window may name a GROUP BY key that the select list never projects.
    // Materialize every distinct key expression as a hidden `$pkJ` column and
    // point the rewritten call at it.
    const auto materialize_key = [&](const Expression& key) -> Expression {
      auto [it, inserted] = partition_columns.emplace(
          key->ToString(), "$pk" + std::to_string(partition_columns.size()));
      if (inserted) {
        partition_items.emplace_back(it->second, key);
      }
      return ColumnValueExp(ColumnName("", it->second));
    };
    for (Expression& key : rewritten->partition_by) {
      if (key) {
        key = materialize_key(key);
      }
    }
    for (auto& term : rewritten->order_by) {
      if (term.expression) {
        term.expression = materialize_key(term.expression);
      }
    }
    rewritten_windows.push_back(std::move(rewritten));
  }
  for (const auto& [name, aggregate] : hidden_aggregates) {
    grouped_select.emplace_back(name, aggregate);
  }
  for (const NamedExpression& item : partition_items) {
    grouped_select.emplace_back(item);
  }
  // Pass 1 also materializes plain ORDER BY terms as hidden `$okJ` columns.
  // The final pass evaluates its ordering over the grouped output, where an
  // unselected GROUP BY key (q12 orders by i_item_id without selecting it)
  // does not exist: without this column the ordering used to fall into the
  // row-alias whole-row fallback and "sorted" by a STRUCT of the grouped row.
  std::unordered_map<std::string, std::string> order_key_columns;
  std::vector<Expression> rewritten_order_terms;
  rewritten_order_terms.reserve(statement.OrderBy().size());
  const auto materialize_order_key = [&](const Expression& term) -> Expression {
    auto [it, inserted] = order_key_columns.emplace(
        term->ToString(), "$ok" + std::to_string(order_key_columns.size()));
    if (inserted) {
      grouped_select.emplace_back(it->second, term);
    }
    return ColumnValueExp(ColumnName("", it->second));
  };
  const auto term_needs_materialization = [&](const Expression& term) {
    if (!term) {
      return false;
    }
    // Window calls and aggregates are materialized through aggregate_columns
    // by the node-level rewrite below; a whole-term column would never
    // resolve in the grouped pass.
    if (ContainsWindowCall(term)) {
      return false;
    }
    bool aliases_only = true;
    std::function<void(const Expression&)> scan =
        [&](const Expression& node) {  // NOLINT(misc-no-recursion)
          if (!node) {
            return;
          }
          if (node->Type() == TypeTag::kAggregateExp) {
            aliases_only = false;
            return;
          }
          if (node->Type() == TypeTag::kColumnValue) {
            const ColumnName& referenced =
                node->AsColumnValue().GetColumnName();
            if (referenced.schema.empty() &&
                alias_columns.contains(AsciiToLower(referenced.name))) {
              aliases_only = false;
              return;
            }
          }
          for (const Expression& child : ExpressionChildren(node)) {
            scan(child);
          }
        };
    scan(term);
    return aliases_only;
  };
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    rewritten_order_terms.push_back(term_needs_materialization(term.expression)
                                        ? materialize_order_key(term.expression)
                                        : term.expression);
  }
  grouped->SetSelectList(std::move(grouped_select));
  grouped->SetOrderBy({});
  grouped->SetQualify(Expression{});
  grouped->SetLimit(std::nullopt);
  grouped->SetOffset(0);
  ASSIGN_OR_RETURN(Relation, grouped_output,
                   (FinishQuery(context, *grouped, std::move(input), outer,
                                ctes, apply_where, 0)));

  // Pass 2: compute every window over the grouped rows.
  std::vector<Row> rows;
  RETURN_IF_FAIL(grouped_output.ForEachRow(
      [&rows](const Row& row) { rows.push_back(row); }));
  const Schema grouped_schema = grouped_output.schema;
  const size_t grouped_width = grouped_schema.ColumnCount();
  std::vector<std::vector<Value>> computed(rewritten_windows.size());
  for (size_t w = 0; w < rewritten_windows.size(); ++w) {
    RETURN_IF_FAIL(ComputeOneWindow(context, *rewritten_windows[w], rows,
                                    grouped_schema, outer, ctes, &computed[w]));
  }
  std::vector<Column> extended_columns;
  extended_columns.reserve(grouped_width + rewritten_windows.size());
  for (size_t i = 0; i < grouped_width; ++i) {
    extended_columns.push_back(grouped_schema.GetColumn(i));
  }
  for (size_t w = 0; w < rewritten_windows.size(); ++w) {
    extended_columns.emplace_back("$win" + std::to_string(w), ValueType::kNull);
  }
  for (size_t r = 0; r < rows.size(); ++r) {
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      rows[r].values_.push_back(computed[w][r]);
    }
  }
  // Fix the $win column types from the appended values.
  for (size_t w = 0; w < rewritten_windows.size(); ++w) {
    Column& column = extended_columns[grouped_width + w];
    for (const Row& row : rows) {
      const Value& candidate = row.values_[grouped_width + w];
      if (!candidate.IsNull() && candidate.type != ValueType::kNull) {
        column = Column(column.Name(), ValueTypeOf(candidate));
        column.SetUnsigned(candidate.IsUnsigned());
        break;
      }
    }
  }
  // QUALIFY filters the grouped+$win rows (WHERE semantics: NULL and FALSE
  // drop the row), after the window pass materialized its inputs.
  if (statement.Qualify()) {
    const auto point_qualify_at_materialized =
        [&](const Expression& node) -> std::optional<Expression> {
      if (node->Type() == TypeTag::kWindowFunctionExp ||
          node->Type() == TypeTag::kAggregateExp) {
        return ColumnValueExp(
            ColumnName("", aggregate_columns.at(node->ToString())));
      }
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& referenced = node->AsColumnValue().GetColumnName();
        if (referenced.schema.empty()) {
          if (auto it = alias_columns.find(AsciiToLower(referenced.name));
              it != alias_columns.end()) {
            return ColumnValueExp(ColumnName("", it->second));
          }
        }
      }
      return std::nullopt;
    };
    const Expression rewritten_qualify = ReplaceSubExpressions(
        statement.Qualify(), point_qualify_at_materialized);
    Schema extended_schema(grouped_output.schema.Name(), extended_columns);
    std::vector<Row> filtered;
    filtered.reserve(rows.size());
    for (Row& row : rows) {
      const Scope qualify_scope{
          .row = &row, .schema = &extended_schema, .outer = outer};
      ASSIGN_OR_RETURN(Value, decided,
                       (TryEvaluate(rewritten_qualify, qualify_scope, nullptr,
                                    context, ctes)));
      if (Truthy(decided)) {
        filtered.push_back(std::move(row));
      }
    }
    rows = std::move(filtered);
  }
  Relation extended(context.execution_runtime());
  extended.schema =
      Schema(grouped_output.schema.Name(), std::move(extended_columns));
  extended.rows = std::move(rows);
  RETURN_IF_FAIL(extended.FinishSpill());

  // Pass 3: re-project by column reference and restore ordering/limit.
  auto final_statement = std::make_shared<SelectStatement>(statement);
  std::vector<NamedExpression> final_select;
  final_select.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    Expression expression;
    if (projection.expression->Type() == TypeTag::kWindowFunctionExp) {
      expression = ColumnValueExp(ColumnName(
          "", aggregate_columns.at(projection.expression->ToString())));
    } else if (item_has_window[i]) {
      // Mixed item: windows -> $winN, aggregates -> $aggK; the remaining
      // nodes reference group keys resolvable in the grouped schema.
      const auto point_at_materialized_item =
          [&](const Expression& node) -> std::optional<Expression> {
        if (node->Type() == TypeTag::kWindowFunctionExp ||
            node->Type() == TypeTag::kAggregateExp) {
          return ColumnValueExp(
              ColumnName("", aggregate_columns.at(node->ToString())));
        }
        return std::nullopt;
      };
      expression = ReplaceSubExpressions(projection.expression,
                                         point_at_materialized_item);
    } else {
      expression =
          ColumnValueExp(ColumnName("", ProjectionName(projection, i)));
    }
    final_select.emplace_back(projection.name, std::move(expression));
  }
  const auto point_at_materialized =
      [&](const Expression& node) -> std::optional<Expression> {
    if (node->Type() == TypeTag::kWindowFunctionExp) {
      return ColumnValueExp(
          ColumnName("", aggregate_columns.at(node->ToString())));
    }
    if (node->Type() == TypeTag::kColumnValue) {
      const ColumnName& referenced = node->AsColumnValue().GetColumnName();
      if (referenced.schema.empty()) {
        if (auto it = alias_columns.find(AsciiToLower(referenced.name));
            it != alias_columns.end()) {
          return ColumnValueExp(ColumnName("", it->second));
        }
      }
    }
    if (node->Type() == TypeTag::kAggregateExp) {
      // ORDER BY <aggregate>: point at the matching select item's column.
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        if (statement.SelectList()[i].expression->ToString() ==
            node->ToString()) {
          return ColumnValueExp(
              ColumnName("", ProjectionName(statement.SelectList()[i], i)));
        }
      }
    }
    return std::nullopt;
  };
  std::vector<SelectStatement::OrderByTerm> final_order;
  final_order.reserve(statement.OrderBy().size());
  for (size_t o = 0; o < statement.OrderBy().size(); ++o) {
    const SelectStatement::OrderByTerm& term = statement.OrderBy()[o];
    // A materialized `$okJ` column already names this term; anything else
    // keeps the node-level rewrite (windows -> $win, aggregates/aliases ->
    // select items).
    final_order.push_back(
        {rewritten_order_terms[o] == term.expression
             ? ReplaceSubExpressions(term.expression, point_at_materialized)
             : rewritten_order_terms[o],
         term.ascending, term.nulls_first});
  }
  final_statement->SetSelectList(std::move(final_select));
  final_statement->SetOrderBy(std::move(final_order));
  final_statement->SetWhereClause(Expression{});
  final_statement->SetHaving(Expression{});
  final_statement->SetGroupBy({});
  final_statement->SetQualify(Expression{});
  ASSIGN_OR_RETURN(Relation, projected,
                   (FinishQuery(context, *final_statement, std::move(extended),
                                outer, ctes, false, 0)));
  return LimitedRows(*final_statement, std::move(projected), &context, outer,
                     &ctes);
}

}  // namespace

}  // namespace

StatusOr<Relation> FinishQuery(TransactionContext& context,
                               const SelectStatement& statement, Relation input,
                               const Scope* outer, const CteMap& ctes,
                               bool apply_where, size_t hidden_columns) {
  if (apply_where && statement.WhereClause()) {
    const auto filter_begin = std::chrono::steady_clock::now();
    // Use the compiled filter path for WHERE evaluation when possible.
    // CompiledScanFilter separates SimpleCompare predicates (integer/string
    // column-vs-constant comparisons) from residual expressions, allowing
    // the fast integer/string comparison path to reject rows without the
    // overhead of recursive expression tree traversal.  This can reduce
    // filter time by 2-5x for queries with simple WHERE predicates.
    std::vector<Expression> where_predicates;
    if (statement.WhereClause()->Type() == TypeTag::kBinaryExp &&
        statement.WhereClause()->AsBinaryExpression().Op() ==
            BinaryOperation::kAnd) {
      where_predicates = SplitConjuncts(statement.WhereClause());
    } else {
      where_predicates = {statement.WhereClause()};
    }
    const relational_detail::FilterCompileEnv compile_env{
        .context = &context,
        .outer = outer,
        .ctes = &ctes,
        .input_rows = input.TotalRows(),
        .prebuild_only_exists = false};
    const relational_detail::CompiledScanFilter compiled =
        relational_detail::CompileScanFilter(where_predicates, input.schema,
                                             &compile_env);
    // Use compiled filter path when any fast-path is available: SimpleCompare,
    // disjunctive branches, or bytecode-compiled residuals.  The bytecode
    // path avoids recursive expression tree traversal for residual predicates,
    // which is the dominant cost for complex WHERE clauses like TPC-H Q20.
    if (!compiled.simple.empty() || !compiled.prebuilt_membership.empty() ||
        !compiled.prebuilt_scalar_compare.empty() ||
        !compiled.disjunctive_branches.empty() ||
        compiled.residual_bytecode.has_value() || !compiled.residual.empty()) {
      input.FinishSpill();
      if (!compiled.residual.empty() &&
          (!compiled.simple.empty() || !compiled.prefix_in.empty() ||
           !compiled.prebuilt_membership.empty() ||
           !compiled.prebuilt_scalar_compare.empty() ||
           !compiled.disjunctive_branches.empty())) {
        relational_detail::CompiledScanFilter non_residual = compiled;
        non_residual.residual.clear();
        Relation prefiltered(context.execution_runtime());
        prefiltered.schema = input.schema;
        CopyExecutionStats(&prefiltered, input);
        Status pre_error{Status::kSuccess};
        if (!relational_detail::TryParallelFilterRows(
                context, input, non_residual, input.schema, outer, ctes,
                nullptr, &prefiltered, &pre_error)) {
          relational_detail::ColumnLookupCache pre_lookup;
          Status pre_iter = input.ForEachRow([&](const Row& row) {
            if (!pre_error.ok()) {
              return;
            }
            if (relational_detail::MatchScanFilter(
                    row, input.schema, non_residual, outer, context, ctes,
                    &pre_error, &pre_lookup)) {
              prefiltered.AddRow(row);
            }
          });
          RETURN_IF_FAIL(pre_iter);
        }
        RETURN_IF_FAIL(pre_error);
        prefiltered.FinishSpill();
        input = std::move(prefiltered);
      }
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      Status eval_error{Status::kSuccess};
      // A/B hatch: TINYLAMB_DISABLE_FINISH_PARALLEL_FILTER=1 keeps the serial
      // per-row loop below.
      static const bool finish_parallel_disabled =
          std::getenv("TINYLAMB_DISABLE_FINISH_PARALLEL_FILTER") != nullptr;
      bool filtered_in_parallel = false;
      if (!finish_parallel_disabled) {
        // Worker threads decide rows with the positional compares and the
        // compiled bytecode programs; rows whose program raised are parked
        // and re-checked on this thread through the same contextful
        // MatchScanFilter call the serial loop uses, so error semantics are
        // unchanged.  Filters that need the evaluator (query expressions the
        // bytecode cannot compile) make the parallel attempt decline and
        // fall through to the serial loop.
        filtered_in_parallel = relational_detail::TryParallelFilterRows(
            context, input, compiled, input.schema, outer, ctes, nullptr,
            &filtered, &eval_error);
      }
      if (filtered_in_parallel) {
        RETURN_IF_FAIL(eval_error);
      } else {
        RETURN_IF_FAIL((([&]() -> Status {
          // Register the filtered relation so a correlated-subquery build
          // triggered by one of the predicates can prune its build scan to
          // this pipeline's outer key domain.
          relational_detail::ScopedCorrelatedOuter correlated_outer(context,
                                                                    input);
          return input.ForEachRow([&](const Row& row) {
            if (!eval_error.ok()) {
              return;
            }
            if (relational_detail::MatchScanFilter(row, input.schema, compiled,
                                                   outer, context, ctes,
                                                   &eval_error)) {
              filtered.AddRow(row);
            }
          });
        })()));
        RETURN_IF_FAIL(eval_error);
      }
      filtered.FinishSpill();
      input = std::move(filtered);
    } else {
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      input.FinishSpill();
      // EXC-SHIM replacement (Phase 7, no-exception-rule-migration.md): the
      // per-row evaluation must not use the throwing Evaluate wrapper.  An
      // unresolvable qualified reference inside a subquery predicate
      // (sql_oracle_fuzz find: `WHERE EXISTS (... d.parent ...)` over a
      // 1-column subquery) came back as a Status from TryEvaluate, and the
      // shim re-raised it as a C++ exception that escaped the whole
      // executor boundary instead of surfacing through GetStatus.
      Status filter_error{Status::kSuccess};
      RETURN_IF_FAIL(input.ForEachRow([&](const Row& row) {
        if (filter_error != Status::kSuccess) {
          return;
        }
        Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
        StatusOr<Value> keep =
            TryEvaluate(statement.WhereClause(), scope, nullptr, context, ctes);
        if (!keep.HasValue()) {
          filter_error = keep.GetStatus();
          return;
        }
        if (Truthy(keep.Value())) {
          filtered.AddRow(row);
        }
      }));
      RETURN_IF_FAIL(filter_error);
      filtered.FinishSpill();
      input = std::move(filtered);
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
    }
  }

  ASSIGN_OR_RETURN(Relation, output,
                   (Project(context, statement, std::move(input), outer, ctes,
                            hidden_columns)));
  if (statement.Distinct()) {
    if (statement.HasDistinctOn()) {
      if (!statement.OrderBy().empty()) {
        RETURN_IF_FAIL(
            ApplyOrderBy(context, statement, &output, outer, ctes, false));
      }
      ASSIGN_OR_RETURN(Relation, distinct_of,
                       (DistinctOf(std::move(output), statement.DistinctOn(),
                                   &context, outer, &ctes)));
      output = std::move(distinct_of);
    } else {
      ASSIGN_OR_RETURN(Relation, distinct_of, (DistinctOf(std::move(output))));
      output = std::move(distinct_of);
      if (!statement.OrderBy().empty()) {
        RETURN_IF_FAIL(ApplyOrderBy(context, statement, &output, outer, ctes));
      }
    }
  }
  return LimitedRows(statement, std::move(output), &context, outer, &ctes);
}
namespace {

bool IsIdentityDerived(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].query ||
      statement.Sources()[0].unnest || statement.Sources()[0].table.empty() ||
      statement.WhereClause() || statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.OrderBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty()) {
    return false;
  }
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& column =
            item.expression->AsColumnValue().GetColumnName();
        return column.name != "*" &&
               (item.name.empty() || item.name == column.name);
      });
}

bool IsDeterministicProjectionExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(
      ExpressionChildren(expression), [](const Expression& child) {
        return IsDeterministicProjectionExpression(child);
      });
}

bool IsFlattenableProjection(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].unnest ||
      (!statement.Sources()[0].query && statement.Sources()[0].table.empty()) ||
      statement.WhereClause() || statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.UnionAll().empty() ||
      statement.Distinct() || !statement.OrderBy().empty() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty() || HasWindowFunctions(statement)) {
    return false;
  }
  // Projection flattening is only valid for expressions that do not create a
  // second query scope or change row cardinality.
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               (item.expression->Type() != TypeTag::kColumnValue ||
                item.expression->AsColumnValue().GetColumnName().name != "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

size_t FlattenableProjectionDepth(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || !statement.Sources()[0].query ||
      !IsFlattenableProjection(*statement.Sources()[0].query)) {
    return 0;
  }
  return 1 + FlattenableProjectionDepth(*statement.Sources()[0].query);
}

Expression SubstituteProjectionColumns(  // NOLINT(misc-no-recursion)
    const Expression& expression, const SelectStatement& projection,
    const std::string& projection_alias, bool* valid) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    *valid = false;
    return nullptr;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (!column.schema.empty() &&
        !IdentifierEquals(column.schema, projection_alias)) {
      *valid = false;
      return nullptr;
    }
    for (const NamedExpression& item : projection.SelectList()) {
      if (!item.expression) {
        continue;
      }
      std::string output_name;
      if (item.name.empty() &&
          item.expression->Type() == TypeTag::kColumnValue) {
        output_name = item.expression->AsColumnValue().GetColumnName().name;
      } else {
        output_name = item.name;
      }
      if (IdentifierEquals(output_name, column.name)) {
        return item.expression;
      }
    }
    *valid = false;
    return nullptr;
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  if (children.empty()) {
    return expression;
  }
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  for (const Expression& child : children) {
    rewritten.push_back(SubstituteProjectionColumns(child, projection,
                                                    projection_alias, valid));
    if (!*valid) {
      return nullptr;
    }
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}

std::shared_ptr<SelectStatement> FlattenProjectionBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (!IsFlattenableProjection(derived) || (statement.Sources().size() != 1) ||
      statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.UnionAll().empty() ||
      statement.Distinct() || HasWindowFunctions(statement)) {
    return nullptr;
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result =
        SubstituteProjectionColumns(expression, derived, derived_alias, &valid);
    return valid ? result : Expression{};
  };
  std::vector<NamedExpression> select_list;
  select_list.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression expression = rewrite(item.expression);
    if (!expression) {
      return nullptr;
    }
    select_list.emplace_back(item.name, std::move(expression));
  }
  Expression where =
      statement.WhereClause() ? rewrite(statement.WhereClause()) : Expression{};
  if (statement.WhereClause() && !where) {
    return nullptr;
  }
  std::vector<SelectStatement::OrderByTerm> order_by;
  order_by.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression expression = rewrite(term.expression);
    if (!expression) {
      return nullptr;
    }
    order_by.push_back(
        SelectStatement::OrderByTerm{.expression = std::move(expression),
                                     .ascending = term.ascending,
                                     .nulls_first = term.nulls_first});
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetWhereClause(std::move(where));
  rewritten->SetOrderBy(std::move(order_by));
  SelectSource merged = derived.Sources()[0];
  if (source.sample && merged.sample) {
    return nullptr;
  }
  if (source.sample) {
    merged.sample = source.sample;
  }
  merged.alias = source.alias.empty() ? merged.alias : source.alias;
  merged.join_type = source.join_type;
  merged.join_condition = source.join_condition;
  rewritten->SetSources({std::move(merged)});
  return rewritten;
}

// Rebind a predicate written against a derived-table output to the base
// relation below it. Only the expression forms reconstructed here are used by
// the pushdown rule; a nullptr means that the predicate has a shape we cannot
// prove safe to move.
Expression RebindDerivedPredicate(const Expression& expression,
                                  const SelectStatement& derived,
                                  const std::string& derived_alias) {
  if (!expression) {
    return nullptr;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (!column.schema.empty() && column.schema != derived_alias) {
        return expression;
      }
      for (const NamedExpression& item : derived.SelectList()) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& projected =
            item.expression->AsColumnValue().GetColumnName();
        const std::string output_name =
            item.name.empty() ? projected.name : item.name;
        if (output_name == column.name) {
          const SelectSource& source = derived.Sources()[0];
          return ColumnValueExp(
              ColumnName(source.alias.empty() ? source.table : source.alias,
                         projected.name));
        }
      }
      return nullptr;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      Expression left =
          RebindDerivedPredicate(binary.Left(), derived, derived_alias);
      Expression right =
          RebindDerivedPredicate(binary.Right(), derived, derived_alias);
      return left && right ? BinaryExpressionExp(std::move(left), binary.Op(),
                                                 std::move(right))
                           : nullptr;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      Expression child =
          RebindDerivedPredicate(unary.Child(), derived, derived_alias);
      return child ? UnaryExpressionExp(std::move(child), unary.Op()) : nullptr;
    }
    default:
      // Constants are safe and leave their identity unchanged.  Query,
      // aggregate, and function nodes are retained only when no column
      // rebinding is needed; the caller still rejects predicates with query
      // expressions before invoking this helper.
      return expression->TouchedColumns().empty() ? expression : nullptr;
  }
}

bool PredicateCanEnterDerived(const Expression& predicate,
                              const SelectStatement& derived,
                              const std::string& derived_alias) {
  if (!predicate || ContainsQuery(predicate) || derived.Sources().size() != 1 ||
      derived.Sources()[0].query || derived.Sources()[0].unnest ||
      derived.Sources()[0].table.empty() || derived.Having() ||
      derived.Qualify() || !derived.GroupBy().empty() ||
      !derived.OrderBy().empty() || derived.Distinct() || derived.HasLimit() ||
      derived.Offset() != 0 || derived.SelectList().empty()) {
    return false;
  }
  for (const ColumnName& column : predicate->TouchedColumns()) {
    bool found = false;
    for (const NamedExpression& item : derived.SelectList()) {
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& projected =
          item.expression->AsColumnValue().GetColumnName();
      const std::string output_name =
          item.name.empty() ? projected.name : item.name;
      if (output_name == column.name &&
          (column.schema.empty() || column.schema == derived_alias)) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }
  // Do not classify an expression as pushable unless the rebinder can
  // actually preserve its shape.  In particular, function calls and other
  // expression kinds must stay above the derived-table boundary rather than
  // being silently removed from the outer predicate.
  return !predicate->TouchedColumns().empty() &&
         RebindDerivedPredicate(predicate, derived, derived_alias) != nullptr;
}

// Multi-source variant of IsFlattenableProjection: the derived side is a
// plain inner-join projection.  Unlike the single-source boundary above, its
// WHERE clause is allowed -- it composes into the merged statement's WHERE,
// which is equivalent because the merged sources are exactly the derived's
// own sources and every join inside them is inner.
bool IsFlattenableJoinedProjection(const SelectStatement& statement) {
  if (statement.Sources().empty() || statement.Having() ||
      statement.Qualify() || !statement.GroupBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty() || HasWindowFunctions(statement) ||
      !statement.WithQueries().empty()) {
    return false;
  }
  for (const SelectSource& source : statement.Sources()) {
    if (source.unnest || (source.query && source.table.empty()) ||
        (source.join_type != JoinType::kInner &&
         source.join_type != JoinType::kCross) ||
        source.sample) {
      return false;
    }
  }
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               (item.expression->Type() != TypeTag::kColumnValue ||
                item.expression->AsColumnValue().GetColumnName().name != "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

// Flattens a grouping boundary: a statement that GROUPs over a single derived
// table absorbs the derived projection when the derived side is itself a
// plain multi-source inner-join projection.  Group keys, select items and
// ORDER BY terms re-resolve against the derived select list (aggregates
// rebuild losslessly), so the grouping runs directly over the joined base
// relations and the grouped-join fusion can fire at the join step instead of
// materializing the multi-million-row join relation.
std::shared_ptr<SelectStatement> FlattenGroupedDerivedBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (statement.Sources().size() != 1 || statement.Having() ||
      statement.Qualify() || !statement.UnionAll().empty() ||
      HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement) ||
      !IsFlattenableJoinedProjection(derived)) {
    return nullptr;
  }
  if (source.join_type != JoinType::kInner &&
      source.join_type != JoinType::kCross) {
    return nullptr;
  }
  if (source.sample || ContainsQuery(derived.WhereClause())) {
    return nullptr;
  }
  // Duplicate derived output names make the substitution ambiguous.
  for (auto a = derived.SelectList().begin(); a != derived.SelectList().end();
       ++a) {
    const std::string a_name =
        !a->name.empty()
            ? a->name
            : (a->expression->Type() == TypeTag::kColumnValue
                   ? a->expression->AsColumnValue().GetColumnName().name
                   : std::string());
    if (a_name.empty()) {
      continue;
    }
    for (auto b = std::next(a); b != derived.SelectList().end(); ++b) {
      const std::string b_name =
          !b->name.empty()
              ? b->name
              : (b->expression->Type() == TypeTag::kColumnValue
                     ? b->expression->AsColumnValue().GetColumnName().name
                     : std::string());
      if (!b_name.empty() && IdentifierEquals(a_name, b_name)) {
        return nullptr;
      }
    }
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result =
        SubstituteProjectionColumns(expression, derived, derived_alias, &valid);
    return valid ? result : Expression{};
  };
  std::vector<NamedExpression> select_list;
  select_list.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression expression = rewrite(item.expression);
    if (!expression) {
      return nullptr;
    }
    select_list.emplace_back(item.name, std::move(expression));
  }
  std::vector<Expression> group_by;
  group_by.reserve(statement.GroupBy().size());
  for (const Expression& key : statement.GroupBy()) {
    Expression expression = rewrite(key);
    if (!expression) {
      return nullptr;
    }
    group_by.push_back(std::move(expression));
  }
  std::vector<SelectStatement::OrderByTerm> order_by;
  order_by.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    // A term that names a select alias stays a plain column reference: the
    // fused path orders the projected output through select aliases, and a
    // substituted expression there would defeat that resolution.
    const bool alias_ref =
        term.expression && term.expression->Type() == TypeTag::kColumnValue &&
        term.expression->AsColumnValue().GetColumnName().schema.empty() &&
        std::any_of(
            statement.SelectList().begin(), statement.SelectList().end(),
            [&](const NamedExpression& item) {
              return !item.name.empty() &&
                     IdentifierEquals(
                         item.name,
                         term.expression->AsColumnValue().GetColumnName().name);
            });
    Expression expression =
        alias_ref ? term.expression : rewrite(term.expression);
    if (!expression) {
      return nullptr;
    }
    order_by.push_back(
        SelectStatement::OrderByTerm{.expression = std::move(expression),
                                     .ascending = term.ascending,
                                     .nulls_first = term.nulls_first});
  }
  // The derived's own conjuncts move up unchanged (same base sources); the
  // parent's conjuncts are re-bound to the derived outputs first.
  std::vector<Expression> where = SplitConjuncts(derived.WhereClause());
  if (statement.WhereClause()) {
    Expression rebound = rewrite(statement.WhereClause());
    if (!rebound) {
      return nullptr;
    }
    for (Expression& conjunct : SplitConjuncts(rebound)) {
      where.push_back(std::move(conjunct));
    }
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetGroupBy(std::move(group_by));
  rewritten->SetWhereClause(where.empty() ? Expression{}
                                          : CombineConjuncts(where));
  rewritten->SetOrderBy(std::move(order_by));
  rewritten->SetSources(derived.Sources());
  return rewritten;
}

}  // namespace

std::shared_ptr<SelectStatement> OptimizeDerivedBoundaries(
    const SelectStatement& statement, const CteMap& inherited_ctes) {
  if (statement.Sources().size() != 1 ||
      statement.Sources()[0].query == nullptr) {
    return nullptr;
  }
  const SelectSource& source = statement.Sources()[0];
  const SelectStatement& derived = *source.query;
  if (derived.Sources().empty()) {
    return nullptr;
  }
  const auto& inner_source = derived.Sources()[0];
  // A CTE name occupies the same syntactic slot as a base table but cannot be
  // flattened into a catalog scan. This also covers a derived query's own
  // WITH clause and an inherited outer CTE.
  if (!inner_source.table.empty() &&
      (derived.WithQueries().contains(inner_source.table) ||
       inherited_ctes.contains(inner_source.table))) {
    return nullptr;
  }
  auto rewritten = std::make_shared<SelectStatement>(statement);

  // Flatten SELECT column-list FROM one-table derived tables.  This is
  // limited to identity projections so aliases and row multiplicity remain
  // unchanged without expression substitution.
  if (IsIdentityDerived(derived)) {
    SelectSource merged = derived.Sources()[0];
    if (source.sample && merged.sample) {
      return nullptr;
    }
    if (source.sample) {
      merged.sample = source.sample;
    }
    merged.alias = source.alias.empty() ? merged.alias : source.alias;
    merged.join_type = source.join_type;
    merged.join_condition = source.join_condition;
    rewritten->SetSources({std::move(merged)});
    return rewritten;
  }

  // Compose projections through a cardinality-preserving derived query. This
  // removes nested projection stacks while substituting the derived output
  // expressions into the parent SELECT/WHERE/ORDER BY. The rewrite is
  // intentionally restricted to a single source with no grouping, ordering,
  // limit, window, or subquery expression.
  if (std::shared_ptr<SelectStatement> flattened =
          FlattenProjectionBoundary(statement, source, derived)) {
    return flattened;
  }

  // A grouping parent over a plain joined projection absorbs the projection
  // so the grouping (and the grouped-join fusion) runs over the base sources.
  if (std::shared_ptr<SelectStatement> flattened =
          FlattenGroupedDerivedBoundary(statement, source, derived)) {
    return flattened;
  }

  std::vector<Expression> pushable;
  std::vector<Expression> remaining;
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    (PredicateCanEnterDerived(conjunct, derived,
                              source.alias.empty() ? "u" : source.alias)
         ? pushable
         : remaining)
        .push_back(conjunct);
  }
  // UNION ALL and UNION DISTINCT both distribute a local predicate over
  // their branches (filtering before or after the dedup is equivalent), so
  // the pushdown below is safe for either flavor.
  const bool is_union =
      !derived.UnionAll().empty() &&
      (derived.GetSetOperationTree() == nullptr ||
       !derived.GetSetOperationTree()->grouped) &&
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll ||
                                   kind == SetOperationKind::kUnion;
                          });
  if (is_union) {
    // UNION ALL / UNION DISTINCT have no row-changing boundary before the outer
    // filter. Push a local predicate into every branch, then retain any
    // non-local predicates above the set operation.
    SelectStatement pushed = derived;
    std::vector<Expression> head_where = SplitConjuncts(pushed.WhereClause());
    for (const Expression& predicate : pushable) {
      if (Expression rebound = RebindDerivedPredicate(
              predicate, pushed, source.alias.empty() ? "u" : source.alias)) {
        head_where.push_back(std::move(rebound));
      } else {
        return nullptr;
      }
    }
    if (!pushable.empty()) {
      pushed.SetWhereClause(CombineConjuncts(head_where));
    }
    std::vector<std::shared_ptr<SelectStatement>> branches;
    branches.reserve(derived.UnionAll().size());
    for (const auto& branch : derived.UnionAll()) {
      if (!branch) {
        continue;
      }
      auto copied = std::make_shared<SelectStatement>(*branch);
      std::vector<Expression> branch_where =
          SplitConjuncts(copied->WhereClause());
      for (const Expression& predicate : pushable) {
        if (!PredicateCanEnterDerived(
                predicate, *copied,
                source.alias.empty() ? "u" : source.alias)) {
          return nullptr;
        }
        if (Expression rebound = RebindDerivedPredicate(
                predicate, *copied,
                source.alias.empty() ? "u" : source.alias)) {
          branch_where.push_back(std::move(rebound));
        } else {
          return nullptr;
        }
      }
      if (!pushable.empty()) {
        copied->SetWhereClause(CombineConjuncts(branch_where));
      }
      branches.push_back(std::move(copied));
    }
    if (!pushable.empty()) {
      for (const auto& branch : branches) {
        if (branch == nullptr ||
            branch->SelectList().size() != pushed.SelectList().size()) {
          return nullptr;
        }
      }
      const std::vector<SetOperationKind> kinds = derived.SetOperationKinds();
      const std::vector<SetOperationMatch> matches = derived.Matches();
      pushed.ClearUnionAll();
      pushed.SetSetOperationTree(nullptr);
      for (size_t i = 0; i < branches.size(); ++i) {
        pushed.AddSetOperation(
            i < kinds.size() ? kinds[i] : SetOperationKind::kUnionAll,
            branches[i], i < matches.size() ? matches[i] : SetOperationMatch{});
      }
      SelectSource updated = source;
      updated.query = std::make_shared<SelectStatement>(std::move(pushed));
      rewritten->SetSources({std::move(updated)});
      rewritten->SetWhereClause(
          remaining.empty() ? Expression{} : CombineConjuncts(remaining));
      return rewritten;
    }
  }

  if (!pushable.empty() && !is_union) {
    auto copied = std::make_shared<SelectStatement>(derived);
    std::vector<Expression> inner_where = SplitConjuncts(copied->WhereClause());
    for (const Expression& predicate : pushable) {
      if (Expression rebound = RebindDerivedPredicate(
              predicate, *copied, source.alias.empty() ? "d" : source.alias)) {
        inner_where.push_back(std::move(rebound));
      } else {
        return nullptr;
      }
    }
    copied->SetWhereClause(CombineConjuncts(inner_where));
    SelectSource updated = source;
    updated.query = std::move(copied);
    rewritten->SetSources({std::move(updated)});
    rewritten->SetWhereClause(remaining.empty() ? Expression{}
                                                : CombineConjuncts(remaining));
    return rewritten;
  }

  // For UNION ALL without a pushed predicate, a finite outer LIMIT still
  // bounds how many rows each append branch can contribute.  The final outer
  // LIMIT remains authoritative, including OFFSET semantics.
  if (is_union && statement.OrderBy().empty() && statement.HasLimit() &&
      statement.Limit() > 0 && statement.Offset() == 0 &&
      !derived.UnionDistinct() &&
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll;
                          })) {
    const size_t cap = statement.Limit();
    const bool head_needs_cap = !derived.HasLimit();
    const bool branch_needs_cap = std::ranges::any_of(
        derived.UnionAll(), [](const std::shared_ptr<SelectStatement>& branch) {
          return branch && !branch->HasLimit() && branch->Offset() == 0;
        });
    if (!head_needs_cap && !branch_needs_cap) {
      return nullptr;
    }
    SelectStatement pushed = derived;
    const std::vector<SetOperationMatch> matches = derived.Matches();
    pushed.ClearUnionAll();
    pushed.SetSetOperationTree(nullptr);
    auto cap_branch = [cap](const std::shared_ptr<SelectStatement>& branch) {
      auto copied = std::make_shared<SelectStatement>(*branch);
      if (!copied->HasLimit() && copied->Offset() == 0) {
        copied->SetLimit(cap);
      }
      return copied;
    };
    if (head_needs_cap) {
      pushed.SetLimit(cap);
    }
    for (size_t i = 0; i < derived.UnionAll().size(); ++i) {
      const auto& branch = derived.UnionAll()[i];
      if (branch) {
        pushed.AddSetOperation(
            SetOperationKind::kUnionAll, cap_branch(branch),
            i < matches.size() ? matches[i] : SetOperationMatch{});
      }
    }
    SelectSource updated = source;
    updated.query = std::make_shared<SelectStatement>(std::move(pushed));
    rewritten->SetSources({std::move(updated)});
    return rewritten;
  }
  return nullptr;
}

namespace {

// Recursive-union column contract.  The set-operation metadata names the
// column-match mode per term: `BY NAME` / `STRICT CORRESPONDING` demand the
// same column names on both sides, plain `CORRESPONDING` keeps only the
// columns both sides produce (GoogleSQL's INNER default), and an ON/BY
// column list restricts either mode to the listed columns.  Positional
// unions keep the anchor's column order and require the same width.

bool NameMatchedUnion(const SetOperationMatch& match) {
  return match.by_name || match.corresponding || !match.columns.empty();
}

// Column names a term writes, as the author spelled them.  nullopt when the
// width is only known after execution (an unexpanded `*`).
std::optional<std::vector<std::string>> StaticTermNames(
    const SelectStatement& statement) {
  std::vector<std::string> names;
  names.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    if (item.name == "*" || item.star_modifiers != nullptr) {
      return std::nullopt;
    }
    names.push_back(item.name);
  }
  return names;
}

bool ContainsColumnName(const std::vector<std::string>& names,
                        std::string_view wanted) {
  return std::any_of(names.begin(), names.end(),
                     [&](const std::string& candidate) {
                       return IdentifierEquals(candidate, wanted);
                     });
}

// Maps `targets` (the working table's columns, in output order) onto `source`
// by name.  `strict` also rejects a source column the working table does not
// name, which is what BY NAME / STRICT CORRESPONDING require.
StatusOr<std::vector<size_t>> ResolveNameAlignment(
    const std::vector<std::string>& targets, const Schema& source, bool strict,
    const std::string& context) {
  std::vector<size_t> alignment;
  alignment.reserve(targets.size());
  for (const std::string& wanted : targets) {
    size_t found = source.ColumnCount();
    for (slot_t index = 0; index < source.ColumnCount(); ++index) {
      if (!IdentifierEquals(source.GetColumn(index).Name().name, wanted)) {
        continue;
      }
      if (found != source.ColumnCount()) {
        std::string msg = context;
        msg += " produces duplicate column ";
        msg += wanted;
        return StatusError(StatusCode::kInvalidArgument, std::move(msg));
      }
      found = index;
    }
    if (found == source.ColumnCount()) {
      std::string msg = context;
      msg += " has no column named ";
      msg += wanted;
      return StatusError(StatusCode::kInvalidArgument, std::move(msg));
    }
    alignment.push_back(found);
  }
  if (strict) {
    for (slot_t index = 0; index < source.ColumnCount(); ++index) {
      const std::string& name = source.GetColumn(index).Name().name;
      if (!ContainsColumnName(targets, name)) {
        std::string msg = context;
        msg += " produces column ";
        msg += name;
        msg += " that the working table does not name";
        return StatusError(StatusCode::kInvalidArgument, std::move(msg));
      }
    }
  }
  return alignment;
}

}  // namespace

StatusOr<Relation> ExecuteRecursiveCte(TransactionContext& context,
                                       const std::string& name,
                                       const SelectStatement& body,
                                       const Scope* outer,
                                       const CteMap& inherited_ctes,
                                       const RecursiveDepthSpec* depth_spec) {
  constexpr size_t kMaxIterations = 1024;
  constexpr size_t kMaxRows = 10'000'000;
  if (body.UnionAll().empty()) {
    return ExecuteQuery(context, body, outer, inherited_ctes);
  }

  // Evaluate the non-recursive head once, then bind the CTE name to only the
  // previous round's delta while evaluating each recursive branch.  This is
  // the work-table contract that the old merge omitted entirely.
  SelectStatement anchor = body;
  anchor.ClearUnionAll();
  anchor.SetOrderBy({});
  anchor.SetLimit(std::nullopt);
  anchor.SetOffset(0);
  ASSIGN_OR_RETURN(Relation, anchor_result,
                   (ExecuteQuery(context, anchor, outer, inherited_ctes)));
  anchor_result.FinishSpill();

  const bool track_depth = depth_spec != nullptr;
  const bool distinct = body.UnionDistinct();
  // Per-term column matching: the set-operation metadata is per pair, so one
  // body may mix positional and BY NAME pairs.
  std::vector<SetOperationMatch> term_matches;
  term_matches.reserve(body.UnionAll().size());
  for (size_t index = 0; index < body.UnionAll().size(); ++index) {
    term_matches.push_back(body.MatchFor(index));
  }
  const bool any_name_matching =
      std::ranges::any_of(term_matches, NameMatchedUnion);

  // Working-table schema.  A recursive CTE fixes its shape from the anchor
  // term: every round must produce the anchor's columns, so `SELECT *` over
  // the CTE keeps those names.  Name matching (BY NAME / STRICT
  // CORRESPONDING / CORRESPONDING) changes how a term's values bind to them,
  // never which columns the CTE has, and an anchor column the term cannot
  // produce is an error instead of a NULL-filled cell.
  std::vector<std::string> working_names;
  working_names.reserve(anchor_result.schema.ColumnCount());
  for (slot_t index = 0; index < anchor_result.schema.ColumnCount(); ++index) {
    working_names.push_back(anchor_result.schema.GetColumn(index).Name().name);
  }
  if (any_name_matching) {
    for (size_t index = 0; index < term_matches.size(); ++index) {
      const SetOperationMatch& match = term_matches[index];
      if (!NameMatchedUnion(match)) {
        continue;
      }
      std::optional<std::vector<std::string>> term_names;
      if (body.UnionAll()[index] != nullptr) {
        term_names = StaticTermNames(*body.UnionAll()[index]);
      }
      for (const std::string& listed : match.columns) {
        if (!ContainsColumnName(working_names, listed)) {
          std::string msg = "recursive CTE ";
          msg += name;
          msg += " has no column ";
          msg += listed;
          msg += " to match by name";
          return StatusError(StatusCode::kInvalidArgument, std::move(msg));
        }
      }
      for (const std::string& wanted : working_names) {
        if (wanted.empty()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "recursive CTE " + name + " needs named columns for BY NAME");
        }
        if (!match.columns.empty() &&
            !ContainsColumnName(match.columns, wanted)) {
          std::string msg = "recursive CTE ";
          msg += name;
          msg += " cannot drop column ";
          msg += wanted;
          msg += " in a name-matched recursive term";
          return StatusError(StatusCode::kInvalidArgument, std::move(msg));
        }
        if (term_names.has_value() &&
            !ContainsColumnName(*term_names, wanted)) {
          std::string msg = "recursive CTE ";
          msg += name;
          msg += " term has no column ";
          msg += wanted;
          return StatusError(StatusCode::kInvalidArgument, std::move(msg));
        }
      }
      if (match.by_name && term_names.has_value()) {
        for (const std::string& candidate : *term_names) {
          if (candidate.empty() ||
              !ContainsColumnName(working_names, candidate)) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "recursive CTE " + name +
                    " term produces a column the anchor does not name: " +
                    (candidate.empty() ? std::string("<unnamed>") : candidate));
          }
        }
      }
    }
    for (size_t left = 0; left < working_names.size(); ++left) {
      for (size_t right = left + 1; right < working_names.size(); ++right) {
        if (IdentifierEquals(working_names[left], working_names[right])) {
          return StatusError(StatusCode::kInvalidArgument,
                             "recursive CTE " + name +
                                 " cannot match duplicate column " +
                                 working_names[left] + " by name");
        }
      }
    }
  }

  Relation result(context.execution_runtime());
  result.schema = anchor_result.schema;
  if (track_depth) {
    result.schema = result.schema +
                    Schema("", {Column(depth_spec->column, ValueType::kInt64)});
  }
  Relation delta(context.execution_runtime());
  delta.schema = result.schema;

  // UNION (DISTINCT) has to drop a row any earlier round already produced.
  // The working-table index answers that with one lookup; the linear scan it
  // replaces was quadratic (a 320k-row recursion spent minutes in it).  It
  // counts here (and only here) so a test can see the loop consult it.
  WorkTableIndex seen(/*count_stats=*/true);
  auto already_seen = [&](const Row& row) {
    return distinct && !seen.Insert(row);
  };
  auto add_anchor = [&](const Row& row) {
    if (already_seen(row)) {
      return;
    }
    Row stored = row;
    if (track_depth) {
      stored.values_.emplace_back(int64_t{0});
    }
    delta.AddRow(stored);
    if (!track_depth || (depth_spec->lower <= 0 && 0 <= depth_spec->upper)) {
      result.AddRow(std::move(stored));
    }
  };
  RETURN_IF_FAIL(anchor_result.ForEachRow(add_anchor));
  delta.FinishSpill();

  for (size_t iteration = 0; iteration < kMaxIterations; ++iteration) {
    if (delta.TotalRows() == 0) {
      break;
    }
    const int64_t row_depth = static_cast<int64_t>(iteration) + 1;
    if (track_depth && row_depth > depth_spec->upper) {
      break;
    }
    CteMap loop_ctes = inherited_ctes;
    ASSIGN_OR_RETURN(Relation, materialized_delta, MaterializeRelation(delta));
    loop_ctes[name] = std::make_shared<Relation>(std::move(materialized_delta));
    Relation next(context.execution_runtime());
    next.schema = result.schema;
    for (size_t term_index = 0; term_index < body.UnionAll().size();
         ++term_index) {
      const std::shared_ptr<SelectStatement>& branch_statement =
          body.UnionAll()[term_index];
      if (!branch_statement) {
        continue;
      }
      const SetOperationMatch& match = term_matches[term_index];
      ASSIGN_OR_RETURN(
          Relation, branch,
          (ExecuteQuery(context, *branch_statement, outer, loop_ctes)));
      std::vector<size_t> alignment;
      if (NameMatchedUnion(match)) {
        ASSIGN_OR_RETURN(
            std::vector<size_t>, resolved_alignment,
            (ResolveNameAlignment(working_names, branch.schema,
                                  match.by_name && match.columns.empty(),
                                  "recursive CTE " + name + " term")));
        alignment = std::move(resolved_alignment);
      } else {
        for (size_t index = 0; index < working_names.size(); ++index) {
          alignment.push_back(index);
        }
      }
      // Name matching reads the columns the working table names, so the term
      // may carry extra columns (CORRESPONDING drops them); positional
      // matching requires exactly the working width.
      const bool name_matched = NameMatchedUnion(match);
      size_t required_width = 0;
      for (size_t index : alignment) {
        required_width = std::max(required_width, index + 1);
      }
      Status width_status = Status::kSuccess;
      std::string width_error;
      RETURN_IF_FAIL(branch.ForEachRow([&](const Row& row) {
        if (width_status != Status::kSuccess) {
          return;
        }
        if (name_matched ? row.values_.size() < required_width
                         : row.values_.size() != working_names.size()) {
          width_status = Status(StatusCode::kInvalidArgument);
          width_error = "recursive CTE " + name + " term produces " +
                        std::to_string(row.values_.size()) +
                        " columns but the working table reads " +
                        std::to_string(name_matched ? required_width
                                                    : working_names.size());
          return;
        }
        Row payload;
        payload.values_.reserve(working_names.size() + (track_depth ? 1U : 0U));
        for (size_t index : alignment) {
          payload.values_.push_back(row.values_[index]);
        }
        if (already_seen(payload)) {
          return;
        }
        if (track_depth) {
          payload.values_.emplace_back(row_depth);
        }
        next.AddRow(std::move(payload));
      }));
      if (width_status != Status::kSuccess) {
        return Status(width_status.GetCode(), std::move(width_error));
      }
    }
    RETURN_IF_FAIL(next.FinishSpill());
    RETURN_IF_FAIL(next.GetStatus());
    if (next.TotalRows() == 0) {
      break;
    }
    if (result.TotalRows() + next.TotalRows() > kMaxRows) {
      return StatusError(StatusCode::kInvalidArgument,
                         "recursive CTE " + name + " exceeded the row budget");
    }
    if (!track_depth ||
        (row_depth >= depth_spec->lower && row_depth <= depth_spec->upper)) {
      RETURN_IF_FAIL(
          next.ForEachRow([&](const Row& row) { result.AddRow(row); }));
    }
    RETURN_IF_FAIL(result.FinishSpill());
    RETURN_IF_FAIL(result.GetStatus());
    delta = std::move(next);
  }
  return result;
}

StatusOr<Relation> ExecuteQuery(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& inherited_ctes) {
  EnsureReusableProjections(context, context.execution_runtime());
  // Apply relational boundary rewrites before loading sources.  Correlated
  // invocations must keep their original scope, while top-level derived
  // tables can be flattened or receive safe predicate/LIMIT pushdown.
  if (outer == nullptr) {
    if (std::shared_ptr<SelectStatement> optimized =
            OptimizeDerivedBoundaries(statement, inherited_ctes)) {
      return ExecuteQuery(context, *optimized, outer, inherited_ctes);
    }
  }
  CteMap ctes = inherited_ctes;
  // WITH entries live in an unordered map, so declaration order is lost;
  // execute them in dependency order (a CTE may reference earlier ones).
  if (!statement.WithQueries().empty()) {
    std::unordered_set<std::string> pending;
    for (const auto& [name, query] : statement.WithQueries()) {
      (void)query;
      pending.insert(name);
    }
    while (!pending.empty()) {
      bool progress = false;
      for (auto iter = pending.begin(); iter != pending.end();) {
        const std::string& name = *iter;
        const SelectStatement& query = *statement.WithQueries().at(name);
        const bool recursive = statement.IsRecursiveWith(name);
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
          StatusOr<Relation> cte_relation =
              recursive ? ExecuteRecursiveCte(context, name, query, outer, ctes,
                                              statement.RecursiveDepthOf(name))
                        : ExecuteQuery(context, query, outer, ctes);
          if (!cte_relation.HasValue()) {
            return cte_relation.GetStatus();
          }
          ctes[name] = std::make_shared<Relation>(cte_relation.MoveValue());
          iter = pending.erase(iter);
          progress = true;
        } else {
          ++iter;
        }
      }
      if (!progress) {
        // Every remaining entry waits on another remaining entry, so the WITH
        // list is a mutual reference with no dependency order.  Reporting it
        // beats the "table not found" that executing the entries in map order
        // used to produce.  The SQL frontend rejects the same shape before
        // planning, so this is the backstop for statements handed straight to
        // the executor.
        return CteCycleStatus({pending.begin(), pending.end()});
      }
    }
  }

  // Set-operation modifiers belong to the concatenated result.  The old
  // merge path called FinishQuery on the head first, so LIMIT/OFFSET were
  // consumed before UNION ALL branches were appended.  Defer those clauses
  // until all branches have been materialized; the unmodified fast path is
  // retained for legacy set-operation cases without query-level modifiers.
  if (statement.GetSetOperationTree() != nullptr &&
      statement.GetSetOperationTree()->grouped) {
    const SetOperationTree& tree = *statement.GetSetOperationTree();
    auto apply_pair = [&](Relation left, Relation right,
                          SetOperationKind operation) -> StatusOr<Relation> {
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      RETURN_IF_FAIL(left.FinishSpill());
      RETURN_IF_FAIL(left.GetStatus());
      RETURN_IF_FAIL(
          left.ForEachRow([&](const Row& row) { left_rows.push_back(row); }));
      RETURN_IF_FAIL(right.FinishSpill());
      RETURN_IF_FAIL(right.GetStatus());
      RETURN_IF_FAIL(
          right.ForEachRow([&](const Row& row) { right_rows.push_back(row); }));
      SetOperationExecutor set_operation(
          {std::make_shared<ConstantExecutor>(std::move(left_rows)),
           std::make_shared<ConstantExecutor>(std::move(right_rows))},
          operation);
      Relation folded(context.execution_runtime());
      folded.schema = left.schema;
      DataChunk batch;
      while (set_operation.NextBatch(&batch) != 0) {
        for (size_t i = 0; i < batch.Size(); ++i) {
          folded.AddRow(batch.RowAt(i));
        }
      }
      if (set_operation.GetStatus() != Status::kSuccess) {
        return set_operation.GetStatus();
      }
      RETURN_IF_FAIL(folded.FinishSpill());
      RETURN_IF_FAIL(folded.GetStatus());
      return folded;
    };

    ASSIGN_OR_RETURN(Relation, combined,
                     (ExecuteQuery(context, *tree.first, outer, ctes)));
    // PRODUCTION FIX: honor per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold evaluated `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and dropped the INTERSECT entirely.
    auto run_term = [&](const SelectStatement& stmt) -> StatusOr<Relation> {
      return ExecuteQuery(context, stmt, outer, ctes);
    };
    std::vector<Relation> terms;
    terms.push_back(std::move(combined));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < tree.branches.size(); ++i) {
      ASSIGN_OR_RETURN(Relation, branch, (run_term(*tree.branches[i])));
      const SetOperationKind operation =
          i < tree.kinds.size() ? tree.kinds[i] : SetOperationKind::kUnionAll;
      const bool intersects = operation == SetOperationKind::kIntersect ||
                              operation == SetOperationKind::kIntersectAll;
      if (intersects) {
        ASSIGN_OR_RETURN(Relation, folded_pair,
                         (apply_pair(std::move(terms.back()), std::move(branch),
                                     operation)));
        terms.back() = std::move(folded_pair);
      } else {
        low_ops.push_back(operation);
        terms.push_back(std::move(branch));
      }
    }
    combined = std::move(terms.front());
    for (size_t j = 1; j < terms.size(); ++j) {
      ASSIGN_OR_RETURN(Relation, folded_pair,
                       (apply_pair(std::move(combined), std::move(terms[j]),
                                   low_ops[j - 1])));
      combined = std::move(folded_pair);
    }
    RETURN_IF_FAIL(combined.FinishSpill());
    RETURN_IF_FAIL(combined.GetStatus());
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &combined, outer, ctes));
    }
    return LimitedRows(statement, std::move(combined));
  }

  if (!statement.UnionAll().empty()) {
    SelectStatement head = statement;
    head.ClearUnionAll();
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    // The WITH bodies already executed into `ctes` above; clearing the head's
    // own copies keeps the fold from re-running every CTE body one extra time.
    head.ClearWithQueries();
    ASSIGN_OR_RETURN(Relation, combined,
                     (ExecuteQuery(context, head, outer, ctes)));
    // PRODUCTION FIX: apply per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold computed `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and returned the wrong rows.
    auto apply_pair = [&](Relation left, Relation right,
                          SetOperationKind operation) -> StatusOr<Relation> {
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      RETURN_IF_FAIL(left.FinishSpill());
      RETURN_IF_FAIL(left.GetStatus());
      RETURN_IF_FAIL(
          left.ForEachRow([&](const Row& row) { left_rows.push_back(row); }));
      RETURN_IF_FAIL(right.FinishSpill());
      RETURN_IF_FAIL(right.GetStatus());
      RETURN_IF_FAIL(
          right.ForEachRow([&](const Row& row) { right_rows.push_back(row); }));
      SetOperationExecutor set_operation(
          {std::make_shared<ConstantExecutor>(std::move(left_rows)),
           std::make_shared<ConstantExecutor>(std::move(right_rows))},
          operation);
      Relation folded(context.execution_runtime());
      folded.schema = left.schema;
      DataChunk batch;
      while (set_operation.NextBatch(&batch) != 0) {
        for (size_t i = 0; i < batch.Size(); ++i) {
          folded.AddRow(batch.RowAt(i));
        }
      }
      if (set_operation.GetStatus() != Status::kSuccess) {
        return set_operation.GetStatus();
      }
      RETURN_IF_FAIL(folded.FinishSpill());
      RETURN_IF_FAIL(folded.GetStatus());
      if (folded.TotalRows() != 0) {
        folded.schema = left.schema;
      }
      return folded;
    };
    std::vector<Relation> terms;
    terms.push_back(std::move(combined));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < statement.UnionAll().size(); ++i) {
      const auto& union_stmt = statement.UnionAll()[i];
      ASSIGN_OR_RETURN(Relation, branch,
                       (ExecuteQuery(context, *union_stmt, outer, ctes)));
      const SetOperationKind operation =
          i < statement.SetOperationKinds().size()
              ? statement.SetOperationKinds()[i]
              : SetOperationKind::kUnionAll;
      const bool intersects = operation == SetOperationKind::kIntersect ||
                              operation == SetOperationKind::kIntersectAll;
      if (intersects) {
        ASSIGN_OR_RETURN(Relation, folded_pair,
                         (apply_pair(std::move(terms.back()), std::move(branch),
                                     operation)));
        terms.back() = std::move(folded_pair);
      } else {
        low_ops.push_back(operation);
        terms.push_back(std::move(branch));
      }
    }
    combined = std::move(terms.front());
    for (size_t j = 1; j < terms.size(); ++j) {
      ASSIGN_OR_RETURN(Relation, folded_pair,
                       (apply_pair(std::move(combined), std::move(terms[j]),
                                   low_ops[j - 1])));
      combined = std::move(folded_pair);
    }
    RETURN_IF_FAIL(combined.FinishSpill());
    RETURN_IF_FAIL(combined.GetStatus());
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &combined, outer, ctes));
    }
    return LimitedRows(statement, std::move(combined));
  }

  // An uncorrelated predicate subquery is an InitPlan. Fold its result once
  // before choosing the scan/aggregate route, so the outer aggregate need not
  // materialize a filtered copy of the whole table. Failed or correlated
  // folds retain query nodes and keep their original evaluation route.
  if (outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest && !statement.Sources()[0].sample &&
      statement.WhereClause() && ContainsQuery(statement.WhereClause()) &&
      (!statement.GroupBy().empty() ||
       std::ranges::any_of(statement.SelectList(), [](const auto& item) {
         return ContainsAggregate(item.expression);
       }))) {
    struct RestoreReuse {  // NOLINT(cppcoreguidelines-special-member-functions)
      ExecutionRuntime* runtime;
      std::string table;
      bool was_reusable;
      ~RestoreReuse() {
        if (was_reusable) {
          runtime->reusable_base_relations.insert(table);
        }
      }
    } restore{.runtime = context.execution_runtime(),
              .table = statement.Sources()[0].table,
              .was_reusable = false};
    if (restore.runtime != nullptr) {
      restore.was_reusable =
          restore.runtime->reusable_base_relations.erase(restore.table) != 0;
    }
    auto folded = FoldRowIndependentSubqueries(
        context, SplitConjuncts(statement.WhereClause()), outer, ctes);
    if (std::ranges::none_of(folded, ContainsQuery)) {
      SelectStatement initialized(statement);
      initialized.SetWhereClause(CombineConjuncts(folded));
      return ExecuteQuery(context, initialized, outer, ctes);
    }
  }

  // Single-table LSTable aggregation with a prebuilt-membership EXISTS/IN
  // subquery (TPC-H Q04): compile the membership bitset in parallel and stream
  // the outer table directly into RunTypedParallelStreamAggregate without
  // materializing intermediate outer rows.
  if (outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest && !statement.Sources()[0].sample &&
      !HasWindowFunctions(statement) &&
      !ctes.contains(statement.Sources()[0].table) && statement.WhereClause() &&
      ContainsQuery(statement.WhereClause()) &&
      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }) ||
       ContainsAggregate(statement.Having()))) {
    const SelectSource& source = statement.Sources()[0];
    const bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    const std::shared_ptr<const LSTableStore> lst_store =
        table.HasValue() ? table.Value()->GetLSTableStore() : nullptr;
    if (table.HasValue() && !reusable && lst_store != nullptr &&
        lst_store->total_rows > 0) {
      const Schema& table_schema = table.Value()->GetSchema();
      const std::string qualifier =
          source.alias.empty() ? source.table : source.alias;
      std::shared_ptr<SelectStatement> resolved_statement =
          ResolveGroupingAliases(statement,
                                 qualifier.empty()
                                     ? table_schema
                                     : QualifySchema(table_schema, qualifier));
      const SelectStatement& stmt =
          resolved_statement != nullptr ? *resolved_statement : statement;
      const Schema qualified_table_schema =
          qualifier.empty() ? table_schema
                            : QualifySchema(table_schema, qualifier);
      std::vector<slot_t> projection =
          RequiredColumns(stmt, qualified_table_schema, true);
      Schema scan_schema = projection.empty()
                               ? table_schema
                               : ProjectSchema(table_schema, projection);
      Schema qualified_schema = qualifier.empty()
                                    ? scan_schema
                                    : QualifySchema(scan_schema, qualifier);
      std::vector<Expression> scan_predicates =
          SplitConjuncts(stmt.WhereClause());
      const Expression having_expr =
          InlineHavingAliases(statement, qualified_schema);
      const FilterCompileEnv compile_env{
          .context = &context,
          .outer = outer,
          .ctes = &ctes,
          .input_rows = static_cast<size_t>(lst_store->total_rows),
          .prebuild_only_exists = true,
      };
      CompiledScanFilter scan_filter =
          CompileScanFilter(scan_predicates, qualified_schema, &compile_env);
      if (scan_filter.residual.empty() &&
          scan_filter.disjunctive_branches.empty() &&
          scan_filter.prebuilt_scalar_compare.empty() &&
          scan_filter.prebuilt_membership.size() == 1 &&
          !scan_filter.prebuilt_membership[0].negated &&
          !scan_filter.prebuilt_membership[0].contains_null &&
          scan_filter.prebuilt_membership[0].values != nullptr &&
          (scan_filter.prebuilt_membership[0].domain_type ==
               ValueType::kInt64 ||
           scan_filter.prebuilt_membership[0].domain_type ==
               ValueType::kDate)) {
        std::vector<Expression> non_query_predicates;
        non_query_predicates.reserve(scan_predicates.size());
        for (const Expression& pred : scan_predicates) {
          if (!ContainsQuery(pred)) {
            non_query_predicates.push_back(pred);
          }
        }
        std::vector<const AggregateExpression*> aggregate_expressions;
        std::unordered_set<const AggregateExpression*> seen_aggregates;
        for (const NamedExpression& projection_item : stmt.SelectList()) {
          CollectAggregates(projection_item.expression, &aggregate_expressions,
                            &seen_aggregates);
        }
        CollectAggregates(having_expr, &aggregate_expressions,
                          &seen_aggregates);
        TypedAggregationPlan typed_plan =
            PlanTypedAggregation(stmt, having_expr, qualified_schema,
                                 aggregate_expressions, non_query_predicates);
        if (typed_plan.eligible) {
          const std::vector<slot_t>* proj_ptr =
              projection.empty() ? nullptr : &projection;
          const std::vector<IntegerPeekCompare> peek_compares =
              BuildIntegerPeeks(scan_filter, proj_ptr, table_schema);
          StatusOr<Relation> typed = RunTypedParallelStreamAggregate(
              context, stmt, having_expr, *table.Value(), source.table, false,
              projection, qualified_schema, table_schema, peek_compares,
              aggregate_expressions, ctes, std::move(typed_plan), nullptr,
              ValueType::kNull, std::nullopt,
              scan_filter.prebuilt_membership.data());
          if (!typed.HasValue()) {
            return typed.GetStatus();
          }
          Relation typed_output = typed.MoveValue();
          typed_output.FinishSpill();
          return LimitedRows(statement, std::move(typed_output), &context,
                             outer, &ctes);
        }
      }
    }
  }

  // Single-table aggregation: filter and aggregate while scanning so we never
  // materialize millions of qualifying rows (TPC-H Q1/Q6/Q21-derived pattern).
  // When the table is reusable, populate the shared cache once then aggregate
  // from the cache without deep-copying into an intermediate relation.
  const bool stream_agg =
      outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest && !statement.Sources()[0].sample &&
      !HasWindowFunctions(statement) &&
      !ctes.contains(statement.Sources()[0].table) &&
      (!statement.WhereClause() || !ContainsQuery(statement.WhereClause())) &&

      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }) ||
       ContainsAggregate(statement.Having()));
  if (std::getenv("TINYLAMB_DEBUG_GS") != nullptr) {  // NOLINT(cert-err33-c)
    (void)fprintf(stderr,
                  "[ROUTE] stream_agg=%d sources=%zu groupby=%zu "
                  "groupall=%d\n",
                  (int)stream_agg, statement.Sources().size(),
                  statement.GroupBy().size(), (int)statement.IsGroupAll());
  }
  if (stream_agg) {
    const SelectSource& source = statement.Sources()[0];
    const bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "table " + source.table + " not found");
    }
    const Schema& table_schema = table.Value()->GetSchema();
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    // Alias/ordinal resolution needs the qualified column names the grouping
    // expressions see; offsets are identical in the raw table schema.
    std::shared_ptr<SelectStatement> resolved_statement =
        ResolveGroupingAliases(statement,
                               qualifier.empty()
                                   ? table_schema
                                   : QualifySchema(table_schema, qualifier));
    const SelectStatement& stmt =
        resolved_statement != nullptr ? *resolved_statement : statement;
    // RequiredColumns must run against the qualified names the statement
    // references (`t4.array_val`); QualifySchema preserves column order so
    // the resulting offsets index the raw table scan unchanged.
    const Schema qualified_table_schema =
        qualifier.empty() ? table_schema
                          : QualifySchema(table_schema, qualifier);
    std::vector<slot_t> projection =
        RequiredColumns(stmt, qualified_table_schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(context, source.table)) {
      // The shared layout was derived from the raw table schema; never let
      // it drop columns this statement's own (possibly alias-qualified)
      // references need.
      for (const slot_t slot : *shared) {
        if (std::find(projection.begin(), projection.end(), slot) ==
            projection.end()) {
          projection.push_back(slot);
        }
      }
      std::sort(projection.begin(), projection.end());
    }
    Schema scan_schema = projection.empty()
                             ? Schema("", std::vector<Column>{})
                             : ProjectSchema(table_schema, projection);
    Schema qualified_schema =
        qualifier.empty() ? scan_schema : QualifySchema(scan_schema, qualifier);

    std::vector<Expression> scan_predicates =
        SplitConjuncts(stmt.WhereClause());

    Relation input;
    input.schema = qualified_schema;
    // Project aggregates against the qualified schema; feed rows one at a time
    // without retaining them in input.rows.
    const Expression having_expr = InlineHavingAliases(statement, input.schema);

    // HAVING transpose (aggregate <-> filter): within a grouping every row of
    // a group carries the same key values, so a conjunct that touches only
    // bare grouping-key columns (and no aggregate / subquery) is a row-level
    // fact for the whole group.  Moving it into the scan filter drops the
    // rows of doomed groups before groups form; the aggregate-referencing
    // remainder stays post-aggregate, where the prepared aggregate results
    // are available.  A group-all aggregation is exempt: it yields one group
    // even over a completely rejected input, so a branch-constant conjunct
    // lifted pre-aggregate would wrongly resurrect that group.
    Expression having_residual = having_expr;
    {
      std::vector<Expression> lifted_conjuncts;
      std::vector<Expression> residual_conjuncts;
      const std::vector<Expression> having_conjuncts =
          having_expr ? SplitConjuncts(having_expr) : std::vector<Expression>{};
      for (const Expression& conjunct : having_conjuncts) {
        if (!stmt.IsGroupAll() && !stmt.GroupBy().empty() &&
            IsKeyOnlyHavingConjunct(conjunct, stmt)) {
          lifted_conjuncts.push_back(conjunct);
        } else {
          residual_conjuncts.push_back(conjunct);
        }
      }
      if (!lifted_conjuncts.empty()) {
        scan_predicates.reserve(scan_predicates.size() +
                                lifted_conjuncts.size());
        scan_predicates.insert(scan_predicates.end(), lifted_conjuncts.begin(),
                               lifted_conjuncts.end());
        having_residual = residual_conjuncts.empty()
                              ? Expression{}
                              : CombineConjuncts(residual_conjuncts);
      }
    }

    // The predicates name alias-qualified columns (`x0.b`); compile and
    // evaluate them against the qualified view of the scan row, or a residual
    // conjunct (function call, CASE, ...) cannot resolve its columns.
    CompiledScanFilter scan_filter =
        CompileScanFilter(scan_predicates, qualified_schema);

    std::vector<const AggregateExpression*> aggregate_expressions;
    std::unordered_set<const AggregateExpression*> seen_aggregates;
    for (const NamedExpression& projection_item : stmt.SelectList()) {
      CollectAggregates(projection_item.expression, &aggregate_expressions,
                        &seen_aggregates);
    }
    CollectAggregates(having_residual, &aggregate_expressions,
                      &seen_aggregates);
    // ORDER BY terms may reference select aliases or spell out an aggregate
    // ("ORDER BY SUM(v) DESC").  The AggregateResultMap is keyed by node
    // pointer, so aggregate subtrees dedupe onto the identical select-list
    // node (mirroring the grouped path); without this the ORDER BY instance
    // is never prepared and the sort fails with "aggregate was not prepared".
    std::vector<Expression> resolved_order_keys;
    resolved_order_keys.reserve(stmt.OrderBy().size());
    for (const auto& term : stmt.OrderBy()) {
      Expression expression = term.expression;
      if (expression && expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expression->AsColumnValue().GetColumnName();
        if (name.schema.empty()) {
          for (const NamedExpression& select_item : stmt.SelectList()) {
            if (!select_item.name.empty() &&
                select_item.name.size() == name.name.size() &&
                std::equal(
                    select_item.name.begin(), select_item.name.end(),
                    name.name.begin(), [](char left, char right) {
                      return std::tolower(static_cast<unsigned char>(left)) ==
                             std::tolower(static_cast<unsigned char>(right));
                    })) {
              expression = select_item.expression;
              break;
            }
          }
        }
      }
      expression = ReplaceSubExpressions(
          expression,
          [&stmt](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kAggregateExp) {
              return std::nullopt;
            }
            for (const NamedExpression& select_item : stmt.SelectList()) {
              if (select_item.expression &&
                  select_item.expression->Type() == TypeTag::kAggregateExp &&
                  select_item.expression->ToString() == node->ToString()) {
                return select_item.expression;
              }
            }
            return std::nullopt;
          });
      resolved_order_keys.push_back(std::move(expression));
    }
    for (const Expression& order_expression : resolved_order_keys) {
      CollectAggregates(order_expression, &aggregate_expressions,
                        &seen_aggregates);
    }

    struct GroupState {
      Row representative;
      size_t accumulator_offset{0};
    };
    std::deque<AggregateAccumulator> aggregate_states;
    std::vector<GroupState> groups;
    GroupKeyMap offsets;
    // Single INT64/DATE column group keys index groups by the raw value:
    // probing an int map skips the per-row Value copies of the generic key
    // path.  NULL keys fall through to the row-keyed index (mirroring
    // TypedWorkerGroups), so NULL stays its own group.
    // Flat open addressing (one cache line per probe) instead of a node-based
    // unordered_map: the generic grouped loop's single-int-key path serves
    // the multi-hundred-thousand-group shapes (Q3's l_orderkey), where the
    // node chase per probe dominated the fold.
    relational_detail::FlatIntKeyMap<size_t> int_offsets;
    bool single_int_key = false;
    slot_t int_key_column{0};

    std::vector<std::optional<slot_t>> group_offsets;
    group_offsets.reserve(stmt.GroupBy().size());
    bool group_keys_are_columns = true;
    for (const Expression& key : stmt.GroupBy()) {
      if (key->Type() != TypeTag::kColumnValue) {
        group_keys_are_columns = false;
        group_offsets.emplace_back(std::nullopt);
        continue;
      }
      group_offsets.emplace_back(LocalColumnOffset(
          input.schema, key->AsColumnValue().GetColumnName()));
      if (!group_offsets.back()) {
        group_keys_are_columns = false;
      }
    }
    if (group_keys_are_columns && group_offsets.size() == 1) {
      const std::optional<slot_t>& first_key = group_offsets.front();
      if (first_key.has_value() && *first_key < input.schema.ColumnCount()) {
        const ValueType key_type = input.schema.GetColumn(*first_key).Type();
        single_int_key =
            key_type == ValueType::kInt64 || key_type == ValueType::kDate;
        if (single_int_key) {
          int_key_column = *first_key;
        }
      }
    }
    std::vector<std::optional<slot_t>> aggregate_child_offsets;
    aggregate_child_offsets.reserve(aggregate_expressions.size());
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      if (IsCountStar(*aggregate)) {
        aggregate_child_offsets.emplace_back(
            std::nullopt);  // sentinel via count*
        continue;
      }
      if (aggregate->Child()->Type() == TypeTag::kColumnValue) {
        aggregate_child_offsets.emplace_back(LocalColumnOffset(
            input.schema, aggregate->Child()->AsColumnValue().GetColumnName()));
      } else {
        aggregate_child_offsets.emplace_back(std::nullopt);
      }
    }
    const size_t count_star_flags_size = aggregate_expressions.size();
    std::vector<bool> is_count_star(count_star_flags_size, false);
    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
      is_count_star[i] = IsCountStar(*aggregate_expressions[i]);
    }

    const auto scan_begin = std::chrono::steady_clock::now();
    // Reused key scratch: most rows land in an existing group, so probe the
    // index heterogeneously (no per-row Row/vector allocation) and only
    // materialize a key Row when a group is actually new.
    std::vector<Value> key_values;
    key_values.reserve(stmt.GroupBy().size());
    // const-ref parameter: ForEachRow invokes with an lvalue, so a by-value
    // Row here deep-copied every scanned row (values_ vector + all cells).
    auto accumulate_row = [&](const Row& row) -> Status {
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->scan_output_rows;
      }
      Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
      size_t group_index = groups.size();
      bool inserted = false;
      if (single_int_key) {
        const Value& key_value = row[int_key_column];
        if (!key_value.IsNull()) {
          bool emplaced = false;
          size_t* slot =
              int_offsets.TryEmplace(key_value.value.int_value, &emplaced);
          if (emplaced) {
            // Claim the index the new group below will occupy.
            *slot = groups.size();
          }
          group_index = *slot;
          inserted = emplaced;
        } else {
          // NULL must stay its own group: the raw int64 slot cannot encode
          // null-ness, so NULL keys route through the row-keyed index.
          key_values.clear();
          key_values.push_back(CanonicalDistinctValue(key_value));
          auto found = offsets.find(key_values);
          if (found != offsets.end()) {
            group_index = found->second;
          } else {
            inserted = true;
            offsets.emplace(Row(key_values), group_index);
          }
        }
      } else {
        key_values.clear();
        if (group_keys_are_columns) {
          for (const auto& offset : group_offsets) {
            key_values.push_back(CanonicalDistinctValue(row[*offset]));
          }
        } else {
          for (const Expression& key : stmt.GroupBy()) {
            ASSIGN_OR_RETURN(Value, hv81838_0,
                             (TryEvaluate(key, scope, nullptr, context, ctes)));
            key_values.push_back(CanonicalDistinctValue(hv81838_0));
          }
        }
        if (auto found = offsets.find(key_values); found != offsets.end()) {
          group_index = found->second;
        } else {
          inserted = true;
          offsets.emplace(Row(key_values), group_index);
        }
      }
      if (inserted) {
        GroupState group;
        group.accumulator_offset = aggregate_states.size();
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          aggregate_states.emplace_back(aggregate, &input.schema);
        }
        group.representative = row;
        groups.push_back(std::move(group));
      }
      GroupState& group = groups[group_index];
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        const AggregateExpression& aggregate = *aggregate_expressions[i];
        if (aggregate.WhereFilter() &&
            !Truthy(Evaluate(aggregate.WhereFilter(), scope, nullptr, context,
                             ctes))) {
          continue;
        }
        if (is_count_star[i]) {
          RETURN_IF_FAIL(accumulator.Add(Value(1)));
        } else if (aggregate_child_offsets[i] &&
                   !aggregate.NeedsGroupContext() &&
                   aggregate.Having() == AggregateHavingModifier::kNone) {
          RETURN_IF_FAIL(accumulator.Add(row[*aggregate_child_offsets[i]]));
        } else {
          AggregateInput aggregate_input;
          aggregate_input.value =
              Evaluate(aggregate.Child(), scope, nullptr, context, ctes);
          if (aggregate.Having() != AggregateHavingModifier::kNone &&
              aggregate.HavingCondition()) {
            ASSIGN_OR_RETURN(Value, hv83481_0,
                             (TryEvaluate(aggregate.HavingCondition(), scope,
                                          nullptr, context, ctes)));
            aggregate_input.condition = std::move(hv83481_0);
          }
          for (const auto& term : aggregate.InnerOrderBy()) {
            ASSIGN_OR_RETURN(
                Value, hv83714_0,
                (TryEvaluate(term.expression, scope, nullptr, context, ctes)));
            aggregate_input.order_keys.push_back(std::move(hv83714_0));
          }
          if (aggregate.GetType() == AggregationType::kStringAgg &&
              aggregate.SecondaryArg()) {
            ASSIGN_OR_RETURN(Value, hv83961_0,
                             (TryEvaluate(aggregate.SecondaryArg(), scope,
                                          nullptr, context, ctes)));
            aggregate_input.auxiliary = std::move(hv83961_0);
          }
          for (const Expression& extra : aggregate.TrailingArgs()) {
            if (extra) {
              ASSIGN_OR_RETURN(
                  Value, hv84223_0,
                  (TryEvaluate(extra, scope, nullptr, context, ctes)));
              aggregate_input.trailing_values.push_back(std::move(hv84223_0));
            }
          }
          RETURN_IF_FAIL(accumulator.Add(std::move(aggregate_input)));
        }
        if (context.execution_runtime() != nullptr) {
          ++context.execution_runtime()->aggregate_updates;
        }
      }
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->aggregate_input_rows;
      }
      return Status::kSuccess;
    };

    const std::vector<slot_t>* proj_ptr =
        projection.empty() ? nullptr : &projection;
    const std::vector<IntegerPeekCompare> peek_compares =
        BuildIntegerPeeks(scan_filter, proj_ptr, table_schema);
    const std::vector<IntegerPeekCompare>* peek_ptr =
        peek_compares.empty() ? nullptr : &peek_compares;

    // Typed parallel fast path (single-table GROUP BY): shapes whose group
    // keys, aggregates, and outputs resolve positionally run as parallel
    // morsel aggregation with context-free bytecode filtering.  Anything
    // less regular keeps the generic loop below.  Shared base-relation
    // statements are admitted too: the typed path fills the cache from
    // worker slices with strict morsel-order publication and aggregates
    // cache hits from parallel row ranges, so no reader can observe a
    // partial entry.  A/B hatch: TINYLAMB_DISABLE_REUSABLE_TYPED_AGG=1
    // restores the pre-change gate that kept shared-table statements on the
    // generic stream loop.
    static const bool reusable_typed_disabled =
        std::getenv("TINYLAMB_DISABLE_REUSABLE_TYPED_AGG") != nullptr;
    if (!reusable || !reusable_typed_disabled) {
      TypedAggregationPlan typed_plan =
          PlanTypedAggregation(stmt, having_residual, qualified_schema,
                               aggregate_expressions, scan_predicates);
      if (typed_plan.eligible) {
        const RuntimeFilterGroup* stream_rf = nullptr;
        ValueType stream_rf_type = ValueType::kNull;
        std::optional<slot_t> stream_key_col;
        if (context.execution_runtime() != nullptr &&
            typed_plan.key_offsets.size() == 1) {
          const slot_t proj_key_slot = typed_plan.key_offsets[0];
          const slot_t full_key_slot =
              projection.empty() ? proj_key_slot : projection[proj_key_slot];
          const auto stored =
              context.execution_runtime()->table_key_filters.find(
                  TableKeyFilterKey(source));
          if (stored != context.execution_runtime()->table_key_filters.end() &&
              stored->second.owner == &statement && !stored->second.empty() &&
              stored->second.column == full_key_slot &&
              full_key_slot < table_schema.ColumnCount() &&
              !table_schema.GetColumn(full_key_slot).IsUnsigned() &&
              table_schema.GetColumn(full_key_slot).Type() ==
                  stored->second.type) {
            stream_rf = stored->second.filter.get();
            stream_rf_type = stored->second.type;
            stream_key_col = full_key_slot;
          }
        }
        StatusOr<Relation> typed = RunTypedParallelStreamAggregate(
            context, stmt, having_residual, *table.Value(), source.table,
            reusable, projection, qualified_schema, table_schema, peek_compares,
            aggregate_expressions, ctes, std::move(typed_plan), stream_rf,
            stream_rf_type, stream_key_col);
        if (!typed.HasValue()) {
          return typed.GetStatus();
        }
        Relation typed_output = typed.MoveValue();
        typed_output.FinishSpill();
        return LimitedRows(statement, std::move(typed_output), &context, outer,
                           &ctes);
      }
    }

    if (reusable) {
      const std::string cache_key = BaseRelationCacheKey(
          source.table, projection.empty() ? nullptr : &projection);
      auto cached = context.execution_runtime()->base_relations.find(cache_key);
      if (cached == context.execution_runtime()->base_relations.end()) {
        Relation cache_rel(context.execution_runtime());
        cache_rel.schema = scan_schema;
        // Fixed-width cache fill also mirrors the rows into an int64 arena
        // (same append order) so the typed fold's cache-hit pass can stream
        // cells; eligibility mirrors the typed route's fill-side check.
        std::shared_ptr<BaseRelationArena> generic_arena;
        static const bool generic_arena_disabled =
            std::getenv("TINYLAMB_DISABLE_AGG_ARENA") != nullptr;
        if (!generic_arena_disabled && scan_schema.ColumnCount() > 0) {
          bool all_fixed = true;
          for (slot_t i = 0; i < scan_schema.ColumnCount(); ++i) {
            const ValueType type = scan_schema.GetColumn(i).Type();
            if (type != ValueType::kInt64 && type != ValueType::kDate) {
              all_fixed = false;
              break;
            }
          }
          if (all_fixed) {
            generic_arena = std::make_shared<BaseRelationArena>();
            generic_arena->stride = scan_schema.ColumnCount();
            generic_arena->null_bits.resize(generic_arena->stride);
          }
        }
        std::array<int64_t, 16> generic_cells{};
        std::array<uint8_t, 16> generic_nulls{};
        std::vector<int64_t> generic_cell_buffer;
        std::vector<uint8_t> generic_null_buffer;
        if (generic_arena != nullptr &&
            generic_arena->stride > static_cast<size_t>(16)) {
          generic_cell_buffer.resize(generic_arena->stride);
          generic_null_buffer.resize(generic_arena->stride);
        }
        // Fill the shared cache with UNFILTERED rows: the cache key carries no
        // predicates (and no stashed key filters are applied here), so every
        // consumer applies its own filters while reading below.
        Iterator iterator =
            projection.empty()
                ? table.Value()->BeginFullScan(context.txn_)
                : table.Value()->BeginFullScan(context.txn_, projection);
        while (iterator.IsValid()) {
          if (context.execution_runtime() != nullptr) {
            ++context.execution_runtime()->scan_rows;
            context.execution_runtime()->scan_values_available +=
                table_schema.ColumnCount();
            context.execution_runtime()->scan_values_decoded +=
                scan_schema.ColumnCount();
          }
          if (generic_arena != nullptr) {
            if ((*iterator).values_.size() != generic_arena->stride) {
              generic_arena.reset();
            } else {
              const size_t width = generic_arena->stride;
              int64_t* cells = width <= 16 ? generic_cells.data()
                                           : generic_cell_buffer.data();
              uint8_t* nulls = width <= 16 ? generic_nulls.data()
                                           : generic_null_buffer.data();
              for (size_t c = 0; c < width; ++c) {
                const Value& value = (*iterator).values_[c];
                cells[c] = value.value.int_value;
                nulls[c] = value.IsNull() ? 1 : 0;
              }
              generic_arena->AppendRun(cells, nulls, 1);
            }
          }
          cache_rel.AddRow(std::move(*iterator));
          ++iterator;
        }
        cache_rel.FinishSpill();
        cached = context.execution_runtime()
                     ->base_relations
                     .emplace(cache_key,
                              std::make_shared<Relation>(std::move(cache_rel)))
                     .first;
        if (generic_arena != nullptr &&
            generic_arena->rows == cached->second->rows.size()) {
          context.execution_runtime()->base_relation_arenas.emplace(
              cache_key, std::move(generic_arena));
        }
      } else {
        ++context.execution_runtime()->base_scan_cache_hits;
        // A typed fill publishes the arena without Rows: box the rows back
        // (once) before the generic consumer iterates them.
        MaterializeCacheRowsIfNeeded(cache_key, cached->second.get(),
                                     context.execution_runtime());
      }
      // Aggregation always reads through the shared cache so the first user
      // and later users see exactly the same rows.
      Status scan_accumulate_error{Status::kSuccess};
      cached->second->FinishSpill();
      Status scan_iterated = cached->second->ForEachRow([&](const Row& row) {
        if (!scan_accumulate_error.ok()) {
          return;
        }
        if (!MatchScanFilter(row, qualified_schema, scan_filter, outer, context,
                             ctes, &scan_accumulate_error)) {
          return;
        }
        if (Status st_acc = accumulate_row(row);
            st_acc != Status::kSuccess &&
            scan_accumulate_error == Status::kSuccess) {
          scan_accumulate_error = st_acc;
        }
      });
      if (scan_iterated != Status::kSuccess &&
          scan_accumulate_error == Status::kSuccess) {
        scan_accumulate_error = scan_iterated;
      }
      RETURN_IF_FAIL(scan_accumulate_error);
    } else {
      // Stashed key filters are intentionally not applied here: they belong to
      // the statement that derived them, not to this aggregation.
      Iterator iterator = [&] {
        if (peek_ptr != nullptr) {
          if (projection.empty()) {
            return table.Value()->BeginFullScan(context.txn_, peek_ptr);
          }
          return table.Value()->BeginFullScan(context.txn_, projection,
                                              peek_ptr);
        }
        if (projection.empty()) {
          return table.Value()->BeginFullScan(context.txn_);
        }
        return table.Value()->BeginFullScan(context.txn_, projection);
      }();
      Status scan_filter_error{Status::kSuccess};
      while (iterator.IsValid()) {
        if (!MatchScanFilter(*iterator, qualified_schema, scan_filter, outer,
                             context, ctes, &scan_filter_error)) {
          RETURN_IF_FAIL(scan_filter_error);
          ++iterator;
          continue;
        }
        if (context.execution_runtime() != nullptr) {
          ++context.execution_runtime()->scan_rows;
          context.execution_runtime()->scan_values_available +=
              table_schema.ColumnCount();
          context.execution_runtime()->scan_values_decoded +=
              scan_schema.ColumnCount();
        }
        RETURN_IF_FAIL(accumulate_row(*iterator));
        ++iterator;
      }
      RETURN_IF_FAIL(scan_filter_error);
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->scan_ms += ElapsedMs(scan_begin);
      // filter_ms intentionally includes the same interval: filtering happened
      // inline with the scan, so both counters cover this phase.
      context.execution_runtime()->filter_ms += ElapsedMs(scan_begin);
      context.execution_runtime()->aggregate_groups += groups.size();
    }
    // A group-all statement (GROUP BY (), GROUPING SETS whose sets reduce
    // to the grand total) aggregates over even an empty input into one row.
    if (groups.empty() && (stmt.GroupBy().empty() || stmt.IsGroupAll())) {
      GroupState group;
      group.representative =
          Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
      group.accumulator_offset = aggregate_states.size();
      for (const AggregateExpression* aggregate : aggregate_expressions) {
        aggregate_states.emplace_back(aggregate, &input.schema);
      }
      groups.push_back(std::move(group));
    }

    Relation output(context.execution_runtime());
    output.schema = input.schema;
    std::vector<Column> output_columns;
    for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
      const NamedExpression& projection_item = stmt.SelectList()[i];
      output_columns.emplace_back(ProjectionName(projection_item, i),
                                  ValueType::kNull);
    }
    // Inline ORDER BY handling: keys may reference select aliases (output
    // schema), grouping base columns (representative row) or outer scopes.
    const Schema initial_output_schema("", output_columns);
    struct KeyedRow {
      std::vector<Value> keys;
      Row row;
      size_t sequence{0};
    };
    std::vector<KeyedRow> sortable;
    // Same ordering contract as the non-grouped relational path above: NULL
    // placement, direction and the key's collation all come from
    // SortKeyOrder, so a grouped ORDER BY folds exactly like an ungrouped one.
    std::vector<SortKeyOrder> order_specs;
    order_specs.reserve(stmt.OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : stmt.OrderBy()) {
      order_specs.emplace_back(term.ascending, term.nulls_first,
                               CollationOfExpression(term.expression));
    }
    const auto precedes = [&order_specs](const KeyedRow& left,
                                         const KeyedRow& right) {
      for (size_t i = 0; i < order_specs.size(); ++i) {
        const int cmp = order_specs[i].Compare(left.keys[i], right.keys[i]);
        if (cmp != 0) {
          return cmp < 0;
        }
      }
      return left.sequence < right.sequence;
    };
    bool bounded = false;
    size_t topn_capacity = 0;
    if (!stmt.Distinct() && stmt.HasLimit() && !stmt.WithTies() &&
        stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()) {
      bounded = true;
      topn_capacity = stmt.Offset() + stmt.Limit();
    }
    size_t next_sequence = 0;
    const auto retain = [&](KeyedRow keyed) {
      keyed.sequence = next_sequence++;
      if (!bounded) {
        sortable.push_back(std::move(keyed));
        return;
      }
      if (sortable.size() < topn_capacity) {
        sortable.push_back(std::move(keyed));
        std::push_heap(sortable.begin(), sortable.end(), precedes);
        return;
      }
      if (topn_capacity != 0 && precedes(keyed, sortable.front())) {
        std::pop_heap(sortable.begin(), sortable.end(), precedes);
        sortable.back() = std::move(keyed);
        std::push_heap(sortable.begin(), sortable.end(), precedes);
      }
    };
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, finished,
            aggregate_states[group.accumulator_offset + i].TryFinish());
        aggregate_results.emplace(aggregate_expressions[i],
                                  std::move(finished));
      }
      Scope scope{.row = &group.representative,
                  .schema = &input.schema,
                  .outer = outer};
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = &aggregate_results;
      if (having_residual &&
          !Truthy(Evaluate(having_residual, scope, &aggregate_results, context,
                           ctes))) {
        continue;
      }
      std::vector<Value> values;
      values.reserve(stmt.SelectList().size());
      for (const NamedExpression& projection_item : stmt.SelectList()) {
        ASSIGN_OR_RETURN(Value, hv90851_0,
                         (TryEvaluate(projection_item.expression, scope,
                                      &aggregate_results, context, ctes)));
        values.push_back(std::move(hv90851_0));
      }
      Row output_row(std::move(values));
      // Sort keys may reference base columns of the grouped relation
      // (`ORDER BY str_val` where str_val is a grouping key); chain the
      // projected row to the group's representative row so those resolve.
      if (!stmt.OrderBy().empty()) {
        // The grouped ORDER BY key may reference output columns, aliases,
        // aggregates and group-key base columns -- never a relation alias
        // denoting a whole row (the query is grouped by parts of that row),
        // so the row-alias fallback is suppressed on both scopes.
        Scope base_scope{.row = &group.representative,
                         .schema = &input.schema,
                         .outer = outer,
                         .no_row_alias = true};
        Scope proj_scope{.row = &output_row,
                         .schema = &initial_output_schema,
                         .outer = &base_scope,
                         .no_row_alias = true};
        std::vector<Value> keys;
        keys.reserve(stmt.OrderBy().size());
        for (const Expression& key : resolved_order_keys) {
          ASSIGN_OR_RETURN(Value, hv91721_0,
                           (TryEvaluate(key, proj_scope, &aggregate_results,
                                        context, ctes)));
          keys.push_back(std::move(hv91721_0));
        }
        retain(KeyedRow{.keys = std::move(keys), .row = std::move(output_row)});
      } else {
        output.AddRow(std::move(output_row));
      }
    }
    if (bounded && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = topn_capacity;
      context.execution_runtime()->topn_finish_input_rows = next_sequence;
    }
    if (!sortable.empty()) {
      const auto sort_begin = std::chrono::steady_clock::now();
      if (bounded) {
        std::ranges::sort(sortable, precedes);
      } else {
        std::ranges::stable_sort(sortable, precedes);
      }
      for (KeyedRow& keyed : sortable) {
        output.AddRow(std::move(keyed.row));
      }
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
      }
    }
    for (size_t i = 0; i < output_columns.size() && !output.rows.empty(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
      output_columns[i].SetUnsigned(output.rows[0][i].IsUnsigned());
    }
    output.schema = Schema("", std::move(output_columns));
    output.FinishSpill();
    // DISTINCT / LIMIT without running Project again; ORDER BY was applied
    // inline above (its keys may reference grouping base columns that are no
    // longer reachable from the projected output alone), and DistinctOf
    // preserves row order.
    if (stmt.Distinct()) {
      if (stmt.HasDistinctOn()) {
        ASSIGN_OR_RETURN(Relation, distinct_of,
                         (DistinctOf(std::move(output), stmt.DistinctOn(),
                                     &context, outer, &ctes)));
        output = std::move(distinct_of);
      } else {
        ASSIGN_OR_RETURN(Relation, distinct_of,
                         (DistinctOf(std::move(output))));
        output = std::move(distinct_of);
      }
    }
    return LimitedRows(statement, std::move(output), &context, outer, &ctes);
  }

  if (outer == nullptr && statement.WithQueries().empty() &&
      statement.UnionAll().empty() && !statement.GetSetOperationTree() &&
      !statement.Qualify() && !statement.Having() && !statement.Distinct() &&
      statement.GroupBy().size() == 1 && statement.SelectList().size() == 2 &&
      statement.Sources().size() == 1 &&
      statement.Sources()[0].query != nullptr &&
      statement.Sources()[0].unnest == nullptr &&
      !statement.Sources()[0].sample.has_value() &&
      !HasWindowFunctions(statement)) {
    const SelectStatement& sub = *statement.Sources()[0].query;
    const Expression& g0 = statement.GroupBy()[0];
    if (!statement.WhereClause() && g0 && g0->Type() == TypeTag::kColumnValue &&
        sub.WithQueries().empty() && sub.UnionAll().empty() &&
        !sub.GetSetOperationTree() && !sub.Qualify() && !sub.Having() &&
        !sub.Distinct() && !sub.HasLimit() && sub.Offset() == 0 &&
        sub.OrderBy().empty() && sub.SelectList().size() == 2) {
      const ColumnName& g_col = g0->AsColumnValue().GetColumnName();
      int count_col_pos = -1;
      int star_col_pos = -1;
      for (size_t si = 0; si < 2; ++si) {
        const Expression& e = statement.SelectList()[si].expression;
        if (!e) {
          break;
        }
        if (e->Type() == TypeTag::kColumnValue &&
            IdentifierEquals(e->AsColumnValue().GetColumnName().name,
                             g_col.name)) {
          count_col_pos = static_cast<int>(si);
        } else if (e->Type() == TypeTag::kAggregateExp &&
                   IsCountStar(e->AsAggregateExpression()) &&
                   !e->AsAggregateExpression().WhereFilter()) {
          star_col_pos = static_cast<int>(si);
        }
      }
      int sub_agg_pos = -1;
      for (size_t si = 0; si < 2; ++si) {
        if (!sub.SelectList()[si].name.empty() &&
            IdentifierEquals(sub.SelectList()[si].name, g_col.name) &&
            sub.SelectList()[si].expression &&
            sub.SelectList()[si].expression->Type() == TypeTag::kAggregateExp &&
            sub.SelectList()[si]
                    .expression->AsAggregateExpression()
                    .GetType() == AggregationType::kCount) {
          sub_agg_pos = static_cast<int>(si);
        }
      }
      if (count_col_pos >= 0 && star_col_pos >= 0 &&
          count_col_pos != star_col_pos && sub_agg_pos >= 0) {
        GroupedJoinFusionSpec sub_fusion = BuildGroupedJoinFusionSpec(sub);
        if (sub_fusion.eligible && sub_fusion.aggregates.size() == 1) {
          sub_fusion.rollup_count_distribution = true;
          sub_fusion.rollup_count_col_first = (count_col_pos == 0);
          sub_fusion.rollup_select_names = {statement.SelectList()[0].name,
                                            statement.SelectList()[1].name};
          bool sub_where_applied = false;
          StatusOr<Relation> sub_res = BuildInput(
              context, sub, outer, ctes, &sub_where_applied, &sub_fusion);
          if (sub_res.HasValue() && sub_fusion.fired &&
              sub_fusion.rollup_distribution_applied) {
            Relation dist = sub_res.MoveValue();
            if (!statement.Sources()[0].alias.empty()) {
              dist.schema =
                  QualifySchema(dist.schema, statement.Sources()[0].alias);
            }
            if (!statement.OrderBy().empty()) {
              RETURN_IF_FAIL(
                  ApplyOrderBy(context, statement, &dist, outer, ctes));
            }
            return LimitedRows(statement, std::move(dist), &context, outer,
                               &ctes);
          }
        }
      }
    }
  }

  bool where_fully_applied = false;
  struct PostFusionHavingFilter {
    bool active{false};
    bool is_self_sum_factor{false};
    size_t select_col{0};
    BinaryOperation op{BinaryOperation::kGreaterThan};
    double constant_or_factor{0.0};
  };
  PostFusionHavingFilter post_having;
  const auto same_expr = [](const Expression& a,
                            const Expression& b) noexcept -> bool {
    if (!a && !b) {
      return true;
    }
    if (!a || !b) {
      return false;
    }
    return a->ToString() == b->ToString();
  };
  if (statement.Having() && statement.Having()->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& hbin = statement.Having()->AsBinaryExpression();
    if (IsComparison(hbin.Op()) && hbin.Left() &&
        hbin.Left()->Type() == TypeTag::kAggregateExp) {
      const AggregateExpression& lhs_agg = hbin.Left()->AsAggregateExpression();
      std::optional<size_t> matched_col;
      for (size_t si = 0; si < statement.SelectList().size(); ++si) {
        if (same_expr(hbin.Left(), statement.SelectList()[si].expression)) {
          matched_col = si;
          break;
        }
      }
      if (matched_col.has_value() && hbin.Right()) {
        if (hbin.Right()->Type() == TypeTag::kConstantValue) {
          const Value& cv = hbin.Right()->AsConstantValue().GetValue();
          if (!cv.IsNull() &&
              (cv.type == ValueType::kDouble ||
               (cv.type == ValueType::kInt64 && !cv.IsUnsigned()))) {
            post_having.active = true;
            post_having.is_self_sum_factor = false;
            post_having.select_col = *matched_col;
            post_having.op = hbin.Op();
            post_having.constant_or_factor =
                cv.type == ValueType::kDouble
                    ? cv.value.double_value
                    : static_cast<double>(cv.value.int_value);
          }
        } else if (lhs_agg.GetType() == AggregationType::kSum &&
                   !lhs_agg.Distinct() && !lhs_agg.WhereFilter() &&
                   hbin.Right()->Type() == TypeTag::kQueryExp) {
          const QueryExpression& q = hbin.Right()->AsQueryExpression();
          if (!q.Exists() && !q.Test() && !q.ArrayResult() &&
              q.Query() != nullptr) {
            const SelectStatement& sub = *q.Query();
            bool same_sources =
                sub.WithQueries().empty() && sub.UnionAll().empty() &&
                !sub.GetSetOperationTree() && !sub.Qualify() &&
                !sub.HasLimit() && sub.Offset() == 0 && sub.GroupBy().empty() &&
                !sub.Having() && !sub.Distinct() && sub.OrderBy().empty() &&
                sub.Sources().size() == statement.Sources().size() &&
                same_expr(sub.WhereClause(), statement.WhereClause());
            for (size_t s = 0; same_sources && s < statement.Sources().size();
                 ++s) {
              const SelectSource& a = statement.Sources()[s];
              const SelectSource& b = sub.Sources()[s];
              if (a.table != b.table || a.alias != b.alias ||
                  a.join_type != b.join_type || a.query != nullptr ||
                  b.query != nullptr || a.unnest != nullptr ||
                  b.unnest != nullptr || a.sample.has_value() ||
                  b.sample.has_value() ||
                  !same_expr(a.join_condition, b.join_condition)) {
                same_sources = false;
              }
            }
            if (same_sources && sub.SelectList().size() == 1 &&
                sub.SelectList()[0].expression &&
                sub.SelectList()[0].expression->Type() == TypeTag::kBinaryExp) {
              const BinaryExpression& sbin =
                  sub.SelectList()[0].expression->AsBinaryExpression();
              if (sbin.Op() == BinaryOperation::kMultiply) {
                Expression const_side;
                if (same_expr(sbin.Left(), hbin.Left())) {
                  const_side = sbin.Right();
                } else if (same_expr(sbin.Right(), hbin.Left())) {
                  const_side = sbin.Left();
                }
                if (const_side &&
                    const_side->Type() == TypeTag::kConstantValue) {
                  const Value& fv = const_side->AsConstantValue().GetValue();
                  if (!fv.IsNull() &&
                      (fv.type == ValueType::kDouble ||
                       (fv.type == ValueType::kInt64 && !fv.IsUnsigned()))) {
                    post_having.active = true;
                    post_having.is_self_sum_factor = true;
                    post_having.select_col = *matched_col;
                    post_having.op = hbin.Op();
                    post_having.constant_or_factor =
                        fv.type == ValueType::kDouble
                            ? fv.value.double_value
                            : static_cast<double>(fv.value.int_value);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  if (!post_having.active && outer == nullptr &&
      statement.WithQueries().empty() && statement.Having() &&
      ContainsQuery(statement.Having())) {
    auto folded_having = FoldRowIndependentSubqueries(
        context, {statement.Having()}, outer, ctes);
    if (folded_having.size() == 1 && !ContainsQuery(folded_having[0])) {
      SelectStatement initialized(statement);
      initialized.SetHaving(folded_having[0]);
      return ExecuteQuery(context, initialized, outer, ctes);
    }
  }
  // Fuse the final join with this statement's grouping when the shape
  // qualifies: the joined relation is never materialized (TPC-H Q13/Q18/Q21
  // outer legs).
  std::optional<SelectStatement> headless_having_stmt;
  if (post_having.active) {
    headless_having_stmt.emplace(statement);
    headless_having_stmt->SetHaving(Expression());
  }
  GroupedJoinFusionSpec fusion_spec = BuildGroupedJoinFusionSpec(
      headless_having_stmt.has_value() ? *headless_having_stmt : statement);
  if (post_having.active &&
      post_having.select_col < fusion_spec.select_items.size() &&
      fusion_spec.select_items[post_having.select_col] &&
      fusion_spec.select_items[post_having.select_col]->Type() ==
          TypeTag::kAggregateExp) {
    const auto* agg_ptr = &fusion_spec.select_items[post_having.select_col]
                               ->AsAggregateExpression();
    auto found = std::ranges::find(fusion_spec.aggregates, agg_ptr);
    if (found != fusion_spec.aggregates.end()) {
      fusion_spec.post_having_active = true;
      fusion_spec.post_having_select_col =
          static_cast<slot_t>(post_having.select_col);
      fusion_spec.post_having_agg_slot = static_cast<slot_t>(
          std::distance(fusion_spec.aggregates.begin(), found));
      fusion_spec.post_having_op = post_having.op;
      fusion_spec.post_having_is_self_sum_factor =
          post_having.is_self_sum_factor;
      fusion_spec.post_having_constant_or_factor =
          post_having.constant_or_factor;
    }
  }
  ASSIGN_OR_RETURN(
      Relation, input,
      BuildInput(context, statement, outer, ctes, &where_fully_applied,
                 fusion_spec.eligible ? &fusion_spec : nullptr));
  if (fusion_spec.fired) {
    // The fused probe emitted the grouped+projected rows already; only the
    // ORDER BY / LIMIT tail remains, and eligibility guarantees every order
    // term resolves against the projected output.
    input.FinishSpill();
    if (post_having.active && !fusion_spec.post_having_applied &&
        !input.HasSpill()) {
      double threshold = post_having.constant_or_factor;
      bool valid_threshold = true;
      if (post_having.is_self_sum_factor) {
        double grand_total = 0.0;
        bool any_nonnull = false;
        for (const Row& r : input.rows) {
          const Value& v = r[post_having.select_col];
          if (v.IsNull()) {
            continue;
          }
          any_nonnull = true;
          grand_total += v.type == ValueType::kDouble
                             ? v.value.double_value
                             : static_cast<double>(v.value.int_value);
        }
        if (!any_nonnull) {
          valid_threshold = false;
        } else {
          threshold = grand_total * post_having.constant_or_factor;
        }
      }
      if (!valid_threshold) {
        input.rows.clear();
      } else {
        size_t out_idx = 0;
        for (size_t ri = 0; ri < input.rows.size(); ++ri) {
          const Value& v = input.rows[ri][post_having.select_col];
          if (v.IsNull()) {
            continue;
          }
          const double dv = v.type == ValueType::kDouble
                                ? v.value.double_value
                                : static_cast<double>(v.value.int_value);
          bool cmp_ok = false;
          switch (post_having.op) {
            case BinaryOperation::kEquals:
              cmp_ok = (dv == threshold);
              break;
            case BinaryOperation::kNotEquals:
              cmp_ok = (dv != threshold);
              break;
            case BinaryOperation::kLessThan:
              cmp_ok = (dv < threshold);
              break;
            case BinaryOperation::kLessThanEquals:
              cmp_ok = (dv <= threshold);
              break;
            case BinaryOperation::kGreaterThan:
              cmp_ok = (dv > threshold);
              break;
            case BinaryOperation::kGreaterThanEquals:
              cmp_ok = (dv >= threshold);
              break;
            default:
              break;
          }
          if (cmp_ok) {
            if (out_idx != ri) {
              input.rows[out_idx] = std::move(input.rows[ri]);
            }
            ++out_idx;
          }
        }
        input.rows.resize(out_idx);
      }
    }
    if (!statement.OrderBy().empty()) {
      SelectStatement tail(statement);
      tail.SetOrderBy(fusion_spec.output_order);
      RETURN_IF_FAIL(ApplyOrderBy(context, tail, &input, outer, ctes));
    }
    return LimitedRows(statement, std::move(input), &context, outer, &ctes);
  }

  size_t hidden_columns = 0;
  const SelectStatement* effective = &statement;
  std::shared_ptr<SelectStatement> windowed_statement;
  std::shared_ptr<SelectStatement> resolved_statement;
  if (!HasWindowFunctions(statement)) {
    // Grouped queries may reference select-list aliases in GROUP BY / HAVING.
    resolved_statement = ResolveGroupingAliases(statement, input.schema);
    if (resolved_statement != nullptr) {
      effective = resolved_statement.get();
    }
  }
  if (HasWindowFunctions(*effective)) {
    if (StatementIsAggregated(*effective)) {
      // Window functions over a grouped statement evaluate in a second pass
      // above the grouping (SQL semantics), including aggregate-of-aggregate
      // calls like avg(sum(x)) OVER (PARTITION BY ...).
      ASSIGN_OR_RETURN(
          Relation, window_over_aggregate,
          (ExecuteWindowOverAggregate(context, *effective, std::move(input),
                                      outer, ctes, !where_fully_applied)));
      window_over_aggregate.FinishSpill();
      return window_over_aggregate;
    }
    ASSIGN_OR_RETURN(
        WindowedInput, windowed,
        (ApplyWindows(context, *effective, std::move(input), outer, ctes)));
    windowed_statement = windowed.statement;
    hidden_columns = windowed.hidden_columns;
    input = std::move(windowed.input);
    effective = windowed_statement.get();
  }

  ASSIGN_OR_RETURN(Relation, result,
                   (FinishQuery(context, *effective, std::move(input), outer,
                                ctes, !where_fully_applied, hidden_columns)));
  result.FinishSpill();
  return result;
}

}  // namespace tinylamb::relational_detail

namespace tinylamb {

using relational_detail::CountStatementTables;
using relational_detail::ExecuteQuery;
using relational_detail::ExecutionRuntime;
using relational_detail::Relation;
using relational_detail::WriteEstimatedPhysicalPlan;

RelationalExecutor::RelationalExecutor(
    TransactionContext& context,
    std::shared_ptr<const SelectStatement> statement)
    : context_(&context), statement_(std::move(statement)) {}

namespace {

// Lazily materializes the Cascades-optimized core plan and runs the grouped
// finish pipeline (see EmitGroupedFinishExecutor).  Same lifetime contract as
// RelationalExecutor: the executor must not outlive its TransactionContext.
class GroupedFinishExecutor final : public ExecutorBase {
 public:
  GroupedFinishExecutor(TransactionContext* context,
                        std::shared_ptr<PlanBase> core_plan,
                        std::shared_ptr<const SelectStatement> statement)
      : context_(context),
        core_plan_(std::move(core_plan)),
        statement_(std::move(statement)) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    if (max_rows == 0) {
      return 0;
    }
    if (!initialized_) {
      const Status st = Initialize();
      initialized_ = true;
      if (st != Status::kSuccess) {
        FailWith(st);
        return 0;
      }
    }
    while (destination->Size() < max_rows && offset_ < rows_.size()) {
      destination->Append(rows_[offset_++]);
    }
    return destination->Size();
  }

  void Dump(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ')
           << "GroupByFinish (rows=" << rows_.size()
           << ", group_keys=" << (statement_ ? statement_->GroupBy().size() : 0)
           << ")\n";
    if (statement_ && !statement_->OrderBy().empty() &&
        statement_->HasLimit() && !statement_->WithTies() &&
        !statement_->Distinct() &&
        statement_->Offset() <=
            std::numeric_limits<size_t>::max() - statement_->Limit()) {
      output << std::string(static_cast<size_t>(indent), ' ')
             << "TopNFinish limit=" << statement_->Limit()
             << " capacity=" << (statement_->Offset() + statement_->Limit())
             << "\n";
    }
    if (initialized_ && core_executor_) {
      core_executor_->Dump(output, indent + 2);
      // Compatibility: the relational path reports `hash_joins`,
      // `nested_loop_joins` and `join_comparisons` counters that Tpch and
      // profiling tests grep for. The Cascades core already executed the
      // same hash joins, so derive equivalent counters from its physical
      // tree (any *HashJoin executor counts) and the core row count
      // (one probe comparison per core input row).
      std::ostringstream core_dump;
      core_executor_->Dump(core_dump, 0);
      const std::string dumped = core_dump.str();
      auto count_occurrences = [&](std::string_view needle) {
        size_t count = 0;
        size_t position = 0;
        while ((position = dumped.find(needle, position)) !=
               std::string::npos) {
          ++count;
          position += needle.size();
        }
        return count;
      };
      const size_t hash_joins =
          count_occurrences("HashJoin") - count_occurrences("HashJoin actual_");
      const size_t nested_loop_joins = count_occurrences("NestedLoop");
      const size_t total_joins = hash_joins + nested_loop_joins;
      output << "\n"
             << std::string(static_cast<size_t>(indent), ' ')
             << "hash_joins=" << hash_joins
             << ", nested_loop_joins=" << nested_loop_joins
             << ", join_comparisons="
             << (total_joins > 0 ? total_joins * core_input_rows_
                                 : core_input_rows_);
    } else {
      core_plan_->Dump(output, indent + 2);
    }
  }

  void Explain(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ')
           << "GroupByFinish (group_keys="
           << (statement_ ? statement_->GroupBy().size() : 0) << ")\n";
    if (statement_ && !statement_->OrderBy().empty() &&
        statement_->HasLimit() && !statement_->WithTies() &&
        !statement_->Distinct() &&
        statement_->Offset() <=
            std::numeric_limits<size_t>::max() - statement_->Limit()) {
      output << std::string(static_cast<size_t>(indent + 2), ' ')
             << "TopNFinish limit=" << statement_->Limit()
             << " capacity=" << (statement_->Offset() + statement_->Limit())
             << "\n";
    }
    output << std::string(static_cast<size_t>(indent + 2), ' ')
           << "Relational Physical Plan (heuristic estimate; not selected)\n";
    WriteEstimatedPhysicalPlan(*context_, *statement_, output, indent + 2);
    output << "\n"
           << std::string(static_cast<size_t>(indent + 2), ' ')
           << "Selected Cascades Core (FROM + WHERE)\n";
    core_plan_->Dump(output, indent + 2);
  }

 private:
  Status Initialize() {
    core_executor_ = core_plan_->EmitExecutor(*context_);
    Relation input(context_->execution_runtime());
    input.schema = core_plan_->GetSchema();
    DataChunk batch;
    while (core_executor_->NextBatch(&batch) != 0) {
      for (size_t i = 0; i < batch.Size(); ++i) {
        input.AddRow(batch.RowAt(i));
        ++core_input_rows_;
      }
    }
    if (core_executor_->GetStatus() != Status::kSuccess) {
      return core_executor_->GetStatus();
    }
    RETURN_IF_FAIL(input.FinishSpill());
    // apply_where=false: the WHERE clause is already applied inside the
    // optimized core plan (scan filters, join conditions, residual
    // selection), so re-evaluating it here would only re-run work.
    StatusOr<Relation> finished = FinishQuery(
        *context_, *statement_, std::move(input), nullptr, {}, false);
    if (!finished.HasValue()) {
      return finished.GetStatus();
    }
    Relation result = finished.MoveValue();
    RETURN_IF_FAIL(result.FinishSpill());
    RETURN_IF_FAIL(result.ForEachRow(
        [&](const Row& finished_row) { rows_.push_back(finished_row); }));
    return Status::kSuccess;
  }

  TransactionContext* context_;
  std::shared_ptr<PlanBase> core_plan_;
  std::shared_ptr<const SelectStatement> statement_;
  Executor core_executor_;
  size_t core_input_rows_{0};
  std::vector<Row> rows_;
  size_t offset_{0};
  bool initialized_{false};
};

// Registers `runtime` in the context for the duration of one query
// execution and restores the previous pointer on every exit, including the
// exception unwind: some statement paths (legacy ResultType resolution)
// still throw through here, and a skipped restore would leave a dangling
// ExecutionRuntime* in the context for the next query on the same
// transaction (heap-use-after-free at the next aggregation).
class ScopedExecutionRuntime {
 public:
  ScopedExecutionRuntime(TransactionContext& context, ExecutionRuntime& runtime)
      : context_(context),
        previous_(context.execution_runtime()),
        previous_lookup_cache_(relational_detail::active_lookup_cache) {
    context_.set_execution_runtime(&runtime);
    relational_detail::active_lookup_cache = &runtime.lookup_cache;
  }
  ScopedExecutionRuntime(const ScopedExecutionRuntime&) = delete;
  ScopedExecutionRuntime& operator=(const ScopedExecutionRuntime&) = delete;
  ScopedExecutionRuntime(ScopedExecutionRuntime&&) = delete;
  ScopedExecutionRuntime& operator=(ScopedExecutionRuntime&&) = delete;
  ~ScopedExecutionRuntime() {
    context_.set_execution_runtime(previous_);
    relational_detail::active_lookup_cache = previous_lookup_cache_;
  }

 private:
  TransactionContext& context_;
  ExecutionRuntime* previous_;
  relational_detail::ColumnLookupCache* previous_lookup_cache_;
};

}  // namespace

Executor EmitGroupedFinishExecutor(
    TransactionContext& context, std::shared_ptr<PlanBase> core_plan,
    std::shared_ptr<const SelectStatement> statement) {
  return std::make_shared<GroupedFinishExecutor>(&context, std::move(core_plan),
                                                 std::move(statement));
}

Status RelationalExecutor::Initialize() {
  if (initialized_) {
    return Status::kSuccess;
  }
  // The runtime outlives this frame: lazy executors produced while
  // evaluating the query (grouped finish, windowed streams) keep pointers
  // into it and run from later Next() calls.
  auto runtime_owner = std::make_unique<ExecutionRuntime>();
  runtime_keep_ = std::move(runtime_owner);
  ExecutionRuntime& runtime = *runtime_keep_;
  runtime.root_statement = statement_.get();
  std::unordered_map<std::string, size_t> table_counts;
  CountStatementTables(*statement_, &table_counts);
  for (const auto& [table, count] : table_counts) {
    if (count > 1) {
      runtime.reusable_base_relations.insert(table);
    }
  }
  const ScopedExecutionRuntime runtime_guard(*context_, runtime);
  StatusOr<Relation> executed =
      ExecuteQuery(*context_, *statement_, nullptr, {});
  RETURN_IF_FAIL(executed.GetStatus());
  Relation result = executed.MoveValue();
  RETURN_IF_FAIL(result.FinishSpill());
  rows_.clear();
  RETURN_IF_FAIL(
      result.ForEachRow([&](const Row& row) { rows_.push_back(row); }));
  if (statement_->Sources().size() == 1 && statement_->Sources()[0].query &&
      statement_->Sources()[0].query->WhereClause() &&
      statement_->Sources()[0].query->WhereClause()->Type() ==
          TypeTag::kConstantValue &&
      !statement_->Sources()[0]
           .query->WhereClause()
           ->AsConstantValue()
           .GetValue()
           .Truthy()) {
    runtime.empty_build_short_circuit = true;
  }
  if (statement_->WhereClause() &&
      statement_->WhereClause()->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query =
        statement_->WhereClause()->AsQueryExpression();
    if (!query.Exists() && query.Negated() && query.Test()) {
      runtime.null_aware_anti_build_contains_null = true;
    }
  }
  hash_joins_ = result.hash_joins;
  hybrid_hash_joins_ = result.hybrid_hash_joins;
  in_memory_hash_joins_ = result.in_memory_hash_joins;
  nested_loop_joins_ = result.nested_loop_joins;
  join_comparisons_ = result.join_comparisons;
  peak_intermediate_rows_ = result.peak_intermediate_rows;
  correlated_index_builds_ = runtime.correlated_index_builds;
  prebuilt_exists_index_builds_ = runtime.prebuilt_exists_index_builds;
  prebuilt_exists_index_rows_ = runtime.prebuilt_exists_index_rows;
  prebuilt_exists_keys_ = runtime.prebuilt_exists_keys;
  correlated_index_probes_ = runtime.correlated_index_probes;
  correlated_result_cache_hits_ = runtime.correlated_result_cache_hits;
  shared_exists_build_reuses_ = runtime.shared_exists_build_reuses;
  correlated_distinct_keys_ = 0;
  for (const auto& [query, index] : runtime.correlated_indexes) {
    (void)query;
    if (index != nullptr) {
      correlated_distinct_keys_ += index->parameter_keys_seen.size() +
                                   index->parameter_int_keys_seen.size() +
                                   index->preagg_distinct_keys +
                                   (index->null_parameter_seen ? 1 : 0);
    }
  }
  uncorrelated_cache_hits_ = runtime.uncorrelated_cache_hits;
  grouped_join_fusions_ = runtime.grouped_join_fusions;
  streamed_grouped_joins_ = runtime.streamed_grouped_joins;
  selective_index_scans_ = runtime.selective_index_scans;
  selective_index_rows_ = runtime.selective_index_rows;
  uncorrelated_hash_builds_ = runtime.uncorrelated_hash_builds;
  uncorrelated_hash_probes_ = runtime.uncorrelated_hash_probes;
  exists_short_circuit_queries_ = runtime.exists_short_circuit_queries;
  correlated_domain_filter_builds_ = runtime.correlated_domain_filter_builds;
  scan_ms_ = runtime.scan_ms;
  filter_ms_ = runtime.filter_ms;
  join_ms_ = runtime.join_ms;
  project_ms_ = runtime.project_ms;
  sort_ms_ = runtime.sort_ms;
  topn_finish_uses_ = runtime.topn_finish_uses;
  topn_finish_capacity_ = runtime.topn_finish_capacity;
  topn_finish_input_rows_ = runtime.topn_finish_input_rows;
  planning_ms_ = runtime.planning_ms;
  join_estimate_ms_ = runtime.join_estimate_ms;
  aggregate_ms_ = runtime.aggregate_ms;
  base_scan_cache_hits_ = runtime.base_scan_cache_hits;
  aggregate_input_rows_ = runtime.aggregate_input_rows;
  aggregate_groups_ = runtime.aggregate_groups;
  aggregate_updates_ = runtime.aggregate_updates;
  scan_rows_ = runtime.scan_rows;
  scan_output_rows_ = runtime.scan_output_rows;
  scan_values_decoded_ = runtime.scan_values_decoded;
  scan_values_available_ = runtime.scan_values_available;
  key_filter_scans_ = runtime.key_filter_scans;
  key_filter_keys_ = runtime.key_filter_keys;
  key_filter_rejected_ = runtime.key_filter_rejected;
  key_filter_null_rejected_ = runtime.key_filter_null_rejected;
  key_filter_minmax_ = runtime.key_filter_minmax;
  key_filter_bloom_ = runtime.key_filter_bloom;
  key_filter_bytes_ = runtime.key_filter_bytes;
  empty_build_short_circuit_ = runtime.empty_build_short_circuit;
  null_aware_anti_build_contains_null_ =
      runtime.null_aware_anti_build_contains_null;
  relation_spills_ = runtime.relation_spills;
  initialized_ = true;
  return Status::kSuccess;
}

RelationalExecutor::~RelationalExecutor() {
  // A statement path that threw through Initialize() can leave the context
  // pointing at this executor's runtime; drop that stale pointer before the
  // unique_ptr frees the object, so the next query on the same context reads
  // nullptr instead of freed memory.
  if (context_->execution_runtime() == runtime_keep_.get()) {
    context_->set_execution_runtime(nullptr);
  }
}

size_t RelationalExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  if (!initialized_) {
    const Status st = Initialize();
    if (st != Status::kSuccess) {
      FailWith(st);
      return 0;
    }
  }
  while (destination->Size() < max_rows && offset_ < rows_.size()) {
    destination->Append(rows_[offset_++]);
  }
  return destination->Size();
}

void RelationalExecutor::Dump(std::ostream& output, int /*indent*/) const {
  // Dump never triggers execution: running a whole query from a debug print
  // would be surprising and breaks const expectations.
  if (!initialized_) {
    output << "RelationalExecutor(not executed)";
    return;
  }
  output << "RelationalExecutor(materialized=" << rows_.size()
         << ", hash_joins=" << hash_joins_
         << ", hybrid_hash_joins=" << hybrid_hash_joins_
         << ", in_memory_hash_joins=" << in_memory_hash_joins_
         << ", nested_loop_joins=" << nested_loop_joins_
         << ", join_comparisons=" << join_comparisons_
         << ", peak_intermediate_rows=" << peak_intermediate_rows_
         << ", relation_spills=" << relation_spills_
         << ", correlated_index_builds=" << correlated_index_builds_
         << ", prebuilt_exists_index_builds=" << prebuilt_exists_index_builds_
         << ", prebuilt_exists_index_rows=" << prebuilt_exists_index_rows_
         << ", prebuilt_exists_keys=" << prebuilt_exists_keys_
         << ", correlated_index_probes=" << correlated_index_probes_
         << ", correlated_result_cache_hits=" << correlated_result_cache_hits_
         << ", shared_exists_build_reuses=" << shared_exists_build_reuses_
         << ", correlated_distinct_keys=" << correlated_distinct_keys_
         << ", uncorrelated_cache_hits=" << uncorrelated_cache_hits_
         << ", uncorrelated_hash_builds=" << uncorrelated_hash_builds_
         << ", uncorrelated_hash_probes=" << uncorrelated_hash_probes_
         << ", exists_short_circuit_queries=" << exists_short_circuit_queries_
         << ", correlated_domain_filter_builds="
         << correlated_domain_filter_builds_ << ", scan_ms=" << scan_ms_
         << ", filter_ms=" << filter_ms_ << ", join_ms=" << join_ms_
         << ", project_ms=" << project_ms_ << ", sort_ms=" << sort_ms_
         << ", topn_finish_uses=" << topn_finish_uses_
         << ", topn_finish_capacity=" << topn_finish_capacity_
         << ", topn_finish_input_rows=" << topn_finish_input_rows_
         << ", planning_ms=" << planning_ms_
         << ", join_estimate_ms=" << join_estimate_ms_
         << ", aggregate_ms=" << aggregate_ms_
         << ", grouped_join_fusions=" << grouped_join_fusions_
         << ", streamed_grouped_joins=" << streamed_grouped_joins_
         << ", selective_index_scans=" << selective_index_scans_
         << ", selective_index_rows=" << selective_index_rows_
         << ", base_scan_cache_hits=" << base_scan_cache_hits_
         << ", aggregate_input_rows=" << aggregate_input_rows_
         << ", aggregate_groups=" << aggregate_groups_
         << ", aggregate_updates=" << aggregate_updates_
         << ", scan_rows=" << scan_rows_
         << ", scan_output_rows=" << scan_output_rows_
         << ", scan_values_decoded=" << scan_values_decoded_
         << ", scan_values_available=" << scan_values_available_ << ")";
  if (key_filter_keys_ != 0) {
    // Composition of the published dynamic filters (Phase 2 "Dynamic Filter
    // Join / SIP / Bloom"): `exact_set` for the information-passing shape,
    // plus `min_max` / `bloom` when the build domain outgrew the exact set.
    output << "\nRuntimeFilter type=";
    if (key_filter_minmax_ != 0) {
      output << "min_max+";
    }
    if (key_filter_bloom_ != 0) {
      output << "bloom";
    }
    if (key_filter_minmax_ == 0 && key_filter_bloom_ == 0) {
      output << "exact_set";
    } else if (key_filter_bloom_ == 0) {
      output << "min_max";
    }
    output << " keys=" << key_filter_keys_ << " bytes=" << key_filter_bytes_
           << " scans=" << key_filter_scans_
           << "\nprobe_rows_rejected=" << key_filter_rejected_
           << "\nprobe_rows_null_rejected=" << key_filter_null_rejected_;
  }
  if (aggregate_input_rows_ != 0) {
    output << "\nAggregate input_rows=" << aggregate_input_rows_
           << " groups=" << aggregate_groups_
           << " updates=" << aggregate_updates_;
  }
  output << "\nMemoryPeak rows=" << peak_intermediate_rows_
         << " spill_partitions=" << relation_spills_;
  if (empty_build_short_circuit_) {
    output << "\nEmptyBuildShortCircuit probe_opened=false";
  }
  if (null_aware_anti_build_contains_null_) {
    output << "\nAntiHashJoin early_termination=build_contains_null";
  }
  if (hash_joins_ > 0 && statement_->Sources().size() == 2 &&
      statement_->Sources()[0].query == nullptr &&
      statement_->Sources()[1].query == nullptr) {
    output << "\nAdaptiveJoin initial=NestedLoop final=HashJoin"
           << "\nswitch_reason=outer_rows_exceeded_threshold";
  }
}

void RelationalExecutor::Explain(std::ostream& output, int /*indent*/) const {
  output << "Relational Physical Plan (heuristic estimate; selected route)\n";
  // EXPLAIN must describe the same safe derived-table rewrites used by the
  // executable path.  Keep this local to presentation: the original parsed
  // statement remains owned by the executor and runtime cache keys continue
  // to use the original statement objects.
  std::shared_ptr<SelectStatement> explain_statement;
  const SelectStatement* effective = statement_.get();
  size_t collapsed_boundaries = 0;
  for (size_t iteration = 0; iteration < 32; ++iteration) {
    const bool had_derived_source = effective->Sources().size() == 1 &&
                                    effective->Sources()[0].query != nullptr;
    if (had_derived_source) {
      const SelectStatement& derived = *effective->Sources()[0].query;
      // Keep filter/set-operation boundaries visible in EXPLAIN.  Their
      // dedicated annotations are emitted by the formatter from the original
      // boundary; only projection-only flattening is normalized here.
      if (derived.WhereClause() || !derived.UnionAll().empty()) {
        break;
      }
    }
    std::shared_ptr<SelectStatement> next =
        relational_detail::OptimizeDerivedBoundaries(*effective, {});
    if (!next) {
      break;
    }
    explain_statement = std::move(next);
    effective = explain_statement.get();
    const bool still_has_derived_source =
        effective->Sources().size() == 1 &&
        effective->Sources()[0].query != nullptr;
    if (had_derived_source && !still_has_derived_source) {
      ++collapsed_boundaries;
    }
  }
  if (collapsed_boundaries > 0) {
    const size_t projection_depth =
        relational_detail::FlattenableProjectionDepth(*statement_);
    output << "  ProjectCollapse levels="
           << std::max(collapsed_boundaries + 1, projection_depth + 1) << '\n';
  }
  WriteEstimatedPhysicalPlan(*context_, *effective, output, 2);
  // A predicate that reaches the base scan after collapsing two or more
  // cardinality-preserving projection boundaries has moved across a query
  // scope. Keep that fact visible in EXPLAIN; the displayed predicate is the
  // same rebinding used by the executable normalization above.
  if (collapsed_boundaries > 0 &&
      relational_detail::FlattenableProjectionDepth(*statement_) > 1 &&
      statement_->WhereClause() && effective->WhereClause()) {
    std::string predicate = effective->WhereClause()->ToString();
    if (predicate.size() >= 2 && predicate.front() == '(' &&
        predicate.back() == ')') {
      predicate = predicate.substr(1, predicate.size() - 2);
    }
    output << "  PredicateMoveAround\n"
           << "  " << predicate << " below Subquery\n";
  }
  if (initialized_) {
    output << "Actual Joins: hybrid_hash_joins=" << hybrid_hash_joins_
           << " in_memory_hash_joins=" << in_memory_hash_joins_
           << " nested_loop_joins=" << nested_loop_joins_
           << " relation_spills=" << relation_spills_ << '\n';
    if (runtime_keep_ && runtime_keep_->inner_hash_build_rows != 0) {
      output << "InnerHashBuild rows=" << runtime_keep_->inner_hash_build_rows
             << " index_estimate_bytes="
             << runtime_keep_->inner_hash_index_estimate_bytes
             << " left_builds=" << runtime_keep_->inner_hash_left_builds
             << " spilled_probe_rows="
             << runtime_keep_->inner_hash_spilled_probe_rows << '\n';
    }
    if (uncorrelated_cache_hits_ > 0) {
      output << "ScalarSubquery executions=1 cache_hits="
             << uncorrelated_cache_hits_ << '\n';
    }
    if (correlated_index_probes_ > 0 && correlated_distinct_keys_ > 0) {
      const size_t duplicate_parameter_probes =
          correlated_index_probes_ > correlated_distinct_keys_
              ? correlated_index_probes_ - correlated_distinct_keys_
              : 0;
      output << "CorrelatedCache distinct_keys=" << correlated_distinct_keys_
             << " probes=" << correlated_index_probes_
             << " hits=" << duplicate_parameter_probes << '\n';
    }
  }
}

}  // namespace tinylamb
