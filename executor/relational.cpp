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
constexpr size_t kParallelGroupMinRows = 262144;

namespace {

// HAVING may reference select-list aliases (`HAVING double_avg > 0.7`).
// Alias references that are not real input columns are rewritten to the
// aliased expression so aggregate aliases resolve during grouping.
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

std::string AsciiToLower(std::string_view text) {
  std::string out(text);
  for (char& c : out) {
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
  }
  return out;
}

}  // namespace

namespace {

// HAVING may only reference group-key columns and prepared aggregates: any
// other base column would need a full representative row per group.
bool HavingReferencesOnlyKeysAndAggregates(
    const Expression& expression,  // NOLINT(misc-no-recursion)
    const Schema& schema, const std::vector<slot_t>& key_offsets) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const auto offset = LocalColumnOffset(
          schema, expression->AsColumnValue().GetColumnName());
      if (!offset) {
        return false;
      }
      return std::find(key_offsets.begin(), key_offsets.end(),
                       static_cast<slot_t>(*offset)) != key_offsets.end();
    }
    case TypeTag::kAggregateExp:
      return true;
    default:
      break;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             [&](const Expression& child) {
                               return HavingReferencesOnlyKeysAndAggregates(
                                   child, schema, key_offsets);
                             });
  return true;
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
    expression = ReplaceSubExpressions(
        expression,
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
        std::optional<BytecodeProgram> compiled = BytecodeCompiler::Compile(
            expression, input.schema, BytecodeCompiler::Context::kValue);
        if (!compiled.has_value()) {
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
                              total_rows / 32768});
        if (workers <= 1) {
          parallel_grouping = false;
        } else {
          GroupKeyMap offsets;
          const size_t chunk_rows = 8192;
          const size_t chunk_count = (total_rows + chunk_rows - 1) / chunk_rows;
          struct WorkerLocal {
            GroupKeyMap offsets;
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
                // The atomic counter this replaced handed chunks to whichever
                // worker grabbed them, so the per-worker partial sums of a
                // DOUBLE aggregate changed run to run and rank boundaries
                // (TPC-DS q65/q67) flipped on the last ulp. Chunks are fixed
                // 8192-row slices, so ownership by index is as balanced as
                // the race was and the merge in worker order becomes a
                // deterministic function of the input.
                for (size_t chunk = w; chunk < chunk_count; chunk += workers) {
                  const size_t begin = chunk * chunk_rows;
                  const size_t end = std::min(total_rows, begin + chunk_rows);
                  for (size_t ri = begin; ri < end; ++ri) {
                    const Row& row = input.rows[ri];
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
                    GroupState& group = local.groups[iter->second];
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
          // Merge partials in worker order.  A group first seen by a worker
          // is adopted wholesale (its accumulator states move onto the
          // shared deque and the offset rebases); overlapping groups merge
          // accumulator state.  Adopt-then-move keeps every partial state
          // visible.
          for (WorkerLocal& local : locals) {
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
    for (const GroupState& group : groups) {
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, finished,
            aggregate_states[group.accumulator_offset + i].TryFinish());
        finished_values[i] = std::move(finished);
      }
      if (having_expr) {
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
  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
    size_t sequence{0};
  };
  std::vector<KeyedRow> sortable;
  RETURN_IF_FAIL(output->FinishSpill());
  Status sort_error{Status::kSuccess};
  Status iterated = output->ForEachRow([&](const Row& row) -> void {
    Scope scope{.row = &row, .schema = &output->schema, .outer = outer};
    std::vector<Value> keys;
    keys.reserve(order_by.size());
    for (const Expression& key : key_expressions) {
      if (sort_error != Status::kSuccess) {
        return;
      }
      StatusOr<Value> hv33968_0 =
          TryEvaluate(key, scope, nullptr, context, ctes);
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
  // The same ordering contract the non-grouped and grouped relational paths
  // use: NULL placement, direction and the key's collation from
  // SortKeyOrder (type/collation.hpp).
  std::vector<SortKeyOrder> order_specs;
  order_specs.reserve(order_by.size());
  for (const SelectStatement::OrderByTerm& term : order_by) {
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
  for (KeyedRow& keyed : sortable) {
    output->AddRow(std::move(keyed.row));
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
    const relational_detail::CompiledScanFilter compiled =
        relational_detail::CompileScanFilter(where_predicates, input.schema);
    // Use compiled filter path when any fast-path is available: SimpleCompare,
    // disjunctive branches, or bytecode-compiled residuals.  The bytecode
    // path avoids recursive expression tree traversal for residual predicates,
    // which is the dominant cost for complex WHERE clauses like TPC-H Q20.
    if (!compiled.simple.empty() || !compiled.disjunctive_branches.empty() ||
        compiled.residual_bytecode.has_value() || !compiled.residual.empty()) {
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      input.FinishSpill();
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

bool IdentifierEquals(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    const auto lhs = static_cast<unsigned char>(left[i]);
    const auto rhs = static_cast<unsigned char>(right[i]);
    if (lhs == rhs) {
      continue;
    }
    if (lhs >= 0x80 || rhs >= 0x80) {
      if (std::tolower(lhs) != std::tolower(rhs)) {
        return false;
      }
      continue;
    }
    const unsigned char folded_left =
        lhs >= 'A' && lhs <= 'Z' ? lhs + ('a' - 'A') : lhs;
    const unsigned char folded_right =
        rhs >= 'A' && rhs <= 'Z' ? rhs + ('a' - 'A') : rhs;
    if (folded_left != folded_right) {
      return false;
    }
  }
  return true;
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
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll ||
                                   kind == SetOperationKind::kUnion;
                          });
  if (is_union) {
    // UNION ALL has no row-changing boundary before the outer filter. Push a
    // local predicate into every branch, then retain any non-local predicates
    // above the set operation.
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
      pushed.ClearUnionAll();
      for (const auto& branch : branches) {
        pushed.AddUnionAll(branch);
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
      !derived.UnionDistinct()) {
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
    pushed.ClearUnionAll();
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
    for (const auto& branch : derived.UnionAll()) {
      if (branch) {
        pushed.AddUnionAll(cap_branch(branch));
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
        return StatusError(StatusCode::kInvalidArgument,
                           context + " produces duplicate column " + wanted);
      }
      found = index;
    }
    if (found == source.ColumnCount()) {
      return StatusError(StatusCode::kInvalidArgument,
                         context + " has no column named " + wanted);
    }
    alignment.push_back(found);
  }
  if (strict) {
    for (slot_t index = 0; index < source.ColumnCount(); ++index) {
      const std::string& name = source.GetColumn(index).Name().name;
      if (!ContainsColumnName(targets, name)) {
        return StatusError(StatusCode::kInvalidArgument,
                           context + " produces column " + name +
                               " that the working table does not name");
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
          return StatusError(StatusCode::kInvalidArgument,
                             "recursive CTE " + name + " has no column " +
                                 listed + " to match by name");
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
          return StatusError(StatusCode::kInvalidArgument,
                             "recursive CTE " + name + " cannot drop column " +
                                 wanted + " in a name-matched recursive term");
        }
        if (term_names.has_value() &&
            !ContainsColumnName(*term_names, wanted)) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "recursive CTE " + name + " term has no column " + wanted);
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
      const SetOperationMatch match = term_matches[term_index];
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

namespace {

// Per-aggregate input plan for the typed parallel stream aggregation.  The
// AggregateAccumulator keeps every piece of aggregate semantics (NULL skip,
// DISTINCT sets, overflow, NaN poisoning); this plan only decides where each
// input value comes from: a plain column of the scanned row or a context-free
// bytecode program evaluated against it.
struct TypedAggregateInput {
  const AggregateExpression* aggregate{nullptr};
  std::optional<slot_t> column;
  std::optional<BytecodeProgram> child;
};

// Slim per-group state classification for the typed stream aggregation.
// Multi-million-group folds (TPC-H Q18/Q21 shapes) stream ~300 bytes of
// AggregateAccumulator per group per aggregate even when the aggregate keeps
// only a count and a scalar total; the slim SoA layout keeps those two scalars
// instead, cutting group-state cache traffic by an order of magnitude.  The
// kinds must mirror AggregateAccumulator::Add/Merge/TryFinish exactly for the
// shapes they admit (NULL skip, INT64 overflow raise, SUM infinity raise,
// AVG-through-double).
enum class SlimAggKind : uint8_t {
  kNone = 0,   // keep the generic AggregateAccumulator for this aggregate
  kCount,      // COUNT(*) / COUNT(expr): NULL-skipping ++count
  kSumInt64,   // SUM over signed INT64: overflow-checked int_total
  kSumDouble,  // SUM over DOUBLE: double total + saw-infinite-input flag
  kAvgDouble,  // AVG over INT64/DOUBLE: sums through a double intermediate
  kCountDistinctInt64,  // COUNT(DISTINCT int64/date): inline slots + overflow
};

// Inline distinct-slot count for kCountDistinctInt64.  Mirrors
// AggregateAccumulator::kInlineDistinctIntCapacity so overflow behavior is
// bit-identical with the generic accumulator.
constexpr size_t kSlimDistinctSlots = 8;

// Select/ORDER BY output items resolved positionally: a group-key column or
// a top-level aggregate.
struct TypedEmitItem {
  bool is_key{false};
  size_t index{0};
};

// Same-type column-vs-column compare conjunct peeled out of the typed
// aggregation's residual filter programs: evaluated positionally per row with
// no bytecode dispatch (NULL rejects, exactly like the three-valued
// conjunct).  INT64/DATE compare their raw integers; DOUBLE compares as
// double (IEEE semantics, so a NaN input rejects every ordered comparison —
// mirroring the AST evaluator).
struct TypedFilterCompare {
  slot_t left{0};
  slot_t right{0};
  BinaryOperation op{BinaryOperation::kEquals};
  bool is_double{false};
  // Col-vs-constant peel (`col op const`): `constant` was constant-folded at
  // plan time and its ValueType equals the column's, so the per-row decision
  // is a raw positional compare.  `op` is already mirrored so the column sits
  // on the left.  NULL columns still reject the row exactly like the
  // col-vs-col peel.
  bool constant_payload{false};
  int64_t int_constant{0};
  double double_constant{0.0};
  // Byte-exact VARCHAR equality/inequality against a plan-owned plain string
  // (no collation on the constant; mirrors MatchSimpleCompare's
  // plain_string_payload contract).
  bool string_payload{false};
  std::string string_constant{};
};

// Constant IN-list peel (`col IN (c1, c2, ...)`): keep the row iff any list
// item equals the column cell.  Every item was a non-NULL same-type plain
// constant at plan time, so a miss rejects the row exactly like the bytecode
// IN (which yields UNKNOWN/FALSE for both the NULL-cell and no-match cases
// in a WHERE context).
struct TypedInListCompare {
  slot_t column{0};
  bool is_double{false};
  bool is_string{false};
  std::vector<int64_t> int_values;
  std::vector<double> double_values;
  std::vector<std::string> strings;
};

BinaryOperation FlipTypedCompare(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

inline bool DecideTypedFilterCompare(const Value& left, const Value& right,
                                     const TypedFilterCompare& compare) {
  if (compare.string_payload) {
    const std::string_view a = left.value.varchar_value;
    const bool equal = a == std::string_view(compare.string_constant);
    return compare.op == BinaryOperation::kEquals ? equal : !equal;
  }
  if (compare.constant_payload) {
    if (compare.is_double) {
      const double a = left.value.double_value;
      switch (compare.op) {
        case BinaryOperation::kEquals:
          return a == compare.double_constant;
        case BinaryOperation::kNotEquals:
          return a != compare.double_constant;
        case BinaryOperation::kLessThan:
          return a < compare.double_constant;
        case BinaryOperation::kLessThanEquals:
          return a <= compare.double_constant;
        case BinaryOperation::kGreaterThan:
          return a > compare.double_constant;
        default:
          return a >= compare.double_constant;
      }
    }
    const int64_t a = left.value.int_value;
    switch (compare.op) {
      case BinaryOperation::kEquals:
        return a == compare.int_constant;
      case BinaryOperation::kNotEquals:
        return a != compare.int_constant;
      case BinaryOperation::kLessThan:
        return a < compare.int_constant;
      case BinaryOperation::kLessThanEquals:
        return a <= compare.int_constant;
      case BinaryOperation::kGreaterThan:
        return a > compare.int_constant;
      default:
        return a >= compare.int_constant;
    }
  }
  if (compare.is_double) {
    const double a = left.value.double_value;
    const double b = right.value.double_value;
    switch (compare.op) {
      case BinaryOperation::kEquals:
        return a == b;
      case BinaryOperation::kNotEquals:
        return a != b;
      case BinaryOperation::kLessThan:
        return a < b;
      case BinaryOperation::kLessThanEquals:
        return a <= b;
      case BinaryOperation::kGreaterThan:
        return a > b;
      default:
        return a >= b;
    }
  }
  const int64_t a = left.value.int_value;
  const int64_t b = right.value.int_value;
  switch (compare.op) {
    case BinaryOperation::kEquals:
      return a == b;
    case BinaryOperation::kNotEquals:
      return a != b;
    case BinaryOperation::kLessThan:
      return a < b;
    case BinaryOperation::kLessThanEquals:
      return a <= b;
    case BinaryOperation::kGreaterThan:
      return a > b;
    default:
      return a >= b;
  }
}

struct TypedAggregationPlan {
  bool eligible{false};
  // Empty when the statement has no GROUP BY (global aggregate).
  std::vector<slot_t> key_offsets;
  std::vector<TypedAggregateInput> aggregates;
  // Set when every aggregate admits a slim kind; plan.slim_kinds[i] then
  // classifies aggregates[i] for the SoA group-state layout.
  bool slim{false};
  std::vector<SlimAggKind> slim_kinds;
  // Select/ORDER BY output items resolved positionally: a group-key column or
  // a top-level aggregate.
  std::vector<TypedEmitItem> emit_items;
  // Engaged only when the statement carries ORDER BY.
  std::vector<TypedEmitItem> order_items;
  std::vector<bool> order_ascending;
  std::vector<bool> order_nulls_first;
  // Whole-conjunct WHERE programs; every one must be truthy per row.
  std::vector<BytecodeProgram> filter_programs;
  // Column-compare conjuncts peeled out of filter_programs (see
  // TypedFilterCompare); evaluated before the programs.
  std::vector<TypedFilterCompare> filter_compares;
  // Constant IN-list conjuncts peeled out of filter_programs (see
  // TypedInListCompare); evaluated with the compares, before the programs.
  std::vector<TypedInListCompare> inlist_compares;
};

TypedAggregationPlan PlanTypedAggregation(
    const SelectStatement& stmt, const Expression& having_expr,
    const Schema& scan_schema,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const std::vector<Expression>& scan_predicates) {
  TypedAggregationPlan plan;
  if (!BytecodeEnabled()) {
    return plan;
  }
  // Operational escape hatch: TINYLAMB_DISABLE_TYPED_AGG=1 routes every
  // statement back through the generic stream-aggregation loop.
  static const bool typed_agg_disabled =
      std::getenv("TINYLAMB_DISABLE_TYPED_AGG") != nullptr;
  if (typed_agg_disabled) {
    return plan;
  }
  if (stmt.Distinct()) {
    return plan;
  }
  // Group keys: plain columns only.
  for (const Expression& key : stmt.GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return plan;
    }
    const auto offset =
        LocalColumnOffset(scan_schema, key->AsColumnValue().GetColumnName());
    if (!offset) {
      return plan;
    }
    plan.key_offsets.push_back(static_cast<slot_t>(*offset));
  }
  // WHERE: every conjunct must compile to context-free bytecode.
  static const bool filter_peek_disabled =
      std::getenv("TINYLAMB_DISABLE_AGG_FILTER_PEEK") != nullptr;
  // A/B hatch: TINYLAMB_DISABLE_TYPED_CONST_PEEK=1 keeps col-vs-constant
  // conjuncts in the bytecode programs (col-vs-col peeling stays on).
  static const bool const_peek_disabled =
      std::getenv("TINYLAMB_DISABLE_TYPED_CONST_PEEK") != nullptr;
  // Only a PREFIX of the conjunct list may peel to positional compares: the
  // typed path evaluates conjuncts in order and a raising program aborts the
  // aggregate, so reordering a peel past a raising conjunct could silently
  // swallow its error.
  bool peel_allowed = !filter_peek_disabled;
  for (const Expression& predicate : scan_predicates) {
    if (!predicate) {
      continue;
    }
    if (ContainsQuery(predicate)) {
      return plan;
    }
    bool peeled = false;
    if (peel_allowed && !const_peek_disabled &&
        predicate->Type() == TypeTag::kInExp) {
      // Constant IN-list peel (`col IN ('RAIL', 'SHIP', ...)`): plain
      // same-type constants only, so the per-row decision is a short linear
      // scan with no dispatch.  NULL cells and no-match both reject the row,
      // which is what the SQL semantics demand in a WHERE conjunct.
      const InExpression& membership = predicate->AsInExpression();
      if (!membership.list_.empty() && membership.list_.size() <= 64 &&
          membership.child_->Type() == TypeTag::kColumnValue) {
        const auto offset = LocalColumnOffset(
            scan_schema, membership.child_->AsColumnValue().GetColumnName());
        if (offset) {
          const Column& column_object =
              scan_schema.GetColumn(static_cast<slot_t>(*offset));
          const ValueType column_type = column_object.Type();
          TypedInListCompare compiled;
          compiled.column = static_cast<slot_t>(*offset);
          bool all_plain = !column_object.IsUnsigned();
          if (column_type == ValueType::kVarChar) {
            compiled.is_string = true;
          } else if (column_type == ValueType::kDouble) {
            compiled.is_double = true;
          } else if (column_type != ValueType::kInt64 &&
                     column_type != ValueType::kDate) {
            all_plain = false;
          }
          for (const Expression& item : membership.list_) {
            if (!all_plain || !item ||
                item->Type() != TypeTag::kConstantValue) {
              all_plain = false;
              break;
            }
            const Value& item_value = item->AsConstantValue().GetValue();
            if (item_value.IsNull() ||
                (!compiled.is_string && item_value.type != column_type)) {
              all_plain = false;
              break;
            }
            if (compiled.is_string) {
              if (item_value.type != ValueType::kVarChar ||
                  item_value.IsCaseInsensitive() ||
                  !PlainStringComparable(item_value.value.varchar_value)) {
                all_plain = false;
                break;
              }
              compiled.strings.emplace_back(item_value.value.varchar_value);
            } else if (compiled.is_double) {
              compiled.double_values.push_back(item_value.value.double_value);
            } else {
              compiled.int_values.push_back(item_value.value.int_value);
            }
          }
          if (all_plain) {
            plan.inlist_compares.push_back(std::move(compiled));
            peeled = true;
          }
        }
      }
    }
    if (peel_allowed && !peeled && predicate->Type() == TypeTag::kBinaryExp) {
      // Column-vs-column compares of one numeric type evaluate positionally:
      // a multi-million-row fold (Q21's l_receiptdate > l_commitdate) skips
      // the per-row bytecode dispatch.  Such a compare can never raise, so
      // the conjunct-order error semantics are unchanged.
      const BinaryExpression& binary = predicate->AsBinaryExpression();
      if (IsComparison(binary.Op()) &&
          binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kColumnValue) {
        const auto left_offset = LocalColumnOffset(
            scan_schema, binary.Left()->AsColumnValue().GetColumnName());
        const auto right_offset = LocalColumnOffset(
            scan_schema, binary.Right()->AsColumnValue().GetColumnName());
        if (left_offset && right_offset) {
          const Column& left_column =
              scan_schema.GetColumn(static_cast<slot_t>(*left_offset));
          const Column& right_column =
              scan_schema.GetColumn(static_cast<slot_t>(*right_offset));
          const ValueType left_type = left_column.Type();
          if (!left_column.IsUnsigned() && !right_column.IsUnsigned() &&
              left_type == right_column.Type() &&
              (left_type == ValueType::kInt64 ||
               left_type == ValueType::kDate ||
               left_type == ValueType::kDouble)) {
            plan.filter_compares.push_back(TypedFilterCompare{
                .left = static_cast<slot_t>(*left_offset),
                .right = static_cast<slot_t>(*right_offset),
                .op = binary.Op(),
                .is_double = left_type == ValueType::kDouble});
            peeled = true;
          }
        }
      } else if (IsComparison(binary.Op()) && !const_peek_disabled &&
                 (binary.Left()->Type() == TypeTag::kColumnValue ||
                  binary.Right()->Type() == TypeTag::kColumnValue)) {
        // Column-vs-constant compares peel the same way after constant
        // folding (Q1's `l_shipdate <= date_sub(date '1998-12-01',
        // INTERVAL 74 day)` folds to a DATE constant).  Folding reuses the
        // scan-filter path's rules, including the NOT-of-ordered-double
        // suppression, so a peel never changes a NaN verdict.  A folding
        // failure keeps the conjunct in bytecode exactly as before.
        ExpressionRewriter rewriter(
            ContainsNotOfOrderedDoubleComparison(predicate, scan_schema)
                ? NotComparisonFreeRules()
                : ExpressionRuleSet::Default());
        rewriter.set_schema(&scan_schema);
        StatusOr<Expression> folded_or = rewriter.TryRewrite(predicate);
        if (folded_or.HasValue() && folded_or.Value() &&
            folded_or.Value()->Type() == TypeTag::kBinaryExp) {
          const BinaryExpression& folded =
              folded_or.Value()->AsBinaryExpression();
          if (IsComparison(folded.Op())) {
            Expression column = folded.Left();
            Expression constant = folded.Right();
            BinaryOperation op = folded.Op();
            if (column->Type() == TypeTag::kConstantValue &&
                constant->Type() == TypeTag::kColumnValue) {
              std::swap(column, constant);
              op = FlipTypedCompare(op);
            }
            if (column->Type() == TypeTag::kColumnValue &&
                constant->Type() == TypeTag::kConstantValue) {
              const auto offset = LocalColumnOffset(
                  scan_schema, column->AsColumnValue().GetColumnName());
              const Value& constant_value =
                  constant->AsConstantValue().GetValue();
              if (offset && !constant_value.IsNull()) {
                const Column& column_object =
                    scan_schema.GetColumn(static_cast<slot_t>(*offset));
                const ValueType column_type = column_object.Type();
                TypedFilterCompare compiled{
                    .left = static_cast<slot_t>(*offset),
                    .op = op,
                    .constant_payload = true};
                bool supported = false;
                if (!column_object.IsUnsigned() &&
                    (column_type == ValueType::kInt64 ||
                     column_type == ValueType::kDate) &&
                    constant_value.type == column_type) {
                  compiled.is_double = false;
                  compiled.int_constant = constant_value.value.int_value;
                  supported = true;
                } else if (!column_object.IsUnsigned() &&
                           column_type == ValueType::kDouble &&
                           constant_value.type == ValueType::kDouble) {
                  compiled.is_double = true;
                  compiled.double_constant = constant_value.value.double_value;
                  supported = true;
                } else if (column_type == ValueType::kVarChar &&
                           constant_value.type == ValueType::kVarChar &&
                           !constant_value.IsCaseInsensitive() &&
                           PlainStringComparable(
                               constant_value.value.varchar_value) &&
                           (op == BinaryOperation::kEquals ||
                            op == BinaryOperation::kNotEquals)) {
                  compiled.string_payload = true;
                  compiled.string_constant =
                      std::string(constant_value.value.varchar_value);
                  supported = true;
                }
                if (supported) {
                  plan.filter_compares.push_back(std::move(compiled));
                  peeled = true;
                }
              }
            }
          }
        }
      }
    }
    if (!peeled) {
      peel_allowed = false;
      std::optional<BytecodeProgram> compiled = BytecodeCompiler::Compile(
          predicate, scan_schema, BytecodeCompiler::Context::kFilter);
      if (!compiled.has_value()) {
        return plan;
      }
      plan.filter_programs.push_back(std::move(*compiled));
    }
  }
  // Aggregates: exactly the accumulator kinds whose partial states merge.
  for (const AggregateExpression* aggregate : aggregate_expressions) {
    const AggregationType type = aggregate->GetType();
    if (type != AggregationType::kCount && type != AggregationType::kSum &&
        type != AggregationType::kAvg && type != AggregationType::kMin &&
        type != AggregationType::kMax) {
      return plan;
    }
    if (aggregate->NeedsGroupContext() || aggregate->WhereFilter() ||
        aggregate->Having() != AggregateHavingModifier::kNone ||
        aggregate->HavingCondition() || !aggregate->InnerOrderBy().empty() ||
        aggregate->SecondaryArg() || !aggregate->TrailingArgs().empty()) {
      return plan;
    }
    if (aggregate->Distinct() && type != AggregationType::kCount) {
      return plan;
    }
    TypedAggregateInput input;
    input.aggregate = aggregate;
    if (!aggregate->Child() || IsCountStar(*aggregate)) {
      plan.aggregates.push_back(std::move(input));
      continue;
    }
    const Expression& child = aggregate->Child();
    if (child->Type() == TypeTag::kColumnValue) {
      const auto offset = LocalColumnOffset(
          scan_schema, child->AsColumnValue().GetColumnName());
      if (!offset) {
        return plan;
      }
      input.column = static_cast<slot_t>(*offset);
    } else {
      if (ContainsQuery(child)) {
        return plan;
      }
      std::optional<BytecodeProgram> compiled = BytecodeCompiler::Compile(
          child, scan_schema, BytecodeCompiler::Context::kValue);
      if (!compiled.has_value()) {
        return plan;
      }
      // A SUM/AVG over a computed child must not land in the unsigned
      // inference path: only exact int64/double results qualify.
      if ((type == AggregationType::kSum || type == AggregationType::kAvg) &&
          compiled->ResultType() != ValueType::kInt64 &&
          compiled->ResultType() != ValueType::kDouble) {
        return plan;
      }
      input.child = std::move(*compiled);
    }
    plan.aggregates.push_back(std::move(input));
  }
  // Slim-state classification: COUNT/SUM/AVG over signed INT64 or DOUBLE
  // inputs keep a packed count+total pair per group instead of the generic
  // ~300-byte accumulator.  One kNone anywhere keeps the whole plan on the
  // chunked-accumulator layout (MIN/MAX need the extreme Value, DISTINCT
  // needs the set, UINT64 needs the unsigned domain — none are slim).
  static const bool slim_disabled =
      std::getenv("TINYLAMB_DISABLE_SLIM_AGG") != nullptr;
  plan.slim_kinds.reserve(plan.aggregates.size());
  bool all_slim = !plan.aggregates.empty() && !slim_disabled;
  for (const TypedAggregateInput& input : plan.aggregates) {
    const AggregateExpression* aggregate = input.aggregate;
    const AggregationType type = aggregate->GetType();
    SlimAggKind kind = SlimAggKind::kNone;
    const bool distinct_free = !aggregate->Distinct();
    if (distinct_free && type == AggregationType::kCount) {
      kind = SlimAggKind::kCount;
    } else if (distinct_free && (type == AggregationType::kSum ||
                                 type == AggregationType::kAvg)) {
      ValueType input_type = ValueType::kNull;
      if (input.column.has_value()) {
        const Column& column =
            scan_schema.GetColumn(static_cast<slot_t>(*input.column));
        if (!column.IsUnsigned()) {
          input_type = column.Type();
        }
      } else if (input.child.has_value()) {
        input_type = input.child->ResultType();
      }
      if (type == AggregationType::kAvg) {
        if (input_type == ValueType::kInt64 ||
            input_type == ValueType::kDouble) {
          kind = SlimAggKind::kAvgDouble;
        }
      } else if (input_type == ValueType::kInt64) {
        kind = SlimAggKind::kSumInt64;
      } else if (input_type == ValueType::kDouble) {
        kind = SlimAggKind::kSumDouble;
      }
    } else if (!distinct_free && type == AggregationType::kCount) {
      // COUNT(DISTINCT ...) over an INT64/DATE input keeps the inline-slot
      // slim layout (same 8-slot threshold as the generic accumulator).
      ValueType input_type = ValueType::kNull;
      if (input.column.has_value()) {
        input_type =
            scan_schema.GetColumn(static_cast<slot_t>(*input.column)).Type();
      } else if (input.child.has_value()) {
        input_type = input.child->ResultType();
      }
      if (input_type == ValueType::kInt64 || input_type == ValueType::kDate) {
        kind = SlimAggKind::kCountDistinctInt64;
      }
    }
    plan.slim_kinds.push_back(kind);
    all_slim = all_slim && kind != SlimAggKind::kNone;
  }
  plan.slim = all_slim;
  // Classifies one output expression as a group-key column or a top-level
  // aggregate; `alias` names the select item it came from for ORDER BY
  // alias resolution (empty for direct select items).
  const auto classify = [&](const Expression& expression,
                            std::string_view alias,
                            TypedEmitItem* item) -> bool {
    if (!expression) {
      return false;
    }
    if (!alias.empty() && expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = TypedEmitItem{.is_key = false,
                              .index = static_cast<size_t>(std::distance(
                                  aggregate_expressions.begin(), pos))};
        return true;
      }
      return false;
    }
    if (expression->Type() == TypeTag::kColumnValue &&
        expression->AsColumnValue().GetColumnName().name != "*") {
      const auto offset = LocalColumnOffset(
          scan_schema, expression->AsColumnValue().GetColumnName());
      if (offset) {
        const auto key_pos =
            std::find(plan.key_offsets.begin(), plan.key_offsets.end(),
                      static_cast<slot_t>(*offset));
        if (key_pos != plan.key_offsets.end()) {
          *item = TypedEmitItem{.is_key = true,
                                .index = static_cast<size_t>(std::distance(
                                    plan.key_offsets.begin(), key_pos))};
          return true;
        }
      }
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = TypedEmitItem{.is_key = false,
                              .index = static_cast<size_t>(std::distance(
                                  aggregate_expressions.begin(), pos))};
        return true;
      }
    }
    return false;
  };
  for (const NamedExpression& item : stmt.SelectList()) {
    TypedEmitItem emit_item;
    if (!classify(item.expression, "", &emit_item)) {
      return plan;
    }
    plan.emit_items.push_back(emit_item);
  }
  for (const auto& term : stmt.OrderBy()) {
    TypedEmitItem order_item;
    if (term.expression && term.expression->Type() == TypeTag::kColumnValue &&
        term.expression->AsColumnValue().GetColumnName().schema.empty()) {
      // ORDER BY terms may reference select-list aliases.
      const ColumnName& name = term.expression->AsColumnValue().GetColumnName();
      bool matched = false;
      for (const auto& item : stmt.SelectList()) {
        if (item.name.size() != name.name.size() ||
            !std::equal(
                item.name.begin(), item.name.end(), name.name.begin(),
                [](char left, char right) {
                  return std::tolower(static_cast<unsigned char>(left)) ==
                         std::tolower(static_cast<unsigned char>(right));
                })) {
          continue;
        }
        if (!classify(item.expression, item.name, &order_item)) {
          return plan;
        }
        matched = true;
        break;
      }
      if (matched) {
        plan.order_items.push_back(order_item);
        plan.order_ascending.push_back(term.ascending);
        plan.order_nulls_first.push_back(
            term.nulls_first.value_or(term.ascending));
        continue;
      }
    }
    if (!classify(term.expression, "", &order_item)) {
      return plan;
    }
    plan.order_items.push_back(order_item);
    plan.order_ascending.push_back(term.ascending);
    plan.order_nulls_first.push_back(term.nulls_first.value_or(term.ascending));
  }
  if (having_expr) {
    if (ContainsQuery(having_expr) ||
        !HavingReferencesOnlyKeysAndAggregates(having_expr, scan_schema,
                                               plan.key_offsets)) {
      return plan;
    }
  }
  plan.eligible = true;
  return plan;
}

// Group states local to one worker (or to the merged result on the main
// thread).  Single INT64/DATE key statements index groups by the raw int
// value; everything else keys a canonical Value row.
// Chunked storage for the flat per-group accumulator states.  A plain
// std::vector<AggregateAccumulator> reallocates on geometric growth, and the
// realloc transient (old buffer + new buffer live together) peaks at ~3x the
// final state size — with 16 workers each folding 1.4M groups x ~300 bytes
// that was a ~20 GB transient spike whose page faults and cache thrash
// dominated the very scans the flat layout was meant to speed up.  Fixed
// chunks never move, and the chunk-pointer vector's growth moves 16-byte
// handles.
class AccumulatorChunks {
 public:
  static constexpr size_t kSlotsPerChunk = 512;

  void Configure(size_t stride) { stride_ = stride; }

  // Wholesale adoption when a merge target takes over a worker's whole state
  // (same stride and chunk geometry, so slot positions carry over as-is).
  void AdoptChunksFrom(AccumulatorChunks&& other) {
    chunks_ = std::move(other.chunks_);
    stride_ = other.stride_;
  }

  [[nodiscard]] AggregateAccumulator& State(size_t group, size_t i) {
    const size_t slot = (group * stride_) + i;
    return chunks_[slot / kSlotsPerChunk]->At(slot % kSlotsPerChunk);
  }
  [[nodiscard]] const AggregateAccumulator& State(size_t group,
                                                  size_t i) const {
    const size_t slot = (group * stride_) + i;
    return chunks_[slot / kSlotsPerChunk]->At(slot % kSlotsPerChunk);
  }

  void AppendGroup(const std::vector<const AggregateExpression*>& aggregates,
                   const std::vector<AggregateAccumulator::Shape>* shapes) {
    for (size_t i = 0; i < aggregates.size(); ++i) {
      if (chunks_.empty() || chunks_.back()->constructed() == kSlotsPerChunk) {
        chunks_.push_back(std::make_unique<Chunk>(kSlotsPerChunk));
      }
      if (shapes != nullptr) {
        chunks_.back()->ConstructAt((*aggregates[i]), (*shapes)[i]);
      } else {
        chunks_.back()->ConstructAt((*aggregates[i]));
      }
    }
  }

 private:
  class Chunk {
   public:
    explicit Chunk(size_t capacity)
        : storage_(new std::byte[capacity * sizeof(AggregateAccumulator)]) {}
    Chunk(const Chunk&) = delete;
    Chunk& operator=(const Chunk&) = delete;
    Chunk(Chunk&&) = delete;
    Chunk& operator=(Chunk&&) = delete;
    ~Chunk() {
      for (size_t i = 0; i < constructed_; ++i) {
        At(i).~AggregateAccumulator();
      }
    }
    AggregateAccumulator& At(size_t index) {
      return *std::launder(reinterpret_cast<AggregateAccumulator*>(
          storage_.get() + (index * sizeof(AggregateAccumulator))));
    }
    AggregateAccumulator& ConstructAt(const AggregateExpression& aggregate) {
      auto* placed =  // NOLINT(cppcoreguidelines-owning-memory)
          new (storage_.get() + (constructed_ * sizeof(AggregateAccumulator)))
              AggregateAccumulator(&aggregate);
      ++constructed_;
      return *placed;
    }
    AggregateAccumulator& ConstructAt(
        const AggregateExpression& aggregate,
        const AggregateAccumulator::Shape& shape) {
      auto* placed =  // NOLINT(cppcoreguidelines-owning-memory)
          new (storage_.get() + (constructed_ * sizeof(AggregateAccumulator)))
              AggregateAccumulator(&aggregate, shape);
      ++constructed_;
      return *placed;
    }
    [[nodiscard]] size_t constructed() const { return constructed_; }

   private:
    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    std::unique_ptr<std::byte[]> storage_;
    size_t constructed_{0};
  };

  std::vector<std::unique_ptr<Chunk>> chunks_;
  size_t stride_{1};
};

struct TypedWorkerGroups {
  bool int_key{false};
  // Open-addressing: a fresh multi-million-group build (Q18/Q21 shapes) costs
  // no per-group node allocation, unlike the node-based row_index below.
  FlatIntKeyMap<size_t> int_index;
  std::unordered_map<Row, size_t, GroupKeyTransparentHash,
                     GroupKeyTransparentEqual>
      row_index;
  // A/B hatch: TINYLAMB_DISABLE_FLAT_GROUP_FOLD=1 restores the per-group
  // vector-of-vector layout.  The flat layout below strides two contiguous
  // vectors instead: group g's canonical keys live at
  // flat_group_keys[g * key_stride + k] and its accumulators at
  // flat_group_states[g * agg_stride + i].  The previous layout cost one heap
  // allocation per group per container — across Q21's two 1.4M-group builds
  // and Q18's 1.5M-group build that is ~5.7M allocations the flat layout
  // never makes, with the group states gaining contiguous locality in the
  // bargain.
  bool flat_layout{true};
  // Slim SoA mode (plan.slim): no AggregateAccumulator storage at all —
  // per (group, aggregate) slots live in three parallel vectors indexed by
  // group * agg_stride + i.  Element width is 8 bytes, so geometric growth
  // moves are cheap (the chunked layout above exists for the ~300-byte
  // accumulators; these need no chunking).
  bool slim{false};
  const std::vector<SlimAggKind>* slim_kinds{nullptr};
  std::vector<int64_t> slim_counts;
  std::vector<int64_t> slim_int_totals;
  std::vector<double> slim_double_totals;
  std::vector<uint8_t> slim_flags;  // bit0: saw an infinite input (SUM double)
  // kCountDistinctInt64 state: kSlimDistinctSlots inline int64 slots per
  // (group, aggregate), the inline occupancy count, and a lazily materialized
  // overflow set.  Slot stride is kSlimDistinctSlots * agg_stride.
  std::vector<int64_t> slim_distinct;
  std::vector<uint8_t> slim_distinct_n;
  std::vector<std::unique_ptr<std::unordered_set<int64_t>>>
      slim_distinct_overflow;
  // Legacy layout (hatch only).
  std::vector<std::vector<Value>> group_keys;  // canonical key values
  std::vector<std::vector<AggregateAccumulator>> group_states;
  // Flat layout: canonical keys in one contiguous vector, accumulators in
  // fixed-size chunks (never moved, so no realloc transient).
  std::vector<Value> flat_group_keys;
  AccumulatorChunks chunked_states;
  size_t key_stride{0};
  size_t agg_stride{0};
  size_t group_count{0};
  // Per-aggregate classification shared by every FindOrInsert of this worker;
  // null falls back to per-construction analysis.
  const std::vector<AggregateAccumulator::Shape>* shapes{nullptr};

  void Configure(bool flat, size_t keys_per_group,
                 size_t aggregates_per_group) {
    flat_layout = flat;
    key_stride = keys_per_group;
    agg_stride = aggregates_per_group;
    chunked_states.Configure(std::max<size_t>(aggregates_per_group, 1));
  }

  void ConfigureSlim(const TypedAggregationPlan& plan) {
    slim = plan.slim;
    slim_kinds = &plan.slim_kinds;
  }

  // Slim state accessors for group g, aggregate i.
  [[nodiscard]] int64_t& SlimCount(size_t g, size_t i) {
    return slim_counts[(g * agg_stride) + i];
  }
  [[nodiscard]] int64_t& SlimIntTotal(size_t g, size_t i) {
    return slim_int_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] double& SlimDoubleTotal(size_t g, size_t i) {
    return slim_double_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] uint8_t& SlimFlags(size_t g, size_t i) {
    return slim_flags[(g * agg_stride) + i];
  }

  // Inserts one int64/date distinct value for kCountDistinctInt64; false when
  // it was already present.  Mirrors AggregateAccumulator::InsertDistinctInt.
  bool SlimInsertDistinct(size_t g, size_t i, int64_t raw) {
    const size_t state = (g * agg_stride) + i;
    int64_t* slots = &slim_distinct[state * kSlimDistinctSlots];
    const uint8_t n = slim_distinct_n[state];
    for (size_t s = 0; s < n; ++s) {
      if (slots[s] == raw) {
        return false;
      }
    }
    if (n < kSlimDistinctSlots) {
      slots[n] = raw;
      slim_distinct_n[state] = static_cast<uint8_t>(n + 1);
      return true;
    }
    auto& overflow = slim_distinct_overflow[state];
    if (!overflow) {
      overflow = std::make_unique<std::unordered_set<int64_t>>();
    }
    return overflow->insert(raw).second;
  }

  // Distinct values seen so far for kCountDistinctInt64 (the value COUNT
  // (DISTINCT) finishes to).
  [[nodiscard]] size_t SlimDistinctTotal(size_t g, size_t i) const {
    const size_t state = (g * agg_stride) + i;
    size_t total = slim_distinct_n[state];
    if (slim_distinct_overflow[state]) {
      total += slim_distinct_overflow[state]->size();
    }
    return total;
  }

  [[nodiscard]] size_t GroupCount() const {
    return flat_layout ? group_count : group_states.size();
  }
  [[nodiscard]] const Value& GroupKey(size_t group, size_t k) const {
    if (flat_layout) {
      return flat_group_keys[(group * key_stride) + k];
    }
    return group_keys[group][k];
  }
  [[nodiscard]] const AggregateAccumulator& State(size_t group,
                                                  size_t i) const {
    if (flat_layout) {
      return chunked_states.State(group, i);
    }
    return group_states[group][i];
  }
  AggregateAccumulator& State(size_t group, size_t i) {
    if (flat_layout) {
      return chunked_states.State(group, i);
    }
    return group_states[group][i];
  }

 private:
  // Slim group state grows in blocks of groups: one zeroing resize per block
  // instead of one tail-insert per group.  Multi-million-group folds paid a
  // vector::_M_fill_insert call (plus its capacity dance) per new group per
  // array before, which callgrind put at ~7% of the fold.  Capacity is
  // derived from the arrays themselves (not a separate counter), so the
  // merge path's std::move adoption carries the padding along for free.
  static constexpr size_t kSlimGroupBlock = 128;
  void ReserveSlimBlock() {
    const size_t stride = agg_stride;
    const size_t have_groups = slim_counts.size() / stride;
    if (group_count < have_groups) {
      return;
    }
    const size_t groups = have_groups + kSlimGroupBlock;
    const size_t cells = groups * stride;
    slim_counts.resize(cells, 0);
    slim_int_totals.resize(cells, 0);
    slim_double_totals.resize(cells, 0.0);
    slim_flags.resize(cells, 0);
    if (std::ranges::any_of(*slim_kinds, [](SlimAggKind kind) {
          return kind == SlimAggKind::kCountDistinctInt64;
        })) {
      slim_distinct.resize(cells * kSlimDistinctSlots, 0);
      slim_distinct_n.resize(cells, 0);
      slim_distinct_overflow.resize(cells);
    }
  }

  void AppendGroup(const std::vector<Value>& canonical_keys,
                   size_t aggregate_count,
                   const std::vector<const AggregateExpression*>& aggregates) {
    if (flat_layout) {
      flat_group_keys.insert(flat_group_keys.end(), canonical_keys.begin(),
                             canonical_keys.end());
      if (!slim) {
        chunked_states.AppendGroup(aggregates, shapes);
      }
    } else {
      group_keys.push_back(canonical_keys);
      group_states.emplace_back();
      auto& states = group_states.back();
      states.reserve(aggregate_count);
      for (size_t i = 0; i < aggregate_count; ++i) {
        if (shapes != nullptr) {
          states.emplace_back(aggregates[i], (*shapes)[i]);
        } else {
          states.emplace_back(aggregates[i]);
        }
      }
    }
    if (slim) {
      ReserveSlimBlock();
    }
    ++group_count;
  }

 public:
  size_t FindOrInsert(const std::vector<Value>& canonical_keys, int64_t raw_key,
                      size_t aggregate_count,
                      const std::vector<const AggregateExpression*>& aggregates,
                      bool* inserted) {
    // A NULL key must stay its own group: the raw int64 slot cannot encode
    // null-ness (NULL's bit pattern collides with 0), so NULL keys always go
    // through the canonical row index even on the int_key fast path.
    const bool use_int_key =
        int_key && !canonical_keys.empty() && !canonical_keys[0].IsNull();
    if (use_int_key) {
      bool emplaced = false;
      size_t* slot = int_index.TryEmplace(raw_key, &emplaced);
      *inserted = emplaced;
      if (emplaced) {
        *slot = GroupCount();
        AppendGroup(canonical_keys, aggregate_count, aggregates);
      }
      return *slot;
    }
    // Heterogeneous lookup: most rows land in an existing group, so probe
    // with the scratch key vector directly instead of paying a Row
    // construction (vector alloc + Value copies) per probe.  The transparent
    // hash reproduces std::hash<Row> exactly, so both paths agree.
    if (auto found = row_index.find(canonical_keys); found != row_index.end()) {
      *inserted = false;
      return found->second;
    }
    Row key(canonical_keys);
    auto [iter, emplaced] = row_index.try_emplace(std::move(key), GroupCount());
    *inserted = emplaced;
    if (emplaced) {
      AppendGroup(canonical_keys, aggregate_count, aggregates);
    }
    return iter->second;
  }

  // Raw-slot access for the arena fold fast path: the caller holds the raw
  // int64 key, probes it directly, and builds canonical keys only on first
  // insert.  Semantics identical to FindOrInsert's int-key branch (NULL keys
  // never reach here; callers fall back to the Row path for those).
  size_t* RawIntSlot(int64_t raw_key, bool* emplaced) {
    return int_index.TryEmplace(raw_key, emplaced);
  }
  // Warms the group slot for a key that FindOrInsert/RawIntSlot will probe a
  // few rows later; the intervening filter work hides the line fill.
  void PrefetchGroupSlot(int64_t raw_key) const { int_index.Prefetch(raw_key); }
  size_t ClaimSlotForNewGroup(
      size_t* slot, const std::vector<Value>& canonical_keys,
      size_t aggregate_count,
      const std::vector<const AggregateExpression*>& aggregates) {
    *slot = GroupCount();
    AppendGroup(canonical_keys, aggregate_count, aggregates);
    return *slot;
  }
};

namespace {

// Positional HAVING classification: `AGG op literal`, `literal op AGG`, or a
// bare `AGG`, where AGG is one of the plan's aggregate nodes.  These shapes
// cover the dominant HAVING forms (Q21's two `COUNT(DISTINCT ...) op n`
// folds evaluate one per merged group); deciding them straight from the
// finished values skips the per-group AggregateResultMap + Scope + expression
// walk, which costs several allocations per group on multi-million-group
// folds.  Anything else keeps the generic path.
struct PositionalHaving {
  size_t agg_index{0};
  BinaryOperation op{BinaryOperation::kEquals};
  Value constant;
  bool has_constant{false};
};

BinaryOperation FlipHavingCompare(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

std::optional<PositionalHaving> TryCompilePositionalHaving(
    const Expression& having,
    const std::vector<TypedAggregateInput>& aggregates) {
  if (!having) {
    return std::nullopt;
  }
  const bool is_binary = having->Type() == TypeTag::kBinaryExp;
  if (!is_binary) {
    // Bare `HAVING AGG`: truthy test on the finished value.
    if (having->Type() != TypeTag::kAggregateExp) {
      return std::nullopt;
    }
    const AggregateExpression* aggregate = &having->AsAggregateExpression();
    for (size_t i = 0; i < aggregates.size(); ++i) {
      if (aggregates[i].aggregate == aggregate) {
        PositionalHaving compiled;
        compiled.agg_index = i;
        compiled.has_constant = false;
        return compiled;
      }
    }
    return std::nullopt;
  }
  const BinaryExpression& binary = having->AsBinaryExpression();
  if (!IsComparison(binary.Op())) {
    return std::nullopt;
  }
  const Expression lhs = binary.Left();
  const Expression rhs = binary.Right();
  const AggregateExpression* aggregate = nullptr;
  bool agg_on_right = false;
  if (lhs && lhs->Type() == TypeTag::kAggregateExp) {
    aggregate = &lhs->AsAggregateExpression();
  } else if (rhs && rhs->Type() == TypeTag::kAggregateExp) {
    aggregate = &rhs->AsAggregateExpression();
    agg_on_right = true;
  } else {
    return std::nullopt;
  }
  const Expression& other = agg_on_right ? lhs : rhs;
  if (!other || other->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  size_t index = aggregates.size();
  for (size_t i = 0; i < aggregates.size(); ++i) {
    if (aggregates[i].aggregate == aggregate) {
      index = i;
      break;
    }
  }
  if (index == aggregates.size()) {
    return std::nullopt;
  }
  PositionalHaving compiled;
  compiled.agg_index = index;
  compiled.op = agg_on_right ? FlipHavingCompare(binary.Op()) : binary.Op();
  compiled.constant = other->AsConstantValue().GetValue();
  compiled.has_constant = true;
  return compiled;
}

}  // namespace

// One (group, aggregate) slim-state update for an already-resolved input
// value.  Shared by the Row-based accumulate loop and the cached-arena fold
// fast path so the two can never drift; false (with *error set) on a raise.
// A null `value` is the COUNT(*) sentinel and always counts.
bool ApplySlimValue(TypedWorkerGroups* local, size_t group_index, size_t i,
                    SlimAggKind kind, const Value* value, Status* error) {
  if (kind == SlimAggKind::kCount) {
    // COUNT(*): value == nullptr counts every surviving row; COUNT(expr)
    // skips NULLs exactly like the streaming accumulator.
    if (value == nullptr || !value->IsNull()) {
      ++local->SlimCount(group_index, i);
    }
    return true;
  }
  if (value == nullptr || value->IsNull()) {
    return true;
  }
  if (kind == SlimAggKind::kCountDistinctInt64) {
    // NULL inputs never enter the set (mirrored by the skip above); the
    // running count stays 0 because the finish derives the cardinality from
    // the inline occupancy and the overflow set.
    if (value->type == ValueType::kInt64 || value->type == ValueType::kDate) {
      static_cast<void>(
          local->SlimInsertDistinct(group_index, i, value->value.int_value));
      return true;
    }
    *error = StatusError(StatusCode::kInvalidArgument,
                         "slim COUNT(DISTINCT) input type mismatch");
    return false;
  }
  switch (kind) {
    case SlimAggKind::kSumInt64: {
      if (value->type != ValueType::kInt64 || value->IsUnsigned()) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim SUM input type mismatch");
        return false;
      }
      int64_t updated = 0;
      if (__builtin_add_overflow(local->SlimIntTotal(group_index, i),
                                 value->value.int_value, &updated)) {
        *error =
            StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
        return false;
      }
      local->SlimIntTotal(group_index, i) = updated;
      ++local->SlimCount(group_index, i);
      return true;
    }
    case SlimAggKind::kSumDouble: {
      if (value->type != ValueType::kDouble) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim SUM input type mismatch");
        return false;
      }
      if (std::isinf(value->value.double_value)) {
        local->SlimFlags(group_index, i) |= 1;
      }
      local->SlimDoubleTotal(group_index, i) += value->value.double_value;
      ++local->SlimCount(group_index, i);
      return true;
    }
    case SlimAggKind::kAvgDouble: {
      // AVG(INT64) sums through a double intermediate, so INT64
      // overflow never applies here (mirrors TryApplyCore).
      double addend = 0.0;
      if (value->type == ValueType::kDouble) {
        addend = value->value.double_value;
      } else if (value->type == ValueType::kInt64) {
        addend = value->IsUnsigned()
                     ? static_cast<double>(
                           static_cast<uint64_t>(value->value.int_value))
                     : static_cast<double>(value->value.int_value);
      } else {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim AVG input type mismatch");
        return false;
      }
      local->SlimDoubleTotal(group_index, i) += addend;
      ++local->SlimCount(group_index, i);
      return true;
    }
    default:
      *error = StatusError(StatusCode::kInvalidArgument,
                           "aggregate has no slim state");
      return false;
  }
}

// Per-morsel cache-fill buffer owned by a scan worker.  Rows land in the
// worker's vector (published in morsel order later); fixed-width fills also
// append row-major int64 cells + null bytes so the published cache gets its
// int64 arena mirror without per-row work under the publish lock.  The typed
// fill instead decodes straight into `mirror` (same arena layout, no Rows).
struct MorselFillBuffer {
  std::vector<Row> rows;
  size_t arena_stride{0};  // 0 = no arena mirror for this morsel.
  std::vector<int64_t> arena_cells;
  std::vector<uint8_t> arena_nulls;
  // Cells actually written (arena_cells/arena_nulls grow geometrically and
  // are cut to this length at publish, keeping the rows*stride == cells.size
  // invariant while skipping the per-row zeroing resize).
  size_t arena_used{0};
  // Typed-fill mirror (rows without Row objects); published through
  // BaseRelationArena::AppendTyped.
  BaseRelationArena mirror;
};

// Appends a decoded TypedCellBatch range (all columns, kept or filtered rows —
// cache fills are unfiltered) into an arena run: null bytes fold into the
// per-column bitmaps, cells copy verbatim, varchar bytes extend the run's
// byte arenas with offset-adjusted end marks.
void AppendBatchToArena(const TypedCellBatch& batch, size_t first_row,
                        size_t run_rows, BaseRelationArena* arena) {
  if (run_rows == 0) {
    return;
  }
  const size_t stride = arena->stride;
  const size_t first = arena->rows;
  arena->cells.insert(
      arena->cells.end(),
      batch.cells.begin() + static_cast<std::ptrdiff_t>(first_row * stride),
      batch.cells.begin() +
          static_cast<std::ptrdiff_t>((first_row + run_rows) * stride));
  for (size_t c = 0; c < stride; ++c) {
    auto& bits = arena->null_bits[c];
    bits.resize(((first + run_rows - 1) >> 6) + 1, 0);
    for (size_t r = 0; r < run_rows; ++r) {
      if (batch.nulls[((first_row + r) * stride) + c] != 0) {
        const size_t row = first + r;
        bits[row >> 6] |= uint64_t{1} << (row & 63);
      }
    }
  }
  size_t varchar_slot = 0;
  for (size_t c = 0; c < stride; ++c) {
    if (arena->slot_types[c] != ValueType::kVarChar) {
      continue;
    }
    BaseRelationArena::VarcharRun& run = arena->varchars[varchar_slot];
    const std::vector<uint32_t>& batch_ends = batch.varchar_ends[varchar_slot];
    const auto run_bytes = static_cast<uint32_t>(run.bytes.size());
    const uint32_t batch_prefix =
        first_row == 0 ? 0U : batch_ends[first_row - 1];
    run.bytes.insert(run.bytes.end(),
                     batch.varchar_bytes[varchar_slot].begin() + batch_prefix,
                     batch.varchar_bytes[varchar_slot].begin() +
                         batch_ends[first_row + run_rows - 1]);
    run.ends.reserve(run.ends.size() + run_rows);
    for (size_t r = 0; r < run_rows; ++r) {
      run.ends.push_back(run_bytes +
                         (batch_ends[first_row + r] - batch_prefix));
    }
    ++varchar_slot;
  }
  arena->rows += run_rows;
}

// Typed cell value for the arena fold: the payload layout follows the slot
// type (DOUBLE bit-casts through the int64 cell; VARCHAR slots never reach
// this helper on the raw-cell paths because their folds need byte strings).
inline Value ArenaCellValue(const BaseRelationArena& arena,
                            const int64_t* cells, size_t column,
                            const Schema& schema) {
  Value value;
  const ValueType type = arena.slot_types.size() == arena.stride
                             ? arena.slot_types[column]
                             : schema.GetColumn(column).Type();
  value.type = type;
  if (type == ValueType::kDouble) {
    value.value.double_value = std::bit_cast<double>(cells[column]);
  } else {
    value.value.int_value = cells[column];
  }
  return value;
}

// MaterializeArenaRange and MaterializeCacheRowsIfNeeded live in
// subquery_runtime.cpp next to the arena they serve.

// Appends one decoded row to the morsel fill buffer (Row + optional arena
// mirror).  `row` must not be touched afterwards when !fill_copy (the row
// moves).
template <typename RowLike>
void AppendFillRow(RowLike&& row, MorselFillBuffer* fill, bool fill_copy) {
  if (fill->arena_stride != 0) {
    const size_t width = fill->arena_stride;
    const size_t base = fill->arena_used;
    if (base + width > fill->arena_cells.size()) {
      const size_t grown = std::max(base + width, fill->arena_cells.size() * 2);
      fill->arena_cells.resize(grown, 0);
      fill->arena_nulls.resize(grown, 0);
    }
    for (size_t c = 0; c < width && c < row.values_.size(); ++c) {
      const Value& value = row.values_[c];
      fill->arena_cells[base + c] = value.value.int_value;
      fill->arena_nulls[base + c] = value.IsNull() ? 1 : 0;
    }
    fill->arena_used = base + width;
  }
  if (fill_copy) {
    fill->rows.emplace_back(row);
  } else {
    fill->rows.push_back(std::forward<RowLike>(row));
  }
}

StatusOr<Relation> RunTypedParallelStreamAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::string& table_name, bool reusable,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema,
    const std::vector<IntegerPeekCompare>& peek_compares,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const CteMap& ctes, TypedAggregationPlan plan) {
  ExecutionRuntime* runtime = context.execution_runtime();
  const auto scan_begin = std::chrono::steady_clock::now();
  const std::vector<Table::ScanMorsel> morsels =
      table.BuildScanMorsels(context.txn_, 8);
  const bool int_key =
      plan.key_offsets.size() == 1 &&
      (qualified_schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kInt64 ||
       qualified_schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kDate);
  // Slot -> VARCHAR-ordinal map for the cell fold (SIZE_MAX = fixed-width
  // slot); filled by the arena setup below.
  std::vector<size_t> varchar_ordinal;
  // Peek compares pre-reject rows inside the scan; they must be disabled
  // while filling the shared cache, whose contract is unfiltered rows.
  const std::optional<std::vector<slot_t>> projection_opt =
      projection.empty() ? std::nullopt
                         : std::optional<std::vector<slot_t>>(projection);

  // Shared base-relation cache: fill on miss (unfiltered rows, per the
  // BaseRelationCacheKey contract), aggregate from it on hit.
  std::shared_ptr<Relation> cached_relation;
  std::shared_ptr<const BaseRelationArena> fold_arena;
  std::string fold_cache_key;
  if (reusable && runtime != nullptr) {
    fold_cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    auto found = runtime->base_relations.find(fold_cache_key);
    if (found != runtime->base_relations.end()) {
      cached_relation = found->second;
      ++runtime->base_scan_cache_hits;
    } else {
      cached_relation = nullptr;
    }
  }
  const bool fill_cache =
      cached_relation == nullptr && reusable && runtime != nullptr;
  const std::vector<IntegerPeekCompare>* peek_ptr =
      fill_cache || peek_compares.empty() ? nullptr : &peek_compares;
  // A/B hatch: TINYLAMB_DISABLE_FILL_COPY=1 restores move-append into the
  // cache-fill buffer.  Copying FIXED-WIDTH fills keeps the scan iterator's
  // decode buffer alive (no per-row vector reallocation for 6M+ rows), the
  // same trade ParallelScan makes for its chunk appends; VARCHAR/ARRAY cells
  // keep the move so long text never deep-copies.
  static const bool fill_copy_disabled =
      std::getenv("TINYLAMB_DISABLE_FILL_COPY") != nullptr;
  bool fill_copy = !fill_copy_disabled;
  for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
    const ValueType type = qualified_schema.GetColumn(i).Type();
    if (type == ValueType::kVarChar || type == ValueType::kArray) {
      fill_copy = false;
      break;
    }
  }

  const auto accumulate_row = [&](auto&& row, TypedWorkerGroups* local,
                                  Status* error, size_t* rows_output,
                                  size_t* updates, MorselFillBuffer* fill,
                                  std::vector<Value>* scratch) -> void {
    // Cache fills must capture UNFILTERED rows (BaseRelationCacheKey
    // contract): every consumer, including this statement, applies its own
    // predicates while reading or aggregating.  The fill below MOVES the row
    // instead of copying it (the copy dominated cache-fill time for the
    // multi-million-row base relations behind Q18/Q21); every read of `row`
    // therefore has to happen before the move.  A raise that skips the fill
    // is unobservable: the cache is only published after a fully successful
    // run.
    // Warms the int-key group slot while the filter work below runs; the key
    // cell was just decoded, so reading it here is free and the slot line is
    // resident by the time FindOrInsert probes.
    if (int_key && !plan.key_offsets.empty() && local != nullptr) {
      const Value& key_cell = row[plan.key_offsets[0]];
      if (!key_cell.IsNull()) {
        local->PrefetchGroupSlot(key_cell.value.int_value);
      }
    }
    bool keep = true;
    for (const TypedFilterCompare& compare : plan.filter_compares) {
      const Value& left = row[compare.left];
      if (compare.constant_payload) {
        if (left.IsNull() || !DecideTypedFilterCompare(left, left, compare)) {
          keep = false;
          break;
        }
        continue;
      }
      const Value& right = row[compare.right];
      if (left.IsNull() || right.IsNull() ||
          !DecideTypedFilterCompare(left, right, compare)) {
        keep = false;
        break;
      }
    }
    if (keep) {
      for (const TypedInListCompare& list : plan.inlist_compares) {
        const Value& cell = row[list.column];
        if (cell.IsNull()) {
          keep = false;
          break;
        }
        bool matched = false;
        if (list.is_string) {
          const std::string_view v = cell.value.varchar_value;
          for (const std::string& item : list.strings) {
            if (v == std::string_view(item)) {
              matched = true;
              break;
            }
          }
        } else if (list.is_double) {
          for (const double item : list.double_values) {
            if (cell.value.double_value == item) {
              matched = true;
              break;
            }
          }
        } else {
          for (const int64_t item : list.int_values) {
            if (cell.value.int_value == item) {
              matched = true;
              break;
            }
          }
        }
        if (!matched) {
          keep = false;
          break;
        }
      }
    }
    for (const BytecodeProgram& program : plan.filter_programs) {
      StatusOr<Value> decided = TryEvaluateRowAuto(program, row);
      if (!decided.HasValue()) {
        *error = decided.GetStatus();
        return;
      }
      if (!Truthy(decided.Value())) {
        keep = false;
        break;
      }
    }
    if (!keep) {
      if (fill != nullptr) {
        AppendFillRow(row, fill, fill_copy);
      }
      return;
    }
    ++*rows_output;
    bool inserted = false;
    size_t group_index = 0;
    // Int-key fast path: probe the raw slot with the cell value directly and
    // build canonical keys only when a new group actually opens.  The hit
    // path (the overwhelmingly common case on multi-million-row folds) then
    // constructs no Value at all for the group key.
    if (int_key && !plan.key_offsets.empty() &&
        !row[plan.key_offsets[0]].IsNull()) {
      const Value& key_cell = row[plan.key_offsets[0]];
      size_t* slot = local->RawIntSlot(key_cell.value.int_value, &inserted);
      if (inserted) {
        scratch->clear();
        scratch->push_back(CanonicalDistinctValue(key_cell));
        group_index = local->ClaimSlotForNewGroup(
            slot, *scratch, plan.aggregates.size(), aggregate_expressions);
      } else {
        group_index = *slot;
      }
    } else {
      scratch->clear();
      int64_t raw_key = 0;
      for (const slot_t offset : plan.key_offsets) {
        scratch->push_back(CanonicalDistinctValue(row[offset]));
        if (scratch->size() == 1 && int_key) {
          raw_key = row[offset].value.int_value;
        }
      }
      group_index =
          local->FindOrInsert(*scratch, raw_key, plan.aggregates.size(),
                              aggregate_expressions, &inserted);
    }
    // Counted per worker and folded into the shared runtime stat after the
    // join: multiple workers racing on the plain counter is a data race.
    ++*updates;
    if (local->slim) {
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const TypedAggregateInput& input = plan.aggregates[i];
        const Value* value = nullptr;
        Value evaluated;
        if (input.column.has_value()) {
          value = &row[*input.column];
        } else if (input.child.has_value()) {
          StatusOr<Value> child_value = TryEvaluateRowAuto(*input.child, row);
          if (!child_value.HasValue()) {
            *error = child_value.GetStatus();
            return;
          }
          evaluated = child_value.MoveValue();
          value = &evaluated;
        }
        if (!ApplySlimValue(local, group_index, i, (*local->slim_kinds)[i],
                            value, error)) {
          return;
        }
      }
      if (fill != nullptr) {
        AppendFillRow(row, fill, fill_copy);
      }
      return;
    }
    for (size_t i = 0; i < plan.aggregates.size(); ++i) {
      const TypedAggregateInput& input = plan.aggregates[i];
      AggregateAccumulator& accumulator = local->State(group_index, i);
      if (input.column.has_value()) {
        if (Status st = accumulator.Add(row[*input.column]);
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (!input.child.has_value()) {
        // COUNT(*).
        if (Status st = accumulator.Add(Value(1)); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      StatusOr<Value> value = TryEvaluateRowAuto(*input.child, row);
      if (!value.HasValue()) {
        *error = value.GetStatus();
        return;
      }
      if (Status st = accumulator.Add(value.Value()); st != Status::kSuccess) {
        *error = st;
        return;
      }
    }
    if (fill != nullptr) {
      AppendFillRow(row, fill, fill_copy);
    }
  };

  // Typed-fill fold: one decoded cell row of a TypedCellBatch through the
  // same filter/key/slim ladder as accumulate_row above, never constructing a
  // Row.  `mirror` (per-morsel arena run) receives every row, filtered or
  // not — cache fills capture UNFILTERED rows (BaseRelationCacheKey
  // contract).  Declined shapes keep the Row loop above; the value contracts
  // (canonical keys, NULL handling, slim kinds) are shared helpers, so the
  // two paths cannot drift semantically.
  const auto fold_cell_row = [&](const TypedCellBatch& batch, size_t r,
                                 TypedWorkerGroups* local, Status* error,
                                 size_t* rows_output, size_t* updates,
                                 std::vector<Value>* scratch,
                                 std::array<Value, 8>* row_values) -> void {
    const size_t base = r * batch.stride;
    const auto make_value = [&](slot_t c) {
      Value value;
      switch (batch.slot_types[c]) {
        case ValueType::kDouble:
          value.type = ValueType::kDouble;
          value.value.double_value =
              std::bit_cast<double>(batch.cells[base + c]);
          break;
        case ValueType::kVarChar:
          // Probe-only view into the batch; copies become owned (Value copy
          // ctor) the moment a key or accumulator retains one.
          value.type = ValueType::kVarChar;
          value.value.varchar_value = batch.Varchar(r, varchar_ordinal[c]);
          break;
        default:
          value.type = batch.slot_types[c];
          value.value.int_value = batch.cells[base + c];
          break;
      }
      return value;
    };
    const auto is_null = [&](slot_t c) { return batch.nulls[base + c] != 0; };
    bool keep = true;
    for (const TypedFilterCompare& compare : plan.filter_compares) {
      if (is_null(compare.left)) {
        keep = false;
        break;
      }
      Value left = make_value(compare.left);
      if (compare.constant_payload) {
        if (!DecideTypedFilterCompare(left, left, compare)) {
          keep = false;
          break;
        }
        continue;
      }
      if (is_null(compare.right)) {
        keep = false;
        break;
      }
      Value right = make_value(compare.right);
      if (!DecideTypedFilterCompare(left, right, compare)) {
        keep = false;
        break;
      }
    }
    if (keep) {
      for (const TypedInListCompare& list : plan.inlist_compares) {
        if (is_null(list.column)) {
          keep = false;
          break;
        }
        bool matched = false;
        if (list.is_string) {
          const std::string_view v =
              make_value(list.column).value.varchar_value;
          for (const std::string& item : list.strings) {
            if (v == std::string_view(item)) {
              matched = true;
              break;
            }
          }
        } else if (list.is_double) {
          const double v = make_value(list.column).value.double_value;
          for (const double item : list.double_values) {
            if (v == item) {
              matched = true;
              break;
            }
          }
        } else {
          const int64_t v = batch.cells[base + list.column];
          for (const int64_t item : list.int_values) {
            if (v == item) {
              matched = true;
              break;
            }
          }
        }
        if (!matched) {
          keep = false;
          break;
        }
      }
    }
    if (!keep) {
      return;
    }
    ++*rows_output;
    bool inserted = false;
    size_t group_index = 0;
    if (int_key && !plan.key_offsets.empty() && !is_null(plan.key_offsets[0])) {
      const int64_t raw_key = batch.cells[base + plan.key_offsets[0]];
      size_t* slot = local->RawIntSlot(raw_key, &inserted);
      if (inserted) {
        scratch->clear();
        for (const slot_t offset : plan.key_offsets) {
          scratch->push_back(CanonicalDistinctValue(make_value(offset)));
        }
        group_index = local->ClaimSlotForNewGroup(
            slot, *scratch, plan.aggregates.size(), aggregate_expressions);
      } else {
        group_index = *slot;
      }
    } else {
      scratch->clear();
      int64_t raw_key = 0;
      for (const slot_t offset : plan.key_offsets) {
        if (is_null(offset)) {
          scratch->emplace_back();
        } else {
          scratch->push_back(CanonicalDistinctValue(make_value(offset)));
        }
        if (scratch->size() == 1 && int_key) {
          raw_key = scratch->back().value.int_value;
        }
      }
      group_index =
          local->FindOrInsert(*scratch, raw_key, plan.aggregates.size(),
                              aggregate_expressions, &inserted);
    }
    ++*updates;
    if (local->slim) {
      // Raw-cell fast path: every slim kind reads at most one column cell,
      // so the common payload kinds (COUNT / SUM / AVG / COUNT(DISTINCT)
      // over fixed-width cells) fold straight from batch.cells without
      // boxing a Value per input.  NULL inputs skip exactly like the generic
      // path (ApplySlimValue returns early on IsNull for every kind); a kind
      // or cell type the raw switch does not cover falls through to the
      // generic boxed call, which raises the same type-mismatch errors.
      const std::vector<SlimAggKind>& kinds = *local->slim_kinds;
      std::array<Value, 8>& stack_values = *row_values;
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const TypedAggregateInput& input = plan.aggregates[i];
        const SlimAggKind kind = kinds[i];
        if (!input.column.has_value()) {
          // COUNT(*) (the only no-column slim input) counts unconditionally;
          // any other shape keeps the generic nullptr sentinel path.
          if (kind == SlimAggKind::kCount) {
            ++local->SlimCount(group_index, i);
            continue;
          }
          if (!ApplySlimValue(local, group_index, i, kind, nullptr, error)) {
            return;
          }
          continue;
        }
        const slot_t column = *input.column;
        if (is_null(column)) {
          continue;
        }
        const ValueType cell_type = batch.slot_types[column];
        const int64_t cell = batch.cells[base + column];
        bool handled = true;
        switch (kind) {
          case SlimAggKind::kCount:
            ++local->SlimCount(group_index, i);
            break;
          case SlimAggKind::kSumInt64:
            if (cell_type == ValueType::kInt64 &&
                !batch.slot_unsigned[column]) {
              int64_t updated = 0;
              if (__builtin_add_overflow(local->SlimIntTotal(group_index, i),
                                         cell, &updated)) {
                *error = StatusError(StatusCode::kIsInfinity,
                                     "integer overflow in SUM");
                return;
              }
              local->SlimIntTotal(group_index, i) = updated;
              ++local->SlimCount(group_index, i);
            } else {
              handled = false;
            }
            break;
          case SlimAggKind::kSumDouble:
            if (cell_type == ValueType::kDouble) {
              const auto addend = std::bit_cast<double>(cell);
              if (std::isinf(addend)) {
                local->SlimFlags(group_index, i) |= 1;
              }
              local->SlimDoubleTotal(group_index, i) += addend;
              ++local->SlimCount(group_index, i);
            } else {
              handled = false;
            }
            break;
          case SlimAggKind::kAvgDouble:
            if (cell_type == ValueType::kDouble) {
              local->SlimDoubleTotal(group_index, i) +=
                  std::bit_cast<double>(cell);
              ++local->SlimCount(group_index, i);
            } else if (cell_type == ValueType::kInt64) {
              local->SlimDoubleTotal(group_index, i) +=
                  batch.slot_unsigned[column]
                      ? static_cast<double>(static_cast<uint64_t>(cell))
                      : static_cast<double>(cell);
              ++local->SlimCount(group_index, i);
            } else {
              handled = false;
            }
            break;
          case SlimAggKind::kCountDistinctInt64:
            if (cell_type == ValueType::kInt64 ||
                cell_type == ValueType::kDate) {
              static_cast<void>(
                  local->SlimInsertDistinct(group_index, i, cell));
            } else {
              handled = false;
            }
            break;
          default:
            handled = false;
            break;
        }
        if (handled) {
          continue;
        }
        stack_values[i] = make_value(column);
        if (!ApplySlimValue(local, group_index, i, kind, &stack_values[i],
                            error)) {
          return;
        }
      }
      return;
    }
    for (size_t i = 0; i < plan.aggregates.size(); ++i) {
      const TypedAggregateInput& input = plan.aggregates[i];
      AggregateAccumulator& accumulator = local->State(group_index, i);
      if (input.column.has_value()) {
        const Value value =
            is_null(*input.column) ? Value() : make_value(*input.column);
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      // COUNT(*): the typed fill declines expression aggregates, so a
      // column-less input here is exactly COUNT(*).
      if (Status st = accumulator.Add(Value(1)); st != Status::kSuccess) {
        *error = st;
        return;
      }
    }
  };

  // Per-morsel fill buffers are moved into the published cache relation as
  // soon as their predecessors have landed, which keeps the cache row order
  // identical to a page-ordered serial scan while holding at most one buffer
  // per worker in flight (never the whole table twice).
  std::mutex publish_mutex;
  size_t next_to_publish = 0;
  std::map<size_t, MorselFillBuffer> pending_publish;
  // Rows landed in the published cache (Row-fill appends plus typed-fill
  // mirrors) — the arena registration guard compares against this, because a
  // typed fill publishes no Rows at all.
  size_t published_rows = 0;
  // Owned unconditionally while filling so an error return frees the rows.
  std::shared_ptr<Relation> cache_owner(fill_cache ? new Relation(runtime)
                                                   : nullptr);
  Relation* cache_target = cache_owner.get();
  if (cache_target != nullptr) {
    cache_target->schema = qualified_schema;
  }
  // A/B hatch: TINYLAMB_DISABLE_AGG_ARENA=1 keeps the cache Row-only.  The
  // arena mirrors the rows with schema-typed cells (INT64/DATE/DOUBLE as
  // 8-byte payloads, VARCHAR bytes in per-slot arenas) so the typed fold's
  // cache-hit pass can stream cells without visiting per-Row heap blocks
  // (Q18/Q21's multi-million-row folds).
  static const bool arena_disabled =
      std::getenv("TINYLAMB_DISABLE_AGG_ARENA") != nullptr;
  std::shared_ptr<BaseRelationArena> fill_arena;
  std::vector<int64_t> arena_cell_buffer;
  std::vector<char> arena_null_buffer;
  // Typed fill (TINYLAMB_TYPED_FILL=1): the scan decodes schema-typed cell
  // batches instead of Rows, the fold consumes raw cells, and the cache is
  // published with the arena only (rows are materialized from it on demand by
  // consumers that cannot take the arena path).  Declined when the plan needs
  // Row-shaped inputs: residual filter programs or expression aggregates.
  static const bool typed_fill_enabled =
      std::getenv("TINYLAMB_DISABLE_TYPED_FILL") == nullptr;
  bool typed_fill = false;
  if (fill_cache && !arena_disabled) {
    bool all_typed = qualified_schema.ColumnCount() > 0;
    for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
      const ValueType type = qualified_schema.GetColumn(i).Type();
      if (type != ValueType::kInt64 && type != ValueType::kDate &&
          type != ValueType::kDouble && type != ValueType::kVarChar) {
        all_typed = false;
        break;
      }
    }
    if (all_typed) {
      fill_arena = std::make_shared<BaseRelationArena>();
      fill_arena->stride = qualified_schema.ColumnCount();
      fill_arena->slot_types.reserve(fill_arena->stride);
      size_t varchar_count = 0;
      for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
        const ValueType type = qualified_schema.GetColumn(i).Type();
        fill_arena->slot_types.push_back(type);
        if (type == ValueType::kVarChar) {
          ++varchar_count;
        }
      }
      fill_arena->null_bits.resize(fill_arena->stride);
      fill_arena->varchars.resize(varchar_count);
      arena_cell_buffer.resize(fill_arena->stride);
      arena_null_buffer.resize(fill_arena->stride);
      varchar_ordinal.assign(fill_arena->stride, SIZE_MAX);
      size_t varchar_ordinal_next = 0;
      for (size_t i = 0; i < fill_arena->stride; ++i) {
        if (fill_arena->slot_types[i] == ValueType::kVarChar) {
          varchar_ordinal[i] = varchar_ordinal_next++;
        }
      }
      typed_fill = typed_fill_enabled && plan.filter_programs.empty();
      if (typed_fill) {
        for (const TypedAggregateInput& input : plan.aggregates) {
          if (input.child.has_value()) {
            typed_fill = false;  // expression aggregates need Rows.
            break;
          }
        }
      }
    }
  }
  const auto publish_morsel = [&](size_t morsel, MorselFillBuffer* buffer) {
    std::scoped_lock lock(publish_mutex);
    // Cut the geometric arena buffers to the written length so the
    // rows*stride == cells.size invariant below holds exactly.
    buffer->arena_cells.resize(buffer->arena_used);
    buffer->arena_nulls.resize(buffer->arena_used);
    pending_publish.emplace(morsel, std::move(*buffer));
    buffer->rows.clear();
    buffer->rows.shrink_to_fit();
    buffer->arena_cells.clear();
    buffer->arena_cells.shrink_to_fit();
    buffer->arena_nulls.clear();
    buffer->arena_nulls.shrink_to_fit();
    while (pending_publish.contains(next_to_publish)) {
      auto node = pending_publish.extract(next_to_publish);
      // Plain push_back: repeated exact reserves would move every existing
      // row per morsel (quadratic).  Geometric growth moves are cheap.
      for (Row& row : node.mapped().rows) {
        cache_target->rows.push_back(std::move(row));
      }
      published_rows += node.mapped().rows.size();
      // Splice the morsel's arena mirror (bulk memcpy, still row order); a
      // layout mismatch drops the mirror — the registration guard keeps an
      // unusable partial out of the runtime map.
      if (fill_arena != nullptr) {
        if (node.mapped().mirror.rows > 0) {
          if (node.mapped().mirror.stride == fill_arena->stride &&
              node.mapped().mirror.slot_types == fill_arena->slot_types) {
            fill_arena->AppendTyped(node.mapped().mirror);
            published_rows += node.mapped().mirror.rows;
          } else {
            fill_arena.reset();
          }
        } else if (node.mapped().arena_stride == fill_arena->stride &&
                   node.mapped().rows.size() * fill_arena->stride ==
                       node.mapped().arena_cells.size()) {
          fill_arena->AppendRun(node.mapped().arena_cells.data(),
                                node.mapped().arena_nulls.data(),
                                node.mapped().rows.size());
        } else if (node.mapped().arena_stride != 0) {
          fill_arena.reset();
        }
      }
      ++next_to_publish;
    }
  };

  auto worker_count = std::min<size_t>(
      {std::thread::hardware_concurrency(),
       cached_relation != nullptr ? size_t{16} : morsels.size(), size_t{16}});
  if (worker_count < 2) {
    worker_count = 1;
  }
  // The per-aggregate classification (array element type, SUM column-name
  // scan) is identical for every group of every worker; derive it once.
  std::vector<AggregateAccumulator::Shape> aggregate_shapes;
  aggregate_shapes.reserve(aggregate_expressions.size());
  for (const AggregateExpression* aggregate : aggregate_expressions) {
    aggregate_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
  }
  // A/B hatch: TINYLAMB_DISABLE_FLAT_GROUP_FOLD=1 restores the per-group
  // vector-of-vector group storage.
  static const bool flat_fold_disabled =
      std::getenv("TINYLAMB_DISABLE_FLAT_GROUP_FOLD") != nullptr;
  std::vector<TypedWorkerGroups> workers(worker_count);
  for (TypedWorkerGroups& local : workers) {
    local.int_key = int_key;
    local.shapes = &aggregate_shapes;
    local.Configure(!flat_fold_disabled, plan.key_offsets.size(),
                    plan.aggregates.size());
    local.ConfigureSlim(plan);
  }
  // A typed fill publishes the arena without Rows.  A hit whose fold shape
  // CAN stream cells goes straight to the arena fold; anything else boxes the
  // rows back here, before the workers spawn (MaterializeCacheRowsParallel
  // latches rows_boxed, so the resized-but-unboxed state is never observable).
  // Mirror of the worker's arena_ok gate: when the fold CANNOT stream cells,
  // box the arena's rows back (parallel) before spawning workers.
  if (cached_relation != nullptr && !arena_disabled && runtime != nullptr &&
      !workers.empty()) {
    // Same formula as the worker's arena_ok gate: when this fold cannot
    // stream cells (non-slim shapes included), the arena-only cache must be
    // boxed back into rows before the workers spawn.
    bool fold_can_stream_arena = workers[0].slim && int_key &&
                                 plan.filter_programs.empty() &&
                                 plan.aggregates.size() <= 8;
    if (fold_can_stream_arena) {
      for (const TypedAggregateInput& input : plan.aggregates) {
        const auto aggregate_index =
            static_cast<size_t>(&input - plan.aggregates.data());
        if (input.child.has_value() ||
            (!input.column.has_value() &&
             (*workers[0].slim_kinds)[aggregate_index] !=
                 SlimAggKind::kCount)) {
          fold_can_stream_arena = false;
          break;
        }
      }
    }
    if (!fold_can_stream_arena && cached_relation->rows.empty()) {
      MaterializeCacheRowsParallel(fold_cache_key, cached_relation.get(),
                                   runtime);
    }
  }
  std::vector<Status> worker_errors(worker_count, Status::kSuccess);
  std::vector<size_t> scanned(worker_count, 0);
  std::vector<size_t> emitted(worker_count, 0);
  std::vector<size_t> aggregate_updates(worker_count, 0);
  std::exception_ptr worker_exception;
  const auto run_worker = [&](size_t worker) {
    try {
      std::vector<Value> scratch;
      scratch.reserve(plan.key_offsets.size());
      if (cached_relation != nullptr && !cached_relation->HasSpill()) {
        // Cache hit: partition the resident rows into contiguous ranges.  A
        // typed fill publishes the arena WITHOUT Rows (resized-but-unboxed
        // once a presize ran); the arena lookup therefore cannot key on
        // rows.empty().
        const std::vector<Row>& rows = cached_relation->rows;
        std::shared_ptr<const BaseRelationArena> hit_arena;
        if (runtime != nullptr) {
          auto found_arena = runtime->base_relation_arenas.find(fold_cache_key);
          if (found_arena != runtime->base_relation_arenas.end()) {
            hit_arena = found_arena->second;
          }
        }
        const size_t total =
            !rows.empty() ? rows.size() : (hit_arena ? hit_arena->rows : 0);
        const size_t begin = total * worker / worker_count;
        const size_t end = total * (worker + 1) / worker_count;
        // Scratch arrays for the arena fold below; every slot is written
        // before it is read within a row, so they carry no cross-row state.
        std::array<Value, 8> cache_hit_values{};
        std::array<const Value*, 8> cache_hit_value_ptrs{};
        // Arena fast path: an int64 mirror of the cache lets the fold stream
        // raw cells instead of visiting per-Row heap blocks.  Requires a
        // single int64/date group key, no bytecode filter residual, slim
        // states, and column-only (or COUNT(*)) aggregate inputs; anything
        // else keeps the Row loop below.  Row and arena are published
        // together, so equal row counts mean the arena is complete.
        std::shared_ptr<const BaseRelationArena> arena;
        bool arena_ok = !arena_disabled && workers[worker].slim && int_key &&
                        plan.filter_programs.empty() &&
                        plan.aggregates.size() <= 8;
        if (arena_ok) {
          for (const TypedAggregateInput& input : plan.aggregates) {
            const auto aggregate_index =
                static_cast<size_t>(&input - plan.aggregates.data());
            if (input.child.has_value() ||
                (!input.column.has_value() &&
                 (*workers[worker].slim_kinds)[aggregate_index] !=
                     SlimAggKind::kCount)) {
              arena_ok = false;
              break;
            }
          }
        }
        if (arena_ok && hit_arena != nullptr && hit_arena->rows == total) {
          arena = hit_arena;
        }
        if (arena != nullptr) {
          const size_t stride = arena->stride;
          std::vector<Value> fallback_row_values;
          std::vector<int64_t> fallback_cells(stride);
          constexpr size_t kArenaPrefetchDistance = 8;
          for (size_t index = begin; index < end; ++index) {
            if (worker_errors[worker] != Status::kSuccess) {
              return;
            }
            ++scanned[worker];
            const int64_t* cells = arena->cells.data() + (index * stride);
            // The cells and null bitmap stream linearly (hardware prefetch
            // covers them); the group-slot probe is the random access, so
            // warm it one window ahead of the row that needs it.
            if (index + kArenaPrefetchDistance < end) {
              const int64_t* ahead = cells + (kArenaPrefetchDistance * stride);
              __builtin_prefetch(ahead);
              __builtin_prefetch(ahead + plan.key_offsets[0]);
              if (!arena->IsNull(index + kArenaPrefetchDistance,
                                 plan.key_offsets[0])) {
                workers[worker].PrefetchGroupSlot(ahead[plan.key_offsets[0]]);
              }
            }
            if (arena->IsNull(index, plan.key_offsets[0])) {
              // NULL keys keep their own group through the canonical row
              // path; materialize the row lazily for the rare case.
              fallback_row_values.clear();
              for (size_t c = 0; c < stride; ++c) {
                if (arena->IsNull(index, c)) {
                  fallback_row_values.emplace_back();
                } else {
                  fallback_row_values.push_back(
                      ArenaCellValue(*arena, cells, c, qualified_schema));
                }
              }
              Row fallback_row(std::move(fallback_row_values));
              accumulate_row(fallback_row, &workers[worker],
                             &worker_errors[worker], &emitted[worker],
                             &aggregate_updates[worker], nullptr, &scratch);
              fallback_row_values = std::move(fallback_row.values_);
              continue;
            }
            bool keep = true;
            for (const TypedFilterCompare& compare : plan.filter_compares) {
              if (arena->IsNull(index, compare.left)) {
                keep = false;
                break;
              }
              if (compare.constant_payload) {
                Value left_value = ArenaCellValue(*arena, cells, compare.left,
                                                  qualified_schema);
                if (!DecideTypedFilterCompare(left_value, left_value,
                                              compare)) {
                  keep = false;
                  break;
                }
                continue;
              }
              if (arena->IsNull(index, compare.right)) {
                keep = false;
                break;
              }
              Value left_value =
                  ArenaCellValue(*arena, cells, compare.left, qualified_schema);
              Value right_value = ArenaCellValue(*arena, cells, compare.right,
                                                 qualified_schema);
              if (!DecideTypedFilterCompare(left_value, right_value, compare)) {
                keep = false;
                break;
              }
            }
            if (keep) {
              for (const TypedInListCompare& list : plan.inlist_compares) {
                // String lists cannot reach the arena: it only mirrors
                // tables whose every column is INT64/DATE.
                if (arena->IsNull(index, list.column)) {
                  keep = false;
                  break;
                }
                const int64_t cell = cells[list.column];
                bool matched = false;
                if (list.is_double) {
                  for (const double item : list.double_values) {
                    if (static_cast<double>(cell) == item) {
                      matched = true;
                      break;
                    }
                  }
                } else {
                  for (const int64_t item : list.int_values) {
                    if (cell == item) {
                      matched = true;
                      break;
                    }
                  }
                }
                if (!matched) {
                  keep = false;
                  break;
                }
              }
            }
            if (!keep) {
              continue;
            }
            ++emitted[worker];
            bool inserted = false;
            const int64_t raw_key = cells[plan.key_offsets[0]];
            size_t* slot = workers[worker].RawIntSlot(raw_key, &inserted);
            size_t group_index = 0;
            if (inserted) {
              scratch.clear();
              for (const slot_t offset : plan.key_offsets) {
                Value key_value;
                key_value.type = qualified_schema.GetColumn(offset).Type();
                key_value.value.int_value = cells[offset];
                scratch.push_back(CanonicalDistinctValue(key_value));
              }
              group_index = workers[worker].ClaimSlotForNewGroup(
                  slot, scratch, plan.aggregates.size(), aggregate_expressions);
            } else {
              group_index = *slot;
            }
            ++aggregate_updates[worker];
            std::array<Value, 8>& stack_values = cache_hit_values;
            std::array<const Value*, 8>& value_ptrs = cache_hit_value_ptrs;
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              const TypedAggregateInput& input = plan.aggregates[i];
              if (!input.column.has_value()) {
                value_ptrs[i] = nullptr;  // COUNT(*) sentinel.
                continue;
              }
              const slot_t column = *input.column;
              if (arena->IsNull(index, column)) {
                stack_values[i] = Value();
              } else {
                stack_values[i] =
                    ArenaCellValue(*arena, cells, column, qualified_schema);
              }
              value_ptrs[i] = &stack_values[i];
            }
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              if (!ApplySlimValue(&workers[worker], group_index, i,
                                  (*workers[worker].slim_kinds)[i],
                                  value_ptrs[i], &worker_errors[worker])) {
                return;
              }
            }
          }
          return;
        }
        // Rows own their cells in a separate heap block, so stream the Value
        // arrays into cache ahead of the consume point; the row headers
        // alone prefetch well in hardware.
        constexpr size_t kPrefetchDistance = 8;
        for (size_t index = begin; index < end; ++index) {
          if (index + kPrefetchDistance < end) {
            const Row& ahead = rows[index + kPrefetchDistance];
            __builtin_prefetch(&ahead, /*write=*/0, /*locality=*/1);
            __builtin_prefetch(ahead.values_.data(), /*write=*/0,
                               /*locality=*/1);
          }
          if (worker_errors[worker] != Status::kSuccess) {
            return;
          }
          ++scanned[worker];
          accumulate_row(rows[index], &workers[worker], &worker_errors[worker],
                         &emitted[worker], &aggregate_updates[worker], nullptr,
                         &scratch);
        }
        return;
      }
      if (cached_relation != nullptr) {
        // Spilled cache: worker 0 streams it serially.
        if (worker != 0) {
          return;
        }
        Status error = Status::kSuccess;
        size_t rows_output = 0;
        size_t updates = 0;
        cached_relation->FinishSpill();
        cached_relation->ForEachRow([&](const Row& row) {
          if (error != Status::kSuccess) {
            return;
          }
          ++scanned[worker];
          accumulate_row(row, &workers[worker], &error, &rows_output, &updates,
                         nullptr, &scratch);
        });
        worker_errors[worker] = error;
        emitted[worker] += rows_output;
        aggregate_updates[worker] += updates;
        return;
      }
      // Static rotation, same contract as the grouped-chunk fold above:
      // worker w scans morsels w, w+W, ... so the per-worker partial sums and
      // the worker-order merge are deterministic (rank boundaries in the
      // ROLLUP queries read these doubles).
      // Per-worker scratch arrays for the fold; every slot is written before
      // it is read within a row, so they carry no cross-row state.
      std::array<Value, 8> fold_row_values{};
      for (size_t morsel = worker; morsel < morsels.size();
           morsel += worker_count) {
        if (worker_errors[worker] != Status::kSuccess) {
          break;
        }
        Iterator iterator =
            table.BeginMorselScan(context.txn_, morsels[morsel], projection_opt,
                                  nullptr, std::nullopt, peek_ptr);
        if (typed_fill) {
          // Typed fill: decode schema-typed cell batches (no per-cell Value),
          // fold from raw cells, mirror every row into the morsel's arena run.
          // The published cache then holds the arena WITHOUT Rows; consumers
          // that cannot stream cells materialize rows from the arena on
          // demand (see the cache-hit path above).
          TypedCellBatch batch;
          MorselFillBuffer fill_buffer;
          fill_buffer.mirror.stride = fill_arena->stride;
          fill_buffer.mirror.slot_types = fill_arena->slot_types;
          fill_buffer.mirror.null_bits.resize(fill_arena->stride);
          fill_buffer.mirror.varchars.resize(fill_arena->varchars.size());
          for (;;) {
            const size_t decoded = iterator.DecodeNextTypedBatch(&batch, 2048);
            if (decoded == 0) {
              break;
            }
            scanned[worker] += decoded;
            AppendBatchToArena(batch, 0, decoded, &fill_buffer.mirror);
            for (size_t r = 0; r < decoded; ++r) {
              // Warm the int-key group slot a few rows ahead; the probe in
              // fold_cell_row below is otherwise a cold cache miss on the
              // first touch of each hot group's line.
              constexpr size_t kKeyPrefetchDistance = 4;
              if (int_key && r + kKeyPrefetchDistance < decoded &&
                  batch.nulls[((r + kKeyPrefetchDistance) * batch.stride) +
                              plan.key_offsets[0]] == 0) {
                workers[worker].PrefetchGroupSlot(
                    batch.cells[((r + kKeyPrefetchDistance) * batch.stride) +
                                plan.key_offsets[0]]);
              }
              fold_cell_row(batch, r, &workers[worker], &worker_errors[worker],
                            &emitted[worker], &aggregate_updates[worker],
                            &scratch, &fold_row_values);
              if (worker_errors[worker] != Status::kSuccess) {
                break;
              }
            }
            if (worker_errors[worker] != Status::kSuccess) {
              break;
            }
          }
          if (worker_errors[worker] == Status::kSuccess &&
              iterator.GetStatus() != Status::kSuccess) {
            worker_errors[worker] = iterator.GetStatus();
          }
          if (fill_cache) {
            publish_morsel(morsel, &fill_buffer);
          }
          if (worker_errors[worker] != Status::kSuccess) {
            break;
          }
          continue;
        }
        MorselFillBuffer fill_buffer;
        if (fill_cache && fill_arena != nullptr) {
          fill_buffer.arena_stride = fill_arena->stride;
        }
        MorselFillBuffer* fill_target = fill_cache ? &fill_buffer : nullptr;
        for (; iterator.IsValid(); ++iterator) {
          ++scanned[worker];
          accumulate_row(*iterator, &workers[worker], &worker_errors[worker],
                         &emitted[worker], &aggregate_updates[worker],
                         fill_target, &scratch);
          if (worker_errors[worker] != Status::kSuccess) {
            break;
          }
        }
        if (fill_cache) {
          publish_morsel(morsel, &fill_buffer);
        }
      }
    } catch (...) {
      if (!worker_exception) {
        worker_exception = std::current_exception();
      }
    }
  };
  std::vector<std::jthread> threads;
  for (size_t worker = 1; worker < worker_count; ++worker) {
    try {
      threads.emplace_back([&, worker] { run_worker(worker); });
    } catch (...) {
      break;  // Resource exhaustion: worker 0 drains the rest serially.
    }
  }
  run_worker(0);
  threads.clear();
  if (worker_exception != nullptr) {
    std::rethrow_exception(worker_exception);
  }
  for (const Status& error : worker_errors) {
    if (error != Status::kSuccess) {
      return error;
    }
  }

  // Publish the cache: the in-order morsel moves above already built the
  // relation while workers ran; once every worker joined, all morsels of a
  // successful run are guaranteed published.
  if (fill_cache) {
    cache_owner->FinishSpill();
    const std::string cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    runtime->base_relations.emplace(cache_key, cache_owner);
    if (fill_arena != nullptr && fill_arena->rows == published_rows) {
      fill_arena->published_rowsless = typed_fill;
      runtime->base_relation_arenas.emplace(cache_key, fill_arena);
    }
  }

  // Merge worker states in worker order (deterministic group creation order).
  TypedWorkerGroups merged;
  merged.int_key = int_key;
  merged.shapes = &aggregate_shapes;
  merged.Configure(!flat_fold_disabled, plan.key_offsets.size(),
                   plan.aggregates.size());
  merged.ConfigureSlim(plan);
  size_t total_scan_rows = 0;
  size_t total_scan_output = 0;
  for (size_t worker = 0; worker < worker_count; ++worker) {
    total_scan_rows += scanned[worker];
    total_scan_output += emitted[worker];
    TypedWorkerGroups& local = workers[worker];
    if (local.GroupCount() == 0) {
      continue;
    }
    if (merged.GroupCount() == 0) {
      merged.int_index = std::move(local.int_index);
      merged.row_index = std::move(local.row_index);
      merged.flat_group_keys = std::move(local.flat_group_keys);
      merged.chunked_states.AdoptChunksFrom(std::move(local.chunked_states));
      merged.group_keys = std::move(local.group_keys);
      merged.group_states = std::move(local.group_states);
      merged.slim_counts = std::move(local.slim_counts);
      merged.slim_int_totals = std::move(local.slim_int_totals);
      merged.slim_double_totals = std::move(local.slim_double_totals);
      merged.slim_flags = std::move(local.slim_flags);
      merged.slim_distinct = std::move(local.slim_distinct);
      merged.slim_distinct_n = std::move(local.slim_distinct_n);
      merged.slim_distinct_overflow = std::move(local.slim_distinct_overflow);
      merged.group_count = local.group_count;
      local.group_count = 0;
      continue;
    }
    std::vector<Value> canonical_keys;
    canonical_keys.reserve(plan.key_offsets.size());
    for (size_t group = 0; group < local.GroupCount(); ++group) {
      int64_t raw_key = 0;
      bool inserted = false;
      size_t target = 0;
      // Int-key merge fast path: probe the merged table with the raw key and
      // build canonical keys only for groups that actually open here.  Every
      // worker's hit (the overwhelmingly common case: the same orderkey
      // appears in several workers' partials) then costs one map probe with
      // no Value copies at all.
      const Value& local_key0 = local.GroupKey(group, 0);
      if (merged.int_key && !local_key0.IsNull()) {
        size_t* slot = merged.RawIntSlot(local_key0.value.int_value, &inserted);
        if (inserted) {
          canonical_keys.clear();
          for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
            canonical_keys.push_back(local.GroupKey(group, k));
          }
          target = merged.ClaimSlotForNewGroup(slot, canonical_keys,
                                               plan.aggregates.size(),
                                               aggregate_expressions);
        } else {
          target = *slot;
        }
      } else {
        canonical_keys.clear();
        for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
          canonical_keys.push_back(local.GroupKey(group, k));
        }
        if (merged.int_key) {
          raw_key = canonical_keys[0].value.int_value;
        }
        target =
            merged.FindOrInsert(canonical_keys, raw_key, plan.aggregates.size(),
                                aggregate_expressions, &inserted);
      }
      if (inserted) {
        // FindOrInsert created empty states; adopt this worker's partials.
        for (size_t i = 0; i < plan.aggregates.size(); ++i) {
          if (merged.slim) {
            merged.SlimCount(target, i) = local.SlimCount(group, i);
            merged.SlimIntTotal(target, i) = local.SlimIntTotal(group, i);
            merged.SlimDoubleTotal(target, i) = local.SlimDoubleTotal(group, i);
            merged.SlimFlags(target, i) = local.SlimFlags(group, i);
            if ((*merged.slim_kinds)[i] == SlimAggKind::kCountDistinctInt64) {
              const size_t dst_state = (target * merged.agg_stride) + i;
              const size_t src_state = (group * local.agg_stride) + i;
              merged.slim_distinct_n[dst_state] =
                  local.slim_distinct_n[src_state];
              std::copy_n(
                  &local.slim_distinct[src_state * kSlimDistinctSlots],
                  kSlimDistinctSlots,
                  &merged.slim_distinct[dst_state * kSlimDistinctSlots]);
              merged.slim_distinct_overflow[dst_state] =
                  std::move(local.slim_distinct_overflow[src_state]);
            }
            continue;
          }
          merged.State(target, i) = std::move(local.State(group, i));
        }
        continue;
      }
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        if (merged.slim) {
          // Slim merge: mirror AggregateAccumulator::Merge for the kinds the
          // slim layout admits (counts add; INT64 sums keep the overflow
          // raise; DOUBLE totals add and OR the saw-infinite-input flag;
          // COUNT(DISTINCT) unions sets).
          const SlimAggKind kind = (*merged.slim_kinds)[i];
          if (kind == SlimAggKind::kCountDistinctInt64) {
            if (local.SlimDistinctTotal(group, i) == 0) {
              continue;
            }
            const size_t src_state = (group * local.agg_stride) + i;
            const uint8_t n = local.slim_distinct_n[src_state];
            const int64_t* slots =
                &local.slim_distinct[src_state * kSlimDistinctSlots];
            for (size_t s = 0; s < n; ++s) {
              static_cast<void>(merged.SlimInsertDistinct(target, i, slots[s]));
            }
            if (const auto& overflow =
                    local.slim_distinct_overflow[src_state]) {
              for (const int64_t raw : *overflow) {
                static_cast<void>(merged.SlimInsertDistinct(target, i, raw));
              }
            }
            continue;
          }
          if (local.SlimCount(group, i) == 0) {
            continue;
          }
          switch (kind) {
            case SlimAggKind::kCount:
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            case SlimAggKind::kSumInt64: {
              int64_t updated = 0;
              if (__builtin_add_overflow(merged.SlimIntTotal(target, i),
                                         local.SlimIntTotal(group, i),
                                         &updated)) {
                return StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in SUM");
              }
              merged.SlimIntTotal(target, i) = updated;
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            }
            case SlimAggKind::kSumDouble:
              merged.SlimDoubleTotal(target, i) +=
                  local.SlimDoubleTotal(group, i);
              merged.SlimFlags(target, i) |= local.SlimFlags(group, i);
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            case SlimAggKind::kAvgDouble:
              merged.SlimDoubleTotal(target, i) +=
                  local.SlimDoubleTotal(group, i);
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            default:
              return StatusError(StatusCode::kInvalidArgument,
                                 "aggregate has no slim state");
          }
          continue;
        }
        if (local.State(group, i).count == 0) {
          continue;
        }
        if (Status st = merged.State(target, i).Merge(local.State(group, i));
            st != Status::kSuccess) {
          return st;
        }
      }
    }
  }
  if (runtime != nullptr) {
    runtime->scan_ms += ElapsedMs(scan_begin);
    runtime->filter_ms += ElapsedMs(scan_begin);
    runtime->scan_rows += total_scan_rows;
    runtime->scan_output_rows += total_scan_output;
    runtime->scan_values_decoded +=
        total_scan_rows * qualified_schema.ColumnCount();
    runtime->scan_values_available +=
        total_scan_rows * table_schema.ColumnCount();
    runtime->typed_parallel_aggregations += 1;
    runtime->aggregate_groups += merged.GroupCount();
    runtime->aggregate_input_rows += total_scan_output;
    for (const size_t worker_updates : aggregate_updates) {
      runtime->aggregate_updates += worker_updates;
    }
  }

  // Global aggregate over an empty input produces one group of NULL/0.
  if (merged.GroupCount() == 0 && plan.key_offsets.empty()) {
    std::vector<Value> canonical_keys;
    bool inserted = false;
    const size_t target =
        merged.FindOrInsert(canonical_keys, 0, plan.aggregates.size(),
                            aggregate_expressions, &inserted);
    (void)target;
    (void)inserted;
  }

  const auto aggregate_begin = std::chrono::steady_clock::now();
  // Minimal representative row for HAVING evaluation: NULLs everywhere
  // except the group-key offsets (HAVING eligibility guarantees it reads
  // nothing else from the base row).
  Row representative;
  std::vector<Column> output_columns;
  output_columns.reserve(stmt.SelectList().size());
  for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
    output_columns.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                ValueType::kNull);
  }
  Relation output(runtime);
  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
    size_t sequence{0};
  };
  std::vector<KeyedRow> sortable;
  const auto precedes = [&plan](const KeyedRow& left, const KeyedRow& right) {
    for (size_t i = 0; i < plan.order_items.size(); ++i) {
      const Value& a = left.keys[i];
      const Value& b = right.keys[i];
      const int c = CompareForOrderBy(a, b);
      if (c == 0) {
        continue;
      }
      const bool nulls_first = plan.order_nulls_first[i];
      const bool a_less = c < 0;
      if (a.IsNull()) {
        return nulls_first;
      }
      if (b.IsNull()) {
        return !nulls_first;
      }
      return plan.order_ascending[i] ? a_less : !a_less;
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
  // The group count is final before emit; reserving up front keeps the rows
  // vector from re-moving every emitted Row on geometric growth.
  output.rows.reserve(bounded ? std::min(topn_capacity, merged.GroupCount())
                              : merged.GroupCount());
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
  std::vector<Value> finished_values;
  // A/B hatch: TINYLAMB_DISABLE_HAVING_FAST=1 restores the per-group
  // AggregateResultMap path for every HAVING shape.
  static const bool having_fast_disabled =
      std::getenv("TINYLAMB_DISABLE_HAVING_FAST") != nullptr;
  std::optional<PositionalHaving> positional_having;
  if (having_expr != nullptr && !having_fast_disabled) {
    positional_having =
        TryCompilePositionalHaving(having_expr, plan.aggregates);
  }
  const size_t merged_group_count = merged.GroupCount();
  for (size_t group = 0; group < merged_group_count; ++group) {
    finished_values.clear();
    finished_values.reserve(plan.aggregates.size());
    if (merged.slim) {
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const int64_t count = merged.SlimCount(group, i);
        Value finished;
        switch ((*merged.slim_kinds)[i]) {
          case SlimAggKind::kCount:
            finished = Value(count);
            break;
          case SlimAggKind::kSumInt64:
            finished =
                count == 0 ? Value() : Value(merged.SlimIntTotal(group, i));
            break;
          case SlimAggKind::kSumDouble: {
            if (count == 0) {
              break;
            }
            const double sum_result = merged.SlimDoubleTotal(group, i);
            if ((merged.SlimFlags(group, i) & 1) == 0 &&
                std::isinf(sum_result)) {
              // Finite inputs whose sum leaves the double range overflow:
              // GoogleSQL raises instead of returning inf.
              return StatusError(StatusCode::kIsInfinity,
                                 "SUM overflow: result is infinite");
            }
            finished = Value(sum_result);
            break;
          }
          case SlimAggKind::kAvgDouble:
            finished = count == 0 ? Value()
                                  : Value(merged.SlimDoubleTotal(group, i) /
                                          static_cast<double>(count));
            break;
          case SlimAggKind::kCountDistinctInt64:
            finished =
                Value(static_cast<int64_t>(merged.SlimDistinctTotal(group, i)));
            break;
          default:
            return StatusError(StatusCode::kInvalidArgument,
                               "aggregate has no slim state");
        }
        finished_values.push_back(std::move(finished));
      }
    } else {
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        ASSIGN_OR_RETURN(Value, finished, merged.State(group, i).TryFinish());
        finished_values.push_back(std::move(finished));
      }
    }
    // Only HAVING reads the aggregate map; building an unordered_map per
    // group cost one allocation per group on multi-million-group folds.
    AggregateResultMap aggregate_results;
    if (having_expr && positional_having) {
      // `AGG op literal` / bare `AGG`: decide straight from the finished
      // value.  A NULL aggregate result compares to UNKNOWN, so the group
      // drops exactly as the generic path's TryEvaluate would decide.
      const Value& left_value = finished_values[positional_having->agg_index];
      Value having_value;
      if (positional_having->has_constant) {
        StatusOr<Value> compared = TryBinary(positional_having->op, left_value,
                                             positional_having->constant);
        if (!compared.HasValue()) {
          return compared.GetStatus();
        }
        having_value = compared.MoveValue();
      } else {
        having_value = left_value;
      }
      if (!Truthy(having_value)) {
        continue;
      }
    } else if (having_expr) {
      aggregate_results.reserve(plan.aggregates.size());
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        aggregate_results.emplace(plan.aggregates[i].aggregate,
                                  finished_values[i]);
      }
      if (representative.values_.size() != qualified_schema.ColumnCount()) {
        representative =
            Row(std::vector<Value>(qualified_schema.ColumnCount(), Value()));
      }
      for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
        representative.values_[plan.key_offsets[k]] = merged.GroupKey(group, k);
      }
      Scope scope{.row = &representative,
                  .schema = &qualified_schema,
                  .outer = nullptr};
      scope.aggregates = &aggregate_results;
      ASSIGN_OR_RETURN(
          Value, having_value,
          (TryEvaluate(having_expr, scope, &aggregate_results, context, ctes)));
      if (!Truthy(having_value)) {
        continue;
      }
    }
    std::vector<Value> values;
    values.reserve(plan.emit_items.size());
    for (const TypedEmitItem& item : plan.emit_items) {
      if (item.is_key) {
        values.push_back(merged.GroupKey(group, item.index));
      } else {
        values.push_back(finished_values[item.index]);
      }
    }
    Row output_row(std::move(values));
    if (output.rows.empty() && sortable.empty()) {
      for (size_t i = 0; i < output_columns.size(); ++i) {
        output_columns[i] =
            Column(output_columns[i].Name(), ValueTypeOf(output_row[i]));
        output_columns[i].SetUnsigned(output_row[i].IsUnsigned());
      }
    }
    if (!plan.order_items.empty()) {
      std::vector<Value> keys;
      keys.reserve(plan.order_items.size());
      for (const TypedEmitItem& item : plan.order_items) {
        if (item.is_key) {
          keys.push_back(merged.GroupKey(group, item.index));
        } else {
          keys.push_back(finished_values[item.index]);
        }
      }
      retain(KeyedRow{.keys = std::move(keys), .row = std::move(output_row)});
    } else {
      output.AddRow(std::move(output_row));
    }
  }
  if (bounded && runtime != nullptr) {
    ++runtime->topn_finish_uses;
    runtime->topn_finish_capacity = topn_capacity;
    runtime->topn_finish_input_rows = next_sequence;
  }
  if (!sortable.empty()) {
    if (bounded) {
      std::ranges::sort(sortable, precedes);
    } else {
      std::ranges::stable_sort(sortable, precedes);
    }
    for (KeyedRow& keyed : sortable) {
      output.AddRow(std::move(keyed.row));
    }
  }
  if (runtime != nullptr) {
    runtime->aggregate_ms += ElapsedMs(aggregate_begin);
  }
  output.schema = Schema("", std::move(output_columns));
  return output;
}

// Statement-level classification for fusing the final join with this
// statement's grouping (see GroupedJoinFusionSpec).  Schema-dependent checks
// (bytecode compilability against the joined schema) run at the join step in
// BuildInput; this pass only decides whether the statement shape qualifies.
GroupedJoinFusionSpec BuildGroupedJoinFusionSpec(
    const SelectStatement& statement) {
  GroupedJoinFusionSpec spec;
  if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr) {
    std::cerr << "[fusion] spec: sources=" << statement.Sources().size()
              << " group_by=" << statement.GroupBy().size()
              << " distinct=" << statement.Distinct()
              << " having=" << (statement.Having() != nullptr)
              << " order=" << statement.OrderBy().size() << "\n";
  }
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_GROUPED_JOIN_FUSION") != nullptr;
  static const bool debug_fusion =
      std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr;
  const auto bail = [&spec](const char* reason) {
    if (debug_fusion) {
      std::cerr << "[fusion] spec bail: " << reason << "\n";
    }
    return spec;
  };
  if (disabled || statement.Sources().size() < 2 || statement.Distinct() ||
      statement.Having() || HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement)) {
    return bail("shape head");
  }
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::ranges::any_of(statement.SelectList(), [](const NamedExpression& e) {
        return ContainsAggregate(e.expression);
      });
  if (!grouped) {
    return bail("not grouped");
  }
  // GROUP BY must not lean on select-list alias resolution: the fused emit
  // evaluates keys against the joined schema before any projection exists.
  for (const Expression& key : statement.GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue ||
        ContainsAggregate(key)) {
      continue;
    }
    const ColumnName& column = key->AsColumnValue().GetColumnName();
    if (!column.schema.empty()) {
      continue;
    }
    for (const NamedExpression& item : statement.SelectList()) {
      // A select item whose expression IS this same column does not shadow
      // it; only an alias for a different expression does.
      if (!item.name.empty() && IdentifierEquals(item.name, column.name) &&
          (!item.expression ||
           item.expression->Type() != TypeTag::kColumnValue ||
           !IdentifierEquals(
               item.expression->AsColumnValue().GetColumnName().name,
               column.name))) {
        return bail("alias group by");  // keep the materializing path
      }
    }
  }
  std::vector<const AggregateExpression*> aggregates;
  std::unordered_set<const AggregateExpression*> seen;
  for (const NamedExpression& item : statement.SelectList()) {
    CollectAggregates(item.expression, &aggregates, &seen);
  }
  for (const AggregateExpression* aggregate : aggregates) {
    // Only mergeable streaming kinds fuse: the parallel build merges
    // thread-local accumulator states.  COUNT(DISTINCT plain column) merges
    // as an exact set union; DISTINCT on other kinds stays serial.  The
    // child may be an arbitrary expression (Q9's SUM of arithmetic): the
    // join-step gate compiles it against the joined schema and falls back to
    // the materializing join when compilation fails.
    const bool distinct_count = aggregate->Distinct() &&
                                aggregate->GetType() == AggregationType::kCount;
    const bool supported_kind =
        aggregate->GetType() == AggregationType::kCount ||
        aggregate->GetType() == AggregationType::kSum ||
        aggregate->GetType() == AggregationType::kMin ||
        aggregate->GetType() == AggregationType::kMax ||
        aggregate->GetType() == AggregationType::kAvg;
    if (!supported_kind || aggregate->WhereFilter() ||
        (!IsCountStar(*aggregate) &&
         (aggregate->Having() != AggregateHavingModifier::kNone ||
          !aggregate->InnerOrderBy().empty() || aggregate->SecondaryArg() ||
          !aggregate->ExtraArgs().empty() || aggregate->NeedsGroupContext() ||
          !aggregate->TrailingArgs().empty() ||
          (aggregate->Distinct() && !distinct_count)))) {
      return bail("aggregate kind");
    }
  }
  // Every ORDER BY term must resolve against the projected output (an
  // ordinal or a select-item alias): the fused path orders after emission.
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (!term.expression) {
      return spec;
    }
    if (term.expression->Type() == TypeTag::kConstantValue) {
      const Value& value = term.expression->AsConstantValue().GetValue();
      if (value.type == ValueType::kInt64 && !value.IsNull() &&
          value.value.int_value >= 1) {
        continue;
      }
      return spec;
    }
    if (term.expression->Type() != TypeTag::kColumnValue ||
        !term.expression->AsColumnValue().GetColumnName().schema.empty()) {
      return spec;
    }
    const ColumnName& column = term.expression->AsColumnValue().GetColumnName();
    const bool matches_alias = std::ranges::any_of(
        statement.SelectList(), [&](const NamedExpression& item) {
          return !item.name.empty() && IdentifierEquals(item.name, column.name);
        });
    if (!matches_alias) {
      return spec;
    }
  }
  spec.group_keys.assign(statement.GroupBy().begin(),
                         statement.GroupBy().end());
  spec.aggregates = std::move(aggregates);
  spec.count_star.reserve(spec.aggregates.size());
  for (const AggregateExpression* aggregate : spec.aggregates) {
    spec.count_star.push_back(IsCountStar(*aggregate));
  }
  spec.select_items.reserve(statement.SelectList().size());
  spec.select_names.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    spec.select_items.push_back(item.expression);
    spec.select_names.push_back(item.name);
  }
  spec.eligible = true;
  return spec;
}

}  // namespace

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
      Row row;
      while (set_operation.Next(&row, nullptr)) {
        folded.AddRow(std::move(row));
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
      Row row;
      while (set_operation.Next(&row, nullptr)) {
        folded.AddRow(std::move(row));
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
    CompiledScanFilter scan_filter =
        CompileScanFilter(scan_predicates, scan_schema);

    Relation input;
    input.schema = qualified_schema;
    // Project aggregates against the qualified schema; feed rows one at a time
    // without retaining them in input.rows.
    const Expression having_expr = InlineHavingAliases(statement, input.schema);
    std::vector<const AggregateExpression*> aggregate_expressions;
    std::unordered_set<const AggregateExpression*> seen_aggregates;
    for (const NamedExpression& projection_item : stmt.SelectList()) {
      CollectAggregates(projection_item.expression, &aggregate_expressions,
                        &seen_aggregates);
    }
    CollectAggregates(having_expr, &aggregate_expressions, &seen_aggregates);
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
          PlanTypedAggregation(stmt, having_expr, qualified_schema,
                               aggregate_expressions, scan_predicates);
      if (typed_plan.eligible) {
        StatusOr<Relation> typed = RunTypedParallelStreamAggregate(
            context, stmt, having_expr, *table.Value(), source.table, reusable,
            projection, qualified_schema, table_schema, peek_compares,
            aggregate_expressions, ctes, std::move(typed_plan));
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
        if (!MatchScanFilter(row, scan_schema, scan_filter, outer, context,
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
        if (!MatchScanFilter(*iterator, scan_schema, scan_filter, outer,
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
      if (having_expr && !Truthy(Evaluate(having_expr, scope,
                                          &aggregate_results, context, ctes))) {
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

  bool where_fully_applied = false;
  // Fuse the final join with this statement's grouping when the shape
  // qualifies: the joined relation is never materialized (TPC-H Q13/Q18/Q21
  // outer legs).
  GroupedJoinFusionSpec fusion_spec = BuildGroupedJoinFusionSpec(statement);
  ASSIGN_OR_RETURN(
      Relation, input,
      BuildInput(context, statement, outer, ctes, &where_fully_applied,
                 fusion_spec.eligible ? &fusion_spec : nullptr));
  if (fusion_spec.fired) {
    // The fused probe emitted the grouped+projected rows already; only the
    // ORDER BY / LIMIT tail remains, and eligibility guarantees every order
    // term resolves against the projected output.
    input.FinishSpill();
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &input, outer, ctes));
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

  bool Next(Row* destination, RowPosition* position) override {
    if (!initialized_) {
      const Status st = Initialize();
      initialized_ = true;
      if (st != Status::kSuccess) {
        return FailWith(st);
      }
    }
    if (offset_ >= rows_.size()) {
      return false;
    }
    *destination = rows_[offset_];
    if (position != nullptr) {
      *position = RowPosition();
    }
    ++offset_;
    return true;
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
    Row row;
    while (core_executor_->Next(&row, nullptr)) {
      input.AddRow(row);
      ++core_input_rows_;
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
  correlated_index_probes_ = runtime.correlated_index_probes;
  correlated_result_cache_hits_ = runtime.correlated_result_cache_hits;
  shared_exists_build_reuses_ = runtime.shared_exists_build_reuses;
  correlated_distinct_keys_ = 0;
  for (const auto& [query, index] : runtime.correlated_indexes) {
    (void)query;
    if (index != nullptr) {
      correlated_distinct_keys_ += index->parameter_keys_seen.size();
    }
  }
  uncorrelated_cache_hits_ = runtime.uncorrelated_cache_hits;
  grouped_join_fusions_ = runtime.grouped_join_fusions;
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

bool RelationalExecutor::Next(Row* destination, RowPosition* position) {
  if (!initialized_) {
    const Status st = Initialize();
    if (st != Status::kSuccess) {
      return FailWith(st);
    }
  }
  if (offset_ >= rows_.size()) {
    return false;
  }
  *destination = rows_[offset_++];
  if (position != nullptr) {
    *position = RowPosition();
  }
  return true;
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
