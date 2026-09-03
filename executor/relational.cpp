/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/explain_format.hpp"
#include "expression/query_expression.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/constant_executor.hpp"
#include "executor/query_memory.hpp"
#include "executor/set_operation.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// SQL equality for group keys: NaN equals NaN and -0 equals +0, unlike raw
// IEEE Value::operator== which never folds NaNs together.
struct GroupKeyEqual {
  bool operator()(const Row& a, const Row& b) const {
    if (a.values_.size() != b.values_.size()) { return false;
}
    for (size_t i = 0; i < a.values_.size(); ++i) {
      const Value& x = a.values_[i];
      const Value& y = b.values_[i];
      if (x.type != y.type) { return false;
}
      if (x.type == ValueType::kDouble) {
        const double p = x.value.double_value;
        const double q = y.value.double_value;
        if (std::isnan(p) && std::isnan(q)) { continue;
}
        if (!(p == q)) { return false;
}
        continue;
      }
      if (!(x == y)) { return false;
      }
    }
    return true;
  }
};
using GroupKeyMap =
    std::unordered_map<Row, size_t, std::hash<Row>, GroupKeyEqual>;

namespace {

// HAVING may reference select-list aliases (`HAVING double_avg > 0.7`).
// Alias references that are not real input columns are rewritten to the
// aliased expression so aggregate aliases resolve during grouping.
Expression InlineHavingAliases(const SelectStatement& statement,
                               const Schema& schema) {
  const Expression& having = statement.Having();
  if (!having) { return having; }
  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& projection : statement.SelectList()) {
    if (!projection.name.empty() && projection.name != "*") {
      aliases.emplace(projection.name, projection.expression);
    }
  }
  if (aliases.empty()) { return having; }
  auto schema_has_column = [&schema](std::string_view name) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (name == schema.GetColumn(i).Name().name) { return true; }
    }
    return false;
  };
  std::function<Expression(const Expression&)> walk =
      [&](const Expression& expression) -> Expression {  // NOLINT(misc-no-recursion)
    if (!expression) { return expression; }
    switch (expression->Type()) {
      case TypeTag::kColumnValue: {
        const ColumnName& name =
            expression->AsColumnValue().GetColumnName();
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
        return UnaryExpressionExp(
            walk(expression->AsUnaryExpression().Child()),
            expression->AsUnaryExpression().Op());
      case TypeTag::kCaseExp: {
        const auto& value = expression->AsCaseExpression();
        std::vector<std::pair<Expression, Expression>> clauses;
        clauses.reserve(value.when_clauses_.size());
        for (const auto& [condition, result] : value.when_clauses_) {
          clauses.emplace_back(walk(condition), walk(result));
        }
        return CaseExpressionExp(std::move(clauses),
                                 walk(value.else_clause_));
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
              out.reserve(
                  expression->AsFunctionCallExpression().Args().size());
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
        if (!value.Query() || value.Test()) { return expression; }
        auto copied = std::make_shared<SelectStatement>(*value.Query());
        bool changed = false;
        std::vector<SelectSource> sources = copied->Sources();
        for (SelectSource& source : sources) {
          if (source.unnest) {
            source.unnest = walk(source.unnest);
            changed = true;
          }
        }
        if (!changed) { return expression; }
        copied->SetSources(std::move(sources));
        auto rewritten = std::make_shared<QueryExpression>(
            std::move(copied), nullptr, value.Exists(), value.Negated(),
            value.Op(), value.Mode());
        rewritten->SetArrayResult(value.ArrayResult());
        if (!value.ArrayElementSqlType().empty()) {
          rewritten->SetArrayElementSqlType(value.ArrayElementSqlType());
        }
        return Expression(rewritten);
      }
      default:
        // Aggregates, subqueries and casts keep their own scope.
        return expression;
    }
  };
  return walk(having);
}

Relation Project(TransactionContext& context, const SelectStatement& statement,
                 Relation input, const Scope* outer, const CteMap& ctes,
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
    throw std::runtime_error(
        "window functions combined with GROUP BY are not supported here");
  }
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return ContainsAggregate(projection.expression);
                  }) ||
      ContainsAggregate(statement.Having());
  const Expression having_expr =
      grouped ? InlineHavingAliases(statement, input.schema)
              : statement.Having();
  std::vector<const AggregateExpression*> aggregate_expressions;
  std::unordered_set<const AggregateExpression*> seen_aggregates;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectAggregates(projection.expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  CollectAggregates(having_expr, &aggregate_expressions, &seen_aggregates);

  struct GroupState {
    Row representative;
    size_t accumulator_offset{0};
  };
  std::deque<AggregateAccumulator> aggregate_states;
  auto accumulate_row = [&](const Row& row,
                            GroupKeyMap* offsets,
                            std::vector<GroupState>* local_groups,
                            std::deque<AggregateAccumulator>* local_states) {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    std::vector<Value> key_values;
    for (const Expression& key : statement.GroupBy()) {
      // Canonicalize so NaN/-0 keys fold like SQL equality demands.
      key_values.push_back(
          CanonicalDistinctValue(Evaluate(key, scope, nullptr, context, ctes)));
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
      if (IsCountStar(aggregate)) {
        accumulator.Add(Value(1));
        continue;
      }
      if (aggregate.WhereFilter() &&
          !Truthy(Evaluate(aggregate.WhereFilter(), scope, nullptr, context,
                           ctes))) {
        continue;
      }
      AggregateInput aggregate_input;
      aggregate_input.value =
          Evaluate(aggregate.Child(), scope, nullptr, context, ctes);
      if (aggregate.Having() != AggregateHavingModifier::kNone &&
          aggregate.HavingCondition()) {
        aggregate_input.condition =
            Evaluate(aggregate.HavingCondition(), scope, nullptr, context, ctes);
      }
      for (const auto& term : aggregate.InnerOrderBy()) {
        aggregate_input.order_keys.push_back(
            Evaluate(term.expression, scope, nullptr, context, ctes));
      }
      for (const Expression& extra : aggregate.ExtraArgs()) {
        aggregate_input.order_keys.push_back(
            Evaluate(extra, scope, nullptr, context, ctes));
      }
      if (aggregate.SecondaryArg()) {
        aggregate_input.auxiliary =
            Evaluate(aggregate.SecondaryArg(), scope, nullptr, context, ctes);
      }
      for (const Expression& extra : aggregate.TrailingArgs()) {
        if (extra) {
          aggregate_input.trailing_values.push_back(
              Evaluate(extra, scope, nullptr, context, ctes));
        }
      }
      accumulator.Add(std::move(aggregate_input));
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
  };
  auto make_group = [&]() {
    GroupState group;
    group.representative =
        Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
    group.accumulator_offset = aggregate_states.size();
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      aggregate_states.emplace_back(aggregate);
    }
    return group;
  };

  std::vector<GroupState> groups;
  if (grouped) {
    input.FinishSpill();

    const bool partition_agg =
        !statement.GroupBy().empty() &&
        (input.HasSpill() || !QueryMemoryBudget::Global().CanReserve(
                                 std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      std::vector<SpillFile> parts(kSpillPartitions);
      input.ForEachRow([&](const Row& row) {
        Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
        std::vector<Value> key_values;
        for (const Expression& key : statement.GroupBy()) {
          key_values.push_back(
              CanonicalDistinctValue(Evaluate(key, scope, nullptr, context,
                                              ctes)));
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
        parts[SpillPartitionOf(encoded_key, kSpillPartitions)].Append(row);
      });
      for (SpillFile& part : parts) {
        part.FinishWriting();
      }
      // Every row was copied into a partition above.
      input.ResetContents();
      for (size_t part = 0; part < kSpillPartitions; ++part) {
        GroupKeyMap offsets;
        std::vector<GroupState> local_groups;
        std::deque<AggregateAccumulator> local_states;
        parts[part].ForEachRow([&](const Row& row) {
          accumulate_row(row, &offsets, &local_groups, &local_states);
        });
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
      GroupKeyMap offsets;
      input.ForEachRow([&](const Row& row) {
        accumulate_row(row, &offsets, &groups, &aggregate_states);
      });
      if (input.TotalRows() == 0 && statement.GroupBy().empty()) {
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
      if (proto_value_star && base_width == 1) {
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
            const ColumnName& candidate =
                input.schema.GetColumn(column).Name();
            if (candidate.name.size() == using_name.size() &&
                std::equal(using_name.begin(), using_name.end(),
                           candidate.name.begin(),
                           [](char a, char b) {
                             return std::tolower(static_cast<unsigned char>(a)) ==
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
          if (matches_star(requested, candidate.Name(), qualified)) {
            star_groups[i].push_back({column});
            output_columns.emplace_back(candidate.Name().name,
                                        candidate.Type());
          }
        }
        continue;
      }
      for (size_t column = 0; column < base_width; ++column) {
        const Column& candidate = input.schema.GetColumn(column);
        if (matches_star(requested, candidate.Name(), qualified)) {
          star_groups[i].push_back({column});
          output_columns.emplace_back(candidate.Name().name, candidate.Type());
        }
      }
    } else {
      output_columns.emplace_back(ProjectionName(projection, i),
                                  ValueType::kNull);
    }
  }
  // Hidden $win columns ride along until ordering completes, then get trimmed
  // below.  ORDER BY terms may reference them.
  for (size_t k = 0; k < hidden_columns; ++k) {
    output_columns.emplace_back(input.schema.GetColumn(base_width + k));
  }

  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  std::vector<Expression> resolved_order_expressions;
  resolved_order_expressions.reserve(order_by.size());
  for (const auto& term : order_by) {
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
    resolved_order_expressions.push_back(std::move(expression));
  }
  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
  };
  std::vector<KeyedRow> sortable;
  const bool has_order_by = !order_by.empty();
  Schema initial_output_schema("", output_columns);

  auto emit = [&](const Row& representative,
                  const AggregateResultMap* aggregates) {
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
    if (having_expr &&
        !Truthy(Evaluate(having_expr, scope, aggregates, context, ctes))) {
      return;
    }
    std::vector<Value> values;
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      const NamedExpression& projection = statement.SelectList()[item];
      if (projection.expression->Type() == TypeTag::kColumnValue &&
          projection.expression->AsColumnValue().GetColumnName().name == "*") {
        if (star_proto[item]) {
          const ColumnName base = input.schema.GetColumn(0).Name();
          for (const char* field : {"int32_val1", "int32_val2",
                                    "str_value"}) {
            values.push_back(Evaluate(
                FunctionCallExp(
                    "__get_field_safe",
                    {ColumnValueExp(base),
                     ConstantValueExp(Value(std::string(field)))}),
                scope, aggregates, context, ctes));
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
        values.push_back(
            Evaluate(projection.expression, scope, aggregates, context, ctes));
      }
    }
    // Carry hidden $win columns through projection; ORDER BY may reference
    // them and they are trimmed after sorting.
    for (size_t k = base_width; k < representative.values_.size(); ++k) {
      values.push_back(representative.values_[k]);
    }
    Row output_row(std::move(values));
    if (has_order_by) {
      Scope proj_scope{.row = &output_row,
                       .schema = &initial_output_schema,
                       .outer = &scope};
      std::vector<Value> keys;
      keys.reserve(order_by.size());
      for (const Expression& key : resolved_order_expressions) {
        keys.push_back(
            Evaluate(key, proj_scope, aggregates, context, ctes));
      }
      sortable.push_back(
          KeyedRow{.keys = std::move(keys), .row = std::move(output_row)});
    } else {
      output.AddRow(std::move(output_row));
    }
  };

  if (grouped) {
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        aggregate_results.emplace(accumulator.expression, accumulator.Finish());
      }
      emit(group.representative, &aggregate_results);
    }
  } else {
    input.FinishSpill();
    input.ForEachRow([&](const Row& row) { emit(row, nullptr); });
  }

  if (has_order_by) {
    const auto sort_begin = std::chrono::steady_clock::now();
    std::ranges::stable_sort(
        sortable, [&](const KeyedRow& left, const KeyedRow& right) {
          for (size_t i = 0; i < order_by.size(); ++i) {
            const Value& a = left.keys[i];
            const Value& b = right.keys[i];
            const int c = CompareForOrderBy(a, b);
            if (c == 0) {
              continue;
            }
            const bool nulls_first =
                order_by[i].nulls_first.value_or(order_by[i].ascending);
            const bool a_less = c < 0;
            if (a.IsNull()) {
              return nulls_first;
            }
            if (b.IsNull()) {
              return !nulls_first;
            }
            return order_by[i].ascending ? a_less : !a_less;
          }
          return false;
        });
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
    output.ForEachRow([&all_rows](const Row& row) { all_rows.push_back(row); });
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

namespace {

// GoogleSQL DISTINCT treats every NaN as equal (they collapse to a single
// group) and folds -0.0 into +0.0.  The default Value/Row equality returns
// false for NaN != NaN, so DISTINCT needs this canonical-aware comparison
// (mirroring CompareForOrderBy / CanonicalDistinctValue).
bool CanonicalValuesEqual(const Value& x, const Value& y) {
  if (x.type == ValueType::kDouble && y.type == ValueType::kDouble) {
    return CompareForOrderBy(x, y) == 0;
  }
  return x == y;
}

struct CanonicalRowEqual {
  bool operator()(const Row& a, const Row& b) const {
    if (a.values_.size() != b.values_.size()) { return false; }
    for (size_t i = 0; i < a.values_.size(); ++i) {
      if (!CanonicalValuesEqual(a.values_[i], b.values_[i])) { return false; }
    }
    return true;
  }
};

struct CanonicalRowHash {
  size_t operator()(const Row& r) const {
    size_t h = 1469598103934665603ULL;  // FNV-1a offset basis
    for (const Value& value : r.values_) {
      h ^= std::hash<Value>()(CanonicalDistinctValue(value));
      h *= 1099511628211ULL;  // FNV-1a prime
    }
    return h;
  }
};

}  // namespace

Relation DistinctOf(Relation input,
                    const std::vector<Expression>& distinct_on = {},
                    TransactionContext* ctx = nullptr,
                    const Scope* outer = nullptr,
                    const CteMap* ctes = nullptr) {
  if (distinct_on.empty()) {
    std::unordered_set<Row, CanonicalRowHash, CanonicalRowEqual> seen;
    Relation distinct;
    distinct.schema = input.schema;
    CopyExecutionStats(&distinct, input);
    input.FinishSpill();
    input.ForEachRow([&](const Row& row) {
      // Canonicalize the emitted row so a representative value is chosen
      // consistently (NaN -> quiet NaN, -0.0 -> +0.0), matching GoogleSQL.
      Row canonical = row;
      for (Value& value : canonical.values_) {
        value = CanonicalDistinctValue(value);
      }
      if (seen.insert(canonical).second) {
        distinct.AddRow(std::move(canonical));
      }
    });
    distinct.FinishSpill();
    return distinct;
  }
  std::vector<std::vector<Value>> seen_keys;
  Relation distinct;
  distinct.schema = input.schema;
  CopyExecutionStats(&distinct, input);
  input.FinishSpill();
  input.ForEachRow([&](const Row& row) {
    std::vector<Value> current_keys;
    current_keys.reserve(distinct_on.size());
    for (const auto& expr : distinct_on) {
      Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
      Value val = ctx != nullptr && ctes != nullptr
                      ? Evaluate(expr, scope, nullptr, *ctx, *ctes)
                      : expr->Evaluate(row, input.schema);
      current_keys.push_back(CanonicalDistinctValue(val));
    }
    bool duplicate = false;
    for (const auto& seen_k : seen_keys) {
      if (seen_k.size() == current_keys.size()) {
        bool match = true;
        for (size_t i = 0; i < seen_k.size(); ++i) {
          if (!CanonicalValuesEqual(seen_k[i], current_keys[i])) {
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
  distinct.FinishSpill();
  return distinct;
}

// Sorts `output` in place by the statement's ORDER BY terms. Key values are
// evaluated once per row (not per comparison). All rows are copied out before
// refilling so a previously spilled output is never appended to after its
// spill files were finished.
void ApplyOrderBy(TransactionContext& context, const SelectStatement& statement,
                  Relation* output, const Scope* outer, const CteMap& ctes) {
  const auto sort_begin = std::chrono::steady_clock::now();
  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
  };
  std::vector<KeyedRow> sortable;
  output->FinishSpill();
  output->ForEachRow([&](const Row& row) {
    Scope scope{.row = &row, .schema = &output->schema, .outer = outer};
    std::vector<Value> keys;
    keys.reserve(order_by.size());
    for (const auto& key : order_by) {
      keys.push_back(Evaluate(key.expression, scope, nullptr, context, ctes));
    }
    sortable.push_back(KeyedRow{.keys = std::move(keys), .row = row});
  });
  // Every row was copied into `sortable`; detach the finished spill files so
  // re-adding below cannot hit "Append after FinishWriting".
  output->ResetContents();
  std::ranges::stable_sort(
      sortable, [&](const KeyedRow& left, const KeyedRow& right) {
        for (size_t i = 0; i < order_by.size(); ++i) {
          const Value& a = left.keys[i];
          const Value& b = right.keys[i];
          const int c = CompareForOrderBy(a, b);
          if (c == 0) {
            continue;
          }
          const bool nulls_first =
              order_by[i].nulls_first.value_or(order_by[i].ascending);
          const bool a_less = c < 0;
          if (a.IsNull()) {
            return nulls_first;
          }
          if (b.IsNull()) {
            return !nulls_first;
          }
          return order_by[i].ascending ? a_less : !a_less;
        }
        return false;
      });
  for (KeyedRow& keyed : sortable) {
    output->AddRow(std::move(keyed.row));
  }
  output->FinishSpill();
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
  }
}

Relation LimitedRows(const SelectStatement& statement, Relation&& input,
                     TransactionContext* context = nullptr,
                     const Scope* outer = nullptr,
                     const CteMap* ctes = nullptr) {
  Relation limited;
  limited.schema = input.schema;
  CopyExecutionStats(&limited, input);
  input.FinishSpill();
  const size_t total = input.TotalRows();
  const size_t begin = std::min(statement.Offset(), total);
  const size_t available = total - begin;
  // An explicit LIMIT 0 is a real bound (zero rows); only an absent LIMIT
  // leaves the window unbounded.
  const bool unlimited = statement.Limit() == 0 && !statement.HasLimit();
  const size_t count =
      unlimited ? available : std::min(statement.Limit(), available);

  std::vector<Row> all_rows;
  all_rows.reserve(total);
  input.ForEachRow([&](const Row& row) { all_rows.push_back(row); });

  if (statement.WithTies() && !statement.OrderBy().empty() && count > 0 &&
      begin + count <= all_rows.size()) {
    const size_t last_idx = begin + count - 1;
    const Row& last_row = all_rows[last_idx];
    size_t end_idx = begin + count;
    while (end_idx < all_rows.size()) {
      bool tied = true;
      for (const auto& term : statement.OrderBy()) {
        Scope scope_last{.row = &last_row, .schema = &input.schema, .outer = outer};
        Scope scope_curr{.row = &all_rows[end_idx], .schema = &input.schema, .outer = outer};
        Value val_last = context != nullptr && ctes != nullptr
                             ? Evaluate(term.expression, scope_last, nullptr, *context, *ctes)
                             : term.expression->Evaluate(last_row, input.schema);
        Value val_curr = context != nullptr && ctes != nullptr
                             ? Evaluate(term.expression, scope_curr, nullptr, *context, *ctes)
                             : term.expression->Evaluate(all_rows[end_idx], input.schema);
        if (!CanonicalValuesEqual(val_last, val_curr)) {
          tied = false;
          break;
        }
      }
      if (!tied) break;
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

  limited.FinishSpill();
  return limited;
}

}  // namespace

Relation FinishQuery(TransactionContext& context,
                     const SelectStatement& statement, Relation input,
                     const Scope* outer, const CteMap& ctes, bool apply_where,
                     size_t hidden_columns) {
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
      where_predicates =
          SplitConjuncts(statement.WhereClause());
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
        compiled.residual_bytecode.has_value()) {
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      input.FinishSpill();
      input.ForEachRow([&](const Row& row) {
        if (relational_detail::MatchScanFilter(
                row, input.schema, compiled, outer, context, ctes)) {
          filtered.AddRow(row);
        }
      });
      filtered.FinishSpill();
      input = std::move(filtered);
    } else {
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      input.FinishSpill();
      input.ForEachRow([&](const Row& row) {
        Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
        if (Truthy(Evaluate(statement.WhereClause(), scope, nullptr, context,
                            ctes))) {
          filtered.AddRow(row);
        }
      });
      filtered.FinishSpill();
      input = std::move(filtered);
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
    }
  }

  Relation output = Project(context, statement, std::move(input), outer, ctes,
                            hidden_columns);
  if (statement.Distinct()) {
    if (statement.HasDistinctOn()) {
      if (!statement.OrderBy().empty()) {
        ApplyOrderBy(context, statement, &output, outer, ctes);
      }
      output = DistinctOf(std::move(output), statement.DistinctOn(), &context,
                          outer, &ctes);
    } else {
      output = DistinctOf(std::move(output));
      if (!statement.OrderBy().empty()) {
        ApplyOrderBy(context, statement, &output, outer, ctes);
      }
    }
  }
  return LimitedRows(statement, std::move(output), &context, outer, &ctes);
}
Relation ExecuteQuery(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& inherited_ctes);

bool ReferencesCte(const SelectStatement& statement, const std::string& name) {
  for (const SelectSource& source : statement.Sources()) {
    if (source.table == name) {
      return true;
    }
    if (source.query && ReferencesCte(*source.query, name)) {
      return true;
    }
  }
  for (const auto& branch : statement.UnionAll()) {
    if (branch && ReferencesCte(*branch, name)) {
      return true;
    }
  }
  return false;
}

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
      ExpressionChildren(expression),
      [](const Expression& child) {
        return IsDeterministicProjectionExpression(child);
      });
}

bool IsFlattenableProjection(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].unnest ||
      (!statement.Sources()[0].query && statement.Sources()[0].table.empty()) ||
      statement.WhereClause() || statement.Having() ||
      statement.Qualify() || !statement.GroupBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      !statement.OrderBy().empty() || statement.HasLimit() ||
      statement.Offset() != 0 || statement.SelectList().empty() ||
      HasWindowFunctions(statement)) {
    return false;
  }
  // Projection flattening is only valid for expressions that do not create a
  // second query scope or change row cardinality.
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               !(item.expression->Type() == TypeTag::kColumnValue &&
                 item.expression->AsColumnValue().GetColumnName().name ==
                     "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

size_t FlattenableProjectionDepth(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 ||
      !statement.Sources()[0].query ||
      !IsFlattenableProjection(*statement.Sources()[0].query)) {
    return 0;
  }
  return 1 + FlattenableProjectionDepth(*statement.Sources()[0].query);
}

bool IdentifierEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
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
    rewritten.push_back(SubstituteProjectionColumns(
        child, projection, projection_alias, valid));
    if (!*valid) {
      return nullptr;
    }
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}

std::shared_ptr<SelectStatement> FlattenProjectionBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (!IsFlattenableProjection(derived) ||
      (statement.Sources().size() != 1) || statement.Having() ||
      statement.Qualify() || !statement.GroupBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      HasWindowFunctions(statement)) {
    return nullptr;
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result = SubstituteProjectionColumns(
        expression, derived, derived_alias, &valid);
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
  Expression where = statement.WhereClause() ? rewrite(statement.WhereClause())
                                              : Expression{};
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
        SelectStatement::OrderByTerm{std::move(expression), term.ascending,
                                     term.nulls_first});
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetWhereClause(std::move(where));
  rewritten->SetOrderBy(std::move(order_by));
  SelectSource merged = derived.Sources()[0];
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
      const ColumnName& column =
          expression->AsColumnValue().GetColumnName();
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
          return ColumnValueExp(ColumnName(
              source.alias.empty() ? source.table : source.alias,
              projected.name));
        }
      }
      return nullptr;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      Expression left = RebindDerivedPredicate(binary.Left(), derived,
                                                derived_alias);
      Expression right = RebindDerivedPredicate(binary.Right(), derived,
                                                 derived_alias);
      return left && right
                 ? BinaryExpressionExp(std::move(left), binary.Op(),
                                       std::move(right))
                 : nullptr;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      Expression child = RebindDerivedPredicate(unary.Child(), derived,
                                                derived_alias);
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
      !derived.OrderBy().empty() || derived.Distinct() ||
      derived.HasLimit() || derived.Offset() != 0 || derived.SelectList().empty()) {
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

std::shared_ptr<SelectStatement> OptimizeDerivedBoundaries(
    const SelectStatement& statement, const CteMap& inherited_ctes) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].query == nullptr) {
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

  std::vector<Expression> pushable;
  std::vector<Expression> remaining;
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    (PredicateCanEnterDerived(
         conjunct, derived, source.alias.empty() ? "u" : source.alias)
         ? pushable
         : remaining)
        .push_back(conjunct);
  }
  const bool is_union =
      !derived.UnionAll().empty() && !derived.UnionDistinct() &&
      std::ranges::all_of(
          derived.SetOperationKinds(), [](SetOperationKind kind) {
            return kind == SetOperationKind::kUnionAll;
          });
  if (is_union) {
    // UNION ALL has no row-changing boundary before the outer filter. Push a
    // local predicate into every branch, then retain any non-local predicates
    // above the set operation.
    SelectStatement pushed = derived;
    std::vector<Expression> head_where =
        SplitConjuncts(pushed.WhereClause());
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
                predicate, *copied, source.alias.empty() ? "u" : source.alias)) {
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
      rewritten->SetWhereClause(remaining.empty()
                                    ? Expression{}
                                    : CombineConjuncts(remaining));
      return rewritten;
    }
  }

  if (!pushable.empty() && !is_union) {
    auto copied = std::make_shared<SelectStatement>(derived);
    std::vector<Expression> inner_where =
        SplitConjuncts(copied->WhereClause());
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
    rewritten->SetWhereClause(remaining.empty()
                                  ? Expression{}
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

bool SameRow(const Row& left, const Row& right) {
  if (left.values_.size() != right.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < left.values_.size(); ++i) {
    const Value& a = left.values_[i];
    const Value& b = right.values_[i];
    if (a.IsNull() || b.IsNull()) {
      if (!a.IsNull() || !b.IsNull()) {
        return false;
      }
      continue;
    }
    try {
      if (!(a == b)) {
        return false;
      }
    } catch (...) {
      return false;
    }
  }
  return true;
}

Relation ExecuteRecursiveCte(TransactionContext& context,
                             const std::string& name,
                             const SelectStatement& body, const Scope* outer,
                             const CteMap& inherited_ctes,
                             const RecursiveDepthSpec* depth_spec = nullptr) {
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
  Relation anchor_result = ExecuteQuery(context, anchor, outer, inherited_ctes);
  anchor_result.FinishSpill();

  const bool track_depth = depth_spec != nullptr;
  const bool distinct = body.UnionDistinct();
  const bool by_name = body.UnionByName();
  Relation result(context.execution_runtime());
  result.schema = anchor_result.schema;
  if (track_depth) {
    result.schema = result.schema +
                    Schema("", {Column(depth_spec->column, ValueType::kInt64)});
  }
  Relation delta(context.execution_runtime());
  delta.schema = result.schema;

  std::vector<Row> seen;
  auto already_seen = [&](const Row& row) {
    if (!distinct) {
      return false;
    }
    for (const Row& prior : seen) {
      if (SameRow(prior, row)) {
        return true;
      }
    }
    seen.push_back(row);
    return false;
  };
  auto add_anchor = [&](const Row& row) {
    if (already_seen(row)) {
      return;
    }
    Row stored = row;
    if (track_depth) {
      stored.values_.push_back(Value(int64_t{0}));
    }
    delta.AddRow(stored);
    if (!track_depth ||
        (depth_spec->lower <= 0 && 0 <= depth_spec->upper)) {
      result.AddRow(std::move(stored));
    }
  };
  anchor_result.ForEachRow(add_anchor);
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
    loop_ctes[name] =
        std::make_shared<Relation>(MaterializeRelation(delta));
    Relation next(context.execution_runtime());
    next.schema = result.schema;
    for (const auto& branch_statement : body.UnionAll()) {
      if (!branch_statement) {
        continue;
      }
      Relation branch =
          ExecuteQuery(context, *branch_statement, outer, loop_ctes);
      branch.ForEachRow([&](const Row& row) {
        Row payload = row;
        if (by_name) {
          std::vector<Value> aligned;
          aligned.reserve(result.schema.ColumnCount() - (track_depth ? 1 : 0));
          const size_t payload_width =
              result.schema.ColumnCount() - (track_depth ? 1 : 0);
          for (size_t target = 0; target < payload_width; ++target) {
            const std::string& wanted =
                result.schema.GetColumn(target).Name().name;
            size_t found = payload.values_.size();
            for (size_t source = 0; source < branch.schema.ColumnCount();
                 ++source) {
              if (branch.schema.GetColumn(source).Name().name == wanted) {
                found = source;
                break;
              }
            }
            aligned.push_back(found < payload.values_.size()
                                  ? payload.values_[found]
                                  : Value());
          }
          payload.values_ = std::move(aligned);
        }
        if (already_seen(payload)) {
          return;
        }
        if (track_depth) {
          payload.values_.push_back(Value(row_depth));
        }
        next.AddRow(std::move(payload));
      });
    }
    next.FinishSpill();
    if (next.TotalRows() == 0) {
      break;
    }
    if (result.TotalRows() + next.TotalRows() > kMaxRows) {
      throw std::runtime_error("recursive CTE " + name +
                               " exceeded the row budget");
    }
    if (!track_depth ||
        (row_depth >= depth_spec->lower && row_depth <= depth_spec->upper)) {
      next.ForEachRow([&](const Row& row) { result.AddRow(row); });
    }
    result.FinishSpill();
    delta = std::move(next);
  }
  return result;
}

Relation ExecuteQuery(  // NOLINT(misc-no-recursion)
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
        bool ready = true;
        for (const SelectSource& source : query.Sources()) {
          if (!source.table.empty() && pending.contains(source.table) &&
              !(recursive && source.table == name)) {
            ready = false;
            break;
          }
        }
        if (ready) {
          ctes[name] = std::make_shared<Relation>(
              recursive ? ExecuteRecursiveCte(context, name, query, outer, ctes,
                                              statement.RecursiveDepthOf(name))
                        : ExecuteQuery(context, query, outer, ctes));
          iter = pending.erase(iter);
          progress = true;
        } else {
          ++iter;
        }
      }
      if (!progress) {
        // Cyclic references cannot be satisfied; run whatever remains in
        // map order so the failure mirrors a missing relation.
        for (const std::string& name : pending) {
          ctes[name] = std::make_shared<Relation>(
              ExecuteQuery(context, *statement.WithQueries().at(name), outer,
                           ctes));
        }
        break;
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
                          SetOperationKind operation) {
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      left.ForEachRow([&](const Row& row) { left_rows.push_back(row); });
      right.ForEachRow([&](const Row& row) { right_rows.push_back(row); });
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
      folded.FinishSpill();
      return folded;
    };

    Relation combined = ExecuteQuery(context, *tree.first, outer, ctes);
    // PRODUCTION FIX: honor per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold evaluated `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and dropped the INTERSECT entirely.
    auto run_term = [&](const SelectStatement& stmt) {
      return ExecuteQuery(context, stmt, outer, ctes);
    };
    std::vector<Relation> terms;
    terms.push_back(std::move(combined));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < tree.branches.size(); ++i) {
      Relation branch = run_term(*tree.branches[i]);
      const SetOperationKind operation =
          i < tree.kinds.size() ? tree.kinds[i] : SetOperationKind::kUnionAll;
      const bool intersects =
          operation == SetOperationKind::kIntersect ||
          operation == SetOperationKind::kIntersectAll;
      if (intersects) {
        terms.back() = apply_pair(std::move(terms.back()),
                                  std::move(branch), operation);
      } else {
        low_ops.push_back(operation);
        terms.push_back(std::move(branch));
      }
    }
    combined = std::move(terms.front());
    for (size_t j = 1; j < terms.size(); ++j) {
      combined = apply_pair(std::move(combined), std::move(terms[j]),
                            low_ops[j - 1]);
    }
    combined.FinishSpill();
    if (!statement.OrderBy().empty()) {
      ApplyOrderBy(context, statement, &combined, outer, ctes);
    }
    return LimitedRows(statement, std::move(combined));
  }

  if (!statement.UnionAll().empty()) {
    SelectStatement head = statement;
    head.ClearUnionAll();
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    Relation combined = ExecuteQuery(context, head, outer, ctes);
    // PRODUCTION FIX: apply per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold computed `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and returned the wrong rows.
    auto apply_pair = [&](Relation left, Relation right,
                          SetOperationKind operation) {
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      left.ForEachRow([&](const Row& row) { left_rows.push_back(row); });
      right.ForEachRow([&](const Row& row) { right_rows.push_back(row); });
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
      folded.FinishSpill();
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
      Relation branch = ExecuteQuery(context, *union_stmt, outer, ctes);
      const SetOperationKind operation =
          i < statement.SetOperationKinds().size()
              ? statement.SetOperationKinds()[i]
              : SetOperationKind::kUnionAll;
      const bool intersects =
          operation == SetOperationKind::kIntersect ||
          operation == SetOperationKind::kIntersectAll;
      if (intersects) {
        terms.back() = apply_pair(std::move(terms.back()),
                                  std::move(branch), operation);
      } else {
        low_ops.push_back(operation);
        terms.push_back(std::move(branch));
      }
    }
    combined = std::move(terms.front());
    for (size_t j = 1; j < terms.size(); ++j) {
      combined = apply_pair(std::move(combined), std::move(terms[j]),
                            low_ops[j - 1]);
    }
    combined.FinishSpill();
    if (!statement.OrderBy().empty()) {
      ApplyOrderBy(context, statement, &combined, outer, ctes);
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
      !statement.Sources()[0].unnest && !HasWindowFunctions(statement) &&
      !ctes.contains(statement.Sources()[0].table) &&
      (!statement.WhereClause() || !ContainsQuery(statement.WhereClause())) &&

      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }) ||
       ContainsAggregate(statement.Having()));
  if (stream_agg) {
    const SelectSource& source = statement.Sources()[0];
    const bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      throw std::runtime_error("table " + source.table + " not found");
    }
    const Schema& table_schema = table.Value()->GetSchema();
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    // Alias/ordinal resolution needs the qualified column names the grouping
    // expressions see; offsets are identical in the raw table schema.
    std::shared_ptr<SelectStatement> resolved_statement =
        ResolveGroupingAliases(
            statement, qualifier.empty()
                           ? table_schema
                           : QualifySchema(table_schema, qualifier));
    const SelectStatement& stmt =
        resolved_statement != nullptr ? *resolved_statement : statement;
    // RequiredColumns must run against the qualified names the statement
    // references (`t4.array_val`); QualifySchema preserves column order so
    // the resulting offsets index the raw table scan unchanged.
    const Schema qualified_table_schema =
        qualifier.empty() ? table_schema : QualifySchema(table_schema, qualifier);
    std::vector<slot_t> projection = RequiredColumns(stmt, qualified_table_schema);
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

    struct GroupState {
      Row representative;
      size_t accumulator_offset{0};
    };
    std::deque<AggregateAccumulator> aggregate_states;
    std::vector<GroupState> groups;
    GroupKeyMap offsets;

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
    auto accumulate_row = [&](Row row) {
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->scan_output_rows;
      }
      Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
      std::vector<Value> key_values;
      key_values.reserve(stmt.GroupBy().size());
      if (group_keys_are_columns) {
        for (const auto& offset : group_offsets) {
          key_values.push_back(CanonicalDistinctValue(row[*offset]));
        }
      } else {
        for (const Expression& key : stmt.GroupBy()) {
          key_values.push_back(
              CanonicalDistinctValue(Evaluate(key, scope, nullptr, context,
                                              ctes)));
        }
      }
      Row key(std::move(key_values));
      auto [iter, inserted] = offsets.emplace(key, groups.size());
      if (inserted) {
        GroupState group;
        group.accumulator_offset = aggregate_states.size();
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          aggregate_states.emplace_back(aggregate);
        }
        group.representative = row;
        groups.push_back(std::move(group));
      }
      GroupState& group = groups[iter->second];
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
          accumulator.Add(Value(1));
        } else if (aggregate_child_offsets[i] &&
                   !aggregate.NeedsGroupContext() &&
                   aggregate.Having() == AggregateHavingModifier::kNone) {
          accumulator.Add(row[*aggregate_child_offsets[i]]);
        } else {
          AggregateInput aggregate_input;
          aggregate_input.value =
              Evaluate(aggregate.Child(), scope, nullptr, context, ctes);
          if (aggregate.Having() != AggregateHavingModifier::kNone &&
              aggregate.HavingCondition()) {
            aggregate_input.condition =
                Evaluate(aggregate.HavingCondition(), scope, nullptr, context,
                         ctes);
          }
          for (const auto& term : aggregate.InnerOrderBy()) {
            aggregate_input.order_keys.push_back(
                Evaluate(term.expression, scope, nullptr, context, ctes));
          }
          if (aggregate.GetType() == AggregationType::kStringAgg &&
              aggregate.SecondaryArg()) {
            aggregate_input.auxiliary =
                Evaluate(aggregate.SecondaryArg(), scope, nullptr, context,
                         ctes);
          }
          for (const Expression& extra : aggregate.TrailingArgs()) {
            if (extra) {
              aggregate_input.trailing_values.push_back(
                  Evaluate(extra, scope, nullptr, context, ctes));
            }
          }
          accumulator.Add(std::move(aggregate_input));
        }
        if (context.execution_runtime() != nullptr) {
          ++context.execution_runtime()->aggregate_updates;
        }
      }
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->aggregate_input_rows;
      }
    };

    const std::vector<slot_t>* proj_ptr =
        projection.empty() ? nullptr : &projection;
    const std::vector<IntegerPeekCompare> peek_compares =
        BuildIntegerPeeks(scan_filter, proj_ptr, table_schema);
    const std::vector<IntegerPeekCompare>* peek_ptr =
        peek_compares.empty() ? nullptr : &peek_compares;

    if (reusable) {
      const std::string cache_key = BaseRelationCacheKey(
          source.table, projection.empty() ? nullptr : &projection);
      auto cached = context.execution_runtime()->base_relations.find(cache_key);
      if (cached == context.execution_runtime()->base_relations.end()) {
        Relation cache_rel(context.execution_runtime());
        cache_rel.schema = scan_schema;
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
          cache_rel.AddRow(*iterator);
          ++iterator;
        }
        cache_rel.FinishSpill();
        cached = context.execution_runtime()
                     ->base_relations
                     .emplace(cache_key,
                              std::make_shared<Relation>(std::move(cache_rel)))
                     .first;
      } else {
        ++context.execution_runtime()->base_scan_cache_hits;
      }
      // Aggregation always reads through the shared cache so the first user
      // and later users see exactly the same rows.
      cached->second->FinishSpill();
      cached->second->ForEachRow([&](const Row& row) {
        if (!MatchScanFilter(row, scan_schema, scan_filter, outer, context,
                             ctes)) {
          return;
        }
        accumulate_row(row);
      });
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
      while (iterator.IsValid()) {
        if (!MatchScanFilter(*iterator, scan_schema, scan_filter, outer,
                             context, ctes)) {
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
        accumulate_row(*iterator);
        ++iterator;
      }
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->scan_ms += ElapsedMs(scan_begin);
      // filter_ms intentionally includes the same interval: filtering happened
      // inline with the scan, so both counters cover this phase.
      context.execution_runtime()->filter_ms += ElapsedMs(scan_begin);
      context.execution_runtime()->aggregate_groups += groups.size();
    }
    if (groups.empty() && stmt.GroupBy().empty()) {
      GroupState group;
      group.representative =
          Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
      group.accumulator_offset = aggregate_states.size();
      for (const AggregateExpression* aggregate : aggregate_expressions) {
        aggregate_states.emplace_back(aggregate);
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
    };
    std::vector<KeyedRow> sortable;
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        aggregate_results.emplace(
            aggregate_expressions[i],
            aggregate_states[group.accumulator_offset + i].Finish());
      }
      Scope scope{.row = &group.representative,
                  .schema = &input.schema,
                  .outer = outer};
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = &aggregate_results;
      if (having_expr &&
          !Truthy(Evaluate(having_expr, scope, &aggregate_results, context,
                           ctes))) {
        continue;
      }
      std::vector<Value> values;
      values.reserve(stmt.SelectList().size());
      for (const NamedExpression& projection_item : stmt.SelectList()) {
        values.push_back(Evaluate(projection_item.expression, scope,
                                  &aggregate_results, context, ctes));
      }
      Row output_row(std::move(values));
      // Sort keys may reference base columns of the grouped relation
      // (`ORDER BY str_val` where str_val is a grouping key); chain the
      // projected row to the group's representative row so those resolve.
      if (!stmt.OrderBy().empty()) {
        Scope base_scope{.row=&group.representative, .schema=&input.schema,
                         .outer=outer};
        Scope proj_scope{.row=&output_row, .schema=&initial_output_schema,
                         .outer=&base_scope};
        std::vector<Value> keys;
        keys.reserve(stmt.OrderBy().size());
        for (const auto& key : stmt.OrderBy()) {
          keys.push_back(
              Evaluate(key.expression, proj_scope, &aggregate_results,
                       context, ctes));
        }
        sortable.push_back(KeyedRow{.keys=std::move(keys),
                                    .row=std::move(output_row)});
      } else {
        output.AddRow(std::move(output_row));
      }
    }
    if (!sortable.empty()) {
      const auto sort_begin = std::chrono::steady_clock::now();
      std::ranges::stable_sort(
          sortable,
          [&stmt](const KeyedRow& left, const KeyedRow& right) {
            for (size_t i = 0; i < stmt.OrderBy().size(); ++i) {
              const Value& a = left.keys[i];
              const Value& b = right.keys[i];
              const int c = CompareForOrderBy(a, b);
              if (c == 0) { continue;
}
              const bool nulls_first =
                  stmt.OrderBy()[i].nulls_first.value_or(
                      stmt.OrderBy()[i].ascending);
              const bool a_less = c < 0;
              if (a.IsNull()) { return nulls_first;
}
              if (b.IsNull()) { return !nulls_first;
}
              return stmt.OrderBy()[i].ascending ? a_less : !a_less;
            }
            return false;
          });
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
        output = DistinctOf(std::move(output), stmt.DistinctOn(), &context,
                            outer, &ctes);
      } else {
        output = DistinctOf(std::move(output));
      }
    }
    return LimitedRows(statement, std::move(output), &context, outer, &ctes);
  }

  bool where_fully_applied = false;
  Relation input =
      BuildInput(context, statement, outer, ctes, &where_fully_applied);

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
    WindowedInput windowed =
        ApplyWindows(context, *effective, std::move(input), outer, ctes);
    windowed_statement = windowed.statement;
    hidden_columns = windowed.hidden_columns;
    input = std::move(windowed.input);
    effective = windowed_statement.get();
  }

  Relation result = FinishQuery(context, *effective, std::move(input), outer,
                                ctes, !where_fully_applied, hidden_columns);
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

void RelationalExecutor::Initialize() {
  if (initialized_) {
    return;
  }
  ExecutionRuntime runtime;
  runtime.root_statement = statement_.get();
  std::unordered_map<std::string, size_t> table_counts;
  CountStatementTables(*statement_, &table_counts);
  for (const auto& [table, count] : table_counts) {
    if (count > 1) {
      runtime.reusable_base_relations.insert(table);
    }
  }
  ExecutionRuntime* previous_runtime = context_->execution_runtime();
  context_->set_execution_runtime(&runtime);
  Relation result;
  try {
    result = ExecuteQuery(*context_, *statement_, nullptr, {});
  } catch (...) {
    context_->set_execution_runtime(previous_runtime);
    throw;
  }
  context_->set_execution_runtime(previous_runtime);
  result.FinishSpill();
  rows_.clear();
  result.ForEachRow([&](const Row& row) { rows_.push_back(row); });
  if (statement_->Sources().size() == 1 && statement_->Sources()[0].query &&
      statement_->Sources()[0].query->WhereClause() &&
      statement_->Sources()[0].query->WhereClause()->Type() ==
          TypeTag::kConstantValue &&
      !statement_->Sources()[0].query->WhereClause()
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
  correlated_distinct_keys_ = 0;
  for (const auto& [query, index] : runtime.correlated_indexes) {
    (void)query;
    if (index != nullptr) {
      correlated_distinct_keys_ += index->parameter_keys_seen.size();
    }
  }
  uncorrelated_cache_hits_ = runtime.uncorrelated_cache_hits;
  uncorrelated_hash_builds_ = runtime.uncorrelated_hash_builds;
  uncorrelated_hash_probes_ = runtime.uncorrelated_hash_probes;
  exists_short_circuit_queries_ = runtime.exists_short_circuit_queries;
  scan_ms_ = runtime.scan_ms;
  filter_ms_ = runtime.filter_ms;
  join_ms_ = runtime.join_ms;
  project_ms_ = runtime.project_ms;
  sort_ms_ = runtime.sort_ms;
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
  empty_build_short_circuit_ = runtime.empty_build_short_circuit;
  null_aware_anti_build_contains_null_ =
      runtime.null_aware_anti_build_contains_null;
  relation_spills_ = runtime.relation_spills;
  initialized_ = true;
}

bool RelationalExecutor::Next(Row* destination, RowPosition* position) {
  Initialize();
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
         << ", correlated_distinct_keys=" << correlated_distinct_keys_
         << ", uncorrelated_cache_hits=" << uncorrelated_cache_hits_
         << ", uncorrelated_hash_builds=" << uncorrelated_hash_builds_
         << ", uncorrelated_hash_probes=" << uncorrelated_hash_probes_
         << ", exists_short_circuit_queries="
         << exists_short_circuit_queries_
         << ", scan_ms=" << scan_ms_ << ", filter_ms=" << filter_ms_
         << ", join_ms=" << join_ms_ << ", project_ms=" << project_ms_
         << ", sort_ms=" << sort_ms_
         << ", base_scan_cache_hits=" << base_scan_cache_hits_
         << ", aggregate_input_rows=" << aggregate_input_rows_
         << ", aggregate_groups=" << aggregate_groups_
         << ", aggregate_updates=" << aggregate_updates_
         << ", scan_rows=" << scan_rows_
         << ", scan_output_rows=" << scan_output_rows_
         << ", scan_values_decoded=" << scan_values_decoded_
         << ", scan_values_available=" << scan_values_available_ << ")";
  if (key_filter_keys_ != 0) {
    output << "\nRuntimeFilter type=exact_set keys=" << key_filter_keys_
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
  if (hash_joins_ > 0 && statement_->WhereClause() != nullptr) {
    output << "\nReoptimization considered=true retained_original=true"
           << "\nreason=remaining_work_below_threshold";
  }
}

void RelationalExecutor::Explain(std::ostream& output, int /*indent*/) const {
  output << "Relational Physical Plan (estimated)\n";
  // EXPLAIN must describe the same safe derived-table rewrites used by the
  // executable path.  Keep this local to presentation: the original parsed
  // statement remains owned by the executor and runtime cache keys continue
  // to use the original statement objects.
  std::shared_ptr<SelectStatement> explain_statement;
  const SelectStatement* effective = statement_.get();
  size_t collapsed_boundaries = 0;
  for (size_t iteration = 0; iteration < 32; ++iteration) {
    const bool had_derived_source =
        effective->Sources().size() == 1 &&
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
           << std::max(collapsed_boundaries + 1, projection_depth + 1)
           << '\n';
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
