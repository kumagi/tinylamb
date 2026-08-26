/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <deque>
#include <iostream>
#include <limits>
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
#include "executor/constant_executor.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/query_memory.hpp"
#include "executor/set_operation.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
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

namespace {

std::vector<Expression> RemoveConstantGroupKeys(
    const std::vector<Expression>& keys) {
  std::vector<Expression> result;
  result.reserve(keys.size());
  for (const Expression& key : keys) {
    // A constant GROUP BY expression cannot split a non-empty input. Keep the
    // fact that the original list was non-empty at the call sites so an empty
    // input still produces zero rows, as SQL requires for GROUP BY.
    if (key && key->Type() == TypeTag::kConstantValue) {
      continue;
    }
    result.push_back(key);
  }
  return result;
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
  const std::vector<Expression> group_keys =
      RemoveConstantGroupKeys(statement.GroupBy());
  std::vector<const AggregateExpression*> aggregate_expressions;
  std::unordered_set<const AggregateExpression*> seen_aggregates;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectAggregates(projection.expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  CollectAggregates(statement.Having(), &aggregate_expressions,
                    &seen_aggregates);

  struct GroupState {
    Row representative;
    size_t accumulator_offset{0};
  };
  std::deque<AggregateAccumulator> aggregate_states;
  auto accumulate_row = [&](const Row& row,
                            std::unordered_map<Row, size_t>* offsets,
                            std::vector<GroupState>* local_groups,
                            std::deque<AggregateAccumulator>* local_states) {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    std::vector<Value> key_values;
    for (const Expression& key : group_keys) {
      key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
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
      if (aggregate.WhereFilter() &&
          !Truthy(Evaluate(aggregate.WhereFilter(), scope, nullptr, context,
                           ctes))) {
        continue;
      }
      if (IsCountStar(aggregate)) {
        accumulator.Add(Value(1));
        continue;
      }
      AggregateInput input;
      try {
        input.value =
            Evaluate(aggregate.Child(), scope, nullptr, context, ctes);
      } catch (...) {
        // A per-row evaluation error (e.g. 1/0 inside an untaken IF/CASE
        // branch) contributes NULL rather than failing the whole aggregate.
        input.value = Value();
      }
      if (aggregate.Having() != AggregateHavingModifier::kNone &&
          aggregate.HavingCondition()) {
        input.condition = Evaluate(aggregate.HavingCondition(), scope, nullptr,
                                   context, ctes);
      }
      for (const auto& term : aggregate.InnerOrderBy()) {
        input.order_keys.push_back(
            Evaluate(term.expression, scope, nullptr, context, ctes));
      }
      if (aggregate.GetType() == AggregationType::kStringAgg &&
          aggregate.SecondaryArg()) {
        input.auxiliary =
            Evaluate(aggregate.SecondaryArg(), scope, nullptr, context, ctes);
      }
      accumulator.Add(std::move(input));
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
        !group_keys.empty() &&
        (input.HasSpill() || !QueryMemoryBudget::Global().CanReserve(
                                 std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      std::vector<SpillFile> parts(kSpillPartitions);
      input.ForEachRow([&](const Row& row) {
        Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
        std::vector<Value> key_values;
        for (const Expression& key : group_keys) {
          key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
        }
        Row key(std::move(key_values));
        parts[SpillPartitionOf(key.EncodeMemcomparableFormat(),
                               kSpillPartitions)]
            .Append(row);
      });
      for (SpillFile& part : parts) {
        part.FinishWriting();
      }
      // Every row was copied into a partition above.
      input.ResetContents();
      for (size_t part = 0; part < kSpillPartitions; ++part) {
        std::unordered_map<Row, size_t> offsets;
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
      std::unordered_map<Row, size_t> offsets;
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
  std::vector<std::vector<size_t>> star_columns(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    if (projection.expression->Type() == TypeTag::kColumnValue &&
        projection.expression->AsColumnValue().GetColumnName().name == "*") {
      const ColumnName& requested =
          projection.expression->AsColumnValue().GetColumnName();
      const bool qualified = !requested.schema.empty();
      for (size_t column = 0; column < base_width; ++column) {
        const Column& candidate = input.schema.GetColumn(column);
        if (matches_star(requested, candidate.Name(), qualified)) {
          star_columns[i].push_back(column);
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
    if (statement.Having() && !Truthy(Evaluate(statement.Having(), scope,
                                               aggregates, context, ctes))) {
      return;
    }
    std::vector<Value> values;
    std::unordered_map<std::string, Value> projection_cache;
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      const NamedExpression& projection = statement.SelectList()[item];
      if (projection.expression->Type() == TypeTag::kColumnValue &&
          projection.expression->AsColumnValue().GetColumnName().name == "*") {
        for (const size_t column : star_columns[item]) {
          values.push_back(representative.values_[column]);
        }
      } else if (ContainsQuery(projection.expression)) {
        // ToString() prints every scalar subquery as "SCALAR_SUBQUERY(...)",
        // so subqueries must never share the projection cache: two
        // syntactically different subqueries would collide on one key.
        values.push_back(
            Evaluate(projection.expression, scope, aggregates, context, ctes));
      } else {
        const std::string cache_key = projection.expression->ToString();
        const auto cached = projection_cache.find(cache_key);
        if (cached != projection_cache.end()) {
          values.push_back(cached->second);
        } else {
          Value value =
              Evaluate(projection.expression, scope, aggregates, context, ctes);
          projection_cache.emplace(cache_key, value);
          values.push_back(std::move(value));
        }
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
      for (const auto& key : order_by) {
        keys.push_back(
            Evaluate(key.expression, proj_scope, aggregates, context, ctes));
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
            if (a == b) {
              continue;
            }
            const bool nulls_first =
                order_by[i].nulls_first.value_or(order_by[i].ascending);
            if (a.IsNull()) {
              return nulls_first;
            }
            if (b.IsNull()) {
              return !nulls_first;
            }
            return order_by[i].ascending ? a < b : b < a;
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
    for (Row& row : output.rows) {
      row.values_.resize(std::min(visible_width, row.values_.size()));
    }
    output_columns.resize(visible_width);
  }
  if (!output.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
    }
  }
  output.schema = Schema("", std::move(output_columns));
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->project_ms += ElapsedMs(project_begin);
  }
  return output;
}

Relation DistinctOf(Relation input) {
  std::unordered_set<Row> seen;
  Relation distinct;
  distinct.schema = input.schema;
  CopyExecutionStats(&distinct, input);
  input.FinishSpill();
  input.ForEachRow([&](const Row& row) {
    if (seen.insert(row).second) {
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
          if (a == b) {
            continue;
          }
          const bool nulls_first =
              order_by[i].nulls_first.value_or(order_by[i].ascending);
          if (a.IsNull()) {
            return nulls_first;
          }
          if (b.IsNull()) {
            return !nulls_first;
          }
          return order_by[i].ascending ? a < b : b < a;
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

Relation LimitedRows(const SelectStatement& statement, Relation&& input) {
  Relation limited;
  limited.schema = input.schema;
  CopyExecutionStats(&limited, input);
  input.FinishSpill();
  const size_t total = input.TotalRows();
  const size_t begin = std::min(statement.Offset(), total);
  const size_t available = total - begin;
  const size_t count = !statement.HasLimit()
                           ? available
                           : std::min(statement.Limit(), available);
  size_t index = 0;
  input.ForEachRow([&](const Row& row) {
    const size_t current = index++;
    if (current < begin || current >= begin + count) {
      return;
    }
    limited.AddRow(row);
  });
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
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
    }
  }

  Relation output = Project(context, statement, std::move(input), outer, ctes,
                            hidden_columns);
  if (statement.Distinct()) {
    output = DistinctOf(std::move(output));
    if (!statement.OrderBy().empty()) {
      ApplyOrderBy(context, statement, &output, outer, ctes);
    }
  }
  return LimitedRows(statement, std::move(output));
}

namespace {

// True when `statement` reads the CTE `name` anywhere: table sources,
// FROM-subqueries, nested set-operation operands, or subqueries inside
// its expressions (scalar IN / EXISTS / quantified forms included).
bool ReferencesCte(const SelectStatement& statement, const std::string& name);

bool ReferencesCte(const SelectStatement& statement, const std::string& name);

// True when any expression subtree contains a subquery whose statement
// references `name` (scalar IN / EXISTS / quantified forms included).
bool ReferencesCteInExpression(
    const Expression& expression,
    const std::string& name) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const auto& query = expression->AsQueryExpression();
    if (ReferencesCte(*query.Query(), name)) {
      return true;
    }
    return ReferencesCteInExpression(query.Test(), name);
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             [&](const Expression& child) {
                               return ReferencesCteInExpression(child, name);
                             });
}

bool ReferencesCte(const SelectStatement& statement,
                   const std::string& name) {  // NOLINT(misc-no-recursion)
  for (const SelectSource& source : statement.Sources()) {
    if (!source.query && !source.unnest && source.table == name) {
      return true;
    }
    if (source.query && ReferencesCte(*source.query, name)) {
      return true;
    }
    if (ReferencesCteInExpression(source.join_condition, name)) {
      return true;
    }
  }
  if (ReferencesCteInExpression(statement.WhereClause(), name) ||
      ReferencesCteInExpression(statement.Having(), name)) {
    return true;
  }
  for (const NamedExpression& item : statement.SelectList()) {
    if (ReferencesCteInExpression(item.expression, name)) {
      return true;
    }
  }
  for (const Expression& key : statement.GroupBy()) {
    if (ReferencesCteInExpression(key, name)) {
      return true;
    }
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (ReferencesCteInExpression(term.expression, name)) {
      return true;
    }
  }
  for (const std::shared_ptr<SelectStatement>& operand : statement.UnionAll()) {
    if (operand && ReferencesCte(*operand, name)) {
      return true;
    }
  }
  return false;
}

// WITH RECURSIVE execution (RecursiveUnion + WorkTableScan): anchor terms
// seed a work table, then every recursive term re-runs with the CTE name
// bound to the previous iteration's rows until an iteration produces
// nothing new. UNION (DISTINCT) terms keep a seen-row set, which also
// terminates cyclic data. Iteration and total-row caps bound runaway
// recursion.
constexpr size_t kMaxRecursiveCteIterations = 1024;
constexpr size_t kMaxRecursiveCteRows = 10'000'000;

Relation ExecuteRecursiveCte(TransactionContext& context,
                             const std::string& name,
                             const SelectStatement& body, const Scope* outer,
                             const CteMap& inherited_ctes,
                             const RecursiveDepthSpec* depth) {
  // Decompose the body into its set-operation terms: the head operand plus
  // every UnionAll() branch.
  std::vector<SelectStatement> terms;
  std::vector<SetOperationKind> kinds;
  SelectStatement head = body;
  head.ClearUnionAll();
  head.SetOrderBy({});
  head.SetLimit(std::nullopt);
  head.SetOffset(0);
  terms.push_back(std::move(head));
  kinds.push_back(SetOperationKind::kUnionAll);
  for (size_t i = 0; i < body.UnionAll().size(); ++i) {
    terms.push_back(*body.UnionAll()[i]);
    kinds.push_back(i < body.SetOperationKinds().size()
                        ? body.SetOperationKinds()[i]
                        : SetOperationKind::kUnionAll);
  }

  std::vector<size_t> anchors;
  std::vector<size_t> recursive_terms;
  for (size_t i = 0; i < terms.size(); ++i) {
    if (ReferencesCte(terms[i], name)) {
      recursive_terms.push_back(i);
    } else {
      anchors.push_back(i);
    }
  }
  if (recursive_terms.empty()) {
    // Nothing actually references the CTE: evaluate as a plain query chain,
    // clause handling included (CTE bodies may legitimately ORDER BY).
    return ExecuteQuery(context, body, outer, inherited_ctes);
  }
  if (anchors.empty()) {
    throw std::runtime_error("recursive CTE " + name +
                             " requires a non-recursive anchor term");
  }

  // UNION [DISTINCT] anywhere in the chain makes membership distinct over
  // the accumulated result (and terminates cyclic data).
  const bool distinct =
      body.Distinct() || std::ranges::any_of(kinds, [](SetOperationKind kind) {
        return kind != SetOperationKind::kUnionAll;
      });

  // WITH DEPTH: every row carries its recursion depth in an appended column.
  const bool track_depth = depth != nullptr;
  const size_t payload_width = track_depth ? 1 : 0;

  Relation output(context.execution_runtime());
  std::unordered_set<Row> seen;
  // DISTINCT membership key: NaN never equals itself and Value's epsilon
  // double equality rejects inf==inf (inf-inf is NaN), so non-finite
  // payloads are canonicalized before hashing/equality or a recursion
  // producing them would never converge.
  auto distinct_key = [](const Row& row) {
    Row key = row;
    for (Value& value : key.values_) {
      if (value.type != ValueType::kDouble) {
        continue;
      }
      const double scalar = value.value.double_value;
      if (std::isnan(scalar)) {
        value = Value(std::string("__tinylamb_nan__"));
      } else if (std::isinf(scalar)) {
        value = Value(std::string(scalar > 0 ? "__tinylamb_pinf__"
                                             : "__tinylamb_ninf__"));
      }
    }
    return key;
  };
  auto append = [&](Relation& destination, const Row& payload,
                    int64_t depth_value, bool apply_range) {
    // DISTINCT membership is decided on the payload columns only: the
    // synthetic depth column must not turn one payload into two members.
    if (distinct && !seen.insert(distinct_key(payload)).second) {
      return;
    }
    // Rows outside [lower, upper] stay hidden even though they were
    // generated (the anchor below a non-zero lower bound, rounds past the
    // upper bound).
    if (track_depth && apply_range &&
        (depth_value < depth->lower || depth_value > depth->upper)) {
      return;
    }
    Row stored = payload;
    if (track_depth) {
      stored.values_.push_back(Value(depth_value));
    }
    destination.AddRow(std::move(stored));
  };

  // Anchor pass seeds both the visible result and the first work-table delta.
  bool schema_initialized = false;
  Relation delta(context.execution_runtime());
  for (const size_t anchor : anchors) {
    Relation part = ExecuteQuery(context, terms[anchor], outer, inherited_ctes);
    part.FinishSpill();
    if (!schema_initialized) {
      output.schema = part.schema;
      if (track_depth && !depth->column.empty()) {
        output.schema = output.schema +
                        Schema("", {Column(depth->column, ValueType::kInt64)});
      }
      delta.schema = output.schema;
      schema_initialized = true;
    }
    if (part.schema.ColumnCount() + payload_width !=
        output.schema.ColumnCount()) {
      throw std::invalid_argument("recursive CTE " + name +
                                  " operands must have equal column counts");
    }
    part.ForEachRow([&](const Row& row) {
      // Anchor rows have depth 0 by definition. They always seed the work
      // table, but only become visible when 0 lies inside the DEPTH range.
      if (distinct) {
        seen.insert(distinct_key(row));
      }
      Row stored = row;
      if (track_depth) {
        stored.values_.push_back(Value(0));
      }
      delta.AddRow(stored);
      const bool anchor_visible =
          !track_depth || (0 >= depth->lower && 0 <= depth->upper);
      if (anchor_visible) {
        output.AddRow(std::move(stored));
      }
    });
  }
  delta.FinishSpill();
  if (!schema_initialized) {
    return output;
  }
  output.FinishSpill();

  // Iterative pass: each round binds the CTE name to the previous round's
  // rows only (standard worktable semantics).
  auto delta_state = std::make_shared<Relation>(std::move(delta));
  const int64_t depth_cap =
      track_depth ? depth->upper : std::numeric_limits<int64_t>::max();
  bool depth_exhausted = false;
  // The first round runs even on an empty seed: a recursive term may carry
  // its own non-recursive branch (e.g. UNION ALL (SELECT 1)) that starts the
  // iteration when every anchor produced nothing.
  bool seeded = delta_state->TotalRows() != 0;
  for (size_t iteration = 0; iteration < kMaxRecursiveCteIterations;
       ++iteration) {
    if (!seeded && iteration > 0 && delta_state->TotalRows() == 0) {
      break;
    }
    if (iteration > 0) {
      seeded = true;
    }
    const int64_t round_depth =
        track_depth ? static_cast<int64_t>(iteration) + 1 : 0;
    if (track_depth && round_depth > depth_cap) {
      // WITH DEPTH exhausted: remaining delta rows cannot produce visible
      // output, so stop cleanly instead of reporting a runaway recursion.
      depth_exhausted = true;
      break;
    }
    if (output.TotalRows() > kMaxRecursiveCteRows) {
      throw std::runtime_error("recursive CTE " + name +
                               " exceeded the row budget of " +
                               std::to_string(kMaxRecursiveCteRows));
    }
    CteMap loop_ctes = inherited_ctes;
    loop_ctes[name] = delta_state;

    Relation next_delta(context.execution_runtime());
    next_delta.schema = output.schema;
    for (const size_t term_index : recursive_terms) {
      Relation part =
          ExecuteQuery(context, terms[term_index], outer, loop_ctes);
      part.FinishSpill();
      if (terms[term_index].Distinct()) {
        part = DistinctOf(std::move(part));
        part.FinishSpill();
      }
      if (part.schema.ColumnCount() + payload_width !=
          output.schema.ColumnCount()) {
        throw std::invalid_argument("recursive CTE " + name +
                                    " operands must have equal column counts");
      }
      part.ForEachRow(
          [&](const Row& row) { append(next_delta, row, round_depth, true); });
    }
    next_delta.FinishSpill();
    next_delta.ForEachRow([&](const Row& row) { output.AddRow(row); });
    output.FinishSpill();
    delta_state = std::make_shared<Relation>(std::move(next_delta));
  }
  if (!depth_exhausted && delta_state->TotalRows() != 0) {
    throw std::runtime_error("recursive CTE " + name + " exceeded " +
                             std::to_string(kMaxRecursiveCteIterations) +
                             " iterations");
  }
  // Trailing ORDER BY / LIMIT on the body shape the accumulated result.
  if (!body.OrderBy().empty() || body.HasLimit()) {
    SelectStatement shaped = body;
    shaped.ClearUnionAll();
    ApplyOrderBy(context, shaped, &output, outer, inherited_ctes);
    return LimitedRows(shaped, std::move(output));
  }
  return output;
}

}  // namespace

Relation ExecuteQuery(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& inherited_ctes) {
  EnsureReusableProjections(context, context.execution_runtime());
  CteMap ctes = inherited_ctes;
  // WITH RECURSIVE lets a definition reference any sibling regardless of
  // textual position, so resolve in dependency order (a definition only runs
  // once every sibling it mentions has been materialized).
  const auto& declared = statement.WithQueryOrder();
  std::vector<std::string> resolution_order;
  std::unordered_set<std::string> resolved;
  bool progress = true;
  while (resolution_order.size() < declared.size() && progress) {
    progress = false;
    for (const std::string& name : declared) {
      if (resolved.contains(name)) {
        continue;
      }
      const auto& query = statement.WithQueries().at(name);
      bool ready = true;
      for (const std::string& sibling : declared) {
        if (sibling == name || resolved.contains(sibling)) {
          continue;
        }
        if (ReferencesCte(*query, sibling)) {
          ready = false;
          break;
        }
      }
      if (!ready) {
        continue;
      }
      resolution_order.push_back(name);
      resolved.insert(name);
      progress = true;
    }
  }
  if (resolution_order.size() != declared.size()) {
    throw std::runtime_error("mutually recursive CTEs are not supported: " +
                             statement.WithQueryOrder().front());
  }
  for (const std::string& name : resolution_order) {
    const auto& query = statement.WithQueries().at(name);
    ctes[name] = std::make_shared<Relation>(
        statement.IsRecursiveWith(name)
            ? ExecuteRecursiveCte(context, name, *query, outer, ctes,
                                  statement.RecursiveDepthOf(name))
            : ExecuteQuery(context, *query, outer, ctes));
  }

  // GROUPING SETS / ROLLUP / CUBE: run the aggregation once per grouping set
  // with excluded group columns rewritten to NULL (and GROUPING(col) to its
  // 0/1 constant), then concatenate.  ORDER BY/LIMIT apply to the combined
  // result, mirroring the set-operation handling below.
  if (!statement.GroupingSets().empty()) {
    Relation combined(context.execution_runtime());
    bool first = true;
    auto lower = [](std::string_view s) {
      std::string out(s);
      for (char& c : out) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      return out;
    };
    for (const auto& set : statement.GroupingSets()) {
      SelectStatement variant = statement;
      variant.ClearUnionAll();
      variant.SetOrderBy({});
      variant.SetLimit(std::nullopt);
      variant.SetOffset(0);
      variant.SetGroupingSets({});
      // Active grouping-set entries keyed by rendered text so plain columns
      // and arbitrary expressions match uniformly.
      std::unordered_set<std::string> active_names;
      std::unordered_set<std::string> active_keys;
      auto collect_touched = [&](const Expression& e,
                                 auto&& collect_self) -> void {
        if (!e || e->Type() == TypeTag::kAggregateExp) {
          return;
        }
        if (e->Type() == TypeTag::kColumnValue) {
          active_names.insert(lower(e->AsColumnValue().GetColumnName().name));
        }
        for (const Expression& child : ExpressionChildren(e)) {
          collect_self(child, collect_self);
        }
      };
      for (const Expression& column : set) {
        if (!column) {
          continue;
        }
        active_keys.insert(lower(column->ToString()));
        collect_touched(column, collect_touched);
      }
      // A projection/having expression stays computable when every column it
      // references belongs to the active grouping set; otherwise GoogleSQL
      // reports it as NULL (aggregated over).
      auto references_inactive_column = [&](const Expression& e) -> bool {
        bool inactive = false;
        auto walk = [&](const Expression& node, auto&& walk_self) -> void {
          if (!node || inactive || node->Type() == TypeTag::kAggregateExp) {
            return;
          }
          if (node->Type() == TypeTag::kColumnValue) {
            const std::string name =
                lower(node->AsColumnValue().GetColumnName().name);
            bool is_group_universe = false;
            for (const Expression& universe_column : statement.GroupBy()) {
              if (!universe_column) {
                continue;
              }
              if (lower(universe_column->ToString()) ==
                  lower(node->ToString())) {
                is_group_universe = true;
                break;
              }
              if (universe_column->Type() == TypeTag::kColumnValue &&
                  lower(
                      universe_column->AsColumnValue().GetColumnName().name) ==
                      name) {
                is_group_universe = true;
                break;
              }
            }
            if (is_group_universe && active_names.count(name) == 0) {
              inactive = true;
            }
            return;
          }
          for (const Expression& child : ExpressionChildren(node)) {
            walk_self(child, walk_self);
          }
        };
        walk(e, walk);
        return inactive;
      };
      auto rewrite = [&](const Expression& expression,
                         auto&& rewrite_self) -> Expression {
        if (!expression) {
          return expression;
        }
        if (expression->Type() == TypeTag::kAggregateExp) {
          return expression;  // aggregates keep their own input scope
        }
        if (references_inactive_column(expression)) {
          return ConstantValueExp(Value());
        }
        if (expression->Type() == TypeTag::kFunctionCallExp) {
          const auto& call = expression->AsFunctionCallExpression();
          std::string name = call.FuncName();
          for (char& c : name) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          if (name == "grouping" && call.Args().size() == 1 && call.Args()[0]) {
            // GROUPING(expr): 1 when expr is aggregated over, else 0.
            const std::string arg_key = lower(call.Args()[0]->ToString());
            return ConstantValueExp(Value(
                active_keys.count(arg_key) != 0 ? int64_t{0} : int64_t{1}));
          }
        }
        std::vector<Expression> children = ExpressionChildren(expression);
        if (children.empty()) {
          return expression;
        }
        bool changed = false;
        for (Expression& child : children) {
          Expression replaced = rewrite_self(child, rewrite_self);
          if (replaced.get() != child.get()) {
            changed = true;
          }
          child = std::move(replaced);
        }
        if (!changed) {
          return expression;
        }
        return WithExpressionChildren(expression, std::move(children));
      };
      std::vector<NamedExpression> projections = variant.SelectList();
      for (NamedExpression& item : projections) {
        item.expression = rewrite(item.expression, rewrite);
      }
      variant.SetSelectList(std::move(projections));
      if (variant.Having()) {
        Expression having = rewrite(variant.Having(), rewrite);
        variant.SetHaving(having);
      }
      std::vector<Expression> variant_group_by(set.begin(), set.end());
      if (variant_group_by.empty()) {
        // The grand-total set: force single-group aggregation even when the
        // projection has no aggregate calls (GROUP BY GROUPING SETS(()))
        // by supplying a constant key that splits nothing.
        variant_group_by.push_back(ConstantValueExp(Value(int64_t{0})));
      }
      variant.SetGroupBy(std::move(variant_group_by));
      Relation rows = ExecuteQuery(context, variant, outer, ctes);
      combined.FinishSpill();
      if (first) {
        combined.schema = rows.schema;
        CopyExecutionStats(&combined, rows);
        first = false;
      } else {
        size_t joins = rows.hash_joins + rows.hybrid_hash_joins +
                       rows.in_memory_hash_joins + rows.nested_loop_joins;
        combined.hash_joins += rows.hash_joins;
        combined.hybrid_hash_joins += rows.hybrid_hash_joins;
        combined.in_memory_hash_joins += rows.in_memory_hash_joins;
        combined.nested_loop_joins += rows.nested_loop_joins;
        (void)joins;
      }
      rows.ForEachRow([&](const Row& row) { combined.AddRow(row); });
    }
    combined.FinishSpill();
    if (!statement.OrderBy().empty()) {
      // Order terms over aggregates cannot re-evaluate against the combined
      // rows; resolve them to their matching output projection column.
      SelectStatement ordered = statement;
      std::vector<SelectStatement::OrderByTerm> terms = ordered.OrderBy();
      const std::vector<NamedExpression>& projections = ordered.SelectList();
      for (auto& term : terms) {
        if (!term.expression || !ContainsAggregate(term.expression)) {
          continue;
        }
        const std::string want = term.expression->ToString();
        for (size_t idx = 0; idx < projections.size(); ++idx) {
          const NamedExpression& item = projections[idx];
          if (!item.expression || item.expression->ToString() != want) {
            continue;
          }
          // The combined relation's physical column name (aliases or the
          // synthesized $exprN) is the only reliable reference here.
          if (idx < combined.schema.ColumnCount()) {
            term.expression =
                ColumnValueExp(combined.schema.GetColumn(idx).Name());
          }
          break;
        }
      }
      ordered.SetOrderBy(std::move(terms));
      ApplyOrderBy(context, ordered, &combined, outer, ctes);
    }
    return LimitedRows(statement, std::move(combined));
  }

  // A set-operation's ORDER BY/LIMIT/OFFSET apply to the concatenated result,
  // not just to the first operand.  The visitor stores the top-level clauses
  // on that first SelectStatement, so execute a copy of the head operand with
  // those clauses deferred until every UNION ALL branch has been appended.
  if (!statement.UnionAll().empty()) {
    const std::vector<SetOperationKind>& operations =
        statement.SetOperationKinds();
    const bool all_union_all =
        std::ranges::all_of(operations, [](SetOperationKind operation) {
          return operation == SetOperationKind::kUnionAll;
        });
    const bool push_limit_shape =
        all_union_all && statement.OrderBy().empty() && !statement.Distinct() &&
        statement.HasLimit() && statement.Limit() != 0;
    const bool push_limit =
        push_limit_shape &&
        statement.Offset() <=
            std::numeric_limits<size_t>::max() - statement.Limit();
    const size_t branch_cap =
        push_limit ? statement.Offset() + statement.Limit() : 0;

    SelectStatement head = statement;
    head.ClearUnionAll();
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    if (push_limit) {
      head.SetLimit(branch_cap);
    }
    Relation combined = ExecuteQuery(context, head, outer, ctes);
    const std::vector<SetOperationMatch>& matches = statement.Matches();
    for (size_t i = 0; i < statement.UnionAll().size(); ++i) {
      SelectStatement branch_statement = *statement.UnionAll()[i];
      if (push_limit && !branch_statement.HasLimit()) {
        const size_t branch_offset = branch_statement.Offset();
        if (branch_offset <= std::numeric_limits<size_t>::max() - branch_cap) {
          branch_statement.SetLimit(branch_offset + branch_cap);
        }
      }
      Relation branch = ExecuteQuery(context, branch_statement, outer, ctes);
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      combined.FinishSpill();
      branch.FinishSpill();

      // BY NAME / CORRESPONDING [BY (...)]: align operands on column names
      // before applying the operation.  Plain mode keeps positions.
      SetOperationMatch match =
          i < matches.size() ? matches[i] : SetOperationMatch{};
      if (match.corresponding || match.by_name) {
        auto fold_case = [](char a, char b) {
          return std::tolower(static_cast<unsigned char>(a)) ==
                 std::tolower(static_cast<unsigned char>(b));
        };
        auto has_column = [&](const Relation& relation,
                              const std::string& want) {
          for (size_t c = 0; c < relation.schema.ColumnCount(); ++c) {
            const ColumnName& candidate = relation.schema.GetColumn(c).Name();
            if (candidate.name.size() == want.size() &&
                std::equal(want.begin(), want.end(), candidate.name.begin(),
                           fold_case)) {
              return true;
            }
          }
          return false;
        };
        std::vector<std::string> target_names;
        // INNER BY NAME intersects the name sets; FULL BY NAME unions them.
        const bool by_name_intersect = match.by_name && match.corresponding;
        if (!match.columns.empty()) {
          target_names = match.columns;
        } else if (match.by_name && !by_name_intersect) {
          // Union of both sides' names, left order first.
          for (size_t c = 0; c < combined.schema.ColumnCount(); ++c) {
            const ColumnName& name = combined.schema.GetColumn(c).Name();
            if (!has_column(branch, name.name)) {
              target_names.push_back(name.name);
            } else {
              bool already = false;
              for (const std::string& seen : target_names) {
                if (seen.size() == name.name.size() &&
                    std::equal(seen.begin(), seen.end(), name.name.begin(),
                               fold_case)) {
                  already = true;
                  break;
                }
              }
              if (!already) {
                target_names.push_back(name.name);
              }
            }
          }
          for (size_t c = 0; c < branch.schema.ColumnCount(); ++c) {
            const ColumnName& name = branch.schema.GetColumn(c).Name();
            if (!has_column(combined, name.name)) {
              target_names.push_back(name.name);
            }
          }
        } else {
          // CORRESPONDING: intersection in left order.
          for (size_t c = 0; c < combined.schema.ColumnCount(); ++c) {
            const ColumnName& name = combined.schema.GetColumn(c).Name();
            if (has_column(branch, name.name)) {
              target_names.push_back(name.name);
            }
          }
        }
        auto project_side = [](const Relation& relation,
                               const std::vector<std::string>& names,
                               bool pad_missing) {
          std::vector<slot_t> indexes;
          std::vector<bool> is_null;
          std::vector<Column> columns;
          for (const std::string& want : names) {
            slot_t found = static_cast<slot_t>(-1);
            for (size_t c = 0; c < relation.schema.ColumnCount(); ++c) {
              const ColumnName& candidate = relation.schema.GetColumn(c).Name();
              if (candidate.name.size() == want.size() &&
                  std::equal(
                      want.begin(), want.end(), candidate.name.begin(),
                      [](char a, char b) {
                        return std::tolower(static_cast<unsigned char>(a)) ==
                               std::tolower(static_cast<unsigned char>(b));
                      })) {
                found = static_cast<slot_t>(c);
                break;
              }
            }
            if (found == static_cast<slot_t>(-1)) {
              if (!pad_missing) {
                throw std::runtime_error("CORRESPONDING BY column " + want +
                                         " not present in both inputs");
              }
              is_null.push_back(true);
              indexes.push_back(static_cast<slot_t>(0));
              columns.emplace_back(want);
              continue;
            }
            is_null.push_back(false);
            indexes.push_back(found);
            columns.push_back(
                relation.schema.GetColumn(static_cast<size_t>(found)));
          }
          std::vector<Row> projected;
          projected.reserve(relation.rows.size());
          relation.ForEachRow([&](const Row& row) {
            Row out;
            out.values_.reserve(indexes.size());
            for (size_t k = 0; k < indexes.size(); ++k) {
              out.values_.push_back(
                  is_null[k] ? Value()
                             : row.values_[static_cast<size_t>(indexes[k])]);
            }
            projected.push_back(std::move(out));
          });
          return std::make_pair(
              std::move(projected),
              Schema(relation.schema.Name(), std::move(columns)));
        };
        auto [left_projected_rows, left_schema] =
            project_side(combined, target_names, true);
        auto [right_projected_rows, right_schema] =
            project_side(branch, target_names, true);
        SetOperationKind op_i =
            i < operations.size() ? operations[i] : SetOperationKind::kUnionAll;
        SetOperationExecutor aligned(
            {std::make_shared<ConstantExecutor>(std::move(left_projected_rows)),
             std::make_shared<ConstantExecutor>(
                 std::move(right_projected_rows))},
            op_i);
        Relation folded_corresponding(context.execution_runtime());
        folded_corresponding.schema = std::move(left_schema);
        CopyExecutionStats(&folded_corresponding, combined);
        Row corr_row;
        while (aligned.Next(&corr_row, nullptr)) {
          folded_corresponding.AddRow(std::move(corr_row));
        }
        folded_corresponding.FinishSpill();
        combined = std::move(folded_corresponding);
        continue;
      }

      combined.ForEachRow([&](const Row& row) { left_rows.push_back(row); });
      branch.ForEachRow([&](const Row& row) { right_rows.push_back(row); });
      SetOperationKind operation = SetOperationKind::kUnionAll;
      if (i < operations.size()) {
        operation = operations[i];
      }
      SetOperationExecutor set_operation(
          {std::make_shared<ConstantExecutor>(std::move(left_rows)),
           std::make_shared<ConstantExecutor>(std::move(right_rows))},
          operation);
      Relation folded(context.execution_runtime());
      folded.schema = combined.schema;
      CopyExecutionStats(&folded, combined);
      Row row;
      while (set_operation.Next(&row, nullptr)) {
        folded.AddRow(std::move(row));
      }
      folded.FinishSpill();
      combined = std::move(folded);
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
      throw std::runtime_error("table " + source.table + " not found; ctes=" +
                               std::to_string(ctes.size()));
    }
    const Schema& table_schema = table.Value()->GetSchema();
    std::vector<slot_t> projection = RequiredColumns(statement, table_schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(context, source.table)) {
      projection = *shared;
    }
    Schema scan_schema = projection.empty()
                             ? Schema("", std::vector<Column>{})
                             : ProjectSchema(table_schema, projection);
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    Schema qualified_schema =
        qualifier.empty() ? scan_schema : QualifySchema(scan_schema, qualifier);

    std::vector<Expression> scan_predicates =
        SplitConjuncts(statement.WhereClause());
    CompiledScanFilter scan_filter =
        CompileScanFilter(scan_predicates, scan_schema);

    Relation input;
    input.schema = qualified_schema;
    // Project aggregates against the qualified schema; feed rows one at a time
    // without retaining them in input.rows.
    std::vector<const AggregateExpression*> aggregate_expressions;
    std::unordered_set<const AggregateExpression*> seen_aggregates;
    for (const NamedExpression& projection_item : statement.SelectList()) {
      CollectAggregates(projection_item.expression, &aggregate_expressions,
                        &seen_aggregates);
    }
    CollectAggregates(statement.Having(), &aggregate_expressions,
                      &seen_aggregates);

    struct GroupState {
      Row representative;
      size_t accumulator_offset{0};
    };
    std::deque<AggregateAccumulator> aggregate_states;
    std::vector<GroupState> groups;
    std::unordered_map<Row, size_t> offsets;
    const std::vector<Expression> group_keys =
        RemoveConstantGroupKeys(statement.GroupBy());

    std::vector<std::optional<slot_t>> group_offsets;
    group_offsets.reserve(group_keys.size());
    bool group_keys_are_columns = true;
    for (const Expression& key : group_keys) {
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
      key_values.reserve(group_keys.size());
      if (group_keys_are_columns) {
        for (const auto& offset : group_offsets) {
          key_values.push_back(row[*offset]);
        }
      } else {
        for (const Expression& key : group_keys) {
          key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
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
          AggregateInput input;
          try {
            input.value =
                Evaluate(aggregate.Child(), scope, nullptr, context, ctes);
          } catch (...) {
            // Untaken IF/CASE branches must not fail the aggregate.
            input.value = Value();
          }
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
          accumulator.Add(std::move(input));
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
    if (groups.empty() && statement.GroupBy().empty()) {
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
    for (size_t i = 0; i < statement.SelectList().size(); ++i) {
      const NamedExpression& projection_item = statement.SelectList()[i];
      output_columns.emplace_back(ProjectionName(projection_item, i),
                                  ValueType::kNull);
    }
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
      if (statement.Having() &&
          !Truthy(Evaluate(statement.Having(), scope, &aggregate_results,
                           context, ctes))) {
        continue;
      }
      std::vector<Value> values;
      values.reserve(statement.SelectList().size());
      for (const NamedExpression& projection_item : statement.SelectList()) {
        values.push_back(Evaluate(projection_item.expression, scope,
                                  &aggregate_results, context, ctes));
      }
      output.AddRow(Row(std::move(values)));
    }
    for (size_t i = 0; i < output_columns.size() && !output.rows.empty(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
    }
    output.schema = Schema("", std::move(output_columns));
    output.FinishSpill();
    // DISTINCT / ORDER BY / LIMIT without running Project again.
    if (statement.Distinct()) {
      output = DistinctOf(std::move(output));
    }
    if (!statement.OrderBy().empty()) {
      ApplyOrderBy(context, statement, &output, outer, ctes);
    }
    return LimitedRows(statement, std::move(output));
  }

  bool where_fully_applied = false;
  Relation input =
      BuildInput(context, statement, outer, ctes, &where_fully_applied);

  size_t hidden_columns = 0;
  const SelectStatement* effective = &statement;
  std::shared_ptr<SelectStatement> windowed_statement;
  if (HasWindowFunctions(statement)) {
    WindowedInput windowed =
        ApplyWindows(context, statement, std::move(input), outer, ctes);
    windowed_statement = windowed.statement;
    hidden_columns = windowed.hidden_columns;
    input = std::move(windowed.input);
    effective = windowed_statement.get();
  }

  Relation result = FinishQuery(context, *effective, std::move(input), outer,
                                ctes, !where_fully_applied, hidden_columns);
  for (const auto& union_stmt : statement.UnionAll()) {
    Relation union_res = ExecuteQuery(context, *union_stmt, outer, ctes);
    union_res.ForEachRow([&](const Row& row) { result.AddRow(row); });
  }
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
  hash_joins_ = result.hash_joins;
  hybrid_hash_joins_ = result.hybrid_hash_joins;
  in_memory_hash_joins_ = result.in_memory_hash_joins;
  nested_loop_joins_ = result.nested_loop_joins;
  join_comparisons_ = result.join_comparisons;
  peak_intermediate_rows_ = result.peak_intermediate_rows;
  correlated_index_builds_ = runtime.correlated_index_builds;
  correlated_index_probes_ = runtime.correlated_index_probes;
  correlated_result_cache_hits_ = runtime.correlated_result_cache_hits;
  uncorrelated_cache_hits_ = runtime.uncorrelated_cache_hits;
  uncorrelated_hash_builds_ = runtime.uncorrelated_hash_builds;
  uncorrelated_hash_probes_ = runtime.uncorrelated_hash_probes;
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
  exists_short_circuit_queries_ = runtime.exists_short_circuit_queries;
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
         << ", uncorrelated_cache_hits=" << uncorrelated_cache_hits_
         << ", uncorrelated_hash_builds=" << uncorrelated_hash_builds_
         << ", uncorrelated_hash_probes=" << uncorrelated_hash_probes_
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
         << ", scan_values_available=" << scan_values_available_
         << ", exists_short_circuit_queries=" << exists_short_circuit_queries_
         << ")";
}

void RelationalExecutor::Explain(std::ostream& output, int /*indent*/) const {
  output << "Relational Physical Plan (estimated)\n";
  WriteEstimatedPhysicalPlan(*context_, *statement_, output, 2);
  if (initialized_) {
    output << "Actual Joins: hybrid_hash_joins=" << hybrid_hash_joins_
           << " in_memory_hash_joins=" << in_memory_hash_joins_
           << " nested_loop_joins=" << nested_loop_joins_
           << " relation_spills=" << relation_spills_ << '\n';
  }
}

}  // namespace tinylamb
