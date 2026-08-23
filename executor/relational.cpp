/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/aggregation.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/named_expression.hpp"
#include "expression/expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/value.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

Relation Project(TransactionContext& context, const SelectStatement& statement,
                 Relation input, const Scope* outer, const CteMap& ctes) {
  const auto project_begin = std::chrono::steady_clock::now();
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return ContainsAggregate(projection.expression);
                  }) ||
      ContainsAggregate(statement.Having());
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
  auto accumulate_row = [&](const Row& row, std::unordered_map<Row, size_t>* offsets,
                            std::vector<GroupState>* local_groups,
                            std::deque<AggregateAccumulator>* local_states) {
    Scope scope{.row=&row, .schema=&input.schema, .outer=outer};
    std::vector<Value> key_values;
    for (const Expression& key : statement.GroupBy()) {
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
      accumulator.Add(
          IsCountStar(aggregate)
              ? Value(1)
              : Evaluate(aggregate.Child(), scope, nullptr, context, ctes));
      if (active_runtime != nullptr) { ++active_runtime->aggregate_updates;
}
    }
    // Only new groups need a representative row.
    if (inserted) { group.representative = row;
}
    if (active_runtime != nullptr) { ++active_runtime->aggregate_input_rows;
}
  };
  auto make_group = [&]() {
    GroupState group;
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
        (input.HasSpill() ||
         !QueryMemoryBudget::Global().CanReserve(
             std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      std::vector<SpillFile> parts(kSpillPartitions);
      input.ForEachRow([&](const Row& row) {
        Scope scope{.row=&row, .schema=&input.schema, .outer=outer};
        std::vector<Value> key_values;
        for (const Expression& key : statement.GroupBy()) {
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
    if (active_runtime != nullptr) { active_runtime->aggregate_groups += groups.size();
}
  }

  Relation output;
  CopyExecutionStats(&output, input);
  std::vector<Column> output_columns;
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    if (projection.expression->Type() == TypeTag::kColumnValue &&
        projection.expression->AsColumnValue().GetColumnName().name == "*") {
      for (size_t column = 0; column < input.schema.ColumnCount(); ++column) {
        output_columns.emplace_back(input.schema.GetColumn(column).Name().name,
                                    input.schema.GetColumn(column).Type());
      }
    } else {
      output_columns.emplace_back(ProjectionName(projection, i),
                                  ValueType::kNull);
    }
  }

  auto emit = [&](const Row& representative,
                  const AggregateResultMap* aggregates) {
    Scope scope{.row=&representative, .schema=&input.schema, .outer=outer};
    if (statement.Having() &&
        !Truthy(Evaluate(statement.Having(), scope, aggregates, context,
                         ctes))) {
      return;
    }
    std::vector<Value> values;
    for (const NamedExpression& projection : statement.SelectList()) {
      if (projection.expression->Type() == TypeTag::kColumnValue &&
          projection.expression->AsColumnValue().GetColumnName().name == "*") {
        values.insert(values.end(), representative.values_.begin(),
                      representative.values_.end());
      } else {
        values.push_back(Evaluate(projection.expression, scope, aggregates,
                                  context, ctes));
      }
    }
    output.AddRow(Row(std::move(values)));
  };

  if (grouped) {
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        aggregate_results.emplace(accumulator.expression,
                                  accumulator.Finish());
      }
      emit(group.representative, &aggregate_results);
    }
  } else {
    input.FinishSpill();
    input.ForEachRow([&](const Row& row) { emit(row, nullptr); });
  }
  output.FinishSpill();
  if (!output.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
    }
  }
  output.schema = Schema("", std::move(output_columns));
  if (active_runtime != nullptr) { active_runtime->project_ms += ElapsedMs(project_begin);
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
    if (seen.insert(row).second) { distinct.AddRow(row);
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
    Scope scope{.row=&row, .schema=&output->schema, .outer=outer};
    std::vector<Value> keys;
    keys.reserve(order_by.size());
    for (const auto& key : order_by) {
      keys.push_back(Evaluate(key.expression, scope, nullptr, context, ctes));
    }
    sortable.push_back(KeyedRow{.keys=std::move(keys), .row=row});
  });
  // Every row was copied into `sortable`; detach the finished spill files so
  // re-adding below cannot hit "Append after FinishWriting".
  output->ResetContents();
  std::ranges::stable_sort(
      sortable,
      [&](const KeyedRow& left, const KeyedRow& right) {
        for (size_t i = 0; i < order_by.size(); ++i) {
          const Value& a = left.keys[i];
          const Value& b = right.keys[i];
          if (a == b) { continue;
}
          // NULLS FIRST on ASC, NULLS LAST on DESC.
          if (a.IsNull()) { return order_by[i].ascending;
}
          if (b.IsNull()) { return !order_by[i].ascending;
}
          return order_by[i].ascending ? a < b : b < a;
        }
        return false;
      });
  for (KeyedRow& keyed : sortable) {
    output->AddRow(std::move(keyed.row));
  }
  output->FinishSpill();
  if (active_runtime != nullptr) { active_runtime->sort_ms += ElapsedMs(sort_begin);
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
  const size_t count = statement.Limit() == 0
                           ? available
                           : std::min(statement.Limit(), available);
  size_t index = 0;
  input.ForEachRow([&](const Row& row) {
    const size_t current = index++;
    if (current < begin || current >= begin + count) { return;
}
    limited.AddRow(row);
  });
  limited.FinishSpill();
  return limited;
}

}  // namespace

Relation FinishQuery(TransactionContext& context,
                     const SelectStatement& statement, Relation input,
                     const Scope* outer, const CteMap& ctes,
                     bool apply_where) {
  if (apply_where && statement.WhereClause()) {
    const auto filter_begin = std::chrono::steady_clock::now();
    Relation filtered;
    filtered.schema = input.schema;
    CopyExecutionStats(&filtered, input);
    input.FinishSpill();
    input.ForEachRow([&](const Row& row) {
      Scope scope{.row=&row, .schema=&input.schema, .outer=outer};
      if (Truthy(Evaluate(statement.WhereClause(), scope, nullptr, context,
                          ctes))) {
        filtered.AddRow(row);
      }
    });
    filtered.FinishSpill();
    input = std::move(filtered);
    if (active_runtime != nullptr) { active_runtime->filter_ms += ElapsedMs(filter_begin);
}
  }

  Relation output = Project(context, statement, std::move(input), outer, ctes);
  if (statement.Distinct()) {
    output = DistinctOf(std::move(output));
  }
  if (!statement.OrderBy().empty()) {
    ApplyOrderBy(context, statement, &output, outer, ctes);
  }
  return LimitedRows(statement, std::move(output));
}
Relation ExecuteQuery(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& inherited_ctes) {
  EnsureReusableProjections(context, active_runtime);
  CteMap ctes = inherited_ctes;
  for (const auto& [name, query] : statement.WithQueries()) {
    ctes[name] = std::make_shared<Relation>(
        ExecuteQuery(context, *query, outer, ctes));
  }

  // Single-table aggregation: filter and aggregate while scanning so we never
  // materialize millions of qualifying rows (TPC-H Q1/Q6/Q21-derived pattern).
  // When the table is reusable, populate the shared cache once then aggregate
  // from the cache without deep-copying into an intermediate relation.
  const bool stream_agg =
      outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
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
        active_runtime != nullptr &&
        active_runtime->reusable_base_relations.contains(source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      throw std::runtime_error("table " + source.table + " not found");
    }
    const Schema& table_schema = table.Value()->GetSchema();
    std::vector<slot_t> projection = RequiredColumns(statement, table_schema);
    if (const std::vector<slot_t>* shared = ReusableProjection(source.table)) {
      projection = *shared;
    }
    Schema scan_schema = projection.empty()
                             ? Schema("", std::vector<Column>{})
                             : ProjectSchema(table_schema, projection);
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    Schema qualified_schema =
        qualifier.empty() ? scan_schema
                          : QualifySchema(scan_schema, qualifier);

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

    std::vector<std::optional<slot_t>> group_offsets;
    group_offsets.reserve(statement.GroupBy().size());
    bool group_keys_are_columns = true;
    for (const Expression& key : statement.GroupBy()) {
      if (key->Type() != TypeTag::kColumnValue) {
        group_keys_are_columns = false;
        group_offsets.emplace_back(std::nullopt);
        continue;
      }
      group_offsets.emplace_back(LocalColumnOffset(
          input.schema, key->AsColumnValue().GetColumnName()));
      if (!group_offsets.back()) { group_keys_are_columns = false;
}
    }
    std::vector<std::optional<slot_t>> aggregate_child_offsets;
    aggregate_child_offsets.reserve(aggregate_expressions.size());
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      if (IsCountStar(*aggregate)) {
        aggregate_child_offsets.emplace_back(std::nullopt);  // sentinel via count*
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
      if (active_runtime != nullptr) { ++active_runtime->scan_output_rows;
}
      Scope scope{.row=&row, .schema=&input.schema, .outer=outer};
      std::vector<Value> key_values;
      key_values.reserve(statement.GroupBy().size());
      if (group_keys_are_columns) {
        for (const auto& offset : group_offsets) {
          key_values.push_back(row[*offset]);
        }
      } else {
        for (const Expression& key : statement.GroupBy()) {
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
        if (is_count_star[i]) {
          accumulator.Add(Value(1));
        } else if (aggregate_child_offsets[i]) {
          accumulator.Add(row[*aggregate_child_offsets[i]]);
        } else {
          accumulator.Add(Evaluate(aggregate_expressions[i]->Child(), scope,
                                   nullptr, context, ctes));
        }
        if (active_runtime != nullptr) { ++active_runtime->aggregate_updates;
}
      }
      if (active_runtime != nullptr) { ++active_runtime->aggregate_input_rows;
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
      auto cached = active_runtime->base_relations.find(cache_key);
      if (cached == active_runtime->base_relations.end()) {
        Relation cache_rel;
        cache_rel.schema = scan_schema;
        // Fill the shared cache with UNFILTERED rows: the cache key carries no
        // predicates (and no stashed key filters are applied here), so every
        // consumer applies its own filters while reading below.
        Iterator iterator =
            projection.empty()
                ? table.Value()->BeginFullScan(context.txn_)
                : table.Value()->BeginFullScan(context.txn_, projection);
        while (iterator.IsValid()) {
          if (active_runtime != nullptr) {
            ++active_runtime->scan_rows;
            active_runtime->scan_values_available += table_schema.ColumnCount();
            active_runtime->scan_values_decoded += scan_schema.ColumnCount();
          }
          cache_rel.AddRow(*iterator);
          ++iterator;
        }
        cache_rel.FinishSpill();
        cached =
            active_runtime->base_relations
                .emplace(cache_key, std::make_shared<Relation>(std::move(cache_rel)))
                .first;
      } else {
        ++active_runtime->base_scan_cache_hits;
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
        if (!MatchScanFilter(*iterator, scan_schema, scan_filter, outer, context,
                             ctes)) {
          ++iterator;
          continue;
        }
        if (active_runtime != nullptr) {
          ++active_runtime->scan_rows;
          active_runtime->scan_values_available += table_schema.ColumnCount();
          active_runtime->scan_values_decoded += scan_schema.ColumnCount();
        }
        accumulate_row(*iterator);
        ++iterator;
      }
    }
    if (active_runtime != nullptr) {
      active_runtime->scan_ms += ElapsedMs(scan_begin);
      // filter_ms intentionally includes the same interval: filtering happened
      // inline with the scan, so both counters cover this phase.
      active_runtime->filter_ms += ElapsedMs(scan_begin);
      active_runtime->aggregate_groups += groups.size();
    }
    if (groups.empty() && statement.GroupBy().empty()) {
      GroupState group;
      group.accumulator_offset = aggregate_states.size();
      for (const AggregateExpression* aggregate : aggregate_expressions) {
        aggregate_states.emplace_back(aggregate);
      }
      groups.push_back(std::move(group));
    }

    Relation output;
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
      Scope scope{.row=&group.representative, .schema=&input.schema, .outer=outer};
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

  return FinishQuery(context, statement, std::move(input), outer, ctes,
                     !where_fully_applied);
}


}  // namespace tinylamb::relational_detail

namespace tinylamb {

using relational_detail::ExecuteQuery;
using relational_detail::CountStatementTables;
using relational_detail::ExecutionRuntime;
using relational_detail::WriteEstimatedPhysicalPlan;
using relational_detail::active_runtime;
using relational_detail::Relation;

RelationalExecutor::RelationalExecutor(
    TransactionContext& context,
    std::shared_ptr<const SelectStatement> statement)
    : context_(&context), statement_(std::move(statement)) {}

void RelationalExecutor::Initialize() {
  if (initialized_) { return;
}
  ExecutionRuntime runtime;
  runtime.root_statement = statement_.get();
  std::unordered_map<std::string, size_t> table_counts;
  CountStatementTables(*statement_, &table_counts);
  for (const auto& [table, count] : table_counts) {
    if (count > 1) { runtime.reusable_base_relations.insert(table);
}
  }
  ExecutionRuntime* previous_runtime = active_runtime;
  active_runtime = &runtime;
  Relation result;
  try {
    result = ExecuteQuery(*context_, *statement_, nullptr, {});
  } catch (...) {
    active_runtime = previous_runtime;
    throw;
  }
  active_runtime = previous_runtime;
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
  relation_spills_ = runtime.relation_spills;
  initialized_ = true;
}

bool RelationalExecutor::Next(Row* destination, RowPosition* position) {
  Initialize();
  if (offset_ >= rows_.size()) { return false;
}
  *destination = rows_[offset_++];
  if (position != nullptr) { *position = RowPosition();
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
         << ", scan_values_available=" << scan_values_available_ << ")";
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
