/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
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
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Recursively gathers every column referenced by an expression, descending
// into subqueries so UNNEST arguments like ARRAY(SELECT ... a ...) report
// their correlated references.
namespace {

void CollectColumnsRecursive(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::unordered_set<ColumnName>* columns) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    CollectStatementColumns(*query.Query(), columns);
    CollectColumnsRecursive(query.Test(), columns);
    return;
  }
  std::unordered_set<ColumnName> touched = expression->TouchedColumns();
  columns->merge(touched);
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectColumnsRecursive(child, columns);
  }
}

}  // namespace

// Expands a lateral (correlated) UNNEST source against the relation
// accumulated from the preceding FROM items: for every prefix row the array
// expression is evaluated with that row in scope and each element becomes an
// output row joined with the prefix row.
// GCC 15 emits a spurious -Wfree-nonheap-object inside the inlined
// std::vector deallocation here even though every container lifetime is
// standard; suppress it just for this function.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
namespace {

StatusOr<Relation> LateralExpandRelation(TransactionContext& context,
                                         const SelectSource& source,
                                         Relation& prefix, const Scope* outer,
                                         const CteMap& ctes) {
  Relation output(context.execution_runtime());
  const std::string qualifier = source.alias.empty() ? "unnest" : source.alias;
  auto element_schema_of = [&](const Value& array_val) {
    Relation elements = UnnestValueToRelation(source, array_val);
    if (!qualifier.empty()) {
      elements.schema = QualifySchema(elements.schema, qualifier);
    }
    return elements;
  };
  const Expression condition =
      source.join_condition ? source.join_condition : nullptr;
  // USING(name) equalities bind positionally: both operands share one bare
  // name, so combined-scope evaluation would be ambiguous (and after
  // coalescing, trivially true).  Each such conjunct resolves to a
  // prefix-side and an element-side column up front and compares them
  // directly; remaining conjuncts keep combined-scope evaluation.
  struct UsingEquality {
    size_t prefix_column;
    size_t element_column;
  };
  std::vector<UsingEquality> using_equalities;
  std::vector<Expression> residual_condition =
      condition ? SplitConjuncts(condition) : std::vector<Expression>{};
  if (!source.using_columns.empty() && condition) {
    auto declared = [&](const ColumnName& name) {
      if (!name.schema.empty()) {
        return false;
      }
      return std::ranges::any_of(
          source.using_columns, [&](const std::string& candidate) {
            return candidate.size() == name.name.size() &&
                   std::equal(
                       candidate.begin(), candidate.end(), name.name.begin(),
                       [](char lhs, char rhs) {
                         return std::tolower(static_cast<unsigned char>(lhs)) ==
                                std::tolower(static_cast<unsigned char>(rhs));
                       });
          });
    };
    // The element relation's column names are stable across expansions:
    // [alias] plus the offset alias when declared.
    std::vector<Column> probe{Column(
        source.alias.empty() ? "unnest" : source.alias, ValueType::kNull)};
    if (!source.offset_alias.empty()) {
      probe.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    const Schema element_names("", std::move(probe));
    for (size_t conjunct_index = 0;
         conjunct_index < residual_condition.size();) {
      const Expression& conjunct = residual_condition[conjunct_index];
      bool handled = false;
      if (IsColumnEqualityPredicate(conjunct)) {
        const BinaryExpression& binary = conjunct->AsBinaryExpression();
        const ColumnName& ln = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rn = binary.Right()->AsColumnValue().GetColumnName();
        if (declared(ln) && declared(rn)) {
          // Hoisted out of the range-for: iterating a temporary vector here
          // trips a spurious -Wfree-nonheap-object diagnostic in GCC 15.
          const std::vector<std::pair<ColumnName, ColumnName>> both_orders{
              {ln, rn}, {rn, ln}};
          for (const auto& [outer_name, inner_name] : both_orders) {
            const auto prefix_offset =
                LocalColumnOffset(prefix.schema, outer_name);
            const auto element_offset =
                LocalColumnOffset(element_names, inner_name);
            if (prefix_offset && element_offset) {
              using_equalities.push_back({*prefix_offset, *element_offset});
              handled = true;
              break;
            }
          }
        }
      }
      if (handled) {
        residual_condition.erase(residual_condition.begin() +
                                 static_cast<ptrdiff_t>(conjunct_index));
      } else {
        ++conjunct_index;
      }
    }
  }
  const bool want_left_nulls = source.join_type == JoinType::kLeft ||
                               source.join_type == JoinType::kFull;
  const bool want_right_nulls = source.join_type == JoinType::kRight ||
                                source.join_type == JoinType::kFull;
  if (!source.using_columns.empty()) {
    output.using_columns =
        std::make_shared<const std::vector<std::string>>(source.using_columns);
  }
  const bool left_outer = want_left_nulls;
  bool any_prefix_row = false;
  bool schema_initialized = false;
  prefix.FinishSpill();
  Status elements_status = Status::kSuccess;
  prefix.ForEachRow([&](const Row& row) -> void {
    any_prefix_row = true;
    Scope scope{.row = &row, .schema = &prefix.schema, .outer = outer};
    Relation elements(context.execution_runtime());
    if (source.unnest) {
      StatusOr<Value> array_or =
          TryEvaluate(source.unnest, scope, nullptr, context, ctes);
      if (!array_or.HasValue()) {
        if (elements_status == Status::kSuccess) {
          elements_status = array_or.GetStatus();
        }
        return;
      }
      const Value array_val = array_or.MoveValue();
      elements = element_schema_of(array_val);
    } else if (source.query) {
      StatusOr<Relation> elements_query =
          ExecuteQuery(context, *source.query, &scope, ctes);
      if (!elements_query.HasValue()) {
        elements_status = elements_query.GetStatus();
        return;
      }
      elements = elements_query.MoveValue();
      if (!source.alias.empty()) {
        elements.schema = QualifySchema(elements.schema, source.alias);
      }
    }
    if (!schema_initialized) {
      output.schema = prefix.schema + elements.schema;
      schema_initialized = true;
    }
    // The element relation's shape is derived from the array value at hand,
    // so a later prefix row can in principle expand to fewer columns than
    // the row that initialized output.schema.  NULL-fill (or trim) back to
    // the initialized arity so every emitted row matches output.schema
    // exactly; Lookup and projection index rows by schema offsets.
    elements.FinishSpill();
    const size_t prefix_width = row.values_.size();
    std::vector<char> element_matched(
        want_right_nulls ? elements.TotalRows() : 0, 0);
    size_t element_index = 0;
    bool matched = false;
    elements.ForEachRow([&](const Row& element_row) {
      const size_t this_element = element_index++;
      Row combined = row + element_row;
      combined.values_.resize(output.schema.ColumnCount());
      if (condition) {
        Scope combined_scope{
            .row = &combined, .schema = &output.schema, .outer = outer};
        if (!source.using_columns.empty()) {
          combined_scope.using_columns = &source.using_columns;
        }
        bool all_match = true;
        for (const UsingEquality& key : using_equalities) {
          const Value& left_value = combined.values_[key.prefix_column];
          const Value& right_value =
              combined.values_[prefix_width + key.element_column];
          if (!Truthy(EvaluateBinary(BinaryOperation::kEquals, left_value,
                                     right_value))) {
            all_match = false;
            break;
          }
        }
        if (all_match) {
          for (const Expression& conjunct : residual_condition) {
            StatusOr<Value> conjunct_value =
                TryEvaluate(conjunct, combined_scope, nullptr, context, ctes);
            if (!conjunct_value.HasValue()) {
              if (elements_status == Status::kSuccess) {
                elements_status = conjunct_value.GetStatus();
              }
              return;
            }
            if (!Truthy(conjunct_value.Value())) {
              all_match = false;
              break;
            }
          }
        }
        if (!all_match) {
          return;
        }
      }
      if (want_right_nulls && this_element < element_matched.size()) {
        element_matched[this_element] = 1;
      }
      matched = true;
      output.AddRow(std::move(combined));
    });
    if (!matched && left_outer &&
        (condition != nullptr || elements.TotalRows() == 0)) {
      std::vector<Value> nulls(output.schema.ColumnCount() - row.values_.size(),
                               Value());
      output.AddRow(ConcatRows(row, Row(std::move(nulls))));
    }
    // RIGHT / FULL OUTER: within this prefix row's expansion, element rows
    // that matched nothing emit once, null-extended on the prefix side.
    if (want_right_nulls && !element_matched.empty()) {
      size_t replay_index = 0;
      elements.ForEachRow([&](const Row& element_row) {
        const size_t index = replay_index++;
        if (index < element_matched.size() && element_matched[index] == 0) {
          std::vector<Value> nulls(prefix.schema.ColumnCount(), Value());
          output.AddRow(Row(std::move(nulls)) + element_row);
        }
      });
    }
  });
  if (!any_prefix_row || !schema_initialized) {
    // Empty prefix (or every prefix row skipped before initialization):
    // still derive the output schema so downstream projection sees the
    // unnest columns; evaluating on an all-NULL representative row keeps
    // this exception-free for constant arrays.
    if (!schema_initialized) {
      Row representative(
          std::vector<Value>(prefix.schema.ColumnCount(), Value()));
      Scope scope{
          .row = &representative, .schema = &prefix.schema, .outer = outer};
      Relation elements(context.execution_runtime());
      if (source.unnest) {
        StatusOr<Value> array_or =
            TryEvaluate(source.unnest, scope, nullptr, context, ctes);
        if (!array_or.HasValue()) {
          return array_or.GetStatus();
        }
        const Value array_val = array_or.MoveValue();
        elements = element_schema_of(array_val);
      } else if (source.query) {
        StatusOr<Relation> elements_query =
            ExecuteQuery(context, *source.query, &scope, ctes);
        if (!elements_query.HasValue()) {
          return elements_query.GetStatus();
        }
        elements = elements_query.MoveValue();
        if (!source.alias.empty()) {
          elements.schema = QualifySchema(elements.schema, source.alias);
        }
      }
      output.schema = prefix.schema + elements.schema;
      schema_initialized = true;
    }
  }
  output.FinishSpill();
  if (elements_status != Status::kSuccess) {
    return elements_status;
  }
  return output;
}
}  // namespace
#pragma GCC diagnostic pop

StatusOr<Relation> BuildInput(TransactionContext& context,
                              const SelectStatement& statement,
                              const Scope* outer, const CteMap& ctes,
                              bool* where_fully_applied,
                              GroupedJoinFusionSpec* fusion) {
  *where_fully_applied = false;
  if (statement.Sources().empty()) {
    Relation singleton;
    singleton.rows.emplace_back();
    singleton.peak_intermediate_rows = 1;
    return singleton;
  }

  // Lateral UNNEST / LATERAL subquery: a FROM item whose UNNEST or LATERAL
  // subquery argument references columns of sibling FROM items or enclosing
  // scopes cannot be materialized in isolation; it expands per-row against
  // the relations accumulated so far.
  {
    bool lateral = false;
    for (const SelectSource& source : statement.Sources()) {
      if (source.is_lateral) {
        lateral = true;
        break;
      }
      if (source.unnest) {
        std::unordered_set<ColumnName> touched;
        CollectColumnsRecursive(source.unnest, &touched);
        if (!touched.empty()) {
          lateral = true;
          break;
        }
      }
    }
    if (lateral) {
      Relation result(context.execution_runtime());
      bool first = true;
      for (const SelectSource& source : statement.Sources()) {
        Relation relation(context.execution_runtime());
        if (source.unnest || source.is_lateral) {
          // Lateral expansion already merges the prefix with each element
          // row (applying the source's join condition); no separate join.
          if (first) {
            Relation singleton(context.execution_runtime());
            singleton.schema = Schema("", std::vector<Column>{});
            singleton.rows.emplace_back();
            singleton.peak_intermediate_rows = 1;
            StatusOr<Relation> expanded =
                LateralExpandRelation(context, source, singleton, outer, ctes);
            if (!expanded.HasValue()) {
              return expanded.GetStatus();
            }
            relation = expanded.MoveValue();
          } else {
            StatusOr<Relation> expanded =
                LateralExpandRelation(context, source, result, outer, ctes);
            if (!expanded.HasValue()) {
              return expanded.GetStatus();
            }
            relation = expanded.MoveValue();
          }
          result = std::move(relation);
          first = false;
        } else {
          ASSIGN_OR_RETURN(Relation, relation_r,
                           (LoadSource(context, source, outer, ctes)));
          relation = std::move(relation_r);
          if (first) {
            result = std::move(relation);
            first = false;
          } else {
            ASSIGN_OR_RETURN(Relation, result_r,
                             (Join(context, std::move(result),
                                   std::move(relation), source, outer, ctes)));
            result = std::move(result_r);
          }
        }
      }
      return result;
    }
  }

  std::vector<Relation> relations(statement.Sources().size());
  std::vector<bool> base_sources(statement.Sources().size(), false);
  std::vector<bool> deferred_derived(statement.Sources().size(), false);
  std::vector<slot_t> deferred_inner_key_col(statement.Sources().size(), 0);
  std::vector<std::vector<slot_t>> projections(statement.Sources().size());
  const bool pure_inner_join =
      statement.Sources().size() >= 2 &&
      std::ranges::all_of(statement.Sources(), [](const SelectSource& s) {
        return !s.from_nested_join && !s.unnest && !s.is_lateral &&
               s.using_columns.empty() &&
               (s.join_type == JoinType::kInner ||
                s.join_type == JoinType::kCross);
      });
  for (size_t i = 0; i < statement.Sources().size(); ++i) {
    const SelectSource& source = statement.Sources()[i];
    base_sources[i] =
        !source.query && !source.unnest && !ctes.contains(source.table);
    if (!base_sources[i]) {
      const SelectStatement* derived = source.query.get();
      if (pure_inner_join && derived != nullptr && !source.sample &&
          derived->WithQueries().empty() && derived->UnionAll().empty() &&
          derived->GetSetOperationTree() == nullptr && !derived->HasLimit() &&
          derived->Offset() == 0 && !derived->Distinct() &&
          !derived->IsGroupAll() && derived->GroupBy().size() == 1 &&
          (!derived->Having() ||
           (derived->Having()->Type() != TypeTag::kConstantValue &&
            !ContainsQuery(derived->Having()))) &&
          std::ranges::any_of(derived->SelectList(),
                              [](const NamedExpression& item) {
                                return item.expression &&
                                       ContainsAggregate(item.expression);
                              }) &&
          !derived->Qualify() && derived->Sources().size() == 1) {
        const SelectSource& inner_src = derived->Sources()[0];
        if (!inner_src.query && !inner_src.unnest && !inner_src.sample &&
            !ctes.contains(inner_src.table) &&
            derived->GroupBy()[0]->Type() == TypeTag::kColumnValue) {
          StatusOr<std::shared_ptr<Table>> inner_table =
              context.GetTable(inner_src.table);
          if (inner_table.HasValue()) {
            const Schema& inner_table_schema = inner_table.Value()->GetSchema();
            const std::string inner_qual =
                inner_src.alias.empty() ? inner_src.table : inner_src.alias;
            const Schema inner_qual_schema =
                inner_qual.empty()
                    ? inner_table_schema
                    : QualifySchema(inner_table_schema, inner_qual);
            const auto inner_key_off = LocalColumnOffset(
                inner_qual_schema,
                derived->GroupBy()[0]->AsColumnValue().GetColumnName());
            if (inner_key_off.has_value() &&
                SingleIntegerJoinKey(inner_table_schema,
                                     {static_cast<slot_t>(*inner_key_off)})) {
              std::vector<Column> derived_cols;
              derived_cols.reserve(derived->SelectList().size());
              std::optional<slot_t> group_out_slot;
              bool schema_ok = !derived->SelectList().empty();
              for (size_t s = 0; s < derived->SelectList().size(); ++s) {
                const Expression& expr = derived->SelectList()[s].expression;
                if (!expr) {
                  schema_ok = false;
                  break;
                }
                const std::string col_name =
                    ProjectionName(derived->SelectList()[s], s);
                if (expr->Type() == TypeTag::kColumnValue) {
                  const auto col_off = LocalColumnOffset(
                      inner_qual_schema, expr->AsColumnValue().GetColumnName());
                  if (!col_off.has_value() || *col_off != *inner_key_off) {
                    schema_ok = false;
                    break;
                  }
                  const Column& base_col =
                      inner_table_schema.GetColumn(*col_off);
                  Column out_col(col_name, base_col.Type());
                  out_col.SetUnsigned(base_col.IsUnsigned());
                  derived_cols.push_back(std::move(out_col));
                  group_out_slot = static_cast<slot_t>(s);
                } else if (expr->Type() == TypeTag::kAggregateExp &&
                           expr->AsAggregateExpression().GetType() ==
                               AggregationType::kCount) {
                  derived_cols.emplace_back(col_name, ValueType::kInt64);
                } else {
                  schema_ok = false;
                  break;
                }
              }
              if (schema_ok && group_out_slot.has_value()) {
                Schema derived_schema("", std::move(derived_cols));
                if (!source.alias.empty()) {
                  derived_schema = QualifySchema(derived_schema, source.alias);
                }
                relations[i].schema = std::move(derived_schema);
                projections[i] = {*group_out_slot};
                deferred_derived[i] = true;
                deferred_inner_key_col[i] = static_cast<slot_t>(*inner_key_off);
                continue;
              }
            }
          }
        }
      }
      ASSIGN_OR_RETURN(Relation, loaded_rel,
                       (LoadSource(context, source, outer, ctes)));
      relations[i] = std::move(loaded_rel);
      // A non-aggregate derived SELECT with a literal FALSE filter is known
      // to be empty independently of its source table.  Preserve the derived
      // schema but discard the rows before the outer join-order loop, so an
      // inner join can avoid opening its other side.
      if (derived != nullptr && derived->WhereClause() &&
          derived->WhereClause()->Type() == TypeTag::kConstantValue &&
          !derived->WhereClause()->AsConstantValue().GetValue().Truthy() &&
          derived->GroupBy().empty() && !derived->Distinct() &&
          !std::ranges::any_of(
              derived->SelectList(), [](const NamedExpression& item) {
                return item.expression && ContainsAggregate(item.expression);
              })) {
        relations[i].ResetContents();
      }
      continue;
    }

    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "table " + source.table + " not found");
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    Schema base_schema = table.Value()->GetSchema();
    if (!context.txn_.HasAnyPageVersionChains()) {
      std::shared_ptr<const LSTableStore> lst_store =
          GetOrBuildLSTableStore(context.txn_, *table.Value(), true);
      if (lst_store != nullptr && lst_store->uncommitted_owner_txn == 0 &&
          lst_store->layout_generation == table.Value()->LayoutGeneration() &&
          lst_store->first_pid == table.Value()->FirstPageId()) {
        bool any_promoted = false;
        std::vector<Column> cols;
        cols.reserve(base_schema.ColumnCount());
        for (size_t ci = 0; ci < base_schema.ColumnCount(); ++ci) {
          Column col = base_schema.GetColumn(ci);
          if (col.GetConstraint().ctype == Constraint::kNothing &&
              ci < lst_store->column_is_unique_nonnull.size() &&
              lst_store->column_is_unique_nonnull[ci] != 0) {
            col = Column(col.Name(), col.Type(),
                         Constraint(Constraint::kPrimaryKey));
            any_promoted = true;
          }
          cols.push_back(std::move(col));
        }
        if (any_promoted) {
          base_schema = Schema(base_schema.Name(), std::move(cols));
        }
      }
    }
    relations[i].schema = qualifier.empty()
                              ? std::move(base_schema)
                              : QualifySchema(base_schema, qualifier);
    projections[i] = RequiredColumns(statement, relations[i].schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(context, source.table)) {
      // The shared layout was derived from the raw table schema, so
      // alias-qualified references (T1.col) resolved against this source's
      // qualified schema may be missing from it; projecting fewer columns
      // than this source needs breaks evaluation downstream.
      std::vector<slot_t> merged(projections[i]);
      for (const slot_t slot : *shared) {
        if (std::ranges::find(merged, slot) == merged.end()) {
          merged.push_back(slot);
        }
      }
      std::ranges::sort(merged);
      projections[i] = std::move(merged);
    }
  }

  // Multi-alias tables share one projected cache entry: take the union of
  // columns required by every reference so each alias can reuse the scan.
  {
    std::unordered_map<std::string, std::set<slot_t>> column_union;
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] ||
          !ReusesBaseRelation(context, statement.Sources()[i])) {
        continue;
      }
      auto& columns = column_union[statement.Sources()[i].table];
      for (slot_t column : projections[i]) {
        columns.insert(column);
      }
    }
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] ||
          !ReusesBaseRelation(context, statement.Sources()[i])) {
        continue;
      }
      const auto& columns = column_union[statement.Sources()[i].table];
      projections[i].assign(columns.begin(), columns.end());
    }
  }

  if (relations.size() == 2 && base_sources[0] && base_sources[1]) {
    ASSIGN_OR_RETURN(std::optional<Relation>, streamed,
                     TryStreamGroupedBaseJoin(context, statement, outer, ctes,
                                              projections, relations, fusion));
    if (streamed) {
      *where_fully_applied = true;
      return std::move(*streamed);
    }
  }

  bool all_outer_reducible = false;
  const std::vector<SelectSource> reduced_sources =
      ReduceOuterJoinsToInner(statement, relations, &all_outer_reducible);
  if (all_outer_reducible) {
    SelectStatement reduced_statement(statement);
    reduced_statement.SetSources(reduced_sources);
    return BuildInput(context, reduced_statement, outer, ctes,
                      where_fully_applied, fusion);
  }

  // OUTER JOIN predicates and USING merges have order-dependent semantics,
  // so retain their syntactic order. The optimizer below is valid for
  // inner/cross joins.
  const bool has_nested_join = std::any_of(
      statement.Sources().begin(), statement.Sources().end(),
      [](const SelectSource& source) { return source.from_nested_join; });
  const bool has_ordered_join = std::any_of(
      statement.Sources().begin() + 1, statement.Sources().end(),
      [](const SelectSource& source) {
        return source.join_type == JoinType::kLeft ||
               source.join_type == JoinType::kRight ||
               source.join_type == JoinType::kFull || source.from_nested_join ||
               source.unnest || !source.using_columns.empty();
      });
  if (has_nested_join) {
    // A parenthesized right-hand join is flattened by the frontend so that
    // column binding remains positional.  Its first source carries the
    // predicate of the outer join, while subsequent sources carry the
    // predicates inside the parentheses.  Reconstruct that right-deep tree
    // here before joining it to the prefix.
    size_t nested_begin = 0;
    while (nested_begin < statement.Sources().size() &&
           !statement.Sources()[nested_begin].from_nested_join) {
      ++nested_begin;
    }
    for (size_t i = 0; i < relations.size(); ++i) {
      if (base_sources[i]) {
        ASSIGN_OR_RETURN(Relation, loaded_rel,
                         (LoadSource(context, statement.Sources()[i], outer,
                                     ctes, &projections[i])));
        relations[i] = std::move(loaded_rel);
      }
    }
    Relation prefix = std::move(relations.front());
    for (size_t i = 1; i < nested_begin; ++i) {
      ASSIGN_OR_RETURN(
          Relation, prefix_r,
          (Join(context, std::move(prefix), std::move(relations[i]),
                statement.Sources()[i], outer, ctes)));
      prefix = std::move(prefix_r);
    }
    Relation nested = std::move(relations[nested_begin]);
    for (size_t i = nested_begin + 1; i < relations.size(); ++i) {
      ASSIGN_OR_RETURN(
          Relation, nested_r,
          (Join(context, std::move(nested), std::move(relations[i]),
                statement.Sources()[i], outer, ctes)));
      nested = std::move(nested_r);
    }
    return Join(context, std::move(prefix), std::move(nested),
                statement.Sources()[nested_begin], outer, ctes);
  }
  if (has_ordered_join) {
    // The ordered path keeps the syntactic join shape, but WHERE conjuncts
    // are still placed as early as they become referenceable, mirroring the
    // inner-join optimizer below: (a) single-source conjuncts filter the
    // owning scan, and (b) conjuncts whose last referenced table is a comma
    // (cross) source become that join's condition, turning what would be an
    // unbounded cross product into an equi/residual join.  Placement only
    // moves a conjunct to the first step where every source it references is
    // already joined -- never below the outer join that supplies one of its
    // columns -- and the full WHERE still runs above the joined output, so
    // outer-join semantics are unchanged.  Volatile or query-bearing
    // conjuncts are left for the top-level filter, since applying them twice
    // must not change how often they evaluate.
    // TINYLAMB_DISABLE_JOIN_REORDER=1 restores the strict syntactic visit
    // order (A/B hatch; results must be identical either way).
    static const bool disable_join_reorder =
        std::getenv("TINYLAMB_DISABLE_JOIN_REORDER") != nullptr;
    std::vector<PredicateInfo> ordered_predicates =
        AnalyzePredicates(statement.WhereClause(), relations);
    // Disjunctions that imply the same equi-join pair on every branch gain
    // synthetic join edges so the greedy reorder and the hash keys see the
    // same connectivity the Cascades memo infers (TPC-DS q13); the original
    // disjunction is untouched and keeps re-verifying matched pairs.
    std::vector<PredicateInfo> disjunct_edges =
        SynthesizeDisjunctEqualityPredicates(ordered_predicates, relations);
    ordered_predicates.insert(ordered_predicates.end(),
                              std::make_move_iterator(disjunct_edges.begin()),
                              std::make_move_iterator(disjunct_edges.end()));
    static const bool debug_ordered =
        std::getenv("TINYLAMB_DEBUG_ORDERED") != nullptr;
    if (debug_ordered) {
      for (size_t p = 0; p < ordered_predicates.size(); ++p) {
        const PredicateInfo& predicate = ordered_predicates[p];
        std::cerr << "[ordered] p" << p << " resolved=" << predicate.resolved
                  << " query=" << predicate.contains_query << " sources=[";
        for (size_t s : predicate.sources) {
          std::cerr << s << ',';
        }
        std::cerr << "] expr=" << predicate.expression->ToString() << '\n';
      }
    }
    std::vector<bool> predicate_placed(ordered_predicates.size(), false);
    const auto placeable = [&](const PredicateInfo& predicate) {
      return predicate.resolved && !predicate.contains_query &&
             SafeToReduceEvaluationCount(predicate.expression);
    };
    // Single-source conjuncts may only pre-filter a PRESERVED side.
    // Pre-filtering a null-supplying side converts some matched rows into
    // NULL-padded rows, which changes the outcome of WHERE terms that
    // distinguish padded rows -- `WHERE b.id IS NULL` over
    // `a LEFT JOIN b` (outer_to_anti) would otherwise resurrect the rows
    // whose match was filtered away.  A source is null-supplying when it is
    // the right side of a LEFT/FULL join; a RIGHT/FULL join additionally
    // makes every earlier source null-supplying.
    std::vector<char> filter_safe(relations.size(), 1);
    for (size_t i = 1; i < statement.Sources().size(); ++i) {
      const JoinType type = statement.Sources()[i].join_type;
      if (type == JoinType::kLeft || type == JoinType::kFull) {
        filter_safe[i] = 0;
      }
      if (type == JoinType::kRight || type == JoinType::kFull) {
        for (size_t j = 0; j < i; ++j) {
          filter_safe[j] = 0;
        }
      }
    }
    std::vector<std::vector<Expression>> ordered_local(relations.size());
    for (size_t p = 0; p < ordered_predicates.size(); ++p) {
      const PredicateInfo& predicate = ordered_predicates[p];
      if (!placeable(predicate) || predicate.sources.size() != 1) {
        continue;
      }
      const size_t owner = *predicate.sources.begin();
      if (!static_cast<bool>(filter_safe[owner])) {
        continue;
      }
      ordered_local[owner].push_back(predicate.expression);
      predicate_placed[p] = true;
    }
    // An ON term local to the null-supplying side of LEFT JOIN can reject
    // build rows before materialization. Keep the original ON term as the
    // semantic backstop; never use a WHERE term here or cross a FULL join.
    for (size_t i = 1; i < relations.size(); ++i) {
      if (statement.Sources()[i].join_type != JoinType::kLeft) {
        continue;
      }
      for (const Expression& predicate :
           SplitConjuncts(statement.Sources()[i].join_condition)) {
        if (IsNonThrowingScanComparison(predicate, relations[i].schema)) {
          ordered_local[i].push_back(predicate);
        }
      }
    }
    if (debug_ordered) {
      for (size_t i = 0; i < ordered_local.size(); ++i) {
        if (!ordered_local[i].empty()) {
          std::cerr << "[ordered] local[" << i
                    << "] count=" << ordered_local[i].size()
                    << " base=" << base_sources[i] << '\n';
        }
      }
    }
    for (size_t i = 0; i < relations.size(); ++i) {
      if (base_sources[i]) {
        ASSIGN_OR_RETURN(
            Relation, loaded_rel,
            (LoadSource(
                context, statement.Sources()[i], outer, ctes, &projections[i],
                ordered_local[i].empty() ? nullptr : &ordered_local[i])));
        relations[i] = std::move(loaded_rel);
      } else if (!ordered_local[i].empty()) {
        RETURN_IF_FAIL(FilterRelation(context, &relations[i], ordered_local[i],
                                      outer, ctes));
      }
    }
    // Maximal runs of inner/cross sources reorder greedily by connectivity:
    // inner joins are associative and commutative, so visiting them in the
    // syntactic chain order is not required.  Outer/USING/nested/lateral
    // sources keep their syntactic position (their null-extension and merge
    // semantics are order dependent) and freeze the run boundaries.  The
    // mirror heuristic drives the choice: prefer candidates that connect to
    // the already-joined prefix, smallest estimated intermediate size first,
    // so dimension chains join before the fact-to-fact joins that need
    // their filters (TPC-DS q72 joins the 2002 date slice before inventory).
    std::vector<size_t> visit_order;
    {
      const auto reorderable = [&](size_t i) {
        const SelectSource& s = statement.Sources()[i];
        return (s.join_type == JoinType::kInner ||
                s.join_type == JoinType::kCross) &&
               !s.from_nested_join && !s.unnest && s.using_columns.empty();
      };
      // Connective conjuncts: the ON conditions of run members plus every
      // unplaced multi-source WHERE conjunct, analyzed per source set.
      struct ConnectiveConjunct {
        std::unordered_set<size_t> sources;
      };
      std::vector<ConnectiveConjunct> connectives;
      const auto collect = [&](const Expression& conjunct) {
        if (!conjunct) {
          return;
        }
        const std::vector<PredicateInfo> analyzed =
            AnalyzePredicates(conjunct, relations);
        if (analyzed.size() == 1 && analyzed[0].resolved) {
          connectives.push_back({analyzed[0].sources});
        }
      };
      for (size_t i = 1; i < relations.size(); ++i) {
        if (reorderable(i)) {
          for (const Expression& conjunct :
               SplitConjuncts(statement.Sources()[i].join_condition)) {
            collect(conjunct);
          }
        }
      }
      for (size_t p = 0; p < ordered_predicates.size(); ++p) {
        const PredicateInfo& predicate = ordered_predicates[p];
        if (!predicate_placed[p] && placeable(predicate) &&
            predicate.sources.size() >= 2) {
          connectives.push_back({predicate.sources});
        }
      }
      const auto connected_to = [&](size_t candidate,
                                    const std::unordered_set<size_t>& joined) {
        // The candidate is not part of `joined` yet: a conjunct connects it
        // when every source it references is either already joined or the
        // candidate itself, and at least one referenced source is new.
        std::unordered_set<size_t> with_candidate = joined;
        with_candidate.insert(candidate);
        const auto connects_candidate =
            [&](const ConnectiveConjunct& conjunct) {
              return conjunct.sources.contains(candidate) &&
                     conjunct.sources.size() >= 2 &&
                     IsSubset(conjunct.sources, with_candidate) &&
                     !IsSubset(conjunct.sources, joined);
            };
        return std::ranges::any_of(connectives, connects_candidate);
      };
      // A source may only join when its own ON condition binds against the
      // prefix plus itself: joining it earlier would leave the ON
      // referencing a relation that is not an input of the join.  ON
      // conjuncts that never resolved (outer-scope qualifiers, subqueries)
      // keep their historic residual handling and do not block.
      std::vector<std::unordered_set<size_t>> on_sources(relations.size());
      for (size_t i = 1; i < relations.size(); ++i) {
        for (const Expression& conjunct :
             SplitConjuncts(statement.Sources()[i].join_condition)) {
          if (!conjunct) {
            continue;
          }
          const std::vector<PredicateInfo> analyzed =
              AnalyzePredicates(conjunct, relations);
          if (analyzed.size() == 1 && analyzed[0].resolved) {
            on_sources[i].insert(analyzed[0].sources.begin(),
                                 analyzed[0].sources.end());
          }
        }
      }
      const auto on_bindable = [&](size_t candidate,
                                   const std::unordered_set<size_t>& joined) {
        std::unordered_set<size_t> with_candidate = joined;
        with_candidate.insert(candidate);
        return IsSubset(on_sources[candidate], with_candidate);
      };
      size_t run_begin = 1;
      while (run_begin < relations.size()) {
        if (disable_join_reorder) {
          visit_order.push_back(run_begin);
          ++run_begin;
          continue;
        }
        if (!reorderable(run_begin)) {
          visit_order.push_back(run_begin);
          ++run_begin;
          continue;
        }
        size_t run_end = run_begin;
        while (run_end < relations.size() && reorderable(run_end)) {
          ++run_end;
        }
        // Start the chain from the prefix-connected member with the fewest
        // rows (the min() estimate cannot see hash fanout, so simulating
        // every start ties on peak; the smallest filtered member is the
        // robust anchor).  Then greedily append connected candidates.
        std::unordered_set<size_t> prefix;
        for (size_t i = 0; i < run_begin; ++i) {
          prefix.insert(i);
        }
        size_t start = run_begin;
        bool have_start = false;
        for (size_t candidate = run_begin; candidate < run_end; ++candidate) {
          if (!on_bindable(candidate, prefix)) {
            continue;
          }
          if (run_begin > 1 && !connected_to(candidate, prefix)) {
            continue;
          }
          const size_t rows = relations[candidate].TotalRows();
          if (!have_start || rows < relations[start].TotalRows()) {
            have_start = true;
            start = candidate;
          }
        }
        if (!have_start) {
          // No member binds against the prefix (a disconnected cross
          // fragment): fall back to the syntactic order for this run.
          for (size_t idx = run_begin; idx < run_end; ++idx) {
            visit_order.push_back(idx);
          }
          run_begin = run_end;
          continue;
        }
        std::vector<size_t> order{start};
        std::unordered_set<size_t> sim_joined = prefix;
        sim_joined.insert(start);
        std::vector<size_t> remaining;
        for (size_t i = run_begin; i < run_end; ++i) {
          if (i != start) {
            remaining.push_back(i);
          }
        }
        while (!remaining.empty()) {
          bool any_bindable = false;
          for (size_t candidate : remaining) {
            any_bindable = any_bindable || on_bindable(candidate, sim_joined);
          }
          if (!any_bindable) {
            // The remaining fragment's ON conditions reference only its own
            // members (a disconnected inner fragment).  Reordering cannot
            // help; preserve the syntactic order, which is the historic
            // shape for this case.
            for (const size_t idx : remaining) {
              order.push_back(idx);
            }
            break;
          }
          size_t next = remaining.front();
          bool next_bindable = on_bindable(next, sim_joined);
          bool next_connected = next_bindable && connected_to(next, sim_joined);
          for (size_t candidate : remaining) {
            const bool cand_bindable = on_bindable(candidate, sim_joined);
            const bool cand_connected =
                cand_bindable && connected_to(candidate, sim_joined);
            const bool cheaper =
                relations[candidate].TotalRows() < relations[next].TotalRows();
            if ((cand_connected && !next_connected) ||
                (cand_connected == next_connected && cand_bindable &&
                 !next_bindable) ||
                (cand_connected == next_connected &&
                 cand_bindable == next_bindable && cheaper)) {
              next = candidate;
              next_connected = cand_connected;
              next_bindable = cand_bindable;
            }
          }
          order.push_back(next);
          sim_joined.insert(next);
          std::erase(remaining, next);
        }
        if (debug_ordered) {
          std::cerr << "[ordered] run [" << run_begin << ',' << run_end
                    << ") start=" << start << " order=";
          for (const size_t idx : order) {
            std::cerr << idx << ',';
          }
          std::cerr << '\n';
        }
        for (const size_t idx : order) {
          visit_order.push_back(idx);
        }
        run_begin = run_end;
      }
    }
    Relation result = std::move(relations.front());
    std::unordered_set<size_t> available{0};
    for (const size_t i : visit_order) {
      available.insert(i);
      SelectSource source = statement.Sources()[i];
      // Outer-join ON conditions keep their syntactic predicate: folding a
      // WHERE conjunct into them would change matching into null padding.
      // Inner and cross joins admit it: sigma_p(A join_c B) equals
      // A join_(c AND p) B, and the join machinery turns the equality
      // conjuncts into hash keys while the residuals re-verify per pair.
      if (source.join_type == JoinType::kCross ||
          source.join_type == JoinType::kInner) {
        std::vector<Expression> applicable;
        for (size_t p = 0; p < ordered_predicates.size(); ++p) {
          const PredicateInfo& predicate = ordered_predicates[p];
          if (predicate_placed[p] || !placeable(predicate) ||
              predicate.sources.size() < 2 || !predicate.sources.contains(i) ||
              !IsSubset(predicate.sources, available)) {
            continue;
          }
          // Same rule as the single-source pre-filters above: a conjunct
          // over a source that a LATER RIGHT/FULL join NULL-supplies must
          // not filter below that join.  `a CROSS JOIN b RIGHT JOIN c ...
          // WHERE f(a, b) IS NULL` evaluated f before the RIGHT join, turned
          // c rows whose matches all failed f into NULL-padded rows, and the
          // padded rows then passed the IS NULL test.
          if (!std::ranges::all_of(predicate.sources, [&](size_t source_index) {
                return static_cast<bool>(filter_safe[source_index]);
              })) {
            continue;
          }
          applicable.push_back(predicate.expression);
          predicate_placed[p] = true;
        }
        if (!applicable.empty()) {
          if (source.join_condition) {
            applicable.push_back(source.join_condition);
          }
          source.join_condition = CombineConjuncts(applicable);
        }
      }
      // Final ordered-join step of a fusable grouped statement with an empty
      // WHERE: the join (including its ON residuals, evaluated per matched
      // pair) feeds the aggregation directly.  Q13's inner LEFT JOIN lives
      // here.  The fused path never materializes the join.
      if (fusion != nullptr && fusion->eligible && i + 1 == relations.size() &&
          !statement.WhereClause() &&
          (source.join_type == JoinType::kLeft ||
           source.join_type == JoinType::kCross ||
           source.join_type == JoinType::kInner)) {
        std::vector<Expression> fusion_predicates;
        if (source.join_condition) {
          for (const Expression& conjunct :
               SplitConjuncts(source.join_condition)) {
            if (source.join_type == JoinType::kLeft &&
                IsNonThrowingScanComparison(conjunct, relations[i].schema)) {
              const std::vector<PredicateInfo> analyzed =
                  AnalyzePredicates(conjunct, relations);
              if (analyzed.size() == 1 && analyzed[0].resolved &&
                  !analyzed[0].contains_query &&
                  analyzed[0].sources.size() == 1 &&
                  analyzed[0].sources.contains(i)) {
                continue;
              }
            }
            fusion_predicates.push_back(conjunct);
          }
        }
        const JoinType fused_type = source.join_type == JoinType::kLeft
                                        ? JoinType::kLeft
                                        : JoinType::kInner;
        ASSIGN_OR_RETURN(std::optional<Relation>, fused,
                         (TryFusedGroupedJoin(context, result, relations[i],
                                              fusion_predicates, fused_type,
                                              outer, ctes, fusion)));
        if (fused.has_value()) {
          result = std::move(*fused);
          fusion->fired = true;
          return result;
        }
      }
      ASSIGN_OR_RETURN(Relation, result_r,
                       (Join(context, std::move(result),
                             std::move(relations[i]), source, outer, ctes)));
      result = std::move(result_r);
    }
    return result;
  }

  // An already materialized inner-join input which is empty makes the whole
  // join empty.  Stop before opening any remaining base-table scans: this is
  // both a correctness-preserving short circuit and important for EXISTS /
  // derived-table plans, where an empty build side must not touch the probe
  // side at all.  Outer/USING/nested joins are excluded above because their
  // null-extension or merge semantics require the ordered path.
  bool has_empty_materialized_input = false;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i] && !deferred_derived[i] &&
        relations[i].TotalRows() == 0) {
      has_empty_materialized_input = true;
      break;
    }
  }
  if (has_empty_materialized_input) {
    Relation empty(context.execution_runtime());
    for (const Relation& relation : relations) {
      if (empty.schema.ColumnCount() == 0) {
        empty.schema = relation.schema;
      } else {
        empty.schema = empty.schema + relation.schema;
      }
    }
    *where_fully_applied = true;
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->empty_build_short_circuit = true;
    }
    return empty;
  }

  std::vector<Expression> all_predicates =
      SplitConjuncts(statement.WhereClause());
  for (size_t i = 1; i < statement.Sources().size(); ++i) {
    const Expression& condition = statement.Sources()[i].join_condition;
    if (condition) {
      all_predicates.push_back(condition);
    }
  }
  const auto planning_begin = std::chrono::steady_clock::now();
  double join_estimate_ms = 0;
  // Join-condition conjuncts that the pipeline below cannot push down
  // (subquery predicates, constant-only conjuncts such as NOT EXISTS(SELECT 1))
  // must still gate the joined output; dropping them silently changes the
  // join semantics.  Unresolvable *plain-column* conjuncts keep their
  // historic silent-drop behavior: executor tests pin queries whose ON
  // clauses reference outer qualifiers and rely on the cross-join fallback.
  std::vector<Expression> join_residual;
  auto mark_applied = [&join_residual](const Expression& expression) {
    if (!expression) {
      return;
    }
    std::erase_if(join_residual, [&](const Expression& kept) {
      return kept.get() == expression.get();
    });
  };
  std::vector<PredicateInfo> predicates = AnalyzePredicates(
      all_predicates.empty() ? Expression() : CombineConjuncts(all_predicates),
      relations);
  // Synthetic equi-join edges implied by every branch of a disjunction
  // (TPC-DS q13 shape): they join the connectivity simulation, the runtime
  // key-filter loop and the per-step hash keys, while the original
  // disjunction stays applied as residual at its own join step.
  {
    std::vector<PredicateInfo> disjunct_edges =
        SynthesizeDisjunctEqualityPredicates(predicates, relations);
    predicates.insert(predicates.end(),
                      std::make_move_iterator(disjunct_edges.begin()),
                      std::make_move_iterator(disjunct_edges.end()));
  }
  {
    // Populate the residual list from the analyzed predicates so only
    // join-condition conjuncts whose evaluation is well-defined survive.
    std::unordered_set<const ExpressionBase*> from_joins;
    for (size_t i = 1; i < statement.Sources().size(); ++i) {
      for (const Expression& conjunct :
           SplitConjuncts(statement.Sources()[i].join_condition)) {
        if (conjunct) {
          from_joins.insert(conjunct.get());
        }
      }
    }
    for (const PredicateInfo& predicate : predicates) {
      const Expression& expr = predicate.expression;
      if (!expr || !from_joins.contains(expr.get())) {
        continue;
      }
      // Note: USING(name) conjuncts are fully handled by the ordered-join
      // path above, so a bare `x = x` seen here is a user-written ON
      // conjunct; it keeps normal residual/local handling instead of being
      // dropped (dropping it would lose the NULL rejection of `x = x`).
      // Historic behavior: an unresolvable plain-column conjunct is dropped
      // rather than evaluated against a schema where it cannot bind.
      if (!predicate.resolved && !predicate.contains_query &&
          !predicate.sources.empty()) {
        continue;
      }
      join_residual.push_back(expr);
    }
  }
  const std::vector<PredicateInfo> where_predicates =
      AnalyzePredicates(statement.WhereClause(), relations);
  auto locally_evaluable = [&](const PredicateInfo& predicate) {
    if (!predicate.resolved || predicate.sources.size() != 1) {
      return false;
    }
    if (!predicate.contains_query) {
      return true;
    }
    if (ContainsOnlyUncorrelatedQueries(context, predicate.expression, ctes)) {
      return true;
    }
    const size_t src_idx = *predicate.sources.begin();
    if (outer == nullptr && relations.size() == 1 && base_sources[src_idx] &&
        predicate.expression &&
        predicate.expression->Type() == TypeTag::kQueryExp) {
      const QueryExpression& q = predicate.expression->AsQueryExpression();
      if (q.Exists() && !q.Test() && !q.ArrayResult() && q.Query() != nullptr &&
          q.Query()->Sources().size() == 1 && !q.Query()->Sources()[0].query &&
          !q.Query()->Sources()[0].unnest &&
          !ctes.contains(q.Query()->Sources()[0].table)) {
        StatusOr<std::shared_ptr<Table>> out_t =
            context.GetTable(statement.Sources()[src_idx].table);
        StatusOr<std::shared_ptr<Table>> in_t =
            context.GetTable(q.Query()->Sources()[0].table);
        if (out_t.HasValue() && in_t.HasValue()) {
          return true;
        }
      }
    }
    return false;
  };
  *where_fully_applied = std::ranges::all_of(
      where_predicates, [&](const PredicateInfo& predicate) {
        return locally_evaluable(predicate) ||
               (predicate.resolved && !predicate.contains_query &&
                predicate.sources.size() > 1);
      });
  std::vector<std::vector<Expression>> local_predicates(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    std::vector<Expression>& local = local_predicates[i];
    for (const PredicateInfo& predicate : predicates) {
      if (predicate.sources.contains(i) && locally_evaluable(predicate)) {
        local.push_back(predicate.expression);
        mark_applied(predicate.expression);
      } else if (predicate.resolved && !predicate.contains_query &&
                 predicate.sources.size() > 1) {
        Expression implied =
            NecessaryLocalDisjunction(predicate.expression, i, relations);
        if (implied) {
          local.push_back(std::move(implied));
        }
      }
    }
  }

  // Push uncorrelated `col IN (SELECT ...)` results as integer key filters
  // before scanning the owning base table (TPC-H Q18).
  if (context.execution_runtime() != nullptr) {
    for (const Expression& expression :
         SplitConjuncts(statement.WhereClause())) {
      if (!expression || expression->Type() != TypeTag::kQueryExp) {
        continue;
      }
      const QueryExpression& query = expression->AsQueryExpression();
      if (query.Exists() || query.Negated() || !query.Test() ||
          query.Test()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      // The stashed set is the MEMBERSHIP set: it may only prune rows that
      // fail `test IN (rows)`.  `x > ANY (S)` / `x = ALL (S)` match on
      // operators other than equality (or over multiple rows), where S is
      // not a pruning superset: `j > ANY (SELECT 5,6)` would drop j=9.
      // Only the equality-IN shape (`kIn`, or `= ANY`) may stash.
      if (query.Mode() != QuantifierMode::kIn &&
          (query.Mode() != QuantifierMode::kAny ||
           query.Op() != BinaryOperation::kEquals)) {
        continue;
      }
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->retained_statements.push_back(
            query.Query());
      }
      const Relation* membership =
          ExecuteCachedUncorrelated(context, *query.Query(), ctes);
      if (membership == nullptr || membership->rows.empty()) {
        continue;
      }
      // Dynamic filter over the membership set (Phase 2 "Dynamic Filter
      // Join / SIP / Bloom").  The builder keeps the [lo, hi] enclosure of the
      // distinct keys alongside the key set and picks the exact set / SIP
      // shape at Build() time, so a large membership no longer forfeits the
      // range half of the filter.
      std::vector<int64_t> keys;
      keys.reserve(membership->rows.size());
      ValueType membership_type = ValueType::kNull;
      bool all_int = true;
      bool saw_null = false;
      auto consume_row = [&](const Row& row) {
        if (row.values_.empty()) {
          return;
        }
        if (row[0].IsNull()) {
          saw_null = true;
          return;
        }
        if (row[0].type != ValueType::kInt64 &&
            row[0].type != ValueType::kDate) {
          all_int = false;
          return;
        }
        if (membership_type == ValueType::kNull) {
          membership_type = row[0].type;
        } else if (membership_type != row[0].type) {
          // A mixed INT64/DATE membership set has no single int64 comparison
          // domain; leave those rows out and let the equality-IN re-check
          // decide them.
          all_int = false;
          return;
        }
        keys.push_back(row[0].value.int_value);
      };
      if (membership->HasSpill()) {
        membership->ForEachRow(consume_row);
      } else {
        for (const Row& row : membership->rows) {
          consume_row(row);
        }
      }
      if (!all_int || keys.empty()) {
        continue;
      }
      std::shared_ptr<const RuntimeFilterGroup> membership_filter =
          RuntimeFilterBuilder::FromKeys(keys, 32768);
      if (membership_filter == nullptr) {
        continue;
      }
      const bool exact_in =
          !saw_null && (membership_filter->has_dense_bits() ||
                        membership_filter->ExactKeys() != nullptr);
      const size_t membership_keys = membership_filter->DistinctKeys();
      const ColumnName& column = query.Test()->AsColumnValue().GetColumnName();
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!base_sources[i]) {
          continue;
        }
        const auto offset = LocalColumnOffset(relations[i].schema, column);
        if (!offset) {
          continue;
        }
        if (!SingleIntegerJoinKey(relations[i].schema,
                                  {static_cast<slot_t>(*offset)})) {
          continue;
        }
        if (relations[i].schema.GetColumn(*offset).Type() != membership_type) {
          continue;
        }
        TableKeyFilter& stored =
            context.execution_runtime()
                ->table_key_filters[TableKeyFilterKey(statement.Sources()[i])];
        if (stored.empty() || membership_keys < stored.key_count) {
          // shared_ptr copy (refcount bump), not a move: the loop below keeps
          // handing the same group to every eligible source.
          stored.filter = membership_filter;
          stored.type = membership_type;
          stored.key_count = membership_keys;
          stored.column = static_cast<slot_t>(*offset);
          stored.owner = &statement;
          stored.source_expr = expression.get();
          stored.exact_in = exact_in;
        }
      }
    }
  }

  // Load selective relations first, then push their integer join keys into
  // later scans (TPC-H Q9: filter lineitem by partkeys matching LIKE).
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i] && !deferred_derived[i] &&
        !local_predicates[i].empty()) {
      RETURN_IF_FAIL(FilterRelation(context, &relations[i], local_predicates[i],
                                    outer, ctes));
    }
  }
  std::vector<size_t> estimated_rows(relations.size(),
                                     std::numeric_limits<size_t>::max());
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i]) {
      if (deferred_derived[i]) {
        if (StatusOr<std::shared_ptr<TableStatistics>> stats = context.GetStats(
                statement.Sources()[i].query->Sources()[0].table);
            stats.HasValue()) {
          estimated_rows[i] = stats.Value()->Rows();
        }
      } else {
        estimated_rows[i] = relations[i].TotalRows();
      }
      continue;
    }
    if (StatusOr<std::shared_ptr<TableStatistics>> stats =
            context.GetStats(statement.Sources()[i].table);
        stats.HasValue()) {
      estimated_rows[i] = stats.Value()->Rows();
    }
  }
  std::vector<size_t> load_order;
  load_order.reserve(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    load_order.push_back(i);
  }
  std::vector<bool> loaded(relations.size(), false);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i] && !deferred_derived[i]) {
      loaded[i] = true;  // already materialized above
    }
  }
  // Transitive join-key domains: a relation whose only filter partners load
  // later (Q9: partsupp's ps_partkey = l_partkey, with lineitem loading
  // after partsupp) can still be pruned through an equality chain --
  // ps_partkey = l_partkey AND p_partkey = l_partkey makes part's
  // (selective, already-loaded) domain valid for partsupp.  Every resolved
  // two-source equality predicate is applied by the greedy join loop below,
  // so all columns of a class carry equal values in the final result, and
  // pruning a not-yet-loaded member to a loaded member's domain can only
  // remove rows that no surviving row could match.  Ambiguous pairings (a
  // name resolving in both relations) never enter the classes.
  struct RelColKey {
    size_t rel;
    ColumnName column;
    bool operator==(const RelColKey& other) const = default;
  };
  struct RelColKeyHash {
    size_t operator()(const RelColKey& key) const noexcept {
      return std::hash<size_t>()(key.rel) * 0x9e3779b97f4a7c15ULL ^
             std::hash<ColumnName>()(key.column);
    }
  };
  std::unordered_map<RelColKey, size_t, RelColKeyHash> key_class_node;
  std::vector<size_t> key_class_parent;
  static const bool transitive_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER") != nullptr;
  const auto key_class_find = [&](size_t node) {
    while (key_class_parent[node] != node) {
      key_class_parent[node] = key_class_parent[key_class_parent[node]];
      node = key_class_parent[node];
    }
    return node;
  };
  const auto key_class_node_id = [&](const RelColKey& key) {
    const size_t next_id = key_class_parent.size();
    const auto [it, inserted] = key_class_node.emplace(key, next_id);
    if (inserted) {
      key_class_parent.push_back(next_id);
    }
    return it->second;
  };
  const auto key_class_union = [&](size_t a, size_t b) {
    a = key_class_find(a);
    b = key_class_find(b);
    if (a != b) {
      key_class_parent[b] = a;
    }
  };
  {
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 ||
          !IsColumnEqualityPredicate(predicate.expression) ||
          predicate.expression->AsBinaryExpression().Op() !=
              BinaryOperation::kEquals) {
        continue;
      }
      const size_t left_rel = *predicate.sources.begin();
      const size_t right_rel = *std::next(predicate.sources.begin());
      const BinaryExpression& binary =
          predicate.expression->AsBinaryExpression();
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto left_of_lhs =
          LocalColumnOffset(relations[left_rel].schema, lhs);
      const auto right_of_rhs =
          LocalColumnOffset(relations[right_rel].schema, rhs);
      const auto left_of_rhs =
          LocalColumnOffset(relations[left_rel].schema, rhs);
      const auto right_of_lhs =
          LocalColumnOffset(relations[right_rel].schema, lhs);
      const bool forward =
          left_of_lhs.has_value() && right_of_rhs.has_value() &&
          !left_of_rhs.has_value() && !right_of_lhs.has_value();
      const bool crossed =
          left_of_rhs.has_value() && right_of_lhs.has_value() &&
          !left_of_lhs.has_value() && !right_of_rhs.has_value();
      if (transitive_filter_disabled) {
        continue;
      }
      if (forward) {
        const Column& lc = relations[left_rel].schema.GetColumn(*left_of_lhs);
        const Column& rc = relations[right_rel].schema.GetColumn(*right_of_rhs);
        if (lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
          key_class_union(key_class_node_id({left_rel, lhs}),
                          key_class_node_id({right_rel, rhs}));
        }
      } else if (crossed) {
        const Column& lc = relations[left_rel].schema.GetColumn(*left_of_rhs);
        const Column& rc = relations[right_rel].schema.GetColumn(*right_of_lhs);
        if (lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
          key_class_union(key_class_node_id({left_rel, rhs}),
                          key_class_node_id({right_rel, lhs}));
        }
      }
    }
    if (!transitive_filter_disabled && key_class_node.size() > 2) {
      std::unordered_set<std::string> known_pairs;
      for (const PredicateInfo& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() != 2 ||
            !IsColumnEqualityPredicate(predicate.expression) ||
            predicate.expression->AsBinaryExpression().Op() !=
                BinaryOperation::kEquals) {
          continue;
        }
        const BinaryExpression& binary =
            predicate.expression->AsBinaryExpression();
        known_pairs.insert(
            EqualityPairKey(binary.Left()->AsColumnValue().GetColumnName(),
                            binary.Right()->AsColumnValue().GetColumnName()));
      }
      for (const auto& [member_a, id_a] : key_class_node) {
        const auto col_a =
            LocalColumnOffset(relations[member_a.rel].schema, member_a.column);
        if (!col_a.has_value()) {
          continue;
        }
        const size_t rep_a = key_class_find(id_a);
        for (const auto& [member_b, id_b] : key_class_node) {
          if (member_a.rel >= member_b.rel || key_class_find(id_b) != rep_a) {
            continue;
          }
          const auto col_b = LocalColumnOffset(relations[member_b.rel].schema,
                                               member_b.column);
          if (!col_b.has_value() ||
              !SameSingleIntegerJoinKey(relations[member_a.rel].schema,
                                        {static_cast<slot_t>(*col_a)},
                                        relations[member_b.rel].schema,
                                        {static_cast<slot_t>(*col_b)})) {
            continue;
          }
          if (!known_pairs
                   .insert(EqualityPairKey(member_a.column, member_b.column))
                   .second) {
            continue;
          }
          Expression eq = BinaryExpressionExp(ColumnValueExp(member_a.column),
                                              BinaryOperation::kEquals,
                                              ColumnValueExp(member_b.column));
          const std::vector<PredicateInfo> analyzed_eq =
              AnalyzePredicates(eq, relations);
          if (analyzed_eq.size() == 1 && analyzed_eq[0].resolved &&
              !analyzed_eq[0].contains_query &&
              analyzed_eq[0].sources.size() == 2) {
            predicates.push_back(analyzed_eq[0]);
          }
        }
      }
    }
  }
  const auto is_derived_selective = [&](size_t index) {
    if (base_sources[index] || deferred_derived[index] ||
        statement.Sources()[index].query == nullptr) {
      return false;
    }
    const SelectStatement& q = *statement.Sources()[index].query;
    return q.WhereClause() != nullptr || !q.GroupBy().empty() ||
           q.Having() != nullptr || q.Distinct() || q.HasLimit();
  };
  const auto selective_source = [&](size_t index) {
    if (is_derived_selective(index)) {
      return true;
    }
    if (loaded[index] && estimated_rows[index] >= 100) {
      return relations[index].TotalRows() * 10 <= estimated_rows[index] * 8;
    }
    if (!local_predicates[index].empty()) return true;
    if (context.execution_runtime() == nullptr) return false;
    const auto stored = context.execution_runtime()->table_key_filters.find(
        TableKeyFilterKey(statement.Sources()[index]));
    return stored != context.execution_runtime()->table_key_filters.end() &&
           stored->second.owner == &statement && !stored->second.empty();
  };
  std::vector<std::vector<size_t>> filter_neighbours(relations.size());
  for (const PredicateInfo& predicate : predicates) {
    if (!predicate.resolved || predicate.contains_query ||
        predicate.sources.size() != 2 ||
        !IsColumnEqualityPredicate(predicate.expression) ||
        predicate.expression->AsBinaryExpression().Op() !=
            BinaryOperation::kEquals)
      continue;
    const size_t index = *predicate.sources.begin();
    const size_t other = *std::next(predicate.sources.begin());
    const auto& binary = predicate.expression->AsBinaryExpression();
    auto own_column =
        LocalColumnOffset(relations[index].schema,
                          binary.Left()->AsColumnValue().GetColumnName());
    auto other_column =
        LocalColumnOffset(relations[other].schema,
                          binary.Right()->AsColumnValue().GetColumnName());
    if (!own_column || !other_column) {
      own_column =
          LocalColumnOffset(relations[index].schema,
                            binary.Right()->AsColumnValue().GetColumnName());
      other_column =
          LocalColumnOffset(relations[other].schema,
                            binary.Left()->AsColumnValue().GetColumnName());
    }
    if (own_column && other_column &&
        SameSingleIntegerJoinKey(
            relations[index].schema, {static_cast<slot_t>(*own_column)},
            relations[other].schema, {static_cast<slot_t>(*other_column)})) {
      filter_neighbours[index].push_back(other);
      filter_neighbours[other].push_back(index);
    }
  }
  if (!transitive_filter_disabled) {
    for (const auto& [member_a, id_a] : key_class_node) {
      const auto col_a =
          LocalColumnOffset(relations[member_a.rel].schema, member_a.column);
      if (!col_a.has_value()) {
        continue;
      }
      const size_t rep_a = key_class_find(id_a);
      for (const auto& [member_b, id_b] : key_class_node) {
        if (member_a.rel >= member_b.rel || key_class_find(id_b) != rep_a) {
          continue;
        }
        const auto col_b =
            LocalColumnOffset(relations[member_b.rel].schema, member_b.column);
        if (col_b.has_value() &&
            SameSingleIntegerJoinKey(relations[member_a.rel].schema,
                                     {static_cast<slot_t>(*col_a)},
                                     relations[member_b.rel].schema,
                                     {static_cast<slot_t>(*col_b)})) {
          filter_neighbours[member_a.rel].push_back(member_b.rel);
          filter_neighbours[member_b.rel].push_back(member_a.rel);
        }
      }
    }
  }
  std::vector<uint8_t> selective(relations.size(), 0);
  const auto effective_load_rows = [&](size_t index) -> double {
    double best_ratio = 1.0;
    for (size_t other : filter_neighbours[index]) {
      if (loaded[other] && selective[other] && estimated_rows[other] > 0) {
        const double ratio = static_cast<double>(relations[other].TotalRows()) /
                             static_cast<double>(estimated_rows[other]);
        best_ratio = std::min(best_ratio, ratio);
      }
    }
    return static_cast<double>(estimated_rows[index]) * best_ratio;
  };
  const auto load_rank = [&](size_t index) {
    const bool has_selective_neighbour = std::ranges::any_of(
        filter_neighbours[index],
        [&](size_t other) { return loaded[other] && selective[other]; });
    const double self_eff = effective_load_rows(index);
    const bool awaits_smaller_selective_neighbour =
        local_predicates[index].empty() &&
        std::ranges::any_of(filter_neighbours[index], [&](size_t other) {
          return !loaded[other] && !local_predicates[other].empty() &&
                 estimated_rows[other] < estimated_rows[index] &&
                 effective_load_rows(other) <= self_eff * 2.0;
        });
    if (deferred_derived[index]) {
      return has_selective_neighbour ? 1 : 2;
    }
    if (selective[index] && has_selective_neighbour &&
        estimated_rows[index] <= 500000)
      return -1;
    if ((selective[index] || has_selective_neighbour) &&
        !awaits_smaller_selective_neighbour)
      return 0;
    if (selective[index] || has_selective_neighbour) return 1;
    return 2;
  };
  struct ExactFilteredEdge {
    slot_t projected_col{0};
    size_t driver_rel{0};
  };
  std::vector<std::vector<ExactFilteredEdge>> exact_filtered_by(
      relations.size());
  struct CachedDriverFilter {
    bool valid{false};
    std::shared_ptr<const RuntimeFilterGroup> filter;
    int64_t observed_min{0};
    int64_t observed_max{-1};
    bool saw_null{false};
    bool cand_exact{false};
    size_t distinct_keys{0};
  };
  std::unordered_map<uint64_t, CachedDriverFilter> driver_filter_cache;
  const auto invalidate_driver_filter_cache = [&](size_t rel_idx) {
    for (auto it = driver_filter_cache.begin();
         it != driver_filter_cache.end();) {
      if ((it->first >> 32) == static_cast<uint64_t>(rel_idx)) {
        it = driver_filter_cache.erase(it);
      } else {
        ++it;
      }
    }
  };
  const auto get_driver_filter = [&](size_t drel,
                                     slot_t dcol) -> const CachedDriverFilter& {
    const uint64_t cache_key =
        (static_cast<uint64_t>(drel) << 32) | static_cast<uint64_t>(dcol);
    auto [it, inserted] = driver_filter_cache.try_emplace(cache_key);
    if (!inserted) {
      return it->second;
    }
    CachedDriverFilter& entry = it->second;
    RuntimeFilterBuilder builder;
    builder.Reserve(relations[drel].TotalRows());
    relations[drel].FinishSpill();
    relations[drel].ForEachRow([&](const Row& row) {
      const Value& value = row[dcol];
      if (value.IsNull()) {
        builder.ObserveNull();
      } else {
        builder.Observe(value.value.int_value);
        entry.observed_min =
            entry.observed_max < 0
                ? value.value.int_value
                : std::min(entry.observed_min, value.value.int_value);
        entry.observed_max =
            std::max(entry.observed_max, value.value.int_value);
      }
    });
    if (builder.empty() || builder.overflowed()) {
      return entry;
    }
    entry.filter = builder.Build(false, 65536);
    if (entry.filter == nullptr || entry.filter->empty() ||
        entry.filter->Has(RuntimeFilterKind::kRejectAll)) {
      entry.filter.reset();
      return entry;
    }
    entry.saw_null = builder.saw_null();
    entry.distinct_keys = entry.filter->DistinctKeys();
    entry.cand_exact =
        !entry.saw_null && (entry.filter->has_dense_bits() ||
                            entry.filter->ExactKeys() != nullptr);
    entry.valid = true;
    return entry;
  };
  std::vector<uint8_t> absorbed(relations.size(), 0);
  static const bool pk_absorption_disabled =
      std::getenv("TINYLAMB_DISABLE_PK_ABSORPTION") != nullptr;
  const bool selects_star = std::ranges::any_of(
      statement.SelectList(), [](const NamedExpression& proj) {
        return proj.expression &&
               proj.expression->Type() == TypeTag::kColumnValue &&
               proj.expression->AsColumnValue().GetColumnName().name == "*";
      });
  const bool plain_inner_sources =
      std::ranges::all_of(statement.Sources(), [](const SelectSource& s) {
        return (s.join_type == JoinType::kInner ||
                s.join_type == JoinType::kCross) &&
               s.join_condition == nullptr && !s.sample.has_value();
      });
  std::unordered_set<ColumnName> outer_referenced;
  const auto collect_expr_cols = [&](const Expression& expr) {
    if (!expr) return;
    for (const ColumnName& cn : expr->TouchedColumns()) {
      outer_referenced.insert(cn);
    }
  };
  if (!pk_absorption_disabled && *where_fully_applied && plain_inner_sources &&
      !selects_star && relations.size() >= 2) {
    for (const NamedExpression& item : statement.SelectList()) {
      collect_expr_cols(item.expression);
    }
    for (const Expression& g : statement.GroupBy()) {
      collect_expr_cols(g);
    }
    collect_expr_cols(statement.Having());
    for (const SelectStatement::OrderByTerm& ob : statement.OrderBy()) {
      collect_expr_cols(ob.expression);
    }
    for (const Expression& d : statement.DistinctOn()) {
      collect_expr_cols(d);
    }
    for (const Expression& r : join_residual) {
      collect_expr_cols(r);
    }
    for (const PredicateInfo& p : predicates) {
      if (!p.resolved || p.contains_query || p.sources.size() > 2) {
        collect_expr_cols(p.expression);
      }
    }
  }
  const size_t min_remaining_relations = 1;
  std::vector<std::shared_ptr<const LSTableStore>> pinned_stores;
  for (size_t position = 0; position < load_order.size(); ++position) {
    for (size_t i = 0; i < selective.size(); ++i)
      selective[i] = selective_source(i);
    auto best = load_order.begin() + static_cast<std::ptrdiff_t>(position);
    for (auto candidate = std::next(best); candidate != load_order.end();
         ++candidate) {
      const int rank = load_rank(*candidate), best_rank = load_rank(*best);
      if (rank < best_rank) {
        best = candidate;
      } else if (rank == best_rank) {
        const double cand_eff = effective_load_rows(*candidate);
        const double best_eff = effective_load_rows(*best);
        if (cand_eff + 1e-6 < best_eff ||
            (std::abs(cand_eff - best_eff) <= 1e-6 &&
             (estimated_rows[*candidate] < estimated_rows[*best] ||
              (estimated_rows[*candidate] == estimated_rows[*best] &&
               *candidate < *best)))) {
          best = candidate;
        }
      }
    }
    std::iter_swap(load_order.begin() + static_cast<std::ptrdiff_t>(position),
                   best);
    const size_t idx = load_order[position];
    if (!base_sources[idx] && !deferred_derived[idx]) {
      loaded[idx] = true;
      continue;
    }
    if (!pk_absorption_disabled && *where_fully_applied &&
        plain_inner_sources && !selects_star && relations.size() >= 2 &&
        base_sources[idx] && !deferred_derived[idx] &&
        !statement.Sources()[idx].sample.has_value() &&
        local_predicates[idx].empty() && projections[idx].size() == 2) {
      size_t active_relations = 0;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!absorbed[i]) ++active_relations;
      }
      const bool has_stored_key_filter =
          context.execution_runtime() != nullptr && [&] {
            const auto stored =
                context.execution_runtime()->table_key_filters.find(
                    TableKeyFilterKey(statement.Sources()[idx]));
            return stored !=
                       context.execution_runtime()->table_key_filters.end() &&
                   stored->second.owner == &statement;
          }();
      if (active_relations > min_remaining_relations &&
          !has_stored_key_filter) {
        StatusOr<std::shared_ptr<Table>> d_table =
            context.GetTable(statement.Sources()[idx].table);
        if (d_table.HasValue()) {
          struct PreJoinEdge {
            slot_t d_col{0};
            size_t other{0};
            slot_t o_col{0};
          };
          std::vector<PreJoinEdge> edges;
          bool predicates_ok = true;
          for (const PredicateInfo& p : predicates) {
            if (!p.resolved || !p.sources.contains(idx)) {
              continue;
            }
            if (p.contains_query || p.sources.size() != 2 ||
                !IsColumnEqualityPredicate(p.expression) ||
                p.expression->AsBinaryExpression().Op() !=
                    BinaryOperation::kEquals) {
              predicates_ok = false;
              break;
            }
            size_t other = *p.sources.begin();
            if (other == idx) {
              other = *std::next(p.sources.begin());
            }
            if (absorbed[other]) {
              predicates_ok = false;
              break;
            }
            const BinaryExpression& bin = p.expression->AsBinaryExpression();
            const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                bin.Right()->AsColumnValue().GetColumnName();
            const auto d_left = LocalColumnOffset(relations[idx].schema, lhs);
            const auto d_right = LocalColumnOffset(relations[idx].schema, rhs);
            const auto o_left = LocalColumnOffset(relations[other].schema, lhs);
            const auto o_right =
                LocalColumnOffset(relations[other].schema, rhs);
            std::optional<slot_t> d_col;
            std::optional<slot_t> o_col;
            if (d_left.has_value() && o_right.has_value() &&
                !d_right.has_value() && !o_left.has_value()) {
              d_col = static_cast<slot_t>(*d_left);
              o_col = static_cast<slot_t>(*o_right);
            } else if (d_right.has_value() && o_left.has_value() &&
                       !d_left.has_value() && !o_right.has_value()) {
              d_col = static_cast<slot_t>(*d_right);
              o_col = static_cast<slot_t>(*o_left);
            }
            if (!d_col.has_value() || !o_col.has_value()) {
              predicates_ok = false;
              break;
            }
            edges.push_back(PreJoinEdge{*d_col, other, *o_col});
          }
          if (predicates_ok && !edges.empty()) {
            std::vector<slot_t> active_cols;
            const auto add_active_col = [&](slot_t c) {
              if (std::ranges::find(active_cols, c) == active_cols.end()) {
                active_cols.push_back(c);
              }
            };
            for (const ColumnName& cn : outer_referenced) {
              if (const auto off =
                      LocalColumnOffset(relations[idx].schema, cn)) {
                add_active_col(static_cast<slot_t>(*off));
              }
            }
            for (const PreJoinEdge& e : edges) {
              add_active_col(e.d_col);
            }
            std::optional<slot_t> d_pk_col;
            std::optional<slot_t> d_payload_col;
            std::optional<size_t> target_rel;
            std::optional<slot_t> target_col;
            bool keep_pk_col = false;
            if (active_cols.size() == 2) {
              for (size_t try_idx = 0; try_idx < 2; ++try_idx) {
                const slot_t cand_pk = active_cols[try_idx];
                const slot_t cand_payload = active_cols[1 - try_idx];
                const Column& pk_col = relations[idx].schema.GetColumn(cand_pk);
                if (pk_col.IsUnsigned() ||
                    (pk_col.Type() != ValueType::kInt64 &&
                     pk_col.Type() != ValueType::kDate)) {
                  continue;
                }
                bool pk_in_outer = false;
                for (const ColumnName& cn : outer_referenced) {
                  if (LocalColumnOffset(relations[idx].schema, cn) ==
                      static_cast<int>(cand_pk)) {
                    pk_in_outer = true;
                    break;
                  }
                }
                std::optional<size_t> cand_t_rel;
                std::optional<slot_t> cand_t_col;
                bool edges_valid = true;
                for (const PreJoinEdge& e : edges) {
                  if (e.d_col == cand_pk) {
                    if (!loaded[e.other] || relations[e.other].HasSpill() ||
                        relations[e.other].rows.empty() ||
                        !SameSingleIntegerJoinKey(
                            relations[idx].schema, {cand_pk},
                            relations[e.other].schema, {e.o_col})) {
                      edges_valid = false;
                      break;
                    }
                    if (cand_t_rel.has_value() &&
                        (*cand_t_rel != e.other || *cand_t_col != e.o_col)) {
                      edges_valid = false;
                      break;
                    }
                    cand_t_rel = e.other;
                    cand_t_col = e.o_col;
                  } else if (e.d_col == cand_payload) {
                    if (loaded[e.other]) {
                      if (relations[e.other].HasSpill() ||
                          relations[e.other].rows.empty()) {
                        edges_valid = false;
                        break;
                      }
                    } else if (!base_sources[e.other] ||
                               deferred_derived[e.other] ||
                               selective[e.other] ||
                               !local_predicates[e.other].empty()) {
                      edges_valid = false;
                      break;
                    }
                    if (cand_t_rel.has_value() && e.other == *cand_t_rel) {
                      edges_valid = false;
                      break;
                    }
                  } else {
                    edges_valid = false;
                    break;
                  }
                }
                if (!edges_valid || !cand_t_rel.has_value() ||
                    !cand_t_col.has_value()) {
                  continue;
                }
                for (const PreJoinEdge& e : edges) {
                  if (e.d_col == cand_payload && e.other == *cand_t_rel) {
                    edges_valid = false;
                    break;
                  }
                }
                if (!edges_valid) {
                  continue;
                }
                bool t_col_referenced = false;
                for (const ColumnName& cn : outer_referenced) {
                  if (LocalColumnOffset(relations[*cand_t_rel].schema, cn) ==
                      static_cast<int>(*cand_t_col)) {
                    t_col_referenced = true;
                    break;
                  }
                }
                if (!t_col_referenced) {
                  for (const PredicateInfo& p : predicates) {
                    if (!p.resolved || p.sources.contains(idx) ||
                        !p.sources.contains(*cand_t_rel) || !p.expression) {
                      continue;
                    }
                    for (const ColumnName& cn :
                         p.expression->TouchedColumns()) {
                      if (LocalColumnOffset(relations[*cand_t_rel].schema,
                                            cn) ==
                          static_cast<int>(*cand_t_col)) {
                        t_col_referenced = true;
                        break;
                      }
                    }
                    if (t_col_referenced) {
                      break;
                    }
                  }
                }
                if (!t_col_referenced) {
                  d_pk_col = cand_pk;
                  d_payload_col = cand_payload;
                  target_rel = *cand_t_rel;
                  target_col = *cand_t_col;
                  keep_pk_col = pk_in_outer;
                  break;
                }
              }
            }
            if (d_pk_col.has_value() && d_payload_col.has_value() &&
                target_rel.has_value() && target_col.has_value()) {
              const Column& pk_col = relations[idx].schema.GetColumn(*d_pk_col);
              const Column& payload_col =
                  relations[idx].schema.GetColumn(*d_payload_col);
              Relation& tgt = relations[*target_rel];
              bool name_collision = false;
              for (size_t ci = 0; ci < tgt.schema.ColumnCount(); ++ci) {
                if (ci == *target_col) {
                  continue;
                }
                if (IdentifierEquals(tgt.schema.GetColumn(ci).Name().name,
                                     payload_col.Name().name) ||
                    (keep_pk_col &&
                     IdentifierEquals(tgt.schema.GetColumn(ci).Name().name,
                                      pk_col.Name().name))) {
                  name_collision = true;
                  break;
                }
              }
              std::shared_ptr<const LSTableStore> store =
                  !name_collision ? GetOrBuildLSTableStore(
                                        context.txn_, *d_table.Value(), true)
                                  : nullptr;
              bool store_valid =
                  store != nullptr && store->total_rows > 0 &&
                  store->layout_generation ==
                      d_table.Value()->LayoutGeneration() &&
                  store->first_pid == d_table.Value()->FirstPageId() &&
                  *d_pk_col < store->column_is_monotonic.size() &&
                  *d_payload_col < store->column_min_int.size() &&
                  store->column_is_monotonic[*d_pk_col] != 0 &&
                  store->column_is_unique_nonnull[*d_pk_col] != 0 &&
                  store->column_has_null[*d_pk_col] == 0 &&
                  store->column_min_int[*d_pk_col] <=
                      store->column_max_int[*d_pk_col];
              if (store_valid) {
                if (store->uncommitted_owner_txn != 0) {
                  store_valid =
                      (store->uncommitted_owner_txn == context.txn_.ID());
                } else if (!context.txn_.IsReadOnly() &&
                           !context.txn_.WriteSetEmpty()) {
                  store_valid = false;
                } else if (context.txn_.HasAnyPageVersionChains()) {
                  for (const page_id_t pid : store->page_ids) {
                    if (context.txn_.HasPageVersionChains(pid)) {
                      store_valid = false;
                      break;
                    }
                  }
                }
              }
              const ValueType p_type = payload_col.Type();
              if (store_valid &&
                  (p_type == ValueType::kInt64 || p_type == ValueType::kDate ||
                   p_type == ValueType::kDouble ||
                   p_type == ValueType::kVarChar)) {
                std::vector<uint32_t> non_empty_blocks;
                non_empty_blocks.reserve(store->blocks.size());
                std::vector<int64_t> blk_max_keys;
                blk_max_keys.reserve(store->blocks.size());
                for (size_t bi = 0; bi < store->blocks.size(); ++bi) {
                  if (store->blocks[bi].row_count > 0) {
                    non_empty_blocks.push_back(static_cast<uint32_t>(bi));
                    blk_max_keys.push_back(
                        store->blocks[bi].columns[*d_pk_col].max_int);
                  }
                }
                if (!non_empty_blocks.empty()) {
                  const int64_t min_k = store->column_min_int[*d_pk_col];
                  const int64_t max_k = store->column_max_int[*d_pk_col];
                  const uint64_t span = static_cast<uint64_t>(max_k) -
                                        static_cast<uint64_t>(min_k) + 1ULL;
                  constexpr size_t kDirShift = 12U;
                  constexpr size_t kDirBuckets = size_t{1} << kDirShift;
                  std::array<uint32_t, kDirBuckets + 1> bucket_lo{};
                  const auto nb_count =
                      static_cast<uint32_t>(non_empty_blocks.size());
                  uint32_t cur_nb = 0;
                  for (size_t b = 0; b < kDirBuckets; ++b) {
                    const int64_t b_min =
                        min_k +
                        static_cast<int64_t>(
                            (static_cast<__uint128_t>(span) * b) >> kDirShift);
                    while (cur_nb + 1 < nb_count &&
                           blk_max_keys[cur_nb] < b_min) {
                      ++cur_nb;
                    }
                    bucket_lo[b] = cur_nb;
                  }
                  bucket_lo[kDirBuckets] = nb_count - 1;
                  const bool p_unsigned = payload_col.IsUnsigned();
                  if (p_type == ValueType::kVarChar) {
                    pinned_stores.push_back(store);
                  }
                  const size_t nworkers = std::max<size_t>(
                      size_t{1}, std::min<size_t>(
                                     {size_t{16},
                                      static_cast<size_t>(
                                          std::thread::hardware_concurrency()),
                                      (tgt.rows.size() / 16384) + 1}));
                  std::vector<uint8_t> worker_had_unmatched(nworkers, 0);
                  auto run_absorb_shard = [&](size_t w) {
                    const size_t r_begin = (tgt.rows.size() * w) / nworkers;
                    const size_t r_end = (tgt.rows.size() * (w + 1)) / nworkers;
                    uint8_t had_unmatched = 0;
                    int64_t last_key = std::numeric_limits<int64_t>::min();
                    bool have_last = false;
                    bool last_matched = false;
                    Value last_val;
                    uint32_t last_nb = 0;
                    uint32_t last_row_in_blk = 0;
                    for (size_t ri = r_begin; ri < r_end; ++ri) {
                      Value& cell = tgt.rows[ri].values_[*target_col];
                      if (cell.IsNull() || cell.value.int_value < min_k ||
                          cell.value.int_value > max_k) [[unlikely]] {
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      const int64_t k = cell.value.int_value;
                      if (have_last && k == last_key) {
                        if (!last_matched) [[unlikely]] {
                          cell.type = ValueType::kNull;
                          cell.SetCollation(1);
                          had_unmatched = 1;
                        } else if (keep_pk_col) {
                          tgt.rows[ri].values_.push_back(last_val);
                        } else {
                          cell = last_val;
                        }
                        continue;
                      }
                      uint32_t nb = last_nb;
                      if (nb >= nb_count || k > blk_max_keys[nb] ||
                          k < store->blocks[non_empty_blocks[nb]]
                                  .columns[*d_pk_col]
                                  .min_int) {
                        if (nb + 1 < nb_count && k <= blk_max_keys[nb + 1] &&
                            k >= store->blocks[non_empty_blocks[nb + 1]]
                                     .columns[*d_pk_col]
                                     .min_int) {
                          nb = nb + 1;
                        } else {
                          const auto b = static_cast<size_t>(
                              (static_cast<__uint128_t>(
                                   static_cast<uint64_t>(k) -
                                   static_cast<uint64_t>(min_k))
                               << kDirShift) /
                              span);
                          const uint32_t lo = bucket_lo[b];
                          const uint32_t hi = std::min<uint32_t>(
                              nb_count - 1, bucket_lo[b + 1] + 1);
                          if (hi - lo <= 4) {
                            nb = lo;
                            while (nb < hi && blk_max_keys[nb] < k) {
                              ++nb;
                            }
                          } else {
                            const auto it = std::lower_bound(
                                blk_max_keys.begin() + lo,
                                blk_max_keys.begin() + hi + 1, k);
                            nb = static_cast<uint32_t>(
                                std::distance(blk_max_keys.begin(), it));
                          }
                        }
                      }
                      const bool same_blk_monotonic =
                          have_last && last_matched && nb == last_nb &&
                          k > last_key;
                      last_nb = nb;
                      last_key = k;
                      have_last = true;
                      const LSTablePageBlock& blk =
                          store->blocks[non_empty_blocks[nb]];
                      const LSTableColumnBlock& pk_blk = blk.columns[*d_pk_col];
                      if (k < pk_blk.min_int || k > pk_blk.max_int) {
                        last_matched = false;
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      uint32_t row_in_blk = blk.row_count;
                      if (static_cast<uint64_t>(pk_blk.max_int) -
                              static_cast<uint64_t>(pk_blk.min_int) + 1ULL ==
                          blk.row_count) {
                        row_in_blk = static_cast<uint32_t>(
                            static_cast<uint64_t>(k) -
                            static_cast<uint64_t>(pk_blk.min_int));
                      } else {
                        const int64_t* const begin_ptr =
                            pk_blk.int_cells.data();
                        const int64_t* const end_ptr =
                            begin_ptr + blk.row_count;
                        const int64_t* search_begin = begin_ptr;
                        const int64_t* search_end = end_ptr;
                        if (same_blk_monotonic &&
                            last_row_in_blk + 1 < blk.row_count) {
                          const uint32_t next_r = last_row_in_blk + 1;
                          if (begin_ptr[next_r] == k) {
                            row_in_blk = next_r;
                            search_begin = end_ptr;
                          } else if (begin_ptr[next_r] < k) {
                            search_begin = begin_ptr + next_r + 1;
                            if (next_r + 16 < blk.row_count &&
                                begin_ptr[next_r + 16] >= k) {
                              search_end = begin_ptr + next_r + 17;
                            } else if (next_r + 64 < blk.row_count &&
                                       begin_ptr[next_r + 64] >= k) {
                              search_begin = begin_ptr + next_r + 16;
                              search_end = begin_ptr + next_r + 65;
                            }
                          }
                        }
                        if (search_begin < search_end) {
                          const int64_t* const found_ptr =
                              std::lower_bound(search_begin, search_end, k);
                          if (found_ptr != search_end && *found_ptr == k)
                              [[likely]] {
                            row_in_blk =
                                static_cast<uint32_t>(found_ptr - begin_ptr);
                          }
                        }
                      }
                      if (row_in_blk >= blk.row_count) [[unlikely]] {
                        last_matched = false;
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      last_row_in_blk = row_in_blk;
                      last_matched = true;
                      const LSTableColumnBlock& pay_blk =
                          blk.columns[*d_payload_col];
                      if (pay_blk.IsNull(row_in_blk)) [[unlikely]] {
                        last_val = Value();
                      } else if (p_type == ValueType::kInt64) {
                        last_val.type = ValueType::kInt64;
                        last_val.SetCollation(0);
                        if (p_unsigned) {
                          last_val.SetUnsigned();
                        }
                        last_val.value.int_value =
                            pay_blk.int_cells[row_in_blk];
                      } else if (p_type == ValueType::kDate) {
                        last_val.type = ValueType::kDate;
                        last_val.SetCollation(0);
                        last_val.value.int_value =
                            pay_blk.int_cells[row_in_blk];
                      } else if (p_type == ValueType::kDouble) {
                        double dv = 0.0;
                        std::memcpy(&dv, &pay_blk.int_cells[row_in_blk],
                                    sizeof(dv));
                        last_val.type = ValueType::kDouble;
                        last_val.SetCollation(0);
                        last_val.value.double_value = dv;
                      } else {
                        last_val.type = ValueType::kVarChar;
                        last_val.SetCollation(0);
                        last_val.value.varchar_value =
                            pay_blk.VarcharAt(row_in_blk);
                      }
                      if (keep_pk_col) {
                        tgt.rows[ri].values_.push_back(last_val);
                      } else if (last_val.type == ValueType::kVarChar) {
                        cell.type = ValueType::kVarChar;
                        cell.SetCollation(0);
                        cell.owned_data.clear();
                        cell.value.varchar_value = last_val.value.varchar_value;
                      } else {
                        cell = last_val;
                      }
                    }
                    worker_had_unmatched[w] = had_unmatched;
                  };
                  if (nworkers <= 1) {
                    run_absorb_shard(0);
                  } else {
                    std::vector<std::jthread> threads;
                    threads.reserve(nworkers - 1);
                    for (size_t w = 1; w < nworkers; ++w) {
                      threads.emplace_back([&, w] { run_absorb_shard(w); });
                    }
                    run_absorb_shard(0);
                  }
                  if (std::ranges::any_of(worker_had_unmatched,
                                          [](uint8_t u) { return u != 0; })) {
                    size_t out_idx = 0;
                    for (size_t ri = 0; ri < tgt.rows.size(); ++ri) {
                      const Value& cell = tgt.rows[ri].values_[*target_col];
                      if (cell.IsNull() && cell.Collation() == 1) {
                        continue;
                      }
                      if (out_idx != ri) {
                        tgt.rows[out_idx] = std::move(tgt.rows[ri]);
                      }
                      ++out_idx;
                    }
                    tgt.rows.resize(out_idx);
                  }
                  Column new_payload_col(payload_col.Name(), payload_col.Type(),
                                         Constraint(Constraint::kNothing));
                  new_payload_col.SetUnsigned(payload_col.IsUnsigned());
                  std::vector<Column> new_cols;
                  new_cols.reserve(tgt.schema.ColumnCount() +
                                   (keep_pk_col ? 1 : 0));
                  for (size_t ci = 0; ci < tgt.schema.ColumnCount(); ++ci) {
                    if (ci == *target_col) {
                      if (keep_pk_col) {
                        Column new_pk_col(pk_col.Name(), pk_col.Type(),
                                          Constraint(Constraint::kNothing));
                        new_pk_col.SetUnsigned(pk_col.IsUnsigned());
                        new_cols.push_back(std::move(new_pk_col));
                      } else {
                        new_cols.push_back(new_payload_col);
                      }
                    } else {
                      new_cols.push_back(tgt.schema.GetColumn(ci));
                    }
                  }
                  if (keep_pk_col) {
                    new_cols.push_back(std::move(new_payload_col));
                  }
                  tgt.schema = Schema("", std::move(new_cols));
                  invalidate_driver_filter_cache(*target_rel);
                  invalidate_driver_filter_cache(idx);
                  loaded[idx] = true;
                  absorbed[idx] = 1;
                  for (PredicateInfo& p : predicates) {
                    if (!p.resolved || !p.sources.contains(idx)) {
                      continue;
                    }
                    bool touches_pk = false;
                    if (p.expression) {
                      for (const ColumnName& cn :
                           p.expression->TouchedColumns()) {
                        if (LocalColumnOffset(relations[idx].schema, cn) ==
                            static_cast<int>(*d_pk_col)) {
                          touches_pk = true;
                          break;
                        }
                      }
                    }
                    if (touches_pk) {
                      mark_applied(p.expression);
                      p.resolved = false;
                    } else {
                      p.sources.erase(idx);
                      p.sources.insert(*target_rel);
                      for (size_t s : p.sources) {
                        if (s != *target_rel) {
                          filter_neighbours[s].push_back(*target_rel);
                          filter_neighbours[*target_rel].push_back(s);
                        }
                      }
                    }
                  }
                  continue;
                }
              }
            }
          }
        }
      }
    }
    // Dynamic filter built from the best driver side (Phase 2 "Dynamic Filter
    // Join / SIP / Bloom").  `key_filter` is a RuntimeFilterGroup, so the same
    // pruning hook serves the exact build key set, the [lo, hi] enclosure and
    // the Bloom membership filter.
    auto target_stats = context.GetStats(
        deferred_derived[idx]
            ? statement.Sources()[idx].query->Sources()[0].table
            : statement.Sources()[idx].table);
    std::shared_ptr<const RuntimeFilterGroup> key_filter;
    ValueType key_filter_type{ValueType::kNull};
    std::optional<slot_t> key_column;  // offset in the projected scan row
    size_t best_driver_rows = std::numeric_limits<size_t>::max();
    double best_driver_coverage = 1.0;
    bool best_driver_selective = false;
    size_t best_driver_rel = std::numeric_limits<size_t>::max();
    bool best_driver_exact = false;
    struct SecondaryColFilter {
      std::shared_ptr<const RuntimeFilterGroup> filter;
      ValueType type{ValueType::kNull};
      slot_t projected_col{0};
      size_t driver_rows{std::numeric_limits<size_t>::max()};
      double coverage{1.0};
      size_t driver_rel{std::numeric_limits<size_t>::max()};
      bool exact{false};
    };
    std::vector<SecondaryColFilter> col_filters;
    const auto record_secondary =
        [&](slot_t pcol, ValueType ptype,
            const std::shared_ptr<const RuntimeFilterGroup>& cand, size_t drows,
            double cov, bool is_selective, size_t drel, bool is_exact) {
          if (!is_selective || cov > 0.50) {
            return;
          }
          for (SecondaryColFilter& sf : col_filters) {
            if (sf.projected_col == pcol) {
              if (cov < sf.coverage - 1e-9 ||
                  (cov <= sf.coverage + 1e-9 && drows < sf.driver_rows)) {
                sf.filter = cand;
                sf.type = ptype;
                sf.driver_rows = drows;
                sf.coverage = cov;
                sf.driver_rel = drel;
                sf.exact = is_exact;
              }
              return;
            }
          }
          col_filters.push_back(SecondaryColFilter{cand, ptype, pcol, drows,
                                                   cov, drel, is_exact});
        };
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 || !predicate.sources.contains(idx) ||
          !IsColumnEqualityPredicate(predicate.expression) ||
          predicate.expression->AsBinaryExpression().Op() !=
              BinaryOperation::kEquals) {
        continue;
      }
      size_t other = *predicate.sources.begin();
      if (other == idx) {
        other = *std::next(predicate.sources.begin());
      }
      if (!loaded[other]) {
        continue;
      }
      const size_t driver_rows = relations[other].TotalRows();
      const bool other_selective =
          is_derived_selective(other) ||
          (estimated_rows[other] >= 100 &&
           driver_rows * 10 <= estimated_rows[other] * 8) ||
          ((!local_predicates[other].empty() ||
            (context.execution_runtime() != nullptr &&
             [&] {
               const auto stored =
                   context.execution_runtime()->table_key_filters.find(
                       TableKeyFilterKey(statement.Sources()[other]));
               return stored != context.execution_runtime()
                                    ->table_key_filters.end() &&
                      stored->second.owner == &statement;
             }())) &&
           (estimated_rows[other] < 100 ||
            driver_rows * 10 <= estimated_rows[other] * 8));
      // Prefer drivers that were themselves filtered (e.g. part LIKE), even
      // if an unfiltered neighbor has slightly fewer rows (supplier).
      if (key_column && best_driver_selective && !other_selective) {
        continue;
      }
      const BinaryExpression& binary =
          predicate.expression->AsBinaryExpression();
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto idx_left = LocalColumnOffset(relations[idx].schema, lhs);
      const auto idx_right = LocalColumnOffset(relations[idx].schema, rhs);
      const auto other_left = LocalColumnOffset(relations[other].schema, lhs);
      const auto other_right = LocalColumnOffset(relations[other].schema, rhs);
      std::optional<slot_t> idx_col;
      std::optional<slot_t> other_col;
      if (idx_left && other_right) {
        idx_col = static_cast<slot_t>(*idx_left);
        other_col = static_cast<slot_t>(*other_right);
      } else if (idx_right && other_left) {
        idx_col = static_cast<slot_t>(*idx_right);
        other_col = static_cast<slot_t>(*other_left);
      }
      if (!idx_col || !other_col) {
        continue;
      }
      if (!SameSingleIntegerJoinKey(relations[idx].schema, {*idx_col},
                                    relations[other].schema, {*other_col})) {
        // Type equality is part of the contract, not just "both integers":
        // INT64 and DATE share the int64 bit domain but not the comparison
        // domain, so a filter built from DATE keys could otherwise prune
        // INT64 probe rows whose bits merely look like dates.
        continue;
      }
      // Map full-schema column index into the projected scan layout.
      const auto& proj = projections[idx];
      const auto proj_it = std::ranges::find(proj, *idx_col);
      if (proj_it == proj.end()) {
        continue;
      }
      const auto projected_col =
          static_cast<slot_t>(std::distance(proj.begin(), proj_it));
      if (relations[idx].schema.GetColumn(*idx_col).GetConstraint().ctype ==
              Constraint::kPrimaryKey &&
          estimated_rows[idx] <= 100000 &&
          driver_rows >= estimated_rows[idx] * 2) {
        continue;
      }
      if (key_column.has_value() && *key_column == projected_col &&
          best_driver_selective && best_driver_exact &&
          driver_rows >= best_driver_rows) {
        continue;
      }

      const CachedDriverFilter& built = get_driver_filter(other, *other_col);
      if (!built.valid) {
        continue;
      }
      const std::shared_ptr<const RuntimeFilterGroup>& candidate = built.filter;
      const int64_t observed_min = built.observed_min;
      const int64_t observed_max = built.observed_max;
      const bool cand_exact = built.cand_exact;
      double coverage = 1.0;
      if (observed_max >= observed_min) {
        double domain_size = static_cast<double>(observed_max) -
                             static_cast<double>(observed_min) + 1.0;
        if (other_selective &&
            relations[other]
                    .schema.GetColumn(*other_col)
                    .GetConstraint()
                    .ctype == Constraint::kPrimaryKey &&
            estimated_rows[other] >= candidate->DistinctKeys() &&
            estimated_rows[other] > 0) {
          domain_size = static_cast<double>(estimated_rows[other]);
        } else if (other_selective && target_stats.HasValue() &&
                   *idx_col < target_stats.Value()->Columns()) {
          const size_t col_distinct =
              target_stats.Value()->Column(*idx_col).Distinct();
          if (col_distinct >= candidate->DistinctKeys() && col_distinct > 0) {
            domain_size = static_cast<double>(col_distinct);
          }
        }
        coverage =
            std::min(1.0, static_cast<double>(candidate->DistinctKeys()) /
                              std::max(1.0, domain_size));
      }
      // Skip near-full key domains (e.g. all supplier keys vs lineitem): the
      // membership test then costs more than the row it saves.  A range-only
      // SIP filter is still worth building there, because the enclosure costs
      // two comparisons and prunes whatever falls outside the driver range.
      if (!other_selective && !built.saw_null &&
          candidate->DistinctKeys() >= driver_rows && driver_rows >= 1000 &&
          !candidate->Has(RuntimeFilterKind::kMinMax)) {
        continue;
      }
      const ValueType col_type =
          relations[idx].schema.GetColumn(*idx_col).Type();
      record_secondary(projected_col, col_type, candidate, driver_rows,
                       coverage, other_selective, other, cand_exact);
      const bool better = !key_filter ||
                          (other_selective && !best_driver_selective) ||
                          (other_selective == best_driver_selective &&
                           (coverage < best_driver_coverage - 1e-9 ||
                            (coverage <= best_driver_coverage + 1e-9 &&
                             driver_rows < best_driver_rows)));
      if (!better) {
        continue;
      }
      key_filter = candidate;
      key_filter_type = col_type;
      key_column = projected_col;
      best_driver_rows = driver_rows;
      best_driver_selective = other_selective;
      best_driver_coverage = coverage;
      best_driver_rel = other;
      best_driver_exact = cand_exact;
    }
    if (!transitive_filter_disabled) {
      // No loaded direct partner (or only a broad one): walk the partner
      // column's equality class (built above) for a loaded transitive driver.
      // The same soundness rules as the direct pass apply -- type-matched
      // integer columns, projected-column mapping, and the near-full-domain
      // skip -- so the filter only ever prunes rows the join would reject
      // anyway.
      for (const PredicateInfo& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() != 2 || !predicate.sources.contains(idx) ||
            !IsColumnEqualityPredicate(predicate.expression) ||
            predicate.expression->AsBinaryExpression().Op() !=
                BinaryOperation::kEquals) {
          continue;
        }
        size_t other = *predicate.sources.begin();
        if (other == idx) {
          other = *std::next(predicate.sources.begin());
        }
        const BinaryExpression& binary =
            predicate.expression->AsBinaryExpression();
        const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
        const auto idx_left = LocalColumnOffset(relations[idx].schema, lhs);
        const auto idx_right = LocalColumnOffset(relations[idx].schema, rhs);
        const auto other_left = LocalColumnOffset(relations[other].schema, lhs);
        const auto other_right =
            LocalColumnOffset(relations[other].schema, rhs);
        std::optional<slot_t> idx_col;
        ColumnName partner_column;
        const bool forward = idx_left.has_value() && other_right.has_value() &&
                             !idx_right.has_value() && !other_left.has_value();
        const bool crossed = idx_right.has_value() && other_left.has_value() &&
                             !idx_left.has_value() && !other_right.has_value();
        if (forward) {
          idx_col = static_cast<slot_t>(*idx_left);
          partner_column = rhs;
        } else if (crossed) {
          idx_col = static_cast<slot_t>(*idx_right);
          partner_column = lhs;
        }
        if (!idx_col.has_value() || partner_column.Empty()) {
          continue;
        }
        const auto node_it = key_class_node.find({other, partner_column});
        if (node_it == key_class_node.end()) {
          continue;
        }
        const size_t rep = key_class_find(node_it->second);
        for (const auto& [member, member_id] : key_class_node) {
          if (key_class_find(member_id) != rep || member.rel == idx ||
              member.rel == other || !loaded[member.rel]) {
            continue;
          }
          const auto member_col =
              LocalColumnOffset(relations[member.rel].schema, member.column);
          if (!member_col.has_value()) {
            continue;
          }
          if (!SameSingleIntegerJoinKey(relations[idx].schema, {*idx_col},
                                        relations[member.rel].schema,
                                        {static_cast<slot_t>(*member_col)})) {
            continue;
          }
          const auto& proj = projections[idx];
          const auto proj_it = std::ranges::find(proj, *idx_col);
          if (proj_it == proj.end()) {
            continue;
          }
          const auto projected_col =
              static_cast<slot_t>(std::distance(proj.begin(), proj_it));
          if (relations[idx].schema.GetColumn(*idx_col).GetConstraint().ctype ==
                  Constraint::kPrimaryKey &&
              estimated_rows[idx] <= 100000 &&
              relations[member.rel].TotalRows() >= estimated_rows[idx] * 2) {
            continue;
          }
          if (key_column.has_value() && *key_column == projected_col &&
              best_driver_selective && best_driver_exact &&
              relations[member.rel].TotalRows() >= best_driver_rows) {
            continue;
          }
          const CachedDriverFilter& built =
              get_driver_filter(member.rel, static_cast<slot_t>(*member_col));
          if (!built.valid) {
            continue;
          }
          const std::shared_ptr<const RuntimeFilterGroup>& candidate =
              built.filter;
          const int64_t observed_min = built.observed_min;
          const int64_t observed_max = built.observed_max;
          const bool cand_exact = built.cand_exact;
          const size_t driver_rows = relations[member.rel].TotalRows();
          const bool member_selective =
              is_derived_selective(member.rel) ||
              (estimated_rows[member.rel] >= 100 &&
               driver_rows * 10 <= estimated_rows[member.rel] * 8) ||
              !local_predicates[member.rel].empty() ||
              (context.execution_runtime() != nullptr && [&] {
                const auto stored =
                    context.execution_runtime()->table_key_filters.find(
                        TableKeyFilterKey(statement.Sources()[member.rel]));
                return stored != context.execution_runtime()
                                     ->table_key_filters.end() &&
                       stored->second.owner == &statement;
              }());
          // Estimated pruning power: the fraction of the driver's key span
          // the filter covers.  A full-span domain (s_suppkey holds every
          // supplier key) rejects nothing however small the driver is; a
          // scattered domain (LIKE-selected p_partkey across the full key
          // range) prunes almost everything the join would reject.
          double coverage = 1.0;
          if (observed_max >= observed_min) {
            double domain_size = static_cast<double>(observed_max) -
                                 static_cast<double>(observed_min) + 1.0;
            if (member_selective && target_stats.HasValue() &&
                *idx_col < target_stats.Value()->Columns()) {
              const size_t col_distinct =
                  target_stats.Value()->Column(*idx_col).Distinct();
              if (col_distinct >= candidate->DistinctKeys() &&
                  col_distinct > 0) {
                domain_size = static_cast<double>(col_distinct);
              }
            }
            coverage =
                std::min(1.0, static_cast<double>(candidate->DistinctKeys()) /
                                  std::max(1.0, domain_size));
          }
          if (!member_selective && !built.saw_null &&
              candidate->DistinctKeys() >= driver_rows && driver_rows >= 1000 &&
              !candidate->Has(RuntimeFilterKind::kMinMax)) {
            continue;
          }
          const ValueType col_type =
              relations[idx].schema.GetColumn(*idx_col).Type();
          record_secondary(projected_col, col_type, candidate, driver_rows,
                           coverage, member_selective, member.rel, cand_exact);
          const bool better = !key_filter ||
                              (member_selective && !best_driver_selective) ||
                              (member_selective == best_driver_selective &&
                               (coverage < best_driver_coverage - 1e-9 ||
                                (coverage <= best_driver_coverage + 1e-9 &&
                                 driver_rows < best_driver_rows)));
          if (!better) {
            continue;
          }
          key_filter = candidate;
          key_filter_type = col_type;
          key_column = projected_col;
          best_driver_rows = driver_rows;
          best_driver_selective = member_selective;
          best_driver_coverage = coverage;
          best_driver_rel = member.rel;
          best_driver_exact = cand_exact;
        }
      }
    }
    if (deferred_derived[idx]) {
      const SelectStatement& inner_stmt = *statement.Sources()[idx].query;
      const SelectSource& inner_source = inner_stmt.Sources()[0];
      const std::string inner_key = TableKeyFilterKey(inner_source);
      const bool use_df = key_column.has_value() && key_filter != nullptr &&
                          context.execution_runtime() != nullptr;
      if (use_df) {
        ++context.execution_runtime()->key_filter_scans;
        context.execution_runtime()->key_filter_keys +=
            key_filter->DistinctKeys();
        if (key_filter->Has(RuntimeFilterKind::kMinMax)) {
          ++context.execution_runtime()->key_filter_minmax;
        }
        if (key_filter->Has(RuntimeFilterKind::kBloom)) {
          ++context.execution_runtime()->key_filter_bloom;
        }
        context.execution_runtime()->key_filter_bytes +=
            key_filter->MemoryBytes();
        TableKeyFilter& stored =
            context.execution_runtime()->table_key_filters[inner_key];
        stored.filter = key_filter;
        stored.type = key_filter_type;
        stored.key_count = key_filter->DistinctKeys();
        stored.column = deferred_inner_key_col[idx];
        stored.owner = &inner_stmt;
        stored.source_expr = nullptr;
        stored.exact_in = false;
      }
      StatusOr<Relation> loaded_rel =
          LoadSource(context, statement.Sources()[idx], outer, ctes);
      if (use_df && context.execution_runtime() != nullptr) {
        auto it =
            context.execution_runtime()->table_key_filters.find(inner_key);
        if (it != context.execution_runtime()->table_key_filters.end() &&
            it->second.owner == &inner_stmt) {
          context.execution_runtime()->table_key_filters.erase(it);
        }
      }
      RETURN_IF_FAIL(loaded_rel.GetStatus());
      relations[idx] = loaded_rel.MoveValue();
      RETURN_IF_FAIL(FilterRelation(context, &relations[idx],
                                    local_predicates[idx], outer, ctes));
      loaded[idx] = true;
      continue;
    }
    // A correlated build may already carry a much smaller necessary key
    // domain on another column. Do not overwrite it with a broad direct
    // join driver (e.g. a few part keys vs a region's many supplier keys).
    // Statistics choose between sound filters; they never decide an answer.
    const ExpressionBase* exact_in_source_expr = nullptr;
    std::shared_ptr<const RuntimeFilterGroup> exact_in_filter;
    if (context.execution_runtime() != nullptr) {
      const auto& filters = context.execution_runtime()->table_key_filters;
      const auto stored =
          filters.find(TableKeyFilterKey(statement.Sources()[idx]));
      if (stored != filters.end() && stored->second.owner == &statement &&
          !stored->second.empty()) {
        if (stored->second.exact_in && stored->second.source_expr != nullptr) {
          exact_in_source_expr = stored->second.source_expr;
          exact_in_filter = stored->second.filter;
        }
        const auto& proj = projections[idx];
        const auto found = std::ranges::find(proj, stored->second.column);
        if (found != proj.end() &&
            stored->second.column < relations[idx].schema.ColumnCount() &&
            relations[idx].schema.GetColumn(stored->second.column).Type() ==
                stored->second.type) {
          auto statistics = context.GetStats(statement.Sources()[idx].table);
          const auto coverage = [&](const RuntimeFilterGroup& filter,
                                    slot_t column, ValueType type) {
            if (!statistics.HasValue() || statistics.Value()->Rows() == 0 ||
                column >= statistics.Value()->Columns())
              return 1.0;
            const auto& stats = statistics.Value()->Column(column);
            if (const auto* exact = filter.ExactKeys();
                exact &&
                !relations[idx].schema.GetColumn(column).IsUnsigned()) {
              double matches = 0;
              for (int64_t key : *exact) {
                if (filter.Reject(key)) continue;
                Value value(key);
                value.type = type;
                matches += stats.EstimateEqual(value);
              }
              return std::clamp(
                  matches / static_cast<double>(statistics.Value()->Rows()),
                  0.0, 1.0);
            }
            return std::min(
                1.0, static_cast<double>(filter.DistinctKeys()) /
                         std::max(1.0, static_cast<double>(stats.Distinct())));
          };
          const auto stored_proj_col =
              static_cast<slot_t>(std::distance(proj.begin(), found));
          const double stored_cov =
              coverage(*stored->second.filter, stored->second.column,
                       stored->second.type);
          record_secondary(stored_proj_col, stored->second.type,
                           stored->second.filter, stored->second.key_count,
                           stored_cov, true, std::numeric_limits<size_t>::max(),
                           false);
          if (!key_column || !key_filter ||
              stored_cov <
                  coverage(*key_filter, proj[*key_column], key_filter_type)) {
            key_filter = stored->second.filter;
            key_filter_type = stored->second.type;
            key_column = stored_proj_col;
            best_driver_exact = stored->second.exact_in;
            best_driver_rel = std::numeric_limits<size_t>::max();
          }
        }
      }
    }
    const bool use_key_filter = key_column.has_value() && key_filter != nullptr;
    // Resolved lazily: the binding owns the group for the duration of the
    // LoadSource call below, which outlives every use inside it.
    ScanRuntimeFilter binding;
    const bool direct_base_scan = base_sources[idx] && !deferred_derived[idx] &&
                                  !statement.Sources()[idx].sample;
    if (use_key_filter && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->key_filter_scans;
      context.execution_runtime()->key_filter_keys +=
          key_filter->DistinctKeys();
      if (key_filter->Has(RuntimeFilterKind::kMinMax)) {
        ++context.execution_runtime()->key_filter_minmax;
      }
      if (key_filter->Has(RuntimeFilterKind::kBloom)) {
        ++context.execution_runtime()->key_filter_bloom;
      }
      context.execution_runtime()->key_filter_bytes +=
          key_filter->MemoryBytes();
      // Remember for later scans of this base table within this statement.
      // The stash is owner-scoped so other statements never inherit it.
      // Column is stored as the full-schema slot so other projections can
      // remap.
      const auto& proj = projections[idx];
      const slot_t full_slot = proj[*key_column];
      TableKeyFilter& stored =
          context.execution_runtime()
              ->table_key_filters[TableKeyFilterKey(statement.Sources()[idx])];
      stored.filter = key_filter;
      stored.type = key_filter_type;
      stored.key_count = key_filter->DistinctKeys();
      stored.column = full_slot;
      stored.owner = &statement;
      if (key_filter != exact_in_filter) {
        stored.source_expr = nullptr;
        stored.exact_in = false;
      }
      binding.group = key_filter;
      binding.type = key_filter_type;
      binding.column = *key_column;
      if (direct_base_scan && best_driver_exact &&
          best_driver_rel < relations.size()) {
        exact_filtered_by[idx].push_back(
            ExactFilteredEdge{*key_column, best_driver_rel});
      }
      for (const SecondaryColFilter& sf : col_filters) {
        if (sf.filter != key_filter && sf.filter != nullptr &&
            !sf.filter->empty()) {
          ScanRuntimeFilter ext;
          ext.group = sf.filter;
          ext.column = sf.projected_col;
          ext.type = sf.type;
          binding.extra.push_back(std::move(ext));
          if (direct_base_scan && sf.exact &&
              sf.driver_rel < relations.size()) {
            exact_filtered_by[idx].push_back(
                ExactFilteredEdge{sf.projected_col, sf.driver_rel});
          }
        }
      }
    } else if (context.execution_runtime() != nullptr) {
      // Fall back to a stash created earlier within this same statement (e.g.
      // an uncorrelated IN pushdown). Entries owned by other statements are
      // never applied here.
      const auto stored = context.execution_runtime()->table_key_filters.find(
          TableKeyFilterKey(statement.Sources()[idx]));
      if (stored != context.execution_runtime()->table_key_filters.end() &&
          stored->second.owner == &statement && !stored->second.empty()) {
        const auto& proj = projections[idx];
        const auto proj_it = std::ranges::find(proj, stored->second.column);
        if (proj_it != proj.end()) {
          binding.group = stored->second.filter;
          binding.type = stored->second.type;
          binding.column =
              static_cast<slot_t>(std::distance(proj.begin(), proj_it));
        }
      }
    }
    const std::vector<Expression>* load_preds_ptr = &local_predicates[idx];
    std::vector<Expression> filtered_local_preds;
    if (direct_base_scan && exact_in_source_expr != nullptr &&
        exact_in_filter != nullptr && exact_in_filter->has_dense_bits() &&
        exact_in_filter->DistinctKeys() >= 4096 &&
        (binding.group == exact_in_filter ||
         std::ranges::any_of(binding.extra, [&](const ScanRuntimeFilter& ext) {
           return ext.group == exact_in_filter;
         }))) {
      StatusOr<std::shared_ptr<Table>> b_table =
          context.GetTable(statement.Sources()[idx].table);
      if (b_table.HasValue()) {
        filtered_local_preds.reserve(local_predicates[idx].size());
        for (const Expression& lp : local_predicates[idx]) {
          if (lp.get() != exact_in_source_expr) {
            filtered_local_preds.push_back(lp);
          }
        }
        load_preds_ptr = &filtered_local_preds;
      }
    }
    ASSIGN_OR_RETURN(Relation, loaded_rel,
                     (LoadSource(context, statement.Sources()[idx], outer, ctes,
                                 &projections[idx], load_preds_ptr, &binding)));
    relations[idx] = std::move(loaded_rel);
    loaded[idx] = true;
    if (direct_base_scan && load_preds_ptr->empty() &&
        binding.group != nullptr && binding.extra.empty() &&
        binding.column < relations[idx].schema.ColumnCount() &&
        relations[idx].schema.GetColumn(binding.column).GetConstraint().ctype ==
            Constraint::kPrimaryKey &&
        !relations[idx].schema.GetColumn(binding.column).IsUnsigned() &&
        relations[idx].TotalRows() == binding.group->DistinctKeys() &&
        (binding.group->has_dense_bits() ||
         binding.group->ExactKeys() != nullptr)) {
      const uint64_t cache_key = (static_cast<uint64_t>(idx) << 32) |
                                 static_cast<uint64_t>(binding.column);
      CachedDriverFilter& entry = driver_filter_cache[cache_key];
      entry.valid = true;
      entry.filter = binding.group;
      entry.observed_min = binding.group->Lo();
      entry.observed_max = binding.group->Hi();
      entry.saw_null = false;
      entry.cand_exact = true;
      entry.distinct_keys = binding.group->DistinctKeys();
    }
  }

  // Exact PK Semi-Join Absorption: when a loaded LSTable base table only
  // projects its single Primary Key column (not referenced in SELECT,
  // GROUP BY, HAVING, ORDER BY, DistinctOn, or non-equality predicates) and
  // its join neighbour's scan already enforced an exact RuntimeFilterGroup
  // built from that PK column, the semi-join has multiplicity 1 for every
  // surviving neighbour row and 0 otherwise, so the PK table's join step can
  // be absorbed without materializing an intermediate relation (TPC-H Q3, Q8,
  // Q9, Q11).
  if (!pk_absorption_disabled && *where_fully_applied && plain_inner_sources &&
      !selects_star && relations.size() >= 2) {
    bool absorbed_any = true;
    while (absorbed_any) {
      absorbed_any = false;
      size_t active_relations = 0;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!absorbed[i]) ++active_relations;
      }
      if (active_relations <= min_remaining_relations) {
        break;
      }
      for (size_t d = 0; d < relations.size(); ++d) {
        if (absorbed[d] || !loaded[d] || relations[d].TotalRows() == 0 ||
            relations[d].schema.ColumnCount() == 0) {
          continue;
        }
        if (base_sources[d]) {
          StatusOr<std::shared_ptr<Table>> d_table =
              context.GetTable(statement.Sources()[d].table);
          if (!d_table.HasValue()) {
            continue;
          }
        }
        if (std::ranges::any_of(outer_referenced, [&](const ColumnName& cn) {
              return LocalColumnOffset(relations[d].schema, cn).has_value();
            })) {
          continue;
        }
        struct NeighbourCol {
          size_t rel{0};
          slot_t col{0};
          bool exact_filtered{false};
        };
        std::vector<NeighbourCol> neighbours;
        std::optional<slot_t> d_pk_col;
        bool predicates_ok = true;
        for (const PredicateInfo& p : predicates) {
          if (!p.resolved || !p.sources.contains(d)) {
            continue;
          }
          if (p.sources.size() == 1) {
            if (!locally_evaluable(p)) {
              predicates_ok = false;
              break;
            }
            continue;
          }
          if (p.contains_query || p.sources.size() != 2 ||
              !IsColumnEqualityPredicate(p.expression) ||
              p.expression->AsBinaryExpression().Op() !=
                  BinaryOperation::kEquals) {
            predicates_ok = false;
            break;
          }
          size_t other = *p.sources.begin();
          if (other == d) {
            other = *std::next(p.sources.begin());
          }
          if (absorbed[other] || relations[other].TotalRows() == 0) {
            predicates_ok = false;
            break;
          }
          const BinaryExpression& bin = p.expression->AsBinaryExpression();
          const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
          const ColumnName& rhs = bin.Right()->AsColumnValue().GetColumnName();
          const auto d_left = LocalColumnOffset(relations[d].schema, lhs);
          const auto d_right = LocalColumnOffset(relations[d].schema, rhs);
          const auto o_left = LocalColumnOffset(relations[other].schema, lhs);
          const auto o_right = LocalColumnOffset(relations[other].schema, rhs);
          std::optional<slot_t> d_col;
          std::optional<slot_t> o_col;
          if (d_left.has_value() && o_right.has_value() &&
              !d_right.has_value() && !o_left.has_value()) {
            d_col = static_cast<slot_t>(*d_left);
            o_col = static_cast<slot_t>(*o_right);
          } else if (d_right.has_value() && o_left.has_value() &&
                     !d_left.has_value() && !o_right.has_value()) {
            d_col = static_cast<slot_t>(*d_right);
            o_col = static_cast<slot_t>(*o_left);
          }
          if (!d_col.has_value() || !o_col.has_value() ||
              (d_pk_col.has_value() && *d_pk_col != *d_col) ||
              !SameSingleIntegerJoinKey(relations[d].schema, {*d_col},
                                        relations[other].schema, {*o_col})) {
            predicates_ok = false;
            break;
          }
          d_pk_col = *d_col;
          const bool has_exact = std::ranges::any_of(
              exact_filtered_by[other], [&](const ExactFilteredEdge& e) {
                return e.projected_col == *o_col && e.driver_rel == d;
              });
          neighbours.push_back(NeighbourCol{other, *o_col, has_exact});
        }
        if (!predicates_ok || !d_pk_col.has_value() || neighbours.empty()) {
          continue;
        }
        const Column& pk_col = relations[d].schema.GetColumn(*d_pk_col);
        if (pk_col.IsUnsigned() || (pk_col.Type() != ValueType::kInt64 &&
                                    pk_col.Type() != ValueType::kDate)) {
          continue;
        }
        if (base_sources[d]) {
          if (pk_col.GetConstraint().ctype != Constraint::kPrimaryKey) {
            continue;
          }
        } else {
          const CachedDriverFilter& d_built = get_driver_filter(d, *d_pk_col);
          if (!d_built.valid || d_built.saw_null ||
              d_built.distinct_keys != relations[d].TotalRows()) {
            continue;
          }
        }
        auto exact_it = std::ranges::find_if(
            neighbours,
            [](const NeighbourCol& nc) { return nc.exact_filtered; });
        if (exact_it == neighbours.end() &&
            !relations[neighbours[0].rel].HasSpill()) {
          const CachedDriverFilter& d_built = get_driver_filter(d, *d_pk_col);
          if (d_built.valid && d_built.cand_exact && !d_built.saw_null &&
              d_built.distinct_keys == relations[d].TotalRows() &&
              d_built.filter != nullptr) {
            const std::shared_ptr<const RuntimeFilterGroup> rf = d_built.filter;
            Relation& nrel = relations[neighbours[0].rel];
            const slot_t ncol = neighbours[0].col;
            size_t out_idx = 0;
            for (size_t ri = 0; ri < nrel.rows.size(); ++ri) {
              const Value& kv = nrel.rows[ri][ncol];
              if (kv.IsNull() || rf->Reject(kv.value.int_value)) {
                continue;
              }
              if (out_idx != ri) {
                nrel.rows[out_idx] = std::move(nrel.rows[ri]);
              }
              ++out_idx;
            }
            nrel.rows.resize(out_idx);
            invalidate_driver_filter_cache(neighbours[0].rel);
            neighbours[0].exact_filtered = true;
            exact_it = neighbours.begin();
          }
        }
        if (exact_it == neighbours.end()) {
          continue;
        }
        const NeighbourCol anchor = *exact_it;
        bool all_neighbours_covered = true;
        for (const NeighbourCol& nc : neighbours) {
          if (nc.exact_filtered ||
              (nc.rel == anchor.rel && nc.col == anchor.col)) {
            continue;
          }
          bool connected_to_anchor = false;
          for (const PredicateInfo& p : predicates) {
            if (!p.resolved || p.contains_query || p.sources.size() != 2 ||
                !p.sources.contains(anchor.rel) ||
                !p.sources.contains(nc.rel) ||
                !IsColumnEqualityPredicate(p.expression) ||
                p.expression->AsBinaryExpression().Op() !=
                    BinaryOperation::kEquals) {
              continue;
            }
            const BinaryExpression& bin = p.expression->AsBinaryExpression();
            const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                bin.Right()->AsColumnValue().GetColumnName();
            const auto a_l =
                LocalColumnOffset(relations[anchor.rel].schema, lhs);
            const auto a_r =
                LocalColumnOffset(relations[anchor.rel].schema, rhs);
            const auto n_l = LocalColumnOffset(relations[nc.rel].schema, lhs);
            const auto n_r = LocalColumnOffset(relations[nc.rel].schema, rhs);
            if ((a_l == anchor.col && n_r == nc.col && !a_r && !n_l) ||
                (a_r == anchor.col && n_l == nc.col && !a_l && !n_r)) {
              connected_to_anchor = true;
              break;
            }
          }
          if (!connected_to_anchor) {
            all_neighbours_covered = false;
            break;
          }
        }
        if (!all_neighbours_covered) {
          continue;
        }
        absorbed[d] = 1;
        for (PredicateInfo& p : predicates) {
          if (p.resolved && p.sources.contains(d)) {
            mark_applied(p.expression);
            p.resolved = false;
          }
        }
        absorbed_any = true;
        break;
      }
      if (!absorbed_any && active_relations > min_remaining_relations) {
        // 2-Column 1:1 PK Payload Column Absorption: when an LSTable base table
        // `d` has two active columns `(pk_col, payload_col)`, either replace
        // `other.fk_col` in-place with `d.payload_col` (when `other.fk_col` is
        // not needed elsewhere) or append `d.payload_col` to a small neighbour
        // relation in the same key equivalence class (TPC-H Q5, Q7, Q8, Q9,
        // Q10).
        for (size_t d = 0; d < relations.size(); ++d) {
          if (absorbed[d] || !base_sources[d] || deferred_derived[d] ||
              !loaded[d] || relations[d].HasSpill() ||
              relations[d].TotalRows() == 0 ||
              relations[d].schema.ColumnCount() < 2) {
            continue;
          }
          StatusOr<std::shared_ptr<Table>> d_table =
              context.GetTable(statement.Sources()[d].table);
          if (!d_table.HasValue()) {
            continue;
          }
          struct JoinEdge {
            slot_t d_col{0};
            size_t other{0};
            slot_t o_col{0};
          };
          std::vector<JoinEdge> edges;
          bool predicates_ok = true;
          for (const PredicateInfo& p : predicates) {
            if (!p.resolved || !p.sources.contains(d)) {
              continue;
            }
            if (p.sources.size() == 1) {
              if (!locally_evaluable(p)) {
                predicates_ok = false;
                break;
              }
              continue;
            }
            if (p.contains_query || p.sources.size() != 2 ||
                !IsColumnEqualityPredicate(p.expression) ||
                p.expression->AsBinaryExpression().Op() !=
                    BinaryOperation::kEquals) {
              predicates_ok = false;
              break;
            }
            size_t other = *p.sources.begin();
            if (other == d) {
              other = *std::next(p.sources.begin());
            }
            if (absorbed[other] || relations[other].HasSpill() ||
                relations[other].TotalRows() == 0) {
              predicates_ok = false;
              break;
            }
            const BinaryExpression& bin = p.expression->AsBinaryExpression();
            const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                bin.Right()->AsColumnValue().GetColumnName();
            const auto d_left = LocalColumnOffset(relations[d].schema, lhs);
            const auto d_right = LocalColumnOffset(relations[d].schema, rhs);
            const auto o_left = LocalColumnOffset(relations[other].schema, lhs);
            const auto o_right =
                LocalColumnOffset(relations[other].schema, rhs);
            std::optional<slot_t> d_col;
            std::optional<slot_t> o_col;
            if (d_left.has_value() && o_right.has_value() &&
                !d_right.has_value() && !o_left.has_value()) {
              d_col = static_cast<slot_t>(*d_left);
              o_col = static_cast<slot_t>(*o_right);
            } else if (d_right.has_value() && o_left.has_value() &&
                       !d_left.has_value() && !o_right.has_value()) {
              d_col = static_cast<slot_t>(*d_right);
              o_col = static_cast<slot_t>(*o_left);
            }
            if (!d_col.has_value() || !o_col.has_value()) {
              predicates_ok = false;
              break;
            }
            edges.push_back(JoinEdge{*d_col, other, *o_col});
          }
          if (!predicates_ok || edges.empty()) {
            continue;
          }
          std::vector<slot_t> active_cols;
          const auto add_active_col = [&](slot_t c) {
            if (std::ranges::find(active_cols, c) == active_cols.end()) {
              active_cols.push_back(c);
            }
          };
          for (const ColumnName& cn : outer_referenced) {
            if (const auto off = LocalColumnOffset(relations[d].schema, cn)) {
              add_active_col(static_cast<slot_t>(*off));
            }
          }
          for (const JoinEdge& e : edges) {
            add_active_col(e.d_col);
          }
          if (active_cols.size() != 2) {
            continue;
          }
          std::optional<slot_t> d_pk_col;
          std::optional<slot_t> d_payload_col;
          std::optional<size_t> target_rel;
          std::optional<slot_t> target_col;
          bool append_payload = false;
          for (size_t try_idx = 0; try_idx < 2; ++try_idx) {
            const slot_t cand_pk = active_cols[try_idx];
            const slot_t cand_payload = active_cols[1 - try_idx];
            const Column& pk_col = relations[d].schema.GetColumn(cand_pk);
            if (pk_col.GetConstraint().ctype != Constraint::kPrimaryKey ||
                pk_col.IsUnsigned() ||
                (pk_col.Type() != ValueType::kInt64 &&
                 pk_col.Type() != ValueType::kDate)) {
              continue;
            }
            bool pk_in_outer = false;
            for (const ColumnName& cn : outer_referenced) {
              if (LocalColumnOffset(relations[d].schema, cn) ==
                  static_cast<int>(cand_pk)) {
                pk_in_outer = true;
                break;
              }
            }
            if (pk_in_outer) {
              continue;
            }
            std::optional<size_t> cand_t_rel;
            std::optional<slot_t> cand_t_col;
            bool edges_valid = true;
            bool has_payload_edges = false;
            std::vector<JoinEdge> pk_edges;
            for (const JoinEdge& e : edges) {
              if (e.d_col == cand_pk) {
                if (!SameSingleIntegerJoinKey(relations[d].schema, {cand_pk},
                                              relations[e.other].schema,
                                              {e.o_col})) {
                  edges_valid = false;
                  break;
                }
                pk_edges.push_back(e);
              } else if (e.d_col == cand_payload) {
                has_payload_edges = true;
              } else {
                edges_valid = false;
                break;
              }
            }
            if (edges_valid && !pk_edges.empty()) {
              for (const JoinEdge& e : pk_edges) {
                if (!cand_t_rel.has_value() ||
                    relations[e.other].rows.size() <
                        relations[*cand_t_rel].rows.size()) {
                  cand_t_rel = e.other;
                  cand_t_col = e.o_col;
                }
              }
              for (const JoinEdge& e : pk_edges) {
                if (e.other == *cand_t_rel && e.o_col == *cand_t_col) {
                  continue;
                }
                bool connected_to_cand = false;
                for (const PredicateInfo& p : predicates) {
                  if (!p.resolved || p.contains_query ||
                      p.sources.size() != 2 ||
                      !p.sources.contains(*cand_t_rel) ||
                      !p.sources.contains(e.other) ||
                      !IsColumnEqualityPredicate(p.expression) ||
                      p.expression->AsBinaryExpression().Op() !=
                          BinaryOperation::kEquals) {
                    continue;
                  }
                  const BinaryExpression& bin =
                      p.expression->AsBinaryExpression();
                  const ColumnName& lhs =
                      bin.Left()->AsColumnValue().GetColumnName();
                  const ColumnName& rhs =
                      bin.Right()->AsColumnValue().GetColumnName();
                  const auto t_l =
                      LocalColumnOffset(relations[*cand_t_rel].schema, lhs);
                  const auto t_r =
                      LocalColumnOffset(relations[*cand_t_rel].schema, rhs);
                  const auto o_l =
                      LocalColumnOffset(relations[e.other].schema, lhs);
                  const auto o_r =
                      LocalColumnOffset(relations[e.other].schema, rhs);
                  if ((t_l == static_cast<int>(*cand_t_col) &&
                       o_r == static_cast<int>(e.o_col) && !t_r && !o_l) ||
                      (t_r == static_cast<int>(*cand_t_col) &&
                       o_l == static_cast<int>(e.o_col) && !t_l && !o_r)) {
                    connected_to_cand = true;
                    break;
                  }
                }
                if (!connected_to_cand) {
                  edges_valid = false;
                  break;
                }
              }
            }
            if (!edges_valid || !cand_t_rel.has_value() ||
                !cand_t_col.has_value()) {
              continue;
            }
            if (has_payload_edges) {
              for (const JoinEdge& e : edges) {
                if (e.d_col == cand_payload && e.other == *cand_t_rel) {
                  edges_valid = false;
                  break;
                }
              }
              if (!edges_valid) {
                continue;
              }
            }
            bool t_col_referenced = pk_edges.size() > 1;
            for (const ColumnName& cn : outer_referenced) {
              if (LocalColumnOffset(relations[*cand_t_rel].schema, cn) ==
                  static_cast<int>(*cand_t_col)) {
                t_col_referenced = true;
                break;
              }
            }
            if (!t_col_referenced) {
              for (const PredicateInfo& p : predicates) {
                if (!p.resolved || p.sources.contains(d) ||
                    !p.sources.contains(*cand_t_rel) || !p.expression) {
                  continue;
                }
                for (const ColumnName& cn : p.expression->TouchedColumns()) {
                  if (LocalColumnOffset(relations[*cand_t_rel].schema, cn) ==
                      static_cast<int>(*cand_t_col)) {
                    t_col_referenced = true;
                    break;
                  }
                }
                if (t_col_referenced) {
                  break;
                }
              }
            }
            if (!t_col_referenced) {
              d_pk_col = cand_pk;
              d_payload_col = cand_payload;
              target_rel = *cand_t_rel;
              target_col = *cand_t_col;
              append_payload = false;
              break;
            }
            if (!has_payload_edges && relations[d].rows.size() <= 65536) {
              std::optional<size_t> best_append_rel;
              std::optional<slot_t> best_append_col;
              if (relations[*cand_t_rel].rows.size() <= 131072) {
                best_append_rel = *cand_t_rel;
                best_append_col = *cand_t_col;
              }
              for (const PredicateInfo& p : predicates) {
                if (!p.resolved || p.contains_query || p.sources.size() != 2 ||
                    !p.sources.contains(*cand_t_rel) ||
                    !IsColumnEqualityPredicate(p.expression) ||
                    p.expression->AsBinaryExpression().Op() !=
                        BinaryOperation::kEquals) {
                  continue;
                }
                size_t alt = *p.sources.begin();
                if (alt == *cand_t_rel) {
                  alt = *std::next(p.sources.begin());
                }
                if (alt == d || absorbed[alt] || relations[alt].HasSpill() ||
                    relations[alt].rows.empty() ||
                    relations[alt].rows.size() > 131072) {
                  continue;
                }
                const BinaryExpression& bin =
                    p.expression->AsBinaryExpression();
                const ColumnName& lhs =
                    bin.Left()->AsColumnValue().GetColumnName();
                const ColumnName& rhs =
                    bin.Right()->AsColumnValue().GetColumnName();
                const auto t_l =
                    LocalColumnOffset(relations[*cand_t_rel].schema, lhs);
                const auto t_r =
                    LocalColumnOffset(relations[*cand_t_rel].schema, rhs);
                const auto a_l = LocalColumnOffset(relations[alt].schema, lhs);
                const auto a_r = LocalColumnOffset(relations[alt].schema, rhs);
                std::optional<slot_t> a_col;
                if (t_l == *cand_t_col && a_r.has_value() && !t_r && !a_l) {
                  a_col = static_cast<slot_t>(*a_r);
                } else if (t_r == *cand_t_col && a_l.has_value() && !t_l &&
                           !a_r) {
                  a_col = static_cast<slot_t>(*a_l);
                }
                if (!a_col.has_value() ||
                    !SameSingleIntegerJoinKey(relations[d].schema, {cand_pk},
                                              relations[alt].schema,
                                              {*a_col})) {
                  continue;
                }
                if (!best_append_rel.has_value() ||
                    relations[alt].rows.size() <
                        relations[*best_append_rel].rows.size()) {
                  best_append_rel = alt;
                  best_append_col = *a_col;
                }
              }
              if (best_append_rel.has_value() && best_append_col.has_value()) {
                d_pk_col = cand_pk;
                d_payload_col = cand_payload;
                target_rel = *best_append_rel;
                target_col = *best_append_col;
                append_payload = true;
                break;
              }
            }
          }
          if (!d_pk_col.has_value() || !d_payload_col.has_value() ||
              !target_rel.has_value() || !target_col.has_value()) {
            continue;
          }
          const Column& payload_col =
              relations[d].schema.GetColumn(*d_payload_col);
          Relation& tgt = relations[*target_rel];
          bool name_collision = false;
          for (size_t ci = 0; ci < tgt.schema.ColumnCount(); ++ci) {
            if (!append_payload && ci == *target_col) {
              continue;
            }
            if (IdentifierEquals(tgt.schema.GetColumn(ci).Name().name,
                                 payload_col.Name().name)) {
              name_collision = true;
              break;
            }
          }
          if (name_collision) {
            continue;
          }
          int64_t min_k = std::numeric_limits<int64_t>::max();
          int64_t max_k = std::numeric_limits<int64_t>::min();
          for (const Row& r : relations[d].rows) {
            const Value& kv = r[*d_pk_col];
            if (kv.IsNull()) continue;
            min_k = std::min(min_k, kv.value.int_value);
            max_k = std::max(max_k, kv.value.int_value);
          }
          if (min_k > max_k) {
            continue;
          }
          const uint64_t span =
              static_cast<uint64_t>(max_k) - static_cast<uint64_t>(min_k) + 1;
          const ValueType p_type = payload_col.Type();
          const bool can_small_ptr_path =
              relations[d].rows.size() <= 65536 && span <= 262144ULL;
          if (append_payload ||
              (can_small_ptr_path &&
               (p_type != ValueType::kInt64 && p_type != ValueType::kDate))) {
            if (!can_small_ptr_path) {
              continue;
            }
            std::vector<const Value*> dense_val_ptrs(static_cast<size_t>(span),
                                                     nullptr);
            for (const Row& r : relations[d].rows) {
              const Value& kv = r[*d_pk_col];
              if (kv.IsNull()) continue;
              const auto off = static_cast<size_t>(
                  static_cast<uint64_t>(kv.value.int_value) -
                  static_cast<uint64_t>(min_k));
              dense_val_ptrs[off] = &r[*d_payload_col];
            }
            const size_t nworkers = std::max<size_t>(
                size_t{1},
                std::min<size_t>(
                    {size_t{16},
                     static_cast<size_t>(std::thread::hardware_concurrency()),
                     tgt.rows.size() / 16384}));
            if (nworkers <= 1) {
              size_t out_idx = 0;
              for (size_t ri = 0; ri < tgt.rows.size(); ++ri) {
                const Value& cell = tgt.rows[ri].values_[*target_col];
                if (cell.IsNull() || cell.value.int_value < min_k ||
                    cell.value.int_value > max_k) {
                  continue;
                }
                const auto off = static_cast<size_t>(
                    static_cast<uint64_t>(cell.value.int_value) -
                    static_cast<uint64_t>(min_k));
                const Value* pv = dense_val_ptrs[off];
                if (pv == nullptr) {
                  continue;
                }
                if (append_payload) {
                  tgt.rows[ri].values_.push_back(*pv);
                } else {
                  tgt.rows[ri].values_[*target_col] = *pv;
                }
                if (out_idx != ri) {
                  tgt.rows[out_idx] = std::move(tgt.rows[ri]);
                }
                ++out_idx;
              }
              tgt.rows.resize(out_idx);
            } else {
              std::vector<uint8_t> worker_had_unmatched(nworkers, 0);
              const size_t total_rows = tgt.rows.size();
              const size_t chunk = (total_rows + nworkers - 1) / nworkers;
              auto run_ptr_shard = [&](size_t w) {
                const size_t lo = w * chunk;
                const size_t hi = std::min(total_rows, lo + chunk);
                uint8_t had_unmatched = 0;
                for (size_t ri = lo; ri < hi; ++ri) {
                  Value& cell = tgt.rows[ri].values_[*target_col];
                  if (cell.IsNull() || cell.value.int_value < min_k ||
                      cell.value.int_value > max_k) [[unlikely]] {
                    cell.type = ValueType::kNull;
                    cell.SetCollation(1);
                    had_unmatched = 1;
                    continue;
                  }
                  const auto off = static_cast<size_t>(
                      static_cast<uint64_t>(cell.value.int_value) -
                      static_cast<uint64_t>(min_k));
                  const Value* pv = dense_val_ptrs[off];
                  if (pv == nullptr) [[unlikely]] {
                    cell.type = ValueType::kNull;
                    cell.SetCollation(1);
                    had_unmatched = 1;
                    continue;
                  }
                  if (append_payload) {
                    tgt.rows[ri].values_.push_back(*pv);
                  } else {
                    cell = *pv;
                  }
                }
                worker_had_unmatched[w] = had_unmatched;
              };
              std::vector<std::jthread> threads;
              threads.reserve(nworkers - 1);
              for (size_t w = 1; w < nworkers; ++w) {
                threads.emplace_back([&, w] { run_ptr_shard(w); });
              }
              run_ptr_shard(0);
              threads.clear();
              if (std::ranges::any_of(worker_had_unmatched,
                                      [](uint8_t u) { return u != 0; })) {
                size_t out_idx = 0;
                for (size_t ri = 0; ri < tgt.rows.size(); ++ri) {
                  const Value& cell = tgt.rows[ri].values_[*target_col];
                  if (cell.IsNull() && cell.Collation() == 1) {
                    continue;
                  }
                  if (out_idx != ri) {
                    tgt.rows[out_idx] = std::move(tgt.rows[ri]);
                  }
                  ++out_idx;
                }
                tgt.rows.resize(out_idx);
              }
            }
          } else {
            if (tgt.rows.size() < 16384 || span > 16777216ULL ||
                (p_type != ValueType::kDate && p_type != ValueType::kInt64)) {
              continue;
            }
            constexpr int32_t kNullPayloadI32 =
                std::numeric_limits<int32_t>::min();
            if (p_type == ValueType::kInt64) {
              bool fits_i32 = true;
              for (const Row& r : relations[d].rows) {
                const Value& pv = r[*d_payload_col];
                if (pv.IsNull()) continue;
                if (pv.value.int_value <= kNullPayloadI32 ||
                    pv.value.int_value > std::numeric_limits<int32_t>::max()) {
                  fits_i32 = false;
                  break;
                }
              }
              if (!fits_i32) {
                continue;
              }
            }
            const bool p_unsigned = payload_col.IsUnsigned();
            const size_t nworkers = std::max<size_t>(
                size_t{1},
                std::min<size_t>(
                    {size_t{16},
                     static_cast<size_t>(std::thread::hardware_concurrency()),
                     tgt.rows.size() / 16384}));
            const size_t d_rows = relations[d].rows.size();
            bool d_monotonic = d_rows >= 64 && span > d_rows * 8;
            if (d_monotonic) {
              int64_t prev_k = std::numeric_limits<int64_t>::min();
              for (const Row& r : relations[d].rows) {
                const Value& kv = r[*d_pk_col];
                if (kv.IsNull() || kv.value.int_value <= prev_k) {
                  d_monotonic = false;
                  break;
                }
                prev_k = kv.value.int_value;
              }
            }
            if (d_monotonic) {
              std::vector<int64_t> mono_keys(d_rows);
              std::vector<int32_t> mono_payloads(d_rows);
              constexpr size_t kNumBuckets = 4096;
              std::array<uint32_t, kNumBuckets> bucket_lo;
              std::array<uint32_t, kNumBuckets> bucket_hi;
              bucket_lo.fill(static_cast<uint32_t>(d_rows));
              bucket_hi.fill(0);
              const auto bucket_of = [&](int64_t k) -> size_t {
                return static_cast<size_t>(
                    ((static_cast<uint64_t>(k) - static_cast<uint64_t>(min_k)) *
                     kNumBuckets) /
                    span);
              };
              for (size_t di = 0; di < d_rows; ++di) {
                const Row& r = relations[d].rows[di];
                const int64_t k = r[*d_pk_col].value.int_value;
                mono_keys[di] = k;
                const Value& pv = r[*d_payload_col];
                mono_payloads[di] =
                    pv.IsNull() ? kNullPayloadI32
                                : static_cast<int32_t>(pv.value.int_value);
                const size_t b = bucket_of(k);
                if (di < bucket_lo[b]) {
                  bucket_lo[b] = static_cast<uint32_t>(di);
                }
                bucket_hi[b] = static_cast<uint32_t>(di + 1);
              }
              std::vector<uint8_t> worker_had_unmatched(nworkers, 0);
              {
                std::vector<std::jthread> threads;
                threads.reserve(nworkers);
                for (size_t w = 0; w < nworkers; ++w) {
                  threads.emplace_back([&, w] {
                    const size_t r_begin = (tgt.rows.size() * w) / nworkers;
                    const size_t r_end = (tgt.rows.size() * (w + 1)) / nworkers;
                    uint8_t had_unmatched = 0;
                    size_t cursor = 0;
                    for (size_t ri = r_begin; ri < r_end; ++ri) {
                      Value& cell = tgt.rows[ri].values_[*target_col];
                      if (cell.IsNull() || cell.value.int_value < min_k ||
                          cell.value.int_value > max_k) [[unlikely]] {
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      const int64_t k = cell.value.int_value;
                      size_t match_idx = d_rows;
                      if (cursor < d_rows && mono_keys[cursor] == k) {
                        match_idx = cursor;
                      } else if (cursor + 1 < d_rows &&
                                 mono_keys[cursor + 1] == k) {
                        match_idx = ++cursor;
                      } else {
                        const size_t b = bucket_of(k);
                        const uint32_t lo = bucket_lo[b];
                        const uint32_t hi = bucket_hi[b];
                        if (lo < hi) {
                          const auto it =
                              std::lower_bound(mono_keys.begin() + lo,
                                               mono_keys.begin() + hi, k);
                          if (it != mono_keys.begin() + hi && *it == k) {
                            match_idx =
                                static_cast<size_t>(it - mono_keys.begin());
                            cursor = match_idx;
                          }
                        }
                      }
                      if (match_idx == d_rows) [[unlikely]] {
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      const int32_t p_i32 = mono_payloads[match_idx];
                      if (p_i32 == kNullPayloadI32) [[unlikely]] {
                        cell = Value();
                      } else {
                        cell.type = p_type;
                        cell.SetCollation(0);
                        if (p_unsigned) {
                          cell.SetUnsigned();
                        }
                        cell.value.int_value = static_cast<int64_t>(p_i32);
                      }
                    }
                    worker_had_unmatched[w] = had_unmatched;
                  });
                }
              }
              if (std::ranges::any_of(worker_had_unmatched,
                                      [](uint8_t u) { return u != 0; })) {
                size_t out_idx = 0;
                for (size_t ri = 0; ri < tgt.rows.size(); ++ri) {
                  const Value& cell = tgt.rows[ri].values_[*target_col];
                  if (cell.IsNull() && cell.Collation() == 1) {
                    continue;
                  }
                  if (out_idx != ri) {
                    tgt.rows[out_idx] = std::move(tgt.rows[ri]);
                  }
                  ++out_idx;
                }
                tgt.rows.resize(out_idx);
              }
            } else {
              static std::mutex dense_payload_mu;
              static std::vector<uint64_t> shared_dense_payload;
              static uint32_t shared_epoch = 0;
              std::unique_lock<std::mutex> payload_lock(dense_payload_mu,
                                                        std::try_to_lock);
              if (!payload_lock.owns_lock()) {
                continue;
              }
              if (shared_dense_payload.size() < static_cast<size_t>(span)) {
                shared_dense_payload.resize(static_cast<size_t>(span), 0ULL);
              }
              ++shared_epoch;
              if (shared_epoch == 0) {
                std::ranges::fill(shared_dense_payload, 0ULL);
                shared_epoch = 1;
              }
              const uint64_t epoch_tag = static_cast<uint64_t>(shared_epoch)
                                         << 32;
              {
                std::vector<std::jthread> threads;
                threads.reserve(nworkers);
                for (size_t w = 0; w < nworkers; ++w) {
                  threads.emplace_back([&, w] {
                    const size_t d_begin = (d_rows * w) / nworkers;
                    const size_t d_end = (d_rows * (w + 1)) / nworkers;
                    for (size_t di = d_begin; di < d_end; ++di) {
                      const Row& r = relations[d].rows[di];
                      const Value& kv = r[*d_pk_col];
                      if (kv.IsNull()) continue;
                      const auto off = static_cast<size_t>(
                          static_cast<uint64_t>(kv.value.int_value) -
                          static_cast<uint64_t>(min_k));
                      const Value& pv = r[*d_payload_col];
                      const int32_t p_i32 =
                          pv.IsNull()
                              ? kNullPayloadI32
                              : static_cast<int32_t>(pv.value.int_value);
                      shared_dense_payload[off] =
                          epoch_tag | static_cast<uint32_t>(p_i32);
                    }
                  });
                }
              }
              std::vector<uint8_t> worker_had_unmatched(nworkers, 0);
              {
                std::vector<std::jthread> threads;
                threads.reserve(nworkers);
                for (size_t w = 0; w < nworkers; ++w) {
                  threads.emplace_back([&, w] {
                    const size_t r_begin = (tgt.rows.size() * w) / nworkers;
                    const size_t r_end = (tgt.rows.size() * (w + 1)) / nworkers;
                    uint8_t had_unmatched = 0;
                    for (size_t ri = r_begin; ri < r_end; ++ri) {
                      Value& cell = tgt.rows[ri].values_[*target_col];
                      if (cell.IsNull() || cell.value.int_value < min_k ||
                          cell.value.int_value > max_k) [[unlikely]] {
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      const auto off = static_cast<size_t>(
                          static_cast<uint64_t>(cell.value.int_value) -
                          static_cast<uint64_t>(min_k));
                      const uint64_t word = shared_dense_payload[off];
                      if ((word & 0xFFFFFFFF00000000ULL) != epoch_tag)
                          [[unlikely]] {
                        cell.type = ValueType::kNull;
                        cell.SetCollation(1);
                        had_unmatched = 1;
                        continue;
                      }
                      const auto p_i32 =
                          static_cast<int32_t>(static_cast<uint32_t>(word));
                      if (p_i32 == kNullPayloadI32) [[unlikely]] {
                        cell = Value();
                      } else {
                        cell.type = p_type;
                        cell.SetCollation(0);
                        if (p_unsigned) {
                          cell.SetUnsigned();
                        }
                        cell.value.int_value = static_cast<int64_t>(p_i32);
                      }
                    }
                    worker_had_unmatched[w] = had_unmatched;
                  });
                }
              }
              if (std::ranges::any_of(worker_had_unmatched,
                                      [](uint8_t u) { return u != 0; })) {
                size_t out_idx = 0;
                for (size_t ri = 0; ri < tgt.rows.size(); ++ri) {
                  const Value& cell = tgt.rows[ri].values_[*target_col];
                  if (cell.IsNull() && cell.Collation() == 1) {
                    continue;
                  }
                  if (out_idx != ri) {
                    tgt.rows[out_idx] = std::move(tgt.rows[ri]);
                  }
                  ++out_idx;
                }
                tgt.rows.resize(out_idx);
              }
            }
          }
          Column new_payload_col(payload_col.Name(), payload_col.Type(),
                                 Constraint(Constraint::kNothing));
          new_payload_col.SetUnsigned(payload_col.IsUnsigned());
          std::vector<Column> new_cols;
          new_cols.reserve(tgt.schema.ColumnCount() + (append_payload ? 1 : 0));
          for (size_t ci = 0; ci < tgt.schema.ColumnCount(); ++ci) {
            if (!append_payload && ci == *target_col) {
              new_cols.push_back(new_payload_col);
            } else {
              new_cols.push_back(tgt.schema.GetColumn(ci));
            }
          }
          if (append_payload) {
            new_cols.push_back(std::move(new_payload_col));
          }
          tgt.schema = Schema("", std::move(new_cols));
          driver_filter_cache.clear();
          if (!append_payload) {
            for (const ExactFilteredEdge& edge : exact_filtered_by[d]) {
              if (edge.projected_col == *d_payload_col) {
                exact_filtered_by[*target_rel].push_back(
                    ExactFilteredEdge{*target_col, edge.driver_rel});
              }
            }
            for (size_t r = 0; r < exact_filtered_by.size(); ++r) {
              for (ExactFilteredEdge& edge : exact_filtered_by[r]) {
                if (edge.driver_rel == d) {
                  edge.driver_rel = std::numeric_limits<size_t>::max();
                }
              }
            }
          }
          relations[d].rows.clear();
          relations[d].rows.shrink_to_fit();
          absorbed[d] = 1;
          for (PredicateInfo& p : predicates) {
            if (!p.resolved || !p.sources.contains(d)) {
              continue;
            }
            if (p.sources.size() == 1) {
              mark_applied(p.expression);
              p.resolved = false;
              continue;
            }
            bool touches_pk = false;
            if (p.expression) {
              for (const ColumnName& cn : p.expression->TouchedColumns()) {
                if (LocalColumnOffset(relations[d].schema, cn) ==
                    static_cast<int>(*d_pk_col)) {
                  touches_pk = true;
                  break;
                }
              }
            }
            if (touches_pk) {
              mark_applied(p.expression);
              p.resolved = false;
            } else {
              p.sources.erase(d);
              p.sources.insert(*target_rel);
            }
          }
          absorbed_any = true;
          break;
        }
      }
    }
  }

  // Pick the best starting table.  The naive approach (smallest row
  // count) can be suboptimal for star-like join graphs where the
  // smallest table is a leaf with only one connection.  Simulate the
  // greedy join from every candidate start and pick the one that
  // yields the lowest estimated peak intermediate row count.
  size_t first = 0;
  size_t best_peak = std::numeric_limits<size_t>::max();
  size_t best_total_sim = std::numeric_limits<size_t>::max();
  size_t best_start_rows = std::numeric_limits<size_t>::max();
  for (size_t start = 0; start < relations.size(); ++start) {
    if (absorbed[start]) {
      continue;
    }
    size_t sim_current = relations[start].TotalRows();
    size_t sim_peak = sim_current;
    size_t sim_total = 0;
    std::unordered_set<size_t> sim_joined{start};
    std::unordered_set<size_t> sim_remaining;
    for (size_t i = 0; i < relations.size(); ++i) {
      if (i != start && !absorbed[i]) {
        sim_remaining.insert(i);
      }
    }
    while (!sim_remaining.empty()) {
      size_t sim_next = *sim_remaining.begin();
      size_t sim_est = std::numeric_limits<size_t>::max();
      bool sim_next_connected = false;
      for (size_t cand : sim_remaining) {
        std::unordered_set<size_t> after = sim_joined;
        after.insert(cand);
        size_t app_count = 0;
        for (const PredicateInfo& p : predicates) {
          if (!p.resolved || p.contains_query || p.sources.size() < 2 ||
              !p.sources.contains(cand) || !IsSubset(p.sources, after)) {
            continue;
          }
          ++app_count;
        }
        const bool connected = app_count > 0;
        const size_t est =
            connected ? std::min(sim_current, relations[cand].TotalRows())
                      : sim_current * relations[cand].TotalRows();
        const bool cheaper =
            est < sim_est ||
            (est == sim_est &&
             relations[cand].TotalRows() < relations[sim_next].TotalRows());
        if ((connected && !sim_next_connected) ||
            (connected == sim_next_connected && cheaper)) {
          sim_next = cand;
          sim_est = est;
          sim_next_connected = connected;
        }
      }
      sim_current = sim_est;
      sim_peak = std::max(sim_peak, sim_current);
      sim_total += sim_current;
      sim_joined.insert(sim_next);
      sim_remaining.erase(sim_next);
    }
    const size_t start_rows = relations[start].TotalRows();
    if (sim_peak < best_peak ||
        (sim_peak == best_peak &&
         (sim_total < best_total_sim ||
          (sim_total == best_total_sim && start_rows < best_start_rows)))) {
      best_peak = sim_peak;
      best_total_sim = sim_total;
      best_start_rows = start_rows;
      first = start;
    }
  }
  Relation result = std::move(relations[first]);
  std::unordered_set<size_t> joined{first};
  std::unordered_set<size_t> remaining;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (i != first && !absorbed[i]) {
      remaining.insert(i);
    }
  }

  // TINYLAMB_LEGACY_PLANNER=1 restores the per-call estimation behavior
  // (fresh maps, exact full-relation sweeps) for A/B comparisons.
  static const bool legacy_planner =
      std::getenv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>>
      frequency_cache;
  const auto collect_deduped_applicable =
      [&](size_t target_rel, const std::unordered_set<size_t>& after_set,
          std::vector<Expression>* out_applicable, bool mark) {
        std::vector<size_t> seen_target_cols;
        for (const PredicateInfo& predicate : predicates) {
          if (!predicate.resolved || predicate.contains_query ||
              predicate.sources.size() < 2 ||
              !predicate.sources.contains(target_rel) ||
              !IsSubset(predicate.sources, after_set)) {
            continue;
          }
          if (mark) {
            mark_applied(predicate.expression);
          }
          if (!transitive_filter_disabled && predicate.sources.size() == 2 &&
              IsColumnEqualityPredicate(predicate.expression) &&
              predicate.expression->AsBinaryExpression().Op() ==
                  BinaryOperation::kEquals) {
            const BinaryExpression& bin =
                predicate.expression->AsBinaryExpression();
            const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                bin.Right()->AsColumnValue().GetColumnName();
            const auto t_l =
                LocalColumnOffset(relations[target_rel].schema, lhs);
            const auto t_r =
                LocalColumnOffset(relations[target_rel].schema, rhs);
            std::optional<size_t> t_col;
            if (t_l.has_value() && !t_r.has_value()) {
              t_col = *t_l;
            } else if (t_r.has_value() && !t_l.has_value()) {
              t_col = *t_r;
            }
            if (t_col.has_value()) {
              if (std::ranges::find(seen_target_cols, *t_col) !=
                  seen_target_cols.end()) {
                continue;
              }
              seen_target_cols.push_back(*t_col);
            }
          }
          out_applicable->push_back(predicate.expression);
        }
      };
  while (!remaining.empty()) {
    if (remaining.size() == 2 && fusion != nullptr && fusion->eligible &&
        *where_fully_applied && join_residual.empty() && !result.HasSpill() &&
        !fusion->group_keys.empty()) {
      auto it = remaining.begin();
      const size_t r1 = *it;
      const size_t r2 = *std::next(it);
      const auto count_varchar_cols = [](const Schema& s) {
        size_t n = 0;
        for (size_t c = 0; c < s.ColumnCount(); ++c) {
          if (s.GetColumn(c).Type() == ValueType::kVarChar) ++n;
        }
        return n;
      };
      const bool group_keys_in_result =
          std::ranges::all_of(fusion->group_keys, [&](const Expression& gk) {
            if (!gk) return false;
            const auto cols = gk->TouchedColumns();
            return !cols.empty() &&
                   std::ranges::all_of(cols, [&](const ColumnName& cn) {
                     return LocalColumnOffset(result.schema, cn).has_value();
                   });
          });
      if (group_keys_in_result && count_varchar_cols(result.schema) >= 2 &&
          count_varchar_cols(relations[r1].schema) == 0 &&
          count_varchar_cols(relations[r2].schema) == 0 &&
          !relations[r1].HasSpill() && !relations[r2].HasSpill()) {
        std::unordered_set<size_t> pair_set{r1, r2};
        std::vector<Expression> pair_applicable;
        for (const PredicateInfo& p : predicates) {
          if (p.resolved && !p.contains_query && p.sources.size() == 2 &&
              p.sources.contains(r1) && p.sources.contains(r2)) {
            pair_applicable.push_back(p.expression);
          }
        }
        if (!pair_applicable.empty()) {
          std::unordered_set<size_t> after_r1 = joined;
          after_r1.insert(r1);
          std::vector<Expression> app_r1;
          collect_deduped_applicable(r1, after_r1, &app_r1, false);
          std::unordered_set<size_t> after_r2 = joined;
          after_r2.insert(r2);
          std::vector<Expression> app_r2;
          collect_deduped_applicable(r2, after_r2, &app_r2, false);
          const bool r1_expands =
              !app_r1.empty() && relations[r1].TotalRows() > result.TotalRows();
          const bool r2_expands =
              !app_r2.empty() && relations[r2].TotalRows() > result.TotalRows();
          if (r1_expands || r2_expands) {
            for (const Expression& expr : pair_applicable) {
              mark_applied(expr);
            }
            ASSIGN_OR_RETURN(Relation, pair_joined,
                             (InnerJoin(context, std::move(relations[r1]),
                                        std::move(relations[r2]),
                                        pair_applicable, outer, ctes)));
            relations[r1] = std::move(pair_joined);
            for (PredicateInfo& p : predicates) {
              if (!p.resolved) continue;
              if (p.sources.contains(r2)) {
                p.sources.erase(r2);
                p.sources.insert(r1);
              }
              if (p.sources.size() == 1 && p.sources.contains(r1)) {
                p.resolved = false;
              }
            }
            remaining.erase(r2);
          }
        }
      }
    }
    size_t next = *remaining.begin();
    size_t next_estimate = std::numeric_limits<size_t>::max();
    bool next_connected = false;
    for (size_t candidate : remaining) {
      // There is no join-order choice at the last step. A frequency sweep
      // of a spilled fact table cannot change the selected input and can
      // cost another complete spill read before the actual hash probe.
      if (remaining.size() == 1) break;
      std::unordered_set<size_t> after = joined;
      after.insert(candidate);
      std::vector<Expression> applicable;
      collect_deduped_applicable(candidate, after, &applicable, false);
      const auto estimate_begin = std::chrono::steady_clock::now();
      const size_t estimate =
          legacy_planner
              ? EstimateJoinRows(result, relations[candidate], applicable)
              : EstimateJoinRowsCached(result, relations[candidate], applicable,
                                       &frequency_cache);
      join_estimate_ms += ElapsedMs(estimate_begin);
      const bool connected = !applicable.empty();
      const bool cheaper =
          estimate < next_estimate ||
          (estimate == next_estimate &&
           relations[candidate].TotalRows() < relations[next].TotalRows());
      if ((connected && !next_connected) ||
          (connected == next_connected && cheaper)) {
        next = candidate;
        next_estimate = estimate;
        next_connected = connected;
      }
    }

    std::unordered_set<size_t> after = joined;
    after.insert(next);
    std::vector<Expression> applicable;
    collect_deduped_applicable(next, after, &applicable, true);
    // Final join step of a fusable grouped statement: feed the matched pairs
    // straight into the aggregation instead of materializing the joined
    // relation.  Everything was already filtered locally, so no join output
    // ever exists.  A nullopt (shape not fusable) falls through to the
    // materializing join below.
    if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr) {
      // The spec is null for statements that never reached the shape gate;
      // reading `fusion->eligible` here would fault (oracle: any grouped
      // query with TINYLAMB_FUSION_DEBUG set).
      std::cerr << "[fusion] greedy hook: eligible="
                << (fusion != nullptr && fusion->eligible)
                << " fully_applied=" << *where_fully_applied
                << " last=" << (remaining.size() == 1)
                << " residual=" << join_residual.size() << "\n";
    }
    if (fusion != nullptr && fusion->eligible && *where_fully_applied &&
        remaining.size() == 1 && join_residual.empty()) {
      const bool group_keys_in_result =
          fusion->group_keys.empty() ||
          std::ranges::all_of(fusion->group_keys, [&](const Expression& gk) {
            if (!gk) return false;
            const auto cols = gk->TouchedColumns();
            return !cols.empty() &&
                   std::ranges::all_of(cols, [&](const ColumnName& cn) {
                     return LocalColumnOffset(result.schema, cn).has_value();
                   });
          });
      const bool flip_fused_sides =
          !result.HasSpill() &&
          (relations[next].HasSpill() ||
           ((relations[next].TotalRows() >= kFlipMinBuildRows &&
             relations[next].TotalRows() > result.TotalRows() * kFlipRatio) ||
            (group_keys_in_result && result.TotalRows() <= 524288 &&
             relations[next].TotalRows() >= result.TotalRows())));
      ASSIGN_OR_RETURN(
          std::optional<Relation>, fused,
          (flip_fused_sides ? TryFusedGroupedJoin(
                                  context, relations[next], result, applicable,
                                  JoinType::kInner, outer, ctes, fusion)
                            : TryFusedGroupedJoin(
                                  context, result, relations[next], applicable,
                                  JoinType::kInner, outer, ctes, fusion)));
      if (fused.has_value()) {
        result = std::move(*fused);
        fusion->fired = true;
        joined.insert(next);
        remaining.erase(next);
        break;
      }
    }
    ASSIGN_OR_RETURN(
        Relation, result_r,
        (InnerJoin(context, std::move(result), std::move(relations[next]),
                   applicable, outer, ctes)));
    result = std::move(result_r);
    joined.insert(next);
    remaining.erase(next);
  }
  if (remaining.empty() && fusion != nullptr && fusion->eligible &&
      !fusion->fired && *where_fully_applied && join_residual.empty() &&
      relations.size() >= 2) {
    Relation absorbed_dummy(context.execution_runtime());
    absorbed_dummy.rows.emplace_back();
    ASSIGN_OR_RETURN(
        std::optional<Relation>, fused,
        (TryFusedGroupedJoin(context, result, absorbed_dummy, {},
                             JoinType::kInner, outer, ctes, fusion)));
    if (fused.has_value()) {
      result = std::move(*fused);
      fusion->fired = true;
    }
  }
  if (!join_residual.empty()) {
    // Subquery-bearing or constant-only ON conjuncts never made it into a
    // scan filter or join key; evaluate them over the joined rows now so the
    // inner-join semantics stay intact.
    RETURN_IF_FAIL(
        FilterRelation(context, &result, join_residual, outer, ctes));
  }
  if (!pinned_stores.empty() && !(fusion != nullptr && fusion->fired)) {
    for (Row& r : result.rows) {
      for (Value& v : r.values_) {
        if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
            !v.value.varchar_value.empty()) {
          v.owned_data.assign(v.value.varchar_value.data(),
                              v.value.varchar_value.size());
          v.value.varchar_value = v.owned_data;
        }
      }
    }
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_estimate_ms += join_estimate_ms;
    context.execution_runtime()->planning_ms +=
        ElapsedMs(planning_begin) - join_estimate_ms;
  }
  return result;
}

}  // namespace tinylamb::relational_detail
