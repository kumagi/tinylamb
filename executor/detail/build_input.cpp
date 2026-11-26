/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
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
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/build_input_internal.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
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
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
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

StatusOr<Relation> BuildInput(TransactionContext& context,
                              const SelectStatement& statement,
                              const Scope* outer, const CteMap& ctes,
                              bool* where_fully_applied,
                              GroupedJoinFusionSpec* fusion,
                              bool ignore_projection_star, bool columnar_output,
                              const PushedScanProjection* pushed_projection,
                              bool* pushed_projection_applied) {
  *where_fully_applied = false;
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied = false;
  }
  bool pushed_proj_applied_local = false;
  struct BuildTimer {
    std::chrono::steady_clock::time_point begin{
        std::chrono::steady_clock::now()};
    const SelectStatement* statement{nullptr};
    explicit BuildTimer(const SelectStatement* stmt) : statement(stmt) {}
    BuildTimer(const BuildTimer&) = delete;
    BuildTimer& operator=(const BuildTimer&) = delete;
    BuildTimer(BuildTimer&&) = delete;
    BuildTimer& operator=(BuildTimer&&) = delete;
    ~BuildTimer() {
      if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") == nullptr) {
        return;
      }
      const double ms = std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - begin)
                            .count();
      if (ms < 15.0) {
        return;
      }
      const auto& sources = statement->Sources();
      std::cerr << "[build] ms=" << ms << " sources=" << sources.size();
      if (!sources.empty()) {
        std::cerr << " t0="
                  << (sources[0].alias.empty() ? sources[0].table
                                               : sources[0].alias);
      }
      std::cerr << "\n";
    }
  } build_timer(&statement);
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
  std::vector<std::vector<slot_t>> deferred_proj_to_inner_col(
      statement.Sources().size());
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
      // Shared CTEs are immutable. Test alias-local, non-throwing predicates
      // before copying their rows, and retain only columns this consumer uses.
      // Qualified references avoid binding an ambiguous bare name early.
      const auto cte = ctes.find(source.table);
      if (pure_inner_join && outer == nullptr && !source.query &&
          !source.unnest && !source.sample && cte != ctes.end() &&
          !cte->second->using_columns) {
        const std::string qualifier =
            source.alias.empty() ? source.table : source.alias;
        const Schema view = QualifySchema(cte->second->schema, qualifier);
        std::vector<Expression> early_predicates;
        for (const auto& predicate : SplitConjuncts(statement.WhereClause())) {
          const auto columns = predicate->TouchedColumns();
          if (!columns.empty() &&
              std::ranges::all_of(columns,
                                  [&](const ColumnName& column) {
                                    return IdentifierEquals(column.schema,
                                                            qualifier);
                                  }) &&
              IsNonThrowingScanComparison(predicate, view)) {
            early_predicates.push_back(predicate);
          }
        }
        const auto kept =
            RequiredColumns(statement, view, ignore_projection_star);
        if (!cte->second->HasSpill() && cte->second->HasColumnarOnly()) {
          RETURN_IF_FAIL(cte->second->GetStatus());
          bool copy_full_row = kept.size() == view.ColumnCount();
          if (copy_full_row) {
            for (size_t k = 0; k < kept.size(); ++k) {
              if (kept[k] != static_cast<slot_t>(k)) {
                copy_full_row = false;
                break;
              }
            }
          }
          bool columnar_done = false;
          Relation filtered(context.execution_runtime());
          filtered.schema = ProjectSchema(view, kept);
          if (early_predicates.empty()) {
            if (copy_full_row) {
              filtered.SetColumnarTable(cte->second->columnar,
                                        cte->second->uniform_cell_types);
            } else {
              bool all_uniform = false;
              filtered.SetColumnarTable(
                  ProjectColumnarTableBySlots(*cte->second->columnar, kept,
                                              &all_uniform),
                  all_uniform);
            }
            columnar_done = true;
          } else {
            const FilterCompileEnv env{.context = &context,
                                       .outer = outer,
                                       .ctes = &ctes,
                                       .input_rows = cte->second->TotalRows()};
            const auto filter = CompileScanFilter(early_predicates, view, &env);
            if (!filter.needs_unsigned_tagging &&
                (filter.residual.empty() || filter.residual_bytecode)) {
              Relation col_filtered(context.execution_runtime());
              col_filtered.schema = view;
              Status filter_error{Status::kSuccess};
              if (TryParallelFilterRows(context, *cte->second, filter, view,
                                        outer, ctes, nullptr, &col_filtered,
                                        &filter_error) &&
                  col_filtered.HasColumnarOnly()) {
                RETURN_IF_FAIL(filter_error);
                if (copy_full_row) {
                  filtered.SetColumnarTable(std::move(col_filtered.columnar),
                                            col_filtered.uniform_cell_types);
                } else {
                  bool all_uniform = false;
                  filtered.SetColumnarTable(
                      ProjectColumnarTableBySlots(*col_filtered.columnar, kept,
                                                  &all_uniform),
                      all_uniform);
                }
                columnar_done = true;
              } else {
                RETURN_IF_FAIL(filter_error);
              }
            }
          }
          if (columnar_done) {
            RETURN_IF_FAIL(filtered.FinishSpill());
            RETURN_IF_FAIL(filtered.GetStatus());
            relations[i] = std::move(filtered);
            continue;
          }
        }
        if (!early_predicates.empty() ||
            (!cte->second->HasSpill() && !cte->second->HasColumnarOnly() &&
             (kept.size() < view.ColumnCount() ||
              cte->second->rows.size() >= 16384))) {
          const FilterCompileEnv env{.context = &context,
                                     .outer = outer,
                                     .ctes = &ctes,
                                     .input_rows = cte->second->TotalRows()};
          const auto filter = CompileScanFilter(early_predicates, view, &env);
          Relation filtered(context.execution_runtime());
          filtered.schema = ProjectSchema(view, kept);
          bool copy_full_row = kept.size() == view.ColumnCount();
          if (copy_full_row) {
            for (size_t k = 0; k < kept.size(); ++k) {
              if (kept[k] != static_cast<slot_t>(k)) {
                copy_full_row = false;
                break;
              }
            }
          }
          const auto append_projection = [&](Relation& output, const Row& row) {
            if (copy_full_row) {
              output.AddRow(row);
              return;
            }
            std::vector<Value> values;
            values.reserve(kept.size());
            for (const slot_t column : kept) {
              values.push_back(row[column]);
            }
            output.AddRow(Row(std::move(values)));
          };
          // ForEachRow propagates a sticky spill error even when the source
          // retained its rows in memory. Preserve that check on the direct
          // parallel traversal as well.
          RETURN_IF_FAIL(cte->second->GetStatus());
          const bool no_filter =
              early_predicates.empty() && !filter.compile_decided_reject;
          const size_t workers =
              !cte->second->HasSpill() && !cte->second->HasColumnarOnly() &&
                      !filter.needs_unsigned_tagging &&
                      filter.simple.size() == early_predicates.size()
                  ? ParallelWorkerCount(cte->second->rows.size() / 4096)
                  : 1;
          if (workers > 1) {
            // Shared CTE storage remains immutable. Each worker copies only
            // the accepted projection; joining shards in input order keeps
            // multiplicity and deterministic encounter order unchanged.
            std::vector<ExecutionRuntime> runtimes(workers);
            std::vector<Relation> shards(workers);
            for (size_t w = 0; w < workers; ++w) {
              shards[w].set_runtime(&runtimes[w]);
            }
            RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
              if (filter.compile_decided_reject) {
                return;
              }
              const auto& rows = cte->second->rows;
              const size_t begin = rows.size() * w / workers;
              const size_t end = rows.size() * (w + 1) / workers;
              if (no_filter) {
                shards[w].rows.reserve(end - begin);
                for (size_t r = begin; r < end; ++r) {
                  append_projection(shards[w], rows[r]);
                }
              } else {
                for (size_t r = begin; r < end; ++r) {
                  if (std::ranges::all_of(filter.simple, [&](const auto& pred) {
                        return MatchSimpleCompare(rows[r], pred);
                      })) {
                    append_projection(shards[w], rows[r]);
                  }
                }
              }
              if (cte->second->uniform_cell_types && !shards[w].HasSpill()) {
                shards[w].uniform_cell_types = true;
              }
            }));
            if (filtered.runtime() != nullptr) {
              for (size_t w = 0; w < workers; ++w) {
                filtered.runtime()->relation_spills +=
                    runtimes[w].relation_spills;
              }
            }
            RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &filtered));
            if (cte->second->uniform_cell_types && !filtered.HasSpill()) {
              filtered.uniform_cell_types = true;
            }
          } else {
            if (no_filter && !cte->second->HasSpill()) {
              filtered.rows.reserve(cte->second->rows.size());
            }
            Status error{Status::kSuccess};
            ColumnLookupCache lookup;
            RETURN_IF_FAIL(cte->second->ForEachRow([&](const Row& row) {
              if (error.ok() &&
                  MatchScanFilter(row, view, filter, outer, context, ctes,
                                  &error, &lookup)) {
                append_projection(filtered, row);
              }
            }));
            RETURN_IF_FAIL(error);
            if (!cte->second->HasSpill() && cte->second->uniform_cell_types &&
                !filtered.HasSpill()) {
              filtered.uniform_cell_types = true;
            }
          }
          RETURN_IF_FAIL(filtered.FinishSpill());
          RETURN_IF_FAIL(filtered.GetStatus());
          relations[i] = std::move(filtered);
          continue;
        }
      }
      const SelectStatement* derived = source.query.get();
      if (pure_inner_join && derived != nullptr && !source.sample &&
          derived->WithQueries().empty() && derived->UnionAll().empty() &&
          derived->GetSetOperationTree() == nullptr && !derived->HasLimit() &&
          derived->Offset() == 0 && !derived->Distinct() &&
          !derived->IsGroupAll() && !derived->GroupBy().empty() &&
          (derived->GroupBy().size() == 1 || !derived->Having()) &&
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
            std::ranges::all_of(derived->GroupBy(), [](const Expression& g) {
              return g && g->Type() == TypeTag::kColumnValue;
            })) {
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
            std::vector<slot_t> group_key_offs;
            group_key_offs.reserve(derived->GroupBy().size());
            bool group_keys_ok = true;
            for (const Expression& g : derived->GroupBy()) {
              const auto off = LocalColumnOffset(
                  inner_qual_schema, g->AsColumnValue().GetColumnName());
              if (!off.has_value() ||
                  !SingleIntegerJoinKey(inner_table_schema,
                                        {static_cast<slot_t>(*off)})) {
                group_keys_ok = false;
                break;
              }
              group_key_offs.push_back(static_cast<slot_t>(*off));
            }
            if (group_keys_ok && !group_key_offs.empty()) {
              auto is_supported_agg_output = [&](const Expression& e,
                                                 ValueType* out_vt) -> bool {
                if (!e) {
                  return false;
                }
                if (e->Type() == TypeTag::kAggregateExp) {
                  const auto at = e->AsAggregateExpression().GetType();
                  if (at == AggregationType::kCount) {
                    *out_vt = ValueType::kInt64;
                    return true;
                  }
                  if (!derived->Having() && (at == AggregationType::kSum ||
                                             at == AggregationType::kMin ||
                                             at == AggregationType::kMax ||
                                             at == AggregationType::kAvg)) {
                    const auto rt = e->ResultType(inner_qual_schema).GetType();
                    if (rt == TypeTag::kBigInt) {
                      *out_vt = ValueType::kInt64;
                      return true;
                    }
                    if (rt == TypeTag::kDouble) {
                      *out_vt = ValueType::kDouble;
                      return true;
                    }
                    if (rt == TypeTag::kDate) {
                      *out_vt = ValueType::kDate;
                      return true;
                    }
                  }
                  return false;
                }
                if (!derived->Having() && e->Type() == TypeTag::kBinaryExp) {
                  const auto& bin = e->AsBinaryExpression();
                  if (bin.Op() != BinaryOperation::kMultiply &&
                      bin.Op() != BinaryOperation::kAdd &&
                      bin.Op() != BinaryOperation::kSubtract &&
                      bin.Op() != BinaryOperation::kDivide) {
                    return false;
                  }
                  const Expression* const_side = nullptr;
                  const Expression* agg_side = nullptr;
                  if (bin.Left() &&
                      bin.Left()->Type() == TypeTag::kConstantValue) {
                    const_side = &bin.Left();
                    agg_side = &bin.Right();
                  } else if (bin.Right() &&
                             bin.Right()->Type() == TypeTag::kConstantValue) {
                    const_side = &bin.Right();
                    agg_side = &bin.Left();
                  }
                  if (const_side != nullptr && agg_side != nullptr &&
                      (*agg_side)->Type() == TypeTag::kAggregateExp &&
                      (*agg_side)->AsAggregateExpression().GetType() ==
                          AggregationType::kSum) {
                    const Value& cv =
                        (*const_side)->AsConstantValue().GetValue();
                    if (!cv.IsNull() && (cv.type == ValueType::kDouble ||
                                         cv.type == ValueType::kInt64)) {
                      const auto rt =
                          e->ResultType(inner_qual_schema).GetType();
                      if (rt == TypeTag::kDouble) {
                        *out_vt = ValueType::kDouble;
                        return true;
                      }
                      if (rt == TypeTag::kBigInt) {
                        *out_vt = ValueType::kInt64;
                        return true;
                      }
                    }
                  }
                }
                return false;
              };
              std::vector<Column> derived_cols;
              derived_cols.reserve(derived->SelectList().size());
              std::vector<slot_t> group_out_slots;
              group_out_slots.reserve(derived->SelectList().size());
              std::vector<slot_t> inner_col_for_proj;
              inner_col_for_proj.reserve(derived->SelectList().size());
              bool schema_ok = !derived->SelectList().empty();
              for (size_t s = 0; s < derived->SelectList().size(); ++s) {
                const Expression& expr = derived->SelectList()[s].expression;
                if (!expr) {
                  schema_ok = false;
                  break;
                }
                const std::string col_name =
                    ProjectionName(derived->SelectList()[s], s);
                ValueType agg_vt = ValueType::kNull;
                if (expr->Type() == TypeTag::kColumnValue) {
                  const auto col_off = LocalColumnOffset(
                      inner_qual_schema, expr->AsColumnValue().GetColumnName());
                  if (!col_off.has_value() ||
                      std::ranges::find(group_key_offs,
                                        static_cast<slot_t>(*col_off)) ==
                          group_key_offs.end()) {
                    schema_ok = false;
                    break;
                  }
                  const Column& base_col =
                      inner_table_schema.GetColumn(*col_off);
                  Column out_col(col_name, base_col.Type());
                  out_col.SetUnsigned(base_col.IsUnsigned());
                  derived_cols.push_back(std::move(out_col));
                  group_out_slots.push_back(static_cast<slot_t>(s));
                  inner_col_for_proj.push_back(static_cast<slot_t>(*col_off));
                } else if (is_supported_agg_output(expr, &agg_vt)) {
                  derived_cols.emplace_back(col_name, agg_vt);
                } else {
                  schema_ok = false;
                  break;
                }
              }
              if (schema_ok && !group_out_slots.empty()) {
                Schema derived_schema("", std::move(derived_cols));
                if (!source.alias.empty()) {
                  derived_schema = QualifySchema(derived_schema, source.alias);
                }
                relations[i].schema = std::move(derived_schema);
                projections[i] = std::move(group_out_slots);
                deferred_proj_to_inner_col[i] = std::move(inner_col_for_proj);
                deferred_derived[i] = true;
                continue;
              }
            }
          }
        }
      }
      auto restricted =
          pure_inner_join && outer == nullptr && derived != nullptr
              ? RestrictUnionInput(context, statement, i, ctes)
              : nullptr;
      SelectSource loading = source;
      if (restricted) {
        loading.query = restricted;
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->retained_statements.push_back(
              restricted);
        }
      }
      const bool single_derived_columnar =
          columnar_output && !statement.Sources()[i].from_nested_join;
      ASSIGN_OR_RETURN(
          Relation, loaded_rel,
          (LoadSource(context, loading, outer, ctes, nullptr, nullptr, nullptr,
                      nullptr, single_derived_columnar)));
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
      // Uniqueness is only an optimization hint here. Do not scan and
      // materialize the entire table merely to discover it during planning.
      const uint64_t gen = table.Value()->LayoutGeneration();
      const page_id_t first_pid = table.Value()->FirstPageId();
      const bool clean_txn =
          context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty();
      std::shared_ptr<const LSTableStore> lst_store =
          table.Value()->GetLSTableStore();
      const bool valid_store = lst_store != nullptr &&
                               lst_store->uncommitted_owner_txn == 0 &&
                               lst_store->layout_generation == gen &&
                               lst_store->first_pid == first_pid;
      std::shared_ptr<const TlcsFile> backing_tlcs =
          table.Value()->GetBackingTlcsStore();
      const bool valid_tlcs =
          gen != 0 && !table.Value()->RowsRewrittenInPlace() && clean_txn &&
          backing_tlcs != nullptr && backing_tlcs->LayoutGeneration() == gen &&
          backing_tlcs->FirstPageId() == first_pid &&
          backing_tlcs->HasColumnSynopses();
      bool any_promoted = false;
      std::vector<Column> cols;
      cols.reserve(base_schema.ColumnCount());
      for (size_t ci = 0; ci < base_schema.ColumnCount(); ++ci) {
        Column col = base_schema.GetColumn(ci);
        if (col.GetConstraint().ctype == Constraint::kNothing) {
          bool is_unique_nonnull =
              valid_store && ci < lst_store->column_is_unique_nonnull.size() &&
              lst_store->column_is_unique_nonnull[ci] != 0;
          if (!is_unique_nonnull && valid_tlcs &&
              ci < backing_tlcs->ColumnSynopses().size() &&
              backing_tlcs->ColumnSynopses()[ci].is_unique_nonnull) {
            is_unique_nonnull = true;
          }
          if (!is_unique_nonnull && gen != 0 &&
              !table.Value()->RowsRewrittenInPlace() && clean_txn) {
            if (auto cached = table.Value()->GetCachedLSTableColumn(
                    static_cast<slot_t>(ci), gen);
                cached != nullptr && cached->is_unique_nonnull) {
              is_unique_nonnull = true;
            }
          }
          if (is_unique_nonnull) {
            col = Column(col.Name(), col.Type(),
                         Constraint(Constraint::kPrimaryKey));
            any_promoted = true;
          }
        }
        cols.push_back(std::move(col));
      }
      if (any_promoted) {
        base_schema = Schema(base_schema.Name(), std::move(cols));
      }
    }
    relations[i].schema = qualifier.empty()
                              ? std::move(base_schema)
                              : QualifySchema(base_schema, qualifier);
    projections[i] =
        RequiredColumns(statement, relations[i].schema, ignore_projection_star);
    const uint64_t columnar_rows = table.Value()->BackingColumnarRowCount();
    const auto* runtime = context.execution_runtime();
    // LoadSource rescans large columnar tables instead of publishing boxed
    // row caches. Widening those scans to every sibling consumer's columns
    // only adds decoding; published typed arenas still keep their common
    // layout so their consumers can reuse it.
    const bool independent_columnar_scan =
        columnar_rows > 131072 &&
        (runtime == nullptr || runtime->base_relation_arenas.empty());
    if (const std::vector<slot_t>* shared =
            independent_columnar_scan
                ? nullptr
                : ReusableProjection(context, source.table)) {
      // The shared layout was derived from the raw table schema, so
      // alias-qualified references (T1.col) resolved against this source's
      // qualified schema may be missing from it; projecting fewer columns
      // than this source needs breaks evaluation downstream.
      std::vector<slot_t> merged(projections[i]);
      merged.reserve(projections[i].size() + shared->size());
      for (const slot_t slot : *shared) {
        if (std::ranges::find(merged, slot) == merged.end()) {
          merged.push_back(slot);
        }
      }
      if (columnar_rows <= 16384 ||
          merged.size() <= projections[i].size() + 3) {
        std::ranges::sort(merged);
        projections[i] = std::move(merged);
      }
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
                      where_fully_applied, fusion, ignore_projection_star,
                      columnar_output, pushed_projection,
                      pushed_projection_applied);
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
    return BuildOrderedJoinInput(context, statement, outer, ctes,
                                 where_fully_applied, fusion, columnar_output,
                                 relations, base_sources, projections);
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
    empty.schema = ConcatRelationSchemas(relations);
    for (size_t i = 1; i < statement.Sources().size(); ++i) {
      RETURN_IF_FAIL(ValidateExpressionColumnsInScope(
          statement.Sources()[i].join_condition, empty.schema, outer));
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
  // (subquery predicates, constant-only conjuncts, outer-correlated predicates,
  // or unresolved column references) must still reach residual evaluation so
  // outer filters apply and invalid references are rejected instead of
  // silently degrading the join to a cross product.
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
    const Schema joined_schema = ConcatRelationSchemas(relations);
    // Populate the residual list from all analyzed ON conjuncts so any
    // unplaced conjunct is evaluated (or rejected if unresolvable) post-join.
    std::unordered_set<const ExpressionBase*> from_joins;
    from_joins.reserve(predicates.size());
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
      if (!predicate.resolved) {
        RETURN_IF_FAIL(
            ValidateExpressionColumnsInScope(expr, joined_schema, outer));
      }
      // Note: USING(name) conjuncts are fully handled by the ordered-join
      // path above, so a bare `x = x` seen here is a user-written ON
      // conjunct; it keeps normal residual/local handling instead of being
      // dropped (dropping it would lose the NULL rejection of `x = x`).
      join_residual.push_back(expr);
    }
  }
  const std::vector<PredicateInfo> where_predicates =
      AnalyzePredicates(statement.WhereClause(), relations);
  // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
  //   (single_source_exists_pushdown_before_inner_join_sound,
  //    prebuilt_exists_outer_domain_filter_sound)
  // Boolean trees of single-key prebuildable EXISTS/NOT EXISTS predicates
  // referencing only `src_idx` evaluate AFTER `LoadSource` applies dimension
  // runtime filters (`rf_keys`) to `relations[src_idx]`, and BEFORE joining
  // wide neighbours (such as 1.92M-row customer_demographics in Q10/Q35/Q69).
  auto is_prebuildable_exists_tree =
      [&](const auto& self, const Expression& expr, const Schema& outer_schema,
          std::vector<ColumnName>* out_outer_cols) -> bool {
    if (!expr) {
      return false;
    }
    if (expr->Type() == TypeTag::kBinaryExp) {
      const auto& bin = expr->AsBinaryExpression();
      if (bin.Op() == BinaryOperation::kAnd ||
          bin.Op() == BinaryOperation::kOr) {
        return self(self, bin.Left(), outer_schema, out_outer_cols) &&
               self(self, bin.Right(), outer_schema, out_outer_cols);
      }
      return false;
    }
    if (expr->Type() != TypeTag::kQueryExp) {
      return false;
    }
    const auto& query = expr->AsQueryExpression();
    if (!query.Exists() || query.Test() || query.ArrayResult() ||
        !query.Query()) {
      return false;
    }
    const SelectStatement& sub = *query.Query();
    if (sub.Sources().empty() || !sub.WithQueries().empty() ||
        !sub.UnionAll().empty() || sub.GetSetOperationTree() || sub.Qualify() ||
        sub.HasLimit() || sub.Offset() != 0 || !sub.GroupBy().empty() ||
        sub.Having() || sub.Distinct() || !sub.OrderBy().empty() ||
        sub.AsStruct() || sub.ForUpdate() || sub.HasDistinctOn() ||
        StatementContainsVolatileFunction(sub)) {
      return false;
    }
    Schema local_schema;
    for (const auto& source : sub.Sources()) {
      if (source.query || source.unnest || source.sample ||
          source.join_condition ||
          (source.join_type != JoinType::kCross &&
           source.join_type != JoinType::kInner)) {
        return false;
      }
      const std::string& qualifier =
          source.alias.empty() ? source.table : source.alias;
      Schema part;
      if (ctes.contains(source.table)) {
        const Relation* cte = ctes.at(source.table).get();
        if (cte == nullptr) {
          return false;
        }
        part = qualifier.empty() ? cte->schema
                                 : QualifySchema(cte->schema, qualifier);
      } else {
        auto table = context.GetTable(source.table);
        auto stats = context.GetStats(source.table);
        if (!table.HasValue() || !stats.HasValue() ||
            stats.Value()->Rows() == 0) {
          return false;
        }
        part = qualifier.empty()
                   ? table.Value()->GetSchema()
                   : QualifySchema(table.Value()->GetSchema(), qualifier);
      }
      local_schema = local_schema.ColumnCount() == 0 ? std::move(part)
                                                     : (local_schema + part);
    }
    for (const auto& item : sub.SelectList()) {
      if (!item.expression ||
          (item.expression->Type() != TypeTag::kConstantValue &&
           (item.expression->Type() != TypeTag::kColumnValue ||
            (item.expression->AsColumnValue().GetColumnName().name != "*" &&
             !LocalColumnOffset(
                 local_schema,
                 item.expression->AsColumnValue().GetColumnName()))))) {
        return false;
      }
    }
    Expression local_key;
    ColumnName outer_key;
    Expression local_key2;
    ColumnName outer_key2;
    const bool allow_cte_pair_key =
        sub.Sources().size() == 1 && !sub.Sources()[0].query &&
        !sub.Sources()[0].unnest && !sub.Sources()[0].sample &&
        ctes.contains(sub.Sources()[0].table);
    Expression local_witness;
    ColumnName outer_witness;
    for (const auto& conjunct : SplitConjuncts(sub.WhereClause())) {
      if (!conjunct || ContainsQuery(conjunct) ||
          ContainsParameterSlot(conjunct) || ContainsVolatileCall(conjunct)) {
        return false;
      }
      if (conjunct->Type() == TypeTag::kBinaryExp) {
        const auto& binary = conjunct->AsBinaryExpression();
        if (binary.Op() == BinaryOperation::kEquals && binary.Left() &&
            binary.Right() && binary.Left()->Type() == TypeTag::kColumnValue &&
            binary.Right()->Type() == TypeTag::kColumnValue) {
          const auto& left = binary.Left()->AsColumnValue().GetColumnName();
          const auto& right = binary.Right()->AsColumnValue().GetColumnName();
          const bool left_local =
              LocalColumnOffset(local_schema, left).has_value();
          const bool right_local =
              LocalColumnOffset(local_schema, right).has_value();
          if (left_local != right_local) {
            if (!local_key) {
              local_key = left_local ? binary.Left() : binary.Right();
              outer_key = left_local ? right : left;
              continue;
            }
            if (allow_cte_pair_key && !local_key2) {
              local_key2 = left_local ? binary.Left() : binary.Right();
              outer_key2 = left_local ? right : left;
              continue;
            }
            return false;
          }
        }
        if (binary.Op() == BinaryOperation::kNotEquals && binary.Left() &&
            binary.Right() && binary.Left()->Type() == TypeTag::kColumnValue &&
            binary.Right()->Type() == TypeTag::kColumnValue) {
          const auto& left = binary.Left()->AsColumnValue().GetColumnName();
          const auto& right = binary.Right()->AsColumnValue().GetColumnName();
          const bool left_local =
              LocalColumnOffset(local_schema, left).has_value();
          const bool right_local =
              LocalColumnOffset(local_schema, right).has_value();
          if (left_local != right_local) {
            if (local_witness) {
              return false;
            }
            local_witness = left_local ? binary.Left() : binary.Right();
            outer_witness = left_local ? right : left;
            continue;
          }
        }
      }
      if (!SafeToReduceEvaluationCountWithSchema(conjunct, local_schema)) {
        return false;
      }
      for (const ColumnName& col : conjunct->TouchedColumns()) {
        if (!LocalColumnOffset(local_schema, col).has_value()) {
          return false;
        }
      }
    }
    const auto probe = LocalColumnOffset(outer_schema, outer_key);
    if (!local_key || !probe) {
      return false;
    }
    const auto local = LocalColumnOffset(
        local_schema, local_key->AsColumnValue().GetColumnName());
    if (!local) {
      return false;
    }
    const Column& inner_col =
        local_schema.GetColumn(static_cast<size_t>(*local));
    const Column& outer_col =
        outer_schema.GetColumn(static_cast<size_t>(*probe));
    if ((inner_col.Type() != ValueType::kInt64 &&
         inner_col.Type() != ValueType::kDate) ||
        inner_col.Type() != outer_col.Type() || inner_col.IsUnsigned() ||
        outer_col.IsUnsigned()) {
      return false;
    }
    out_outer_cols->push_back(outer_key);
    if (local_key2) {
      if (local_witness) {
        return false;
      }
      const auto probe2 = LocalColumnOffset(outer_schema, outer_key2);
      const auto local2 = LocalColumnOffset(
          local_schema, local_key2->AsColumnValue().GetColumnName());
      if (!probe2 || !local2 || *local2 == *local) {
        return false;
      }
      const Column& inner_col2 =
          local_schema.GetColumn(static_cast<size_t>(*local2));
      const Column& outer_col2 =
          outer_schema.GetColumn(static_cast<size_t>(*probe2));
      if ((inner_col2.Type() != ValueType::kInt64 &&
           inner_col2.Type() != ValueType::kDate) ||
          inner_col2.Type() != outer_col2.Type() || inner_col2.IsUnsigned() ||
          outer_col2.IsUnsigned()) {
        return false;
      }
      out_outer_cols->push_back(outer_key2);
    }
    if (local_witness) {
      const auto w_probe = LocalColumnOffset(outer_schema, outer_witness);
      const auto w_local = LocalColumnOffset(
          local_schema, local_witness->AsColumnValue().GetColumnName());
      if (!w_probe || !w_local) {
        return false;
      }
      const Column& iw_col =
          local_schema.GetColumn(static_cast<size_t>(*w_local));
      const Column& ow_col =
          outer_schema.GetColumn(static_cast<size_t>(*w_probe));
      if ((iw_col.Type() != ValueType::kInt64 &&
           iw_col.Type() != ValueType::kDate) ||
          iw_col.Type() != ow_col.Type() || iw_col.IsUnsigned() ||
          ow_col.IsUnsigned()) {
        return false;
      }
      out_outer_cols->push_back(outer_witness);
    }
    return true;
  };
  auto post_load_exists_owner =
      [&](const PredicateInfo& predicate) -> std::optional<size_t> {
    if (outer != nullptr || relations.size() <= 1 || !predicate.resolved ||
        predicate.sources.size() > 1 || !predicate.contains_query ||
        ContainsOnlyUncorrelatedQueries(context, predicate.expression, ctes)) {
      return std::nullopt;
    }
    std::optional<size_t> matched_owner;
    for (size_t cand = 0; cand < relations.size(); ++cand) {
      if (!predicate.sources.empty() && !predicate.sources.contains(cand)) {
        continue;
      }
      if (base_sources[cand]) {
        uint64_t est = 0;
        if (StatusOr<std::shared_ptr<Table>> tbl =
                context.GetTable(statement.Sources()[cand].table);
            tbl.HasValue() && tbl.Value() != nullptr) {
          est = tbl.Value()->BackingColumnarRowCount();
        }
        if (StatusOr<std::shared_ptr<TableStatistics>> st =
                context.GetStats(statement.Sources()[cand].table);
            st.HasValue()) {
          est = std::max<uint64_t>(est, st.Value()->Rows());
        }
        if (est > 250000) {
          continue;
        }
      }
      std::vector<ColumnName> outer_cols;
      if (!is_prebuildable_exists_tree(is_prebuildable_exists_tree,
                                       predicate.expression,
                                       relations[cand].schema, &outer_cols) ||
          outer_cols.empty()) {
        continue;
      }
      std::vector<Relation> single_scope(1);
      single_scope[0].schema = relations[cand].schema;
      if (!ExpressionUsesOnlyScopes(context, predicate.expression, single_scope,
                                    ctes)) {
        continue;
      }
      const bool touches_other =
          std::ranges::any_of(outer_cols, [&](const ColumnName& col) {
            for (size_t r = 0; r < relations.size(); ++r) {
              if (r != cand &&
                  LocalColumnOffset(relations[r].schema, col).has_value()) {
                return true;
              }
            }
            return false;
          });
      if (touches_other) {
        continue;
      }
      if (matched_owner.has_value()) {
        return std::nullopt;
      }
      matched_owner = cand;
    }
    return matched_owner;
  };
  auto post_load_exists_evaluable = [&](const PredicateInfo& predicate) {
    return post_load_exists_owner(predicate).has_value();
  };
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
    if (post_load_exists_evaluable(predicate)) {
      return false;
    }
    const size_t src_idx = *predicate.sources.begin();
    if (outer == nullptr && relations.size() > 1) {
      size_t src_est_rows = std::numeric_limits<size_t>::max();
      if (base_sources[src_idx]) {
        if (StatusOr<std::shared_ptr<TableStatistics>> st =
                context.GetStats(statement.Sources()[src_idx].table);
            st.HasValue()) {
          src_est_rows = st.Value()->Rows();
        }
      } else if (!deferred_derived[src_idx]) {
        src_est_rows = relations[src_idx].TotalRows();
      }
      if (src_est_rows <= 50000) {
        std::vector<Relation> single_scope(1);
        single_scope[0].schema = relations[src_idx].schema;
        if (ExpressionUsesOnlyScopes(context, predicate.expression,
                                     single_scope, ctes)) {
          std::unordered_set<ColumnName> all_cols;
          CollectExpressionColumns(predicate.expression, &all_cols);
          const bool touches_other_relation =
              std::ranges::any_of(all_cols, [&](const ColumnName& col) {
                if (col.name == "*") {
                  return false;
                }
                for (size_t r = 0; r < relations.size(); ++r) {
                  if (r != src_idx &&
                      LocalColumnOffset(relations[r].schema, col).has_value()) {
                    return true;
                  }
                }
                return false;
              });
          if (!touches_other_relation) {
            return true;
          }
        }
      }
    }
    if (outer == nullptr && relations.size() == 1 && base_sources[src_idx] &&
        predicate.expression) {
      std::vector<ColumnName> outer_cols;
      std::vector<Relation> single_scope(1);
      single_scope[0].schema = relations[src_idx].schema;
      if (is_prebuildable_exists_tree(is_prebuildable_exists_tree,
                                      predicate.expression,
                                      relations[src_idx].schema, &outer_cols) &&
          !outer_cols.empty() &&
          ExpressionUsesOnlyScopes(context, predicate.expression, single_scope,
                                   ctes)) {
        return true;
      }
      if (predicate.expression->Type() == TypeTag::kQueryExp) {
        const QueryExpression& q = predicate.expression->AsQueryExpression();
        if (q.Exists() && !q.Test() && !q.ArrayResult() &&
            q.Query() != nullptr && q.Query()->Sources().size() == 1 &&
            !q.Query()->Sources()[0].query && !q.Query()->Sources()[0].unnest &&
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
    }
    return false;
  };
  *where_fully_applied = std::ranges::all_of(
      where_predicates, [&](const PredicateInfo& predicate) {
        return locally_evaluable(predicate) ||
               post_load_exists_evaluable(predicate) ||
               (predicate.resolved && !predicate.contains_query &&
                predicate.sources.size() > 1);
      });
  std::vector<std::vector<Expression>> local_predicates(relations.size());
  std::vector<std::vector<Expression>> post_load_exists_predicates(
      relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    std::vector<Expression>& local = local_predicates[i];
    std::vector<Expression>& post_exists = post_load_exists_predicates[i];
    for (const PredicateInfo& predicate : predicates) {
      if (predicate.sources.contains(i) && locally_evaluable(predicate)) {
        local.push_back(predicate.expression);
        mark_applied(predicate.expression);
      } else if (post_load_exists_owner(predicate) == i) {
        post_exists.push_back(predicate.expression);
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
      if (membership == nullptr || membership->TotalRows() == 0) {
        continue;
      }
      // Dynamic filter over the membership set (Phase 2 "Dynamic Filter
      // Join / SIP / Bloom").  The builder keeps the [lo, hi] enclosure of the
      // distinct keys alongside the key set and picks the exact set / SIP
      // shape at Build() time, so a large membership no longer forfeits the
      // range half of the filter.
      std::vector<int64_t> keys;
      keys.reserve(membership->TotalRows());
      ValueType membership_type = ValueType::kNull;
      bool all_int = true;
      bool saw_null = false;
      // UINT64 shares the INT64 storage, so SQL signedness rides the value's
      // otherwise-unused collation tag. Raw int64 bits are a proper superset
      // of SQL equality (UINT64(2^64-1) vs INT64(-1) never compare equal), so
      // the set is a faithful membership only when its signedness is uniform
      // -- and agrees with the probed column's.
      bool membership_unsigned = false;
      bool unsigned_decided = false;
      bool mixed_unsigned = false;
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
        if (!unsigned_decided) {
          membership_unsigned = row[0].IsUnsigned();
          unsigned_decided = true;
        } else if (membership_unsigned != row[0].IsUnsigned()) {
          // Mixed signedness: no single comparison domain either, exactly
          // like the mixed INT64/DATE case above.
          mixed_unsigned = true;
        }
        keys.push_back(row[0].value.int_value);
      };
      if (membership->HasColumnarOnly() || membership->HasSpill()) {
        RETURN_IF_FAIL(membership->ForEachRow(consume_row));
      } else {
        for (const Row& row : membership->rows) {
          consume_row(row);
        }
      }
      if (!all_int || mixed_unsigned || keys.empty()) {
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
        // Raw int64 bits are a superset of SQL equality only while the probed
        // column and the membership set share the same signedness: a UINT64
        // membership row stores 2^64-1 as the int64 bits -1, so it prunes a
        // signed -1 column value SQL never calls equal (and vice versa). The
        // superset is harmless *as a filter* because the equality-IN re-check
        // still sees the row, but `exact_in` licenses dropping that predicate
        // (dense keys + >= 4096 distinct, below), which would publish the
        // false match. Withholding exact_in keeps the predicate and lets the
        // canonical comparison decide -- the same reason the coverage
        // estimator refuses unsigned columns' exact keys.
        const bool exact_in_here =
            exact_in && relations[i].schema.GetColumn(*offset).IsUnsigned() ==
                            membership_unsigned;
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
          stored.SetOwner(statement);
          stored.source_expr = expression.get();
          stored.exact_in = exact_in_here;
        }
      }
    }
  }

  // Load selective relations first, then push their integer join keys into
  // later scans (TPC-H Q9: filter lineitem by partkeys matching LIKE).
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i] && !deferred_derived[i]) {
      if (!local_predicates[i].empty()) {
        RETURN_IF_FAIL(FilterRelation(context, &relations[i],
                                      local_predicates[i], outer, ctes));
      }
      for (const Expression& ep : post_load_exists_predicates[i]) {
        RETURN_IF_FAIL(
            FilterRelation(context, &relations[i], {ep}, outer, ctes));
      }
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
      return (std::hash<size_t>()(key.rel) * 0x9e3779b97f4a7c15ULL) ^
             std::hash<ColumnName>()(key.column);
    }
  };
  std::unordered_map<RelColKey, size_t, RelColKeyHash> key_class_node;
  std::vector<size_t> key_class_parent;
  static const bool transitive_filter_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER") !=
      nullptr;
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
          key_class_union(key_class_node_id({.rel = left_rel, .column = lhs}),
                          key_class_node_id({.rel = right_rel, .column = rhs}));
        }
      } else if (crossed) {
        const Column& lc = relations[left_rel].schema.GetColumn(*left_of_rhs);
        const Column& rc = relations[right_rel].schema.GetColumn(*right_of_lhs);
        if (lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
          key_class_union(key_class_node_id({.rel = left_rel, .column = rhs}),
                          key_class_node_id({.rel = right_rel, .column = lhs}));
        }
      }
    }
    if (!transitive_filter_disabled && key_class_node.size() > 2) {
      std::unordered_set<std::string> known_pairs;
      known_pairs.reserve(predicates.size());
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
    if (!local_predicates[index].empty() ||
        !post_load_exists_predicates[index].empty()) {
      return true;
    }
    if (context.execution_runtime() == nullptr) {
      return false;
    }
    const auto stored = context.execution_runtime()->table_key_filters.find(
        TableKeyFilterKey(statement.Sources()[index]));
    return stored != context.execution_runtime()->table_key_filters.end() &&
           stored->second.OwnedBy(statement) && !stored->second.empty();
  };
  std::vector<std::vector<size_t>> filter_neighbours(relations.size());
  for (const PredicateInfo& predicate : predicates) {
    if (!predicate.resolved || predicate.contains_query ||
        predicate.sources.size() != 2 ||
        !IsColumnEqualityPredicate(predicate.expression) ||
        predicate.expression->AsBinaryExpression().Op() !=
            BinaryOperation::kEquals) {
      continue;
    }
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
  std::vector<uint8_t> small_domain_filtered(relations.size(), 0);
  std::vector<double> small_domain_ratio(relations.size(), 1.0);
  const auto effective_load_rows = [&](size_t index) -> double {
    double best_ratio = small_domain_ratio[index];
    for (size_t other : filter_neighbours[index]) {
      if (loaded[other] && selective[other] && estimated_rows[other] > 0) {
        const double ratio = static_cast<double>(relations[other].TotalRows()) /
                             static_cast<double>(estimated_rows[other]);
        best_ratio = std::min(best_ratio, ratio);
      }
    }
    return static_cast<double>(estimated_rows[index]) * best_ratio;
  };
  static const bool pk_absorption_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PK_ABSORPTION") != nullptr;
  const auto is_large_fact = [&](size_t index) {
    if (!base_sources[index] || estimated_rows[index] < 16384) {
      return false;
    }
    for (size_t c = 0; c < relations[index].schema.ColumnCount(); ++c) {
      if (relations[index].schema.GetColumn(c).GetConstraint().ctype ==
          Constraint::kPrimaryKey) {
        return false;
      }
    }
    return true;
  };
  const bool single_large_source =
      std::ranges::count_if(load_order, is_large_fact) == 1;
  const bool wide_fact_scan =
      single_large_source && std::ranges::any_of(load_order, [&](size_t index) {
        return is_large_fact(index) && estimated_rows[index] >= 65536;
      });
  static const bool dimension_preload_disabled =
      ::tinylamb::platform::GetEnv(
          "TINYLAMB_DISABLE_DEFERRED_DIMENSION_PRELOAD") != nullptr;
  size_t fact_rows = 0;
  for (size_t i : load_order) {
    if (is_large_fact(i)) {
      fact_rows = std::max(fact_rows, estimated_rows[i]);
    }
  }
  // A 1-column join key (customer_sk) is enough to build the group id.
  // Preload it when every aggregate is a numeric column expression so the
  // fact can stay in its column blocks and be charged without row objects.
  const bool columnar_fact_aggs = [&] {
    if (fusion == nullptr || !fusion->eligible || fusion->aggregates.empty() ||
        fusion->aggregates.size() > 8) {
      return false;
    }
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      if (fusion->count_star[i]) {
        continue;
      }
      const AggregateExpression* agg = fusion->aggregates[i];
      if (agg == nullptr || agg->Distinct() || agg->HasWhereFilter() ||
          agg->NeedsGroupContext()) {
        return false;
      }
      const AggregationType type = agg->GetType();
      if (type == AggregationType::kMin || type == AggregationType::kMax ||
          type == AggregationType::kCount) {
        if (!agg->Child()) {
          return false;
        }
        if (agg->Child()->Type() == TypeTag::kColumnValue) {
          continue;
        }
        // COUNT(a * b) is non-null exactly when both factors are.
        if (type == AggregationType::kCount &&
            agg->Child()->Type() == TypeTag::kBinaryExp) {
          const auto& binary = agg->Child()->AsBinaryExpression();
          if (binary.Op() == BinaryOperation::kMultiply && binary.Left() &&
              binary.Right() &&
              binary.Left()->Type() == TypeTag::kColumnValue &&
              binary.Right()->Type() == TypeTag::kColumnValue) {
            continue;
          }
        }
        return false;
      }
      const bool is_var_or_stddev = type == AggregationType::kStddevSamp ||
                                    type == AggregationType::kStddevPop ||
                                    type == AggregationType::kVarSamp ||
                                    type == AggregationType::kVarPop;
      if (type != AggregationType::kSum && type != AggregationType::kAvg &&
          !is_var_or_stddev) {
        return false;
      }
      const Expression& child = agg->Child();
      if (!child) {
        return false;
      }
      if (child->Type() == TypeTag::kColumnValue) {
        continue;
      }
      if (is_var_or_stddev) {
        return false;
      }
      if (child->Type() == TypeTag::kBinaryExp) {
        const auto& binary = child->AsBinaryExpression();
        if (binary.Op() == BinaryOperation::kMultiply && binary.Left() &&
            binary.Right() && binary.Left()->Type() == TypeTag::kColumnValue &&
            binary.Right()->Type() == TypeTag::kColumnValue) {
          continue;
        }
      }
      return false;
    }
    return true;
  }();
  const auto preload_dimension = [&](size_t index) {
    return !dimension_preload_disabled && fusion != nullptr &&
           fusion->eligible && single_large_source && base_sources[index] &&
           !is_large_fact(index) &&
           (projections[index].size() > 2 ||
            (columnar_fact_aggs && projections[index].size() == 1)) &&
           estimated_rows[index] <= fact_rows / 4;
  };
  const auto load_rank = [&](size_t index) {
    const bool has_selective_neighbour = std::ranges::any_of(
        filter_neighbours[index],
        [&](size_t other) { return loaded[other] && selective[other]; });
    const bool awaits_smaller_selective_neighbour =
        (local_predicates[index].empty() ||
         (is_large_fact(index) && estimated_rows[index] >= 5000000)) &&
        std::ranges::any_of(filter_neighbours[index], [&](size_t other) {
          if (loaded[other] ||
              (local_predicates[other].empty() &&
               post_load_exists_predicates[other].empty()) ||
              estimated_rows[other] >= estimated_rows[index]) {
            return false;
          }
          if (!local_predicates[index].empty()) {
            // When the fact itself has local predicates (e.g. TPC-DS Q48
            // store_sales), preload genuinely small filtered dimensions
            // (<= 500k rows, e.g. customer_address 250k) so the fact receives
            // their RF without full-scanning multi-million-row dimensions like
            // customer_demographics (1.92M) that benefit from the fact's RF.
            return estimated_rows[other] <= 500000 &&
                   estimated_rows[other] * 4 < estimated_rows[index];
          }
          const bool other_has_sel = std::ranges::any_of(
              filter_neighbours[other],
              [&](size_t nb) { return loaded[nb] && selective[nb]; });
          return !has_selective_neighbour || !other_has_sel ||
                 estimated_rows[other] < 5000000 ||
                 effective_load_rows(index) * 2.0 >= effective_load_rows(other);
        });
    if (deferred_derived[index]) {
      return has_selective_neighbour ? 1 : 2;
    }
    // A moderately sized dimension can unlock a deferred fact probe. Compare
    // its size with the fact, rather than fixing a scale-specific row cutoff.
    // Local fact filters keep the normal selective-first strategy.
    // However, if the fact already has a highly selective loaded neighbour
    // making its effective output much smaller than a multi-million-row
    // already-filtered dimension (e.g. TPC-H Q8: lineitem filtered by part
    // ~390k << orders ~2.3M), scan the fact first so it pushes a selective key
    // filter into the dimension's sorted PK runs. Never defer small/unfiltered
    // dimensions with local equality predicates (e.g. TPC-DS
    // customer_demographics 1.9M -> ~27k rows).
    const auto dominated_by_selective_fact = [&](size_t dim_idx) {
      if (estimated_rows[dim_idx] < 5000000) {
        return false;
      }
      const bool dim_has_sel = std::ranges::any_of(
          filter_neighbours[dim_idx],
          [&](size_t nb) { return loaded[nb] && selective[nb]; });
      if (!dim_has_sel) {
        return false;
      }
      const double dim_eff = effective_load_rows(dim_idx);
      return std::ranges::any_of(load_order, [&](size_t f) {
        if (loaded[f] || !is_large_fact(f) || !local_predicates[f].empty()) {
          return false;
        }
        const bool f_has_sel = std::ranges::any_of(
            filter_neighbours[f],
            [&](size_t nb) { return loaded[nb] && selective[nb]; });
        return f_has_sel && effective_load_rows(f) * 2.0 < dim_eff;
      });
    };
    if (is_large_fact(index) && local_predicates[index].empty() &&
        std::ranges::any_of(load_order, [&](size_t other) {
          return !loaded[other] && preload_dimension(other) &&
                 !dominated_by_selective_fact(other);
        })) {
      return 2;
    }
    if (preload_dimension(index) && !dominated_by_selective_fact(index)) {
      return 0;
    }

    if (!pk_absorption_disabled && local_predicates[index].empty() &&
        projections[index].size() == 2 && estimated_rows[index] <= 50000 &&
        !filter_neighbours[index].empty() &&
        std::ranges::all_of(filter_neighbours[index],
                            [&](size_t other) { return loaded[other]; })) {
      return -2;
    }
    if (((selective[index] && has_selective_neighbour) ||
         small_domain_filtered[index] != 0) &&
        estimated_rows[index] <= 500000) {
      return -1;
    }
    if ((selective[index] || has_selective_neighbour) &&
        !awaits_smaller_selective_neighbour) {
      return 0;
    }
    if (fusion != nullptr && fusion->eligible && single_large_source &&
        base_sources[index] &&
        (projections[index].size() > 2 ||
         (projections[index].size() == 2 && wide_fact_scan)) &&
        estimated_rows[index] <= 32768) {
      // Load small dimensions before a fact scan so the last scan can feed
      // the fused join directly and use every dimension's runtime filter.
      return 0;
    }
    if (selective[index] || has_selective_neighbour) {
      return 1;
    }
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
  const auto append_rel_nonnull_ints = [](const Relation& rel, slot_t col,
                                          std::vector<int64_t>* out) {
    out->reserve(out->size() + rel.TotalRows());
    if (rel.HasColumnarOnly()) {
      for (const ColumnarChunk& chunk : rel.columnar->chunks) {
        if (chunk.rows == 0 || col >= chunk.columns.size()) {
          continue;
        }
        const ColumnarColumn& c = chunk.columns[col];
        // T3872: keys are int64. The constant fallback below reads
        // Value::value.int_value -- the union's int arm -- which is only
        // meaningful for kInt64/kDate. A VARCHAR constant reinterpreted the
        // string_view pointer and a DOUBLE constant the IEEE-754 bits as a
        // key, poisoning the runtime filter's key domain, so non-integral
        // columns stop here (the same guard covers the flat/generic arms).
        if (c.type != ValueType::kInt64 && c.type != ValueType::kDate) {
          continue;
        }
        if (c.encoding == ColumnarColumn::Encoding::kConstant && c.is_uniform) {
          if (!c.const_is_null &&
              (c.type == ValueType::kInt64 || c.type == ValueType::kDate)) {
            out->insert(out->end(), chunk.rows, c.const_int);
          }
        } else if (c.encoding == ColumnarColumn::Encoding::kFlat &&
                   c.is_uniform &&
                   (c.type == ValueType::kInt64 ||
                    c.type == ValueType::kDate)) {
          const int64_t* ints = c.ints.data();
          if (!c.has_null) {
            out->insert(out->end(), ints, ints + chunk.rows);
          } else {
            const uint8_t* nulls = c.nulls.data();
            for (size_t r = 0; r < chunk.rows; ++r) {
              if (nulls[r] == 0) {
                out->push_back(ints[r]);
              }
            }
          }
        } else if (c.is_uniform && (c.type == ValueType::kInt64 ||
                                    c.type == ValueType::kDate)) {
          int64_t v = 0;
          for (size_t r = 0; r < chunk.rows; ++r) {
            if (c.TryReadNonNullInt(r, &v)) {
              out->push_back(v);
            }
          }
        } else {
          Value v;
          for (size_t r = 0; r < chunk.rows; ++r) {
            c.WriteValueInto(r, &v);
            if (!v.IsNull()) {
              out->push_back(v.value.int_value);
            }
          }
        }
      }
      return;
    }
    for (const Row& row : rel.rows) {
      const Value& key = row[col];
      // T3872: same union-misread guard as the columnar path above.
      if (!key.IsNull() &&
          (key.type == ValueType::kInt64 || key.type == ValueType::kDate)) {
        out->push_back(key.value.int_value);
      }
    }
  };
  const auto rel_int_min_max = [](const Relation& rel, slot_t col,
                                  int64_t* out_min, int64_t* out_max) -> bool {
    int64_t min_v = std::numeric_limits<int64_t>::max();
    int64_t max_v = std::numeric_limits<int64_t>::min();
    const auto update = [&](int64_t iv) {
      min_v = std::min(min_v, iv);
      max_v = std::max(max_v, iv);
    };
    if (rel.HasColumnarOnly()) {
      for (const ColumnarChunk& chunk : rel.columnar->chunks) {
        if (chunk.rows == 0 || col >= chunk.columns.size()) {
          continue;
        }
        const ColumnarColumn& c = chunk.columns[col];
        if (c.encoding == ColumnarColumn::Encoding::kConstant && c.is_uniform) {
          if (!c.const_is_null &&
              (c.type == ValueType::kInt64 || c.type == ValueType::kDate)) {
            update(c.const_int);
          }
          // Non-integer constants (VARCHAR/DOUBLE/...) store their value in
          // a different union member; reading int_value would produce a
          // garbage extreme. This int-domain helper only accepts int keys.
        } else if (c.encoding == ColumnarColumn::Encoding::kFlat &&
                   c.is_uniform &&
                   (c.type == ValueType::kInt64 ||
                    c.type == ValueType::kDate)) {
          const int64_t* ints = c.ints.data();
          if (!c.has_null) {
            for (size_t r = 0; r < chunk.rows; ++r) {
              update(ints[r]);
            }
          } else {
            const uint8_t* nulls = c.nulls.data();
            for (size_t r = 0; r < chunk.rows; ++r) {
              if (nulls[r] == 0) {
                update(ints[r]);
              }
            }
          }
        } else if (c.is_uniform && (c.type == ValueType::kInt64 ||
                                    c.type == ValueType::kDate)) {
          int64_t iv = 0;
          for (size_t r = 0; r < chunk.rows; ++r) {
            if (c.TryReadNonNullInt(r, &iv)) {
              update(iv);
            }
          }
        } else {
          Value v;
          for (size_t r = 0; r < chunk.rows; ++r) {
            c.WriteValueInto(r, &v);
            if (!v.IsNull()) {
              update(v.value.int_value);
            }
          }
        }
      }
      *out_min = min_v;
      *out_max = max_v;
      return min_v <= max_v;
    }
    for (const Row& r : rel.rows) {
      const Value& kv = r[col];
      if (!kv.IsNull()) {
        update(kv.value.int_value);
      }
    }
    *out_min = min_v;
    *out_max = max_v;
    return min_v <= max_v;
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
    if (relations[drel].FinishSpill() != Status::kSuccess) {
      return entry;
    }
    const auto observe_val = [&](int64_t iv) { builder.Observe(iv); };
    Status iteration = Status::kSuccess;
    if (relations[drel].HasColumnarOnly()) {
      for (const ColumnarChunk& chunk : relations[drel].columnar->chunks) {
        if (chunk.rows == 0 || dcol >= chunk.columns.size()) {
          continue;
        }
        const ColumnarColumn& c = chunk.columns[dcol];
        if (c.encoding == ColumnarColumn::Encoding::kConstant && c.is_uniform) {
          if (c.const_is_null || c.type == ValueType::kNull) {
            builder.ObserveNull();
          } else if (c.type == ValueType::kInt64 ||
                     c.type == ValueType::kDate) {
            observe_val(c.const_int);
          } else {
            Value v;
            c.WriteValueInto(0, &v);
            if (v.IsNull()) {
              builder.ObserveNull();
            } else {
              observe_val(v.value.int_value);
            }
          }
        } else if (c.encoding == ColumnarColumn::Encoding::kFlat &&
                   c.is_uniform &&
                   (c.type == ValueType::kInt64 ||
                    c.type == ValueType::kDate)) {
          const int64_t* ints = c.ints.data();
          if (!c.has_null) {
            builder.Observe(std::span<const int64_t>(ints, chunk.rows));
          } else {
            const uint8_t* nulls = c.nulls.data();
            for (size_t r = 0; r < chunk.rows; ++r) {
              if (nulls[r] != 0) {
                builder.ObserveNull();
              } else {
                observe_val(ints[r]);
              }
            }
          }
        } else if (c.is_uniform && (c.type == ValueType::kInt64 ||
                                    c.type == ValueType::kDate)) {
          std::array<int64_t, 1024> keys{};
          size_t count = 0;
          for (size_t r = 0; r < chunk.rows; ++r) {
            int64_t iv = 0;
            if (!c.TryReadNonNullInt(r, &iv)) {
              builder.ObserveNull();
            } else {
              keys[count++] = iv;
              if (count == keys.size()) {
                builder.Observe(std::span<const int64_t>(keys.data(), count));
                count = 0;
              }
            }
          }
          builder.Observe(std::span<const int64_t>(keys.data(), count));
        } else {
          Value v;
          for (size_t r = 0; r < chunk.rows; ++r) {
            c.WriteValueInto(r, &v);
            if (v.IsNull()) {
              builder.ObserveNull();
            } else {
              observe_val(v.value.int_value);
            }
          }
        }
      }
    } else {
      iteration = relations[drel].ForEachRow([&](const Row& row) {
        const Value& value = row[dcol];
        if (value.IsNull()) {
          builder.ObserveNull();
        } else {
          observe_val(value.value.int_value);
        }
      });
    }
    if (iteration != Status::kSuccess || builder.empty() ||
        builder.overflowed()) {
      return entry;
    }
    entry.filter = builder.Build(false, 65536);
    if (entry.filter == nullptr || entry.filter->empty() ||
        entry.filter->Has(RuntimeFilterKind::kRejectAll)) {
      entry.filter.reset();
      return entry;
    }
    entry.observed_min = entry.filter->Lo();
    entry.observed_max = entry.filter->Hi();
    entry.saw_null = builder.saw_null();
    entry.distinct_keys = entry.filter->DistinctKeys();
    entry.cand_exact =
        !entry.saw_null && (entry.filter->has_dense_bits() ||
                            entry.filter->ExactKeys() != nullptr);
    entry.valid = true;
    return entry;
  };
  std::vector<uint8_t> absorbed(relations.size(), 0);
  std::vector<uint8_t> payload_absorbed_into(relations.size(), 0);
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
  const bool all_inner_equi_sources =
      std::ranges::all_of(statement.Sources(), [](const SelectSource& s) {
        return (s.join_type == JoinType::kInner ||
                s.join_type == JoinType::kCross) &&
               s.using_columns.empty() && !s.from_nested_join &&
               !s.sample.has_value();
      });
  std::unordered_set<ColumnName> outer_referenced;
  const auto collect_expr_cols = [&](const Expression& expr) {
    if (!expr) {
      return;
    }
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
  std::optional<size_t> deferred_probe_rel;
  std::optional<DeferredLSTableProbe> deferred_probe;
  const auto rel_row_count = [&](size_t r) -> size_t {
    return (deferred_probe_rel.has_value() && *deferred_probe_rel == r &&
            deferred_probe.has_value() && deferred_probe->store != nullptr)
               ? static_cast<size_t>(deferred_probe->store->total_rows)
               : relations[r].TotalRows();
  };
  const auto materialize_deferred = [&]() -> Status {
    struct DeferredTimer {
      ExecutionRuntime* runtime{nullptr};
      std::chrono::steady_clock::time_point begin{
          std::chrono::steady_clock::now()};
      explicit DeferredTimer(ExecutionRuntime* rt) : runtime(rt) {}
      DeferredTimer(const DeferredTimer&) = delete;
      DeferredTimer& operator=(const DeferredTimer&) = delete;
      DeferredTimer(DeferredTimer&&) = delete;
      DeferredTimer& operator=(DeferredTimer&&) = delete;
      ~DeferredTimer() {
        if (runtime != nullptr) {
          runtime->deferred_materialize_ms +=
              std::chrono::duration<double, std::milli>(
                  std::chrono::steady_clock::now() - begin)
                  .count();
        }
      }
    } deferred_timer(context.execution_runtime());
    if (!deferred_probe_rel.has_value() || !deferred_probe.has_value()) {
      return Status::kSuccess;
    }
    const size_t d_idx = *deferred_probe_rel;
    if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr) {
      std::cerr << "[deferred] materialize table="
                << statement.Sources()[d_idx].table
                << " rows=" << deferred_probe->store->total_rows
                << " pk=" << deferred_probe->pk_lookups.size() << "\n";
    }
    DeferredLSTableProbe dp = std::move(*deferred_probe);
    deferred_probe.reset();
    deferred_probe_rel.reset();
    const bool want_columnar =
        dp.columnar_output ||
        ((columnar_output || (fusion != nullptr && fusion->eligible)) &&
         plain_inner_sources && !selects_star && join_residual.empty());
    Schema saved_schema = std::move(relations[d_idx].schema);
    ASSIGN_OR_RETURN(Relation, loaded_rel,
                     (LoadSource(context, statement.Sources()[d_idx], outer,
                                 ctes, &dp.projection, &dp.local_preds,
                                 &dp.runtime_filter, nullptr, want_columnar)));
    // A sticky spill error means LoadSource dropped rows; never resolve
    // lookups over a silently truncated relation.
    RETURN_IF_FAIL(loaded_rel.GetStatus());
    if (!dp.pk_lookups.empty()) {
      const size_t final_cols = saved_schema.ColumnCount();
      // The PK-lookup path below mutates the ColumnarTable through a
      // const_pointer_cast, so it is only safe while this Relation holds the
      // sole reference; a table shared with a CTE cell or retained store must
      // fall back to the row path instead.
      bool can_columnar_pk = !loaded_rel.HasSpill() &&
                             loaded_rel.HasColumnarOnly() &&
                             loaded_rel.columnar != nullptr &&
                             loaded_rel.columnar.use_count() == 1;
      if (can_columnar_pk) {
        for (const DeferredLSTablePkLookup& lkp : dp.pk_lookups) {
          if (lkp.store == nullptr || !lkp.store->HasColumn(lkp.store_pk_col) ||
              !lkp.store->HasColumn(lkp.payload_col) ||
              lkp.store_pk_col >= lkp.store->column_min_int.size() ||
              lkp.store_pk_col >= lkp.store->column_max_int.size() ||
              lkp.target_proj_col >= final_cols) {
            can_columnar_pk = false;
            break;
          }
          for (const LSTablePageBlock& blk : lkp.store->blocks) {
            if (blk.row_count == 0) {
              continue;
            }
            if (lkp.store_pk_col >= blk.columns.size() ||
                lkp.payload_col >= blk.columns.size() ||
                blk.columns[lkp.store_pk_col].int_cells.size() <
                    blk.row_count ||
                (lkp.payload_type != ValueType::kVarChar &&
                 blk.columns[lkp.payload_col].int_cells.size() <
                     blk.row_count)) {
              can_columnar_pk = false;
              break;
            }
          }
          if (!can_columnar_pk) {
            break;
          }
          for (const ColumnarChunk& ch : loaded_rel.columnar->chunks) {
            if (ch.rows > 0 && lkp.pk_col >= ch.columns.size()) {
              can_columnar_pk = false;
              break;
            }
          }
          if (!can_columnar_pk) {
            break;
          }
        }
      }
      if (can_columnar_pk) {
        auto mutable_tbl =
            std::const_pointer_cast<ColumnarTable>(loaded_rel.columnar);
        auto& chunks = mutable_tbl->chunks;
        for (ColumnarChunk& ch : chunks) {
          if (ch.columns.size() < final_cols) {
            ch.columns.resize(final_cols);
          }
        }
        const size_t nchunks = chunks.size();
        const size_t nworkers = std::max<size_t>(
            size_t{1},
            std::min<size_t>(
                {size_t{16},
                 QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 nchunks}));
        for (const DeferredLSTablePkLookup& lkp : dp.pk_lookups) {
          struct DeferredBlkMeta {
            int64_t min_k{0};
            int64_t max_k{0};
            uint32_t row_count{0};
            uint8_t dense_contiguous{0};
            uint8_t has_null_payload{0};
            const int64_t* pk_cells{nullptr};
            const int64_t* pay_cells{nullptr};
            const LSTableColumnBlock* pay_blk{nullptr};
          };
          std::vector<DeferredBlkMeta> blk_meta;
          blk_meta.reserve(lkp.store->blocks.size());
          std::vector<int64_t> blk_max_keys;
          blk_max_keys.reserve(lkp.store->blocks.size());
          for (const LSTablePageBlock& blk : lkp.store->blocks) {
            if (blk.row_count == 0) {
              continue;
            }
            const LSTableColumnBlock& pk_b = blk.columns[lkp.store_pk_col];
            const LSTableColumnBlock& pay_b = blk.columns[lkp.payload_col];
            const bool is_dense =
                (static_cast<uint64_t>(pk_b.max_int) -
                     static_cast<uint64_t>(pk_b.min_int) + 1ULL ==
                 blk.row_count);
            blk_meta.push_back(DeferredBlkMeta{
                .min_k = pk_b.min_int,
                .max_k = pk_b.max_int,
                .row_count = blk.row_count,
                .dense_contiguous = static_cast<uint8_t>(is_dense),
                .has_null_payload = static_cast<uint8_t>(pay_b.has_null),
                .pk_cells = pk_b.int_cells.data(),
                .pay_cells = pay_b.int_cells.data(),
                .pay_blk = &pay_b});
            blk_max_keys.push_back(pk_b.max_int);
          }
          const int64_t min_k = lkp.store->column_min_int[lkp.store_pk_col];
          const int64_t max_k = lkp.store->column_max_int[lkp.store_pk_col];
          if (blk_meta.empty() || min_k > max_k) {
            chunks.clear();
            mutable_tbl->total_rows = 0;
            break;
          }
          const __uint128_t span =
              static_cast<__uint128_t>(static_cast<uint64_t>(max_k) -
                                       static_cast<uint64_t>(min_k)) +
              1;
          constexpr size_t kDirShift = 12U;
          constexpr size_t kDirBuckets = size_t{1} << kDirShift;
          std::array<uint32_t, kDirBuckets + 1> bucket_lo{};
          const auto nb_count = static_cast<uint32_t>(blk_meta.size());
          uint32_t cur_nb = 0;
          for (size_t b = 0; b < kDirBuckets; ++b) {
            const int64_t b_min =
                min_k + static_cast<int64_t>(
                            (static_cast<__uint128_t>(span) * b) >> kDirShift);
            while (cur_nb + 1 < nb_count && blk_max_keys[cur_nb] < b_min) {
              ++cur_nb;
            }
            bucket_lo[b] = cur_nb;
          }
          bucket_lo[kDirBuckets] = nb_count - 1;
          const ValueType p_type = lkp.payload_type;
          const bool p_unsigned = lkp.payload_unsigned;
          const DeferredBlkMeta* const meta_ptr = blk_meta.data();

          auto run_col_lookup = [&](size_t w) {
            for (size_t ci = w; ci < nchunks; ci += nworkers) {
              ColumnarChunk& ch = chunks[ci];
              if (ch.rows == 0) {
                continue;
              }
              const ColumnarColumn& key_col = ch.columns[lkp.pk_col];
              const bool flat_int_key =
                  key_col.encoding == ColumnarColumn::Encoding::kFlat &&
                  key_col.is_uniform && !key_col.is_interval &&
                  (key_col.type == ValueType::kInt64 ||
                   key_col.type == ValueType::kDate) &&
                  key_col.ints.size() >= ch.rows &&
                  (!key_col.has_null || key_col.nulls.size() >= ch.rows);
              const int64_t* const kcells =
                  flat_int_key ? key_col.ints.data() : nullptr;
              const uint8_t* const knulls = (flat_int_key && key_col.has_null)
                                                ? key_col.nulls.data()
                                                : nullptr;

              ColumnarColumn out_col;
              out_col.type = p_type;
              out_col.is_uniform = true;
              out_col.is_unsigned = p_unsigned;
              out_col.has_null = false;
              out_col.encoding = ColumnarColumn::Encoding::kFlat;
              if (p_type == ValueType::kInt64 || p_type == ValueType::kDate) {
                out_col.ints.resize(ch.rows);
              } else if (p_type == ValueType::kDouble) {
                out_col.doubles.resize(ch.rows);
              } else if (p_type == ValueType::kVarChar) {
                out_col.strings.resize(ch.rows);
              }
              ChunkStringArena* out_arena = (p_type == ValueType::kVarChar)
                                                ? out_col.EnsureLocalArena()
                                                : nullptr;
              std::vector<uint8_t> matched_mask;
              bool had_unmatched = false;
              int64_t last_key = std::numeric_limits<int64_t>::min();
              bool have_last = false;
              bool last_matched = false;
              bool last_is_null = false;
              int64_t last_raw_bits = 0;
              std::string_view last_sv;
              uint32_t last_nb = 0;
              uint32_t last_row_in_blk = 0;
              auto mark_unmatched = [&](size_t r) {
                if (!had_unmatched) {
                  matched_mask.assign(ch.rows, 1);
                  had_unmatched = true;
                }
                matched_mask[r] = 0;
              };
              auto mark_out_null = [&](size_t r) {
                if (!out_col.has_null) {
                  out_col.has_null = true;
                  out_col.nulls.assign(ch.rows, 0);
                }
                out_col.nulls[r] = 1;
              };
              for (size_t r = 0; r < ch.rows; ++r) {
                int64_t k = 0;
                if (flat_int_key) {
                  if ((knulls != nullptr && knulls[r] != 0) ||
                      kcells[r] < min_k || kcells[r] > max_k) [[unlikely]] {
                    mark_unmatched(r);
                    continue;
                  }
                  k = kcells[r];
                } else {
                  if (!key_col.TryReadNonNullInt(r, &k) || k < min_k ||
                      k > max_k) [[unlikely]] {
                    mark_unmatched(r);
                    continue;
                  }
                }
                if (!have_last || k != last_key) {
                  bool same_blk_mono = false;
                  uint32_t nb = 0;
                  if (have_last && k >= meta_ptr[last_nb].min_k &&
                      k <= meta_ptr[last_nb].max_k) {
                    nb = last_nb;
                    same_blk_mono = (k > last_key);
                  } else {
                    const auto b_idx = static_cast<size_t>(
                        (static_cast<__uint128_t>(static_cast<uint64_t>(k) -
                                                  static_cast<uint64_t>(min_k))
                         << kDirShift) /
                        span);
                    nb = bucket_lo[b_idx];
                    while (nb + 1 < nb_count && meta_ptr[nb].max_k < k) {
                      ++nb;
                    }
                  }
                  last_key = k;
                  have_last = true;
                  last_nb = nb;
                  const DeferredBlkMeta& bm = meta_ptr[nb];
                  if (k < bm.min_k || k > bm.max_k) [[unlikely]] {
                    last_matched = false;
                  } else {
                    uint32_t row_in_blk = bm.row_count;
                    if (bm.dense_contiguous != 0) {
                      row_in_blk = static_cast<uint32_t>(
                          static_cast<uint64_t>(k) -
                          static_cast<uint64_t>(bm.min_k));
                    } else {
                      const int64_t* const bp = bm.pk_cells;
                      const int64_t* const ep = bp + bm.row_count;
                      const int64_t* sb = bp;
                      const int64_t* se = ep;
                      if (same_blk_mono && last_row_in_blk < bm.row_count) {
                        const uint32_t cur_r = last_row_in_blk;
                        if (bp[cur_r] == k) {
                          row_in_blk = cur_r;
                          sb = ep;
                        } else if (bp[cur_r] < k) {
                          if (cur_r + 1 < bm.row_count && bp[cur_r + 1] == k) {
                            row_in_blk = cur_r + 1;
                            sb = ep;
                          } else {
                            sb = bp + cur_r + 1;
                            if (cur_r + 16 < bm.row_count &&
                                bp[cur_r + 16] >= k) {
                              se = bp + cur_r + 17;
                            }
                          }
                        }
                      }
                      if (sb < se) {
                        const int64_t* const fp = std::lower_bound(sb, se, k);
                        last_row_in_blk = static_cast<uint32_t>(fp - bp);
                        if (fp != se && *fp == k) [[likely]] {
                          row_in_blk = last_row_in_blk;
                        }
                      } else {
                        last_row_in_blk = row_in_blk;
                      }
                    }
                    if (row_in_blk >= bm.row_count) [[unlikely]] {
                      last_matched = false;
                    } else {
                      last_matched = true;
                      last_is_null = (bm.has_null_payload != 0) &&
                                     bm.pay_blk->IsNull(row_in_blk);
                      if (!last_is_null) [[likely]] {
                        if (p_type == ValueType::kVarChar) {
                          last_sv = bm.pay_blk->VarcharAt(row_in_blk);
                        } else {
                          last_raw_bits = bm.pay_cells[row_in_blk];
                        }
                      }
                    }
                  }
                }
                if (!last_matched) [[unlikely]] {
                  mark_unmatched(r);
                  continue;
                }
                if (last_is_null) [[unlikely]] {
                  mark_out_null(r);
                } else if (p_type == ValueType::kInt64 ||
                           p_type == ValueType::kDate) {
                  out_col.ints[r] = last_raw_bits;
                } else if (p_type == ValueType::kDouble) {
                  out_col.doubles[r] = std::bit_cast<double>(last_raw_bits);
                } else if (p_type == ValueType::kVarChar) {
                  out_col.strings[r] = out_arena->Store(last_sv);
                }
              }
              ch.columns[lkp.target_proj_col] = std::move(out_col);
              if (had_unmatched) {
                size_t kept = 0;
                for (size_t r = 0; r < ch.rows; ++r) {
                  kept += matched_mask[r];
                }
                for (ColumnarColumn& col : ch.columns) {
                  if (!col.is_uniform) {
                    if (col.fallback.size() == ch.rows) {
                      size_t out_i = 0;
                      for (size_t r = 0; r < ch.rows; ++r) {
                        if (matched_mask[r] != 0) {
                          if (out_i != r) {
                            col.fallback[out_i] = std::move(col.fallback[r]);
                          }
                          ++out_i;
                        }
                      }
                      col.fallback.resize(out_i);
                    }
                    continue;
                  }
                  if (col.encoding == ColumnarColumn::Encoding::kConstant) {
                    col.const_rows = kept;
                    continue;
                  }
                  if (col.type == ValueType::kNull &&
                      col.encoding == ColumnarColumn::Encoding::kFlat &&
                      col.nulls.empty()) {
                    continue;
                  }
                  if (col.encoding != ColumnarColumn::Encoding::kFlat) {
                    col.MaterializeToFlat(ch.rows);
                  }
                  if (col.has_null && col.nulls.size() == ch.rows) {
                    size_t out_i = 0;
                    for (size_t r = 0; r < ch.rows; ++r) {
                      if (matched_mask[r] != 0) {
                        col.nulls[out_i++] = col.nulls[r];
                      }
                    }
                    col.nulls.resize(out_i);
                  }
                  if ((col.type == ValueType::kInt64 ||
                       col.type == ValueType::kDate) &&
                      col.ints.size() == ch.rows) {
                    size_t out_i = 0;
                    for (size_t r = 0; r < ch.rows; ++r) {
                      if (matched_mask[r] != 0) {
                        col.ints[out_i++] = col.ints[r];
                      }
                    }
                    col.ints.resize(out_i);
                  } else if (col.type == ValueType::kDouble &&
                             col.doubles.size() == ch.rows) {
                    size_t out_i = 0;
                    for (size_t r = 0; r < ch.rows; ++r) {
                      if (matched_mask[r] != 0) {
                        col.doubles[out_i++] = col.doubles[r];
                      }
                    }
                    col.doubles.resize(out_i);
                  } else if (col.type == ValueType::kVarChar &&
                             col.strings.size() == ch.rows) {
                    size_t out_i = 0;
                    for (size_t r = 0; r < ch.rows; ++r) {
                      if (matched_mask[r] != 0) {
                        col.strings[out_i++] = col.strings[r];
                      }
                    }
                    col.strings.resize(out_i);
                  }
                }
                ch.rows = kept;
              }
            }
          };
          if (nworkers <= 1) {
            run_col_lookup(0);
          } else {
            std::vector<std::jthread> threads;
            threads.reserve(nworkers - 1);
            for (size_t w = 1; w < nworkers; ++w) {
              threads.emplace_back([&, w] { run_col_lookup(w); });
            }
            run_col_lookup(0);
          }
        }
        size_t new_total = 0;
        for (const ColumnarChunk& ch : chunks) {
          new_total += ch.rows;
        }
        mutable_tbl->total_rows = new_total;
        loaded_rel.peak_intermediate_rows =
            std::max(loaded_rel.peak_intermediate_rows, new_total);
        loaded_rel.schema = std::move(saved_schema);
      } else {
        if (loaded_rel.HasColumnarOnly()) {
          RETURN_IF_FAIL(loaded_rel.EnsureRowsMaterializedChecked());
        }
        if (loaded_rel.HasSpill()) {
          // The PK-lookup rewrite below edits `rows` in place and only sees
          // the in-memory part; under memory pressure LoadSource may have
          // spilled (EnsureSpill clears `rows`). Bring every row back before
          // rewriting.
          std::vector<Row> all_rows;
          all_rows.reserve(loaded_rel.TotalRows());
          RETURN_IF_FAIL(loaded_rel.ForEachRow(
              [&](const Row& row) { all_rows.push_back(row); }));
          loaded_rel.ResetContents();
          loaded_rel.rows = std::move(all_rows);
        }
        size_t out_idx = 0;
        for (size_t ri = 0; ri < loaded_rel.rows.size(); ++ri) {
          Row& row = loaded_rel.rows[ri];
          if (row.values_.size() < final_cols) {
            row.values_.resize(final_cols);
          }
          bool row_matched = true;
          for (const DeferredLSTablePkLookup& lkp : dp.pk_lookups) {
            const Value& pk_v = row.values_[lkp.pk_col];
            if (pk_v.IsNull()) {
              row_matched = false;
              break;
            }
            const int64_t k = pk_v.value.int_value;
            const auto& blks = lkp.store->blocks;
            auto it = std::lower_bound(
                blks.begin(), blks.end(), k,
                [&](const LSTablePageBlock& b, int64_t key) {
                  return b.row_count == 0 ||
                         b.columns[lkp.store_pk_col].max_int < key;
                });
            if (it == blks.end() || it->row_count == 0) {
              row_matched = false;
              break;
            }
            const LSTableColumnBlock& pk_b = it->columns[lkp.store_pk_col];
            if (k < pk_b.min_int || k > pk_b.max_int) {
              row_matched = false;
              break;
            }
            const int64_t* begin_p = pk_b.int_cells.data();
            const int64_t* end_p = begin_p + it->row_count;
            const int64_t* found_p = std::lower_bound(begin_p, end_p, k);
            if (found_p == end_p || *found_p != k) {
              row_matched = false;
              break;
            }
            const auto r_in_b = static_cast<size_t>(found_p - begin_p);
            const LSTableColumnBlock& pay_b = it->columns[lkp.payload_col];
            Value resolved;
            if (!pay_b.IsNull(r_in_b)) {
              if (lkp.payload_type == ValueType::kVarChar) {
                const std::string_view sv = pay_b.VarcharAt(r_in_b);
                resolved.type = ValueType::kVarChar;
                resolved.value.varchar_value = sv;
              } else if (lkp.payload_type == ValueType::kDouble) {
                resolved =
                    Value(std::bit_cast<double>(pay_b.int_cells[r_in_b]));
              } else if (lkp.payload_type == ValueType::kDate) {
                resolved = Value::DateFromDays(pay_b.int_cells[r_in_b]);
              } else {
                resolved = Value(pay_b.int_cells[r_in_b]);
                if (lkp.payload_unsigned) {
                  resolved.SetUnsigned();
                }
              }
            }
            row.values_[lkp.target_proj_col] = std::move(resolved);
          }
          if (row_matched) {
            if (out_idx != ri) {
              loaded_rel.rows[out_idx] = std::move(row);
            }
            ++out_idx;
          }
        }
        loaded_rel.rows.resize(out_idx);
        loaded_rel.schema = std::move(saved_schema);
      }
    }
    relations[d_idx] = std::move(loaded_rel);
    invalidate_driver_filter_cache(d_idx);
    return Status::kSuccess;
  };
  Schema empty_input_schema;
  for (const Relation& relation : relations) {
    empty_input_schema = empty_input_schema.ColumnCount() == 0
                             ? relation.schema
                             : empty_input_schema + relation.schema;
  }
  std::set<std::pair<size_t, size_t>> pushed_small_domain_edges;
  const auto propagate_small_domain = [&](size_t src_idx) {
    if (outer != nullptr || !plain_inner_sources ||
        relations[src_idx].TotalRows() == 0 ||
        relations[src_idx].TotalRows() > 128 || relations[src_idx].HasSpill()) {
      return;
    }
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 ||
          !predicate.sources.contains(src_idx) ||
          !IsColumnEqualityPredicate(predicate.expression) ||
          predicate.expression->AsBinaryExpression().Op() !=
              BinaryOperation::kEquals) {
        continue;
      }
      size_t other = *predicate.sources.begin();
      if (other == src_idx) {
        other = *std::next(predicate.sources.begin());
      }
      if (loaded[other] || !base_sources[other] || deferred_derived[other] ||
          absorbed[other] != 0) {
        continue;
      }
      const BinaryExpression& bin = predicate.expression->AsBinaryExpression();
      const Expression& lexp = bin.Left();
      const Expression& rexp = bin.Right();
      const auto l_src = LocalColumnOffset(
          relations[src_idx].schema, lexp->AsColumnValue().GetColumnName());
      const auto r_oth = LocalColumnOffset(
          relations[other].schema, rexp->AsColumnValue().GetColumnName());
      const auto r_src = LocalColumnOffset(
          relations[src_idx].schema, rexp->AsColumnValue().GetColumnName());
      const auto l_oth = LocalColumnOffset(
          relations[other].schema, lexp->AsColumnValue().GetColumnName());
      const bool lr_match = l_src.has_value() && r_oth.has_value();
      const bool rl_match = r_src.has_value() && l_oth.has_value();
      if (lr_match == rl_match) {
        continue;
      }
      const size_t own_col = lr_match ? *l_src : *r_src;
      const size_t other_col = lr_match ? *r_oth : *l_oth;
      const Expression& other_expr = lr_match ? rexp : lexp;
      if (own_col >= relations[src_idx].schema.ColumnCount() ||
          other_col >= relations[other].schema.ColumnCount()) {
        continue;
      }
      const Column& own_c = relations[src_idx].schema.GetColumn(own_col);
      const Column& other_c = relations[other].schema.GetColumn(other_col);
      if (own_c.IsInterval() || other_c.IsInterval() ||
          own_c.Type() != other_c.Type() ||
          own_c.IsUnsigned() != other_c.IsUnsigned()) {
        continue;
      }
      // Signed integer/date equijoins already use RuntimeFilter +
      // filter_neighbours; push scalar IN filters for VarChar / Double /
      // unsigned equijoins that RuntimeFilter skips.
      if (own_c.Type() != ValueType::kVarChar &&
          own_c.Type() != ValueType::kDouble &&
          (!own_c.IsUnsigned() || (own_c.Type() != ValueType::kInt64 &&
                                   own_c.Type() != ValueType::kDate))) {
        continue;
      }
      if (pushed_small_domain_edges.contains({other, other_col})) {
        continue;
      }
      constexpr size_t kMaxSmallDomainValues = 32;
      std::vector<Expression> in_items;
      std::unordered_set<std::string> seen_strings;
      std::unordered_set<uint64_t> seen_scalars;
      bool valid = true;
      const Status domain_iter =
          relations[src_idx].ForEachRow([&](const Row& row) {
            if (!valid) {
              return;
            }
            const Value& v = row[own_col];
            if (v.IsNull()) {
              return;
            }
            if (own_c.Type() == ValueType::kVarChar) {
              if (v.Collation() != 0) {
                valid = false;
                return;
              }
              if (seen_strings.emplace(v.value.varchar_value).second) {
                if (seen_strings.size() > kMaxSmallDomainValues) {
                  valid = false;
                  return;
                }
                in_items.push_back(ConstantValueExp(
                    Value(std::string(v.value.varchar_value))));
              }
            } else if (own_c.Type() == ValueType::kDouble) {
              double d = v.value.double_value;
              if (std::isnan(d)) {
                return;
              }
              if (d == 0.0) {
                d = 0.0;
              }
              const auto bits = std::bit_cast<uint64_t>(d);
              if (seen_scalars.insert(bits).second) {
                if (seen_scalars.size() > kMaxSmallDomainValues) {
                  valid = false;
                  return;
                }
                in_items.push_back(ConstantValueExp(Value(d)));
              }
            } else {
              const auto bits = static_cast<uint64_t>(v.value.int_value);
              if (seen_scalars.insert(bits).second) {
                if (seen_scalars.size() > kMaxSmallDomainValues) {
                  valid = false;
                  return;
                }
                Value c = (own_c.Type() == ValueType::kDate)
                              ? Value::DateFromDays(v.value.int_value)
                              : Value(v.value.int_value);
                if (own_c.IsUnsigned()) {
                  c.SetUnsigned();
                }
                in_items.push_back(ConstantValueExp(c));
              }
            }
          });
      if (!domain_iter.ok() || !valid || in_items.empty()) {
        continue;
      }
      const Expression probe_cmp = BinaryExpressionExp(
          other_expr, BinaryOperation::kEquals, in_items.front());
      if (!IsNonThrowingScanComparison(probe_cmp, relations[other].schema)) {
        continue;
      }
      pushed_small_domain_edges.emplace(other, other_col);
      local_predicates[other].push_back(InExpressionExp(other_expr, in_items));
      small_domain_filtered[other] = 1;
      const double est_other =
          std::max<double>(1.0, static_cast<double>(estimated_rows[other]));
      const double ratio = std::min(
          0.1,
          std::max(0.001, static_cast<double>(in_items.size()) / est_other));
      small_domain_ratio[other] = std::min(small_domain_ratio[other], ratio);
    }
  };
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/SelfJoinSharedScanDualFilterSplit.lean
  // (sound) Self-join shared scan with dual local predicate split (#13, e.g. L1
  // person p/p2, C2 users u/u2): reads a repeated base table once into the
  // union of needed columns and splits surviving projected rows by each alias's
  // local predicates.
  static const bool self_join_shared_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SELF_JOIN_SHARED_SCAN") !=
      nullptr;
  if (!self_join_shared_disabled && outer == nullptr && ctes.empty() &&
      plain_inner_sources && !selects_star && *where_fully_applied &&
      relations.size() >= 2) {
    for (size_t i = 0; i < relations.size(); ++i) {
      if (!base_sources[i] || loaded[i] || deferred_derived[i] ||
          projections[i].empty() || statement.Sources()[i].sample.has_value() ||
          !post_load_exists_predicates[i].empty() ||
          InputTupleFilter(statement, fusion, i) != nullptr ||
          estimated_rows[i] > 200000) {
        continue;
      }
      const std::string& tbl_name = statement.Sources()[i].table;
      if (tbl_name.empty()) {
        continue;
      }
      if (context.execution_runtime() != nullptr) {
        const auto& filters = context.execution_runtime()->table_key_filters;
        const auto sit =
            filters.find(TableKeyFilterKey(statement.Sources()[i]));
        if (sit != filters.end() && sit->second.OwnedBy(statement) &&
            !sit->second.empty()) {
          continue;
        }
      }
      for (size_t j = i + 1; j < relations.size(); ++j) {
        if (!base_sources[j] || loaded[j] || deferred_derived[j] ||
            projections[j].empty() ||
            statement.Sources()[j].table != tbl_name ||
            statement.Sources()[j].sample.has_value() ||
            !post_load_exists_predicates[j].empty() ||
            InputTupleFilter(statement, fusion, j) != nullptr ||
            estimated_rows[j] > 200000) {
          continue;
        }
        if (context.execution_runtime() != nullptr) {
          const auto& filters = context.execution_runtime()->table_key_filters;
          const auto sjt =
              filters.find(TableKeyFilterKey(statement.Sources()[j]));
          if (sjt != filters.end() && sjt->second.OwnedBy(statement) &&
              !sjt->second.empty()) {
            continue;
          }
        }
        // Build projected schemas and compile local predicates for both sides.
        const Schema schema_i =
            ProjectSchema(relations[i].schema, projections[i]);
        const Schema schema_j =
            ProjectSchema(relations[j].schema, projections[j]);
        std::vector<BytecodeProgram> progs_i;
        std::vector<BytecodeProgram> progs_j;
        bool compile_ok = true;
        for (const Expression& lp : local_predicates[i]) {
          if (!lp || ContainsQuery(lp) || ContainsParameterSlot(lp)) {
            compile_ok = false;
            break;
          }
          auto compiled = BytecodeCompiler::CompileRowCached(
              lp, schema_i, BytecodeCompiler::Context::kFilter);
          if (!compiled.has_value()) {
            compile_ok = false;
            break;
          }
          progs_i.push_back(std::move(*compiled));
        }
        for (size_t k = 0; compile_ok && k < local_predicates[j].size(); ++k) {
          const Expression& lp = local_predicates[j][k];
          if (!lp || ContainsQuery(lp) || ContainsParameterSlot(lp)) {
            compile_ok = false;
            break;
          }
          auto compiled = BytecodeCompiler::CompileRowCached(
              lp, schema_j, BytecodeCompiler::Context::kFilter);
          if (!compiled.has_value()) {
            compile_ok = false;
            break;
          }
          progs_j.push_back(std::move(*compiled));
        }
        if (!compile_ok) {
          continue;
        }
        // Union of projected base-table columns.
        std::vector<slot_t> union_proj = projections[i];
        for (const slot_t s : projections[j]) {
          if (std::ranges::find(union_proj, s) == union_proj.end()) {
            union_proj.push_back(s);
          }
        }
        std::ranges::sort(union_proj);
        std::vector<size_t> pos_i(projections[i].size());
        std::vector<size_t> pos_j(projections[j].size());
        for (size_t k = 0; k < projections[i].size(); ++k) {
          pos_i[k] = static_cast<size_t>(
              std::distance(union_proj.begin(),
                            std::ranges::find(union_proj, projections[i][k])));
        }
        for (size_t k = 0; k < projections[j].size(); ++k) {
          pos_j[k] = static_cast<size_t>(
              std::distance(union_proj.begin(),
                            std::ranges::find(union_proj, projections[j][k])));
        }
        ASSIGN_OR_RETURN(
            Relation, raw_scan,
            (LoadSource(context, statement.Sources()[i], outer, ctes,
                        &union_proj, nullptr, nullptr, nullptr,
                        /*columnar_output=*/false)));
        RETURN_IF_FAIL(raw_scan.GetStatus());
        if (raw_scan.HasSpill() || raw_scan.HasColumnarOnly()) {
          // If spilled or forced columnar, fall back cleanly on this pair.
          continue;
        }
        Relation rel_i(context.execution_runtime());
        rel_i.schema = schema_i;
        Relation rel_j(context.execution_runtime());
        rel_j.schema = schema_j;
        rel_i.rows.reserve((raw_scan.rows.size() / 2) + 16);
        rel_j.rows.reserve((raw_scan.rows.size() / 2) + 16);
        const size_t wi = projections[i].size();
        const size_t wj = projections[j].size();
        Row cand_i(std::vector<Value>(wi, Value()));
        Row cand_j(std::vector<Value>(wj, Value()));
        for (const Row& raw : raw_scan.rows) {
          auto copy_owned_cell = [](const Value& src, Value* dst) {
            *dst = src;
            if (dst->type == ValueType::kVarChar && dst->owned_data.empty() &&
                !dst->value.varchar_value.empty()) {
              dst->owned_data.assign(dst->value.varchar_value);
              dst->value.varchar_value = dst->owned_data;
            }
          };
          if (!progs_i.empty() || !progs_j.empty()) {
            for (size_t k = 0; k < wi; ++k) {
              copy_owned_cell(raw[pos_i[k]], &cand_i.values_[k]);
            }
            bool keep_i = true;
            for (const BytecodeProgram& prog : progs_i) {
              ASSIGN_OR_RETURN(Value, fv, prog.TryEvaluateRow(cand_i));
              if (!Truthy(fv)) {
                keep_i = false;
                break;
              }
            }
            if (keep_i) {
              rel_i.AddRow(cand_i);
            }
            for (size_t k = 0; k < wj; ++k) {
              copy_owned_cell(raw[pos_j[k]], &cand_j.values_[k]);
            }
            bool keep_j = true;
            for (const BytecodeProgram& prog : progs_j) {
              ASSIGN_OR_RETURN(Value, fv, prog.TryEvaluateRow(cand_j));
              if (!Truthy(fv)) {
                keep_j = false;
                break;
              }
            }
            if (keep_j) {
              rel_j.AddRow(cand_j);
            }
          } else {
            for (size_t k = 0; k < wi; ++k) {
              copy_owned_cell(raw[pos_i[k]], &cand_i.values_[k]);
            }
            for (size_t k = 0; k < wj; ++k) {
              copy_owned_cell(raw[pos_j[k]], &cand_j.values_[k]);
            }
            rel_i.AddRow(cand_i);
            rel_j.AddRow(cand_j);
          }
        }
        relations[i] = std::move(rel_i);
        relations[j] = std::move(rel_j);
        loaded[i] = true;
        loaded[j] = true;
        if (relations[i].TotalRows() == 0 || relations[j].TotalRows() == 0) {
          Relation empty(context.execution_runtime());
          empty.schema = std::move(empty_input_schema);
          *where_fully_applied = true;
          if (pushed_projection_applied != nullptr) {
            *pushed_projection_applied = false;
          }
          if (auto* runtime = context.execution_runtime()) {
            runtime->empty_build_short_circuit = true;
            runtime->planning_ms += ElapsedMs(planning_begin);
          }
          return empty;
        }
        propagate_small_domain(i);
        propagate_small_domain(j);
        break;
      }
    }
  }
  for (size_t position = 0; position < load_order.size(); ++position) {
    for (size_t i = 0; i < selective.size(); ++i) {
      selective[i] = static_cast<uint8_t>(selective_source(i));
    }
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
      propagate_small_domain(idx);
      continue;
    }
    if (loaded[idx]) {
      continue;
    }
    if (!pk_absorption_disabled && *where_fully_applied &&
        plain_inner_sources && !selects_star && relations.size() >= 2 &&
        base_sources[idx] && !deferred_derived[idx] &&
        !statement.Sources()[idx].sample.has_value() &&
        local_predicates[idx].empty() &&
        post_load_exists_predicates[idx].empty() &&
        projections[idx].size() == 2) {
      size_t active_relations = 0;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (absorbed[i] == 0U) {
          ++active_relations;
        }
      }
      const bool has_stored_key_filter =
          context.execution_runtime() != nullptr && [&] {
            const auto stored =
                context.execution_runtime()->table_key_filters.find(
                    TableKeyFilterKey(statement.Sources()[idx]));
            return stored !=
                       context.execution_runtime()->table_key_filters.end() &&
                   stored->second.OwnedBy(statement);
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
          edges.reserve(predicates.size());
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
            if (absorbed[other] != 0U) {
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
            edges.push_back(
                PreJoinEdge{.d_col = *d_col, .other = other, .o_col = *o_col});
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
                        (relations[e.other].TotalRows() == 0 &&
                         deferred_probe_rel != e.other) ||
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
                          relations[e.other].TotalRows() == 0) {
                        edges_valid = false;
                        break;
                      }
                    } else if (!base_sources[e.other] ||
                               deferred_derived[e.other] ||
                               (selective[e.other] != 0U) ||
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
                if (deferred_probe_rel == *cand_t_rel &&
                    std::ranges::any_of(edges, [&](const PreJoinEdge& e) {
                      return e.d_col == cand_payload;
                    })) {
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
                  target_rel = cand_t_rel;
                  target_col = cand_t_col;
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
              bool columnar_tgt_ok = true;
              if (tgt.HasColumnarOnly()) {
                if (tgt.columnar == nullptr || tgt.columnar.use_count() != 1) {
                  columnar_tgt_ok = false;
                } else {
                  for (const auto& ch : tgt.columnar->chunks) {
                    if (ch.rows == 0) {
                      continue;
                    }
                    if (*target_col >= ch.columns.size()) {
                      columnar_tgt_ok = false;
                      break;
                    }
                    const auto& kc = ch.columns[*target_col];
                    if (!kc.is_uniform ||
                        kc.encoding != ColumnarColumn::Encoding::kFlat ||
                        (kc.type != ValueType::kInt64 &&
                         kc.type != ValueType::kDate)) {
                      columnar_tgt_ok = false;
                      break;
                    }
                  }
                }
              }
              bool name_collision = !columnar_tgt_ok;
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
                  !name_collision
                      ? GetOrBuildLSTableStore(context.txn_, *d_table.Value(),
                                               true, &projections[idx])
                      : nullptr;
              bool store_valid =
                  store != nullptr && store->total_rows > 0 &&
                  store->layout_generation ==
                      d_table.Value()->LayoutGeneration() &&
                  store->first_pid == d_table.Value()->FirstPageId() &&
                  store->HasColumn(*d_pk_col) &&
                  store->HasColumn(*d_payload_col) &&
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
                struct AbsorbBlockMeta {
                  int64_t min_k{0};
                  int64_t max_k{0};
                  uint32_t row_count{0};
                  uint8_t dense_contiguous{0};
                  uint8_t has_null_payload{0};
                  const int64_t* pk_cells{nullptr};
                  const int64_t* pay_cells{nullptr};
                  const LSTableColumnBlock* pay_blk{nullptr};
                };
                std::vector<AbsorbBlockMeta> blk_meta;
                blk_meta.reserve(store->blocks.size());
                std::vector<int64_t> blk_max_keys;
                blk_max_keys.reserve(store->blocks.size());
                for (const auto& blk : store->blocks) {
                  if (blk.row_count > 0) {
                    const LSTableColumnBlock& pk_b = blk.columns[*d_pk_col];
                    const LSTableColumnBlock& pay_b =
                        blk.columns[*d_payload_col];
                    const bool is_dense =
                        (static_cast<uint64_t>(pk_b.max_int) -
                             static_cast<uint64_t>(pk_b.min_int) + 1ULL ==
                         blk.row_count);
                    blk_meta.push_back(AbsorbBlockMeta{
                        .min_k = pk_b.min_int,
                        .max_k = pk_b.max_int,
                        .row_count = blk.row_count,
                        .dense_contiguous = static_cast<uint8_t>(is_dense),
                        .has_null_payload =
                            static_cast<uint8_t>(pay_b.has_null),
                        .pk_cells = pk_b.int_cells.data(),
                        .pay_cells = pay_b.int_cells.data(),
                        .pay_blk = &pay_b});
                    blk_max_keys.push_back(pk_b.max_int);
                  }
                }
                if (!blk_meta.empty()) {
                  const int64_t min_k = store->column_min_int[*d_pk_col];
                  const int64_t max_k = store->column_max_int[*d_pk_col];
                  const __uint128_t span =
                      static_cast<__uint128_t>(static_cast<uint64_t>(max_k) -
                                               static_cast<uint64_t>(min_k)) +
                      1;
                  constexpr size_t kDirShift = 12U;
                  constexpr size_t kDirBuckets = size_t{1} << kDirShift;
                  std::array<uint32_t, kDirBuckets + 1> bucket_lo{};
                  const auto nb_count = static_cast<uint32_t>(blk_meta.size());
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
                  if (p_type == ValueType::kVarChar ||
                      deferred_probe_rel == *target_rel) {
                    pinned_stores.push_back(store);
                  }
                  if (deferred_probe_rel == *target_rel &&
                      deferred_probe.has_value()) {
                    const slot_t tgt_proj_col =
                        keep_pk_col ? tgt.schema.ColumnCount() : *target_col;
                    deferred_probe->pk_lookups.push_back(
                        DeferredLSTablePkLookup{
                            .store = store,
                            .pk_col = *target_col,
                            .store_pk_col = *d_pk_col,
                            .payload_col = *d_payload_col,
                            .target_proj_col = tgt_proj_col,
                            .payload_type = p_type,
                            .payload_unsigned = p_unsigned,
                        });
                  } else if (tgt.HasColumnarOnly() && tgt.columnar != nullptr) {
                    // Columnar in-place/append 1:1 PK payload absorption
                    // (avoids Row materialization when target is ColumnarTable,
                    // e.g. lineitem <- orders in Q9, lineitem <- supplier in
                    // Q8).
                    auto mutable_tbl =
                        std::const_pointer_cast<ColumnarTable>(tgt.columnar);
                    auto& chunks = mutable_tbl->chunks;
                    const size_t nchunks = chunks.size();
                    const size_t nworkers = std::max<size_t>(
                        size_t{1},
                        std::min<size_t>(
                            {size_t{16},
                             QueryParallelWorkerLimit(
                                 std::thread::hardware_concurrency()),
                             nchunks}));
                    auto run_col_absorb = [&](size_t w) {
                      const auto nb_cnt =
                          static_cast<uint32_t>(blk_meta.size());
                      const AbsorbBlockMeta* const meta_ptr = blk_meta.data();
                      for (size_t ci = w; ci < nchunks; ci += nworkers) {
                        ColumnarChunk& ch = chunks[ci];
                        if (ch.rows == 0) {
                          continue;
                        }
                        const ColumnarColumn& key_col = ch.columns[*target_col];
                        const int64_t* const kcells = key_col.ints.data();
                        const bool k_has_null = key_col.has_null;
                        ColumnarColumn out_col;
                        out_col.type = p_type;
                        out_col.is_uniform = true;
                        out_col.is_unsigned = p_unsigned;
                        out_col.has_null = false;
                        out_col.encoding = ColumnarColumn::Encoding::kFlat;
                        if (p_type == ValueType::kInt64 ||
                            p_type == ValueType::kDate) {
                          out_col.ints.resize(ch.rows);
                        } else if (p_type == ValueType::kDouble) {
                          out_col.doubles.resize(ch.rows);
                        } else if (p_type == ValueType::kVarChar) {
                          out_col.strings.resize(ch.rows);
                        }
                        ChunkStringArena* out_arena =
                            (p_type == ValueType::kVarChar)
                                ? out_col.EnsureLocalArena()
                                : nullptr;
                        std::vector<uint8_t> matched_mask;
                        bool had_unmatched = false;
                        int64_t last_key = std::numeric_limits<int64_t>::min();
                        bool have_last = false;
                        bool last_matched = false;
                        bool last_is_null = false;
                        int64_t last_raw_bits = 0;
                        std::string_view last_sv;
                        uint32_t last_nb = 0;
                        uint32_t last_row_in_blk = 0;
                        auto mark_unmatched = [&](size_t r) {
                          if (!had_unmatched) {
                            matched_mask.assign(ch.rows, 1);
                            had_unmatched = true;
                          }
                          matched_mask[r] = 0;
                        };
                        auto mark_out_null = [&](size_t r) {
                          if (!out_col.has_null) {
                            out_col.has_null = true;
                            out_col.nulls.assign(ch.rows, 0);
                          }
                          out_col.nulls[r] = 1;
                        };
                        for (size_t r = 0; r < ch.rows; ++r) {
                          if ((k_has_null && key_col.nulls[r] != 0) ||
                              kcells[r] < min_k || kcells[r] > max_k)
                              [[unlikely]] {
                            mark_unmatched(r);
                            continue;
                          }
                          const int64_t k = kcells[r];
                          if (!have_last || k != last_key) {
                            bool same_blk_mono = false;
                            uint32_t nb = 0;
                            if (have_last && k >= meta_ptr[last_nb].min_k &&
                                k <= meta_ptr[last_nb].max_k) {
                              nb = last_nb;
                              same_blk_mono = (k > last_key);
                            } else {
                              const auto b_idx = static_cast<size_t>(
                                  (static_cast<__uint128_t>(
                                       static_cast<uint64_t>(k) -
                                       static_cast<uint64_t>(min_k))
                                   << kDirShift) /
                                  span);
                              nb = bucket_lo[b_idx];
                              while (nb + 1 < nb_cnt &&
                                     meta_ptr[nb].max_k < k) {
                                ++nb;
                              }
                            }
                            last_key = k;
                            have_last = true;
                            last_nb = nb;
                            const AbsorbBlockMeta& bm = meta_ptr[nb];
                            if (k < bm.min_k || k > bm.max_k) [[unlikely]] {
                              last_matched = false;
                            } else {
                              uint32_t row_in_blk = bm.row_count;
                              if (bm.dense_contiguous != 0) {
                                row_in_blk = static_cast<uint32_t>(
                                    static_cast<uint64_t>(k) -
                                    static_cast<uint64_t>(bm.min_k));
                              } else {
                                const int64_t* const bp = bm.pk_cells;
                                const int64_t* const ep = bp + bm.row_count;
                                const int64_t* sb = bp;
                                const int64_t* se = ep;
                                if (same_blk_mono &&
                                    last_row_in_blk < bm.row_count) {
                                  const uint32_t cur_r = last_row_in_blk;
                                  if (bp[cur_r] == k) {
                                    row_in_blk = cur_r;
                                    sb = ep;
                                  } else if (bp[cur_r] < k) {
                                    if (cur_r + 1 < bm.row_count &&
                                        bp[cur_r + 1] == k) {
                                      row_in_blk = cur_r + 1;
                                      sb = ep;
                                    } else {
                                      sb = bp + cur_r + 1;
                                      if (cur_r + 16 < bm.row_count &&
                                          bp[cur_r + 16] >= k) {
                                        se = bp + cur_r + 17;
                                      }
                                    }
                                  }
                                }
                                if (sb < se) {
                                  const int64_t* const fp =
                                      std::lower_bound(sb, se, k);
                                  last_row_in_blk =
                                      static_cast<uint32_t>(fp - bp);
                                  if (fp != se && *fp == k) [[likely]] {
                                    row_in_blk = last_row_in_blk;
                                  }
                                } else {
                                  last_row_in_blk = row_in_blk;
                                }
                              }
                              if (row_in_blk >= bm.row_count) [[unlikely]] {
                                last_matched = false;
                              } else {
                                last_matched = true;
                                last_is_null = (bm.has_null_payload != 0) &&
                                               bm.pay_blk->IsNull(row_in_blk);
                                if (!last_is_null) [[likely]] {
                                  if (p_type == ValueType::kVarChar) {
                                    last_sv = bm.pay_blk->VarcharAt(row_in_blk);
                                  } else {
                                    last_raw_bits = bm.pay_cells[row_in_blk];
                                  }
                                }
                              }
                            }
                          }
                          if (!last_matched) [[unlikely]] {
                            mark_unmatched(r);
                            continue;
                          }
                          if (last_is_null) [[unlikely]] {
                            mark_out_null(r);
                          } else if (p_type == ValueType::kInt64 ||
                                     p_type == ValueType::kDate) {
                            out_col.ints[r] = last_raw_bits;
                          } else if (p_type == ValueType::kDouble) {
                            out_col.doubles[r] =
                                std::bit_cast<double>(last_raw_bits);
                          } else if (p_type == ValueType::kVarChar) {
                            out_col.strings[r] = out_arena->Store(last_sv);
                          }
                        }
                        if (keep_pk_col) {
                          ch.columns.push_back(std::move(out_col));
                        } else {
                          ch.columns[*target_col] = std::move(out_col);
                        }
                        if (had_unmatched) {
                          size_t kept = 0;
                          for (size_t r = 0; r < ch.rows; ++r) {
                            kept += matched_mask[r];
                          }
                          for (ColumnarColumn& col : ch.columns) {
                            if (!col.is_uniform) {
                              size_t out_i = 0;
                              for (size_t r = 0; r < ch.rows; ++r) {
                                if (matched_mask[r] != 0) {
                                  if (out_i != r) {
                                    col.fallback[out_i] =
                                        std::move(col.fallback[r]);
                                  }
                                  ++out_i;
                                }
                              }
                              col.fallback.resize(out_i);
                            } else if (col.encoding ==
                                       ColumnarColumn::Encoding::kConstant) {
                              col.const_rows = kept;
                            } else {
                              if (col.has_null) {
                                size_t out_i = 0;
                                for (size_t r = 0; r < ch.rows; ++r) {
                                  if (matched_mask[r] != 0) {
                                    col.nulls[out_i++] = col.nulls[r];
                                  }
                                }
                                col.nulls.resize(out_i);
                              }
                              if (col.type == ValueType::kInt64 ||
                                  col.type == ValueType::kDate) {
                                size_t out_i = 0;
                                for (size_t r = 0; r < ch.rows; ++r) {
                                  if (matched_mask[r] != 0) {
                                    col.ints[out_i++] = col.ints[r];
                                  }
                                }
                                col.ints.resize(out_i);
                              } else if (col.type == ValueType::kDouble) {
                                size_t out_i = 0;
                                for (size_t r = 0; r < ch.rows; ++r) {
                                  if (matched_mask[r] != 0) {
                                    col.doubles[out_i++] = col.doubles[r];
                                  }
                                }
                                col.doubles.resize(out_i);
                              } else if (col.type == ValueType::kVarChar) {
                                size_t out_i = 0;
                                for (size_t r = 0; r < ch.rows; ++r) {
                                  if (matched_mask[r] != 0) {
                                    col.strings[out_i++] = col.strings[r];
                                  }
                                }
                                col.strings.resize(out_i);
                              }
                            }
                          }
                          ch.rows = kept;
                        }
                      }
                    };
                    if (nworkers <= 1) {
                      run_col_absorb(0);
                    } else {
                      std::vector<std::jthread> threads;
                      threads.reserve(nworkers - 1);
                      for (size_t w = 1; w < nworkers; ++w) {
                        threads.emplace_back([&, w] { run_col_absorb(w); });
                      }
                      run_col_absorb(0);
                    }
                    size_t new_total = 0;
                    for (const auto& ch : chunks) {
                      new_total += ch.rows;
                    }
                    mutable_tbl->total_rows = new_total;
                  } else {
                    const size_t nworkers = std::max<size_t>(
                        size_t{1},
                        std::min<size_t>(
                            {size_t{16},
                             QueryParallelWorkerLimit(
                                 std::thread::hardware_concurrency()),
                             (tgt.rows.size() / 16384) + 1}));
                    // Own small dimension strings once. Borrowed page views
                    // otherwise allocate a fresh payload every time a fact row
                    // is moved while later joins grow its Value vector.
                    std::shared_ptr<QueryMemoryCharge> payload_charge;
                    std::vector<std::vector<Value>> owned_payloads;
                    if (p_type == ValueType::kVarChar &&
                        tgt.rows.size() >= 1024) {
                      size_t payload_rows = 0;
                      size_t payload_bytes = 0;
                      for (const auto& block : blk_meta) {
                        payload_rows += block.row_count;
                        payload_bytes +=
                            (block.row_count * (sizeof(Value) + 64)) +
                            block.pay_blk->VarcharBytes().size();
                      }
                      if (payload_rows <= 65536 &&
                          payload_rows <= tgt.rows.size() / 4) {
                        payload_charge = TryReserveQueryMemory(payload_bytes);
                        if (payload_charge) {
                          owned_payloads.resize(blk_meta.size());
                          for (size_t b = 0; b < blk_meta.size(); ++b) {
                            const auto& block = blk_meta[b];
                            auto& values = owned_payloads[b];
                            values.resize(block.row_count);
                            for (uint32_t r = 0; r < block.row_count; ++r) {
                              if (!block.pay_blk->IsNull(r)) {
                                values[r].CopyFromStringView(
                                    block.pay_blk->VarcharAt(r));
                              }
                            }
                          }
                        }
                      }
                    }
                    const auto assign_text = [](Value& dst,
                                                std::string_view text,
                                                const Value* owned) {
                      if (owned != nullptr) {
                        dst = *owned;
                      } else {
                        dst.type = ValueType::kVarChar;
                        dst.SetCollation(0);
                        dst.owned_data.clear();
                        dst.value.varchar_value = text;
                      }
                    };
                    std::vector<uint8_t> worker_had_unmatched(nworkers, 0);
                    auto run_absorb_shard = [&](size_t w) {
                      const size_t r_begin = (tgt.rows.size() * w) / nworkers;
                      const size_t r_end =
                          (tgt.rows.size() * (w + 1)) / nworkers;
                      uint8_t had_unmatched = 0;
                      int64_t last_key = std::numeric_limits<int64_t>::min();
                      bool have_last = false;
                      bool last_matched = false;
                      bool last_is_null = false;
                      int64_t last_raw_bits = 0;
                      std::string_view last_sv;
                      const Value* last_owned = nullptr;
                      uint32_t last_nb = 0;
                      uint32_t last_row_in_blk = 0;
                      const AbsorbBlockMeta* const meta_ptr = blk_meta.data();
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
                            Value& dst = tgt.rows[ri].values_.emplace_back();
                            if (!last_is_null) [[likely]] {
                              dst.type = p_type;
                              dst.SetCollation(0);
                              if (p_type == ValueType::kVarChar) {
                                assign_text(dst, last_sv, last_owned);
                              } else if (p_type == ValueType::kDouble) {
                                dst.value.double_value =
                                    std::bit_cast<double>(last_raw_bits);
                              } else {
                                if (p_unsigned) {
                                  dst.SetUnsigned();
                                }
                                dst.value.int_value = last_raw_bits;
                              }
                            }
                          } else if (last_is_null) [[unlikely]] {
                            cell = Value();
                          } else if (p_type == ValueType::kVarChar) {
                            assign_text(cell, last_sv, last_owned);
                          } else if (p_type == ValueType::kDouble) {
                            cell.type = ValueType::kDouble;
                            cell.SetCollation(0);
                            cell.value.double_value =
                                std::bit_cast<double>(last_raw_bits);
                          } else {
                            cell.type = p_type;
                            cell.SetCollation(0);
                            if (p_unsigned) {
                              cell.SetUnsigned();
                            }
                            cell.value.int_value = last_raw_bits;
                          }
                          continue;
                        }
                        uint32_t nb = last_nb;
                        if (nb >= nb_count || k > meta_ptr[nb].max_k ||
                            k < meta_ptr[nb].min_k) {
                          if (nb + 1 < nb_count &&
                              k <= meta_ptr[nb + 1].max_k &&
                              k >= meta_ptr[nb + 1].min_k) {
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
                              while (nb < hi && meta_ptr[nb].max_k < k) {
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
                            have_last && nb == last_nb && k > last_key;
                        if (nb != last_nb) {
                          last_row_in_blk = 0;
                        }
                        last_nb = nb;
                        last_key = k;
                        have_last = true;
                        const AbsorbBlockMeta& bm = meta_ptr[nb];
                        if (k < bm.min_k || k > bm.max_k) {
                          last_matched = false;
                          cell.type = ValueType::kNull;
                          cell.SetCollation(1);
                          had_unmatched = 1;
                          continue;
                        }
                        uint32_t row_in_blk = bm.row_count;
                        if (bm.dense_contiguous != 0) {
                          row_in_blk = static_cast<uint32_t>(
                              static_cast<uint64_t>(k) -
                              static_cast<uint64_t>(bm.min_k));
                        } else {
                          const int64_t* const begin_ptr = bm.pk_cells;
                          const int64_t* const end_ptr =
                              begin_ptr + bm.row_count;
                          const int64_t* search_begin = begin_ptr;
                          const int64_t* search_end = end_ptr;
                          if (same_blk_monotonic &&
                              last_row_in_blk < bm.row_count) {
                            const uint32_t cur_r = last_row_in_blk;
                            if (begin_ptr[cur_r] == k) {
                              row_in_blk = cur_r;
                              search_begin = end_ptr;
                            } else if (begin_ptr[cur_r] < k) {
                              if (cur_r + 1 < bm.row_count &&
                                  begin_ptr[cur_r + 1] == k) {
                                row_in_blk = cur_r + 1;
                                search_begin = end_ptr;
                              } else {
                                search_begin = begin_ptr + cur_r + 1;
                                if (cur_r + 16 < bm.row_count &&
                                    begin_ptr[cur_r + 16] >= k) {
                                  search_end = begin_ptr + cur_r + 17;
                                } else if (cur_r + 64 < bm.row_count &&
                                           begin_ptr[cur_r + 64] >= k) {
                                  search_begin = begin_ptr + cur_r + 16;
                                  search_end = begin_ptr + cur_r + 65;
                                }
                              }
                            }
                          }
                          if (search_begin < search_end) {
                            const int64_t* const found_ptr =
                                std::lower_bound(search_begin, search_end, k);
                            last_row_in_blk =
                                static_cast<uint32_t>(found_ptr - begin_ptr);
                            if (found_ptr != search_end && *found_ptr == k)
                                [[likely]] {
                              row_in_blk = last_row_in_blk;
                            }
                          } else {
                            last_row_in_blk = row_in_blk;
                          }
                        }
                        if (row_in_blk >= bm.row_count) [[unlikely]] {
                          last_matched = false;
                          cell.type = ValueType::kNull;
                          cell.SetCollation(1);
                          had_unmatched = 1;
                          continue;
                        }
                        last_matched = true;
                        last_is_null = (bm.has_null_payload != 0) &&
                                       bm.pay_blk->IsNull(row_in_blk);
                        if (!last_is_null) [[likely]] {
                          if (p_type == ValueType::kVarChar) {
                            last_sv = bm.pay_blk->VarcharAt(row_in_blk);
                            last_owned = owned_payloads.empty()
                                             ? nullptr
                                             : &owned_payloads[nb][row_in_blk];
                          } else {
                            last_raw_bits = bm.pay_cells[row_in_blk];
                          }
                        }
                        if (keep_pk_col) {
                          Value& dst = tgt.rows[ri].values_.emplace_back();
                          if (!last_is_null) [[likely]] {
                            dst.type = p_type;
                            dst.SetCollation(0);
                            if (p_type == ValueType::kVarChar) {
                              assign_text(dst, last_sv, last_owned);
                            } else if (p_type == ValueType::kDouble) {
                              dst.value.double_value =
                                  std::bit_cast<double>(last_raw_bits);
                            } else {
                              if (p_unsigned) {
                                dst.SetUnsigned();
                              }
                              dst.value.int_value = last_raw_bits;
                            }
                          }
                        } else if (last_is_null) [[unlikely]] {
                          cell = Value();
                        } else if (p_type == ValueType::kVarChar) {
                          assign_text(cell, last_sv, last_owned);
                        } else if (p_type == ValueType::kDouble) {
                          cell.type = ValueType::kDouble;
                          cell.SetCollation(0);
                          cell.value.double_value =
                              std::bit_cast<double>(last_raw_bits);
                        } else {
                          cell.type = p_type;
                          cell.SetCollation(0);
                          if (p_unsigned) {
                            cell.SetUnsigned();
                          }
                          cell.value.int_value = last_raw_bits;
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
                  payload_absorbed_into[*target_rel] = 1;
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
    if (deferred_probe_rel.has_value()) {
      if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr) {
        std::cerr << "[deferred] interrupted by "
                  << statement.Sources()[idx].table << "\n";
      }
      RETURN_IF_FAIL(materialize_deferred());
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
          col_filters.push_back(SecondaryColFilter{.filter = cand,
                                                   .type = ptype,
                                                   .projected_col = pcol,
                                                   .driver_rows = drows,
                                                   .coverage = cov,
                                                   .driver_rel = drel,
                                                   .exact = is_exact});
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
                      stored->second.OwnedBy(statement);
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
      if (other_selective &&
          std::ranges::any_of(
              exact_filtered_by[other], [&](const ExactFilteredEdge& e) {
                return e.projected_col == *other_col && loaded[e.driver_rel] &&
                       selective[e.driver_rel] != 0U &&
                       relations[e.driver_rel].TotalRows() < driver_rows;
              })) {
        continue;
      }
      if (key_filter != nullptr && best_driver_selective &&
          best_driver_coverage <= 0.25 && local_predicates[other].empty() &&
          relations[other].schema.GetColumn(*other_col).GetConstraint().ctype !=
              Constraint::kPrimaryKey &&
          !std::ranges::any_of(exact_filtered_by[other],
                               [&](const ExactFilteredEdge& e) {
                                 return e.projected_col == *other_col;
                               }) &&
          target_stats.HasValue() &&
          *idx_col < target_stats.Value()->Columns()) {
        const size_t col_distinct =
            target_stats.Value()->Column(*idx_col).Distinct();
        if (col_distinct > 0 && driver_rows >= col_distinct * 2) {
          continue;
        }
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
                       stored->second.OwnedBy(statement);
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
      const bool use_df =
          key_column.has_value() &&
          *key_column < deferred_proj_to_inner_col[idx].size() &&
          key_filter != nullptr && context.execution_runtime() != nullptr;
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
        stored.column = deferred_proj_to_inner_col[idx][*key_column];
        stored.SetOwner(inner_stmt);
        stored.source_expr = nullptr;
        stored.exact_in = false;
      }
      StatusOr<Relation> loaded_rel =
          LoadSource(context, statement.Sources()[idx], outer, ctes);
      if (use_df && context.execution_runtime() != nullptr) {
        auto it =
            context.execution_runtime()->table_key_filters.find(inner_key);
        if (it != context.execution_runtime()->table_key_filters.end() &&
            it->second.OwnedBy(inner_stmt)) {
          context.execution_runtime()->table_key_filters.erase(it);
        }
      }
      RETURN_IF_FAIL(loaded_rel.GetStatus());
      relations[idx] = loaded_rel.MoveValue();
      RETURN_IF_FAIL(FilterRelation(context, &relations[idx],
                                    local_predicates[idx], outer, ctes));
      for (const Expression& ep : post_load_exists_predicates[idx]) {
        RETURN_IF_FAIL(
            FilterRelation(context, &relations[idx], {ep}, outer, ctes));
      }
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
      if (stored != filters.end() && stored->second.OwnedBy(statement) &&
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
                column >= statistics.Value()->Columns()) {
              return 1.0;
            }
            const auto& stats = statistics.Value()->Column(column);
            if (const auto* exact = filter.ExactKeys();
                exact &&
                !relations[idx].schema.GetColumn(column).IsUnsigned()) {
              double matches = 0;
              for (int64_t key : *exact) {
                if (filter.Reject(key)) {
                  continue;
                }
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
      stored.SetOwner(statement);
      if (key_filter != exact_in_filter) {
        stored.source_expr = nullptr;
        stored.exact_in = false;
      }
      binding.group = key_filter;
      binding.type = key_filter_type;
      binding.column = *key_column;
      if (direct_base_scan && best_driver_exact &&
          best_driver_rel < relations.size()) {
        exact_filtered_by[idx].push_back(ExactFilteredEdge{
            .projected_col = *key_column, .driver_rel = best_driver_rel});
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
                ExactFilteredEdge{.projected_col = sf.projected_col,
                                  .driver_rel = sf.driver_rel});
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
          stored->second.OwnedBy(statement) && !stored->second.empty()) {
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
    if (direct_base_scan && context.execution_runtime() != nullptr &&
        !context.execution_runtime()->scoped_extra_table_rfs.empty()) {
      const std::string& tbl_name = statement.Sources()[idx].table;
      const auto& proj = projections[idx];
      for (const auto& erf :
           context.execution_runtime()->scoped_extra_table_rfs) {
        if (erf.table_name != tbl_name || erf.group == nullptr) {
          continue;
        }
        const auto orig_col = LocalColumnOffset(
            relations[idx].schema, ColumnName("", erf.column_name));
        if (!orig_col.has_value()) {
          continue;
        }
        const Column& col_def =
            relations[idx].schema.GetColumn(static_cast<size_t>(*orig_col));
        if (col_def.IsUnsigned() || (col_def.Type() != ValueType::kInt64 &&
                                     col_def.Type() != ValueType::kDate)) {
          continue;
        }
        slot_t target_col = *orig_col;
        if (!proj.empty()) {
          const auto pit = std::ranges::find(proj, *orig_col);
          if (pit == proj.end()) {
            continue;
          }
          target_col = static_cast<slot_t>(std::distance(proj.begin(), pit));
        }
        ScanRuntimeFilter injected{
            .group = erf.group,
            .column = target_col,
            .type = col_def.Type(),
        };
        if (!binding.enabled()) {
          binding = std::move(injected);
        } else if (binding.extra.size() < 7) {
          binding.extra.push_back(std::move(injected));
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
    const ScanTupleFilter* tuple_filter =
        InputTupleFilter(statement, fusion, idx);
    if (tuple_filter == nullptr && !deferred_probe_rel.has_value() &&
        post_load_exists_predicates[idx].empty() &&
        (is_large_fact(idx) || load_preds_ptr->empty()) && fusion != nullptr &&
        fusion->eligible && *where_fully_applied && plain_inner_sources &&
        !selects_star && join_residual.empty() && direct_base_scan &&
        !projections[idx].empty() && projections[idx].size() <= 16) {
      bool any_other_loaded = false;
      for (size_t r = 0; r < relations.size(); ++r) {
        if (r != idx &&
            (loaded[r] || (!base_sources[r] && !deferred_derived[r])) &&
            absorbed[r] == 0U && !relations[r].HasSpill() &&
            relations[r].TotalRows() > 0) {
          any_other_loaded = true;
          break;
        }
      }
      bool rem_all_2col_leaf = any_other_loaded;
      for (size_t rem_pos = position + 1;
           rem_all_2col_leaf && rem_pos < load_order.size(); ++rem_pos) {
        const size_t u = load_order[rem_pos];
        if (!base_sources[u] && !deferred_derived[u]) {
          bool u_has_predicate = false;
          for (const PredicateInfo& p : predicates) {
            if (p.resolved && p.sources.contains(u)) {
              u_has_predicate = true;
              break;
            }
          }
          // Under HAVING, only skip already-materialized unjoined singleton
          // derived tables (e.g. scalar HAVING thresholds); deferring past a
          // joined derived table would push all fact probes through
          // deferred_left_probe and disable compact_counts HAVING prefiltering.
          const auto is_simple_count_having_gt = [&](const Expression& expr) {
            if (!expr || expr->Type() != TypeTag::kBinaryExp) {
              return false;
            }
            const auto& bin = expr->AsBinaryExpression();
            if (bin.Op() != BinaryOperation::kGreaterThan || !bin.Left() ||
                !bin.Right() || bin.Left()->Type() != TypeTag::kAggregateExp ||
                bin.Right()->Type() != TypeTag::kConstantValue) {
              return false;
            }
            const auto& agg = bin.Left()->AsAggregateExpression();
            const Value& v = bin.Right()->AsConstantValue().GetValue();
            return agg.GetType() == AggregationType::kCount &&
                   v.type == ValueType::kInt64 && !v.IsUnsigned() &&
                   v.value.int_value >= 1 && !fusion->aggregates.empty() &&
                   fusion->count_star.size() == fusion->aggregates.size() &&
                   std::ranges::all_of(fusion->count_star,
                                       [](bool star) { return star; }) &&
                   std::ranges::find(fusion->aggregates, &agg) !=
                       fusion->aggregates.end();
          };
          if (!fusion->having || is_simple_count_having_gt(fusion->having) ||
              (!u_has_predicate && !relations[u].HasSpill() &&
               relations[u].TotalRows() <= 1)) {
            continue;
          }
        }
        if (!base_sources[u] || deferred_derived[u] ||
            statement.Sources()[u].sample.has_value() ||
            !local_predicates[u].empty() ||
            !post_load_exists_predicates[u].empty() ||
            projections[u].size() != 2) {
          rem_all_2col_leaf = false;
          break;
        }
        bool u_connected_to_idx = false;
        for (const PredicateInfo& p : predicates) {
          if (!p.resolved || !p.sources.contains(u)) {
            continue;
          }
          if (p.contains_query || p.sources.size() != 2 ||
              !p.sources.contains(idx) ||
              !IsColumnEqualityPredicate(p.expression) ||
              p.expression->AsBinaryExpression().Op() !=
                  BinaryOperation::kEquals) {
            rem_all_2col_leaf = false;
            break;
          }
          u_connected_to_idx = true;
        }
        if (!u_connected_to_idx) {
          rem_all_2col_leaf = false;
          break;
        }
      }
      bool cols_all_scalar = rem_all_2col_leaf;
      std::vector<Column> proj_cols;
      if (cols_all_scalar) {
        proj_cols.reserve(projections[idx].size());
        for (const slot_t col_slot : projections[idx]) {
          if (col_slot >= relations[idx].schema.ColumnCount()) {
            cols_all_scalar = false;
            break;
          }
          const Column& col = relations[idx].schema.GetColumn(col_slot);
          if (col.IsUnsigned() || (col.Type() != ValueType::kInt64 &&
                                   col.Type() != ValueType::kDate &&
                                   col.Type() != ValueType::kDouble)) {
            cols_all_scalar = false;
            break;
          }
          proj_cols.push_back(col);
        }
      }
      if (cols_all_scalar) {
        StatusOr<std::shared_ptr<Table>> b_table =
            context.GetTable(statement.Sources()[idx].table);
        if (b_table.HasValue()) {
          bool allow_block_lazy = false;
          if (const auto tlcs = b_table.Value()->GetBackingTlcsStore();
              tlcs != nullptr && tlcs->TotalRows() >= 65536 &&
              tlcs->HasColumnSynopses()) {
            allow_block_lazy = true;
          }
          std::shared_ptr<const LSTableStore> store =
              GetOrBuildLSTableStore(context.txn_, *b_table.Value(), true,
                                     &projections[idx], allow_block_lazy);
          bool store_valid =
              store != nullptr && store->total_rows >= 32768 &&
              store->layout_generation == b_table.Value()->LayoutGeneration() &&
              store->first_pid == b_table.Value()->FirstPageId();
          if (store_valid) {
            if (store->uncommitted_owner_txn != 0) {
              store_valid = (store->uncommitted_owner_txn == context.txn_.ID());
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
          if (store_valid) {
            Schema proj_schema(relations[idx].schema.Name(),
                               std::move(proj_cols));
            std::vector<IntegerPeekCompare> peeks;
            std::vector<NullPeekCompare> null_peeks;
            bool preds_ok = load_preds_ptr->empty();
            if (!preds_ok &&
                std::ranges::none_of(*load_preds_ptr, [](const Expression& e) {
                  return ContainsQuery(e);
                })) {
              const CompiledScanFilter sf =
                  CompileScanFilter(*load_preds_ptr, proj_schema);
              if (!sf.compile_decided_reject && sf.prefix_in.empty() &&
                  sf.prebuilt_membership.empty() &&
                  sf.prebuilt_scalar_compare.empty() &&
                  sf.disjunctive_branches.empty()) {
                peeks = BuildIntegerPeeks(sf, &projections[idx],
                                          b_table.Value()->GetSchema());
                null_peeks = BuildNullPeeks(sf, proj_schema, &projections[idx],
                                            b_table.Value()->GetSchema());
                preds_ok = peeks.size() == sf.simple.size() &&
                           null_peeks.size() == sf.residual.size();
              }
            }
            if (preds_ok) {
              relations[idx].schema = std::move(proj_schema);
              pinned_stores.push_back(store);
              deferred_probe = DeferredLSTableProbe{
                  .store = store,
                  .projection = projections[idx],
                  .runtime_filter = binding,
                  .local_preds = *load_preds_ptr,
                  .local_preds_covered_by_peeks = true,
                  .integer_peeks = std::move(peeks),
                  .null_peeks = std::move(null_peeks),
                  .pk_lookups = {},
              };
              deferred_probe_rel = idx;
              loaded[idx] = true;
              continue;
            }
          }
        }
      }
    }
    // Fused grouping consumes columnar join inputs even when its final output
    // is row-oriented. Keep base scans in that representation as well.
    const bool fusion_columnar = fusion != nullptr && fusion->eligible &&
                                 plain_inner_sources && !selects_star &&
                                 join_residual.empty();
    const bool allow_col = (columnar_output || fusion_columnar) &&
                           !has_nested_join && !has_ordered_join &&
                           tuple_filter == nullptr;
    bool allow_star_fact_col = false;
    if (!allow_col && columnar_output && relations.size() >= 3 &&
        relations.size() <= 9 && !has_nested_join && !has_ordered_join &&
        fusion == nullptr && tuple_filter == nullptr && plain_inner_sources &&
        !selects_star && base_sources[idx] && !deferred_derived[idx] &&
        !ReusesBaseRelation(context, statement.Sources()[idx])) {
      if (auto tbl = context.GetTable(statement.Sources()[idx].table);
          tbl.HasValue()) {
        if (tbl.Value()->BackingColumnarRowCount() >= 262144) {
          allow_star_fact_col = true;
        }
      }
    }
    const bool allow_pushed = allow_col && outer == nullptr &&
                              *where_fully_applied && join_residual.empty() &&
                              pushed_projection != nullptr &&
                              pushed_projection_applied != nullptr;
    bool call_pushed_applied = false;
    ASSIGN_OR_RETURN(
        Relation, loaded_rel,
        (LoadSource(context, statement.Sources()[idx], outer, ctes,
                    &projections[idx], load_preds_ptr, &binding, tuple_filter,
                    allow_col || allow_star_fact_col,
                    allow_pushed ? pushed_projection : nullptr,
                    allow_pushed ? &call_pushed_applied : nullptr)));
    relations[idx] = std::move(loaded_rel);
    loaded[idx] = true;
    // The ordered outer/USING path returned above. A newly filtered empty
    // input therefore annihilates this inner join, just like the preloaded
    // derived-input check. Keep the original schema even after absorption.
    RETURN_IF_FAIL(relations[idx].GetStatus());
    if (!post_load_exists_predicates[idx].empty() &&
        relations[idx].TotalRows() > 0) {
      // Lean proof: lean/TinylambOpt/ResearchProofs/IndividualQueryGaps.lean
      //   (MultiChannelExistsBitsetPushdown_sound,
      //    MultiChannelAntiExistsBitsetPushdown_sound)
      // Evaluate plain single-EXISTS conjuncts before disjunctive OR-EXISTS
      // trees so the reduced outer-key domain from `EXISTS(S)` automatically
      // seeds the semi-filter for `EXISTS(W) OR EXISTS(C)`.
      std::vector<Expression> ordered_exists = post_load_exists_predicates[idx];
      std::ranges::stable_sort(
          ordered_exists, [](const Expression& a, const Expression& b) {
            const bool a_simple = a && a->Type() == TypeTag::kQueryExp;
            const bool b_simple = b && b->Type() == TypeTag::kQueryExp;
            return a_simple && !b_simple;
          });
      for (const Expression& ep : ordered_exists) {
        RETURN_IF_FAIL(
            FilterRelation(context, &relations[idx], {ep}, outer, ctes));
        if (relations[idx].TotalRows() == 0) {
          break;
        }
      }
      RETURN_IF_FAIL(relations[idx].GetStatus());
    }
    if (relations[idx].TotalRows() == 0) {
      Relation empty(context.execution_runtime());
      empty.schema = std::move(empty_input_schema);
      *where_fully_applied = true;
      if (pushed_projection_applied != nullptr) {
        *pushed_projection_applied = false;
      }
      if (auto* runtime = context.execution_runtime()) {
        runtime->empty_build_short_circuit = true;
        runtime->planning_ms += ElapsedMs(planning_begin);
      }
      return empty;
    }
    pushed_proj_applied_local = call_pushed_applied;
    if (direct_base_scan && load_preds_ptr->empty() &&
        post_load_exists_predicates[idx].empty() && binding.group != nullptr &&
        binding.extra.empty() &&
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
    propagate_small_domain(idx);
  }

  // Exact PK Semi-Join Absorption: when a loaded LSTable base table only
  // projects its single Primary Key column (not referenced in SELECT,
  // GROUP BY, HAVING, ORDER BY, DistinctOn, or non-equality predicates) and
  // its join neighbour's scan already enforced an exact RuntimeFilterGroup
  // built from that PK column, the semi-join has multiplicity 1 for every
  // surviving neighbour row and 0 otherwise, so the PK table's join step can
  // be absorbed without materializing an intermediate relation (TPC-H Q3, Q8,
  // Q9, Q11).
  const auto parallel_clear_rows = [](std::vector<Row>& rows) {
    const size_t n = rows.size();
    if (n < 32768) {
      std::vector<Row>().swap(rows);
      return;
    }
    const size_t nw = std::max<size_t>(
        size_t{1}, std::min<size_t>({size_t{16},
                                     QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
                                     n / 16384}));
    if (nw > 1) {
      auto clear_chunk = [&](size_t w) {
        const size_t lo = (n * w) / nw;
        const size_t hi = (n * (w + 1)) / nw;
        for (size_t i = lo; i < hi; ++i) {
          std::vector<Value>().swap(rows[i].values_);
        }
      };
      // Join the workers before destroying the callable they reference.
      std::vector<std::jthread> threads;
      threads.reserve(nw - 1);
      for (size_t w = 1; w < nw; ++w) {
        threads.emplace_back([&, w] { clear_chunk(w); });
      }
      clear_chunk(0);
    }
    std::vector<Row>().swap(rows);
  };
  const bool keep_build_for_fused = deferred_probe_rel && fusion != nullptr &&
                                    !fusion->group_keys.empty() &&
                                    !fusion->aggregates.empty();
  const size_t effective_min_remaining =
      keep_build_for_fused ? std::max<size_t>(min_remaining_relations, 2)
                           : min_remaining_relations;
  bool deferred_multi_payload_pending =
      !pk_absorption_disabled && deferred_probe_rel && deferred_probe &&
      *where_fully_applied && plain_inner_sources && !selects_star &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_DEFERRED_MULTI_PAYLOAD") ==
          nullptr;
  if (deferred_multi_payload_pending) {
    goto run_zero_payload_first;
  }
deferred_multi_payload_phase:
  // A deferred fact can borrow several columns from a unique dimension.
  // Append payloads instead of overwriting the foreign key: other joins and
  // runtime filters may still need its original domain.
  if (deferred_multi_payload_pending) {
    deferred_multi_payload_pending = false;
    const size_t fact = *deferred_probe_rel;
    for (size_t d = 0; d < relations.size(); ++d) {
      if (std::ranges::count(absorbed, uint8_t{0}) <= 2) {
        break;  // Keep one build relation for the fused grouped probe.
      }
      if (d == fact || absorbed[d] != 0 || !base_sources[d] ||
          deferred_derived[d] || !loaded[d] || payload_absorbed_into[d] != 0 ||
          relations[d].HasSpill() || relations[d].TotalRows() == 0 ||
          projections[d].size() != relations[d].schema.ColumnCount()) {
        continue;
      }
      std::optional<EqualityKey> edge;
      bool safe = true;
      for (const auto& predicate : predicates) {
        if (!predicate.resolved || !predicate.sources.contains(d)) {
          continue;
        }
        if (predicate.sources.size() == 1) {
          safe = safe && locally_evaluable(predicate);
          continue;
        }
        if (predicate.contains_query || predicate.sources.size() != 2 ||
            !predicate.sources.contains(fact) ||
            !IsColumnEqualityPredicate(predicate.expression) ||
            predicate.expression->AsBinaryExpression().Op() !=
                BinaryOperation::kEquals) {
          safe = false;
          break;
        }
        const auto keys =
            EqualityKeys(relations[fact].schema, relations[d].schema,
                         {predicate.expression});
        if (keys.size() != 1 || keys[0].null_safe || keys[0].left_offset != 0 ||
            keys[0].right_offset != 0 ||
            (edge &&
             (edge->left != keys[0].left || edge->right != keys[0].right))) {
          safe = false;
          break;
        }
        edge = keys[0];
      }
      if (!safe || !edge || edge->left >= deferred_probe->projection.size() ||
          std::ranges::any_of(deferred_probe->pk_lookups,
                              [&](const auto& lookup) {
                                return lookup.target_proj_col == edge->left;
                              }) ||
          !SameSingleIntegerJoinKey(
              relations[fact].schema, {static_cast<slot_t>(edge->left)},
              relations[d].schema, {static_cast<slot_t>(edge->right)})) {
        continue;
      }
      std::vector<slot_t> payloads;
      payloads.reserve(outer_referenced.size());
      for (const auto& name : outer_referenced) {
        if (const auto column = LocalColumnOffset(relations[d].schema, name);
            column && std::ranges::find(payloads, *column) == payloads.end()) {
          payloads.push_back(static_cast<slot_t>(*column));
        }
      }
      std::ranges::sort(payloads);
      const bool single_scalar_pk_lookup =
          payloads.size() == 1 &&
          payloads[0] != static_cast<slot_t>(edge->right) && [&] {
            const Column& source = relations[d].schema.GetColumn(payloads[0]);
            return !source.IsUnsigned() &&
                   (source.Type() == ValueType::kInt64 ||
                    source.Type() == ValueType::kDate ||
                    source.Type() == ValueType::kDouble);
          }();
      if ((payloads.size() < 2 && !single_scalar_pk_lookup) ||
          payloads.size() + deferred_probe->pk_lookups.size() > 4 ||
          deferred_probe->runtime_filter.extra.size() >= 4 ||
          relations[fact].schema.ColumnCount() + payloads.size() >
              std::numeric_limits<slot_t>::max()) {
        continue;
      }
      // Moving a nearly unique dimension tuple into the probe prevents the
      // fused operator from factoring it into a build-side dictionary. Keep
      // those dimensions as joins; append only sufficiently repeated tuples,
      // except for a single fixed-width scalar column which avoids
      // materializing the fact probe in multi-way joins (e.g. TPC-DS Q23).
      // Sampling chooses a plan, never removes a row or an equality check.
      const size_t d_rows = relations[d].TotalRows();
      const size_t samples = std::min<size_t>(1024, d_rows);
      GroupKeyMap sampled_payloads;
      if (relations[d].HasColumnarOnly()) {
        // NOLINTNEXTLINE(bugprone-inc-dec-in-conditions) deliberate
        const auto& chunks = relations[d].columnar->chunks;
        size_t chunk_idx = 0;
        size_t chunk_base = 0;
        for (size_t sample = 0; sample < samples; ++sample) {
          const size_t target_row = (sample * d_rows) / samples;
          while (chunk_idx < chunks.size() &&
                 chunk_base + chunks[chunk_idx].rows <= target_row) {
            chunk_base += chunks[chunk_idx].rows;
            ++chunk_idx;
          }
          const size_t local_row = target_row - chunk_base;
          std::vector<Value> values;
          values.reserve(payloads.size());
          Value cell;
          for (const slot_t column : payloads) {
            chunks[chunk_idx].columns[column].WriteValueInto(local_row, &cell);
            values.push_back(CanonicalDistinctValue(cell));
          }
          sampled_payloads.emplace(Row(std::move(values)), 0);
        }
      } else {
        for (size_t sample = 0; sample < samples; ++sample) {
          const auto& row = relations[d].rows[(sample * d_rows) / samples];
          std::vector<Value> values;
          values.reserve(payloads.size());
          for (const slot_t column : payloads) {
            values.push_back(CanonicalDistinctValue(row[column]));
          }
          sampled_payloads.emplace(Row(std::move(values)), 0);
        }
      }
      if (!single_scalar_pk_lookup && samples > 64 &&
          sampled_payloads.size() > samples / 8) {
        continue;
      }
      auto table = context.GetTable(statement.Sources()[d].table);
      if (!table.HasValue()) {
        continue;
      }
      if (edge->right >= projections[d].size() ||
          std::ranges::any_of(payloads, [&](slot_t p) {
            return static_cast<size_t>(p) >= projections[d].size();
          })) {
        continue;
      }
      const auto pk = projections[d][edge->right];
      std::vector<slot_t> pk_proj;
      pk_proj.reserve(payloads.size() + 1);
      pk_proj.push_back(pk);
      for (slot_t payload : payloads) {
        if (const slot_t pslot = projections[d][payload]; pslot != pk) {
          pk_proj.push_back(pslot);
        }
      }
      auto store =
          GetOrBuildLSTableStore(context.txn_, *table.Value(), true, &pk_proj);
      bool valid =
          store && store->total_rows > 0 &&
          store->layout_generation == table.Value()->LayoutGeneration() &&
          store->first_pid == table.Value()->FirstPageId() &&
          store->HasColumn(pk) && pk < store->column_is_monotonic.size() &&
          store->column_is_monotonic[pk] != 0 &&
          store->column_is_unique_nonnull[pk] != 0 &&
          store->column_has_null[pk] == 0 &&
          std::ranges::all_of(
              payloads,
              [&](slot_t p) { return store->HasColumn(projections[d][p]); });
      if (valid) {
        if (store->uncommitted_owner_txn != 0) {
          valid = store->uncommitted_owner_txn == context.txn_.ID();
        } else if (!context.txn_.IsReadOnly() &&
                   !context.txn_.WriteSetEmpty()) {
          valid = false;
        } else if (context.txn_.HasAnyPageVersionChains()) {
          valid = std::ranges::none_of(store->page_ids, [&](page_id_t pid) {
            return context.txn_.HasPageVersionChains(pid);
          });
        }
      }
      // `store` can be null (failed txn, in-place rewrites, a fresh table with
      // generation 0, surviving version chains, ...); `valid` already treats
      // that as a rejection, so short-circuit here instead of dereferencing it.
      const bool payload_mismatch =
          store != nullptr && std::ranges::any_of(payloads, [&](slot_t column) {
            const auto& source = relations[d].schema.GetColumn(column);
            if (projections[d][column] >= store->column_min_int.size() ||
                relations[fact].schema.Offset(source.Name()) >= 0 ||
                (source.Type() != ValueType::kInt64 &&
                 source.Type() != ValueType::kDate &&
                 source.Type() != ValueType::kDouble &&
                 source.Type() != ValueType::kVarChar)) {
              return true;
            }
            const auto value_mismatch = [&](const Value& value) {
              return !value.IsNull() &&
                     (value.type != source.Type() ||
                      value.IsUnsigned() != source.IsUnsigned() ||
                      (value.type == ValueType::kVarChar &&
                       value.Collation() != 0));
            };
            if (relations[d].HasColumnarOnly()) {
              for (const ColumnarChunk& chunk : relations[d].columnar->chunks) {
                if (chunk.rows == 0 || column >= chunk.columns.size()) {
                  continue;
                }
                const ColumnarColumn& c = chunk.columns[column];
                if (c.is_uniform && c.type == source.Type() &&
                    c.is_unsigned == source.IsUnsigned() &&
                    (c.type != ValueType::kVarChar || !c.is_interval)) {
                  continue;
                }
                Value value;
                for (size_t r = 0; r < chunk.rows;
                     ++r) {  // NOLINT(bugprone-inc-dec-in-conditions)
                  c.WriteValueInto(r, &value);
                  if (value_mismatch(value)) {
                    return true;
                  }
                }
              }
              return false;
            }
            return std::ranges::any_of(relations[d].rows, [&](const Row& row) {
              return value_mismatch(row[column]);
            });
          });
      if (!valid || payload_mismatch) {
        continue;
      }
      std::vector<int64_t> keys;
      append_rel_nonnull_ints(relations[d], static_cast<slot_t>(edge->right),
                              &keys);
      auto exact = RuntimeFilterBuilder::FromKeys(keys, 100000);
      if (!exact ||
          (!exact->has_dense_bits() && exact->ExactKeys() == nullptr)) {
        continue;
      }
      std::vector<Column> columns;
      columns.reserve(relations[fact].schema.ColumnCount() + payloads.size());
      for (size_t c = 0; c < relations[fact].schema.ColumnCount(); ++c) {
        columns.push_back(relations[fact].schema.GetColumn(c));
      }
      for (slot_t payload : payloads) {
        const auto& source = relations[d].schema.GetColumn(payload);
        deferred_probe->pk_lookups.push_back(DeferredLSTablePkLookup{
            .store = store,
            .pk_col = static_cast<slot_t>(edge->left),
            .store_pk_col = pk,
            .payload_col = projections[d][payload],
            .target_proj_col = static_cast<slot_t>(columns.size()),
            .payload_type = source.Type(),
            .payload_unsigned = source.IsUnsigned(),
        });
        Column appended(source.Name(), source.Type(), Constraint(),
                        source.Scale());
        appended.SetUnsigned(source.IsUnsigned());
        appended.SetInterval(source.IsInterval());
        appended.SetIntegerBits(source.IntegerBits());
        appended.SetHint(source.Hint());
        columns.push_back(std::move(appended));
      }
      deferred_probe->runtime_filter.extra.push_back(ScanRuntimeFilter{
          .group = std::move(exact),
          .column = static_cast<slot_t>(edge->left),
          .type = relations[fact].schema.GetColumn(edge->left).Type()});
      relations[fact].schema = Schema("", std::move(columns));
      pinned_stores.push_back(std::move(store));
      absorbed[d] = 1;
      for (auto& predicate : predicates) {
        if (predicate.resolved && predicate.sources.contains(d)) {
          mark_applied(predicate.expression);
          predicate.resolved = false;
        }
      }
      parallel_clear_rows(relations[d].rows);
      relations[d].columnar.reset();
    }
  }
run_zero_payload_first:
  if (!pk_absorption_disabled && *where_fully_applied && plain_inner_sources &&
      !selects_star && relations.size() >= 2) {
    bool absorbed_any = true;
    while (absorbed_any) {
      absorbed_any = false;
      size_t active_relations = 0;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (absorbed[i] == 0U) {
          ++active_relations;
        }
      }
      if (active_relations <= effective_min_remaining) {
        break;
      }
      for (size_t d = 0; d < relations.size(); ++d) {
        if ((absorbed[d] != 0U) || !loaded[d] || deferred_probe_rel == d ||
            relations[d].TotalRows() == 0 ||
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
          if ((absorbed[other] != 0U) || rel_row_count(other) == 0) {
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
          d_pk_col = d_col;
          const bool has_exact = std::ranges::any_of(
              exact_filtered_by[other], [&](const ExactFilteredEdge& e) {
                return e.projected_col == *o_col && e.driver_rel == d;
              });
          neighbours.push_back(NeighbourCol{
              .rel = other, .col = *o_col, .exact_filtered = has_exact});
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
            deferred_probe_rel != neighbours[0].rel &&
            !relations[neighbours[0].rel].HasSpill() &&
            !relations[neighbours[0].rel].HasColumnarOnly()) {
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
          if (nc.rel == anchor.rel && nc.col == anchor.col) {
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
      if (!absorbed_any && deferred_multi_payload_pending) {
        goto deferred_multi_payload_phase;
      }
      if (!absorbed_any && active_relations > effective_min_remaining) {
        // 2-Column 1:1 PK Payload Column Absorption: when an LSTable base table
        // `d` has two active columns `(pk_col, payload_col)`, either replace
        // `other.fk_col` in-place with `d.payload_col` (when `other.fk_col` is
        // not needed elsewhere) or append `d.payload_col` to a small neighbour
        // relation in the same key equivalence class (TPC-H Q5, Q7, Q8, Q9,
        // Q10).
        for (size_t d = 0; d < relations.size(); ++d) {
          if ((absorbed[d] != 0U) || !base_sources[d] || deferred_derived[d] ||
              !loaded[d] || deferred_probe_rel == d ||
              relations[d].HasSpill() || relations[d].TotalRows() == 0 ||
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
            if ((absorbed[other] != 0U) || relations[other].HasSpill() ||
                rel_row_count(other) == 0) {
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
            edges.push_back(
                JoinEdge{.d_col = *d_col, .other = other, .o_col = *o_col});
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
                    rel_row_count(e.other) < rel_row_count(*cand_t_rel)) {
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
            if (!t_col_referenced &&
                (deferred_probe_rel != *cand_t_rel ||
                 (!has_payload_edges && payload_absorbed_into[d] == 0))) {
              d_pk_col = cand_pk;
              d_payload_col = cand_payload;
              target_rel = cand_t_rel;
              target_col = cand_t_col;
              append_payload = false;
              break;
            }
            if (!has_payload_edges && relations[d].TotalRows() <= 65536) {
              std::optional<size_t> best_append_rel;
              std::optional<slot_t> best_append_col;
              if (deferred_probe_rel != *cand_t_rel &&
                  !relations[*cand_t_rel].HasSpill() &&
                  !relations[*cand_t_rel].HasColumnarOnly() &&
                  !relations[*cand_t_rel].rows.empty() &&
                  relations[*cand_t_rel].rows.size() <= 131072) {
                best_append_rel = cand_t_rel;
                best_append_col = cand_t_col;
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
                if (alt == d || (absorbed[alt] != 0U) ||
                    relations[alt].HasSpill() ||
                    relations[alt].HasColumnarOnly() ||
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
                    rel_row_count(alt) < rel_row_count(*best_append_rel)) {
                  best_append_rel = alt;
                  best_append_col = a_col;
                }
              }
              if (best_append_rel.has_value() && best_append_col.has_value()) {
                d_pk_col = cand_pk;
                d_payload_col = cand_payload;
                target_rel = best_append_rel;
                target_col = best_append_col;
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
          if (deferred_probe_rel != *target_rel &&
              (tgt.HasSpill() || tgt.HasColumnarOnly() || tgt.rows.empty())) {
            continue;
          }
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
          if (!rel_int_min_max(relations[d], *d_pk_col, &min_k, &max_k)) {
            continue;
          }
          const uint64_t span =
              static_cast<uint64_t>(max_k) - static_cast<uint64_t>(min_k) + 1;
          if (span == 0) {
            continue;  // the full INT64 domain cannot be dense
          }
          const ValueType p_type = payload_col.Type();
          const bool can_small_ptr_path =
              relations[d].TotalRows() <= 65536 && span <= 262144ULL;
          if (deferred_probe_rel == *target_rel) {
            // Keep a filtered PK dimension as an exact key domain plus a
            // snapshot-valid PAX lookup. Decoding the fact into Rows just to
            // replace its foreign key would defeat deferred scan fusion.
            if (!deferred_probe || append_payload ||
                payload_absorbed_into[d] != 0 ||
                deferred_probe->pk_lookups.size() >= 4 ||
                deferred_probe->runtime_filter.extra.size() >= 4 ||
                *target_col >= deferred_probe->projection.size() ||
                std::ranges::count(absorbed, uint8_t{0}) < 3 ||
                *d_pk_col >= projections[d].size() ||
                *d_payload_col >= projections[d].size() ||
                std::ranges::any_of(
                    deferred_probe->pk_lookups, [&](const auto& lookup) {
                      return lookup.target_proj_col == *target_col ||
                             lookup.pk_col == *target_col;
                    })) {
              continue;
            }
            const auto pk_slot = projections[d][*d_pk_col];
            const auto payload_slot = projections[d][*d_payload_col];
            const std::vector<slot_t> pk_proj =
                (pk_slot == payload_slot)
                    ? std::vector<slot_t>{pk_slot}
                    : std::vector<slot_t>{pk_slot, payload_slot};
            auto store = GetOrBuildLSTableStore(context.txn_, *d_table.Value(),
                                                true, &pk_proj);
            bool valid = store && store->total_rows > 0 &&
                         store->layout_generation ==
                             d_table.Value()->LayoutGeneration() &&
                         store->first_pid == d_table.Value()->FirstPageId() &&
                         store->HasColumn(pk_slot) &&
                         store->HasColumn(payload_slot) &&
                         pk_slot < store->column_is_monotonic.size() &&
                         store->column_is_monotonic[pk_slot] != 0 &&
                         store->column_is_unique_nonnull[pk_slot] != 0 &&
                         store->column_has_null[pk_slot] == 0 &&
                         payload_slot < store->column_min_int.size();
            if (valid) {
              if (store->uncommitted_owner_txn != 0) {
                valid = store->uncommitted_owner_txn == context.txn_.ID();
              } else if (!context.txn_.IsReadOnly() &&
                         !context.txn_.WriteSetEmpty()) {
                valid = false;
              } else if (context.txn_.HasAnyPageVersionChains()) {
                valid =
                    std::ranges::none_of(store->page_ids, [&](page_id_t pid) {
                      return context.txn_.HasPageVersionChains(pid);
                    });
              }
            }
            if (!valid ||
                (p_type != ValueType::kInt64 && p_type != ValueType::kDate &&
                 p_type != ValueType::kDouble &&
                 p_type != ValueType::kVarChar)) {
              continue;
            }
            std::vector<int64_t> keys;
            append_rel_nonnull_ints(relations[d], *d_pk_col, &keys);
            auto exact = RuntimeFilterBuilder::FromKeys(keys, 100000);
            if (!exact ||
                (!exact->has_dense_bits() && exact->ExactKeys() == nullptr)) {
              continue;
            }
            ScanRuntimeFilter filter;
            filter.group = std::move(exact);
            filter.type = relations[d].schema.GetColumn(*d_pk_col).Type();
            filter.column = *target_col;
            deferred_probe->runtime_filter.extra.push_back(std::move(filter));
            deferred_probe->pk_lookups.push_back(DeferredLSTablePkLookup{
                .store = store,
                .pk_col = *target_col,
                .store_pk_col = pk_slot,
                .payload_col = payload_slot,
                .target_proj_col = *target_col,
                .payload_type = p_type,
                .payload_unsigned = payload_col.IsUnsigned(),
            });
            pinned_stores.push_back(std::move(store));
          } else if (append_payload ||
                     (can_small_ptr_path && (p_type != ValueType::kInt64 &&
                                             p_type != ValueType::kDate))) {
            if (!can_small_ptr_path || relations[d].HasColumnarOnly()) {
              continue;
            }
            std::vector<const Value*> dense_val_ptrs(static_cast<size_t>(span),
                                                     nullptr);
            for (const Row& r : relations[d].rows) {
              const Value& kv = r[*d_pk_col];
              if (kv.IsNull()) {
                continue;
              }
              const auto off = static_cast<size_t>(
                  static_cast<uint64_t>(kv.value.int_value) -
                  static_cast<uint64_t>(min_k));
              dense_val_ptrs[off] = &r[*d_payload_col];
            }
            const size_t nworkers = std::max<size_t>(
                size_t{1},
                std::min<size_t>({size_t{16},
                                  QueryParallelWorkerLimit(
                                      std::thread::hardware_concurrency()),
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
                  if (pv->owned_data.empty()) {
                    Value& dst = tgt.rows[ri].values_.emplace_back();
                    dst.type = pv->type;
                    dst.SetCollation(pv->Collation());
                    if (pv->IsUnsigned()) {
                      dst.SetUnsigned();
                    }
                    dst.value = pv->value;
                  } else {
                    tgt.rows[ri].values_.push_back(*pv);
                  }
                } else if (pv->owned_data.empty()) {
                  Value& dst = tgt.rows[ri].values_[*target_col];
                  dst.type = pv->type;
                  dst.SetCollation(pv->Collation());
                  if (pv->IsUnsigned()) {
                    dst.SetUnsigned();
                  }
                  dst.owned_data.clear();
                  dst.value = pv->value;
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
                    if (pv->owned_data.empty()) {
                      Value& dst = tgt.rows[ri].values_.emplace_back();
                      dst.type = pv->type;
                      dst.SetCollation(pv->Collation());
                      if (pv->IsUnsigned()) {
                        dst.SetUnsigned();
                      }
                      dst.value = pv->value;
                    } else {
                      tgt.rows[ri].values_.push_back(*pv);
                    }
                  } else if (pv->owned_data.empty()) {
                    cell.type = pv->type;
                    cell.SetCollation(pv->Collation());
                    if (pv->IsUnsigned()) {
                      cell.SetUnsigned();
                    }
                    cell.owned_data.clear();
                    cell.value = pv->value;
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
            if (relations[d].HasColumnarOnly() || tgt.rows.size() < 16384 ||
                span > 16777216ULL ||
                (p_type != ValueType::kDate && p_type != ValueType::kInt64)) {
              continue;
            }
            constexpr int32_t kNullPayloadI32 =
                std::numeric_limits<int32_t>::min();
            if (p_type == ValueType::kInt64) {
              bool fits_i32 = true;
              for (const Row& r : relations[d].rows) {
                const Value& pv = r[*d_payload_col];
                if (pv.IsNull()) {
                  continue;
                }
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
                std::min<size_t>({size_t{16},
                                  QueryParallelWorkerLimit(
                                      std::thread::hardware_concurrency()),
                                  tgt.rows.size() / 16384}));
            const size_t d_rows = relations[d].rows.size();
            bool d_monotonic =
                d_rows >= 64 && span > 1048576ULL && span > d_rows * 8;
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
              std::array<uint32_t, kNumBuckets> bucket_lo{};
              std::array<uint32_t, kNumBuckets> bucket_hi{};
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
                    int64_t last_k = std::numeric_limits<int64_t>::min();
                    bool have_last = false;
                    size_t last_match = d_rows;
                    const int64_t* const keys_ptr = mono_keys.data();
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
                      if (have_last && k == last_k) {
                        match_idx = last_match;
                      } else {
                        if (cursor < d_rows && keys_ptr[cursor] == k) {
                          match_idx = cursor;
                        } else if (cursor < d_rows && k > keys_ptr[cursor]) {
                          if (cursor + 1 < d_rows &&
                              keys_ptr[cursor + 1] == k) {
                            match_idx = ++cursor;
                          } else if (cursor + 64 < d_rows &&
                                     keys_ptr[cursor + 64] >= k) {
                            size_t lo = cursor + 1;
                            size_t step = 4;
                            while (lo + step < d_rows &&
                                   keys_ptr[lo + step] < k) {
                              lo += step;
                              step <<= 1;
                            }
                            const size_t hi = std::min(d_rows, lo + step + 1);
                            const int64_t* const found = std::lower_bound(
                                keys_ptr + lo, keys_ptr + hi, k);
                            cursor = static_cast<size_t>(found - keys_ptr);
                            if (cursor < d_rows && keys_ptr[cursor] == k) {
                              match_idx = cursor;
                            }
                          } else {
                            const size_t b = bucket_of(k);
                            const uint32_t lo = std::max<uint32_t>(
                                bucket_lo[b], static_cast<uint32_t>(cursor));
                            const uint32_t hi = bucket_hi[b];
                            if (lo < hi) {
                              const int64_t* const found = std::lower_bound(
                                  keys_ptr + lo, keys_ptr + hi, k);
                              cursor = static_cast<size_t>(found - keys_ptr);
                              if (found != keys_ptr + hi && *found == k) {
                                match_idx = cursor;
                              }
                            }
                          }
                        } else {
                          const size_t b = bucket_of(k);
                          const uint32_t lo = bucket_lo[b];
                          const uint32_t hi = bucket_hi[b];
                          if (lo < hi) {
                            const int64_t* const found = std::lower_bound(
                                keys_ptr + lo, keys_ptr + hi, k);
                            cursor = static_cast<size_t>(found - keys_ptr);
                            if (found != keys_ptr + hi && *found == k) {
                              match_idx = cursor;
                            }
                          }
                        }
                        have_last = true;
                        last_k = k;
                        last_match = match_idx;
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
                      if (kv.IsNull()) {
                        continue;
                      }
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
          payload_absorbed_into[*target_rel] = 1;
          driver_filter_cache.clear();
          if (!append_payload) {
            for (const ExactFilteredEdge& edge : exact_filtered_by[d]) {
              if (edge.projected_col == *d_payload_col) {
                exact_filtered_by[*target_rel].push_back(
                    ExactFilteredEdge{.projected_col = *target_col,
                                      .driver_rel = edge.driver_rel});
              }
            }
            for (auto& r : exact_filtered_by) {
              for (ExactFilteredEdge& edge : r) {
                if (edge.driver_rel == d) {
                  edge.driver_rel = std::numeric_limits<size_t>::max();
                }
              }
            }
          }
          parallel_clear_rows(relations[d].rows);
          relations[d].columnar.reset();
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

  size_t non_absorbed_count = 0;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (absorbed[i] == 0U) {
      ++non_absorbed_count;
    }
  }
  if (deferred_probe_rel.has_value() && non_absorbed_count < 2) {
    // The fact relation has not been moved yet. A global aggregate can
    // charge its columns in place; anything else still becomes rows before
    // the join loop starts.
    bool scalar_done = false;
    const bool deferred_distinct_only =
        fusion != nullptr && fusion->eligible && fusion->aggregates.empty() &&
        !fusion->group_keys.empty() && !fusion->having &&
        !fusion->post_having_active && fusion->topn_limit == 0 &&
        !fusion->rollup_count_distribution && fusion->output_order.empty();
    if (deferred_probe.has_value() && fusion != nullptr && fusion->eligible &&
        !fusion->fired &&
        ((fusion->group_keys.empty() && columnar_fact_aggs) ||
         deferred_distinct_only) &&
        *where_fully_applied && join_residual.empty()) {
      Relation absorbed_dummy(context.execution_runtime());
      absorbed_dummy.rows.emplace_back();
      ASSIGN_OR_RETURN(
          std::optional<Relation>, fused,
          (TryFusedGroupedJoin(context, relations[*deferred_probe_rel],
                               absorbed_dummy, {}, JoinType::kInner, outer,
                               ctes, fusion, &*deferred_probe)));
      if (fused.has_value()) {
        relations[*deferred_probe_rel] = std::move(*fused);
        deferred_probe.reset();
        deferred_probe_rel.reset();
        fusion->fired = true;
        scalar_done = true;
      }
    }
    if (!scalar_done && deferred_probe_rel.has_value()) {
      RETURN_IF_FAIL(materialize_deferred());
    }
  }

  std::optional<Relation> star_joined;
  if (fusion == nullptr && !deferred_probe_rel && plain_inner_sources &&
      !selects_star && *where_fully_applied &&
      std::ranges::all_of(join_residual, [&](const Expression& expression) {
        return std::ranges::any_of(predicates, [&](const PredicateInfo& p) {
          return p.resolved && !p.contains_query && p.sources.size() > 1 &&
                 p.expression == expression;
        });
      })) {
    ASSIGN_OR_RETURN(std::optional<Relation>, joined_star,
                     TryMaterializedStarJoin(relations, absorbed, predicates,
                                             statement, columnar_output));
    star_joined = std::move(joined_star);
    if (star_joined) {
      join_residual.clear();
    }
  }
  // Factorized join tree (#9, #11, #12) and small-dim star direct scan (#14)
  // for 3+ table acyclic equality join trees with grouped or scalar aggregates.
  // Lean proofs: Wave9AnalyticsRemaining.lean (SelfJoinBridgeBipartiteWeights,
  // FilteredSemiJoinCountBeforeGroup).
  // For 3..5-table ANSI INNER JOINs with multi-column GROUP BY (e.g. LDBC L2,
  // BigBench B3), the downstream typed-stream / greedy path avoids per-row
  // string key encoding; restrict 3..5-table ANSI entry to <= 1 group key.
  if (!star_joined && fusion != nullptr && fusion->eligible && !fusion->fired &&
      (plain_inner_sources ||
       (all_inner_equi_sources &&
        (non_absorbed_count >= 6 || fusion->group_keys.size() <= 1))) &&
      (!deferred_probe_rel.has_value() ||
       estimated_rows[*deferred_probe_rel] <= 4000000) &&
      !selects_star && *where_fully_applied && outer == nullptr &&
      ctes.empty() && non_absorbed_count >= 3 && non_absorbed_count <= 16 &&
      !fusion->aggregates.empty() && fusion->aggregates.size() <= 8 &&
      !fusion->rollup_count_distribution && !fusion->anti_unmatched_only &&
      std::ranges::all_of(join_residual, [&](const Expression& expression) {
        return std::ranges::any_of(predicates, [&](const PredicateInfo& p) {
          return p.resolved && !p.contains_query && p.sources.size() > 1 &&
                 p.expression == expression;
        });
      })) {
    bool all_binary_equi = true;
    for (const PredicateInfo& p : predicates) {
      if (!p.resolved || p.sources.size() < 2) {
        continue;
      }
      if (p.contains_query || p.sources.size() != 2 ||
          !IsColumnEqualityPredicate(p.expression) ||
          p.expression->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        all_binary_equi = false;
        break;
      }
    }
    bool aggs_simple = true;
    for (size_t ai = 0; ai < fusion->aggregates.size(); ++ai) {
      if (fusion->count_star[ai]) {
        continue;
      }
      const AggregateExpression* agg = fusion->aggregates[ai];
      if (agg == nullptr || agg->Distinct() || agg->WhereFilter() ||
          agg->Having() != AggregateHavingModifier::kNone ||
          !agg->InnerOrderBy().empty() || agg->SecondaryArg() ||
          !agg->ExtraArgs().empty() || !agg->TrailingArgs().empty() ||
          !agg->Child() || agg->Child()->Type() != TypeTag::kColumnValue ||
          agg->NeedsGroupContext() || agg->HasInnerGroupBy()) {
        aggs_simple = false;
        break;
      }
      const auto ak = agg->GetType();
      if (ak != AggregationType::kCount && ak != AggregationType::kSum &&
          ak != AggregationType::kAvg && ak != AggregationType::kMin &&
          ak != AggregationType::kMax) {
        aggs_simple = false;
        break;
      }
    }
    bool gkeys_simple =
        std::ranges::all_of(fusion->group_keys, [](const Expression& gk) {
          return gk && gk->Type() == TypeTag::kColumnValue;
        });
    bool all_small_enough = true;
    size_t max_active_rows = 0;
    for (size_t i = 0; i < relations.size(); ++i) {
      if (i < absorbed.size() && absorbed[i] == 0U) {
        const size_t r = (deferred_probe_rel == i) ? estimated_rows[i]
                                                   : relations[i].TotalRows();
        max_active_rows = std::max(max_active_rows, r);
        if (r > 4000000) {
          all_small_enough = false;
          break;
        }
      }
    }
    if (all_binary_equi && aggs_simple && gkeys_simple && all_small_enough) {
      if (deferred_probe_rel.has_value()) {
        RETURN_IF_FAIL(materialize_deferred());
      }
      ASSIGN_OR_RETURN(
          std::optional<Relation>, small_dim_star,
          (TrySmallDimStarDirectAggregate(context, relations, absorbed,
                                          predicates, outer, ctes, fusion)));
      if (small_dim_star.has_value()) {
        if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr) {
          std::cerr << "[afj] small_dim_star fired n=" << non_absorbed_count
                    << "\n";
        }
        star_joined = std::move(*small_dim_star);
        join_residual.clear();
      } else {
        ASSIGN_OR_RETURN(
            std::optional<Relation>, factorized_tree,
            (TryFactorizedJoinTreeAggregate(context, relations, absorbed,
                                            predicates, outer, ctes, fusion)));
        if (factorized_tree.has_value()) {
          if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") !=
              nullptr) {
            std::cerr << "[afj] factorized_tree fired n=" << non_absorbed_count
                      << "\n";
          }
          star_joined = std::move(*factorized_tree);
          join_residual.clear();
        } else if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") !=
                   nullptr) {
          std::cerr << "[afj] both declined n=" << non_absorbed_count << "\n";
        }
      }
    } else if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") !=
               nullptr) {
      std::cerr << "[afj] gate2 skip equi=" << all_binary_equi
                << " agg=" << aggs_simple << " gk=" << gkeys_simple << "\n";
    }
  } else if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr &&
             fusion != nullptr && non_absorbed_count >= 3) {
    std::cerr << "[afj] gate1 skip star=" << star_joined.has_value()
              << " elig=" << fusion->eligible << " fired=" << fusion->fired
              << " plain=" << plain_inner_sources
              << " star_sel=" << selects_star
              << " where=" << *where_fully_applied
              << " n=" << non_absorbed_count
              << " aggs=" << fusion->aggregates.size()
              << " res=" << join_residual.size() << "\n";
  }

  // Lean-backed (IndividualQueryGaps.lean:
  // CompositeKeySemiJoinChainReduction_sound): for all-INNER multi-way join
  // chains with composite (2+ column) int64 equijoins `A ⋈_{k1=k2} B ⋈_{j2=j3}
  // C`, propagate composite-key semi-joins from smaller relations to larger
  // neighbors (2 passes) before the greedy join loop.
  if (!star_joined && all_inner_equi_sources && non_absorbed_count >= 2 &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COMPOSITE_SEMIJOIN") ==
          nullptr) {
    struct CompEdge {
      size_t s1{0};
      size_t s2{0};
      std::vector<std::pair<slot_t, slot_t>> cols;
    };
    std::map<std::pair<size_t, size_t>, std::vector<std::pair<slot_t, slot_t>>>
        edge_map;
    for (const PredicateInfo& p : predicates) {
      if (!p.resolved || p.contains_query || p.sources.size() != 2 ||
          !IsColumnEqualityPredicate(p.expression) ||
          p.expression->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        continue;
      }
      auto it = p.sources.begin();
      const size_t sa = *it++;
      const size_t sb = *it;
      if (sa >= relations.size() || sb >= relations.size() ||
          absorbed[sa] != 0U || absorbed[sb] != 0U ||
          deferred_probe_rel == sa || deferred_probe_rel == sb ||
          relations[sa].HasSpill() || relations[sb].HasSpill()) {
        continue;
      }
      const auto& be = p.expression->AsBinaryExpression();
      const ColumnName& ln = be.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rn = be.Right()->AsColumnValue().GetColumnName();
      auto la = LocalColumnOffset(relations[sa].schema, ln);
      auto rb = LocalColumnOffset(relations[sb].schema, rn);
      if (!la || !rb) {
        la = LocalColumnOffset(relations[sa].schema, rn);
        rb = LocalColumnOffset(relations[sb].schema, ln);
      }
      if (!la || !rb) {
        continue;
      }
      const ValueType ta = relations[sa].schema.GetColumn(*la).Type();
      const ValueType tb = relations[sb].schema.GetColumn(*rb).Type();
      if ((ta != ValueType::kInt64 && ta != ValueType::kDate) || ta != tb) {
        continue;
      }
      const auto key = sa < sb ? std::pair{sa, sb} : std::pair{sb, sa};
      const auto cols = sa < sb ? std::pair{*la, *rb} : std::pair{*rb, *la};
      edge_map[key].emplace_back(cols);
    }
    std::vector<CompEdge> comp_edges;
    for (auto& [pair_idx, cols] : edge_map) {
      if (cols.size() >= 2) {
        comp_edges.push_back(CompEdge{
            .s1 = pair_idx.first,
            .s2 = pair_idx.second,
            .cols = std::move(cols),
        });
      }
    }
    if (!comp_edges.empty()) {
      struct VecIntHash {
        size_t operator()(const std::vector<int64_t>& v) const noexcept {
          uint64_t h = 14695981039346656037ULL;
          for (int64_t x : v) {
            uint64_t z = static_cast<uint64_t>(x) + 0x9e3779b97f4a7c15ULL;
            z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
            z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
            h ^= z ^ (z >> 31);
            h *= 1099511628211ULL;
          }
          return static_cast<size_t>(h);
        }
      };
      for (int pass = 0; pass < 2; ++pass) {
        bool any_reduced = false;
        for (const CompEdge& e : comp_edges) {
          const size_t n1 = rel_row_count(e.s1);
          const size_t n2 = rel_row_count(e.s2);
          if (n1 == 0 || n2 == 0) {
            continue;
          }
          const bool s1_smaller = (n1 <= n2);
          const size_t small_i = s1_smaller ? e.s1 : e.s2;
          const size_t large_i = s1_smaller ? e.s2 : e.s1;
          const size_t n_small = s1_smaller ? n1 : n2;
          const size_t n_large = s1_smaller ? n2 : n1;
          if (n_small > 200000 || n_large < 64 || n_small * 2 > n_large ||
              (relations[large_i].columnar && n_large > 500000)) {
            continue;
          }
          RETURN_IF_FAIL(relations[small_i].EnsureRowsMaterialized());
          RETURN_IF_FAIL(relations[large_i].EnsureRowsMaterialized());
          std::unordered_set<std::vector<int64_t>, VecIntHash> keys;
          keys.reserve(n_small);
          std::vector<int64_t> kbuf(e.cols.size());
          for (const Row& r : relations[small_i].rows) {
            bool has_null = false;
            for (size_t k = 0; k < e.cols.size(); ++k) {
              const slot_t c = s1_smaller ? e.cols[k].first : e.cols[k].second;
              if (r[c].IsNull()) {
                has_null = true;
                break;
              }
              kbuf[k] = r[c].value.int_value;
            }
            if (!has_null) {
              keys.insert(kbuf);
            }
          }
          std::vector<Row> kept;
          kept.reserve(std::min(n_large, (keys.size() * 2) + 16));
          for (Row& r : relations[large_i].rows) {
            bool has_null = false;
            for (size_t k = 0; k < e.cols.size(); ++k) {
              const slot_t c = s1_smaller ? e.cols[k].second : e.cols[k].first;
              if (r[c].IsNull()) {
                has_null = true;
                break;
              }
              kbuf[k] = r[c].value.int_value;
            }
            if (!has_null && keys.contains(kbuf)) {
              kept.push_back(std::move(r));
            }
          }
          const bool reduced = kept.size() < relations[large_i].rows.size();
          relations[large_i].rows = std::move(kept);
          if (reduced) {
            relations[large_i].columnar.reset();
            any_reduced = true;
          }
        }
        if (!any_reduced) {
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
  for (size_t start = 0; !star_joined && start < relations.size(); ++start) {
    if (absorbed[start] != 0U || deferred_probe_rel == start) {
      continue;
    }
    size_t sim_current = rel_row_count(start);
    size_t sim_peak = sim_current;
    size_t sim_total = 0;
    std::unordered_set<size_t> sim_joined{start};
    std::unordered_set<size_t> sim_remaining;
    sim_remaining.reserve(relations.size());
    for (size_t i = 0; i < relations.size(); ++i) {
      if (i != start && (absorbed[i] == 0U)) {
        sim_remaining.insert(i);
      }
    }
    while (!sim_remaining.empty()) {
      size_t sim_next = *sim_remaining.begin();
      size_t sim_est = std::numeric_limits<size_t>::max();
      bool sim_next_equijoin = false;
      bool sim_next_connected = false;
      for (size_t cand : sim_remaining) {
        std::unordered_set<size_t> after = sim_joined;
        after.insert(cand);
        size_t app_count = 0;
        bool has_equi = false;
        for (const PredicateInfo& p : predicates) {
          if (!p.resolved || p.contains_query || p.sources.size() < 2 ||
              !p.sources.contains(cand) || !IsSubset(p.sources, after)) {
            continue;
          }
          ++app_count;
          if (!has_equi && IsColumnEqualityPredicate(p.expression)) {
            has_equi = true;
          }
        }
        const bool connected = app_count > 0;
        const size_t cand_rows = rel_row_count(cand);
        const size_t est = has_equi ? std::min(sim_current, cand_rows)
                                    : sim_current * cand_rows;
        const bool cheaper =
            est < sim_est ||
            (est == sim_est && cand_rows < rel_row_count(sim_next));
        if ((has_equi && !sim_next_equijoin) ||
            (has_equi == sim_next_equijoin &&
             ((connected && !sim_next_connected) ||
              (connected == sim_next_connected && cheaper)))) {
          sim_next = cand;
          sim_est = est;
          sim_next_equijoin = has_equi;
          sim_next_connected = connected;
        }
      }
      sim_current = sim_est;
      sim_peak = std::max(sim_peak, sim_current);
      sim_total += sim_current;
      sim_joined.insert(sim_next);
      sim_remaining.erase(sim_next);
    }
    const size_t start_rows = rel_row_count(start);
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
  std::vector<Schema> initial_schemas;
  initial_schemas.reserve(relations.size());
  for (const Relation& rel : relations) {
    initial_schemas.push_back(rel.schema);
  }
  Relation result =
      star_joined ? std::move(*star_joined) : std::move(relations[first]);
  std::unordered_set<size_t> joined{first};
  std::unordered_set<size_t> remaining;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!star_joined && i != first && (absorbed[i] == 0U)) {
      remaining.insert(i);
    }
  }

  // A truthy TINYLAMB_LEGACY_PLANNER restores per-call estimation behavior;
  // an explicit "0"/"false" must leave the optimized path active.
  static const bool legacy_planner = [] {
    const char* env = ::tinylamb::platform::GetEnv("TINYLAMB_LEGACY_PLANNER");
    return env != nullptr && env[0] != '\0' && std::string_view(env) != "0" &&
           std::string_view(env) != "false";
  }();
  std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>>
      frequency_cache;
  const auto collect_deduped_applicable =
      [&](size_t target_rel, const std::unordered_set<size_t>& after_set,
          std::vector<Expression>* out_applicable, bool mark) {
        struct SeenTargetEquality {
          size_t target_col{0};
          size_t other_rel{0};
          size_t other_col{0};
        };
        std::vector<SeenTargetEquality> seen_target_cols;
        const Schema& target_schema =
            relations[target_rel].schema.ColumnCount() > 0
                ? relations[target_rel].schema
                : initial_schemas[target_rel];
        // Lean proof:
        // lean/TinylambOpt/ResearchProofs/TransitiveEquivPredicatePushdownAndDupKeyDrop.lean
        // (sound) Union-Find transitive equivalence closure over equality edges
        // already established inside `after_set \ {target_rel}` (never via
        // unjoined rels).
        std::unordered_map<uint64_t, size_t> uf_node_id;
        std::vector<size_t> uf_parent;
        const auto uf_get = [&](size_t r, size_t col) -> size_t {
          const uint64_t k =
              (static_cast<uint64_t>(r) << 32) | static_cast<uint64_t>(col);
          auto [it, ins] = uf_node_id.emplace(k, uf_parent.size());
          if (ins) {
            uf_parent.push_back(it->second);
          }
          return it->second;
        };
        const auto uf_find = [&](size_t x) -> size_t {
          while (uf_parent[x] != x) {
            uf_parent[x] = uf_parent[uf_parent[x]];
            x = uf_parent[x];
          }
          return x;
        };
        if (!transitive_filter_disabled) {
          for (const PredicateInfo& p : predicates) {
            if (!p.resolved || p.contains_query || p.sources.size() != 2 ||
                p.sources.contains(target_rel) ||
                !IsColumnEqualityPredicate(p.expression) ||
                p.expression->AsBinaryExpression().Op() !=
                    BinaryOperation::kEquals) {
              continue;
            }
            auto sit = p.sources.begin();
            const size_t r0 = *sit++;
            const size_t r1 = *sit;
            if (!after_set.contains(r0) || !after_set.contains(r1)) {
              continue;
            }
            const Schema& s0 = relations[r0].schema.ColumnCount() > 0
                                   ? relations[r0].schema
                                   : initial_schemas[r0];
            const Schema& s1 = relations[r1].schema.ColumnCount() > 0
                                   ? relations[r1].schema
                                   : initial_schemas[r1];
            const BinaryExpression& pbin = p.expression->AsBinaryExpression();
            const ColumnName& pl = pbin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& pr =
                pbin.Right()->AsColumnValue().GetColumnName();
            const auto l0 = LocalColumnOffset(s0, pl);
            const auto r0o = LocalColumnOffset(s0, pr);
            const auto l1 = LocalColumnOffset(s1, pl);
            const auto r1o = LocalColumnOffset(s1, pr);
            std::optional<size_t> c0;
            std::optional<size_t> c1;
            if (l0.has_value() && r1o.has_value() && !r0o.has_value() &&
                !l1.has_value()) {
              c0 = static_cast<size_t>(*l0);
              c1 = static_cast<size_t>(*r1o);
            } else if (r0o.has_value() && l1.has_value() && !l0.has_value() &&
                       !r1o.has_value()) {
              c0 = static_cast<size_t>(*r0o);
              c1 = static_cast<size_t>(*l1);
            }
            if (!c0.has_value() || !c1.has_value()) {
              continue;
            }
            const Column& col0 = s0.GetColumn(*c0);
            const Column& col1 = s1.GetColumn(*c1);
            if ((col0.Type() != ValueType::kInt64 &&
                 col0.Type() != ValueType::kDate) ||
                col0.Type() != col1.Type() || col0.IsUnsigned() ||
                col1.IsUnsigned() || col0.IsInterval() || col1.IsInterval()) {
              continue;
            }
            const size_t ra = uf_find(uf_get(r0, *c0));
            const size_t rb = uf_find(uf_get(r1, *c1));
            if (ra != rb) {
              uf_parent[rb] = ra;
            }
          }
        }
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
            size_t other_rel = *predicate.sources.begin();
            if (other_rel == target_rel) {
              other_rel = *std::next(predicate.sources.begin());
            }
            const Schema& other_schema =
                relations[other_rel].schema.ColumnCount() > 0
                    ? relations[other_rel].schema
                    : initial_schemas[other_rel];
            const BinaryExpression& bin =
                predicate.expression->AsBinaryExpression();
            const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rhs =
                bin.Right()->AsColumnValue().GetColumnName();
            const auto t_l = LocalColumnOffset(target_schema, lhs);
            const auto t_r = LocalColumnOffset(target_schema, rhs);
            const auto o_l = LocalColumnOffset(other_schema, lhs);
            const auto o_r = LocalColumnOffset(other_schema, rhs);
            std::optional<size_t> t_col;
            std::optional<size_t> o_col;
            if (t_l.has_value() && o_r.has_value() && !t_r.has_value() &&
                !o_l.has_value()) {
              t_col = t_l;
              o_col = o_r;
            } else if (t_r.has_value() && o_l.has_value() && !t_l.has_value() &&
                       !o_r.has_value()) {
              t_col = t_r;
              o_col = o_l;
            }
            if (t_col.has_value() && o_col.has_value()) {
              bool redundant = false;
              for (const SeenTargetEquality& prev : seen_target_cols) {
                if (prev.target_col != *t_col) {
                  continue;
                }
                if (prev.other_rel == other_rel && prev.other_col == *o_col) {
                  redundant = true;
                  break;
                }
                if (prev.other_rel != other_rel) {
                  const Schema& prev_schema =
                      relations[prev.other_rel].schema.ColumnCount() > 0
                          ? relations[prev.other_rel].schema
                          : initial_schemas[prev.other_rel];
                  const Column& prev_c = prev_schema.GetColumn(prev.other_col);
                  const Column& cur_c = other_schema.GetColumn(*o_col);
                  const Column& tgt_c = target_schema.GetColumn(*t_col);
                  if ((prev_c.Type() == ValueType::kInt64 ||
                       prev_c.Type() == ValueType::kDate) &&
                      prev_c.Type() == cur_c.Type() &&
                      prev_c.Type() == tgt_c.Type() && !prev_c.IsUnsigned() &&
                      !cur_c.IsUnsigned() && !tgt_c.IsUnsigned() &&
                      !prev_c.IsInterval() && !cur_c.IsInterval() &&
                      !tgt_c.IsInterval()) {
                    const auto it_a = uf_node_id.find(
                        (static_cast<uint64_t>(prev.other_rel) << 32) |
                        static_cast<uint64_t>(prev.other_col));
                    const auto it_b = uf_node_id.find(
                        (static_cast<uint64_t>(other_rel) << 32) |
                        static_cast<uint64_t>(*o_col));
                    if (it_a != uf_node_id.end() && it_b != uf_node_id.end() &&
                        uf_find(it_a->second) == uf_find(it_b->second)) {
                      redundant = true;
                    }
                  }
                  if (redundant) {
                    break;
                  }
                }
              }
              if (redundant) {
                continue;
              }
              seen_target_cols.push_back(
                  SeenTargetEquality{.target_col = *t_col,
                                     .other_rel = other_rel,
                                     .other_col = *o_col});
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
          if (s.GetColumn(c).Type() == ValueType::kVarChar) {
            ++n;
          }
        }
        return n;
      };
      const bool group_keys_in_result =
          std::ranges::all_of(fusion->group_keys, [&](const Expression& gk) {
            if (!gk) {
              return false;
            }
            const auto cols = gk->TouchedColumns();
            return !cols.empty() &&
                   std::ranges::all_of(cols, [&](const ColumnName& cn) {
                     return LocalColumnOffset(result.schema, cn).has_value();
                   });
          });
      if (deferred_probe_rel != r1 && deferred_probe_rel != r2 &&
          group_keys_in_result && count_varchar_cols(result.schema) >= 2 &&
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
            // The address now holds a completely different relation: drop the
            // frequency maps derived from its previous rows.
            frequency_cache.erase(&relations[r1]);
            frequency_cache.erase(&relations[r2]);
            for (PredicateInfo& p : predicates) {
              if (!p.resolved) {
                continue;
              }
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
    if (deferred_probe_rel.has_value() && remaining.size() > 1) {
      for (size_t c : remaining) {
        if (deferred_probe_rel != c) {
          next = c;
          break;
        }
      }
    }
    size_t next_estimate = std::numeric_limits<size_t>::max();
    bool next_equijoin = false;
    bool next_connected = false;
    const auto has_equijoin = [&](size_t cand,
                                  const std::vector<Expression>& app) -> bool {
      return !app.empty() &&
             !EqualityKeys(result.schema, relations[cand].schema, app).empty();
    };
    const auto pick_better = [&](size_t candidate, bool cand_equijoin,
                                 bool cand_connected, size_t cand_estimate) {
      const bool cheaper =
          cand_estimate < next_estimate ||
          (cand_estimate == next_estimate &&
           relations[candidate].TotalRows() < relations[next].TotalRows());
      if ((cand_equijoin && !next_equijoin) ||
          (cand_equijoin == next_equijoin &&
           ((cand_connected && !next_connected) ||
            (cand_connected == next_connected && cheaper)))) {
        next = candidate;
        next_estimate = cand_estimate;
        next_equijoin = cand_equijoin;
        next_connected = cand_connected;
      }
    };
    for (size_t candidate : remaining) {
      // There is no join-order choice at the last step. A frequency sweep
      // of a spilled fact table cannot change the selected input and can
      // cost another complete spill read before the actual hash probe.
      if (remaining.size() == 1) {
        break;
      }
      if (deferred_probe_rel == candidate) {
        continue;
      }
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
      const bool equijoin = has_equijoin(candidate, applicable);
      pick_better(candidate, equijoin, connected, estimate);
    }
    const size_t pair_res_rows = result.TotalRows();
    const size_t pair_next_rows = relations[next].TotalRows();
    const size_t pair_rows =
        (pair_res_rows > 0 &&
         pair_next_rows > std::numeric_limits<size_t>::max() / pair_res_rows)
            ? std::numeric_limits<size_t>::max()
            : pair_res_rows * pair_next_rows;
    const bool nested_loop_explosion =
        !next_equijoin && pair_res_rows > 1 && pair_next_rows > 1 &&
        (pair_rows > 262144 || next_estimate > 262144);
    // A numeric fact probe is only consumed by the final fused aggregate.
    // Materializing it to refine an intermediate estimate rebuilds every
    // fact row that the columnar charge was meant to avoid.
    if ((!next_connected || nested_loop_explosion ||
         next_estimate >
             std::max(result.TotalRows(), relations[next].TotalRows()) * 2 ||
         next_estimate > 262144) &&
        deferred_probe_rel.has_value() && remaining.size() > 1 &&
        remaining.contains(*deferred_probe_rel) &&
        (!columnar_fact_aggs || nested_loop_explosion ||
         (!next_connected && result.TotalRows() > 1 &&
          relations[next].TotalRows() > 1 && next_estimate > 262144))) {
      RETURN_IF_FAIL(materialize_deferred());
      next_estimate = std::numeric_limits<size_t>::max();
      next_equijoin = false;
      next_connected = false;
      for (size_t candidate : remaining) {
        std::unordered_set<size_t> after = joined;
        after.insert(candidate);
        std::vector<Expression> applicable;
        collect_deduped_applicable(candidate, after, &applicable, false);
        const auto estimate_begin = std::chrono::steady_clock::now();
        const size_t estimate =
            legacy_planner
                ? EstimateJoinRows(result, relations[candidate], applicable)
                : EstimateJoinRowsCached(result, relations[candidate],
                                         applicable, &frequency_cache);
        join_estimate_ms += ElapsedMs(estimate_begin);
        const bool connected = !applicable.empty();
        const bool equijoin = has_equijoin(candidate, applicable);
        pick_better(candidate, equijoin, connected, estimate);
      }
    }

    std::unordered_set<size_t> after = joined;
    after.insert(next);
    std::vector<Expression> applicable;
    collect_deduped_applicable(next, after, &applicable, true);
    if (deferred_probe_rel == next && deferred_probe.has_value() &&
        deferred_probe->pk_lookups.empty() && !result.HasSpill() &&
        result.TotalRows() > 0 && result.TotalRows() <= 16384 &&
        !applicable.empty()) {
      const auto eqs =
          EqualityKeys(result.schema, relations[next].schema, applicable);
      for (const EqualityKey& eq : eqs) {
        if (eq.null_safe || eq.left_offset != 0 || eq.right_offset != 0 ||
            eq.right >= deferred_probe->projection.size()) {
          continue;
        }
        const Column& lc = result.schema.GetColumn(eq.left);
        const Column& rc = relations[next].schema.GetColumn(eq.right);
        if (lc.IsUnsigned() || rc.IsUnsigned() || lc.Type() != rc.Type() ||
            (lc.Type() != ValueType::kInt64 && lc.Type() != ValueType::kDate)) {
          continue;
        }
        RuntimeFilterBuilder rfb;
        rfb.Reserve(result.TotalRows());
        if (result.HasColumnarOnly()) {
          for (const ColumnarChunk& ch : result.columnar->chunks) {
            const ColumnarColumn& cc = ch.columns[eq.left];
            for (size_t r = 0; r < ch.rows; ++r) {
              int64_t iv = 0;
              if (cc.TryReadNonNullInt(r, &iv)) {
                rfb.Observe(iv);
              }
            }
          }
        } else {
          for (const Row& r : result.rows) {
            const Value& kv = r[eq.left];
            if (!kv.IsNull()) {
              rfb.Observe(kv.value.int_value);
            }
          }
        }
        auto grp = rfb.Build(false);
        if (!grp || grp->empty()) {
          continue;
        }
        ScanRuntimeFilter refined{
            .group = std::move(grp),
            .column = static_cast<slot_t>(eq.right),
            .type = rc.Type(),
        };
        ScanRuntimeFilter& cur = deferred_probe->runtime_filter;
        if (!cur.enabled()) {
          cur = std::move(refined);
        } else if (refined.group->DistinctKeys() < cur.group->DistinctKeys()) {
          ScanRuntimeFilter prev_primary{
              .group = std::move(cur.group),
              .column = cur.column,
              .type = cur.type,
          };
          std::vector<ScanRuntimeFilter> extras = std::move(cur.extra);
          cur = std::move(refined);
          cur.extra.push_back(std::move(prev_primary));
          for (auto& ex : extras) {
            if (cur.extra.size() < 7) {
              cur.extra.push_back(std::move(ex));
            }
          }
        } else if (cur.extra.size() < 7) {
          cur.extra.push_back(std::move(refined));
        }
      }
    }
    // Final join step of a fusable grouped statement: feed the matched pairs
    // straight into the aggregation instead of materializing the joined
    // relation.  Everything was already filtered locally, so no join output
    // ever exists.  A nullopt (shape not fusable) falls through to the
    // materializing join below.
    if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr) {
      // The spec is null for statements that never reached the shape gate;
      // reading `fusion->eligible` here would fault (oracle: any grouped
      // query with TINYLAMB_FUSION_DEBUG set).
      std::cerr << "[fusion] greedy hook: eligible="
                << (fusion != nullptr && fusion->eligible)
                << " fully_applied=" << *where_fully_applied
                << " last=" << (remaining.size() == 1)
                << " residual=" << join_residual.size()
                << " next=" << statement.Sources()[next].table
                << " left_rows=" << result.TotalRows()
                << " right_rows=" << rel_row_count(next) << "\n";
    }
    if (fusion != nullptr && fusion->eligible && *where_fully_applied &&
        remaining.size() == 1 && join_residual.empty()) {
      const bool group_keys_in_result =
          fusion->group_keys.empty() ||
          std::ranges::all_of(fusion->group_keys, [&](const Expression& gk) {
            if (!gk) {
              return false;
            }
            const auto cols = gk->TouchedColumns();
            return !cols.empty() &&
                   std::ranges::all_of(cols, [&](const ColumnName& cn) {
                     return LocalColumnOffset(result.schema, cn).has_value();
                   });
          });
      const size_t next_rows = rel_row_count(next);
      const bool flip_fused_sides =
          !result.HasSpill() &&
          (deferred_probe_rel == next || relations[next].HasSpill() ||
           ((next_rows >= kFlipMinBuildRows &&
             next_rows > result.TotalRows() * kFlipRatio) ||
            (group_keys_in_result && result.TotalRows() <= 524288 &&
             next_rows >= result.TotalRows())));
      const DeferredLSTableProbe* dp_ptr =
          (flip_fused_sides && deferred_probe_rel == next &&
           deferred_probe.has_value())
              ? &*deferred_probe
              : nullptr;
      const bool tried_deferred_fusion = (dp_ptr != nullptr);
      if (deferred_probe_rel == next && dp_ptr == nullptr) {
        RETURN_IF_FAIL(materialize_deferred());
      }
      ASSIGN_OR_RETURN(
          std::optional<Relation>, fused,
          (flip_fused_sides ? TryFusedGroupedJoin(
                                  context, relations[next], result, applicable,
                                  JoinType::kInner, outer, ctes, fusion, dp_ptr)
                            : TryFusedGroupedJoin(
                                  context, result, relations[next], applicable,
                                  JoinType::kInner, outer, ctes, fusion)));
      if (!fused.has_value() && tried_deferred_fusion &&
          deferred_probe_rel == next) {
        RETURN_IF_FAIL(materialize_deferred());
        ASSIGN_OR_RETURN(
            std::optional<Relation>, retry_fused,
            (TryFusedGroupedJoin(context, relations[next], result, applicable,
                                 JoinType::kInner, outer, ctes, fusion)));
        fused = std::move(retry_fused);
      }
      if (fused.has_value()) {
        deferred_probe.reset();
        deferred_probe_rel.reset();
        result = std::move(*fused);
        fusion->fired = true;
        joined.insert(next);
        remaining.erase(next);
        break;
      }
    }
    if (deferred_probe_rel == next) {
      RETURN_IF_FAIL(materialize_deferred());
    }
    ASSIGN_OR_RETURN(
        Relation, result_r,
        (InnerJoin(context, std::move(result), std::move(relations[next]),
                   applicable, outer, ctes)));
    result = std::move(result_r);
    joined.insert(next);
    remaining.erase(next);
    // Lean proof: lean/TinylambOpt/ResearchProofs/PostJoinDeadColumnPrune.lean
    // (sound)
    static const bool post_join_prune_disabled =
        ::tinylamb::platform::GetEnv(
            "TINYLAMB_DISABLE_POST_JOIN_DEAD_COL_PRUNE") != nullptr;
    if (!post_join_prune_disabled && !remaining.empty() && !selects_star &&
        !result.HasSpill() && result.using_columns == nullptr &&
        result.schema.ColumnCount() >= 6 && !statement.HasDistinctOn() &&
        !statement.ForUpdate() && !statement.Sources().empty() &&
        std::ranges::all_of(statement.Sources(), [](const auto& s) {
          return s.using_columns.empty() && !s.is_lateral && !s.unnest &&
                 !s.from_nested_join;
        })) {
      bool unique_col_names = true;
      std::unordered_set<std::string> seen_full_names;
      for (size_t c = 0; c < result.schema.ColumnCount(); ++c) {
        const ColumnName& cn = result.schema.GetColumn(c).Name();
        std::string key = AsciiToLower(cn.schema) + "." + AsciiToLower(cn.name);
        if (!seen_full_names.insert(std::move(key)).second) {
          unique_col_names = false;
          break;
        }
      }
      if (unique_col_names) {
        SelectStatement future(statement);
        std::vector<SelectSource> rem_sources;
        rem_sources.reserve(remaining.size());
        for (size_t r = 0; r < statement.Sources().size(); ++r) {
          if (remaining.contains(r)) {
            rem_sources.push_back(statement.Sources()[r]);
          }
        }
        future.SetSources(std::move(rem_sources));
        std::vector<Expression> rem_preds;
        for (const PredicateInfo& p : predicates) {
          if (!p.resolved || !IsSubset(p.sources, joined)) {
            if (p.expression) {
              rem_preds.push_back(p.expression);
            }
          }
        }
        for (const Expression& jr : join_residual) {
          if (jr) {
            rem_preds.push_back(jr);
          }
        }
        if (!*where_fully_applied && statement.WhereClause()) {
          rem_preds.push_back(statement.WhereClause());
        }
        future.SetWhereClause(CombineConjuncts(rem_preds));
        const std::vector<slot_t> live_cols =
            RequiredColumns(future, result.schema);
        if (!live_cols.empty() &&
            live_cols.size() + 2 <= result.schema.ColumnCount()) {
          if (result.HasColumnarOnly()) {
            bool all_uniform = true;
            result.columnar = ProjectColumnarTableBySlots(
                *result.columnar, live_cols, &all_uniform);
            result.uniform_cell_types = all_uniform;
            result.schema = ProjectSchema(result.schema, live_cols);
          } else {
            const size_t n_rows = result.rows.size();
            const size_t workers = ParallelWorkerCount(n_rows / 8192);
            RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
              const size_t begin = n_rows * w / workers;
              const size_t end = n_rows * (w + 1) / workers;
              for (size_t r = begin; r < end; ++r) {
                auto& vals = result.rows[r].values_;
                for (size_t c = 0; c < live_cols.size(); ++c) {
                  if (c != live_cols[c]) {
                    vals[c] = std::move(vals[live_cols[c]]);
                  }
                }
                vals.resize(live_cols.size());
              }
            }));
            result.schema = ProjectSchema(result.schema, live_cols);
          }
        }
      }
    }
  }
  if (deferred_probe_rel.has_value()) {
    RETURN_IF_FAIL(materialize_deferred());
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
  if (!pinned_stores.empty()) {
    // VARCHAR cells produced from these stores are views. Keep the stores
    // alive for the rest of the query instead of copying every string onto
    // the result rows. A missing runtime cannot hold the stores, so that
    // path still materializes owned bytes before the locals die.
    if (auto* runtime = context.execution_runtime()) {
      runtime->retained_scan_stores.reserve(
          runtime->retained_scan_stores.size() + pinned_stores.size());
      for (auto& store : pinned_stores) {
        runtime->retained_scan_stores.emplace_back(std::move(store));
      }
    } else {
      if (result.HasColumnarOnly()) {
        RETURN_IF_FAIL(result.EnsureRowsMaterialized());
      }
      for (Row& r : result.rows) {
        for (Value& v : r.values_) {
          if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
              !v.value.varchar_value.empty()) {
            v.owned_data.assign(v.value.varchar_value);
            v.value.varchar_value = v.owned_data;
          }
        }
      }
    }
    pinned_stores.clear();
  }
  {
    size_t total_dead_rows = 0;
    for (const Relation& rel : relations) {
      total_dead_rows += rel.rows.size();
    }
    if (total_dead_rows >= 32768) {
      std::vector<std::pair<Row*, size_t>> ranges;
      ranges.reserve(relations.size());
      for (Relation& rel : relations) {
        if (!rel.rows.empty()) {
          ranges.emplace_back(rel.rows.data(), rel.rows.size());
        }
      }
      const size_t nw = std::max<size_t>(
          size_t{1}, std::min<size_t>({size_t{16},
                                       QueryParallelWorkerLimit(
                                           std::thread::hardware_concurrency()),
                                       total_dead_rows / 16384}));
      if (nw > 1) {
        std::vector<std::jthread> threads;
        threads.reserve(nw - 1);
        auto clear_shard = [&](size_t w) {
          const size_t lo = (total_dead_rows * w) / nw;
          const size_t hi = (total_dead_rows * (w + 1)) / nw;
          size_t base = 0;
          for (const auto& [ptr, len] : ranges) {
            const size_t next_base = base + len;
            if (next_base > lo && base < hi) {
              const size_t r_lo = lo > base ? lo - base : 0;
              const size_t r_hi = std::min(len, hi - base);
              for (size_t i = r_lo; i < r_hi; ++i) {
                std::vector<Value>().swap(ptr[i].values_);
              }
            }
            base = next_base;
          }
        };
        for (size_t w = 1; w < nw; ++w) {
          threads.emplace_back([&, w] { clear_shard(w); });
        }
        clear_shard(0);
      }
      for (Relation& rel : relations) {
        std::vector<Row>().swap(rel.rows);
      }
    }
  }
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied =
        pushed_proj_applied_local && join_residual.empty() &&
        !deferred_probe_rel.has_value() && !star_joined.has_value();
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_estimate_ms += join_estimate_ms;
    context.execution_runtime()->planning_ms +=
        ElapsedMs(planning_begin) - join_estimate_ms;
  }
  return result;
}

}  // namespace tinylamb::relational_detail
