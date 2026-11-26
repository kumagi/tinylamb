/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
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
#include "common/parallel_work.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/constant_executor.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/grouped_numeric_hash.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/relational_projection.hpp"
#include "executor/detail/relational_set_ops.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
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
#include "expression/cast_expression.hpp"
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
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {



std::optional<SelectStatement> GroupDistinctSetInput(
    const SelectStatement& statement) {
  if (statement.Sources().size() < 2 || !statement.GroupBy().empty() ||
      statement.SelectList().empty() || statement.HasLimit() ||
      statement.Offset() != 0 || statement.WithTies() ||
      statement.ForUpdate() || statement.Having() || statement.Qualify() ||
      statement.HasDistinctOn() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() || !statement.WithQueries().empty() ||
      HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement)) {
    return std::nullopt;
  }
  std::vector<Expression> keys;
  for (const auto& item : statement.SelectList()) {
    if (!item.expression || item.expression->Type() != TypeTag::kColumnValue ||
        item.expression->AsColumnValue().GetColumnName().name == "*" ||
        item.expression->AsColumnValue().GetColumnName().name.empty()) {
      return std::nullopt;
    }
    keys.push_back(item.expression);
  }
  SelectStatement grouped(statement);
  grouped.SetGroupBy(std::move(keys));
  grouped.SetDistinct(false);
  return grouped;
}

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
  RETURN_IF_FAIL(anchor_result.FinishSpill());

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
  RETURN_IF_FAIL(delta.FinishSpill());

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
    if (auto* runtime = context.execution_runtime()) {
      // Every round rebinds this CTE name to a fresh working Relation while
      // the recursive branch statements keep the same ASTs and addresses.
      // The uncorrelated-subquery caches (pointer fast path, memoized
      // structural key, and the node-keyed scalar/membership caches) would
      // otherwise hand the new round the PREVIOUS round's value for a
      // subquery that reads this CTE. Drop the CTE-dependent entries so this
      // round evaluates against the new working table. base_relations holds
      // reusable source-scan materializations; keep it separate from these
      // per-round subquery results.
      runtime->uncorrelated_results.clear();
      runtime->uncorrelated_results_by_fingerprint.clear();
      runtime->cte_subquery_results.clear();
      runtime->uncorrelated_key_memo.clear();
      runtime->uncorrelated_membership_by_node.clear();
      runtime->uncorrelated_scalar_by_node.clear();
    }
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
    // `result` keeps growing across rounds: finishing its spill writer here
    // made the next round's AddRow fail with "Append after FinishWriting"
    // once the accumulated rows spilled under a small budget.
    RETURN_IF_FAIL(result.GetStatus());
    delta = std::move(next);
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  return result;
}

namespace {

bool BranchHasLargeDataScan(TransactionContext& context,
                            const SelectStatement& stmt, const CteMap& ctes) {
  constexpr uint64_t kLargeParallelScanRows = 65536;
  for (const SelectSource& source : stmt.Sources()) {
    if (source.query) {
      if (BranchHasLargeDataScan(context, *source.query, ctes)) {
        return true;
      }
      continue;
    }
    if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      if (cte->second != nullptr &&
          cte->second->TotalRows() >= kLargeParallelScanRows) {
        return true;
      }
      continue;
    }
    if (auto st = context.GetStats(source.table);
        st.HasValue() && st.Value()->Rows() >= kLargeParallelScanRows) {
      return true;
    }
    if (auto tbl = context.GetTable(source.table); tbl.HasValue()) {
      if (tbl.Value()->BackingColumnarRowCount() >= kLargeParallelScanRows) {
        return true;
      }
    }
  }
  for (const auto& [name, cte_stmt] : stmt.WithQueries()) {
    (void)name;
    if (cte_stmt != nullptr &&
        BranchHasLargeDataScan(context, *cte_stmt, ctes)) {
      return true;
    }
  }
  return false;
}

bool BranchLooksNonTrivial(TransactionContext& context,
                           const SelectStatement& stmt, const CteMap& ctes) {
  if (stmt.Sources().size() > 1 || !stmt.UnionAll().empty() ||
      stmt.GetSetOperationTree() != nullptr || !stmt.WithQueries().empty() ||
      HasWindowFunctions(stmt) ||
      (stmt.WhereClause() && ContainsQuery(stmt.WhereClause()))) {
    return true;
  }
  for (const SelectSource& source : stmt.Sources()) {
    if (source.query) {
      if (BranchLooksNonTrivial(context, *source.query, ctes)) {
        return true;
      }
      return true;
    }
    if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      if (cte->second != nullptr && cte->second->TotalRows() >= 256) {
        return true;
      }
      continue;
    }
    if (auto st = context.GetStats(source.table);
        st.HasValue() && st.Value()->Rows() >= 256) {
      return true;
    }
    if (auto tbl = context.GetTable(source.table); tbl.HasValue()) {
      if (tbl.Value()->BackingColumnarRowCount() >= 256) {
        return true;
      }
    }
  }
  return false;
}

void InitWorkerRuntime(const ExecutionRuntime& parent,
                       ExecutionRuntime* worker) {
  worker->root_statement = parent.root_statement;
  worker->reusable_base_relations = parent.reusable_base_relations;
  worker->reusable_projections = parent.reusable_projections;
  worker->grouping_rank_limit = parent.grouping_rank_limit;
  worker->shared_base_cache = parent.shared_base_cache;
  // Deliberately do not copy cte_subquery_results (or the other uncorrelated
  // caches): a worker runtime is created fresh for each parallel dispatch,
  // so inheriting a prior round's CTE bindings would be the stale-state bug.
  // An empty cache here is what keeps recursive rounds isolated.
}

void MergeWorkerRuntimeIntoParent(ExecutionRuntime& worker,
                                  ExecutionRuntime* parent) {
  if (parent == nullptr) {
    return;
  }
  parent->retained_scan_stores.insert(
      parent->retained_scan_stores.end(),
      std::make_move_iterator(worker.retained_scan_stores.begin()),
      std::make_move_iterator(worker.retained_scan_stores.end()));
  parent->retained_statements.insert(
      parent->retained_statements.end(),
      std::make_move_iterator(worker.retained_statements.begin()),
      std::make_move_iterator(worker.retained_statements.end()));
  for (auto& [k, arena] : worker.base_relation_arenas) {
    parent->base_relation_arenas.emplace(k, std::move(arena));
  }
  parent->shared_exists_build_reuses += worker.shared_exists_build_reuses;
  parent->correlated_index_builds += worker.correlated_index_builds;
  parent->prebuilt_exists_index_builds += worker.prebuilt_exists_index_builds;
  parent->prebuilt_exists_index_rows += worker.prebuilt_exists_index_rows;
  parent->prebuilt_exists_keys += worker.prebuilt_exists_keys;
  parent->correlated_index_probes += worker.correlated_index_probes;
  parent->correlated_result_cache_hits += worker.correlated_result_cache_hits;
  parent->uncorrelated_cache_hits += worker.uncorrelated_cache_hits;
  parent->uncorrelated_hash_builds += worker.uncorrelated_hash_builds;
  parent->uncorrelated_hash_probes += worker.uncorrelated_hash_probes;
  parent->exists_short_circuit_queries += worker.exists_short_circuit_queries;
  parent->scan_ms += worker.scan_ms;
  parent->filter_ms += worker.filter_ms;
  parent->join_ms += worker.join_ms;
  parent->project_ms += worker.project_ms;
  parent->sort_ms += worker.sort_ms;
  parent->boundary_ms += worker.boundary_ms;
  parent->build_input_ms += worker.build_input_ms;
  parent->window_ms += worker.window_ms;
  parent->window_layout_ms += worker.window_layout_ms;
  parent->deferred_materialize_ms += worker.deferred_materialize_ms;
  parent->window_input_rows += worker.window_input_rows;
  parent->rank_bounds += worker.rank_bounds;
  parent->topn_finish_uses += worker.topn_finish_uses;
  parent->topn_finish_capacity =
      std::max(parent->topn_finish_capacity, worker.topn_finish_capacity);
  parent->topn_finish_input_rows += worker.topn_finish_input_rows;
  parent->base_scan_cache_hits += worker.base_scan_cache_hits;
  parent->aggregate_input_rows += worker.aggregate_input_rows;
  parent->aggregate_groups += worker.aggregate_groups;
  parent->aggregate_updates += worker.aggregate_updates;
  parent->aggregate_ms += worker.aggregate_ms;
  parent->planning_ms += worker.planning_ms;
  parent->join_estimate_ms += worker.join_estimate_ms;
  parent->partitioned_aggregate_merges += worker.partitioned_aggregate_merges;
  parent->partitioned_numeric_aggregations +=
      worker.partitioned_numeric_aggregations;
  parent->dictionary_aggregations += worker.dictionary_aggregations;
  parent->dictionary_summary_aggregations +=
      worker.dictionary_summary_aggregations;
  parent->dictionary_aggregate_rows += worker.dictionary_aggregate_rows;
  parent->partitioned_dictionary_aggregations +=
      worker.partitioned_dictionary_aggregations;
  parent->typed_parallel_aggregations += worker.typed_parallel_aggregations;
  parent->grouped_join_fusions += worker.grouped_join_fusions;
  parent->pushed_group_rows += worker.pushed_group_rows;
  parent->pushed_group_joins += worker.pushed_group_joins;
  parent->transfer_seed_rows += worker.transfer_seed_rows;
  parent->transfer_scan_tested += worker.transfer_scan_tested;
  parent->transfer_scan_rejected += worker.transfer_scan_rejected;
  parent->streamed_grouped_joins += worker.streamed_grouped_joins;
  parent->selective_index_scans += worker.selective_index_scans;
  parent->selective_index_rows += worker.selective_index_rows;
  parent->scan_rows += worker.scan_rows;
  parent->scan_output_rows += worker.scan_output_rows;
  parent->scan_values_decoded += worker.scan_values_decoded;
  parent->scan_values_available += worker.scan_values_available;
  parent->relation_spills += worker.relation_spills;
  parent->inner_hash_build_rows += worker.inner_hash_build_rows;
  parent->inner_hash_index_estimate_bytes +=
      worker.inner_hash_index_estimate_bytes;
  parent->inner_hash_left_builds += worker.inner_hash_left_builds;
  parent->inner_hash_spilled_probe_rows += worker.inner_hash_spilled_probe_rows;
  parent->key_filter_scans += worker.key_filter_scans;
  parent->key_filter_keys += worker.key_filter_keys;
  parent->key_filter_rejected += worker.key_filter_rejected;
  parent->key_filter_null_rejected += worker.key_filter_null_rejected;
  parent->key_filter_minmax += worker.key_filter_minmax;
  parent->key_filter_bloom += worker.key_filter_bloom;
  parent->key_filter_bytes += worker.key_filter_bytes;
  parent->correlated_outer_serials += worker.correlated_outer_serials;
  parent->correlated_domain_filter_builds +=
      worker.correlated_domain_filter_builds;
  parent->empty_build_short_circuit |= worker.empty_build_short_circuit;
  parent->null_aware_anti_build_contains_null |=
      worker.null_aware_anti_build_contains_null;
}

}  // namespace

StatusOr<std::optional<std::vector<Relation>>> TryExecuteBranchesParallel(
    TransactionContext& context,
    std::span<const SelectStatement* const> branches, const CteMap& ctes,
    bool columnar_output) {
  static const bool parallel_branches_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_BRANCHES") !=
      nullptr;
  if (parallel_branches_disabled || branches.size() < 2) {
    return std::optional<std::vector<Relation>>{};
  }
  const size_t avail = ParallelWorkerCount(256);
  if (avail < 2) {
    return std::optional<std::vector<Relation>>{};
  }
  size_t non_trivial = 0;
  for (const SelectStatement* branch : branches) {
    if (branch == nullptr || StatementContainsVolatileFunction(*branch) ||
        BranchHasLargeDataScan(context, *branch, ctes)) {
      return std::optional<std::vector<Relation>>{};
    }
    if (BranchLooksNonTrivial(context, *branch, ctes)) {
      ++non_trivial;
    }
  }
  if (non_trivial < 2) {
    return std::optional<std::vector<Relation>>{};
  }

  const size_t count = branches.size();
  const size_t team_size = std::min(count, avail);
  ExecutionRuntime* const parent_runtime = context.execution_runtime();
  ExecutionRuntime* const saved_raw_runtime = context.raw_execution_runtime();
  std::vector<ExecutionRuntime> worker_runtimes(count);
  if (parent_runtime != nullptr) {
    if (!parent_runtime->shared_base_cache) {
      parent_runtime->shared_base_cache =
          std::make_shared<SharedBaseRelationCache>();
    }
    for (size_t i = 0; i < count; ++i) {
      InitWorkerRuntime(*parent_runtime, &worker_runtimes[i]);
      worker_runtimes[i].shared_base_cache = parent_runtime->shared_base_cache;
    }
  }
  std::vector<std::optional<Relation>> results(count);
  std::vector<Status> statuses(count, Status::kSuccess);
  std::atomic<size_t> next_branch{0};
  std::atomic<size_t> active_workers{team_size};

  // Clear parent runtime visibility while parallel branch teams run so any
  // nested ParallelPool task without a thread-local override cannot race on
  // parent ExecutionRuntime maps.
  struct ParentRuntimeReset {
    TransactionContext& ctx;
    ExecutionRuntime* saved_raw;
    ParentRuntimeReset(TransactionContext& c, ExecutionRuntime* s)
        : ctx(c), saved_raw(s) {
      ctx.set_execution_runtime(nullptr);
    }
    ParentRuntimeReset(const ParentRuntimeReset&) = delete;
    ParentRuntimeReset& operator=(const ParentRuntimeReset&) = delete;
    ParentRuntimeReset(ParentRuntimeReset&&) = delete;
    ParentRuntimeReset& operator=(ParentRuntimeReset&&) = delete;
    ~ParentRuntimeReset() { ctx.set_execution_runtime(saved_raw); }
  } reset_guard(context, saved_raw_runtime);
  TransactionContext::ScopedRuntimeOverride clear_caller_tls(context, nullptr);

  const auto run_worker = [&]() {
    ScopedQueryParallelismBudget budget(&active_workers, avail);
    for (;;) {
      const size_t b = next_branch.fetch_add(1, std::memory_order_relaxed);
      if (b >= count) {
        break;
      }
      try {
        TransactionContext::ScopedRuntimeOverride rt_guard(
            context, parent_runtime != nullptr ? &worker_runtimes[b] : nullptr);
        StatusOr<Relation> executed = ExecuteQuery(
            context, *branches[b], nullptr, ctes, nullptr, columnar_output);
        if (!executed.HasValue()) {
          statuses[b] = executed.GetStatus();
        } else {
          Relation rel = executed.MoveValue();
          rel.set_runtime(parent_runtime);
          results[b].emplace(std::move(rel));
        }
      } catch (const std::exception& error) {
        statuses[b] = StatusError(StatusCode::kRuntimeError, error.what());
      } catch (...) {
        statuses[b] =
            StatusError(StatusCode::kRuntimeError, "parallel branch failed");
      }
    }
    active_workers.fetch_sub(1, std::memory_order_acq_rel);
  };

  {
    std::vector<std::jthread> helpers;
    helpers.reserve(team_size - 1);
    for (size_t w = 1; w < team_size; ++w) {
      helpers.emplace_back(run_worker);
    }
    run_worker();
  }

  for (size_t b = 0; b < count; ++b) {
    if (!statuses[b].ok()) {
      return statuses[b];
    }
  }
  std::vector<Relation> output;
  output.reserve(count);
  for (size_t b = 0; b < count; ++b) {
    MergeWorkerRuntimeIntoParent(worker_runtimes[b], parent_runtime);
    if (!results[b].has_value()) {
      return StatusError(StatusCode::kRuntimeError,
                         "missing parallel branch output");
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes
    // success
    output.push_back(
        std::move(*results[b]));  // NOLINT(bugprone-unchecked-optional-access)
  }
  return std::optional<std::vector<Relation>>(std::move(output));
}

namespace {


template <typename ApplyPairFn>
StatusOr<Relation> FoldLowPrecedenceSetTerms(
    std::vector<Relation>& terms, std::span<const SetOperationKind> low_ops,
    ApplyPairFn&& apply_pair) {
  Relation combined = std::move(terms.front());
  size_t j = 1;
  while (j < terms.size()) {
    if (low_ops[j - 1] == SetOperationKind::kUnion && j + 1 < terms.size() &&
        low_ops[j] == SetOperationKind::kUnion) {
      size_t run_end = j + 1;
      while (run_end + 1 < terms.size() &&
             low_ops[run_end] == SetOperationKind::kUnion) {
        ++run_end;
      }
      std::vector<Relation*> batch;
      batch.reserve(1 + (run_end - j + 1));
      batch.push_back(&combined);
      for (size_t k = j; k <= run_end; ++k) {
        batch.push_back(&terms[k]);
      }
      if (TryColumnarMultiUnion(batch)) {
        j = run_end + 1;
        continue;
      }
    }
    ASSIGN_OR_RETURN(
        Relation, folded_pair,
        (apply_pair(std::move(combined), std::move(terms[j]), low_ops[j - 1])));
    combined = std::move(folded_pair);
    ++j;
  }
  return combined;
}

uint64_t EstimateBranchMaxScanRows(TransactionContext& context,
                                   const SelectStatement& stmt,
                                   const CteMap& ctes) {
  uint64_t best = 0;
  for (const SelectSource& source : stmt.Sources()) {
    if (source.query) {
      best = std::max(best,
                      EstimateBranchMaxScanRows(context, *source.query, ctes));
      continue;
    }
    if (const auto cte = ctes.find(source.table);
        cte != ctes.end() && cte->second != nullptr) {
      best = std::max<uint64_t>(best, cte->second->TotalRows());
      continue;
    }
    if (auto st = context.GetStats(source.table); st.HasValue()) {
      best = std::max<uint64_t>(best, st.Value()->Rows());
      continue;
    }
    if (auto tbl = context.GetTable(source.table); tbl.HasValue()) {
      best = std::max<uint64_t>(best, tbl.Value()->BackingColumnarRowCount());
    }
  }
  return best;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
// (prebuilt_exists_outer_domain_filter_sound)
std::shared_ptr<SelectStatement> TryRestrictGroupedCteByPeerEquijoinDomain(
    const SelectStatement& consumer, const std::string& name,
    const SelectStatement& active_body, const CteMap& ctes) {
  if (!consumer.UnionAll().empty() || consumer.GetSetOperationTree() ||
      consumer.Sources().size() < 2 ||
      StatementContainsVolatileFunction(consumer) ||
      active_body.GroupBy().empty() || active_body.IsGroupAll() ||
      active_body.HasLimit() || active_body.Offset() != 0 ||
      !active_body.UnionAll().empty() || active_body.GetSetOperationTree() ||
      HasWindowFunctions(active_body) ||
      StatementContainsVolatileFunction(active_body)) {
    return nullptr;
  }
  for (const auto& [other_name, unused] : consumer.WithQueries()) {
    (void)unused;
    if (other_name == name) {
      continue;
    }
    std::unordered_set<std::string> refs;
    CollectCteBodyOuterRefs(consumer, other_name, &refs);
    if (refs.contains(name)) {
      return nullptr;
    }
  }
  if (ContainsQuery(consumer.WhereClause()) ||
      ContainsQuery(consumer.Having()) || ContainsQuery(consumer.Qualify()) ||
      std::ranges::any_of(consumer.SelectList(), [](const auto& it) {
        return ContainsQuery(it.expression);
      })) {
    return nullptr;
  }
  std::optional<size_t> my_src_idx;
  for (size_t si = 0; si < consumer.Sources().size(); ++si) {
    const SelectSource& src = consumer.Sources()[si];
    const bool inner_like = (si == 0 || src.join_type == JoinType::kInner ||
                             src.join_type == JoinType::kCross) &&
                            !src.unnest && !src.sample && !src.is_lateral &&
                            !src.from_nested_join &&
                            src.using_columns.empty() &&
                            !ContainsQuery(src.join_condition);
    if (!inner_like) {
      return nullptr;
    }
    if (!src.query && IdentifierEquals(src.table, name)) {
      if (my_src_idx.has_value()) {
        return nullptr;
      }
      my_src_idx = si;
    }
  }
  if (!my_src_idx.has_value()) {
    return nullptr;
  }

  std::vector<Expression> all_conjuncts =
      SplitConjuncts(consumer.WhereClause());
  for (size_t si = 1; si < consumer.Sources().size(); ++si) {
    if (consumer.Sources()[si].join_condition) {
      for (Expression& c :
           SplitConjuncts(consumer.Sources()[si].join_condition)) {
        all_conjuncts.push_back(std::move(c));
      }
    }
  }

  auto resolve_consumer_source =
      [&](const ColumnName& col) -> std::optional<size_t> {
    if (!col.schema.empty()) {
      for (size_t si = 0; si < consumer.Sources().size(); ++si) {
        const auto& s = consumer.Sources()[si];
        const std::string al = s.alias.empty() ? s.table : s.alias;
        if (IdentifierEquals(col.schema, al)) {
          return si;
        }
      }
      return std::nullopt;
    }
    return std::nullopt;
  };

  const Relation* best_peer_rel = nullptr;
  size_t best_peer_slot = 0;
  size_t best_my_key_idx = 0;

  for (const Expression& conj : all_conjuncts) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const BinaryExpression& bin = conj->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kEquals || !bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    ColumnName lhs = bin.Left()->AsColumnValue().GetColumnName();
    ColumnName rhs = bin.Right()->AsColumnValue().GetColumnName();
    auto s_lhs = resolve_consumer_source(lhs);
    auto s_rhs = resolve_consumer_source(rhs);
    if (!s_lhs.has_value() || !s_rhs.has_value() || *s_lhs == *s_rhs) {
      continue;
    }
    size_t peer_si = 0;
    ColumnName my_col;
    ColumnName peer_col;
    if (*s_lhs == *my_src_idx) {
      peer_si = *s_rhs;
      my_col = lhs;
      peer_col = rhs;
    } else if (*s_rhs == *my_src_idx) {
      peer_si = *s_lhs;
      my_col = rhs;
      peer_col = lhs;
    } else {
      continue;
    }
    const SelectSource& peer_src = consumer.Sources()[peer_si];
    if (peer_src.query) {
      continue;
    }
    const auto peer_it = ctes.find(peer_src.table);
    if (peer_it == ctes.end() || peer_it->second == nullptr ||
        peer_it->second->HasSpill() || peer_it->second->TotalRows() > 4500) {
      continue;
    }
    const Relation& peer_rel = *peer_it->second;
    std::optional<size_t> peer_slot =
        LocalColumnOffset(peer_rel.schema, ColumnName("", peer_col.name));
    if (!peer_slot.has_value() || *peer_slot >= peer_rel.schema.ColumnCount()) {
      continue;
    }
    const Column& pcol_meta = peer_rel.schema.GetColumn(*peer_slot);
    if (pcol_meta.IsUnsigned() || pcol_meta.IsInterval() ||
        (pcol_meta.Type() != ValueType::kInt64 &&
         pcol_meta.Type() != ValueType::kDate &&
         pcol_meta.Type() != ValueType::kVarChar)) {
      continue;
    }
    std::optional<size_t> my_key_idx;
    for (size_t ki = 0; ki < active_body.SelectList().size(); ++ki) {
      if (IdentifierEquals(ProjectionName(active_body.SelectList()[ki], ki),
                           my_col.name)) {
        if (my_key_idx.has_value()) {
          my_key_idx.reset();
          break;
        }
        my_key_idx = ki;
      }
    }
    if (!my_key_idx.has_value()) {
      continue;
    }
    const Expression& key_expr =
        active_body.SelectList()[*my_key_idx].expression;
    if (!key_expr || key_expr->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const bool is_group_col =
        std::ranges::any_of(active_body.GroupBy(), [&](const Expression& g) {
          return g && g->Type() == TypeTag::kColumnValue &&
                 g->ToString() == key_expr->ToString();
        });
    if (!is_group_col) {
      continue;
    }
    if (best_peer_rel == nullptr ||
        peer_rel.TotalRows() < best_peer_rel->TotalRows()) {
      best_peer_rel = &peer_rel;
      best_peer_slot = *peer_slot;
      best_my_key_idx = *my_key_idx;
    }
  }

  if (best_peer_rel == nullptr) {
    return nullptr;
  }

  const ValueType pvt = best_peer_rel->schema.GetColumn(best_peer_slot).Type();
  std::vector<Expression> in_items;
  if (pvt == ValueType::kVarChar) {
    std::unordered_set<std::string> seen;
    seen.reserve(best_peer_rel->TotalRows());
    bool ok = true;
    auto collect_str = [&](std::string_view sv) {
      if (!PlainStringComparable(sv)) {
        ok = false;
        return;
      }
      if (seen.emplace(sv).second) {
        in_items.push_back(
            std::make_shared<ConstantValue>(Value(std::string(sv))));
      }
    };
    if (best_peer_rel->columnar != nullptr) {
      for (const ColumnarChunk& ch : best_peer_rel->columnar->chunks) {
        if (best_peer_slot >= ch.columns.size()) {
          ok = false;
          break;
        }
        const ColumnarColumn& cc = ch.columns[best_peer_slot];
        for (size_t r = 0; r < ch.rows && ok; ++r) {
          if (!cc.IsNull(r)) {
            collect_str(cc.StringAt(r));
          }
        }
      }
    } else {
      for (const Row& row : best_peer_rel->rows) {
        if (best_peer_slot < row.Size() && !row[best_peer_slot].IsNull()) {
          if (row[best_peer_slot].type != ValueType::kVarChar ||
              row[best_peer_slot].Collation() != 0) {
            ok = false;
            break;
          }
          collect_str(row[best_peer_slot].AsStringView());
        }
      }
    }
    if (!ok) {
      return nullptr;
    }
  } else {
    FlatIntKeyMap<uint8_t> seen;
    seen.Reserve(best_peer_rel->TotalRows());
    bool ok = true;
    auto collect_int = [&](int64_t iv) {
      bool emplaced = false;
      uint8_t* slot = seen.TryEmplace(iv, &emplaced);
      if (emplaced) {
        *slot = 1;
        Value v = (pvt == ValueType::kDate)
                      ? Value::DateFromDays(static_cast<int32_t>(iv))
                      : Value(iv);
        in_items.push_back(std::make_shared<ConstantValue>(std::move(v)));
      }
    };
    if (best_peer_rel->columnar != nullptr) {
      for (const ColumnarChunk& ch : best_peer_rel->columnar->chunks) {
        if (best_peer_slot >= ch.columns.size()) {
          ok = false;
          break;
        }
        const ColumnarColumn& cc = ch.columns[best_peer_slot];
        for (size_t r = 0; r < ch.rows && ok; ++r) {
          if (!cc.IsNull(r)) {
            collect_int(cc.IntAt(r));
          }
        }
      }
    } else {
      for (const Row& row : best_peer_rel->rows) {
        if (best_peer_slot < row.Size() && !row[best_peer_slot].IsNull()) {
          if (row[best_peer_slot].type != pvt ||
              row[best_peer_slot].IsUnsigned() ||
              row[best_peer_slot].Collation() != 0) {
            ok = false;
            break;
          }
          collect_int(row[best_peer_slot].value.int_value);
        }
      }
    }
    if (!ok) {
      return nullptr;
    }
  }

  const Expression& key_expr =
      active_body.SelectList()[best_my_key_idx].expression;
  Expression domain_pred;
  if (in_items.empty()) {
    domain_pred = std::make_shared<BinaryExpression>(
        std::make_shared<ConstantValue>(Value(int64_t{0})),
        BinaryOperation::kEquals,
        std::make_shared<ConstantValue>(Value(int64_t{1})));
  } else if (in_items.size() == 1) {
    domain_pred = std::make_shared<BinaryExpression>(
        key_expr, BinaryOperation::kEquals, std::move(in_items[0]));
  } else {
    domain_pred = std::make_shared<InExpression>(key_expr, std::move(in_items));
  }

  auto rewritten = std::make_shared<SelectStatement>(active_body);
  std::vector<Expression> where_conj = SplitConjuncts(rewritten->WhereClause());
  where_conj.push_back(std::move(domain_pred));
  rewritten->SetWhereClause(CombineConjuncts(where_conj));
  return rewritten;
}

}  // namespace

StatusOr<Relation> ExecuteQuery(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& inherited_ctes,
    GroupedOutputSink* output_sink, bool columnar_output) {
  EnsureReusableProjections(context, context.execution_runtime());
  if (outer == nullptr) {
    if (ExecutionRuntime* runtime = context.execution_runtime();
        runtime != nullptr && !runtime->shared_base_cache &&
        (!runtime->reusable_base_relations.empty() ||
         !statement.WithQueries().empty() || !statement.UnionAll().empty() ||
         statement.GetSetOperationTree() != nullptr)) {
      runtime->shared_base_cache = std::make_shared<SharedBaseRelationCache>();
    }
  }
  if (outer == nullptr && output_sink == nullptr) {
    ASSIGN_OR_RETURN(std::optional<Relation>, banded,
                     TryExecutePartitionBandedOrderByLimit(
                         context, statement, inherited_ctes, columnar_output));
    if (banded) {
      return std::move(*banded);
    }
    ASSIGN_OR_RETURN(std::optional<Relation>, lex_prefix,
                     TryExecuteLexicographicDimPrefixTopK(
                         context, statement, inherited_ctes, columnar_output));
    if (lex_prefix) {
      return std::move(*lex_prefix);
    }
  }
  // Apply relational boundary rewrites before loading sources.  Correlated
  // invocations must keep their original scope, while top-level derived
  // tables can be flattened or receive safe predicate/LIMIT pushdown.
  if (outer == nullptr) {
    const auto boundary_begin = std::chrono::steady_clock::now();
    std::shared_ptr<SelectStatement> optimized =
        OptimizeDerivedBoundaries(statement, inherited_ctes, &context);
    if (auto* runtime = context.execution_runtime()) {
      runtime->boundary_ms += ElapsedMs(boundary_begin);
    }
    if (optimized) {
      return ExecuteQuery(context, *optimized, outer, inherited_ctes,
                          output_sink, columnar_output);
    }
  }
  if (outer == nullptr && output_sink == nullptr) {
    ASSIGN_OR_RETURN(std::optional<Relation>, null_counts,
                     TryExecuteColumnarNullMaskScalarCounts(context, statement,
                                                            inherited_ctes));
    if (null_counts) {
      return std::move(*null_counts);
    }
    ASSIGN_OR_RETURN(std::optional<Relation>, multi_buckets,
                     TryExecuteMultiBucketScalarCrossJoin(context, statement,
                                                          inherited_ctes));
    if (multi_buckets) {
      return std::move(*multi_buckets);
    }
    ASSIGN_OR_RETURN(std::optional<Relation>, promo_ret_thresh,
                     TryExecutePromoReturnThresholdCounts(context, statement,
                                                          inherited_ctes));
    if (promo_ret_thresh) {
      return std::move(*promo_ret_thresh);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, mkt_share,
        TryExecuteMarketShareYearRoutes(context, statement, inherited_ctes));
    if (mkt_share) {
      return std::move(*mkt_share);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, ord_fetch,
        TryExecuteOrderedFetchTopN(context, statement, inherited_ctes));
    if (ord_fetch) {
      return std::move(*ord_fetch);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, surr_isect,
        TryExecuteSurrogateIntersectCount(context, statement, inherited_ctes));
    if (surr_isect) {
      return std::move(*surr_isect);
    }
    ASSIGN_OR_RETURN(std::optional<Relation>, two_pass_avg,
                     TryExecuteTwoPassGroupAvgThreshold(
                         context, statement, inherited_ctes, columnar_output));
    if (two_pass_avg) {
      return std::move(*two_pass_avg);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, dir_buckets,
        TryExecuteDirectionBuckets(context, statement, inherited_ctes));
    if (dir_buckets) {
      return std::move(*dir_buckets);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, retire_exists,
        TryExecuteRetireWitnessExists(context, statement, inherited_ctes));
    if (retire_exists) {
      return std::move(*retire_exists);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, rebucket_hist,
        TryExecuteRebucketLeftJoinHist(context, statement, inherited_ctes));
    if (rebucket_hist) {
      return std::move(*rebucket_hist);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, rev_runs,
        TryExecuteContiguousRevenueRuns(context, statement, inherited_ctes));
    if (rev_runs) {
      return std::move(*rev_runs);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, radix_cnt,
        TryExecuteRadixPacked2x32JoinCount(context, statement, inherited_ctes));
    if (radix_cnt) {
      return std::move(*radix_cnt);
    }
    ASSIGN_OR_RETURN(std::optional<Relation>, posting_band_agg,
                     TryExecutePostingBandJoinScalarAgg(context, statement,
                                                        inherited_ctes));
    if (posting_band_agg) {
      return std::move(*posting_band_agg);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, pushed,
        TryPushGroupedCteJoins(context, statement, inherited_ctes));
    if (pushed) {
      return std::move(*pushed);
    }
    ASSIGN_OR_RETURN(std::optional<Relation>, qcount,
                     TryExecuteQuotientGidDistinctSetOpCount(context, statement,
                                                             inherited_ctes));
    if (qcount) {
      return std::move(*qcount);
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
      std::vector<std::string> ready_names;
      for (const std::string& name : pending) {
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
          ready_names.push_back(name);
        }
      }
      if (ready_names.empty()) {
        // Every remaining entry waits on another remaining entry, so the WITH
        // list is a mutual reference with no dependency order.  Reporting it
        // beats the "table not found" that executing the entries in map order
        // used to produce.  The SQL frontend rejects the same shape before
        // planning, so this is the backstop for statements handed straight to
        // the executor.
        return CteCycleStatus({pending.begin(), pending.end()});
      }
      std::ranges::sort(ready_names);

      struct PreparedCte {
        std::string name;
        const SelectStatement* body{nullptr};
        std::shared_ptr<SelectStatement> restricted;
      };
      std::vector<PreparedCte> parallel_batch;
      std::vector<std::string> serial_batch;
      for (const std::string& name : ready_names) {
        if (outer != nullptr || statement.IsRecursiveWith(name)) {
          serial_batch.push_back(name);
          continue;
        }
        const SelectStatement& query = *statement.WithQueries().at(name);
        auto restricted = PushSingleUseCtePredicates(context, statement, name);
        if (!restricted) {
          restricted = PushSharedGroupedCtePredicates(context, statement, name);
        }
        if (!restricted) {
          restricted =
              PushGroupedCteJoinDomains(context, statement, name, ctes);
        }
        if (!restricted) {
          restricted = GroupPresenceOnlyCte(statement, name, &context);
        }
        if (restricted && context.execution_runtime() != nullptr) {
          context.execution_runtime()->retained_statements.push_back(
              restricted);
        }
        parallel_batch.push_back(
            PreparedCte{.name = name,
                        .body = restricted ? restricted.get() : &query,
                        .restricted = std::move(restricted)});
      }

      if (parallel_batch.size() >= 2) {
        std::vector<const SelectStatement*> branch_ptrs;
        branch_ptrs.reserve(parallel_batch.size());
        for (const auto& item : parallel_batch) {
          branch_ptrs.push_back(item.body);
        }
        ASSIGN_OR_RETURN(
            std::optional<std::vector<Relation>>, executed,
            (TryExecuteBranchesParallel(context, branch_ptrs, ctes, true)));
        if (executed.has_value()) {
          for (size_t i = 0; i < parallel_batch.size(); ++i) {
            ctes[parallel_batch[i].name] =
                std::make_shared<Relation>(std::move((*executed)[i]));
            pending.erase(parallel_batch[i].name);
          }
          parallel_batch.clear();
        }
      }
      if (parallel_batch.size() >= 2) {
        std::ranges::stable_sort(
            parallel_batch, [&](const PreparedCte& a, const PreparedCte& b) {
              return EstimateBranchMaxScanRows(context, *a.body, ctes) <
                     EstimateBranchMaxScanRows(context, *b.body, ctes);
            });
      }
      for (size_t bi = 0; bi < parallel_batch.size(); ++bi) {
        auto& item = parallel_batch[bi];
        if (bi > 0 && outer == nullptr) {
          if (!item.restricted) {
            item.restricted =
                PushGroupedCteJoinDomains(context, statement, item.name, ctes);
            if (item.restricted) {
              item.body = item.restricted.get();
              if (context.execution_runtime() != nullptr) {
                context.execution_runtime()->retained_statements.push_back(
                    item.restricted);
              }
            }
          }
          if (auto peer_restricted = TryRestrictGroupedCteByPeerEquijoinDomain(
                  statement, item.name, *item.body, ctes)) {
            item.restricted = std::move(peer_restricted);
            item.body = item.restricted.get();
            if (context.execution_runtime() != nullptr) {
              context.execution_runtime()->retained_statements.push_back(
                  item.restricted);
            }
          }
        }
        ASSIGN_OR_RETURN(
            Relation, cte_relation,
            (ExecuteQuery(context, *item.body, outer, ctes, nullptr, true)));
        ctes[item.name] = std::make_shared<Relation>(std::move(cte_relation));
        pending.erase(item.name);
      }
      for (const std::string& name : serial_batch) {
        const SelectStatement& query = *statement.WithQueries().at(name);
        const bool recursive = statement.IsRecursiveWith(name);
        auto restricted =
            outer == nullptr && !recursive
                ? PushSingleUseCtePredicates(context, statement, name)
                : nullptr;
        if (!restricted && outer == nullptr && !recursive) {
          restricted = PushSharedGroupedCtePredicates(context, statement, name);
        }
        if (!restricted && outer == nullptr && !recursive) {
          restricted =
              PushGroupedCteJoinDomains(context, statement, name, ctes);
        }
        if (!restricted && outer == nullptr && !recursive) {
          restricted = GroupPresenceOnlyCte(statement, name, &context);
        }
        if (restricted && context.execution_runtime() != nullptr) {
          context.execution_runtime()->retained_statements.push_back(
              restricted);
        }
        StatusOr<Relation> cte_relation =
            recursive ? ExecuteRecursiveCte(context, name, query, outer, ctes,
                                            statement.RecursiveDepthOf(name))
                      : ExecuteQuery(context, restricted ? *restricted : query,
                                     outer, ctes, nullptr, true);
        if (!cte_relation.HasValue()) {
          return cte_relation.GetStatus();
        }
        ctes[name] = std::make_shared<Relation>(cte_relation.MoveValue());
        pending.erase(name);
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
      if (operation == SetOperationKind::kUnionAll &&
          TryConcatenateColumnarUnionAll(left, right)) {
        return left;
      }
      if (operation != SetOperationKind::kUnionAll &&
          TryColumnarSetOperation(left, right, operation)) {
        return left;
      }
      RETURN_IF_FAIL(left.EnsureRowsMaterialized());
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
      if ((operation == SetOperationKind::kUnionAll ||
           operation == SetOperationKind::kUnion) &&
          NormalizeMovableSetInputs(left, right)) {
        auto charge = operation == SetOperationKind::kUnion
                          ? ResidentSetCharge(left, right)
                          : std::nullopt;
        if (operation == SetOperationKind::kUnionAll || charge) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          left.MergeInMemoryShard(std::move(right));
          right = Relation(context.execution_runtime());
          if (operation == SetOperationKind::kUnion && !left.HasSpill()) {
            RETURN_IF_FAIL(DistinctResidentRows(left));
          }
          RETURN_IF_FAIL(left.FinishSpill());
          RETURN_IF_FAIL(left.GetStatus());
          if (operation == SetOperationKind::kUnionAll || !left.HasSpill()) {
            return left;
          }
          // A pending memory charge can trigger spilling during the merge.
          // The general UNION below deduplicates this concatenation plus an
          // empty right input, preserving its spill-capable implementation.
        }
      }
      if (operation != SetOperationKind::kUnionAll &&
          operation != SetOperationKind::kUnion &&
          NormalizeMovableSetInputs(left, right)) {
        if (auto charge = ResidentSetCharge(left, right)) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          RETURN_IF_FAIL(ReduceResidentRows(left, right, operation));
          return left;
        }
      }
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

    // When every set operator is DISTINCT, each operand contributes presence
    // only. Group projected join keys before materializing repeated matches.
    // Any ALL operator keeps the original multiplicities of every input.
    const bool distinct_inputs =
        !tree.kinds.empty() && tree.kinds.size() == tree.branches.size() &&
        std::ranges::all_of(tree.kinds, IsPresenceOnlySetOperation);
    std::deque<SelectStatement> grouped_owned;
    std::vector<const SelectStatement*> term_ptrs;
    term_ptrs.reserve(1 + tree.branches.size());
    auto prepare_term = [&](const SelectStatement& stmt) {
      if (distinct_inputs) {
        if (auto grouped = GroupDistinctSetInput(stmt)) {
          grouped_owned.push_back(std::move(*grouped));
          term_ptrs.push_back(&grouped_owned.back());
          return;
        }
      }
      term_ptrs.push_back(&stmt);
    };
    prepare_term(*tree.first);
    for (const auto& branch_ptr : tree.branches) {
      prepare_term(*branch_ptr);
    }

    const bool branch_columnar_tree = true;
    std::optional<std::vector<Relation>> parallel_terms;
    if (outer == nullptr && term_ptrs.size() >= 2) {
      ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, executed,
                       (TryExecuteBranchesParallel(context, term_ptrs, ctes,
                                                   branch_columnar_tree)));
      parallel_terms = std::move(executed);
    }

    auto take_term = [&](size_t idx) -> StatusOr<Relation> {
      if (parallel_terms.has_value()) {
        return std::move((*parallel_terms)[idx]);
      }
      return ExecuteQuery(context, *term_ptrs[idx], outer, ctes, nullptr,
                          branch_columnar_tree);
    };
    ASSIGN_OR_RETURN(Relation, combined, take_term(0));
    // PRODUCTION FIX: honor per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold evaluated `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and dropped the INTERSECT entirely.
    std::vector<Relation> terms;
    terms.push_back(std::move(combined));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < tree.branches.size(); ++i) {
      ASSIGN_OR_RETURN(Relation, branch, take_term(i + 1));
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
    ASSIGN_OR_RETURN(Relation, combined_folded,
                     (FoldLowPrecedenceSetTerms(terms, low_ops, apply_pair)));
    combined = std::move(combined_folded);
    RETURN_IF_FAIL(combined.FinishSpill());
    RETURN_IF_FAIL(combined.GetStatus());
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &combined, outer, ctes));
    }
    ASSIGN_OR_RETURN(
        Relation, limited,
        (LimitedRows(statement, std::move(combined), &context, outer, &ctes)));
    if (!columnar_output && limited.HasColumnarOnly()) {
      RETURN_IF_FAIL(limited.EnsureRowsMaterialized());
    }
    return limited;
  }

  if (!statement.UnionAll().empty()) {
    SelectStatement head = statement;
    head.ClearUnionAll();
    // The flat representation already owns the operands below. Its head is
    // now a single SELECT; retaining the parser's ungrouped tree prevents
    // the same DISTINCT-input grouping used for all following operands.
    head.SetSetOperationTree(nullptr);
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    // The WITH bodies already executed into `ctes` above; clearing the head's
    // own copies keeps the fold from re-running every CTE body one extra time.
    head.ClearWithQueries();
    // Expanded grouping sets have identical FROM/WHERE inputs. Build that
    // input once, then aggregate each set over a shared immutable relation.
    // Compare complete normalized statements so joins and filters cannot be
    // accidentally shared across distinct UNION branches.
    std::optional<Relation> shared_input;
    bool shared_where_applied = false;
    auto input_shape = [](const SelectStatement& part) {
      SelectStatement shape(part);
      shape.SetSelectList({});
      shape.SetGroupBy({});
      shape.SetGroupAll(false);
      shape.SetHaving(nullptr);
      shape.SetOrderBy({});
      shape.SetLimit(std::nullopt);
      shape.SetOffset(0);
      shape.SetWithTies(false);
      shape.ClearUnionAll();
      shape.SetSetOperationTree(nullptr);
      shape.ClearWithQueries();
      return std::string(shape.Fingerprint());
    };
    auto shareable = [&](const SelectStatement& part) {
      // A ROLLUP / GROUPING SETS expansion includes a grand-total branch,
      // which has no GROUP BY keys but is still produced from the same input.
      // It is a legitimate coarsest level of the state-reuse chain
      // (PlanExactIntegerGroupingSets handles IsGroupAll sets explicitly).
      // Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
      //   (rollup_two_level_state_combine_sound,
      //    rollup_three_level_state_combine_sound,
      //    rollup_grand_total_requires_coalesce)
      return (!part.GroupBy().empty() || part.IsGroupAll()) &&
             !part.Distinct() && !part.HasDistinctOn() && !part.Having() &&
             !part.Qualify() && !StatementContainsVolatileFunction(part) &&
             ContainsOnlyUncorrelatedQueries(context, part.WhereClause(),
                                             ctes) &&
             std::ranges::none_of(part.SelectList(),
                                  [](const auto& item) {
                                    return ContainsQuery(item.expression);
                                  }) &&
             std::ranges::all_of(part.Sources(), [&](const auto& source) {
               return !source.unnest && !source.sample && !source.is_lateral &&
                      !source.from_nested_join &&
                      source.using_columns.empty() &&
                      !ContainsQuery(source.join_condition);
             });
    };
    bool share =
        outer == nullptr && statement.UnionAll().size() >= 2 && shareable(head);
    const std::string shape = input_shape(head);
    std::vector<NamedExpression> needed = head.SelectList();
    for (size_t i = 0; share && i < statement.UnionAll().size(); ++i) {
      const auto& part = *statement.UnionAll()[i];
      const auto match = statement.MatchFor(i);
      share =
          shareable(part) && !part.HasLimit() && part.Offset() == 0 &&
          part.OrderBy().empty() && part.WithQueries().empty() &&
          part.UnionAll().empty() && !match.by_name && !match.corresponding &&
          match.columns.empty() &&
          (i >= statement.SetOperationKinds().size() ||
           statement.SetOperationKinds()[i] == SetOperationKind::kUnionAll) &&
          input_shape(part) == shape;
      needed.insert(needed.end(), part.SelectList().begin(),
                    part.SelectList().end());
      for (const auto& key : part.GroupBy()) {
        needed.emplace_back("", key);
      }
    }
    std::vector<SelectStatement> exact_sets;
    bool surrogate_pre_agg_used = false;
    if (share) {
      SelectStatement scan(head);
      scan.SetSelectList(std::move(needed));
      std::vector<const SelectStatement*> sets{&head};
      for (const auto& branch : statement.UnionAll()) {
        sets.push_back(branch.get());
      }
      auto partial = PlanExactIntegerGroupingSets(sets);
      std::optional<Relation> surrogate_input;
      if (partial) {
        ASSIGN_OR_RETURN(std::optional<Relation>, maybe_surrogate,
                         (TrySurrogatePreAggExactGroupingInput(
                             context, head, *partial, ctes)));
        if (maybe_surrogate.has_value()) {
          surrogate_input.emplace(std::move(*maybe_surrogate));
          shared_where_applied = true;
          exact_sets = std::move(partial->sets);
          surrogate_pre_agg_used = true;
        }
      }
      if (surrogate_input.has_value()) {
        RETURN_IF_FAIL(surrogate_input->FinishSpill());
        RETURN_IF_FAIL(surrogate_input->GetStatus());
        shared_input.emplace(std::move(*surrogate_input));
      } else {
        GroupedJoinFusionSpec partial_fusion;
        if (partial) {
          partial_fusion = BuildGroupedJoinFusionSpec(partial->partial);
          partial_fusion.require_exact_integer_totals =
              !partial->allow_float_sums;
          const auto* runtime = context.execution_runtime();
          partial_fusion.columnar_output =
              runtime != nullptr && runtime->grouping_rank_limit.has_value();
        }
        ASSIGN_OR_RETURN(
            Relation, input,
            (BuildInput(context, partial ? partial->partial : scan, nullptr,
                        ctes, &shared_where_applied,
                        partial ? &partial_fusion : nullptr,
                        /*ignore_projection_star=*/false,
                        /*columnar_output=*/!partial.has_value())));
        if (partial && partial_fusion.fired) {
          std::vector<Column> columns;
          for (size_t i = 0; i < input.schema.ColumnCount(); ++i) {
            columns.push_back(input.schema.GetColumn(i));
            if (i < head.GroupBy().size()) {
              columns.back().Name() =
                  head.GroupBy()[i]->AsColumnValue().GetColumnName();
            }
          }
          input.schema = Schema("", std::move(columns));
          exact_sets = std::move(partial->sets);
        }
        if (!shared_where_applied && scan.WhereClause()) {
          RETURN_IF_FAIL(FilterRelation(context, &input,
                                        SplitConjuncts(scan.WhereClause()),
                                        nullptr, ctes));
          shared_where_applied = true;
        }
        RETURN_IF_FAIL(input.FinishSpill());
        RETURN_IF_FAIL(input.GetStatus());
        shared_input.emplace(std::move(input));
      }
    }
    std::optional<std::vector<Relation>> grouped_sets;
    size_t grouped_set_index = 0;
    // An empty shared input still owes the grand-total row of every
    // keyless set (GROUPING SETS ((a),()) over no rows -> one NULL total);
    // the compacted path builds groups from input rows only, so leave empty
    // inputs to the per-set evaluation below, which aggregates () correctly.
    if (shared_input && shared_where_applied && shared_input->TotalRows() > 0) {
      std::vector<const SelectStatement*> sets{&head};
      for (const auto& branch : statement.UnionAll()) {
        sets.push_back(branch.get());
      }
      if (!exact_sets.empty()) {
        sets.clear();
        for (const auto& set : exact_sets) {
          sets.push_back(&set);
        }
      }
      ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, computed,
                       (TryCompactGroupingSets(
                           context, sets, *shared_input,
                           !exact_sets.empty() && !surrogate_pre_agg_used)));
      grouped_sets = std::move(computed);
      if (!grouped_sets && shared_input->HasColumnarOnly()) {
        RETURN_IF_FAIL(shared_input->EnsureRowsMaterialized());
      }
    }
    bool normalized_grouped_sets = false;
    if (grouped_sets) {
      // Every term here is UNION ALL over the same grouping input. Resolve
      // common numeric types once, instead of rescanning the growing prefix
      // after every ROLLUP level. All rows are still inspected before moving.
      std::vector<Relation*> terms;
      terms.reserve(grouped_sets->size());
      for (auto& relation : *grouped_sets) {
        RETURN_IF_FAIL(relation.EnsureRowsMaterialized());
        terms.push_back(&relation);
      }
      normalized_grouped_sets = NormalizeMovableSetRelations(terms);
    }
    const bool distinct_inputs =
        statement.SetOperationKinds().size() == statement.UnionAll().size() &&
        std::ranges::all_of(statement.SetOperationKinds(),
                            IsPresenceOnlySetOperation);
    // The fast path reorders branches and may evaluate a branch more than
    // once (once to build the semi-filter seed, once for the real INTERSECT).
    // Sampling and volatile functions are not repeatable, so such a query
    // must take the ordinary single-evaluation set-operation path; the
    // helper also covers TABLESAMPLE nested in sources/subqueries.
    const auto branches_are_repeatable = [&]() -> bool {
      if (StatementContainsVolatileFunction(head)) {
        return false;
      }
      return std::ranges::all_of(statement.UnionAll(), [](const auto& branch) {
        return branch != nullptr && !StatementContainsVolatileFunction(*branch);
      });
    };
    const bool all_intersect_distinct =
        !shared_input && outer == nullptr && !statement.UnionAll().empty() &&
        statement.SetOperationKinds().size() == statement.UnionAll().size() &&
        std::ranges::all_of(statement.SetOperationKinds(),
                            [](SetOperationKind k) {
                              return k == SetOperationKind::kIntersect;
                            }) &&
        branches_are_repeatable();
    size_t shared_term_index = 0;
    std::deque<SelectStatement> union_grouped_owned;
    std::vector<const SelectStatement*> unshared_ptrs;
    std::optional<std::vector<Relation>> union_parallel_terms;
    const bool branch_columnar_flat = true;
    if (!shared_input && outer == nullptr && !statement.UnionAll().empty()) {
      unshared_ptrs.reserve(1 + statement.UnionAll().size());
      auto prepare_union_term = [&](const SelectStatement& part) {
        if (distinct_inputs) {
          if (auto grouped = GroupDistinctSetInput(part)) {
            union_grouped_owned.push_back(std::move(*grouped));
            unshared_ptrs.push_back(&union_grouped_owned.back());
            return;
          }
        }
        unshared_ptrs.push_back(&part);
      };
      prepare_union_term(head);
      for (const auto& union_stmt : statement.UnionAll()) {
        prepare_union_term(*union_stmt);
      }
      if (!all_intersect_distinct) {
        ASSIGN_OR_RETURN(
            std::optional<std::vector<Relation>>, executed,
            (TryExecuteBranchesParallel(context, unshared_ptrs, ctes,
                                        branch_columnar_flat)));
        union_parallel_terms = std::move(executed);
      }
    }

    size_t union_parallel_index = 0;
    auto run_shared_term =
        [&](const SelectStatement& original_part) -> StatusOr<Relation> {
      const size_t term_idx = union_parallel_index++;
      if (union_parallel_terms.has_value()) {
        return std::move((*union_parallel_terms)[term_idx]);
      }
      const SelectStatement& part =
          exact_sets.empty() ? original_part : exact_sets[shared_term_index++];
      if (grouped_sets) {
        return std::move((*grouped_sets)[grouped_set_index++]);
      }
      if (!shared_input) {
        if (!unshared_ptrs.empty() && term_idx < unshared_ptrs.size()) {
          return ExecuteQuery(context, *unshared_ptrs[term_idx], outer, ctes,
                              nullptr, branch_columnar_flat);
        }
        if (distinct_inputs) {
          if (auto grouped = GroupDistinctSetInput(part)) {
            return ExecuteQuery(context, *grouped, outer, ctes, nullptr,
                                branch_columnar_flat);
          }
        }
        return ExecuteQuery(context, part, outer, ctes, nullptr,
                            branch_columnar_flat);
      }
      auto resolved = ResolveGroupingAliases(part, shared_input->schema);
      const auto& effective = resolved ? *resolved : part;
      if (shared_where_applied) {
        std::vector<const AggregateExpression*> aggregates;
        std::unordered_set<const AggregateExpression*> seen;
        for (const auto& item : effective.SelectList()) {
          CollectAggregates(item.expression, &aggregates, &seen);
        }
        ASSIGN_OR_RETURN(std::optional<Relation>, compact,
                         (TryCompactRelationAggregate(
                             context, effective, *shared_input, aggregates)));
        if (compact) {
          return std::move(*compact);
        }
      }
      ASSIGN_OR_RETURN(Relation, input, MaterializeRelation(*shared_input));
      if (HasWindowFunctions(effective)) {
        return ExecuteWindowOverAggregate(context, effective, std::move(input),
                                          nullptr, ctes, !shared_where_applied);
      }
      return FinishQuery(context, effective, std::move(input), nullptr, ctes,
                         !shared_where_applied, 0);
    };
    // PRODUCTION FIX: apply per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold computed `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and returned the wrong rows.
    auto apply_pair = [&](Relation left, Relation right,
                          SetOperationKind operation) -> StatusOr<Relation> {
      if (operation == SetOperationKind::kUnionAll &&
          TryConcatenateColumnarUnionAll(left, right)) {
        return left;
      }
      if (operation != SetOperationKind::kUnionAll &&
          TryColumnarSetOperation(left, right, operation)) {
        return left;
      }
      RETURN_IF_FAIL(left.EnsureRowsMaterialized());
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
      if ((operation == SetOperationKind::kUnionAll ||
           operation == SetOperationKind::kUnion) &&
          ((normalized_grouped_sets && !left.HasSpill() && !right.HasSpill()) ||
           NormalizeMovableSetInputs(left, right))) {
        auto charge = operation == SetOperationKind::kUnion
                          ? ResidentSetCharge(left, right)
                          : std::nullopt;
        if (operation == SetOperationKind::kUnionAll || charge) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          left.MergeInMemoryShard(std::move(right));
          right = Relation(context.execution_runtime());
          if (operation == SetOperationKind::kUnion && !left.HasSpill()) {
            RETURN_IF_FAIL(DistinctResidentRows(left));
          }
          RETURN_IF_FAIL(left.FinishSpill());
          RETURN_IF_FAIL(left.GetStatus());
          if (operation == SetOperationKind::kUnionAll || !left.HasSpill()) {
            return left;
          }
          // A pending memory charge can trigger spilling during the merge.
          // The general UNION below deduplicates this concatenation plus an
          // empty right input, preserving its spill-capable implementation.
        }
      }
      if (operation != SetOperationKind::kUnionAll &&
          operation != SetOperationKind::kUnion &&
          NormalizeMovableSetInputs(left, right)) {
        if (auto charge = ResidentSetCharge(left, right)) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          RETURN_IF_FAIL(ReduceResidentRows(left, right, operation));
          return left;
        }
      }
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
    Relation combined(context.execution_runtime());
    bool handled_intersect_fast = false;
    if (all_intersect_distinct && unshared_ptrs.size() >= 2 &&
        context.execution_runtime() != nullptr) {
      // INTERSECT DISTINCT is commutative & associative (Lean proof:
      // lean/TinylambOpt/TpchSf10VectorizedExec.lean
      // intersect_dim_key_semifilter_sound). Evaluate the smallest-fact branch
      // first, then project its surviving dimension attribute tuples back to
      // matching dimension PKs as an exact runtime filter for each subsequent
      // branch's fact scan.
      std::function<size_t(const SelectStatement&)> max_base_rows =
          [&](const SelectStatement& s) -> size_t {
        size_t best = 0;
        for (const auto& src : s.Sources()) {
          if (src.query) {
            best = std::max(best, max_base_rows(*src.query));
          } else if (!src.unnest && !src.sample && !ctes.contains(src.table)) {
            if (auto st = context.GetStats(src.table); st.HasValue()) {
              best = std::max(best, st.Value()->Rows());
            }
          }
        }
        return best;
      };
      std::vector<size_t> order(unshared_ptrs.size());
      for (size_t idx = 0; idx < order.size(); ++idx) {
        order[idx] = idx;
      }
      std::vector<size_t> branch_sizes(unshared_ptrs.size(), 0);
      for (size_t idx = 0; idx < unshared_ptrs.size(); ++idx) {
        branch_sizes[idx] = max_base_rows(*unshared_ptrs[idx]);
      }
      std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return branch_sizes[a] < branch_sizes[b];
      });
      auto try_build_semi_rf = [&](const Relation& seed,
                                   const SelectStatement& next_stmt)
          -> std::optional<ExecutionRuntime::ScopedExtraTableRf> {
        if (!seed.HasColumnarOnly() || seed.columnar == nullptr ||
            seed.TotalRows() == 0 || seed.TotalRows() > 250000 ||
            next_stmt.Sources().size() < 2 || !next_stmt.WhereClause()) {
          return std::nullopt;
        }
        // A star can contribute several output slots. In that case the
        // select-list index cannot be used to address the seed columns.
        if (std::ranges::any_of(next_stmt.SelectList(), [](const auto& item) {
              return item.expression &&
                     item.expression->Type() == TypeTag::kColumnValue &&
                     item.expression->AsColumnValue().GetColumnName().name ==
                         "*";
            })) {
          return std::nullopt;
        }
        // An outer join can emit preserved-side rows whose NULL-extended
        // attributes still satisfy the INTERSECT seed (NULLs hash to a
        // distinct tag), so filtering the fact scan could drop rows that
        // must survive. Only inner/cross joins keep every emitted row
        // determined by the fact key.
        for (const auto& s : next_stmt.Sources()) {
          if (s.join_type != JoinType::kInner &&
              s.join_type != JoinType::kCross) {
            return std::nullopt;
          }
        }
        // Locate a dimension base source D whose attributes appear in
        // next_stmt.SelectList(), and a large fact table F joined to D.pk
        // either directly or through a 1-level derived table (e.g.
        // __tpcds_sk_pre).
        const auto& sources = next_stmt.Sources();
        for (size_t di = 0; di < sources.size(); ++di) {
          const auto& dsrc = sources[di];
          if (dsrc.query || dsrc.unnest || dsrc.sample ||
              ctes.contains(dsrc.table)) {
            continue;
          }
          auto dstats = context.GetStats(dsrc.table);
          auto dtbl = context.GetTable(dsrc.table);
          if (!dstats.HasValue() || !dtbl.HasValue() ||
              dstats.Value()->Rows() == 0 || dstats.Value()->Rows() > 2000000) {
            continue;
          }
          const Schema& dschema = dtbl.Value()->GetSchema();
          const std::string dqual =
              dsrc.alias.empty() ? dsrc.table : dsrc.alias;
          const Schema qdschema = QualifySchema(dschema, dqual);

          // Collect (out_idx, dim_col_slot) pairs projected from D.
          std::vector<std::pair<size_t, slot_t>> dim_out_pairs;
          for (size_t oi = 0; oi < next_stmt.SelectList().size(); ++oi) {
            const Expression& ex = next_stmt.SelectList()[oi].expression;
            if (!ex || ex->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const auto off = LocalColumnOffset(
                qdschema, ex->AsColumnValue().GetColumnName());
            if (!off.has_value()) {
              continue;
            }
            const ValueType vt =
                dschema.GetColumn(static_cast<size_t>(*off)).Type();
            // The semi-filter hashes raw columnar cells with std::hash.
            // A VARCHAR cell does not carry its Value collation tag, so a
            // case-insensitive 'ABC' and 'abc' would hash differently and
            // the filter would reject a row that INTERSECT considers equal.
            // Keep the fast path to collation-free scalar keys only.
            if (vt != ValueType::kInt64 && vt != ValueType::kDate) {
              dim_out_pairs.clear();
              break;
            }
            if (oi >= seed.schema.ColumnCount() ||
                seed.schema.GetColumn(oi).Type() != vt) {
              dim_out_pairs.clear();
              break;
            }
            dim_out_pairs.emplace_back(oi, *off);
          }
          if (dim_out_pairs.empty()) {
            continue;
          }
          // Find equality predicate D.pk = other.fk.
          std::string fact_table;
          std::string fact_fk_col;
          slot_t dim_pk_slot = 0;
          bool found_join = false;
          for (const Expression& conj :
               SplitConjuncts(next_stmt.WhereClause())) {
            if (!conj || conj->Type() != TypeTag::kBinaryExp) {
              continue;
            }
            const auto& bin = conj->AsBinaryExpression();
            if (bin.Op() != BinaryOperation::kEquals || !bin.Left() ||
                !bin.Right() || bin.Left()->Type() != TypeTag::kColumnValue ||
                bin.Right()->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName& ln = bin.Left()->AsColumnValue().GetColumnName();
            const ColumnName& rn = bin.Right()->AsColumnValue().GetColumnName();
            const auto loff = LocalColumnOffset(qdschema, ln);
            const auto roff = LocalColumnOffset(qdschema, rn);
            if (loff.has_value() == roff.has_value()) {
              continue;
            }
            const slot_t dslot = loff.has_value() ? *loff : *roff;
            const ColumnName& other_cn = loff.has_value() ? rn : ln;
            const Column& pk_col =
                dschema.GetColumn(static_cast<size_t>(dslot));
            if (pk_col.IsUnsigned() || (pk_col.Type() != ValueType::kInt64 &&
                                        pk_col.Type() != ValueType::kDate)) {
              continue;
            }
            // Resolve other_cn against another source in next_stmt.
            for (size_t fi = 0; fi < sources.size(); ++fi) {
              if (fi == di) {
                continue;
              }
              const auto& fsrc = sources[fi];
              const std::string fqual =
                  fsrc.alias.empty() ? fsrc.table : fsrc.alias;
              if (!other_cn.schema.empty() && other_cn.schema != fqual) {
                continue;
              }
              if (!fsrc.query && !fsrc.unnest && !fsrc.sample &&
                  !ctes.contains(fsrc.table)) {
                auto fstats = context.GetStats(fsrc.table);
                auto ftbl = context.GetTable(fsrc.table);
                if (fstats.HasValue() && ftbl.HasValue() &&
                    fstats.Value()->Rows() >= 1000000) {
                  if (LocalColumnOffset(ftbl.Value()->GetSchema(),
                                        ColumnName("", other_cn.name))
                          .has_value()) {
                    fact_table = fsrc.table;
                    fact_fk_col = other_cn.name;
                    dim_pk_slot = dslot;
                    found_join = true;
                    break;
                  }
                }
              } else if (fsrc.query) {
                const SelectStatement& sub = *fsrc.query;
                // Same outer-join hazard one level down: the runtime
                // filter lands on isrc's base-table scan inside sub.
                bool sub_outer = false;
                for (const auto& ss : sub.Sources()) {
                  if (ss.join_type != JoinType::kInner &&
                      ss.join_type != JoinType::kCross) {
                    sub_outer = true;
                    break;
                  }
                }
                if (sub_outer) {
                  continue;
                }
                for (const auto& sitem : sub.SelectList()) {
                  if (sitem.name == other_cn.name && sitem.expression &&
                      sitem.expression->Type() == TypeTag::kColumnValue) {
                    const ColumnName& inner_cn =
                        sitem.expression->AsColumnValue().GetColumnName();
                    for (const auto& isrc : sub.Sources()) {
                      if (isrc.query || isrc.unnest || isrc.sample ||
                          ctes.contains(isrc.table)) {
                        continue;
                      }
                      const std::string iqual =
                          isrc.alias.empty() ? isrc.table : isrc.alias;
                      if (!inner_cn.schema.empty() &&
                          inner_cn.schema != iqual) {
                        continue;
                      }
                      auto istats = context.GetStats(isrc.table);
                      auto itbl = context.GetTable(isrc.table);
                      if (istats.HasValue() && itbl.HasValue() &&
                          istats.Value()->Rows() >= 1000000 &&
                          LocalColumnOffset(itbl.Value()->GetSchema(),
                                            ColumnName("", inner_cn.name))
                              .has_value()) {
                        fact_table = isrc.table;
                        fact_fk_col = inner_cn.name;
                        dim_pk_slot = dslot;
                        found_join = true;
                        break;
                      }
                    }
                    if (found_join) {
                      break;
                    }
                  }
                }
                if (found_join) {
                  break;
                }
              }
            }
            if (found_join) {
              break;
            }
          }
          if (!found_join) {
            continue;
          }
          // Build 64-bit hash set of seed's dim_out_pairs (IS NOT DISTINCT
          // FROM: NULLs hash to a distinct tag). A 64-bit hash match is a sound
          // superset for a runtime filter (false positives are harmlessly
          // filtered by the exact INTERSECT; false negatives are impossible).
          // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
          //   (intersect_dim_key_semifilter_sound)
          auto hash_mix = [](uint64_t h, uint64_t v) noexcept -> uint64_t {
            v += 0x9e3779b97f4a7c15ULL;
            v = (v ^ (v >> 30)) * 0xbf58476d1ce4e5b9ULL;
            v = (v ^ (v >> 27)) * 0x94d049bb133111ebULL;
            v ^= (v >> 31);
            return (h * 0x100000001b3ULL) ^ v;
          };
          auto hash_col_cell = [&](uint64_t h, const ColumnarColumn& col,
                                   size_t r, bool* ok) noexcept -> uint64_t {
            if (!col.is_uniform) {
              *ok = false;
              return h;
            }
            if (col.has_null && col.IsNull(r)) {
              return hash_mix(h, 0x5555aaaa5555aaaaULL);
            }
            if (col.type == ValueType::kVarChar) {
              const std::string_view sv = col.StringAt(r);
              const uint64_t sh = std::hash<std::string_view>{}(sv);
              return hash_mix(hash_mix(h, static_cast<uint64_t>(sv.size())),
                              sh);
            }
            if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
              return hash_mix(h, static_cast<uint64_t>(col.IntAt(r)));
            }
            *ok = false;
            return h;
          };
          FlatIntSet seed_hashes;
          seed_hashes.Reserve(seed.TotalRows());
          bool seed_ok = true;
          for (const ColumnarChunk& ch : seed.columnar->chunks) {
            for (size_t r = 0; r < ch.rows; ++r) {
              uint64_t h = 0xcbf29ce484222325ULL;
              for (const auto& [oi, _] : dim_out_pairs) {
                if (oi >= ch.columns.size()) {
                  seed_ok = false;
                  break;
                }
                h = hash_col_cell(h, ch.columns[oi], r, &seed_ok);
                if (!seed_ok) {
                  break;
                }
              }
              if (!seed_ok) {
                break;
              }
              seed_hashes.Insert(std::bit_cast<int64_t>(h));
            }
            if (!seed_ok) {
              break;
            }
          }
          if (!seed_ok || seed_hashes.Empty()) {
            continue;
          }
          // Scan D (projected to pk + dim_out_pairs) in parallel and collect
          // all D.pk whose hashed attribute tuple belongs to seed_hashes.
          std::vector<slot_t> dproj;
          dproj.push_back(dim_pk_slot);
          for (const auto& [_, dcol] : dim_out_pairs) {
            dproj.push_back(dcol);
          }
          std::ranges::sort(dproj);
          dproj.erase(std::ranges::unique(dproj).begin(), dproj.end());
          const auto pk_pos = static_cast<size_t>(std::distance(
              dproj.begin(), std::ranges::lower_bound(dproj, dim_pk_slot)));
          std::vector<size_t> attr_pos;
          attr_pos.reserve(dim_out_pairs.size());
          for (const auto& [_, dcol] : dim_out_pairs) {
            attr_pos.push_back(static_cast<size_t>(std::distance(
                dproj.begin(), std::ranges::lower_bound(dproj, dcol))));
          }
          StatusOr<Relation> drel_or = LoadSource(
              context, dsrc, nullptr, ctes, &dproj, nullptr, nullptr, nullptr,
              /*columnar_output=*/true);
          if (!drel_or.HasValue() || !drel_or.Value().GetStatus().ok() ||
              !drel_or.Value().HasColumnarOnly() ||
              drel_or.Value().columnar == nullptr) {
            continue;
          }
          const Relation& drel = drel_or.Value();
          const size_t d_nchunks = drel.columnar->chunks.size();
          const size_t d_workers =
              drel.TotalRows() >= 32768
                  ? std::min<size_t>(QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
                                     std::max<size_t>(1, d_nchunks))
                  : 1;
          std::vector<std::vector<int64_t>> worker_pks(d_workers);
          std::atomic<bool> scan_failed{false};
          auto scan_worker = [&](size_t w) {
            const size_t cbegin = (d_nchunks * w) / d_workers;
            const size_t cend = (d_nchunks * (w + 1)) / d_workers;
            auto& out_pks = worker_pks[w];
            for (size_t ci = cbegin; ci < cend; ++ci) {
              if (scan_failed.load(std::memory_order_relaxed)) {
                return;
              }
              const ColumnarChunk& ch = drel.columnar->chunks[ci];
              if (pk_pos >= ch.columns.size() ||
                  !ch.columns[pk_pos].is_uniform) {
                scan_failed.store(true, std::memory_order_relaxed);
                return;
              }
              const ColumnarColumn& pkc = ch.columns[pk_pos];
              for (size_t r = 0; r < ch.rows; ++r) {
                if (pkc.has_null && pkc.IsNull(r)) {
                  continue;
                }
                uint64_t h = 0xcbf29ce484222325ULL;
                bool ok = true;
                for (size_t ap : attr_pos) {
                  if (ap >= ch.columns.size()) {
                    ok = false;
                    break;
                  }
                  h = hash_col_cell(h, ch.columns[ap], r, &ok);
                  if (!ok) {
                    break;
                  }
                }
                if (!ok) {
                  scan_failed.store(true, std::memory_order_relaxed);
                  return;
                }
                if (seed_hashes.Contains(std::bit_cast<int64_t>(h))) {
                  out_pks.push_back(pkc.IntAt(r));
                }
              }
            }
          };
          if (d_workers > 1) {
            // A failed task can leave its key list incomplete. Discard
            // the candidate filter so it cannot reject a matching row.
            if (!RunParallelTasks(d_workers, scan_worker).ok()) {
              continue;
            }
          } else {
            scan_worker(0);
          }
          if (scan_failed.load(std::memory_order_relaxed)) {
            continue;
          }
          size_t matched_pks = 0;
          for (const auto& vp : worker_pks) {
            matched_pks += vp.size();
          }
          if (matched_pks == 0 || matched_pks * 10 > drel.TotalRows() * 7) {
            continue;
          }
          RuntimeFilterBuilder rfb;
          for (const auto& vp : worker_pks) {
            for (int64_t pk : vp) {
              rfb.Observe(pk);
            }
          }
          auto grp = rfb.Build(false);
          if (grp == nullptr) {
            continue;
          }
          return ExecutionRuntime::ScopedExtraTableRf{
              .table_name = std::move(fact_table),
              .column_name = std::move(fact_fk_col),
              .group = std::move(grp),
          };
        }
        return std::nullopt;
      };

      ASSIGN_OR_RETURN(Relation, first_rel,
                       (ExecuteQuery(context, *unshared_ptrs[order[0]], outer,
                                     ctes, nullptr, branch_columnar_flat)));
      combined = std::move(first_rel);
      // The head branch's executed schema carries the authoritative output
      // names (star items never appear in SelectList() 1:1). Capture them
      // when the fold reaches the head so the post-loop rename below does
      // not depend on SelectList().size() matching the schema width.
      std::vector<ColumnName> head_column_names;
      for (size_t oi = 1; oi < order.size(); ++oi) {
        const SelectStatement& next_stmt = *unshared_ptrs[order[oi]];
        auto semi_rf = try_build_semi_rf(combined, next_stmt);
        auto* rt = context.execution_runtime();
        struct
            ScopedRfPop {  // NOLINT(cppcoreguidelines-special-member-functions):
                           // scope-owned cleanup.
          ExecutionRuntime* r{nullptr};
          ~ScopedRfPop() {
            if (r != nullptr) {
              r->scoped_extra_table_rfs.pop_back();
            }
          }
        } pop_guard;
        if (semi_rf.has_value() && rt != nullptr) {
          rt->scoped_extra_table_rfs.push_back(std::move(*semi_rf));
          pop_guard.r = rt;
        }
        ASSIGN_OR_RETURN(Relation, branch,
                         (ExecuteQuery(context, next_stmt, outer, ctes, nullptr,
                                       branch_columnar_flat)));
        RETURN_IF_FAIL(combined.GetStatus());
        RETURN_IF_FAIL(branch.GetStatus());
        if (branch.schema.ColumnCount() != combined.schema.ColumnCount()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "INTERSECT input column counts differ");
        }
        if (order[oi] == 0) {
          head_column_names.reserve(branch.schema.ColumnCount());
          for (size_t hc = 0; hc < branch.schema.ColumnCount(); ++hc) {
            head_column_names.push_back(branch.schema.GetColumn(hc).Name());
          }
        }
        ASSIGN_OR_RETURN(Relation, folded_pair,
                         (apply_pair(std::move(combined), std::move(branch),
                                     SetOperationKind::kIntersect)));
        combined = std::move(folded_pair);
      }
      // The branch fold checks input widths. Keep its resolved types and
      // attributes, and restore names from the original leftmost branch.
      if (order[0] != 0) {
        if (head_column_names.size() != combined.schema.ColumnCount()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "INTERSECT result column counts differ");
        }
        std::vector<Column> renamed;
        renamed.reserve(combined.schema.ColumnCount());
        for (size_t ci = 0; ci < combined.schema.ColumnCount(); ++ci) {
          Column col = combined.schema.GetColumn(ci);
          col.Name() = std::move(head_column_names[ci]);
          renamed.push_back(std::move(col));
        }
        combined.schema = Schema("", std::move(renamed));
      }
      handled_intersect_fast = true;
    }
    if (!handled_intersect_fast) {
      ASSIGN_OR_RETURN(Relation, first_combined, run_shared_term(head));
      combined = std::move(first_combined);
      std::vector<Relation> terms;
      terms.push_back(std::move(combined));
      std::vector<SetOperationKind> low_ops;
      for (size_t i = 0; i < statement.UnionAll().size(); ++i) {
        const auto& union_stmt = statement.UnionAll()[i];
        ASSIGN_OR_RETURN(Relation, branch, run_shared_term(*union_stmt));
        const SetOperationKind operation =
            i < statement.SetOperationKinds().size()
                ? statement.SetOperationKinds()[i]
                : SetOperationKind::kUnionAll;
        const bool intersects = operation == SetOperationKind::kIntersect ||
                                operation == SetOperationKind::kIntersectAll;
        if (intersects) {
          ASSIGN_OR_RETURN(Relation, folded_pair,
                           (apply_pair(std::move(terms.back()),
                                       std::move(branch), operation)));
          terms.back() = std::move(folded_pair);
        } else {
          low_ops.push_back(operation);
          terms.push_back(std::move(branch));
        }
      }
      ASSIGN_OR_RETURN(Relation, combined_folded,
                       (FoldLowPrecedenceSetTerms(terms, low_ops, apply_pair)));
      combined = std::move(combined_folded);
    }
    RETURN_IF_FAIL(combined.FinishSpill());
    RETURN_IF_FAIL(combined.GetStatus());
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &combined, outer, ctes));
    }
    ASSIGN_OR_RETURN(
        Relation, limited,
        (LimitedRows(statement, std::move(combined), &context, outer, &ctes)));
    if (!columnar_output && limited.HasColumnarOnly()) {
      RETURN_IF_FAIL(limited.EnsureRowsMaterialized());
    }
    return limited;
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
      ~RestoreReuse() noexcept {
        if (was_reusable) {
          // This set is only an optimization hint. A failed allocation while
          // restoring it must not turn stack unwinding into terminate.
          try {
            runtime->reusable_base_relations.insert(table);
          } catch (...) {  // NOLINT(bugprone-empty-catch)
          }
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
      return ExecuteQuery(context, initialized, outer, ctes, output_sink,
                          columnar_output);
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
    const uint64_t columnar_rows =
        (lst_store != nullptr)
            ? lst_store->total_rows
            : (table.HasValue() ? table.Value()->BackingColumnarRowCount() : 0);
    if (table.HasValue() && !reusable && columnar_rows > 0) {
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
          .input_rows = static_cast<size_t>(columnar_rows),
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
          !scan_filter.prebuilt_membership[0].packed_pair_col2.has_value() &&
          scan_filter.prebuilt_membership[0].distinct_witness_minmax ==
              nullptr &&
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
        TypedAggregationPlan typed_plan = PlanTypedAggregation(
            stmt, having_expr, qualified_schema, aggregate_expressions,
            non_query_predicates, /*allow_composite_select=*/true);
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
          RETURN_IF_FAIL(typed_output.FinishSpill());
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
  if (::tinylamb::platform::GetEnv("TINYLAMB_DEBUG_GS") !=
      nullptr) {  // NOLINT(cert-err33-c)
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
    Expression having_expr = InlineHavingAliases(statement, input.schema);
    if (having_expr && ContainsQuery(having_expr)) {
      auto folded =
          FoldRowIndependentSubqueries(context, {having_expr}, outer, ctes);
      if (folded.size() == 1) {
        having_expr = std::move(folded[0]);
      }
    }

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

    if (having_residual) {
      having_residual = ReplaceSubExpressions(
          having_residual,
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
    }
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
    auto accumulate_row =
        [&](const Row& row, int64_t repetitions = 1,
            const Value* prepared_key = nullptr,
            const std::vector<AggregateAccumulator>* prepared_states =
                nullptr) -> Status {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->scan_output_rows += repetitions;
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
        if (prepared_key != nullptr) {
          key_values.push_back(CanonicalDistinctValue(*prepared_key));
        } else if (group_keys_are_columns) {
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
        if (aggregate.WhereFilter()) {
          ASSIGN_OR_RETURN(Value, passes,
                           TryEvaluate(aggregate.WhereFilter(), scope, nullptr,
                                       context, ctes));
          if (!Truthy(passes)) {
            continue;
          }
        }
        if (prepared_states != nullptr) {
          RETURN_IF_FAIL(accumulator.Merge((*prepared_states)[i]));
        } else if (repetitions > 1) {
          // Only TryFoldDictionaryAggregateRows supplies multiplicities. It
          // proves the restricted aggregate shape and the exact AVG bound.
          ASSIGN_OR_RETURN(
              Value, value,
              (is_count_star[i] ? StatusOr<Value>(Value(1))
                                : TryEvaluate(aggregate.Child(), scope, nullptr,
                                              context, ctes)));
          if (!value.IsNull()) {
            switch (aggregate.GetType()) {
              case AggregationType::kCount:
                accumulator.count += repetitions;
                break;
              case AggregationType::kAvg:
                accumulator.count += repetitions;
                accumulator.total +=
                    (value.type == ValueType::kDouble
                         ? value.value.double_value
                         : static_cast<double>(value.value.int_value)) *
                    static_cast<double>(repetitions);
                break;
              default:
                RETURN_IF_FAIL(accumulator.Add(value));
                break;
            }
          }
        } else if (is_count_star[i]) {
          RETURN_IF_FAIL(accumulator.Add(Value(1)));
        } else if (aggregate_child_offsets[i] &&
                   !aggregate.NeedsGroupContext() &&
                   aggregate.Having() == AggregateHavingModifier::kNone) {
          RETURN_IF_FAIL(accumulator.Add(row[*aggregate_child_offsets[i]]));
        } else {
          AggregateInput aggregate_input;
          ASSIGN_OR_RETURN(
              Value, argument,
              TryEvaluate(aggregate.Child(), scope, nullptr, context, ctes));
          aggregate_input.value = std::move(argument);
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
          context.execution_runtime()->aggregate_updates += repetitions;
        }
      }
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->aggregate_input_rows += repetitions;
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
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_REUSABLE_TYPED_AGG") !=
        nullptr;
    // The aggregate list above uses resolved ORDER BY nodes. Give the typed
    // planner those same nodes: the original COUNT(*) spelling can be a
    // different AST object from the SELECT aggregate it resolves to.
    SelectStatement typed_statement(stmt);
    auto typed_order = stmt.OrderBy();
    for (size_t i = 0; i < typed_order.size(); ++i) {
      typed_order[i].expression = resolved_order_keys[i];
    }
    typed_statement.SetOrderBy(std::move(typed_order));
    if (!reusable || !reusable_typed_disabled) {
      TypedAggregationPlan typed_plan = PlanTypedAggregation(
          typed_statement, having_residual, qualified_schema,
          aggregate_expressions, scan_predicates,
          /*allow_composite_select=*/true);
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
              stored->second.OwnedBy(statement) && !stored->second.empty() &&
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
            context, typed_statement, having_residual, *table.Value(),
            source.table, reusable, projection, qualified_schema, table_schema,
            peek_compares, aggregate_expressions, ctes, std::move(typed_plan),
            stream_rf, stream_rf_type, stream_key_col);
        if (!typed.HasValue()) {
          return typed.GetStatus();
        }
        Relation typed_output = typed.MoveValue();
        RETURN_IF_FAIL(typed_output.FinishSpill());
        return LimitedRows(statement, std::move(typed_output), &context, outer,
                           &ctes);
      }
      ASSIGN_OR_RETURN(
          std::optional<Relation>, columnar_tlcs_result,
          relational_detail::TryRunColumnarTlcsAggregate(
              context, typed_statement, having_residual, *table.Value(),
              projection, qualified_schema, table_schema,
              /*typed_plan=*/nullptr, aggregate_expressions, scan_predicates,
              /*resident_store_hint=*/nullptr, context.execution_runtime(),
              /*publish_lstable_marker=*/false, /*fold_cache_key=*/{},
              scan_begin));
      if (columnar_tlcs_result) {
        RETURN_IF_FAIL(columnar_tlcs_result->FinishSpill());
        return LimitedRows(statement, std::move(*columnar_tlcs_result),
                           &context, outer, &ctes);
      }
    }

    if (!having_expr) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, dictionary_result,
          TryDictionaryExpressionAggregate(
              context, typed_statement, *table.Value(), projection,
              qualified_schema, aggregate_expressions, scan_predicates, ctes));
      if (dictionary_result) {
        RETURN_IF_FAIL(dictionary_result->FinishSpill());
        return LimitedRows(statement, std::move(*dictionary_result), &context,
                           outer, &ctes);
      }
    }

    bool dictionary_rows_folded = false;
    if (!reusable) {
      ASSIGN_OR_RETURN(
          bool, folded,
          TryFoldDictionaryAggregateRows(
              context, stmt, *table.Value(), projection, qualified_schema,
              aggregate_expressions, scan_predicates,
              [&](const Row& row, int64_t repetitions) -> Status {
                Status filter_status{Status::kSuccess};
                if (!MatchScanFilter(row, qualified_schema, scan_filter, outer,
                                     context, ctes, &filter_status)) {
                  return filter_status;
                }
                return accumulate_row(row, repetitions);
              },
              [&](const Row& row, int64_t repetitions, const Value& key,
                  const std::vector<AggregateAccumulator>& states) {
                return accumulate_row(row, repetitions, &key, &states);
              }));
      dictionary_rows_folded = folded;
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
            ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_AGG_ARENA") !=
            nullptr;
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
        // IsValid() turns false on EOF *and* on a failed page read (e.g. an
        // evicted page whose spill read fails); never cache or aggregate a
        // silently truncated scan.
        RETURN_IF_FAIL(iterator.GetStatus());
        // AddRow() is void: a spill-write failure is latched as a sticky
        // first-error on the Relation. Never publish a truncated relation into
        // the shared cache, or every later cache hit reports the partial rows
        // as the complete answer.
        RETURN_IF_FAIL(cache_rel.FinishSpill());
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
      const Status cache_finish_status = cached->second->FinishSpill();
      if (cache_finish_status != Status::kSuccess) {
        scan_accumulate_error = cache_finish_status;
      }
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
    } else if (!dictionary_rows_folded) {
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
      RETURN_IF_FAIL(iterator.GetStatus());
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
    // Same ordering contract as the non-grouped relational path above: NULL
    // placement, direction and the key's collation all come from
    // SortKeyOrder, so a grouped ORDER BY folds exactly like an ungrouped one.
    std::vector<SortKeyOrder> order_specs;
    order_specs.reserve(stmt.OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : stmt.OrderBy()) {
      order_specs.emplace_back(term.ascending, term.nulls_first,
                               CollationOfExpression(term.expression));
    }
    bool bounded = false;
    size_t topn_capacity = 0;
    if (!stmt.Distinct() && stmt.HasLimit() && !stmt.WithTies() &&
        stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()) {
      bounded = true;
      topn_capacity = stmt.Offset() + stmt.Limit();
    }
    const bool top1_with_ties = !stmt.Distinct() && stmt.HasLimit() &&
                                stmt.WithTies() && stmt.Limit() == 1 &&
                                stmt.Offset() == 0;
    ExternalKeyedSorter sorter(order_specs, bounded, topn_capacity,
                               top1_with_ties);
    // Aggregate expression identities are fixed across groups. Reuse their
    // map nodes, while still finishing every aggregate before testing HAVING
    // so errors in a discarded group remain observable.
    AggregateResultMap aggregate_results;
    aggregate_results.reserve(aggregate_expressions.size());
    std::vector<Value*> aggregate_slots;
    aggregate_slots.reserve(aggregate_expressions.size());
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      auto [slot, inserted] = aggregate_results.emplace(aggregate, Value());
      aggregate_slots.push_back(&slot->second);
    }
    for (const GroupState& group : groups) {
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, finished,
            aggregate_states[group.accumulator_offset + i].TryFinish());
        *aggregate_slots[i] = std::move(finished);
      }
      Scope scope{.row = &group.representative,
                  .schema = &input.schema,
                  .outer = outer};
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = &aggregate_results;
      if (having_residual) {
        ASSIGN_OR_RETURN(Value, passes,
                         TryEvaluate(having_residual, scope, &aggregate_results,
                                     context, ctes));
        if (!Truthy(passes)) {
          continue;
        }
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
        RETURN_IF_FAIL(sorter.Add(
            KeyedRow{.keys = std::move(keys), .row = std::move(output_row)}));
      } else {
        output.AddRow(std::move(output_row));
      }
    }
    if (sorter.UsedBoundedHeap() && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = topn_capacity;
      context.execution_runtime()->topn_finish_input_rows =
          sorter.TotalInputRows();
    }
    if (!sorter.Empty()) {
      const auto sort_begin = std::chrono::steady_clock::now();
      RETURN_IF_FAIL(sorter.EmitSorted([&](Row&& sorted_row) {
        output.AddRow(std::move(sorted_row));
        return Status::kSuccess;
      }));
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
      }
    }
    RefineProjectionTypes(output, output_columns);
    output.schema = Schema("", std::move(output_columns));
    RETURN_IF_FAIL(output.FinishSpill());
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

  if (HasWindowFunctions(statement) && StatementIsAggregated(statement)) {
    ASSIGN_OR_RETURN(Relation, window_over_aggregate,
                     (ExecuteWindowOverAggregate(
                         context, statement, Relation(), outer, ctes, true,
                         true, columnar_output || outer == nullptr)));
    if (!columnar_output && window_over_aggregate.HasColumnarOnly()) {
      RETURN_IF_FAIL(window_over_aggregate.EnsureRowsMaterialized());
    }
    return window_over_aggregate;
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
  const auto same_expr = [](const Expression& a, const Expression& b) -> bool {
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
          // This shortcut stores its threshold and operands as DOUBLE.
          // Integer predicates must keep the canonical comparison, including
          // adjacent values above 2^53 and unsigned operands.
          if (!cv.IsNull() && cv.type == ValueType::kDouble) {
            post_having.active = true;
            post_having.is_self_sum_factor = false;
            post_having.select_col = *matched_col;
            post_having.op = hbin.Op();
            post_having.constant_or_factor = cv.value.double_value;
          }
        } else if (hbin.Right()->Type() == TypeTag::kQueryExp &&
                   !statement.Distinct() && !statement.HasLimit() &&
                   statement.Offset() == 0 && statement.WithQueries().empty() &&
                   statement.UnionAll().empty() &&
                   statement.GetSetOperationTree() == nullptr) {
          // Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
          //   (grouped_having_global_sum_fubini_sound,
          //    grouped_having_distinct_sum_fubini_bug)
          // Match HAVING SUM(e) <cmp> (SELECT SUM(e) * factor FROM <same>
          // WHERE <same>) so TryFusedGroupedJoin computes the grand total
          // from group sums in a single pass without scanning the inputs twice
          // or materializing a CTE.
          const auto& left_agg = hbin.Left()->AsAggregateExpression();
          const auto& right_qe = hbin.Right()->AsQueryExpression();
          if (left_agg.GetType() == AggregationType::kSum &&
              !left_agg.Distinct() && !left_agg.HasWhereFilter() &&
              right_qe.Query() != nullptr && !right_qe.Test() &&
              !right_qe.Exists() && !right_qe.Negated() &&
              !right_qe.ArrayResult()) {
            const auto& sub_stmt = *right_qe.Query();
            if (sub_stmt.GroupBy().empty() && !sub_stmt.Having() &&
                !sub_stmt.Qualify() && sub_stmt.OrderBy().empty() &&
                !sub_stmt.Distinct() && !sub_stmt.HasLimit() &&
                sub_stmt.Offset() == 0 && sub_stmt.WithQueries().empty() &&
                sub_stmt.UnionAll().empty() &&
                sub_stmt.GetSetOperationTree() == nullptr &&
                sub_stmt.SelectList().size() == 1 &&
                sub_stmt.Sources().size() == statement.Sources().size() &&
                same_expr(statement.WhereClause(), sub_stmt.WhereClause())) {
              bool same_sources = true;
              for (size_t si = 0; si < statement.Sources().size(); ++si) {
                const auto& a = statement.Sources()[si];
                const auto& b = sub_stmt.Sources()[si];
                if (a.query != nullptr || b.query != nullptr || a.unnest ||
                    b.unnest || a.sample || b.sample ||
                    a.join_type != b.join_type || a.table != b.table ||
                    a.alias != b.alias ||
                    !same_expr(a.join_condition, b.join_condition)) {
                  same_sources = false;
                  break;
                }
              }
              const Expression& sub_expr = sub_stmt.SelectList()[0].expression;
              if (same_sources && sub_expr &&
                  sub_expr->Type() == TypeTag::kBinaryExp &&
                  sub_expr->AsBinaryExpression().Op() ==
                      BinaryOperation::kMultiply) {
                const auto& mbin = sub_expr->AsBinaryExpression();
                auto extract_factor = [&](const Expression& a,
                                          const Expression& b,
                                          double* factor_out) -> bool {
                  if (!a || !b || a->Type() != TypeTag::kAggregateExp ||
                      b->Type() != TypeTag::kConstantValue ||
                      !same_expr(a, hbin.Left())) {
                    return false;
                  }
                  const Value& fv = b->AsConstantValue().GetValue();
                  if (fv.IsNull()) {
                    return false;
                  }
                  double f = 0.0;
                  if (fv.type == ValueType::kDouble) {
                    f = fv.value.double_value;
                  } else if (fv.type == ValueType::kInt64) {
                    // This shortcut stores the factor as DOUBLE. Decline any
                    // integer that is not exactly representable, including
                    // every UINT64 factor (the old code converted uint64_t
                    // to double and rounded factors above 2^53, flipping
                    // HAVING results at the boundary).
                    constexpr int64_t kExactDoubleLimit = int64_t{1} << 53;
                    if (fv.IsUnsigned() ||
                        fv.value.int_value > kExactDoubleLimit ||
                        fv.value.int_value < -kExactDoubleLimit) {
                      return false;
                    }
                    f = static_cast<double>(fv.value.int_value);
                  } else {
                    return false;
                  }
                  if (!std::isfinite(f)) {
                    return false;
                  }
                  *factor_out = f;
                  return true;
                };
                double factor = 0.0;
                if (extract_factor(mbin.Left(), mbin.Right(), &factor) ||
                    extract_factor(mbin.Right(), mbin.Left(), &factor)) {
                  post_having.active = true;
                  post_having.is_self_sum_factor = true;
                  post_having.select_col = *matched_col;
                  post_having.op = hbin.Op();
                  post_having.constant_or_factor = factor;
                }
              }
            }
          }
        }
      }
    }
  }
  if (!post_having.active && outer == nullptr && statement.Having() &&
      ContainsQuery(statement.Having())) {
    auto folded_having = FoldRowIndependentSubqueries(
        context, {statement.Having()}, outer, ctes);
    if (folded_having.size() == 1 && !ContainsQuery(folded_having[0])) {
      SelectStatement initialized(statement);
      initialized.ClearWithQueries();
      initialized.SetHaving(folded_having[0]);
      return ExecuteQuery(context, initialized, outer, ctes, output_sink,
                          columnar_output);
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
      fusion_spec.post_having_agg_slot = static_cast<slot_t>(
          std::distance(fusion_spec.aggregates.begin(), found));
      fusion_spec.post_having_op = post_having.op;
      fusion_spec.post_having_is_self_sum_factor =
          post_having.is_self_sum_factor;
      fusion_spec.post_having_constant_or_factor =
          post_having.constant_or_factor;
    }
  }
  struct RankLimitGuard {
    ExecutionRuntime* runtime{nullptr};
    std::optional<ExecutionRuntime::GroupingRankLimit> saved;
    RankLimitGuard() = default;
    RankLimitGuard(const RankLimitGuard&) = delete;
    RankLimitGuard& operator=(const RankLimitGuard&) = delete;
    RankLimitGuard(RankLimitGuard&&) = delete;
    RankLimitGuard& operator=(RankLimitGuard&&) = delete;
    ~RankLimitGuard() {
      if (runtime != nullptr) {
        runtime->grouping_rank_limit = std::move(saved);
      }
    }
  } rank_guard;
  if (auto* runtime = context.execution_runtime();
      runtime != nullptr && statement.Sources().size() == 1 &&
      statement.Sources()[0].query != nullptr && statement.Qualify() &&
      statement.Qualify()->Type() == TypeTag::kBinaryExp &&
      statement.GroupBy().empty()) {
    const WindowFunctionCallExpression* rank = nullptr;
    const NamedExpression* rank_item = nullptr;
    for (const auto& item : statement.SelectList()) {
      if (!item.expression ||
          item.expression->Type() != TypeTag::kWindowFunctionExp) {
        continue;
      }
      if (rank != nullptr) {
        rank = nullptr;
        rank_item = nullptr;
        break;
      }
      rank = &item.expression->AsWindowFunctionCallExpression();
      rank_item = &item;
    }
    const auto& predicate = statement.Qualify()->AsBinaryExpression();
    // The QUALIFY left side must be this select-list RANK (the same window
    // expression or its alias); an unrelated RANK() in QUALIFY must not drive
    // the pre-aggregation group pruning.
    const bool predicate_is_rank =
        rank_item != nullptr && predicate.Left() &&
        (same_expr(predicate.Left(), rank_item->expression) ||
         (predicate.Left()->Type() == TypeTag::kColumnValue &&
          !rank_item->name.empty() &&
          predicate.Left()->AsColumnValue().GetColumnName().name ==
              rank_item->name));
    const bool descending_rank =
        predicate_is_rank && rank != nullptr && rank->function == "RANK" &&
        rank->args.empty() && !rank->where_filter && !rank->distinct &&
        rank->inner_order_by.empty() && !rank->inner_limit &&
        !rank->has_frame && rank->partition_by.size() == 1 &&
        rank->order_by.size() == 1 && !rank->order_by[0].ascending &&
        rank->partition_by[0] &&
        rank->partition_by[0]->Type() == TypeTag::kColumnValue &&
        rank->order_by[0].expression &&
        rank->order_by[0].expression->Type() == TypeTag::kColumnValue &&
        (predicate.Op() == BinaryOperation::kLessThanEquals ||
         predicate.Op() == BinaryOperation::kLessThan) &&
        predicate.Right() &&
        predicate.Right()->Type() == TypeTag::kConstantValue;
    if (descending_rank) {
      const auto& bound = predicate.Right()->AsConstantValue().GetValue();
      if (bound.type == ValueType::kInt64 && !bound.IsUnsigned() &&
          bound.value.int_value > 0) {
        const int64_t kept =
            bound.value.int_value -
            (predicate.Op() == BinaryOperation::kLessThan ? 1 : 0);
        if (kept > 0) {
          rank_guard.runtime = runtime;
          rank_guard.saved = std::move(runtime->grouping_rank_limit);
          runtime->grouping_rank_limit = ExecutionRuntime::GroupingRankLimit{
              .partition_name =
                  rank->partition_by[0]->AsColumnValue().GetColumnName().name,
              .order_name = rank->order_by[0]
                                .expression->AsColumnValue()
                                .GetColumnName()
                                .name,
              .limit = static_cast<size_t>(kept)};
        }
      }
    }
  }
  const auto build_input_begin = std::chrono::steady_clock::now();
  // A sink consumes the final projected groups. Keep all semantic tails on
  // the ordinary path unless there is nothing above those groups to apply.
  if (output_sink != nullptr && !statement.Having() &&
      statement.OrderBy().empty() && !statement.HasLimit() &&
      statement.Offset() == 0 && !statement.Distinct() &&
      !HasWindowFunctions(statement)) {
    fusion_spec.output_sink = output_sink;
  }
  if (fusion_spec.eligible) {
    fusion_spec.columnar_output =
        columnar_output && fusion_spec.output_sink == nullptr &&
        (statement.OrderBy().empty() ||
         (statement.HasLimit() && !statement.WithTies() &&
          !statement.Qualify()));
  }
  relational_detail::PushedScanProjection pushed_projection;
  const relational_detail::PushedScanProjection* pushed_projection_ptr =
      nullptr;
  bool pushed_projection_applied = false;
  if (columnar_output && outer == nullptr && !fusion_spec.eligible &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest &&
      !statement.Sources()[0].from_nested_join &&
      !statement.Sources()[0].sample &&
      statement.Sources()[0].using_columns.empty() &&
      !ctes.contains(statement.Sources()[0].table) &&
      !HasWindowFunctions(statement) && !StatementIsAggregated(statement) &&
      !statement.Distinct() && !statement.Having() && !statement.Qualify() &&
      statement.GroupBy().empty() && statement.OrderBy().empty() &&
      !statement.SelectList().empty()) {
    StatusOr<std::shared_ptr<Table>> table_or =
        context.GetTable(statement.Sources()[0].table);
    if (table_or.HasValue()) {
      const Schema& raw_schema = table_or.Value()->GetSchema();
      const std::string& qualifier = statement.Sources()[0].alias.empty()
                                         ? statement.Sources()[0].table
                                         : statement.Sources()[0].alias;
      const Schema qualified_schema =
          qualifier.empty() ? raw_schema : QualifySchema(raw_schema, qualifier);
      bool ok = true;
      std::vector<Column> out_cols;
      out_cols.reserve(statement.SelectList().size());
      pushed_projection.expressions.reserve(statement.SelectList().size());
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        const NamedExpression& item = statement.SelectList()[i];
        if (!item.expression || ContainsQuery(item.expression) ||
            ContainsAggregate(item.expression) ||
            ContainsParameterSlot(item.expression)) {
          ok = false;
          break;
        }
        if (item.expression->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn =
              item.expression->AsColumnValue().GetColumnName();
          if (cn.name == "*" || !relational_detail::ResolveScalarColumnOffset(
                                     qualified_schema, cn)
                                     .has_value()) {
            ok = false;
            break;
          }
        }
        out_cols.push_back(ProjectionExpressionColumn(
            ProjectionName(item, i), item.expression, qualified_schema));
        pushed_projection.expressions.push_back(item.expression);
      }
      if (ok) {
        pushed_projection.output_schema = Schema("", std::move(out_cols));
        pushed_projection_ptr = &pushed_projection;
      }
    }
  }
  const bool input_columnar =
      columnar_output ||
      (!fusion_spec.eligible && outer == nullptr &&
       ((StatementIsAggregated(statement) && !statement.Qualify()) ||
        HasWindowFunctions(statement)) &&
       output_sink == nullptr);
  ASSIGN_OR_RETURN(
      Relation, input,
      BuildInput(context, statement, outer, ctes, &where_fully_applied,
                 fusion_spec.eligible ? &fusion_spec : nullptr, false,
                 input_columnar, pushed_projection_ptr,
                 &pushed_projection_applied));
  if (auto* runtime = context.execution_runtime()) {
    runtime->build_input_ms += ElapsedMs(build_input_begin);
  }
  if (fusion_spec.fired) {
    // The fused probe emitted the grouped+projected rows already; only the
    // ORDER BY / LIMIT tail remains, and eligibility guarantees every order
    // term resolves against the projected output.
    RETURN_IF_FAIL(input.FinishSpill());
    RETURN_IF_FAIL(input.GetStatus());
    if (post_having.active && !fusion_spec.post_having_applied) {
      // UINT64 aggregates live in int_value with the unsigned tag; reading
      // them as signed would turn values >= 2^63 negative.
      const auto having_numeric = [](const Value& v) -> double {
        if (v.type == ValueType::kDouble) {
          return v.value.double_value;
        }
        return v.IsUnsigned() ? static_cast<double>(
                                    static_cast<uint64_t>(v.value.int_value))
                              : static_cast<double>(v.value.int_value);
      };
      double threshold = post_having.constant_or_factor;
      bool valid_threshold = true;
      if (post_having.is_self_sum_factor) {
        double grand_total = 0.0;
        bool any_nonnull = false;
        RETURN_IF_FAIL(input.ForEachRow([&](const Row& r) {
          const Value& v = r[post_having.select_col];
          if (v.IsNull()) {
            return;
          }
          any_nonnull = true;
          grand_total += having_numeric(v);
        }));
        if (!any_nonnull) {
          valid_threshold = false;
        } else {
          threshold = grand_total * post_having.constant_or_factor;
        }
      }
      auto row_passes_having = [&](const Row& r) -> bool {
        const Value& v = r[post_having.select_col];
        if (v.IsNull()) {
          return false;
        }
        const double dv = having_numeric(v);
        switch (post_having.op) {
          case BinaryOperation::kEquals:
            return dv == threshold;
          case BinaryOperation::kNotEquals:
            return dv != threshold;
          case BinaryOperation::kLessThan:
            return dv < threshold;
          case BinaryOperation::kLessThanEquals:
            return dv <= threshold;
          case BinaryOperation::kGreaterThan:
            return dv > threshold;
          case BinaryOperation::kGreaterThanEquals:
            return dv >= threshold;
          default:
            return false;
        }
      };
      if (!valid_threshold) {
        input.ResetContents();
      } else if (!input.HasSpill()) {
        RETURN_IF_FAIL(input.EnsureRowsMaterialized());
        size_t out_idx = 0;
        for (size_t ri = 0; ri < input.rows.size(); ++ri) {
          if (row_passes_having(input.rows[ri])) {
            if (out_idx != ri) {
              input.rows[out_idx] = std::move(input.rows[ri]);
            }
            ++out_idx;
          }
        }
        input.rows.resize(out_idx);
      } else {
        Relation filtered(context.execution_runtime());
        filtered.schema = input.schema;
        filtered.using_columns = input.using_columns;
        CopyExecutionStats(&filtered, input);
        RETURN_IF_FAIL(input.ForEachRow([&](const Row& r) {
          if (row_passes_having(r)) {
            filtered.AddRow(r);
          }
        }));
        RETURN_IF_FAIL(filtered.FinishSpill());
        RETURN_IF_FAIL(filtered.GetStatus());
        input = std::move(filtered);
      }
    }
    if (!statement.OrderBy().empty()) {
      SelectStatement tail(statement);
      tail.SetOrderBy(fusion_spec.output_order);
      RETURN_IF_FAIL(ApplyOrderBy(context, tail, &input, outer, ctes));
    }
    return LimitedRows(statement, std::move(input), &context, outer, &ctes);
  }

  if (pushed_projection_applied) {
    RETURN_IF_FAIL(input.FinishSpill());
    RETURN_IF_FAIL(input.GetStatus());
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
                                      outer, ctes, !where_fully_applied, false,
                                      columnar_output || outer == nullptr)));
      RETURN_IF_FAIL(window_over_aggregate.FinishSpill());
      RETURN_IF_FAIL(window_over_aggregate.GetStatus());
      if (!columnar_output && window_over_aggregate.HasColumnarOnly()) {
        RETURN_IF_FAIL(window_over_aggregate.EnsureRowsMaterialized());
      }
      return window_over_aggregate;
    }
    ASSIGN_OR_RETURN(
        WindowedInput, windowed,
        (ApplyWindows(
            context, *effective, std::move(input), outer, ctes,
            where_fully_applied || effective->WhereClause() == nullptr)));
    windowed_statement = windowed.statement;
    hidden_columns = windowed.hidden_columns;
    input = std::move(windowed.input);
    effective = windowed_statement.get();
  }

  ASSIGN_OR_RETURN(Relation, result,
                   (FinishQuery(context, *effective, std::move(input), outer,
                                ctes, !where_fully_applied, hidden_columns)));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (!columnar_output && result.HasColumnarOnly()) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
  }
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

Status StreamRelationNextBatch(
    const std::shared_ptr<const relational_detail::ColumnarTable>& columnar,
    const Schema& output_schema, size_t* columnar_chunk_idx,
    size_t* columnar_row_in_chunk, std::vector<Row>* rows, size_t* offset,
    std::vector<std::shared_ptr<SpillFile>>* spill_files,
    size_t* spill_file_idx, std::optional<SpillFile::RowReader>* spill_reader,
    DataChunk* destination, size_t max_rows) {
  if (columnar != nullptr) {
    const size_t nchunks = columnar->chunks.size();
    while (*columnar_chunk_idx < nchunks &&
           *columnar_row_in_chunk >=
               columnar->chunks[*columnar_chunk_idx].rows) {
      ++(*columnar_chunk_idx);
      *columnar_row_in_chunk = 0;
    }
    if (*columnar_chunk_idx >= nchunks) {
      return Status::kSuccess;
    }
    const size_t first_cols =
        columnar->chunks[*columnar_chunk_idx].columns.size();
    if (output_schema.ColumnCount() == first_cols && first_cols > 0) {
      destination->Initialize(output_schema, max_rows);
    }
    while (destination->Size() < max_rows && *columnar_chunk_idx < nchunks) {
      const auto& chunk = columnar->chunks[*columnar_chunk_idx];
      if (*columnar_row_in_chunk >= chunk.rows) {
        ++(*columnar_chunk_idx);
        *columnar_row_in_chunk = 0;
        continue;
      }
      const size_t avail = chunk.rows - *columnar_row_in_chunk;
      const size_t take = std::min(avail, max_rows - destination->Size());
      if (destination->ColumnCount() == 0 && !chunk.columns.empty()) {
        std::vector<ValueType> types;
        types.reserve(chunk.columns.size());
        for (const auto& col : chunk.columns) {
          types.push_back(col.type);
        }
        destination->Initialize(types, max_rows);
        for (size_t c = 0; c < chunk.columns.size(); ++c) {
          destination->ColumnAt(c).SetAllowHeterogeneous(true);
          if (chunk.columns[c].is_unsigned) {
            destination->ColumnAt(c).SetUnsigned(true);
          }
        }
      }
      if (destination->ColumnCount() != chunk.columns.size()) {
        Row tmp;
        for (size_t i = 0; i < take; ++i) {
          chunk.MaterializeRowInto(*columnar_row_in_chunk + i, &tmp);
          destination->Append(std::move(tmp));
        }
      } else {
        for (size_t c = 0; c < chunk.columns.size(); ++c) {
          const auto& src_col = chunk.columns[c];
          ColumnVector& dst_col = destination->ColumnAt(c);
          ZoneMap* zm = destination->MutableZoneMapAt(c);
          if (src_col.IsFlat() && src_col.is_uniform && !src_col.is_interval) {
            const uint8_t* nulls =
                src_col.has_null ? src_col.nulls.data() + *columnar_row_in_chunk
                                 : nullptr;
            switch (src_col.type) {
              case ValueType::kNull:
                dst_col.AppendNullBatch(take, zm);
                continue;
              case ValueType::kInt64:
              case ValueType::kDate:
                dst_col.AppendIntBatch(
                    src_col.type, src_col.is_unsigned,
                    src_col.ints.data() + *columnar_row_in_chunk, nulls, take,
                    zm);
                continue;
              case ValueType::kDouble:
                if (!src_col.is_unsigned) {
                  dst_col.AppendDoubleBatch(
                      src_col.doubles.data() + *columnar_row_in_chunk, nulls,
                      take, zm);
                  continue;
                }
                break;
              case ValueType::kVarChar:
                dst_col.AppendStringViewBatch(
                    src_col.strings.data() + *columnar_row_in_chunk, nulls,
                    take, zm);
                continue;
              case ValueType::kArray:
              default:
                break;
            }
          }
          for (size_t i = 0; i < take; ++i) {
            Value val;
            src_col.WriteValueInto(*columnar_row_in_chunk + i, &val);
            if (zm != nullptr) {
              zm->Add(val);
            }
            dst_col.Append(std::move(val));
          }
        }
        destination->CommitBatchAppend(take);
      }
      *columnar_row_in_chunk += take;
      if (*columnar_row_in_chunk >= chunk.rows) {
        ++(*columnar_chunk_idx);
        *columnar_row_in_chunk = 0;
      }
    }
    return Status::kSuccess;
  }

  while (destination->Size() < max_rows && *offset < rows->size()) {
    destination->Append(std::move((*rows)[(*offset)++]));
  }
  if (*offset >= rows->size() && !rows->empty()) {
    std::vector<Row>().swap(*rows);
    *offset = 0;
  }
  while (destination->Size() < max_rows &&
         *spill_file_idx < spill_files->size()) {
    if (!spill_reader->has_value()) {
      ASSIGN_OR_RETURN(SpillFile::RowReader, reader,
                       (*spill_files)[*spill_file_idx]->OpenRowReader());
      *spill_reader = std::move(reader);
    }
    Row row;
    bool has_row = false;
    RETURN_IF_FAIL((*spill_reader)->Next(&row, &has_row));
    if (has_row) {
      destination->Append(std::move(row));
    } else {
      spill_reader->reset();
      (*spill_files)[*spill_file_idx].reset();
      ++(*spill_file_idx);
    }
  }
  return Status::kSuccess;
}

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
    const Status stream_status = StreamRelationNextBatch(
        columnar_, output_schema_, &columnar_chunk_idx_,
        &columnar_row_in_chunk_, &rows_, &offset_, &spill_files_,
        &spill_file_idx_, &spill_reader_, destination, max_rows);
    if (stream_status != Status::kSuccess) {
      FailWith(stream_status);
      return 0;
    }
    return destination->Size();
  }

  void Dump(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ')
           << "GroupByFinish (rows=" << total_rows_
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
    output_schema_ = result.schema;
    total_rows_ = result.TotalRows();
    if (result.HasColumnarOnly()) {
      columnar_ = std::move(result.columnar);
    } else {
      RETURN_IF_FAIL(result.TakeRowAndSpillBacking(&rows_, &spill_files_));
    }
    return Status::kSuccess;
  }

  TransactionContext* context_;
  std::shared_ptr<PlanBase> core_plan_;
  std::shared_ptr<const SelectStatement> statement_;
  Executor core_executor_;
  size_t core_input_rows_{0};
  std::vector<Row> rows_;
  std::shared_ptr<const relational_detail::ColumnarTable> columnar_;
  std::vector<std::shared_ptr<SpillFile>> spill_files_;
  size_t spill_file_idx_{0};
  std::optional<SpillFile::RowReader> spill_reader_;
  Schema output_schema_;
  size_t columnar_chunk_idx_{0};
  size_t columnar_row_in_chunk_{0};
  size_t offset_{0};
  size_t total_rows_{0};
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
        previous_(context.raw_execution_runtime()),
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
  if (!runtime.shared_base_cache &&
      (!runtime.reusable_base_relations.empty() ||
       !statement_->WithQueries().empty() || !statement_->UnionAll().empty() ||
       statement_->GetSetOperationTree() != nullptr)) {
    runtime.shared_base_cache =
        std::make_shared<relational_detail::SharedBaseRelationCache>();
  }
  const ScopedExecutionRuntime runtime_guard(*context_, runtime);
  const auto execute_begin = std::chrono::steady_clock::now();
  StatusOr<Relation> executed =
      ExecuteQuery(*context_, *statement_, nullptr, {}, nullptr, true);
  runtime.execute_ms += relational_detail::ElapsedMs(execute_begin);
  RETURN_IF_FAIL(executed.GetStatus());
  Relation result = executed.MoveValue();
  RETURN_IF_FAIL(result.FinishSpill());
  rows_.clear();
  columnar_.reset();
  spill_files_.clear();
  spill_file_idx_ = 0;
  spill_reader_.reset();
  columnar_chunk_idx_ = 0;
  columnar_row_in_chunk_ = 0;
  offset_ = 0;
  output_schema_ = result.schema;
  total_row_count_ = result.TotalRows();
  if (result.HasColumnarOnly()) {
    columnar_ = std::move(result.columnar);
  } else {
    RETURN_IF_FAIL(result.TakeRowAndSpillBacking(&rows_, &spill_files_));
  }
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
  pushed_group_rows_ = runtime.pushed_group_rows;
  pushed_group_joins_ = runtime.pushed_group_joins;
  transfer_seed_rows_ = runtime.transfer_seed_rows;
  transfer_scan_tested_ = runtime.transfer_scan_tested;
  transfer_scan_rejected_ = runtime.transfer_scan_rejected;
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
  const Status stream_status = StreamRelationNextBatch(
      columnar_, output_schema_, &columnar_chunk_idx_, &columnar_row_in_chunk_,
      &rows_, &offset_, &spill_files_, &spill_file_idx_, &spill_reader_,
      destination, max_rows);
  if (stream_status != Status::kSuccess) {
    FailWith(stream_status);
    return 0;
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
  output << "RelationalExecutor(materialized=" << total_row_count_
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
         << ", dictionary_aggregations="
         << (runtime_keep_ ? runtime_keep_->dictionary_aggregations : 0)
         << ", dictionary_summary_aggregations="
         << (runtime_keep_ ? runtime_keep_->dictionary_summary_aggregations : 0)
         << ", dictionary_aggregate_rows="
         << (runtime_keep_ ? runtime_keep_->dictionary_aggregate_rows : 0)
         << ", partitioned_dictionary_aggregations="
         << (runtime_keep_ ? runtime_keep_->partitioned_dictionary_aggregations
                           : 0)
         << ", partitioned_numeric_aggregations="
         << (runtime_keep_ ? runtime_keep_->partitioned_numeric_aggregations
                           : 0)
         << ", aggregate_ms=" << aggregate_ms_
         << ", grouped_join_fusions=" << grouped_join_fusions_
         << ", pushed_group_rows=" << pushed_group_rows_
         << ", pushed_group_joins=" << pushed_group_joins_
         << ", transfer_seed_rows=" << transfer_seed_rows_
         << ", transfer_scan_tested=" << transfer_scan_tested_
         << ", transfer_scan_rejected=" << transfer_scan_rejected_
         << ", streamed_grouped_joins=" << streamed_grouped_joins_
         << ", selective_index_scans=" << selective_index_scans_
         << ", selective_index_rows=" << selective_index_rows_
         << ", base_scan_cache_hits=" << base_scan_cache_hits_
         << ", aggregate_input_rows=" << aggregate_input_rows_
         << ", aggregate_groups=" << aggregate_groups_
         << ", aggregate_updates=" << aggregate_updates_
         << ", scan_rows=" << scan_rows_
         << ", scan_output_rows=" << scan_output_rows_
         << ", execute_ms=" << (runtime_keep_ ? runtime_keep_->execute_ms : 0)
         << ", boundary_ms=" << (runtime_keep_ ? runtime_keep_->boundary_ms : 0)
         << ", build_input_ms="
         << (runtime_keep_ ? runtime_keep_->build_input_ms : 0)
         << ", window_ms=" << (runtime_keep_ ? runtime_keep_->window_ms : 0)
         << ", window_layout_ms="
         << (runtime_keep_ ? runtime_keep_->window_layout_ms : 0)
         << ", window_input_rows="
         << (runtime_keep_ ? runtime_keep_->window_input_rows : 0)
         << ", rank_bounds=" << (runtime_keep_ ? runtime_keep_->rank_bounds : 0)
         << ", deferred_materialize_ms="
         << (runtime_keep_ ? runtime_keep_->deferred_materialize_ms : 0)
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
  // Execution materializes the statement's WITH entries into the inherited
  // CTE map before boundary-flattening the body, so a derived source whose
  // inner scan names a CTE is refused the flatten. Mirror that guard with
  // the WITH names alone: OptimizeDerivedBoundaries only calls contains()
  // on the map, never dereferences the relations.
  relational_detail::CteMap explain_ctes;
  for (const auto& [name, query] : statement_->WithQueries()) {
    (void)query;
    explain_ctes.emplace(name, nullptr);
  }
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
        relational_detail::OptimizeDerivedBoundaries(*effective, explain_ctes,
                                                     context_);
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
