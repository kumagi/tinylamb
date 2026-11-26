/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
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
#include "common/exception_status.hpp"
#include "common/log_message.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/platform/range_selection.hpp"
#include "common/query_parallelism.hpp"
#include "common/regex_guard.hpp"
#include "common/sorted_distinct_runs.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/query_memory.hpp"
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

StatusOr<Relation> RunTypedParallelStreamAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::string& table_name, bool reusable,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema,
    const std::vector<IntegerPeekCompare>& peek_compares,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const CteMap& ctes, TypedAggregationPlan plan,
    const RuntimeFilterGroup* stream_runtime_filter,
    ValueType stream_runtime_filter_type,
    std::optional<slot_t> stream_key_column,
    const PrebuiltMembershipPredicate* stream_prebuilt_membership) {
  ExecutionRuntime* runtime = context.execution_runtime();
  const auto scan_begin = std::chrono::steady_clock::now();
  const bool group_phases =
      ::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr;
  auto phase_begin = scan_begin;
  const auto phase = [&](const char* name, size_t workers, size_t groups) {
    if (group_phases) {
      std::cerr << "[typed-aggregate] " << name
                << " ms=" << ElapsedMs(phase_begin) << " workers=" << workers
                << " groups=" << groups << '\n';
      phase_begin = std::chrono::steady_clock::now();
    }
  };
  auto direct_lstable_store = SnapshotColumnStore(
      context, table, projection, qualified_schema.ColumnCount(),
      /*allow_lazy_build=*/false);
  const bool int_key =
      plan.key_offsets.size() == 1 &&
      (qualified_schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kInt64 ||
       qualified_schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kDate);
  const bool packed_varchar_key =
      !int_key && !plan.key_offsets.empty() && plan.key_offsets.size() <= 2 &&
      std::ranges::all_of(plan.key_offsets, [&](slot_t offset) {
        return qualified_schema.GetColumn(offset).Type() == ValueType::kVarChar;
      });
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
  const bool lstable_skip_cache_copy =
      (direct_lstable_store != nullptr &&
       direct_lstable_store->total_rows > 16384) ||
      table.BackingColumnarRowCount() > 16384;
  bool same_query_reuse = false;
  if (reusable && runtime != nullptr && stream_runtime_filter == nullptr) {
    fold_cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    auto found = runtime->base_relations.find(fold_cache_key);
    if (found != runtime->base_relations.end()) {
      same_query_reuse = true;
      cached_relation = found->second;
      ++runtime->base_scan_cache_hits;
      if (cached_relation != nullptr && cached_relation->rows.empty() &&
          !cached_relation->HasSpill() &&
          !runtime->base_relation_arenas.contains(fold_cache_key) &&
          (!cached_relation->HasColumnarOnly() || lstable_skip_cache_copy)) {
        cached_relation.reset();
      }
    } else {
      cached_relation = nullptr;
    }
  } else if (reusable && runtime != nullptr &&
             stream_runtime_filter != nullptr) {
    fold_cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    if (runtime->base_relations.contains(fold_cache_key)) {
      same_query_reuse = true;
      ++runtime->base_scan_cache_hits;
    }
  }
  const bool raw_fill_cache =
      cached_relation == nullptr && reusable && runtime != nullptr;
  const bool publish_lstable_marker =
      raw_fill_cache && lstable_skip_cache_copy &&
      !runtime->base_relations.contains(fold_cache_key);
  const bool fill_cache = raw_fill_cache && !lstable_skip_cache_copy &&
                          stream_runtime_filter == nullptr &&
                          stream_prebuilt_membership == nullptr;
  // When no resident LSTableStore covers this projection yet, try streaming
  // compressed TlcsFile blocks directly first so large TLCS-backed tables do
  // not pay multi-GB lazy LSTableStore deserialization before reaching the
  // zero-copy columnar kernel. If the same query re-scans the same table
  // projection (e.g. TPC-DS Q09's 10 subqueries on store_sales), populate the
  // per-column cache on the second pass so subsequent scans stay in RAM.
  bool tried_columnar_tlcs = false;
  if (direct_lstable_store == nullptr && cached_relation == nullptr &&
      !fill_cache && !same_query_reuse && stream_runtime_filter == nullptr &&
      stream_prebuilt_membership == nullptr) {
    tried_columnar_tlcs = true;
    ASSIGN_OR_RETURN(
        std::optional<Relation>, columnar_tlcs_result,
        TryRunColumnarTlcsAggregate(
            context, stmt, having_expr, table, projection, qualified_schema,
            table_schema, &plan, aggregate_expressions, {}, nullptr, runtime,
            publish_lstable_marker, fold_cache_key, scan_begin));
    if (columnar_tlcs_result) {
      phase("columnar-tlcs-agg",
            QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
            columnar_tlcs_result->rows.size());
      return std::move(*columnar_tlcs_result);
    }
    direct_lstable_store = SnapshotColumnStore(context, table, projection,
                                               qualified_schema.ColumnCount(),
                                               /*allow_lazy_build=*/true);
  } else if (direct_lstable_store == nullptr) {
    direct_lstable_store = SnapshotColumnStore(context, table, projection,
                                               qualified_schema.ColumnCount(),
                                               /*allow_lazy_build=*/true);
  }
  // A string's byte length is already available from the column's offsets.
  // Preserve the bytecode child for row/arena fallbacks and recognize only
  // this exact context-free operation for the direct column scan.
  std::vector<std::optional<slot_t>> byte_length_columns(
      plan.aggregates.size());
  bool direct_children = true;
  for (size_t i = 0; i < plan.aggregates.size(); ++i) {
    const auto& input = plan.aggregates[i];
    if (!input.child) {
      continue;
    }
    Expression child = input.aggregate->Child();
    if (child && child->Type() == TypeTag::kCastExp &&
        child->AsCastExpression().TargetValueType() == ValueType::kDouble) {
      child = child->AsCastExpression().Child();
    }
    if (plan.slim && child && child->Type() == TypeTag::kFunctionCallExp &&
        !input.aggregate->Distinct()) {
      const auto& function = child->AsFunctionCallExpression();
      if (IdentifierEquals(function.FuncName(), "byte_length") &&
          function.Args().size() == 1 &&
          function.Args()[0]->Type() == TypeTag::kColumnValue) {
        const auto column = LocalColumnOffset(
            qualified_schema,
            function.Args()[0]->AsColumnValue().GetColumnName());
        if (column &&
            qualified_schema.GetColumn(*column).Type() == ValueType::kVarChar) {
          byte_length_columns[i] = static_cast<slot_t>(*column);
        }
      }
    }
    direct_children = direct_children && byte_length_columns[i].has_value();
  }
  // The slim fold paths below stream raw cells through a fixed
  // std::array<Value, 8> per-row scratch indexed by aggregate slot.  Plans
  // with more than 8 aggregates must keep the generic path (or they write
  // past the array); the non-slim folds never touch the scratch and stay
  // eligible at any aggregate count.
  if (direct_lstable_store != nullptr && cached_relation == nullptr &&
      !fill_cache && !plan.null_filters.empty()) {
    std::erase_if(plan.null_filters, [&](const TypedNullCompare& nf) {
      if (nf.is_null) {
        return false;
      }
      const slot_t full_c =
          projection.empty() ? nf.column : projection[nf.column];
      return full_c < direct_lstable_store->column_has_null.size() &&
             direct_lstable_store->column_has_null[full_c] == 0;
    });
    if (plan.null_filters.empty() && plan.only_null_filter_programs) {
      plan.filter_programs.clear();
    }
  }
  const bool use_direct_lstable =
      direct_lstable_store != nullptr && cached_relation == nullptr &&
      !fill_cache && plan.filter_programs.empty() &&
      plan.null_filters.empty() && direct_children &&
      (!plan.slim || plan.aggregates.size() <= 8);
  if (use_direct_lstable && !having_expr && stream_runtime_filter == nullptr &&
      stream_prebuilt_membership == nullptr) {
    if (!reusable) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, unique_result,
          TryUniqueGroupLimit(*direct_lstable_store, plan, stmt, projection,
                              qualified_schema, runtime));
      if (unique_result) {
        return std::move(*unique_result);
      }
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, dictionary_result,
        TryDictionaryAggregate(*direct_lstable_store, plan, stmt, projection,
                               qualified_schema, runtime));
    if (dictionary_result) {
      return std::move(*dictionary_result);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, composite_result,
        TryCompositeDictionaryAggregate(*direct_lstable_store, plan, stmt,
                                        projection, qualified_schema, runtime));
    if (composite_result) {
      return std::move(*composite_result);
    }
    if (!reusable) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, partitioned_result,
          TryPartitionedNumericTopN(*direct_lstable_store, plan, stmt,
                                    projection, qualified_schema, runtime));
      if (partitioned_result) {
        return std::move(*partitioned_result);
      }
    }
  }
  if (direct_lstable_store != nullptr && cached_relation == nullptr &&
      !fill_cache && stream_runtime_filter == nullptr &&
      stream_prebuilt_membership == nullptr &&
      (plan.filter_programs.empty() || plan.only_null_filter_programs)) {
    std::vector<slot_t> fast_proj_to_full(qualified_schema.ColumnCount());
    std::vector<ValueType> fast_proj_types(qualified_schema.ColumnCount());
    std::vector<uint8_t> fast_proj_unsigned(qualified_schema.ColumnCount(), 0);
    for (slot_t c = 0; c < qualified_schema.ColumnCount(); ++c) {
      fast_proj_to_full[c] = projection.empty() ? c : projection[c];
      fast_proj_types[c] = qualified_schema.GetColumn(c).Type();
      fast_proj_unsigned[c] =
          qualified_schema.GetColumn(c).IsUnsigned() ? 1 : 0;
    }
    const size_t early_workers = std::max<size_t>(
        1, std::min<size_t>(
               {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                std::max<size_t>(1, direct_lstable_store->blocks.size()),
                size_t{16}}));
    ASSIGN_OR_RETURN(
        std::optional<Relation>, scalar_fast_result,
        TryRunDirectLSTableScalarFast(
            *direct_lstable_store, plan, stmt, having_expr, fast_proj_to_full,
            fast_proj_types, fast_proj_unsigned, qualified_schema, table_schema,
            direct_children, stream_runtime_filter, stream_prebuilt_membership,
            early_workers, runtime, publish_lstable_marker, fold_cache_key,
            scan_begin));
    if (scalar_fast_result) {
      phase("lstable-scalar-fast", early_workers, 1);
      return std::move(*scalar_fast_result);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, bounded_fast_result,
        TryRunDirectLSTableBoundedIntFast(
            *direct_lstable_store, plan, stmt, having_expr, context, ctes,
            fast_proj_to_full, fast_proj_types, fast_proj_unsigned,
            qualified_schema, table_schema, direct_children,
            stream_runtime_filter, stream_prebuilt_membership, early_workers,
            runtime, publish_lstable_marker, fold_cache_key, scan_begin));
    if (bounded_fast_result) {
      phase("lstable-bounded-int-fast", early_workers,
            bounded_fast_result->rows.size());
      return std::move(*bounded_fast_result);
    }
  }
  if (!tried_columnar_tlcs && cached_relation == nullptr && !fill_cache &&
      stream_runtime_filter == nullptr &&
      stream_prebuilt_membership == nullptr) {
    ASSIGN_OR_RETURN(
        std::optional<Relation>, columnar_tlcs_result,
        TryRunColumnarTlcsAggregate(
            context, stmt, having_expr, table, projection, qualified_schema,
            table_schema, &plan, aggregate_expressions, {},
            direct_lstable_store.get(), runtime, publish_lstable_marker,
            fold_cache_key, scan_begin));
    if (columnar_tlcs_result) {
      phase("columnar-tlcs-agg",
            QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
            columnar_tlcs_result->rows.size());
      return std::move(*columnar_tlcs_result);
    }
  }
  Status morsel_status = Status::kSuccess;
  const std::vector<Table::ScanMorsel> morsels =
      (use_direct_lstable || cached_relation != nullptr)
          ? std::vector<Table::ScanMorsel>{}
          : table.BuildScanMorsels(context.txn_, 8, &morsel_status);
  RETURN_IF_FAIL(morsel_status);
  const std::vector<IntegerPeekCompare>* peek_ptr =
      fill_cache || peek_compares.empty() ? nullptr : &peek_compares;
  const bool compares_covered_by_peeks =
      !fill_cache && peek_ptr != nullptr &&
      std::ranges::all_of(plan.filter_compares, [&](const auto& compare) {
        if (!compare.constant_payload || compare.is_double ||
            compare.string_payload ||
            compare.left >= qualified_schema.ColumnCount()) {
          return false;
        }
        const slot_t column =
            projection.empty() ? compare.left : projection[compare.left];
        return std::ranges::any_of(peek_compares, [&](const auto& peek) {
          return peek.column == column && peek.op == compare.op &&
                 peek.constant == compare.int_constant;
        });
      });
  const bool bulk_count =
      !fill_cache && stream_prebuilt_membership == nullptr && plan.slim &&
      plan.key_offsets.empty() && plan.filter_programs.empty() &&
      plan.filter_disjunctions.empty() && plan.inlist_compares.empty() &&
      std::ranges::all_of(plan.aggregates,
                          [](const auto& input) {
                            return IsCountStar(*input.aggregate) &&
                                   !input.aggregate->Distinct();
                          }) &&
      std::ranges::all_of(plan.filter_compares, [&](const auto& compare) {
        if (!compare.constant_payload || compare.is_double ||
            compare.string_payload ||
            compare.left >= qualified_schema.ColumnCount()) {
          return false;
        }
        const slot_t column =
            projection.empty() ? compare.left : projection[compare.left];
        return std::ranges::any_of(peek_compares, [&](const auto& peek) {
          return peek.column == column && peek.op == compare.op &&
                 peek.constant == compare.int_constant;
        });
      });
  // A/B hatch: TINYLAMB_DISABLE_FILL_COPY=1 restores move-append into the
  // cache-fill buffer.  Copying FIXED-WIDTH fills keeps the scan iterator's
  // decode buffer alive (no per-row vector reallocation for 6M+ rows), the
  // same trade ParallelScan makes for its chunk appends; VARCHAR/ARRAY cells
  // keep the move so long text never deep-copies.
  static const bool fill_copy_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FILL_COPY") != nullptr;
  bool fill_copy = !fill_copy_disabled;
  for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
    const ValueType type = qualified_schema.GetColumn(i).Type();
    if (type == ValueType::kVarChar || type == ValueType::kArray) {
      fill_copy = false;
      break;
    }
  }

  const auto match_disjunctions = [&](auto&& compare) {
    return std::ranges::all_of(
        plan.filter_disjunctions, [&](const auto& branches) {
          return std::ranges::any_of(branches, [&](const auto& conjunction) {
            return std::ranges::all_of(conjunction, compare);
          });
        });
  };

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
      keep = match_disjunctions([&](const TypedFilterCompare& compare) {
        const Value& left = row[compare.left];
        const Value& right =
            row[compare.constant_payload ? compare.left : compare.right];
        return !left.IsNull() && !right.IsNull() &&
               DecideTypedFilterCompare(left, right, compare);
      });
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
          matched = MatchTypedStringInList(cell.value.varchar_value, list);
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
    } else if (packed_varchar_key) {
      uint64_t packed = 0;
      bool can_pack = true;
      for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
        const Value& key_cell = row[plan.key_offsets[k]];
        uint32_t part = 0;
        if (key_cell.IsNull() || key_cell.type != ValueType::kVarChar ||
            key_cell.IsCaseInsensitive() ||
            !TryPackShortVarcharSpan(key_cell.value.varchar_value, &part)) {
          can_pack = false;
          break;
        }
        packed |= static_cast<uint64_t>(part) << (k * 32);
      }
      if (can_pack) {
        size_t* slot =
            local->RawIntSlot(static_cast<int64_t>(packed), &inserted);
        if (inserted) {
          scratch->clear();
          for (const slot_t offset : plan.key_offsets) {
            scratch->push_back(CanonicalDistinctValue(row[offset]));
          }
          group_index = local->ClaimSlotForNewGroup(
              slot, *scratch, plan.aggregates.size(), aggregate_expressions);
        } else {
          group_index = *slot;
        }
      } else {
        scratch->clear();
        for (const slot_t offset : plan.key_offsets) {
          scratch->push_back(CanonicalDistinctValue(row[offset]));
        }
        group_index = local->FindOrInsert(*scratch, 0, plan.aggregates.size(),
                                          aggregate_expressions, &inserted);
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
        } else if (input.conditional.has_value()) {
          evaluated = EvaluateTypedConditionalOnRow(*input.conditional, row);
          value = &evaluated;
        } else if (input.double_expr.has_value()) {
          evaluated = EvaluateTypedDoubleExprOnRow(*input.double_expr, row);
          value = &evaluated;
        } else if (input.int_expr.has_value()) {
          StatusOr<Value> iv = EvaluateTypedIntExprOnRow(*input.int_expr, row);
          if (!iv.HasValue()) {
            *error = iv.GetStatus();
            return;
          }
          evaluated = iv.MoveValue();
          value = &evaluated;
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
      if (input.conditional.has_value()) {
        if (Status st = accumulator.Add(
                EvaluateTypedConditionalOnRow(*input.conditional, row));
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.double_expr.has_value()) {
        if (Status st = accumulator.Add(
                EvaluateTypedDoubleExprOnRow(*input.double_expr, row));
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.int_expr.has_value()) {
        StatusOr<Value> iv = EvaluateTypedIntExprOnRow(*input.int_expr, row);
        if (!iv.HasValue()) {
          *error = iv.GetStatus();
          return;
        }
        if (Status st = accumulator.Add(iv.MoveValue());
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
      if (value.type == ValueType::kInt64 && batch.slot_unsigned[c]) {
        value.SetUnsigned();
      }
      return value;
    };
    const auto is_null = [&](slot_t c) { return batch.nulls[base + c] != 0; };
    const int64_t* row_cells = batch.cells.data() + base;
    const bool filter_compares_done_in_scan = compares_covered_by_peeks;
    bool keep = true;
    if (!filter_compares_done_in_scan) {
      for (const TypedFilterCompare& compare : plan.filter_compares) {
        if (!DecideTypedFilterCompareCells(compare, row_cells, is_null,
                                           make_value)) {
          keep = false;
          break;
        }
      }
    }
    if (keep) {
      keep = match_disjunctions([&](const TypedFilterCompare& compare) {
        return DecideTypedFilterCompareCells(compare, row_cells, is_null,
                                             make_value);
      });
    }
    if (keep) {
      for (const TypedInListCompare& list : plan.inlist_compares) {
        if (is_null(list.column)) {
          keep = false;
          break;
        }
        bool matched = false;
        if (list.is_string) {
          matched = MatchTypedStringInList(
              make_value(list.column).value.varchar_value, list);
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
      const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_key);
      if (local->mru_groups[mru_idx] != SIZE_MAX &&
          local->mru_keys[mru_idx] == raw_key) {
        group_index = local->mru_groups[mru_idx];
      } else {
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
        local->mru_keys[mru_idx] = raw_key;
        local->mru_groups[mru_idx] = group_index;
      }
    } else if (packed_varchar_key) {
      uint64_t packed = 0;
      bool can_pack = true;
      for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
        const slot_t offset = plan.key_offsets[k];
        uint32_t part = 0;
        if (is_null(offset) ||
            !TryPackShortVarcharSpan(batch.Varchar(r, varchar_ordinal[offset]),
                                     &part)) {
          can_pack = false;
          break;
        }
        packed |= static_cast<uint64_t>(part) << (k * 32);
      }
      if (can_pack) {
        const auto raw_packed = static_cast<int64_t>(packed);
        const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_packed);
        if (local->mru_groups[mru_idx] != SIZE_MAX &&
            local->mru_keys[mru_idx] == raw_packed) {
          group_index = local->mru_groups[mru_idx];
        } else {
          size_t* slot = local->RawIntSlot(raw_packed, &inserted);
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
          local->mru_keys[mru_idx] = raw_packed;
          local->mru_groups[mru_idx] = group_index;
        }
      } else {
        scratch->clear();
        for (const slot_t offset : plan.key_offsets) {
          if (is_null(offset)) {
            scratch->emplace_back();
          } else {
            scratch->push_back(CanonicalDistinctValue(make_value(offset)));
          }
        }
        group_index = local->FindOrInsert(*scratch, 0, plan.aggregates.size(),
                                          aggregate_expressions, &inserted);
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
      // Raw-cell fast path: the common payload kinds (COUNT / SUM / AVG /
      // COUNT(DISTINCT) over fixed-width cells) read at most one column cell
      // and fold straight from batch.cells without boxing a Value per input.
      // Expression inputs are the exception: double_expr goes through
      // EvaluateTypedDoubleExprCells, whose kGeneric shape reads several
      // cells (up to 16 ops) and whose 2-3-column kinds read multiple cells.
      // NULL inputs skip exactly like the generic path (ApplySlimValue
      // returns early on IsNull for every kind); a kind or cell type the raw
      // switch does not cover falls through to the generic boxed call, which
      // raises the same type-mismatch errors.
      const std::vector<SlimAggKind>& kinds = *local->slim_kinds;
      std::array<Value, 8>& stack_values = *row_values;
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const TypedAggregateInput& input = plan.aggregates[i];
        const SlimAggKind kind = kinds[i];
        if (input.double_expr.has_value()) {
          double addend = 0.0;
          if (!EvaluateTypedDoubleExprCells(*input.double_expr, row_cells,
                                            is_null, &addend)) {
            continue;
          }
          if (kind == SlimAggKind::kSumDouble) {
            if (std::isinf(addend)) {
              local->SlimFlags(group_index, i) |= 1;
            }
            local->SlimDoubleTotal(group_index, i) += addend;
            ++local->SlimCount(group_index, i);
            continue;
          }
          if (kind == SlimAggKind::kAvgDouble) {
            local->SlimDoubleTotal(group_index, i) += addend;
            ++local->SlimCount(group_index, i);
            continue;
          }
          stack_values[i] = Value(addend);
          if (!ApplySlimValue(local, group_index, i, kind, &stack_values[i],
                              error)) {
            return;
          }
          continue;
        }
        if (input.int_expr.has_value()) {
          int64_t addend = 0;
          if (!EvaluateTypedIntExprCells(*input.int_expr, row_cells, is_null,
                                         &addend, error)) {
            if (*error != Status::kSuccess) {
              return;
            }
            continue;
          }
          stack_values[i] = Value(addend);
          if (!ApplySlimValue(local, group_index, i, kind, &stack_values[i],
                              error)) {
            return;
          }
          continue;
        }
        ValueType cell_type = ValueType::kNull;
        int64_t cell = 0;
        bool cell_unsigned = false;
        std::optional<slot_t> fallback_col;
        if (input.conditional.has_value()) {
          if (!DecideTypedFilterCompareCells(input.conditional->condition,
                                             row_cells, is_null, make_value)) {
            if (!input.conditional->has_else_constant) {
              continue;
            }
            cell_type = input.conditional->result_type;
            cell = input.conditional->else_int_constant;
          } else if (input.conditional->then_column.has_value()) {
            const slot_t col = *input.conditional->then_column;
            if (is_null(col)) {
              continue;
            }
            cell_type = batch.slot_types[col];
            cell = row_cells[col];
            cell_unsigned = batch.slot_unsigned[col];
            fallback_col = col;
          } else {
            cell_type = input.conditional->result_type;
            cell = input.conditional->then_int_constant;
          }
        } else if (input.column.has_value()) {
          const slot_t column = *input.column;
          if (is_null(column)) {
            continue;
          }
          cell_type = batch.slot_types[column];
          cell = row_cells[column];
          cell_unsigned = batch.slot_unsigned[column];
          fallback_col = column;
        } else {
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
        bool handled = true;
        if (!ApplySlimCell(local, group_index, i, kind, cell_type, cell,
                           cell_unsigned, &handled, error)) {
          return;
        }
        if (handled) {
          continue;
        }
        if (fallback_col.has_value()) {
          stack_values[i] = make_value(*fallback_col);
        } else {
          stack_values[i] = Value(cell);
          stack_values[i].type = cell_type;
        }
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
      if (input.conditional.has_value()) {
        Value value;
        if (DecideTypedFilterCompareCells(input.conditional->condition,
                                          row_cells, is_null, make_value)) {
          if (input.conditional->then_column.has_value()) {
            const slot_t col = *input.conditional->then_column;
            if (!is_null(col)) {
              value = make_value(col);
            }
          } else {
            value = Value(input.conditional->then_int_constant);
            value.type = input.conditional->result_type;
          }
        } else if (input.conditional->has_else_constant) {
          value = Value(input.conditional->else_int_constant);
        }
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.double_expr.has_value()) {
        double dbl = 0.0;
        const Value value = EvaluateTypedDoubleExprCells(
                                *input.double_expr, row_cells, is_null, &dbl)
                                ? Value(dbl)
                                : Value();
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.int_expr.has_value()) {
        int64_t iv = 0;
        const bool ok = EvaluateTypedIntExprCells(*input.int_expr, row_cells,
                                                  is_null, &iv, error);
        if (*error != Status::kSuccess) {
          return;
        }
        const Value value = ok ? Value(iv) : Value();
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
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_AGG_ARENA") != nullptr;
  std::shared_ptr<BaseRelationArena> fill_arena;
  std::vector<int64_t> arena_cell_buffer;
  std::vector<char> arena_null_buffer;
  // Typed fill: the scan decodes schema-typed cell
  // batches instead of Rows, the fold consumes raw cells, and the cache is
  // published with the arena only (rows are materialized from it on demand by
  // consumers that cannot take the arena path). Non-reusable scans fold the
  // same decoded batches without mirroring them. Programs needing a Row use
  // owned scratch cells and the existing filter/accumulator evaluator.
  static const bool typed_fill_enabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TYPED_FILL") == nullptr;
  bool typed_fill = false;
  if (!arena_disabled) {
    bool all_typed = qualified_schema.ColumnCount() > 0;
    for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
      const ValueType type = qualified_schema.GetColumn(i).Type();
      // Row-fill mirrors contain fixed-width cells only. VARCHAR requires
      // the typed decoder's byte arenas and cumulative end offsets.
      // Interval columns are VARCHAR cells carrying an interval tag that the
      // typed arena/batch boxing would drop (Value::IsInterval reads
      // collation_), so keep them on the Row path that preserves the tag.
      if ((type == ValueType::kVarChar && !typed_fill_enabled) ||
          (type != ValueType::kInt64 && type != ValueType::kDate &&
           type != ValueType::kDouble && type != ValueType::kVarChar) ||
          qualified_schema.GetColumn(i).IsInterval()) {
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
      // slot_types is final; build the shared lookup cache before any
      // concurrent reader (filter/boxing workers) touches the arena.
      fill_arena->PrepareVarcharOrdinals();
      arena_cell_buffer.resize(fill_arena->stride);
      arena_null_buffer.resize(fill_arena->stride);
      varchar_ordinal.assign(fill_arena->stride, SIZE_MAX);
      size_t varchar_ordinal_next = 0;
      for (size_t i = 0; i < fill_arena->stride; ++i) {
        if (fill_arena->slot_types[i] == ValueType::kVarChar) {
          varchar_ordinal[i] = varchar_ordinal_next++;
        }
      }
      typed_fill = typed_fill_enabled;
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

  const size_t effective_morsels =
      use_direct_lstable
          ? std::max<size_t>(1, direct_lstable_store->blocks.size())
          : morsels.size();
  auto worker_count = std::min<size_t>(
      {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
       cached_relation != nullptr ? size_t{16} : effective_morsels,
       size_t{16}});
  if (worker_count < 2) {
    worker_count = 1;
  }
  // Floating-point SUM/AVG over workers merges partial sums with a blind `+=`,
  // which reassociates the additions and can differ from the canonical
  // row-order accumulator (e.g. 2^53 + many small terms). Keep DOUBLE sums on
  // one worker so the fold order matches the AST ground truth.
  // A checked SUM(INT64) is order-sensitive the same way: per-worker partial
  // accumulators start at 0, so a range-local running sum that wraps
  // fabricates "integer overflow in SUM" the row-order canonical never
  // raises, and an overflow that only materializes across range boundaries
  // goes undetected (each partial stays inside INT64). The checked add lives
  // both in the per-row fold and in the worker-order merge, so splitting is
  // unsound for it in every shape; keep the fold on one worker (COUNT/MIN/
  // MAX stay parallel-capable kinds).
  bool bounded = false;
  size_t topn_capacity = 0;
  if (!stmt.Distinct() && stmt.HasLimit() && !stmt.WithTies() &&
      stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()) {
    bounded = true;
    topn_capacity = stmt.Offset() + stmt.Limit();
  }
  // A/B hatch: TINYLAMB_DISABLE_HAVING_FAST=1 restores the per-group
  // AggregateResultMap path for every HAVING shape.
  static const bool having_fast_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_HAVING_FAST") != nullptr;
  std::vector<PositionalHaving> positional_having;
  if (having_expr != nullptr && !having_fast_disabled) {
    if (!TryCompilePositionalHavingConjuncts(having_expr, plan.aggregates,
                                             &positional_having)) {
      positional_having.clear();
    }
  }
  // A/B hatch: TINYLAMB_DISABLE_FLAT_GROUP_FOLD=1 restores the per-group
  // vector-of-vector group storage.
  static const bool flat_fold_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FLAT_GROUP_FOLD") !=
      nullptr;
  const slot_t early_mono_full_key =
      (int_key && plan.key_offsets.size() == 1)
          ? (projection.empty() ? plan.key_offsets[0]
                                : projection[plan.key_offsets[0]])
          : 0;
  const bool early_monotonic_int_key =
      use_direct_lstable && direct_lstable_store->total_rows >= 65536 &&
      stream_prebuilt_membership == nullptr && int_key &&
      plan.key_offsets.size() == 1 && !flat_fold_disabled && plan.slim &&
      plan.order_items.empty() && plan.dependent_key_programs.empty() &&
      !bounded && (!having_expr || !positional_having.empty()) &&
      early_mono_full_key < direct_lstable_store->column_is_monotonic.size() &&
      direct_lstable_store->column_is_monotonic[early_mono_full_key] != 0 &&
      direct_lstable_store->column_has_null[early_mono_full_key] == 0;
  if (plan.slim && std::ranges::any_of(plan.slim_kinds, [&](SlimAggKind k) {
        if (k == SlimAggKind::kSumInt64) {
          return true;
        }
        return !early_monotonic_int_key &&
               (k == SlimAggKind::kSumDouble || k == SlimAggKind::kAvgDouble);
      })) {
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
  std::vector<TypedWorkerGroups> workers(worker_count);
  for (TypedWorkerGroups& local : workers) {
    local.int_key = int_key;
    local.packed_varchar_key = packed_varchar_key;
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
    bool fold_can_stream_arena =
        workers[0].slim && int_key && plan.filter_disjunctions.empty() &&
        plan.filter_programs.empty() && plan.aggregates.size() <= 8;
    if (fold_can_stream_arena) {
      for (const TypedAggregateInput& input : plan.aggregates) {
        const auto aggregate_index =
            static_cast<size_t>(&input - plan.aggregates.data());
        if (input.child.has_value() ||
            (!input.column.has_value() && !input.conditional.has_value() &&
             !input.double_expr.has_value() && !input.int_expr.has_value() &&
             (*workers[0].slim_kinds)[aggregate_index] !=
                 SlimAggKind::kCount)) {
          fold_can_stream_arena = false;
          break;
        }
      }
    }
    if (!fold_can_stream_arena && cached_relation->rows.empty() &&
        !cached_relation->HasColumnarOnly()) {
      RETURN_IF_FAIL(MaterializeCacheRowsParallel(
          fold_cache_key, cached_relation.get(), runtime));
    }
  }
  std::vector<Status> worker_errors(worker_count, Status::kSuccess);
  // Row-level instrumentation must not make all workers invalidate the same
  // cache line. Keep each worker's frequently updated counters together and
  // isolate them from its neighbors; publish totals only after the join.
  struct alignas(64) WorkerCounters {
    size_t scanned{0};
    size_t emitted{0};
    size_t updates{0};
    size_t dictionary_rows{0};
  };
  std::vector<WorkerCounters> counters(worker_count);
  std::vector<size_t> reclaimed_groups(worker_count, 0);

  std::vector<slot_t> proj_to_full(qualified_schema.ColumnCount());
  std::vector<ValueType> proj_types(qualified_schema.ColumnCount());
  std::vector<uint8_t> proj_unsigned(qualified_schema.ColumnCount(), 0);
  for (slot_t c = 0; c < qualified_schema.ColumnCount(); ++c) {
    proj_to_full[c] = projection.empty() ? c : projection[c];
    proj_types[c] = qualified_schema.GetColumn(c).Type();
    proj_unsigned[c] = qualified_schema.GetColumn(c).IsUnsigned() ? 1 : 0;
  }

  // A scalar fold over one integer interval needs neither boxed rows nor a
  // per-cell predicate/aggregate dispatcher. Keep the original block and row
  // order, including NULL filtering and SUM's infinity provenance bit.
  bool scalar_interval =
      use_direct_lstable && stream_runtime_filter == nullptr &&
      stream_prebuilt_membership == nullptr && plan.slim &&
      plan.key_offsets.empty() && plan.aggregates.size() == 1 &&
      plan.filter_programs.empty() && plan.filter_disjunctions.empty() &&
      plan.inlist_compares.empty() && compares_covered_by_peeks &&
      peek_ptr != nullptr && !peek_ptr->empty();
  int64_t interval_low = std::numeric_limits<int64_t>::min();
  int64_t interval_high = std::numeric_limits<int64_t>::max();
  bool interval_empty = false;
  slot_t interval_column = 0;
  std::optional<slot_t> scalar_value_column;
  SlimAggKind scalar_kind = SlimAggKind::kNone;
  if (scalar_interval) {
    interval_column = peek_ptr->front().column;
    scalar_kind = plan.slim_kinds[0];
    const auto& argument = plan.aggregates[0];
    if (scalar_kind == SlimAggKind::kCount &&
        IsCountStar(*argument.aggregate)) {
      // COUNT(*) only consumes the filter column.
    } else if ((scalar_kind == SlimAggKind::kSumDouble ||
                scalar_kind == SlimAggKind::kAvgDouble) &&
               argument.column &&
               proj_types[*argument.column] == ValueType::kDouble) {
      scalar_value_column = proj_to_full[*argument.column];
    } else {
      scalar_interval = false;
    }
    for (const auto& compare : *peek_ptr) {
      if (compare.column != interval_column) {
        scalar_interval = false;
        break;
      }
      const int64_t value = compare.constant;
      switch (compare.op) {
        case BinaryOperation::kEquals:
          interval_low = std::max(interval_low, value);
          interval_high = std::min(interval_high, value);
          break;
        case BinaryOperation::kGreaterThanEquals:
          interval_low = std::max(interval_low, value);
          break;
        case BinaryOperation::kGreaterThan:
          if (value == std::numeric_limits<int64_t>::max()) {
            interval_empty = true;
          } else {
            interval_low = std::max(interval_low, value + 1);
          }
          break;
        case BinaryOperation::kLessThanEquals:
          interval_high = std::min(interval_high, value);
          break;
        case BinaryOperation::kLessThan:
          if (value == std::numeric_limits<int64_t>::min()) {
            interval_empty = true;
          } else {
            interval_high = std::min(interval_high, value - 1);
          }
          break;
        default:
          scalar_interval = false;
          break;
      }
    }
    interval_empty = interval_empty || interval_low > interval_high;
  }

  // Lean proof: lean/TinylambOpt/ResearchProofs/Wave8H2o.lean
  // (InRangeSynopsis_sound).
  // A scalar fold over one integer IN-list AND one integer interval on a
  // direct LSTableStore avoids boxed rows and per-cell predicate/aggregate
  // dispatch while preserving block zone-map/bloom skipping, NULL semantics,
  // and signed/floating-point overflow/infinity provenance.
  struct ScalarInRangeAgg {
    SlimAggKind kind{SlimAggKind::kNone};
    std::optional<slot_t> full_column;
  };
  bool scalar_in_range_sum =
      !scalar_interval && use_direct_lstable &&
      stream_runtime_filter == nullptr &&
      stream_prebuilt_membership == nullptr && plan.slim &&
      plan.key_offsets.empty() && !plan.aggregates.empty() &&
      plan.aggregates.size() <= 4 && plan.filter_programs.empty() &&
      plan.filter_disjunctions.empty() && plan.inlist_compares.size() == 1 &&
      compares_covered_by_peeks && peek_ptr != nullptr && !peek_ptr->empty();
  slot_t in_range_in_col = 0;
  slot_t in_range_range_col = 0;
  int64_t in_range_low = std::numeric_limits<int64_t>::min();
  int64_t in_range_high = std::numeric_limits<int64_t>::max();
  bool in_range_empty = false;
  int64_t in_range_in_min = 0;
  int64_t in_range_in_max = 0;
  uint64_t in_range_in_mask = 0;
  bool in_range_use_mask = false;
  std::vector<int64_t> in_range_in_vals;
  std::vector<ScalarInRangeAgg> in_range_aggs;
  if (scalar_in_range_sum) {
    const TypedInListCompare& inlist = plan.inlist_compares[0];
    if (inlist.is_string || inlist.is_double || inlist.int_values.empty() ||
        inlist.column >= qualified_schema.ColumnCount() ||
        proj_unsigned[inlist.column] != 0 ||
        (proj_types[inlist.column] != ValueType::kInt64 &&
         proj_types[inlist.column] != ValueType::kDate)) {
      scalar_in_range_sum = false;
    } else {
      in_range_in_col = proj_to_full[inlist.column];
      in_range_in_vals = inlist.int_values;
      in_range_in_min = in_range_in_vals[0];
      in_range_in_max = in_range_in_vals[0];
      for (const int64_t v : in_range_in_vals) {
        in_range_in_min = std::min(in_range_in_min, v);
        in_range_in_max = std::max(in_range_in_max, v);
      }
      const uint64_t span = static_cast<uint64_t>(in_range_in_max) -
                            static_cast<uint64_t>(in_range_in_min);
      if (span < 64ULL) {
        in_range_use_mask = true;
        in_range_in_mask = 0;
        for (const int64_t v : in_range_in_vals) {
          in_range_in_mask |=
              (uint64_t{1} << (static_cast<uint64_t>(v) -
                               static_cast<uint64_t>(in_range_in_min)));
        }
      } else if (in_range_in_vals.size() > 16) {
        scalar_in_range_sum = false;
      }
    }
    if (scalar_in_range_sum) {
      in_range_range_col = peek_ptr->front().column;
      for (const auto& compare : *peek_ptr) {
        if (compare.column != in_range_range_col) {
          scalar_in_range_sum = false;
          break;
        }
        const int64_t value = compare.constant;
        switch (compare.op) {
          case BinaryOperation::kEquals:
            in_range_low = std::max(in_range_low, value);
            in_range_high = std::min(in_range_high, value);
            break;
          case BinaryOperation::kGreaterThanEquals:
            in_range_low = std::max(in_range_low, value);
            break;
          case BinaryOperation::kGreaterThan:
            if (value == std::numeric_limits<int64_t>::max()) {
              in_range_empty = true;
            } else {
              in_range_low = std::max(in_range_low, value + 1);
            }
            break;
          case BinaryOperation::kLessThanEquals:
            in_range_high = std::min(in_range_high, value);
            break;
          case BinaryOperation::kLessThan:
            if (value == std::numeric_limits<int64_t>::min()) {
              in_range_empty = true;
            } else {
              in_range_high = std::min(in_range_high, value - 1);
            }
            break;
          default:
            scalar_in_range_sum = false;
            break;
        }
      }
      in_range_empty = in_range_empty || in_range_low > in_range_high;
    }
    if (scalar_in_range_sum) {
      in_range_aggs.reserve(plan.aggregates.size());
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const auto& input = plan.aggregates[i];
        const SlimAggKind kind = plan.slim_kinds[i];
        if (input.conditional.has_value() || input.double_expr.has_value() ||
            input.coalesce_double.has_value() || input.int_expr.has_value() ||
            input.child.has_value()) {
          scalar_in_range_sum = false;
          break;
        }
        if (kind == SlimAggKind::kCount && IsCountStar(*input.aggregate)) {
          in_range_aggs.push_back({kind, std::nullopt});
        } else if (input.column.has_value() &&
                   *input.column < qualified_schema.ColumnCount() &&
                   ((kind == SlimAggKind::kSumInt64 &&
                     proj_unsigned[*input.column] == 0 &&
                     (proj_types[*input.column] == ValueType::kInt64 ||
                      proj_types[*input.column] == ValueType::kDate)) ||
                    ((kind == SlimAggKind::kSumDouble ||
                      kind == SlimAggKind::kAvgDouble) &&
                     proj_types[*input.column] == ValueType::kDouble))) {
          in_range_aggs.push_back({kind, proj_to_full[*input.column]});
        } else {
          scalar_in_range_sum = false;
          break;
        }
      }
    }
  }

  const bool monotonic_int_key =
      use_direct_lstable && direct_lstable_store->total_rows >= 65536 &&
      stream_prebuilt_membership == nullptr && int_key &&
      plan.key_offsets.size() == 1 && !flat_fold_disabled && plan.slim &&
      plan.order_items.empty() && plan.dependent_key_programs.empty() &&
      !bounded && (!having_expr || !positional_having.empty()) &&
      proj_to_full[plan.key_offsets[0]] <
          direct_lstable_store->column_is_monotonic.size() &&
      direct_lstable_store
              ->column_is_monotonic[proj_to_full[plan.key_offsets[0]]] != 0 &&
      direct_lstable_store
              ->column_has_null[proj_to_full[plan.key_offsets[0]]] == 0;

  bool dict_group_key = false;
  size_t wide_group_dictionary_size = 0;
  if (use_direct_lstable && plan.key_offsets.size() == 1) {
    const auto& dictionary =
        direct_lstable_store->dictionaries[proj_to_full[plan.key_offsets[0]]];
    if (dictionary.wide_code_bytes != 0) {
      wide_group_dictionary_size = dictionary.entries.size();
    }
  }
  size_t dict_group_stride1 = 1;
  size_t dict_group_total_codes = 0;
  if (use_direct_lstable && !int_key && !plan.key_offsets.empty() &&
      plan.key_offsets.size() <= 2) {
    const slot_t full0 = proj_to_full[plan.key_offsets[0]];
    if (full0 < direct_lstable_store->dictionaries.size() &&
        direct_lstable_store->dictionaries[full0].active &&
        direct_lstable_store->dictionaries[full0].value_type ==
            ValueType::kVarChar &&
        direct_lstable_store->column_has_null[full0] == 0) {
      const size_t n0 =
          direct_lstable_store->dictionaries[full0].entries.size();
      if (plan.key_offsets.size() == 1 && n0 > 0 && n0 <= 256) {
        dict_group_key = true;
        dict_group_total_codes = n0;
      } else if (plan.key_offsets.size() == 2) {
        const slot_t full1 = proj_to_full[plan.key_offsets[1]];
        if (full1 < direct_lstable_store->dictionaries.size() &&
            direct_lstable_store->dictionaries[full1].active &&
            direct_lstable_store->dictionaries[full1].value_type ==
                ValueType::kVarChar &&
            direct_lstable_store->column_has_null[full1] == 0) {
          const size_t n1 =
              direct_lstable_store->dictionaries[full1].entries.size();
          if (n0 > 0 && n1 > 0 && n0 * n1 <= 256) {
            dict_group_key = true;
            dict_group_stride1 = n1;
            dict_group_total_codes = n0 * n1;
          }
        }
      }
    }
  }
  bool bounded_int_group_key = false;
  int64_t bounded_int_min = 0;
  size_t bounded_int_span = 0;
  if (use_direct_lstable && int_key && !monotonic_int_key &&
      plan.key_offsets.size() == 1 && plan.slim && !plan.aggregates.empty() &&
      plan.aggregates.size() <= 4 &&
      std::ranges::all_of(plan.slim_kinds, [](SlimAggKind k) {
        return k == SlimAggKind::kCount || k == SlimAggKind::kSumInt64 ||
               k == SlimAggKind::kSumDouble || k == SlimAggKind::kAvgDouble;
      })) {
    const slot_t full0 = proj_to_full[plan.key_offsets[0]];
    if (full0 < direct_lstable_store->column_has_null.size() &&
        direct_lstable_store->column_has_null[full0] == 0 &&
        full0 < direct_lstable_store->column_min_int.size() &&
        direct_lstable_store->column_min_int[full0] <=
            direct_lstable_store->column_max_int[full0]) {
      const int64_t lo = direct_lstable_store->column_min_int[full0];
      const int64_t hi = direct_lstable_store->column_max_int[full0];
      const uint64_t span =
          static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) + 1ULL;
      if (span > 0 && span <= 262144ULL) {
        bounded_int_group_key = true;
        bounded_int_min = lo;
        bounded_int_span = static_cast<size_t>(span);
      }
    }
  }

  struct LSTableAggDictFilter {
    slot_t full_column{0};
    std::array<uint64_t, 4> pass_mask{};
    std::array<uint8_t, 256> pass{};
  };
  std::vector<LSTableAggDictFilter> lstable_agg_dict_filters;
  QueryMemoryCharge wide_filter_charge;
  std::vector<std::optional<EncodedDictionaryPredicate>> wide_string_filters;
  std::vector<uint8_t> filter_compare_covered;
  std::vector<uint8_t> inlist_compare_covered;
  if (use_direct_lstable) {
    wide_string_filters.resize(plan.filter_compares.size());
    filter_compare_covered.assign(plan.filter_compares.size(), 0);
    inlist_compare_covered.assign(plan.inlist_compares.size(), 0);
    std::vector<int> col_to_df(table_schema.ColumnCount(), -1);
    auto get_or_init_df = [&](slot_t full_col) -> LSTableAggDictFilter& {
      if (col_to_df[full_col] < 0) {
        col_to_df[full_col] = static_cast<int>(lstable_agg_dict_filters.size());
        LSTableAggDictFilter df;
        df.full_column = full_col;
        const auto& dict = direct_lstable_store->dictionaries[full_col];
        const size_t n_entries = dict.value_type == ValueType::kVarChar
                                     ? dict.StringEntryCount()
                                     : (dict.value_type == ValueType::kDouble
                                            ? dict.DoubleEntryCount()
                                            : dict.IntEntryCount());
        for (size_t code = 0; code < n_entries; ++code) {
          df.pass[code] = 1;
          df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
        }
        lstable_agg_dict_filters.push_back(df);
      }
      return lstable_agg_dict_filters[static_cast<size_t>(col_to_df[full_col])];
    };
    // An earlier predicate can make evaluating a later dictionary in full
    // more expensive than testing its surviving rows. This is only a cost
    // estimate: declining a mask retains the ordinary per-row comparison.
    auto expected_filter_rows =
        static_cast<double>(direct_lstable_store->total_rows);
    for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
      const TypedFilterCompare& cmp = plan.filter_compares[fi];
      if (!cmp.constant_payload) {
        continue;
      }
      const slot_t full_col = proj_to_full[cmp.left];
      if (full_col >= direct_lstable_store->dictionaries.size()) {
        continue;
      }
      const auto& dictionary = direct_lstable_store->dictionaries[full_col];
      if (cmp.string_payload &&
          (cmp.op == BinaryOperation::kLike ||
           cmp.op == BinaryOperation::kNotLike) &&
          dictionary.wide_code_bytes != 0 &&
          static_cast<double>(dictionary.StringEntryCount()) <=
              expected_filter_rows / 2) {
        wide_string_filters[fi] = CompileDictionaryPredicate(
            *direct_lstable_store, cmp, projection, wide_filter_charge);
        if (wide_string_filters[fi]) {
          expected_filter_rows *= wide_string_filters[fi]->domain_fraction;
        }
        // Leave this predicate uncovered by the narrow-dictionary kernels.
        // The general direct-column loop consults this mask by row code.
      }
      if (!dictionary.active) {
        continue;
      }
      const auto& dict = direct_lstable_store->dictionaries[full_col];
      if (cmp.string_payload && dict.value_type == ValueType::kVarChar) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        const size_t dict_size = dict.StringEntryCount();
        for (size_t code = 0; code < dict_size; ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const bool ok =
              MatchTypedStringCompare(dict.StringEntryAt(code), cmp);
          if (!ok) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        filter_compare_covered[fi] = 1;
      } else if (cmp.is_double && dict.value_type == ValueType::kDouble) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        const auto double_entries = dict.DoubleEntries();
        for (size_t code = 0; code < double_entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const double a = double_entries[code];
          bool ok = false;
          switch (cmp.op) {
            case BinaryOperation::kEquals:
              ok = (a == cmp.double_constant);
              break;
            case BinaryOperation::kNotEquals:
              ok = (a != cmp.double_constant);
              break;
            case BinaryOperation::kLessThan:
              ok = (a < cmp.double_constant);
              break;
            case BinaryOperation::kLessThanEquals:
              ok = (a <= cmp.double_constant);
              break;
            case BinaryOperation::kGreaterThan:
              ok = (a > cmp.double_constant);
              break;
            default:
              ok = (a >= cmp.double_constant);
              break;
          }
          if (!ok) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        filter_compare_covered[fi] = 1;
      }
    }
    for (size_t li = 0; li < plan.inlist_compares.size(); ++li) {
      const TypedInListCompare& list = plan.inlist_compares[li];
      const slot_t full_col = proj_to_full[list.column];
      if (full_col >= direct_lstable_store->dictionaries.size() ||
          !direct_lstable_store->dictionaries[full_col].active) {
        continue;
      }
      const auto& dict = direct_lstable_store->dictionaries[full_col];
      if (list.is_string && dict.value_type == ValueType::kVarChar) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        const size_t dict_size = dict.StringEntryCount();
        for (size_t code = 0; code < dict_size; ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const std::string_view sv = dict.StringEntryAt(code);
          const bool matched = MatchTypedStringInList(sv, list);
          if (!matched) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        inlist_compare_covered[li] = 1;
      } else if (list.is_double && dict.value_type == ValueType::kDouble) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        const auto double_entries = dict.DoubleEntries();
        for (size_t code = 0; code < double_entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const double dv = double_entries[code];
          bool matched = false;
          for (const double item : list.double_values) {
            if (dv == item) {
              matched = true;
              break;
            }
          }
          if (!matched) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        inlist_compare_covered[li] = 1;
      } else if (!list.is_string && !list.is_double &&
                 dict.value_type == ValueType::kInt64) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        const auto int_entries = dict.IntEntries();
        for (size_t code = 0; code < int_entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const int64_t iv = int_entries[code];
          bool matched = false;
          for (const int64_t item : list.int_values) {
            if (iv == item) {
              matched = true;
              break;
            }
          }
          if (!matched) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        inlist_compare_covered[li] = 1;
      }
    }
  }

  struct FastDictDoubleTerm {
    TypedDoubleExprInput::Shape shape{TypedDoubleExprInput::Shape::kGeneric};
    TypedIntExprInput::Shape int_shape{TypedIntExprInput::Shape::kGeneric};
    bool is_int_term{false};
    bool byte_length{false};
    slot_t full_col0{0};
    slot_t full_col1{0};
    slot_t full_col2{0};
    double k0{0.0};
    double k1{0.0};
    size_t primary_agg_idx{0};
    bool reuse_prev_mul_const_minus{false};
  };
  bool fast_dict_double_agg = false;
  std::vector<FastDictDoubleTerm> fast_dict_terms;
  fast_dict_terms.reserve(plan.aggregates.size());
  std::vector<std::pair<size_t, size_t>> fast_dict_dup_aggs;
  fast_dict_dup_aggs.reserve(plan.aggregates.size());
  std::vector<std::pair<slot_t, bool>> fast_empty_filters;
  fast_empty_filters.reserve(plan.filter_compares.size());
  bool all_fast_dict_filters_covered = use_direct_lstable;
  if (use_direct_lstable) {
    for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
      if (filter_compare_covered[fi] != 0) {
        continue;
      }
      const TypedFilterCompare& cmp = plan.filter_compares[fi];
      if (bounded_int_group_key && cmp.constant_payload && cmp.string_payload &&
          cmp.string_constant.empty() &&
          (cmp.op == BinaryOperation::kEquals ||
           cmp.op == BinaryOperation::kNotEquals)) {
        fast_empty_filters.emplace_back(proj_to_full[cmp.left],
                                        cmp.op == BinaryOperation::kEquals);
        continue;
      }
      if (!cmp.constant_payload || cmp.is_double || cmp.string_payload) {
        all_fast_dict_filters_covered = false;
        break;
      }
      const slot_t full_col = proj_to_full[cmp.left];
      const bool in_peek = std::ranges::any_of(
          peek_compares, [&](const IntegerPeekCompare& peek) {
            return peek.column == full_col && peek.op == cmp.op &&
                   peek.constant == cmp.int_constant;
          });
      if (!in_peek) {
        all_fast_dict_filters_covered = false;
        break;
      }
    }
    for (size_t li = 0;
         all_fast_dict_filters_covered && li < plan.inlist_compares.size();
         ++li) {
      if (inlist_compare_covered[li] == 0) {
        all_fast_dict_filters_covered = false;
        break;
      }
    }
  }
  if (use_direct_lstable &&
      (dict_group_key || bounded_int_group_key || plan.key_offsets.empty()) &&
      plan.slim && plan.filter_programs.empty() &&
      plan.filter_disjunctions.empty() && all_fast_dict_filters_covered &&
      plan.aggregates.size() <= 8) {
    bool eligible = true;
    const auto col_no_null = [&](slot_t full_col) noexcept -> bool {
      return full_col < direct_lstable_store->column_has_null.size() &&
             direct_lstable_store->column_has_null[full_col] == 0;
    };
    const auto safe_int_col_bounds =
        [&](slot_t full_col, platform::Int128* lo_out,
            platform::Int128* hi_out) noexcept -> bool {
      if (!col_no_null(full_col) ||
          full_col >= direct_lstable_store->column_min_int.size() ||
          direct_lstable_store->column_min_int[full_col] >
              direct_lstable_store->column_max_int[full_col]) {
        return false;
      }
      *lo_out = direct_lstable_store->column_min_int[full_col];
      *hi_out = direct_lstable_store->column_max_int[full_col];
      return true;
    };
    const auto safe_int_sum_bound =
        [&](platform::Int128 lo, platform::Int128 hi,
            platform::Int128 limit) noexcept -> bool {
      if (lo < std::numeric_limits<int64_t>::min() ||
          hi > std::numeric_limits<int64_t>::max()) {
        return false;
      }
      const platform::Int128 max_abs =
          std::max(lo < 0 ? -lo : lo, hi < 0 ? -hi : hi);
      const platform::Int128 nrows =
          std::max<platform::Int128>(direct_lstable_store->total_rows, 1);
      return max_abs <= limit / nrows;
    };
    for (const IntegerPeekCompare& pred : peek_compares) {
      if (!col_no_null(pred.column)) {
        eligible = false;
        break;
      }
    }
    for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
      if (!col_no_null(df.full_column)) {
        eligible = false;
        break;
      }
    }
    for (const auto& [column, unused] : fast_empty_filters) {
      if (!col_no_null(column)) {
        eligible = false;
        break;
      }
    }
    if (eligible && stream_prebuilt_membership != nullptr &&
        !col_no_null(proj_to_full[stream_prebuilt_membership->column])) {
      eligible = false;
    }
    for (size_t i = 0; eligible && i < plan.aggregates.size(); ++i) {
      const TypedAggregateInput& input = plan.aggregates[i];
      const SlimAggKind kind = plan.slim_kinds[i];
      const bool byte_length =
          bounded_int_group_key && byte_length_columns[i].has_value();
      if (input.conditional.has_value() ||
          (input.child.has_value() && !byte_length)) {
        eligible = false;
        break;
      }
      if (kind == SlimAggKind::kCount) {
        if (byte_length &&
            !col_no_null(proj_to_full[*byte_length_columns[i]])) {
          eligible = false;
          break;
        }
        if (input.column.has_value() &&
            !col_no_null(proj_to_full[*input.column])) {
          eligible = false;
          break;
        }
        if (input.double_expr.has_value() || input.int_expr.has_value()) {
          eligible = false;
          break;
        }
        continue;
      }
      if (kind != SlimAggKind::kSumDouble && kind != SlimAggKind::kAvgDouble &&
          kind != SlimAggKind::kSumInt64) {
        eligible = false;
        break;
      }
      FastDictDoubleTerm term;
      if (byte_length &&
          (kind == SlimAggKind::kSumInt64 || kind == SlimAggKind::kAvgDouble)) {
        term.full_col0 = proj_to_full[*byte_length_columns[i]];
        platform::Int128 upper_bound = 0;
        const auto& dictionary =
            direct_lstable_store->dictionaries[term.full_col0];
        if (dictionary.active || dictionary.wide_code_bytes != 0) {
          size_t maximum = 0;
          // Persistent stores expose entries through flat views only.
          for (size_t code = 0; code < dictionary.StringEntryCount(); ++code) {
            maximum = std::max(maximum, dictionary.StringEntryAt(code).size());
          }
          upper_bound = static_cast<platform::Int128>(maximum) *
                        direct_lstable_store->total_rows;
        } else {
          for (const auto& block : direct_lstable_store->blocks) {
            upper_bound += block.columns[term.full_col0].VarcharBytes().size();
          }
        }
        // Every prefix and worker subtotal is exact in both INT64 and
        // DOUBLE. This permits an integer sum without changing AVG rounding.
        const platform::Int128 limit =
            kind == SlimAggKind::kAvgDouble
                ? static_cast<platform::Int128>(int64_t{1} << 53)
                : static_cast<platform::Int128>(
                      std::numeric_limits<int64_t>::max());
        if (!col_no_null(term.full_col0) || upper_bound > limit) {
          eligible = false;
          break;
        }
        term.is_int_term = true;
        term.byte_length = true;
      } else if (input.column.has_value() &&
                 proj_types[*input.column] == ValueType::kDouble &&
                 (kind == SlimAggKind::kSumDouble ||
                  kind == SlimAggKind::kAvgDouble)) {
        term.shape = TypedDoubleExprInput::Shape::kSingleDoubleCol;
        term.full_col0 = proj_to_full[*input.column];
        if (!col_no_null(term.full_col0)) {
          eligible = false;
          break;
        }
      } else if (input.double_expr.has_value() &&
                 input.double_expr->shape !=
                     TypedDoubleExprInput::Shape::kGeneric &&
                 (kind == SlimAggKind::kSumDouble ||
                  kind == SlimAggKind::kAvgDouble)) {
        term.shape = input.double_expr->shape;
        term.full_col0 = proj_to_full[input.double_expr->col0];
        term.full_col1 = proj_to_full[input.double_expr->col1];
        term.full_col2 = proj_to_full[input.double_expr->col2];
        term.k0 = input.double_expr->k0;
        term.k1 = input.double_expr->k1;
        if (!col_no_null(term.full_col0) ||
            (term.shape != TypedDoubleExprInput::Shape::kSingleDoubleCol &&
             !col_no_null(term.full_col1)) ||
            (term.shape == TypedDoubleExprInput::Shape::
                               kMulConstMinusColMulConstPlusCol &&
             !col_no_null(term.full_col2))) {
          eligible = false;
          break;
        }
      } else if (!dict_group_key &&
                 (kind == SlimAggKind::kSumInt64 ||
                  kind == SlimAggKind::kAvgDouble) &&
                 input.column.has_value() &&
                 proj_types[*input.column] == ValueType::kInt64 &&
                 proj_unsigned[*input.column] == 0) {
        const slot_t full0 = proj_to_full[*input.column];
        platform::Int128 lo0 = 0;
        platform::Int128 hi0 = 0;
        const platform::Int128 limit =
            kind == SlimAggKind::kAvgDouble
                ? static_cast<platform::Int128>(int64_t{1} << 53)
                : static_cast<platform::Int128>(
                      std::numeric_limits<int64_t>::max());
        if (!safe_int_col_bounds(full0, &lo0, &hi0) ||
            !safe_int_sum_bound(lo0, hi0, limit)) {
          eligible = false;
          break;
        }
        term.is_int_term = true;
        term.int_shape = TypedIntExprInput::Shape::kSingleIntCol;
        term.full_col0 = full0;
      } else if (!dict_group_key &&
                 (kind == SlimAggKind::kSumInt64 ||
                  kind == SlimAggKind::kAvgDouble) &&
                 input.int_expr.has_value() &&
                 input.int_expr->shape != TypedIntExprInput::Shape::kGeneric) {
        const TypedIntExprInput& ie = *input.int_expr;
        const slot_t full0 = proj_to_full[ie.col0];
        const slot_t full1 = proj_to_full[ie.col1];
        const slot_t full2 = proj_to_full[ie.col2];
        platform::Int128 lo0 = 0, hi0 = 0;
        if (!safe_int_col_bounds(full0, &lo0, &hi0)) {
          eligible = false;
          break;
        }
        platform::Int128 lo = lo0, hi = hi0;
        if (ie.shape != TypedIntExprInput::Shape::kSingleIntCol) {
          platform::Int128 lo1 = 0, hi1 = 0;
          if (!safe_int_col_bounds(full1, &lo1, &hi1)) {
            eligible = false;
            break;
          }
          const platform::Int128 p0 = lo0 * lo1;
          const platform::Int128 p1 = lo0 * hi1;
          const platform::Int128 p2 = hi0 * lo1;
          const platform::Int128 p3 = hi0 * hi1;
          lo = std::min({p0, p1, p2, p3});
          hi = std::max({p0, p1, p2, p3});
          if (lo < std::numeric_limits<int64_t>::min() ||
              hi > std::numeric_limits<int64_t>::max()) {
            eligible = false;
            break;
          }
          if (ie.shape == TypedIntExprInput::Shape::kMulTwoIntColsPlusCol) {
            platform::Int128 lo2 = 0, hi2 = 0;
            if (!safe_int_col_bounds(full2, &lo2, &hi2)) {
              eligible = false;
              break;
            }
            lo += lo2;
            hi += hi2;
          }
        }
        const platform::Int128 limit =
            kind == SlimAggKind::kAvgDouble
                ? static_cast<platform::Int128>(int64_t{1} << 53)
                : static_cast<platform::Int128>(
                      std::numeric_limits<int64_t>::max());
        if (!safe_int_sum_bound(lo, hi, limit)) {
          eligible = false;
          break;
        }
        term.is_int_term = true;
        term.int_shape = ie.shape;
        term.full_col0 = full0;
        term.full_col1 = full1;
        term.full_col2 = full2;
      } else {
        eligible = false;
        break;
      }
      bool found_dup = false;
      for (const FastDictDoubleTerm& existing : fast_dict_terms) {
        if (existing.is_int_term == term.is_int_term &&
            existing.byte_length == term.byte_length &&
            existing.shape == term.shape &&
            existing.int_shape == term.int_shape &&
            existing.full_col0 == term.full_col0 &&
            existing.full_col1 == term.full_col1 &&
            existing.full_col2 == term.full_col2 && existing.k0 == term.k0 &&
            existing.k1 == term.k1) {
          fast_dict_dup_aggs.emplace_back(i, existing.primary_agg_idx);
          found_dup = true;
          break;
        }
      }
      if (!found_dup) {
        term.primary_agg_idx = i;
        if (!term.is_int_term && !fast_dict_terms.empty() &&
            term.shape ==
                TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol) {
          const FastDictDoubleTerm& prev = fast_dict_terms.back();
          if (!prev.is_int_term &&
              prev.shape == TypedDoubleExprInput::Shape::kMulConstMinusCol &&
              prev.full_col0 == term.full_col0 &&
              prev.full_col1 == term.full_col1 && prev.k0 == term.k0) {
            term.reuse_prev_mul_const_minus = true;
          }
        }
        fast_dict_terms.push_back(term);
      }
    }
    if (bounded_int_group_key && fast_dict_terms.size() > 1) {
      eligible = false;
    }
    fast_dict_double_agg = eligible;
  }
  const bool bounded_has_double_term =
      fast_dict_double_agg && bounded_int_group_key &&
      !fast_dict_terms.empty() && !fast_dict_terms[0].is_int_term;
  const bool bounded_has_int_term =
      fast_dict_double_agg && bounded_int_group_key &&
      !fast_dict_terms.empty() && fast_dict_terms[0].is_int_term;
  std::vector<std::vector<double>> bounded_worker_sums(
      bounded_has_double_term ? worker_count : 0);
  // Per-group "an input term was itself infinite" marks (SlimFlags bit0
  // semantics). An infinite total without such a mark is a finite-input
  // overflow and must raise, not be reported as +-inf.
  std::vector<std::vector<uint8_t>> bounded_worker_inf(
      bounded_has_double_term ? worker_count : 0);
  std::vector<std::vector<int64_t>> bounded_worker_int_sums(
      bounded_has_int_term ? worker_count : 0);
  std::vector<std::vector<uint32_t>> bounded_worker_counts(
      (fast_dict_double_agg && bounded_int_group_key) ? worker_count : 0);

  const auto check_slim_group_having = [&](TypedWorkerGroups& local,
                                           size_t group, Status* err) -> bool {
    for (const PositionalHaving& clause : positional_having) {
      Value finished;
      if (!FinishSlimCell(local, group, clause.agg_index,
                          (*local.slim_kinds)[clause.agg_index], &finished,
                          err)) {
        return false;
      }
      Value right_finished;
      if (clause.rhs_is_agg &&
          !FinishSlimCell(local, group, clause.rhs_agg_index,
                          (*local.slim_kinds)[clause.rhs_agg_index],
                          &right_finished, err)) {
        return false;
      }
      if (!EvalPositionalHavingValues(
              clause, finished, clause.rhs_is_agg ? &right_finished : nullptr,
              err)) {
        return false;
      }
    }
    return true;
  };
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
        if (rows.empty() && hit_arena == nullptr &&
            cached_relation->HasColumnarOnly() &&
            cached_relation->columnar != nullptr) {
          const auto& chunks = cached_relation->columnar->chunks;
          const size_t col_total = cached_relation->columnar->total_rows;
          const size_t begin = col_total * worker / worker_count;
          const size_t end = col_total * (worker + 1) / worker_count;
          Row chunk_row;
          size_t offset = 0;
          for (const ColumnarChunk& chunk : chunks) {
            const size_t chunk_end_row = offset + chunk.rows;
            if (chunk_end_row > begin && offset < end) {
              const size_t r_begin = begin > offset ? begin - offset : 0;
              const size_t r_end = std::min(chunk.rows, end - offset);
              for (size_t r = r_begin; r < r_end; ++r) {
                if (worker_errors[worker] != Status::kSuccess) {
                  return;
                }
                chunk.MaterializeRowInto(r, &chunk_row);
                ++counters[worker].scanned;
                accumulate_row(chunk_row, &workers[worker],
                               &worker_errors[worker],
                               &counters[worker].emitted,
                               &counters[worker].updates, nullptr, &scratch);
              }
            }
            offset = chunk_end_row;
            if (offset >= end) {
              break;
            }
          }
          return;
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
        // states, and column/conditional/double_expr (or COUNT(*)) aggregate
        // inputs; anything else keeps the Row loop below.
        std::shared_ptr<const BaseRelationArena> arena;
        bool arena_ok = !arena_disabled && workers[worker].slim && int_key &&
                        plan.filter_disjunctions.empty() &&
                        plan.filter_programs.empty() &&
                        plan.aggregates.size() <= 8;
        if (arena_ok) {
          for (const TypedAggregateInput& input : plan.aggregates) {
            const auto aggregate_index =
                static_cast<size_t>(&input - plan.aggregates.data());
            if (input.child.has_value() ||
                (!input.column.has_value() && !input.conditional.has_value() &&
                 !input.double_expr.has_value() &&
                 !input.int_expr.has_value() &&
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
          fallback_row_values.reserve(stride);
          constexpr size_t kArenaPrefetchDistance = 8;
          for (size_t index = begin; index < end; ++index) {
            if (worker_errors[worker] != Status::kSuccess) {
              return;
            }
            ++counters[worker].scanned;
            const int64_t* cells = arena->cells.data() + (index * stride);
            const auto arena_is_null = [&](slot_t c) {
              return arena->IsNull(index, c);
            };
            const auto arena_make_value = [&](slot_t c) {
              if (arena->slot_types.size() == stride &&
                  arena->slot_types[c] == ValueType::kVarChar) {
                Value v;
                v.type = ValueType::kVarChar;
                v.value.varchar_value =
                    arena->Varchar(index, varchar_ordinal[c]);
                return v;
              }
              return ArenaCellValue(*arena, cells, c, qualified_schema);
            };
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
                      arena_make_value(static_cast<slot_t>(c)));
                }
              }
              Row fallback_row(std::move(fallback_row_values));
              accumulate_row(fallback_row, &workers[worker],
                             &worker_errors[worker], &counters[worker].emitted,
                             &counters[worker].updates, nullptr, &scratch);
              fallback_row_values = std::move(fallback_row.values_);
              continue;
            }
            bool keep = true;
            for (const TypedFilterCompare& compare : plan.filter_compares) {
              if (!DecideTypedFilterCompareCells(compare, cells, arena_is_null,
                                                 arena_make_value)) {
                keep = false;
                break;
              }
            }
            if (keep) {
              for (const TypedInListCompare& list : plan.inlist_compares) {
                if (arena->IsNull(index, list.column)) {
                  keep = false;
                  break;
                }
                bool matched = false;
                if (list.is_string) {
                  matched = MatchTypedStringInList(
                      arena->Varchar(index, varchar_ordinal[list.column]),
                      list);
                } else if (list.is_double) {
                  const auto v = std::bit_cast<double>(cells[list.column]);
                  for (const double item : list.double_values) {
                    if (v == item) {
                      matched = true;
                      break;
                    }
                  }
                } else {
                  const int64_t cell = cells[list.column];
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
            ++counters[worker].emitted;
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
                if (key_value.type == ValueType::kInt64 &&
                    qualified_schema.GetColumn(offset).IsUnsigned()) {
                  key_value.SetUnsigned();
                }
                scratch.push_back(CanonicalDistinctValue(key_value));
              }
              group_index = workers[worker].ClaimSlotForNewGroup(
                  slot, scratch, plan.aggregates.size(), aggregate_expressions);
            } else {
              group_index = *slot;
            }
            ++counters[worker].updates;
            std::array<Value, 8>& stack_values = cache_hit_values;
            std::array<const Value*, 8>& value_ptrs = cache_hit_value_ptrs;
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              const TypedAggregateInput& input = plan.aggregates[i];
              if (input.column.has_value()) {
                const slot_t column = *input.column;
                if (arena->IsNull(index, column)) {
                  stack_values[i] = Value();
                } else {
                  stack_values[i] = arena_make_value(column);
                }
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              if (input.conditional.has_value()) {
                stack_values[i] =
                    input.conditional->has_else_constant
                        ? Value(input.conditional->else_int_constant)
                        : Value();
                if (DecideTypedFilterCompareCells(input.conditional->condition,
                                                  cells, arena_is_null,
                                                  arena_make_value)) {
                  if (input.conditional->then_column.has_value()) {
                    const slot_t col = *input.conditional->then_column;
                    if (!arena->IsNull(index, col)) {
                      stack_values[i] = arena_make_value(col);
                    }
                  } else {
                    stack_values[i] =
                        Value(input.conditional->then_int_constant);
                    stack_values[i].type = input.conditional->result_type;
                  }
                }
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              if (input.double_expr.has_value()) {
                double dbl = 0.0;
                stack_values[i] =
                    EvaluateTypedDoubleExprCells(*input.double_expr, cells,
                                                 arena_is_null, &dbl)
                        ? Value(dbl)
                        : Value();
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              if (input.int_expr.has_value()) {
                int64_t iv = 0;
                const bool ok = EvaluateTypedIntExprCells(
                    *input.int_expr, cells, arena_is_null, &iv,
                    &worker_errors[worker]);
                if (worker_errors[worker] != Status::kSuccess) {
                  return;
                }
                stack_values[i] = ok ? Value(iv) : Value();
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              value_ptrs[i] = nullptr;  // COUNT(*) sentinel.
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
          ++counters[worker].scanned;
          accumulate_row(rows[index], &workers[worker], &worker_errors[worker],
                         &counters[worker].emitted, &counters[worker].updates,
                         nullptr, &scratch);
        }
        return;
      }
      if (cached_relation != nullptr) {
        // Spilled cache: worker 0 streams it serially.
        if (worker != 0) {
          return;
        }
        Status error = cached_relation->FinishSpill();
        size_t rows_output = 0;
        size_t updates = 0;
        if (error == Status::kSuccess) {
          const Status read_status =
              cached_relation->ForEachRow([&](const Row& row) {
                if (error != Status::kSuccess) {
                  return;
                }
                ++counters[worker].scanned;
                accumulate_row(row, &workers[worker], &error, &rows_output,
                               &updates, nullptr, &scratch);
              });
          if (error == Status::kSuccess && read_status != Status::kSuccess) {
            error = read_status;
          }
        }
        worker_errors[worker] = error;
        counters[worker].emitted += rows_output;
        counters[worker].updates += updates;
        return;
      }
      // Static rotation, same contract as the grouped-chunk fold above:
      // worker w scans morsels w, w+W, ... so the per-worker partial sums and
      // the worker-order merge are deterministic (rank boundaries in the
      // ROLLUP queries read these doubles).
      // Per-worker scratch arrays for the fold; every slot is written before
      // it is read within a row, so they carry no cross-row state.
      std::array<Value, 8> fold_row_values{};
      const size_t morsel_begin =
          fill_cache ? worker : (morsels.size() * worker) / worker_count;
      const size_t morsel_end =
          fill_cache ? morsels.size()
                     : (morsels.size() * (worker + 1)) / worker_count;
      const size_t morsel_step = fill_cache ? worker_count : 1;
      const RuntimeFilterGroup* active_rf =
          fill_cache ? nullptr : stream_runtime_filter;
      const std::optional<slot_t> active_key_col =
          active_rf != nullptr ? stream_key_column : std::nullopt;
      const ValueType active_rf_type =
          active_rf != nullptr ? stream_runtime_filter_type : ValueType::kNull;

      if (use_direct_lstable) {
        const size_t total_blocks = direct_lstable_store->blocks.size();
        const size_t blk_begin = (total_blocks * worker) / worker_count;
        const size_t blk_end = (total_blocks * (worker + 1)) / worker_count;
        TypedWorkerGroups& local = workers[worker];
        Status& err = worker_errors[worker];

        const RuntimeFilterGroup* lstable_rf = nullptr;
        if (active_rf != nullptr && !active_rf->empty() &&
            active_key_col.has_value() &&
            *active_key_col < table_schema.ColumnCount()) {
          const Column& kcol = table_schema.GetColumn(*active_key_col);
          if ((kcol.Type() == ValueType::kInt64 ||
               kcol.Type() == ValueType::kDate) &&
              !kcol.IsUnsigned() &&
              // kNull (unobserved build type) is only sound for a group
              // that rejects unconditionally -- the AcceptsColumn contract.
              (active_rf_type == kcol.Type() ||
               (active_rf_type == ValueType::kNull &&
                active_rf->RejectsAll()))) {
            lstable_rf = active_rf;
          }
        }

        if (scalar_interval) {
          if (interval_empty) {
            return;
          }
          size_t matching_rows = 0;
          int64_t count = 0;
          double sum = 0.0;
          bool saw_infinity = false;
          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            const auto& block = direct_lstable_store->blocks[bi];
            if (!block.MatchesZoneMaps(peek_ptr, nullptr, std::nullopt)) {
              continue;
            }
            const auto& filter = block.columns[interval_column];
            if (!scalar_value_column) {
              size_t block_count = 0;
              if (!filter.has_null) {
                for (uint32_t r = 0; r < block.row_count; ++r) {
                  const int64_t key = filter.int_cells[r];
                  block_count += static_cast<size_t>(key >= interval_low &&
                                                     key <= interval_high);
                }
              } else {
                for (uint32_t r = 0; r < block.row_count; ++r) {
                  const int64_t key = filter.int_cells[r];
                  block_count += static_cast<size_t>(!filter.IsNull(r) &&
                                                     key >= interval_low &&
                                                     key <= interval_high);
                }
              }
              matching_rows += block_count;
              count += static_cast<int64_t>(block_count);
              continue;
            }
            const auto& values = block.columns[*scalar_value_column];
            for (uint32_t r = 0; r < block.row_count; ++r) {
              const int64_t key = filter.int_cells[r];
              if (filter.IsNull(r) || key < interval_low ||
                  key > interval_high) {
                continue;
              }
              ++matching_rows;
              if (!values.IsNull(r)) {
                const auto value = std::bit_cast<double>(values.int_cells[r]);
                sum += value;
                ++count;
                saw_infinity = saw_infinity || std::isinf(value);
              }
            }
          }
          if (matching_rows > 0) {
            bool inserted = false;
            const size_t group =
                local.FindOrInsert({}, 0, plan.aggregates.size(),
                                   aggregate_expressions, &inserted);
            local.SlimCount(group, 0) = count;
            if (scalar_value_column) {
              local.SlimDoubleTotal(group, 0) = sum;
              if (scalar_kind == SlimAggKind::kSumDouble && saw_infinity) {
                local.SlimFlags(group, 0) |= 1;
              }
            }
            counters[worker].scanned += matching_rows;
            counters[worker].emitted += matching_rows;
            counters[worker].updates += matching_rows;
          }
          return;
        }

        if (scalar_in_range_sum) {
          // Lean proof: lean/TinylambOpt/ResearchProofs/Wave8H2o.lean
          // (InRangeSynopsis_sound).
          if (in_range_empty) {
            return;
          }
          const size_t num_aggs = in_range_aggs.size();
          size_t matching_rows = 0;
          std::array<int64_t, 4> counts{};
          std::array<int64_t, 4> int_sums{};
          std::array<double, 4> dbl_sums{};
          std::array<bool, 4> saw_inf{};

          const bool is_count_and_sum_int =
              num_aggs == 2 &&
              ((in_range_aggs[0].kind == SlimAggKind::kCount &&
                in_range_aggs[1].kind == SlimAggKind::kSumInt64) ||
               (in_range_aggs[0].kind == SlimAggKind::kSumInt64 &&
                in_range_aggs[1].kind == SlimAggKind::kCount));
          const size_t cs_count_idx =
              (is_count_and_sum_int &&
               in_range_aggs[0].kind == SlimAggKind::kCount)
                  ? 0
                  : 1;
          const size_t cs_sum_idx = 1 - cs_count_idx;
          const slot_t cs_sum_col =
              is_count_and_sum_int ? *in_range_aggs[cs_sum_idx].full_column : 0;
          const uint64_t range_span = static_cast<uint64_t>(in_range_high) -
                                      static_cast<uint64_t>(in_range_low);

          const auto match_in = [&](int64_t v) -> bool {
            if (in_range_use_mask) {
              const uint64_t d = static_cast<uint64_t>(v) -
                                 static_cast<uint64_t>(in_range_in_min);
              return d < 64ULL && (((in_range_in_mask >> d) & 1ULL) != 0);
            }
            if (v < in_range_in_min || v > in_range_in_max) {
              return false;
            }
            return std::ranges::any_of(
                in_range_in_vals,
                [v](const int64_t item) { return v == item; });
          };

          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            const auto& block = direct_lstable_store->blocks[bi];
            if (block.row_count == 0 ||
                !block.MatchesZoneMaps(peek_ptr, nullptr, std::nullopt)) {
              continue;
            }
            const auto& in_blk = block.columns[in_range_in_col];
            if (!in_blk.is_unsigned) {
              if (!in_blk.has_min_max) {
                continue;
              }
              if (in_range_in_max < in_blk.min_int ||
                  in_range_in_min > in_blk.max_int) {
                continue;
              }
              if (in_range_in_vals.size() <= 16) {
                bool any_may_match = false;
                for (const int64_t v : in_range_in_vals) {
                  if (v >= in_blk.min_int && v <= in_blk.max_int &&
                      in_blk.BlockBloomMayContain(v)) {
                    any_may_match = true;
                    break;
                  }
                }
                if (!any_may_match) {
                  continue;
                }
              }
            }
            const auto& range_blk = block.columns[in_range_range_col];
            const uint32_t nrows = block.row_count;
            const int64_t* __restrict in_cells = in_blk.int_cells.data();
            const int64_t* __restrict range_cells = range_blk.int_cells.data();

            if (is_count_and_sum_int && in_range_use_mask && !in_blk.has_null &&
                !range_blk.has_null && !block.columns[cs_sum_col].has_null) {
              const int64_t* __restrict sum_cells =
                  block.columns[cs_sum_col].int_cells.data();
              size_t blk_match = 0;
              int64_t blk_sum = int_sums[cs_sum_idx];
              for (uint32_t r = 0; r < nrows; ++r) {
                const uint64_t d = static_cast<uint64_t>(in_cells[r]) -
                                   static_cast<uint64_t>(in_range_in_min);
                const uint64_t rd = static_cast<uint64_t>(range_cells[r]) -
                                    static_cast<uint64_t>(in_range_low);
                const bool hit =
                    (d < 64ULL) &
                    (((in_range_in_mask >> (d & 63ULL)) & 1ULL) != 0) &
                    (rd <= range_span);
                if (hit) {
                  ++blk_match;
                  int64_t updated = 0;
                  if (__builtin_add_overflow(blk_sum, sum_cells[r], &updated)) {
                    err = StatusError(StatusCode::kIsInfinity,
                                      "integer overflow in SUM");
                    return;
                  }
                  blk_sum = updated;
                }
              }
              matching_rows += blk_match;
              counts[cs_count_idx] += static_cast<int64_t>(blk_match);
              counts[cs_sum_idx] += static_cast<int64_t>(blk_match);
              int_sums[cs_sum_idx] = blk_sum;
              continue;
            }

            const bool check_nulls = in_blk.has_null || range_blk.has_null;
            for (uint32_t r = 0; r < nrows; ++r) {
              if (check_nulls && (in_blk.IsNull(r) || range_blk.IsNull(r))) {
                continue;
              }
              const uint64_t rd = static_cast<uint64_t>(range_cells[r]) -
                                  static_cast<uint64_t>(in_range_low);
              if (rd > range_span || !match_in(in_cells[r])) {
                continue;
              }
              ++matching_rows;
              for (size_t a = 0; a < num_aggs; ++a) {
                const auto& agg = in_range_aggs[a];
                if (agg.kind == SlimAggKind::kCount) {
                  ++counts[a];
                  continue;
                }
                const auto& vcol = block.columns[*agg.full_column];
                if (vcol.IsNull(r)) {
                  continue;
                }
                if (agg.kind == SlimAggKind::kSumInt64) {
                  int64_t updated = 0;
                  if (__builtin_add_overflow(int_sums[a], vcol.int_cells[r],
                                             &updated)) {
                    err = StatusError(StatusCode::kIsInfinity,
                                      "integer overflow in SUM");
                    return;
                  }
                  int_sums[a] = updated;
                  ++counts[a];
                } else {
                  const auto v = std::bit_cast<double>(vcol.int_cells[r]);
                  dbl_sums[a] += v;
                  ++counts[a];
                  saw_inf[a] = saw_inf[a] || std::isinf(v);
                }
              }
            }
          }

          if (matching_rows > 0) {
            bool inserted = false;
            const size_t group =
                local.FindOrInsert({}, 0, plan.aggregates.size(),
                                   aggregate_expressions, &inserted);
            for (size_t a = 0; a < num_aggs; ++a) {
              local.SlimCount(group, a) = counts[a];
              if (in_range_aggs[a].kind == SlimAggKind::kSumInt64) {
                local.SlimIntTotal(group, a) = int_sums[a];
              } else if (in_range_aggs[a].kind == SlimAggKind::kSumDouble ||
                         in_range_aggs[a].kind == SlimAggKind::kAvgDouble) {
                local.SlimDoubleTotal(group, a) = dbl_sums[a];
                if (in_range_aggs[a].kind == SlimAggKind::kSumDouble &&
                    saw_inf[a]) {
                  local.SlimFlags(group, a) |= 1;
                }
              }
            }
            counters[worker].scanned += matching_rows;
            counters[worker].emitted += matching_rows;
            counters[worker].updates += matching_rows;
          }
          return;
        }

        if (bulk_count && lstable_rf == nullptr &&
            lstable_agg_dict_filters.empty()) {
          size_t matching_rows = 0;
          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            const LSTablePageBlock& blk = direct_lstable_store->blocks[bi];
            if (blk.row_count == 0 ||
                !blk.MatchesZoneMaps(peek_ptr, nullptr, std::nullopt)) {
              continue;
            }
            if (peek_ptr == nullptr || peek_ptr->empty()) {
              counters[worker].scanned += blk.row_count;
              matching_rows += blk.row_count;
              continue;
            }
            if (peek_ptr->size() == 1 &&
                !blk.columns[(*peek_ptr)[0].column].has_null) {
              const IntegerPeekCompare& pred = (*peek_ptr)[0];
              const int64_t* __restrict pcells =
                  blk.columns[pred.column].int_cells.data();
              const int64_t k = pred.constant;
              const uint32_t nrows = blk.row_count;
              size_t blk_cnt = 0;
              switch (pred.op) {
                case BinaryOperation::kGreaterThan:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] > k);
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] >= k);
                  }
                  break;
                case BinaryOperation::kLessThan:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] < k);
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] <= k);
                  }
                  break;
                case BinaryOperation::kEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] == k);
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] != k);
                  }
                  break;
                default:
                  break;
              }
              counters[worker].scanned += blk_cnt;
              matching_rows += blk_cnt;
              continue;
            }
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              bool ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk.columns[pred.column];
                if (pcol.IsNull(r) ||
                    !MatchIntegerCompare(pcol.int_cells[r], pred.op,
                                         pred.constant)) {
                  ok = false;
                  break;
                }
              }
              if (ok) {
                ++counters[worker].scanned;
                ++matching_rows;
              }
            }
          }
          if (matching_rows > 0) {
            bool inserted = false;
            const size_t group =
                local.FindOrInsert({}, 0, plan.aggregates.size(),
                                   aggregate_expressions, &inserted);
            for (size_t agg = 0; agg < plan.aggregates.size(); ++agg) {
              auto& count = local.SlimCount(group, agg);
              if (std::cmp_greater(
                      matching_rows,
                      std::numeric_limits<int64_t>::max() - count)) {
                err = StatusError(StatusCode::kIsInfinity,
                                  "integer overflow in COUNT");
                break;
              }
              count += static_cast<int64_t>(matching_rows);
            }
            counters[worker].emitted += matching_rows;
            counters[worker].updates += matching_rows;
          }
          return;
        }

        std::array<uint16_t, 256> dict_pair_to_group{};
        dict_pair_to_group.fill(0xFFFFU);
        std::vector<uint32_t> wide_code_to_group;
        if (wide_group_dictionary_size != 0) {
          // One extra slot is the NULL group. Different codes may still
          // canonicalize to the same key; FindOrInsert remains authoritative.
          wide_code_to_group.assign(wide_group_dictionary_size + 1, UINT32_MAX);
        }
        std::vector<uint32_t> blk_sel_rows;
        std::vector<uint8_t> blk_row_groups;
        if (fast_dict_double_agg && bounded_int_group_key) {
          if (bounded_has_double_term) {
            bounded_worker_sums[worker].assign(bounded_int_span, 0.0);
            bounded_worker_inf[worker].assign(bounded_int_span, 0);
          }
          if (bounded_has_int_term) {
            bounded_worker_int_sums[worker].assign(bounded_int_span, 0);
          }
          bounded_worker_counts[worker].assign(bounded_int_span, 0U);
        }
        bool has_ungrouped = false;
        size_t ungrouped_idx = 0;
        bool has_mono_key = false;
        int64_t cur_mono_key = 0;
        size_t cur_mono_group = 0;
        bool have_rf_cache = false;
        int64_t last_rf_key = 0;
        bool last_rf_rejected = false;
        const slot_t mono_key_full_col =
            monotonic_int_key ? proj_to_full[plan.key_offsets[0]] : 0;
        const ValueType mono_key_type = monotonic_int_key
                                            ? proj_types[plan.key_offsets[0]]
                                            : ValueType::kNull;
        const bool mono_key_unsigned =
            monotonic_int_key && proj_unsigned[plan.key_offsets[0]] != 0;

        for (size_t bi = blk_begin; bi < blk_end; ++bi) {
          if (err != Status::kSuccess) {
            break;
          }
          const LSTablePageBlock& blk = direct_lstable_store->blocks[bi];
          if (blk.row_count == 0) {
            continue;
          }
          if (!blk.MatchesZoneMaps(peek_ptr, lstable_rf, active_key_col)) {
            continue;
          }
          bool zone_rejected = false;
          for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
            const LSTableColumnBlock& dcol = blk.columns[df.full_column];
            if (((dcol.dict_mask[0] & df.pass_mask[0]) |
                 (dcol.dict_mask[1] & df.pass_mask[1]) |
                 (dcol.dict_mask[2] & df.pass_mask[2]) |
                 (dcol.dict_mask[3] & df.pass_mask[3])) == 0) {
              zone_rejected = true;
              break;
            }
          }
          if (zone_rejected) {
            continue;
          }
          for (const TypedFilterCompare& cmp : plan.filter_compares) {
            if (!cmp.constant_payload || !cmp.is_double) {
              continue;
            }
            const LSTableColumnBlock& dcol =
                blk.columns[proj_to_full[cmp.left]];
            if (dcol.has_nan || std::isnan(cmp.double_constant)) {
              continue;
            }
            if (!dcol.has_double_min_max) {
              zone_rejected = true;
              break;
            }
            switch (cmp.op) {
              case BinaryOperation::kEquals:
                if (cmp.double_constant < dcol.min_double ||
                    cmp.double_constant > dcol.max_double) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kNotEquals:
                if (dcol.min_double == cmp.double_constant &&
                    dcol.max_double == cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kLessThan:
                if (dcol.min_double >= cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kLessThanEquals:
                if (dcol.min_double > cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kGreaterThan:
                if (dcol.max_double <= cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kGreaterThanEquals:
                if (dcol.max_double < cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              default:
                break;
            }
            if (zone_rejected) {
              break;
            }
          }
          if (zone_rejected) {
            continue;
          }

          bool dict_all_pass = !blk.has_any_null;
          if (dict_all_pass) {
            for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
              const LSTableColumnBlock& dcol = blk.columns[df.full_column];
              if (dcol.has_null || dcol.dict_codes.empty() ||
                  ((dcol.dict_mask[0] & ~df.pass_mask[0]) |
                   (dcol.dict_mask[1] & ~df.pass_mask[1]) |
                   (dcol.dict_mask[2] & ~df.pass_mask[2]) |
                   (dcol.dict_mask[3] & ~df.pass_mask[3])) != 0) {
                dict_all_pass = false;
                break;
              }
            }
          }
          bool peek_all_pass = !blk.has_any_null && lstable_rf == nullptr &&
                               active_rf == nullptr;
          if (peek_all_pass) {
            for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
              if (filter_compare_covered[fi] != 0) {
                continue;
              }
              const TypedFilterCompare& cmp = plan.filter_compares[fi];
              if (!cmp.constant_payload || cmp.string_payload) {
                peek_all_pass = false;
                break;
              }
              const LSTableColumnBlock& col =
                  blk.columns[proj_to_full[cmp.left]];
              if (cmp.is_double) {
                if (col.has_nan || std::isnan(cmp.double_constant) ||
                    !col.has_double_min_max) {
                  peek_all_pass = false;
                  break;
                }
                switch (cmp.op) {
                  case BinaryOperation::kEquals:
                    if (col.min_double != cmp.double_constant ||
                        col.max_double != cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    if (cmp.double_constant >= col.min_double &&
                        cmp.double_constant <= col.max_double) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    if (col.max_double >= cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThanEquals:
                    if (col.max_double > cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    if (col.min_double <= cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  default:
                    if (col.min_double < cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                }
              } else {
                if (!col.has_min_max) {
                  peek_all_pass = false;
                  break;
                }
                switch (cmp.op) {
                  case BinaryOperation::kEquals:
                    if (col.min_int != cmp.int_constant ||
                        col.max_int != cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    if (cmp.int_constant >= col.min_int &&
                        cmp.int_constant <= col.max_int) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    if (col.max_int >= cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThanEquals:
                    if (col.max_int > cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    if (col.min_int <= cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  default:
                    if (col.min_int < cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                }
              }
              if (!peek_all_pass) {
                break;
              }
            }
          }
          if (peek_all_pass) {
            for (size_t li = 0; li < plan.inlist_compares.size(); ++li) {
              if (inlist_compare_covered[li] == 0) {
                peek_all_pass = false;
                break;
              }
            }
          }
          const bool block_all_pass = peek_all_pass && dict_all_pass &&
                                      plan.filter_disjunctions.empty() &&
                                      stream_prebuilt_membership == nullptr;

          if (block_all_pass && lstable_rf == nullptr &&
              (peek_ptr == nullptr || peek_ptr->empty()) && !blk.has_any_null &&
              monotonic_int_key && plan.aggregates.size() == 1 && local.slim &&
              (*local.slim_kinds)[0] == SlimAggKind::kSumDouble &&
              plan.aggregates[0].column.has_value() &&
              proj_types[*plan.aggregates[0].column] == ValueType::kDouble) {
            const int64_t* key_cells =
                blk.columns[mono_key_full_col].int_cells.data();
            const slot_t val_full_col =
                proj_to_full[*plan.aggregates[0].column];
            const LSTableColumnBlock& val_col = blk.columns[val_full_col];
            const int64_t* val_cells = val_col.int_cells.data();
            const bool use_val_dict =
                val_full_col < direct_lstable_store->dictionaries.size() &&
                direct_lstable_store->dictionaries[val_full_col].active &&
                direct_lstable_store->dictionaries[val_full_col].value_type ==
                    ValueType::kDouble &&
                !val_col.dict_codes.empty();
            const uint8_t* val_codes =
                use_val_dict ? val_col.dict_codes.data() : nullptr;
            const double* val_dict =
                use_val_dict ? direct_lstable_store->dictionaries[val_full_col]
                                   .DoubleEntries()
                                   .data()
                             : nullptr;
            const uint32_t nrows = blk.row_count;
            counters[worker].scanned += nrows;
            counters[worker].emitted += nrows;
            counters[worker].updates += nrows;
            if (use_val_dict) {
              counters[worker].dictionary_rows += nrows;
            }
            const bool can_fast_having =
                having_expr != nullptr && positional_having.size() == 1 &&
                positional_having[0].agg_index == 0 &&
                positional_having[0].has_constant &&
                !positional_having[0].constant.IsNull() &&
                !positional_having[0].constant.IsUnsigned() &&
                (positional_having[0].constant.type == ValueType::kDouble ||
                 positional_having[0].constant.type == ValueType::kInt64);
            const BinaryOperation fast_having_op =
                can_fast_having ? positional_having[0].op
                                : BinaryOperation::kEquals;
            const double fast_having_const =
                can_fast_having
                    ? (positional_having[0].constant.type == ValueType::kDouble
                           ? positional_having[0].constant.value.double_value
                           : static_cast<double>(
                                 positional_having[0].constant.value.int_value))
                    : 0.0;
            uint32_t r = 0;
            while (r < nrows) {
              const int64_t raw_key = key_cells[r];
              uint32_t r_end = r + 1;
              while (r_end < nrows && key_cells[r_end] == raw_key) {
                ++r_end;
              }
              double run_sum = 0.0;
              uint8_t run_flags = 0;
              if (val_codes != nullptr) {
                for (uint32_t k = r; k < r_end; ++k) {
                  const double val = val_dict[val_codes[k]];
                  if (std::isinf(val)) [[unlikely]] {
                    run_flags |= 1;
                  }
                  run_sum += val;
                }
              } else {
                for (uint32_t k = r; k < r_end; ++k) {
                  const auto val = std::bit_cast<double>(val_cells[k]);
                  if (std::isinf(val)) [[unlikely]] {
                    run_flags |= 1;
                  }
                  run_sum += val;
                }
              }
              const auto run_cnt = static_cast<int64_t>(r_end - r);
              r = r_end;
              if (!has_mono_key) [[unlikely]] {
                has_mono_key = true;
                cur_mono_key = raw_key;
                cur_mono_group = local.AppendMonotonicIntGroup(
                    raw_key, mono_key_type, mono_key_unsigned);
                bool emp = false;
                *local.int_index.TryEmplace(raw_key, &emp) = cur_mono_group;
              } else if (raw_key != cur_mono_key) {
                if (can_fast_having && r_end < nrows && run_flags == 0 &&
                    !std::isinf(run_sum)) {
                  bool keep_run = false;
                  switch (fast_having_op) {
                    case BinaryOperation::kGreaterThan:
                      keep_run = (run_sum > fast_having_const);
                      break;
                    case BinaryOperation::kGreaterThanEquals:
                      keep_run = (run_sum >= fast_having_const);
                      break;
                    case BinaryOperation::kLessThan:
                      keep_run = (run_sum < fast_having_const);
                      break;
                    case BinaryOperation::kLessThanEquals:
                      keep_run = (run_sum <= fast_having_const);
                      break;
                    case BinaryOperation::kEquals:
                      keep_run = (run_sum == fast_having_const);
                      break;
                    case BinaryOperation::kNotEquals:
                      keep_run = (run_sum != fast_having_const);
                      break;
                    default:
                      keep_run = true;
                      break;
                  }
                  if (!keep_run) {
                    ++reclaimed_groups[worker];
                    continue;
                  }
                }
                if (cur_mono_group > 0 && having_expr != nullptr &&
                    !positional_having.empty()) {
                  const bool passed =
                      check_slim_group_having(local, cur_mono_group, &err);
                  if (err != Status::kSuccess) {
                    return;
                  }
                  if (!passed) {
                    local.ResetMonotonicIntGroup(cur_mono_group, raw_key,
                                                 mono_key_type,
                                                 mono_key_unsigned);
                    cur_mono_key = raw_key;
                    ++reclaimed_groups[worker];
                  } else {
                    cur_mono_group = local.AppendMonotonicIntGroup(
                        raw_key, mono_key_type, mono_key_unsigned);
                    cur_mono_key = raw_key;
                  }
                } else {
                  cur_mono_group = local.AppendMonotonicIntGroup(
                      raw_key, mono_key_type, mono_key_unsigned);
                  cur_mono_key = raw_key;
                }
              }
              local.SlimFlags(cur_mono_group, 0) |= run_flags;
              local.SlimDoubleTotal(cur_mono_group, 0) += run_sum;
              local.SlimCount(cur_mono_group, 0) += run_cnt;
            }
            continue;
          }

          if (fast_dict_double_agg && lstable_rf == nullptr &&
              !blk.has_any_null) {
            struct BlockTermPtrs {
              const int64_t* col0{nullptr};
              const int64_t* col1{nullptr};
              const int64_t* col2{nullptr};
              const uint8_t* codes0{nullptr};
              const uint8_t* codes1{nullptr};
              const uint8_t* codes2{nullptr};
              const double* dict0{nullptr};
              const double* dict1{nullptr};
              const double* dict1_omd{nullptr};
              const double* dict2{nullptr};
              const double* dict2_opt{nullptr};
            };
            std::array<BlockTermPtrs, 8> term_ptrs{};
            const size_t nterms = fast_dict_terms.size();
            const auto bind_double_dict =
                [&](slot_t full_col, const uint8_t** codes_out,
                    const double** dict_out, const double** omd_out,
                    const double** opt_out, double k_omd, double k_opt) {
                  if (full_col < direct_lstable_store->dictionaries.size() &&
                      direct_lstable_store->dictionaries[full_col].active &&
                      direct_lstable_store->dictionaries[full_col].value_type ==
                          ValueType::kDouble &&
                      !blk.columns[full_col].dict_codes.empty()) {
                    const auto& d =
                        direct_lstable_store->dictionaries[full_col];
                    *codes_out = blk.columns[full_col].dict_codes.data();
                    *dict_out = d.DoubleEntries().data();
                    if (omd_out != nullptr && k_omd == 1.0 &&
                        !d.one_minus_double_entries.empty()) {
                      *omd_out = d.one_minus_double_entries.data();
                    }
                    if (opt_out != nullptr && k_opt == 1.0 &&
                        !d.one_plus_double_entries.empty()) {
                      *opt_out = d.one_plus_double_entries.data();
                    }
                  }
                };
            for (size_t ti = 0; ti < nterms; ++ti) {
              const FastDictDoubleTerm& term = fast_dict_terms[ti];
              if (term.byte_length) {
                // String lengths use VarcharAt below, not numeric operand
                // pointers. The default second operand is not a loaded column.
                continue;
              }
              term_ptrs[ti].col0 = blk.columns[term.full_col0].int_cells.data();
              if (term.is_int_term) {
                if (term.int_shape != TypedIntExprInput::Shape::kSingleIntCol) {
                  term_ptrs[ti].col1 =
                      blk.columns[term.full_col1].int_cells.data();
                  if (term.int_shape ==
                      TypedIntExprInput::Shape::kMulTwoIntColsPlusCol) {
                    term_ptrs[ti].col2 =
                        blk.columns[term.full_col2].int_cells.data();
                  }
                }
              } else {
                bind_double_dict(term.full_col0, &term_ptrs[ti].codes0,
                                 &term_ptrs[ti].dict0, nullptr, nullptr, 0.0,
                                 0.0);
                if (term.shape !=
                    TypedDoubleExprInput::Shape::kSingleDoubleCol) {
                  term_ptrs[ti].col1 =
                      blk.columns[term.full_col1].int_cells.data();
                  bind_double_dict(term.full_col1, &term_ptrs[ti].codes1,
                                   &term_ptrs[ti].dict1,
                                   &term_ptrs[ti].dict1_omd, nullptr, term.k0,
                                   0.0);
                  if (term.shape == TypedDoubleExprInput::Shape::
                                        kMulConstMinusColMulConstPlusCol) {
                    term_ptrs[ti].col2 =
                        blk.columns[term.full_col2].int_cells.data();
                    bind_double_dict(term.full_col2, &term_ptrs[ti].codes2,
                                     &term_ptrs[ti].dict2, nullptr,
                                     &term_ptrs[ti].dict2_opt, 0.0, term.k1);
                  }
                }
              }
            }
            std::array<uint32_t, 256> blk_group_counts{};
            const uint32_t nrows = blk.row_count;
            uint32_t blk_matched = 0;
            const int64_t* mship_cells =
                stream_prebuilt_membership != nullptr
                    ? blk.columns
                          [proj_to_full[stream_prebuilt_membership->column]]
                              .int_cells.data()
                    : nullptr;

            enum class BlockSelectivityMode : uint8_t {
              kEmpty = 0,
              kSparseGather = 1,
              kSelectionPrefetch = 2,
              kDenseAllPass = 3,
            };
            const auto build_block_selection = [&]() -> BlockSelectivityMode {
              if (block_all_pass && fast_empty_filters.empty()) {
                blk_matched = nrows;
                return BlockSelectivityMode::kDenseAllPass;
              }
              if (blk_sel_rows.size() < nrows) {
                blk_sel_rows.resize(nrows);
              }
              uint32_t* __restrict sel = blk_sel_rows.data();
              uint32_t matched = 0;
              if (fast_empty_filters.empty() && dict_all_pass &&
                  mship_cells == nullptr && peek_ptr != nullptr &&
                  peek_ptr->size() == 1) {
                const IntegerPeekCompare& pred = (*peek_ptr)[0];
                const int64_t* __restrict pcells =
                    blk.columns[pred.column].int_cells.data();
                const int64_t k = pred.constant;
                switch (pred.op) {
                  case BinaryOperation::kLessThanEquals:
                    for (uint32_t r = 0; r < nrows; ++r) {
                      sel[matched] = r;
                      matched += static_cast<uint32_t>(pcells[r] <= k);
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    for (uint32_t r = 0; r < nrows; ++r) {
                      sel[matched] = r;
                      matched += static_cast<uint32_t>(pcells[r] < k);
                    }
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    for (uint32_t r = 0; r < nrows; ++r) {
                      sel[matched] = r;
                      matched += static_cast<uint32_t>(pcells[r] >= k);
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    for (uint32_t r = 0; r < nrows; ++r) {
                      sel[matched] = r;
                      matched += static_cast<uint32_t>(pcells[r] > k);
                    }
                    break;
                  case BinaryOperation::kEquals:
                    for (uint32_t r = 0; r < nrows; ++r) {
                      sel[matched] = r;
                      matched += static_cast<uint32_t>(pcells[r] == k);
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    for (uint32_t r = 0; r < nrows; ++r) {
                      sel[matched] = r;
                      matched += static_cast<uint32_t>(pcells[r] != k);
                    }
                    break;
                  default:
                    break;
                }
              } else if (fast_empty_filters.empty() && dict_all_pass &&
                         mship_cells == nullptr && peek_ptr != nullptr &&
                         peek_ptr->size() == 2 &&
                         (*peek_ptr)[0].column == (*peek_ptr)[1].column &&
                         (*peek_ptr)[0].op ==
                             BinaryOperation::kGreaterThanEquals &&
                         (*peek_ptr)[1].op == BinaryOperation::kLessThan &&
                         (*peek_ptr)[1].constant >= (*peek_ptr)[0].constant) {
                const int64_t* __restrict pcells =
                    blk.columns[(*peek_ptr)[0].column].int_cells.data();
                const int64_t plo = (*peek_ptr)[0].constant;
                const auto pspan = static_cast<uint64_t>(
                    (*peek_ptr)[1].constant - (*peek_ptr)[0].constant);
                for (uint32_t r = 0; r < nrows; ++r) {
                  sel[matched] = r;
                  matched += static_cast<uint32_t>(std::cmp_less(
                      static_cast<uint64_t>(pcells[r] - plo), pspan));
                }
              } else if (fast_empty_filters.empty() && !dict_all_pass &&
                         lstable_agg_dict_filters.size() == 1 &&
                         peek_all_pass && mship_cells == nullptr) {
                const LSTableColumnBlock& dcol =
                    blk.columns[lstable_agg_dict_filters[0].full_column];
                if (!dcol.dict_codes.empty()) {
                  const uint8_t* __restrict dc0 = dcol.dict_codes.data();
                  const uint8_t* __restrict pass0 =
                      lstable_agg_dict_filters[0].pass.data();
                  for (uint32_t r = 0; r < nrows; ++r) {
                    sel[matched] = r;
                    matched += pass0[dc0[r]];
                  }
                }
              } else {
                for (uint32_t r = 0; r < nrows; ++r) {
                  if (!std::ranges::all_of(
                          fast_empty_filters, [&](const auto& filter) {
                            return blk.columns[filter.first]
                                       .VarcharAt(r)
                                       .empty() == filter.second;
                          })) {
                    continue;
                  }
                  if (!dict_all_pass) {
                    bool dict_ok = true;
                    for (const LSTableAggDictFilter& df :
                         lstable_agg_dict_filters) {
                      const LSTableColumnBlock& dcol =
                          blk.columns[df.full_column];
                      if (dcol.dict_codes.empty() ||
                          df.pass[dcol.dict_codes[r]] == 0) {
                        dict_ok = false;
                        break;
                      }
                    }
                    if (!dict_ok) {
                      continue;
                    }
                  }
                  if (!peek_all_pass && peek_ptr != nullptr &&
                      !peek_ptr->empty()) {
                    bool peek_ok = true;
                    for (const IntegerPeekCompare& pred : *peek_ptr) {
                      if (!MatchIntegerCompare(
                              blk.columns[pred.column].int_cells[r], pred.op,
                              pred.constant)) {
                        peek_ok = false;
                        break;
                      }
                    }
                    if (!peek_ok) {
                      continue;
                    }
                  }
                  if (mship_cells != nullptr &&
                      !stream_prebuilt_membership->values->Contains(
                          mship_cells[r])) {
                    continue;
                  }
                  sel[matched++] = r;
                }
              }
              blk_matched = matched;
              if (matched == 0) {
                return BlockSelectivityMode::kEmpty;
              }
              if (matched == nrows) {
                return BlockSelectivityMode::kDenseAllPass;
              }
              return (matched * 20U >= nrows)
                         ? BlockSelectivityMode::kSelectionPrefetch
                         : BlockSelectivityMode::kSparseGather;
            };

            const auto resolve_dict_group_slow =
                [&](uint32_t r, size_t pair_code) -> uint16_t {
              scratch.clear();
              for (const slot_t offset : plan.key_offsets) {
                const LSTableColumnBlock& kcol =
                    blk.columns[proj_to_full[offset]];
                Value kv;
                kv.type = ValueType::kVarChar;
                kv.value.varchar_value = kcol.VarcharAt(r);
                scratch.push_back(CanonicalDistinctValue(kv));
              }
              bool inserted = false;
              const size_t new_g =
                  local.FindOrInsert(scratch, 0, plan.aggregates.size(),
                                     aggregate_expressions, &inserted);
              const auto g = static_cast<uint16_t>(new_g);
              dict_pair_to_group[pair_code] = g;
              return g;
            };

            // Specialized unrolled fast path for Q01's 5-term grouped double
            // aggregation across all three block selectivity regimes.
            const bool is_q1_5term_shape =
                dict_group_key && dict_group_total_codes > 0 &&
                dict_group_total_codes <= 8 && nterms == 5 &&
                fast_dict_terms[0].shape ==
                    TypedDoubleExprInput::Shape::kSingleDoubleCol &&
                fast_dict_terms[1].shape ==
                    TypedDoubleExprInput::Shape::kSingleDoubleCol &&
                fast_dict_terms[2].shape ==
                    TypedDoubleExprInput::Shape::kMulConstMinusCol &&
                fast_dict_terms[3].shape ==
                    TypedDoubleExprInput::Shape::
                        kMulConstMinusColMulConstPlusCol &&
                fast_dict_terms[3].reuse_prev_mul_const_minus &&
                fast_dict_terms[4].shape ==
                    TypedDoubleExprInput::Shape::kSingleDoubleCol &&
                fast_dict_terms[2].full_col0 == fast_dict_terms[1].full_col0 &&
                fast_dict_terms[4].full_col0 == fast_dict_terms[2].full_col1 &&
                term_ptrs[0].codes0 != nullptr &&
                term_ptrs[2].dict1_omd != nullptr &&
                term_ptrs[3].dict2_opt != nullptr;
            if (is_q1_5term_shape) {
              const BlockSelectivityMode mode = build_block_selection();
              if (mode != BlockSelectivityMode::kEmpty) {
                const slot_t key_full0 = proj_to_full[plan.key_offsets[0]];
                const uint8_t* __restrict codes0 =
                    blk.columns[key_full0].dict_codes.data();
                const uint8_t* __restrict codes1 =
                    plan.key_offsets.size() == 2
                        ? blk.columns[proj_to_full[plan.key_offsets[1]]]
                              .dict_codes.data()
                        : nullptr;
                const uint8_t* __restrict q_codes = term_ptrs[0].codes0;
                const double* __restrict q_dict = term_ptrs[0].dict0;
                const int64_t* __restrict p_cells = term_ptrs[1].col0;
                const uint8_t* __restrict d_codes = term_ptrs[2].codes1;
                const double* __restrict d_dict = term_ptrs[2].dict1;
                const double* __restrict omd_dict = term_ptrs[2].dict1_omd;
                const uint8_t* __restrict t_codes = term_ptrs[3].codes2;
                const double* __restrict opt_dict = term_ptrs[3].dict2_opt;
                const uint32_t* __restrict sel = blk_sel_rows.data();
                alignas(64) std::array<std::array<double, 5>, 8> blk_sums{};
                std::array<uint32_t, 8> blk_pair_counts{};
                std::array<uint32_t, 8> blk_pair_first_row{};
                if (mode == BlockSelectivityMode::kDenseAllPass) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const size_t pc = codes1 == nullptr
                                          ? static_cast<size_t>(codes0[r])
                                          : (static_cast<size_t>(codes0[r]) *
                                             dict_group_stride1) +
                                                codes1[r];
                    if (blk_pair_counts[pc] == 0) [[unlikely]] {
                      blk_pair_first_row[pc] = r;
                    }
                    ++blk_pair_counts[pc];
                    const uint8_t dc = d_codes[r];
                    const double q = q_dict[q_codes[r]];
                    const auto p = std::bit_cast<double>(p_cells[r]);
                    const double disc_p = p * omd_dict[dc];
                    const double charge = disc_p * opt_dict[t_codes[r]];
                    const double d = d_dict[dc];
                    double* __restrict gs = blk_sums[pc].data();
                    gs[0] += q;
                    gs[1] += p;
                    gs[2] += disc_p;
                    gs[3] += charge;
                    gs[4] += d;
                  }
                } else if (mode == BlockSelectivityMode::kSelectionPrefetch &&
                           blk_matched * 2U < nrows) {
                  constexpr uint32_t kPrefetchAhead = 16;
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    if (si + kPrefetchAhead < blk_matched) {
                      __builtin_prefetch(&p_cells[sel[si + kPrefetchAhead]], 0,
                                         1);
                    }
                    const uint32_t r = sel[si];
                    const size_t pc = codes1 == nullptr
                                          ? static_cast<size_t>(codes0[r])
                                          : (static_cast<size_t>(codes0[r]) *
                                             dict_group_stride1) +
                                                codes1[r];
                    if (blk_pair_counts[pc] == 0) [[unlikely]] {
                      blk_pair_first_row[pc] = r;
                    }
                    ++blk_pair_counts[pc];
                    const uint8_t dc = d_codes[r];
                    const double q = q_dict[q_codes[r]];
                    const auto p = std::bit_cast<double>(p_cells[r]);
                    const double disc_p = p * omd_dict[dc];
                    const double charge = disc_p * opt_dict[t_codes[r]];
                    const double d = d_dict[dc];
                    double* __restrict gs = blk_sums[pc].data();
                    gs[0] += q;
                    gs[1] += p;
                    gs[2] += disc_p;
                    gs[3] += charge;
                    gs[4] += d;
                  }
                } else {
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    const uint32_t r = sel[si];
                    const size_t pc = codes1 == nullptr
                                          ? static_cast<size_t>(codes0[r])
                                          : (static_cast<size_t>(codes0[r]) *
                                             dict_group_stride1) +
                                                codes1[r];
                    if (blk_pair_counts[pc] == 0) [[unlikely]] {
                      blk_pair_first_row[pc] = r;
                    }
                    ++blk_pair_counts[pc];
                    const uint8_t dc = d_codes[r];
                    const double q = q_dict[q_codes[r]];
                    const auto p = std::bit_cast<double>(p_cells[r]);
                    const double disc_p = p * omd_dict[dc];
                    const double charge = disc_p * opt_dict[t_codes[r]];
                    const double d = d_dict[dc];
                    double* __restrict gs = blk_sums[pc].data();
                    gs[0] += q;
                    gs[1] += p;
                    gs[2] += disc_p;
                    gs[3] += charge;
                    gs[4] += d;
                  }
                }
                for (size_t pc = 0; pc < dict_group_total_codes && pc < 8;
                     ++pc) {
                  if (blk_pair_counts[pc] == 0) {
                    continue;
                  }
                  uint16_t g = dict_pair_to_group[pc];
                  if (g == 0xFFFFU) [[unlikely]] {
                    g = resolve_dict_group_slow(blk_pair_first_row[pc], pc);
                  }
                  blk_group_counts[g] += blk_pair_counts[pc];
                  for (size_t ti = 0; ti < 5; ++ti) {
                    local.SlimDoubleTotal(
                        g, fast_dict_terms[ti].primary_agg_idx) +=
                        blk_sums[pc][ti];
                  }
                }
              }
            } else if (plan.key_offsets.empty()) {
              std::array<double, 8> blk_ungrouped_sums{};
              std::array<int64_t, 8> blk_ungrouped_int_sums{};
              const bool is_q6_fast =
                  !dict_all_pass && lstable_agg_dict_filters.size() == 2 &&
                  peek_ptr != nullptr && peek_ptr->size() == 2 &&
                  (*peek_ptr)[0].column == (*peek_ptr)[1].column &&
                  (*peek_ptr)[0].op == BinaryOperation::kGreaterThanEquals &&
                  (*peek_ptr)[1].op == BinaryOperation::kLessThan &&
                  (*peek_ptr)[1].constant >= (*peek_ptr)[0].constant &&
                  mship_cells == nullptr && nterms == 1 &&
                  !fast_dict_terms[0].is_int_term &&
                  fast_dict_terms[0].shape ==
                      TypedDoubleExprInput::Shape::kMulTwoDoubleCols &&
                  !blk.columns[lstable_agg_dict_filters[0].full_column]
                       .dict_codes.empty() &&
                  !blk.columns[lstable_agg_dict_filters[1].full_column]
                       .dict_codes.empty();
              const bool is_all_pass_single_int =
                  block_all_pass && nterms == 1 &&
                  fast_dict_terms[0].is_int_term;
              const bool is_single_peek_single_int =
                  !block_all_pass && dict_all_pass && mship_cells == nullptr &&
                  peek_ptr != nullptr && peek_ptr->size() == 1 && nterms == 1 &&
                  fast_dict_terms[0].is_int_term;
              if (is_all_pass_single_int) {
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const int64_t* __restrict c0 = term_ptrs[0].col0;
                int64_t isum = 0;
                if (term.int_shape == TypedIntExprInput::Shape::kSingleIntCol) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    isum += c0[r];
                  }
                } else if (term.int_shape ==
                           TypedIntExprInput::Shape::kMulTwoIntCols) {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    isum += c0[r] * c1[r];
                  }
                } else {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  const int64_t* __restrict c2 = term_ptrs[0].col2;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    isum += (c0[r] * c1[r]) + c2[r];
                  }
                }
                blk_matched = nrows;
                blk_ungrouped_int_sums[0] = isum;
              } else if (is_single_peek_single_int) {
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const IntegerPeekCompare& pred = (*peek_ptr)[0];
                const int64_t* __restrict pcells =
                    blk.columns[pred.column].int_cells.data();
                const int64_t pconst = pred.constant;
                const BinaryOperation pop = pred.op;
                const int64_t* __restrict c0 = term_ptrs[0].col0;
                int64_t isum = 0;
                uint32_t matched = 0;
                if (term.int_shape == TypedIntExprInput::Shape::kSingleIntCol) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const bool ok = MatchIntegerCompare(pcells[r], pop, pconst);
                    matched += ok ? 1U : 0U;
                    isum += ok ? c0[r] : 0;
                  }
                } else if (term.int_shape ==
                           TypedIntExprInput::Shape::kMulTwoIntCols) {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const bool ok = MatchIntegerCompare(pcells[r], pop, pconst);
                    matched += ok ? 1U : 0U;
                    isum += ok ? (c0[r] * c1[r]) : 0;
                  }
                } else {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  const int64_t* __restrict c2 = term_ptrs[0].col2;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const bool ok = MatchIntegerCompare(pcells[r], pop, pconst);
                    matched += ok ? 1U : 0U;
                    isum += ok ? ((c0[r] * c1[r]) + c2[r]) : 0;
                  }
                }
                blk_matched = matched;
                blk_ungrouped_int_sums[0] = isum;
              } else if (is_q6_fast) {
                const uint8_t* __restrict dc0 =
                    blk.columns[lstable_agg_dict_filters[0].full_column]
                        .dict_codes.data();
                const uint8_t* __restrict dc1 =
                    blk.columns[lstable_agg_dict_filters[1].full_column]
                        .dict_codes.data();
                const uint8_t* __restrict pass0 =
                    lstable_agg_dict_filters[0].pass.data();
                const uint8_t* __restrict pass1 =
                    lstable_agg_dict_filters[1].pass.data();
                const int64_t* __restrict pcells =
                    blk.columns[(*peek_ptr)[0].column].int_cells.data();
                const auto plo_u =
                    static_cast<uint64_t>((*peek_ptr)[0].constant);
                const auto pspan = static_cast<uint64_t>(
                    (*peek_ptr)[1].constant - (*peek_ptr)[0].constant);
                const BlockTermPtrs& ptrs = term_ptrs[0];
                if (blk_sel_rows.size() < nrows) {
                  blk_sel_rows.resize(nrows);
                }
                uint32_t* __restrict sel = blk_sel_rows.data();
                // Stage 1: Compact selection vector over 1-byte dictionary
                // codes (8 KiB total per block) without touching 8-byte pcells.
                uint32_t matched = 0;
                for (uint32_t r = 0; r < nrows; ++r) {
                  sel[matched] = r;
                  matched += static_cast<uint32_t>(
                      (pass0[dc0[r]] & pass1[dc1[r]]) != 0);
                }
                // Stage 2: Refine selection vector in-place by date range only
                // for the surviving dictionary-matched rows.
                if (!peek_all_pass && matched > 0) {
                  uint32_t date_matched = 0;
                  for (uint32_t si = 0; si < matched; ++si) {
                    const uint32_t r = sel[si];
                    const uint64_t d = static_cast<uint64_t>(pcells[r]) - plo_u;
                    sel[date_matched] = r;
                    date_matched += static_cast<uint32_t>(d < pspan);
                  }
                  matched = date_matched;
                }
                blk_matched = matched;
                double sum0 = 0.0;
                for (uint32_t si = 0; si < matched; ++si) {
                  const uint32_t r = sel[si];
                  const double v0 = ptrs.codes0 != nullptr
                                        ? ptrs.dict0[ptrs.codes0[r]]
                                        : std::bit_cast<double>(ptrs.col0[r]);
                  const double v1 = ptrs.codes1 != nullptr
                                        ? ptrs.dict1[ptrs.codes1[r]]
                                        : std::bit_cast<double>(ptrs.col1[r]);
                  sum0 += v0 * v1;
                }
                blk_ungrouped_sums[0] = sum0;
              } else {
                for (uint32_t r = 0; r < nrows; ++r) {
                  if (!dict_all_pass) {
                    bool dict_ok = true;
                    for (const LSTableAggDictFilter& df :
                         lstable_agg_dict_filters) {
                      const LSTableColumnBlock& dcol =
                          blk.columns[df.full_column];
                      if (dcol.dict_codes.empty() ||
                          df.pass[dcol.dict_codes[r]] == 0) {
                        dict_ok = false;
                        break;
                      }
                    }
                    if (!dict_ok) {
                      continue;
                    }
                  }
                  if (!peek_all_pass && peek_ptr != nullptr &&
                      !peek_ptr->empty()) {
                    bool peek_ok = true;
                    for (const IntegerPeekCompare& pred : *peek_ptr) {
                      if (!MatchIntegerCompare(
                              blk.columns[pred.column].int_cells[r], pred.op,
                              pred.constant)) {
                        peek_ok = false;
                        break;
                      }
                    }
                    if (!peek_ok) {
                      continue;
                    }
                  }
                  if (mship_cells != nullptr &&
                      !stream_prebuilt_membership->values->Contains(
                          mship_cells[r])) {
                    continue;
                  }
                  ++blk_matched;
                  double prev_val = 0.0;
                  for (size_t ti = 0; ti < nterms; ++ti) {
                    const FastDictDoubleTerm& term = fast_dict_terms[ti];
                    const BlockTermPtrs& ptrs = term_ptrs[ti];
                    if (term.is_int_term) {
                      int64_t iv = 0;
                      switch (term.int_shape) {
                        case TypedIntExprInput::Shape::kSingleIntCol:
                          iv = ptrs.col0[r];
                          break;
                        case TypedIntExprInput::Shape::kMulTwoIntCols:
                          iv = ptrs.col0[r] * ptrs.col1[r];
                          break;
                        case TypedIntExprInput::Shape::kMulTwoIntColsPlusCol:
                          iv = (ptrs.col0[r] * ptrs.col1[r]) + ptrs.col2[r];
                          break;
                        default:
                          break;
                      }
                      blk_ungrouped_int_sums[ti] += iv;
                      continue;
                    }
                    const double v0 = ptrs.codes0 != nullptr
                                          ? ptrs.dict0[ptrs.codes0[r]]
                                          : std::bit_cast<double>(ptrs.col0[r]);
                    double val = 0.0;
                    switch (term.shape) {
                      case TypedDoubleExprInput::Shape::kSingleDoubleCol:
                        val = v0;
                        break;
                      case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                        const double v1 =
                            ptrs.codes1 != nullptr
                                ? ptrs.dict1[ptrs.codes1[r]]
                                : std::bit_cast<double>(ptrs.col1[r]);
                        val = v0 * v1;
                        break;
                      }
                      case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                        const double f1 =
                            ptrs.dict1_omd != nullptr
                                ? ptrs.dict1_omd[ptrs.codes1[r]]
                                : (term.k0 -
                                   (ptrs.codes1 != nullptr
                                        ? ptrs.dict1[ptrs.codes1[r]]
                                        : std::bit_cast<double>(ptrs.col1[r])));
                        val = v0 * f1;
                        break;
                      }
                      case TypedDoubleExprInput::Shape::
                          kMulConstMinusColMulConstPlusCol: {
                        const double f2 =
                            ptrs.dict2_opt != nullptr
                                ? ptrs.dict2_opt[ptrs.codes2[r]]
                                : (term.k1 +
                                   (ptrs.codes2 != nullptr
                                        ? ptrs.dict2[ptrs.codes2[r]]
                                        : std::bit_cast<double>(ptrs.col2[r])));
                        if (term.reuse_prev_mul_const_minus) {
                          val = prev_val * f2;
                        } else {
                          const double f1 =
                              ptrs.dict1_omd != nullptr
                                  ? ptrs.dict1_omd[ptrs.codes1[r]]
                                  : (term.k0 - (ptrs.codes1 != nullptr
                                                    ? ptrs.dict1[ptrs.codes1[r]]
                                                    : std::bit_cast<double>(
                                                          ptrs.col1[r])));
                          val = v0 * f1 * f2;
                        }
                        break;
                      }
                      default:
                        break;
                    }
                    prev_val = val;
                    blk_ungrouped_sums[ti] += val;
                  }
                }
              }
              if (blk_matched > 0) {
                if (!has_ungrouped) {
                  bool inserted = false;
                  ungrouped_idx =
                      local.FindOrInsert({}, 0, plan.aggregates.size(),
                                         aggregate_expressions, &inserted);
                  has_ungrouped = true;
                }
                blk_group_counts[ungrouped_idx] = blk_matched;
                for (size_t ti = 0; ti < nterms; ++ti) {
                  const size_t pidx = fast_dict_terms[ti].primary_agg_idx;
                  if (fast_dict_terms[ti].is_int_term) {
                    if ((*local.slim_kinds)[pidx] == SlimAggKind::kAvgDouble) {
                      local.SlimDoubleTotal(ungrouped_idx, pidx) +=
                          static_cast<double>(blk_ungrouped_int_sums[ti]);
                    } else {
                      local.SlimIntTotal(ungrouped_idx, pidx) +=
                          blk_ungrouped_int_sums[ti];
                    }
                  } else {
                    local.SlimDoubleTotal(ungrouped_idx, pidx) +=
                        blk_ungrouped_sums[ti];
                  }
                }
              }
            } else if (bounded_int_group_key) {
              const slot_t key_full0 = proj_to_full[plan.key_offsets[0]];
              const int64_t* __restrict key_cells =
                  blk.columns[key_full0].int_cells.data();
              uint32_t* __restrict w_cnts =
                  bounded_worker_counts[worker].data();
              const BlockSelectivityMode mode = build_block_selection();
              if (mode == BlockSelectivityMode::kEmpty) {
                continue;
              }
              if (nterms == 0) {
                if (mode == BlockSelectivityMode::kDenseAllPass) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    ++w_cnts[idx];
                  }
                } else if (mode == BlockSelectivityMode::kSelectionPrefetch) {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    if (si + 8 < blk_matched) {
                      const auto pf_idx = static_cast<size_t>(
                          key_cells[sel[si + 8]] - bounded_int_min);
                      __builtin_prefetch(&w_cnts[pf_idx], 1, 1);
                    }
                    const auto idx = static_cast<size_t>(key_cells[sel[si]] -
                                                         bounded_int_min);
                    ++w_cnts[idx];
                  }
                } else {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    const auto idx = static_cast<size_t>(key_cells[sel[si]] -
                                                         bounded_int_min);
                    ++w_cnts[idx];
                  }
                }
              } else if (bounded_has_int_term) {
                int64_t* __restrict w_isums =
                    bounded_worker_int_sums[worker].data();
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const BlockTermPtrs& ptrs = term_ptrs[0];
                auto eval_int_term_at = [&](uint32_t r) -> int64_t {
                  if (term.byte_length) {
                    return static_cast<int64_t>(
                        blk.columns[term.full_col0].VarcharAt(r).size());
                  }
                  switch (term.int_shape) {
                    case TypedIntExprInput::Shape::kSingleIntCol:
                      return ptrs.col0[r];
                    case TypedIntExprInput::Shape::kMulTwoIntCols:
                      return ptrs.col0[r] * ptrs.col1[r];
                    case TypedIntExprInput::Shape::kMulTwoIntColsPlusCol:
                      return (ptrs.col0[r] * ptrs.col1[r]) + ptrs.col2[r];
                    default:
                      return 0;
                  }
                };
                if (mode == BlockSelectivityMode::kDenseAllPass) {
                  if (term.int_shape ==
                      TypedIntExprInput::Shape::kSingleIntCol) {
                    const int64_t* __restrict c0 = ptrs.col0;
                    for (uint32_t r = 0; r < nrows; ++r) {
                      const auto idx =
                          static_cast<size_t>(key_cells[r] - bounded_int_min);
                      w_isums[idx] += c0[r];
                      ++w_cnts[idx];
                    }
                  } else {
                    for (uint32_t r = 0; r < nrows; ++r) {
                      const auto idx =
                          static_cast<size_t>(key_cells[r] - bounded_int_min);
                      w_isums[idx] += eval_int_term_at(r);
                      ++w_cnts[idx];
                    }
                  }
                } else if (mode == BlockSelectivityMode::kSelectionPrefetch) {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    if (si + 8 < blk_matched) {
                      const uint32_t pf_r = sel[si + 8];
                      const auto pf_idx = static_cast<size_t>(key_cells[pf_r] -
                                                              bounded_int_min);
                      __builtin_prefetch(&w_isums[pf_idx], 1, 1);
                      __builtin_prefetch(&w_cnts[pf_idx], 1, 1);
                    }
                    const uint32_t r = sel[si];
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    w_isums[idx] += eval_int_term_at(r);
                    ++w_cnts[idx];
                  }
                } else {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    const uint32_t r = sel[si];
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    w_isums[idx] += eval_int_term_at(r);
                    ++w_cnts[idx];
                  }
                }
              } else {
                double* __restrict w_sums = bounded_worker_sums[worker].data();
                uint8_t* __restrict w_inf = bounded_worker_inf[worker].data();
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const BlockTermPtrs& ptrs = term_ptrs[0];
                auto eval_double_term_at = [&](uint32_t r) -> double {
                  const double v0 = ptrs.codes0 != nullptr
                                        ? ptrs.dict0[ptrs.codes0[r]]
                                        : std::bit_cast<double>(ptrs.col0[r]);
                  switch (term.shape) {
                    case TypedDoubleExprInput::Shape::kSingleDoubleCol:
                      return v0;
                    case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                      const double v1 =
                          ptrs.codes1 != nullptr
                              ? ptrs.dict1[ptrs.codes1[r]]
                              : std::bit_cast<double>(ptrs.col1[r]);
                      return v0 * v1;
                    }
                    case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                      const double f1 =
                          ptrs.dict1_omd != nullptr
                              ? ptrs.dict1_omd[ptrs.codes1[r]]
                              : (term.k0 -
                                 (ptrs.codes1 != nullptr
                                      ? ptrs.dict1[ptrs.codes1[r]]
                                      : std::bit_cast<double>(ptrs.col1[r])));
                      return v0 * f1;
                    }
                    case TypedDoubleExprInput::Shape::
                        kMulConstMinusColMulConstPlusCol: {
                      const double f1 =
                          ptrs.dict1_omd != nullptr
                              ? ptrs.dict1_omd[ptrs.codes1[r]]
                              : (term.k0 -
                                 (ptrs.codes1 != nullptr
                                      ? ptrs.dict1[ptrs.codes1[r]]
                                      : std::bit_cast<double>(ptrs.col1[r])));
                      const double f2 =
                          ptrs.dict2_opt != nullptr
                              ? ptrs.dict2_opt[ptrs.codes2[r]]
                              : (term.k1 +
                                 (ptrs.codes2 != nullptr
                                      ? ptrs.dict2[ptrs.codes2[r]]
                                      : std::bit_cast<double>(ptrs.col2[r])));
                      return v0 * f1 * f2;
                    }
                    default:
                      return 0.0;
                  }
                };
                if (mode == BlockSelectivityMode::kDenseAllPass) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    {
                      const double term_v = eval_double_term_at(r);
                      if (std::isinf(term_v)) [[unlikely]] {
                        w_inf[idx] = 1;
                      }
                      w_sums[idx] += term_v;
                    }
                    ++w_cnts[idx];
                  }
                } else if (mode == BlockSelectivityMode::kSelectionPrefetch) {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    if (si + 8 < blk_matched) {
                      const uint32_t pf_r = sel[si + 8];
                      const auto pf_idx = static_cast<size_t>(key_cells[pf_r] -
                                                              bounded_int_min);
                      __builtin_prefetch(&w_sums[pf_idx], 1, 1);
                      __builtin_prefetch(&w_cnts[pf_idx], 1, 1);
                    }
                    const uint32_t r = sel[si];
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    {
                      const double term_v = eval_double_term_at(r);
                      if (std::isinf(term_v)) [[unlikely]] {
                        w_inf[idx] = 1;
                      }
                      w_sums[idx] += term_v;
                    }
                    ++w_cnts[idx];
                  }
                } else {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    const uint32_t r = sel[si];
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    {
                      const double term_v = eval_double_term_at(r);
                      if (std::isinf(term_v)) [[unlikely]] {
                        w_inf[idx] = 1;
                      }
                      w_sums[idx] += term_v;
                    }
                    ++w_cnts[idx];
                  }
                }
              }
              counters[worker].scanned += blk_matched;
              counters[worker].emitted += blk_matched;
              counters[worker].updates += blk_matched;
              continue;
            } else {
              const BlockSelectivityMode mode = build_block_selection();
              if (mode != BlockSelectivityMode::kEmpty) {
                const slot_t key_full0 = proj_to_full[plan.key_offsets[0]];
                const uint8_t* __restrict codes0 =
                    blk.columns[key_full0].dict_codes.data();
                const uint8_t* __restrict codes1 =
                    plan.key_offsets.size() == 2
                        ? blk.columns[proj_to_full[plan.key_offsets[1]]]
                              .dict_codes.data()
                        : nullptr;
                if (blk_row_groups.size() < blk_matched) {
                  blk_row_groups.resize(blk_matched);
                }
                uint8_t* __restrict rg = blk_row_groups.data();
                if (mode == BlockSelectivityMode::kDenseAllPass) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const size_t pair_code =
                        codes1 == nullptr ? static_cast<size_t>(codes0[r])
                                          : (static_cast<size_t>(codes0[r]) *
                                             dict_group_stride1) +
                                                codes1[r];
                    uint16_t g = dict_pair_to_group[pair_code];
                    if (g == 0xFFFFU) [[unlikely]] {
                      g = resolve_dict_group_slow(r, pair_code);
                    }
                    rg[r] = static_cast<uint8_t>(g);
                    ++blk_group_counts[g];
                  }
                } else {
                  const uint32_t* __restrict sel = blk_sel_rows.data();
                  for (uint32_t si = 0; si < blk_matched; ++si) {
                    const uint32_t r = sel[si];
                    const size_t pair_code =
                        codes1 == nullptr ? static_cast<size_t>(codes0[r])
                                          : (static_cast<size_t>(codes0[r]) *
                                             dict_group_stride1) +
                                                codes1[r];
                    uint16_t g = dict_pair_to_group[pair_code];
                    if (g == 0xFFFFU) [[unlikely]] {
                      g = resolve_dict_group_slow(r, pair_code);
                    }
                    rg[si] = static_cast<uint8_t>(g);
                    ++blk_group_counts[g];
                  }
                }
                const size_t n_groups_now =
                    std::min<size_t>(local.GroupCount(), 256);
                for (size_t ti = 0; ti < nterms; ++ti) {
                  const FastDictDoubleTerm& term = fast_dict_terms[ti];
                  const BlockTermPtrs& ptrs = term_ptrs[ti];
                  alignas(64) std::array<double, 256> grp_acc{};
                  for (size_t g = 0; g < n_groups_now; ++g) {
                    if (blk_group_counts[g] != 0) {
                      grp_acc[g] =
                          local.SlimDoubleTotal(g, term.primary_agg_idx);
                    }
                  }
                  auto eval_term_at = [&](uint32_t r) -> double {
                    const double v0 = ptrs.codes0 != nullptr
                                          ? ptrs.dict0[ptrs.codes0[r]]
                                          : std::bit_cast<double>(ptrs.col0[r]);
                    switch (term.shape) {
                      case TypedDoubleExprInput::Shape::kSingleDoubleCol:
                        return v0;
                      case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                        const double v1 =
                            ptrs.codes1 != nullptr
                                ? ptrs.dict1[ptrs.codes1[r]]
                                : std::bit_cast<double>(ptrs.col1[r]);
                        return v0 * v1;
                      }
                      case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                        const double f1 =
                            ptrs.dict1_omd != nullptr
                                ? ptrs.dict1_omd[ptrs.codes1[r]]
                                : (term.k0 -
                                   (ptrs.codes1 != nullptr
                                        ? ptrs.dict1[ptrs.codes1[r]]
                                        : std::bit_cast<double>(ptrs.col1[r])));
                        return v0 * f1;
                      }
                      case TypedDoubleExprInput::Shape::
                          kMulConstMinusColMulConstPlusCol: {
                        const double f1 =
                            ptrs.dict1_omd != nullptr
                                ? ptrs.dict1_omd[ptrs.codes1[r]]
                                : (term.k0 -
                                   (ptrs.codes1 != nullptr
                                        ? ptrs.dict1[ptrs.codes1[r]]
                                        : std::bit_cast<double>(ptrs.col1[r])));
                        const double f2 =
                            ptrs.dict2_opt != nullptr
                                ? ptrs.dict2_opt[ptrs.codes2[r]]
                                : (term.k1 +
                                   (ptrs.codes2 != nullptr
                                        ? ptrs.dict2[ptrs.codes2[r]]
                                        : std::bit_cast<double>(ptrs.col2[r])));
                        return v0 * f1 * f2;
                      }
                      default:
                        return 0.0;
                    }
                  };
                  if (mode == BlockSelectivityMode::kDenseAllPass) {
                    for (uint32_t r = 0; r < nrows; ++r) {
                      grp_acc[rg[r]] += eval_term_at(r);
                    }
                  } else if (mode == BlockSelectivityMode::kSelectionPrefetch) {
                    const uint32_t* __restrict sel = blk_sel_rows.data();
                    for (uint32_t si = 0; si < blk_matched; ++si) {
                      if (si + 16 < blk_matched && ptrs.col0 != nullptr) {
                        __builtin_prefetch(&ptrs.col0[sel[si + 16]], 0, 1);
                      }
                      grp_acc[rg[si]] += eval_term_at(sel[si]);
                    }
                  } else {
                    const uint32_t* __restrict sel = blk_sel_rows.data();
                    for (uint32_t si = 0; si < blk_matched; ++si) {
                      grp_acc[rg[si]] += eval_term_at(sel[si]);
                    }
                  }
                  for (size_t g = 0; g < n_groups_now; ++g) {
                    if (blk_group_counts[g] != 0) {
                      local.SlimDoubleTotal(g, term.primary_agg_idx) =
                          grp_acc[g];
                    }
                  }
                }
              }
            }
            if (blk_matched > 0) {
              counters[worker].scanned += blk_matched;
              counters[worker].emitted += blk_matched;
              counters[worker].updates += blk_matched;
              for (size_t g = 0; g < local.GroupCount() && g < 256; ++g) {
                const uint32_t cnt = blk_group_counts[g];
                if (cnt == 0) {
                  continue;
                }
                for (const auto& [dup_i, prim_i] : fast_dict_dup_aggs) {
                  if ((*local.slim_kinds)[dup_i] == SlimAggKind::kSumInt64) {
                    local.SlimIntTotal(g, dup_i) =
                        local.SlimIntTotal(g, prim_i);
                  } else if ((*local.slim_kinds)[prim_i] ==
                             SlimAggKind::kSumInt64) {
                    local.SlimDoubleTotal(g, dup_i) =
                        static_cast<double>(local.SlimIntTotal(g, prim_i));
                  } else {
                    local.SlimDoubleTotal(g, dup_i) =
                        local.SlimDoubleTotal(g, prim_i);
                  }
                }
                for (size_t i = 0; i < plan.aggregates.size(); ++i) {
                  local.SlimCount(g, i) += cnt;
                  if ((*local.slim_kinds)[i] == SlimAggKind::kSumDouble &&
                      !std::isfinite(local.SlimDoubleTotal(g, i)))
                      [[unlikely]] {
                    // Only contributing rows in this group can authorize
                    // an infinite total. An excluded infinity (or one from
                    // another group) must not hide finite-input overflow.
                    const BlockSelectivityMode selected_mode =
                        build_block_selection();
                    for (uint32_t si = 0; si < blk_matched; ++si) {
                      const uint32_t r =
                          selected_mode == BlockSelectivityMode::kDenseAllPass
                              ? si
                              : blk_sel_rows[si];
                      if (dict_group_key) {
                        const auto& first =
                            blk.columns[proj_to_full[plan.key_offsets[0]]];
                        const size_t pair_code =
                            plan.key_offsets.size() == 1
                                ? static_cast<size_t>(first.dict_codes[r])
                                : (static_cast<size_t>(first.dict_codes[r]) *
                                   dict_group_stride1) +
                                      blk.columns
                                          [proj_to_full[plan.key_offsets[1]]]
                                              .dict_codes[r];
                        if (dict_pair_to_group[pair_code] != g) {
                          continue;
                        }
                      }
                      double addend = 0.0;
                      if (plan.aggregates[i].column.has_value()) {
                        addend = std::bit_cast<double>(
                            blk.columns
                                [proj_to_full[*plan.aggregates[i].column]]
                                    .int_cells[r]);
                      } else if (plan.aggregates[i].double_expr.has_value()) {
                        const TypedDoubleExprInput& de =
                            *plan.aggregates[i].double_expr;
                        const auto c0 = std::bit_cast<double>(
                            blk.columns[proj_to_full[de.col0]].int_cells[r]);
                        if (de.shape ==
                            TypedDoubleExprInput::Shape::kSingleDoubleCol) {
                          addend = c0;
                        } else {
                          const auto c1 = std::bit_cast<double>(
                              blk.columns[proj_to_full[de.col1]].int_cells[r]);
                          if (de.shape ==
                              TypedDoubleExprInput::Shape::kMulTwoDoubleCols) {
                            addend = c0 * c1;
                          } else if (de.shape == TypedDoubleExprInput::Shape::
                                                     kMulConstMinusCol) {
                            addend = c0 * (de.k0 - c1);
                          } else {
                            const auto c2 = std::bit_cast<double>(
                                blk.columns[proj_to_full[de.col2]]
                                    .int_cells[r]);
                            addend = c0 * (de.k0 - c1) * (de.k1 + c2);
                          }
                        }
                      }
                      if (std::isinf(addend)) {
                        local.SlimFlags(g, i) |= 1;
                        break;
                      }
                    }
                  }
                }
              }
            }
            continue;
          }

          const LSTableColumnBlock* const rf_col =
              (lstable_rf != nullptr && active_key_col.has_value())
                  ? &blk.columns[*active_key_col]
                  : nullptr;
          const bool rf_has_dense =
              rf_col != nullptr && lstable_rf->has_dense_bits();
          const uint64_t* const rf_dense_bits =
              rf_has_dense ? lstable_rf->dense_bits_data() : nullptr;
          const size_t rf_dense_words =
              rf_has_dense ? lstable_rf->dense_bits_word_count() : 0;
          const int64_t rf_lo = rf_has_dense ? lstable_rf->Lo() : 0;
          const int64_t rf_hi = rf_has_dense ? lstable_rf->Hi() : 0;
          const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
          const bool rf_col_monotonic =
              rf_has_dense && !rf_col_has_null &&
              *active_key_col <
                  direct_lstable_store->column_is_monotonic.size() &&
              direct_lstable_store->column_is_monotonic[*active_key_col] != 0;

          for (uint32_t r = 0; r < blk.row_count; ++r) {
            if (rf_col != nullptr) {
              if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
                continue;
              }
              const int64_t key_val = rf_col->int_cells[r];
              if (rf_has_dense) {
                bool accepted = false;
                if (key_val >= rf_lo && key_val <= rf_hi) {
                  const uint64_t off = static_cast<uint64_t>(key_val) -
                                       static_cast<uint64_t>(rf_lo);
                  accepted = (rf_dense_bits[off >> 6] &
                              (uint64_t{1} << (off & 63))) != 0;
                }
                if (!accepted) {
                  if (rf_col_monotonic) {
                    if (key_val > rf_hi) {
                      break;
                    }
                    int64_t next_valid_key = rf_lo;
                    if (key_val >= rf_lo) {
                      const uint64_t next_off = static_cast<uint64_t>(key_val) -
                                                static_cast<uint64_t>(rf_lo) +
                                                1ULL;
                      auto w_idx = static_cast<size_t>(next_off >> 6);
                      if (w_idx >= rf_dense_words) {
                        break;
                      }
                      uint64_t word =
                          rf_dense_bits[w_idx] & (~0ULL << (next_off & 63));
                      while (word == 0) {
                        if (++w_idx >= rf_dense_words) {
                          break;
                        }
                        word = rf_dense_bits[w_idx];
                      }
                      if (w_idx >= rf_dense_words) {
                        break;
                      }
                      const uint64_t found_off =
                          (static_cast<uint64_t>(w_idx) << 6) +
                          static_cast<uint64_t>(std::countr_zero(word));
                      next_valid_key = rf_lo + static_cast<int64_t>(found_off);
                      if (next_valid_key > rf_hi) {
                        break;
                      }
                    }
                    if (rf_col->has_min_max &&
                        next_valid_key > rf_col->max_int) {
                      break;
                    }
                    const uint32_t row_count = blk.row_count;
                    const int64_t* const cells = rf_col->int_cells.data();
                    if (r + 1 < row_count && cells[r + 1] < next_valid_key) {
                      uint32_t lo = r + 1;
                      uint32_t step = 4;
                      while (lo + step < row_count &&
                             cells[lo + step] < next_valid_key) {
                        lo += step;
                        step <<= 1;
                      }
                      const uint32_t hi = std::min(row_count, lo + step + 1);
                      const int64_t* found = std::lower_bound(
                          cells + lo, cells + hi, next_valid_key);
                      r = static_cast<uint32_t>(found - cells) - 1;
                    }
                  }
                  continue;
                }
              } else {
                if (!have_rf_cache || key_val != last_rf_key) {
                  have_rf_cache = true;
                  last_rf_key = key_val;
                  last_rf_rejected = lstable_rf->Reject(key_val);
                }
                if (last_rf_rejected) {
                  continue;
                }
              }
            }
            if (stream_prebuilt_membership != nullptr) {
              const LSTableColumnBlock& mcol =
                  blk.columns[proj_to_full[stream_prebuilt_membership->column]];
              if (mcol.IsNull(r) ||
                  !stream_prebuilt_membership->values->Contains(
                      mcol.int_cells[r])) {
                continue;
              }
            }
            if (!peek_all_pass && peek_ptr != nullptr && !peek_ptr->empty()) {
              bool peek_ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk.columns[pred.column];
                if (pcol.IsNull(r) ||
                    !MatchIntegerCompare(pcol.int_cells[r], pred.op,
                                         pred.constant)) {
                  peek_ok = false;
                  break;
                }
              }
              if (!peek_ok) {
                continue;
              }
            }
            ++counters[worker].scanned;

            const auto is_null = [&](slot_t c) noexcept -> bool {
              return blk.has_any_null && blk.columns[proj_to_full[c]].IsNull(r);
            };
            const auto make_value = [&](slot_t c) -> Value {
              const LSTableColumnBlock& col = blk.columns[proj_to_full[c]];
              Value value;
              switch (proj_types[c]) {
                case ValueType::kDouble:
                  value.type = ValueType::kDouble;
                  value.value.double_value =
                      std::bit_cast<double>(col.int_cells[r]);
                  break;
                case ValueType::kVarChar:
                  value.type = ValueType::kVarChar;
                  value.value.varchar_value = col.VarcharAt(r);
                  break;
                default:
                  value.type = proj_types[c];
                  value.value.int_value = col.int_cells[r];
                  break;
              }
              if (value.type == ValueType::kInt64 && proj_unsigned[c] != 0) {
                value.SetUnsigned();
              }
              return value;
            };
            const auto eval_filter_compare =
                [&](const TypedFilterCompare& compare) -> bool {
              const LSTableColumnBlock& lcol =
                  blk.columns[proj_to_full[compare.left]];
              if (lcol.IsNull(r)) {
                return false;
              }
              if (compare.constant_payload) {
                if (compare.string_payload) {
                  const std::string_view sv = lcol.VarcharAt(r);
                  return MatchTypedStringCompare(sv, compare);
                }
                if (compare.is_double) {
                  const auto a = std::bit_cast<double>(lcol.int_cells[r]);
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
                const int64_t a = lcol.int_cells[r];
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
              const LSTableColumnBlock& rcol =
                  blk.columns[proj_to_full[compare.right]];
              if (rcol.IsNull(r)) {
                return false;
              }
              if (compare.is_double) {
                const auto a = std::bit_cast<double>(lcol.int_cells[r]);
                const auto b = std::bit_cast<double>(rcol.int_cells[r]);
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
              const int64_t a = lcol.int_cells[r];
              const int64_t b = rcol.int_cells[r];
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
            };
            const auto eval_double_expr = [&](const TypedDoubleExprInput& expr,
                                              double* out) -> bool {
              switch (expr.shape) {
                case TypedDoubleExprInput::Shape::kSingleDoubleCol: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  if (c0.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]);
                  return true;
                }
                case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  const LSTableColumnBlock& c1 =
                      blk.columns[proj_to_full[expr.col1]];
                  if (c0.IsNull(r) || c1.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]) *
                         std::bit_cast<double>(c1.int_cells[r]);
                  return true;
                }
                case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  const LSTableColumnBlock& c1 =
                      blk.columns[proj_to_full[expr.col1]];
                  if (c0.IsNull(r) || c1.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]) *
                         (expr.k0 - std::bit_cast<double>(c1.int_cells[r]));
                  return true;
                }
                case TypedDoubleExprInput::Shape::
                    kMulConstMinusColMulConstPlusCol: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  const LSTableColumnBlock& c1 =
                      blk.columns[proj_to_full[expr.col1]];
                  const LSTableColumnBlock& c2 =
                      blk.columns[proj_to_full[expr.col2]];
                  if (c0.IsNull(r) || c1.IsNull(r) || c2.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]) *
                         (expr.k0 - std::bit_cast<double>(c1.int_cells[r])) *
                         (expr.k1 + std::bit_cast<double>(c2.int_cells[r]));
                  return true;
                }
                case TypedDoubleExprInput::Shape::kGeneric:
                  break;
              }
              std::array<double, 16> stack{};
              size_t sp = 0;
              // Producers cap ops at 16 entries so pushes can never exceed
              // the stack; keep that bound explicit here.
              if (expr.ops.size() > stack.size()) {
                return false;
              }
              for (const TypedDoubleExprOp& op : expr.ops) {
                switch (op.kind) {
                  case TypedDoubleExprOp::Kind::kLoadDoubleCol: {
                    const LSTableColumnBlock& c =
                        blk.columns[proj_to_full[op.column]];
                    if (c.IsNull(r)) {
                      return false;
                    }
                    stack[sp++] = std::bit_cast<double>(c.int_cells[r]);
                    break;
                  }
                  case TypedDoubleExprOp::Kind::kLoadIntColAsDouble: {
                    const LSTableColumnBlock& c =
                        blk.columns[proj_to_full[op.column]];
                    if (c.IsNull(r)) {
                      return false;
                    }
                    stack[sp++] = static_cast<double>(c.int_cells[r]);
                    break;
                  }
                  case TypedDoubleExprOp::Kind::kConstant:
                    stack[sp++] = op.constant;
                    break;
                  case TypedDoubleExprOp::Kind::kAdd:
                    stack[sp - 2] += stack[sp - 1];
                    --sp;
                    break;
                  case TypedDoubleExprOp::Kind::kSub:
                    stack[sp - 2] -= stack[sp - 1];
                    --sp;
                    break;
                  case TypedDoubleExprOp::Kind::kMul:
                    stack[sp - 2] *= stack[sp - 1];
                    --sp;
                    break;
                }
              }
              *out = stack[0];
              return true;
            };
            const auto eval_int_expr = [&](const TypedIntExprInput& expr,
                                           int64_t* out,
                                           Status* error) -> bool {
              std::array<int64_t, 16> stack{};
              size_t sp = 0;
              // Producers cap ops at 16 entries so pushes can never exceed
              // the stack; keep that bound explicit here.
              if (expr.ops.size() > stack.size()) {
                *error =
                    StatusError(StatusCode::kInvalidArgument,
                                "typed INT expression operand stack overflow");
                return false;
              }
              for (const TypedIntExprOp& op : expr.ops) {
                switch (op.kind) {
                  case TypedIntExprOp::Kind::kLoadIntCol: {
                    const LSTableColumnBlock& c =
                        blk.columns[proj_to_full[op.column]];
                    if (c.IsNull(r)) {
                      return false;
                    }
                    stack[sp++] = c.int_cells[r];
                    break;
                  }
                  case TypedIntExprOp::Kind::kConstant:
                    stack[sp++] = op.constant;
                    break;
                  case TypedIntExprOp::Kind::kAdd: {
                    int64_t v = 0;
                    if (__builtin_add_overflow(stack[sp - 2], stack[sp - 1],
                                               &v)) {
                      *error = StatusError(StatusCode::kInvalidArgument,
                                           "integer overflow on '+'");
                      return false;
                    }
                    stack[sp - 2] = v;
                    --sp;
                    break;
                  }
                  case TypedIntExprOp::Kind::kSub: {
                    int64_t v = 0;
                    if (__builtin_sub_overflow(stack[sp - 2], stack[sp - 1],
                                               &v)) {
                      *error = StatusError(StatusCode::kInvalidArgument,
                                           "integer overflow on '-'");
                      return false;
                    }
                    stack[sp - 2] = v;
                    --sp;
                    break;
                  }
                  case TypedIntExprOp::Kind::kMul: {
                    int64_t v = 0;
                    if (__builtin_mul_overflow(stack[sp - 2], stack[sp - 1],
                                               &v)) {
                      *error = StatusError(StatusCode::kInvalidArgument,
                                           "integer overflow on '*'");
                      return false;
                    }
                    stack[sp - 2] = v;
                    --sp;
                    break;
                  }
                }
              }
              *out = stack[0];
              return true;
            };

            if (!block_all_pass) {
              bool keep = true;
              for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
                const LSTableColumnBlock& dcol = blk.columns[df.full_column];
                if (dcol.dict_codes.empty() ||
                    df.pass[dcol.dict_codes[r]] == 0) {
                  keep = false;
                  break;
                }
              }
              if (!keep) {
                continue;
              }
              const bool filter_compares_done_in_peek =
                  peek_ptr != nullptr && compares_covered_by_peeks;
              if (!filter_compares_done_in_peek) {
                for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
                  if (filter_compare_covered[fi] != 0) {
                    continue;
                  }
                  const auto& compare = plan.filter_compares[fi];
                  const auto& mask = wide_string_filters[fi];
                  const bool matches =
                      mask ? mask->Matches(
                                 blk.columns[proj_to_full[compare.left]]
                                     .wide_code_begin +
                                 r)
                           : eval_filter_compare(compare);
                  if (!matches) {
                    keep = false;
                    break;
                  }
                }
                if (!keep) {
                  continue;
                }
              }
              if (!match_disjunctions(eval_filter_compare)) {
                continue;
              }
              for (size_t li = 0; li < plan.inlist_compares.size(); ++li) {
                if (inlist_compare_covered[li] != 0) {
                  continue;
                }
                const TypedInListCompare& list = plan.inlist_compares[li];
                const LSTableColumnBlock& lcol =
                    blk.columns[proj_to_full[list.column]];
                if (lcol.IsNull(r)) {
                  keep = false;
                  break;
                }
                bool matched = false;
                if (list.is_string) {
                  matched = MatchTypedStringInList(lcol.VarcharAt(r), list);
                } else if (list.is_double) {
                  const auto v = std::bit_cast<double>(lcol.int_cells[r]);
                  for (const double item : list.double_values) {
                    if (v == item) {
                      matched = true;
                      break;
                    }
                  }
                } else {
                  const int64_t v = lcol.int_cells[r];
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
              if (!keep) {
                continue;
              }
            }

            ++counters[worker].emitted;
            size_t group_index = 0;
            if (plan.key_offsets.empty()) {
              if (!has_ungrouped) {
                bool inserted = false;
                ungrouped_idx =
                    local.FindOrInsert({}, 0, plan.aggregates.size(),
                                       aggregate_expressions, &inserted);
                has_ungrouped = true;
              }
              group_index = ungrouped_idx;
            } else if (monotonic_int_key) {
              const int64_t raw_key =
                  blk.columns[mono_key_full_col].int_cells[r];
              if (!has_mono_key) {
                has_mono_key = true;
                cur_mono_key = raw_key;
                cur_mono_group = local.AppendMonotonicIntGroup(
                    raw_key, mono_key_type, mono_key_unsigned);
                bool emp = false;
                *local.int_index.TryEmplace(raw_key, &emp) = cur_mono_group;
              } else if (raw_key != cur_mono_key) {
                if (cur_mono_group > 0 && having_expr != nullptr &&
                    !positional_having.empty()) {
                  const bool passed =
                      check_slim_group_having(local, cur_mono_group, &err);
                  if (err != Status::kSuccess) {
                    return;
                  }
                  if (!passed) {
                    local.ResetMonotonicIntGroup(cur_mono_group, raw_key,
                                                 mono_key_type,
                                                 mono_key_unsigned);
                    cur_mono_key = raw_key;
                    ++reclaimed_groups[worker];
                  } else {
                    cur_mono_group = local.AppendMonotonicIntGroup(
                        raw_key, mono_key_type, mono_key_unsigned);
                    cur_mono_key = raw_key;
                  }
                } else {
                  cur_mono_group = local.AppendMonotonicIntGroup(
                      raw_key, mono_key_type, mono_key_unsigned);
                  cur_mono_key = raw_key;
                }
              }
              group_index = cur_mono_group;
            } else if (wide_group_dictionary_size != 0) {
              const slot_t key_offset = plan.key_offsets[0];
              const auto& column = blk.columns[proj_to_full[key_offset]];
              const size_t code = is_null(key_offset)
                                      ? wide_group_dictionary_size
                                      : column.DictionaryCodeAt(r);
              auto& cached = wide_code_to_group[code];
              if (cached == UINT32_MAX) {
                scratch.clear();
                scratch.push_back(
                    is_null(key_offset)
                        ? Value()
                        : CanonicalDistinctValue(make_value(key_offset)));
                bool inserted = false;
                cached = static_cast<uint32_t>(
                    local.FindOrInsert(scratch, 0, plan.aggregates.size(),
                                       aggregate_expressions, &inserted));
              }
              group_index = cached;
            } else if (dict_group_key) {
              const slot_t full0 = proj_to_full[plan.key_offsets[0]];
              const uint8_t c0 = blk.columns[full0].dict_codes[r];
              size_t pair_code = c0;
              if (plan.key_offsets.size() == 2) {
                const slot_t full1 = proj_to_full[plan.key_offsets[1]];
                const uint8_t c1 = blk.columns[full1].dict_codes[r];
                pair_code = (static_cast<size_t>(c0) * dict_group_stride1) + c1;
              }
              uint16_t g = dict_pair_to_group[pair_code];
              if (g == 0xFFFFU) [[unlikely]] {
                scratch.clear();
                for (const slot_t offset : plan.key_offsets) {
                  scratch.push_back(CanonicalDistinctValue(make_value(offset)));
                }
                int64_t raw_key = 0;
                bool inserted = false;
                const size_t new_g =
                    local.FindOrInsert(scratch, raw_key, plan.aggregates.size(),
                                       aggregate_expressions, &inserted);
                g = static_cast<uint16_t>(new_g);
                dict_pair_to_group[pair_code] = g;
              }
              group_index = g;
            } else if (int_key && !plan.key_offsets.empty() &&
                       !is_null(plan.key_offsets[0])) {
              const LSTableColumnBlock& key_col =
                  blk.columns[proj_to_full[plan.key_offsets[0]]];
              if (r + 8 < blk.row_count) {
                local.PrefetchGroupSlot(key_col.int_cells[r + 8]);
              }
              const int64_t raw_key = key_col.int_cells[r];
              const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_key);
              if (local.mru_groups[mru_idx] != SIZE_MAX &&
                  local.mru_keys[mru_idx] == raw_key) {
                group_index = local.mru_groups[mru_idx];
              } else {
                bool inserted = false;
                size_t* slot = local.RawIntSlot(raw_key, &inserted);
                if (inserted) {
                  scratch.clear();
                  for (const slot_t offset : plan.key_offsets) {
                    scratch.push_back(
                        CanonicalDistinctValue(make_value(offset)));
                  }
                  group_index = local.ClaimSlotForNewGroup(
                      slot, scratch, plan.aggregates.size(),
                      aggregate_expressions);
                } else {
                  group_index = *slot;
                }
                local.mru_keys[mru_idx] = raw_key;
                local.mru_groups[mru_idx] = group_index;
              }
            } else if (packed_varchar_key) {
              uint64_t packed = 0;
              bool can_pack = true;
              for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
                const slot_t offset = plan.key_offsets[k];
                uint32_t part = 0;
                if (is_null(offset) ||
                    !TryPackShortVarcharSpan(
                        blk.columns[proj_to_full[offset]].VarcharAt(r),
                        &part)) {
                  can_pack = false;
                  break;
                }
                packed |= static_cast<uint64_t>(part) << (k * 32);
              }
              if (can_pack) {
                const auto raw_packed = static_cast<int64_t>(packed);
                const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_packed);
                if (local.mru_groups[mru_idx] != SIZE_MAX &&
                    local.mru_keys[mru_idx] == raw_packed) {
                  group_index = local.mru_groups[mru_idx];
                } else {
                  bool inserted = false;
                  size_t* slot = local.RawIntSlot(raw_packed, &inserted);
                  if (inserted) {
                    scratch.clear();
                    for (const slot_t offset : plan.key_offsets) {
                      scratch.push_back(
                          CanonicalDistinctValue(make_value(offset)));
                    }
                    group_index = local.ClaimSlotForNewGroup(
                        slot, scratch, plan.aggregates.size(),
                        aggregate_expressions);
                  } else {
                    group_index = *slot;
                  }
                  local.mru_keys[mru_idx] = raw_packed;
                  local.mru_groups[mru_idx] = group_index;
                }
              } else {
                scratch.clear();
                for (const slot_t offset : plan.key_offsets) {
                  if (is_null(offset)) {
                    scratch.emplace_back();
                  } else {
                    scratch.push_back(
                        CanonicalDistinctValue(make_value(offset)));
                  }
                }
                bool inserted = false;
                group_index =
                    local.FindOrInsert(scratch, 0, plan.aggregates.size(),
                                       aggregate_expressions, &inserted);
              }
            } else {
              scratch.clear();
              int64_t raw_key = 0;
              for (const slot_t offset : plan.key_offsets) {
                if (is_null(offset)) {
                  scratch.emplace_back();
                } else {
                  scratch.push_back(CanonicalDistinctValue(make_value(offset)));
                }
                if (scratch.size() == 1 && int_key) {
                  raw_key = scratch.back().value.int_value;
                }
              }
              bool inserted = false;
              group_index =
                  local.FindOrInsert(scratch, raw_key, plan.aggregates.size(),
                                     aggregate_expressions, &inserted);
            }

            ++counters[worker].updates;
            if (local.slim) {
              const std::vector<SlimAggKind>& kinds = *local.slim_kinds;
              for (size_t i = 0; i < plan.aggregates.size(); ++i) {
                const TypedAggregateInput& input = plan.aggregates[i];
                const SlimAggKind kind = kinds[i];
                if (byte_length_columns[i]) {
                  const slot_t column = *byte_length_columns[i];
                  if (is_null(column)) {
                    continue;
                  }
                  const auto size =
                      blk.columns[proj_to_full[column]].VarcharAt(r).size();
                  fold_row_values[i] =
                      input.child->ResultType() == ValueType::kDouble
                          ? Value(static_cast<double>(size))
                          : Value(static_cast<int64_t>(size));
                  if (!ApplySlimValue(&local, group_index, i, kind,
                                      &fold_row_values[i], &err)) {
                    return;
                  }
                  continue;
                }
                if (input.double_expr.has_value()) {
                  double addend = 0.0;
                  if (!eval_double_expr(*input.double_expr, &addend)) {
                    continue;
                  }
                  if (kind == SlimAggKind::kSumDouble) {
                    if (std::isinf(addend)) {
                      local.SlimFlags(group_index, i) |= 1;
                    }
                    local.SlimDoubleTotal(group_index, i) += addend;
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  if (kind == SlimAggKind::kAvgDouble) {
                    local.SlimDoubleTotal(group_index, i) += addend;
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  fold_row_values[i] = Value(addend);
                  if (!ApplySlimValue(&local, group_index, i, kind,
                                      &fold_row_values[i], &err)) {
                    return;
                  }
                  continue;
                }
                if (input.int_expr.has_value()) {
                  int64_t addend = 0;
                  if (!eval_int_expr(*input.int_expr, &addend, &err)) {
                    if (err != Status::kSuccess) {
                      return;
                    }
                    continue;
                  }
                  if (kind == SlimAggKind::kSumInt64) {
                    int64_t updated = 0;
                    if (__builtin_add_overflow(
                            local.SlimIntTotal(group_index, i), addend,
                            &updated)) {
                      err = StatusError(StatusCode::kIsInfinity,
                                        "integer overflow in SUM");
                      return;
                    }
                    local.SlimIntTotal(group_index, i) = updated;
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  if (kind == SlimAggKind::kAvgDouble) {
                    local.SlimDoubleTotal(group_index, i) +=
                        static_cast<double>(addend);
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  fold_row_values[i] = Value(addend);
                  if (!ApplySlimValue(&local, group_index, i, kind,
                                      &fold_row_values[i], &err)) {
                    return;
                  }
                  continue;
                }
                ValueType cell_type = ValueType::kNull;
                int64_t cell = 0;
                bool cell_unsigned = false;
                std::optional<slot_t> fallback_col;
                if (input.conditional.has_value()) {
                  if (!eval_filter_compare(input.conditional->condition)) {
                    if (!input.conditional->has_else_constant) {
                      continue;
                    }
                    cell_type = input.conditional->result_type;
                    cell = input.conditional->else_int_constant;
                  } else if (input.conditional->then_column.has_value()) {
                    const slot_t col = *input.conditional->then_column;
                    if (is_null(col)) {
                      continue;
                    }
                    cell_type = proj_types[col];
                    if (cell_type == ValueType::kInt64 ||
                        cell_type == ValueType::kDouble ||
                        cell_type == ValueType::kDate) {
                      cell = blk.columns[proj_to_full[col]].int_cells[r];
                    }
                    cell_unsigned = proj_unsigned[col] != 0;
                    fallback_col = col;
                  } else {
                    cell_type = input.conditional->result_type;
                    cell = input.conditional->then_int_constant;
                  }
                } else if (input.column.has_value()) {
                  const slot_t column = *input.column;
                  if (is_null(column)) {
                    continue;
                  }
                  cell_type = proj_types[column];
                  if (cell_type == ValueType::kInt64 ||
                      cell_type == ValueType::kDouble ||
                      cell_type == ValueType::kDate) {
                    cell = blk.columns[proj_to_full[column]].int_cells[r];
                  }
                  cell_unsigned = proj_unsigned[column] != 0;
                  fallback_col = column;
                } else {
                  if (kind == SlimAggKind::kCount) {
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  if (!ApplySlimValue(&local, group_index, i, kind, nullptr,
                                      &err)) {
                    return;
                  }
                  continue;
                }
                bool handled = true;
                if (!ApplySlimCell(&local, group_index, i, kind, cell_type,
                                   cell, cell_unsigned, &handled, &err)) {
                  return;
                }
                if (handled) {
                  continue;
                }
                if (fallback_col.has_value()) {
                  fold_row_values[i] = make_value(*fallback_col);
                } else {
                  fold_row_values[i] = Value(cell);
                  fold_row_values[i].type = cell_type;
                }
                if (!ApplySlimValue(&local, group_index, i, kind,
                                    &fold_row_values[i], &err)) {
                  return;
                }
              }
              continue;
            }

            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              const TypedAggregateInput& input = plan.aggregates[i];
              AggregateAccumulator& accumulator = local.State(group_index, i);
              if (input.column.has_value()) {
                const Value value = is_null(*input.column)
                                        ? Value()
                                        : make_value(*input.column);
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (input.conditional.has_value()) {
                Value value;
                if (eval_filter_compare(input.conditional->condition)) {
                  if (input.conditional->then_column.has_value()) {
                    const slot_t col = *input.conditional->then_column;
                    if (!is_null(col)) {
                      value = make_value(col);
                    }
                  } else {
                    value = Value(input.conditional->then_int_constant);
                    value.type = input.conditional->result_type;
                  }
                } else if (input.conditional->has_else_constant) {
                  value = Value(input.conditional->else_int_constant);
                }
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (input.double_expr.has_value()) {
                double dbl = 0.0;
                const Value value = eval_double_expr(*input.double_expr, &dbl)
                                        ? Value(dbl)
                                        : Value();
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (input.int_expr.has_value()) {
                int64_t iv = 0;
                const bool ok = eval_int_expr(*input.int_expr, &iv, &err);
                if (err != Status::kSuccess) {
                  return;
                }
                const Value value = ok ? Value(iv) : Value();
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (Status st = accumulator.Add(Value(1));
                  st != Status::kSuccess) {
                err = st;
                return;
              }
            }
          }
        }
        if (monotonic_int_key && local.group_count > 1) {
          bool emp = false;
          *local.int_index.TryEmplace(cur_mono_key, &emp) =
              local.group_count - 1;
        }
        return;
      }
      // Morsels share the projected schema. Keep decode storage for the whole
      // worker so short morsels do not allocate and zero a full batch each.
      TypedCellBatch batch;
      for (size_t morsel = morsel_begin; morsel < morsel_end;
           morsel += morsel_step) {
        if (worker_errors[worker] != Status::kSuccess) {
          break;
        }
        Iterator iterator = table.BeginMorselScan(
            context.txn_, morsels[morsel], projection_opt, nullptr,
            active_key_col, peek_ptr, active_rf, active_rf_type);
        if (typed_fill) {
          // Typed fill: decode schema-typed cell batches (no per-cell Value),
          // fold from raw cells, mirror every row into the morsel's arena run.
          // The published cache then holds the arena WITHOUT Rows; consumers
          // that cannot stream cells materialize rows from the arena on
          // demand (see the cache-hit path above).
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
            counters[worker].scanned += decoded;
            iterator.DropPageLatch();
            if (fill_cache) {
              AppendBatchToArena(batch, 0, decoded, &fill_buffer.mirror);
            }
            if (bulk_count) {
              bool inserted = false;
              const size_t group = workers[worker].FindOrInsert(
                  {}, 0, plan.aggregates.size(), aggregate_expressions,
                  &inserted);
              for (size_t aggregate = 0; aggregate < plan.aggregates.size();
                   ++aggregate) {
                auto& count = workers[worker].SlimCount(group, aggregate);
                if (std::cmp_greater(
                        decoded, std::numeric_limits<int64_t>::max() - count)) {
                  worker_errors[worker] = StatusError(
                      StatusCode::kIsInfinity, "integer overflow in COUNT");
                  break;
                }
                count += static_cast<int64_t>(decoded);
              }
              counters[worker].emitted += decoded;
              counters[worker].updates += decoded;
              if (!worker_errors[worker].ok()) {
                break;
              }
              continue;
            }
            // Slim folds index the fixed 8-slot fold_row_values scratch by
            // aggregate slot; more than 8 aggregates must take the generic
            // decoded-row path (accumulate_row) instead.
            const bool direct_fold =
                plan.filter_programs.empty() &&
                (!plan.slim || plan.aggregates.size() <= 8) &&
                std::ranges::none_of(plan.aggregates, [](const auto& input) {
                  return input.child.has_value();
                });
            Row decoded_row(std::vector<Value>(batch.stride));
            for (size_t r = 0; r < decoded; ++r) {
              if (!direct_fold) {
                size_t varchar_slot = 0;
                for (size_t c = 0; c < batch.stride; ++c) {
                  Value value;
                  if (!batch.nulls[(r * batch.stride) + c]) {
                    if (batch.slot_types[c] == ValueType::kVarChar) {
                      value =
                          Value(std::string(batch.Varchar(r, varchar_slot)));
                    } else if (batch.slot_types[c] == ValueType::kDouble) {
                      value = Value(std::bit_cast<double>(
                          batch.cells[(r * batch.stride) + c]));
                    } else {
                      value = Value(batch.cells[(r * batch.stride) + c]);
                      value.type = batch.slot_types[c];
                      if (batch.slot_unsigned[c]) {
                        value = value.WithUnsigned();
                      }
                    }
                  }
                  if (batch.slot_types[c] == ValueType::kVarChar) {
                    ++varchar_slot;
                  }
                  decoded_row.values_[c] = std::move(value);
                }
                accumulate_row(decoded_row, &workers[worker],
                               &worker_errors[worker],
                               &counters[worker].emitted,
                               &counters[worker].updates, nullptr, &scratch);
                if (!worker_errors[worker].ok()) {
                  break;
                }
                continue;
              }
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
                            &counters[worker].emitted,
                            &counters[worker].updates, &scratch,
                            &fold_row_values);
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
          if (worker_errors[worker] != Status::kSuccess) {
            break;
          }
          if (fill_cache) {
            publish_morsel(morsel, &fill_buffer);
          }
          continue;
        }
        MorselFillBuffer fill_buffer;
        if (fill_cache && fill_arena != nullptr) {
          fill_buffer.arena_stride = fill_arena->stride;
        }
        MorselFillBuffer* fill_target = fill_cache ? &fill_buffer : nullptr;
        for (; iterator.IsValid(); ++iterator) {
          ++counters[worker].scanned;
          accumulate_row(*iterator, &workers[worker], &worker_errors[worker],
                         &counters[worker].emitted, &counters[worker].updates,
                         fill_target, &scratch);
          if (worker_errors[worker] != Status::kSuccess) {
            break;
          }
        }
        if (worker_errors[worker] == Status::kSuccess &&
            iterator.GetStatus() != Status::kSuccess) {
          worker_errors[worker] = iterator.GetStatus();
        }
        if (worker_errors[worker] != Status::kSuccess) {
          break;
        }
        if (fill_cache) {
          publish_morsel(morsel, &fill_buffer);
        }
      }
    } catch (const std::exception& error) {
      worker_errors[worker] =
          StatusError(StatusCode::kRuntimeError, error.what());
    } catch (...) {
      worker_errors[worker] =
          StatusError(StatusCode::kRuntimeError, "parallel aggregate failed");
    }
  };
  phase("prepare", worker_count, 0);
  RETURN_IF_FAIL(RunParallelTasks(worker_count, run_worker));
  phase("fold", worker_count, 0);
  for (const Status& error : worker_errors) {
    if (error != Status::kSuccess) {
      return error;
    }
  }
  if (runtime != nullptr) {
    const size_t dictionary_rows =
        std::accumulate(counters.begin(), counters.end(), size_t{0},
                        [](size_t total, const WorkerCounters& worker) {
                          return total + worker.dictionary_rows;
                        });
    if (dictionary_rows != 0) {
      ++runtime->dictionary_aggregations;
      runtime->dictionary_aggregate_rows += dictionary_rows;
    }
  }

  // Publish the cache: the in-order morsel moves above already built the
  // relation while workers ran; once every worker joined, all morsels of a
  // successful run are guaranteed published.
  if (fill_cache) {
    RETURN_IF_FAIL(cache_owner->FinishSpill());
    const std::string cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    runtime->base_relations.emplace(cache_key, cache_owner);
    if (fill_arena != nullptr && fill_arena->rows == published_rows) {
      fill_arena->published_rowsless = typed_fill;
      runtime->base_relation_arenas.emplace(cache_key, fill_arena);
    }
  } else if (publish_lstable_marker && runtime != nullptr) {
    auto marker = std::make_shared<Relation>(runtime);
    marker->schema = qualified_schema;
    runtime->base_relations.emplace(fold_cache_key, std::move(marker));
  }

  // Merge worker states in worker order (deterministic group creation order).
  if (fast_dict_double_agg && bounded_int_group_key) {
    const ValueType ktype = proj_types[plan.key_offsets[0]];
    const bool kunsigned = proj_unsigned[plan.key_offsets[0]] != 0;
    const size_t emit_workers = (bounded_int_span >= 4096 && worker_count > 1 &&
                                 plan.order_items.empty() && !bounded &&
                                 (!having_expr || !positional_having.empty()))
                                    ? worker_count
                                    : 1;
    for (size_t ew = 0; ew < emit_workers; ++ew) {
      const size_t idx_begin = (bounded_int_span * ew) / emit_workers;
      const size_t idx_end = (bounded_int_span * (ew + 1)) / emit_workers;
      TypedWorkerGroups& dst = workers[ew];
      for (size_t idx = idx_begin; idx < idx_end; ++idx) {
        uint64_t total_cnt = 0;
        double total_sum = 0.0;
        int64_t total_isum = 0;
        for (size_t w = 0; w < worker_count; ++w) {
          if (!bounded_worker_counts[w].empty()) {
            const uint32_t c = bounded_worker_counts[w][idx];
            if (c != 0) {
              total_cnt += c;
              if (bounded_has_double_term) {
                total_sum += bounded_worker_sums[w][idx];
              }
              if (bounded_has_int_term) {
                total_isum += bounded_worker_int_sums[w][idx];
              }
            }
          }
        }
        if (total_cnt == 0) {
          continue;
        }
        const int64_t raw_key = bounded_int_min + static_cast<int64_t>(idx);
        const size_t g = dst.AppendMonotonicIntGroup(raw_key, ktype, kunsigned);
        for (size_t i = 0; i < plan.aggregates.size(); ++i) {
          dst.SlimCount(g, i) = static_cast<int64_t>(total_cnt);
          const SlimAggKind kind = (*dst.slim_kinds)[i];
          if (kind == SlimAggKind::kSumInt64) {
            dst.SlimIntTotal(g, i) = total_isum;
          } else if (kind == SlimAggKind::kSumDouble ||
                     kind == SlimAggKind::kAvgDouble) {
            const double val = bounded_has_int_term
                                   ? static_cast<double>(total_isum)
                                   : total_sum;
            dst.SlimDoubleTotal(g, i) = val;
            // Flag only when an input term was infinite; an infinite total
            // from finite inputs is an overflow the finalizer must raise.
            if (std::isinf(val) && bounded_has_double_term) [[unlikely]] {
              for (size_t w = 0; w < worker_count; ++w) {
                if (!bounded_worker_inf[w].empty() &&
                    bounded_worker_inf[w][idx] != 0) {
                  dst.SlimFlags(g, i) |= 1;
                  break;
                }
              }
            }
          }
        }
      }
    }
  }
  TypedWorkerGroups merged;
  merged.int_key = int_key;
  merged.packed_varchar_key = packed_varchar_key;
  merged.shapes = &aggregate_shapes;
  merged.Configure(!flat_fold_disabled, plan.key_offsets.size(),
                   plan.aggregates.size());
  merged.ConfigureSlim(plan);
  size_t total_worker_groups = 0;
  size_t total_reclaimed_groups = 0;
  for (size_t worker = 0; worker < worker_count; ++worker) {
    total_worker_groups += workers[worker].GroupCount();
    total_reclaimed_groups += reclaimed_groups[worker];
  }
  bool can_parallel_disjoint_emit =
      int_key && plan.key_offsets.size() == 1 && !flat_fold_disabled &&
      plan.slim && plan.dependent_key_programs.empty() &&
      (total_worker_groups + total_reclaimed_groups >= 4096 ||
       monotonic_int_key ||
       (fast_dict_double_agg && bounded_int_group_key &&
        bounded_int_span >= 4096)) &&
      plan.order_items.empty() && !bounded &&
      (!having_expr || !positional_having.empty());
  if (can_parallel_disjoint_emit) {
    size_t prev_w = worker_count;
    for (size_t w = 0; w < worker_count; ++w) {
      if (workers[w].GroupCount() == 0) {
        continue;
      }
      if (workers[w].has_non_int_key || workers[w].indexed_groups != 0 ||
          workers[w].min_int_key > workers[w].max_int_key) {
        can_parallel_disjoint_emit = false;
        break;
      }
      if (prev_w < worker_count &&
          workers[prev_w].max_int_key > workers[w].min_int_key) {
        can_parallel_disjoint_emit = false;
        break;
      }
      prev_w = w;
    }
  }
  if (can_parallel_disjoint_emit) {
    std::vector<size_t> skipped_group(worker_count, SIZE_MAX);
    // Where each skipped boundary group was absorbed (worker, group).  When a
    // single-group worker B (min == max == K) is itself absorbed into the
    // previous worker A, the next worker C must merge into A's group, not
    // into B's skipped one, or C's partial would never be emitted.
    std::vector<std::pair<size_t, size_t>> skipped_into(worker_count,
                                                        {SIZE_MAX, SIZE_MAX});
    size_t boundary_merged_groups = 0;
    size_t prev_w = worker_count;
    for (size_t w = 0; w < worker_count; ++w) {
      if (workers[w].GroupCount() == 0) {
        continue;
      }
      if (prev_w < worker_count &&
          workers[prev_w].max_int_key == workers[w].min_int_key) {
        const int64_t bkey = workers[w].min_int_key;
        const size_t* dst_slot = workers[prev_w].int_index.Find(bkey);
        const size_t* src_slot = workers[w].int_index.Find(bkey);
        if (dst_slot != nullptr && src_slot != nullptr) {
          size_t dst_w = prev_w;
          size_t target = *dst_slot;
          while (skipped_group[dst_w] == target) {
            const auto [next_w, next_group] = skipped_into[dst_w];
            dst_w = next_w;
            target = next_group;
          }
          const size_t group = *src_slot;
          TypedWorkerGroups& dst = workers[dst_w];
          TypedWorkerGroups& src = workers[w];
          for (size_t i = 0; i < plan.aggregates.size(); ++i) {
            const SlimAggKind kind = (*dst.slim_kinds)[i];
            if (IsSlimDistinctKind(kind)) {
              if (src.SlimDistinctTotal(group, i) == 0) {
                continue;
              }
              const size_t src_state = (group * src.agg_stride) + i;
              const uint8_t n = src.slim_distinct_n[src_state];
              const int64_t* slots =
                  &src.slim_distinct[src_state * kSlimDistinctSlots];
              for (size_t s = 0; s < n; ++s) {
                static_cast<void>(dst.SlimInsertDistinct(target, i, slots[s]));
              }
              if (src_state < src.slim_distinct_overflow.size() &&
                  src.slim_distinct_overflow[src_state]) {
                src.slim_distinct_overflow[src_state]->ForEach(
                    [&](int64_t raw, uint8_t) {
                      static_cast<void>(dst.SlimInsertDistinct(target, i, raw));
                    });
              }
              continue;
            }
            Status merge_err = Status::kSuccess;
            if (!MergeSlimCell(&dst, target, src, group, i, kind, &merge_err)) {
              return merge_err;
            }
          }
          skipped_group[w] = group;
          skipped_into[w] = {dst_w, target};
          ++boundary_merged_groups;
        }
      }
      prev_w = w;
    }

    size_t total_scan_rows = 0;
    size_t total_scan_output = 0;
    for (size_t worker = 0; worker < worker_count; ++worker) {
      total_scan_rows += counters[worker].scanned;
      total_scan_output += counters[worker].emitted;
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
      runtime->aggregate_groups +=
          total_worker_groups + total_reclaimed_groups - boundary_merged_groups;
      runtime->aggregate_input_rows += total_scan_output;
      for (const auto& counter : counters) {
        runtime->aggregate_updates += counter.updates;
      }
    }

    const auto aggregate_begin = std::chrono::steady_clock::now();
    std::vector<std::vector<Row>> worker_outputs(worker_count);
    std::vector<Status> emit_errors(worker_count, Status::kSuccess);
    const auto emit_worker = [&](size_t w) {
      try {
        TypedWorkerGroups& local = workers[w];
        const size_t gcount = local.GroupCount();
        if (gcount > 0) {
          std::vector<Value> local_finished;
          local_finished.reserve(plan.aggregates.size());
          auto& out_rows = worker_outputs[w];
          if (!having_expr) {
            out_rows.reserve(gcount);
          }
          for (size_t group = 0; group < gcount; ++group) {
            if (group == skipped_group[w]) {
              continue;
            }
            local_finished.clear();
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              Value finished;
              if (!FinishSlimCell(local, group, i, (*local.slim_kinds)[i],
                                  &finished, &emit_errors[w])) {
                return;
              }
              local_finished.push_back(std::move(finished));
            }
            if (having_expr) {
              bool keep_group = true;
              for (const PositionalHaving& clause : positional_having) {
                const Value* right_ptr =
                    clause.rhs_is_agg ? &local_finished[clause.rhs_agg_index]
                                      : nullptr;
                keep_group = EvalPositionalHavingValues(
                    clause, local_finished[clause.agg_index], right_ptr,
                    &emit_errors[w]);
                if (!emit_errors[w].ok()) {
                  return;
                }
                if (!keep_group) {
                  break;
                }
              }
              if (!keep_group) {
                continue;
              }
            }
            std::vector<Value> values;
            values.reserve(plan.emit_items.size());
            for (const TypedEmitItem& item : plan.emit_items) {
              if (const PostAggExpr* post_agg = item.post_agg.get()) {
                // Composite item: evaluate it over this group's results.
                std::vector<Value> cells;
                cells.reserve(post_agg->slots.size());
                for (const auto& slot : post_agg->slots) {
                  cells.push_back(slot.is_key
                                      ? local.GroupKey(group, slot.index)
                                      : local_finished[slot.index]);
                }
                StatusOr<Value> evaluated = post_agg->expr->TryEvaluate(
                    Row(std::move(cells)), post_agg->eval_schema);
                if (!evaluated.HasValue()) {
                  emit_errors[w] = evaluated.GetStatus();
                  return;
                }
                values.push_back(evaluated.MoveValue());
              } else if (item.constant) {
                values.push_back(*item.constant);
              } else if (item.is_key) {
                values.push_back(local.GroupKey(group, item.index));
              } else {
                values.push_back(local_finished[item.index]);
              }
            }
            out_rows.emplace_back(std::move(values));
          }
        }
        local = TypedWorkerGroups{};
      } catch (...) {
        emit_errors[w] = StatusFromCurrentException("emit worker failed");
      }
    };
    std::vector<std::jthread> emit_threads;
    emit_threads.reserve(worker_count - 1);
    for (size_t w = 1; w < worker_count; ++w) {
      try {
        emit_threads.emplace_back([&, w] { emit_worker(w); });
      } catch (...) {
        emit_worker(w);
      }
    }
    emit_worker(0);
    emit_threads.clear();
    for (const Status& err : emit_errors) {
      if (err != Status::kSuccess) {
        return err;
      }
    }
    std::vector<Column> output_columns;
    output_columns.reserve(stmt.SelectList().size());
    for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
      output_columns.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                  ValueType::kNull);
    }
    Relation output(runtime);
    size_t total_emitted = 0;
    for (const auto& out_rows : worker_outputs) {
      total_emitted += out_rows.size();
    }
    const size_t reserve_rows = QueryMemoryBudget::Global().CanReserve(
                                    (total_emitted * sizeof(Row)) + 64)
                                    ? total_emitted
                                    : std::min<size_t>(total_emitted, 65536);
    output.rows.reserve(reserve_rows);
    bool schema_types_set = false;
    for (auto& out_rows : worker_outputs) {
      for (Row& r : out_rows) {
        if (!schema_types_set) {
          for (size_t i = 0; i < output_columns.size(); ++i) {
            output_columns[i] =
                Column(output_columns[i].Name(), ValueTypeOf(r[i]));
            output_columns[i].SetUnsigned(r[i].IsUnsigned());
          }
          schema_types_set = true;
        }
        output.AddRow(std::move(r));
      }
    }
    if (runtime != nullptr) {
      runtime->aggregate_ms += ElapsedMs(aggregate_begin);
    }
    output.schema = Schema("", std::move(output_columns));
    return output;
  }
  if (monotonic_int_key && !can_parallel_disjoint_emit) {
    for (TypedWorkerGroups& local : workers) {
      if (local.GroupCount() > 0) {
        for (size_t g = 0; g < local.GroupCount(); ++g) {
          bool emp = false;
          *local.int_index.TryEmplace(local.GroupKey(g, 0).value.int_value,
                                      &emp) = g;
        }
        break;
      }
    }
  }
  size_t total_scan_rows = 0;
  size_t total_scan_output = 0;
  for (size_t worker = 0; worker < worker_count; ++worker) {
    total_scan_rows += counters[worker].scanned;
    total_scan_output += counters[worker].emitted;
    TypedWorkerGroups& local = workers[worker];
    if (local.GroupCount() == 0) {
      continue;
    }
    if (merged.GroupCount() == 0) {
      merged.int_index = std::move(local.int_index);
      merged.key_index = std::move(local.key_index);
      merged.indexed_groups = local.indexed_groups;
      local.indexed_groups = 0;
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
      if (total_worker_groups > merged.group_count) {
        merged.ReserveGroups(total_worker_groups);
      }
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
      if (merged.int_key && !local.GroupKey(group, 0).IsNull()) {
        const Value& local_key0 = local.GroupKey(group, 0);
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
            if (IsSlimDistinctKind((*merged.slim_kinds)[i])) {
              const size_t dst_state = (target * merged.agg_stride) + i;
              const size_t src_state = (group * local.agg_stride) + i;
              merged.slim_distinct_n[dst_state] =
                  local.slim_distinct_n[src_state];
              std::copy_n(
                  &local.slim_distinct[src_state * kSlimDistinctSlots],
                  kSlimDistinctSlots,
                  &merged.slim_distinct[dst_state * kSlimDistinctSlots]);
              if (src_state < local.slim_distinct_overflow.size() &&
                  local.slim_distinct_overflow[src_state]) {
                if (dst_state >= merged.slim_distinct_overflow.size()) {
                  merged.slim_distinct_overflow.resize(
                      merged.slim_distinct_n.size());
                }
                merged.slim_distinct_overflow[dst_state] =
                    std::move(local.slim_distinct_overflow[src_state]);
              }
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
          if (IsSlimDistinctKind(kind)) {
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
            if (src_state < local.slim_distinct_overflow.size() &&
                local.slim_distinct_overflow[src_state]) {
              local.slim_distinct_overflow[src_state]->ForEach([&](int64_t raw,
                                                                   uint8_t) {
                static_cast<void>(merged.SlimInsertDistinct(target, i, raw));
              });
            }
            continue;
          }
          Status merge_err = Status::kSuccess;
          if (!MergeSlimCell(&merged, target, local, group, i, kind,
                             &merge_err)) {
            return merge_err;
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
  phase("merge", worker_count, merged.GroupCount());
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
    for (const auto& counter : counters) {
      runtime->aggregate_updates += counter.updates;
    }
  }

  // Global aggregate over an empty input produces one group of NULL/0.
  if (merged.GroupCount() == 0 &&
      (stmt.GroupBy().empty() || stmt.IsGroupAll())) {
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
  sortable.reserve(bounded ? std::min(topn_capacity, merged.GroupCount())
                           : merged.GroupCount());
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
  // The group count is final before emit; reserving up front keeps the rows
  // vector from re-moving every emitted Row on geometric growth.
  output.rows.reserve(bounded
                          ? std::min(topn_capacity, merged.GroupCount())
                          : (having_expr != nullptr
                                 ? std::min<size_t>(merged.GroupCount(), 1024)
                                 : merged.GroupCount()));
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
  const size_t merged_group_count = merged.GroupCount();
  for (size_t group = 0; group < merged_group_count; ++group) {
    ASSIGN_OR_RETURN(std::vector<Value>, dependent_keys,
                     EvaluateDependentKeys(plan, merged, group));
    finished_values.clear();
    finished_values.reserve(plan.aggregates.size());
    if (merged.slim) {
      Status finish_err = Status::kSuccess;
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        Value finished;
        if (!FinishSlimCell(merged, group, i, (*merged.slim_kinds)[i],
                            &finished, &finish_err)) {
          return finish_err;
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
    if (having_expr && !positional_having.empty()) {
      // `AGG op literal`, `AGG op AGG`, or bare `AGG` (or a conjunction of
      // them): decide straight from the finished values.  A NULL aggregate
      // result compares to UNKNOWN, so the group drops.
      bool keep_group = true;
      Status having_err = Status::kSuccess;
      for (const PositionalHaving& clause : positional_having) {
        const Value* right_ptr = clause.rhs_is_agg
                                     ? &finished_values[clause.rhs_agg_index]
                                     : nullptr;
        keep_group = EvalPositionalHavingValues(
            clause, finished_values[clause.agg_index], right_ptr, &having_err);
        if (!having_err.ok()) {
          return having_err;
        }
        if (!keep_group) {
          break;
        }
      }
      if (!keep_group) {
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
    KeyedRow keyed;
    if (!plan.order_items.empty()) {
      keyed.sequence = next_sequence;
      keyed.keys.reserve(plan.order_items.size());
      for (const TypedEmitItem& item : plan.order_items) {
        if (const PostAggExpr* post_agg = item.post_agg.get()) {
          std::vector<Value> cells;
          cells.reserve(post_agg->slots.size());
          for (const auto& slot : post_agg->slots) {
            cells.push_back(slot.is_key ? merged.GroupKey(group, slot.index)
                                        : finished_values[slot.index]);
          }
          const Row eval_row(std::move(cells));
          ASSIGN_OR_RETURN(
              Value, evaluated,
              post_agg->expr->TryEvaluate(eval_row, post_agg->eval_schema));
          keyed.keys.push_back(std::move(evaluated));
          continue;
        }
        if (item.dependent_key) {
          keyed.keys.push_back(dependent_keys[*item.dependent_key]);
          continue;
        }
        keyed.keys.push_back(
            item.constant ? *item.constant
                          : (item.is_key ? merged.GroupKey(group, item.index)
                                         : finished_values[item.index]));
      }
      // Aggregate errors and HAVING have already been evaluated. Output
      // strings and other payload columns are needed only for heap winners.
      if (bounded &&
          (topn_capacity == 0 || (sortable.size() == topn_capacity &&
                                  !precedes(keyed, sortable.front())))) {
        ++next_sequence;
        continue;
      }
    }
    std::vector<Value> values;
    values.reserve(plan.emit_items.size());
    for (const TypedEmitItem& item : plan.emit_items) {
      if (const PostAggExpr* post_agg = item.post_agg.get()) {
        // Composite select item (ROUND(SUM(x), 4), SUM(x) + 1): evaluate it
        // over the finished aggregates / group keys. Emitting the item's
        // default index instead returned the first raw aggregate.
        std::vector<Value> cells;
        cells.reserve(post_agg->slots.size());
        for (const auto& slot : post_agg->slots) {
          cells.push_back(slot.is_key ? merged.GroupKey(group, slot.index)
                                      : finished_values[slot.index]);
        }
        const Row eval_row(std::move(cells));
        ASSIGN_OR_RETURN(
            Value, evaluated,
            post_agg->expr->TryEvaluate(eval_row, post_agg->eval_schema));
        values.push_back(std::move(evaluated));
      } else if (item.dependent_key) {
        values.push_back(dependent_keys[*item.dependent_key]);
      } else if (item.constant) {
        values.push_back(*item.constant);
      } else if (item.is_key) {
        values.push_back(merged.GroupKey(group, item.index));
      } else {
        values.push_back(finished_values[item.index]);
      }
    }
    Row output_row(std::move(values));
    if (output.TotalRows() == 0 && sortable.empty()) {
      for (size_t i = 0; i < output_columns.size(); ++i) {
        output_columns[i] =
            Column(output_columns[i].Name(), ValueTypeOf(output_row[i]));
        output_columns[i].SetUnsigned(output_row[i].IsUnsigned());
      }
    }
    if (!plan.order_items.empty()) {
      keyed.row = std::move(output_row);
      retain(std::move(keyed));
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
  phase("finish", worker_count, merged.GroupCount());
  output.schema = Schema("", std::move(output_columns));
  return output;
}

}  // namespace tinylamb::relational_detail
