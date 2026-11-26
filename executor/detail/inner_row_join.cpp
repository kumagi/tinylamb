/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_join.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/row_join_internal.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/rewrite.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {
using namespace row_join_detail;
StatusOr<Relation> InnerJoin(TransactionContext& context, Relation left,
                             Relation right,
                             const std::vector<Expression>& predicates,
                             const Scope* outer, const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const size_t raw_key_count = equality_keys.size();
  RETURN_IF_FAIL(DisableRawCollatedKeys(left, right, &equality_keys));
  const bool allow_varchar_elision = equality_keys.size() == raw_key_count;
  const std::vector<Expression> residual =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? predicates
          : ResidualJoinPredicates(left.schema, right.schema, predicates,
                                   allow_varchar_elision);

  const auto compiled_residuals =
      outer == nullptr ? CompileProbeResiduals(residual, result.schema)
                       : std::nullopt;
  const auto* probe_residuals =
      compiled_residuals && !compiled_residuals->empty() ? &*compiled_residuals
                                                         : nullptr;
  const CompiledScanFilter residual_filter =
      CompileScanFilter(residual, result.schema);
  Status residual_error{Status::kSuccess};
  auto matches = [&](const Row& combined) {
    if (residual.empty()) {
      return true;
    }
    if (residual_error != Status::kSuccess) {
      return false;
    }
    // Join residuals share WHERE's scalar/three-valued semantics. Compile
    // once instead of walking the expression tree for every matching pair.
    return MatchScanFilter(combined, result.schema, residual_filter, outer,
                           context, ctes, &residual_error);
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    RETURN_IF_FAIL(left.FinishSpill());
    RETURN_IF_FAIL(right.FinishSpill());
    ASSIGN_OR_RETURN(
        std::optional<Relation>, cross_col,
        TryColumnarCrossOrNullJoin(context, left, right, false, false,
                                   result.schema, residual, nullptr, result));
    if (cross_col.has_value()) {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      return *std::move(cross_col);
    }
    if (right.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(right.TotalRows() * 128)) {
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
    }
    Row combined_scratch{std::vector<Value>{}};
    if (!right.HasSpill()) {
      RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
        const Status st = right.ForEachRow([&](const Row& right_row) {
          ++result.join_comparisons;
          ConcatRowsInto(&combined_scratch, left_row, right_row);
          if (matches(combined_scratch)) {
            result.AddRow(Row(combined_scratch));
          }
        });
        if (st != Status::kSuccess && residual_error == Status::kSuccess) {
          residual_error = st;
        }
      }));
    } else {
      // A spilled inner would otherwise be re-read from disk once per
      // outer row.  Stream it once in budget-bounded chunks and re-scan
      // the outer per chunk (block nested loop).
      const Status chunked = ForEachRowChunk(
          right, NestedLoopInnerChunkBytes(),
          [&](const std::vector<Row>& inner_rows, size_t) -> Status {
            return left.ForEachRow([&](const Row& left_row) {
              for (const Row& right_row : inner_rows) {
                ++result.join_comparisons;
                ConcatRowsInto(&combined_scratch, left_row, right_row);
                if (matches(combined_scratch)) {
                  result.AddRow(Row(combined_scratch));
                }
              }
            });
          });
      if (chunked != Status::kSuccess && residual_error == Status::kSuccess) {
        residual_error = chunked;
      }
    }
  } else {
    ++result.hash_joins;
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    // Choose the build before deciding whether to partition. A spilled
    // probe does not require repartitioning when the smaller build still fits:
    // ForEachRow streams its resident prefix and spill files exactly once.
    static const bool flip_disabled =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JOIN_FLIP") != nullptr;
    const bool flip = !flip_disabled && !has_null_safe_key &&
                      right.TotalRows() >= kFlipMinBuildRows &&
                      right.TotalRows() > left.TotalRows() * kFlipRatio &&
                      !left.HasSpill();
    Relation* build_side_mut = flip ? &left : &right;
    const Relation& build_side = *build_side_mut;
    Relation& probe_side = flip ? right : left;
    const size_t index_bytes =
        build_side.TotalRows() *
        (build_side.HasColumnarOnly()
             ? ((build_side.schema.ColumnCount() * 24) + 64)
             : 64);
    const bool hybrid = build_side.HasSpill() ||
                        !QueryMemoryBudget::Global().CanReserve(index_bytes);
    // D8 (docs/design.md): null-safe keys use the hybrid operator too, so a
    // spilled input is never silently reduced to its resident prefix.
    // Biased keys degrade to residuals here: the hybrid operator cannot
    // bias, and the residual verifier keeps results exact.
    if (std::ranges::any_of(equality_keys,
                            [](const EqualityKey& key) {
                              return key.left_offset != 0 ||
                                     key.right_offset != 0;
                            }) &&
        (has_null_safe_key || hybrid)) {
      std::vector<EqualityKey> plain_keys;
      plain_keys.reserve(equality_keys.size());
      for (const EqualityKey& key : equality_keys) {
        if (key.left_offset == 0 && key.right_offset == 0) {
          plain_keys.push_back(key);
        }
      }
      equality_keys = std::move(plain_keys);
    }
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    std::vector<int64_t> left_key_offsets;
    left_key_offsets.reserve(equality_keys.size());
    std::vector<int64_t> right_key_offsets;
    right_key_offsets.reserve(equality_keys.size());
    left_columns.reserve(equality_keys.size());
    right_columns.reserve(equality_keys.size());
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
      left_key_offsets.push_back(key.left_offset);
      right_key_offsets.push_back(key.right_offset);
    }
    if (hybrid) {
      ++result.hybrid_hash_joins;
      ASSIGN_OR_RETURN(
          Relation, joined,
          (HybridHashJoin(std::move(left), std::move(right), left_columns,
                          right_columns, matches, false,
                          &result.join_comparisons, has_null_safe_key, false)));
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      RETURN_IF_FAIL(residual_error);
      return joined;
    }
    ++result.in_memory_hash_joins;
    QueryMemoryCharge index_charge(index_bytes);
    if (auto* runtime = context.execution_runtime()) {
      runtime->inner_hash_build_rows += build_side.TotalRows();
      runtime->inner_hash_index_estimate_bytes += index_bytes;
      runtime->inner_hash_left_builds += flip ? 1 : 0;
      runtime->inner_hash_spilled_probe_rows +=
          probe_side.HasSpill() ? probe_side.TotalRows() : 0;
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, joined_col,
        (TryColumnarHashJoin(context, left, right, flip, false, false,
                             has_null_safe_key, false, left_columns,
                             right_columns, left_key_offsets, right_key_offsets,
                             residual, compiled_residuals, nullptr, result)));
    if (joined_col.has_value()) {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      return *std::move(joined_col);
    }
    if (build_side_mut->HasColumnarOnly()) {
      RETURN_IF_FAIL(build_side_mut->EnsureRowsMaterialized());
    }
    if (probe_side.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(probe_side.TotalRows() * 128)) {
      RETURN_IF_FAIL(probe_side.EnsureRowsMaterialized());
    }
    // Build the hash on the smaller side and stream the bigger side: the
    // build's random bucket writes scale with the build row count while a
    // streamed probe walks its rows once.  Emitted rows keep the
    // original-left ++ original-right column order; only the walk direction
    // flips, so the output row order may differ from the unflipped join
    // (inner-join consumers are order-insensitive; ORDER BY applies later).
    // Inner joins only: outer emissions and shared matched flags assume the
    // unflipped roles.
    const std::vector<slot_t>& build_columns =
        flip ? left_columns : right_columns;
    const std::vector<slot_t>& probe_columns =
        flip ? right_columns : left_columns;
    const std::vector<int64_t>& build_key_offsets =
        flip ? left_key_offsets : right_key_offsets;
    const std::vector<int64_t>& probe_key_offsets =
        flip ? right_key_offsets : left_key_offsets;
    const bool can_parallelize =
        (residual.empty() || compiled_residuals.has_value()) &&
        !has_null_safe_key && !probe_side.HasSpill() &&
        probe_side.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize
            ? std::min<size_t>(
                  kParallelProbeMaxWorkers,
                  QueryParallelWorkerLimit(std::thread::hardware_concurrency()))
            : 1;
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    const auto no_mark = [](const Row&) noexcept {};
    const auto no_unmatched = [](const Row&) noexcept {};
    const bool stream_is_left = !flip;
    const size_t fixed_row_bytes =
        FixedJoinRowBytes(left, right, result.schema);
    if (integer_key) {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
          build_side, probe_side, probe_columns, false, stream_is_left, nullptr,
          nullptr, nullptr, fixed_row_bytes, can_parallelize, probe_workers,
          probe_residuals, &result, &residual_error,
          [&](const Row& row) { return HasNullKey(row, build_columns); },
          [&](const Row& row) {
            return AddJoinKeyBias(IntegerJoinKey(row, build_columns[0]),
                                  build_key_offsets[0]);
          },
          [&](const Row& row) { return HasNullKey(row, probe_columns); },
          [&](const Row& row) {
            return AddJoinKeyBias(IntegerJoinKey(row, probe_columns[0]),
                                  probe_key_offsets[0]);
          },
          matches, no_mark, no_unmatched));
    } else if (!has_null_safe_key &&
               CompositeIntJoinKey(left.schema, left_columns, right.schema,
                                   right_columns)) {
      constexpr auto kInvalidPacked2x32 = static_cast<int64_t>(
          (static_cast<uint64_t>(0x80000000ULL) << 32) | 0x80000000ULL);
      auto fits_i32 = [](int64_t v) noexcept {
        return v >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
               v <= static_cast<int64_t>(std::numeric_limits<int32_t>::max());
      };
      auto pack_2x32 = [](int64_t k0, int64_t k1) noexcept -> int64_t {
        return static_cast<int64_t>(
            (static_cast<uint64_t>(
                 static_cast<uint32_t>(static_cast<int32_t>(k0)))
             << 32) |
            static_cast<uint64_t>(
                static_cast<uint32_t>(static_cast<int32_t>(k1))));
      };
      bool can_pack_2x32 = (build_columns.size() == 2);
      if (can_pack_2x32) {
        for (const Row& row : build_side.rows) {
          if (HasNullKey(row, build_columns)) {
            continue;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, build_columns[0]), build_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, build_columns[1]), build_key_offsets[1]);
          if (!fits_i32(k0) || !fits_i32(k1) ||
              pack_2x32(k0, k1) == kInvalidPacked2x32) {
            can_pack_2x32 = false;
            break;
          }
        }
      }
      if (can_pack_2x32) {
        RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
            build_side, probe_side, probe_columns, false, stream_is_left,
            nullptr, nullptr, nullptr, fixed_row_bytes, can_parallelize,
            probe_workers, probe_residuals, &result, &residual_error,
            [&](const Row& row) { return HasNullKey(row, build_columns); },
            [&](const Row& row) {
              const int64_t k0 = AddJoinKeyBias(
                  IntegerJoinKey(row, build_columns[0]), build_key_offsets[0]);
              const int64_t k1 = AddJoinKeyBias(
                  IntegerJoinKey(row, build_columns[1]), build_key_offsets[1]);
              return pack_2x32(k0, k1);
            },
            [&](const Row& row) { return HasNullKey(row, probe_columns); },
            [&](const Row& row) noexcept -> int64_t {
              const int64_t k0 = AddJoinKeyBias(
                  IntegerJoinKey(row, probe_columns[0]), probe_key_offsets[0]);
              const int64_t k1 = AddJoinKeyBias(
                  IntegerJoinKey(row, probe_columns[1]), probe_key_offsets[1]);
              if (!fits_i32(k0) || !fits_i32(k1)) {
                return kInvalidPacked2x32;
              }
              return pack_2x32(k0, k1);
            },
            matches, no_mark, no_unmatched));
      } else {
        RETURN_IF_FAIL(
            (ExecuteFlatBucketJoin<CompositeIntKey, CompositeIntKeyHash>(
                build_side, probe_side, probe_columns, false, stream_is_left,
                nullptr, nullptr, nullptr, fixed_row_bytes, can_parallelize,
                probe_workers, probe_residuals, &result, &residual_error,
                [&](const Row& row) { return HasNullKey(row, build_columns); },
                [&](const Row& row) {
                  return EncodeCompositeIntKey(row, build_columns,
                                               &build_key_offsets);
                },
                [&](const Row& row) { return HasNullKey(row, probe_columns); },
                [&](const Row& row) {
                  return EncodeCompositeIntKey(row, probe_columns,
                                               &probe_key_offsets);
                },
                matches, no_mark, no_unmatched)));
      }
    } else if (!has_null_safe_key &&
               CanBorrowJoinKeys(build_side, probe_side, build_columns,
                                 probe_columns, build_key_offsets,
                                 probe_key_offsets)) {
      // Every predicate contributed exactly one equality key. Validated
      // domains and overflow-free biases make a bucket hit the final verdict.
      const bool exact_keys = predicates.size() == equality_keys.size();
      const bool parallel =
          can_parallelize ||
          (exact_keys && probe_side.rows.size() >= kParallelProbeMinLeftRows);
      const size_t borrowed_workers =
          can_parallelize
              ? probe_workers
              : std::min<size_t>(kParallelProbeMaxWorkers,
                                 QueryParallelWorkerLimit(
                                     std::thread::hardware_concurrency()));
      RETURN_IF_FAIL(
          (ExecuteFlatBucketJoin<BorrowedJoinKey, BorrowedJoinKeyHash>(
              build_side, probe_side, probe_columns, false, stream_is_left,
              nullptr, nullptr, nullptr, fixed_row_bytes, parallel,
              borrowed_workers, probe_residuals, &result, &residual_error,
              [&](const Row& row) { return HasNullKey(row, build_columns); },
              [&](const Row& row) {
                return BorrowedJoinKey{.row = &row,
                                       .columns = &build_columns,
                                       .biases = &build_key_offsets};
              },
              [&](const Row& row) { return HasNullKey(row, probe_columns); },
              [&](const Row& row) {
                return BorrowedJoinKey{.row = &row,
                                       .columns = &probe_columns,
                                       .biases = &probe_key_offsets};
              },
              [&](const Row& combined) {
                return exact_keys || matches(combined);
              },
              no_mark, no_unmatched)));
    } else {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<std::string>(
          build_side, probe_side, probe_columns, false, stream_is_left, nullptr,
          nullptr, nullptr, fixed_row_bytes, can_parallelize, probe_workers,
          probe_residuals, &result, &residual_error,
          [&](const Row& row) {
            return !has_null_safe_key && HasNullKey(row, build_columns);
          },
          [&](const Row& row) {
            return has_null_safe_key
                       ? EncodeNullSafeJoinKey(row, build_columns)
                       : EncodeJoinKey(row, build_columns, &build_key_offsets);
          },
          [&](const Row& row) {
            return !has_null_safe_key && HasNullKey(row, probe_columns);
          },
          [&](const Row& row) {
            return has_null_safe_key
                       ? EncodeNullSafeJoinKey(row, probe_columns)
                       : EncodeJoinKey(row, probe_columns, &probe_key_offsets);
          },
          matches, no_mark, no_unmatched));
    }
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (left.uniform_cell_types && right.uniform_cell_types &&
      !result.HasSpill()) {
    result.uniform_cell_types = true;
  }
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_ms += ElapsedMs(join_begin);
  }
  if (residual_error == Status::kSuccess && result.join_comparisons == 0 &&
      !residual.empty()) {
    Row dummy_row(std::vector<Value>(result.schema.ColumnCount()));
    Scope scope{.row = &dummy_row, .schema = &result.schema, .outer = outer};
    scope.using_columns = result.using_columns.get();
    const auto validate_columns = [&](const auto& self,
                                      const Expression& expr) -> Status {
      if (!expr || expr->Type() == TypeTag::kQueryExp) {
        return Status::kSuccess;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (col.name != "*") {
          StatusOr<Value> val = TryLookup(col, scope);
          if (!val.HasValue()) {
            return val.GetStatus();
          }
        }
        return Status::kSuccess;
      }
      for (const Expression& child : ExpressionChildren(expr)) {
        RETURN_IF_FAIL(self(self, child));
      }
      return Status::kSuccess;
    };
    for (const Expression& predicate : residual) {
      const Status st = validate_columns(validate_columns, predicate);
      if (st != Status::kSuccess) {
        residual_error = st;
        break;
      }
    }
  }
  RETURN_IF_FAIL(residual_error);
  return result;
}

}  // namespace tinylamb::relational_detail
