/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_join.hpp"
#include "executor/detail/columnar_table.hpp"
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
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
using namespace row_join_detail;
StatusOr<Relation> Join(TransactionContext& context, Relation left,
                        Relation right, const SelectSource& source,
                        const Scope* outer, const CteMap& ctes) {
  // An outer join elsewhere in the statement fixes the surrounding join
  // tree, not the build side of an inner edge. Reuse the inner operator's
  // smaller-build selection while retaining left-then-right column order.
  // USING metadata belongs to the ordered operator and stays on that path.
  if (source.join_type == JoinType::kInner && source.using_columns.empty() &&
      left.using_columns == nullptr && right.using_columns == nullptr &&
      right.TotalRows() >= kFlipMinBuildRows &&
      right.TotalRows() > left.TotalRows() * kFlipRatio && !left.HasSpill() &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_ORDERED_INNER_FLIP") ==
          nullptr) {
    const auto predicates = SplitConjuncts(source.join_condition);
    const Schema flip_schema = left.schema + right.schema;
    if (std::ranges::all_of(predicates, [&](const Expression& pred) {
          return SafeToReduceEvaluationCountWithSchema(pred, flip_schema);
        })) {
      return InnerJoin(context, std::move(left), std::move(right), predicates,
                       outer, ctes);
    }
  }
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
  std::vector<std::string> merged_using;
  auto append_using = [&](const std::vector<std::string>& names) {
    for (const std::string& name : names) {
      const std::string lowered = AsciiToLower(name);
      if (!ContainsIdentifier(merged_using, lowered)) {
        merged_using.push_back(lowered);
      }
    }
  };
  if (left.using_columns != nullptr) {
    append_using(*left.using_columns);
  }
  if (right.using_columns != nullptr) {
    append_using(*right.using_columns);
  }
  append_using(source.using_columns);
  std::vector<std::string> retained_using;
  retained_using.reserve(merged_using.size());
  for (const std::string& name : merged_using) {
    const bool declared_now = ContainsIdentifier(source.using_columns, name);
    const bool right_collision =
        !MatchingColumnOffsets(right.schema, name).empty();
    if (!declared_now && right_collision) {
      continue;
    }
    retained_using.push_back(name);
  }
  if (!retained_using.empty()) {
    result.using_columns = std::make_shared<const std::vector<std::string>>(
        std::move(retained_using));
  }
  const bool want_left_nulls = source.join_type == JoinType::kLeft ||
                               source.join_type == JoinType::kFull;
  const bool want_right_nulls = source.join_type == JoinType::kRight ||
                                source.join_type == JoinType::kFull;
  RETURN_IF_FAIL(left.FinishSpill());
  RETURN_IF_FAIL(right.FinishSpill());
  RETURN_IF_FAIL(left.GetStatus());
  RETURN_IF_FAIL(right.GetStatus());
  const std::vector<Expression> predicates =
      SplitConjuncts(source.join_condition);
  std::vector<Expression> ordinary_predicates;
  ordinary_predicates.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    if (!IsDeclaredUsingEquality(predicate, source.using_columns)) {
      ordinary_predicates.push_back(predicate);
    }
  }
  std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, ordinary_predicates);
  const size_t raw_key_count = equality_keys.size();
  RETURN_IF_FAIL(DisableRawCollatedKeys(left, right, &equality_keys));
  const bool allow_varchar_elision = equality_keys.size() == raw_key_count;
  struct UsingJoinKey {
    std::vector<size_t> left_columns;
    std::vector<size_t> right_columns;
  };
  std::vector<UsingJoinKey> using_residuals;
  using_residuals.reserve(source.using_columns.size());
  for (const std::string& name : source.using_columns) {
    std::vector<size_t> left_columns = MatchingColumnOffsets(left.schema, name);
    std::vector<size_t> right_columns =
        MatchingColumnOffsets(right.schema, name);
    if (left_columns.empty() || right_columns.empty()) {
      continue;
    }
    if (left_columns.size() > 1 &&
        (left.using_columns == nullptr ||
         !ContainsIdentifier(*left.using_columns, name))) {
      return StatusError(StatusCode::kAmbiguousQuery,
                         "ambiguous USING column: " + name);
    }
    if (right_columns.size() > 1 &&
        (right.using_columns == nullptr ||
         !ContainsIdentifier(*right.using_columns, name))) {
      return StatusError(StatusCode::kAmbiguousQuery,
                         "ambiguous USING column: " + name);
    }
    // USING columns with the same ValueType but different unsigned tags do
    // NOT compare equal under the canonical `=` (-1 INT64 vs UINT64_MAX).
    // Only fold into a raw equality key when both sides share the tag;
    // otherwise keep a residual that rechecks with TryEvaluateBinary.
    const bool same_single_type =
        left_columns.size() == 1 && right_columns.size() == 1 &&
        left.schema.GetColumn(left_columns.front()).Type() ==
            right.schema.GetColumn(right_columns.front()).Type() &&
        left.schema.GetColumn(left_columns.front()).IsUnsigned() ==
            right.schema.GetColumn(right_columns.front()).IsUnsigned();
    if (same_single_type) {
      const EqualityKey key{.left = left_columns.front(),
                            .right = right_columns.front(),
                            .null_safe = false};
      ASSIGN_OR_RETURN(bool, collated,
                       RequiresCollatedJoinComparison(left, right, key));
      if (!collated) {
        equality_keys.push_back(key);
        continue;
      }
    }
    using_residuals.push_back(
        {std::move(left_columns), std::move(right_columns)});
  }
  const std::vector<Expression> residual =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? ordinary_predicates
          : ResidualJoinPredicates(left.schema, right.schema,
                                   ordinary_predicates, allow_varchar_elision);
  const auto compiled_residuals =
      outer == nullptr && using_residuals.empty()
          ? CompileProbeResiduals(residual, result.schema)
          : std::nullopt;
  const auto* probe_residuals =
      compiled_residuals && !compiled_residuals->empty() ? &*compiled_residuals
                                                         : nullptr;
  Status residual_error{Status::kSuccess};
  auto matches = [&](const Row& combined) {
    const auto merged_value = [&](const std::vector<size_t>& columns,
                                  const size_t offset) -> const Value* {
      for (const size_t column : columns) {
        const Value& value = combined[column + offset];
        if (!value.IsNull()) {
          return &value;
        }
      }
      return nullptr;
    };
    for (const UsingJoinKey& key : using_residuals) {
      const Value* left_value = merged_value(key.left_columns, 0);
      const Value* right_value =
          merged_value(key.right_columns, left.schema.ColumnCount());
      if (left_value == nullptr || right_value == nullptr) {
        return false;
      }
      if ((left_value->type == ValueType::kDouble &&
           std::isnan(left_value->value.double_value)) ||
          (right_value->type == ValueType::kDouble &&
           std::isnan(right_value->value.double_value))) {
        return false;
      }
      StatusOr<Value> equal =
          TryBinary(BinaryOperation::kEquals, *left_value, *right_value);
      if (!equal.HasValue()) {
        residual_error = equal.GetStatus();
        return false;
      }
      if (!Truthy(equal.Value())) {
        return false;
      }
    }
    if (residual.empty()) {
      return true;
    }
    Scope scope{.row = &combined, .schema = &result.schema, .outer = outer};
    scope.using_columns = result.using_columns.get();
    return std::ranges::all_of(residual, [&](const Expression& predicate) {
      if (residual_error != Status::kSuccess) {
        return false;
      }
      StatusOr<Value> value =
          TryEvaluate(predicate, scope, nullptr, context, ctes);
      if (!value.HasValue()) {
        residual_error = value.GetStatus();
        return false;
      }
      return Truthy(value.Value());
    });
  };
  auto emit_unmatched = [&](const Row& left_row) {
    if (!want_left_nulls) {
      return;
    }
    std::vector<Value> nulls(right.schema.ColumnCount());
    result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    if (using_residuals.empty()) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, cross_col,
          TryColumnarCrossOrNullJoin(context, left, right, want_left_nulls,
                                     want_right_nulls, result.schema, residual,
                                     result.using_columns, result));
      if (cross_col.has_value()) {
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->join_ms += ElapsedMs(join_begin);
        }
        return *std::move(cross_col);
      }
    }
    if (right.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(right.TotalRows() * 128)) {
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
    }
    std::vector<char> right_matched(want_right_nulls ? right.TotalRows() : 0,
                                    0);
    Row combined_scratch{std::vector<Value>{}};
    auto probe_row = [&](const Row& left_row, const Row& right_row,
                         size_t right_idx, bool* matched) {
      ++result.join_comparisons;
      ConcatRowsInto(&combined_scratch, left_row, right_row);
      if (matches(combined_scratch)) {
        if (want_right_nulls && right_idx < right_matched.size()) {
          right_matched[right_idx] = 1;
        }
        result.AddRow(Row(combined_scratch));
        *matched = true;
      }
    };
    if (!right.HasSpill()) {
      RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
        bool matched = false;
        size_t right_idx = 0;
        const Status st = right.ForEachRow([&](const Row& right_row) {
          probe_row(left_row, right_row, right_idx, &matched);
          ++right_idx;
        });
        if (st != Status::kSuccess && residual_error == Status::kSuccess) {
          residual_error = st;
        }
        if (!matched) {
          emit_unmatched(left_row);
        }
      }));
    } else {
      // A spilled inner would otherwise be re-read from disk once per
      // outer row.  Stream it once in budget-bounded chunks and re-scan
      // the outer per chunk (block nested loop).
      std::vector<char> left_matched(want_left_nulls ? left.TotalRows() : 0, 0);
      const Status chunked = ForEachRowChunk(
          right, NestedLoopInnerChunkBytes(),
          [&](const std::vector<Row>& inner_rows, size_t inner_base) -> Status {
            size_t left_idx = 0;
            return left.ForEachRow([&](const Row& left_row) {
              bool matched = false;
              size_t right_idx = inner_base;
              for (const Row& right_row : inner_rows) {
                probe_row(left_row, right_row, right_idx, &matched);
                ++right_idx;
              }
              if (matched && left_idx < left_matched.size()) {
                left_matched[left_idx] = 1;
              }
              ++left_idx;
            });
          });
      if (chunked != Status::kSuccess && residual_error == Status::kSuccess) {
        residual_error = chunked;
      }
      if (want_left_nulls) {
        size_t left_idx = 0;
        RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
          if (left_idx >= left_matched.size() || left_matched[left_idx] == 0) {
            emit_unmatched(left_row);
          }
          ++left_idx;
        }));
      }
    }
    if (want_right_nulls) {
      size_t index = 0;
      RETURN_IF_FAIL(right.ForEachRow([&](const Row& right_row) {
        if (index < right_matched.size() && right_matched[index] == 0) {
          std::vector<Value> nulls(left.schema.ColumnCount());
          result.AddRow(ConcatRows(Row(std::move(nulls)), right_row));
        }
        ++index;
      }));
    }
  } else {
    ++result.hash_joins;
    // The output vector otherwise doubles and moves every Row. A full outer
    // join emits about one row per input side; an inner join emits at least
    // the build matches. This only reserves the shell, not the cell storage.
    if (!left.HasSpill() && !right.HasSpill()) {
      const size_t hint =
          left.rows.size() + (want_right_nulls ? right.rows.size() : 0);
      if (hint > result.rows.capacity() && hint <= (size_t{1} << 20)) {
        result.rows.reserve(hint);
      }
    }
    // D8 (docs/design.md): the hybrid operator now implements null-safe
    // keys and LEFT/RIGHT/FULL outer with full partition reprocessing.
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    static const bool flip_disabled =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JOIN_FLIP") != nullptr;
    const bool flip = !flip_disabled && !has_null_safe_key &&
                      using_residuals.empty() && source.using_columns.empty() &&
                      (source.join_type == JoinType::kFull ||
                       source.join_type == JoinType::kRight ||
                       source.join_type == JoinType::kLeft ||
                       source.join_type == JoinType::kInner) &&
                      right.TotalRows() >= kFlipMinBuildRows &&
                      right.TotalRows() > left.TotalRows() * kFlipRatio &&
                      !left.HasSpill();
    Relation* build_side_mut = flip ? &left : &right;
    const Relation& build_side = *build_side_mut;
    Relation& probe_side = flip ? right : left;
    const bool hybrid = ShouldHybridJoin(probe_side, build_side);
    // Biased keys (`col = col +/- constant`) ride only the in-memory int and
    // memcomparable key paths; the hybrid spilling operator and null-safe
    // encoding cannot bias, so those entries degrade back to residuals
    // (semantics unchanged, the residual verifier keeps them exact).
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
                          right_columns, matches, want_left_nulls,
                          &result.join_comparisons, has_null_safe_key,
                          want_right_nulls)));
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      joined.using_columns = result.using_columns;
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      RETURN_IF_FAIL(residual_error);
      return joined;
    }
    ++result.in_memory_hash_joins;
    ASSIGN_OR_RETURN(
        std::optional<Relation>, joined_col,
        (TryColumnarHashJoin(
            context, left, right, flip, want_left_nulls, want_right_nulls,
            has_null_safe_key, !using_residuals.empty(), left_columns,
            right_columns, left_key_offsets, right_key_offsets, residual,
            compiled_residuals, result.using_columns, result)));
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
    const bool stream_is_left = !flip;
    const bool probe_outer_emit = flip ? want_right_nulls : want_left_nulls;
    const bool want_build_nulls = flip ? want_left_nulls : want_right_nulls;
    const std::vector<slot_t>& build_columns =
        flip ? left_columns : right_columns;
    const std::vector<slot_t>& probe_columns =
        flip ? right_columns : left_columns;
    const std::vector<int64_t>& build_key_offsets =
        flip ? left_key_offsets : right_key_offsets;
    const std::vector<int64_t>& probe_key_offsets =
        flip ? right_key_offsets : left_key_offsets;
    const Row* build_base =
        build_side.rows.empty() ? nullptr : build_side.rows.data();
    std::vector<char> build_matched(
        want_build_nulls ? build_side.TotalRows() : 0, 0);
    auto mark_build = [&](const Row& build_row) {
      if (!want_build_nulls || build_base == nullptr) {
        return;
      }
      const auto index = static_cast<size_t>(&build_row - build_base);
      if (index < build_matched.size()) {
        build_matched[index] = 1;
      }
    };
    auto emit_unmatched_probe = [&](const Row& probe_row) {
      if (!probe_outer_emit) {
        return;
      }
      if (stream_is_left) {
        std::vector<Value> nulls(right.schema.ColumnCount());
        result.AddRow(ConcatRows(probe_row, Row(std::move(nulls))));
      } else {
        std::vector<Value> nulls(left.schema.ColumnCount());
        result.AddRow(ConcatRows(Row(std::move(nulls)), probe_row));
      }
    };
    // Parallel chunked probe (see ParallelHashProbe): needs a pure row
    // decision (no residual re-entering TryEvaluate) and a probe side big
    // enough to amortize the threads. Unmatched build rows use private
    // per-worker bitmaps merged after probing.
    static const bool probe_disabled =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_PROBE") !=
        nullptr;
    bool can_parallelize =
        !probe_disabled &&
        (residual.empty() || compiled_residuals.has_value()) &&
        using_residuals.empty() && !has_null_safe_key &&
        !probe_side.HasSpill() &&
        probe_side.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize
            ? std::min<size_t>(
                  kParallelProbeMaxWorkers,
                  QueryParallelWorkerLimit(std::thread::hardware_concurrency()))
            : 1;
    std::shared_ptr<QueryMemoryCharge> matched_charge;
    if (can_parallelize && want_build_nulls) {
      matched_charge = TryReserveQueryMemory(build_side.rows.size());
      can_parallelize = matched_charge != nullptr;
    }
    auto* matched_ptr = want_build_nulls ? &build_matched : nullptr;
    std::vector<uint64_t> build_matched_bits;
    auto* matched_bits_ptr = want_build_nulls ? &build_matched_bits : nullptr;
    const size_t fixed_row_bytes =
        FixedJoinRowBytes(left, right, result.schema);
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
          build_side, probe_side, probe_columns, probe_outer_emit,
          stream_is_left, build_base, matched_ptr, matched_bits_ptr,
          fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
          &result, &residual_error,
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
          matches, mark_build, emit_unmatched_probe));
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
      const auto packed =
          BuildPackedJoinRanges(build_side, build_columns, build_key_offsets);
      if (packed) {
        RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
            build_side, probe_side, probe_columns, probe_outer_emit,
            stream_is_left, build_base, matched_ptr, matched_bits_ptr,
            fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
            &result, &residual_error,
            [&](const Row& row) { return HasNullKey(row, build_columns); },
            [&](const Row& row) {
              return packed->Encode(row, build_columns, build_key_offsets);
            },
            [&](const Row& row) { return HasNullKey(row, probe_columns); },
            [&](const Row& row) {
              return packed->Encode(row, probe_columns, probe_key_offsets);
            },
            matches, mark_build, emit_unmatched_probe));
      } else {
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
              build_side, probe_side, probe_columns, probe_outer_emit,
              stream_is_left, build_base, matched_ptr, matched_bits_ptr,
              fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
              &result, &residual_error,
              [&](const Row& row) { return HasNullKey(row, build_columns); },
              [&](const Row& row) {
                const int64_t k0 =
                    AddJoinKeyBias(IntegerJoinKey(row, build_columns[0]),
                                   build_key_offsets[0]);
                const int64_t k1 =
                    AddJoinKeyBias(IntegerJoinKey(row, build_columns[1]),
                                   build_key_offsets[1]);
                return pack_2x32(k0, k1);
              },
              [&](const Row& row) { return HasNullKey(row, probe_columns); },
              [&](const Row& row) noexcept -> int64_t {
                const int64_t k0 =
                    AddJoinKeyBias(IntegerJoinKey(row, probe_columns[0]),
                                   probe_key_offsets[0]);
                const int64_t k1 =
                    AddJoinKeyBias(IntegerJoinKey(row, probe_columns[1]),
                                   probe_key_offsets[1]);
                if (!fits_i32(k0) || !fits_i32(k1)) {
                  return kInvalidPacked2x32;
                }
                return pack_2x32(k0, k1);
              },
              matches, mark_build, emit_unmatched_probe));
        } else {
          RETURN_IF_FAIL(
              (ExecuteFlatBucketJoin<CompositeIntKey, CompositeIntKeyHash>(
                  build_side, probe_side, probe_columns, probe_outer_emit,
                  stream_is_left, build_base, matched_ptr, matched_bits_ptr,
                  fixed_row_bytes, can_parallelize, probe_workers,
                  probe_residuals, &result, &residual_error,
                  [&](const Row& row) {
                    return HasNullKey(row, build_columns);
                  },
                  [&](const Row& row) {
                    return EncodeCompositeIntKey(row, build_columns,
                                                 &build_key_offsets);
                  },
                  [&](const Row& row) {
                    return HasNullKey(row, probe_columns);
                  },
                  [&](const Row& row) {
                    return EncodeCompositeIntKey(row, probe_columns,
                                                 &probe_key_offsets);
                  },
                  matches, mark_build, emit_unmatched_probe)));
        }
      }
    } else if (!has_null_safe_key &&
               CanBorrowJoinKeys(build_side, probe_side, build_columns,
                                 probe_columns, build_key_offsets,
                                 probe_key_offsets)) {
      RETURN_IF_FAIL(
          (ExecuteFlatBucketJoin<BorrowedJoinKey, BorrowedJoinKeyHash>(
              build_side, probe_side, probe_columns, probe_outer_emit,
              stream_is_left, build_base, matched_ptr, matched_bits_ptr,
              fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
              &result, &residual_error,
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
              matches, mark_build, emit_unmatched_probe)));
    } else {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<std::string>(
          build_side, probe_side, probe_columns, probe_outer_emit,
          stream_is_left, build_base, matched_ptr, matched_bits_ptr,
          fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
          &result, &residual_error,
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
          matches, mark_build, emit_unmatched_probe));
    }
    // Emit unmatched build-side rows (RIGHT/FULL without flip -> right;
    // LEFT/FULL with flip -> left), moving cells directly on the hot path.
    if (want_build_nulls) {
      const size_t pad_cols =
          flip ? right.schema.ColumnCount() : left.schema.ColumnCount();
      if (!build_side.HasSpill() && build_side.rows.size() >= 8192) {
        const size_t total_build = build_side.rows.size();
        const size_t workers = std::min<size_t>(
            ParallelWorkerCount(total_build / 4096), total_build);
        std::vector<Relation> shards(workers);
        std::vector<ExecutionRuntime> shard_runtimes(workers);
        for (size_t w = 0; w < workers; ++w) {
          shards[w].set_runtime(&shard_runtimes[w]);
          shards[w].schema = result.schema;
        }
        const size_t flush_granularity = Relation::ChargeFlushGranularity();
        auto emit_unmatched_row = [&](Relation& local, size_t i) {
          Row& build_row = build_side_mut->rows[i];
          if (flip) {
            Row out = std::move(build_row);
            out.values_.resize(out.values_.size() + pad_cols);
            if (fixed_row_bytes != 0) {
              local.AddRowWithKnownBytes(std::move(out), fixed_row_bytes,
                                         flush_granularity);
            } else {
              local.AddRow(std::move(out));
            }
          } else if (build_row.values_.capacity() >=
                     pad_cols + build_row.Count()) {
            const size_t bw = build_row.Count();
            build_row.values_.resize(pad_cols + bw);
            std::move_backward(
                build_row.values_.begin(),
                build_row.values_.begin() + static_cast<std::ptrdiff_t>(bw),
                build_row.values_.end());
            std::fill_n(build_row.values_.begin(), pad_cols, Value());
            if (fixed_row_bytes != 0) {
              local.AddRowWithKnownBytes(std::move(build_row), fixed_row_bytes,
                                         flush_granularity);
            } else {
              local.AddRow(std::move(build_row));
            }
          } else {
            Row out(std::vector<Value>(pad_cols + build_row.Count()));
            std::move(
                build_row.values_.begin(), build_row.values_.end(),
                out.values_.begin() + static_cast<std::ptrdiff_t>(pad_cols));
            std::vector<Value>().swap(build_row.values_);
            if (fixed_row_bytes != 0) {
              local.AddRowWithKnownBytes(std::move(out), fixed_row_bytes,
                                         flush_granularity);
            } else {
              local.AddRow(std::move(out));
            }
          }
        };
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          Relation& local = shards[w];
          if (!build_matched_bits.empty()) {
            const size_t total_words = build_matched_bits.size();
            const size_t word_begin = (total_words * w) / workers;
            const size_t word_end = (total_words * (w + 1)) / workers;
            size_t unmatched_count = 0;
            for (size_t k = word_begin; k < word_end; ++k) {
              uint64_t inv = ~build_matched_bits[k];
              if (k + 1 == total_words && (total_build & 63) != 0) {
                inv &= (uint64_t{1} << (total_build & 63)) - 1;
              }
              unmatched_count += static_cast<size_t>(std::popcount(inv));
            }
            if (unmatched_count > 0 && Relation::HasAmpleMemoryBudget()) {
              local.rows.reserve(std::min<size_t>(unmatched_count, 65536));
            }
            for (size_t k = word_begin; k < word_end; ++k) {
              uint64_t inv = ~build_matched_bits[k];
              if (k + 1 == total_words && (total_build & 63) != 0) {
                inv &= (uint64_t{1} << (total_build & 63)) - 1;
              }
              const size_t base_i = k * 64;
              while (inv != 0) {
                const auto bit = static_cast<size_t>(std::countr_zero(inv));
                emit_unmatched_row(local, base_i + bit);
                inv &= (inv - 1);
              }
            }
            return;
          }
          const size_t begin = (total_build * w) / workers;
          const size_t end = (total_build * (w + 1)) / workers;
          size_t unmatched_count = 0;
          for (size_t i = begin; i < end; ++i) {
            if (i >= build_matched.size() || build_matched[i] == 0) {
              ++unmatched_count;
            }
          }
          if (unmatched_count > 0 && Relation::HasAmpleMemoryBudget()) {
            local.rows.reserve(std::min<size_t>(unmatched_count, 65536));
          }
          for (size_t i = begin; i < end; ++i) {
            if (i < build_matched.size() && build_matched[i] != 0) {
              continue;
            }
            emit_unmatched_row(local, i);
          }
        }));
        if (result.runtime() != nullptr) {
          for (const auto& rt : shard_runtimes) {
            result.runtime()->relation_spills += rt.relation_spills;
          }
        }
        RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &result));
      } else {
        size_t index = 0;
        RETURN_IF_FAIL(build_side.ForEachRow([&](const Row& build_row) {
          const bool matched =
              !build_matched_bits.empty()
                  ? ((build_matched_bits[index >> 6] >> (index & 63)) & 1ULL) !=
                        0
                  : (index < build_matched.size() && build_matched[index] != 0);
          if (!matched) {
            std::vector<Value> nulls(pad_cols);
            result.AddRow(flip ? ConcatRows(build_row, Row(std::move(nulls)))
                               : ConcatRows(Row(std::move(nulls)), build_row));
          }
          ++index;
        }));
      }
    }
    RETURN_IF_FAIL(result.FinishSpill());
    RETURN_IF_FAIL(result.GetStatus());
    if (left.uniform_cell_types && right.uniform_cell_types &&
        !result.HasSpill()) {
      result.uniform_cell_types = true;
    }
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
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

bool IsSubset(const std::unordered_set<size_t>& values,
              const std::unordered_set<size_t>& superset) {
  return std::ranges::all_of(
      values, [&](size_t value) { return superset.contains(value); });
}

std::vector<SelectSource> ReduceOuterJoinsToInner(
    const SelectStatement& statement, const std::vector<Relation>& schemas,
    bool* reduced_all) {
  std::vector<SelectSource> reduced = statement.Sources();
  *reduced_all = false;
  if (reduced.size() != schemas.size()) {
    return reduced;
  }
  const Expression& where = statement.WhereClause();
  if (!where) {
    return reduced;
  }
  const std::vector<PredicateInfo> predicates =
      AnalyzePredicates(where, schemas);
  const bool resolved =
      std::ranges::all_of(predicates, [](const PredicateInfo& predicate) {
        return predicate.resolved && !predicate.contains_query;
      });
  if (!resolved) {
    return reduced;
  }
  // Resolve bare names against the same schemas used by predicate analysis.
  // A qualifier-only check misses valid null rejection in ordinary SQL such
  // as `WHERE return_reason = reason_key` after a LEFT JOIN.
  // ExpressionRejectsNullsWhere calls its matcher once per (outer join, WHERE
  // node); resolving a column with a fresh linear, case-insensitive schema
  // walk on every call made this quadratic in the number of joins. Resolve
  // each column touched by WHERE once up front and reuse the result.
  std::unordered_map<ColumnName, std::vector<size_t>> where_column_sources;
  for (const ColumnName& column : where->TouchedColumns()) {
    auto& sources = where_column_sources[column];
    for (size_t source = 0; source < schemas.size(); ++source) {
      if (LocalColumnOffset(schemas[source].schema, column)) {
        sources.push_back(source);
      }
    }
  }
  const auto rejects_side = [&](size_t begin, size_t end) {
    return ExpressionRejectsNullsWhere(
        where,
        [&](const ColumnName& column) {
          const auto found = where_column_sources.find(column);
          if (found == where_column_sources.end()) {
            return false;
          }
          return std::ranges::any_of(found->second, [&](size_t source) {
            return source >= begin && source < end;
          });
        },
        /*bare_column_rejects_null=*/true);
  };
  bool any_outer = false;
  bool all_reducible = true;
  for (size_t index = 1; index < reduced.size(); ++index) {
    const JoinType type = reduced[index].join_type;
    if (type != JoinType::kLeft && type != JoinType::kRight &&
        type != JoinType::kFull) {
      continue;
    }
    any_outer = true;
    if (reduced[index].query || reduced[index].unnest ||
        reduced[index].from_nested_join) {
      all_reducible = false;
      continue;
    }
    bool rejects = false;
    if (type == JoinType::kLeft) {
      rejects = rejects_side(index, index + 1);
    } else if (type == JoinType::kRight) {
      rejects = rejects_side(0, index);
    } else {
      rejects = rejects_side(0, index) && rejects_side(index, index + 1);
    }
    if (rejects) {
      reduced[index].join_type = JoinType::kInner;
    } else {
      all_reducible = false;
    }
  }
  *reduced_all = any_outer && all_reducible;
  return reduced;
}

}  // namespace tinylamb::relational_detail
