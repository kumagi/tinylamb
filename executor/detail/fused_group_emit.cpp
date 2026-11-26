/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/detail/fused_group_emit.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
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
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

Status FusedGroupEmitter::Emit() const {
  const auto active_group_count = [&]() noexcept -> size_t {
    return use_group_ptrs ? group_ptrs.size() : groups.size();
  };
  const auto active_group_at = [&](size_t idx) noexcept -> const FusedGroup& {
    return use_group_ptrs ? *group_ptrs[idx] : groups[idx];
  };

  // Emit: finish the accumulators into an AggregateResultMap keyed by the
  // aggregate node, then evaluate the select list against the representative
  // row — the same contract as Project's generic grouped emit.
  std::vector<Column> output_columns;
  output_columns.reserve(fusion->select_items.size());
  for (const std::string& name : fusion->select_names) {
    output_columns.emplace_back(name, ValueType::kNull);
  }
  struct DirectEmitItem {
    bool is_agg{false};
    size_t index{0};
    bool has_agg_scale{false};
    double agg_scale{1.0};
    std::optional<Value> constant{std::nullopt};
    std::optional<BytecodeProgram> program{std::nullopt};
  };
  std::vector<DirectEmitItem> direct_emit;
  std::vector<slot_t> emit_needed_cols;
  emit_needed_cols.reserve(fusion->select_items.size());
  std::vector<size_t> emit_needed_aggs;
  emit_needed_aggs.reserve(fusion->select_items.size());
  bool has_emit_program = false;
  size_t emit_row_width = result.schema.ColumnCount();
  std::optional<BytecodeProgram> having_prog;
  std::vector<slot_t> having_cols;
  std::vector<size_t> having_aggs;
  size_t having_row_width = result.schema.ColumnCount();
  bool can_direct_emit = result.using_columns == nullptr;
  if (can_direct_emit) {
    const auto static_agg_type = [&](size_t ai) -> std::optional<ValueType> {
      if (raw_aggregates.raw_count_fusion) {
        return ValueType::kInt64;
      }
      if (raw_aggregates.raw_sum_fusion) {
        switch (raw_aggregates.raw_agg_kinds[ai]) {
          case RawAggKind::kCountStar:
          case RawAggKind::kCountValue:
          case RawAggKind::kCountDistinctInt64:
          case RawAggKind::kSumInt64:
            return ValueType::kInt64;
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble:
          case RawAggKind::kStddevSamp:
          case RawAggKind::kStddevPop:
          case RawAggKind::kVarSamp:
          case RawAggKind::kVarPop:
            return ValueType::kDouble;
          case RawAggKind::kConstantExtremum:
            if (!raw_aggregates.raw_constants[ai].IsNull()) {
              return raw_aggregates.raw_constants[ai].type;
            }
            if (fusion->aggregates[ai]->Child()) {
              const TypeTag ct = fusion->aggregates[ai]
                                     ->Child()
                                     ->ResultType(result.schema)
                                     .GetType();
              return ct == TypeTag::kDouble ? ValueType::kDouble
                                            : ValueType::kInt64;
            }
            return ValueType::kDouble;
          default:
            return std::nullopt;
        }
      }
      switch (fusion->aggregates[ai]->GetType()) {
        case AggregationType::kCount:
          return ValueType::kInt64;
        case AggregationType::kAvg:
        case AggregationType::kStddevSamp:
        case AggregationType::kStddevPop:
        case AggregationType::kVarSamp:
        case AggregationType::kVarPop:
          return ValueType::kDouble;
        default:
          return std::nullopt;
      }
    };
    std::optional<Schema> emit_schema_opt;
    std::vector<std::string> agg_col_names;
    const auto ensure_emit_schema = [&]() -> const Schema* {
      if (!emit_schema_opt.has_value()) {
        std::vector<Column> cols;
        cols.reserve(result.schema.ColumnCount() + fusion->aggregates.size());
        for (size_t ci = 0; ci < result.schema.ColumnCount(); ++ci) {
          cols.push_back(result.schema.GetColumn(ci));
        }
        agg_col_names.resize(fusion->aggregates.size());
        for (size_t ai = 0; ai < fusion->aggregates.size(); ++ai) {
          const auto t = static_agg_type(ai);
          if (!t.has_value()) {
            return nullptr;
          }
          std::string nm = "__fused_agg_" + std::to_string(ai);
          if (LocalColumnOffset(result.schema, ColumnName(nm)).has_value()) {
            return nullptr;
          }
          agg_col_names[ai] = nm;
          cols.emplace_back(std::move(nm), *t);
        }
        emit_schema_opt = Schema("", std::move(cols));
        emit_row_width = emit_schema_opt->ColumnCount();
      }
      return &*emit_schema_opt;
    };
    const std::function<Expression(const Expression&, std::vector<slot_t>*,
                                   std::vector<size_t>*)>
        rewrite_for_emit = [&](const Expression& expr,
                               std::vector<slot_t>* used_cols,
                               std::vector<size_t>* used_aggs) -> Expression {
      if (!expr || ContainsQuery(expr) || ContainsParameterSlot(expr)) {
        return {};
      }
      if (expr->Type() == TypeTag::kConstantValue) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const auto off = LocalColumnOffset(
            result.schema, expr->AsColumnValue().GetColumnName());
        if (!off.has_value()) {
          return {};
        }
        used_cols->push_back(static_cast<slot_t>(*off));
        return expr;
      }
      if (expr->Type() == TypeTag::kAggregateExp) {
        const auto* agg_ptr = &expr->AsAggregateExpression();
        auto found = std::ranges::find(fusion->aggregates, agg_ptr);
        if (found == fusion->aggregates.end()) {
          return {};
        }
        const auto ai = static_cast<size_t>(
            std::distance(fusion->aggregates.begin(), found));
        if (raw_aggregates.raw_sum_fusion &&
            raw_aggregates.raw_const_folded[ai]) {
          return ConstantValueExp(raw_aggregates.raw_constants[ai]);
        }
        if (!static_agg_type(ai).has_value() ||
            ensure_emit_schema() == nullptr) {
          return {};
        }
        used_aggs->push_back(ai);
        return ColumnValueExp(agg_col_names[ai]);
      }
      if (expr->Type() == TypeTag::kUnaryExp) {
        auto child = rewrite_for_emit(expr->AsUnaryExpression().Child(),
                                      used_cols, used_aggs);
        if (!child) {
          return {};
        }
        return UnaryExpressionExp(std::move(child),
                                  expr->AsUnaryExpression().Op());
      }
      if (expr->Type() == TypeTag::kBinaryExp) {
        const auto& bin = expr->AsBinaryExpression();
        auto lhs = rewrite_for_emit(bin.Left(), used_cols, used_aggs);
        auto rhs = rewrite_for_emit(bin.Right(), used_cols, used_aggs);
        if (!lhs || !rhs) {
          return {};
        }
        return BinaryExpressionExp(std::move(lhs), bin.Op(), std::move(rhs));
      }
      return {};
    };
    direct_emit.reserve(fusion->select_items.size());
    const auto try_match_scaled_agg = [&](const Expression& expr,
                                          DirectEmitItem* out_item) -> bool {
      if (!raw_aggregates.raw_sum_fusion || !expr ||
          expr->Type() != TypeTag::kBinaryExp) {
        return false;
      }
      const BinaryExpression& bin = expr->AsBinaryExpression();
      if (bin.Op() != BinaryOperation::kMultiply || !bin.Left() ||
          !bin.Right()) {
        return false;
      }
      const auto extract_finite_scale = [&](const Expression& cand,
                                            double* out_scale) -> bool {
        if (!cand || ContainsParameterSlot(cand)) {
          return false;
        }
        bool negate = false;
        const Expression* inner = &cand;
        if ((*inner)->Type() == TypeTag::kUnaryExp) {
          const auto& unary = (*inner)->AsUnaryExpression();
          if (unary.Op() != UnaryOperation::kMinus || !unary.Child()) {
            return false;
          }
          negate = true;
          inner = &unary.Child();
        }
        if ((*inner)->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const Value& cval = (*inner)->AsConstantValue().GetValue();
        if (cval.IsNull()) {
          return false;
        }
        double scale = 1.0;
        if (cval.type == ValueType::kDouble) {
          if (!std::isfinite(cval.value.double_value)) {
            return false;
          }
          scale = cval.value.double_value;
        } else if (cval.type == ValueType::kInt64) {
          scale = cval.IsUnsigned() ? static_cast<double>(static_cast<uint64_t>(
                                          cval.value.int_value))
                                    : static_cast<double>(cval.value.int_value);
        } else {
          return false;
        }
        *out_scale = negate ? -scale : scale;
        return true;
      };
      const Expression* agg_expr = nullptr;
      double scale = 1.0;
      if (bin.Left()->Type() == TypeTag::kAggregateExp &&
          extract_finite_scale(bin.Right(), &scale)) {
        agg_expr = &bin.Left();
      } else if (bin.Right()->Type() == TypeTag::kAggregateExp &&
                 extract_finite_scale(bin.Left(), &scale)) {
        agg_expr = &bin.Right();
      } else {
        return false;
      }
      const auto* agg_ptr = &(*agg_expr)->AsAggregateExpression();
      auto found = std::ranges::find(fusion->aggregates, agg_ptr);
      if (found == fusion->aggregates.end()) {
        return false;
      }
      const auto ai =
          static_cast<size_t>(std::distance(fusion->aggregates.begin(), found));
      if (raw_aggregates.raw_const_folded[ai] ||
          static_agg_type(ai) != ValueType::kDouble) {
        return false;
      }
      *out_item = DirectEmitItem{.is_agg = true,
                                 .index = ai,
                                 .has_agg_scale = true,
                                 .agg_scale = scale};
      return true;
    };
    for (const Expression& item : fusion->select_items) {
      if (!item) {
        can_direct_emit = false;
        break;
      }
      if (item->Type() == TypeTag::kConstantValue &&
          !ContainsParameterSlot(item)) {
        direct_emit.push_back({.constant = item->AsConstantValue().GetValue()});
      } else if (item->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            result.schema, item->AsColumnValue().GetColumnName());
        if (!col_off.has_value()) {
          can_direct_emit = false;
          break;
        }
        direct_emit.push_back({.is_agg = false, .index = *col_off});
      } else if (item->Type() == TypeTag::kAggregateExp) {
        const auto* agg_ptr = &item->AsAggregateExpression();
        auto found = std::ranges::find(fusion->aggregates, agg_ptr);
        if (found == fusion->aggregates.end()) {
          can_direct_emit = false;
          break;
        }
        const auto ai = static_cast<size_t>(
            std::distance(fusion->aggregates.begin(), found));
        if (raw_aggregates.raw_sum_fusion &&
            raw_aggregates.raw_const_folded[ai]) {
          direct_emit.push_back({.constant = raw_aggregates.raw_constants[ai]});
        } else {
          direct_emit.push_back({.is_agg = true, .index = ai});
        }
      } else if (DirectEmitItem scaled_di;
                 try_match_scaled_agg(item, &scaled_di)) {
        direct_emit.push_back(std::move(scaled_di));
      } else {
        std::vector<slot_t> cols_used;
        std::vector<size_t> aggs_used;
        Expression rw = rewrite_for_emit(item, &cols_used, &aggs_used);
        const Schema* es = rw ? ensure_emit_schema() : nullptr;
        std::optional<BytecodeProgram> prog =
            es != nullptr ? BytecodeCompiler::CompileRowCached(
                                rw, *es, BytecodeCompiler::Context::kValue)
                          : std::nullopt;
        if (!prog.has_value()) {
          can_direct_emit = false;
          break;
        }
        emit_needed_cols.insert(emit_needed_cols.end(), cols_used.begin(),
                                cols_used.end());
        emit_needed_aggs.insert(emit_needed_aggs.end(), aggs_used.begin(),
                                aggs_used.end());
        has_emit_program = true;
        DirectEmitItem di;
        di.program = std::move(prog);
        direct_emit.push_back(std::move(di));
      }
    }
    if (has_emit_program) {
      std::ranges::sort(emit_needed_cols);
      emit_needed_cols.erase(
          std::unique(emit_needed_cols.begin(), emit_needed_cols.end()),
          emit_needed_cols.end());
      std::ranges::sort(emit_needed_aggs);
      emit_needed_aggs.erase(
          std::unique(emit_needed_aggs.begin(), emit_needed_aggs.end()),
          emit_needed_aggs.end());
    }
    if (can_direct_emit && fusion->having) {
      Expression rw_having =
          rewrite_for_emit(fusion->having, &having_cols, &having_aggs);
      const Schema* es = rw_having ? ensure_emit_schema() : nullptr;
      having_prog = es != nullptr
                        ? BytecodeCompiler::CompileRowCached(
                              rw_having, *es, BytecodeCompiler::Context::kValue)
                        : std::nullopt;
      if (!having_prog.has_value()) {
        can_direct_emit = false;
      } else {
        having_row_width = emit_row_width;
        std::ranges::sort(having_cols);
        having_cols.erase(std::unique(having_cols.begin(), having_cols.end()),
                          having_cols.end());
        std::ranges::sort(having_aggs);
        having_aggs.erase(std::unique(having_aggs.begin(), having_aggs.end()),
                          having_aggs.end());
      }
    }
  }
  const auto finish_raw_agg = [&](const FusedGroup& group,
                                  size_t i) -> StatusOr<Value> {
    if (raw_aggregates.raw_const_folded[i]) {
      return raw_aggregates.raw_constants[i];
    }
    const auto slot = static_cast<size_t>(
        static_cast<unsigned char>(raw_aggregates.raw_agg_slot[i]));
    if (group.RawInfFlags()[slot] && raw_aggregates.slot_raises_inf[slot]) {
      return StatusError(StatusCode::kIsInfinity,
                         "SUM overflow: result is infinite");
    }
    switch (raw_aggregates.raw_agg_kinds[i]) {
      case RawAggKind::kConstantExtremum:
        return group.RawCounts()[slot] == 0 ? Value()
                                            : raw_aggregates.raw_constants[i];
      case RawAggKind::kCountStar:
      case RawAggKind::kCountValue:
        return Value(group.RawCounts()[slot]);
      case RawAggKind::kSumInt64:
        return group.RawCounts()[slot] == 0 ? Value()
                                            : Value(group.RawInts()[slot]);
      case RawAggKind::kSumDouble:
        return group.RawCounts()[slot] == 0 ? Value()
                                            : Value(group.RawDoubles()[slot]);
      case RawAggKind::kAvgDouble:
        return group.RawCounts()[slot] == 0
                   ? Value()
                   : Value(group.RawDoubles()[slot] /
                           static_cast<double>(group.RawCounts()[slot]));
      case RawAggKind::kStddevSamp:
      case RawAggKind::kStddevPop:
      case RawAggKind::kVarSamp:
      case RawAggKind::kVarPop: {
        const int64_t n = group.RawCounts()[slot];
        const bool is_samp =
            raw_aggregates.raw_agg_kinds[i] == RawAggKind::kStddevSamp ||
            raw_aggregates.raw_agg_kinds[i] == RawAggKind::kVarSamp;
        if (n < (is_samp ? 2 : 1)) {
          return Value();
        }
        const platform::ExtendedFloat mean = group.RawStatX()[slot] / n;
        const platform::ExtendedFloat ssd =
            group.RawStatXX()[slot] - (n * mean * mean);
        // Catastrophic cancellation in n*mean*mean can make ssd slightly
        // negative for large means; clamp to zero to match the canonical
        // accumulator instead of emitting NaN / a negative variance.
        const platform::ExtendedFloat variance =
            ssd > 0 ? ssd / (is_samp ? n - 1 : n) : platform::ExtendedFloat{0};
        const bool is_stddev =
            raw_aggregates.raw_agg_kinds[i] == RawAggKind::kStddevSamp ||
            raw_aggregates.raw_agg_kinds[i] == RawAggKind::kStddevPop;
        return Value(static_cast<double>(is_stddev ? platform::Sqrt(variance)
                                                   : variance));
      }
      case RawAggKind::kCountDistinctInt64:
        return Value(static_cast<int64_t>(
            group.raw_int_set ? group.raw_int_set->Size()
                              : static_cast<size_t>(group.RawCounts()[slot])));
      case RawAggKind::kNone:
        break;
    }
    return Value();
  };
  const auto apply_agg_scale = [](const DirectEmitItem& d, Value v) -> Value {
    if (d.has_agg_scale && !v.IsNull() && v.type == ValueType::kDouble) {
      v.value.double_value *= d.agg_scale;
    }
    return v;
  };
  const auto try_unpack_key_cell = [&](const FusedGroup& grp, size_t col_idx,
                                       bool own_pk_strings,
                                       Value* out) -> bool {
    if (!grp.key.values_.empty()) {
      for (size_t k = 0; k < key_shapes.size() && k < grp.key.values_.size();
           ++k) {
        if (std::cmp_equal(
                key_shapes[k].direct_column,
                col_idx)) {  // NOLINT(modernize-use-integer-sign-comparison)
          *out = grp.key.values_[k];
          if (own_pk_strings && out->type == ValueType::kVarChar &&
              !out->IsNull() && out->owned_data.empty()) {
            *out = Value(std::string(out->value.varchar_value));
          }
          return true;
        }
      }
      return false;
    }
    if (layout.packed_2x32_unpack_only && grp.has_key_hash) {
      if (col_idx == layout.packed_2x32_slot0) {
        const auto v0 =
            static_cast<int64_t>(static_cast<int32_t>(grp.key_hash >> 32));
        *out = layout.packed_2x32_type0 == ValueType::kDate
                   ? Value::DateFromDays(v0)
                   : Value(v0);
        return true;
      }
      if (col_idx == layout.packed_2x32_slot1) {
        const auto v1 = static_cast<int64_t>(
            static_cast<int32_t>(grp.key_hash & 0xffffffffULL));
        *out = layout.packed_2x32_type1 == ValueType::kDate
                   ? Value::DateFromDays(v1)
                   : Value(v1);
        return true;
      }
    }
    if (layout.split_key_unpack_only && grp.has_key_hash) {
      if (layout.split_has_left_probe && layout.split_left_slot.has_value() &&
          col_idx == *layout.split_left_slot) {
        const auto v1 = static_cast<int64_t>(
            static_cast<int32_t>(grp.key_hash & 0xffffffffULL));
        *out = layout.split_left_type == ValueType::kDate
                   ? Value::DateFromDays(v1)
                   : Value(v1);
        return true;
      }
      if (col_idx < left_width && col_idx < 16 &&
          def_col_to_pk_lookup[col_idx] >= 0) {
        const auto li = static_cast<size_t>(
            static_cast<unsigned char>(def_col_to_pk_lookup[col_idx]));
        const PreparedDeferredPkGroup& pk_grp =
            def_pk_groups[prepared_pk_lookups[li].group_idx];
        if (pk_grp.use_subgid) {
          const uint32_t p_card = pk_grp.subgid_count;
          const uint64_t mask =
              p_card <= 1
                  ? 0ULL
                  : ((uint64_t{1} << std::bit_width(p_card - 1)) - 1ULL);
          const auto sg = static_cast<uint32_t>(
              (grp.key_hash >> pk_grp.subgid_shift) & mask);
          const auto [nb, rin] = pk_grp.subgid_rep_ptr[sg];
          read_pk_cell(li, nb, rin, out);
          if (own_pk_strings && out->type == ValueType::kVarChar &&
              !out->IsNull()) {
            *out = Value(std::string(out->value.varchar_value));
          }
          return true;
        }
      }
    }
    if (layout.left_pack_unpack_only && grp.has_key_hash) {
      for (const LeftPackedIntCol& lpi : layout.left_pack_ints) {
        if (lpi.col == col_idx) {
          const uint64_t code = (grp.key_hash >> lpi.shift) & lpi.mask;
          if (code == lpi.null_code) {
            *out = Value();
          } else {
            const auto iv =
                static_cast<int64_t>(static_cast<uint64_t>(lpi.min_val) + code);
            *out = lpi.type == ValueType::kDate ? Value::DateFromDays(iv)
                                                : Value(iv);
          }
          return true;
        }
      }
      if (layout.left_pack_uses_dict) {
        for (const size_t dc : layout.left_pack_dict_cols) {
          if (dc == col_idx) {
            const auto d_id = static_cast<uint32_t>(
                (grp.key_hash >> layout.left_pack_dict_shift) &
                layout.left_pack_dict_mask);
            if (layout.left_col_only) {
              const auto [ci, r] = layout.left_pack_dict_rep_col_loc[d_id];
              if (ci == std::numeric_limits<uint32_t>::max()) {
                *out = Value();
              } else {
                left.columnar->chunks[ci].columns[col_idx].WriteValueInto(r,
                                                                          out);
                if (own_pk_strings && out->type == ValueType::kVarChar &&
                    !out->IsNull()) {
                  *out = Value(std::string(out->value.varchar_value));
                }
              }
            } else {
              *out = left.rows[layout.left_pack_dict_rep_rows[d_id]]
                         .values_[col_idx];
            }
            return true;
          }
        }
      }
      if (layout.left_pack_uses_right &&
          layout.split_right_rep_ptr != nullptr) {
        const auto r_subgid =
            static_cast<uint32_t>((grp.key_hash >> layout.split_right_shift) &
                                  layout.left_pack_right_mask);
        const uint32_t rep_idx = layout.split_right_rep_ptr[r_subgid];
        if (col_idx >= left_width) {
          const Value& v = read_right_cell(rep_idx, col_idx - left_width, out);
          if (&v != out) {
            *out = v;
          }
          if (own_pk_strings && out->type == ValueType::kVarChar &&
              !out->IsNull() && out->owned_data.empty()) {
            *out = Value(std::string(out->value.varchar_value));
          }
          return true;
        }
        if (join_type == JoinType::kInner) {
          for (const EqualityKey& eq : equality_keys) {
            if (eq.left == col_idx && eq.left_offset == 0 &&
                eq.right_offset == 0 && !eq.null_safe) {
              const Value& v = read_right_cell(rep_idx, eq.right, out);
              if (&v != out) {
                *out = v;
              }
              if (own_pk_strings && out->type == ValueType::kVarChar &&
                  !out->IsNull() && out->owned_data.empty()) {
                *out = Value(std::string(out->value.varchar_value));
              }
              return true;
            }
          }
        }
      }
    }
    return false;
  };
  const auto rep_cell = [&](const FusedGroup& grp, size_t col_idx,
                            Value* col_scratch) -> const Value& {
    if (grp.representative != nullptr) {
      return grp.representative->values_[col_idx];
    }
    if (grp.rep_left == nullptr &&
        grp.rep_col_chunk == std::numeric_limits<uint32_t>::max() &&
        try_unpack_key_cell(grp, col_idx, false, col_scratch)) {
      return *col_scratch;
    }
    if (col_idx < left_width && grp.rep_left == nullptr &&
        grp.rep_col_chunk != std::numeric_limits<uint32_t>::max()) {
      left.columnar->chunks[grp.rep_col_chunk].columns[col_idx].WriteValueInto(
          grp.rep_col_row, col_scratch);
      return *col_scratch;
    }
    if (col_idx < left_width && grp.rep_left == nullptr &&
        grp.rep_right_idx != kInvalidRightIdx) {
      for (const EqualityKey& eq : equality_keys) {
        if (eq.left == col_idx && eq.left_offset == 0 && eq.right_offset == 0 &&
            !eq.null_safe) {
          return read_right_cell(grp.rep_right_idx, eq.right, col_scratch);
        }
      }
      return kNullCell;
    }
    if (col_idx < left_width) {
      return grp.rep_left != nullptr ? grp.rep_left->values_[col_idx]
                                     : kNullCell;
    }
    return read_right_cell(grp.rep_right_idx, col_idx - left_width,
                           col_scratch);
  };
  const auto direct_key_value = [&](const FusedGroup& grp, size_t col_idx,
                                    bool own_pk_strings = true) -> Value {
    Value col_tmp;
    if (try_unpack_key_cell(grp, col_idx, own_pk_strings, &col_tmp)) {
      return CanonicalDistinctValue(col_tmp);
    }
    return CanonicalDistinctValue(rep_cell(grp, col_idx, &col_tmp));
  };
  // Reject count groups before materializing their representative and output
  // row. A selective HAVING can discard almost every joined group.
  if (fusion->post_having_active && !fusion->post_having_is_self_sum_factor &&
      fusion->post_having_agg_slot < fusion->aggregates.size()) {
    const slot_t orig_slot = fusion->post_having_agg_slot;
    const bool count_only = raw_aggregates.raw_count_fusion && orig_slot == 0;
    const bool count_slot =
        raw_aggregates.raw_sum_fusion &&
        (raw_aggregates.raw_agg_kinds[orig_slot] == RawAggKind::kCountStar ||
         raw_aggregates.raw_agg_kinds[orig_slot] == RawAggKind::kCountValue);
    if (count_only || count_slot) {
      const size_t slot =
          count_only
              ? 0
              : static_cast<size_t>(raw_aggregates.raw_agg_slot[orig_slot]);
      const double threshold = fusion->post_having_constant_or_factor;
      const auto rejected = [&](const FusedGroup& group) {
        const auto count = static_cast<double>(
            count_only ? group.raw_count : group.RawCounts()[slot]);
        switch (fusion->post_having_op) {
          case BinaryOperation::kEquals:
            return !(count == threshold);
          case BinaryOperation::kNotEquals:
            return !(count != threshold);
          case BinaryOperation::kLessThan:
            return !(count < threshold);
          case BinaryOperation::kLessThanEquals:
            return !(count <= threshold);
          case BinaryOperation::kGreaterThan:
            return !(count > threshold);
          case BinaryOperation::kGreaterThanEquals:
            return !(count >= threshold);
          default:
            return false;
        }
      };
      // Compact once in encounter order. Keeping this loop explicit avoids
      // forcing the library remove_if search loop through an unroll request
      // that ThinLTO cannot satisfy for this large group state.
      size_t retained = 0;
      if (use_group_ptrs) {
        for (FusedGroup* ptr : group_ptrs) {
          if (!rejected(*ptr)) {
            group_ptrs[retained++] = ptr;
          }
        }
        group_ptrs.resize(retained);
      } else {
        for (size_t index = 0; index < groups.size(); ++index) {
          if (!rejected(groups[index])) {
            if (retained != index) {
              groups[retained] = std::move(groups[index]);
            }
            ++retained;
          }
        }
        groups.erase(groups.begin() + static_cast<std::ptrdiff_t>(retained),
                     groups.end());
      }
      fusion->post_having_applied = true;
    }
  }
  phase_merge_done = std::chrono::steady_clock::now();
  if (fusion->post_having_active && raw_aggregates.raw_sum_fusion &&
      fusion->post_having_agg_slot < fusion->aggregates.size()) {
    const slot_t orig_agg_slot = fusion->post_having_agg_slot;
    const RawAggKind akind = raw_aggregates.raw_agg_kinds[orig_agg_slot];
    if (raw_aggregates.raw_agg_slot[orig_agg_slot] >= 0 &&
        (akind == RawAggKind::kSumDouble || akind == RawAggKind::kSumInt64)) {
      const auto agg_slot = static_cast<slot_t>(static_cast<unsigned char>(
          raw_aggregates.raw_agg_slot[orig_agg_slot]));
      bool any_inf = false;
      const size_t group_count = active_group_count();
      for (size_t gi = 0; gi < group_count; ++gi) {
        if (active_group_at(gi).RawInfFlags()[agg_slot] != 0) {
          any_inf = true;
          break;
        }
      }
      // INT64 sums compare as doubles below; that is only exact while every
      // group sum and the grand total stay within +-2^53. Otherwise leave
      // HAVING to the canonical integer comparison (post_having_applied
      // stays false).
      if (!any_inf && akind == RawAggKind::kSumInt64) {
        constexpr platform::Int128 kExactLimit = platform::Int128{1} << 53;
        platform::Int128 grand = 0;
        for (size_t gi = 0; gi < group_count; ++gi) {
          const FusedGroup& grp = active_group_at(gi);
          if (grp.RawCounts()[agg_slot] <= 0) {
            continue;
          }
          const platform::Int128 v = grp.RawInts()[agg_slot];
          grand += v;
          if (v > kExactLimit || v < -kExactLimit || grand > kExactLimit ||
              grand < -kExactLimit) {
            any_inf = true;  // Reuse the "skip the shortcut" exit.
            break;
          }
        }
      }
      if (!any_inf) {
        auto group_sum_val = [&](const FusedGroup& grp) noexcept -> double {
          return akind == RawAggKind::kSumDouble
                     ? grp.RawDoubles()[agg_slot]
                     : static_cast<double>(grp.RawInts()[agg_slot]);
        };
        double threshold = fusion->post_having_constant_or_factor;
        bool valid_threshold = true;
        if (fusion->post_having_is_self_sum_factor) {
          double grand_total = 0.0;
          bool any_nonnull = false;
          for (size_t gi = 0; gi < group_count; ++gi) {
            const FusedGroup& grp = active_group_at(gi);
            if (grp.RawCounts()[agg_slot] > 0) {
              any_nonnull = true;
              grand_total += group_sum_val(grp);
            }
          }
          if (!any_nonnull) {
            valid_threshold = false;
          } else {
            threshold = grand_total * fusion->post_having_constant_or_factor;
            if (!std::isfinite(threshold)) {
              valid_threshold = false;
            }
          }
        }
        if (valid_threshold) {
          size_t out = 0;
          for (size_t gi = 0; gi < group_count; ++gi) {
            const FusedGroup& grp = active_group_at(gi);
            if (grp.RawCounts()[agg_slot] <= 0) {
              continue;
            }
            const double val = group_sum_val(grp);
            bool pass = false;
            switch (fusion->post_having_op) {
              case BinaryOperation::kEquals:
                pass = (val == threshold);
                break;
              case BinaryOperation::kNotEquals:
                pass = (val != threshold);
                break;
              case BinaryOperation::kLessThan:
                pass = (val < threshold);
                break;
              case BinaryOperation::kLessThanEquals:
                pass = (val <= threshold);
                break;
              case BinaryOperation::kGreaterThan:
                pass = (val > threshold);
                break;
              case BinaryOperation::kGreaterThanEquals:
                pass = (val >= threshold);
                break;
              default:
                break;
            }
            if (pass) {
              if (use_group_ptrs) {
                group_ptrs[out++] = group_ptrs[gi];
              } else {
                if (out != gi) {
                  groups[out] = std::move(groups[gi]);
                }
                ++out;
              }
            }
          }
          if (use_group_ptrs) {
            group_ptrs.resize(out);
          } else {
            groups.resize(out);
          }
          fusion->post_having_applied = true;
        }
      }
    } else if (fusion->post_having_is_self_sum_factor &&
               active_group_count() == 0) {
      fusion->post_having_applied = true;
    }
  }
  bool direct_having_applied = false;
  if (can_direct_emit && having_prog.has_value() &&
      !fusion->post_having_applied) {
    const size_t group_count = active_group_count();
    if (group_count > 0) {
      const size_t having_workers = ParallelWorkerCount(group_count / 4096);
      std::vector<std::vector<uint32_t>> kept_shards(having_workers);
      std::vector<Status> having_errors(having_workers, Status::kSuccess);
      RETURN_IF_FAIL(RunParallelTasks(having_workers, [&](size_t hw) {
        const size_t begin = (group_count * hw) / having_workers;
        const size_t end = (group_count * (hw + 1)) / having_workers;
        std::vector<uint32_t>& kept = kept_shards[hw];
        kept.reserve(end - begin);
        Row eval_row;
        eval_row.values_.resize(having_row_width);
        for (size_t gi = begin; gi < end; ++gi) {
          const FusedGroup& grp = active_group_at(gi);
          if (raw_aggregates.raw_sum_fusion) {
            for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
              if (grp.RawInfFlags()[s] != 0 &&
                  raw_aggregates.slot_raises_inf[s]) {
                having_errors[hw] =
                    StatusError(StatusCode::kIsInfinity,
                                "SUM overflow: result is infinite");
                return;
              }
            }
          }
          for (const slot_t col : having_cols) {
            eval_row.values_[col] = direct_key_value(grp, col, false);
          }
          for (const size_t ai : having_aggs) {
            if (raw_aggregates.raw_count_fusion) {
              eval_row.values_[left_width + right_width + ai] =
                  Value(grp.raw_count);
            } else if (raw_aggregates.raw_sum_fusion) {
              auto fin = finish_raw_agg(grp, ai);
              if (!fin.HasValue()) {
                having_errors[hw] = fin.GetStatus();
                return;
              }
              eval_row.values_[left_width + right_width + ai] =
                  std::move(fin.Value());
            } else {
              auto fin = states[grp.state_offset + ai].TryFinish();
              if (!fin.HasValue()) {
                having_errors[hw] = fin.GetStatus();
                return;
              }
              eval_row.values_[left_width + right_width + ai] =
                  std::move(fin.Value());
            }
          }
          Value hv;
          Status hst;
          const auto fast =
              having_prog->TryEvaluateRowFast(eval_row, &hv, &hst);
          if (fast == BytecodeProgram::FastEval::kError) {
            having_errors[hw] = std::move(hst);
            return;
          }
          if (fast == BytecodeProgram::FastEval::kUnsupported) {
            auto slow = having_prog->TryEvaluateRow(eval_row);
            if (!slow.HasValue()) {
              having_errors[hw] = slow.GetStatus();
              return;
            }
            hv = std::move(slow.Value());
          }
          if (Truthy(hv)) {
            kept.push_back(static_cast<uint32_t>(gi));
          }
        }
      }));
      for (const Status& err : having_errors) {
        RETURN_IF_FAIL(err);
      }
      size_t total_kept = 0;
      for (const auto& s : kept_shards) {
        total_kept += s.size();
      }
      if (use_group_ptrs) {
        std::vector<FusedGroup*> next_ptrs;
        next_ptrs.reserve(total_kept);
        for (const auto& shard : kept_shards) {
          for (const uint32_t idx : shard) {
            next_ptrs.push_back(group_ptrs[idx]);
          }
        }
        group_ptrs = std::move(next_ptrs);
      } else {
        std::vector<FusedGroup> next_groups;
        next_groups.reserve(total_kept);
        for (const auto& shard : kept_shards) {
          for (const uint32_t idx : shard) {
            next_groups.push_back(std::move(groups[idx]));
          }
        }
        groups = std::move(next_groups);
      }
    }
    direct_having_applied = true;
  }
  if (fusion->topn_limit > 0 && fusion->topn_limit < active_group_count() &&
      !fusion->output_order.empty() && can_direct_emit &&
      (raw_aggregates.raw_count_fusion || raw_aggregates.raw_sum_fusion)) {
    struct FastTopNTerm {
      size_t col_idx{0};
      SortKeyOrder order;
    };
    std::vector<FastTopNTerm> topn_terms;
    topn_terms.reserve(fusion->output_order.size());
    bool can_topn = true;
    const size_t group_count = active_group_count();
    if (raw_aggregates.raw_sum_fusion) {
      for (size_t gi = 0; gi < group_count; ++gi) {
        const FusedGroup& g = active_group_at(gi);
        for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
          if (g.RawInfFlags()[s] != 0) {
            can_topn = false;
            break;
          }
        }
        if (!can_topn) {
          break;
        }
      }
    }
    for (const SelectStatement::OrderByTerm& term : fusion->output_order) {
      if (!can_topn || !term.expression) {
        can_topn = false;
        break;
      }
      std::optional<size_t> resolved_col;
      if (term.expression->Type() == TypeTag::kConstantValue) {
        const Value& cv = term.expression->AsConstantValue().GetValue();
        if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
            cv.value.int_value >= 1 &&
            static_cast<size_t>(cv.value.int_value) <= direct_emit.size()) {
          resolved_col = static_cast<size_t>(cv.value.int_value - 1);
        }
      } else if (term.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
        if (cn.schema.empty()) {
          for (size_t si = 0; si < fusion->select_names.size(); ++si) {
            if (!fusion->select_names[si].empty() &&
                IdentifierEquals(fusion->select_names[si], cn.name)) {
              resolved_col = si;
              break;
            }
          }
        }
      }
      if (!resolved_col.has_value() || *resolved_col >= direct_emit.size() ||
          direct_emit[*resolved_col].program.has_value()) {
        can_topn = false;
        break;
      }
      topn_terms.push_back(
          FastTopNTerm{.col_idx = *resolved_col,
                       .order = SortKeyOrder(term.ascending, term.nulls_first,
                                             Collation::kNone)});
    }
    if (can_topn && !topn_terms.empty()) {
      std::vector<size_t> group_order(group_count);
      for (size_t gi = 0; gi < group_count; ++gi) {
        group_order[gi] = gi;
      }
      const auto cmp_groups = [&](size_t ga, size_t gb) -> bool {
        const FusedGroup& a = active_group_at(ga);
        const FusedGroup& b = active_group_at(gb);
        for (const FastTopNTerm& t : topn_terms) {
          const DirectEmitItem& d = direct_emit[t.col_idx];
          int cmp = 0;
          if (d.constant) {
            continue;
          }
          if (!d.is_agg) {
            Value va;
            Value vb;
            cmp = t.order.Compare(rep_cell(a, d.index, &va),
                                  rep_cell(b, d.index, &vb));
          } else if (raw_aggregates.raw_count_fusion) {
            cmp = t.order.Compare(Value(a.raw_count), Value(b.raw_count));
          } else {
            auto fa = finish_raw_agg(a, d.index);
            auto fb = finish_raw_agg(b, d.index);
            cmp = t.order.Compare(apply_agg_scale(d, std::move(fa.Value())),
                                  apply_agg_scale(d, std::move(fb.Value())));
          }
          if (cmp != 0) {
            return cmp < 0;
          }
        }
        return ga < gb;
      };
      std::partial_sort(
          group_order.begin(),
          group_order.begin() + static_cast<std::ptrdiff_t>(fusion->topn_limit),
          group_order.end(), cmp_groups);
      if (use_group_ptrs) {
        std::vector<FusedGroup*> top_ptrs;
        top_ptrs.reserve(fusion->topn_limit);
        for (size_t k = 0; k < fusion->topn_limit; ++k) {
          top_ptrs.push_back(group_ptrs[group_order[k]]);
        }
        group_ptrs = std::move(top_ptrs);
      } else {
        std::vector<FusedGroup> top_groups;
        top_groups.reserve(fusion->topn_limit);
        for (size_t k = 0; k < fusion->topn_limit; ++k) {
          top_groups.push_back(std::move(groups[group_order[k]]));
        }
        groups = std::move(top_groups);
      }
    }
  }
  if (!can_direct_emit ||
      (!fusion->columnar_output && fusion->output_sink == nullptr)) {
    result.rows.reserve(active_group_count());
  }
  if (track_group_phases) {
    phase_emit_begin = std::chrono::steady_clock::now();
    std::cerr << "[fusion] pre_emit_ms=" << ElapsedMs(join_begin)
              << " setup_ms="
              << std::chrono::duration<double, std::milli>(phase_probe_begin -
                                                           join_begin)
                     .count()
              << " index_ms="
              << std::chrono::duration<double, std::milli>(phase_buckets_done -
                                                           join_begin)
                     .count()
              << " states_ms="
              << std::chrono::duration<double, std::milli>(phase_states_done -
                                                           phase_buckets_done)
                     .count()
              << " probe_ms="
              << std::chrono::duration<double, std::milli>(phase_merge_begin -
                                                           phase_probe_begin)
                     .count()
              << " merge_ms="
              << std::chrono::duration<double, std::milli>(phase_merge_done -
                                                           phase_merge_begin)
                     .count()
              << " post_ms="
              << std::chrono::duration<double, std::milli>(phase_emit_begin -
                                                           phase_merge_done)
                     .count()
              << " groups=" << active_group_count()
              << " left=" << effective_left_rows << "\n";
  }
  const size_t total_out_groups = active_group_count();
  bool pushed_output = false;
  std::shared_ptr<QueryMemoryCharge> push_batch_charge;
  if (fusion->output_sink != nullptr && can_direct_emit && !has_emit_program &&
      (raw_aggregates.raw_count_fusion || raw_aggregates.raw_sum_fusion ||
       fusion->aggregates.empty()) &&
      (!fusion->having || direct_having_applied ||
       fusion->post_having_applied) &&
      !fusion->post_having_active && fusion->topn_limit == 0 &&
      !fusion->rollup_count_distribution) {
    const size_t emit_workers = ParallelWorkerCount(total_out_groups / 8192);
    // Preserve the ordinary output's first-row schema, including all-null
    // columns. Actual cell tags remain in the batches; consumers must check
    // their own narrower representation before accepting a build.
    if (total_out_groups > 0) {
      const auto eval_sink_cell =
          [&](size_t gi, const DirectEmitItem& d) -> StatusOr<Value> {
        const FusedGroup& grp = active_group_at(gi);
        if (d.constant) {
          return *d.constant;
        }
        if (!d.is_agg) {
          return direct_key_value(grp, d.index);
        }
        if (raw_aggregates.raw_count_fusion) {
          return Value(grp.raw_count);
        }
        ASSIGN_OR_RETURN(Value, fin, finish_raw_agg(grp, d.index));
        return apply_agg_scale(d, std::move(fin));
      };
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        const auto& d = direct_emit[c];
        ASSIGN_OR_RETURN(Value, first_val, eval_sink_cell(0, d));
        std::optional<Value> sample;
        if (!first_val.IsNull()) {
          sample = std::move(first_val);
        } else if (!d.constant) {
          for (size_t gi = 1; gi < total_out_groups; ++gi) {
            ASSIGN_OR_RETURN(Value, cand, eval_sink_cell(gi, d));
            if (cand.IsNull()) {
              continue;
            }
            if (!sample.has_value()) {
              sample = std::move(cand);
              break;
            }
          }
          // Preserve homogeneous vs heterogeneous nullable type semantics
          // when group 0 was NULL by verifying remaining non-null values.
          if (sample.has_value()) {
            const ValueType expected_t = ValueTypeOf(*sample);
            const bool expected_u = sample->IsUnsigned();
            const bool expected_iv = sample->IsInterval();
            for (size_t gi = 1; gi < total_out_groups; ++gi) {
              ASSIGN_OR_RETURN(Value, cand, eval_sink_cell(gi, d));
              if (cand.IsNull()) {
                continue;
              }
              if (ValueTypeOf(cand) != expected_t ||
                  cand.IsUnsigned() != expected_u ||
                  cand.IsInterval() != expected_iv) {
                sample.reset();
                break;
              }
            }
          }
        }
        if (sample.has_value()) {
          output_columns[c] =
              Column(output_columns[c].Name(), ValueTypeOf(*sample));
          output_columns[c].SetUnsigned(sample->IsUnsigned());
          output_columns[c].SetInterval(sample->IsInterval());
        } else {
          output_columns[c] =
              Column(output_columns[c].Name(), ValueType::kNull);
        }
      }
    }
    push_batch_charge =
        TryReserveQueryMemory(emit_workers * GroupedOutputSink::kBatchRows *
                              direct_emit.size() * sizeof(Value));
    bool accepted = false;
    if (push_batch_charge) {
      Status begin_status = Status::kSuccess;
      RETURN_IF_FAIL(RunParallelTasks(1, [&](size_t) {
        auto begun = fusion->output_sink->begin(Schema("", output_columns),
                                                total_out_groups, emit_workers);
        begin_status = begun.GetStatus();
        if (begun.HasValue()) accepted = begun.Value();
      }));
      RETURN_IF_FAIL(begin_status);
    }
    if (accepted) {
      fusion->output_sink->used = true;
      pushed_output = true;
      std::vector<Status> errors(emit_workers, Status::kSuccess);
      RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t worker) {
        const auto emit = [&]() -> Status {
          const size_t end = total_out_groups * (worker + 1) / emit_workers;
          const size_t width = direct_emit.size();
          std::vector<Value> batch(GroupedOutputSink::kBatchRows * width);
          for (size_t first = total_out_groups * worker / emit_workers;
               first < end; first += GroupedOutputSink::kBatchRows) {
            const size_t count =
                std::min(GroupedOutputSink::kBatchRows, end - first);
            for (size_t r = 0; r < count; ++r) {
              const auto& group = active_group_at(first + r);
              if (raw_aggregates.raw_sum_fusion) {
                for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
                  if (group.RawInfFlags()[s] &&
                      raw_aggregates.slot_raises_inf[s]) {
                    return StatusError(StatusCode::kIsInfinity,
                                       "SUM overflow: result is infinite");
                  }
                }
              }
              for (size_t c = 0; c < width; ++c) {
                const auto& d = direct_emit[c];
                Value& cell = batch[(r * width) + c];
                if (d.constant) {
                  cell = *d.constant;
                } else if (!d.is_agg) {
                  cell = direct_key_value(group, d.index);
                } else if (raw_aggregates.raw_count_fusion) {
                  cell = Value(group.raw_count);
                } else {
                  ASSIGN_OR_RETURN(Value, value,
                                   finish_raw_agg(group, d.index));
                  cell = apply_agg_scale(d, std::move(value));
                }
              }
            }
            RETURN_IF_FAIL(fusion->output_sink->push(
                worker, first,
                std::span<const Value>(batch.data(), count * width)));
          }
          return Status::kSuccess;
        };
        errors[worker] = emit();
      }));
      for (const Status& error : errors) {
        RETURN_IF_FAIL(error);
      }
      if (auto* runtime = context.execution_runtime()) {
        runtime->pushed_group_rows += total_out_groups;
      }
    }
  }
  const size_t flush_granularity = Relation::ChargeFlushGranularity();
  const auto eval_direct_group =
      [&](const FusedGroup& group, const std::vector<uint8_t>* skip_cols,
          bool own_pk_strings, std::vector<Value>* finished_aggs, Row* eval_row,
          auto&& emit_cell_fn) -> Status {
    const auto prepare_eval_row = [&]() -> Status {
      if (!has_emit_program) {
        return Status::kSuccess;
      }
      for (const slot_t col : emit_needed_cols) {
        eval_row->values_[col] = direct_key_value(group, col, own_pk_strings);
      }
      for (const size_t ai : emit_needed_aggs) {
        if (raw_aggregates.raw_count_fusion) {
          eval_row->values_[left_width + right_width + ai] =
              Value(group.raw_count);
        } else if (raw_aggregates.raw_sum_fusion) {
          ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, ai));
          eval_row->values_[left_width + right_width + ai] =
              std::move(finished);
        } else {
          eval_row->values_[left_width + right_width + ai] =
              (*finished_aggs)[ai];
        }
      }
      return Status::kSuccess;
    };
    const auto eval_direct_item =
        [&](const DirectEmitItem& d) -> StatusOr<Value> {
      Value v;
      Status eval_st;
      const auto fast = d.program->TryEvaluateRowFast(*eval_row, &v, &eval_st);
      if (fast == BytecodeProgram::FastEval::kError) {
        return eval_st;
      }
      if (fast == BytecodeProgram::FastEval::kUnsupported) {
        ASSIGN_OR_RETURN(Value, fallback_v,
                         d.program->TryEvaluateRow(*eval_row));
        v = std::move(fallback_v);
      }
      return v;
    };
    if (raw_aggregates.raw_count_fusion) {
      RETURN_IF_FAIL(prepare_eval_row());
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        if (skip_cols != nullptr && (*skip_cols)[c] != 0) {
          continue;
        }
        const DirectEmitItem& d = direct_emit[c];
        if (d.program.has_value()) {
          ASSIGN_OR_RETURN(Value, v, eval_direct_item(d));
          emit_cell_fn(c, std::move(v));
        } else {
          emit_cell_fn(
              c, d.constant ? *d.constant
                 : d.is_agg ? Value(group.raw_count)
                            : direct_key_value(group, d.index, own_pk_strings));
        }
      }
      return Status::kSuccess;
    }
    if (raw_aggregates.raw_sum_fusion) {
      for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
        if (group.RawInfFlags()[s] && raw_aggregates.slot_raises_inf[s]) {
          return StatusError(StatusCode::kIsInfinity,
                             "SUM overflow: result is infinite");
        }
      }
      RETURN_IF_FAIL(prepare_eval_row());
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        if (skip_cols != nullptr && (*skip_cols)[c] != 0) {
          continue;
        }
        const DirectEmitItem& d = direct_emit[c];
        if (d.program.has_value()) {
          ASSIGN_OR_RETURN(Value, v, eval_direct_item(d));
          emit_cell_fn(c, std::move(v));
          continue;
        }
        if (d.constant || !d.is_agg) {
          emit_cell_fn(
              c, d.constant ? *d.constant
                            : direct_key_value(group, d.index, own_pk_strings));
          continue;
        }
        const size_t i = d.index;
        if (raw_aggregates.raw_const_folded[i]) {
          emit_cell_fn(c, raw_aggregates.raw_constants[i]);
          continue;
        }
        const auto slot = static_cast<size_t>(
            static_cast<unsigned char>(raw_aggregates.raw_agg_slot[i]));
        const int64_t count = group.RawCounts()[slot];
        switch (raw_aggregates.raw_agg_kinds[i]) {
          case RawAggKind::kCountStar:
          case RawAggKind::kCountValue:
            emit_cell_fn(c, Value(count));
            break;
          case RawAggKind::kSumInt64:
            emit_cell_fn(c,
                         count == 0 ? Value() : Value(group.RawInts()[slot]));
            break;
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble: {
            if (count == 0) {
              emit_cell_fn(c, Value());
            } else {
              double val =
                  raw_aggregates.raw_agg_kinds[i] == RawAggKind::kAvgDouble
                      ? group.RawDoubles()[slot] / static_cast<double>(count)
                      : group.RawDoubles()[slot];
              if (d.has_agg_scale) {
                val *= d.agg_scale;
              }
              emit_cell_fn(c, Value(val));
            }
            break;
          }
          default: {
            ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, i));
            emit_cell_fn(c, apply_agg_scale(d, std::move(finished)));
            break;
          }
        }
      }
      return Status::kSuccess;
    }
    finished_aggs->clear();
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      AggregateAccumulator& accumulator = states[group.state_offset + i];
      ASSIGN_OR_RETURN(Value, finished, accumulator.TryFinish());
      finished_aggs->push_back(std::move(finished));
    }
    RETURN_IF_FAIL(prepare_eval_row());
    for (size_t c = 0; c < direct_emit.size(); ++c) {
      if (skip_cols != nullptr && (*skip_cols)[c] != 0) {
        continue;
      }
      const DirectEmitItem& d = direct_emit[c];
      if (d.program.has_value()) {
        ASSIGN_OR_RETURN(Value, v, eval_direct_item(d));
        emit_cell_fn(c, std::move(v));
      } else {
        emit_cell_fn(c, d.constant ? *d.constant
                        : d.is_agg
                            ? apply_agg_scale(d, (*finished_aggs)[d.index])
                            : direct_key_value(group, d.index, own_pk_strings));
      }
    }
    return Status::kSuccess;
  };
  const bool emit_own_pk_strings = context.execution_runtime() == nullptr;
  const auto emit_direct =
      [&](size_t begin, size_t end, Relation* output,
          std::vector<ColumnTypeTracker>* col_tracks) -> Status {
    if (!output->HasSpill() && output->rows.empty() && end > begin) {
      output->rows.reserve(end - begin);
    }
    std::vector<Value> finished_aggs;
    finished_aggs.reserve(fusion->aggregates.size());
    Row eval_row;
    if (has_emit_program) {
      eval_row.values_.resize(emit_row_width);
    }
    for (size_t index = begin; index < end; ++index) {
      const FusedGroup& group = active_group_at(index);
      Row output_row;
      output_row.values_.reserve(direct_emit.size());
      size_t row_bytes = 64;
      RETURN_IF_FAIL(eval_direct_group(
          group, nullptr, emit_own_pk_strings, &finished_aggs, &eval_row,
          [&](size_t c, Value cell) {
            if (col_tracks != nullptr && c < col_tracks->size()) {
              (*col_tracks)[c].Observe(cell);
            }
            row_bytes += cell.type == ValueType::kArray
                             ? EstimateValueBytes(cell)
                             : EstimateScalarValueBytes(cell);
            output_row.values_.push_back(std::move(cell));
          }));
      output->AddRowWithKnownBytes(std::move(output_row), row_bytes,
                                   flush_granularity);
    }
    RETURN_IF_FAIL(output->FinishSpill());
    return output->GetStatus();
  };
  const bool fast_col_emit =
      (raw_aggregates.raw_count_fusion || raw_aggregates.raw_sum_fusion ||
       fusion->aggregates.empty()) &&
      !has_emit_program &&
      std::ranges::none_of(direct_emit, [](const DirectEmitItem& item) {
        return item.program.has_value();
      });
  const auto emit_direct_columnar =
      [&](size_t begin, size_t end, std::vector<ColumnarChunk>* out_chunks,
          std::vector<ColumnTypeTracker>* col_tracks) -> Status {
    if (end <= begin) {
      return Status::kSuccess;
    }
    std::vector<Value> finished_aggs;
    finished_aggs.reserve(fusion->aggregates.size());
    Row eval_row;
    if (has_emit_program) {
      eval_row.values_.resize(emit_row_width);
    }
    const auto track_uniform_col = [&](size_t c, const ColumnarColumn& col,
                                       bool non_null_seen) {
      if (!non_null_seen || col_tracks == nullptr || c >= col_tracks->size()) {
        return;
      }
      ColumnTypeTracker& ct = (*col_tracks)[c];
      if (!ct.seen_non_null) {
        ct.seen_non_null = true;
        ct.type = col.type;
        ct.is_unsigned = col.is_unsigned;
        ct.is_interval = col.is_interval;
      } else if (ct.type != col.type || ct.is_unsigned != col.is_unsigned ||
                 ct.is_interval != col.is_interval) {
        ct.mixed = true;
      }
    };
    const auto track_fallback_col = [&](size_t c, const ColumnarColumn& col) {
      if (col_tracks == nullptr || c >= col_tracks->size()) {
        return;
      }
      ColumnTypeTracker& ct = (*col_tracks)[c];
      for (const Value& v : col.fallback) {
        ct.Observe(v);
      }
    };
    out_chunks->reserve((end - begin + kColumnarChunkRows - 1) /
                        kColumnarChunkRows);
    std::vector<uint32_t> chunk_rch;
    std::vector<size_t> chunk_rr;
    for (size_t c_begin = begin; c_begin < end; c_begin += kColumnarChunkRows) {
      const size_t c_end = std::min(end, c_begin + kColumnarChunkRows);
      const size_t chunk_rows = c_end - c_begin;
      ColumnarChunk chunk;
      chunk.rows = chunk_rows;
      chunk.columns.resize(direct_emit.size());
      std::vector<uint8_t> is_const_col(direct_emit.size(), 0);
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        const DirectEmitItem& d = direct_emit[c];
        if (!d.program.has_value() && d.constant.has_value() &&
            TrySetConstantColumnarColumn(&chunk.columns[c], ValueType::kNull,
                                         false, false, *d.constant, chunk_rows,
                                         &chunk)) {
          is_const_col[c] = 1;
          if (col_tracks != nullptr && c < col_tracks->size()) {
            (*col_tracks)[c].Observe(*d.constant);
          }
          continue;
        }
        ValueType init_vt = ValueType::kNull;
        bool init_unsigned = false;
        if (!d.program.has_value() && d.is_agg) {
          if (raw_aggregates.raw_count_fusion) {
            init_vt = ValueType::kInt64;
          } else if (raw_aggregates.raw_sum_fusion &&
                     d.index < raw_aggregates.raw_agg_kinds.size()) {
            switch (raw_aggregates.raw_agg_kinds[d.index]) {
              case RawAggKind::kCountStar:
              case RawAggKind::kCountValue:
              case RawAggKind::kCountDistinctInt64:
              case RawAggKind::kSumInt64:
                init_vt = ValueType::kInt64;
                break;
              case RawAggKind::kSumDouble:
              case RawAggKind::kAvgDouble:
              case RawAggKind::kStddevSamp:
              case RawAggKind::kStddevPop:
              case RawAggKind::kVarSamp:
              case RawAggKind::kVarPop:
                init_vt = ValueType::kDouble;
                break;
              default:
                break;
            }
          }
        } else if (!d.program.has_value() && !d.is_agg) {
          if (d.index < left_width) {
            if (d.index < 16 && def_col_to_pk_lookup[d.index] >= 0) {
              const auto& lkp = prepared_pk_lookups[static_cast<size_t>(
                  def_col_to_pk_lookup[d.index])];
              init_vt = lkp.payload_type;
              init_unsigned = lkp.payload_unsigned;
            } else if ((left.uniform_cell_types || left.HasColumnarOnly() ||
                        deferred_left_probe != nullptr) &&
                       (d.index >= def_overwritten_by_pk.size() ||
                        def_overwritten_by_pk[d.index] == 0)) {
              const Column& src_col = left.schema.GetColumn(d.index);
              if (!src_col.IsInterval() &&
                  (src_col.Type() == ValueType::kInt64 ||
                   src_col.Type() == ValueType::kDate ||
                   src_col.Type() == ValueType::kDouble ||
                   src_col.Type() == ValueType::kVarChar)) {
                init_vt = src_col.Type();
                init_unsigned = src_col.IsUnsigned();
              }
            }
          } else if (d.index - left_width < right_width &&
                     (right.uniform_cell_types || right.HasColumnarOnly())) {
            const Column& src_col =
                right.schema.GetColumn(d.index - left_width);
            if (!src_col.IsInterval() &&
                (src_col.Type() == ValueType::kInt64 ||
                 src_col.Type() == ValueType::kDate ||
                 src_col.Type() == ValueType::kDouble ||
                 src_col.Type() == ValueType::kVarChar)) {
              init_vt = src_col.Type();
              init_unsigned = src_col.IsUnsigned();
            }
          }
        }
        chunk.columns[c].Init(
            init_vt, init_unsigned, false,
            (!fast_col_emit && init_vt != ValueType::kNull) ? chunk_rows : 0);
      }
      ChunkStringArena* arena = chunk.string_arena.get();
      if (fast_col_emit) {
        if (raw_aggregates.raw_sum_fusion &&
            (has_double_raw_agg || has_int_raw_agg || has_stat_raw_agg)) {
          for (size_t index = c_begin; index < c_end; ++index) {
            const FusedGroup& group = active_group_at(index);
            for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
              if (group.RawInfFlags()[s] && raw_aggregates.slot_raises_inf[s]) {
                return StatusError(StatusCode::kIsInfinity,
                                   "SUM overflow: result is infinite");
              }
            }
          }
        }
        bool all_hash_only = true;
        for (size_t r = 0; r < chunk_rows; ++r) {
          const FusedGroup& grp = active_group_at(c_begin + r);
          if (grp.representative != nullptr || grp.rep_left != nullptr ||
              grp.rep_col_chunk != std::numeric_limits<uint32_t>::max() ||
              !grp.has_key_hash) {
            all_hash_only = false;
            break;
          }
        }
        bool have_chunk_rlocs = false;
        bool chunk_rlocs_ok = false;
        bool right_keepalive_added = false;
        for (size_t c = 0; c < direct_emit.size(); ++c) {
          if (is_const_col[c] != 0) {
            continue;
          }
          const DirectEmitItem& d = direct_emit[c];
          ColumnarColumn& col = chunk.columns[c];
          auto emit_col_via_rep = [&](size_t start_r) -> Status {
            for (size_t r = start_r; r < chunk_rows; ++r) {
              Value cell = direct_key_value(active_group_at(c_begin + r),
                                            d.index, false);
              if (col_tracks != nullptr && c < col_tracks->size()) {
                (*col_tracks)[c].Observe(cell);
              }
              if (cell.type == ValueType::kVarChar && arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              col.AppendValue(cell, arena, r);
            }
            return Status::kSuccess;
          };
          if (d.is_agg) {
            if (raw_aggregates.raw_count_fusion) {
              col.ints.resize(chunk_rows);
              int64_t* dst = col.ints.data();
              for (size_t r = 0; r < chunk_rows; ++r) {
                dst[r] = active_group_at(c_begin + r).raw_count;
              }
              track_uniform_col(c, col, true);
              continue;
            }
            const size_t i = d.index;
            if (!raw_aggregates.raw_const_folded[i]) {
              const auto slot = static_cast<size_t>(
                  static_cast<unsigned char>(raw_aggregates.raw_agg_slot[i]));
              const RawAggKind kind = raw_aggregates.raw_agg_kinds[i];
              if (kind == RawAggKind::kCountStar ||
                  kind == RawAggKind::kCountValue) {
                col.ints.resize(chunk_rows);
                int64_t* dst = col.ints.data();
                for (size_t r = 0; r < chunk_rows; ++r) {
                  dst[r] = active_group_at(c_begin + r).RawCounts()[slot];
                }
                track_uniform_col(c, col, true);
                continue;
              }
              if (kind == RawAggKind::kSumInt64) {
                col.ints.resize(chunk_rows);
                int64_t* dst = col.ints.data();
                bool non_null_seen = false;
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const FusedGroup& g = active_group_at(c_begin + r);
                  if (g.RawCounts()[slot] == 0) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0;
                  } else {
                    dst[r] = g.RawInts()[slot];
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              if (kind == RawAggKind::kSumDouble ||
                  kind == RawAggKind::kAvgDouble) {
                col.doubles.resize(chunk_rows);
                double* dst = col.doubles.data();
                const bool is_avg = (kind == RawAggKind::kAvgDouble);
                const bool scaled = d.has_agg_scale;
                const double scale = d.agg_scale;
                bool non_null_seen = false;
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const FusedGroup& g = active_group_at(c_begin + r);
                  const int64_t cnt = g.RawCounts()[slot];
                  if (cnt == 0) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0.0;
                  } else {
                    const double val =
                        is_avg
                            ? (g.RawDoubles()[slot] / static_cast<double>(cnt))
                            : g.RawDoubles()[slot];
                    dst[r] = scaled ? (val * scale) : val;
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              if (kind == RawAggKind::kStddevSamp ||
                  kind == RawAggKind::kStddevPop ||
                  kind == RawAggKind::kVarSamp || kind == RawAggKind::kVarPop) {
                col.doubles.resize(chunk_rows);
                double* dst = col.doubles.data();
                const bool is_samp = (kind == RawAggKind::kStddevSamp ||
                                      kind == RawAggKind::kVarSamp);
                const bool is_stddev = (kind == RawAggKind::kStddevSamp ||
                                        kind == RawAggKind::kStddevPop);
                const int64_t min_n = is_samp ? 2 : 1;
                const bool scaled = d.has_agg_scale;
                const double scale = d.agg_scale;
                bool non_null_seen = false;
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const FusedGroup& g = active_group_at(c_begin + r);
                  const int64_t n = g.RawCounts()[slot];
                  if (n < min_n) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0.0;
                  } else {
                    const platform::ExtendedFloat mean = g.RawStatX()[slot] / n;
                    const platform::ExtendedFloat ssd =
                        g.RawStatXX()[slot] - (n * mean * mean);
                    const platform::ExtendedFloat variance =
                        ssd > 0 ? ssd / (is_samp ? n - 1 : n)
                                : platform::ExtendedFloat{0};
                    const auto val = static_cast<double>(
                        is_stddev ? platform::Sqrt(variance) : variance);
                    dst[r] = scaled ? (val * scale) : val;
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
            }
            for (size_t r = 0; r < chunk_rows; ++r) {
              Value cell;
              if (raw_aggregates.raw_const_folded[i]) {
                cell = raw_aggregates.raw_constants[i];
              } else {
                ASSIGN_OR_RETURN(
                    Value, agg_cell,
                    finish_raw_agg(active_group_at(c_begin + r), i));
                cell = apply_agg_scale(d, std::move(agg_cell));
              }
              if (col_tracks != nullptr && c < col_tracks->size()) {
                (*col_tracks)[c].Observe(cell);
              }
              if (cell.type == ValueType::kVarChar && arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              col.AppendValue(cell, arena, r);
            }
            continue;
          }
          if (all_hash_only && layout.packed_2x32_unpack_only &&
              (d.index == layout.packed_2x32_slot0 ||
               d.index == layout.packed_2x32_slot1) &&
              col.type != ValueType::kNull) {
            col.ints.resize(chunk_rows);
            int64_t* dst = col.ints.data();
            const bool first = (d.index == layout.packed_2x32_slot0);
            for (size_t r = 0; r < chunk_rows; ++r) {
              const uint64_t kh = active_group_at(c_begin + r).key_hash;
              dst[r] = static_cast<int64_t>(static_cast<int32_t>(
                  first ? (kh >> 32) : (kh & 0xffffffffULL)));
            }
            track_uniform_col(c, col, true);
            continue;
          }
          if (all_hash_only && layout.split_key_unpack_only &&
              layout.split_has_left_probe &&
              layout.split_left_slot.has_value() &&
              d.index == *layout.split_left_slot &&
              col.type != ValueType::kNull) {
            col.ints.resize(chunk_rows);
            int64_t* dst = col.ints.data();
            for (size_t r = 0; r < chunk_rows; ++r) {
              dst[r] = static_cast<int64_t>(static_cast<int32_t>(
                  active_group_at(c_begin + r).key_hash & 0xffffffffULL));
            }
            track_uniform_col(c, col, true);
            continue;
          }
          if (all_hash_only && layout.split_key_unpack_only &&
              d.index < left_width && d.index < 16 &&
              def_col_to_pk_lookup[d.index] >= 0 &&
              col.type != ValueType::kNull) {
            const auto li = static_cast<size_t>(
                static_cast<unsigned char>(def_col_to_pk_lookup[d.index]));
            const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[li];
            const PreparedDeferredPkGroup& pk_grp =
                def_pk_groups[lkp.group_idx];
            if (pk_grp.use_subgid && pk_grp.subgid_rep_ptr != nullptr &&
                (col.type != ValueType::kVarChar ||
                 pk_grp.store_owner != nullptr)) {
              const uint32_t p_card = pk_grp.subgid_count;
              const uint64_t p_mask =
                  p_card <= 1
                      ? 0ULL
                      : ((uint64_t{1} << std::bit_width(p_card - 1)) - 1ULL);
              const uint8_t p_shift = pk_grp.subgid_shift;
              const auto* rep_ptrs = pk_grp.subgid_rep_ptr;
              const auto* blk_ptr = lkp.blocks.data();
              bool non_null_seen = false;
              if (col.type == ValueType::kVarChar) {
                col.encoding_owner = pk_grp.store_owner;
                chunk.keepalives.push_back(pk_grp.store_owner);
                col.strings.resize(chunk_rows);
                std::string_view* dst = col.strings.data();
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const auto sg = static_cast<uint32_t>(
                      (active_group_at(c_begin + r).key_hash >> p_shift) &
                      p_mask);
                  const auto [nb, rin] = rep_ptrs[sg];
                  const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                  if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = {};
                  } else {
                    dst[r] = pb.pay_blk->VarcharAt(rin);
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              if (col.type == ValueType::kDouble) {
                col.doubles.resize(chunk_rows);
                double* dst = col.doubles.data();
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const auto sg = static_cast<uint32_t>(
                      (active_group_at(c_begin + r).key_hash >> p_shift) &
                      p_mask);
                  const auto [nb, rin] = rep_ptrs[sg];
                  const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                  if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0.0;
                  } else {
                    const auto dv = std::bit_cast<double>(pb.pay_cells[rin]);
                    dst[r] = dv == 0.0 ? 0.0 : dv;
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              col.ints.resize(chunk_rows);
              int64_t* dst = col.ints.data();
              for (size_t r = 0; r < chunk_rows; ++r) {
                const auto sg = static_cast<uint32_t>(
                    (active_group_at(c_begin + r).key_hash >> p_shift) &
                    p_mask);
                const auto [nb, rin] = rep_ptrs[sg];
                const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                  if (!col.has_null) {
                    col.has_null = true;
                    col.nulls.assign(chunk_rows, 0);
                  }
                  col.nulls[r] = 1;
                  dst[r] = 0;
                } else {
                  dst[r] = pb.pay_cells[rin];
                  non_null_seen = true;
                }
              }
              track_uniform_col(c, col, non_null_seen);
              continue;
            }
          }
          if (all_hash_only && layout.left_pack_unpack_only &&
              d.index < left_width && col.type != ValueType::kNull) {
            bool handled_lp = false;
            for (const LeftPackedIntCol& lpi : layout.left_pack_ints) {
              if (lpi.col == d.index && (col.type == ValueType::kInt64 ||
                                         col.type == ValueType::kDate)) {
                col.ints.resize(chunk_rows);
                int64_t* dst = col.ints.data();
                bool non_null_seen = false;
                const uint64_t mask = lpi.mask;
                const uint8_t shift = lpi.shift;
                const uint64_t null_code = lpi.null_code;
                const auto min_u = static_cast<uint64_t>(lpi.min_val);
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const uint64_t kh = active_group_at(c_begin + r).key_hash;
                  const uint64_t code = (kh >> shift) & mask;
                  if (code == null_code) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0;
                  } else {
                    dst[r] = static_cast<int64_t>(min_u + code);
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                handled_lp = true;
                break;
              }
            }
            if (handled_lp) {
              continue;
            }
            if (layout.left_pack_uses_dict && layout.left_col_only &&
                col.type == ValueType::kVarChar &&
                std::ranges::find(layout.left_pack_dict_cols, d.index) !=
                    layout.left_pack_dict_cols.end()) {
              if (arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              const size_t dict_n = layout.left_pack_dict_rep_col_loc.size();
              std::vector<std::pair<uint8_t, std::string_view>> dict_strs(
                  dict_n, {uint8_t{1}, std::string_view{}});
              bool bad_dict_type = false;
              Value rep_tmp;
              for (size_t di = 0; di < dict_n; ++di) {
                const auto [rci, rr] = layout.left_pack_dict_rep_col_loc[di];
                if (rci == std::numeric_limits<uint32_t>::max()) {
                  continue;
                }
                left.columnar->chunks[rci].columns[d.index].WriteValueInto(
                    rr, &rep_tmp);
                if (!rep_tmp.IsNull()) {
                  if (rep_tmp.type != ValueType::kVarChar) {
                    bad_dict_type = true;
                    break;
                  }
                  dict_strs[di] = {uint8_t{0},
                                   arena->Store(rep_tmp.value.varchar_value)};
                }
              }
              if (!bad_dict_type) {
                col.strings.resize(chunk_rows);
                std::string_view* dst = col.strings.data();
                bool non_null_seen = false;
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const auto di = static_cast<size_t>(
                      (active_group_at(c_begin + r).key_hash >>
                       layout.left_pack_dict_shift) &
                      layout.left_pack_dict_mask);
                  const auto& entry = dict_strs[di];
                  if (entry.first != 0) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = {};
                  } else {
                    dst[r] = entry.second;
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
            }
          }
          std::optional<size_t> eq_right_col;
          if (d.index < left_width && join_type == JoinType::kInner &&
              col.type != ValueType::kNull && col.type != ValueType::kVarChar &&
              d.index < left.schema.ColumnCount()) {
            const Column& lc = left.schema.GetColumn(d.index);
            for (const EqualityKey& eq : equality_keys) {
              if (eq.left == d.index && eq.left_offset == 0 &&
                  eq.right_offset == 0 && !eq.null_safe &&
                  eq.right < right.schema.ColumnCount()) {
                const Column& rc = right.schema.GetColumn(eq.right);
                if (lc.Type() == rc.Type() &&
                    lc.IsUnsigned() == rc.IsUnsigned() &&
                    lc.Type() != ValueType::kVarChar) {
                  eq_right_col = eq.right;
                  break;
                }
              }
            }
          }
          if ((d.index >= left_width || eq_right_col.has_value()) &&
              col.type != ValueType::kNull) {
            const size_t rc =
                d.index >= left_width ? (d.index - left_width) : *eq_right_col;
            const bool can_split_right_rep =
                all_hash_only && layout.split_right_rep_ptr != nullptr &&
                ((layout.split_key_unpack_only &&
                  layout.split_uses_right_subgid) ||
                 (layout.left_pack_unpack_only && layout.left_pack_uses_right));
            const uint64_t r_mask =
                !can_split_right_rep
                    ? 0ULL
                    : ((layout.left_pack_unpack_only &&
                        layout.left_pack_uses_right)
                           ? layout.left_pack_right_mask
                           : (layout.split_right_subgid_count <= 1
                                  ? 0ULL
                                  : ((uint64_t{1} << std::bit_width(
                                          layout.split_right_subgid_count -
                                          1)) -
                                     1ULL)));
            const auto get_right_idx =
                [&](const FusedGroup& grp) noexcept -> uint32_t {
              if (grp.rep_right_idx != kInvalidRightIdx) {
                return grp.rep_right_idx;
              }
              if (can_split_right_rep && grp.has_key_hash &&
                  grp.key.values_.empty()) {
                const auto r_subgid = static_cast<size_t>(
                    (grp.key_hash >> layout.split_right_shift) & r_mask);
                if (r_subgid < layout.split_right_rep_count) {
                  return layout.split_right_rep_ptr[r_subgid];
                }
              }
              return kInvalidRightIdx;
            };
            const auto get_right_val =
                [&](const FusedGroup& grp) noexcept -> const Value* {
              if (grp.representative != nullptr) {
                return &grp.representative->values_[d.index];
              }
              const uint32_t r_idx = get_right_idx(grp);
              if (r_idx != kInvalidRightIdx) {
                return &right_ptr->rows[r_idx].values_[rc];
              }
              return nullptr;
            };
            if (right_col_only && !have_chunk_rlocs) {
              have_chunk_rlocs = true;
              chunk_rlocs_ok = true;
              chunk_rch.resize(chunk_rows);
              chunk_rr.resize(chunk_rows);
              for (size_t r = 0; r < chunk_rows; ++r) {
                const FusedGroup& grp = active_group_at(c_begin + r);
                if (grp.representative != nullptr) {
                  chunk_rlocs_ok = false;
                  break;
                }
                const uint32_t r_idx = get_right_idx(grp);
                if (r_idx == kInvalidRightIdx) {
                  chunk_rlocs_ok = false;
                  break;
                }
                const auto [rch, rr] = right_locator.Locate(r_idx);
                chunk_rch[r] = static_cast<uint32_t>(rch);
                chunk_rr[r] = rr;
              }
            }
            bool non_null_seen = false;
            bool demoted = false;
            size_t resume_r = chunk_rows;
            const auto demote_at = [&](size_t r) {
              col.DemoteToFallback(r);
              demoted = true;
              resume_r = r;
            };
            const auto set_null_row = [&](size_t r) {
              if (!col.has_null) {
                col.has_null = true;
                col.nulls.assign(chunk_rows, 0);
              }
              col.nulls[r] = 1;
            };
            const auto emit_uniform_loop = [&](auto col_tag, ValueType exp_vt,
                                               bool exp_uns, auto* dst_ptr,
                                               auto zero_val,
                                               auto&& store_from_leaf,
                                               auto&& store_from_val) {
              constexpr bool kRightColOnly = decltype(col_tag)::value;
              if constexpr (kRightColOnly) {
                if (chunk_rlocs_ok) {
                  const ColumnarChunk* const right_chunks =
                      right_arg.columnar->chunks.data();
                  const uint32_t* const rchs = chunk_rch.data();
                  const size_t* const rrs = chunk_rr.data();
                  for (size_t r = 0; r < chunk_rows; ++r) {
                    const ColumnarColumn& sc =
                        right_chunks[rchs[r]].columns[rc];
                    const ColumnarColumn* leaf = &sc;
                    size_t lr = rrs[r];
                    const bool is_null =
                        (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                            ? !sc.ResolveLeaf(lr, &leaf, &lr)
                            : leaf->NonGatheredIsNull(lr);
                    if (is_null) {
                      set_null_row(r);
                      dst_ptr[r] = zero_val;
                    } else if (leaf->is_uniform && leaf->type == exp_vt &&
                               leaf->is_unsigned == exp_uns &&
                               !leaf->is_interval) {
                      dst_ptr[r] = store_from_leaf(leaf, lr);
                      non_null_seen = true;
                    } else {
                      demote_at(r);
                      break;
                    }
                  }
                  return;
                }
              }
              for (size_t r = 0; r < chunk_rows; ++r) {
                const FusedGroup& grp = active_group_at(c_begin + r);
                if constexpr (kRightColOnly) {
                  if (grp.representative != nullptr) {
                    demote_at(r);
                    break;
                  }
                  const uint32_t r_idx = get_right_idx(grp);
                  if (r_idx == kInvalidRightIdx) {
                    demote_at(r);
                    break;
                  }
                  const auto [rch, rr] = right_locator.Locate(r_idx);
                  const ColumnarColumn& sc =
                      right_arg.columnar->chunks[rch].columns[rc];
                  const ColumnarColumn* leaf = &sc;
                  size_t lr = rr;
                  const bool is_null =
                      (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                          ? !sc.ResolveLeaf(rr, &leaf, &lr)
                          : leaf->NonGatheredIsNull(lr);
                  if (is_null) {
                    set_null_row(r);
                    dst_ptr[r] = zero_val;
                  } else if (leaf->is_uniform && leaf->type == exp_vt &&
                             leaf->is_unsigned == exp_uns &&
                             !leaf->is_interval) {
                    dst_ptr[r] = store_from_leaf(leaf, lr);
                    non_null_seen = true;
                  } else {
                    demote_at(r);
                    break;
                  }
                } else {
                  const Value* cp = get_right_val(grp);
                  if (cp == nullptr) {
                    demote_at(r);
                    break;
                  }
                  if (cp->IsNull()) {
                    set_null_row(r);
                    dst_ptr[r] = zero_val;
                  } else if (cp->type == exp_vt &&
                             cp->IsUnsigned() == exp_uns && !cp->IsInterval() &&
                             (exp_uns || cp->Collation() == 0)) {
                    dst_ptr[r] = store_from_val(cp);
                    non_null_seen = true;
                  } else {
                    demote_at(r);
                    break;
                  }
                }
              }
            };
            const auto run_right_emit = [&](auto&& emit_with_tag) {
              if (right_col_only) {
                emit_with_tag(std::true_type{});
              } else {
                emit_with_tag(std::false_type{});
              }
            };
            if (col.type == ValueType::kVarChar) {
              col.strings.resize(chunk_rows);
              std::string_view* dst = col.strings.data();
              if (right_col_only) {
                col.encoding_owner = right_col_owner;
                if (!right_keepalive_added) {
                  chunk.keepalives.push_back(right_col_owner);
                  right_keepalive_added = true;
                }
              }
              run_right_emit([&](auto col_tag) {
                emit_uniform_loop(
                    col_tag, ValueType::kVarChar, false, dst,
                    std::string_view{},
                    [](const ColumnarColumn* leaf, size_t lr) {
                      switch (leaf->encoding) {
                        case ColumnarColumn::Encoding::kDictionary: {
                          const uint32_t code =
                              leaf->borrowed_dict_u8 != nullptr
                                  ? leaf->borrowed_dict_u8[lr]
                                  : leaf->dict_codes[lr];
                          return leaf->dict->StringEntryAt(code);
                        }
                        case ColumnarColumn::Encoding::kFlat:
                          return leaf->strings[lr];
                        case ColumnarColumn::Encoding::kConstant:
                          return leaf->const_string;
                        default:
                          return leaf->StringAt(lr);
                      }
                    },
                    [&](const Value* cp) {
                      if (arena == nullptr) {
                        arena = chunk.EnsureArena();
                      }
                      return arena->Store(cp->value.varchar_value);
                    });
              });
            } else if (col.type == ValueType::kDouble) {
              col.doubles.resize(chunk_rows);
              double* dst = col.doubles.data();
              run_right_emit([&](auto col_tag) {
                emit_uniform_loop(
                    col_tag, ValueType::kDouble, false, dst, 0.0,
                    [](const ColumnarColumn* leaf, size_t lr) {
                      return leaf->DoubleAt(lr);
                    },
                    [](const Value* cp) { return cp->value.double_value; });
              });
            } else {
              col.ints.resize(chunk_rows);
              int64_t* dst = col.ints.data();
              const ValueType exp_vt = col.type;
              const bool exp_uns = col.is_unsigned;
              run_right_emit([&](auto col_tag) {
                emit_uniform_loop(
                    col_tag, exp_vt, exp_uns, dst, int64_t{0},
                    [](const ColumnarColumn* leaf, size_t lr) {
                      return leaf->IntAt(lr);
                    },
                    [](const Value* cp) { return cp->value.int_value; });
              });
            }
            if (!demoted) {
              track_uniform_col(c, col, non_null_seen);
              continue;
            }
            track_fallback_col(c, col);
            RETURN_IF_FAIL(emit_col_via_rep(resume_r));
            continue;
          }
          RETURN_IF_FAIL(emit_col_via_rep(0));
        }
        out_chunks->push_back(std::move(chunk));
        continue;
      }
      for (size_t index = c_begin, local_r = 0; index < c_end;
           ++index, ++local_r) {
        const FusedGroup& group = active_group_at(index);
        RETURN_IF_FAIL(eval_direct_group(
            group, &is_const_col, false, &finished_aggs, &eval_row,
            [&](size_t c, const Value& cell) {
              if (col_tracks != nullptr && c < col_tracks->size()) {
                (*col_tracks)[c].Observe(cell);
              }
              if (cell.type == ValueType::kVarChar && arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              chunk.columns[c].AppendValue(cell, arena, local_r);
            }));
      }
      out_chunks->push_back(std::move(chunk));
    }
    return Status::kSuccess;
  };
  bool finalized_output_types = false;
  bool all_uniform_columns = false;
  std::shared_ptr<ColumnarTable> out_columnar;
  if (can_direct_emit && !pushed_output) {
    const size_t emit_denom = direct_emit.size() >= 6 ? 2048 : 8192;
    const size_t emit_workers =
        (raw_aggregates.raw_count_fusion || raw_aggregates.raw_sum_fusion ||
         fusion->aggregates.empty()) &&
                ::tinylamb::platform::GetEnv(
                    "TINYLAMB_DISABLE_PARALLEL_FUSED_EMIT") == nullptr
            ? ParallelWorkerCount(total_out_groups / emit_denom)
            : 1;
    std::vector<std::vector<ColumnTypeTracker>> emit_col_tracks(
        emit_workers, std::vector<ColumnTypeTracker>(output_columns.size()));
    if (fusion->columnar_output) {
      out_columnar = std::make_shared<ColumnarTable>();
      if (emit_workers > 1) {
        std::vector<std::vector<ColumnarChunk>> shard_chunks(emit_workers);
        std::vector<Status> errors(emit_workers, Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t w) {
          errors[w] =
              emit_direct_columnar((total_out_groups * w) / emit_workers,
                                   (total_out_groups * (w + 1)) / emit_workers,
                                   &shard_chunks[w], &emit_col_tracks[w]);
        }));
        for (const Status& err : errors) {
          RETURN_IF_FAIL(err);
        }
        for (auto& chunks : shard_chunks) {
          for (ColumnarChunk& chunk : chunks) {
            out_columnar->AppendChunk(std::move(chunk));
          }
        }
      } else {
        std::vector<ColumnarChunk> chunks;
        RETURN_IF_FAIL(emit_direct_columnar(0, total_out_groups, &chunks,
                                            emit_col_tracks.data()));
        for (ColumnarChunk& chunk : chunks) {
          out_columnar->AppendChunk(std::move(chunk));
        }
      }
      if (right_col_owner != nullptr) {
        out_columnar->AppendKeepalive(right_col_owner);
      }
    } else if (emit_workers > 1) {
      // Finishing independent raw states does not reassociate their inputs.
      // Contiguous shards retain the historical group order after the merge.
      std::vector<ExecutionRuntime> runtimes(emit_workers);
      std::vector<Relation> shards(emit_workers);
      std::vector<Status> errors(emit_workers, Status::kSuccess);
      for (size_t w = 0; w < emit_workers; ++w) {
        shards[w].set_runtime(&runtimes[w]);
      }
      RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t w) {
        errors[w] = emit_direct((total_out_groups * w) / emit_workers,
                                (total_out_groups * (w + 1)) / emit_workers,
                                &shards[w], &emit_col_tracks[w]);
      }));
      for (size_t w = 0; w < emit_workers; ++w) {
        RETURN_IF_FAIL(errors[w]);
        if (result.runtime() != nullptr) {
          result.runtime()->relation_spills += runtimes[w].relation_spills;
        }
      }
      RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &result));
    } else {
      RETURN_IF_FAIL(
          emit_direct(0, total_out_groups, &result, emit_col_tracks.data()));
    }
    all_uniform_columns =
        ColumnTypeTracker::FinalizeColumns(emit_col_tracks, output_columns);
    finalized_output_types = true;
  }
  Row eval_rep_row;
  std::vector<ColumnTypeTracker> fallback_col_tracks;
  if (!can_direct_emit) {
    eval_rep_row.values_.resize(result.schema.ColumnCount());
    fallback_col_tracks.resize(output_columns.size());
  }
  for (size_t group_index = can_direct_emit ? total_out_groups : 0;
       group_index < total_out_groups; ++group_index) {
    const FusedGroup& group = active_group_at(group_index);
    AggregateResultMap aggregate_results;
    aggregate_results.reserve(fusion->aggregates.size());
    if (raw_aggregates.raw_count_fusion) {
      for (auto& aggregate : fusion->aggregates) {
        aggregate_results.emplace(aggregate, Value(group.raw_count));
      }
    } else if (raw_aggregates.raw_sum_fusion) {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, i));
        aggregate_results.emplace(fusion->aggregates[i], std::move(finished));
      }
    } else {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        AggregateAccumulator& accumulator = states[group.state_offset + i];
        ASSIGN_OR_RETURN(Value, finished, accumulator.TryFinish());
        aggregate_results.emplace(accumulator.expression, std::move(finished));
      }
    }
    const Row* scope_row = group.representative.get();
    if (scope_row == nullptr) {
      Value col_tmp;
      for (size_t c = 0; c < result.schema.ColumnCount(); ++c) {
        eval_rep_row.values_[c] = rep_cell(group, c, &col_tmp);
      }
      scope_row = &eval_rep_row;
    }
    Scope scope{.row = scope_row, .schema = &result.schema, .outer = outer};
    if (fusion->having) {
      ASSIGN_OR_RETURN(Value, keep,
                       TryEvaluate(fusion->having, scope, &aggregate_results,
                                   context, ctes));
      if (!Truthy(keep)) {
        continue;
      }
    }
    Row output_row;
    output_row.values_.reserve(fusion->select_items.size());
    size_t row_bytes = 64;
    for (size_t ci = 0; ci < fusion->select_items.size(); ++ci) {
      ASSIGN_OR_RETURN(Value, value,
                       (TryEvaluate(fusion->select_items[ci], scope,
                                    &aggregate_results, context, ctes)));
      if (ci < fallback_col_tracks.size()) {
        fallback_col_tracks[ci].Observe(value);
      }
      row_bytes += value.type == ValueType::kArray
                       ? EstimateValueBytes(value)
                       : EstimateScalarValueBytes(value);
      output_row.values_.push_back(std::move(value));
    }
    result.AddRowWithKnownBytes(std::move(output_row), row_bytes,
                                flush_granularity);
  }
  if (!can_direct_emit) {
    all_uniform_columns = ColumnTypeTracker::FinalizeColumns(
        std::span<const std::vector<ColumnTypeTracker>>(&fallback_col_tracks,
                                                        1),
        output_columns);
    finalized_output_types = true;
  }
  result.schema = Schema("", std::move(output_columns));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (finalized_output_types) {
    result.uniform_cell_types =
        !result.HasSpill() && all_uniform_columns && !result.rows.empty();
  }
  if (out_columnar != nullptr) {
    const bool non_empty = out_columnar->total_rows > 0;
    result.SetColumnarTable(std::move(out_columnar),
                            all_uniform_columns && non_empty);
  } else if (fusion->columnar_output && !pushed_output && !result.HasSpill()) {
    const bool non_empty = !result.rows.empty();
    std::shared_ptr<ColumnarTable> fallback_col;
    if (non_empty) {
      ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                       BuildColumnarTableFromRows(result.schema, result.rows));
      fallback_col = std::move(built);
    } else {
      fallback_col = std::make_shared<ColumnarTable>();
    }
    if (right_col_owner != nullptr) {
      fallback_col->AppendKeepalive(right_col_owner);
    }
    result.SetColumnarTable(std::move(fallback_col),
                            all_uniform_columns && non_empty);
  }
  return Status::kSuccess;
}

}  // namespace tinylamb::relational_detail
