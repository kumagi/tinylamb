/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/process.hpp"
#include "common/regex_guard.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/unary_expression.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
bool TryClassifyAggregate(const AggregateExpression* agg,
                          const Schema& qualified_schema,
                          std::span<const slot_t> proj_to_full,
                          const Schema& table_schema, ColAggSpec* out) {
  if (agg == nullptr || agg->WhereFilter() ||
      agg->Having() != AggregateHavingModifier::kNone ||
      agg->HavingCondition() != nullptr || !agg->InnerOrderBy().empty() ||
      agg->InnerLimit().has_value() || agg->SecondaryArg() ||
      !agg->TrailingArgs().empty()) {
    return false;
  }
  out->expr = agg;
  out->needs_bytes = false;
  out->needs_lengths_only = false;

  const AggregationType at = agg->GetType();
  const bool distinct = agg->Distinct();
  const Expression& child = agg->Child();

  // COUNT(*) or COUNT(constant)
  if (at == AggregationType::kCount && !distinct) {
    if (!child ||
        (child->Type() == TypeTag::kConstantValue &&
         !child->AsConstantValue().GetValue().IsNull()) ||
        (child->Type() == TypeTag::kColumnValue &&
         child->AsColumnValue().GetColumnName().name == "*")) {
      out->kind = ColAggKind::kCountStar;
      return true;
    }
  }
  if (!child) {
    return false;
  }

  // Recognize optional `CAST(BYTE_LENGTH(col) AS FLOAT64)` or
  // `BYTE_LENGTH(col)`.
  Expression inner = child;
  if (inner->Type() == TypeTag::kCastExp &&
      inner->AsCastExpression().TargetValueType() == ValueType::kDouble) {
    inner = inner->AsCastExpression().Child();
  }
  if (!distinct && inner && inner->Type() == TypeTag::kFunctionCallExp) {
    const auto& fn = inner->AsFunctionCallExpression();
    if (IdentifierEquals(fn.FuncName(), "byte_length") &&
        fn.Args().size() == 1 && fn.Args()[0] &&
        fn.Args()[0]->Type() == TypeTag::kColumnValue) {
      const auto slot = LocalColumnOffset(
          qualified_schema, fn.Args()[0]->AsColumnValue().GetColumnName());
      if (slot && *slot < qualified_schema.ColumnCount() &&
          qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar) {
        // Lean proof:
        // lean/TinylambOpt/ResearchProofs/VarcharOffsetOnlyLengthSum.lean
        // (sound)
        out->full_col = MapSlot(*slot, proj_to_full);
        out->needs_lengths_only = true;
        switch (at) {
          case AggregationType::kCount:
            out->kind = ColAggKind::kCountByteLen;
            return true;
          case AggregationType::kSum:
            out->kind = ColAggKind::kSumByteLen;
            return true;
          case AggregationType::kAvg:
            out->kind = ColAggKind::kAvgByteLen;
            return true;
          case AggregationType::kMin:
            out->kind = ColAggKind::kMinByteLen;
            return true;
          case AggregationType::kMax:
            out->kind = ColAggKind::kMaxByteLen;
            return true;
          default:
            return false;
        }
      }
    }
  }

  // Unwrap identity or numeric widening cast to FLOAT64 for SUM/AVG(CAST(int AS
  // FLOAT64)).
  Expression leaf = child;
  if (at == AggregationType::kAvg && leaf->Type() == TypeTag::kCastExp &&
      leaf->AsCastExpression().TargetValueType() == ValueType::kDouble) {
    leaf = leaf->AsCastExpression().Child();
  }
  if (!distinct &&
      (at == AggregationType::kSum || at == AggregationType::kAvg) && leaf &&
      leaf->Type() != TypeTag::kColumnValue &&
      leaf->ResultType(qualified_schema).GetType() == TypeTag::kDouble) {
    auto arith = std::make_shared<ColDoubleArithExpr>();
    size_t max_depth = 0;
    const std::function<bool(const Expression&, size_t)> compile_rpn =
        [&](const Expression& e, size_t depth) -> bool {
      if (!e || depth > 24) {
        return false;
      }
      max_depth = std::max(max_depth, depth);
      if (e->Type() == TypeTag::kConstantValue) {
        const Value& cv = e->AsConstantValue().GetValue();
        if (cv.IsNull() || cv.IsUnsigned()) {
          return false;
        }
        if (cv.type == ValueType::kDouble) {
          arith->rpn.push_back({.op = ColDoubleArithExpr::Op::kConst,
                                .constant = cv.value.double_value});
          return true;
        }
        if (cv.type == ValueType::kInt64) {
          arith->rpn.push_back(
              {.op = ColDoubleArithExpr::Op::kConst,
               .constant = static_cast<double>(cv.value.int_value)});
          return true;
        }
        return false;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        const auto s = LocalColumnOffset(qualified_schema,
                                         e->AsColumnValue().GetColumnName());
        if (!s || *s >= qualified_schema.ColumnCount()) {
          return false;
        }
        const Column& c = qualified_schema.GetColumn(*s);
        if (c.IsUnsigned() || c.IsInterval()) {
          return false;
        }
        const slot_t fc = MapSlot(*s, proj_to_full);
        const ValueType vt = table_schema.GetColumn(fc).Type();
        if (vt != ValueType::kDouble && vt != ValueType::kInt64) {
          return false;
        }
        if (std::ranges::find(arith->referenced_cols, fc) ==
            arith->referenced_cols.end()) {
          arith->referenced_cols.push_back(fc);
        }
        arith->rpn.push_back({.op = vt == ValueType::kDouble
                                        ? ColDoubleArithExpr::Op::kColDouble
                                        : ColDoubleArithExpr::Op::kColInt64,
                              .full_col = fc});
        return true;
      }
      if (e->Type() == TypeTag::kCastExp &&
          e->AsCastExpression().TargetValueType() == ValueType::kDouble &&
          !e->AsCastExpression().ReturnNullOnError()) {
        return compile_rpn(e->AsCastExpression().Child(), depth + 1);
      }
      if (e->Type() == TypeTag::kUnaryExp &&
          e->AsUnaryExpression().Op() == UnaryOperation::kMinus &&
          e->ResultType(qualified_schema).GetType() == TypeTag::kDouble) {
        if (!compile_rpn(e->AsUnaryExpression().Child(), depth + 1)) {
          return false;
        }
        arith->rpn.push_back({.op = ColDoubleArithExpr::Op::kNeg});
        return true;
      }
      if (e->Type() == TypeTag::kBinaryExp &&
          e->ResultType(qualified_schema).GetType() == TypeTag::kDouble) {
        const auto& bin = e->AsBinaryExpression();
        ColDoubleArithExpr::Op rop = ColDoubleArithExpr::Op::kAdd;
        switch (bin.Op()) {
          case BinaryOperation::kAdd:
            rop = ColDoubleArithExpr::Op::kAdd;
            break;
          case BinaryOperation::kSubtract:
            rop = ColDoubleArithExpr::Op::kSub;
            break;
          case BinaryOperation::kMultiply:
            rop = ColDoubleArithExpr::Op::kMul;
            break;
          default:
            return false;
        }
        if (!compile_rpn(bin.Left(), depth + 1) ||
            !compile_rpn(bin.Right(), depth + 1)) {
          return false;
        }
        arith->rpn.push_back({.op = rop});
        return true;
      }
      return false;
    };
    if (compile_rpn(leaf, 1) && !arith->referenced_cols.empty() &&
        arith->rpn.size() <= 30) {
      out->kind = at == AggregationType::kSum ? ColAggKind::kSumDouble
                                              : ColAggKind::kAvgDouble;
      out->full_col = arith->referenced_cols[0];
      out->double_arith = std::move(arith);
      return true;
    }
  }
  if (!leaf || leaf->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const auto slot = LocalColumnOffset(qualified_schema,
                                      leaf->AsColumnValue().GetColumnName());
  if (!slot || *slot >= qualified_schema.ColumnCount()) {
    return false;
  }
  const Column& col = qualified_schema.GetColumn(*slot);
  if (col.IsUnsigned() || col.IsInterval()) {
    return false;
  }
  out->full_col = MapSlot(*slot, proj_to_full);
  const ValueType vt = table_schema.GetColumn(out->full_col).Type();

  if (distinct) {
    if (at != AggregationType::kCount) {
      return false;
    }
    if (vt == ValueType::kInt64) {
      out->kind = ColAggKind::kCountDistinctInt64;
      return true;
    }
    if (vt == ValueType::kDate) {
      out->kind = ColAggKind::kCountDistinctDate;
      return true;
    }
    if (vt == ValueType::kDouble) {
      out->kind = ColAggKind::kCountDistinctDouble;
      return true;
    }
    if (vt == ValueType::kVarChar) {
      out->kind = ColAggKind::kCountDistinctVarChar;
      out->needs_bytes = true;
      return true;
    }
    return false;
  }

  switch (at) {
    case AggregationType::kCount:
      out->kind = ColAggKind::kCountCol;
      out->needs_lengths_only = (vt == ValueType::kVarChar);
      return true;
    case AggregationType::kSum:
      if (vt == ValueType::kInt64) {
        out->kind = ColAggKind::kSumInt64;
        return true;
      }
      if (vt == ValueType::kDouble) {
        out->kind = ColAggKind::kSumDouble;
        return true;
      }
      return false;
    case AggregationType::kAvg:
      if (vt == ValueType::kInt64 || vt == ValueType::kDouble) {
        out->kind = ColAggKind::kAvgDouble;
        return true;
      }
      return false;
    case AggregationType::kMin:
      if (vt == ValueType::kInt64) {
        out->kind = ColAggKind::kMinInt64;
        return true;
      }
      if (vt == ValueType::kDate) {
        out->kind = ColAggKind::kMinDate;
        return true;
      }
      if (vt == ValueType::kDouble) {
        out->kind = ColAggKind::kMinDouble;
        return true;
      }
      if (vt == ValueType::kVarChar) {
        out->kind = ColAggKind::kMinVarChar;
        out->needs_bytes = true;
        return true;
      }
      return false;
    case AggregationType::kMax:
      if (vt == ValueType::kInt64) {
        out->kind = ColAggKind::kMaxInt64;
        return true;
      }
      if (vt == ValueType::kDate) {
        out->kind = ColAggKind::kMaxDate;
        return true;
      }
      if (vt == ValueType::kDouble) {
        out->kind = ColAggKind::kMaxDouble;
        return true;
      }
      if (vt == ValueType::kVarChar) {
        out->kind = ColAggKind::kMaxVarChar;
        out->needs_bytes = true;
        return true;
      }
      return false;
    default:
      return false;
  }
}

bool TryClassifyGroupKey(const Expression& expr, const Schema& qualified_schema,
                         std::span<const slot_t> proj_to_full,
                         const Schema& table_schema, ColGroupKeySpec* out) {
  if (!expr || ContainsParameterSlot(expr) || ContainsQuery(expr) ||
      ContainsVolatileCall(expr)) {
    return false;
  }
  out->original_expr_str = expr->ToString();

  if (expr->Type() == TypeTag::kConstantValue) {
    const Value& v = expr->AsConstantValue().GetValue();
    if (!v.IsNull() && !v.IsUnsigned() && v.type == ValueType::kInt64) {
      out->kind = ColKeyKind::kConstInt64;
      out->const_int = v.value.int_value;
      return true;
    }
    return false;
  }

  if (expr->Type() == TypeTag::kColumnValue) {
    const auto slot = LocalColumnOffset(qualified_schema,
                                        expr->AsColumnValue().GetColumnName());
    if (!slot || *slot >= qualified_schema.ColumnCount()) {
      return false;
    }
    const Column& col = qualified_schema.GetColumn(*slot);
    if (col.IsUnsigned() || col.IsInterval()) {
      return false;
    }
    out->full_col = MapSlot(*slot, proj_to_full);
    switch (table_schema.GetColumn(out->full_col).Type()) {
      case ValueType::kInt64:
        out->kind = ColKeyKind::kColInt64;
        return true;
      case ValueType::kDate:
        out->kind = ColKeyKind::kColDate;
        return true;
      case ValueType::kDouble:
        out->kind = ColKeyKind::kColDouble;
        return true;
      case ValueType::kVarChar:
        out->kind = ColKeyKind::kColVarChar;
        return true;
      default:
        return false;
    }
  }

  if (expr->Type() == TypeTag::kBinaryExp) {
    const auto& bin = expr->AsBinaryExpression();
    if (bin.Op() == BinaryOperation::kAdd ||
        bin.Op() == BinaryOperation::kSubtract) {
      Expression col_e;
      Expression const_e;
      bool negate = false;
      if (bin.Left() && bin.Left()->Type() == TypeTag::kColumnValue &&
          bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
        col_e = bin.Left();
        const_e = bin.Right();
        negate = (bin.Op() == BinaryOperation::kSubtract);
      } else if (bin.Op() == BinaryOperation::kAdd && bin.Left() &&
                 bin.Left()->Type() == TypeTag::kConstantValue && bin.Right() &&
                 bin.Right()->Type() == TypeTag::kColumnValue) {
        col_e = bin.Right();
        const_e = bin.Left();
      }
      if (col_e && const_e) {
        const Value& cv = const_e->AsConstantValue().GetValue();
        if (!cv.IsNull() && !cv.IsUnsigned() && cv.type == ValueType::kInt64 &&
            (!negate ||
             cv.value.int_value != std::numeric_limits<int64_t>::min())) {
          const auto slot = LocalColumnOffset(
              qualified_schema, col_e->AsColumnValue().GetColumnName());
          if (slot && *slot < qualified_schema.ColumnCount()) {
            const Column& col = qualified_schema.GetColumn(*slot);
            if (!col.IsUnsigned() && !col.IsInterval()) {
              out->full_col = MapSlot(*slot, proj_to_full);
              if (table_schema.GetColumn(out->full_col).Type() ==
                  ValueType::kInt64) {
                out->kind = ColKeyKind::kColInt64Delta;
                out->const_int =
                    negate ? -cv.value.int_value : cv.value.int_value;
                return true;
              }
            }
          }
        }
      }
    }
  }

  if (expr->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = expr->AsFunctionCallExpression();
    if (const auto unit = CompileUtcTimestampTrunc(call)) {
      if (call.Args()[0] && call.Args()[0]->Type() == TypeTag::kColumnValue) {
        const auto slot = LocalColumnOffset(
            qualified_schema, call.Args()[0]->AsColumnValue().GetColumnName());
        if (slot && *slot < qualified_schema.ColumnCount() &&
            qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar) {
          out->kind = ColKeyKind::kUtcTimestampTrunc;
          out->full_col = MapSlot(*slot, proj_to_full);
          out->trunc_unit = *unit;
          return true;
        }
      }
      return false;
    }
    if (const auto unit = CompileUtcTimestampExtract(call)) {
      if (call.Args()[0] && call.Args()[0]->Type() == TypeTag::kColumnValue) {
        const auto slot = LocalColumnOffset(
            qualified_schema, call.Args()[0]->AsColumnValue().GetColumnName());
        if (slot && *slot < qualified_schema.ColumnCount() &&
            qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar) {
          out->kind = ColKeyKind::kUtcTimestampExtract;
          out->full_col = MapSlot(*slot, proj_to_full);
          out->trunc_unit = *unit;
          return true;
        }
      }
      return false;
    }
    if (IdentifierEquals(call.FuncName(), "regexp_replace") &&
        call.Args().size() == 3 && call.Args()[0] &&
        call.Args()[0]->Type() == TypeTag::kColumnValue && call.Args()[1] &&
        call.Args()[1]->Type() == TypeTag::kConstantValue && call.Args()[2] &&
        call.Args()[2]->Type() == TypeTag::kConstantValue) {
      const auto slot = LocalColumnOffset(
          qualified_schema, call.Args()[0]->AsColumnValue().GetColumnName());
      const Value& pat = call.Args()[1]->AsConstantValue().GetValue();
      const Value& rep = call.Args()[2]->AsConstantValue().GetValue();
      if (slot && *slot < qualified_schema.ColumnCount() &&
          qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar &&
          pat.type == ValueType::kVarChar && !pat.IsCaseInsensitive() &&
          rep.type == ValueType::kVarChar && !rep.IsCaseInsensitive()) {
        std::string scratch;
        auto validate = ReplaceCheckedRegexView(
            "", pat.value.varchar_value, rep.value.varchar_value, &scratch);
        if (!validate.HasValue()) {
          return false;
        }
        out->kind = ColKeyKind::kRegexReplaceVarChar;
        out->full_col = MapSlot(*slot, proj_to_full);
        out->regex_pattern = std::string(pat.value.varchar_value);
        out->regex_replacement = std::string(rep.value.varchar_value);
        return true;
      }
    }
    return false;
  }

  if (expr->Type() == TypeTag::kCaseExp) {
    const auto& ce = expr->AsCaseExpression();
    if (ce.when_clauses_.size() != 1 || !ce.when_clauses_[0].first ||
        !ce.when_clauses_[0].second || !ce.else_clause_ ||
        ce.else_clause_->Type() != TypeTag::kConstantValue ||
        ce.when_clauses_[0].second->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const Value& else_v = ce.else_clause_->AsConstantValue().GetValue();
    if (else_v.IsCaseInsensitive() ||
        (!else_v.IsNull() && (else_v.type != ValueType::kVarChar ||
                              !PlainStringComparable(else_v.AsStringView())))) {
      return false;
    }
    const auto slot = LocalColumnOffset(
        qualified_schema,
        ce.when_clauses_[0].second->AsColumnValue().GetColumnName());
    if (!slot || *slot >= qualified_schema.ColumnCount() ||
        qualified_schema.GetColumn(*slot).Type() != ValueType::kVarChar) {
      return false;
    }
    std::vector<TypedFilterCompare> conds;
    for (const Expression& conj : SplitConjuncts(ce.when_clauses_[0].first)) {
      auto cmp = TryCompileTypedFilterCompare(conj, qualified_schema, true);
      if (!cmp || !cmp->constant_payload || cmp->is_double ||
          cmp->string_payload) {
        return false;
      }
      cmp->left = MapSlot(cmp->left, proj_to_full);
      conds.push_back(std::move(*cmp));
    }
    out->kind = ColKeyKind::kCaseWhenVarChar;
    out->full_col = MapSlot(*slot, proj_to_full);
    out->case_conditions = std::move(conds);
    out->case_else_is_null = else_v.IsNull();
    if (!else_v.IsNull()) {
      out->case_else_str = std::string(else_v.AsStringView());
    }
    return true;
  }
  return false;
}

StatusOr<Value> MaterializeKeyCellValue(const ColGroupKeySpec& spec,
                                        const RuntimeKeyCell& cell) {
  if (cell.is_null) {
    return Value();
  }
  switch (spec.kind) {
    case ColKeyKind::kColInt64:
    case ColKeyKind::kConstInt64:
    case ColKeyKind::kColInt64Delta:
    case ColKeyKind::kUtcTimestampExtract:
      return Value(cell.int_val);
    case ColKeyKind::kColDate:
      return Value::DateFromDays(cell.int_val);
    case ColKeyKind::kColDouble:
      return Value(cell.double_val);
    case ColKeyKind::kUtcTimestampTrunc:
      return FormatUtcTimestampTruncKey(cell.int_val);
    case ColKeyKind::kColVarChar:
    case ColKeyKind::kCaseWhenVarChar:
    case ColKeyKind::kRegexReplaceVarChar:
      return cell.str_len == 0 ? Value("")
                               : Value(std::string(cell.str_ptr, cell.str_len));
  }
  return Value();
}

StatusOr<Value> EvaluateBindingValue(
    const OutputSlotBinding& b, const std::vector<ColGroupKeySpec>& key_specs,
    const RuntimeKeyCell* keys, const std::vector<Value>& finished_aggs,
    std::vector<Value>* cached_key_values) {
  switch (b.kind) {
    case OutputSlotBinding::Kind::kConstValue:
      return b.constant;
    case OutputSlotBinding::Kind::kAgg:
      return finished_aggs[b.index];
    case OutputSlotBinding::Kind::kKey: {
      Value& cached = (*cached_key_values)[b.index];
      // NOLINTNEXTLINE(clang-analyzer-core.NullDereference) intentional pattern
      // in this code
      if (!cached.IsNull() ||
          keys[b.index]    // NOLINT(clang-analyzer-core.NullDereference)
              .is_null) {  // NOLINT(clang-analyzer-core.NullDereference)
        // NOLINTNEXTLINE(clang-analyzer-core.NullDereference) intentional
        // pattern in this code
        if (keys[b.index]    // NOLINT(clang-analyzer-core.NullDereference)
                .is_null) {  // NOLINT(clang-analyzer-core.NullDereference)
          return Value();
        }
        return cached;
      }
      ASSIGN_OR_RETURN(
          Value, kv,
          MaterializeKeyCellValue(key_specs[b.index], keys[b.index]));
      cached = std::move(kv);
      return cached;
    }
    case OutputSlotBinding::Kind::kPostAgg: {
      Row arg_row;
      arg_row.values_.reserve(b.post_agg->slots.size());
      for (const auto& s : b.post_agg->slots) {
        if (s.is_key) {
          ASSIGN_OR_RETURN(
              Value, kv,
              // NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)
              // intentional pattern in this code
              MaterializeKeyCellValue(  // NOLINT(clang-analyzer-core.NonNullParamChecker)
                  key_specs[s.index],
                  keys[s.index]));  // NOLINT(clang-analyzer-core.NonNullParamChecker)
          arg_row.values_.push_back(std::move(kv));
        } else {
          arg_row.values_.push_back(finished_aggs[s.index]);
        }
      }
      if (b.post_agg->program.has_value()) {
        return b.post_agg->program->TryEvaluateRow(arg_row);
      }
      return b.post_agg->expr->TryEvaluate(arg_row, b.post_agg->eval_schema);
    }
  }
  return Value();
}

StatusOr<bool> EvalPositionalHavingOnStates(
    const std::vector<PositionalHaving>& clauses,
    const std::vector<ColAggSpec>& agg_specs, const RuntimeAggState* ast,
    std::vector<Value>* finished_scratch) {
  for (const PositionalHaving& clause : clauses) {
    if (!clause.rhs_is_agg && clause.has_constant &&
        clause.constant.type == ValueType::kInt64 &&
        (agg_specs[clause.agg_index].kind == ColAggKind::kCountStar ||
         agg_specs[clause.agg_index].kind == ColAggKind::kCountCol ||
         agg_specs[clause.agg_index].kind == ColAggKind::kCountByteLen)) {
      const int64_t lv = ast[clause.agg_index].count;
      const int64_t rv = clause.constant.value.int_value;
      bool ok = true;
      switch (clause.op) {
        case BinaryOperation::kEquals:
          ok = lv == rv;
          break;
        case BinaryOperation::kNotEquals:
          ok = lv != rv;
          break;
        case BinaryOperation::kLessThan:
          ok = lv < rv;
          break;
        case BinaryOperation::kLessThanEquals:
          ok = lv <= rv;
          break;
        case BinaryOperation::kGreaterThan:
          ok = lv > rv;
          break;
        case BinaryOperation::kGreaterThanEquals:
          ok = lv >= rv;
          break;
        default:
          ok = true;
          break;
      }
      if (!ok) {
        return false;
      }
      continue;
    }
    ASSIGN_OR_RETURN(
        Value, lhs_value,
        (FinishAggState(agg_specs[clause.agg_index], ast[clause.agg_index])));
    (*finished_scratch)[clause.agg_index] = std::move(lhs_value);
    if (clause.rhs_is_agg) {
      ASSIGN_OR_RETURN(Value, rhs_value,
                       (FinishAggState(agg_specs[clause.rhs_agg_index],
                                       ast[clause.rhs_agg_index])));
      (*finished_scratch)[clause.rhs_agg_index] = std::move(rhs_value);
    }
    const Value* rhs = clause.rhs_is_agg
                           ? &(*finished_scratch)[clause.rhs_agg_index]
                           : nullptr;
    Status err = Status::kSuccess;
    const bool passes = EvalPositionalHavingValues(
        clause, (*finished_scratch)[clause.agg_index], rhs, &err);
    RETURN_IF_FAIL(err);
    if (!passes) {
      return false;
    }
  }
  return true;
}

ColumnarStageSchedule BuildColumnarStageSchedule(
    const Schema& table_schema, const ColumnarPredicateSet& preds,
    const std::vector<ColGroupKeySpec>& key_specs,
    const std::vector<ColAggSpec>& agg_specs) {
  const size_t ncols = table_schema.ColumnCount();
  std::vector<uint8_t> in_stage1a(ncols, 0);
  ColumnarStageSchedule sched;
  for (const auto& pc : preds.contains_prechecks) {
    if (in_stage1a[pc.full_col] == 0U) {
      in_stage1a[pc.full_col] = 1;
      sched.stage1a_cols.push_back(pc.full_col);
    }
  }
  std::vector<uint8_t> in_stage1b(ncols, 0);
  std::vector<uint8_t> s1b_bytes(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    if (c < preds.col_referenced.size() && preds.col_referenced[c] != 0U &&
        in_stage1a[c] == 0U) {
      in_stage1b[c] = 1;
      if (c < preds.col_needs_varchar_bytes.size() &&
          preds.col_needs_varchar_bytes[c] != 0U) {
        s1b_bytes[c] = 1;
      }
    }
  }
  std::vector<uint8_t> payload_needed(ncols, 0);
  std::vector<uint8_t> payload_bytes(ncols, 0);
  std::vector<uint32_t> non_case_refs(ncols, 0);
  std::vector<uint32_t> case_target_refs(ncols, 0);
  std::vector<size_t> case_key_owner(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    if (in_stage1a[c] != 0U || in_stage1b[c] != 0U) {
      ++non_case_refs[c];
    }
  }
  for (size_t ki = 0; ki < key_specs.size(); ++ki) {
    const auto& ks = key_specs[ki];
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    payload_needed[ks.full_col] = 1;
    if (table_schema.GetColumn(ks.full_col).Type() == ValueType::kVarChar) {
      payload_bytes[ks.full_col] = 1;
    }
    if (ks.kind == ColKeyKind::kCaseWhenVarChar &&
        !ks.case_conditions.empty()) {
      ++case_target_refs[ks.full_col];
      case_key_owner[ks.full_col] = ki;
    } else {
      ++non_case_refs[ks.full_col];
    }
    for (const auto& cc : ks.case_conditions) {
      payload_needed[cc.left] = 1;
      ++non_case_refs[cc.left];
      if (!cc.constant_payload) {
        payload_needed[cc.right] = 1;
        ++non_case_refs[cc.right];
      }
    }
  }
  for (const auto& as : agg_specs) {
    if (as.kind == ColAggKind::kCountStar) {
      continue;
    }
    if (as.double_arith != nullptr) {
      for (const slot_t fc : as.double_arith->referenced_cols) {
        payload_needed[fc] = 1;
        ++non_case_refs[fc];
      }
      continue;
    }
    payload_needed[as.full_col] = 1;
    ++non_case_refs[as.full_col];
    if (as.needs_bytes) {
      payload_bytes[as.full_col] = 1;
    }
  }
  // Lean proof: lean/TinylambOpt/GroupKeyAndDictionaryRewrites.lean
  // (conditional_key_disjoint_partition_sound): isolate VARCHAR columns
  // referenced exclusively by a single `CASE WHEN` target (e.g. `Referer` in
  // ClickBench Q40) into `cond_cols` so blocks where 0 surviving rows satisfy
  // the guard never read or decompress that column.
  std::vector<uint8_t> is_cond_col(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    if (case_target_refs[c] == 1 && non_case_refs[c] == 0 &&
        table_schema.GetColumn(c).Type() == ValueType::kVarChar) {
      is_cond_col[c] = 1;
      sched.cond_cols.push_back(static_cast<slot_t>(c));
      sched.cond_guards.push_back(key_specs[case_key_owner[c]].case_conditions);
    }
  }
  const bool has_stage1_filters = !sched.stage1a_cols.empty() || !preds.Empty();
  if (!has_stage1_filters) {
    for (size_t c = 0; c < ncols; ++c) {
      if (is_cond_col[c] != 0U) {
        continue;
      }
      if (in_stage1b[c] != 0U || payload_needed[c] != 0U) {
        sched.stage1b_cols.push_back(static_cast<slot_t>(c));
        const bool need_b = s1b_bytes[c] != 0U || payload_bytes[c] != 0U;
        sched.stage1b_len_only.push_back(need_b ? 0 : 1);
      }
    }
  } else {
    for (size_t c = 0; c < ncols; ++c) {
      if (in_stage1b[c] != 0U) {
        if (!sched.stage1a_cols.empty() && s1b_bytes[c] != 0U) {
          sched.stage1c_cols.push_back(static_cast<slot_t>(c));
        } else {
          sched.stage1b_cols.push_back(static_cast<slot_t>(c));
          sched.stage1b_len_only.push_back(
              (s1b_bytes[c] != 0U || payload_bytes[c] != 0U) ? 0 : 1);
        }
      }
    }
    for (size_t c = 0; c < ncols; ++c) {
      if (in_stage1a[c] == 0U && in_stage1b[c] == 0U && is_cond_col[c] == 0U &&
          payload_needed[c] != 0U) {
        sched.stage2_cols.push_back(static_cast<slot_t>(c));
        sched.stage2_len_only.push_back(payload_bytes[c] != 0U ? 0 : 1);
      }
    }
  }
  sched.col_in_payload = std::move(payload_needed);
  return sched;
}

StatusOr<bool> ValidateDeltaKeysNoOverflow(
    const TlcsFile& tlcs, const std::vector<uint32_t>& candidate_blocks,
    const std::vector<ColGroupKeySpec>& key_specs) {
  std::vector<std::pair<slot_t, int64_t>> checks;
  checks.reserve(key_specs.size());
  for (const auto& ks : key_specs) {
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      checks.emplace_back(ks.full_col, ks.const_int);
    }
  }
  if (checks.empty()) {
    return true;
  }
  const uint32_t dg_count = tlcs.DirGroupCount();
  const uint32_t dg_size = tlcs.DirGroupSize();
  std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
  for (uint32_t g = 0; g < dg_count; ++g) {
    ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                     tlcs.GetDirGroup(g));
    dir_groups[g] = std::move(grp);
  }
  for (const uint32_t bidx : candidate_blocks) {
    const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
    for (const auto& [col, delta] : checks) {
      const auto& meta = blk.columns[col];
      if (meta.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (!meta.has_min_max) {
        return false;
      }
      int64_t tmp = 0;
      if (__builtin_add_overflow(meta.min_int, delta, &tmp) ||
          __builtin_add_overflow(meta.max_int, delta, &tmp)) {
        return false;
      }
    }
  }
  return true;
}

// Lean proof: lean/TinylambOpt/GroupKeyAndDictionaryRewrites.lean
// (group_by_functional_expr_reduction_sound, group_by_overflow_expr_drop_bug)
StatusOr<KeyProjectionMap> BuildKeyProjectionMap(
    const TlcsFile& tlcs, const std::vector<uint32_t>& candidate_blocks,
    const std::vector<ColGroupKeySpec>& key_specs) {
  KeyProjectionMap km;
  km.full_to_phys.assign(key_specs.size(), -1);
  km.delta_base_phys.assign(key_specs.size(), -1);

  bool any_delta = false;
  for (const auto& ks : key_specs) {
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      any_delta = true;
      break;
    }
  }
  bool delta_safe = false;
  if (any_delta) {
    ASSIGN_OR_RETURN(
        bool, ok,
        ValidateDeltaKeysNoOverflow(tlcs, candidate_blocks, key_specs));
    delta_safe = ok;
  }

  std::vector<std::pair<slot_t, int32_t>> int64_bases;
  int64_bases.reserve(key_specs.size());
  for (size_t i = 0; i < key_specs.size(); ++i) {
    const auto& ks = key_specs[i];
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    if (ks.kind == ColKeyKind::kColInt64) {
      const auto p = static_cast<int32_t>(km.phys_specs.size());
      km.phys_specs.push_back(ks);
      km.full_to_phys[i] = p;
      int64_bases.emplace_back(ks.full_col, p);
    }
  }
  if (int64_bases.empty() || !delta_safe) {
    km.phys_specs.clear();
    std::fill(km.full_to_phys.begin(), km.full_to_phys.end(), -1);
    for (size_t i = 0; i < key_specs.size(); ++i) {
      if (key_specs[i].kind == ColKeyKind::kConstInt64) {
        continue;
      }
      km.full_to_phys[i] = static_cast<int32_t>(km.phys_specs.size());
      km.phys_specs.push_back(key_specs[i]);
    }
    return km;
  }

  for (size_t i = 0; i < key_specs.size(); ++i) {
    const auto& ks = key_specs[i];
    if (ks.kind == ColKeyKind::kConstInt64 || km.full_to_phys[i] >= 0) {
      continue;
    }
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      int32_t base_p = -1;
      for (const auto& [col, p] : int64_bases) {
        if (col == ks.full_col) {
          base_p = p;
          break;
        }
      }
      if (base_p >= 0) {
        km.delta_base_phys[i] = base_p;
        continue;
      }
    }
    km.full_to_phys[i] = static_cast<int32_t>(km.phys_specs.size());
    km.phys_specs.push_back(ks);
  }
  return km;
}

bool BindExpressionToOutputSlot(const Expression& expr,
                                const std::vector<ColGroupKeySpec>& key_specs,
                                const std::vector<ColAggSpec>& agg_specs,
                                const std::vector<NamedExpression>* select_list,
                                OutputSlotBinding* out) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    out->kind = OutputSlotBinding::Kind::kConstValue;
    out->constant = expr->AsConstantValue().GetValue();
    return true;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression* ptr = &expr->AsAggregateExpression();
    for (size_t i = 0; i < agg_specs.size(); ++i) {
      if (agg_specs[i].expr == ptr) {
        out->kind = OutputSlotBinding::Kind::kAgg;
        out->index = i;
        return true;
      }
    }
    const std::string s = expr->ToString();
    for (size_t i = 0; i < agg_specs.size(); ++i) {
      if (agg_specs[i].expr->ToString() == s) {
        out->kind = OutputSlotBinding::Kind::kAgg;
        out->index = i;
        return true;
      }
    }
    return false;
  }
  const std::string expr_str = expr->ToString();
  for (size_t i = 0; i < key_specs.size(); ++i) {
    if (key_specs[i].original_expr_str == expr_str) {
      out->kind = OutputSlotBinding::Kind::kKey;
      out->index = i;
      return true;
    }
  }
  if (select_list != nullptr && expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = expr->AsColumnValue().GetColumnName();
    if (cn.schema.empty()) {
      for (const NamedExpression& se : *select_list) {
        if (!se.name.empty() && IdentifierEquals(se.name, cn.name)) {
          return BindExpressionToOutputSlot(se.expression, key_specs, agg_specs,
                                            nullptr, out);
        }
      }
    }
  }
  return false;
}

// Lean proof: TinylambOpt.TlcsBoundaryAggregate. The file's boundary
// frequencies are exact. The reservoir's NDV, histogram and MCV are estimates.
StatusOr<std::optional<Relation>> TryFoldTlcsBoundaryAggregate(
    const TlcsFile& tlcs, const SelectStatement& stmt, const Schema& schema,
    const Schema& qualified_schema, const ColumnarPredicateSet& preds,
    const std::vector<ColGroupKeySpec>& keys,
    const std::vector<ColAggSpec>& aggregates,
    const std::vector<OutputSlotBinding>& emit,
    const std::vector<ColumnarOrderBinding>& order,
    std::span<const NamedExpression> output_names, ExecutionRuntime* runtime) {
  if (tlcs.TotalRows() > static_cast<uint64_t>(INT64_MAX) ||
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TLCS_BOUNDARY_AGG") !=
          nullptr) {
    return std::nullopt;
  }
  const bool extrema =
      preds.Empty() && keys.empty() && !aggregates.empty() &&
      std::ranges::all_of(aggregates, [](const ColAggSpec& agg) {
        return agg.kind == ColAggKind::kMinInt64 ||
               agg.kind == ColAggKind::kMaxInt64 ||
               agg.kind == ColAggKind::kMinDate ||
               agg.kind == ColAggKind::kMaxDate ||
               agg.kind == ColAggKind::kCountCol ||
               agg.kind == ColAggKind::kCountStar;
      });
  const bool grouped_count = keys.size() == 1 && preds.Empty();
  const bool frequency =
      keys.size() <= 1 && aggregates.size() == 1 &&
      aggregates[0].kind == ColAggKind::kCountStar &&
      (grouped_count || (preds.compares.size() == 1 && preds.nulls.empty() &&
                         preds.inlists.empty()));
  if (!extrema && !frequency) {
    return std::nullopt;
  }
  // DecodeTlcsColumnStats' result is not discarded: a decode failure makes
  // the optional empty and the fast path below is skipped immediately.
  auto stats =
      DecodeTlcsColumnStats(tlcs.SerializedColumnStats(), schema.ColumnCount());
  if (!stats) {
    return std::nullopt;
  }
  const auto valid_stats = [&](slot_t slot) {
    const auto& s = (*stats)[slot];
    const auto& col = schema.GetColumn(slot);
    return s.Type() == col.Type() && !col.IsUnsigned() &&
           (col.Type() == ValueType::kDate ||
            col.Type() == ValueType::kInt64) &&
           s.Count() <= tlcs.TotalRows() &&
           s.NullCount() == tlcs.TotalRows() - s.Count();
  };
  struct BoundRow {
    RuntimeKeyCell key;
    std::vector<Value> aggs;
    std::vector<Value> order_values;
  };
  std::vector<BoundRow> rows;
  if (extrema) {
    rows.resize(1);
    rows[0].aggs.reserve(aggregates.size());
    for (const auto& agg : aggregates) {
      if (agg.kind == ColAggKind::kCountStar) {
        rows[0].aggs.emplace_back(static_cast<int64_t>(tlcs.TotalRows()));
        continue;
      }
      if (!valid_stats(agg.full_col)) {
        return std::nullopt;
      }
      const auto& s = (*stats)[agg.full_col];
      if (agg.kind == ColAggKind::kCountCol) {
        rows[0].aggs.emplace_back(static_cast<int64_t>(s.Count()));
      } else if (s.Count() == 0) {
        rows[0].aggs.emplace_back();
      } else {
        const bool min = agg.kind == ColAggKind::kMinInt64 ||
                         agg.kind == ColAggKind::kMinDate;
        const auto& boundary = min ? s.LowestValues() : s.HighestValues();
        if (boundary.empty() ||
            std::ranges::any_of(boundary, [&](const auto& item) {
              return item.value.type != s.Type() || item.value.IsUnsigned();
            })) {
          return std::nullopt;
        }
        const auto best = std::ranges::min_element(
            boundary, [min](const auto& a, const auto& b) {
              const int c = CompareForOrderBy(a.value, b.value);
              return min ? c < 0 : c > 0;
            });
        rows[0].aggs.push_back(best->value);
      }
    }
  } else {
    const auto* compare = grouped_count ? nullptr : preds.compares.data();
    const slot_t slot = grouped_count ? keys[0].full_col : compare->left;
    if (!valid_stats(slot) ||
        (compare != nullptr &&
         (!compare->constant_payload || compare->string_payload ||
          compare->is_double || !IsComparison(compare->op))) ||
        (!keys.empty() &&
         (keys[0].full_col != slot || keys[0].kind != ColKeyKind::kColInt64))) {
      return std::nullopt;
    }
    const auto& s = (*stats)[slot];
    std::vector<ValueFrequency> domain;
    domain.reserve(s.LowestValues().size() + s.HighestValues().size());
    size_t covered = 0;
    for (const auto* boundary : {&s.LowestValues(), &s.HighestValues()}) {
      for (const auto& item : *boundary) {
        if (item.value.type != s.Type() || item.value.IsUnsigned()) {
          return std::nullopt;
        }
        const auto found =
            std::ranges::find(domain, item.value, &ValueFrequency::value);
        if (found != domain.end()) {
          if (found->count != item.count) {
            return std::nullopt;
          }
          continue;
        }
        if (item.count == 0 || item.count > s.Count() - covered) {
          return std::nullopt;
        }
        domain.push_back(item);
        covered += item.count;
      }
    }
    const auto raw = [&](const Value& v) -> int64_t {
      return s.Type() == ValueType::kDate ? v.DateDays() : v.AsInt64();
    };
    if (covered != s.Count() && !keys.empty()) {
      using Frequencies = std::shared_ptr<const TlcsFile::IntFrequencies>;
      ASSIGN_OR_RETURN(
          Frequencies, histogram,
          tlcs.GetOrComputeIntFrequencies(slot, schema, grouped_count));
      if (histogram) {
        const auto entries = tlcs.Dictionary(slot).IntEntries();
        domain.clear();
        covered = 0;
        for (size_t i = 0; i < histogram->counts.size(); ++i) {
          const uint64_t count = histogram->counts[i];
          if (count > s.Count() - covered) {
            return std::nullopt;
          }
          if (count > 0) {
            const int64_t value =
                histogram->first_value
                    ? *histogram->first_value + static_cast<int64_t>(i)
                    : entries[i];
            domain.push_back(
                {.value = Value(value), .count = static_cast<size_t>(count)});
          }
          covered += static_cast<size_t>(count);
        }
      }
    }
    if (covered != s.Count()) {
      // Exact equality/inequality needs one known frequency, not a full
      // domain. Its complement is over non-NULL rows only.
      if (compare == nullptr || !keys.empty() ||
          (compare->op != BinaryOperation::kEquals &&
           compare->op != BinaryOperation::kNotEquals)) {
        return std::nullopt;
      }
      const auto it = std::ranges::find_if(domain, [&](const auto& item) {
        return raw(item.value) == compare->int_constant;
      });
      if (it == domain.end()) {
        return std::nullopt;
      }
      const size_t count = compare->op == BinaryOperation::kEquals
                               ? it->count
                               : s.Count() - it->count;
      rows.push_back({.key = {},
                      .aggs = {Value(static_cast<int64_t>(count))},
                      .order_values = {}});
    } else {
      TlcsFile::DecodedColumnChunk chunk;
      chunk.int_cells.reserve(domain.size());
      for (const auto& item : domain) {
        chunk.int_cells.push_back(raw(item.value));
      }
      std::vector<const TlcsFile::DecodedColumnChunk*> columns(
          schema.ColumnCount(), nullptr);
      columns[slot] = &chunk;
      size_t total = 0;
      for (uint32_t i = 0; i < domain.size(); ++i) {
        if (!RowMatchesColPtrs(preds, columns, i)) {
          continue;
        }
        total += domain[i].count;
        if (!keys.empty()) {
          rows.push_back(
              {.key = {.int_val = chunk.int_cells[i]},
               .aggs = {Value(static_cast<int64_t>(domain[i].count))},
               .order_values = {}});
        }
      }
      if (keys.empty()) {
        rows.push_back({.key = {},
                        .aggs = {Value(static_cast<int64_t>(total))},
                        .order_values = {}});
      } else if (grouped_count && s.NullCount() != 0) {
        // Lean: exact_grouped_null_count. COUNT(*) includes NULL key rows.
        rows.push_back({.key = {.is_null = true},
                        .aggs = {Value(static_cast<int64_t>(s.NullCount()))},
                        .order_values = {}});
      }
    }
  }
  for (auto& row : rows) {
    std::vector<Value> cached_keys(keys.size());
    for (const auto& item : order) {
      ASSIGN_OR_RETURN(Value, v,
                       EvaluateBindingValue(item.binding, keys, &row.key,
                                            row.aggs, &cached_keys));
      row.order_values.push_back(std::move(v));
    }
  }
  std::stable_sort(rows.begin(), rows.end(), [&](const auto& a, const auto& b) {
    for (size_t i = 0; i < order.size(); ++i) {
      const int cmp =
          order[i].order.Compare(a.order_values[i], b.order_values[i]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return false;
  });
  const size_t cap =
      stmt.HasLimit() ? stmt.Offset() + stmt.Limit() : rows.size();
  Relation result(runtime);
  std::vector<Column> out_cols;
  out_cols.reserve(emit.size());
  for (size_t i = 0; i < emit.size(); ++i) {
    if (i >= output_names.size()) {
      out_cols.emplace_back("col_" + std::to_string(i), ValueType::kInt64);
      continue;
    }
    const auto& item = output_names[i];
    const auto type = item.expression->ResultType(qualified_schema).GetType();
    out_cols.emplace_back(ProjectionName(item, i), type == TypeTag::kDate
                                                       ? ValueType::kDate
                                                       : ValueType::kInt64);
  }
  for (size_t r = 0; r < std::min(cap, rows.size()); ++r) {
    std::vector<Value> values;
    values.reserve(emit.size());
    std::vector<Value> cached_keys(keys.size());
    for (size_t i = 0; i < emit.size(); ++i) {
      ASSIGN_OR_RETURN(Value, v,
                       EvaluateBindingValue(emit[i], keys, &rows[r].key,
                                            rows[r].aggs, &cached_keys));
      if (!v.IsNull() && result.rows.empty()) {
        out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
        out_cols[i].SetUnsigned(v.IsUnsigned());
      }
      values.push_back(std::move(v));
    }
    result.AddRow(Row(std::move(values)));
  }
  result.schema = Schema("", std::move(out_cols));
  return result;
}

}  // namespace tinylamb::relational_detail
