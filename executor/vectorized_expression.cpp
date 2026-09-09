/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/vectorized_expression.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/selection_vector.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

StatusOr<Value> EvaluateRowFallback(const Expression& expr, const Row& row,
                                    const Schema& schema) {
  return expr->TryEvaluate(row, schema);
}

}  // namespace

StatusOr<ColumnVector> VectorizedExpression::Evaluate(
    const Expression& expr, const Schema& schema, const DataChunk& chunk,
    const SelectionVector* sel) {
  if (!expr) {
    return ColumnVector(ValueType::kNull, 0);
  }

  const size_t total_rows = chunk.Size();
  const size_t active_count = sel != nullptr ? sel->Size() : total_rows;
  if (total_rows == 0 || active_count == 0) {
    return ColumnVector(ValueType::kNull, 0);
  }

  switch (expr->Type()) {
    case TypeTag::kConstantValue: {
      const Value val = expr->AsConstantValue().GetValue();
      ColumnVector result(val.type, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        result.Append(val);
      }
      return result;
    }
    case TypeTag::kColumnValue: {
      const auto& col_val = expr->AsColumnValue();
      const int offset = schema.Offset(col_val.GetColumnName());
      if (offset < 0 || static_cast<size_t>(offset) >= chunk.ColumnCount()) {
        CHECK_MSG(false, "column not found in schema: " +
                             col_val.GetColumnName().ToString());
      }
      const ColumnVector& src = chunk.ColumnAt(static_cast<size_t>(offset));
      if (sel == nullptr) {
        ColumnVector result(src.Type(), total_rows);
        for (size_t i = 0; i < total_rows; ++i) {
          result.AppendFrom(src, i);
        }
        return result;
      }
      ColumnVector result(src.Type(), sel->Size());
      for (size_t i = 0; i < sel->Size(); ++i) {
        result.AppendFrom(src, (*sel)[i]);
      }
      return result;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expr->AsUnaryExpression();
      ASSIGN_OR_RETURN(ColumnVector, child_vec,
                       (Evaluate(unary.Child(), schema, chunk, sel)));
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const Value child_val = child_vec.ValueAt(i);
        ASSIGN_OR_RETURN(Value, unary_value,
                         (TryEvaluateUnary(unary.Op(), child_val)));
        result.Append(unary_value);
      }
      return result;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expr->AsBinaryExpression();
      ASSIGN_OR_RETURN(ColumnVector, left_vec,
                       (Evaluate(binary.Left(), schema, chunk, sel)));
      ASSIGN_OR_RETURN(ColumnVector, right_vec,
                       (Evaluate(binary.Right(), schema, chunk, sel)));
      if (left_vec.Type() == ValueType::kInt64 &&
          right_vec.Type() == ValueType::kInt64) {
        const auto op = binary.Op();
        if (op == BinaryOperation::kAnd || op == BinaryOperation::kOr ||
            op == BinaryOperation::kXor) {
          ColumnVector result(ValueType::kInt64, active_count);
          const auto& l_ints = left_vec.IntegerData();
          const auto& r_ints = right_vec.IntegerData();
          for (size_t i = 0; i < active_count; ++i) {
            const bool l_null = left_vec.IsNull(i);
            const bool r_null = right_vec.IsNull(i);
            const bool lv = !l_null && l_ints[i] != 0;
            const bool rv = !r_null && r_ints[i] != 0;
            // Match EvaluateBinary three-valued logic: FALSE AND NULL is
            // FALSE, TRUE OR NULL is TRUE; XOR with any NULL is NULL.
            if (op == BinaryOperation::kAnd) {
              if ((!l_null && !lv) || (!r_null && !rv)) {
                result.Append(Value(int64_t{0}));
              } else if (l_null || r_null) {
                result.Append(Value());
              } else {
                result.Append(Value(int64_t{1}));
              }
            } else if (op == BinaryOperation::kOr) {
              if ((!l_null && lv) || (!r_null && rv)) {
                result.Append(Value(int64_t{1}));
              } else if (l_null || r_null) {
                result.Append(Value());
              } else {
                result.Append(Value(int64_t{0}));
              }
            } else {
              if (l_null || r_null) {
                result.Append(Value());
              } else {
                result.Append(Value(static_cast<int64_t>(lv ^ rv ? 1 : 0)));
              }
            }
          }
          return result;
        }
      }
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const Value left_val = left_vec.ValueAt(i);
        const Value right_val = right_vec.ValueAt(i);
        ASSIGN_OR_RETURN(Value, bin_value,
                         (TryEvaluateBinary(binary.Op(), left_val, right_val)));
        result.Append(bin_value);
      }
      return result;
    }
    case TypeTag::kCastExp: {
      const auto& cast = expr->AsCastExpression();
      ASSIGN_OR_RETURN(ColumnVector, child_vec,
                       (Evaluate(cast.Child(), schema, chunk, sel)));
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const Value child_val = child_vec.ValueAt(i);
        if (child_val.IsNull()) {
          result.Append(Value());
        } else {
          Row tmp_row({child_val});
          Schema tmp_schema("tmp", {Column("c", child_val.type)});
          ASSIGN_OR_RETURN(Value, casted,
                           (cast.TryEvaluate(tmp_row, tmp_schema)));
          result.Append(casted);
        }
      }
      return result;
    }
    case TypeTag::kCaseExp: {
      const auto& case_expr = expr->AsCaseExpression();
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const size_t row_idx = sel != nullptr ? (*sel)[i] : i;
        const Row row = chunk.RowAt(row_idx);
        ASSIGN_OR_RETURN(Value, branch_value,
                         (case_expr.TryEvaluate(row, schema)));
        result.Append(branch_value);
      }
      return result;
    }
    default: {
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const size_t row_idx = sel != nullptr ? (*sel)[i] : i;
        const Row row = chunk.RowAt(row_idx);
        ASSIGN_OR_RETURN(Value, fallback,
                         (EvaluateRowFallback(expr, row, schema)));
        result.Append(fallback);
      }
      return result;
    }
  }
}

StatusOr<ValidityBitmap> VectorizedExpression::EvaluateFilter(
    const Expression& expr, const Schema& schema, const DataChunk& chunk,
    const SelectionVector* sel) {
  const size_t total_rows = chunk.Size();
  if (total_rows == 0) {
    return ValidityBitmap(0, false);
  }

  if (sel != nullptr && sel->Empty()) {
    return ValidityBitmap(total_rows, false);
  }
  // Evaluate only the selected rows: evaluating masked-out rows can raise
  // (e.g. division by zero) for rows the caller already excluded.
  ASSIGN_OR_RETURN(ColumnVector, result_vec,
                   (Evaluate(expr, schema, chunk, sel)));
  ValidityBitmap bitmap(total_rows, false);

  if (sel == nullptr) {
    for (size_t i = 0; i < total_rows; ++i) {
      if (!result_vec.IsNull(i) && result_vec.ValueAt(i).Truthy()) {
        bitmap.SetBit(i);
      }
    }
  } else {
    for (size_t i = 0; i < sel->Size(); ++i) {
      const size_t row_idx = (*sel)[i];
      if (row_idx < total_rows && !result_vec.IsNull(i) &&
          result_vec.ValueAt(i).Truthy()) {
        bitmap.SetBit(row_idx);
      }
    }
  }
  return bitmap;
}

Status VectorizedExpression::FilterDataChunk(const Expression& expr,
                                             const Schema& schema,
                                             const DataChunk& chunk,
                                             SelectionVector* output_sel,
                                             const SelectionVector* input_sel) {
  assert(output_sel != nullptr);
  ASSIGN_OR_RETURN(ValidityBitmap, mask,
                   (EvaluateFilter(expr, schema, chunk, input_sel)));
  if (input_sel != nullptr) {
    input_sel->Filter(mask, output_sel);
  } else {
    mask.ToSelectionVector(output_sel);
  }
  return Status::kSuccess;
}

Value VectorizedExpression::Aggregate(AggregationType type,
                                      const ColumnVector& col,
                                      const SelectionVector* sel) {
  switch (type) {
    case AggregationType::kLogicalAnd:
      return col.AggregateLogicalAnd(sel);
    case AggregationType::kLogicalOr:
      return col.AggregateLogicalOr(sel);
    case AggregationType::kBitAnd:
      return col.AggregateBitAnd(sel);
    case AggregationType::kBitOr:
      return col.AggregateBitOr(sel);
    case AggregationType::kBitXor:
      return col.AggregateBitXor(sel);
    default:
      CHECK_MSG(false, "unsupported vectorized aggregate type");
  }
}

Value VectorizedExpression::AggregateLogicalAnd(const ColumnVector& col,
                                                const SelectionVector* sel) {
  return col.AggregateLogicalAnd(sel);
}

Value VectorizedExpression::AggregateLogicalOr(const ColumnVector& col,
                                               const SelectionVector* sel) {
  return col.AggregateLogicalOr(sel);
}

Value VectorizedExpression::AggregateBitAnd(const ColumnVector& col,
                                            const SelectionVector* sel) {
  return col.AggregateBitAnd(sel);
}

Value VectorizedExpression::AggregateBitOr(const ColumnVector& col,
                                           const SelectionVector* sel) {
  return col.AggregateBitOr(sel);
}

Value VectorizedExpression::AggregateBitXor(const ColumnVector& col,
                                            const SelectionVector* sel) {
  return col.AggregateBitXor(sel);
}

}  // namespace tinylamb
