/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/vectorized_expression.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/selection_vector.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

Value EvaluateRowFallback(const Expression& expr, const Row& row,
                          const Schema& schema) {
  return expr->Evaluate(row, schema);
}

}  // namespace

ColumnVector VectorizedExpression::Evaluate(const Expression& expr,
                                            const Schema& schema,
                                            const DataChunk& chunk,
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
        throw std::runtime_error("column not found in schema: " +
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
      const ColumnVector child_vec =
          Evaluate(unary.Child(), schema, chunk, sel);
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const Value child_val = child_vec.ValueAt(i);
        result.Append(EvaluateUnary(unary.Op(), child_val));
      }
      return result;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expr->AsBinaryExpression();
      const ColumnVector left_vec =
          Evaluate(binary.Left(), schema, chunk, sel);
      const ColumnVector right_vec =
          Evaluate(binary.Right(), schema, chunk, sel);
      if (left_vec.Type() == ValueType::kInt64 &&
          right_vec.Type() == ValueType::kInt64) {
        const auto op = binary.Op();
        if (op == BinaryOperation::kAnd || op == BinaryOperation::kOr ||
            op == BinaryOperation::kXor) {
          ColumnVector result(ValueType::kInt64, active_count);
          const auto& l_ints = left_vec.IntegerData();
          const auto& r_ints = right_vec.IntegerData();
          for (size_t i = 0; i < active_count; ++i) {
            if (left_vec.IsNull(i) || right_vec.IsNull(i)) {
              result.Append(Value());
            } else {
              const int64_t lv = l_ints[i];
              const int64_t rv = r_ints[i];
              int64_t res = 0;
              switch (op) {
                case BinaryOperation::kAnd:
                  res = (lv != 0 && rv != 0) ? 1 : 0;
                  break;
                case BinaryOperation::kOr:
                  res = (lv != 0 || rv != 0) ? 1 : 0;
                  break;
                case BinaryOperation::kXor:
                  res = ((lv != 0) ^ (rv != 0)) ? 1 : 0;
                  break;
                default:
                  break;
              }
              result.Append(Value(res));
            }
          }
          return result;
        }
      }
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const Value left_val = left_vec.ValueAt(i);
        const Value right_val = right_vec.ValueAt(i);
        result.Append(EvaluateBinary(binary.Op(), left_val, right_val));
      }
      return result;
    }
    case TypeTag::kCastExp: {
      const auto& cast = expr->AsCastExpression();
      const ColumnVector child_vec =
          Evaluate(cast.Child(), schema, chunk, sel);
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const Value child_val = child_vec.ValueAt(i);
        if (child_val.IsNull()) {
          result.Append(Value());
        } else {
          Row tmp_row({child_val});
          Schema tmp_schema("tmp", {Column("c", child_val.type)});
          result.Append(cast.Evaluate(tmp_row, tmp_schema));
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
        result.Append(case_expr.Evaluate(row, schema));
      }
      return result;
    }
    default: {
      ColumnVector result(ValueType::kNull, active_count);
      for (size_t i = 0; i < active_count; ++i) {
        const size_t row_idx = sel != nullptr ? (*sel)[i] : i;
        const Row row = chunk.RowAt(row_idx);
        result.Append(EvaluateRowFallback(expr, row, schema));
      }
      return result;
    }
  }
}

ValidityBitmap VectorizedExpression::EvaluateFilter(
    const Expression& expr, const Schema& schema, const DataChunk& chunk,
    const SelectionVector* sel) {
  const size_t total_rows = chunk.Size();
  if (total_rows == 0) {
    return ValidityBitmap(0, false);
  }

  if (sel != nullptr && sel->Empty()) {
    return ValidityBitmap(total_rows, false);
  }

  const ColumnVector result_vec = Evaluate(expr, schema, chunk, nullptr);
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
      if (row_idx < total_rows && !result_vec.IsNull(row_idx) &&
          result_vec.ValueAt(row_idx).Truthy()) {
        bitmap.SetBit(row_idx);
      }
    }
  }
  return bitmap;
}

void VectorizedExpression::FilterDataChunk(const Expression& expr,
                                          const Schema& schema,
                                          const DataChunk& chunk,
                                          SelectionVector* output_sel,
                                          const SelectionVector* input_sel) {
  assert(output_sel != nullptr);
  const ValidityBitmap mask =
      EvaluateFilter(expr, schema, chunk, input_sel);
  if (input_sel != nullptr) {
    input_sel->Filter(mask, output_sel);
  } else {
    mask.ToSelectionVector(output_sel);
  }
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
      throw std::invalid_argument("unsupported vectorized aggregate type");
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
