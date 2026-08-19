/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "projection.hpp"

#include <cstddef>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"

namespace {

bool IntConstant(const tinylamb::Expression& expression, int64_t* value) {
  if (expression->Type() != tinylamb::TypeTag::kConstantValue) return false;
  const tinylamb::Value constant =
      expression->AsConstantValue().GetValue();
  if (constant.type != tinylamb::ValueType::kInt64) return false;
  *value = constant.value.int_value;
  return true;
}

bool Affine(const tinylamb::Expression& expression,
            const tinylamb::Schema& schema, uint16_t* column,
            int64_t* multiplier, int64_t* addend) {
  if (expression->Type() == tinylamb::TypeTag::kColumnValue) {
    const int offset = schema.Offset(
        expression->AsColumnValue().GetColumnName());
    if (offset < 0 || schema.GetColumn(offset).Type() !=
                          tinylamb::ValueType::kInt64) return false;
    *column = static_cast<uint16_t>(offset);
    return true;
  }
  if (expression->Type() != tinylamb::TypeTag::kBinaryExp) return false;
  const auto& binary = expression->AsBinaryExpression();
  int64_t constant = 0;
  if (binary.Op() == tinylamb::BinaryOperation::kMultiply &&
      IntConstant(binary.Right(), &constant) &&
      Affine(binary.Left(), schema, column, multiplier, addend)) {
    *multiplier *= constant;
    *addend *= constant;
    return true;
  }
  if (binary.Op() == tinylamb::BinaryOperation::kAdd &&
      IntConstant(binary.Right(), &constant) &&
      Affine(binary.Left(), schema, column, multiplier, addend)) {
    *addend += constant;
    return true;
  }
  return false;
}

}  // namespace

namespace tinylamb {

Projection::Projection(std::vector<NamedExpression> expressions,
                       Schema input_schema, Executor src,
                       size_t jit_threshold_rows)
    : expressions_(std::move(expressions)),
      input_schema_(std::move(input_schema)),
      src_(std::move(src)),
      jit_threshold_rows_(jit_threshold_rows) {
  bytecodes_.reserve(expressions_.size());
  for (const NamedExpression& expression : expressions_) {
    bytecodes_.push_back(
        BytecodeCompiler::Compile(expression.expression, input_schema_));
  }
  jit_states_.resize(expressions_.size());
  for (size_t index = 0; index < expressions_.size(); ++index) {
    JitProjectionState& state = jit_states_[index];
    state.eligible = Affine(expressions_[index].expression, input_schema_,
                            &state.column, &state.multiplier, &state.addend);
  }
}

bool Projection::Next(Row* dst, RowPosition* rp) {
  if (output_offset_ >= output_batch_.Size()) {
    if (NextBatch(&output_batch_) == 0) return false;
    output_offset_ = 0;
  }
  *dst = output_batch_.RowAt(output_offset_);
  if (rp) *rp = output_batch_.PositionAt(output_offset_);
  ++output_offset_;
  return true;
}

size_t Projection::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  destination->Reserve(max_rows);
  if (src_->NextBatch(&input_batch_, max_rows) == 0) return 0;
  std::vector<std::optional<ColumnVector>> evaluated(expressions_.size());
  for (size_t index = 0; index < bytecodes_.size(); ++index) {
    JitProjectionState& jit = jit_states_[index];
    jit.rows_seen += input_batch_.Size();
    if (jit.eligible && !jit.attempted &&
        jit.rows_seen >= jit_threshold_rows_) {
      jit.attempted = true;
      jit.kernel = JitInt64Kernels::CompileProjection();
    }
    if (jit.kernel && input_batch_.ZoneMapAt(jit.column).NullCount() == 0) {
      std::vector<int64_t> output(input_batch_.Size());
      jit.kernel->Project(input_batch_.ColumnAt(jit.column).IntegerData().data(),
                          output.data(), output.size(), jit.multiplier,
                          jit.addend);
      evaluated[index].emplace(ValueType::kInt64, output.size());
      for (int64_t value : output) evaluated[index]->Append(Value(value));
      ++jit_batches_;
    } else if (bytecodes_[index]) {
      evaluated[index].emplace(
          bytecodes_[index]->EvaluateBatch(input_batch_));
    }
  }
  for (size_t row_index = 0; row_index < input_batch_.Size(); ++row_index) {
    std::vector<Value> result;
    result.reserve(expressions_.size());
    std::optional<Row> row;
    for (size_t expression_index = 0;
         expression_index < expressions_.size(); ++expression_index) {
      const NamedExpression& named = expressions_[expression_index];
      if (evaluated[expression_index]) {
        result.push_back(evaluated[expression_index]->ValueAt(row_index));
        continue;
      }
      if (named.expression->Type() == TypeTag::kColumnValue) {
        const int offset = input_schema_.Offset(
            named.expression->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          result.push_back(
              input_batch_.ColumnAt(static_cast<size_t>(offset))
                  .ValueAt(row_index));
          continue;
        }
      }
      if (!row) row = input_batch_.RowAt(row_index);
      result.push_back(named.expression->Evaluate(*row, input_schema_));
    }
    destination->Append(Row(std::move(result)),
                        input_batch_.PositionAt(row_index));
  }
  return destination->Size();
}

void Projection::Dump(std::ostream& o, int indent) const {
  o << "Projection: [";
  for (size_t i = 0; i < expressions_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << expressions_[i];
  }
  o << "]\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
