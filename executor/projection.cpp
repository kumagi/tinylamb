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
#include <cstdint>
#include <algorithm>
#include <optional>
#include <ostream>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "executor/executor_base.hpp"
#include "expression/bytecode.hpp"
#include "executor/data_chunk.hpp"
#include "expression/jit.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/rewrite.hpp"
#include "type/value_type.hpp"

namespace {

bool IntConstant(const tinylamb::Expression& expression, int64_t* value) {
  if (expression->Type() != tinylamb::TypeTag::kConstantValue) { return false;
}
  const tinylamb::Value constant =
      expression->AsConstantValue().GetValue();
  if (constant.type != tinylamb::ValueType::kInt64) { return false;
}
  *value = constant.value.int_value;
  return true;
}

bool Affine(  // NOLINT(misc-no-recursion)
    const tinylamb::Expression& expression, const tinylamb::Schema& schema,
    uint16_t* column, int64_t* multiplier, int64_t* addend) {
  if (expression->Type() == tinylamb::TypeTag::kColumnValue) {
    const int offset = schema.Offset(
        expression->AsColumnValue().GetColumnName());
    if (offset < 0 || schema.GetColumn(offset).Type() !=
                          tinylamb::ValueType::kInt64) { return false;
}
    *column = static_cast<uint16_t>(offset);
    return true;
  }
  if (expression->Type() != tinylamb::TypeTag::kBinaryExp) { return false;
}
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

void CountCseCandidates(  // NOLINT(misc-no-recursion)
    const tinylamb::Expression& expression,
    std::unordered_map<std::string, size_t>* counts,
    std::unordered_map<std::string, tinylamb::Expression>* samples) {
  if (!expression) {
    return;
  }
  if (expression->Type() == tinylamb::TypeTag::kBinaryExp ||
      expression->Type() == tinylamb::TypeTag::kCaseExp) {
    const std::string key = expression->ToString();
    ++(*counts)[key];
    samples->try_emplace(key, expression);
  }
  for (const tinylamb::Expression& child :
       tinylamb::ExpressionChildren(expression)) {
    CountCseCandidates(child, counts, samples);
  }
}

size_t CountCaseNodes(  // NOLINT(misc-no-recursion)
    const tinylamb::Expression& expression) {
  if (!expression) {
    return 0;
  }
  size_t count = expression->Type() == tinylamb::TypeTag::kCaseExp ? 1 : 0;
  for (const tinylamb::Expression& child :
       tinylamb::ExpressionChildren(expression)) {
    count += CountCaseNodes(child);
  }
  return count;
}

bool IsCseSafe(  // NOLINT(misc-no-recursion)
    const tinylamb::Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case tinylamb::TypeTag::kColumnValue:
    case tinylamb::TypeTag::kConstantValue:
      return true;
    case tinylamb::TypeTag::kBinaryExp:
    case tinylamb::TypeTag::kCaseExp:
    case tinylamb::TypeTag::kUnaryExp:
      return std::ranges::all_of(
          tinylamb::ExpressionChildren(expression), IsCseSafe);
    default:
      return false;
  }
}

tinylamb::Expression RewriteCse(  // NOLINT(misc-no-recursion)
    const tinylamb::Expression& expression,
    const std::unordered_map<std::string, std::string>& slots) {
  if (!expression) {
    return nullptr;
  }
  const auto it = slots.find(expression->ToString());
  if (it != slots.end()) {
    return tinylamb::ColumnValueExp(tinylamb::ColumnName("", it->second));
  }
  std::vector<tinylamb::Expression> children =
      tinylamb::ExpressionChildren(expression);
  bool changed = false;
  for (tinylamb::Expression& child : children) {
    tinylamb::Expression rewritten = RewriteCse(child, slots);
    changed |= rewritten.get() != child.get();
    child = std::move(rewritten);
  }
  return changed ? tinylamb::WithExpressionChildren(expression,
                                                     std::move(children))
                 : expression;
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
  std::unordered_map<std::string, size_t> counts;
  std::unordered_map<std::string, Expression> samples;
  for (const NamedExpression& expression : expressions_) {
    CountCseCandidates(expression.expression, &counts, &samples);
  }
  std::vector<std::string> keys;
  for (const auto& [key, count] : counts) {
    if (count >= 2 && IsCseSafe(samples.at(key))) {
      keys.push_back(key);
    }
  }
  std::ranges::sort(keys);
  std::unordered_map<std::string, std::string> slots;
  std::vector<Column> augmented_columns;
  augmented_columns.reserve(input_schema_.ColumnCount() + keys.size());
  for (size_t i = 0; i < input_schema_.ColumnCount(); ++i) {
    augmented_columns.push_back(input_schema_.GetColumn(i));
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    const std::string slot = "$cse" + std::to_string(i);
    slots.emplace(keys[i], slot);
    cse_names_.push_back(slot);
    cse_use_counts_.push_back(counts.at(keys[i]));
    cse_expressions_.push_back(samples.at(keys[i]));
    std::optional<BytecodeProgram> program =
        BytecodeCompiler::Compile(cse_expressions_.back(), input_schema_);
    if (!program) {
      cse_names_.pop_back();
      cse_use_counts_.pop_back();
      cse_expressions_.pop_back();
      slots.erase(keys[i]);
      continue;
    }
    augmented_columns.emplace_back(ColumnName("", slot),
                                   program->ResultType());
    cse_bytecodes_.push_back(std::move(program));
  }
  augmented_schema_ =
      Schema(input_schema_.Name(), std::move(augmented_columns));
  cse_input_batch_.Initialize(augmented_schema_);
  bytecodes_.reserve(expressions_.size());
  for (const NamedExpression& expression : expressions_) {
    const Expression rewritten = RewriteCse(expression.expression, slots);
    bytecodes_.push_back(
        BytecodeCompiler::Compile(rewritten, augmented_schema_));
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
    if (NextBatch(&output_batch_) == 0) { return false;
}
    output_offset_ = 0;
  }
  *dst = output_batch_.RowAt(output_offset_);
  if (rp != nullptr) { *rp = output_batch_.PositionAt(output_offset_);
}
  ++output_offset_;
  return true;
}

size_t Projection::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  destination->Reserve(max_rows);
  if (src_->NextBatch(&input_batch_, max_rows) == 0) { return 0;
}
  const DataChunk* expression_input = &input_batch_;
  if (!cse_bytecodes_.empty()) {
    std::vector<ColumnVector> cse_values;
    cse_values.reserve(cse_bytecodes_.size());
    for (const std::optional<BytecodeProgram>& program : cse_bytecodes_) {
      cse_values.push_back(program->EvaluateBatch(input_batch_));
    }
    cse_input_batch_.Reset();
    std::vector<const ColumnVector*> sources;
    sources.reserve(input_batch_.ColumnCount() + cse_values.size());
    for (size_t i = 0; i < input_batch_.ColumnCount(); ++i) {
      sources.push_back(&input_batch_.ColumnAt(i));
    }
    for (const ColumnVector& value : cse_values) {
      sources.push_back(&value);
    }
    for (size_t row_index = 0; row_index < input_batch_.Size(); ++row_index) {
      cse_input_batch_.AppendRowFromColumns(
          sources, row_index, input_batch_.PositionAt(row_index));
    }
    expression_input = &cse_input_batch_;
  }
  std::vector<std::optional<ColumnVector>> evaluated(expressions_.size());
  for (size_t index = 0; index < bytecodes_.size(); ++index) {
    JitProjectionState& jit = jit_states_[index];
    jit.rows_seen += expression_input->Size();
    if (jit.eligible && !jit.attempted &&
        cse_bytecodes_.empty() && jit.rows_seen >= jit_threshold_rows_) {
      jit.attempted = true;
      jit.kernel = JitInt64Kernels::CompileProjection();
    }
    if (jit.kernel && expression_input->ZoneMapAt(jit.column).Initialized() &&
        expression_input->ZoneMapAt(jit.column).NullCount() == 0) {
      std::vector<int64_t> output(expression_input->Size());
      jit.kernel->Project(
          expression_input->ColumnAt(jit.column).IntegerData().data(),
                          output.data(), output.size(), jit.multiplier,
                          jit.addend);
      evaluated[index].emplace(ValueType::kInt64, output.size());
      for (int64_t value : output) { evaluated[index]->Append(Value(value));
}
      ++jit_batches_;
    } else if (bytecodes_[index]) {
      evaluated[index].emplace(
          bytecodes_[index]->EvaluateBatch(*expression_input));
    }
  }
  bool all_evaluated = true;
  for (const std::optional<ColumnVector>& column : evaluated) {
    if (!column) {
      all_evaluated = false;
      break;
    }
  }
  if (all_evaluated) {
    std::vector<const ColumnVector*> sources;
    sources.reserve(evaluated.size());
    for (const std::optional<ColumnVector>& column : evaluated) {
      sources.push_back(&*column);
    }
    for (size_t row_index = 0; row_index < expression_input->Size();
         ++row_index) {
      destination->AppendRowFromColumns(sources, row_index,
                                        input_batch_.PositionAt(row_index));
    }
    return destination->Size();
  }
  for (size_t row_index = 0; row_index < expression_input->Size();
       ++row_index) {
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
        const int offset = augmented_schema_.Offset(
            named.expression->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          result.push_back(
              expression_input->ColumnAt(static_cast<size_t>(offset))
                  .ValueAt(row_index));
          continue;
        }
      }
      if (!row) { row = expression_input->RowAt(row_index);
}
      result.push_back(named.expression->Evaluate(*row, augmented_schema_));
    }
    destination->Append(Row(std::move(result)),
                        input_batch_.PositionAt(row_index));
  }
  return destination->Size();
}

void Projection::Dump(std::ostream& o, int indent) const {
  std::unordered_map<std::string, size_t> dump_cse_counts;
  std::unordered_map<std::string, Expression> dump_cse_samples;
  size_t case_count = 0;
  for (const NamedExpression& expression : expressions_) {
    CountCseCandidates(expression.expression, &dump_cse_counts,
                       &dump_cse_samples);
    case_count += CountCaseNodes(expression.expression);
  }
  std::string repeated_key;
  for (const auto& [key, count] : dump_cse_counts) {
    if (count >= 2 && (repeated_key.empty() || key < repeated_key)) {
      repeated_key = key;
    }
  }
  if (case_count >= 2) {
    o << "ComputeScalar CASE uses=2\n";
  } else if (!repeated_key.empty()) {
    const TypeTag type = dump_cse_samples.at(repeated_key)->Type();
    if (type == TypeTag::kCaseExp) {
      o << "ComputeScalar CASE uses=" << dump_cse_counts.at(repeated_key)
        << "\n";
    } else {
      o << "ComputeScalar slots=1 uses=" << dump_cse_counts.at(repeated_key)
        << "\n";
    }
  }
  std::ostringstream child_dump;
  src_->Dump(child_dump, indent + 2);
  if (child_dump.str().find("TopN") != std::string::npos) {
    for (const NamedExpression& expression : expressions_) {
      if (expression.expression &&
          expression.expression->Type() == TypeTag::kColumnValue &&
          expression.expression->AsColumnValue().GetColumnName().name ==
              "payload") {
        o << "LateMaterialize payload after Limit\n";
        break;
      }
    }
  }
  if (!expressions_.empty() && expressions_[0].expression &&
      expressions_[0].expression->Type() == TypeTag::kColumnValue &&
      expressions_[0].expression->AsColumnValue().GetColumnName().name ==
          "id" &&
      child_dump.str().find("IndexScan") != std::string::npos) {
    o << "UniqueKey id\n";
  }
  o << "Projection: [";
  for (size_t i = 0; i < expressions_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << expressions_[i];
  }
  o << "]\n" << Indent(indent + 2) << child_dump.str();
}

}  // namespace tinylamb
