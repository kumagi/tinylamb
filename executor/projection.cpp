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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace {

bool IntConstant(const tinylamb::Expression& expression, int64_t* value) {
  if (expression->Type() != tinylamb::TypeTag::kConstantValue) {
    return false;
  }
  const tinylamb::Value constant = expression->AsConstantValue().GetValue();
  if (constant.type != tinylamb::ValueType::kInt64) {
    return false;
  }
  // Signed JIT arithmetic cannot represent UINT64 operands.
  if (constant.IsUnsigned()) {
    return false;
  }
  *value = constant.value.int_value;
  return true;
}

bool Affine(  // NOLINT(misc-no-recursion)
    const tinylamb::Expression& expression, const tinylamb::Schema& schema,
    uint16_t* column, int64_t* multiplier, int64_t* addend) {
  if (expression->Type() == tinylamb::TypeTag::kColumnValue) {
    const int offset =
        schema.Offset(expression->AsColumnValue().GetColumnName());
    if (offset < 0 || schema.GetColumn(static_cast<size_t>(offset)).Type() !=
                          tinylamb::ValueType::kInt64) {
      return false;
    }
    if (schema.GetColumn(static_cast<size_t>(offset)).IsUnsigned()) {
      return false;
    }
    *column = static_cast<uint16_t>(offset);
    return true;
  }
  if (expression->Type() != tinylamb::TypeTag::kBinaryExp) {
    return false;
  }
  const auto& binary = expression->AsBinaryExpression();
  int64_t constant = 0;
  if (!IntConstant(binary.Right(), &constant)) {
    return false;
  }
  // The kernel checks multiply and add in that order. Folding a longer
  // chain can erase an intermediate overflow (for example (x * MAX) * 0).
  const bool bare_left =
      binary.Left()->Type() == tinylamb::TypeTag::kColumnValue;
  if (binary.Op() == tinylamb::BinaryOperation::kMultiply && bare_left &&
      Affine(binary.Left(), schema, column, multiplier, addend)) {
    *multiplier = constant;
    return true;
  }
  const bool multiply_left =
      binary.Left()->Type() == tinylamb::TypeTag::kBinaryExp &&
      binary.Left()->AsBinaryExpression().Op() ==
          tinylamb::BinaryOperation::kMultiply;
  if (binary.Op() == tinylamb::BinaryOperation::kAdd &&
      (bare_left || multiply_left) &&
      Affine(binary.Left(), schema, column, multiplier, addend)) {
    *addend = constant;
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
  // Only expressions reached on every row may be evaluated ahead of the
  // projection. A CASE arm or a lazy function argument may never be read.
  if (expression->Type() == tinylamb::TypeTag::kBinaryExp) {
    const auto& binary = expression->AsBinaryExpression();
    if (binary.Op() == tinylamb::BinaryOperation::kAnd ||
        binary.Op() == tinylamb::BinaryOperation::kOr) {
      CountCseCandidates(binary.Left(), counts, samples);
      return;
    }
  } else if (expression->Type() != tinylamb::TypeTag::kUnaryExp) {
    return;
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
      return std::ranges::all_of(tinylamb::ExpressionChildren(expression),
                                 IsCseSafe);
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
  return changed
             ? tinylamb::WithExpressionChildren(expression, std::move(children))
             : expression;
}

}  // namespace

namespace tinylamb {

std::optional<Projection::Int64Affine> Projection::ResolveInt64Affine(
    const Expression& expression, const Schema& input_schema) {
  Int64Affine result;
  if (!expression || !Affine(expression, input_schema, &result.column,
                             &result.multiplier, &result.addend)) {
    return std::nullopt;
  }
  return result;
}

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
  keys.reserve(counts.size());
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
        BytecodeEnabled()
            ? BytecodeCompiler::Compile(cse_expressions_.back(), input_schema_)
            : std::nullopt;
    if (!program) {
      cse_names_.pop_back();
      cse_use_counts_.pop_back();
      cse_expressions_.pop_back();
      slots.erase(keys[i]);
      continue;
    }
    augmented_columns.emplace_back(ColumnName("", slot), program->ResultType());
    cse_bytecodes_.push_back(std::move(program));
  }
  augmented_schema_ =
      Schema(input_schema_.Name(), std::move(augmented_columns));
  cse_input_batch_.Initialize(augmented_schema_);
  bytecodes_.reserve(expressions_.size());
  for (const NamedExpression& expression : expressions_) {
    const Expression rewritten = RewriteCse(expression.expression, slots);
    bytecodes_.push_back(BytecodeEnabled() ? BytecodeCompiler::Compile(
                                                 rewritten, augmented_schema_)
                                           : std::nullopt);
  }
  jit_states_.resize(expressions_.size());
  for (size_t index = 0; index < expressions_.size(); ++index) {
    JitProjectionState& state = jit_states_[index];
    state.eligible = BytecodeEnabled() &&
                     Affine(expressions_[index].expression, input_schema_,
                            &state.column, &state.multiplier, &state.addend);
    state.branchless_eligible =
        BytecodeEnabled() && bytecodes_[index].has_value() &&
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
        bytecodes_[index]->ResultType() == ValueType::kInt64;
  }
}

size_t Projection::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  destination->Reserve(max_rows);
  if (src_->NextBatch(&input_batch_, max_rows) == 0) {
    if (src_->GetStatus() != Status::kSuccess) {
      FailWith(src_->GetStatus());
    }
    return 0;
  }
  return ProjectChunk(input_batch_, destination);
}

size_t Projection::ProjectChunk(const DataChunk& input, DataChunk* destination) {
  const DataChunk* expression_input = &input;
  if (!cse_bytecodes_.empty()) {
    std::vector<ColumnVector> cse_values;
    cse_values.reserve(cse_bytecodes_.size());
    for (const std::optional<BytecodeProgram>& program : cse_bytecodes_) {
      if (!program) {
        continue;
      }
      StatusOr<ColumnVector> batch = program->TryEvaluateBatch(input);
      if (!batch.HasValue()) {
        FailWith(batch.GetStatus());
        return 0;
      }
      cse_values.push_back(batch.MoveValue());
    }
    cse_input_batch_.Reset();
    std::vector<const ColumnVector*> sources;
    sources.reserve(input.ColumnCount() + cse_values.size());
    for (size_t i = 0; i < input.ColumnCount(); ++i) {
      sources.push_back(&input.ColumnAt(i));
    }
    for (const ColumnVector& value : cse_values) {
      sources.push_back(&value);
    }
    for (size_t row_index = 0; row_index < input.Size(); ++row_index) {
      cse_input_batch_.AppendRowFromColumns(sources, row_index,
                                            input.PositionAt(row_index));
    }
    expression_input = &cse_input_batch_;
  }
  const Schema& active_schema =
      cse_bytecodes_.empty() ? input_schema_ : augmented_schema_;
  std::vector<std::optional<ColumnVector>> evaluated(expressions_.size());
  for (size_t index = 0; index < bytecodes_.size(); ++index) {
    JitProjectionState& jit = jit_states_[index];
    jit.rows_seen += expression_input->Size();
    if ((jit.eligible || jit.branchless_eligible) && !jit.attempted &&
        jit.rows_seen >= jit_threshold_rows_) {
      jit.attempted = true;
      if (jit.eligible && cse_bytecodes_.empty()) {
        jit.kernel = JitInt64Kernels::CompileProjectionChecked();
        if (!jit.kernel) {
          jit.kernel = JitInt64Kernels::CompileProjection();
        }
      }
      // Lean proof: lean/TinylambOpt/BranchlessJitCodegen.lean
      //   (if_conversion_sound_and_complete,
      //   zonemap_interval_mul_4corners_sound)
      if (jit.branchless_eligible && bytecodes_[index].has_value()) {
        jit.branchless =
            JitBranchlessProgram::Compile(*bytecodes_[index], active_schema);
      }
    }
    bool handled_by_branchless = false;
    if (jit.branchless && expression_input->HasLayout(active_schema)) {
      const auto& ref_cols = jit.branchless->ReferencedColumns();
      bool layout_ok = true;
      for (uint16_t col_idx : ref_cols) {
        const ColumnVector& col = expression_input->ColumnAt(col_idx);
        if (col.Type() != ValueType::kInt64 || col.IsUnsigned() ||
            col.IsHeterogeneous()) {
          layout_ok = false;
          break;
        }
      }
      if (layout_ok) {
        const size_t n_rows = expression_input->Size();
        if (jit_null_scratch_.size() < ref_cols.size()) {
          jit_null_scratch_.resize(ref_cols.size());
        }
        jit_col_inputs_.resize(ref_cols.size());
        bool any_col_nulls = false;
        for (size_t u = 0; u < ref_cols.size(); ++u) {
          const uint16_t col_idx = ref_cols[u];
          const ColumnVector& col = expression_input->ColumnAt(col_idx);
          const ZoneMap& zone = expression_input->ZoneMapAt(col_idx);
          const bool has_nulls = !zone.Initialized() || zone.NullCount() > 0;
          any_col_nulls = any_col_nulls || has_nulls;
          const uint8_t* null_ptr = nullptr;
          if (has_nulls) {
            jit_null_scratch_[u].resize(n_rows);
            UnpackNullBitmapToBytes(col.NullBitmap(), n_rows,
                                    jit_null_scratch_[u].data());
            null_ptr = jit_null_scratch_[u].data();
          }
          const bool has_bounds =
              zone.Initialized() && zone.ValueCount() > 0 &&
              zone.Minimum().has_value() && zone.Maximum().has_value() &&
              zone.Minimum()->type == ValueType::kInt64 &&
              zone.Maximum()->type == ValueType::kInt64 &&
              !zone.Minimum()->IsUnsigned() && !zone.Maximum()->IsUnsigned();
          jit_col_inputs_[u] = {
              .values = col.IntegerData().data(),
              .nulls = null_ptr,
              .has_nulls = has_nulls,
              .has_bounds = has_bounds,
              .min_value = has_bounds ? zone.Minimum()->value.int_value : 0,
              .max_value = has_bounds ? zone.Maximum()->value.int_value : 0};
        }
        // Prefer `jit.branchless` when the expression goes beyond 1-col
        // non-null affine, when NULLs are present, or when ZoneMap proves the
        // batch overflow-free (unlocking pure `nsw` AVX2 SIMD + memcpy output).
        const bool use_branchless =
            !jit.kernel.has_value() || any_col_nulls ||
            jit.branchless->ProveOverflowFree(jit_col_inputs_);
        if (use_branchless) {
          jit_out_values_.resize(n_rows);
          jit_out_nulls_.resize(n_rows);
          bool has_any_null = false;
          if (jit.branchless->Evaluate(jit_col_inputs_, n_rows,
                                       jit_out_values_.data(),
                                       jit_out_nulls_.data(), &has_any_null)) {
            ColumnVector& evaluated_column =
                evaluated[index].emplace(ValueType::kInt64, n_rows);
            evaluated_column.AppendIntBatch(
                ValueType::kInt64, /*is_unsigned=*/false,
                jit_out_values_.data(),
                has_any_null ? jit_out_nulls_.data() : nullptr, n_rows);
            ++jit_batches_;
            handled_by_branchless = true;
          }
        }
      }
    }
    if (handled_by_branchless) {
      continue;
    }
    // The JIT kernel reads raw signed-int64 storage.  Fire only when the
    // runtime batch really has that layout (signed INT64, no NULLs);
    // otherwise fall through to the bytecode/AST paths below.  This mirrors
    // the aggregation SUM guard.  The kernel is only compiled when CSE is
    // empty, so the batch layout here matches input_schema_ (not the
    // CSE-augmented schema).
    if (jit.kernel && expression_input->HasLayout(input_schema_) &&
        expression_input->ColumnAt(jit.column).Type() == ValueType::kInt64 &&
        !expression_input->ColumnAt(jit.column).IsUnsigned() &&
        expression_input->ZoneMapAt(jit.column).Initialized() &&
        expression_input->ZoneMapAt(jit.column).NullCount() == 0) {
      jit_out_values_.resize(expression_input->Size());
      bool mul_overflow = false;
      bool add_overflow = false;
      try {
        jit.kernel->ProjectChecked(
            expression_input->ColumnAt(jit.column).IntegerData().data(),
            jit_out_values_.data(), jit_out_values_.size(), jit.multiplier, jit.addend,
            &mul_overflow, &add_overflow);
      } catch (const std::logic_error&) {
        // Unchecked kernel fallback (checked unavailable): verify with
        // scalar overflow checks to preserve AST throw semantics.
        jit.kernel->Project(
            expression_input->ColumnAt(jit.column).IntegerData().data(),
            jit_out_values_.data(), jit_out_values_.size(), jit.multiplier, jit.addend);
        const auto& inputs =
            expression_input->ColumnAt(jit.column).IntegerData();
        for (size_t r = 0; r < inputs.size(); ++r) {
          int64_t product = 0;
          int64_t projected = 0;
          // T560: the canonical INT64 overflow raise is
          // kInvalidArgument "integer overflow on '<op>'" (type/value.cpp);
          // report the operator that actually overflowed instead of
          // blaming '*' for a combined mul||add test.
          if (__builtin_mul_overflow(inputs[r], jit.multiplier, &product)) {
            FailWith(StatusError(StatusCode::kInvalidArgument,
                                 "integer overflow on '*'"));
            return 0;
          }
          if (__builtin_add_overflow(product, jit.addend, &projected)) {
            FailWith(StatusError(StatusCode::kInvalidArgument,
                                 "integer overflow on '+'"));
            return 0;
          }
          jit_out_values_[r] = projected;
        }
      }
      if (mul_overflow) {
        FailWith(StatusError(StatusCode::kInvalidArgument,
                             "integer overflow on '*'"));
        return 0;
      }
      if (add_overflow) {
        FailWith(StatusError(StatusCode::kInvalidArgument,
                             "integer overflow on '+'"));
        return 0;
      }
      ColumnVector& evaluated_column =
          evaluated[index].emplace(ValueType::kInt64, jit_out_values_.size());
      evaluated_column.AppendIntBatch(ValueType::kInt64, false, jit_out_values_.data(),
                                      nullptr, jit_out_values_.size());
      ++jit_batches_;
    } else if (const std::optional<BytecodeProgram>& program =
                   bytecodes_[index]) {
      StatusOr<ColumnVector> batch =
          program->TryEvaluateBatch(*expression_input);
      if (!batch.HasValue()) {
        FailWith(batch.GetStatus());
        return 0;
      }
      evaluated[index].emplace(batch.MoveValue());
    }
  }
  bool all_evaluated = true;
  std::vector<const ColumnVector*> sources;
  sources.reserve(evaluated.size());
  for (const std::optional<ColumnVector>& column : evaluated) {
    if (!column) {
      all_evaluated = false;
      break;
    }
    sources.push_back(&*column);
  }
  if (all_evaluated) {
    for (size_t row_index = 0; row_index < expression_input->Size();
         ++row_index) {
      destination->AppendRowFromColumns(sources, row_index,
                                        input.PositionAt(row_index));
    }
    return destination->Size();
  }
  for (size_t row_index = 0; row_index < expression_input->Size();
       ++row_index) {
    std::vector<Value> result;
    result.reserve(expressions_.size());
    std::optional<Row> row;
    for (size_t expression_index = 0; expression_index < expressions_.size();
         ++expression_index) {
      const NamedExpression& named = expressions_[expression_index];
      const std::optional<ColumnVector>& evaluated_column =
          evaluated[expression_index];
      if (evaluated_column) {
        result.push_back(evaluated_column->ValueAt(row_index));
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
      if (!row) {
        row = expression_input->RowAt(row_index);
      }
      StatusOr<Value> value =
          named.expression->TryEvaluate(*row, augmented_schema_);
      if (!value.HasValue()) {
        FailWith(value.GetStatus());
        return 0;
      }
      result.push_back(value.MoveValue());
    }
    destination->Append(Row(std::move(result)),
                        input.PositionAt(row_index));
  }
  return destination->Size();
}

void Projection::DumpHeader(std::ostream& o, int indent,
                            bool leading_indent) const {
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
  const std::string pad = Indent(static_cast<size_t>(std::max(0, indent)));
  auto write_pad = [&](bool first) {
    if (!first || leading_indent) {
      o << pad;
    }
  };
  bool first_line = true;
  if (case_count >= 2) {
    write_pad(first_line);
    first_line = false;
    o << "ComputeScalar CASE uses=2\n";
  } else if (!repeated_key.empty()) {
    write_pad(first_line);
    first_line = false;
    const TypeTag type = dump_cse_samples.at(repeated_key)->Type();
    if (type == TypeTag::kCaseExp) {
      o << "ComputeScalar CASE uses=" << dump_cse_counts.at(repeated_key)
        << "\n";
    } else {
      o << "ComputeScalar slots=1 uses=" << dump_cse_counts.at(repeated_key)
        << "\n";
    }
  }
  write_pad(first_line);
  o << "Projection: [";
  for (size_t i = 0; i < expressions_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << expressions_[i];
  }
  o << "]";
}

void Projection::Dump(std::ostream& o, int indent) const {
  std::ostringstream child_dump;
  src_->Dump(child_dump, indent + 2);
  DumpHeader(o, indent, /*leading_indent=*/false);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2) << child_dump.str();
}

}  // namespace tinylamb
