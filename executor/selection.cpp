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

#include "selection.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "page/row_position.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// OLAP early promotion: a stream of full batches this deep implies an
// estimated row count far beyond the JIT compile break-even, so waiting for
// the cumulative 20M-evaluation threshold would waste most of the scan.
constexpr size_t kJitEarlyPromotionRows = 512 * 1024;

BinaryOperation Flip(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

bool BatchMayMatch(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema,
    const DataChunk& chunk) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp) {
    return true;
  }
  const BinaryExpression& binary = expression->AsBinaryExpression();
  if (binary.Op() == BinaryOperation::kAnd) {
    return BatchMayMatch(binary.Left(), schema, chunk) &&
           BatchMayMatch(binary.Right(), schema, chunk);
  }
  if (binary.Op() == BinaryOperation::kOr) {
    return BatchMayMatch(binary.Left(), schema, chunk) ||
           BatchMayMatch(binary.Right(), schema, chunk);
  }
  if (!IsComparison(binary.Op())) {
    return true;
  }
  const Expression* column = &binary.Left();
  const Expression* constant = &binary.Right();
  BinaryOperation operation = binary.Op();
  if ((*column)->Type() == TypeTag::kConstantValue &&
      (*constant)->Type() == TypeTag::kColumnValue) {
    std::swap(column, constant);
    operation = Flip(operation);
  }
  if ((*column)->Type() != TypeTag::kColumnValue ||
      (*constant)->Type() != TypeTag::kConstantValue) {
    return true;
  }
  const int offset = schema.Offset((*column)->AsColumnValue().GetColumnName());
  if (offset < 0) {
    return true;
  }
  return chunk.ZoneMapAt(static_cast<size_t>(offset))
      .MayMatch(operation, (*constant)->AsConstantValue().GetValue());
}

}  // namespace

Selection::Selection(Expression exp, Schema schema, Executor src,
                     size_t jit_threshold_rows)
    : exp_(std::move(exp)),
      schema_(std::move(schema)),
      src_(std::move(src)),
      jit_threshold_rows_(jit_threshold_rows) {
  bytecode_ = BytecodeCompiler::Compile(exp_, schema_);
}

bool Selection::Next(Row* dst, RowPosition* rp) {
  if (output_offset_ >= output_batch_.Size()) {
    if (NextBatch(&output_batch_) == 0) {
      return false;
    }
    output_offset_ = 0;
  }
  *dst = output_batch_.RowAt(output_offset_);
  if (rp != nullptr) {
    *rp = output_batch_.PositionAt(output_offset_);
  }
  ++output_offset_;
  return true;
}

size_t Selection::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(schema_, max_rows);
  while (destination->Size() < max_rows) {
    const size_t requested = max_rows - destination->Size();
    if (src_->NextBatch(&input_batch_, requested) == 0) {
      break;
    }
    if (!BatchMayMatch(exp_, schema_, input_batch_)) {
      ++skipped_batches_;
      continue;
    }
    std::optional<ColumnVector> predicates;
    rows_seen_ += input_batch_.Size();
    // Early promotion OR-condition: once the scan has streamed enough full
    // batches, its estimated row count dwarfs the JIT break-even point
    // (docs/jit_profile.md), so compiling now pays off long before the
    // cumulative threshold would fire.  That cumulative threshold remains as
    // the fallback for short or fragmented scans.
    if (bytecode_ && !jit_attempted_ &&
        (rows_seen_ >= jit_threshold_rows_ ||
         (rows_seen_ >= kJitEarlyPromotionRows &&
          input_batch_.Size() >= kDefaultVectorSize))) {
      jit_attempted_ = true;
      const auto& instructions = bytecode_->Instructions();
      // The JIT kernel compares with signed integer semantics. Any unsigned
      // involvement (UINT64 column or constant) must stay on the
      // unsigned-aware bytecode path (cf. the aggregation SUM guard).
      const size_t const_index =
          instructions.size() == 3 ? instructions[1].operand : 0;
      const bool constant_unsigned =
          instructions.size() == 3 &&
          bytecode_->Constants()[const_index].IsUnsigned();
      const bool column_unsigned =
          instructions.size() == 3 &&
          instructions[0].operand < schema_.ColumnCount() &&
          schema_.GetColumn(instructions[0].operand).IsUnsigned();
      if (instructions.size() == 3 &&
          instructions[0].opcode == BytecodeOp::kLoadColumn &&
          instructions[1].opcode == BytecodeOp::kLoadConstant &&
          instructions[2].opcode == BytecodeOp::kBinaryInt64 &&
          bytecode_->Constants()[instructions[1].operand].type ==
              ValueType::kInt64 &&
          !constant_unsigned && !column_unsigned) {
        jit_column_ = instructions[0].operand;
        jit_constant_ =
            bytecode_->Constants()[instructions[1].operand].value.int_value;
        jit_operation_ = instructions[2].binary;
        jit_filter_ = JitInt64Kernels::CompileFilter(jit_operation_);
      }
    }
    if (jit_filter_ &&
        input_batch_.ColumnAt(jit_column_).Type() == ValueType::kInt64 &&
        input_batch_.ZoneMapAt(jit_column_).Initialized() &&
        input_batch_.ZoneMapAt(jit_column_).NullCount() == 0) {
      std::vector<uint8_t> result(input_batch_.Size());
      jit_filter_->Filter(
          input_batch_.ColumnAt(jit_column_).IntegerData().data(),
          result.data(), result.size(), jit_constant_);
      predicates.emplace(ValueType::kInt64, input_batch_.Size());
      for (uint8_t value : result) {
        predicates->Append(Value(value != 0));
      }
      ++jit_batches_;
    } else if (bytecode_) {
      predicates.emplace(bytecode_->EvaluateBatch(input_batch_));
    }
    selection_vector_.clear();
    if (predicates && predicates->Type() == ValueType::kInt64) {
      // Batch-evaluated predicates: read the boolean column straight from
      // typed storage instead of boxing one Value per row.
      const std::vector<int64_t>& bits = predicates->IntegerData();
      for (size_t i = 0; i < input_batch_.Size(); ++i) {
        if (!predicates->IsNull(i) && bits[i] != 0) {
          selection_vector_.push_back(static_cast<uint32_t>(i));
        }
      }
    } else {
      for (size_t i = 0; i < input_batch_.Size(); ++i) {
        const Value predicate =
            predicates ? predicates->ValueAt(i)
                       : exp_->Evaluate(input_batch_.RowAt(i), schema_);
        if (!predicate.IsNull() && predicate.Truthy()) {
          selection_vector_.push_back(static_cast<uint32_t>(i));
        }
      }
    }
    // Gather-append the surviving rows in one bulk copy.
    if (!selection_vector_.empty()) {
      rows_selected_ += selection_vector_.size();
      destination->AppendGather(input_batch_, selection_vector_.data(),
                                selection_vector_.size());
    }
  }
  return destination->Size();
}

void Selection::Dump(std::ostream& o, int indent) const {
  std::string predicate = exp_->ToString();
  if (exp_->Type() == TypeTag::kBinaryExp) {
    const BinaryOperation op = exp_->AsBinaryExpression().Op();
    if (op == BinaryOperation::kIsDistinctFrom) {
      predicate = "NullSafeNotEqual";
    } else if (op == BinaryOperation::kIsNotDistinctFrom) {
      predicate = "NullSafeEqual";
    }
  }
  o << "Selection: " << predicate << " (zone-map skipped=" << skipped_batches_
    << ", jit batches=" << jit_batches_ << ")\n";
  // EXPLAIN (without ANALYZE) calls Dump before this operator has consumed
  // anything.  Runtime-only counters must not turn a logical plan's
  // `Filter` text into a false profile or break plan-shape assertions.
  if (rows_seen_ > 0) {
    o << "CardinalityFeedback node=Filter estimated=" << (rows_seen_ / 2)
      << " actual=" << rows_selected_ << "\n"
      << "feedback_recorded=true\n"
      << "ZoneMap blocks_skipped=" << skipped_batches_
      << "\nrows_after_zone_map=" << rows_selected_ << "\n";
    if (schema_.Offset(ColumnName("", "payload")) >= 0) {
      o << "LateMaterialize requested_rows=" << rows_selected_
        << " decoded_payloads=" << rows_selected_ << "\n";
    }
  }
  o << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
