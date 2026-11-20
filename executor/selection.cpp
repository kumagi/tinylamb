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
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/simd_comparison.hpp"
#include "executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/rewrite.hpp"
#include "page/row_position.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// OLAP early promotion: a stream of full batches this deep implies an
// estimated row count far beyond the JIT compile break-even, so waiting for
// the cumulative 20M-evaluation threshold would waste most of the scan.
constexpr size_t kJitEarlyPromotionRows = size_t{512} * 1024;

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
  // Zone maps over stored INT64 payloads use signed bounds. UINT64 shares
  // that storage, so pruning it here can discard values above INT64_MAX.
  if (schema.GetColumn(static_cast<size_t>(offset)).IsUnsigned() ||
      (*constant)->AsConstantValue().GetValue().IsUnsigned()) {
    return true;
  }
  return chunk.ZoneMapAt(static_cast<size_t>(offset))
      .MayMatch(operation, (*constant)->AsConstantValue().GetValue());
}

}  // namespace

Selection::Selection(Expression exp, Schema schema, Executor src,
                     size_t jit_threshold_rows)
    : exp_(std::move(exp)),
      conjuncts_(SplitConjuncts(exp_)),
      schema_(std::move(schema)),
      src_(std::move(src)),
      jit_threshold_rows_(jit_threshold_rows) {
  if (BytecodeEnabled() && jit_threshold_rows_ >= kJitEarlyPromotionRows &&
      exp_ && exp_->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& binary = exp_->AsBinaryExpression();
    if (IsComparison(binary.Op())) {
      const Expression* column = &binary.Left();
      const Expression* constant = &binary.Right();
      BinaryOperation operation = binary.Op();
      if ((*column)->Type() == TypeTag::kConstantValue &&
          (*constant)->Type() == TypeTag::kColumnValue) {
        std::swap(column, constant);
        operation = Flip(operation);
      }
      if ((*column)->Type() == TypeTag::kColumnValue &&
          (*constant)->Type() == TypeTag::kConstantValue) {
        const int offset =
            schema_.Offset((*column)->AsColumnValue().GetColumnName());
        const Value& value = (*constant)->AsConstantValue().GetValue();
        if (offset >= 0 &&
            (schema_.GetColumn(static_cast<size_t>(offset)).Type() ==
                 ValueType::kInt64 ||
             schema_.GetColumn(static_cast<size_t>(offset)).Type() ==
                 ValueType::kDate) &&
            value.type ==
                schema_.GetColumn(static_cast<size_t>(offset)).Type() &&
            !value.IsUnsigned() &&
            !schema_.GetColumn(static_cast<size_t>(offset)).IsUnsigned()) {
          simd_filter_ = SimdIntFilter{.enabled = true,
                                       .column = static_cast<size_t>(offset),
                                       .operation = operation,
                                       .constant = value.value.int_value};
        }
      }
    }
  }
  // The JIT kernel below is derived from the compiled program, so disabling
  // bytecode also disables the filter JIT (differential-testing hook).
  bytecode_ = BytecodeEnabled()
                  ? BytecodeCompiler::Compile(
                        exp_, schema_, BytecodeCompiler::Context::kFilter)
                  : std::nullopt;
  // When the whole predicate cannot compile (a single IS NOT NULL / IN over
  // a function / LIKE conjunct is enough), compile the conjuncts that CAN be
  // lowered so NextBatch evaluates them batch-wise and pays the per-row AST
  // walk only for the conjuncts that need it and the rows that survive.
  if (!bytecode_ && BytecodeEnabled() && conjuncts_.size() >= 2) {
    conjunct_plans_.reserve(conjuncts_.size());
    bool any_compiled = false;
    for (const Expression& conjunct : conjuncts_) {
      ConjunctPlan plan;
      if (conjunct) {
        plan.program = BytecodeCompiler::Compile(
            conjunct, schema_, BytecodeCompiler::Context::kFilter);
        any_compiled = any_compiled || plan.program.has_value();
      }
      conjunct_plans_.push_back(std::move(plan));
    }
    if (!any_compiled) {
      conjunct_plans_.clear();
    }
  }
}

size_t Selection::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(schema_, max_rows);
  while (destination->Size() < max_rows) {
    const size_t requested = max_rows - destination->Size();
    if (src_->NextBatch(&input_batch_, requested) == 0) {
      if (src_->GetStatus() != Status::kSuccess) {
        FailWith(src_->GetStatus());
        return 0;
      }
      break;
    }
    AppendSelected(input_batch_, destination);
    if (GetStatus() != Status::kSuccess) {
      return 0;
    }
  }
  return destination->Size();
}

void Selection::AppendSelected(const DataChunk& input, DataChunk* destination) {
  if (!BatchMayMatch(exp_, schema_, input)) {
    ++skipped_batches_;
    return;
  }
  std::optional<ColumnVector> predicates;
  rows_seen_ += input.Size();
  if (simd_filter_.enabled) {
    const ColumnVector& simd_column =
        input.ColumnAt(simd_filter_.column);
    const ZoneMap& simd_zone = input.ZoneMapAt(simd_filter_.column);
    if (input.HasLayout(schema_) &&
        simd_column.Type() == schema_.GetColumn(simd_filter_.column).Type() &&
        !simd_column.IsUnsigned() && simd_zone.Initialized() &&
        simd_zone.NullCount() == 0) {
      ValidityBitmap mask;
      SimdComparisonKernel::CompareInt64(
          simd_column.IntegerData().data(), simd_column.Size(),
          simd_filter_.operation, simd_filter_.constant, &mask);
      mask.ToSelectionVector(&simd_selection_vector_);
      if (!simd_selection_vector_.Empty()) {
        rows_selected_ += simd_selection_vector_.Size();
        destination->AppendGather(input, simd_selection_vector_.Data(),
                                  simd_selection_vector_.Size());
      }
      return;
    }
  }
  // Early promotion OR-condition: once the scan has streamed enough full
  // batches, its estimated row count dwarfs the JIT break-even point
  // (docs/jit_profile.md), so compiling now pays off long before the
  // cumulative threshold would fire.  That cumulative threshold remains as
  // the fallback for short or fragmented scans.
  if (bytecode_ && !jit_attempted_ &&
      (rows_seen_ >= jit_threshold_rows_ ||
       (rows_seen_ >= kJitEarlyPromotionRows &&
        input.Size() >= kDefaultVectorSize))) {
    jit_attempted_ = true;
    const auto& instructions = bytecode_->Instructions();
    // The operand fields are only meaningful once the opcodes match;
    // reading them before validation would index the constant pool with
    // a garbage uint16_t.
    // The bytecode compiler emits operands left-to-right, so a literal on
    // the left ("5 < x") is LoadConstant->LoadColumn. Accept both orders
    // like the zone-map and SIMD gates above, flipping the operation.
    const bool column_first =
        instructions[0].opcode == BytecodeOp::kLoadColumn &&
        instructions[1].opcode == BytecodeOp::kLoadConstant;
    const bool constant_first =
        instructions[0].opcode == BytecodeOp::kLoadConstant &&
        instructions[1].opcode == BytecodeOp::kLoadColumn;
    const size_t column_insn = column_first ? 0 : 1;
    const size_t constant_insn = column_first ? 1 : 0;
    const bool simple_int_cmp =
        instructions.size() == 3 && (column_first || constant_first) &&
        instructions[2].opcode == BytecodeOp::kBinaryInt64 &&
        instructions[constant_insn].operand < bytecode_->Constants().size() &&
        bytecode_->Constants()[instructions[constant_insn].operand].type ==
            ValueType::kInt64;
    // The JIT kernel compares with signed integer semantics. Any unsigned
    // involvement (UINT64 column or constant) must stay on the
    // unsigned-aware bytecode path (cf. the aggregation SUM guard).
    const bool constant_unsigned =
        simple_int_cmp &&
        bytecode_->Constants()[instructions[constant_insn].operand].IsUnsigned();
    const bool column_unsigned =
        simple_int_cmp && instructions[column_insn].operand < schema_.ColumnCount() &&
        schema_.GetColumn(instructions[column_insn].operand).IsUnsigned();
    if (simple_int_cmp && !constant_unsigned && !column_unsigned) {
      jit_column_ = instructions[column_insn].operand;
      jit_constant_ =
          bytecode_->Constants()[instructions[constant_insn].operand].value.int_value;
      jit_operation_ = constant_first ? Flip(instructions[2].binary)
                                      : instructions[2].binary;
      jit_filter_ = JitInt64Kernels::CompileFilter(jit_operation_);
    }
    // Lean proof: lean/TinylambOpt/BranchlessJitCodegen.lean
    //   (if_conversion_sound_and_complete, jit_3vl_lane_sound)
    jit_branchless_ = JitBranchlessProgram::Compile(*bytecode_, schema_);
    if (jit_branchless_) {
      jit_null_scratch_.resize(jit_branchless_->ReferencedColumns().size());
    }
  }
  // The JIT kernel compares with signed integer semantics over raw int64
  // storage.  Only fire when the runtime batch really carries a signed
  // INT64 layout: an unsigned column under a signed schema would compare
  // UINT64 > INT64_MAX keys wrong with ICmpSLT/SGT.
  if (jit_filter_ &&
      input.ColumnAt(jit_column_).Type() == ValueType::kInt64 &&
      !input.ColumnAt(jit_column_).IsUnsigned() &&
      input.HasLayout(schema_) &&
      input.ZoneMapAt(jit_column_).Initialized() &&
      input.ZoneMapAt(jit_column_).NullCount() == 0) {
    jit_keep_bytes_.resize(input.Size());
    jit_filter_->Filter(
        input.ColumnAt(jit_column_).IntegerData().data(),
        jit_keep_bytes_.data(), jit_keep_bytes_.size(), jit_constant_);
    selection_vector_.clear();
    for (size_t i = 0; i < jit_keep_bytes_.size(); ++i) {
      if (jit_keep_bytes_[i] != 0) {
        selection_vector_.push_back(static_cast<uint32_t>(i));
      }
    }
    ++jit_batches_;
    if (!selection_vector_.empty()) {
      rows_selected_ += selection_vector_.size();
      destination->AppendGather(input, selection_vector_.data(),
                                selection_vector_.size());
    }
    return;
  }
  if (jit_branchless_ && input.HasLayout(schema_)) {
    const auto& ref_cols = jit_branchless_->ReferencedColumns();
    bool layout_ok = true;
    for (uint16_t col_idx : ref_cols) {
      const ColumnVector& col = input.ColumnAt(col_idx);
      if (col.Type() != ValueType::kInt64 || col.IsUnsigned() ||
          col.IsHeterogeneous()) {
        layout_ok = false;
        break;
      }
    }
    if (layout_ok) {
      const size_t n_rows = input.Size();
      jit_col_inputs_.resize(ref_cols.size());
      for (size_t u = 0; u < ref_cols.size(); ++u) {
        const uint16_t col_idx = ref_cols[u];
        const ColumnVector& col = input.ColumnAt(col_idx);
        const ZoneMap& zone = input.ZoneMapAt(col_idx);
        const bool has_nulls = !zone.Initialized() || zone.NullCount() > 0;
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
      jit_keep_bytes_.resize(n_rows);
      if (jit_branchless_->Filter(jit_col_inputs_, n_rows,
                                  jit_keep_bytes_.data())) {
        selection_vector_.clear();
        for (size_t i = 0; i < n_rows; ++i) {
          if (jit_keep_bytes_[i] != 0) {
            selection_vector_.push_back(static_cast<uint32_t>(i));
          }
        }
        ++jit_batches_;
        if (!selection_vector_.empty()) {
          rows_selected_ += selection_vector_.size();
          destination->AppendGather(input, selection_vector_.data(),
                                    selection_vector_.size());
        }
        return;
      }
      // When `Filter` returns false, at least one row raised an unguarded
      // arithmetic/domain error. Fall straight to the tolerant per-row
      // conjunct path below (skipping whole-expression `TryEvaluateBatch`,
      // which would fail on the same row anyway).
    } else if (bytecode_) {
      StatusOr<ColumnVector> batch =
          bytecode_->TryEvaluateBatch(input);
      if (batch.HasValue()) {
        predicates.emplace(batch.MoveValue());
      }
    }
  } else if (bytecode_) {
    StatusOr<ColumnVector> batch = bytecode_->TryEvaluateBatch(input);
    if (batch.HasValue()) {
      predicates.emplace(batch.MoveValue());
    }
    // A batch-level error does not decide any single row: the compiled
    // program is one left-to-right AND tree, but WHERE semantics are
    // commutative (a conjunct that cleanly rejects the row suppresses
    // sibling errors), so on failure the row loop below must evaluate
    // conjunct-wise instead of propagating the error.
  }
  selection_vector_.clear();
  if (!predicates && !conjunct_plans_.empty()) {
    // Two-tier conjunct evaluation: tier 1 batch-evaluates every compiled
    // conjunct and drops rows where any of them is FALSE or NULL (a clean
    // reject also suppresses sibling errors, matching the tolerant WHERE
    // semantics below); tier 2 runs the remaining conjuncts per row and
    // only on the survivors.  Conjuncts whose batch evaluation failed this
    // batch rejoin the per-row set, mirroring the whole-predicate fallback.
    selection_vector_.reserve(input.Size());
    for (uint32_t i = 0; i < input.Size(); ++i) {
      selection_vector_.push_back(i);
    }
    std::vector<size_t> per_row;
    per_row.reserve(conjunct_plans_.size());
    for (size_t c = 0; c < conjunct_plans_.size(); ++c) {
      if (selection_vector_.empty()) {
        break;
      }
      const std::optional<BytecodeProgram>& program =
          conjunct_plans_[c].program;
      if (!program) {
        per_row.push_back(c);
        continue;
      }
      StatusOr<ColumnVector> batch = program->TryEvaluateBatch(input);
      if (!batch.HasValue()) {
        per_row.push_back(c);
        continue;
      }
      const ColumnVector& column = batch.Value();
      scratch_indices_.clear();
      if (column.Type() == ValueType::kInt64) {
        const std::vector<int64_t>& bits = column.IntegerData();
        for (uint32_t idx : selection_vector_) {
          if (!column.IsNull(idx) && bits[idx] != 0) {
            scratch_indices_.push_back(idx);
          }
        }
      } else {
        for (uint32_t idx : selection_vector_) {
          const Value predicate_value = column.ValueAt(idx);
          if (!predicate_value.IsNull() && predicate_value.Truthy()) {
            scratch_indices_.push_back(idx);
          }
        }
      }
      selection_vector_.swap(scratch_indices_);
    }
    if (!per_row.empty() && !selection_vector_.empty()) {
      std::vector<Expression> per_row_exprs;
      per_row_exprs.reserve(per_row.size());
      for (size_t c : per_row) {
        per_row_exprs.push_back(conjuncts_[c]);
      }
      scratch_indices_.clear();
      for (uint32_t idx : selection_vector_) {
        StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
            per_row_exprs, [&](const Expression& conjunct) {
              return conjunct->TryEvaluate(input.RowAt(idx), schema_);
            });
        if (!pass.HasValue()) {
          FailWith(pass.GetStatus());
          return;
        }
        if (pass.Value()) {
          scratch_indices_.push_back(idx);
        }
      }
      selection_vector_.swap(scratch_indices_);
    }
    if (!selection_vector_.empty()) {
      rows_selected_ += selection_vector_.size();
      destination->AppendGather(input, selection_vector_.data(),
                                selection_vector_.size());
    }
    return;
  }
  if (predicates && predicates->Type() == ValueType::kInt64) {
    // Batch-evaluated predicates: read the boolean column straight from
    // typed storage instead of boxing one Value per row.
    const std::vector<int64_t>& bits = predicates->IntegerData();
    for (size_t i = 0; i < input.Size(); ++i) {
      if (!predicates->IsNull(i) && bits[i] != 0) {
        selection_vector_.push_back(static_cast<uint32_t>(i));
      }
    }
  } else {
    for (size_t i = 0; i < input.Size(); ++i) {
      if (predicates) {
        const Value predicate = predicates->ValueAt(i);
        if (!predicate.IsNull() && predicate.Truthy()) {
          selection_vector_.push_back(static_cast<uint32_t>(i));
        }
        continue;
      }
      StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
          conjuncts_, [&](const Expression& conjunct) {
            return conjunct->TryEvaluate(input.RowAt(i), schema_);
          });
      if (!pass.HasValue()) {
        FailWith(pass.GetStatus());
        return;
      }
      if (pass.Value()) {
        selection_vector_.push_back(static_cast<uint32_t>(i));
      }
    }
  }
  // Gather-append the surviving rows in one bulk copy.
  if (!selection_vector_.empty()) {
    rows_selected_ += selection_vector_.size();
    destination->AppendGather(input, selection_vector_.data(),
                              selection_vector_.size());
  }
}

void Selection::DumpHeader(std::ostream& o, int indent,
                           bool leading_indent) const {
  std::string predicate = exp_->ToString();
  if (exp_->Type() == TypeTag::kBinaryExp) {
    const BinaryOperation op = exp_->AsBinaryExpression().Op();
    if (op == BinaryOperation::kIsDistinctFrom) {
      predicate = "NullSafeNotEqual";
    } else if (op == BinaryOperation::kIsNotDistinctFrom) {
      predicate = "NullSafeEqual";
    }
  }
  const std::string pad = Indent(static_cast<size_t>(std::max(0, indent)));
  if (leading_indent) {
    o << pad;
  }
  o << "Selection: " << predicate << " (zone-map skipped=" << skipped_batches_
    << ", jit batches=" << jit_batches_ << ")";
  // EXPLAIN (without ANALYZE) calls Dump before this operator has consumed
  // anything.  Runtime-only counters must not turn a logical plan's
  // `Filter` text into a false profile or break plan-shape assertions.
  if (rows_seen_ > 0) {
    o << "\n"
      << pad << "SelectionStats rows_evaluated=" << rows_seen_
      << " rows_selected=" << rows_selected_ << "\n"
      << pad << "ZoneMap blocks_skipped=" << skipped_batches_;
  }
}

void Selection::Dump(std::ostream& o, int indent) const {
  DumpHeader(o, indent, /*leading_indent=*/false);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
