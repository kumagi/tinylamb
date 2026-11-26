/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/string_util.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"

namespace tinylamb::relational_detail {

ProgramShape ClassifyProgram(const BytecodeProgram& program,
                             const Expression& expr, const Schema* schema) {
  ProgramShape shape;
  shape.fast = program.FastInterpretable();
  const std::vector<BytecodeInstruction>& instructions = program.Instructions();
  if (instructions.size() == 1 &&
      instructions[0].opcode == BytecodeOp::kLoadColumn) {
    shape.direct_column = instructions[0].operand;
  } else if (instructions.size() == 2 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kExtractDatePart) {
    shape.extract_date_col = static_cast<int32_t>(instructions[0].operand);
    shape.extract_date_part = instructions[1].operand;
  } else if (instructions.size() == 3 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kLoadColumn &&
             instructions[2].opcode == BytecodeOp::kBinaryDouble &&
             instructions[2].binary == BinaryOperation::kMultiply) {
    shape.mul_col0 = static_cast<int32_t>(instructions[0].operand);
    shape.mul_col1 = static_cast<int32_t>(instructions[1].operand);
  } else if (instructions.size() == 5 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kLoadConstant &&
             instructions[2].opcode == BytecodeOp::kLoadColumn &&
             instructions[3].opcode == BytecodeOp::kBinaryDouble &&
             instructions[3].binary == BinaryOperation::kSubtract &&
             instructions[4].opcode == BytecodeOp::kBinaryDouble &&
             instructions[4].binary == BinaryOperation::kMultiply) {
    const Value& k = program.Constants()[instructions[1].operand];
    if (!k.IsNull() && (k.type == ValueType::kDouble ||
                        (k.type == ValueType::kInt64 && !k.IsUnsigned()))) {
      shape.mul_one_minus_col0 = static_cast<int32_t>(instructions[0].operand);
      shape.mul_one_minus_col1 = static_cast<int32_t>(instructions[2].operand);
      shape.mul_one_minus_const = k.type == ValueType::kDouble
                                      ? k.value.double_value
                                      : static_cast<double>(k.value.int_value);
    }
  } else if (instructions.size() == 9 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kLoadConstant &&
             instructions[2].opcode == BytecodeOp::kLoadColumn &&
             instructions[3].opcode == BytecodeOp::kBinaryDouble &&
             instructions[3].binary == BinaryOperation::kSubtract &&
             instructions[4].opcode == BytecodeOp::kBinaryDouble &&
             instructions[4].binary == BinaryOperation::kMultiply &&
             instructions[5].opcode == BytecodeOp::kLoadColumn &&
             instructions[6].opcode == BytecodeOp::kLoadColumn &&
             instructions[7].opcode == BytecodeOp::kBinaryDouble &&
             instructions[7].binary == BinaryOperation::kMultiply &&
             instructions[8].opcode == BytecodeOp::kBinaryDouble &&
             instructions[8].binary == BinaryOperation::kSubtract) {
    const Value& k = program.Constants()[instructions[1].operand];
    if (!k.IsNull() && (k.type == ValueType::kDouble ||
                        (k.type == ValueType::kInt64 && !k.IsUnsigned()))) {
      shape.profit_col0 = static_cast<int32_t>(instructions[0].operand);
      shape.profit_col1 = static_cast<int32_t>(instructions[2].operand);
      shape.profit_col2 = static_cast<int32_t>(instructions[5].operand);
      shape.profit_col3 = static_cast<int32_t>(instructions[6].operand);
      shape.profit_const = k.type == ValueType::kDouble
                               ? k.value.double_value
                               : static_cast<double>(k.value.int_value);
    }
  }
  if (instructions.size() == 3 &&
      instructions[2].opcode == BytecodeOp::kBinaryVarchar &&
      (instructions[2].binary == BinaryOperation::kLike ||
       instructions[2].binary == BinaryOperation::kNotLike) &&
      instructions[0].opcode == BytecodeOp::kLoadColumn &&
      instructions[1].opcode == BytecodeOp::kLoadConstant) {
    // The stack pops right then left, so the constant is the pattern only
    // when it was pushed second.  A collated pattern never reaches the fast
    // interpreter (ProgramIsFastInterpretable keeps such programs canonical);
    // mirror that gate so both paths agree.
    const Value& pattern = program.Constants()[instructions[1].operand];
    if (pattern.type == ValueType::kVarChar && pattern.Collation() == 0) {
      shape.like_column = static_cast<int32_t>(instructions[0].operand);
      shape.like_pattern = pattern.value.varchar_value;
      shape.like_negated = instructions[2].binary == BinaryOperation::kNotLike;
    }
  }
  if (expr && schema != nullptr && expr->Type() == TypeTag::kFunctionCallExp &&
      !shape.HasPairDirect()) {
    const FunctionCallExpression& call = expr->AsFunctionCallExpression();
    if ((call.FuncName() == "coalesce" || call.FuncName() == "ifnull") &&
        call.Args().size() == 2) {
      auto unwrap_double_cast = [](Expression e, bool* saw_cast) -> Expression {
        if (e && e->Type() == TypeTag::kCastExp &&
            e->AsCastExpression().TargetValueType() == ValueType::kDouble &&
            e->AsCastExpression().Child() != nullptr) {
          if (saw_cast != nullptr) {
            *saw_cast = true;
          }
          return e->AsCastExpression().Child();
        }
        return e;
      };
      const Expression& a0 = unwrap_double_cast(call.Args()[0], nullptr);
      const Expression& a1 = unwrap_double_cast(call.Args()[1], nullptr);
      bool zero_ok = false;
      if (a1 && a1->Type() == TypeTag::kConstantValue) {
        const Value& kv = a1->AsConstantValue().GetValue();
        if (!kv.IsNull() && !kv.IsUnsigned()) {
          if ((kv.type == ValueType::kDouble && kv.value.double_value == 0.0) ||
              (kv.type == ValueType::kInt64 && kv.value.int_value == 0)) {
            zero_ok = true;
          }
        }
      }
      if (zero_ok && a0 && a0->Type() == TypeTag::kBinaryExp &&
          a0->AsBinaryExpression().Op() == BinaryOperation::kMultiply) {
        const BinaryExpression& mb = a0->AsBinaryExpression();
        bool saw_left_cast = false;
        bool saw_right_cast = false;
        const Expression& ml = unwrap_double_cast(mb.Left(), &saw_left_cast);
        const Expression& mr = unwrap_double_cast(mb.Right(), &saw_right_cast);
        if (ml && ml->Type() == TypeTag::kColumnValue && mr &&
            mr->Type() == TypeTag::kColumnValue) {
          const auto off0 =
              LocalColumnOffset(*schema, ml->AsColumnValue().GetColumnName());
          const auto off1 =
              LocalColumnOffset(*schema, mr->AsColumnValue().GetColumnName());
          if (off0.has_value() && off1.has_value()) {
            const Column& c0 = schema->GetColumn(*off0);
            const Column& c1 = schema->GetColumn(*off1);
            const bool t0_ok =
                !c0.IsUnsigned() && (c0.Type() == ValueType::kDouble ||
                                     c0.Type() == ValueType::kInt64);
            const bool t1_ok =
                !c1.IsUnsigned() && (c1.Type() == ValueType::kDouble ||
                                     c1.Type() == ValueType::kInt64);
            const bool is_double_mul = c0.Type() == ValueType::kDouble ||
                                       c1.Type() == ValueType::kDouble ||
                                       saw_left_cast || saw_right_cast;
            if (t0_ok && t1_ok && is_double_mul) {
              shape.coalesce_mul_col0 = static_cast<int32_t>(*off0);
              shape.coalesce_mul_col1 = static_cast<int32_t>(*off1);
            }
          }
        }
      }
    }
  }
  if (expr && schema != nullptr && expr->Type() == TypeTag::kCaseExp &&
      !shape.HasPairDirect()) {
    const CaseExpression& ce = expr->AsCaseExpression();
    if (ce.when_clauses_.size() == 1 &&
        (!ce.else_clause_ ||
         ce.else_clause_->Type() == TypeTag::kConstantValue)) {
      const Value null_else;
      const Value& ev = ce.else_clause_
                            ? ce.else_clause_->AsConstantValue().GetValue()
                            : null_else;
      const bool else_zero_int = !ev.IsNull() && ev.type == ValueType::kInt64 &&
                                 !ev.IsUnsigned() && ev.value.int_value == 0;
      const bool else_zero_double = !ev.IsNull() &&
                                    ev.type == ValueType::kDouble &&
                                    ev.value.double_value == 0.0;
      if (else_zero_int || else_zero_double || ev.IsNull()) {
        const Expression& cond = ce.when_clauses_[0].first;
        const Expression& val = ce.when_clauses_[0].second;
        ProgramShape::CaseCondKind ck = ProgramShape::CaseCondKind::kNone;
        int32_t ccol = -1;
        int64_t cint = 0;
        std::string_view cstr0;
        std::string_view cstr1;
        if (cond && cond->Type() == TypeTag::kBinaryExp) {
          const BinaryExpression& cb = cond->AsBinaryExpression();
          if (cb.Op() == BinaryOperation::kEquals && cb.Left() && cb.Right() &&
              cb.Left()->Type() == TypeTag::kColumnValue &&
              cb.Right()->Type() == TypeTag::kConstantValue) {
            const auto off = LocalColumnOffset(
                *schema, cb.Left()->AsColumnValue().GetColumnName());
            const Value& cv = cb.Right()->AsConstantValue().GetValue();
            if (off.has_value() && !cv.IsNull()) {
              const Column& sc = schema->GetColumn(*off);
              if (sc.Type() == ValueType::kInt64 && !sc.IsUnsigned() &&
                  cv.type == ValueType::kInt64 && !cv.IsUnsigned()) {
                ck = ProgramShape::CaseCondKind::kColEqInt;
                ccol = static_cast<int32_t>(*off);
                cint = cv.value.int_value;
              } else if (sc.Type() == ValueType::kVarChar &&
                         cv.type == ValueType::kVarChar &&
                         cv.Collation() == 0 && !cv.IsCaseInsensitive()) {
                ck = ProgramShape::CaseCondKind::kColEqStr;
                ccol = static_cast<int32_t>(*off);
                cstr0 = cv.value.varchar_value;
              }
            }
          } else if (cb.Op() == BinaryOperation::kLike && cb.Left() &&
                     cb.Right() && cb.Left()->Type() == TypeTag::kColumnValue &&
                     cb.Right()->Type() == TypeTag::kConstantValue) {
            const auto off = LocalColumnOffset(
                *schema, cb.Left()->AsColumnValue().GetColumnName());
            const Value& cv = cb.Right()->AsConstantValue().GetValue();
            if (off.has_value() &&
                schema->GetColumn(*off).Type() == ValueType::kVarChar &&
                !cv.IsNull() && cv.type == ValueType::kVarChar &&
                cv.Collation() == 0 && !cv.IsCaseInsensitive()) {
              const std::string_view pat = cv.value.varchar_value;
              if (pat.size() >= 2 && pat.back() == '%' &&
                  pat.substr(0, pat.size() - 1).find_first_of("%_") ==
                      std::string_view::npos) {
                ck = ProgramShape::CaseCondKind::kColLikePrefix;
                ccol = static_cast<int32_t>(*off);
                cstr0 = pat.substr(0, pat.size() - 1);
              }
            }
          } else if ((cb.Op() == BinaryOperation::kOr ||
                      cb.Op() == BinaryOperation::kAnd) &&
                     cb.Left() && cb.Right() &&
                     cb.Left()->Type() == TypeTag::kBinaryExp &&
                     cb.Right()->Type() == TypeTag::kBinaryExp) {
            const BinaryExpression& b0 = cb.Left()->AsBinaryExpression();
            const BinaryExpression& b1 = cb.Right()->AsBinaryExpression();
            const BinaryOperation exp_op = (cb.Op() == BinaryOperation::kOr)
                                               ? BinaryOperation::kEquals
                                               : BinaryOperation::kNotEquals;
            if (b0.Op() == exp_op && b1.Op() == exp_op && b0.Left() &&
                b0.Right() && b1.Left() && b1.Right() &&
                b0.Left()->Type() == TypeTag::kColumnValue &&
                b0.Right()->Type() == TypeTag::kConstantValue &&
                b1.Left()->Type() == TypeTag::kColumnValue &&
                b1.Right()->Type() == TypeTag::kConstantValue) {
              const auto off0 = LocalColumnOffset(
                  *schema, b0.Left()->AsColumnValue().GetColumnName());
              const auto off1 = LocalColumnOffset(
                  *schema, b1.Left()->AsColumnValue().GetColumnName());
              const Value& v0 = b0.Right()->AsConstantValue().GetValue();
              const Value& v1 = b1.Right()->AsConstantValue().GetValue();
              if (off0.has_value() && off1.has_value() && *off0 == *off1 &&
                  schema->GetColumn(*off0).Type() == ValueType::kVarChar &&
                  !v0.IsNull() && !v1.IsNull() &&
                  v0.type == ValueType::kVarChar &&
                  v1.type == ValueType::kVarChar && v0.Collation() == 0 &&
                  v1.Collation() == 0 && !v0.IsCaseInsensitive() &&
                  !v1.IsCaseInsensitive()) {
                ck = (cb.Op() == BinaryOperation::kOr)
                         ? ProgramShape::CaseCondKind::kColEq2StrOr
                         : ProgramShape::CaseCondKind::kColNe2StrAnd;
                ccol = static_cast<int32_t>(*off0);
                cstr0 = v0.value.varchar_value;
                cstr1 = v1.value.varchar_value;
              }
            }
          }
        }
        if (ck == ProgramShape::CaseCondKind::kNone && cond) {
          const auto terms = SplitConjuncts(cond);
          bool valid = !terms.empty() && terms.size() <= 2;
          for (const auto& term : terms) {
            if (!valid || term->Type() != TypeTag::kBinaryExp) {
              valid = false;
              break;
            }
            const auto& comparison = term->AsBinaryExpression();
            const auto op = comparison.Op();
            if (op != BinaryOperation::kEquals &&
                op != BinaryOperation::kLessThan &&
                op != BinaryOperation::kLessThanEquals &&
                op != BinaryOperation::kGreaterThan &&
                op != BinaryOperation::kGreaterThanEquals) {
              valid = false;
              break;
            }
            auto constant =
                comparison.Right() &&
                        comparison.Right()->Type() == TypeTag::kConstantValue
                    ? std::optional<Value>(
                          comparison.Right()->AsConstantValue().GetValue())
                    : TryDecideConstantConjunct(comparison.Right());
            const bool is_date_cmp =
                constant.has_value() && constant->type == ValueType::kDate;
            if (!constant ||
                (!is_date_cmp && (constant->type != ValueType::kInt64 ||
                                  constant->IsUnsigned()))) {
              valid = false;
              break;
            }
            Expression lhs = comparison.Left(), rhs;
            if (is_date_cmp && lhs && lhs->Type() == TypeTag::kCastExp &&
                lhs->AsCastExpression().TargetValueType() == ValueType::kDate) {
              lhs = lhs->AsCastExpression().Child();
            }
            if (!is_date_cmp && lhs && lhs->Type() == TypeTag::kBinaryExp &&
                lhs->AsBinaryExpression().Op() == BinaryOperation::kSubtract) {
              rhs = lhs->AsBinaryExpression().Right();
              lhs = lhs->AsBinaryExpression().Left();
            }
            const auto column = [&](const Expression& e) -> int32_t {
              if (!e || e->Type() != TypeTag::kColumnValue) {
                return -1;
              }
              auto offset = LocalColumnOffset(
                  *schema, e->AsColumnValue().GetColumnName());
              if (!offset) {
                return -1;
              }
              const Column& sc = schema->GetColumn(*offset);
              if (is_date_cmp) {
                if (sc.Type() != ValueType::kDate) {
                  return -1;
                }
              } else if (sc.Type() != ValueType::kInt64 || sc.IsUnsigned()) {
                return -1;
              }
              return static_cast<int32_t>(*offset);
            };
            const int32_t left_col = column(lhs);
            const int32_t right_col = column(rhs);
            if (left_col < 0 || (rhs && right_col < 0)) {
              valid = false;
              break;
            }
            shape.case_int_comparisons.push_back({left_col, right_col, op,
                                                  constant->value.int_value,
                                                  is_date_cmp});
          }
          if (valid) {
            ck = ProgramShape::CaseCondKind::kIntComparisons;
          } else {
            shape.case_int_comparisons.clear();
          }
        }
        ProgramShape::CaseValKind vk = ProgramShape::CaseValKind::kNone;
        int32_t vcol0 = -1;
        int32_t vcol1 = -1;
        double vconst = 1.0;
        std::shared_ptr<const BytecodeProgram> vprog_shared;
        if (ck != ProgramShape::CaseCondKind::kNone && val) {
          // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
          //   (count_case_one_else_null_sound)
          if ((else_zero_int || ev.IsNull()) &&
              val->Type() == TypeTag::kConstantValue) {
            const Value& tv = val->AsConstantValue().GetValue();
            if (!tv.IsNull() && tv.type == ValueType::kInt64 &&
                !tv.IsUnsigned() && tv.value.int_value == 1) {
              vk = ProgramShape::CaseValKind::kIntConst1;
            }
          } else if (val->Type() == TypeTag::kColumnValue) {
            const auto voff = LocalColumnOffset(
                *schema, val->AsColumnValue().GetColumnName());
            if (voff.has_value()) {
              const Column& vc = schema->GetColumn(*voff);
              if (vc.Type() == ValueType::kDouble) {
                vk = ProgramShape::CaseValKind::kDirectDoubleCol;
                vcol0 = static_cast<int32_t>(*voff);
              } else if ((else_zero_int || ev.IsNull()) &&
                         vc.Type() == ValueType::kInt64 && !vc.IsUnsigned()) {
                vk = ProgramShape::CaseValKind::kDirectInt64Col;
                vcol0 = static_cast<int32_t>(*voff);
              }
            }
          } else if (val->Type() == TypeTag::kBinaryExp) {
            std::optional<BytecodeProgram> vprog =
                BytecodeCompiler::CompileRowCached(
                    val, *schema, BytecodeCompiler::Context::kValue);
            if (vprog.has_value()) {
              const ProgramShape vs = ClassifyProgram(*vprog);
              if (vs.mul_one_minus_col0 >= 0) {
                vk = ProgramShape::CaseValKind::kMulOneMinus;
                vcol0 = vs.mul_one_minus_col0;
                vcol1 = vs.mul_one_minus_col1;
                vconst = vs.mul_one_minus_const;
              } else if (vprog->ResultType() == ValueType::kDouble) {
                size_t d_depth = 0;
                bool d_ok = true;
                for (const auto& inst : vprog->Instructions()) {
                  if (inst.opcode == BytecodeOp::kLoadColumn) {
                    if (inst.operand < schema->ColumnCount()) {
                      const Column& col = schema->GetColumn(inst.operand);
                      d_ok = d_ok && (col.Type() == ValueType::kDouble ||
                                      (col.Type() == ValueType::kInt64 &&
                                       !col.IsUnsigned()));
                    } else {
                      d_ok = false;
                    }
                    ++d_depth;
                  } else if (inst.opcode == BytecodeOp::kLoadConstant) {
                    const Value& kv = vprog->Constants()[inst.operand];
                    d_ok = d_ok && !kv.IsNull() && !kv.IsUnsigned() &&
                           (kv.type == ValueType::kDouble ||
                            kv.type == ValueType::kInt64);
                    ++d_depth;
                  } else if (inst.opcode == BytecodeOp::kBinaryDouble &&
                             d_depth >= 2 &&
                             (inst.binary == BinaryOperation::kAdd ||
                              inst.binary == BinaryOperation::kSubtract ||
                              inst.binary == BinaryOperation::kMultiply ||
                              inst.binary == BinaryOperation::kDivide)) {
                    --d_depth;
                  } else {
                    d_ok = false;
                  }
                  if (!d_ok || d_depth > 16) {
                    d_ok = false;
                    break;
                  }
                }
                if (d_ok && d_depth == 1) {
                  vk = ProgramShape::CaseValKind::kDoubleArith;
                  vprog_shared =
                      std::make_shared<BytecodeProgram>(std::move(*vprog));
                }
              }
            }
          }
        }
        if (ck != ProgramShape::CaseCondKind::kNone &&
            vk != ProgramShape::CaseValKind::kNone) {
          shape.case_cond = ck;
          shape.case_val = vk;
          shape.case_else_null = ev.IsNull();
          shape.case_cond_col = ccol;
          shape.case_cond_int = cint;
          shape.case_cond_str0 = cstr0;
          shape.case_cond_str1 = cstr1;
          shape.case_val_col0 = vcol0;
          shape.case_val_col1 = vcol1;
          shape.case_val_const = vconst;
          shape.case_val_program = std::move(vprog_shared);
        }
      }
    }
  }
  return shape;
}

// Column cells referenced by a program's loads (the scratch-mask copy set).
void CollectLoadCells(const BytecodeProgram& program,
                      std::vector<slot_t>* cells) {
  for (const BytecodeInstruction& instruction : program.Instructions()) {
    if (instruction.opcode == BytecodeOp::kLoadColumn) {
      cells->push_back(instruction.operand);
    }
  }
}

StatusOr<Value> EvaluateProgramValue(const BytecodeProgram& program,
                                     const ProgramShape& shape,
                                     const Row& scratch) {
  if (shape.direct_column >= 0) {
    return scratch[static_cast<slot_t>(shape.direct_column)];
  }
  if (shape.like_column >= 0) {
    const Value& cell = scratch[static_cast<slot_t>(shape.like_column)];
    // Collated operands keep the canonical verdict (case folding and the
    // '_' collation raise live in the evaluator); NULL propagates as the
    // comparison NULL it is, so both fall through to the program.
    if (!cell.IsNull() && cell.type == ValueType::kVarChar &&
        cell.Collation() == 0 && !cell.IsCaseInsensitive()) {
      const bool matched = Like(cell.value.varchar_value, shape.like_pattern);
      return Value(shape.like_negated ? !matched : matched);
    }
  }
  if (shape.fast) {
    Value out;
    Status error{Status::kSuccess};
    switch (program.TryEvaluateRowFast(scratch, &out, &error)) {
      case BytecodeProgram::FastEval::kDone:
        return out;
      case BytecodeProgram::FastEval::kError:
        return error;
      case BytecodeProgram::FastEval::kUnsupported:
        break;
    }
  }
  return program.TryEvaluateRow(scratch);
}

// A scalar aggregate over NULL tests depends on a small nullness signature,
// not on the joined payload. Count signatures during a FULL join, preserving
// duplicate matches and each unmatched build/probe row. Only nonnegative
// integer SUM inputs qualify, so folding repeated inputs cannot hide overflow.
StatusOr<std::optional<Relation>> TryNullSignatureFullAggregate(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<Expression>& predicates, const Scope* outer,
    const CteMap& ctes, const GroupedJoinFusionSpec& fusion) {
  if (!fusion.group_keys.empty() || fusion.having ||
      fusion.post_having_active || fusion.rollup_count_distribution ||
      left.HasSpill() || right.HasSpill() || left.using_columns ||
      right.using_columns || right.rows.size() >= UINT32_MAX) {
    return std::optional<Relation>{};
  }
  const Schema schema = left.schema + right.schema;
  const auto keys = EqualityKeys(left.schema, right.schema, predicates);
  if (keys.empty() || keys.size() > CompositeIntKey::kMaxParts ||
      HasMixedKeySignednessOrType(left, right, keys) ||
      !ResidualJoinPredicates(left.schema, right.schema, predicates).empty()) {
    return std::optional<Relation>{};
  }
  std::vector<slot_t> left_keys, right_keys;
  right_keys.reserve(keys.size());
  for (const auto& key : keys) {
    const auto type = left.schema.GetColumn(key.left).Type();
    if (key.null_safe || key.left_offset != 0 || key.right_offset != 0 ||
        type != right.schema.GetColumn(key.right).Type() ||
        (type != ValueType::kInt64 && type != ValueType::kDate)) {
      return std::optional<Relation>{};
    }
    left_keys.push_back(static_cast<slot_t>(key.left));
    right_keys.push_back(static_cast<slot_t>(key.right));
  }
  std::vector<slot_t> null_columns;
  const auto null_expression = [&](const auto& self,
                                   const Expression& expression) -> bool {
    if (!expression || ContainsParameterSlot(expression)) {
      return false;
    }
    if (expression->Type() == TypeTag::kConstantValue) {
      const auto& value = expression->AsConstantValue().GetValue();
      return value.IsNull() ||
             (value.type == ValueType::kInt64 && !value.IsUnsigned() &&
              value.value.int_value >= 0);
    }
    if (expression->Type() == TypeTag::kUnaryExp) {
      const auto& unary = expression->AsUnaryExpression();
      if (unary.Op() == UnaryOperation::kNot) {
        return self(self, unary.Child());
      }
      if ((unary.Op() != UnaryOperation::kIsNull &&
           unary.Op() != UnaryOperation::kIsNotNull) ||
          !unary.Child() || unary.Child()->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const auto slot = LocalColumnOffset(
          schema, unary.Child()->AsColumnValue().GetColumnName());
      if (!slot) {
        return false;
      }
      if (std::ranges::find(null_columns, *slot) == null_columns.end()) {
        null_columns.push_back(static_cast<slot_t>(*slot));
      }
      return null_columns.size() <= 8;
    }
    if (expression->Type() == TypeTag::kBinaryExp) {
      const auto& binary = expression->AsBinaryExpression();
      return (binary.Op() == BinaryOperation::kAnd ||
              binary.Op() == BinaryOperation::kOr) &&
             self(self, binary.Left()) && self(self, binary.Right());
    }
    if (expression->Type() == TypeTag::kCaseExp) {
      const auto& branch = expression->AsCaseExpression();
      for (const auto& [condition, value] : branch.when_clauses_) {
        if (!self(self, condition) || !self(self, value)) {
          return false;
        }
      }
      return !branch.else_clause_ || self(self, branch.else_clause_);
    }
    return false;
  };
  for (size_t i = 0; i < fusion.aggregates.size(); ++i) {
    const auto* aggregate = fusion.aggregates[i];
    if ((aggregate->GetType() != AggregationType::kSum &&
         aggregate->GetType() != AggregationType::kCount) ||
        aggregate->Distinct() ||
        (!fusion.count_star[i] &&
         !null_expression(null_expression, aggregate->Child()))) {
      return std::optional<Relation>{};
    }
  }
  // The ordinary grouped emitter can evaluate scalar expressions of the
  // finished aggregates; no input column may escape the aggregate boundary.
  const size_t signature_count = size_t{1} << null_columns.size();
  const size_t bytes = (right.rows.size() * 128) + (signature_count * 16);
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<Relation>{};
  }
  QueryMemoryCharge charge(bytes);
  const auto started = std::chrono::steady_clock::now();
  const size_t left_width = left.schema.ColumnCount();
  const auto signature = [&](const Row* lhs, const Row* rhs) {
    size_t mask = 0;
    for (size_t i = 0; i < null_columns.size(); ++i) {
      const size_t column = null_columns[i];
      const bool is_null =
          column < left_width
              ? lhs == nullptr || (*lhs)[column].IsNull()
              : rhs == nullptr || (*rhs)[column - left_width].IsNull();
      mask |= static_cast<size_t>(is_null) << i;
    }
    return mask;
  };
  FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash> buckets;
  buckets.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    if (!HasNullKey(row, right_keys)) {
      buckets.emplace(EncodeCompositeIntKey(row, right_keys), &row);
    }
  }
  std::vector<uint64_t> counts(signature_count, 0);
  std::vector<uint8_t> matched(right.rows.size(), 0);
  uint64_t comparisons = 0;
  for (const Row& row : left.rows) {
    bool found = false;
    if (!HasNullKey(row, left_keys)) {
      const auto [begin, end] =
          buckets.equal_range(EncodeCompositeIntKey(row, left_keys));
      for (auto iter = begin; iter != end; ++iter) {
        ++comparisons;
        ++counts[signature(&row, iter->second)];
        matched[static_cast<size_t>(iter->second - right.rows.data())] = 1;
        found = true;
      }
    }
    if (!found) {
      ++counts[signature(&row, nullptr)];
    }
  }
  for (size_t i = 0; i < right.rows.size(); ++i) {
    if (matched[i] == 0) {
      ++counts[signature(nullptr, &right.rows[i])];
    }
  }
  Row representative(std::vector<Value>(schema.ColumnCount(), Value(1)));
  Scope scope{.row = &representative, .schema = &schema, .outer = outer};
  AggregateResultMap finished;
  for (size_t i = 0; i < fusion.aggregates.size(); ++i) {
    const auto* aggregate = fusion.aggregates[i];
    const bool is_count = aggregate->GetType() == AggregationType::kCount;
    int64_t sum = 0;
    bool any = false;
    for (size_t mask = 0; mask < signature_count; ++mask) {
      if (counts[mask] == 0) {
        continue;
      }
      for (size_t c = 0; c < null_columns.size(); ++c) {
        representative.values_[null_columns[c]] =
            (mask & (size_t{1} << c)) != 0 ? Value() : Value(1);
      }
      Value value(1);
      if (!fusion.count_star[i]) {
        ASSIGN_OR_RETURN(
            Value, evaluated,
            TryEvaluate(aggregate->Child(), scope, nullptr, context, ctes));
        value = std::move(evaluated);
      }
      if (value.IsNull()) {
        continue;
      }
      const int64_t contribution = is_count ? 1 : value.value.int_value;
      int64_t increment = 0;
      if (counts[mask] > static_cast<uint64_t>(INT64_MAX) ||
          __builtin_mul_overflow(
              contribution, static_cast<int64_t>(counts[mask]), &increment) ||
          __builtin_add_overflow(sum, increment, &sum)) {
        return StatusError(StatusCode::kIsInfinity,
                           "integer overflow in aggregate");
      }
      any = true;
    }
    finished.emplace(aggregate, any || is_count ? Value(sum) : Value());
  }
  Row output;
  std::vector<Column> columns;
  columns.reserve(fusion.select_items.size());
  for (size_t i = 0; i < fusion.select_items.size(); ++i) {
    ASSIGN_OR_RETURN(
        Value, value,
        TryEvaluate(fusion.select_items[i], scope, &finished, context, ctes));
    columns.emplace_back(fusion.select_names[i], ValueTypeOf(value));
    columns.back().SetUnsigned(value.IsUnsigned());
    output.values_.push_back(std::move(value));
  }
  Relation result(context.execution_runtime());
  result.schema = Schema("", std::move(columns));
  result.hash_joins = left.hash_joins + right.hash_joins + 1;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons =
      left.join_comparisons + right.join_comparisons + comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  result.AddRow(std::move(output));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (auto* runtime = context.execution_runtime()) {
    ++runtime->grouped_join_fusions;
    runtime->join_ms += ElapsedMs(started);
  }
  return std::optional<Relation>(std::move(result));
}

}  // namespace tinylamb::relational_detail
