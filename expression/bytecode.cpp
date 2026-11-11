/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <cassert>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/encoder.hpp"
#include "common/exc_shim.hpp"
#include "executor/data_chunk.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

ValueType ValueTypeFor(const Type& type) {
  switch (type.GetType()) {
    case TypeTag::kInteger:
    case TypeTag::kBigInt:
      return ValueType::kInt64;
    case TypeTag::kDouble:
      return ValueType::kDouble;
    case TypeTag::kVarChar:
      return ValueType::kVarChar;
    case TypeTag::kDate:
      return ValueType::kDate;
    default:
      return ValueType::kNull;
  }
}

ExpressionRuleSet MakeValueRules(const ExpressionRuleSet& base_rules) {
  ExpressionRuleSet rules = base_rules;
  rules.Remove("inner_join_not_null_inference");
  // Truthiness-preserving collapses (x AND TRUE -> x, x OR x -> x,
  // NOT NOT x -> x, ...) are only sound where the result is consumed as a
  // predicate.  As a *value* the AST always normalizes these to
  // BOOLEAN (or NULL), while the collapsed operand may carry -1, "abc",
  // etc. (oracle-found: `(-1 OR -1)` -> AST TRUE, VM -1).
  for (const char* name :
       {"boolean_identity", "double_negation", "xor_boolean_identity",
        "and_idempotent", "or_idempotent", "absorption_and", "absorption_or",
        "absorption_and_reversed", "absorption_or_reversed",
        "factor_or_common_and", "boolean_filter_pullup",
        // Neutral-constant identities drop the 0/1.0 operand, which
        // erases the INT64<->DOUBLE promotion the AST applies for
        // mixed-type arithmetic (`CASE..THEN 1.0..END * i` stays IEEE
        // double in the reference but becomes a checked INT64 kernel
        // after the fold).  Where both sides are statically INT64 the
        // kernel is identical anyway, so dropping the fold only gives
        // up work the VM can do at full speed.
        "identity_add_zero", "identity_subtract_zero", "identity_multiply_one",
        "identity_divide_one"}) {
    rules.Remove(name);
  }
  return rules;
}

const ExpressionRuleSet& DefaultValueRules() {
  static const ExpressionRuleSet rules =
      MakeValueRules(ExpressionRuleSet::Default());
  return rules;
}

const ExpressionRuleSet& NotComparisonValueRules() {
  static const ExpressionRuleSet rules =
      MakeValueRules(NotComparisonFreeRules());
  return rules;
}

std::optional<BytecodeOp> BinaryOpcode(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
      return BytecodeOp::kBinaryInt64;
    case ValueType::kDouble:
      return BytecodeOp::kBinaryDouble;
    case ValueType::kVarChar:
      return BytecodeOp::kBinaryVarchar;
    case ValueType::kDate:
      return BytecodeOp::kBinaryDate;
    case ValueType::kArray:
    case ValueType::kNull:
      return std::nullopt;
  }
  return std::nullopt;
}

// DATE parts come directly from the calendar, including signed/wide years.
// VARCHAR retains the AST's string-slice/stoll parsing contract.
StatusOr<Value> TryExtractDatePart(const Value& value, uint16_t part) {
  if (value.IsNull()) {
    return Value();
  }
  if (value.IsInterval()) {
    return ExecuteFunction(part == 0   ? "extract_year"
                           : part == 1 ? "extract_month"
                                       : "extract_day",
                           {value});
  }
  if (value.type == ValueType::kDate) {
    if (value.value.int_value < kDateMinDays ||
        kDateMaxDays < value.value.int_value) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE value out of range");
    }
    const std::chrono::year_month_day ymd{std::chrono::sys_days{
        std::chrono::days{static_cast<int>(value.value.int_value)}}};
    if (part == 0) {
      return Value(static_cast<int64_t>(static_cast<int>(ymd.year())));
    }
    if (part == 1) {
      return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
    }
    return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
  }
  if (value.type != ValueType::kVarChar) {
    return StatusError(StatusCode::kInvalidArgument,
                       "EXTRACT requires DATE or STRING");
  }
  const std::string_view text(value.value.varchar_value);
  if (text.size() < 10) {
    return StatusError(StatusCode::kInvalidArgument, "invalid DATE value");
  }
  const size_t begin = part == 0 ? 0 : (part == 1 ? 5 : 8);
  const size_t length = part == 0 ? 4 : 2;
  const std::string_view digits = text.substr(begin, length);
  // std::stoll semantics: leading C-locale whitespace, optional sign, at
  // least one digit; anything else is std::invalid_argument, which the AST
  // maps to "invalid DATE value: <text>".  Slices are at most 4 characters,
  // so int64 overflow (stoll's out_of_range) is unreachable here.
  size_t index = 0;
  const auto is_space = [](char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' ||
           c == '\r';
  };
  while (index < digits.size() && is_space(digits[index])) {
    ++index;
  }
  bool negative = false;
  if (index < digits.size() && (digits[index] == '+' || digits[index] == '-')) {
    negative = digits[index] == '-';
    ++index;
  }
  if (index >= digits.size() || digits[index] < '0' || '9' < digits[index]) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid DATE value: " + std::string(text));
  }
  int64_t parsed = 0;
  while (index < digits.size() && '0' <= digits[index] &&
         digits[index] <= '9') {
    parsed = (parsed * 10) + (digits[index] - '0');
    ++index;
  }
  return Value(negative ? -parsed : parsed);
}

bool CompileNode(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema,
    BytecodeProgram* program) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const int offset =
          schema.Offset(expression->AsColumnValue().GetColumnName());
      if (offset < 0 ||
          std::cmp_greater(offset, std::numeric_limits<uint16_t>::max())) {
        return false;
      }
      program->AddInstruction({.opcode = BytecodeOp::kLoadColumn,
                               .operand = static_cast<uint16_t>(offset)});
      return true;
    }
    case TypeTag::kConstantValue: {
      uint16_t offset = 0;
      if (!program->AddConstant(expression->AsConstantValue().GetValue(),
                                &offset)) {
        return false;
      }
      program->AddInstruction(
          {.opcode = BytecodeOp::kLoadConstant, .operand = offset});
      return true;
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      // D7 (docs/design.md): AND/OR compile to short-circuit control flow so
      // the VM skips the right operand exactly when the AST does, keeping the
      // three-valued truth table AND the error-occurrence conditions
      // identical.  Layout (AND):
      //     <left>
      //     JumpIfFalse -> end     (left==FALSE: result FALSE, skip right)
      //     <right>
      //     Binary AND             (left TRUE/NULL: merge three-valued)
      //   end:
      // OR uses JumpIfTrue symmetrically.  The jump peeks the stack (does not
      // pop), so every path reaches `end` with exactly one value.
      if (binary.Op() == BinaryOperation::kAnd ||
          binary.Op() == BinaryOperation::kOr) {
        if (!CompileNode(binary.Left(), schema, program)) {
          return false;
        }
        const size_t jump_index = program->Size();
        program->AddInstruction({.opcode = binary.Op() == BinaryOperation::kAnd
                                               ? BytecodeOp::kJumpIfFalse
                                               : BytecodeOp::kJumpIfTrue});
        if (!CompileNode(binary.Right(), schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kBinaryInt64,
                                 .operand = 0,
                                 .binary = binary.Op()});
        // Patch: jump lands just past the merge instruction.
        program->SetJumpTarget(jump_index,
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(jump_index));
        return true;
      }
      if (!CompileNode(binary.Left(), schema, program) ||
          !CompileNode(binary.Right(), schema, program)) {
        return false;
      }
      const ValueType left_type =
          ValueTypeFor(binary.Left()->ResultType(schema));
      const ValueType right_type =
          ValueTypeFor(binary.Right()->ResultType(schema));
      if (binary.Op() == BinaryOperation::kLike ||
          binary.Op() == BinaryOperation::kNotLike) {
        if (left_type != ValueType::kVarChar ||
            right_type != ValueType::kVarChar) {
          return false;
        }
      }
      ValueType operand_type = left_type;
      if (operand_type == ValueType::kDouble ||
          right_type == ValueType::kDouble) {
        operand_type = ValueType::kDouble;
      }
      // Interval arithmetic (any VARCHAR operand under +,-,*,/,%) returns a
      // VARCHAR interval string from EvaluateBinary, but BinaryResultType
      // declares kBigInt for those shapes -- the program's static result
      // type would then mismatch the Value appended per row ("column vector
      // type mismatch").  Reject the shape so callers fall back to the AST
      // evaluator, the semantic ground truth.
      const bool arithmetic = binary.Op() == BinaryOperation::kAdd ||
                              binary.Op() == BinaryOperation::kSubtract ||
                              binary.Op() == BinaryOperation::kMultiply ||
                              binary.Op() == BinaryOperation::kDivide ||
                              binary.Op() == BinaryOperation::kModulo;
      if (arithmetic && (left_type == ValueType::kVarChar ||
                         right_type == ValueType::kVarChar)) {
        return false;
      }
      // A kNull static type means BOOLEAN (or genuinely untyped NULL) on
      // one side.  The AST treats numeric-with-bool arithmetic as a mixed
      // numeric promotion and evaluates it in IEEE DOUBLE (never raising
      // integer overflow), while a typed kernel here would add in the
      // operand's own domain and raise at a different boundary.
      if (arithmetic &&
          (left_type == ValueType::kNull || right_type == ValueType::kNull)) {
        return false;
      }
      const std::optional<BytecodeOp> opcode = BinaryOpcode(operand_type);
      if (!opcode.has_value()) {
        return false;
      }
      program->AddInstruction(
          {.opcode = *opcode, .operand = 0, .binary = binary.Op()});
      return true;
    }
    case TypeTag::kInExp: {
      // `x IN (a, b, ...)` compiles to the equivalent OR-of-equalities
      // chain.  The VM's binary instruction dispatches through the same
      // canonical evaluator the AST uses, so NULL and NaN semantics are
      // identical; any operand the compiler cannot lower (subqueries,
      // unsupported functions) fails compilation and the caller falls back
      // to the AST evaluator.
      const InExpression& in = expression->AsInExpression();
      // String IN additionally checks explicit collation conflicts; plain
      // equality bytecode cannot represent that check. Keep it on the AST.
      const auto numeric_or_date = [&](const Expression& operand) {
        const auto type = ValueTypeFor(operand->ResultType(schema));
        return type == ValueType::kInt64 || type == ValueType::kDouble ||
               type == ValueType::kDate;
      };
      if (in.list_.empty() || !numeric_or_date(in.child_) ||
          !std::ranges::all_of(in.list_, numeric_or_date)) {
        return false;
      }
      Expression expanded = BinaryExpressionExp(
          in.child_, BinaryOperation::kEquals, in.list_.back());
      for (size_t i = in.list_.size() - 1; i-- > 0;) {
        expanded = BinaryExpressionExp(
            BinaryExpressionExp(in.child_, BinaryOperation::kEquals,
                                in.list_[i]),
            BinaryOperation::kOr, expanded);
      }
      return CompileNode(expanded, schema, program);
    }
    case TypeTag::kCaseExp: {
      // Searched CASE lowers to chained conditional jumps.  Layout (the
      // conditional jump PEEKS the top, so each WHEN condition is popped
      // explicitly on both of its exits):
      //     cond_0; JumpIfTrue -> T0; Pop
      //     cond_1; JumpIfTrue -> T1; Pop
      //     <else | NULL constant>;  Jump -> END
      // T0: Pop; then_0; Jump -> END
      // T1: Pop; then_1; Jump -> END
      // END:
      // TryPickBranch semantics hold branch-for-branch: a NULL condition is
      // not taken (kJumpIfTrue only fires on TRUE), a raising condition
      // aborts the whole CASE in evaluation order, and no match yields
      // ELSE-or-NULL.  Branch types must unify (ResultType kInvalid fails
      // the final type check exactly as before).
      const CaseExpression& case_expression = expression->AsCaseExpression();
      if (case_expression.when_clauses_.empty()) {
        return false;
      }
      std::vector<size_t> then_jumps;
      std::vector<size_t> branch_ends;
      for (const auto& when : case_expression.when_clauses_) {
        if (!when.first || !when.second) {
          return false;
        }
        if (!CompileNode(when.first, schema, program)) {
          return false;
        }
        then_jumps.push_back(program->Size());
        program->AddInstruction({.opcode = BytecodeOp::kJumpIfTrue});
        program->AddInstruction({.opcode = BytecodeOp::kPop});
      }
      if (case_expression.else_clause_ != nullptr) {
        if (!CompileNode(case_expression.else_clause_, schema, program)) {
          return false;
        }
      } else {
        uint16_t offset = 0;
        if (!program->AddConstant(Value(), &offset)) {
          return false;
        }
        program->AddInstruction(
            {.opcode = BytecodeOp::kLoadConstant, .operand = offset});
      }
      const size_t end_jump = program->Size();
      program->AddInstruction({.opcode = BytecodeOp::kJump});
      for (size_t i = 0; i < case_expression.when_clauses_.size(); ++i) {
        // Jump target: relative from the jump instruction itself.
        program->SetJumpTarget(then_jumps[i],
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(then_jumps[i]));
        program->AddInstruction({.opcode = BytecodeOp::kPop});
        if (!CompileNode(case_expression.when_clauses_[i].second, schema,
                         program)) {
          return false;
        }
        branch_ends.push_back(program->Size());
        program->AddInstruction({.opcode = BytecodeOp::kJump});
      }
      program->SetJumpTarget(end_jump, static_cast<int32_t>(program->Size()) -
                                           static_cast<int32_t>(end_jump));
      for (const size_t branch_end : branch_ends) {
        program->SetJumpTarget(branch_end,
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(branch_end));
      }
      return true;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expression->AsUnaryExpression();
      if (!CompileNode(unary.Child(), schema, program)) {
        return false;
      }
      // The null-check / truthiness family is type-agnostic on both legs:
      // TryEvaluateUnary dispatches on the value itself and the fast
      // interpreter implements the same table for any slot type.
      switch (unary.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
          program->AddInstruction({.opcode = BytecodeOp::kUnaryInt64,
                                   .operand = 0,
                                   .binary = BinaryOperation::kAdd,
                                   .unary = unary.Op()});
          return true;
        default:
          break;
      }
      const ValueType operand_type =
          ValueTypeFor(unary.Child()->ResultType(schema));
      if (operand_type != ValueType::kInt64 &&
          operand_type != ValueType::kDouble) {
        return false;
      }
      program->AddInstruction({.opcode = operand_type == ValueType::kDouble
                                             ? BytecodeOp::kUnaryDouble
                                             : BytecodeOp::kUnaryInt64,
                               .operand = 0,
                               .binary = BinaryOperation::kAdd,
                               .unary = unary.Op()});
      return true;
    }
    case TypeTag::kFunctionCallExp: {
      const FunctionCallExpression& call =
          expression->AsFunctionCallExpression();
      if (call.FuncName() == "byte_length" ||
          call.FuncName() == "octet_length" || call.FuncName() == "length") {
        if (call.Args().size() != 1 ||
            !CompileNode(call.Args()[0], schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kByteLength});
        return true;
      }
      if (call.FuncName() == "coalesce" || call.FuncName() == "ifnull") {
        if (call.Args().empty() ||
            (call.FuncName() == "ifnull" && call.Args().size() != 2)) {
          return false;
        }
        std::vector<size_t> exits;
        for (size_t i = 0; i < call.Args().size(); ++i) {
          if (!CompileNode(call.Args()[i], schema, program)) {
            return false;
          }
          if (i + 1 != call.Args().size()) {
            exits.push_back(program->Size());
            program->AddInstruction({.opcode = BytecodeOp::kJumpIfNotNull});
            program->AddInstruction({.opcode = BytecodeOp::kPop});
          }
        }
        for (const auto jump : exits) {
          program->SetJumpTarget(jump, static_cast<int32_t>(program->Size()) -
                                           static_cast<int32_t>(jump));
        }
        return true;
      }
      // EXTRACT(year|month|day FROM x) lowers to one dedicated op whose
      // kernel reproduces the AST evaluator; every other function (and any
      // odd arity) fails compilation and the caller falls back to the AST
      // evaluator, the semantic ground truth.
      uint16_t part = 0;
      if (call.FuncName() == "extract_year") {
        part = 0;
      } else if (call.FuncName() == "extract_month") {
        part = 1;
      } else if (call.FuncName() == "extract_day") {
        part = 2;
      } else {
        return false;
      }
      if (call.Args().size() != 1) {
        return false;
      }
      if (!CompileNode(call.Args()[0], schema, program)) {
        return false;
      }
      program->AddInstruction(
          {.opcode = BytecodeOp::kExtractDatePart, .operand = part});
      return true;
    }
    default:
      return false;
  }
}

}  // namespace

namespace {

// The fast interpreter supports these opcodes, including control-flow jumps.
// VARCHAR/DATE operations delegate to the canonical scalar evaluator.
bool FastOpcode(BytecodeOp opcode) {
  switch (opcode) {
    case BytecodeOp::kLoadColumn:
    case BytecodeOp::kLoadConstant:
    case BytecodeOp::kBinaryInt64:
    case BytecodeOp::kBinaryDouble:
    case BytecodeOp::kBinaryVarchar:
    case BytecodeOp::kBinaryDate:
    case BytecodeOp::kUnaryInt64:
    case BytecodeOp::kUnaryDouble:
    case BytecodeOp::kExtractDatePart:
    case BytecodeOp::kJump:
    case BytecodeOp::kJumpIfFalse:
    case BytecodeOp::kJumpIfTrue:
    case BytecodeOp::kJumpIfNotNull:
    case BytecodeOp::kPop:
      return true;
    default:
      return false;
  }
}

// Compile-time under-approximation: every instruction is fast-shaped and
// every constant loaded is numeric (or NULL).  Programs built by hand
// (BytecodeProgram{} placeholders) never get the flag.
bool ProgramIsFastInterpretable(const BytecodeProgram& program) {
  if (program.Instructions().empty()) {
    return false;
  }
  for (const BytecodeInstruction& instruction : program.Instructions()) {
    if (!FastOpcode(instruction.opcode)) {
      return false;
    }
    if (instruction.opcode == BytecodeOp::kLoadConstant) {
      const Value& constant = program.Constants()[instruction.operand];
      switch (constant.type) {
        case ValueType::kNull:
        case ValueType::kInt64:
        case ValueType::kDouble:
          if (constant.IsUnsigned()) {
            return false;
          }
          break;
        case ValueType::kVarChar:
          // A collated string constant compares under folding rules the
          // fast slot cannot reproduce; keep those programs canonical.
          if (constant.Collation() != 0) {
            return false;
          }
          break;
        default:
          return false;
      }
    }
  }
  return true;
}

}  // namespace

namespace {
bool EncodeCompileTree(const Expression& expression, Encoder* encoder,
                       size_t* nodes, size_t depth = 0) {
  if (++*nodes > 2048 || depth > 128) {
    return false;
  }
  *encoder << static_cast<bool>(expression);
  if (!expression) {
    return true;
  }
  *encoder << static_cast<uint32_t>(expression->Type());
  const auto child = [&](const Expression& value) {
    return EncodeCompileTree(value, encoder, nodes, depth + 1);
  };
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      *encoder << expression->AsColumnValue().GetColumnName();
      return true;
    case TypeTag::kConstantValue: {
      const Value& value = expression->AsConstantValue().GetValue();
      if (value.type == ValueType::kArray ||
          (value.type == ValueType::kVarChar &&
           value.value.varchar_value.size() > 16384)) {
        return false;
      }
      const ExpressionBase& expr_base = *expression;
      if (typeid(expr_base) != typeid(ConstantValue)) {
        const auto evaluated = expression->TryEvaluate(Row(), Schema());
        if (!evaluated.HasValue() ||
            Encode(evaluated.Value()) != Encode(value) ||
            evaluated.Value().Collation() != value.Collation()) {
          return false;
        }
      }
      *encoder << value << value.Collation();
      return true;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      *encoder << static_cast<uint32_t>(binary.Op());
      return child(binary.Left()) && child(binary.Right());
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      *encoder << static_cast<uint32_t>(unary.Op());
      return child(unary.Child());
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      *encoder << static_cast<uint64_t>(in.list_.size());
      if (!child(in.child_)) {
        return false;
      }
      return std::ranges::all_of(
          in.list_, [&](const auto& value) { return child(value); });
    }
    case TypeTag::kCaseExp: {
      const auto& conditional = expression->AsCaseExpression();
      *encoder << conditional.IsFromIf()
               << static_cast<uint64_t>(conditional.when_clauses_.size());
      for (const auto& clause : conditional.when_clauses_) {
        if (!child(clause.first) || !child(clause.second)) {
          return false;
        }
      }
      return child(conditional.else_clause_);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      if (call.FuncName() != "coalesce" && call.FuncName() != "ifnull" &&
          call.FuncName() != "byte_length" &&
          call.FuncName() != "octet_length" && call.FuncName() != "length") {
        return false;
      }
      *encoder << call.FuncName() << static_cast<uint64_t>(call.Args().size());
      return std::ranges::all_of(call.Args(), child);
    }
    default:
      // Functions, subqueries, aggregates and other context-bearing nodes
      // keep the ordinary compiler; none enter the retained cache.
      return false;
  }
}
}  // namespace

std::optional<std::string> ScalarExpressionCacheKey(
    const std::vector<Expression>& expressions, const Schema& schema) {
  std::string key;
  key.reserve(512);
  Encoder encoder(key);
  encoder << schema << static_cast<uint64_t>(expressions.size());
  size_t nodes = 0;
  for (const auto& expression : expressions) {
    if (!EncodeCompileTree(expression, &encoder, &nodes)) {
      return std::nullopt;
    }
  }
  if (key.size() > 16384) {
    return std::nullopt;
  }
  return key;
}

Expression FreezeScalarBindings(const Expression& expression) {
  if (!expression) {
    return nullptr;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    return ConstantValueExp(expression->AsConstantValue().GetValue());
  }
  auto children = ExpressionChildren(expression);
  if (children.empty()) {
    return expression;
  }
  for (auto& child : children) {
    child = FreezeScalarBindings(child);
  }
  return WithExpressionChildren(expression, std::move(children));
}

std::optional<BytecodeProgram> BytecodeCompiler::CompileRowCached(
    const Expression& expression, const Schema& schema, Context context) {
  if (!expression) {
    return std::nullopt;
  }
  std::string key;
  key.reserve(512);
  Encoder encoder(key);
  encoder << static_cast<uint8_t>(context) << schema;
  size_t nodes = 0;
  if (!EncodeCompileTree(expression, &encoder, &nodes)) {
    return Compile(expression, schema, context);
  }
  if (key.size() > 16384) {
    return Compile(expression, schema, context);
  }
  struct Cache {
    size_t bytes{0};
    std::unordered_map<std::string, std::optional<BytecodeProgram>> entries;
  };
  static thread_local Cache cache;
  if (auto found = cache.entries.find(key); found != cache.entries.end()) {
    return found->second;
  }
  auto program = Compile(expression, schema, context);
  if (!program && expression &&
      (expression->Type() == TypeTag::kCaseExp ||
       expression->Type() == TypeTag::kFunctionCallExp)) {
    // CompileNode emits the same branch dispatch as the AST without requiring
    // one static output vector type. Keep each branch's returned Value intact.
    BytecodeProgram dynamic;
    if (CompileNode(expression, schema, &dynamic)) {
      dynamic.SetResultType(ValueType::kNull);
      dynamic.SetFastInterpretable(ProgramIsFastInterpretable(dynamic));
      program = std::move(dynamic);
    }
  }
  size_t bytes = key.capacity() + 128;
  if (program) {
    bytes +=
        (program->Instructions().capacity() * sizeof(BytecodeInstruction)) +
        (program->Constants().capacity() * sizeof(Value));
    for (const auto& value : program->Constants()) {
      if (value.type == ValueType::kVarChar) {
        bytes += (2 * value.value.varchar_value.size()) + 32;
      }
    }
  }
  constexpr size_t kLimit = size_t{2} * 1024 * 1024;
  if (bytes > kLimit) {
    return program;
  }
  if (cache.entries.size() >= 256 || cache.bytes > kLimit - bytes) {
    cache.entries.clear();
    cache.bytes = 0;
  }
  cache.bytes += bytes;
  cache.entries.emplace(std::move(key), program);
  return program;
}

std::optional<BytecodeProgram> BytecodeCompiler::Compile(
    const Expression& expression, const Schema& schema, Context context) {
  try {
    // Mirror the optimizer's split: the not-inference rule turns NULL into
    // FALSE, so it is only sound in filter contexts (see plan/optimizer.cpp).
    const ExpressionRuleSet& base_rules =
        ContainsNotOfOrderedDoubleComparison(expression, schema)
            ? NotComparisonFreeRules()
            : ExpressionRuleSet::Default();
    const ExpressionRuleSet* rules = &base_rules;
    if (context == Context::kValue) {
      rules = (&base_rules) == &ExpressionRuleSet::Default()
                  ? &DefaultValueRules()
                  : &NotComparisonValueRules();
    }
    ExpressionRewriter rewriter(*rules);

    rewriter.set_schema(&schema);
    StatusOr<Expression> folded_or = rewriter.TryRewrite(expression);
    if (!folded_or.HasValue()) {
      return std::nullopt;
    }
    const Expression folded = folded_or.MoveValue();
    BytecodeProgram program;
    if (!CompileNode(folded, schema, &program)) {
      return std::nullopt;
    }
    const auto uniform_coalesce = [&](const auto& self,
                                      const Expression& node) -> bool {
      if (!node) {
        return true;
      }
      if (node->Type() == TypeTag::kFunctionCallExp) {
        const auto& call = node->AsFunctionCallExpression();
        if (call.FuncName() == "coalesce" || call.FuncName() == "ifnull") {
          ValueType common = ValueType::kNull;
          for (const auto& arg : call.Args()) {
            const auto type = ValueTypeFor(arg->ResultType(schema));
            if (type == ValueType::kNull) {
              if (arg->Type() != TypeTag::kConstantValue ||
                  !arg->AsConstantValue().GetValue().IsNull()) {
                return false;
              }
              continue;
            }
            if (common != ValueType::kNull && common != type) {
              return false;
            }
            common = type;
          }
        }
      }
      return std::ranges::all_of(
          ExpressionChildren(node),
          [&](const auto& child) { return self(self, child); });
    };
    if (!uniform_coalesce(uniform_coalesce, folded)) {
      return std::nullopt;
    }
    const ValueType result_type = ValueTypeFor(folded->ResultType(schema));
    if (result_type == ValueType::kNull) {
      return std::nullopt;
    }
    program.SetResultType(result_type);
    program.SetFastInterpretable(ProgramIsFastInterpretable(program));
    return program;
  } catch (const std::exception&) {
    // Plans with partially qualified or deferred schemas remain valid.  An
    // unsupported bytecode shape must fall back to the expression tree.
    return std::nullopt;
  }
}

StatusOr<ColumnVector> BytecodeProgram::TryEvaluateBatch(
    const DataChunk& input) const {
  // Infer the value domain as well as the physical type: the static result
  // type alone cannot distinguish UINT64 from INT64.
  ColumnVector result(ValueType::kNull, input.Size());
  std::vector<Value> stack;
  stack.reserve(instructions_.size());
  for (size_t row = 0; row < input.Size(); ++row) {
    stack.clear();
    // D7: a program counter (not a range-for) so the short-circuit jumps can
    // skip the right operand; jump targets are relative to the jump itself.
    for (size_t pc = 0; pc < instructions_.size(); ++pc) {
      const BytecodeInstruction& instruction = instructions_[pc];
      switch (instruction.opcode) {
        case BytecodeOp::kLoadColumn:
          stack.push_back(input.ColumnAt(instruction.operand).ValueAt(row));
          break;
        case BytecodeOp::kLoadConstant:
          stack.push_back(constants_[instruction.operand]);
          break;
        case BytecodeOp::kJumpIfFalse:
        case BytecodeOp::kJumpIfTrue: {
          assert(!stack.empty());
          const Value& top = stack.back();
          const bool decides = instruction.opcode == BytecodeOp::kJumpIfFalse
                                   ? (!top.IsNull() && !top.Truthy())
                                   : (!top.IsNull() && top.Truthy());
          if (decides) {
            // The AST normalizes a short-circuit result to BOOLEAN
            // (Value(op == kOr)); replace the raw truthy operand so e.g.
            // `(-1 OR x)` yields TRUE, not -1.
            stack.back() = Value(instruction.opcode == BytecodeOp::kJumpIfTrue);
            pc += static_cast<size_t>(instruction.jump_target);
            --pc;  // the for-loop ++pc lands on the target.
          }
          break;
        }
        case BytecodeOp::kJumpIfNotNull:
          if (!stack.back().IsNull()) {
            pc += static_cast<size_t>(instruction.jump_target);
            --pc;
          }
          break;
        case BytecodeOp::kJump:
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
          break;
        case BytecodeOp::kBinaryInt64:
        case BytecodeOp::kBinaryDouble:
        case BytecodeOp::kBinaryVarchar:
        case BytecodeOp::kBinaryDate: {
          // Only compiler-generated programs reach here; an empty stack means
          // a malformed program.
          assert(!stack.empty());
          Value right = std::move(stack.back());
          stack.pop_back();
          assert(!stack.empty());
          Value left = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(
              Value, combined,
              (TryEvaluateBinary(instruction.binary, left, right)));
          stack.push_back(std::move(combined));
          break;
        }
        case BytecodeOp::kUnaryInt64:
        case BytecodeOp::kUnaryDouble: {
          assert(!stack.empty());
          Value child = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(Value, result_value,
                           (TryEvaluateUnary(instruction.unary, child)));
          stack.push_back(std::move(result_value));
          break;
        }
        case BytecodeOp::kExtractDatePart: {
          assert(!stack.empty());
          Value child = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(Value, part_value,
                           TryExtractDatePart(child, instruction.operand));
          stack.push_back(std::move(part_value));
          break;
        }
        case BytecodeOp::kByteLength: {
          assert(!stack.empty());
          const auto& child = stack.back();
          const Value result_value =
              child.IsNull() ? Value()
                             : Value(static_cast<int64_t>(
                                   child.type == ValueType::kVarChar
                                       ? child.value.varchar_value.size()
                                       : child.AsString().size()));
          stack.back() = result_value;
          break;
        }
        case BytecodeOp::kPop: {
          assert(!stack.empty());
          stack.pop_back();
          break;
        }
      }
    }
    if (stack.size() != 1) {
      return StatusError(StatusCode::kRuntimeError, "invalid bytecode stack");
    }
    result.Append(stack.back());
  }
  return result;
}

ColumnVector BytecodeProgram::EvaluateBatch(const DataChunk& input) const {
  return ExcShimUnwrap(TryEvaluateBatch(input),
                       "BytecodeProgram::EvaluateBatch");
}

StatusOr<Value> BytecodeProgram::TryEvaluateRow(const Row& row) const {
  // The interpreter has no call instruction, so evaluation cannot re-enter
  // this function; reusing one stack per thread removes a heap allocation
  // per row on the hot aggregation/filter paths.
  thread_local std::vector<Value> stack;
  stack.clear();
  stack.reserve(instructions_.size());
  for (size_t pc = 0; pc < instructions_.size(); ++pc) {
    const BytecodeInstruction& instruction = instructions_[pc];
    switch (instruction.opcode) {
      case BytecodeOp::kLoadColumn:
        stack.push_back(row[instruction.operand]);
        break;
      case BytecodeOp::kLoadConstant:
        stack.push_back(constants_[instruction.operand]);
        break;
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        assert(!stack.empty());
        const Value& top = stack.back();
        const bool decides = instruction.opcode == BytecodeOp::kJumpIfFalse
                                 ? (!top.IsNull() && !top.Truthy())
                                 : (!top.IsNull() && top.Truthy());
        if (decides) {
          // Match the AST: a short-circuit AND/OR evaluates to a normalized
          // BOOLEAN, never the raw truthy operand value.
          stack.back() = Value(instruction.opcode == BytecodeOp::kJumpIfTrue);
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        break;
      }
      case BytecodeOp::kJumpIfNotNull:
        if (!stack.back().IsNull()) {
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        break;
      case BytecodeOp::kJump:
        pc += static_cast<size_t>(instruction.jump_target);
        --pc;
        break;
      case BytecodeOp::kBinaryInt64:
      case BytecodeOp::kBinaryDouble:
      case BytecodeOp::kBinaryVarchar:
      case BytecodeOp::kBinaryDate: {
        assert(!stack.empty());
        Value right = std::move(stack.back());
        stack.pop_back();
        assert(!stack.empty());
        Value left = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, combined,
                         (TryEvaluateBinary(instruction.binary, left, right)));
        stack.push_back(std::move(combined));
        break;
      }
      case BytecodeOp::kUnaryInt64:
      case BytecodeOp::kUnaryDouble: {
        assert(!stack.empty());
        Value child = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, result_value,
                         (TryEvaluateUnary(instruction.unary, child)));
        stack.push_back(std::move(result_value));
        break;
      }
      case BytecodeOp::kExtractDatePart: {
        assert(!stack.empty());
        Value child = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, part_value,
                         TryExtractDatePart(child, instruction.operand));
        stack.push_back(std::move(part_value));
        break;
      }
      case BytecodeOp::kByteLength: {
        assert(!stack.empty());
        const auto& child = stack.back();
        const Value result_value =
            child.IsNull() ? Value()
                           : Value(static_cast<int64_t>(
                                 child.type == ValueType::kVarChar
                                     ? child.value.varchar_value.size()
                                     : child.AsString().size()));
        stack.back() = result_value;
        break;
      }
      case BytecodeOp::kPop: {
        assert(!stack.empty());
        stack.pop_back();
        break;
      }
    }
  }
  if (stack.size() != 1) {
    return StatusError(StatusCode::kRuntimeError, "invalid bytecode stack");
  }
  return std::move(stack.back());
}

Value BytecodeProgram::EvaluateRow(const Row& row) const {
  return ExcShimUnwrap(TryEvaluateRow(row), "BytecodeProgram::EvaluateRow");
}

namespace {

// One boxing-free stack slot: the raw payload plus the runtime type.  Doubles
// ride in the same int64 payload through memcpy on push/pop.  VARCHAR
// operands carry a view into the row cell (or program constant) so string
// compares avoid Value construction; collated strings never reach this slot.
struct FastSlot {
  int64_t bits;
  ValueType type;
  bool is_null;
  std::string_view text;
};

bool FastTruthy(const FastSlot& slot) {
  if (slot.is_null) {
    return false;
  }
  if (slot.type == ValueType::kInt64) {
    return slot.bits != 0;
  }
  if (slot.type == ValueType::kDouble) {
    double d = 0.0;
    std::memcpy(&d, &slot.bits, sizeof(d));
    return d != 0.0;
  }
  return true;
}

// Int64 operands promote to double on the double opcode (the compiler emits
// kBinaryDouble for mixed-operand shapes).
void FastSlotToDouble(const FastSlot& slot, double* out) {
  if (slot.type == ValueType::kDouble) {
    std::memcpy(out, &slot.bits, sizeof(double));
  } else {
    *out = static_cast<double>(slot.bits);
  }
}

FastSlot FastSlotFromValue(const Value& value, bool* unsupported) {
  FastSlot slot{
      .bits = 0, .type = value.type, .is_null = value.IsNull(), .text = {}};
  switch (value.type) {
    case ValueType::kInt64:
    case ValueType::kDate:
      if (value.IsUnsigned()) {
        *unsupported = true;
        break;
      }
      slot.bits = value.value.int_value;
      break;
    case ValueType::kDouble:
      std::memcpy(&slot.bits, &value.value.double_value, sizeof(double));
      break;
    case ValueType::kVarChar:
      // A collated (case-insensitive) string compares under folding rules the
      // plain view cannot reproduce; those programs stay on the canonical VM.
      if (value.Collation() != 0) {
        *unsupported = true;
        break;
      }
      slot.text = value.value.varchar_value;
      break;
    case ValueType::kNull:
      break;
    default:
      *unsupported = true;
      break;
  }
  return slot;
}

Value FastValueFromSlot(const FastSlot& slot) {
  if (slot.is_null) {
    return {};
  }
  switch (slot.type) {
    case ValueType::kInt64:
      // Value's numeric constructors are explicit, so the braced form the
      // check prefers does not compile here.
      return Value(slot.bits);  // NOLINT(modernize-return-braced-init-list)
    case ValueType::kDouble: {
      double d = 0.0;
      std::memcpy(&d, &slot.bits, sizeof(d));
      return Value(d);  // NOLINT(modernize-return-braced-init-list)
    }
    case ValueType::kDate:
      return Value::DateFromDays(slot.bits);
    case ValueType::kVarChar: {
      // Plain view-carrying value (collation 0): no owned copy.
      Value string_value;
      string_value.type = ValueType::kVarChar;
      string_value.value.varchar_value = slot.text;
      return string_value;
    }
    default:
      return {};
  }
}

}  // namespace

BytecodeProgram::FastEval BytecodeProgram::TryEvaluateRowFast(
    const Row& row, Value* out, Status* error) const {
  if (!fast_interpretable_) {
    return FastEval::kUnsupported;
  }
  // Column projections and NULL tests need no VM stack or payload decoding.
  // Preserve the actual Value domain, including unsigned integers and arrays.
  if (!instructions_.empty() && instructions_.size() <= 3 &&
      instructions_[0].opcode == BytecodeOp::kLoadColumn &&
      instructions_[0].operand < row.Count()) {
    const Value& value = row[instructions_[0].operand];
    if (instructions_.size() == 1) {
      *out = value;
      return FastEval::kDone;
    }
    const auto& test = instructions_[1];
    const bool negate = instructions_.size() == 3 &&
                        instructions_[2].opcode == BytecodeOp::kUnaryInt64 &&
                        instructions_[2].unary == UnaryOperation::kNot;
    if ((instructions_.size() == 2 || negate) &&
        test.opcode == BytecodeOp::kUnaryInt64 &&
        (test.unary == UnaryOperation::kIsNull ||
         test.unary == UnaryOperation::kIsNotNull)) {
      const bool result = test.unary == UnaryOperation::kIsNull
                              ? value.IsNull()
                              : !value.IsNull();
      *out = Value(static_cast<int64_t>(result != negate));
      return FastEval::kDone;
    }
  }
  // The interpreter has no call instruction; one stack per thread removes a
  // heap allocation per row (same trick as TryEvaluateRow).
  thread_local std::vector<FastSlot> stack;
  stack.clear();
  stack.reserve(instructions_.size());
  const auto push_null = [](std::vector<FastSlot>& s) {
    s.push_back(FastSlot{
        .bits = 0, .type = ValueType::kNull, .is_null = true, .text = {}});
  };
  const auto push_int = [](std::vector<FastSlot>& s, int64_t v) {
    s.push_back(FastSlot{
        .bits = v, .type = ValueType::kInt64, .is_null = false, .text = {}});
  };
  const auto push_double = [](std::vector<FastSlot>& s, double v) {
    FastSlot slot{
        .bits = 0, .type = ValueType::kDouble, .is_null = false, .text = {}};
    std::memcpy(&slot.bits, &v, sizeof(v));
    s.push_back(slot);
  };
  const auto push_error = [error](StatusCode code, const char* message) {
    *error = StatusError(code, message);
    return FastEval::kError;
  };
  // Indexed walk (not a range-for): the jump ops need pc arithmetic.
  for (size_t pc = 0; pc < instructions_.size(); ++pc) {
    const BytecodeInstruction& instruction = instructions_[pc];
    switch (instruction.opcode) {
      case BytecodeOp::kJumpIfNotNull:
        if (!stack.back().is_null) {
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        continue;
      case BytecodeOp::kJump:
        pc += static_cast<size_t>(instruction.jump_target);
        --pc;
        continue;
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        // Same contract as the canonical interpreter: a decided AND/OR
        // short-circuit replaces the operand with a normalized BOOLEAN
        // (INT64 0/1); an undecided jump leaves the operand in place.
        const FastSlot& top = stack.back();
        const bool decides = instruction.opcode == BytecodeOp::kJumpIfFalse
                                 ? (!top.is_null && !FastTruthy(top))
                                 : (!top.is_null && FastTruthy(top));
        if (decides) {
          stack.back() = FastSlot{
              .bits = instruction.opcode == BytecodeOp::kJumpIfTrue ? 1 : 0,
              .type = ValueType::kInt64,
              .is_null = false,
              .text = {}};
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        continue;
      }
      case BytecodeOp::kPop: {
        stack.pop_back();
        continue;
      }
      case BytecodeOp::kLoadColumn: {
        if (instruction.operand >= row.Count()) {
          return FastEval::kUnsupported;
        }
        const Value& cell = row[instruction.operand];
        bool unsupported = false;
        stack.push_back(FastSlotFromValue(cell, &unsupported));
        if (unsupported) {
          return FastEval::kUnsupported;
        }
        break;
      }
      case BytecodeOp::kLoadConstant: {
        bool unsupported = false;
        stack.push_back(
            FastSlotFromValue(constants_[instruction.operand], &unsupported));
        if (unsupported) {
          return FastEval::kUnsupported;
        }
        break;
      }
      case BytecodeOp::kBinaryInt64:
      case BytecodeOp::kBinaryDouble:
      case BytecodeOp::kBinaryVarchar:
      case BytecodeOp::kBinaryDate: {
        const FastSlot right = stack.back();
        stack.pop_back();
        const FastSlot left = stack.back();
        stack.pop_back();
        // Three-valued logical ops and IS [NOT] DISTINCT FROM resolve before
        // NULL propagation, exactly like the canonical dispatch.
        switch (instruction.binary) {
          case BinaryOperation::kAnd: {
            const bool lt = FastTruthy(left);
            const bool rt = FastTruthy(right);
            if ((!left.is_null && !lt) || (!right.is_null && !rt)) {
              push_int(stack, 0);
            } else if (left.is_null || right.is_null) {
              push_null(stack);
            } else {
              push_int(stack, 1);
            }
            continue;
          }
          case BinaryOperation::kOr: {
            const bool lt = FastTruthy(left);
            const bool rt = FastTruthy(right);
            if ((!left.is_null && lt) || (!right.is_null && rt)) {
              push_int(stack, 1);
            } else if (left.is_null || right.is_null) {
              push_null(stack);
            } else {
              push_int(stack, 0);
            }
            continue;
          }
          case BinaryOperation::kXor: {
            if (left.is_null || right.is_null) {
              push_null(stack);
            } else {
              push_int(stack, FastTruthy(left) != FastTruthy(right) ? 1 : 0);
            }
            continue;
          }
          case BinaryOperation::kIsDistinctFrom:
          case BinaryOperation::kIsNotDistinctFrom: {
            bool equal = false;
            if (left.is_null || right.is_null) {
              equal = left.is_null && right.is_null;
            } else if ((left.type != ValueType::kInt64 &&
                        left.type != ValueType::kDouble) ||
                       (right.type != ValueType::kInt64 &&
                        right.type != ValueType::kDouble)) {
              // Raw bits describe numeric equality only. Text lives in
              // `text`, and DATE must retain its distinct comparison domain.
              // Let the canonical VM handle these null-safe comparisons.
              return FastEval::kUnsupported;
            } else if (left.type == ValueType::kDouble ||
                       right.type == ValueType::kDouble) {
              double l = 0.0;
              double r = 0.0;
              FastSlotToDouble(left, &l);
              FastSlotToDouble(right, &r);
              equal = l == r || (std::isnan(l) && std::isnan(r));
            } else {
              equal = left.bits == right.bits;
            }
            push_int(stack, static_cast<int64_t>(
                                instruction.binary ==
                                        BinaryOperation::kIsNotDistinctFrom
                                    ? equal
                                    : !equal));
            continue;
          }
          default:
            break;
        }
        if (left.is_null || right.is_null) {
          push_null(stack);
          continue;
        }
        if (instruction.opcode == BytecodeOp::kBinaryInt64) {
          if (left.type != ValueType::kInt64 ||
              right.type != ValueType::kInt64) {
            return FastEval::kUnsupported;
          }
          switch (instruction.binary) {
            case BinaryOperation::kEquals:
              push_int(stack, static_cast<int64_t>(left.bits == right.bits));
              continue;
            case BinaryOperation::kNotEquals:
              push_int(stack, static_cast<int64_t>(left.bits != right.bits));
              continue;
            case BinaryOperation::kLessThan:
              push_int(stack, static_cast<int64_t>(left.bits < right.bits));
              continue;
            case BinaryOperation::kLessThanEquals:
              push_int(stack, static_cast<int64_t>(left.bits <= right.bits));
              continue;
            case BinaryOperation::kGreaterThan:
              push_int(stack, static_cast<int64_t>(left.bits > right.bits));
              continue;
            case BinaryOperation::kGreaterThanEquals:
              push_int(stack, static_cast<int64_t>(left.bits >= right.bits));
              continue;
            case BinaryOperation::kAdd:
            case BinaryOperation::kSubtract:
            case BinaryOperation::kMultiply: {
              int64_t result = 0;
              bool overflow = false;
              if (instruction.binary == BinaryOperation::kAdd) {
                overflow =
                    __builtin_add_overflow(left.bits, right.bits, &result);
              } else if (instruction.binary == BinaryOperation::kSubtract) {
                overflow =
                    __builtin_sub_overflow(left.bits, right.bits, &result);
              } else {
                overflow =
                    __builtin_mul_overflow(left.bits, right.bits, &result);
              }
              if (overflow) {
                return push_error(
                    StatusCode::kInvalidArgument,
                    instruction.binary == BinaryOperation::kAdd
                        ? "integer overflow on '+'"
                    : instruction.binary == BinaryOperation::kSubtract
                        ? "integer overflow on '-'"
                        : "integer overflow on '*'");
              }
              push_int(stack, result);
              continue;
            }
            case BinaryOperation::kDivide:
              // kDivide always divides in double (the type contract declares
              // it DOUBLE, and the canonical kDivide-numeric shortcut runs
              // before any integer kernel), so INT64_MIN/-1 is a large finite
              // double, not an overflow error.
              {
                const auto ld = static_cast<double>(left.bits);
                const auto rd = static_cast<double>(right.bits);
                if (rd == 0.0) {
                  return push_error(StatusCode::kIsInfinity,
                                    "division by zero");
                }
                const double result = ld / rd;
                if (std::isfinite(ld) && std::isfinite(rd) &&
                    std::isinf(result)) {
                  return push_error(StatusCode::kIsInfinity, "double overflow");
                }
                push_double(stack, result);
              }
              continue;
            case BinaryOperation::kModulo:
              if (right.bits == 0) {
                return push_error(StatusCode::kIsInfinity, "modulo by zero");
              }
              if (left.bits == std::numeric_limits<int64_t>::min() &&
                  right.bits == -1) {
                return push_error(StatusCode::kInvalidArgument,
                                  "integer overflow on '%'");
              }
              push_int(stack, left.bits % right.bits);
              continue;
            default:
              return FastEval::kUnsupported;
          }
        }
        // kBinaryVarchar / kBinaryDate: delegate to the canonical binary
        // dispatch so string ordering, LIKE matching, and date coercions are
        // instruction-for-instruction identical; the fast path only removes
        // the Value stack traffic around the call.  (Collated operands never
        // reach this opcode -- FastSlotFromValue refuses them.)
        if (instruction.opcode == BytecodeOp::kBinaryVarchar ||
            instruction.opcode == BytecodeOp::kBinaryDate) {
          StatusOr<Value> combined =
              TryEvaluateBinary(instruction.binary, FastValueFromSlot(left),
                                FastValueFromSlot(right));
          if (!combined.HasValue()) {
            *error = combined.GetStatus();
            return FastEval::kError;
          }
          bool result_unsupported = false;
          stack.push_back(
              FastSlotFromValue(combined.Value(), &result_unsupported));
          if (result_unsupported) {
            return FastEval::kUnsupported;
          }
          continue;
        }
        // kBinaryDouble: int64 operands promote to double (the compiler
        // picked this opcode for mixed-operand shapes too, so the runtime
        // types decide which canonical ladder the op takes).
        if ((left.type != ValueType::kInt64 &&
             left.type != ValueType::kDouble) ||
            (right.type != ValueType::kInt64 &&
             right.type != ValueType::kDouble)) {
          return FastEval::kUnsupported;
        }
        double l = 0.0;
        double r = 0.0;
        FastSlotToDouble(left, &l);
        FastSlotToDouble(right, &r);
        const bool l_nan = std::isnan(l);
        const bool r_nan = std::isnan(r);
        switch (instruction.binary) {
          case BinaryOperation::kEquals:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l == r));
            continue;
          case BinaryOperation::kNotEquals:
            push_int(stack, l_nan || r_nan ? 1 : static_cast<int64_t>(l != r));
            continue;
          case BinaryOperation::kLessThan:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l < r));
            continue;
          case BinaryOperation::kLessThanEquals:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l <= r));
            continue;
          case BinaryOperation::kGreaterThan:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l > r));
            continue;
          case BinaryOperation::kGreaterThanEquals:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l >= r));
            continue;
          case BinaryOperation::kAdd:
            push_double(stack, l + r);
            continue;
          case BinaryOperation::kSubtract:
            push_double(stack, l - r);
            continue;
          case BinaryOperation::kMultiply:
            push_double(stack, l * r);
            continue;
          case BinaryOperation::kDivide:
            if (r == 0.0) {
              return push_error(StatusCode::kIsInfinity, "division by zero");
            }
            {
              const double result = l / r;
              if (std::isfinite(l) && std::isfinite(r) && std::isinf(result)) {
                return push_error(StatusCode::kIsInfinity, "double overflow");
              }
              push_double(stack, result);
            }
            continue;
          case BinaryOperation::kModulo:
            // Homogeneous DOUBLE modulo has no kernel (TryArithmetic rejects
            // it); only the mixed-promotion ladder defines fmod.
            if (left.type == ValueType::kDouble &&
                right.type == ValueType::kDouble) {
              return push_error(StatusCode::kInvalidArgument,
                                "unsupported binary operation");
            }
            if (r == 0.0) {
              return push_error(StatusCode::kIsInfinity, "division by zero");
            }
            push_double(stack, std::fmod(l, r));
            continue;
          default:
            return FastEval::kUnsupported;
        }
      }
      case BytecodeOp::kUnaryInt64:
      case BytecodeOp::kUnaryDouble: {
        const FastSlot child = stack.back();
        stack.pop_back();
        switch (instruction.unary) {
          case UnaryOperation::kIsNull:
            push_int(stack, static_cast<int64_t>(child.is_null));
            continue;
          case UnaryOperation::kIsNotNull:
            push_int(stack, static_cast<int64_t>(!child.is_null));
            continue;
          case UnaryOperation::kIsTrue:
            push_int(stack,
                     static_cast<int64_t>(!child.is_null && FastTruthy(child)));
            continue;
          case UnaryOperation::kIsNotTrue:
            push_int(stack,
                     static_cast<int64_t>(child.is_null || !FastTruthy(child)));
            continue;
          case UnaryOperation::kIsFalse:
            push_int(stack, static_cast<int64_t>(!child.is_null &&
                                                 !FastTruthy(child)));
            continue;
          case UnaryOperation::kIsNotFalse:
            push_int(stack,
                     static_cast<int64_t>(child.is_null || FastTruthy(child)));
            continue;
          case UnaryOperation::kNot:
            if (child.is_null) {
              push_null(stack);
            } else {
              push_int(stack, static_cast<int64_t>(!FastTruthy(child)));
            }
            continue;
          case UnaryOperation::kMinus:
            if (child.is_null) {
              push_null(stack);
              continue;
            }
            if (child.type == ValueType::kDouble) {
              double d = 0.0;
              std::memcpy(&d, &child.bits, sizeof(d));
              push_double(stack, -d);
              continue;
            }
            if (child.type == ValueType::kInt64) {
              if (child.bits == std::numeric_limits<int64_t>::min()) {
                return push_error(StatusCode::kIsInfinity,
                                  "integer overflow in unary minus");
              }
              push_int(stack, -child.bits);
              continue;
            }
            return push_error(StatusCode::kInvalidArgument,
                              "unary minus requires a number");
          case UnaryOperation::kBitwiseNot:
            if (child.is_null) {
              push_null(stack);
              continue;
            }
            if (child.type != ValueType::kInt64) {
              return push_error(StatusCode::kInvalidArgument,
                                "bitwise not requires an integer");
            }
            push_int(stack,
                     static_cast<int64_t>(~static_cast<uint64_t>(child.bits)));
            continue;
          default:
            return FastEval::kUnsupported;
        }
      }
      case BytecodeOp::kExtractDatePart: {
        const FastSlot child = stack.back();
        stack.pop_back();
        if (child.is_null) {
          push_null(stack);
          continue;
        }
        if (child.type != ValueType::kDate) {
          // VARCHAR inputs take the string-slice kernel on the canonical
          // path.
          return FastEval::kUnsupported;
        }
        if (child.bits < kDateMinDays || kDateMaxDays < child.bits) {
          return push_error(StatusCode::kInvalidArgument,
                            "DATE value out of range");
        }
        const std::chrono::year_month_day ymd{std::chrono::sys_days{
            std::chrono::days{static_cast<int>(child.bits)}}};
        if (instruction.operand == 0) {
          push_int(stack, static_cast<int64_t>(static_cast<int>(ymd.year())));
        } else if (instruction.operand == 1) {
          push_int(stack,
                   static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
        } else {
          push_int(stack,
                   static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
        }
        continue;
      }
      default:
        return FastEval::kUnsupported;
    }
  }
  if (stack.size() != 1) {
    return push_error(StatusCode::kRuntimeError, "invalid bytecode stack");
  }
  *out = FastValueFromSlot(stack.back());
  return FastEval::kDone;
}

bool BytecodeEnabled() {
  // Not cached: the differential harness flips the variable between the two
  // executions of the same statement inside one process.
  return std::getenv("TINYLAMB_DISABLE_BYTECODE") == nullptr;
}

StatusOr<Value> TryEvaluateRowAuto(const BytecodeProgram& program,
                                   const Row& row) {
  if (program.FastInterpretable()) {
    Value out;
    Status error{Status::kSuccess};
    switch (program.TryEvaluateRowFast(row, &out, &error)) {
      case BytecodeProgram::FastEval::kDone:
        return out;
      case BytecodeProgram::FastEval::kError:
        return error;
      case BytecodeProgram::FastEval::kUnsupported:
        break;
    }
  }
  return program.TryEvaluateRow(row);
}

}  // namespace tinylamb
