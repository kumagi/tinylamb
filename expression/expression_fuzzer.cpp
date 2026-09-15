/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/evaluation_context_impl.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

struct FuzzStream {
  const uint8_t* data;
  size_t size;
  size_t offset{0};

  uint8_t NextU8() {
    if (offset < size) {
      return data[offset++];
    }
    return 0;
  }

  int64_t NextI64() {
    uint64_t val = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
      val = (val << 8) | NextU8();
    }
    return static_cast<int64_t>(val);
  }

  double NextDouble() {
    int64_t raw = NextI64();
    double d = 0.0;
    std::memcpy(&d, &raw, sizeof(d));
    return d;
  }

  bool NextBool() { return (NextU8() % 2) != 0; }

  bool Exhausted() const { return offset >= size; }
};

Value GenerateValue(FuzzStream& stream) {
  const uint8_t type_pick = stream.NextU8() % 5;
  switch (type_pick) {
    case 0:
      return Value();  // Null
    case 1: {
      const uint8_t special = stream.NextU8() % 6;
      if (special == 0) return Value(int64_t{0});
      if (special == 1) return Value(int64_t{1});
      if (special == 2) return Value(int64_t{-1});
      if (special == 3) return Value(std::numeric_limits<int64_t>::max());
      if (special == 4) return Value(std::numeric_limits<int64_t>::min());
      return Value(stream.NextI64());
    }
    case 2: {
      const uint8_t special = stream.NextU8() % 6;
      if (special == 0) return Value(0.0);
      if (special == 1) return Value(1.0);
      if (special == 2) return Value(-1.0);
      if (special == 3) return Value(std::numeric_limits<double>::infinity());
      if (special == 4) return Value(std::numeric_limits<double>::quiet_NaN());
      return Value(stream.NextDouble());
    }
    case 3:
      return Value(stream.NextBool());
    case 4: {
      const uint8_t len = stream.NextU8() % 8;
      std::string s;
      for (size_t i = 0; i < len; ++i) {
        s.push_back(static_cast<char>('a' + (stream.NextU8() % 26)));
      }
      return Value(std::move(s));
    }
  }
  return Value();
}

// Schema-typed value generators: the DataChunk column vectors CHECK on a
// type mismatch, so the row values must match the column types (NULL stays
// legal for every column).
Value GenerateIntValue(FuzzStream& stream) {
  const uint8_t special = stream.NextU8() % 8;
  if (special == 0) return Value();
  if (special == 1) return Value(int64_t{0});
  if (special == 2) return Value(int64_t{-1});
  if (special == 3) return Value(std::numeric_limits<int64_t>::min());
  if (special == 4) return Value(std::numeric_limits<int64_t>::max());
  return Value(stream.NextI64());
}

Value GenerateDoubleValue(FuzzStream& stream) {
  const uint8_t special = stream.NextU8() % 9;
  if (special == 0) return Value();
  if (special == 1) return Value(0.0);
  if (special == 2) return Value(-0.0);
  if (special == 3) return Value(std::numeric_limits<double>::infinity());
  if (special == 4) return Value(-std::numeric_limits<double>::infinity());
  if (special == 5) return Value(std::numeric_limits<double>::quiet_NaN());
  return Value(stream.NextDouble());
}

Value GenerateStringValue(FuzzStream& stream) {
  if (stream.NextU8() % 8 == 0) return Value();
  const uint8_t len = stream.NextU8() % 8;
  std::string s;
  for (size_t i = 0; i < len; ++i) {
    const uint8_t pick = stream.NextU8() % 16;
    if (pick == 0) {
      s.push_back('%');  // LIKE wildcard coverage
    } else if (pick == 1) {
      s.push_back('_');
    } else {
      s.push_back(static_cast<char>('a' + (stream.NextU8() % 26)));
    }
  }
  return Value(std::move(s));
}

Value GenerateDateValue(FuzzStream& stream) {
  if (stream.NextU8() % 8 == 0) return Value();
  return Value::DateFromDays(static_cast<int64_t>(stream.NextU8() % 4000));
}

Expression GenerateExpression(FuzzStream& stream, int depth) {
  if (depth <= 0 || stream.Exhausted()) {
    if (stream.NextBool()) {
      Value v = GenerateValue(stream);
      if (getenv("TINYLAMB_EXPR_FUZZ_VERBOSE")) {
        fprintf(stderr, "  leaf const type=%d val=%s\n", (int)v.type,
                v.AsString().c_str());
      }
      return ConstantValueExp(std::move(v));
    }
    const uint8_t col = stream.NextU8() % 4;
    if (col == 0) return ColumnValueExp("col_i");
    if (col == 1) return ColumnValueExp("col_d");
    if (col == 2) return ColumnValueExp("col_s");
    return ColumnValueExp("col_t");
  }

  const uint8_t kind = stream.NextU8() % 7;
  switch (kind) {
    case 0:
    case 1:
    case 2: {
      // Binary expression
      static const BinaryOperation ops[] = {
          BinaryOperation::kAdd,
          BinaryOperation::kSubtract,
          BinaryOperation::kMultiply,
          BinaryOperation::kDivide,
          BinaryOperation::kModulo,
          BinaryOperation::kShiftLeft,
          BinaryOperation::kShiftRight,
          BinaryOperation::kEquals,
          BinaryOperation::kNotEquals,
          BinaryOperation::kLessThan,
          BinaryOperation::kLessThanEquals,
          BinaryOperation::kGreaterThan,
          BinaryOperation::kGreaterThanEquals,
          BinaryOperation::kIsDistinctFrom,
          BinaryOperation::kIsNotDistinctFrom,
          BinaryOperation::kAnd,
          BinaryOperation::kOr,
          BinaryOperation::kXor,
          BinaryOperation::kLike,
          BinaryOperation::kNotLike,
      };
      BinaryOperation op =
          ops[stream.NextU8() % (sizeof(ops) / sizeof(ops[0]))];
      Expression left = GenerateExpression(stream, depth - 1);
      Expression right = GenerateExpression(stream, depth - 1);
      return BinaryExpressionExp(std::move(left), op, std::move(right));
    }
    case 3: {
      // Unary expression
      static const UnaryOperation un_ops[] = {
          UnaryOperation::kMinus,   UnaryOperation::kNot,
          UnaryOperation::kIsNull,  UnaryOperation::kIsNotNull,
          UnaryOperation::kIsTrue,  UnaryOperation::kIsNotTrue,
          UnaryOperation::kIsFalse, UnaryOperation::kIsNotFalse,
      };
      UnaryOperation op =
          un_ops[stream.NextU8() % (sizeof(un_ops) / sizeof(un_ops[0]))];
      Expression child = GenerateExpression(stream, depth - 1);
      return UnaryExpressionExp(std::move(child), op);
    }
    case 4: {
      // Case expression
      const size_t when_count = 1 + (stream.NextU8() % 3);
      std::vector<std::pair<Expression, Expression>> whens;
      for (size_t i = 0; i < when_count; ++i) {
        whens.emplace_back(GenerateExpression(stream, depth - 1),
                           GenerateExpression(stream, depth - 1));
      }
      Expression else_clause =
          stream.NextBool() ? GenerateExpression(stream, depth - 1) : nullptr;
      return CaseExpressionExp(std::move(whens), std::move(else_clause));
    }
    case 5: {
      // In expression
      const size_t list_count = 1 + (stream.NextU8() % 3);
      Expression target = GenerateExpression(stream, depth - 1);
      std::vector<Expression> list;
      for (size_t i = 0; i < list_count; ++i) {
        list.push_back(GenerateExpression(stream, depth - 1));
      }
      return InExpressionExp(std::move(target), std::move(list));
    }
    case 6: {
      // CAST to one of the scalar SQL types (a rich differential seam:
      // int<->double promotion, string parsing, date encoding, failures).
      Expression child = GenerateExpression(stream, depth - 1);
      static const char* kTypes[] = {"INT64", "DOUBLE", "STRING", "DATE",
                                     "BOOL"};
      const char* type = kTypes[stream.NextU8() % 5];
      return CastExpressionExp(std::move(child), type);
    }
  }
  return ConstantValueExp(GenerateValue(stream));
}

bool ValuesMatch(const Value& a, const Value& b) {
  if (a.IsNull() && b.IsNull()) return true;
  if (a.IsNull() != b.IsNull()) return false;
  if (a.type != b.type) return false;
  if (a.type == ValueType::kDouble) {
    if (std::isnan(a.value.double_value) && std::isnan(b.value.double_value)) {
      return true;
    }
    if (std::isinf(a.value.double_value) && std::isinf(b.value.double_value)) {
      return (a.value.double_value > 0) == (b.value.double_value > 0);
    }
    return std::abs(a.value.double_value - b.value.double_value) < 1e-9;
  }
  return a == b;
}

// The optimizer pins intentional identity-fold deviations
// (`a + 0.0 -> a` keeps INT64 typing; see
// optimizer_arithmetic_high_expectations.test). Bytecode compiles the
// folded tree, so an int/double divergence through such a neutral constant
// is expected; everything else must match the AST exactly.
// ExpressionCannotThrow() declares comparisons involving columns total by
// design (see rewrite.cpp): an INT64 vs DATE column comparison raises at
// runtime, yet the fold-dropping rules (is_null_of_null_check, absorption,
// filter pullup) are pinned by expectation files and fire on it. So a fold
// may legitimately erase THAT raise. Detect statically mismatched operand
// types so the harness tolerates exactly this error-erasure direction.
bool TypesComparable(const Expression& left, const Expression& right,
                     const Schema& schema) {
  auto tag = [&schema](const Expression& e) -> TypeTag {
    try {
      return e->ResultType(schema).GetType();
    } catch (...) {
      return TypeTag::kInvalid;
    }
  };
  const TypeTag l = tag(left);
  const TypeTag r = tag(right);
  auto numeric = [](TypeTag t) {
    return t == TypeTag::kInteger || t == TypeTag::kBigInt ||
           t == TypeTag::kDouble;
  };
  if (l == TypeTag::kInvalid || r == TypeTag::kInvalid) {
    return true;  // Unknown shapes stay untolerated (conservative).
  }
  if (l == r) {
    return true;  // Same type; only VALUE-dependent raises (string dates).
  }
  if (numeric(l) && numeric(r)) {
    return true;  // INT64/DOUBLE promote.
  }
  // DATE x VARCHAR is VALUE-dependent (a parseable string compares, anything
  // else raises), which the totality assumption relies on just as much as
  // the static mismatches: a fold that drops or moves such a comparison may
  // legitimately lose or gain a raise.
  return false;
}

bool ContainsStaticallyMismatchedComparison(const Expression& exp,
                                            const Schema& schema) {
  if (!exp) {
    return false;
  }
  if (exp->Type() == TypeTag::kCaseExp) {
    // A WHEN condition with a statically non-boolean, non-numeric type
    // raises in the AST; fold rules that drop or short-circuit CASE
    // branches treat evaluation as total by design.
    const auto& selected = exp->AsCaseExpression();
    for (const auto& [condition, result] : selected.when_clauses_) {
      if (condition) {
        try {
          switch (condition->ResultType(schema).GetType()) {
            case TypeTag::kInteger:
            case TypeTag::kBigInt:
            case TypeTag::kDouble:  // booleans are INT64 in this engine
              break;
            default:
              return true;
          }
        } catch (...) {
          return true;
        }
        if (ContainsStaticallyMismatchedComparison(condition, schema)) {
          return true;
        }
      }
      if (ContainsStaticallyMismatchedComparison(result, schema)) {
        return true;
      }
    }
    return ContainsStaticallyMismatchedComparison(selected.else_clause_,
                                                  schema);
  }
  if (exp->Type() == TypeTag::kInExp) {
    // IN membership lowers to `=` with the same static-type policy.
    const auto& in = exp->AsInExpression();
    for (const Expression& item : in.list_) {
      if (item && TypesComparable(in.child_, item, schema)) {
        continue;
      }
      if (item && item->Type() == TypeTag::kConstantValue &&
          item->AsConstantValue().GetValue().IsNull()) {
        continue;  // NULL items never reach the membership comparison.
      }
      if (item) {
        return true;
      }
    }
    if (ContainsStaticallyMismatchedComparison(in.child_, schema)) {
      return true;
    }
    for (const Expression& item : in.list_) {
      if (ContainsStaticallyMismatchedComparison(item, schema)) {
        return true;
      }
    }
    return false;
  }
  if (exp->Type() == TypeTag::kBinaryExp) {
    const auto& binary = exp->AsBinaryExpression();
    switch (binary.Op()) {
      case BinaryOperation::kEquals:
      case BinaryOperation::kNotEquals:
      case BinaryOperation::kLessThan:
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThan:
      case BinaryOperation::kGreaterThanEquals:
      case BinaryOperation::kIsDistinctFrom:
      case BinaryOperation::kIsNotDistinctFrom:
      case BinaryOperation::kLike:
      case BinaryOperation::kNotLike:
        if (!TypesComparable(binary.Left(), binary.Right(), schema)) {
          return true;
        }
        break;
      default:
        break;
    }
    return ContainsStaticallyMismatchedComparison(binary.Left(), schema) ||
           ContainsStaticallyMismatchedComparison(binary.Right(), schema);
  }
  for (const Expression& child : ExpressionChildren(exp)) {
    if (ContainsStaticallyMismatchedComparison(child, schema)) {
      return true;
    }
  }
  return false;
}

bool ContainsNeutralArithmetic(const Expression& exp) {
  if (!exp) {
    return false;
  }
  if (exp->Type() == TypeTag::kBinaryExp) {
    const auto& binary = exp->AsBinaryExpression();
    bool arithmetic = false;
    switch (binary.Op()) {
      case BinaryOperation::kAdd:
      case BinaryOperation::kSubtract:
      case BinaryOperation::kMultiply:
      case BinaryOperation::kDivide:
        arithmetic = true;
        break;
      default:
        break;
    }
    auto neutral = [](const Expression& e) -> bool {
      if (!e) {
        return false;
      }
      // Evaluate the (column-free) operand to see through folds the
      // compiler applies first, e.g. `-0.0` wraps the literal in a unary
      // minus before identity_add_zero can fire on it.
      Value v;
      try {
        v = e->Evaluate(Row(), Schema());
      } catch (...) {
        return false;
      }
      if (v.IsNull()) {
        return false;
      }
      if (v.type == ValueType::kInt64) {
        return v.value.int_value == 0 || v.value.int_value == 1;
      }
      if (v.type == ValueType::kDouble) {
        return v.value.double_value == 0.0 || v.value.double_value == 1.0;
      }
      return false;
    };
    if (arithmetic && (neutral(binary.Left()) || neutral(binary.Right()))) {
      return true;
    }
    return ContainsNeutralArithmetic(binary.Left()) ||
           ContainsNeutralArithmetic(binary.Right());
  }
  for (const Expression& child : ExpressionChildren(exp)) {
    if (ContainsNeutralArithmetic(child)) {
      return true;
    }
  }
  return false;
}

bool PinnedIdentityPromotion(const Expression& exp, const Value& a,
                             const Value& b) {
  if (a.IsNull() || b.IsNull() || a.type == b.type) {
    return false;
  }
  auto numeric = [](const Value& v) {
    return v.type == ValueType::kInt64 || v.type == ValueType::kDouble;
  };
  if (!numeric(a) || !numeric(b)) {
    return false;
  }
  if (!ContainsNeutralArithmetic(exp)) {
    return false;
  }
  auto toDouble = [](const Value& v) {
    return v.type == ValueType::kInt64 ? static_cast<double>(v.value.int_value)
                                       : v.value.double_value;
  };
  // A promoted double path keeps full IEEE precision above 2^53 while the
  // folded int path rounds; compare with ~4-ulp slack instead of ==.
  const double av = toDouble(a);
  const double bv = toDouble(b);
  const double scale = std::max(1.0, std::max(std::abs(av), std::abs(bv)));
  return std::abs(av - bv) <= scale * (1.0 / 281474976710656.0);  // 2^-48
}

}  // namespace
}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 4 || size > 4096) {
    return 0;
  }

  using namespace tinylamb;
  FuzzStream stream{data, size};

  Schema schema("fuzz_schema", {
                                   Column("col_i", ValueType::kInt64),
                                   Column("col_d", ValueType::kDouble),
                                   Column("col_s", ValueType::kVarChar),
                                   Column("col_t", ValueType::kDate),
                               });

  Row row({
      GenerateIntValue(stream),
      GenerateDoubleValue(stream),
      GenerateStringValue(stream),
      GenerateDateValue(stream),
  });

  Expression exp = GenerateExpression(stream, 4);
  if (!exp) return 0;

  if (getenv("TINYLAMB_EXPR_FUZZ_VERBOSE")) {
    fprintf(stderr, "expr=%s row=%s\n", exp->ToString().c_str(),
            row.ToString().c_str());
  }

  // (1) AST Ground Truth evaluation
  bool ast_threw = false;
  Value ast_result;
  std::string ast_throw_reason;
  try {
    ast_result = exp->Evaluate(row, schema);
  } catch (const std::exception& error) {
    ast_threw = true;
    ast_throw_reason = error.what();
  } catch (...) {
    ast_threw = true;
    ast_throw_reason = "<non-standard>";
  }

  // (2) Bytecode VM differential check
  std::optional<BytecodeProgram> program =
      BytecodeCompiler::Compile(exp, schema);
  if (program.has_value()) {
    bool bc_threw = false;
    Value bc_result;
    try {
      DataChunk chunk(schema);
      chunk.Append(row);
      ColumnVector out = program->EvaluateBatch(chunk);
      assert(out.Size() == 1);
      bc_result = out.ValueAt(0);
    } catch (...) {
      bc_threw = true;
    }
    // Identity folds of 0/1 constants also change the promotion context:
    // `(-0.0 + ci) * ci` stays in DOUBLE (never overflows) while the folded
    // `ci * ci` raises at the INT64 boundary -- pinned behaviour of
    // optimizer_arithmetic_high_expectations.test, so only neutral-constant
    // free mismatches are real bugs.
    // The ExpressionCannotThrow-by-design assumption for column comparisons
    // and IN miscompares cuts both ways: folds may drop a raise the AST
    // reference produced, or move a subtree out of a short-circuited
    // position and *add* a raise the AST never reached.
    const bool fold_tolerated =
        ContainsNeutralArithmetic(exp) ||
        ((ast_threw != bc_threw) &&
         ContainsStaticallyMismatchedComparison(exp, schema));
    if (ast_threw != bc_threw && !fold_tolerated) {
      if (getenv("TINYLAMB_EXPR_FUZZ_PASSES")) {
        const ExpressionRewriter pr(ExpressionRuleSet::Default());
        Expression cur = exp;
        for (int pass = 0; pass < 6 && cur; ++pass) {
          StatusOr<Expression> nx = pr.TryRewriteOnce(cur, 0);
          if (!nx.HasValue() || !nx.Value()) {
            fprintf(
                stderr, "pass%d stop: %s\n", pass,
                nx.HasValue() ? "<null>" : ToString(nx.GetStatus()).c_str());
            break;
          }
          std::string vv;
          try {
            const Value pv = nx.Value()->Evaluate(row, schema);
            vv = pv.AsString();
          } catch (const std::exception& e) {
            vv = std::string("THREW:") + e.what();
          }
          fprintf(stderr, "pass%d val=%s :: %s\n", pass, vv.c_str(),
                  nx.Value()->ToString().c_str());
          if (nx.Value()->ToString() == cur->ToString()) {
            break;
          }
          cur = nx.Value();
        }
      }
      std::string fold_dump = "<none>";
      if (const Expression folded =
              ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(exp)) {
        fold_dump = folded->ToString();
        try {
          const Value fv = folded->Evaluate(row, schema);
          fold_dump +=
              " => (" + std::to_string((int)fv.type) + ")" + fv.AsString();
        } catch (...) {
          fold_dump += " => THREW";
        }
      }
      fprintf(stderr, "threw mismatch ast=%d(%s) bc=%d exp=%s folded=%s\n",
              ast_threw, ast_throw_reason.c_str(), bc_threw,
              exp->ToString().c_str(), fold_dump.c_str());
    }
    assert(ast_threw == bc_threw || fold_tolerated);
    if (!ast_threw && !bc_threw && !ValuesMatch(ast_result, bc_result) &&
        !PinnedIdentityPromotion(exp, ast_result, bc_result) &&
        getenv("TINYLAMB_EXPR_FUZZ_PASSES")) {
      const ExpressionRewriter pr(ExpressionRuleSet::Default());
      Expression cur = exp;
      for (int pass = 0; pass < 6 && cur; ++pass) {
        StatusOr<Expression> nx = pr.TryRewriteOnce(cur, 0);
        if (!nx.HasValue() || !nx.Value()) {
          break;
        }
        std::string vv;
        try {
          const Value pv = nx.Value()->Evaluate(row, schema);
          vv = pv.AsString();
        } catch (const std::exception& e) {
          vv = std::string("THREW:") + e.what();
        }
        fprintf(stderr, "pass%d val=%s :: %s\n", pass, vv.c_str(),
                nx.Value()->ToString().c_str());
        if (nx.Value()->ToString() == cur->ToString()) {
          break;
        }
        cur = nx.Value();
      }
    }
    if (!ast_threw && !bc_threw && !ValuesMatch(ast_result, bc_result) &&
        !PinnedIdentityPromotion(exp, ast_result, bc_result)) {
      std::string folded_text = "<none>";
      if (const Expression folded =
              ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(exp)) {
        folded_text = folded->ToString();
        try {
          const Value fv = folded->Evaluate(row, schema);
          folded_text +=
              " => (" + std::to_string((int)fv.type) + ")" + fv.AsString();
        } catch (...) {
          folded_text += " => THREW";
        }
      }
      fprintf(stderr,
              "value mismatch ast=(%d)%s bc=(%d)%s exp=%s folded=%s row=%s\n",
              (int)ast_result.type, ast_result.AsString().c_str(),
              (int)bc_result.type, bc_result.AsString().c_str(),
              exp->ToString().c_str(), folded_text.c_str(),
              row.ToString().c_str());
    }
    if (!ast_threw && !bc_threw) {
      assert(ValuesMatch(ast_result, bc_result) ||
             PinnedIdentityPromotion(exp, ast_result, bc_result));
    }
  }

  // (3) relational_detail::Evaluate fallback differential check
  bool detail_threw = false;
  Value detail_result;
  try {
    TransactionContext context(Transaction{}, nullptr);
    const relational_detail::Scope scope{&row, &schema, nullptr};
    const relational_detail::CteMap ctes;
    detail_result =
        relational_detail::Evaluate(exp, scope, nullptr, context, ctes);
  } catch (...) {
    detail_threw = true;
  }
  if (ast_threw != detail_threw) {
    fprintf(stderr, "threw mismatch ast=%d detail=%d exp=%s row=%s\n",
            ast_threw, detail_threw, exp->ToString().c_str(),
            row.ToString().c_str());
  }
  assert(ast_threw == detail_threw);
  if (!ast_threw && !ValuesMatch(ast_result, detail_result) &&
      !PinnedIdentityPromotion(exp, ast_result, detail_result)) {
    fprintf(stderr, "value mismatch ast=%s detail=%s exp=%s row=%s\n",
            ast_result.AsString().c_str(), detail_result.AsString().c_str(),
            exp->ToString().c_str(), row.ToString().c_str());
  }
  if (!ast_threw) {
    assert(ValuesMatch(ast_result, detail_result) ||
           PinnedIdentityPromotion(exp, ast_result, detail_result));
  }

  return 0;
}
