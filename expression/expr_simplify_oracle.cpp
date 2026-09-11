/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/expr_simplify_oracle.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

// Static generator type. NULL leaves are untyped Values; the SQL serializer
// recovers the intended type from this tag.
enum class GenType : uint8_t { kInt, kFloat, kBool, kString };

struct TypedExpr {
  Expression expr;
  GenType type = GenType::kInt;
};

class Gen {
 public:
  explicit Gen(std::mt19937& rng) : rng_(rng) {}
  int Pick(int lo, int hi) {
    return std::uniform_int_distribution<int>(lo, hi)(rng_);
  }
  bool Chance(int percent) { return Pick(1, 100) <= percent; }

 private:
  std::mt19937& rng_;
};

Value MakeIntLeaf(Gen& g) {
  switch (g.Pick(0, 9)) {
    case 0:
      return Value(int64_t{0});
    case 1:
      return Value(int64_t{1});
    case 2:
      return Value(int64_t{-1});
    case 3:
      return Value(kInt64Max);
    case 4:
      return Value(kInt64Min);
    case 5:
      return Value(static_cast<int64_t>(g.Pick(-5, 5)));
    default: {
      // Wider magnitudes stress overflow rewrites (constant folding must
      // raise instead of wrapping).
      const auto hi = static_cast<int64_t>(g.Pick(0, 1000000));
      const auto lo = static_cast<int64_t>(g.Pick(0, 1000000));
      int64_t v = (hi << 32) | lo;
      return Value(g.Chance(50) ? v : -v);
    }
  }
}

Value MakeFloatLeaf(Gen& g) {
  switch (g.Pick(0, 9)) {
    case 0:
      return Value(0.0);
    case 1:
      return Value(1.0);
    case 2:
      return Value(-1.0);
    case 3:
      return Value(2.5);
    case 4:
      return Value(-1.5);
    case 5:
      return Value(std::numeric_limits<double>::infinity());
    case 6:
      return Value(-std::numeric_limits<double>::infinity());
    case 7:
      return Value(std::numeric_limits<double>::quiet_NaN());
    default:
      return Value(g.Pick(-50, 50) / 10.0);
  }
}

std::string MakeStringLeaf(Gen& g, bool extended) {
  // Small alphabet plus LIKE metacharacters; space added in extended mode for trim rewrites.
  static constexpr std::string_view kAlphabet = "ab%_ ";
  const int len = g.Pick(0, 3);
  std::string s;
  const int max_char_idx = extended ? 4 : 3;
  for (int i = 0; i < len; ++i) {
    s.push_back(kAlphabet[static_cast<size_t>(g.Pick(0, max_char_idx))]);
  }
  return s;
}

TypedExpr GenLeaf(Gen& g, GenType type, const ExprGenConfig& config) {
  if (g.Chance(config.null_percent)) {
    return {.expr = ConstantValueExp(Value()), .type = type};
  }
  switch (type) {
    case GenType::kInt:
      return {.expr = ConstantValueExp(MakeIntLeaf(g)), .type = type};
    case GenType::kFloat:
      return {.expr = ConstantValueExp(MakeFloatLeaf(g)), .type = type};
    case GenType::kBool:
      return {.expr = ConstantValueExp(Value(g.Chance(50))), .type = type};
    case GenType::kString: {
      std::string s = MakeStringLeaf(g, config.extended_ops);
      return {.expr = ConstantValueExp(Value(std::move(s))), .type = type};
    }
  }
  return {.expr = ConstantValueExp(Value(int64_t{0})), .type = GenType::kInt};
}

GenType RandomType(Gen& g) {
  switch (g.Pick(0, 3)) {
    case 0:
      return GenType::kInt;
    case 1:
      return GenType::kFloat;
    case 2:
      return GenType::kBool;
    default:
      return GenType::kString;
  }
}

// Forward declaration for recursive generation.
TypedExpr GenTyped(Gen& g, GenType want, int depth,
                   const ExprGenConfig& config);

TypedExpr GenNumeric(Gen& g, int depth, const ExprGenConfig& config) {
  // Result type follows the engine's coercion: any float input widens.
  auto binary = [&](BinaryOperation op) {
    TypedExpr left = GenTyped(g, GenType::kInt, depth - 1, config);
    TypedExpr right = GenTyped(g, GenType::kInt, depth - 1, config);
    if (g.Chance(40)) {
      left = GenTyped(g, GenType::kFloat, depth - 1, config);
    }
    if (g.Chance(40)) {
      right = GenTyped(g, GenType::kFloat, depth - 1, config);
    }
    GenType result =
        (left.type == GenType::kFloat || right.type == GenType::kFloat ||
         op == BinaryOperation::kDivide)
            ? GenType::kFloat
            : GenType::kInt;
    return TypedExpr{.expr = BinaryExpressionExp(std::move(left.expr), op,
                                                 std::move(right.expr)),
                     .type = result};
  };
  const int choice_limit = config.extended_ops ? 19 : 9;
  switch (g.Pick(0, choice_limit)) {
    case 0:
      return binary(BinaryOperation::kAdd);
    case 1:
      return binary(BinaryOperation::kSubtract);
    case 2:
      return binary(BinaryOperation::kMultiply);
    case 3:
      return binary(BinaryOperation::kDivide);
    case 4: {
      // MOD stays integral in the engine.
      TypedExpr left = GenTyped(g, GenType::kInt, depth - 1, config);
      TypedExpr right = GenTyped(g, GenType::kInt, depth - 1, config);
      return {.expr = BinaryExpressionExp(std::move(left.expr),
                                          BinaryOperation::kModulo,
                                          std::move(right.expr)),
              .type = GenType::kInt};
    }
    case 5: {
      TypedExpr child = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      GenType result = child.type;
      return {.expr = FunctionCallExp("abs", {std::move(child.expr)}),
              .type = result};
    }
    case 6: {
      if (!config.extended_ops) {
        // GREATEST/LEAST over a homogeneous numeric list.
        GenType elem = g.Chance(50) ? GenType::kInt : GenType::kFloat;
        std::vector<Expression> args;
        const int count = g.Pick(2, 3);
        args.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
          args.push_back(GenTyped(g, elem, depth - 1, config).expr);
        }
        return {.expr = FunctionCallExp(g.Chance(50) ? "greatest" : "least",
                                        std::move(args)),
                .type = elem};
      }
      // GREATEST/LEAST over numeric arguments (supports mixed int/float).
      std::vector<Expression> args;
      const int count = g.Pick(2, 3);
      args.reserve(static_cast<size_t>(count));
      GenType result_type = GenType::kInt;
      for (int i = 0; i < count; ++i) {
        GenType arg_type = g.Chance(50) ? GenType::kInt : GenType::kFloat;
        if (arg_type == GenType::kFloat) {
          result_type = GenType::kFloat;
        }
        args.push_back(GenTyped(g, arg_type, depth - 1, config).expr);
      }
      return {.expr = FunctionCallExp(g.Chance(50) ? "greatest" : "least",
                                      std::move(args)),
              .type = result_type};
    }
    case 7: {
      TypedExpr child = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      GenType result = child.type;
      return {.expr = UnaryExpressionExp(std::move(child.expr),
                                          UnaryOperation::kMinus),
              .type = result};
    }
    case 8: {
      if (!config.extended_ops) {
        GenType elem = g.Chance(50) ? GenType::kInt : GenType::kFloat;
        if (g.Chance(50)) {
          std::vector<Expression> args;
          const int count = g.Pick(2, 3);
          args.reserve(static_cast<size_t>(count));
          for (int i = 0; i < count; ++i) {
            args.push_back(GenTyped(g, elem, depth - 1, config).expr);
          }
          return {.expr = FunctionCallExp("coalesce", std::move(args)),
                  .type = elem};
        }
        // NULLIF takes exactly 2 arguments.
        return {.expr = FunctionCallExp(
                    "nullif", {GenTyped(g, elem, depth - 1, config).expr,
                               GenTyped(g, elem, depth - 1, config).expr}),
                .type = elem};
      }
      if (g.Chance(50)) {
        GenType elem = g.Chance(50) ? GenType::kInt : GenType::kFloat;
        std::vector<Expression> args;
        const int count = g.Pick(2, 3);
        args.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
          args.push_back(GenTyped(g, elem, depth - 1, config).expr);
        }
        return {.expr = FunctionCallExp("coalesce", std::move(args)),
                .type = elem};
      }
      // NULLIF takes 2 arguments and returns the type of the first argument.
      TypedExpr left = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      TypedExpr right = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      GenType result_type = left.type;
      return {.expr = FunctionCallExp(
                  "nullif", {std::move(left.expr), std::move(right.expr)}),
              .type = result_type};
    }
    case 9: {
      if (!config.extended_ops) {
        GenType from = g.Chance(50) ? GenType::kInt : GenType::kFloat;
        TypedExpr child = GenTyped(g, from, depth - 1, config);
        GenType to = (from == GenType::kInt) ? GenType::kFloat : GenType::kInt;
        return {
            .expr = CastExpressionExp(std::move(child.expr),
                                      to == GenType::kInt ? "INT64" : "FLOAT64"),
            .type = to};
      }
      // Bitwise ops: __bit_and, __bit_or, __bit_xor
      static constexpr std::array<const char*, 3> kBitOps = {
          "__bit_and", "__bit_or", "__bit_xor"};
      const char* fn = kBitOps[static_cast<size_t>(g.Pick(0, 2))];
      TypedExpr left = GenTyped(g, GenType::kInt, depth - 1, config);
      if (left.type != GenType::kInt) {
        left = {.expr = CastExpressionExp(std::move(left.expr), "INT64"),
                .type = GenType::kInt};
      }
      TypedExpr right = GenTyped(g, GenType::kInt, depth - 1, config);
      if (right.type != GenType::kInt) {
        right = {.expr = CastExpressionExp(std::move(right.expr), "INT64"),
                 .type = GenType::kInt};
      }
      return {.expr = FunctionCallExp(
                  fn, {std::move(left.expr), std::move(right.expr)}),
              .type = GenType::kInt};
    }
    case 10: {
      // Bit shifts: __shift_left, __shift_right with safe shift amount (0..63)
      const char* fn = g.Chance(50) ? "__shift_left" : "__shift_right";
      TypedExpr left = GenTyped(g, GenType::kInt, depth - 1, config);
      if (left.type != GenType::kInt) {
        left = {.expr = CastExpressionExp(std::move(left.expr), "INT64"),
                .type = GenType::kInt};
      }
      Expression right =
          ConstantValueExp(Value(static_cast<int64_t>(g.Pick(-2, 70))));
      return {.expr = FunctionCallExp(
                  fn, {std::move(left.expr), std::move(right)}),
              .type = GenType::kInt};
    }
    case 11: {
      // length(string)
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp("length", {std::move(child.expr)}),
              .type = GenType::kInt};
    }
    case 12: {
      static constexpr std::array<const char*, 3> kSafeOps = {
          "safe_add", "safe_subtract", "safe_multiply"};
      const char* fn = kSafeOps[static_cast<size_t>(g.Pick(0, 2))];
      TypedExpr left = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      TypedExpr right = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      GenType result =
          (left.type == GenType::kFloat || right.type == GenType::kFloat)
              ? GenType::kFloat
              : GenType::kInt;
      return {.expr = FunctionCallExp(
                  fn, {std::move(left.expr), std::move(right.expr)}),
              .type = result};
    }
    case 13: {
      static constexpr std::array<const char*, 4> kRoundingOps = {
          "ceil", "floor", "round", "trunc"};
      const char* fn = kRoundingOps[static_cast<size_t>(g.Pick(0, 3))];
      TypedExpr child = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      GenType result = child.type;
      if (std::string_view(fn) == "round" && g.Chance(50)) {
        Expression digits =
            ConstantValueExp(Value(static_cast<int64_t>(g.Pick(-2, 4))));
        return {.expr = FunctionCallExp(
                    fn, {std::move(child.expr), std::move(digits)}),
                .type = result};
      }
      return {.expr = FunctionCallExp(fn, {std::move(child.expr)}),
              .type = result};
    }
    case 14: {
      TypedExpr child = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      return {.expr = FunctionCallExp("sign", {std::move(child.expr)}),
              .type = child.type};
    }
    case 15: {
      static constexpr std::array<const char*, 8> kMathOps = {
          "sqrt", "cbrt", "exp", "ln", "cos", "sin", "tan", "pow"};
      const char* fn = kMathOps[static_cast<size_t>(g.Pick(0, 7))];
      if (std::string_view(fn) == "pow") {
        TypedExpr left = GenTyped(
            g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
        TypedExpr right = GenTyped(
            g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
        return {.expr = FunctionCallExp(
                    fn, {std::move(left.expr), std::move(right.expr)}),
                .type = GenType::kFloat};
      }
      TypedExpr child = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      return {.expr = FunctionCallExp(fn, {std::move(child.expr)}),
              .type = GenType::kFloat};
    }
    case 16: {
      if (g.Chance(33)) {
        TypedExpr left = GenTyped(g, GenType::kInt, depth - 1, config);
        TypedExpr right = GenTyped(g, GenType::kInt, depth - 1, config);
        return {.expr = FunctionCallExp(
                    "div", {std::move(left.expr), std::move(right.expr)}),
                .type = GenType::kInt};
      }
      const char* fn = g.Chance(50) ? "safe_divide" : "ieee_divide";
      TypedExpr left = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      TypedExpr right = GenTyped(
          g, g.Chance(50) ? GenType::kInt : GenType::kFloat, depth - 1, config);
      return {.expr = FunctionCallExp(
                  fn, {std::move(left.expr), std::move(right.expr)}),
              .type = GenType::kFloat};
    }
    case 17: {
      static constexpr std::array<const char*, 4> kStrToIntOps = {
          "length", "char_length", "ascii", "unicode"};
      const char* fn = kStrToIntOps[static_cast<size_t>(g.Pick(0, 3))];
      TypedExpr str = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp(fn, {std::move(str.expr)}),
              .type = GenType::kInt};
    }
    case 18: {
      // Numeric CAST exercises cast folding/pushdown.
      GenType from = g.Chance(50) ? GenType::kInt : GenType::kFloat;
      TypedExpr child = GenTyped(g, from, depth - 1, config);
      GenType to = (from == GenType::kInt) ? GenType::kFloat : GenType::kInt;
      return {
          .expr = CastExpressionExp(std::move(child.expr),
                                    to == GenType::kInt ? "INT64" : "FLOAT64"),
          .type = to};
    }
    case 19:
    default: {
      TypedExpr child = GenTyped(g, GenType::kInt, depth - 1, config);
      return {.expr = UnaryExpressionExp(std::move(child.expr),
                                          UnaryOperation::kBitwiseNot),
              .type = GenType::kInt};
    }
  }
}

TypedExpr GenBool(Gen& g, int depth, const ExprGenConfig& config) {
  const int choice_limit = config.extended_ops ? 12 : 11;
  switch (g.Pick(0, choice_limit)) {
    case 0:
    case 1: {
      static constexpr std::array<BinaryOperation, 8> kComparisons{
          BinaryOperation::kEquals,          BinaryOperation::kNotEquals,
          BinaryOperation::kLessThan,        BinaryOperation::kLessThanEquals,
          BinaryOperation::kGreaterThan,     BinaryOperation::kGreaterThanEquals,
          BinaryOperation::kIsDistinctFrom,  BinaryOperation::kIsNotDistinctFrom};
      const int cmp_limit = config.extended_ops ? 7 : 5;
      BinaryOperation op = kComparisons[static_cast<size_t>(g.Pick(0, cmp_limit))];
      TypedExpr left = GenTyped(g, GenType::kInt, depth - 1, config);
      TypedExpr right = GenTyped(g, GenType::kInt, depth - 1, config);
      if (g.Chance(40)) {
        left = GenTyped(g, GenType::kFloat, depth - 1, config);
      }
      if (g.Chance(40)) {
        right = GenTyped(g, GenType::kFloat, depth - 1, config);
      }
      return {.expr = BinaryExpressionExp(std::move(left.expr), op,
                                          std::move(right.expr)),
              .type = GenType::kBool};
    }
    case 2:
    case 3: {
      static constexpr std::array<BinaryOperation, 3> kLogic{
          BinaryOperation::kAnd, BinaryOperation::kOr, BinaryOperation::kXor};
      BinaryOperation op = kLogic[static_cast<size_t>(g.Pick(0, 2))];
      // Hoisted: as function arguments the two GenTyped calls had unspecified
      // evaluation order, so gcc and clang consumed the RNG stream in
      // opposite orders and the "same seed -> same tree" replay contract
      // broke across compilers.
      TypedExpr left = GenTyped(g, GenType::kBool, depth - 1, config);
      TypedExpr right = GenTyped(g, GenType::kBool, depth - 1, config);
      if (config.extended_ops) {
        if (g.Chance(15)) {
          right = {.expr = left.expr, .type = left.type};
        } else if (g.Chance(15)) {
          BinaryOperation inner_op = (op == BinaryOperation::kAnd)
                                         ? BinaryOperation::kOr
                                         : BinaryOperation::kAnd;
          TypedExpr y = GenTyped(g, GenType::kBool, depth - 1, config);
          right = {.expr = BinaryExpressionExp(left.expr, inner_op,
                                               std::move(y.expr)),
                   .type = GenType::kBool};
        } else if (g.Chance(15)) {
          GenType operand_type = g.Chance(50) ? GenType::kInt : GenType::kFloat;
          left = GenTyped(g, operand_type, depth - 1, config);
          if (g.Chance(30)) {
            right = {.expr = left.expr, .type = left.type};
          } else {
            right = GenTyped(g, operand_type, depth - 1, config);
          }
        }
      }
      return {.expr = BinaryExpressionExp(std::move(left.expr), op,
                                          std::move(right.expr)),
              .type = GenType::kBool};
    }
    case 4: {
      TypedExpr child = GenTyped(g, GenType::kBool, depth - 1, config);
      if (config.extended_ops) {
        if (g.Chance(20)) {
          child = {.expr = UnaryExpressionExp(std::move(child.expr),
                                              UnaryOperation::kNot),
                   .type = GenType::kBool};
        } else if (g.Chance(15)) {
          child = GenTyped(g, g.Chance(50) ? GenType::kInt : GenType::kFloat,
                           depth - 1, config);
        }
      }
      return {.expr = UnaryExpressionExp(std::move(child.expr),
                                         UnaryOperation::kNot),
              .type = GenType::kBool};
    }
    case 5: {
      // LIKE over small strings; the other side stays a leaf so the
      // prefix-range rewrite sees a foldable pattern.
      TypedExpr target = GenTyped(g, GenType::kString, depth - 1, config);
      TypedExpr pattern = GenLeaf(g, GenType::kString, config);
      BinaryOperation op =
          g.Chance(70) ? BinaryOperation::kLike : BinaryOperation::kNotLike;
      return {.expr = BinaryExpressionExp(std::move(target.expr), op,
                                          std::move(pattern.expr)),
              .type = GenType::kBool};
    }
    case 6: {
      static constexpr std::array<UnaryOperation, 6> kPredicates{
          UnaryOperation::kIsNull,  UnaryOperation::kIsNotNull,
          UnaryOperation::kIsTrue,  UnaryOperation::kIsNotTrue,
          UnaryOperation::kIsFalse, UnaryOperation::kIsNotFalse};
      UnaryOperation op = kPredicates[static_cast<size_t>(g.Pick(0, 5))];
      GenType child_type = RandomType(g);
      if (op == UnaryOperation::kIsTrue || op == UnaryOperation::kIsNotTrue ||
          op == UnaryOperation::kIsFalse || op == UnaryOperation::kIsNotFalse) {
        child_type = GenType::kBool;
      }
      return {.expr = UnaryExpressionExp(
                  GenTyped(g, child_type, depth - 1, config).expr, op),
              .type = GenType::kBool};
    }
    case 7: {
      // IN over a homogeneous list with edge constants mixed in.
      GenType elem = RandomType(g);
      if (elem == GenType::kString) {
        elem = GenType::kInt;  // keep IN lists numeric/bool-typed.
      }
      TypedExpr target = GenTyped(g, elem, depth - 1, config);
      std::vector<Expression> list;
      const int count = g.Pick(1, 4);
      list.reserve(static_cast<size_t>(count));
      for (int i = 0; i < count; ++i) {
        list.push_back(g.Chance(60) ? GenTyped(g, elem, depth - 1, config).expr
                                    : GenLeaf(g, elem, config).expr);
      }
      return {.expr = InExpressionExp(std::move(target.expr), std::move(list)),
              .type = GenType::kBool};
    }
    case 8: {
      std::vector<Expression> args;
      const int count = g.Pick(2, 3);
      args.reserve(static_cast<size_t>(count));
      for (int i = 0; i < count; ++i) {
        args.push_back(GenTyped(g, GenType::kBool, depth - 1, config).expr);
      }
      return {.expr = FunctionCallExp("coalesce", std::move(args)),
              .type = GenType::kBool};
    }
    case 9: {
      // NULLIF takes exactly 2 arguments.
      TypedExpr first = GenTyped(g, GenType::kBool, depth - 1, config);
      TypedExpr second = GenTyped(g, GenType::kBool, depth - 1, config);
      return {.expr = FunctionCallExp(
                  "nullif", {std::move(first.expr), std::move(second.expr)}),
              .type = GenType::kBool};
    }
    case 10: {
      // Boolean CASE: branches stay boolean so the result is boolean.
      const int branches = g.Pick(1, 2);
      std::vector<std::pair<Expression, Expression>> whens;
      whens.reserve(static_cast<size_t>(branches));
      for (int i = 0; i < branches; ++i) {
        // Hoisted for a defined evaluation order (see GenBool logic case).
        TypedExpr when = GenTyped(g, GenType::kBool, depth - 1, config);
        TypedExpr then = GenTyped(g, GenType::kBool, depth - 1, config);
        whens.emplace_back(std::move(when.expr), std::move(then.expr));
      }
      Expression otherwise =
          g.Chance(70) ? GenTyped(g, GenType::kBool, depth - 1, config).expr
                       : nullptr;
      return {.expr = CaseExpressionExp(std::move(whens), std::move(otherwise)),
              .type = GenType::kBool};
    }
    case 11: {
      if (!config.extended_ops) {
        return GenLeaf(g, GenType::kBool, config);
      }
      // String prefix/suffix predicates: starts_with, ends_with
      TypedExpr hay = GenTyped(g, GenType::kString, depth - 1, config);
      TypedExpr prefix = GenTyped(g, GenType::kString, depth - 1, config);
      const char* fn = g.Chance(50) ? "starts_with" : "ends_with";
      return {.expr = FunctionCallExp(
                  fn, {std::move(hay.expr), std::move(prefix.expr)}),
              .type = GenType::kBool};
    }
    case 12: {
      if (!config.extended_ops) {
        return GenLeaf(g, GenType::kBool, config);
      }
      TypedExpr left = GenTyped(g, GenType::kString, depth - 1, config);
      TypedExpr right = GenTyped(g, GenType::kString, depth - 1, config);
      BinaryOperation op =
          g.Chance(50) ? BinaryOperation::kEquals : BinaryOperation::kNotEquals;
      return {.expr = BinaryExpressionExp(std::move(left.expr), op,
                                          std::move(right.expr)),
              .type = GenType::kBool};
    }
    default:
      return GenLeaf(g, GenType::kBool, config);
  }
}

TypedExpr GenString(Gen& g, int depth, const ExprGenConfig& config) {
  if (!config.extended_ops || depth <= 0 || g.Chance(30)) {
    return GenLeaf(g, GenType::kString, config);
  }
  switch (g.Pick(0, 13)) {
    case 0: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp("upper", {std::move(child.expr)}),
              .type = GenType::kString};
    }
    case 1: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp("lower", {std::move(child.expr)}),
              .type = GenType::kString};
    }
    case 2: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp("trim", {std::move(child.expr)}),
              .type = GenType::kString};
    }
    case 3: {
      const int count = g.Pick(2, 3);
      std::vector<Expression> args;
      args.reserve(static_cast<size_t>(count));
      for (int i = 0; i < count; ++i) {
        args.push_back(GenTyped(g, GenType::kString, depth - 1, config).expr);
      }
      return {.expr = FunctionCallExp("concat", std::move(args)),
              .type = GenType::kString};
    }
    case 4: {
      TypedExpr str = GenTyped(g, GenType::kString, depth - 1, config);
      Expression pos =
          ConstantValueExp(Value(static_cast<int64_t>(g.Pick(-3, 4))));
      if (g.Chance(50)) {
        Expression len =
            ConstantValueExp(Value(static_cast<int64_t>(g.Pick(0, 4))));
        return {.expr = FunctionCallExp(
                    "substr",
                    {std::move(str.expr), std::move(pos), std::move(len)}),
                .type = GenType::kString};
      }
      return {.expr = FunctionCallExp("substr",
                                      {std::move(str.expr), std::move(pos)}),
              .type = GenType::kString};
    }
    case 5: {
      TypedExpr str = GenTyped(g, GenType::kString, depth - 1, config);
      TypedExpr from = GenLeaf(g, GenType::kString, config);
      TypedExpr to = GenLeaf(g, GenType::kString, config);
      return {.expr = FunctionCallExp(
                  "replace",
                  {std::move(str.expr), std::move(from.expr), std::move(to.expr)}),
              .type = GenType::kString};
    }
    case 6: {
      if (g.Chance(50)) {
        const int count = g.Pick(2, 3);
        std::vector<Expression> args;
        args.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
          args.push_back(GenTyped(g, GenType::kString, depth - 1, config).expr);
        }
        return {.expr = FunctionCallExp("coalesce", std::move(args)),
                .type = GenType::kString};
      }
      // NULLIF takes exactly 2 arguments.
      TypedExpr first = GenTyped(g, GenType::kString, depth - 1, config);
      TypedExpr second = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp(
                  "nullif", {std::move(first.expr), std::move(second.expr)}),
              .type = GenType::kString};
    }
    case 7: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp("reverse", {std::move(child.expr)}),
              .type = GenType::kString};
    }
    case 8: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      Expression count =
          ConstantValueExp(Value(static_cast<int64_t>(g.Pick(0, 3))));
      return {.expr = FunctionCallExp(
                  "repeat", {std::move(child.expr), std::move(count)}),
              .type = GenType::kString};
    }
    case 9: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      return {.expr = FunctionCallExp("initcap", {std::move(child.expr)}),
              .type = GenType::kString};
    }
    case 10: {
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      Expression len =
          ConstantValueExp(Value(static_cast<int64_t>(g.Pick(0, 6))));
      Expression pad = ConstantValueExp(Value(std::string("*")));
      const char* fn = g.Chance(50) ? "lpad" : "rpad";
      return {.expr = FunctionCallExp(
                  fn, {std::move(child.expr), std::move(len), std::move(pad)}),
              .type = GenType::kString};
    }
    case 11: {
      const char* fn = g.Chance(50) ? "left" : "right";
      TypedExpr child = GenTyped(g, GenType::kString, depth - 1, config);
      Expression len =
          ConstantValueExp(Value(static_cast<int64_t>(g.Pick(0, 4))));
      return {.expr = FunctionCallExp(
                  fn, {std::move(child.expr), std::move(len)}),
              .type = GenType::kString};
    }
    case 12: {
      Expression code =
          ConstantValueExp(Value(static_cast<int64_t>(g.Pick(65, 90))));
      return {.expr = FunctionCallExp("chr", {std::move(code)}),
              .type = GenType::kString};
    }
    default: {
      const int branches = g.Pick(1, 2);
      std::vector<std::pair<Expression, Expression>> whens;
      whens.reserve(static_cast<size_t>(branches));
      for (int i = 0; i < branches; ++i) {
        TypedExpr when = GenTyped(g, GenType::kBool, depth - 1, config);
        TypedExpr then = GenTyped(g, GenType::kString, depth - 1, config);
        whens.emplace_back(std::move(when.expr), std::move(then.expr));
      }
      Expression otherwise =
          g.Chance(70) ? GenTyped(g, GenType::kString, depth - 1, config).expr
                       : nullptr;
      return {.expr = CaseExpressionExp(std::move(whens), std::move(otherwise)),
              .type = GenType::kString};
    }
  }
}

TypedExpr GenTyped(Gen& g, GenType want, int depth,
                   const ExprGenConfig& config) {
  if (depth <= 0 || g.Chance(30)) {
    return GenLeaf(g, want, config);
  }
  switch (want) {
    case GenType::kInt:
    case GenType::kFloat: {
      // CASE with numeric branches widens like the arithmetic ops.
      if (g.Chance(20)) {
        GenType branch = g.Chance(50) ? GenType::kInt : GenType::kFloat;
        const int branches = g.Pick(1, 2);
        std::vector<std::pair<Expression, Expression>> whens;
        GenType result = GenType::kInt;
        for (int i = 0; i < branches; ++i) {
          TypedExpr value = GenTyped(g, branch, depth - 1, config);
          result = (result == GenType::kFloat || value.type == GenType::kFloat)
                       ? GenType::kFloat
                       : GenType::kInt;
          whens.emplace_back(
              GenTyped(g, GenType::kBool, depth - 1, config).expr,
              std::move(value.expr));
        }
        Expression otherwise = nullptr;
        if (g.Chance(70)) {
          TypedExpr value = GenTyped(g, branch, depth - 1, config);
          result = (result == GenType::kFloat || value.type == GenType::kFloat)
                       ? GenType::kFloat
                       : GenType::kInt;
          otherwise = std::move(value.expr);
        }
        return {
            .expr = CaseExpressionExp(std::move(whens), std::move(otherwise)),
            .type = result};
      }
      TypedExpr numeric = GenNumeric(g, depth, config);
      if (want == GenType::kInt && numeric.type == GenType::kFloat) {
        // Narrow via CAST so the caller keeps its static type.
        return {.expr = CastExpressionExp(std::move(numeric.expr), "INT64"),
                .type = GenType::kInt};
      }
      if (want == GenType::kFloat && numeric.type == GenType::kInt) {
        return {.expr = CastExpressionExp(std::move(numeric.expr), "FLOAT64"),
                .type = GenType::kFloat};
      }
      return numeric;
    }
    case GenType::kBool:
      return GenBool(g, depth, config);
    case GenType::kString:
      return GenString(g, depth, config);
  }
  return GenLeaf(g, want, config);
}

// ---- Serializers -----------------------------------------------------------

std::string QuoteString(const std::string& s) {
  std::string out = "'";
  for (char c : s) {
    out += (c == '\'') ? "''" : std::string(1, c);
  }
  out += "'";
  return out;
}

std::string FormatDouble(double d) {
  std::array<char, 64> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), d);
  (void)ec;
  return std::string{buffer.data(), static_cast<size_t>(ptr - buffer.data())};
}

std::string NullSql(GenType type) {
  switch (type) {
    case GenType::kInt:
      return "CAST(NULL AS INT64)";
    case GenType::kFloat:
      return "CAST(NULL AS FLOAT64)";
    case GenType::kBool:
      return "CAST(NULL AS BOOL)";
    case GenType::kString:
      return "CAST(NULL AS STRING)";
  }
  return "CAST(NULL AS INT64)";
}

}  // namespace

std::string FormatSimplifyValue(const Value& value) {
  if (value.IsNull()) {
    return "NULL";
  }
  switch (value.type) {
    case ValueType::kInt64:
      return std::to_string(value.value.int_value);
    case ValueType::kDouble: {
      const double d = value.value.double_value;
      if (std::isnan(d)) {
        return "nan";
      }
      if (std::isinf(d)) {
        return d > 0 ? "inf" : "-inf";
      }
      return FormatDouble(d);
    }
    case ValueType::kVarChar:
      return QuoteString(std::string(value.value.varchar_value));
    default:
      return value.AsString();
  }
}

bool SimplifyValuesEqual(const Value& a, const Value& b) {
  if (a.IsNull() && b.IsNull()) {
    return true;
  }
  if (a.IsNull() != b.IsNull()) {
    return false;
  }
  if (a.type != b.type) {
    return false;
  }
  if (a.type == ValueType::kDouble) {
    const double x = a.value.double_value;
    const double y = b.value.double_value;
    if (std::isnan(x) && std::isnan(y)) {
      return true;
    }
    if (std::isinf(x) && std::isinf(y)) {
      return (x > 0) == (y > 0);
    }
    return x == y;
  }
  return a == b;
}

namespace {

// SQL serializer needs the generator static type for untyped NULLs, so it
// re-derives the type bottom-up with the same coercion the generator used.
GenType InferType(const Expression& expr) {
  if (!expr) {
    return GenType::kInt;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue: {
      const Value& v = expr->AsConstantValue().GetValue();
      if (v.IsNull()) {
        return GenType::kInt;
      }
      if (v.type == ValueType::kDouble) {
        return GenType::kFloat;
      }
      if (v.type == ValueType::kVarChar) {
        return GenType::kString;
      }
      return GenType::kInt;
    }
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      if (bin.Op() == BinaryOperation::kAnd ||
          bin.Op() == BinaryOperation::kOr ||
          bin.Op() == BinaryOperation::kXor ||
          bin.Op() == BinaryOperation::kLike ||
          bin.Op() == BinaryOperation::kNotLike ||
          bin.Op() == BinaryOperation::kEquals ||
          bin.Op() == BinaryOperation::kNotEquals ||
          bin.Op() == BinaryOperation::kLessThan ||
          bin.Op() == BinaryOperation::kLessThanEquals ||
          bin.Op() == BinaryOperation::kGreaterThan ||
          bin.Op() == BinaryOperation::kGreaterThanEquals ||
          bin.Op() == BinaryOperation::kIsDistinctFrom ||
          bin.Op() == BinaryOperation::kIsNotDistinctFrom) {
        return GenType::kBool;
      }
      if (bin.Op() == BinaryOperation::kModulo ||
          bin.Op() == BinaryOperation::kShiftLeft ||
          bin.Op() == BinaryOperation::kShiftRight) {
        return GenType::kInt;
      }
      if (bin.Op() == BinaryOperation::kDivide) {
        return GenType::kFloat;
      }
      return (InferType(bin.Left()) == GenType::kFloat ||
              InferType(bin.Right()) == GenType::kFloat)
                 ? GenType::kFloat
                 : GenType::kInt;
    }
    case TypeTag::kUnaryExp:
      switch (expr->AsUnaryExpression().Op()) {
        case UnaryOperation::kMinus:
        case UnaryOperation::kBitwiseNot:
          return InferType(expr->AsUnaryExpression().Child());
        default:
          return GenType::kBool;
      }
    case TypeTag::kCaseExp: {
      const auto& c = expr->AsCaseExpression();
      GenType result = GenType::kInt;
      bool first = true;
      for (const auto& [cond, value] : c.when_clauses_) {
        (void)cond;
        GenType t = InferType(value);
        if (first) {
          result = t;
          first = false;
        } else if (t != result) {
          if (t == GenType::kFloat || result == GenType::kFloat) {
            result = GenType::kFloat;
          } else if (t == GenType::kBool || result == GenType::kBool) {
            result = GenType::kBool;
          }
        }
      }
      if (c.else_clause_) {
        GenType t = InferType(c.else_clause_);
        if (first) {
          result = t;
        } else if (t != result) {
          if (t == GenType::kFloat || result == GenType::kFloat) {
            result = GenType::kFloat;
          } else if (t == GenType::kBool || result == GenType::kBool) {
            result = GenType::kBool;
          }
        }
      }
      return result;
    }
    case TypeTag::kInExp:
      return GenType::kBool;
    case TypeTag::kFunctionCallExp: {
      const auto& call = expr->AsFunctionCallExpression();
      const std::string& name = call.FuncName();
      if (name == "upper" || name == "lower" || name == "concat" ||
          name == "substr" || name == "substring" || name == "trim" ||
          name == "ltrim" || name == "rtrim" || name == "replace" ||
          name == "repeat" || name == "reverse") {
        return GenType::kString;
      }
      if (name == "starts_with" || name == "ends_with") {
        return GenType::kBool;
      }
      if (name == "length" || name == "char_length" ||
          name == "character_length" || name == "byte_length" ||
          name == "strpos" || name == "instr" ||
          name == "__bit_and" || name == "__bit_or" ||
          name == "__bit_xor" || name == "__shift_left" ||
          name == "__shift_right" || name == "div") {
        return GenType::kInt;
      }
      if (name == "pow" || name == "power" || name == "sqrt" ||
          name == "cbrt" || name == "ln" || name == "log" ||
          name == "log10" || name == "exp" || name == "cos" ||
          name == "sin" || name == "tan" || name == "acos" ||
          name == "asin" || name == "atan" || name == "atan2" ||
          name == "cosh" || name == "sinh" || name == "tanh" ||
          name == "pi" || name == "radians" || name == "degrees" ||
          name == "ieee_divide" || name == "safe_divide") {
        return GenType::kFloat;
      }
      if (name == "greatest" || name == "least") {
        for (const auto& arg : call.Args()) {
          if (InferType(arg) == GenType::kFloat) {
            return GenType::kFloat;
          }
        }
        if (!call.Args().empty()) {
          return InferType(call.Args().front());
        }
        return GenType::kInt;
      }
      if (name == "safe_add" || name == "safe_subtract" ||
          name == "safe_multiply") {
        for (const auto& arg : call.Args()) {
          if (InferType(arg) == GenType::kFloat) {
            return GenType::kFloat;
          }
        }
        return GenType::kInt;
      }
      if (name == "abs" || name == "round" || name == "trunc" ||
          name == "truncate" || name == "ceil" || name == "ceiling" ||
          name == "floor" || name == "sign") {
        if (!call.Args().empty()) {
          return InferType(call.Args().front());
        }
        return GenType::kInt;
      }
      if (name == "nullif" || name == "ifnull") {
        if (!call.Args().empty()) {
          return InferType(call.Args().front());
        }
        return GenType::kInt;
      }
      if (name == "coalesce") {
        for (const auto& arg : call.Args()) {
          if (InferType(arg) == GenType::kFloat) {
            return GenType::kFloat;
          }
        }
        if (!call.Args().empty()) {
          return InferType(call.Args().front());
        }
        return GenType::kInt;
      }
      if (name == "mod") {
        for (const auto& arg : call.Args()) {
          if (InferType(arg) == GenType::kFloat) {
            return GenType::kFloat;
          }
        }
        return GenType::kInt;
      }
      return GenType::kInt;
    }
    case TypeTag::kCastExp: {
      const std::string& target = expr->AsCastExpression().TargetTypeName();
      if (target == "FLOAT64" || target == "DOUBLE" || target == "FLOAT") {
        return GenType::kFloat;
      }
      if (target == "BOOL" || target == "BOOLEAN") {
        return GenType::kBool;
      }
      if (target == "STRING" || target == "VARCHAR" || target == "TEXT") {
        return GenType::kString;
      }
      return GenType::kInt;
    }
    default:
      return GenType::kInt;
  }
}

std::string ConstantSql(const Value& v, GenType type) {
  if (v.IsNull()) {
    return NullSql(type);
  }
  if (v.type == ValueType::kInt64) {
    if (v.value.int_value == kInt64Min) {
      return "(-9223372036854775807 - 1)";
    }
    return std::to_string(v.value.int_value);
  }
  if (v.type == ValueType::kDouble) {
    const double d = v.value.double_value;
    if (std::isnan(d)) {
      return "CAST('NaN' AS FLOAT64)";
    }
    if (std::isinf(d)) {
      return d > 0 ? "CAST('Infinity' AS FLOAT64)"
                   : "-CAST('Infinity' AS FLOAT64)";
    }
    // Integral doubles must keep a float marker: FormatDouble(3.0) is "3",
    // which would re-parse as an INT64 literal and change the static type.
    std::string text = FormatDouble(d);
    if (text.find_first_of(".eEnN") == std::string::npos) {
      text += ".0";
    }
    return text;
  }
  if (v.type == ValueType::kVarChar) {
    return QuoteString(std::string(v.value.varchar_value));
  }
  // Bools are stored as INT64 0/1.
  if (type == GenType::kBool) {
    return v.value.int_value != 0 ? "TRUE" : "FALSE";
  }
  return std::to_string(v.value.int_value);
}

std::string BinarySql(BinaryOperation op) {
  switch (op) {
    case BinaryOperation::kAdd:
      return "+";
    case BinaryOperation::kSubtract:
      return "-";
    case BinaryOperation::kMultiply:
      return "*";
    case BinaryOperation::kDivide:
      return "/";
    case BinaryOperation::kEquals:
      return "=";
    case BinaryOperation::kNotEquals:
      return "!=";
    case BinaryOperation::kLessThan:
      return "<";
    case BinaryOperation::kLessThanEquals:
      return "<=";
    case BinaryOperation::kGreaterThan:
      return ">";
    case BinaryOperation::kGreaterThanEquals:
      return ">=";
    case BinaryOperation::kLike:
      return "LIKE";
    case BinaryOperation::kNotLike:
      return "NOT LIKE";
    case BinaryOperation::kAnd:
      return "AND";
    case BinaryOperation::kOr:
      return "OR";
    case BinaryOperation::kXor:
      return "XOR";
    case BinaryOperation::kShiftLeft:
      return "<<";
    case BinaryOperation::kShiftRight:
      return ">>";
    case BinaryOperation::kIsDistinctFrom:
      return "IS DISTINCT FROM";
    case BinaryOperation::kIsNotDistinctFrom:
      return "IS NOT DISTINCT FROM";
    default:
      return "+";
  }
}

std::string UnarySql(UnaryOperation op, const std::string& child) {
  switch (op) {
    case UnaryOperation::kNot:
      return "(NOT " + child + ")";
    case UnaryOperation::kMinus:
      return "(-" + child + ")";
    case UnaryOperation::kBitwiseNot:
      return "(~" + child + ")";
    case UnaryOperation::kIsNull:
      return "(" + child + " IS NULL)";
    case UnaryOperation::kIsNotNull:
      return "(" + child + " IS NOT NULL)";
    case UnaryOperation::kIsTrue:
      return "(" + child + " IS TRUE)";
    case UnaryOperation::kIsNotTrue:
      return "(" + child + " IS NOT TRUE)";
    case UnaryOperation::kIsFalse:
      return "(" + child + " IS FALSE)";
    case UnaryOperation::kIsNotFalse:
      return "(" + child + " IS NOT FALSE)";
  }
  return child;
}

std::string Upper(std::string s) {
  for (char& c : s) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return s;
}

std::string SerializeSql(const Expression& expr, GenType type);

std::string SerializeSql(const Expression& expr, GenType type) {
  if (!expr) {
    return NullSql(type);
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      return ConstantSql(expr->AsConstantValue().GetValue(), type);
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      if (bin.Op() == BinaryOperation::kModulo) {
        return "MOD(" + SerializeSql(bin.Left(), GenType::kInt) + ", " +
               SerializeSql(bin.Right(), GenType::kInt) + ")";
      }
      if (bin.Op() == BinaryOperation::kShiftLeft ||
          bin.Op() == BinaryOperation::kShiftRight) {
        return "(" + SerializeSql(bin.Left(), GenType::kInt) + " " +
               BinarySql(bin.Op()) + " " +
               SerializeSql(bin.Right(), GenType::kInt) + ")";
      }
      if (bin.Op() == BinaryOperation::kIsDistinctFrom ||
          bin.Op() == BinaryOperation::kIsNotDistinctFrom) {
        return "(" + SerializeSql(bin.Left(), InferType(bin.Left())) + " " +
               BinarySql(bin.Op()) + " " +
               SerializeSql(bin.Right(), InferType(bin.Right())) + ")";
      }
      GenType element = GenType::kInt;
      if (bin.Op() == BinaryOperation::kAnd ||
          bin.Op() == BinaryOperation::kOr ||
          bin.Op() == BinaryOperation::kXor) {
        element = GenType::kBool;
      } else if (bin.Op() == BinaryOperation::kLike ||
                 bin.Op() == BinaryOperation::kNotLike) {
        element = GenType::kString;
      } else if (bin.Op() != BinaryOperation::kEquals &&
                 bin.Op() != BinaryOperation::kNotEquals &&
                 bin.Op() != BinaryOperation::kLessThan &&
                 bin.Op() != BinaryOperation::kLessThanEquals &&
                 bin.Op() != BinaryOperation::kGreaterThan &&
                 bin.Op() != BinaryOperation::kGreaterThanEquals) {
        element = InferType(expr);
      }
      return "(" + SerializeSql(bin.Left(), element) + " " +
             BinarySql(bin.Op()) + " " + SerializeSql(bin.Right(), element) +
             ")";
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expr->AsUnaryExpression();
      GenType child_type =
          (unary.Op() == UnaryOperation::kMinus ||
           unary.Op() == UnaryOperation::kBitwiseNot)
              ? InferType(expr)
              : (unary.Op() == UnaryOperation::kNot ||
                         unary.Op() == UnaryOperation::kIsTrue ||
                         unary.Op() == UnaryOperation::kIsNotTrue ||
                         unary.Op() == UnaryOperation::kIsFalse ||
                         unary.Op() == UnaryOperation::kIsNotFalse
                     ? GenType::kBool
                     : InferType(unary.Child()));
      return UnarySql(unary.Op(), SerializeSql(unary.Child(), child_type));
    }
    case TypeTag::kCaseExp: {
      const auto& c = expr->AsCaseExpression();
      GenType branch = InferType(expr);
      std::string out = "(CASE";
      for (const auto& [cond, value] : c.when_clauses_) {
        out += " WHEN " + SerializeSql(cond, GenType::kBool) + " THEN " +
               SerializeSql(value, branch);
      }
      if (c.else_clause_) {
        out += " ELSE " + SerializeSql(c.else_clause_, branch);
      }
      return out + " END)";
    }
    case TypeTag::kInExp: {
      const auto& in = expr->AsInExpression();
      GenType element = InferType(in.child_);
      std::string out = "(" + SerializeSql(in.child_, element) + " IN (";
      for (size_t i = 0; i < in.list_.size(); ++i) {
        if (i != 0) {
          out += ", ";
        }
        out += SerializeSql(in.list_[i], element);
      }
      return out + "))";
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expr->AsFunctionCallExpression();
      const std::string& fname = call.FuncName();
      if (fname == "__bit_and" && call.Args().size() == 2) {
        return "(" + SerializeSql(call.Args()[0], GenType::kInt) + " & " +
               SerializeSql(call.Args()[1], GenType::kInt) + ")";
      }
      if (fname == "__bit_or" && call.Args().size() == 2) {
        return "(" + SerializeSql(call.Args()[0], GenType::kInt) + " | " +
               SerializeSql(call.Args()[1], GenType::kInt) + ")";
      }
      if (fname == "__bit_xor" && call.Args().size() == 2) {
        return "(" + SerializeSql(call.Args()[0], GenType::kInt) + " ^ " +
               SerializeSql(call.Args()[1], GenType::kInt) + ")";
      }
      if (fname == "__shift_left" && call.Args().size() == 2) {
        return "(" + SerializeSql(call.Args()[0], GenType::kInt) + " << " +
               SerializeSql(call.Args()[1], GenType::kInt) + ")";
      }
      if (fname == "__shift_right" && call.Args().size() == 2) {
        return "(" + SerializeSql(call.Args()[0], GenType::kInt) + " >> " +
               SerializeSql(call.Args()[1], GenType::kInt) + ")";
      }
      if ((fname == "substr" || fname == "substring") &&
          call.Args().size() >= 2) {
        std::string out =
            "SUBSTR(" + SerializeSql(call.Args()[0], GenType::kString);
        for (size_t i = 1; i < call.Args().size(); ++i) {
          out += ", " + SerializeSql(call.Args()[i], GenType::kInt);
        }
        return out + ")";
      }
      if (fname == "length" || fname == "char_length" ||
          fname == "character_length" || fname == "byte_length") {
        return Upper(fname) + "(" +
               SerializeSql(call.Args()[0], GenType::kString) + ")";
      }
      if (fname == "starts_with" || fname == "ends_with" ||
          fname == "strpos" || fname == "instr") {
        return Upper(fname) + "(" +
               SerializeSql(call.Args()[0], GenType::kString) + ", " +
               SerializeSql(call.Args()[1], GenType::kString) + ")";
      }
      if (fname == "upper" || fname == "lower" || fname == "trim" ||
          fname == "ltrim" || fname == "rtrim" || fname == "reverse") {
        std::string out = Upper(fname) + "(";
        for (size_t i = 0; i < call.Args().size(); ++i) {
          if (i != 0) out += ", ";
          out += SerializeSql(call.Args()[i], GenType::kString);
        }
        return out + ")";
      }
      if (fname == "replace" || fname == "concat") {
        std::string out = Upper(fname) + "(";
        for (size_t i = 0; i < call.Args().size(); ++i) {
          if (i != 0) out += ", ";
          out += SerializeSql(call.Args()[i], GenType::kString);
        }
        return out + ")";
      }
      const std::string name = Upper(fname);
      std::string out = name + "(";
      GenType element = InferType(expr);
      for (size_t i = 0; i < call.Args().size(); ++i) {
        if (i != 0) {
          out += ", ";
        }
        GenType arg_type = element;
        if ((fname == "round" || fname == "trunc" || fname == "truncate") &&
            i == 1) {
          arg_type = GenType::kInt;
        }
        out += SerializeSql(call.Args()[i], arg_type);
      }
      return out + ")";
    }
    case TypeTag::kCastExp: {
      const auto& cast = expr->AsCastExpression();
      GenType child = GenType::kInt;
      if (cast.TargetTypeName() == "FLOAT64" ||
          cast.TargetTypeName() == "INT64") {
        child = InferType(cast.Child());
      }
      return "(CAST(" + SerializeSql(cast.Child(), child) + " AS " +
             cast.TargetTypeName() + "))";
    }
    default:
      return NullSql(type);
  }
}

std::string SerializeSExpr(const Expression& expr, GenType type);

std::string ConstantSExpr(const Value& v, GenType type) {
  if (v.IsNull()) {
    switch (type) {
      case GenType::kInt:
        return "(n int)";
      case GenType::kFloat:
        return "(n float)";
      case GenType::kBool:
        return "(n bool)";
      case GenType::kString:
        return "(n string)";
    }
  }
  if (v.type == ValueType::kInt64) {
    if (type == GenType::kBool) {
      return v.value.int_value != 0 ? "(b true)" : "(b false)";
    }
    return "(i " + std::to_string(v.value.int_value) + ")";
  }
  if (v.type == ValueType::kDouble) {
    const double d = v.value.double_value;
    if (std::isnan(d)) {
      return "(f nan)";
    }
    if (std::isinf(d)) {
      return d > 0 ? "(f inf)" : "(f -inf)";
    }
    return "(f " + FormatDouble(d) + ")";
  }
  if (v.type == ValueType::kVarChar) {
    return "(s " + QuoteString(std::string(v.value.varchar_value)) + ")";
  }
  return "(n int)";
}

std::string BinarySExpr(BinaryOperation op) {
  switch (op) {
    case BinaryOperation::kAdd:
      return "add";
    case BinaryOperation::kSubtract:
      return "sub";
    case BinaryOperation::kMultiply:
      return "mul";
    case BinaryOperation::kDivide:
      return "div";
    case BinaryOperation::kModulo:
      return "mod";
    case BinaryOperation::kEquals:
      return "eq";
    case BinaryOperation::kNotEquals:
      return "ne";
    case BinaryOperation::kLessThan:
      return "lt";
    case BinaryOperation::kLessThanEquals:
      return "le";
    case BinaryOperation::kGreaterThan:
      return "gt";
    case BinaryOperation::kGreaterThanEquals:
      return "ge";
    case BinaryOperation::kLike:
      return "like";
    case BinaryOperation::kNotLike:
      return "nlike";
    case BinaryOperation::kAnd:
      return "and";
    case BinaryOperation::kOr:
      return "or";
    case BinaryOperation::kXor:
      return "xor";
    case BinaryOperation::kShiftLeft:
      return "shl";
    case BinaryOperation::kShiftRight:
      return "shr";
    case BinaryOperation::kIsDistinctFrom:
      return "distinct";
    case BinaryOperation::kIsNotDistinctFrom:
      return "notdistinct";
    default:
      return "add";
  }
}

std::string UnarySExpr(UnaryOperation op) {
  switch (op) {
    case UnaryOperation::kNot:
      return "not";
    case UnaryOperation::kMinus:
      return "neg";
    case UnaryOperation::kBitwiseNot:
      return "bitnot";
    case UnaryOperation::kIsNull:
      return "isnull";
    case UnaryOperation::kIsNotNull:
      return "isnotnull";
    case UnaryOperation::kIsTrue:
      return "istrue";
    case UnaryOperation::kIsNotTrue:
      return "isnottrue";
    case UnaryOperation::kIsFalse:
      return "isfalse";
    case UnaryOperation::kIsNotFalse:
      return "isnotfalse";
  }
  return "not";
}

std::string SerializeSExpr(const Expression& expr, GenType type) {
  if (!expr) {
    return "(n int)";
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      return ConstantSExpr(expr->AsConstantValue().GetValue(), type);
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      // Operand types are inferred per-operand: the comparison node's own
      // boolean type must never leak into its numeric operands (serializing
      // -5 as `(b true)` is lossy and broke the Python cross-check).
      GenType left_type = InferType(bin.Left());
      GenType right_type = InferType(bin.Right());
      if (bin.Op() == BinaryOperation::kAnd ||
          bin.Op() == BinaryOperation::kOr ||
          bin.Op() == BinaryOperation::kXor) {
        left_type = GenType::kBool;
        right_type = GenType::kBool;
      } else if (bin.Op() == BinaryOperation::kLike ||
                 bin.Op() == BinaryOperation::kNotLike) {
        left_type = GenType::kString;
        right_type = GenType::kString;
      } else if (bin.Op() == BinaryOperation::kModulo ||
                 bin.Op() == BinaryOperation::kShiftLeft ||
                 bin.Op() == BinaryOperation::kShiftRight) {
        left_type = GenType::kInt;
        right_type = GenType::kInt;
      }
      return "(" + BinarySExpr(bin.Op()) + " " +
             SerializeSExpr(bin.Left(), left_type) + " " +
             SerializeSExpr(bin.Right(), right_type) + ")";
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expr->AsUnaryExpression();
      GenType child =
          (unary.Op() == UnaryOperation::kMinus ||
           unary.Op() == UnaryOperation::kBitwiseNot)
              ? InferType(expr)
              : GenType::kBool;
      if (unary.Op() != UnaryOperation::kMinus &&
          unary.Op() != UnaryOperation::kBitwiseNot &&
          unary.Op() != UnaryOperation::kNot &&
          unary.Op() != UnaryOperation::kIsTrue &&
          unary.Op() != UnaryOperation::kIsNotTrue &&
          unary.Op() != UnaryOperation::kIsFalse &&
          unary.Op() != UnaryOperation::kIsNotFalse) {
        child = InferType(unary.Child());
      }
      return "(" + UnarySExpr(unary.Op()) + " " +
             SerializeSExpr(unary.Child(), child) + ")";
    }
    case TypeTag::kCaseExp: {
      const auto& c = expr->AsCaseExpression();
      GenType branch = InferType(expr);
      std::string out = "(case";
      for (const auto& [cond, value] : c.when_clauses_) {
        out += " (" + SerializeSExpr(cond, GenType::kBool) + " " +
               SerializeSExpr(value, branch) + ")";
      }
      out += " ";
      out +=
          c.else_clause_ ? SerializeSExpr(c.else_clause_, branch) : "(n int)";
      return out + ")";
    }
    case TypeTag::kInExp: {
      const auto& in = expr->AsInExpression();
      GenType element = InferType(in.child_);
      std::string out = "(in " + SerializeSExpr(in.child_, element);
      for (const Expression& item : in.list_) {
        out += " " + SerializeSExpr(item, element);
      }
      return out + ")";
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expr->AsFunctionCallExpression();
      const std::string& fname = call.FuncName();
      std::string out = "(" + fname;
      if (fname == "__bit_and" || fname == "__bit_or" ||
          fname == "__bit_xor" || fname == "__shift_left" ||
          fname == "__shift_right") {
        for (const Expression& arg : call.Args()) {
          out += " " + SerializeSExpr(arg, GenType::kInt);
        }
      } else if (fname == "substr" || fname == "substring") {
        if (!call.Args().empty()) {
          out += " " + SerializeSExpr(call.Args()[0], GenType::kString);
          for (size_t i = 1; i < call.Args().size(); ++i) {
            out += " " + SerializeSExpr(call.Args()[i], GenType::kInt);
          }
        }
      } else if (fname == "length" || fname == "char_length" ||
                 fname == "character_length" || fname == "byte_length") {
        for (const Expression& arg : call.Args()) {
          out += " " + SerializeSExpr(arg, GenType::kString);
        }
      } else if (fname == "starts_with" || fname == "ends_with" ||
                 fname == "upper" || fname == "lower" || fname == "trim" ||
                 fname == "ltrim" || fname == "rtrim" || fname == "reverse" ||
                 fname == "replace" || fname == "concat") {
        for (const Expression& arg : call.Args()) {
          out += " " + SerializeSExpr(arg, GenType::kString);
        }
      } else {
        GenType element = InferType(expr);
        for (size_t i = 0; i < call.Args().size(); ++i) {
          GenType arg_type = element;
          if ((fname == "round" || fname == "trunc" || fname == "truncate") &&
              i == 1) {
            arg_type = GenType::kInt;
          }
          out += " " + SerializeSExpr(call.Args()[i], arg_type);
        }
      }
      return out + ")";
    }
    case TypeTag::kCastExp: {
      const auto& cast = expr->AsCastExpression();
      std::string tag = "cast-int";
      if (cast.TargetTypeName() == "FLOAT64") {
        tag = "cast-float";
      } else if (cast.TargetTypeName() == "BOOL") {
        tag = "cast-bool";
      } else if (cast.TargetTypeName() == "STRING") {
        tag = "cast-string";
      }
      return "(" + tag + " " +
             SerializeSExpr(cast.Child(), InferType(cast.Child())) + ")";
    }
    default:
      return "(n int)";
  }
}

struct EvalOutcome {
  bool threw = false;
  std::string error;
  Value value;
};

EvalOutcome EvaluateTree(const Expression& expr) {
  EvalOutcome outcome;
  try {
    outcome.value = expr->Evaluate(Row(), Schema());
  } catch (const std::exception& error) {
    outcome.threw = true;
    outcome.error = error.what();
  } catch (...) {
    outcome.threw = true;
    outcome.error = "unknown exception";
  }
  return outcome;
}

}  // namespace

std::string ToSimplifySql(const Expression& expr) {
  return SerializeSql(expr, InferType(expr));
}

std::string ToSimplifySExpr(const Expression& expr) {
  return SerializeSExpr(expr, InferType(expr));
}

GeneratedExpr GenerateSimplifyExpr(std::mt19937& rng,
                                   const ExprGenConfig& config) {
  Gen g(rng);
  ExprGenConfig bounded = config;
  bounded.max_depth = std::clamp(bounded.max_depth, 1, 6);
  TypedExpr typed = GenTyped(g, RandomType(g), bounded.max_depth, bounded);
  GeneratedExpr out;
  out.expr = std::move(typed.expr);
  GenType inferred = InferType(out.expr);
  out.sql = SerializeSql(out.expr, inferred);
  out.sexpr = SerializeSExpr(out.expr, inferred);
  return out;
}

std::string CheckSimplifyEquivalence(const Expression& expr) {
  if (!expr) {
    return "null expression";
  }
  const EvalOutcome original = EvaluateTree(expr);
  Expression rewritten;
  try {
    rewritten = ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expr);
    rewritten = RewriteTypedArithmetic(rewritten, Schema());
  } catch (const std::exception& error) {
    return std::string("rewriter threw: ") + error.what() +
           "\noriginal: " + expr->ToString();
  } catch (...) {
    return "rewriter threw an unknown exception\noriginal: " + expr->ToString();
  }
  const EvalOutcome simplified = EvaluateTree(rewritten);
  if (original.threw != simplified.threw) {
    std::ostringstream detail;
    detail << "throw mismatch\noriginal: " << expr->ToString()
           << "\nrewritten: " << rewritten->ToString() << "\noriginal "
           << (original.threw ? std::string("threw: ") + original.error
                              : "value: " + FormatSimplifyValue(original.value))
           << "\nrewritten "
           << (simplified.threw
                   ? std::string("threw: ") + simplified.error
                   : "value: " + FormatSimplifyValue(simplified.value))
           << "\nsql: SELECT " << ToSimplifySql(expr)
           << "\nsexpr: " << ToSimplifySExpr(expr);
    return detail.str();
  }
  if (!original.threw &&
      !SimplifyValuesEqual(original.value, simplified.value)) {
    std::ostringstream detail;
    detail << "value mismatch\noriginal: " << expr->ToString()
           << "\nrewritten: " << rewritten->ToString()
           << "\noriginal value: " << FormatSimplifyValue(original.value)
           << "\nrewritten value: " << FormatSimplifyValue(simplified.value)
           << "\nsql: SELECT " << ToSimplifySql(expr)
           << "\nsexpr: " << ToSimplifySExpr(expr);
    return detail.str();
  }
  return "";
}

Expression ShrinkSimplifyCounterexample(const Expression& expr) {
  if (!expr || CheckSimplifyEquivalence(expr).empty()) {
    return expr;
  }
  // Typed-neutral replacements ordered from most to least destructive.
  const std::vector<Expression> kReplacements = {
      ConstantValueExp(Value()),
      ConstantValueExp(Value(int64_t{0})),
      ConstantValueExp(Value(int64_t{1})),
      ConstantValueExp(Value(true)),
      ConstantValueExp(Value(0.0)),
      ConstantValueExp(Value(std::string("")))};
  std::function<Expression(const Expression&)> shrink =
      [&](const Expression& node) -> Expression {
    if (!node) {
      return node;
    }
    for (const Expression& replacement : kReplacements) {
      if (CheckSimplifyEquivalence(replacement).empty()) {
        continue;  // replacement must itself still mismatch to be useful
      }
      // A replacement that preserves the mismatch is strictly smaller.
      return replacement;
    }
    std::vector<Expression> children = ExpressionChildren(node);
    bool changed = false;
    for (Expression& child : children) {
      Expression shrunk = shrink(child);
      if (shrunk != child) {
        child = std::move(shrunk);
        changed = true;
      }
    }
    if (!changed) {
      return node;
    }
    Expression candidate;
    try {
      candidate = WithExpressionChildren(node, children);
    } catch (...) {
      return node;
    }
    if (!CheckSimplifyEquivalence(candidate).empty()) {
      return candidate;
    }
    return node;
  };
  Expression current = expr;
  for (int i = 0; i < 8; ++i) {
    Expression next = shrink(current);
    if (next == current) {
      break;
    }
    current = std::move(next);
  }
  return current;
}

}  // namespace tinylamb
