/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/collation.hpp"

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// The collator argument of a COLLATE call, when it is the string literal
// GoogleSQL requires. Anything else (a column, a parameter, a nested call) is
// resolved per row at evaluation time instead, so it contributes no static
// collation — which is why this is optional and not a plain string_view: an
// empty *literal* is still a collator (COLLATE(x, '') folds), while no
// literal at all is not.
std::optional<std::string_view> LiteralCollationSpec(
    const Expression& collator) {
  if (collator == nullptr || collator->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const ConstantValue& constant = collator->AsConstantValue();
  const Value& value = constant.GetValue();
  if (value.type != ValueType::kVarChar) {
    return std::nullopt;
  }
  return std::string_view(value.value.varchar_value);
}

}  // namespace

Collation CollationOfExpression(const Expression& expression) {
  if (expression == nullptr ||
      expression->Type() != TypeTag::kFunctionCallExp) {
    return Collation::kNone;
  }
  const FunctionCallExpression& call = expression->AsFunctionCallExpression();
  if (call.FuncName() != "collate" || call.Args().size() != 2) {
    return Collation::kNone;
  }
  const std::optional<std::string_view> spec =
      LiteralCollationSpec(call.Args()[1]);
  if (!spec.has_value()) {
    return Collation::kNone;
  }
  // The same resolution COLLATE() itself applies at evaluation time, so the
  // static answer and the tag a value carries never disagree.
  return CollationOfSpec(*spec);
}

std::optional<CollatedOrdinal> AsCollatedOrdinal(const Expression& key) {
  if (key == nullptr || key->Type() != TypeTag::kFunctionCallExp) {
    return std::nullopt;
  }
  const FunctionCallExpression& call = key->AsFunctionCallExpression();
  if (call.FuncName() != "collate" || call.Args().size() != 2) {
    return std::nullopt;
  }
  const Expression& operand = call.Args()[0];
  const Expression& spec = call.Args()[1];
  if (operand == nullptr || operand->Type() != TypeTag::kConstantValue ||
      spec == nullptr || spec->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const Value& ordinal = operand->AsConstantValue().GetValue();
  const Value& collator = spec->AsConstantValue().GetValue();
  if (ordinal.type != ValueType::kInt64 || ordinal.IsNull() ||
      collator.type != ValueType::kVarChar) {
    return std::nullopt;
  }
  return CollatedOrdinal{.ordinal = ordinal.value.int_value,
                         .spec = std::string(collator.value.varchar_value)};
}

Expression ApplyCollationSpec(Expression expression, std::string_view spec) {
  return FunctionCallExp(
      "collate",
      std::vector<Expression>{std::move(expression),
                              ConstantValueExp(Value(std::string(spec)))});
}

Expression ReapplyCollation(Expression resolved, const Expression& parsed_key) {
  if (resolved == nullptr || parsed_key == nullptr ||
      parsed_key->Type() != TypeTag::kFunctionCallExp) {
    return resolved;
  }
  const FunctionCallExpression& call = parsed_key->AsFunctionCallExpression();
  if (call.FuncName() != "collate" || call.Args().size() != 2) {
    return resolved;
  }
  return FunctionCallExp(
      "collate", std::vector<Expression>{std::move(resolved), call.Args()[1]});
}

Collation CollationOfOrdering(const std::vector<Expression>& keys) {
  Collation resolved = Collation::kNone;
  for (const Expression& key : keys) {
    resolved = ResolveCollation(resolved, CollationOfExpression(key));
  }
  return resolved;
}

}  // namespace tinylamb
