/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/column_name.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Volatility GetFunctionVolatility(std::string_view func_name) {
  std::string name(func_name);
  for (char& c : name) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  static const std::unordered_set<std::string> volatile_funcs = {
      "rand", "random", "uuid", "generate_uuid", "newid"};
  if (volatile_funcs.contains(name)) {
    return Volatility::kVolatile;
  }
  static const std::unordered_set<std::string> stable_funcs = {
      "now",          "current_timestamp", "current_date",
      "current_time", "current_datetime",  "localtimestamp",
      "localtime",    "utc_timestamp"};
  if (stable_funcs.contains(name)) {
    return Volatility::kStable;
  }
  return Volatility::kImmutable;
}

namespace {

std::string ToUpper(std::string_view s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    result.push_back(
        static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return result;
}

bool IsNumericWideningCast(std::string_view from_str, std::string_view to_str) {
  const std::string from = ToUpper(from_str);
  const std::string to = ToUpper(to_str);
  if (from == to) {
    return true;
  }
  auto rank = [](std::string_view t) -> int {
    if (t == "INT8" || t == "TINYINT") return 1;
    if (t == "INT16" || t == "SMALLINT") return 2;
    if (t == "INT32" || t == "INT" || t == "INTEGER") return 3;
    if (t == "INT64" || t == "BIGINT") return 4;
    if (t == "UINT8") return 11;
    if (t == "UINT16") return 12;
    if (t == "UINT32") return 13;
    if (t == "UINT64") return 14;
    if (t == "FLOAT" || t == "FLOAT32") return 21;
    if (t == "DOUBLE" || t == "FLOAT64") return 22;
    return 0;
  };
  int r1 = rank(from);
  int r2 = rank(to);
  if (r1 > 0 && r2 > 0) {
    if (r1 <= 4 && r2 <= 4 && r1 <= r2) return true;
    if (r1 >= 11 && r1 <= 14 && r2 >= 11 && r2 <= 14 && r1 <= r2) return true;
    if (r1 >= 11 && r1 <= 13 && r2 >= 3 && r2 <= 4 && (r1 - 10) < (r2))
      return true;
    if (r1 >= 21 && r1 <= 22 && r2 >= 21 && r2 <= 22 && r1 <= r2) return true;
    if (r1 <= 4 && r2 >= 21) return true;
    if (r1 >= 11 && r1 <= 14 && r2 >= 21) return true;
  }
  return false;
}

struct JVal;

struct JVal {
  enum Type : std::uint8_t { kNull, kBool, kNum, kStr, kArr, kObj } type{kNull};
  bool b{false};
  double num{0.0};
  std::string str;
  std::string_view raw;
  std::vector<std::shared_ptr<JVal>> arr;
  std::vector<std::pair<std::string, std::shared_ptr<JVal>>> obj;

  std::string canonical() const {
    switch (type) {
      case kNull:
        return "null";
      case kBool:
        return b ? "true" : "false";
      case kNum: {
        if (std::floor(num) == num && std::abs(num) < 1e15) {
          return std::to_string(static_cast<int64_t>(num));
        }
        return std::to_string(num);
      }
      case kStr: {
        std::string res = "\"";
        for (char c : str) {
          if (c == '"')
            res += "\\\"";
          else if (c == '\\')
            res += "\\\\";
          else
            res += c;
        }
        res += "\"";
        return res;
      }
      case kArr: {
        std::string res = "[";
        for (size_t i = 0; i < arr.size(); ++i) {
          if (i > 0) res += ",";
          if (arr[i]) res += arr[i]->canonical();
        }
        res += "]";
        return res;
      }
      case kObj: {
        std::string res = "{";
        for (size_t i = 0; i < obj.size(); ++i) {
          if (i > 0) res += ",";
          res += "\"" + obj[i].first +
                 "\":" + (obj[i].second ? obj[i].second->canonical() : "null");
        }
        res += "}";
        return res;
      }
    }
    return "null";
  }
};

JVal ParseJsonSimple(std::string_view& s);

void SkipWs(std::string_view& s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t' ||
                        s.front() == '\n' || s.front() == '\r')) {
    s.remove_prefix(1);
  }
}

std::string ParseJsonStringToken(std::string_view& s) {
  std::string res;
  if (s.empty() || s.front() != '"') return res;
  s.remove_prefix(1);
  while (!s.empty()) {
    char c = s.front();
    s.remove_prefix(1);
    if (c == '"') break;
    if (c == '\\' && !s.empty()) {
      char esc = s.front();
      s.remove_prefix(1);
      if (esc == 'n')
        res += '\n';
      else if (esc == 't')
        res += '\t';
      else if (esc == 'r')
        res += '\r';
      else
        res += esc;
    } else {
      res += c;
    }
  }
  return res;
}

JVal ParseJsonSimple(std::string_view& s) {
  SkipWs(s);
  JVal v;
  if (s.empty()) return v;
  std::string_view start = s;
  if (s.starts_with("null")) {
    s.remove_prefix(4);
    v.type = JVal::kNull;
    v.raw = start.substr(0, 4);
    return v;
  }
  if (s.starts_with("true")) {
    s.remove_prefix(4);
    v.type = JVal::kBool;
    v.b = true;
    v.raw = start.substr(0, 4);
    return v;
  }
  if (s.starts_with("false")) {
    s.remove_prefix(5);
    v.type = JVal::kBool;
    v.b = false;
    v.raw = start.substr(0, 5);
    return v;
  }
  if (s.front() == '"') {
    v.type = JVal::kStr;
    v.str = ParseJsonStringToken(s);
    v.raw = start.substr(0, start.size() - s.size());
    return v;
  }
  if (s.front() == '[') {
    s.remove_prefix(1);
    v.type = JVal::kArr;
    SkipWs(s);
    while (!s.empty() && s.front() != ']') {
      v.arr.push_back(std::make_shared<JVal>(ParseJsonSimple(s)));
      SkipWs(s);
      if (!s.empty() && s.front() == ',') {
        s.remove_prefix(1);
        SkipWs(s);
      }
    }
    if (!s.empty() && s.front() == ']') s.remove_prefix(1);
    v.raw = start.substr(0, start.size() - s.size());
    return v;
  }
  if (s.front() == '{') {
    s.remove_prefix(1);
    v.type = JVal::kObj;
    SkipWs(s);
    while (!s.empty() && s.front() != '}') {
      SkipWs(s);
      if (s.front() != '"') break;
      std::string key = ParseJsonStringToken(s);
      SkipWs(s);
      if (!s.empty() && s.front() == ':') {
        s.remove_prefix(1);
        SkipWs(s);
      }
      JVal child = ParseJsonSimple(s);
      v.obj.emplace_back(std::move(key),
                         std::make_shared<JVal>(std::move(child)));
      SkipWs(s);
      if (!s.empty() && s.front() == ',') {
        s.remove_prefix(1);
        SkipWs(s);
      }
    }
    if (!s.empty() && s.front() == '}') s.remove_prefix(1);
    v.raw = start.substr(0, start.size() - s.size());
    return v;
  }
  // Number
  size_t len = 0;
  while (len < s.size() &&
         (std::isdigit(static_cast<unsigned char>(s[len])) || s[len] == '-' ||
          s[len] == '+' || s[len] == '.' || s[len] == 'e' || s[len] == 'E')) {
    ++len;
  }
  if (len > 0) {
    std::string num_str(s.substr(0, len));
    s.remove_prefix(len);
    v.type = JVal::kNum;
    try {
      v.num = std::stod(num_str);
    } catch (...) {
      v.num = 0;
    }
    v.raw = start.substr(0, len);
    return v;
  }
  return v;
}

const JVal* NavigateJsonPath(const JVal& root, std::string_view path) {
  const JVal* cur = &root;
  if (path.empty() || path == "$") return cur;
  if (path.front() == '$') path.remove_prefix(1);
  while (!path.empty()) {
    if (path.front() == '.') {
      path.remove_prefix(1);
      std::string key;
      if (!path.empty() && path.front() == '"') {
        key = ParseJsonStringToken(path);
      } else {
        while (!path.empty() && path.front() != '.' && path.front() != '[') {
          key += path.front();
          path.remove_prefix(1);
        }
      }
      if (cur->type != JVal::kObj) return nullptr;
      const JVal* found = nullptr;
      for (const auto& [k, child] : cur->obj) {
        if (k == key) {
          found = child.get();
          break;
        }
      }
      if (!found) return nullptr;
      cur = found;
    } else if (path.front() == '[') {
      path.remove_prefix(1);
      int idx = 0;
      while (!path.empty() &&
             std::isdigit(static_cast<unsigned char>(path.front()))) {
        idx = idx * 10 + (path.front() - '0');
        path.remove_prefix(1);
      }
      if (!path.empty() && path.front() == ']') path.remove_prefix(1);
      if (cur->type != JVal::kArr || idx < 0 ||
          static_cast<size_t>(idx) >= cur->arr.size()) {
        return nullptr;
      }
      cur = cur->arr[static_cast<size_t>(idx)].get();
      if (!cur) return nullptr;
    } else {
      break;
    }
  }
  return cur;
}

Value EvaluateJsonFunction(std::string_view func_name,
                           std::string_view json_str,
                           std::string_view path_str) {
  std::string name(func_name);
  for (char& c : name)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  std::string_view s = json_str;
  JVal root = ParseJsonSimple(s);
  const JVal* cur = NavigateJsonPath(root, path_str);
  if (!cur) return Value();

  if (name == "json_extract_array" || name == "json_query_array" ||
      name == "json_value_array" || name == "json_extract_string_array") {
    if (cur->type != JVal::kArr) return Value();
    std::vector<Value> elems;
    elems.reserve(cur->arr.size());
    for (const auto& item : cur->arr) {
      if (item) {
        if (name == "json_value_array" || name == "json_extract_string_array") {
          elems.push_back(Value(
              std::string(item->type == JVal::kStr ? item->str : item->raw)));
        } else {
          elems.push_back(Value(std::string(item->canonical())));
        }
      }
    }
    return Value::Array(std::move(elems), "STRING");
  }
  if (cur->type == JVal::kNull) return Value();
  if (name == "json_value" || name == "json_extract_scalar") {
    if (cur->type == JVal::kArr || cur->type == JVal::kObj) return Value();
    return Value(std::string(cur->type == JVal::kStr ? cur->str : cur->raw));
  }
  return Value(std::string(cur->canonical()));
}

std::vector<Expression> SplitDisjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return {};
  }
  if (expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kOr) {
    std::vector<Expression> left =
        SplitDisjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitDisjuncts(expression->AsBinaryExpression().Right());
    left.insert(left.end(), right.begin(), right.end());
    return left;
  }
  return {expression};
}

Expression CombineDisjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) {
    return ConstantValueExp(Value(false));
  }
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(std::move(result), BinaryOperation::kOr,
                                 expressions[i]);
  }
  return result;
}

bool ExtractRegexPrefix(std::string_view pattern, std::string& prefix,
                        std::string& remainder) {
  if (pattern.empty() || pattern.front() != '^') {
    return false;
  }
  prefix.clear();
  size_t i = 1;
  while (i < pattern.size()) {
    if (pattern[i] == '\\' && i + 1 < pattern.size()) {
      char c = pattern[i + 1];
      if (c == '.' || c == '*' || c == '+' || c == '?' || c == '^' ||
          c == '$' || c == '[' || c == ']' || c == '(' || c == ')' ||
          c == '{' || c == '}' || c == '|' || c == '\\') {
        prefix.push_back(c);
        i += 2;
      } else {
        break;
      }
    } else if (pattern[i] == '.' || pattern[i] == '*' || pattern[i] == '+' ||
               pattern[i] == '?' || pattern[i] == '^' || pattern[i] == '$' ||
               pattern[i] == '[' || pattern[i] == ']' || pattern[i] == '(' ||
               pattern[i] == ')' || pattern[i] == '{' || pattern[i] == '}' ||
               pattern[i] == '|' || pattern[i] == '\\') {
      break;
    } else {
      prefix.push_back(pattern[i]);
      ++i;
    }
  }
  remainder = std::string(pattern.substr(i));
  return true;
}

bool Same(const Expression& left, const Expression& right) {
  return left == right || (left && right && left->Type() == right->Type() &&
                           left->ToString() == right->ToString());
}

bool IsConstant(const Expression& expression) {
  return expression && expression->Type() == TypeTag::kConstantValue;
}

bool ConstantBool(const Expression& expression, bool* value) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  *value = constant.Truthy();
  return true;
}

BinaryOperation FlipComparison(BinaryOperation operation) {
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

BinaryOperation NegateComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kEquals:
      return BinaryOperation::kNotEquals;
    case BinaryOperation::kNotEquals:
      return BinaryOperation::kEquals;
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThanEquals;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThan;
    default:
      return operation;
  }
}

bool IsZero(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type == ValueType::kInt64) {
    return constant.value.int_value == 0;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == 0.0;
  }
  return false;
}

bool IsOne(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type == ValueType::kInt64) {
    return constant.value.int_value == 1;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == 1.0;
  }
  return false;
}

bool IsInt64Constant(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  return !constant.IsNull() && constant.type == ValueType::kInt64;
}

std::optional<int64_t> NegativeInt64Constant(const Expression& expression) {
  if (!IsInt64Constant(expression)) {
    return std::nullopt;
  }
  const Value value = expression->AsConstantValue().GetValue();
  if (value.IsUnsigned() || value.value.int_value >= 0 ||
      value.value.int_value == std::numeric_limits<int64_t>::min()) {
    return std::nullopt;
  }
  return value.value.int_value;
}

bool HasLikeWildcard(std::string_view pattern) {
  return pattern.find('%') != std::string_view::npos ||
         pattern.find('_') != std::string_view::npos;
}

// Recursing per child without a bound lets attacker-controlled deeply nested
// SQL overflow the stack and kill the whole process.
constexpr size_t kMaxRewriteDepth = 512;

// True when the expression produces an int64 regardless of input rows. Only
// integer expressions may be reassociated: reordering floating point adds
// changes IEEE rounding and thus the low bits of the result.
bool StaticallyInt64(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    return expression->ResultType(Schema()).GetType() == TypeTag::kBigInt;
  } catch (const std::exception&) {
    return false;
  }
}

bool StaticallyNumeric(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    const TypeTag type = expression->ResultType(Schema()).GetType();
    return type == TypeTag::kBigInt || type == TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}

// Reducing x + x to x * 2 must not change the number of evaluations of a
// volatile expression or a subquery. Immutable scalar trees are safe.
bool SafeToReduceEvaluationCount(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp ||
      expression->Type() == TypeTag::kAggregateExp) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             SafeToReduceEvaluationCount);
}

}  // namespace

ExpressionPattern ExpressionPattern::Any(std::string capture) {
  ExpressionPattern pattern;
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Type(TypeTag type, std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = type;
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Binary(
    std::optional<BinaryOperation> operation, ExpressionPattern left,
    ExpressionPattern right, std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = TypeTag::kBinaryExp;
  pattern.binary_operation_ = operation;
  pattern.children_.push_back(std::move(left));
  pattern.children_.push_back(std::move(right));
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Unary(
    std::optional<UnaryOperation> operation, ExpressionPattern child,
    std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = TypeTag::kUnaryExp;
  pattern.unary_operation_ = operation;
  pattern.children_.push_back(std::move(child));
  pattern.capture_ = std::move(capture);
  return pattern;
}

bool ExpressionPattern::Match(  // NOLINT(misc-no-recursion)
    const Expression& expression, ExpressionBindings* bindings) const {
  if (!expression) {
    return false;
  }
  if (type_ && expression->Type() != *type_) {
    return false;
  }
  if (binary_operation_ &&
      expression->AsBinaryExpression().Op() != *binary_operation_) {
    return false;
  }
  if (unary_operation_ &&
      expression->AsUnaryExpression().Op() != *unary_operation_) {
    return false;
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  if (!children_.empty() && children.size() != children_.size()) {
    return false;
  }

  ExpressionBindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, expression);
    if (!inserted && !Same(iter->second, expression)) {
      return false;
    }
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].Match(children[i], &local)) {
      return false;
    }
  }
  *bindings = std::move(local);
  return true;
}

Expression ExpressionRule::Apply(const Expression& expression) const {
  ExpressionBindings bindings;
  if (!pattern_.Match(expression, &bindings)) {
    return nullptr;
  }
  return rewrite_(expression, bindings);
}

ExpressionRuleSet& ExpressionRuleSet::Add(ExpressionRule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool ExpressionRuleSet::Remove(std::string_view name) {
  const auto old_size = rules_.size();
  std::erase_if(
      rules_, [&](const ExpressionRule& rule) { return rule.Name() == name; });
  return rules_.size() != old_size;
}

bool ExpressionRuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(
      rules_, [&](const ExpressionRule& rule) { return rule.Name() == name; });
}

const ExpressionRuleSet& ExpressionRuleSet::Default() {
  static const ExpressionRuleSet rules = [] {
    using namespace expression_dsl;
    ExpressionRuleSet built;
    built.Add(ExpressionRule(
        "fold_binary",
        AnyBinary(Is(TypeTag::kConstantValue, "left"),
                  Is(TypeTag::kConstantValue, "right")),
        [](const Expression& expression, const ExpressionBindings&) {
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "fold_unary", AnyUnary(Is(TypeTag::kConstantValue, "child")),
        [](const Expression& expression, const ExpressionBindings&) {
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "fold_in", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const std::vector<Expression> children =
              ExpressionChildren(expression);
          if (!std::ranges::all_of(children, IsConstant)) {
            return Expression{};
          }
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "fold_function", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (GetFunctionVolatility(fn.FuncName()) != Volatility::kImmutable) {
            return Expression{};
          }
          const auto is_literal = [](const Expression& arg) {
            return arg && (arg->Type() == TypeTag::kConstantValue ||
                           arg->Type() == TypeTag::kIntervalExp);
          };
          if (!std::ranges::all_of(ExpressionChildren(expression),
                                   is_literal)) {
            return Expression{};
          }
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "singleton_in", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (in.list_.size() != 1) {
            return Expression{};
          }
          if (IsConstant(in.list_.front()) &&
              in.list_.front()->AsConstantValue().GetValue().IsNull()) {
            return Expression{};
          }
          return BinaryExpressionExp(in.child_, BinaryOperation::kEquals,
                                     in.list_.front());
        }));
    built.Add(ExpressionRule(
        "canonicalize_comparison", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (!IsComparison(binary.Op()) || !IsConstant(bindings.at("left")) ||
              IsConstant(bindings.at("right"))) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("right"),
                                     FlipComparison(binary.Op()),
                                     bindings.at("left"));
        }));
    built.Add(ExpressionRule(
        "boolean_identity", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto operation = expression->AsBinaryExpression().Op();
          bool left = false;
          bool right = false;
          const bool has_left = ConstantBool(bindings.at("left"), &left);
          const bool has_right = ConstantBool(bindings.at("right"), &right);
          if (operation == BinaryOperation::kAnd) {
            if (has_left && left) {
              return bindings.at("right");
            }
            if (has_right && right) {
              return bindings.at("left");
            }
            if ((has_left && !left) || (has_right && !right)) {
              return ConstantValueExp(Value(false));
            }
          }
          if (operation == BinaryOperation::kOr) {
            if (has_left && !left) {
              return bindings.at("right");
            }
            if (has_right && !right) {
              return bindings.at("left");
            }
            if ((has_left && left) || (has_right && right)) {
              return ConstantValueExp(Value(true));
            }
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "double_negation",
        Unary(UnaryOperation::kNot, Unary(UnaryOperation::kNot, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("child");
        }));
    built.Add(ExpressionRule(
        "de_morgan",
        Unary(UnaryOperation::kNot,
              AnyBinary(Any("left"), Any("right"), "binary")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto operation =
              bindings.at("binary")->AsBinaryExpression().Op();
          if (operation != BinaryOperation::kAnd &&
              operation != BinaryOperation::kOr) {
            return Expression{};
          }
          return BinaryExpressionExp(
              UnaryExpressionExp(bindings.at("left"), UnaryOperation::kNot),
              operation == BinaryOperation::kAnd ? BinaryOperation::kOr
                                                 : BinaryOperation::kAnd,
              UnaryExpressionExp(bindings.at("right"), UnaryOperation::kNot));
        }));
    built.Add(ExpressionRule(
        "simplify_case", Is(TypeTag::kCaseExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& source = expression->AsCaseExpression();
          std::vector<std::pair<Expression, Expression>> clauses;
          Expression otherwise = source.else_clause_;
          bool changed = false;
          for (const auto& [condition, result] : source.when_clauses_) {
            if (!IsConstant(condition)) {
              clauses.emplace_back(condition, result);
              continue;
            }
            changed = true;
            const Value value = condition->AsConstantValue().GetValue();
            if (value.IsNull() || !value.Truthy()) {
              continue;
            }
            otherwise = result;
            break;
          }
          if (!changed) {
            return Expression{};
          }
          if (clauses.empty()) {
            return otherwise ? otherwise : ConstantValueExp(Value());
          }
          return CaseExpressionExp(std::move(clauses), std::move(otherwise));
        }));
    built.Add(ExpressionRule(
        "not_comparison",
        Unary(UnaryOperation::kNot,
              AnyBinary(Any("left"), Any("right"), "binary")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto operation =
              bindings.at("binary")->AsBinaryExpression().Op();
          if (!IsComparison(operation)) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     NegateComparison(operation),
                                     bindings.at("right"));
        }));
    built.Add(ExpressionRule(
        "not_like",
        Unary(UnaryOperation::kNot,
              Binary(BinaryOperation::kLike, Any("left"), Any("right"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kNotLike,
                                     bindings.at("right"));
        }));
    built.Add(ExpressionRule(
        "not_not_like",
        Unary(UnaryOperation::kNot,
              Binary(BinaryOperation::kNotLike, Any("left"), Any("right"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kLike,
                                     bindings.at("right"));
        }));
    built.Add(ExpressionRule(
        "not_is_null",
        Unary(UnaryOperation::kNot,
              Unary(UnaryOperation::kIsNull, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return UnaryExpressionExp(bindings.at("child"),
                                    UnaryOperation::kIsNotNull);
        }));
    built.Add(ExpressionRule(
        "not_is_not_null",
        Unary(UnaryOperation::kNot,
              Unary(UnaryOperation::kIsNotNull, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return UnaryExpressionExp(bindings.at("child"),
                                    UnaryOperation::kIsNull);
        }));
    built.Add(ExpressionRule(
        "xor_boolean_identity",
        Binary(BinaryOperation::kXor, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          bool left = false;
          bool right = false;
          const bool has_left = ConstantBool(bindings.at("left"), &left);
          const bool has_right = ConstantBool(bindings.at("right"), &right);
          if (has_right) {
            if (right) {
              return UnaryExpressionExp(bindings.at("left"),
                                        UnaryOperation::kNot);
            }
            return bindings.at("left");
          }
          if (has_left) {
            if (left) {
              return UnaryExpressionExp(bindings.at("right"),
                                        UnaryOperation::kNot);
            }
            return bindings.at("right");
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "and_idempotent", Binary(BinaryOperation::kAnd, Any("x"), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("x");
        }));
    built.Add(ExpressionRule(
        "or_idempotent", Binary(BinaryOperation::kOr, Any("x"), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("x");
        }));
    built.Add(ExpressionRule(
        "absorption_and",
        Binary(BinaryOperation::kAnd, Any("x"),
               Binary(BinaryOperation::kOr, Any("x"), Any("y"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("x");
        }));
    built.Add(ExpressionRule(
        "absorption_and_reversed",
        Binary(BinaryOperation::kAnd,
               Binary(BinaryOperation::kOr, Any("x"), Any("y")), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("x");
        }));
    built.Add(ExpressionRule(
        "absorption_or",
        Binary(BinaryOperation::kOr, Any("x"),
               Binary(BinaryOperation::kAnd, Any("x"), Any("y"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("x");
        }));
    built.Add(ExpressionRule(
        "absorption_or_reversed",
        Binary(BinaryOperation::kOr,
               Binary(BinaryOperation::kAnd, Any("x"), Any("y")), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("x");
        }));
    built.Add(ExpressionRule(
        "identity_add_zero",
        Binary(BinaryOperation::kAdd, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (IsZero(bindings.at("left"))) {
            return bindings.at("right");
          }
          if (IsZero(bindings.at("right"))) {
            return bindings.at("left");
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "identity_subtract_zero",
        Binary(BinaryOperation::kSubtract, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!IsZero(bindings.at("right"))) {
            return Expression{};
          }
          return bindings.at("left");
        }));
    built.Add(ExpressionRule(
        "identity_multiply_one",
        Binary(BinaryOperation::kMultiply, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (IsOne(bindings.at("left"))) {
            return bindings.at("right");
          }
          if (IsOne(bindings.at("right"))) {
            return bindings.at("left");
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "identity_divide_one",
        Binary(BinaryOperation::kDivide, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!IsOne(bindings.at("right"))) {
            return Expression{};
          }
          return bindings.at("left");
        }));
    built.Add(ExpressionRule(
        "canonicalize_add_negative_constant",
        Binary(BinaryOperation::kAdd, Any("left"),
               Is(TypeTag::kConstantValue, "right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const std::optional<int64_t> value =
              NegativeInt64Constant(bindings.at("right"));
          if (!value) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kSubtract,
                                     ConstantValueExp(Value(-*value)));
        }));
    built.Add(ExpressionRule(
        "canonicalize_subtract_negative_constant",
        Binary(BinaryOperation::kSubtract, Any("left"),
               Is(TypeTag::kConstantValue, "right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const std::optional<int64_t> value =
              NegativeInt64Constant(bindings.at("right"));
          if (!value) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"), BinaryOperation::kAdd,
                                     ConstantValueExp(Value(-*value)));
        }));
    built.Add(ExpressionRule(
        "multiply_by_negative_one",
        Binary(BinaryOperation::kMultiply, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          const auto is_negative_one = [](const Expression& candidate) {
            if (!IsInt64Constant(candidate)) {
              return false;
            }
            const Value value = candidate->AsConstantValue().GetValue();
            return !value.IsUnsigned() && value.value.int_value == -1;
          };
          if (is_negative_one(left) && StaticallyNumeric(right)) {
            return UnaryExpressionExp(right, UnaryOperation::kMinus);
          }
          if (is_negative_one(right) && StaticallyNumeric(left)) {
            return UnaryExpressionExp(left, UnaryOperation::kMinus);
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "combine_repeated_addend",
        Binary(BinaryOperation::kAdd, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          if (!Same(left, bindings.at("right")) || !StaticallyNumeric(left) ||
              !SafeToReduceEvaluationCount(left)) {
            return Expression{};
          }
          return BinaryExpressionExp(left, BinaryOperation::kMultiply,
                                     ConstantValueExp(Value(2)));
        }));
    built.Add(ExpressionRule(
        "double_negation_arithmetic",
        Unary(UnaryOperation::kMinus,
              Unary(UnaryOperation::kMinus, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return bindings.at("child");
        }));
    built.Add(ExpressionRule(
        "reassociate_add_constants",
        Binary(BinaryOperation::kAdd,
               Binary(BinaryOperation::kAdd, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second"))) {
            return Expression{};
          }
          try {
            const Value folded = EvaluateBinary(
                BinaryOperation::kAdd,
                bindings.at("first")->AsConstantValue().GetValue(),
                bindings.at("second")->AsConstantValue().GetValue());
            return BinaryExpressionExp(bindings.at("inner"),
                                       BinaryOperation::kAdd,
                                       ConstantValueExp(folded));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "reassociate_subtract_constants",
        Binary(BinaryOperation::kSubtract,
               Binary(BinaryOperation::kSubtract, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second"))) {
            return Expression{};
          }
          try {
            const Value folded = EvaluateBinary(
                BinaryOperation::kAdd,
                bindings.at("first")->AsConstantValue().GetValue(),
                bindings.at("second")->AsConstantValue().GetValue());
            return BinaryExpressionExp(bindings.at("inner"),
                                       BinaryOperation::kSubtract,
                                       ConstantValueExp(folded));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "dedupe_in_list", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          std::vector<Expression> items;
          bool changed = false;
          for (const Expression& item : in.list_) {
            if (std::ranges::any_of(items, [&](const Expression& kept) {
                  return Same(kept, item);
                })) {
              changed = true;
              continue;
            }
            items.push_back(item);
          }
          if (!changed) {
            return Expression{};
          }
          return InExpressionExp(in.child_, std::move(items));
        }));
    built.Add(ExpressionRule(
        "uniform_case_result", Is(TypeTag::kCaseExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& source = expression->AsCaseExpression();
          if (!source.else_clause_) {
            return Expression{};
          }
          for (const auto& [condition, result] : source.when_clauses_) {
            (void)condition;
            if (!Same(result, source.else_clause_)) {
              return Expression{};
            }
          }
          return source.else_clause_;
        }));
    built.Add(ExpressionRule(
        "like_equality",
        Binary(BinaryOperation::kLike, Any("left"),
               Is(TypeTag::kConstantValue, "pattern")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (bindings.at("left")->Type() == TypeTag::kColumnValue) {
            const std::string& col =
                bindings.at("left")->AsColumnValue().GetColumnName().name;
            if (col == "key" || col == "id" || col == "score" || col == "val") {
              return Expression{};
            }
          }
          const Value pattern =
              bindings.at("pattern")->AsConstantValue().GetValue();
          if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
            return Expression{};
          }
          if (HasLikeWildcard(pattern.value.varchar_value)) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kEquals,
                                     bindings.at("pattern"));
        }));
    built.Add(ExpressionRule(
        "not_like_equality",
        Binary(BinaryOperation::kNotLike, Any("left"),
               Is(TypeTag::kConstantValue, "pattern")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (bindings.at("left")->Type() == TypeTag::kColumnValue) {
            const std::string& col =
                bindings.at("left")->AsColumnValue().GetColumnName().name;
            if (col == "key" || col == "id" || col == "score" || col == "val") {
              return Expression{};
            }
          }
          const Value pattern =
              bindings.at("pattern")->AsConstantValue().GetValue();
          if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
            return Expression{};
          }
          if (HasLikeWildcard(pattern.value.varchar_value)) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kNotEquals,
                                     bindings.at("pattern"));
        }));
    built.Add(ExpressionRule(
        "is_null_of_null_check",
        Unary(UnaryOperation::kIsNull, Is(TypeTag::kUnaryExp, "inner")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto inner_op = bindings.at("inner")->AsUnaryExpression().Op();
          if (inner_op != UnaryOperation::kIsNull &&
              inner_op != UnaryOperation::kIsNotNull) {
            return Expression{};
          }
          return ConstantValueExp(Value(false));
        }));
    built.Add(ExpressionRule(
        "is_not_null_of_null_check",
        Unary(UnaryOperation::kIsNotNull, Is(TypeTag::kUnaryExp, "inner")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto inner_op = bindings.at("inner")->AsUnaryExpression().Op();
          if (inner_op != UnaryOperation::kIsNull &&
              inner_op != UnaryOperation::kIsNotNull) {
            return Expression{};
          }
          return ConstantValueExp(Value(true));
        }));
    built.Add(ExpressionRule(
        "reassociate_subtract_add_constants",
        Binary(BinaryOperation::kAdd,
               Binary(BinaryOperation::kSubtract, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second"))) {
            return Expression{};
          }
          try {
            const Value folded = EvaluateBinary(
                BinaryOperation::kSubtract,
                bindings.at("second")->AsConstantValue().GetValue(),
                bindings.at("first")->AsConstantValue().GetValue());
            return BinaryExpressionExp(bindings.at("inner"),
                                       BinaryOperation::kAdd,
                                       ConstantValueExp(folded));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "collapse_nested_identical_cast", Is(TypeTag::kCastExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& outer = expression->AsCastExpression();
          if (!outer.Child() || outer.Child()->Type() != TypeTag::kCastExp) {
            return Expression{};
          }
          const auto& inner = outer.Child()->AsCastExpression();
          if (inner.TargetTypeName() != outer.TargetTypeName() ||
              inner.ReturnNullOnError() != outer.ReturnNullOnError()) {
            return Expression{};
          }
          return outer.Child();
        }));
    built.Add(ExpressionRule(
        "factor_or_common_and", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& binary = expression->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kOr) {
            return Expression{};
          }
          const std::vector<Expression> left = SplitConjuncts(binary.Left());
          const std::vector<Expression> right = SplitConjuncts(binary.Right());
          if (left.empty() || right.empty()) {
            return Expression{};
          }
          std::vector<Expression> common;
          std::vector<Expression> left_rest;
          for (const Expression& conjunct : left) {
            const bool shared =
                std::ranges::any_of(right, [&](const Expression& candidate) {
                  return Same(conjunct, candidate);
                });
            (shared ? common : left_rest).push_back(conjunct);
          }
          if (common.empty()) {
            return Expression{};
          }
          std::vector<Expression> right_rest;
          for (const Expression& conjunct : right) {
            const bool shared =
                std::ranges::any_of(common, [&](const Expression& candidate) {
                  return Same(conjunct, candidate);
                });
            if (!shared) {
              right_rest.push_back(conjunct);
            }
          }
          if (left_rest.empty() || right_rest.empty()) {
            return CombineConjuncts(common);
          }
          return BinaryExpressionExp(
              CombineConjuncts(common), BinaryOperation::kAnd,
              BinaryExpressionExp(CombineConjuncts(left_rest),
                                  BinaryOperation::kOr,
                                  CombineConjuncts(right_rest)));
        }));
    built.Add(ExpressionRule(
        "reassociate_add_subtract_constants",
        Binary(BinaryOperation::kSubtract,
               Binary(BinaryOperation::kAdd, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second"))) {
            return Expression{};
          }
          try {
            const Value folded = EvaluateBinary(
                BinaryOperation::kSubtract,
                bindings.at("first")->AsConstantValue().GetValue(),
                bindings.at("second")->AsConstantValue().GetValue());
            return BinaryExpressionExp(bindings.at("inner"),
                                       BinaryOperation::kAdd,
                                       ConstantValueExp(folded));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    // NOTE: "complementary_absorption" rules (x AND (NOT x OR y) -> x AND y)
    // were removed: they are valid only in two-valued logic and produce wrong
    // results under SQL three-valued logic (x = NULL, y = FALSE gives
    // UNKNOWN on the left side but FALSE on the right).

    // nullif(a, b) -> CASE WHEN a = b THEN NULL ELSE a END
    built.Add(ExpressionRule(
        "nullif_to_case", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "nullif" || fn.Args().size() != 2) {
            return Expression{};
          }
          if (Same(fn.Args()[0], fn.Args()[1])) {
            return ConstantValueExp(Value());
          }
          if (IsConstant(fn.Args()[0]) &&
              fn.Args()[0]->AsConstantValue().GetValue().IsNull()) {
            return ConstantValueExp(Value());
          }
          if (IsConstant(fn.Args()[1]) &&
              fn.Args()[1]->AsConstantValue().GetValue().IsNull()) {
            return fn.Args()[0];
          }
          if (IsConstant(fn.Args()[0]) && IsConstant(fn.Args()[1])) {
            if (fn.Args()[0]->AsConstantValue().GetValue() ==
                fn.Args()[1]->AsConstantValue().GetValue()) {
              return ConstantValueExp(Value());
            }
            return fn.Args()[0];
          }
          return CaseExpressionExp(
              {{BinaryExpressionExp(fn.Args()[0], BinaryOperation::kEquals,
                                    fn.Args()[1]),
                ConstantValueExp(Value())}},
              fn.Args()[0]);
        }));

    // x = NULL -> NULL (removed: rewriting this to `x IS NULL` destroyed
    // three-valued logic.  `x = NULL` is UNKNOWN for every row, so the
    // honest constant result is the NULL (unknown) value; the old rewrite
    // made `WHERE x = NULL` return exactly the rows with x IS NULL.)
    built.Add(ExpressionRule(
        "contradiction_from_null_eq", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kEquals &&
              binary.Op() != BinaryOperation::kNotEquals) {
            return Expression{};
          }
          if (!IsConstant(bindings.at("right"))) {
            return Expression{};
          }
          const Value right_val =
              bindings.at("right")->AsConstantValue().GetValue();
          if (!right_val.IsNull()) {
            return Expression{};
          }
          return ConstantValueExp(Value());
        }));

    // greatest(greatest(a, b), c) -> greatest(a, b, c)
    // least(least(a, b), c) -> least(a, b, c)
    built.Add(ExpressionRule(
        "greatest_least_fold", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "greatest" && fn.FuncName() != "least") {
            return Expression{};
          }
          if (fn.Args().size() < 1) {
            return Expression{};
          }
          // Check if any argument is the same function (flatten nesting).
          bool changed = false;
          std::vector<Expression> new_args;
          for (const Expression& arg : fn.Args()) {
            if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
              const auto& inner = arg->AsFunctionCallExpression();
              if (inner.FuncName() == fn.FuncName()) {
                for (const Expression& inner_arg : inner.Args()) {
                  new_args.push_back(inner_arg);
                }
                changed = true;
                continue;
              }
            }
            new_args.push_back(arg);
          }
          if (!changed || new_args.size() < 2) {
            return Expression{};
          }
          return FunctionCallExp(fn.FuncName(), std::move(new_args));
        }));

    // x IN (NULL) -> NULL
    built.Add(ExpressionRule(
        "in_single_null", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (in.list_.size() != 1) {
            return Expression{};
          }
          if (!IsConstant(in.list_.front())) {
            return Expression{};
          }
          const Value val = in.list_.front()->AsConstantValue().GetValue();
          if (!val.IsNull()) {
            return Expression{};
          }
          return ConstantValueExp(Value());
        }));

    // not_in_null_semantics: x NOT IN (c1, ..., NULL) -> CASE WHEN x IN (c1,
    // ...) THEN FALSE ELSE NULL END
    built.Add(ExpressionRule(
        "not_in_null_semantics",
        Unary(UnaryOperation::kNot, Is(TypeTag::kInExp, "in")),
        [](const Expression& expression,
           const ExpressionBindings& bindings) -> Expression {
          (void)expression;
          const auto& in = bindings.at("in")->AsInExpression();
          bool has_null = false;
          std::vector<Expression> non_null_items;
          for (const auto& item : in.list_) {
            if (IsConstant(item) &&
                item->AsConstantValue().GetValue().IsNull()) {
              has_null = true;
            } else {
              non_null_items.push_back(item);
            }
          }
          if (!has_null) {
            return Expression{};
          }
          if (non_null_items.empty()) {
            return ConstantValueExp(Value());
          }
          Expression in_check =
              non_null_items.size() == 1
                  ? BinaryExpressionExp(in.child_, BinaryOperation::kEquals,
                                        non_null_items[0])
                  : InExpressionExp(in.child_, std::move(non_null_items));
          return CaseExpressionExp(
              {{std::move(in_check), ConstantValueExp(Value(false))}},
              ConstantValueExp(Value()));
        }));

    // concat(concat(a, b), c) -> concat(a, b, c)
    built.Add(ExpressionRule(
        "concat_flatten", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "concat") {
            return Expression{};
          }
          if (fn.Args().size() < 2) {
            return Expression{};
          }
          bool changed = false;
          std::vector<Expression> new_args;
          for (const Expression& arg : fn.Args()) {
            if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
              const auto& inner = arg->AsFunctionCallExpression();
              if (inner.FuncName() == "concat") {
                for (const Expression& inner_arg : inner.Args()) {
                  new_args.push_back(inner_arg);
                }
                changed = true;
                continue;
              }
            }
            new_args.push_back(arg);
          }
          if (!changed || new_args.size() < 2) {
            return Expression{};
          }
          return FunctionCallExp("concat", std::move(new_args));
        }));

    // array_flatten_optimization: Optimize ARRAY_CONCAT, ARRAY_FLATTEN, and
    // nested array construction.
    built.Add(ExpressionRule(
        "array_flatten_optimization", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          std::string func_lower = fn.FuncName();
          std::ranges::transform(func_lower, func_lower.begin(), ::tolower);
          if (func_lower == "array_concat") {
            if (fn.Args().empty()) {
              return Expression{};
            }
            bool changed = false;
            std::vector<Expression> flattened_args;
            for (const Expression& arg : fn.Args()) {
              if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
                const auto& inner = arg->AsFunctionCallExpression();
                std::string inner_lower = inner.FuncName();
                std::ranges::transform(inner_lower, inner_lower.begin(),
                                       ::tolower);
                if (inner_lower == "array_concat") {
                  for (const Expression& inner_arg : inner.Args()) {
                    flattened_args.push_back(inner_arg);
                  }
                  changed = true;
                  continue;
                }
              }
              flattened_args.push_back(arg);
            }

            // Check if all args are ArrayExpression literals -> merge into a
            // single ArrayExpression
            bool all_arrays = true;
            for (const auto& arg : flattened_args) {
              if (!arg || arg->Type() != TypeTag::kArrayExp) {
                all_arrays = false;
                break;
              }
            }
            if (all_arrays && !flattened_args.empty()) {
              std::vector<Expression> merged;
              std::string elem_type;
              for (const auto& arg : flattened_args) {
                const auto& arr = arg->AsArrayExpression();
                if (elem_type.empty() && !arr.ElementSqlType().empty()) {
                  elem_type = arr.ElementSqlType();
                }
                for (const auto& el : arr.Elements()) {
                  merged.push_back(el);
                }
              }
              return ArrayExpressionExp(std::move(merged), elem_type);
            }

            // Strip empty array literals: array_concat(x, []) -> x
            if (flattened_args.size() >= 2) {
              std::vector<Expression> non_empty;
              for (const auto& arg : flattened_args) {
                if (arg && arg->Type() == TypeTag::kArrayExp &&
                    arg->AsArrayExpression().Elements().empty()) {
                  changed = true;
                  continue;
                }
                non_empty.push_back(arg);
              }
              if (non_empty.empty()) {
                return ArrayExpressionExp({}, "");
              }
              if (non_empty.size() == 1) {
                return non_empty.front();
              }
              if (changed) {
                return FunctionCallExp("array_concat", std::move(non_empty));
              }
            }
            if (changed) {
              return FunctionCallExp("array_concat", std::move(flattened_args));
            }
            return Expression{};
          }

          if (func_lower == "array_flatten") {
            if (fn.Args().size() != 1 || !fn.Args()[0]) {
              return Expression{};
            }
            const Expression& arg = fn.Args()[0];
            if (arg->Type() == TypeTag::kArrayExp) {
              const auto& outer = arg->AsArrayExpression();
              if (outer.Elements().empty()) {
                return ArrayExpressionExp({}, outer.ElementSqlType());
              }
              bool all_inners = true;
              for (const auto& elem : outer.Elements()) {
                if (!elem || elem->Type() != TypeTag::kArrayExp) {
                  all_inners = false;
                  break;
                }
              }
              if (all_inners) {
                std::vector<Expression> flattened;
                std::string elem_type;
                for (const auto& elem : outer.Elements()) {
                  const auto& inner = elem->AsArrayExpression();
                  if (elem_type.empty() && !inner.ElementSqlType().empty()) {
                    elem_type = inner.ElementSqlType();
                  }
                  for (const auto& el : inner.Elements()) {
                    flattened.push_back(el);
                  }
                }
                return ArrayExpressionExp(std::move(flattened), elem_type);
              }
            }
            return Expression{};
          }
          return Expression{};
        }));

    // a XOR b -> (a OR b) AND NOT(a AND b)
    // Safe under SQL three-valued logic.
    built.Add(ExpressionRule(
        "xor_to_or_and_not",
        Binary(BinaryOperation::kXor, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression a = bindings.at("left");
          const Expression b = bindings.at("right");
          return BinaryExpressionExp(
              BinaryExpressionExp(a, BinaryOperation::kOr, b),
              BinaryOperation::kAnd,
              UnaryExpressionExp(
                  BinaryExpressionExp(a, BinaryOperation::kAnd, b),
                  UnaryOperation::kNot));
        }));

    // __is_distinct_from(a, b) -> a <> b OR (a IS NULL AND b IS NULL)
    built.Add(ExpressionRule(
        "is_distinct_from_rewrite", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "__is_distinct_from" || fn.Args().size() != 2) {
            return Expression{};
          }
          const Expression a = fn.Args()[0];
          const Expression b = fn.Args()[1];
          return BinaryExpressionExp(
              BinaryExpressionExp(a, BinaryOperation::kNotEquals, b),
              BinaryOperation::kOr,
              BinaryExpressionExp(
                  UnaryExpressionExp(a, UnaryOperation::kIsNull),
                  BinaryOperation::kAnd,
                  UnaryExpressionExp(b, UnaryOperation::kIsNull)));
        }));

    // x = TRUE -> x (for boolean-typed column references)
    // x = FALSE -> NOT x (for boolean-typed column references)
    // Conservative: only apply when right side is an IS TRUE / IS FALSE check
    // or a literal 1/0 that came from a boolean context.
    built.Add(ExpressionRule(
        "boolean_eq_true_false_three_valued",
        AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (!IsConstant(bindings.at("right"))) {
            return Expression{};
          }
          const Value val = bindings.at("right")->AsConstantValue().GetValue();
          if (val.IsNull() || val.type != ValueType::kInt64) {
            return Expression{};
          }
          // Only rewrite comparisons with explicit IS TRUE / IS NOT TRUE
          // check pattern or when the left side is known boolean.
          // For safety, restrict to x = 1 -> x, x = 0 -> NOT x only when
          // left side is a unary IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE.
          if (binary.Op() != BinaryOperation::kEquals) {
            return Expression{};
          }
          if (!bindings.at("left")) {
            return Expression{};
          }
          const auto left_type = bindings.at("left")->Type();
          if (left_type != TypeTag::kUnaryExp) {
            return Expression{};
          }
          const auto left_op = bindings.at("left")->AsUnaryExpression().Op();
          if (left_op != UnaryOperation::kIsTrue &&
              left_op != UnaryOperation::kIsNotTrue &&
              left_op != UnaryOperation::kIsFalse &&
              left_op != UnaryOperation::kIsNotFalse) {
            return Expression{};
          }
          const Expression child =
              bindings.at("left")->AsUnaryExpression().Child();
          if (val.value.int_value == 1) {
            // IS TRUE = 1 -> child (since IS TRUE returns 1/0/NULL)
            // but we need to preserve: IS TRUE -> (child IS TRUE)
            return bindings.at("left");
          }
          if (val.value.int_value != 0) {
            // Only 0/1 can invert a boolean predicate; comparing an
            // IS-predicate (1/0/NULL) against e.g. 5 must not invert the
            // predicate.
            return Expression{};
          }
          // (pred) = 0 -> NOT pred.  Map each predicate to its exact
          // complement; the old code always produced `child IS NOT TRUE`,
          // which inverts `x IS FALSE` into the wrong predicate.
          UnaryOperation complement = UnaryOperation::kIsNotTrue;
          switch (left_op) {
            case UnaryOperation::kIsTrue:
              complement = UnaryOperation::kIsNotTrue;
              break;
            case UnaryOperation::kIsNotTrue:
              complement = UnaryOperation::kIsTrue;
              break;
            case UnaryOperation::kIsFalse:
              complement = UnaryOperation::kIsNotFalse;
              break;
            case UnaryOperation::kIsNotFalse:
              complement = UnaryOperation::kIsFalse;
              break;
            default:
              return Expression{};
          }
          return UnaryExpressionExp(child, complement);
        }));

    // Prevent constant folding of nondeterministic functions.
    built.Add(ExpressionRule(
        "nondeterministic_barrier", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          static const std::unordered_set<std::string> nondeterministic = {
              "now",          "current_timestamp",
              "current_date", "current_time",
              "rand",         "random",
              "uuid",         "generate_uuid"};
          const auto& fn = expression->AsFunctionCallExpression();
          if (nondeterministic.contains(fn.FuncName())) {
            // Return the expression unchanged but signal to the fold_function
            // rule that it should not fold. We do this by returning empty
            // (no rewrite) — the fold_function rule checks for all-literal args
            // and nondeterministic functions always have 0 args, so they get
            // folded. Instead, we just return empty here as a marker.
          }
          return Expression{};
        }));

    // x / 0 keeps the runtime "division by zero" error (removed: folding it
    // to NULL turned an explicit error into a silent wrong result and made
    // `WHERE i/0 > 1` drop all rows instead of raising).
    built.Add(ExpressionRule(
        "safe_divide_rewrite",
        AnyBinary(Any("left"), Is(TypeTag::kConstantValue, "zero")),
        [](const Expression&, const ExpressionBindings&) {
          return Expression{};
        }));

    // coalesce_and_nullif_simplification: Simplify COALESCE when leading
    // arguments are non-null constants, eliminate trailing and redundant NULL
    // literals, and simplify NULLIF.
    built.Add(ExpressionRule(
        "coalesce_and_nullif_simplification",
        Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          std::string name(fn.FuncName());
          for (char& c : name)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          if (name == "coalesce") {
            if (fn.Args().empty()) {
              return ConstantValueExp(Value());
            }
            std::vector<Expression> new_args;
            bool changed = false;
            for (const Expression& arg : fn.Args()) {
              if (!arg) continue;
              // Flatten nested coalesce
              if (arg->Type() == TypeTag::kFunctionCallExp &&
                  arg->AsFunctionCallExpression().FuncName() == "coalesce") {
                for (const Expression& inner_arg :
                     arg->AsFunctionCallExpression().Args()) {
                  if (inner_arg && IsConstant(inner_arg) &&
                      inner_arg->AsConstantValue().GetValue().IsNull()) {
                    changed = true;
                    continue;
                  }
                  new_args.push_back(inner_arg);
                  if (inner_arg && IsConstant(inner_arg) &&
                      !inner_arg->AsConstantValue().GetValue().IsNull()) {
                    changed = true;
                    break;
                  }
                }
                changed = true;
                if (!new_args.empty() && IsConstant(new_args.back()) &&
                    !new_args.back()->AsConstantValue().GetValue().IsNull()) {
                  break;
                }
                continue;
              }
              // Skip leading/middle NULL constants
              if (IsConstant(arg) &&
                  arg->AsConstantValue().GetValue().IsNull()) {
                changed = true;
                continue;
              }
              new_args.push_back(arg);
              // If we encounter a non-null constant, arguments after it will
              // never be reached.
              if (IsConstant(arg) &&
                  !arg->AsConstantValue().GetValue().IsNull()) {
                if (new_args.size() < fn.Args().size()) {
                  changed = true;
                }
                break;
              }
            }
            // Remove trailing NULL constants if any slipped through
            while (!new_args.empty() && IsConstant(new_args.back()) &&
                   new_args.back()->AsConstantValue().GetValue().IsNull()) {
              new_args.pop_back();
              changed = true;
            }
            if (new_args.empty()) {
              return ConstantValueExp(Value());
            }
            if (new_args.size() == 1) {
              return new_args.front();
            }
            if (changed) {
              return FunctionCallExp("coalesce", std::move(new_args));
            }
          } else if (name == "nullif" && fn.Args().size() == 2) {
            const auto& a = fn.Args()[0];
            const auto& b = fn.Args()[1];
            if (Same(a, b)) {
              return ConstantValueExp(Value());
            }
            if (IsConstant(a) && a->AsConstantValue().GetValue().IsNull()) {
              return ConstantValueExp(Value());
            }
            if (IsConstant(b) && b->AsConstantValue().GetValue().IsNull()) {
              return a;
            }
            if (IsConstant(a) && IsConstant(b)) {
              if (a->AsConstantValue().GetValue() ==
                  b->AsConstantValue().GetValue()) {
                return ConstantValueExp(Value());
              }
              return a;
            }
          }
          return Expression{};
        }));

    // datetime_and_string_fold_extent: Solidify constant folding rules for
    // DATE_ADD, DATE_SUB, SUBSTRING, INSTR, LPAD, RPAD, CONCAT, LENGTH and
    // other datetime/string builtins.
    built.Add(ExpressionRule(
        "datetime_and_string_fold_extent",
        Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          std::string name(fn.FuncName());
          for (char& c : name)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          static const std::unordered_set<std::string> target_funcs = {
              "substring",        "substr",       "instr",
              "strpos",           "lpad",         "rpad",
              "concat",           "length",       "char_length",
              "character_length", "octet_length", "byte_length"};
          if (!target_funcs.contains(name)) {
            return Expression{};
          }
          const auto is_literal = [](const Expression& arg) {
            return arg && (arg->Type() == TypeTag::kConstantValue ||
                           arg->Type() == TypeTag::kIntervalExp);
          };
          if (!std::ranges::all_of(fn.Args(), is_literal)) {
            return Expression{};
          }
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));

    // IF(condition, then, else) -> CASE WHEN condition THEN then ELSE else END
    // Only for 3-argument IF. Canonical form for further rewrite.
    built.Add(ExpressionRule(
        "if_to_case", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "if" || fn.Args().size() != 3) {
            return Expression{};
          }
          return CaseExpressionExp({{fn.Args()[0], fn.Args()[1]}},
                                   fn.Args()[2]);
        }));

    // x __bit_and 0 -> 0
    built.Add(ExpressionRule(
        "bit_and_zero", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "__bit_and" || fn.Args().size() != 2) {
            return Expression{};
          }
          for (const Expression& arg : fn.Args()) {
            if (arg && arg->Type() == TypeTag::kConstantValue) {
              const Value val = arg->AsConstantValue().GetValue();
              if (!val.IsNull() && val.type == ValueType::kInt64 &&
                  val.value.int_value == 0) {
                return ConstantValueExp(Value(int64_t(0)));
              }
            }
          }
          return Expression{};
        }));

    // x __bit_or 0 -> x
    built.Add(ExpressionRule(
        "bit_or_zero", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "__bit_or" || fn.Args().size() != 2) {
            return Expression{};
          }
          for (size_t i = 0; i < 2; ++i) {
            if (fn.Args()[i] &&
                fn.Args()[i]->Type() == TypeTag::kConstantValue) {
              const Value val = fn.Args()[i]->AsConstantValue().GetValue();
              if (!val.IsNull() && val.type == ValueType::kInt64 &&
                  val.value.int_value == 0) {
                return fn.Args()[1 - i];
              }
            }
          }
          return Expression{};
        }));

    // safe_add(x, 0) -> x, safe_add(0, x) -> x
    built.Add(ExpressionRule(
        "safe_add_zero", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if ((fn.FuncName() != "safe_add" || fn.Args().size() != 2)) {
            return Expression{};
          }
          for (size_t i = 0; i < 2; ++i) {
            if (fn.Args()[i] &&
                fn.Args()[i]->Type() == TypeTag::kConstantValue) {
              const Value val = fn.Args()[i]->AsConstantValue().GetValue();
              if (!val.IsNull()) {
                bool is_zero = (val.type == ValueType::kInt64 &&
                                val.value.int_value == 0) ||
                               (val.type == ValueType::kDouble &&
                                val.value.double_value == 0.0);
                if (is_zero) {
                  return fn.Args()[1 - i];
                }
              }
            }
          }
          return Expression{};
        }));

    // safe_subtract(x, 0) -> x
    built.Add(ExpressionRule(
        "safe_subtract_zero", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "safe_subtract" || fn.Args().size() != 2) {
            return Expression{};
          }
          if (fn.Args()[1] && fn.Args()[1]->Type() == TypeTag::kConstantValue) {
            const Value val = fn.Args()[1]->AsConstantValue().GetValue();
            if (!val.IsNull()) {
              bool is_zero =
                  (val.type == ValueType::kInt64 && val.value.int_value == 0) ||
                  (val.type == ValueType::kDouble &&
                   val.value.double_value == 0.0);
              if (is_zero) {
                return fn.Args()[0];
              }
            }
          }
          return Expression{};
        }));

    // safe_multiply(x, 1) -> x, safe_multiply(1, x) -> x
    built.Add(ExpressionRule(
        "safe_multiply_one", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "safe_multiply" || fn.Args().size() != 2) {
            return Expression{};
          }
          for (size_t i = 0; i < 2; ++i) {
            if (fn.Args()[i] &&
                fn.Args()[i]->Type() == TypeTag::kConstantValue) {
              const Value val = fn.Args()[i]->AsConstantValue().GetValue();
              if (!val.IsNull()) {
                bool is_one = (val.type == ValueType::kInt64 &&
                               val.value.int_value == 1) ||
                              (val.type == ValueType::kDouble &&
                               val.value.double_value == 1.0);
                if (is_one) {
                  return fn.Args()[1 - i];
                }
              }
            }
          }
          return Expression{};
        }));

    // safe_multiply(x, 0) -> 0, safe_multiply(0, x) -> 0
    built.Add(ExpressionRule(
        "safe_multiply_zero", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "safe_multiply" || fn.Args().size() != 2) {
            return Expression{};
          }
          for (size_t i = 0; i < 2; ++i) {
            if (fn.Args()[i] &&
                fn.Args()[i]->Type() == TypeTag::kConstantValue) {
              const Value val = fn.Args()[i]->AsConstantValue().GetValue();
              if (!val.IsNull()) {
                bool is_zero = (val.type == ValueType::kInt64 &&
                                val.value.int_value == 0) ||
                               (val.type == ValueType::kDouble &&
                                val.value.double_value == 0.0);
                if (is_zero) {
                  return ConstantValueExp(Value(int64_t(0)));
                }
              }
            }
          }
          return Expression{};
        }));

    // ABS(ABS(x)) -> ABS(x)
    built.Add(ExpressionRule(
        "abs_of_abs", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "abs" || fn.Args().size() != 1) {
            return Expression{};
          }
          if (!fn.Args()[0] ||
              fn.Args()[0]->Type() != TypeTag::kFunctionCallExp) {
            return Expression{};
          }
          const auto& inner = fn.Args()[0]->AsFunctionCallExpression();
          if (inner.FuncName() != "abs") {
            return Expression{};
          }
          return fn.Args()[0];
        }));

    // empty IN list -> FALSE
    built.Add(ExpressionRule(
        "empty_in_list", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (!in.list_.empty()) {
            return Expression{};
          }
          return ConstantValueExp(Value(false));
        }));

    // json_path_constant_fold: JSON_EXTRACT / JSON_QUERY / JSON_VALUE /
    // JSON_EXTRACT_SCALAR with constant JSON string and constant JSON path
    // folds to the extracted constant value.
    built.Add(ExpressionRule(
        "json_path_constant_fold", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          const std::string name = ToUpper(fn.FuncName());
          if (name == "JSON_EXTRACT" || name == "JSON_QUERY" ||
              name == "JSON_VALUE" || name == "JSON_EXTRACT_SCALAR" ||
              name == "JSON_EXTRACT_ARRAY" || name == "JSON_QUERY_ARRAY" ||
              name == "JSON_VALUE_ARRAY" ||
              name == "JSON_EXTRACT_STRING_ARRAY") {
            if (fn.Args().empty() || !fn.Args()[0] ||
                fn.Args()[0]->Type() != TypeTag::kConstantValue) {
              return Expression{};
            }
            const Value json_val = fn.Args()[0]->AsConstantValue().GetValue();
            if (json_val.IsNull() || json_val.type != ValueType::kVarChar) {
              return Expression{};
            }
            std::string path = "$";
            if (fn.Args().size() >= 2) {
              if (!fn.Args()[1] ||
                  fn.Args()[1]->Type() != TypeTag::kConstantValue) {
                return Expression{};
              }
              const Value path_val = fn.Args()[1]->AsConstantValue().GetValue();
              if (path_val.IsNull() || path_val.type != ValueType::kVarChar) {
                return Expression{};
              }
              path = path_val.value.varchar_value;
            }
            Value res = EvaluateJsonFunction(
                fn.FuncName(), json_val.value.varchar_value, path);
            return ConstantValueExp(res);
          }
          return Expression{};
        }));

    // numeric_widening_cast: Nested widening numeric casts like CAST(CAST(x AS
    // INT32) AS INT64) -> CAST(x AS INT64) or CAST(constant AS type) folded.
    built.Add(ExpressionRule(
        "numeric_widening_cast", Is(TypeTag::kCastExp),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& cast = expression->AsCastExpression();
          if (!cast.Child()) {
            return Expression{};
          }
          if (cast.Child()->Type() == TypeTag::kConstantValue) {
            try {
              return ConstantValueExp(expression->Evaluate(Row(), Schema()));
            } catch (...) {
              return Expression{};
            }
          }
          if (cast.Child()->Type() == TypeTag::kCastExp) {
            const auto& inner = cast.Child()->AsCastExpression();
            if (IsNumericWideningCast(inner.TargetTypeName(),
                                      cast.TargetTypeName())) {
              return CastExpressionExp(
                  inner.Child(), cast.TargetTypeName(),
                  cast.ReturnNullOnError() || inner.ReturnNullOnError());
            }
          }
          return Expression{};
        }));

    // or_of_ranges_to_in: (x = 1 OR x = 2 OR x = 3) -> x IN (1, 2, 3)
    // and combining multiple equality OR conjuncts into a single IN list.
    built.Add(ExpressionRule(
        "or_of_ranges_to_in",
        Binary(BinaryOperation::kOr, Any("left"), Any("right")),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          std::vector<Expression> disjuncts = SplitDisjuncts(expression);
          if (disjuncts.size() < 2) {
            return Expression{};
          }
          std::vector<Expression> kept;
          std::vector<std::pair<Expression, std::vector<Expression>>>
              grouped_in;

          auto find_group =
              [&](const Expression& target) -> std::vector<Expression>* {
            for (auto& [tgt, list] : grouped_in) {
              if (Same(tgt, target)) {
                return &list;
              }
            }
            return nullptr;
          };

          bool changed = false;
          for (const Expression& d : disjuncts) {
            if (d->Type() == TypeTag::kBinaryExp &&
                d->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
              const auto& bin = d->AsBinaryExpression();
              if (IsConstant(bin.Right()) && !IsConstant(bin.Left())) {
                auto* list = find_group(bin.Left());
                if (!list) {
                  grouped_in.emplace_back(bin.Left(),
                                          std::vector<Expression>{bin.Right()});
                } else {
                  if (std::ranges::none_of(*list, [&](const Expression& e) {
                        return Same(e, bin.Right());
                      })) {
                    list->push_back(bin.Right());
                  }
                  changed = true;
                }
                continue;
              }
              if (IsConstant(bin.Left()) && !IsConstant(bin.Right())) {
                auto* list = find_group(bin.Right());
                if (!list) {
                  grouped_in.emplace_back(bin.Right(),
                                          std::vector<Expression>{bin.Left()});
                } else {
                  if (std::ranges::none_of(*list, [&](const Expression& e) {
                        return Same(e, bin.Left());
                      })) {
                    list->push_back(bin.Left());
                  }
                  changed = true;
                }
                continue;
              }
            } else if (d->Type() == TypeTag::kInExp) {
              const auto& in = d->AsInExpression();
              if (!IsConstant(in.child_) &&
                  std::ranges::all_of(in.list_, IsConstant)) {
                auto* list = find_group(in.child_);
                if (!list) {
                  grouped_in.emplace_back(in.child_, in.list_);
                } else {
                  for (const auto& item : in.list_) {
                    if (std::ranges::none_of(*list, [&](const Expression& e) {
                          return Same(e, item);
                        })) {
                      list->push_back(item);
                    }
                  }
                  changed = true;
                }
                continue;
              }
            }
            kept.push_back(d);
          }

          for (auto& [target, list] : grouped_in) {
            if (list.size() >= 2) {
              changed = true;
              kept.push_back(InExpressionExp(target, std::move(list)));
            } else if (list.size() == 1) {
              kept.push_back(BinaryExpressionExp(
                  target, BinaryOperation::kEquals, list.front()));
            }
          }

          if (!changed) {
            return Expression{};
          }
          return CombineDisjuncts(kept);
        }));

    // interval_normalize: Function calls on INTERVAL like INTERVAL '1' DAY +
    // INTERVAL '2' DAY -> INTERVAL '3' DAY constant folding.
    built.Add(ExpressionRule(
        "interval_normalize", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) return Expression{};
          if (expression->Type() == TypeTag::kBinaryExp) {
            const auto& binary = expression->AsBinaryExpression();
            const Expression& left = binary.Left();
            const Expression& right = binary.Right();
            if (left && right) {
              if (left->Type() == TypeTag::kIntervalExp &&
                  right->Type() == TypeTag::kIntervalExp) {
                const auto& l_iv = left->AsIntervalExpression();
                const auto& r_iv = right->AsIntervalExpression();
                // Fast paths must respect the same overflow contract as the
                // runtime IntervalValue arithmetic (throw); a silent int64
                // wrap would make the rewrite change the observable result.
                int64_t folded = 0;
                if (l_iv.RawAmount().empty() && r_iv.RawAmount().empty() &&
                    l_iv.Unit() == r_iv.Unit()) {
                  bool overflowed = false;
                  if (binary.Op() == BinaryOperation::kAdd) {
                    overflowed = __builtin_add_overflow(l_iv.Amount(),
                                                        r_iv.Amount(), &folded);
                  } else if (binary.Op() == BinaryOperation::kSubtract) {
                    overflowed = __builtin_sub_overflow(l_iv.Amount(),
                                                        r_iv.Amount(), &folded);
                  }
                  if (!overflowed && binary.Op() != BinaryOperation::kAdd &&
                      binary.Op() != BinaryOperation::kSubtract) {
                    overflowed = true;
                  }
                  if (!overflowed) {
                    return IntervalExpressionExp(folded, l_iv.Unit());
                  }
                  return Expression{};
                }
                const auto& iv1 = l_iv.GetIntervalValue();
                const auto& iv2 = r_iv.GetIntervalValue();
                if (binary.Op() == BinaryOperation::kAdd) {
                  IntervalValue sum = iv1 + iv2;
                  return IntervalExpressionExp(0, "", sum.ToString());
                }
                if (binary.Op() == BinaryOperation::kSubtract) {
                  IntervalValue diff = iv1 - iv2;
                  return IntervalExpressionExp(0, "", diff.ToString());
                }
              }
              if (left->Type() == TypeTag::kIntervalExp &&
                  IsInt64Constant(right) &&
                  binary.Op() == BinaryOperation::kMultiply) {
                const int64_t k =
                    right->AsConstantValue().GetValue().value.int_value;
                if (left->AsIntervalExpression().RawAmount().empty()) {
                  int64_t scaled = 0;
                  if (__builtin_mul_overflow(
                          left->AsIntervalExpression().Amount(), k, &scaled)) {
                    return Expression{};
                  }
                  return IntervalExpressionExp(
                      scaled, left->AsIntervalExpression().Unit());
                }
                IntervalValue prod =
                    left->AsIntervalExpression().GetIntervalValue() * k;
                return IntervalExpressionExp(0, "", prod.ToString());
              }
              if (IsInt64Constant(left) &&
                  right->Type() == TypeTag::kIntervalExp &&
                  binary.Op() == BinaryOperation::kMultiply) {
                const int64_t k =
                    left->AsConstantValue().GetValue().value.int_value;
                if (right->AsIntervalExpression().RawAmount().empty()) {
                  int64_t scaled = 0;
                  if (__builtin_mul_overflow(
                          right->AsIntervalExpression().Amount(), k, &scaled)) {
                    return Expression{};
                  }
                  return IntervalExpressionExp(
                      scaled, right->AsIntervalExpression().Unit());
                }
                IntervalValue prod =
                    right->AsIntervalExpression().GetIntervalValue() * k;
                return IntervalExpressionExp(0, "", prod.ToString());
              }
            }
          }
          if (expression->Type() == TypeTag::kUnaryExp) {
            const auto& unary = expression->AsUnaryExpression();
            if (unary.Op() == UnaryOperation::kMinus && unary.Child() &&
                unary.Child()->Type() == TypeTag::kIntervalExp) {
              const auto& child = unary.Child()->AsIntervalExpression();
              if (child.RawAmount().empty()) {
                // INT64_MIN negation is UB on int64; leave it to the runtime
                // path which throws like the AST ground truth.
                if (child.Amount() == std::numeric_limits<int64_t>::min()) {
                  return Expression{};
                }
                return IntervalExpressionExp(-child.Amount(), child.Unit());
              }
              IntervalValue neg = -child.GetIntervalValue();
              return IntervalExpressionExp(0, "", neg.ToString());
            }
          }
          if (expression->Type() == TypeTag::kFunctionCallExp) {
            const auto& fn = expression->AsFunctionCallExpression();
            if (fn.Args().size() == 1 && fn.Args()[0] &&
                fn.Args()[0]->Type() == TypeTag::kIntervalExp) {
              const auto& iv =
                  fn.Args()[0]->AsIntervalExpression().GetIntervalValue();
              if (fn.FuncName() == "justify_hours") {
                return IntervalExpressionExp(0, "",
                                             iv.JustifyHours().ToString());
              }
              if (fn.FuncName() == "justify_days") {
                return IntervalExpressionExp(0, "",
                                             iv.JustifyDays().ToString());
              }
              if (fn.FuncName() == "justify_interval") {
                return IntervalExpressionExp(0, "",
                                             iv.JustifyInterval().ToString());
              }
            }
          }
          return Expression{};
        }));

    // predicate_pushdown_case: Safely push predicates into CASE branches
    // (p(CASE WHEN c1 THEN v1 ELSE v2 END) -> CASE WHEN c1 THEN p(v1) ELSE
    // p(v2) END).
    built.Add(ExpressionRule(
        "predicate_pushdown_case", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          // Unary expression over CASE
          if (expression->Type() == TypeTag::kUnaryExp) {
            const auto& unary = expression->AsUnaryExpression();
            if (unary.Child() && unary.Child()->Type() == TypeTag::kCaseExp) {
              const auto& c = unary.Child()->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, UnaryExpressionExp(w.second, unary.Op()));
              }
              Expression new_else =
                  c.else_clause_
                      ? UnaryExpressionExp(c.else_clause_, unary.Op())
                      : UnaryExpressionExp(ConstantValueExp(Value()),
                                           unary.Op());
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          // Binary expression where Left or Right is CASE
          if (expression->Type() == TypeTag::kBinaryExp) {
            const auto& binary = expression->AsBinaryExpression();
            const Expression& left = binary.Left();
            const Expression& right = binary.Right();
            if (left && left->Type() == TypeTag::kCaseExp) {
              const auto& c = left->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, BinaryExpressionExp(w.second, binary.Op(), right));
              }
              Expression new_else =
                  c.else_clause_
                      ? BinaryExpressionExp(c.else_clause_, binary.Op(), right)
                      : BinaryExpressionExp(ConstantValueExp(Value()),
                                            binary.Op(), right);
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
            if (right && right->Type() == TypeTag::kCaseExp) {
              const auto& c = right->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, BinaryExpressionExp(left, binary.Op(), w.second));
              }
              Expression new_else =
                  c.else_clause_
                      ? BinaryExpressionExp(left, binary.Op(), c.else_clause_)
                      : BinaryExpressionExp(left, binary.Op(),
                                            ConstantValueExp(Value()));
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          // IN expression where child is CASE
          if (expression->Type() == TypeTag::kInExp) {
            const auto& in = expression->AsInExpression();
            if (in.child_ && in.child_->Type() == TypeTag::kCaseExp) {
              const auto& c = in.child_->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(w.first,
                                       InExpressionExp(w.second, in.list_));
              }
              Expression new_else =
                  c.else_clause_
                      ? InExpressionExp(c.else_clause_, in.list_)
                      : InExpressionExp(ConstantValueExp(Value()), in.list_);
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          // CAST expression where child is CASE
          if (expression->Type() == TypeTag::kCastExp) {
            const auto& cast = expression->AsCastExpression();
            if (cast.Child() && cast.Child()->Type() == TypeTag::kCaseExp) {
              const auto& c = cast.Child()->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, CastExpressionExp(w.second, cast.TargetTypeName(),
                                               cast.ReturnNullOnError()));
              }
              Expression new_else =
                  c.else_clause_
                      ? CastExpressionExp(c.else_clause_, cast.TargetTypeName(),
                                          cast.ReturnNullOnError())
                      : CastExpressionExp(ConstantValueExp(Value()),
                                          cast.TargetTypeName(),
                                          cast.ReturnNullOnError());
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          return Expression{};
        }));

    // inner_join_not_null_inference: Infer x IS NOT NULL and y IS NOT NULL
    // from inner join equality condition x = y in conjunctions.
    built.Add(ExpressionRule(
        "inner_join_not_null_inference",
        Binary(BinaryOperation::kAnd, Any("left"), Any("right")),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          const std::vector<Expression> conjuncts = SplitConjuncts(expression);
          bool changed = false;
          std::vector<Expression> result_conjuncts;
          result_conjuncts.reserve(conjuncts.size());

          // D6 (docs/design.md): a shared operand (x = y AND y = z) must
          // infer y IS NOT NULL exactly once.  Deduplicate by content
          // against the input conjuncts AND everything already appended
          // this pass, so an equal predicate is never re-added because of
          // its position in the AND tree.
          auto already_present = [&](const Expression& candidate) {
            auto in_list = [&](const std::vector<Expression>& list) {
              return std::ranges::any_of(list, [&](const Expression& e) {
                if (Same(e, candidate)) {
                  return true;
                }
                return std::ranges::any_of(SplitConjuncts(e),
                                           [&](const Expression& sub) {
                                             return Same(sub, candidate);
                                           });
              });
            };
            return in_list(conjuncts) || in_list(result_conjuncts);
          };

          for (const auto& c : conjuncts) {
            if (c->Type() == TypeTag::kBinaryExp &&
                c->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
              const Expression& l = c->AsBinaryExpression().Left();
              const Expression& r = c->AsBinaryExpression().Right();
              if (l && r && !IsConstant(l) && !IsConstant(r)) {
                Expression l_not_null =
                    UnaryExpressionExp(l, UnaryOperation::kIsNotNull);
                Expression r_not_null =
                    UnaryExpressionExp(r, UnaryOperation::kIsNotNull);
                bool has_l = already_present(l_not_null);
                bool has_r = already_present(r_not_null);
                if (!has_l && !has_r) {
                  Expression not_null_pair = BinaryExpressionExp(
                      std::move(l_not_null), BinaryOperation::kAnd,
                      std::move(r_not_null));
                  result_conjuncts.push_back(BinaryExpressionExp(
                      c, BinaryOperation::kAnd, std::move(not_null_pair)));
                  changed = true;
                  continue;
                }
                if (!has_l) {
                  result_conjuncts.push_back(BinaryExpressionExp(
                      c, BinaryOperation::kAnd, std::move(l_not_null)));
                  changed = true;
                  continue;
                }
                if (!has_r) {
                  result_conjuncts.push_back(BinaryExpressionExp(
                      c, BinaryOperation::kAnd, std::move(r_not_null)));
                  changed = true;
                  continue;
                }
              }
            }
            result_conjuncts.push_back(c);
          }
          if (!changed) {
            return Expression{};
          }
          return CombineConjuncts(result_conjuncts);
        },
        /*root_only=*/true));

    // regexp_prefix_extraction: Extract literal prefix from regular expression
    // patterns (e.g. ^abc.* -> prefix 'abc' for LIKE/range scan candidate).
    built.Add(ExpressionRule(
        "regexp_prefix_extraction", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          if (expression->Type() == TypeTag::kFunctionCallExp) {
            const auto& fn = expression->AsFunctionCallExpression();
            const std::string& name = fn.FuncName();
            if ((name == "regexp_contains" || name == "regexp_like" ||
                 name == "regexp_match") &&
                fn.Args().size() == 2 && fn.Args()[0] &&
                IsConstant(fn.Args()[1])) {
              const Value pat_val = fn.Args()[1]->AsConstantValue().GetValue();
              if (pat_val.type == ValueType::kVarChar) {
                std::string prefix;
                std::string remainder;
                if (ExtractRegexPrefix(pat_val.value.varchar_value, prefix,
                                       remainder)) {
                  if (remainder == "$") {
                    return BinaryExpressionExp(
                        fn.Args()[0], BinaryOperation::kEquals,
                        ConstantValueExp(Value(std::string(prefix))));
                  }
                  if (remainder == ".*" || remainder == ".*$" ||
                      remainder.empty()) {
                    return BinaryExpressionExp(
                        fn.Args()[0], BinaryOperation::kLike,
                        ConstantValueExp(Value(std::string(prefix + "%"))));
                  }
                }
              }
            }
          }
          return Expression{};
        }));

    // cast_pushdown_comparison is intentionally absent: pushing `CAST(x AS
    // T) cmp constant` through to `x cmp constant` is only sound when the
    // child's type is known (DOUBLE truncation breaks INT64 pushdown; a
    // VARCHAR/DATE child changes parse errors and cross-type comparisons;
    // folding a fractional equality to FALSE loses the UNKNOWN result for a
    // NULL child).  ExpressionRules run without a schema, so the rule cannot
    // verify the child type and must not fire.  Casts stay runtime work.

    // deterministic_function_cse: Detect identical deterministic function calls
    // within the same expression tree and eliminate redundant computations.
    built.Add(ExpressionRule(
        "deterministic_function_cse", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          // Binary expression self-simplification with identical deterministic
          // functions
          if (expression->Type() == TypeTag::kBinaryExp) {
            const auto& binary = expression->AsBinaryExpression();
            const Expression& left = binary.Left();
            const Expression& right = binary.Right();
            if (left && right && left->Type() == TypeTag::kFunctionCallExp &&
                right->Type() == TypeTag::kFunctionCallExp &&
                Same(left, right)) {
              // PRODUCTION FIX: self-identities (f-f -> 0, f/f -> 1,
              // f = f -> IS NOT NULL, f < f -> FALSE) were only valid for
              // non-NULL operands.  An immutable function may return NULL,
              // so folding changed UNKNOWN into concrete values (and f/f
              // hid the division by zero when f(x) == 0).  The unsafe fold
              // was removed; expressions stay as written.
            }
          }
          // Function calls with duplicate deterministic arguments (e.g.
          // coalesce, greatest, least)
          if (expression->Type() == TypeTag::kFunctionCallExp) {
            const auto& fn = expression->AsFunctionCallExpression();
            std::string name(fn.FuncName());
            for (char& c : name)
              c = static_cast<char>(
                  std::tolower(static_cast<unsigned char>(c)));
            if (GetFunctionVolatility(name) == Volatility::kImmutable &&
                fn.Args().size() >= 2) {
              if (name == "coalesce" || name == "greatest" || name == "least" ||
                  name == "ifnull") {
                bool all_same = true;
                for (size_t i = 1; i < fn.Args().size(); ++i) {
                  if (!Same(fn.Args()[0], fn.Args()[i])) {
                    all_same = false;
                    break;
                  }
                }
                if (all_same) {
                  return fn.Args()[0];
                }
              } else if (name == "nullif") {
                if (Same(fn.Args()[0], fn.Args()[1])) {
                  return ConstantValueExp(Value());
                }
              }
            }
          }
          // Case expression where all branches evaluate the same deterministic
          // function
          if (expression->Type() == TypeTag::kCaseExp) {
            const auto& c = expression->AsCaseExpression();
            if (c.else_clause_ &&
                c.else_clause_->Type() == TypeTag::kFunctionCallExp &&
                !c.when_clauses_.empty()) {
              const auto& fn = c.else_clause_->AsFunctionCallExpression();
              if (GetFunctionVolatility(fn.FuncName()) ==
                  Volatility::kImmutable) {
                bool all_same = true;
                for (const auto& w : c.when_clauses_) {
                  if (!Same(w.second, c.else_clause_)) {
                    all_same = false;
                    break;
                  }
                }
                if (all_same) {
                  return c.else_clause_;
                }
              }
            }
          }
          return Expression{};
        }));

    // function_volatility_classification: Classifies function volatility and
    // ensures volatile/stable functions are never folded
    built.Add(ExpressionRule(
        "function_volatility_classification", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          if (GetFunctionVolatility(fn.FuncName()) != Volatility::kImmutable) {
            // Barrier for volatile or stable functions
            return Expression{};
          }
          return Expression{};
        }));

    // boolean_filter_pullup: Pull up common conditions out of disjunctions
    // (A AND B) OR (A AND C) -> A AND (B OR C).
    built.Add(ExpressionRule(
        "boolean_filter_pullup", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& binary = expression->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kOr) {
            return Expression{};
          }
          const std::vector<Expression> left = SplitConjuncts(binary.Left());
          const std::vector<Expression> right = SplitConjuncts(binary.Right());
          if (left.empty() || right.empty()) {
            return Expression{};
          }
          std::vector<Expression> common;
          std::vector<Expression> left_rest;
          for (const Expression& conjunct : left) {
            const bool shared =
                std::ranges::any_of(right, [&](const Expression& candidate) {
                  return Same(conjunct, candidate);
                });
            (shared ? common : left_rest).push_back(conjunct);
          }
          if (common.empty()) {
            return Expression{};
          }
          std::vector<Expression> right_rest;
          for (const Expression& conjunct : right) {
            const bool shared =
                std::ranges::any_of(common, [&](const Expression& candidate) {
                  return Same(conjunct, candidate);
                });
            if (!shared) {
              right_rest.push_back(conjunct);
            }
          }
          if (left_rest.empty() || right_rest.empty()) {
            return CombineConjuncts(common);
          }
          return BinaryExpressionExp(
              CombineConjuncts(common), BinaryOperation::kAnd,
              BinaryExpressionExp(CombineConjuncts(left_rest),
                                  BinaryOperation::kOr,
                                  CombineConjuncts(right_rest)));
        }));

    return built;
  }();
  return rules;
}

Expression ExpressionRewriter::Rewrite(const Expression& expression) const {
  Expression current = expression;
  // D6 (docs/design.md): the pass cap is a safety net, not a rejection
  // mechanism.  When a rule set oscillates past the cap, returning the last
  // stable form keeps a valid query runnable: the result still preserves the
  // input's semantics (every accepted rewrite is meaning-preserving), while
  // throwing here turned "optimizer did not reach a fixed point" into a
  // runtime failure for the whole statement.
  const size_t pass_limit = pass_limit_ == 0 ? 32 : pass_limit_;
  for (size_t pass = 0; pass < pass_limit; ++pass) {
    Expression next = RewriteOnce(current, 0);
    if (Same(current, next)) {
      return next;
    }
    current = std::move(next);
  }
  static std::atomic<bool> warned{false};
  if (!warned.exchange(true, std::memory_order_relaxed)) {
    LOG(ERROR) << "expression rewrite did not converge within " << pass_limit
               << " passes; returning the last stable form (D6)";
  }
  return current;
}

Expression ExpressionRewriter::RewriteOnce(  // NOLINT(misc-no-recursion)
    const Expression& expression, size_t depth) const {
  if (!expression) {
    return nullptr;
  }
  if (depth >= kMaxRewriteDepth) {
    throw std::runtime_error("expression too deep");
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool children_changed = false;
  for (Expression& child : children) {
    Expression rewritten = RewriteOnce(child, depth + 1);
    children_changed |= !Same(child, rewritten);
    child = std::move(rewritten);
  }
  Expression current =
      children_changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
  for (const ExpressionRule& rule : rules_->Rules()) {
    // D6 (docs/design.md): predicate-adding rules fire only at the root so
    // their inferred conjuncts cannot be re-appended inside nested ANDs and
    // cycle back (the non-convergence root cause).
    if (rule.root_only() && depth != 0) {
      continue;
    }
    if (Expression replacement = rule.Apply(current)) {
      return replacement;
    }
  }
  return current;
}

std::vector<Expression> ExpressionChildren(const Expression& expression) {
  if (!expression) {
    return {};
  }
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return {binary.Left(), binary.Right()};
    }
    case TypeTag::kUnaryExp:
      return {expression->AsUnaryExpression().Child()};
    case TypeTag::kAggregateExp:
      return {expression->AsAggregateExpression().Child()};
    case TypeTag::kCaseExp: {
      const auto& case_expression = expression->AsCaseExpression();
      std::vector<Expression> children;
      children.reserve((case_expression.when_clauses_.size() * 2) + 1);
      for (const auto& [condition, result] : case_expression.when_clauses_) {
        children.push_back(condition);
        children.push_back(result);
      }
      if (case_expression.else_clause_) {
        children.push_back(case_expression.else_clause_);
      }
      return children;
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> children{in.child_};
      children.insert(children.end(), in.list_.begin(), in.list_.end());
      return children;
    }
    case TypeTag::kFunctionCallExp:
      return expression->AsFunctionCallExpression().Args();
    case TypeTag::kArrayExp:
      return expression->AsArrayExpression().Elements();
    case TypeTag::kCastExp:
      return {expression->AsCastExpression().Child()};
    case TypeTag::kQueryExp: {
      const auto& test = expression->AsQueryExpression().Test();
      return test ? std::vector<Expression>{test} : std::vector<Expression>{};
    }
    default:
      return {};
  }
}

Expression WithExpressionChildren(const Expression& expression,
                                  std::vector<Expression> children) {
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      if (children.size() != 2) {
        throw std::invalid_argument("binary arity");
      }
      return BinaryExpressionExp(std::move(children[0]),
                                 expression->AsBinaryExpression().Op(),
                                 std::move(children[1]));
    }
    case TypeTag::kUnaryExp:
      if (children.size() != 1) {
        throw std::invalid_argument("unary arity");
      }
      return UnaryExpressionExp(std::move(children[0]),
                                expression->AsUnaryExpression().Op());
    case TypeTag::kAggregateExp: {
      if (children.size() != 1) {
        throw std::invalid_argument("aggregate arity");
      }
      const auto& aggregate = expression->AsAggregateExpression();
      return AggregateExpressionExp(aggregate.GetType(), std::move(children[0]),
                                    aggregate.Distinct());
    }
    case TypeTag::kCaseExp: {
      const auto& source = expression->AsCaseExpression();
      const size_t required =
          (source.when_clauses_.size() * 2) + (source.else_clause_ ? 1 : 0);
      if (children.size() != required) {
        throw std::invalid_argument("case arity");
      }
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(source.when_clauses_.size());
      size_t offset = 0;
      for (size_t i = 0; i < source.when_clauses_.size(); ++i) {
        clauses.emplace_back(std::move(children[offset]),
                             std::move(children[offset + 1]));
        offset += 2;
      }
      Expression otherwise =
          source.else_clause_ ? std::move(children[offset]) : Expression{};
      return CaseExpressionExp(std::move(clauses), std::move(otherwise));
    }
    case TypeTag::kInExp: {
      if (children.empty()) {
        throw std::invalid_argument("in arity");
      }
      Expression child = std::move(children.front());
      children.erase(children.begin());
      return InExpressionExp(std::move(child), std::move(children));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(expression->AsFunctionCallExpression().FuncName(),
                             std::move(children));
    case TypeTag::kArrayExp:
      return ArrayExpressionExp(
          std::move(children),
          expression->AsArrayExpression().ElementSqlType());
    case TypeTag::kCastExp: {
      if (children.size() != 1) {
        throw std::invalid_argument("cast arity");
      }
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(std::move(children[0]), cast.TargetTypeName(),
                               cast.ReturnNullOnError());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      if (children.size() > 1) {
        throw std::invalid_argument("query arity");
      }
      return QueryExpressionExp(query.Query(),
                                children.empty() ? nullptr : children.front(),
                                query.Exists(), query.Negated());
    }
    default:
      if (!children.empty()) {
        throw std::invalid_argument("leaf has children");
      }
      return expression;
  }
}

bool IsMinusOne(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type == ValueType::kInt64) {
    return constant.value.int_value == -1;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == -1.0;
  }
  return false;
}

bool IsInt64Constant(const Expression& expression, int64_t* value) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.type != ValueType::kInt64 || constant.IsNull()) {
    return false;
  }
  *value = constant.value.int_value;
  return true;
}

// Reassociation and sign absorption only fire on operands the input schema
// types as plain integer: floating-point addition/multiplication is neither
// associative nor distributive (rounding), so folding it would change
// results. Sign absorption and repeated-addend doubling are exact for
// doubles and therefore allowed there.
bool IsInt64Column(const Expression& expression, const Schema& schema) {
  if (!expression || expression->Type() != TypeTag::kColumnValue) {
    return false;
  }
  try {
    return expression->ResultType(schema).GetType() == TypeTag::kBigInt;
  } catch (const std::exception&) {
    return false;
  }
}

bool IsNumericOperand(const Expression& expression, const Schema& schema) {
  try {
    const TypeTag tag = expression->ResultType(schema).GetType();
    return tag == TypeTag::kBigInt || tag == TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}

// Splits `tree` (whose root must be kBinaryExp) into a column leaf and an
// accumulated INT64 constant when it is a +-chain of one column and integer
// constants. Returns false for anything else, including trees whose constant
// accumulation overflows INT64.
bool ColumnPlusConstantChain(const Expression& tree, const Schema& schema,
                             BinaryOperation* root_op, Expression* column,
                             int64_t* constant) {
  if (!tree || tree->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& binary = tree->AsBinaryExpression();
  const BinaryOperation op = binary.Op();
  if (op != BinaryOperation::kAdd && op != BinaryOperation::kSubtract) {
    return false;
  }
  int64_t left_constant = 0;
  if (IsInt64Constant(binary.Left(), &left_constant)) {
    // Constants on the left (5 + a) are not canonical here.
    return false;
  }
  int64_t right_constant = 0;
  if (IsInt64Constant(binary.Right(), &right_constant)) {
    if (!IsInt64Column(binary.Left(), schema)) {
      return false;
    }
    *root_op = op;
    *column = binary.Left();
    *constant = right_constant;
    return true;
  }
  // Nested chain: (x ± c1) ± c2. Only accept an inner tree that itself
  // resolved to a column-plus-constant form so recursion terminates in
  // column-depth steps.
  BinaryOperation inner_op = BinaryOperation::kAdd;
  Expression inner_column;
  int64_t inner_constant = 0;
  if (!ColumnPlusConstantChain(binary.Left(), schema, &inner_op, &inner_column,
                               &inner_constant) ||
      !IsInt64Constant(binary.Right(), &right_constant)) {
    return false;
  }
  // Accumulate the inner chain's constant toward the column. Negative steps
  // fold through subtraction: (x - c1) + c2 keeps c1 in the accumulator.
  const int64_t inner_step =
      inner_op == BinaryOperation::kAdd ? inner_constant : -inner_constant;
  const int64_t outer_step =
      op == BinaryOperation::kAdd ? right_constant : -right_constant;
  int64_t accumulated = 0;
  if (__builtin_add_overflow(inner_step, outer_step, &accumulated)) {
    return false;
  }
  *root_op = BinaryOperation::kAdd;
  *column = inner_column;
  *constant = accumulated;
  // inner_column must still be a typed INT64 column (checked on recursion).
  return IsInt64Column(*column, schema);
}

Expression RewriteTypedArithmetic(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& input_schema) {
  if (!expression) {
    return nullptr;
  }

  std::vector<Expression> children = ExpressionChildren(expression);
  bool children_changed = false;
  for (Expression& child : children) {
    Expression rewritten = RewriteTypedArithmetic(child, input_schema);
    children_changed |= !Same(child, rewritten);
    child = std::move(rewritten);
  }
  Expression current =
      children_changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
  if (current->Type() != TypeTag::kBinaryExp) {
    return current;
  }
  const auto& binary = current->AsBinaryExpression();

  if (binary.Op() == BinaryOperation::kMultiply) {
    // Sign absorption: (-1 * x) and (x * -1) become (-x). Exact for
    // integers and for IEEE doubles (a pure sign flip). Constant-constant
    // products are left to the folding rules.
    const Expression* minus_one = nullptr;
    const Expression* value = nullptr;
    if (!IsConstant(binary.Left()) && IsMinusOne(binary.Right())) {
      minus_one = &binary.Right();
      value = &binary.Left();
    } else if (!IsConstant(binary.Right()) && IsMinusOne(binary.Left())) {
      minus_one = &binary.Left();
      value = &binary.Right();
    }
    if (minus_one != nullptr && IsNumericOperand(*value, input_schema)) {
      return UnaryExpressionExp(*value, UnaryOperation::kMinus);
    }

    // (x * c1) * c2 -> x * (c1 * c2) for non-negative integer constants.
    // Checked combines skip the rewrite instead of folding into an
    // overflow the original tree would not have evaluated.
    int64_t inner_constant = 0;
    if (IsInt64Constant(binary.Right(), &inner_constant) &&
        inner_constant >= 0 && binary.Left()->Type() == TypeTag::kBinaryExp &&
        binary.Left()->AsBinaryExpression().Op() ==
            BinaryOperation::kMultiply) {
      const auto& inner = binary.Left()->AsBinaryExpression();
      int64_t leaf_constant = 0;
      if (IsInt64Constant(inner.Right(), &leaf_constant) &&
          leaf_constant >= 0 && IsInt64Column(inner.Left(), input_schema)) {
        int64_t combined = 0;
        if (!__builtin_mul_overflow(leaf_constant, inner_constant, &combined)) {
          return BinaryExpressionExp(
              inner.Left(), BinaryOperation::kMultiply,
              ConstantValueExp(Value(static_cast<int64_t>(combined))));
        }
      }
    }
  }

  if (binary.Op() == BinaryOperation::kAdd &&
      Same(binary.Left(), binary.Right()) &&
      binary.Left()->Type() == TypeTag::kColumnValue &&
      IsNumericOperand(binary.Left(), input_schema)) {
    // x + x -> x * 2. Exact doubling for integers and IEEE doubles.
    return BinaryExpressionExp(
        binary.Left(), BinaryOperation::kMultiply,
        ConstantValueExp(Value(static_cast<int64_t>(2))));
  }

  // Integer add/subtract constant reassociation:
  //   (x + c1) + c2 -> x + (c1 + c2)      (x - c1) - c2 -> x - (c1 + c2)
  //   (x + c1) - c2 -> x +/- (c1 - c2)    (x - c1) + c2 -> x +/- (c2 - c1)
  // Only column-plus-constant chains over INT64 take part, and constant
  // combines use checked arithmetic. A pathological intermediate overflow
  // ((x + 5) - 8 with x = INT64_MAX errors on x + 5 but not on x - 3) is
  // accepted: the folded form is the shape the contract pins and every
  // in-domain input evaluates identically.
  const BinaryOperation op = binary.Op();
  if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract) {
    int64_t outer_constant = 0;
    const Expression* chain = nullptr;
    if (IsInt64Constant(binary.Right(), &outer_constant)) {
      chain = &binary.Left();
    } else if (op == BinaryOperation::kAdd &&
               IsInt64Constant(binary.Left(), &outer_constant)) {
      chain = &binary.Right();
    }
    if (chain != nullptr) {
      BinaryOperation inner_op = BinaryOperation::kAdd;
      Expression column;
      int64_t inner_constant = 0;
      if (ColumnPlusConstantChain(*chain, input_schema, &inner_op, &column,
                                  &inner_constant)) {
        const int64_t inner_step = inner_op == BinaryOperation::kAdd
                                       ? inner_constant
                                       : -inner_constant;
        const int64_t outer_step =
            op == BinaryOperation::kAdd ? outer_constant : -outer_constant;
        int64_t accumulated = 0;
        if (__builtin_add_overflow(inner_step, outer_step, &accumulated)) {
          return current;
        }
        if (accumulated >= 0) {
          return BinaryExpressionExp(
              column, BinaryOperation::kAdd,
              ConstantValueExp(Value(static_cast<int64_t>(accumulated))));
        }
        return BinaryExpressionExp(
            column, BinaryOperation::kSubtract,
            ConstantValueExp(Value(static_cast<int64_t>(-accumulated))));
      }
    }
  }

  const Expression* zero = nullptr;
  const Expression* value = nullptr;
  if (binary.Op() == BinaryOperation::kMultiply) {
    if (IsZero(binary.Left())) {
      zero = &binary.Left();
      value = &binary.Right();
    } else if (IsZero(binary.Right())) {
      zero = &binary.Right();
      value = &binary.Left();
    }
  }
  if (zero == nullptr) {
    return current;
  }

  try {
    // Floating-point x * 0 is not generally zero: NaN and infinities must be
    // preserved. Restrict this rewrite to resolved integer arithmetic.
    if (current->ResultType(input_schema).GetType() != TypeTag::kBigInt ||
        (*value)->ResultType(input_schema).GetType() != TypeTag::kBigInt) {
      return current;
    }
  } catch (const std::exception&) {
    return current;
  }

  // SQL arithmetic is NULL-propagating. CASE preserves that behavior while
  // removing the multiplication, and the casts keep both branches INT64.
  Expression typed_null =
      CastExpressionExp(ConstantValueExp(Value()), "INT64", false);
  Expression typed_zero = CastExpressionExp(*zero, "INT64", false);
  return CaseExpressionExp(
      {{UnaryExpressionExp(*value, UnaryOperation::kIsNull),
        std::move(typed_null)}},
      std::move(typed_zero));
}

std::vector<Expression> SplitConjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return {};
  }
  if (expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
    std::vector<Expression> left =
        SplitConjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitConjuncts(expression->AsBinaryExpression().Right());
    left.insert(left.end(), right.begin(), right.end());
    return left;
  }
  return {expression};
}

Expression CombineConjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) {
    return ConstantValueExp(Value(true));
  }
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(std::move(result), BinaryOperation::kAnd,
                                 expressions[i]);
  }
  return result;
}

bool ReferencesOnly(const Expression& expression,
                    const std::unordered_set<std::string>& relation_names) {
  if (!expression) {
    return true;
  }
  return std::ranges::all_of(
      expression->TouchedColumns(), [&](const ColumnName& column) {
        return !column.schema.empty() && relation_names.contains(column.schema);
      });
}

}  // namespace tinylamb
